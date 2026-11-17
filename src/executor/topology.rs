//! Translated from `Topology` in `taskflow/core/graph.hpp` and `_set_up_graph`
//! in `taskflow/core/executor.hpp` (Taskflow v4.1.0).
//!
//! A [`Topology`] is one run of one [`Taskflow`]: the counter that says how
//! much of the graph is still outstanding, the policy that decides whether to
//! run the graph again, and the slot a `wait` blocks on. Upstream keeps it in a
//! `std::shared_ptr` that both the `tf::Future` and every queued `Node*` hold a
//! share of; here it is an `Arc` shared by exactly two owners -- the
//! [`RunHandle`](crate::RunHandle) the run was handed back as, and the
//! taskflow's queue of pending runs -- and the handle's borrow is what keeps
//! the *taskflow* alive, which is the whole reason `run` hands back a handle
//! with a lifetime instead of a detachable future.
//!
//! # The one `unsafe` boundary in the executor
//!
//! A worker queue is `WorkerQueue<Job>` -- one type, built when the executor
//! is, with no lifetime to spare -- while the thing a job points at lives for
//! exactly one run. Erasing that lifetime is what [`Job`] is for, and it is
//! the only place in the crate where a reference is reconstructed from a raw
//! pointer. The argument that makes it sound is one sentence long, which is
//! why this is the boundary worth having: a job referencing a run exists only
//! between [`Topology::set_up`] and [`Topology::finish`], and `finish` -- the
//! release of the thread that owns the topology and the taskflow's borrow --
//! is the *last* thing the worker that ends the run does.
//!
//! The join counter reaching zero is not the end of that window, because a
//! `run_n` or `run_until` topology may send its graph round again: a worker
//! that drives the counter to zero either re-arms the graph and queues its
//! sources afresh, in which case the run is still in flight, or ends the run
//! -- and `finish` is the last statement of that path, after the callback,
//! after the taskflow's next queued run has been started, and after the
//! executor's own bookkeeping.
//!
//! The topology is heap-allocated before the first job that names it is
//! built, and the `Arc` that allocates it is held by the run handle for as
//! long as the handle lives. Nothing can take it away while a job is queued:
//! `run` has no fallible step between scheduling and returning, and the
//! handle blocks when it is dropped. The taskflow's run queue holds a second
//! share, dropped by the tear-down that pops the finished run -- which is
//! strictly before [`Topology::finish`], so a share is outstanding throughout.
//!
//! Two things keep that boundary as small as it is. A job addresses its node
//! by [`Task`] index rather than by pointer, so the node comes out of the
//! graph through safe code; and the pointer to the taskflow lives in the job
//! rather than in the topology, so [`Topology`] itself is an ordinary `Send +
//! Sync` value with no raw pointer in it.

use std::any::Any;
use std::panic::{self, AssertUnwindSafe};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Poll, Waker};

use crate::graph::{Node, Task};
use crate::task::TaskRef;
use crate::taskflow::Taskflow;

/// One unit of work in a worker's queue: upstream's `Node*`, plus what
/// upstream reaches through the node's `_topology` and `_parent` pointers.
///
/// Upstream's queues hold a bare `Node*` because a `Node` knows its own
/// topology; a node here is a slot in an arena that knows nothing about the
/// run it is taking part in, so the job carries the run instead. The three
/// fields are the thin pair that costs: a [`Task`] index, and one pointer
/// each to the taskflow and to the run.
///
/// See the module docs for why the pointers are raw and what keeps them
/// valid.
#[derive(Debug, Hash)]
pub(crate) struct Job {
    /// Which task of the taskflow's graph to run.
    task: Task,
    /// The taskflow being run, whose graph `task` indexes.
    taskflow: NonNull<Taskflow>,
    /// The run `task` is part of.
    topology: NonNull<Topology>,
}

// SAFETY: a `Job` is created on the thread that schedules a task and
// dereferenced on whichever worker dequeues it, so sending it across threads
// amounts to sharing the three values it addresses. All three are `Sync` --
// asserted below, rather than assumed -- and none of them is mutated through
// anything but an atomic or a lock while the run is in flight: the graph is
// borrowed immutably for the whole run, and the topology's own fields are an
// `AtomicUsize` and a `Mutex`. What keeps the *targets* alive is the run
// handle, as the module docs set out.
unsafe impl Send for Job {}

impl Job {
    /// Builds the job that runs `task` of `taskflow` as part of `topology`.
    ///
    /// Safe: it only takes addresses. Turning them back into references is
    /// [`Job::parts`], and that is where the obligation sits.
    pub(crate) fn new(task: Task, taskflow: &Taskflow, topology: &Topology) -> Self {
        Self {
            task,
            taskflow: NonNull::from(taskflow),
            topology: NonNull::from(topology),
        }
    }

    /// Resolves this job into the node to run, the taskflow it belongs to and
    /// the run it is part of.
    ///
    /// # Safety
    ///
    /// The run this job belongs to must still be in flight: the
    /// [`RunHandle`](crate::RunHandle) that owns the topology must not have
    /// been dropped, which also means the taskflow is still borrowed -- or
    /// owned -- by it. That holds for every job from the moment
    /// [`Topology::set_up`] counts it until [`Topology::finish`] releases the
    /// handle, and a job exists only inside that window -- see the module
    /// docs.
    ///
    /// # Panics
    ///
    /// Panics if the job's task is not a live node of the taskflow's graph,
    /// which cannot happen for a job built by the scheduler: a run borrows the
    /// graph immutably, so no node can be erased while a job names it.
    pub(crate) unsafe fn parts<'j>(&self) -> (&'j Node, &'j Taskflow, &'j Topology) {
        // SAFETY: the caller promises the run is still in flight, which is
        // exactly the promise that both targets are still alive: the topology
        // is the `Arc` that a run method allocates and its `RunHandle` then
        // holds a share of, and the taskflow is what that handle borrows -- or
        // owns, for a run handed a taskflow by value. Neither target is ever
        // moved: both are allocated before the first job is built, and only
        // the `Arc` and the `Box`, not their contents, are ever moved.
        let (taskflow, topology) = unsafe { (self.taskflow.as_ref(), self.topology.as_ref()) };
        (taskflow.graph().node(self.task), taskflow, topology)
    }
}

/// The part of a run a cancel token may touch: upstream's
/// `NodeBase::_estate`, in an allocation of its own.
///
/// Upstream's `tf::Future` *is* the cancel token -- it holds a
/// `std::weak_ptr<Topology>` and `cancel()` sets a bit through it -- and a
/// [`Canceller`](crate::Canceller) is the same move over a
/// `Weak<Cancellable>`. The indirection is what makes the token lifetime-free:
/// a [`RunHandle`](crate::RunHandle) carries the lifetimes of the executor and
/// the taskflow it borrows, and a token that a `'static` task body can capture
/// cannot carry either.
///
/// A separate allocation rather than a `Weak<Topology>`, for two reasons. The
/// module docs above rest on the topology having exactly two owners -- the run
/// handle and the taskflow's run queue -- and a token that could upgrade to an
/// `Arc<Topology>` would be a third; and it would hand a caller `join`,
/// `finish` and the run policy along with the one bit it asked for.
///
/// The estate word carries three of upstream's `ESTATE` bits, at upstream's
/// values: `CANCELLED`, which [`Cancellable::cancel`] sets, and the
/// `EXCEPTION | CAUGHT` pair that [`Topology::record_panic`] sets together.
/// `CANCELLED` and `EXCEPTION` mean the same thing to the scheduler, which is
/// why [`Cancellable::cancelled`] is a mask over those two, as upstream's
/// `Topology::cancelled` is; `CAUGHT` is not part of that mask and says only
/// that the payload slot is taken.
pub(crate) struct Cancellable {
    /// Upstream's `_estate`, holding the three bits a topology uses.
    estate: AtomicU32,
    /// Whether the run is over, which is to say whether there is still
    /// anything to cancel.
    ///
    /// Upstream needs no equivalent: its token's `weak_ptr` is dead once the
    /// tear-down has popped the topology off the taskflow's queue, so the
    /// allocation going away *is* its answer to "has this run finished". Here
    /// a run's topology is owned by a handle that outlives the run -- a caller
    /// may hold it long after `wait` returned -- so the allocation is still
    /// there and the question needs its own answer. [`Topology::finish`]
    /// writes this before it releases a waiter, so a `cancel` after a `wait`
    /// always reports `false`.
    finished: AtomicBool,
}

impl Cancellable {
    /// Upstream's `ESTATE::CANCELLED`: this run has been cancelled.
    pub(crate) const CANCELLED: u32 = 0x4000_0000;
    /// Upstream's `ESTATE::EXCEPTION`: a task of this run failed, which to the
    /// scheduler means the same as `CANCELLED`.
    pub(crate) const EXCEPTION: u32 = 0x1000_0000;
    /// Upstream's `ESTATE::CAUGHT`: this run's one payload slot is taken, so
    /// a later failure has nowhere to go. Set together with `EXCEPTION` and
    /// read on its own, which is how upstream decides that a panic is the
    /// first one -- see [`Topology::record_panic`].
    pub(crate) const CAUGHT: u32 = 0x2000_0000;

    /// A run that is neither cancelled nor finished.
    fn new() -> Self {
        Self {
            estate: AtomicU32::new(0),
            finished: AtomicBool::new(false),
        }
    }

    /// Sets `bits` in the estate word, reporting whether this call was the
    /// first to set them.
    ///
    /// Upstream's `_estate.fetch_or`, plus the answer
    /// [`Topology::record_panic`] needs: the task that first sets
    /// `EXCEPTION | CAUGHT` is the one whose panic the run reports, and every
    /// later one is a task whose failure the run already knows about.
    pub(crate) fn set(&self, bits: u32) -> bool {
        // Relaxed, as every upstream read and write of `_estate` is: the word
        // races with the scheduler by construction -- a task already running
        // finishes whether or not it sees the bit -- and what carries a run's
        // *results* between threads is the join counter and the completion
        // lock, not this.
        self.estate.fetch_or(bits, Ordering::Relaxed) & bits == 0
    }

    /// Whether this run is cancelled: upstream's `Topology::cancelled`, which
    /// is either bit of the pair rather than `CANCELLED` alone.
    pub(crate) fn cancelled(&self) -> bool {
        self.estate.load(Ordering::Relaxed) & (Self::CANCELLED | Self::EXCEPTION) != 0
    }

    /// Cancels the run, reporting `false` if it had already finished.
    ///
    /// Upstream's `Future::cancel`, whose `false` is its `weak_ptr::lock()`
    /// failing. Cancelling twice reports `true` twice, as upstream's does: the
    /// answer is about the run, not about this caller.
    pub(crate) fn cancel(&self) -> bool {
        if self.finished.load(Ordering::Relaxed) {
            return false;
        }
        self.set(Self::CANCELLED);
        true
    }

    /// Records that the run is over, so that a later `cancel` reports `false`.
    fn mark_finished(&self) {
        self.finished.store(true, Ordering::Relaxed);
    }
}

/// One run of one [`Taskflow`]: upstream's `tf::Topology`.
///
/// One upstream member is not here: `_promise` is a [`Completion`] behind a
/// `Mutex`, because the payload a panicking task hands back is not a value
/// `std::promise` would carry. Upstream's `_estate` *is* here, as
/// [`Cancellable`], but in its own allocation rather than inline -- see that
/// type for why.
pub(crate) struct Topology {
    /// How many nodes of this run are outstanding: upstream's
    /// `NodeBase::_join_counter` on the topology. Set to the number of source
    /// tasks by [`Topology::set_up`], raised by one for each successor that
    /// becomes ready and lowered by one as each node finishes, so it reaches
    /// zero exactly once -- when the last node of the run is done.
    join_counter: AtomicUsize,
    /// What decides whether the graph runs again, and what to call when it
    /// does not: upstream's `_predicate` and `_on_finish`.
    policy: Mutex<RunPolicy>,
    /// Everything a waiter needs, and the flag it waits on.
    completion: Mutex<Completion>,
    /// Signalled by [`Topology::finish`], under the lock.
    finished: Condvar,
    /// The cancellation and exception bits of this run, upstream's `_estate`,
    /// shared with every [`Canceller`](crate::Canceller) handed out for it.
    ///
    /// Owned here in the sense that upstream's `_estate` is a member of the
    /// topology; the extra allocation is what lets a token outlive the run
    /// handle's borrow. See [`Cancellable`].
    estate: Arc<Cancellable>,
}

/// Upstream's `Topology::_predicate` and `_on_finish`, which is what makes a
/// topology able to run its graph more than once.
///
/// Behind a `Mutex` because both are called on whichever worker happens to
/// finish a run, and the predicate is `FnMut` -- upstream's is a mutable
/// lambda too, since `run_n`'s is a counter. The lock is uncontended by
/// construction: the only thread that ever evaluates a run's policy is the one
/// that drove its join counter to zero, and there is exactly one of those per
/// run. It is a lock rather than an atomic because the values behind it are
/// arbitrary closures.
///
/// Both are `'static`, as [`Work`](crate::Work) is and for the same reason:
/// the topology is `Arc`-shared with the taskflow's run queue, which has no
/// lifetime to spare. Upstream's callbacks capture by reference; a caller that
/// wants that here shares an `Arc` with the closure instead.
struct RunPolicy {
    /// Called once before the first run and once after each run finishes.
    /// `true` means "do not run the graph again".
    predicate: Box<dyn FnMut() -> bool + Send>,
    /// Called once, after the last run of the graph. An `Option` because it is
    /// `FnOnce` and therefore has to be moved out to be called.
    on_finish: Option<Box<dyn FnOnce() + Send>>,
}

/// The completion slot, in place of upstream's `std::promise<void>`.
///
/// Who gets woken is deliberately two fields rather than one: a thread
/// blocked in [`RunHandle::wait`](crate::RunHandle::wait) is woken through the
/// condvar, and a future polling a run is woken through the [`Waker`] left by
/// [`Topology::poll_join`]. A handle is waited for one way or the other, never
/// both, but keeping the two slots separate is what lets the blocking path
/// stay a condvar rather than becoming a hand-rolled futex over the waker.
struct Completion {
    /// Whether the run has finished. The condvar's predicate.
    finished: bool,
    /// What the first task to panic panicked with, in place of upstream's
    /// `std::exception_ptr`. Taken by whoever waits, so that it is reported
    /// once.
    panic: Option<Box<dyn Any + Send>>,
    /// A waker left by a future waiting on this run, woken instead of -- and
    /// as well as -- the condvar.
    waker: Option<Waker>,
}

impl Topology {
    /// A run that has not started, over the policy `run_until` was given:
    /// `predicate` decides after each run whether to run the graph again, and
    /// `on_finish` is called once the answer is no.
    ///
    /// Upstream's `Topology(Taskflow&, P&&, C&&)`, minus the taskflow: a
    /// topology here deliberately holds no reference to the graph it runs, so
    /// the taskflow is a parameter of every method that needs it.
    pub(crate) fn new<P, C>(predicate: P, on_finish: C) -> Self
    where
        P: FnMut() -> bool + Send + 'static,
        C: FnOnce() + Send + 'static,
    {
        Self {
            join_counter: AtomicUsize::new(0),
            policy: Mutex::new(RunPolicy {
                predicate: Box::new(predicate),
                on_finish: Some(Box::new(on_finish)),
            }),
            completion: Mutex::new(Completion {
                finished: false,
                panic: None,
                waker: None,
            }),
            finished: Condvar::new(),
            estate: Arc::new(Cancellable::new()),
        }
    }

    /// Whether this run has been cancelled: upstream's `Topology::cancelled`.
    ///
    /// Read once per job, at the top of `WorkerThread::invoke`, which is
    /// upstream's `_is_parent_cancelled` early-out at the top of `_invoke`,
    /// and once more by [`Topology::run_again`].
    pub(crate) fn cancelled(&self) -> bool {
        self.estate.cancelled()
    }

    /// Cancels this run, reporting `false` if it has already finished:
    /// upstream's `Future::cancel`.
    pub(crate) fn cancel(&self) -> bool {
        self.estate.cancel()
    }

    /// The state a [`Canceller`](crate::Canceller) holds a `Weak` to.
    pub(crate) fn cancellable(&self) -> &Arc<Cancellable> {
        &self.estate
    }

    /// Prepares every node of `taskflow` for a run of this topology and
    /// returns how many of them are source tasks.
    ///
    /// Upstream's `_set_up_graph` plus the `_join_counter.store` that
    /// `_set_up_topology` does around it. The one departure is what happens to
    /// the sources: upstream swaps them to the front of its node vector and
    /// then bulk-schedules that prefix, which an arena cannot do -- a shuffle
    /// would re-point every live [`Task`] at another node. So the sources are
    /// found twice instead, here and in [`Topology::sources`], which costs a
    /// second walk over the graph and saves the allocation that collecting
    /// them would need.
    ///
    /// # Panics
    ///
    /// Panics if some task has 2^32 or more strong dependencies, which needs
    /// more duplicate edges than a [`Task`] index can address nodes.
    pub(crate) fn set_up(&self, taskflow: &Taskflow) -> usize {
        let graph = taskflow.graph();
        let mut num_sources = 0;

        for (task, node) in graph.iter() {
            // Upstream packs the strong-dependency count into the low 28 bits
            // of `_nstate` and masks it back out in `_invoke`. Nothing else
            // uses the word in this port yet, so the count gets all 32 bits
            // and no mask; the mask is what would have to come back when the
            // scheduler state bits do.
            let strong = TaskRef::new(graph, task).num_strong_dependencies();
            node.nstate().store(
                u32::try_from(strong).expect("a task has fewer than 2^32 strong dependencies"),
                Ordering::Relaxed,
            );
            node.estate().store(0, Ordering::Relaxed);
            node.join_counter().store(strong, Ordering::Relaxed);

            // A source is a node with no predecessors, which is not the same
            // test as "no strong dependencies" and is upstream's. The two part
            // company once conditioners exist: a task reached only by a
            // conditioner has no strong dependency, but it is still not a
            // place a run may start.
            if node.num_predecessors() == 0 {
                num_sources += 1;
            }
        }

        // Before any source can be queued, since a source can run and finish
        // -- and decrement this -- before the scheduling call that queued it
        // has returned.
        self.join_counter.store(num_sources, Ordering::Relaxed);
        num_sources
    }

    /// The jobs that start a run: one per source task of `taskflow`.
    ///
    /// Call it only on a topology [`Topology::set_up`] has just returned a
    /// non-zero count for, since the count is what the join counter was set
    /// to.
    pub(crate) fn sources<'t>(&'t self, taskflow: &'t Taskflow) -> impl Iterator<Item = Job> + 't {
        taskflow
            .graph()
            .iter()
            .filter(|(_, node)| node.num_predecessors() == 0)
            .map(move |(task, _)| Job::new(task, taskflow, self))
    }

    /// Accounts for one more node of this run becoming ready, before it is
    /// queued.
    ///
    /// Upstream's `node->_parent->_join_counter.fetch_add(1)`, where a
    /// top-level node's parent *is* the topology.
    pub(crate) fn job_queued(&self) {
        self.join_counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Accounts for one node of this run having finished, and reports whether
    /// it was the last.
    ///
    /// Upstream's `_tear_down_nonasync`, down to the ordering: the
    /// release half publishes everything the finished node wrote to whoever
    /// reads a zero, and the acquire half is what makes the zero reliable.
    pub(crate) fn job_finished(&self) -> bool {
        self.join_counter.fetch_sub(1, Ordering::AcqRel) == 1
    }

    /// How many jobs of this run are still outstanding, read without blocking.
    ///
    /// Zero means the run's last job has finished. This is the counter
    /// upstream's `corun` waits on -- `p->_join_counter.load(acquire) == 0` --
    /// and [`Executor::corun`](crate::Executor::corun) is the one caller:
    /// every other waiter has [`Topology::join`] and wants to be asleep, while
    /// a corunning worker must not sleep at all. Acquire for upstream's
    /// ordering; what makes it safe to *drop* a topology afterwards is
    /// [`Topology::join`], not this read.
    pub(crate) fn outstanding(&self) -> usize {
        self.join_counter.load(Ordering::Acquire)
    }

    /// Whether the run that has just finished should run the graph again:
    /// upstream's `!tpg->cancelled() && !tpg->_predicate()`, now both halves of
    /// it.
    ///
    /// This is the whole of the re-run decision, in one place and deliberately
    /// not inlined into the tear-down. The `&&` short-circuits, as upstream's
    /// does, which is the one thing worth noticing about the order: a cancelled
    /// run does not ask its predicate at all, so a `run_n` counter is not spent
    /// by a round that will never happen.
    ///
    /// A panicking task stops a `run_n` or `run_until` run too, and does it
    /// through [`Topology::cancelled`] rather than through a term of its own:
    /// the panic half upstream keeps commented out beside this condition is a
    /// task setting `EXCEPTION`, which is already one of the two bits that
    /// reads.
    pub(crate) fn run_again(&self) -> bool {
        !self.cancelled() && !self.stop_requested()
    }

    /// Evaluates the run predicate, reporting `true` for "stop".
    ///
    /// A panic out of the predicate is caught and recorded as the run's
    /// failure, and answers "stop". Upstream lets the exception propagate out
    /// of `_tear_down_topology`; here that would unwind past
    /// [`Topology::finish`] and leave the waiter blocked for good, so the
    /// failure is reported the way a panicking task's is.
    fn stop_requested(&self) -> bool {
        // The guard is dropped before the payload is recorded, so that the two
        // locks of a topology are never held at once.
        let evaluated = {
            let mut policy = self.policy();
            panic::catch_unwind(AssertUnwindSafe(|| (policy.predicate)()))
        };

        match evaluated {
            Ok(stop) => stop,
            Err(payload) => {
                self.record_panic(payload);
                true
            }
        }
    }

    /// Calls the on-finish callback, once. Upstream's `tpg->_on_finish()`.
    ///
    /// A panic out of the callback is caught and recorded, for the reason
    /// [`Topology::stop_requested`] gives.
    pub(crate) fn carry_out_callback(&self) {
        // Taken out from under the lock, so that the callback runs with no
        // lock of this topology held: it is user code, and the only thing it
        // may not do is reach back into the run that is finishing.
        let on_finish = self.policy().on_finish.take();

        if let Some(on_finish) = on_finish
            && let Err(payload) = panic::catch_unwind(AssertUnwindSafe(on_finish))
        {
            self.record_panic(payload);
        }
    }

    /// Records that a task of this run panicked, keeping the first payload and
    /// cancelling whatever the run had not started.
    ///
    /// Upstream's `_process_exception`, restricted to the one anchor this port
    /// has: upstream walks the node's parent chain to find the enclosing
    /// anchor and ends at the topology, which is constructed
    /// `EXPLICITLY_ANCHORED` for exactly that reason, so for a top-level task
    /// the whole of the walk is this one `fetch_or`. The subflow and runtime
    /// anchors it also finds are M4's.
    ///
    /// Two things come out of the one atomic. `EXCEPTION` is half of
    /// [`Topology::cancelled`], so setting it is the whole of "the rest of the
    /// run is cancelled": every job not yet dequeued is skipped by the
    /// early-out at the top of `WorkerThread::invoke`, and
    /// [`Topology::run_again`] answers no. And `CAUGHT` says the payload slot
    /// is taken, so the first panic is the one reported and later ones are
    /// dropped -- as upstream keeps only the first `exception_ptr`, for the
    /// same reason: with several tasks failing, the one the caller is told
    /// about should be the one that failed first, not the one that happened to
    /// finish unwinding last.
    ///
    /// Upstream masks `CAUGHT` alone where this masks both bits, which is the
    /// same test while the only thing that sets `EXCEPTION` on a topology is
    /// this method, setting the pair together. A second anchor kind would
    /// break that equivalence, which is why the pair is set in one call rather
    /// than in two.
    pub(crate) fn record_panic(&self, payload: Box<dyn Any + Send>) {
        // Upstream's `constexpr static auto flag = EXCEPTION | CAUGHT`, and
        // its one `fetch_or` over the pair. That atomic is the arbiter rather
        // than the lock, so a later panic of a run that already has one is
        // dropped on the way out of here without ever touching the completion
        // slot -- which is what keeps a thousand simultaneously failing tasks
        // off it.
        let flag = Cancellable::EXCEPTION | Cancellable::CAUGHT;
        if self.estate.set(flag) {
            self.lock().panic = Some(payload);
        }
    }

    /// Marks this run finished and releases whoever is waiting for it.
    /// Upstream's `_carry_out_promise`.
    ///
    /// **This is the last thing the caller may do with the topology.** The
    /// thread it releases owns the topology and frees it on the way out, and
    /// it owns the taskflow's borrow, so every node of the graph goes out of
    /// reach at the same moment. The notification is therefore sent *under*
    /// the lock, not after it: a waiter cannot return from
    /// [`Condvar::wait`](std::sync::Condvar::wait) until it has the mutex
    /// back, so holding it until the notify is sent makes the unlock this
    /// thread's last touch of the topology -- and an unlock a waiter has not
    /// yet observed cannot be a use-after-free.
    pub(crate) fn finish(&self) {
        // Before the lock, so that it is ordered before the unlock that
        // releases a waiter: a `cancel` that follows a `wait` -- or follows the
        // `join` in `RunHandle::drop` -- has to report that there was nothing
        // left to cancel. A relaxed store is enough for exactly that reason;
        // the happens-before is the completion lock's, not this word's.
        self.estate.mark_finished();

        let mut completion = self.lock();
        completion.finished = true;
        let waker = completion.waker.take();
        self.finished.notify_all();
        drop(completion);

        // The waker was moved out above, so this touches nothing that the
        // released waiter may have freed.
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Blocks until this run has finished and returns what its first
    /// panicking task panicked with, if any.
    ///
    /// Idempotent, which is what lets [`RunHandle`](crate::RunHandle) call it
    /// from both `wait` and `Drop`: a second call finds the flag already set
    /// and the payload already taken.
    pub(crate) fn join(&self) -> Option<Box<dyn Any + Send>> {
        let mut completion = self.lock();
        while !completion.finished {
            completion = self
                .finished
                .wait(completion)
                .unwrap_or_else(PoisonError::into_inner);
        }
        completion.panic.take()
    }

    /// Reports whether this run has finished, leaving `waker` to be woken when
    /// it does if it has not, and takes the panic payload when it has.
    ///
    /// The non-blocking half of [`Topology::join`], and the whole of what
    /// `impl Future for RunHandle` needs. Registering the waker and reading the
    /// flag happen under one lock, which is what stops a run finishing between
    /// the two and leaving nobody to wake.
    pub(crate) fn poll_join(&self, waker: &Waker) -> Poll<Option<Box<dyn Any + Send>>> {
        let mut completion = self.lock();
        if completion.finished {
            return Poll::Ready(completion.panic.take());
        }

        // Replaced rather than kept, because a future may be polled by a
        // different task each time; `will_wake` is the cheap way to skip the
        // clone in the common case where it is not.
        if !completion
            .waker
            .as_ref()
            .is_some_and(|current| current.will_wake(waker))
        {
            completion.waker = Some(waker.clone());
        }
        Poll::Pending
    }

    /// The completion slot. A poisoned lock is taken rather than propagated,
    /// as the executor's other locks are: the panic that poisoned it is
    /// already recorded in the slot, and refusing to hand the slot back would
    /// turn a reported failure into a stuck `wait`.
    fn lock(&self) -> MutexGuard<'_, Completion> {
        self.completion
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The run policy. Poisoning is taken rather than propagated, as it is for
    /// the completion slot: a predicate or callback that panicked has already
    /// been recorded as this run's failure.
    fn policy(&self) -> MutexGuard<'_, RunPolicy> {
        self.policy.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl core::fmt::Debug for Topology {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let completion = self.lock();
        f.debug_struct("Topology")
            .field("outstanding", &self.join_counter.load(Ordering::Relaxed))
            .field("finished", &completion.finished)
            .field("panicked", &completion.panic.is_some())
            .field("cancelled", &self.cancelled())
            .finish()
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}

    // The `Send` impl on `Job` rests on these three: a job hands out a
    // `&Node`, a `&Taskflow` and a `&Topology` on whichever worker dequeues
    // it, so all three have to be shareable across threads for that to mean
    // anything. `Topology` is also moved into the run handle, which a caller
    // may move to another thread.
    assert_send_sync::<Topology>();
    assert_send_sync::<Taskflow>();
    assert_send_sync::<Node>();

    // A `Canceller` is a `Weak<Cancellable>` and has to be `Send + Sync +
    // 'static`, which it is exactly when `Cancellable` is `Send + Sync` --
    // it holds nothing but atomics, and nothing with a lifetime.
    assert_send_sync::<Cancellable>();
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::Wake;

    use super::*;
    use crate::prelude::*;

    /// A run that runs its graph exactly once and has nothing to call
    /// afterwards: upstream's `run(f)` policy, which is what most of these
    /// cases want.
    fn once() -> Topology {
        Topology::new(|| true, || {})
    }

    /// A diamond, which is the smallest shape with a task that is neither a
    /// source nor a sink: `a -> {b, c} -> d`.
    fn diamond() -> (Taskflow, [Task; 4]) {
        let mut taskflow = Taskflow::new();
        let (a, b, c, d) = taskflow.emplace_n((|| {}, || {}, || {}, || {}));
        taskflow.task(a).precede([b, c]);
        taskflow.task(d).succeed([b, c]);
        (taskflow, [a, b, c, d])
    }

    #[test]
    fn setting_up_a_graph_counts_sources_and_arms_every_join_counter() {
        let (taskflow, [a, b, c, d]) = diamond();
        let topology = once();

        assert_eq!(topology.set_up(&taskflow), 1, "only `a` is a source");
        assert_eq!(topology.join_counter.load(Ordering::Relaxed), 1);

        let graph = taskflow.graph();
        for (task, expected) in [(a, 0_u32), (b, 1), (c, 1), (d, 2)] {
            let node = graph.node(task);
            assert_eq!(
                node.join_counter().load(Ordering::Relaxed),
                usize::from(u16::try_from(expected).expect("a small count"))
            );
            assert_eq!(node.nstate().load(Ordering::Relaxed), expected);
        }
    }

    /// Setting a topology up twice has to leave the graph exactly as the first
    /// time did, or the second run of a taskflow would start with counters the
    /// first run had already spent.
    #[test]
    fn setting_up_the_same_graph_again_rearms_it() {
        let (taskflow, [_, b, _, d]) = diamond();
        let topology = once();

        topology.set_up(&taskflow);
        // Spend the counters, as a run would.
        taskflow
            .graph()
            .node(b)
            .join_counter()
            .store(0, Ordering::Relaxed);
        taskflow
            .graph()
            .node(d)
            .join_counter()
            .store(0, Ordering::Relaxed);

        assert_eq!(topology.set_up(&taskflow), 1);
        assert_eq!(
            taskflow
                .graph()
                .node(d)
                .join_counter()
                .load(Ordering::Relaxed),
            2
        );
    }

    #[test]
    fn the_sources_are_the_tasks_with_no_predecessors() {
        let (taskflow, [a, ..]) = diamond();
        let topology = once();
        topology.set_up(&taskflow);

        let sources: Vec<Task> = topology.sources(&taskflow).map(|job| job.task).collect();
        assert_eq!(sources, vec![a]);
    }

    /// An empty graph has no sources, and so does a graph whose every node is
    /// in a cycle. Both answer zero, which is what tells `run` there is
    /// nothing to schedule.
    #[test]
    fn a_graph_with_no_source_sets_up_to_zero() {
        let topology = once();
        assert_eq!(topology.set_up(&Taskflow::new()), 0);

        let mut cyclic = Taskflow::new();
        let (a, b) = cyclic.emplace_n((|| {}, || {}));
        cyclic.task(a).precede([b]);
        cyclic.task(b).precede([a]);
        assert_eq!(topology.set_up(&cyclic), 0);
    }

    /// The join counter is the whole of the run's bookkeeping, so the shape of
    /// it is worth pinning down on its own: one node finishing is not the end
    /// of a run that queued two.
    #[test]
    fn a_run_ends_when_the_last_outstanding_job_finishes() {
        let (taskflow, _) = diamond();
        let topology = once();
        topology.set_up(&taskflow);

        // `a` finishes, having made `b` and `c` ready.
        topology.job_queued();
        topology.job_queued();
        assert!(!topology.job_finished());

        // `b` finishes; `c` has not.
        assert!(!topology.job_finished());
        // `c` finishes and makes `d` ready.
        topology.job_queued();
        assert!(!topology.job_finished());
        // `d` finishes, and nothing is outstanding.
        assert!(topology.job_finished());
    }

    #[test]
    fn the_first_panic_is_the_one_reported() {
        let topology = once();
        topology.record_panic(Box::new("first"));
        topology.record_panic(Box::new("second"));
        topology.finish();

        let payload = topology.join().expect("a panic was recorded");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"first"));
        // Taken, not cloned: the second waiter has nothing left to report.
        assert!(topology.join().is_none());
    }

    /// The other half of recording a panic, and the whole of "the rest of the
    /// run is cancelled": the bit the scheduler's early-out reads is set by
    /// the same atomic that claims the payload slot.
    #[test]
    fn recording_a_panic_cancels_the_rest_of_the_run() {
        let topology = once();
        assert!(!topology.cancelled());

        topology.record_panic(Box::new("boom"));
        assert!(topology.cancelled(), "the run was not cancelled");
        assert!(
            !topology.run_again(),
            "a failed run does not go round again"
        );

        // `CAUGHT` is not part of what `cancelled` reads, so a run cancelled
        // by its handle has a payload slot still to claim.
        let cancelled = once();
        assert!(cancelled.cancel());
        cancelled.record_panic(Box::new("boom"));
        cancelled.finish();
        let payload = cancelled.join().expect("a panic was recorded");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
    }

    #[test]
    fn joining_a_finished_run_does_not_block() {
        let topology = once();
        topology.finish();
        assert!(topology.join().is_none());
        assert!(topology.join().is_none());
    }

    /// The predicate is upstream's, counter and all: `run_n(f, k)` asks it
    /// once before the first run and once after each run, so a topology set up
    /// for `k` runs has to answer "again" exactly `k - 1` times.
    #[test]
    fn a_counting_predicate_runs_the_graph_the_number_of_times_asked() {
        for runs in 0..5_usize {
            let mut remaining = runs;
            let topology = Topology::new(
                move || {
                    let stop = remaining == 0;
                    remaining = remaining.saturating_sub(1);
                    stop
                },
                || {},
            );

            // The call `run_until` makes before scheduling anything, then one
            // per tear-down, as the worker makes them.
            let mut scheduled = 0;
            if !topology.stop_requested() {
                scheduled = 1;
                while topology.run_again() {
                    scheduled += 1;
                }
            }
            assert_eq!(scheduled, runs, "a topology asked for {runs} runs");
        }
    }

    /// A predicate that panics must not unwind out of the tear-down: that would
    /// skip `finish` and leave the waiter blocked for good. It is recorded as
    /// the run's failure instead, and it means "stop".
    #[test]
    fn a_panicking_predicate_stops_the_run_and_is_reported() {
        let topology = Topology::new(|| panic!("predicate"), || {});

        assert!(!topology.run_again(), "a failed predicate means stop");
        topology.finish();

        let payload = topology.join().expect("the panic was recorded");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"predicate"));
    }

    /// The estate word is upstream's, and the two things it owes the rest of
    /// the port are that either of the cancelling bits means "cancelled" and
    /// that a setter is told whether it was the first.
    /// [`Topology::record_panic`] leans on both: a panicking task sets
    /// `EXCEPTION`, and the first one to do it is the one whose failure the
    /// run reports.
    #[test]
    fn the_estate_word_is_either_bit_and_reports_the_first_setter() {
        let state = Cancellable::new();
        assert!(!state.cancelled());

        assert!(state.set(Cancellable::CANCELLED), "the first setter");
        assert!(state.cancelled());
        assert!(!state.set(Cancellable::CANCELLED), "the second setter");

        // The other bit on its own means cancelled too, which is what makes
        // the panic term a term of `cancelled()` rather than of `run_again`.
        let failed = Cancellable::new();
        assert!(failed.set(Cancellable::EXCEPTION));
        assert!(failed.cancelled());
        assert!(!failed.set(Cancellable::EXCEPTION));

        // And they are distinct bits, so neither setter answers for the other.
        let both = Cancellable::new();
        assert!(both.set(Cancellable::CANCELLED));
        assert!(both.set(Cancellable::EXCEPTION));

        // `CAUGHT` is the third, and the one that is not part of what
        // `cancelled` reads: it says the payload slot is taken, nothing more.
        let caught = Cancellable::new();
        assert!(caught.set(Cancellable::CAUGHT));
        assert!(!caught.cancelled());
        assert!(!caught.set(Cancellable::EXCEPTION | Cancellable::CAUGHT));
    }

    /// Cancelling is upstream's `Future::cancel`: `true` while the run is
    /// there to cancel, `false` once it has finished -- which is its
    /// `weak_ptr::lock()` failing -- and harmless twice over.
    #[test]
    fn cancelling_reports_false_only_once_the_run_has_finished() {
        let topology = once();
        assert!(!topology.cancelled());

        assert!(topology.cancel(), "a run in flight can be cancelled");
        assert!(topology.cancel(), "cancelling twice is still true");
        assert!(topology.cancelled());

        topology.finish();
        assert!(!topology.cancel(), "a finished run has nothing to cancel");
        // The bit stays set: a cancelled run that has finished is still a
        // cancelled run, which is what a `was_cancelled` query would read.
        assert!(topology.cancelled());
    }

    /// The cancellation half of upstream's
    /// `!tpg->cancelled() && !tpg->_predicate()`: a run told to go round again
    /// does not, once it is cancelled. The counting predicate is the one that
    /// shows the short-circuit, since a predicate that was never asked has not
    /// spent a round.
    #[test]
    fn a_cancelled_run_does_not_go_round_again() {
        let asked = Arc::new(AtomicUsize::new(0));
        let topology = {
            let asked = Arc::clone(&asked);
            Topology::new(
                move || {
                    asked.fetch_add(1, Ordering::Relaxed);
                    false
                },
                || {},
            )
        };

        assert!(topology.run_again(), "the predicate says go round again");
        assert_eq!(asked.load(Ordering::Relaxed), 1);

        assert!(topology.cancel());
        assert!(!topology.run_again(), "a cancelled run stops");
        assert_eq!(
            asked.load(Ordering::Relaxed),
            1,
            "the predicate was asked by a run that will not happen"
        );
    }

    /// The callback is upstream's `_on_finish`: called once, after the last
    /// run, and a panic out of it is reported rather than propagated.
    #[test]
    fn the_callback_is_called_once_and_its_panic_is_recorded() {
        let calls = Arc::new(AtomicUsize::new(0));
        let topology = {
            let calls = Arc::clone(&calls);
            Topology::new(
                || true,
                move || {
                    calls.fetch_add(1, Ordering::Relaxed);
                },
            )
        };

        topology.carry_out_callback();
        topology.carry_out_callback();
        assert_eq!(calls.load(Ordering::Relaxed), 1, "`FnOnce` means once");

        let panicking = Topology::new(|| true, || panic!("callback"));
        panicking.carry_out_callback();
        panicking.finish();
        let payload = panicking.join().expect("the panic was recorded");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"callback"));
    }

    /// `poll_join` is the non-blocking half of `join`: pending until the run
    /// finishes, and then the same payload `join` would have handed back.
    #[test]
    fn polling_a_run_leaves_a_waker_and_is_woken_by_finish() {
        let woken = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountingWaker(Arc::clone(&woken))));

        let topology = once();
        assert!(topology.poll_join(&waker).is_pending());
        // Polling again with the same waker keeps the one already stored.
        assert!(topology.poll_join(&waker).is_pending());
        assert_eq!(woken.load(Ordering::Relaxed), 0);

        topology.record_panic(Box::new("boom"));
        topology.finish();
        assert_eq!(woken.load(Ordering::Relaxed), 1, "the waker was not woken");

        let Poll::Ready(payload) = topology.poll_join(&waker) else {
            panic!("a finished run is ready");
        };
        let payload = payload.expect("a panic was recorded");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));

        // Taken, not cloned, exactly as `join` takes it.
        assert!(matches!(topology.poll_join(&waker), Poll::Ready(None)));
        assert_eq!(woken.load(Ordering::Relaxed), 1, "no second wake");
    }

    /// A waker that counts, in place of a runtime.
    struct CountingWaker(Arc<AtomicUsize>);

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A waiter that arrives before the run finishes has to be released by
    /// `finish`, which is the one piece of this file that a single-threaded
    /// test cannot reach.
    #[test]
    fn a_blocked_waiter_is_released_by_finish() {
        let topology = once();
        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| topology.join());
            topology.record_panic(Box::new(String::from("boom")));
            topology.finish();
            let payload = waiter.join().expect("the waiter panicked");
            let payload = payload.expect("a panic was recorded");
            assert_eq!(
                payload.downcast_ref::<String>().map(String::as_str),
                Some("boom")
            );
        });
    }
}

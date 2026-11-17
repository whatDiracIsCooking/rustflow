//! Translated from `Topology` in `taskflow/core/graph.hpp` and `_set_up_graph`
//! in `taskflow/core/executor.hpp` (Taskflow v4.1.0).
//!
//! A [`Topology`] is one run of one [`Taskflow`]: the counter that says how
//! much of the graph is still outstanding, and the slot a `wait` blocks on.
//! Upstream keeps it in a `std::shared_ptr` that both the `tf::Future` and
//! every queued `Node*` hold a share of; here the
//! [`RunHandle`](crate::RunHandle) owns it outright and its borrow is what
//! keeps it alive, which is the whole reason `run` hands back a handle with a
//! lifetime instead of a detachable future.
//!
//! # The one `unsafe` boundary in the executor
//!
//! A worker queue is `WorkerQueue<Job>` -- one type, built when the executor
//! is, with no lifetime to spare -- while the thing a job points at lives for
//! exactly one run. Erasing that lifetime is what [`Job`] is for, and it is
//! the only place in the crate where a reference is reconstructed from a raw
//! pointer. The argument that makes it sound is one sentence long, which is
//! why this is the boundary worth having: a job referencing a run exists only
//! between [`Topology::set_up`] and the moment the topology's join counter
//! hits zero, and [`Topology::finish`] -- the release of the thread that owns
//! the topology and the taskflow's borrow -- is the *last* thing the worker
//! that drove the counter to zero does.
//!
//! The topology is heap-allocated before the first job that names it is
//! built, and it has exactly two owners over its life: the stack frame of
//! `Executor::run`, up to the point where that frame hands the box to the
//! handle it is about to return, and the handle thereafter. Neither can go
//! away while a job is queued -- `run` has no fallible step between
//! scheduling and returning, and the handle blocks.
//!
//! Two things keep that boundary as small as it is. A job addresses its node
//! by [`Task`] index rather than by pointer, so the node comes out of the
//! graph through safe code; and the pointer to the taskflow lives in the job
//! rather than in the topology, so [`Topology`] itself is an ordinary `Send +
//! Sync` value with no raw pointer in it.

use std::any::Any;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::task::Waker;

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
    /// been dropped, which also means the taskflow is still borrowed by it.
    /// That holds for every job from the moment [`Topology::set_up`] counts it
    /// until the topology's join counter reaches zero, and a job exists only
    /// inside that window -- see the module docs.
    ///
    /// # Panics
    ///
    /// Panics if the job's task is not a live node of the taskflow's graph,
    /// which cannot happen for a job built by the scheduler: a run borrows the
    /// graph immutably, so no node can be erased while a job names it.
    pub(crate) unsafe fn parts<'j>(&self) -> (&'j Node, &'j Taskflow, &'j Topology) {
        // SAFETY: the caller promises the run is still in flight, which is
        // exactly the promise that both targets are still alive: the topology
        // is the `Box` that `Executor::run` builds and its `RunHandle` then
        // owns, and the taskflow is what that handle borrows. Neither target
        // is ever moved -- the box is allocated before the first job is built
        // and only the box itself, not its contents, is ever moved.
        let (taskflow, topology) = unsafe { (self.taskflow.as_ref(), self.topology.as_ref()) };
        (taskflow.graph().node(self.task), taskflow, topology)
    }
}

/// One run of one [`Taskflow`]: upstream's `tf::Topology`.
///
/// Three upstream members are not here, each for its own reason:
///
/// - `_promise` is a [`Completion`] behind a `Mutex`, because the payload a
///   panicking task hands back is not a value `std::promise` would carry;
/// - `_predicate` and `_on_finish` arrive with `run_n`/`run_until`, which is
///   the only thing that can make a topology run more than once;
/// - `_estate`, upstream's cancellation and exception bits, is not needed yet.
///   Cancellation is a later milestone, and "the first panic wins" is already
///   decided by the `Mutex` that [`Topology::record_panic`] takes, so a state
///   word would be a second answer to a question already answered.
pub(crate) struct Topology {
    /// How many nodes of this run are outstanding: upstream's
    /// `NodeBase::_join_counter` on the topology. Set to the number of source
    /// tasks by [`Topology::set_up`], raised by one for each successor that
    /// becomes ready and lowered by one as each node finishes, so it reaches
    /// zero exactly once -- when the last node of the run is done.
    join_counter: AtomicUsize,
    /// Everything a waiter needs, and the flag it waits on.
    completion: Mutex<Completion>,
    /// Signalled by [`Topology::finish`], under the lock.
    finished: Condvar,
}

/// The completion slot, in place of upstream's `std::promise<void>`.
///
/// Who gets woken is deliberately two fields rather than one: a thread
/// blocked in [`RunHandle::wait`](crate::RunHandle::wait) is woken through the
/// condvar, and a future polling a run is woken through the [`Waker`] it left
/// behind. Nothing leaves a waker yet -- the async run handle and
/// `run_n`/`run_until` are what will -- but the slot is the shape they need,
/// so they can fill it in without reopening this file.
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
    /// A run that has not started.
    pub(crate) fn new() -> Self {
        Self {
            join_counter: AtomicUsize::new(0),
            completion: Mutex::new(Completion {
                finished: false,
                panic: None,
                waker: None,
            }),
            finished: Condvar::new(),
        }
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

    /// Stores what a panicking task panicked with, keeping the first.
    ///
    /// Upstream keeps the first exception too, and for the same reason: with
    /// several tasks failing, the one the caller is told about should be the
    /// one that failed first, not the one that happened to finish unwinding
    /// last.
    pub(crate) fn record_panic(&self, payload: Box<dyn Any + Send>) {
        let mut completion = self.lock();
        if completion.panic.is_none() {
            completion.panic = Some(payload);
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

    /// The completion slot. A poisoned lock is taken rather than propagated,
    /// as the executor's other locks are: the panic that poisoned it is
    /// already recorded in the slot, and refusing to hand the slot back would
    /// turn a reported failure into a stuck `wait`.
    fn lock(&self) -> MutexGuard<'_, Completion> {
        self.completion
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl core::fmt::Debug for Topology {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let completion = self.lock();
        f.debug_struct("Topology")
            .field("outstanding", &self.join_counter.load(Ordering::Relaxed))
            .field("finished", &completion.finished)
            .field("panicked", &completion.panic.is_some())
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
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

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
        let topology = Topology::new();

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
        let topology = Topology::new();

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
        let topology = Topology::new();
        topology.set_up(&taskflow);

        let sources: Vec<Task> = topology.sources(&taskflow).map(|job| job.task).collect();
        assert_eq!(sources, vec![a]);
    }

    /// An empty graph has no sources, and so does a graph whose every node is
    /// in a cycle. Both answer zero, which is what tells `run` there is
    /// nothing to schedule.
    #[test]
    fn a_graph_with_no_source_sets_up_to_zero() {
        let topology = Topology::new();
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
        let topology = Topology::new();
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
        let topology = Topology::new();
        topology.record_panic(Box::new("first"));
        topology.record_panic(Box::new("second"));
        topology.finish();

        let payload = topology.join().expect("a panic was recorded");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"first"));
        // Taken, not cloned: the second waiter has nothing left to report.
        assert!(topology.join().is_none());
    }

    #[test]
    fn joining_a_finished_run_does_not_block() {
        let topology = Topology::new();
        topology.finish();
        assert!(topology.join().is_none());
        assert!(topology.join().is_none());
    }

    /// A waiter that arrives before the run finishes has to be released by
    /// `finish`, which is the one piece of this file that a single-threaded
    /// test cannot reach.
    #[test]
    fn a_blocked_waiter_is_released_by_finish() {
        let topology = Topology::new();
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

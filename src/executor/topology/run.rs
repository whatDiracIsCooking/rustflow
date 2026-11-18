//! Translated from `Topology` in `taskflow/core/graph.hpp` and `_set_up_graph`
//! in `taskflow/core/executor.hpp` (Taskflow v4.1.0).
//!
//! One run of one [`Taskflow`](crate::Taskflow): the counter that says how
//! much of the graph is still outstanding, the policy that decides whether to
//! run the graph again, the bits a cancel token may set, and the slot a `wait`
//! blocks on.
//!
//! # Design
//!
//! Two upstream members are not inline. `_promise` is a [`Completion`] behind
//! a `Mutex`, the payload a panicking task hands back not being a value a
//! `std::promise` would carry; and `_estate` is a [`Cancellable`] in an
//! allocation of its own, so that a [`Canceller`](crate::Canceller) can
//! outlive the lifetimes a [`RunHandle`](crate::RunHandle) carries without
//! being able to upgrade to a share of the run -- which would hold the
//! taskflow's storage alive for a stray `cancel`. A third, `_taskflow`, is a
//! share of that storage behind a lock, because it has to be released at a
//! particular moment rather than whenever the topology is dropped; that moment
//! and the lock order are one list in `ownership`'s `# Design` section. One
//! run is a sentinel: [`Topology::free_standing`] is what a free async task's
//! job names in place of upstream's null `_topology`, and `DESIGN.md` has why
//! that is a run rather than an `Option`.

use std::any::Any;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::task::{Poll, Waker};

use super::ownership::Storage;
use crate::executor::Shared;
use crate::graph::{Graph, Node};
use crate::task::TaskRef;
use crate::taskflow::Flow;

/// The part of a run a cancel token may touch: upstream's `NodeBase::_estate`,
/// in an allocation of its own for the reason the module docs give. It is also
/// where the port keeps upstream's `ESTATE` vocabulary, being the one estate
/// word with a name of its own; the other is a `NodeBase`'s, which reads the
/// same constants.
pub(crate) struct Cancellable {
    /// Upstream's `_estate`, holding the three bits a topology uses.
    pub(super) estate: AtomicU32,
    /// Whether the run is over, which is to say whether there is still anything to
    /// cancel. Upstream needs no equivalent, its token's `weak_ptr` being dead once
    /// the tear-down has popped the topology off the taskflow's queue; here the
    /// topology is shared with a handle that outlives the run, so the question
    /// needs its own answer. [`Topology::finish`] writes this before it releases a
    /// waiter, so a `cancel` after a `wait` always reports `false`.
    finished: AtomicBool,
}

impl Cancellable {
    /// Upstream's `ESTATE::CANCELLED`: this run has been cancelled.
    pub(crate) const CANCELLED: u32 = 0x4000_0000;
    /// Upstream's `ESTATE::EXCEPTION`: a task of this run failed, which to the
    /// scheduler means the same as `CANCELLED`.
    pub(crate) const EXCEPTION: u32 = 0x1000_0000;
    /// Upstream's `ESTATE::CAUGHT`: this run's one payload slot is taken, so a
    /// later failure has nowhere to go. Set together with `EXCEPTION` and read
    /// on its own, which is how upstream decides that a panic is the first
    /// one.
    pub(crate) const CAUGHT: u32 = 0x2000_0000;
    /// Upstream's `ESTATE::EXPLICITLY_ANCHORED`: somebody is waiting for the graph
    /// scheduled under this base, so a failure inside that graph is theirs to deal
    /// with and the panic walk stops there. It is the one of the four never set on
    /// a *run*, a topology being treated as anchored by `Job::record_panic`
    /// without a bit of its own; what carries it is a `NodeBase`.
    pub(crate) const EXPLICITLY_ANCHORED: u32 = 0x8000_0000;

    /// A run that is neither cancelled nor finished.
    pub(super) fn new() -> Self {
        Self {
            estate: AtomicU32::new(0),
            finished: AtomicBool::new(false),
        }
    }

    /// Sets `bits` in the estate word, reporting whether this call was the
    /// first to set them: upstream's `_estate.fetch_or`, plus the answer
    /// [`Topology::record_panic`] needs.
    pub(crate) fn set(&self, bits: u32) -> bool {
        // Relaxed, as every upstream read and write of `_estate` is: the word
        // races with the scheduler by construction -- a task already running
        // finishes whether or not it sees the bit -- and what carries a run's
        // *results* between threads is the join counter and the completion
        // lock, not this.
        self.estate.fetch_or(bits, Ordering::Relaxed) & bits == 0
    }

    /// Claims the one payload slot of this run, reporting whether this caller
    /// was the first to do so: the run's half of `NodeBase::claim_panic`, and
    /// the same atomic. `EXCEPTION` beside `CAUGHT` so that claiming the slot
    /// is also what cancels the rest of the run, and `CAUGHT` masked on its
    /// own because that is the bit which says the slot is taken.
    pub(crate) fn claim_panic(&self) -> bool {
        let flag = Self::EXCEPTION | Self::CAUGHT;
        self.estate.fetch_or(flag, Ordering::Relaxed) & Self::CAUGHT == 0
    }

    /// Whether this run is cancelled: upstream's `Topology::cancelled`, which
    /// is either bit of the pair rather than `CANCELLED` alone.
    pub(crate) fn cancelled(&self) -> bool {
        self.estate.load(Ordering::Relaxed) & (Self::CANCELLED | Self::EXCEPTION) != 0
    }

    /// Cancels the run, reporting `false` if it had already finished:
    /// upstream's `Future::cancel`, whose `false` is its `weak_ptr::lock()`
    /// failing.
    ///
    /// Cancelling twice reports `true` twice, as upstream's does: the answer
    /// is about the run, not about this caller.
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

/// One run of one [`Taskflow`](crate::Taskflow): upstream's `tf::Topology`.
pub(crate) struct Topology {
    /// How many nodes of this run are outstanding: upstream's
    /// `NodeBase::_join_counter` on the topology, raised by the source count
    /// when the graph is scheduled and by one per successor that becomes ready,
    /// lowered as each node finishes, so it reaches zero exactly once. Only the
    /// jobs whose parent is the run count here; a nested graph counts down the
    /// node it hangs off, which is itself one of these.
    pub(super) join_counter: AtomicUsize,
    /// What decides whether the graph runs again, and what to call when it
    /// does not: upstream's `_predicate` and `_on_finish`.
    policy: Mutex<RunPolicy>,
    /// Everything a waiter needs, and the flag it waits on.
    completion: Mutex<Completion>,
    /// Signalled by [`Topology::finish`], under the lock.
    finished: Condvar,
    /// The cancellation and exception bits of this run, upstream's `_estate`,
    /// shared with every [`Canceller`](crate::Canceller) handed out for it.
    pub(super) estate: Arc<Cancellable>,
    /// This run's share of its hold on the taskflow's storage -- upstream's
    /// `Topology::_taskflow` -- until the tear-down takes it back out. `None`
    /// means there is no graph to run, which is [`Topology::nothing_to_run`]:
    /// never set up, never queued, never torn down. The lock is the first entry
    /// in `ownership`'s list of lock orderings.
    storage: Mutex<Option<Arc<Storage>>>,
}

/// Upstream's `Topology::_predicate` and `_on_finish`, which is what makes a
/// topology able to run its graph more than once.
///
/// Behind a `Mutex` because both are called on whichever worker happens to
/// finish a run, and the predicate is `FnMut` -- upstream's is a mutable
/// lambda too, `run_n`'s being a counter -- and a lock rather than an atomic
/// because the values behind it are arbitrary closures. It is uncontended by
/// construction, the only thread that evaluates a run's policy being the one
/// that drove its join counter to zero. Both are `'static`, as
/// [`Work`](crate::Work) is: the topology is `Arc`-shared with the taskflow's
/// run queue, which has no lifetime to spare.
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
/// Who gets woken is deliberately two fields rather than one: a thread blocked
/// in [`RunHandle::wait`](crate::RunHandle::wait) is woken through the condvar,
/// and a future polling a run through the [`Waker`] left by
/// [`Topology::poll_join`]. Keeping the slots separate is what lets the
/// blocking path stay a condvar rather than a hand-rolled futex.
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
    /// A run of `flow`'s graph that has not started, over the policy `run_until`
    /// was given: `predicate` decides after each run whether to run the graph
    /// again, and `on_finish` is called once the answer is no. Upstream's
    /// `Topology(Taskflow&, P&&, C&&)`. The hold this builds releases no waiter --
    /// [`Storage::ungated`] has which runs want that -- where a run a caller gets a
    /// [`RunHandle`](crate::RunHandle) on goes through [`Topology::for_run`].
    pub(crate) fn new<P, C>(predicate: P, on_finish: C, flow: Arc<Flow>) -> Self
    where
        P: FnMut() -> bool + Send + 'static,
        C: FnOnce() + Send + 'static,
    {
        Self::over(predicate, on_finish, Some(Storage::ungated(flow)))
    }

    /// The same run, for a caller that will hand a
    /// [`RunHandle`](crate::RunHandle) on it out: its hold on the storage names
    /// the run back, so that the last share of that hold releases whoever
    /// waits. Upstream has one constructor because it has nothing to name back
    /// -- the `shared_ptr<Topology>` its `_fetch_enqueue` makes is the run's
    /// only owner -- and `Arc::new_cyclic` is what lets the hold be built with
    /// the run's own `Weak` in it. `executor` is whose run count the caller is
    /// about to count this run in; the decrement is the last share's.
    pub(crate) fn for_run<P, C>(
        predicate: P,
        on_finish: C,
        flow: Arc<Flow>,
        executor: Arc<Shared>,
    ) -> Arc<Self>
    where
        P: FnMut() -> bool + Send + 'static,
        C: FnOnce() + Send + 'static,
    {
        Arc::new_cyclic(|run| {
            Self::over(
                predicate,
                on_finish,
                Some(Storage::gating(flow, run, executor)),
            )
        })
    }

    /// The one run every free-standing async node claims to be part of: a
    /// sentinel that is never cancelled, never finished, never an anchor and
    /// holds no taskflow. One per process rather than per
    /// [`Executor`](crate::Executor), which is the stronger form of "shared":
    /// it is immutable in every way a run can be read, so two executors have
    /// nothing to disagree about.
    pub(crate) fn free_standing() -> &'static Arc<Self> {
        /// The sentinel itself, built on the first async task of the process
        /// and never dropped. One allocation, which is what keeps an async
        /// task at upstream's one-allocation cost.
        static FREE_STANDING: OnceLock<Arc<Topology>> = OnceLock::new();

        FREE_STANDING.get_or_init(|| {
            let topology = Self::over(|| true, || {}, None);
            // The phantom job that makes "never finishes" structural rather
            // than a rule somebody has to keep. Every job raises this counter
            // before it is queued and lowers it as it finishes -- the
            // scheduler's rule for every topology, not this one's -- so the
            // counter is one plus the number of free nodes in flight: it can
            // be driven down to one but never to zero, `job_finished` never
            // reports a last job, and the tear-down, which would evaluate a
            // run policy and release a waiter that does not exist, is
            // unreachable.
            topology.join_counter.store(1, Ordering::Relaxed);
            Arc::new(topology)
        })
    }

    /// Whether this run is the sentinel [`Topology::free_standing`] rather
    /// than a run somebody started: upstream's `node->_topology == nullptr`,
    /// which it tests in `_tear_down_async` and in `_process_exception`. The
    /// identity of the one allocation stands in for the null, and `Job::free`
    /// is the only way onto a job, so this is as exact as upstream's test.
    pub(crate) fn is_free_standing(&self) -> bool {
        std::ptr::eq(self, &**Self::free_standing())
    }

    /// A run that is over before it begins and has no graph at all: what
    /// [`RunHandle::finished`](crate::RunHandle) is built on, where upstream
    /// hands back a ready `std::future` fulfilled on the spot. There is one
    /// here so that every handle has the same shape; it is finished before it
    /// is handed out and holds no share of any taskflow.
    pub(crate) fn nothing_to_run() -> Self {
        let topology = Self::over(|| true, || {}, None);
        topology.finish();
        topology
    }

    /// The one constructor, over a run that may or may not have a graph.
    fn over<P, C>(predicate: P, on_finish: C, storage: Option<Arc<Storage>>) -> Self
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
            storage: Mutex::new(storage),
        }
    }

    /// This run's share of its hold on the taskflow's storage, or `None` once
    /// the tear-down has released it: what the tear-down reaches the graph and
    /// the run queue through -- the job that got it there having released its
    /// own share -- and what `Shared::set_up_topology` schedules the graph
    /// under, so that every job of the run shares one hold.
    pub(crate) fn storage(&self) -> Option<Arc<Storage>> {
        self.lock_storage().clone()
    }

    /// Hands this run's share of its hold on the taskflow's storage *out*,
    /// leaving the run with none: [`Topology::release_flow`] without the
    /// release, for the one caller that has something to do with the share
    /// before the run is over -- a [`corun`](crate::Executor::corun) frame,
    /// which schedules its graph through this hold and keeps it until no job of
    /// that graph holds a copy. Nothing is dropped here, so the one lock this
    /// takes touches nothing of the run's; the run is left as the tear-down
    /// leaves it, so a later `release_flow` releases the waiter itself.
    pub(crate) fn take_storage(&self) -> Option<Arc<Storage>> {
        self.lock_storage().take()
    }

    /// Releases this run's share of its hold on the taskflow's storage, and
    /// with it the thread waiting on the run. Called by the tear-down of the
    /// *final* run of a graph, as its last act. It is one act rather than two:
    /// the share goes, and if it was the last then
    /// [`Drop for Storage`](Storage) releases the waiter on the spot -- and if
    /// not, the worker still holding a copy of a preempted node's job does so a
    /// few instructions later. [`Topology::finish`] is called directly for a
    /// hold that names no run, which also makes this idempotent.
    pub(crate) fn release_flow(&self) {
        // Taken out from under the lock and dropped outside it: the drop runs
        // a destructor that takes this topology's completion lock, and the
        // two are never held at once.
        let released = self.lock_storage().take();

        let releases_the_waiter = released
            .as_ref()
            .is_some_and(|storage| storage.gates_completion());
        drop(released);

        if !releases_the_waiter {
            self.finish();
        }
    }

    /// The share slot. Poisoning is taken rather than propagated, as the rest
    /// of this type's locks are: the slot holds one `Option` and nothing under
    /// the lock can panic.
    fn lock_storage(&self) -> MutexGuard<'_, Option<Arc<Storage>>> {
        self.storage.lock().unwrap_or_else(PoisonError::into_inner)
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

    /// Accounts for `jobs` more nodes of this run becoming ready, before any of
    /// them is queued: upstream's `node->_parent->_join_counter.fetch_add(..)`,
    /// where a top-level node's parent *is* the topology.
    /// `Parent::count_queued` is the only caller.
    pub(super) fn jobs_queued(&self, jobs: usize) {
        self.join_counter.fetch_add(jobs, Ordering::Relaxed);
    }

    /// Accounts for one node of this run having finished, and reports whether
    /// it was the last: upstream's `_tear_down_nonasync`, down to the
    /// ordering. The release half publishes everything the finished node wrote
    /// to whoever reads a zero, and the acquire half makes the zero reliable.
    pub(crate) fn job_finished(&self) -> bool {
        self.join_counter.fetch_sub(1, Ordering::AcqRel) == 1
    }

    /// Whether the run that has just finished should run the graph again:
    /// upstream's `!tpg->cancelled() && !tpg->_predicate()`, now both halves of
    /// it. The `&&` short-circuits, as upstream's does, so a cancelled run does
    /// not ask its predicate and a `run_n` counter is not spent by a round that
    /// will never happen. A panicking task stops such a run through
    /// [`Topology::cancelled`] rather than a term of its own.
    pub(crate) fn run_again(&self) -> bool {
        !self.cancelled() && !self.stop_requested()
    }

    /// Evaluates the run predicate, reporting `true` for "stop".
    ///
    /// A panic out of the predicate is caught and recorded as the run's
    /// failure, and answers "stop". Upstream lets the exception propagate out
    /// of `_tear_down_topology`; here that would unwind past
    /// [`Topology::finish`] and leave the waiter blocked for good.
    pub(super) fn stop_requested(&self) -> bool {
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

    /// Calls the on-finish callback, once: upstream's `tpg->_on_finish()`.
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

    /// Records that this run failed, keeping the first payload and cancelling
    /// whatever the run had not started: the delivery half of upstream's
    /// `_process_exception` with the run as the anchor. This is where a failing
    /// task of a top-level graph ends up, through the walk in
    /// `Job::record_panic`; the two other callers have no node and no chain to
    /// walk -- a run policy's predicate and its on-finish callback.
    ///
    /// Two things come out of the one atomic. `EXCEPTION` is half of
    /// [`Topology::cancelled`], so setting it is the whole of "the rest of the
    /// run is cancelled". And `CAUGHT` says the payload slot is taken, so the
    /// first panic is the one reported, as upstream keeps only the first
    /// `exception_ptr`.
    pub(crate) fn record_panic(&self, payload: Box<dyn Any + Send>) {
        if self.claim_panic() {
            self.keep_panic(payload);
        }
    }

    /// Claims this run's one payload slot, reporting whether this caller was
    /// the first: the run's half of `NodeBase::claim_panic`. The atomic is the
    /// arbiter rather than the lock, so a later panic of a run that already has
    /// one never touches the completion slot -- which is what keeps a thousand
    /// simultaneously failing tasks off it.
    pub(crate) fn claim_panic(&self) -> bool {
        self.estate.claim_panic()
    }

    /// Puts `payload` in the slot this caller has just claimed, where a `wait`
    /// will find it.
    pub(crate) fn keep_panic(&self, payload: Box<dyn Any + Send>) {
        self.lock().panic = Some(payload);
    }

    /// Marks this run finished and releases whoever is waiting for it: upstream's
    /// `_carry_out_promise`.
    ///
    /// **This is the last thing the caller may do with the run or with the graph.**
    /// The thread it releases is the one that can edit the taskflow again the moment
    /// it wakes, so a touch of the graph after this point would be a share of the
    /// storage outstanding during that edit. The ordinary way here is through
    /// [`Drop for Storage`](Storage), which releases the storage on its way; the two
    /// callers that reach it directly are a run whose hold releases nobody and
    /// [`Topology::nothing_to_run`], which has no storage and no run. The
    /// notification is sent *under* the lock, a waiter not being able to return
    /// from [`Condvar::wait`](std::sync::Condvar::wait) without the mutex.
    ///
    /// In debug builds the wake rule in `ownership`'s docs is asserted here: a
    /// run whose hold *gates* its completion is never finished while the
    /// topology still holds it, the waiter this wakes being able to find the
    /// storage held. Every path here has emptied the slot first, so the whole
    /// suite checks an ordering the stress cases only sample. An ungated hold
    /// is exempt: a hand-built test topology may be finished with one.
    pub(crate) fn finish(&self) {
        debug_assert!(
            self.lock_storage()
                .as_ref()
                .is_none_or(|storage| !storage.gates_completion()),
            "a run is finished only after its hold on the storage has left the topology: \
             the thread this wakes may edit the taskflow at once (#138, #141)"
        );

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

    /// Blocks until this run has finished and returns what its first panicking
    /// task panicked with, if any.
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

    /// Reports whether this run has finished, leaving `waker` to be woken when it
    /// does if it has not, and takes the panic payload when it has: the
    /// non-blocking half of [`Topology::join`]. Registering the waker and reading
    /// the flag happen under one lock, which is what stops a run finishing between
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

/// Prepares every node of `graph` for a run and returns how many of them are
/// source tasks: upstream's `Executor::_set_up_graph`, applied to any graph,
/// which is what makes a nested run possible at all. Three departures from
/// upstream, all three falling out of what this port's nodes are:
///
/// - **No topology and no parent are written.** Upstream stores both on every
///   node here; they travel with the `Job` instead, so this needs neither and
///   is a free function rather than a method on [`Topology`].
/// - **The sources are not moved to the front.** Upstream partitions its node
///   vector so that the sources are a prefix it can bulk schedule; an arena
///   cannot, a shuffle re-pointing every live [`Task`](crate::Task) at another
///   node. So they are found twice, here and in `Job::sources`.
/// - **The state word is written through the mask.** Upstream assigns
///   `NSTATE::NONE` and accumulates the count into the word with `+=`, masking
///   it back out when it arms the join counter; here the count is checked
///   against [`Node::STRONG_DEPENDENCIES_MASK`] and stored, which rules out a
///   count being mistaken for a state bit.
///
/// Clearing the state bits is upstream's `node->_nstate = NSTATE::NONE`, and it
/// is what makes a re-run start clean: a node left `PREEMPTED` or
/// `JOINED_SUBFLOW` by a cancelled run would have the next run treat its first
/// entry as a resumption and skip its body. The nodes set up here are the ones
/// about to start, and a parent whose nested graph is being set up is never a
/// node of that graph, so the one way to reach this with live state in the word
/// is a graph the run is already inside of -- which `executor::module` refuses.
/// Emptying the payload slot is upstream's `node->_exception_ptr = nullptr`, in
/// the same loop and for the same reason.
///
/// # Panics
///
/// Panics if some task has more strong dependencies than the count field of the
/// state word can hold, which needs a quarter of a billion duplicate edges into
/// one node.
pub(crate) fn set_up_graph(graph: &Graph) -> usize {
    let mut num_sources = 0;

    for (task, node) in graph.iter() {
        let strong = TaskRef::new(graph, task).num_strong_dependencies();
        let counted = u32::try_from(strong)
            .ok()
            .filter(|count| count & !Node::STRONG_DEPENDENCIES_MASK == 0)
            .expect("a task has fewer strong dependencies than the state word can count");

        node.nstate().store(counted, Ordering::Relaxed);
        node.join_counter().store(strong, Ordering::Relaxed);

        // Upstream's `node->_exception_ptr = nullptr`: a payload left on a
        // node by a failure nobody came to collect must not be reported by
        // the next run of the same graph. The `swap` is what keeps a mutex
        // off this path for the runs that did not fail -- a payload is only
        // ever put in a slot by a claimant that set `CAUGHT` in the same
        // word, so a word without that bit cannot have one under it.
        if node.estate().swap(0, Ordering::Relaxed) & Cancellable::CAUGHT != 0 {
            node.base().clear_panic();
        }

        // A source is a node with no predecessors, which is not the same test
        // as "no strong dependencies" and is upstream's. The two part company
        // once conditioners exist: a task reached only by a conditioner has no
        // strong dependency, but it is still not a place a run may start.
        if node.num_predecessors() == 0 {
            num_sources += 1;
        }
    }

    num_sources
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

    // A run is shared with every job of it, and with the run handle a caller
    // may move to another thread.
    assert_send_sync::<Topology>();
    // A `Canceller` is a `Weak<Cancellable>` and has to be `Send + Sync +
    // 'static`, which it is exactly when this is `Send + Sync`: it holds
    // nothing but atomics, and nothing with a lifetime.
    assert_send_sync::<Cancellable>();
};

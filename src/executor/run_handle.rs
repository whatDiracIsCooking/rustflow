//! Translated from `taskflow/core/executor.hpp` (Taskflow v4.1.0): the `run`,
//! `run_n` and `run_until` overload set, `_set_up_topology`,
//! `_tear_down_topology`, `_increment_topology` and `wait_for_all`.
//!
//! # `RunHandle` where upstream has `tf::Future`
//!
//! Upstream's `run` hands back a `tf::Future<void>` that shares ownership of
//! the topology with every queued node, and documents a rule it cannot
//! enforce: *do not mutate a taskflow while it is running*. Break it and the
//! scheduler walks edges the user is editing.
//!
//! Here the rule is the type system's. [`Executor::run`] borrows the executor
//! and the taskflow for the same lifetime and puts that lifetime on the
//! handle, so a taskflow cannot be edited or dropped -- nor the executor
//! dropped -- while a handle naming them is alive. Running it *again* is
//! allowed, as it is upstream, and is what the queue on [`Taskflow`] is for:
//! two runs of one taskflow take its graph in turn rather than at once.
//!
//! That leaves one hole, the one `std::thread::scope` has: a handle the caller
//! simply forgets about would end the borrow while the run is still going. So
//! [`Drop`] blocks, exactly as a scope's does, and the borrow becomes
//! airtight.
//!
//! Two things follow from that, and both are deliberate:
//!
//! - There is no detached run and no `std::future`-style transfer of the
//!   result to somewhere else in the program. A topology outliving its handle
//!   is the one thing the design does not allow.
//! - [`Executor::wait_for_all`] cannot be the thing that makes dropping an
//!   executor safe, because it is already impossible to drop an executor with
//!   a run in flight. It is here because upstream has it and because the
//!   async run handle will need the count it reads.
//!
//! # The `run` family
//!
//! Upstream spells twelve overloads of three names: `run`, `run_n` and
//! `run_until`, each over a borrowed or a moved-in taskflow and each with or
//! without an on-finish callback. Rust has no overloading, so the twelve are
//! twelve methods here, named after what they vary: `_n` and `_until` for the
//! repeat count and the predicate, `_with` for the callback, and `run_owned`
//! for the half of the set that takes the taskflow by value. Each of them is
//! two lines over [`Executor::run_until_with`] or
//! [`Executor::run_owned_until_with`], which are the two that do the work --
//! the same shape as upstream, whose ten short overloads all forward to the
//! two long ones.

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, PoisonError, Weak};
use std::task::{Context, Poll};

use crate::executor::topology::{Cancellable, Topology};
use crate::executor::worker::WorkerThread;
use crate::executor::{Executor, Shared};
use crate::taskflow::Taskflow;

/// Why a run did not finish cleanly.
///
/// One variant so far, and the enum is `#[non_exhaustive]` because later
/// milestones have more ways for a run to fail. Cancellation is deliberately
/// not one of them: a cancelled run completes *normally*, so
/// [`RunHandle::wait`] reports `Ok(())` for it, exactly as upstream's
/// `fu.get()` does not throw after a `fu.cancel()`.
#[non_exhaustive]
pub enum RunError {
    /// A task panicked, carrying what it panicked with.
    ///
    /// This is the port's reading of upstream's `std::exception_ptr`: a panic
    /// escaping a task body is caught where upstream catches an exception,
    /// stored in the topology, and surfaced here. The payload is whatever the
    /// task panicked with, so a caller that wants upstream's behaviour --
    /// the failure continuing to propagate on *this* thread -- can hand it to
    /// [`std::panic::resume_unwind`].
    ///
    /// Only the first panicking task of a run is reported, and the rest of the
    /// run still runs: a panic that cancels the remainder of its own run is
    /// issue #32, which sets the `EXCEPTION` bit of the run's estate word that
    /// [`RunHandle::cancel`] sets `CANCELLED` in.
    Panicked(Box<dyn Any + Send>),
}

impl RunError {
    /// The panic message, when the payload is one of the two types a
    /// `panic!` produces.
    fn message(&self) -> Option<&str> {
        let Self::Panicked(payload) = self;
        payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
    }
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.message() {
            Some(message) => write!(f, "a task panicked: {message}"),
            None => f.write_str("a task panicked"),
        }
    }
}

impl fmt::Debug for RunError {
    /// Hand-written because the payload is a `Box<dyn Any + Send>`, which has
    /// no `Debug` -- and because this is what `unwrap` prints.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Panicked({self})")
    }
}

impl std::error::Error for RunError {}

/// A run of a taskflow in progress, and the right to wait for it.
///
/// Returned by [`Executor::run`]. Upstream's `tf::Future<void>`, with the
/// differences the module docs set out: the handle borrows the executor and
/// the taskflow for as long as it lives, and dropping it blocks until the run
/// is over.
///
/// ```
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let executor = Executor::new(4);
///
/// let mut taskflow = Taskflow::new();
/// let (a, b) = taskflow.emplace_n((|| println!("a"), || println!("b")));
/// taskflow.task(a).precede([b]);
///
/// executor.run(&taskflow).wait().expect("no task panicked");
///
/// // The borrow is over, so the taskflow can be edited and run again.
/// taskflow.emplace(|| println!("c"));
/// executor.run(&taskflow).wait().expect("no task panicked");
/// ```
pub struct RunHandle<'a> {
    /// The run. Heap-allocated because every queued job holds its address, so
    /// it has to stay put while this handle moves -- out of [`Executor::run`],
    /// for one -- and shared because the taskflow's run queue holds the other
    /// share until the tear-down pops it.
    topology: Arc<Topology>,
    /// The taskflow, for a run that was handed one by value.
    ///
    /// [`Executor::run_owned`] is upstream's `run(Taskflow&&)`: the run owns
    /// the taskflow and nothing outside it can name it, so it is dropped here
    /// -- after the blocking [`Drop`] below has seen the last run finish, since
    /// a field is dropped after the body of its owner's `drop`.
    taskflow: Option<Box<Taskflow>>,
    /// The borrows that make "do not mutate a running taskflow" a compile
    /// error. A marker rather than two reference fields because nothing reads
    /// either one: their whole job is to be held.
    borrow: PhantomData<(&'a Executor, &'a Taskflow)>,
}

/// The right to cancel a run, with no lifetime attached.
///
/// Handed out by [`RunHandle::canceller`], and `Clone + Send + Sync +
/// 'static`, so it can be captured by a task body, stored in a struct or sent
/// to another thread.
///
/// Upstream needs no such type: its `tf::Future` *is* the cancel token, and
/// its test suite cancels a run from a task of another taskflow by capturing
/// the future by reference. A task body here is `'static` -- see
/// [`Work`](crate::Work) -- so it cannot capture a [`RunHandle`], which
/// borrows the executor and the taskflow the run is of. This is the part of
/// the handle that *can* be captured.
///
/// See [`RunHandle::cancel`] for what cancelling does and does not promise,
/// and the [`executor`](crate::executor) module docs for the whole of it.
#[derive(Clone)]
pub struct Canceller {
    /// The run's cancellation state, or what is left of it.
    ///
    /// `Weak`, because a token may outlive the run's topology: the topology is
    /// freed when the [`RunHandle`] that owns it is dropped, and nothing stops
    /// a token outliving that. Upstream's `std::weak_ptr<Topology>`, and a
    /// failed upgrade here is its failed `lock()`.
    state: Weak<Cancellable>,
}

impl Canceller {
    /// Cancels the run, reporting `false` if it has already finished.
    ///
    /// Upstream's `tf::Future::cancel` through a `weak_ptr` whose referent may
    /// be gone, which is the one difference from [`RunHandle::cancel`]: this
    /// reports `false` both for a run that has finished and for one whose
    /// handle has since been dropped. Everything else -- non-preemptive,
    /// harmless twice, and the run still completing normally -- is the same,
    /// and [`RunHandle::cancel`] says it.
    pub fn cancel(&self) -> bool {
        self.state
            .upgrade()
            .is_some_and(|state| Cancellable::cancel(&state))
    }
}

impl fmt::Debug for Canceller {
    /// Hand-written because `Weak`'s own prints `(Weak)`, and what is worth
    /// seeing is whether there is still a run on the other end of it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Canceller")
            .field("live", &(self.state.strong_count() > 0))
            .finish()
    }
}

impl<'a> RunHandle<'a> {
    /// Cancels this run, reporting `false` if it has already finished.
    ///
    /// Upstream's `tf::Future::cancel`, promise for promise: the executor
    /// stops scheduling tasks of this run onwards, and tasks already running
    /// are left to finish, cancellation being non-preemptive. So a cancelled
    /// run is one that stops *early*, by however much the pool had not got to
    /// yet, and "none of it had started" and "all of it ran" are both possible
    /// outcomes of a cancellation that raced the run.
    ///
    /// The run then completes normally. [`RunHandle::wait`] returns `Ok(())`,
    /// as upstream's `fu.get()` does not throw; the on-finish callback still
    /// runs; and a `run_n` or `run_until` run does not go round again however
    /// many rounds its predicate had left. A run that was cancelled is
    /// therefore indistinguishable afterwards from one whose tasks did
    /// nothing, which is upstream's behaviour and the decision issue #29 took;
    /// a `was_cancelled` query is a follow-up if anyone needs one.
    ///
    /// Cancelling twice is harmless and reports `true` twice: the answer is
    /// about the run, not about this caller. The `false` is upstream's
    /// `weak_ptr::lock()` failing, and it means the run was over before the
    /// call -- nothing was cancelled because there was nothing left to cancel.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let executor = Executor::new(4);
    /// let mut taskflow = Taskflow::new();
    /// for _ in 0..1000 {
    ///     taskflow.emplace(|| std::thread::sleep(std::time::Duration::from_millis(1)));
    /// }
    ///
    /// let run = executor.run(&taskflow);
    /// assert!(run.cancel(), "the run had not finished");
    ///
    /// // A cancelled run completes normally; what it does not do is finish
    /// // the work it had not started.
    /// run.wait().expect("no task panicked");
    /// ```
    pub fn cancel(&self) -> bool {
        self.topology.cancel()
    }

    /// A token that cancels this run from anywhere, including from a task
    /// body.
    ///
    /// The lifetime-free half of [`RunHandle::cancel`]; see [`Canceller`] for
    /// why it exists. Any number may be handed out, and each of them cancels
    /// the one run this handle is for.
    #[must_use]
    pub fn canceller(&self) -> Canceller {
        Canceller {
            state: Arc::downgrade(self.topology.cancellable()),
        }
    }

    /// Blocks until the run finishes, and reports whether a task panicked.
    ///
    /// Upstream's `tf::Future::get`, which rethrows the stored exception on
    /// the calling thread; a `Result` says the same thing without unwinding
    /// somebody else's stack for them. See [`RunError::Panicked`] for how to
    /// get upstream's behaviour back.
    ///
    /// When this returns, the run is not merely finished but fully accounted
    /// for: [`Executor::num_topologies`] no longer counts it, so a
    /// [`Executor::wait_for_all`] that follows cannot block on it. Dropping
    /// the handle without waiting gives the same guarantee, for the same
    /// reason. Upstream's `tf::Future` does not -- see
    /// `Shared::tear_down_topology` for the ordering that buys it.
    ///
    /// Waiting from inside a task body works, but it blocks the worker running
    /// that task, so the run being waited for needs another worker to make
    /// progress on. `corun`, which lends the blocked worker to the inner run
    /// instead, is the tool for that and is a later milestone.
    pub fn wait(self) -> Result<(), RunError> {
        outcome(self.topology.join())
    }

    /// The handle for a run that is over before it began: an empty or sourceless
    /// taskflow, or a predicate that was already true.
    ///
    /// Upstream returns a ready `std::future` from a promise it fulfils on the
    /// spot, for the reason its comment gives -- the caller gets a handle that
    /// behaves like every other one. `taskflow` is `Some` only for the
    /// `run_owned` family, which owns what it was handed even when it runs
    /// nothing with it.
    fn finished(taskflow: Option<Box<Taskflow>>) -> Self {
        // The policy is never consulted: the topology is finished before it is
        // ever handed out, so nothing can ask it to run again, and the
        // callback has already been called by the run method itself.
        let topology = Topology::new(|| true, || {});
        topology.finish();

        Self {
            topology: Arc::new(topology),
            taskflow,
            borrow: PhantomData,
        }
    }
}

/// Reads a completion slot's payload as a run result.
fn outcome(panic: Option<Box<dyn Any + Send>>) -> Result<(), RunError> {
    match panic {
        Some(payload) => Err(RunError::Panicked(payload)),
        None => Ok(()),
    }
}

impl Drop for RunHandle<'_> {
    /// Blocks until the run finishes, as [`std::thread::scope`] does.
    ///
    /// This is what makes the handle's borrow mean something: a run can never
    /// outlive it, so a queued job can never outlive what it points at. After
    /// [`RunHandle::wait`] it finds the run already over and returns at once.
    fn drop(&mut self) {
        // A panic payload nobody waited for is dropped here rather than
        // re-raised: this `Drop` may itself be running during an unwind, and a
        // panic from there aborts the process. Upstream's `Future` likewise
        // lets an unobserved exception go.
        drop(self.topology.join());
    }
}

/// A run can be awaited instead of waited for, on any runtime and without one:
/// the completion slot holds the [`Waker`](std::task::Waker) of whoever is
/// polling, and the worker that finishes the run wakes it where it would
/// otherwise signal the condvar. Nothing here spawns, blocks or sleeps, so
/// there is no executor to agree with and no feature flag to turn on.
///
/// Three things are worth knowing before awaiting one:
///
/// - Polling does not consume the handle, so the handle is still there when the
///   future is ready -- and dropping it then returns at once, the run being
///   over. A future polled again after it was ready reports success: the panic
///   payload, like [`RunHandle::wait`]'s, is handed over once.
/// - Dropping the handle *before* the run is over blocks the thread that drops
///   it, which on an async runtime means blocking a runtime thread. A cancelled
///   task holding a `RunHandle` therefore waits out its run.
/// - Neither the blocking nor the awaiting path can do anything about a handle
///   that is leaked rather than dropped: [`std::mem::forget`] skips the `Drop`
///   that the borrow leans on, and what to do about that is the open question
///   of issue #40 rather than something this impl settles.
///
/// ```
/// use std::future::Future;
/// use std::pin::pin;
/// use std::sync::Arc;
/// use std::task::{Context, Poll, Wake, Waker};
///
/// use rustflow::Executor;
/// use rustflow::prelude::*;
///
/// /// The whole runtime this needs: unpark the thread that is waiting.
/// struct Unpark(std::thread::Thread);
///
/// impl Wake for Unpark {
///     fn wake(self: Arc<Self>) {
///         self.0.unpark();
///     }
/// }
///
/// let executor = Executor::new(2);
/// let mut taskflow = Taskflow::new();
/// let (a, b) = taskflow.emplace_n((|| {}, || {}));
/// taskflow.task(a).precede([b]);
///
/// let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
/// let mut context = Context::from_waker(&waker);
/// let mut run = pin!(executor.run_n(&taskflow, 3));
///
/// let result = loop {
///     if let Poll::Ready(result) = run.as_mut().poll(&mut context) {
///         break result;
///     }
///     std::thread::park();
/// };
/// result.expect("no task panicked");
/// ```
impl Future for RunHandle<'_> {
    type Output = Result<(), RunError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.topology.poll_join(context.waker()).map(outcome)
    }
}

impl fmt::Debug for RunHandle<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunHandle")
            .field("topology", &self.topology)
            .field("owns_taskflow", &self.taskflow.is_some())
            .finish()
    }
}

impl Executor {
    /// Runs `taskflow` once and returns a handle to wait on.
    ///
    /// Upstream's `run`, which is `run_n(taskflow, 1)` there and here.
    ///
    /// Several runs of one taskflow may be in the air at once: the second and
    /// later ones queue up on the taskflow and each is started by the
    /// tear-down of the one before it, so they share the graph in turn rather
    /// than at the same time. That is upstream's arrangement, and it is why
    /// `run` may be called again without waiting for the previous run.
    ///
    /// A taskflow with no source task -- an empty one, or one whose every
    /// node is in a cycle -- schedules nothing and the returned handle is
    /// already finished. Upstream returns a ready future for the empty case
    /// and blocks forever on the cyclic one; a static graph with a cycle has
    /// no execution order, and reporting that as "nothing ran" beats reporting
    /// it as a hang.
    pub fn run<'a>(&'a self, taskflow: &'a Taskflow) -> RunHandle<'a> {
        self.run_n_with(taskflow, 1, || {})
    }

    /// Runs `taskflow` once and calls `on_finish` when the run is over.
    ///
    /// Upstream's `run(Taskflow&, C&&)`. The callback runs on whichever worker
    /// finished the run, before the handle is released, so a
    /// [`RunHandle::wait`] that returns has already seen it; a panic out of it
    /// is reported as the run's failure, since unwinding out of the tear-down
    /// would leave the waiter blocked for good.
    pub fn run_with<'a, C>(&'a self, taskflow: &'a Taskflow, on_finish: C) -> RunHandle<'a>
    where
        C: FnOnce() + Send + 'static,
    {
        self.run_n_with(taskflow, 1, on_finish)
    }

    /// Runs `taskflow` `times` times over, one run after the other.
    ///
    /// Upstream's `run_n`. Zero runs it not at all and hands back a finished
    /// handle, as upstream's already-true predicate does.
    pub fn run_n<'a>(&'a self, taskflow: &'a Taskflow, times: usize) -> RunHandle<'a> {
        self.run_n_with(taskflow, times, || {})
    }

    /// Runs `taskflow` `times` times over and calls `on_finish` once the last
    /// of them is done. Upstream's `run_n(Taskflow&, size_t, C&&)`.
    pub fn run_n_with<'a, C>(
        &'a self,
        taskflow: &'a Taskflow,
        times: usize,
        on_finish: C,
    ) -> RunHandle<'a>
    where
        C: FnOnce() + Send + 'static,
    {
        self.run_until_with(taskflow, repeat(times), on_finish)
    }

    /// Runs `taskflow` over and over until `predicate` returns `true`.
    ///
    /// Upstream's `run_until`. The predicate is asked once before the first
    /// run and once after each run finishes, so a predicate that is true from
    /// the start runs the graph not at all.
    pub fn run_until<'a, P>(&'a self, taskflow: &'a Taskflow, predicate: P) -> RunHandle<'a>
    where
        P: FnMut() -> bool + Send + 'static,
    {
        self.run_until_with(taskflow, predicate, || {})
    }

    /// Runs `taskflow` until `predicate` returns `true`, then calls
    /// `on_finish`. Upstream's `run_until(Taskflow&, P&&, C&&)`, and the one
    /// the rest of the borrowing half of the family forwards to.
    ///
    /// The predicate and the callback are `Send + 'static` where upstream's
    /// capture by reference, for the reason
    /// [`Work`](crate::Work) is `'static` too: both outlive this call -- they
    /// live on the topology, which the taskflow's run queue shares -- so there
    /// is no lifetime for them to borrow. A caller that wants upstream's
    /// `[&counter]` shares an [`Arc`] with the closure instead.
    ///
    /// # Panics
    ///
    /// Panics if `predicate` panics on this thread, which is the one call of
    /// it that happens before a run exists to report a failure on. Every later
    /// call is on a worker, and a panic there is reported from
    /// [`RunHandle::wait`] like a task's.
    pub fn run_until_with<'a, P, C>(
        &'a self,
        taskflow: &'a Taskflow,
        mut predicate: P,
        on_finish: C,
    ) -> RunHandle<'a>
    where
        P: FnMut() -> bool + Send + 'static,
        C: FnOnce() + Send + 'static,
    {
        // Upstream's `if(f.empty() || p()) { c(); return ready future; }`.
        if !taskflow.has_source_task() || predicate() {
            on_finish();
            return RunHandle::finished(None);
        }

        let topology = Arc::new(Topology::new(predicate, on_finish));
        self.start(taskflow, &topology);

        RunHandle {
            topology,
            taskflow: None,
            borrow: PhantomData,
        }
    }

    /// Runs `taskflow` once, taking ownership of it, and drops it when the run
    /// is over.
    ///
    /// Upstream's `run(Taskflow&&)`, which hands the taskflow to an async task
    /// that holds it for the life of the run. Here the handle holds it, which
    /// is the same guarantee with no extra machinery: the taskflow is dropped
    /// when the handle is, and the handle does not finish dropping until the
    /// last run of the graph has.
    ///
    /// This is the one member of the family with no taskflow to borrow, so it
    /// is also the one a caller can use when there is nothing to borrow *from*
    /// -- a taskflow built inside a function and run on the way out. The
    /// handle still borrows the executor: a run in flight needs the pool that
    /// is running it, and dropping an [`Executor`] stops its workers whether
    /// or not a run is outstanding.
    pub fn run_owned(&self, taskflow: Taskflow) -> RunHandle<'_> {
        self.run_owned_n_with(taskflow, 1, || {})
    }

    /// Runs `taskflow` once, taking ownership of it, and calls `on_finish`
    /// when the run is over. Upstream's `run(Taskflow&&, C&&)`.
    pub fn run_owned_with<C>(&self, taskflow: Taskflow, on_finish: C) -> RunHandle<'_>
    where
        C: FnOnce() + Send + 'static,
    {
        self.run_owned_n_with(taskflow, 1, on_finish)
    }

    /// Runs `taskflow` `times` times over, taking ownership of it. Upstream's
    /// `run_n(Taskflow&&, size_t)`.
    pub fn run_owned_n(&self, taskflow: Taskflow, times: usize) -> RunHandle<'_> {
        self.run_owned_n_with(taskflow, times, || {})
    }

    /// Runs `taskflow` `times` times over, taking ownership of it, and calls
    /// `on_finish` once the last run is done. Upstream's
    /// `run_n(Taskflow&&, size_t, C&&)`.
    pub fn run_owned_n_with<C>(
        &self,
        taskflow: Taskflow,
        times: usize,
        on_finish: C,
    ) -> RunHandle<'_>
    where
        C: FnOnce() + Send + 'static,
    {
        self.run_owned_until_with(taskflow, repeat(times), on_finish)
    }

    /// Runs `taskflow` until `predicate` returns `true`, taking ownership of
    /// it. Upstream's `run_until(Taskflow&&, P&&)`.
    pub fn run_owned_until<P>(&self, taskflow: Taskflow, predicate: P) -> RunHandle<'_>
    where
        P: FnMut() -> bool + Send + 'static,
    {
        self.run_owned_until_with(taskflow, predicate, || {})
    }

    /// Runs `taskflow` until `predicate` returns `true`, taking ownership of
    /// it, and calls `on_finish` afterwards. Upstream's
    /// `run_until(Taskflow&&, P&&, C&&)`, and the one the owning half of the
    /// family forwards to.
    ///
    /// # Panics
    ///
    /// Panics if `predicate` panics on this thread, as
    /// [`Executor::run_until_with`] does.
    pub fn run_owned_until_with<P, C>(
        &self,
        taskflow: Taskflow,
        mut predicate: P,
        on_finish: C,
    ) -> RunHandle<'_>
    where
        P: FnMut() -> bool + Send + 'static,
        C: FnOnce() + Send + 'static,
    {
        // Boxed before anything can name it: a job holds the taskflow's
        // address, so it has to stay put while this handle moves.
        let taskflow = Box::new(taskflow);

        if !taskflow.has_source_task() || predicate() {
            on_finish();
            return RunHandle::finished(Some(taskflow));
        }

        let topology = Arc::new(Topology::new(predicate, on_finish));
        self.start(&taskflow, &topology);

        RunHandle {
            topology,
            taskflow: Some(taskflow),
            borrow: PhantomData,
        }
    }

    /// Counts `topology` as a run of `taskflow` and starts it if no earlier
    /// run of that taskflow is still going.
    ///
    /// The tail of upstream's `run_until`: `_increment_topology()`, then
    /// `_fetch_enqueue`, then `_set_up_topology` if this run is the front of
    /// the queue. A run queued behind another is counted here all the same, so
    /// that [`Executor::wait_for_all`] waits for it and the worker loop's fast
    /// path knows there is something coming.
    fn start(&self, taskflow: &Taskflow, topology: &Arc<Topology>) {
        self.shared.increment_topology();

        if taskflow.enqueue_run(Arc::clone(topology)) == 0 {
            self.shared.set_up_topology(None, taskflow, topology);
        }
    }

    /// Blocks until no run is in flight on this executor. Upstream's
    /// `wait_for_all`.
    ///
    /// Upstream's destructor calls this so that a detached future cannot
    /// outlive the pool. Nothing here needs it for that -- a [`RunHandle`]
    /// borrows the executor and blocks on drop, so a run in flight is already
    /// a run whose handle is alive and whose borrow keeps the executor around.
    /// What it answers is the question upstream's `num_topologies` answers:
    /// is the executor idle.
    pub fn wait_for_all(&self) {
        let mut idle = self
            .shared
            .topologies
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        while self.shared.num_topologies() != 0 {
            idle = self
                .shared
                .no_topologies
                .wait(idle)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

/// The repeat predicate, upstream's `[repeat]() mutable { return repeat-- == 0; }`.
///
/// Asked once before the first run and once after each run, so it has to say
/// "stop" on its `times + 1`-th call. The decrement saturates where upstream's
/// `size_t` wraps, which makes no difference -- nothing asks again once it has
/// said stop -- and means the arithmetic cannot be the thing that goes wrong.
fn repeat(times: usize) -> impl FnMut() -> bool + Send + 'static {
    let mut remaining = times;
    move || {
        let stop = remaining == 0;
        remaining = remaining.saturating_sub(1);
        stop
    }
}

impl Shared {
    /// Arms `topology` over `taskflow`'s graph and queues its source tasks.
    ///
    /// Upstream's `_set_up_topology(Worker*, Topology*)`, whose `Worker*` is
    /// this `worker`: a thread that is one of this executor's workers pushes
    /// the sources onto its own queue, and any other thread goes through the
    /// shared buffers. `run` is the second case, and the tear-down -- which
    /// both re-runs a topology and starts the next one of a taskflow's queue
    /// -- is the first.
    ///
    /// Scheduling a run does not touch the topology count: a run is counted
    /// once, when its handle is created, however many times its graph goes
    /// round.
    pub(crate) fn set_up_topology(
        &self,
        worker: Option<&WorkerThread>,
        taskflow: &Taskflow,
        topology: &Topology,
    ) {
        let num_sources = topology.set_up(taskflow);
        debug_assert!(
            num_sources > 0,
            "a run is only ever created for a graph with a source task"
        );

        match worker {
            // Upstream's `_bulk_schedule(Worker&, ...)`, which is
            // `WorkerThread::schedule` per job here: the deque behind a
            // worker's queue grows, so there is no bulk push to make.
            Some(worker) => {
                for job in topology.sources(taskflow) {
                    worker.schedule(job);
                }
            }
            None => self.bulk_schedule(topology.sources(taskflow)),
        }
    }

    /// Ends a run whose last job has just finished, or sends its graph round
    /// again. Upstream's `_tear_down_topology`.
    ///
    /// Called by the worker that drove the topology's join counter to zero,
    /// and the last thing that worker does with any of its three arguments.
    pub(crate) fn tear_down_topology(
        &self,
        worker: Option<&WorkerThread>,
        taskflow: &Taskflow,
        topology: &Topology,
    ) {
        // Upstream's case 1: the predicate says run the graph again. Same
        // topology, so it is neither counted again nor finished -- and
        // `Topology::set_up` re-arms every join counter the run just spent.
        if topology.run_again() {
            self.set_up_topology(worker, taskflow, topology);
            return;
        }

        // Upstream's case 2: the final run of this topology -- which is where
        // a cancelled run arrives, since `run_again` answers no for one. Every
        // line below therefore runs for a cancelled run too, and that is the
        // point: cancelling stops the *scheduling*, not the accounting, so the
        // callback still fires, the next queued run of this taskflow still
        // starts, and the waiter is still released with `Ok(())`.
        //
        // The callback first, as upstream calls `_on_finish()` first, so that a
        // waiter released below has already seen whatever the callback did.
        topology.carry_out_callback();

        // Then the next run of this taskflow, if one queued up behind this
        // one, started *before* this run is released rather than after it as
        // upstream does. The reason is this port's lifetimes: `finish` below
        // releases the thread that owns this run's handle, and for a
        // `run_owned` handle that is the thread which then drops the taskflow,
        // so nothing may touch `taskflow` after it. Starting the next run here
        // is safe for the converse reason -- a taskflow with a run queued
        // behind this one is one that some other handle still borrows, which
        // is to say one nobody can be about to drop. A `run_owned` taskflow is
        // named by nothing but its own handle, so its queue can never hold a
        // second run.
        if let Some(next) = taskflow.pop_finished_run(topology) {
            self.set_up_topology(worker, taskflow, &next);
        }

        // The accounting, and only then the completion slot. This is
        // deliberately *not* upstream's order -- its `_tear_down_topology`
        // calls `_carry_out_promise()` and only then `_decrement_topology()`,
        // so a caller whose `tf::Future` has just come back can still see
        // `num_topologies()` counting the run it has already been handed the
        // result of. The window is small, real and free to close, and worth
        // closing because that count is what `wait_for_all` blocks on and what
        // the worker loop's fast path reads: here, once a `wait` returns, the
        // run is fully accounted for. The release in `decrement_topology`'s
        // `fetch_sub` and the lock `finish` takes are what carry that to the
        // waiting thread, whose own read of the count is relaxed.
        //
        // The order is also the only one the port's lifetimes allow, which is
        // what makes it free: `finish` releases the thread that owns the
        // topology *and* the taskflow's borrow, so it has to be this worker's
        // last touch of either, while `decrement_topology` reads only the
        // executor's shared state -- which every worker holds a share of, and
        // which therefore outlives all of them.
        self.decrement_topology();
        topology.finish();
    }
}

impl WorkerThread {
    /// Ends a run whose last job this worker has just finished.
    ///
    /// The worker's end of [`Shared::tear_down_topology`], which lives in this
    /// module because every line of it is about the run rather than about the
    /// worker; what the worker brings is the queue the next run's source tasks
    /// go onto.
    pub(crate) fn tear_down_topology(&self, taskflow: &Taskflow, topology: &Topology) {
        self.shared
            .tear_down_topology(Some(self), taskflow, topology);
    }

    /// Accounts for one job of `topology` having finished, ending the run if it
    /// was the last.
    ///
    /// Upstream's `_tear_down_nonasync` for a top-level node. It is the tail of
    /// `WorkerThread::invoke` and the whole of what a job skipped by the
    /// cancelled early-out at the top of that method still owes: the join
    /// counters have to reach zero for the run to finish at all, which is why
    /// upstream's cancelled branch goes to `_tear_down_invoke` rather than
    /// simply returning.
    ///
    /// **The last thing the caller may do with either argument**, for the
    /// reason [`Shared::tear_down_topology`] gives.
    pub(crate) fn finish_job(&self, taskflow: &Taskflow, topology: &Topology) {
        if topology.job_finished() {
            self.tear_down_topology(taskflow, topology);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::prelude::*;

    #[test]
    fn an_empty_taskflow_runs_to_completion_without_a_topology() {
        let executor = Executor::new(2);
        let taskflow = Taskflow::new();

        let handle = executor.run(&taskflow);
        assert_eq!(executor.num_topologies(), 0, "nothing was scheduled");
        handle.wait().expect("an empty run cannot panic");

        executor.wait_for_all();
    }

    /// A static graph with a cycle has no source and therefore no execution
    /// order. Upstream blocks forever on it; this port finishes the run
    /// without having scheduled anything, which is what `run`'s docs promise.
    #[test]
    fn a_taskflow_with_no_source_runs_nothing_rather_than_blocking() {
        let executor = Executor::new(2);
        let mut taskflow = Taskflow::new();
        let (a, b) = taskflow.emplace_n((|| panic!("never reached"), || panic!("never reached")));
        taskflow.task(a).precede([b]);
        taskflow.task(b).precede([a]);

        executor.run(&taskflow).wait().expect("nothing ran");
        assert_eq!(executor.num_topologies(), 0);
    }

    #[test]
    fn a_run_error_says_what_the_task_panicked_with() {
        let from_str = RunError::Panicked(Box::new("boom"));
        assert_eq!(from_str.to_string(), "a task panicked: boom");
        assert_eq!(format!("{from_str:?}"), "Panicked(a task panicked: boom)");

        let from_string = RunError::Panicked(Box::new(String::from("boom")));
        assert_eq!(from_string.to_string(), "a task panicked: boom");

        let from_other = RunError::Panicked(Box::new(7_u8));
        assert_eq!(from_other.to_string(), "a task panicked");
    }

    /// `num_topologies` is what [`Executor::wait_for_all`] blocks on and what
    /// the worker loop's fast path reads, so a run that has already handed its
    /// result back must not still be counted. The check is inside the loop
    /// rather than after it because the defect it guards is a window of a few
    /// instructions between a worker releasing the waiter and finishing its
    /// bookkeeping: one iteration almost never lands in it, and the window is
    /// wider the fewer cores there are to go round. The first version of this
    /// scheduler had that window -- it is upstream's order -- and a two-core
    /// run of `a_thousand_runs_of_a_diamond_chain` is what caught it.
    #[test]
    fn a_finished_run_is_no_longer_counted_once_its_result_is_handed_back() {
        const RUNS: usize = 1000;

        let executor = Executor::new(2);
        let mut taskflow = Taskflow::new();
        let (a, b) = taskflow.emplace_n((|| {}, || {}));
        taskflow.task(a).precede([b]);

        for _ in 0..RUNS {
            executor.run(&taskflow).wait().expect("no task panicked");
            assert_eq!(executor.num_topologies(), 0, "`wait` returned too early");
        }

        // The same promise on the path that never waits explicitly: the
        // handle's `Drop` blocks, so it owes exactly what `wait` owes.
        for _ in 0..RUNS {
            drop(executor.run(&taskflow));
            assert_eq!(executor.num_topologies(), 0, "`drop` returned too early");
        }

        // And `wait_for_all` has nothing left to wait for.
        executor.wait_for_all();
        assert_eq!(executor.num_topologies(), 0);
    }

    /// The borrow is the whole design, so it is worth a compile-time check
    /// that the handle really does carry both halves of it.
    #[test]
    fn a_handle_borrows_the_executor_and_the_taskflow() {
        fn send<T: Send>() {}
        send::<RunHandle<'static>>();

        let executor = Executor::new(1);
        let taskflow = Taskflow::new();
        let handle = executor.run(&taskflow);
        assert!(format!("{handle:?}").contains("RunHandle"));
    }

    /// A [`Canceller`] is the whole reason the estate word has an allocation
    /// of its own, so the bounds it is for are worth asserting rather than
    /// assuming: a task body is `FnMut() + Send + 'static`, and a token that
    /// missed any of these could not be captured by one.
    #[test]
    fn a_canceller_is_clone_send_sync_and_static() {
        fn clone_send_sync_static<T: Clone + Send + Sync + 'static>() {}
        clone_send_sync_static::<Canceller>();

        let executor = Executor::new(1);
        let mut taskflow = Taskflow::new();
        taskflow.emplace(|| {});

        let handle = executor.run(&taskflow);
        let canceller = handle.canceller();
        assert_eq!(format!("{canceller:?}"), "Canceller { live: true }");
        handle.wait().expect("no task panicked");
    }

    /// A run that was over before the handle was handed out has nothing to
    /// cancel -- the empty, sourceless and already-true-predicate cases, which
    /// all come back from `RunHandle::finished`. Upstream's `EmptyFuture` is
    /// the same answer to the same question from the other end: a future with
    /// no topology at all.
    #[test]
    fn cancelling_a_run_that_never_started_reports_false() {
        let executor = Executor::new(2);
        let empty = Taskflow::new();

        let handle = executor.run(&empty);
        let canceller = handle.canceller();
        assert!(!handle.cancel(), "nothing was ever scheduled");
        assert!(!canceller.cancel());

        // And the same for a graph with work but no round to run it in.
        let mut taskflow = Taskflow::new();
        taskflow.emplace(|| panic!("never reached"));
        let handle = executor.run_n(&taskflow, 0);
        assert!(!handle.cancel());
    }

    /// A token may outlive the run's topology, which is what the `Weak` is
    /// for: the handle owns the topology and frees it when it is dropped, and
    /// a token left over reports `false` rather than reaching freed memory.
    /// Upstream's failed `weak_ptr::lock()`, exactly.
    #[test]
    fn a_canceller_outlives_its_handle_and_then_reports_false() {
        let executor = Executor::new(2);
        let mut taskflow = Taskflow::new();
        taskflow.emplace(|| {});

        let canceller = {
            let handle = executor.run(&taskflow);
            let canceller = handle.canceller();
            handle.wait().expect("no task panicked");
            canceller
        };

        assert!(!canceller.cancel(), "the run is gone");
        assert_eq!(format!("{canceller:?}"), "Canceller { live: false }");
        // Still `false` for a clone made after the run went away.
        assert!(!canceller.clone().cancel());
    }

    /// The repeat predicate is the whole of `run_n`, so its shape is worth
    /// pinning down away from a scheduler: `false` as many times as there are
    /// runs, then `true`.
    #[test]
    fn the_repeat_predicate_says_stop_once_the_count_is_spent() {
        for times in 0..4_usize {
            let mut predicate = repeat(times);
            for run in 0..times {
                assert!(!predicate(), "{times} runs, asked for run {run}");
            }
            assert!(predicate(), "{times} runs, asked once too often");
            // Asked again -- which nothing does -- it still says stop rather
            // than wrapping round.
            assert!(predicate());
        }
    }

    /// `run_n` runs the graph the number of times asked, and no run of it is
    /// still counted when the handle has been waited for.
    #[test]
    fn run_n_runs_the_graph_that_many_times() {
        let executor = Executor::new(2);
        let counter = Arc::new(AtomicUsize::new(0));

        let mut taskflow = Taskflow::new();
        let (a, b) = {
            let first = Arc::clone(&counter);
            let second = Arc::clone(&counter);
            taskflow.emplace_n((
                move || {
                    first.fetch_add(1, Ordering::Relaxed);
                },
                move || {
                    second.fetch_add(1, Ordering::Relaxed);
                },
            ))
        };
        taskflow.task(a).precede([b]);

        for times in [0, 1, 7] {
            counter.store(0, Ordering::Relaxed);
            executor
                .run_n(&taskflow, times)
                .wait()
                .expect("no task panicked");
            assert_eq!(counter.load(Ordering::Relaxed), times * 2);
            assert_eq!(executor.num_topologies(), 0);
        }
    }

    /// A run of a taskflow handed over by value owns it and frees it when the
    /// handle goes, which is the whole difference between `run_owned` and
    /// `run`. The `Drop` counter is what shows the taskflow is freed after the
    /// run rather than before it.
    #[test]
    fn run_owned_frees_the_taskflow_after_the_last_run() {
        struct CountOnDrop(Arc<AtomicUsize>);

        impl Drop for CountOnDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let executor = Executor::new(2);
        let runs = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));

        let mut taskflow = Taskflow::new();
        for _ in 0..4 {
            let counted = CountOnDrop(Arc::clone(&drops));
            let runs = Arc::clone(&runs);
            taskflow.emplace(move || {
                // The guard is captured by the body, so it is dropped when the
                // graph is -- which is what this case is watching for.
                let _ = &counted;
                runs.fetch_add(1, Ordering::Relaxed);
            });
        }

        let handle = executor.run_owned_n(taskflow, 3);
        handle.wait().expect("no task panicked");

        assert_eq!(runs.load(Ordering::Relaxed), 12, "three runs of four tasks");
        assert_eq!(drops.load(Ordering::Relaxed), 4, "one drop per task body");
        assert_eq!(executor.num_topologies(), 0);
    }

    /// Two runs of one taskflow may be outstanding at once: both are counted
    /// from the moment they are handed out, and the second does not start until
    /// the first is over. The sink's own assertion is what checks the second
    /// part -- it would see a count from the other run if they overlapped.
    #[test]
    fn a_second_run_of_one_taskflow_queues_behind_the_first() {
        let executor = Executor::new(4);
        let inside = Arc::new(AtomicUsize::new(0));

        let mut taskflow = Taskflow::new();
        let (enter, leave) = {
            let entering = Arc::clone(&inside);
            let leaving = Arc::clone(&inside);
            taskflow.emplace_n((
                move || {
                    assert_eq!(
                        entering.fetch_add(1, Ordering::SeqCst),
                        0,
                        "another run was still going"
                    );
                },
                move || {
                    assert_eq!(leaving.fetch_sub(1, Ordering::SeqCst), 1);
                },
            ))
        };
        taskflow.task(enter).precede([leave]);

        let first = executor.run(&taskflow);
        let second = executor.run(&taskflow);
        assert_eq!(executor.num_topologies(), 2, "both runs are counted");

        first.wait().expect("the first run panicked");
        second.wait().expect("the second run panicked");
        assert_eq!(inside.load(Ordering::SeqCst), 0);
        assert_eq!(executor.num_topologies(), 0);
    }
}

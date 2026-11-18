//! Translated from `taskflow/core/executor.hpp` (Taskflow v4.1.0): the `run`,
//! `run_n` and `run_until` overload set, `_set_up_topology`,
//! `_tear_down_topology`, `_increment_topology` and `wait_for_all`.
//!
//! Upstream's `run` hands back a `tf::Future<void>`: a `std::future` with a
//! `std::weak_ptr<Topology>` beside it, which owns nothing and is there so
//! that `cancel()` has something to reach through. The rule upstream documents
//! and cannot enforce is *do not mutate a taskflow while it is running*; here
//! it is the type system's, [`Executor::run`] borrowing the executor and the
//! taskflow for the lifetime it puts on the handle. Running the taskflow
//! *again* is allowed, as upstream, and is what the queue on [`Taskflow`] is
//! for; [`Drop`] blocks until the run is over, so that the ordinary way of
//! using a handle never leaves a run going on behind the caller's back.
//!
//! # Design
//!
//! Blocking in `Drop` is not a guarantee, `Drop` not being guaranteed to run:
//! [`std::mem::forget`], a [`ManuallyDrop`](std::mem::ManuallyDrop), a
//! [`Box::leak`] and an [`Rc`](std::rc::Rc) cycle all skip it, and under NLL
//! the borrow ends at the handle's last use -- the `forget` call. What closes
//! that is ownership rather than `Drop`, and `DESIGN.md` has the decision:
//! every queued job holds a share of the run *and* of the taskflow's storage,
//! so a leaked handle leaks rather than dangling. Two consequences are this
//! module's. **Editing the taskflow panics** rather than racing the scheduler,
//! [`Taskflow::graph_mut`] finding the run's shares in the way -- a finished
//! run is never in the way, however recently it finished. And **dropping the
//! executor is allowed**, which it otherwise is not: the workers stop, the
//! queued jobs are dropped, and the run never finishes, which nothing waits
//! for, the thing that would have waited being the leaked handle. There is no
//! detached run either, and deliberately: a run outliving its handle is what
//! the design does not *offer*, as distinct from what it cannot survive.
//!
//! Upstream spells twelve overloads of three names, over a borrowed or a
//! moved-in taskflow and with or without an on-finish callback. Rust has no
//! overloading, so the twelve are twelve methods named after what they vary --
//! `_n` and `_until` for the repeat count and the predicate, `_with` for the
//! callback, `run_owned` for the half that takes the taskflow by value -- each
//! two lines over [`Executor::run_until_with`] or
//! [`Executor::run_owned_until_with`], as upstream's ten short overloads
//! forward to its two long ones.

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError, Weak};
use std::task::{Context, Poll};

use crate::executor::topology::{
    Cancellable, Finished, GraphOwner, Job, Parent, Topology, set_up_graph,
};
use crate::executor::worker::WorkerThread;
use crate::executor::{Executor, Shared};
use crate::graph::Node;
use crate::taskflow::{Flow, Taskflow};

/// Why a run did not finish cleanly -- and what an
/// [`AsyncHandle`](crate::AsyncHandle)'s `wait`, a
/// [`TaskGroup::try_corun`](crate::TaskGroup) and a `Runtime::try_corun`
/// report about the work *they* waited for, which is why
/// [`CorunError::Run`](crate::CorunError) carries this. `#[non_exhaustive]`
/// because later milestones have more ways for a run to fail; cancellation is
/// deliberately not one, a cancelled run completing *normally*, as upstream's
/// `fu.get()` does not throw after a `fu.cancel()`.
#[non_exhaustive]
pub enum RunError {
    /// A task panicked, carrying what it panicked with.
    ///
    /// The port's reading of upstream's `std::exception_ptr`: a panic escaping
    /// a task body is caught where upstream catches an exception, stored in
    /// the topology, and surfaced here, so a caller who wants the failure to
    /// propagate on *this* thread hands the payload to
    /// [`std::panic::resume_unwind`]. Only the first panicking task is
    /// reported, and the panic cancels the rest of the run on the terms
    /// [`RunHandle::cancel`] describes, setting the `EXCEPTION` bit of the
    /// word that method sets `CANCELLED` in; the *next* run starts clean.
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

/// A run of a taskflow in progress, and the right to wait for it: upstream's
/// `tf::Future<void>`, with the differences the module docs set out.
///
/// ```
/// use rustflow::prelude::*;
/// use rustflow::Executor;
/// let executor = Executor::new(4);
/// let mut taskflow = Taskflow::new();
/// let (a, b) = taskflow.emplace_n((|| println!("a"), || println!("b")));
/// taskflow.task(a).precede([b]);
/// executor.run(&taskflow).wait().expect("no task panicked");
///
/// // The borrow is over, so the taskflow can be edited and run again.
/// taskflow.emplace(|| println!("c"));
/// executor.run(&taskflow).wait().expect("no task panicked");
/// ```
pub struct RunHandle<'a> {
    /// The run, shared with the taskflow's queue of runs and with this run's
    /// every queued job.
    topology: Arc<Topology>,
    /// The taskflow, for a run that was handed one by value:
    /// [`Executor::run_owned`], upstream's `run(Taskflow&&)`. Dropped after
    /// the blocking [`Drop`] below has seen the last run finish, a field being
    /// dropped after the body of its owner's `drop`.
    taskflow: Option<Taskflow>,
    /// The borrows that make "do not mutate a running taskflow" a compile
    /// error. A marker rather than two reference fields because nothing reads
    /// either one: their whole job is to be held.
    borrow: PhantomData<(&'a Executor, &'a Taskflow)>,
}

/// The right to cancel a run, with no lifetime attached.
///
/// Handed out by [`RunHandle::canceller`], and `Clone + Send + Sync +
/// 'static`, so that it can be captured by a task body, stored in a struct or
/// sent to another thread.
///
/// Upstream needs no such type: its `tf::Future` *is* the cancel token, and
/// its own tests cancel a run from another taskflow's task by capturing the
/// future by reference. A task body here is `'static`, so it cannot capture a
/// [`RunHandle`]; this is the part that can be. [`RunHandle::cancel`] has what
/// cancelling promises.
#[derive(Clone)]
pub struct Canceller {
    /// The run's cancellation state, or what is left of it: upstream's
    /// `std::weak_ptr<Topology>`, `Weak` because a token may outlive the
    /// topology, which goes with the [`RunHandle`] owning it.
    state: Weak<Cancellable>,
}

impl Canceller {
    /// Cancels the run, reporting `false` if it has already finished.
    ///
    /// Upstream's `tf::Future::cancel` through a `weak_ptr` whose referent may
    /// be gone, the one difference from [`RunHandle::cancel`]: `false` covers
    /// both a finished run and one whose handle has been dropped.
    pub fn cancel(&self) -> bool {
        self.state
            .upgrade()
            .is_some_and(|state| Cancellable::cancel(&state))
    }
}

impl fmt::Debug for Canceller {
    /// Hand-written: `Weak`'s own prints `(Weak)`, where what is worth seeing
    /// is whether there is still a run on the other end.
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
    /// stops scheduling tasks of this run onwards and tasks already running
    /// are left to finish, cancellation being non-preemptive, so "none of it
    /// had started" and "all of it ran" are both outcomes of a cancellation
    /// that raced the run. The run then completes normally --
    /// [`RunHandle::wait`] returns `Ok(())`, the callback still runs, and a
    /// `run_n` or `run_until` run does not go round again -- so a cancelled
    /// run is indistinguishable afterwards from one whose tasks did nothing.
    /// Cancelling twice is harmless and reports `true` twice; the `false` is
    /// upstream's `weak_ptr::lock()` failing.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    /// let executor = Executor::new(4);
    /// let mut taskflow = Taskflow::new();
    /// for _ in 0..1000 {
    ///     taskflow.emplace(|| std::thread::sleep(std::time::Duration::from_millis(1)));
    /// }
    /// let run = executor.run(&taskflow);
    /// // What `cancel` *reports* is deliberately not asserted: a finished
    /// // run answers `false`, and whether this one has is a race with the
    /// // pool. `tests/cancellation.rs` pins the behaviour.
    /// run.cancel();
    /// // A cancelled run completes normally, minus the work it had not begun.
    /// run.wait().expect("no task panicked");
    /// ```
    pub fn cancel(&self) -> bool {
        self.topology.cancel()
    }

    /// A token that cancels this run from anywhere, a task body included: the
    /// lifetime-free half of [`RunHandle::cancel`], of which any number may be
    /// handed out, each cancelling the one run this handle is for.
    #[must_use]
    pub fn canceller(&self) -> Canceller {
        Canceller {
            state: Arc::downgrade(self.topology.cancellable()),
        }
    }

    /// Blocks until the run finishes, and reports whether a task panicked.
    ///
    /// Upstream's `tf::Future::get`, which rethrows the stored exception on
    /// the calling thread where a `Result` says the same without unwinding
    /// somebody else's stack; [`RunError::Panicked`] has how to get that
    /// behaviour back. When this returns the run is not merely finished but
    /// fully accounted for: [`Executor::num_topologies`] no longer counts it
    /// and no job holds a share of the taskflow's storage, so the taskflow can
    /// be edited at once. Dropping the handle promises the same two, and
    /// upstream's `tf::Future` neither. Waiting inside a task body blocks that
    /// worker, so [`Executor::corun`] lends a worker to the inner run.
    pub fn wait(self) -> Result<(), RunError> {
        outcome(self.topology.join())
    }

    /// The handle for a run that is over before it began: a sourceless
    /// taskflow, or a predicate already true. Upstream returns a ready
    /// `std::future` from a promise it fulfils on the spot, so that the caller
    /// gets a handle behaving like every other. `taskflow` is `Some` only for
    /// the `run_owned` family, which owns what it was handed.
    fn finished(taskflow: Option<Taskflow>) -> Self {
        Self {
            // Finished before it is handed out, and holding no share of any
            // taskflow: `Topology::nothing_to_run` says what follows from
            // that.
            topology: Arc::new(Topology::nothing_to_run()),
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
    /// Blocks until the run finishes.
    ///
    /// What makes the handle's borrow mean something *in practice*: a handle
    /// dropped rather than leaked cannot leave a run going after the borrow it
    /// was scheduled under ended. The module docs say why that is no
    /// guarantee; after [`RunHandle::wait`] this returns at once.
    fn drop(&mut self) {
        // A payload nobody waited for is dropped rather than re-raised: this
        // `Drop` may itself run during an unwind, where a second panic aborts.
        // Upstream's `Future` likewise lets one go.
        drop(self.topology.join());
    }
}

/// A run can be awaited instead of waited for, on any runtime and without one:
/// the completion slot holds the [`Waker`](std::task::Waker) of whoever is
/// polling, and the worker that finishes the run wakes it where it would
/// otherwise signal the condvar, so nothing here spawns, blocks or sleeps.
/// Polling does not consume the handle, so it is still there when the future
/// is ready and dropping it then returns at once; a future polled again after
/// it was ready reports success, the payload being handed over once. Dropping
/// the handle *before* the run is over blocks the dropping thread.
///
/// ```
/// use std::future::Future;
/// use std::pin::pin;
/// use std::sync::Arc;
/// use std::task::{Context, Poll, Wake, Waker};
/// use rustflow::Executor;
/// use rustflow::prelude::*;
///
/// /// The whole runtime this needs: unpark the thread that is waiting.
/// struct Unpark(std::thread::Thread);
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
/// let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
/// let mut context = Context::from_waker(&waker);
/// let mut run = pin!(executor.run_n(&taskflow, 3));
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
    /// Several runs of one taskflow may be in the air at once: the later ones
    /// queue up on the taskflow, each started by the tear-down of the one
    /// before, which is upstream's arrangement and why `run` may be called
    /// again without waiting. A taskflow with no source task schedules nothing
    /// and the handle is already finished, where upstream blocks for ever on
    /// the cyclic case.
    pub fn run<'a>(&'a self, taskflow: &'a Taskflow) -> RunHandle<'a> {
        self.run_n_with(taskflow, 1, || {})
    }

    /// Runs `taskflow` once and calls `on_finish` when the run is over:
    /// upstream's `run(Taskflow&, C&&)`. The callback runs on whichever worker
    /// finished the run, before the handle is released, so a returning
    /// [`RunHandle::wait`] has already seen it; a panic out of it is the run's
    /// failure, unwinding out of the tear-down leaving the waiter blocked.
    pub fn run_with<'a, C>(&'a self, taskflow: &'a Taskflow, on_finish: C) -> RunHandle<'a>
    where
        C: FnOnce() + Send + 'static,
    {
        self.run_n_with(taskflow, 1, on_finish)
    }

    /// Runs `taskflow` `times` times over, one run after the other: upstream's
    /// `run_n`, where zero runs it not at all and hands back a finished handle,
    /// as an already-true predicate does.
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

    /// Runs `taskflow` over and over until `predicate` returns `true`:
    /// upstream's `run_until`. The predicate is asked once before the first
    /// run and once after each, so one true from the start runs nothing.
    pub fn run_until<'a, P>(&'a self, taskflow: &'a Taskflow, predicate: P) -> RunHandle<'a>
    where
        P: FnMut() -> bool + Send + 'static,
    {
        self.run_until_with(taskflow, predicate, || {})
    }

    /// Runs `taskflow` until `predicate` returns `true`, then calls
    /// `on_finish`: upstream's `run_until(Taskflow&, P&&, C&&)`, which the
    /// borrowing half of the family forwards to.
    ///
    /// The predicate and the callback are `Send + 'static` where upstream's
    /// capture by reference, for the reason [`Work`](crate::Work) is `'static`
    /// too: both outlive this call, living on the topology the taskflow's run
    /// queue shares. Upstream's `[&counter]` becomes a shared [`Arc`].
    ///
    /// # Panics
    ///
    /// Panics if `predicate` panics on this thread, the one call of it that
    /// happens before a run exists to report a failure on; every later call is
    /// on a worker, reported from [`RunHandle::wait`] like a task's.
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
        if !taskflow.flow().has_source_task() || predicate() {
            on_finish();
            return RunHandle::finished(None);
        }

        let topology = Topology::for_run(
            predicate,
            on_finish,
            Arc::clone(taskflow.flow()),
            Arc::clone(&self.shared),
        );
        self.start(taskflow.flow(), &topology);

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
    /// that holds it for the life of the run; here the handle holds it, the
    /// same guarantee with no extra machinery. The one member of the family
    /// with no taskflow to borrow, and so the one for when there is nothing to
    /// borrow *from*; the handle still borrows the executor, dropping which
    /// stops its workers.
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
        // Nothing is boxed and nothing has to stay put: what a job names is
        // the taskflow's *storage*, already a shared allocation of its own, so
        // this value is free to move into the handle below.
        if !taskflow.flow().has_source_task() || predicate() {
            on_finish();
            return RunHandle::finished(Some(taskflow));
        }

        let topology = Topology::for_run(
            predicate,
            on_finish,
            Arc::clone(taskflow.flow()),
            Arc::clone(&self.shared),
        );
        self.start(taskflow.flow(), &topology);

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
    /// the queue. A run queued behind another is counted all the same, so that
    /// [`Executor::wait_for_all`] waits for it and the worker loop knows there
    /// is something coming.
    fn start(&self, flow: &Arc<Flow>, topology: &Arc<Topology>) {
        self.shared.increment_topology();

        if flow.enqueue_run(Arc::clone(topology)) == 0 {
            self.shared.set_up_topology(None, topology);
        }
    }

    /// Blocks until this executor is carrying nothing -- no run, no free async
    /// task: upstream's `wait_for_all`.
    ///
    /// Upstream's destructor calls this so that a detached future cannot
    /// outlive the pool. A *run* does not need it for that -- a [`RunHandle`]
    /// borrows the executor and blocks on drop -- but an async task does, an
    /// [`Executor::silent_async`](crate::Executor::silent_async) handing out
    /// no handle and a dropped [`AsyncHandle`](crate::AsyncHandle) detaching.
    /// Once this returns, every taskflow whose runs it waited for is editable,
    /// as after a `wait`: the decrement that wakes it is performed by the last
    /// share of each run's hold on its storage.
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
/// `size_t` wraps, which changes nothing, nothing asking again once it has
/// said stop.
fn repeat(times: usize) -> impl FnMut() -> bool + Send + 'static {
    let mut remaining = times;
    move || {
        let stop = remaining == 0;
        remaining = remaining.saturating_sub(1);
        stop
    }
}

impl Shared {
    /// Arms every node of `owner`'s graph for `topology`, counts its source
    /// tasks against `parent` and queues them.
    ///
    /// Upstream's `_schedule_graph(Worker&, Graph&, Topology*, NodeBase*)`,
    /// the one scheduling primitive in the executor: a run's own graph goes
    /// through it with [`Parent::Run`], every nested graph with the node or
    /// anchor it hangs off. The order of the three steps is the whole of it --
    /// the parent is told about the sources *before* any is queued, a source
    /// being able to run, finish and decrement that counter before this call
    /// returns. Upstream takes a `Graph&` where this takes whatever owns the
    /// graph, a job owning what it names. The `worker` is upstream's: one of
    /// this executor's workers pushes onto its own queue, any other thread
    /// through the shared buffers. Scheduling does not touch the topology
    /// count, a run being counted once, when its handle is created.
    pub(crate) fn schedule_graph(
        &self,
        worker: Option<&WorkerThread>,
        owner: &GraphOwner,
        topology: &Arc<Topology>,
        parent: &Parent,
    ) {
        let num_sources = set_up_graph(owner.graph());
        parent.count_queued(topology, num_sources);

        match worker {
            // Upstream's `_bulk_schedule(Worker&, ...)`, which is
            // `WorkerThread::schedule` per job here: the deque behind a
            // worker's queue grows, so there is no bulk push to make.
            Some(worker) => {
                for job in Job::sources(owner, topology, parent) {
                    worker.schedule(job);
                }
            }
            None => self.bulk_schedule(Job::sources(owner, topology, parent)),
        }
    }

    /// Arms `topology` over `flow`'s graph and queues its source tasks:
    /// upstream's `_set_up_topology(Worker*, Topology*)`,
    /// [`Shared::schedule_graph`] with the topology as its own parent, which
    /// is upstream's `_set_up_graph(g, tpg, tpg)`. Upstream *stores* the
    /// source count where `schedule_graph` adds it, the same write -- a
    /// topology reaches this with nothing outstanding.
    pub(crate) fn set_up_topology(&self, worker: Option<&WorkerThread>, topology: &Arc<Topology>) {
        let storage = topology
            .storage()
            .expect("a run being set up holds a share of its hold on the storage");
        debug_assert!(
            storage.flow().has_source_task(),
            "a run is only ever created for a graph with a source task"
        );
        self.schedule_graph(worker, &GraphOwner::run(&storage), topology, &Parent::Run);
    }

    /// Ends a run whose last job has just finished, or sends its graph round
    /// again: upstream's `_tear_down_topology`, called by the worker that drove
    /// the topology's join counter to zero and the last thing that worker does
    /// with either argument. The graph is
    /// not an argument, where every other method of this family takes it: the
    /// job that got here has already given up its share of the taskflow's
    /// storage, so what this reaches the graph through is the run's own share,
    /// taken back out of the topology below.
    pub(crate) fn tear_down_topology(
        &self,
        worker: Option<&WorkerThread>,
        topology: &Arc<Topology>,
    ) {
        let storage = topology
            .storage()
            .expect("a run in flight holds a share of the taskflow it runs");
        let flow = storage.flow();

        // Upstream's case 1: the predicate says run the graph again. The same
        // topology, so neither counted again nor finished, `set_up_graph`
        // re-arming every join counter the run just spent.
        if topology.run_again() {
            self.set_up_topology(worker, topology);
            return;
        }

        // Upstream's case 2: the final run of this topology, where a
        // cancelled run arrives too, `run_again` answering no for one --
        // cancelling stops the *scheduling*, not the accounting. The callback
        // first, as upstream's `_on_finish()`, so that a waiter released below
        // has already seen whatever it did.
        topology.carry_out_callback();

        // Then the next run of this taskflow, if one queued behind this one,
        // started *before* this run is released where upstream starts it
        // after: the thread released below is free to edit the taskflow the
        // moment it wakes, so the last touch of the graph comes first. A
        // `run_owned` taskflow is named by nothing but its own handle, so its
        // queue can never hold a second run at all.
        if let Some(next) = flow.pop_finished_run(topology) {
            self.set_up_topology(worker, &next);
        }

        // The last two acts, in this order: this thread's own share of the
        // run's hold on the storage, then the run's. What releases a waiting
        // thread is the drop of the *last* share of that hold, wherever it is
        // -- `Drop for Storage` takes the run out of the executor's count and
        // then wakes the completion slot -- so these two lines are not merely
        // before the release but are what performs it, here when nothing else
        // holds a copy of a preempted node's job and on the worker dropping
        // that copy when something does. `executor::topology::ownership`'s
        // own docs have the lock and wake orderings this is one of.
        drop(storage);
        topology.release_flow();
    }
}

impl WorkerThread {
    /// Ends a run whose last job this worker has just finished.
    ///
    /// The worker's end of [`Shared::tear_down_topology`], which lives here
    /// because every line of it is about the run; what the worker brings is
    /// the queue the next run's source tasks go onto.
    pub(crate) fn tear_down_topology(&self, topology: &Arc<Topology>) {
        self.shared.tear_down_topology(Some(self), topology);
    }

    /// Accounts for one finished job against the parent its graph was
    /// scheduled under, ending the run or resuming that parent if it was the
    /// last.
    ///
    /// Upstream's `_tear_down_nonasync`, both branches: the tail of
    /// `WorkerThread::invoke`, and the whole of what a job skipped by the
    /// cancelled early-out still owes, the join counters having to reach zero
    /// for anything to finish. **The last thing the caller may do with the
    /// run**, and the caller must already have given up its share of the
    /// storage, which is why the only way here is [`Job::finish`].
    pub(crate) fn finish_job(&self, finished: Finished) {
        let (parent, topology) = finished.into_parts();

        match parent {
            // Upstream's `parent == node->_topology` branch: the last job of
            // the run's own graph ends the run.
            Parent::Run => {
                if topology.job_finished() {
                    self.tear_down_topology(&topology);
                }
            }
            // Upstream's `else`: the parent is a node whose graph this job
            // belonged to, or a corun's anchor.
            Parent::Nested(nested) => {
                // Read before the decrement, as upstream reads it: the
                // instant that counter reaches zero the parent may be resumed
                // on another worker, and a resumed parent clears its own
                // `PREEMPTED` bit, so a later read could miss the bit asking
                // for the resumption. (Upstream's comment is about the node
                // being *deleted* by then, which an arena and a share rule
                // out.)
                let base = nested.base();
                let preempted = base.nstate().load(Ordering::Relaxed) & Node::PREEMPTED != 0;

                if base.join_counter().fetch_sub(1, Ordering::AcqRel) == 1 && preempted {
                    // Upstream's `_update_cache(worker, cache, parent)` is
                    // not ported: the parent goes back on a queue like any
                    // other job. A corun's anchor is never preempted and has
                    // no job to resume -- the frame waiting on it watches the
                    // counter this decrement drove to zero.
                    if let Some(job) = nested.resume() {
                        self.schedule(job.clone());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::prelude::*;

    /// How long a handshake waits for the other side before it gives up.
    ///
    /// Far longer than any rendezvous in this module needs, pinned to one
    /// core included: nothing here passes because of this number, and the
    /// only thing that reaches it is a side that is never going to arrive.
    const GIVE_UP_AFTER: Duration = Duration::from_secs(120);

    /// Spins until `flag` is set, failing rather than hanging if it never is.
    ///
    /// The same handshake `tests/basics.rs` uses for issue #40's cases, and a
    /// spin rather than a condvar for the reason it gives: one end of every
    /// use of it is a task body, and a worker that parked would be a worker
    /// not running the rest of the run. `yield_now` rather than a busy
    /// `spin_loop` because the other end is often the *main* thread, which on
    /// a single core has to be let in.
    ///
    /// `what` finishes the sentence "gave up waiting for ...", because that
    /// message is the whole diagnostic if a scheduling defect trips it.
    fn spin_until(what: &str, flag: &AtomicBool) {
        let deadline = Instant::now() + GIVE_UP_AFTER;
        while !flag.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "gave up waiting for {what}");
            thread::yield_now();
        }
    }

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

        // A finished run reports `false` whether or not its topology is still
        // there, which is the half of this that holds the moment `wait`
        // returns: `Topology::finish` marks the run finished before it
        // releases the waiting thread.
        assert!(!canceller.cancel(), "the run is gone");
        assert!(!canceller.clone().cancel());

        // That the topology has actually been *freed* is a question about who
        // else holds a share of it, and the answer the instant a `wait`
        // returns is "possibly a worker": the thread that ends a run holds its
        // share across the `finish` that releases the waiter, and drops it a
        // few instructions later. So the pool is shut down -- which joins
        // every worker -- before the token is asked, and then nothing but the
        // token can name the run. Asserting it any earlier is a race, and was
        // one: this case failed about one run in ten on two cores.
        drop(executor);
        assert_eq!(format!("{canceller:?}"), "Canceller { live: false }");
        assert!(!canceller.cancel());
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
    ///
    /// The count is read while the first run is *held open*, which is issue
    /// #76: the graph is two atomic operations, so the version of this case
    /// that read the count straight after handing out the two handles was
    /// asserting a state the pool was free to have left already -- and `0` is
    /// then the right answer, since the last share of a run's hold on its
    /// storage decrements the count before it releases a waiter (#138; the
    /// tear-down did, when this was written). CI's two cores found that; this
    /// box's twenty-four
    /// hid it. So the source task of the first run signals that it is in and
    /// waits, which makes "both runs outstanding" a fact about the program
    /// rather than about the machine: the first cannot finish while its own
    /// task is waiting, and the second cannot start until the first has
    /// finished.
    ///
    /// Holding the first run open sharpens the queueing half rather than
    /// distorting it, which is why the count assertion is kept where #67
    /// dropped `empty_future`'s: the window in which an overlapping second
    /// run would trip the `fetch_add` assertion below is now as wide as the
    /// main thread's own progress, instead of being whatever two atomics
    /// take.
    #[test]
    fn a_second_run_of_one_taskflow_queues_behind_the_first() {
        let executor = Executor::new(4);
        let inside = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));

        let mut taskflow = Taskflow::new();
        let (enter, leave) = {
            let entering = Arc::clone(&inside);
            let leaving = Arc::clone(&inside);
            let entered = Arc::clone(&entered);
            let released = Arc::clone(&released);
            taskflow.emplace_n((
                move || {
                    assert_eq!(
                        entering.fetch_add(1, Ordering::SeqCst),
                        0,
                        "another run was still going"
                    );
                    // The first run to get here holds its run open until the
                    // main thread has read the count; the second finds the
                    // flag already set and does not wait at all.
                    entered.store(true, Ordering::Release);
                    spin_until("the main thread to read the topology count", &released);
                },
                move || {
                    assert_eq!(leaving.fetch_sub(1, Ordering::SeqCst), 1);
                },
            ))
        };
        taskflow.task(enter).precede([leave]);

        let first = executor.run(&taskflow);
        let second = executor.run(&taskflow);

        // With the first run's source task waiting, the first run is in
        // flight and the second is still behind it in the taskflow's queue --
        // and a queued run is counted from the moment it is handed out, which
        // is the property under test.
        spin_until("the first run to start", &entered);
        assert_eq!(executor.num_topologies(), 2, "both runs are counted");

        released.store(true, Ordering::Release);
        first.wait().expect("the first run panicked");
        second.wait().expect("the second run panicked");
        assert_eq!(inside.load(Ordering::SeqCst), 0);
        assert_eq!(executor.num_topologies(), 0);
    }
}

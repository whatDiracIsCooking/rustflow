//! Translated from `taskflow/core/executor.hpp` (Taskflow v4.1.0): `corun`,
//! `corun_until`, `_corun_until` and `_corun_graph`.
//!
//! # Why `run(..).wait()` from inside a task deadlocks
//!
//! A task body runs on a worker thread, and an executor's workers are a fixed
//! pool: the only threads that will ever take a job out of its queues. So when
//! a task calls [`Executor::run`] and then waits -- with
//! [`RunHandle::wait`](crate::RunHandle::wait), or by letting the handle drop,
//! which blocks the same way -- it parks one of those threads in the inner
//! run's completion slot. The work it is waiting for is sitting in the pool's
//! queues; the thread that could have taken it is asleep.
//!
//! Nothing in the executor undoes that. A parked thread is woken by the
//! topology it waited on finishing, and the topology finishes when some
//! *other* worker runs its last job, so the pool has to have a worker to
//! spare. One nested wait on a four-worker executor leaves three. Four at once
//! leave none, and then no inner graph can start: every thread that could run
//! one is waiting for one. At a single worker it takes just one nested wait --
//! the inner run's sources are queued, the only worker is asleep, and the run
//! that would wake it needs that worker. That is a deadlock, not a slow patch,
//! and it is what upstream's documentation means by
//! `//executor.run(tf).wait(); <- blocking the worker without doing anything
//! will introduce deadlock`.
//!
//! [`Executor::corun`] is the answer, and the whole of it is: do not park.
//! The calling worker stays in the work-stealing loop, running the inner
//! graph's jobs and anybody else's, until the inner run's last job has
//! finished. The pool never loses a thread to waiting, so no amount of nesting
//! can drain it. `WorkerThread::corun_until`, in `executor::worker`, is that
//! loop, and its comment says what separates it from the worker's own.
//!
//! Three consequences worth stating, because they are what a caller feels:
//!
//! - The wait is not ordered. A corunning worker runs whatever it can steal,
//!   which includes work from runs the caller knows nothing about, so `corun`
//!   returns no sooner than the inner run finishes and no later than the next
//!   job it picked up is done.
//! - `corun` is for a worker of *this* executor and nothing else. From any
//!   other thread it is [`CorunError::NotAWorker`]; see below.
//! - Nesting a run on a *different* executor and blocking on it is still
//!   fine, and still costs a thread. Two pools cannot starve each other the
//!   way one pool starves itself.
//!
//! # The inner run's anchor and topology
//!
//! Every queued job holds a share of the run it belongs to, of the storage of
//! the taskflow it walks and of the parent its graph was scheduled under,
//! which is what `executor::topology` rests on: none of the three can be
//! freed while a job can name it. A corun adds nothing to that argument and
//! takes nothing away. What it does add is a second reason the inner run
//! cannot be left behind: both the anchor and the topology are owned by
//! `InnerRun`, a value this module creates on the corun frame and never hands
//! out, so a caller cannot hold either and therefore cannot leak them -- and
//! the `Drop` that ends the run is the only way the frame can be left, an
//! unwind through it included.
//!
//! The anchor is what the inner graph is scheduled *under*, and it is
//! upstream's local `NodeBase anchor` in `corun`: the jobs of the graph count
//! it down rather than counting down the topology, and the corunning frame
//! waits for it to reach zero. The topology is still there and still the run
//! -- it is what `join` waits on and what a run policy reports to -- but
//! nothing drives *its* counter to zero, so this frame is its own tear-down.
//! Scheduling under an anchor rather than under the topology is what the
//! parent machinery of #47 is for.
//!
//! That makes a corun the closure-shaped version of the promise
//! [`Executor::run`] makes with a lifetime: the borrow of `taskflow` ends when
//! this function returns, and it cannot return until the run is over.
//!
//! # Why a failure comes back here rather than from `wait`
//!
//! A task of the inner graph that panics does not report to the inner run.
//! It reports to the **anchor**, because the anchor is what this frame is
//! waiting on and this frame is therefore what can do something about it --
//! that is the whole of what upstream's `ESTATE::EXPLICITLY_ANCHORED` is for,
//! and `corun_graph` below -- crate-private, so it is named rather than
//! linked -- is where the bit is set. So the payload comes out of the corun
//! call, on the worker that made it, and not out of a `wait` somewhere else:
//! there is no handle to wait on, and the task body that called `corun` is
//! the only code that knows what the inner graph was for.
//!
//! The inner run's own payload slot is still read, as a backstop, because one
//! kind of failure has no node to walk up from and lands there instead: this
//! run's policy callbacks, which a corun gives nothing to do, and anything a
//! future node kind records against a topology directly.
//!
//! Which leaves the question of what to *do* with it, and both answers are
//! wanted often enough that this module offers each by name:
//!
//! - [`Executor::corun`] re-raises it, with
//!   [`resume_unwind`](std::panic::resume_unwind). That is upstream's
//!   `_rethrow_exception` exactly, and it is the shape that composes: a
//!   caller who wrapped the call in nothing has the failure propagate out of
//!   its own task body, where the executor catches it as that task's failure
//!   and the *outer* run reports it. The payload keeps its identity all the
//!   way, so a handler two levels up still downcasts it to what the inner
//!   task panicked with.
//! - [`Executor::try_corun`] hands it back as a [`CorunError::Run`]. That is
//!   the shape for a caller who means to inspect or ignore the failure, and
//!   it is what upstream's callers write a `try`/`catch` around.
//!
//! Both leave the rest of the inner graph cancelled, on the terms the
//! [`executor`](crate::executor) module docs set out for any failure:
//! claiming the anchor's payload slot sets the same `EXCEPTION` bit that
//! stops jobs being started, so the siblings of the failing task that had not
//! begun do not run. The outer run is untouched by either shape -- the walk
//! stopped at the anchor -- unless the body lets the panic through, which is
//! precisely how `corun` propagates.

use std::any::Any;
use std::fmt;
use std::panic;
use std::sync::Arc;

use crate::executor::Executor;
use crate::executor::run_handle::RunError;
use crate::executor::topology::{GraphOwner, Nested, Topology};
use crate::executor::worker::WorkerThread;
use crate::taskflow::{Flow, Taskflow};

/// Runs `flow`'s graph under `parent` and waits for it on the calling worker,
/// handing back what the first task of it to fail panicked with.
///
/// Upstream's `Executor::_corun_graph`, and the one primitive every blocking
/// wait on a nested graph is built from: [`Executor::corun`] here,
/// `Subflow::join` (#50) and `Runtime::corun` (#51). Four steps, in
/// upstream's order, and the order is the whole of it:
///
/// 1. a graph with nothing to run is nothing to wait for, so this returns at
///    once -- upstream's `if(g.empty()) return`;
/// 2. `parent` is anchored **before** the graph is scheduled, because a
///    source of it can fail before this call has returned and a failure that
///    arrives while the bit is clear walks past the frame that is waiting;
/// 3. the graph goes under `parent`, so its jobs count that parent down
///    rather than the run, and the calling worker stays in the work-stealing
///    loop until they have all finished -- which is what keeps a nested wait
///    from costing the pool a thread;
/// 4. the guard is dropped and the payload taken, which is upstream's
///    `p->_rethrow_exception()` with the decision of what to do about it left
///    to the caller. That is what gives the two shapes [`Executor::corun`]
///    and [`Executor::try_corun`] one implementation.
///
/// The graph arrives as whatever owns it rather than as a `&Graph`, because a
/// job of this graph has to own the storage its node indices resolve in and a
/// reference cannot be owned. It is any of the four kinds of graph owner
/// `Shared::schedule_graph` takes since #74, which is what lets the two
/// callers it has today share one implementation: [`Executor::corun`] hands
/// over a share of a taskflow's storage, and `Subflow::join` (#50) the lease
/// of the subgraph its own node owns.
///
/// `worker` is upstream's `Worker&`: the real one when the caller is a
/// dispatch arm inside `WorkerThread::invoke`, and a
/// [`corun_view`](WorkerThread::corun_view) of it when the caller is a task
/// *body*, which cannot name the frame running it. Either way the loop is the
/// same one.
pub(crate) fn corun_graph(
    worker: &mut WorkerThread,
    owner: &GraphOwner,
    topology: &Arc<Topology>,
    parent: &Arc<Nested>,
) -> Option<Box<dyn Any + Send>> {
    // Upstream's `g.empty()`, widened to the graph that *has* nothing to run
    // -- an empty one, or one whose every node is in a cycle. Scheduling one
    // would queue no job, so nothing would ever drive the parent's counter
    // off zero and the wait below would be over before it began; returning
    // here says the same thing without taking a guard for it.
    if !owner.has_source_task() {
        return None;
    }

    {
        let _anchor = parent.base().anchor_explicitly();
        worker
            .shared
            .schedule_graph(Some(worker), owner, topology, &parent.as_parent());
        worker.corun_until(|| parent.outstanding() == 0);
    }

    parent.base().take_panic()
}

/// Why a [`Executor::corun`] or [`Executor::corun_until`] call did not do what
/// it was asked.
///
/// `#[non_exhaustive]` for the same reason [`RunError`] is: later milestones
/// have more ways for a nested run to fail. Cancellation turned out not to be
/// one of them, which is worth recording because it was the reason this was
/// written that way. A corun's topology is the one `InnerRun` owns on the
/// corunning frame: nothing outside this module can name it and no
/// [`Canceller`](crate::Canceller) is ever handed out for it, so there is no
/// way to cancel a corun, and cancelling the *outer* run does not reach it
/// either -- the task that called `corun` was already running, and
/// cancellation is not preemptive. Upstream agrees by construction: its
/// `Executor::corun` schedules the graph with a null topology and a fresh
/// local anchor as the parent, so the `_is_parent_cancelled` check at the top
/// of its `_invoke` answers no for the nodes of it -- until one of them
/// fails. A failure under the anchor marks it, which is how the rest of a
/// corun graph is skipped, and that is a failure rather than a cancellation
/// however much the scheduler reads the two as one bit. Where the
/// cancellation question does come back is the corun upstream *does* pass an
/// enclosing topology for -- a subflow or a module task, which is M4, and
/// which is also where this enum is likeliest to gain a variant.
#[non_exhaustive]
pub enum CorunError {
    /// The calling thread is not a worker of this executor.
    ///
    /// Upstream throws here -- *"corun must be called by a worker of the
    /// executor"* -- and the shape of the failure is the only part this port
    /// changes. It is an error rather than a panic because it is the one thing
    /// a caller can act on: the fix is [`Executor::run`], which is what a
    /// thread outside the pool should have called. It is an error rather than
    /// silently running the graph the blocking way because a `corun` on a
    /// non-worker is a mistake about *which thread the code is on*, and
    /// quietly doing the other thing would hide it.
    NotAWorker,
    /// A task of the corun graph panicked.
    ///
    /// The same failure [`Executor::run`] reports, carried unchanged so that
    /// there is one answer in the crate to "a task panicked" and one place
    /// ([`RunError::Panicked`]) that says what to do with the payload.
    ///
    /// Returned by [`Executor::try_corun`] only. The other two calls of this
    /// module cannot produce it: [`Executor::corun`] re-raises that failure
    /// instead of reporting it, and [`Executor::corun_until`] waits on a
    /// predicate rather than on a graph of its own.
    Run(RunError),
}

impl fmt::Display for CorunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAWorker => f.write_str("corun must be called from a worker of this executor"),
            Self::Run(error) => error.fmt(f),
        }
    }
}

impl fmt::Debug for CorunError {
    /// Hand-written because [`RunError`]'s is: the panic payload is a
    /// `Box<dyn Any + Send>`, which has no `Debug`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAWorker => f.write_str("NotAWorker"),
            Self::Run(error) => write!(f, "Run({error:?})"),
        }
    }
}

impl std::error::Error for CorunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotAWorker => None,
            Self::Run(error) => Some(error),
        }
    }
}

/// The run and the anchor of one [`Executor::corun`], held for as long as the
/// corun frame runs.
///
/// The anchor is upstream's local `NodeBase anchor` in `corun` and plays the
/// same part: the thing the inner graph's nodes count down to, and the thing
/// a failure inside that graph is delivered to, belonging to the corunning
/// frame for the duration.
///
/// Shared, as [`Executor::run`]'s own topology is, because every queued job of
/// the run holds a share of it -- so a job of a corun cannot outlive its run
/// any more than a job of a `run` can. [`Drop`] is then not what keeps the
/// topology alive but what keeps the *frame* honest, and it is the reason this
/// is a type rather than a local: an unwind through the frame holding one --
/// which only a scheduler bug could cause, since a *task*'s panic is caught
/// and recorded instead -- must not take the caller's borrow of the taskflow
/// with it while a job of this run is still in a queue.
///
/// The unwind [`Executor::corun`] *does* raise is not that frame's: it is
/// raised by `corun` itself once `corun_payload` has returned and this value
/// has been dropped, which is why those are two functions.
struct InnerRun {
    /// The run. Nothing outside this module can name it.
    topology: Arc<Topology>,
    /// What the inner graph is scheduled under, and what the corun frame
    /// waits for: upstream's local `NodeBase anchor`.
    anchor: Arc<Nested>,
}

impl InnerRun {
    /// A run of `flow`'s graph that has not started, with an anchor of its
    /// own to count down.
    ///
    /// A corun runs its graph exactly once, so the run policy is the only one
    /// that says "do not go round again" from the start, with no callback to
    /// carry out: `run_n`/`run_until` are for a taskflow a caller runs. The
    /// anchor has no node to resume, which is what tells the last job of the
    /// graph that nothing is waiting to be re-queued -- this frame is.
    fn new(flow: &Arc<Flow>) -> Self {
        Self {
            topology: Arc::new(Topology::new(|| true, || {}, Arc::clone(flow))),
            anchor: Nested::new(None),
        }
    }

    /// The topology, which is what the run policy reports to and what `join`
    /// waits on. A failing *task* of the inner graph reports to the anchor
    /// instead -- the module docs say why -- so this is the backstop rather
    /// than the usual path.
    fn topology(&self) -> &Arc<Topology> {
        &self.topology
    }

    /// The anchor, to schedule the graph under.
    fn anchor(&self) -> &Arc<Nested> {
        &self.anchor
    }

    /// Ends the run: the last touch of the graph, and then the completion
    /// slot.
    ///
    /// With an anchor as the parent, no job of this run ever drives the
    /// topology's counter to zero, so nothing reaches
    /// `Shared::tear_down_topology` and this frame owes what that function
    /// would have done. Two of its steps do not apply -- a corun's policy
    /// cannot ask for another round and its topology is never on a taskflow's
    /// run queue -- which leaves releasing the run's share of the storage and
    /// releasing whoever waits, in that order, for the reason
    /// `executor::topology` gives: by the time this frame returns, its
    /// caller may edit the taskflow again.
    ///
    /// Idempotent, so that [`Drop`] can call it after the frame already has.
    fn end(&self) {
        self.topology.release_flow();
        self.topology.finish();
    }

    /// Blocks until the run is over and hands back whatever reached the
    /// run's own payload slot, if anything.
    ///
    /// Called after [`InnerRun::end`], so it does not actually block; what it
    /// is for is the payload, which it takes so that the caller reports it
    /// once. A failing task of the inner graph does not come through here --
    /// it reports to the anchor, which `corun_graph` takes the payload from
    /// -- so what this can still find is a failure with no node to walk up
    /// from: this run's policy callbacks, or a node kind that records against
    /// a topology directly.
    fn join(&self) -> Option<Box<dyn std::any::Any + Send>> {
        self.topology.join()
    }
}

impl Drop for InnerRun {
    /// Ends the run if the frame did not get that far.
    ///
    /// On the ordinary path this is a second, idempotent `end` and a `join`
    /// that finds the payload already taken. What it is here for is an unwind
    /// through the corun frame -- which a scheduler bug could cause, since a
    /// *task*'s panic is caught and recorded instead: a corun's topology is
    /// finished by its own frame, so a frame that unwound past that point
    /// would leave a run nothing could ever complete, and a `join` waiting on
    /// it would block for ever.
    fn drop(&mut self) {
        self.end();

        // A payload nobody asked for is dropped here rather than re-raised,
        // for the reason the run handle gives: this may be running during an
        // unwind, and a panic from a `Drop` in an unwind aborts the process.
        drop(self.join());
    }
}

impl Executor {
    /// Runs `taskflow` from inside one of this executor's own workers, without
    /// parking that worker.
    ///
    /// Upstream's `corun`. The calling worker schedules `taskflow`'s sources
    /// and then keeps working -- on this graph and on anything else it can
    /// steal -- until the run's last job has finished. The module docs say why
    /// this exists and what calling [`Executor::run`] here would do instead.
    ///
    /// `taskflow` is borrowed for the call, which is the whole of the
    /// "do not mutate a running taskflow" rule for a corun: the borrow ends
    /// when the run is over, because the run being over is what lets this
    /// function return. As with [`Executor::run`], two overlapping runs of the
    /// *same* taskflow are the caller's responsibility -- they would put two
    /// workers on one node, which [`Work::invoke`](crate::Work::invoke)
    /// reports as a panic.
    ///
    /// A taskflow with no source task -- an empty one, or one whose every node
    /// is in a cycle -- runs nothing and returns `Ok`. Upstream returns early
    /// on the empty graph too; the cyclic one is this port's reading, the same
    /// one [`Executor::run`] documents.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let executor = Arc::new(Executor::new(1));
    /// let counter = Arc::new(AtomicUsize::new(0));
    ///
    /// // What the task body runs: one worker is enough, because the worker
    /// // that coruns it is the one that runs it.
    /// let inner = Arc::new({
    ///     let mut inner = Taskflow::new();
    ///     let counter = Arc::clone(&counter);
    ///     inner.emplace(move || {
    ///         counter.fetch_add(1, Ordering::Relaxed);
    ///     });
    ///     inner
    /// });
    ///
    /// let mut outer = Taskflow::new();
    /// outer.emplace({
    ///     let executor = Arc::clone(&executor);
    ///     let inner = Arc::clone(&inner);
    ///     move || executor.corun(&inner).expect("a worker may corun")
    /// });
    ///
    /// executor.run(&outer).wait().expect("no task panicked");
    /// assert_eq!(counter.load(Ordering::Relaxed), 1);
    ///
    /// // And from a thread that is not a worker of this executor, it is an
    /// // error rather than a hang.
    /// assert!(executor.corun(&inner).is_err());
    /// ```
    ///
    /// # Panics
    ///
    /// Panics with the payload of the first task of `taskflow` to panic,
    /// which is upstream's rethrow: `corun` propagates a failure of the inner
    /// graph on the calling worker, where the task body that called it can
    /// catch it as its own -- or not, in which case the executor catches it
    /// where it catches any other failing task and the *outer* run reports
    /// it. The payload keeps its identity, because it is handed to
    /// [`std::panic::resume_unwind`] rather than re-panicked: a caller that
    /// catches it downcasts to exactly what the inner task panicked with, and
    /// the panic hook does not print it a second time.
    ///
    /// [`Executor::try_corun`] is the same call with that failure as an
    /// `Err` instead. The module docs say which to reach for.
    ///
    /// # Errors
    ///
    /// [`CorunError::NotAWorker`] if the calling thread is not a worker of
    /// this executor, which is the only error this shape returns -- a failing
    /// task is the panic above, not an error.
    pub fn corun(&self, taskflow: &Taskflow) -> Result<(), CorunError> {
        match self.corun_payload(taskflow)? {
            // Upstream's `p->_rethrow_exception()`, and the reason this is the
            // last line of the call rather than something done with the inner
            // run still on the frame: the unwind starts here, so everything
            // that run owed has already been paid.
            Some(payload) => panic::resume_unwind(payload),
            None => Ok(()),
        }
    }

    /// Runs `taskflow` from inside one of this executor's own workers,
    /// reporting a failing task as an error rather than propagating it.
    ///
    /// [`Executor::corun`] with the one difference the name says: a task of
    /// `taskflow` that panics comes back as
    /// [`CorunError::Run`]`(`[`RunError::Panicked`]`)` carrying the payload,
    /// in the shape [`RunHandle::wait`](crate::RunHandle::wait) reports a
    /// failing run in, instead of being raised on this thread. Everything
    /// else -- the anchor, the loop, the borrow of `taskflow`, what happens
    /// to the rest of the inner graph -- is that method's, and its docs are
    /// where the explanation lives.
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use rustflow::prelude::*;
    /// use rustflow::{Executor, RunError};
    ///
    /// let executor = Arc::new(Executor::new(2));
    /// let inner = Arc::new({
    ///     let mut inner = Taskflow::new();
    ///     inner.emplace(|| panic!("x"));
    ///     inner
    /// });
    ///
    /// let mut outer = Taskflow::new();
    /// outer.emplace({
    ///     let executor = Arc::clone(&executor);
    ///     let inner = Arc::clone(&inner);
    ///     move || {
    ///         // The failure is the caller's to deal with, so the task that
    ///         // coruns survives it and the outer run completes.
    ///         let error = executor.try_corun(&inner).expect_err("`x` panicked");
    ///         assert_eq!(error.to_string(), "a task panicked: x");
    ///     }
    /// });
    ///
    /// executor.run(&outer).wait().expect("the outer task caught it");
    /// ```
    ///
    /// # Errors
    ///
    /// [`CorunError::NotAWorker`] if the calling thread is not a worker of
    /// this executor, and [`CorunError::Run`] if a task of `taskflow`
    /// panicked. Only the first panicking task is reported, as with
    /// [`Executor::run`]; what the rest of the inner graph does is
    /// [`Executor::corun`]'s answer.
    pub fn try_corun(&self, taskflow: &Taskflow) -> Result<(), CorunError> {
        match self.corun_payload(taskflow)? {
            Some(payload) => Err(CorunError::Run(RunError::Panicked(payload))),
            None => Ok(()),
        }
    }

    /// Both shapes of corun, up to what is done with the payload.
    ///
    /// The whole of upstream's `corun` template: an anchor this frame owns --
    /// upstream's local `NodeBase`, in an [`Arc`] here because every job of
    /// the graph holds a share of it -- the graph scheduled under it, and the
    /// calling worker kept in the loop until that anchor's counter reaches
    /// zero. What this adds is the topology, which upstream passes as
    /// `nullptr` here and this port cannot: a queued job owns a share of its
    /// run, and the run is what the policy callbacks report to.
    fn corun_payload(
        &self,
        taskflow: &Taskflow,
    ) -> Result<Option<Box<dyn Any + Send>>, CorunError> {
        let id = self.this_worker_id().ok_or(CorunError::NotAWorker)?;

        let run = InnerRun::new(taskflow.flow());
        let mut payload = None;
        if taskflow.flow().has_source_task() {
            // Counted as the `run` family counts its own, and for the reason
            // that family's `start` gives: a worker skips the steal loop while
            // `num_topologies` is zero, so this run has to be visible there
            // before its sources are.
            self.shared.increment_topology();

            // The corun view is this frame's stand-in for the `Worker&`
            // upstream hands `_corun_graph`; `WorkerThread::corun_view` says
            // why the real one cannot be reached from a task body.
            let mut worker = WorkerThread::corun_view(Arc::clone(&self.shared), id);
            payload = corun_graph(
                &mut worker,
                &GraphOwner::flow(taskflow.flow()),
                run.topology(),
                run.anchor(),
            );

            // The decrement every run gets, which for a corun is this frame's
            // rather than a tear-down's -- see `InnerRun::end`, which is the
            // rest of what the tear-down would have done.
            self.shared.decrement_topology();
        }

        // Nothing to schedule means nobody to finish the run either, and the
        // line below covers both cases: a graph with no source was never
        // counted, so there is nothing to decrement for it.
        run.end();

        // The anchor is where a failing task of the inner graph reports, so
        // the payload above is almost always the whole answer. The run's own
        // slot is still read, as the backstop it is: a failure with no node
        // to walk up from -- a run policy's callback -- lands there instead,
        // and so does one recorded against the topology directly by a node
        // kind the executor does not run yet.
        Ok(payload.or_else(|| run.join()))
    }

    /// Keeps the calling worker in the work-stealing loop until `predicate`
    /// returns `true`.
    ///
    /// Upstream's `corun_until`, and the primitive under
    /// [`Executor::corun`]: it is how a worker waits for anything at all --
    /// a run on another executor, a thread it spawned, a flag another task
    /// sets -- without taking its thread out of the pool. Later milestones'
    /// `Runtime::corun_until` is this method.
    ///
    /// `predicate` is called on the calling worker, between jobs, and may run
    /// many times; it must not block, and it must eventually return `true`.
    /// This loop has nothing else that ends it: a predicate that never becomes
    /// true keeps the worker running other people's jobs forever, which is
    /// upstream's behaviour and is the same bug as a `while` loop that never
    /// exits.
    ///
    /// A panic in a job this loop happens to run is delivered to whatever is
    /// waiting for *that* job's graph -- its own run, or a frame blocked on a
    /// nested graph it belongs to -- and reported to whoever waits there, so
    /// there is nothing for this method to report but the one failure below.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicBool, Ordering};
    ///
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let executor = Arc::new(Executor::new(2));
    /// let ready = Arc::new(AtomicBool::new(false));
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace({
    ///     let executor = Arc::clone(&executor);
    ///     let ready = Arc::clone(&ready);
    ///     move || {
    ///         // Work this executor knows nothing about, waited for without
    ///         // giving up the worker.
    ///         let elsewhere = std::thread::spawn({
    ///             let ready = Arc::clone(&ready);
    ///             move || ready.store(true, Ordering::Release)
    ///         });
    ///         executor
    ///             .corun_until(|| ready.load(Ordering::Acquire))
    ///             .expect("a worker may corun");
    ///         elsewhere.join().expect("the spawned thread panicked");
    ///     }
    /// });
    ///
    /// executor.run(&taskflow).wait().expect("no task panicked");
    /// assert!(ready.load(Ordering::Relaxed));
    /// ```
    ///
    /// # Errors
    ///
    /// [`CorunError::NotAWorker`] if the calling thread is not a worker of
    /// this executor. It is the only error this method can return.
    pub fn corun_until(&self, predicate: impl FnMut() -> bool) -> Result<(), CorunError> {
        let id = self.this_worker_id().ok_or(CorunError::NotAWorker)?;

        WorkerThread::corun_view(Arc::clone(&self.shared), id).corun_until(predicate);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::prelude::*;

    /// A chain of `tasks` tasks, each bumping `counter`.
    fn chain(tasks: usize, counter: &Arc<AtomicUsize>) -> Taskflow {
        let mut taskflow = Taskflow::new();
        let mut previous = None;
        for _ in 0..tasks {
            let counter = Arc::clone(counter);
            let task = taskflow.emplace(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            });
            if let Some(previous) = previous {
                taskflow.task(previous).precede([task]);
            }
            previous = Some(task);
        }
        taskflow
    }

    /// The acceptance case this issue names: upstream throws from a non-worker
    /// and this port must report it, rather than panicking and rather than
    /// parking the caller on a run no worker has been told about.
    #[test]
    fn corun_from_a_non_worker_is_an_error() {
        let executor = Executor::new(2);
        let counter = Arc::new(AtomicUsize::new(0));
        let taskflow = chain(2, &counter);

        assert!(matches!(
            executor.corun(&taskflow),
            Err(CorunError::NotAWorker)
        ));
        assert!(matches!(
            executor.corun_until(|| true),
            Err(CorunError::NotAWorker)
        ));

        // The refusal is the whole of it: nothing was scheduled, nothing ran,
        // and the executor is still idle.
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        assert_eq!(executor.num_topologies(), 0);
        executor.wait_for_all();
    }

    /// A worker of *another* executor is not a worker of this one, which is
    /// what the per-executor thread-id map buys over a thread-local.
    #[test]
    fn corun_from_a_worker_of_another_executor_is_an_error() {
        let host = Executor::new(1);
        let other = Arc::new(Executor::new(1));
        let counter = Arc::new(AtomicUsize::new(0));
        let inner = Arc::new(chain(1, &counter));

        let mut outer = Taskflow::new();
        let refused = Arc::new(AtomicUsize::new(0));
        outer.emplace({
            let other = Arc::clone(&other);
            let inner = Arc::clone(&inner);
            let refused = Arc::clone(&refused);
            move || {
                if matches!(other.corun(&inner), Err(CorunError::NotAWorker)) {
                    refused.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        host.run(&outer).wait().expect("no task panicked");
        assert_eq!(refused.load(Ordering::Relaxed), 1);
        assert_eq!(counter.load(Ordering::Relaxed), 0, "nothing was corun");
    }

    /// One worker, one task, one corun: the smallest arrangement in which
    /// `run(..).wait()` would deadlock and `corun` must not.
    #[test]
    fn a_single_worker_coruns_an_inner_graph() {
        let executor = Arc::new(Executor::new(1));
        let counter = Arc::new(AtomicUsize::new(0));
        let inner = Arc::new(chain(3, &counter));

        let mut outer = Taskflow::new();
        outer.emplace({
            let executor = Arc::clone(&executor);
            let inner = Arc::clone(&inner);
            move || {
                executor.corun(&inner).expect("a worker may corun");

                // A `corun` that has returned is accounted for, not merely
                // finished -- the same promise `wait` makes, and for the same
                // reason: the tear-down decrements the count before it
                // releases this frame. With one worker nothing else can be
                // running, so the only topology left is the outer run that
                // this task belongs to.
                assert_eq!(executor.num_topologies(), 1);
            }
        });

        executor.run(&outer).wait().expect("no task panicked");
        assert_eq!(counter.load(Ordering::Relaxed), 3);
        executor.wait_for_all();
        assert_eq!(executor.num_topologies(), 0, "the inner run was accounted");
    }

    /// A corun of a graph with no source schedules nothing and still returns:
    /// the empty case upstream returns early on, and the cyclic one this port
    /// reports as a run that ran nothing.
    #[test]
    fn coruning_a_graph_with_no_source_runs_nothing() {
        let executor = Arc::new(Executor::new(2));

        let mut cyclic = Taskflow::new();
        let (a, b) = cyclic.emplace_n((|| panic!("never reached"), || panic!("never reached")));
        cyclic.task(a).precede([b]);
        cyclic.task(b).precede([a]);
        let cyclic = Arc::new(cyclic);
        let empty = Arc::new(Taskflow::new());

        let mut outer = Taskflow::new();
        outer.emplace({
            let executor = Arc::clone(&executor);
            let cyclic = Arc::clone(&cyclic);
            let empty = Arc::clone(&empty);
            move || {
                executor.corun(&empty).expect("an empty graph coruns");
                executor.corun(&cyclic).expect("a cyclic graph coruns");
            }
        });

        executor.run(&outer).wait().expect("no task panicked");
        assert_eq!(executor.num_topologies(), 0);
    }

    /// A panic in a corun task is reported to the corunning worker, in the
    /// same shape `wait` reports it, and does not escape onto the worker:
    /// `try_corun` is the shape for a caller that means to look at it, so the
    /// task that called it survives and the outer run completes.
    #[test]
    fn a_panicking_corun_task_is_reported_as_an_error() {
        let executor = Arc::new(Executor::new(2));
        let inner = Arc::new({
            let mut inner = Taskflow::new();
            inner.emplace(|| panic!("boom"));
            inner
        });

        let mut outer = Taskflow::new();
        let message = Arc::new(std::sync::Mutex::new(String::new()));
        outer.emplace({
            let executor = Arc::clone(&executor);
            let inner = Arc::clone(&inner);
            let message = Arc::clone(&message);
            move || {
                let error = executor.try_corun(&inner).expect_err("the task panicked");
                *message.lock().expect("the slot is not poisoned") = error.to_string();
            }
        });

        executor
            .run(&outer)
            .wait()
            .expect("the outer task survived");
        assert_eq!(
            *message.lock().expect("the slot is not poisoned"),
            "a task panicked: boom"
        );
    }

    /// The other shape: `corun` re-raises the payload on the corunning
    /// worker, so a task body that does not catch it fails with it and the
    /// outer run reports that -- with the payload still the one the *inner*
    /// task panicked with, which is what `resume_unwind` buys over a fresh
    /// panic.
    #[test]
    fn a_panicking_corun_task_propagates_through_the_calling_task() {
        let executor = Arc::new(Executor::new(2));
        let inner = Arc::new({
            let mut inner = Taskflow::new();
            inner.emplace(|| panic!("boom"));
            inner
        });

        let mut outer = Taskflow::new();
        outer.emplace({
            let executor = Arc::clone(&executor);
            let inner = Arc::clone(&inner);
            move || executor.corun(&inner).expect("a worker may corun")
        });

        let error = executor
            .run(&outer)
            .wait()
            .expect_err("the corun panic became the outer task's failure");
        // One variant, so this is a plain destructuring from inside the
        // crate; `tests/exceptions.rs` is where it has to be a `match`.
        let RunError::Panicked(payload) = &error;
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
    }

    /// And the same payload can be caught by the *task* that called `corun`,
    /// which is upstream's
    /// `REQUIRE_THROWS_WITH_AS(executor.corun(tf), "x", ..)` inside a task
    /// body: `catch_unwind` around `corun` is what a C++ `try`/`catch`
    /// around it is.
    #[test]
    fn a_calling_task_can_catch_what_corun_re_raises() {
        let executor = Arc::new(Executor::new(2));
        let inner = Arc::new({
            let mut inner = Taskflow::new();
            inner.emplace(|| panic!("boom"));
            inner
        });

        let caught = Arc::new(AtomicUsize::new(0));
        let mut outer = Taskflow::new();
        outer.emplace({
            let executor = Arc::clone(&executor);
            let inner = Arc::clone(&inner);
            let caught = Arc::clone(&caught);
            move || {
                let payload = panic::catch_unwind(AssertUnwindSafe(|| {
                    executor.corun(&inner).expect("a worker may corun");
                }))
                .expect_err("the corun re-raised the inner panic");
                assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
                caught.fetch_add(1, Ordering::Relaxed);
            }
        });

        executor
            .run(&outer)
            .wait()
            .expect("the outer task caught it");
        assert_eq!(caught.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_corun_error_says_which_failure_it_is() {
        let not_a_worker = CorunError::NotAWorker;
        assert_eq!(
            not_a_worker.to_string(),
            "corun must be called from a worker of this executor"
        );
        assert_eq!(format!("{not_a_worker:?}"), "NotAWorker");
        assert!(std::error::Error::source(&not_a_worker).is_none());

        let panicked = CorunError::Run(RunError::Panicked(Box::new("boom")));
        assert_eq!(panicked.to_string(), "a task panicked: boom");
        assert_eq!(
            format!("{panicked:?}"),
            "Run(Panicked(a task panicked: boom))"
        );
        assert!(std::error::Error::source(&panicked).is_some());
    }
}

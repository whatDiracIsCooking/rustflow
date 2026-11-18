//! Translated from `taskflow/core/executor.hpp` (Taskflow v4.1.0): `corun`,
//! `corun_until`, `_corun_until` and `_corun_graph`.
//!
//! # Design
//!
//! `DESIGN.md` has why a worker that waits for a nested graph does not park,
//! which is the whole of what this module is for, and what a caller feels for
//! it. The rest is the two things a corun does that a run does not.
//!
//! The inner graph is scheduled under an **anchor** rather than under the
//! topology, upstream's local `NodeBase anchor`: its jobs count that anchor
//! down and the corunning frame waits for it to reach zero, while the topology
//! is still the run -- what `join` waits on, what a run policy reports to --
//! with nothing driving *its* counter, so this frame is its own tear-down. The
//! anchor is also where a failing task of the inner graph reports, upstream's
//! `ESTATE::EXPLICITLY_ANCHORED`, so the payload comes out of the corun call
//! on the worker that made it rather than out of a `wait` somewhere else;
//! `DESIGN.md` has why there are two shapes of that, and both leave the rest
//! of the inner graph cancelled. The run's own slot is read as a backstop, for
//! the one failure with no node to walk up from: a run policy's callback.
//!
//! And the frame waits *twice*, which `DESIGN.md` also has: the anchor
//! reaching zero says every job has finished, not that every copy of a
//! preempted node's job has been dropped, and the caller's next line may be an
//! edit of the taskflow. So the frame takes its run's hold on the storage out
//! of the topology, schedules the graph through it, and waits in the same loop
//! for `Arc::get_mut` to call that hold unique -- the one question no mere
//! holder of a share can answer. It terminates, every other holder being a
//! finished job or a copy on its way out of a frame that owes nothing; costs
//! the pool no thread; and covers the nested graphs, a job of a module's
//! target graph keeping the hold alive through its parent chain. The other
//! callers of `corun_graph` need none of it, waiting on a *node* inside a run
//! somebody else is waiting on.
//!
//! The anchor is taken *before* the graph is scheduled, here and in
//! [`Runtime::try_corun`](crate::Runtime::try_corun): a source can fail
//! before the scheduling call returns, and a failure arriving while the bit
//! is clear would walk straight past the waiting frame.
//!
//! Cancellation is not one of the ways a corun fails, which is worth
//! recording rather than leaving as an absence. A corun's topology is the one
//! `InnerRun` owns on the frame, which nothing outside this module can name
//! and no [`Canceller`](crate::Canceller) is handed out for, and cancelling
//! the *outer* run does not reach it either, the task that called `corun`
//! being already running. Upstream agrees by construction, its `corun`
//! scheduling the graph with a null topology and a fresh local anchor, so that
//! `_is_parent_cancelled` answers no for the nodes of it -- until one fails,
//! which marks the anchor and is how the rest of a corun graph is skipped,
//! however much the scheduler reads failure and cancellation as one bit.
//!
//! `InnerRun` is a type rather than a pair of locals so that its [`Drop`] can
//! keep the *frame* honest: an unwind through the frame -- which only a
//! scheduler bug could cause, a *task*'s panic being caught and recorded --
//! must not take the caller's borrow of the taskflow with it while a job is
//! still queued. The unwind [`Executor::corun`] *does* raise is not that
//! frame's; `corun` raises it once `InnerRun` has been dropped, which is why
//! those are two functions.

use std::any::Any;
use std::fmt;
use std::panic;
use std::sync::Arc;

use crate::executor::Executor;
use crate::executor::run_handle::RunError;
use crate::executor::topology::{GraphOwner, Nested, Storage, Topology};
use crate::executor::worker::WorkerThread;
use crate::taskflow::{Flow, Taskflow};

/// Runs `flow`'s graph under `parent` and waits for it on the calling worker,
/// handing back what the first task of it to fail panicked with.
///
/// Upstream's `Executor::_corun_graph`, the one primitive every blocking wait
/// on a nested graph is built from. The payload is handed back rather than
/// raised, which gives the two shapes of corun one implementation.
///
/// The graph arrives as whatever owns it rather than as a `&Graph`, a job of
/// it having to own the storage its node indices resolve in. `worker` is
/// upstream's `Worker&`: the real one when the caller is a dispatch arm in
/// `WorkerThread::invoke`, a [`corun_view`](WorkerThread::corun_view) of it
/// when the caller is a task *body*, which cannot name the frame running it.
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
/// `#[non_exhaustive]` for [`RunError`]'s reason, and cancellation is not one
/// of the ways, which the module docs' `# Design` section has.
#[non_exhaustive]
pub enum CorunError {
    /// The calling thread is not a worker of this executor.
    ///
    /// Upstream throws here -- *"corun must be called by a worker of the
    /// executor"* -- and an error rather than a panic is all this port
    /// changes: the fix is [`Executor::run`].
    NotAWorker,
    /// A task of the corun graph panicked.
    ///
    /// The same failure [`Executor::run`] reports, so that one place
    /// ([`RunError::Panicked`]) says what to do with the payload. Returned by
    /// [`Executor::try_corun`] only.
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
/// Both are shared, every queued job of the run holding a share, so [`Drop`]
/// is not what keeps them alive; what it is for is the `# Design` section's.
struct InnerRun {
    /// The run. Nothing outside this module can name it.
    topology: Arc<Topology>,
    /// What the inner graph is scheduled under, and what the corun frame
    /// waits for: upstream's local `NodeBase anchor`.
    anchor: Arc<Nested>,
}

impl InnerRun {
    /// A run of `flow`'s graph that has not started, with an anchor of its own
    /// to count down. A corun runs its graph once, so its policy says "do not
    /// go round again" from the start, and the anchor has no node to resume.
    fn new(flow: &Arc<Flow>) -> Self {
        Self {
            topology: Arc::new(Topology::new(|| true, || {}, Arc::clone(flow))),
            anchor: Nested::new(None),
        }
    }

    /// The topology: what the run policy reports to and what `join` waits on.
    fn topology(&self) -> &Arc<Topology> {
        &self.topology
    }

    /// The anchor, to schedule the graph under.
    fn anchor(&self) -> &Arc<Nested> {
        &self.anchor
    }

    /// This run's hold on the taskflow's storage, taken out for the frame to
    /// own: what the graph is scheduled through, and what the frame waits to
    /// become unique -- which it never could while the run kept a share of its
    /// own. Panics if called twice, or after [`InnerRun::end`].
    fn take_hold(&self) -> Arc<Storage> {
        self.topology
            .take_storage()
            .expect("a corun's run holds its storage until this frame takes it")
    }

    /// Ends the run: the last touch of the graph, and then the completion
    /// slot. This frame is its own tear-down, for the `# Design` section's
    /// reason, minus a corun's policy asking for no second round and its
    /// topology never being on a taskflow's run queue. Idempotent, so that
    /// [`Drop`] can call it after the frame already has.
    fn end(&self) {
        self.topology.release_flow();
    }

    /// Blocks until the run is over and hands back whatever reached the run's
    /// own payload slot. Called after [`InnerRun::end`], so it does not
    /// actually block; what it is for is the payload, taken once.
    fn join(&self) -> Option<Box<dyn std::any::Any + Send>> {
        self.topology.join()
    }
}

impl Drop for InnerRun {
    /// Ends the run if the frame did not get that far: on the ordinary path a
    /// second, idempotent `end` and a `join` that finds the payload taken.
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
    /// steal -- until the run's last job has finished; the module docs say
    /// why.
    ///
    /// `taskflow` is borrowed for the call, which is the whole of the "do not
    /// mutate a running taskflow" rule for a corun, and "over" is as strong
    /// here as for [`RunHandle::wait`](crate::RunHandle::wait): a caller that
    /// owns the taskflow may edit it on the next line, no job of the run and
    /// no copy of one holding a share of its storage by then. Two overlapping
    /// runs of the *same* taskflow are the caller's responsibility, and a
    /// taskflow with no source task runs nothing and returns `Ok`.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let executor = Arc::new(Executor::new(1));
    /// let counter = Arc::new(AtomicUsize::new(0));
    ///
    /// // One worker is enough: the worker that coruns it runs it.
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
    /// upstream's rethrow: the failure propagates on the calling worker, to be
    /// caught by the body that called `corun` or reported by the *outer* run.
    /// The payload keeps its identity, going to
    /// [`std::panic::resume_unwind`] rather than to a fresh `panic!`.
    /// [`Executor::try_corun`] is the same call with it as an `Err`.
    ///
    /// # Errors
    ///
    /// [`CorunError::NotAWorker`] if the calling thread is not a worker of
    /// this executor, which is the only error this shape returns.
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
    /// [`CorunError::Run`]`(`[`RunError::Panicked`]`)`, in the shape
    /// [`RunHandle::wait`](crate::RunHandle::wait) reports a failing run in.
    /// Everything else is that method's.
    ///
    /// ```
    /// use std::sync::Arc;
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
    /// panicked -- only the first such task, as with [`Executor::run`].
    pub fn try_corun(&self, taskflow: &Taskflow) -> Result<(), CorunError> {
        match self.corun_payload(taskflow)? {
            Some(payload) => Err(CorunError::Run(RunError::Panicked(payload))),
            None => Ok(()),
        }
    }

    /// Both shapes of corun, up to what is done with the payload: the whole of
    /// upstream's `corun` template, plus the topology upstream passes as
    /// `nullptr` and this port cannot, a queued job owning a share of its run.
    fn corun_payload(
        &self,
        taskflow: &Taskflow,
    ) -> Result<Option<Box<dyn Any + Send>>, CorunError> {
        let id = self.this_worker_id().ok_or(CorunError::NotAWorker)?;

        let run = InnerRun::new(taskflow.flow());
        let mut payload = None;
        if taskflow.flow().has_source_task() {
            // Counted as the `run` family counts its own: a worker skips the
            // steal loop while `num_topologies` is zero, so this run has to
            // be visible there before its sources are.
            self.shared.increment_topology();

            // This frame's stand-in for the `Worker&` upstream hands
            // `_corun_graph`; `WorkerThread::corun_view` says why the real
            // one cannot be reached from a task body.
            let mut worker = WorkerThread::corun_view(Arc::clone(&self.shared), id);

            // The run's own hold on the storage, taken out so that this
            // frame owns it: the graph goes under it as
            // `Shared::set_up_topology` puts a `run`'s graph under its own.
            let mut hold = run.take_hold();
            payload = corun_graph(
                &mut worker,
                &GraphOwner::run(&hold),
                run.topology(),
                run.anchor(),
            );

            // The `# Design` section's second wait, asked on the one
            // allocation that can answer it.
            worker.corun_until(|| Arc::get_mut(&mut hold).is_some());

            // Which makes this the last share of the taskflow's storage any
            // run of it holds, so the caller may edit the taskflow the
            // instant this call returns.
            drop(hold);

            // The decrement every run gets, here this frame's rather than a
            // tear-down's -- `InnerRun::end` is the rest of it -- and after
            // the waits, so that a returning call means an accounted run.
            self.shared.decrement_topology();
        }

        // Nothing to schedule means nobody to finish the run either, and a
        // graph with no source was never counted.
        run.end();

        // The anchor is where a failing task reports; the run's own slot is
        // the backstop, for a failure with no node to walk up from.
        Ok(payload.or_else(|| run.join()))
    }

    /// Keeps the calling worker in the work-stealing loop until `predicate`
    /// returns `true`.
    ///
    /// Upstream's `corun_until`, and the primitive under [`Executor::corun`]:
    /// how a worker waits for anything at all -- a run on another executor, a
    /// thread it spawned, a flag another task sets -- without parking.
    ///
    /// `predicate` is called on the calling worker, between jobs, and may run
    /// many times; it must not block, and it must eventually return `true`,
    /// this loop having nothing else that ends it -- one that never does keeps
    /// the worker running other people's jobs for ever. A panic in a job this
    /// loop happens to run goes to whatever waits for *that* job's graph.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicBool, Ordering};
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
                // finished -- the same promise `wait` makes. The frame
                // decrements the count itself, after its hold on the storage
                // has become unique and before it returns, where a `wait`er's
                // decrement is the last share's (#138). With one worker
                // nothing else can be running, so the only topology left is
                // the outer run that this task belongs to.
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

    /// The guarantee `Executor::corun` documents, asserted on the mechanism
    /// rather than on a race: when the call returns, the only share of the
    /// coruned taskflow's storage left is the taskflow's own.
    ///
    /// A composition makes it worth asserting -- a module task is a node that
    /// *preempts*, so its job is copied into two places no `finish` reaches,
    /// and before #112 those copies could outlive the anchor's counter
    /// reaching zero. `tests/corun.rs` is where the same claim is put under
    /// contention and where the numbers for before and after are; this is the
    /// one that says what is being counted.
    #[test]
    fn a_corun_leaves_no_share_of_the_taskflow_it_ran() {
        let executor = Arc::new(Executor::new(4));
        let counter = Arc::new(AtomicUsize::new(0));
        let target = Arc::new(chain(2, &counter));
        let shares = Arc::new(AtomicUsize::new(0));

        let mut driver = Taskflow::new();
        driver.emplace({
            let executor = Arc::clone(&executor);
            let counter = Arc::clone(&counter);
            let target = Arc::clone(&target);
            let shares = Arc::clone(&shares);
            move || {
                let mut coruned = chain(1, &counter);
                coruned.composed_of(Arc::clone(&target));

                executor.corun(&coruned).expect("a worker may corun");
                shares.store(Arc::strong_count(coruned.flow()), Ordering::Relaxed);

                // Which is the same statement from the caller's side: the
                // edit a `wait` is allowed, with nothing in between.
                coruned.clear();
                assert!(coruned.is_empty(), "the edit took");
            }
        });

        executor.run(&driver).wait().expect("no task panicked");
        assert_eq!(counter.load(Ordering::Relaxed), 3);
        assert_eq!(
            shares.load(Ordering::Relaxed),
            1,
            "a corun that has returned holds nothing of the taskflow it ran"
        );
        executor.wait_for_all();
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

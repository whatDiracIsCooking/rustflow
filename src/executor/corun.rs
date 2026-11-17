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
//! can drain it. [`WorkerThread::corun_until`] is that loop, and it says what
//! separates it from the worker's own.
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
//! # The inner run's topology
//!
//! Every queued job holds the address of the topology it belongs to, and
//! `executor::topology` sets out what keeps that address valid: the run's
//! topology must outlive every job that names it. For [`Executor::run`] the
//! owner is the [`RunHandle`](crate::RunHandle) it returns, which is also
//! where that argument has a hole worth knowing about -- `mem::forget` skips
//! the handle's blocking `Drop` and ends its borrow with the run still going
//! (issue #40).
//!
//! `corun` has no such exposure, and not because it is more careful: the
//! topology is owned by [`InnerRun`], a value this module creates on the corun
//! frame and never hands out. A caller cannot hold it, so a caller cannot
//! forget it, and the `Drop` that waits for the run is the only way the frame
//! can be left -- including an unwind through it.

use std::fmt;
use std::sync::Arc;

use crate::executor::Executor;
use crate::executor::run_handle::RunError;
use crate::executor::topology::Topology;
use crate::executor::worker::WorkerThread;
use crate::taskflow::Taskflow;

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
/// of its `_invoke` answers no for every node of it. Where that stops being
/// true is the corun upstream *does* pass an enclosing topology for -- a
/// subflow or a module task, which is M4, and which is also where this enum
/// is likeliest to gain a variant.
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

/// The topology of one [`Executor::corun`], owned for exactly as long as a
/// queued job can name it.
///
/// Upstream's local `NodeBase anchor` in `corun`, which plays the same part:
/// the thing the inner graph's nodes count down to, living on the corunning
/// worker's stack for the duration.
///
/// Boxed, as [`Executor::run`] boxes its own, so that the address every job
/// carries cannot be invalidated by the value moving. [`Drop`] is what makes
/// the ownership mean something, and it is the reason this is a type rather
/// than a local: an unwind through the corun frame -- which a scheduler bug
/// could cause, since a *task*'s panic is caught and recorded instead -- must
/// not free a topology while a job still names it.
struct InnerRun {
    /// The run. Nothing else owns it, and nothing outside this module can.
    topology: Box<Topology>,
}

impl InnerRun {
    /// A run that has not started.
    ///
    /// A corun runs its graph exactly once, so the run policy is the only one
    /// that says "do not go round again" from the start, with no callback to
    /// carry out: `run_n`/`run_until` are for a taskflow a caller runs, and
    /// the whole of this topology is a counter the corun frame waits on.
    fn new() -> Self {
        Self {
            topology: Box::new(Topology::new(|| true, || {})),
        }
    }

    /// The topology, to arm and to schedule from.
    fn topology(&self) -> &Topology {
        &self.topology
    }

    /// Blocks until the run is over and hands back what its first panicking
    /// task panicked with, if any.
    ///
    /// Called when the join counter has already reached zero, so what it
    /// actually waits for is the few instructions between the last job's
    /// counter decrement and the `Topology::finish` that releases this frame.
    /// That is also the point of calling it at all: `finish` is the finishing
    /// worker's last touch of the topology, so returning from here is what
    /// makes the topology safe to drop.
    fn join(&self) -> Option<Box<dyn std::any::Any + Send>> {
        self.topology.join()
    }
}

impl Drop for InnerRun {
    /// Waits for the run, as [`RunHandle`](crate::RunHandle)'s does.
    fn drop(&mut self) {
        // A payload nobody asked for is dropped here rather than re-raised,
        // for the reason the run handle gives: this may be running during an
        // unwind, and a panic from a `Drop` in an unwind aborts the process.
        drop(self.topology.join());
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
    /// # Errors
    ///
    /// [`CorunError::NotAWorker`] if the calling thread is not a worker of
    /// this executor, and [`CorunError::Run`] if a task of `taskflow`
    /// panicked. Only the first panicking task is reported, as with
    /// [`Executor::run`]; the rest of the graph still runs.
    pub fn corun(&self, taskflow: &Taskflow) -> Result<(), CorunError> {
        let id = self.this_worker_id().ok_or(CorunError::NotAWorker)?;

        let run = InnerRun::new();
        if taskflow.has_source_task() {
            let topology = run.topology();

            // Counted as the `run` family counts its own, and for the reason
            // that family's `start` gives: a worker skips the steal loop while
            // `num_topologies` is zero, so this run has to be visible there
            // before its sources are. The matching decrement is the one every
            // run gets, from the tear-down of its last job.
            self.shared.increment_topology();
            self.shared.set_up_topology(None, taskflow, topology);

            // Upstream's predicate, exactly: the run is over when its join
            // counter reaches zero. `join` below, not this read, is what then
            // makes the topology safe to drop.
            WorkerThread::corun_view(Arc::clone(&self.shared), id)
                .corun_until(|| topology.outstanding() == 0);
        } else {
            // Nothing to schedule, so nobody will finish the run: this frame
            // does, as `Executor::run` does for the same graph. Nothing was
            // counted either, so there is nothing to decrement.
            run.topology().finish();
        }

        match run.join() {
            Some(payload) => Err(CorunError::Run(RunError::Panicked(payload))),
            None => Ok(()),
        }
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
    /// A panic in a job this loop happens to run is recorded on *that* job's
    /// own run and reported to whoever waits for it, so there is nothing for
    /// this method to report but the one failure below.
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
    /// same shape `wait` reports it, and does not escape onto the worker.
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
                let error = executor.corun(&inner).expect_err("the task panicked");
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

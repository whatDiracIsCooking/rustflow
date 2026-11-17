//! Translated from `unittests/test_cancellation.cpp` (Taskflow v4.1.0).
//!
//! Upstream's file is nine cases over one shape: submit a graph far too long
//! to finish, cancel it, and check that it stopped early and still completed.
//! Four of them are gated here -- `EmptyFuture`, `Future`,
//! `BasicCancellation` and `MultipleCancellations` -- and the other five need
//! machinery this port does not have yet, so they are named rather than
//! half-ported: `CancelInfiniteLoop` and `CancelFromAnother` need condition
//! tasks (issue #33), `CancelSubflow` and `CancelComposition` need subflows
//! and module tasks (M4), and `CancelFromAsync` needs async tasks (M5).
//!
//! Three adaptations, and the first is the only one that changes what is
//! being asserted:
//!
//! - **`EmptyFuture` becomes a token whose run has completed.** Upstream
//!   default-constructs a `tf::Future<void>`, which holds no topology at all,
//!   and checks that `cancel()` reports `false`. There is no such value here:
//!   a [`RunHandle`] is only ever handed out by a `run`, which is the point of
//!   it borrowing what it runs. The same question -- does cancelling something
//!   there is nothing to cancel report `false`? -- is asked of a
//!   [`Canceller`](rustflow::Canceller) whose run has finished, which is the
//!   state upstream's failed `weak_ptr::lock()` describes.
//! - **`fu.wait_for(0ms) == std::future_status::ready` becomes one poll of the
//!   handle.** A `RunHandle` is a [`Future`], so asking whether a run is
//!   complete *without* waiting for it is a single `poll`; see
//!   [`poll_once`].
//! - **The shared counter is an `AtomicUsize`** where upstream's is an
//!   `std::atomic<int>&` captured by reference. Task bodies here are
//!   `Send + 'static`, so the sharing is spelled as an [`Arc`].
//!
//! The sleeping task bodies and the task counts are upstream's, down to the
//! `counter < 10000` bound: 10000 tasks of 100ms each is a graph no pool
//! finishes in the time the case takes, so a run that *has* finished by the
//! time the counter is read is a run that was not cancelled.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

use rustflow::prelude::*;
use rustflow::{Executor, RunError, RunHandle};

/// Upstream's task count, and the bound every cancellation case checks the
/// counter against.
const TASKS: usize = 10_000;

/// How long one of upstream's cancellable tasks sleeps for.
const SLEEP: Duration = Duration::from_millis(100);

/// Upstream's cancellable graph: `TASKS` independent tasks, each sleeping and
/// then bumping `counter`.
///
/// Every task is a source, as upstream's are, so the whole graph is queued the
/// moment the run starts and a cancellation has the maximum number of jobs
/// left to skip.
fn sleepers(counter: &Arc<AtomicUsize>) -> Taskflow {
    let mut taskflow = Taskflow::new();
    for _ in 0..TASKS {
        let counter = Arc::clone(counter);
        taskflow.emplace(move || {
            thread::sleep(SLEEP);
            counter.fetch_add(1, Ordering::Relaxed);
        });
    }
    taskflow
}

/// A graph of `tasks` independent tasks that only bump `counter`.
fn bumpers(counter: &Arc<AtomicUsize>, tasks: usize) -> Taskflow {
    let mut taskflow = Taskflow::new();
    for _ in 0..tasks {
        let counter = Arc::clone(counter);
        taskflow.emplace(move || {
            counter.fetch_add(1, Ordering::Relaxed);
        });
    }
    taskflow
}

/// Upstream's `fu.wait_for(std::chrono::milliseconds(0))`: is this run
/// complete, asked without waiting for it.
///
/// A no-op waker is the whole runtime this needs -- there is nothing to wake,
/// since the answer is read on the spot.
fn poll_once(run: &mut RunHandle<'_>) -> Poll<Result<(), RunError>> {
    let mut context = Context::from_waker(Waker::noop());
    Pin::new(run).poll(&mut context)
}

/// A callback that counts the runs it was handed to.
fn count_callback(callbacks: &Arc<AtomicUsize>) -> impl FnOnce() + Send + 'static {
    let callbacks = Arc::clone(callbacks);
    move || {
        callbacks.fetch_add(1, Ordering::Relaxed);
    }
}

/// Upstream's `Future`: the uncancelled baseline, which is what makes the
/// cancellation cases mean anything -- 100 tasks, waited for, all 100 run.
#[test]
fn future() {
    let executor = Executor::new(4);
    let counter = Arc::new(AtomicUsize::new(0));
    let taskflow = bumpers(&counter, 100);

    executor.run(&taskflow).wait().expect("no task panicked");

    assert_eq!(counter.load(Ordering::Relaxed), 100);
    assert_eq!(executor.num_topologies(), 0);
}

/// Upstream's `EmptyFuture`, as the module docs describe: cancelling a run
/// that has completed reports `false`, because there is nothing left to
/// cancel. Upstream's is a failed `weak_ptr::lock()`; here the run is one that
/// has finished, the state the token is for.
#[test]
fn empty_future() {
    let executor = Executor::new(4);
    let counter = Arc::new(AtomicUsize::new(0));
    let taskflow = bumpers(&counter, 100);

    let run = executor.run(&taskflow);
    let canceller = run.canceller();
    assert!(run.cancel(), "the run had not finished");
    run.wait().expect("a cancelled run completes normally");

    assert!(
        !canceller.cancel(),
        "a run that has completed has nothing to cancel"
    );
    // The handle's own `cancel` answers the same for a run that was over
    // before it began, which is the other way to have nothing to cancel.
    assert!(!executor.run_n(&taskflow, 0).cancel(), "no round to cancel");
}

/// Upstream's `BasicCancellation`, both rounds: a graph of 10000 sleeping
/// tasks, cancelled, then waited for.
///
/// The second round is `run_n(.., 100)`, and it is the one that pins down the
/// cancellation term in the re-run decision: a hundred rounds of this graph is
/// a million task-seconds, so a cancelled run that still consulted its
/// predicate would not come back inside the case's timeout, let alone with the
/// counter under one round's worth of work.
#[test]
fn basic_cancellation() {
    let executor = Executor::new(4);
    let counter = Arc::new(AtomicUsize::new(0));
    let taskflow = sleepers(&counter);

    // A new round, as upstream puts it.
    counter.store(0, Ordering::Relaxed);
    let run = executor.run(&taskflow);
    assert!(run.cancel(), "the run had not finished");
    run.wait().expect("a cancelled run completes normally");
    assert!(
        counter.load(Ordering::Relaxed) < TASKS,
        "the whole graph ran, so nothing was cancelled"
    );

    // A new round.
    counter.store(0, Ordering::Relaxed);
    let run = executor.run_n(&taskflow, 100);
    assert!(run.cancel(), "the run had not finished");
    run.wait().expect("a cancelled run completes normally");
    assert!(
        counter.load(Ordering::Relaxed) < TASKS,
        "a cancelled run went round again"
    );

    assert_eq!(executor.num_topologies(), 0);
}

/// Upstream's `MultipleCancellations`: four graphs of 10000 sleeping tasks on
/// one executor, all four cancelled, then `wait_for_all`.
///
/// The four polls at the end are upstream's four `wait_for(0ms) == ready`
/// checks, and they are a slightly different statement here than there.
/// Upstream fulfils a run's promise and *then* decrements its topology count;
/// this port decrements first and releases the waiter last, deliberately --
/// `Shared::tear_down_topology` says why -- so what `wait_for_all` returning
/// guarantees here is the count, and a poll could in principle be beaten by
/// the handful of instructions between the last run's decrement and its
/// `Topology::finish`. The assertion is kept as upstream wrote it rather than
/// softened into a wait, because what it is really for -- a cancelled run
/// left with work still scheduled -- is a failure of seconds, not of
/// instructions.
#[test]
fn multiple_cancellations() {
    let executor = Executor::new(4);
    let counter = Arc::new(AtomicUsize::new(0));
    let taskflows = [
        sleepers(&counter),
        sleepers(&counter),
        sleepers(&counter),
        sleepers(&counter),
    ];

    counter.store(0, Ordering::Relaxed);
    let mut runs: Vec<RunHandle<'_>> = taskflows.iter().map(|tf| executor.run(tf)).collect();
    assert_eq!(executor.num_topologies(), 4, "four runs are in flight");

    for (run, which) in runs.iter().zip(1..) {
        assert!(run.cancel(), "run {which} had not finished");
    }

    executor.wait_for_all();
    assert!(
        counter.load(Ordering::Relaxed) < TASKS,
        "four whole graphs ran, so nothing was cancelled"
    );

    for (run, which) in runs.iter_mut().zip(1..) {
        assert!(
            matches!(poll_once(run), Poll::Ready(Ok(()))),
            "run {which} was not complete once the executor was idle"
        );
    }
    assert_eq!(executor.num_topologies(), 0);
}

/// Cancelling stops the scheduling and nothing else: the on-finish callback
/// of a cancelled run still fires, and the next run queued behind it on the
/// same taskflow still starts -- which is what makes the second handle here
/// able to complete at all, since the first run's tear-down is what starts it.
///
/// Not an upstream case. Upstream's suite never cancels a run with another
/// queued behind it, and the two promises are the ones most easily lost by
/// short-circuiting the tear-down instead of the re-run decision.
#[test]
fn a_cancelled_run_still_calls_back_and_starts_the_next_queued_run() {
    let executor = Executor::new(4);
    let counter = Arc::new(AtomicUsize::new(0));
    let callbacks = Arc::new(AtomicUsize::new(0));
    let taskflow = sleepers(&counter);

    let first = executor.run_with(&taskflow, count_callback(&callbacks));
    let second = executor.run_with(&taskflow, count_callback(&callbacks));
    assert!(first.cancel(), "the first run had not finished");
    assert!(second.cancel(), "the second run had not started");

    first.wait().expect("a cancelled run completes normally");
    second.wait().expect("a cancelled run completes normally");

    assert_eq!(
        callbacks.load(Ordering::Relaxed),
        2,
        "a cancelled run still calls back"
    );
    assert!(counter.load(Ordering::Relaxed) < TASKS);
    assert_eq!(executor.num_topologies(), 0);
}

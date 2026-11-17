//! Translated from `unittests/test_cancellation.cpp` (Taskflow v4.1.0).
//!
//! Upstream's file is nine cases over one shape: submit a graph far too long
//! to finish, cancel it, and check that it stopped early and still completed.
//! Six of them are here. Four arrived with the cancellation issue (#29) --
//! `EmptyFuture`, `Future`, `BasicCancellation` and `MultipleCancellations` --
//! and `CancelInfiniteLoop` and `CancelFromAnother` with issue #33, once there
//! were condition tasks to loop with.
//!
//! The remaining three need machinery this port does not have yet, so they are
//! named rather than half-ported: `CancelSubflow` and `CancelComposition` need
//! subflows and module tasks (M4), and `CancelFromAsync` needs async tasks
//! (M5). Upstream's commented-out `CancelLinearChain` is left commented out.
//!
//! Four adaptations, and the first two are the ones that change what is
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
//! - **`fu.wait_for(0ms) == std::future_status::ready` becomes a wait.** A
//!   `RunHandle` is a [`Future`], so asking whether a run is complete
//!   *without* waiting for it is a single `poll` -- see [`poll_once`] -- and
//!   `cancel_from_another` asks exactly that, where `Pending` is the answer
//!   by construction. Upstream's four *ready* polls in
//!   `MultipleCancellations` are a different statement: they read "the
//!   executor being idle means every run is complete", which is true upstream
//!   and deliberately false here, because `Shared::tear_down_topology`
//!   decrements the topology count before it releases the run's waiter.
//!   `multiple_cancellations` says what is waited for in their place.
//! - **The shared counter is an `AtomicUsize`** where upstream's is an
//!   `std::atomic<int>&` captured by reference. Task bodies here are
//!   `Send + 'static`, so the sharing is spelled as an [`Arc`].
//! - **A task cancels another run through a token, not a captured future.**
//!   `cancel_from_another` says why, and it is the reason
//!   [`Canceller`](rustflow::Canceller) exists.
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
    // What this cancel *reports* is deliberately not asserted, and is not to
    // be "tightened" back into an `assert!` (#67): a hundred `fetch_add`s
    // across four workers finish in microseconds, so this run may well be
    // over by the time the call lands -- and `cancel` then reports `false`,
    // which is its contract, not a fault. Nothing is lost by dropping that
    // assertion, because upstream's `EmptyFuture` makes no such claim either:
    // it cancels a default-constructed `tf::Future` and checks only the
    // `false`. The call itself stays, so that what the two assertions below
    // describe is a run which was asked to cancel and completed anyway,
    // whichever side of the race it fell on.
    run.cancel();
    run.wait().expect("a cancelled run completes normally");

    // Race-free, unlike the call above, and for a reason in the library
    // rather than in the timings: `Topology::finish` marks the run finished
    // *before* it releases a waiter under the completion lock, so a `cancel`
    // ordered after a returned `wait` can only report `false`.
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
/// The four waits at the end are upstream's four `wait_for(0ms) == ready`
/// checks, and they have to be waits here. Upstream fulfils a run's promise
/// and *then* decrements its topology count, so its polls assert a real
/// implication: the executor being idle means every run is complete. This
/// port decrements first and releases the waiter last, deliberately --
/// `Shared::tear_down_topology` says why -- so that implication is one it
/// does not offer, and a poll here could be beaten by the handful of
/// instructions between a run's decrement and its `Topology::finish`. Four
/// polls are four chances to lose that race, which is #67's reasoning applied
/// to this case: an assertion that cannot hold is not made sounder by the
/// window being narrow.
///
/// So what is asserted is that each run does complete, and the teeth are
/// where they already were -- `counter < TASKS`, read once the executor is
/// idle, which a cancelled run left with work still scheduled fails by a
/// factor of four.
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
    let runs: Vec<RunHandle<'_>> = taskflows.iter().map(|tf| executor.run(tf)).collect();
    assert_eq!(executor.num_topologies(), 4, "four runs are in flight");

    for (run, which) in runs.iter().zip(1..) {
        assert!(run.cancel(), "run {which} had not finished");
    }

    executor.wait_for_all();
    assert!(
        counter.load(Ordering::Relaxed) < TASKS,
        "four whole graphs ran, so nothing was cancelled"
    );

    for (run, which) in runs.into_iter().zip(1..) {
        run.wait()
            .unwrap_or_else(|error| panic!("run {which} failed: {error}"));
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

/// Upstream's `CancelInfiniteLoop`: a hundred condition tasks that each
/// precede themselves, cancelled, and then waited for.
///
/// Nothing here terminates on its own, so this is the case that says
/// cancellation reaches the conditional branch at all: a `cancel` consulted
/// only at the re-run decision would leave a hundred weak edges rescheduling
/// each other for ever and this test would hang rather than fail. The pairs
/// are independent, so a four-worker pool has every one of them live at once.
#[test]
fn cancel_infinite_loop() {
    let executor = Executor::new(4);
    let mut taskflow = Taskflow::new();

    for _ in 0..100 {
        let a = taskflow.emplace(|| {});
        let b = taskflow.emplace_condition(|| 0);
        taskflow.task(a).precede([b]);
        taskflow.task(b).precede([b]);
    }

    let run = executor.run(&taskflow);
    assert!(run.cancel(), "an endless run cannot have finished");
    run.wait().expect("a cancelled run completes normally");

    assert_eq!(executor.num_topologies(), 0);
}

/// Upstream's `CancelFromAnother`: a task of a second taskflow cancels the
/// first run, which is a single endless loop, and the first run's wait is
/// still pending when it does.
///
/// Upstream's task captures the `tf::Future` by reference. A task body here is
/// `FnMut() + Send + 'static`, so it cannot capture a [`RunHandle`] -- the
/// handle borrows the executor and the taskflow -- and a
/// [`Canceller`](rustflow::Canceller), which is `Clone + Send + Sync +
/// 'static`, is the part of it that can. That is the whole reason the token
/// exists; see `RunHandle::canceller`.
///
/// Upstream arranges the ordering with `fu.wait_for(100ms) == timeout`, which
/// is a hopeful 100 ms rather than a guarantee. No wait is needed here and
/// none is used: the first graph is `a -> b`, `b -> b`, which cannot terminate
/// by itself, so a poll of its handle is `Pending` by construction however
/// slow or fast the pool is, and the poll happens on this thread strictly
/// before the second run is submitted. The cancel is therefore observed to be
/// what ends the first run, with no race to lose.
#[test]
fn cancel_from_another() {
    let executor = Executor::new(4);
    let mut taskflow = Taskflow::new();

    // Upstream's single infinite loop.
    let a = taskflow.emplace(|| {});
    let b = taskflow.emplace_condition(|| 0);
    taskflow.task(a).precede([b]);
    taskflow.task(b).precede([b]);

    let mut run = executor.run(&taskflow);
    assert!(
        poll_once(&mut run).is_pending(),
        "an endless run cannot have finished"
    );

    // Upstream's `another`, whose one task cancels the run above. The
    // assertion inside it is upstream's `REQUIRE(fu.cancel() == true)`, and
    // the `expect` below is what reports it: a panicking task body is stored
    // on its own topology and raised from `wait`.
    let mut another = Taskflow::new();
    let canceller = run.canceller();
    another.emplace(move || {
        assert!(canceller.cancel(), "the endless run had already finished");
    });
    executor
        .run(&another)
        .wait()
        .expect("a task of the second taskflow panicked");

    run.wait().expect("a cancelled run completes normally");
    assert_eq!(executor.num_topologies(), 0);
}

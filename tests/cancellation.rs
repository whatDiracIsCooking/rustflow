//! Translated from `unittests/test_cancellation.cpp` (Taskflow v4.1.0).
//!
//! Upstream's file is nine cases over one shape: submit a graph far too long
//! to finish, cancel it, and check that it stopped early and still completed.
//! Eight of them are here. Four arrived with the cancellation issue (#29) --
//! `EmptyFuture`, `Future`, `BasicCancellation` and `MultipleCancellations` --
//! `CancelInfiniteLoop` and `CancelFromAnother` with issue #33, once there
//! were condition tasks to loop with, and `CancelSubflow` and
//! `CancelComposition` with #55, once there were subflows and module tasks to
//! cancel *through*. Those last two are at the end of the file.
//!
//! The one case still named rather than half-ported is `CancelFromAsync`,
//! which needs async tasks (M5). Upstream's commented-out `CancelLinearChain`
//! is left commented out.
//!
//! Five adaptations, and the first three are the ones that change what is
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
//! - **`CancelComposition` asserts completion where upstream asserts
//!   nothing.** Upstream counts the cancels that reported `true` and requires
//!   `success <= N` -- a count of at most `N` things being at most `N`, which
//!   holds however the race fell and would hold if `cancel` were
//!   `return false`. What a hundred rounds of a hundred runs can be asked is
//!   that every handle completes and the executor is left with nothing in
//!   flight, and that is what `cancel_composition` asserts. How many of the
//!   cancels won is deliberately not asserted: runs of one taskflow are
//!   serialized, so a round may be over before the cancels land, and a lower
//!   bound on that number would be an assertion about the scheduler's speed.
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
//!
//! Three cases are the port's own, and the last two are there because that
//! bound is weaker than it looks once a nested graph is involved.
//! `cancel_subflow` and `cancel_composition` both cancel microseconds after
//! submitting, so on this box the cancel wins long before a subflow body or a
//! module node has been dequeued: the bound is satisfied by a run in which
//! *nothing* nested ever started, which a port that read the run's own bit and
//! never looked at a parent would pass. The two
//! `cancelling_a_run_skips_..` cases arrange the other order with a handshake
//! -- a body reports that it is running and waits for the cancel to have
//! happened before its children are scheduled -- and then ask for a count of
//! zero rather than a bound. Each says so in its own docs, and neither waits
//! without a deadline; [`spin_until`] is why.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

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

/// Upstream's `CancelSubflow`: the sleeping graph again, but one level down --
/// a hundred subflow tasks, each emplacing a hundred of the sleepers above,
/// half of them joined explicitly and half left to the implicit join.
///
/// Upstream's `if(i % 2) sf.join();`, and the alternation is the point: the
/// two joins reach the children by different routes -- an explicit one coruns
/// the subgraph from inside the body, an implicit one preempts the node and
/// lets the last child resume it -- and a cancellation has to stop the
/// children either way. Both routes schedule the subgraph under the *node*
/// rather than under the run, so a job of it is skipped by the ancestor half
/// of `Job::is_parent_cancelled` rather than by the topology's own bit; a port
/// that read only the run would leave ten thousand sleepers to finish.
///
/// The counter bound is upstream's and it is the same arithmetic the cases
/// above rest on: ten thousand children at 100 ms each is far more work than
/// the case has time for, so a counter under `TASKS` is a run that stopped
/// early.
#[test]
fn cancel_subflow() {
    let executor = Executor::new(4);
    let counter = Arc::new(AtomicUsize::new(0));
    let taskflow = sleeping_subflows(&counter);

    // A new round, as upstream puts it.
    counter.store(0, Ordering::Relaxed);
    let run = executor.run(&taskflow);
    assert!(run.cancel(), "the run had not finished");
    run.wait().expect("a cancelled run completes normally");
    assert!(
        counter.load(Ordering::Relaxed) < TASKS,
        "every child of every subflow ran, so nothing was cancelled"
    );

    // A new round: upstream's three overlapping runs of the same taskflow,
    // which queue behind one another and are cancelled in one go. The first
    // cannot have finished -- it is 10000 sleeping tasks -- and the other two
    // cannot have *started*, so all three cancels report `true` without a
    // race to lose.
    counter.store(0, Ordering::Relaxed);
    let runs: Vec<RunHandle<'_>> = (0..3).map(|_| executor.run(&taskflow)).collect();
    for (run, which) in runs.iter().zip(1..) {
        assert!(run.cancel(), "run {which} had not finished");
    }
    for (run, which) in runs.into_iter().zip(1..) {
        run.wait()
            .unwrap_or_else(|error| panic!("run {which} failed: {error}"));
    }
    assert!(
        counter.load(Ordering::Relaxed) < TASKS,
        "three cancelled runs did a whole run's worth of work"
    );

    assert_eq!(executor.num_topologies(), 0);
}

/// Upstream's cancellable graph one level down: `SUBFLOWS` subflow tasks, each
/// emplacing `CHILDREN` of [`sleepers`]' children, alternately joined
/// explicitly and left to the implicit join.
fn sleeping_subflows(counter: &Arc<AtomicUsize>) -> Taskflow {
    /// Upstream's outer hundred.
    const SUBFLOWS: usize = 100;
    /// Upstream's inner hundred, so that the two together are [`TASKS`]
    /// sleeping children.
    const CHILDREN: usize = TASKS / SUBFLOWS;

    let mut taskflow = Taskflow::new();
    for which in 0..SUBFLOWS {
        let counter = Arc::clone(counter);
        taskflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
            for _ in 0..CHILDREN {
                let counter = Arc::clone(&counter);
                subflow.emplace(move || {
                    thread::sleep(SLEEP);
                    counter.fetch_add(1, Ordering::Relaxed);
                });
            }

            // Upstream's `if(i % 2) sf.join();`, with the even half left to
            // the implicit join the dispatch arm performs.
            if which % 2 == 1 {
                subflow.join().expect("a fresh subflow is joinable");
            }
        });
    }
    taskflow
}

/// Upstream's `CancelComposition`: four taskflows composed into a tree three
/// modules deep, run a hundred times a round for a hundred rounds, with every
/// one of those runs cancelled.
///
/// What this asks that the cases above do not is whether a cancellation
/// survives *composition*. A module node schedules its target's graph under
/// itself and preempts -- the whole of `executor::module` -- so the jobs of a
/// composed graph are two or three levels below the run that was cancelled,
/// and a cancelled module task has to both skip its target and still release
/// the successors waiting on it. Ten thousand runs is upstream's way of
/// reaching every interleaving of that between a cancel and a module node
/// being entered, and the shape of the assertion is why it can afford them:
/// each run is sixteen tasks and finishes whether or not the cancel lands, so
/// what is checked is that it *finishes* -- a module left preempted, or a
/// target graph scheduled under a node whose run was already torn down, would
/// hang this case rather than fail it.
///
/// The module docs say why upstream's `REQUIRE(success <= N)` is not what is
/// asserted here.
#[test]
fn cancel_composition() {
    /// Upstream's `r`: how many rounds.
    const ROUNDS: usize = 100;
    /// Upstream's `N`: how many runs of `f4` a round submits and cancels.
    const RUNS: usize = 100;

    let executor = Executor::new(4);

    // Upstream's `f1`: two independent tasks.
    let f1 = Arc::new({
        let mut f1 = Taskflow::with_name("F1");
        f1.emplace(|| {});
        f1.emplace(|| {});
        f1
    });

    // Upstream's `f2`: `f2A -> f2C`, `f2B -> f2C`, and a module task of `f1`
    // with no edges at all -- a second, independent source of the graph.
    let f2 = Arc::new({
        let mut f2 = Taskflow::with_name("F2");
        let f2a = f2.emplace(|| {});
        let f2b = f2.emplace(|| {});
        let f2c = f2.emplace(|| {});
        f2.task(f2a).precede([f2c]);
        f2.task(f2b).precede([f2c]);
        f2.composed_of(Arc::clone(&f1));
        f2
    });

    // Upstream's `f3`: a module task of `f2` and one regular task.
    let f3 = Arc::new({
        let mut f3 = Taskflow::with_name("F3");
        f3.composed_of(Arc::clone(&f2));
        f3.emplace(|| {});
        f3
    });

    // Upstream's `f4`: the module task of `f3` preceding one of `f2`. So `f2`
    // is composed twice over -- once through `f3` and once directly -- and
    // the edge is what makes that safe: a target's graph carries the node
    // state of the run walking it, so the two module tasks must not be in
    // flight at the same time. Upstream's shape has them strictly ordered,
    // and runs of one taskflow are serialized, so no two entries of `f2`
    // ever overlap.
    let mut f4 = Taskflow::with_name("F4");
    let module_of_f3 = f4.composed_of(Arc::clone(&f3));
    let module_of_f2 = f4.composed_of(Arc::clone(&f2));
    f4.task(module_of_f3).precede([module_of_f2]);

    for round in 1..=ROUNDS {
        let runs: Vec<RunHandle<'_>> = (0..RUNS).map(|_| executor.run(&f4)).collect();

        // Upstream's `success += (fu.cancel() ? 1 : 0)`. The answers are not
        // collected, for the reason the module docs give: a round of
        // sixteen-task runs may be over before the cancels land, so the only
        // true statement about the count is upstream's vacuous one.
        for run in &runs {
            run.cancel();
        }

        executor.wait_for_all();
        for (run, which) in runs.into_iter().zip(1..) {
            run.wait()
                .unwrap_or_else(|error| panic!("round {round}, run {which} failed: {error}"));
        }
        assert_eq!(
            executor.num_topologies(),
            0,
            "round {round}: a run was left in flight"
        );
    }
}

/// How long a handshake waits for the other side before it gives up.
const GIVE_UP_AFTER: Duration = Duration::from_secs(120);

/// Spins until `flag` is set, failing rather than hanging if it never is.
///
/// The deadline-asserting spin of `executor::run_handle`'s own cases, which
/// `tests/runtimes.rs` carries a copy of too: a defect that stopped a
/// cancellation from reaching a nested graph should fail the case that is
/// waiting for it rather than hang the suite.
fn spin_until(what: &str, flag: &AtomicBool) {
    let deadline = Instant::now() + GIVE_UP_AFTER;
    while !flag.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        thread::yield_now();
    }
}

/// Spins until `counter` reaches `target`, failing rather than hanging.
///
/// [`spin_until`] over a count rather than a flag, for the handshakes that
/// wait on more than one body having arrived.
fn spin_until_count(what: &str, counter: &AtomicUsize, target: usize) {
    let deadline = Instant::now() + GIVE_UP_AFTER;
    while counter.load(Ordering::Acquire) < target {
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        thread::yield_now();
    }
}

/// Not an upstream case, and the one that gives `cancel_subflow` its teeth: a
/// cancellation that lands while a subflow body is *between* emplacing its
/// children and joining them skips every one of those children.
///
/// Upstream's own case cannot say that, and neither can the port of it above.
/// A hundred subflow tasks are queued and the cancel lands microseconds later,
/// so on this box nothing has started by then: `counter < TASKS` is satisfied
/// by a run in which no child was ever scheduled, which is a weaker statement
/// than the one the case is named for and is satisfied by a port that reads
/// the run's bit and nothing else. Two bodies and a handshake make the
/// stronger statement checkable, and `counter == 0` rather than a bound is
/// what makes it worth checking: the children are scheduled *after* the
/// cancel is known to have happened, under a node rather than under the run,
/// so each is skipped by the ancestor walk of `Job::is_parent_cancelled` or
/// not at all.
///
/// Both joins are here for the reason `cancel_subflow` alternates them: the
/// explicit one schedules the subgraph from inside the body, the implicit one
/// after it returns, and a cancellation has to reach the children either way.
#[test]
fn cancelling_a_run_skips_the_children_of_a_subflow_that_has_not_joined() {
    /// One subflow task per join kind.
    const BODIES: usize = 2;
    /// Enough children that a port which ran them anyway would be obvious in
    /// the counter rather than in the clock.
    const CHILDREN: usize = 100;

    let executor = Executor::new(4);
    let counter = Arc::new(AtomicUsize::new(0));
    // Bumped by each body once its children are emplaced, and read by this
    // thread before it cancels.
    let waiting = Arc::new(AtomicUsize::new(0));
    // Set by this thread once the run is cancelled, and waited for by both
    // bodies before they let their children be scheduled.
    let cancelled = Arc::new(AtomicBool::new(false));

    let mut taskflow = Taskflow::new();
    for body in 0..BODIES {
        let counter = Arc::clone(&counter);
        let waiting = Arc::clone(&waiting);
        let cancelled = Arc::clone(&cancelled);
        taskflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
            for _ in 0..CHILDREN {
                let counter = Arc::clone(&counter);
                subflow.emplace(move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                });
            }

            waiting.fetch_add(1, Ordering::Release);
            // Spinning rather than sleeping, because this is a worker thread
            // and the thread it is waiting for is not one: the main thread
            // below cancels and sets the flag, so this wait cannot starve the
            // pool of anything the cancel needs.
            spin_until("the run to be cancelled", &cancelled);

            if body % 2 == 1 {
                subflow.join().expect("a fresh subflow is joinable");
            }
        });
    }

    let run = executor.run(&taskflow);
    spin_until_count("both subflow bodies to emplace", &waiting, BODIES);
    assert!(
        run.cancel(),
        "both bodies are still waiting, so the run is live"
    );
    cancelled.store(true, Ordering::Release);

    run.wait().expect("a cancelled run completes normally");
    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "a child was scheduled under a cancelled run and ran anyway"
    );
    assert_eq!(executor.num_topologies(), 0);
}

/// Not an upstream case, and `cancel_composition`'s teeth for the same reason:
/// a cancellation that lands while a *module* task is mid-flight skips the
/// rest of the graph it is running.
///
/// Upstream's composition case runs ten thousand sixteen-task runs and asserts
/// that they all complete, which is a statement about tear-down rather than
/// about cancellation: a run cancelled before its first module node is
/// dequeued never enters a target graph at all. Here the target's first task
/// reports that it is running and waits, so the cancel is known to have
/// happened while the module node is preempted and its graph is in flight --
/// and the one task of that graph still to come must not run.
#[test]
fn cancelling_a_run_skips_the_rest_of_a_module_graph_that_is_mid_flight() {
    let executor = Executor::new(4);
    let counter = Arc::new(AtomicUsize::new(0));
    let waiting = Arc::new(AtomicBool::new(false));
    let cancelled = Arc::new(AtomicBool::new(false));

    let target = Arc::new({
        let mut target = Taskflow::new();
        let first = {
            let waiting = Arc::clone(&waiting);
            let cancelled = Arc::clone(&cancelled);
            target.emplace(move || {
                waiting.store(true, Ordering::Release);
                spin_until("the run to be cancelled", &cancelled);
            })
        };
        let second = {
            let counter = Arc::clone(&counter);
            target.emplace(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            })
        };
        target.task(first).precede([second]);
        target
    });

    let mut taskflow = Taskflow::new();
    let module = taskflow.composed_of(Arc::clone(&target));
    let after = {
        let counter = Arc::clone(&counter);
        taskflow.emplace(move || {
            counter.fetch_add(1, Ordering::Relaxed);
        })
    };
    taskflow.task(module).precede([after]);

    let run = executor.run(&taskflow);
    spin_until("the composed graph to start", &waiting);
    assert!(run.cancel(), "the module's first task is still waiting");
    cancelled.store(true, Ordering::Release);

    run.wait().expect("a cancelled run completes normally");
    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "the rest of the composed graph, or the task after the module, ran anyway"
    );
    assert_eq!(executor.num_topologies(), 0);
}

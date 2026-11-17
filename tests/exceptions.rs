//! Translated from `unittests/test_exceptions.cpp` (Taskflow v4.1.0).
//!
//! The cases ported here are the four the panic-cancellation issue names, and
//! they are the four of that file that need nothing but a static graph and a
//! conditioner: `Exception.StaticTask`, `Exception.ConditionTask` and
//! `Exception.MultiConditionTask`, each a linear chain `A..F` whose `C` fails,
//! and `Exception.ThreadSafety`, a thousand tasks that all fail at once. All
//! four run at 1..=4 workers, which is upstream's own sweep.
//!
//! The rest of that file is deferred rather than half-ported, because every
//! one of its remaining cases needs a node kind this port does not run yet:
//! the `Runtime` and `RuntimeCorun1..2` cases, the async family (`Async`,
//! `DependentAsync`, `RuntimeAsync`), `Semaphore`, `TaskGroup`, the
//! `Probe1..4` cases and the algorithm ones (`Algorithm`,
//! `NestedAlgorithm`). They arrive with M4 and later, together with the
//! runtime anchors those cases wait on.
//!
//! Two of the subflow family are here as of subflow tasking (#50), at the end
//! of the file: `Exception.SubflowTask` and `Exception.JoinedSubflow1`, which
//! are the cases about where a failure *inside* a subgraph is delivered.
//! `JoinedSubflow2` and `3`, `NestedSubflow` and `NestedSubflow2` are #55's,
//! with the rest of that issue's exception work.
//!
//! Two more are here as of the anchor walk (#48): `Exception.ExecutorCorun1`
//! -- its static half, the subflow half being #55's -- and
//! `Exception.ExecutorCorun2`, which are the cases about a failure inside a
//! *nested* graph being delivered to whoever is waiting for that graph rather
//! than to the run. They are at the end of the file, after the four above.
//!
//! And `Exception.ModuleTask` is here as of module tasks (#49), at the very
//! end: a failure inside a composed taskflow, which that walk delivers to the
//! composing run because a module node is no anchor. Its commented-out second
//! half, the same composition with a subflow inside it, waits on #55 with the
//! rest of the subflow family.
//!
//! Three adaptations, and the first is the only one that changes what is
//! asserted:
//!
//! - **A panic where upstream throws.** Upstream's failing body throws
//!   `std::runtime_error("x")` and its `REQUIRE_THROWS_WITH_AS` checks the
//!   type and the message on the way out of `get()`. A task body here panics
//!   with `"x"` instead, and the equivalent check is that
//!   [`RunHandle::wait`](rustflow::RunHandle::wait) hands back
//!   [`RunError::Panicked`] whose payload downcasts to that `&str` -- see
//!   that variant for why a `Result` stands in for a rethrow.
//! - **`counter == 2` is the whole of the cancellation claim.** Upstream's
//!   chain is deliberately serial, so the two tasks before the failing one
//!   have run and the three after it have not. That is what makes the number
//!   deterministic at any worker count, and it is the one shape in which
//!   "tasks not yet scheduled are skipped" has a single right answer rather
//!   than a range.
//! - **The thread-safety case counts payloads.** Upstream's only assertion is
//!   that *a* `"x"` comes back from a thousand simultaneous throws, since what
//!   it is really checking is that a thread sanitizer sees no race on the
//!   `exception_ptr`. The payload here is a type that counts its own drops, so
//!   the case can assert the sharper thing directly: of the tasks that failed,
//!   exactly one payload reached the caller and every other was dropped.
//!
//! The default panic hook prints each panic on its way past. The harness
//! captures that, so it shows up only when a test fails, which is when it is
//! worth seeing; `--nocapture` is where to look for it otherwise. Replacing
//! the hook would silence every other test running beside these, so it stays
//! as it is.

use std::panic;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::prelude::*;
use rustflow::{CorunError, Executor, RunError};

/// How many times a task body has run, shared with that body.
type Counter = Arc<AtomicUsize>;

/// A fresh counter.
fn counter() -> Counter {
    Arc::new(AtomicUsize::new(0))
}

/// Which handle kind the six tasks of a chain are built with.
///
/// Upstream writes `static_task`, `condition_task` and `multicondition_task`
/// as three copies of one function that differ only in what the six bodies
/// return. Here they differ only in which `emplace` builds them, so the three
/// cases share [`chain`] and this says which one is being run.
#[derive(Clone, Copy, Debug)]
enum Kind {
    /// Upstream's `static_task`: six bodies returning nothing.
    Static,
    /// Upstream's `condition_task`: six bodies returning `0`, which selects
    /// successor position 0 -- the next task of the chain.
    Condition,
    /// Upstream's `multicondition_task`: the same with `{0}` in place of `0`.
    MultiCondition,
}

/// Builds upstream's chain: six tasks of kind `kind`, each counting its own
/// invocation, linearized `A -> B -> C -> D -> E -> F`.
///
/// The conditioner kinds each select successor position 0, so all three
/// chains run exactly the same six tasks in the same order; what differs is
/// whether the edges are strong dependencies or weak ones.
fn chain(taskflow: &mut Taskflow, kind: Kind, counter: &Counter) -> [Task; 6] {
    let mut tasks = Vec::with_capacity(6);
    for _ in 0..6 {
        let counter = Arc::clone(counter);
        tasks.push(match kind {
            Kind::Static => taskflow.emplace(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            }),
            Kind::Condition => taskflow.emplace_condition(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                0
            }),
            Kind::MultiCondition => taskflow.emplace_multi_condition(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                vec![0]
            }),
        });
    }
    taskflow.linearize(tasks.iter().copied());
    tasks.try_into().expect("six tasks were emplaced")
}

/// The run failure inside a corun failure, which is the one variant of
/// [`CorunError`] these cases can see: a task of the corun graph panicked.
///
/// `NotAWorker` is the other, and reaching it here would be a bug in the test
/// rather than in the port -- every corun below is made from a task body.
fn corun_failure(error: &CorunError) -> &RunError {
    match error {
        CorunError::Run(error) => error,
        other => panic!("unexpected corun error: {other:?}"),
    }
}

/// Asserts that `error` is the panic the failing body raised.
fn assert_failed_with_x(error: &RunError, context: &str) {
    assert_failed_with(error, "x", context);
}

/// The same check over whichever payload the case expects, which the subflow
/// cases need: upstream's `subflow_task` panics with `"x"` from one body and
/// `"y"` from another, and which of the two comes back is the whole claim.
fn assert_failed_with(error: &RunError, expected: &str, context: &str) {
    let RunError::Panicked(payload) = error else {
        panic!("{context}: unexpected error: {error:?}");
    };
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&expected),
        "{context}: the payload is not the one the failing task panicked with"
    );
}

/// Upstream's `static_task(W)`, `condition_task(W)` and
/// `multicondition_task(W)`, which are one case over three handle kinds.
///
/// A clean run of the chain first, so that the six bodies are known to run at
/// all and the counter the failing run is measured against means something.
/// Then `C`'s work is replaced by one that panics -- upstream's `C.work(...)`,
/// which likewise turns a conditioner into a static task -- and the run is
/// asked for twice, because a repeat is where a run that failed to clean up
/// after itself would show: the second failing run has to behave exactly as
/// the first, since each run has its own topology and every node's state is
/// re-armed before a run's sources are queued.
fn exception_chain(workers: usize, kind: Kind) {
    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();
    let counter = counter();
    let [_, _, c, ..] = chain(&mut taskflow, kind, &counter);

    executor
        .run(&taskflow)
        .wait()
        .expect("no task of the clean chain panics");
    assert_eq!(
        counter.swap(0, Ordering::Relaxed),
        6,
        "{workers} worker(s), {kind:?}: the whole chain runs when nothing fails"
    );

    taskflow.task(c).work(|| panic!("x"));

    for attempt in 1..=2 {
        let context = format!("{workers} worker(s), {kind:?}, attempt {attempt}");
        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("`C` panicked, so the run failed");
        assert_failed_with_x(&error, &context);

        // Upstream's `REQUIRE(counter == 2)`: `A` and `B` ran, `C` failed, and
        // `D`, `E` and `F` were never scheduled. The chain is serial, so this
        // is the one number it can be -- nothing was in flight beside `C` to
        // be left to finish.
        assert_eq!(
            counter.swap(0, Ordering::Relaxed),
            2,
            "{context}: only the tasks before the failing one run"
        );
    }
}

// ----------------------------------------------------------------------------
// Testcase: StaticTask
// ----------------------------------------------------------------------------

#[test]
fn a_panicking_static_task_cancels_the_rest_of_the_chain() {
    for workers in 1..=4 {
        exception_chain(workers, Kind::Static);
    }
}

// ----------------------------------------------------------------------------
// Testcase: ConditionTask
// ----------------------------------------------------------------------------

#[test]
fn a_panicking_task_cancels_the_rest_of_a_conditioner_chain() {
    for workers in 1..=4 {
        exception_chain(workers, Kind::Condition);
    }
}

// ----------------------------------------------------------------------------
// Testcase: MultiConditionTask
// ----------------------------------------------------------------------------

#[test]
fn a_panicking_task_cancels_the_rest_of_a_multi_conditioner_chain() {
    for workers in 1..=4 {
        exception_chain(workers, Kind::MultiCondition);
    }
}

// ----------------------------------------------------------------------------
// Testcase: ThreadSafety
// ----------------------------------------------------------------------------

/// How many tasks fail at once. Upstream's thousand.
const FAILING_TASKS: usize = 1000;

/// What the thread-safety case panics with: upstream's `"x"`, carried by a
/// payload that counts its own drops.
///
/// The count is what lets the case assert that exactly one payload survived
/// the race. Upstream cannot ask that question -- its thousand
/// `exception_ptr`s are reference-counted handles to one object per throw,
/// and what it is testing is that the refcounting does not race -- so this is
/// the port's own assertion rather than a translated one.
struct Failure {
    /// Upstream's `std::runtime_error("x")` message.
    what: &'static str,
    /// Bumped when this payload is dropped, wherever that happens.
    dropped: Counter,
}

impl Drop for Failure {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

/// Upstream's `thread_safety(W)`: a thousand tasks with no dependencies
/// between them, every one of which fails.
///
/// Upstream checks that the run reports `"x"` and that a thread sanitizer
/// sees no race while it does. The equivalent race here is over the one
/// payload slot on the topology, which is claimed by an atomic `fetch_or` and
/// written under the completion lock, so the payload counts below are the
/// assertion: whatever the pool managed to start, exactly one of those
/// panics reached the caller.
#[test]
fn a_thousand_tasks_failing_at_once_report_exactly_one_panic() {
    for workers in 1..=4 {
        let executor = Executor::new(workers);
        let mut taskflow = Taskflow::new();
        let started = counter();
        let dropped = counter();

        for _ in 0..FAILING_TASKS {
            let started = Arc::clone(&started);
            let dropped = Arc::clone(&dropped);
            taskflow.emplace(move || {
                started.fetch_add(1, Ordering::Relaxed);
                panic::panic_any(Failure {
                    what: "x",
                    dropped: Arc::clone(&dropped),
                });
            });
        }

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("every task of this taskflow panics");
        let RunError::Panicked(payload) = &error else {
            panic!("{workers} worker(s): unexpected error: {error:?}");
        };
        let failure = payload
            .downcast_ref::<Failure>()
            .expect("the payload is the one a task panicked with");
        assert_eq!(failure.what, "x", "{workers} worker(s)");

        // The run is over, and a run is over only once every job of it has
        // finished, so both counts are settled and readable from here.
        let started = started.load(Ordering::Relaxed);
        assert!(
            (1..=FAILING_TASKS).contains(&started),
            "{workers} worker(s): {started} tasks started, out of {FAILING_TASKS}"
        );
        assert_eq!(
            dropped.load(Ordering::Relaxed),
            started - 1,
            "{workers} worker(s): every payload but one is dropped by the run"
        );

        // And the one that was not is this one, which has been sitting in the
        // `error` above all along.
        drop(error);
        assert_eq!(
            dropped.load(Ordering::Relaxed),
            started,
            "{workers} worker(s): the reported payload was not the surviving one"
        );
    }
}

// ----------------------------------------------------------------------------
// Testcase: ExecutorCorun1 (static half)
// ----------------------------------------------------------------------------

/// Upstream's `executor_corun_1(W)`, first half: a task of `taskflow2`
/// coruns `taskflow1`, whose one task fails with `"x"`, and the corun call is
/// where that failure arrives.
///
/// Upstream writes it as `REQUIRE_THROWS_WITH_AS(executor.corun(taskflow1),
/// "x", std::runtime_error)` in the body, which is a `try`/`catch` around
/// the corun: [`try_corun`](rustflow::Executor::try_corun) is that shape
/// without the unwind, so the task that calls it survives and the outer run
/// completes `Ok`, exactly as upstream's `executor.run(taskflow2).get()`
/// does not throw.
///
/// The second half of upstream's case puts a hundred subflows in `taskflow1`
/// and is #55's: a subflow task is a node kind this port does not run yet.
#[test]
fn a_corun_of_a_failing_taskflow_reports_to_the_calling_task() {
    for workers in 1..=4 {
        let executor = Arc::new(Executor::new(workers));
        let inner = Arc::new({
            let mut inner = Taskflow::new();
            inner.emplace(|| panic!("x"));
            inner
        });

        let corun = counter();
        let mut outer = Taskflow::new();
        outer.emplace({
            let executor = Arc::clone(&executor);
            let inner = Arc::clone(&inner);
            let corun = Arc::clone(&corun);
            move || {
                let error = executor
                    .try_corun(&inner)
                    .expect_err("the corun task panicked");
                assert_failed_with_x(corun_failure(&error), &format!("{workers} worker(s)"));
                corun.fetch_add(1, Ordering::Relaxed);
            }
        });

        executor
            .run(&outer)
            .wait()
            .expect("the corunning task caught the failure");
        assert_eq!(
            corun.load(Ordering::Relaxed),
            1,
            "{workers} worker(s): the corunning task ran once"
        );
    }
}

// ----------------------------------------------------------------------------
// Testcase: ExecutorCorun2
// ----------------------------------------------------------------------------

/// Upstream's `executor_corun_2(W)`: the `A..F` chain whose `C` fails, corun
/// from a task of another taskflow, twice -- once letting the failure
/// propagate and once catching it.
///
/// Both halves are upstream's and both are about where the payload goes:
///
/// - `corun` re-raises it on the corunning worker, so the task body that did
///   not catch it fails with it, the executor records *that* as the outer
///   run's failure, and `wait` reports `"x"` -- upstream's
///   `REQUIRE_THROWS_WITH_AS(executor.run(taskflow2).get(), "x", ..)`;
/// - `try_corun` hands it back instead, so the calling task survives and the
///   outer run completes.
///
/// `counter == 2` after the first half and `== 4` after the second is
/// upstream's arithmetic, and it is the cancellation claim: the chain is
/// serial, so `A` and `B` ran and `D`, `E` and `F` were never scheduled --
/// the anchor that caught the failure is marked, which is what stops the rest
/// of a nested graph.
#[test]
fn an_uncaught_corun_failure_becomes_the_outer_runs_failure() {
    for workers in 1..=4 {
        let executor = Arc::new(Executor::new(workers));
        let counter = counter();
        let mut inner = Taskflow::new();
        let [_, _, c, ..] = chain(&mut inner, Kind::Static, &counter);
        inner.task(c).work(|| panic!("x"));
        let inner = Arc::new(inner);

        // Upstream's "uncaught corun exception propagates to the topology".
        let mut uncaught = Taskflow::new();
        uncaught.emplace({
            let executor = Arc::clone(&executor);
            let inner = Arc::clone(&inner);
            move || executor.corun(&inner).expect("a worker may corun")
        });
        let error = executor
            .run(&uncaught)
            .wait()
            .expect_err("the corunning task did not catch `x`");
        assert_failed_with_x(&error, &format!("{workers} worker(s), uncaught"));
        assert_eq!(
            counter.load(Ordering::Relaxed),
            2,
            "{workers} worker(s), uncaught: only the tasks before the failing one run"
        );

        // Upstream's "catch corun exception directly", which runs the same
        // chain again: the counter is not reset, so it reaches four.
        let mut caught = Taskflow::new();
        caught.emplace({
            let executor = Arc::clone(&executor);
            let inner = Arc::clone(&inner);
            move || {
                let error = executor.try_corun(&inner).expect_err("`C` panicked");
                assert_failed_with_x(corun_failure(&error), "caught");
            }
        });
        executor
            .run(&caught)
            .wait()
            .expect("the corunning task caught `x`");
        assert_eq!(
            counter.load(Ordering::Relaxed),
            4,
            "{workers} worker(s), caught: the second corun ran the same two tasks"
        );
    }
}

// ----------------------------------------------------------------------------
// Testcase: ModuleTask
// ----------------------------------------------------------------------------

/// Upstream's `module_task(W)`: `taskflow2` composed of a `taskflow1` whose
/// one task panics, and the failure comes out of the composing run's `wait`.
///
/// What this adds to the cases above is where the payload travels. A module
/// node is not an anchor -- it preempted itself and undertook to look at
/// nothing -- so the walk in `Job::record_panic` marks it on the way past and
/// carries the payload to the first ancestor that *is* anchored, which for a
/// composition is the run. So a failure one graph down surfaces exactly as a
/// top-level task's does, and `executor::module` needed no code of its own
/// for it.
///
/// The commented-out half of upstream's case -- the same composition with a
/// subflow inside it -- waits on #55, as the subflow family above does.
#[test]
fn a_panic_inside_a_module_fails_the_composing_run() {
    for workers in 1..=4 {
        let executor = Executor::new(workers);

        let mut target = Taskflow::new();
        target.emplace(|| panic!("x"));

        let mut taskflow = Taskflow::new();
        taskflow.composed_of(Arc::new(target));

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("the task of the composed taskflow panicked");
        assert_failed_with_x(&error, &format!("{workers} worker(s)"));
    }
}

// ----------------------------------------------------------------------------
// Exception.SubflowTask
// ----------------------------------------------------------------------------

/// Upstream's `subflow_task(W)`, both halves: a hundred subflow tasks whose
/// *body* panics with `"x"` after emplacing a child that would panic with
/// `"z"`, and then a hundred whose body returns and whose child panics with
/// `"y"`.
///
/// What each half pins is which payload survives, and they are different
/// claims:
///
/// - the body's `"x"` is reported, and the `"z"` of the child it had already
///   emplaced is never raised at all. A body that panicked is still followed
///   by the implicit join -- upstream's exception handler is the body of its
///   first switch and does not return -- so those children *are* scheduled;
///   what stops them is that the walk which recorded `"x"` marked their
///   parent, so each is skipped as the child of a failed node;
/// - with no failure in the body, the child's `"y"` is what the walk carries
///   up. A subflow node left to the implicit join is no anchor -- it
///   preempted itself and undertook to look at nothing -- so the payload goes
///   past it to the run.
#[test]
fn a_panic_in_a_subflow_body_beats_the_child_it_emplaced() {
    for workers in 1..=4 {
        let context = format!("{workers} worker(s)");
        let executor = Executor::new(workers);

        let mut taskflow = Taskflow::new();
        for _ in 0..100 {
            taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
                subflow.emplace(|| {});
                subflow.emplace(|| panic!("z"));
                panic!("x");
            });
        }

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("every subflow body panicked");
        assert_failed_with(&error, "x", &context);

        let mut taskflow = Taskflow::new();
        for _ in 0..100 {
            taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
                subflow.emplace(|| {});
                subflow.emplace(|| panic!("y"));
            });
        }

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("a child of every subflow panicked");
        assert_failed_with(&error, "y", &context);
    }
}

// ----------------------------------------------------------------------------
// Exception.JoinedSubflow1
// ----------------------------------------------------------------------------

/// Upstream's `joined_subflow_1(W)`: one subflow task spawning a hundred
/// subflow tasks, each of which emplaces two panicking children and joins
/// them explicitly.
///
/// This is the anchor priority of #48 under three levels of nesting. Each
/// `join` anchors its own node for the length of its corun, so a child's
/// `"x"` is delivered *there* rather than to the run -- and `join` re-raises
/// it, which makes it the failure of the body that joined, which the executor
/// records against the next thing waiting, which is eventually the run. So the
/// payload travels up one level at a time and `wait` reports the same `"x"`
/// upstream's `get()` throws.
#[test]
fn a_panic_under_a_hundred_joined_subflows_reaches_the_run() {
    for workers in 1..=4 {
        let context = format!("{workers} worker(s)");

        let mut taskflow = Taskflow::new();
        taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
            for _ in 0..100 {
                subflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
                    for _ in 0..2 {
                        subflow.emplace(|| panic!("x"));
                    }
                    // Upstream's note: nothing after this line is guaranteed
                    // to be reached, because the failure cancels the rest of
                    // the subgraph and the join may therefore succeed with
                    // both children skipped. Letting the panic through is the
                    // one behaviour that does not depend on which happened.
                    subflow.join().expect("a fresh subflow is joinable");
                });
            }
        });

        let error = Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect_err("the children of every joined subflow panicked");
        assert_failed_with(&error, "x", &context);
    }
}

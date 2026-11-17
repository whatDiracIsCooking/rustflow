//! Translated from `unittests/test_exceptions.cpp` (Taskflow v4.1.0).
//!
//! The cases ported here are the four the panic-cancellation issue names, and
//! they are the four of that file that need nothing but a static graph and a
//! conditioner: `Exception.StaticTask`, `Exception.ConditionTask` and
//! `Exception.MultiConditionTask`, each a linear chain `A..F` whose `C` fails,
//! and `Exception.ThreadSafety`, a thousand tasks that all fail at once. All
//! four run at 1..=4 workers, which is upstream's own sweep.
//!
//! The rest of that file is deferred rather than half-ported, in three
//! groups and for three different reasons:
//!
//! - the async family -- `Async`, `Async.Runtime`, `Runtime.Async`,
//!   `RuntimeAsync`, `DependentAsync.Runtime`, `TaskGroup.Async` and
//!   `TaskGroup.Basics` -- needs the `async` node kinds, which are M5 and M6,
//!   and with them the one shape of the anchor walk no graph can reach yet:
//!   an *implicit* anchor that nothing explicit sits above.
//! - `Semaphore` and the algorithm cases, `Algorithm` and `NestedAlgorithm`,
//!   need node kinds and parallel algorithms later milestones bring.
//! - `Probe1..4` read a failed task's payload back off the node it failed on,
//!   through `tf::Task::exception_ptr()`. That accessor is deliberately not
//!   part of this port: `Job::record_panic` *drops* a payload that loses the
//!   race for an anchor rather than parking it on the node, which
//!   `a_thousand_tasks_failing_at_once_report_exactly_one_panic` below pins
//!   from the other side. So those four wait on that decision being
//!   revisited rather than on a milestone.
//!
//! The subflow family is here in full, and the file is ordered by the issue a
//! case arrived with rather than by upstream's layout.
//! `Exception.SubflowTask` and `Exception.JoinedSubflow1` arrived with
//! subflow tasking (#50) and are the cases about where a failure *inside* a
//! subgraph is delivered; `JoinedSubflow2`, `JoinedSubflow3`,
//! `NestedSubflow` and `NestedSubflow2` arrived with #55 and are the cases
//! about the **anchor priority** -- which of several things waiting on a
//! chain of nested graphs gets the payload, and what the others see instead.
//!
//! `Exception.ExecutorCorun1` and `Exception.ExecutorCorun2` are here as of
//! the anchor walk (#48), the subflow half of `ExecutorCorun1` as of #55:
//! they are the cases about a failure inside a *nested* graph being delivered
//! to whoever is waiting for that graph rather than to the run.
//! `Exception.RuntimeCorun1` and `RuntimeCorun2` are the same two from inside
//! a runtime task, and are #55's -- which makes them the one pair here where
//! an explicit anchor and an implicit one are both on the chain, since a
//! runtime node is implicitly anchored for as long as its body runs.
//!
//! And `Exception.ModuleTask` is here as of module tasks (#49): a failure
//! inside a composed taskflow, which that walk delivers to the composing run
//! because a module node is no anchor. Its second half -- the same
//! composition with a subflow inside it, which upstream leaves commented out
//! -- is ported with it as of #55, because this port can now run it and
//! it is the deepest chain of unanchored nodes the suite has.
//!
//! Five adaptations, and the first is the only one that changes what is
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
//! - **A corun from a task where upstream coruns from an async task.**
//!   `NestedSubflow` and `NestedSubflow2` each corun the nested taskflow four
//!   more times after the run -- from `async`, from `silent_async`, and from
//!   each of those with a runtime handle -- which is one question asked of
//!   four node kinds M5 brings. The one node kind this milestone has that can
//!   corun is an ordinary task, so each of those cases coruns the taskflow
//!   once from a task of a second taskflow in place of the four, and says so
//!   where it does it.
//! - **A `post_join` flag is asserted only where upstream can.**
//!   `JoinedSubflow1` says in a comment that its own flag cannot be checked,
//!   because the failure cancels the children and a join with nothing left to
//!   run succeeds; that comment is honoured below and the flag is left out
//!   with it. `JoinedSubflow2` is the case where the same flag *can* be
//!   asserted and upstream does assert it -- its sixteen children are all
//!   sources of the subgraph, so at least one of them runs and fails however
//!   the pool is sized -- and it is asserted here too.
//!
//! The default panic hook prints each panic on its way past. The harness
//! captures that, so it shows up only when a test fails, which is when it is
//! worth seeing; `--nocapture` is where to look for it otherwise. Replacing
//! the hook would silence every other test running beside these, so it stays
//! as it is.

use std::panic;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use rustflow::prelude::*;
use rustflow::{CorunError, Executor, JoinError, RunError};

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
/// instead, and is
/// [`a_corun_of_failing_subflows_reports_to_the_calling_task`] below.
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
/// Upstream's commented-out half -- the same composition with a subflow
/// inside it, failing with `"y"` -- is here as of #55, because the port can
/// run it and it is the longest chain of *unanchored* nodes the suite has: a
/// child of a subflow of a module of a run. Nothing on it is waiting for
/// anything, so the walk marks three bases on its way past and the run is
/// still what reports the payload.
#[test]
fn a_panic_inside_a_module_fails_the_composing_run() {
    for workers in 1..=4 {
        let context = format!("{workers} worker(s)");
        let executor = Executor::new(workers);

        let mut target = Taskflow::new();
        target.emplace(|| panic!("x"));

        let mut taskflow = Taskflow::new();
        taskflow.composed_of(Arc::new(target));

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("the task of the composed taskflow panicked");
        assert_failed_with_x(&error, &context);

        // Upstream's commented-out half: the composed taskflow's one task is
        // a subflow task whose child fails.
        let mut target = Taskflow::new();
        target.emplace_subflow(|subflow: &mut Subflow<'_>| {
            subflow.emplace(|| panic!("y"));
        });

        let mut taskflow = Taskflow::new();
        taskflow.composed_of(Arc::new(target));

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("the child of the composed subflow panicked");
        assert_failed_with(&error, "y", &context);
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

// ----------------------------------------------------------------------------
// Exception.JoinedSubflow2
// ----------------------------------------------------------------------------

/// Upstream's `joined_subflow_2(W)`: one subflow task emplacing sixteen
/// children that all panic, joining them explicitly, and catching what the
/// join hands back.
///
/// `post_join == false` is upstream's assertion and the anchor priority is
/// what makes it hold: the `try_join` anchors the node for the length of the
/// nested run, so the payload stops there instead of travelling to the run,
/// and a body that was handed a failure cannot have fallen through to the
/// line after the join. It is also the case where that flag *can* be
/// asserted, unlike `JoinedSubflow1`'s -- the sixteen children are all
/// sources of the subgraph, so at least one of them runs and fails whatever
/// the pool does.
///
/// The failing run at the end is the port being stricter than upstream rather
/// than different from it. Upstream calls `wait()` and never looks at what the
/// run reports, and there *is* something to report: a `try_join` that failed
/// leaves the subflow joinable, so the implicit join runs the same sixteen
/// children a second time and nothing is catching that one.
/// [`Subflow::try_join`](rustflow::Subflow::try_join) sets out the obligation
/// a body takes on by catching; this is what declining it looks like from
/// outside.
#[test]
fn a_body_that_catches_its_joins_failure_stops_where_it_caught_it() {
    /// Upstream's sixteen children.
    const CHILDREN: usize = 16;

    for workers in 1..=4 {
        let context = format!("{workers} worker(s)");
        let post_join = Arc::new(AtomicBool::new(false));

        let mut taskflow = Taskflow::new();
        taskflow.emplace_subflow({
            let post_join = Arc::clone(&post_join);
            move |subflow: &mut Subflow<'_>| {
                for _ in 0..CHILDREN {
                    subflow.emplace(|| panic!("x"));
                }

                // Upstream's `try { sf0.join(); post_join = true; }
                // catch(const std::runtime_error& re) { .. }`: `try_join` is
                // that `try`/`catch` without the unwind, so the store is
                // reached only on the path where the join succeeded.
                match subflow.try_join() {
                    Ok(()) => post_join.store(true, Ordering::Relaxed),
                    Err(error) => assert_failed_with(join_failure(&error), "x", "the join"),
                }
            }
        });

        let error = Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect_err("the implicit join raised what the explicit one caught");
        assert_failed_with(&error, "x", &context);

        assert!(
            !post_join.load(Ordering::Relaxed),
            "{context}: the body went on past a join that failed"
        );
    }
}

/// The run failure inside a join failure, which is the one variant of
/// [`JoinError`] these cases can see: a task of the subgraph panicked.
///
/// [`corun_failure`]'s counterpart, and a `match` for the same reason --
/// `JoinError` is `#[non_exhaustive]` outside the crate. Reaching
/// `AlreadyJoined` here would be a bug in the test: every join below is the
/// first one its body makes.
fn join_failure(error: &JoinError) -> &RunError {
    match error {
        JoinError::Run(error) => error,
        other => panic!("unexpected join error: {other:?}"),
    }
}

// ----------------------------------------------------------------------------
// Exception.JoinedSubflow3
// ----------------------------------------------------------------------------

/// Upstream's `joined_subflow_3(N)`: four children chained `W -> X -> Y -> Z`
/// inside one subflow task whose `X` fails, once left to the implicit join and
/// once joined explicitly.
///
/// `num_tasks == 2` is upstream's assertion for both halves and is the
/// `counter == 2` of the `A..F` cases one level down: the children are
/// serial, so `W` ran, `X` failed, and `Y` and `Z` were never scheduled. What
/// differs between the halves is only where the payload was raised -- the
/// run's own slot for the implicit join, and the body that called `join` for
/// the explicit one, which re-raises it and so fails the subflow task itself
/// -- and `wait` reports the same payload either way. That is the pair's
/// point: the two joins are different mechanisms with one observable answer.
#[test]
fn only_the_children_before_a_failing_one_run_under_either_join() {
    for workers in 1..=4 {
        let executor = Executor::new(workers);

        // Upstream's implicit-join half, failing with `"x"`.
        let num_tasks = counter();
        let mut taskflow = Taskflow::new();
        taskflow.emplace_subflow(chained_children(&num_tasks, "x", Join::Implicit));

        let context = format!("{workers} worker(s), implicit");
        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("the second child panicked");
        assert_failed_with(&error, "x", &context);
        assert_eq!(
            num_tasks.load(Ordering::Relaxed),
            2,
            "{context}: only the children up to the failing one ran"
        );

        // Upstream's explicit-join half, failing with `"y"`. Upstream clears
        // and refills one taskflow; a second one says the same thing without
        // the clear, and the fresh counter is upstream's `num_tasks = 0`.
        let num_tasks = counter();
        let mut taskflow = Taskflow::new();
        taskflow.emplace_subflow(chained_children(&num_tasks, "y", Join::Explicit));

        let context = format!("{workers} worker(s), explicit");
        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("the join re-raised what the second child panicked with");
        assert_failed_with(&error, "y", &context);
        assert_eq!(
            num_tasks.load(Ordering::Relaxed),
            2,
            "{context}: only the children up to the failing one ran"
        );
    }
}

/// Which join a subflow case uses, which is the only difference between the
/// two halves of `joined_subflow_3` and between upstream's `NestedSubflow`
/// and `NestedSubflow2`.
#[derive(Clone, Copy, Debug)]
enum Join {
    /// The body returns and the dispatch arm runs the subgraph for it. That
    /// join anchors nothing: the node is preempted and has undertaken to look
    /// at no payload, so a failure inside walks straight past it.
    Implicit,
    /// The body calls `join`, which anchors its own node for the length of the
    /// nested run and re-raises whatever the subgraph failed with.
    Explicit,
}

/// Upstream's four children of `joined_subflow_3`: `W -> X -> Y -> Z`, each
/// counting itself, with `X` panicking with `payload`.
fn chained_children(
    counter: &Counter,
    payload: &'static str,
    join: Join,
) -> impl for<'a> FnMut(&mut Subflow<'a>) + Send + 'static {
    let counter = Arc::clone(counter);
    move |subflow: &mut Subflow<'_>| {
        let w = subflow.emplace(bump(&counter));
        let x = {
            let counter = Arc::clone(&counter);
            subflow.emplace(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                // `panic_any` rather than `panic!`, so that the payload is
                // the `&'static str` the assertions downcast to rather than
                // the `String` a formatted message would be.
                panic::panic_any(payload);
            })
        };
        let y = subflow.emplace(bump(&counter));
        let z = subflow.emplace(bump(&counter));
        subflow.linearize([w, x, y, z]);

        if matches!(join, Join::Explicit) {
            subflow.join().expect("a fresh subflow is joinable");
        }
    }
}

/// A body that counts itself: upstream's `[&num_tasks](){ ++num_tasks; }`.
fn bump(counter: &Counter) -> impl FnMut() + Send + 'static {
    let counter = Arc::clone(counter);
    move || {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

// ----------------------------------------------------------------------------
// Exception.NestedSubflow and Exception.NestedSubflow2
// ----------------------------------------------------------------------------

/// Upstream's `nested_subflow(N)`: four levels of subflow, the innermost of
/// which fails, with every level left to the implicit join.
#[test]
fn a_panic_four_subflows_down_reaches_the_run_through_implicit_joins() {
    for workers in 1..=4 {
        nested_subflow(workers, Join::Implicit);
    }
}

/// Upstream's `nested_subflow_2(N)`: the same four levels with every one of
/// them joined explicitly.
#[test]
fn a_panic_four_subflows_down_reaches_the_run_through_explicit_joins() {
    for workers in 1..=4 {
        nested_subflow(workers, Join::Explicit);
    }
}

/// How many levels of subflow upstream's two nested cases build.
const LEVELS: usize = 4;

/// Upstream's `nested_subflow` and `nested_subflow_2`, which differ only in
/// whether each level joins explicitly.
///
/// `num_tasks == 12` is upstream's assertion for both, and it is the same
/// serial-chain arithmetic as the `A..F` cases three levels further in: two
/// children ran at each of the four levels, each of the three inner bodies
/// counted itself, and the failing child counted itself -- `2 * 4 + 3 + 1` --
/// while the `Y` and `Z` of every level never started and neither did the
/// nine remaining rounds of the `run_n`.
///
/// What the pair is about is the anchor priority, and the two halves take
/// opposite routes to the same payload:
///
/// - with every level left to the implicit join, no base on the chain is an
///   anchor at all. The walk marks four of them on the way past -- which is
///   what cancels the rest of each level -- and the payload lands on the run,
///   exactly as the module case's does;
/// - with every level joined explicitly, the walk stops at the first one it
///   meets, the innermost `join`. That re-raises the payload, failing the body
///   that joined, whose failure then walks up to the join above it, and so on:
///   four hand-offs rather than one walk. The payload keeps its identity
///   through all four, which is what
///   [`resume_unwind`](std::panic::resume_unwind) is for and what makes the
///   two halves indistinguishable from the run's point of view.
fn nested_subflow(workers: usize, join: Join) {
    /// Upstream's `run_n(taskflow, 10)`.
    const ROUNDS: usize = 10;
    /// Upstream's `REQUIRE(num_tasks == 12)`.
    const COUNTED: usize = 2 * LEVELS + (LEVELS - 1) + 1;

    let context = format!("{workers} worker(s), {join:?}");
    let executor = Arc::new(Executor::new(workers));
    let num_tasks = counter();

    let taskflow = Arc::new({
        let mut taskflow = Taskflow::new();
        taskflow.emplace_subflow(nested_body(&num_tasks, 1, join));
        taskflow
    });

    // The first round fails, which cancels the run, so the other nine never
    // start -- a claim the count below makes as much as it makes one about the
    // levels.
    let error = executor
        .run_n(&taskflow, ROUNDS)
        .wait()
        .expect_err("the innermost child panicked");
    assert_failed_with(&error, "x", &context);
    assert_eq!(
        num_tasks.load(Ordering::Relaxed),
        COUNTED,
        "{context}: two children of each level, three bodies and the failing child"
    );

    // Upstream coruns the same taskflow four more times here -- from an
    // `async` task, from a `silent_async` task and from each of those with a
    // runtime handle -- and the module docs say why one corun from an ordinary
    // task stands in for the four. The question is the same one with a level
    // added: the chain now roots at the corun's own anchor rather than at a
    // run, so the payload comes out of `try_corun` and the outer run completes
    // normally.
    num_tasks.store(0, Ordering::Relaxed);
    let mut outer = Taskflow::new();
    outer.emplace({
        let executor = Arc::clone(&executor);
        let taskflow = Arc::clone(&taskflow);
        let context = context.clone();
        move || {
            let error = executor
                .try_corun(&taskflow)
                .expect_err("the innermost child panicked");
            assert_failed_with(corun_failure(&error), "x", &context);
        }
    });

    executor
        .run(&outer)
        .wait()
        .expect("the corunning task caught the failure");
    assert_eq!(
        num_tasks.load(Ordering::Relaxed),
        COUNTED,
        "{context}, corun: the nested graph ran exactly as far as it did under the run"
    );
}

/// What a subflow body is, spelled out because [`nested_body`] returns one of
/// itself: the bound [`FlowBuilder::emplace_subflow`] takes, boxed so that the
/// recursion has a type.
type NestedBody = dyn for<'a> FnMut(&mut Subflow<'a>) + Send + 'static;

/// Upstream's four nested bodies, built by recursion over `level` rather than
/// as four copies of one lambda.
///
/// `level` counts from one, as upstream's names do (`sf1` outwards). Every
/// level but the first counts its own body -- upstream's `++num_tasks` at the
/// top of the three inner lambdas -- and every level emplaces
/// `V -> W -> X -> Y -> Z` where `X` is the next level down, or at [`LEVELS`]
/// the child that fails.
///
/// The recursion happens while the graph is being *built*, which is to say
/// while the body above it is running, so each round of a `run_n` rebuilds the
/// levels below it exactly as upstream's nested lambdas do.
fn nested_body(counter: &Counter, level: usize, join: Join) -> Box<NestedBody> {
    let counter = Arc::clone(counter);
    Box::new(move |subflow: &mut Subflow<'_>| {
        // Upstream's `++num_tasks` at the top of `sf2`, `sf3` and `sf4`'s
        // bodies, which the outermost body does not have: it is the task of
        // the taskflow itself rather than a child of anything.
        if level > 1 {
            counter.fetch_add(1, Ordering::Relaxed);
        }

        let v = subflow.emplace(bump(&counter));
        let w = subflow.emplace(bump(&counter));
        let x = if level < LEVELS {
            subflow.emplace_subflow(nested_body(&counter, level + 1, join))
        } else {
            let counter = Arc::clone(&counter);
            subflow.emplace(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                panic!("x")
            })
        };
        let y = subflow.emplace(bump(&counter));
        let z = subflow.emplace(bump(&counter));
        subflow.linearize([v, w, x, y, z]);

        if matches!(join, Join::Explicit) {
            subflow.join().expect("a fresh subflow is joinable");
        }
    })
}

// ----------------------------------------------------------------------------
// This port's own: two anchors on one chain
// ----------------------------------------------------------------------------

/// Not an upstream case: two explicit anchors on one chain with the inner one
/// catching, so that nothing reaches the outer one or the run.
///
/// Every upstream case lets the payload travel all the way to the top, which
/// cannot tell "the innermost anchor was chosen" apart from "every anchor on
/// the chain was handed a copy" -- `JoinedSubflow1` and the explicit half of
/// `nested_subflow` would pass either way. This is the difference, and it is
/// the half of `Job::record_panic`'s walk that the suite would otherwise take
/// on trust.
///
/// It needs the inner body to do two things: catch the failure *and* erase the
/// child. A join that failed leaves the subflow joinable, so the implicit join
/// would otherwise run the child again and raise the same panic where nothing
/// is catching it -- which is exactly what the case above asserts happens.
/// [`Subflow::try_join`](rustflow::Subflow::try_join) names that obligation;
/// this is a body meeting it.
#[test]
fn the_innermost_explicit_anchor_catches_and_nothing_travels_past_it() {
    for workers in 1..=4 {
        let context = format!("{workers} worker(s)");
        let caught = counter();

        let mut taskflow = Taskflow::new();
        taskflow.emplace_subflow({
            let caught = Arc::clone(&caught);
            move |outer: &mut Subflow<'_>| {
                let caught = Arc::clone(&caught);
                outer.emplace_subflow(move |inner: &mut Subflow<'_>| {
                    let child = inner.emplace(|| panic!("x"));

                    let error = inner.try_join().expect_err("the child panicked");
                    assert_failed_with(join_failure(&error), "x", "the inner join");
                    caught.fetch_add(1, Ordering::Relaxed);

                    // The failure ends here, and this is what saying so looks
                    // like: with the child erased the implicit join has
                    // nothing to run and raises nothing.
                    inner.erase(child);
                });

                // Nothing failed under *this* join, because the body above
                // kept its subgraph's failure to itself.
                outer.join().expect("a fresh subflow is joinable");
            }
        });

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("the inner join caught the only failure there was");
        assert_eq!(
            caught.load(Ordering::Relaxed),
            1,
            "{context}: the inner body caught its child's panic exactly once"
        );
    }
}

// ----------------------------------------------------------------------------
// Testcase: ExecutorCorun1 (subflow half)
// ----------------------------------------------------------------------------

/// Upstream's `executor_corun_1(W)`, second half: `taskflow1` is a hundred
/// subflow tasks, each emplacing a hundred children that all panic with
/// `"y"`, and a task of `taskflow2` coruns it.
///
/// The static half says where a failure of a corun graph goes; this one says
/// that it still goes there from a graph *inside* that graph. A child of a
/// subflow of a corun sits two unanchored bases below the anchor -- the child
/// is waiting for nothing and neither is the subflow node, which was left to
/// the implicit join -- so the walk marks both and hands the payload to the
/// one frame that is blocked, which is the `try_corun` call and not the outer
/// run.
///
/// Ten thousand panicking children is upstream's count, and almost none of
/// them run: the first failure marks the anchor, and a job under a marked
/// anchor is skipped by `Job::is_parent_cancelled`. That is what makes a case
/// of this shape cost milliseconds, and it is the thing being tested as much
/// as the delivery is.
#[test]
fn a_corun_of_failing_subflows_reports_to_the_calling_task() {
    for workers in 1..=4 {
        let context = format!("{workers} worker(s)");
        let executor = Arc::new(Executor::new(workers));
        let inner = Arc::new(failing_subflows("y"));

        let corun = counter();
        let mut outer = Taskflow::new();
        outer.emplace({
            let executor = Arc::clone(&executor);
            let inner = Arc::clone(&inner);
            let corun = Arc::clone(&corun);
            let context = context.clone();
            move || {
                let error = executor
                    .try_corun(&inner)
                    .expect_err("a child of every subflow panicked");
                assert_failed_with(corun_failure(&error), "y", &context);
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
            "{context}: the corunning task ran once"
        );
    }
}

/// Upstream's `taskflow1` for the second halves of `executor_corun_1` and
/// `runtime_corun_1`: a hundred subflow tasks, each emplacing a hundred
/// children that all panic with `payload`.
fn failing_subflows(payload: &'static str) -> Taskflow {
    /// Upstream's hundred subflow tasks.
    const SUBFLOWS: usize = 100;
    /// Upstream's hundred children of each.
    const CHILDREN: usize = 100;

    let mut taskflow = Taskflow::new();
    for _ in 0..SUBFLOWS {
        taskflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
            for _ in 0..CHILDREN {
                subflow.emplace(move || panic::panic_any(payload));
            }
        });
    }
    taskflow
}

// ----------------------------------------------------------------------------
// Testcase: RuntimeCorun1
// ----------------------------------------------------------------------------

/// Upstream's `runtime_corun_1(W)`: `ExecutorCorun1`'s corun made from a
/// *runtime* task's body, in upstream's three phases -- a one-task graph
/// caught, a graph of a hundred failing subflows caught, and that second graph
/// again with the catch taken off so the failure propagates.
///
/// A runtime task is where the anchor priority has two candidates for the
/// first time. The node is `IMPLICITLY_ANCHORED` for as long as its body runs
/// -- it has undertaken to look at its own slot once whatever it spawned is
/// done -- and the corun inside that body anchors itself *explicitly*.
/// Upstream's `_process_exception` prefers the explicit one and says so in a
/// comment, "Implicit anchor has the lowest priority", and the catching body
/// below reads that preference from both ends: the payload comes out of
/// [`Executor::try_corun`](rustflow::Executor::try_corun), and
/// [`Runtime::try_corun`](rustflow::Runtime::try_corun) -- which is what reads
/// the node's own slot -- finds nothing at all.
///
/// Upstream's third phase changes the body to `executor.corun(..)` and nothing
/// else, so the payload is raised on the worker, fails the runtime task, and
/// the walk carries it to the run: the node is implicitly anchored at that
/// moment too, and the run above it is an anchor by definition and outranks
/// it.
///
/// One adaptation, and not to what is asserted: upstream clears `taskflow1`
/// and refills it between the first two phases, where a task body here owns an
/// [`Arc`] of what it coruns, so the second graph is a second taskflow. The
/// *body* is replaced in place, through
/// [`TaskMut::runtime_work`](rustflow::TaskMut::runtime_work), which is
/// upstream's `task.work(..)`.
#[test]
fn a_runtime_body_catches_a_corun_failure_until_it_is_asked_not_to() {
    for workers in 1..=4 {
        let context = format!("{workers} worker(s)");
        let executor = Arc::new(Executor::new(workers));

        // Upstream's first `taskflow1`: one task, which fails.
        let one_task = Arc::new({
            let mut flow = Taskflow::new();
            flow.emplace(|| panic!("x"));
            flow
        });
        // Upstream's refilled `taskflow1`.
        let subflows = Arc::new(failing_subflows("x"));

        let mut taskflow = Taskflow::new();
        let task = taskflow.emplace_runtime(catching(&executor, &one_task, &context));
        executor
            .run(&taskflow)
            .wait()
            .expect("the runtime body caught `x`");

        taskflow
            .task(task)
            .runtime_work(catching(&executor, &subflows, &context));
        executor
            .run(&taskflow)
            .wait()
            .expect("the runtime body caught the `x` of a nested subflow");

        // Upstream's "change it to parent propagation".
        taskflow.task(task).runtime_work({
            let executor = Arc::clone(&executor);
            let subflows = Arc::clone(&subflows);
            move |_runtime: &mut Runtime<'_>| {
                executor
                    .corun(&subflows)
                    .expect("a runtime body runs on a worker of this executor");
            }
        });
        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("the runtime body let `x` through");
        assert_failed_with(&error, "x", &context);
    }
}

/// Upstream's catching runtime body: corun `flow`, expect its failure with
/// `"x"`, and then check that the node's own slot stayed empty.
///
/// That second check is the anchor priority stated as the negative, and it is
/// why the handle is used at all here -- upstream's body takes a
/// `tf::Runtime&` and never touches it.
fn catching(
    executor: &Arc<Executor>,
    flow: &Arc<Taskflow>,
    context: &str,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static {
    let executor = Arc::clone(executor);
    let flow = Arc::clone(flow);
    let context = context.to_owned();
    move |runtime: &mut Runtime<'_>| {
        let error = executor
            .try_corun(&flow)
            .expect_err("a task of the corun graph panicked");
        assert_failed_with(corun_failure(&error), "x", &context);

        runtime.try_corun().unwrap_or_else(|error| {
            panic!("{context}: the runtime node's own slot took a payload: {error:?}")
        });
    }
}

// ----------------------------------------------------------------------------
// Testcase: RuntimeCorun2
// ----------------------------------------------------------------------------

/// Upstream's `runtime_corun_2(W)`: `ExecutorCorun2` with a runtime task doing
/// the corunning -- the `A..F` chain whose `C` fails, corun once uncaught and
/// once caught, with `counter == 2` and then `== 4`.
///
/// Everything the pair asserts is `ExecutorCorun2`'s, and upstream repeats it
/// from a runtime body because the node the corun hangs under is a different
/// one: an implicitly anchored node rather than a plain static task. Both
/// halves say that it makes no difference, which is the preference
/// `a_runtime_body_catches_a_corun_failure_until_it_is_asked_not_to` states
/// directly.
#[test]
fn an_uncaught_corun_failure_in_a_runtime_body_becomes_the_outer_runs_failure() {
    for workers in 1..=4 {
        let executor = Arc::new(Executor::new(workers));
        let counter = counter();
        let mut inner = Taskflow::new();
        let [_, _, c, ..] = chain(&mut inner, Kind::Static, &counter);
        inner.task(c).work(|| panic!("x"));
        let inner = Arc::new(inner);

        // Upstream's "uncaught corun exception propagates to the topology".
        let mut uncaught = Taskflow::new();
        uncaught.emplace_runtime({
            let executor = Arc::clone(&executor);
            let inner = Arc::clone(&inner);
            move |_runtime: &mut Runtime<'_>| {
                executor
                    .corun(&inner)
                    .expect("a runtime body runs on a worker of this executor");
            }
        });
        let error = executor
            .run(&uncaught)
            .wait()
            .expect_err("the corunning runtime task did not catch `x`");
        assert_failed_with_x(&error, &format!("{workers} worker(s), uncaught"));
        assert_eq!(
            counter.load(Ordering::Relaxed),
            2,
            "{workers} worker(s), uncaught: only the tasks before the failing one run"
        );

        // Upstream's "catch corun exception directly", which runs the same
        // chain again: the counter is not reset, so it reaches four.
        let mut caught = Taskflow::new();
        caught.emplace_runtime({
            let executor = Arc::clone(&executor);
            let inner = Arc::clone(&inner);
            move |_runtime: &mut Runtime<'_>| {
                let error = executor.try_corun(&inner).expect_err("`C` panicked");
                assert_failed_with_x(corun_failure(&error), "caught");
            }
        });
        executor
            .run(&caught)
            .wait()
            .expect("the corunning runtime task caught `x`");
        assert_eq!(
            counter.load(Ordering::Relaxed),
            4,
            "{workers} worker(s), caught: the second corun ran the same two tasks"
        );
    }
}

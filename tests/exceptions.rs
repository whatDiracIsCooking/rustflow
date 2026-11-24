//! Translated from `unittests/test_exceptions.cpp` (Taskflow v4.1.0).
//!
//! The cases ported here are the four the panic-cancellation issue names, and
//! they are the four of that file that need nothing but a static graph and a
//! conditioner: `Exception.StaticTask`, `Exception.ConditionTask` and
//! `Exception.MultiConditionTask`, each a linear chain `A..F` whose `C` fails,
//! and `Exception.ThreadSafety`, a thousand tasks that all fail at once. All
//! four run at 1..=4 workers, which is upstream's own sweep.
//!
//! The async family is at the end of the file, in upstream's own order:
//! `Async`, `Async.Runtime`, `DependentAsync.Runtime`, `RuntimeAsync`,
//! `TaskGroup.Basics`, `TaskGroup.Async` and `Runtime.Async`, with
//! `Semaphore` -- which upstream puts ahead of the task-group cases -- after
//! them. All eight arrived with #94, once there were free node kinds to hang
//! them on, a task group to open and semaphores the executor honours; between
//! them they are the only cases in this suite that reach the shape no graph
//! can make -- an *implicit* anchor with nothing explicit above it, which is
//! what a group's anchor and an entered runtime node both are.
//!
//! Three of those eight run a shorter worker sweep than upstream's eight:
//! `TaskGroup.Async` stops at five, `Runtime.Async` at four and `Semaphore`
//! at three, and each says where it stops why. The sweep is there to cover
//! "one worker does all of it" and "several workers steal it", and the
//! reduced ends reach both; CI runs on about two cores, so the upper half of
//! upstream's sweep would cost oversubscribed runs of the heaviest cases here
//! for no shape the lower half does not already make. `Semaphore` has a
//! second reason of its own, which its case states: its graph is a serial
//! chain in which at most one task is ever runnable. Nothing else about any
//! of the eight is reduced -- the hundred rounds of a hundred children are
//! upstream's own, and so are the 1024 failures of the two `.Runtime` cases.
//!
//! One group of that file is still not here, and it is not waiting on a node
//! kind: `Probe1..4` read a failed task's payload back off the node it failed
//! on, through `tf::Task::exception_ptr()`. That accessor is deliberately not
//! part of this port: `Job::record_panic` *drops* a payload that loses the
//! race for an anchor rather than parking it on the node, which
//! `a_thousand_tasks_failing_at_once_report_exactly_one_panic` below pins
//! from the other side. So those four wait on that decision being revisited
//! rather than on a milestone.
//!
//! The algorithm pair, `Algorithm` and `NestedAlgorithm`, is at the end of
//! the file as of #463 -- a `for_each_index` whose every body panics, and the
//! same with the panic two graphs down through a `corun`. They were the last
//! of upstream's file waiting on a milestone, the parallel algorithms having
//! landed with #164-#179.
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
//! - **A silent task's assertion is recorded rather than raised.** Upstream
//!   writes `REQUIRE`s inside `silent_async` bodies in three places here --
//!   the two `silent_async` coruns of `NestedSubflow` and `NestedSubflow2`,
//!   and the silent half of `Runtime.Async` -- where a failure would be
//!   caught by the task wrapper and then dropped, there being no handle to
//!   report it to. `Executor::silent_async` says so in as many words. So each
//!   such body hands its outcome to a shared cell instead and the assertion
//!   is made on this thread once the task is known to have run, which is the
//!   same claim without a case that can pass in silence.
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
use std::sync::atomic::{AtomicBool, Ordering};

use rustflow::algorithm::partitioner::DefaultPartitioner;
use rustflow::prelude::*;
use rustflow::{AsyncHandle, CorunError, Executor, JoinError, RunError, Semaphore};

// Four topics: the matrix macro, the counting the cases assert on, the
// counting body most of them attach, and the one handshake in the file --
// `RuntimeAsync`'s fourth phase, which waits for a child to finish. The
// `AtomicBool` flags below are not counters and stay where they are.
#[macro_use]
mod common;
#[path = "common/bodies.rs"]
mod bodies;
#[path = "common/counters.rs"]
mod counters;
#[path = "common/spin.rs"]
mod spin;

use bodies::bump;
use counters::{Counter, count, counter};
use spin::spin_until;

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
// Invented: the multi-condition arm's own panic path
// ----------------------------------------------------------------------------

/// A multi-condition body that panics selects nothing, and its payload comes
/// back from `wait` exactly as a static task's does.
///
/// Not a repeat of the case above, and the reason is a line of
/// [`exception_chain`]: it replaces `C`'s work through
/// `taskflow.task(c).work(..)`, which turns a conditioner into a *static*
/// task -- upstream's own case does the same -- so none of the three chains
/// above ever enters a conditioner arm's failure path. The single-condition
/// counterpart is `tests/control_flow.rs`'s
/// `a_panicking_conditioner_selects_nothing`; this is the arm beside it,
/// whose empty selection is an `unwrap_or_default` on a `Vec` rather than a
/// `None`, and the one the coverage listing showed nothing reaching.
#[test]
fn a_panicking_multi_conditioner_selects_nothing() {
    for workers in 1..=4 {
        let context = format!("{workers} worker(s)");
        let executor = Executor::new(workers);
        let mut taskflow = Taskflow::new();

        let counter = counter();
        let cond = taskflow.emplace_multi_condition(|| panic!("x"));
        let branches: Vec<Task> = (0..3).map(|_| taskflow.emplace(bump(&counter))).collect();
        taskflow.task(cond).precede(branches);

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("the multi-conditioner panicked");
        assert_failed_with_x(&error, &context);
        assert_eq!(
            count(&counter),
            0,
            "{context}: a panicking multi-conditioner selects no successor"
        );
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
        let started = count(&started);
        assert!(
            (1..=FAILING_TASKS).contains(&started),
            "{workers} worker(s): {started} tasks started, out of {FAILING_TASKS}"
        );
        assert_eq!(
            count(&dropped),
            started - 1,
            "{workers} worker(s): every payload but one is dropped by the run"
        );

        // And the one that was not is this one, which has been sitting in the
        // `error` above all along.
        drop(error);
        assert_eq!(
            count(&dropped),
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
            count(&corun),
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
            count(&counter),
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
            count(&counter),
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
            count(&num_tasks),
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
            count(&num_tasks),
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
        count(&num_tasks),
        COUNTED,
        "{context}: two children of each level, three bodies and the failing child"
    );

    // Upstream coruns the same taskflow four more times here, from each of
    // the four free node kinds that can corun: an `async` task, a
    // `silent_async` one, and each of those handed a runtime. The question is
    // the run's with a level added -- the chain now roots at the corun's own
    // anchor rather than at a run, so the payload comes out of `try_corun`
    // and nothing is left for the task itself to report -- and asking it of
    // four node kinds is asking whether the anchor the corun pushes depends
    // on what is under it. It does not, which is why all four read the same.
    //
    // Upstream's commented-out `rt.corun(taskflow)` lines are left out with
    // them: a `Runtime` here has no corun-a-taskflow form, and upstream's own
    // two runtime bodies call `executor.corun(taskflow)` exactly as the plain
    // two do.
    let corun = {
        let executor = Arc::clone(&executor);
        let taskflow = Arc::clone(&taskflow);
        let context = context.clone();
        move || {
            let error = executor
                .try_corun(&taskflow)
                .expect_err("the innermost child panicked");
            assert_failed_with(corun_failure(&error), "x", &context);
        }
    };

    // Upstream's `executor.async([&](){ .. }).get()`.
    num_tasks.store(0, Ordering::Relaxed);
    executor
        .spawn(corun.clone())
        .wait()
        .expect("the corunning async task caught the failure");
    assert_eq!(
        count(&num_tasks),
        COUNTED,
        "{context}, corun from an async task: the nested graph ran exactly as \
         far as it did under the run"
    );

    // Upstream's `executor.silent_async([&](){ .. }); executor.wait_for_all();`.
    // The assertion inside `corun` would be dropped here rather than
    // reported -- the module docs' silent-task adaptation -- so the body says
    // that it got all the way through it and the check is this thread's.
    num_tasks.store(0, Ordering::Relaxed);
    let silent_finished = Arc::new(AtomicBool::new(false));
    executor.silent_async({
        let corun = corun.clone();
        let finished = Arc::clone(&silent_finished);
        move || {
            corun();
            finished.store(true, Ordering::Release);
        }
    });
    executor.wait_for_all();
    assert!(
        silent_finished.load(Ordering::Acquire),
        "{context}, corun from a silent async task: the body did not reach its end"
    );
    assert_eq!(
        count(&num_tasks),
        COUNTED,
        "{context}, corun from a silent async task: the nested graph ran \
         exactly as far as it did under the run"
    );

    // Upstream's `executor.async([&](tf::Runtime&){ .. }).get()`.
    num_tasks.store(0, Ordering::Relaxed);
    executor
        .spawn_runtime({
            let corun = corun.clone();
            move |_runtime: &mut Runtime<'_>| corun()
        })
        .wait()
        .expect("the corunning runtime-shaped async task caught the failure");
    assert_eq!(
        count(&num_tasks),
        COUNTED,
        "{context}, corun from an async task's runtime: the nested graph ran \
         exactly as far as it did under the run"
    );

    // Upstream's `executor.silent_async([&](tf::Runtime&){ .. });
    // executor.wait_for_all();`, with the same shared cell as the plain
    // silent one above.
    num_tasks.store(0, Ordering::Relaxed);
    let silent_runtime_finished = Arc::new(AtomicBool::new(false));
    executor.silent_async_runtime({
        let finished = Arc::clone(&silent_runtime_finished);
        move |_runtime: &mut Runtime<'_>| {
            corun();
            finished.store(true, Ordering::Release);
        }
    });
    executor.wait_for_all();
    assert!(
        silent_runtime_finished.load(Ordering::Acquire),
        "{context}, corun from a silent async task's runtime: the body did \
         not reach its end"
    );
    assert_eq!(
        count(&num_tasks),
        COUNTED,
        "{context}, corun from a silent async task's runtime: the nested \
         graph ran exactly as far as it did under the run"
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
            count(&caught),
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
        assert_eq!(count(&corun), 1, "{context}: the corunning task ran once");
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
            count(&counter),
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
            count(&counter),
            4,
            "{workers} worker(s), caught: the second corun ran the same two tasks"
        );
    }
}

// ----------------------------------------------------------------------------
// Testcase: Async
// ----------------------------------------------------------------------------

/// Upstream's `async_task(W)`: the three things a free-standing task can do
/// with a failure, asked of the two forms that have one.
///
/// A handle-bearing task delivers its panic to the handle and a silent one
/// drops it, which is the whole case -- and the clean `spawn` first is what
/// makes the other two mean anything, since an executor that failed to run a
/// body at all would pass a case that only looked for failures.
fn async_task(workers: usize) {
    let context = format!("{workers} worker(s)");
    let executor = Executor::new(workers);

    let one = executor.spawn(|| 1);
    assert_eq!(
        one.wait().expect("the body did not panic"),
        1,
        "{context}: an async task's result reaches its handle"
    );

    // The return type is written out because Rust has nothing to infer it
    // from: a body that only panics never produces an `R`. Upstream's lambda
    // is deduced `void` for the same reason and `1` above is its `int`.
    let failed = executor.spawn(|| -> i32 { panic!("x") });
    let error = failed.wait().expect_err("the body panicked");
    assert_failed_with_x(&error, &context);

    // Upstream's "exception is caught without any action": nothing is waiting
    // for this one, so the payload is dropped where the wrapper caught it and
    // the executor carries on. The `wait_for_all` is what says it ran at all.
    executor.silent_async(|| panic!("y"));
    executor.wait_for_all();
}

worker_matrix! {
    async_task {
        an_async_tasks_panic_reaches_its_handle_and_nothing_else_1_thread(1),
        an_async_tasks_panic_reaches_its_handle_and_nothing_else_2_threads(2),
        an_async_tasks_panic_reaches_its_handle_and_nothing_else_3_threads(3),
        an_async_tasks_panic_reaches_its_handle_and_nothing_else_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// Testcase: Async.Runtime
// ----------------------------------------------------------------------------

/// How many failures upstream's two runtime-shaped async cases submit at
/// once: its `for(size_t i=0; i<1024; i++)`.
const FAILING_FREE_TASKS: usize = 1024;

/// Upstream's `async_with_runtime(W)`: 1024 runtime-shaped async tasks whose
/// bodies all panic, and every one of the 1024 handles reports it.
///
/// The volume is what the case is for. One such task says that an explicitly
/// anchored free node routes its own body's panic to its handle; a thousand
/// at once say that the slot each payload lands in is the slot that node's
/// handle reads, which is the claim a shared slot or a lost payload would
/// break -- and they say it while the walk in `Job::record_panic` is being
/// run from every worker at the same time.
fn async_with_runtime(workers: usize) {
    let context = format!("{workers} worker(s)");
    let executor = Executor::new(workers);

    let handles: Vec<_> = (0..FAILING_FREE_TASKS)
        .map(|_| executor.spawn_runtime(|_runtime: &mut Runtime<'_>| panic!("x")))
        .collect();

    for (task, handle) in handles.into_iter().enumerate() {
        let error = handle.wait().expect_err("the body panicked");
        assert_failed_with_x(&error, &format!("{context}, task {task}"));
    }

    // Upstream's "silently caught by the task".
    executor.silent_async_runtime(|_runtime: &mut Runtime<'_>| panic!("x"));
    executor.wait_for_all();
}

worker_matrix! {
    async_with_runtime {
        a_thousand_runtime_shaped_async_tasks_each_report_their_own_panic_1_thread(1),
        a_thousand_runtime_shaped_async_tasks_each_report_their_own_panic_2_threads(2),
        a_thousand_runtime_shaped_async_tasks_each_report_their_own_panic_3_threads(3),
        a_thousand_runtime_shaped_async_tasks_each_report_their_own_panic_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// Testcase: DependentAsync.Runtime
// ----------------------------------------------------------------------------

/// Upstream's `dependent_async_with_runtime(W)`: the case above over the
/// dependent-async pair instead, which is the same node with a dependency
/// count in front of it.
///
/// Upstream's own loop asserts nothing: it declares `futures` and then writes
/// `auto [t, f] = executor.dependent_async(..)` inside the loop, so every
/// handle is dropped where it is made and the `for(auto& fu : futures)` below
/// iterates an empty vector. What the case *means* to check is what is
/// checked here -- each of the 1024 handles reports the panic of its own body
/// -- which is upstream's `Async.Runtime` assertion over the dependent form,
/// and the one the comment above its loop describes.
fn dependent_async_with_runtime(workers: usize) {
    let context = format!("{workers} worker(s)");
    let executor = Executor::new(workers);

    let handles: Vec<_> = (0..FAILING_FREE_TASKS)
        .map(|_| {
            let (_task, handle) =
                executor.dependent_async_runtime(|_runtime: &mut Runtime<'_>| panic!("x"), []);
            handle
        })
        .collect();

    for (task, handle) in handles.into_iter().enumerate() {
        let error = handle.wait().expect_err("the body panicked");
        assert_failed_with_x(&error, &format!("{context}, task {task}"));
    }

    // Upstream's "silently caught by the task".
    executor.silent_dependent_async_runtime(|_runtime: &mut Runtime<'_>| panic!("x"), []);
    executor.wait_for_all();
}

worker_matrix! {
    dependent_async_with_runtime {
        a_thousand_runtime_shaped_dependent_async_tasks_each_report_their_own_panic_1_thread(1),
        a_thousand_runtime_shaped_dependent_async_tasks_each_report_their_own_panic_2_threads(2),
        a_thousand_runtime_shaped_dependent_async_tasks_each_report_their_own_panic_3_threads(3),
        a_thousand_runtime_shaped_dependent_async_tasks_each_report_their_own_panic_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// Testcase: RuntimeAsync
// ----------------------------------------------------------------------------

/// Upstream's `runtime_async_task(W)` from the commented-out `RuntimeAsync`
/// block: the three things a *graph's* runtime task sees when the work it
/// spawned fails, which are three different answers to "who is waiting".
///
/// Upstream leaves the whole case commented out and does not say why; this
/// port can run it, as it runs the commented-out half of `ModuleTask`, so it
/// is here rather than named and skipped. The three phases are upstream's own
/// and each changes exactly one thing:
///
/// - a child spawned with a **handle** delivers its panic to that handle, and
///   nothing reaches the task that spawned it. So the run succeeds and the
///   second task still runs;
/// - a **silent** child's panic goes up to the first anchor, which is the
///   runtime node itself while its body is entered, and
///   [`Runtime::try_corun`] is what takes it. A body that takes it has
///   handled it, so again the run succeeds;
/// - the same silent child with the corun left **bare**: `corun` re-raises,
///   the body unwinds, and the payload is the run's -- so the successor never
///   runs and the flag stays at zero.
///
/// A fourth phase is this port's own: the second one with the order between
/// the child and the corun forced the other way round, the body waiting for
/// the child to *finish* and only then coruning. The payload is still the
/// corun's. A corun-first body and a children-first body have to agree about
/// where a silent child's failure goes, or the delivery is a race that the
/// second phase samples rather than checks.
///
/// Two adaptations, and the second is the one that changes a line rather than
/// its spelling:
///
/// - the first phase's body **waits** on its child's handle, which blocks the
///   worker, so the case needs a worker to spare and upstream runs it at two
///   threads and up. That sweep is kept, and it is the reason this is the one
///   case in the file that does not start at one;
/// - upstream's third phase sleeps a second before its bare `corun` so that
///   the silent child has certainly failed by the time the corun looks. No
///   sleep is needed here and none is used, and what makes that true is the
///   *destination* rather than the visibility a sleep looks like it is
///   buying. Visibility is never in question: a child's payload is recorded
///   by `Job::record_panic` before its job releases the parent count the
///   corun waits on, so a corun that has observed the counter has observed
///   the payload. What a sleep would be covering is which anchor the payload
///   was recorded *on*, and an entered runtime node is an anchor from before
///   its body ran until after the last child has been counted off -- so a
///   child that fails before the corun is reached delivers to the same slot
///   as one that fails during it, which is the fourth phase's whole claim.
///   Sleeping would only make the case a second slower per worker count.
fn runtime_async_task(workers: usize) {
    let context = format!("{workers} worker(s)");
    let executor = Executor::new(workers);
    let flag = counter();
    let mut taskflow = Taskflow::new();

    // Upstream's first phase: `rt.async` twice, once cleanly and once into a
    // panic, with the second task of the graph reporting that the run went on.
    taskflow.emplace_runtime({
        let context = context.clone();
        move |runtime: &mut Runtime<'_>| {
            let one = runtime.spawn(|| 1);
            assert_eq!(
                one.wait().expect("the child did not panic"),
                1,
                "{context}: a runtime child's result reaches its handle"
            );

            let failed = runtime.spawn(|| -> i32 { panic!("z") });
            let error = failed.wait().expect_err("the child panicked");
            assert_failed_with(&error, "z", &context);
        }
    });
    taskflow.emplace({
        let flag = Arc::clone(&flag);
        move || {
            flag.store(1, Ordering::Relaxed);
        }
    });

    executor
        .run(&taskflow)
        .wait()
        .expect("a child's panic went to its own handle, not to the run");
    assert_eq!(count(&flag), 1, "{context}: the rest of the graph ran");

    // Upstream's second phase: a silent child, and a corun that takes what it
    // left behind. The body has handled the failure, so the run has none.
    flag.store(0, Ordering::Relaxed);
    taskflow.clear();
    let caught = taskflow.emplace_runtime({
        let context = context.clone();
        let flag = Arc::clone(&flag);
        move |runtime: &mut Runtime<'_>| {
            runtime.silent_async(|| panic!("a"));
            let error = runtime
                .try_corun()
                .expect_err("the silent child's panic landed on this node");
            assert_failed_with(&error, "a", &context);
            flag.store(1, Ordering::Relaxed);
        }
    });
    let after_caught = taskflow.emplace({
        let flag = Arc::clone(&flag);
        move || {
            flag.store(2, Ordering::Relaxed);
        }
    });
    taskflow.task(caught).precede([after_caught]);

    executor
        .run(&taskflow)
        .wait()
        .expect("the runtime body caught its silent child's panic");
    assert_eq!(
        count(&flag),
        2,
        "{context}: the successor of a body that caught its child ran"
    );

    // Upstream's third phase: the same silent child with the corun bare, so
    // the payload is re-raised and becomes the run's.
    flag.store(0, Ordering::Relaxed);
    taskflow.clear();
    let uncaught = taskflow.emplace_runtime({
        let flag = Arc::clone(&flag);
        move |runtime: &mut Runtime<'_>| {
            runtime.silent_async(|| panic!("a"));
            runtime.corun();
            // Upstream's comment says this line cannot be asserted on; here
            // it is simply not reached, because `corun` unwinds.
            flag.store(1, Ordering::Relaxed);
        }
    });
    let after_uncaught = taskflow.emplace({
        let flag = Arc::clone(&flag);
        move || {
            flag.store(2, Ordering::Relaxed);
        }
    });
    taskflow.task(uncaught).precede([after_uncaught]);

    let error = executor
        .run(&taskflow)
        .wait()
        .expect_err("the bare corun re-raised the child's panic");
    assert_failed_with(&error, "a", &context);
    assert_eq!(
        count(&flag),
        0,
        "{context}: neither the rest of the failing body nor its successor ran"
    );

    // This port's own fourth phase: the second one with the order between the
    // child and the corun forced the other way round. The child is a
    // dependent-async one so that the body's wait is a handshake rather than a
    // margin -- `FINISHED` is set after the walk that delivers the payload, so
    // a corun entered once `is_done` answers `true` is entered against a
    // failure that has already been delivered somewhere, and the only question
    // left is where it went.
    flag.store(0, Ordering::Relaxed);
    taskflow.clear();
    let waited = taskflow.emplace_runtime({
        let context = context.clone();
        let flag = Arc::clone(&flag);
        move |runtime: &mut Runtime<'_>| {
            let child = runtime.silent_dependent_async(|| panic!("a"), []);
            spin_until("the silent child to finish", || child.is_done());

            let error = runtime
                .try_corun()
                .expect_err("a child that failed first still reports here");
            assert_failed_with(&error, "a", &context);
            flag.store(1, Ordering::Relaxed);
        }
    });
    let after_waited = taskflow.emplace({
        let flag = Arc::clone(&flag);
        move || {
            flag.store(2, Ordering::Relaxed);
        }
    });
    taskflow.task(waited).precede([after_waited]);

    executor
        .run(&taskflow)
        .wait()
        .expect("the runtime body caught the child it had waited for");
    assert_eq!(
        count(&flag),
        2,
        "{context}: the successor of a body that caught its child ran"
    );
}

worker_matrix! {
    runtime_async_task {
        a_runtime_bodys_children_report_to_their_handle_their_node_or_the_run_2_threads(2),
        a_runtime_bodys_children_report_to_their_handle_their_node_or_the_run_3_threads(3),
        a_runtime_bodys_children_report_to_their_handle_their_node_or_the_run_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// Testcase: TaskGroup.Basics
// ----------------------------------------------------------------------------

/// Upstream's `Exception.TaskGroup.Basics`: a thread that is not a worker of
/// the executor cannot open a task group.
///
/// Upstream's constructor throws and the case catches anything at all;
/// [`Executor::task_group`] returns a [`CorunError`] instead, so the
/// assertion is which error it is. The refusal is the same refusal
/// [`Executor::corun`] makes, and for the same reason: waiting on a group
/// means coruning, which only a worker can do.
#[test]
fn only_a_worker_may_open_a_task_group() {
    let executor = Executor::new(4);
    let error = executor
        .task_group()
        .expect_err("the test thread is no worker of this executor");
    assert!(
        matches!(error, CorunError::NotAWorker),
        "unexpected error: {error:?}"
    );
}

// ----------------------------------------------------------------------------
// Testcase: TaskGroup.Async
// ----------------------------------------------------------------------------

/// Upstream's rounds in `task_group_async_task` and `runtime_async_task`:
/// `for(size_t r=0; r<100; r++)` and `for(size_t r=1; r<100; r++)`.
const CORUN_ROUNDS: usize = 100;

/// How many failing children each round of those two cases spawns, twice
/// over: upstream's `for(size_t i=0; i<100; i=i+1)`.
const FAILING_CHILDREN: usize = 100;

/// Upstream's `task_group_async_task(W)`: a hundred rounds of a hundred
/// failing children spawned into one group, each round once with handles and
/// once silently.
///
/// The two halves are the group's half of the same split `RuntimeAsync` makes
/// of a runtime node, and the rounds are what make it a statement about
/// *reuse*:
///
/// - with handles, every payload is delivered to its own handle, so the group
///   sees nothing, the corun is quiet, and all hundred children run --
///   nothing was cancelled, which `counter == 100` is what says;
/// - silently, the first payload to land marks the group's anchor, which is
///   the bit the scheduler reads as cancellation, so the children that had
///   not started are skipped. At most one child per worker can have got past
///   its own early-out by then, which is upstream's `counter <= W`.
///
/// Then the group is used again, a hundred times over: taking the payload is
/// what clears the mark, so a group that reported a failure and a fresh one
/// have to behave identically. A port that left the mark set would fail the
/// very next round's `counter == 100`.
///
/// `is_cancelled` stays false throughout, as upstream asserts, and that is
/// not the same question: it reads the parents *above* the group's anchor,
/// where the payload lands on the anchor itself.
///
/// Upstream sweeps one through eight workers and this stops at five, which
/// the module docs argue; the `counter <= W` bound is what the sweep is for
/// and it is asked at each of the five.
///
/// The whole body runs inside an [`Executor::spawn`] because only a worker
/// may open a group at all -- upstream's own reason for writing it that way,
/// and the case above is the other side of it.
fn task_group_async_task(workers: usize) {
    let context = format!("{workers} worker(s)");
    let executor = Arc::new(Executor::new(workers));

    executor
        .spawn({
            let executor = Arc::clone(&executor);
            move || {
                let group = executor
                    .task_group()
                    .expect("an async task runs on a worker, which may open a group");
                let counter = counter();

                for round in 0..CORUN_ROUNDS {
                    let context = format!("{context}, round {round}");

                    // Upstream's "async will propagate exceptions to future".
                    counter.store(0, Ordering::Relaxed);
                    let handles: Vec<AsyncHandle<()>> = (0..FAILING_CHILDREN)
                        .map(|_| {
                            let counter = Arc::clone(&counter);
                            group.spawn(move || {
                                counter.fetch_add(1, Ordering::Relaxed);
                                panic!("x")
                            })
                        })
                        .collect();

                    group.try_corun().unwrap_or_else(|error| {
                        panic!("{context}: a handled child's panic reached the group: {error:?}")
                    });
                    assert!(
                        !group.is_cancelled(),
                        "{context}: nothing above the group failed"
                    );
                    assert_eq!(
                        count(&counter),
                        FAILING_CHILDREN,
                        "{context}: no child was skipped, since the group was never marked"
                    );

                    // Upstream drops the futures its `tg.async` hands back,
                    // which is where the payloads went: each one is read
                    // here instead, which costs nothing after the corun and
                    // is the other half of "propagate exceptions to future".
                    for handle in handles {
                        let error = handle.wait().expect_err("the child panicked");
                        assert_failed_with_x(&error, &context);
                    }

                    // Upstream's "silent async will propagate exceptions to
                    // caller".
                    counter.store(0, Ordering::Relaxed);
                    for _ in 0..FAILING_CHILDREN {
                        let counter = Arc::clone(&counter);
                        group.silent_async(move || {
                            counter.fetch_add(1, Ordering::Relaxed);
                            panic!("x")
                        });
                    }

                    let error = group
                        .try_corun()
                        .expect_err("a silent child's panic landed on the group");
                    assert_failed_with_x(&error, &context);
                    let ran = count(&counter);
                    assert!(
                        ran <= workers,
                        "{context}: {ran} children ran past the mark, which is more than \
                         the {workers} that can have been in flight when it was set"
                    );
                    assert!(
                        !group.is_cancelled(),
                        "{context}: taking the payload cleared the mark"
                    );
                }
            }
        })
        .wait()
        .expect("no assertion inside the group's owner failed");
}

worker_matrix! {
    task_group_async_task {
        a_groups_failing_children_report_to_their_handles_or_to_the_group_1_thread(1),
        a_groups_failing_children_report_to_their_handles_or_to_the_group_2_threads(2),
        a_groups_failing_children_report_to_their_handles_or_to_the_group_3_threads(3),
        a_groups_failing_children_report_to_their_handles_or_to_the_group_4_threads(4),
        a_groups_failing_children_report_to_their_handles_or_to_the_group_5_threads(5),
    }
}

// ----------------------------------------------------------------------------
// Testcase: Runtime.Async
// ----------------------------------------------------------------------------

/// Upstream's second `runtime_async_task(W)`, the one behind
/// `Exception.Runtime.Async`: the case above with a runtime node's anchor in
/// place of a group's.
///
/// Upstream writes the two cases as one body with `tg.` swapped for `rt.`,
/// and that is the point of having both: a group's anchor is a `NodeBase` on
/// the opening task's frame and a runtime node's is the node the body is
/// running on, so the two reach the same walk by different routes and have to
/// answer the same. Everything
/// `a_groups_failing_children_report_to_their_handles_or_to_the_group` says
/// about the two halves, the rounds and `is_cancelled` holds here unchanged.
///
/// Upstream's own sweep is one through eight workers; this file's is one
/// through four, which is the sweep every other case here runs and reaches
/// both shapes the case is about -- one worker coruning its own children, and
/// several stealing them.
fn runtime_async_from_a_free_node(workers: usize) {
    let context = format!("{workers} worker(s)");
    let executor = Executor::new(workers);

    executor
        .spawn_runtime(move |runtime: &mut Runtime<'_>| {
            let counter = counter();

            for round in 0..CORUN_ROUNDS {
                let context = format!("{context}, round {round}");

                // Upstream's "async will propagate exceptions to future".
                counter.store(0, Ordering::Relaxed);
                let handles: Vec<AsyncHandle<()>> = (0..FAILING_CHILDREN)
                    .map(|_| {
                        let counter = Arc::clone(&counter);
                        runtime.spawn(move || {
                            counter.fetch_add(1, Ordering::Relaxed);
                            panic!("x")
                        })
                    })
                    .collect();

                runtime.try_corun().unwrap_or_else(|error| {
                    panic!("{context}: a handled child's panic reached this node: {error:?}")
                });
                assert!(
                    !runtime.is_cancelled(),
                    "{context}: nothing above this node failed"
                );
                assert_eq!(
                    count(&counter),
                    FAILING_CHILDREN,
                    "{context}: no child was skipped, since the node was never marked"
                );

                for handle in handles {
                    let error = handle.wait().expect_err("the child panicked");
                    assert_failed_with_x(&error, &context);
                }

                // Upstream's "silent async will propagate exceptions to
                // caller".
                counter.store(0, Ordering::Relaxed);
                for _ in 0..FAILING_CHILDREN {
                    let counter = Arc::clone(&counter);
                    runtime.silent_async(move || {
                        counter.fetch_add(1, Ordering::Relaxed);
                        panic!("x")
                    });
                }

                let error = runtime
                    .try_corun()
                    .expect_err("a silent child's panic landed on this node");
                assert_failed_with_x(&error, &context);
                let ran = count(&counter);
                assert!(
                    ran <= workers,
                    "{context}: {ran} children ran past the mark, which is more than the \
                     {workers} that can have been in flight when it was set"
                );
                assert!(
                    !runtime.is_cancelled(),
                    "{context}: taking the payload cleared the mark"
                );
            }
        })
        .wait()
        .expect("no assertion inside the runtime body failed");
}

worker_matrix! {
    runtime_async_from_a_free_node {
        a_runtime_nodes_failing_children_report_to_their_handles_or_to_the_node_1_thread(1),
        a_runtime_nodes_failing_children_report_to_their_handles_or_to_the_node_2_threads(2),
        a_runtime_nodes_failing_children_report_to_their_handles_or_to_the_node_3_threads(3),
        a_runtime_nodes_failing_children_report_to_their_handles_or_to_the_node_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// Testcase: Semaphore
// ----------------------------------------------------------------------------

/// Upstream's `semaphore1(W)`: a failing task between an `acquire` and the
/// `release` that was going to answer it, twice over.
///
/// The arithmetic is the whole case and it is about what a *cancelled* run
/// does to a count it took. `A` acquires, `B` fails, so `C` and `D` are
/// skipped -- and `D` was the task that releases, so the count stays taken: a
/// semaphore of one is left at zero by a run that failed, and nothing gives
/// it back. [`Semaphore::reset`] is what does, which is why upstream resets
/// before running the graph a second time, and why the second run reports the
/// same failure rather than hanging on a count nobody holds.
///
/// Upstream sweeps one through four workers; three is where this port stops,
/// since the chain is serial and the fourth worker changes nothing about a
/// graph in which at most one task is ever runnable.
fn semaphore1(workers: usize) {
    let context = format!("{workers} worker(s)");
    let executor = Executor::new(workers);
    let semaphore = Arc::new(Semaphore::new(1));
    let mut taskflow = Taskflow::new();

    let a = taskflow.emplace(|| {});
    let b = taskflow.emplace(|| panic!("exception"));
    let c = taskflow.emplace(|| {});
    let d = taskflow.emplace(|| {});
    taskflow.linearize([a, b, c, d]);
    taskflow.task(a).acquire(&semaphore);
    taskflow.task(d).release(&semaphore);

    assert_eq!(semaphore.value(), 1, "{context}: nothing has acquired yet");

    // Upstream's comment: when `B` fails, `D` will not run, and so the
    // semaphore is not released.
    let error = executor
        .run(&taskflow)
        .wait()
        .expect_err("`B` panicked, so the run failed");
    assert_failed_with(&error, "exception", &context);
    assert_eq!(
        semaphore.value(),
        0,
        "{context}: the count `A` took was never given back"
    );

    // Upstream's "reset the semaphore to a clean state before running the
    // taskflow again".
    semaphore.reset();
    assert_eq!(
        semaphore.value(),
        1,
        "{context}: the reset gave the count back"
    );

    let error = executor
        .run(&taskflow)
        .wait()
        .expect_err("`B` panicked again, so the second run failed too");
    assert_failed_with(&error, "exception", &context);
}

worker_matrix! {
    semaphore1 {
        a_failing_task_between_an_acquire_and_its_release_leaves_the_count_taken_1_thread(1),
        a_failing_task_between_an_acquire_and_its_release_leaves_the_count_taken_2_threads(2),
        a_failing_task_between_an_acquire_and_its_release_leaves_the_count_taken_3_threads(3),
    }
}

// ----------------------------------------------------------------------------
// Testcase: Algorithm
// ----------------------------------------------------------------------------

/// Upstream's `algorithm(W)`: a `for_each_index(0, 50, 1)` whose every body
/// fails, ten times over, each round's own run reporting that failure.
///
/// The ten rounds are upstream's own stress loop and are kept: each round
/// builds a fresh taskflow, so what they repeat is a whole algorithm task
/// being built, failing and torn down, which is the shape in which a run that
/// did not clean up after itself would show.
///
/// The worker sweep is what makes the case cover two deliveries rather than
/// one, because `for_each_index` is a runtime task: at one worker it takes
/// its own single-worker arm, so the whole walk runs inline in that task's
/// body and the panic is the node's own, while above one worker the walk is
/// spawned and the panic is a child's, reaching the run through the runtime
/// node's implicit anchor. Upstream's default partitioner is spelled out
/// here because this port takes it as an argument rather than defaulting it.
fn algorithm(workers: usize) {
    let executor = Executor::new(workers);

    for round in 1..=10 {
        let mut taskflow = Taskflow::new();
        taskflow.for_each_index(
            0_i64,
            50,
            1,
            |_| panic!("algorithm"),
            DefaultPartitioner::default(),
        );

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("every body of the range panicked");
        assert_failed_with(
            &error,
            "algorithm",
            &format!("{workers} worker(s), round {round}"),
        );
    }
}

worker_matrix! {
    algorithm {
        a_panicking_for_each_index_body_fails_the_run_1_thread(1),
        a_panicking_for_each_index_body_fails_the_run_2_threads(2),
        a_panicking_for_each_index_body_fails_the_run_3_threads(3),
        a_panicking_for_each_index_body_fails_the_run_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// Testcase: NestedAlgorithm
// ----------------------------------------------------------------------------

/// Upstream's `nested_algorithm(W)`: a `for_each_index(0, 10, 1)` whose every
/// body coruns a second taskflow of its own, itself a
/// `for_each_index(0, 10, 1)` whose every body fails, ten times over -- and
/// the failure two graphs down is what the outer run reports.
///
/// This is the one case here whose answer had to be read off the port's own
/// contract first, and that contract is upstream's: `DESIGN.md`'s "Waiting on
/// a nested graph comes in two shapes" entry keeps `corun` as the rethrowing
/// call, so a payload the corunning worker is handed goes to
/// `resume_unwind` and becomes the calling body's own failure unless that
/// body catches it. So the assertion below is upstream's assertion, not an
/// adaptation of it: nothing in the body catches, so the chunk task fails
/// with `"oops"`, and the walk carries it up to the run exactly as
/// [`an_uncaught_corun_failure_becomes_the_outer_runs_failure`] has it.
/// [`Executor::try_corun`] is the call a body that meant to inspect the
/// failure would make instead, and no body here means to.
fn nested_algorithm(workers: usize) {
    let executor = Arc::new(Executor::new(workers));

    for round in 1..=10 {
        let mut taskflow = Taskflow::new();
        taskflow.for_each_index(
            0_i64,
            10,
            1,
            {
                let executor = Arc::clone(&executor);
                move |_| {
                    let mut inner = Taskflow::new();
                    inner.for_each_index(
                        0_i64,
                        10,
                        1,
                        |_| panic!("oops"),
                        DefaultPartitioner::default(),
                    );
                    executor
                        .corun(&inner)
                        .expect("a chunk body runs on a worker, so it may corun");
                }
            },
            DefaultPartitioner::default(),
        );

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("the corun of the inner range re-raised its panic");
        assert_failed_with(
            &error,
            "oops",
            &format!("{workers} worker(s), round {round}"),
        );
    }
}

worker_matrix! {
    nested_algorithm {
        a_panic_under_a_corun_for_each_index_reaches_the_outer_run_1_thread(1),
        a_panic_under_a_corun_for_each_index_reaches_the_outer_run_2_threads(2),
        a_panic_under_a_corun_for_each_index_reaches_the_outer_run_3_threads(3),
        a_panic_under_a_corun_for_each_index_reaches_the_outer_run_4_threads(4),
    }
}

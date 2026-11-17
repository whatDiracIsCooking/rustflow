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
//! the subflow family (`SubflowTask`, `JoinedSubflow1..3`, `NestedSubflow`,
//! `NestedSubflow2`, `DetachedSubflow`), the `Runtime`, `Corun` and `Module`
//! cases, the async family (`AsyncTask`, `DependentAsyncTask`,
//! `SilentAsyncTask`, `RuntimeAsyncTask`), `Semaphore`, and the algorithm
//! cases (`ForEach`, `Reduce`, `Transform`, `Sort`, `Scan`, `Pipeline`...).
//! They arrive with M4 and later, together with the subflow and runtime
//! anchors that upstream's `_process_exception` walks to and this port does
//! not have.
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
use rustflow::{Executor, RunError};

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

/// Asserts that `error` is the panic the failing body raised.
fn assert_failed_with_x(error: &RunError, context: &str) {
    let RunError::Panicked(payload) = error else {
        panic!("{context}: unexpected error: {error:?}");
    };
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&"x"),
        "{context}: the payload is not the one `C` panicked with"
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

//! Invented: upstream has no unit test for observers at all, `test_observer`
//! not existing anywhere under Taskflow v4.1.0's `unittests/`. These cases are
//! this port's own, over the worker matrix #167 asks for.
//!
//! [`mixed_graph`] builds one task of each kind the acceptance names --
//! static, condition, subflow, runtime -- wired so that the condition always
//! takes the same branch and the subflow and runtime bodies spawn nothing,
//! so neither preempts its own node: every node the executor actually
//! dispatches is entered through the scheduler's dispatch exactly once. A
//! [`rustflow::Executor::spawn`]ed or
//! [`rustflow::Executor::silent_async`]ed body is the fifth kind, `async`,
//! run outside any graph.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::observer::chrome::ChromeObserver;
use rustflow::observer::{Observer, TaskView, WorkerView};
use rustflow::prelude::*;

#[macro_use]
mod common;

/// Counts [`Observer::set_up`], [`Observer::on_entry`] and
/// [`Observer::on_exit`], and nothing about the views each call is handed:
/// that half of the surface is exercised by [`rustflow::observer`]'s own
/// unit tests.
#[derive(Default)]
struct Counting {
    set_up_workers: AtomicUsize,
    entries: AtomicUsize,
    exits: AtomicUsize,
}

impl Observer for Counting {
    fn set_up(&self, num_workers: usize) {
        self.set_up_workers.store(num_workers, Ordering::SeqCst);
    }

    fn on_entry(&self, _worker: WorkerView<'_>, _task: TaskView<'_>) {
        self.entries.fetch_add(1, Ordering::SeqCst);
    }

    fn on_exit(&self, _worker: WorkerView<'_>, _task: TaskView<'_>) {
        self.exits.fetch_add(1, Ordering::SeqCst);
    }
}

/// Builds a graph of one static, one condition, one subflow and one runtime
/// task into `taskflow`, and returns how many of its nodes the executor
/// dispatches: four of the five it adds, the condition's other branch never
/// running.
///
/// `cond` always selects `selected` over `not_selected`; `subflow` and
/// `runtime` spawn no work of their own, so neither preempts its own node --
/// each is entered through the scheduler's dispatch exactly once, which is
/// what makes "nodes dispatched" and "tasks run" the same count here. See
/// `src/executor/worker.rs`'s `observer_prologue`/`observer_epilogue` docs
/// for the node kinds that do not have that property in general.
fn mixed_graph(taskflow: &mut Taskflow) -> usize {
    let a = taskflow.emplace(|| {});
    let selected = taskflow.emplace(|| {});
    let not_selected = taskflow.emplace(|| {});
    let cond = taskflow.emplace_condition(|| 0);
    let subflow = taskflow.emplace_subflow(|_subflow: &mut Subflow<'_>| {});
    let runtime = taskflow.emplace_runtime(|_runtime: &mut Runtime<'_>| {});

    taskflow
        .task(cond)
        .succeed([a])
        .precede([selected, not_selected]);

    // `a`, `cond`, `selected`, `subflow`, `runtime`: five dispatches.
    // `not_selected` is never scheduled.
    let _ = (subflow, runtime);
    5
}

/// A fresh executor registers no observer, which is the state the zero-cost
/// path -- `ObserverList::is_empty`'s one relaxed load -- is for.
#[test]
fn a_fresh_executor_has_no_observers() {
    let executor = Executor::new(2);
    assert_eq!(executor.num_observers(), 0);
}

/// `set_up` sees the executor's own worker count, and a registered observer's
/// `on_entry`/`on_exit` counts both equal the number of static, condition,
/// subflow, runtime and async tasks run -- #167's acceptance, read literally.
fn observer_sees_every_task_kind(workers: usize) {
    let executor = Executor::new(workers);
    let observer = executor.make_observer(Counting::default());
    assert_eq!(observer.set_up_workers.load(Ordering::SeqCst), workers);
    assert_eq!(executor.num_observers(), 1);

    let mut taskflow = Taskflow::new();
    let dispatched = mixed_graph(&mut taskflow);
    executor
        .run(&taskflow)
        .wait()
        .expect("no task of the graph panicked");

    // The fifth kind: a task with no graph at all.
    executor.silent_async(|| {});
    executor.wait_for_all();

    let expected = dispatched + 1;
    assert_eq!(
        observer.entries.load(Ordering::SeqCst),
        expected,
        "on_entry must fire once per task run"
    );
    assert_eq!(
        observer.exits.load(Ordering::SeqCst),
        expected,
        "on_exit must fire once per task run"
    );
}

worker_matrix! {
    observer_sees_every_task_kind {
        observer_sees_every_task_kind_1_worker(1),
        observer_sees_every_task_kind_2_workers(2),
        observer_sees_every_task_kind_3_workers(3),
        observer_sees_every_task_kind_4_workers(4),
    }
}

/// Two observers registered at once both see every task, each counting on
/// its own: neither's registration nor its counts can be the other's.
#[test]
fn two_observers_both_see_every_task() {
    let executor = Executor::new(4);
    let first = executor.make_observer(Counting::default());
    let second = executor.make_observer(Counting::default());
    assert_eq!(executor.num_observers(), 2);

    let mut taskflow = Taskflow::new();
    let dispatched = mixed_graph(&mut taskflow);
    executor
        .run(&taskflow)
        .wait()
        .expect("no task of the graph panicked");

    for observer in [&first, &second] {
        assert_eq!(observer.entries.load(Ordering::SeqCst), dispatched);
        assert_eq!(observer.exits.load(Ordering::SeqCst), dispatched);
    }
}

/// `remove_observer` stops the counts: a run after removal must not move
/// them, and `num_observers` must report the removal.
#[test]
fn remove_observer_stops_the_counts() {
    let executor = Executor::new(2);
    let observer = executor.make_observer(Counting::default());

    let mut before = Taskflow::new();
    before.emplace(|| {});
    executor
        .run(&before)
        .wait()
        .expect("no task of the graph panicked");

    let after_first_run = observer.entries.load(Ordering::SeqCst);
    assert_eq!(after_first_run, 1);

    executor.remove_observer(&observer);
    assert_eq!(executor.num_observers(), 0);

    let mut after = Taskflow::new();
    after.emplace(|| {});
    executor
        .run(&after)
        .wait()
        .expect("no task of the graph panicked");

    assert_eq!(
        observer.entries.load(Ordering::SeqCst),
        after_first_run,
        "a removed observer must not be notified of a later run"
    );
    assert_eq!(observer.exits.load(Ordering::SeqCst), after_first_run);
}

/// Removing an observer that was never registered on this executor -- here,
/// one already removed -- is a no-op rather than a panic.
#[test]
fn removing_an_already_removed_observer_is_a_no_op() {
    let executor = Executor::new(1);
    let observer = executor.make_observer(Counting::default());

    executor.remove_observer(&observer);
    executor.remove_observer(&observer);

    assert_eq!(executor.num_observers(), 0);
}

/// `make_observer` hands back a share the caller can keep using after it is
/// registered: dropping the `Arc<Executor>`'s own clone of it does not stop
/// the caller's.
#[test]
fn make_observer_returns_a_usable_share() {
    let executor = Executor::new(2);
    let observer: Arc<Counting> = executor.make_observer(Counting::default());

    let mut taskflow = Taskflow::new();
    taskflow.emplace(|| {});
    executor
        .run(&taskflow)
        .wait()
        .expect("no task of the graph panicked");

    assert_eq!(observer.entries.load(Ordering::SeqCst), 1);
    assert_eq!(observer.exits.load(Ordering::SeqCst), 1);
}

/// Splits a Chrome trace-event dump into its individual flat objects, by
/// brace depth alone: a hand-rolled reader for exactly the shape
/// [`ChromeObserver::dump`] produces (one flat object per event, no
/// nesting, and no task name here that embeds a literal `{` or `}`) rather
/// than a `serde_json` dev-dependency -- #172's acceptance asks for this
/// check, not a general-purpose JSON parser.
fn parse_trace_events(trace: &str) -> Vec<&str> {
    let trace = trace.trim();
    assert!(
        trace.starts_with('[') && trace.ends_with(']'),
        "a dump must be a JSON array: {trace:?}"
    );
    let inner = &trace[1..trace.len() - 1];

    let mut events = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (i, ch) in inner.char_indices() {
        match ch {
            '{' => {
                if depth == 0 {
                    start = i;
                }
                depth += 1;
            }
            '}' => {
                depth = depth.checked_sub(1).expect("a '}' always follows a '{'");
                if depth == 0 {
                    events.push(&inner[start..=i]);
                }
            }
            _ => {}
        }
    }
    events
}

/// Reads the raw text after `"key":` in one flat JSON object up to the next
/// top-level `,` or `}`, with surrounding quotes stripped if there were
/// any. Good enough for the plain decimal and plain-ASCII-name fields
/// [`ChromeObserver::dump`] writes; see [`parse_trace_events`] for why this
/// is hand-rolled rather than a real parser.
fn trace_field<'a>(event: &'a str, key: &str) -> &'a str {
    let needle = format!("\"{key}\":");
    let after = event
        .find(&needle)
        .unwrap_or_else(|| panic!("event has no {key:?} field: {event:?}"))
        + needle.len();
    let rest = &event[after..];
    let end = rest.find([',', '}']).unwrap_or(rest.len());
    rest[..end].trim_matches('"')
}

/// [`ChromeObserver`] records one segment per dispatch and dumps them as a
/// Chrome trace-event JSON array: `num_tasks` matches the dispatch count,
/// the dump parses back into exactly that many flat objects, and each
/// object's `dur` is non-negative and `tid` names one of this executor's
/// workers -- #172's acceptance, read literally.
#[test]
fn chrome_observer_dumps_a_valid_trace() {
    let num_workers = 3;
    let executor = Executor::new(num_workers);
    let observer = executor.make_observer(ChromeObserver::new());

    let mut taskflow = Taskflow::new();
    let dispatched = mixed_graph(&mut taskflow);
    executor
        .run(&taskflow)
        .wait()
        .expect("no task of the graph panicked");

    assert_eq!(observer.num_tasks(), dispatched);

    let trace = observer.dump_to_string();
    let events = parse_trace_events(&trace);
    assert_eq!(
        events.len(),
        dispatched,
        "the dump must hold exactly one event per dispatched task"
    );

    for event in events {
        let dur: i128 = trace_field(event, "dur")
            .parse()
            .unwrap_or_else(|e| panic!("dur is not a decimal integer in {event:?}: {e}"));
        assert!(
            dur >= 0,
            "a segment's duration must never be negative: {event:?}"
        );

        let tid: usize = trace_field(event, "tid")
            .parse()
            .unwrap_or_else(|e| panic!("tid is not a decimal integer in {event:?}: {e}"));
        assert!(
            tid < num_workers,
            "tid {tid} names no worker of this {num_workers}-worker executor"
        );
    }
}

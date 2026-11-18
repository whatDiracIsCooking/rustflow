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

use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::observer::chrome::ChromeObserver;
use rustflow::observer::tfprof::TfProfObserver;
use rustflow::observer::{Observer, TaskView, WorkerView};
use rustflow::prelude::*;

#[macro_use]
mod common;
#[path = "common/panics.rs"]
mod panics;

use panics::assert_panicked_with;

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
///
/// Every dispatched task is given its own name, distinct from every other
/// one -- `tfprof_observer_dumps_a_valid_block` below resolves each
/// recorded segment's name back out of the dump's string table and checks
/// it against this set.
fn mixed_graph(taskflow: &mut Taskflow) -> usize {
    let a = taskflow.emplace(|| {});
    let selected = taskflow.emplace(|| {});
    let not_selected = taskflow.emplace(|| {});
    let cond = taskflow.emplace_condition(|| 0);
    let subflow = taskflow.emplace_subflow(|_subflow: &mut Subflow<'_>| {});
    let runtime = taskflow.emplace_runtime(|_runtime: &mut Runtime<'_>| {});

    taskflow.task(a).name("a");
    taskflow.task(selected).name("selected");
    taskflow.task(not_selected).name("not_selected");
    taskflow.task(cond).name("cond");
    taskflow.task(subflow).name("subflow");
    taskflow.task(runtime).name("runtime");

    taskflow
        .task(cond)
        .succeed([a])
        .precede([selected, not_selected]);

    // `a`, `cond`, `selected`, `subflow`, `runtime`: five dispatches.
    // `not_selected` is never scheduled.
    let _ = (subflow, runtime);
    5
}

/// The names [`mixed_graph`] gives its five dispatched tasks, in no
/// particular order.
const MIXED_GRAPH_DISPATCHED_NAMES: [&str; 5] = ["a", "selected", "cond", "subflow", "runtime"];

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

/// Each dispatched task's own worker agrees with itself:
/// [`WorkerView::queue_size`] reads zero exactly when
/// [`WorkerView::is_queue_empty`] reports empty. With one worker there is no
/// sibling to steal between the two reads, so the two methods describing the
/// same queue cannot disagree -- unlike across workers, which
/// `WorkerView::queue_size`'s own doc says not to expect.
#[test]
fn queue_size_agrees_with_is_queue_empty_on_one_worker() {
    struct Checking;

    impl Observer for Checking {
        fn set_up(&self, _num_workers: usize) {}

        fn on_entry(&self, worker: WorkerView<'_>, _task: TaskView<'_>) {
            assert_eq!(
                worker.queue_size() == 0,
                worker.is_queue_empty(),
                "queue_size and is_queue_empty disagree on worker {}",
                worker.id()
            );
        }

        fn on_exit(&self, _worker: WorkerView<'_>, _task: TaskView<'_>) {}
    }

    let executor = Executor::new(1);
    let _observer = executor.make_observer(Checking);

    let mut taskflow = Taskflow::new();
    mixed_graph(&mut taskflow);
    executor
        .run(&taskflow)
        .wait()
        .expect("no task of the graph panicked");
}

/// A worker's `queue_size()` never exceeds the number of tasks this run
/// could possibly have put in it, no matter how many workers are racing to
/// steal from and push into that queue: a bound every reading must satisfy,
/// rather than a specific count that scheduling luck could make or break.
#[test]
fn queue_size_never_exceeds_the_tasks_in_the_run() {
    struct Bounding {
        max_queue_size: AtomicUsize,
        dispatched: AtomicUsize,
    }

    impl Observer for Bounding {
        fn set_up(&self, _num_workers: usize) {}

        fn on_entry(&self, worker: WorkerView<'_>, _task: TaskView<'_>) {
            self.dispatched.fetch_add(1, Ordering::SeqCst);
            self.max_queue_size
                .fetch_max(worker.queue_size(), Ordering::SeqCst);
        }

        fn on_exit(&self, _worker: WorkerView<'_>, _task: TaskView<'_>) {}
    }

    let executor = Executor::new(4);
    let observer = executor.make_observer(Bounding {
        max_queue_size: AtomicUsize::new(0),
        dispatched: AtomicUsize::new(0),
    });

    let mut taskflow = Taskflow::new();
    let dispatched = mixed_graph(&mut taskflow);
    executor
        .run(&taskflow)
        .wait()
        .expect("no task of the graph panicked");

    assert_eq!(observer.dispatched.load(Ordering::SeqCst), dispatched);
    assert!(
        observer.max_queue_size.load(Ordering::SeqCst) <= dispatched,
        "a worker's queue can never hold more than the tasks this run dispatched"
    );
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

/// A hand-rolled little-endian reader over [`TfProfObserver::dump`]'s binary
/// block -- #193's acceptance asks for exactly this, rather than a
/// `serde_json` dev-dependency, since the format isn't JSON to begin with.
struct TfpReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> TfpReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn u32(&mut self) -> u32 {
        let value = u32::from_le_bytes(self.bytes[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        value
    }

    fn u64(&mut self) -> u64 {
        let value = u64::from_le_bytes(self.bytes[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        value
    }

    fn u8(&mut self) -> u8 {
        let value = self.bytes[self.pos];
        self.pos += 1;
        value
    }

    fn varint(&mut self) -> u64 {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = self.u8();
            value |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                return value;
            }
            shift += 7;
        }
    }

    fn bytes(&mut self, len: usize) -> &'a [u8] {
        let slice = &self.bytes[self.pos..self.pos + len];
        self.pos += len;
        slice
    }
}

/// One decoded segment out of [`TfProfObserver::dump`]'s binary block: the
/// fields a reader needs to check the acceptance's claims, resolved out of
/// the varints, the fixed-width fields and the string table.
struct DecodedSegment {
    worker: u32,
    level: u32,
    name: String,
}

/// Parses `bytes` -- a full [`TfProfObserver::dump`] block -- into its
/// decoded segments, after checking the structural invariants the format's
/// own layout promises: the header's `str_table_len` is exactly the number
/// of bytes the string table occupies, every `(worker, level)` block's
/// `num_segs` accounts for exactly that many segments, and nothing is left
/// over at the end of the buffer.
fn parse_tfp_block(bytes: &[u8]) -> (u64, Vec<DecodedSegment>) {
    let mut reader = TfpReader::new(bytes);
    let uid = reader.u64();
    let _origin_us = reader.u64();
    let str_table_len = reader.u32();
    let num_wl = reader.u32();

    let string_table = reader
        .bytes(usize::try_from(str_table_len).unwrap())
        .to_vec();

    let mut segments = Vec::new();
    for _ in 0..num_wl {
        let worker = reader.u32();
        let level = reader.u32();
        let num_segs = reader.u32();

        let mut previous_beg_us = 0u64;
        for _ in 0..num_segs {
            let delta_beg = reader.varint();
            let _duration = reader.varint();
            let name_off = reader.u32();
            let packed = reader.u8();

            let beg_us = previous_beg_us + delta_beg;
            previous_beg_us = beg_us;

            let name_len = usize::from(packed & 0x1F);
            let name = if name_len == 0 {
                String::new()
            } else {
                let start = usize::try_from(name_off).unwrap();
                String::from_utf8(string_table[start..start + name_len].to_vec())
                    .expect("mixed_graph's task names are plain ASCII")
            };

            segments.push(DecodedSegment {
                worker,
                level,
                name,
            });
        }
    }

    assert_eq!(
        reader.pos,
        bytes.len(),
        "a dump must have nothing left over after its last segment"
    );

    (uid, segments)
}

/// [`TfProfObserver`] records one segment per dispatch and dumps them as
/// upstream's binary block: `num_tasks` matches the dispatch count, the
/// dump's `(worker, level)` blocks hold exactly that many segments between
/// them, every segment's worker names one of this executor's workers, and
/// resolving each segment's name out of the string table reproduces exactly
/// the set of names [`mixed_graph`] gave its dispatched tasks -- #193's
/// acceptance, read literally.
#[test]
fn tfprof_observer_dumps_a_valid_block() {
    let num_workers = 3;
    let executor = Executor::new(num_workers);
    let observer = executor.make_observer(TfProfObserver::new());

    let mut taskflow = Taskflow::new();
    let dispatched = mixed_graph(&mut taskflow);
    executor
        .run(&taskflow)
        .wait()
        .expect("no task of the graph panicked");

    assert_eq!(observer.num_tasks(), dispatched);

    let mut dump = Vec::new();
    observer
        .dump(&mut dump)
        .expect("writing to a Vec never fails");

    let (_uid, segments) = parse_tfp_block(&dump);
    assert_eq!(
        segments.len(),
        dispatched,
        "the dump must hold exactly one segment per dispatched task"
    );

    for segment in &segments {
        let worker = usize::try_from(segment.worker).unwrap();
        assert!(
            worker < num_workers,
            "worker {worker} names no worker of this {num_workers}-worker executor"
        );
    }

    let mut resolved_names: Vec<&str> = segments.iter().map(|s| s.name.as_str()).collect();
    resolved_names.sort_unstable();
    let mut expected_names = MIXED_GRAPH_DISPATCHED_NAMES;
    expected_names.sort_unstable();
    assert_eq!(
        resolved_names, expected_names,
        "every resolved name must be exactly the set mixed_graph gave its dispatched tasks"
    );

    // Every segment here is a top-level dispatch with nothing of its own
    // worker preempting above it, so every one lands at level 0.
    assert!(
        segments.iter().all(|segment| segment.level == 0),
        "no task in mixed_graph preempts its own node"
    );
}

/// [`TfProfObserver::summary`] reports the right task count per type for a
/// graph with a known mix of static, condition and subflow tasks, and its
/// per-worker counts sum to the total -- #193's acceptance, read literally.
#[test]
fn tfprof_observer_summary_reports_counts_by_type_and_worker() {
    let num_workers = 3;
    let executor = Executor::new(num_workers);
    let observer = executor.make_observer(TfProfObserver::new());

    let mut taskflow = Taskflow::new();
    let dispatched = mixed_graph(&mut taskflow);
    executor
        .run(&taskflow)
        .wait()
        .expect("no task of the graph panicked");

    let report = observer.summary_to_string();

    // mixed_graph dispatches two static tasks (`a`, `selected`), one
    // condition, one subflow and one runtime task.
    let mut expected_type_counts = HashMap::new();
    expected_type_counts.insert("static", 2u64);
    expected_type_counts.insert("condition", 1u64);
    expected_type_counts.insert("subflow", 1u64);
    expected_type_counts.insert("runtime", 1u64);

    for (type_name, expected_count) in &expected_type_counts {
        let count = type_row_count(&report, type_name)
            .unwrap_or_else(|| panic!("summary has no row for task type {type_name:?}:\n{report}"));
        assert_eq!(
            count, *expected_count,
            "task type {type_name:?} has the wrong count in:\n{report}"
        );
    }

    let total_by_type: u64 = expected_type_counts.values().sum();
    assert_eq!(total_by_type, u64::try_from(dispatched).unwrap());

    let worker_counts = worker_row_counts(&report);
    assert!(
        !worker_counts.is_empty(),
        "summary has no worker rows:\n{report}"
    );
    let total_by_worker: u64 = worker_counts.iter().sum();
    assert_eq!(
        total_by_worker,
        u64::try_from(dispatched).unwrap(),
        "per-worker counts must sum to the total dispatched:\n{report}"
    );
}

/// Finds the `[Aggregate Task Statistics]` row for `type_name` -- its first
/// whitespace-separated field -- and returns its `Count` column, the
/// second field.
fn type_row_count(report: &str, type_name: &str) -> Option<u64> {
    report.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        if fields.next()? != type_name {
            return None;
        }
        fields.next()?.parse().ok()
    })
}

/// Returns every `[Worker Utilization]` row's `Tasks` column, the second
/// whitespace-separated field of a row whose first field parses as a plain
/// worker id -- which the header, the rules and the `Total` row's "Total"
/// label all fail to do, leaving only the data rows.
fn worker_row_counts(report: &str) -> Vec<u64> {
    report
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let _id: usize = fields.next()?.parse().ok()?;
            fields.next()?.parse().ok()
        })
        .collect()
}

// ----------------------------------------------------------------------------
// The panic-path cells: a hook that fails
// ----------------------------------------------------------------------------

/// An observer whose hooks panic with `"x"`, one hook per instance.
///
/// One type over a [`Hook`] rather than three types, because what each case
/// below varies is only which call unwinds.
#[derive(Debug)]
struct Panicking(Hook);

/// Which of [`Panicking`]'s three hooks is the one that panics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Hook {
    SetUp,
    OnEntry,
    OnExit,
}

impl Panicking {
    /// Panics with `"x"` if `hook` is this instance's own. A bare `panic!`
    /// of a literal rather than an `assert!`, so that the payload is the
    /// `&str` `assert_panicked_with` reads back.
    fn fail_if(&self, hook: Hook) {
        if self.0 == hook {
            panic!("x");
        }
    }
}

impl Observer for Panicking {
    fn set_up(&self, _num_workers: usize) {
        self.fail_if(Hook::SetUp);
    }

    fn on_entry(&self, _worker: WorkerView<'_>, _task: TaskView<'_>) {
        self.fail_if(Hook::OnEntry);
    }

    fn on_exit(&self, _worker: WorkerView<'_>, _task: TaskView<'_>) {
        self.fail_if(Hook::OnExit);
    }
}

/// Runs a one-task taskflow on `executor` and asserts it ran, which is this
/// section's third claim: the executor is still usable.
///
/// The two hook cases below withdraw the failing observer first, and that is
/// the claim being made rather than a weakening of it. A hook that panics
/// panics on every dispatch it is called on, so a run made while it is still
/// registered fails by construction; what is in question is whether the
/// worker that called it survived to run anything at all.
fn still_runs(executor: &Executor) {
    let ran = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&ran);
    let mut taskflow = Taskflow::new();
    taskflow.emplace(move || {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    executor
        .run(&taskflow)
        .wait()
        .expect("the executor is reusable after a hook failed");
    assert_eq!(ran.load(Ordering::SeqCst), 1);
}

/// A panicking `set_up` is reported to whoever registered the observer, and
/// the observer is not registered: `make_observer` calls `set_up` on the
/// calling thread, before it inserts anything, so the payload comes back out
/// of that call rather than out of a run.
///
/// The one hook of the three whose failure is not a scheduling question at
/// all, and the reason this case asserts a payload where the rest of the
/// suite's panic cases assert a [`rustflow::RunError`]: no run exists yet.
#[test]
fn a_panicking_set_up_hook_is_reported_to_the_registering_caller() {
    let executor = Executor::new(2);

    let payload = panic::catch_unwind(AssertUnwindSafe(|| {
        executor.make_observer(Panicking(Hook::SetUp))
    }))
    .expect_err("`set_up` panicked, so `make_observer` never returned");

    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&"x"),
        "the payload keeps its identity"
    );
    assert_eq!(
        executor.num_observers(),
        0,
        "an observer whose `set_up` failed is not registered"
    );
    still_runs(&executor);
}

/// A panicking `on_entry` fails the run it was called from and leaves the
/// executor usable.
///
/// The bar #304 set, which took the first of its two answers: the hook is
/// called under a `catch_unwind` and its payload goes to the run, where
/// before it unwound out of the worker's scheduling loop, dropped the job in
/// flight without finishing it and hung the `wait` below for ever. What the
/// case pins is all three halves of that answer -- the error, the payload
/// being the *hook's* rather than the task's, and the executor still being
/// usable afterwards.
#[test]
fn a_panicking_on_entry_hook_fails_the_run_and_leaves_the_executor_usable() {
    let executor = Executor::new(2);
    let observer = executor.make_observer(Panicking(Hook::OnEntry));

    let mut taskflow = Taskflow::new();
    taskflow.emplace(|| {});
    let error = executor
        .run(&taskflow)
        .wait()
        .expect_err("`on_entry` panicked, so the run failed");

    // The payload is the hook's own and not a task's: the one task of the
    // graph above is an empty body that cannot panic.
    assert_panicked_with(error, "x");

    executor.remove_observer(&observer);
    still_runs(&executor);
}

/// The same for `on_exit`, which is the hook after the dispatch rather than
/// the one before it: #304's fix covers both, where before the unwind from
/// here skipped the node's own tear-down, so neither the semaphores it held
/// nor its successors were released.
#[test]
fn a_panicking_on_exit_hook_fails_the_run_and_leaves_the_executor_usable() {
    let executor = Executor::new(2);
    let observer = executor.make_observer(Panicking(Hook::OnExit));

    let mut taskflow = Taskflow::new();
    taskflow.emplace(|| {});
    let error = executor
        .run(&taskflow)
        .wait()
        .expect_err("`on_exit` panicked, so the run failed");

    // The payload is the hook's own and not a task's: the one task of the
    // graph above is an empty body that cannot panic.
    assert_panicked_with(error, "x");

    executor.remove_observer(&observer);
    still_runs(&executor);
}

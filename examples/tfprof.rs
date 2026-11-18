//! Invented: this port's own example, not a translation of anything
//! upstream ships. Taskflow has no example exercising its built-in
//! `TFProfObserver` directly -- only the bare `ObserverInterface`, which
//! `examples/observer.cpp` demonstrates and `examples/observer.rs`
//! translates. No `//!` provenance line for that reason: an invented file
//! carries none, the way `src/executor/scope.rs` and
//! `src/observer/tfprof.rs` already do.
//!
//! Demonstrates [`TfProfObserver`] (#193), and specifically
//! [`TfProfObserver::summary`] rather than [`TfProfObserver::dump`]:
//! `dump` writes upstream's binary `.tfp` block, meant for a @TFProf viewer
//! to read rather than for this example to pretty-print, where `summary` is
//! already the plain-text report `TfProfObserver`'s own module docs say
//! upstream's `Summary::dump` keeps. There is no upstream print to check
//! this example's output against, so this file's whole claim is in its own
//! assertions: the summary's per-task-type counts match the mix of tasks
//! this example's graph actually dispatches.

use std::collections::HashMap;

use rustflow::Executor;
use rustflow::observer::tfprof::TfProfObserver;
use rustflow::prelude::*;

fn main() {
    let executor = Executor::new(2);
    let observer = executor.make_observer(TfProfObserver::new());

    // One static task (A), a condition that always selects its first
    // successor (cond), a second static task on that branch (selected) and
    // a subflow with an empty body so it does not preempt its own node --
    // see `tests/observers.rs`'s `mixed_graph` for why an empty subflow
    // body keeps "tasks dispatched" and "segments recorded" equal.
    let mut taskflow = Taskflow::with_name("tfprof");
    let a = taskflow.emplace(|| println!("TaskA"));
    let selected = taskflow.emplace(|| println!("TaskSelected"));
    let not_selected = taskflow.emplace(|| println!("TaskNotSelected"));
    let cond = taskflow.emplace_condition(|| 0);
    let subflow = taskflow.emplace_subflow(|_subflow: &mut Subflow<'_>| {});

    taskflow.task(a).name("A").precede([cond]);
    taskflow
        .task(cond)
        .name("cond")
        .precede([selected, not_selected]);
    taskflow.task(selected).name("selected").precede([subflow]);
    taskflow.task(not_selected).name("not_selected");
    taskflow.task(subflow).name("subflow");

    assert!(executor.run(&taskflow).wait().is_ok());

    // Dispatched: A, cond, selected, subflow -- four. not_selected is never
    // scheduled, the condition always choosing index 0.
    assert_eq!(observer.num_tasks(), 4);

    let report = observer.summary_to_string();
    println!("{report}");

    let counts = aggregate_type_counts(&report);
    assert_eq!(counts.get("static").copied(), Some(2));
    assert_eq!(counts.get("condition").copied(), Some(1));
    assert_eq!(counts.get("subflow").copied(), Some(1));

    let total: u64 = counts.values().sum();
    assert_eq!(total, 4);
}

/// Every task-type name [`TfProfObserver::summary`]'s `[Aggregate Task
/// Statistics]` table can head a row with.
const TASK_TYPE_NAMES: [&str; 7] = [
    "static",
    "subflow",
    "condition",
    "module",
    "async",
    "placeholder",
    "runtime",
];

/// Reads the `[Aggregate Task Statistics]` table's `Type` and `Count`
/// columns out of a [`TfProfObserver::summary`] report, by field position:
/// a row's first whitespace-separated field is the type name, its second
/// the count.
fn aggregate_type_counts(report: &str) -> HashMap<&str, u64> {
    let mut counts = HashMap::new();
    for line in report.lines() {
        let mut fields = line.split_whitespace();
        let Some(type_name) = fields.next() else {
            continue;
        };
        if !TASK_TYPE_NAMES.contains(&type_name) {
            continue;
        }
        if let Some(count) = fields.next().and_then(|field| field.parse().ok()) {
            counts.insert(type_name, count);
        }
    }
    counts
}

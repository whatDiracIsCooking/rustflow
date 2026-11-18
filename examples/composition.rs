//! Translated from `examples/composition.cpp` (Taskflow v4.1.0).
//!
//! Demonstrates composing one taskflow into another with
//! [`FlowBuilder::composed_of`](rustflow::FlowBuilder::composed_of).
//!
//! `composed_of` takes anything [`Composable`](rustflow::flow_builder::Composable)
//! rather than a `Taskflow&` the way upstream's template over its
//! `GraphLike` concept does; the implementor used throughout this file is
//! [`Arc<Taskflow>`], which is how a composed target outlives the call that
//! composes it -- upstream leaves that to the caller's own storage, this
//! port to a share of it. `composed_of` itself returns a bare
//! [`Task`](rustflow::Task) index rather than a handle with upstream's
//! chained `.name(...)`, so naming a module task here is always its own
//! statement -- `taskflow.task(module).name(...)` -- rather than one
//! upstream writes on the same line as the call that created it.
//!
//! Upstream prints as each task runs; this port prints and asserts the
//! counts a faithful run of the same graphs produces, so a non-zero exit is
//! the test.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::prelude::*;

/// Composes `f1` into a four-task `f2`, runs it three times, and checks that
/// `f1`'s own task ran once per run of `f2`.
///
/// Upstream's `composition_example_1`.
fn composition_example_1() {
    println!("Composition example 1");

    let executor = Executor::default();

    // f1 has three independent tasks, two of which precede the third.
    let f1_runs = Arc::new(AtomicUsize::new(0));
    let mut f1 = Taskflow::with_name("F1");
    let f1a = f1.emplace(|| println!("F1 TaskA"));
    let f1b = f1.emplace(|| println!("F1 TaskB"));
    let f1c = {
        let f1_runs = Arc::clone(&f1_runs);
        f1.emplace(move || {
            println!("F1 TaskC");
            f1_runs.fetch_add(1, Ordering::Relaxed);
        })
    };
    f1.task(f1a).name("f1A");
    f1.task(f1b).name("f1B");
    f1.task(f1c).name("f1C");
    f1.task(f1a).precede([f1c]);
    f1.task(f1b).precede([f1c]);
    let f1 = Arc::new(f1);

    // f2A ---
    //        |----> f2C ----> f1_module_task ----> f2D
    // f2B ---
    let mut f2 = Taskflow::with_name("F2");
    let f2a = f2.emplace(|| println!("  F2 TaskA"));
    let f2b = f2.emplace(|| println!("  F2 TaskB"));
    let f2c = f2.emplace(|| println!("  F2 TaskC"));
    let f2d = f2.emplace(|| println!("  F2 TaskD"));
    f2.task(f2a).name("f2A");
    f2.task(f2b).name("f2B");
    f2.task(f2c).name("f2C");
    f2.task(f2d).name("f2D");

    f2.task(f2a).precede([f2c]);
    f2.task(f2b).precede([f2c]);

    let f1_module_task = f2.composed_of(Arc::clone(&f1));
    f2.task(f1_module_task).name("module");
    f2.task(f2c).precede([f1_module_task]);
    f2.task(f1_module_task).precede([f2d]);

    let dot = f2.dump();
    println!("{dot}");
    assert_eq!(
        dot,
        concat!(
            "digraph Taskflow {\n",
            "  compound=true;\n",
            "  subgraph cluster_p0 {\n",
            "    label=\"Taskflow: F2\";\n",
            "    p0[label=\"f2A\" ];\n",
            "    p0 -> p2;\n",
            "    p1[label=\"f2B\" ];\n",
            "    p1 -> p2;\n",
            "    p2[label=\"f2C\" ];\n",
            "    p2 -> p4;\n",
            "    p3[label=\"f2D\" ];\n",
            "    p4[shape=box3d, color=blue, label=\"module\"];\n",
            "    p4 -> p3;\n",
            "  }\n",
            "    subgraph cluster_p1 {\n",
            "      label=\"module\";\n",
            "      p1_0[label=\"f1A\" ];\n",
            "      p1_0 -> p1_2;\n",
            "      p1_1[label=\"f1B\" ];\n",
            "      p1_1 -> p1_2;\n",
            "      p1_2[label=\"f1C\" ];\n",
            "    }\n",
            "}\n",
        )
    );

    assert!(executor.run_n(&f2, 3).wait().is_ok());

    // One run of f2 runs the whole of f1 once, through the module task.
    assert_eq!(f1_runs.load(Ordering::Relaxed), 3);
}

/// Composes `f1` into `f2`, `f2` into `f3`, and `f3` plus `f2` again into
/// `f4`, then runs `f4` with `run_until` three times, checking that each
/// `f4` run runs `f3`'s own task exactly once -- so the predicate's reported
/// iteration counts hold for the whole composed tree, not just its top task.
///
/// Upstream's `composition_example_2`.
fn composition_example_2() {
    println!("Composition example 2");

    let executor = Executor::default();

    // f1 has two independent tasks.
    let mut f1 = Taskflow::with_name("F1");
    let f1a = f1.emplace(|| println!("F1 TaskA"));
    let f1b = f1.emplace(|| println!("F1 TaskB"));
    f1.task(f1a).name("f1A");
    f1.task(f1b).name("f1B");
    let f1 = Arc::new(f1);

    //  f2A ---
    //         |----> f2C
    //  f2B ---
    //
    //  f1_module_task
    let mut f2 = Taskflow::with_name("F2");
    let f2a = f2.emplace(|| println!("  F2 TaskA"));
    let f2b = f2.emplace(|| println!("  F2 TaskB"));
    let f2c = f2.emplace(|| println!("  F2 TaskC"));
    f2.task(f2a).name("f2A");
    f2.task(f2b).name("f2B");
    f2.task(f2c).name("f2C");

    f2.task(f2a).precede([f2c]);
    f2.task(f2b).precede([f2c]);
    let f1_module_task = f2.composed_of(Arc::clone(&f1));
    f2.task(f1_module_task).name("module_of_f1");
    let f2 = Arc::new(f2);

    // f3 has a module task (f2) and a regular task.
    let f3_runs = Arc::new(AtomicUsize::new(0));
    let mut f3 = Taskflow::with_name("F3");
    let f2_module_task_in_f3 = f3.composed_of(Arc::clone(&f2));
    f3.task(f2_module_task_in_f3).name("module_of_f2");
    let f3a = {
        let f3_runs = Arc::clone(&f3_runs);
        f3.emplace(move || {
            println!("      F3 TaskA");
            f3_runs.fetch_add(1, Ordering::Relaxed);
        })
    };
    f3.task(f3a).name("f3A");
    let f3 = Arc::new(f3);

    // f4: f3_module_task -> f2_module_task
    let mut f4 = Taskflow::new();
    f4.set_name("F4");
    let f3_module_task = f4.composed_of(Arc::clone(&f3));
    f4.task(f3_module_task).name("module_of_f3");
    let f2_module_task = f4.composed_of(Arc::clone(&f2));
    f4.task(f2_module_task).name("module_of_f2");
    f4.task(f3_module_task).precede([f2_module_task]);

    let dot = f4.dump();
    println!("{dot}");
    assert_eq!(
        dot,
        concat!(
            "digraph Taskflow {\n",
            "  compound=true;\n",
            "  subgraph cluster_p0 {\n",
            "    label=\"Taskflow: F4\";\n",
            "    p0[shape=box3d, color=blue, label=\"module_of_f3\"];\n",
            "    p0 -> p1;\n",
            "    p1[shape=box3d, color=blue, label=\"module_of_f2\"];\n",
            "  }\n",
            "    subgraph cluster_p2 {\n",
            "      label=\"module_of_f2\";\n",
            "      p2_0[label=\"f2A\" ];\n",
            "      p2_0 -> p2_2;\n",
            "      p2_1[label=\"f2B\" ];\n",
            "      p2_1 -> p2_2;\n",
            "      p2_2[label=\"f2C\" ];\n",
            "      p2_3[shape=box3d, color=blue, label=\"module_of_f1\"];\n",
            "    }\n",
            "      subgraph cluster_p3 {\n",
            "        label=\"module_of_f1\";\n",
            "        p3_0[label=\"f1A\" ];\n",
            "        p3_1[label=\"f1B\" ];\n",
            "      }\n",
            "    subgraph cluster_p1 {\n",
            "      label=\"module_of_f3\";\n",
            "      p1_0[shape=box3d, color=blue, label=\"module_of_f2\"];\n",
            "      p1_1[label=\"f3A\" ];\n",
            "    }\n",
            "}\n",
        )
    );

    // Upstream's three `run_until` calls each decrement a captured counter
    // one lower than the last, running `f4` once per check that is not yet
    // zero: `iter = 1, 2, 3` therefore run `f4` 1, 2 and 3 times in turn,
    // for `1 + 2 + 3 = 6` runs overall -- which this checks through `f3A`,
    // the one task in the whole composed tree that runs exactly once per
    // run of `f4`.
    run_until_counting_down(&executor, &f4, 1, "First run_until finished");

    // Upstream's second call discards the returned future without a `get`,
    // relying on it being non-blocking to drop. A `RunHandle` here blocks in
    // `Drop` instead -- see its module docs -- so dropping this one unbound
    // still waits for the run, and the three calls observe the same order
    // either way.
    run_until_counting_down(&executor, &f4, 2, "Second run_until finished");

    run_until_counting_down(&executor, &f4, 3, "Third run_until finished");

    assert_eq!(f3_runs.load(Ordering::Relaxed), 1 + 2 + 3);
}

/// Runs `taskflow` with a predicate that counts `iterations` down to zero,
/// printing a blank line on every check as upstream's own predicate does,
/// then the finish message once the run stops.
fn run_until_counting_down(
    executor: &Executor,
    taskflow: &Taskflow,
    iterations: i64,
    finished: &'static str,
) {
    let mut iter = iterations;
    executor
        .run_until_with(
            taskflow,
            move || {
                println!();
                // Upstream's postfix `iter-- == 0`: compare first, then
                // decrement regardless of the answer.
                let checked = iter;
                iter -= 1;
                checked == 0
            },
            move || println!("{finished}"),
        )
        .wait()
        .expect("no task panicked");
}

fn main() {
    composition_example_1();
    composition_example_2();
}

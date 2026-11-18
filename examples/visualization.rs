//! Translated from `examples/visualization.cpp` (Taskflow v4.1.0).
//!
//! Demonstrates [`Taskflow::dump`](rustflow::Taskflow::dump) to visualize a
//! taskflow graph in Graphviz DOT.
//!
//! Upstream writes the DOT text straight to `std::cout`; this port builds the
//! same text as a `String` -- see the [`dump`](rustflow::dump) module docs
//! for the one way its content departs from upstream's own (an arena index
//! standing in for an address that cannot be reproduced run to run) -- and
//! prints **and** asserts it, so a non-zero exit is the test.

use rustflow::Executor;
use rustflow::prelude::*;

fn main() {
    let mut taskflow = Taskflow::with_name("Visualization Demo");

    // ------------------------------------------------------
    // Static Tasking
    // ------------------------------------------------------
    let a = taskflow.emplace(|| println!("Task A"));
    let b = taskflow.emplace(|| println!("Task B"));
    let c = taskflow.emplace(|| println!("Task C"));
    let d = taskflow.emplace(|| println!("Task D"));
    let e = taskflow.emplace(|| println!("Task E"));

    taskflow.task(a).precede([b, c, e]);
    taskflow.task(c).precede([d]);
    taskflow.task(b).precede([d, e]);

    println!("[dump without name assignment]");
    let unnamed = taskflow.dump();
    println!("{unnamed}");
    assert_eq!(
        unnamed,
        concat!(
            "digraph Taskflow {\n",
            "  compound=true;\n",
            "  subgraph cluster_p0 {\n",
            "    label=\"Taskflow: Visualization Demo\";\n",
            "    p0[label=\"p0\" ];\n",
            "    p0 -> p1;\n",
            "    p0 -> p2;\n",
            "    p0 -> p4;\n",
            "    p1[label=\"p1\" ];\n",
            "    p1 -> p3;\n",
            "    p1 -> p4;\n",
            "    p2[label=\"p2\" ];\n",
            "    p2 -> p3;\n",
            "    p3[label=\"p3\" ];\n",
            "    p4[label=\"p4\" ];\n",
            "  }\n",
            "}\n",
        )
    );

    println!("[dump with name assignment]");
    taskflow.task(a).name("A");
    taskflow.task(b).name("B");
    taskflow.task(c).name("C");
    taskflow.task(d).name("D");
    taskflow.task(e).name("E");

    // If the graph contains solely static tasks, it can be dumped without
    // running it first.
    let named = taskflow.dump();
    println!("{named}");
    assert_eq!(
        named,
        concat!(
            "digraph Taskflow {\n",
            "  compound=true;\n",
            "  subgraph cluster_p0 {\n",
            "    label=\"Taskflow: Visualization Demo\";\n",
            "    p0[label=\"A\" ];\n",
            "    p0 -> p1;\n",
            "    p0 -> p2;\n",
            "    p0 -> p4;\n",
            "    p1[label=\"B\" ];\n",
            "    p1 -> p3;\n",
            "    p1 -> p4;\n",
            "    p2[label=\"C\" ];\n",
            "    p2 -> p3;\n",
            "    p3[label=\"D\" ];\n",
            "    p4[label=\"E\" ];\n",
            "  }\n",
            "}\n",
        )
    );

    // ------------------------------------------------------
    // Dynamic Tasking
    // ------------------------------------------------------
    taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
        let s1 = subflow.emplace(|| print!("subflow task1"));
        subflow.task(s1).name("s1");
        let s2 = subflow.emplace(|| print!("subflow task2"));
        subflow.task(s2).name("s2");
        let s3 = subflow.emplace(|| print!("subflow task3"));
        subflow.task(s3).name("s3");
    });

    // In order to visualize subflow tasks, the taskflow must run first, to
    // spawn the dynamic tasks.
    let executor = Executor::default();
    executor.run(&taskflow).wait().expect("no task panicked");

    // Upstream's own program never calls `retain(true)` on this subflow,
    // so -- on both sides of the port -- s1, s2 and s3 are already gone by
    // the time this dump runs: a subflow's subgraph is cleared on an
    // implicit join unless the body opts out, the same rule `run.rs`'s
    // example demonstrates the other way. What is left to see is the
    // subflow task itself, dumped as the plain, unnamed node an empty
    // subgraph produces.
    let after_run = taskflow.dump();
    println!("{after_run}");
    assert_eq!(
        after_run,
        concat!(
            "digraph Taskflow {\n",
            "  compound=true;\n",
            "  subgraph cluster_p0 {\n",
            "    label=\"Taskflow: Visualization Demo\";\n",
            "    p0[label=\"A\" ];\n",
            "    p0 -> p1;\n",
            "    p0 -> p2;\n",
            "    p0 -> p4;\n",
            "    p1[label=\"B\" ];\n",
            "    p1 -> p3;\n",
            "    p1 -> p4;\n",
            "    p2[label=\"C\" ];\n",
            "    p2 -> p3;\n",
            "    p3[label=\"D\" ];\n",
            "    p4[label=\"E\" ];\n",
            "    p5[label=\"p5\" ];\n",
            "  }\n",
            "}\n",
        )
    );
}

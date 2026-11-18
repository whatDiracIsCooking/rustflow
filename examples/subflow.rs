//! Translated from `examples/subflow.cpp` (Taskflow v4.1.0).
//!
//! `TaskA` precedes `TaskB` and `TaskC`, both of which precede `TaskD`.
//! `TaskB` is a subflow task: while it runs, it spawns three more tasks --
//! `B1`, `B2` and `B3` -- with `B3` depending on the other two, then retains
//! the subgraph so it survives the join and shows up in the dump below.
//!
//! Upstream's program only *prints* as it goes and dumps the graph for a
//! human to read. This milestone's examples print *and* **assert**: every
//! line upstream shows is also checked with `assert!` or `assert_eq!`, so a
//! non-zero exit from the binary is itself the test. See the README's
//! "Examples" section.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use rustflow::Executor;
use rustflow::prelude::*;

fn main() {
    // Upstream's `tf::Executor executor(4)`.
    let executor = Executor::new(4);
    let mut taskflow = Taskflow::with_name("Subflow Demo");

    // Upstream captures `cap` into B's closure directly; here it is an `Arc`
    // so the three subtasks -- each a separate `'static` closure -- can each
    // hold a share of it rather than a reference into B's own stack frame.
    let cap: Arc<Vec<i64>> = Arc::new(vec![1, 2, 3, 4, 5, 6, 7, 8]);
    let sum = Arc::new(AtomicI64::new(0));
    let product = Arc::new(AtomicI64::new(0));
    let difference = Arc::new(AtomicI64::new(0));

    let a = taskflow.emplace(|| println!("TaskA"));
    let b = taskflow.emplace_subflow({
        let cap = Arc::clone(&cap);
        let sum = Arc::clone(&sum);
        let product = Arc::clone(&product);
        let difference = Arc::clone(&difference);
        move |subflow: &mut Subflow<'_>| {
            println!("TaskB is spawning B1, B2, and B3 ...");

            let b1 = {
                let cap = Arc::clone(&cap);
                let sum = Arc::clone(&sum);
                subflow.emplace(move || {
                    let total: i64 = cap.iter().sum();
                    println!("  Subtask B1: reduce sum = {total}");
                    sum.store(total, Ordering::Relaxed);
                })
            };
            let b2 = {
                let cap = Arc::clone(&cap);
                let product = Arc::clone(&product);
                subflow.emplace(move || {
                    let total: i64 = cap.iter().product();
                    println!("  Subtask B2: reduce multiply = {total}");
                    product.store(total, Ordering::Relaxed);
                })
            };
            let b3 = {
                let cap = Arc::clone(&cap);
                let difference = Arc::clone(&difference);
                subflow.emplace(move || {
                    let total = cap.iter().fold(0i64, |acc, &x| acc - x);
                    println!("  Subtask B3: reduce minus = {total}");
                    difference.store(total, Ordering::Relaxed);
                })
            };

            subflow.task(b1).name("B1").precede([b3]);
            subflow.task(b2).name("B2").precede([b3]);
            subflow.task(b3).name("B3");

            // Retain the subflow for visualization purposes, upstream's
            // `subflow.retain(true)` under this port's getter/setter split --
            // see `Subflow::set_retain`'s docs for why the two are not one
            // overloaded name here.
            subflow.set_retain(true);
        }
    });
    let c = taskflow.emplace(|| println!("TaskC"));
    let d = taskflow.emplace(|| println!("TaskD"));

    taskflow.task(a).name("A").precede([b, c]);
    taskflow.task(b).name("B").precede([d]);
    taskflow.task(c).name("C").precede([d]);
    taskflow.task(d).name("D");

    // Upstream's `executor.run_n(taskflow, 3).get()`: block until the third
    // run finishes.
    assert!(executor.run_n(&taskflow, 3).wait().is_ok());

    // Examine the graph. Note that we must have run the subflow at least
    // once for it to be created -- the run above did that three times over.
    let dot = taskflow.dump();
    println!("{dot}");
    assert_eq!(
        dot,
        concat!(
            "digraph Taskflow {\n",
            "  compound=true;\n",
            "  subgraph cluster_p0 {\n",
            "    label=\"Taskflow: Subflow Demo\";\n",
            "    p0[label=\"A\" ];\n",
            "    p0 -> p1;\n",
            "    p0 -> p2;\n",
            "    p1[label=\"B\" ];\n",
            "    p1 -> p3;\n",
            "    subgraph cluster_p1_sub {\n",
            "      label=\"Subflow: B\";\n",
            "      color=blue;\n",
            "      p1_sub_0[label=\"B1\" ];\n",
            "      p1_sub_0 -> p1_sub_2;\n",
            "      p1_sub_1[label=\"B2\" ];\n",
            "      p1_sub_1 -> p1_sub_2;\n",
            "      p1_sub_2[label=\"B3\" ];\n",
            "    }\n",
            "    p1_sub_0 -> p1 [ltail=cluster_p1_sub style=dashed color=blue];\n",
            "    p2[label=\"C\" ];\n",
            "    p2 -> p3;\n",
            "    p3[label=\"D\" ];\n",
            "  }\n",
            "}\n",
        )
    );

    // What upstream only shows a human by eye, the port checks outright: the
    // three subtasks computed what the upstream program's `printf`s show.
    assert_eq!(sum.load(Ordering::Relaxed), 36);
    assert_eq!(product.load(Ordering::Relaxed), 40_320);
    assert_eq!(difference.load(Ordering::Relaxed), -36);
}

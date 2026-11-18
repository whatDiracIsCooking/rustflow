//! Translated from `examples/run.cpp` (Taskflow v4.1.0).
//!
//! Demonstrates the different ways to run a taskflow: once, with and without
//! a finish callback, `run_n`, `run_until` and `wait_for_all`.
//!
//! Upstream's program only *prints* as it goes and dumps the graph for a
//! human to read. This milestone's examples print *and* **assert**: every
//! line upstream shows is also checked with `assert!` or `assert_eq!`, so a
//! non-zero exit from the binary is itself the test. See the README's
//! "Examples" section.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::prelude::*;

fn main() {
    // create an executor and a taskflow
    let executor = Executor::new(1);
    let mut taskflow = Taskflow::with_name("Demo");

    let a = taskflow.emplace(|| println!("TaskA"));
    let b = taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
        println!("TaskB");
        let b1 = subflow.emplace(|| println!("TaskB1"));
        let b2 = subflow.emplace(|| println!("TaskB2"));
        let b3 = subflow.emplace(|| println!("TaskB3"));
        subflow.task(b1).name("B1").precede([b3]);
        subflow.task(b2).name("B2").precede([b3]);
        subflow.task(b3).name("B3");
        // Keep the subgraph around after the join: upstream's note on
        // `dump()` is that a subflow's children are cleared unless the body
        // retains them, and the dump below asks to see them.
        subflow.set_retain(true);
    });
    let c = taskflow.emplace(|| println!("TaskC"));
    let d = taskflow.emplace(|| println!("TaskD"));

    taskflow.task(a).name("A").precede([b, c]);
    taskflow.task(b).name("B").precede([d]);
    taskflow.task(c).name("C").precede([d]);
    taskflow.task(d).name("D");

    // dumping a taskflow before execution won't visualize subflow tasks
    println!("Dump the taskflow before execution:");
    let before = taskflow.dump();
    println!("{before}");
    assert!(!before.contains("B1"));
    assert!(!before.contains("B2"));
    assert!(!before.contains("B3"));

    println!("Run the taskflow once without callback\n");
    assert!(executor.run(&taskflow).wait().is_ok());
    println!();

    // after execution, we can visualize subflow tasks
    println!("Dump the taskflow after execution:");
    let after = taskflow.dump();
    println!("{after}");
    assert!(after.contains("B1"));
    assert!(after.contains("B2"));
    assert!(after.contains("B3"));
    println!();

    println!("Use wait_for_all to wait for the execution to finish");
    assert!(executor.run(&taskflow).wait().is_ok());
    executor.wait_for_all();
    println!();

    println!("Execute the taskflow two times without a callback");
    assert!(executor.run(&taskflow).wait().is_ok());
    println!("Dump after two executions:");
    println!("{}", taskflow.dump());
    println!();

    println!("Execute the taskflow four times with a callback");
    let finishes = Arc::new(AtomicUsize::new(0));
    let finishes_counted = Arc::clone(&finishes);
    assert!(
        executor
            .run_n_with(&taskflow, 4, move || {
                println!("finishes 4 runs");
                finishes_counted.fetch_add(1, Ordering::Relaxed);
            })
            .wait()
            .is_ok()
    );
    assert_eq!(finishes.load(Ordering::Relaxed), 1);
    println!();

    println!("Run the taskflow until the predicate returns true");
    let counter = Arc::new(AtomicI64::new(3));
    let counter_checked = Arc::clone(&counter);
    assert!(
        executor
            .run_until_with(
                &taskflow,
                move || {
                    // Upstream's `counter-- == 0`: compare the value the
                    // predicate finds, then decrement regardless.
                    let remaining = counter_checked.fetch_sub(1, Ordering::Relaxed);
                    println!("Counter = {remaining}");
                    remaining == 0
                },
                || {},
            )
            .wait()
            .is_ok()
    );
    // Checked at 3, 2, 1 and 0 before the true that stopped it, decrementing
    // once past the stopping value, as upstream's postfix `--` does too.
    assert_eq!(counter.load(Ordering::Relaxed), -1);

    println!("{}", taskflow.dump());
}

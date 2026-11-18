//! Translated from `examples/multi_condition.cpp` (Taskflow v4.1.0).
//!
//! How a multi-condition task jumps to several successors at once:
//!
//! ```text
//! A ----> B
//!   |
//!   |---> C
//!   |
//!   |---> D
//! ```
//!
//! `A` returns the indices `{0, 2}`, which select `B` and `D` -- the first
//! and third of its three successors -- and leave `C`, index 1, unselected.
//!
//! Upstream's program only *prints* as it runs. This milestone's examples
//! print *and* **assert**: every line upstream shows is also checked with
//! `assert!` or `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rustflow::Executor;
use rustflow::prelude::*;

fn main() {
    let executor = Executor::default();
    let mut taskflow = Taskflow::with_name("Multi-Conditional Tasking Demo");

    let ran_b = Arc::new(AtomicBool::new(false));
    let ran_c = Arc::new(AtomicBool::new(false));
    let ran_d = Arc::new(AtomicBool::new(false));

    // Upstream returns a `tf::SmallVector<int>`; the port's multi-conditioner
    // returns a plain `Vec<i32>` instead -- see
    // `FlowBuilder::emplace_multi_condition`.
    let a = taskflow.emplace_multi_condition(|| {
        println!("A");
        vec![0, 2]
    });
    let b = {
        let ran_b = Arc::clone(&ran_b);
        taskflow.emplace(move || {
            println!("B");
            ran_b.store(true, Ordering::Relaxed);
        })
    };
    let c = {
        let ran_c = Arc::clone(&ran_c);
        taskflow.emplace(move || {
            println!("C");
            ran_c.store(true, Ordering::Relaxed);
        })
    };
    let d = {
        let ran_d = Arc::clone(&ran_d);
        taskflow.emplace(move || {
            println!("D");
            ran_d.store(true, Ordering::Relaxed);
        })
    };

    taskflow.task(a).precede([b, c, d]);

    let dot = taskflow.dump();
    println!("{dot}");
    assert_eq!(taskflow.num_tasks(), 4);

    assert!(executor.run(&taskflow).wait().is_ok());

    // The path taken: indices 0 and 2 of A's three successors are B and D;
    // index 1, C, is the one `{0, 2}` leaves unselected.
    assert!(ran_b.load(Ordering::Relaxed), "index 0 selects B");
    assert!(
        !ran_c.load(Ordering::Relaxed),
        "index 1 is not in {{0, 2}}, so C never runs"
    );
    assert!(ran_d.load(Ordering::Relaxed), "index 2 selects D");
}

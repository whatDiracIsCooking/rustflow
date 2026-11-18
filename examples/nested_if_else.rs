//! Translated from `examples/nested_if_else.cpp` (Taskflow v4.1.0).
//!
//! Nested if-else control flow built from three condition tasks in series:
//! `cond1` checks `i > 1`, `cond2` checks `i > 2` and `cond3` checks `i > 3`,
//! each selecting its first successor (the "equal" branch) on `false` and
//! its second (the next `cond`, or "greater than 3") on `true`.
//!
//! Upstream's program only *prints* as it runs. This milestone's examples
//! print *and* **assert**: every line upstream shows is also checked with
//! `assert!` or `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use rustflow::Executor;
use rustflow::prelude::*;

fn main() {
    let executor = Executor::default();
    let mut taskflow = Taskflow::new();

    // Upstream's `int i;` captured by reference from four closures; shared
    // through an `Arc` instead, as `FnMut() + Send + 'static` requires.
    let i = Arc::new(AtomicI64::new(0));

    let initi = {
        let i = Arc::clone(&i);
        taskflow.emplace(move || i.store(3, Ordering::Relaxed))
    };
    let cond1 = {
        let i = Arc::clone(&i);
        taskflow.emplace_condition(move || i32::from(i.load(Ordering::Relaxed) > 1))
    };
    let cond2 = {
        let i = Arc::clone(&i);
        taskflow.emplace_condition(move || i32::from(i.load(Ordering::Relaxed) > 2))
    };
    let cond3 = {
        let i = Arc::clone(&i);
        taskflow.emplace_condition(move || i32::from(i.load(Ordering::Relaxed) > 3))
    };

    let ran_equl1 = Arc::new(AtomicBool::new(false));
    let ran_equl2 = Arc::new(AtomicBool::new(false));
    let ran_equl3 = Arc::new(AtomicBool::new(false));
    let ran_grtr3 = Arc::new(AtomicBool::new(false));

    let equl1 = {
        let ran = Arc::clone(&ran_equl1);
        taskflow.emplace(move || {
            println!("i=1");
            ran.store(true, Ordering::Relaxed);
        })
    };
    let equl2 = {
        let ran = Arc::clone(&ran_equl2);
        taskflow.emplace(move || {
            println!("i=2");
            ran.store(true, Ordering::Relaxed);
        })
    };
    let equl3 = {
        let ran = Arc::clone(&ran_equl3);
        taskflow.emplace(move || {
            println!("i=3");
            ran.store(true, Ordering::Relaxed);
        })
    };
    let grtr3 = {
        let ran = Arc::clone(&ran_grtr3);
        taskflow.emplace(move || {
            println!("i>3");
            ran.store(true, Ordering::Relaxed);
        })
    };

    taskflow.task(initi).precede([cond1]);
    taskflow.task(cond1).precede([equl1, cond2]);
    taskflow.task(cond2).precede([equl2, cond3]);
    taskflow.task(cond3).precede([equl3, grtr3]);

    let dot = taskflow.dump();
    println!("{dot}");
    assert_eq!(taskflow.num_tasks(), 8);

    assert!(executor.run(&taskflow).wait().is_ok());

    // The path taken: `i = 3`, so `cond1` (3 > 1) selects `cond2`, `cond2`
    // (3 > 2) selects `cond3`, and `cond3` (3 > 3 is false) selects `equl3` --
    // the one branch of the four that runs.
    assert_eq!(i.load(Ordering::Relaxed), 3);
    assert!(!ran_equl1.load(Ordering::Relaxed));
    assert!(!ran_equl2.load(Ordering::Relaxed));
    assert!(ran_equl3.load(Ordering::Relaxed));
    assert!(!ran_grtr3.load(Ordering::Relaxed));
}

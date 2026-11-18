//! Translated from `examples/do_while_loop.cpp` (Taskflow v4.1.0).
//!
//! Do-while control flow: `body` always runs once before `cond` checks
//! `i < 5` and selects `body` again while true, `done` once false -- the one
//! difference from `while_loop` being that the check comes after the first
//! iteration rather than before it.
//!
//! Upstream's program only *prints* as it runs, and its own `dump` call is
//! commented out, so there is no graph dump to translate here either.
//! This milestone's examples print *and* **assert**: every line upstream
//! shows is also checked with `assert!` or `assert_eq!`, so a non-zero exit
//! from the binary is itself the test. See the README's "Examples" section. A
//! `do_while_loop` is a cyclic graph, so `body_runs` pins the iteration count
//! explicitly rather than trusting `i`'s final value alone to say the loop
//! ran the right number of times.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::prelude::*;

fn main() {
    let executor = Executor::default();
    let mut taskflow = Taskflow::new();

    let i = Arc::new(AtomicI64::new(0));
    let body_runs = Arc::new(AtomicUsize::new(0));

    let init = {
        let i = Arc::clone(&i);
        taskflow.emplace(move || {
            println!("i=0");
            i.store(0, Ordering::Relaxed);
        })
    };
    let body = {
        let i = Arc::clone(&i);
        let body_runs = Arc::clone(&body_runs);
        taskflow.emplace(move || {
            print!("i++ => i=");
            i.fetch_add(1, Ordering::Relaxed);
            body_runs.fetch_add(1, Ordering::Relaxed);
        })
    };
    let cond = {
        let i = Arc::clone(&i);
        taskflow.emplace_condition(move || {
            let value = i.load(Ordering::Relaxed);
            println!("{value}");
            i32::from(value >= 5)
        })
    };
    let done = taskflow.emplace(|| println!("done"));

    taskflow.task(init).name("init");
    taskflow.task(body).name("do i++");
    taskflow.task(cond).name("while i<5");
    taskflow.task(done).name("done");

    taskflow.task(init).precede([body]);
    taskflow.task(body).precede([cond]);
    // `cond`'s successors in this order: index 0 (while) is `body`, index 1
    // (break) is `done`.
    taskflow.task(cond).precede([body, done]);

    assert!(executor.run(&taskflow).wait().is_ok());

    assert_eq!(
        body_runs.load(Ordering::Relaxed),
        5,
        "the loop body runs exactly five times"
    );
    assert_eq!(i.load(Ordering::Relaxed), 5, "upstream's final `i`");
}

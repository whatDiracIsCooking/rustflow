//! Translated from `examples/while_loop.cpp` (Taskflow v4.1.0).
//!
//! While-loop control flow: `cond` checks `i < 5` and selects `body` while
//! true, `done` once false; `body` increments `i` and `back` closes the
//! cycle back to `cond`.
//!
//! Upstream's program only *prints* as it runs. This milestone's examples
//! print *and* **assert**: every line upstream shows is also checked with
//! `assert!` or `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section. A `while_loop` is a cyclic
//! graph, so `body_runs` pins the iteration count explicitly rather than
//! trusting `i`'s final value alone to say the loop ran the right number of
//! times.

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
    let cond = {
        let i = Arc::clone(&i);
        taskflow.emplace_condition(move || {
            println!("while i<5");
            i32::from(i.load(Ordering::Relaxed) >= 5)
        })
    };
    let body = {
        let i = Arc::clone(&i);
        let body_runs = Arc::clone(&body_runs);
        taskflow.emplace(move || {
            let before = i.fetch_add(1, Ordering::Relaxed);
            println!("i++={before}");
            body_runs.fetch_add(1, Ordering::Relaxed);
        })
    };
    // Upstream's unconditional `return 0`: a condition task with one
    // successor, `cond`, that it always selects.
    let back = taskflow.emplace_condition(|| {
        println!("back");
        0
    });
    let done = taskflow.emplace(|| println!("done"));

    taskflow.task(init).name("init");
    taskflow.task(cond).name("while i<5");
    taskflow.task(body).name("i++");
    taskflow.task(back).name("back");
    taskflow.task(done).name("done");

    taskflow.task(init).precede([cond]);
    // `cond`'s successors in this order: index 0 (while) is `body`, index 1
    // (break) is `done`.
    taskflow.task(cond).precede([body, done]);
    taskflow.task(body).precede([back]);
    taskflow.task(back).precede([cond]);

    let dot = taskflow.dump();
    println!("{dot}");
    for label in ["init", "while i<5", "i++", "back", "done"] {
        assert!(dot.contains(&format!("label=\"{label}\"")));
    }

    assert!(executor.run(&taskflow).wait().is_ok());

    assert_eq!(
        body_runs.load(Ordering::Relaxed),
        5,
        "the loop body runs exactly five times"
    );
    assert_eq!(i.load(Ordering::Relaxed), 5, "upstream's final `i`");
}

//! Translated from `examples/if_else.cpp` (Taskflow v4.1.0).
//!
//! If-else control flow built from one condition task: `cond` returns `0` to
//! select `yes`, the first of its two successors, and `1` to select `no`,
//! the second.
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
    let mut taskflow = Taskflow::new();

    // Upstream's `auto [init, cond, yes, no] = taskflow.emplace(...)`: the
    // four callables there are a static task, a condition task and two more
    // statics, picked apart by upstream's overloaded `emplace`.
    // `FlowBuilder::emplace_n` only emplaces statics (every callable in its
    // `Callables` tuple is `FnMut()`), so a mixed tuple like this one needs
    // its own call per task instead of the single destructuring upstream
    // writes.
    let init = taskflow.emplace(|| {});
    let cond = taskflow.emplace_condition(|| 0);

    let ran_yes = Arc::new(AtomicBool::new(false));
    let ran_no = Arc::new(AtomicBool::new(false));
    let yes = {
        let ran_yes = Arc::clone(&ran_yes);
        taskflow.emplace(move || {
            println!("yes");
            ran_yes.store(true, Ordering::Relaxed);
        })
    };
    let no = {
        let ran_no = Arc::clone(&ran_no);
        taskflow.emplace(move || {
            println!("no");
            ran_no.store(true, Ordering::Relaxed);
        })
    };

    taskflow.task(init).name("init");
    taskflow.task(cond).name("cond");
    taskflow.task(yes).name("yes");
    taskflow.task(no).name("no");

    taskflow.task(cond).succeed([init]);

    // With this order, when `cond` returns 0, execution moves on to `yes`.
    // When `cond` returns 1, execution moves on to `no`.
    taskflow.task(cond).precede([yes, no]);

    let dot = taskflow.dump();
    println!("{dot}");
    for label in ["init", "cond", "yes", "no"] {
        assert!(dot.contains(&format!("label=\"{label}\"")));
    }

    assert!(executor.run(&taskflow).wait().is_ok());

    // The path taken: `cond` always returns 0 here, so `yes` runs and `no`
    // does not.
    assert!(ran_yes.load(Ordering::Relaxed), "cond selected yes");
    assert!(
        !ran_no.load(Ordering::Relaxed),
        "cond never selects no here"
    );
}

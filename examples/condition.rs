//! Translated from `examples/condition.cpp` (Taskflow v4.1.0).
//!
//! The one-node loop a condition task drives:
//!
//! ```text
//!       A
//!       |
//!       v
//!       B<---|
//!       |    |
//!       v    |
//!       C----|
//!       |
//!       v
//!       D
//! ```
//!
//! `A` initializes a counter to zero, `B` increments it, `C` is a condition
//! task that loops back to `B` until the counter reaches five and then
//! selects `D`.
//!
//! Upstream's program only *prints* as it runs. This milestone's examples
//! print *and* **assert**: every line upstream shows is also checked with
//! `assert!` or `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::prelude::*;

fn main() {
    let executor = Executor::default();
    let mut taskflow = Taskflow::with_name("Conditional Tasking Demo");

    // Upstream's `int counter` captured by reference from three task bodies;
    // a body here is `FnMut() + Send + 'static`, so the shared state lives in
    // an `Arc` instead. `loops` is this port's own addition: it counts `B`'s
    // invocations directly, so the assertion below pins the iteration count
    // rather than relying on the counter's final value alone.
    let counter = Arc::new(AtomicI64::new(0));
    let loops = Arc::new(AtomicUsize::new(0));

    let a = {
        let counter = Arc::clone(&counter);
        taskflow.emplace(move || {
            println!("initializes the counter to zero");
            counter.store(0, Ordering::Relaxed);
        })
    };
    let b = {
        let counter = Arc::clone(&counter);
        let loops = Arc::clone(&loops);
        taskflow.emplace(move || {
            println!("loops to increment the counter");
            counter.fetch_add(1, Ordering::Relaxed);
            loops.fetch_add(1, Ordering::Relaxed);
        })
    };
    let c = {
        let counter = Arc::clone(&counter);
        taskflow.emplace_condition(move || {
            let value = counter.load(Ordering::Relaxed);
            print!("counter is {value} -> ");
            if value != 5 {
                println!("loops again (goes to B)");
                0
            } else {
                println!("breaks the loop (goes to D)");
                1
            }
        })
    };
    let d = {
        let counter = Arc::clone(&counter);
        taskflow.emplace(move || {
            println!(
                "done with counter equal to {}",
                counter.load(Ordering::Relaxed)
            );
        })
    };

    taskflow.task(a).name("A").precede([b]);
    taskflow.task(b).name("B").precede([c]);
    // `C`'s successors in this order: index 0 (loop) is `B`, index 1 (break)
    // is `D`.
    taskflow.task(c).name("C").precede([b, d]);
    taskflow.task(d).name("D");

    let dot = taskflow.dump();
    println!("{dot}");
    for label in ["A", "B", "C", "D"] {
        assert!(dot.contains(&format!("label=\"{label}\"")));
    }

    assert!(executor.run(&taskflow).wait().is_ok());

    // The path taken: `C` breaks the cycle only once the counter reaches
    // five, so `B` -- and the cycle through `C` -- runs exactly five times.
    assert_eq!(
        loops.load(Ordering::Relaxed),
        5,
        "B runs exactly five times"
    );
    assert_eq!(
        counter.load(Ordering::Relaxed),
        5,
        "upstream's `assert(counter == 5)`"
    );
}

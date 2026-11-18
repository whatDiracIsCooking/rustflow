//! Translated from `examples/runtime_async.cpp` (Taskflow v4.1.0).
//!
//! A single runtime task spawns ten asynchronous tasks of its own with
//! `silent_async` -- cheaper than `async` here because nothing needs their
//! individual results -- then `corun`s to wait for all ten before reading
//! the counter they incremented.
//!
//! Upstream's program only *prints* as it goes. This milestone's examples
//! print *and* **assert**: every line upstream shows is also checked with
//! `assert!` or `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::prelude::*;

fn main() {
    let mut taskflow = Taskflow::with_name("Subflow Async");
    let executor = Executor::default();

    let counter = Arc::new(AtomicUsize::new(0));

    taskflow.emplace_runtime({
        let counter = Arc::clone(&counter);
        move |rt: &mut Runtime<'_>| {
            for _ in 0..10 {
                let counter = Arc::clone(&counter);
                rt.silent_async(move || {
                    println!("async task from the subflow");
                    counter.fetch_add(1, Ordering::Relaxed);
                });
            }
            rt.corun();
            println!("{} = 10", counter.load(Ordering::Relaxed));
        }
    });

    assert!(executor.run(&taskflow).wait().is_ok());
    assert_eq!(counter.load(Ordering::Relaxed), 10);
}

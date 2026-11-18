//! Translated from `examples/async.cpp` (Taskflow v4.1.0).
//!
//! Demonstrates creating asynchronous tasks straight from the executor --
//! with and without a handle on the result, with and without a [`Runtime`]
//! of their own -- and from a runtime task's own body, where every task it
//! spawns is guaranteed to finish before that body's implicit join.
//!
//! Upstream's program only *prints* as it goes. This milestone's examples
//! print *and* **assert**: every line upstream shows is also checked with
//! `assert!` or `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section.
//!
//! Named `asyncs.rs` rather than `async.rs`: `async` is a Rust keyword and
//! cannot name a binary target, the same reason `tests/asyncs.rs` has its
//! name.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::prelude::*;
use rustflow::{Executor, Runtime};

fn main() {
    let executor = Executor::default();

    // create asynchronous tasks from the executor (using the executor as a
    // thread pool).
    //
    // Upstream names this task `"async1"`. The name goes on here through the
    // `_named` twin of any submitting form -- `spawn_named` for this one --
    // and an observer is what reads it back; this example registers none, so
    // it takes the unnamed twin.
    let fu1 = executor.spawn(|| {
        println!("async task returns 1");
        1
    });

    // a silent async task hands back no handle at all
    executor.silent_async(|| {
        println!("silent async does not return");
    });

    // create async tasks with a runtime
    let fu2 = executor.spawn_runtime(|runtime: &mut Runtime<'_>| {
        println!(
            "async task with a runtime: {} workers",
            runtime.num_workers()
        );
    });

    executor.silent_async_runtime(|runtime: &mut Runtime<'_>| {
        println!(
            "silent async task with a runtime: {} workers",
            runtime.num_workers()
        );
    });

    assert_eq!(fu1.wait().expect("the body did not panic"), 1);
    fu2.wait().expect("the body did not panic");

    executor.wait_for_all(); // wait for the two silent async tasks to finish

    // create asynchronous tasks from a runtime task of a taskflow;
    // all asynchronous tasks are guaranteed to finish when the runtime task
    // joins
    let mut taskflow = Taskflow::new();

    let counter = Arc::new(AtomicUsize::new(0));
    let spawned = Arc::clone(&counter);

    taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
        for _ in 0..100 {
            let counter = Arc::clone(&spawned);
            runtime.silent_async(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            });
        }
        runtime.corun();

        // when the runtime task joins, all spawned tasks have finished
        if spawned.load(Ordering::Relaxed) == 100 {
            println!("async tasks spawned from the runtime all finish");
        } else {
            // upstream `throw`s `std::runtime_error`; a panicking task body
            // is this port's exception, caught by the executor and reported
            // from `wait()` below.
            panic!("this should not happen");
        }
    });

    executor.run(&taskflow).wait().expect("no task panicked");

    assert_eq!(counter.load(Ordering::Relaxed), 100);
}

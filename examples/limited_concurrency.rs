//! Translated from `examples/limited_concurrency.cpp` (Taskflow v4.1.0).
//!
//! A semaphore constrains five independent tasks to run one at a time.
//! Upstream's own `sl()` sleeps a second per task so a human watching stdout
//! can see the serialization by eye; the port instead tracks how many of the
//! five tasks are inside the critical section at once and asserts the
//! observed maximum never exceeded the semaphore's count, which is the
//! deterministic version of the same claim and the one #197 asks for.
//!
//! Upstream's program only *prints* as it goes. This milestone's examples
//! print *and* **assert**: every line upstream shows is also checked with
//! `assert!` or `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::prelude::*;
use rustflow::{Executor, Semaphore};

fn main() {
    let executor = Executor::new(4);
    let mut taskflow = Taskflow::new();

    // define a critical region of 1 worker
    let semaphore = Arc::new(Semaphore::new(1));

    // how many of the five tasks are inside the critical section right now,
    // and the largest that count was ever observed to be -- upstream shows
    // this only by eye, in the order "A".."E" happen to print in.
    let held = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    // create five tasks in the taskflow
    let names = ["A", "B", "C", "D", "E"];
    let tasks: Vec<Task> = names
        .iter()
        .map(|&name| {
            let held = Arc::clone(&held);
            let peak = Arc::clone(&peak);
            taskflow.emplace(move || {
                let concurrent = held.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(concurrent, Ordering::SeqCst);
                println!("{name}");
                held.fetch_sub(1, Ordering::SeqCst);
            })
        })
        .collect();

    for task in &tasks {
        taskflow.task(*task).acquire(&semaphore).release(&semaphore);
    }

    // upstream discards `run`'s future and calls `wait_for_all` instead,
    // because a dropped `std::future<void>` does not block; this port's
    // `RunHandle` does block on drop (see the crate docs), so `wait()` here
    // is both the submission and the wait upstream's two calls together are.
    executor.run(&taskflow).wait().expect("no task panicked");

    // the concurrency bound was never exceeded, and -- since every task
    // acquires the same semaphore -- it was reached exactly once: one task
    // at a time, the way upstream's comment describes the region.
    assert!(peak.load(Ordering::SeqCst) <= semaphore.max_value());
    assert_eq!(peak.load(Ordering::SeqCst), semaphore.max_value());
}

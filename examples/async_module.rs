//! Translated from `examples/async_module.cpp` (Taskflow v4.1.0).
//!
//! Demonstrates launching whole taskflows as asynchronous tasks through
//! [`make_module_task`], first as four independent `spawn`s and then as a
//! chain of four dependent-async tasks -- each taskflow run twice over, once
//! per launch, which [`make_module_task`]'s own docs say a module task's
//! `FnMut` body allows.
//!
//! Upstream's program only *prints* as it goes. This milestone's examples
//! print *and* **assert**: every line upstream shows is also checked with
//! `assert!` or `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::algorithm::module::make_module_task;
use rustflow::prelude::*;

/// A one-task taskflow that bumps `counter` each time it runs, standing for
/// upstream's `A`, `B`, `C` and `D`.
fn taskflow_counting(counter: &Arc<AtomicUsize>, name: &'static str) -> Arc<Taskflow> {
    let mut taskflow = Taskflow::new();
    let counter = Arc::clone(counter);
    taskflow.emplace(move || {
        counter.fetch_add(1, Ordering::Relaxed);
        println!("Taskflow {name}");
    });
    Arc::new(taskflow)
}

fn main() {
    let executor = Executor::default();

    let counter_a = Arc::new(AtomicUsize::new(0));
    let counter_b = Arc::new(AtomicUsize::new(0));
    let counter_c = Arc::new(AtomicUsize::new(0));
    let counter_d = Arc::new(AtomicUsize::new(0));

    let a = taskflow_counting(&counter_a, "A");
    let b = taskflow_counting(&counter_b, "B");
    let c = taskflow_counting(&counter_c, "C");
    let d = taskflow_counting(&counter_d, "D");

    // launch the four taskflows using async
    println!("launching four taskflows using async ...");
    executor.silent_async_runtime(make_module_task(Arc::clone(&a)));
    executor.silent_async_runtime(make_module_task(Arc::clone(&b)));
    executor.silent_async_runtime(make_module_task(Arc::clone(&c)));
    executor.silent_async_runtime(make_module_task(Arc::clone(&d)));
    executor.wait_for_all();

    assert_eq!(counter_a.load(Ordering::Relaxed), 1);
    assert_eq!(counter_b.load(Ordering::Relaxed), 1);
    assert_eq!(counter_c.load(Ordering::Relaxed), 1);
    assert_eq!(counter_d.load(Ordering::Relaxed), 1);

    // launch the same four taskflows with dependencies between them
    println!("launching four taskflows using dependent async ...");
    let ta = executor.silent_dependent_async_runtime(make_module_task(a), []);
    let tb = executor.silent_dependent_async_runtime(make_module_task(b), [&ta]);
    let tc = executor.silent_dependent_async_runtime(make_module_task(c), [&tb]);
    let (_td, fd) = executor.dependent_async_runtime(make_module_task(d), [&tc]);
    fd.wait().expect("no task in the chain panicked");

    // every taskflow ran once more, as the second half of the module body's
    // `FnMut` contract promises
    assert_eq!(counter_a.load(Ordering::Relaxed), 2);
    assert_eq!(counter_b.load(Ordering::Relaxed), 2);
    assert_eq!(counter_c.load(Ordering::Relaxed), 2);
    assert_eq!(counter_d.load(Ordering::Relaxed), 2);
}

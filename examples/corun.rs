//! Translated from `examples/corun.cpp` (Taskflow v4.1.0).
//!
//! `N` inner taskflows of `T` tasks each are each run from a task of the
//! outer taskflow, on an executor with only two workers. Running an inner
//! taskflow with [`Executor::run`] from inside a task of the outer one can
//! deadlock: every worker may end up blocked waiting on a run that needs a
//! worker to make progress, and there are none left. [`Executor::corun`]
//! avoids that by keeping the calling worker in the work-stealing loop
//! while it waits, so it can still run other work -- including the inner
//! taskflow's own tasks.
//!
//! Upstream's program prints nothing and only exercises the pattern; this
//! milestone's examples **assert** a result in every case, so this one
//! checks that every task of every inner taskflow ran. See the README's
//! "Examples" section.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::prelude::*;

fn main() {
    const N: usize = 100;
    const T: usize = 1000;

    // Upstream's `tf::Executor executor(2)`.
    let executor = Arc::new(Executor::new(2));
    let mut taskflow = Taskflow::new();

    let counter = Arc::new(AtomicUsize::new(0));

    // Upstream's `std::array<tf::Taskflow, N> taskflows`, each held behind
    // an `Arc` here rather than referenced from the outer task's closure:
    // every one of those closures is `'static`.
    let inner: Vec<Arc<Taskflow>> = (0..N)
        .map(|_| {
            let mut taskflow = Taskflow::new();
            for _ in 0..T {
                let counter = Arc::clone(&counter);
                taskflow.emplace(move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                });
            }
            Arc::new(taskflow)
        })
        .collect();

    for taskflow_n in &inner {
        let executor = Arc::clone(&executor);
        let taskflow_n = Arc::clone(taskflow_n);
        taskflow.emplace(move || {
            executor.corun(&taskflow_n).expect("a worker may corun");
            // executor.run(&taskflow_n).wait() <-- can result in deadlock
        });
    }

    assert!(executor.run(&taskflow).wait().is_ok());
    assert_eq!(counter.load(Ordering::Relaxed), N * T);
}

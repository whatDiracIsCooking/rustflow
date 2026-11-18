//! Translated from `examples/worker_interface.cpp` (Taskflow v4.1.0).
//!
//! Upstream's program implements a `tf::WorkerInterface` whose
//! `scheduler_prologue` affines the worker's OS thread to a CPU core equal
//! to its id, via `w.thread()`, and whose `scheduler_epilogue` prints that
//! the worker left the loop. This port's `Worker` has no thread handle --
//! `src/executor/worker.rs`'s module docs say why, splitting a worker into
//! `Worker` and a thread-only `WorkerThread` the hook cannot reach -- so
//! this example keeps only the half upstream's `make_worker_interface` is
//! standing in for here: the `WorkerInterface` trait, with
//! `scheduler_prologue`/`scheduler_epilogue` counting their own calls per
//! worker instead of setting affinity.
//!
//! Upstream's program only *prints* and returns. This milestone's examples
//! print *and* **assert**: every hook upstream shows firing is checked here
//! to have fired *exactly once per worker*, so a non-zero exit from the
//! binary is itself the test. See the README's "Examples" section.

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::{Executor, Worker, WorkerInterface};

/// Counts how many times each worker's prologue and epilogue hooks ran.
struct CountingInterface {
    /// One counter per worker, indexed by [`Worker::id`].
    prologue: Vec<AtomicUsize>,
    /// One counter per worker, indexed by [`Worker::id`].
    epilogue: Vec<AtomicUsize>,
}

impl CountingInterface {
    fn new(workers: usize) -> Self {
        Self {
            prologue: (0..workers).map(|_| AtomicUsize::new(0)).collect(),
            epilogue: (0..workers).map(|_| AtomicUsize::new(0)).collect(),
        }
    }
}

impl WorkerInterface for CountingInterface {
    fn scheduler_prologue(&self, worker: &Worker) {
        println!(
            "worker {} prepares to enter the work-stealing loop",
            worker.id()
        );
        self.prologue[worker.id()].fetch_add(1, Ordering::Relaxed);
    }

    fn scheduler_epilogue(&self, worker: &Worker, _panicked: Option<Box<dyn Any + Send>>) {
        println!("worker {} left the work-stealing loop", worker.id());
        self.epilogue[worker.id()].fetch_add(1, Ordering::Relaxed);
    }
}

fn main() {
    const WORKERS: usize = 4;

    let interface = Arc::new(CountingInterface::new(WORKERS));

    // Upstream's `tf::Executor executor(4, tf::make_worker_interface<CustomWorkerBehavior>())`.
    // Scoped so the executor's `Drop` -- which joins every worker thread --
    // runs before the counters below are read: the epilogue fires on a
    // worker's way out, and nothing here submits work for it to run first.
    {
        let _executor = Executor::with_worker_interface(WORKERS, interface.clone());
    }

    for id in 0..WORKERS {
        assert_eq!(
            interface.prologue[id].load(Ordering::Relaxed),
            1,
            "worker {id}'s prologue ran more or less than once"
        );
        assert_eq!(
            interface.epilogue[id].load(Ordering::Relaxed),
            1,
            "worker {id}'s epilogue ran more or less than once"
        );
    }
}

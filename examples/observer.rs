//! Translated from `examples/observer.cpp` (Taskflow v4.1.0).
//!
//! Demonstrates [`Observer`], the hook an [`Executor`] calls around every
//! task a worker runs: a custom observer watches eight independent tasks
//! run and counts entries and exits as it goes.
//!
//! Upstream's program only *prints* as each hook fires. This milestone's
//! examples print *and* **assert**: every line upstream shows is also
//! checked, here that the entry and exit counts both equal the number of
//! tasks run, so a non-zero exit from the binary is itself the test. See
//! the README's "Examples" section.
//!
//! Upstream builds and registers its observer in one call,
//! `executor.make_observer<MyObserver>("MyObserver")`, a variadic template
//! that constructs `MyObserver` from the arguments given and registers the
//! `shared_ptr` it returns. Rust generics are not variadic, so
//! [`Executor::make_observer`] takes an already-built value instead: the
//! constructor-like printing upstream's `MyObserver::MyObserver` does on
//! construction happens in [`MyObserver::new`] below, called before the
//! value is handed to `make_observer`. See the `observer` module's "Design"
//! section for why.

use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::observer::{Observer, TaskView, WorkerView};
use rustflow::prelude::*;

/// Upstream's `MyObserver`: prints around every task dispatch, and counts
/// entries and exits so this example can assert the two end up equal.
struct MyObserver {
    /// How many times [`on_entry`](Observer::on_entry) has fired.
    entries: AtomicUsize,
    /// How many times [`on_exit`](Observer::on_exit) has fired.
    exits: AtomicUsize,
}

impl MyObserver {
    /// Upstream's constructor, which only prints `name`; this port's
    /// `make_observer` does not build the value itself (see this module's
    /// doc comment), so building happens here instead, before the result is
    /// handed to it.
    fn new(name: &str) -> Self {
        println!("constructing observer {name}");
        Self {
            entries: AtomicUsize::new(0),
            exits: AtomicUsize::new(0),
        }
    }
}

impl Observer for MyObserver {
    // set_up is a constructor-like method that will be called exactly once
    // passing the number of workers
    fn set_up(&self, num_workers: usize) {
        println!("setting up observer with {num_workers} workers");
    }

    // on_entry will be called before a worker runs a task
    fn on_entry(&self, worker: WorkerView<'_>, task: TaskView<'_>) {
        println!("worker {} ready to run {}", worker.id(), task.name());
        self.entries.fetch_add(1, Ordering::Relaxed);
    }

    // on_exit will be called after a worker completes a task
    fn on_exit(&self, worker: WorkerView<'_>, task: TaskView<'_>) {
        println!("worker {} finished running {}", worker.id(), task.name());
        self.exits.fetch_add(1, Ordering::Relaxed);
    }
}

fn main() {
    let executor = Executor::default();

    // Create a taskflow of eight tasks
    let mut taskflow = Taskflow::new();

    let a = taskflow.emplace(|| println!("1"));
    let b = taskflow.emplace(|| println!("2"));
    let c = taskflow.emplace(|| println!("3"));
    let d = taskflow.emplace(|| println!("4"));
    let e = taskflow.emplace(|| println!("5"));
    let f = taskflow.emplace(|| println!("6"));
    let g = taskflow.emplace(|| println!("7"));
    let h = taskflow.emplace(|| println!("8"));

    taskflow.task(a).name("A");
    taskflow.task(b).name("B");
    taskflow.task(c).name("C");
    taskflow.task(d).name("D");
    taskflow.task(e).name("E");
    taskflow.task(f).name("F");
    taskflow.task(g).name("G");
    taskflow.task(h).name("H");

    // create a default observer: built here, then registered -- see this
    // module's doc comment for why that is two calls rather than upstream's
    // one `make_observer<MyObserver>("MyObserver")`.
    let observer = executor.make_observer(MyObserver::new("MyObserver"));

    // run the taskflow
    assert!(executor.run(&taskflow).wait().is_ok());

    // What upstream only shows a human by eye, in the trace printed above,
    // the port checks outright: eight independent tasks dispatched means
    // eight entries and eight exits.
    assert_eq!(observer.entries.load(Ordering::Relaxed), 8);
    assert_eq!(observer.exits.load(Ordering::Relaxed), 8);

    // remove the observer (optional)
    executor.remove_observer(&observer);
    assert_eq!(executor.num_observers(), 0);
}

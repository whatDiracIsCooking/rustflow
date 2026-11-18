//! Translated from `examples/subflow_exception.cpp` (Taskflow v4.1.0).
//!
//! Demonstrates an exception raised inside a subflow reaching the run that
//! spawned it.
//!
//! Upstream's `sf.join()` rethrows the subgraph's exception on the worker
//! running the body, which propagates out of the enclosing
//! `executor.run(taskflow).get()` for a `try`/`catch` to see. This port's
//! [`Subflow::join`](rustflow::Subflow::join) does the same -- it panics
//! with the subgraph's own payload via `std::panic::resume_unwind` -- so the
//! panic crosses from the subflow body to the top-level run exactly the way
//! a task's own panic would, and [`RunHandle::wait`](rustflow::RunHandle::wait)
//! reports it as [`RunError::Panicked`](rustflow::RunError::Panicked).
//!
//! Upstream prints the caught exception's message; this port asserts it, so
//! a non-zero exit is the test.

use rustflow::prelude::*;
use rustflow::{Executor, RunError};

fn main() {
    let executor = Executor::default();
    let mut taskflow = Taskflow::new();

    taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
        let a = subflow.emplace(|| {
            println!("Task A");
            panic!("exception on A");
        });
        let b = subflow.emplace(|| {
            println!("Task B");
        });
        subflow.task(a).precede([b]);
        // `join()` panics with the subgraph's payload rather than returning
        // an `Err`, upstream's `sf.join()` throwing the same way; the `Ok`
        // case is a fresh subflow's only other outcome, so there is nothing
        // to inspect here.
        subflow.join().expect("a fresh subflow is joinable");
    });

    match executor.run(&taskflow).wait() {
        Ok(()) => panic!("expected the subflow's exception to reach the run"),
        Err(RunError::Panicked(payload)) => {
            let message = payload
                .downcast_ref::<&str>()
                .copied()
                .expect("a `panic!` with a string literal payload is a `&str`");
            println!("exception thrown from running the taskflow: {message}");
            assert_eq!(message, "exception on A");
        }
        // `RunError` is `#[non_exhaustive]`; nothing else exists yet for a
        // run of this taskflow to report.
        Err(other) => panic!("unexpected run error: {other}"),
    }
}

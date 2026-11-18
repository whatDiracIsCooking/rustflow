//! Translated from `examples/exception.cpp` (Taskflow v4.1.0).
//!
//! Demonstrates capturing a panic thrown from a task running inside a
//! taskflow.
//!
//! Upstream's `executor.run(taskflow).get()` rethrows whatever exception
//! reached it on the calling thread, caught with a `try`/`catch`. This
//! port's [`RunHandle::wait`](rustflow::RunHandle::wait) returns a
//! [`Result`] instead of unwinding, with the payload in
//! [`RunError::Panicked`](rustflow::RunError::Panicked) -- so the `catch`
//! becomes a `match` on `Err`.
//!
//! Upstream prints the caught exception's message for a human to read; this
//! port asserts it is one of the two tasks' own messages, so a non-zero exit
//! is the test.

use rustflow::prelude::*;
use rustflow::{Executor, RunError};

fn main() {
    let executor = Executor::default();
    let mut taskflow = Taskflow::with_name("exception");

    // Upstream destructures `taskflow.emplace(...)` into a tuple of four
    // `tf::Task`s with structured binding; `emplace_n` is this port's answer
    // -- see `FlowBuilder::emplace_n`'s docs for why the tuple is the
    // argument as well as the result.
    let (a, b, c, d) = taskflow.emplace_n((
        || println!("TaskA"),
        || {
            println!("TaskB");
            panic!("Exception on Task B");
        },
        || {
            println!("TaskC");
            panic!("Exception on Task C");
        },
        || println!("TaskD will not be printed due to exception"),
    ));

    taskflow.task(a).precede([b, c]); // A runs before B and C
    taskflow.task(d).succeed([b, c]); // D runs after B and C

    match executor.run(&taskflow).wait() {
        Ok(()) => panic!("expected a task to panic"),
        Err(RunError::Panicked(payload)) => {
            // Only the first panicking task is reported, and which of B or C
            // that is races the scheduler -- see `RunError::Panicked`'s own
            // docs -- so the port checks membership rather than one fixed
            // message the way upstream's single `catch` could not either.
            let message = payload
                .downcast_ref::<&str>()
                .copied()
                .expect("a `panic!` with a string literal payload is a `&str`");
            println!("{message}");
            assert!(["Exception on Task B", "Exception on Task C"].contains(&message));
        }
        // `RunError` is `#[non_exhaustive]` for the later milestones its own
        // docs mention; nothing else exists yet for a run of this taskflow
        // to report.
        Err(other) => panic!("unexpected run error: {other}"),
    }
}

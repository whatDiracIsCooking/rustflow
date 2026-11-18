//! Translated from `examples/cancel.cpp` (Taskflow v4.1.0).
//!
//! Demonstrates cancelling a submitted taskflow and waiting for the
//! cancellation to take effect.
//!
//! Upstream builds 1000 one-second tasks so that an uncancelled run would
//! take `1000 / P` seconds, and shows a cancelled one finishing in a small
//! fraction of that. A full-size version of that would make this example the
//! slowest thing the gate runs for an assertion that is only ever about
//! *proportion*, so the port shrinks both knobs -- fewer tasks, a
//! millisecond sleep instead of a second -- while keeping the same shape:
//! many more tasks than workers, each slow enough that a cancelled run is
//! unmistakably a small multiple of one task's sleep rather than the whole
//! queue's.
//!
//! Upstream prints the elapsed time for a human to judge "much less than
//! 1000 seconds" by eye; this port checks the same claim with `assert!`, so
//! a non-zero exit is the test.

use std::time::{Duration, Instant};

use rustflow::Executor;
use rustflow::prelude::*;

/// How long each task sleeps for.
const TASK_DURATION: Duration = Duration::from_millis(20);

/// How many tasks the taskflow queues: many more than [`WORKERS`], so that an
/// uncancelled run would take a visibly longer multiple of [`TASK_DURATION`].
const NUM_TASKS: usize = 200;

/// How many workers run the tasks.
const WORKERS: usize = 4;

fn main() {
    let executor = Executor::new(WORKERS);
    let mut taskflow = Taskflow::with_name("cancel");

    // A taskflow graph of `NUM_TASKS` tasks, each sleeping `TASK_DURATION`.
    // Ideally, the taskflow completes in `NUM_TASKS / WORKERS` sleeps.
    for _ in 0..NUM_TASKS {
        taskflow.emplace(|| std::thread::sleep(TASK_DURATION));
    }

    // Submit the taskflow.
    let start = Instant::now();
    let run = executor.run(&taskflow);

    // Submit a cancel request to cancel every task not already running.
    run.cancel();

    // Wait until the cancellation finishes.
    assert!(run.wait().is_ok());
    let elapsed = start.elapsed();

    // The duration should be much less than running every task to
    // completion: at most a handful of `TASK_DURATION`s, one per worker that
    // had already started a task when the cancellation landed, rather than
    // `NUM_TASKS / WORKERS` of them.
    println!("taskflow completes in {elapsed:?}");
    let uncancelled = TASK_DURATION * u32::try_from(NUM_TASKS / WORKERS).unwrap();
    assert!(
        elapsed < uncancelled / 2,
        "a cancelled run took {elapsed:?}, not much less than the \
         uncancelled {uncancelled:?} it is being compared against"
    );
}

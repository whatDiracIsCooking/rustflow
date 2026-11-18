//! Translated from `unittests/test_data_pipelines.cpp` (Taskflow v4.1.0).
//!
//! The three typed pipe bodies every `DataPipeline` family in that file
//! repeats: the serial pipe that stops the pipeline after `N` tokens, and the
//! two parallel pipes that collect what they are handed -- one passing a
//! value on, one ending the chain.
//!
//! A topic of its own for the reason [`super`]'s docs give: these are
//! `DataPipe`s, where `common/pipeline.rs`'s builders are the untyped
//! `Pipe`s of the sibling suites, and the two sets have no caller in common.
//! The `Ifelse` and `PipelineinPipeline` families' own pipes branch on a
//! source value or drive a sub-pipeline, so they stay where they are.
//!
//! # Design
//!
//! **Each builder takes its stopping point as a closure, not a number**, for
//! the reason `common/pipeline.rs` gives in full: a family that stops at a
//! fixed `N` passes `move || n`, and `Looping.DataPipelines`, whose condition
//! task grows the count round by round, passes a read of the `AtomicUsize` it
//! advances.
//!
//! **The value a pipe produces is `counter + 1`, not the counter.** That is
//! upstream's own `source[j1] + 1` over a `std::iota` source, and it is what
//! makes a zero in a later pipe's input tell a missed write apart from a
//! value that was never produced.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::pipeline::data_pipeline::{DataPipe, parallel_pipe_with_flow, serial_pipe_with_flow};

/// The `void -> i32` serial pipe every family starts with: upstream's
/// `tf::make_data_pipe<void, int>(tf::PipeType::SERIAL, [](tf::Pipeflow& pf)
/// { if(j1 == N) { pf.stop(); return 0; } ... })`.
///
/// Stops the pipeline once `counter` has reached `stop_at()`, and otherwise
/// checks the token reached the line its number says it should, counts
/// itself, and hands the next pipe `counter + 1`.
pub fn counting_source(
    num_lines: usize,
    stop_at: impl Fn() -> usize + Send + Sync + 'static,
    counter: &Arc<AtomicUsize>,
) -> DataPipe<(), i32> {
    let counter = Arc::clone(counter);
    serial_pipe_with_flow::<(), i32>(move |(), pf| {
        let current = counter.load(Ordering::Relaxed);
        if current == stop_at() {
            pf.stop();
            return 0;
        }
        assert_eq!(pf.token() % num_lines, pf.line());
        counter.fetch_add(1, Ordering::Relaxed);
        i32::try_from(current + 1).expect("a token count fits in an i32")
    })
}

/// The `i32 -> void` parallel pipe that ends a chain by collecting what it
/// was handed, with no order of its own to assert: upstream's last pipe of
/// `3P(SSP)` and of `3P(SPP)`.
///
/// What the collection then holds is a job for `common/tokens.rs`'s own
/// `assert_consecutive_from`, and the ticket check is upstream's
/// `REQUIRE(j3 < N)` -- a `fetch_add` here, since this pipe's own tokens run
/// at once.
pub fn collecting_pipe(
    num_lines: usize,
    limit: impl Fn() -> usize + Send + Sync + 'static,
    counter: &Arc<AtomicUsize>,
    collection: &Arc<Mutex<Vec<i32>>>,
) -> DataPipe<i32, ()> {
    let counter = Arc::clone(counter);
    let collection = Arc::clone(collection);
    parallel_pipe_with_flow::<i32, ()>(move |value, pf| {
        let ticket = counter.fetch_add(1, Ordering::Relaxed);
        assert!(ticket < limit());
        assert_eq!(pf.token() % num_lines, pf.line());
        collection.lock().expect("uncontended").push(value);
    })
}

/// [`collecting_pipe`], plus the one step on: it hands the next pipe what it
/// collected *plus one*, so that a serial pipe after it reads a value only
/// this one could have produced.
pub fn propagating_pipe(
    num_lines: usize,
    limit: impl Fn() -> usize + Send + Sync + 'static,
    counter: &Arc<AtomicUsize>,
    collection: &Arc<Mutex<Vec<i32>>>,
) -> DataPipe<i32, i32> {
    let counter = Arc::clone(counter);
    let collection = Arc::clone(collection);
    parallel_pipe_with_flow::<i32, i32>(move |value, pf| {
        let ticket = counter.fetch_add(1, Ordering::Relaxed);
        assert!(ticket < limit());
        assert_eq!(pf.token() % num_lines, pf.line());
        collection.lock().expect("uncontended").push(value);
        value + 1
    })
}

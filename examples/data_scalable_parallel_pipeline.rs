//! Translated from `examples/data_scalable_parallel_pipeline.cpp` (Taskflow
//! v4.1.0), with two deliberate departures from it below.
//!
//! Builds a [`DataPipeline`] over a three-stage typed chain
//! (`void -> i32 -> i64 -> i32`), runs it, then grows the chain to five
//! stages and runs again -- the typed analogue of `scalable_pipeline`'s
//! [`Pipeline::reset_pipes`].
//!
//! **Departure one, forced by the port's types rather than chosen.**
//! Upstream's own program is unfinished: its comments ask "is this ok" of
//! its own `std::vector<ScalableDataPipeBase*>`, and the five-pipe chain it
//! resets to appends two pipes of the *wrong* input type for the slots they
//! fill (`pipe_callable1`, typed `void -> int`, into two `int -> float` and
//! `float -> int` slots) -- a bug upstream's own template machinery does not
//! catch until the callable actually runs. [`DataPipelineBuilder::pipe`]
//! checks exactly this at compile time, so there is no faithful port of
//! upstream's five-pipe chain to write; this file's second chain is typed
//! consistently instead, which is the whole difference [`DataPipeline`]'s
//! own docs say this builder exists to make.
//!
//! **Departure two, the Friction this file reports.** [`Pipeline`] exposes
//! both [`Pipeline::reset_pipes`] and [`Pipeline::reset_lines`] -- the
//! "scalable" forms upstream's `ScalablePipeline::reset` splits into two
//! overloads -- but [`DataPipeline`] exposes neither, only
//! [`DataPipeline::reset`]. There is no method on this type that replaces
//! its chain or its line count in place, so "resetting" one here means
//! building a second [`DataPipeline`] from scratch, as this file does for
//! its five-stage run, rather than calling through to the [`Pipeline`]
//! underneath it the way [`DataPipeline::reset`] itself already does for
//! the one reset form it does have.
//!
//! Upstream's program only *prints* each stage's result; floats are
//! avoided throughout (`i64` in their place) so every narrowing conversion
//! stays a checked `try_from` rather than an `as` cast this crate's
//! `cast_possible_truncation` lint would flag. This milestone's examples
//! print *and* **assert**: every line upstream shows is also checked with
//! `assert!` or `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section.

use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::pipeline::data_pipeline::{DataPipeline, serial_pipe, serial_pipe_with_flow};
use rustflow::pipeline::pipe::Pipeflow;
use rustflow::prelude::*;

const NUM_LINES: usize = 4;

/// The first pipe of either chain: upstream's `pipe_callable1`, stopping
/// after 5 scheduling tokens and returning the token number otherwise.
fn source_pipe() -> impl Fn((), &mut Pipeflow) -> i32 {
    |(), pf| {
        if pf.token() == 5 {
            pf.stop();
            return 0;
        }
        println!("stage 1: input token = {}", pf.token());
        i32::try_from(pf.token()).expect("a token count fits in an i32")
    }
}

fn main() {
    let executor = Executor::default();

    // The chain's own final results, in strict token order -- every pipe
    // below is serial, so the sink's own calls arrive in that order.
    let results: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

    // First run: void -> i32 -> i64 -> i32, three compute pipes plus a sink
    // that records the value for this file's own assertion (upstream's
    // chain ends at the third pipe's own `int` output with nothing reading
    // it back out).
    let pipeline = {
        let results = Arc::clone(&results);
        DataPipeline::builder(NUM_LINES)
            .pipe(serial_pipe_with_flow::<(), i32>(source_pipe()))
            .pipe(serial_pipe::<i32, i64>(|value| i64::from(value) + 1))
            .pipe(serial_pipe::<i64, i32>(|value| {
                i32::try_from(value + 1).expect("a small chain stays in range")
            }))
            .pipe(serial_pipe::<i32, ()>(move |value| {
                results.lock().expect("uncontended").push(value);
            }))
            .build()
    };

    let mut taskflow = Taskflow::with_name("pipeline");
    let task = taskflow.composed_of(&pipeline);
    taskflow.task(task).name("pipeline");

    let dot = taskflow.dump();
    println!("{dot}");
    assert!(dot.contains("label=\"pipeline\""));

    assert!(executor.run(&taskflow).wait().is_ok());

    assert_eq!(pipeline.num_tokens(), 5);
    assert_eq!(
        results.lock().expect("uncontended").as_slice(),
        [2, 3, 4, 5, 6]
    );
    results.lock().expect("uncontended").clear();

    // Second run: a fresh `DataPipeline`, over the same number of lines but
    // a five-stage chain -- the typed stand-in for upstream's reset to a
    // longer pipe range, built from scratch for the reason this file's own
    // doc comment gives.
    let pipeline2 = {
        let results = Arc::clone(&results);
        DataPipeline::builder(NUM_LINES)
            .pipe(serial_pipe_with_flow::<(), i32>(source_pipe()))
            .pipe(serial_pipe::<i32, i64>(|value| i64::from(value) + 1))
            .pipe(serial_pipe::<i64, i32>(|value| {
                i32::try_from(value + 1).expect("a small chain stays in range")
            }))
            .pipe(serial_pipe::<i32, i64>(|value| i64::from(value) + 1))
            .pipe(serial_pipe::<i64, i32>(|value| {
                i32::try_from(value + 1).expect("a small chain stays in range")
            }))
            .pipe(serial_pipe::<i32, ()>(move |value| {
                results.lock().expect("uncontended").push(value);
            }))
            .build()
    };

    let mut taskflow2 = Taskflow::with_name("pipeline");
    let task2 = taskflow2.composed_of(&pipeline2);
    taskflow2.task(task2).name("pipeline");

    assert!(executor.run(&taskflow2).wait().is_ok());

    assert_eq!(pipeline2.num_lines(), NUM_LINES);
    assert_eq!(pipeline2.num_pipes(), 6);
    assert_eq!(pipeline2.num_tokens(), 5);
    assert_eq!(
        results.lock().expect("uncontended").as_slice(),
        [4, 5, 6, 7, 8]
    );
}

//! Translated from `examples/pipeline.cpp` (Taskflow v4.1.0).
//!
//! A pipeline of three pipes (serial-parallel-serial) over four parallel
//! lines, propagating a series of token numbers and adding one to the
//! result at each of the last two stages:
//!
//! ```text
//! o -> o -> o
//! |         |
//! v         v
//! o -> o -> o
//! |         |
//! v         v
//! o -> o -> o
//! |         |
//! v         v
//! o -> o -> o
//! ```
//!
//! Upstream's program only *prints* each stage's result. This milestone's
//! examples print *and* **assert**: every line upstream shows is also
//! checked with `assert!` or `assert_eq!`, so a non-zero exit from the
//! binary is itself the test. See the README's "Examples" section.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
use rustflow::pipeline::scheduler::Pipeline;
use rustflow::prelude::*;

fn main() {
    let mut taskflow = Taskflow::with_name("pipeline");
    let executor = Executor::default();

    const NUM_LINES: usize = 4;

    // Upstream's `std::array<size_t, num_lines> buffer`, captured by the
    // three pipes' closures below -- shared through an `Arc` rather than by
    // reference, since a [`Pipe`]'s callable is `Fn + Send + Sync + 'static`
    // and a `PipeType::Parallel` pipe runs it from more than one line's
    // worker at once.
    let buffer: Arc<Vec<AtomicUsize>> =
        Arc::new((0..NUM_LINES).map(|_| AtomicUsize::new(0)).collect());

    // The second (parallel) pipe's own inputs, collected to demonstrate the
    // one thing a parallel pipe does not promise: an order among the tokens
    // it runs. Sorted before comparison below, unlike the final stage's
    // results.
    let stage2_seen: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));

    // The third (serial) pipe's own outputs, in the order its tokens arrive
    // -- a serial pipe's own guarantee, so this is compared *without*
    // sorting.
    let results: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));

    let pipe0 = {
        let buffer = Arc::clone(&buffer);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            // generate only 5 scheduling tokens
            if pf.token() == 5 {
                pf.stop();
            } else {
                println!("stage 1: input token = {}", pf.token());
                buffer[pf.line()].store(pf.token(), Ordering::Relaxed);
            }
        })
    };

    let pipe1 = {
        let buffer = Arc::clone(&buffer);
        let stage2_seen = Arc::clone(&stage2_seen);
        Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
            let previous = buffer[pf.line()].load(Ordering::Relaxed);
            println!("stage 2: input buffer[{}] = {previous}", pf.line());
            stage2_seen.lock().expect("uncontended").push(previous);
            buffer[pf.line()].store(previous + 1, Ordering::Relaxed);
        })
    };

    let pipe2 = {
        let buffer = Arc::clone(&buffer);
        let results = Arc::clone(&results);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            let previous = buffer[pf.line()].load(Ordering::Relaxed);
            println!("stage 3: input buffer[{}] = {previous}", pf.line());
            let value = previous + 1;
            buffer[pf.line()].store(value, Ordering::Relaxed);
            results.lock().expect("uncontended").push(value);
        })
    };

    // the pipeline consists of three pipes (serial-parallel-serial) and up
    // to four concurrent scheduling tokens
    let pipeline = Pipeline::new(NUM_LINES, vec![pipe0, pipe1, pipe2]);

    // build the pipeline graph using composition
    let init = taskflow.emplace(|| println!("ready"));
    let task = taskflow.composed_of(&pipeline);
    let stop = taskflow.emplace(|| println!("stopped"));
    taskflow.task(init).name("starting pipeline");
    taskflow.task(task).name("pipeline");
    taskflow.task(stop).name("pipeline stopped");

    // create task dependency
    taskflow.task(init).precede([task]);
    taskflow.task(task).precede([stop]);

    // dump the pipeline graph structure (with composition)
    let dot = taskflow.dump();
    println!("{dot}");
    assert!(dot.contains("label=\"pipeline\""));
    assert!(dot.contains("shape=box3d, color=blue"));

    // run the pipeline
    assert!(executor.run(&taskflow).wait().is_ok());

    assert_eq!(pipeline.num_tokens(), 5);

    // The last (serial) pipe's results come out in strict token order: the
    // first five tokens, each run through two "+1" stages.
    assert_eq!(
        results.lock().expect("uncontended").as_slice(),
        [2, 3, 4, 5, 6]
    );

    // The parallel pipe promises no order among its own tokens, so the
    // check sorts first: the *set* of values it saw is the first five
    // tokens, unincremented.
    let mut seen = stage2_seen.lock().expect("uncontended").clone();
    seen.sort_unstable();
    assert_eq!(seen, [0, 1, 2, 3, 4]);
}

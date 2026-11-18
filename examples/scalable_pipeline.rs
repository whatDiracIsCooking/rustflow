//! Translated from `examples/scalable_pipeline.cpp` (Taskflow v4.1.0).
//!
//! A pipeline over a runtime-sized range of pipes -- upstream's
//! `tf::ScalablePipeline`, this port's own [`Pipeline`] since one type
//! stands for both of upstream's templates (`scheduler`'s own module docs
//! say why). Builds a chain of three identical serial pipes, runs it, then
//! [`Pipeline::reset_pipes`] grows the chain to five and runs again.
//!
//! **One sharp edge, not in upstream:** composing a [`Pipeline`] into a
//! graph and *then* calling [`Pipeline::reset_pipes`] leaves that
//! composition pointing at the pre-reset graph -- `scheduler`'s own `#
//! Design` section says why, and upstream's own order (compose once, reset,
//! rerun the same composed task) does not survive the translation. This
//! port therefore composes a *second* time, into a second taskflow, after
//! the reset -- the two runs below are two separate graphs rather than one
//! graph run twice, which is the one place this file's structure departs
//! from upstream's.
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

const NUM_LINES: usize = 4;

/// Builds one pipe of a chain of `num_pipes` identical ones: upstream's one
/// `pipe_callable`, reused by value for every `pipes.emplace_back` call
/// there. Rust closures with only `Clone` captures already implement
/// `Clone` themselves, so this could instead build the closure once and
/// `.clone()` it per pipe the way upstream does; built fresh per call
/// instead, since a plain function call reads the same and needs no extra
/// bound spelled out at the call site.
fn make_pipe(
    buffer: Arc<Vec<AtomicUsize>>,
    results: Arc<Mutex<Vec<usize>>>,
    num_pipes: usize,
) -> Pipe {
    Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
        if pf.pipe() == 0 {
            // first stage generates only 5 scheduling tokens and saves the
            // token number into the buffer.
            if pf.token() == 5 {
                pf.stop();
            } else {
                println!("stage 1: input token = {}", pf.token());
                buffer[pf.line()].store(pf.token(), Ordering::Relaxed);
            }
        } else {
            // other stages propagate the previous result to this pipe and
            // increment it by one
            let previous = buffer[pf.line()].load(Ordering::Relaxed);
            println!(
                "stage {}: input buffer[{}] = {previous}",
                pf.pipe() + 1,
                pf.line()
            );
            let value = previous + 1;
            buffer[pf.line()].store(value, Ordering::Relaxed);
            if pf.pipe() == num_pipes - 1 {
                results.lock().expect("uncontended").push(value);
            }
        }
    })
}

fn main() {
    let executor = Executor::default();

    // Carried across both runs, the way upstream's `buffer` array is not
    // part of the pipeline's own state and so is not reset by
    // `reset_pipes` either.
    let buffer: Arc<Vec<AtomicUsize>> =
        Arc::new((0..NUM_LINES).map(|_| AtomicUsize::new(0)).collect());
    let results: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));

    // create a pipeline of four parallel lines using a vector of three pipes
    let pipes = (0..3)
        .map(|_| make_pipe(Arc::clone(&buffer), Arc::clone(&results), 3))
        .collect();
    let mut pipeline = Pipeline::new(NUM_LINES, pipes);

    // build the pipeline graph using composition
    let mut taskflow = Taskflow::with_name("pipeline");
    let init = taskflow.emplace(|| println!("ready"));
    let task = taskflow.composed_of(&pipeline);
    let stop = taskflow.emplace(|| println!("stopped"));
    taskflow.task(init).name("starting pipeline");
    taskflow.task(task).name("pipeline");
    taskflow.task(stop).name("pipeline stopped");
    taskflow.task(init).precede([task]);
    taskflow.task(task).precede([stop]);

    // dump the pipeline graph structure (with composition)
    let dot = taskflow.dump();
    println!("{dot}");
    assert!(dot.contains("label=\"pipeline\""));

    // run the pipeline
    assert!(executor.run(&taskflow).wait().is_ok());

    assert_eq!(pipeline.num_pipes(), 3);
    assert_eq!(pipeline.num_tokens(), 5);
    assert_eq!(
        results.lock().expect("uncontended").as_slice(),
        [2, 3, 4, 5, 6]
    );
    results.lock().expect("uncontended").clear();

    // reset the pipeline to a new range of five pipes and start from the
    // initial state (i.e., token counts from zero)
    let more_pipes = (0..5)
        .map(|_| make_pipe(Arc::clone(&buffer), Arc::clone(&results), 5))
        .collect();
    pipeline.reset_pipes(more_pipes);
    assert_eq!(pipeline.num_pipes(), 5);
    assert_eq!(pipeline.num_tokens(), 0);

    // Composing *after* the reset, into a graph of its own: this module's
    // own doc says why the already-composed `task` above cannot be reused
    // for this second run.
    let mut taskflow2 = Taskflow::with_name("pipeline");
    let init2 = taskflow2.emplace(|| println!("ready"));
    let task2 = taskflow2.composed_of(&pipeline);
    let stop2 = taskflow2.emplace(|| println!("stopped"));
    taskflow2.task(init2).name("starting pipeline");
    taskflow2.task(task2).name("pipeline");
    taskflow2.task(stop2).name("pipeline stopped");
    taskflow2.task(init2).precede([task2]);
    taskflow2.task(task2).precede([stop2]);

    assert!(executor.run(&taskflow2).wait().is_ok());

    assert_eq!(pipeline.num_tokens(), 5);
    assert_eq!(
        results.lock().expect("uncontended").as_slice(),
        [4, 5, 6, 7, 8]
    );
}

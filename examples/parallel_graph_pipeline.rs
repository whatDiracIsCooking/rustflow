//! Translated from `examples/parallel_graph_pipeline.cpp` (Taskflow
//! v4.1.0).
//!
//! Pipelines a topological order of a small DAG --
//!
//! ```text
//!     |-> B
//! A --|
//!     |-> C
//! ```
//!
//! -- through three linearly dependent stage functions, over two parallel
//! lines. "Parallel" here names the lines the pipeline runs its tokens
//! over, not [`PipeType::Parallel`]: all three pipes are
//! [`PipeType::Serial`], as upstream's own program builds them.
//!
//! Upstream's program only *prints* each stage call. This milestone's
//! examples print *and* **assert**: every line upstream shows is also
//! checked with `assert!` or `assert_eq!`, so a non-zero exit from the
//! binary is itself the test. See the README's "Examples" section.

use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
use rustflow::pipeline::scheduler::Pipeline;
use rustflow::prelude::*;

/// 1st-stage function: upstream's `f1`.
fn f1(node: &str, log: &Mutex<Vec<String>>) {
    println!("f1({node})");
    log.lock().expect("uncontended").push(node.to_string());
}

/// 2nd-stage function: upstream's `f2`.
fn f2(node: &str, log: &Mutex<Vec<String>>) {
    println!("f2({node})");
    log.lock().expect("uncontended").push(node.to_string());
}

/// 3rd-stage function: upstream's `f3`.
fn f3(node: &str, log: &Mutex<Vec<String>>) {
    println!("f3({node})");
    log.lock().expect("uncontended").push(node.to_string());
}

fn main() {
    let mut taskflow = Taskflow::with_name("graph processing pipeline");
    let executor = Executor::default();

    const NUM_LINES: usize = 2;

    // a topological order of the graph
    //    |-> B
    // A--|
    //    |-> C
    let nodes: Arc<Vec<String>> = Arc::new(vec!["A".to_string(), "B".to_string(), "C".to_string()]);

    // Each stage's own log, one entry per call: every pipe here is serial,
    // so each log fills in strict token order -- the same order `nodes`
    // lists its entries in.
    let f1_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let f2_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let f3_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    // the pipeline consists of three serial pipes and up to two concurrent
    // scheduling tokens
    let pipe0 = {
        let nodes = Arc::clone(&nodes);
        let f1_log = Arc::clone(&f1_log);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            if pf.token() == nodes.len() {
                pf.stop();
            } else {
                f1(&nodes[pf.token()], &f1_log);
            }
        })
    };

    let pipe1 = {
        let nodes = Arc::clone(&nodes);
        let f2_log = Arc::clone(&f2_log);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            f2(&nodes[pf.token()], &f2_log);
        })
    };

    let pipe2 = {
        let nodes = Arc::clone(&nodes);
        let f3_log = Arc::clone(&f3_log);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            f3(&nodes[pf.token()], &f3_log);
        })
    };

    let pipeline = Pipeline::new(NUM_LINES, vec![pipe0, pipe1, pipe2]);

    // build the pipeline graph using composition
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

    assert_eq!(pipeline.num_tokens(), nodes.len());
    assert_eq!(
        f1_log.lock().expect("uncontended").as_slice(),
        ["A", "B", "C"]
    );
    assert_eq!(
        f2_log.lock().expect("uncontended").as_slice(),
        ["A", "B", "C"]
    );
    assert_eq!(
        f3_log.lock().expect("uncontended").as_slice(),
        ["A", "B", "C"]
    );
}

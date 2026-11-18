//! Translated from `examples/taskflow_pipeline.cpp` (Taskflow v4.1.0).
//!
//! Propagates a sequence of input tokens through three linearly dependent
//! taskflows, one per pipe, over two parallel lines -- parallelism both
//! inside each taskflow and across the pipeline's own lines. Each pipe
//! runs its taskflow through [`Executor::corun`], the same pattern
//! `examples/corun.rs` demonstrates: a pipe callable runs on one of this
//! executor's own workers (as the pipeline's line task that called it
//! does), so `corun` rather than a nested [`Executor::run`] is what keeps
//! that worker in the work-stealing loop while the inner taskflow finishes,
//! instead of risking every worker blocked on a run with none left to
//! drive it.
//!
//! Upstream's program only *prints* as each task runs. This milestone's
//! examples print *and* **assert**: every line upstream shows is also
//! checked with `assert!` or `assert_eq!`, so a non-zero exit from the
//! binary is itself the test. See the README's "Examples" section.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
use rustflow::pipeline::scheduler::Pipeline;
use rustflow::prelude::*;

/// Taskflow on the first pipe: upstream's `make_taskflow1`. `A1` precedes
/// `B1` and `C1`; `D1` succeeds both.
fn make_taskflow1(runs: Arc<AtomicUsize>) -> Taskflow {
    let mut taskflow = Taskflow::new();
    let a1 = {
        let runs = Arc::clone(&runs);
        taskflow.emplace(move || {
            println!("A1");
            runs.fetch_add(1, Ordering::Relaxed);
        })
    };
    let b1 = {
        let runs = Arc::clone(&runs);
        taskflow.emplace(move || {
            println!("B1");
            runs.fetch_add(1, Ordering::Relaxed);
        })
    };
    let c1 = {
        let runs = Arc::clone(&runs);
        taskflow.emplace(move || {
            println!("C1");
            runs.fetch_add(1, Ordering::Relaxed);
        })
    };
    let d1 = taskflow.emplace(move || {
        println!("D1");
        runs.fetch_add(1, Ordering::Relaxed);
    });
    taskflow.task(a1).precede([b1, c1]);
    taskflow.task(d1).succeed([b1, c1]);
    taskflow
}

/// Taskflow on the second pipe: upstream's `make_taskflow2`, linearized
/// `A2 -> B2 -> C2 -> D2`.
fn make_taskflow2(runs: Arc<AtomicUsize>) -> Taskflow {
    let mut taskflow = Taskflow::new();
    let tasks: Vec<_> = ["A2", "B2", "C2", "D2"]
        .into_iter()
        .map(|name| {
            let runs = Arc::clone(&runs);
            taskflow.emplace(move || {
                println!("{name}");
                runs.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect();
    taskflow.linearize(tasks);
    taskflow
}

/// Taskflow on the third pipe: upstream's `make_taskflow3`. `A3` precedes
/// all three of `B3`, `C3` and `D3`.
fn make_taskflow3(runs: Arc<AtomicUsize>) -> Taskflow {
    let mut taskflow = Taskflow::new();
    let a3 = {
        let runs = Arc::clone(&runs);
        taskflow.emplace(move || {
            println!("A3");
            runs.fetch_add(1, Ordering::Relaxed);
        })
    };
    let rest: Vec<_> = ["B3", "C3", "D3"]
        .into_iter()
        .map(|name| {
            let runs = Arc::clone(&runs);
            taskflow.emplace(move || {
                println!("{name}");
                runs.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect();
    taskflow.task(a3).precede(rest);
    taskflow
}

fn main() {
    let mut taskflow = Taskflow::with_name("taskflow processing pipeline");
    let executor = Arc::new(Executor::new(4));

    const NUM_LINES: usize = 2;

    // One run counter per inner taskflow, incremented by every one of its
    // own four tasks.
    let runs1 = Arc::new(AtomicUsize::new(0));
    let runs2 = Arc::new(AtomicUsize::new(0));
    let runs3 = Arc::new(AtomicUsize::new(0));

    let taskflows = [
        Arc::new(make_taskflow1(Arc::clone(&runs1))),
        Arc::new(make_taskflow2(Arc::clone(&runs2))),
        Arc::new(make_taskflow3(Arc::clone(&runs3))),
    ];

    // the pipeline consists of three serial pipes and up to two concurrent
    // scheduling tokens
    let pipe0 = {
        let executor = Arc::clone(&executor);
        let inner = Arc::clone(&taskflows[0]);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            if pf.token() == 5 {
                // we only handle five scheduling tokens
                pf.stop();
                return;
            }
            println!("begin token {}", pf.token());
            executor.corun(&inner).expect("a worker may corun");
        })
    };

    let pipe1 = {
        let executor = Arc::clone(&executor);
        let inner = Arc::clone(&taskflows[1]);
        Pipe::new(PipeType::Serial, move |_pf: &mut Pipeflow| {
            executor.corun(&inner).expect("a worker may corun");
        })
    };

    let pipe2 = {
        let executor = Arc::clone(&executor);
        let inner = Arc::clone(&taskflows[2]);
        Pipe::new(PipeType::Serial, move |_pf: &mut Pipeflow| {
            executor.corun(&inner).expect("a worker may corun");
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

    assert_eq!(pipeline.num_tokens(), 5);
    // Each inner taskflow has four tasks, coruns once per token.
    assert_eq!(runs1.load(Ordering::Relaxed), 4 * 5);
    assert_eq!(runs2.load(Ordering::Relaxed), 4 * 5);
    assert_eq!(runs3.load(Ordering::Relaxed), 4 * 5);
}

//! Translated from `examples/parallel_data_pipeline.cpp` (Taskflow v4.1.0).
//!
//! A [`DataPipeline`] of three serial pipes, propagating a typed value
//! rather than a side buffer: `void -> i32 -> String -> void`, over three
//! parallel lines.
//!
//! Upstream's program only *prints* each stage's result. This milestone's
//! examples print *and* **assert**: every line upstream shows is also
//! checked with `assert!` or `assert_eq!`, so a non-zero exit from the
//! binary is itself the test. See the README's "Examples" section.

use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::pipeline::data_pipeline::{DataPipeline, serial_pipe, serial_pipe_with_flow};
use rustflow::prelude::*;

fn main() {
    // dataflow => void -> int -> std::string -> void
    let mut taskflow = Taskflow::with_name("pipeline");
    let executor = Executor::default();

    const NUM_LINES: usize = 3;

    // The third pipe's own outputs, in strict token order -- a serial
    // pipe's own guarantee, so this is compared without sorting.
    let results: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let pipeline = DataPipeline::builder(NUM_LINES)
        .pipe(serial_pipe_with_flow::<(), i32>(|(), pf| {
            if pf.token() == 5 {
                pf.stop();
                return 0;
            }
            println!("first pipe returns {}", pf.token());
            // Upstream's implicit `size_t -> int` narrowing; this port's
            // `cast_possible_truncation` lint asks for the check spelled out.
            i32::try_from(pf.token()).expect("a token count fits in an i32")
        }))
        .pipe(serial_pipe::<i32, String>(|input| {
            println!("second pipe returns a string of {}", input + 100);
            (input + 100).to_string()
        }))
        .pipe({
            let results = Arc::clone(&results);
            serial_pipe::<String, ()>(move |input| {
                println!("third pipe receives the input string {input}");
                results.lock().expect("uncontended").push(input);
            })
        })
        .build();

    // build the pipeline graph using composition
    let task = taskflow.composed_of(&pipeline);
    taskflow.task(task).name("pipeline");

    // dump the pipeline graph structure (with composition)
    let dot = taskflow.dump();
    println!("{dot}");
    assert!(dot.contains("label=\"pipeline\""));
    assert!(dot.contains("shape=box3d, color=blue"));

    // run the pipeline
    assert!(executor.run(&taskflow).wait().is_ok());

    assert_eq!(pipeline.num_tokens(), 5);
    assert_eq!(
        results.lock().expect("uncontended").as_slice(),
        ["100", "101", "102", "103", "104"]
    );
}

//! Translated from `examples/text_pipeline.cpp` (Taskflow v4.1.0).
//!
//! A pipeline of three pipes (serial-parallel-serial) over two parallel
//! lines, computing the most frequent character of each input string:
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
//! o -> o -> o  (string -> character counts -> (char, count))
//! ```
//!
//! Upstream's own line buffer is a `std::variant<std::string,
//! std::unordered_map<char, size_t>, std::pair<char, size_t>>` per line,
//! read and written through the pipes' shared capture of `buffer` by
//! reference; this port's [`Data`] enum stands in for the variant, and
//! every pipe closes over an `Arc` share of the buffer rather than a
//! reference, for the reason [`Pipe`]'s own docs give for its callable
//! bound.
//!
//! Upstream's program only *prints* each stage's result. This milestone's
//! examples print *and* **assert**: every line upstream shows is also
//! checked with `assert!` or `assert_eq!`, so a non-zero exit from the
//! binary is itself the test. See the README's "Examples" section.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
use rustflow::pipeline::scheduler::Pipeline;
use rustflow::prelude::*;

/// A line's own buffer slot, stood in for upstream's `std::variant`: the
/// first (serial) pipe leaves the input string here, the second (parallel)
/// pipe reads it and leaves a character-count map in its place, and the
/// third (serial) pipe reads that back out.
enum Data {
    /// The input string a line's token is carrying: upstream's
    /// `std::string` alternative.
    Text(String),
    /// Each character's count in that string: upstream's
    /// `std::unordered_map<char, size_t>` alternative.
    Counts(HashMap<char, usize>),
}

fn main() {
    let mut taskflow = Taskflow::with_name("text-processing pipeline");
    let executor = Executor::default();

    const NUM_LINES: usize = 2;

    // input data
    let input: Arc<Vec<String>> = Arc::new(
        [
            "abade", "ddddf", "eefge", "xyzzd", "ijjjj", "jiiii", "kkijk",
        ]
        .into_iter()
        .map(String::from)
        .collect(),
    );

    // custom data storage: one slot per line, see `Data`'s own docs
    let buffer: Arc<Vec<Mutex<Option<Data>>>> =
        Arc::new((0..NUM_LINES).map(|_| Mutex::new(None)).collect());

    // The second (parallel) pipe's own inputs, collected to demonstrate the
    // one thing a parallel pipe does not promise: an order among the
    // tokens it runs. Sorted before comparison below, unlike the final
    // stage's results.
    let stage2_seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    // The third (serial) pipe's own outputs, in strict token order -- a
    // serial pipe's own guarantee, so this is compared without sorting.
    let results: Arc<Mutex<Vec<(char, usize)>>> = Arc::new(Mutex::new(Vec::new()));

    // the pipeline consists of three pipes (serial-parallel-serial) and up
    // to two concurrent scheduling tokens

    // first pipe processes the input data
    let pipe0 = {
        let input = Arc::clone(&input);
        let buffer = Arc::clone(&buffer);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            if pf.token() == input.len() {
                pf.stop();
            } else {
                let text = input[pf.token()].clone();
                println!("stage 1: input token = {text}");
                *buffer[pf.line()].lock().expect("uncontended") = Some(Data::Text(text));
            }
        })
    };

    // second pipe counts the frequency of each character
    let pipe1 = {
        let buffer = Arc::clone(&buffer);
        let stage2_seen = Arc::clone(&stage2_seen);
        Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
            let mut slot = buffer[pf.line()].lock().expect("uncontended");
            let Some(Data::Text(text)) = slot.take() else {
                panic!("the first pipe always leaves text for the second to read");
            };
            stage2_seen.lock().expect("uncontended").push(text.clone());
            let mut counts = HashMap::new();
            for c in text.chars() {
                *counts.entry(c).or_insert(0) += 1;
            }
            println!("stage 2: map = {counts:?}");
            *slot = Some(Data::Counts(counts));
        })
    };

    // third pipe reduces the most frequent character
    let pipe2 = {
        let buffer = Arc::clone(&buffer);
        let results = Arc::clone(&results);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            let mut slot = buffer[pf.line()].lock().expect("uncontended");
            let Some(Data::Counts(counts)) = slot.take() else {
                panic!("the second pipe always leaves counts for the third to read");
            };
            let (&character, &count) = counts
                .iter()
                .max_by_key(|&(_, &count)| count)
                .expect("the input string is never empty");
            println!("stage 3: {character}:{count}");
            results
                .lock()
                .expect("uncontended")
                .push((character, count));
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

    assert_eq!(pipeline.num_tokens(), input.len());

    // The final (serial) stage's results, in strict token order.
    assert_eq!(
        results.lock().expect("uncontended").as_slice(),
        [
            ('a', 2),
            ('d', 4),
            ('e', 3),
            ('z', 2),
            ('j', 4),
            ('i', 4),
            ('k', 3),
        ]
    );

    // The parallel stage promises no order among its own tokens, so the
    // check sorts first: the *set* of strings it saw is the whole input.
    let mut seen = stage2_seen.lock().expect("uncontended").clone();
    seen.sort_unstable();
    let mut expected: Vec<String> = input.iter().cloned().collect();
    expected.sort_unstable();
    assert_eq!(seen, expected);
}

//! Translated from `examples/parallel_for.cpp` (Taskflow v4.1.0): its
//! `demo_for_each`, the STL-iterator half of that upstream program. The
//! scalar-index and range-based halves of the same upstream file are
//! `examples/for_loop.rs`'s own provenance instead -- that file's doc
//! comment says why the one upstream source splits across two ports here.
//!
//! Upstream builds its parallel loop straight from `taskflow.for_each(beg,
//! end, c, part)`, over an iterator pair borrowed from the enclosing scope.
//! This crate's closest match is [`AlgorithmBuilder::for_each`], the
//! owned-data `FlowBuilder` wrapper -- run first below, over an
//! `Arc<Mutex<_>>` rather than a borrowed iterator pair, since nothing
//! stands in here for a C++ reference captured by a task upstream trusts
//! the graph to serialise against. [`Runtime::for_each`] itself runs
//! second, from inside a runtime task body, over a slice that body owns
//! outright with no `Arc` and no lock
//! at all -- the shape with no upstream equivalent, since upstream's
//! `tf::Runtime` has no loop algorithms of its own.
//!
//! Upstream's program only *prints* each element as it runs. This
//! milestone's examples print *and* **assert**: every line upstream shows is
//! also checked with `assert!` or `assert_eq!`, so a non-zero exit from the
//! binary is itself the test. See the README's "Examples" section.
//!
//! [`AlgorithmBuilder::for_each`]: rustflow::algorithm::AlgorithmBuilder::for_each

use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::algorithm::partitioner::{
    DefaultClosureWrapper, DefaultPartitioner, StaticPartitioner,
};
use rustflow::prelude::*;

/// Small enough to keep `run-examples.sh`'s serial suite fast, large enough
/// to cross every partitioner's own chunking logic.
const N: usize = 2_000;

fn main() {
    let count = i32::try_from(N).expect("N fits in i32");

    // --- `AlgorithmBuilder::for_each`, the owned-data wrapper closest to
    // upstream's own `taskflow.for_each(data.begin(), data.end(), c, part)`.
    let data = Arc::new(Mutex::new((0..count).collect::<Vec<i32>>()));
    let mut taskflow = Taskflow::new();
    taskflow.for_each(
        Arc::clone(&data),
        |v: &mut i32| {
            println!("[for_each] element = {v}");
            *v *= 2;
        },
        // Upstream's `tf::StaticPartitioner()`: equal chunks decided before
        // any element runs, the right choice here since every element costs
        // the same.
        StaticPartitioner::<DefaultClosureWrapper>::default(),
    );
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    let doubled = data.lock().expect("uncontended");
    let expected: Vec<i32> = (0..count).map(|v| v * 2).collect();
    assert_eq!(*doubled, expected, "for_each must double every element");
    drop(doubled);

    // --- `Runtime::for_each`, called directly from a runtime task body on a
    // slice it owns outright: no `Arc<Mutex<_>>` stands between the task and
    // its own data, because nothing outside this closure can see either.
    let mut taskflow = Taskflow::new();
    taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
        let mut local: Vec<i32> = (0..count).collect();
        runtime.for_each(
            &mut local,
            |v: &mut i32| *v += 1,
            DefaultPartitioner::default(),
        );
        let expected: Vec<i32> = (0..count).map(|v| v + 1).collect();
        assert_eq!(
            local, expected,
            "Runtime::for_each must increment every element"
        );
    });
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("no task panicked");
}

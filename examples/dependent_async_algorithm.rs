//! Translated from `examples/dependent_async_algorithm.cpp` (Taskflow
//! v4.1.0): a three-stage pipeline -- for-each, transform, reduce -- chained
//! through [`Executor`]'s dependent-async tasks rather than a `Taskflow`'s
//! graph edges.
//!
//! Upstream builds each stage from a free function --
//! `tf::make_for_each_task`, `tf::make_transform_task`,
//! `tf::make_reduce_task` -- that hands back a plain callable, which
//! `executor.silent_dependent_async` then submits directly. This crate's
//! `make_for_each_task`, `make_transform_task` and `make_reduce_task` are
//! that same family, so each stage below is a single
//! [`Executor::silent_dependent_async_runtime`] call over a factory's
//! product, with no closure written at the call site: the product is handed
//! its `&mut Runtime` when it *runs*, and never needed one to be built. The
//! owned-data `FlowBuilder` wrappers on `AlgorithmBuilder` -- what every
//! other algorithm example in this milestone calls -- do not reach here at
//! all, since each of them builds its task through
//! `FlowBuilder::emplace_runtime` and a dependent-async task is not queued
//! into any `Taskflow` for that to call. The factory is the layer the two
//! forms share.
//!
//! The `Arc<Mutex<_>>` the three stages pass between them is a real
//! divergence from upstream, whose three factories alias one `std::vector`
//! across all three stages with no lock at all. The factories do not remove
//! the lock -- a writing factory is handed a locked vector and the family's
//! read side accepts one, which is what lets stage B and stage C read what
//! stage A wrote -- they remove the hand-rolling of it; `DESIGN.md`'s
//! decisions on the factory family say why the port accepts the lock rather
//! than an aliasing stand-in that would need `unsafe`. The two dependency
//! edges named below are what make the sharing sound, and are required:
//! `src/algorithm/mod.rs`'s `SliceSource` docs state that two stages sharing
//! one lock must be joined by an edge even when both only read.
//!
//! Upstream's program only *prints* its result. This milestone's examples
//! print *and* **assert**: the same check upstream throws on is also
//! checked with `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section.

use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::algorithm::partitioner::DefaultPartitioner;
use rustflow::algorithm::{make_for_each_task, make_reduce_task, make_transform_task};

/// Upstream's `N = 65536`, kept here too: every element is touched by three
/// cheap passes with no allocation per element, so the whole chain stays
/// well under `run-examples.sh`'s per-example budget.
const N: usize = 65_536;

fn main() {
    let executor = Executor::default();

    let data = Arc::new(Mutex::new(vec![0_i32; N]));
    let doubled = Arc::new(Mutex::new(vec![0_i32; N]));
    let sum = Arc::new(Mutex::new(1_i32));

    // for-each: upstream's `tf::make_for_each_task(data.begin(), data.end(),
    // [](int& i){ i = 1; })`.
    let stage_a = executor.silent_dependent_async_runtime(
        make_for_each_task(
            Arc::clone(&data),
            |i: &mut i32| *i = 1,
            DefaultPartitioner::default(),
        ),
        [],
    );

    // transform: upstream's `tf::make_transform_task(data.begin(),
    // data.end(), data.begin(), [](int& i){ return i*2; })`. Upstream
    // transforms in place; this port's `Runtime::transform` always writes
    // into a second slice (that module's own `# Design` section says why one
    // `&mut [T]` cannot stand in for both), so the doubled values land in
    // their own `Vec` -- which is also what keeps this one call's read
    // source and write target two distinct locks, as the factory family
    // requires.
    let stage_b = executor.silent_dependent_async_runtime(
        make_transform_task(
            Arc::clone(&data),
            Arc::clone(&doubled),
            |i: &i32| i * 2,
            DefaultPartitioner::default(),
        ),
        [&stage_a],
    );

    // reduce: upstream's `tf::make_reduce_task(data.begin(), data.end(),
    // sum, std::plus<int>{})` -- over `doubled` here, where upstream's
    // in-place transform left the doubled values in `data` itself.
    let _stage_c = executor.silent_dependent_async_runtime(
        make_reduce_task(
            Arc::clone(&doubled),
            Arc::clone(&sum),
            |a: &i32, b: &i32| a + b,
            DefaultPartitioner::default(),
        ),
        [&stage_b],
    );

    // wait for all async tasks to complete
    executor.wait_for_all();

    // verify the result. `make_reduce_task` folds into an `Arc<Mutex<T>>`, so
    // reading its answer back out is the one lock this program still takes:
    // none of the three stages takes one of its own.
    let total = *sum.lock().expect("uncontended");
    let expected = i32::try_from(N).expect("N fits in i32") * 2 + 1;
    assert_eq!(total, expected, "INCORRECT RESULT");
    println!("CORRECT RESULT");
}

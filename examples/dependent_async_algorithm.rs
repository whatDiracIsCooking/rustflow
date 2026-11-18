//! Translated from `examples/dependent_async_algorithm.cpp` (Taskflow
//! v4.1.0): a three-stage pipeline -- for-each, transform, reduce -- chained
//! through [`Executor`]'s dependent-async tasks rather than a `Taskflow`'s
//! graph edges.
//!
//! Upstream builds each stage from a free function --
//! `tf::make_for_each_task`, `tf::make_transform_task`,
//! `tf::make_reduce_task` -- that hands back a plain callable, which
//! `executor.silent_dependent_async` then submits directly. This crate has
//! no such free functions: [`Runtime::for_each`], [`Runtime::transform`] and
//! [`Runtime::reduce`] are methods on a live `&mut Runtime`, not callables a
//! factory can hand back cold, because the chunk loop underneath every one
//! of them is built on [`Runtime::scope`] (`src/algorithm/for_each.rs`'s own
//! `# Design` section says why). So each stage here is
//! [`Executor::silent_dependent_async_runtime`] wrapping a closure that
//! locks its own `Arc<Mutex<_>>` state and calls the matching `Runtime::`
//! method -- see this example's PR for why that shape, not a free function,
//! is what the port actually has, and what it costs: three explicit locks
//! where upstream's iterators alias the same `std::vector` across all three
//! stages with none at all, trusted only by the dependency edges named
//! below. None of the owned-data wrappers on [`AlgorithmBuilder`] -- the
//! owned-data `FlowBuilder` wrappers every other example in this milestone
//! uses -- apply here at all: all three build a task by calling
//! [`FlowBuilder::emplace_runtime`](rustflow::FlowBuilder::emplace_runtime),
//! and a dependent-async task is not queued into any `Taskflow` for that to
//! call.
//!
//! Upstream's program only *prints* its result. This milestone's examples
//! print *and* **assert**: the same check upstream throws on is also
//! checked with `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section.
//!
//! [`AlgorithmBuilder`]: rustflow::algorithm::AlgorithmBuilder

use std::sync::{Arc, Mutex};

use rustflow::algorithm::partitioner::DefaultPartitioner;
use rustflow::{Executor, Runtime};

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
    let stage_a = {
        let data = Arc::clone(&data);
        executor.silent_dependent_async_runtime(
            move |runtime: &mut Runtime<'_>| {
                let mut guard = data
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                runtime.for_each(
                    &mut guard,
                    |i: &mut i32| *i = 1,
                    DefaultPartitioner::default(),
                );
            },
            [],
        )
    };

    // transform: upstream's `tf::make_transform_task(data.begin(),
    // data.end(), data.begin(), [](int& i){ return i*2; })`. Upstream
    // transforms in place; this port's `Runtime::transform` always writes
    // into a second slice (that module's own `# Design` section says why
    // one `&mut [T]` cannot stand in for both), so the doubled values land
    // in their own `Vec` instead.
    let stage_b = {
        let data = Arc::clone(&data);
        let doubled = Arc::clone(&doubled);
        executor.silent_dependent_async_runtime(
            move |runtime: &mut Runtime<'_>| {
                let data_guard = data
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let mut doubled_guard = doubled
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                runtime.transform(
                    &data_guard,
                    &mut doubled_guard,
                    |i: &i32| i * 2,
                    DefaultPartitioner::default(),
                );
            },
            [&stage_a],
        )
    };

    // reduce: upstream's `tf::make_reduce_task(data.begin(), data.end(),
    // sum, std::plus<int>{})`.
    let _stage_c = {
        let doubled = Arc::clone(&doubled);
        let sum = Arc::clone(&sum);
        executor.silent_dependent_async_runtime(
            move |runtime: &mut Runtime<'_>| {
                let doubled_guard = doubled
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let mut sum_guard = sum
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                runtime.reduce(
                    &doubled_guard,
                    &mut sum_guard,
                    |a: &i32, b: &i32| a + b,
                    DefaultPartitioner::default(),
                );
            },
            [&stage_b],
        )
    };

    // wait for all async tasks to complete
    executor.wait_for_all();

    // verify the result
    let total = *sum.lock().expect("uncontended");
    let expected = i32::try_from(N).expect("N fits in i32") * 2 + 1;
    assert_eq!(total, expected, "INCORRECT RESULT");
    println!("CORRECT RESULT");
}

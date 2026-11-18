//! Translated from `examples/parallel_reduce.cpp` (Taskflow v4.1.0): its
//! `reduce` and `transform_reduce` procedures (upstream picks one of its
//! three procedures from `argv[1]`; this example runs both, fixed, the way
//! every other port in this milestone turns a command-line choice into a
//! sequence run once). `reduce_by_index` is not carried over -- covering it
//! here as well would make this file the slowest of the seven, past what
//! "Keep each example fast" asks for, and `rustflow::algorithm::reduce`'s
//! own doctest already exercises [`Runtime::reduce_by_index`].
//!
//! The first section runs upstream's `reduce` benchmark through
//! [`ReduceBuilder::reduce`], the owned-data `FlowBuilder` wrapper closest
//! to upstream's own `taskflow.reduce(beg, end, init, bop)`. The second
//! replaces upstream's `Data` struct (`a*a + 2*a*b + b*b`, chosen there only
//! to give `transform` something to compute) with the algebraically
//! identical `(a + b) * (a + b)`, and runs it through [`Runtime::transform_reduce`]
//! directly, from inside a runtime task body -- the slice-borrowing form with
//! no upstream equivalent, since upstream's `tf::Runtime` has no reduction
//! algorithms of its own.
//!
//! Upstream's program only *prints* its timings and a correctness line. This
//! milestone's examples print *and* **assert**: every correctness check
//! upstream prints is also checked with `assert!` or `assert_eq!`, so a
//! non-zero exit from the binary is itself the test. See the README's
//! "Examples" section.

use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::algorithm::partitioner::DefaultPartitioner;
use rustflow::prelude::*;

/// Small enough to keep `run-examples.sh`'s serial suite fast, large enough
/// to cross the default partitioner's own chunking logic.
const N: usize = 5_000;

/// A tiny xorshift64 generator: this crate's dependency policy admits no
/// `rand`-family crate for an example that only needs *some* varied data,
/// not real randomness -- see the README's "Dependency policy" section.
fn next_u64(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// One pseudo-random value in `[-bound, bound]`, read off `state`.
///
/// `bound` is always called here with a positive literal, so the
/// `u64::try_from` below never fails.
fn next_bounded(state: &mut u64, bound: i32) -> i32 {
    let bound_u64 = u64::try_from(bound).expect("bound is positive");
    let span = bound_u64 * 2 + 1;
    let magnitude = i64::try_from(next_u64(state) % span).expect("remainder fits in i64");
    i32::try_from(magnitude - i64::from(bound)).expect("value fits back in i32")
}

fn main() {
    reduce_finds_the_minimum();
    transform_reduce_finds_the_minimum_of_a_derived_value();
}

/// Upstream's `reduce(N)`: the minimum of `N` pseudo-random values, via
/// [`ReduceBuilder::reduce`].
fn reduce_finds_the_minimum() {
    let mut state = 0xC0FF_EE01_u64;
    let data: Arc<Vec<i32>> = Arc::new(
        (0..N)
            .map(|_| next_bounded(&mut state, 1_000_000))
            .collect(),
    );
    let expected = *data.iter().min().expect("N > 0");

    let init = Arc::new(Mutex::new(i32::MAX));
    let mut taskflow = Taskflow::new();
    taskflow.reduce(
        Arc::clone(&data),
        Arc::clone(&init),
        |a: &i32, b: &i32| (*a).min(*b),
        DefaultPartitioner::default(),
    );
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    assert_eq!(*init.lock().expect("uncontended"), expected);
}

/// Upstream's `transform_reduce(N)`, with its `Data::transform` simplified
/// to an algebraically identical expression (see this file's own doc
/// comment): the minimum, over `N` pseudo-random pairs, of `(a + b)^2`, via
/// [`Runtime::transform_reduce`] called directly from a runtime task body.
fn transform_reduce_finds_the_minimum_of_a_derived_value() {
    let mut state = 0xBAD5_EED2_u64;
    let pairs: Vec<(i32, i32)> = (0..N)
        .map(|_| {
            (
                next_bounded(&mut state, 1_000),
                next_bounded(&mut state, 1_000),
            )
        })
        .collect();
    let expected = pairs
        .iter()
        .map(|&(a, b)| {
            let sum = i64::from(a) + i64::from(b);
            sum * sum
        })
        .min()
        .expect("N > 0");

    let mut taskflow = Taskflow::new();
    taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
        let mut minimum = i64::MAX;
        runtime.transform_reduce(
            &pairs,
            &mut minimum,
            |a: &i64, b: &i64| (*a).min(*b),
            |&(a, b): &(i32, i32)| {
                let sum = i64::from(a) + i64::from(b);
                sum * sum
            },
            DefaultPartitioner::default(),
        );
        assert_eq!(minimum, expected);
    });
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("no task panicked");
}

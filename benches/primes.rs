//! Translated from `benchmarks/primes/` (Taskflow v4.1.0).
//!
//! Counts the primes in `[1, value)` by trial division. Upstream's
//! `primes_taskflow` is one `reduce_by_index` over
//! `IndexRange<size_t>(1, value, 1)` with `DynamicPartitioner{primes_chunk}`,
//! whose local operator folds `is_prime(i)` over a chunk and whose global one
//! is `std::plus`. There is no graph here beyond that single node, so
//! "ours includes building the graph" from `BENCHMARKS.md`'s own header costs
//! this shape one node and nothing else -- the whole measurement is the
//! partitioner handing out chunks of ten indices and the trial division
//! inside them.
//!
//! The baseline is `(1..value).into_par_iter().filter(is_prime).count()`, in
//! the role TBB's `parallel_reduce` over `blocked_range(1, value,
//! primes_chunk)` plays in upstream's own `tbb.cpp`. One asymmetry to read
//! the table with: upstream hands TBB the same chunk of ten it hands the
//! `DynamicPartitioner`, while rayon's parallel iterator splits adaptively
//! from the length of the range and takes no chunk size at all. That is
//! rayon's own scheduling decision and the thing being compared, so it is
//! left alone rather than forced to ten with `with_min_len`.
//!
//! `is_prime` is upstream's lambda from `primes.hpp`, down to its `6k ± 1`
//! wheel, and both columns run that same function: the count they
//! agree on is asserted against a single-threaded reference before either is
//! measured, so a wrong answer fails the benchmark rather than being timed.
//!
//! A third column, `rustflow-split`, runs the identical task under
//! `SplittingPartitioner` at the same `primes_chunk` -- the port's own chunk
//! scheme, whose share re-splits when another worker steals it. This is the
//! shape it was built for, the cost of a chunk rising with the index. No
//! figure for it is claimed here: the milestone's A/B issue is what measures
//! the pair.

use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rustflow::Executor;
use rustflow::algorithm::index_range::IndexRange;
use rustflow::algorithm::partitioner::{
    DefaultClosureWrapper, DynamicPartitioner, SplittingPartitioner,
};
use rustflow::algorithm::reduce::ReducePartitioner;
use rustflow::prelude::*;

#[path = "common/baseline.rs"]
mod baseline;
mod common;

/// Upper bounds on the range, two points off upstream's sweep (`p = 10`,
/// `100`, .. `primes_limit = 1e8`, ×10 a step): one where the fixed cost of a
/// run still shows against a thousand chunks of work, one where the trial
/// division dominates it. Upstream's own top point is minutes of arithmetic
/// per round, which criterion's twenty samples an id would multiply.
const SIZES: [usize; 2] = [10_000, 1_000_000];

/// Upstream's `primes_chunk`: the [`DynamicPartitioner`]'s chunk size.
const PRIMES_CHUNK: usize = 10;

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// Upstream's `is_prime` from `primes.hpp`: trial division over the `6k ± 1`
/// wheel, with 2 and 3 answered directly.
fn is_prime(n: usize) -> bool {
    if n == 2 || n == 3 {
        return true;
    }

    // Upstream's `n % 2 == 0 || n % 3 == 0`, spelled as the standard
    // library's own predicate for it.
    if n <= 1 || n.is_multiple_of(2) || n.is_multiple_of(3) {
        return false;
    }

    // Upstream's `for (int i = 5; i * i <= n; i += 6)`, over `size_t` rather
    // than `int` so the square cannot overflow the index type the range is
    // built from.
    let mut i = 5;
    while i * i <= n {
        if n.is_multiple_of(i) || n.is_multiple_of(i + 2) {
            return false;
        }
        i += 6;
    }

    true
}

/// Upstream's `primes_taskflow`: one `reduce_by_index` task over
/// `[1, value)`, run to completion, answering the count it folded.
fn primes_rustflow<P>(value: usize, executor: &Executor, part: P) -> usize
where
    P: ReducePartitioner + Clone + Send + 'static,
{
    let mut taskflow = Taskflow::new();
    // Upstream's `size_t sum = 0` lives on the stack of the function that
    // builds the taskflow and is read after the run; the port's algorithm
    // wrappers take their accumulator as shared owned state instead, since a
    // task body is `'static` and cannot borrow a caller's local.
    let sum = Arc::new(Mutex::new(0_usize));

    taskflow.reduce_by_index(
        IndexRange::new(1_usize, value, 1),
        Arc::clone(&sum),
        |subrange: IndexRange<usize>, running_total: Option<usize>| {
            let mut residual = running_total.unwrap_or(0);
            let mut i = subrange.begin();
            while i < subrange.end() {
                // Upstream's `residual += is_prime(i)`, which leans on C++'s
                // `bool` to `size_t` promotion.
                residual += usize::from(is_prime(i));
                i += subrange.step_size();
            }
            residual
        },
        |a: &usize, b: &usize| a + b,
        part,
    );

    executor.run(&taskflow).wait().expect("no task panicked");

    *sum.lock().expect("uncontended after the run")
}

/// Upstream's own chunk scheme for this shape, behind the `rustflow` ids.
fn dynamic() -> DynamicPartitioner<DefaultClosureWrapper> {
    DynamicPartitioner::new(PRIMES_CHUNK)
}

/// The partitioner behind the `rustflow-split` ids: the port's own splitting
/// scheme at the same [`PRIMES_CHUNK`], so the two ids differ in how a chunk
/// reaches a worker and in nothing else. No figure is claimed for either
/// here -- the milestone's A/B issue measures them.
fn splitting() -> SplittingPartitioner<DefaultClosureWrapper> {
    SplittingPartitioner::new(PRIMES_CHUNK)
}

/// The same count as a rayon parallel iterator, in the role TBB's
/// `parallel_reduce` plays upstream.
fn primes_rayon(value: usize, pool: &rayon::ThreadPool) -> usize {
    pool.install(|| (1..value).into_par_iter().filter(|&i| is_prime(i)).count())
}

/// The same count on one thread, by the same `is_prime`: the answer both
/// measured variants have to produce.
fn primes_sequential(value: usize) -> usize {
    (1..value).filter(|&i| is_prime(i)).count()
}

/// Registers both variants at every size and worker count.
fn bench_primes(c: &mut Criterion) {
    let mut group = c.benchmark_group("primes");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    // Untimed, once per size: the reference count every id below is checked
    // against.
    let references = SIZES.map(primes_sequential);

    for workers in common::worker_counts() {
        let executor = common::executor(workers);
        let pool = baseline::rayon_pool(workers);

        for (size, expected) in SIZES.into_iter().zip(references) {
            // Untimed, and once per worker count as well as per size, since a
            // count that is only right on one thread is still wrong: a bad
            // answer fails the benchmark here rather than being measured
            // below.
            assert_eq!(
                primes_rustflow(size, &executor, dynamic()),
                expected,
                "rustflow counts the primes below {size} at {workers} worker(s)"
            );
            assert_eq!(
                primes_rustflow(size, &executor, splitting()),
                expected,
                "rustflow-split counts the primes below {size} at {workers} worker(s)"
            );
            assert_eq!(
                primes_rayon(size, &pool),
                expected,
                "rayon counts the primes below {size} at {workers} worker(s)"
            );

            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| {
                    b.iter(|| primes_rustflow(black_box(size), &executor, dynamic()));
                },
            );
            group.bench_function(
                BenchmarkId::new(format!("rustflow-split/{workers}w"), size),
                |b| {
                    b.iter(|| primes_rustflow(black_box(size), &executor, splitting()));
                },
            );
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), size), |b| {
                b.iter(|| primes_rayon(black_box(size), &pool));
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_primes);
criterion_main!(benches);

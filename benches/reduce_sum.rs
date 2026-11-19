//! Translated from `benchmarks/reduce_sum/` (Taskflow v4.1.0).
//!
//! `N` `f64`s summed once: upstream's `reduce_sum_taskflow` builds a fresh
//! `tf::Taskflow`, calls `reduce_by_index(IndexRange(0, vec.size(), 1),
//! result, lop, std::plus<double>())` -- `lop` folding one sub-range into
//! the running total it is handed -- and runs it, timing both, so "ours
//! includes building the graph" from `BENCHMARKS.md`'s own header applies
//! here unchanged. `par_iter().sum::<f64>()` is the baseline, in the role
//! TBB's `parallel_reduce` plays upstream.
//!
//! **The assertion is a tolerance, not an equality**, and that is a property
//! of the shape rather than a weakened check: floating-point addition is not
//! associative, so this crate's per-worker local folded by `gop`, rayon's
//! own tree of partial sums and a sequential `Iterator::sum` each add the
//! same `N` values in a different order and land on three slightly different
//! `f64`s. The tolerance is relative and far tighter than any mis-chunking
//! would be -- dropping or repeating one chunk of a million-element sum
//! moves the answer by a fraction of a percent, five orders of magnitude
//! past [`TOLERANCE`] -- so the check still catches what it is for. See
//! `benches/for_each.rs` for the opposite case, where every output depends
//! on one input and the comparison is exact.
//!
//! One departure from upstream: the input is filled. Upstream's `main.cpp`
//! only `resize`s its `vec` and never writes it, so every round sums
//! zeroes -- an answer a run that skipped half the range would also reach.
//! The port fills the vector from the same generator `benches/sort.rs` uses,
//! which is what gives the untimed check below its power.
//!
//! That check is the untimed part of the run: before any id of a size is
//! timed, both variants run once at that size and worker count and are
//! compared against a sequential sum, so a wrong answer fails the benchmark
//! rather than being timed.
//!
//! A third column, `rustflow-split`, runs the identical task under
//! `SplittingPartitioner` -- the port's own chunk scheme, whose share
//! re-splits when another worker steals it -- so the two rustflow ids differ
//! only in how a chunk reaches a worker, and the tolerance above covers it
//! for the same reason. No figure for it is claimed here: the milestone's
//! A/B issue is what measures the pair.

use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rustflow::Executor;
use rustflow::algorithm::partitioner::{DefaultClosureWrapper, SplittingPartitioner};
use rustflow::algorithm::reduce::ReducePartitioner;
use rustflow::algorithm::{DefaultPartitioner, IndexRange};
use rustflow::prelude::*;

#[path = "common/baseline.rs"]
mod baseline;
mod common;

/// Element counts, two points off upstream's sweep (`N = 10..=1e9`, ×10 each
/// step): one where the fixed cost of building and running a one-task
/// taskflow dominates the additions, one where the additions dominate it.
const SIZES: [usize; 2] = [1_000, 1_000_000];

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// The relative distance two summation orders of the same input may land
/// apart and still be the same answer. See the module docs for why this is a
/// tolerance at all, and why a mis-chunked run is nowhere near this close.
const TOLERANCE: f64 = 1e-6;

/// `n` pseudo-random `f64`s in `[0, 1)`, over a linear congruential
/// generator rather than `rand`, so a run is reproducible. See the module
/// docs for why the input is generated at all, where upstream's is not.
fn inputs(n: usize) -> Vec<f64> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..n)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            // 53 bits of mantissa over 2^53: the whole of an `f64`'s
            // precision in `[0, 1)`, and no rounding on the way in.
            (state >> 11) as f64 / 9_007_199_254_740_992.0
        })
        .collect()
}

/// Upstream's `reduce_sum_taskflow`: a fresh [`Taskflow`], one
/// `reduce_by_index` task over the whole index range, run to completion, and
/// the total it folded into its seed.
///
/// The seed is `0.0` and the `Mutex` it lives behind is this port's answer to
/// upstream's `double result` captured by reference -- the shape
/// `AlgorithmBuilder::reduce_by_index` takes. Both are built inside the
/// measured region, as upstream's are.
fn reduce_sum_rustflow<P>(data: &Arc<Vec<f64>>, executor: &Executor, part: P) -> f64
where
    P: ReducePartitioner + Clone + Send + 'static,
{
    let mut taskflow = Taskflow::new();
    let total = Arc::new(Mutex::new(0.0_f64));
    let values = Arc::clone(data);
    taskflow.reduce_by_index(
        IndexRange::new(0_usize, data.len(), 1),
        Arc::clone(&total),
        move |sub: IndexRange<usize>, running: Option<f64>| {
            let mut partial = running.unwrap_or(0.0);
            for i in (sub.begin()..sub.end()).step_by(sub.step_size()) {
                partial += values[i];
            }
            partial
        },
        |a: &f64, b: &f64| a + b,
        part,
    );
    executor.run(&taskflow).wait().expect("no task panicked");

    *total.lock().expect("uncontended once the run has joined")
}

/// The same sum as a rayon parallel iterator, in the role TBB's
/// `parallel_reduce` plays upstream.
fn reduce_sum_rayon(data: &[f64], pool: &rayon::ThreadPool) -> f64 {
    pool.install(|| data.par_iter().sum::<f64>())
}

/// The partitioner behind the `rustflow-split` ids: the port's own
/// splitting scheme, with no configured chunk size, where the `rustflow` ids
/// take the default guided one. No figure is claimed for either here -- the
/// milestone's A/B issue measures them.
fn splitting() -> SplittingPartitioner<DefaultClosureWrapper> {
    SplittingPartitioner::default()
}

/// Asserts `actual` is within [`TOLERANCE`] of `expected`, relatively.
fn assert_close(actual: f64, expected: f64, what: &str) {
    let tolerance = TOLERANCE * expected.abs().max(1.0);
    assert!(
        (actual - expected).abs() <= tolerance,
        "{what}: {actual} is further than {tolerance} from the sequential sum {expected}"
    );
}

/// Runs both variants once, outside any measured region, and checks each
/// against a sequential sum of the same input.
///
/// The untimed half of this target's acceptance: a chunk the partitioner
/// never hands out, or a local never merged, fails here rather than
/// producing a figure.
fn verify(data: &Arc<Vec<f64>>, workers: usize, executor: &Executor, pool: &rayon::ThreadPool) {
    let size = data.len();
    let expected: f64 = data.iter().sum();

    assert_close(
        reduce_sum_rustflow(data, executor, DefaultPartitioner::default()),
        expected,
        &format!("rustflow/{workers}w at size {size}"),
    );
    assert_close(
        reduce_sum_rustflow(data, executor, splitting()),
        expected,
        &format!("rustflow-split/{workers}w at size {size}"),
    );
    assert_close(
        reduce_sum_rayon(data, pool),
        expected,
        &format!("rayon/{workers}w at size {size}"),
    );
}

/// Registers both variants at every size and worker count.
fn bench_reduce_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("reduce_sum");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in common::worker_counts() {
        let executor = common::executor(workers);
        let pool = baseline::rayon_pool(workers);

        for size in SIZES {
            // Read-only on both sides, so one vector serves every sample of
            // both ids -- upstream's `vec` is likewise filled outside
            // `measure_time_taskflow`.
            let data = Arc::new(inputs(size));
            verify(&data, workers, &executor, &pool);

            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| {
                    b.iter(|| {
                        reduce_sum_rustflow(
                            black_box(&data),
                            &executor,
                            DefaultPartitioner::default(),
                        )
                    });
                },
            );
            group.bench_function(
                BenchmarkId::new(format!("rustflow-split/{workers}w"), size),
                |b| {
                    b.iter(|| reduce_sum_rustflow(black_box(&data), &executor, splitting()));
                },
            );
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), size), |b| {
                b.iter(|| reduce_sum_rayon(black_box(&data), &pool));
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_reduce_sum);
criterion_main!(benches);

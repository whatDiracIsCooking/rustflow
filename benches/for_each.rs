//! Translated from `benchmarks/for_each/` (Taskflow v4.1.0).
//!
//! `N` `f64`s, each replaced by its own tangent: upstream's
//! `for_each_taskflow` builds a fresh `tf::Taskflow`, calls
//! `for_each_by_index(IndexRange(0, vec.size(), 1), ..)` over a body that
//! walks one sub-range, and runs it, timing both -- "ours includes building
//! the graph" from `BENCHMARKS.md`'s own header applies here unchanged.
//! `par_iter_mut().for_each` is the baseline, in the role TBB's
//! `parallel_for` plays upstream, and it is the first shape in this suite
//! where the baseline is the *same program* rather than a different schedule
//! for the same work: both sides split one index range into chunks and both
//! decide the split at run time.
//!
//! Two departures from upstream, both forced rather than chosen.
//!
//! The `rustflow` column's vector is a slice of `AtomicU64` holding
//! `f64::to_bits` patterns, not a `Vec<f64>`. Upstream's body captures a
//! global `std::vector<double>` by reference and writes `vec[i]` from
//! whichever worker holds that chunk; the ported body is one
//! `Fn(IndexRange<usize>)` shared by every chunk, so it cannot hold a
//! `&mut [f64]` -- nothing in the type of an `IndexRange` tells the compiler
//! that two chunks are disjoint. `examples/for_loop.rs`'s own 2-D grid is a
//! slice of atomics for exactly this reason. A relaxed `u64` load and store
//! is the same instruction a plain one is on the targets this is measured
//! on, so the per-element cost is upstream's; the baseline keeps a plain
//! `Vec<f64>`, since `par_iter_mut` hands each element a real `&mut f64`.
//!
//! Each sample runs on freshly generated values, because this shape mutates
//! its input in place and criterion's plain `iter` would otherwise measure
//! `tan` composed with itself a hundred times over instead of `tan` once.
//! Each id owns **one** buffer, allocated before anything is timed as
//! upstream allocates `vec` in `main`, and
//! `criterion::Bencher::iter_batched_ref`'s untimed setup rewrites it from
//! the generator between iterations. `BatchSize::PerIteration` is the only
//! correct batch size for that: a batch of `k` would refill once and run
//! `k` times. `BENCHMARKS.md`'s "#366 change" section has the measurement
//! that made this the shape of the harness.
//!
//! Correctness is checked in the untimed part of the run: before any id of a
//! size is timed, both variants run once at that size and worker count and
//! are compared element by element against a sequential `f64::tan`
//! reference, so a chunk left unvisited fails the benchmark instead of
//! being timed. The comparison is exact rather than a tolerance, unlike
//! `benches/reduce_sum.rs`'s: each output here depends on exactly one input
//! element, and both columns call the same `f64::tan` on it.
//!
//! A third column, `rustflow-split`, runs the identical task under
//! `SplittingPartitioner` -- the port's own chunk scheme, whose share
//! re-splits when another worker steals it -- so the two rustflow ids differ
//! only in how a chunk reaches a worker. No figure for it is claimed here:
//! the milestone's A/B issue is what measures the pair.

use std::cell::RefCell;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rayon::iter::{IntoParallelRefMutIterator, ParallelIterator};
use rustflow::Executor;
use rustflow::algorithm::for_each_index::IndexDispatch;
use rustflow::algorithm::partitioner::{DefaultClosureWrapper, SplittingPartitioner};
use rustflow::algorithm::{DefaultPartitioner, IndexRange};
use rustflow::prelude::*;

#[path = "common/baseline.rs"]
mod baseline;
mod common;

/// Element counts, two points off upstream's sweep (`N = 10..=1e8`, ×10 each
/// step): one where the fixed cost of building and running a one-task
/// taskflow still shows, one where the per-element `tan` dominates it.
const SIZES: [usize; 2] = [1_000, 1_000_000];

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// `n` pseudo-random `f64`s in `[0, 1)`: upstream's `d = ::rand()` loop, over
/// a linear congruential generator rather than `rand`, so a run is
/// reproducible.
///
/// Scaled into the unit interval rather than left at upstream's
/// `[0, RAND_MAX]`, so that `tan` is evaluated where it is well conditioned
/// and the reference below is a meaningful comparison rather than a race
/// between two argument reductions.
fn generated(n: usize) -> impl Iterator<Item = f64> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..n).map(move |_| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        // 53 bits of mantissa over 2^53: the whole of an `f64`'s precision
        // in `[0, 1)`, and no rounding on the way in.
        (state >> 11) as f64 / 9_007_199_254_740_992.0
    })
}

/// [`generated`]'s first `n` values as a vector: what a `rayon` id's own
/// buffer is allocated as, and what [`verify`] compares against.
fn inputs(n: usize) -> Vec<f64> {
    generated(n).collect()
}

/// The partitioner behind the `rustflow-split` ids: the port's own
/// splitting scheme, with no configured chunk size, where the `rustflow` ids
/// take the default guided one. No figure is claimed for either here -- the
/// milestone's A/B issue measures them.
fn splitting() -> SplittingPartitioner<DefaultClosureWrapper> {
    SplittingPartitioner::default()
}

/// Rewrites `data` from [`generated`], allocating nothing.
///
/// The untimed setup of a `rayon` id: the buffer is the id's own, so each
/// iteration is handed the same input the one before it was.
fn refill(data: &mut [f64]) {
    let values = generated(data.len());
    for (slot, value) in data.iter_mut().zip(values) {
        *slot = value;
    }
}

/// [`refill`] through the cell vector the `rustflow` body writes.
fn refill_cells(cells: &[AtomicU64]) {
    for (cell, value) in cells.iter().zip(generated(cells.len())) {
        cell.store(value.to_bits(), Ordering::Relaxed);
    }
}

/// `data` as the cell vector the `rustflow` body writes through. See the
/// module docs for why the elements are atomics.
fn cells(data: &[f64]) -> Arc<Vec<AtomicU64>> {
    Arc::new(data.iter().map(|d| AtomicU64::new(d.to_bits())).collect())
}

/// Reads every cell back as an `f64`, for the untimed comparison.
fn read_back(cells: &[AtomicU64]) -> Vec<f64> {
    cells
        .iter()
        .map(|cell| f64::from_bits(cell.load(Ordering::Relaxed)))
        .collect()
}

/// Upstream's `for_each_taskflow`: a fresh [`Taskflow`], one
/// `for_each_by_index` task over the whole index range, run to completion.
///
/// Generic over the partitioner so that the `rustflow` and `rustflow-split`
/// ids are the same code under the two chunk schemes -- see the module docs.
fn for_each_rustflow<P>(cells: &Arc<Vec<AtomicU64>>, executor: &Executor, part: P)
where
    P: IndexDispatch + Clone + Send + 'static,
{
    let mut taskflow = Taskflow::new();
    let body_cells = Arc::clone(cells);
    taskflow.for_each_by_index(
        IndexRange::new(0_usize, cells.len(), 1),
        move |sub: IndexRange<usize>| {
            for i in (sub.begin()..sub.end()).step_by(sub.step_size()) {
                let cell = &body_cells[i];
                let x = f64::from_bits(cell.load(Ordering::Relaxed));
                cell.store(x.tan().to_bits(), Ordering::Relaxed);
            }
        },
        part,
    );
    executor.run(&taskflow).wait().expect("no task panicked");
}

/// The same loop as a rayon mutable parallel iterator, in the role TBB's
/// `parallel_for` plays upstream.
fn for_each_rayon(data: &mut [f64], pool: &rayon::ThreadPool) {
    pool.install(|| {
        data.par_iter_mut().for_each(|d| *d = d.tan());
    });
}

/// Runs both variants once, outside any measured region, and checks each
/// against the sequential reference.
///
/// The untimed half of this target's acceptance: a chunk the partitioner
/// never hands out, or a baseline that stops early, fails here rather than
/// producing a figure.
fn verify(size: usize, workers: usize, executor: &Executor, pool: &rayon::ThreadPool) {
    let input = inputs(size);
    let expected: Vec<f64> = input.iter().map(|d| d.tan()).collect();

    // Both buffers built before `cells` shadows the function of the same
    // name, and one per id so that neither reads the other's work.
    let split_cells = cells(&input);
    let cells = cells(&input);
    for_each_rustflow(&cells, executor, DefaultPartitioner::default());
    let actual = read_back(&cells);
    let mismatch = actual.iter().zip(&expected).position(|(a, b)| a != b);
    assert_eq!(
        mismatch, None,
        "rustflow/{workers}w at size {size}: first element not equal to its own tangent"
    );

    for_each_rustflow(&split_cells, executor, splitting());
    let actual = read_back(&split_cells);
    let mismatch = actual.iter().zip(&expected).position(|(a, b)| a != b);
    assert_eq!(
        mismatch, None,
        "rustflow-split/{workers}w at size {size}: first element not equal to its own tangent"
    );

    let mut data = input;
    for_each_rayon(&mut data, pool);
    let mismatch = data.iter().zip(&expected).position(|(a, b)| a != b);
    assert_eq!(
        mismatch, None,
        "rayon/{workers}w at size {size}: first element not equal to its own tangent"
    );
}

/// Registers both variants at every size and worker count.
fn bench_for_each(c: &mut Criterion) {
    let mut group = c.benchmark_group("for_each");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in common::worker_counts() {
        let executor = common::executor(workers);
        let pool = baseline::rayon_pool(workers);

        for size in SIZES {
            verify(size, workers, &executor, &pool);

            // One buffer per id, allocated here and rewritten by the untimed
            // setup of every iteration. Allocating it per iteration instead
            // puts an 8 MB allocation, fill and free either side of the
            // timed region and ~0.7 ms of it *inside*, which is what
            // `BENCHMARKS.md`'s "#366 change" section measures.
            // Built before `cells` shadows the function of the same
            // name; one buffer per id, never shared between two.
            let split_cells = cells(&inputs(size));
            let cells = cells(&inputs(size));
            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| {
                    b.iter_batched_ref(
                        || refill_cells(&cells),
                        |_: &mut ()| {
                            for_each_rustflow(
                                black_box(&cells),
                                &executor,
                                DefaultPartitioner::default(),
                            );
                        },
                        BatchSize::PerIteration,
                    );
                },
            );

            group.bench_function(
                BenchmarkId::new(format!("rustflow-split/{workers}w"), size),
                |b| {
                    b.iter_batched_ref(
                        || refill_cells(&split_cells),
                        |_: &mut ()| {
                            for_each_rustflow(black_box(&split_cells), &executor, splitting());
                        },
                        BatchSize::PerIteration,
                    );
                },
            );

            // A `RefCell` only so that the untimed setup and the timed
            // closure can each reach this id's one buffer; nothing but this
            // thread ever borrows it, and `par_iter_mut` still gets the
            // `&mut [f64]` the module docs require.
            let data = RefCell::new(inputs(size));
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), size), |b| {
                b.iter_batched_ref(
                    || refill(&mut data.borrow_mut()),
                    |_: &mut ()| {
                        let mut data = data.borrow_mut();
                        for_each_rayon(black_box(&mut data), &pool);
                    },
                    BatchSize::PerIteration,
                );
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_for_each);
criterion_main!(benches);

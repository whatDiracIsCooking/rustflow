//! Translated from `benchmarks/sort/` (Taskflow v4.1.0).
//!
//! `N` random `f64`s, sorted once: upstream's `measure_time_taskflow` builds
//! a fresh `tf::Taskflow`, calls `taskflow.sort(vec.begin(), vec.end())` and
//! runs it, timing both -- "ours includes building the graph" from
//! `BENCHMARKS.md`'s own header applies here unchanged. `rayon`'s
//! `par_sort_unstable` is the baseline, in the role TBB's `parallel_sort`
//! plays upstream (the acceptance this benchmark was written against names
//! it directly).
//!
//! One adaptation, forced by criterion rather than chosen: upstream
//! regenerates its data once per outer round and runs every sample of that
//! round against the same, now-partly-sorted `vec` (`taskflow.cpp`'s `vec`
//! is a file-level global, mutated in place by the previous round's sort).
//! Criterion's `Bencher::iter` would do worse than that -- every sample
//! after the first would sort data the *same* closure already finished
//! sorting a moment before -- so each iteration here is
//! [`criterion::Bencher::iter_batched_ref`] instead, whose untimed setup
//! re-shuffles the one buffer the id owns. That buffer is allocated before
//! anything is timed, where upstream's global lives, and
//! `BatchSize::PerIteration` is the only correct batch size for it: a batch
//! of `k` would shuffle once and sort `k` times. `BENCHMARKS.md`'s "#366
//! change" section has the measurement that made this the shape of the
//! harness.

use std::cell::RefCell;
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rayon::slice::ParallelSliceMut;
use rustflow::Executor;
use rustflow::prelude::*;

#[path = "common/baseline.rs"]
mod baseline;
mod common;

/// Element counts, two points off upstream's sweep (`N = 10..=1e8`, ×10 each
/// step): one inside [`src/algorithm/sort.rs`][cutoff]'s own cutoff for
/// `f64` (`4096`, below `16` bytes), where the fixed cost of a run still
/// shows, one well above it, where the per-element cost dominates.
///
/// [cutoff]: rustflow::algorithm::sort
const SIZES: [usize; 2] = [1_000, 1_000_000];

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// `n` random `f64`s: upstream's `d = ::rand()` loop, over a linear
/// congruential generator rather than `rand`, so a run is reproducible.
fn generated(n: usize) -> impl Iterator<Item = f64> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..n).map(move |_| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        // Upstream's `::rand()` range (`[0, RAND_MAX]`), scaled to `f64`
        // rather than left as `int`: `taskflow.cpp`'s `vec` is already a
        // `vector<double>`, so there is no integer type here to match.
        (state >> 11) as f64
    })
}

/// [`generated`]'s first `n` values as a vector: what each id's own buffer
/// is allocated as.
fn shuffled(n: usize) -> Vec<f64> {
    generated(n).collect()
}

/// Rewrites `data` from [`generated`], allocating nothing: the untimed setup
/// that hands every iteration the same unsorted input the one before it was
/// handed.
fn reshuffle(data: &mut [f64]) {
    let values = generated(data.len());
    for (slot, value) in data.iter_mut().zip(values) {
        *slot = value;
    }
}

/// Upstream's `sort_taskflow`: a fresh [`Taskflow`], `taskflow.sort`, run to
/// completion. `sort_by` rather than `sort`, since `f64` is not [`Ord`] --
/// upstream's own `vec` is a plain `vector<double>` sorted with no
/// comparator, `std::less<double>{}` implicit in it.
fn sort_rustflow(data: &Arc<Mutex<Vec<f64>>>, executor: &Executor) {
    let mut taskflow = Taskflow::new();
    taskflow.sort_by(Arc::clone(data), f64::total_cmp);
    executor.run(&taskflow).wait().expect("no task panicked");
}

/// The same sort as a rayon parallel slice sort, in the role TBB's
/// `parallel_sort` plays upstream.
fn sort_rayon(data: &mut [f64], pool: &rayon::ThreadPool) {
    pool.install(|| data.par_sort_unstable_by(f64::total_cmp));
}

/// Registers both variants at every size and worker count.
fn bench_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in common::worker_counts() {
        let executor = common::executor(workers);
        let pool = baseline::rayon_pool(workers);

        for size in SIZES {
            // One buffer per id, allocated here and re-shuffled by the
            // untimed setup of every iteration. Allocating it per iteration
            // instead puts an 8 MB allocation, fill and free either side of
            // the timed region and part of it *inside*, which is what
            // `BENCHMARKS.md`'s "#366 change" section measures. The `Mutex`
            // is the one `sort_by` takes rather than one this harness adds,
            // and it is now built outside the timed region too.
            let data = Arc::new(Mutex::new(shuffled(size)));
            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| {
                    b.iter_batched_ref(
                        || reshuffle(&mut data.lock().expect("uncontended between iterations")),
                        |_: &mut ()| sort_rustflow(black_box(&data), &executor),
                        BatchSize::PerIteration,
                    );
                },
            );

            // A `RefCell` only so that the untimed setup and the timed
            // closure can each reach this id's one buffer; nothing but this
            // thread ever borrows it, and `par_sort_unstable_by` still gets
            // the `&mut [f64]` it needs.
            let plain = RefCell::new(shuffled(size));
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), size), |b| {
                b.iter_batched_ref(
                    || reshuffle(&mut plain.borrow_mut()),
                    |_: &mut ()| {
                        let mut plain = plain.borrow_mut();
                        sort_rayon(black_box(&mut plain), &pool);
                    },
                    BatchSize::PerIteration,
                );
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_sort);
criterion_main!(benches);

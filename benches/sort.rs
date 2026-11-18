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
//! sorting a moment before -- so each sample here is
//! [`criterion::Bencher::iter_batched`] instead: a fresh shuffle in the
//! untimed setup, consumed by the timed sort.

use std::hint::black_box;
use std::num::NonZeroUsize;
use std::thread::available_parallelism;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rayon::slice::ParallelSliceMut;
use rustflow::Executor;
use rustflow::prelude::*;

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

/// The worker counts every shape is measured at: one, and the machine's own.
fn worker_counts() -> Vec<usize> {
    let parallel = available_parallelism().map_or(1, NonZeroUsize::get);
    if parallel > 1 {
        vec![1, parallel]
    } else {
        vec![1]
    }
}

/// `n` random `f64`s: upstream's `d = ::rand()` loop, over a linear
/// congruential generator rather than `rand`, so a run is reproducible.
fn shuffled(n: usize) -> Vec<f64> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..n)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            // Upstream's `::rand()` range (`[0, RAND_MAX]`), scaled to `f64`
            // rather than left as `int`: `taskflow.cpp`'s `vec` is already a
            // `vector<double>`, so there is no integer type here to match.
            (state >> 11) as f64
        })
        .collect()
}

/// Upstream's `sort_taskflow`: a fresh [`Taskflow`], `taskflow.sort`, run to
/// completion. `sort_by` rather than `sort`, since `f64` is not [`Ord`] --
/// upstream's own `vec` is a plain `vector<double>` sorted with no
/// comparator, `std::less<double>{}` implicit in it.
fn sort_rustflow(data: Vec<f64>, executor: &Executor) {
    let mut taskflow = Taskflow::new();
    let data = std::sync::Arc::new(std::sync::Mutex::new(data));
    taskflow.sort_by(data, f64::total_cmp);
    executor.run(&taskflow).wait().expect("no task panicked");
}

/// The same sort as a rayon parallel slice sort, in the role TBB's
/// `parallel_sort` plays upstream.
fn sort_rayon(mut data: Vec<f64>, pool: &rayon::ThreadPool) {
    pool.install(|| data.par_sort_unstable_by(f64::total_cmp));
}

/// Registers both variants at every size and worker count.
fn bench_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in worker_counts() {
        let executor = Executor::new(workers);
        // rayon's answer to TBB's `global_control(max_allowed_parallelism)`.
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .expect("a rayon pool of the requested size");

        for size in SIZES {
            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| {
                    b.iter_batched(
                        || shuffled(size),
                        |data| sort_rustflow(black_box(data), &executor),
                        BatchSize::LargeInput,
                    );
                },
            );
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), size), |b| {
                b.iter_batched(
                    || shuffled(size),
                    |data| sort_rayon(black_box(data), &pool),
                    BatchSize::LargeInput,
                );
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_sort);
criterion_main!(benches);

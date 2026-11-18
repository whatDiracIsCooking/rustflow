//! Translated from `benchmarks/embarrassing_parallelism/` (Taskflow v4.1.0).
//!
//! `N` independent tasks, no edges at all, each doing a fixed amount of
//! floating-point work. This is the shape a scheduler should be best at and
//! the one where a work-stealing queue earns its keep, so it is also the
//! fairest comparison against rayon: `(0..n).into_par_iter().for_each(..)` is
//! exactly this program, and it is what anyone would actually write.
//!
//! The asymmetry to keep in mind is the one `binary_tree.rs` describes: our
//! measured region builds `N` nodes first, because upstream's measured region
//! does, while rayon's parallel iterator has no graph to build.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rustflow::Executor;
use rustflow::prelude::*;

#[path = "common/baseline.rs"]
mod baseline;
#[path = "common/body.rs"]
mod body;
mod common;

/// Task counts, two points off upstream's sweep (`1..=1 << 20`, doubling).
const SIZES: [usize; 2] = [1 << 12, 1 << 16];

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// `num_tasks` independent tasks in one taskflow, run once. Upstream's
/// `embarrassing_parallelism_taskflow`.
fn flat_rustflow(num_tasks: u64, executor: &Executor) {
    let taskflow = build_rustflow(num_tasks);
    executor.run(&taskflow).wait().expect("no task panicked");
}

/// The declaration half of [`flat_rustflow`] on its own: build the taskflow,
/// run nothing, drop it.
///
/// Not an upstream benchmark. It is here because the measured region upstream
/// defined -- build *and* run -- is the one thing that makes the rayon column
/// hard to read: a parallel iterator has no graph to declare, so part of what
/// the `rustflow` ids above report is a cost rayon never pays. This says how
/// much of it, and the subtraction is what `BENCHMARKS.md` quotes.
fn build_rustflow(num_tasks: u64) -> Taskflow {
    let mut taskflow = Taskflow::new();
    for i in 0..num_tasks {
        taskflow.emplace(move || {
            black_box(body::dummy(i));
        });
    }
    taskflow
}

/// The same work as a rayon parallel iterator, in the role TBB's flow graph
/// plays upstream.
fn flat_rayon(num_tasks: u64, pool: &rayon::ThreadPool) {
    pool.install(|| {
        (0..num_tasks).into_par_iter().for_each(|i| {
            black_box(body::dummy(i));
        });
    });
}

/// Registers both variants at every size and worker count.
fn bench_embarrassing_parallelism(c: &mut Criterion) {
    let mut group = c.benchmark_group("embarrassing_parallelism");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    // Registered once, outside the worker loop: declaring a taskflow is the
    // calling thread's work whatever size the executor is, so a per-worker id
    // would measure the same thing twice. Criterion times the drop too, which
    // belongs here -- the `rustflow` ids below drop their taskflow inside the
    // measured region as well.
    for size in SIZES {
        let num_tasks = u64::try_from(size).expect("a task count fits a u64");
        group.bench_function(BenchmarkId::new("rustflow-build", size), |b| {
            b.iter(|| build_rustflow(black_box(num_tasks)));
        });
    }

    for workers in common::worker_counts() {
        let executor = common::executor(workers);
        let pool = baseline::rayon_pool(workers);

        for size in SIZES {
            let num_tasks = u64::try_from(size).expect("a task count fits a u64");
            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| {
                    b.iter(|| flat_rustflow(black_box(num_tasks), &executor));
                },
            );
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), size), |b| {
                b.iter(|| flat_rayon(black_box(num_tasks), &pool));
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_embarrassing_parallelism);
criterion_main!(benches);

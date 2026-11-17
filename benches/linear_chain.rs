//! Translated from `benchmarks/linear_chain/` (Taskflow v4.1.0).
//!
//! A chain of `L` tasks, each a dependency of the next, so the graph has no
//! parallelism at all: what it measures is the scheduler's per-task cost on
//! the critical path, and -- at more than one worker -- what the workers that
//! have nothing to do cost the one that does.
//!
//! **No rayon variant.** rayon parallelises over data, and the shapes it
//! offers -- `par_iter`, `join`, `scope` -- all express "these may run at the
//! same time". A chain says the opposite about every pair of tasks, so the
//! only rayon program with this shape is a sequential loop, which measures the
//! loop and not the baseline's scheduler. Upstream has a TBB flow graph to
//! compare against here and we do not; the honest answer is to bench ours
//! alone rather than to dress a `for` loop up as a framework.
//!
//! The measured region is upstream's: `measure_time_taskflow` wraps building
//! the taskflow *and* running it, so the per-task cost reported includes
//! emplacing the node and linking the edge. The executor is built once,
//! outside, as upstream's function-static one is.

use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::available_parallelism;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustflow::Executor;
use rustflow::prelude::*;

/// Chain lengths, two points off upstream's sweep.
///
/// Upstream's `main.cpp` runs `L = 1 << i` for `i` in `1..=25`, which is 25
/// graphs up to 33M tasks -- a shape criterion cannot sample a hundred times.
/// These two are points *on* that sweep rather than sizes of our own: one
/// small enough that the fixed cost of a run still shows, one where the
/// per-task cost dominates it.
const LENGTHS: [usize; 2] = [1 << 10, 1 << 14];

/// Samples per measurement, and the time criterion may spend reaching them.
///
/// Below criterion's defaults (100 samples, 3s warm-up, 5s measurement)
/// because this suite is 4 shapes * 2 sizes * 2 worker counts and the whole
/// run has to fit in one sitting. The binding constraint on this box is noise,
/// not sample count -- see `BENCHMARKS.md`.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// The worker counts every shape is measured at: one, and the machine's own.
///
/// Upstream defaults to `-t 1` and takes the thread count as an option; issue
/// #18 asks for one worker and `available_parallelism`, which are the two ends
/// of that option. They collapse to one measurement on a single-core box.
fn worker_counts() -> Vec<usize> {
    let parallel = available_parallelism().map_or(1, NonZeroUsize::get);
    if parallel > 1 {
        vec![1, parallel]
    } else {
        vec![1]
    }
}

/// Builds a chain of `length` counting tasks and runs it once.
///
/// Upstream's `linear_chain_taskflow`. Its counter is a plain `size_t` captured
/// by reference, which the chain's own serialisation makes safe; a Rust task
/// body is `Send + 'static` and owns what it captures, so the equivalent is a
/// shared relaxed atomic. That is one atomic more per task than upstream pays,
/// and it is the same for every variant measured here.
fn chain(length: usize, executor: &Executor) {
    let counter = Arc::new(AtomicUsize::new(0));

    let mut taskflow = Taskflow::new();
    let tasks: Vec<Task> = (0..length)
        .map(|_| {
            let counter = Arc::clone(&counter);
            taskflow.emplace(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect();
    taskflow.linearize(tasks);

    executor.run(&taskflow).wait().expect("no task panicked");

    // Upstream's `assert(counter == tasks.size())`, kept in a release build
    // rather than compiled out of one. It is the only thing standing between
    // this benchmark and timing a run that scheduled nothing, which is what a
    // sourceless graph would quietly do.
    assert_eq!(
        counter.load(Ordering::Relaxed),
        length,
        "every task in the chain runs exactly once"
    );
}

/// Registers the chain at every length and worker count.
fn bench_linear_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("linear_chain");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in worker_counts() {
        let executor = Executor::new(workers);
        for length in LENGTHS {
            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), length),
                |b| b.iter(|| chain(black_box(length), &executor)),
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_linear_chain);
criterion_main!(benches);

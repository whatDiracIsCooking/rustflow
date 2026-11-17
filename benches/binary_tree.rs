//! Translated from `benchmarks/binary_tree/` (Taskflow v4.1.0).
//!
//! A complete binary tree of `N = 2^layers` slots, where node `i` precedes
//! `2i` and `2i + 1`: one source, then parallelism that doubles with every
//! level until the last one holds half the graph. Where the linear chain is
//! all critical path, this is mostly fan-out.
//!
//! **The rayon variant is a recursive `join`**, which is the shape rayon
//! actually has for a tree -- and the comparison is not symmetric, which is
//! the point to keep in mind when reading the numbers: our run builds a graph
//! of `N - 1` nodes and `N - 2` edges inside the measured region, as upstream
//! measures its own graph construction, while rayon's recursion builds no
//! graph at all and discovers the same tree as it descends. So the rustflow
//! column is "declare the tree, then run it" and the rayon column is "run the
//! tree"; a graph is worth its construction when it is run more than once,
//! which neither of these measures.

use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::available_parallelism;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustflow::Executor;
use rustflow::prelude::*;

/// Tree sizes, as upstream's layer counts: `N = 1 << layers` slots, of which
/// `N - 1` are tasks.
///
/// Upstream sweeps `layers` from 1 to 25; these are two points on that sweep,
/// chosen as in `linear_chain.rs` and for the same reason.
const LAYERS: [u32; 2] = [12, 16];

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

/// Builds the tree as a taskflow and runs it once. Upstream's
/// `binary_tree_taskflow`.
///
/// Node `i` lives at `tasks[i - 1]`: upstream leaves `tasks[0]` an empty
/// `tf::Task` that names no node, and this port has no empty handle -- every
/// [`Task`] names a slot -- so the offset replaces it rather than a
/// placeholder node, which would be a second source task running nothing.
fn tree_rustflow(layers: u32, executor: &Executor) {
    let size = 1usize << layers;
    let counter = Arc::new(AtomicUsize::new(0));

    let mut taskflow = Taskflow::new();
    let tasks: Vec<Task> = (1..size)
        .map(|_| {
            let counter = Arc::clone(&counter);
            taskflow.emplace(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect();

    for i in 1..size {
        let (left, right) = (i << 1, (i << 1) + 1);
        // Upstream's condition, both children or neither. On a power-of-two
        // size it never splits a pair: `left < size` and `right == size` would
        // need an odd size.
        if left < size && right < size {
            taskflow
                .task(tasks[i - 1])
                .precede([tasks[left - 1], tasks[right - 1]]);
        }
    }

    executor.run(&taskflow).wait().expect("no task panicked");

    // Upstream's `assert(counter + 1 == tasks.size())`.
    assert_eq!(
        counter.load(Ordering::Relaxed),
        size - 1,
        "every node of the tree runs exactly once"
    );
}

/// The same tree as a recursive `rayon::join`, in the role TBB's flow graph
/// plays upstream.
fn tree_rayon(layers: u32, pool: &rayon::ThreadPool) {
    let size = 1usize << layers;
    let counter = AtomicUsize::new(0);

    pool.install(|| visit(1, size, &counter));

    assert_eq!(
        counter.load(Ordering::Relaxed),
        size - 1,
        "every node of the tree runs exactly once"
    );
}

/// Counts node `i`, then forks its two children, mirroring the edges
/// [`tree_rustflow`] declares.
fn visit(i: usize, size: usize, counter: &AtomicUsize) {
    counter.fetch_add(1, Ordering::Relaxed);

    let (left, right) = (i << 1, (i << 1) + 1);
    if left < size && right < size {
        rayon::join(
            || visit(left, size, counter),
            || visit(right, size, counter),
        );
    }
}

/// Registers both variants at every size and worker count.
fn bench_binary_tree(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_tree");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in worker_counts() {
        let executor = Executor::new(workers);
        // rayon's answer to TBB's `global_control(max_allowed_parallelism)`:
        // a pool of exactly this many threads, so both variants are measured
        // with the same hardware to work with.
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .expect("a rayon pool of the requested size");

        for layers in LAYERS {
            let size = 1usize << layers;
            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| {
                    b.iter(|| tree_rustflow(black_box(layers), &executor));
                },
            );
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), size), |b| {
                b.iter(|| tree_rayon(black_box(layers), &pool));
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_binary_tree);
criterion_main!(benches);

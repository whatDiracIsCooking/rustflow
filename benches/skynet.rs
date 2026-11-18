//! Translated from `benchmarks/skynet/` (Taskflow v4.1.0).
//!
//! The skynet microbenchmark: a ten-way tree of depth `MaxDepth` whose leaves
//! are the integers `0..10^MaxDepth`, summed back up the tree. Upstream's
//! `skynet_one_tf` spawns ten children per level into a `tf::TaskGroup` with
//! `silent_async`, `corun`s for them, and adds up the ten results; the root is
//! one `executor.async(...)` whose handle is waited, because a group may only
//! be opened by a worker. Both are translated as they stand.
//!
//! Two ids:
//!
//! - **`rustflow-group`** is upstream's `skynet/taskflow.cpp`: a
//!   [`TaskGroup`](rustflow::TaskGroup) per level, ten `silent_async`
//!   submissions into it, one `corun` to join, and the sum of the ten slots.
//!   The id carries `group` rather than being plain `rustflow` because the
//!   submission path is the thing this shape measures, and it is the same
//!   path `fibonacci.rs`'s id of that name takes.
//! - **`rayon`** is `rayon::scope` plus ten `spawn`s per level, which is the
//!   form upstream's own non-Taskflow column has: `skynet/tbb.cpp` runs a
//!   `tbb::task_group`, `tg.run` ten times, then `tg.wait`.
//!
//! There is no graph here, so the construction asymmetry
//! `binary_tree.rs` and `embarrassing_parallelism.rs` warn about is absent:
//! both ids measure "run the recursion" and nothing else.
//!
//! # What this shape is here to measure
//!
//! A parent that submits ten children and coruns is a **preempted parent**,
//! and what the shape costs is dominated by what happens to it when its last
//! child finishes: it goes back on a queue rather than being resumed on the
//! spot, which is the half of upstream's locality optimisation
//! (`WorkerThread::finish_job`) this port has not taken. `BENCHMARKS.md`'s
//! `Work` section names that as unmeasured -- "the shapes that would [move]
//! are the nested ones, and nothing here measures those yet" -- and this
//! target is one of the five that do. A chain or a flat submission round has
//! no preempted parent in it at all; here every one of the
//! `(10^MaxDepth - 1) / 9` interior nodes is one.
//!
//! # The depths are smaller than upstream's sweep, and the stack is why
//!
//! Upstream sweeps `MaxDepth = 1..=8`, a hundred million leaves at the top.
//! The sizes below stop at 4, and the limit is not the time a round takes but
//! a **stack overflow** -- the one `fibonacci.rs` documents for this same
//! `rustflow-group` shape. A corun keeps its worker in the work-stealing
//! loop, so every job that worker picks up while waiting nests one frame
//! deeper; and a [`TaskGroup`](rustflow::TaskGroup)'s children go into the
//! shared buffers rather than onto the submitting worker's queue, so a corun
//! is handed the oldest outstanding job in the pool -- somebody else's
//! unfinished subtree -- instead of the child it is waiting for, and nests
//! one frame per outstanding *child* rather than one per level of recursion.
//! `src/task_group.rs`'s module docs have that argument in full.
//!
//! Measured in a release build rather than assumed: at one worker depth 4
//! passes and depth 5 aborts, and at 24 workers depth 5 passes and depth 6
//! aborts. One worker is the binding end, for the reason `fibonacci.rs`
//! gives -- the one worker that coruns is the only worker there is to pick
//! the outstanding children up -- so depth 4, a factor of ten in task count
//! inside it, is what this id can carry. **Neither size is chosen for
//! speed**, and if the cliff moves these move with it.
//!
//! # Departures from upstream
//!
//! - **The ten slots are an `Arc<[AtomicU64; 10]>`**, where upstream's child
//!   writes `results[i]`, an element of a `std::array` on the frame that
//!   spawned it. A body here is `Send + 'static` and cannot borrow a frame,
//!   so the array is shared by refcount and its elements are atomics. That is
//!   one allocation and ten refcount pairs per interior node, which upstream
//!   does not pay; the `rayon` id does not pay it either, since a scope's
//!   closures may borrow the frame the scope is on. `Relaxed` on both ends is
//!   sound for the reason the slots exist: what orders a child's store
//!   against the parent's load is the `corun` between them.
//! - **The executor arrives as an `Arc`**, because a child opens a group of
//!   its own and needs the executor to open it, from a `'static` closure.
//!   Upstream captures `&exe`.
//! - **The sum is checked outside the measured region**, against
//!   `n * (n - 1) / 2` for `n = 10^MaxDepth`: the leaves are exactly the
//!   integers below `n`, each counted once, so the shape has a closed-form
//!   answer. Upstream checks nothing -- `skynet(executor, MaxDepth)` discards
//!   `skynet_one_tf`'s return value -- which would let a recursion that lost
//!   a subtree benchmark as a fast one.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustflow::Executor;

#[path = "common/baseline.rs"]
mod baseline;
mod common;

/// Children per level, which is what makes this shape skynet rather than a
/// binary recursion: upstream's `for (size_t i = 0; i < 10; ++i)`.
const CHILDREN: usize = 10;

/// The two depths this shape is measured at, two points off upstream's
/// `MaxDepth = 1..=8` sweep.
///
/// 1110 spawned tasks and 11110, a decade apart: the smaller is where the
/// fixed cost of a round -- the root submission and its wait -- still shows
/// against the tree, and the larger is where the per-task cost dominates it,
/// which is the choice `BENCHMARKS.md`'s "What is measured" asks every table
/// to make. The larger is also the deepest this id reaches at one worker, for
/// the reason the module header gives.
const SIZES: [u32; 2] = [3, 4];

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// The sum the tree must produce: the leaves are the integers below
/// `10^max_depth`, so the answer is `n * (n - 1) / 2`.
///
/// Upstream has no such reference; see the module header on why there is one
/// here.
fn skynet_answer(max_depth: u32) -> u64 {
    let leaves = 10u64.pow(max_depth);
    leaves * (leaves - 1) / 2
}

/// Upstream's `skynet_one_tf(exe, BaseNum, Depth, MaxDepth)`: ten children
/// into a group of this level's own, a `corun` to join them, and the sum of
/// the ten slots.
///
/// `offset` is upstream's `depthOffset`, the span of leaf indices one child
/// owns, computed as a power rather than by its multiplying loop.
fn skynet_group(base: u64, depth: u32, max_depth: u32, executor: &Arc<Executor>) -> u64 {
    if depth == max_depth {
        return base;
    }

    let offset = 10u64.pow(max_depth - depth - 1);
    let group = executor
        .task_group()
        .expect("the recursion runs on a worker, which may open a group");
    let results: Arc<[AtomicU64; CHILDREN]> = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));

    for i in 0..CHILDREN {
        let index = u64::try_from(i).expect("a child index below ten fits in a u64");
        group.silent_async({
            let executor = Arc::clone(executor);
            let results = Arc::clone(&results);
            move || {
                let sum = skynet_group(base + offset * index, depth + 1, max_depth, &executor);
                results[i].store(sum, Ordering::Relaxed);
            }
        });
    }

    group.corun();

    results
        .iter()
        .map(|slot| slot.load(Ordering::Relaxed))
        .sum()
}

/// The same recursion as a `rayon::scope` of ten `spawn`s, in the role
/// upstream's `tbb::task_group` plays.
///
/// The slots are a plain `[u64; 10]` split into ten disjoint `&mut`, which is
/// the surcharge the module header says this id does not pay.
fn skynet_rayon(base: u64, depth: u32, max_depth: u32) -> u64 {
    if depth == max_depth {
        return base;
    }

    let offset = 10u64.pow(max_depth - depth - 1);
    let mut results = [0u64; CHILDREN];

    rayon::scope(|scope| {
        for (i, slot) in results.iter_mut().enumerate() {
            let index = u64::try_from(i).expect("a child index below ten fits in a u64");
            scope.spawn(move |_| {
                *slot = skynet_rayon(base + offset * index, depth + 1, max_depth);
            });
        }
    });

    results.iter().sum()
}

/// Upstream's `skynet(executor, MaxDepth)` around [`skynet_group`]: the
/// recursion has to start on a worker, so the whole of it runs inside one
/// `spawn` whose handle is waited.
fn group_on(max_depth: u32, executor: &Arc<Executor>) -> u64 {
    executor
        .spawn({
            let executor = Arc::clone(executor);
            move || skynet_group(0, 0, max_depth, &executor)
        })
        .wait()
        .expect("nothing under the recursion panicked")
}

/// The rayon wrapper. `install` is what holds the recursion to the worker
/// count this row is measuring, as in every other shape here.
fn rayon_on(max_depth: u32, pool: &rayon::ThreadPool) -> u64 {
    pool.install(|| skynet_rayon(0, 0, max_depth))
}

/// Registers both ids at every depth and worker count.
fn bench_skynet(c: &mut Criterion) {
    let mut group = c.benchmark_group("skynet");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in common::worker_counts() {
        // An `Arc` because the recursion reaches the executor from a
        // `'static` closure.
        let executor = Arc::new(common::executor(workers));
        let pool = baseline::rayon_pool(workers);

        for max_depth in SIZES {
            let answer = skynet_answer(max_depth);

            // One untimed run of each id, checked against the closed-form
            // answer before criterion times anything: a recursion that lost a
            // subtree then fails here rather than reporting a good number.
            assert_eq!(
                group_on(max_depth, &executor),
                answer,
                "skynet({max_depth})"
            );
            assert_eq!(rayon_on(max_depth, &pool), answer, "skynet({max_depth})");

            group.bench_function(
                BenchmarkId::new(format!("rustflow-group/{workers}w"), max_depth),
                |b| b.iter(|| black_box(group_on(black_box(max_depth), &executor))),
            );
            group.bench_function(
                BenchmarkId::new(format!("rayon/{workers}w"), max_depth),
                |b| b.iter(|| black_box(rayon_on(black_box(max_depth), &pool))),
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_skynet);
criterion_main!(benches);

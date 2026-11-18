//! Translated from `benchmarks/async_task/` (Taskflow v4.1.0).
//!
//! `N` independent asynchronous submissions and one wait: no graph, no edges
//! and no handles -- upstream's `async_task_taskflow` calls `silent_async`
//! `N` times and then `executor.wait_for_all()`, which is the only thing
//! waiting for a body nobody holds a handle on. The task body is upstream's
//! `func`, a single relaxed increment, so what is measured is the submission
//! path and nothing else.
//!
//! **The rayon baseline is `rayon::scope` plus `spawn`**, which is the shape
//! upstream's own non-Taskflow column has: its `tbb.cpp` runs a
//! `tbb::task_group` -- `tg.run` `N` times, then `tg.wait` -- and a scope is
//! rayon's form for "submit one task at a time, join them all at the end".
//! `par_iter` would be the wrong baseline here even though it is faster: it
//! splits a range, so it submits a handful of jobs rather than `N` of them,
//! and `embarrassing_parallelism.rs` already measures that comparison.
//!
//! # The two `rustflow` ids are two submission paths, and that is the point
//!
//! `Executor::silent_async` called from the benchmark's own thread is the
//! **external** submission path: a thread that is not a worker cannot push
//! onto a worker's queue, so every job goes into the shared overflow buffers
//! and is distributed by stealing. That is the shape `BENCHMARKS.md`'s "#60
//! finding" section took apart -- the crate's one measurement of this path --
//! and this benchmark is the whole of that path with no task body in front of
//! it, so the per-task figures below are the ones to read against it rather
//! than to re-derive. What the `rustflow` id's own ~415 ns a task goes on is
//! the "#125 attribution" section of that file: 87% of it is the three heap
//! blocks a free async task is, allocated by this thread and freed by a
//! worker, and the counters and the notifier together are under 5%.
//!
//! rayon's scope does *not* measure that: `ThreadPool::install` makes the
//! calling thread a worker of the pool for the length of the closure, so its
//! `N` spawns go onto that worker's own LIFO deque. Without `install` the
//! scope would run on rayon's global pool and ignore the worker count this
//! row is measuring, so there is no version of the baseline that submits from
//! outside. The `rustflow-group` id is the comparison that closes the gap: a
//! [`TaskGroup`](rustflow::TaskGroup) opened inside one `spawn`ed body
//! submits the same `N` tasks *from a worker*, onto its own queue, and
//! `corun`s for them. It is upstream's TBB variant translated literally, and
//! what it costs against the external id is what the buffers cost.
//!
//! Both `rustflow` ids pay one thing upstream does not: a task body here is
//! `Send + 'static`, so the shared counter is an `Arc<AtomicUsize>` and every
//! submission clones it, where upstream's `[&]` captures a reference to the
//! caller's frame. That is one refcount pair per task, the same surcharge
//! `linear_chain.rs` documents, and the rayon id does not pay it -- a scope's
//! closures may borrow the frame the scope is on.

use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::available_parallelism;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustflow::Executor;

/// Task counts, two points off upstream's sweep (`S = 1..=2097152`, doubling).
///
/// The same two points the other shapes take, for the reason
/// `linear_chain.rs` gives: criterion samples one size a hundredfold rather
/// than sweeping, so each table takes a size where the fixed cost of a
/// submission round still shows and one where the per-task cost dominates it.
const SIZES: [usize; 2] = [1 << 12, 1 << 16];

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

/// Upstream's `func`: one relaxed increment, so that the measured region is
/// the submission and not the work.
fn func(counter: &AtomicUsize) {
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Upstream's `async_task_taskflow`: `num_tasks` silent submissions from the
/// calling thread, then `wait_for_all`.
fn async_rustflow(num_tasks: usize, executor: &Executor) {
    let counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..num_tasks {
        let counter = Arc::clone(&counter);
        executor.silent_async(move || func(&counter));
    }

    executor.wait_for_all();

    // Upstream's `if(counter != num_tasks) throw`, kept in a release build
    // rather than compiled out of one: it is the only thing standing between
    // this benchmark and timing a wait that waited for nothing.
    assert_eq!(
        counter.load(Ordering::Relaxed),
        num_tasks,
        "every submitted body runs exactly once"
    );
}

/// The same `num_tasks` submissions made from a *worker*, into a task group:
/// upstream's `async_task_tbb` translated literally, with a
/// [`TaskGroup`](rustflow::TaskGroup) where it has a `tbb::task_group`.
///
/// Not one of upstream's Taskflow variants, and the module header says what
/// it is here for: it is the same `N` tasks down the worker-aware path, so the
/// difference between this and [`async_rustflow`] is what submitting from
/// outside the pool costs. The wrapping `spawn` is what gets the submissions
/// onto a worker at all -- a group may only be opened by one -- and is one
/// task against `num_tasks`, so the counter still answers `num_tasks`.
fn group_rustflow(num_tasks: usize, executor: &Arc<Executor>) {
    let counter = Arc::new(AtomicUsize::new(0));

    let submit = executor.spawn({
        let executor = Arc::clone(executor);
        let counter = Arc::clone(&counter);
        move || {
            let group = executor
                .task_group()
                .expect("a spawned body runs on a worker, which may open a group");

            for _ in 0..num_tasks {
                let counter = Arc::clone(&counter);
                group.silent_async(move || func(&counter));
            }

            group.corun();
        }
    });
    submit.wait().expect("no task panicked");

    assert_eq!(
        counter.load(Ordering::Relaxed),
        num_tasks,
        "every submitted body runs exactly once"
    );
}

/// The same `num_tasks` tasks as a rayon scope, in the role TBB's task group
/// plays upstream.
///
/// The counter is a plain [`AtomicUsize`] on this frame rather than an
/// [`Arc`]: a scope's spawns may borrow it, which is the asymmetry the module
/// header names.
fn async_rayon(num_tasks: usize, pool: &rayon::ThreadPool) {
    let counter = AtomicUsize::new(0);

    pool.install(|| {
        rayon::scope(|scope| {
            for _ in 0..num_tasks {
                scope.spawn(|_| func(&counter));
            }
        });
    });

    assert_eq!(
        counter.load(Ordering::Relaxed),
        num_tasks,
        "every spawned body runs exactly once"
    );
}

/// Registers all three variants at every size and worker count.
fn bench_async_task(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_task");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in worker_counts() {
        // An `Arc` because `group_rustflow`'s body has to reach the executor
        // from a `'static` closure; the other id borrows it through the same
        // handle, so both measure one pool.
        let executor = Arc::new(Executor::new(workers));
        // rayon's answer to TBB's `global_control(max_allowed_parallelism)`.
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .expect("a rayon pool of the requested size");

        for size in SIZES {
            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| b.iter(|| async_rustflow(black_box(size), &executor)),
            );
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), size), |b| {
                b.iter(|| async_rayon(black_box(size), &pool));
            });
            group.bench_function(
                BenchmarkId::new(format!("rustflow-group/{workers}w"), size),
                |b| b.iter(|| group_rustflow(black_box(size), &executor)),
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_async_task);
criterion_main!(benches);

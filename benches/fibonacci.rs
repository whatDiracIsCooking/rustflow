//! Translated from `benchmarks/fibonacci/` (Taskflow v4.1.0).
//!
//! The naive recursion, parallelised the way a work-stealing scheduler is
//! supposed to make easy: one branch spawned, the other computed on the
//! calling worker, and a wait that keeps that worker in the pool instead of
//! parking it. Upstream's `fibonacci_taskflow` wraps the whole recursion in
//! one `executor.async(...)` and reads the handle, because the recursion has
//! to start on a worker; so does each `rustflow` id here.
//!
//! Four variants, three of which are ours to choose between:
//!
//! - **`rustflow-group`** is upstream's `fibonacci/taskflow.cpp` line for
//!   line: a [`TaskGroup`](rustflow::TaskGroup) per level, one
//!   `silent_async` into it, the `n - 2` branch inline -- upstream's "tail
//!   optimization" -- and `corun` to join.
//! - **`rustflow-runtime`** is the same recursion through a runtime task,
//!   which is upstream's `unittests/test_runtimes.cpp` `Runtime.Fibonacci`
//!   rather than its benchmark: `rt.silent_async` with a runtime-shaped body,
//!   which this port spells
//!   [`silent_async_runtime`](rustflow::Runtime::silent_async_runtime), and
//!   `rt.corun`. It needs no executor handle and no group, since the node the
//!   children hang off is the runtime task's own.
//! - **`rustflow-runtime-group`** is the first of those two crossed with the
//!   second: upstream's group per level again, but opened on the
//!   [`Runtime`](rustflow::Runtime) running that level rather than on the
//!   executor, which is the route #372 added. The child goes onto that
//!   worker's own queue and the `corun` pops that same end, where
//!   `rustflow-group` leaves it in the shared buffers and coruns on a
//!   stealing view of the queue -- and that, bar the ordering below, is the
//!   only difference between the two bodies. #380 measures the pair.
//! - **`rayon`** is `rayon::join`, which is the shape rayon has for exactly
//!   this and is what anyone would write: a fork of two closures, the second
//!   of which usually runs on the forking thread.
//!
//! **`rustflow-runtime-group` takes upstream's `FibonacciSwapped` ordering**,
//! which is the one thing the route forces: the group borrows the runtime for
//! as long as it is open, so the `n - 2` branch -- which opens a group of its
//! own -- is the one that runs *before* it rather than inside it. Upstream
//! writes both orderings -- `unittests/test_task_group.cpp` has `Fibonacci`
//! and `FibonacciSwapped`, and `tests/task_group.rs` ports both -- so this is
//! upstream's own line rather than a liberty; what it costs the column is
//! that a level submits its child only once the inline branch is done, where
//! the other three expose both branches at once.
//!
//! Unlike every other shape in this suite there is **no graph to build**, so
//! the asymmetry `binary_tree.rs` and `embarrassing_parallelism.rs` have to
//! warn about is absent here: all four ids measure "run the recursion" and
//! nothing else. What they do not share is how much of the recursion is
//! scheduled at all -- `rayon::join` runs the second closure inline when
//! nobody steals the first, so a `join` that nobody steals from is direct
//! recursion with one atomic, while a group or a runtime task submits a job
//! per level whatever the pool is doing.
//!
//! # `N` is smaller than upstream's, and `rustflow-group` is why (#119)
//!
//! Upstream sweeps `f = 1..=40` and its own unit test asserts `fib(25)`. The
//! sizes below are smaller, and the limit is not the time a round takes but a
//! **stack overflow** -- the nested-corun stack bound, which `DESIGN.md`'s
//! "Executor core" states once for the port and `BENCHMARKS.md`'s "The #359
//! measurement" measures: a group's corun nests one frame per outstanding
//! *task* rather than one per level of recursion.
//!
//! #119 took that cost off the **runtime** variant: a
//! [`Runtime`](rustflow::Runtime) hands its own worker to the submission,
//! which is upstream's `if(auto w = this_worker(); w)` branch of
//! `_schedule_async_task`, so the child goes onto that worker's LIFO queue
//! and the corun pops it straight back. `rustflow-runtime` reaches `fib(40)`
//! at one worker and at twenty-four in a release build -- the top of
//! upstream's own sweep -- and `tests/runtimes.rs` now asserts upstream's
//! `fib(25)`.
//!
//! `rustflow-group` keeps the old ceiling, and since every id measures the
//! same `n` it is the one that sets it -- `rustflow-runtime-group` nests a
//! frame per level like the runtime id, for
//! [`Runtime::task_group`](rustflow::Runtime::task_group)'s reason, and is
//! held to these sizes only so that a row compares the four columns. A
//! [`TaskGroup`](rustflow::TaskGroup) holds a worker *id* rather than the
//! worker, and its `corun` waits on the stealing end of the queue in any
//! case, so its children stay in the shared FIFO buffers;
//! [`TaskGroup::corun`](rustflow::TaskGroup::corun)'s own docs state the bound
//! that follows. Measured in this profile rather than assumed, because the
//! debug figure is not it: at one worker `fib(17)` passes and `fib(18)`
//! aborts, and at 24 workers `fib(23)` passes and `fib(24)` aborts. One
//! worker is the binding end -- the one worker that coruns is the only worker
//! there is to pick the outstanding children up -- so `fib(15)`, three levels
//! inside it, is what the group id can carry. `BENCHMARKS.md`'s `fibonacci`
//! section and its "#119 measurement" have both, and "The #359 measurement"
//! has the bound itself. **Neither size is chosen for speed**, and if the
//! cliff moves, these move with it.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustflow::{Executor, Runtime};

#[path = "common/baseline.rs"]
mod baseline;
mod common;

/// The two `n` this shape is measured at, both inside the stack-depth cliff
/// the module header describes -- which since #119 is `rustflow-group`'s
/// alone, and at **one** worker, the binding end, sits between `fib(17)` and
/// `fib(18)`. Neither runtime-route id is bound there -- `rustflow-runtime`
/// would carry upstream's `fib(25)` and its whole sweep to `fib(40)` -- but
/// every id shares these sizes so that a row of the table compares them.
const SIZES: [u64; 2] = [12, 15];

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// The answer, iteratively, so that every variant's result can be checked.
///
/// Upstream carries the sequence as a table in `fibonacci.hpp` and its
/// benchmark checks nothing against it; this is the same guard the other
/// shapes' counters are -- the one thing standing between the benchmark and
/// timing a recursion that returned early.
fn fibonacci_reference(n: u64) -> u64 {
    let mut pair = (0u64, 1u64);
    for _ in 0..n {
        pair = (pair.1, pair.0 + pair.1);
    }
    pair.0
}

/// Upstream's `fibonacci(size_t n, tf::Executor& executor)`: the `n - 1`
/// branch into a group of this level's own, the `n - 2` branch on this very
/// worker, and a `corun` to join them.
///
/// Two adaptations, both the port's shapes rather than the algorithm's.
/// Upstream's child writes `res1`, a local of the frame that spawned it; a
/// body here is `'static` and cannot borrow a frame, so the slot is an
/// `Arc<AtomicU64>`. And the executor arrives as an `Arc` because the child
/// opens a group of its own and a group needs the executor to open it --
/// which is what `tests/task_group.rs`'s port of the same recursion does.
/// `Relaxed` on both ends of the slot is sound for the reason it is there:
/// what orders the child's store against this frame's load is the `corun`.
fn fibonacci_group(n: u64, executor: &Arc<Executor>) -> u64 {
    if n < 2 {
        return n;
    }

    let group = executor
        .task_group()
        .expect("the recursion runs on a worker, which may open a group");
    let first = Arc::new(AtomicU64::new(0));

    group.silent_async({
        let executor = Arc::clone(executor);
        let first = Arc::clone(&first);
        move || first.store(fibonacci_group(n - 1, &executor), Ordering::Relaxed)
    });

    let second = fibonacci_group(n - 2, executor);

    group.corun();

    first.load(Ordering::Relaxed) + second
}

/// The same recursion under a runtime task, which is upstream's
/// `Runtime.Fibonacci`: `rt.silent_async` with a runtime-shaped body, so that
/// the child can spawn children of its own, and `rt.corun` to wait for
/// everything left outstanding under this node.
///
/// Note what the tail optimisation means here: the `n - 2` branch spawns its
/// children under *this* node too, so the corun waits for those as well.
fn fibonacci_runtime(n: u64, runtime: &mut Runtime<'_>) -> u64 {
    if n < 2 {
        return n;
    }

    let first = Arc::new(AtomicU64::new(0));
    runtime.silent_async_runtime({
        let first = Arc::clone(&first);
        move |child: &mut Runtime<'_>| {
            first.store(fibonacci_runtime(n - 1, child), Ordering::Relaxed);
        }
    });

    let second = fibonacci_runtime(n - 2, runtime);

    runtime.corun();

    first.load(Ordering::Relaxed) + second
}

/// The same recursion as [`fibonacci_group`] with the group opened on the
/// [`Runtime`] running this level, so that the child goes onto this worker's
/// own queue and the `corun` pops it straight back.
///
/// Upstream's `fibonacci_swapped` ordering, for the reason the module header
/// gives: the group borrows the runtime for as long as it is open, so the
/// branch computed on this worker is the one that runs before it. The child
/// is a runtime-shaped body because it opens a group of its own, which is
/// also what replaces the `Arc<Executor>` [`fibonacci_group`] threads through
/// the recursion.
fn fibonacci_runtime_group(n: u64, runtime: &mut Runtime<'_>) -> u64 {
    if n < 2 {
        return n;
    }

    let second = fibonacci_runtime_group(n - 2, runtime);
    let first = Arc::new(AtomicU64::new(0));

    let group = runtime.task_group();
    group.silent_async_runtime({
        let first = Arc::clone(&first);
        move |child: &mut Runtime<'_>| {
            first.store(fibonacci_runtime_group(n - 1, child), Ordering::Relaxed);
        }
    });

    group.corun();

    first.load(Ordering::Relaxed) + second
}

/// The same recursion as `rayon::join`, in the role TBB's task group plays
/// upstream.
fn fibonacci_rayon(n: u64) -> u64 {
    if n < 2 {
        return n;
    }

    let (first, second) = rayon::join(|| fibonacci_rayon(n - 1), || fibonacci_rayon(n - 2));

    first + second
}

/// Upstream's `fibonacci_taskflow` around [`fibonacci_group`]: the recursion
/// has to start on a worker, so the whole of it runs inside one `spawn`.
fn group_on(n: u64, executor: &Arc<Executor>) {
    let result = executor
        .spawn({
            let executor = Arc::clone(executor);
            move || fibonacci_group(n, &executor)
        })
        .wait()
        .expect("nothing under the recursion panicked");

    assert_eq!(result, fibonacci_reference(n), "fib({n})");
}

/// The same wrapper for [`fibonacci_runtime`], which needs a runtime-shaped
/// body rather than a plain one: upstream's
/// `executor.async([&](tf::Runtime& rt){...})`.
fn runtime_on(n: u64, executor: &Executor) {
    let result = Arc::new(AtomicU64::new(0));

    let task = executor.spawn_runtime({
        let result = Arc::clone(&result);
        move |runtime: &mut Runtime<'_>| {
            result.store(fibonacci_runtime(n, runtime), Ordering::Relaxed);
        }
    });
    task.wait().expect("nothing under the recursion panicked");

    assert_eq!(
        result.load(Ordering::Relaxed),
        fibonacci_reference(n),
        "fib({n})"
    );
}

/// [`runtime_on`] for the runtime-route group: the same runtime-shaped root
/// body, since the recursion it starts needs a [`Runtime`] rather than an
/// executor.
fn runtime_group_on(n: u64, executor: &Executor) {
    let result = Arc::new(AtomicU64::new(0));

    let task = executor.spawn_runtime({
        let result = Arc::clone(&result);
        move |runtime: &mut Runtime<'_>| {
            result.store(fibonacci_runtime_group(n, runtime), Ordering::Relaxed);
        }
    });
    task.wait().expect("nothing under the recursion panicked");

    assert_eq!(
        result.load(Ordering::Relaxed),
        fibonacci_reference(n),
        "fib({n})"
    );
}

/// The rayon wrapper. `install` is what holds the recursion to the worker
/// count this row is measuring, as in every other shape here.
fn rayon_on(n: u64, pool: &rayon::ThreadPool) {
    let result = pool.install(|| fibonacci_rayon(n));

    assert_eq!(result, fibonacci_reference(n), "fib({n})");
}

/// Registers all four variants at every size and worker count.
fn bench_fibonacci(c: &mut Criterion) {
    let mut group = c.benchmark_group("fibonacci");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in common::worker_counts() {
        // An `Arc` because the group recursion reaches the executor from a
        // `'static` closure; the runtime id borrows it through the same
        // handle, so both measure one pool.
        let executor = Arc::new(common::executor(workers));
        let pool = baseline::rayon_pool(workers);

        for n in SIZES {
            group.bench_function(
                BenchmarkId::new(format!("rustflow-group/{workers}w"), n),
                |b| b.iter(|| group_on(black_box(n), &executor)),
            );
            group.bench_function(
                BenchmarkId::new(format!("rustflow-runtime/{workers}w"), n),
                |b| b.iter(|| runtime_on(black_box(n), &executor)),
            );
            group.bench_function(
                BenchmarkId::new(format!("rustflow-runtime-group/{workers}w"), n),
                |b| b.iter(|| runtime_group_on(black_box(n), &executor)),
            );
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), n), |b| {
                b.iter(|| rayon_on(black_box(n), &pool));
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_fibonacci);
criterion_main!(benches);

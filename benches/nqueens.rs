//! Translated from `benchmarks/nqueens/` (Taskflow v4.1.0).
//!
//! The classic backtracking count: place one queen per column, prune the
//! placements that attack an earlier one, and count the complete boards.
//! Upstream's `nqueens(j, a, executor)` opens a `tf::TaskGroup` per level,
//! `silent_async`s one child per legal placement in column `j`, gathers each
//! child's count through a per-child slot (`parts`), `corun`s, and returns
//! the sum; the root is one `executor.async(...)` whose handle is read,
//! because a group may only be opened by a worker. Both are translated as
//! they stand.
//!
//! Two ids:
//!
//! - **`rustflow-group`** is upstream's `nqueens/taskflow.cpp`: a
//!   [`TaskGroup`](rustflow::TaskGroup) per level, one `silent_async` per
//!   legal placement, one `corun` to join, and the sum of the slots. The id
//!   carries `group` rather than being plain `rustflow` because the
//!   submission path is the thing this shape measures, and it is the same
//!   path `fibonacci.rs`'s id of that name takes.
//! - **`rayon`** is `rayon::scope` plus one `spawn` per legal placement,
//!   which is the form upstream's own non-Taskflow column has:
//!   `nqueens/tbb.cpp` runs a `tbb::task_group`, `g.run` per placement, then
//!   `g.wait`.
//!
//! There is no graph here, so the construction asymmetry `binary_tree.rs` and
//! `embarrassing_parallelism.rs` warn about is absent: both ids measure "run
//! the recursion" and nothing else. Unlike `skynet`, the tree is **ragged**
//! -- a level spawns between zero and `N` children depending on what the
//! pruning leaves -- so the per-level fan-out is data, not a constant, and a
//! level near the bottom of the board typically spawns one or two children
//! where the root spawns `N`.
//!
//! # What this shape is here to measure
//!
//! A parent that submits its children and coruns is a **preempted parent**,
//! and what the shape costs is dominated by what happens to it when its last
//! child finishes: it goes back on a queue rather than being resumed on the
//! spot, which is the half of upstream's locality optimisation
//! (`WorkerThread::finish_job`) this port has not taken. `BENCHMARKS.md`'s
//! `Work` section names that as unmeasured -- "the shapes that would [move]
//! are the nested ones, and nothing here measures those yet" -- and this
//! target is one of the five that do. What it adds over `skynet` is that its
//! fan-out varies: most of its interior nodes wait on a handful of children
//! rather than on ten, so the re-queue is a larger share of each parent's
//! cost.
//!
//! # The boards are smaller than upstream's sweep, and the stack is why
//!
//! Upstream's `main.cpp` sweeps `q = 1..=14` by default. The sizes below stop
//! at 8, and the limit is not the time a round takes but a **stack
//! overflow**: the nested-corun stack bound, which `DESIGN.md`'s "Executor
//! core" states once for the port and
//! [`TaskGroup::corun`](rustflow::TaskGroup::corun)'s docs name for a caller.
//!
//! Measured in a release build rather than assumed: at one worker `q = 8`
//! passes and `q = 9` aborts, and at 24 workers `q = 10` passes and
//! `q = 11` aborts. One worker is the binding end -- the one worker that
//! coruns is the only worker there is to pick the outstanding children up,
//! and `BENCHMARKS.md`'s "The #359 measurement" counts what that costs here
//! -- so `q = 8`, a factor of four in task count inside it, is what this id
//! can carry, which is the margin `fibonacci.rs`'s own sizes keep. **Neither
//! size is chosen for speed**, and if the cliff moves these move with it.
//!
//! # Departures from upstream
//!
//! - **A child owns its board.** Upstream builds an `N`-by-`N` `buf` per
//!   level and hands child `i` a reference to `buf[i]`, which lives on the
//!   parent's frame. A body here is `Send + 'static` and cannot borrow a
//!   frame, so each child takes an owned `Vec<u8>` of the parent's board with
//!   its own placement appended. That is the same `N` boards of `N` bytes per
//!   level upstream allocates, one `Vec` at a time rather than a `Vec` of
//!   `Vec`s, and a board is built for a placement upstream rejects too --
//!   `queens_ok` has to read it to reject it.
//! - **The slots are an `Arc<Vec<AtomicU64>>`**, for the same reason: a
//!   parent's `parts` cannot be borrowed by a `'static` child. `Relaxed` on
//!   both ends is sound for the reason the slots exist -- what orders a
//!   child's store against the parent's load is the `corun` between them --
//!   and a slot a rejected placement never fills keeps the zero upstream
//!   writes into it explicitly.
//! - **The executor arrives as an `Arc`**, because a child opens a group of
//!   its own and needs the executor to open it, from a `'static` closure.
//!   Upstream captures `&executor`.
//! - **`queens_ok` counts in `i32`.** Upstream's board is `char` and its
//!   diagonal test is `q == p - (j - i)`, which it is happy to let go
//!   negative. A `u8` would wrap there, so the comparison is widened rather
//!   than the board.
//! - **The count is checked outside the measured region**, against upstream's
//!   own `answers` table. Upstream checks it too, after the clock stops; the
//!   difference is only that the check here runs once per id before criterion
//!   times the id a hundred times.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustflow::Executor;

#[path = "common/baseline.rs"]
mod baseline;
mod common;

/// The two board sizes this shape is measured at, two points off upstream's
/// `q = 1..=14` sweep.
///
/// 152 spawned tasks and 2056, a factor of thirteen apart: the smaller is
/// where the fixed cost of a round -- the root submission and its wait --
/// still shows against the tree, and the larger is where the per-task cost
/// dominates it, which is the choice `BENCHMARKS.md`'s "What is measured"
/// asks every table to make. The larger is also the widest board this id
/// reaches at one worker, for the reason the module header gives.
const SIZES: [usize; 2] = [6, 8];

/// Upstream's `answers` table from `nqueens.hpp`, indexed by board size, with
/// its leading entry for the empty board and cut off at the top of upstream's
/// own default sweep (`-q 14`) rather than at its 18.
///
/// Carried rather than recomputed so that the check below is upstream's own:
/// `answers[6]` is 4 and `answers[8]` is 92.
const ANSWERS: [u64; 15] = [
    0, 1, 0, 0, 2, 10, 4, 40, 92, 352, 724, 2680, 14200, 73712, 365_596,
];

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// Upstream's `queens_ok(n, a)`: true when no two of the first `n` queens
/// attack each other.
///
/// Both ids call this, so the pruning is the same work on both sides of a row.
fn queens_ok(n: usize, board: &[u8]) -> bool {
    let placed = &board[..n];

    for (i, &column) in placed.iter().enumerate() {
        let p = i32::from(column);

        for (j, &other) in placed.iter().enumerate().skip(i + 1) {
            let q = i32::from(other);
            let span = i32::try_from(j - i).expect("a board side below fifteen fits in an i32");

            if q == p || q == p - span || q == p + span {
                return false;
            }
        }
    }

    true
}

/// Upstream's `nqueens(int j, std::vector<char>& a, tf::Executor&)`: one child
/// per legal placement in column `row` into a group of this level's own, a
/// `corun` to join them, and the sum of the slots.
///
/// `row` is upstream's `j`, the number of columns already placed; the board's
/// length is its `N`.
fn nqueens_group(row: usize, board: &[u8], executor: &Arc<Executor>) -> u64 {
    let side = board.len();
    if row == side {
        return 1;
    }

    let group = executor
        .task_group()
        .expect("the recursion runs on a worker, which may open a group");
    let parts: Arc<Vec<AtomicU64>> = Arc::new((0..side).map(|_| AtomicU64::new(0)).collect());

    for i in 0..side {
        let mut next = board.to_vec();
        next[row] = u8::try_from(i).expect("a board side below fifteen fits in a u8");

        // Upstream's `else parts[i] = 0`, which the slot already holds.
        if !queens_ok(row + 1, &next) {
            continue;
        }

        group.silent_async({
            let executor = Arc::clone(executor);
            let parts = Arc::clone(&parts);
            move || {
                let count = nqueens_group(row + 1, &next, &executor);
                parts[i].store(count, Ordering::Relaxed);
            }
        });
    }

    group.corun();

    parts.iter().map(|slot| slot.load(Ordering::Relaxed)).sum()
}

/// The same recursion as a `rayon::scope` of one `spawn` per legal placement,
/// in the role upstream's `tbb::task_group` plays.
///
/// The slots are a plain `Vec<u64>` split into disjoint `&mut`, so this id
/// pays neither the refcount pair nor the atomic the module header describes;
/// a child still owns its board, because a scope's `spawn` is `Send` too.
fn nqueens_rayon(row: usize, board: &[u8]) -> u64 {
    let side = board.len();
    if row == side {
        return 1;
    }

    let mut parts = vec![0u64; side];

    rayon::scope(|scope| {
        for (i, slot) in parts.iter_mut().enumerate() {
            let mut next = board.to_vec();
            next[row] = u8::try_from(i).expect("a board side below fifteen fits in a u8");

            if queens_ok(row + 1, &next) {
                scope.spawn(move |_| {
                    *slot = nqueens_rayon(row + 1, &next);
                });
            }
        }
    });

    parts.iter().sum()
}

/// Upstream's `nqueens(executor, 0, buf)` around [`nqueens_group`]: the
/// recursion has to start on a worker, so the whole of it runs inside one
/// `spawn` whose handle is read.
///
/// The empty board is upstream's `std::vector<char> buf(num_nqueens)`, built
/// here per round rather than once per measurement -- one `side`-byte
/// allocation against the `side`-byte allocation every node of the tree makes.
fn group_on(side: usize, executor: &Arc<Executor>) -> u64 {
    executor
        .spawn({
            let executor = Arc::clone(executor);
            move || nqueens_group(0, &vec![0u8; side], &executor)
        })
        .wait()
        .expect("nothing under the recursion panicked")
}

/// The rayon wrapper. `install` is what holds the recursion to the worker
/// count this row is measuring, as in every other shape here.
fn rayon_on(side: usize, pool: &rayon::ThreadPool) -> u64 {
    pool.install(|| nqueens_rayon(0, &vec![0u8; side]))
}

/// Registers both ids at every board size and worker count.
fn bench_nqueens(c: &mut Criterion) {
    let mut group = c.benchmark_group("nqueens");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in common::worker_counts() {
        // An `Arc` because the recursion reaches the executor from a
        // `'static` closure.
        let executor = Arc::new(common::executor(workers));
        let pool = baseline::rayon_pool(workers);

        for side in SIZES {
            let answer = ANSWERS[side];

            // One untimed run of each id, checked against upstream's own
            // table before criterion times anything: a recursion that lost a
            // subtree then fails here rather than reporting a good number.
            assert_eq!(group_on(side, &executor), answer, "nqueens({side})");
            assert_eq!(rayon_on(side, &pool), answer, "nqueens({side})");

            group.bench_function(
                BenchmarkId::new(format!("rustflow-group/{workers}w"), side),
                |b| b.iter(|| black_box(group_on(black_box(side), &executor))),
            );
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), side), |b| {
                b.iter(|| black_box(rayon_on(black_box(side), &pool)))
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_nqueens);
criterion_main!(benches);

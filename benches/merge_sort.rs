//! Translated from `benchmarks/merge_sort/` (Taskflow v4.1.0).
//!
//! The textbook merge sort, parallelised the way upstream's
//! `merge_sort/taskflow.cpp` does: below a cutoff of 1024 elements a range is
//! sorted in place and the recursion stops; above it the range is split at
//! its midpoint, the right half is `silent_async`ed into a
//! [`TaskGroup`](rustflow::TaskGroup) of that level's own, the left half is
//! sorted on the calling worker, `corun` joins the two, and the two sorted
//! halves are merged through a second array of the same length and copied
//! back. The root is one `Executor::spawn` whose handle is waited, because
//! the recursion has to start on a worker.
//!
//! This is `benches/sort.rs`'s data under a different schedule, and the two
//! are worth reading together: there the whole sort is one
//! `taskflow.sort_by` call and the baseline is `rayon::par_sort_unstable_by`,
//! so both columns are a library's own sort. Here the recursion is written
//! out, so what a row measures is one `silent_async` and one `corun` per
//! level against **`rayon::join`** -- the baseline this shape asks for, as
//! upstream's `tbb.cpp` writes the same recursion on a `tbb::task_group`.
//!
//! # Design
//!
//! **How the two halves reach the two branches.** A
//! [`TaskGroup::silent_async`](rustflow::TaskGroup::silent_async) body is
//! `FnOnce() + Send + 'static`, so it cannot hold the `&mut [f64]` that
//! `split_at_mut` would hand it: upstream's child captures the whole
//! `std::vector<double>` by reference and is trusted to touch only
//! `[mid, end)`. The `rustflow` column therefore keeps the array as a slice
//! of `AtomicU64` holding `f64::to_bits` patterns, shared by `Arc`, and
//! passes the `begin`/`mid`/`end` indices upstream passes -- the same
//! departure `benches/for_each.rs` records, for the same reason, and the one
//! that keeps this port free of `unsafe`. Disjointness is then a property of
//! the index arithmetic rather than something the compiler checks, which is
//! exactly the property the untimed assertion below exists to catch: two
//! branches that overlapped would lose or duplicate elements and the output
//! would not match a sequentially sorted copy. A relaxed load and store are
//! the instructions a plain load and store are on the targets this is
//! measured on. The `rayon` column does get real `&mut [f64]` halves out of
//! `split_at_mut`, since a `rayon::join` body may borrow its frame; that
//! asymmetry is in the baseline's favour and stated in `BENCHMARKS.md`.
//!
//! **What bounds the depth.** Upstream's 1024-element cutoff, kept
//! unchanged: a range is halved until it is at most 1024 long, so the
//! recursion is `ceil(log2(N / 1024))` levels deep -- 4 levels and ~15
//! spawned tasks at `N = 10_000`, 10 levels and ~1023 at `N = 1_000_000` --
//! and the tasks outstanding at any moment are bounded by that count. That
//! is what makes this shape safe where `benches/fibonacci.rs`'s
//! `rustflow-group` id is not: the nested-corun stack bound that `DESIGN.md`'s
//! "Executor core" states costs a frame per outstanding task, about a
//! kilobyte of a worker's two megabytes in a release build, so a thousand
//! `msort` frames is inside it -- measured at both worker counts rather than
//! assumed.
//!
//! **Two departures from upstream beyond the array's type.** Upstream wraps
//! the recursion in *two* nested `executor.async` calls --
//! `merge_sort_taskflow` submits `msort(executor, vec)`, which allocates the
//! buffer and then submits the recursion again and blocks a worker on its
//! `wait()` -- and both wrap the same work, so this port submits one root
//! task and waits its handle, which is what the shape is for. And each
//! iteration runs on a fresh shuffle through
//! [`criterion::Bencher::iter_batched_ref`], as `benches/sort.rs` does and
//! for its reason: the input is mutated in place, so criterion's plain
//! `iter` would time a sort of already-sorted data on every sample after
//! the first. Each id owns one array, allocated before anything is timed and
//! re-shuffled by that untimed setup, with `BatchSize::PerIteration` as the
//! only batch size that is correct for one buffer -- a batch of `k` would
//! shuffle once and sort `k` times. The *merge* buffer stays inside the
//! timed region on both sides, where upstream's
//! `std::vector<T> buffer(data.size())` is. `BENCHMARKS.md`'s "#366 change"
//! section has the measurement behind the split.
//!
//! Correctness is checked before any id is timed: both columns sort one
//! shuffle at each size and worker count outside every measured region, and
//! each output is compared element by element against a copy sorted by
//! `sort_unstable_by` on one thread. The comparison is exact rather than a
//! tolerance -- a sort permutes its input and invents no values, so the
//! sorted sequence of a multiset of `f64`s is unique whatever order the
//! halves were merged in.

use std::cell::RefCell;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rustflow::Executor;

#[path = "common/baseline.rs"]
mod baseline;
mod common;

/// Element counts, two points off upstream's sweep (`N = 10..=1e8`, ×10 each
/// step): one where the fixed cost of a run and of the first few levels
/// still shows, one where the per-task cost of a thousand of them dominates
/// it.
///
/// Upstream's sweep starts at `N = 10`, and its first three points are below
/// the 1024-element cutoff, where this shape degenerates into one
/// `sort_unstable_by` and spawns nothing at all -- `benches/sort.rs` already
/// measures that case at `N = 1_000`. `10_000` is the first point of the
/// sweep where there is a recursion to measure.
const SIZES: [usize; 2] = [10_000, 1_000_000];

/// Upstream's sequential cutoff, `merge_sort/taskflow.cpp`'s `n <= 1024`: the
/// range length at or below which a level sorts in place instead of
/// splitting. It is what bounds the depth, which the module docs expand on.
const CUTOFF: usize = 1024;

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// `n` pseudo-random `f64`s: upstream's `d = ::rand()` loop, over a linear
/// congruential generator rather than `rand`, so a run is reproducible.
/// `benches/sort.rs`'s generator, element for element, so that the two
/// shapes sort the same data.
fn generated(n: usize) -> impl Iterator<Item = f64> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..n).map(move |_| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 11) as f64
    })
}

/// [`generated`]'s first `n` values as a vector: what each id's own array is
/// allocated as, and what [`verify`] compares against.
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

/// [`reshuffle`] through the cell array the `rustflow` recursion sorts.
fn reshuffle_cells(cells: &[AtomicU64]) {
    for (cell, value) in cells.iter().zip(generated(cells.len())) {
        cell.store(value.to_bits(), Ordering::Relaxed);
    }
}

/// `data` as the cell array the `rustflow` recursion sorts through. See the
/// module docs for why the elements are atomics.
fn cells(data: &[f64]) -> Arc<Vec<AtomicU64>> {
    Arc::new(data.iter().map(|d| AtomicU64::new(d.to_bits())).collect())
}

/// `n` cells for the merge buffer: upstream's
/// `std::vector<T> buffer(data.size())`, zero-filled and overwritten range by
/// range.
fn buffer(n: usize) -> Arc<Vec<AtomicU64>> {
    Arc::new((0..n).map(|_| AtomicU64::new(0)).collect())
}

/// One cell as the `f64` it holds.
fn load(cell: &AtomicU64) -> f64 {
    f64::from_bits(cell.load(Ordering::Relaxed))
}

/// Reads the whole array back, for the untimed comparison.
fn read_back(cells: &[AtomicU64]) -> Vec<f64> {
    cells.iter().map(load).collect()
}

/// Upstream's
/// `msort(tf::Executor&, std::vector<T>&, std::vector<T>&, size_t, size_t)`:
/// the right half into a group of this level's own, the left half on this
/// very worker, a `corun` to join them, and the merge back through `buffer`.
///
/// `Relaxed` on every cell is sound for the reason it is there, which is
/// `benches/fibonacci.rs`'s: what orders a child's stores against this
/// frame's loads is the `corun`, and no two frames ever touch the same index
/// at once.
fn msort_rustflow(
    executor: &Arc<Executor>,
    data: &Arc<Vec<AtomicU64>>,
    buffer: &Arc<Vec<AtomicU64>>,
    begin: usize,
    end: usize,
) {
    let n = end - begin;

    if n <= CUTOFF {
        // Upstream's `std::sort(data.begin() + begin, data.begin() + end)`,
        // which needs a real slice: the range is copied out, sorted and
        // written back. `sort_unstable_by` with `total_cmp` for
        // `benches/sort.rs`'s reason -- `f64` is not `Ord`, and upstream's
        // `std::less<double>` is implicit in its own call.
        let mut leaf: Vec<f64> = data[begin..end].iter().map(load).collect();
        leaf.sort_unstable_by(f64::total_cmp);
        for (cell, sorted) in data[begin..end].iter().zip(&leaf) {
            cell.store(sorted.to_bits(), Ordering::Relaxed);
        }
        return;
    }

    let mid = begin + n / 2;

    let group = executor
        .task_group()
        .expect("the recursion runs on a worker, which may open a group");
    group.silent_async({
        let executor = Arc::clone(executor);
        let data = Arc::clone(data);
        let buffer = Arc::clone(buffer);
        move || msort_rustflow(&executor, &data, &buffer, mid, end)
    });

    msort_rustflow(executor, data, buffer, begin, mid);

    group.corun();

    merge_cells(data, buffer, begin, mid, end);
}

/// Upstream's `std::merge` into `buffer` followed by its `std::copy` back:
/// `[begin, mid)` and `[mid, end)` are each sorted, and `[begin, end)` is
/// their merge.
fn merge_cells(data: &[AtomicU64], buffer: &[AtomicU64], begin: usize, mid: usize, end: usize) {
    let mut left = begin;
    let mut right = mid;

    for slot in &buffer[begin..end] {
        // `std::merge` takes from the first range unless the second's head
        // compares less, which is what keeps the merge stable; `total_cmp`
        // stands in for `std::less<double>` as it does at the leaves.
        let take_left = if left == mid {
            false
        } else {
            right == end || load(&data[left]).total_cmp(&load(&data[right])).is_le()
        };

        let source = if take_left {
            left += 1;
            left - 1
        } else {
            right += 1;
            right - 1
        };
        slot.store(data[source].load(Ordering::Relaxed), Ordering::Relaxed);
    }

    for (cell, merged) in data[begin..end].iter().zip(&buffer[begin..end]) {
        cell.store(merged.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

/// Upstream's `merge_sort_taskflow`: the recursion has to start on a worker,
/// so the whole of it runs inside one `spawn` whose handle is waited. The
/// buffer is allocated where upstream allocates it, inside the timed region.
fn sort_rustflow(data: &Arc<Vec<AtomicU64>>, executor: &Arc<Executor>) {
    let n = data.len();
    executor
        .spawn({
            let executor = Arc::clone(executor);
            let data = Arc::clone(data);
            move || {
                let buffer = buffer(n);
                msort_rustflow(&executor, &data, &buffer, 0, n);
            }
        })
        .wait()
        .expect("no task under the recursion panicked");
}

/// The same recursion on `rayon::join`, in the role TBB's `task_group` plays
/// upstream -- and not `par_sort_unstable_by`, which is what
/// `benches/sort.rs` already compares against.
///
/// The halves are `split_at_mut`'s, since a `rayon::join` body may borrow the
/// frame that forked it. The right half goes first because `join` runs its
/// *second* closure on the forking thread when nobody steals it, which is
/// where upstream's left half runs.
fn msort_rayon(data: &mut [f64], buffer: &mut [f64]) {
    let n = data.len();

    if n <= CUTOFF {
        data.sort_unstable_by(f64::total_cmp);
        return;
    }

    let mid = n / 2;
    {
        let (left, right) = data.split_at_mut(mid);
        let (left_buffer, right_buffer) = buffer.split_at_mut(mid);
        rayon::join(
            || msort_rayon(right, right_buffer),
            || msort_rayon(left, left_buffer),
        );
    }

    merge_slices(data, buffer, mid);
}

/// `merge_cells` over plain slices: the baseline's `std::merge` and
/// `std::copy`.
fn merge_slices(data: &mut [f64], buffer: &mut [f64], mid: usize) {
    {
        let (left, right) = data.split_at(mid);
        let mut i = 0;
        let mut j = 0;

        for slot in buffer.iter_mut() {
            let take_left = if i == left.len() {
                false
            } else {
                j == right.len() || left[i].total_cmp(&right[j]).is_le()
            };

            if take_left {
                *slot = left[i];
                i += 1;
            } else {
                *slot = right[j];
                j += 1;
            }
        }
    }

    data.copy_from_slice(buffer);
}

/// The baseline's root: the buffer allocated where the `rustflow` column
/// allocates it, and `install` to hold the recursion to the worker count this
/// row is measuring.
fn sort_rayon(data: &mut [f64], pool: &rayon::ThreadPool) {
    let mut buffer = vec![0.0; data.len()];
    pool.install(|| msort_rayon(data, &mut buffer));
}

/// Runs both variants once, outside any measured region, and checks each
/// output against a sequentially sorted copy of its own input.
///
/// The untimed half of this target's acceptance: two branches that overlapped
/// a range, or a merge that dropped one, fail here rather than producing a
/// figure.
fn verify(size: usize, workers: usize, executor: &Arc<Executor>, pool: &rayon::ThreadPool) {
    let input = shuffled(size);
    let mut expected = input.clone();
    expected.sort_unstable_by(f64::total_cmp);

    let cells = cells(&input);
    sort_rustflow(&cells, executor);
    let actual = read_back(&cells);
    let mismatch = actual.iter().zip(&expected).position(|(a, b)| a != b);
    assert_eq!(
        mismatch, None,
        "rustflow/{workers}w at size {size}: first index whose element is not the sorted one"
    );

    let mut data = input;
    sort_rayon(&mut data, pool);
    let mismatch = data.iter().zip(&expected).position(|(a, b)| a != b);
    assert_eq!(
        mismatch, None,
        "rayon/{workers}w at size {size}: first index whose element is not the sorted one"
    );
}

/// Registers both variants at every size and worker count.
fn bench_merge_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_sort");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in common::worker_counts() {
        // An `Arc` because every level reaches the executor from a `'static`
        // closure to open a group of its own, as `benches/fibonacci.rs`'s
        // group variant does.
        let executor = Arc::new(common::executor(workers));
        let pool = baseline::rayon_pool(workers);

        for size in SIZES {
            verify(size, workers, &executor, &pool);

            // One array per id, allocated here and re-shuffled by the
            // untimed setup of every iteration. Allocating it per iteration
            // instead puts an 8 MB allocation, fill and free either side of
            // the timed region and part of it *inside*, which is what
            // `BENCHMARKS.md`'s "#366 change" section measures. The merge
            // buffer is a different question and stays where upstream puts
            // it, inside.
            let cells = cells(&shuffled(size));
            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| {
                    b.iter_batched_ref(
                        || reshuffle_cells(&cells),
                        |_: &mut ()| sort_rustflow(black_box(&cells), &executor),
                        BatchSize::PerIteration,
                    );
                },
            );

            // A `RefCell` only so that the untimed setup and the timed
            // closure can each reach this id's one array; nothing but this
            // thread ever borrows it, and the recursion still gets the
            // `&mut [f64]` its `split_at_mut` needs.
            let data = RefCell::new(shuffled(size));
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), size), |b| {
                b.iter_batched_ref(
                    || reshuffle(&mut data.borrow_mut()),
                    |_: &mut ()| {
                        let mut data = data.borrow_mut();
                        sort_rayon(black_box(&mut data), &pool);
                    },
                    BatchSize::PerIteration,
                );
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_merge_sort);
criterion_main!(benches);

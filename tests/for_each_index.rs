//! Translated from the `for_each_index` and `for_each_by_index` sections of
//! `unittests/test_for_each.cpp` (Taskflow v4.1.0).
//!
//! Upstream's `for_each<P>(unsigned)` tests `for_each_index` and `for_each`
//! (the slice form) in one function, under one family of `ParallelFor.*`
//! `TEST_CASE`s; `src/algorithm/for_each.rs` is a sibling module ported
//! separately and claims those names for the `for_each` half (its own
//! issue's acceptance), so [`parallel_for_index`] below is this file's own
//! name for the `for_each_index` half of that same upstream function, over
//! every partitioner the way upstream's `TEST_CASE`s already sweep it.
//!
//! Three upstream sections are not here, each for a reason already settled
//! in `src/algorithm/for_each_index.rs`'s own `# Design` section: that
//! module does not take `beg`/`end`/`step`/`range` as upstream's deferred
//! `std::ref` providers, only as fixed values, so there is nothing here to
//! call `StatefulParallelFor.*`, `StatefulForEach.NegativeIndexRange.*` or
//! the `std::ref(range)` half of `ForEachIndex`'s own stateful case with.
//! `MultipleParallelForEach.*` is the fourth: it drives `for_each` and
//! `for_each_index` over the same taskflow in one case, which is exactly
//! the shared-file boundary `for_each.rs` and this file agreed to keep by
//! not testing each other's half.
//!
//! `ForEachIndex.NegativeIndex.*`'s sweep keeps upstream's full
//! `beg`/`end`/`step` ranges -- the case is cheap per run, the loop is what
//! upstream actually asserts over, and trimming it would be trimming the
//! claim rather than the runtime.
//!
//! [`range_based_for_each_index_nd`] at the bottom is this file's own case
//! for `for_each_by_index_nd` (#192): no upstream case to translate, since
//! `IndexRanges<T, N>` for `N > 1` is this port's own addition to
//! `for_each_by_index`, but swept over the same partitioner family and
//! worker-count matrix as [`range_based_for_each_index`] above it.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::algorithm::AlgorithmBuilder;
use rustflow::algorithm::for_each_index::IndexDispatch;
use rustflow::algorithm::index_range::{IndexRange, IndexRanges, distance};
use rustflow::algorithm::partitioner::{
    DefaultClosureWrapper, DefaultPartitioner, DynamicPartitioner, GuidedPartitioner,
    RandomPartitioner, StaticPartitioner,
};
use rustflow::prelude::*;

#[macro_use]
mod common;
#[path = "common/counters.rs"]
mod counters;

use counters::{count, counter};

// ----------------------------------------------------------------------------
// the for_each_index half of upstream's `for_each<P>(unsigned)`
// ----------------------------------------------------------------------------

/// The `for_each_index` half of upstream's shared `for_each<P>(unsigned)`:
/// every `n` from 0 to 150, every power-of-two step up to 16 and every
/// listed chunk size, `taskflow.for_each_index(beg, beg + n, s, ...)` visits
/// exactly the indices upstream's own loop computes, each exactly once.
fn parallel_for_index<P>(make: impl Fn(usize) -> P, workers: usize)
where
    P: IndexDispatch + Clone + Send + 'static,
{
    let executor = Executor::new(workers);
    let vec: Vec<AtomicI64> = (0..1024).map(|_| AtomicI64::new(-1)).collect();
    let vec = Arc::new(vec);

    for n in 0_i64..=150 {
        let beg: i64 = fastrand_like(n) % 300 - 150;
        let end = beg + n;

        for s in [1_i64, 2, 4, 8, 16] {
            for &c in &[0_usize, 1, 3, 7, 99] {
                let counter = counter();
                let counted = Arc::clone(&counter);
                let vec_body = Arc::clone(&vec);

                let mut taskflow = Taskflow::new();
                taskflow.for_each_index(
                    beg,
                    end,
                    s,
                    move |i: i64| {
                        counted.fetch_add(1, Ordering::Relaxed);
                        let slot = usize::try_from(i - beg).expect("i - beg is non-negative here");
                        vec_body[slot].store(i, Ordering::Relaxed);
                    },
                    make(c),
                );

                executor.run(&taskflow).wait().expect("no task panicked");

                let expected = usize::try_from((n + s - 1) / s).expect("non-negative count");
                assert_eq!(count(&counter), expected, "n={n} s={s} c={c}");

                let mut i = beg;
                while i < end {
                    let slot = usize::try_from(i - beg).expect("i - beg is non-negative here");
                    assert_eq!(vec[slot].load(Ordering::Relaxed), i, "n={n} s={s} c={c}");
                    vec[slot].store(-1, Ordering::Relaxed);
                    i += s;
                }
                for (slot, value) in vec.iter().enumerate() {
                    assert_eq!(
                        value.load(Ordering::Relaxed),
                        -1,
                        "slot {slot}, n={n} s={s} c={c}"
                    );
                }
            }
        }
    }
}

/// A small, deterministic stand-in for upstream's `::rand() % 300`: this
/// suite only needs `beg` to vary across `n`, not to be unpredictable, and a
/// fixed recurrence keeps a failing case reproducible without a seed to
/// report.
fn fastrand_like(n: i64) -> i64 {
    (n.wrapping_mul(2_654_435_761) >> 8) & 0x7fff_ffff
}

fn parallel_for_index_static(workers: usize) {
    parallel_for_index(StaticPartitioner::<DefaultClosureWrapper>::new, workers);
}

fn parallel_for_index_guided(workers: usize) {
    parallel_for_index(GuidedPartitioner::<DefaultClosureWrapper>::new, workers);
}

fn parallel_for_index_dynamic(workers: usize) {
    parallel_for_index(DynamicPartitioner::<DefaultClosureWrapper>::new, workers);
}

fn parallel_for_index_random(workers: usize) {
    parallel_for_index(RandomPartitioner::<DefaultClosureWrapper>::new, workers);
}

worker_matrix! {
    parallel_for_index_static {
        parallel_for_index_static_1_worker(1),
        parallel_for_index_static_2_workers(2),
        parallel_for_index_static_3_workers(3),
        parallel_for_index_static_4_workers(4),
    }
}

worker_matrix! {
    parallel_for_index_guided {
        parallel_for_index_guided_1_worker(1),
        parallel_for_index_guided_2_workers(2),
        parallel_for_index_guided_3_workers(3),
        parallel_for_index_guided_4_workers(4),
    }
}

worker_matrix! {
    parallel_for_index_dynamic {
        parallel_for_index_dynamic_1_worker(1),
        parallel_for_index_dynamic_2_workers(2),
        parallel_for_index_dynamic_3_workers(3),
        parallel_for_index_dynamic_4_workers(4),
    }
}

worker_matrix! {
    parallel_for_index_random {
        parallel_for_index_random_1_worker(1),
        parallel_for_index_random_2_workers(2),
        parallel_for_index_random_3_workers(3),
        parallel_for_index_random_4_workers(4),
    }
}

// ----------------------------------------------------------------------------
// ForEachIndex.NegativeIndex
// ----------------------------------------------------------------------------

/// Upstream's `test_for_each_index_negative`: every `(beg, end, step)` with
/// a negative step from 10 down to -10 visits exactly as many indices as a
/// plain backward walk would.
fn for_each_index_negative(workers: usize) {
    let executor = Executor::new(workers);

    for beg in (-10..=10_i32).rev() {
        for end in (-10..=beg).rev() {
            for s in 1..=(beg - end) {
                let mut n = 0_i32;
                let mut b = beg;
                while b > end {
                    n += 1;
                    b -= s;
                }

                let counter = counter();
                let counted = Arc::clone(&counter);

                let mut taskflow = Taskflow::new();
                taskflow.for_each_index(
                    beg,
                    end,
                    -s,
                    move |_: i32| {
                        counted.fetch_add(1, Ordering::Relaxed);
                    },
                    DefaultPartitioner::default(),
                );
                executor.run(&taskflow).wait().expect("no task panicked");

                assert_eq!(
                    count(&counter),
                    usize::try_from(n).expect("non-negative count"),
                    "beg={beg} end={end} step={s}"
                );
            }
        }
    }
}

worker_matrix! {
    for_each_index_negative {
        for_each_index_negative_1_worker(1),
        for_each_index_negative_2_workers(2),
        for_each_index_negative_3_workers(3),
        for_each_index_negative_4_workers(4),
    }
}

// ----------------------------------------------------------------------------
// ForEachIndex.InvalidRange
// ----------------------------------------------------------------------------

/// An invalid range (`beg > end` on a non-negative step) visits nothing.
#[test]
fn for_each_index_invalid_range_visits_nothing() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&counter);

    let mut flow = Taskflow::new();
    flow.for_each_index(
        0_i32,
        -1,
        1,
        move |i: i32| {
            counted.fetch_add(usize::try_from(i).unwrap_or(0), Ordering::Relaxed);
        },
        DefaultPartitioner::default(),
    );

    Executor::new(2)
        .run(&flow)
        .wait()
        .expect("no task panicked");
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

// ----------------------------------------------------------------------------
// ForEachIndex.HeterogeneousRange
// ----------------------------------------------------------------------------

/// `beg`/`end`/`step` of an unsigned index type: the sum `1 + .. + 9`.
#[test]
fn for_each_index_heterogeneous_range_sums_to_the_triangular_number() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&counter);

    let from: usize = 1;
    let to: usize = 10;
    let step: usize = 1;

    let mut flow = Taskflow::new();
    flow.for_each_index(
        from,
        to,
        step,
        move |i: usize| {
            counted.fetch_add(i, Ordering::Relaxed);
        },
        DefaultPartitioner::default(),
    );

    Executor::new(2)
        .run(&flow)
        .wait()
        .expect("no task panicked");
    assert_eq!(counter.load(Ordering::Relaxed), to * (to - 1) / 2);
}

// ----------------------------------------------------------------------------
// range-based for_each_index (for_each_by_index)
// ----------------------------------------------------------------------------

/// Upstream's `range_based_for_each_index<P>`: every `(beg, end, step)` with
/// a negative step from 10 down to -10, `for_each_by_index` hands every
/// chunk its own correctly-sized sub-range and every chunk together covers
/// the whole range exactly once.
fn range_based_for_each_index<P>(make: impl Fn(usize) -> P, workers: usize)
where
    P: IndexDispatch + Clone + Send + 'static,
{
    let executor = Executor::new(workers);

    for beg in (-10..=10_i32).rev() {
        for end in (-10..=beg).rev() {
            for s in 1..=(beg - end) {
                let n = distance(beg, end, -s);

                for c in 0..3_usize {
                    let counter = Arc::new(AtomicUsize::new(0));
                    let counted = Arc::clone(&counter);

                    let range = IndexRange::new(beg, end, -s);
                    assert_eq!(range.size(), n, "beg={beg} end={end} step={s}");

                    let mut taskflow = Taskflow::new();
                    taskflow.for_each_by_index(
                        range,
                        move |chunk: IndexRange<i32>| {
                            let mut l = 0_usize;
                            let mut j = chunk.begin();
                            while j > chunk.end() {
                                l += 1;
                                j += chunk.step_size();
                            }
                            assert_eq!(chunk.size(), l);
                            counted.fetch_add(l, Ordering::Relaxed);
                        },
                        make(c),
                    );
                    executor.run(&taskflow).wait().expect("no task panicked");
                    assert_eq!(
                        counter.load(Ordering::Relaxed),
                        n,
                        "beg={beg} end={end} step={s} c={c}"
                    );
                }
            }
        }
    }
}

fn range_based_for_each_index_static(workers: usize) {
    range_based_for_each_index(StaticPartitioner::<DefaultClosureWrapper>::new, workers);
}

fn range_based_for_each_index_guided(workers: usize) {
    range_based_for_each_index(GuidedPartitioner::<DefaultClosureWrapper>::new, workers);
}

fn range_based_for_each_index_dynamic(workers: usize) {
    range_based_for_each_index(DynamicPartitioner::<DefaultClosureWrapper>::new, workers);
}

worker_matrix! {
    range_based_for_each_index_static {
        range_based_for_each_index_static_1_worker(1),
        range_based_for_each_index_static_2_workers(2),
        range_based_for_each_index_static_3_workers(3),
        range_based_for_each_index_static_4_workers(4),
    }
}

worker_matrix! {
    range_based_for_each_index_guided {
        range_based_for_each_index_guided_1_worker(1),
        range_based_for_each_index_guided_2_workers(2),
        range_based_for_each_index_guided_3_workers(3),
        range_based_for_each_index_guided_4_workers(4),
    }
}

worker_matrix! {
    range_based_for_each_index_dynamic {
        range_based_for_each_index_dynamic_1_worker(1),
        range_based_for_each_index_dynamic_2_workers(2),
        range_based_for_each_index_dynamic_3_workers(3),
        range_based_for_each_index_dynamic_4_workers(4),
    }
}

// ----------------------------------------------------------------------------
// ClosureWrapper.for_each_index.*
// ----------------------------------------------------------------------------

/// Upstream's `ClosureWrapper.for_each_index.Static`: a [`StaticPartitioner`]
/// with chunk size 1 spawns exactly one task per worker, so its wrapper
/// runs exactly once per worker and every element sees that worker's own
/// thread-local context.
#[test]
fn closure_wrapper_for_each_index_static_runs_once_per_worker() {
    for workers in 1..16 {
        let executor = Executor::new(workers);
        let wrapper_calls = Arc::new(AtomicUsize::new(0));
        let element_calls = Arc::new(AtomicUsize::new(0));
        let seen: Arc<Vec<AtomicI64>> = Arc::new((0..100).map(|_| AtomicI64::new(-1)).collect());

        let counted_wrapper = Arc::clone(&wrapper_calls);
        let part = StaticPartitioner::with_closure_wrapper(1, move |task: &mut dyn FnMut()| {
            counted_wrapper.fetch_add(1, Ordering::Relaxed);
            let workers = i32::try_from(workers).expect("worker count fits in i32");
            CONTEXT_FOR_CLOSURE_WRAPPER_TEST.with(|c| c.set(workers));
            task();
            CONTEXT_FOR_CLOSURE_WRAPPER_TEST.with(|c| c.set(0));
        });

        let counted_element = Arc::clone(&element_calls);
        let seen_body = Arc::clone(&seen);
        let mut taskflow = Taskflow::new();
        taskflow.for_each_index(
            0_i32,
            100,
            1,
            move |i: i32| {
                counted_element.fetch_add(1, Ordering::Relaxed);
                let context = CONTEXT_FOR_CLOSURE_WRAPPER_TEST.with(std::cell::Cell::get);
                let slot = usize::try_from(i).expect("non-negative");
                seen_body[slot].store(i64::from(context), Ordering::Relaxed);
            },
            part,
        );
        executor.run(&taskflow).wait().expect("no task panicked");

        assert_eq!(
            element_calls.load(Ordering::Relaxed),
            100,
            "{workers} worker(s)"
        );
        let expected_context = i64::from(i32::try_from(workers).unwrap());
        for (i, value) in seen.iter().enumerate() {
            assert_eq!(
                value.load(Ordering::Relaxed),
                expected_context,
                "index {i}, {workers} worker(s)"
            );
        }
        assert_eq!(
            wrapper_calls.load(Ordering::Relaxed),
            workers,
            "{workers} worker(s)"
        );
    }
}

thread_local! {
    static CONTEXT_FOR_CLOSURE_WRAPPER_TEST: std::cell::Cell<i32> = const { std::cell::Cell::new(0) };
}

/// Upstream's `ClosureWrapper.for_each_index.Dynamic`: a [`DynamicPartitioner`]
/// may spawn fewer tasks than there are workers if one worker's cursor race
/// drains the range first, so the wrapper runs at most once per worker
/// rather than exactly once -- upstream's own comment on the case.
#[test]
fn closure_wrapper_for_each_index_dynamic_runs_at_most_once_per_worker() {
    const UPPER: usize = 1000;

    for workers in 1..16_usize {
        let executor = Executor::new(workers);
        let wrapper_calls = Arc::new(AtomicUsize::new(0));
        let element_calls = Arc::new(AtomicUsize::new(0));

        let counted_wrapper = Arc::clone(&wrapper_calls);
        let chunk = (UPPER / workers / 2).max(1);
        let part =
            DynamicPartitioner::with_closure_wrapper(chunk, move |task: &mut dyn FnMut()| {
                counted_wrapper.fetch_add(1, Ordering::Relaxed);
                task();
            });

        let counted_element = Arc::clone(&element_calls);
        let mut taskflow = Taskflow::new();
        taskflow.for_each_index(
            0_i64,
            i64::try_from(UPPER).unwrap(),
            1,
            move |_: i64| {
                counted_element.fetch_add(1, Ordering::Relaxed);
            },
            part,
        );
        executor.run(&taskflow).wait().expect("no task panicked");

        assert_eq!(
            element_calls.load(Ordering::Relaxed),
            UPPER,
            "{workers} worker(s)"
        );
        assert!(
            wrapper_calls.load(Ordering::Relaxed) <= workers,
            "{workers} worker(s)"
        );
    }
}

// ----------------------------------------------------------------------------
// for_each_by_index_nd (#192)
// ----------------------------------------------------------------------------

/// Every `(i, j)` of a 2-D range is visited exactly once, for every
/// partitioner and every worker count: this file's own case for
/// `for_each_by_index_nd`, over the same sweep shape as
/// [`range_based_for_each_index`].
fn range_based_for_each_index_nd<P>(make: impl Fn(usize) -> P, workers: usize)
where
    P: IndexDispatch + Clone + Send + 'static,
{
    let executor = Executor::new(workers);
    let rows = 9_usize;
    let cols = 13_usize;

    for c in 0..3_usize {
        let seen: Arc<Vec<AtomicUsize>> =
            Arc::new((0..rows * cols).map(|_| AtomicUsize::new(0)).collect());
        let marked = Arc::clone(&seen);

        let range = IndexRanges::from_ranges([
            IndexRange::new(0_i32, i32::try_from(rows).expect("rows fits in i32"), 1),
            IndexRange::new(0_i32, i32::try_from(cols).expect("cols fits in i32"), 1),
        ]);

        let mut taskflow = Taskflow::new();
        taskflow.for_each_by_index_nd(
            range,
            move |chunk: IndexRanges<i32, 2>| {
                let (i_beg, i_end, i_step) = chunk.dim(0);
                let (j_beg, j_end, j_step) = chunk.dim(1);
                let mut i = i_beg;
                while if i_step > 0 { i < i_end } else { i > i_end } {
                    let mut j = j_beg;
                    while if j_step > 0 { j < j_end } else { j > j_end } {
                        let row = usize::try_from(i).expect("row is non-negative in this test");
                        let col = usize::try_from(j).expect("col is non-negative in this test");
                        marked[row * cols + col].fetch_add(1, Ordering::Relaxed);
                        j += j_step;
                    }
                    i += i_step;
                }
            },
            make(c),
        );
        executor.run(&taskflow).wait().expect("no task panicked");

        for (idx, value) in seen.iter().enumerate() {
            assert_eq!(
                value.load(Ordering::Relaxed),
                1,
                "cell {idx}, c={c}, {workers} worker(s)"
            );
        }
    }
}

fn range_based_for_each_index_nd_static(workers: usize) {
    range_based_for_each_index_nd(StaticPartitioner::<DefaultClosureWrapper>::new, workers);
}

fn range_based_for_each_index_nd_guided(workers: usize) {
    range_based_for_each_index_nd(GuidedPartitioner::<DefaultClosureWrapper>::new, workers);
}

fn range_based_for_each_index_nd_dynamic(workers: usize) {
    range_based_for_each_index_nd(DynamicPartitioner::<DefaultClosureWrapper>::new, workers);
}

fn range_based_for_each_index_nd_random(workers: usize) {
    range_based_for_each_index_nd(RandomPartitioner::<DefaultClosureWrapper>::new, workers);
}

worker_matrix! {
    range_based_for_each_index_nd_static {
        range_based_for_each_index_nd_static_1_worker(1),
        range_based_for_each_index_nd_static_2_workers(2),
        range_based_for_each_index_nd_static_3_workers(3),
        range_based_for_each_index_nd_static_4_workers(4),
    }
}

worker_matrix! {
    range_based_for_each_index_nd_guided {
        range_based_for_each_index_nd_guided_1_worker(1),
        range_based_for_each_index_nd_guided_2_workers(2),
        range_based_for_each_index_nd_guided_3_workers(3),
        range_based_for_each_index_nd_guided_4_workers(4),
    }
}

worker_matrix! {
    range_based_for_each_index_nd_dynamic {
        range_based_for_each_index_nd_dynamic_1_worker(1),
        range_based_for_each_index_nd_dynamic_2_workers(2),
        range_based_for_each_index_nd_dynamic_3_workers(3),
        range_based_for_each_index_nd_dynamic_4_workers(4),
    }
}

worker_matrix! {
    range_based_for_each_index_nd_random {
        range_based_for_each_index_nd_random_1_worker(1),
        range_based_for_each_index_nd_random_2_workers(2),
        range_based_for_each_index_nd_random_3_workers(3),
        range_based_for_each_index_nd_random_4_workers(4),
    }
}

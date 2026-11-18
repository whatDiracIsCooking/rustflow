//! Translated from the parallel-sort sections of `unittests/test_sort.cpp`
//! (Taskflow v4.1.0): `ParallelSort.int.*`, `.ldouble.*` (`.float` here, since
//! Rust has no 80-bit extended type), `.object.*`, `.MoveOnlyObject.*` and
//! `.Async.*`. The `.DependentAsync.*`/`.SilentAsync.*`/
//! `.SilentDependentAsync.*` cases are not a separate port: they wrap the
//! identical `make_sort_task` closure in a different async-submission
//! flavour, which is what `tests/dependent_asyncs.rs` and `tests/asyncs.rs`
//! already exercise for every task shape, sort included.
//!
//! One adaptation, forced by cost rather than chosen: upstream sweeps every
//! case at `N = 100_000` (`1_000_000` for the move-only case). [`SIZES`]
//! keeps the two ends that matter and drops the sizes between them --
//! `src/algorithm/sort.rs`'s own unit tests already check every element is
//! classified correctly by the partition at the sizes in between -- so this
//! suite's job, like upstream's `TEST_CASE` list, is the worker-count matrix.
//! `0` and `100` are below the sort's smallest cutoff bucket (`128`, for
//! objects of 512 bytes or more) on every type this file sorts, so they
//! always take [`Runtime::sort_by`]'s serial short-circuit; `50_000` is
//! above its largest bucket (`4096`, for objects under 16 bytes) on every
//! type here too, so it always takes the full recursive, parallel path.
//! Which bucket a given `T` actually lands in is therefore not this file's
//! concern, unlike the module's own unit tests.

use std::sync::{Arc, Mutex};

use rustflow::prelude::*;
use rustflow::{Executor, Runtime};

#[macro_use]
mod common;

/// Sizes guaranteed to land below every type's cutoff (`0`, `1`, `100`) and
/// one guaranteed to land above every type's (`50_000`); this file's own
/// docs say why no size in between is needed here.
const SIZES: [usize; 5] = [0, 1, 10, 100, 50_000];

/// A linear congruential generator rather than `rand`: deterministic, so a
/// failure reproduces, and spread enough to exercise every branch of the
/// three-way partition (runs both above and below the pivot, and ties).
fn scattered(n: usize, modulus: i64) -> Vec<i64> {
    let mut state = 0x2545_f491_4f6c_dd1du64;
    (0..n)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let unsigned = (state >> 33) % u64::try_from(modulus).expect("modulus fits a u64");
            i64::try_from(unsigned).expect("modulus is small enough to fit an i64") - modulus / 2
        })
        .collect()
}

// ----------------------------------------------------------------------------
// ParallelSort.int / ParallelSort.float
// ----------------------------------------------------------------------------

/// Upstream's `ps_pod<T>`, over `i64`: `Runtime::sort`, `T: Ord`, checked
/// against `std`'s own sort at every size in [`SIZES`].
fn parallel_sort_int(workers: usize) {
    for &n in &SIZES {
        let data = scattered(n, 1000);
        let mut expected = data.clone();
        expected.sort_unstable();

        let mut taskflow = Taskflow::new();
        let mut to_sort = Some(data);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut data = to_sort.take().expect("this task runs once");
            runtime.sort(&mut data);
            assert_eq!(data, expected, "n = {n}");
            to_sort = Some(data);
        });

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }
}

worker_matrix! {
    parallel_sort_int {
        parallel_sort_int_1_thread(1),
        parallel_sort_int_2_threads(2),
        parallel_sort_int_3_threads(3),
        parallel_sort_int_4_threads(4),
    }
}

/// `ps_pod<long double>`'s role, over `f64`: `Runtime::sort_by` with
/// [`f64::total_cmp`], since `f64` is not [`Ord`].
fn parallel_sort_float(workers: usize) {
    for &n in &SIZES {
        let data: Vec<f64> = scattered(n, 1000).into_iter().map(|v| v as f64).collect();
        let mut expected = data.clone();
        expected.sort_unstable_by(f64::total_cmp);

        let mut taskflow = Taskflow::new();
        let mut to_sort = Some(data);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut data = to_sort.take().expect("this task runs once");
            runtime.sort_by(&mut data, f64::total_cmp);
            assert_eq!(data, expected, "n = {n}");
            to_sort = Some(data);
        });

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }
}

worker_matrix! {
    parallel_sort_float {
        parallel_sort_float_1_thread(1),
        parallel_sort_float_2_threads(2),
        parallel_sort_float_3_threads(3),
        parallel_sort_float_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// ParallelSort.object
// ----------------------------------------------------------------------------

/// Upstream's `Object`: ordered by a derived `sum()` rather than by field
/// order, which also produces plenty of ties for the three-way partition's
/// middle zone to collect.
#[derive(Clone)]
struct Object {
    integers: [i64; 10],
}

impl Object {
    fn sum(&self) -> i64 {
        self.integers.iter().sum()
    }
}

/// Upstream's `ps_object`: `Runtime::sort_by` with a comparator over a
/// derived key, checked against the same comparator run through `std`.
fn parallel_sort_object(workers: usize) {
    for &n in &SIZES {
        let raw = scattered(10 * n, 2000);
        let data: Vec<Object> = raw
            .chunks(10)
            .map(|chunk| Object {
                integers: chunk.try_into().expect("chunks(10) yields 10 elements"),
            })
            .collect();
        let mut expected = data.clone();
        expected.sort_unstable_by_key(Object::sum);
        let expected_sums: Vec<i64> = expected.iter().map(Object::sum).collect();

        let mut taskflow = Taskflow::new();
        let mut to_sort = Some(data);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut data = to_sort.take().expect("this task runs once");
            runtime.sort_by(&mut data, |a: &Object, b: &Object| a.sum().cmp(&b.sum()));
            // Ties (equal sums) may land in either order, since `Object`
            // carries no identity beyond its sum -- `std::is_sorted` is
            // upstream's own bar, so the sum sequence, not the struct
            // sequence, is what is checked against it.
            let sums: Vec<i64> = data.iter().map(Object::sum).collect();
            assert_eq!(sums, expected_sums, "n = {n}");
            to_sort = Some(data);
        });

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }
}

worker_matrix! {
    parallel_sort_object {
        parallel_sort_object_1_thread(1),
        parallel_sort_object_2_threads(2),
        parallel_sort_object_3_threads(3),
        parallel_sort_object_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// ParallelSort with a custom, order-reversing comparator
// ----------------------------------------------------------------------------

/// A second custom-comparator case over `Runtime::sort_by`, descending on a
/// plain `Ord` type: upstream's acceptance names "the custom-comparator
/// cases" in the plural, and this is the one `.object` does not cover --
/// `cmp` reversing a type that already has a natural order, rather than
/// standing in for one that has none.
fn parallel_sort_descending(workers: usize) {
    for &n in &SIZES {
        let data = scattered(n, 1000);
        let mut expected = data.clone();
        expected.sort_unstable_by(|a, b| b.cmp(a));

        let mut taskflow = Taskflow::new();
        let mut to_sort = Some(data);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut data = to_sort.take().expect("this task runs once");
            runtime.sort_by(&mut data, |a: &i64, b: &i64| b.cmp(a));
            assert_eq!(data, expected, "n = {n}");
            to_sort = Some(data);
        });

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }
}

worker_matrix! {
    parallel_sort_descending {
        parallel_sort_descending_1_thread(1),
        parallel_sort_descending_2_threads(2),
        parallel_sort_descending_3_threads(3),
        parallel_sort_descending_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// ParallelSort.MoveOnlyObject
// ----------------------------------------------------------------------------

/// Upstream's `MoveOnly1`: no `Copy`, and -- unlike upstream's C++ type,
/// which keeps a move constructor -- no [`Clone`] either, so this compiling
/// and passing at all is the proof that nothing in the sort ever duplicates
/// an element; every relocation is [`<[T]>::swap`].
struct MoveOnly(i64);

/// Upstream's `move_only_ps`: `Runtime::sort_by` over a type with no `Copy`
/// and no `Clone`.
fn parallel_sort_move_only(workers: usize) {
    for &n in &SIZES {
        let raw = scattered(n, 100);
        let mut expected = raw.clone();
        expected.sort_unstable();
        let data: Vec<MoveOnly> = raw.into_iter().map(MoveOnly).collect();

        let mut taskflow = Taskflow::new();
        let mut to_sort = Some(data);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut data = to_sort.take().expect("this task runs once");
            runtime.sort_by(&mut data, |a: &MoveOnly, b: &MoveOnly| a.0.cmp(&b.0));
            let values: Vec<i64> = data.iter().map(|m| m.0).collect();
            assert_eq!(values, expected, "n = {n}");
            to_sort = Some(data);
        });

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }
}

worker_matrix! {
    parallel_sort_move_only {
        parallel_sort_move_only_1_thread(1),
        parallel_sort_move_only_2_threads(2),
        parallel_sort_move_only_3_threads(3),
        parallel_sort_move_only_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// ParallelSort.Async
// ----------------------------------------------------------------------------

/// Upstream's `async`: `Runtime::sort` reached through
/// [`Executor::spawn_runtime`] rather than a graph task at all -- the
/// direct equivalent of wrapping `make_sort_task` in `executor.async`, since
/// there is no free-standing `make_sort_task` to hand an async submission
/// here (`src/algorithm/sort.rs`'s own module docs say why `Runtime::sort`
/// is called directly instead).
fn parallel_sort_async(workers: usize) {
    let executor = Executor::new(workers);
    for &n in &SIZES {
        let data = scattered(n, 1000);
        let mut expected = data.clone();
        expected.sort_unstable();

        let data = Arc::new(Mutex::new(data));
        let handle = executor.spawn_runtime({
            let data = Arc::clone(&data);
            move |runtime: &mut Runtime<'_>| {
                let mut guard = data
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                runtime.sort(&mut guard);
            }
        });
        handle.wait().expect("no task panicked");

        assert_eq!(*data.lock().expect("uncontended"), expected, "n = {n}");
    }
}

worker_matrix! {
    parallel_sort_async {
        parallel_sort_async_1_thread(1),
        parallel_sort_async_2_threads(2),
        parallel_sort_async_3_threads(3),
        parallel_sort_async_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// FlowBuilder::sort / FlowBuilder::sort_by, over owned data
// ----------------------------------------------------------------------------

/// Upstream's plain, non-stateful `taskflow.sort(beg, end)`, through
/// [`SortBuilder::sort`] over a shared `Arc<Mutex<Vec<T>>>` rather than an
/// iterator pair a caller must keep alive.
#[test]
fn flow_builder_sort_locks_and_runs_over_owned_data() {
    let raw = scattered(50_000, 1000);
    let mut expected = raw.clone();
    expected.sort_unstable();

    let data = Arc::new(Mutex::new(raw));
    let mut taskflow = Taskflow::new();
    taskflow.sort(Arc::clone(&data));

    Executor::new(3).run(&taskflow).wait().expect("no panic");

    assert_eq!(*data.lock().expect("uncontended"), expected);
}

/// Upstream's `taskflow.sort(beg, end, cmp)`, through [`SortBuilder::sort_by`].
#[test]
fn flow_builder_sort_by_locks_and_runs_with_a_custom_comparator() {
    let raw = scattered(50_000, 1000);
    let mut expected = raw.clone();
    expected.sort_unstable_by(|a, b| b.cmp(a));

    let data = Arc::new(Mutex::new(raw));
    let mut taskflow = Taskflow::new();
    taskflow.sort_by(Arc::clone(&data), |a: &i64, b: &i64| b.cmp(a));

    Executor::new(3).run(&taskflow).wait().expect("no panic");

    assert_eq!(*data.lock().expect("uncontended"), expected);
}

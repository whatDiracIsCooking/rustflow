//! Translated from `unittests/test_fill.cpp` (Taskflow v4.1.0): `ParallelFill.*`,
//! `ParallelFillN.*`, `ParallelFillStateful.4`, `ParallelFillStatefulMixed.4`
//! and `ParallelFillNStateful.4`.
//!
//! Two adaptations, both forced by the port rather than chosen:
//!
//! - **No `ParallelFillAsync`/`ParallelFillNAsync`.** Both call
//!   `executor.async(tf::make_fill_task(...))` directly, over the
//!   free-standing function upstream builds. This port collapses
//!   `make_fill_task` into [`rustflow::Runtime::fill`] itself --
//!   `src/algorithm/fill.rs`'s own doc comment says why, following the same
//!   choice `src/algorithm/for_each.rs`/`tests/for_each.rs` already made for
//!   `make_for_each_task` -- so there is no free function left to hand
//!   `Executor::spawn`/`silent_async` directly; a caller after the same
//!   effect reaches `Runtime::fill` through `Executor::spawn_runtime`
//!   instead, already covered by `tests/asyncs.rs`.
//! - **One sweep per worker count, not upstream's fixed four.** Upstream's
//!   suite is one `TEST_CASE` per worker count (1 to 4) and element type
//!   (`int`, `long double`); this file's `every_element_is_filled_exactly_once`
//!   additionally sweeps chunk size and length the way
//!   `src/algorithm/fill.rs`'s own unit test does, over `i64` alone -- the
//!   element type does not interact with the partitioning logic this port
//!   is checking, so one representative type covers it.

use rustflow::algorithm::SlicePartitioner;
use rustflow::algorithm::partitioner::{
    DefaultClosureWrapper, DefaultPartitioner, DynamicPartitioner, GuidedPartitioner,
    RandomPartitioner, StaticPartitioner,
};
use rustflow::prelude::*;

#[macro_use]
mod common;
#[path = "common/slice_graphs.rs"]
mod slice_graphs;

use slice_graphs::{
    assert_all, assert_filled_to, bounded_by_a_setup_task, from_a_setup_task, over_lengths,
    over_owned_data, wrapper_runs_once_per_worker,
};

// ----------------------------------------------------------------------------
// ParallelFill
// ----------------------------------------------------------------------------

/// Upstream's `fill_sync<T>(W, N, val)`, generalized over every partitioner:
/// every element of a freshly allocated `Vec<i64>` ends up `val`.
fn parallel_fill<P>(workers: usize, make: impl Fn() -> P)
where
    P: SlicePartitioner + Send + 'static,
{
    over_lengths(workers, make, |runtime, data, part| {
        let n = data.len();
        runtime.fill(data, 12, part);
        assert!(data.iter().all(|&v| v == 12), "n={n}");
    });
}

fn parallel_fill_static(workers: usize) {
    parallel_fill(workers, StaticPartitioner::<DefaultClosureWrapper>::default);
}

fn parallel_fill_dynamic(workers: usize) {
    parallel_fill(
        workers,
        DynamicPartitioner::<DefaultClosureWrapper>::default,
    );
}

fn parallel_fill_random(workers: usize) {
    parallel_fill(workers, RandomPartitioner::<DefaultClosureWrapper>::default);
}

fn parallel_fill_default(workers: usize) {
    parallel_fill(workers, DefaultPartitioner::default);
}

worker_matrix! {
    parallel_fill_static {
        parallel_fill_static_1_thread(1),
        parallel_fill_static_2_threads(2),
        parallel_fill_static_3_threads(3),
        parallel_fill_static_4_threads(4),
    }
}

worker_matrix! {
    parallel_fill_dynamic {
        parallel_fill_dynamic_1_thread(1),
        parallel_fill_dynamic_2_threads(2),
        parallel_fill_dynamic_3_threads(3),
        parallel_fill_dynamic_4_threads(4),
    }
}

worker_matrix! {
    parallel_fill_random {
        parallel_fill_random_1_thread(1),
        parallel_fill_random_2_threads(2),
        parallel_fill_random_3_threads(3),
        parallel_fill_random_4_threads(4),
    }
}

worker_matrix! {
    parallel_fill_default {
        parallel_fill_default_1_thread(1),
        parallel_fill_default_2_threads(2),
        parallel_fill_default_3_threads(3),
        parallel_fill_default_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// ParallelFillN
// ----------------------------------------------------------------------------

/// Upstream's `fill_n_partioners<P>(W, N, part)`: `fill_n` touches only the
/// first `N / 2` elements of a length-`N` vector, leaving the rest at their
/// initial value.
fn parallel_fill_n<P>(workers: usize, make: impl Fn() -> P)
where
    P: SlicePartitioner + Send + 'static,
{
    over_lengths(workers, make, |runtime, data, part| {
        let n = data.len();
        let count = n / 2;
        runtime.fill_n(data, count, 12, part);
        assert!(data[..count].iter().all(|&v| v == 12), "n={n}");
        assert!(data[count..].iter().all(|&v| v == 0), "n={n}");
    });
}

fn parallel_fill_n_static(workers: usize) {
    parallel_fill_n(workers, StaticPartitioner::<DefaultClosureWrapper>::default);
}

fn parallel_fill_n_dynamic(workers: usize) {
    parallel_fill_n(
        workers,
        DynamicPartitioner::<DefaultClosureWrapper>::default,
    );
}

fn parallel_fill_n_random(workers: usize) {
    parallel_fill_n(workers, RandomPartitioner::<DefaultClosureWrapper>::default);
}

fn parallel_fill_n_default(workers: usize) {
    parallel_fill_n(workers, DefaultPartitioner::default);
}

worker_matrix! {
    parallel_fill_n_static {
        parallel_fill_n_static_1_thread(1),
        parallel_fill_n_static_2_threads(2),
        parallel_fill_n_static_3_threads(3),
        parallel_fill_n_static_4_threads(4),
    }
}

worker_matrix! {
    parallel_fill_n_dynamic {
        parallel_fill_n_dynamic_1_thread(1),
        parallel_fill_n_dynamic_2_threads(2),
        parallel_fill_n_dynamic_3_threads(3),
        parallel_fill_n_dynamic_4_threads(4),
    }
}

worker_matrix! {
    parallel_fill_n_random {
        parallel_fill_n_random_1_thread(1),
        parallel_fill_n_random_2_threads(2),
        parallel_fill_n_random_3_threads(3),
        parallel_fill_n_random_4_threads(4),
    }
}

worker_matrix! {
    parallel_fill_n_default {
        parallel_fill_n_default_1_thread(1),
        parallel_fill_n_default_2_threads(2),
        parallel_fill_n_default_3_threads(3),
        parallel_fill_n_default_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// ParallelFillStateful / ParallelFillStatefulMixed / ParallelFillNStateful
// ----------------------------------------------------------------------------

/// Upstream's `ParallelFillStateful.4`: a borrowed slice computed by a
/// runtime task from state an earlier task set up, rather than two tasks
/// sharing `std::ref(beg)`/`std::ref(end)` -- `src/algorithm/for_each.rs`'s
/// `# Design` section says why that is the direct equivalent here.
#[test]
fn parallel_fill_stateful() {
    let backing = from_a_setup_task(4, |runtime, data| {
        runtime.fill(
            data,
            12,
            GuidedPartitioner::<DefaultClosureWrapper>::default(),
        );
    });

    assert_all(&backing, 12);
}

/// Upstream's `ParallelFillStatefulMixed.4`: one of the two bounds is
/// recomputed by an earlier task, the other is a fixed slice length known
/// up front -- mixed the same way by slicing a fixed-length `Vec` to the
/// bound a setup task provides.
#[test]
fn parallel_fill_stateful_mixed() {
    let backing = bounded_by_a_setup_task(1, |runtime, data| {
        runtime.fill(
            data,
            12,
            GuidedPartitioner::<DefaultClosureWrapper>::default(),
        );
    });

    assert_all(&backing, 12);
}

/// Upstream's `ParallelFillNStateful.4`.
#[test]
fn parallel_fill_n_stateful() {
    let backing = from_a_setup_task(1, |runtime, data| {
        runtime.fill_n(
            data,
            5000,
            12,
            GuidedPartitioner::<DefaultClosureWrapper>::default(),
        );
    });

    assert_filled_to(&backing, 5000, 12, 0);
}

// ----------------------------------------------------------------------------
// The owned-data wrapper and the closure wrapper
// ----------------------------------------------------------------------------

/// `FlowBuilder::fill` over owned, `Arc`-shared data: upstream's
/// `taskflow.fill(a.begin(), a.end(), val, part)` half of
/// `fill_partioners`/`fill_n_partioners` (the `make_fill_task` half is the
/// omission this file's own docs explain).
#[test]
fn flow_builder_fill_locks_and_runs_over_owned_data() {
    let data = over_owned_data(|taskflow, data| {
        taskflow.fill(
            data,
            9,
            GuidedPartitioner::<DefaultClosureWrapper>::default(),
        );
    });

    assert_all(&data, 9);
}

/// `FlowBuilder::fill_n` over owned, `Arc`-shared data.
#[test]
fn flow_builder_fill_n_locks_and_runs_over_owned_data() {
    let data = over_owned_data(|taskflow, data| {
        taskflow.fill_n(
            data,
            250,
            9,
            GuidedPartitioner::<DefaultClosureWrapper>::default(),
        );
    });

    assert_filled_to(&data, 250, 9, 0);
}

/// The closure wrapper runs once per worker task over that worker's whole
/// share, not once per element. Upstream's `test_fill.cpp` carries no
/// `ClosureWrapper.fill.*` case of its own (unlike `test_transform.cpp`'s);
/// this is the same shape `tests/for_each.rs`'s `closure_wrapper_for_each_static`
/// checks, over `fill` instead.
#[test]
fn closure_wrapper_runs_once_per_worker() {
    wrapper_runs_once_per_worker(|runtime, data, part| {
        runtime.fill(data, 1, part);
        assert!(data.iter().all(|&v| v == 1));
    });
}

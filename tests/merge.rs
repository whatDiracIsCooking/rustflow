//! Translated from `unittests/test_merge.cpp` (Taskflow v4.1.0).
//!
//! Upstream's `pm_basic` (the equal/unequal size sweep and the empty-input
//! cases), `pm_corank_boundary` (the `j == 0`/`i == 0` co-rank edges that
//! used to be an out-of-bounds read upstream) and `pm_custom_cmp` are
//! translated below, each swept over a worker matrix and checked against a
//! `std`-sorted concatenation, exactly as the issue's acceptance criteria
//! name. 1..=4 workers rather than upstream's 1..=8, for `tests/modules.rs`'s
//! reason: CI runs on about two cores, and the upper half of upstream's
//! sweep covers no shape the first four do not.
//!
//! What is not here:
//!
//! - **`pm_stateful`.** Upstream sets `std::ref`-wrapped iterators from a
//!   preceding init task so the merge task reads bounds that do not exist
//!   until the graph runs. [`rustflow::Runtime::merge`] already takes its
//!   slices straight from the enclosing runtime body's own stack frame --
//!   that is what `Runtime::scope` buys over `silent_async`, `src/algorithm/
//!   merge.rs`'s `# Design` section says -- so there is no later rebinding
//!   to stand in for.
//! - **`pm_async`/`pm_silent_async`/`pm_dependent_async`.** Those launch
//!   `tf::make_merge_task` from the async family rather than a graph node;
//!   [`make_merge_task_coverage`] below is this port's equivalent, over one
//!   representative size rather than upstream's full sweep, since it
//!   exercises the same [`rustflow::Runtime::merge_by`] call the sized sweep
//!   above already covers end to end.
//!
//! [`make_merge_task`]: rustflow::algorithm::merge::make_merge_task

use std::sync::{Arc, Mutex};

use rustflow::algorithm::merge::{make_merge_task, make_merge_task_by};
use rustflow::prelude::*;
use rustflow::{Executor, Xorshift32};

#[macro_use]
mod common;

/// Upstream's `d = rand() % 2000 - 1000`, sorted: `n` values in `[-1000,
/// 1000)`, ascending. `rng` is threaded through rather than reseeded per
/// call, so two sequential calls in one case never draw the same sequence.
fn random_sorted(rng: &mut Xorshift32, n: usize) -> Vec<i64> {
    let mut values: Vec<i64> = (0..n)
        .map(|_| i64::from(rng.next_u32() % 2000) - 1000)
        .collect();
    values.sort_unstable();
    values
}

/// The same draw, descending: [`random_sorted`] reversed rather than drawn
/// and sorted a second way, so the two orderings over one `n` share no
/// coincidental correlation with how `rng` advances.
fn random_sorted_desc(rng: &mut Xorshift32, n: usize) -> Vec<i64> {
    let mut values = random_sorted(rng, n);
    values.reverse();
    values
}

/// Upstream's ground truth, `std::merge(data1, data2, gt.begin())`, taken
/// instead the way the issue's acceptance criteria name it: a `std`-sorted
/// concatenation, which is the same multiset of values in the same order for
/// two inputs already sorted under `cmp`.
fn expected_merge<T: Ord + Clone>(a: &[T], b: &[T], ascending: bool) -> Vec<T> {
    let mut all: Vec<T> = a.iter().chain(b.iter()).cloned().collect();
    if ascending {
        all.sort();
    } else {
        all.sort_by(|x, y| y.cmp(x));
    }
    all
}

/// Upstream's `check_merge<T>`, over [`rustflow::Runtime::merge`]: builds two
/// sorted, random inputs of sizes `n1` and `n2`, merges them with `workers`
/// workers, and checks the result against [`expected_merge`].
fn check_runtime_merge(workers: usize, n1: usize, n2: usize, seed: u32) {
    let mut rng = Xorshift32::new(seed);
    let data1 = random_sorted(&mut rng, n1);
    let data2 = random_sorted(&mut rng, n2);
    let expected = expected_merge(&data1, &data2, true);

    let result = Arc::new(Mutex::new(Vec::new()));
    let published = Arc::clone(&result);

    let mut taskflow = Taskflow::new();
    taskflow.emplace_runtime(move |rt: &mut Runtime<'_>| {
        let mut out = vec![0_i64; data1.len() + data2.len()];
        rt.merge(&data1, &data2, &mut out);
        *published.lock().expect("uncontended") = out;
    });

    Executor::new(workers)
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    assert_eq!(
        *result.lock().expect("uncontended"),
        expected,
        "{workers} worker(s), n1={n1}, n2={n2}"
    );
}

/// Upstream's `pm_basic`: the equal-size sweep, the unequal sizes that
/// exercise co-rank's boundary conditions, and the explicit empty-input
/// cases.
fn pm_basic(workers: usize) {
    check_runtime_merge(workers, 0, 0, 1);
    check_runtime_merge(workers, 0, 100, 2);
    check_runtime_merge(workers, 100, 0, 3);
    check_runtime_merge(workers, 1, 1, 4);

    for n in [1, 2, 3, 7, 15, 99, 1000] {
        check_runtime_merge(workers, n, n, u32::try_from(n).expect("small n") + 10);
    }

    check_runtime_merge(workers, 1, 1000, 100);
    check_runtime_merge(workers, 1000, 1, 101);
    check_runtime_merge(workers, 3, 2000, 102);
    check_runtime_merge(workers, 2000, 3, 103);
}

/// Upstream's `pm_corank_boundary`: one input empty or far smaller than the
/// other, which used to reach `co_rank`'s `j == 0` (or `i == 0`) case with no
/// guard against the out-of-bounds read that case exists to avoid.
fn pm_corank_boundary(workers: usize) {
    check_runtime_merge(workers, 1000, 0, 200);
    check_runtime_merge(workers, 1, 0, 201);
    check_runtime_merge(workers, 100, 0, 202);

    check_runtime_merge(workers, 0, 1000, 203);
    check_runtime_merge(workers, 0, 1, 204);

    check_runtime_merge(workers, 1000, 1, 205);
    check_runtime_merge(workers, 2000, 1, 206);
    check_runtime_merge(workers, 1, 1000, 207);
    check_runtime_merge(workers, 1, 2000, 208);
}

/// Upstream's `pm_custom_cmp`: the same sweep under `std::greater<int>`,
/// which is [`Runtime::merge_by`] with `|x, y| x > y` here.
fn pm_custom_cmp(workers: usize) {
    for n in [0, 1, 99, 1000] {
        let seed = u32::try_from(n).expect("small n") + 300;

        let mut rng = Xorshift32::new(seed);
        let data1 = random_sorted_desc(&mut rng, n);
        let data2 = random_sorted_desc(&mut rng, n / 2 + 1);
        let expected = expected_merge(&data1, &data2, false);

        let result = Arc::new(Mutex::new(Vec::new()));
        let published = Arc::clone(&result);

        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(move |rt: &mut Runtime<'_>| {
            let mut out = vec![0_i64; data1.len() + data2.len()];
            rt.merge_by(&data1, &data2, &mut out, |x, y| x > y);
            *published.lock().expect("uncontended") = out;
        });

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(
            *result.lock().expect("uncontended"),
            expected,
            "{workers} worker(s), n={n}"
        );
    }
}

worker_matrix! {
    pm_basic {
        parallel_merge_basic_1worker(1),
        parallel_merge_basic_2workers(2),
        parallel_merge_basic_3workers(3),
        parallel_merge_basic_4workers(4),
    }
}

worker_matrix! {
    pm_corank_boundary {
        parallel_merge_corank_boundary_1worker(1),
        parallel_merge_corank_boundary_2workers(2),
        parallel_merge_corank_boundary_3workers(3),
        parallel_merge_corank_boundary_4workers(4),
    }
}

worker_matrix! {
    pm_custom_cmp {
        parallel_merge_custom_cmp_1worker(1),
        parallel_merge_custom_cmp_2workers(2),
        parallel_merge_custom_cmp_3workers(3),
        parallel_merge_custom_cmp_4workers(4),
    }
}

/// This port's equivalent of upstream's `pm_async`/`pm_silent_async`/
/// `pm_dependent_async`: [`make_merge_task`] and [`make_merge_task_by`], run
/// from the async family rather than from a graph, over one representative
/// size -- the module docs above say why not the full sweep.
#[test]
fn make_merge_task_coverage() {
    for workers in 1..=4 {
        let mut rng = Xorshift32::new(400);
        let data1 = random_sorted(&mut rng, 777);
        let data2 = random_sorted(&mut rng, 555);
        let expected = expected_merge(&data1, &data2, true);

        let executor = Executor::new(workers);
        let (body, result) = make_merge_task(data1, data2);
        executor.silent_async_runtime(body);
        executor.wait_for_all();

        assert_eq!(*result.lock().expect("uncontended"), expected);
    }

    for workers in 1..=4 {
        let mut rng = Xorshift32::new(401);
        let data1 = random_sorted_desc(&mut rng, 321);
        let data2 = random_sorted_desc(&mut rng, 456);
        let expected = expected_merge(&data1, &data2, false);

        let executor = Executor::new(workers);
        let (body, result) = make_merge_task_by(data1, data2, |x: &i64, y: &i64| x > y);
        executor.silent_async_runtime(body);
        executor.wait_for_all();

        assert_eq!(*result.lock().expect("uncontended"), expected);
    }
}

/// [`MergeBuilder::merge`] and [`MergeBuilder::merge_by`] as graph
/// nodes: the wrapper the issue's "Scope" section names, over the same
/// algorithm the sweeps above already check end to end.
#[test]
fn flow_builder_merge_coverage() {
    for workers in 1..=4 {
        let mut rng = Xorshift32::new(500);
        let data1 = random_sorted(&mut rng, 321);
        let data2 = random_sorted(&mut rng, 456);
        let expected = expected_merge(&data1, &data2, true);

        let mut taskflow = Taskflow::new();
        let (_task, result) = taskflow.merge(data1, data2);

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(*result.lock().expect("uncontended"), expected);
    }

    for workers in 1..=4 {
        let mut rng = Xorshift32::new(501);
        let data1 = random_sorted_desc(&mut rng, 222);
        let data2 = random_sorted_desc(&mut rng, 111);
        let expected = expected_merge(&data1, &data2, false);

        let mut taskflow = Taskflow::new();
        let (_task, result) = taskflow.merge_by(data1, data2, |x: &i64, y: &i64| x > y);

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(*result.lock().expect("uncontended"), expected);
    }
}

/// Empty inputs through every one of the three entry points, not only
/// [`Runtime::merge`]: zero-length `Vec<T>`s are the one case where
/// [`make_merge_task`]'s scratch buffer (`a.iter().chain(b.iter())`) has
/// nothing to clone from, which the module docs' `# Design` section argues
/// is still sound.
#[test]
fn empty_inputs_through_every_entry_point() {
    let executor = Executor::new(2);
    let (body, result) = make_merge_task::<i64>(Vec::new(), Vec::new());
    executor.silent_async_runtime(body);
    executor.wait_for_all();
    assert_eq!(*result.lock().expect("uncontended"), Vec::<i64>::new());

    let mut taskflow = Taskflow::new();
    let (_task, result) = taskflow.merge::<i64>(Vec::new(), Vec::new());
    Executor::new(2)
        .run(&taskflow)
        .wait()
        .expect("no task panicked");
    assert_eq!(*result.lock().expect("uncontended"), Vec::<i64>::new());
}

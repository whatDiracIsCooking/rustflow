//! Translated from the `find_if`, `find_if_not`, `min_element` and
//! `max_element` sections of `unittests/test_find.cpp` (Taskflow v4.1.0):
//! `find_if.*`, `find_if_not.*`, `min_element.*`, `max_element.*` and their
//! `ClosureWrapper.*` cases, together with the `silent_dependent_async`
//! section at the end of that file, which
//! `make_find_if_task_runs_from_a_silent_dependent_async_body` ports over
//! [`make_find_if_task`] -- the one form neither a `Runtime::` call nor an
//! [`AlgorithmBuilder`](rustflow::algorithm::AlgorithmBuilder) wrapper
//! reaches, since a dependent-async task is never queued into a `Taskflow`
//! for `emplace_runtime` to call. The `silent_async` section just before it
//! is not ported: it differs from that one in the spawning method alone,
//! which `tests/asyncs.rs` already covers for every body shape.
//!
//! Two adaptations, each forced by the port rather than chosen:
//!
//! - **One sweep per worker count, not a `rand()`-seeded 65536-length one.**
//!   Upstream's `test_find_if<P>(W)` runs every `n` up to `65536` and every
//!   chunk size in `{0, 1, 3, 7, 99}` against freshly randomized input,
//!   checked against `std::find_if` on the same vector. This suite checks
//!   the identical property -- the parallel answer matches a plain
//!   sequential scan -- over the representative `n`/chunk-size sweep this
//!   file's own `LENGTHS`/`CHUNK_SIZES` narrow to, with input drawn from
//!   [`Xorshift64`] rather than `rand()`, for the reason
//!   `src/algorithm/partitioner.rs`'s own `# Design` section gives for the
//!   same choice there: determinism costs nothing a correctness check needs.
//! - **The earliest-match race gets its own dedicated case**, not folded
//!   into the randomized sweep: `::rand()` input has only upstream's own two
//!   predicates (`== 5`, `== 0`) to match against, neither guaranteed to hit
//!   more than once, so nothing in upstream's own suite actually pins
//!   "earliest, not merely any" the way `find_if_reports_the_earliest_match`
//!   in `src/algorithm/find.rs` and this file's own
//!   `find_if_reports_the_earliest_of_several_known_matches` do, each against
//!   an input built to guarantee more than one match at known positions.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::Xorshift64;
use rustflow::algorithm::find::{FindPartitioner, make_find_if_task};
use rustflow::algorithm::partitioner::{
    DefaultClosureWrapper, DynamicPartitioner, GuidedPartitioner, RandomPartitioner,
    StaticPartitioner,
};
use rustflow::prelude::*;
use rustflow::{Executor, Runtime};

#[macro_use]
mod common;

/// The `n` side of every sweep below: a representative subset of upstream's
/// `0..=65536`, for the reason this file's own docs give.
const LENGTHS: [usize; 7] = [0, 1, 2, 7, 37, 150, 1000];

/// The chunk-size side of every sweep, upstream's own `{0, 1, 3, 7, 99}`
/// verbatim.
const CHUNK_SIZES: [usize; 5] = [0, 1, 3, 7, 99];

/// A deterministic stand-in for upstream's `::rand() % (2 * n) + 1`: every
/// value is in `[1, 2n]`, so `0` never appears and a predicate that looks
/// for it always reports "not found" -- upstream's own `P2` case.
fn random_input(n: usize, seed: u64) -> Vec<i64> {
    let mut rng = Xorshift64::new(seed);
    (0..n)
        .map(|_| {
            let span = u64::try_from(2 * n).unwrap_or(u64::MAX).max(1);
            i64::try_from(rng.next_u64() % span).expect("span fits in an i64") + 1
        })
        .collect()
}

// ----------------------------------------------------------------------------
// find_if
// ----------------------------------------------------------------------------

/// Upstream's `test_find_if<P>(W)`: a parallel [`Runtime::find_if`] over
/// randomized input must agree with [`Iterator::position`] on the same
/// vector, for a predicate that matches (`== 5`) and one that cannot
/// (`== 0`, since every value here is at least `1`) -- upstream's `res2 ==
/// input.end()` check.
fn find_if_case<P>(workers: usize, make: impl Fn(usize) -> P)
where
    P: FindPartitioner + Send + 'static,
{
    let executor = Executor::new(workers);

    for n in LENGTHS {
        for chunk_size in CHUNK_SIZES {
            let input = random_input(n, 0xF1D0_0001 ^ u64::try_from(n).unwrap_or(0));
            let expected_hit = input.iter().position(|&x| x == 5);
            let expected_miss = input.iter().position(|&x| x == 0);
            assert_eq!(expected_miss, None, "0 never appears in random_input");

            let mut taskflow = Taskflow::new();
            // `emplace_runtime` wants `FnMut`; this task runs exactly once
            // per `taskflow.clear()`-free iteration here, so the partitioner
            // is taken out of an `Option` rather than moved directly.
            let mut part = Some(make(chunk_size));
            let input_for_task = input.clone();
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let part = part.take().expect("this task runs once");
                let mut hit = None;
                let mut miss = None;
                runtime.find_if(&input_for_task, &mut hit, |&x| x == 5, part);
                runtime.find_if(
                    &input_for_task,
                    &mut miss,
                    |&x| x == 0,
                    StaticPartitioner::<DefaultClosureWrapper>::default(),
                );
                assert_eq!(hit, expected_hit, "n={n} chunk_size={chunk_size}");
                assert_eq!(miss, expected_miss, "n={n} chunk_size={chunk_size}");
            });

            executor.run(&taskflow).wait().expect("no task panicked");
        }
    }
}

fn find_if_static(workers: usize) {
    find_if_case(workers, StaticPartitioner::<DefaultClosureWrapper>::new);
}

fn find_if_guided(workers: usize) {
    find_if_case(workers, GuidedPartitioner::<DefaultClosureWrapper>::new);
}

fn find_if_dynamic(workers: usize) {
    find_if_case(workers, DynamicPartitioner::<DefaultClosureWrapper>::new);
}

fn find_if_random(workers: usize) {
    find_if_case(workers, RandomPartitioner::<DefaultClosureWrapper>::new);
}

worker_matrix! {
    find_if_static {
        find_if_static_1thread(1),
        find_if_static_2threads(2),
        find_if_static_3threads(3),
        find_if_static_4threads(4),
        find_if_static_5threads(5),
        find_if_static_6threads(6),
        find_if_static_7threads(7),
        find_if_static_8threads(8),
    }
}

worker_matrix! {
    find_if_guided {
        find_if_guided_1thread(1),
        find_if_guided_2threads(2),
        find_if_guided_3threads(3),
        find_if_guided_4threads(4),
        find_if_guided_5threads(5),
        find_if_guided_6threads(6),
        find_if_guided_7threads(7),
        find_if_guided_8threads(8),
    }
}

worker_matrix! {
    find_if_dynamic {
        find_if_dynamic_1thread(1),
        find_if_dynamic_2threads(2),
        find_if_dynamic_3threads(3),
        find_if_dynamic_4threads(4),
        find_if_dynamic_5threads(5),
        find_if_dynamic_6threads(6),
        find_if_dynamic_7threads(7),
        find_if_dynamic_8threads(8),
    }
}

worker_matrix! {
    find_if_random {
        find_if_random_1thread(1),
        find_if_random_2threads(2),
        find_if_random_3threads(3),
        find_if_random_4threads(4),
        find_if_random_5threads(5),
        find_if_random_6threads(6),
        find_if_random_7threads(7),
        find_if_random_8threads(8),
    }
}

// ----------------------------------------------------------------------------
// find_if_not
// ----------------------------------------------------------------------------

/// Upstream's `test_find_if_not<P>(W)`: input is either `5` or `0`, so
/// `find_if_not(== 5)` and `find_if_not(== 0)` are complements of each
/// other, both checked against [`Iterator::position`].
fn find_if_not_case<P>(workers: usize, make: impl Fn(usize) -> P)
where
    P: FindPartitioner + Send + 'static,
{
    let executor = Executor::new(workers);

    for n in LENGTHS {
        for chunk_size in CHUNK_SIZES {
            let mut rng = Xorshift64::new(0xF1D0_000F ^ u64::try_from(n).unwrap_or(0));
            let input: Vec<i64> = (0..n)
                .map(|_| {
                    let span = u64::try_from(n + 1).unwrap_or(1).max(1);
                    if rng.next_u64().is_multiple_of(span) {
                        0
                    } else {
                        5
                    }
                })
                .collect();
            let expected_not5 = input.iter().position(|&x| x != 5);
            let expected_not0 = input.iter().position(|&x| x != 0);

            let mut taskflow = Taskflow::new();
            let mut part = Some(make(chunk_size));
            let input_for_task = input.clone();
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let part = part.take().expect("this task runs once");
                let mut not5 = None;
                let mut not0 = None;
                runtime.find_if_not(&input_for_task, &mut not5, |&x| x == 5, part);
                runtime.find_if_not(
                    &input_for_task,
                    &mut not0,
                    |&x| x == 0,
                    StaticPartitioner::<DefaultClosureWrapper>::default(),
                );
                assert_eq!(not5, expected_not5, "n={n} chunk_size={chunk_size}");
                assert_eq!(not0, expected_not0, "n={n} chunk_size={chunk_size}");
            });

            executor.run(&taskflow).wait().expect("no task panicked");
        }
    }
}

fn find_if_not_static(workers: usize) {
    find_if_not_case(workers, StaticPartitioner::<DefaultClosureWrapper>::new);
}

fn find_if_not_guided(workers: usize) {
    find_if_not_case(workers, GuidedPartitioner::<DefaultClosureWrapper>::new);
}

fn find_if_not_dynamic(workers: usize) {
    find_if_not_case(workers, DynamicPartitioner::<DefaultClosureWrapper>::new);
}

fn find_if_not_random(workers: usize) {
    find_if_not_case(workers, RandomPartitioner::<DefaultClosureWrapper>::new);
}

worker_matrix! {
    find_if_not_static {
        find_if_not_static_1thread(1),
        find_if_not_static_2threads(2),
        find_if_not_static_3threads(3),
        find_if_not_static_4threads(4),
        find_if_not_static_5threads(5),
        find_if_not_static_6threads(6),
        find_if_not_static_7threads(7),
        find_if_not_static_8threads(8),
    }
}

worker_matrix! {
    find_if_not_guided {
        find_if_not_guided_1thread(1),
        find_if_not_guided_2threads(2),
        find_if_not_guided_3threads(3),
        find_if_not_guided_4threads(4),
        find_if_not_guided_5threads(5),
        find_if_not_guided_6threads(6),
        find_if_not_guided_7threads(7),
        find_if_not_guided_8threads(8),
    }
}

worker_matrix! {
    find_if_not_dynamic {
        find_if_not_dynamic_1thread(1),
        find_if_not_dynamic_2threads(2),
        find_if_not_dynamic_3threads(3),
        find_if_not_dynamic_4threads(4),
        find_if_not_dynamic_5threads(5),
        find_if_not_dynamic_6threads(6),
        find_if_not_dynamic_7threads(7),
        find_if_not_dynamic_8threads(8),
    }
}

worker_matrix! {
    find_if_not_random {
        find_if_not_random_1thread(1),
        find_if_not_random_2threads(2),
        find_if_not_random_3threads(3),
        find_if_not_random_4threads(4),
        find_if_not_random_5threads(5),
        find_if_not_random_6threads(6),
        find_if_not_random_7threads(7),
        find_if_not_random_8threads(8),
    }
}

// ----------------------------------------------------------------------------
// The earliest-match race, pinned directly
// ----------------------------------------------------------------------------

/// The correctness point #177 is about, over every partitioner: an input
/// with several known matches must report the earliest one regardless of
/// which chunk a worker happens to finish first. Sweeping worker counts and
/// chunk sizes inside one test is a stress case over how the chunks land,
/// not a repeat-count argument that the race is handled.
fn find_if_reports_the_earliest_of_several_known_matches<P>(make: impl Fn(usize) -> P)
where
    P: FindPartitioner + Send + 'static,
{
    let n = 5000;
    let matches = [3usize, 400, 401, 4000, 4999];
    let mut input = vec![0i32; n];
    for &at in &matches {
        input[at] = 1;
    }

    for workers in 1..=8usize {
        for chunk_size in [0usize, 1, 3, 11] {
            let mut taskflow = Taskflow::new();
            let input = input.clone();
            // `emplace_runtime` wants `FnMut`; this task runs exactly once
            // per `taskflow`, so the partitioner is taken out of an `Option`
            // rather than moved directly.
            let mut part = Some(make(chunk_size));
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let part = part.take().expect("this task runs once");
                let mut result = None;
                runtime.find_if(&input, &mut result, |&x| x == 1, part);
                assert_eq!(
                    result,
                    Some(matches[0]),
                    "workers={workers} chunk_size={chunk_size}"
                );
            });
            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");
        }
    }
}

#[test]
fn find_if_reports_the_earliest_match_static() {
    find_if_reports_the_earliest_of_several_known_matches(
        StaticPartitioner::<DefaultClosureWrapper>::new,
    );
}

#[test]
fn find_if_reports_the_earliest_match_guided() {
    find_if_reports_the_earliest_of_several_known_matches(
        GuidedPartitioner::<DefaultClosureWrapper>::new,
    );
}

#[test]
fn find_if_reports_the_earliest_match_dynamic() {
    find_if_reports_the_earliest_of_several_known_matches(
        DynamicPartitioner::<DefaultClosureWrapper>::new,
    );
}

#[test]
fn find_if_reports_the_earliest_match_random() {
    find_if_reports_the_earliest_of_several_known_matches(
        RandomPartitioner::<DefaultClosureWrapper>::new,
    );
}

// ----------------------------------------------------------------------------
// min_element / max_element
// ----------------------------------------------------------------------------

/// Upstream's `test_min_element<P>(W)` and `test_max_element<P>(W)` folded
/// into one case: both are checked against a sequential fold over the same
/// input, by value (ties are not pinned to an index, matching upstream's own
/// `*res_1 == min_element` value check rather than an iterator-identity one).
fn min_max_case<P>(workers: usize, make: impl Fn(usize) -> P)
where
    P: FindPartitioner + Send + 'static,
{
    let executor = Executor::new(workers);

    for n in LENGTHS {
        for chunk_size in CHUNK_SIZES {
            let input = random_input(n, 0xF1D0_00A0 ^ u64::try_from(n).unwrap_or(0));
            let expected_min = input.iter().min().copied();
            let expected_max = input.iter().max().copied();

            let mut taskflow = Taskflow::new();
            let mut part_min = Some(make(chunk_size));
            let mut part_max = Some(make(chunk_size));
            let input_for_task = input.clone();
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let part_min = part_min.take().expect("this task runs once");
                let part_max = part_max.take().expect("this task runs once");
                let mut min_result = None;
                let mut max_result = None;
                runtime.min_element(&input_for_task, &mut min_result, |a, b| a < b, part_min);
                runtime.max_element(&input_for_task, &mut max_result, |a, b| a < b, part_max);
                assert_eq!(
                    min_result.map(|i| input_for_task[i]),
                    expected_min,
                    "n={n} chunk_size={chunk_size}"
                );
                assert_eq!(
                    max_result.map(|i| input_for_task[i]),
                    expected_max,
                    "n={n} chunk_size={chunk_size}"
                );
                assert_eq!(min_result.is_none(), n == 0);
                assert_eq!(max_result.is_none(), n == 0);
            });

            executor.run(&taskflow).wait().expect("no task panicked");
        }
    }
}

fn min_max_static(workers: usize) {
    min_max_case(workers, StaticPartitioner::<DefaultClosureWrapper>::new);
}

fn min_max_guided(workers: usize) {
    min_max_case(workers, GuidedPartitioner::<DefaultClosureWrapper>::new);
}

fn min_max_dynamic(workers: usize) {
    min_max_case(workers, DynamicPartitioner::<DefaultClosureWrapper>::new);
}

fn min_max_random(workers: usize) {
    min_max_case(workers, RandomPartitioner::<DefaultClosureWrapper>::new);
}

worker_matrix! {
    min_max_static {
        min_max_static_1thread(1),
        min_max_static_2threads(2),
        min_max_static_3threads(3),
        min_max_static_4threads(4),
        min_max_static_5threads(5),
        min_max_static_6threads(6),
        min_max_static_7threads(7),
        min_max_static_8threads(8),
    }
}

worker_matrix! {
    min_max_guided {
        min_max_guided_1thread(1),
        min_max_guided_2threads(2),
        min_max_guided_3threads(3),
        min_max_guided_4threads(4),
        min_max_guided_5threads(5),
        min_max_guided_6threads(6),
        min_max_guided_7threads(7),
        min_max_guided_8threads(8),
    }
}

worker_matrix! {
    min_max_dynamic {
        min_max_dynamic_1thread(1),
        min_max_dynamic_2threads(2),
        min_max_dynamic_3threads(3),
        min_max_dynamic_4threads(4),
        min_max_dynamic_5threads(5),
        min_max_dynamic_6threads(6),
        min_max_dynamic_7threads(7),
        min_max_dynamic_8threads(8),
    }
}

worker_matrix! {
    min_max_random {
        min_max_random_1thread(1),
        min_max_random_2threads(2),
        min_max_random_3threads(3),
        min_max_random_4threads(4),
        min_max_random_5threads(5),
        min_max_random_6threads(6),
        min_max_random_7threads(7),
        min_max_random_8threads(8),
    }
}

// ----------------------------------------------------------------------------
// Closure Wrapper
// ----------------------------------------------------------------------------

const UPPER: usize = 1000;

/// Upstream's `ClosureWrapper.find.Static`: with a chunk size of `1` every
/// worker gets exactly one chunk, so the wrapper runs exactly `tc` times.
#[test]
fn closure_wrapper_find_static() {
    for tc in 1..16usize {
        let wrapper_called = std::sync::Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();
        let wrapper_called_body = std::sync::Arc::clone(&wrapper_called);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let range: Vec<i32> = (0..i32::try_from(UPPER).unwrap()).collect();
            let wrapper_called = std::sync::Arc::clone(&wrapper_called_body);
            let wrapper = move |task: &mut dyn FnMut()| {
                wrapper_called.fetch_add(1, Ordering::Relaxed);
                task();
            };
            let mut result = None;
            runtime.find_if(
                &range,
                &mut result,
                |&x| x == 500,
                StaticPartitioner::with_closure_wrapper(1, wrapper),
            );
            assert_eq!(result, Some(500));
        });
        Executor::new(tc)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert_eq!(wrapper_called.load(Ordering::Relaxed), tc, "tc={tc}");
    }
}

/// Upstream's `ClosureWrapper.find.Dynamic`: the dynamic family always
/// spawns exactly `tc` tasks for find (unlike the non-breaking algorithms),
/// but the bar this file holds to is upstream's own `<=`.
#[test]
fn closure_wrapper_find_dynamic() {
    for tc in 1..16usize {
        let wrapper_called = std::sync::Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();
        let wrapper_called_body = std::sync::Arc::clone(&wrapper_called);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let range: Vec<i32> = (0..i32::try_from(UPPER).unwrap()).collect();
            let wrapper_called = std::sync::Arc::clone(&wrapper_called_body);
            let wrapper = move |task: &mut dyn FnMut()| {
                wrapper_called.fetch_add(1, Ordering::Relaxed);
                task();
            };
            let mut result = None;
            runtime.find_if(
                &range,
                &mut result,
                |&x| x == 500,
                DynamicPartitioner::with_closure_wrapper(1, wrapper),
            );
            assert_eq!(result, Some(500));
        });
        Executor::new(tc)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert!(wrapper_called.load(Ordering::Relaxed) <= tc, "tc={tc}");
    }
}

/// Upstream's `ClosureWrapper.find_if_not.Static`.
#[test]
fn closure_wrapper_find_if_not_static() {
    for tc in 1..16usize {
        let wrapper_called = std::sync::Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();
        let wrapper_called_body = std::sync::Arc::clone(&wrapper_called);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let range: Vec<i32> = (0..i32::try_from(UPPER).unwrap()).collect();
            let wrapper_called = std::sync::Arc::clone(&wrapper_called_body);
            let wrapper = move |task: &mut dyn FnMut()| {
                wrapper_called.fetch_add(1, Ordering::Relaxed);
                task();
            };
            let mut result = None;
            runtime.find_if_not(
                &range,
                &mut result,
                |&x| x != 500,
                StaticPartitioner::with_closure_wrapper(1, wrapper),
            );
            assert_eq!(result, Some(500));
        });
        Executor::new(tc)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert_eq!(wrapper_called.load(Ordering::Relaxed), tc, "tc={tc}");
    }
}

/// Upstream's `ClosureWrapper.find_if_not.Dynamic`.
#[test]
fn closure_wrapper_find_if_not_dynamic() {
    for tc in 1..16usize {
        let wrapper_called = std::sync::Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();
        let wrapper_called_body = std::sync::Arc::clone(&wrapper_called);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let range: Vec<i32> = (0..i32::try_from(UPPER).unwrap()).collect();
            let wrapper_called = std::sync::Arc::clone(&wrapper_called_body);
            let wrapper = move |task: &mut dyn FnMut()| {
                wrapper_called.fetch_add(1, Ordering::Relaxed);
                task();
            };
            let mut result = None;
            runtime.find_if_not(
                &range,
                &mut result,
                |&x| x != 500,
                DynamicPartitioner::with_closure_wrapper(1, wrapper),
            );
            assert_eq!(result, Some(500));
        });
        Executor::new(tc)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert!(wrapper_called.load(Ordering::Relaxed) <= tc, "tc={tc}");
    }
}

/// Upstream's `ClosureWrapper.min_element.Static`.
#[test]
fn closure_wrapper_min_element_static() {
    for tc in 1..16usize {
        let wrapper_called = std::sync::Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();
        let wrapper_called_body = std::sync::Arc::clone(&wrapper_called);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut range: Vec<i32> = (0..i32::try_from(UPPER).unwrap()).collect();
            range[500] = -10;
            let wrapper_called = std::sync::Arc::clone(&wrapper_called_body);
            let wrapper = move |task: &mut dyn FnMut()| {
                wrapper_called.fetch_add(1, Ordering::Relaxed);
                task();
            };
            let mut result = None;
            runtime.min_element(
                &range,
                &mut result,
                |a: &i32, b: &i32| a < b,
                StaticPartitioner::with_closure_wrapper(1, wrapper),
            );
            assert_eq!(result.map(|i| range[i]), Some(-10));
        });
        Executor::new(tc)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert_eq!(wrapper_called.load(Ordering::Relaxed), tc, "tc={tc}");
    }
}

/// Upstream's `ClosureWrapper.min_element.Dynamic`.
#[test]
fn closure_wrapper_min_element_dynamic() {
    for tc in 1..16usize {
        let wrapper_called = std::sync::Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();
        let wrapper_called_body = std::sync::Arc::clone(&wrapper_called);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut range: Vec<i32> = (0..i32::try_from(UPPER).unwrap()).collect();
            range[500] = -10;
            let wrapper_called = std::sync::Arc::clone(&wrapper_called_body);
            let wrapper = move |task: &mut dyn FnMut()| {
                wrapper_called.fetch_add(1, Ordering::Relaxed);
                task();
            };
            let mut result = None;
            runtime.min_element(
                &range,
                &mut result,
                |a: &i32, b: &i32| a < b,
                DynamicPartitioner::with_closure_wrapper(1, wrapper),
            );
            assert_eq!(result.map(|i| range[i]), Some(-10));
        });
        Executor::new(tc)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert!(wrapper_called.load(Ordering::Relaxed) <= tc, "tc={tc}");
    }
}

/// Upstream's `ClosureWrapper.max_element.Static`.
#[test]
fn closure_wrapper_max_element_static() {
    for tc in 1..16usize {
        let wrapper_called = std::sync::Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();
        let wrapper_called_body = std::sync::Arc::clone(&wrapper_called);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut range: Vec<i32> = (0..i32::try_from(UPPER).unwrap()).collect();
            range[500] = 1_000_000;
            let wrapper_called = std::sync::Arc::clone(&wrapper_called_body);
            let wrapper = move |task: &mut dyn FnMut()| {
                wrapper_called.fetch_add(1, Ordering::Relaxed);
                task();
            };
            let mut result = None;
            runtime.max_element(
                &range,
                &mut result,
                |a: &i32, b: &i32| a < b,
                StaticPartitioner::with_closure_wrapper(1, wrapper),
            );
            assert_eq!(result.map(|i| range[i]), Some(1_000_000));
        });
        Executor::new(tc)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert_eq!(wrapper_called.load(Ordering::Relaxed), tc, "tc={tc}");
    }
}

/// Upstream's `ClosureWrapper.max_element.Dynamic`.
#[test]
fn closure_wrapper_max_element_dynamic() {
    for tc in 1..16usize {
        let wrapper_called = std::sync::Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();
        let wrapper_called_body = std::sync::Arc::clone(&wrapper_called);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut range: Vec<i32> = (0..i32::try_from(UPPER).unwrap()).collect();
            range[500] = 1_000_000;
            let wrapper_called = std::sync::Arc::clone(&wrapper_called_body);
            let wrapper = move |task: &mut dyn FnMut()| {
                wrapper_called.fetch_add(1, Ordering::Relaxed);
                task();
            };
            let mut result = None;
            runtime.max_element(
                &range,
                &mut result,
                |a: &i32, b: &i32| a < b,
                DynamicPartitioner::with_closure_wrapper(1, wrapper),
            );
            assert_eq!(result.map(|i| range[i]), Some(1_000_000));
        });
        Executor::new(tc)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert!(wrapper_called.load(Ordering::Relaxed) <= tc, "tc={tc}");
    }
}

// ----------------------------------------------------------------------------
// silent dependent async
// ----------------------------------------------------------------------------

/// Upstream's `silent_dependent_async(W)`: two `make_find_if_task` bodies
/// handed straight to an executor, the second waiting on the first, with
/// `wait_for_all` the only handle on either -- the submitting form no
/// `Runtime::find_if` call and no `AlgorithmBuilder::find_if` wrapper can
/// reach.
///
/// Narrowed the two ways this file's own docs describe for every case above:
/// one deterministic input per `n` in `LENGTHS` rather than a `rand()`-seeded
/// sweep to `65536`, and two worker counts rather than eight, since what is
/// new here is the submitting form and not the search.
#[test]
fn make_find_if_task_runs_from_a_silent_dependent_async_body() {
    for workers in [1, 4] {
        let executor = Executor::new(workers);

        for n in LENGTHS {
            let input = Arc::new(random_input(n, 0xF1D0_0295 ^ u64::try_from(n).unwrap_or(0)));
            let expected_hit = input.iter().position(|&x| x == 5);
            let hit = Arc::new(Mutex::new(None));
            let miss = Arc::new(Mutex::new(None));

            let first = executor.silent_dependent_async_runtime(
                make_find_if_task(
                    Arc::clone(&input),
                    Arc::clone(&hit),
                    |&x: &i64| x == 5,
                    GuidedPartitioner::<DefaultClosureWrapper>::default(),
                ),
                [],
            );
            executor.silent_dependent_async_runtime(
                make_find_if_task(
                    Arc::clone(&input),
                    Arc::clone(&miss),
                    |&x: &i64| x == 0,
                    GuidedPartitioner::<DefaultClosureWrapper>::default(),
                ),
                [&first],
            );
            executor.wait_for_all();

            assert_eq!(
                *hit.lock().expect("uncontended"),
                expected_hit,
                "workers={workers} n={n}"
            );
            assert_eq!(
                *miss.lock().expect("uncontended"),
                None,
                "workers={workers} n={n}: 0 never appears in random_input"
            );
        }
    }
}

// ----------------------------------------------------------------------------
// the owned-data builder forms
// ----------------------------------------------------------------------------

/// `AlgorithmBuilder::find_if_not` over owned data: the `Taskflow::` form of
/// the search, which takes an `Arc<Vec<_>>` and publishes into an
/// `Arc<Mutex<_>>` instead of borrowing a slice and an output the way every
/// `Runtime::find_if_not` case above does.
///
/// The index it publishes is the *first* the predicate rejects, not merely
/// one of them: the input rejects at index 4 and again at 6, so a scan that
/// reported either would pass a weaker assertion than this one. An input the
/// predicate holds for everywhere publishes `None`.
#[test]
fn the_builder_find_if_not_reports_the_first_rejected_index() {
    let data = Arc::new(vec![5, 5, 5, 5, 7, 5, 9]);
    let rejected = Arc::new(Mutex::new(None));
    let all_fives = Arc::new(vec![5; 64]);
    let none = Arc::new(Mutex::new(Some(99)));

    let mut taskflow = Taskflow::new();
    taskflow.find_if_not(
        Arc::clone(&data),
        Arc::clone(&rejected),
        |&x: &i32| x == 5,
        GuidedPartitioner::<DefaultClosureWrapper>::default(),
    );
    taskflow.find_if_not(
        Arc::clone(&all_fives),
        Arc::clone(&none),
        |&x: &i32| x == 5,
        GuidedPartitioner::<DefaultClosureWrapper>::default(),
    );

    Executor::new(3)
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    assert_eq!(*rejected.lock().expect("uncontended"), Some(4));
    assert_eq!(*none.lock().expect("uncontended"), None);
}

/// `AlgorithmBuilder::min_element` and `AlgorithmBuilder::max_element` over
/// owned data, the `Taskflow::` forms of the fold the `min_max_case` sweep
/// above runs through `Runtime::`.
///
/// Handed the same input and the same `a < b`, the two publish indices of
/// opposite ends of it -- which is the pair's whole content, `max_element`
/// being `min_element` with `cmp` flipped. The input's extremes are at known
/// positions and appear once each, so the published index is pinned and not
/// just the value under it.
#[test]
fn the_builder_min_and_max_element_report_opposite_ends() {
    let data = Arc::new(vec![4, 1, 7, 9, 2]);
    let smallest = Arc::new(Mutex::new(None));
    let largest = Arc::new(Mutex::new(None));

    let mut taskflow = Taskflow::new();
    taskflow.min_element(
        Arc::clone(&data),
        Arc::clone(&smallest),
        |a: &i32, b: &i32| a < b,
        GuidedPartitioner::<DefaultClosureWrapper>::default(),
    );
    taskflow.max_element(
        Arc::clone(&data),
        Arc::clone(&largest),
        |a: &i32, b: &i32| a < b,
        GuidedPartitioner::<DefaultClosureWrapper>::default(),
    );

    Executor::new(3)
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    assert_eq!(*smallest.lock().expect("uncontended"), Some(1));
    assert_eq!(*largest.lock().expect("uncontended"), Some(3));
}

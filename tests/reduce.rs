//! Translated from `unittests/test_reduce.cpp` (Taskflow v4.1.0).
//!
//! `ReduceMin.*` and `ReduceSum.*` are [`reduce_min`] and [`reduce_sum`];
//! `TransformReduce.*` is [`transform_reduce`]; the binary
//! `TransformReduceBinary.*` (upstream's own test names call it
//! `BinaryTransformReduce`) is [`binary_transform_reduce`];
//! `ReduceByIndex*.*` (upstream's `ReduceByIndexSum`) is
//! [`reduce_by_index_sum`]. Each is swept over every partitioner family and
//! 1..=4 workers -- `tests/merge.rs`'s own doc comment says why 4 rather
//! than upstream's 8: CI runs on about two cores, and the upper half of
//! upstream's sweep covers no shape the first four do not.
//!
//! Each of those sweep bodies builds its [`Executor`] once and runs the whole
//! sweep on it, which is where upstream puts its own `tf::Executor
//! executor(W);` -- above the same double loop, not inside it. A pool per
//! iteration is 2495 thread pools spawned and joined per case, and what that
//! buys is wall clock rather than coverage: every iteration already builds
//! its own taskflow, partitioner and expected answer, and the sweep asserts
//! nothing about a worker pool's first run.
//!
//! `ClosureWrapper.Reduce.*` and `ClosureWrapper.TransformReduce.*` are
//! [`closure_wrapper_reduce`] and [`closure_wrapper_transform_reduce`]: the
//! counting-wrapper case the issue asks for, pinning that the mutex behind
//! [`rustflow::algorithm::reduce::ReducePartitioner::drive_fold`] is taken
//! once per worker *task*, never once per chunk or per element.
//!
//! What is not here: the `TransformReduce.MoveOnlyData.*` cases.
//! `src/algorithm/reduce.rs`'s own `# Design` section says why --
//! `data: &[T]` is borrowed, not consumed, so this port's `reduce` needs
//! `T: Clone` where upstream's needs nothing, and the move-only cases exist
//! upstream only to prove the opposite.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::algorithm::index_range::IndexRange;
use rustflow::algorithm::partitioner::{
    DefaultClosureWrapper, DynamicPartitioner, GuidedPartitioner, RandomPartitioner,
    StaticPartitioner,
};
use rustflow::algorithm::reduce::ReducePartitioner;
use rustflow::prelude::*;
use rustflow::{Executor, Runtime, Xorshift32};

#[macro_use]
mod common;

/// Upstream's `std::rand() % 100 - 50`, `n` values: the same spread
/// `ReduceMin`/`ReduceSum` draw from, seeded rather than reseeded from the
/// OS so a failing case is reproducible.
fn random_values(rng: &mut Xorshift32, n: usize) -> Vec<i64> {
    (0..n)
        .map(|_| i64::from(rng.next_u32() % 100) - 50)
        .collect()
}

/// Upstream's `reduce_min`: [`Runtime::reduce`] with a `bop` that has no
/// identity (`min`), checked against a sequential scan, over every prefix
/// length from 1 to `vec.len()` and a sweep of chunk sizes -- the "`bop`
/// never sees a fabricated identity" property the issue asks for, since
/// every local here is seeded from a real element of `data`, not
/// `i64::MAX`.
fn reduce_min<P>(workers: usize, make_part: impl Fn(usize) -> P)
where
    P: ReducePartitioner + Send + 'static,
{
    let mut rng = Xorshift32::new(7);
    let vec = random_values(&mut rng, 500);
    let executor = Executor::new(workers);

    for n in 1..vec.len() {
        for chunk_size in [0usize, 1, 3, 7, 99] {
            let prefix = vec[..n].to_vec();
            let want = *prefix.iter().min().expect("non-empty prefix");
            let part = make_part(chunk_size);

            let mut taskflow = Taskflow::new();
            let mut part = Some(part);
            taskflow.emplace_runtime(move |rt: &mut Runtime<'_>| {
                let part = part.take().expect("this task runs once");
                let mut pmin = i64::MAX;
                rt.reduce(
                    &prefix,
                    &mut pmin,
                    |a: &i64, b: &i64| if *a < *b { *a } else { *b },
                    part,
                );
                assert_eq!(pmin, want, "n={n} chunk_size={chunk_size}");
            });

            executor.run(&taskflow).wait().expect("no task panicked");
        }
    }
}

/// Upstream's `reduce_sum`: [`Runtime::reduce`] with plain addition, checked
/// against a sequential sum.
fn reduce_sum<P>(workers: usize, make_part: impl Fn(usize) -> P)
where
    P: ReducePartitioner + Send + 'static,
{
    let mut rng = Xorshift32::new(11);
    let vec = random_values(&mut rng, 500);
    let executor = Executor::new(workers);

    for n in 1..vec.len() {
        for chunk_size in [0usize, 1, 3, 7, 99] {
            let prefix = vec[..n].to_vec();
            let want: i64 = prefix.iter().sum();
            let part = make_part(chunk_size);

            let mut taskflow = Taskflow::new();
            let mut part = Some(part);
            taskflow.emplace_runtime(move |rt: &mut Runtime<'_>| {
                let part = part.take().expect("this task runs once");
                let mut sol = 0_i64;
                rt.reduce(&prefix, &mut sol, |a: &i64, b: &i64| a + b, part);
                assert_eq!(sol, want, "n={n} chunk_size={chunk_size}");
            });

            executor.run(&taskflow).wait().expect("no task panicked");
        }
    }
}

/// Upstream's `transform_reduce`: `uop` extracts a value, `bop` folds `min`
/// over it.
fn transform_reduce<P>(workers: usize, make_part: impl Fn(usize) -> P)
where
    P: ReducePartitioner + Send + 'static,
{
    let mut rng = Xorshift32::new(13);
    let vec = random_values(&mut rng, 500);
    let executor = Executor::new(workers);

    for n in 1..vec.len() {
        for chunk_size in [0usize, 1, 3, 7, 99] {
            let prefix = vec[..n].to_vec();
            let want = *prefix.iter().min().expect("non-empty prefix");
            let part = make_part(chunk_size);

            let mut taskflow = Taskflow::new();
            let mut part = Some(part);
            taskflow.emplace_runtime(move |rt: &mut Runtime<'_>| {
                let part = part.take().expect("this task runs once");
                let mut pmin = i64::MAX;
                rt.transform_reduce(
                    &prefix,
                    &mut pmin,
                    |a: &i64, b: &i64| if *a < *b { *a } else { *b },
                    |x: &i64| *x,
                    part,
                );
                assert_eq!(pmin, want, "n={n} chunk_size={chunk_size}");
            });

            executor.run(&taskflow).wait().expect("no task panicked");
        }
    }
}

/// Upstream's `binary_transform_reduce` (test names: `BinaryTransformReduce`;
/// the issue's own acceptance names it `TransformReduceBinary`): two inputs,
/// `uop` takes `min` of the pair, `bop` folds `min` over that.
fn binary_transform_reduce<P>(workers: usize, make_part: impl Fn(usize) -> P)
where
    P: ReducePartitioner + Send + 'static,
{
    let mut rng = Xorshift32::new(17);
    let vec1 = random_values(&mut rng, 500);
    let vec2 = random_values(&mut rng, 500);
    let executor = Executor::new(workers);

    for n in 1..vec1.len() {
        for chunk_size in [0usize, 1, 3, 7, 99] {
            let prefix1 = vec1[..n].to_vec();
            let prefix2 = vec2[..n].to_vec();
            let want = prefix1
                .iter()
                .chain(prefix2.iter())
                .min()
                .copied()
                .expect("non-empty prefix");
            let part = make_part(chunk_size);

            let mut taskflow = Taskflow::new();
            let mut part = Some(part);
            taskflow.emplace_runtime(move |rt: &mut Runtime<'_>| {
                let part = part.take().expect("this task runs once");
                let mut pmin = i64::MAX;
                rt.transform_reduce2(
                    &prefix1,
                    &prefix2,
                    &mut pmin,
                    |a: &i64, b: &i64| if *a < *b { *a } else { *b },
                    |x: &i64, y: &i64| if *x < *y { *x } else { *y },
                    part,
                );
                assert_eq!(pmin, want, "n={n} chunk_size={chunk_size}");
            });

            executor.run(&taskflow).wait().expect("no task panicked");
        }
    }
}

/// Upstream's `reduce_by_index_sum`: `lop` sums its own sub-range (seeded
/// from `running_total.unwrap_or(0)`, never a fabricated identity of the
/// *final* answer), `gop` folds the locals, starting from a non-zero `sol`.
fn reduce_by_index_sum<P>(workers: usize, make_part: impl Fn(usize) -> P)
where
    P: ReducePartitioner + Send + 'static,
{
    let mut rng = Xorshift32::new(19);
    let vec = random_values(&mut rng, 500);
    let executor = Executor::new(workers);

    for n in 1..vec.len() {
        for chunk_size in [0usize, 1, 3, 7, 99] {
            let prefix = vec[..n].to_vec();
            let want: i64 = 10 + prefix.iter().sum::<i64>();
            let part = make_part(chunk_size);

            let mut taskflow = Taskflow::new();
            let mut part = Some(part);
            taskflow.emplace_runtime(move |rt: &mut Runtime<'_>| {
                let part = part.take().expect("this task runs once");
                let mut sol = 10_i64;
                rt.reduce_by_index(
                    IndexRange::new(0usize, prefix.len(), 1),
                    &mut sol,
                    |sub: IndexRange<usize>, running: Option<i64>| {
                        let mut lsum = running.unwrap_or(0);
                        for i in (sub.begin()..sub.end()).step_by(sub.step_size()) {
                            lsum += prefix[i];
                        }
                        lsum
                    },
                    |a: &i64, b: &i64| a + b,
                    part,
                );
                assert_eq!(sol, want, "n={n} chunk_size={chunk_size}");
            });

            executor.run(&taskflow).wait().expect("no task panicked");
        }
    }
}

fn reduce_min_static(workers: usize) {
    reduce_min(workers, StaticPartitioner::<DefaultClosureWrapper>::new);
}
fn reduce_min_guided(workers: usize) {
    reduce_min(workers, GuidedPartitioner::<DefaultClosureWrapper>::new);
}
fn reduce_min_dynamic(workers: usize) {
    reduce_min(workers, DynamicPartitioner::<DefaultClosureWrapper>::new);
}
fn reduce_min_random(workers: usize) {
    reduce_min(workers, RandomPartitioner::<DefaultClosureWrapper>::new);
}

worker_matrix! {
    reduce_min_static {
        reduce_min_static_1worker(1),
        reduce_min_static_2workers(2),
        reduce_min_static_3workers(3),
        reduce_min_static_4workers(4),
    }
}
worker_matrix! {
    reduce_min_guided {
        reduce_min_guided_1worker(1),
        reduce_min_guided_2workers(2),
        reduce_min_guided_3workers(3),
        reduce_min_guided_4workers(4),
    }
}
worker_matrix! {
    reduce_min_dynamic {
        reduce_min_dynamic_1worker(1),
        reduce_min_dynamic_2workers(2),
        reduce_min_dynamic_3workers(3),
        reduce_min_dynamic_4workers(4),
    }
}
worker_matrix! {
    reduce_min_random {
        reduce_min_random_1worker(1),
        reduce_min_random_2workers(2),
        reduce_min_random_3workers(3),
        reduce_min_random_4workers(4),
    }
}

fn reduce_sum_static(workers: usize) {
    reduce_sum(workers, StaticPartitioner::<DefaultClosureWrapper>::new);
}
fn reduce_sum_guided(workers: usize) {
    reduce_sum(workers, GuidedPartitioner::<DefaultClosureWrapper>::new);
}
fn reduce_sum_dynamic(workers: usize) {
    reduce_sum(workers, DynamicPartitioner::<DefaultClosureWrapper>::new);
}
fn reduce_sum_random(workers: usize) {
    reduce_sum(workers, RandomPartitioner::<DefaultClosureWrapper>::new);
}

worker_matrix! {
    reduce_sum_static {
        reduce_sum_static_1worker(1),
        reduce_sum_static_2workers(2),
        reduce_sum_static_3workers(3),
        reduce_sum_static_4workers(4),
    }
}
worker_matrix! {
    reduce_sum_guided {
        reduce_sum_guided_1worker(1),
        reduce_sum_guided_2workers(2),
        reduce_sum_guided_3workers(3),
        reduce_sum_guided_4workers(4),
    }
}
worker_matrix! {
    reduce_sum_dynamic {
        reduce_sum_dynamic_1worker(1),
        reduce_sum_dynamic_2workers(2),
        reduce_sum_dynamic_3workers(3),
        reduce_sum_dynamic_4workers(4),
    }
}
worker_matrix! {
    reduce_sum_random {
        reduce_sum_random_1worker(1),
        reduce_sum_random_2workers(2),
        reduce_sum_random_3workers(3),
        reduce_sum_random_4workers(4),
    }
}

fn transform_reduce_static(workers: usize) {
    transform_reduce(workers, StaticPartitioner::<DefaultClosureWrapper>::new);
}
fn transform_reduce_guided(workers: usize) {
    transform_reduce(workers, GuidedPartitioner::<DefaultClosureWrapper>::new);
}
fn transform_reduce_dynamic(workers: usize) {
    transform_reduce(workers, DynamicPartitioner::<DefaultClosureWrapper>::new);
}
fn transform_reduce_random(workers: usize) {
    transform_reduce(workers, RandomPartitioner::<DefaultClosureWrapper>::new);
}

worker_matrix! {
    transform_reduce_static {
        transform_reduce_static_1worker(1),
        transform_reduce_static_2workers(2),
        transform_reduce_static_3workers(3),
        transform_reduce_static_4workers(4),
    }
}
worker_matrix! {
    transform_reduce_guided {
        transform_reduce_guided_1worker(1),
        transform_reduce_guided_2workers(2),
        transform_reduce_guided_3workers(3),
        transform_reduce_guided_4workers(4),
    }
}
worker_matrix! {
    transform_reduce_dynamic {
        transform_reduce_dynamic_1worker(1),
        transform_reduce_dynamic_2workers(2),
        transform_reduce_dynamic_3workers(3),
        transform_reduce_dynamic_4workers(4),
    }
}
worker_matrix! {
    transform_reduce_random {
        transform_reduce_random_1worker(1),
        transform_reduce_random_2workers(2),
        transform_reduce_random_3workers(3),
        transform_reduce_random_4workers(4),
    }
}

fn binary_transform_reduce_static(workers: usize) {
    binary_transform_reduce(workers, StaticPartitioner::<DefaultClosureWrapper>::new);
}
fn binary_transform_reduce_guided(workers: usize) {
    binary_transform_reduce(workers, GuidedPartitioner::<DefaultClosureWrapper>::new);
}
fn binary_transform_reduce_dynamic(workers: usize) {
    binary_transform_reduce(workers, DynamicPartitioner::<DefaultClosureWrapper>::new);
}
fn binary_transform_reduce_random(workers: usize) {
    binary_transform_reduce(workers, RandomPartitioner::<DefaultClosureWrapper>::new);
}

worker_matrix! {
    binary_transform_reduce_static {
        binary_transform_reduce_static_1worker(1),
        binary_transform_reduce_static_2workers(2),
        binary_transform_reduce_static_3workers(3),
        binary_transform_reduce_static_4workers(4),
    }
}
worker_matrix! {
    binary_transform_reduce_guided {
        binary_transform_reduce_guided_1worker(1),
        binary_transform_reduce_guided_2workers(2),
        binary_transform_reduce_guided_3workers(3),
        binary_transform_reduce_guided_4workers(4),
    }
}
worker_matrix! {
    binary_transform_reduce_dynamic {
        binary_transform_reduce_dynamic_1worker(1),
        binary_transform_reduce_dynamic_2workers(2),
        binary_transform_reduce_dynamic_3workers(3),
        binary_transform_reduce_dynamic_4workers(4),
    }
}
worker_matrix! {
    binary_transform_reduce_random {
        binary_transform_reduce_random_1worker(1),
        binary_transform_reduce_random_2workers(2),
        binary_transform_reduce_random_3workers(3),
        binary_transform_reduce_random_4workers(4),
    }
}

fn reduce_by_index_sum_static(workers: usize) {
    reduce_by_index_sum(workers, StaticPartitioner::<DefaultClosureWrapper>::new);
}
fn reduce_by_index_sum_guided(workers: usize) {
    reduce_by_index_sum(workers, GuidedPartitioner::<DefaultClosureWrapper>::new);
}
fn reduce_by_index_sum_dynamic(workers: usize) {
    reduce_by_index_sum(workers, DynamicPartitioner::<DefaultClosureWrapper>::new);
}
fn reduce_by_index_sum_random(workers: usize) {
    reduce_by_index_sum(workers, RandomPartitioner::<DefaultClosureWrapper>::new);
}

worker_matrix! {
    reduce_by_index_sum_static {
        reduce_by_index_sum_static_1worker(1),
        reduce_by_index_sum_static_2workers(2),
        reduce_by_index_sum_static_3workers(3),
        reduce_by_index_sum_static_4workers(4),
    }
}
worker_matrix! {
    reduce_by_index_sum_guided {
        reduce_by_index_sum_guided_1worker(1),
        reduce_by_index_sum_guided_2workers(2),
        reduce_by_index_sum_guided_3workers(3),
        reduce_by_index_sum_guided_4workers(4),
    }
}
worker_matrix! {
    reduce_by_index_sum_dynamic {
        reduce_by_index_sum_dynamic_1worker(1),
        reduce_by_index_sum_dynamic_2workers(2),
        reduce_by_index_sum_dynamic_3workers(3),
        reduce_by_index_sum_dynamic_4workers(4),
    }
}
worker_matrix! {
    reduce_by_index_sum_random {
        reduce_by_index_sum_random_1worker(1),
        reduce_by_index_sum_random_2workers(2),
        reduce_by_index_sum_random_3workers(3),
        reduce_by_index_sum_random_4workers(4),
    }
}

/// Upstream's `ClosureWrapper.Reduce.Static`/`.Dynamic`: the closure
/// wrapper -- and so the merge it wraps -- runs exactly once per worker
/// *task*, over that worker's whole share, for the static family; at most
/// once per worker for the dynamic family. This is the counting-wrapper
/// case the issue's acceptance names: it is what proves the per-chunk fold
/// stays local and only the final merge is locked, rather than every
/// element taking the lock.
fn closure_wrapper_reduce(workers: usize) {
    const UPPER: i64 = 1000;

    for chunk_size in [0usize, 1, 3, 7, 99] {
        let range: Vec<i64> = (0..UPPER).collect();

        let wrapper_called = Arc::new(AtomicUsize::new(0));
        let called = Arc::clone(&wrapper_called);
        let mut taskflow = Taskflow::new();
        let range_body = range.clone();
        taskflow.emplace_runtime(move |rt: &mut Runtime<'_>| {
            let called = Arc::clone(&called);
            let wrapper = move |task: &mut dyn FnMut()| {
                called.fetch_add(1, Ordering::Relaxed);
                task();
            };
            let mut result = 0_i64;
            rt.reduce(
                &range_body,
                &mut result,
                |a: &i64, b: &i64| a + b,
                StaticPartitioner::with_closure_wrapper(chunk_size, wrapper),
            );
            assert_eq!(result, (UPPER - 1) * UPPER / 2);
        });
        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert_eq!(wrapper_called.load(Ordering::Relaxed), workers);

        let wrapper_called = Arc::new(AtomicUsize::new(0));
        let called = Arc::clone(&wrapper_called);
        let mut taskflow = Taskflow::new();
        let range_body = range.clone();
        taskflow.emplace_runtime(move |rt: &mut Runtime<'_>| {
            let called = Arc::clone(&called);
            let wrapper = move |task: &mut dyn FnMut()| {
                called.fetch_add(1, Ordering::Relaxed);
                task();
            };
            let mut result = 0_i64;
            rt.reduce(
                &range_body,
                &mut result,
                |a: &i64, b: &i64| a + b,
                DynamicPartitioner::with_closure_wrapper(chunk_size, wrapper),
            );
            assert_eq!(result, (UPPER - 1) * UPPER / 2);
        });
        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert!(wrapper_called.load(Ordering::Relaxed) <= workers);
    }
}

worker_matrix! {
    closure_wrapper_reduce {
        closure_wrapper_reduce_1worker(1),
        closure_wrapper_reduce_2workers(2),
        closure_wrapper_reduce_3workers(3),
        closure_wrapper_reduce_4workers(4),
    }
}

/// Upstream's `ClosureWrapper.TransformReduce.*` and
/// `ClosureWrapper.TransformReduce2.*`: the same once-per-worker property,
/// for [`Runtime::transform_reduce`] and [`Runtime::transform_reduce2`].
fn closure_wrapper_transform_reduce(workers: usize) {
    const UPPER: i64 = 1000;
    let range: Vec<i64> = (0..UPPER).collect();

    let wrapper_called = Arc::new(AtomicUsize::new(0));
    let called = Arc::clone(&wrapper_called);
    let mut taskflow = Taskflow::new();
    let range_body = range.clone();
    taskflow.emplace_runtime(move |rt: &mut Runtime<'_>| {
        let called = Arc::clone(&called);
        let wrapper = move |task: &mut dyn FnMut()| {
            called.fetch_add(1, Ordering::Relaxed);
            task();
        };
        let mut result = 0_i64;
        rt.transform_reduce(
            &range_body,
            &mut result,
            |a: &i64, b: &i64| a + b,
            |x: &i64| -x,
            StaticPartitioner::with_closure_wrapper(1, wrapper),
        );
        assert_eq!(result, -((UPPER - 1) * UPPER / 2));
    });
    Executor::new(workers)
        .run(&taskflow)
        .wait()
        .expect("no task panicked");
    assert_eq!(wrapper_called.load(Ordering::Relaxed), workers);

    let range2: Vec<i64> = vec![-2; usize::try_from(UPPER).expect("small")];
    let wrapper_called = Arc::new(AtomicUsize::new(0));
    let called = Arc::clone(&wrapper_called);
    let mut taskflow = Taskflow::new();
    let range1_body = range.clone();
    let range2_body = range2.clone();
    taskflow.emplace_runtime(move |rt: &mut Runtime<'_>| {
        let called = Arc::clone(&called);
        let wrapper = move |task: &mut dyn FnMut()| {
            called.fetch_add(1, Ordering::Relaxed);
            task();
        };
        let mut result = 0_i64;
        rt.transform_reduce2(
            &range1_body,
            &range2_body,
            &mut result,
            |a: &i64, b: &i64| a + b,
            |a: &i64, b: &i64| a * b,
            StaticPartitioner::with_closure_wrapper(1, wrapper),
        );
        assert_eq!(result, -((UPPER - 1) * UPPER));
    });
    Executor::new(workers)
        .run(&taskflow)
        .wait()
        .expect("no task panicked");
    assert_eq!(wrapper_called.load(Ordering::Relaxed), workers);
}

worker_matrix! {
    closure_wrapper_transform_reduce {
        closure_wrapper_transform_reduce_1worker(1),
        closure_wrapper_transform_reduce_2workers(2),
        closure_wrapper_transform_reduce_3workers(3),
        closure_wrapper_transform_reduce_4workers(4),
    }
}

/// The owned-data [`ReduceBuilder`] wrappers, exercised through
/// `rustflow::prelude` alone -- no direct `use` of `ReduceBuilder` itself --
/// over every one of the four algorithms.
#[test]
fn flow_builder_reduce_wrappers_coverage() {
    let data = Arc::new((1..=200_i64).collect::<Vec<i64>>());

    let sum = Arc::new(Mutex::new(0_i64));
    let mut taskflow = Taskflow::new();
    taskflow.reduce(
        Arc::clone(&data),
        Arc::clone(&sum),
        |a: &i64, b: &i64| a + b,
        GuidedPartitioner::<DefaultClosureWrapper>::default(),
    );
    Executor::new(3).run(&taskflow).wait().expect("no panic");
    assert_eq!(*sum.lock().expect("uncontended"), 200 * 201 / 2);

    let squared = Arc::new(Mutex::new(0_i64));
    let mut taskflow = Taskflow::new();
    taskflow.transform_reduce(
        Arc::clone(&data),
        Arc::clone(&squared),
        |a: &i64, b: &i64| a + b,
        |x: &i64| x * x,
        GuidedPartitioner::<DefaultClosureWrapper>::default(),
    );
    Executor::new(3).run(&taskflow).wait().expect("no panic");
    assert_eq!(
        *squared.lock().expect("uncontended"),
        data.iter().map(|x| x * x).sum::<i64>()
    );

    let data2 = Arc::new((1..=200_i64).rev().collect::<Vec<i64>>());
    let dot = Arc::new(Mutex::new(0_i64));
    let mut taskflow = Taskflow::new();
    taskflow.transform_reduce2(
        Arc::clone(&data),
        Arc::clone(&data2),
        Arc::clone(&dot),
        |a: &i64, b: &i64| a + b,
        |x: &i64, y: &i64| x * y,
        GuidedPartitioner::<DefaultClosureWrapper>::default(),
    );
    Executor::new(3).run(&taskflow).wait().expect("no panic");
    assert_eq!(
        *dot.lock().expect("uncontended"),
        data.iter()
            .zip(data2.iter())
            .map(|(x, y)| x * y)
            .sum::<i64>()
    );

    let indexed = Arc::new(Mutex::new(10_i64));
    let mut taskflow = Taskflow::new();
    let data_lop = Arc::clone(&data);
    taskflow.reduce_by_index(
        IndexRange::new(0usize, data.len(), 1),
        Arc::clone(&indexed),
        move |sub: IndexRange<usize>, running: Option<i64>| {
            let mut lsum = running.unwrap_or(0);
            for i in (sub.begin()..sub.end()).step_by(sub.step_size()) {
                lsum += data_lop[i];
            }
            lsum
        },
        |a: &i64, b: &i64| a + b,
        GuidedPartitioner::<DefaultClosureWrapper>::default(),
    );
    Executor::new(3).run(&taskflow).wait().expect("no panic");
    assert_eq!(
        *indexed.lock().expect("uncontended"),
        10 + data.iter().sum::<i64>()
    );
}

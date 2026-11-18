//! Translated from `unittests/test_transform.cpp` (Taskflow v4.1.0):
//! `ParallelTransform.*`, `ParallelTransform2.*` and `ClosureWrapper.transform.*`.
//!
//! Three differences from upstream's file:
//!
//! - **One element type, one container.** Upstream's `parallel_transform<T,
//!   P>`/`parallel_transform2<T, P>` run over both `std::vector<int>` and
//!   `std::list<int>`, since its iterator-pair API does not care which.
//!   [`rustflow::Runtime::transform`]/`transform2` take `&[A]`/`&mut [B]`
//!   slices, so there is exactly one container to run them over; `Vec<i64>`
//!   stands in for upstream's `int`.
//! - **No `ParallelTransform3`.** Upstream's case drives `Subflow::transform`
//!   over a `std::multimap::equal_range` whose size depends on random input
//!   data, checked against a `std::transform` reference built the same way.
//!   What it is actually testing -- that `transform` run from inside a
//!   `Subflow` produces the same result as a plain runtime task -- is
//!   already true here by construction: `Subflow` and `Taskflow` both reach
//!   [`Runtime::transform`] through the identical
//!   [`FlowBuilder::emplace_runtime`], so a second test of the same
//!   partitioning logic through a different entry point adds no coverage
//!   `parallel_transform`/`parallel_transform2` do not already give it.
//! - **A dependent-async case upstream has none of.**
//!   `unittests/test_transform.cpp` submits every case through a graph task,
//!   with no `executor.async(tf::make_transform_task(...))` counterpart of
//!   `test_fill.cpp`'s `ParallelFillAsync`; the two cases at the end of this
//!   file submit `make_transform_task`/`make_transform2_task` through the
//!   dependent-async forms anyway, since that is the submission the
//!   `AlgorithmBuilder` wrapper cannot reach -- `tests/fill.rs`'s own docs
//!   say why.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::algorithm::SlicePartitioner;
use rustflow::algorithm::partitioner::{
    DefaultClosureWrapper, DynamicPartitioner, GuidedPartitioner, RandomPartitioner,
    StaticPartitioner,
};
use rustflow::algorithm::{make_transform_task, make_transform2_task};
use rustflow::prelude::*;
use rustflow::{Executor, Runtime};

#[macro_use]
mod common;

/// The length side of the sweep: upstream's `for(N=0; N<1000; N=(N+1)*2)`
/// narrowed to a representative subset, the way `tests/fill.rs`'s `LENGTHS`
/// narrows upstream's fixed `10000`.
const LENGTHS: [usize; 6] = [0, 1, 2, 7, 37, 999];

// ----------------------------------------------------------------------------
// ParallelTransform
// ----------------------------------------------------------------------------

/// Upstream's `parallel_transform<T, P>(W)`: `out[i] == to_string(in[i] +
/// 10)` in upstream, `out[i] == in[i] + 10` here -- the string conversion
/// does not interact with the partitioning logic this port is checking, so
/// the arithmetic itself is the assertion.
fn parallel_transform<P>(workers: usize, make: impl Fn() -> P)
where
    P: SlicePartitioner + Send + 'static,
{
    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();

    for n in LENGTHS {
        taskflow.clear();
        let mut part = Some(make());
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let part = part.take().expect("this task runs once");
            let input: Vec<i64> = (0..i64::try_from(n).expect("n fits in an i64")).collect();
            let mut out = vec![0i64; n];
            runtime.transform(&input, &mut out, |x: &i64| x + 10, part);
            assert!(
                out.iter().enumerate().all(|(i, &v)| v == input[i] + 10),
                "n={n}"
            );
        });
        executor.run(&taskflow).wait().expect("no task panicked");
    }
}

fn parallel_transform_guided(workers: usize) {
    parallel_transform(workers, GuidedPartitioner::<DefaultClosureWrapper>::default);
}

fn parallel_transform_random(workers: usize) {
    parallel_transform(workers, RandomPartitioner::<DefaultClosureWrapper>::default);
}

fn parallel_transform_static(workers: usize) {
    parallel_transform(workers, StaticPartitioner::<DefaultClosureWrapper>::default);
}

worker_matrix! {
    parallel_transform_guided {
        parallel_transform_guided_1_thread(1),
        parallel_transform_guided_2_threads(2),
        parallel_transform_guided_3_threads(3),
        parallel_transform_guided_4_threads(4),
    }
}

worker_matrix! {
    parallel_transform_random {
        parallel_transform_random_1_thread(1),
        parallel_transform_random_2_threads(2),
        parallel_transform_random_3_threads(3),
        parallel_transform_random_4_threads(4),
    }
}

worker_matrix! {
    parallel_transform_static {
        parallel_transform_static_1_thread(1),
        parallel_transform_static_2_threads(2),
        parallel_transform_static_3_threads(3),
        parallel_transform_static_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// ParallelTransform2
// ----------------------------------------------------------------------------

/// Upstream's `parallel_transform2<T, P>(W)`: `out[i] == to_string(in1[i] +
/// in2[i] + 10)` in upstream, where `in1` and `in2` are the same source
/// range (`std::ref(src_beg)` passed twice) -- `out[i] == 2 * in[i] + 10`
/// here, the arithmetic equivalent.
fn parallel_transform2<P>(workers: usize, make: impl Fn() -> P)
where
    P: SlicePartitioner + Send + 'static,
{
    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();

    for n in LENGTHS {
        taskflow.clear();
        let mut part = Some(make());
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let part = part.take().expect("this task runs once");
            let input: Vec<i64> = (0..i64::try_from(n).expect("n fits in an i64")).collect();
            let mut out = vec![0i64; n];
            runtime.transform2(
                &input,
                &input,
                &mut out,
                |x: &i64, y: &i64| x + y + 10,
                part,
            );
            assert!(
                out.iter().enumerate().all(|(i, &v)| v == 2 * input[i] + 10),
                "n={n}"
            );
        });
        executor.run(&taskflow).wait().expect("no task panicked");
    }
}

fn parallel_transform2_guided(workers: usize) {
    parallel_transform2(workers, GuidedPartitioner::<DefaultClosureWrapper>::default);
}

fn parallel_transform2_dynamic(workers: usize) {
    parallel_transform2(
        workers,
        DynamicPartitioner::<DefaultClosureWrapper>::default,
    );
}

fn parallel_transform2_static(workers: usize) {
    parallel_transform2(workers, StaticPartitioner::<DefaultClosureWrapper>::default);
}

fn parallel_transform2_random(workers: usize) {
    parallel_transform2(workers, RandomPartitioner::<DefaultClosureWrapper>::default);
}

worker_matrix! {
    parallel_transform2_guided {
        parallel_transform2_guided_1_thread(1),
        parallel_transform2_guided_2_threads(2),
        parallel_transform2_guided_3_threads(3),
        parallel_transform2_guided_4_threads(4),
    }
}

worker_matrix! {
    parallel_transform2_dynamic {
        parallel_transform2_dynamic_1_thread(1),
        parallel_transform2_dynamic_2_threads(2),
        parallel_transform2_dynamic_3_threads(3),
        parallel_transform2_dynamic_4_threads(4),
    }
}

worker_matrix! {
    parallel_transform2_static {
        parallel_transform2_static_1_thread(1),
        parallel_transform2_static_2_threads(2),
        parallel_transform2_static_3_threads(3),
        parallel_transform2_static_4_threads(4),
    }
}

worker_matrix! {
    parallel_transform2_random {
        parallel_transform2_random_1_thread(1),
        parallel_transform2_random_2_threads(2),
        parallel_transform2_random_3_threads(3),
        parallel_transform2_random_4_threads(4),
    }
}

// ----------------------------------------------------------------------------
// The owned-data wrapper
// ----------------------------------------------------------------------------

/// `FlowBuilder::transform` over owned, `Arc`-shared data.
#[test]
fn flow_builder_transform_locks_and_runs_over_owned_data() {
    let input = Arc::new(vec![1i64, 2, 3, 4, 5]);
    let out = Arc::new(Mutex::new(vec![0i64; 5]));
    let mut taskflow = Taskflow::new();
    taskflow.transform(
        Arc::clone(&input),
        Arc::clone(&out),
        |x: &i64| x * 3,
        GuidedPartitioner::<DefaultClosureWrapper>::default(),
    );

    Executor::new(3).run(&taskflow).wait().expect("no panic");

    assert_eq!(*out.lock().expect("uncontended"), vec![3, 6, 9, 12, 15]);
}

/// `FlowBuilder::transform2` over owned, `Arc`-shared data.
#[test]
fn flow_builder_transform2_locks_and_runs_over_owned_data() {
    let a = Arc::new(vec![1i64, 2, 3]);
    let b = Arc::new(vec![10i64, 20, 30]);
    let out = Arc::new(Mutex::new(vec![0i64; 3]));
    let mut taskflow = Taskflow::new();
    taskflow.transform2(
        Arc::clone(&a),
        Arc::clone(&b),
        Arc::clone(&out),
        |x: &i64, y: &i64| x + y,
        GuidedPartitioner::<DefaultClosureWrapper>::default(),
    );

    Executor::new(3).run(&taskflow).wait().expect("no panic");

    assert_eq!(*out.lock().expect("uncontended"), vec![11, 22, 33]);
}

// ----------------------------------------------------------------------------
// Closure Wrapper
// ----------------------------------------------------------------------------

thread_local! {
    /// Upstream's `GetThreadSpecificContext()`: see `tests/for_each.rs`'s
    /// identical thread-local.
    static CONTEXT: std::cell::Cell<i32> = const { std::cell::Cell::new(0) };
}

const UPPER: usize = 1000;

/// Upstream's `ClosureWrapper.transform.Static`: with a chunk size of `1`
/// every worker gets exactly one chunk, so the wrapper runs exactly `tc`
/// times.
#[test]
fn closure_wrapper_transform_static() {
    for tc in 1..16usize {
        let wrapper_called = Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();

        let wrapper_called_body = Arc::clone(&wrapper_called);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let input = vec![0i32; UPPER];
            let mut out = vec![0i32; UPPER];
            let wrapper_called = Arc::clone(&wrapper_called_body);
            let wrapper = move |task: &mut dyn FnMut()| {
                wrapper_called.fetch_add(1, Ordering::Relaxed);
                CONTEXT.set(i32::try_from(tc).expect("tc fits in an i32"));
                task();
                CONTEXT.set(0);
            };
            runtime.transform(
                &input,
                &mut out,
                |_: &i32| CONTEXT.get(),
                StaticPartitioner::with_closure_wrapper(1, wrapper),
            );
            assert!(out.iter().all(|&v| v == i32::try_from(tc).unwrap()));
        });

        Executor::new(tc)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(wrapper_called.load(Ordering::Relaxed), tc, "tc={tc}");
    }
}

/// Upstream's `ClosureWrapper.transform.Dynamic`: the dynamic family may
/// spawn fewer tasks than there are workers, so the wrapper runs *at most*
/// `tc` times.
#[test]
fn closure_wrapper_transform_dynamic() {
    for tc in 1..16usize {
        let wrapper_called = Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();

        let wrapper_called_body = Arc::clone(&wrapper_called);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let input: Vec<i32> = (0..i32::try_from(UPPER).unwrap()).collect();
            let mut out = vec![0i32; UPPER];
            let wrapper_called = Arc::clone(&wrapper_called_body);
            let wrapper = move |task: &mut dyn FnMut()| {
                wrapper_called.fetch_add(1, Ordering::Relaxed);
                CONTEXT.set(i32::try_from(tc).expect("tc fits in an i32"));
                task();
                CONTEXT.set(0);
            };
            runtime.transform(
                &input,
                &mut out,
                |_: &i32| CONTEXT.get(),
                DynamicPartitioner::with_closure_wrapper(1, wrapper),
            );
            assert!(out.iter().all(|&v| v == i32::try_from(tc).unwrap()));
        });

        Executor::new(tc)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert!(
            wrapper_called.load(Ordering::Relaxed) <= tc,
            "tc={tc}: the dynamic family is not obliged to use every worker"
        );
    }
}

// ----------------------------------------------------------------------------
// The factory, submitted outside a graph
// ----------------------------------------------------------------------------

/// `make_transform_task` handed straight to a submission with no handle of
/// its own and waited for through [`Executor::wait_for_all`]: the shape
/// `test_fill.cpp`'s `ParallelFillAsync` has and `test_transform.cpp` does
/// not, over the one-input overload.
#[test]
fn make_transform_task_runs_from_a_silent_dependent_async_body() {
    let executor = Executor::new(3);

    for n in LENGTHS {
        let input = Arc::new((0..i64::try_from(n).expect("n fits in an i64")).collect::<Vec<_>>());
        let out = Arc::new(Mutex::new(vec![-1i64; n]));
        let _task = executor.silent_dependent_async_runtime(
            make_transform_task(
                Arc::clone(&input),
                Arc::clone(&out),
                |x: &i64| x * 2,
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            ),
            [],
        );
        executor.wait_for_all();

        let guard = out.lock().expect("uncontended");
        assert!(
            guard.iter().enumerate().all(|(i, &v)| v == 2 * input[i]),
            "n={n}"
        );
    }
}

/// The same submission over the two-input overload, waited for through the
/// pair's own handle rather than the whole executor -- the other
/// dependent-async flavour, the way `tests/fill.rs` pairs them too.
#[test]
fn make_transform2_task_runs_from_a_dependent_async_body() {
    let executor = Executor::new(3);

    for n in LENGTHS {
        let a = Arc::new((0..i64::try_from(n).expect("n fits in an i64")).collect::<Vec<_>>());
        let b = Arc::new(a.iter().map(|x| x * 10).collect::<Vec<_>>());
        let out = Arc::new(Mutex::new(vec![-1i64; n]));
        let (_task, done) = executor.dependent_async_runtime(
            make_transform2_task(
                Arc::clone(&a),
                Arc::clone(&b),
                Arc::clone(&out),
                |x: &i64, y: &i64| x + y,
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            ),
            [],
        );
        done.wait().expect("no task panicked");

        let guard = out.lock().expect("uncontended");
        assert!(
            guard.iter().enumerate().all(|(i, &v)| v == a[i] + b[i]),
            "n={n}"
        );
    }
}

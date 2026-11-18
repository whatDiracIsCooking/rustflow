//! Translated from `unittests/test_fill.cpp` and
//! `unittests/test_generate.cpp` (Taskflow v4.1.0).
//!
//! The graphs those two files run their one algorithm call in: the
//! partitioner sweep of `fill_partioners`/`generate_partioners`, the two
//! shapes of `*Stateful` graph, the owned-data `taskflow.fill(...)` call, and
//! the closure-wrapper case. Each one takes the algorithm call and its
//! assertions as a closure, so what is shared here is only the graph around
//! them.
//!
//! A topic of its own for the reason [`super`]'s docs give: these are the
//! graphs upstream builds for an algorithm over a *whole* slice with nothing
//! to collect, which is `fill` and `generate` and no other suite here.
//!
//! # Design
//!
//! **The element type is `i64` throughout.** Upstream runs each of these
//! over `int` and `long double` both; `tests/fill.rs`'s own docs say why one
//! representative type covers the partitioning this port is checking, and
//! fixing it here is what lets a case hand over a closure rather than a
//! generic function.
//!
//! **The closure wrapper is boxed.** [`CountingWrapper`] is a
//! `Box<dyn Fn(&mut dyn FnMut()) + Send + Sync>` rather than the bare closure
//! each suite wrote inline, because the fixture builds the wrapper and the
//! case's own closure has to name the partitioner type it is handed. The box
//! keeps the blanket `impl<T: Fn(&mut dyn FnMut())> ClosureWrapper for T` as
//! the path under test, which a named wrapper struct would have replaced with
//! the trait's own direct impl.

use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::algorithm::SlicePartitioner;
use rustflow::algorithm::partitioner::StaticPartitioner;
use rustflow::prelude::*;
use rustflow::{Executor, Runtime};

/// The length side of the sweep: a representative subset of upstream's
/// `10000`, small enough to run the full partitioner matrix against every
/// worker count without the algorithm's own unit tests (which cover lengths
/// up to 150) duplicating the work.
pub const LENGTHS: [usize; 4] = [0, 1, 999, 10_000];

/// How long a vector a `*Stateful` case's setup task allocates: upstream's
/// own `a.resize(10000, 0)`.
const STATEFUL_LENGTH: usize = 10_000;

/// Asserts every element of `data` is `value`: upstream's
/// `REQUIRE(a == std_a)` against a vector `std::fill`ed with the same value.
pub fn assert_all<T: Copy + PartialEq + Debug>(data: &Mutex<Vec<T>>, value: T) {
    let data = data.lock().expect("uncontended");
    assert!(
        data.iter().all(|&element| element == value),
        "every one of the {} elements is {value:?}",
        data.len()
    );
}

/// Asserts the first `count` elements of `data` are `value` and that the rest
/// are still `untouched`: the `*_n` half of upstream's own checks, which is
/// the one that pins that the count was respected.
pub fn assert_filled_to<T: Copy + PartialEq + Debug>(
    data: &Mutex<Vec<T>>,
    count: usize,
    value: T,
    untouched: T,
) {
    let data = data.lock().expect("uncontended");
    assert!(
        data[..count].iter().all(|&element| element == value),
        "the first {count} elements are {value:?}"
    );
    assert!(
        data[count..].iter().all(|&element| element == untouched),
        "the elements past {count} are still {untouched:?}"
    );
}

/// Runs `body` over a freshly allocated, zeroed `Vec<i64>` of each length in
/// [`LENGTHS`], one run of a one-task graph per length: upstream's
/// `fill_partioners<P>(W, N, part)` and `generate_partioners<P>(W, N, part)`,
/// with `N` swept here rather than fixed by the case.
///
/// `make` is called once per length, since a partitioner is moved into the
/// task that uses it, and `body` is handed that partitioner along with the
/// vector so that the call under test and the assertions on what it wrote
/// stay in the case.
pub fn over_lengths<P, F>(workers: usize, make: impl Fn() -> P, body: F)
where
    P: SlicePartitioner + Send + 'static,
    F: Fn(&mut Runtime<'_>, &mut [i64], P) + Send + Sync + 'static,
{
    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();
    let body = Arc::new(body);

    for n in LENGTHS {
        taskflow.clear();
        let mut part = Some(make());
        let body = Arc::clone(&body);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let part = part.take().expect("this task runs once");
            let mut data = vec![0i64; n];
            body(runtime, &mut data, part);
        });
        executor.run(&taskflow).wait().expect("no task panicked");
    }
}

/// Upstream's `*Stateful` graph: a setup task that allocates the vector and a
/// runtime task that runs `body` over it, in that order, and the vector
/// handed back for the case to assert on.
///
/// Upstream's two tasks share `std::ref(beg)`/`std::ref(end)`, the iterator
/// pair its algorithm call re-reads when the task runs; a borrowed slice
/// computed by the runtime task from state the setup task left behind is the
/// direct equivalent here -- `src/algorithm/for_each.rs`'s `# Design` section
/// says why.
pub fn from_a_setup_task(
    workers: usize,
    body: impl FnOnce(&mut Runtime<'_>, &mut [i64]) + Send + 'static,
) -> Arc<Mutex<Vec<i64>>> {
    let backing: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
    let mut taskflow = Taskflow::new();

    let setup = {
        let backing = Arc::clone(&backing);
        taskflow.emplace(move || {
            *backing.lock().expect("uncontended") = vec![0; STATEFUL_LENGTH];
        })
    };

    let runtime_task = {
        let backing = Arc::clone(&backing);
        let mut body = Some(body);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let body = body.take().expect("this task runs once");
            let mut guard = backing.lock().expect("uncontended");
            body(runtime, &mut guard);
        })
    };

    taskflow.task(setup).precede([runtime_task]);

    Executor::new(workers)
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    backing
}

/// Upstream's `*StatefulMixed` graph: the vector is allocated up front and
/// only its *end* is what the setup task computes, so `body` is handed the
/// slice that bound cuts rather than the whole of it.
pub fn bounded_by_a_setup_task(
    workers: usize,
    body: impl FnOnce(&mut Runtime<'_>, &mut [i64]) + Send + 'static,
) -> Arc<Mutex<Vec<i64>>> {
    let backing: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(vec![0; STATEFUL_LENGTH]));
    let at = Arc::new(Mutex::new(0usize));
    let mut taskflow = Taskflow::new();

    let setup = {
        let at = Arc::clone(&at);
        taskflow.emplace(move || {
            *at.lock().expect("uncontended") = STATEFUL_LENGTH;
        })
    };

    let runtime_task = {
        let backing = Arc::clone(&backing);
        let at = Arc::clone(&at);
        let mut body = Some(body);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let body = body.take().expect("this task runs once");
            let mut guard = backing.lock().expect("uncontended");
            let end = *at.lock().expect("uncontended");
            body(runtime, &mut guard[..end]);
        })
    };

    taskflow.task(setup).precede([runtime_task]);

    Executor::new(workers)
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    backing
}

/// Upstream's `taskflow.fill(a.begin(), a.end(), val, part)` half of
/// `fill_partioners`, over data the task owns a share of rather
/// than borrows: `build` puts the one task on the graph, and the data comes
/// back for the case to assert on.
pub fn over_owned_data(
    build: impl FnOnce(&mut Taskflow, Arc<Mutex<Vec<i32>>>),
) -> Arc<Mutex<Vec<i32>>> {
    let data = Arc::new(Mutex::new(vec![0i32; 500]));
    let mut taskflow = Taskflow::new();
    build(&mut taskflow, Arc::clone(&data));

    Executor::new(3).run(&taskflow).wait().expect("no panic");

    data
}

/// A closure wrapper that counts its invocations, boxed so that a case can
/// name the partitioner type it is handed: this module's `# Design` section
/// says why the box rather than a struct of its own.
pub type CountingWrapper = Box<dyn Fn(&mut dyn FnMut()) + Send + Sync>;

/// Asserts that a closure wrapper runs once per worker task over that
/// worker's whole share, not once per element, for every worker count from
/// `1` to `15`.
///
/// This port's own case: neither `test_fill.cpp` nor `test_generate.cpp`
/// carries a `ClosureWrapper.*` case of its own, where `test_transform.cpp`
/// does, and `tests/for_each.rs`'s `closure_wrapper_for_each_static` is the
/// same shape over `for_each`.
///
/// `body` is handed a chunk size of one and a thousand elements, so a
/// wrapper called per element would count a thousand and one called per
/// worker task counts the worker count.
pub fn wrapper_runs_once_per_worker(
    body: impl Fn(&mut Runtime<'_>, &mut [i32], StaticPartitioner<CountingWrapper>)
    + Send
    + Sync
    + 'static,
) {
    let body = Arc::new(body);

    for tc in 1..16usize {
        let wrapper_called = Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();

        let body = Arc::clone(&body);
        let wrapper_called_body = Arc::clone(&wrapper_called);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut data = vec![0i32; 1000];
            let wrapper_called = Arc::clone(&wrapper_called_body);
            let wrapper: CountingWrapper = Box::new(move |task: &mut dyn FnMut()| {
                wrapper_called.fetch_add(1, Ordering::Relaxed);
                task();
            });
            body(
                runtime,
                &mut data,
                StaticPartitioner::with_closure_wrapper(1, wrapper),
            );
        });

        Executor::new(tc)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(wrapper_called.load(Ordering::Relaxed), tc, "tc={tc}");
    }
}

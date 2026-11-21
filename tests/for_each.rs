//! Translated from the `for_each` sections of `unittests/test_for_each.cpp`
//! (Taskflow v4.1.0): `ParallelFor.*`, `StatefulParallelFor.*` and
//! `ClosureWrapper.for_each.*`. The `for_each_index`/`for_each_by_index`
//! halves of that file, and its `ClosureWrapper.for_each_index.*` cases, are
//! a separate port, not here.
//!
//! Three adaptations, each forced by the port rather than chosen:
//!
//! - **One sweep per worker count, not 755.** Upstream's `for_each<P>(W)`
//!   runs every `n` in `0..=150` against every chunk size in `{0, 1, 3, 7,
//!   99}` on *one* `tf::Executor`, reused across the whole sweep. The shape
//!   survives -- one [`Executor`] per worker count, [`Taskflow::clear`]
//!   between runs -- but the `n` side of the sweep is the representative
//!   subset `[0, 1, 2, 7, 37, 150]`: `src/algorithm/for_each.rs`'s own
//!   `every_element_is_visited_exactly_once` unit test already covers every
//!   `n` up to 150 against every chunk size, so this suite's job is the
//!   worker-count matrix upstream's `TEST_CASE` list enumerates, not a
//!   second exhaustive `n` sweep.
//! - **`StatefulParallelFor` is a borrowed slice, not two edges onto a
//!   shared iterator.** Upstream's case runs `for_each(std::ref(beg),
//!   std::ref(end), ...)` *and* `for_each_index(std::ref(ibeg),
//!   std::ref(iend), ...)` as two tasks after one `init`, splitting a vector
//!   down the middle between them. Only the `for_each` half is this issue's;
//!   `init` here precedes a single runtime task that locks the backing
//!   `Vec`, splits it itself, and calls [`Runtime::for_each`] on the left
//!   half -- `src/algorithm/for_each.rs`'s `# Design` section says why a
//!   borrowed slice gets upstream's lazy-fetch behaviour for free this way.
//! - **`ClosureWrapper.for_each.*` has no `GetThreadSpecificContext`.**
//!   Upstream's cases read back a thread-local to prove the wrapper ran
//!   *before* the chunk body, on the worker that is about to run it; this
//!   port uses [`std::thread_local`] the same way, which is the direct
//!   translation rather than a different mechanism.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::algorithm::SlicePartitioner;
use rustflow::algorithm::make_for_each_task;
use rustflow::algorithm::partitioner::{
    DefaultClosureWrapper, DynamicPartitioner, GuidedPartitioner, Partitioner, RandomPartitioner,
    SplittingPartitioner, StaticPartitioner,
};
use rustflow::prelude::*;
use rustflow::{Executor, Runtime};

#[macro_use]
mod common;

/// The `n` side of the sweep every `parallel_for`/`stateful_parallel_for`
/// case runs: a representative subset of upstream's `0..=150`, for the
/// reason this file's own docs give.
const LENGTHS: [usize; 6] = [0, 1, 2, 7, 37, 150];

/// The chunk-size side of the sweep, upstream's own `{0, 1, 3, 7, 99}`
/// verbatim -- `0` is "let the partitioner work one out",
/// the rest force varying degrees of striding.
const CHUNK_SIZES: [usize; 5] = [0, 1, 3, 7, 99];

// ----------------------------------------------------------------------------
// ParallelFor
// ----------------------------------------------------------------------------

/// Upstream's `for_each<P>(W)`, over the `for_each` half only: every element
/// of a freshly allocated `Vec<i64>` of length `n` is visited exactly once,
/// for every `n` and chunk size in the sweep this file's docs narrow to.
fn parallel_for<P>(workers: usize, make: impl Fn(usize) -> P)
where
    P: SlicePartitioner + Send + 'static,
{
    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();

    for n in LENGTHS {
        for chunk_size in CHUNK_SIZES {
            taskflow.clear();

            let counter = Arc::new(AtomicUsize::new(0));
            let counter_body = Arc::clone(&counter);
            // `emplace_runtime` wants `FnMut`; this task runs exactly once
            // per `taskflow.clear()`, so the partitioner is taken out of an
            // `Option` rather than moved directly.
            let mut part = Some(make(chunk_size));
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let part = part.take().expect("this task runs once");
                let mut vec = vec![-1i64; n];
                runtime.for_each(
                    &mut vec,
                    |slot: &mut i64| {
                        counter_body.fetch_add(1, Ordering::Relaxed);
                        *slot = 1;
                    },
                    part,
                );
                assert!(
                    vec.iter().all(|&v| v == 1),
                    "every element of {n} was visited"
                );
            });

            executor.run(&taskflow).wait().expect("no task panicked");
            assert_eq!(
                counter.load(Ordering::Relaxed),
                n,
                "{workers} workers, n={n}, chunk_size={chunk_size}"
            );
        }
    }
}

fn parallel_for_guided(workers: usize) {
    parallel_for(workers, GuidedPartitioner::<DefaultClosureWrapper>::new);
}

fn parallel_for_dynamic(workers: usize) {
    parallel_for(workers, DynamicPartitioner::<DefaultClosureWrapper>::new);
}

fn parallel_for_static(workers: usize) {
    parallel_for(workers, StaticPartitioner::<DefaultClosureWrapper>::new);
}

fn parallel_for_random(workers: usize) {
    parallel_for(workers, RandomPartitioner::<DefaultClosureWrapper>::new);
}

worker_matrix! {
    parallel_for_guided {
        parallel_for_guided_1_thread(1),
        parallel_for_guided_2_threads(2),
        parallel_for_guided_3_threads(3),
        parallel_for_guided_4_threads(4),
        parallel_for_guided_5_threads(5),
        parallel_for_guided_6_threads(6),
        parallel_for_guided_7_threads(7),
        parallel_for_guided_8_threads(8),
        parallel_for_guided_9_threads(9),
        parallel_for_guided_10_threads(10),
        parallel_for_guided_11_threads(11),
        parallel_for_guided_12_threads(12),
    }
}

worker_matrix! {
    parallel_for_dynamic {
        parallel_for_dynamic_1_thread(1),
        parallel_for_dynamic_2_threads(2),
        parallel_for_dynamic_3_threads(3),
        parallel_for_dynamic_4_threads(4),
        parallel_for_dynamic_5_threads(5),
        parallel_for_dynamic_6_threads(6),
        parallel_for_dynamic_7_threads(7),
        parallel_for_dynamic_8_threads(8),
        parallel_for_dynamic_9_threads(9),
        parallel_for_dynamic_10_threads(10),
        parallel_for_dynamic_11_threads(11),
        parallel_for_dynamic_12_threads(12),
    }
}

worker_matrix! {
    parallel_for_static {
        parallel_for_static_1_thread(1),
        parallel_for_static_2_threads(2),
        parallel_for_static_3_threads(3),
        parallel_for_static_4_threads(4),
        parallel_for_static_5_threads(5),
        parallel_for_static_6_threads(6),
        parallel_for_static_7_threads(7),
        parallel_for_static_8_threads(8),
        parallel_for_static_9_threads(9),
        parallel_for_static_10_threads(10),
        parallel_for_static_11_threads(11),
        parallel_for_static_12_threads(12),
    }
}

worker_matrix! {
    parallel_for_random {
        parallel_for_random_1_thread(1),
        parallel_for_random_2_threads(2),
        parallel_for_random_3_threads(3),
        parallel_for_random_4_threads(4),
        parallel_for_random_5_threads(5),
        parallel_for_random_6_threads(6),
        parallel_for_random_7_threads(7),
        parallel_for_random_8_threads(8),
        parallel_for_random_9_threads(9),
        parallel_for_random_10_threads(10),
        parallel_for_random_11_threads(11),
        parallel_for_random_12_threads(12),
    }
}

// ----------------------------------------------------------------------------
// SplittingPartitioner
// ----------------------------------------------------------------------------

/// The same sweep under `SplittingPartitioner`, the port's own fifth family,
/// which has no upstream counterpart and so no upstream worker matrix to
/// mirror -- two worker counts rather than twelve, since what is new here is
/// the driving path and not the sweep.
///
/// That path is the one thing `SlicePartitioner::drive` does for this family
/// and for no other: the slice is carved into one contiguous share per worker
/// and each share handed to a task of its own, which may halve what it is
/// left with onto a second task when a foreign worker picks it up. "Every
/// element exactly once" therefore says more here than it does for the four
/// strided or cursor-driven families above, because which chunks exist at all
/// depends on which worker ran the share.
fn parallel_for_splitting(workers: usize) {
    parallel_for(workers, SplittingPartitioner::<DefaultClosureWrapper>::new);
}

worker_matrix! {
    parallel_for_splitting {
        parallel_for_splitting_2_threads(2),
        parallel_for_splitting_4_threads(4),
    }
}

/// `SplittingPartitioner::with_closure_wrapper`, the constructor the sweep
/// above's `new` does not reach, and the one place a closure wrapper means
/// something different for this family than for the other four: it wraps a
/// whole *task*, not each chunk that task walks.
///
/// So the count it reports is the number of tasks the shares became, which is
/// one per share plus one per share a steal re-split -- bounded below by the
/// share count and above by nothing a run promises. Both sides of that are
/// asserted: at least one wrapped task per share, and every element still
/// visited exactly once however many there were.
#[test]
fn a_splitting_partitioners_wrapper_wraps_each_task_a_share_became() {
    const N: usize = 1_000;
    const W: usize = 4;

    let tasks = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&tasks);
    let part = SplittingPartitioner::with_closure_wrapper(8, move |body: &mut dyn FnMut()| {
        counted.fetch_add(1, Ordering::Relaxed);
        body();
    });
    assert_eq!(part.chunk_size(), 8, "the chunk size given is kept");

    let data = Arc::new(Mutex::new(vec![0u32; N]));
    let mut taskflow = Taskflow::new();
    taskflow.for_each(Arc::clone(&data), |x: &mut u32| *x += 1, part);

    Executor::new(W)
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    assert!(
        data.lock().expect("uncontended").iter().all(|&x| x == 1),
        "every element visited exactly once, whatever the shares split into"
    );
    assert!(
        tasks.load(Ordering::Relaxed) >= W,
        "one wrapped task per share at least, and one more per share a steal re-split"
    );
}

// ----------------------------------------------------------------------------
// StatefulParallelFor
// ----------------------------------------------------------------------------

/// Upstream's `stateful_for_each<P>(W)`, over the `for_each` half only: an
/// `init` task resizes the backing `Vec` and records where the left half
/// ends, which a single runtime task then reads back to build the slice it
/// hands [`Runtime::for_each`] -- this file's own docs say why that is the
/// direct equivalent of upstream's `std::ref(beg)`/`std::ref(end)`.
fn stateful_parallel_for<P>(workers: usize, make: impl Fn(usize) -> P)
where
    P: SlicePartitioner + Send + 'static,
{
    let executor = Executor::new(workers);

    for n in LENGTHS {
        for chunk_size in CHUNK_SIZES {
            let mut taskflow = Taskflow::new();
            let backing: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
            let half = Arc::new(Mutex::new(0usize));
            let counter = Arc::new(AtomicUsize::new(0));

            let setup = {
                let backing = Arc::clone(&backing);
                let half = Arc::clone(&half);
                taskflow.emplace(move || {
                    *backing.lock().expect("uncontended") = vec![-1; n];
                    *half.lock().expect("uncontended") = n / 2;
                })
            };

            let mut part = Some(make(chunk_size));
            let runtime_task = {
                let backing = Arc::clone(&backing);
                let half = Arc::clone(&half);
                let counter = Arc::clone(&counter);
                taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                    let part = part.take().expect("this task runs once");
                    let mut guard = backing.lock().expect("uncontended");
                    let at = *half.lock().expect("uncontended");
                    let (left, _right) = guard.split_at_mut(at);
                    runtime.for_each(
                        left,
                        |slot: &mut i64| {
                            counter.fetch_add(1, Ordering::Relaxed);
                            *slot = 8;
                        },
                        part,
                    );
                })
            };

            taskflow.task(setup).precede([runtime_task]);

            executor.run(&taskflow).wait().expect("no task panicked");

            let guard = backing.lock().expect("uncontended");
            let at = *half.lock().expect("uncontended");
            assert_eq!(
                counter.load(Ordering::Relaxed),
                at,
                "{workers} workers, n={n}, chunk_size={chunk_size}"
            );
            assert!(guard[..at].iter().all(|&v| v == 8));
        }
    }
}

fn stateful_parallel_for_guided(workers: usize) {
    stateful_parallel_for(workers, GuidedPartitioner::<DefaultClosureWrapper>::new);
}

fn stateful_parallel_for_dynamic(workers: usize) {
    stateful_parallel_for(workers, DynamicPartitioner::<DefaultClosureWrapper>::new);
}

fn stateful_parallel_for_static(workers: usize) {
    stateful_parallel_for(workers, StaticPartitioner::<DefaultClosureWrapper>::new);
}

fn stateful_parallel_for_random(workers: usize) {
    stateful_parallel_for(workers, RandomPartitioner::<DefaultClosureWrapper>::new);
}

worker_matrix! {
    stateful_parallel_for_guided {
        stateful_parallel_for_guided_1_thread(1),
        stateful_parallel_for_guided_2_threads(2),
        stateful_parallel_for_guided_3_threads(3),
        stateful_parallel_for_guided_4_threads(4),
        stateful_parallel_for_guided_5_threads(5),
        stateful_parallel_for_guided_6_threads(6),
        stateful_parallel_for_guided_7_threads(7),
        stateful_parallel_for_guided_8_threads(8),
        stateful_parallel_for_guided_9_threads(9),
        stateful_parallel_for_guided_10_threads(10),
        stateful_parallel_for_guided_11_threads(11),
        stateful_parallel_for_guided_12_threads(12),
    }
}

worker_matrix! {
    stateful_parallel_for_dynamic {
        stateful_parallel_for_dynamic_1_thread(1),
        stateful_parallel_for_dynamic_2_threads(2),
        stateful_parallel_for_dynamic_3_threads(3),
        stateful_parallel_for_dynamic_4_threads(4),
        stateful_parallel_for_dynamic_5_threads(5),
        stateful_parallel_for_dynamic_6_threads(6),
        stateful_parallel_for_dynamic_7_threads(7),
        stateful_parallel_for_dynamic_8_threads(8),
        stateful_parallel_for_dynamic_9_threads(9),
        stateful_parallel_for_dynamic_10_threads(10),
        stateful_parallel_for_dynamic_11_threads(11),
        stateful_parallel_for_dynamic_12_threads(12),
    }
}

worker_matrix! {
    stateful_parallel_for_static {
        stateful_parallel_for_static_1_thread(1),
        stateful_parallel_for_static_2_threads(2),
        stateful_parallel_for_static_3_threads(3),
        stateful_parallel_for_static_4_threads(4),
        stateful_parallel_for_static_5_threads(5),
        stateful_parallel_for_static_6_threads(6),
        stateful_parallel_for_static_7_threads(7),
        stateful_parallel_for_static_8_threads(8),
        stateful_parallel_for_static_9_threads(9),
        stateful_parallel_for_static_10_threads(10),
        stateful_parallel_for_static_11_threads(11),
        stateful_parallel_for_static_12_threads(12),
    }
}

worker_matrix! {
    stateful_parallel_for_random {
        stateful_parallel_for_random_1_thread(1),
        stateful_parallel_for_random_2_threads(2),
        stateful_parallel_for_random_3_threads(3),
        stateful_parallel_for_random_4_threads(4),
        stateful_parallel_for_random_5_threads(5),
        stateful_parallel_for_random_6_threads(6),
        stateful_parallel_for_random_7_threads(7),
        stateful_parallel_for_random_8_threads(8),
        stateful_parallel_for_random_9_threads(9),
        stateful_parallel_for_random_10_threads(10),
        stateful_parallel_for_random_11_threads(11),
        stateful_parallel_for_random_12_threads(12),
    }
}

// ----------------------------------------------------------------------------
// Closure Wrapper
// ----------------------------------------------------------------------------

thread_local! {
    /// Upstream's `GetThreadSpecificContext()`: a thread-local an inline
    /// wrapper sets before running a chunk's body and clears after, so the
    /// body can read back which worker's wrapper is currently active.
    static CONTEXT: Cell<i32> = const { Cell::new(0) };
}

const UPPER: usize = 1000;

/// Upstream's `ClosureWrapper.for_each.Static`: with a chunk size of `1`
/// every worker gets exactly one chunk, so the wrapper runs exactly `tc`
/// times -- once per worker task, wrapping that whole task rather than one
/// element.
#[test]
fn closure_wrapper_for_each_static() {
    for tc in 1..16usize {
        let wrapper_called = Arc::new(AtomicUsize::new(0));
        let count = Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();

        let wrapper_called_body = Arc::clone(&wrapper_called);
        let count_body = Arc::clone(&count);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut range = vec![0i32; UPPER];
            let wrapper_called = Arc::clone(&wrapper_called_body);
            let wrapper = move |task: &mut dyn FnMut()| {
                wrapper_called.fetch_add(1, Ordering::Relaxed);
                CONTEXT.set(i32::try_from(tc).expect("tc fits in an i32"));
                task();
                CONTEXT.set(0);
            };
            let count = Arc::clone(&count_body);
            runtime.for_each(
                &mut range,
                move |i: &mut i32| {
                    count.fetch_add(1, Ordering::Relaxed);
                    *i = CONTEXT.get();
                },
                StaticPartitioner::with_closure_wrapper(1, wrapper),
            );

            assert!(range.iter().all(|&v| v == i32::try_from(tc).unwrap()));
        });

        Executor::new(tc)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(count.load(Ordering::Relaxed), UPPER, "tc={tc}");
        assert_eq!(wrapper_called.load(Ordering::Relaxed), tc, "tc={tc}");
    }
}

/// Upstream's `ClosureWrapper.for_each.Dynamic`: the dynamic family may
/// spawn fewer tasks than there are workers, so the wrapper runs *at most*
/// `tc` times.
#[test]
fn closure_wrapper_for_each_dynamic() {
    for tc in 1..16usize {
        let wrapper_called = Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();

        let wrapper_called_body = Arc::clone(&wrapper_called);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut range = vec![0i32; UPPER];
            let wrapper_called = Arc::clone(&wrapper_called_body);
            let wrapper = move |task: &mut dyn FnMut()| {
                wrapper_called.fetch_add(1, Ordering::Relaxed);
                CONTEXT.set(i32::try_from(tc).expect("tc fits in an i32"));
                task();
                CONTEXT.set(0);
            };
            runtime.for_each(
                &mut range,
                |i: &mut i32| *i = CONTEXT.get(),
                DynamicPartitioner::with_closure_wrapper(1, wrapper),
            );

            assert!(range.iter().all(|&v| v == i32::try_from(tc).unwrap()));
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
// ParallelFor.DependentAsync
// ----------------------------------------------------------------------------

/// Upstream's `dependent_async`: `make_for_each_task` handed straight to
/// [`Executor::dependent_async_runtime`], the submission neither
/// [`Runtime::for_each`] nor the `AlgorithmBuilder` wrapper reaches.
///
/// Two adaptations. Upstream splits one `vector` between a
/// `make_for_each_task` over its first half and a
/// `make_for_each_index_task` over its second; the index half is
/// `for_each_index`'s port, not this file's, and the factory here owns a
/// share of the whole `Vec` rather than a pair of iterators into part of
/// one, so the case runs as one submission over all of `data`. And it is
/// waited for through the pair's own handle rather than
/// `wait_for_all`, which is what `tests/sort.rs` covers for the silent
/// flavour.
#[test]
fn make_for_each_task_runs_from_a_dependent_async_body() {
    let executor = Executor::new(3);

    for &n in &[0usize, 1, 7, 150, 65_536] {
        let data = Arc::new(Mutex::new(vec![0u32; n]));
        let (_task, done) = executor.dependent_async_runtime(
            make_for_each_task(
                Arc::clone(&data),
                |d: &mut u32| *d = 10,
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            ),
            [],
        );
        done.wait().expect("no task panicked");

        let guard = data.lock().expect("uncontended");
        assert_eq!(guard.len(), n);
        assert!(guard.iter().all(|&v| v == 10), "n = {n}");
    }
}

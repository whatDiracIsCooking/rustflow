//! Translated from `unittests/test_asyncs.cpp` (Taskflow v4.1.0).
//!
//! Two of upstream's groups are here, and they are the two that need nothing
//! but a closure handed to the executor: `Async`, a hundred thousand
//! value-returning submissions whose handles are collected and then read, and
//! `NestedAsync`, the same hundred thousand with a chain of three more
//! submitted from inside each body. Both run at one, two, four, eight and
//! sixteen workers, one `#[test]` per worker count as upstream has one
//! `TEST_CASE`, so a failure names the pool it happened on.
//!
//! Each case asserts two separate things, which is why the counter is there
//! at all when the handles already carry a value: the counter says every body
//! *ran*, and the sum over the handles says every body's result reached the
//! caller who asked for it. Upstream asserts the same pair -- `counter == N`
//! after `wait_for_all`, then `-c == 2*N` over the futures -- and the order
//! matters, since `wait_for_all` is the only thing that waits for a body
//! nobody is holding a handle on.
//!
//! **The three `TaskParams` branches of `Async` are one branch here.**
//! Upstream's loop picks between `async(f)`, `async(DefaultTaskParams{}, f)`
//! and `async(TaskParams{std::to_string(i)}, f)` on `i % 3`, and all three
//! differ only in the name the node is given. An async node has no name worth
//! reading in this milestone -- nothing hands one back, and the observers
//! that would are a later step -- so the port has one unnamed submitting call
//! and the three branches collapse onto it.
//!
//! Upstream's remaining groups are named rather than half-ported, each with
//! what it waits on:
//!
//! - **`MixedAsync.<N>Executors`** (upstream's `mixed_executor_async`) and
//!   **`AsyncRuntime`** / **`RecursiveAsyncRuntime`** need runtime-shaped
//!   bodies: `spawn_runtime` and `silent_async_runtime` on the executor,
//!   which are #90, and `Runtime::spawn` / `Runtime::silent_async`, which are
//!   #92. The dispatch arm refuses such a body today rather than quietly
//!   doing nothing with it.
//! - **`MixedAsync.<N>threads`** (upstream's `mixed_async`) submits plain
//!   bodies from the tasks of a *running* graph, which is the one of these
//!   that #88 could run. It is held back to land with its namesake under #90,
//!   so that both groups called `MixedAsync` arrive together rather than this
//!   file growing a half of one.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::{AsyncHandle, Executor};

/// Upstream's `int N = 100000`, in both of its helpers.
///
/// A stress count rather than a correctness one: what a hundred thousand
/// submissions buy over ten is the race between a submitting thread and a
/// pool that is already draining the buffers, which is the whole of what
/// `_schedule_async_task` has to get right.
const TASKS: usize = 100_000;

/// What every body returns, as upstream's do: a value whose sum over all
/// handles is the assertion.
const RESULT: i32 = -2;

/// Upstream's `-c == 2*N`, the other way round.
fn expected_sum() -> i32 {
    RESULT * i32::try_from(TASKS).expect("a hundred thousand fits in an i32")
}

/// Upstream's `async(unsigned W)`: `TASKS` value-returning submissions, each
/// counting itself, waited for as a pool and then read one handle at a time.
fn spawned_tasks(workers: usize) {
    let executor = Executor::new(workers);
    let counter = Arc::new(AtomicUsize::new(0));

    let handles: Vec<AsyncHandle<i32>> = (0..TASKS)
        .map(|_| {
            let counter = Arc::clone(&counter);
            executor.spawn(move || {
                // Relaxed, as upstream's is: what orders these increments
                // against the assertion below is `wait_for_all`, not this
                // word.
                counter.fetch_add(1, Ordering::Relaxed);
                RESULT
            })
        })
        .collect();

    // Upstream's `executor.wait_for_all()`, and the claim it is here to
    // check: an async task is counted in `num_topologies` like a run, so the
    // call that waits for the executor to be idle covers work that has no run
    // to belong to.
    executor.wait_for_all();
    assert_eq!(counter.load(Ordering::Relaxed), TASKS, "{workers} workers");

    // Every handle, after the wait, so that each of these is a read of a slot
    // that has already been filled rather than a block.
    let sum: i32 = handles
        .into_iter()
        .map(|handle| {
            assert!(handle.is_done(), "wait_for_all covers an async task");
            handle.wait().expect("no body panicked")
        })
        .sum();
    assert_eq!(sum, expected_sum(), "{workers} workers");
}

/// Upstream's `nested_async(unsigned W)`: the same submissions, each body
/// submitting another from the worker it is running on, three levels down.
///
/// The four levels are what the case is for. A submission from a worker of
/// this executor is a submission from inside a task body, and it has to be
/// visible to the pool before the body it came from finishes -- otherwise the
/// counters could reach zero with work still to come, and `wait_for_all`
/// would return early. At one worker it is also the case that cannot be
/// allowed to park: the only thread there is is the one running the body that
/// just queued more work for it.
fn nested_spawned_tasks(workers: usize) {
    /// One level of upstream's hand-written nest: count this level, then
    /// submit the next.
    ///
    /// The inner handle is **dropped**, which detaches -- the task still runs
    /// -- exactly as upstream discards the `std::future` its inner
    /// `executor.async` returns. So nothing in the chain is waited for
    /// directly, and `wait_for_all` is the only thing that covers it.
    fn nest(executor: &Arc<Executor>, counter: &Arc<AtomicUsize>, levels: usize) {
        counter.fetch_add(1, Ordering::Relaxed);
        if levels == 0 {
            return;
        }

        let inner_executor = Arc::clone(executor);
        let inner_counter = Arc::clone(counter);
        let _detached = executor.spawn(move || nest(&inner_executor, &inner_counter, levels - 1));
    }

    let executor = Arc::new(Executor::new(workers));
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(TASKS);
    for _ in 0..TASKS {
        let nested = Arc::clone(&executor);
        let counter = Arc::clone(&counter);
        handles.push(executor.spawn(move || {
            // Three more below this one, which is upstream's four bodies per
            // outer task.
            nest(&nested, &counter, 3);
            RESULT
        }));
    }

    executor.wait_for_all();
    assert_eq!(
        counter.load(Ordering::Relaxed),
        4 * TASKS,
        "{workers} workers: every level of every nest ran"
    );

    let sum: i32 = handles
        .into_iter()
        .map(|handle| handle.wait().expect("no body panicked"))
        .sum();
    assert_eq!(sum, expected_sum(), "{workers} workers");
}

#[test]
fn async_1_thread() {
    spawned_tasks(1);
}

#[test]
fn async_2_threads() {
    spawned_tasks(2);
}

#[test]
fn async_4_threads() {
    spawned_tasks(4);
}

#[test]
fn async_8_threads() {
    spawned_tasks(8);
}

#[test]
fn async_16_threads() {
    spawned_tasks(16);
}

#[test]
fn nested_async_1_thread() {
    nested_spawned_tasks(1);
}

#[test]
fn nested_async_2_threads() {
    nested_spawned_tasks(2);
}

#[test]
fn nested_async_4_threads() {
    nested_spawned_tasks(4);
}

#[test]
fn nested_async_8_threads() {
    nested_spawned_tasks(8);
}

#[test]
fn nested_async_16_threads() {
    nested_spawned_tasks(16);
}

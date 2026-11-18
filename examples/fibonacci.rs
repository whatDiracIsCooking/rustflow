//! Translated from `examples/fibonacci.cpp` (Taskflow v4.1.0).
//!
//! Upstream's own program computes `fib(N)` by recursing through
//! `tf::TaskGroup::silent_async` and `corun` -- a worker-side recursive
//! function, with no taskflow graph at all. This milestone's assignment
//! asks for the other classic shape recursive tasking is written in
//! upstream's tutorials: a taskflow whose one task is a subflow that spawns
//! two more subflow tasks, one per recursive call, and joins them before
//! combining their results. `TaskGroup`'s own recursive shape is exercised
//! nowhere in this crate's examples, only in `tests/task_group.rs`.
//!
//! # Design
//!
//! Upstream passes `res1`/`res2` by mutable reference into each recursive
//! call's closure; a subflow task's body is `FnMut + Send + 'static`, so
//! this port passes an `Arc<AtomicU64>` each recursive call stores its
//! answer into instead, joined by the parent before it adds the two.
//!
//! Upstream also takes `N` from `argv`; this example fixes a small `N` so
//! that `devtools/run-examples.sh` runs it deterministically and fast -- see
//! the module docs' note on why `fibonacci` is the one example here that can
//! genuinely hang or blow the stack if that depth grows unchecked.
//!
//! Upstream prints `fib[N] = ...` and the elapsed time. This example prints
//! the former and **asserts** it against a plain recursive `fib`, so a wrong
//! answer -- or a subflow graph that never joins -- shows as a failed
//! assertion or a hang the test must be re-read for, rather than a quiet
//! wrong number.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rustflow::Executor;
use rustflow::prelude::*;

/// Fibonacci `n` through plain recursion, the reference value [`fib`]'s
/// subflow version is checked against.
fn expected(n: u64) -> u64 {
    if n < 2 {
        n
    } else {
        expected(n - 1) + expected(n - 2)
    }
}

/// Computes `fib(n)` by spawning two subflow tasks -- one per recursive call
/// -- and joining them before adding their results into `result`.
fn fib(subflow: &mut Subflow<'_>, n: u64, result: &Arc<AtomicU64>) {
    if n < 2 {
        result.store(n, Ordering::Relaxed);
        return;
    }

    let left = Arc::new(AtomicU64::new(0));
    let right = Arc::new(AtomicU64::new(0));

    {
        let left = Arc::clone(&left);
        subflow.emplace_subflow(move |subflow: &mut Subflow<'_>| fib(subflow, n - 1, &left));
    }
    {
        let right = Arc::clone(&right);
        subflow.emplace_subflow(move |subflow: &mut Subflow<'_>| fib(subflow, n - 2, &right));
    }

    // Upstream's `tg.corun()`, the point where its recursive function waits
    // for the task it just spawned and the one it called inline before
    // continuing; this port's subflow joins both children at once instead.
    subflow.join().expect("a fresh subflow is joinable");

    result.store(
        left.load(Ordering::Relaxed) + right.load(Ordering::Relaxed),
        Ordering::Relaxed,
    );
}

fn main() {
    // Kept small on purpose: 176 subflow tasks deep enough to show the
    // recursion works, nowhere near `tests/task_group.rs`'s observed cliff
    // for a very different recursive shape.
    const N: u64 = 10;

    let mut taskflow = Taskflow::with_name("Fibonacci");
    let result = Arc::new(AtomicU64::new(0));

    {
        let result = Arc::clone(&result);
        taskflow.emplace_subflow(move |subflow: &mut Subflow<'_>| fib(subflow, N, &result));
    }

    let executor = Executor::default();
    assert!(executor.run(&taskflow).wait().is_ok());

    let answer = result.load(Ordering::Relaxed);
    println!("fib[{N}] = {answer}");
    assert_eq!(answer, expected(N));
}

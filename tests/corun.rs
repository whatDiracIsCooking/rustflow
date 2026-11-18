//! Translated from `unittests/test_basics.cpp` (Taskflow v4.1.0): `NestedRuns`.
//!
//! Upstream's case is three levels deep. A taskflow `C` whose second task runs
//! a taskflow `B`, whose second task runs a taskflow `A`, every task bumping
//! one shared counter -- 220200 bumps by the end, at one through sixteen
//! workers. Two adaptations, and the first one is the reason this file exists:
//!
//! - **Upstream gives each level its own executor; here all three share one.**
//!   Upstream's `A`, `B` and `C` each hold a `tf::Executor`, so its nested
//!   `run_n(..).wait()` blocks a worker of one pool on work queued to another
//!   -- which costs a thread but cannot starve, because the pool doing the
//!   work is never the pool that is waiting. One pool waiting on itself is the
//!   deadlock [`Executor::corun`] exists for, so that is what this translation
//!   does: one executor, and every nested run a `corun`. It is upstream's
//!   graph and upstream's arithmetic over the arrangement upstream's own
//!   documentation says will deadlock without `corun`.
//! - **`run_n` is a later issue**, so `run_n(taskflow, n)` is spelled as `n`
//!   runs of the same taskflow -- which is what it is: each run of a taskflow
//!   finishes before the next one starts.
//!
//! The counter is an `AtomicUsize` where upstream's is an `int&`. Upstream can
//! get away with the plain `int` because its chains serialise every task that
//! touches it; this port's task bodies are `Send + 'static`, so the sharing
//! has to be spelled out anyway.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::prelude::*;

// The matrix macro and the counting body every task here attaches.
// `common/counters.rs` is not among them: the counters below are read inside
// a task body and from a spinning thread as well as from the assertions, so
// they are spelled out where they are read.
#[macro_use]
mod common;
#[path = "common/bodies.rs"]
mod bodies;

use bodies::bump;

/// Upstream's `executor.run_n(taskflow, runs).wait()`.
///
/// From a worker of `executor` that is `corun`, which is the whole point of
/// the case: a blocking wait here would park the only threads that can run
/// what it is waiting for. From any other thread -- the one that starts the
/// outermost run -- it is `run(..).wait()`, which is what a thread outside the
/// pool should do and what `corun` refuses to do.
fn run_n(executor: &Executor, taskflow: &Taskflow, runs: usize) {
    match executor.this_worker() {
        Some(worker) => {
            assert!(
                worker.id() < executor.num_workers(),
                "a worker's id is an index into its own executor"
            );
            for _ in 0..runs {
                executor.corun(taskflow).expect("a worker may corun");
            }
        }
        None => {
            for _ in 0..runs {
                executor.run(taskflow).wait().expect("no task panicked");
            }
        }
    }
}

/// Upstream's `A::run`: a two-task chain, ten runs, 20 bumps.
fn run_a(executor: &Arc<Executor>, counter: &Arc<AtomicUsize>) {
    let mut taskflow = Taskflow::new();
    let (a1, a2) = taskflow.emplace_n((bump(counter), bump(counter)));
    taskflow.task(a1).precede([a2]);

    run_n(executor, &taskflow, 10);
}

/// Upstream's `B::run`: a two-task chain whose second task is a whole run of
/// `A`, a hundred runs, 100 * (2 + 20) = 2200 bumps.
fn run_b(executor: &Arc<Executor>, counter: &Arc<AtomicUsize>) {
    let mut taskflow = Taskflow::new();
    let b1 = taskflow.emplace(bump(counter));
    let b2 = taskflow.emplace({
        let executor = Arc::clone(executor);
        let counter = Arc::clone(counter);
        let mut bump = bump(&counter);
        move || {
            bump();
            run_a(&executor, &counter);
        }
    });
    taskflow.task(b1).precede([b2]);

    run_n(executor, &taskflow, 100);
}

/// Upstream's `C::run`: the same shape again over `B`, a hundred runs,
/// 100 * (2 + 2200) = 220200 bumps.
fn run_c(executor: &Arc<Executor>, counter: &Arc<AtomicUsize>) {
    let mut taskflow = Taskflow::new();
    let c1 = taskflow.emplace(bump(counter));
    let c2 = taskflow.emplace({
        let executor = Arc::clone(executor);
        let counter = Arc::clone(counter);
        let mut bump = bump(&counter);
        move || {
            bump();
            run_b(&executor, &counter);
        }
    });
    taskflow.task(c1).precede([c2]);

    run_n(executor, &taskflow, 100);
}

/// Upstream's `nested_runs(unsigned w)`.
fn nested_runs(workers: usize) {
    let executor = Arc::new(Executor::new(workers));
    let counter = Arc::new(AtomicUsize::new(0));

    // What this family costs is iteration rather than waiting: the 10/100/100
    // fan-out above is upstream's own, and it is 110100 nested runs of a
    // two-task graph -- 220200 bodies, and a topology set up, coruned and torn
    // down for each run -- with no sleep anywhere in it.
    run_c(&executor, &counter);

    assert_eq!(counter.load(Ordering::Relaxed), 220_200);

    // Every one of those 110100 runs is accounted for, the inner ones
    // included: a `corun` counts its topology the way `run` does, so a leak
    // here would show up as a `wait_for_all` that never returns.
    executor.wait_for_all();
    assert_eq!(executor.num_topologies(), 0);
}

worker_matrix! {
    nested_runs {
        nested_runs_1_thread(1),
        nested_runs_2_threads(2),
        nested_runs_3_threads(3),
        nested_runs_4_threads(4),
        nested_runs_8_threads(8),
        nested_runs_16_threads(16),
    }
}

// --------------------------------------------------------
// Testcase: corun_until
// --------------------------------------------------------

/// `corun_until`'s own case, which upstream has only as the documented example
/// on the method: a worker waiting for something the executor knows nothing
/// about -- there, a `std::async` future; here, a spawned thread.
///
/// The shape is what makes it a test rather than a demonstration. One worker,
/// and the thread it is waiting for will not finish until the counter reaches
/// `BUMPS`, which only that one worker can do. So if `corun_until` parked, or
/// looped without taking work, this would hang -- and if it is the loop it
/// claims to be, the worker runs the rest of the graph while it waits.
#[test]
fn corun_until_runs_the_work_a_parked_worker_would_have_slept_through() {
    const BUMPS: usize = 8;

    let executor = Arc::new(Executor::new(1));
    let counter = Arc::new(AtomicUsize::new(0));

    let mut taskflow = Taskflow::new();
    for _ in 0..BUMPS {
        taskflow.emplace(bump(&counter));
    }
    taskflow.emplace({
        let executor = Arc::clone(&executor);
        let counter = Arc::clone(&counter);
        move || {
            let gate = std::thread::spawn({
                let counter = Arc::clone(&counter);
                move || {
                    while counter.load(Ordering::Relaxed) < BUMPS {
                        std::thread::yield_now();
                    }
                }
            });

            executor
                .corun_until(|| gate.is_finished())
                .expect("a worker may corun");

            gate.join().expect("the gate thread panicked");
        }
    });

    executor.run(&taskflow).wait().expect("no task panicked");
    assert_eq!(counter.load(Ordering::Relaxed), BUMPS);
    executor.wait_for_all();
}

// --------------------------------------------------------
// Testcase: the taskflow a corun returned from
// --------------------------------------------------------

/// A chain of `tasks` tasks, each bumping `counter`: the graph a round of the
/// case below composes.
fn chain(tasks: usize, counter: &Arc<AtomicUsize>) -> Taskflow {
    let mut taskflow = Taskflow::new();
    let mut previous = None;
    for _ in 0..tasks {
        let task = taskflow.emplace(bump(counter));
        if let Some(previous) = previous {
            taskflow.task(previous).precede([task]);
        }
        previous = Some(task);
    }
    taskflow
}

/// A `corun` that has returned leaves its taskflow editable, which is the same
/// promise `RunHandle::wait` makes and is issue #112.
///
/// `tests/modules.rs::a_composition_is_editable_the_instant_its_run_reports_done`
/// is this case one entry point over: #109 found that a node which *preempts*
/// is dequeued as a job and that job is then copied -- onto the frame still
/// inside the dispatch arm, into the parent its nested graph is scheduled
/// under, and once more as the job the last child re-queues to resume it --
/// and that only the last of those three is ever spent by finishing it. The
/// other two hold a share of the taskflow's storage for a few instructions
/// after the resumed node has finished, so the thread that was waiting can be
/// released while a share is still outstanding and its next `emplace` panics.
///
/// #109 fixed that for a thread waiting on a run's completion slot, by making
/// the last share of the run's hold on the storage what releases it. A corun
/// frame is not released by that slot at all: it waits on an *anchor*, in the
/// work-stealing loop, until the anchor's join counter reaches zero -- so the
/// ordering `Drop for Storage` establishes does not reach it, and the same
/// straggling copies can outlive the zero this frame is watching for.
///
/// Four things have to coincide, which is the whole shape of this case:
///
/// - **a corun**, so that the wait is on an anchor rather than on a run;
/// - **a preempting node under it**, which is the composition: a module task
///   of the coruned graph is the node whose job gets copied;
/// - **an edit with nothing in between** -- not a sleep, not a retry -- on a
///   taskflow the corunning body *owns*, which is what makes `Arc::get_mut`
///   in `Taskflow::graph_mut` the assertion;
/// - **contention**, because the window is a handful of instructions wide and
///   the thread that has to lose the race is another worker. Several threads
///   each running their own executor is what supplies it, as it is in #109's
///   two cases.
///
/// The one number that is not arbitrary is the four workers. One runs the body
/// that coruns, one runs the coruned graph, and the other two are **parked** --
/// which is what widens the window from "a few instructions" to "a syscall":
/// the worker that re-queues the job resuming the preempted node notifies the
/// pool while still holding the copy, and waking a parked thread is far slower
/// than the corunning frame needs to steal that job, finish it and return. At
/// two workers, with nobody to wake, this case never reproduced the race at
/// all -- 72000 coruns of it pinned to two cores, with the share count
/// instrumented, saw no straggler. At four it fails against the unfixed
/// scheduler 8 runs out of 8 unpinned and 6 out of 8 pinned to two cores.
///
/// A round is one corun of a three-task graph and one edit, so the whole case
/// is under a tenth of a second.
#[test]
fn a_coruned_taskflow_is_editable_the_instant_corun_returns() {
    /// Rounds per thread. Each is one corun and one edit, and the edit is the
    /// assertion.
    const ROUNDS: usize = 400;
    /// Threads, each with a four-worker executor of its own.
    const THREADS: usize = 12;

    let editors: Vec<std::thread::JoinHandle<()>> = (0..THREADS)
        .map(|_| {
            std::thread::spawn(|| {
                // Four, for the reason the docs above give: one worker for
                // the body, one for the graph it coruns, and two parked.
                let executor = Arc::new(Executor::new(4));
                let counter = Arc::new(AtomicUsize::new(0));
                let target = Arc::new(chain(2, &counter));

                // One task, which does every round from inside a worker:
                // `corun` is a worker's call, so the taskflow being edited has
                // to be one a task body owns.
                let mut driver = Taskflow::new();
                driver.emplace({
                    let executor = Arc::clone(&executor);
                    let counter = Arc::clone(&counter);
                    let target = Arc::clone(&target);
                    move || {
                        for round in 0..ROUNDS {
                            let mut coruned = Taskflow::new();
                            let own = coruned.emplace(bump(&counter));
                            let module = coruned.composed_of(Arc::clone(&target));
                            coruned.task(own).precede([module]);

                            counter.store(0, Ordering::Relaxed);
                            executor.corun(&coruned).expect("a worker may corun");
                            assert_eq!(
                                counter.load(Ordering::Relaxed),
                                3,
                                "round {round}: one task and a two-task composition"
                            );

                            // The edit. `Taskflow::graph_mut` panics here if
                            // any share of this taskflow's storage is still
                            // outstanding.
                            coruned.clear();
                            assert!(coruned.is_empty(), "round {round}: the edit took");
                        }
                    }
                });

                executor
                    .run(&driver)
                    .wait()
                    .expect("the body that coruns and edits");
            })
        })
        .collect();

    for editor in editors {
        assert!(
            editor.join().is_ok(),
            "a corun returned while a share of the taskflow it ran was still held"
        );
    }
}

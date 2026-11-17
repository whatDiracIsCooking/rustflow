//! Translated from `unittests/test_modules.cpp` (Taskflow v4.1.0).
//!
//! Four cases of that file, and all four are about one claim: a module task
//! runs the whole of another taskflow's graph before its own successors run,
//! however deeply the compositions nest and however many runs of the
//! composing taskflow are queued behind each other. The arithmetic is what
//! carries that -- a composition of a chain of five, run `n` times, has to
//! count `15 * n` and not `10 * n` or `15 * n - 5`.
//!
//! A fifth case at the end is this port's own, and says so where it sits:
//! cancelling a run while a module's target graph is part-way through. It
//! stands in for upstream's `CancelComposition` from
//! `unittests/test_cancellation.cpp`, which asserts only that a hundred
//! cancelled runs of a four-level composition all finish, where this one
//! pins which tasks ran.
//!
//! Two cases of that file are deliberately not here, each named with the
//! milestone that brings it:
//!
//! - `Module4` builds its module tasks with `tf::make_module_task` from
//!   `taskflow/algorithm/module.hpp`, which is the algorithm library and
//!   milestone 6;
//! - `Module.ModuleWithAsyncLaunch` and `Module.AsyncLaunchWithModule` launch
//!   their compositions through `executor.async`, which is milestone 5.
//!
//! Nothing here composes an *adopted* graph either, and nothing upstream does
//! in this file: `adopt` is the owned form of the same node kind, and running
//! one waits on issue #74 -- `executor::module` says why the two forms part
//! company in this port where they do not upstream.
//!
//! Three adaptations:
//!
//! - **The target is shared, not borrowed.** Upstream's `composed_of(f0)`
//!   takes a reference and leaves `f0` alive by convention; here the target is
//!   an `Arc<Taskflow>`, so every case wraps its targets once they are built
//!   and never edits them again. That is the port's ownership rule rather than
//!   anything about these cases.
//! - **1..=4 workers, where upstream sweeps 1..=8** for the three `Module`
//!   cases; `ParallelModules` is 1..=4 upstream too. The sweep exists to
//!   cover "one worker does everything" and "several workers steal the
//!   module's children", and four reaches both; CI runs on about two cores,
//!   so the upper half of upstream's sweep would cost four more
//!   oversubscribed runs of the heaviest cases in this suite for no shape the
//!   first four do not already make.
//! - **Nothing here is `#[ignore]`d, which was measured rather than
//!   assumed.** These are the heaviest cases in the suite by task count --
//!   `Module1` alone runs its composition 7500 times, `ParallelModules`
//!   builds 80000 tasks -- and the sixteen of them together take 1.7 seconds
//!   run one after another in a debug build, 1.5 pinned to a single core. So
//!   each is a tenth of a second rather than the seconds that would earn an
//!   `#[ignore]`, and they stay in the default run where a regression will be
//!   seen.
//!
//! One shape worth naming, because it is the difference between these cases
//! and the one thing a module task may not do. A target composed twice into
//! one taskflow is one graph run twice in one run of that taskflow, and
//! upstream orders the two module tasks with an edge every time it does it --
//! `m1_1.precede(m1_2)`. That is not decoration: two *unordered* module tasks
//! over one target would put two workers on one node, which
//! `executor::module` documents as the caller's error and `Work::invoke`
//! reports as a panic. Every `precede` between two module tasks below is
//! upstream's own.
//!
//! The hundred queued runs each case ends with are fine for the same reason
//! written from the other side: runs of one taskflow serialise through that
//! taskflow's run queue, so a hundred `run`s of a composition are a hundred
//! runs of each target in a row rather than at once.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rustflow::prelude::*;
use rustflow::{Executor, RunHandle};

/// How many times a task body has run, shared with that body.
type Counter = Arc<AtomicUsize>;

/// A fresh counter.
fn counter() -> Counter {
    Arc::new(AtomicUsize::new(0))
}

/// A target taskflow of `tasks` tasks in a chain, each counting itself:
/// upstream's `f0`, whose five tasks are `A -> B -> C -> D -> E`.
///
/// Shared from the start, since a module task owns a share of its target.
fn chain(tasks: usize, counter: &Counter) -> Arc<Taskflow> {
    let mut taskflow = Taskflow::new();
    let tasks: Vec<Task> = (0..tasks)
        .map(|_| {
            let counter = Arc::clone(counter);
            taskflow.emplace(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect();
    taskflow.linearize(tasks.iter().copied());
    Arc::new(taskflow)
}

/// A taskflow composed of `target` twice, the two module tasks ordered:
/// upstream's `m1_1 = f1.composed_of(f0); m1_2 = f1.composed_of(f0);
/// m1_1.precede(m1_2)`, which is the level-doubling step `Module2` and
/// `Module3` are built out of.
fn composed_twice(target: &Arc<Taskflow>) -> Arc<Taskflow> {
    let mut taskflow = Taskflow::new();
    let first = taskflow.composed_of(Arc::clone(target));
    let second = taskflow.composed_of(Arc::clone(target));
    taskflow.task(first).precede([second]);
    Arc::new(taskflow)
}

/// Upstream's `for(int n=0; n<100; n++) { cnt = 0; executor.run_n(f, n).get();
/// REQUIRE(cnt == k*n); }`.
///
/// This is the assertion all three `Module` cases turn on, `n = 0` included:
/// a run count of zero runs the graph not at all, so a composition that
/// counted anything there would be a module task running without its node.
fn run_n_counts_per_run(
    executor: &Executor,
    taskflow: &Taskflow,
    counter: &Counter,
    per_run: usize,
    context: &str,
) {
    for n in 0..100 {
        counter.store(0, Ordering::Relaxed);
        executor
            .run_n(taskflow, n)
            .wait()
            .expect("no task of a composition panicked");
        assert_eq!(
            counter.load(Ordering::Relaxed),
            per_run * n,
            "{context}: {n} run(s) of a composition counting {per_run} each"
        );
    }
}

/// Upstream's trailing `cnt = 0; for(int n=0; n<100; n++) executor.run(f);
/// executor.wait_for_all(); REQUIRE(cnt == 100*k);`: a hundred runs queued
/// before anything waits.
///
/// The handles are held across the wait rather than dropped as upstream drops
/// its futures, because dropping one here blocks until that run is over --
/// which would turn the hundred queued runs into a hundred runs waited for one
/// at a time, and the queueing is the point. `tests/basics.rs` makes the same
/// move for the same reason.
fn a_hundred_queued_runs_count(
    executor: &Executor,
    taskflow: &Taskflow,
    counter: &Counter,
    per_run: usize,
    context: &str,
) {
    counter.store(0, Ordering::Relaxed);
    let handles: Vec<RunHandle<'_>> = (0..100).map(|_| executor.run(taskflow)).collect();
    executor.wait_for_all();
    assert_eq!(
        counter.load(Ordering::Relaxed),
        per_run * 100,
        "{context}: a hundred queued runs of a composition counting {per_run} each"
    );
    drop(handles);
}

// ----------------------------------------------------------------------------
// Testcase: Module1
// ----------------------------------------------------------------------------

/// Upstream's `module1(W)`: one module task of `f0` at the end of `f1`'s own
/// chain of five, then a second module task of the same `f0` behind the first.
///
/// The four measurements are upstream's, and each adds one thing to the last:
/// a single run counts ten (five of `f1`, five of `f0`); a hundred runs count
/// a thousand, which is where a module node that forgot to clear `PREEMPTED`
/// would stop the second run dead; `15 * n` for every `n` once the second
/// module task is there, which is where a node whose join counter was not
/// re-armed would come up short; and the hundred queued runs, which is the
/// same arithmetic with the runs overlapping in the taskflow's queue instead
/// of being waited for.
fn module1(workers: usize) {
    let context = format!("{workers} worker(s)");
    let executor = Executor::new(workers);
    let counter = counter();

    let f0 = chain(5, &counter);

    let mut f1 = Taskflow::new();
    let own: Vec<Task> = (0..5)
        .map(|_| {
            let counter = Arc::clone(&counter);
            f1.emplace(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect();
    f1.linearize(own.iter().copied());
    let first_module = f1.composed_of(Arc::clone(&f0));
    f1.task(own[4]).precede([first_module]);

    executor.run(&f1).wait().expect("no task panicked");
    assert_eq!(
        counter.load(Ordering::Relaxed),
        10,
        "{context}: five tasks of `f1` and the five of its module"
    );

    counter.store(0, Ordering::Relaxed);
    executor.run_n(&f1, 100).wait().expect("no task panicked");
    assert_eq!(
        counter.load(Ordering::Relaxed),
        10 * 100,
        "{context}: a hundred runs of the same composition"
    );

    // Upstream's second module task of the same target, ordered behind the
    // first -- which is what makes `f0` run twice per run of `f1`.
    let second_module = f1.composed_of(Arc::clone(&f0));
    f1.task(first_module).precede([second_module]);

    run_n_counts_per_run(&executor, &f1, &counter, 15, &context);
    a_hundred_queued_runs_count(&executor, &f1, &counter, 15, &context);
}

#[test]
fn module1_1_worker() {
    module1(1);
}

#[test]
fn module1_2_workers() {
    module1(2);
}

#[test]
fn module1_3_workers() {
    module1(3);
}

#[test]
fn module1_4_workers() {
    module1(4);
}

// ----------------------------------------------------------------------------
// Testcase: Module2
// ----------------------------------------------------------------------------

/// Upstream's `module2(W)`: a two-level composition, `f2` of two `f1`s of two
/// `f0`s, where `f0` is a chain of five. Each level doubles, so a run of `f2`
/// counts twenty.
///
/// What this adds to `Module1` is a module task whose *target* contains module
/// tasks: the parent chain is two deep, and the node resumed by the last job
/// of a target graph is itself a node somebody is waiting for.
fn module2(workers: usize) {
    let context = format!("{workers} worker(s)");
    let executor = Executor::new(workers);
    let counter = counter();

    let f0 = chain(5, &counter);
    let f1 = composed_twice(&f0);
    let f2 = composed_twice(&f1);

    run_n_counts_per_run(&executor, &f2, &counter, 20, &context);
    a_hundred_queued_runs_count(&executor, &f2, &counter, 20, &context);
}

#[test]
fn module2_1_worker() {
    module2(1);
}

#[test]
fn module2_2_workers() {
    module2(2);
}

#[test]
fn module2_3_workers() {
    module2(3);
}

#[test]
fn module2_4_workers() {
    module2(4);
}

// ----------------------------------------------------------------------------
// Testcase: Module3
// ----------------------------------------------------------------------------

/// Upstream's `module3(W)`: a three-level composition over a chain of two, so
/// a run of `f3` counts sixteen.
///
/// The level that `Module2` does not reach is the one worth having: the parent
/// chain is three deep, which is the first shape where a job's chain of
/// parents is longer than anything the executor can produce by accident.
fn module3(workers: usize) {
    let context = format!("{workers} worker(s)");
    let executor = Executor::new(workers);
    let counter = counter();

    let f0 = chain(2, &counter);
    let f1 = composed_twice(&f0);
    let f2 = composed_twice(&f1);
    let f3 = composed_twice(&f2);

    run_n_counts_per_run(&executor, &f3, &counter, 16, &context);
    a_hundred_queued_runs_count(&executor, &f3, &counter, 16, &context);
}

#[test]
fn module3_1_worker() {
    module3(1);
}

#[test]
fn module3_2_workers() {
    module3(2);
}

#[test]
fn module3_3_workers() {
    module3(3);
}

#[test]
fn module3_4_workers() {
    module3(4);
}

// ----------------------------------------------------------------------------
// Testcase: ParallelModules
// ----------------------------------------------------------------------------

/// Upstream's `parallel_modules(W)`: a hundred taskflows of a hundred
/// eight-task fragments each, every one of them composed into one taskflow
/// that is run once. `100 * 100 * 8 = 80000` tasks under a hundred module
/// tasks.
///
/// The hundred module tasks here are independent, which is the one case in
/// this file where they are -- and it is not the forbidden shape, because the
/// hundred targets are a hundred *different* taskflows. What it does test is
/// the scheduler under a hundred live parents at once: a hundred `Nested`
/// chains, each counting down its own module node, and a run that only
/// finishes when the last of them has resumed its node.
fn parallel_modules(workers: usize) {
    let context = format!("{workers} worker(s)");
    let executor = Executor::new(workers);
    let counter = counter();

    let mut taskflow = Taskflow::new();
    for _ in 0..100 {
        let mut target = Taskflow::new();
        for _ in 0..100 {
            // Upstream's eight tasks and its four edges: `A -> {B, C}` and
            // `D -> {E, F}`, with `G` and `H` left independent.
            let fragment: Vec<Task> = (0..8)
                .map(|_| {
                    let counter = Arc::clone(&counter);
                    target.emplace(move || {
                        counter.fetch_add(1, Ordering::Relaxed);
                    })
                })
                .collect();
            target.task(fragment[0]).precede([fragment[1], fragment[2]]);
            target.task(fragment[3]).precede([fragment[4], fragment[5]]);
        }
        taskflow.composed_of(Arc::new(target));
    }

    executor.run(&taskflow).wait().expect("no task panicked");
    assert_eq!(
        counter.load(Ordering::Relaxed),
        80000,
        "{context}: every task of every composed taskflow ran exactly once"
    );
}

#[test]
fn parallel_modules_1_worker() {
    parallel_modules(1);
}

#[test]
fn parallel_modules_2_workers() {
    parallel_modules(2);
}

#[test]
fn parallel_modules_3_workers() {
    parallel_modules(3);
}

#[test]
fn parallel_modules_4_workers() {
    parallel_modules(4);
}

// ----------------------------------------------------------------------------
// Testcase: cancelling a composition
// ----------------------------------------------------------------------------

/// Long enough that only a scheduling bug reaches it, short enough to report
/// rather than hang a CI run: `tests/work_stealing.rs` carries the same
/// constant for the same reason, which is that `cargo test` has no
/// per-case timeout where doctest gave upstream one.
const GIVE_UP_AFTER: Duration = Duration::from_secs(120);

/// Spins until `ready`, and fails with `what` rather than spinning for ever.
fn spin_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + GIVE_UP_AFTER;
    while !ready() {
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        thread::yield_now();
    }
}

/// Cancelling a run skips the tasks of a module graph it had not started, and
/// the run still finishes.
///
/// This is the deterministic core of upstream's `CancelComposition`, which
/// cancels a hundred runs of a four-level composition a hundred times over and
/// asserts only that they all finish -- a shape that is about volume rather
/// than about which tasks ran. The rendezvous here pins the thing itself: the
/// first task of the target graph is inside the module when the run is
/// cancelled, so the three tasks behind it are the module's *remaining* tasks,
/// and they are skipped by the same early-out as any other task of a cancelled
/// run. Nothing but that early-out is involved -- every job of a target graph
/// holds a share of the run's own topology, so `Job::is_parent_cancelled`
/// answers yes for all of them without the parent chain being walked.
///
/// The module node is still entered twice, which is what keeps the run from
/// hanging: the skipped children are torn down rather than dropped, the last
/// of them resumes the node, and the second entry clears `PREEMPTED` and
/// releases successors that are themselves skipped. A `wait` that returned at
/// all is the whole of that claim.
#[test]
fn cancelling_a_run_skips_the_rest_of_a_module() {
    for workers in 1..=4 {
        let context = format!("{workers} worker(s)");
        let executor = Executor::new(workers);
        let counter = counter();
        let inside = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::new(AtomicBool::new(false));

        let mut target = Taskflow::new();
        let gate = target.emplace({
            let inside = Arc::clone(&inside);
            let cancelled = Arc::clone(&cancelled);
            move || {
                inside.store(true, Ordering::Release);
                spin_until("the run to be cancelled", || {
                    cancelled.load(Ordering::Acquire)
                });
            }
        });
        let rest: Vec<Task> = (0..3)
            .map(|_| {
                let counter = Arc::clone(&counter);
                target.emplace(move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                })
            })
            .collect();
        target.linearize([gate].into_iter().chain(rest.iter().copied()));

        let mut taskflow = Taskflow::new();
        taskflow.composed_of(Arc::new(target));

        let run = executor.run(&taskflow);
        spin_until("the module's target graph to start", || {
            inside.load(Ordering::Acquire)
        });
        assert!(
            run.cancel(),
            "{context}: the run was still in flight, so there was something to cancel"
        );
        cancelled.store(true, Ordering::Release);

        run.wait().expect("cancelling a run is not a failure of it");
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "{context}: the tasks behind the gate were never started"
        );
    }
}

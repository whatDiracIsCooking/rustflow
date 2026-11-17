//! Translated from `unittests/test_runtimes.cpp` (Taskflow v4.1.0).
//!
//! Two cases of that file are here, and they are the two that need nothing
//! but a runtime task: `Runtime.Schedule.ModuleTask`, which bypasses a
//! graph's dependencies to run a module task the run would otherwise never
//! reach, and `Runtime.Cancel`, where a spinning body asks whether its run
//! has been cancelled and stops when it has. Two of this port's own follow,
//! on the public surface of the handle and on what `schedule` does with a
//! task that is not the runtime task's to schedule.
//!
//! The rest of upstream's file is named rather than half-ported, each with
//! what it waits on:
//!
//! - `Runtime.ExternalGraph.Simple` needs `run_n` and `Executor::corun`
//!   together, and is issue #55's;
//! - `Fibonacci`, `FibonacciSwapped`, `ImplicitJoin`,
//!   `RecursiveImplicitJoin`, `MergeSort` and `QuickSort` are all built on
//!   `rt.async` / `rt.silent_async`, which is milestone 5. They are also the
//!   only cases that give a runtime task children, so they are what will
//!   first reach the preemption branch of the dispatch arm from the public
//!   API; until then that branch is driven by hand in the unit tests beside
//!   it.
//!
//! Three adaptations, all of them the port's shapes rather than changes to
//! what is being asserted:
//!
//! - **The counter is an atomic.** Upstream's cases share a plain `int` by
//!   reference across tasks that cannot overlap. A task body here is
//!   `'static`, so the counter is an `Arc<AtomicI32>`, and the orderings are
//!   deliberately `Relaxed` -- what orders these increments against each
//!   other is the graph, and against the assertion is the `wait`.
//! - **The module node is built as a module node.** Upstream writes
//!   `tf.placeholder()` and turns it into a module task later with
//!   `module_task.composed_of(module_flow)`; `composed_of` here is a builder
//!   method that creates the node, so the node is created as one and the
//!   edges are added afterwards. Same graph, same indices in the same order.
//! - **The worker counts are this port's.** Upstream runs both cases on a
//!   default executor, which is one worker per core. Each is run at 1, 2, 3
//!   and 4 workers instead, because one worker is the interesting case for
//!   both -- a `schedule` that went to the wrong queue and a cancel that
//!   needed a spare thread would each pass at four and hang at one.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rustflow::prelude::*;
use rustflow::{Executor, RunError};

/// How long a handshake waits for the other side before it gives up.
///
/// The deadline-asserting spin the rest of the suite uses -- `spin_until`
/// below is the one in `executor::run_handle`'s own cases -- rather than a
/// bare loop: a scheduling defect here would otherwise hang the suite instead
/// of failing one case.
const GIVE_UP_AFTER: Duration = Duration::from_secs(120);

/// Spins until `flag` is set, failing rather than hanging if it never is.
fn spin_until(what: &str, flag: &AtomicBool) {
    let deadline = Instant::now() + GIVE_UP_AFTER;
    while !flag.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        thread::yield_now();
    }
}

/// Upstream's `Runtime.Schedule.ModuleTask`.
///
/// The graph is built so that the *only* way the module task runs is the
/// runtime task scheduling it: a condition task at the entry picks its first
/// successor, which is the runtime task, so the branch through `A` -- the
/// module task's one strong predecessor, and the one task that would set
/// `value` to -100 -- is never taken. `value == 5` is therefore the whole
/// assertion: one for the runtime body, two for the module's own graph, and
/// two for the chain the module node releases when it is done.
///
/// That last part is what makes this more than a test of `schedule`: a
/// scheduled module task is still entered twice, preempted for its target
/// graph in between, and its successors still wait for the whole of it.
#[test]
fn a_runtime_task_schedules_a_module_task_the_graph_cannot_reach() {
    for workers in 1..=4 {
        let value = Arc::new(AtomicI32::new(0));

        let module_flow = Arc::new({
            let mut module_flow = Taskflow::new();
            let m1 = module_flow.emplace(count(&value));
            let m2 = module_flow.emplace(count(&value));
            module_flow.task(m1).precede([m2]);
            module_flow
        });

        let mut taskflow = Taskflow::new();
        let a = {
            let value = Arc::clone(&value);
            taskflow.emplace(move || value.store(-100, Ordering::Relaxed))
        };
        let module = taskflow.composed_of(Arc::clone(&module_flow));
        let b = taskflow.emplace(count(&value));
        let c = taskflow.emplace(count(&value));
        taskflow.task(a).precede([module]);
        taskflow.task(module).precede([b]);
        taskflow.task(b).precede([c]);

        let entrypoint = taskflow.emplace_condition(|| 0);
        let schedule = {
            let value = Arc::clone(&value);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                value.fetch_add(1, Ordering::Relaxed);
                runtime.schedule(module);
            })
        };
        taskflow.task(entrypoint).precede([schedule, a]);

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(
            value.load(Ordering::Relaxed),
            5,
            "{workers} worker(s): the scheduled module task and everything \
             after it ran, and `A` did not"
        );
    }
}

/// A body that bumps `value`, which is upstream's `[&](){ value++; }`.
fn count(value: &Arc<AtomicI32>) -> impl FnMut() + Send + 'static {
    let value = Arc::clone(value);
    move || {
        value.fetch_add(1, Ordering::Relaxed);
    }
}

/// Upstream's `Runtime.Cancel`.
///
/// Cancellation is not preemptive -- nothing interrupts a body -- so the only
/// way a running task learns of it is to ask, and `is_cancelled` is the
/// question. The handshake is upstream's and is what the case is really
/// about: the cancel has to arrive *after* the body has started, or it would
/// cancel the run before the runtime task was ever dequeued and the body
/// would never see anything.
#[test]
fn a_runtime_body_stops_when_its_run_is_cancelled() {
    for workers in 1..=4 {
        let reached = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::new(AtomicBool::new(false));

        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime({
            let reached = Arc::clone(&reached);
            let cancelled = Arc::clone(&cancelled);
            move |runtime: &mut Runtime<'_>| {
                reached.store(true, Ordering::Release);

                // Upstream's `while(!cancelled)` with a deadline on it, for
                // the reason `spin_until` has one: a defect that stopped the
                // cancel from being visible here should fail this case rather
                // than hang the suite. The body spins rather than sleeping
                // because it is on a worker thread -- the one this executor
                // may have -- and sleeping would be the deadlock
                // `Executor::corun` exists to avoid.
                let deadline = Instant::now() + GIVE_UP_AFTER;
                while !cancelled.load(Ordering::Acquire) {
                    assert!(
                        Instant::now() < deadline,
                        "gave up waiting for the cancellation to be visible"
                    );
                    thread::yield_now();
                    if runtime.is_cancelled() {
                        cancelled.store(true, Ordering::Release);
                        break;
                    }
                }
            }
        });

        let executor = Executor::new(workers);
        let run = executor.run(&taskflow);
        spin_until("the runtime body to start", &reached);
        assert!(run.cancel(), "the run had not finished");

        // A cancelled run completes normally, which is upstream's
        // `future.get()` not throwing.
        run.wait().expect("a cancelled run is not a failed one");
        assert!(
            cancelled.load(Ordering::Acquire),
            "{workers} worker(s): the body saw the cancellation"
        );
    }
}

/// This port's own: what a runtime body can see of where it is running.
///
/// Upstream's `rt.executor()` has no equivalent here -- the README's design
/// decisions say why -- so this is the surface that replaced it, asserted
/// from outside the crate where a caller would use it. `corun` is in the same
/// case because this is what it does in this milestone: a body with nothing
/// spawned has nothing to wait for, so both shapes of it return at once.
#[test]
fn a_runtime_body_sees_the_pool_and_coruns_nothing() {
    for workers in 1..=4 {
        let seen = Arc::new(AtomicI32::new(-1));

        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime({
            let seen = Arc::clone(&seen);
            move |runtime: &mut Runtime<'_>| {
                assert!(!runtime.is_cancelled(), "the run is live");
                assert!(runtime.worker().id() < runtime.num_workers());
                runtime.try_corun().expect("nothing was spawned to fail");
                runtime.corun();
                runtime.corun_all();

                let workers =
                    i32::try_from(runtime.num_workers()).expect("a worker count fits in an i32");
                seen.store(workers, Ordering::Relaxed);
            }
        });

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(
            seen.load(Ordering::Relaxed),
            i32::try_from(workers).expect("a worker count fits in an i32"),
            "the body saw the pool it is running on"
        );
    }
}

/// This port's own: `schedule` is limited to the runtime task's own graph,
/// and an index that graph does not have is the stale-index panic rather than
/// whatever node another graph has at it.
///
/// The panic happens inside the body, so it is reported the way any failing
/// task is -- as the run's failure, from `wait` -- which is also what keeps
/// the case from taking a worker thread down with it.
#[test]
fn scheduling_a_task_of_another_graph_is_reported_as_the_run_failing() {
    let other = {
        let mut other = Taskflow::new();
        for _ in 0..3 {
            other.emplace(|| {});
        }
        other
    };
    // The last task of a three-task graph, in a graph of one: an index this
    // runtime task's own graph does not have.
    let foreign = other
        .graph()
        .tasks()
        .last()
        .expect("the other taskflow has tasks");

    let mut taskflow = Taskflow::new();
    taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| runtime.schedule(foreign));

    let error = Executor::new(2)
        .run(&taskflow)
        .wait()
        .expect_err("the body panicked on a task of another graph");

    // One variant today, so this is the shape `tests/exceptions.rs` uses: a
    // `match` rather than a destructuring, because `RunError` is
    // `#[non_exhaustive]` outside the crate.
    match error {
        RunError::Panicked(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .expect("the stale-index panic is a formatted message");
            assert!(
                message.contains("out of range or erased"),
                "unexpected panic message: {message}"
            );
        }
        error => panic!("unexpected failure: {error:?}"),
    }
}

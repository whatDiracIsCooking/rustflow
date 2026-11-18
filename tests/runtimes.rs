//! Translated from `unittests/test_runtimes.cpp` (Taskflow v4.1.0).
//!
//! **The whole of upstream's file is here as of #92.** Two cases need nothing
//! but a runtime task: `Runtime.Schedule.ModuleTask`, which bypasses a
//! graph's dependencies to run a module task the run would otherwise never
//! reach, and `Runtime.Cancel`, where a spinning body asks whether its run
//! has been cancelled and stops when it has. `Runtime.ExternalGraph.Simple`
//! arrived with #55 -- a hundred runtime tasks each corunning a taskflow of
//! its own, a hundred times over, which is the case that puts `run_n` and
//! [`Executor::corun`](rustflow::Executor::corun) under one run.
//!
//! The six that waited for `rt.async` and `rt.silent_async` -- `Fibonacci`,
//! `FibonacciSwapped`, `ImplicitJoin`, `RecursiveImplicitJoin`, `MergeSort`
//! and `QuickSort` -- are the back half of the file. They are the cases that
//! give a runtime task *children*, which is what makes them the first to
//! reach the preemption branch of the dispatch arm from the public API: a
//! body that spawns and does not wait leaves its node preempted, and the last
//! child to finish is what resumes it. Three of this port's own follow, on
//! the public surface of the handle, on what `schedule` does with a task that
//! is not the runtime task's to schedule, and on where a child's panic is
//! reported.
//!
//! # The recursive cases run upstream's own inputs, since #119
//!
//! `Fibonacci`, `FibonacciSwapped`, `MergeSort` and `QuickSort` assert
//! **fib(25)** and sort **up to ten thousand elements**, which are upstream's
//! figures. They did not until #119, and what the reduction was about is
//! worth keeping, because the ceiling it describes still binds
//! `tests/task_group.rs`.
//!
//! A corun keeps its worker in the work-stealing loop, so every job that
//! worker picks up while waiting nests one frame deeper. Until #119 a child
//! spawned from a task body went into the shared buffers, which hand work
//! back **FIFO**, where upstream pushes onto the submitting worker's own
//! **LIFO** queue -- the `if(auto w = this_worker(); w)` branch of
//! `_schedule_async_task`. A corun therefore got the oldest outstanding job
//! in the pool, which is somebody else's unfinished subtree, rather than the
//! child it had just spawned, and the nesting grew one frame per outstanding
//! child instead of one per level of recursion: fib(14) passed and fib(15)
//! aborted, 2000 elements passed and 4000 aborted, so this file ran fib(12)
//! and stopped at a thousand.
//!
//! A [`Runtime`](rustflow::Runtime) now hands its own worker to the
//! submission, so these cases take upstream's branch and nest one frame per
//! level of recursion. Re-measured in a debug build, which is where
//! `cargo test` runs: the whole file passes at upstream's inputs, fib(28) and
//! fib(30) pass too -- the latter in 77 s, which is what now decides the size
//! -- and a chain of nested coruns reaches 500 levels at one worker before a
//! worker's 2 MiB stack gives out. So the bound on these shapes is their task
//! count rather than their depth.
//!
//! `tests/task_group.rs` keeps the reduced inputs, and that is the other half
//! of the same measurement: a [`TaskGroup`](rustflow::TaskGroup) has no
//! `WorkerThread` to hand the submission and waits on the stealing end of the
//! queue in any case, so it stays on the buffers -- `src/task_group.rs`'s
//! module docs have the argument and the figures. `tests/subflows.rs`
//! computes fib(20) at one worker through a *graph*, which was always
//! scheduled by the worker-aware path, and is the comparison that said the
//! cliff was the route and not the depth.
//!
//! Four adaptations, all of them the port's shapes rather than changes to
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
//! - **The worker counts are this port's** for the two cases that had none of
//!   their own. Upstream runs them on a default executor, which is one worker
//!   per core; each is run at 1, 2, 3 and 4 workers instead, because one
//!   worker is the interesting case for both -- a `schedule` that went to the
//!   wrong queue and a cancel that needed a spare thread would each pass at
//!   four and hang at one. The six cases ported in #92 keep upstream's own
//!   counts: 1 to 4 workers for the two Fibonaccis, 1 to 8 for the rest.
//! - **A slot shared with a child stands in for a borrowed local.** Upstream's
//!   children capture `&res1` or `&data` -- references into the frame that
//!   spawned them -- which a `'static` body cannot do. The Fibonaccis hand
//!   each child an `Arc<AtomicU64>` to write its half into, and the two sorts
//!   share the buffer as an `Arc<[AtomicI32]>`; `Shared` below has that
//!   argument in full, including why every access of it can be relaxed.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rustflow::prelude::*;
use rustflow::{Executor, RunError, Xorshift32};

// Only the spin: there is no numbered family here to want the matrix macro,
// and what the cases count is a per-index value in the shared buffer below
// rather than a tally `common/counters.rs` could carry.
#[path = "common/spin.rs"]
mod spin;

use spin::spin_until;

/// How long the one handshake written out below waits before it gives up.
///
/// [`spin_until`] is what every other wait in this suite is, and it is not
/// what that one can be: its loop sets the flag it is waiting on from inside
/// itself. The number is the same 120 seconds for the same reason -- a
/// scheduling defect should fail one case rather than hang the suite.
const GIVE_UP_AFTER: Duration = Duration::from_secs(120);

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
        spin_until("the runtime body to start", || {
            reached.load(Ordering::Acquire)
        });
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
                assert_eq!(runtime.worker_id(), runtime.worker().id());
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

/// This port's own: `Runtime::corun_until` is upstream's
/// `rt.executor().corun_until`, made public rather than private to one
/// algorithm for `scan.hpp`'s own reason -- "need to do public corun because
/// multiple workers can call this" -- and this is that shape: every one of
/// this run's runtime tasks is a source with no edges, so they all reach a
/// shared barrier and every one but the last waits at it.
///
/// One worker is the case worth pinning: with nothing else queued, the
/// barrier can only complete by each waiting call recursing into a job still
/// queued, which is exactly what makes `corun_until` safe to call from
/// inside a body regardless of how many threads the pool actually has.
#[test]
fn a_runtime_bodys_corun_until_passes_a_barrier_every_task_reaches() {
    for workers in 1..=4 {
        let arrived = Arc::new(AtomicI32::new(0));
        let past = Arc::new(AtomicI32::new(0));
        let total = i32::try_from(workers).expect("a worker count fits in an i32");

        let mut taskflow = Taskflow::new();
        for _ in 0..workers {
            let arrived = Arc::clone(&arrived);
            let past = Arc::clone(&past);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                if arrived.fetch_add(1, Ordering::AcqRel) + 1 == total {
                    // The last one through releases everybody else.
                    arrived.store(0, Ordering::Release);
                } else {
                    runtime.corun_until(|| arrived.load(Ordering::Acquire) == 0);
                }
                past.fetch_add(1, Ordering::Relaxed);
            });
        }

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(
            past.load(Ordering::Relaxed),
            total,
            "{workers} worker(s): every runtime task passed the barrier"
        );
    }
}

/// Issue #168's fourth item, confirmed: a runtime-shaped body returned by a
/// free function -- the shape every `make_xxx_task` upstream's algorithm
/// headers define takes -- type-checks through `emplace_runtime` exactly as
/// an inline closure does, with no `Callables` impl needed for it.
#[test]
fn a_runtime_shaped_body_from_a_free_function_type_checks() {
    let counter = Arc::new(AtomicI32::new(0));

    let mut taskflow = Taskflow::new();
    taskflow.emplace_runtime(make_counting_task(&counter));

    Executor::new(2)
        .run(&taskflow)
        .wait()
        .expect("no task panicked");
    assert_eq!(counter.load(Ordering::Relaxed), 1);
}

/// Upstream's `make_xxx_task` free functions, boiled down to the one shape
/// this seam needs: a function returning the `impl for<'a> FnMut(&mut
/// Runtime<'a>) + Send + 'static` that `taskflow.emplace_runtime(..)` wants.
fn make_counting_task(
    counter: &Arc<AtomicI32>,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static {
    let counter = Arc::clone(counter);
    move |runtime: &mut Runtime<'_>| {
        runtime.corun();
        counter.fetch_add(1, Ordering::Relaxed);
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

/// Upstream's `Runtime.ExternalGraph.Simple`: a hundred taskflows of four
/// chained tasks, one runtime task per taskflow corunning it, and the lot run
/// a hundred times -- so each of the hundred counters is bumped four times a
/// round and reads 400 at the end.
///
/// `400` is the whole assertion and it needs no handshake of any kind: it is
/// arithmetic over work the run cannot have finished without doing, since a
/// corun returns only once the graph it scheduled is done and the run returns
/// only once every runtime task has.
///
/// What it adds to the two cases above is the *shape* rather than one more
/// method: a hundred external graphs under one run is where a corun stops
/// being a single nested wait. A worker that enters one corun and then steals
/// another runtime task coruns again on top of it, so the pool works its way
/// through frames stacked however deep the stealing happens to go -- which is
/// exactly the thing the `executor::corun` module docs say a nested `run` plus
/// `wait` cannot survive, and which at one worker has no second thread to hide
/// a mistake behind.
#[test]
fn a_hundred_runtime_tasks_corun_an_external_graph_each() {
    /// Upstream's `N`: how many external graphs, and therefore how many
    /// runtime tasks.
    const GRAPHS: usize = 100;
    /// Upstream's `run_n(taskflow, 100)`.
    const ROUNDS: usize = 100;
    /// Upstream's four-task chain, one per external graph.
    const CHAIN: usize = 4;

    for workers in 1..=4 {
        // Shared with the task bodies, which are `'static` and so cannot
        // borrow the executor the way upstream's lambdas capture it.
        let executor = Arc::new(Executor::new(workers));

        let results: Vec<Arc<AtomicI32>> =
            (0..GRAPHS).map(|_| Arc::new(AtomicI32::new(0))).collect();
        let graphs: Vec<Arc<Taskflow>> = results
            .iter()
            .map(|result| {
                let mut graph = Taskflow::new();
                let chain: Vec<Task> = (0..CHAIN).map(|_| graph.emplace(count(result))).collect();
                graph.linearize(chain);
                Arc::new(graph)
            })
            .collect();

        let mut taskflow = Taskflow::new();
        for graph in &graphs {
            let executor = Arc::clone(&executor);
            let graph = Arc::clone(graph);
            // Upstream's body takes a `tf::Runtime&` it never uses: what the
            // case is about is that the *corun* is made from inside a task,
            // and the handle is there to say which task kind it is made from.
            taskflow.emplace_runtime(move |_runtime: &mut Runtime<'_>| {
                executor
                    .corun(&graph)
                    .expect("a runtime body runs on a worker of this executor");
            });
        }

        executor
            .run_n(&taskflow, ROUNDS)
            .wait()
            .expect("no task panicked");

        let expected =
            i32::try_from(ROUNDS * CHAIN).expect("four tasks on a hundred rounds fit in an i32");
        for (which, result) in results.iter().enumerate() {
            assert_eq!(
                result.load(Ordering::Relaxed),
                expected,
                "{workers} worker(s): graph {which} ran its whole chain on every round"
            );
        }
    }
}

/// Upstream's `fibonacci(size_t N, tf::Runtime& rt)`, the recursion behind
/// `Runtime.Fibonacci`.
///
/// Two adaptations, and both are the port's shapes rather than changes to the
/// algorithm. Upstream's child captures `&res1`, a local of the frame that
/// spawned it; a child body here is `'static` and cannot borrow a stack
/// frame, so the slot it writes is an `Arc<AtomicU64>` shared with it. And
/// `Relaxed` is enough on both ends of that slot: what orders the child's
/// store against this frame's load is the `corun` below, whose predicate is
/// an acquire load of the very counter the child's tear-down released.
fn fibonacci(n: u64, runtime: &mut Runtime<'_>) -> u64 {
    if n < 2 {
        return n;
    }

    let first = Arc::new(AtomicU64::new(0));
    runtime.silent_async_runtime({
        let first = Arc::clone(&first);
        move |child: &mut Runtime<'_>| {
            first.store(fibonacci(n - 1, child), Ordering::Relaxed);
        }
    });

    // Upstream's "tail optimization": the other half of the recursion runs on
    // *this* runtime rather than on a child of its own, so one call spawns
    // one task. Note that it therefore spawns its own children under this
    // node too -- the corun below waits for those as well, which is what
    // makes the predicate "this node's counter is back to one" rather than
    // "my two children are done".
    let second = fibonacci(n - 2, runtime);

    // Upstream's comment: "use corun to avoid blocking the worker from
    // waiting the two children tasks to finish".
    runtime.corun();

    first.load(Ordering::Relaxed) + second
}

/// Upstream's `fibonacci_swapped`: the same recursion with the spawn *after*
/// the tail call rather than before it.
///
/// What the swap is for is the order the counter is touched in -- the direct
/// recursion runs, spawning and corunning on this node, and only then is this
/// level's own child spawned -- so a protocol that happened to work only when
/// every child was spawned before any grandchild would fail here.
fn fibonacci_swapped(n: u64, runtime: &mut Runtime<'_>) -> u64 {
    if n < 2 {
        return n;
    }

    let first = fibonacci_swapped(n - 1, runtime);

    let second = Arc::new(AtomicU64::new(0));
    runtime.silent_async_runtime({
        let second = Arc::clone(&second);
        move |child: &mut Runtime<'_>| {
            second.store(fibonacci_swapped(n - 2, child), Ordering::Relaxed);
        }
    });

    runtime.corun();

    first + second.load(Ordering::Relaxed)
}

/// Upstream's `fibonacci(size_t T, size_t N)`: the recursion above under one
/// `spawn_runtime`, which is its `executor.async([&](tf::Runtime& rt){...})`.
fn fibonacci_on(workers: usize, n: u64, recurse: fn(u64, &mut Runtime<'_>) -> u64) -> u64 {
    let executor = Executor::new(workers);
    let result = Arc::new(AtomicU64::new(0));

    let task = executor.spawn_runtime({
        let result = Arc::clone(&result);
        move |runtime: &mut Runtime<'_>| {
            result.store(recurse(n, runtime), Ordering::Relaxed);
        }
    });
    task.wait().expect("nothing under the recursion panicked");

    result.load(Ordering::Relaxed)
}

/// Upstream's `Runtime.Fibonacci.1thread` through `.4threads`.
///
/// The case that exercises the family recursively: every level spawns one
/// runtime-shaped child under its own node and coruns on it, so the pool
/// works through a tree of tens of thousands of tasks nested as deep as the
/// recursion goes. One worker is the interesting end -- a corun that could
/// not run its own children would have nowhere else to get them from.
///
/// `fib(25)`, which is upstream's own figure: #119 gave a runtime body's
/// submission the queue of the worker running it, so the corun below pops
/// the child it just spawned and the recursion nests one frame per level
/// rather than one per outstanding child. The header has the measurement
/// this file ran at before that, and how far past 25 the shape now goes.
///
/// **How a regression surfaces**: a wrong *answer* for a lost store, and a
/// **hang** for a lost resumption or a counter that never reaches one. That
/// is inherent to the shape upstream is testing, and is why the join-counter
/// protocol itself is pinned by assertions in `executor::runtime_async`'s own
/// white-box cases instead, where every job is driven by hand. What this case
/// adds is the depth and the worker counts.
#[test]
fn a_runtime_task_computes_fibonacci_recursively() {
    for workers in 1..=4 {
        assert_eq!(
            fibonacci_on(workers, 25, fibonacci),
            75025,
            "{workers} worker(s): fib(25)"
        );
    }
}

/// Upstream's `Runtime.FibonacciSwapped.1thread` through `.4threads`.
///
/// [`fibonacci_swapped`] has what the swap is for; everything the case above
/// says about `n` and about how a regression surfaces holds here too.
#[test]
fn a_runtime_task_computes_fibonacci_with_the_spawn_last() {
    for workers in 1..=4 {
        assert_eq!(
            fibonacci_on(workers, 25, fibonacci_swapped),
            75025,
            "{workers} worker(s): fib(25)"
        );
    }
}

/// Upstream's `Runtime.ImplicitJoin.1thread` through `.8threads`.
///
/// Two hundred children waited for *explicitly* with a corun, then two
/// hundred more left to the **implicit** join at the end of the body -- and
/// the whole assertion is that the successor task sees all four hundred. The
/// second half is what the case is named for and is the one a successor can
/// observe: nothing in the body waits for it, so the only thing that can make
/// `400` true is the node staying preempted until its last child is done.
///
/// Upstream's `rt.async` is [`Runtime::spawn`] here, and its discarded
/// `std::future` is a dropped [`AsyncHandle`](rustflow::AsyncHandle) -- which
/// detaches the *result* and not the task, so a dropped handle changes
/// nothing about the join.
///
/// **How a regression surfaces**: as `200` or `400` failing, for a child that
/// was never counted; and as a hang if a child's tear-down never resumes the
/// node, which `executor::runtime_async` pins by hand.
#[test]
fn a_runtime_task_joins_its_children_explicitly_and_implicitly() {
    /// Upstream's loop bound, four times over.
    const CHILDREN: i32 = 100;

    for workers in 1..=8 {
        let counter = Arc::new(AtomicI32::new(0));

        let mut taskflow = Taskflow::new();
        let a = taskflow.emplace({
            let counter = Arc::clone(&counter);
            move || counter.store(0, Ordering::Relaxed)
        });
        let b = taskflow.emplace_runtime({
            let counter = Arc::clone(&counter);
            move |runtime: &mut Runtime<'_>| {
                for _ in 0..CHILDREN {
                    drop(runtime.spawn(count(&counter)));
                }
                for _ in 0..CHILDREN {
                    runtime.silent_async(count(&counter));
                }
                runtime.corun();
                assert_eq!(
                    counter.load(Ordering::Relaxed),
                    2 * CHILDREN,
                    "the corun waited for every child spawned before it"
                );

                for _ in 0..CHILDREN {
                    drop(runtime.spawn(count(&counter)));
                }
                for _ in 0..CHILDREN {
                    runtime.silent_async(count(&counter));
                }
                // No second corun: the implicit join at the end of the body
                // is what the successor below is waiting on.
            }
        });
        let c = taskflow.emplace({
            let counter = Arc::clone(&counter);
            move || {
                assert_eq!(
                    counter.load(Ordering::Relaxed),
                    4 * CHILDREN,
                    "the implicit join waited for the second two hundred"
                );
            }
        });
        taskflow.task(a).precede([b]);
        taskflow.task(b).precede([c]);

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(
            counter.load(Ordering::Relaxed),
            4 * CHILDREN,
            "{workers} worker(s): every child ran"
        );
    }
}

/// Upstream's `Runtime.RecursiveImplicitJoin.1thread` through `.8threads`.
///
/// A hundred children, each spawning ten of its own, with **no corun
/// anywhere**: every one of the 2200 tasks is waited for by an implicit join,
/// and the grandchildren are joined by their own parent's node rather than by
/// the runtime task's. So this is the case that pins that the join nests --
/// a child's node is the parent of its children exactly as the runtime task's
/// node is the parent of the children.
///
/// Upstream spawns one of its two hundreds with `async` and the other with
/// `silent_async`, and swaps which of the two the grandchildren use; this
/// keeps that, because what it covers is both handle-returning forms
/// delivering a *completion* rather than a value.
///
/// **How a regression surfaces**: as the final count, for a lost child; as a
/// hang, for a lost resumption at either level.
#[test]
fn a_runtime_task_joins_its_children_and_their_children() {
    /// Upstream's `N1`.
    const CHILDREN: i32 = 100;
    /// Upstream's `N2`.
    const GRANDCHILDREN: i32 = 10;

    for workers in 1..=8 {
        let counter = Arc::new(AtomicI32::new(0));

        let mut taskflow = Taskflow::new();
        let a = taskflow.emplace({
            let counter = Arc::clone(&counter);
            move || counter.store(0, Ordering::Relaxed)
        });
        let b = taskflow.emplace_runtime({
            let counter = Arc::clone(&counter);
            move |runtime: &mut Runtime<'_>| {
                for _ in 0..CHILDREN {
                    let counter = Arc::clone(&counter);
                    drop(runtime.spawn_runtime(move |child: &mut Runtime<'_>| {
                        counter.fetch_add(1, Ordering::Relaxed);
                        for _ in 0..GRANDCHILDREN {
                            child.silent_async(count(&counter));
                        }
                    }));
                }
                for _ in 0..CHILDREN {
                    let counter = Arc::clone(&counter);
                    runtime.silent_async_runtime(move |child: &mut Runtime<'_>| {
                        counter.fetch_add(1, Ordering::Relaxed);
                        for _ in 0..GRANDCHILDREN {
                            drop(child.spawn(count(&counter)));
                        }
                    });
                }
            }
        });
        let total = 2 * (CHILDREN + CHILDREN * GRANDCHILDREN);
        let c = taskflow.emplace({
            let counter = Arc::clone(&counter);
            move || assert_eq!(counter.load(Ordering::Relaxed), total)
        });
        taskflow.task(a).precede([b]);
        taskflow.task(b).precede([c]);

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(
            counter.load(Ordering::Relaxed),
            total,
            "{workers} worker(s): every child and grandchild ran"
        );
    }
}

/// The input sizes the two sort cases run at, which are upstream's: its merge
/// sort sweeps 10, 100, 1000 and 10000, and its quick sort the same plus 1.
/// One constant covers both, so the merge sort also runs the single element,
/// which costs nothing and asserts the base case twice.
///
/// Ten thousand elements overflowed a worker's stack before #119, which is
/// why this list stopped at a thousand; the module header has both
/// measurements, and `tests/task_group.rs` is where the reduced sizes still
/// live, for the reason that file's header gives.
const SIZES: [usize; 5] = [1, 10, 100, 1000, 10000];

/// The buffer upstream's two sort cases share between a parent and its
/// children: `std::vector<int>` there, `Arc<[AtomicI32]>` here.
///
/// This is the one adaptation the two cases below need, and it is the port's
/// aliasing rules rather than a change to what is being tested. Upstream's
/// children capture `&data` -- a reference to the caller's vector -- and
/// write into disjoint index ranges of it; a child body here is `'static`, so
/// it can hold a share of the buffer but not a borrow of it, and a share of a
/// `Vec` cannot be written through at all. An element that is itself an
/// atomic is the shape that keeps the algorithm: the ranges are disjoint, so
/// no two children ever touch the same element, and every load and store is
/// therefore [`Ordering::Relaxed`] -- what orders a child's writes against
/// the parent that merges them is the corun, whose predicate is an acquire
/// load of the counter the child's tear-down released.
type Shared = Arc<[AtomicI32]>;

/// `n` pseudo-random values under 100 -- upstream's `::rand() % 100` -- as the
/// buffer to sort and the answer to check it against.
///
/// Seeded rather than random: this crate's own [`Xorshift32`] where upstream
/// calls `rand()`, so that a failure is reproducible and no dependency is
/// added for it. The seed is mixed with `n` so that the four sizes of one
/// case are four different inputs.
fn unsorted(n: usize) -> (Shared, Vec<i32>) {
    let mut rng = Xorshift32::new(u32::try_from(n).expect("the sizes here fit in a u32") + 1);
    let values: Vec<i32> = (0..n)
        .map(|_| i32::try_from(rng.next_u32() % 100).expect("a value under 100 fits in an i32"))
        .collect();

    let data: Shared = values.iter().map(|value| AtomicI32::new(*value)).collect();
    let mut sorted = values;
    sorted.sort_unstable();
    (data, sorted)
}

/// What the buffer holds, as a plain `Vec` to compare against the answer.
fn values(data: &Shared) -> Vec<i32> {
    data.iter()
        .map(|slot| slot.load(Ordering::Relaxed))
        .collect()
}

/// Upstream's `std::sort(beg, end)` over one range of the buffer: the base
/// case of both sorts.
///
/// Out and back rather than in place, because `sort_unstable` wants a
/// `&mut [i32]` and the buffer is a slice of atomics. The range is this
/// frame's alone, so the copy is as safe as upstream's in-place sort of the
/// same range.
fn sort_range(data: &Shared, beg: usize, end: usize) {
    let mut range: Vec<i32> = data[beg..end]
        .iter()
        .map(|slot| slot.load(Ordering::Relaxed))
        .collect();
    range.sort_unstable();
    for (slot, value) in data[beg..end].iter().zip(range) {
        slot.store(value, Ordering::Relaxed);
    }
}

/// Upstream's `std::iter_swap`, over two elements of the buffer.
fn swap(data: &Shared, left: usize, right: usize) {
    let held = data[left].load(Ordering::Relaxed);
    data[left].store(data[right].load(Ordering::Relaxed), Ordering::Relaxed);
    data[right].store(held, Ordering::Relaxed);
}

/// Upstream's `merge_sort_spawn`: sort the left half on a child, the right
/// half on this runtime, corun, and merge.
fn merge_sort_spawn(runtime: &mut Runtime<'_>, data: &Shared, beg: usize, end: usize) {
    if end <= beg || end - beg == 1 {
        return;
    }
    if end - beg <= 5 {
        sort_range(data, beg, end);
        return;
    }

    // Upstream's `(beg + end + 1) / 2`, which is this rounding up.
    let mid = (beg + end).div_ceil(2);

    runtime.silent_async_runtime({
        let data = Arc::clone(data);
        move |child: &mut Runtime<'_>| merge_sort_spawn(child, &data, beg, mid)
    });

    // Upstream's tail call: the right half on this runtime rather than on a
    // second child.
    merge_sort_spawn(runtime, data, mid, end);

    // The merge below reads what the child wrote, so this wait is not an
    // optimisation: it is the ordering.
    runtime.corun();

    let left: Vec<i32> = data[beg..mid]
        .iter()
        .map(|slot| slot.load(Ordering::Relaxed))
        .collect();
    let right: Vec<i32> = data[mid..end]
        .iter()
        .map(|slot| slot.load(Ordering::Relaxed))
        .collect();

    let mut merged = left.iter().peekable();
    let mut other = right.iter().peekable();
    for slot in &data[beg..end] {
        let take_left = match (merged.peek(), other.peek()) {
            (Some(left), Some(right)) => left < right,
            (Some(_), None) => true,
            (None, _) => false,
        };
        let value = if take_left {
            merged.next()
        } else {
            other.next()
        };
        slot.store(
            *value.expect("the two halves hold the whole range"),
            Ordering::Relaxed,
        );
    }
}

/// Upstream's `Runtime.MergeSort.1thread` through `.8threads`.
///
/// Recursive spawning where the parent *reads what its children wrote*, which
/// is what makes the corun load-bearing rather than tidy: a corun that
/// returned early would merge half-sorted halves, so the assertion is on the
/// sorted buffer rather than on a count.
///
/// **How a regression surfaces**: as an unsorted buffer for a corun that
/// returned too early, and as a hang for a corun that never returns. The
/// sizes are upstream's, which [`SIZES`] sets out; the recursion nests coruns
/// many levels deep at one worker, which is what makes the second a real
/// risk.
#[test]
fn a_runtime_task_merge_sorts_recursively() {
    for workers in 1..=8 {
        let executor = Executor::new(workers);

        for size in SIZES {
            let (data, sorted) = unsorted(size);

            // Upstream's `executor.async([&data, end](tf::Runtime& rt){ ...
            // }).wait()`, which is this port's `spawn_runtime`.
            let task = executor.spawn_runtime({
                let data = Arc::clone(&data);
                move |runtime: &mut Runtime<'_>| merge_sort_spawn(runtime, &data, 0, size)
            });
            task.wait().expect("nothing under the sort panicked");

            assert_eq!(
                values(&data),
                sorted,
                "{workers} worker(s): {size} elements merge-sorted"
            );
        }
    }
}

/// Upstream's `quick_sort_spawn`: partition in place, sort the left part on a
/// child, tail-recurse on the right, and **do not corun**.
///
/// The missing corun is the point of the case. Nothing here reads what a
/// child wrote -- the partition already put every element on its own side --
/// so the only thing that makes the buffer sorted when the run finishes is
/// the implicit join at the end of each body, nested as deep as the
/// recursion.
fn quick_sort_spawn(runtime: &mut Runtime<'_>, data: &Shared, beg: usize, end: usize) {
    if end <= beg || end - beg == 1 {
        return;
    }
    if end - beg <= 5 {
        sort_range(data, beg, end);
        return;
    }

    // Upstream's `pvt = beg + distance/2`, swapped out of the way and used as
    // the pivot value.
    swap(data, beg + (end - beg) / 2, end - 1);
    let pivot_value = data[end - 1].load(Ordering::Relaxed);

    // Upstream's `std::partition(beg, end-1, item < *(end-1))`.
    let mut pivot = beg;
    for index in beg..end - 1 {
        if data[index].load(Ordering::Relaxed) < pivot_value {
            swap(data, index, pivot);
            pivot += 1;
        }
    }
    swap(data, pivot, end - 1);

    runtime.silent_async_runtime({
        let data = Arc::clone(data);
        move |child: &mut Runtime<'_>| quick_sort_spawn(child, &data, beg, pivot)
    });

    quick_sort_spawn(runtime, data, pivot + 1, end);
}

/// Upstream's `Runtime.QuickSort.1thread` through `.8threads`.
///
/// The implicit join on its own, over a recursion hundreds of levels deep in
/// the worst case: no body here waits for anything, so every one of the
/// thousands of nodes must be resumed by its own last child for the run to
/// finish at all -- and the run finishing is what the assertion reads.
///
/// Upstream reuses one taskflow across its five sizes and resizes the vector
/// its body captured by reference; the body here owns a share of the buffer
/// instead, so the taskflow is rebuilt per size. Same graph, one task, one
/// run per size -- [`SIZES`] of them, which are upstream's five.
///
/// **How a regression surfaces**: as an unsorted buffer if a child's writes
/// are not waited for, and as a hang if a resumption is lost -- which with no
/// corun anywhere is the one thing holding the whole recursion together.
#[test]
fn a_runtime_task_quick_sorts_recursively() {
    for workers in 1..=8 {
        let executor = Executor::new(workers);

        for size in SIZES {
            let (data, sorted) = unsorted(size);

            let mut taskflow = Taskflow::new();
            taskflow.emplace_runtime({
                let data = Arc::clone(&data);
                move |runtime: &mut Runtime<'_>| quick_sort_spawn(runtime, &data, 0, size)
            });

            executor.run(&taskflow).wait().expect("no task panicked");

            assert_eq!(
                values(&data),
                sorted,
                "{workers} worker(s): {size} elements quick-sorted"
            );
        }
    }
}

/// This port's own: a child that panics is reported from its parent's
/// `corun`, and the parent's other children are cancelled.
///
/// The anchor path through the public API, which until #92 could only be
/// driven from inside the crate: an entered runtime node is an implicit
/// anchor, so the walk leaves a child's payload in that node's slot, and
/// `try_corun` is the frame that looks. Upstream's `Exception.RuntimeAsync`
/// is the same shape with `rt.corun()` re-raising instead.
///
/// It also pins the other half of what an anchor does, which is that
/// collecting the payload *un-marks* the node: a child spawned after the
/// corun runs, where one spawned before it and still queued would have been
/// skipped. Upstream's `_rethrow_exception` clears `EXCEPTION | CAUGHT` for
/// exactly that reason, and `NodeBase::take_panic` is that line.
///
/// **How a regression surfaces**: as `Ok(())` from the first corun, which is
/// a failure nobody will ever hear about; or as the second child never
/// running, which is a body that can never recover from a failure it has
/// already dealt with.
#[test]
fn a_failing_child_is_reported_from_its_parents_corun() {
    for workers in 1..=4 {
        let reported = Arc::new(AtomicBool::new(false));

        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime({
            let reported = Arc::clone(&reported);
            move |runtime: &mut Runtime<'_>| {
                runtime.silent_async(|| panic!("the child's own failure"));

                let error = runtime
                    .try_corun()
                    .expect_err("the child panicked under this node");
                match error {
                    RunError::Panicked(payload) => {
                        assert_eq!(
                            payload.downcast_ref::<&str>(),
                            Some(&"the child's own failure"),
                            "the payload keeps its identity"
                        );
                        reported.store(true, Ordering::Release);
                    }
                    error => panic!("unexpected failure: {error:?}"),
                }

                // Taking the payload is upstream's `_rethrow_exception`,
                // which clears `EXCEPTION | CAUGHT` as well as the slot --
                // so the node that was marked by the walk is clean again,
                // and a body that has collected its failure can go on
                // spawning. That is what "a second corun starts clean"
                // means, and it is worth pinning from outside the crate
                // because the two halves are written in different modules.
                let ran = Arc::new(AtomicBool::new(false));
                runtime.silent_async({
                    let ran = Arc::clone(&ran);
                    move || ran.store(true, Ordering::Release)
                });
                runtime.try_corun().expect("the slot was emptied");
                assert!(
                    ran.load(Ordering::Acquire),
                    "a child spawned after the failure was collected runs: \
                     collecting it un-marked the node"
                );
            }
        });

        // The run reports nothing: the payload was taken by the corun, which
        // is what an anchor *is*, and the body returned normally afterwards.
        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("the failure was collected inside the body");

        assert!(
            reported.load(Ordering::Acquire),
            "{workers} worker(s): the corun reported the child's panic"
        );
    }
}

/// This port's own, closing issue #230: `Runtime::schedule` targeting a
/// nonpreemptive runtime task does not panic even while that task's body is
/// genuinely still running, which is exactly the shape a pipeline line needs
/// and an ordinary `Handle::Runtime` line task could not safely give it --
/// that handle kind sets `PREEMPTED` before its body runs, so `schedule`
/// would see it as "already entered" for the whole time the body is up,
/// whether or not the body ever spawned anything.
///
/// `line` and `caller` are both source tasks with no edge between them, so
/// at `workers >= 2` they run concurrently. `caller` spins until it has seen
/// `line`'s body start, which puts the `schedule` call squarely inside that
/// body's run rather than before or after it, then calls it.
///
/// What `caller` does *after* the call under test is this case's own: the
/// bypass queues a second, genuine run of `line`'s body, which
/// `NonpreemptiveRuntimeWork`'s shared lock -- the same one-worker-per-node
/// assertion every work handle in the crate keeps -- refuses outright if it
/// is ever actually entered while the first is still running. A real
/// pipeline's own dependency counting never lets that happen; this case has
/// to arrange it by hand, because nothing in `Runtime::schedule` itself
/// serialises the two runs. `caller` keeps every worker occupied until
/// `line`'s first run has genuinely returned -- itself, by spinning on
/// `done` before its own body ends, and every worker beyond the two this
/// case needs, with `filler` tasks that spin on the same signal -- so the
/// second run is never even eligible to start until the first has let go of
/// the lock it was never going to contend with in a correctly-driven
/// pipeline. This is this case's own scaffolding, not something
/// `Runtime::schedule` or `Handle::NonpreemptiveRuntime` provides, and the
/// acceptance bar is narrower than it looks: a panic here would mean the
/// `is_entered` guard saw `line` as still entered, not that two genuine runs
/// can safely overlap -- they still may not, and nothing about this node
/// kind claims otherwise.
#[test]
fn schedule_targets_a_nonpreemptive_node_whose_body_is_still_running() {
    for workers in 2..=4 {
        let started = Arc::new(AtomicBool::new(false));
        let proceed = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let runs = Arc::new(AtomicI32::new(0));

        let mut taskflow = Taskflow::new();
        let line = {
            let started = Arc::clone(&started);
            let proceed = Arc::clone(&proceed);
            let done = Arc::clone(&done);
            let runs = Arc::clone(&runs);
            taskflow.emplace_nonpreemptive_runtime(move |_| {
                started.store(true, Ordering::Release);
                spin_until("the caller to schedule this task", || {
                    proceed.load(Ordering::Acquire)
                });
                runs.fetch_add(1, Ordering::Relaxed);
                // Set on every run, so the bypassed run (which finds
                // `proceed` already `true` and does not wait on it) marks
                // itself done too -- harmless, since `caller` has already
                // moved on to waiting on it by the time any run can reach
                // this line.
                done.store(true, Ordering::Release);
            })
        };
        let _caller = {
            let started = Arc::clone(&started);
            let proceed = Arc::clone(&proceed);
            let done = Arc::clone(&done);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                spin_until("the line task to start", || started.load(Ordering::Acquire));
                // The call under test: `line`'s body has started and has not
                // yet observed `proceed`, so it is still running right here.
                runtime.schedule(line);
                proceed.store(true, Ordering::Release);
                // Keep this worker busy until `line`'s first run has
                // actually returned and dropped the lock its body holds --
                // see the function docs for why the bypass's second run must
                // not be *eligible* to start before then.
                spin_until("the line task's first run to finish", || {
                    done.load(Ordering::Acquire)
                });
            })
        };
        // One per worker this case does not otherwise occupy, each keeping
        // its own worker out of the steal loop on the same signal `caller`
        // waits on -- so at every worker count in the matrix, nothing is
        // free to pick up the bypassed run until it is safe to.
        let _fillers: Vec<_> = (2..workers)
            .map(|_| {
                let done = Arc::clone(&done);
                taskflow.emplace(move || {
                    spin_until("the line task's first run to finish", || {
                        done.load(Ordering::Acquire)
                    });
                })
            })
            .collect();

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked, in particular not `schedule`'s guard");

        assert_eq!(
            runs.load(Ordering::Relaxed),
            2,
            "{workers} worker(s): the body ran once as a source task and \
             once more through the bypass"
        );
    }
}

/// Issue #245, closing the hole #230 documented but could not close from the
/// caller's side: `Runtime::schedule` on a nonpreemptive node whose body is
/// genuinely running no longer races a second worker onto the same
/// callable. It defers instead, and the claim holder's own release queues
/// the deferred run once it is actually safe to.
///
/// The shape is
/// [`schedule_targets_a_nonpreemptive_node_whose_body_is_still_running`]'s
/// own, minus every bit of scaffolding that case's docs say exists only to
/// keep the bypassed run *ineligible* to start before `line`'s first run has
/// returned -- the `_fillers` and the `done` handshake `caller` waits on
/// afterwards. None of that is needed once the scheduler keeps its own
/// invariant: with every worker beyond `caller`'s free the moment `schedule`
/// is called, this case puts the deferred run back in direct competition
/// with the one still running, which is exactly what issue #245 closes.
///
/// `caller`'s own `schedule` call lands while `line`'s body is
/// *deterministically* still inside its own lock, not merely probably so:
/// `line` cannot pass its `spin_until` until `proceed` is `true`, and
/// `caller` does not set `proceed` until after the call under test -- program
/// order on `caller`'s own single thread, not timing. The `Release` on
/// `started.store` and the `Acquire` on the load `caller`'s own `spin_until`
/// reads carry that guarantee across threads too: everything sequenced
/// before `line`'s `started.store` -- including the claim its dispatch arm
/// takes before this body ever runs -- is visible to `caller` by the time it
/// sees `started` turn `true`, so the bypass is guaranteed to find that claim
/// held.
#[test]
fn schedule_defers_a_bypass_onto_a_nonpreemptive_node_still_running() {
    for workers in 2..=4 {
        let started = Arc::new(AtomicBool::new(false));
        let proceed = Arc::new(AtomicBool::new(false));
        let runs = Arc::new(AtomicI32::new(0));

        let mut taskflow = Taskflow::new();
        let line = {
            let started = Arc::clone(&started);
            let proceed = Arc::clone(&proceed);
            let runs = Arc::clone(&runs);
            taskflow.emplace_nonpreemptive_runtime(move |_| {
                started.store(true, Ordering::Release);
                spin_until("the caller to schedule this task", || {
                    proceed.load(Ordering::Acquire)
                });
                runs.fetch_add(1, Ordering::Relaxed);
            })
        };
        let _caller = {
            let started = Arc::clone(&started);
            let proceed = Arc::clone(&proceed);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                spin_until("the line task to start", || started.load(Ordering::Acquire));
                // `line`'s dispatch claimed its callable before this body
                // ever ran, by the ordering argument above, so this call
                // lands while that claim is held: deferred rather than
                // queued is the only way this can go without tripping
                // `lock_callable`'s assertion -- every worker beyond this
                // one is free right now, with nothing keeping it from
                // stealing a job this call queued outright.
                runtime.schedule(line);
                proceed.store(true, Ordering::Release);
            })
        };

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked, in particular not `lock_callable`'s assertion");

        assert_eq!(
            runs.load(Ordering::Relaxed),
            2,
            "{workers} worker(s): the body ran once as a source task and \
             once more through the deferred bypass, with nothing lost"
        );
    }
}

/// Issue #245's other half: a target whose handle kind *does* preempt
/// itself, caught in the window before it has. A `Handle::Subflow` is not
/// `is_entered` for any of its body's own run -- `PREEMPTED` goes on only
/// afterwards, and only if that body built a sourced, joinable subgraph --
/// so `Runtime::schedule` targeting one while its body is still building a
/// subgraph had nothing at all guarding it, same as a nonpreemptive node's
/// whole run above.
///
/// `spawner`'s body builds exactly one child, so it preempts once it
/// returns: the deferred bypass this case forces can only be honoured once
/// the node is genuinely idle again, which is the *second* entry, after that
/// one child has finished and re-queued it -- not the point the body merely
/// stops running, which is the harder half of this hole and the reason
/// `src/executor/subflow.rs`'s `invoke` defers its own release to that
/// second entry rather than to the moment the body returns. `caller`'s own
/// `schedule` call is placed and ordered exactly as the nonpreemptive case
/// above, so it is just as deterministic: by the time `caller` observes
/// `started`, `spawner`'s dispatch has already claimed its callable, and
/// `caller`'s own bypass lands squarely inside that claim.
#[test]
fn schedule_defers_a_bypass_onto_a_subflow_node_still_building_its_subgraph() {
    for workers in 2..=4 {
        let started = Arc::new(AtomicBool::new(false));
        let proceed = Arc::new(AtomicBool::new(false));
        let runs = Arc::new(AtomicI32::new(0));

        let mut taskflow = Taskflow::new();
        let spawner = {
            let started = Arc::clone(&started);
            let proceed = Arc::clone(&proceed);
            let runs = Arc::clone(&runs);
            taskflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
                runs.fetch_add(1, Ordering::Relaxed);
                started.store(true, Ordering::Release);
                spin_until("the caller to schedule this task", || {
                    proceed.load(Ordering::Acquire)
                });
                // A sourced, joinable subgraph: this entry preempts the node
                // once it returns, which is what puts the deferred release
                // at the second entry under test rather than at this one.
                subflow.emplace(|| {});
            })
        };
        let _caller = {
            let started = Arc::clone(&started);
            let proceed = Arc::clone(&proceed);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                spin_until("the subflow task to start", || {
                    started.load(Ordering::Acquire)
                });
                // Called while `spawner`'s body is still inside its own
                // lock and well before it has even set `PREEMPTED` -- the
                // exact window `is_entered` cannot see, and every worker
                // beyond this one is free to race onto that callable if
                // this queued a job outright instead of deferring.
                runtime.schedule(spawner);
                proceed.store(true, Ordering::Release);
            })
        };

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked, in particular not `lock_callable`'s assertion");

        assert_eq!(
            runs.load(Ordering::Relaxed),
            2,
            "{workers} worker(s): the body ran once, preempted for its own \
             child, and ran a second time once the deferred bypass was \
             honoured after that child finished"
        );
    }
}

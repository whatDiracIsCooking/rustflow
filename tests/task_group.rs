//! Translated from `unittests/test_task_group.cpp` (Taskflow v4.1.0).
//!
//! Every group of that file is here, one `#[test]` per worker count as
//! upstream has one `TEST_CASE`, so a failure names the pool it happened on:
//! `Basics` once, `Async` and `DependentAsync` at one to eight workers,
//! `Cancellation` at two to eight -- upstream's own `REQUIRE(W>1)`, since the
//! case needs a worker to spare for the task that opens the group --
//! `Fibonacci` and `FibonacciSwapped` at one to four, and `MergeSort` and
//! `QuickSort` at one to eight.
//!
//! The last four are the ones worth reading the file for. A task group is
//! what makes recursive parallelism a plain recursive *function* -- no graph,
//! no handle to thread through, and no worker parked on a child -- so what
//! they pin is that a group opened inside a group's own child works, to
//! whatever depth the recursion goes, at any worker count including one.
//!
//! # Where this file departs from upstream, and why
//!
//! - **Shared counters are atomics.** Upstream's `Async` case shares a
//!   `std::atomic<size_t>` and its `DependentAsync` case a plain `size_t`,
//!   relying on its chain for the exclusivity. A plain counter cannot be
//!   shared across `'static` task bodies in Rust at all, so both are
//!   [`AtomicUsize`] here; the chain is still what the case is about, and the
//!   count is still what it asserts.
//! - **The two sorts take owned halves.** Upstream passes one
//!   `std::vector<int>&` down the recursion with a pair of indices or
//!   iterators, so its two halves are aliased mutable views of one buffer --
//!   disjoint, but only by argument. The port splits the data instead: each
//!   half is moved into the branch that sorts it and handed back, which is
//!   the same recursion over the same partition with the aliasing question
//!   answered by ownership rather than by a comment. The one place the halves
//!   must be *recombined* is where upstream merges in place, so the merge
//!   below is upstream's loop over two owned halves.
//! - **The handles are waited on.** Upstream drops the future its
//!   `tg.async` hands back; here every one is kept and waited on *after* the
//!   corun, which costs nothing (the task has finished by then) and says the
//!   second half of what the case is about: the bodies ran, and their results
//!   reached the caller who asked for them. `tests/asyncs.rs` asserts the
//!   same pair for the same reason.
//! - **Every handshake has a deadline.** The `Cancellation` case deliberately
//!   wedges the other `W - 1` workers in a spin, which is how it makes the
//!   hundred cancelled children provably *not yet started*. Upstream spins on
//!   `while(arrivals != 0)` for ever; here a child that is never released
//!   gives up and panics, which a group reports through
//!   [`TaskGroup::corun`] -- so a lost release fails the case instead of
//!   wedging a worker for the rest of the run. That is also why the blocked
//!   children are spawned with [`TaskGroup::silent_async`] rather than
//!   upstream's `async`: a silent child's panic reaches the group, where a
//!   detached handle's would be dropped and the case would pass in silence.
//! - **The data is drawn from the crate's own generator**, since the crate
//!   has no random dependency and `rustflow::Xorshift32` is public. A fixed
//!   seed per case, so a failure stays reproducible.
//! - **Every case that opens a group runs over both routes.** A group is
//!   opened either on the executor, which can name the opening worker only by
//!   id, or on a `Runtime`, which holds that worker's own scheduling loop --
//!   `src/task_group.rs` has the pair, and #372 added the second. The two
//!   routes need different task bodies, which is all `Route` below decides:
//!   the children, the inputs and the assertions are the same either way.
//!
//!   The four recursive cases carry the route down the recursion as well, and
//!   on the runtime route they take upstream's `FibonacciSwapped` ordering --
//!   the branch this worker computes itself runs *before* the group opens
//!   rather than across its life, which is why `fibonacci` and
//!   `fibonacci_swapped` are two recursions on the id route and one here.
//!   That was the only ordering the route had until #395 gave it
//!   `TaskGroup::with_runtime`, and these cases keep it: it is upstream's own
//!   line either way, and the tail-optimised ordering over the lent runtime
//!   has its cases beside the method in `src/task_group.rs`. The two sorts
//!   hand their halves back through a slot rather than a handle for a second
//!   consequence of the same shape: a body that is handed a `Runtime` of its
//!   own has no value-returning spawning form.
//! - **The four recursive cases run at smaller inputs than upstream's**, and
//!   that is the one deviation here which is about the port rather than about
//!   Rust. Upstream asserts `fib(25)` and sorts up to ten thousand elements;
//!   this file asserts `fib(12)` and stops both sorts at a thousand.
//!
//!   The reason is stack depth, and it is not the recursion's own. A corun
//!   keeps its worker in the work-stealing loop, so every job it picks up
//!   while waiting nests one frame deeper -- and the jobs picked up by the
//!   corun of a group opened on the *executor* are the oldest outstanding
//!   ones, because that group's children go to the executor's shared buffers
//!   (crossbeam `Injector`s, which hand work back FIFO) and because its corun
//!   waits on the stealing end of its worker's queue, which is FIFO too.
//!   Upstream pushes onto the submitting worker's own LIFO queue and pops
//!   that same end -- the `if(auto w = this_worker(); w)` branch of
//!   `_schedule_async_task` -- so its corun takes its own newest child first
//!   and nests one frame per level of *recursion*. The id route takes the
//!   largest outstanding subtree first and nests one frame per outstanding
//!   *child*, which for a recursive case is most of the task tree.
//!
//!   #119 gave upstream's branch to the submitters that could name the worker
//!   running them, which a group could not, and #372 gave it to the route
//!   that can: a group opened on a [`Runtime`](rustflow::Runtime) holds the
//!   worker rather than its id. The sizes below are still the id route's,
//!   since every case runs over both -- re-measured in a debug build against
//!   a worker's default 2 MiB stack, that route passes `fib(15)` and aborts
//!   at `fib(16)`, passes 2000 elements and aborts at 3000. They therefore
//!   sit a factor of two or so inside a cliff that belongs to one route and
//!   not to the other; the last case in this file is where the runtime route
//!   is taken past it, and raising the shared sizes is a scheduling change
//!   rather than a test one. `tests/runtimes.rs`, whose recursion goes
//!   through a `Runtime` directly, runs upstream's own `fib(25)` and ten
//!   thousand elements, and `tests/subflows.rs` computes `fib(20)` at one
//!   worker because a subflow's children are a *graph*, scheduled through the
//!   worker-aware path all along.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::prelude::*;
use rustflow::{Executor, TaskGroup, Xorshift32};

// The matrix macro, the spin both halves of the `Cancellation` handshake wait
// on, and the payload check the one case here that expects a failure makes.
// `common/counters.rs` is not among them: half the counters here are a
// recursion's return value rather than a tally of what ran, and the two sorts
// compare vectors instead of counting at all.
#[macro_use]
mod common;
#[path = "common/panics.rs"]
mod panics;
#[path = "common/spin.rs"]
mod spin;

use panics::assert_panicked_with;
use spin::spin_until;

/// Upstream's `for(size_t i=0; i<1000; ++i)`, in both of the first two
/// cases: a thousand children per group, and a thousand-link chain.
const TASKS: usize = 1000;

/// What the two recursive Fibonacci cases compute.
///
/// Upstream's is 25, and the module docs have why a group asserts a smaller
/// one: the stack a group's corun nests, which #119 left where it was.
/// `fib(15)` is the largest that passes and `fib(16)` aborts, so this is a
/// factor of two inside the cliff rather than at it.
const FIBONACCI_N: usize = 12;

/// `fibonacci(12)`, which is what both recursive cases must come back with.
const FIBONACCI_OF_N: usize = 144;

/// Upstream's `::rand()%100` over a seed of this file's own.
const SEED: u32 = 0x5EED_1793;

/// Opens a group on the calling worker, failing the case rather than
/// returning an error: every call below is already inside a task body, so the
/// refusal is a defect in the executor and not a condition to handle.
fn group_of(executor: &Executor) -> TaskGroup<'_> {
    executor
        .task_group()
        .expect("a task body runs on a worker, which may open a task group")
}

/// Which of the two routes a case opens its groups through.
///
/// Upstream has one, its group finding the submitting worker through a
/// thread-local; the port's two differ in what the group can hold, and the
/// module docs have what that decides.
#[derive(Clone, Copy, Debug)]
enum Route {
    /// `Executor::task_group`, from a body that was handed nothing: the group
    /// holds the opening worker's id and queues its children in the shared
    /// buffers.
    ById,
    /// `Runtime::task_group`, from a body that was handed the scheduler: the
    /// group holds that worker's own scheduling loop and queues its children
    /// on it.
    OnWorker,
}

/// Runs `case` on a worker of `executor`, with a group opened through
/// `route`, and hands back what the task reported.
///
/// The outcome is returned rather than unwrapped because what a failure of it
/// means is the case's to say -- for most of them a child panicked, and for
/// the cancellation case a handshake timed out. The two routes take two
/// different task bodies, which is the whole of what this hides.
fn in_a_group(
    executor: &Arc<Executor>,
    route: Route,
    case: impl FnOnce(&TaskGroup<'_>) + Send + 'static,
) -> Result<(), RunError> {
    match route {
        Route::ById => {
            let opener = Arc::clone(executor);
            executor.spawn(move || case(&group_of(&opener))).wait()
        }
        Route::OnWorker => executor
            .spawn_runtime(move |runtime: &mut Runtime<'_>| case(&runtime.task_group()))
            .wait(),
    }
}

/// Upstream's `TaskGroup.Basics`: a group can be opened from every kind of
/// task body the executor runs, and a group nothing has been spawned into is
/// empty.
///
/// What it really pins is which *thread* may open one -- every one of the
/// five bodies runs on a worker, which is the whole of
/// [`Executor::task_group`]'s precondition -- and the counter is what says
/// all five ran rather than that none of them panicked.
#[test]
fn basics() {
    let executor = Arc::new(Executor::new(4));
    let opened = Arc::new(AtomicUsize::new(0));

    // A body that opens a group, checks it is empty and says it ran.
    let open = {
        let executor = Arc::clone(&executor);
        let opened = Arc::clone(&opened);
        move || {
            let group = group_of(&executor);
            assert_eq!(group.size(), 0);
            opened.fetch_add(1, Ordering::Relaxed);
        }
    };

    // From a task of a taskflow.
    let mut taskflow = Taskflow::new();
    taskflow.emplace(open.clone());
    executor.run(&taskflow).wait().expect("no task panicked");

    // From the four kinds of free-standing task.
    executor.spawn(open.clone()).wait().expect("no panic");
    executor.silent_async(open.clone());
    let (_task, handle) = executor.dependent_async(open.clone(), []);
    handle.wait().expect("no panic");
    executor.silent_dependent_async(open, []);

    // The two silent ones have no handle, so this is what waits for them.
    executor.wait_for_all();
    assert_eq!(opened.load(Ordering::Relaxed), 5);
}

/// `basics` over the other route: the five kinds of task body that are handed
/// a [`Runtime`](rustflow::Runtime) can each open a group on the worker
/// running them, and that group starts empty too.
///
/// Upstream has no such case because it has no such route; what this pins is
/// the half of the precondition the route removes. `Runtime::task_group`
/// cannot refuse, so a body that gets one is proof on its own, and the
/// counter is again what says all five ran.
#[test]
fn basics_on_worker() {
    let executor = Arc::new(Executor::new(4));
    let opened = Arc::new(AtomicUsize::new(0));

    // A runtime-shaped body that opens a group, checks it is empty and says
    // it ran.
    let open = {
        let opened = Arc::clone(&opened);
        move |runtime: &mut Runtime<'_>| {
            let group = runtime.task_group();
            assert_eq!(group.size(), 0);
            opened.fetch_add(1, Ordering::Relaxed);
        }
    };

    // From a runtime task of a taskflow.
    let mut taskflow = Taskflow::new();
    taskflow.emplace_runtime(open.clone());
    executor.run(&taskflow).wait().expect("no task panicked");

    // From the four kinds of free-standing runtime-shaped task.
    executor
        .spawn_runtime(open.clone())
        .wait()
        .expect("no panic");
    executor.silent_async_runtime(open.clone());
    let (_task, handle) = executor.dependent_async_runtime(open.clone(), []);
    handle.wait().expect("no panic");
    executor.silent_dependent_async_runtime(open, []);

    // The two silent ones have no handle, so this is what waits for them.
    executor.wait_for_all();
    assert_eq!(opened.load(Ordering::Relaxed), 5);
}

/// Upstream's `TaskGroup.Async.<W>threads`: a thousand of each of the two
/// plain spawning forms into one group, then one corun.
///
/// The counter is what says every body ran and the handles are what say every
/// `spawn`ed result arrived -- and the corun is what makes the second claim
/// free, since a handle waited on after it has already been delivered. At one
/// worker it is also a statement about the corun loop itself: the worker that
/// spawned the two thousand children is the only one that can run them.
fn async_tasks(workers: usize, route: Route) {
    let executor = Arc::new(Executor::new(workers));
    let counter = Arc::new(AtomicUsize::new(0));

    in_a_group(&executor, route, {
        let counter = Arc::clone(&counter);
        move |group| {
            let mut handles = Vec::with_capacity(TASKS);

            for _ in 0..TASKS {
                handles.push(group.spawn({
                    let counter = Arc::clone(&counter);
                    move || counter.fetch_add(1, Ordering::Relaxed)
                }));
                group.silent_async({
                    let counter = Arc::clone(&counter);
                    move || {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }

            group.corun();
            assert_eq!(counter.load(Ordering::Relaxed), 2 * TASKS);
            assert_eq!(group.size(), 0);

            for handle in handles {
                handle.wait().expect("no child panicked");
            }
        }
    })
    .expect("no task panicked");

    assert_eq!(counter.load(Ordering::Relaxed), 2 * TASKS);
}

worker_matrix! {
    async_tasks {
        async_tasks_1thread(1, Route::ById),
        async_tasks_2threads(2, Route::ById),
        async_tasks_3threads(3, Route::ById),
        async_tasks_4threads(4, Route::ById),
        async_tasks_5threads(5, Route::ById),
        async_tasks_6threads(6, Route::ById),
        async_tasks_7threads(7, Route::ById),
        async_tasks_8threads(8, Route::ById),
        async_tasks_on_worker_1thread(1, Route::OnWorker),
        async_tasks_on_worker_2threads(2, Route::OnWorker),
        async_tasks_on_worker_3threads(3, Route::OnWorker),
        async_tasks_on_worker_4threads(4, Route::OnWorker),
        async_tasks_on_worker_5threads(5, Route::OnWorker),
        async_tasks_on_worker_6threads(6, Route::OnWorker),
        async_tasks_on_worker_7threads(7, Route::OnWorker),
        async_tasks_on_worker_8threads(8, Route::OnWorker),
    }
}

/// Upstream's `TaskGroup.DependentAsync.<W>threads`: a thousand-link chain
/// built one link at a time while the links already submitted are running,
/// once with each of the two dependent forms.
///
/// The group is what makes the chain waitable at all: only the last link's
/// handle knows when the chain is over, and a `silent_dependent_async` chain
/// has no handle anywhere. One corun covers every link of it.
fn dependent_async_chains(workers: usize, route: Route) {
    let executor = Arc::new(Executor::new(workers));
    let counter = Arc::new(AtomicUsize::new(0));

    in_a_group(&executor, route, {
        let counter = Arc::clone(&counter);
        move |group| {
            // Upstream's first block: the silent form, where the chain is
            // the only thing that knows the order.
            let bump = |counter: &Arc<AtomicUsize>| {
                let counter = Arc::clone(counter);
                move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            };

            let mut previous = group.silent_dependent_async(bump(&counter), []);
            for _ in 1..TASKS {
                previous = group.silent_dependent_async(bump(&counter), [&previous]);
            }
            group.corun();
            assert_eq!(counter.load(Ordering::Relaxed), TASKS);

            // Upstream's second block: the handle-returning form, whose
            // first link is the one that resets the counter and which
            // depends on nothing, as upstream's does.
            let (reset, reset_handle) = {
                let counter = Arc::clone(&counter);
                group.dependent_async(move || counter.store(0, Ordering::Relaxed), [])
            };

            let mut handles = vec![reset_handle];
            let mut previous = reset;
            for _ in 0..TASKS {
                let counter = Arc::clone(&counter);
                let (task, handle) = group.dependent_async(
                    move || {
                        counter.fetch_add(1, Ordering::Relaxed);
                    },
                    [&previous],
                );
                previous = task;
                handles.push(handle);
            }
            group.corun();
            assert_eq!(counter.load(Ordering::Relaxed), TASKS);

            for handle in handles {
                handle.wait().expect("no child panicked");
            }
        }
    })
    .expect("no task panicked");

    assert_eq!(counter.load(Ordering::Relaxed), TASKS);
}

worker_matrix! {
    dependent_async_chains {
        dependent_async_chains_1thread(1, Route::ById),
        dependent_async_chains_2threads(2, Route::ById),
        dependent_async_chains_3threads(3, Route::ById),
        dependent_async_chains_4threads(4, Route::ById),
        dependent_async_chains_5threads(5, Route::ById),
        dependent_async_chains_6threads(6, Route::ById),
        dependent_async_chains_7threads(7, Route::ById),
        dependent_async_chains_8threads(8, Route::ById),
        dependent_async_chains_on_worker_1thread(1, Route::OnWorker),
        dependent_async_chains_on_worker_2threads(2, Route::OnWorker),
        dependent_async_chains_on_worker_3threads(3, Route::OnWorker),
        dependent_async_chains_on_worker_4threads(4, Route::OnWorker),
        dependent_async_chains_on_worker_5threads(5, Route::OnWorker),
        dependent_async_chains_on_worker_6threads(6, Route::OnWorker),
        dependent_async_chains_on_worker_7threads(7, Route::OnWorker),
        dependent_async_chains_on_worker_8threads(8, Route::OnWorker),
    }
}

/// Upstream's `TaskGroup.Cancellation.<W>threads`: the other `W - 1` workers
/// are deliberately blocked, a hundred children are spawned behind them, the
/// group is cancelled, the blocked children are released, and none of the
/// hundred ever ran.
///
/// The blocking is not incidental -- it is what makes "had not started yet"
/// true of all hundred rather than likely -- and it is also the one hazard in
/// this file, so both halves of it are bounded: a blocked child gives up
/// after [`spin_until`]'s deadline and panics, and the arrivals handshake does the
/// same, so a release that never comes fails the case instead of wedging the
/// pool. A regression in the skip path shows up two ways and neither hangs
/// here: a child that ran anyway lands on the counter, and a skipped child
/// whose job was not torn down leaves the anchor's counter above zero, which
/// the `size` assertion reads before the final corun returns.
fn cancellation(workers: usize, route: Route) {
    assert!(
        workers > 1,
        "the case needs a worker besides the one blocked"
    );

    let executor = Arc::new(Executor::new(workers));
    let ran = Arc::new(AtomicUsize::new(0));

    in_a_group(&executor, route, {
        let ran = Arc::clone(&ran);
        move |group| {
            let arrivals = Arc::new(AtomicUsize::new(0));
            let release = Arc::new(AtomicBool::new(false));

            // Upstream's `tg.async([&](){ ++arrivals; while(arrivals != 0); })`,
            // once per worker this body is not running on. Silent, so
            // that a child which gives up waiting reports to the group.
            for _ in 0..workers - 1 {
                let arrivals = Arc::clone(&arrivals);
                let release = Arc::clone(&release);
                group.silent_async(move || {
                    arrivals.fetch_add(1, Ordering::Release);
                    spin_until("the task group to release the blocked child", || {
                        release.load(Ordering::Acquire)
                    });
                });
            }

            // Upstream's `while(arrivals != W-1)`: every other worker is
            // inside a child of this group and will stay there.
            spin_until("the other workers to block", || {
                arrivals.load(Ordering::Acquire) == workers - 1
            });

            // The hundred that must never run, queued behind workers that
            // cannot take them.
            for _ in 0..100 {
                let ran = Arc::clone(&ran);
                group.silent_async(move || {
                    ran.fetch_add(1, Ordering::Relaxed);
                });
            }

            assert!(!group.is_cancelled());
            group.cancel();
            assert!(group.is_cancelled());

            release.store(true, Ordering::Release);

            // Every child is still accounted for -- the hundred skipped
            // ones included -- so this returns only once the counter is
            // back to zero.
            group.corun();
            assert_eq!(group.size(), 0);
            assert_eq!(ran.load(Ordering::Relaxed), 0);
        }
    })
    .expect("no task panicked, so every blocked child was released in time");

    assert_eq!(ran.load(Ordering::Relaxed), 0);
}

worker_matrix! {
    cancellation {
        cancellation_2threads(2, Route::ById),
        cancellation_3threads(3, Route::ById),
        cancellation_4threads(4, Route::ById),
        cancellation_5threads(5, Route::ById),
        cancellation_6threads(6, Route::ById),
        cancellation_7threads(7, Route::ById),
        cancellation_8threads(8, Route::ById),
        cancellation_on_worker_2threads(2, Route::OnWorker),
        cancellation_on_worker_3threads(3, Route::OnWorker),
        cancellation_on_worker_4threads(4, Route::OnWorker),
        cancellation_on_worker_5threads(5, Route::OnWorker),
        cancellation_on_worker_6threads(6, Route::OnWorker),
        cancellation_on_worker_7threads(7, Route::OnWorker),
        cancellation_on_worker_8threads(8, Route::OnWorker),
    }
}

/// Upstream's `fibonacci(N, executor)`: one branch spawned into a group of
/// this call's own, the other computed on this very worker, and a corun to
/// join them.
///
/// The group is opened *before* the inline branch and corun after it, which
/// is upstream's "tail optimization": the worker does useful work instead of
/// waiting, and the only thing it ever waits for is the one child it spawned.
/// Relaxed on both ends of `res1` is upstream's plain `size_t` write and
/// read, and what makes it sound is the corun rather than the ordering: the
/// child's tear-down releases the anchor's counter that
/// [`TaskGroup::corun`] acquires.
fn fibonacci(n: usize, executor: &Arc<Executor>) -> usize {
    if n < 2 {
        return n;
    }

    let group = group_of(executor);
    let res1 = Arc::new(AtomicUsize::new(0));

    group.silent_async({
        let executor = Arc::clone(executor);
        let res1 = Arc::clone(&res1);
        move || res1.store(fibonacci(n - 1, &executor), Ordering::Relaxed)
    });

    let res2 = fibonacci(n - 2, executor);

    group.corun();

    res1.load(Ordering::Relaxed) + res2
}

/// Upstream's `fibonacci_swapped`: the same recursion with the inline branch
/// computed *before* the child is spawned.
///
/// The group still exists across that inline branch, which opens groups of
/// its own -- so what this orders differently is when the group acquires its
/// first child, and therefore what the panic walk and the join counter see
/// while the recursion is at its deepest.
fn fibonacci_swapped(n: usize, executor: &Arc<Executor>) -> usize {
    if n < 2 {
        return n;
    }

    let group = group_of(executor);
    let res1 = Arc::new(AtomicUsize::new(0));

    let res2 = fibonacci_swapped(n - 2, executor);

    group.silent_async({
        let executor = Arc::clone(executor);
        let res1 = Arc::clone(&res1);
        move || res1.store(fibonacci_swapped(n - 1, &executor), Ordering::Relaxed)
    });

    group.corun();

    res1.load(Ordering::Relaxed) + res2
}

/// The same recursion through the other route: the group is opened on the
/// [`Runtime`](rustflow::Runtime) running this level, so its child goes onto
/// this worker's own queue and the corun pops it straight back.
///
/// Upstream's `fibonacci_swapped` ordering, and on this route it is the only
/// one available: the group borrows the runtime for as long as it is open, so
/// the branch computed on this worker is the one that runs *before* it. The
/// child is a runtime-shaped body because it opens a group of its own, which
/// is also why its answer comes back through `res1` rather than a handle.
fn fibonacci_on_worker(n: usize, runtime: &mut Runtime<'_>) -> usize {
    if n < 2 {
        return n;
    }

    let res2 = fibonacci_on_worker(n - 2, runtime);
    let res1 = Arc::new(AtomicUsize::new(0));

    let group = runtime.task_group();
    group.silent_async_runtime({
        let res1 = Arc::clone(&res1);
        move |child: &mut Runtime<'_>| {
            res1.store(fibonacci_on_worker(n - 1, child), Ordering::Relaxed);
        }
    });

    group.corun();

    res1.load(Ordering::Relaxed) + res2
}

/// Upstream's `fibonacci(T, N)`: the recursion has to start on a worker, so
/// the whole of it runs inside one async task.
///
/// `recurse` is one of the two shapes above, so that the eight cases below
/// are eight lines rather than two copies of four.
fn fibonacci_at(workers: usize, recurse: fn(usize, &Arc<Executor>) -> usize) {
    let executor = Arc::new(Executor::new(workers));
    let fib = executor
        .spawn({
            let executor = Arc::clone(&executor);
            move || recurse(FIBONACCI_N, &executor)
        })
        .wait()
        .expect("no task panicked");

    assert_eq!(fib, FIBONACCI_OF_N);
}

/// `fibonacci_at` for the runtime route: the same `n` and the same answer,
/// started in the one task body that hands the recursion a runtime.
///
/// The answer comes back through a slot for the reason
/// [`fibonacci_on_worker`] gives, which is the one shape difference between
/// this driver and the one above.
fn fibonacci_on_worker_at(workers: usize) {
    let executor = Executor::new(workers);
    let fib = Arc::new(AtomicUsize::new(0));

    executor
        .spawn_runtime({
            let fib = Arc::clone(&fib);
            move |runtime: &mut Runtime<'_>| {
                fib.store(fibonacci_on_worker(FIBONACCI_N, runtime), Ordering::Relaxed);
            }
        })
        .wait()
        .expect("no task panicked");

    assert_eq!(fib.load(Ordering::Relaxed), FIBONACCI_OF_N);
}

worker_matrix! {
    fibonacci_at {
        fibonacci_1thread(1, fibonacci),
        fibonacci_2threads(2, fibonacci),
        fibonacci_3threads(3, fibonacci),
        fibonacci_4threads(4, fibonacci),
        fibonacci_swapped_1thread(1, fibonacci_swapped),
        fibonacci_swapped_2threads(2, fibonacci_swapped),
        fibonacci_swapped_3threads(3, fibonacci_swapped),
        fibonacci_swapped_4threads(4, fibonacci_swapped),
    }
}

worker_matrix! {
    fibonacci_on_worker_at {
        fibonacci_on_worker_1thread(1),
        fibonacci_on_worker_2threads(2),
        fibonacci_on_worker_3threads(3),
        fibonacci_on_worker_4threads(4),
    }
}

/// Upstream's `end - beg <= 5` cut-off, under which both sorts stop
/// recursing and sort what is left where it lies.
const SORT_DIRECTLY: usize = 5;

/// Upstream's `merge_sort_spawn`: half the data sorted in a child of a group
/// opened here, half sorted on this worker, a corun to join them and then the
/// merge.
///
/// The two halves are owned rather than aliased, for the reason the module
/// docs give, which also decides the shape of the merge: upstream copies each
/// half out into a `tmpl` / `tmpr` before writing back over them, and here
/// the halves *are* those copies.
fn merge_sort(executor: &Arc<Executor>, mut data: Vec<u32>) -> Vec<u32> {
    if data.len() <= SORT_DIRECTLY {
        data.sort_unstable();
        return data;
    }

    // Upstream's `m = (beg + end + 1) / 2`, which over a length is the
    // halfway point rounded up.
    let middle = data.len().div_ceil(2);
    let right = data.split_off(middle);
    let left = data;

    let group = group_of(executor);
    let sorted_left = group.spawn({
        let executor = Arc::clone(executor);
        move || merge_sort(&executor, left)
    });

    let sorted_right = merge_sort(executor, right);

    group.corun();

    // The child has finished, so this does not block: what it does is take
    // the half it sorted, which is the value upstream wrote back in place.
    let sorted_left = sorted_left.wait().expect("no child panicked");

    merge(sorted_left, sorted_right)
}

/// `merge_sort` through the other route, with the two differences the module
/// docs set out: the half this worker sorts itself is sorted before the group
/// opens, and the child's half comes back through a slot because a
/// runtime-shaped body returns nothing.
fn merge_sort_on_worker(runtime: &mut Runtime<'_>, mut data: Vec<u32>) -> Vec<u32> {
    if data.len() <= SORT_DIRECTLY {
        data.sort_unstable();
        return data;
    }

    let middle = data.len().div_ceil(2);
    let right = data.split_off(middle);
    let left = data;

    let sorted_right = merge_sort_on_worker(runtime, right);
    let sorted_left = Arc::new(Mutex::new(Vec::new()));

    let group = runtime.task_group();
    group.silent_async_runtime({
        let sorted_left = Arc::clone(&sorted_left);
        move |child: &mut Runtime<'_>| {
            let sorted = merge_sort_on_worker(child, left);
            *sorted_left.lock().expect("the slot is not poisoned") = sorted;
        }
    });

    group.corun();

    // The child has finished, so this takes the half it sorted rather than
    // waiting for it.
    let sorted_left = std::mem::take(&mut *sorted_left.lock().expect("the slot is not poisoned"));

    merge(sorted_left, sorted_right)
}

/// Upstream's merge loop, over two owned halves: the smaller head of either
/// side until one runs out, then whatever is left of the other.
fn merge(left: Vec<u32>, right: Vec<u32>) -> Vec<u32> {
    let mut merged = Vec::with_capacity(left.len() + right.len());
    let mut left = left.into_iter().peekable();
    let mut right = right.into_iter().peekable();

    loop {
        let take_left = match (left.peek(), right.peek()) {
            (Some(l), Some(r)) => l < r,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };

        let next = if take_left { left.next() } else { right.next() };
        merged.push(next.expect("the side that was peeked has an element"));
    }

    merged
}

/// Upstream's `quick_sort_spawn`: partition in place, one side sorted in a
/// child of a group opened here, the other on this worker, and a corun to
/// join them.
///
/// The pivot is excluded from both sides, as upstream's `[beg, pvt)` and
/// `[pvt + 1, end)` are, so the recursion strictly shrinks and the
/// concatenation needs no merge at all.
fn quick_sort(executor: &Arc<Executor>, mut data: Vec<u32>) -> Vec<u32> {
    if data.len() <= SORT_DIRECTLY {
        data.sort_unstable();
        return data;
    }

    let at = partition(&mut data);
    let right = data.split_off(at + 1);
    let pivot = data.pop().expect("the partition left the pivot in place");
    let left = data;

    let group = group_of(executor);
    let sorted_left = group.spawn({
        let executor = Arc::clone(executor);
        move || quick_sort(&executor, left)
    });

    let mut sorted = quick_sort(executor, right);

    group.corun();

    let mut result = sorted_left.wait().expect("no child panicked");
    result.push(pivot);
    result.append(&mut sorted);
    result
}

/// `quick_sort` through the other route, with [`merge_sort_on_worker`]'s two
/// differences: the side this worker sorts itself goes first, and the child's
/// side comes back through a slot.
fn quick_sort_on_worker(runtime: &mut Runtime<'_>, mut data: Vec<u32>) -> Vec<u32> {
    if data.len() <= SORT_DIRECTLY {
        data.sort_unstable();
        return data;
    }

    let at = partition(&mut data);
    let right = data.split_off(at + 1);
    let pivot = data.pop().expect("the partition left the pivot in place");
    let left = data;

    let mut sorted = quick_sort_on_worker(runtime, right);
    let sorted_left = Arc::new(Mutex::new(Vec::new()));

    let group = runtime.task_group();
    group.silent_async_runtime({
        let sorted_left = Arc::clone(&sorted_left);
        move |child: &mut Runtime<'_>| {
            let halved = quick_sort_on_worker(child, left);
            *sorted_left.lock().expect("the slot is not poisoned") = halved;
        }
    });

    group.corun();

    let mut result = std::mem::take(&mut *sorted_left.lock().expect("the slot is not poisoned"));
    result.push(pivot);
    result.append(&mut sorted);
    result
}

/// Upstream's three lines around `std::partition`: the middle element is
/// swapped to the end to serve as the pivot, everything smaller than it is
/// moved to the front, and the pivot is swapped back to the boundary.
///
/// Returns the pivot's index, which is upstream's returned `pvt`.
fn partition(data: &mut [u32]) -> usize {
    let last = data.len() - 1;
    data.swap(last / 2, last);

    let pivot = data[last];
    let mut boundary = 0;
    for index in 0..last {
        if data[index] < pivot {
            data.swap(index, boundary);
            boundary += 1;
        }
    }

    data.swap(boundary, last);
    boundary
}

/// `end` values from upstream's two sort cases, by factors of ten as
/// upstream's loops go -- stopping one step short of its ten thousand, for
/// the reason the module docs give: 2000 elements are the most that pass
/// through a group, and these sizes stay inside that.
const MERGE_SORT_SIZES: [usize; 3] = [10, 100, 1000];
const QUICK_SORT_SIZES: [usize; 4] = [1, 10, 100, 1000];

/// `count` values in upstream's `::rand() % 100` range, drawn from a fixed
/// seed so that a failure stays reproducible.
fn data_of(count: usize, rng: &mut Xorshift32) -> Vec<u32> {
    (0..count).map(|_| rng.next_u32() % 100).collect()
}

/// Runs one of a pair of sorting recursions on a worker and hands back what
/// it sorted, through whichever route the case is running.
///
/// The slot on the runtime side is the shape difference the module docs give,
/// and it is here rather than in each driver so that the two sorts below stay
/// one function each.
fn sorted_on_a_worker(
    executor: &Arc<Executor>,
    route: Route,
    data: Vec<u32>,
    by_id: fn(&Arc<Executor>, Vec<u32>) -> Vec<u32>,
    on_worker: fn(&mut Runtime<'_>, Vec<u32>) -> Vec<u32>,
) -> Vec<u32> {
    match route {
        Route::ById => {
            let opener = Arc::clone(executor);
            executor
                .spawn(move || by_id(&opener, data))
                .wait()
                .expect("no task panicked")
        }
        Route::OnWorker => {
            let slot = Arc::new(Mutex::new(Vec::new()));
            executor
                .spawn_runtime({
                    let slot = Arc::clone(&slot);
                    move |runtime: &mut Runtime<'_>| {
                        let sorted = on_worker(runtime, data);
                        *slot.lock().expect("the slot is not poisoned") = sorted;
                    }
                })
                .wait()
                .expect("no task panicked");

            std::mem::take(&mut *slot.lock().expect("the slot is not poisoned"))
        }
    }
}

/// Upstream's `merge_sort(unsigned W)`: four sizes, each sorted by the
/// recursion above from inside one async task and compared against
/// `std::sort`'s answer.
fn merge_sort_at(workers: usize, route: Route) {
    let executor = Arc::new(Executor::new(workers));
    let mut rng = Xorshift32::new(SEED);

    for size in MERGE_SORT_SIZES {
        let data = data_of(size, &mut rng);
        let mut gold = data.clone();
        gold.sort_unstable();

        let sorted = sorted_on_a_worker(&executor, route, data, merge_sort, merge_sort_on_worker);

        assert_eq!(sorted, gold, "{size} elements at {workers} workers");
    }
}

worker_matrix! {
    merge_sort_at {
        merge_sort_1thread(1, Route::ById),
        merge_sort_2threads(2, Route::ById),
        merge_sort_3threads(3, Route::ById),
        merge_sort_4threads(4, Route::ById),
        merge_sort_5threads(5, Route::ById),
        merge_sort_6threads(6, Route::ById),
        merge_sort_7threads(7, Route::ById),
        merge_sort_8threads(8, Route::ById),
        merge_sort_on_worker_1thread(1, Route::OnWorker),
        merge_sort_on_worker_2threads(2, Route::OnWorker),
        merge_sort_on_worker_3threads(3, Route::OnWorker),
        merge_sort_on_worker_4threads(4, Route::OnWorker),
        merge_sort_on_worker_5threads(5, Route::OnWorker),
        merge_sort_on_worker_6threads(6, Route::OnWorker),
        merge_sort_on_worker_7threads(7, Route::OnWorker),
        merge_sort_on_worker_8threads(8, Route::OnWorker),
    }
}

/// Upstream's `quick_sort(unsigned W)`: five sizes through **one taskflow**,
/// re-run per size, where the merge sort's case goes through an async task.
///
/// Keeping upstream's difference is worth the `Mutex`: a group opened from a
/// task of a running graph has a job of that run above it rather than a free
/// node, so this is the one case here that pins a group whose anchor's chain
/// sits under a real topology. The lock stands in for the
/// `std::vector<int>&` upstream's task body captures by reference -- it is
/// taken twice per run, outside the recursion.
fn quick_sort_at(workers: usize, route: Route) {
    let executor = Arc::new(Executor::new(workers));
    let mut rng = Xorshift32::new(SEED);
    let slot: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));

    let mut taskflow = Taskflow::new();
    match route {
        Route::ById => {
            taskflow.emplace({
                let executor = Arc::clone(&executor);
                let slot = Arc::clone(&slot);
                move || {
                    let data = std::mem::take(&mut *slot.lock().expect("the slot is not poisoned"));
                    let sorted = quick_sort(&executor, data);
                    *slot.lock().expect("the slot is not poisoned") = sorted;
                }
            });
        }
        // The same task of the same taskflow, as the runtime-shaped kind:
        // this is the one case here whose group is opened from a task of a
        // running graph on either route.
        Route::OnWorker => {
            taskflow.emplace_runtime({
                let slot = Arc::clone(&slot);
                move |runtime: &mut Runtime<'_>| {
                    let data = std::mem::take(&mut *slot.lock().expect("the slot is not poisoned"));
                    let sorted = quick_sort_on_worker(runtime, data);
                    *slot.lock().expect("the slot is not poisoned") = sorted;
                }
            });
        }
    }

    for size in QUICK_SORT_SIZES {
        let data = data_of(size, &mut rng);
        let mut gold = data.clone();
        gold.sort_unstable();

        *slot.lock().expect("the slot is not poisoned") = data;
        executor.run(&taskflow).wait().expect("no task panicked");

        let sorted = slot.lock().expect("the slot is not poisoned");
        assert_eq!(*sorted, gold, "{size} elements at {workers} workers");
    }
}

worker_matrix! {
    quick_sort_at {
        quick_sort_1thread(1, Route::ById),
        quick_sort_2threads(2, Route::ById),
        quick_sort_3threads(3, Route::ById),
        quick_sort_4threads(4, Route::ById),
        quick_sort_5threads(5, Route::ById),
        quick_sort_6threads(6, Route::ById),
        quick_sort_7threads(7, Route::ById),
        quick_sort_8threads(8, Route::ById),
        quick_sort_on_worker_1thread(1, Route::OnWorker),
        quick_sort_on_worker_2threads(2, Route::OnWorker),
        quick_sort_on_worker_3threads(3, Route::OnWorker),
        quick_sort_on_worker_4threads(4, Route::OnWorker),
        quick_sort_on_worker_5threads(5, Route::OnWorker),
        quick_sort_on_worker_6threads(6, Route::OnWorker),
        quick_sort_on_worker_7threads(7, Route::OnWorker),
        quick_sort_on_worker_8threads(8, Route::OnWorker),
    }
}

/// Upstream's `TaskGroup::silent_async` over a `[](tf::Runtime&){}` body:
/// the runtime-shaped form with no handle of any kind, which is why a corun
/// is the only thing that can report for it.
///
/// Both claims are the method's own docs'. The body is handed a working
/// [`Runtime`](rustflow::Runtime) -- it reads the pool it runs in and spawns
/// a child of its own, which [`TaskGroup::corun`] waits for along with the
/// body -- and a failure under it lands on the group's *anchor* rather than
/// on a handle it does not have, so [`TaskGroup::try_corun`] is what hands
/// the payload back. Upstream's own suite drives neither: its `TaskGroup`
/// cases are the plain and dependent forms, and it has no failing body
/// anywhere.
#[test]
fn a_silent_runtime_shaped_child_is_joined_and_reported_by_its_group() {
    const WORKERS: usize = 2;

    let executor = Arc::new(Executor::new(WORKERS));
    let ran = Arc::new(AtomicUsize::new(0));

    executor
        .spawn({
            let executor = Arc::clone(&executor);
            let ran = Arc::clone(&ran);
            move || {
                let group = group_of(&executor);

                group.silent_async_runtime({
                    let ran = Arc::clone(&ran);
                    move |spawner: &mut Runtime<'_>| {
                        assert_eq!(spawner.num_workers(), WORKERS);
                        ran.fetch_add(1, Ordering::Relaxed);
                        let ran = Arc::clone(&ran);
                        spawner.silent_async(move || {
                            ran.fetch_add(1, Ordering::Relaxed);
                        });
                    }
                });

                group.corun();
                assert_eq!(
                    ran.load(Ordering::Relaxed),
                    2,
                    "the body and the child it spawned were both waited for"
                );
                assert_eq!(group.size(), 0);

                // Nothing holds a handle on this one, so the anchor is where
                // its failure lands and the corun is what reports it.
                group.silent_async_runtime(|_: &mut Runtime<'_>| panic!("under the group"));
                let error = group.try_corun().expect_err("the child failed");
                assert_panicked_with(error, "under the group");
            }
        })
        .wait()
        .expect("the group's own failure was taken by `try_corun`");

    assert_eq!(ran.load(Ordering::Relaxed), 2);
}

/// A recursion of groups through the runtime route reaches a depth the id
/// route cannot (#372).
///
/// Both halves of the bar are `BENCHMARKS.md`'s, in the profile the test
/// suite runs under: at one worker the id route passes `fib(15)` -- 986
/// spawned children -- and **aborts the process** at `fib(16)`, because a
/// corun with no owning end of its worker's queue is handed the oldest
/// outstanding job in the pool and so nests one stack frame per outstanding
/// *task*. The runtime route pops the child it queued itself and nests one
/// frame per level of recursion instead, so the `n` here is upstream's own
/// `fib(25)`: 121392 spawned children, a hundred and twenty times the id
/// route's largest, under twenty-five nested frames.
///
/// One worker, which is the binding end: more of them spread the outstanding
/// set over more stacks and let the id route reach further. Completion is the
/// whole assertion, since what a regression does here is abort rather than
/// return a wrong answer -- queueing these children where the id route does
/// takes the process with it.
#[test]
fn a_recursion_of_groups_on_a_worker_outruns_the_id_routes_stack() {
    /// Upstream's own `fibonacci(25)`, ten levels past the largest this
    /// file's other route completes at one worker.
    const DEEP_N: usize = 25;

    /// `fibonacci(25)`.
    const DEEP_FIBONACCI: usize = 75025;

    let executor = Executor::new(1);
    let fib = Arc::new(AtomicUsize::new(0));

    executor
        .spawn_runtime({
            let fib = Arc::clone(&fib);
            move |runtime: &mut Runtime<'_>| {
                fib.store(fibonacci_on_worker(DEEP_N, runtime), Ordering::Relaxed);
            }
        })
        .wait()
        .expect("no task panicked");

    assert_eq!(fib.load(Ordering::Relaxed), DEEP_FIBONACCI);
}

/// A group's hand-written `Debug` reports what it is waiting for, and never
/// the executor it borrows.
///
/// That is the impl's own reason for not being a derive, and the three fields
/// are the whole of it: the worker the group was opened on, how many children
/// are outstanding and whether it has been cancelled. A derive would print
/// the anchor and the `Route` instead, and the route *is* the borrow -- the
/// whole executor on one side of it and the lent scheduling loop on the other
/// -- so each reading below is asserted as an exact string: a return to a
/// derive, or a fourth field, fails the case rather than widening a line.
///
/// The worker is read back through [`Executor::this_worker_id`] rather than
/// assumed, which is why the case runs at four workers instead of one and
/// over both routes: that field is the one the two routes compute
/// differently, the id route holding the number and the runtime route asking
/// the worker it borrows for it, and a pool with one worker in it could not
/// tell a wrong answer from the right one.
///
/// Both readings with a child in them are deterministic without a sleep. The
/// middle one is taken while the child spins on a release only this body can
/// set, so the child is outstanding until the body says otherwise; the last
/// is taken after the corun, where the count is zero whether the cancellation
/// skipped the child or it ran to the end, since a skipped child's tear-down
/// counts the same anchor down. The middle reading is *compared* after the
/// release rather than where it is taken, so that a regression in it fails
/// the case at once instead of leaving the child spinning into
/// [`spin_until`]'s deadline.
#[test]
fn the_debug_output_reports_the_group_and_not_the_executor_it_borrows() {
    const WORKERS: usize = 4;

    for route in [Route::ById, Route::OnWorker] {
        let executor = Arc::new(Executor::new(WORKERS));

        in_a_group(&executor, route, {
            let opener = Arc::clone(&executor);
            move |group| {
                let worker = opener
                    .this_worker_id()
                    .expect("a task group is opened on a worker");

                assert_eq!(
                    format!("{group:?}"),
                    format!("TaskGroup {{ worker: {worker}, size: 0, cancelled: false }}"),
                    "a group nothing has been spawned into names its worker and waits for nothing"
                );

                let release = Arc::new(AtomicBool::new(false));
                group.silent_async({
                    let release = Arc::clone(&release);
                    move || {
                        spin_until("the debug case to release the group's one child", || {
                            release.load(Ordering::Acquire)
                        });
                    }
                });

                let outstanding = format!("{group:?}");

                group.cancel();
                release.store(true, Ordering::Release);
                group.corun();

                assert_eq!(
                    outstanding,
                    format!("TaskGroup {{ worker: {worker}, size: 1, cancelled: false }}"),
                    "a child that cannot finish yet is what the group is waiting for"
                );
                assert_eq!(
                    format!("{group:?}"),
                    format!("TaskGroup {{ worker: {worker}, size: 0, cancelled: true }}"),
                    "the corun emptied the group and the cancelled bit is never cleared"
                );
            }
        })
        .expect("no child panicked, so the group's one child was released in time");
    }
}

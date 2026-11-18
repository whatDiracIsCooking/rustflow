//! Translated from `unittests/test_asyncs.cpp` (Taskflow v4.1.0).
//!
//! Five of upstream's groups are here. `Async` is a hundred thousand
//! value-returning submissions whose handles are collected and then read, and
//! `NestedAsync` is the same hundred thousand with a chain of three more
//! submitted from inside each body; both run at one, two, four, eight and
//! sixteen workers, one `#[test]` per worker count as upstream has one
//! `TEST_CASE`, so a failure names the pool it happened on.
//! `MixedAsync.<N>Executors` is the third and is about something else
//! entirely -- N *executors*, submitting to each other -- and its own
//! function says what it pins.
//!
//! Each of those first two cases asserts two separate things, which is why
//! the counter is there at all when the handles already carry a value: the
//! counter says every body
//! *ran*, and the sum over the handles says every body's result reached the
//! caller who asked for it. Upstream asserts the same pair -- `counter == N`
//! after `wait_for_all`, then `-c == 2*N` over the futures -- and the order
//! matters, since `wait_for_all` is the only thing that waits for a body
//! nobody is holding a handle on.
//!
//! **The three `TaskParams` branches of `Async` are two here.** Upstream's
//! loop picks between `async(f)`, `async(DefaultTaskParams{}, f)` and
//! `async(TaskParams{std::to_string(i)}, f)` on `i % 3`, and all three differ
//! only in the name the node is given. Its first two are one call in the
//! port, an unnamed submission being the one that passes
//! `DefaultTaskParams{}`; its third is
//! [`Executor::spawn_named`](rustflow::Executor::spawn_named), so the pick
//! survives as `i % 3 == 2`. Neither upstream nor the loop here ever reads
//! one of those names back, which is what [`names_reach_an_observer`] is for.
//!
//! **`AsyncRuntime`** and **`RecursiveAsyncRuntime`** are the fourth and
//! fifth, and are #92's: both need a runtime body able to spawn work *under
//! its own node*, which is
//! [`Runtime::spawn`](rustflow::Runtime::spawn) and
//! [`Runtime::silent_async`](rustflow::Runtime::silent_async). The bodies
//! themselves arrived with #90 -- `MixedAsync.<N>Executors` submits them --
//! but a body that cannot spawn a child cannot be recursive, and
//! `Runtime::corun` had nothing to wait for until then. Both keep upstream's
//! own figures and worker counts: a thousand children per body at one through
//! eight workers, and a hundred-by-ten tree at one through four.
//!
//! Neither is affected by the stack-depth cliff `tests/runtimes.rs` measures
//! and issue #119 owns, and the reason is worth knowing before adding a case
//! here: nesting needs a *corun* to hold a frame open while its worker runs
//! other people's work, and the deepest thing in this file is one corun over
//! children that spawn nothing. `RecursiveAsyncRuntime` recurses two levels
//! and coruns nowhere, so its 1101 tasks cost one frame each.
//!
//! One of upstream's groups is still named rather than ported:
//!
//! - **`MixedAsync.<N>threads`** (upstream's `mixed_async`) submits plain
//!   bodies from the tasks of a *running* graph. It needs nothing this crate
//!   lacks -- #88's `spawn` and `silent_async` are the whole of it -- and is
//!   a follow-up rather than part of #90 or #92, both of which are about the
//!   runtime-shaped bodies the groups above submit.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rustflow::observer::{Observer, TaskView, WorkerView};
use rustflow::prelude::*;

// The matrix macro, and the spin the wait on the last share below is.
// `wait_until_ran` is deliberately not that spin: its message names how many
// of how many bodies arrived, which a `what` fixed before the wait begins
// cannot say -- so it keeps its own loop and [`GIVE_UP_AFTER`].
#[macro_use]
mod common;
#[path = "common/spin.rs"]
mod spin;

use rustflow::{AsyncHandle, Executor};
use spin::spin_until;

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
        .map(|task| {
            let counter = Arc::clone(&counter);
            let body = move || {
                // Relaxed, as upstream's is: what orders these increments
                // against the assertion below is `wait_for_all`, not this
                // word.
                counter.fetch_add(1, Ordering::Relaxed);
                RESULT
            };

            // Upstream's `i % 3` over two branches rather than three, for the
            // reason the module docs give.
            if task % 3 == 2 {
                executor.spawn_named(task.to_string(), body)
            } else {
                executor.spawn(body)
            }
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

/// Upstream's `const size_t T = 1000` in `mixed_executor_async`: how many
/// rounds of submissions each executor gets.
const ROUNDS: usize = 1000;

/// How many workers each of the N executors is given.
///
/// **Upstream's `std::vector<tf::Executor> executors(N)` gives each one the
/// hardware concurrency**, which at N = 8 is eight pools of as many threads
/// as the machine has cores. What the case is about is which executor a
/// worker belongs to rather than how much parallelism there is, and two
/// workers per pool exercises both answers `this_worker_id` can give while
/// keeping the thread count a function of N alone.
const WORKERS_EACH: usize = 2;

/// How long the case waits for the work it submitted before giving up.
///
/// Upstream waits with `while(counter.load() != 4000*N);`, a spin with no way
/// out. A deadline is what turns "a body never ran" from a hung test run into
/// a failed assertion -- and nothing here passes because of the number: the
/// only thing that reaches it is an increment that is never coming.
const GIVE_UP_AFTER: Duration = Duration::from_secs(120);

/// What every body of `mixed_executor_async` shares: the executors it may
/// submit to, and the two things the test thread reads afterwards.
///
/// One allocation shared by every body, where upstream's lambdas capture
/// `&executors` and `&counter` out of the enclosing frame. A body here is
/// `Send + 'static` and cannot borrow either, which is the capture
/// `tests/corun.rs` makes the same way and for the same reason.
struct Pools {
    /// The N executors, named by index as upstream names them by subscript.
    executors: Vec<Arc<Executor>>,
    /// Upstream's `counter`: one increment per body that ran.
    ran: AtomicUsize,
    /// How many times a body found itself on a worker of the wrong executor,
    /// which is upstream's `check_wid` as a number instead of as a `REQUIRE`.
    ///
    /// It has to come back as a number, and that is the one thing this case
    /// cannot spell upstream's way: a panic inside a *detached* async body is
    /// caught and delivered to a handle nobody kept, or dropped with the node
    /// for a silent task, so an assertion in there would be swallowed and the
    /// only symptom would be an increment that never arrives -- a timeout
    /// rather than a failure. Counting instead keeps the regression an
    /// assertion on the thread that can report it.
    misplaced: AtomicUsize,
}

impl Pools {
    /// `n` executors and two counters at zero.
    fn new(n: usize) -> Arc<Self> {
        Arc::new(Self {
            executors: (0..n)
                .map(|_| Arc::new(Executor::new(WORKERS_EACH)))
                .collect(),
            ran: AtomicUsize::new(0),
            misplaced: AtomicUsize::new(0),
        })
    }

    /// Upstream's `check_wid` plus its `counter.fetch_add`: records that a
    /// body submitted to executor `mine` ran, and whether it ran where it was
    /// submitted.
    ///
    /// The claim is about `this_worker_id` rather than about async tasks: the
    /// map that answers it is per-executor, so a worker of one pool is a
    /// stranger to every other pool. Upstream's `!= -1` / `== -1` pair is
    /// `is_some` / `is_none` here.
    fn ran_on(&self, mine: usize) {
        for (index, executor) in self.executors.iter().enumerate() {
            if executor.this_worker_id().is_some() != (index == mine) {
                self.misplaced.fetch_add(1, Ordering::Relaxed);
            }
        }
        // Last, so that a body counted as having run has already answered the
        // question above: the spin below is what the assertions wait on.
        self.ran.fetch_add(1, Ordering::Relaxed);
    }

    /// Spins until `self.ran` reads `target`, failing rather than hanging if
    /// it never does: upstream's `while(counter.load() != 4000*N);`.
    fn wait_until_ran(&self, target: usize) {
        let deadline = Instant::now() + GIVE_UP_AFTER;
        loop {
            let seen = self.ran.load(Ordering::Relaxed);
            if seen == target {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "gave up waiting for every body of every executor: {seen} of {target}"
            );
            thread::yield_now();
        }
    }
}

/// Spins until nothing but `pools` itself holds a share of it.
///
/// Not upstream's problem and very much this port's: every body holds a share
/// of the [`Pools`], which holds a share of each executor, so the *last*
/// share of an executor must be the test thread's. A worker that dropped it
/// would run `Executor::drop` on a thread that destructor is about to join,
/// and the node holding the last body is dropped by its worker just *after*
/// the counter `wait_for_all` watches -- so this is a short wait rather than
/// an invariant, and [`spin_until`]'s deadline rather than a bare spin.
fn wait_for_sole_owner(pools: &Arc<Pools>) {
    spin_until("the last async node to be dropped", || {
        Arc::strong_count(pools) == 1
    });
}

/// Upstream's `mixed_executor_async(size_t N)`: `N` executors, each handed
/// `ROUNDS` rounds of two submissions, each of whose bodies submits a
/// **runtime-shaped** body to one of its neighbours.
///
/// Four bodies per round per executor -- a `spawn` and its inner
/// `spawn_runtime`, a `silent_async` and its inner `silent_async_runtime` --
/// which is upstream's `4000*N` increments at `ROUNDS == 1000`. Two things
/// are pinned at once and both need the cross-executor shape:
///
/// - **`this_worker_id` is per-executor.** Every one of the four bodies walks
///   all N executors and records whether it is a worker of exactly the one it
///   was submitted to, which is upstream's `check_wid`.
/// - **A runtime-shaped body runs wherever it was submitted.** The inner
///   submissions go to `j % N` and `(j + 1) % N`, so a worker of one pool
///   hands a `Runtime` body to another pool, and the executor that runs it is
///   the one that has to account for it -- which the `num_topologies` of each
///   is read to check.
///
/// Both inner handles are dropped, which detaches, exactly as upstream
/// discards the futures its `async` calls return: what waits for the work is
/// the counter and then `wait_for_all`, in upstream's order. The order
/// matters -- a `wait_for_all` on one executor says nothing about a body of
/// *another* executor that is about to submit work to it, so the counter is
/// the real wait and the `wait_for_all`s are what leave every pool idle.
fn mixed_executor_async(n: usize) {
    let pools = Pools::new(n);

    for j in 0..ROUNDS {
        for i in 0..n {
            // Upstream's `executors[i].async([&, i, j](){ ... })`, whose body
            // submits `executors[j % N].async([](tf::Runtime&){ ... })`.
            let outer = Arc::clone(&pools);
            let inner_index = j % n;
            let _detached = pools.executors[i].spawn(move || {
                outer.ran_on(i);

                let inner = Arc::clone(&outer);
                let _detached =
                    outer.executors[inner_index].spawn_runtime(move |_: &mut Runtime<'_>| {
                        inner.ran_on(inner_index);
                    });
            });

            // And upstream's silent pair, whose inner submission goes to
            // `(j + 1) % N` instead.
            let outer = Arc::clone(&pools);
            let inner_index = (j + 1) % n;
            pools.executors[i].silent_async(move || {
                outer.ran_on(i);

                let inner = Arc::clone(&outer);
                outer.executors[inner_index].silent_async_runtime(move |_: &mut Runtime<'_>| {
                    inner.ran_on(inner_index);
                });
            });
        }
    }

    pools.wait_until_ran(4 * ROUNDS * n);

    for executor in &pools.executors {
        executor.wait_for_all();
        assert_eq!(
            executor.num_topologies(),
            0,
            "{n} executors: every async task of this one is accounted for"
        );
    }

    assert_eq!(
        pools.misplaced.load(Ordering::Relaxed),
        0,
        "{n} executors: every body ran on a worker of the executor it was \
         submitted to, and of no other"
    );

    wait_for_sole_owner(&pools);
}

worker_matrix! {
    spawned_tasks {
        async_1_thread(1),
        async_2_threads(2),
        async_4_threads(4),
        async_8_threads(8),
        async_16_threads(16),
    }
}

worker_matrix! {
    nested_spawned_tasks {
        nested_async_1_thread(1),
        nested_async_2_threads(2),
        nested_async_4_threads(4),
        nested_async_8_threads(8),
        nested_async_16_threads(16),
    }
}

worker_matrix! {
    mixed_executor_async {
        mixed_async_1_executor(1),
        mixed_async_2_executors(2),
        mixed_async_4_executors(4),
        mixed_async_5_executors(5),
        mixed_async_6_executors(6),
        mixed_async_7_executors(7),
        mixed_async_8_executors(8),
    }
}

/// Upstream's `async_runtime(size_t W)`: four runtime tasks of one graph,
/// spawning four thousand children between them, two of which corun and two
/// of which leave their children to the implicit join.
///
/// The `4005` is the whole assertion and it is arithmetic over work the run
/// cannot have finished without doing -- three plain tasks, two thousand
/// children under the two bodies that corun, and two thousand and two under
/// the two that do not. What the two halves pin is the pair the join is made
/// of: a corun returns only when this node's children are done, and a body
/// that returns without coruning keeps its node preempted until they are.
///
/// Upstream's `sf.async` is [`Runtime::spawn`] here and its discarded
/// `std::future` a dropped [`AsyncHandle`], which detaches the result and not
/// the task.
fn runtime_spawned_tasks(workers: usize) {
    /// Upstream's `for(int i=0; i<1000; i++)`, four times over.
    const CHILDREN: usize = 1000;
    /// Upstream's `REQUIRE(counter == 4005)`.
    const TOTAL: usize = 3 + 2 * CHILDREN + 2 * (CHILDREN + 1);

    let executor = Executor::new(workers);
    let counter = Arc::new(AtomicUsize::new(0));
    let bump = |counter: &Arc<AtomicUsize>| {
        let counter = Arc::clone(counter);
        move || {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    };

    let mut taskflow = Taskflow::new();
    let a = taskflow.emplace(bump(&counter));
    let b = taskflow.emplace(bump(&counter));
    taskflow.emplace(bump(&counter));

    // Upstream's `S1`: a thousand silent children, waited for explicitly.
    let s1 = taskflow.emplace_runtime({
        let counter = Arc::clone(&counter);
        move |runtime: &mut Runtime<'_>| {
            for _ in 0..CHILDREN {
                runtime.silent_async(bump(&counter));
            }
            runtime.corun();
        }
    });

    // Upstream's `S2`: one more than a thousand, left to the implicit join.
    let s2 = taskflow.emplace_runtime({
        let counter = Arc::clone(&counter);
        move |runtime: &mut Runtime<'_>| {
            runtime.silent_async(bump(&counter));
            for _ in 0..CHILDREN {
                runtime.silent_async(bump(&counter));
            }
        }
    });

    // Upstream's third: a thousand *handle-returning* children and a corun,
    // which is what pins that a dropped handle changes nothing about the
    // join.
    taskflow.emplace_runtime({
        let counter = Arc::clone(&counter);
        move |runtime: &mut Runtime<'_>| {
            runtime.silent_async(bump(&counter));
            for _ in 0..CHILDREN {
                drop(runtime.spawn(bump(&counter)));
            }
            runtime.corun();
        }
    });

    // Upstream's fourth: handle-returning children and no corun at all.
    taskflow.emplace_runtime({
        let counter = Arc::clone(&counter);
        move |runtime: &mut Runtime<'_>| {
            for _ in 0..CHILDREN {
                drop(runtime.spawn(bump(&counter)));
            }
        }
    });

    taskflow.task(a).precede([s1, s2]);
    taskflow.task(s1).precede([b]);
    taskflow.task(s2).precede([b]);

    executor.run(&taskflow).wait().expect("no task panicked");

    assert_eq!(
        counter.load(Ordering::Relaxed),
        TOTAL,
        "{workers} workers: every child of every runtime task ran"
    );
}

/// Upstream's `recursive_async_runtime(size_t num_threads)`: a free
/// runtime-shaped task spawning a hundred runtime-shaped children, each
/// spawning ten of its own, with **no corun anywhere** -- once through
/// [`Executor::spawn_runtime`] and once through
/// [`Executor::dependent_async_runtime`].
///
/// What it adds to `Runtime.RecursiveImplicitJoin` in `tests/runtimes.rs`,
/// which is the same shape under a *graph's* runtime task, is the root: the
/// node every one of those 1101 tasks is ultimately counted against is a
/// **free** node rather than a task of a run, so the thing that waits is an
/// [`AsyncHandle`] rather than a `wait` on a run. The second half is the one
/// case in the suite that reaches the runtime-shaped *dependent*-async form
/// on the executor, whose node is entered twice like any other reentrant one.
fn recursive_runtime_spawned_tasks(workers: usize) {
    /// Upstream's `N1`.
    const CHILDREN: usize = 100;
    /// Upstream's `N2`.
    const GRANDCHILDREN: usize = 10;
    /// Upstream's `REQUIRE(counter == N2*N1 + N1 + 1)`.
    const TOTAL: usize = GRANDCHILDREN * CHILDREN + CHILDREN + 1;

    /// The body both halves submit: count, spawn `CHILDREN` children, and
    /// return without waiting for any of them.
    fn spawn_tree(
        counter: Arc<AtomicUsize>,
    ) -> impl for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static {
        move |runtime: &mut Runtime<'_>| {
            counter.fetch_add(1, Ordering::Relaxed);
            for _ in 0..CHILDREN {
                let counter = Arc::clone(&counter);
                runtime.silent_async_runtime(move |child: &mut Runtime<'_>| {
                    counter.fetch_add(1, Ordering::Relaxed);
                    for _ in 0..GRANDCHILDREN {
                        let counter = Arc::clone(&counter);
                        // Upstream's innermost body takes a `tf::Runtime&` it
                        // never uses, which is worth keeping: it is a
                        // runtime-shaped *leaf*, so its node is entered once
                        // and finishes on that entry.
                        child.silent_async_runtime(move |_: &mut Runtime<'_>| {
                            counter.fetch_add(1, Ordering::Relaxed);
                        });
                    }
                });
            }
        }
    }

    let executor = Executor::new(workers);

    let counter = Arc::new(AtomicUsize::new(0));
    executor
        .spawn_runtime(spawn_tree(Arc::clone(&counter)))
        .wait()
        .expect("nothing under the tree panicked");
    assert_eq!(
        counter.load(Ordering::Relaxed),
        TOTAL,
        "{workers} workers: the handle waited for the whole tree"
    );

    let counter = Arc::new(AtomicUsize::new(0));
    let (_task, done) = executor.dependent_async_runtime(spawn_tree(Arc::clone(&counter)), []);
    done.wait().expect("nothing under the tree panicked");
    assert_eq!(
        counter.load(Ordering::Relaxed),
        TOTAL,
        "{workers} workers: the same tree under a dependent-async root"
    );
}

worker_matrix! {
    runtime_spawned_tasks {
        async_runtime_1_thread(1),
        async_runtime_2_threads(2),
        async_runtime_3_threads(3),
        async_runtime_4_threads(4),
        async_runtime_5_threads(5),
        async_runtime_6_threads(6),
        async_runtime_7_threads(7),
        async_runtime_8_threads(8),
    }
}

worker_matrix! {
    recursive_runtime_spawned_tasks {
        recursive_async_runtime_1_thread(1),
        recursive_async_runtime_2_threads(2),
        recursive_async_runtime_3_threads(3),
        recursive_async_runtime_4_threads(4),
    }
}

/// Records the name of every task a worker enters, as a *set*.
///
/// Which names were seen rather than how many times each was, because a
/// runtime-shaped node is entered a second time to report its outcome and
/// nothing below is about that count.
#[derive(Default)]
struct Names(Mutex<BTreeSet<String>>);

impl Observer for Names {
    fn set_up(&self, _num_workers: usize) {}

    fn on_entry(&self, _worker: WorkerView<'_>, task: TaskView<'_>) {
        self.0
            .lock()
            .expect("an observer's own lock is never held across a body")
            .insert(task.name().to_string());
    }

    fn on_exit(&self, _worker: WorkerView<'_>, _task: TaskView<'_>) {}
}

/// Invented: the name a free submission gives its node, read back the one way
/// a caller can read it -- an observer's `on_entry` -- over every free form of
/// the family, against a submission that names nothing.
///
/// Issue #258's bar. Upstream's own cases pass a `TaskParams` and never look
/// at the name again, so what is added here is the read: the name reaches the
/// node the task is, and an unnamed submission leaves that node with the
/// empty name upstream's `DefaultTaskParams` leaves it. All three receivers,
/// because each builds its own `Submission` and the name travels beside that
/// rather than inside it. One worker, because the question is which name is
/// on which node rather than who ran it.
#[test]
fn names_reach_an_observer() {
    let executor = Arc::new(Executor::new(1));
    let seen = executor.make_observer(Names::default());

    // The executor's own four, parented to nothing.
    executor
        .spawn_named("spawned", || {})
        .wait()
        .expect("the body did not panic");
    executor.silent_async_named("silent", || {});
    let _detached = executor.spawn_runtime_named("runtime", |_: &mut Runtime<'_>| {});
    executor.silent_async_runtime_named("silent runtime", |_: &mut Runtime<'_>| {});

    // A runtime body's four, counted against the node the body is of, and
    // waited for by its implicit join. The outer submission is left unnamed
    // on purpose, so the empty name below is asserted twice over.
    executor
        .spawn_runtime(|runtime: &mut Runtime<'_>| {
            let _handle = runtime.spawn_named("runtime's spawned", || {});
            runtime.silent_async_named("runtime's silent", || {});
            let _nested =
                runtime.spawn_runtime_named("runtime's runtime", |_: &mut Runtime<'_>| {});
            runtime
                .silent_async_runtime_named("runtime's silent runtime", |_: &mut Runtime<'_>| {});
        })
        .wait()
        .expect("nothing under the body panicked");

    // And a task group's four, counted against the group's anchor.
    executor
        .spawn({
            let executor = Arc::clone(&executor);
            move || {
                let group = executor.task_group().expect("a worker may open a group");
                let _handle = group.spawn_named("group's spawned", || {});
                group.silent_async_named("group's silent", || {});
                let _nested =
                    group.spawn_runtime_named("group's runtime", |_: &mut Runtime<'_>| {});
                group
                    .silent_async_runtime_named("group's silent runtime", |_: &mut Runtime<'_>| {});
                group.corun();
            }
        })
        .wait()
        .expect("no task of the group panicked");

    // The default, which this issue did not change: upstream's
    // `DefaultTaskParams{}` leaves `_name` default-constructed.
    executor.silent_async(|| {});

    executor.wait_for_all();
    assert_eq!(
        *seen.0.lock().expect("no body panicked"),
        BTreeSet::from([
            String::new(),
            "group's runtime".to_string(),
            "group's silent".to_string(),
            "group's silent runtime".to_string(),
            "group's spawned".to_string(),
            "runtime".to_string(),
            "runtime's runtime".to_string(),
            "runtime's silent".to_string(),
            "runtime's silent runtime".to_string(),
            "runtime's spawned".to_string(),
            "silent".to_string(),
            "silent runtime".to_string(),
            "spawned".to_string(),
        ])
    );
}

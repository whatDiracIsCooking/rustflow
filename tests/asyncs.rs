//! Translated from `unittests/test_asyncs.cpp` (Taskflow v4.1.0).
//!
//! Three of upstream's groups are here. `Async` is a hundred thousand
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
//! - **`AsyncRuntime`** and **`RecursiveAsyncRuntime`** need a runtime body
//!   to be able to spawn work *under its own node*: `Runtime::spawn` and
//!   `Runtime::silent_async`, which are #92. The bodies themselves arrived
//!   with #90 -- `MixedAsync.<N>Executors` below submits them -- but a body
//!   that cannot spawn a child cannot be recursive, and
//!   `Runtime::corun` has nothing to wait for until then.
//! - **`MixedAsync.<N>threads`** (upstream's `mixed_async`) submits plain
//!   bodies from the tasks of a *running* graph. It needs nothing this crate
//!   lacks -- #88's `spawn` and `silent_async` are the whole of it -- and is
//!   a follow-up rather than part of #90, which is about the runtime-shaped
//!   bodies the group above submits.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rustflow::{AsyncHandle, Executor, Runtime};

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
    fn spin_until_ran(&self, target: usize) {
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
/// an invariant, and a deadline rather than a spin, for the reason
/// [`GIVE_UP_AFTER`] gives.
fn spin_until_sole_owner(pools: &Arc<Pools>) {
    let deadline = Instant::now() + GIVE_UP_AFTER;
    while Arc::strong_count(pools) != 1 {
        assert!(
            Instant::now() < deadline,
            "gave up waiting for the last async node to be dropped"
        );
        thread::yield_now();
    }
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

    pools.spin_until_ran(4 * ROUNDS * n);

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

    spin_until_sole_owner(&pools);
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

#[test]
fn mixed_async_1_executor() {
    mixed_executor_async(1);
}

#[test]
fn mixed_async_2_executors() {
    mixed_executor_async(2);
}

#[test]
fn mixed_async_4_executors() {
    mixed_executor_async(4);
}

#[test]
fn mixed_async_5_executors() {
    mixed_executor_async(5);
}

#[test]
fn mixed_async_6_executors() {
    mixed_executor_async(6);
}

#[test]
fn mixed_async_7_executors() {
    mixed_executor_async(7);
}

#[test]
fn mixed_async_8_executors() {
    mixed_executor_async(8);
}

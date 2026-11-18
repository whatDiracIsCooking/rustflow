//! Translated from `unittests/test_dependent_asyncs.cpp` (Taskflow v4.1.0).
//!
//! Fourteen of upstream's groups, which is every one of them that needs
//! nothing but a closure, a handle and the edges between them: the graph is
//! built one task at a time while the tasks already submitted are running,
//! which is the whole of what `Executor::silent_dependent_async` and
//! `Executor::dependent_async` are for. The worker counts are upstream's, one
//! `#[test]` per count as it has one `TEST_CASE`, so a failure names the pool
//! it happened on.
//!
//! **`DependentAsync.FromRuntime`** and
//! **`SilentDependentAsync.FromRuntime`** are the last two and are #92's:
//! they build their chain from inside a *runtime body*, through
//! [`Runtime::dependent_async`](rustflow::Runtime::dependent_async) and its
//! three siblings, so every link is counted against the runtime task's own
//! node and the body's implicit join is what waits for the chain.
//!
//! One group is deliberately absent:
//!
//! - **`DependentAsync.MixedAlgorithms`** and
//!   **`SilentDependentAsync.MixedAlgorithms`** build their dependencies out
//!   of `tf::make_for_each_task`, `make_reduce_task`, `make_sort_task` and
//!   friends, which are the algorithm library and milestone 6. Nothing about
//!   the dependency protocol is particular to them.
//!
//! # How the shared results are written
//!
//! Upstream's bodies write `std::vector<int>` (and
//! `std::vector<tf::CachelineAligned<int>>`) at disjoint indices, reading the
//! elements their dependencies wrote. The port's [`Results`] is a shared
//! vector of [`AtomicI64`], and three things about that are worth saying:
//!
//! - **Atomics because Rust says so, not because the ordering needs them.**
//!   Several tasks write the vector at once, so in Rust it has to be an
//!   interior-mutable type whatever the indices are; `Relaxed` is enough on
//!   every one of these accesses because what orders a read after the write
//!   it depends on is the dependency edge -- the predecessor's tear-down
//!   releases and the successor's dequeue acquires -- and a relaxed access
//!   ordered by a happens-before still sees it.
//! - **`i64` where upstream has `int`.** `SilentDependentAsync.LinearChain`
//!   accumulates `1 + 2 + ... + 100000`, which is about `5e9` and overflows a
//!   32-bit `int` long before the chain ends; the assertion upstream makes is
//!   of the recurrence rather than of a value, so it passes there regardless.
//!   An `i64` makes it true as well as passing.
//! - **No cacheline padding.** `tf::CachelineAligned` is a false-sharing
//!   measure rather than a correctness one, and these cases are not
//!   measurements.
//!
//! # Two shapes that do not survive translation unchanged
//!
//! **An assertion inside a `silent_dependent_async` body is not a failing
//! test.** Such a body's panic is kept on its own node -- that is the
//! documented behaviour and `executor::dependent_async`'s own cases pin it --
//! so a `REQUIRE` upstream makes inside one would be swallowed here.
//! `DependentAsync.ParallelGraphConstruction` is the case that does it, and
//! the port counts the mismatches in an atomic that the test thread asserts
//! on instead.
//!
//! **Upstream guards every case with `doctest::timeout(300)`.** Cargo's test
//! harness has no equivalent, and these cases are bulk throughput whose only
//! real failure mode is a release that never happens -- a task that never
//! runs, which is a counter that never reaches zero and therefore a
//! `wait_for_all` that never returns. The shapes whose regression that is are
//! pinned by *assertion*, with a deadline, in `executor::dependent_async`'s
//! unit tests; what these add is the scale at which the race actually
//! happens.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use rustflow::prelude::*;
use rustflow::{AsyncTask, Executor};

// Only the matrix macro. What the cases here record is a per-task *value*
// rather than a count -- the `Results` array below -- and the three counters
// that are counts are read with the array in the same assertion, so
// `common/counters.rs` would carry a third of them.
#[macro_use]
mod common;

/// Where the tasks of one case leave their results: upstream's
/// `std::vector<int>`, shared with every body that writes it.
type Results = Arc<Vec<AtomicI64>>;

/// `count` results, all zero.
fn results(count: usize) -> Results {
    filled(count, 0)
}

/// `count` results, all `value` -- upstream's `std::vector<int> data(n, -1)`,
/// whose initial value is what tells a result that was never written from one
/// that was.
fn filled(count: usize, value: i64) -> Results {
    Arc::new((0..count).map(|_| AtomicI64::new(value)).collect())
}

/// The result at `index`.
///
/// `Relaxed`, for the reason the module docs give: the dependency edge is
/// what orders this against the write it is reading.
fn read(results: &Results, index: usize) -> i64 {
    results[index].load(Ordering::Relaxed)
}

/// Writes `value` as the result at `index`.
fn write(results: &Results, index: usize, value: i64) {
    results[index].store(value, Ordering::Relaxed);
}

/// An index as the arithmetic the bodies do over it.
///
/// A conversion rather than an `as` cast because the crate denies
/// `cast_possible_truncation`, and the panic cannot happen: every index here
/// is a loop bound of at most a hundred thousand.
fn index(index: usize) -> i64 {
    i64::try_from(index).expect("a test's index fits in an i64")
}

/// Upstream's `fibonacci` table, which is what both of its Fibonacci cases
/// assert against.
///
/// Copied rather than computed on purpose: a table generated by the same
/// recurrence the tasks implement would make either case assert that
/// addition is addition, where what is under test is that the dependencies
/// resolved in the right order.
const FIBONACCI: [u64; 93] = [
    0,
    1,
    1,
    2,
    3,
    5,
    8,
    13,
    21,
    34,
    55,
    89,
    144,
    233,
    377,
    610,
    987,
    1597,
    2584,
    4181,
    6765,
    10946,
    17711,
    28657,
    46368,
    75025,
    121393,
    196418,
    317811,
    514229,
    832040,
    1346269,
    2178309,
    3524578,
    5702887,
    9227465,
    14930352,
    24157817,
    39088169,
    63245986,
    102334155,
    165580141,
    267914296,
    433494437,
    701408733,
    1134903170,
    1836311903,
    2971215073,
    4807526976,
    7778742049,
    12586269025,
    20365011074,
    32951280099,
    53316291173,
    86267571272,
    139583862445,
    225851433717,
    365435296162,
    591286729879,
    956722026041,
    1548008755920,
    2504730781961,
    4052739537881,
    6557470319842,
    10610209857723,
    17167680177565,
    27777890035288,
    44945570212853,
    72723460248141,
    117669030460994,
    190392490709135,
    308061521170129,
    498454011879264,
    806515533049393,
    1304969544928657,
    2111485077978050,
    3416454622906707,
    5527939700884757,
    8944394323791464,
    14472334024676221,
    23416728348467685,
    37889062373143906,
    61305790721611591,
    99194853094755497,
    160500643816367088,
    259695496911122585,
    420196140727489673,
    679891637638612258,
    1100087778366101931,
    1779979416004714189,
    2880067194370816120,
    4660046610375530309,
    7540113804746346429,
];

/// Upstream's `int N = 100000`, in `embarrassing_parallelism` and
/// `silent_dependent_async_linear_chain`.
///
/// A stress count rather than a correctness one, and the two use it for
/// opposite shapes: a hundred thousand tasks with no edges at all, and a
/// hundred thousand with one edge each, which is a chain whose every link is
/// submitted while the link before it may already be running.
const TASKS: usize = 100_000;

/// How many workers a case gets where upstream writes `tf::Executor
/// executor;`.
///
/// Upstream's default is the hardware's thread count, which on the two-core
/// box CI runs on would be two; four keeps the case oversubscribed, which is
/// the only thing a default-constructed executor contributes to it.
const DEFAULT_WORKERS: usize = 4;

/// Upstream's `DependentAsync.NullDependency`: a default-constructed
/// `AsyncTask` names no task, so naming it as a dependency waits for nothing.
///
/// The case is here rather than in the crate's own tests because it is
/// upstream's, and it is the one place the empty handle is used as what it is
/// -- a placeholder a caller may hold before deciding whether there is
/// anything to wait for.
#[test]
fn null_dependency() {
    let executor = Executor::new(DEFAULT_WORKERS);
    let values = results(3);
    let dummy = AsyncTask::default();

    let first = executor.silent_dependent_async(
        {
            let values = Arc::clone(&values);
            move || write(&values, 0, 100)
        },
        [&dummy],
    );
    let second = executor.silent_dependent_async(
        {
            let values = Arc::clone(&values);
            move || write(&values, 1, 200)
        },
        [&dummy],
    );
    let (_third, third_value) = executor.dependent_async(
        {
            let values = Arc::clone(&values);
            move || {
                let sum = read(&values, 0) + read(&values, 1);
                write(&values, 2, sum);
                sum
            }
        },
        [&first, &second],
    );

    assert_eq!(third_value.wait().expect("no body panicked"), 300);
    assert_eq!(read(&values, 0), 100);
    assert_eq!(read(&values, 1), 200);
    assert_eq!(read(&values, 2), read(&values, 0) + read(&values, 1));
}

/// Upstream's `embarrassing_parallelism(unsigned W)`: a hundred thousand
/// dependent-async tasks with no dependencies at all, half of them silent and
/// half handing back a handle nobody reads.
///
/// What it covers that the crate's own cases cannot is the submission path
/// under load: a node with no dependencies is queued by the submitting thread
/// itself, so this is a hundred thousand of those racing a pool that is
/// already draining the buffers. The handles of the second half are
/// **dropped**, which detaches -- the task still runs -- exactly as upstream
/// discards the `std::pair` its `dependent_async` returns.
fn embarrassing_parallelism(workers: usize) {
    let executor = Executor::new(workers);
    let counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..TASKS / 2 {
        let counter = Arc::clone(&counter);
        executor.silent_dependent_async(
            move || {
                counter.fetch_add(1, Ordering::Relaxed);
            },
            [],
        );
    }

    for _ in TASKS / 2..TASKS {
        let counter = Arc::clone(&counter);
        executor.dependent_async(
            move || {
                counter.fetch_add(1, Ordering::Relaxed);
            },
            [],
        );
    }

    executor.wait_for_all();
    assert_eq!(counter.load(Ordering::Relaxed), TASKS, "{workers} workers");
}

/// Upstream's `silent_dependent_async_linear_chain(unsigned W)`: a hundred
/// thousand tasks, each depending on the one before it and reading what it
/// wrote.
///
/// The assertion is upstream's recurrence rather than a table of values, and
/// it is the strongest thing a chain can say: every link ran after the link
/// it named, and read the value that link wrote.
fn linear_chain(workers: usize) {
    let executor = Executor::new(workers);
    let values = results(TASKS);

    let mut previous: Option<AsyncTask> = None;
    for step in 0..TASKS {
        let task = executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || {
                    let value = if step == 0 {
                        1
                    } else {
                        read(&values, step - 1) + index(step)
                    };
                    write(&values, step, value);
                }
            },
            // Upstream clears its vector and pushes the new task, so the
            // dependency list is always the one task before this one; an
            // `Option` is that list.
            previous.as_ref(),
        );
        previous = Some(task);
    }

    executor.wait_for_all();

    assert_eq!(read(&values, 0), 1, "{workers} workers");
    for step in 1..TASKS {
        assert_eq!(
            read(&values, step),
            read(&values, step - 1) + index(step),
            "{workers} workers, step {step}"
        );
    }
}

/// Upstream's `simple_graph(unsigned W)`, whose shape is its own comment:
///
/// ```text
///    |--> 1   |--> 4
/// 0 ----> 2 -----> 5
///    |--> 3   |--> 6
/// ```
///
/// A hundred rounds of it, each waited for as a whole, which is what makes
/// the arithmetic an assertion about *order*: every body reads a result its
/// dependency wrote, so a release that came too early would show up as a zero
/// rather than as a race nobody sees.
fn simple_graph(workers: usize) {
    let executor = Executor::new(workers);

    for id in 0..100 {
        let id = index(id);
        let values = results(7);

        let first = executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || write(&values, 0, 100 + id)
            },
            [],
        );

        // Upstream passes `tasks.begin(), tasks.end()` over a vector holding
        // only task 0, which is the one-element list these three name.
        executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || write(&values, 1, read(&values, 0) * 6 + id)
            },
            [&first],
        );
        let second = executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || write(&values, 2, read(&values, 0) - 200 + id)
            },
            [&first],
        );
        executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || write(&values, 3, read(&values, 0) / 9 + id)
            },
            [&first],
        );

        executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || write(&values, 4, read(&values, 2) + 66 + id)
            },
            [&second],
        );
        executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || write(&values, 5, read(&values, 2) - 999 + id)
            },
            [&second],
        );
        executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || write(&values, 6, read(&values, 2) * 9 / 13 + id)
            },
            [&second],
        );

        executor.wait_for_all();

        assert_eq!(read(&values, 0), 100 + id, "{workers} workers, round {id}");
        assert_eq!(read(&values, 1), read(&values, 0) * 6 + id);
        assert_eq!(read(&values, 2), read(&values, 0) - 200 + id);
        assert_eq!(read(&values, 3), read(&values, 0) / 9 + id);
        assert_eq!(read(&values, 4), read(&values, 2) + 66 + id);
        assert_eq!(read(&values, 5), read(&values, 2) - 999 + id);
        assert_eq!(read(&values, 6), read(&values, 2) * 9 / 13 + id);
    }
}

/// Upstream's `simple_graph_2(unsigned W)`, the ten-task shape its own
/// comment draws, with four of the ten handing back a handle.
///
/// The handles are what this adds to `simple_graph`: the one for task 9 is
/// waited for, and the other three are then asserted to be **done already**
/// -- upstream's `fu.wait_for(1us) == future_status::ready`, which is
/// `is_done()` here -- because task 9 depends transitively on all of them. A
/// result that arrived at a handle late would fail that rather than block.
fn simple_graph_2(workers: usize) {
    let executor = Executor::new(workers);

    for id in 0..100 {
        let id = index(id);
        let values = results(10);

        let first = executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || write(&values, 0, 100 + id)
            },
            [],
        );
        let second = executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || write(&values, 1, 6 * id)
            },
            [],
        );
        let third = executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || write(&values, 2, read(&values, 0) + read(&values, 1) + id)
            },
            [&first, &second],
        );

        let (fourth, fourth_value) = executor.dependent_async(
            {
                let values = Arc::clone(&values);
                move || {
                    let value = read(&values, 2) + id;
                    write(&values, 3, value);
                    value
                }
            },
            [&third],
        );
        let fifth = executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || write(&values, 4, read(&values, 2) + id)
            },
            [&third],
        );
        let (sixth, sixth_value) = executor.dependent_async(
            {
                let values = Arc::clone(&values);
                move || {
                    let value = read(&values, 2) + id;
                    write(&values, 5, value);
                    value
                }
            },
            [&third],
        );
        let seventh = executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || write(&values, 6, read(&values, 2) + id)
            },
            [&third],
        );

        let (eighth, eighth_value) = executor.dependent_async(
            {
                let values = Arc::clone(&values);
                move || {
                    let value = read(&values, 3) + read(&values, 4) + id;
                    write(&values, 7, value);
                    value
                }
            },
            [&fourth, &fifth],
        );
        let ninth = executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || write(&values, 8, read(&values, 5) + read(&values, 6) + id)
            },
            [&sixth, &seventh],
        );

        let (_tenth, tenth_value) = executor.dependent_async(
            {
                let values = Arc::clone(&values);
                move || {
                    let value = read(&values, 0)
                        + read(&values, 1)
                        + read(&values, 2)
                        + read(&values, 7)
                        + read(&values, 8)
                        + id;
                    write(&values, 9, value);
                    value
                }
            },
            [&first, &second, &third, &eighth, &ninth],
        );

        assert_eq!(
            tenth_value.wait().expect("no body panicked"),
            read(&values, 9),
            "{workers} workers, round {id}"
        );

        for (task, handle, result) in [
            ("3", fourth_value, 3),
            ("5", sixth_value, 5),
            ("7", eighth_value, 7),
        ] {
            assert!(
                handle.is_done(),
                "task {task} is finished before the task that depends on it"
            );
            assert_eq!(
                handle.wait().expect("no body panicked"),
                read(&values, result)
            );
        }

        assert_eq!(read(&values, 0), 100 + id);
        assert_eq!(read(&values, 1), 6 * id);
        assert_eq!(read(&values, 2), read(&values, 0) + read(&values, 1) + id);
        assert_eq!(read(&values, 3), read(&values, 2) + id);
        assert_eq!(read(&values, 4), read(&values, 2) + id);
        assert_eq!(read(&values, 5), read(&values, 2) + id);
        assert_eq!(read(&values, 6), read(&values, 2) + id);
        assert_eq!(read(&values, 7), read(&values, 3) + read(&values, 4) + id);
        // Upstream's case 8 asserts `results[5] + results[5] + id`, which
        // passes there only because tasks 5 and 6 compute the same value in
        // this graph. The body's own arithmetic is what is asserted here.
        assert_eq!(read(&values, 8), read(&values, 5) + read(&values, 6) + id);
        assert_eq!(
            read(&values, 9),
            read(&values, 0)
                + read(&values, 1)
                + read(&values, 2)
                + read(&values, 7)
                + read(&values, 8)
                + id
        );
    }
}

/// Upstream's `make_complex_graph(tf::Executor&, int r)`: 10202 tasks in four
/// levels, 10100 of which name exactly one dependency and 101 of which name a
/// hundred.
///
/// This is the case that exercises the protocol at width. A level-3 task is
/// submitted naming a hundred predecessors, an arbitrary number of which have
/// already finished and the rest of which are running, so the guard on the
/// join counter is doing its work on every one of these hundred-and-one
/// submissions -- and the final task names the hundred level-3 tasks and
/// hands back the value the test waits on.
fn complex_graph(executor: &Executor, round: i64) {
    /// Upstream's `int count = 10202`.
    const COUNT: usize = 10202;

    let values = results(COUNT);

    // Task 0.
    let first = executor.silent_dependent_async(
        {
            let values = Arc::clone(&values);
            move || write(&values, 0, 100 + round)
        },
        [],
    );

    // Tasks 1 to 100, each on task 0.
    let level_1: Vec<AsyncTask> = (1..=100)
        .map(|task| {
            executor.silent_dependent_async(
                {
                    let values = Arc::clone(&values);
                    move || write(&values, task, read(&values, 0) + index(task) + round)
                },
                [&first],
            )
        })
        .collect();

    // Tasks 101 to 10100, each on one task of level 1.
    let level_2: Vec<AsyncTask> = (101..=10100)
        .map(|task| {
            let parent = (task - 1) / 100;
            executor.silent_dependent_async(
                {
                    let values = Arc::clone(&values);
                    move || write(&values, task, read(&values, parent) + index(task) + round)
                },
                // Upstream's `std::next(begin, (i-1)/100-1), std::next(begin,
                // (i-1)/100)`, which is a one-element range of level 1.
                &level_1[parent - 1..parent],
            )
        })
        .collect();

    // Tasks 10101 to 10200, each on a hundred tasks of level 2.
    let level_3: Vec<AsyncTask> = (10101..=10200)
        .map(|task| {
            let group = task - 10101;
            let begin = (group + 1) * 100 + 1;
            executor.silent_dependent_async(
                {
                    let values = Arc::clone(&values);
                    move || {
                        let sum: i64 = (begin..begin + 100)
                            .map(|read_at| read(&values, read_at))
                            .sum();
                        write(&values, task, sum + index(task) + round);
                    }
                },
                &level_2[group * 100..group * 100 + 100],
            )
        })
        .collect();

    // Task 10201, on the whole of level 3, and the one handle the case waits
    // on: upstream's `.second.get()`.
    let (_last, last_value) = executor.dependent_async(
        {
            let values = Arc::clone(&values);
            move || {
                let sum: i64 = (10101..=10200).map(|task| read(&values, task)).sum();
                let total = sum + 10201 + round;
                write(&values, 10201, total);
                total
            }
        },
        level_3.iter(),
    );
    let total = last_value.wait().expect("no body panicked");

    assert_eq!(read(&values, 0), 100 + round, "round {round}");
    for task in 1..=100 {
        assert_eq!(read(&values, task), read(&values, 0) + index(task) + round);
    }
    for task in 101..=10100 {
        assert_eq!(
            read(&values, task),
            read(&values, (task - 1) / 100) + index(task) + round
        );
    }
    for task in 10101..=10200 {
        let begin = (task - 10101 + 1) * 100 + 1;
        let sum: i64 = (begin..begin + 100)
            .map(|read_at| read(&values, read_at))
            .sum();
        assert_eq!(read(&values, task), sum + index(task) + round);
    }
    let sum: i64 = (10101..=10200).map(|task| read(&values, task)).sum();
    assert_eq!(read(&values, 10201), sum + 10201 + round);
    assert_eq!(
        total,
        read(&values, 10201),
        "the handle has the task's value"
    );
}

/// Upstream's `complex_graph(unsigned W)`: ten rounds of the graph above on
/// one executor, which is also what makes the rounds interesting -- the tasks
/// of round `r + 1` are submitted while the last of round `r` may still be
/// finishing.
fn complex_graph_rounds(workers: usize) {
    let executor = Executor::new(workers);
    for round in 0..10 {
        complex_graph(&executor, round);
    }
}

/// Upstream's `complex_graph_from_worker(unsigned W, int R)`: the same graph,
/// built `rounds` times at once **from inside task bodies of a running
/// taskflow**.
///
/// Two things are under test that `complex_graph_rounds` cannot reach. The
/// submissions come from workers of the executor rather than from an outside
/// thread, which is the other half of upstream's `_schedule_dependent_async_task`;
/// and each of those bodies then *blocks* its worker on the handle of the
/// task it submitted last, so the pool has to make progress with `rounds`
/// workers parked. Upstream's comment says the same thing as the parameters:
/// `W` must be larger than `R`.
fn complex_graph_from_worker(workers: usize, rounds: i64) {
    let executor = Arc::new(Executor::new(workers));
    let mut taskflow = Taskflow::new();

    for round in 0..rounds {
        let executor = Arc::clone(&executor);
        taskflow.emplace(move || complex_graph(&executor, round));
    }

    executor
        .run(&taskflow)
        .wait()
        .expect("no task of the graph panicked");
    executor.wait_for_all();
}

/// Upstream's `binary_tree(unsigned W)`: 65535 tasks in sixteen levels, each
/// naming its parent in the tree and reading what the parent wrote.
///
/// Level by level, and every task of a level is submitted before any of the
/// next -- so a parent may be finished, running or still queued when its
/// child names it, which is the three branches of the protocol in one case.
/// The assertion is that the value at each node is its depth, which only
/// holds if every child ran after its own parent.
fn binary_tree(workers: usize) {
    /// Upstream's `size_t L = 16`.
    const LEVELS: usize = 16;

    let executor = Executor::new(workers);
    let data = results(1 << LEVELS);

    let mut parents: Vec<AsyncTask> = Vec::new();
    let mut task_id = 1;
    for level in 0..LEVELS {
        let mut children = Vec::with_capacity(1 << level);
        for node in 0..(1_usize << level) {
            let id = task_id;
            if id == 1 {
                children.push(executor.silent_dependent_async(
                    {
                        let data = Arc::clone(&data);
                        move || write(&data, id, 1)
                    },
                    [],
                ));
            } else {
                // Upstream's `.first`: the handle on the task, with the
                // handle on its result dropped -- which detaches it, since
                // `wait_for_all` is what this case waits on.
                let (task, _value) = executor.dependent_async(
                    {
                        let data = Arc::clone(&data);
                        move || write(&data, id, read(&data, id / 2) + 1)
                    },
                    [&parents[node / 2]],
                );
                children.push(task);
            }
            task_id += 1;
        }
        parents = children;
    }

    executor.wait_for_all();

    let mut task_id = 1;
    for level in 0..LEVELS {
        for _ in 0..(1_usize << level) {
            assert_eq!(
                read(&data, task_id),
                index(level) + 1,
                "{workers} workers, task {task_id}"
            );
            task_id += 1;
        }
    }
}

/// Upstream's `complete_linear_chain(unsigned W)`: two thousand tasks, each
/// naming **every** task submitted before it -- `N choose 2` dependencies,
/// about two million of them -- and two executors doing it at once.
///
/// What it covers is the arithmetic rather than the order: a task that names
/// a thousand dependencies of which nine hundred have already finished has to
/// count exactly nine hundred off and wait for exactly a hundred, and the
/// chain's value is what shows it did. Two executors because upstream uses
/// two, which is the one case here that puts two pools' sentinel-run
/// bookkeeping side by side -- the sentinel is one per process, not one per
/// executor.
fn complete_linear_chain(workers: usize) {
    /// Upstream's `int N = 2000`.
    const CHAIN: usize = 2000;

    let first_executor = Executor::new(workers);
    let second_executor = Executor::new(workers);
    let values = results(2 * CHAIN);

    let mut tasks: Vec<AsyncTask> = Vec::new();
    for step in 0..CHAIN {
        let task = first_executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || {
                    let value = if step == 0 {
                        1
                    } else {
                        read(&values, step - 1) + index(step)
                    };
                    write(&values, step, value);
                }
            },
            tasks.iter(),
        );
        tasks.push(task);
    }

    first_executor.wait_for_all();

    assert_eq!(read(&values, 0), 1, "{workers} workers");
    for step in 1..CHAIN {
        assert_eq!(
            read(&values, step),
            read(&values, step - 1) + index(step),
            "{workers} workers, step {step}"
        );
    }

    // The second executor, whose body upstream writes as one expression for
    // every step including the first -- so step 0 reads `results[N-1]`, the
    // last result the first half wrote, and the assertion that follows is
    // `results[N] == results[N-1]` because it adds zero to it.
    let mut tasks: Vec<AsyncTask> = Vec::new();
    for step in 0..CHAIN {
        let task = second_executor.silent_dependent_async(
            {
                let values = Arc::clone(&values);
                move || {
                    let previous = read(&values, step + CHAIN - 1);
                    write(&values, step + CHAIN, previous + index(step));
                }
            },
            tasks.iter(),
        );
        tasks.push(task);
    }

    second_executor.wait_for_all();

    assert_eq!(
        read(&values, CHAIN),
        read(&values, CHAIN - 1),
        "{workers} workers"
    );
    for step in 1..CHAIN {
        assert_eq!(
            read(&values, step + CHAIN),
            read(&values, step + CHAIN - 1) + index(step),
            "{workers} workers, step {step}"
        );
    }
}

/// Upstream's `parallel_graph_construction(unsigned W)`: a Pascal diagram of
/// 125250 async tasks whose **dependencies are named by several workers at
/// once**, each from a task of a running taskflow.
///
/// ```text
///  0 1 2 3
///  |/|/| /
///  4 5 6
///  |/| /
///  7 8
///  |/
///  9
/// ```
///
/// The taskflow's own edges are what make it safe to read a handle another
/// worker wrote: the graph task that submits node `id` succeeds the graph
/// tasks that submitted its two parents, so their handles are in place by the
/// time it looks. That is the case's point -- the protocol has to work when
/// the thread naming a dependency is not the thread that created it.
///
/// Two adaptations, both of them the module docs' general ones made concrete:
/// the handles live in `OnceLock`s because several workers fill the vector at
/// once, and the ordering assertion upstream makes *inside* each async body
/// counts a mismatch instead, since a panic there would be kept on the node
/// and never reach the test thread.
fn parallel_graph_construction(workers: usize) {
    /// Upstream's `int L = 500`.
    const LEVELS: usize = 500;

    let count = (1 + LEVELS) * LEVELS / 2;
    let executor = Arc::new(Executor::new(workers));
    let data = filled(count, -1);
    let submitted: Arc<Vec<OnceLock<AsyncTask>>> =
        Arc::new((0..count).map(|_| OnceLock::new()).collect());
    let mismatches = Arc::new(AtomicUsize::new(0));

    let mut taskflow = Taskflow::new();
    let mut tasks: Vec<Task> = Vec::with_capacity(count);
    let mut id: usize = 0;
    for level in (1..=LEVELS).rev() {
        for _ in 0..level {
            let right = id.checked_sub(level);
            let left = id.checked_sub(level + 1);

            let task = taskflow.emplace({
                let executor = Arc::clone(&executor);
                let data = Arc::clone(&data);
                let submitted = Arc::clone(&submitted);
                let mismatches = Arc::clone(&mismatches);
                move || {
                    let task = match (left, right) {
                        // Upstream's `REQUIRE(async_tasks[pl].empty() ==
                        // false)`: this body runs after the two that
                        // submitted its parents, so both handles are there.
                        // A panic here is a task of the *graph* failing and
                        // is reported from the run.
                        (Some(left), Some(right)) => {
                            let left_task = submitted[left]
                                .get()
                                .expect("the left parent was submitted");
                            let right_task = submitted[right]
                                .get()
                                .expect("the right parent was submitted");
                            assert!(!left_task.empty() && !right_task.empty());

                            executor.silent_dependent_async(
                                {
                                    let data = Arc::clone(&data);
                                    let mismatches = Arc::clone(&mismatches);
                                    move || {
                                        // Upstream's two `REQUIRE`s inside the
                                        // async body, counted rather than
                                        // raised: a silent task's panic is
                                        // kept on its own node, so raising it
                                        // here would fail nothing.
                                        if read(&data, left) != index(left)
                                            || read(&data, right) != index(right)
                                        {
                                            mismatches.fetch_add(1, Ordering::Relaxed);
                                        }
                                        write(&data, id, index(id));
                                    }
                                },
                                [left_task, right_task],
                            )
                        }
                        _ => executor.silent_dependent_async(
                            {
                                let data = Arc::clone(&data);
                                move || write(&data, id, index(id))
                            },
                            [],
                        ),
                    };
                    submitted[id]
                        .set(task)
                        .expect("one submission per node of the diagram");
                }
            });

            tasks.push(task);
            if let Some(right) = right {
                taskflow.task(tasks[id]).succeed([tasks[right]]);
            }
            if let Some(left) = left {
                taskflow.task(tasks[id]).succeed([tasks[left]]);
            }
            id += 1;
        }
    }

    executor
        .run(&taskflow)
        .wait()
        .expect("no task of the graph panicked");
    executor.wait_for_all();

    assert_eq!(
        mismatches.load(Ordering::Relaxed),
        0,
        "{workers} workers: every async task ran after both of its parents"
    );
    for node in 0..count {
        assert_eq!(read(&data, node), index(node), "{workers} workers");
    }
}

/// Upstream's `iterative_fibonacci(unsigned W)`: 93 tasks, each naming the
/// two before it, and each **waited for before the next is submitted**.
///
/// So this is the protocol's narrow case rather than its wide one: every
/// dependency named has already finished, which is the branch that counts the
/// dependency off at submission and must therefore schedule the task itself.
/// A task left waiting for a finished predecessor would hang here rather than
/// return a wrong number, which is what the 93 handles rule out one at a
/// time.
fn iterative_fibonacci(workers: usize) {
    let executor = Executor::new(workers);

    let mut tasks: Vec<AsyncTask> = Vec::new();
    let mut previous = 0;
    let mut before_that = 0;

    for step in 0..=92 {
        let (task, value) = if step < 2 {
            let value = u64::try_from(step).expect("0 or 1");
            executor.dependent_async(move || value, [])
        } else {
            executor.dependent_async(
                move || before_that + previous,
                [&tasks[step - 1], &tasks[step - 2]],
            )
        };

        tasks.push(task);
        before_that = previous;
        previous = value.wait().expect("no body panicked");

        assert_eq!(previous, FIBONACCI[step], "{workers} workers, step {step}");
    }
}

/// Upstream's `recursive_fibonacci(unsigned W)`: `fib(n)` as two
/// dependency-free tasks and a `corun_until` that waits for both.
///
/// The one case here that **cannot** be made to work by blocking: at one
/// worker, the thread inside `fib` is the only thread there is, so a `wait`
/// on either handle would deadlock. `corun_until` is what keeps that thread
/// in the work-stealing loop until its own children are done, and
/// `AsyncTask::is_done` is the predicate -- upstream's own, verbatim.
fn recursive_fibonacci(workers: usize) {
    /// Upstream's `std::function<int(int)> fib`, which recurses through the
    /// executor: two tasks per level, each of which runs this again.
    fn fib(executor: &Arc<Executor>, n: u64) -> u64 {
        if n < 2 {
            return n;
        }

        let (first, first_value) = executor.dependent_async(
            {
                let executor = Arc::clone(executor);
                move || fib(&executor, n - 1)
            },
            [],
        );
        let (second, second_value) = executor.dependent_async(
            {
                let executor = Arc::clone(executor);
                move || fib(&executor, n - 2)
            },
            [],
        );

        executor
            .corun_until(|| first.is_done() && second.is_done())
            .expect("a task body runs on a worker of its own executor");

        first_value.wait().expect("no body panicked")
            + second_value.wait().expect("no body panicked")
    }

    let executor = Arc::new(Executor::new(workers));

    // Upstream's `for(int i = 0; i <= 11; ++i)`, over the table rather than
    // over the indices so that the bound and the value come from one place.
    for (step, expected) in FIBONACCI.iter().enumerate().take(12) {
        let (_task, value) = executor.dependent_async(
            {
                let executor = Arc::clone(&executor);
                move || fib(&executor, u64::try_from(step).expect("at most 11"))
            },
            [],
        );
        assert_eq!(
            value.wait().expect("no body panicked"),
            *expected,
            "{workers} workers, step {step}"
        );
    }
}

worker_matrix! {
    embarrassing_parallelism {
        embarrassing_parallelism_1_thread(1),
        embarrassing_parallelism_2_threads(2),
        embarrassing_parallelism_4_threads(4),
        embarrassing_parallelism_8_threads(8),
        embarrassing_parallelism_16_threads(16),
    }
}

worker_matrix! {
    linear_chain {
        silent_dependent_async_linear_chain_1_thread(1),
        silent_dependent_async_linear_chain_2_threads(2),
        silent_dependent_async_linear_chain_4_threads(4),
        silent_dependent_async_linear_chain_8_threads(8),
        silent_dependent_async_linear_chain_16_threads(16),
    }
}

worker_matrix! {
    simple_graph {
        silent_dependent_async_simple_graph_1_thread(1),
        silent_dependent_async_simple_graph_2_threads(2),
        silent_dependent_async_simple_graph_4_threads(4),
        silent_dependent_async_simple_graph_8_threads(8),
        silent_dependent_async_simple_graph_16_threads(16),
    }
}

worker_matrix! {
    simple_graph_2 {
        simple_graph_2_1_thread(1),
        simple_graph_2_2_threads(2),
        simple_graph_2_4_threads(4),
        simple_graph_2_8_threads(8),
        simple_graph_2_16_threads(16),
    }
}

worker_matrix! {
    complex_graph_rounds {
        complex_graph_1_thread(1),
        complex_graph_2_threads(2),
        complex_graph_4_threads(4),
        complex_graph_8_threads(8),
        complex_graph_16_threads(16),
    }
}

worker_matrix! {
    complex_graph_from_worker {
        complex_graph_from_worker_2_threads(2, 1),
        complex_graph_from_worker_4_threads(4, 3),
        complex_graph_from_worker_8_threads(8, 7),
        complex_graph_from_worker_16_threads(16, 10),
    }
}

worker_matrix! {
    binary_tree {
        binary_tree_1_thread(1),
        binary_tree_2_threads(2),
        binary_tree_3_threads(3),
        binary_tree_4_threads(4),
        binary_tree_8_threads(8),
        binary_tree_16_threads(16),
    }
}

worker_matrix! {
    complete_linear_chain {
        complete_linear_chain_1_thread(1),
        complete_linear_chain_2_threads(2),
        complete_linear_chain_4_threads(4),
        complete_linear_chain_8_threads(8),
        complete_linear_chain_16_threads(16),
    }
}

worker_matrix! {
    parallel_graph_construction {
        parallel_graph_construction_1_thread(1),
        parallel_graph_construction_2_threads(2),
        parallel_graph_construction_4_threads(4),
        parallel_graph_construction_8_threads(8),
        parallel_graph_construction_16_threads(16),
    }
}

worker_matrix! {
    iterative_fibonacci {
        iterative_fibonacci_1_thread(1),
        iterative_fibonacci_2_threads(2),
        iterative_fibonacci_4_threads(4),
        iterative_fibonacci_8_threads(8),
    }
}

worker_matrix! {
    recursive_fibonacci {
        recursive_fibonacci_1_thread(1),
        recursive_fibonacci_2_threads(2),
        recursive_fibonacci_4_threads(4),
        recursive_fibonacci_8_threads(8),
    }
}

/// Upstream's `silent_dependent_async_from_runtime(unsigned)` and
/// `dependent_async_from_runtime(unsigned)`, which differ only in whether the
/// chain is built with the handle-returning form.
///
/// A chain of a thousand and one links, built **from inside a runtime body**
/// and left to that body's implicit join: each link names the one before it,
/// so the asserted order is the dependency protocol's, and nothing waits for
/// any of them -- what makes the run's `wait` enough is that every link is
/// counted against the runtime task's own node.
///
/// Upstream alternates the body shape along the chain, odd links taking a
/// `tf::Runtime&` and even links taking nothing, and that is the half worth
/// keeping: the node kind decides the protocol -- a dependent-async node
/// releases its successors when it finishes -- while the body shape decides
/// only what the body is handed, so a chain that alternates pins that the two
/// are independent.
///
/// `counter` is an atomic where upstream shares a plain `int`, for
/// [`Results`]' reason: the chain serialises the accesses, so `Relaxed` is
/// right and the ordering claim is the chain's rather than the word's.
fn chain_from_runtime(workers: usize, handles: bool) {
    /// Upstream's `for(size_t i=1; i<=1000; ++i)`.
    const LINKS: usize = 1000;

    let executor = Executor::new(workers);
    let counter = Arc::new(AtomicUsize::new(0));

    let mut taskflow = Taskflow::new();
    taskflow.emplace_runtime({
        let counter = Arc::clone(&counter);
        move |runtime: &mut Runtime<'_>| {
            /// One link's body: assert that every link before it has run,
            /// and count itself.
            fn link(counter: &Arc<AtomicUsize>, step: usize) -> impl FnOnce() + Send + 'static {
                let counter = Arc::clone(counter);
                move || {
                    assert_eq!(
                        counter.fetch_add(1, Ordering::Relaxed),
                        step,
                        "a link ran before the link it depends on"
                    );
                }
            }

            let mut previous = if handles {
                let (task, _value) = runtime.dependent_async(link(&counter, 0), []);
                task
            } else {
                runtime.silent_dependent_async(link(&counter, 0), [])
            };

            for step in 1..=LINKS {
                let body = link(&counter, step);
                previous = match (handles, step % 2) {
                    // Upstream's odd links: a runtime-shaped body, whose
                    // `Runtime` it never uses.
                    (true, 1) => {
                        let (task, _value) = runtime.dependent_async_runtime(
                            move |_: &mut Runtime<'_>| body(),
                            [&previous],
                        );
                        task
                    }
                    (true, _) => {
                        let (task, _value) = runtime.dependent_async(body, [&previous]);
                        task
                    }
                    (false, 1) => runtime.silent_dependent_async_runtime(
                        move |_: &mut Runtime<'_>| body(),
                        [&previous],
                    ),
                    (false, _) => runtime.silent_dependent_async(body, [&previous]),
                };
            }
            // No corun: the implicit join at the end of this body is what the
            // run's own `wait` below is waiting through.
        }
    });

    executor.run(&taskflow).wait().expect("no task panicked");

    assert_eq!(
        counter.load(Ordering::Relaxed),
        LINKS + 1,
        "{workers} workers: every link of the chain ran, in order"
    );
}

worker_matrix! {
    chain_from_runtime {
        silent_dependent_async_from_runtime_1_thread(1, false),
        silent_dependent_async_from_runtime_2_threads(2, false),
        silent_dependent_async_from_runtime_3_threads(3, false),
        silent_dependent_async_from_runtime_4_threads(4, false),
        dependent_async_from_runtime_1_thread(1, true),
        dependent_async_from_runtime_2_threads(2, true),
        dependent_async_from_runtime_3_threads(3, true),
        dependent_async_from_runtime_4_threads(4, true),
    }
}

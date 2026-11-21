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

use std::collections::BTreeSet;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use rustflow::algorithm::module::make_module_task;
use rustflow::observer::{Observer, TaskView, WorkerView};
use rustflow::prelude::*;
use rustflow::{AsyncTask, AsyncWork, Executor};

// The matrix macro, and the payload check the panic-path case at the end of
// the file makes. Not `common/counters.rs`: what the cases here record is a
// per-task *value* rather than a count -- the `Results` array below -- and
// the three counters that are counts are read with the array in the same
// assertion, so that topic would carry a third of them.
#[macro_use]
mod common;
#[path = "common/panics.rs"]
mod panics;

use panics::assert_panicked_with;

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
///
/// The silent half is **named**, as upstream's
/// `TaskParams{std::to_string(i)}` names it, and the other half is left
/// unnamed, which is its `DefaultTaskParams{}`. Nothing here reads a name
/// back -- [`names_reach_an_observer`] is what does -- so what the two halves
/// pin at this scale is that naming a submission costs it nothing else.
fn embarrassing_parallelism(workers: usize) {
    let executor = Executor::new(workers);
    let counter = Arc::new(AtomicUsize::new(0));

    for task in 0..TASKS / 2 {
        let counter = Arc::clone(&counter);
        executor.silent_dependent_async_named(
            task.to_string(),
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

/// Records the name of every task a worker enters, as a *set*.
///
/// `tests/asyncs.rs` carries the same observer for the free forms, and for
/// its reason: which names were seen rather than how many times each was,
/// a runtime-shaped node being entered a second time to report its outcome.
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

/// Invented: the name a dependent submission gives its node, read back the
/// one way a caller can read it -- an observer's `on_entry` -- over all four
/// of the executor's dependent forms, against a submission that names
/// nothing.
///
/// Issue #258's bar, and the dependent half of `tests/asyncs.rs`'s case of
/// the same name. Each receiver's four named links are a *chain*, so three of
/// every four names is read off a node that was queued by the tear-down of
/// its predecessor rather than by the submitting thread: the name is set at
/// construction, before any dependency can release the node, which is the one
/// ordering question the dependent path adds. One worker, because the question
/// is which name is on which node rather than who ran it.
#[test]
fn names_reach_an_observer() {
    /// The four dependent forms of one receiver, as a chain of four names.
    ///
    /// A macro rather than a function because the four methods are the same
    /// four names on three unrelated types -- `Executor`, `Runtime` and
    /// `TaskGroup` -- and nothing in the crate's public surface relates them.
    macro_rules! chain {
        ($receiver:expr, $who:literal) => {{
            let first = $receiver.silent_dependent_async_named(concat!($who, "silent"), || {}, []);
            let (second, value) =
                $receiver.dependent_async_named(concat!($who, "handled"), || {}, [&first]);
            let third = $receiver.silent_dependent_async_runtime_named(
                concat!($who, "silent runtime"),
                |_: &mut Runtime<'_>| {},
                [&second],
            );
            let (_fourth, done) = $receiver.dependent_async_runtime_named(
                concat!($who, "runtime"),
                |_: &mut Runtime<'_>| {},
                [&third],
            );
            (value, done)
        }};
    }

    let executor = Arc::new(Executor::new(1));
    let seen = executor.make_observer(Names::default());

    // The executor's own four, parented to nothing.
    let (value, done) = chain!(executor, "");

    // The default, which this issue did not change: upstream's
    // `DefaultTaskParams{}` leaves `_name` default-constructed.
    let _unnamed = executor.silent_dependent_async(|| {}, []);

    value.wait().expect("no body panicked");
    done.wait().expect("nothing under the last link failed");

    // A runtime body's four, counted against the node the body is of and
    // waited for by its implicit join.
    executor
        .spawn_runtime(|runtime: &mut Runtime<'_>| {
            let _handles = chain!(runtime, "runtime's ");
        })
        .wait()
        .expect("nothing under the body panicked");

    // And a task group's four, counted against the group's anchor. A group
    // whose outstanding children are a dependent chain is corun to the end of
    // it rather than left, for the reason `src/task_group.rs` gives.
    executor
        .spawn({
            let executor = Arc::clone(&executor);
            move || {
                let group = executor.task_group().expect("a worker may open a group");
                let _handles = chain!(group, "group's ");
                group.corun();
            }
        })
        .wait()
        .expect("no task of the group panicked");

    executor.wait_for_all();
    assert_eq!(
        *seen.0.lock().expect("no body panicked"),
        BTreeSet::from([
            String::new(),
            "group's handled".to_string(),
            "group's runtime".to_string(),
            "group's silent".to_string(),
            "group's silent runtime".to_string(),
            "handled".to_string(),
            "runtime".to_string(),
            "runtime's handled".to_string(),
            "runtime's runtime".to_string(),
            "runtime's silent".to_string(),
            "runtime's silent runtime".to_string(),
            "silent".to_string(),
            "silent runtime".to_string(),
        ])
    );
}

/// Pushes `label` onto a chain's shared order log, which is what every link
/// of [`an_unnamed_dependent_chain_joins_each_link_with_what_it_spawned`]
/// records in place of asserting where it runs.
fn push(order: &Arc<Mutex<Vec<&'static str>>>, label: &'static str) {
    order
        .lock()
        .expect("the order log's lock is never held across a body")
        .push(label);
}

/// Invented, as the unnamed counterpart of [`names_reach_an_observer`]'s
/// chain: the four dependent forms a *nested* receiver has without the name
/// parameter -- a `Runtime` body's and a `TaskGroup`'s, the unnamed forms on
/// the `Executor` itself being driven all over this file.
///
/// Upstream's `FromRuntime` cases are the chain this translates, and
/// [`chain_from_runtime`] above is their bulk form. What this adds is the one
/// claim that form cannot make, its runtime-shaped links never using the
/// `Runtime` they are handed: a runtime-shaped dependent link is joined with
/// everything *it* spawned before the next link is released, so each child
/// below lands between its own link and the one after it. Both chains record
/// into a log rather than asserting in place, for the reason this file's own
/// docs give -- a silent body's panic stays on its node, so an assertion
/// inside one is not a failing test.
#[test]
fn an_unnamed_dependent_chain_joins_each_link_with_what_it_spawned() {
    /// What one receiver's chain records, in order.
    const EXPECTED: [&str; 6] = [
        "silent",
        "handled",
        "silent runtime",
        "silent runtime's child",
        "runtime",
        "runtime's child",
    ];

    /// One receiver's four unnamed dependent forms as a chain, of which the
    /// last two spawn a child each out of the `Runtime` they are handed.
    ///
    /// A macro for [`names_reach_an_observer`]'s reason: the same four names
    /// on two unrelated types, with nothing in the public surface relating
    /// them.
    macro_rules! unnamed_chain {
        ($nested:expr, $order:expr) => {{
            let first = $nested.silent_dependent_async(
                {
                    let order = Arc::clone($order);
                    move || push(&order, "silent")
                },
                [],
            );
            let (second, value) = $nested.dependent_async(
                {
                    let order = Arc::clone($order);
                    move || {
                        push(&order, "handled");
                        7_usize
                    }
                },
                [&first],
            );
            let third = $nested.silent_dependent_async_runtime(
                {
                    let order = Arc::clone($order);
                    move |spawner: &mut Runtime<'_>| {
                        push(&order, "silent runtime");
                        let order = Arc::clone(&order);
                        spawner.silent_async(move || push(&order, "silent runtime's child"));
                    }
                },
                [&second],
            );
            let (_fourth, done) = $nested.dependent_async_runtime(
                {
                    let order = Arc::clone($order);
                    move |spawner: &mut Runtime<'_>| {
                        push(&order, "runtime");
                        let order = Arc::clone(&order);
                        spawner.silent_async(move || push(&order, "runtime's child"));
                    }
                },
                [&third],
            );
            (value, done)
        }};
    }

    let executor = Arc::new(Executor::new(4));

    // A runtime body's four, counted against the node the body is of.
    let from_runtime = Arc::new(Mutex::new(Vec::new()));
    executor
        .spawn_runtime({
            let from_runtime = Arc::clone(&from_runtime);
            move |runtime: &mut Runtime<'_>| {
                let (value, done) = unnamed_chain!(runtime, &from_runtime);
                // Every link and both children are done once this returns,
                // so neither handle below has anything left to wait for.
                runtime.corun();
                assert_eq!(value.wait().expect("no link panicked"), 7);
                done.wait().expect("nothing under the last link failed");
            }
        })
        .wait()
        .expect("nothing under the body failed");

    // And a task group's four, counted against the group's anchor and corun
    // to the end for the reason `src/task_group.rs` gives.
    let from_group = Arc::new(Mutex::new(Vec::new()));
    executor
        .spawn({
            let executor = Arc::clone(&executor);
            let from_group = Arc::clone(&from_group);
            move || {
                let group = executor.task_group().expect("a worker may open a group");
                let (value, done) = unnamed_chain!(group, &from_group);
                group.corun();
                assert_eq!(value.wait().expect("no link panicked"), 7);
                done.wait().expect("nothing under the last link failed");
            }
        })
        .wait()
        .expect("no task of the group panicked");

    assert_eq!(
        *from_runtime
            .lock()
            .expect("the body that wrote it has returned"),
        EXPECTED,
        "the chain built from a runtime body ran in order"
    );
    assert_eq!(
        *from_group
            .lock()
            .expect("the body that wrote it has returned"),
        EXPECTED,
        "and so did the one built from a task group"
    );
}

// ----------------------------------------------------------------------------
// The panic-path cell: a module task submitted as a dependent-async task
// ----------------------------------------------------------------------------
//
// Not upstream's: `test_dependent_asyncs.cpp` has no failing body anywhere,
// and the handle a failure comes back through is this port's own shape --
// `AsyncHandle::wait` hands back a `Result` where upstream's `get()`
// rethrows.

/// A panic inside the taskflow a [`make_module_task`] body runs reaches the
/// handle of the dependent-async task that submitted it, rather than the
/// node's own panic slot or the thread that failed.
///
/// The whole chain between the two is what this pins: the failing task's
/// payload is recorded against its own graph, that graph is the nested one a
/// runtime task scheduled, and the entered runtime node is the implicit
/// anchor the walk stops at -- which is the node this task's handle is a
/// handle on. The dependency is there for the other half of the submission:
/// the body runs only once `first` is done, and a failure of it still
/// releases what was waiting on the node.
#[test]
fn a_panic_in_a_module_task_submitted_as_a_dependent_async_reaches_its_handle() {
    let executor = Executor::new(4);
    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));

    let mut target = Taskflow::new();
    let head = target.emplace({
        let before = Arc::clone(&before);
        move || {
            before.fetch_add(1, Ordering::Relaxed);
        }
    });
    let failing = target.emplace(|| panic!("x"));
    let tail = target.emplace({
        let after = Arc::clone(&after);
        move || {
            after.fetch_add(1, Ordering::Relaxed);
        }
    });
    target.linearize([head, failing, tail]);
    let target = Arc::new(target);

    let ran_first = Arc::new(AtomicUsize::new(0));
    let first = executor.silent_dependent_async(
        {
            let ran_first = Arc::clone(&ran_first);
            move || {
                ran_first.fetch_add(1, Ordering::Relaxed);
            }
        },
        [],
    );
    let (_module, done) = executor.dependent_async_runtime(make_module_task(target), [&first]);

    let error = done
        .wait()
        .expect_err("a task of the composed taskflow panicked");
    assert_panicked_with(error, "x");

    assert_eq!(ran_first.load(Ordering::Relaxed), 1, "the dependency ran");
    assert_eq!(
        before.load(Ordering::Relaxed),
        1,
        "the task before the failing one ran"
    );
    assert_eq!(
        after.load(Ordering::Relaxed),
        0,
        "the task after it was cancelled with the rest of the composition"
    );

    // The third claim: the executor is usable after the failed submission.
    let ran = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&ran);
    let mut taskflow = Taskflow::new();
    taskflow.emplace(move || {
        counted.fetch_add(1, Ordering::Relaxed);
    });
    executor
        .run(&taskflow)
        .wait()
        .expect("the executor is reusable after a failed submission");
    assert_eq!(ran.load(Ordering::Relaxed), 1);
}

// ----------------------------------------------------------------------------
// The handle's own share count
// ----------------------------------------------------------------------------

/// `AsyncTask::use_count` counts the shares of one task, and is `0` for an
/// empty handle -- upstream's own documented example, `tf::AsyncTask task;
/// assert(task.use_count() == 0);`, which its suite has no case for.
///
/// What the port adds to that claim is in the accessor's own docs: a task the
/// scheduler still names is counted too, so the count is read here once with
/// the pool still up, where the only safe claim is "at least the handle", and
/// once after the pool has come down, where the handles are the only owners
/// left and the arithmetic of a clone and a reset is exact.
#[test]
fn use_count_counts_the_shares_of_one_task() {
    let mut empty = AsyncTask::default();
    assert_eq!(empty.use_count(), 0, "upstream's own example");
    empty.reset();
    assert_eq!(empty.use_count(), 0, "a reset empty handle manages nothing");
    assert!(empty.empty());

    let mut task = {
        let executor = Executor::new(2);
        let (task, done) = executor.dependent_async(|| {}, []);
        done.wait().expect("the body did not panic");
        assert!(
            task.use_count() >= 1,
            "the handle is itself a share, and the scheduler may still hold one"
        );
        executor.wait_for_all();
        task
    };

    // The pool is down, so every share the scheduler held went with it.
    assert_eq!(task.use_count(), 1, "one handle, and nothing else");

    let clone = task.clone();
    assert_eq!(
        task.use_count(),
        2,
        "a clone takes a share of the same task"
    );
    assert_eq!(
        clone.use_count(),
        task.use_count(),
        "which both handles read the same way"
    );
    drop(clone);
    assert_eq!(task.use_count(), 1, "and gives it back when it is dropped");

    task.reset();
    assert_eq!(task.use_count(), 0, "a reset handle manages nothing");
    assert!(task.empty());
}

/// `AsyncTask::hash_value` is the identity of the task a handle names, so it
/// agrees with the handle's `Eq`: a clone of a handle hashes the same, two
/// empty handles are one identity, and a `HashSet` of handles counts tasks
/// rather than handles. Upstream's `async_task.hpp` documents the accessor
/// with an example that only prints the number, and its suite has no case
/// for it at all.
///
/// Read with the pool down, as [`use_count_counts_the_shares_of_one_task`]
/// above is: while the executor still holds shares it may free a node whose
/// address another task is then allocated at, which is a property of
/// upstream's `std::hash<Node*>` too and not one a test may race with.
#[test]
fn hash_value_is_the_identity_of_the_task_a_handle_names() {
    let empty = AsyncTask::default();
    assert_eq!(
        empty.hash_value(),
        AsyncTask::default().hash_value(),
        "two empty handles are one identity"
    );

    let (first, second) = {
        let executor = Executor::new(2);
        let (first, done_first) = executor.dependent_async(|| {}, []);
        let (second, done_second) = executor.dependent_async(|| {}, []);
        done_first.wait().expect("the body did not panic");
        done_second.wait().expect("the body did not panic");
        executor.wait_for_all();
        (first, second)
    };

    // Consistent with `Eq`, which is the one promise a hash of an address can
    // keep: equal handles hash equal, so a map keyed on handles finds a clone
    // of a key it was given.
    let clone = first.clone();
    assert_eq!(first, clone, "a clone is a share of the same task");
    assert_eq!(first.hash_value(), clone.hash_value());

    // Distinct tasks: a hash of an address may in principle collide, but then
    // the handles must still compare unequal -- `basics.rs`'s `hash` makes the
    // same reservation for `TaskRef::hash_value`.
    assert_ne!(first, second, "two submissions are two tasks");
    assert!(first.hash_value() != second.hash_value() || first != second);

    let mut released = first.clone();
    assert_eq!(released.hash_value(), first.hash_value());
    released.reset();
    assert_eq!(
        released.hash_value(),
        empty.hash_value(),
        "a reset handle is an empty one, and hashes as one"
    );
}

/// `AsyncTask::take_panic` is where a **silent** task's own failure is
/// parked: upstream's `exception_ptr()`. A silent task has no future to raise
/// through -- `exceptions.rs` calls that upstream's "silently caught by the
/// task" -- so the handle is the only way back to the payload, and it hands
/// it over exactly once.
///
/// Upstream's suite reaches `exception_ptr()` from no case of its own; what
/// is asserted here is the accessor's own documented contract, over the one
/// shape that can fill the slot.
#[test]
fn take_panic_hands_a_silent_tasks_own_failure_over_once() {
    let executor = Executor::new(2);

    let clean = executor.silent_dependent_async(|| {}, []);
    let failed = executor.silent_dependent_async(|| panic!("x"), []);
    executor.wait_for_all();

    assert!(clean.is_done() && failed.is_done());
    assert!(
        clean.take_panic().is_none(),
        "a task that did not fail parked nothing"
    );
    assert!(
        AsyncTask::default().take_panic().is_none(),
        "and an empty handle names no slot at all"
    );

    let payload = failed
        .take_panic()
        .expect("the silent task's own failure, which nothing else collected");
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&"x"),
        "the payload keeps its identity"
    );
    assert!(
        failed.take_panic().is_none(),
        "taken rather than peeked: a second caller gets nothing"
    );
}

// ----------------------------------------------------------------------------
// What the handle prints
// ----------------------------------------------------------------------------

/// `AsyncTask`'s `Debug` is hand-written so that a task's own captures never
/// reach the output: what it prints is the node's address, whether the task
/// has finished and how many shares of it are outstanding, and an empty
/// handle prints no node at all. Upstream's handle has no `operator<<` for
/// this to translate -- but a handle that printed its captures would put a
/// caller's data in every log line that formatted one.
///
/// The unfinished reading is made deterministic by a barrier the body parks
/// on: the body cannot be past it until this thread arrives there too, so
/// `done: false` is a fact rather than a race. The finished reading is taken
/// with the pool down, where the handle is the only owner left, exactly as
/// [`use_count_counts_the_shares_of_one_task`] above takes its own.
#[test]
fn the_debug_output_names_the_node_and_never_a_captured_value() {
    assert_eq!(
        format!("{:?}", AsyncTask::default()),
        "AsyncTask { node: None }",
        "an empty handle names no node, so it has nothing else to print"
    );

    let gate = Arc::new(Barrier::new(2));
    let task = {
        let executor = Executor::new(2);
        let secret = String::from("a-captured-secret");
        let parked = Arc::clone(&gate);
        let (task, done) = executor.dependent_async(
            move || {
                assert_eq!(secret, "a-captured-secret", "the body got its capture");
                parked.wait();
            },
            [],
        );

        // The body is parked on the barrier until this thread arrives, so the
        // task stays unfinished for as long as the reading takes.
        let running = format!("{task:?}");
        assert!(
            running.starts_with("AsyncTask { node: 0x"),
            "a live handle prints the node it names, by address: {running}"
        );
        assert!(
            running.contains("done: false"),
            "the body has not returned yet: {running}"
        );
        assert!(
            !running.contains("secret"),
            "a body's captures are what this impl exists to keep out: {running}"
        );

        gate.wait();
        done.wait().expect("the body did not panic");
        executor.wait_for_all();
        task
    };

    let finished = format!("{task:?}");
    assert!(
        finished.contains("done: true"),
        "the body returned before the pool came down: {finished}"
    );
    assert!(
        finished.contains("use_count: 1"),
        "the pool is down, so this handle is the only share left: {finished}"
    );
}

// ----------------------------------------------------------------------------
// The panic path's other side: a graph-built handle a cancelled run skips
// ----------------------------------------------------------------------------

/// A `Handle::DependentAsync` built into a taskflow's graph and then *skipped*
/// by a cancelled run has nothing drained for it, and the worker that dequeued
/// the skipped job survives it.
///
/// The counterpart of `executor::dependent_async`'s own
/// `a_dependent_async_node_in_a_graph_refuses_the_run`, which dispatches such
/// a node where this one never gets that far. A successor list is a *free*
/// node's state, so the cancellation early-out asks both questions -- is this
/// a free node at all, and is it a dependent-async one -- before it drains
/// anything; an early-out that asked only the second would go looking for a
/// list a graph node does not have.
#[test]
fn a_graph_built_dependent_async_handle_is_skipped_with_nothing_drained() {
    let executor = Executor::new(1);

    let mut taskflow = Taskflow::new();
    let failing = taskflow.emplace(|| panic!("x"));
    let free = taskflow
        .graph_mut()
        .add_named_node("free", Handle::DependentAsync(AsyncWork::plain(|| {})));
    taskflow.task(failing).precede([free]);

    let error = executor
        .run(&taskflow)
        .wait()
        .expect_err("the first task panicked, so the run failed");
    assert_panicked_with(error, "x");

    // One worker, which is the worker that dequeued the skipped job: that the
    // run above returned at all is half the claim, and that this one does is
    // the other half.
    let ran = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&ran);
    let mut after = Taskflow::new();
    after.emplace(move || {
        counted.fetch_add(1, Ordering::Relaxed);
    });
    executor
        .run(&after)
        .wait()
        .expect("the executor is usable after the skipped job");
    assert_eq!(ran.load(Ordering::Relaxed), 1);
}

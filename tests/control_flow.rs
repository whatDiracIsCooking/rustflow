//! Translated from `unittests/test_control_flow.cpp` (Taskflow v4.1.0).
//!
//! Every case of that file is here, each at upstream's own sweep of worker
//! counts: `LoopCond` and `MultipleBranches` arrived with the
//! conditional-scheduling issue (#31), and `FlipCoinCond`, `CyclicCond`,
//! `BTreeCondition`, `Cond2Cond`, `MultipleLoops`, `MultiCondBinaryTree` and
//! `MultiCond.MultipleRuns` with issue #33, once there was a scheduler to
//! drive them. `MultipleBranches` keeps the four of its eight worker counts
//! that say something distinct, its fan-out being the same whether four
//! workers share it or eight.
//!
//! The last four arrived with #54, each because it needed a node kind this
//! port did not have before milestone 4 rather than because it was awkward:
//!
//! - `DynamicBTreeCondition`, `NestedCond` and `CondSubflow` build their
//!   graphs from inside a running task through a `tf::Subflow` (#50).
//! - `HierCondition` composes three taskflows with `composed_of`, one of them
//!   from inside a subflow, so it needs module tasks (#49) as well.
//!
//! None of the four was ever approximated by a static or condition graph of
//! the same shape while it waited: a control-flow case that reports green
//! while testing something else is worse than an absent one. Upstream's two
//! `Cond.Types`/`MultiCond.Types` cases are `static_assert`s on its callable
//! traits, and they are already ported, in `tests/flow_builder.rs` with the
//! rest of the builder's surface.
//!
//! Three standing adaptations, because the port is not quite where upstream is:
//!
//! - **A dropped handle is a waited-for run.** `loop_cond` ends with four bare
//!   `run(taskflow)` calls, a `run_n(taskflow, 10)` and a `wait_for_all()`,
//!   all of which are here; upstream leaves the four futures unobserved and
//!   lets its per-taskflow queue line the runs up behind each other, while a
//!   [`RunHandle`](rustflow::RunHandle) borrows its taskflow and blocks on
//!   drop, so each of those statements waits where upstream's merely queues.
//!   The observable is unchanged either way -- fifteen runs of a hundred
//!   iterations is `state == 1500`.
//! - **`::rand()` is not a dependency.** `multiple_branches` draws its branch
//!   set, and `flip_coin_cond` its coins, from the crate's own [`Xorshift32`]
//!   instead, seeded per worker count by [`seed_for`] and with the seed in
//!   every assertion message, so a failure reproduces exactly. The draws
//!   themselves are upstream's.
//! - **A `REQUIRE` inside a task body is a panic.** Upstream's assertions in
//!   task bodies are doctest's, which record a failure and carry on; here they
//!   are `assert!`s, whose panic the executor stores on the topology and
//!   reports from `wait`. [`run`] is what turns that back into a failed test,
//!   so a case whose graph asserts must go through it rather than dropping its
//!   handle -- and the three cases that end on `wait_for_all` the way upstream
//!   does hold their handles across it and drain them afterwards, so that the
//!   waiting is still `wait_for_all`'s and a failed body is still reported.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

use rustflow::prelude::*;
use rustflow::{Executor, RunError, Xorshift32};

// The matrix macro and the counting every case here asserts on.
// `common/bodies.rs` is not among them: a counting task is added by
// [`counted`] below, which hands its callers back the task they then order,
// where `bump` hands back a body. The `AtomicI64` loop counters of
// `loop_cond` and `hierarchical_loop` are upstream's own signed `int`s and
// are read where they are, since `count` is over the unsigned share.
#[macro_use]
mod common;
#[path = "common/counters.rs"]
mod counters;

use counters::{Counter, count, counter};

/// Adds a static task that does nothing but count its own invocations.
fn counted(taskflow: &mut Taskflow, counter: &Counter) -> Task {
    let counter = Arc::clone(counter);
    taskflow.emplace(move || {
        counter.fetch_add(1, Ordering::Relaxed);
    })
}

/// A seed per worker count, so that the two seeded cases below do not all draw
/// the same sequence and any one of their cases can be re-run on its own.
fn seed_for(workers: usize) -> u32 {
    0x5EED_0000 + u32::try_from(workers).expect("a small worker count")
}

/// Runs `taskflow` to completion, turning a panicking task body -- which is
/// what a failed assertion inside one is -- back into a failed test.
fn run(executor: &Executor, taskflow: &Taskflow) {
    executor
        .run(taskflow)
        .wait()
        .expect("a task of this taskflow panicked");
}

// ----------------------------------------------------------------------------
// Testcase: LoopCond
// ----------------------------------------------------------------------------

/// Upstream's `loop_cond(unsigned w)`.
///
/// `B` is a condition task that precedes *itself* and `C`: it returns 0 --
/// successor position 0, which is `B` -- a hundred times, then 1, which is
/// `C`. So the whole run is a single-node cycle driven a hundred times by a
/// weak edge, which is the shape that cannot terminate at all unless the
/// conditioner's branch is real: every other handle releases every successor.
fn loop_cond(workers: usize) {
    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();

    // Upstream's `int counter = -1; int state = 0;`, captured by reference
    // from three task bodies. A body here is `FnMut() + Send + 'static`, so
    // the two live in `Arc`s instead; relaxed is enough for the same reason
    // upstream can use plain `int`s, which is that the graph never has two of
    // these three nodes runnable at once and the queue a job travels through
    // is what publishes the writes.
    let counter = Arc::new(AtomicI64::new(-1));
    let state = Arc::new(AtomicI64::new(0));

    let a = {
        let counter = Arc::clone(&counter);
        taskflow.emplace(move || counter.store(0, Ordering::Relaxed))
    };
    let b = {
        let counter = Arc::clone(&counter);
        let state = Arc::clone(&state);
        taskflow.emplace_condition(move || {
            let counter = counter.fetch_add(1, Ordering::Relaxed) + 1;
            let state = state.fetch_add(1, Ordering::Relaxed) + 1;
            assert_eq!(
                counter % 100,
                state % 100,
                "the loop counter and the run-spanning state must stay in step"
            );
            i32::from(counter >= 100)
        })
    };
    let c = {
        let counter = Arc::clone(&counter);
        taskflow.emplace(move || {
            assert_eq!(
                counter.load(Ordering::Relaxed),
                100,
                "`C` runs only on the iteration that leaves the cycle"
            );
            counter.store(0, Ordering::Relaxed);
        })
    };

    taskflow.task(a).precede([b]);
    taskflow.task(b).precede([b, c]);

    // Upstream's `REQUIRE`s on the edge counts, which are what say the cycle
    // is wired the way the rest of this test assumes: `B`'s edge from itself
    // is weak and so cannot gate it, while its edge from `A` is strong.
    let a_ref = taskflow.task_ref(a);
    assert_eq!(a_ref.num_strong_dependencies(), 0);
    assert_eq!(a_ref.num_weak_dependencies(), 0);
    assert_eq!(a_ref.num_predecessors(), 0);

    let b_ref = taskflow.task_ref(b);
    assert_eq!(b_ref.num_strong_dependencies(), 1);
    assert_eq!(b_ref.num_weak_dependencies(), 1);
    assert_eq!(b_ref.num_predecessors(), 2);

    run(&executor, &taskflow);
    assert_eq!(counter.load(Ordering::Relaxed), 0, "`C` reset the counter");
    assert_eq!(state.load(Ordering::Relaxed), 100, "a hundred iterations");

    // Upstream's four bare `run`s, its `run_n(taskflow, 10)` and its
    // `wait_for_all`: fourteen more runs of a hundred iterations each, the
    // last ten of them re-arming the same topology through `Topology::set_up`
    // rather than starting a new one. A `run_n` re-run is what says the
    // counters a cycle drove to zero are reset between runs, since the tenth
    // run of the cycle has to behave exactly as the first did.
    for _ in 0..4 {
        drop(executor.run(&taskflow));
    }
    executor
        .run_n(&taskflow, 10)
        .wait()
        .expect("a task of this taskflow panicked");
    executor.wait_for_all();

    assert_eq!(
        state.load(Ordering::Relaxed),
        1500,
        "fifteen runs of a hundred iterations"
    );
}

worker_matrix! {
    loop_cond {
        loop_cond_1_worker(1),
        loop_cond_2_workers(2),
        loop_cond_3_workers(3),
        loop_cond_4_workers(4),
    }
}

// ----------------------------------------------------------------------------
// Testcase: FlipCoinCond
// ----------------------------------------------------------------------------

/// Upstream's `rounds`: how many times the coin-flip graph is run.
const ROUNDS: usize = 10_000;

/// Upstream's `std::rand() % 2`, drawn from the crate's own generator.
///
/// `0` advances the chain and `1` sends it back to the start, which is the
/// order every coin task below is wired in.
fn flip(rng: &mut Xorshift32) -> i32 {
    i32::from(!rng.next_u32().is_multiple_of(2))
}

/// Adds one of upstream's bare coin tasks: a conditioner that flips and
/// nothing else, with a generator of its own drawn from `seeds`.
fn coin(taskflow: &mut Taskflow, seeds: &mut Xorshift32) -> Task {
    let mut rng = Xorshift32::new(seeds.next_u32());
    taskflow.emplace_condition(move || flip(&mut rng))
}

/// Upstream's `flip_coin_cond(unsigned w)`.
///
/// Five conditioners in a chain, each of which either advances to the next or
/// sends the token back to `B`, and `B` counts the attempts. A round ends only
/// when all five coins come up 0 in a row, so the number of attempts per round
/// is geometric with mean 32, and the assertion is on the mean over ten
/// thousand rounds.
///
/// It is the one case in this file whose statistic could in principle fail on
/// a fair coin -- the band upstream asserts is about three standard errors
/// wide -- so the generators are seeded and the whole case is deterministic:
/// the graph holds a single token at every instant, so each conditioner draws
/// its own sequence in its own order whatever the worker count, and the total
/// below is a fixed function of `seed` alone.
///
/// Upstream's ten thousand rounds are kept, which is some 640000 task
/// invocations and makes this the heaviest case in the file by an order of
/// magnitude. It is still a fifth of a second, pinned to one core or not, so
/// it stays in the default set rather than earning an `#[ignore]`.
fn flip_coin_cond(workers: usize, seed: u32) {
    let mut taskflow = Taskflow::new();

    // Upstream's `steps`, reset by `A` once per round, and its `total_steps`,
    // accumulated by `G` across rounds.
    let steps = counter();
    let total_steps = counter();

    // One generator per coin task, all drawn from one seed rather than
    // hand-picked, so that no two of the five are accidentally in step.
    let mut seeds = Xorshift32::new(seed);

    let a = {
        let steps = Arc::clone(&steps);
        taskflow.emplace(move || steps.store(0, Ordering::Relaxed))
    };
    let b = {
        let steps = Arc::clone(&steps);
        let mut rng = Xorshift32::new(seeds.next_u32());
        taskflow.emplace_condition(move || {
            steps.fetch_add(1, Ordering::Relaxed);
            flip(&mut rng)
        })
    };
    let c = coin(&mut taskflow, &mut seeds);
    let d = coin(&mut taskflow, &mut seeds);
    let e = coin(&mut taskflow, &mut seeds);
    let f = coin(&mut taskflow, &mut seeds);
    let g = {
        let steps = Arc::clone(&steps);
        let total_steps = Arc::clone(&total_steps);
        taskflow.emplace(move || {
            total_steps.fetch_add(count(&steps), Ordering::Relaxed);
        })
    };

    taskflow.task(a).precede([b]);
    taskflow.task(b).precede([c, b]);
    taskflow.task(c).precede([d, b]);
    taskflow.task(d).precede([e, b]);
    taskflow.task(e).precede([f, b]);
    taskflow.task(f).precede([g, b]);

    let executor = Executor::new(workers);
    executor
        .run_n(&taskflow, ROUNDS)
        .wait()
        .expect("a task of this taskflow panicked");

    // Upstream's `REQUIRE(std::fabs(average_steps - 32.0) < 1.0)`, in
    // integers: the mean is within one of 32 exactly when the total is within
    // `ROUNDS` of `32 * ROUNDS`, and an integer comparison says so without
    // asking what a `usize` rounds to.
    let total = count(&total_steps);
    assert!(
        total.abs_diff(32 * ROUNDS) < ROUNDS,
        "seed {seed}, {workers} worker(s): {total} attempts over {ROUNDS} rounds \
         is not within one of 32 per round"
    );
}

worker_matrix! {
    flip_coin_cond {
        flip_coin_cond_1_worker(1, seed_for(1)),
        flip_coin_cond_2_workers(2, seed_for(2)),
        flip_coin_cond_3_workers(3, seed_for(3)),
        flip_coin_cond_4_workers(4, seed_for(4)),
    }
}

// ----------------------------------------------------------------------------
// Testcase: CyclicCond
// ----------------------------------------------------------------------------

/// Upstream's `total_iteration`: how many times the cycle goes round, and how
/// many branches the conditioner that drives it has.
const TOTAL_ITERATION: usize = 1000;

/// Upstream's `cyclic_cond(unsigned w)`.
///
/// ```text
///       ____________________
///       |                  |
///       v                  |
///  S -> A -> branch -> many branches -> T
/// ```
///
/// `branch` returns `sel++`, so iteration `i` selects the `i`th of its
/// thousand successors; each of those marks itself passed and goes back to `A`
/// until the iteration count is spent, when it goes to `T` instead. So the
/// property is a strong one: every branch is taken exactly once, in order, and
/// the single node that leaves the cycle is the thousandth.
fn cyclic_cond(workers: usize) {
    let executor = Executor::new(workers);
    let mut flow = Taskflow::new();

    let s = flow.emplace(|| {});

    let num_iterations = counter();
    let a = {
        let num_iterations = Arc::clone(&num_iterations);
        flow.emplace(move || {
            num_iterations.fetch_add(1, Ordering::Relaxed);
        })
    };
    flow.task(s).precede([a]);

    // Upstream's `std::vector<bool> pass`, one flag per branch, and its
    // `pass_T`.
    let pass: Arc<Vec<AtomicBool>> = Arc::new(
        (0..TOTAL_ITERATION)
            .map(|_| AtomicBool::new(false))
            .collect(),
    );
    let pass_t = Arc::new(AtomicBool::new(false));

    let t = {
        let num_iterations = Arc::clone(&num_iterations);
        let pass_t = Arc::clone(&pass_t);
        flow.emplace(move || {
            assert_eq!(
                count(&num_iterations),
                TOTAL_ITERATION,
                "`T` runs only after the cycle is spent"
            );
            pass_t.store(true, Ordering::Relaxed);
        })
    };
    // Upstream's `sel` is an `int` captured by reference; a conditioner's body
    // is `FnMut`, so the state it is the only reader and writer of lives in
    // the closure instead of in an `Arc`.
    let branch = {
        let mut sel = 0;
        flow.emplace_condition(move || {
            let selected = sel;
            sel += 1;
            selected
        })
    };
    flow.task(a).precede([branch]);

    for index in 0..TOTAL_ITERATION {
        let branch_task = {
            let num_iterations = Arc::clone(&num_iterations);
            let pass = Arc::clone(&pass);
            flow.emplace_condition(move || {
                assert!(
                    !pass[index].swap(true, Ordering::Relaxed),
                    "branch {index} was taken twice"
                );
                // 0 is `A`, which closes the cycle; 1 is `T`, which leaves it.
                i32::from(count(&num_iterations) >= TOTAL_ITERATION)
            })
        };
        flow.task(branch).precede([branch_task]);
        flow.task(branch_task).precede([a, t]);
    }

    run(&executor, &flow);

    assert!(
        pass_t.load(Ordering::Relaxed),
        "{workers} worker(s): the cycle never reached `T`"
    );
    for (index, passed) in pass.iter().enumerate() {
        assert!(
            passed.load(Ordering::Relaxed),
            "{workers} worker(s): branch {index} was never taken"
        );
    }
}

worker_matrix! {
    cyclic_cond {
        cyclic_cond_1_worker(1),
        cyclic_cond_2_workers(2),
        cyclic_cond_3_workers(3),
        cyclic_cond_4_workers(4),
        cyclic_cond_5_workers(5),
        cyclic_cond_6_workers(6),
        cyclic_cond_7_workers(7),
        cyclic_cond_8_workers(8),
    }
}

// ----------------------------------------------------------------------------
// Testcase: BTreeCondition
// ----------------------------------------------------------------------------

/// Upstream's `BTreeCondition`, for one worker count.
///
/// A complete binary tree of `level` levels whose levels alternate: an even
/// level counts, an odd one decides. A static task releases both its
/// successors, so each counted level doubles; a conditioner takes one of its
/// two -- 0 on even `j`, 1 on odd -- so each decided level does not. The
/// counted levels therefore see 1, 2, 4, ... tasks and the total is
/// `2^((level + 1) / 2) - 1`, which is upstream's assertion and is wrong by a
/// factor of two the moment a weak edge releases a successor it did not name.
///
/// Upstream writes this as a single case over both loops, `w` and `level`. It
/// is split per worker count here, as the rest of this file is, so that a
/// failure names its worker count without a message; the executor is hoisted
/// out of the `level` loop for the same reason upstream's is not -- it has
/// eight worker counts' worth of thread spawning to amortise and we have one.
fn btree_condition(workers: usize) {
    let executor = Executor::new(workers);

    for level in 1..12 {
        let mut flow = Taskflow::new();
        let counter = counter();
        let mut previous: Vec<Task> = Vec::new();

        for depth in 0..level {
            let mut tasks = Vec::new();
            for index in 0..(1_usize << depth) {
                if depth % 2 == 0 {
                    tasks.push(counted(&mut flow, &counter));
                } else if index % 2 == 0 {
                    tasks.push(flow.emplace_condition(|| 0));
                } else {
                    tasks.push(flow.emplace_condition(|| 1));
                }
            }

            for (index, &parent) in previous.iter().enumerate() {
                flow.task(parent)
                    .precede([tasks[2 * index], tasks[2 * index + 1]]);
            }
            previous = tasks;
        }

        run(&executor, &flow);

        assert_eq!(
            count(&counter),
            (1_usize << ((level + 1) / 2)) - 1,
            "{workers} worker(s), {level} level(s)"
        );
    }
}

worker_matrix! {
    btree_condition {
        btree_condition_1_worker(1),
        btree_condition_2_workers(2),
        btree_condition_3_workers(3),
        btree_condition_4_workers(4),
        btree_condition_5_workers(5),
        btree_condition_6_workers(6),
        btree_condition_7_workers(7),
        btree_condition_8_workers(8),
    }
}

// ----------------------------------------------------------------------------
// Testcase: DynamicBTreeCondition
// ----------------------------------------------------------------------------

/// Upstream's `conditional_spawn(counter, max_depth, depth, subflow)`: two
/// copies of `A -> cond -> {B, C}` per level, where `B` and `C` are subflow
/// tasks that spawn the next level and `cond` takes exactly one of them.
///
/// A free function, as upstream's is, because the two subflow bodies it
/// emplaces call it again with one more depth. Each level therefore runs
/// `2 * (1 + f(depth + 1))` counting tasks, which is the arithmetic behind the
/// case's one assertion.
fn conditional_spawn(counter: &Counter, max_depth: i32, depth: i32, subflow: &mut Subflow<'_>) {
    if depth >= max_depth {
        return;
    }

    for _ in 0..2 {
        let a = {
            let counter = Arc::clone(counter);
            subflow.emplace(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            })
        };
        let b = {
            let counter = Arc::clone(counter);
            subflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
                conditional_spawn(&counter, max_depth, depth + 1, subflow);
            })
        };
        let c = {
            let counter = Arc::clone(counter);
            subflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
                conditional_spawn(&counter, max_depth, depth + 1, subflow);
            })
        };

        // Upstream's `[depth](){ if(depth%2) return 1; else return 0; }`:
        // odd levels go right, even levels left, so the tree is a path per
        // branch rather than a doubling fan-out.
        let cond = subflow.emplace_condition(move || i32::from(depth % 2 != 0));
        subflow.task(cond).precede([b, c]);
        subflow.task(a).precede([cond]);
    }
}

/// Upstream's `DynamicBTreeCondition`, for one worker count.
///
/// One subflow task that counts itself and then spawns six levels of that
/// recursion, run four times. `2^(max_depth+1) - 1` bodies per run is
/// upstream's own count and it is what says both halves of the mechanism
/// work: a conditioner inside a subgraph releases one successor out of two,
/// and a subflow task inside a subgraph is joined by the level above it
/// before that level's node finishes. Either failing changes the total.
///
/// Upstream writes this as a single case over `w`; it is split per worker
/// count here, as the rest of this file is, so that a failure names its
/// worker count without a message.
fn dynamic_btree_condition(workers: usize) {
    const MAX_DEPTH: i32 = 6;
    const RUNS: usize = 4;

    let executor = Executor::new(workers);
    let counter = counter();

    let mut flow = Taskflow::new();
    flow.emplace_subflow({
        let counter = Arc::clone(&counter);
        move |subflow: &mut Subflow<'_>| {
            counter.fetch_add(1, Ordering::Relaxed);
            conditional_spawn(&counter, MAX_DEPTH, 0, subflow);
        }
    });

    executor
        .run_n(&flow, RUNS)
        .wait()
        .expect("a task of this taskflow panicked");

    assert_eq!(
        count(&counter),
        ((1_usize << (MAX_DEPTH + 1)) - 1) * RUNS,
        "{workers} worker(s)"
    );
}

worker_matrix! {
    dynamic_btree_condition {
        dynamic_btree_condition_1_worker(1),
        dynamic_btree_condition_2_workers(2),
        dynamic_btree_condition_3_workers(3),
        dynamic_btree_condition_4_workers(4),
        dynamic_btree_condition_5_workers(5),
        dynamic_btree_condition_6_workers(6),
        dynamic_btree_condition_7_workers(7),
        dynamic_btree_condition_8_workers(8),
    }
}

// ----------------------------------------------------------------------------
// Testcase: NestedCond
// ----------------------------------------------------------------------------

/// Upstream's `[&, repeat=0]() mutable { if(repeat++ < limit) return 0;
/// repeat = 0; return 1; }`: the conditioner that drives one of the three
/// nested loops.
///
/// It returns 0 `limit` times, then resets and returns 1 -- so the task it
/// precedes runs `limit + 1` times per entry. The reset is what matters for
/// the outermost of the three, which is one node for the whole of a ten-run
/// `run_n`: a conditioner that kept its state would loop once and never
/// again. The two inner ones are emplaced afresh on every entry to the
/// subflow above them, so their state starts at zero either way.
fn loop_back(limit: i32) -> impl FnMut() -> i32 + Send + 'static {
    let mut repeat = 0;
    move || {
        if repeat < limit {
            repeat += 1;
            0
        } else {
            repeat = 0;
            1
        }
    }
}

/// Upstream's `nested_cond(w)`: three condition loops one inside another, the
/// middle one inside a subflow and the innermost inside a subflow of that.
///
/// The counter sits in the innermost loop's body, so the only thing that can
/// produce upstream's `(inner+1)*(mid+1)*(outer+1)*repeat` is every level
/// going round the right number of times *and* each level's subflow being
/// joined before the conditioner above it runs. A middle loop that re-entered
/// its subflow before the inner loop had finished would count too few; a
/// subflow whose state survived an entry would count too many.
fn nested_cond(workers: usize) {
    const OUTER_LOOP: i32 = 3;
    const MID_LOOP: i32 = 4;
    const INNER_LOOP: i32 = 5;
    const REPEAT: usize = 10;

    let executor = Executor::new(workers);
    let counter = counter();
    let mut flow = Taskflow::new();

    let s = flow.emplace(|| {});
    let a = {
        let counter = Arc::clone(&counter);
        flow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
            //           ___________
            //          |           |
            //          v           |
            //   S1 -> A1 -> B1 -> cond
            let s1 = subflow.emplace(|| {});
            let a1 = subflow.emplace(|| {});
            subflow.task(a1).succeed([s1]);

            let b1 = {
                let counter = Arc::clone(&counter);
                subflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
                    //           ___________
                    //          |           |
                    //          v           |
                    //   S2 -> A2 -> B2 -> cond
                    //          |
                    //          -----> C, D, E
                    let s2 = subflow.emplace(|| {});
                    let a2 = subflow.emplace(|| {});
                    subflow.task(a2).succeed([s2]);

                    let b2 = {
                        let counter = Arc::clone(&counter);
                        subflow.emplace(move || {
                            counter.fetch_add(1, Ordering::Relaxed);
                        })
                    };
                    subflow.task(b2).succeed([a2]);

                    let inner = subflow.emplace_condition(loop_back(INNER_LOOP));
                    subflow.task(inner).name("cond").succeed([b2]).precede([a2]);

                    // Upstream's three redundant tasks: leaves of the
                    // subgraph that nothing depends on, released afresh every
                    // time `A2` runs. What they add to the case is work the
                    // subgraph's own join has to wait for that is not on the
                    // loop's chain -- a node a cycle re-releases has to be
                    // counted again each time round.
                    for name in ["C", "D", "E"] {
                        let redundant = subflow.emplace(|| {});
                        subflow.task(redundant).name(name).succeed([a2]);
                    }
                })
            };
            subflow.task(b1).succeed([a1]);

            let mid = subflow.emplace_condition(loop_back(MID_LOOP));
            subflow.task(mid).name("cond").succeed([b1]).precede([a1]);
        })
    };
    flow.task(a).succeed([s]);

    let outer = flow.emplace_condition(loop_back(OUTER_LOOP));
    flow.task(outer).succeed([a]).precede([a]);

    executor
        .run_n(&flow, REPEAT)
        .wait()
        .expect("a task of this taskflow panicked");

    let per_run = usize::try_from((INNER_LOOP + 1) * (MID_LOOP + 1) * (OUTER_LOOP + 1))
        .expect("a small product");
    assert_eq!(count(&counter), per_run * REPEAT, "{workers} worker(s)");
}

worker_matrix! {
    nested_cond {
        nested_cond_1_worker(1),
        nested_cond_2_workers(2),
        nested_cond_3_workers(3),
        nested_cond_4_workers(4),
        nested_cond_5_workers(5),
        nested_cond_6_workers(6),
        nested_cond_7_workers(7),
        nested_cond_8_workers(8),
    }
}

// ----------------------------------------------------------------------------
// Testcase: Cond2Cond
// ----------------------------------------------------------------------------

/// Upstream's `cond2cond(unsigned w)`.
///
/// ```text
///         ______
///        |      |
///        v      |
///   S -> A -> cond1 -> cond2 -> D
///              |         |
///              -> B      -> (back to cond1, and to A)
/// ```
///
/// Two conditioners in series, the second of which branches back into the
/// first as well as into the task that precedes both of them. What it pins is
/// the successor *positions*: `cond1` returns 0 on its first visit of a round
/// and 1 afterwards, so `B` -- its third successor, and the one upstream fills
/// with `REQUIRE(false)` -- must never run, while the three visits of `A`, four
/// of `cond1` and three of `cond2` per round are what say each branch went
/// where it was told.
///
/// The edges are added in upstream's order, not in a tidier one, because that
/// order *is* the test: a conditioner's return value is an index into the
/// successor list in the order the edges were added.
fn cond2cond(workers: usize) {
    const REPEAT: usize = 10;

    let mut flow = Taskflow::new();

    let num_visit_a = counter();
    let num_visit_c1 = counter();
    let num_visit_c2 = counter();

    // Upstream's two `int`s, shared because `D` resets what the conditioners
    // bump.
    let iteration_c1 = counter();
    let iteration_c2 = counter();

    let s = flow.emplace(|| {});
    let a = counted(&mut flow, &num_visit_a);
    flow.task(s).precede([a]);

    let cond1 = {
        let num_visit_c1 = Arc::clone(&num_visit_c1);
        let iteration_c1 = Arc::clone(&iteration_c1);
        flow.emplace_condition(move || {
            num_visit_c1.fetch_add(1, Ordering::Relaxed);
            let iteration = iteration_c1.fetch_add(1, Ordering::Relaxed) + 1;
            i32::from(iteration != 1)
        })
    };
    flow.task(a).precede([cond1]);
    flow.task(cond1).precede([a]);

    let cond2 = {
        let num_visit_c2 = Arc::clone(&num_visit_c2);
        let iteration_c2 = Arc::clone(&iteration_c2);
        flow.emplace_condition(move || {
            num_visit_c2.fetch_add(1, Ordering::Relaxed);
            let iteration = iteration_c2.fetch_add(1, Ordering::Relaxed);
            i32::try_from(iteration).expect("a handful of iterations per round")
        })
    };
    flow.task(cond1).precede([cond2]);
    flow.task(cond2).precede([cond1, a]);

    // Upstream's `B`, whose body is `REQUIRE(false)`: a panicking task body is
    // what a failed assertion inside one is, and `run` below reports it.
    let b = flow.emplace(|| panic!("`B` is never the selected branch"));
    flow.task(cond1).precede([b]);

    let d = {
        let iteration_c1 = Arc::clone(&iteration_c1);
        let iteration_c2 = Arc::clone(&iteration_c2);
        flow.emplace(move || {
            iteration_c1.store(0, Ordering::Relaxed);
            iteration_c2.store(0, Ordering::Relaxed);
        })
    };
    flow.task(cond2).precede([d]);

    let executor = Executor::new(workers);
    executor
        .run_n(&flow, REPEAT)
        .wait()
        .expect("a task of this taskflow panicked");

    assert_eq!(
        count(&num_visit_a),
        3 * REPEAT,
        "{workers} worker(s): visits of `A`"
    );
    assert_eq!(
        count(&num_visit_c1),
        4 * REPEAT,
        "{workers} worker(s): visits of `cond1`"
    );
    assert_eq!(
        count(&num_visit_c2),
        3 * REPEAT,
        "{workers} worker(s): visits of `cond2`"
    );
}

worker_matrix! {
    cond2cond {
        cond2cond_1_worker(1),
        cond2cond_2_workers(2),
        cond2cond_3_workers(3),
        cond2cond_4_workers(4),
        cond2cond_5_workers(5),
        cond2cond_6_workers(6),
        cond2cond_7_workers(7),
        cond2cond_8_workers(8),
    }
}

// ----------------------------------------------------------------------------
// Testcase: HierCondition
// ----------------------------------------------------------------------------

/// Upstream's `c1`/`c2` loop, which `tf1` and `tf2` are each one copy of:
/// `A` resets the shared counter, `B` checks that its own call count and that
/// counter have stayed in step, and `C` drives the pair round a hundred times.
///
/// `B`'s `state` is the whole point of it. The taskflow this builds is
/// composed twice over -- once directly by `tf3`, once from inside a subflow
/// of `tf3` -- and a module task runs the *same* graph, so the same closure
/// state, every time: `state % 100 == counter % 100` holds across every
/// module run there is only if each run of the loop is a hundred iterations
/// exactly and no two runs of it overlap.
fn hierarchical_loop(name: &str, counter: &Arc<AtomicI64>, check_on_entry: bool) -> Taskflow {
    let mut taskflow = Taskflow::with_name(name);

    let a = {
        let counter = Arc::clone(counter);
        taskflow.emplace(move || {
            // Upstream's `c2A` asserts the loop it is about to reset ran to
            // its hundredth iteration; its `c1A` only resets.
            if check_on_entry {
                assert_eq!(
                    counter.load(Ordering::Relaxed),
                    100,
                    "the loop before this one finished"
                );
            }
            counter.store(0, Ordering::Relaxed);
        })
    };
    let b = {
        let counter = Arc::clone(counter);
        let mut state = 0_i64;
        taskflow.emplace(move || {
            let called = state;
            state += 1;
            assert_eq!(
                called % 100,
                counter.load(Ordering::Relaxed) % 100,
                "the module's own call count and the loop counter stay in step"
            );
        })
    };
    let c = {
        let counter = Arc::clone(counter);
        taskflow.emplace_condition(move || {
            i32::from(counter.fetch_add(1, Ordering::Relaxed) + 1 >= 100)
        })
    };

    taskflow.task(a).name("A").precede([b]);
    taskflow.task(b).name("B").precede([c]);
    taskflow.task(c).name("C").precede([b]);

    taskflow
}

/// Upstream's `hierarchical_condition(w)`: a top-level taskflow of condition
/// loops that also composes three taskflows, one of them from inside a
/// subflow that composes it a second time.
///
/// Every assertion is upstream's, and between them they pin the one thing
/// this case is for: a module task and a subflow task are *nodes*, so the
/// graph above them does not continue until the whole graph below them has
/// finished -- however many condition loops that graph ran. `grab`'s three
/// assertions are the end of that chain: it runs when `mod0`, `mod1` and
/// `sync` have, and it can only see `c1 == 100` if the module `sbf1` reached
/// through a subflow ran its hundred iterations too.
fn hierarchical_condition(workers: usize) {
    let executor = Executor::new(workers);

    let c1 = Arc::new(AtomicI64::new(0));
    let c2 = Arc::new(AtomicI64::new(0));
    let c2_repeat = Arc::new(AtomicI64::new(0));

    // Upstream's `tf0("c0")`, which it composes without ever putting a task
    // in it: a module task whose target has nothing to run.
    let tf0 = Arc::new(Taskflow::with_name("c0"));
    let tf1 = Arc::new(hierarchical_loop("c1", &c1, false));
    let tf2 = Arc::new(hierarchical_loop("c2", &c2, true));

    let mut tf3 = Taskflow::with_name("top");

    let init = {
        let c1 = Arc::clone(&c1);
        let c2 = Arc::clone(&c2);
        let c2_repeat = Arc::clone(&c2_repeat);
        tf3.emplace(move || {
            c1.store(0, Ordering::Relaxed);
            c2.store(0, Ordering::Relaxed);
            c2_repeat.store(0, Ordering::Relaxed);
        })
    };
    tf3.task(init).name("init");

    let loop1 = {
        let c2 = Arc::clone(&c2);
        tf3.emplace_condition(move || i32::from(c2.fetch_add(1, Ordering::Relaxed) + 1 >= 100))
    };
    tf3.task(loop1).name("loop1");

    let loop2 = {
        let c2 = Arc::clone(&c2);
        let c2_repeat = Arc::clone(&c2_repeat);
        tf3.emplace_condition(move || {
            c2.store(0, Ordering::Relaxed);
            i32::from(c2_repeat.fetch_add(1, Ordering::Relaxed) + 1 >= 100)
        })
    };
    tf3.task(loop2).name("loop2");

    let sync = {
        let c2 = Arc::clone(&c2);
        let c2_repeat = Arc::clone(&c2_repeat);
        tf3.emplace(move || {
            assert_eq!(c2.load(Ordering::Relaxed), 0, "loop2 reset c2");
            assert_eq!(
                c2_repeat.load(Ordering::Relaxed),
                100,
                "loop2 went round a hundred times"
            );
            c2_repeat.store(0, Ordering::Relaxed);
        })
    };
    tf3.task(sync).name("sync");

    let grab = {
        let c1 = Arc::clone(&c1);
        let c2 = Arc::clone(&c2);
        let c2_repeat = Arc::clone(&c2_repeat);
        tf3.emplace(move || {
            assert_eq!(
                c1.load(Ordering::Relaxed),
                100,
                "both modules of c1 ran their hundred iterations"
            );
            assert_eq!(c2.load(Ordering::Relaxed), 0);
            assert_eq!(c2_repeat.load(Ordering::Relaxed), 0);
        })
    };
    tf3.task(grab).name("grab");

    let mod0 = tf3.composed_of(Arc::clone(&tf0));
    tf3.task(mod0).name("module0");
    let mod1 = tf3.composed_of(Arc::clone(&tf1));
    tf3.task(mod1).name("module1");

    let sbf1 = tf3.emplace_subflow({
        let tf1 = Arc::clone(&tf1);
        move |subflow: &mut Subflow<'_>| {
            let sbf1_1 = subflow.emplace(|| {});
            subflow.task(sbf1_1).name("sbf1_1");
            let module1 = subflow.composed_of(Arc::clone(&tf1));
            subflow.task(module1).name("module1").succeed([sbf1_1]);
            let sbf1_2 = subflow.emplace(|| {});
            subflow.task(sbf1_2).name("sbf1_2").succeed([module1]);
            subflow.join().expect("a fresh subflow is joinable");
        }
    });
    tf3.task(sbf1).name("sbf1");

    let mod2 = tf3.composed_of(Arc::clone(&tf2));
    tf3.task(mod2).name("module2");

    tf3.task(init).precede([mod0, sbf1, loop1]);
    tf3.task(loop1).precede([loop1, mod2]);
    tf3.task(loop2).succeed([mod2]).precede([loop1, sync]);
    tf3.task(mod0).precede([grab]);
    tf3.task(sbf1).precede([mod1]);
    tf3.task(mod1).precede([grab]);
    tf3.task(sync).precede([grab]);

    // Upstream's `run`, `run_n(10)` and `wait_for_all`. The handles are held
    // across the wait so that `wait_for_all` is what waits, and drained
    // afterwards so that an assertion inside a task body is reported rather
    // than dropped with its run.
    let handles = [executor.run(&tf3), executor.run_n(&tf3, 10)];
    executor.wait_for_all();
    for handle in handles {
        handle
            .wait()
            .unwrap_or_else(|error| panic!("{workers} worker(s): {error}"));
    }
}

worker_matrix! {
    hierarchical_condition {
        hier_condition_1_worker(1),
        hier_condition_2_workers(2),
        hier_condition_3_workers(3),
        hier_condition_4_workers(4),
        hier_condition_5_workers(5),
        hier_condition_6_workers(6),
        hier_condition_7_workers(7),
        hier_condition_8_workers(8),
    }
}

// ----------------------------------------------------------------------------
// Testcase: CondSubflow
// ----------------------------------------------------------------------------

/// Upstream's `data[i] = i*(i+1)/2*123`, which is nothing but a value that
/// depends on `i` in a way a stray zero or a repeated index cannot imitate.
fn triangular(i: usize) -> usize {
    i * (i + 1) / 2 * 123
}

/// Upstream's `condition_subflow(W)`: a condition task that sends the run
/// back into one subflow task a thousand times, each entry building a
/// one-task subgraph for the index the counter has reached.
///
/// Upstream's `num_tasks() == 4` before and after is the case's own
/// statement that the subgraph is cleared rather than accumulated -- a
/// thousand entries that each kept their child would be a thousand tasks. It
/// holds here by construction rather than by policy, because a subgraph is a
/// `Graph` inside its node's handle and not part of the taskflow's own arena,
/// so the dump is asserted beside it: no subflow cluster in it is the
/// observable that a retained subgraph would break, and it is the same claim
/// upstream's count makes.
fn condition_subflow(workers: usize) {
    const I: usize = 1000;

    let context = format!("{workers} worker(s)");
    let executor = Executor::new(workers);

    let data: Arc<Vec<AtomicUsize>> = Arc::new((0..I).map(|_| AtomicUsize::new(0)).collect());
    let i = counter();

    let mut taskflow = Taskflow::new();

    let init = {
        let i = Arc::clone(&i);
        taskflow.emplace(move || i.store(0, Ordering::Relaxed))
    };
    taskflow.task(init).name("init");

    let subflow = {
        let data = Arc::clone(&data);
        let i = Arc::clone(&i);
        taskflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
            // Upstream's `[&, i]`: the index is captured by value as it
            // stands when the *body* runs, which is what makes each entry's
            // subgraph a different one.
            let index = count(&i);
            let data = Arc::clone(&data);
            let child = subflow.emplace(move || {
                assert!(index < I, "the conditioner stops at I");
                data[index].store(triangular(index), Ordering::Relaxed);
            });
            subflow.task(child).name(index.to_string());
        })
    };
    taskflow.task(subflow).name("subflow");

    let cond = {
        let i = Arc::clone(&i);
        taskflow.emplace_condition(move || i32::from(i.fetch_add(1, Ordering::Relaxed) + 1 >= I))
    };
    taskflow.task(cond).name("cond");

    let stop = taskflow.emplace(|| {});
    taskflow.task(stop).name("stop");

    taskflow.task(init).precede([subflow]);
    taskflow.task(subflow).precede([cond]);
    taskflow.task(cond).precede([subflow, stop]);

    assert_eq!(taskflow.num_tasks(), 4, "{context}: before the run");
    run(&executor, &taskflow);
    assert_eq!(taskflow.num_tasks(), 4, "{context}: after the run");
    assert!(
        !taskflow.dump().contains("Subflow: subflow"),
        "{context}: the subgraph of the last entry was cleared"
    );

    for j in 0..I {
        assert_eq!(
            data[j].load(Ordering::Relaxed),
            triangular(j),
            "{context}: entry {j} of the first run"
        );
        data[j].store(0, Ordering::Relaxed);
    }

    // Upstream's `run_n(1)`, `run_n(10)`, `run_n(100)` and `wait_for_all`:
    // a hundred and eleven more runs of the thousand-entry loop, queued
    // behind each other on the one taskflow.
    let handles = [
        executor.run_n(&taskflow, 1),
        executor.run_n(&taskflow, 10),
        executor.run_n(&taskflow, 100),
    ];
    executor.wait_for_all();
    for handle in handles {
        handle
            .wait()
            .unwrap_or_else(|error| panic!("{context}: {error}"));
    }

    assert_eq!(
        taskflow.num_tasks(),
        4,
        "{context}: after a hundred and more"
    );
    for j in 0..I {
        assert_eq!(
            data[j].load(Ordering::Relaxed),
            triangular(j),
            "{context}: entry {j} of the later runs"
        );
    }
}

worker_matrix! {
    condition_subflow {
        cond_subflow_1_worker(1),
        cond_subflow_2_workers(2),
        cond_subflow_3_workers(3),
        cond_subflow_4_workers(4),
        cond_subflow_5_workers(5),
        cond_subflow_6_workers(6),
        cond_subflow_7_workers(7),
        cond_subflow_8_workers(8),
    }
}

// ----------------------------------------------------------------------------
// Testcase: MultipleBranches
// ----------------------------------------------------------------------------

/// Upstream's `multiple_branches(unsigned W)`.
///
/// A placeholder is wired to a hundred `X -> Y` chains and only then given
/// multi-condition work -- upstream's `A.work(..)` on an already-connected
/// task -- returning a set of indices drawn from `-10..=110`. Every index in
/// `0..100` runs one chain, worth two increments; every index outside it must
/// select nothing at all, which is what makes `2 * ans == counter` an
/// assertion about the range check rather than about the fan-out.
fn multiple_branches(workers: usize, seed: u32) {
    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();

    let counter = counter();
    let a = taskflow.placeholder();

    for _ in 0..100 {
        let x = counted(&mut taskflow, &counter);
        let y = counted(&mut taskflow, &counter);
        taskflow.task(x).precede([y]);
        taskflow.task(a).precede([x]);
    }

    // Upstream's `for(int i=-10; i<=110; i++) if(::rand() % 10 == 0)`.
    let mut rng = Xorshift32::new(seed);
    let mut conditions = Vec::new();
    let mut answer = 0;
    for index in -10..=110 {
        if rng.next_u32().is_multiple_of(10) {
            conditions.push(index);
            if (0..100).contains(&index) {
                answer += 1;
            }
        }
    }
    let selected = conditions.clone();
    taskflow
        .task(a)
        .multi_condition_work(move || conditions.clone());

    run(&executor, &taskflow);

    assert_eq!(
        2 * answer,
        count(&counter),
        "seed {seed}, {workers} worker(s), branches {selected:?}"
    );
}

worker_matrix! {
    multiple_branches {
        multiple_branches_1_worker(1, seed_for(1)),
        multiple_branches_2_workers(2, seed_for(2)),
        multiple_branches_3_workers(3, seed_for(3)),
        multiple_branches_4_workers(4, seed_for(4)),
    }
}

// ----------------------------------------------------------------------------
// Testcase: MultipleLoops
// ----------------------------------------------------------------------------

/// Adds one of upstream's `multiple_loops` nodes: a multi-conditioner that
/// opens both of its branches once and then loops on the first ten times.
///
/// The `bool` and the `int` are upstream's init-captures, one pair per node,
/// and they stay in the closure for the reason `cyclic_cond`'s `sel` does --
/// the node is the only reader and writer, and only one instance of it is ever
/// runnable.
fn self_loop(taskflow: &mut Taskflow, counter: &Counter) -> Task {
    let counter = Arc::clone(counter);
    let mut first = true;
    let mut rounds = 0;
    taskflow.emplace_multi_condition(move || {
        if first {
            first = false;
            return vec![0, 1];
        }
        counter.fetch_add(1, Ordering::Relaxed);
        rounds += 1;
        // -1 names no successor, which is how this node's loop ends.
        vec![if rounds < 10 { 0 } else { -1 }]
    })
}

/// Upstream's `multiple_loops(unsigned W)`.
///
/// Four self-looping multi-conditioners in a chain, `B -> C -> D -> E`, each
/// wired as `precede(itself, the next one)`. The first visit of each returns
/// `{0, 1}`, which is the case's point: one body selects two successors at
/// once, one of them itself, so the node is re-entered *while* the chain
/// carries on past it and the four loops are live together. Ten counted
/// iterations each is upstream's `counter == 40`, and `E`, which has only
/// itself as a successor, is where the `{0, 1}` of an index with no successor
/// has to select nothing.
fn multiple_loops(workers: usize) {
    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();

    let counter = counter();
    let a = taskflow.emplace(|| {});
    let b = self_loop(&mut taskflow, &counter);
    let c = self_loop(&mut taskflow, &counter);
    let d = self_loop(&mut taskflow, &counter);
    let e = self_loop(&mut taskflow, &counter);

    taskflow.task(a).precede([b]);
    taskflow.task(b).precede([b, c]);
    taskflow.task(c).precede([c, d]);
    taskflow.task(d).precede([d, e]);
    taskflow.task(e).precede([e]);

    run(&executor, &taskflow);

    assert_eq!(
        count(&counter),
        40,
        "{workers} worker(s): ten counted iterations of each of the four loops"
    );
}

worker_matrix! {
    multiple_loops {
        multiple_loops_1_worker(1),
        multiple_loops_2_workers(2),
        multiple_loops_3_workers(3),
        multiple_loops_4_workers(4),
        multiple_loops_5_workers(5),
        multiple_loops_6_workers(6),
        multiple_loops_7_workers(7),
        multiple_loops_8_workers(8),
    }
}

// ----------------------------------------------------------------------------
// Testcase: MultiCondBinaryTree
// ----------------------------------------------------------------------------

/// Upstream's `N`: the depth of the tree, and the number of runs of it.
const BINARY_TREE_LEVELS: usize = 10;

/// Upstream's `binary_tree(unsigned w)`.
///
/// A thousand-node heap of multi-conditioners, every one of which counts
/// itself and returns `{0, 1}`. Nothing here is a static task and nothing is
/// ever waited for: the whole tree is driven by weak edges, so a node that was
/// released by a strong dependency it does not have, or released twice,
/// shows up directly in the count. Ten runs of it is upstream's
/// `((1 << N) - 1) * N`.
fn binary_tree(workers: usize) {
    let mut taskflow = Taskflow::new();
    let executor = Executor::new(workers);

    let counter = counter();
    let tasks: Vec<Task> = (1..(1_usize << BINARY_TREE_LEVELS))
        .map(|_| {
            let counter = Arc::clone(&counter);
            taskflow.emplace_multi_condition(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                vec![0, 1]
            })
        })
        .collect();

    // The heap's edges, left child first so that the `{0, 1}` above names
    // both. A leaf names neither, which is the out-of-range rule again.
    for (index, &parent) in tasks.iter().enumerate() {
        let left = index * 2 + 1;
        let right = left + 1;
        if let Some(&child) = tasks.get(left) {
            taskflow.task(parent).precede([child]);
        }
        if let Some(&child) = tasks.get(right) {
            taskflow.task(parent).precede([child]);
        }
    }

    executor
        .run_n(&taskflow, BINARY_TREE_LEVELS)
        .wait()
        .expect("a task of this taskflow panicked");

    assert_eq!(
        count(&counter),
        ((1_usize << BINARY_TREE_LEVELS) - 1) * BINARY_TREE_LEVELS,
        "{workers} worker(s): every node of the tree runs once per run"
    );
}

worker_matrix! {
    binary_tree {
        multi_cond_binary_tree_1_worker(1),
        multi_cond_binary_tree_2_workers(2),
        multi_cond_binary_tree_3_workers(3),
        multi_cond_binary_tree_4_workers(4),
    }
}

// ----------------------------------------------------------------------------
// Testcase: MultiCond.MultipleRuns
// ----------------------------------------------------------------------------

/// Upstream's `multi_cond_multiple_runs(unsigned W)`.
///
/// ```text
///   init  ----------------> yes
///   cond1 -> tempTask1 ---->
///   cond2 -> tempTask2 ---->
/// ```
///
/// `yes` has three strong dependencies and only ever collects two of them:
/// `cond1` returns 0 and releases `tempTask1`, while `cond2` returns 1 and it
/// has a single successor, so `tempTask2` never runs. A conditioner's edge
/// being weak is what makes that a deadlock-free end to the run rather than a
/// hang, and `flag` staying false through one, two and three rounds is what
/// says the unreleased strong dependency is not quietly forgotten between
/// runs.
fn multi_cond_multiple_runs(workers: usize) {
    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();

    let flag = Arc::new(AtomicBool::new(false));

    let init = taskflow.emplace(|| {});
    let cond1 = taskflow.emplace_condition(|| 0);
    let cond2 = taskflow.emplace_condition(|| 1);
    let yes = {
        let flag = Arc::clone(&flag);
        taskflow.emplace(move || flag.store(true, Ordering::Relaxed))
    };
    // Upstream's `no`, which it declares and never wires to anything.
    let _no = taskflow.emplace(|| {});

    let temp_task1 = taskflow.emplace(|| {});
    let temp_task2 = taskflow.emplace(|| {});
    taskflow.task(cond2).precede([temp_task2]);
    taskflow.task(temp_task2).precede([yes]);
    taskflow.task(init).precede([yes]);

    taskflow.task(cond1).precede([temp_task1]);
    taskflow.task(temp_task1).precede([yes]);

    // Not upstream's, and the mechanism the rest of the case rests on: all
    // three of `yes`'s dependencies are strong, because the conditioners
    // precede the temporaries rather than `yes` itself.
    assert_eq!(taskflow.task_ref(yes).num_strong_dependencies(), 3);
    assert_eq!(taskflow.task_ref(yes).num_weak_dependencies(), 0);

    assert!(!flag.load(Ordering::Relaxed));
    for rounds in 1..=3 {
        executor
            .run_n(&taskflow, rounds)
            .wait()
            .expect("a task of this taskflow panicked");
        assert!(
            !flag.load(Ordering::Relaxed),
            "{workers} worker(s), {rounds} round(s): `yes` ran with a strong \
             dependency unreleased"
        );
    }
}

worker_matrix! {
    multi_cond_multiple_runs {
        multi_cond_multiple_runs_1_worker(1),
        multi_cond_multiple_runs_2_workers(2),
        multi_cond_multiple_runs_3_workers(3),
        multi_cond_multiple_runs_4_workers(4),
    }
}

// ----------------------------------------------------------------------------
// The issue's two extra checks, which upstream has no case for
// ----------------------------------------------------------------------------

/// An index that names no successor runs nothing, and the run still ends.
///
/// Upstream's `-10..=110` sweep covers this inside a larger assertion;
/// pinning it on its own is what says *which* half of the range check each
/// bound exercises, and `i32::MIN` is the value an `as` cast would turn into
/// a successor position rather than reject.
#[test]
fn an_out_of_range_index_runs_no_successor() {
    for condition in [-1, 2, 7, i32::MIN, i32::MAX] {
        for workers in 1..=4 {
            let executor = Executor::new(workers);
            let mut taskflow = Taskflow::new();

            let branches = counter();
            let conditioner = counter();
            let cond = {
                let conditioner = Arc::clone(&conditioner);
                taskflow.emplace_condition(move || {
                    conditioner.fetch_add(1, Ordering::Relaxed);
                    condition
                })
            };
            let yes = counted(&mut taskflow, &branches);
            let no = counted(&mut taskflow, &branches);
            taskflow.task(cond).precede([yes, no]);

            run(&executor, &taskflow);

            assert_eq!(
                count(&conditioner),
                1,
                "index {condition}, {workers} worker(s)"
            );
            assert_eq!(
                count(&branches),
                0,
                "index {condition} names no successor, so neither branch runs"
            );
        }
    }
}

/// Running one graph twice has to behave identically, which is the issue's
/// second extra check and the property that says the counters are re-armed
/// rather than merely spent.
///
/// The shape is chosen so that a counter left unreset would show: `e` has two
/// strong dependencies, one of them on the branch the conditioner selects. A
/// second run that started from `e`'s spent counter would run `e` early, or
/// twice, or not at all -- all three visible in the per-run deltas.
#[test]
fn the_same_graph_run_twice_behaves_identically() {
    for workers in 1..=4 {
        let executor = Executor::new(workers);
        let mut taskflow = Taskflow::new();

        let counters: [Counter; 5] = core::array::from_fn(|_| counter());
        let a = counted(&mut taskflow, &counters[0]);
        let selected = counted(&mut taskflow, &counters[2]);
        let unselected = counted(&mut taskflow, &counters[3]);
        let joined = counted(&mut taskflow, &counters[4]);
        let cond = {
            let conditioner = Arc::clone(&counters[1]);
            taskflow.emplace_condition(move || {
                conditioner.fetch_add(1, Ordering::Relaxed);
                0
            })
        };

        taskflow.task(a).precede([cond, joined]);
        taskflow.task(cond).precede([selected, unselected]);
        taskflow.task(selected).precede([joined]);

        assert_eq!(
            taskflow.task_ref(joined).num_strong_dependencies(),
            2,
            "`e` waits for both `a` and the selected branch"
        );

        let snapshot = |counters: &[Counter; 5]| counters.iter().map(count).collect::<Vec<_>>();

        run(&executor, &taskflow);
        let first = snapshot(&counters);
        assert_eq!(
            first,
            vec![1, 1, 1, 0, 1],
            "{workers} worker(s): `a`, the conditioner, the selected branch \
             and the join each run once, the unselected branch never"
        );

        run(&executor, &taskflow);
        let second = snapshot(&counters);
        let deltas: Vec<usize> = second
            .iter()
            .zip(&first)
            .map(|(second, first)| second - first)
            .collect();
        assert_eq!(deltas, first, "{workers} worker(s): the second run differs");
    }
}

/// A conditioner whose body panics selects nothing: the panic is recorded and
/// reported from `wait` exactly as a static task's is, and the run ends.
///
/// Cancelling the *rest* of the run on a panic is a separate issue (#32); what
/// this pins is that the branch is simply not taken, and in particular that
/// the failure is not the unconditional "release every successor" that a
/// non-conditioner gets.
#[test]
fn a_panicking_conditioner_selects_nothing() {
    for workers in 1..=4 {
        let executor = Executor::new(workers);
        let mut taskflow = Taskflow::new();

        let branches = counter();
        let cond = taskflow.emplace_condition(|| panic!("the conditioner failed"));
        let yes = counted(&mut taskflow, &branches);
        let no = counted(&mut taskflow, &branches);
        taskflow.task(cond).precede([yes, no]);

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("the conditioner panicked");
        if let RunError::Panicked(payload) = &error {
            assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&"the conditioner failed"),
                "{workers} worker(s)"
            );
        } else {
            panic!("unexpected error: {error:?}");
        }
        assert_eq!(
            count(&branches),
            0,
            "{workers} worker(s): a panicking conditioner selects no successor"
        );
    }
}

/// A multi-condition task that selects every one of its successors is the
/// other extreme of the same code path, and the one that shows the weak edges
/// really are uncounted: each successor runs exactly once even though none of
/// them was ever waited for.
#[test]
fn a_multi_condition_can_select_every_successor() {
    for workers in 1..=4 {
        let executor = Executor::new(workers);
        let mut taskflow = Taskflow::new();

        let branches = counter();
        let cond = taskflow.emplace_multi_condition(|| (0..8).collect());
        let successors: Vec<Task> = (0..8).map(|_| counted(&mut taskflow, &branches)).collect();
        taskflow.task(cond).precede(successors.iter().copied());

        run(&executor, &taskflow);

        assert_eq!(
            count(&branches),
            8,
            "{workers} worker(s): every selected successor runs once"
        );
        for &successor in &successors {
            assert_eq!(
                taskflow.task_ref(successor).num_strong_dependencies(),
                0,
                "a conditioner's edge is a weak dependency"
            );
        }
    }
}

/// A placeholder wired to its branches first and given single-condition work
/// afterwards is a conditioner, and the index it returns picks the branch.
///
/// `multiple_branches` above is upstream's `A.work(..)` on an
/// already-connected task for the *multi*-condition arm; this is the same
/// shape for the single-condition one, which is the arm upstream's
/// `Cond.Types` reaches by return type instead. Giving the work twice is what
/// says the arm replaces whatever was there: the second index selects the
/// other branch, on a graph whose edges never moved.
#[test]
fn a_placeholder_given_condition_work_is_a_conditioner() {
    for workers in 1..=4 {
        let executor = Executor::new(workers);
        let mut taskflow = Taskflow::new();

        let cond = taskflow.placeholder();
        let branches: [Counter; 2] = [counter(), counter()];
        let yes = counted(&mut taskflow, &branches[0]);
        let no = counted(&mut taskflow, &branches[1]);
        taskflow.task(cond).precede([yes, no]);

        assert_eq!(taskflow.task_ref(cond).task_type(), TaskType::Placeholder);
        taskflow.task(cond).condition_work(|| 0);
        assert_eq!(
            taskflow.task_ref(cond).task_type(),
            TaskType::Condition,
            "{workers} worker(s): the work is what makes the task a conditioner"
        );

        run(&executor, &taskflow);
        assert_eq!(
            [count(&branches[0]), count(&branches[1])],
            [1, 0],
            "{workers} worker(s): index 0 selects the first successor"
        );

        // The same task, told to select the other branch: a conditioner's work
        // is replaceable like any other task's.
        taskflow.task(cond).condition_work(|| 1);
        run(&executor, &taskflow);
        assert_eq!(
            [count(&branches[0]), count(&branches[1])],
            [1, 1],
            "{workers} worker(s): the replaced body selects the second"
        );
    }
}

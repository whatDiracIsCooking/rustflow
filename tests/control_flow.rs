//! Translated from `unittests/test_control_flow.cpp` (Taskflow v4.1.0).
//!
//! The cases ported here are the two the conditional-scheduling issue names:
//! `LoopCond`, a conditioner that closes a cycle on itself, and
//! `MultipleBranches`, a multi-condition task that fans out to an arbitrary
//! subset of a hundred successors. Both run at 1..=4 workers: that is
//! upstream's own sweep for `LoopCond`, and the four of its eight that say
//! something distinct for `MultipleBranches`, whose fan-out is the same
//! whether four workers share it or eight. The rest of that file -- the
//! coin-flip, subflow, module and cancellation cases -- arrives with the
//! milestones that make those node kinds runnable.
//!
//! Two standing adaptations, because the port is not quite where upstream is:
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
//!   set from the crate's own [`Xorshift32`] instead, seeded per worker count
//!   and with the seed in every assertion message, so a failure reproduces
//!   exactly. The draw itself is upstream's: one per index over `-10..=110`,
//!   kept when `draw % 10 == 0`.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use rustflow::prelude::*;
use rustflow::{Executor, RunError, Xorshift32};

/// How many times a task body has run, shared with that body.
type Counter = Arc<AtomicUsize>;

/// A fresh counter.
fn counter() -> Counter {
    Arc::new(AtomicUsize::new(0))
}

/// Adds a static task that does nothing but count its own invocations.
fn counted(taskflow: &mut Taskflow, counter: &Counter) -> Task {
    let counter = Arc::clone(counter);
    taskflow.emplace(move || {
        counter.fetch_add(1, Ordering::Relaxed);
    })
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

#[test]
fn loop_cond_1_worker() {
    loop_cond(1);
}

#[test]
fn loop_cond_2_workers() {
    loop_cond(2);
}

#[test]
fn loop_cond_3_workers() {
    loop_cond(3);
}

#[test]
fn loop_cond_4_workers() {
    loop_cond(4);
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
        counter.load(Ordering::Relaxed),
        "seed {seed}, {workers} worker(s), branches {selected:?}"
    );
}

/// A seed per worker count, so the four cases do not all draw the same branch
/// set and any one of them can be re-run on its own.
fn seed_for(workers: usize) -> u32 {
    0x5EED_0000 + u32::try_from(workers).expect("a small worker count")
}

#[test]
fn multiple_branches_1_worker() {
    multiple_branches(1, seed_for(1));
}

#[test]
fn multiple_branches_2_workers() {
    multiple_branches(2, seed_for(2));
}

#[test]
fn multiple_branches_3_workers() {
    multiple_branches(3, seed_for(3));
}

#[test]
fn multiple_branches_4_workers() {
    multiple_branches(4, seed_for(4));
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
                conditioner.load(Ordering::Relaxed),
                1,
                "index {condition}, {workers} worker(s)"
            );
            assert_eq!(
                branches.load(Ordering::Relaxed),
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

        let snapshot = |counters: &[Counter; 5]| {
            counters
                .iter()
                .map(|counter| counter.load(Ordering::Relaxed))
                .collect::<Vec<_>>()
        };

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
            branches.load(Ordering::Relaxed),
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
            branches.load(Ordering::Relaxed),
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

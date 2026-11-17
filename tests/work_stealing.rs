//! Translated from `unittests/test_work_stealing.cpp` (Taskflow v4.1.0).
//!
//! Upstream parametrises every case over one through eight workers by calling
//! one helper per case, and so does this file: one `fn` per upstream helper,
//! one `#[test]` per worker count. What the suite is for is the scheduler as a
//! whole -- a task body that spins until every *other* worker has reached a
//! rendezvous is a deadlock unless work stealing, the notifier and the
//! conditioner branch all behave -- so these are assertions about the
//! executor, not about the graph they are written on.
//!
//! Three cases upstream has are not here, because each needs something this
//! port has not got. None of them is weakened into a lookalike: a starvation
//! test that cannot starve is worse than no test, since it reports green while
//! exercising nothing.
//!
//! - **`SubflowStarvation`** is `Starvation.Loop` built inside a subflow.
//!   Subflows are milestone 4 (#50), and the stress half of that milestone is
//!   #54, which is where this case belongs.
//! - **`Oversubscription`** and **`Continuation`** both assert that a chain of
//!   tasks runs end to end on the *same* worker: `Continuation` compares
//!   `this_worker_id()` across a thousand chained tasks, and
//!   `Oversubscription` asserts `set.size() <= W/2` for `W/2` independent
//!   hundred-task chains, which can only hold if a chain never visits a queue
//!   at all. That is upstream's `_update_cache`/`TF_INVOKE_CONTINUATION`,
//!   which `Worker::invoke` says in so many words is deliberately not ported
//!   yet: its own comment puts the optimisation with the benchmarks (#18)
//!   rather than with the first correct version, and no issue tracks it on its
//!   own. Both cases were written against this port and run before being
//!   taken out, so this is measured rather than assumed: `Continuation` fails
//!   at two and at eight workers on the identity assertion itself (`task 87
//!   ran on another worker`, worker 7 against worker 1), and
//!   `Oversubscription` fails on its very first run at both counts, with
//!   `set.size()` equal to the whole pool rather than half of it. Both become
//!   portable, as written, the day that optimisation lands.
//!
//! One case is here with its middle third missing, named where it is cut:
//! `Waiter`'s second phase runs its check inside `executor.async(...)`, and
//! async tasks are milestone 5. The first and third phases need nothing that
//! is missing and are upstream's own.
//!
//! One case is here with an adaptation rather than a cut, and it is the one
//! worth arguing about: `EmbarrasingStarvation` wraps its 65536-task fan-out
//! in `parent.composed_of(taskflow)` and runs the *parent*. Module tasks are
//! milestone 4 (#49), so what runs here is the fan-out itself. The assertion
//! it carries is untouched and still has teeth -- `set.size() == W` fails if a
//! single worker is left asleep -- but the shapes are not identical and the
//! difference favours upstream: a module expands into the queue of the one
//! worker that ran the module node, so upstream's fan-out has to be *stolen*,
//! where this one is handed to the shared buffers by the submitting thread.
//! #49 should re-point this case at `composed_of`.
//!
//! Two standing adaptations across the file:
//!
//! - **`::rand()` is not a dependency.** The three cases that draw random
//!   numbers -- `Starvation.BranchesWithPivot` for its pivot worker and its
//!   90/10 split, `SkewedStarvation` for its left/right edge order,
//!   `NAryStarvation` for the pivot of each level -- draw from the crate's own
//!   [`Xorshift32`] instead, seeded per worker count and with the seed in
//!   every assertion message, so that a failing case re-runs identically.
//! - **A spin has a deadline.** Upstream's rendezvous are `while(cond)
//!   std::this_thread::yield();` under a `doctest::timeout(300)`. `cargo test`
//!   has no per-case timeout, and the failure mode these cases exist to catch
//!   is a spin that never ends, so [`spin_until`] carries the timeout that
//!   doctest was providing. It is not a retry and not a relaxation: every
//!   assertion is upstream's, and the deadline only decides whether a
//!   scheduling bug reports itself or hangs the run.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use rustflow::prelude::*;
use rustflow::{Executor, Xorshift32};

/// How long a spin waits for what it is spinning on before it gives up.
///
/// Far longer than any rendezvous in this file needs, even pinned to a single
/// core with eight workers sharing it: nothing here passes because of this
/// number, and the only thing that reaches it is a worker that is never going
/// to arrive.
const GIVE_UP_AFTER: Duration = Duration::from_secs(120);

/// Upstream's `while(!ready) std::this_thread::yield();`.
///
/// `what` is what the caller is waiting for, phrased to finish the sentence
/// "gave up waiting for ...", because the assertion message is the whole
/// diagnostic when a scheduling bug trips it.
fn spin_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + GIVE_UP_AFTER;
    while !ready() {
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        thread::yield_now();
    }
}

/// A seed per worker count, so that two cases at different worker counts do
/// not draw the same sequence and any one of them re-runs on its own.
fn seed_for(workers: usize) -> u32 {
    0x5_7EAD + u32::try_from(workers).expect("a small worker count")
}

/// Upstream's `std::unordered_set<int> set` behind its `std::mutex`: the ids
/// of the workers that reached a task body.
#[derive(Debug, Default)]
struct Involved(Mutex<HashSet<usize>>);

impl Involved {
    /// A fresh set, shared with the task bodies that record into it.
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The set itself. A body that fails an assertion while holding this lock
    /// poisons it, and the poison is not the failure worth reporting.
    fn ids(&self) -> MutexGuard<'_, HashSet<usize>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Upstream's `set.insert(executor.this_worker_id())`.
    fn record(&self, executor: &Executor) {
        let id = executor
            .this_worker_id()
            .expect("a task body runs on a worker of the executor running it");
        self.ids().insert(id);
    }

    /// How many distinct workers have been recorded.
    fn count(&self) -> usize {
        self.ids().len()
    }

    /// Upstream's `set.clear()`.
    fn clear(&self) {
        self.ids().clear();
    }
}

/// A counter shared with the task bodies that bump it.
fn shared_counter() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

/// Upstream's `taskflow.emplace([&](){ counter.fetch_add(1, relaxed); })`.
fn bump(taskflow: &mut Taskflow, counter: &Arc<AtomicUsize>) -> Task {
    let counter = Arc::clone(counter);
    taskflow.emplace(move || {
        counter.fetch_add(1, Ordering::Relaxed);
    })
}

/// Upstream's linear chain of counting tasks, and the handle of its last task.
fn counting_chain(taskflow: &mut Taskflow, counter: &Arc<AtomicUsize>, length: usize) -> Task {
    let mut prev: Option<Task> = None;
    for _ in 0..length {
        let curr = bump(taskflow, counter);
        if let Some(prev) = prev {
            taskflow.task(curr).succeed([prev]);
        }
        prev = Some(curr);
    }
    prev.expect("a chain has at least one task")
}

/// Runs `taskflow` once and turns a panicking body -- which is what a failed
/// assertion inside one is -- back into a failed test.
fn run(executor: &Executor, taskflow: &Taskflow) {
    executor
        .run(taskflow)
        .wait()
        .expect("a task of this taskflow panicked");
}

/// Upstream's `executor.run_n(taskflow, runs).wait()`, the same way.
fn run_n(executor: &Executor, taskflow: &Taskflow, runs: usize) {
    executor
        .run_n(taskflow, runs)
        .wait()
        .expect("a task of this taskflow panicked");
}

// ----------------------------------------------------------------------------
// Testcase: Starvation.Branches
// ----------------------------------------------------------------------------

/// Upstream's `starvation_branches(W)`.
///
/// A hundred-task chain, then `W - W/2` branches that count and `W/2`
/// branches that spin until the counting ones are done. The spinners occupy
/// half the pool for as long as it takes, so the run only ends if the other
/// half is awake and stealing: a pool that went to sleep during the chain and
/// was not woken by the fan-out starves here.
fn starvation_branches(workers: usize) {
    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();
    let counter = shared_counter();

    // Upstream's `W - W/2 + 100`: the chain plus the counting branches.
    let target = workers - workers / 2 + 100;

    let curr = counting_chain(&mut taskflow, &counter, 100);

    for _ in workers / 2..workers {
        let branch = bump(&mut taskflow, &counter);
        taskflow.task(branch).succeed([curr]);
    }

    for _ in 0..workers / 2 {
        let counter = Arc::clone(&counter);
        let branch = taskflow.emplace(move || {
            spin_until("the counting branches to finish", || {
                counter.load(Ordering::Relaxed) == target
            });
        });
        taskflow.task(branch).succeed([curr]);
    }

    run(&executor, &taskflow);

    assert_eq!(
        counter.load(Ordering::Relaxed),
        target,
        "{workers} worker(s)"
    );
}

#[test]
fn starvation_branches_1_worker() {
    starvation_branches(1);
}

#[test]
fn starvation_branches_2_workers() {
    starvation_branches(2);
}

#[test]
fn starvation_branches_3_workers() {
    starvation_branches(3);
}

#[test]
fn starvation_branches_4_workers() {
    starvation_branches(4);
}

#[test]
fn starvation_branches_5_workers() {
    starvation_branches(5);
}

#[test]
fn starvation_branches_6_workers() {
    starvation_branches(6);
}

#[test]
fn starvation_branches_7_workers() {
    starvation_branches(7);
}

#[test]
fn starvation_branches_8_workers() {
    starvation_branches(8);
}

// ----------------------------------------------------------------------------
// Testcase: Starvation.BranchesWithPivot
// ----------------------------------------------------------------------------

/// Upstream's `starvation_branch_with_pivot(W)`.
///
/// A thousand-task chain, then a thousand branches of which about one in ten
/// counts and the rest spin -- except on one worker drawn at random, the
/// pivot, where even a spinning branch returns at once. So the run can only
/// end if the pivot worker keeps finding work: every other worker is liable
/// to be parked inside a branch that is waiting for increments the pivot has
/// not reached yet, and those increments sit in the queues of the very
/// workers that are blocked.
fn starvation_branch_with_pivot(workers: usize, seed: u32) {
    const N: usize = 1000;

    let executor = Arc::new(Executor::new(workers));
    let mut taskflow = Taskflow::new();
    let counter = shared_counter();

    // Upstream's `target`, which its task bodies capture by reference and read
    // at run time rather than at build time: it is still being incremented
    // while the graph is built, and what a blocker waits for is the final
    // value.
    let target = shared_counter();

    let mut rng = Xorshift32::new(seed);
    let pivot = usize::try_from(rng.next_u32()).expect("a u32 fits in a usize") % workers;

    let curr = counting_chain(&mut taskflow, &counter, N);

    for _ in 0..N {
        // Upstream's `rand() % 10`: wait with probability 0.9, count with
        // probability 0.1.
        let branch = if rng.next_u32().is_multiple_of(10) {
            target.fetch_add(1, Ordering::Relaxed);
            bump(&mut taskflow, &counter)
        } else {
            let executor = Arc::clone(&executor);
            let counter = Arc::clone(&counter);
            let target = Arc::clone(&target);
            taskflow.emplace(move || {
                if executor.this_worker_id() != Some(pivot) {
                    spin_until("the pivot worker to run every counting branch", || {
                        counter.load(Ordering::Relaxed) == target.load(Ordering::Relaxed) + N
                    });
                }
            })
        };
        taskflow.task(branch).succeed([curr]);
    }

    run(&executor, &taskflow);

    assert_eq!(
        counter.load(Ordering::Relaxed),
        target.load(Ordering::Relaxed) + N,
        "seed {seed}, {workers} worker(s), pivot {pivot}"
    );
}

#[test]
fn starvation_branch_with_pivot_1_worker() {
    starvation_branch_with_pivot(1, seed_for(1));
}

#[test]
fn starvation_branch_with_pivot_2_workers() {
    starvation_branch_with_pivot(2, seed_for(2));
}

#[test]
fn starvation_branch_with_pivot_3_workers() {
    starvation_branch_with_pivot(3, seed_for(3));
}

#[test]
fn starvation_branch_with_pivot_4_workers() {
    starvation_branch_with_pivot(4, seed_for(4));
}

#[test]
fn starvation_branch_with_pivot_5_workers() {
    starvation_branch_with_pivot(5, seed_for(5));
}

#[test]
fn starvation_branch_with_pivot_6_workers() {
    starvation_branch_with_pivot(6, seed_for(6));
}

#[test]
fn starvation_branch_with_pivot_7_workers() {
    starvation_branch_with_pivot(7, seed_for(7));
}

#[test]
fn starvation_branch_with_pivot_8_workers() {
    starvation_branch_with_pivot(8, seed_for(8));
}

// ----------------------------------------------------------------------------
// Testcase: Starvation.Loop
// ----------------------------------------------------------------------------

/// Upstream's `starvation_loop(W)`.
///
/// The same chain-then-fan-out, ten times round a cycle closed by a condition
/// task: `cond` precedes the chain's second task and the stop task, returns 0
/// nine times and 1 on the tenth, and `merge` joins all 1024 branches before
/// it. Every branch records its worker and then waits for all `W` of them to
/// arrive, so `merge`'s `set.count() == W` is not a statistical claim -- the
/// rendezvous cannot complete unless every worker is woken for every
/// iteration, after a hundred-task serial chain has given the pool every
/// chance to fall asleep.
fn starvation_loop(workers: usize) {
    const L: usize = 100;
    const B: usize = 1024;

    assert!(B > workers, "upstream's `REQUIRE(B > W)`");

    let executor = Arc::new(Executor::new(workers));
    let mut taskflow = Taskflow::new();

    let counter = shared_counter();
    let barrier = shared_counter();
    let involved = Involved::new();

    let merge = {
        let counter = Arc::clone(&counter);
        let barrier = Arc::clone(&barrier);
        let involved = Arc::clone(&involved);
        taskflow.emplace(move || {
            assert_eq!(barrier.load(Ordering::Relaxed), B, "{workers} worker(s)");
            assert_eq!(
                counter.load(Ordering::Relaxed),
                L + B - 1,
                "{workers} worker(s)"
            );
            assert_eq!(
                involved.count(),
                workers,
                "{workers} worker(s): every worker runs a branch"
            );
            counter.store(0, Ordering::Relaxed);
            barrier.store(0, Ordering::Relaxed);
            involved.clear();
        })
    };

    // Upstream's `[n=0]() mutable { return ++n >= 10 ? 1 : 0; }`.
    let cond = {
        let mut n = 0;
        taskflow.emplace_condition(move || {
            n += 1;
            i32::from(n >= 10)
        })
    };

    let stop = {
        let counter = Arc::clone(&counter);
        let barrier = Arc::clone(&barrier);
        let involved = Arc::clone(&involved);
        taskflow.emplace(move || {
            assert_eq!(barrier.load(Ordering::Relaxed), 0, "{workers} worker(s)");
            assert_eq!(counter.load(Ordering::Relaxed), 0, "{workers} worker(s)");
            assert_eq!(involved.count(), 0, "{workers} worker(s)");
        })
    };

    // The chain, whose first task does not count: it is the one task of the
    // chain the loop does not re-enter, so counting it would make the total
    // differ between the first iteration and the nine after it.
    let mut prev: Option<Task> = None;
    let mut second: Option<Task> = None;
    for l in 0..L {
        let curr = if l == 0 {
            taskflow.emplace(|| {})
        } else {
            bump(&mut taskflow, &counter)
        };
        if let Some(prev) = prev {
            taskflow.task(curr).succeed([prev]);
        }
        if l == 1 {
            second = Some(curr);
        }
        prev = Some(curr);
    }
    let curr = prev.expect("the chain has a hundred tasks");
    let second = second.expect("the chain has a second task");

    taskflow.task(cond).precede([second, stop]);

    for _ in 0..B {
        let executor = Arc::clone(&executor);
        let counter = Arc::clone(&counter);
        let barrier = Arc::clone(&barrier);
        let involved = Arc::clone(&involved);
        let branch = taskflow.emplace(move || {
            involved.record(&executor);

            barrier.fetch_add(1, Ordering::Relaxed);
            spin_until("every worker to reach the fan-out", || {
                barrier.load(Ordering::Relaxed) >= workers
            });

            counter.fetch_add(1, Ordering::Relaxed);
        });
        taskflow.task(branch).succeed([curr]).precede([merge]);
    }

    taskflow.task(merge).precede([cond]);

    run(&executor, &taskflow);
}

#[test]
fn starvation_loop_1_worker() {
    starvation_loop(1);
}

#[test]
fn starvation_loop_2_workers() {
    starvation_loop(2);
}

#[test]
fn starvation_loop_3_workers() {
    starvation_loop(3);
}

#[test]
fn starvation_loop_4_workers() {
    starvation_loop(4);
}

#[test]
fn starvation_loop_5_workers() {
    starvation_loop(5);
}

#[test]
fn starvation_loop_6_workers() {
    starvation_loop(6);
}

#[test]
fn starvation_loop_7_workers() {
    starvation_loop(7);
}

#[test]
fn starvation_loop_8_workers() {
    starvation_loop(8);
}

// ----------------------------------------------------------------------------
// Testcase: EmbarrasingStarvation
// ----------------------------------------------------------------------------

/// Upstream's `embarrasing_starvation(W)`, less its module task -- the file
/// header says what that costs and why it is not faked.
///
/// 65536 independent tasks, each recording its worker and then waiting for all
/// `W` of them to have arrived. There is no dependency anywhere, so the only
/// thing between a worker and the work is whether it was woken.
fn embarrasing_starvation(workers: usize) {
    const B: usize = 65536;

    assert!(B > workers, "upstream's `REQUIRE(B > W)`");

    let executor = Arc::new(Executor::new(workers));
    let mut taskflow = Taskflow::new();

    let barrier = shared_counter();
    let involved = Involved::new();

    for _ in 0..B {
        let executor = Arc::clone(&executor);
        let barrier = Arc::clone(&barrier);
        let involved = Arc::clone(&involved);
        taskflow.emplace(move || {
            involved.record(&executor);

            barrier.fetch_add(1, Ordering::Relaxed);
            spin_until("every worker to reach the fan-out", || {
                barrier.load(Ordering::Relaxed) >= workers
            });
        });
    }

    run(&executor, &taskflow);

    assert_eq!(
        involved.count(),
        workers,
        "{workers} worker(s): every worker runs a task"
    );
}

#[test]
fn embarrasing_starvation_1_worker() {
    embarrasing_starvation(1);
}

#[test]
fn embarrasing_starvation_2_workers() {
    embarrasing_starvation(2);
}

#[test]
fn embarrasing_starvation_3_workers() {
    embarrasing_starvation(3);
}

#[test]
fn embarrasing_starvation_4_workers() {
    embarrasing_starvation(4);
}

#[test]
fn embarrasing_starvation_5_workers() {
    embarrasing_starvation(5);
}

#[test]
fn embarrasing_starvation_6_workers() {
    embarrasing_starvation(6);
}

#[test]
fn embarrasing_starvation_7_workers() {
    embarrasing_starvation(7);
}

#[test]
fn embarrasing_starvation_8_workers() {
    embarrasing_starvation(8);
}

// ----------------------------------------------------------------------------
// Testcase: SkewedStarvation
// ----------------------------------------------------------------------------

/// Upstream's `skewed_starvation(W)`.
///
/// A chain of `W` levels, each forking into a `left` task that yields and a
/// `right` task that blocks its worker until the end of the run; `left` is the
/// parent of the next level, so the graph is as lopsided as a binary fork can
/// be. By the last level `W - 1` workers are blocked and exactly one is left
/// to reach the stop task, which releases them -- 1024 times over, since each
/// run has to assemble that rendezvous again from a pool that has just
/// finished one.
fn skewed_starvation(workers: usize, seed: u32) {
    let executor = Arc::new(Executor::new(workers));
    let mut taskflow = Taskflow::new();

    let stop = Arc::new(AtomicBool::new(false));
    let count = shared_counter();
    let involved = Involved::new();
    let mut rng = Xorshift32::new(seed);

    let mut parent = {
        let stop = Arc::clone(&stop);
        let count = Arc::clone(&count);
        let involved = Arc::clone(&involved);
        taskflow.emplace(move || {
            involved.clear();
            count.store(0, Ordering::Relaxed);
            stop.store(false, Ordering::Relaxed);
        })
    };
    taskflow.task(parent).name("root");

    for w in 0..workers {
        let right = {
            let executor = Arc::clone(&executor);
            let stop = Arc::clone(&stop);
            let count = Arc::clone(&count);
            let involved = Arc::clone(&involved);
            taskflow.emplace(move || {
                if w > 0 {
                    involved.record(&executor);
                    count.fetch_add(1, Ordering::Release);
                    spin_until("the stop task to release the blocked workers", || {
                        stop.load(Ordering::Relaxed)
                    });
                }
            })
        };
        taskflow.task(right).name(format!("right-{w}"));

        let left = taskflow.emplace(thread::yield_now);
        taskflow.task(left).name(format!("left-{w}"));

        // Upstream's `rand() & 1`: which of the two the parent names first
        // decides which one a worker runs itself and which one is left for a
        // thief, and the case is about the blocked side, not about that.
        if rng.next_u32() & 1 == 1 {
            taskflow.task(parent).precede([left, right]);
        } else {
            taskflow.task(parent).precede([right, left]);
        }

        parent = left;
    }

    let last = {
        let stop = Arc::clone(&stop);
        let count = Arc::clone(&count);
        let involved = Arc::clone(&involved);
        taskflow.emplace(move || {
            spin_until("the other W-1 workers to block", || {
                count.load(Ordering::Acquire) + 1 == workers
            });
            stop.store(true, Ordering::Relaxed);

            assert_eq!(
                involved.count() + 1,
                workers,
                "seed {seed}, {workers} worker(s): every other worker blocked on a right task"
            );
        })
    };
    taskflow.task(last).name("stop");
    taskflow.task(parent).precede([last]);

    run_n(&executor, &taskflow, 1024);
}

#[test]
fn skewed_starvation_1_worker() {
    skewed_starvation(1, seed_for(1));
}

#[test]
fn skewed_starvation_2_workers() {
    skewed_starvation(2, seed_for(2));
}

#[test]
fn skewed_starvation_3_workers() {
    skewed_starvation(3, seed_for(3));
}

#[test]
fn skewed_starvation_4_workers() {
    skewed_starvation(4, seed_for(4));
}

#[test]
fn skewed_starvation_5_workers() {
    skewed_starvation(5, seed_for(5));
}

#[test]
fn skewed_starvation_6_workers() {
    skewed_starvation(6, seed_for(6));
}

#[test]
fn skewed_starvation_7_workers() {
    skewed_starvation(7, seed_for(7));
}

#[test]
fn skewed_starvation_8_workers() {
    skewed_starvation(8, seed_for(8));
}

// ----------------------------------------------------------------------------
// Testcase: NAryStarvation
// ----------------------------------------------------------------------------

/// Upstream's `nary_starvation(W)`.
///
/// [`skewed_starvation`]'s shape widened: `W` levels of 1024 tasks each, one
/// task per level picked as the parent of the next and exactly one other --
/// never the parent -- blocking its worker for the rest of the run. Which one
/// is which is drawn per level, so the blocker is somewhere in the middle of a
/// thousand siblings rather than adjacent to the pivot, and a worker that only
/// looks at the top of its own queue finds the wrong end of the level.
fn nary_starvation(workers: usize, seed: u32) {
    const N: usize = 1024;

    let executor = Arc::new(Executor::new(workers));
    let mut taskflow = Taskflow::new();

    let stop = Arc::new(AtomicBool::new(false));
    let count = shared_counter();
    let involved = Involved::new();
    let mut rng = Xorshift32::new(seed);

    let mut parent = {
        let stop = Arc::clone(&stop);
        let count = Arc::clone(&count);
        let involved = Arc::clone(&involved);
        taskflow.emplace(move || {
            involved.clear();
            count.store(0, Ordering::Relaxed);
            stop.store(false, Ordering::Relaxed);
        })
    };
    taskflow.task(parent).name("root");

    for w in 0..workers {
        // Upstream's `size_t p = rand()%N`: the pivot of this level.
        let p = usize::try_from(rng.next_u32()).expect("a u32 fits in a usize") % N;
        let mut pivot: Option<Task> = None;

        for i in 0..N {
            let task = {
                let executor = Arc::clone(&executor);
                let stop = Arc::clone(&stop);
                let count = Arc::clone(&count);
                let involved = Arc::clone(&involved);
                taskflow.emplace(move || {
                    // The blocker cannot be the pivot; everyone else returns.
                    if i != (p + 1) % N {
                        thread::yield_now();
                        return;
                    }
                    if w > 0 {
                        involved.record(&executor);
                        count.fetch_add(1, Ordering::Release);
                        spin_until("the stop task to release the blocked workers", || {
                            stop.load(Ordering::Relaxed)
                        });
                    }
                })
            };
            taskflow.task(task).name(w.to_string());
            taskflow.task(parent).precede([task]);

            if p == i {
                pivot = Some(task);
            }
        }

        parent = pivot.expect("every level names a pivot");
    }

    let last = {
        let stop = Arc::clone(&stop);
        let count = Arc::clone(&count);
        let involved = Arc::clone(&involved);
        taskflow.emplace(move || {
            spin_until("the other W-1 workers to block", || {
                count.load(Ordering::Acquire) + 1 == workers
            });
            stop.store(true, Ordering::Relaxed);

            assert_eq!(
                involved.count() + 1,
                workers,
                "seed {seed}, {workers} worker(s): every other worker blocked on a level"
            );
        })
    };
    taskflow.task(last).name("stop");
    taskflow.task(parent).precede([last]);

    run_n(&executor, &taskflow, 5);
}

#[test]
fn nary_starvation_1_worker() {
    nary_starvation(1, seed_for(1));
}

#[test]
fn nary_starvation_2_workers() {
    nary_starvation(2, seed_for(2));
}

#[test]
fn nary_starvation_3_workers() {
    nary_starvation(3, seed_for(3));
}

#[test]
fn nary_starvation_4_workers() {
    nary_starvation(4, seed_for(4));
}

#[test]
fn nary_starvation_5_workers() {
    nary_starvation(5, seed_for(5));
}

#[test]
fn nary_starvation_6_workers() {
    nary_starvation(6, seed_for(6));
}

#[test]
fn nary_starvation_7_workers() {
    nary_starvation(7, seed_for(7));
}

#[test]
fn nary_starvation_8_workers() {
    nary_starvation(8, seed_for(8));
}

// ----------------------------------------------------------------------------
// Testcase: WavefrontStarvation
// ----------------------------------------------------------------------------

/// Upstream's `wavefront_starvation(W)`.
///
/// A `W` by `W` wavefront: every task precedes its right and lower neighbour,
/// so the anti-diagonal is the one generation that is `W` wide. Every task on
/// it but the top-right one blocks its worker; the top-right one waits for the
/// other `W - 1` to have blocked and then releases them. 1024 runs of it, so
/// the pool has to re-form that full-width generation a thousand times.
fn wavefront_starvation(workers: usize) {
    const RUNS: usize = 1024;

    let executor = Arc::new(Executor::new(workers));
    let mut taskflow = Taskflow::new();

    let stop = Arc::new(AtomicBool::new(false));
    let count = shared_counter();
    let blocked = shared_counter();
    let involved = Involved::new();

    let mut grid: Vec<Vec<Task>> = Vec::with_capacity(workers);
    for i in 0..workers {
        let mut row = Vec::with_capacity(workers);
        for j in 0..workers {
            let task = if i + j == 0 {
                // The source, which resets everything the run before it left.
                let stop = Arc::clone(&stop);
                let count = Arc::clone(&count);
                let blocked = Arc::clone(&blocked);
                let involved = Arc::clone(&involved);
                taskflow.emplace(move || {
                    count.fetch_add(1, Ordering::Relaxed);
                    stop.store(false, Ordering::Relaxed);
                    blocked.store(0, Ordering::Relaxed);
                    involved.clear();
                })
            } else if i + j + 1 == workers {
                let executor = Arc::clone(&executor);
                let stop = Arc::clone(&stop);
                let count = Arc::clone(&count);
                let blocked = Arc::clone(&blocked);
                let involved = Arc::clone(&involved);
                taskflow.emplace(move || {
                    count.fetch_add(1, Ordering::Relaxed);

                    if i == 0 && j + 1 == workers {
                        spin_until("the other W-1 workers to block", || {
                            blocked.load(Ordering::Acquire) + 1 == workers
                        });
                        stop.store(true, Ordering::Relaxed);

                        assert_eq!(
                            involved.count() + 1,
                            workers,
                            "{workers} worker(s): the whole anti-diagonal was spread"
                        );
                    } else {
                        involved.record(&executor);
                        blocked.fetch_add(1, Ordering::Release);
                        spin_until("the top-right task to release the diagonal", || {
                            stop.load(Ordering::Relaxed)
                        });
                    }
                })
            } else {
                bump(&mut taskflow, &count)
            };
            taskflow.task(task).name(format!("{i}, {j}"));
            row.push(task);
        }
        grid.push(row);
    }

    for i in 0..workers {
        for j in 0..workers {
            if i + 1 < workers {
                taskflow.task(grid[i][j]).precede([grid[i + 1][j]]);
            }
            if j + 1 < workers {
                taskflow.task(grid[i][j]).precede([grid[i][j + 1]]);
            }
        }
    }

    run_n(&executor, &taskflow, RUNS);

    assert_eq!(
        count.load(Ordering::Relaxed),
        workers * workers * RUNS,
        "{workers} worker(s)"
    );
}

#[test]
fn wavefront_starvation_1_worker() {
    wavefront_starvation(1);
}

#[test]
fn wavefront_starvation_2_workers() {
    wavefront_starvation(2);
}

#[test]
fn wavefront_starvation_3_workers() {
    wavefront_starvation(3);
}

#[test]
fn wavefront_starvation_4_workers() {
    wavefront_starvation(4);
}

#[test]
fn wavefront_starvation_5_workers() {
    wavefront_starvation(5);
}

#[test]
fn wavefront_starvation_6_workers() {
    wavefront_starvation(6);
}

#[test]
fn wavefront_starvation_7_workers() {
    wavefront_starvation(7);
}

#[test]
fn wavefront_starvation_8_workers() {
    wavefront_starvation(8);
}

// ----------------------------------------------------------------------------
// Testcase: Waiter
// ----------------------------------------------------------------------------

/// Upstream's `waiter(W)`, less its middle phase.
///
/// Three phases upstream, of which the second is the one this port cannot
/// run: it submits a closure with `executor.async(...)` and has it wait, from
/// inside the pool, until the other `W - 1` workers have gone to sleep. Async
/// tasks are milestone 5, and there is no other way to put a single closure on
/// a worker without a graph to hang it on, so that phase is absent rather than
/// imitated.
///
/// The two that are here are upstream's own, and they are what makes
/// `num_waiters` worth anything: an executor with nothing to do must park
/// every worker, a worker running a task must not be counted as parked, and a
/// task must be able to watch the rest of the pool fall asleep underneath it.
/// The crate's own docs call `num_waiters` a diagnostic and never a predicate,
/// which is exactly how upstream uses it here -- every assertion below is
/// about a pool that has already settled, not about a race.
fn waiter(workers: usize) {
    let executor = Arc::new(Executor::new(workers));

    spin_until("every worker to park", || executor.num_waiters() == workers);
    assert_eq!(
        executor.num_waiters(),
        workers,
        "an executor with nothing to do parks every worker"
    );

    let mut taskflow = Taskflow::new();

    for _ in 0..2048 {
        let executor = Arc::clone(&executor);
        taskflow.emplace(move || {
            assert!(
                executor.num_waiters() < workers,
                "{workers} worker(s): a worker running a task is not a waiter"
            );
        });
    }

    taskflow.emplace({
        let executor = Arc::clone(&executor);
        move || {
            spin_until("the other W-1 workers to park", || {
                executor.num_waiters() + 1 == workers
            });
        }
    });

    run(&executor, &taskflow);
}

#[test]
fn waiter_1_worker() {
    waiter(1);
}

#[test]
fn waiter_2_workers() {
    waiter(2);
}

#[test]
fn waiter_3_workers() {
    waiter(3);
}

#[test]
fn waiter_4_workers() {
    waiter(4);
}

#[test]
fn waiter_5_workers() {
    waiter(5);
}

#[test]
fn waiter_6_workers() {
    waiter(6);
}

#[test]
fn waiter_7_workers() {
    waiter(7);
}

#[test]
fn waiter_8_workers() {
    waiter(8);
}

//! Translated from `unittests/test_subflows.cpp` (Taskflow v4.1.0).
//!
//! Every case of that file this port can run is here. One family is not
//! ported at all -- `DetachedSubflow` and `TreeSubflow`, which are commented
//! out upstream because `Subflow::detach` is gone from v4, so they would be
//! ported from a dead header.
//!
//! What is here is `JoinedSubflow` (both subcases, at 1..=8 workers),
//! `FibSubflow` (at upstream's worker counts), the three `MultipleSubflowRuns`
//! families -- the interleaved one, `ChangedTaskflow` and `Predicate`, each at
//! 1..=8 and all three added by #54, which is where subflows and the run
//! family are driven together -- and `Subflow.BitState` (at 1..=4). None of
//! them is slow enough to be worth an `#[ignore]`: the whole file is a
//! quarter of a second in a debug build.
//!
//! Two cases of this port's own close the file. Both are about the lease: the
//! subgraph a body retained is still in the node after the run, which is what
//! a DOT dump of a subflow is for, and it is still in the *right* node when
//! the subflow task is one graph down, inside a composed taskflow or an
//! adopted graph.
//!
//! Four adaptations, none of which changes what is asserted, and the
//! `MultipleSubflowRuns` cases name two more of their own:
//!
//! - **`fb.reduce` becomes one static task.** Upstream's `Complex` subcase
//!   sums ten `1`s with `tf::Taskflow::reduce`, which is an algorithm rather
//!   than a scheduler feature and arrives with M6. One static task summing the
//!   same vector stands in for it, and the two assertions around it -- the sum
//!   is 0 before and 10 after -- are upstream's own and are what the case is
//!   really about: an intra-subflow dependency ordering two tasks the body
//!   built.
//! - **Shared state is an `Arc` of an atomic.** Upstream's bodies capture
//!   `[&]` and write locals of the enclosing function: a `std::vector<int>`,
//!   an `int sum`, two `std::atomic<int>` counters. A body here is
//!   `FnMut + Send + 'static`, so each one takes a share instead -- which also
//!   makes the single-threaded reads in upstream's assertions honest about
//!   being read from whichever worker got there.
//! - **An assertion inside a body is reported by `wait`.** Upstream's
//!   `REQUIRE` inside a subflow body aborts the test from the worker thread.
//!   Here a panicking body is caught and recorded as the run's failure, so the
//!   `expect` on `wait` is what prints it -- the message included, since the
//!   payload of a failed `assert!` is a `String`.
//! - **`fibonacci_spawn` returns through a share.** Upstream's recursion
//!   writes `int res1, res2` on the parent's stack frame, which a `'static`
//!   body cannot borrow; each level hands its children an `Arc<AtomicU64>`
//!   apiece and reads them after the join instead. The join is what makes
//!   that safe, and it is the point of the case.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::prelude::*;
use rustflow::{Executor, Node, RunHandle, SubflowWork, TaskMut};

/// A counter shared with the task bodies that bump it.
type Counter = Arc<AtomicUsize>;

/// A fresh counter.
fn counter() -> Counter {
    Arc::new(AtomicUsize::new(0))
}

/// Bumps `counter` and hands back what it was, which is how every ordering
/// assertion below is phrased.
fn bump(counter: &Counter) -> usize {
    counter.fetch_add(1, Ordering::SeqCst)
}

/// What `counter` has counted.
fn count(counter: &Counter) -> usize {
    counter.load(Ordering::SeqCst)
}

// ----------------------------------------------------------------------------
// JoinedSubflow
// ----------------------------------------------------------------------------

/// Upstream's `joined_subflow(W)` `Trivial` subcase: four tasks in a chain,
/// of which three build subgraphs -- one empty and explicitly joined, one
/// nesting two levels of subflow, and one with an intra-subgraph dependency
/// around a subflow of its own.
///
/// Every assertion in it is an ordering claim, and the implicit join is what
/// makes each one hold: a subflow node does not finish until its whole
/// subgraph has, so the next task of the chain sees every nested task
/// counted. `fu3v` counts all ten bodies, `fu3v_` the four that take part in
/// the third subflow's internal ordering, which is upstream's arithmetic.
#[test]
fn a_chain_of_nested_subflows_runs_in_order() {
    for workers in 1..=8 {
        let context = format!("{workers} worker(s)");
        let fu3v = counter();
        let fu3v_ = counter();

        let mut taskflow = Taskflow::new();

        // An empty subflow, joined explicitly: nothing to run, and the join
        // still answers.
        let subflow1 = taskflow.emplace_subflow({
            let fu3v = Arc::clone(&fu3v);
            move |subflow: &mut Subflow<'_>| {
                bump(&fu3v);
                subflow.join().expect("a fresh subflow is joinable");
            }
        });
        taskflow.task(subflow1).name("subflow1");

        // A subflow of a subflow of a subflow, each left to the implicit join
        // except the innermost, which joins itself.
        let subflow2 = taskflow.emplace_subflow({
            let fu3v = Arc::clone(&fu3v);
            move |subflow: &mut Subflow<'_>| {
                bump(&fu3v);
                let fu3v = Arc::clone(&fu3v);
                subflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
                    bump(&fu3v);
                    let fu3v = Arc::clone(&fu3v);
                    subflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
                        bump(&fu3v);
                        subflow.join().expect("a fresh subflow is joinable");
                    });
                });
            }
        });
        taskflow.task(subflow2).name("subflow2");

        let subflow3 = taskflow.emplace_subflow({
            let fu3v = Arc::clone(&fu3v);
            let fu3v_ = Arc::clone(&fu3v_);
            move |subflow: &mut Subflow<'_>| {
                // Upstream's `REQUIRE(fu3v == 4)`: subflow1, subflow2 and the
                // two subflows nested inside it have all finished.
                assert_eq!(count(&fu3v), 4, "the subflows before this one ran");
                bump(&fu3v);
                bump(&fu3v_);

                let subflow3_ = {
                    let fu3v = Arc::clone(&fu3v);
                    let fu3v_ = Arc::clone(&fu3v_);
                    subflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
                        // Its two predecessors inside this subgraph have run.
                        assert_eq!(count(&fu3v_), 3, "s1 and s2 ran first");
                        bump(&fu3v);
                        bump(&fu3v_);
                        subflow.join().expect("a fresh subflow is joinable");
                    })
                };
                let s1 = {
                    let fu3v = Arc::clone(&fu3v);
                    let fu3v_ = Arc::clone(&fu3v_);
                    subflow.emplace(move || {
                        bump(&fu3v_);
                        bump(&fu3v);
                    })
                };
                let s2 = {
                    let fu3v = Arc::clone(&fu3v);
                    let fu3v_ = Arc::clone(&fu3v_);
                    subflow.emplace(move || {
                        bump(&fu3v_);
                        bump(&fu3v);
                    })
                };
                let s3 = {
                    let fu3v = Arc::clone(&fu3v);
                    let fu3v_ = Arc::clone(&fu3v_);
                    subflow.emplace(move || {
                        bump(&fu3v);
                        assert_eq!(count(&fu3v_), 4, "the subflow before this one ran");
                    })
                };

                // `s1, s2 -> subflow3_ -> s3`, which is upstream's shape and
                // the whole of what the two assertions above rest on.
                subflow.linearize([s1, subflow3_]);
                subflow.linearize([s2, subflow3_]);
                subflow.linearize([subflow3_, s3]);

                assert_eq!(count(&fu3v_), 1, "nothing of the subgraph has run");
            }
        });
        taskflow.task(subflow3).name("subflow3");

        let subflow4 = taskflow.emplace({
            let fu3v = Arc::clone(&fu3v);
            move || {
                bump(&fu3v);
            }
        });
        taskflow.task(subflow4).name("subflow4");

        taskflow.linearize([subflow1, subflow2, subflow3, subflow4]);

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .unwrap_or_else(|error| panic!("{context}: {error}"));

        // Upstream asserts these two totals in its detached twin, and they
        // are the same ten bodies and four orderings here.
        assert_eq!(count(&fu3v), 10, "{context}");
        assert_eq!(count(&fu3v_), 4, "{context}");
    }
}

/// Upstream's `joined_subflow(W)` `Complex` subcase: one subflow task mixing
/// work it does itself, children that depend on each other, and a nested
/// subflow that depends on ten of its siblings.
///
/// `count == 32` and `sum == 10` at the end are upstream's, and between them
/// they pin both kinds of ordering a subflow has: the edges the body builds
/// inside its own subgraph, and the implicit join that makes the whole
/// subgraph finish before the node does.
#[test]
fn a_subflow_orders_the_work_it_builds_against_itself() {
    for workers in 1..=8 {
        let context = format!("{workers} worker(s)");
        let data: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
        let sum = Arc::new(AtomicU64::new(0));
        let counted = counter();

        let mut taskflow = Taskflow::new();
        let fill = taskflow.emplace({
            let data = Arc::clone(&data);
            move || {
                let mut data = data.lock().expect("uncontended");
                data.clear();
                data.extend(std::iter::repeat_n(1, 10));
            }
        });

        let spawner = taskflow.emplace_subflow({
            let data = Arc::clone(&data);
            let sum = Arc::clone(&sum);
            let counted = Arc::clone(&counted);
            move |subflow: &mut Subflow<'_>| {
                // Upstream's `fb.reduce(data.begin(), data.end(), sum, plus)`,
                // as one static task: `reduce` is M6, and what this case
                // needs of it is a task with a predecessor and a successor.
                let reduce = {
                    let data = Arc::clone(&data);
                    let sum = Arc::clone(&sum);
                    subflow.emplace(move || {
                        let total: i32 = data.lock().expect("uncontended").iter().sum();
                        sum.fetch_add(
                            u64::try_from(total).expect("ten ones sum to ten"),
                            Ordering::SeqCst,
                        );
                    })
                };
                let before = {
                    let sum = Arc::clone(&sum);
                    subflow.emplace(move || {
                        assert_eq!(sum.load(Ordering::SeqCst), 0, "before the sum");
                    })
                };
                let after = {
                    let sum = Arc::clone(&sum);
                    subflow.emplace(move || {
                        assert_eq!(sum.load(Ordering::SeqCst), 10, "after the sum");
                    })
                };
                subflow.linearize([before, reduce, after]);

                for _ in 0..10 {
                    bump(&counted);
                }

                // The nested subflow, which every one of the ten tasks below
                // precedes: it asserts the count it can only see once they
                // have all run, and then builds a chain of eleven more.
                let nested = {
                    let counted = Arc::clone(&counted);
                    subflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
                        assert_eq!(count(&counted), 20, "the ten siblings ran");
                        bump(&counted);

                        let mut previous = {
                            let counted = Arc::clone(&counted);
                            subflow.emplace(move || {
                                assert_eq!(bump(&counted), 21, "the chain starts at 21");
                            })
                        };
                        for i in 0..10 {
                            let next = {
                                let counted = Arc::clone(&counted);
                                subflow.emplace(move || {
                                    assert_eq!(bump(&counted), 22 + i, "the chain is serial");
                                })
                            };
                            subflow.linearize([previous, next]);
                            previous = next;
                        }
                    })
                };
                for _ in 0..10 {
                    let counted = Arc::clone(&counted);
                    let sibling = subflow.emplace(move || {
                        bump(&counted);
                    });
                    subflow.linearize([sibling, nested]);
                }
            }
        });
        taskflow.task(fill).precede([spawner]);

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .unwrap_or_else(|error| panic!("{context}: {error}"));

        assert_eq!(count(&counted), 32, "{context}");
        assert_eq!(sum.load(Ordering::SeqCst), 10, "{context}");
    }
}

// ----------------------------------------------------------------------------
// FibSubflow
// ----------------------------------------------------------------------------

/// Upstream's `fibonacci_spawn`: each level spawns two subflow tasks, joins
/// them explicitly and adds up what they left behind.
///
/// `joinable()` before and after the join is upstream's own assertion, and it
/// is the reason the recursion works at all: the join is what makes the two
/// children's results readable by the frame that spawned them.
fn fibonacci_spawn(n: u32, subflow: &mut Subflow<'_>) -> u64 {
    if n < 2 {
        return u64::from(n);
    }

    let left = Arc::new(AtomicU64::new(0));
    let right = Arc::new(AtomicU64::new(0));

    {
        let left = Arc::clone(&left);
        subflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
            left.store(fibonacci_spawn(n - 1, subflow), Ordering::SeqCst);
        });
    }
    {
        let right = Arc::clone(&right);
        subflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
            right.store(fibonacci_spawn(n - 2, subflow), Ordering::SeqCst);
        });
    }

    assert!(subflow.joinable(), "a fresh subflow is joinable");
    subflow.join().expect("a fresh subflow is joinable");
    assert!(
        !subflow.joinable(),
        "a joined subflow is not joinable again"
    );

    left.load(Ordering::SeqCst) + right.load(Ordering::SeqCst)
}

/// Upstream's `fibonacci(W)` at its own worker counts: `fib(20) == 6765`,
/// computed by a tree of some twenty thousand subflow tasks, each of which
/// coruns its own two children.
///
/// This is the case that would hang or blow the stack if a join did anything
/// but keep the joining worker in the work-stealing loop: at one worker,
/// every level of the recursion is waiting for a graph only that same worker
/// can run. Measured at 0.12 s for all seven worker counts together in a
/// debug build, so it stays in the default set.
#[test]
fn a_recursive_subflow_computes_fibonacci() {
    for workers in [1, 2, 4, 5, 6, 7, 8] {
        let result = Arc::new(AtomicU64::new(0));

        let mut taskflow = Taskflow::new();
        taskflow.emplace_subflow({
            let result = Arc::clone(&result);
            move |subflow: &mut Subflow<'_>| {
                result.store(fibonacci_spawn(20, subflow), Ordering::SeqCst);
            }
        });

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .unwrap_or_else(|error| panic!("{workers} worker(s): {error}"));

        assert_eq!(
            result.load(Ordering::SeqCst),
            6765,
            "{workers} worker(s): fib(20)"
        );
    }
}

// ----------------------------------------------------------------------------
// MultipleSubflowRuns
// ----------------------------------------------------------------------------

/// The four tasks all three `MultipleSubflowRuns` cases build: `A -> {B, C}`,
/// `{B, C} -> D`, where `B` is a subflow task that counts itself and spawns
/// three children of its own with `B1, B2 -> B3`.
///
/// Seven bodies run per run of it, which is the arithmetic behind every
/// assertion in the three cases below, and the three children are rebuilt from
/// scratch on every entry to `B` -- which is the point of running one
/// subflow-bearing taskflow a thousand times.
fn multiple_runs_diamond(taskflow: &mut Taskflow, counted: &Counter) -> (Task, Task, Task, Task) {
    let a = {
        let counted = Arc::clone(counted);
        taskflow.emplace(move || {
            bump(&counted);
        })
    };
    let b = {
        let counted = Arc::clone(counted);
        taskflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
            bump(&counted);
            let mut child = || {
                let counted = Arc::clone(&counted);
                subflow.emplace(move || {
                    bump(&counted);
                })
            };
            let b1 = child();
            let b2 = child();
            let b3 = child();
            subflow.linearize([b1, b3]);
            subflow.linearize([b2, b3]);
        })
    };
    let c = {
        let counted = Arc::clone(counted);
        taskflow.emplace(move || {
            bump(&counted);
        })
    };
    let d = {
        let counted = Arc::clone(counted);
        taskflow.emplace(move || {
            bump(&counted);
        })
    };

    taskflow.task(a).precede([b, c]);
    taskflow.task(b).precede([d]);
    taskflow.task(c).precede([d]);

    (a, b, c, d)
}

/// Upstream's `multiple_subflow_runs(W)`: a thousand runs of that diamond,
/// interleaved three ways, and `count == 7000` at the end.
///
/// Four hundred and ninety-nine runs are issued without waiting -- every
/// other one of them with an on-finish callback that asserts the running
/// total -- then a five hundredth is issued and waited for, which is
/// upstream's "synchronise the first 500 runs", and five hundred more are
/// asked for in one `run_n`. The callbacks
/// are what make it more than a stress test: `count == (i+1)*7` inside the
/// callback of the `i`-th run can only hold if runs of one taskflow are
/// serialised and a callback sees the whole of its own run and none of the
/// next, which is the contract a queued re-run has.
///
/// Two adaptations, neither of which changes what is asserted:
///
/// - **`fu.valid()` and `fu.wait_for(1s) == ready` have no analogue.** They
///   are `std::future`'s own surface -- a moved-from future is invalid, and a
///   ready one answers without blocking -- where a
///   [`RunHandle`](rustflow::RunHandle) is a handle to a run and is never in
///   either state. What upstream is really asserting is that every one of the
///   five hundred runs is over by the time `wait_for_all` returns, and this
///   port says that with `num_topologies() == 0`, which
///   [`RunHandle::wait`](rustflow::RunHandle::wait) documents as the stronger
///   claim: a run that is accounted for cannot block a `wait_for_all`.
/// - **The handles are drained afterwards rather than dropped.** A dropped
///   handle discards the run's result, and these runs carry assertions in
///   their callbacks, so each handle is waited for in turn -- after
///   `wait_for_all`, so the waiting is upstream's and the `wait` is only how
///   a failed callback is reported.
fn multiple_subflow_runs(workers: usize) {
    let context = format!("{workers} worker(s)");
    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();
    let counted = counter();
    multiple_runs_diamond(&mut taskflow, &counted);

    let mut handles: Vec<RunHandle<'_>> = Vec::with_capacity(500);
    for i in 0..500 {
        if i == 499 {
            // Upstream's `run(taskflow).get()` -- the five hundredth run, and
            // the barrier the four hundred and ninety-nine before it are
            // queued behind -- then five hundred more.
            executor
                .run(&taskflow)
                .wait()
                .unwrap_or_else(|error| panic!("{context}: {error}"));
            handles.push(executor.run_n(&taskflow, 500));
        } else if i % 2 == 1 {
            handles.push(executor.run(&taskflow));
        } else {
            let counted = Arc::clone(&counted);
            handles.push(executor.run_with(&taskflow, move || {
                assert_eq!(
                    count(&counted),
                    (i + 1) * 7,
                    "{workers} worker(s): run {i} sees its own seven tasks and no more"
                );
            }));
        }
    }

    executor.wait_for_all();
    assert_eq!(
        executor.num_topologies(),
        0,
        "{context}: every run is accounted for"
    );

    for (index, handle) in handles.into_iter().enumerate() {
        handle
            .wait()
            .unwrap_or_else(|error| panic!("{context}: handle {index}: {error}"));
    }

    assert_eq!(count(&counted), 7000, "{context}");
}

/// Upstream's eight `MultipleSubflowRuns` cases, which are one helper at one
/// through eight workers.
#[test]
fn interleaved_runs_of_a_subflow_bearing_taskflow_count_every_body() {
    for workers in 1..=8 {
        multiple_subflow_runs(workers);
    }
}

// ----------------------------------------------------------------------------
// MultipleSubflowRuns.ChangedTaskflow
// ----------------------------------------------------------------------------

/// Upstream's `multiple_subflow_runs_with_changed_taskflow(W)`: the same
/// diamond, run ten times, then edited and run ten times again, twice over.
///
/// The two tasks appended between the runs do nothing, so the count goes up by
/// seventy each time whatever is added -- which is the assertion: a taskflow
/// that grew a node between runs is re-run from a graph of the new size, with
/// the subflow node's own state reset along with everything else. The last of
/// the three runs is the one upstream leaves to `wait_for_all`, and so does
/// this.
fn multiple_subflow_runs_with_changed_taskflow(workers: usize) {
    let context = format!("{workers} worker(s)");
    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();
    let counted = counter();
    let (_, _, _, d) = multiple_runs_diamond(&mut taskflow, &counted);

    executor
        .run_n(&taskflow, 10)
        .wait()
        .unwrap_or_else(|error| panic!("{context}: {error}"));
    assert_eq!(count(&counted), 70, "{context}: ten runs of seven tasks");

    let e = taskflow.emplace(|| {});
    taskflow.task(d).precede([e]);
    executor
        .run_n(&taskflow, 10)
        .wait()
        .unwrap_or_else(|error| panic!("{context}: {error}"));
    assert_eq!(count(&counted), 140, "{context}: ten more");

    let f = taskflow.emplace(|| {});
    taskflow.task(e).precede([f]);
    let handle = executor.run_n(&taskflow, 10);
    executor.wait_for_all();
    handle
        .wait()
        .unwrap_or_else(|error| panic!("{context}: {error}"));
    assert_eq!(count(&counted), 210, "{context}: ten more again");
}

/// Upstream's eight `MultipleSubflowRuns.ChangedTaskflow` cases.
#[test]
fn a_subflow_bearing_taskflow_edited_between_runs_runs_at_its_new_size() {
    for workers in 1..=8 {
        multiple_subflow_runs_with_changed_taskflow(workers);
    }
}

// ----------------------------------------------------------------------------
// MultipleSubflowRuns.Predicate
// ----------------------------------------------------------------------------

/// Upstream's `[run=10]() mutable { return run-- == 0; }`.
///
/// Asked once before the first run and once after each, so it ends the run
/// after ten of them -- the eleventh call is the one that compares zero.
fn ten_runs() -> impl FnMut() -> bool + Send + 'static {
    let mut remaining = 10_i32;
    move || {
        let compared = remaining;
        remaining -= 1;
        compared == 0
    }
}

/// Upstream's on-finish callback: seventy bodies ran, and the counter goes
/// back to zero for the next `run_until`.
fn seventy_then_reset(counted: &Counter, context: String) -> impl FnOnce() + Send + 'static {
    let counted = Arc::clone(counted);
    move || {
        assert_eq!(count(&counted), 70, "{context}: ten runs of seven tasks");
        counted.store(0, Ordering::SeqCst);
    }
}

/// Upstream's `multiple_subflow_runs_with_predicate(W)`: three `run_until`s
/// over the same diamond, each stopped by a ten-run predicate and each
/// resetting the counter from its callback.
///
/// What the three in a row are for is the predicate's own state: the second
/// and third `run_until` get a fresh one, so a port that left a run counter on
/// the topology rather than in the predicate would stop short or not at all.
/// The callback's `count == 70` is the same assertion three times and it is
/// upstream's.
///
/// One adaptation: upstream drops the middle handle on the floor and lets its
/// per-taskflow queue line the three up. Dropping a handle here waits too, but
/// it also discards the run's result -- and this run's assertion is *in* its
/// callback -- so the middle one is waited for like the other two. The three
/// are serialised either way.
fn multiple_subflow_runs_with_predicate(workers: usize) {
    let context = format!("{workers} worker(s)");
    let executor = Executor::new(workers);
    let mut taskflow = Taskflow::new();
    let counted = counter();
    multiple_runs_diamond(&mut taskflow, &counted);

    for round in 0..3 {
        let context = format!("{context}: round {round}");
        executor
            .run_until_with(
                &taskflow,
                ten_runs(),
                seventy_then_reset(&counted, context.clone()),
            )
            .wait()
            .unwrap_or_else(|error| panic!("{context}: {error}"));
        assert_eq!(count(&counted), 0, "{context}: the callback reset it");
    }
}

/// Upstream's eight `MultipleSubflowRuns.Predicate` cases.
#[test]
fn a_predicate_stops_a_run_of_a_subflow_bearing_taskflow_after_ten() {
    for workers in 1..=8 {
        multiple_subflow_runs_with_predicate(workers);
    }
}

// ----------------------------------------------------------------------------
// Subflow.BitState
// ----------------------------------------------------------------------------

/// Upstream's `bit_state(W)`: a conditioner sends the run back round to the
/// same subflow task a hundred times, and every entry has to find the node's
/// two subflow bits clear however the entry before it left them.
///
/// The body leaves both set -- it joins, which sets `JOINED_SUBFLOW`, and then
/// asks to retain, which sets `RETAIN_SUBFLOW` -- so a `Subflow` that did not
/// reset them would see a joined, retained subflow on its second entry, and
/// the two assertions inside the body are the whole case. The node's own word
/// is checked afterwards too, because the join counter shares it: a state bit
/// re-armed as a dependency count would hang the next run rather than fail
/// this one.
#[test]
fn a_re_entered_subflow_starts_from_clean_bits() {
    for workers in 1..=4 {
        let context = format!("{workers} worker(s)");
        let entries = counter();

        let mut taskflow = Taskflow::new();
        let init = taskflow.emplace(|| {});
        let spawner = taskflow.emplace_subflow({
            let entries = Arc::clone(&entries);
            move |subflow: &mut Subflow<'_>| {
                assert!(subflow.joinable(), "a fresh subflow is joinable");
                assert!(!subflow.retain(), "a fresh subflow retains nothing");
                bump(&entries);
                subflow.join().expect("a fresh subflow is joinable");
                subflow.set_retain(true);
            }
        });
        let loop_back = taskflow.emplace_condition({
            let mut round = 0;
            move || {
                round += 1;
                i32::from(round > 100)
            }
        });

        taskflow.linearize([init, spawner, loop_back]);
        // The conditioner's position 0 is the subflow task again, which is
        // the cycle; position 1 is nothing, which ends the run.
        taskflow.task(loop_back).precede([spawner]);

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .unwrap_or_else(|error| panic!("{context}: {error}"));

        assert_eq!(count(&entries), 101, "{context}: entries to the subflow");

        // The last entry's bits are still there -- nothing clears them but
        // the next entry -- and the count field of the word is the one strong
        // dependency the node has.
        let nstate = taskflow
            .graph()
            .node(spawner)
            .nstate()
            .load(Ordering::SeqCst);
        assert_eq!(
            nstate & Node::STRONG_DEPENDENCIES_MASK,
            1,
            "{context}: one strong dependency, `init`"
        );
        assert_eq!(nstate & Node::PREEMPTED, 0, "{context}: not preempted");
        assert_eq!(
            nstate & (Node::JOINED_SUBFLOW | Node::RETAIN_SUBFLOW),
            Node::JOINED_SUBFLOW | Node::RETAIN_SUBFLOW,
            "{context}: the last entry's own bits"
        );
    }
}

// ----------------------------------------------------------------------------
// The retained subgraph, which is this port's own case
// ----------------------------------------------------------------------------

/// A subgraph the body retained is in the node after the run, and a dump
/// shows it as a cluster; a subgraph it did not retain is gone.
///
/// Upstream's own `dump` documentation is where this comes from -- "a subflow
/// is cleared when it is joined, so `retain(true)` it if you mean to dump it"
/// -- and it is the acceptance case for the lease: the graph a run of the
/// subgraph took out of the node has to go back in, and only if the body asked
/// for it. Both halves are checked for the implicit join and for an explicit
/// one, because the graph goes home by a different route in each.
#[test]
fn a_retained_subgraph_survives_the_run_and_dumps() {
    for retain in [false, true] {
        for joined in [false, true] {
            let context = format!("retain {retain}, explicit join {joined}");

            let mut taskflow = Taskflow::with_name("dynamic");
            let spawner = taskflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
                let child = subflow.emplace(|| {});
                TaskMut::new(subflow.graph_mut(), child).name("child");
                if joined {
                    subflow.join().expect("a fresh subflow is joinable");
                }
                subflow.set_retain(retain);
            });
            taskflow.task(spawner).name("spawner");

            // Nothing to dump before the run: the body has not run, so the
            // subgraph does not exist yet.
            assert!(
                !taskflow.dump().contains("Subflow: spawner"),
                "{context}: before the run"
            );

            Executor::new(2)
                .run(&taskflow)
                .wait()
                .unwrap_or_else(|error| panic!("{context}: {error}"));

            let dot = taskflow.dump();
            assert_eq!(
                dot.contains("Subflow: spawner"),
                retain,
                "{context}: the cluster after the run"
            );
            assert_eq!(
                dot.contains("label=\"child\""),
                retain,
                "{context}: the child after the run"
            );
        }
    }
}

/// A subflow task one graph down: inside a composed taskflow, and inside an
/// adopted graph.
///
/// Upstream has this as the commented-out second half of
/// `Exception.ModuleTask` and nowhere else, and it is worth a case of its own
/// here because the lease resolves its way home through whatever owns the
/// graph the subflow node lives in -- a share of a taskflow's storage for the
/// composed form, a path through the composing node for the adopted one. A
/// retained subgraph is how that resolution is observed: the graph has to go
/// back into the node the dump finds, not into some other arena's node at the
/// same index.
#[test]
fn a_subflow_runs_and_retains_one_graph_down() {
    for workers in 1..=4 {
        let context = format!("{workers} worker(s)");
        let counted = counter();

        // The composed form: a taskflow whose only task is a subflow task,
        // reached through a module node of another taskflow.
        let mut target = Taskflow::with_name("target");
        let spawner = target.emplace_subflow({
            let counted = Arc::clone(&counted);
            move |subflow: &mut Subflow<'_>| {
                let counted = Arc::clone(&counted);
                let child = subflow.emplace(move || {
                    bump(&counted);
                });
                TaskMut::new(subflow.graph_mut(), child).name("child");
                subflow.set_retain(true);
            }
        });
        target.task(spawner).name("spawner");
        let target = Arc::new(target);

        let mut taskflow = Taskflow::new();
        taskflow.composed_of(Arc::clone(&target));

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .unwrap_or_else(|error| panic!("{context}: composed: {error}"));

        assert_eq!(count(&counted), 1, "{context}: composed");
        assert!(
            target.dump().contains("Subflow: spawner"),
            "{context}: composed: the retained subgraph is in the target's node"
        );

        // The adopted form: the same subflow task in a graph the module node
        // owns outright, which the lease reaches by a path rather than a
        // share.
        let mut adopted = Graph::new();
        adopted.add_named_node(
            "spawner",
            Handle::Subflow {
                work: SubflowWork::new({
                    let counted = Arc::clone(&counted);
                    move |subflow: &mut Subflow<'_>| {
                        let counted = Arc::clone(&counted);
                        let child = subflow.emplace(move || {
                            bump(&counted);
                        });
                        TaskMut::new(subflow.graph_mut(), child).name("child");
                        subflow.set_retain(true);
                    }
                }),
                subgraph: Mutex::new(Graph::new()),
            },
        );

        let mut taskflow = Taskflow::with_name("adopting");
        taskflow.adopt(adopted);

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .unwrap_or_else(|error| panic!("{context}: adopted: {error}"));

        assert_eq!(count(&counted), 2, "{context}: adopted");
        let dot = taskflow.dump();
        assert!(
            dot.contains("Subflow: spawner"),
            "{context}: adopted: the retained subgraph is in the adopted node"
        );
        assert!(
            dot.contains("label=\"child\""),
            "{context}: adopted: and the child it built is in it"
        );
    }
}

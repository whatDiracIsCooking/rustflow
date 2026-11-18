//! Acceptance cases for [`Runtime::scope`](rustflow::Runtime::scope), issue
//! #164.
//!
//! No `//!` provenance line: `Runtime::scope` is this port's own addition,
//! with no upstream case to translate. These would otherwise sit in
//! `tests/runtimes.rs`, but that suite is owned by a sibling change landing
//! in parallel with this one, so the cases live here instead, under the
//! same three questions the issue's "Acceptance" section asks: a scope whose
//! children borrow a slice rather than own it, a child's panic against the
//! sibling it still joined -- by running it or, issue #237, by dropping it
//! unstarted -- and a cancelled run's unstarted children.

use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use rustflow::Executor;
use rustflow::prelude::*;

#[path = "common/cancel.rs"]
mod cancel;
#[path = "common/panics.rs"]
mod panics;
#[path = "common/spin.rs"]
mod spin;

use cancel::spin_until_cancelled;
use panics::assert_panicked_with;
use spin::spin_until;

/// A value whose [`Drop`] counts itself, standing in for whatever a scoped
/// child's closure captured: what the cancellation case below needs to see
/// torn down even though the body it would have run never does.
struct DropGuard(Arc<AtomicUsize>);

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Release);
    }
}

/// A scope whose two children write disjoint halves of a slice borrowed from
/// the runtime task's own frame, which is the whole point of `Scope` over
/// [`Runtime::silent_async`](rustflow::Runtime::silent_async): the array
/// lives on the task's own stack, never behind an `Arc`, and both children
/// are joined before the task reads it back.
#[test]
fn a_scope_joins_children_that_write_disjoint_halves_of_a_borrowed_slice() {
    for workers in 1..=4 {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut data = [0u64; 10];

            runtime.scope(|s| {
                let (left, right) = data.split_at_mut(5);
                s.spawn(|| {
                    for slot in left.iter_mut() {
                        *slot = 1;
                    }
                });
                s.spawn(|| {
                    for slot in right.iter_mut() {
                        *slot = 2;
                    }
                });
            });

            let sum: u64 = data.iter().sum();
            assert_eq!(
                sum,
                5 + 10,
                "{workers} worker(s): both halves were written before scope returned"
            );
        });

        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }
}

/// A child that panics is the scope's own failure, reaching the run exactly
/// as a plain task's would -- but only after the scope has joined every
/// sibling. "Joined" is #164's contract: run to completion, *or dropped
/// unstarted* -- a child cancellation never starts is the second, not a
/// missing join -- so this pins both outcomes a sibling may actually take,
/// via the same `DropGuard` instrument
/// `a_cancelled_run_drops_every_unstarted_child_before_scope_returns` below
/// uses for an explicitly cancelled run.
///
/// Which worker picks up which child first matters a great deal, issue
/// #237: on a core-starved runner the panicking child can be dispatched,
/// fail, and have that failure recorded on the ancestor chain before any
/// worker ever dispatches the sibling, which then takes the identical
/// cancellation early-out
/// `a_cancelled_run_drops_every_unstarted_child_before_scope_returns` pins
/// for an explicit `cancel` -- the sibling's closure is dropped rather than
/// run, and the drop still counts toward the join `Runtime::scope` waits
/// for. `Runtime::scope`'s own `catch_unwind` is reused here, around the
/// call this case makes, purely so this body can read the drop count on the
/// very next line, while the scope frame has unquestionably already
/// returned -- a check made right after `.wait()` cannot tell "joined before
/// `scope` returned" apart from "joined eventually", since nothing else
/// prevents a `Runtime::scope` that skipped its own join from leaning on the
/// same implicit join every runtime task's body gets for free at the end of
/// its node.
#[test]
fn a_child_panic_reaches_the_run_after_the_scope_has_joined_its_sibling() {
    for workers in 1..=4 {
        let ran = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let joined_before_panic_reraised = Arc::new(AtomicBool::new(false));

        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime({
            let ran = Arc::clone(&ran);
            let dropped = Arc::clone(&dropped);
            let joined_before_panic_reraised = Arc::clone(&joined_before_panic_reraised);
            move |runtime: &mut Runtime<'_>| {
                let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
                    runtime.scope(|s| {
                        s.spawn(|| panic!("the child's own failure"));
                        s.spawn({
                            let ran = Arc::clone(&ran);
                            let guard = DropGuard(Arc::clone(&dropped));
                            move || {
                                ran.fetch_add(1, Ordering::Release);
                                drop(guard);
                            }
                        });
                    });
                }));
                // Whichever of the two outcomes the sibling took, its
                // `DropGuard` has fired by now: once immediately, explicitly,
                // on the path where the body ran, or once when the job
                // holding the never-called closure was dropped instead. The
                // payload this call re-raises keeps its identity regardless.
                joined_before_panic_reraised
                    .store(dropped.load(Ordering::Acquire) == 1, Ordering::Release);
                if let Err(payload) = outcome {
                    panic::resume_unwind(payload);
                }
            }
        });

        let error = Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect_err("the child's panic reaches the run");

        assert_panicked_with(error, "the child's own failure");
        assert!(
            joined_before_panic_reraised.load(Ordering::Acquire),
            "{workers} worker(s): the sibling was neither run nor dropped \
             before the panic was re-raised"
        );
    }
}

/// The drop half of the case above, forced rather than left to the race CI
/// found it by: one worker for two children, and the sibling's `spawn` call
/// made *first* in source order so that it is pushed first onto that one
/// worker's queue. `src/executor/queue.rs`'s own docs have the rest: the
/// owner pops its *own* queue in LIFO order, so the panicking child --
/// pushed second, landing on top -- is the one this single worker dispatches
/// first, every time, with no race against a thief there is no second
/// worker to be. By the time that panic is recorded, the sibling has not
/// been dispatched at all, and takes the cancellation early-out
/// `executor::worker`'s dispatch already has for an explicitly cancelled run
/// -- the same path
/// `a_cancelled_run_drops_every_unstarted_child_before_scope_returns` pins,
/// reached here through a sibling's panic instead of a `cancel` call, which
/// is the one distinction issue #237 asks this file to cover.
#[test]
fn a_sibling_is_dropped_not_run_when_one_worker_favours_its_panicking_twin() {
    let ran = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let joined_before_panic_reraised = Arc::new(AtomicBool::new(false));

    let mut taskflow = Taskflow::new();
    taskflow.emplace_runtime({
        let ran = Arc::clone(&ran);
        let dropped = Arc::clone(&dropped);
        let joined_before_panic_reraised = Arc::clone(&joined_before_panic_reraised);
        move |runtime: &mut Runtime<'_>| {
            let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
                runtime.scope(|s| {
                    // Pushed first: on the one worker below, this is the job
                    // left at the *bottom* of that worker's LIFO queue.
                    s.spawn({
                        let ran = Arc::clone(&ran);
                        let guard = DropGuard(Arc::clone(&dropped));
                        move || {
                            ran.fetch_add(1, Ordering::Release);
                            drop(guard);
                        }
                    });
                    // Pushed second, so it is the one this worker's own
                    // `pop()` returns first.
                    s.spawn(|| panic!("the child's own failure"));
                });
            }));
            joined_before_panic_reraised
                .store(dropped.load(Ordering::Acquire) == 1, Ordering::Release);
            if let Err(payload) = outcome {
                panic::resume_unwind(payload);
            }
        }
    });

    let error = Executor::new(1)
        .run(&taskflow)
        .wait()
        .expect_err("the child's panic reaches the run");
    assert_panicked_with(error, "the child's own failure");

    assert_eq!(
        ran.load(Ordering::Acquire),
        0,
        "the one worker reaches the panicking child first by construction, \
         so the sibling is never dispatched at all"
    );
    assert_eq!(
        dropped.load(Ordering::Acquire),
        1,
        "the sibling's closure is dropped unstarted all the same"
    );
    assert!(
        joined_before_panic_reraised.load(Ordering::Acquire),
        "the drop happened before the panic was re-raised"
    );
}

/// A run cancelled before `Runtime::scope` is even entered drops every child
/// it spawns rather than running any of them, and does so *before* `scope`
/// returns: the one worker running the runtime task's body reads the drop
/// count on the very next line.
///
/// The handshake cancels the run first and only then calls `scope`, which is
/// what makes every child here "unstarted" by construction rather than by a
/// race against however many workers happen to be free -- the same
/// determinism `tests/runtimes.rs`'s own cancellation case relies on.
#[test]
fn a_cancelled_run_drops_every_unstarted_child_before_scope_returns() {
    const CHILDREN: usize = 64;

    for workers in 1..=4 {
        let reached = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::new(AtomicBool::new(false));
        let ran = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));

        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime({
            let reached = Arc::clone(&reached);
            let cancelled = Arc::clone(&cancelled);
            let ran = Arc::clone(&ran);
            let dropped = Arc::clone(&dropped);
            move |runtime: &mut Runtime<'_>| {
                reached.store(true, Ordering::Release);

                // The cancel has to arrive *after* the body has started and
                // *before* `scope` is entered, so every child below is
                // spawned under a run already cancelled.
                spin_until_cancelled(&cancelled, || runtime.is_cancelled());

                runtime.scope(|s| {
                    for _ in 0..CHILDREN {
                        let ran = Arc::clone(&ran);
                        let guard = DropGuard(Arc::clone(&dropped));
                        s.spawn(move || {
                            // Never reached: the job this closure belongs to
                            // takes the cancellation early-out before this
                            // body is ever called, which is the one thing
                            // this case exists to pin.
                            ran.fetch_add(1, Ordering::Release);
                            drop(guard);
                        });
                    }
                });

                assert_eq!(
                    dropped.load(Ordering::Acquire),
                    CHILDREN,
                    "every spawned child was dropped by the time scope \
                     returned, with scope itself still on the stack"
                );
            }
        });

        let executor = Executor::new(workers);
        let run = executor.run(&taskflow);
        spin_until("the runtime body to start", || {
            reached.load(Ordering::Acquire)
        });
        assert!(run.cancel(), "the run had not finished");

        // A cancelled run completes normally, as `tests/runtimes.rs`'s own
        // cancellation case documents.
        run.wait().expect("a cancelled run is not a failed one");
        assert_eq!(
            ran.load(Ordering::Relaxed),
            0,
            "{workers} worker(s): no child ran once the run was cancelled"
        );
        assert_eq!(
            dropped.load(Ordering::Relaxed),
            CHILDREN,
            "{workers} worker(s): every child was dropped all the same"
        );
    }
}

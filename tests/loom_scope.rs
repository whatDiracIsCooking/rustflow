//! A [`loom`](https://docs.rs/loom) model of the rule `src/executor/scope.rs`
//! states: **`Runtime::scope` does not return until every child it spawned
//! has been joined -- run to completion, panicked, or dropped unstarted --
//! on every path out of it.** Issue #164 is where `Scope` got this model,
//! alongside the `unsafe` erasure the rule is what makes sound.
//!
//! **This file is not part of the gate**, for `tests/loom_notifier.rs`'s
//! reason: loom needs a `cfg(loom)` build of the whole graph, which only
//! `RUSTFLAGS` sets, and the cases below are empty without it. The command is
//! the notifier model's own, and every model under `tests/` builds in one go:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=/target/rustflow-loom \
//!     cargo test --release --test loom_scope
//! ```
//!
//! `.github/workflows/loom.yml` finds and runs it weekly and on
//! `workflow_dispatch`, alongside every other `tests/loom_*.rs`, which is the
//! only thing watching this model outside a deliberate local run (#128).
//!
//! # What is modelled, and what is not
//!
//! The model is **standalone**, in `tests/loom_storage.rs`'s sense: it does
//! not drive `Scope` or `Runtime`, it restates the one property that makes
//! the erasure in `Scope::spawn` sound in a few dozen lines of loom types. A
//! borrow a child closure captures is stood in for by [`Borrow`] -- an
//! `alive` flag the "frame" that opened the scope flips to `false` the
//! instant it gets control back, which in the real crate is the caller's very
//! next line after `Runtime::scope` returns, since nothing else marks when a
//! stack local's true lifetime ends. A child's [`access`] is the one load of
//! that flag an erased closure's body would make, asserting it is still
//! `true`: a child that ever sees `false` ran -- or even just read the flag
//! -- after the data it borrowed may already have been dropped or
//! overwritten, which is the use-after-free the real erasure risks if nothing
//! joins before `scope` returns.
//!
//! [`run_child`] carries the three paths `src/executor/scope.rs`'s module
//! docs name, as one function whose [`Outcome`] picks the ending: a child
//! that completes normally, one that panics after it has already touched the
//! borrow, and one that is "cancelled" -- dropped before its body ever runs.
//! That third child never *calls* anything that touches the borrow, but its
//! **drop does**: a closure that captured a guard runs that guard's `Drop`
//! when the never-called closure is dropped, and a `Drop` may write through
//! the borrow as freely as the body would have. So the cancelled arm's one
//! [`access`] stands for the drop, and it has to come *before* the arm sets
//! `done` -- the same `done` flag a real job's early-out still sets, and the
//! order issue #416 found the real early-out getting wrong: it counted the
//! job off first and let the node's recycling drop the body afterwards, one
//! `recycle` after `Runtime::scope` had already been told every child was
//! joined. The one thing that must hold across all three paths is
//! [`one_scope`]'s `join` knob: when it waits for `done` before flipping
//! `alive`, no child -- on any of the three paths -- can ever observe
//! `false`.
//!
//! The model earns its keep by **mutation**, as `tests/loom_storage.rs` and
//! `tests/loom_anchor.rs` do: alongside the join the crate ships, it carries
//! the one upstream-shaped alternative that this port rejected -- `scope`
//! returning as soon as `f` does, upstream's own `spawn`/`silent_async`
//! detaching rather than joining -- and asserts that loom *finds* it, on
//! every one of the three paths that could hide a mutation that only broke
//! one of them. It carries a second mutation as well, #416's own:
//! [`Outcome::CancelledCountedFirst`] is the cancelled path with the drop
//! moved after the count, and loom has to find that one *with the join in
//! place*, since the join is exactly what that order defeats. Before #416 the
//! cancelled arm made no access at all, which encoded the belief that a
//! never-called closure cannot touch its borrow; a model built on that
//! belief passed against the broken order and would have passed against any
//! order, which is why it had no cancelled-path mutation to fail.
//!
//! [`one_scope`] is one child and one flag, and the real `scope` waits on a
//! **number**: issue #423 asked what else could let a frame past that wait,
//! and the answer no one-child model can express is a predicate that reads
//! the number wrong. So [`two_children`] is the same property over
//! `Runtime::try_corun`'s own shape of signal -- a counter raised once per
//! child and counted off once per child's teardown, which the frame waits to
//! see back at the bottom -- and it carries two mutations of its own. One is
//! #416's order again, on *one* of two children, where the sibling's honest
//! count is what hides it: loom has to find that a counter reaching bottom is
//! not the same claim as every body being dropped. The other is a
//! [`Join::SomeChild`] frame, which goes on as soon as the counter has moved
//! at all -- the shape a predicate off by one child would have -- and is the
//! one ordering the flag model cannot even state, a single child's flag
//! making "something finished" and "everything finished" the same sentence.
//!
//! The *raising* of that counter is deliberately not modelled, and cannot
//! race: every raise is made by the frame that then waits, before the queue
//! push that lets any child be picked up, so the frame's own read of the
//! number is ordered after all of them by program order. What a child does to
//! the number is the only half two threads disagree about, and that is the
//! half above.
//!
//! What the model does not say anything about: the accounting that makes
//! `done` get set at all (that is the join counter and the preemption
//! protocol `executor::runtime`'s own module docs and `tests/loom_anchor.rs`
//! cover), or the `transmute` itself (a lifetime bound has no runtime
//! representation to race on; what can race is exactly the one `bool` this
//! model is built around).

#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::thread;

/// What a child closure borrowed, stood in for by one flag: `true` for as
/// long as the real data would still be valid to read, `false` from the
/// instant the frame that owns it gets control back and could drop or
/// overwrite it.
struct Borrow {
    alive: AtomicBool,
}

impl Borrow {
    fn new() -> Self {
        Self {
            alive: AtomicBool::new(true),
        }
    }

    /// The one access an erased child closure's body makes: a plain load, as
    /// a read of whatever it captured would be. Panics the calling thread if
    /// the data is already gone, which is loom's way of reporting a use that
    /// raced the frame reclaiming it.
    fn access(&self) {
        assert!(
            self.alive.load(Ordering::Acquire),
            "a child used its borrow after the frame that owned it took it back"
        );
    }

    /// The frame reclaiming what it lent out: the real crate's equivalent is
    /// the line right after `Runtime::scope` returns, which may drop or
    /// overwrite a stack local with no further warning to anything that
    /// still thinks it is borrowed.
    fn reclaim(&self) {
        self.alive.store(false, Ordering::Release);
    }
}

/// Which of `src/executor/scope.rs`'s three paths a child takes.
#[derive(Clone, Copy)]
enum Outcome {
    /// The child runs its body to the end: `enter`'s first branch in
    /// `executor::runtime`, with nothing left outstanding.
    Completes,
    /// The child touches its borrow and then panics: the payload still goes
    /// up the anchor chain and the job's teardown still runs, which is why
    /// `done` is still set here exactly as it is for `Completes`.
    Panics,
    /// The child is dropped before its body is ever called: the
    /// cancellation early-out in `executor::worker`'s dispatch, which never
    /// reaches the closure's body -- but drops the closure, and a guard it
    /// captured runs its `Drop` through the borrow then -- and still counts
    /// the job off, which is the only reason `done` is set on this path too.
    /// The drop comes first, as `WorkerThread::discard_skipped_body` makes
    /// it in the real early-out.
    Cancelled,
    /// **A mutation**, issue #416's: the cancelled path with its two steps
    /// in the order the real early-out had before that issue -- the job
    /// counted off, and the body dropped only afterwards, by the node's
    /// recycling. Not a path the crate ships; what `mutation_a_cancelled_
    /// childs_drop_after_the_count_is_found_with_the_join` asserts loom can
    /// tell apart from `Cancelled`.
    CancelledCountedFirst,
}

/// Runs one child against `borrow` under `outcome`, then takes `count_off`
/// -- upstream's teardown, which runs on every one of the three paths and is
/// the one thing a waiting frame can actually wait on. A parameter rather
/// than one flag, because the two frames below wait on different shapes of
/// the same signal: `one_scope` on a flag, `two_children` on the counter the
/// real predicate reads.
fn run_child(outcome: Outcome, borrow: &Borrow, count_off: impl Fn()) {
    match outcome {
        Outcome::Completes => {
            borrow.access();
            count_off();
        }
        Outcome::Panics => {
            borrow.access();
            // The real dispatch's own `catch_unwind` in
            // `WorkerThread::invoke_one`: a job's panic never unwinds past
            // the worker that ran it, so this model catches it at the same
            // point rather than at the top of the spawned thread, where an
            // uncaught unwind would abort the model instead of just this
            // one child.
            let _ = std::panic::catch_unwind(|| panic!("the child's own failure"));
            count_off();
        }
        Outcome::Cancelled => {
            // The one touch a never-called closure makes: the `Drop` of what
            // it captured, which the early-out runs by dropping the body --
            // before it counts the job off.
            borrow.access();
            count_off();
        }
        Outcome::CancelledCountedFirst => {
            // #416's order: counted off first, so a frame waiting on the
            // count may already have moved on when the drop touches the
            // borrow.
            count_off();
            borrow.access();
        }
    }
}

/// One scope, one child, and the frame it borrowed from -- `join` is the knob
/// the mutations below turn: `true` is the join `Runtime::scope` ships,
/// `false` is `scope` returning as soon as `f` does, the way upstream's own
/// `spawn`/`silent_async` detach rather than wait.
fn one_scope(outcome: Outcome, join: bool) {
    let borrow = Arc::new(Borrow::new());
    let done = Arc::new(AtomicBool::new(false));

    let child = {
        let borrow = Arc::clone(&borrow);
        let done = Arc::clone(&done);
        thread::spawn(move || {
            run_child(outcome, &borrow, || done.store(true, Ordering::Release));
        })
    };

    if join {
        // `Runtime::try_corun`'s predicate, abstracted to the one flag this
        // model needs: `scope` does not proceed past this line until the
        // child has been counted off, on every path.
        while !done.load(Ordering::Acquire) {
            thread::yield_now();
        }
    }

    // `Runtime::scope` has returned: the caller's own next line, which may
    // reclaim whatever the child borrowed with no further signal to it.
    borrow.reclaim();

    // `run_child` catches its own panic on the `Outcome::Panics` path, so
    // this join never sees one; it still has to happen; a dropped
    // `JoinHandle` would leave the spawned thread dangling past the end of
    // the model closure.
    child.join().unwrap();
}

/// How a frame reads the one number it waits on, which is the knob
/// `two_children`'s mutations turn.
#[derive(Clone, Copy)]
enum Join {
    /// `Runtime::try_corun`'s own predicate: the counter back at the bottom,
    /// which is every child counted off.
    EveryChild,
    /// **A mutation**: the frame goes on as soon as the counter has moved at
    /// all. The shape a predicate off by one child has -- and the one the
    /// single-child model above cannot express, there being no difference
    /// between the first count and the last when there is only one.
    SomeChild,
}

/// Two children under one scope, joined through the counter rather than
/// through a flag: `outstanding` starts at one per child, as
/// `Parent::count_queued` raises the runtime node's join counter per spawn,
/// and each child counts itself off where a real job's teardown does.
fn two_children(first: Outcome, second: Outcome, join: Join) {
    let borrow = Arc::new(Borrow::new());
    let outstanding = Arc::new(AtomicUsize::new(2));

    let children = [first, second].map(|outcome| {
        let borrow = Arc::clone(&borrow);
        let outstanding = Arc::clone(&outstanding);
        thread::spawn(move || {
            run_child(outcome, &borrow, || {
                outstanding.fetch_sub(1, Ordering::Release);
            });
        })
    });

    let settled = match join {
        Join::EveryChild => 0,
        Join::SomeChild => 1,
    };
    while outstanding.load(Ordering::Acquire) > settled {
        thread::yield_now();
    }

    // `Runtime::scope` has returned, as in `one_scope`: the caller's next
    // line may reclaim whatever either child borrowed.
    borrow.reclaim();

    for child in children {
        child.join().unwrap();
    }
}

/// `bounded_model`'s identical reason in `tests/loom_notifier.rs`: the
/// unbounded search is exponential in the atomics this protocol touches, and
/// three preemptions is loom's own recommendation and already enough to find
/// every ordering below.
fn bounded_model(f: impl Fn() + Sync + Send + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(f);
}

// --- The join the crate ships, on every path a child can take -------------

/// A child that runs to completion never sees its borrow reclaimed before it
/// is done with it.
#[test]
fn a_completing_child_is_joined_before_its_borrow_is_reclaimed() {
    bounded_model(|| one_scope(Outcome::Completes, true));
}

/// A child that panics still touched its borrow before the panic, and the
/// join still waits for that to happen first.
#[test]
fn a_panicking_child_is_joined_before_its_borrow_is_reclaimed() {
    bounded_model(|| one_scope(Outcome::Panics, true));
}

/// A cancelled child touches its borrow exactly once, from the drop of what
/// its never-called closure captured, and the join waits for that drop
/// because the early-out makes it before it counts the job off. The
/// liveness half rides along: the join still has to *terminate*, which it
/// can only do if the early-out still sets `done`.
#[test]
fn a_cancelled_child_is_joined_before_its_borrow_is_reclaimed() {
    bounded_model(|| one_scope(Outcome::Cancelled, true));
}

// --- The mutation: `scope` returning without the join, and loom has to find
// --- every path where that is reachable at all ----------------------------

/// Mutation, on the completing path: `scope` returns as soon as `f` does,
/// upstream's own `spawn`/`silent_async` shape. The reclaim can land while
/// the child is still mid-access.
#[test]
#[should_panic(expected = "a child used its borrow after the frame that owned it took it back")]
fn mutation_a_completing_child_is_found_without_the_join() {
    bounded_model(|| one_scope(Outcome::Completes, false));
}

/// The identical mutation, on the panicking path: a child whose access
/// happens before its panic is just as exposed as one that never panics at
/// all, which is the point of modelling both rather than only the simpler
/// one.
#[test]
#[should_panic(expected = "a child used its borrow after the frame that owned it took it back")]
fn mutation_a_panicking_child_is_found_without_the_join() {
    bounded_model(|| one_scope(Outcome::Panics, false));
}

/// The identical mutation, on the cancelled path, which it could not reach
/// before issue #416 gave that path an access to race: a dropped closure's
/// guard is as exposed to a `scope` that does not wait as a body that ran.
#[test]
#[should_panic(expected = "a child used its borrow after the frame that owned it took it back")]
fn mutation_a_cancelled_child_is_found_without_the_join() {
    bounded_model(|| one_scope(Outcome::Cancelled, false));
}

// --- The second mutation, #416's: the join is in place, and the cancelled
// --- path counts the job off before it drops the body -----------------------

/// Issue #416's order, under the join the crate ships: `scope` waits for
/// `done` as it should, and the early-out sets `done` *before* the drop
/// touches the borrow, so the wait is satisfied while the drop is still to
/// come. Loom has to find the interleaving where the frame reclaims first --
/// it is the one `recycle` of a window the box took ~22 full-suite runs to
/// hit once -- which is what makes this model a check on the order rather
/// than only on the join.
#[test]
#[should_panic(expected = "a child used its borrow after the frame that owned it took it back")]
fn mutation_a_cancelled_childs_drop_after_the_count_is_found_with_the_join() {
    bounded_model(|| one_scope(Outcome::CancelledCountedFirst, true));
}

// --- The same property over the counter the real predicate reads, and the
// --- two mutations only more than one child can have ------------------------

/// Two children taking two different paths -- one runs its body, one is
/// dropped unstarted -- and the frame waits for the counter to come back to
/// the bottom, which is what `Runtime::try_corun` waits for. Neither child
/// can see its borrow reclaimed, whichever order they finish in.
#[test]
fn a_mixed_pair_of_children_is_joined_before_the_borrow_is_reclaimed() {
    bounded_model(|| two_children(Outcome::Completes, Outcome::Cancelled, Join::EveryChild));
}

/// Mutation, issue #416's order on one of two children: the honest sibling
/// counts off after its own access, so the counter can reach the bottom with
/// the cancelled child's drop still to come. Loom has to find that the
/// counter settling is a claim about *counts* and not about bodies, which is
/// the whole of what #416 was.
#[test]
#[should_panic(expected = "a child used its borrow after the frame that owned it took it back")]
fn mutation_one_of_two_children_counted_before_its_drop_is_found() {
    bounded_model(|| {
        two_children(
            Outcome::Completes,
            Outcome::CancelledCountedFirst,
            Join::EveryChild,
        );
    });
}

/// Mutation, the predicate rather than the order: both children are honest
/// -- each drops before it counts off -- and the frame goes on as soon as the
/// counter has moved at all, which is what `Runtime::scope` would do if its
/// wait were short by one child. Loom finds the sibling still holding the
/// borrow, and the single-child model cannot state this at all.
#[test]
#[should_panic(expected = "a child used its borrow after the frame that owned it took it back")]
fn mutation_a_join_satisfied_by_the_first_of_two_counts_is_found() {
    bounded_model(|| two_children(Outcome::Cancelled, Outcome::Cancelled, Join::SomeChild));
}

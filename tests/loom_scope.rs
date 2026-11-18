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
//! borrow, and one that is "cancelled" -- dropped before its body ever runs,
//! so it never touches the borrow at all but still has to be *counted*, the
//! same `done` flag a real job's early-out still sets. The one thing that
//! must hold across all three is [`one_scope`]'s `join` knob: when it waits
//! for `done` before flipping `alive`, no child -- on any of the three paths
//! -- can ever observe `false`.
//!
//! The model earns its keep by **mutation**, as `tests/loom_storage.rs` and
//! `tests/loom_anchor.rs` do: alongside the join the crate ships, it carries
//! the one upstream-shaped alternative that this port rejected -- `scope`
//! returning as soon as `f` does, upstream's own `spawn`/`silent_async`
//! detaching rather than joining -- and asserts that loom *finds* it, on
//! every one of the three paths that could hide a mutation that only broke
//! one of them.
//!
//! What the model does not say anything about: the accounting that makes
//! `done` get set at all (that is the join counter and the preemption
//! protocol `executor::runtime`'s own module docs and `tests/loom_anchor.rs`
//! cover), or the `transmute` itself (a lifetime bound has no runtime
//! representation to race on; what can race is exactly the one `bool` this
//! model is built around).

#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
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
    /// reaches the closure at all -- so it never touches the borrow -- but
    /// still counts the job off, which is the only reason `done` is set on
    /// this path too.
    Cancelled,
}

/// Runs one child against `borrow` under `outcome`, then marks `done` --
/// upstream's teardown, which runs on every one of the three paths and is
/// the one thing `one_scope`'s wait can actually wait on.
fn run_child(outcome: Outcome, borrow: &Borrow, done: &AtomicBool) {
    match outcome {
        Outcome::Completes => {
            borrow.access();
            done.store(true, Ordering::Release);
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
            done.store(true, Ordering::Release);
        }
        Outcome::Cancelled => {
            // No `access()`: the early-out never calls this closure's body.
            done.store(true, Ordering::Release);
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
        thread::spawn(move || run_child(outcome, &borrow, &done))
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

/// A cancelled child never touches its borrow at all, so there is nothing
/// for the reclaim to race -- this is the liveness half: the join still has
/// to *terminate*, which it can only do if the early-out still sets `done`.
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

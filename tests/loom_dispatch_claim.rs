//! A [`loom`](https://docs.rs/loom) model of the protocol `Node::dispatch_claim`
//! gives `Runtime::schedule`'s bypass (`src/graph.rs`'s `DispatchClaim`,
//! taken from and released back to `src/executor/runtime.rs`'s
//! `schedule_bypassing`, `invoke_nonpreemptive` and `src/executor/subflow.rs`'s
//! `invoke`). Issue #245 is where the claim got this model, closing a hole
//! `Runtime::schedule`'s own guard -- `is_entered` -- could not see: a node
//! whose callable is locked right now, by a dispatch of a handle kind
//! `is_entered` never answers `true` for, or has not answered `true` for yet.
//!
//! **This file is not part of the gate**, for `tests/loom_notifier.rs`'s
//! reason: loom needs a `cfg(loom)` build of the whole graph, which only
//! `RUSTFLAGS` sets, and the cases below are empty without it. The command is
//! every model's, and all of them build in one go:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=/target/rustflow-loom \
//!     cargo test --release --test loom_dispatch_claim
//! ```
//!
//! `.github/workflows/loom.yml` runs it weekly and on `workflow_dispatch`,
//! alongside every other `tests/loom_*.rs`.
//!
//! # What is modelled, and what is not
//!
//! Standalone, `tests/loom_scope.rs`'s trade: this restates `DispatchClaim`'s
//! three operations -- `claim`, `release`, `wake` -- over one `AtomicU8`
//! rather than driving the real `Node`, `Runtime` and `WorkerThread`, which
//! would mean a `cfg(loom)` build of most of the executor.
//!
//! - [`busy`] stands in for the node's own callable `Mutex`: a flag that
//!   panics the instant two "dispatches" are inside it at once, which is
//!   this model's version of what `lock_callable`'s real `try_lock` catches.
//! - [`dispatch`] is one worker running the node once under a claim it
//!   already holds: enter the callable, leave it, release -- and, if the
//!   release reports a schedule arrived while it was held, do all of that
//!   again, which is the one requeue the real dispatch arms perform
//!   themselves rather than handing to anyone else.
//! - [`one_run`] fixes the one thing issue #245's own ordering argument
//!   establishes deterministically (`src/graph.rs`'s `DispatchClaim` docs)
//!   rather than leaving it to loom's search: `claim` happens, synchronously,
//!   before either of the two threads below is even spawned, which is the
//!   scenario the issue names -- a target that is *already* running --
//!   rather than the different, pre-existing question of two unrelated
//!   queueings racing before either has claimed anything, which
//!   `DispatchClaim` does not answer and this model does not either.
//!
//! The model earns its keep by **mutation**, `tests/loom_storage.rs`'s way:
//! alongside the real `wake`, it carries [`wake_unguarded`] -- `is_entered`'s
//! own pre-#245 shape, "queue now" with no claim check at all -- and asserts
//! that loom *finds* the overlap that makes possible.

#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use loom::thread;

/// `DispatchClaim::CLAIMED`, restated.
const CLAIMED: u8 = 0b01;
/// `DispatchClaim::PENDING`, restated.
const PENDING: u8 = 0b10;

/// `DispatchClaim::claim`, restated.
fn claim(state: &AtomicU8) {
    state.fetch_or(CLAIMED, Ordering::AcqRel);
}

/// `DispatchClaim::release`, restated.
fn release(state: &AtomicU8) -> bool {
    let previous = state.update(Ordering::AcqRel, Ordering::Acquire, |value| {
        if value & PENDING == 0 { 0 } else { CLAIMED }
    });
    previous & PENDING != 0
}

/// `DispatchClaim::wake`, restated: the fix this model carries.
fn wake(state: &AtomicU8) -> bool {
    let previous = state.update(Ordering::AcqRel, Ordering::Acquire, |value| {
        if value & CLAIMED == 0 {
            CLAIMED
        } else {
            value | PENDING
        }
    });
    previous & CLAIMED == 0
}

/// The mutation: `Runtime::schedule`'s own shape before issue #245, over this
/// model's state word rather than `is_entered`'s four preempting handle
/// kinds -- every call reports "queue now", with no claim check at all.
fn wake_unguarded(_state: &AtomicU8) -> bool {
    true
}

/// The node's callable `Mutex`, stood in for by a flag that panics rather
/// than blocks: `lock_callable`'s real `try_lock` is what a second entrant
/// finds there, and panicking here is this model's version of that.
fn enter_callable(busy: &AtomicBool) {
    let already = busy.swap(true, Ordering::AcqRel);
    assert!(
        !already,
        "two dispatches locked the same node's callable at once"
    );
}

/// The matching unlock: `lock_callable`'s guard, dropped.
fn leave_callable(busy: &AtomicBool) {
    busy.store(false, Ordering::Release);
}

/// One worker running the node once, under a claim it already holds: enters
/// and leaves the callable, then releases -- running itself again, exactly
/// once more, if the release reports a schedule arrived while it was held.
/// Bounded to one extra run because this model's single `wake`-or-mutation
/// call can set `PENDING` at most once.
fn dispatch(state: &AtomicU8, busy: &AtomicBool, runs: &AtomicUsize) {
    enter_callable(busy);
    runs.fetch_add(1, Ordering::Relaxed);
    leave_callable(busy);

    if release(state) {
        claim(state);
        enter_callable(busy);
        runs.fetch_add(1, Ordering::Relaxed);
        leave_callable(busy);
        release(state);
    }
}

/// One run of the model: a node already claimed -- `claim` runs here, on the
/// model's own thread, before either of the two spawned below -- then one
/// thread runs [`dispatch`] over that claim while a second calls
/// `schedule_wake` (the real [`wake`] or the [`wake_unguarded`] mutation)
/// concurrently. If `schedule_wake` reports "queue now", the second thread
/// also runs [`dispatch`], exactly as a real worker that picked up the job
/// `Runtime::schedule`'s bypass queued would.
fn one_run(schedule_wake: fn(&AtomicU8) -> bool) {
    let state = Arc::new(AtomicU8::new(0));
    let busy = Arc::new(AtomicBool::new(false));
    let runs = Arc::new(AtomicUsize::new(0));

    claim(&state);

    let dispatcher = {
        let state = Arc::clone(&state);
        let busy = Arc::clone(&busy);
        let runs = Arc::clone(&runs);
        thread::spawn(move || dispatch(&state, &busy, &runs))
    };

    let waker = {
        let state = Arc::clone(&state);
        let busy = Arc::clone(&busy);
        let runs = Arc::clone(&runs);
        thread::spawn(move || {
            if schedule_wake(&state) {
                dispatch(&state, &busy, &runs);
            }
        })
    };

    dispatcher.join().unwrap();
    waker.join().unwrap();
}

/// `bounded_model`'s identical reason in every other model here: the
/// unbounded search is exponential in the atomics this protocol touches, and
/// three preemptions is loom's own recommendation and already enough to find
/// every ordering below.
fn bounded_model(f: impl Fn() + Sync + Send + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(f);
}

/// The protocol the crate ships: a `wake` call that arrives while a dispatch
/// already holds the claim is deferred, never raced -- whatever interleaving
/// loom tries, the callable is never entered twice at once.
#[test]
fn a_wake_during_an_already_claimed_dispatch_is_deferred_not_raced() {
    bounded_model(|| one_run(wake));
}

/// Mutation: a `wake` with no claim check at all is `Runtime::schedule`'s own
/// pre-#245 shape -- `is_entered` answering `false` for the whole of a
/// non-preempting body's run, or for a preempting one's body before it has
/// set `PREEMPTED` -- and loom finds the overlap it makes possible.
#[test]
#[should_panic(expected = "two dispatches locked the same node's callable at once")]
fn mutation_an_unguarded_wake_is_found_racing_the_callable() {
    bounded_model(|| one_run(wake_unguarded));
}

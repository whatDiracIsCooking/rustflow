//! A [`loom`](https://docs.rs/loom) model of the protocol `Node::dispatch_claim`
//! gives `Runtime::schedule`'s bypass (`src/graph/node.rs`'s `DispatchClaim`,
//! taken by `src/executor/runtime.rs`'s `schedule_bypassing` and given back
//! by `src/executor/worker.rs`'s `invoke_one`, once per job it dequeues).
//! Issue #245 is where the claim got this model, closing a hole
//! `Runtime::schedule`'s own guard -- `is_entered` -- could not see: a node
//! whose callable is locked right now, by a dispatch of a handle kind
//! `is_entered` never answers `true` for, or has not answered `true` for yet.
//! Issue #320 is where the release moved to `invoke_one` from the five
//! dispatch arms that used to make it, and gave this file its second
//! scenario: the arms were not all of the ways a dequeued job retires.
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
//!   again, which is the one requeue `invoke_one` performs itself rather
//!   than handing to anyone else.
//! - [`skipped`] is the other way a dequeued job retires: `invoke_one`'s
//!   cancellation early-out, which dispatches nothing at all and so takes
//!   and gives back the claim around a no-op.
//! - [`one_run`] fixes the one thing issue #245's own ordering argument
//!   establishes deterministically (`src/graph.rs`'s `DispatchClaim` docs)
//!   rather than leaving it to loom's search: `claim` happens, synchronously,
//!   before either of the two threads below is even spawned, which is the
//!   scenario the issue names -- a target that is *already* running --
//!   rather than the different, pre-existing question of two unrelated
//!   queueings racing before either has claimed anything, which
//!   `DispatchClaim` does not answer and this model does not either.
//!
//! The model earns its keep by **mutation**, `tests/loom_storage.rs`'s way,
//! one per scenario: alongside the real `wake` it carries [`wake_unguarded`]
//! -- `is_entered`'s own pre-#245 shape, "queue now" with no claim check at
//! all -- and asserts that loom *finds* the overlap that makes possible;
//! alongside [`skipped`] it carries [`skipped_leaking`], the pre-#320
//! early-out that returned before the dispatch and so released nothing, and
//! asserts the claim is found left behind.

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
///
/// `invoke_one`'s generic pair, with the handle `match` between the two
/// halves stood in for by the callable; what the real one takes before the
/// `match` is the `claim` [`one_run`] makes before spawning.
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

/// One worker retiring a job it dequeued without dispatching the node at
/// all: `invoke_one`'s cancellation early-out, which claims and releases
/// around a no-op. The inner pair is its requeue of a bypass that landed
/// while the claim was held -- the requeued job is skipped the same way, and
/// its own release is what clears the claim.
fn skipped(state: &AtomicU8) {
    claim(state);
    if release(state) {
        claim(state);
        release(state);
    }
}

/// The mutation: the same early-out before issue #320, which returned before
/// the dispatch `match` and so neither claimed nor released anything.
fn skipped_leaking(_state: &AtomicU8) {}

/// One run of the skipped-job model: an *idle* node -- no claim is taken
/// here, unlike [`one_run`] -- that two threads bypass concurrently. Exactly
/// one of them is told to queue, since the other's `wake` finds the claim
/// the first took, and the one told to queue runs `retire` over the job it
/// queued: a job of a cancelled run, which the real `invoke_one` skips.
///
/// The claim must be idle again once both threads have finished, whichever
/// of them queued and whenever the other's `wake` landed: a bit left set
/// here is a node no later bypass can ever queue.
fn one_skipped_run(retire: fn(&AtomicU8)) {
    let state = Arc::new(AtomicU8::new(0));

    let bypasses: Vec<_> = (0..2)
        .map(|_| {
            let state = Arc::clone(&state);
            thread::spawn(move || {
                if wake(&state) {
                    retire(&state);
                }
            })
        })
        .collect();

    for bypass in bypasses {
        bypass.join().unwrap();
    }

    // `Acquire`, pairing with the `AcqRel` of whichever `release` stored
    // last: this reads the word a dispatch published on its way out, which
    // is the same pairing the next `wake` to arrive would make.
    assert_eq!(
        state.load(Ordering::Acquire),
        0,
        "the claim was handed back: a later bypass is told to queue"
    );
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

/// The other half of the protocol the crate ships, issue #320's: a job that
/// retires without dispatching the node hands the claim back all the same,
/// so the word is idle again whatever order the two bypasses land in and
/// whichever of them was the one told to queue.
#[test]
fn a_bypass_whose_job_is_skipped_leaves_the_claim_idle() {
    bounded_model(|| one_skipped_run(skipped));
}

/// Mutation: the cancellation early-out's own pre-#320 shape, returning
/// before the dispatch and so releasing nothing -- the claim is found left
/// behind on a node no dispatch holds, which is exactly the hang a later
/// bypass of that node met.
#[test]
#[should_panic(expected = "the claim was handed back")]
fn mutation_a_skipped_job_that_releases_nothing_leaks_the_claim() {
    bounded_model(|| one_skipped_run(skipped_leaking));
}

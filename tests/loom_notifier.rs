//! A [`loom`](https://docs.rs/loom) model of [`NonblockingNotifier`]'s wait
//! protocol: the interleavings of prepare/cancel/commit against
//! `notify_one`/`notify_n`/`notify_all` for two and three threads.
//!
//! **This file is not part of the gate.** loom works by replacing the atomics
//! with instrumented ones and re-running the model once per interleaving, so it
//! needs a different build of the whole graph -- `cfg(loom)`, which only
//! `RUSTFLAGS` sets -- and the cases below are empty without it:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=/target/rustflow-loom \
//!     cargo test --release --test loom_notifier
//! ```
//!
//! Give it a `CARGO_TARGET_DIR` of its own: `--cfg loom` makes every crate in
//! the graph a separate build, so sharing the gate's directory forces a full
//! rebuild in both directions every time you switch.
//!
//! What the model is for, beyond the obvious lost-wakeup question:
//! `notifier.rs` reads a parked thread's handle out of an [`UnsafeCell`] with
//! no lock, and the argument that this is sound is an argument about *when* a
//! waiter slot is reachable. loom checks that argument directly -- its own
//! `UnsafeCell` panics on two overlapping accesses -- and reports a waiter that
//! parks with nobody left to wake it as a deadlock.
//!
//! One thing the model cannot see, and the reason [`wait_for`] loops rather than
//! asserting the predicate the moment `commit_wait` returns: that the predicate
//! is visible to a notified waiter follows from the sequentially consistent
//! *fence* in `notify_one` synchronising with the waiter's sequentially
//! consistent *load* of the state word, and loom deliberately models a `SeqCst`
//! load as a plain acquire (`rt::thread::seq_cst` is a documented no-op there,
//! "may fail to model correct code, but will not silently allow bugs"). Looping
//! is also what a worker does, so the model is the more faithful for it.
//!
//! [`UnsafeCell`]: std::cell::UnsafeCell

#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;

use rustflow::executor::notifier::{NonblockingNotifier, Notifier};

/// The waiter's half of the protocol, as a worker will drive it: check the
/// predicate, open a wait, check again, then either sleep or back out -- and on
/// waking, go round again, because a notification is a hint to look for work
/// rather than a delivery of it.
///
/// The loop is not padding. It is the shape the executor's worker loop has, and
/// it is also what makes the model's liveness property checkable: returning
/// from `commit_wait` means *a* notification arrived, while leaving this
/// function means the waiter actually saw the work. A notifier that drops a
/// wakeup leaves this spinning in a `commit_wait` nobody will end, which loom
/// reports as a deadlock.
fn wait_for(notifier: &NonblockingNotifier, predicate: &AtomicBool, worker_id: usize) {
    loop {
        if predicate.load(Ordering::Acquire) {
            return;
        }
        notifier.prepare_wait(worker_id);
        if predicate.load(Ordering::Acquire) {
            notifier.cancel_wait(worker_id);
            return;
        }
        notifier.commit_wait(worker_id);
    }
}

/// A model with a bound on how often loom may preempt a thread.
///
/// The unbounded search is exponential in the number of atomic operations, and
/// this protocol has a lot of them: one `notify_n` call alone is a fence, a
/// load and a compare-exchange per wake. Unbounded, the four cases below did
/// not finish in a quarter of an hour; bounded at three, they take about a
/// second, and still explore every interleaving that needs three or fewer
/// preemptions -- which is loom's own recommendation, and where bugs of this
/// kind live.
fn bounded_model(f: impl Fn() + Sync + Send + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(f);
}

/// One waiter, one notifier: `notify_one` must not be lost, wherever it lands
/// in the waiter's two-phase protocol.
///
/// A lost wakeup leaves the waiter parked with the model finished, which loom
/// reports as a deadlock.
#[test]
fn notify_one_is_never_lost() {
    bounded_model(|| {
        let notifier = Arc::new(NonblockingNotifier::new(1));
        let predicate = Arc::new(AtomicBool::new(false));

        let waiter = {
            let (notifier, predicate) = (Arc::clone(&notifier), Arc::clone(&predicate));
            thread::spawn(move || wait_for(&notifier, &predicate, 0))
        };

        predicate.store(true, Ordering::Release);
        notifier.notify_one();

        waiter.join().unwrap();
        assert_eq!(notifier.num_waiters(), 0);
    });
}

/// The same, with `notify_n(1)` in place of `notify_one`: the two take
/// different code paths to the same state word.
#[test]
fn notify_n_is_never_lost() {
    bounded_model(|| {
        let notifier = Arc::new(NonblockingNotifier::new(1));
        let predicate = Arc::new(AtomicBool::new(false));

        let waiter = {
            let (notifier, predicate) = (Arc::clone(&notifier), Arc::clone(&predicate));
            thread::spawn(move || wait_for(&notifier, &predicate, 0))
        };

        predicate.store(true, Ordering::Release);
        notifier.notify_n(1);

        waiter.join().unwrap();
        assert_eq!(notifier.num_waiters(), 0);
    });
}

/// A cancel racing a notification: both resolve the same ticket, and exactly
/// one of them may take it out of the pre-waiter count.
///
/// If they both did, or neither did, the notifier's own drop assertion fires:
/// the state word has to come back to an empty stack with no pre-waiters.
#[test]
fn a_cancel_and_a_notification_resolve_one_ticket_once() {
    bounded_model(|| {
        let notifier = Arc::new(NonblockingNotifier::new(1));

        let waiter = {
            let notifier = Arc::clone(&notifier);
            thread::spawn(move || {
                notifier.prepare_wait(0);
                notifier.cancel_wait(0);
            })
        };

        notifier.notify_one();

        waiter.join().unwrap();
        assert_eq!(notifier.num_waiters(), 0);
        // The notifier must still be usable: a cancel that left a ghost
        // pre-waiter behind would make this notification try to wake it.
        notifier.notify_all();
        assert_eq!(notifier.num_waiters(), 0);
    });
}

/// Two waiters and a notifier that hands out one targeted wake and then a
/// broadcast. `notify_n(1)` can only release one of them, so the second must be
/// released by the `notify_all` -- whether it is still a pre-waiter by then, or
/// already parked on the waiter stack.
///
/// This is the three-thread case, and the only one that puts two slots on the
/// stack at once: it is what exercises the stack link and the handle read in
/// `unpark_chain`.
#[test]
fn two_waiters_are_released_by_notify_n_then_notify_all() {
    bounded_model(|| {
        // Three slots, not two, so that `notify_n(1)` stays on its own path
        // instead of collapsing into `notify_all`.
        let notifier = Arc::new(NonblockingNotifier::new(3));
        let predicate = Arc::new(AtomicBool::new(false));

        let waiters: Vec<_> = (0..2)
            .map(|worker_id| {
                let (notifier, predicate) = (Arc::clone(&notifier), Arc::clone(&predicate));
                thread::spawn(move || wait_for(&notifier, &predicate, worker_id))
            })
            .collect();

        predicate.store(true, Ordering::Release);
        notifier.notify_n(1);
        notifier.notify_all();

        for waiter in waiters {
            waiter.join().unwrap();
        }
        assert_eq!(notifier.num_waiters(), 0);
    });
}

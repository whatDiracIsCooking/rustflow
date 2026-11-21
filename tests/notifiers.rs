//! Translated from `unittests/test_notifiers.cpp` (Taskflow v4.1.0).
//!
//! Upstream's file registers two suites off one set of templates, one per
//! notifier, and so does this one: the case templates below are generic over
//! [`Notifier`], and each type's section instantiates the ones its own
//! acceptance list names.
//!
//! The cases ported are the ones issues #7 and #13 name. For
//! [`AtomicNotifier`] (#7): the four `invariants`, `notify_before_commit` at 5
//! and 8 threads, `cancel_only_no_ghost_waiters`, `bug2_notify_n_exact_size`
//! (spelled `notify_n_exact_size` here), `notify_n_zero_is_noop`,
//! `bug3_epoch_field`, `notify_one_at_prewaiter_window` and
//! `rapid_cancel_integrity`. For [`NonblockingNotifier`] (#13): every
//! `NonblockingNotifier.*` case -- the six `invariants`,
//! `notify_before_commit` at 1/2/4/8/15 threads,
//! `cancel_only_no_ghost_waiters`, `notify_n_exact_size`,
//! `notify_n_zero_is_noop`, `notify_n_no_over_release`,
//! `rapid_cancel_integrity`, `rapid_cancel_epoch_integrity` and
//! `notify_one_at_prewaiter_window`.
//!
//! Upstream's `REGISTER_CORE_TESTS` battery -- the `no_missing_*` families,
//! `fuzz_stress` and the 31- and 64-thread cases -- is **not** ported here, nor
//! are the two `Basic` stress cases or `AtomicNotifier`'s
//! `notify_n_releases_committed` at `N8.k8`/`N31.k32`; none of them is in
//! either issue's acceptance list.
//!
//! Two shape changes that are Rust's, not upstream's:
//!
//! - `std::thread::scope` replaces `std::vector<std::thread>` plus a join loop,
//!   so the worker threads borrow the notifier and the test's counters
//!   directly instead of going through an `Arc`.
//! - Where upstream asserts a timed-out flag and *then* joins -- which would
//!   hang the suite on exactly the failure it is trying to report, because the
//!   thread it is about to join is the one still parked --
//!   [`notify_one_at_prewaiter_window`] records the verdict, drains the
//!   notifier so the join can finish, and asserts after the scope.
//!
//! Five case shapes are this file's own rather than upstream's, and each says
//! so in its own doc comment: [`notify_n_resolves_k_prewaiter_tickets`],
//! [`a_spurious_unpark_does_not_end_a_wait`],
//! [`a_finished_waiter_is_not_claimable`], the two that read a notifier's
//! `Debug`, and the two that drop a notifier with a waiter still registered.
//! Three of them follow from the port's one departure from upstream -- a
//! thread handle and [`std::thread::park`] where upstream has a futex -- and
//! the other two ask of `notify_n` and of a dump what upstream's suite never
//! asks.

// The notifiers are built out of `loom`'s atomics under `cfg(loom)`, and
// loom's only run inside its own model: this suite is the `std` one, and
// `tests/loom_notifier.rs` is the other half.
#![cfg(not(loom))]

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rustflow::executor::notifier::{AtomicNotifier, NonblockingNotifier, Notifier};

/// How many times a case that manufactures spurious wakeups unparks the
/// sleeping thread. One unpark would do -- a thread holds at most one token --
/// but the waiter is somewhere in a park loop rather than at a known point, so
/// the case spends enough of them, yielding between, that it lands inside the
/// loop rather than only before it.
const SPURIOUS_WAKES: usize = 1_000;

/// How many times a case yields before notifying, where what it needs is a
/// waiter that has reached its `park` rather than one that is still on its way
/// there. Nothing observable says "it is asleep now", which is why the cases
/// that need it also run rounds.
const SETTLE_YIELDS: usize = 64;

// Only the matrix macro: every helper this file would otherwise take from
// `tests/common/` it does without, because a notifier case counts with the
// counters it declares inside its own `thread::scope`.
#[macro_use]
mod common;

// ============================================================================
// Shared templates, upstream's function templates over the notifier type
// ============================================================================

/// `notify_all()` fires BEFORE threads call `commit_wait()`. Threads must NOT
/// block (no lost wakeup). Exercises the window between `prepare_wait()` and
/// `commit_wait()` where the notification arrives.
fn notify_before_commit<T: Notifier + Sync>(notifier: &T) {
    let n = notifier.size();

    let prep_count = AtomicUsize::new(0);
    let committed = AtomicUsize::new(0);
    let notified = AtomicBool::new(false);
    let (prep_count, committed, notified) = (&prep_count, &committed, &notified);

    thread::scope(|scope| {
        for id in 0..n {
            scope.spawn(move || {
                notifier.prepare_wait(id);
                prep_count.fetch_add(1, Ordering::Release);

                // Spin until the main thread has already called notify_all().
                while !notified.load(Ordering::Acquire) {
                    thread::yield_now();
                }

                notifier.commit_wait(id);
                committed.fetch_add(1, Ordering::Release);
            });
        }

        while prep_count.load(Ordering::Acquire) != n {
            thread::yield_now();
        }

        // Notify before any thread calls commit_wait.
        notifier.notify_all();
        notified.store(true, Ordering::Release);
    });

    assert_eq!(committed.load(Ordering::Acquire), n);
    assert_eq!(notifier.num_waiters(), 0);
}

/// Forces ALL N threads to become committed waiters (`num_waiters() == N`),
/// then verifies `notify_n(k)` alone releases at least `min(k, N)` of them.
/// `notify_all()` drains the rest so the round completes.
///
/// This is a targeted regression for `notify_n` boundary semantics and for
/// spurious early-exit from `commit_wait` due to an epoch field bug.
fn notify_n_releases_committed<T: Notifier + Sync>(notifier: &T, k: usize, rounds: usize) {
    let n = notifier.size();

    let round = AtomicUsize::new(0);
    let prepared = AtomicUsize::new(0);
    let committed_done = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let (round, prepared, committed_done, stop) = (&round, &prepared, &committed_done, &stop);

    thread::scope(|scope| {
        for id in 0..n {
            scope.spawn(move || {
                let mut local_round = 0;

                while !stop.load(Ordering::Relaxed) {
                    while round.load(Ordering::Acquire) == local_round
                        && !stop.load(Ordering::Relaxed)
                    {
                        thread::yield_now();
                    }

                    if stop.load(Ordering::Relaxed) {
                        break;
                    }

                    // No cancel path: we want all threads to become committed
                    // waiters.
                    notifier.prepare_wait(id);
                    prepared.fetch_add(1, Ordering::Release);

                    notifier.commit_wait(id);
                    committed_done.fetch_add(1, Ordering::Release);

                    local_round += 1;
                }
            });
        }

        for r in 1..=rounds {
            prepared.store(0, Ordering::Relaxed);
            committed_done.store(0, Ordering::Relaxed);

            round.store(r, Ordering::Release);

            while prepared.load(Ordering::Acquire) != n {
                thread::yield_now();
            }

            // Wait until ALL threads are truly registered waiters.
            while notifier.num_waiters() != n {
                thread::yield_now();
            }

            let target = k.min(n);
            notifier.notify_n(k);

            let deadline = Instant::now() + Duration::from_secs(5);
            while committed_done.load(Ordering::Acquire) < target && Instant::now() < deadline {
                thread::yield_now();
            }
            assert!(
                committed_done.load(Ordering::Acquire) >= target,
                "round {r}: notify_n({k}) released \
                 {released} of {target} committed waiters",
                released = committed_done.load(Ordering::Acquire),
            );

            // Drain remaining waiters.
            notifier.notify_all();

            let drain_deadline = Instant::now() + Duration::from_secs(10);
            while committed_done.load(Ordering::Acquire) != n && Instant::now() < drain_deadline {
                notifier.notify_all();
                thread::yield_now();
            }

            assert_eq!(committed_done.load(Ordering::Acquire), n, "round {r} drain");
            assert_eq!(notifier.num_waiters(), 0, "round {r} waiter count");
        }

        stop.store(true, Ordering::Release);
        notifier.notify_all();
    });

    assert_eq!(notifier.num_waiters(), 0);
}

/// After all N threads are fully committed (`num_waiters() == N`),
/// `notify_n(0)` must not wake any of them. Immediately after the call
/// `num_waiters()` must still be N. Then `notify_all()` drains cleanly.
///
/// Catches implementations that treat 0 as "notify all" or misread the loop
/// bound.
fn notify_n_zero_is_noop<T: Notifier + Sync>(notifier: &T) {
    let n = notifier.size();

    let prepared = AtomicUsize::new(0);
    let committed = AtomicUsize::new(0);
    let (prepared, committed) = (&prepared, &committed);

    thread::scope(|scope| {
        for id in 0..n {
            scope.spawn(move || {
                notifier.prepare_wait(id);
                prepared.fetch_add(1, Ordering::Release);
                notifier.commit_wait(id);
                committed.fetch_add(1, Ordering::Release);
            });
        }

        while prepared.load(Ordering::Acquire) != n {
            thread::yield_now();
        }
        while notifier.num_waiters() != n {
            thread::yield_now();
        }

        notifier.notify_n(0);

        // No thread should have woken up.
        assert_eq!(notifier.num_waiters(), n);
        assert_eq!(committed.load(Ordering::Acquire), 0);

        notifier.notify_all();
    });

    assert_eq!(committed.load(Ordering::Acquire), n);
    assert_eq!(notifier.num_waiters(), 0);
}

/// `notify_one()` fires in the pre-waiter window (after `prepare_wait`, before
/// `commit_wait`).
///
/// Works for both notifiers, which account for a pre-waiter differently:
/// [`NonblockingNotifier`] leaves `num_waiters()` at 0 until the waiter parks
/// but keeps a pre-waiter count in its state word, while for
/// [`AtomicNotifier`] `num_waiters()` increments immediately at
/// `prepare_wait`. Either way the notification has somebody to find even
/// though nobody is asleep yet.
fn notify_one_at_prewaiter_window<T: Notifier + Sync>(notifier: &T, rounds: usize) {
    for round in 0..rounds {
        let prepared = AtomicBool::new(false);
        let go = AtomicBool::new(false);
        let done = AtomicBool::new(false);

        let mut woke_in_time = false;

        thread::scope(|scope| {
            scope.spawn(|| {
                notifier.prepare_wait(0);
                prepared.store(true, Ordering::Release);
                while !go.load(Ordering::Acquire) {
                    thread::yield_now();
                }
                notifier.commit_wait(0);
                done.store(true, Ordering::Release);
            });

            while !prepared.load(Ordering::Acquire) {
                thread::yield_now();
            }

            // Fire notify_one while the thread is in the pre-wait window.
            notifier.notify_one();
            go.store(true, Ordering::Release);

            let deadline = Instant::now() + Duration::from_secs(5);
            while !done.load(Ordering::Acquire) && Instant::now() < deadline {
                thread::yield_now();
            }

            woke_in_time = done.load(Ordering::Acquire);
            if !woke_in_time {
                // Unstick the waiter so the scope's join can finish and the
                // assertion below is what fails, not the test harness.
                notifier.notify_all();
            }
        });

        assert!(woke_in_time, "round {round}: notify_one was lost");
        assert_eq!(notifier.num_waiters(), 0);
    }
}

/// Many sequential prepare+cancel cycles; verify the notifier is still usable
/// afterwards. For [`AtomicNotifier`], cancels adjust the waiter count -- this
/// stresses count accuracy. For [`NonblockingNotifier`], cancels advance the
/// epoch -- this stresses the epoch field.
fn rapid_cancel_integrity<T: Notifier + Sync>(notifier: &T, cycles: usize) {
    for _ in 0..cycles {
        notifier.prepare_wait(0);
        notifier.cancel_wait(0);
    }
    assert_eq!(notifier.num_waiters(), 0);

    let committed = AtomicUsize::new(0);

    thread::scope(|scope| {
        scope.spawn(|| {
            notifier.prepare_wait(0);
            notifier.commit_wait(0);
            committed.fetch_add(1, Ordering::Release);
        });

        while notifier.num_waiters() < 1 {
            thread::yield_now();
        }
        notifier.notify_all();
    });

    assert_eq!(committed.load(Ordering::Acquire), 1);
    assert_eq!(notifier.num_waiters(), 0);
}

/// After `notify_n(k)` releases at least `min(k, N)` committed waiters, the
/// ones it did not release must still be parked: `num_waiters()` must equal
/// `N` minus the number that woke. `notify_all()` then drains them and the
/// total must be `N`.
///
/// Catches an off-by-one in `notify_n`'s loop, or a loop that iterates too
/// many times and spends a wake it was not given.
fn notify_n_does_not_over_release<T: Notifier + Sync>(notifier: &T, k: usize, rounds: usize) {
    let n = notifier.size();

    let round = AtomicUsize::new(0);
    let prepared = AtomicUsize::new(0);
    let committed_done = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let (round, prepared, committed_done, stop) = (&round, &prepared, &committed_done, &stop);

    let target = k.min(n);

    thread::scope(|scope| {
        for id in 0..n {
            scope.spawn(move || {
                let mut local_round = 0;

                while !stop.load(Ordering::Relaxed) {
                    while round.load(Ordering::Acquire) == local_round
                        && !stop.load(Ordering::Relaxed)
                    {
                        thread::yield_now();
                    }

                    if stop.load(Ordering::Relaxed) {
                        break;
                    }

                    notifier.prepare_wait(id);
                    prepared.fetch_add(1, Ordering::Release);

                    notifier.commit_wait(id);
                    committed_done.fetch_add(1, Ordering::Release);

                    local_round += 1;
                }
            });
        }

        for r in 1..=rounds {
            prepared.store(0, Ordering::Relaxed);
            committed_done.store(0, Ordering::Relaxed);

            round.store(r, Ordering::Release);

            while prepared.load(Ordering::Acquire) != n {
                thread::yield_now();
            }

            // Wait until ALL threads are truly registered waiters.
            while notifier.num_waiters() != n {
                thread::yield_now();
            }

            notifier.notify_n(k);

            let deadline = Instant::now() + Duration::from_secs(5);
            while committed_done.load(Ordering::Acquire) < target && Instant::now() < deadline {
                thread::yield_now();
            }
            let woke = committed_done.load(Ordering::Acquire);
            assert!(
                woke >= target,
                "round {r}: notify_n({k}) released {woke} of {target} committed waiters",
            );

            // The rest must still be parked: no over-release.
            assert_eq!(
                notifier.num_waiters(),
                n - woke,
                "round {r}: notify_n({k}) woke {woke} and left the wrong number parked",
            );

            notifier.notify_all();

            let drain_deadline = Instant::now() + Duration::from_secs(10);
            while committed_done.load(Ordering::Acquire) != n && Instant::now() < drain_deadline {
                notifier.notify_all();
                thread::yield_now();
            }

            assert_eq!(committed_done.load(Ordering::Acquire), n, "round {r} drain");
            assert_eq!(notifier.num_waiters(), 0, "round {r} waiter count");
        }

        stop.store(true, Ordering::Release);
        notifier.notify_all();
    });

    assert_eq!(notifier.num_waiters(), 0);
}

/// A wait ends on a notification and on nothing else: a waiter whose thread is
/// unparked without one re-reads the condition and sleeps on.
///
/// This case is the port's rather than upstream's, and it is the one the park
/// registry asks for. Upstream parks on a futex word, so the only thing that
/// can wake its waiter is a write to that word; here the sleep is
/// [`std::thread::park`], and a `park` returns both for an `unpark` and for no
/// reason at all -- and a waiter that was claimed but left without parking
/// leaves a token behind that makes its *next* `park` return at once, which
/// both notifiers' docs name as a consequence they tolerate. So the condition
/// has to be the state word rather than the wake, and that is what this
/// asserts: the token is manufactured by unparking the sleeper's own thread,
/// no notification having happened, and `commit_wait` is still waiting
/// afterwards (#430).
fn a_spurious_unpark_does_not_end_a_wait<T: Notifier + Sync>(notifier: &T) {
    let sleeper = OnceLock::new();
    let committed = AtomicBool::new(false);
    let (sleeper, committed) = (&sleeper, &committed);

    let mut left_on_a_spurious_wake = false;

    thread::scope(|scope| {
        scope.spawn(move || {
            notifier.prepare_wait(0);
            sleeper
                .set(thread::current())
                .expect("the waiter publishes its own handle once");
            notifier.commit_wait(0);
            committed.store(true, Ordering::Release);
        });

        let sleeper = loop {
            if let Some(handle) = sleeper.get() {
                break handle;
            }
            thread::yield_now();
        };

        for _ in 0..SPURIOUS_WAKES {
            sleeper.unpark();
            thread::yield_now();
        }

        left_on_a_spurious_wake = committed.load(Ordering::Acquire);

        // Drain: `notify_all` releases a waiter wherever it is in the protocol,
        // which is what lets this loop end whether or not the wait was still
        // running when the verdict above was taken.
        while !committed.load(Ordering::Acquire) {
            notifier.notify_all();
            thread::yield_now();
        }
    });

    assert!(
        !left_on_a_spurious_wake,
        "an unpark that was not a notification ended the wait"
    );
    assert_eq!(notifier.num_waiters(), 0);
}

// ============================================================================
// NonblockingNotifier-only helpers, for semantics the two types do not share
// ============================================================================

/// `num_waiters()` counts committed (parked) threads only, never pre-waiters.
fn num_waiters_counts_only_committed() {
    let notifier = NonblockingNotifier::new(4);

    assert_eq!(notifier.num_waiters(), 0);

    // A single-threaded prepare: a pre-waiter is not a committed waiter.
    notifier.prepare_wait(0);
    assert_eq!(notifier.num_waiters(), 0);
    notifier.cancel_wait(0);
    assert_eq!(notifier.num_waiters(), 0);

    // Two sequential prepare+cancel cycles; num_waiters must remain 0.
    notifier.prepare_wait(1);
    notifier.cancel_wait(1);
    notifier.prepare_wait(2);
    notifier.cancel_wait(2);
    assert_eq!(notifier.num_waiters(), 0);

    // Now let a thread actually commit: it must show up as 1 waiter.
    thread::scope(|scope| {
        scope.spawn(|| {
            notifier.prepare_wait(3);
            notifier.commit_wait(3);
        });

        while notifier.num_waiters() != 1 {
            thread::yield_now();
        }
        assert_eq!(notifier.num_waiters(), 1);

        notifier.notify_all();
    });

    assert_eq!(notifier.num_waiters(), 0);
}

/// Many sequential prepare+cancel cycles to exercise the epoch counter, which
/// a cancel advances exactly as a notification does.
fn rapid_cancel_epoch_integrity(cycles: usize) {
    let notifier = NonblockingNotifier::new(2);

    // Saturate the epoch counter.
    for _ in 0..cycles {
        notifier.prepare_wait(0);
        notifier.cancel_wait(0);
    }
    assert_eq!(notifier.num_waiters(), 0);

    // Verify the notifier is still usable after epoch saturation.
    let committed = AtomicUsize::new(0);

    thread::scope(|scope| {
        scope.spawn(|| {
            notifier.prepare_wait(0);
            notifier.commit_wait(0);
            committed.fetch_add(1, Ordering::Release);
        });

        while notifier.num_waiters() != 1 {
            thread::yield_now();
        }
        notifier.notify_all();
    });

    assert_eq!(committed.load(Ordering::Acquire), 1);
    assert_eq!(notifier.num_waiters(), 0);
}

/// `notify_n(k)` resolves exactly `k` of the tickets outstanding in the
/// pre-wait window, and the waiters behind them still sleep.
///
/// Upstream's `notify_n` battery, and the three cases above it here, drive
/// every thread to a *committed* wait before notifying, which is the other arm
/// of [`NonblockingNotifier`]'s loop: the arm that pops the waiter stack. This
/// one is the pre-waiter arm, which a notification reaches whenever work is
/// published while a worker is still deciding whether to sleep -- the common
/// case under load, and the arm whose own accounting the type's docs rest on,
/// the epoch advancing once per resolved ticket so that `k` of them are
/// resolved and the rest stay owed. A waiter whose ticket was resolved returns
/// from `commit_wait` without parking at all, so the number still parked
/// afterwards is `size() - k` exactly (#430).
fn notify_n_resolves_k_prewaiter_tickets(notifier: &NonblockingNotifier, k: usize, rounds: usize) {
    let n = notifier.size();
    assert!(k < n, "the pre-waiter arm needs more tickets than wakes");

    for round in 0..rounds {
        let prepared = AtomicUsize::new(0);
        let go = AtomicBool::new(false);
        let committed = AtomicUsize::new(0);
        let (prepared, go, committed) = (&prepared, &go, &committed);

        let mut parked = usize::MAX;

        thread::scope(|scope| {
            for id in 0..n {
                scope.spawn(move || {
                    notifier.prepare_wait(id);
                    prepared.fetch_add(1, Ordering::Release);
                    while !go.load(Ordering::Acquire) {
                        thread::yield_now();
                    }
                    notifier.commit_wait(id);
                    committed.fetch_add(1, Ordering::Release);
                });
            }

            while prepared.load(Ordering::Acquire) != n {
                thread::yield_now();
            }

            // Every ticket is outstanding and nobody has committed: this is the
            // window the pre-waiter arm is reached from.
            notifier.notify_n(k);
            go.store(true, Ordering::Release);

            // The `k` resolved tickets leave without parking, so the count can
            // only climb to `n - k`; reading it the moment it gets there is
            // reading it at its peak.
            let deadline = Instant::now() + Duration::from_secs(5);
            while notifier.num_waiters() != n - k && Instant::now() < deadline {
                thread::yield_now();
            }
            parked = notifier.num_waiters();

            while committed.load(Ordering::Acquire) != n {
                notifier.notify_all();
                thread::yield_now();
            }
        });

        assert_eq!(
            parked,
            n - k,
            "round {round}: notify_n({k}) left {parked} of {n} pre-waiters asleep",
        );
        assert_eq!(notifier.num_waiters(), 0);
    }
}

// ============================================================================
// AtomicNotifier-only helpers, for semantics the two types do not share
// ============================================================================

/// A waiter that has left its wait is not claimable: the next notification
/// goes to a waiter that is still asleep, not to the slot of one that has
/// already finished.
///
/// This case is [`AtomicNotifier`]'s alone, and it is the price of the simpler
/// state word: with no waiter stack to pop, a notifier hands out a wake by
/// scanning the slots for an armed one, so a slot whose owner has gone must be
/// out of service by the time that owner returns. Spending the wake on it
/// would leave a genuine waiter asleep -- it is `claim_one` that stops at the
/// first armed slot, so a stale one shadows every slot behind it.
///
/// Rounds, because what the case needs is one round in which the second waiter
/// is really parked when the notification arrives: `num_waiters()` counts a
/// waiter from its `prepare_wait`, so nothing observable says "it is asleep
/// now", and a waiter that has not parked yet reads the new epoch and leaves
/// without needing the wake at all (#430).
fn a_finished_waiter_is_not_claimable(rounds: usize) {
    let notifier = AtomicNotifier::new(2);

    for round in 0..rounds {
        // Slot 0 completes a whole wait, which is what makes it a slot with a
        // past rather than a slot nobody has used.
        let first = AtomicBool::new(false);
        thread::scope(|scope| {
            scope.spawn(|| {
                notifier.prepare_wait(0);
                notifier.commit_wait(0);
                first.store(true, Ordering::Release);
            });

            while notifier.num_waiters() != 1 {
                thread::yield_now();
            }
            notifier.notify_all();
        });
        assert!(
            first.load(Ordering::Acquire),
            "round {round}: the first waiter never left"
        );

        // Now slot 1 waits, and the one wake handed out has to reach it.
        let woke = AtomicBool::new(false);
        let mut woke_in_time = false;
        thread::scope(|scope| {
            scope.spawn(|| {
                notifier.prepare_wait(1);
                notifier.commit_wait(1);
                woke.store(true, Ordering::Release);
            });

            while notifier.num_waiters() != 1 {
                thread::yield_now();
            }
            for _ in 0..SETTLE_YIELDS {
                thread::yield_now();
            }
            notifier.notify_one();

            let deadline = Instant::now() + Duration::from_secs(5);
            while !woke.load(Ordering::Acquire) && Instant::now() < deadline {
                thread::yield_now();
            }
            woke_in_time = woke.load(Ordering::Acquire);
            if !woke_in_time {
                // Unstick the waiter so the scope's join can finish and the
                // assertion below is what fails, not the test harness.
                notifier.notify_all();
            }
        });

        assert!(
            woke_in_time,
            "round {round}: the wake went to a slot whose owner had finished"
        );
    }

    assert_eq!(notifier.num_waiters(), 0);
}

// ============================================================================
// TEST CASES: AtomicNotifier
// ============================================================================

// --- invariants ---

#[test]
fn atomic_notifier_invariants_size() {
    assert_eq!(AtomicNotifier::new(1).size(), 1);
    assert_eq!(AtomicNotifier::new(4).size(), 4);
    assert_eq!(AtomicNotifier::new(31).size(), 31);
}

#[test]
fn atomic_notifier_invariants_initial_num_waiters_is_zero() {
    let notifier = AtomicNotifier::new(8);
    assert_eq!(notifier.num_waiters(), 0);
}

#[test]
fn atomic_notifier_invariants_notify_on_empty_is_noop() {
    let notifier = AtomicNotifier::new(4);
    notifier.notify_one();
    notifier.notify_all();
    notifier.notify_n(0);
    notifier.notify_n(2);
    notifier.notify_n(4);
    notifier.notify_n(5); // n > size() -- exercises the notify_n boundary
    assert_eq!(notifier.num_waiters(), 0);
}

#[test]
fn atomic_notifier_invariants_prepare_cancel_restores_state() {
    let notifier = AtomicNotifier::new(4);

    notifier.prepare_wait(0);
    assert_eq!(notifier.num_waiters(), 1);
    notifier.cancel_wait(0);
    assert_eq!(notifier.num_waiters(), 0);

    notifier.prepare_wait(1);
    notifier.prepare_wait(2);
    assert_eq!(notifier.num_waiters(), 2);
    notifier.cancel_wait(2);
    notifier.cancel_wait(1);
    assert_eq!(notifier.num_waiters(), 0);
}

// --- notify_before_commit ---

worker_matrix! {
    notify_before_commit {
        atomic_notifier_notify_before_commit_5threads(&AtomicNotifier::new(5)),
        atomic_notifier_notify_before_commit_8threads(&AtomicNotifier::new(8)),
    }
}

// --- cancel_only (no ghost waiters) ---

#[test]
fn atomic_notifier_cancel_only_no_ghost_waiters_1thread() {
    let notifier = AtomicNotifier::new(1);
    assert_eq!(notifier.num_waiters(), 0);
    for _ in 0..10_000 {
        notifier.prepare_wait(0);
        notifier.cancel_wait(0);
        assert_eq!(notifier.num_waiters(), 0);
    }
}

#[test]
fn atomic_notifier_cancel_only_no_ghost_waiters_8threads() {
    const N: usize = 8;
    let notifier = AtomicNotifier::new(N);

    let start = AtomicBool::new(false);
    let start_ref = &start;
    let notifier_ref = &notifier;

    thread::scope(|scope| {
        for id in 0..N {
            scope.spawn(move || {
                while !start_ref.load(Ordering::Acquire) {
                    thread::yield_now();
                }
                for _ in 0..500 {
                    notifier_ref.prepare_wait(id);
                    notifier_ref.cancel_wait(id);
                }
            });
        }

        start.store(true, Ordering::Release);
    });

    assert_eq!(notifier.num_waiters(), 0);
}

// --- bug2: notify_n boundary (k == N must invoke the notify_all path) ---

worker_matrix! {
    notify_n_releases_committed {
        atomic_notifier_notify_n_exact_size_4threads(&AtomicNotifier::new(4), 4, 50),
        atomic_notifier_notify_n_exact_size_8threads(&AtomicNotifier::new(8), 8, 50),
        atomic_notifier_notify_n_exact_size_15threads(&AtomicNotifier::new(15), 15, 30),
    }
}

// --- bug3: epoch field (prepare_wait must store the epoch, not the count) ---

worker_matrix! {
    notify_n_releases_committed {
        atomic_notifier_bug3_epoch_field_n2(&AtomicNotifier::new(2), 1, 20),
        atomic_notifier_bug3_epoch_field_n4(&AtomicNotifier::new(4), 2, 20),
        atomic_notifier_bug3_epoch_field_n8(&AtomicNotifier::new(8), 4, 20),
    }
}

// --- notify_n(0) is a strict noop ---

worker_matrix! {
    notify_n_zero_is_noop {
        atomic_notifier_notify_n_zero_is_noop_4threads(&AtomicNotifier::new(4)),
        atomic_notifier_notify_n_zero_is_noop_8threads(&AtomicNotifier::new(8)),
    }
}

// --- notify_one fires in the pre-waiter window ---

worker_matrix! {
    notify_one_at_prewaiter_window {
        atomic_notifier_notify_one_at_prewaiter_window_50rounds(&AtomicNotifier::new(1), 50),
        atomic_notifier_notify_one_at_prewaiter_window_200rounds(&AtomicNotifier::new(1), 200),
    }
}

// --- rapid_cancel_integrity ---
// Stresses that many prepare+cancel cycles keep the waiter count accurate and
// leave the notifier fully functional.

worker_matrix! {
    rapid_cancel_integrity {
        atomic_notifier_rapid_cancel_integrity_1k_cycles(&AtomicNotifier::new(2), 1_000),
        atomic_notifier_rapid_cancel_integrity_100k_cycles(&AtomicNotifier::new(2), 100_000),
    }
}

// --- a wake goes to a waiter that is still waiting ---

worker_matrix! {
    a_spurious_unpark_does_not_end_a_wait {
        atomic_notifier_a_spurious_unpark_does_not_end_a_wait(&AtomicNotifier::new(1)),
    }
}

worker_matrix! {
    a_finished_waiter_is_not_claimable {
        atomic_notifier_a_finished_waiter_is_not_claimable_50rounds(50),
    }
}

// --- the dump, and the drop ---

/// A dump reports the notifier's own state: the two halves of the state word,
/// read apart, and the slot count beside them. The two fields share one word,
/// so a `Debug` that reported either of them wrong -- or reported the word
/// itself -- would be a dump that lies about the thing it exists to show, and
/// a scheduling bug read off it would be read off the wrong number (#430).
#[test]
fn atomic_notifier_debug_reports_its_own_state() {
    let notifier = AtomicNotifier::new(4);

    let dump = format!("{notifier:?}");
    assert!(dump.starts_with("AtomicNotifier"), "{dump}");
    assert!(dump.contains("size: 4"), "{dump}");
    assert!(dump.contains("epoch: 0"), "{dump}");
    assert!(dump.contains("num_waiters: 0"), "{dump}");

    // Both halves away from zero at once, and moved by different amounts, so
    // that neither can stand in for the other.
    notifier.prepare_wait(0);
    notifier.prepare_wait(1);
    notifier.notify_all();

    let dump = format!("{notifier:?}");
    assert!(dump.contains("epoch: 1"), "{dump}");
    assert!(dump.contains("num_waiters: 2"), "{dump}");

    notifier.cancel_wait(1);
    notifier.cancel_wait(0);
    let dump = format!("{notifier:?}");
    assert!(dump.contains("epoch: 1"), "{dump}");
    assert!(dump.contains("num_waiters: 0"), "{dump}");
}

/// Dropping a notifier a thread is still waiting on is a programming error,
/// not a condition to recover from: the thread would be left parked with
/// nothing to wake it. Upstream asserts it in its destructor and so does this
/// port, which is a promise only a debug build keeps -- hence the `cfg` (#430).
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "an AtomicNotifier was dropped with waiters still registered")]
fn dropping_an_atomic_notifier_with_a_waiter_registered_panics() {
    let notifier = AtomicNotifier::new(2);
    notifier.prepare_wait(0);
    drop(notifier);
}

// ============================================================================
// TEST CASES: NonblockingNotifier
// ============================================================================

// --- invariants (NonblockingNotifier counts only committed threads) ---

#[test]
fn nonblocking_notifier_invariants_size() {
    assert_eq!(NonblockingNotifier::new(1).size(), 1);
    assert_eq!(NonblockingNotifier::new(4).size(), 4);
    assert_eq!(NonblockingNotifier::new(31).size(), 31);
}

#[test]
fn nonblocking_notifier_invariants_capacity() {
    let notifier = NonblockingNotifier::new(8);
    assert_eq!(notifier.capacity(), 1 << NonblockingNotifier::STACK_BITS);
}

#[test]
fn nonblocking_notifier_invariants_initial_num_waiters_is_zero() {
    let notifier = NonblockingNotifier::new(8);
    assert_eq!(notifier.num_waiters(), 0);
}

#[test]
fn nonblocking_notifier_invariants_notify_on_empty_is_noop() {
    let notifier = NonblockingNotifier::new(4);
    notifier.notify_one();
    notifier.notify_all();
    notifier.notify_n(0);
    notifier.notify_n(2);
    notifier.notify_n(4);
    notifier.notify_n(5); // n > size() -- exercises the notify_n boundary
    assert_eq!(notifier.num_waiters(), 0);
}

#[test]
fn nonblocking_notifier_invariants_prepare_does_not_increment_num_waiters() {
    // Pre-waiters must NOT show up in num_waiters(); only committed threads do.
    let notifier = NonblockingNotifier::new(4);
    notifier.prepare_wait(0);
    assert_eq!(notifier.num_waiters(), 0);
    notifier.cancel_wait(0);
    assert_eq!(notifier.num_waiters(), 0);
}

#[test]
fn nonblocking_notifier_invariants_num_waiters_counts_only_committed() {
    num_waiters_counts_only_committed();
}

// --- notify_before_commit ---

worker_matrix! {
    notify_before_commit {
        nonblocking_notifier_notify_before_commit_1thread(&NonblockingNotifier::new(1)),
        nonblocking_notifier_notify_before_commit_2threads(&NonblockingNotifier::new(2)),
        nonblocking_notifier_notify_before_commit_4threads(&NonblockingNotifier::new(4)),
        nonblocking_notifier_notify_before_commit_8threads(&NonblockingNotifier::new(8)),
        nonblocking_notifier_notify_before_commit_15threads(&NonblockingNotifier::new(15)),
    }
}

// --- cancel_only (no ghost waiters) ---

#[test]
fn nonblocking_notifier_cancel_only_no_ghost_waiters_1thread() {
    let notifier = NonblockingNotifier::new(1);
    assert_eq!(notifier.num_waiters(), 0);
    for _ in 0..2_000 {
        notifier.prepare_wait(0);
        assert_eq!(notifier.num_waiters(), 0);
        notifier.cancel_wait(0);
        assert_eq!(notifier.num_waiters(), 0);
    }
}

#[test]
fn nonblocking_notifier_cancel_only_no_ghost_waiters_8threads() {
    const N: usize = 8;
    let notifier = NonblockingNotifier::new(N);

    // Each thread does sequential prepare+cancel rounds. Upstream drives them
    // off a round counter rather than letting them run free, because a
    // NonblockingNotifier resolves tickets in order: a thread that has taken a
    // ticket yields until the one ahead of it has decided.
    let round = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let (round, stop, notifier_ref) = (&round, &stop, &notifier);

    thread::scope(|scope| {
        for id in 0..N {
            scope.spawn(move || {
                let mut local_round = 0;

                while !stop.load(Ordering::Relaxed) {
                    while round.load(Ordering::Acquire) == local_round
                        && !stop.load(Ordering::Relaxed)
                    {
                        thread::yield_now();
                    }

                    if stop.load(Ordering::Relaxed) {
                        break;
                    }

                    notifier_ref.prepare_wait(id);
                    notifier_ref.cancel_wait(id);

                    local_round += 1;
                }
            });
        }

        for r in 1..=300 {
            round.store(r, Ordering::Release);
            // Allow all threads time to complete the round before moving on.
            thread::yield_now();
        }

        stop.store(true, Ordering::Release);
        round.fetch_add(1, Ordering::Release);
    });

    assert_eq!(notifier.num_waiters(), 0);
}

// --- notify_n boundary (k == N must invoke the notify_all path) ---

worker_matrix! {
    notify_n_releases_committed {
        nonblocking_notifier_notify_n_exact_size_4threads(&NonblockingNotifier::new(4), 4, 50),
        nonblocking_notifier_notify_n_exact_size_8threads(&NonblockingNotifier::new(8), 8, 50),
        nonblocking_notifier_notify_n_exact_size_15threads(&NonblockingNotifier::new(15), 15, 30),
    }
}

// --- notify_n(0) is a strict noop ---

worker_matrix! {
    notify_n_zero_is_noop {
        nonblocking_notifier_notify_n_zero_is_noop_4threads(&NonblockingNotifier::new(4)),
        nonblocking_notifier_notify_n_zero_is_noop_8threads(&NonblockingNotifier::new(8)),
    }
}

// --- notify_n(k) wakes k and not one more ---

worker_matrix! {
    notify_n_does_not_over_release {
        nonblocking_notifier_notify_n_no_over_release_n8_k1(&NonblockingNotifier::new(8), 1, 100),
        nonblocking_notifier_notify_n_no_over_release_n8_k3(&NonblockingNotifier::new(8), 3, 100),
        nonblocking_notifier_notify_n_no_over_release_n15_k7(&NonblockingNotifier::new(15), 7, 60),
        nonblocking_notifier_notify_n_no_over_release_n31_k15(&NonblockingNotifier::new(31), 15, 40),
    }
}

// --- rapid_cancel_integrity ---
// Stresses that many prepare+cancel cycles keep the state word accurate and
// leave the notifier fully functional.

worker_matrix! {
    rapid_cancel_integrity {
        nonblocking_notifier_rapid_cancel_integrity_1k_cycles(&NonblockingNotifier::new(2), 1_000),
        nonblocking_notifier_rapid_cancel_integrity_100k_cycles(&NonblockingNotifier::new(2), 100_000),
    }
}

worker_matrix! {
    rapid_cancel_epoch_integrity {
        nonblocking_notifier_rapid_cancel_epoch_integrity_1k_cycles(1_000),
        nonblocking_notifier_rapid_cancel_epoch_integrity_100k_cycles(100_000),
    }
}

// --- notify_one fires in the pre-waiter window ---

worker_matrix! {
    notify_one_at_prewaiter_window {
        nonblocking_notifier_notify_one_at_prewaiter_window_50rounds(&NonblockingNotifier::new(1), 50),
        nonblocking_notifier_notify_one_at_prewaiter_window_200rounds(&NonblockingNotifier::new(1), 200),
    }
}

// --- notify_n(k) over the pre-waiter arm resolves k tickets and no more ---

worker_matrix! {
    notify_n_resolves_k_prewaiter_tickets {
        nonblocking_notifier_notify_n_resolves_1_of_4_prewaiter_tickets(&NonblockingNotifier::new(4), 1, 20),
        nonblocking_notifier_notify_n_resolves_3_of_8_prewaiter_tickets(&NonblockingNotifier::new(8), 3, 20),
        nonblocking_notifier_notify_n_resolves_7_of_15_prewaiter_tickets(&NonblockingNotifier::new(15), 7, 10),
    }
}

// --- a wake goes to a waiter that is still waiting ---

worker_matrix! {
    a_spurious_unpark_does_not_end_a_wait {
        nonblocking_notifier_a_spurious_unpark_does_not_end_a_wait(&NonblockingNotifier::new(1)),
    }
}

// --- the dump, and the drop ---

/// A dump reports the notifier's own state: the epoch, the pre-waiter count
/// and the stack top, read apart out of the one word that holds all three, and
/// the slot count beside them. The empty stack is all ones rather than zero --
/// index 0 is a real waiter -- so a `Debug` that passed the raw field through
/// would report a waiter that is not there, which is the one thing a dump of a
/// sleeping executor must not do (#430).
#[test]
fn nonblocking_notifier_debug_reports_its_own_state() {
    let notifier = NonblockingNotifier::new(4);

    let dump = format!("{notifier:?}");
    assert!(dump.starts_with("NonblockingNotifier"), "{dump}");
    assert!(dump.contains("size: 4"), "{dump}");
    assert!(dump.contains("epoch: 0"), "{dump}");
    assert!(dump.contains("prewaiters: 0"), "{dump}");
    assert!(dump.contains("stack_top: None"), "{dump}");

    notifier.prepare_wait(0);
    let dump = format!("{notifier:?}");
    assert!(dump.contains("epoch: 0"), "{dump}");
    assert!(dump.contains("prewaiters: 1"), "{dump}");
    assert!(dump.contains("stack_top: None"), "{dump}");

    // A cancel resolves the ticket: the epoch moves off zero while the stack
    // stays empty, which is the state an all-ones stack field has to survive.
    notifier.cancel_wait(0);
    let dump = format!("{notifier:?}");
    assert!(dump.contains("epoch: 1"), "{dump}");
    assert!(dump.contains("prewaiters: 0"), "{dump}");
    assert!(dump.contains("stack_top: None"), "{dump}");
}

/// Dropping a notifier a thread is still waiting on is a programming error,
/// not a condition to recover from: the thread would be left parked with
/// nothing to wake it. Upstream asserts it in its destructor and so does this
/// port, which is a promise only a debug build keeps -- hence the `cfg` (#430).
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "a NonblockingNotifier was dropped with waiters still registered")]
fn dropping_a_nonblocking_notifier_with_a_waiter_registered_panics() {
    let notifier = NonblockingNotifier::new(2);
    notifier.prepare_wait(0);
    drop(notifier);
}

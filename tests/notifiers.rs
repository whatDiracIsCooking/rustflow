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

// The notifiers are built out of `loom`'s atomics under `cfg(loom)`, and
// loom's only run inside its own model: this suite is the `std` one, and
// `tests/loom_notifier.rs` is the other half.
#![cfg(not(loom))]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rustflow::executor::notifier::{AtomicNotifier, NonblockingNotifier, Notifier};

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

#[test]
fn atomic_notifier_notify_before_commit_5threads() {
    notify_before_commit(&AtomicNotifier::new(5));
}

#[test]
fn atomic_notifier_notify_before_commit_8threads() {
    notify_before_commit(&AtomicNotifier::new(8));
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

#[test]
fn atomic_notifier_notify_n_exact_size_4threads() {
    notify_n_releases_committed(&AtomicNotifier::new(4), 4, 50);
}

#[test]
fn atomic_notifier_notify_n_exact_size_8threads() {
    notify_n_releases_committed(&AtomicNotifier::new(8), 8, 50);
}

#[test]
fn atomic_notifier_notify_n_exact_size_15threads() {
    notify_n_releases_committed(&AtomicNotifier::new(15), 15, 30);
}

// --- bug3: epoch field (prepare_wait must store the epoch, not the count) ---

#[test]
fn atomic_notifier_bug3_epoch_field_n2() {
    notify_n_releases_committed(&AtomicNotifier::new(2), 1, 20);
}

#[test]
fn atomic_notifier_bug3_epoch_field_n4() {
    notify_n_releases_committed(&AtomicNotifier::new(4), 2, 20);
}

#[test]
fn atomic_notifier_bug3_epoch_field_n8() {
    notify_n_releases_committed(&AtomicNotifier::new(8), 4, 20);
}

// --- notify_n(0) is a strict noop ---

#[test]
fn atomic_notifier_notify_n_zero_is_noop_4threads() {
    notify_n_zero_is_noop(&AtomicNotifier::new(4));
}

#[test]
fn atomic_notifier_notify_n_zero_is_noop_8threads() {
    notify_n_zero_is_noop(&AtomicNotifier::new(8));
}

// --- notify_one fires in the pre-waiter window ---

#[test]
fn atomic_notifier_notify_one_at_prewaiter_window_50rounds() {
    notify_one_at_prewaiter_window(&AtomicNotifier::new(1), 50);
}

#[test]
fn atomic_notifier_notify_one_at_prewaiter_window_200rounds() {
    notify_one_at_prewaiter_window(&AtomicNotifier::new(1), 200);
}

// --- rapid_cancel_integrity ---
// Stresses that many prepare+cancel cycles keep the waiter count accurate and
// leave the notifier fully functional.

#[test]
fn atomic_notifier_rapid_cancel_integrity_1k_cycles() {
    rapid_cancel_integrity(&AtomicNotifier::new(2), 1_000);
}

#[test]
fn atomic_notifier_rapid_cancel_integrity_100k_cycles() {
    rapid_cancel_integrity(&AtomicNotifier::new(2), 100_000);
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

#[test]
fn nonblocking_notifier_notify_before_commit_1thread() {
    notify_before_commit(&NonblockingNotifier::new(1));
}

#[test]
fn nonblocking_notifier_notify_before_commit_2threads() {
    notify_before_commit(&NonblockingNotifier::new(2));
}

#[test]
fn nonblocking_notifier_notify_before_commit_4threads() {
    notify_before_commit(&NonblockingNotifier::new(4));
}

#[test]
fn nonblocking_notifier_notify_before_commit_8threads() {
    notify_before_commit(&NonblockingNotifier::new(8));
}

#[test]
fn nonblocking_notifier_notify_before_commit_15threads() {
    notify_before_commit(&NonblockingNotifier::new(15));
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

#[test]
fn nonblocking_notifier_notify_n_exact_size_4threads() {
    notify_n_releases_committed(&NonblockingNotifier::new(4), 4, 50);
}

#[test]
fn nonblocking_notifier_notify_n_exact_size_8threads() {
    notify_n_releases_committed(&NonblockingNotifier::new(8), 8, 50);
}

#[test]
fn nonblocking_notifier_notify_n_exact_size_15threads() {
    notify_n_releases_committed(&NonblockingNotifier::new(15), 15, 30);
}

// --- notify_n(0) is a strict noop ---

#[test]
fn nonblocking_notifier_notify_n_zero_is_noop_4threads() {
    notify_n_zero_is_noop(&NonblockingNotifier::new(4));
}

#[test]
fn nonblocking_notifier_notify_n_zero_is_noop_8threads() {
    notify_n_zero_is_noop(&NonblockingNotifier::new(8));
}

// --- notify_n(k) wakes k and not one more ---

#[test]
fn nonblocking_notifier_notify_n_no_over_release_n8_k1() {
    notify_n_does_not_over_release(&NonblockingNotifier::new(8), 1, 100);
}

#[test]
fn nonblocking_notifier_notify_n_no_over_release_n8_k3() {
    notify_n_does_not_over_release(&NonblockingNotifier::new(8), 3, 100);
}

#[test]
fn nonblocking_notifier_notify_n_no_over_release_n15_k7() {
    notify_n_does_not_over_release(&NonblockingNotifier::new(15), 7, 60);
}

#[test]
fn nonblocking_notifier_notify_n_no_over_release_n31_k15() {
    notify_n_does_not_over_release(&NonblockingNotifier::new(31), 15, 40);
}

// --- rapid_cancel_integrity ---
// Stresses that many prepare+cancel cycles keep the state word accurate and
// leave the notifier fully functional.

#[test]
fn nonblocking_notifier_rapid_cancel_integrity_1k_cycles() {
    rapid_cancel_integrity(&NonblockingNotifier::new(2), 1_000);
}

#[test]
fn nonblocking_notifier_rapid_cancel_integrity_100k_cycles() {
    rapid_cancel_integrity(&NonblockingNotifier::new(2), 100_000);
}

#[test]
fn nonblocking_notifier_rapid_cancel_epoch_integrity_1k_cycles() {
    rapid_cancel_epoch_integrity(1_000);
}

#[test]
fn nonblocking_notifier_rapid_cancel_epoch_integrity_100k_cycles() {
    rapid_cancel_epoch_integrity(100_000);
}

// --- notify_one fires in the pre-waiter window ---

#[test]
fn nonblocking_notifier_notify_one_at_prewaiter_window_50rounds() {
    notify_one_at_prewaiter_window(&NonblockingNotifier::new(1), 50);
}

#[test]
fn nonblocking_notifier_notify_one_at_prewaiter_window_200rounds() {
    notify_one_at_prewaiter_window(&NonblockingNotifier::new(1), 200);
}

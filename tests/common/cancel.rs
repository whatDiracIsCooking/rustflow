//! Translated from `unittests/test_runtimes.cpp` (Taskflow v4.1.0).
//!
//! The one handshake a task body needs to see its own run cancelled:
//! upstream's `Runtime.Cancel` loop, which `tests/scope.rs` repeats to put
//! every child of a scope under a run that is already cancelled.
//!
//! A topic of its own rather than part of `common/spin.rs`, which every
//! waiting suite here pulls: that file's `spin_until` waits on a condition
//! some *other* thread sets, and this loop sets the flag it is waiting on
//! from inside itself, so a caller of one wants nothing of the other.

use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// How long the handshake waits for the cancellation to become visible
/// before it gives up.
///
/// The same 120 seconds `common/spin.rs` gives every other wait in these
/// suites, for the same reason: `cargo test` has no per-case timeout where
/// doctest gave upstream one, so a scheduling defect should fail the case
/// that is waiting rather than hang the binary.
const GIVE_UP_AFTER: Duration = Duration::from_secs(120);

/// Spins until `is_cancelled` answers true, recording that in `cancelled`:
/// upstream's `while(!cancelled) { std::this_thread::yield(); if
/// (rt.is_cancelled()) { cancelled = true; break; } }`.
///
/// Cancellation is not preemptive -- nothing interrupts a body -- so asking
/// is the only way a running task learns of it, and the caller's own cancel
/// has to arrive *after* this body has started, or it would cancel the run
/// before the task was ever dequeued.
///
/// The body spins rather than sleeping because it is on a worker thread --
/// possibly the only one this executor has -- and sleeping there would be the
/// deadlock [`Executor::corun`](rustflow::Executor::corun) exists to avoid.
pub fn spin_until_cancelled(cancelled: &AtomicBool, mut is_cancelled: impl FnMut() -> bool) {
    let deadline = Instant::now() + GIVE_UP_AFTER;
    while !cancelled.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "gave up waiting for the cancellation to be visible"
        );
        thread::yield_now();
        if is_cancelled() {
            cancelled.store(true, Ordering::Release);
            break;
        }
    }
}

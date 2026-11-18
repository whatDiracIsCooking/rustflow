//! Waiting on another thread's progress, which is how every handshake in
//! these suites is phrased.
//!
//! A topic of its own for the reason [`super`]'s docs give: most suites
//! count, and only the ones with a rendezvous in them spin.

use std::thread;
use std::time::{Duration, Instant};

/// How long a spin waits for what it is spinning on before it gives up.
///
/// Far longer than any rendezvous in these suites needs, even pinned to a
/// single core with eight workers sharing it: nothing passes because of this
/// number, and the only thing that reaches it is a thread that is never
/// going to arrive. It exists because `cargo test` has no per-case timeout
/// where doctest gave upstream one, so a scheduling defect should fail the
/// case that is waiting rather than hang the binary.
const GIVE_UP_AFTER: Duration = Duration::from_secs(120);

/// Upstream's `while(!ready) std::this_thread::yield();`, with a deadline.
///
/// `what` is what the caller is waiting for, phrased to finish the sentence
/// "gave up waiting for ...", because that message is the whole diagnostic
/// when a scheduling bug trips it.
///
/// `yield_now` rather than a condvar because one end of most handshakes here
/// is a task body: a worker that parked would be a worker not running the
/// rest of the work, up to and including the very task the other end is
/// waiting for.
///
/// A caller that must *not* fail on the deadline -- a body spinning inside a
/// worker that may be mid-unwind, say -- folds its own give-up into `ready`
/// rather than reaching for a second spin: see `wait_or_give_up` in
/// `tests/basics.rs`.
pub fn spin_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + GIVE_UP_AFTER;
    while !ready() {
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        thread::yield_now();
    }
}

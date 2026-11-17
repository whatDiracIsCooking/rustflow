//! Translated from `taskflow/core/atomic_notifier.hpp` (Taskflow v4.1.0).
//!
//! A notifier is the sleep/wake side of work stealing: a worker that steals and
//! finds nothing parks here until someone pushes work. It plays the role of a
//! condition variable, except that the predicate it waits on -- "is there work
//! anywhere?" -- is not guarded by any mutex, so a notification costs nothing
//! when no worker is asleep.
//!
//! The protocol is two-phase and the caller drives it:
//!
//! ```text
//! if predicate { return act(); }
//! notifier.prepare_wait(worker_id);
//! if predicate { notifier.cancel_wait(worker_id); return act(); }
//! notifier.commit_wait(worker_id);
//! ```
//!
//! while the notifying side publishes the predicate and then calls
//! [`Notifier::notify_one`] or a sibling. The ordering is Dekker's algorithm: a
//! waiter writes [`AtomicNotifier`]'s state word and *then* reads the
//! predicate; a notifier writes the predicate and *then* reads the state word,
//! with sequentially consistent fences between. One of the two therefore sees
//! the other's write, so the case where the waiter sleeps on work that has
//! already arrived cannot happen.
//!
//! # The one real departure from upstream
//!
//! Upstream parks a worker with C++20's `std::atomic::wait`/`notify_one` on the
//! state word itself -- a futex wait on a 64-bit location. `std` has no such
//! operation (`std::sync::atomic` grew no `wait`), and this crate takes no
//! dependency to get one, so the per-waiter sleep is [`std::thread::park`] and
//! the wake is [`std::thread::Thread::unpark`]. That costs a registry: each
//! waiter publishes its own [`Thread`] handle and an "armed" flag before it
//! checks the epoch, and a notifier claims an armed waiter to wake it.
//!
//! Two consequences worth naming, both of which the epoch loop in
//! [`AtomicNotifier::commit_wait`] already tolerates:
//!
//! - A wake is *best effort*, exactly as the futex one is: a notifier that
//!   finds no armed waiter wakes nobody, and that is correct, because a waiter
//!   not yet armed has not yet read the epoch and so will read the new one.
//! - A claimed waiter that leaves without parking leaves an unconsumed unpark
//!   token behind, which makes its next `park` return at once. That is a
//!   spurious wakeup, which the loop handles by re-checking the epoch.
//!
//! The wake is still targeted rather than a broadcast: [`AtomicNotifier::notify_one`]
//! claims one armed waiter and no more. What keeps `notify_n(k)` from spending
//! two of its `k` wakes on the same waiter is the order of the two checks after
//! `park` returns -- the epoch is re-read *before* the waiter re-arms, so a
//! waiter whose epoch has moved on is never armed again and cannot be claimed
//! a second time.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::thread::{self, Thread};

/// The sleep/wake protocol a worker uses when stealing finds nothing.
///
/// The three waiting methods take the caller's worker id, which indexes the
/// per-waiter slot it owns: a worker may be in one wait at a time, and
/// `prepare_wait`, `cancel_wait` and `commit_wait` must all be called from the
/// thread that owns that id. The notifying methods may be called from any
/// thread, including one that is not a waiter at all.
///
/// The trait exists so that the nonblocking variant of upstream's notifier can
/// slot in behind the same surface later; [`DefaultNotifier`] names whichever
/// implementation the executor builds on today.
pub trait Notifier {
    /// The number of waiter slots, i.e. the worker count this notifier was
    /// built for. Valid worker ids are `0..size()`.
    fn size(&self) -> usize;

    /// How many waiters this notifier is currently accounting for.
    ///
    /// For [`AtomicNotifier`] a waiter counts from [`Notifier::prepare_wait`],
    /// not from [`Notifier::commit_wait`], so a non-zero count does not mean
    /// anybody is actually asleep.
    fn num_waiters(&self) -> usize;

    /// Opens a wait: registers `worker_id` as a waiter and takes a snapshot of
    /// the notification epoch, against which [`Notifier::commit_wait`] decides
    /// whether a notification has already arrived.
    fn prepare_wait(&self, worker_id: usize);

    /// Abandons a wait opened by [`Notifier::prepare_wait`] without sleeping,
    /// which is what a worker does when its second predicate check finds work.
    fn cancel_wait(&self, worker_id: usize);

    /// Closes a wait opened by [`Notifier::prepare_wait`], sleeping until a
    /// notification arrives -- or returning immediately if one arrived inside
    /// the window between the two calls.
    fn commit_wait(&self, worker_id: usize);

    /// Wakes one waiter, if there is one. Cheap when there is not.
    fn notify_one(&self);

    /// Wakes every waiter, if there are any. Cheap when there are not.
    fn notify_all(&self);

    /// Wakes `n` waiters, or every waiter when `n >= size()`.
    fn notify_n(&self, n: usize);
}

/// The notifier the executor builds on today.
///
/// It is [`AtomicNotifier`] until the nonblocking variant of upstream's
/// notifier is ported, which is what the [`Notifier`] trait is for.
pub type DefaultNotifier = AtomicNotifier;

/// A notifier whose whole shared state is one 64-bit word: a notification
/// epoch in the high 32 bits and a waiter count in the low 32.
///
/// ```
/// use rustflow::executor::notifier::{AtomicNotifier, Notifier};
///
/// let notifier = AtomicNotifier::new(4);
/// assert_eq!(notifier.size(), 4);
/// assert_eq!(notifier.num_waiters(), 0);
///
/// // Notifying with nobody waiting is a no-op, not an error.
/// notifier.notify_all();
///
/// // Opening a wait and then finding work is the common case.
/// notifier.prepare_wait(0);
/// assert_eq!(notifier.num_waiters(), 1);
/// notifier.cancel_wait(0);
/// assert_eq!(notifier.num_waiters(), 0);
/// ```
pub struct AtomicNotifier {
    /// The notification epoch in the high 32 bits, the waiter count in the low
    /// 32. One word, so that a notifier reads both in a single atomic load.
    state: AtomicU64,
    /// One slot per worker id, each on its own pair of cache lines.
    waiters: Box<[Waiter]>,
}

/// One worker's slot: the epoch it is waiting out, and how to wake it.
///
/// Padded to two cache lines, as upstream pads its own `Waiter`: the whole
/// point of a per-waiter slot is that an idle worker's bookkeeping does not
/// bounce the cache line a busy one is using.
#[repr(align(128))]
struct Waiter {
    /// The epoch [`AtomicNotifier::prepare_wait`] saw, kept in the shifted-down
    /// form so the comparison in `commit_wait` needs no cast. Only the owning
    /// worker reads or writes it; it is atomic because the notifier is shared,
    /// not because two threads race on it.
    epoch: AtomicU64,
    /// Whether this waiter is parked, or close enough to parked that a notifier
    /// must wake it. Claimed with a swap, so exactly one notifier wakes it.
    armed: AtomicBool,
    /// The parked thread's handle. Behind a lock because a notifier reads it
    /// while the waiter may be replacing it, and `Thread` is not an atomic.
    thread: Mutex<Option<Thread>>,
}

/// One waiter, as a delta to the low half of the state word.
const WAITER_INC: u64 = 1;
/// Where the epoch starts within the state word.
const EPOCH_SHIFT: u32 = 32;
/// One notification, as a delta to the high half of the state word.
const EPOCH_INC: u64 = 1 << EPOCH_SHIFT;
/// The bits of the state word that hold the waiter count.
const WAITER_MASK: u64 = EPOCH_INC - 1;

impl AtomicNotifier {
    /// Builds a notifier with `n` waiter slots, one per worker.
    #[must_use]
    pub fn new(n: usize) -> Self {
        Self {
            state: AtomicU64::new(0),
            waiters: (0..n).map(|_| Waiter::new()).collect(),
        }
    }

    /// Bumps the epoch, if anybody is waiting, and reports whether it did.
    ///
    /// The leading fence is upstream's: it orders the caller's predicate store
    /// before the state load below, which is the notifying half of Dekker's
    /// algorithm. The compare-exchange is sequentially consistent so that the
    /// new epoch lands in the total order before [`Self::claim_one`] or
    /// [`Self::claim_all`] reads the armed flags.
    fn bump_epoch(&self) -> bool {
        fence(Ordering::SeqCst);
        let mut state = self.state.load(Ordering::Relaxed);
        while state & WAITER_MASK != 0 {
            // Wrapping, as upstream's unsigned arithmetic wraps: the epoch is a
            // 32-bit field inside the word and is allowed to roll over.
            match self.state.compare_exchange_weak(
                state,
                state.wrapping_add(EPOCH_INC),
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => state = observed,
            }
        }
        false
    }

    /// Wakes the first armed waiter, if any.
    fn claim_one(&self) {
        fence(Ordering::SeqCst);
        for waiter in &self.waiters {
            if waiter.armed.swap(false, Ordering::SeqCst) {
                waiter.unpark();
                return;
            }
        }
    }

    /// Wakes every armed waiter.
    fn claim_all(&self) {
        fence(Ordering::SeqCst);
        for waiter in &self.waiters {
            if waiter.armed.swap(false, Ordering::SeqCst) {
                waiter.unpark();
            }
        }
    }

    /// The epoch currently published in the state word, shifted down.
    fn epoch(&self) -> u64 {
        self.state.load(Ordering::SeqCst) >> EPOCH_SHIFT
    }
}

impl Notifier for AtomicNotifier {
    fn size(&self) -> usize {
        self.waiters.len()
    }

    fn num_waiters(&self) -> usize {
        let count = self.state.load(Ordering::Relaxed) & WAITER_MASK;
        usize::try_from(count).expect("a waiter count is bounded by the worker count")
    }

    fn prepare_wait(&self, worker_id: usize) {
        let prev = self.state.fetch_add(WAITER_INC, Ordering::Relaxed);
        // The epoch, not the whole word: a waiter is waiting for the *count* to
        // be irrelevant and the epoch to move.
        self.waiters[worker_id]
            .epoch
            .store(prev >> EPOCH_SHIFT, Ordering::Relaxed);
        // The waiting half of Dekker's algorithm: this fence orders the state
        // write above before the caller's second predicate check below.
        fence(Ordering::SeqCst);
    }

    fn cancel_wait(&self, _worker_id: usize) {
        // The slot needs no cleanup, which is why upstream leaves its own
        // parameter unnamed: `prepare_wait` only wrote the slot's epoch, and
        // the next `prepare_wait` overwrites it. Only the count has to go back.
        self.state.fetch_sub(WAITER_INC, Ordering::Relaxed);
    }

    fn commit_wait(&self, worker_id: usize) {
        let waiter = &self.waiters[worker_id];
        let epoch = waiter.epoch.load(Ordering::Relaxed);

        waiter.arm();
        loop {
            if self.epoch() != epoch {
                break;
            }
            thread::park();
            // Re-read the epoch *before* re-arming. A waiter that is leaving
            // must not be claimable, or a second `notify_one` could spend its
            // wake on it and leave a genuine waiter asleep.
            if self.epoch() != epoch {
                break;
            }
            // A spurious wakeup with the epoch unmoved: arm again and sleep on.
            waiter.armed.store(true, Ordering::SeqCst);
            fence(Ordering::SeqCst);
        }
        waiter.disarm();

        self.state.fetch_sub(WAITER_INC, Ordering::Relaxed);
    }

    fn notify_one(&self) {
        if self.bump_epoch() {
            self.claim_one();
        }
    }

    fn notify_all(&self) {
        if self.bump_epoch() {
            self.claim_all();
        }
    }

    fn notify_n(&self, n: usize) {
        if n >= self.waiters.len() {
            self.notify_all();
        } else {
            for _ in 0..n {
                self.notify_one();
            }
        }
    }
}

impl Drop for AtomicNotifier {
    fn drop(&mut self) {
        // Upstream asserts the same thing in its destructor: dropping a
        // notifier somebody is still waiting on would leave that thread parked
        // forever with nothing left to wake it.
        debug_assert_eq!(
            self.state.load(Ordering::Relaxed) & WAITER_MASK,
            0,
            "an AtomicNotifier was dropped with waiters still registered"
        );
    }
}

impl core::fmt::Debug for AtomicNotifier {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let state = self.state.load(Ordering::Relaxed);
        f.debug_struct("AtomicNotifier")
            .field("size", &self.waiters.len())
            .field("epoch", &(state >> EPOCH_SHIFT))
            .field("num_waiters", &(state & WAITER_MASK))
            .finish()
    }
}

impl Waiter {
    /// A slot nobody is waiting in.
    fn new() -> Self {
        Self {
            epoch: AtomicU64::new(0),
            armed: AtomicBool::new(false),
            thread: Mutex::new(None),
        }
    }

    /// Publishes the calling thread as this slot's sleeper and arms the slot.
    ///
    /// The fence pairs with the one in [`AtomicNotifier::claim_one`]: between
    /// them, a waiter that misses the new epoch is a waiter the notifier sees
    /// armed.
    fn arm(&self) {
        *self.lock() = Some(thread::current());
        self.armed.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
    }

    /// Takes the slot out of service once its wait is over.
    fn disarm(&self) {
        self.armed.store(false, Ordering::SeqCst);
        *self.lock() = None;
    }

    /// Wakes whoever is parked in this slot. Harmless if nobody is: an unpark
    /// on a thread that is not parked leaves a token that its next park spends,
    /// and one on a thread that has finished does nothing.
    fn unpark(&self) {
        let thread = self.lock().clone();
        if let Some(thread) = thread {
            thread.unpark();
        }
    }

    /// The slot's thread handle. Poisoning carries no information here -- the
    /// guarded value is one `Option<Thread>` and nothing under the lock can
    /// panic -- so a poisoned lock is taken rather than propagated.
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Thread>> {
        self.thread
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::{AtomicNotifier, EPOCH_SHIFT, Notifier, WAITER_MASK, Waiter};
    use std::sync::atomic::Ordering;

    /// The whole design rests on the two fields sharing one word without
    /// overlapping, which no test outside this module can see.
    #[test]
    fn the_state_word_splits_into_an_epoch_and_a_count() {
        let notifier = AtomicNotifier::new(2);

        notifier.prepare_wait(0);
        notifier.prepare_wait(1);
        notifier.notify_all();

        let state = notifier.state.load(Ordering::Relaxed);
        assert_eq!(state >> EPOCH_SHIFT, 1, "one notification, one epoch");
        assert_eq!(state & WAITER_MASK, 2, "both waiters still registered");

        notifier.cancel_wait(0);
        notifier.cancel_wait(1);
        assert_eq!(notifier.state.load(Ordering::Relaxed) >> EPOCH_SHIFT, 1);
        assert_eq!(notifier.num_waiters(), 0);
    }

    /// `prepare_wait` records the epoch, not the state word: a waiter that
    /// compared whole words would see the count move and wake on nothing.
    #[test]
    fn prepare_wait_records_the_epoch_alone() {
        let notifier = AtomicNotifier::new(2);

        notifier.notify_all(); // no waiters: the epoch stays at 0
        notifier.prepare_wait(0);
        notifier.prepare_wait(1);

        assert_eq!(notifier.waiters[0].epoch.load(Ordering::Relaxed), 0);
        assert_eq!(
            notifier.waiters[1].epoch.load(Ordering::Relaxed),
            0,
            "the second waiter saw a count of 1, which is not an epoch"
        );

        notifier.cancel_wait(1);
        notifier.cancel_wait(0);
    }

    /// A notification with nobody registered must not move the epoch, or every
    /// later waiter would see a wake it was not there for.
    #[test]
    fn notifying_an_empty_notifier_does_not_move_the_epoch() {
        let notifier = AtomicNotifier::new(4);

        notifier.notify_one();
        notifier.notify_all();
        notifier.notify_n(0);
        notifier.notify_n(4);
        notifier.notify_n(9);

        assert_eq!(notifier.state.load(Ordering::Relaxed), 0);
    }

    /// Upstream aligns its waiter slot to two cache lines; losing that would
    /// turn every idle worker into false sharing on a neighbour's slot.
    #[test]
    fn a_waiter_slot_is_padded_to_two_cache_lines() {
        assert_eq!(align_of::<Waiter>(), 128);
        assert_eq!(size_of::<Waiter>() % 128, 0);
    }
}

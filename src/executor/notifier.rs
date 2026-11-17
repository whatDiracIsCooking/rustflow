//! Translated from `taskflow/core/nonblocking_notifier.hpp` and
//! `taskflow/core/atomic_notifier.hpp` (Taskflow v4.1.0).
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
//! waiter writes the notifier's state word and *then* reads the predicate; a
//! notifier writes the predicate and *then* reads the state word, with
//! sequentially consistent fences between. One of the two therefore sees the
//! other's write, so the case where the waiter sleeps on work that has already
//! arrived cannot happen.
//!
//! Upstream ships two notifiers behind that one protocol, and this module ports
//! both:
//!
//! - [`NonblockingNotifier`] is the Eigen-style event count: a lock-free stack
//!   of committed waiters, a count of waiters still in the pre-wait stage, and
//!   an epoch, all packed into one word. Upstream made it the default in v4.0
//!   "due to its robustness", and [`DefaultNotifier`] follows it there.
//! - [`AtomicNotifier`] is the smaller one, with no waiter stack: an epoch and
//!   a waiter count, and a notifier that scans the slots for someone to wake.
//!
//! # The one real departure from upstream
//!
//! Upstream parks a worker with C++20's `std::atomic::wait`/`notify_one` on the
//! waiter's own state word -- a futex wait. `std` has no such operation
//! (`std::sync::atomic` grew no `wait`), and this crate takes no dependency to
//! get one, so the per-waiter sleep is [`std::thread::park`] and the wake is
//! [`std::thread::Thread::unpark`]. That costs a registry: a waiter publishes
//! its own [`Thread`] handle before it makes itself claimable, and a notifier
//! reads that handle to wake it.
//!
//! The two types pay for the registry differently, and it is the one place
//! where the ports diverge from each other rather than from upstream:
//!
//! - [`NonblockingNotifier`] keeps the handle in the waiter slot itself and
//!   reads it with no lock at all, because its waiter stack already makes the
//!   access exclusive: a notifier may only touch a slot it has popped, and the
//!   slot's owner cannot leave the park loop until that notifier has published
//!   the wake. Its notify path is therefore lock-free, as upstream's is.
//! - [`AtomicNotifier`] has no stack to pop from -- a notifier scans every slot
//!   looking for an armed one -- so nothing makes that access exclusive and the
//!   handle sits behind a per-slot [`Mutex`]. That is the cost of the simpler
//!   state word, and the reason this module's default is the other type.

use crossbeam_utils::CachePadded;

use self::sync::{AtomicBool, AtomicU32, AtomicU64, Mutex, Ordering, Thread, fence, thread};

/// Either `std`'s or `loom`'s concurrency primitives, depending on whether this
/// is a `cfg(loom)` build.
///
/// `tests/loom_notifier.rs` models the protocol above, and loom can only see
/// operations that go through its own instrumented types. Nothing here is a
/// second implementation: the `loom` types mirror the `std` API one for one, so
/// the `cfg` only picks which set the module is compiled against. A normal
/// build -- the gate's build -- is the `std` one.
mod sync {
    #[cfg(not(loom))]
    pub(super) use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering, fence};
    #[cfg(not(loom))]
    pub(super) use std::sync::{Mutex, MutexGuard};
    #[cfg(not(loom))]
    pub(super) use std::thread::{self, Thread};

    #[cfg(loom)]
    pub(super) use loom::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering, fence};
    #[cfg(loom)]
    pub(super) use loom::sync::{Mutex, MutexGuard};
    #[cfg(loom)]
    pub(super) use loom::thread::{self, Thread};

    pub(super) use self::unsafe_cell::UnsafeCell;

    /// An [`UnsafeCell`](std::cell::UnsafeCell) whose accesses loom can see.
    ///
    /// loom's own cell replaces `get()` with closure-taking accessors, so that
    /// it can bracket an access and catch one that overlaps another. The `std`
    /// shim exposes the same two methods over a plain cell; both are `unsafe`
    /// to call, which is what keeps a `// SAFETY:` comment on every use.
    mod unsafe_cell {
        #[cfg(not(loom))]
        #[derive(Debug)]
        pub(in crate::executor::notifier) struct UnsafeCell<T>(core::cell::UnsafeCell<T>);

        #[cfg(not(loom))]
        impl<T> UnsafeCell<T> {
            pub(in crate::executor::notifier) fn new(data: T) -> Self {
                Self(core::cell::UnsafeCell::new(data))
            }

            /// # Safety
            ///
            /// No other access to the cell may overlap this one mutably.
            pub(in crate::executor::notifier) unsafe fn with<R>(
                &self,
                f: impl FnOnce(&T) -> R,
            ) -> R {
                // SAFETY: the caller guarantees no overlapping mutable access,
                // so this shared reference is the only live one.
                f(unsafe { &*self.0.get() })
            }

            /// # Safety
            ///
            /// No other access to the cell may overlap this one at all.
            pub(in crate::executor::notifier) unsafe fn with_mut<R>(
                &self,
                f: impl FnOnce(&mut T) -> R,
            ) -> R {
                // SAFETY: the caller guarantees no overlapping access, so this
                // exclusive reference is the only live one.
                f(unsafe { &mut *self.0.get() })
            }
        }

        #[cfg(loom)]
        #[derive(Debug)]
        pub(in crate::executor::notifier) struct UnsafeCell<T>(loom::cell::UnsafeCell<T>);

        #[cfg(loom)]
        impl<T> UnsafeCell<T> {
            pub(in crate::executor::notifier) fn new(data: T) -> Self {
                Self(loom::cell::UnsafeCell::new(data))
            }

            /// # Safety
            ///
            /// No other access to the cell may overlap this one mutably.
            pub(in crate::executor::notifier) unsafe fn with<R>(
                &self,
                f: impl FnOnce(&T) -> R,
            ) -> R {
                self.0.with(|data| {
                    // SAFETY: the caller guarantees no overlapping mutable
                    // access; loom checks that claim for every interleaving it
                    // explores.
                    f(unsafe { &*data })
                })
            }

            /// # Safety
            ///
            /// No other access to the cell may overlap this one at all.
            pub(in crate::executor::notifier) unsafe fn with_mut<R>(
                &self,
                f: impl FnOnce(&mut T) -> R,
            ) -> R {
                self.0.with_mut(|data| {
                    // SAFETY: the caller guarantees no overlapping access;
                    // loom checks that claim for every interleaving it
                    // explores.
                    f(unsafe { &mut *data })
                })
            }
        }
    }
}

/// The sleep/wake protocol a worker uses when stealing finds nothing.
///
/// The three waiting methods take the caller's worker id, which indexes the
/// per-waiter slot it owns: a worker may be in one wait at a time, and
/// `prepare_wait`, `cancel_wait` and `commit_wait` must all be called from the
/// thread that owns that id. The notifying methods may be called from any
/// thread, including one that is not a waiter at all.
///
/// The trait exists so that the two notifiers upstream ships slot in behind the
/// same surface; [`DefaultNotifier`] names whichever implementation the
/// executor builds on today.
pub trait Notifier {
    /// The number of waiter slots, i.e. the worker count this notifier was
    /// built for. Valid worker ids are `0..size()`.
    fn size(&self) -> usize;

    /// How many waiters this notifier is currently accounting for.
    ///
    /// The two implementations count different things, exactly as upstream's
    /// do. For [`NonblockingNotifier`] a waiter counts only once it is parked
    /// in [`Notifier::commit_wait`]; for [`AtomicNotifier`] it counts from
    /// [`Notifier::prepare_wait`], so a non-zero count does not mean anybody is
    /// actually asleep.
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
/// It is [`NonblockingNotifier`], which is what upstream made its own default
/// in v4.0 "due to its robustness": it wakes a *named* waiter popped off a
/// stack instead of scanning for one, and its notify path takes no lock. The
/// alias exists so that the choice is made in one place; [`AtomicNotifier`] is
/// still here, and still tested, because upstream keeps both.
pub type DefaultNotifier = NonblockingNotifier;

/// The Eigen-style event count: a lock-free stack of parked waiters, a count of
/// waiters still deciding, and an epoch, in one 64-bit word.
///
/// ```
/// use rustflow::executor::notifier::{NonblockingNotifier, Notifier};
///
/// let notifier = NonblockingNotifier::new(4);
/// assert_eq!(notifier.size(), 4);
/// assert_eq!(notifier.num_waiters(), 0);
///
/// // Notifying with nobody waiting is a no-op, not an error.
/// notifier.notify_all();
///
/// // Opening a wait and then finding work is the common case. A pre-waiter is
/// // not a waiter yet, so it never shows up in `num_waiters`.
/// notifier.prepare_wait(0);
/// assert_eq!(notifier.num_waiters(), 0);
/// notifier.cancel_wait(0);
/// assert_eq!(notifier.num_waiters(), 0);
/// ```
///
/// # The state word
///
/// ```text
/// [ 32-bit epoch | 16-bit pre-waiter count | 16-bit stack top ]
/// ```
///
/// The stack top is a waiter index, or [`Self::STACK_BITS`] ones -- the value
/// `STACK_MASK`, which is this port's spelling of upstream's null pointer -- for
/// an empty stack. That is why the word starts at `STACK_MASK` rather than at
/// zero.
///
/// # Tickets
///
/// [`Notifier::prepare_wait`] takes a ticket: the pre-waiter count it found,
/// which is also the number of pre-waiters ahead of it. The epoch advances once
/// per *resolved* request -- a commit, a cancel or a notification -- so the
/// distance between the epoch a waiter saw and the epoch now says whose turn it
/// is. A waiter whose turn has not come yields until the waiter ahead of it has
/// decided; one whose turn has passed has been notified and leaves at once.
/// Keeping that order is what lets `notify_n(k)` hand out exactly `k` wakes.
pub struct NonblockingNotifier {
    /// Epoch, pre-waiter count and stack top, in the layout above. One word, so
    /// that a notifier reads all three in a single atomic load and swings all
    /// three in a single compare-exchange.
    state: AtomicU64,
    /// One slot per worker id, each on its own cache line.
    waiters: Box<[CachePadded<NonblockingWaiter>]>,
}

/// One worker's slot in [`NonblockingNotifier`]: a stack link, the ticket its
/// owner is waiting out, and how to wake it.
///
/// Upstream calls this `Waiter` and links it with a `Waiter*`; here the link is
/// the index of the next slot, which keeps the stack a lock-free stack without
/// making it a lock-free stack *of pointers*.
struct NonblockingWaiter {
    /// The slot below this one on the waiter stack, or `STACK_MASK` for the
    /// bottom. Written by the owner before it pushes itself, and read by
    /// whichever notifier pops it.
    next: AtomicU64,
    /// The whole state word this slot's owner saw in `prepare_wait`, from which
    /// `commit_wait` and `cancel_wait` recover its epoch and ticket. Only the
    /// owning worker reads or writes it; it is atomic because the notifier is
    /// shared, not because two threads race on it.
    epoch: AtomicU64,
    /// [`NonblockingWaiter::NOT_SIGNALED`], `WAITING` or `SIGNALED`. Upstream's
    /// tri-state: a notifier that finds a waiter not yet parked can skip the
    /// wake entirely, because that waiter will see `SIGNALED` instead of
    /// parking.
    state: AtomicU32,
    /// The parked thread's handle, published before the slot is pushed onto the
    /// stack and read by the notifier that pops it.
    thread: ThreadSlot,
}

/// A slot's [`Thread`] handle, readable without a lock.
///
/// This is the whole of this module's `unsafe`, and the only reason it is sound
/// is an invariant of upstream's waiter stack, restated here because the
/// `unsafe` blocks below cite it:
///
/// > A slot is reachable only through the stack top in the state word. Its owner
/// > pushes it there and no one else does; a notifier removes it with a
/// > compare-exchange and therefore owns it alone; and the owner cannot leave
/// > [`NonblockingNotifier::park`] until that notifier has stored `SIGNALED`.
///
/// So the handle has exactly one writer -- the owner, before the push -- and
/// readers only inside the window where the owner is blocked. Reading it
/// *before* storing `SIGNALED` is what keeps the window closed, and
/// `tests/loom_notifier.rs` is what checks the claim.
struct ThreadSlot(sync::UnsafeCell<Option<Thread>>);

// SAFETY: the type's own invariant, above: the cell is written only by the
// slot's owner before the slot becomes reachable, and read only by the notifier
// that has made it unreachable again, so no two accesses to it overlap even
// though slots are shared between threads.
unsafe impl Sync for ThreadSlot {}

impl ThreadSlot {
    /// An empty slot.
    fn new() -> Self {
        Self(sync::UnsafeCell::new(None))
    }

    /// Publishes the calling thread as this slot's sleeper.
    ///
    /// # Safety
    ///
    /// The caller must own the slot and the slot must be unreachable: either it
    /// has not been pushed onto the waiter stack yet, or it has been popped and
    /// signalled.
    unsafe fn publish(&self) {
        // SAFETY: the caller guarantees the slot is unreachable, so no notifier
        // can be reading the handle while this writes it.
        unsafe { self.0.with_mut(|handle| *handle = Some(thread::current())) }
    }

    /// Clones out the handle of whoever is parked in this slot.
    ///
    /// # Safety
    ///
    /// The caller must have popped this slot off the waiter stack and must not
    /// yet have stored `SIGNALED` into the slot's state: before that store, the
    /// owner cannot have returned from `commit_wait` and so cannot be
    /// overwriting the handle.
    unsafe fn parked(&self) -> Option<Thread> {
        // SAFETY: the caller guarantees the slot is popped and unsignalled, so
        // the owner is inside its park loop and the handle is quiescent.
        unsafe { self.0.with(Clone::clone) }
    }
}

impl NonblockingWaiter {
    /// Pushed onto the stack, or not yet parked: a notifier that finds this
    /// state has nobody to wake.
    const NOT_SIGNALED: u32 = 0;
    /// Parked, and waiting to be told otherwise.
    const WAITING: u32 = 1;
    /// Told. The owner stops parking and leaves `commit_wait`.
    const SIGNALED: u32 = 2;

    /// A slot nobody is waiting in.
    fn new() -> Self {
        Self {
            next: AtomicU64::new(NonblockingNotifier::STACK_MASK),
            epoch: AtomicU64::new(0),
            state: AtomicU32::new(Self::NOT_SIGNALED),
            thread: ThreadSlot::new(),
        }
    }
}

impl NonblockingNotifier {
    /// How many bits of the state word hold the waiter stack's top index, and
    /// so the base-two logarithm of [`Self::capacity`].
    pub const STACK_BITS: u32 = 16;

    /// The stack top field, and -- being every bit of it set -- the port's
    /// spelling of upstream's null stack pointer.
    const STACK_MASK: u64 = (1 << Self::STACK_BITS) - 1;
    /// Where the pre-waiter count starts within the state word.
    const PREWAITER_SHIFT: u32 = 16;
    /// How many bits the pre-waiter count gets.
    const PREWAITER_BITS: u32 = 16;
    /// The bits of the state word that hold the pre-waiter count.
    const PREWAITER_MASK: u64 = ((1 << Self::PREWAITER_BITS) - 1) << Self::PREWAITER_SHIFT;
    /// One pre-waiter, as a delta to the state word.
    const PREWAITER_INC: u64 = 1 << Self::PREWAITER_SHIFT;
    /// Where the epoch starts within the state word.
    const EPOCH_SHIFT: u32 = 32;
    /// How many bits the epoch gets.
    const EPOCH_BITS: u32 = 32;
    /// The bits of the state word that hold the epoch.
    const EPOCH_MASK: u64 = ((1 << Self::EPOCH_BITS) - 1) << Self::EPOCH_SHIFT;
    /// One resolved request, as a delta to the state word.
    const EPOCH_INC: u64 = 1 << Self::EPOCH_SHIFT;

    /// Builds a notifier with `n` waiter slots, one per worker.
    ///
    /// # Panics
    ///
    /// If `n` does not leave room for a pre-waiter count, i.e. if it is not
    /// below `2^16 - 1`. Upstream throws on the same bound; a worker count that
    /// large is a programming error rather than a condition to recover from.
    #[must_use]
    pub fn new(n: usize) -> Self {
        assert!(
            n < (1 << Self::PREWAITER_BITS) - 1,
            "a NonblockingNotifier supports up to {} waiters, not {n}",
            (1u64 << Self::PREWAITER_BITS) - 1
        );
        Self {
            state: AtomicU64::new(Self::STACK_MASK),
            waiters: (0..n)
                .map(|_| CachePadded::new(NonblockingWaiter::new()))
                .collect(),
        }
    }

    /// The most waiter slots this notifier type can address, which is what the
    /// stack top field in the state word can hold.
    #[must_use]
    pub fn capacity(&self) -> usize {
        1 << Self::STACK_BITS
    }

    /// The slot at a stack index.
    fn waiter(&self, index: u64) -> &NonblockingWaiter {
        let index = usize::try_from(index).expect("a stack index is 16 bits wide");
        &self.waiters[index]
    }

    /// The epoch at which a waiter's own request comes up, recovered from the
    /// state word it saw in `prepare_wait`: its epoch plus its ticket.
    ///
    /// The arithmetic wraps, as upstream's unsigned arithmetic wraps. Both
    /// fields are allowed to roll over, and only the *difference* between two
    /// epochs is ever read -- see [`Self::ticket_delta`].
    fn ticket_epoch(seen: u64) -> u64 {
        let ticket = (seen & Self::PREWAITER_MASK) >> Self::PREWAITER_SHIFT;
        (seen & Self::EPOCH_MASK).wrapping_add(ticket << Self::EPOCH_SHIFT)
    }

    /// Where a waiter's turn stands: negative if the waiter ahead of it has not
    /// decided yet, zero if it is this waiter's turn, positive if its turn has
    /// passed, which means it has been notified.
    ///
    /// Upstream's `int64_t((state & EPOCH_MASK) - epoch)`. The signed reading of
    /// an unsigned difference is correct while the true difference stays inside
    /// the signed range, and here it cannot exceed the 2^16 outstanding
    /// pre-waiters.
    fn ticket_delta(state: u64, ticket_epoch: u64) -> i64 {
        (state & Self::EPOCH_MASK)
            .wrapping_sub(ticket_epoch)
            .cast_signed()
    }

    /// Whether the state word accounts for nobody at all: empty stack, no
    /// pre-waiters. The case every `notify_*` is cheap for.
    fn is_empty(state: u64) -> bool {
        state & Self::STACK_MASK == Self::STACK_MASK && state & Self::PREWAITER_MASK == 0
    }

    /// How many waiters the state word has in the pre-wait stage.
    fn prewaiters(state: u64) -> u64 {
        (state & Self::PREWAITER_MASK) >> Self::PREWAITER_SHIFT
    }

    /// Parks the calling thread in its own slot until a notifier signals it.
    ///
    /// Upstream waits on the slot's state word itself; here the slot's handle is
    /// already published, so the tri-state only has to decide *whether* to park:
    /// a waiter that has already been signalled never parks, and one that parks
    /// is one a notifier has promised to unpark.
    fn park(waiter: &NonblockingWaiter) {
        // The acquire on failure is what orders a notifier's read of the handle
        // -- which happens before its `SIGNALED` store -- ahead of anything this
        // thread does after leaving the loop, the next `publish` included.
        if waiter
            .state
            .compare_exchange(
                NonblockingWaiter::NOT_SIGNALED,
                NonblockingWaiter::WAITING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            while waiter.state.load(Ordering::Acquire) != NonblockingWaiter::SIGNALED {
                // A `park` may return spuriously, and an `unpark` that arrives
                // before the park leaves a token the park spends at once;
                // either way the state word, not the park, is the condition.
                thread::park();
            }
        }
    }

    /// Signals a chain of popped waiters, from `head` down, and unparks the ones
    /// that had made it as far as parking.
    ///
    /// Every slot reached from `head` has been removed from the state word by
    /// the caller's compare-exchange, which is what makes the handle read below
    /// sound and what keeps two notifiers from spending a wake on one waiter.
    fn unpark_chain(&self, head: u64) {
        let mut index = head;
        while index != Self::STACK_MASK {
            let waiter = self.waiter(index);
            // The link has to be read before the signal: once the owner sees
            // `SIGNALED` it may start another wait and overwrite it.
            let next = waiter.next.load(Ordering::Relaxed);
            // SAFETY: `waiter` is one of the slots the caller popped, so no
            // other notifier holds it, and nothing has signalled it yet -- so
            // its owner is still inside `park` and the handle is quiescent.
            let parked = unsafe { waiter.thread.parked() };
            if waiter
                .state
                .swap(NonblockingWaiter::SIGNALED, Ordering::AcqRel)
                == NonblockingWaiter::WAITING
            {
                parked
                    .expect("a waiter publishes its thread handle before it joins the stack")
                    .unpark();
            }
            index = next;
        }
    }
}

impl Notifier for NonblockingNotifier {
    fn size(&self) -> usize {
        self.waiters.len()
    }

    fn num_waiters(&self) -> usize {
        self.waiters
            .iter()
            .filter(|waiter| waiter.state.load(Ordering::Relaxed) == NonblockingWaiter::WAITING)
            .count()
    }

    fn prepare_wait(&self, worker_id: usize) {
        let seen = self.state.fetch_add(Self::PREWAITER_INC, Ordering::Relaxed);
        // The whole word, not just the epoch: the pre-waiter count in it is this
        // waiter's ticket, and `commit_wait` needs both halves.
        self.waiters[worker_id].epoch.store(seen, Ordering::Relaxed);
        // The waiting half of Dekker's algorithm: this fence orders the state
        // write above before the caller's second predicate check below.
        fence(Ordering::SeqCst);
    }

    fn cancel_wait(&self, worker_id: usize) {
        let ticket_epoch =
            Self::ticket_epoch(self.waiters[worker_id].epoch.load(Ordering::Relaxed));
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            let delta = Self::ticket_delta(state, ticket_epoch);
            if delta < 0 {
                // The pre-waiter ahead of this one has not decided its fate.
                // Wait for it to commit, cancel, or be notified.
                thread::yield_now();
                state = self.state.load(Ordering::Relaxed);
                continue;
            }
            if delta > 0 {
                // Already notified, which means a notifier has taken this
                // waiter out of the pre-waiter count on its behalf.
                return;
            }
            debug_assert_ne!(
                state & Self::PREWAITER_MASK,
                0,
                "cancelling without a ticket"
            );
            let resolved = state
                .wrapping_sub(Self::PREWAITER_INC)
                .wrapping_add(Self::EPOCH_INC);
            match self.state.compare_exchange_weak(
                state,
                resolved,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => state = observed,
            }
        }
    }

    fn commit_wait(&self, worker_id: usize) {
        let waiter = &self.waiters[worker_id];
        let index = u64::try_from(worker_id).expect("a worker id is bounded by the waiter count");

        waiter
            .state
            .store(NonblockingWaiter::NOT_SIGNALED, Ordering::Relaxed);
        // SAFETY: the slot is this thread's own and is not on the waiter stack:
        // the only way onto the stack is the compare-exchange below, and the
        // notifier that last popped the slot cannot still be holding it, since
        // this thread only returned from its previous `commit_wait` once that
        // notifier had stored `SIGNALED`.
        unsafe { waiter.thread.publish() };

        let ticket_epoch = Self::ticket_epoch(waiter.epoch.load(Ordering::Relaxed));
        let mut state = self.state.load(Ordering::SeqCst);
        loop {
            let delta = Self::ticket_delta(state, ticket_epoch);
            if delta < 0 {
                // The pre-waiter ahead of this one has not decided its fate.
                thread::yield_now();
                state = self.state.load(Ordering::SeqCst);
                continue;
            }
            if delta > 0 {
                // Already notified: nothing was pushed, so nothing is owed.
                return;
            }
            debug_assert_ne!(
                state & Self::PREWAITER_MASK,
                0,
                "committing without a ticket"
            );
            // Leave the pre-waiter count, resolve the ticket, and become the top
            // of the stack. Upstream distinguishes "the stack was empty" from
            // "it was not" to choose between a null link and a pointer; with the
            // link held as an index, `STACK_MASK` is both the empty stack and
            // the null link, so the two cases are one assignment.
            waiter
                .next
                .store(state & Self::STACK_MASK, Ordering::Relaxed);
            let pushed = (state
                .wrapping_sub(Self::PREWAITER_INC)
                .wrapping_add(Self::EPOCH_INC)
                & !Self::STACK_MASK)
                | index;
            match self.state.compare_exchange_weak(
                state,
                pushed,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => state = observed,
            }
        }
        Self::park(waiter);
    }

    fn notify_one(&self) {
        // The notifying half of Dekker's algorithm: the caller's predicate store
        // is ordered before the state load below.
        fence(Ordering::SeqCst);
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if Self::is_empty(state) {
                return;
            }
            let prewaiters = Self::prewaiters(state);
            let notified = if prewaiters == 0 {
                // Pop the top of the stack. No epoch bump: upstream's note is
                // that the ABA problem cannot arise here, because a slot is
                // re-pushed only after a pre-wait, which bumps the epoch anyway.
                let top = self.waiter(state & Self::STACK_MASK);
                (state & Self::EPOCH_MASK).wrapping_add(top.next.load(Ordering::Relaxed))
            } else {
                // Somebody is still in the pre-wait stage: resolving its ticket
                // is the whole notification, and it never parks.
                state
                    .wrapping_add(Self::EPOCH_INC)
                    .wrapping_sub(Self::PREWAITER_INC)
            };
            match self.state.compare_exchange_weak(
                state,
                notified,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if prewaiters == 0 {
                        let popped = state & Self::STACK_MASK;
                        // Cut the popped slot loose: the chain below it stays on
                        // the stack and is not this notification's to wake.
                        self.waiter(popped)
                            .next
                            .store(Self::STACK_MASK, Ordering::Relaxed);
                        self.unpark_chain(popped);
                    }
                    return;
                }
                Err(observed) => state = observed,
            }
        }
    }

    fn notify_all(&self) {
        fence(Ordering::SeqCst);
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if Self::is_empty(state) {
                return;
            }
            // Empty the stack and the pre-waiter count in one swing, resolving
            // every outstanding ticket.
            let notified = (state & Self::EPOCH_MASK)
                .wrapping_add(Self::EPOCH_INC.wrapping_mul(Self::prewaiters(state)))
                .wrapping_add(Self::STACK_MASK);
            match self.state.compare_exchange_weak(
                state,
                notified,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if state & Self::STACK_MASK != Self::STACK_MASK {
                        self.unpark_chain(state & Self::STACK_MASK);
                    }
                    return;
                }
                Err(observed) => state = observed,
            }
        }
    }

    fn notify_n(&self, n: usize) {
        if n == 0 {
            return;
        }
        if n >= self.waiters.len() {
            self.notify_all();
            return;
        }
        // `n < waiters.len()`, and `new` bounded that by the pre-waiter field.
        let mut owed = u64::try_from(n).expect("n is bounded by the waiter count");

        fence(Ordering::SeqCst);
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if Self::is_empty(state) {
                return;
            }
            let prewaiters = Self::prewaiters(state);
            let (notified, remaining) = if prewaiters == 0 {
                let top = self.waiter(state & Self::STACK_MASK);
                (
                    (state & Self::EPOCH_MASK).wrapping_add(top.next.load(Ordering::Relaxed)),
                    owed - 1,
                )
            } else {
                // Pre-waiters first, as many at once as are owed: each costs one
                // epoch bump and one pre-waiter, and none of them parks.
                let unblocked = owed.min(prewaiters);
                (
                    state
                        .wrapping_add(Self::EPOCH_INC.wrapping_mul(unblocked))
                        .wrapping_sub(Self::PREWAITER_INC.wrapping_mul(unblocked)),
                    owed - unblocked,
                )
            };
            match self.state.compare_exchange_weak(
                state,
                notified,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    owed = remaining;
                    if prewaiters == 0 {
                        let popped = state & Self::STACK_MASK;
                        self.waiter(popped)
                            .next
                            .store(Self::STACK_MASK, Ordering::Relaxed);
                        self.unpark_chain(popped);
                    }
                    if owed == 0 {
                        return;
                    }
                    // C++ leaves `expected` untouched on a successful exchange,
                    // so upstream spends the next iteration on a doomed
                    // compare-exchange to reload it. The value is ours here, and
                    // the next exchange validates it either way.
                    state = notified;
                }
                Err(observed) => state = observed,
            }
        }
    }
}

impl Drop for NonblockingNotifier {
    fn drop(&mut self) {
        // Upstream asserts the same thing in its destructor: dropping a notifier
        // somebody is still waiting on would leave that thread parked forever
        // with nothing left to wake it.
        debug_assert_eq!(
            self.state.load(Ordering::Relaxed) & (Self::STACK_MASK | Self::PREWAITER_MASK),
            Self::STACK_MASK,
            "a NonblockingNotifier was dropped with waiters still registered"
        );
    }
}

impl core::fmt::Debug for NonblockingNotifier {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let state = self.state.load(Ordering::Relaxed);
        let top = state & Self::STACK_MASK;
        f.debug_struct("NonblockingNotifier")
            .field("size", &self.waiters.len())
            .field("epoch", &(state >> Self::EPOCH_SHIFT))
            .field("prewaiters", &Self::prewaiters(state))
            .field(
                "stack_top",
                &if top == Self::STACK_MASK {
                    None
                } else {
                    Some(top)
                },
            )
            .finish()
    }
}

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
///
/// # Two consequences of the park registry
///
/// Both are tolerated by the epoch loop in [`Notifier::commit_wait`]:
///
/// - A wake is *best effort*, exactly as upstream's futex one is: a notifier
///   that finds no armed waiter wakes nobody, and that is correct, because a
///   waiter not yet armed has not yet read the epoch and so will read the new
///   one.
/// - A claimed waiter that leaves without parking leaves an unconsumed unpark
///   token behind, which makes its next `park` return at once. That is a
///   spurious wakeup, which the loop handles by re-checking the epoch.
///
/// The wake is still targeted rather than a broadcast: [`Notifier::notify_one`]
/// claims one armed waiter and no more. What keeps `notify_n(k)` from spending
/// two of its `k` wakes on the same waiter is the order of the two checks after
/// `park` returns -- the epoch is re-read *before* the waiter re-arms, so a
/// waiter whose epoch has moved on is never armed again and cannot be claimed a
/// second time.
pub struct AtomicNotifier {
    /// The notification epoch in the high 32 bits, the waiter count in the low
    /// 32. One word, so that a notifier reads both in a single atomic load.
    state: AtomicU64,
    /// One slot per worker id, each on its own cache line.
    waiters: Box<[CachePadded<AtomicWaiter>]>,
}

/// One worker's slot in [`AtomicNotifier`]: the epoch it is waiting out, and
/// how to wake it.
///
/// Upstream calls this `Waiter`; both notifiers have one, so they are named
/// after their notifier here.
struct AtomicWaiter {
    /// The epoch [`Notifier::prepare_wait`] saw, kept in the shifted-down form
    /// so the comparison in `commit_wait` needs no cast. Only the owning worker
    /// reads or writes it; it is atomic because the notifier is shared, not
    /// because two threads race on it.
    epoch: AtomicU64,
    /// Whether this waiter is parked, or close enough to parked that a notifier
    /// must wake it. Claimed with a swap, so exactly one notifier wakes it.
    armed: AtomicBool,
    /// The parked thread's handle. Behind a lock because a notifier reads it
    /// while the waiter may be replacing it, and `Thread` is not an atomic.
    /// [`NonblockingNotifier`] needs no such lock, which is the better half of
    /// why it is the default.
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
            waiters: (0..n)
                .map(|_| CachePadded::new(AtomicWaiter::new()))
                .collect(),
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

impl AtomicWaiter {
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
    fn lock(&self) -> sync::MutexGuard<'_, Option<Thread>> {
        self.thread
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::{
        AtomicNotifier, AtomicWaiter, EPOCH_SHIFT, NonblockingNotifier, NonblockingWaiter,
        Notifier, WAITER_MASK,
    };
    use crossbeam_utils::CachePadded;
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

    /// Upstream aligns its waiter slot to a cache line; losing that would turn
    /// every idle worker into false sharing on a neighbour's slot. The padding
    /// is `crossbeam_utils::CachePadded`, which is two lines wide on the
    /// architectures where a prefetcher pulls the neighbour in as well.
    #[test]
    fn a_waiter_slot_is_padded_to_two_cache_lines() {
        assert_eq!(align_of::<CachePadded<AtomicWaiter>>(), 128);
        assert_eq!(size_of::<CachePadded<AtomicWaiter>>() % 128, 0);
        assert_eq!(align_of::<CachePadded<NonblockingWaiter>>(), 128);
        assert_eq!(size_of::<CachePadded<NonblockingWaiter>>() % 128, 0);
    }

    /// The pre-waiter count and the stack top share a word with the epoch, and
    /// the empty stack is all ones rather than zero -- an index of 0 is a real
    /// waiter. No test outside this module can see any of that.
    #[test]
    fn the_state_word_splits_into_an_epoch_a_count_and_a_stack() {
        let notifier = NonblockingNotifier::new(2);
        let state = |notifier: &NonblockingNotifier| notifier.state.load(Ordering::Relaxed);

        assert_eq!(
            state(&notifier),
            NonblockingNotifier::STACK_MASK,
            "a fresh notifier is an empty stack, not a stack holding waiter 0"
        );

        notifier.prepare_wait(0);
        assert_eq!(NonblockingNotifier::prewaiters(state(&notifier)), 1);
        assert_eq!(state(&notifier) >> EPOCH_SHIFT, 0, "nothing resolved yet");

        notifier.prepare_wait(1);
        assert_eq!(NonblockingNotifier::prewaiters(state(&notifier)), 2);

        // A cancel resolves one ticket: the count goes down, the epoch goes up.
        notifier.cancel_wait(0);
        assert_eq!(NonblockingNotifier::prewaiters(state(&notifier)), 1);
        assert_eq!(state(&notifier) >> EPOCH_SHIFT, 1);

        notifier.cancel_wait(1);
        assert_eq!(state(&notifier) & NonblockingNotifier::PREWAITER_MASK, 0);
        assert_eq!(state(&notifier) >> EPOCH_SHIFT, 2);
        assert_eq!(
            state(&notifier) & NonblockingNotifier::STACK_MASK,
            NonblockingNotifier::STACK_MASK
        );
    }

    /// A ticket's turn comes when the epoch has resolved every ticket ahead of
    /// it, and the comparison has to survive the epoch field wrapping.
    #[test]
    fn a_ticket_comes_up_when_the_epochs_ahead_of_it_are_spent() {
        let seen = |epoch: u64, ticket: u64| {
            (epoch << EPOCH_SHIFT) | (ticket << NonblockingNotifier::PREWAITER_SHIFT)
        };
        // The delta is measured in whole epochs, the unit the field is shifted
        // by, which is what lets the two fields share a word.
        let epochs = |count: i64| count * (1i64 << EPOCH_SHIFT);

        // Second in line at epoch 7: its turn is epoch 9.
        let ticket = NonblockingNotifier::ticket_epoch(seen(7, 2));
        assert_eq!(
            NonblockingNotifier::ticket_delta(seen(8, 0), ticket),
            epochs(-1),
            "the waiter ahead has not decided"
        );
        assert_eq!(
            NonblockingNotifier::ticket_delta(seen(9, 0), ticket),
            epochs(0),
            "its turn"
        );
        assert_eq!(
            NonblockingNotifier::ticket_delta(seen(10, 0), ticket),
            epochs(1),
            "its turn has passed, so it was notified"
        );

        // The same, one epoch short of the 32-bit field wrapping.
        let last = (1u64 << 32) - 1;
        let ticket = NonblockingNotifier::ticket_epoch(seen(last, 2));
        assert_eq!(
            NonblockingNotifier::ticket_delta(seen(0, 0), ticket),
            epochs(-1)
        );
        assert_eq!(
            NonblockingNotifier::ticket_delta(seen(1, 0), ticket),
            epochs(0)
        );
        assert_eq!(
            NonblockingNotifier::ticket_delta(seen(2, 0), ticket),
            epochs(1)
        );
    }

    /// `notify_n(0)` and a notification nobody is waiting for must both leave
    /// the word exactly as it was, or a later waiter sees a wake it missed.
    #[test]
    fn notifying_an_empty_nonblocking_notifier_does_not_move_the_epoch() {
        let notifier = NonblockingNotifier::new(4);

        notifier.notify_one();
        notifier.notify_all();
        notifier.notify_n(0);
        notifier.notify_n(4);
        notifier.notify_n(9);

        assert_eq!(
            notifier.state.load(Ordering::Relaxed),
            NonblockingNotifier::STACK_MASK
        );
    }

    /// A parked waiter is one that reached `WAITING`, and `num_waiters` counts
    /// those and nothing else.
    #[test]
    fn a_pre_waiter_is_not_a_waiter() {
        let notifier = NonblockingNotifier::new(2);

        notifier.prepare_wait(0);
        assert_eq!(notifier.num_waiters(), 0, "a pre-waiter is still deciding");
        assert_eq!(
            notifier.waiters[0].state.load(Ordering::Relaxed),
            NonblockingWaiter::NOT_SIGNALED
        );
        notifier.cancel_wait(0);
        assert_eq!(notifier.num_waiters(), 0);
    }

    /// The capacity is the stack field's width, not the slot count: it is what
    /// bounds a notifier's `size`, and upstream reports the same number.
    #[test]
    fn the_capacity_is_the_width_of_the_stack_field() {
        assert_eq!(NonblockingNotifier::new(8).capacity(), 1 << 16);
        assert_eq!(
            NonblockingNotifier::new(8).capacity(),
            1 << NonblockingNotifier::STACK_BITS
        );
    }

    /// The alias is the executor's one choice of notifier, and it is upstream's.
    #[test]
    fn the_default_notifier_is_the_nonblocking_one() {
        let notifier = super::DefaultNotifier::new(1);
        assert_eq!(notifier.size(), 1);
        assert_eq!(
            format!("{notifier:?}")
                .split_once(' ')
                .map(|(head, _)| head),
            Some("NonblockingNotifier")
        );
    }
}

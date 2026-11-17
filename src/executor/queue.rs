//! Work-stealing queues: one LIFO deque per worker, plus a handful of shared
//! overflow buffers that non-worker threads push into.
//!
//! This is deliberately *not* a translation of `taskflow/core/wsq.hpp`
//! (Taskflow v4.1.0). Upstream hand-writes the Chase-Lev deque twice, once
//! bounded and once unbounded; `crossbeam-deque` is the same algorithm from the
//! same paper, better tested than our own port of it would be, so the queues
//! themselves are a dependency rather than a port.
//!
//! What *is* carried over from upstream is the shape of the layer:
//!
//! - the steal protocol callers see -- an item, or "empty", and nothing else;
//! - `bit_width(num_workers)` shared buffers, upstream's `Executor::_buffers`,
//!   whose comment records that "empirically, we find that log2(N) performs
//!   best";
//! - picking one of them by hashing the item, as `Executor::_spill` does.
//!
//! The one behavioural difference worth stating up front: upstream's per-worker
//! queue is a `BoundedWSQ` (256 items by default) whose `try_push` can fail, so
//! a *worker* that overflows its own queue spills into the shared buffers. A
//! crossbeam deque is unbounded, so [`WorkerQueue::push`] cannot fail and that
//! spill path disappears -- [`Buffers`] exists here only for callers that have
//! no queue of their own, i.e. threads outside the executor.

use std::hash::{DefaultHasher, Hash, Hasher};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_utils::CachePadded;

/// A worker's own queue of pending items.
///
/// One thread -- the worker that owns it -- pushes and pops at the back, in
/// last-in-first-out order, so the item pushed most recently (the one most
/// likely to still be in cache) is the next one run. Any number of other
/// threads steal from the front through a [`Thief`], in first-in-first-out
/// order, which is both where the contention is lowest and where the oldest
/// and therefore coarsest work tends to sit.
///
/// The type is `!Sync` by construction: [`push`](Self::push) and
/// [`pop`](Self::pop) take `&self` but the handle cannot be shared across
/// threads, which is how "only the owner may push or pop" is enforced here.
/// Upstream states that rule in a doc comment and trusts the caller.
#[derive(Debug)]
pub struct WorkerQueue<T> {
    deque: Worker<T>,
}

impl<T: Send> WorkerQueue<T> {
    /// Creates an empty queue.
    #[must_use]
    pub fn new() -> Self {
        Self {
            deque: Worker::new_lifo(),
        }
    }

    /// Pushes an item onto the back of the queue.
    ///
    /// This cannot fail. Upstream's bounded queue returns `false` once it is
    /// full and the caller spills into the shared [`Buffers`]; the deque behind
    /// this queue grows instead, so a worker never has to spill.
    pub fn push(&self, item: T) {
        self.deque.push(item);
    }

    /// Pops the most recently pushed item, or returns `None` if the queue is
    /// empty.
    ///
    /// `None` also covers the case where the last remaining item was stolen out
    /// from under this call; the item is not lost, the thief has it.
    pub fn pop(&self) -> Option<T> {
        self.deque.pop()
    }

    /// Returns `true` if the queue held no items at the moment of the call.
    ///
    /// Worth only what upstream's `empty()` is worth: by the time the answer is
    /// read, a thief may have emptied the queue or the owner may have filled
    /// it.
    pub fn is_empty(&self) -> bool {
        self.deque.is_empty()
    }

    /// Creates a handle other threads can steal from.
    ///
    /// Each call returns an independent handle onto the same queue; they are
    /// also [`Clone`], so the executor can hand one to every worker.
    #[must_use]
    pub fn thief(&self) -> Thief<T> {
        Thief {
            stealer: self.deque.stealer(),
        }
    }
}

impl<T: Send> Default for WorkerQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// The thief side of a [`WorkerQueue`]: steals from the front, from any thread.
#[derive(Debug)]
pub struct Thief<T> {
    stealer: Stealer<T>,
}

impl<T: Send> Thief<T> {
    /// Steals the oldest item from the queue, or returns `None` if there was
    /// nothing to take.
    ///
    /// `None` means the queue looked empty, not that the attempt lost a race:
    /// a contended attempt is retried inside this call, so callers see
    /// upstream's two-outcome protocol -- an item, or empty -- and need no
    /// third "try again" case.
    pub fn steal(&self) -> Option<T> {
        settle(|| self.stealer.steal())
    }

    /// Returns `true` if the queue held no items at the moment of the call.
    pub fn is_empty(&self) -> bool {
        self.stealer.is_empty()
    }
}

impl<T> Clone for Thief<T> {
    fn clone(&self) -> Self {
        Self {
            stealer: self.stealer.clone(),
        }
    }
}

/// The shared overflow buffers: where threads with no queue of their own put
/// work for the executor to pick up.
///
/// There are `bit_width(num_workers)` of them -- 3 buffers for 4 workers, 6 for
/// 32 -- which is upstream's `Executor::_buffers`, sized by its own empirical
/// result that "log2(N) performs best". Fewer buffers than workers is the
/// point: this is the cold path, and one queue per worker would cost every
/// thief a sweep over mostly-empty queues.
///
/// An item is routed to a buffer by its hash, so that two threads scheduling
/// two different items usually contend on two different buffers. Upstream
/// hashes the node pointer it is about to push; there is no pointer to hash
/// here, so the item's own [`Hash`] stands in, reduced to a buffer the same way
/// `Executor::_spill` reduces its pointer.
#[derive(Debug)]
pub struct Buffers<T> {
    // Upstream gives each queue `alignas(TF_CACHELINE_SIZE)`. crossbeam already
    // pads an `Injector`'s head and tail internally, so this is belt and
    // braces, but it keeps the no-false-sharing requirement visible at the
    // layer that depends on it rather than inside a dependency.
    buffers: Vec<CachePadded<Injector<T>>>,
}

impl<T: Send> Buffers<T> {
    /// Creates the buffers for an executor of `num_workers` workers.
    ///
    /// # Panics
    ///
    /// Panics if `num_workers` is zero: `bit_width(0)` is zero, which would
    /// leave nowhere to push. Upstream throws for the same reason -- "executor
    /// must define at least one worker".
    #[must_use]
    pub fn new(num_workers: usize) -> Self {
        assert!(num_workers > 0, "an executor needs at least one worker");
        Self {
            buffers: (0..bit_width(num_workers))
                .map(|_| CachePadded::new(Injector::new()))
                .collect(),
        }
    }

    /// Returns the number of buffers.
    ///
    /// The executor needs this to address them: upstream numbers its steal
    /// victims `0..num_workers + num_buffers` and treats the tail of that range
    /// as buffer indices.
    #[must_use]
    pub fn num_buffers(&self) -> usize {
        self.buffers.len()
    }

    /// Returns the index of the first buffer that was not empty, or `None` if
    /// they all were.
    ///
    /// The buffers are not read atomically with respect to each other, so this
    /// is a hint -- the same hint upstream checks before a worker commits to
    /// sleeping. The *index* is the part that matters to the caller: a worker
    /// that finds work here points its sticky victim at the buffer holding it
    /// rather than at the first buffer, which is what upstream does when it
    /// walks the buffers one at a time.
    #[must_use]
    pub fn first_nonempty(&self) -> Option<usize> {
        self.buffers.iter().position(|buffer| !buffer.is_empty())
    }

    /// Returns `true` if every buffer was empty at the moment of the call.
    ///
    /// As much of a hint as [`first_nonempty`](Self::first_nonempty), which is
    /// what it asks.
    pub fn is_empty(&self) -> bool {
        self.first_nonempty().is_none()
    }

    /// Returns the index of the buffer [`push`](Self::push) routes `item` to.
    ///
    /// Stable for a given item and a given buffer count, which is what makes
    /// the routing testable; it is not a promise to callers, who should treat
    /// which buffer an item lands in as an implementation detail.
    #[must_use]
    pub fn buffer_of(&self, item: &T) -> usize
    where
        T: Hash,
    {
        // A fixed-seed hasher, not `RandomState`: the executor does not need an
        // unpredictable mapping, and a reproducible one is easier to reason
        // about when a scheduling bug turns out to be a routing bug.
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        reduce(hasher.finish(), self.buffers.len())
    }

    /// Pushes an item into one of the buffers.
    ///
    /// Callable from any thread, including several at once.
    pub fn push(&self, item: T)
    where
        T: Hash,
    {
        self.buffers[self.buffer_of(&item)].push(item);
    }

    /// Steals an item from the buffer at `index`, or returns `None` if there
    /// was nothing to take.
    ///
    /// Like [`Thief::steal`], a contended attempt is retried internally, so
    /// `None` means the buffer looked empty.
    ///
    /// # Panics
    ///
    /// Panics if `index` is not below [`num_buffers`](Self::num_buffers).
    pub fn steal_from(&self, index: usize) -> Option<T> {
        let buffer = &self.buffers[index];
        settle(|| buffer.steal())
    }
}

/// Runs a steal attempt to a decision: an item, or empty.
///
/// crossbeam reports a lost race as a third outcome, [`Steal::Retry`], which
/// upstream's queues do not have -- their `steal` reports a lost compare-and-
/// swap as "empty" and leaves the caller to pick another victim. Treating a
/// race as emptiness here would be worse than upstream, not equal to it:
/// an `Injector` also answers `Retry` while a *concurrent* steal is in flight,
/// and a worker that read that as "empty" could park with work sitting in a
/// buffer. So the race is settled here instead.
fn settle<T>(mut attempt: impl FnMut() -> Steal<T>) -> Option<T> {
    loop {
        match attempt() {
            Steal::Success(item) => return Some(item),
            Steal::Empty => return None,
            Steal::Retry => std::hint::spin_loop(),
        }
    }
}

/// The number of bits needed to represent `n`, i.e. C++20 `std::bit_width`.
const fn bit_width(n: usize) -> u32 {
    usize::BITS - n.leading_zeros()
}

/// Reduces a hash to an index below `len`.
///
/// Upstream shifts its node pointer right by 16 before taking the remainder,
/// because the low bits of an allocation are nearly constant; the shift is kept
/// so the two implementations can be compared line by line, and it costs
/// nothing on a hash whose bits are already well mixed.
fn reduce(hash: u64, len: usize) -> usize {
    // `len` is a bit width, so it is at most 64 and both conversions are exact.
    // They are spelled out rather than cast with `as` because this crate treats
    // a silent wrap in the scheduler as a bug, not a convenience.
    let len = u64::try_from(len).expect("a bit width always fits in a u64");
    usize::try_from((hash >> 16) % len).expect("a value below 64 always fits in a usize")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;

    /// How long a concurrency test keeps trying before it gives up and lets the
    /// assertion report what went missing. Generous: it is only ever reached
    /// when the test is about to fail.
    const GIVE_UP_AFTER: Duration = Duration::from_secs(30);

    #[test]
    fn the_sharing_model_holds_at_compile_time() {
        fn send<T: Send>() {}
        fn send_and_sync<T: Send + Sync>() {}

        // A worker queue moves to its worker and is never shared; the thief
        // handle and the buffers are shared by every worker at once. The
        // executor is built on exactly this split.
        send::<WorkerQueue<usize>>();
        send_and_sync::<Thief<usize>>();
        send_and_sync::<Buffers<usize>>();
    }

    #[test]
    fn the_owner_takes_the_newest_item_first() {
        let queue = WorkerQueue::new();
        assert!(queue.is_empty());

        queue.push(1);
        queue.push(2);
        queue.push(3);
        assert!(!queue.is_empty());

        assert_eq!(queue.pop(), Some(3));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), None);
        assert!(queue.is_empty());
    }

    #[test]
    fn a_thief_takes_the_oldest_item_first() {
        let queue = WorkerQueue::new();
        let thief = queue.thief();
        assert_eq!(thief.steal(), None);

        queue.push(1);
        queue.push(2);
        queue.push(3);

        assert_eq!(thief.steal(), Some(1));
        assert_eq!(thief.steal(), Some(2));
        assert_eq!(thief.clone().steal(), Some(3));
        assert_eq!(thief.steal(), None);
        assert!(thief.is_empty());
    }

    #[test]
    fn pushing_past_upstreams_bounded_capacity_still_succeeds() {
        // Upstream's per-worker queue holds 256 items and then fails the push.
        let queue = WorkerQueue::new();
        for item in 0..4096 {
            queue.push(item);
        }

        let mut popped = 0;
        while queue.pop().is_some() {
            popped += 1;
        }
        assert_eq!(popped, 4096);
    }

    #[test]
    fn every_item_reaches_exactly_one_consumer() {
        const ITEMS: usize = 10_000;

        for thieves in 2..=8 {
            let queue = WorkerQueue::new();
            let taken = AtomicUsize::new(0);
            let give_up = Instant::now() + GIVE_UP_AFTER;

            let mut collected = thread::scope(|scope| {
                let handles: Vec<_> = (0..thieves)
                    .map(|_| {
                        let thief = queue.thief();
                        let taken = &taken;
                        scope.spawn(move || {
                            let mut mine = Vec::new();
                            while taken.load(Ordering::Relaxed) < ITEMS && Instant::now() < give_up
                            {
                                if let Some(item) = thief.steal() {
                                    mine.push(item);
                                    taken.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    thread::yield_now();
                                }
                            }
                            mine
                        })
                    })
                    .collect();

                // The owner competes for its own items from the other end of the
                // deque, which is where `pop` racing `steal` for the last item
                // gets exercised.
                let mut mine = Vec::new();
                for item in 0..ITEMS {
                    queue.push(item);
                    if item % 4 == 0
                        && let Some(item) = queue.pop()
                    {
                        mine.push(item);
                        taken.fetch_add(1, Ordering::Relaxed);
                    }
                }
                while taken.load(Ordering::Relaxed) < ITEMS && Instant::now() < give_up {
                    if let Some(item) = queue.pop() {
                        mine.push(item);
                        taken.fetch_add(1, Ordering::Relaxed);
                    } else {
                        thread::yield_now();
                    }
                }

                handles.into_iter().fold(mine, |mut all, handle| {
                    all.extend(handle.join().expect("a thief panicked"));
                    all
                })
            });

            // Equality against the whole range is the no-duplicates and
            // no-losses check in one: a duplicate displaces a missing item.
            collected.sort_unstable();
            assert_eq!(
                collected,
                (0..ITEMS).collect::<Vec<_>>(),
                "with {thieves} thieves"
            );
        }
    }

    #[test]
    fn buffer_count_is_the_bit_width_of_the_worker_count() {
        for (workers, expected) in [
            (1, 1),
            (2, 2),
            (3, 2),
            (4, 3),
            (7, 3),
            (8, 4),
            (16, 5),
            (1024, 11),
        ] {
            assert_eq!(
                Buffers::<usize>::new(workers).num_buffers(),
                expected,
                "{workers} workers"
            );
        }
    }

    #[test]
    #[should_panic(expected = "an executor needs at least one worker")]
    fn there_are_no_buffers_without_a_worker() {
        let _ = Buffers::<usize>::new(0);
    }

    #[test]
    fn an_item_always_routes_to_the_same_buffer() {
        let buffers = Buffers::new(32);

        for item in 0..256_usize {
            let index = buffers.buffer_of(&item);
            assert_eq!(index, buffers.buffer_of(&item), "item {item} moved");
            assert!(index < buffers.num_buffers());

            buffers.push(item);
            assert_eq!(
                buffers.steal_from(index),
                Some(item),
                "item {item} landed in another buffer"
            );
        }

        assert!(buffers.is_empty());
    }

    #[test]
    fn the_first_nonempty_buffer_is_the_one_holding_work() {
        let buffers = Buffers::new(32);
        assert_eq!(buffers.first_nonempty(), None);
        assert!(buffers.is_empty());

        // Find an item that routes to a buffer other than the first, so that
        // the answer distinguishes "some buffer" from "buffer zero".
        let item = (0..1024_usize)
            .find(|item| buffers.buffer_of(item) > 0)
            .expect("the routing reaches every buffer");
        let index = buffers.buffer_of(&item);

        buffers.push(item);
        assert_eq!(buffers.first_nonempty(), Some(index));
        assert!(!buffers.is_empty());

        assert_eq!(buffers.steal_from(index), Some(item));
        assert_eq!(buffers.first_nonempty(), None);
    }

    #[test]
    fn routing_uses_every_buffer() {
        let buffers = Buffers::new(16);
        let mut hits = vec![0_usize; buffers.num_buffers()];

        for item in 0..1000_usize {
            hits[buffers.buffer_of(&item)] += 1;
        }

        assert!(
            hits.iter().all(|&count| count > 0),
            "unused buffer: {hits:?}"
        );
    }

    #[test]
    fn every_external_push_is_stolen_exactly_once() {
        const PUSHERS: usize = 4;
        const PER_PUSHER: usize = 2_000;
        const ITEMS: usize = PUSHERS * PER_PUSHER;

        let buffers = Buffers::new(8);
        let taken = AtomicUsize::new(0);
        let give_up = Instant::now() + GIVE_UP_AFTER;

        let mut collected = thread::scope(|scope| {
            for pusher in 0..PUSHERS {
                let buffers = &buffers;
                scope.spawn(move || {
                    for item in 0..PER_PUSHER {
                        buffers.push(pusher * PER_PUSHER + item);
                    }
                });
            }

            let thieves: Vec<_> = (0..4)
                .map(|_| {
                    let buffers = &buffers;
                    let taken = &taken;
                    scope.spawn(move || {
                        let mut mine = Vec::new();
                        while taken.load(Ordering::Relaxed) < ITEMS && Instant::now() < give_up {
                            let mut idle = true;
                            for index in 0..buffers.num_buffers() {
                                if let Some(item) = buffers.steal_from(index) {
                                    mine.push(item);
                                    taken.fetch_add(1, Ordering::Relaxed);
                                    idle = false;
                                }
                            }
                            if idle {
                                thread::yield_now();
                            }
                        }
                        mine
                    })
                })
                .collect();

            thieves.into_iter().fold(Vec::new(), |mut all, thief| {
                all.extend(thief.join().expect("a thief panicked"));
                all
            })
        });

        collected.sort_unstable();
        assert_eq!(collected, (0..ITEMS).collect::<Vec<_>>());
    }
}

//! Work-stealing queues: one LIFO deque per worker, plus a handful of shared
//! overflow buffers that non-worker threads push into.
//!
//! # Design
//!
//! Deliberately *not* a translation of `taskflow/core/wsq.hpp` (Taskflow
//! v4.1.0). Upstream hand-writes the Chase-Lev deque twice, bounded and
//! unbounded; `crossbeam-deque` is the same algorithm from the same paper and
//! better tested than our own port of it would be, so the queues themselves
//! are a dependency rather than a port. What is carried over is the shape of
//! the layer: the two-outcome steal protocol, the buffer count and the routing
//! into them, each named below where it is implemented.
//!
//! One behavioural difference follows from the dependency. Upstream's
//! per-worker queue is a `BoundedWSQ` whose `try_push` can fail, so a *worker*
//! that overflows spills into the shared buffers; a crossbeam deque is
//! unbounded, so [`WorkerQueue::push`] cannot fail and that path disappears.
//! [`Buffers`] is then for callers with no owning end of a queue to reach, of
//! which there are three -- a thread outside the executor, a corun loop, and a
//! task body that cannot name the `WorkerThread` frame below it -- and the
//! `!Sync` of the owning end below is what makes those one fact rather than
//! three decisions.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_utils::CachePadded;

/// A worker's own queue of pending items.
///
/// One thread -- the worker that owns it -- pushes and pops at the back, in
/// last-in-first-out order, so the item pushed most recently is the next one
/// run; any number of other threads steal from the front through a [`Thief`],
/// in first-in-first-out order, where contention is lowest and the coarsest
/// work tends to sit. `!Sync` by construction, so that "only the owner may
/// push or pop" is enforced here rather than, as upstream has it, stated in a
/// doc comment and left to the caller.
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

    /// Pushes an item onto the back of the queue. Cannot fail, where
    /// upstream's bounded queue returns `false` when full and spills.
    pub fn push(&self, item: T) {
        self.deque.push(item);
    }

    /// Pops the most recently pushed item, or `None` if the queue is empty --
    /// which covers the last item being stolen out from under this call, in
    /// which case it is not lost, the thief has it.
    pub fn pop(&self) -> Option<T> {
        self.deque.pop()
    }

    /// Whether the queue held no items at the moment of the call, worth what
    /// upstream's `empty()` is: by the time the answer is read, a thief may
    /// have emptied the queue or the owner filled it.
    pub fn is_empty(&self) -> bool {
        self.deque.is_empty()
    }

    /// Creates a handle other threads can steal from: independent per call,
    /// and [`Clone`], so the executor can hand one to every worker.
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
    /// Steals the oldest item from the queue, or `None` if there was nothing
    /// to take -- meaning the queue looked empty, not that a race was lost: a
    /// contended attempt is retried inside this call, so callers see
    /// upstream's two-outcome protocol and need no "try again" case.
    pub fn steal(&self) -> Option<T> {
        settle(|| self.stealer.steal())
    }

    /// Whether the queue held no items at the moment of the call.
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
/// There are `bit_width(num_workers)` of them -- 3 for 4 workers, 6 for 32 --
/// which is upstream's `Executor::_buffers`, sized by its own comment that
/// "empirically, we find that log2(N) performs best". Fewer buffers than
/// workers is the point: this is the cold path, and one queue per worker would
/// cost every thief a sweep over mostly-empty queues. A single item is routed
/// by its hash, so that two threads scheduling two items usually contend on
/// two different buffers -- upstream hashes the node pointer it is about to
/// push, and the item's own [`Hash`] stands in here. A batch is dealt
/// round-robin instead and hashes nothing; see [`push_bulk`](Self::push_bulk).
///
/// More buffers measurably help the submission path this type exists for, and
/// the count is still `bit_width(num_workers)` because it is not a property of
/// that path alone: `Shared::num_queues` is workers plus buffers, and that one
/// number is also every worker's victim space, the spin budget before a worker
/// parks, and the length of the [`first_nonempty`](Self::first_nonempty)
/// sweep. `BENCHMARKS.md`'s section on the buffer count has what a wider
/// victim space costs a chain and a task-group recursion, and what sizing the
/// two apart would have to look like if the win is ever wanted.
#[derive(Debug)]
pub struct Buffers<T> {
    // Upstream gives each queue `alignas(TF_CACHELINE_SIZE)`. crossbeam already
    // pads an `Injector`'s head and tail internally, so this is belt and
    // braces, but it keeps the no-false-sharing requirement visible at the
    // layer that depends on it rather than inside a dependency.
    buffers: Vec<CachePadded<Injector<T>>>,
    /// Where the next [`push_bulk`](Self::push_bulk) starts dealing, so that
    /// two batches do not both begin at buffer zero. Advanced once per batch,
    /// never per item, and padded because it is the one *written* field here:
    /// the `Vec`'s pointer and length are read by every steal attempt, and a
    /// counter on their cache line would dirty it for every thief.
    rotation: CachePadded<AtomicUsize>,
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
            rotation: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    /// The number of buffers, which the executor needs in order to address
    /// them: upstream numbers its steal victims `0..num_workers + num_buffers`
    /// and treats the tail of that range as buffer indices.
    #[must_use]
    pub fn num_buffers(&self) -> usize {
        self.buffers.len()
    }

    /// The index of the first buffer that was not empty, or `None` if they
    /// all were: a hint, the buffers not being read atomically with respect to
    /// each other, and the same hint upstream checks before a worker commits
    /// to sleeping. The index is the part that matters -- a worker that finds
    /// work here points its sticky victim at the buffer holding it.
    #[must_use]
    pub fn first_nonempty(&self) -> Option<usize> {
        self.buffers.iter().position(|buffer| !buffer.is_empty())
    }

    /// Whether every buffer was empty at the moment of the call: as much of a
    /// hint as [`first_nonempty`](Self::first_nonempty), which it asks.
    pub fn is_empty(&self) -> bool {
        self.first_nonempty().is_none()
    }

    /// The index of the buffer [`push`](Self::push) routes `item` to: stable
    /// for a given item and buffer count, which is what makes the routing
    /// testable, and not a promise to callers, for whom which buffer an item
    /// lands in is an implementation detail.
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

    /// Pushes an item into one of the buffers, from any thread, including
    /// several at once.
    pub fn push(&self, item: T)
    where
        T: Hash,
    {
        self.buffers[self.buffer_of(&item)].push(item);
    }

    /// Pushes a batch of items into the buffers, dealing them round-robin, and
    /// reports how many there were.
    ///
    /// The whole of the external submission path: one graph's source tasks,
    /// queued by a thread that is no worker. Upstream's counterpart is
    /// `Executor::_bulk_spill_round_robin`, dealing *chunks* of
    /// `N / num_buffers` and picking its starting buffer by a multiplicative
    /// hash of the first node pointer. Two differences, both measured and
    /// recorded in `BENCHMARKS.md`'s section on external submission: nothing
    /// is hashed per item, the hash being spent to reach a distribution
    /// dealing already has; and items are dealt rather than chunks, a chunked
    /// deal filling one buffer at a time and leaving a thief whose sticky
    /// victim the producer has moved past with nothing to take. The starting
    /// buffer rotates per batch rather than being hashed, which costs one
    /// relaxed `fetch_add` and spreads two concurrent batches as upstream's
    /// hash is meant to.
    pub fn push_bulk(&self, items: impl IntoIterator<Item = T>) -> usize {
        let num_buffers = self.buffers.len();
        // Relaxed: nothing is ordered by this counter, and two batches that
        // read the same value are merely dealt from the same buffer.
        let mut index = self.rotation.fetch_add(1, Ordering::Relaxed) % num_buffers;

        let mut pushed = 0;
        for item in items {
            self.buffers[index].push(item);
            pushed += 1;
            index += 1;
            if index == num_buffers {
                index = 0;
            }
        }
        pushed
    }

    /// Steals an item from the buffer at `index`, or `None` if there was
    /// nothing to take -- which, as for [`Thief::steal`], means it looked
    /// empty rather than that a race was lost.
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
/// upstream's queues do not have -- their `steal` reports a lost
/// compare-and-swap as "empty" and leaves the caller to pick another victim.
/// Reading a race as emptiness here would be worse than upstream rather than
/// equal to it: an `Injector` answers `Retry` while a *concurrent* steal is in
/// flight too, so a worker could park with work sitting in a buffer.
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
/// the low bits of an allocation being nearly constant; the shift is kept so
/// that the two can be compared line by line, and costs nothing on a hash
/// whose bits are already well mixed.
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
    fn a_bulk_push_deals_evenly_over_every_buffer() {
        let buffers = Buffers::new(32);
        let num_buffers = buffers.num_buffers();
        const ITEMS: usize = 1000;

        assert_eq!(buffers.push_bulk(0..ITEMS), ITEMS);

        // This is the histogram #60 asked for, as an assertion: a batch dealt
        // round-robin differs by at most one item between buffers, where the
        // hash merely tends to.
        let mut histogram = vec![0_usize; num_buffers];
        for (index, count) in histogram.iter_mut().enumerate() {
            while buffers.steal_from(index).is_some() {
                *count += 1;
            }
        }
        assert_eq!(histogram.iter().sum::<usize>(), ITEMS, "{histogram:?}");
        let smallest = histogram.iter().min().expect("at least one buffer");
        let largest = histogram.iter().max().expect("at least one buffer");
        assert!(largest - smallest <= 1, "uneven deal: {histogram:?}");
        assert!(buffers.is_empty());
    }

    #[test]
    fn an_empty_bulk_push_pushes_nothing() {
        let buffers = Buffers::<usize>::new(8);
        assert_eq!(buffers.push_bulk(std::iter::empty()), 0);
        assert!(buffers.is_empty());
    }

    #[test]
    fn two_bulk_pushes_start_at_different_buffers() {
        let buffers = Buffers::new(32);
        let num_buffers = buffers.num_buffers();

        // One item each, so the only thing that can separate them is the
        // rotation: without it every batch would begin at buffer zero.
        let starts: Vec<usize> = (0..num_buffers)
            .map(|item| {
                assert_eq!(buffers.push_bulk([item]), 1);
                buffers
                    .first_nonempty()
                    .inspect(|&index| {
                        assert_eq!(buffers.steal_from(index), Some(item));
                    })
                    .expect("the item is in some buffer")
            })
            .collect();

        assert_eq!(
            starts,
            (0..num_buffers).collect::<Vec<_>>(),
            "the deal did not rotate"
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
                    let base = pusher * PER_PUSHER;
                    // Half the pushers go one item at a time and half in bulk,
                    // so that the two paths are exercised against each other.
                    if pusher % 2 == 0 {
                        for item in 0..PER_PUSHER {
                            buffers.push(base + item);
                        }
                    } else {
                        assert_eq!(buffers.push_bulk(base..base + PER_PUSHER), PER_PUSHER);
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

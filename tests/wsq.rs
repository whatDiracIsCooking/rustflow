//! Translated from `unittests/test_wsq.cpp` (Taskflow v4.1.0).
//!
//! Upstream's file is two suites over two hand-written Chase-Lev deques,
//! `BoundedWSQ` and `UnboundedWSQ`, and this port has neither: the queues
//! themselves are `crossbeam-deque`, and `src/executor/queue.rs` carries the
//! layer around them -- [`WorkerQueue`] and its `Thief`, plus the shared
//! spill [`Buffers`] a thread with no queue of its own pushes into. So the
//! cases here are upstream's assertions about *that layer*: the owner's
//! push/pop order, the order a thief takes them in, thieves racing the owner
//! and each other with every item seen exactly once, and a batch dealt into
//! the buffers.
//!
//! The bounded/unbounded split does not survive the port -- there is one
//! queue type, and its push cannot fail -- so each pair of upstream cases
//! collapses to one test here, named after the `UnboundedWSQ` half, which is
//! the one whose semantics the port has.
//!
//! Upstream cases with no counterpart here, each for the same underlying
//! reason -- the deque's own invariants are `crossbeam-deque`'s to test, and
//! a lookalike written over its public API would assert the dependency's
//! documented behaviour rather than this port's:
//!
//! - **`BoundedWSQ.Owner.LogSize=2` through `=10`.** The body fills a ring of
//!   `1 << LogSize` items, requires the next `try_push` to fail, drains, and
//!   repeats to stress the wrap-around of the circular buffer. The port's
//!   queue is unbounded, so there is no capacity to fill, no `try_push` that
//!   can return `false`, and the growth that replaces both is the
//!   dependency's.
//! - **`UnboundedWSQ.Resize`.** Asserts `capacity()` doubling to the next
//!   power of two as a batch is pushed, and not shrinking on drain. The port
//!   publishes no capacity, and `DESIGN.md`'s entry on `queue_size` versus
//!   `queue_capacity` has why there is no number it could honestly return.
//! - **The `empty_value()` half of `BoundedWSQ.ValueType` and
//!   `UnboundedWSQ.ValueType`.** Both `static_assert` that a queue of
//!   pointers reports emptiness as `nullptr` and a queue of values as
//!   `std::nullopt`. The port's `pop` and `steal` return `Option<T>` for
//!   every `T`, so there is no pointer specialisation to assert -- what is
//!   kept is the rest of the case, below, run over both a value type and a
//!   reference type.
//! - **Every `BoundedWSQ.*Consumer*` case**, by the collapse above: the
//!   bounded and unbounded consumer sweeps differ only in whether the
//!   producer retries a failed push.
//!
//! Three standing adaptations:
//!
//! - **The batch path is `Buffers`, not the queue.** Upstream's
//!   `try_bulk_push`/`bulk_push` is an owner pushing a batch onto its own
//!   deque; this port has no such method, because the batch it exists for is
//!   external submission, which lands in the spill buffers instead. So the
//!   `BulkPush` cases are translated over [`Buffers::push_bulk`], and their
//!   thieves sweep the buffers rather than one queue.
//! - **Sizes are smaller.** Upstream sweeps a single case to 777777 items
//!   under a `doctest::timeout(300)`; these sweep to [`MAX_OWNER_ITEMS`] and
//!   [`MAX_CONTENDED_ITEMS`]. The sizes are there to cross the deque's growth
//!   boundaries repeatedly while every thread is live, which the sweep still
//!   does several times over, and nothing in the file asserts anything about
//!   a particular size.
//! - **A spin has a deadline.** `cargo test` has no per-case timeout where
//!   doctest gave upstream one, and the failure these cases exist to catch is
//!   an item that is never taken, so each loop that waits for one carries
//!   [`GIVE_UP_AFTER`] in its condition. It relaxes nothing: the deadline
//!   only decides whether the case *reports* a lost item or hangs the binary,
//!   and the assertion that reports it is upstream's own whole-range
//!   comparison.

use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rustflow::{Buffers, WorkerQueue};

#[macro_use]
mod common;

/// The largest size the single-threaded sweeps reach, where upstream reaches
/// 777777.
const MAX_OWNER_ITEMS: usize = 262_143;

/// The largest size the sweeps with thieves running reach, where upstream
/// reaches 265720. Smaller than [`MAX_OWNER_ITEMS`] because every size is run
/// once per entry of the thief matrix, and twice within each.
const MAX_CONTENDED_ITEMS: usize = 9_841;

/// How long a loop waiting on another thread's steals keeps trying before it
/// gives up and lets the assertion report what went missing. Generous: it is
/// only ever reached when the case is about to fail.
const GIVE_UP_AFTER: Duration = Duration::from_secs(60);

// ============================================================================
// The owner alone
// ============================================================================

/// `UnboundedWSQ.Owner`: the owner pushes `N` items and takes them all back
/// twice -- newest first through [`WorkerQueue::pop`], oldest first through
/// `Thief::steal` -- over upstream's sweep of sizes.
#[test]
fn unbounded_wsq_owner() {
    let queue = WorkerQueue::new();
    let thief = queue.thief();

    let mut n = 1;
    while n <= MAX_OWNER_ITEMS {
        assert!(queue.is_empty(), "{n} items: the queue did not drain");

        for item in 0..n {
            queue.push(item);
        }
        assert_eq!(queue.len(), n);
        for expected in (0..n).rev() {
            assert_eq!(queue.pop(), Some(expected), "{n} items");
        }
        assert_eq!(queue.pop(), None);

        for item in 0..n {
            queue.push(item);
        }
        assert_eq!(thief.len(), n);
        for expected in 0..n {
            assert_eq!(thief.steal(), Some(expected), "{n} items");
        }
        assert_eq!(thief.steal(), None);

        n = n * 2 + 1;
    }
}

/// `UnboundedWSQ.ValueType`: pop takes the newest item and steal the oldest,
/// whatever the item type is, and an empty queue answers neither.
///
/// Upstream's case is two queues because its emptiness sentinel depends on
/// the item type; here it is one body over two, for the reason the module
/// docs give.
#[test]
fn unbounded_wsq_value_type() {
    fn both_ends<T: Send + Copy + Debug + PartialEq>(items: [T; 4]) {
        let queue = WorkerQueue::new();
        let thief = queue.thief();
        assert_eq!(queue.pop(), None);

        for item in items {
            queue.push(item);
        }
        for item in items.into_iter().rev() {
            assert_eq!(queue.pop(), Some(item));
        }
        assert_eq!(queue.pop(), None);

        for item in items {
            queue.push(item);
        }
        for item in items {
            assert_eq!(thief.steal(), Some(item));
        }
        assert_eq!(thief.steal(), None);
    }

    both_ends([1, 2, 3, 4]);

    // A queue of references stands in for upstream's `BoundedWSQ<void*>`:
    // the case is that it is the same queue, with no `nullptr` of its own.
    let gold = [1, 2, 3, 4];
    both_ends([&gold[0], &gold[1], &gold[2], &gold[3]]);
}

// ============================================================================
// Thieves against the owner
// ============================================================================

/// `UnboundedWSQ.`*n*`Consumer`: `consumers` threads steal while the owner
/// pushes and pops, and every item is taken exactly once.
///
/// Upstream's three phases, in its order: the owner alone on both ends of the
/// queue, then the thieves against it.
fn unbounded_wsq_n_consumers(consumers: usize) {
    let queue = WorkerQueue::new();
    let thief = queue.thief();

    let mut n = 1;
    while n <= MAX_CONTENDED_ITEMS {
        assert!(queue.is_empty(), "{n} items: the queue did not drain");

        // Push and pop.
        for item in 0..n {
            queue.push(item);
        }
        assert_eq!(queue.len(), n);
        for expected in (0..n).rev() {
            assert_eq!(queue.pop(), Some(expected), "{n} items");
        }
        assert!(queue.is_empty());

        // Push and steal.
        for item in 0..n {
            queue.push(item);
        }
        assert_eq!(thief.len(), n);
        for expected in 0..n {
            assert_eq!(thief.steal(), Some(expected), "{n} items");
        }
        assert!(queue.is_empty());

        // Push against the thieves, and compete with them from the owner's
        // end, which is where `pop` racing `steal` for the last item lives.
        let taken = AtomicUsize::new(0);
        let deadline = Instant::now() + GIVE_UP_AFTER;
        let mut collected = thread::scope(|scope| {
            let thieves: Vec<_> = (0..consumers)
                .map(|_| {
                    let thief = queue.thief();
                    let taken = &taken;
                    scope.spawn(move || steal_until(taken, n, deadline, || thief.steal()))
                })
                .collect();

            for item in 0..n {
                queue.push(item);
            }

            let mine = steal_until(&taken, n, deadline, || queue.pop());
            thieves.into_iter().fold(mine, |mut all, thief| {
                all.extend(thief.join().expect("a thief panicked"));
                all
            })
        });

        // Equality against the whole range is upstream's sorted comparison
        // against its gold vector: no duplicates and no losses in one, a
        // duplicate displacing a missing item.
        collected.sort_unstable();
        assert_eq!(
            collected,
            (0..n).collect::<Vec<_>>(),
            "{consumers} consumers, {n} items"
        );
        assert_eq!(thief.steal(), None);
        assert_eq!(queue.pop(), None);
        assert!(queue.is_empty());

        n = n * 3 + 1;
    }
}

worker_matrix! {
    unbounded_wsq_n_consumers {
        unbounded_wsq_1_consumer(1),
        unbounded_wsq_2_consumers(2),
        unbounded_wsq_3_consumers(3),
        unbounded_wsq_4_consumers(4),
        unbounded_wsq_5_consumers(5),
        unbounded_wsq_6_consumers(6),
        unbounded_wsq_7_consumers(7),
        unbounded_wsq_8_consumers(8),
    }
}

// ============================================================================
// Thieves against a batch
// ============================================================================

/// `UnboundedWSQ.`*n*`Consumer.BulkPush`: a batch dealt into the spill
/// buffers, drained first by this thread and then against `consumers`
/// thieves, with every item taken exactly once.
///
/// The buffer count is `bit_width(consumers)` rather than a constant, so that
/// upstream's thief matrix sweeps the buffer counts an executor of that many
/// workers would have -- one buffer at the low end, four at the high.
fn unbounded_wsq_n_consumers_bulk_push(consumers: usize) {
    let buffers = Buffers::new(consumers);

    let mut n = 1;
    while n <= MAX_CONTENDED_ITEMS {
        assert_eq!(buffers.first_nonempty(), None, "{n} items: buffers dirty");

        // A single item goes where its own hash sends it, which is the one
        // thing `first_nonempty` can be pinned against exactly: with one item
        // in flight, the first non-empty buffer is that item's buffer. Any
        // item would do; the round's size is one that moves across the sweep.
        let item = n;
        buffers.push(item);
        let index = buffers.buffer_of(&item);
        assert!(index < buffers.num_buffers());
        assert_eq!(buffers.first_nonempty(), Some(index));
        assert_eq!(buffers.steal_from(index), Some(item));
        assert_eq!(buffers.first_nonempty(), None);

        // Deal the batch and drain it buffer by buffer. Upstream asserts its
        // batch comes back in order, from one queue; a deal spreads this one
        // over every buffer, so the order that survives is per buffer -- each
        // holds an ascending slice of the batch, which is what distinguishes
        // a round-robin deal from items piled into one buffer.
        assert_eq!(buffers.push_bulk(0..n), n);
        let mut collected = Vec::with_capacity(n);
        for index in 0..buffers.num_buffers() {
            let mut dealt = Vec::new();
            while let Some(item) = buffers.steal_from(index) {
                dealt.push(item);
            }
            assert!(
                dealt.is_sorted(),
                "buffer {index} is out of order: {dealt:?}"
            );
            collected.extend(dealt);
        }
        collected.sort_unstable();
        assert_eq!(collected, (0..n).collect::<Vec<_>>(), "{n} items");
        assert_eq!(buffers.first_nonempty(), None);

        // Deal the batch again, this time against the thieves.
        let taken = AtomicUsize::new(0);
        let deadline = Instant::now() + GIVE_UP_AFTER;
        let mut collected = thread::scope(|scope| {
            let thieves: Vec<_> = (0..consumers)
                .map(|_| {
                    let buffers = &buffers;
                    let taken = &taken;
                    scope.spawn(move || {
                        let mut sweep = 0;
                        steal_until(taken, n, deadline, move || {
                            // One buffer per attempt, cycling: a thief that
                            // drained a buffer before looking at the next
                            // would hold it against the others for as long as
                            // the deal keeps refilling it.
                            sweep = (sweep + 1) % buffers.num_buffers();
                            buffers.steal_from(sweep)
                        })
                    })
                })
                .collect();

            assert_eq!(buffers.push_bulk(0..n), n);

            let mine = steal_until(&taken, n, deadline, || {
                buffers
                    .first_nonempty()
                    .and_then(|index| buffers.steal_from(index))
            });
            thieves.into_iter().fold(mine, |mut all, thief| {
                all.extend(thief.join().expect("a thief panicked"));
                all
            })
        });

        collected.sort_unstable();
        assert_eq!(
            collected,
            (0..n).collect::<Vec<_>>(),
            "{consumers} consumers, {n} items"
        );
        assert_eq!(buffers.first_nonempty(), None);

        n = n * 3 + 1;
    }
}

worker_matrix! {
    unbounded_wsq_n_consumers_bulk_push {
        unbounded_wsq_1_consumer_bulk_push(1),
        unbounded_wsq_2_consumers_bulk_push(2),
        unbounded_wsq_3_consumers_bulk_push(3),
        unbounded_wsq_4_consumers_bulk_push(4),
        unbounded_wsq_5_consumers_bulk_push(5),
        unbounded_wsq_6_consumers_bulk_push(6),
        unbounded_wsq_7_consumers_bulk_push(7),
        unbounded_wsq_8_consumers_bulk_push(8),
    }
}

// ============================================================================
// Shared by the two contended cases
// ============================================================================

/// Upstream's `while(consumed != N)` loop: take whatever `attempt` yields,
/// counting each item into `taken`, until all `n` are accounted for or
/// `deadline` passes, and report what this thread got.
///
/// `Relaxed` throughout, as upstream's `consumed` is: the count decides only
/// when to stop looking, and what orders the items themselves is the join at
/// the end of the scope.
fn steal_until<T>(
    taken: &AtomicUsize,
    n: usize,
    deadline: Instant,
    mut attempt: impl FnMut() -> Option<T>,
) -> Vec<T> {
    let mut mine = Vec::new();
    while taken.load(Ordering::Relaxed) < n && Instant::now() < deadline {
        if let Some(item) = attempt() {
            mine.push(item);
            taken.fetch_add(1, Ordering::Relaxed);
        } else {
            thread::yield_now();
        }
    }
    mine
}

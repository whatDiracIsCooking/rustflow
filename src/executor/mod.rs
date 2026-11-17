//! Translated from `taskflow/core/executor.hpp` (Taskflow v4.1.0): the
//! constructor, the bookkeeping half of `_spawn`, and `_shutdown`.
//!
//! The scheduler is built bottom-up, one submodule at a time, and the split is
//! our own arrangement -- upstream keeps the whole thing in `executor.hpp` with
//! the queues in `wsq.hpp`:
//!
//! - [`queue`] holds the queues a worker owns and the overflow buffers that
//!   threads outside the executor push into;
//! - [`notifier`] the sleep/wake protocol that idles a worker which finds no
//!   work;
//! - [`rng`] the one random number the steal loop needs;
//! - [`worker`] a worker and the scheduling loop it runs.
//!
//! What is left here is what the *executor* owns: the worker array, the
//! buffers, the notifier, the thread-id map, and the lifecycle -- spawn the
//! workers, hand them the shared state, join them on drop. See each submodule
//! for what it does and does not owe to upstream.

pub mod notifier;
pub mod queue;
pub mod rng;
pub mod worker;

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::thread::{self, JoinHandle, ThreadId};

use crossbeam_utils::CachePadded;

use crate::executor::notifier::{DefaultNotifier, Notifier};
use crate::executor::queue::{Buffers, WorkerQueue};
use crate::executor::worker::{Worker, WorkerInterface, WorkerThread};

/// One unit of work in a worker's queue: upstream's `Node*`.
///
/// A placeholder, and deliberately an empty one. The executor cannot schedule
/// anything until a topology exists to schedule, so there is nothing yet for a
/// job to *be*, and guessing now would mean guessing the shape of the thing
/// `run` hands to a worker. The scheduling loop is written in terms of this
/// type and nothing else, so filling it in is a change to this declaration and
/// to `WorkerThread::invoke`.
#[derive(Debug, Hash)]
pub(crate) struct Job;

/// A work-stealing executor: a fixed pool of worker threads over a shared set
/// of queues.
///
/// ```
/// use rustflow::Executor;
///
/// let executor = Executor::new(4);
/// assert_eq!(executor.num_workers(), 4);
///
/// // Four worker queues, plus the bit_width(4) = 3 shared buffers that
/// // threads outside the executor push into.
/// assert_eq!(executor.num_queues(), 7);
///
/// // The thread that built it is not one of its workers.
/// assert_eq!(executor.this_worker_id(), None);
/// ```
///
/// Dropping the executor shuts the pool down and joins every thread, which is
/// upstream's destructor. There is nothing to run on it yet: `run` and the
/// topology behind it are the next step.
pub struct Executor {
    /// Everything the executor shares with its workers.
    ///
    /// Behind an `Arc` because each worker thread holds the same state and
    /// nothing bounds the workers' lifetimes to this value's scope; [`Drop`] is
    /// what guarantees they are gone before the last clone is.
    pub(crate) shared: Arc<Shared>,
    /// The spawned threads, in worker-id order, joined by [`Drop`].
    pub(crate) threads: Vec<JoinHandle<()>>,
}

/// The executor state every worker thread holds a handle to.
///
/// The fields are `pub(crate)`, and so is the type, because the scheduler is
/// spread over several modules on purpose: the methods that `run` a graph are
/// `impl Executor` blocks in the modules that own that part of the port, and
/// they reach the queues and the notifier through here. Nothing is public,
/// though -- a caller outside the crate has no business with a worker's queue.
pub(crate) struct Shared {
    /// One record per worker, in id order.
    ///
    /// Cache-padded, as upstream pads `Worker::_done`: shutting the pool down
    /// writes every worker's flag, and an unpadded array would put several of
    /// them on a line that a running worker is reading.
    pub(crate) workers: Box<[CachePadded<Worker>]>,
    /// The overflow buffers that threads outside the executor push into.
    pub(crate) buffers: Buffers<Job>,
    /// Where a worker with nothing to steal goes to sleep.
    pub(crate) notifier: CachePadded<DefaultNotifier>,
    /// How many topologies are running, i.e. upstream's `_num_topologies`.
    ///
    /// Zero until `run` exists, which is why the whole steal loop still folds
    /// away through its fast path today. Padded separately from the notifier
    /// because upstream pads it separately, with a comment that says the false
    /// sharing between the two "can cause serious performance drop".
    pub(crate) num_topologies: CachePadded<AtomicUsize>,
    /// Which worker a thread is, by thread id.
    ///
    /// Upstream keeps the same map for the reason its comment gives: a
    /// thread-local would be unreliable on Windows, and a per-executor map
    /// answers "is the caller a worker *of this executor*" in one lookup
    /// instead of a lookup plus a check. Published once, before any worker
    /// leaves the start gate, so that reading it needs no lock.
    pub(crate) thread_ids: OnceLock<HashMap<ThreadId, usize>>,
    /// Holds every worker until the executor is built. See [`StartGate`].
    pub(crate) start: StartGate,
}

impl Shared {
    /// The number of queues a worker can steal from: one per worker, plus the
    /// shared buffers. Upstream's `num_queues`, and the index space its victim
    /// draw runs over.
    pub(crate) fn num_queues(&self) -> usize {
        self.workers.len() + self.buffers.num_buffers()
    }

    /// How many topologies are running. Relaxed, as upstream reads it: every
    /// caller treats the answer as a hint and re-checks under the notifier's
    /// two-phase guard before it sleeps.
    pub(crate) fn num_topologies(&self) -> usize {
        self.num_topologies.load(Ordering::Relaxed)
    }
}

/// The gate every worker waits at before it enters the scheduling loop.
///
/// Upstream has no equivalent, and pays for it: `_spawn` fills the thread-id
/// map from the spawning thread while the threads it has already created are
/// running, and initialises each worker's fields *inside* that worker's thread,
/// which is why `_wait_for_task` has a comment about avoiding "data race with
/// `_spawn`". Holding the workers at a gate until the executor is built costs
/// one synchronisation per executor and buys two things: every [`Worker`] is
/// fully initialised before any thread can observe it, and the thread-id map is
/// immutable by the time anyone can read it, so `this_worker_id` is a plain
/// lookup rather than a locked one.
///
/// [`Executor::drop`] opens the gate as well as setting the done flags, so that
/// a panic part-way through spawning cannot leave a worker waiting here.
pub(crate) struct StartGate {
    /// Whether the executor has finished building.
    open: Mutex<bool>,
    /// Signalled once, when `open` becomes true.
    opened: Condvar,
}

impl StartGate {
    /// A closed gate.
    fn new() -> Self {
        Self {
            open: Mutex::new(false),
            opened: Condvar::new(),
        }
    }

    /// Opens the gate and releases every waiter. Idempotent.
    pub(crate) fn open(&self) {
        *self.lock() = true;
        self.opened.notify_all();
    }

    /// Blocks until the gate is open.
    pub(crate) fn wait(&self) {
        let _open = self
            .opened
            .wait_while(self.lock(), |open| !*open)
            .unwrap_or_else(PoisonError::into_inner);
    }

    /// The flag. Poisoning carries no information here -- the guarded value is
    /// one `bool` and nothing under the lock can panic -- so a poisoned lock is
    /// taken rather than propagated, as the notifier's is.
    fn lock(&self) -> MutexGuard<'_, bool> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Executor {
    /// Creates an executor with `num_workers` worker threads.
    ///
    /// # Panics
    ///
    /// Panics if `num_workers` is zero, where upstream throws: an executor with
    /// no workers has nowhere to put work and nothing to run it.
    #[must_use]
    pub fn new(num_workers: usize) -> Self {
        Self::spawn(num_workers, None)
    }

    /// Creates an executor with `num_workers` worker threads, calling
    /// `interface` on each of them as it enters and leaves the scheduling loop.
    ///
    /// # Panics
    ///
    /// Panics if `num_workers` is zero, as [`Executor::new`] does.
    #[must_use]
    pub fn with_worker_interface(num_workers: usize, interface: Arc<dyn WorkerInterface>) -> Self {
        Self::spawn(num_workers, Some(interface))
    }

    /// The number of worker threads. Upstream's `num_workers`.
    #[must_use]
    pub fn num_workers(&self) -> usize {
        self.shared.workers.len()
    }

    /// The number of queues a worker can steal from: one per worker plus the
    /// shared buffers. Upstream's `num_queues`.
    #[must_use]
    pub fn num_queues(&self) -> usize {
        self.shared.num_queues()
    }

    /// How many workers the notifier is accounting for right now.
    ///
    /// Upstream's `num_waiters`, and worth what the notifier says it is worth:
    /// a worker counts from the moment it opens a wait, not from the moment it
    /// actually sleeps.
    #[must_use]
    pub fn num_waiters(&self) -> usize {
        self.shared.notifier.num_waiters()
    }

    /// How many topologies are running. Upstream's `num_topologies`.
    ///
    /// Zero until there is something to run.
    #[must_use]
    pub fn num_topologies(&self) -> usize {
        self.shared.num_topologies()
    }

    /// Returns the calling thread's worker in this executor, or `None` if the
    /// caller is not one of its workers.
    ///
    /// Upstream's `this_worker`. A thread that is a worker of a *different*
    /// executor gets `None` here, which is the whole point of the map being
    /// per-executor rather than a thread-local.
    #[must_use]
    pub fn this_worker(&self) -> Option<&Worker> {
        self.worker_of(thread::current().id())
    }

    /// Returns the calling thread's worker id in this executor, or `None` if
    /// the caller is not one of its workers.
    ///
    /// Upstream's `this_worker_id`, which answers `-1` for a non-worker; an
    /// `Option` says the same thing without a sentinel.
    #[must_use]
    pub fn this_worker_id(&self) -> Option<usize> {
        self.this_worker().map(Worker::id)
    }

    /// The worker that owns `thread`, if this executor has one.
    fn worker_of(&self, thread: ThreadId) -> Option<&Worker> {
        let &id = self.shared.thread_ids.get()?.get(&thread)?;
        self.shared.workers.get(id).map(|worker| &**worker)
    }

    /// Builds the shared state and spawns the pool. Upstream's constructor plus
    /// the half of `_spawn` that is bookkeeping rather than scheduling.
    fn spawn(num_workers: usize, interface: Option<Arc<dyn WorkerInterface>>) -> Self {
        assert!(num_workers > 0, "an executor needs at least one worker");

        // The queues are built out here because their two ends go to different
        // owners: the thief into the worker record the whole pool shares, the
        // owning end into the one thread allowed to push and pop.
        let queues: Vec<WorkerQueue<Job>> = (0..num_workers).map(|_| WorkerQueue::new()).collect();
        let workers = queues
            .iter()
            .enumerate()
            .map(|(id, queue)| CachePadded::new(Worker::new(id, queue.thief())))
            .collect();

        // The executor exists before the first thread does, so that an unwind
        // part-way through spawning runs `Drop` and takes the threads already
        // spawned down with it.
        let mut executor = Self {
            shared: Arc::new(Shared {
                workers,
                buffers: Buffers::new(num_workers),
                notifier: CachePadded::new(DefaultNotifier::new(num_workers)),
                num_topologies: CachePadded::new(AtomicUsize::new(0)),
                thread_ids: OnceLock::new(),
                start: StartGate::new(),
            }),
            threads: Vec::with_capacity(num_workers),
        };

        for (id, queue) in queues.into_iter().enumerate() {
            let shared = Arc::clone(&executor.shared);
            let interface = interface.clone();
            let thread = thread::Builder::new()
                // Upstream leaves its threads unnamed. A name costs one
                // allocation per executor and is what a debugger, a profile and
                // a panic message all show instead of an anonymous thread.
                .name(format!("rustflow-worker-{id}"))
                .spawn(move || WorkerThread::new(shared, id, queue).run(interface.as_deref()))
                .expect("the OS refused to spawn a worker thread");
            executor.threads.push(thread);
        }

        let thread_ids = executor
            .threads
            .iter()
            .enumerate()
            .map(|(id, thread)| (thread.thread().id(), id))
            .collect();
        assert!(
            executor.shared.thread_ids.set(thread_ids).is_ok(),
            "the thread-id map is published exactly once, at the end of spawn"
        );

        executor.shared.start.open();
        executor
    }
}

impl Default for Executor {
    /// An executor with one worker per available core.
    ///
    /// Upstream defaults to `std::thread::hardware_concurrency()` and throws if
    /// that reports zero; [`std::thread::available_parallelism`] reports an
    /// error instead of a zero, and a single worker is a better answer to it
    /// than a panic -- the pool still runs everything, just not in parallel.
    fn default() -> Self {
        Self::new(thread::available_parallelism().map_or(1, NonZeroUsize::get))
    }
}

impl Drop for Executor {
    /// Upstream's `_shutdown`: tell every worker to stop, wake the ones that
    /// are asleep, and join them all.
    ///
    /// Upstream first calls `wait_for_all` so that a topology still running
    /// finishes before the pool goes away. There is no topology to wait for
    /// yet; when there is, this is where that wait belongs.
    fn drop(&mut self) {
        for worker in &self.shared.workers {
            // Relaxed, as upstream's `test_and_set` is: `notify_all` below
            // carries the sequentially consistent fence that orders this write
            // against a worker's last look at the flag before it sleeps, which
            // is the notifier's two-phase guarantee.
            worker.done.store(true, Ordering::Relaxed);
        }

        // Before the notification, not after: a worker still at the gate has
        // never looked at the notifier, so waking the notifier would not reach
        // it. Normally the gate is long since open and this is a no-op.
        self.shared.start.open();

        self.shared.notifier.notify_all();

        for thread in self.threads.drain(..) {
            // A worker that panicked is still a worker that stopped, and its
            // payload has already been through the epilogue. Upstream likewise
            // joins and looks at nothing.
            let _ = thread.join();
        }
    }
}

impl core::fmt::Debug for Executor {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Executor")
            .field("num_workers", &self.num_workers())
            .field("num_queues", &self.num_queues())
            .field("num_waiters", &self.num_waiters())
            .field("num_topologies", &self.num_topologies())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;

    #[test]
    #[should_panic(expected = "an executor needs at least one worker")]
    fn an_executor_needs_at_least_one_worker() {
        let _ = Executor::new(0);
    }

    /// The steal loop draws a victim with `rng() % (num_queues - 1)`, so it
    /// needs at least two queues to draw from -- which upstream's constructor
    /// comment claims is "guaranteed >= 2", and is, but only because the buffer
    /// count is a bit width and never zero.
    #[test]
    fn there_are_always_at_least_two_queues_to_steal_from() {
        for num_workers in 1..=8 {
            let executor = Executor::new(num_workers);
            assert_eq!(executor.num_workers(), num_workers);
            assert!(
                executor.num_queues() >= 2,
                "{num_workers} workers gave {} queues",
                executor.num_queues()
            );
            assert!(executor.num_queues() > num_workers, "no buffers");
        }
    }

    /// Every worker thread is in the map, exactly once, under its own id: the
    /// map is what `this_worker_id` is, so a missing or duplicated entry would
    /// make a worker invisible to the executor it belongs to.
    #[test]
    fn the_thread_id_map_covers_every_worker_exactly_once() {
        let executor = Executor::new(5);
        let map = executor
            .shared
            .thread_ids
            .get()
            .expect("the map is published before `spawn` returns");

        assert_eq!(map.len(), 5);
        let mut ids: Vec<usize> = map.values().copied().collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..5).collect::<Vec<_>>());

        for (thread, &id) in map {
            assert_eq!(
                executor.worker_of(*thread).map(Worker::id),
                Some(id),
                "worker {id} does not answer to its own thread id"
            );
        }
    }

    /// The gate has to release a waiter that arrived before it opened *and* one
    /// that arrives after, or a worker spawned late would never start.
    #[test]
    fn the_start_gate_releases_waiters_on_both_sides_of_the_open() {
        let gate = Arc::new(StartGate::new());
        let passed = Arc::new(AtomicBool::new(false));

        let early = {
            let gate = Arc::clone(&gate);
            let passed = Arc::clone(&passed);
            thread::spawn(move || {
                gate.wait();
                passed.store(true, Ordering::SeqCst);
            })
        };

        assert!(!*gate.lock(), "a fresh gate is closed");
        gate.open();
        early.join().expect("the early waiter panicked");
        assert!(passed.load(Ordering::SeqCst));

        // An open gate stops nobody, and opening it twice is harmless.
        gate.open();
        gate.wait();
    }

    /// `Default` is the only constructor that can land on a worker count we did
    /// not choose, so the floor matters: zero workers would panic in `spawn`.
    #[test]
    fn a_default_executor_has_at_least_one_worker() {
        let executor = Executor::default();
        assert!(executor.num_workers() >= 1);
        assert_eq!(executor.num_topologies(), 0);
        assert_eq!(executor.this_worker_id(), None);
    }
}

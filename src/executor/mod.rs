//! Translated from `taskflow/core/executor.hpp` (Taskflow v4.1.0): the
//! constructor, the bookkeeping half of `_spawn`, and `_shutdown`.
//!
//! What this module owns is what the *executor* owns: the worker array, the
//! buffers, the notifier, the thread-id map, and the lifecycle -- spawn the
//! workers, hand them the shared state, join them on drop. The scheduler is
//! built bottom-up one submodule at a time, which is our own arrangement,
//! upstream keeping all of it in `executor.hpp` and `wsq.hpp`:
//!
//! - [`queue`] the queues a worker owns and the buffers other threads push
//!   into, [`notifier`] the sleep/wake protocol that idles a worker with
//!   nothing to steal, [`rng`] the one random number the steal loop needs,
//!   [`worker`] a worker and the loop it runs;
//! - `topology` a run and its cancellation bits (`run`), the job a queue
//!   carries and the walk that finds the anchor a failure reaches (`job`), and
//!   what a job owns -- the graph, the storage, the parent its graph was
//!   scheduled under (`ownership`) -- all crate-private;
//! - `async_node` the free-standing node an async task is, `async` and
//!   `dependent_async` the two node kinds that run on one, `runtime_async` the
//!   same submissions from a [`Runtime`](crate::Runtime) body, which is where
//!   a free node acquires a *parent*;
//! - `module`, `subflow` and `runtime` one dynamic-tasking node kind each,
//!   reached by one line from the worker's handle match;
//! - [`run_handle`] the `run` family, the tear-down that decides whether a
//!   finished run goes round again, the handle whose borrow is this port's
//!   answer to "do not mutate a running taskflow", and the
//!   [`Canceller`](crate::Canceller) that cancels a run from elsewhere.
//!
//! # Design
//!
//! The scheduler contains no `unsafe` at all, which is a property to keep
//! rather than a coincidence to notice: its one `unsafe` boundary was a job
//! holding raw pointers at the run and at the taskflow's storage, and it went
//! when a job started owning what it names. What is left is [`notifier`]'s
//! waiter stack, where a worker about to park publishes its
//! [`Thread`](std::thread::Thread) handle into a slot only the notifier that
//! popped it may read, modelled by `tests/loom_notifier.rs`. A node kind that
//! needs a raw pointer is a design to reconsider, not a `// SAFETY:` to write.
//!
//! A graph with no source task -- empty, or with every node in a cycle -- is
//! nothing to run, and every entry point says so where upstream tests
//! `graph.empty()`. The widening is not cosmetic: a sourceless graph queues no
//! job, so nothing would drive the join counter of the node that preempted
//! itself for it, and a node left `PREEMPTED` hangs the run.
//!
//! The protocols the submodules share are in `DESIGN.md`: what a run promises
//! when it is cancelled, which anchor a failure inside a nested graph is
//! delivered to, what a nested graph counts down, the two entries a node that
//! parents one is dispatched through, and the two hooks a
//! [`Semaphore`](crate::Semaphore) puts around the dispatch. One interaction
//! between those hooks and cancellation is upstream's behaviour and **recorded
//! rather than fixed**: a job parked on a semaphore when its run is cancelled
//! *stays parked*. Nothing sweeps a waiting list, so it is scheduled only when
//! some other task releases, and until then the run's counter never reaches
//! zero -- the one way a cancelled run does not finish promptly, a parked job
//! having nobody to dequeue it where a queued one is torn down by the
//! early-out. The order of the hooks bounds it, the cancellation check coming
//! before the acquire; what is left is the race between the two, and the task
//! that would have released being itself one the cancellation skipped.
//! [`Semaphore::reset`](crate::Semaphore::reset) is no rescue: it drops the
//! waiting list, unscheduling those tasks for good.
pub(crate) mod r#async;
pub(crate) mod async_node;
pub mod corun;
pub(crate) mod dependent_async;
pub(crate) mod module;
pub mod notifier;
pub mod queue;
pub mod rng;
pub mod run_handle;
pub(crate) mod runtime;
pub(crate) mod runtime_async;
pub mod scope;
pub(crate) mod subflow;
pub(crate) mod topology;
pub mod worker;

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::thread::{self, JoinHandle, ThreadId};

use crossbeam_utils::CachePadded;

use crate::executor::notifier::{DefaultNotifier, Notifier};
use crate::executor::queue::{Buffers, WorkerQueue};
use crate::executor::topology::Job;
use crate::executor::worker::{Worker, WorkerInterface, WorkerThread};

/// A work-stealing executor: a fixed pool of worker threads over a shared set
/// of queues. [`Executor::run`] puts a taskflow on it.
///
/// ```
/// use rustflow::Executor;
///
/// let executor = Executor::new(4);
/// assert_eq!(executor.num_workers(), 4);
/// // Four worker queues, plus the bit_width(4) = 3 shared buffers.
/// assert_eq!(executor.num_queues(), 7);
/// // The thread that built it is not one of its workers.
/// assert_eq!(executor.this_worker_id(), None);
/// ```
pub struct Executor {
    /// Everything the executor shares with its workers, behind an `Arc`
    /// because nothing bounds a worker's lifetime to this value's scope:
    /// [`Drop`] is what guarantees they are gone before the last clone is.
    pub(crate) shared: Arc<Shared>,
    /// The spawned threads, in worker-id order, joined by [`Drop`].
    pub(crate) threads: Vec<JoinHandle<()>>,
}

/// The executor state every worker thread holds a handle to.
///
/// The fields are `pub(crate)` because the scheduler is spread over several
/// modules on purpose: the methods that `run` a graph are `impl Executor`
/// blocks in the modules owning that part of the port, reaching the queues
/// and the notifier through here.
pub(crate) struct Shared {
    /// One record per worker, in id order. Cache-padded, as upstream pads
    /// `Worker::_done`: shutting the pool down writes every worker's flag, and
    /// an unpadded array would share a line a worker is reading.
    pub(crate) workers: Box<[CachePadded<Worker>]>,
    /// The overflow buffers that threads outside the executor push into.
    pub(crate) buffers: Buffers<Job>,
    /// Where a worker with nothing to steal goes to sleep.
    pub(crate) notifier: CachePadded<DefaultNotifier>,
    /// How many topologies are running: upstream's `_num_topologies`, runs
    /// plus the free async tasks counted in here as upstream's own `async` and
    /// `silent_async` are. Read on the hot path -- a worker with nothing to
    /// steal checks it before sweeping the queues, zero meaning every queue is
    /// empty -- and padded separately from the notifier, as upstream pads it,
    /// their false sharing being what its comment says "can cause serious
    /// performance drop".
    pub(crate) num_topologies: CachePadded<AtomicUsize>,
    /// How many *free async tasks* are queued or running: the half of
    /// `num_topologies` that [`Drop`] waits for, which upstream has no
    /// equivalent of and `DESIGN.md` has the decision for. Padded not for
    /// being read on the hot path -- it is *written* by every async submission
    /// and tear-down, and a line shared with the count a worker checks before
    /// every sweep is the false sharing upstream warns about.
    pub(crate) num_asyncs: CachePadded<AtomicUsize>,
    /// The lock [`Executor::wait_for_all`] reads `num_topologies` under.
    /// Upstream waits on the atomic itself, which C++20 can do and Rust
    /// cannot; the lock is what closes the window between a waiter reading a
    /// non-zero count and sleeping. Nothing on the scheduling path takes it.
    pub(crate) topologies: Mutex<()>,
    /// Signalled when `num_topologies` falls to zero, under that lock.
    pub(crate) no_topologies: Condvar,
    /// The lock [`Drop`] reads `num_asyncs` under: a pair of its own, so that
    /// a run finishing does not wake a shutdown waiting for async tasks.
    pub(crate) asyncs: Mutex<()>,
    /// Signalled when `num_asyncs` falls to zero.
    pub(crate) no_asyncs: Condvar,
    /// Which worker a thread is, by thread id. Upstream keeps the same map
    /// for the reason its comment gives -- a thread-local would be unreliable
    /// on Windows, and a per-executor map answers "a worker *of this
    /// executor*" -- published once, before any worker leaves the gate, so
    /// that reading it needs no lock.
    pub(crate) thread_ids: OnceLock<HashMap<ThreadId, usize>>,
    /// Holds every worker until the executor is built. See [`StartGate`].
    pub(crate) start: StartGate,
}

impl Shared {
    /// The number of queues a worker can steal from, one per worker plus the
    /// shared buffers: upstream's `num_queues`, and its victim draw's index
    /// space.
    pub(crate) fn num_queues(&self) -> usize {
        self.workers.len() + self.buffers.num_buffers()
    }

    /// How many topologies are running. Relaxed, as upstream reads it: every
    /// caller treats the answer as a hint and re-checks under the notifier's
    /// guard before sleeping.
    pub(crate) fn num_topologies(&self) -> usize {
        self.num_topologies.load(Ordering::Relaxed)
    }

    /// Counts one more run -- or one more free async task -- as in flight.
    /// Upstream's `_increment_topology`.
    pub(crate) fn increment_topology(&self) {
        self.num_topologies.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one run -- or one free async task -- as finished, waking
    /// [`Executor::wait_for_all`] if it was the last: `_decrement_topology`.
    pub(crate) fn decrement_topology(&self) {
        if self.num_topologies.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Under the lock: a waiter that read a non-zero count but has
            // not reached `wait` still holds it, so this notification cannot
            // arrive before the waiter is listening and be lost.
            let _idle = self
                .topologies
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.no_topologies.notify_all();
        }
    }

    /// Queues `jobs` from a thread that has no queue of its own: upstream's
    /// `_bulk_schedule(I first, size_t)`, everything going to the shared
    /// buffers and the notifier being told how many workers are worth waking.
    /// A worker scheduling its own successors uses
    /// [`WorkerThread::schedule`](crate::executor::worker::WorkerThread)
    /// instead. For a graph of 65536 sources this call *is* the run's critical
    /// path -- one producer pushing every job while 24 workers wait -- so the
    /// per-item cost is worth more than it looks; `BENCHMARKS.md` measured it.
    pub(crate) fn bulk_schedule(&self, jobs: impl IntoIterator<Item = Job>) {
        let num_jobs = self.buffers.push_bulk(jobs);

        // Notified after the pushes, as upstream notifies: a worker woken
        // before its work is visible would sweep the queues, find nothing and
        // go back to sleep. Upstream's commented-out experiment in
        // `_bulk_schedule` is this trade and records that waking first bought
        // nothing; measured here too, the run took close to twice as long,
        // the woken workers sweeping the very cache lines the producer is
        // still pushing into.
        if num_jobs > 0 {
            self.notifier.notify_n(num_jobs);
        }
    }
}

/// The gate every worker waits at before it enters the scheduling loop.
///
/// Upstream has no equivalent and pays for it: `_spawn` fills the thread-id map
/// while the threads it has created are running, which is why `_wait_for_task`
/// has a comment about avoiding "data race with `_spawn`". One synchronisation
/// per executor buys a fully initialised [`Worker`] before any thread can
/// observe it, and a thread-id map immutable by the time anybody can read it,
/// so that `this_worker_id` needs no lock. [`Executor::drop`] opens the gate
/// as well as setting the done flags, so that a panic part-way through
/// spawning cannot leave a worker waiting here.
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

    /// The flag. Poisoning carries no information -- the guarded value is one
    /// `bool` -- so a poisoned lock is taken rather than propagated.
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
        Self::spawn_pool(num_workers, None)
    }

    /// Creates an executor with `num_workers` worker threads, calling
    /// `interface` on each as it enters and leaves the scheduling loop.
    ///
    /// # Panics
    ///
    /// Panics if `num_workers` is zero, as [`Executor::new`] does.
    #[must_use]
    pub fn with_worker_interface(num_workers: usize, interface: Arc<dyn WorkerInterface>) -> Self {
        Self::spawn_pool(num_workers, Some(interface))
    }

    /// The number of worker threads. Upstream's `num_workers`.
    #[must_use]
    pub fn num_workers(&self) -> usize {
        self.shared.workers.len()
    }

    /// The number of queues a worker can steal from: upstream's `num_queues`.
    #[must_use]
    pub fn num_queues(&self) -> usize {
        self.shared.num_queues()
    }

    /// How many workers the notifier is accounting for: upstream's
    /// `num_waiters`, worth what [`Notifier::num_waiters`] says, since which
    /// workers a notifier counts is its own business and the two here count
    /// different things, as upstream's do. A diagnostic, never a predicate --
    /// "the pool is idle" is [`Executor::num_topologies`].
    #[must_use]
    pub fn num_waiters(&self) -> usize {
        self.shared.notifier.num_waiters()
    }

    /// How many topologies are running, and the count
    /// [`Executor::wait_for_all`] blocks on reaching zero: upstream's
    /// `num_topologies`, one per [`RunHandle`](crate::RunHandle) whose run has
    /// not finished plus one per *free* async task queued or running. A child
    /// of a [`Runtime`](crate::Runtime) body or of a
    /// [`TaskGroup`](crate::TaskGroup) is in neither, the parent waiting for
    /// it accounting for it.
    #[must_use]
    pub fn num_topologies(&self) -> usize {
        self.shared.num_topologies()
    }

    /// The calling thread's worker in this executor, or `None` if the caller
    /// is not one of its workers: upstream's `this_worker`. A worker of a
    /// *different* executor gets `None`, which is the point of the map being
    /// per-executor rather than a thread-local.
    #[must_use]
    pub fn this_worker(&self) -> Option<&Worker> {
        self.worker_of(thread::current().id())
    }

    /// The calling thread's worker id, or `None` for a non-worker: upstream's
    /// `this_worker_id`, which answers `-1` where an `Option` needs no
    /// sentinel.
    #[must_use]
    pub fn this_worker_id(&self) -> Option<usize> {
        self.this_worker().map(Worker::id)
    }

    /// The worker that owns `thread`, if this executor has one.
    fn worker_of(&self, thread: ThreadId) -> Option<&Worker> {
        let &id = self.shared.thread_ids.get()?.get(&thread)?;
        self.shared.workers.get(id).map(|worker| &**worker)
    }

    /// Builds the shared state and spawns the pool: upstream's constructor
    /// plus the bookkeeping half of `_spawn`. Named `spawn_pool` because
    /// [`Executor::spawn`] is the public submission of an async task.
    fn spawn_pool(num_workers: usize, interface: Option<Arc<dyn WorkerInterface>>) -> Self {
        assert!(num_workers > 0, "an executor needs at least one worker");

        // Built out here because the two ends go to different owners: the
        // thief into the worker record the pool shares, the owning end into
        // the one thread allowed to push and pop.
        let queues: Vec<WorkerQueue<Job>> = (0..num_workers).map(|_| WorkerQueue::new()).collect();
        let workers = queues
            .iter()
            .enumerate()
            .map(|(id, queue)| CachePadded::new(Worker::new(id, queue.thief())))
            .collect();

        // The executor exists before the first thread does, so that an unwind
        // part-way through spawning runs `Drop` and takes the threads already
        // spawned with it.
        let mut executor = Self {
            shared: Arc::new(Shared {
                workers,
                buffers: Buffers::new(num_workers),
                notifier: CachePadded::new(DefaultNotifier::new(num_workers)),
                num_topologies: CachePadded::new(AtomicUsize::new(0)),
                num_asyncs: CachePadded::new(AtomicUsize::new(0)),
                topologies: Mutex::new(()),
                no_topologies: Condvar::new(),
                asyncs: Mutex::new(()),
                no_asyncs: Condvar::new(),
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
                // allocation and is what a debugger, a profile and a panic
                // message show instead of an anonymous thread.
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
    /// An executor with one worker per available core. Upstream defaults to
    /// `std::thread::hardware_concurrency()` and throws if that reports zero;
    /// [`std::thread::available_parallelism`] reports an error instead, and a
    /// single worker is the better answer than a panic -- the pool still runs
    /// everything, just not in parallel.
    fn default() -> Self {
        Self::new(thread::available_parallelism().map_or(1, NonZeroUsize::get))
    }
}

impl Drop for Executor {
    /// Upstream's `_shutdown`: wait for the work nobody else can wait for,
    /// tell every worker to stop, wake the ones asleep, and join them all.
    ///
    /// The wait is for **free async tasks only** -- `num_asyncs` rather than
    /// `num_topologies`, where upstream's first line is `wait_for_all` -- and
    /// `DESIGN.md` has the split: such a task has no handle borrowing the
    /// executor, while a *run* in flight here is one whose
    /// [`RunHandle`](crate::RunHandle) was leaked, and the answer to a leaked
    /// handle is that the pool still comes down. The foot-gun that comes with
    /// the wait is upstream's too: dropping the last share of an executor
    /// *from inside one of its own async tasks* deadlocks.
    fn drop(&mut self) {
        // Before the `done` flags, not after: a worker told to stop leaves
        // its loop with whatever is still queued unrun, so the flags cannot be
        // set while an async task waits for a worker to pick it up.
        self.shared.wait_for_asyncs();

        for worker in &self.shared.workers {
            // Relaxed, as upstream's `test_and_set` is: `notify_all` below
            // carries the sequentially consistent fence that orders this write
            // against a worker's last look at the flag before it sleeps.
            worker.done.store(true, Ordering::Relaxed);
        }

        // Before the notification: a worker still at the gate has never
        // looked at the notifier, so a wake would not reach it. Normally the
        // gate is long since open and this is a no-op.
        self.shared.start.open();

        self.shared.notifier.notify_all();

        for thread in self.threads.drain(..) {
            // A worker that panicked is still a worker that stopped, its
            // payload having been through the epilogue. Upstream likewise
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

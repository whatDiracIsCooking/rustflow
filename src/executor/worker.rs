//! Translated from `taskflow/core/worker.hpp` (Taskflow v4.1.0).
//!
//! The scheduling loop itself -- upstream's `Executor::_spawn`,
//! `_exploit_task`, `_explore_task` and `_wait_for_task`, from
//! `taskflow/core/executor.hpp` of the same version -- is translated here too,
//! rather than into the parent module, because every line of it reads or writes
//! one worker's own state. The parent module keeps what the *executor* owns:
//! the worker array, the buffers, the notifier and the thread-id map.
//!
//! # Why a worker is two types here and one type upstream
//!
//! Upstream's `Worker` holds all of it in one struct: the id, the sticky
//! victim, the random generator, the bounded work-stealing queue, the thread
//! handle and the `done` flag. It can, because C++ lets the executor reach into
//! a worker from any thread and leaves "only the owner pops its own queue" as a
//! comment.
//!
//! Here the compiler asks which fields are shared, and the answer splits the
//! struct in two:
//!
//! - [`Worker`] is the part other threads touch -- the id, the `done` flag the
//!   executor sets at shutdown, and the [`Thief`] every other worker steals
//!   through. It is `Sync`, it lives in the executor's worker array, and it is
//!   what a [`WorkerInterface`] is handed.
//! - [`WorkerThread`] is the part only the worker's own thread touches: the
//!   owning end of its queue, its sticky victim and its generator. It is
//!   created inside the spawned thread and moves nowhere, so the "only the
//!   owner pushes or pops" rule that upstream writes in a comment is here a
//!   property of where the value lives.
//!
//! The one upstream field with no equivalent is `_thread`: a `JoinHandle` is
//! owned by whoever will join it, so the executor keeps the handles and the
//! worker does not. That costs the port upstream's `WorkerInterface` trick of
//! setting thread affinity from the prologue, which needs the handle; a
//! `Worker` here cannot hand one out.

use std::any::Any;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use super::{Job, Shared};
use crate::executor::notifier::Notifier;
use crate::executor::queue::{Thief, WorkerQueue};
use crate::executor::rng::Xorshift32;

/// A worker of an [`Executor`](crate::Executor), as every thread but its own
/// sees it.
///
/// A worker id is an index in `0..num_workers`, fixed when the executor is
/// built. The fields are `pub(crate)` rather than private because the
/// scheduling loop in this module and the run methods that later modules add to
/// `Executor` are all part of one scheduler that happens to be spread over
/// several files; `pub(crate)` is what lets those files stay separate.
#[derive(Debug)]
pub struct Worker {
    /// This worker's id, i.e. its index in the executor's worker array.
    pub(crate) id: usize,
    /// Set once, by the executor, when it wants this worker to leave the
    /// scheduling loop. Upstream's `_done`, and like upstream's it is read and
    /// written with relaxed ordering: the notifier's fences are what order a
    /// shutdown against a worker going to sleep, not this flag.
    pub(crate) done: AtomicBool,
    /// The steal side of this worker's queue, so that siblings can take work
    /// from it. The owning side lives in [`WorkerThread`].
    pub(crate) thief: Thief<Job>,
}

impl Worker {
    /// Creates the shared record for the worker with id `id`.
    pub(crate) fn new(id: usize, thief: Thief<Job>) -> Self {
        Self {
            id,
            done: AtomicBool::new(false),
            thief,
        }
    }

    /// Returns this worker's id, an index in `0..num_workers`.
    #[must_use]
    pub fn id(&self) -> usize {
        self.id
    }

    /// Returns `true` if this worker's queue held nothing at the moment of the
    /// call.
    ///
    /// Upstream exposes `queue_size()` and `queue_capacity()` here. A
    /// `crossbeam` deque reports neither -- and its capacity would be a moving
    /// target anyway, since it grows where upstream's is bounded -- so this is
    /// the part of that interface the port can answer, and it is the only part
    /// the scheduler itself asks for.
    #[must_use]
    pub fn is_queue_empty(&self) -> bool {
        self.thief.is_empty()
    }

    /// Returns `true` once the executor has asked this worker to stop.
    pub(crate) fn is_done(&self) -> bool {
        self.done.load(Ordering::Relaxed)
    }
}

/// A hook that runs on every worker thread, around the scheduling loop.
///
/// Upstream's `tf::WorkerInterface`, whose stated purpose is to let a caller
/// configure a worker thread -- its name, its priority, its CPU affinity --
/// from inside that thread, at the only two moments the executor offers.
///
/// Both methods take `&self` and run on every worker simultaneously, so an
/// implementation that accumulates anything needs its own synchronisation; that
/// is why the trait requires `Send + Sync` rather than leaving it to the
/// `Arc<dyn WorkerInterface>` the executor stores.
pub trait WorkerInterface: Send + Sync {
    /// Runs on the worker thread before it enters the scheduling loop.
    fn scheduler_prologue(&self, worker: &Worker);

    /// Runs on the worker thread after it leaves the scheduling loop, with the
    /// payload of the panic that ended the loop if one did.
    ///
    /// The payload stands in for upstream's `std::exception_ptr`: a panic is
    /// this port's translation of an exception escaping a task body, and
    /// [`panic::catch_unwind`] hands back exactly what was panicked with. It is
    /// `None` on every normal shutdown.
    fn scheduler_epilogue(&self, worker: &Worker, panicked: Option<Box<dyn Any + Send>>);
}

/// The worker state that belongs to the worker's own thread, and the scheduling
/// loop over it.
///
/// Created inside the spawned thread and dropped when that thread ends.
pub(crate) struct WorkerThread {
    /// Everything the executor shares with its workers.
    pub(crate) shared: Arc<Shared>,
    /// This worker's id, the same as `self.worker().id`, kept here so that the
    /// hot path indexes the worker array instead of chasing it.
    pub(crate) id: usize,
    /// The owning end of this worker's queue: push and pop happen here, and
    /// nowhere else.
    pub(crate) queue: WorkerQueue<Job>,
    /// The queue this worker tries to steal from first.
    ///
    /// Upstream's `_sticky_victim`, and the reason the steal loop is not purely
    /// random: a worker that stole from somebody once is likely to again, and
    /// starting there turns a successful steal into a cache-warm one.
    pub(crate) sticky_victim: usize,
    /// This worker's victim-picking generator. Per-worker, because a shared one
    /// would be a contended cache line on the hottest path there is.
    pub(crate) rng: Xorshift32,
}

/// What one round of stealing came back with.
///
/// Upstream's `_explore_task` returns a `bool` and writes the stolen task
/// through a reference, which leaves "returned true and the task is still null"
/// as a third case the caller has to remember to handle. Naming the three
/// outcomes is the same control flow with the case list written down.
pub(crate) enum Stolen {
    /// A job to run.
    Job(Job),
    /// Nothing was found. The caller should consider sleeping.
    Nothing,
    /// The executor has asked this worker to stop.
    Shutdown,
}

impl WorkerThread {
    /// Builds the thread-side state for worker `id`. Call it from the worker's
    /// own thread: the generator is seeded from the calling thread's identity.
    pub(crate) fn new(shared: Arc<Shared>, id: usize, queue: WorkerQueue<Job>) -> Self {
        Self {
            shared,
            id,
            queue,
            // Upstream starts every worker on its own queue. It is empty at
            // that point, so the first steal is wasted; what it buys is that a
            // worker which is handed work directly finds it without a draw.
            sticky_victim: id,
            rng: Xorshift32::new(seed_from_current_thread()),
        }
    }

    /// Runs this worker to completion: prologue, scheduling loop, epilogue.
    ///
    /// This is the body of upstream's `_spawn` lambda. The one addition is the
    /// wait on the start gate, which the parent module explains.
    pub(crate) fn run(mut self, interface: Option<&dyn WorkerInterface>) {
        self.shared.start.wait();

        if let Some(interface) = interface {
            interface.scheduler_prologue(self.worker());
        }

        // Upstream wraps the loop in `try { } catch(...)` and hands the
        // exception to the epilogue. A panic is the equivalent here, and
        // `AssertUnwindSafe` is the honest annotation: the state a panicking
        // task could leave half-written is this worker's own, and this worker
        // is about to stop touching it.
        let panicked = panic::catch_unwind(AssertUnwindSafe(|| self.schedule())).err();

        if let Some(interface) = interface {
            interface.scheduler_epilogue(self.worker(), panicked);
        }
        // Without an interface the payload is dropped here, which is upstream's
        // behaviour too: it stores the exception pointer and, with no interface
        // to hand it to, lets it go. The default panic hook has already printed
        // it by this point.
    }

    /// The work-stealing loop: drain the local queue, then steal or sleep until
    /// there is something to drain again. Upstream's `while(1)` over
    /// `_exploit_task` and `_wait_for_task`, which breaks when the second one
    /// says the worker is done.
    fn schedule(&mut self) {
        let mut job = None;
        loop {
            self.exploit_task(job.take());
            match self.wait_for_task() {
                Some(next) => job = Some(next),
                None => break,
            }
        }
    }

    /// Runs `job`, then drains this worker's own queue.
    ///
    /// Upstream's `_exploit_task`. Local work comes first and in LIFO order:
    /// the job pushed most recently is the one most likely to still be in
    /// cache, and taking it keeps the queue's older, coarser work available for
    /// thieves.
    fn exploit_task(&mut self, first: Option<Job>) {
        let mut job = first;
        while let Some(next) = job {
            self.invoke(next);
            job = self.queue.pop();
        }
    }

    /// Runs one job.
    ///
    /// Upstream's `_invoke` runs the node's work and schedules its successors.
    /// Nothing can enqueue a [`Job`] yet -- there is no `run` and no graph
    /// behind it until the topology lands -- so the body of this is the one
    /// part of the scheduling loop that is still a stub rather than a
    /// translation.
    fn invoke(&mut self, _job: Job) {}

    /// Steals until something turns up, the worker is told to stop, or the
    /// attempts run out. Upstream's `_explore_task`.
    pub(crate) fn explore_task(&mut self) -> Stolen {
        // Fast path, upstream's: with no topology running, every queue is empty
        // by the executor's own invariant, so there is nothing to find and the
        // caller should go to sleep. Relaxed is enough because this is a hint
        // -- a topology that starts between this load and the sleep is caught
        // by the notifier's two-phase guard in `wait_for_task`.
        if self.shared.num_topologies() == 0 {
            return Stolen::Nothing;
        }

        let max_victim = self.shared.num_queues();
        // Upstream's `MAX_STEALS`: keep trying for a couple of passes over the
        // queues before admitting that there is nothing to steal.
        let max_steals = (max_victim + 1) * 2;

        let mut num_steals = 0;
        let mut victim = self.sticky_victim;

        loop {
            if let Some(job) = self.steal_from(victim) {
                self.sticky_victim = victim;
                return Stolen::Job(job);
            }

            // Empty: draw a new victim, never ourselves -- our own queue is the
            // one we just drained, so it is the least likely to hold anything.
            let draw = usize::try_from(self.rng.next_u32())
                .expect("a u32 fits in a usize on any target this crate supports");
            victim = victim_excluding(draw, self.id, max_victim);

            num_steals += 1;
            if num_steals > max_steals {
                // Still nothing after a couple of passes: stop burning a core
                // on it. Upstream yields on every attempt past that point and
                // gives up 150 attempts later, by which time the notifier is
                // the cheaper place to wait.
                thread::yield_now();
                if num_steals > 150 + max_steals {
                    return Stolen::Nothing;
                }
            }

            if self.worker().is_done() {
                return Stolen::Shutdown;
            }
        }
    }

    /// Sleeps until there is work, or until the executor shuts this worker
    /// down. Upstream's `_wait_for_task`, whose `goto explore_task` is this
    /// loop.
    ///
    /// Returns the job to run next, or `None` when the worker should leave the
    /// scheduling loop.
    pub(crate) fn wait_for_task(&mut self) -> Option<Job> {
        loop {
            match self.explore_task() {
                Stolen::Job(job) => return Some(job),
                Stolen::Shutdown => return None,
                Stolen::Nothing => {}
            }

            // Everything below is inside the notifier's two-phase guard: the
            // predicate is "some queue is non-empty, or we are done", and
            // `prepare_wait` has to come first so that a notification which
            // arrives while we are checking cannot be lost. Each check that
            // finds work must therefore cancel the wait before it leaves.
            self.shared.notifier.prepare_wait(self.id);

            // Fast path, upstream's: with no topology running there is nothing
            // to scan for, so skip both O(N) sweeps and sleep.
            if self.shared.num_topologies() == 0 {
                if self.worker().is_done() {
                    self.shared.notifier.cancel_wait(self.id);
                    return None;
                }
                self.shared.notifier.commit_wait(self.id);
                continue;
            }

            // Condition #1: the shared buffers must be empty. They are checked
            // first because a job in a buffer was pushed by a thread that has
            // no queue of its own, and nobody else is going to come looking.
            //
            // Upstream walks the buffers one at a time and points the sticky
            // victim at the one it found. `Buffers` answers emptiness over all
            // of them at once and does not hand out the individual queues, so
            // the sticky victim goes to the first buffer and the draw in
            // `explore_task` finds the rest: a steal attempt wasted on the cold
            // path, against a per-buffer accessor that nothing else wants yet.
            if !self.shared.buffers.is_empty() {
                self.shared.notifier.cancel_wait(self.id);
                self.sticky_victim = self.shared.workers.len();
                continue;
            }

            // Condition #2: every other worker's queue must be empty. Our own
            // is skipped, as upstream skips it: we drained it on the way here,
            // and anything pushed to it since was pushed by us.
            if let Some(victim) = self.first_nonempty_sibling() {
                self.shared.notifier.cancel_wait(self.id);
                self.sticky_victim = victim;
                continue;
            }

            // Condition #3: we must still be wanted.
            if self.worker().is_done() {
                self.shared.notifier.cancel_wait(self.id);
                return None;
            }

            self.shared.notifier.commit_wait(self.id);
        }
    }

    /// Steals from queue `victim`, which is a worker queue below
    /// `num_workers` and a shared buffer above it.
    ///
    /// The two kinds of queue being one index space is upstream's arrangement:
    /// it is what lets a victim be drawn with a single remainder.
    fn steal_from(&self, victim: usize) -> Option<Job> {
        let num_workers = self.shared.workers.len();
        if victim < num_workers {
            self.shared.workers[victim].thief.steal()
        } else {
            self.shared.buffers.steal_from(victim - num_workers)
        }
    }

    /// Returns the index of a sibling worker whose queue is not empty.
    ///
    /// Indexed rather than iterated, as upstream is, and for a reason upstream
    /// states: its `_spawn` is still writing other workers' fields while this
    /// runs. That race is gone here -- every [`Worker`] is fully built before
    /// any thread starts -- but the mapping `k -> k + (k >= id)` is still the
    /// cheapest way to walk every worker but oneself.
    fn first_nonempty_sibling(&self) -> Option<usize> {
        let workers = &self.shared.workers;
        (0..workers.len() - 1)
            .map(|k| k + usize::from(k >= self.id))
            .find(|&victim| !workers[victim].is_queue_empty())
    }

    /// This worker's shared record.
    fn worker(&self) -> &Worker {
        &self.shared.workers[self.id]
    }
}

/// Maps a raw draw onto the queues other than `id`.
///
/// Upstream's trick, in two lines: a remainder over one fewer queue than there
/// are, then a shift past the hole left by `id`. Every victim but `id` is
/// equally likely, and no branch is needed to reject a self-steal.
fn victim_excluding(draw: usize, id: usize, max_victim: usize) -> usize {
    debug_assert!(
        max_victim >= 2,
        "an executor always has at least one worker queue and one buffer"
    );
    debug_assert!(id < max_victim - 1, "a worker id is below the queue count");

    let victim = draw % (max_victim - 1);
    victim + usize::from(victim >= id)
}

/// A seed for one worker's generator, drawn from the calling thread's identity.
///
/// Upstream seeds from `std::hash<std::thread::id>`, which gives every worker
/// of every executor in the process a different stream without a shared
/// counter. The two halves of the hash are folded together rather than one of
/// them being dropped: a `ThreadId` is a small counter, so its hash can carry
/// what little entropy it has in either half.
fn seed_from_current_thread() -> u32 {
    let mut hasher = DefaultHasher::new();
    thread::current().id().hash(&mut hasher);
    let hash = hasher.finish();
    let folded = (hash >> 32) ^ (hash & u64::from(u32::MAX));
    u32::try_from(folded).expect("the fold of two 32-bit halves is a 32-bit value")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The victim map has to be a bijection onto `0..max_victim` minus `id`,
    /// which is small enough to check exhaustively: a worker that can steal
    /// from itself wastes attempts, and one that can draw an out-of-range
    /// victim indexes past the worker array.
    #[test]
    fn a_drawn_victim_is_never_the_worker_itself() {
        for max_victim in 2..=20 {
            for id in 0..max_victim - 1 {
                let mut hit = vec![false; max_victim];
                for draw in 0..max_victim - 1 {
                    let victim = victim_excluding(draw, id, max_victim);
                    assert_ne!(victim, id, "worker {id} drew itself");
                    assert!(victim < max_victim, "worker {id} drew {victim}");
                    assert!(!hit[victim], "worker {id} drew {victim} twice");
                    hit[victim] = true;
                }

                // One draw per victim, so every victim but `id` must be hit
                // exactly once: a draw that cannot reach some queue would leave
                // that queue's work for the sticky victim alone to find.
                for (victim, &hit) in hit.iter().enumerate() {
                    assert_eq!(hit, victim != id, "victim {victim}, worker {id}");
                }
            }
        }
    }

    /// A draw far larger than the queue count still lands in range -- the
    /// generator knows nothing about how many queues there are.
    #[test]
    fn a_large_draw_still_lands_in_range() {
        for draw in [u32::MAX, u32::MAX - 1, 1 << 31, 12345] {
            let draw = usize::try_from(draw).expect("a u32 fits in a usize");
            let victim = victim_excluding(draw, 2, 5);
            assert_ne!(victim, 2);
            assert!(victim < 5);
        }
    }

    /// Two threads must not share a stream, or two workers would walk the same
    /// victims in the same order and contend on every attempt.
    #[test]
    fn two_threads_seed_differently() {
        let mine = seed_from_current_thread();
        let theirs = thread::spawn(seed_from_current_thread)
            .join()
            .expect("the seeding thread panicked");
        assert_ne!(mine, theirs);
    }
}

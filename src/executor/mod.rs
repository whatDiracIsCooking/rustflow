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
//! - [`worker`] a worker and the scheduling loop it runs;
//! - `topology` one run of one taskflow, the job a queue carries, the parent
//!   that job's graph was scheduled under, the walk that decides which anchor
//!   a failure is delivered to, and the run's cancellation bits --
//!   the one module here that is crate-private, since a caller has no
//!   business with a run's internals. It is also where the ownership
//!   argument lives: a job owns a share of its run, of the taskflow's
//!   storage and of its parent chain, which is what keeps a queue of jobs
//!   from being a queue of raw pointers;
//! - `async_node` the free-standing node an async task is -- a one-node
//!   graph of its own, which is what lets a node that belongs to no taskflow
//!   and no run be dispatched by the same code as every other node -- and
//!   `async` and `dependent_async` the two node kinds that run on one: the
//!   submitting calls, the two counters that make a free one something
//!   [`Executor::wait_for_all`] and [`Drop`] can wait for, and the protocol
//!   that builds a graph of them one task at a time while the tasks already
//!   submitted run;
//! - `runtime_async` the same submissions from a [`Runtime`](crate::Runtime)
//!   body, which is where a free node acquires a *parent* -- so its children
//!   are counted against the node that spawned them rather than against the
//!   executor, and go onto the queue of the worker running the body;
//! - `module`, `subflow` and `runtime` one dynamic-tasking node kind each,
//!   reached by one line from the worker's handle match. `module` runs a
//!   composed taskflow as of #49 and an adopted graph as of #74, `runtime` a
//!   runtime task as of #51 -- and, as of #90, the free node whose body is
//!   handed a `Runtime` too, since everything that hands a body one belongs
//!   together -- and `subflow` a subflow task as of #50. They are
//!   separate files so that the three could land without meeting in the
//!   worker, which is what they did;
//! - [`run_handle`] the `run` family itself -- `run`, `run_n`, `run_until` and
//!   the owning half of each -- the tear-down that decides whether a finished
//!   run goes round again, the handle whose borrow is this port's answer to
//!   upstream's "do not mutate a running taskflow", what that answer does and
//!   does not promise if the handle is leaked, and the
//!   [`Canceller`](crate::Canceller) that cancels a run from outside it.
//!
//! What is left here is what the *executor* owns: the worker array, the
//! buffers, the notifier, the thread-id map, and the lifecycle -- spawn the
//! workers, hand them the shared state, join them on drop. See each submodule
//! for what it does and does not owe to upstream.
//!
//! The scheduler itself contains no `unsafe` at all, and that is a property
//! to keep rather than a coincidence to notice: its one `unsafe` boundary
//! was a job holding raw pointers at the run and at the taskflow's storage,
//! and it went when a job started owning what it names instead -- the
//! README's "A run owns the graph it walks", and `topology`'s own argument.
//! What is left is [`notifier`]'s lock-free waiter stack, where a worker
//! about to park publishes its [`Thread`](std::thread::Thread) handle into a
//! slot that only the notifier which has popped it may read. That is the
//! whole of the crate's `unsafe`, and `tests/loom_notifier.rs` is what
//! models the invariant it rests on. A node kind added here that would need
//! a raw pointer to reach something is a design to reconsider, not a
//! `// SAFETY:` comment to write.
//!
//! # Cancelling a run
//!
//! [`RunHandle::cancel`](crate::RunHandle::cancel) and
//! [`Canceller::cancel`](crate::Canceller::cancel) are upstream's
//! `tf::Future::cancel`, and the whole of what they promise is that *the
//! executor stops scheduling tasks of that run onwards*. Cancellation is not
//! preemptive: a task already running is left to finish, and nothing
//! interrupts, polls or asks it to stop early. The three pieces are spread
//! over three modules on purpose, since each belongs with the state it reads:
//!
//! - cancelling sets a bit in the run's estate word -- upstream's
//!   `ESTATE::CANCELLED`, which lives in `topology` in an allocation a
//!   lifetime-free token can hold a `Weak` to;
//! - a job of a cancelled run is skipped at the top of
//!   `WorkerThread::invoke` in [`worker`], which is upstream's
//!   `_is_parent_cancelled` early-out, and torn down without being run so that
//!   the run's join counters still reach zero;
//! - a cancelled run does not go round again, which is the cancellation half
//!   of upstream's `!tpg->cancelled() && !tpg->_predicate()` in the tear-down
//!   in [`run_handle`].
//!
//! So a cancelled run *completes*, normally and promptly: its callback fires,
//! the next queued run of its taskflow starts, and a
//! [`wait`](crate::RunHandle::wait) on it returns `Ok(())` exactly as
//! upstream's `fu.get()` does not throw. What it does not do is run the work
//! the pool had not got to yet. How much that is depends on the race between
//! the call and the pool, which is why upstream's own documentation says "we
//! may still see some tasks complete their executions or none".
//!
//! "Promptly" has exactly one exception, which is upstream's too and is set
//! out under "Semaphores" below: a task already *parked* on a semaphore is
//! not a task the pool has yet to get to, and a cancelled run with one of
//! those does not finish until somebody releases that semaphore.
//!
//! A panicking task is the second way a run gets cancelled, and it reuses all
//! three pieces: upstream's `_process_exception` sets `ESTATE::EXCEPTION`
//! rather than `CANCELLED`, and the early-out and the re-run decision both
//! read the two bits as one. So a panic cancels the rest of its own run on
//! exactly the terms above -- tasks already running finish, tasks not yet
//! scheduled do not run -- and the one thing it adds is the payload, which
//! [`wait`](crate::RunHandle::wait) hands back as
//! [`RunError::Panicked`](crate::RunError::Panicked) where a cancelled run
//! gets `Ok(())`. Only the first panicking task of a run is reported;
//! `ESTATE::CAUGHT`, set by the same atomic, is what drops the rest.
//!
//! # Anchors: who a failure is reported to
//!
//! "A panic cancels the rest of its own run" is the claim above that a
//! nested graph makes more precise, because a failure inside one is not the
//! whole run's business: somebody may be waiting for exactly that graph, and
//! if so the failure is *theirs*. The thing that is waiting is an **anchor**,
//! and `Job::record_panic` in `topology` is upstream's `_process_exception`:
//! a walk from the failing node up the parent chain, marking `EXCEPTION` on
//! every base it passes so that each nested graph the failure is inside of
//! stops starting work, and stopping at the first anchor it finds. There are
//! three kinds, and they are tried in this order:
//!
//! 1. an **explicit** anchor -- upstream's `ESTATE::EXPLICITLY_ANCHORED` --
//!    is a frame that is blocked on this graph right now: an
//!    [`Executor::corun`], a subflow's `join` (#50), a runtime task's own
//!    `corun` (#51) or a [`TaskGroup::corun`](crate::TaskGroup) (#93). It is
//!    a bit held by a guard for
//!    exactly as long as that frame waits, so a walk that reaches it hands
//!    the payload to a thread that is about to look. Whoever takes it decides
//!    what to do: `corun` re-raises it on its own worker,
//!    [`Executor::try_corun`] returns it. One kind of node carries the bit
//!    without a frame behind it: a free node whose runtime-shaped body hands
//!    back an [`AsyncHandle`](crate::AsyncHandle) is anchored for its whole
//!    life (#90, #92), because the waiter is whoever holds that handle and
//!    the node is what answers to it;
//! 2. an **implicit** anchor -- upstream's `NSTATE::IMPLICITLY_ANCHORED` --
//!    is a node that preempted itself and means to look at its payload slot
//!    when it is resumed. The *innermost* one the walk saw gets the payload,
//!    because that is the one closest to the failure. #51's runtime tasks set
//!    the bit, as does #90's free node with a runtime-shaped body -- they
//!    share the entry that sets it -- and a
//!    [`TaskGroup`](crate::TaskGroup)'s anchor carries it from the moment the
//!    group is opened (#93), which is what keeps a silent child's failure for
//!    the next `corun` to report. A preempted subflow node does not set it,
//!    which is upstream's arrangement too;
//! 3. the **run**, which is every chain's root and is anchored by definition
//!    -- upstream constructs its topology `EXPLICITLY_ANCHORED` to say so.
//!    Its payload slot is the completion slot, so this is the case where a
//!    `wait` reports the failure, and it is the only case a static graph has.
//!
//! A node whose nested graph merely *preempted* it is no anchor at all, which
//! is upstream's arrangement rather than something left out: the children of
//! a module task (#49) and of a subflow left to its implicit join (#50)
//! report straight past it to the run, so a failure in one of those surfaces
//! from `wait` like any other failure of that run. What the walk does for
//! such a node is mark it, so the siblings of the failing job are skipped.
//!
//! Whichever anchor is found, one `fetch_or` of `EXCEPTION | CAUGHT` on it
//! decides whether this payload is the first and therefore the one kept --
//! the same rule a run has always applied, now applied wherever the failure
//! is delivered. A payload that loses is dropped, at a `NodeBase` anchor as
//! much as at a run; `Job::record_panic` is where that parts company with
//! upstream, and says why.
//!
//! # Parents, and what a nested graph counts down
//!
//! Every graph the executor schedules is scheduled *under* something, and
//! that something is its **parent**: the thing whose join counter the jobs of
//! that graph decrement as they finish, so that the last of them to reach
//! zero is what the graph being over means. There are two kinds, upstream's
//! `NodeBase*` either way:
//!
//! - **the run**, which is the parent of every node of the taskflow's own
//!   graph. Its counter reaching zero ends the run -- the callback fires, the
//!   next queued run of the taskflow starts, and whoever is waiting is
//!   released;
//! - **something nested inside the run**: a node whose own graph is running
//!   -- a subflow's subgraph, a module's target graph -- or the anchor a
//!   [`corun`](Executor::corun) schedules a whole taskflow under. Its counter
//!   reaching zero means that *nested* graph is over, which ends nothing: it
//!   either resumes the node that was waiting for it or releases the frame
//!   that is watching the counter. A node whose own children are *async
//!   tasks* rather than a graph is the same thing counted the same way (#92),
//!   and a [`TaskGroup`](crate::TaskGroup)'s anchor (#93) is a parent of this
//!   kind with no run around it at all -- a shared `NodeBase` the group's
//!   children count down and a `corun` reads.
//!
//! The parent is carried by the job rather than stored on the node, which is
//! what this port has instead of upstream's `node->_parent`, and
//! `executor::topology` has the argument for why. One consequence is worth
//! naming here: a parent chain is a chain of *jobs*, so the parent of a node
//! is reached as the job that node was dequeued as -- which is exactly what
//! has to be queued again to resume it.
//!
//! # The preemption protocol
//!
//! A node that is the parent of a graph cannot finish when its body returns:
//! its successors must not run until the nested graph has. Upstream's answer,
//! and this port's, is to enter such a node twice, with `NSTATE::PREEMPTED`
//! ([`Node::PREEMPTED`](crate::Node::PREEMPTED)) saying which entry this is:
//!
//! 1. **First entry.** `WorkerThread::invoke` dispatches on the handle, the
//!    arm runs whatever the node does -- a subflow body builds its graph, a
//!    module task has one already -- and then, if there is a graph to run,
//!    sets `PREEMPTED` on the node and schedules that graph *under the node*.
//!    The arm answers "preempted", and the worker returns at once: the node's
//!    own join counter is not re-armed and not one successor is released,
//!    because the node has not finished.
//! 2. **While the children run.** They are ordinary jobs. Each one that
//!    becomes ready is counted against the node, exactly as a top-level
//!    node's successors are counted against the run, so the node's counter
//!    tracks its nested graph the way the run's tracks the whole graph.
//! 3. **Last child.** The tear-down of the last job of the nested graph finds
//!    the node's counter at zero and the `PREEMPTED` bit set, and queues the
//!    node's own job a second time. The bit is read *before* the decrement,
//!    because the instant the counter reaches zero another worker may be
//!    running the node already.
//! 4. **Second entry.** The `PREEMPTED` fast path at the top of `invoke`
//!    skips the cancellation check -- a node that has been entered has to come
//!    back out through the dispatch whatever happened in the meantime -- the
//!    arm clears the bit and answers with an ordinary release, and the node
//!    finishes like any other: counter re-armed, successors released,
//!    tear-down against its own parent.
//!
//! The protocol costs one requeue per nested graph where upstream, whose
//! `_update_cache` is not ported (issue #62), resumes the parent on the
//! worker that finished the last child.
//!
//! All three arms that can preempt a node now do: a module task (#49, #74),
//! a runtime task that spawned work (#51) and a subflow left to its implicit
//! join (#50). A fourth node *kind* joined them with the async family: a free
//! node whose body is handed a `Runtime` (#90) runs the same protocol through
//! `executor::runtime`'s own entry, which is what makes an
//! [`AsyncHandle`](crate::AsyncHandle) on one an answer about the children it
//! spawned (#92) rather than only about the body.
//!
//! # Semaphores: what a node does before and after the dispatch
//!
//! A [`Semaphore`](crate::Semaphore) constrains a *set* of tasks rather than
//! locking one, and the executor's whole part in that is two hooks around
//! `WorkerThread::invoke`'s dispatch -- upstream's `to_acquire` block before
//! its `invoke_task:` label and its `to_release` block after the switch:
//!
//! - **before the dispatch**, on a first entry only, the node acquires every
//!   semaphore it asked for, in order. If one of them has nothing left to
//!   give, the node does not run: the job is parked in that semaphore, which
//!   owns a clone of it, and anything the rollback of the counts already
//!   taken woke is queued. Whoever releases that semaphore schedules the
//!   parked job, and it enters the dispatch again as a first entry;
//! - **after the dispatch**, on the entry that *finishes* the node, it
//!   releases every semaphore it asked to release, and whatever those
//!   releases wake is queued -- before the node's own successors, as
//!   upstream queues them.
//!
//! "On a first entry only" and "on the entry that finishes" are the
//! preemption protocol above read from the semaphores' side: a module,
//! subflow or runtime node acquires once, keeps what it took for the length
//! of its nested graph, and gives it back when the second entry completes
//! it. A node that acquires a semaphore its own nested graph also acquires
//! therefore needs a count for each, which is why upstream's own test of
//! that shape uses a semaphore of two.
//!
//! Parking is the one thing here that is not accounted the way everything
//! else in the scheduler is, and it is sound for a reason worth stating: a
//! parked job is *not* finished, so nothing is counted down. The clone in
//! the semaphore inherits the count the dequeued job was holding against its
//! parent, so the run stays as outstanding as it was and cannot finish while
//! one of its tasks is parked. The jobs a release wakes are queued through
//! the shared buffers rather than kept as this worker's continuation, which
//! `WorkerThread::invoke_one` argues at the hook.
//!
//! A task that releases a semaphore already at its maximum is upstream's
//! `TF_THROW`, and here it is that task's own failure: the payload names the
//! maximum it would have gone past, is delivered to the same anchor a panic
//! in the body would reach, and surfaces from
//! [`wait`](crate::RunHandle::wait) as
//! [`RunError::Panicked`](crate::RunError::Panicked). The semaphore's
//! counter is left alone.
//!
//! ## The hazard cancellation leaves
//!
//! One interaction is upstream's behaviour and is **recorded rather than
//! fixed**: a job parked on a semaphore when its run is cancelled *stays
//! parked*. Nothing sweeps a waiting list, so that job is scheduled only
//! when some other task releases the semaphore -- and until then the run's
//! join counter never reaches zero, so the run does not finish and a `wait`
//! on it does not return. A parked task of a cancelled run is therefore not
//! like a queued one: the queued one is dequeued and torn down by the
//! early-out at the top of the dispatch, which is what lets a cancelled run
//! complete promptly, while the parked one has nobody to dequeue it.
//!
//! The order of the two hooks is what bounds this: the cancellation check
//! comes *before* the acquire, so a job that reaches the dispatch after the
//! cancellation tears down without acquiring anything. What is left is the
//! race -- a cancellation between the check and the acquire -- and the case
//! where a task that was going to do the releasing is itself one of the
//! tasks the cancellation skipped. Both are upstream's, so cancelling a run
//! whose tasks acquire a semaphore nothing outside that run will release is
//! a shape to avoid rather than one the executor rescues.
//! [`Semaphore::reset`](crate::Semaphore::reset) is not the rescue either:
//! it drops the waiting list, which unschedules those tasks for good.
//!
//! # Why cancellation reads the whole chain
//!
//! The early-out at the top of `invoke` is upstream's
//! `Node::_is_parent_cancelled`, and with parents it has two terms rather
//! than one: the run's own cancellation bits, and the estate of every parent
//! above this job. The second term is what stops the children of a failed
//! subflow from running when the run as a whole is still going -- a failure
//! inside a nested graph is the nested graph's, and the node that owns it is
//! where it is recorded.
//!
//! Upstream reads only the immediate parent, because its `_process_exception`
//! walks the chain *upwards* and marks every ancestor as it records the
//! failure -- which the walk above now does too, so one level would be enough
//! here as well. Reading every level is kept because it costs one relaxed
//! load per level of nesting and answers strictly more: a job whose own
//! parent is unmarked can still have a marked *grandparent*, when the failure
//! was in a sibling subtree under the same anchor, and every job under a
//! failed anchor is one whose result nobody will collect.

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
/// [`Executor::run`] is what puts a taskflow on it; dropping the executor
/// shuts the pool down and joins every thread, which is upstream's
/// destructor.
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
    /// How many topologies are running, i.e. upstream's `_num_topologies`:
    /// runs, plus the free async tasks `executor::async` counts in here as
    /// upstream's own `async` and `silent_async` do.
    ///
    /// Read on the hot path: a worker with nothing to steal checks it before
    /// it sweeps the queues, because zero means every queue is empty by the
    /// executor's own invariant. Padded separately from the notifier because
    /// upstream pads it separately, with a comment that says the false
    /// sharing between the two "can cause serious performance drop".
    pub(crate) num_topologies: CachePadded<AtomicUsize>,
    /// How many *free async tasks* are queued or running: the half of
    /// `num_topologies` that [`Drop`] waits for.
    ///
    /// Upstream has no second counter and needs none: its destructor calls
    /// `wait_for_all`, which waits for every kind of work at once, and a
    /// detached `tf::Future` is as much its problem as a submitted
    /// `silent_async`. This port can tell the two apart and must: a run in
    /// flight is a run whose [`RunHandle`](crate::RunHandle) borrows the
    /// executor, so the only way to reach `Drop` with one outstanding is to
    /// have *leaked* that handle -- and the documented answer to a leaked
    /// handle is that the pool still comes down. An async task has no handle
    /// that borrows anything, so nothing but this can keep the pool alive
    /// long enough to run it. `executor::async`, which owns the counting,
    /// has the decision in full.
    ///
    /// Padded for `num_topologies`' reason rather than because this one is
    /// read on the hot path -- it is not: it is *written* by every async
    /// submission and tear-down, and sharing a line with the count a worker
    /// checks before every sweep is exactly the false sharing upstream's own
    /// comment warns about.
    pub(crate) num_asyncs: CachePadded<AtomicUsize>,
    /// The lock [`Executor::wait_for_all`] reads `num_topologies` under.
    ///
    /// Upstream waits on the atomic itself, which C++20 can do and Rust
    /// cannot; a mutex and a condvar are the equivalent, and the lock is what
    /// closes the window between a waiter reading a non-zero count and
    /// actually sleeping. Nothing on the scheduling path touches it -- only a
    /// run that has just finished and a thread that asked to wait.
    pub(crate) topologies: Mutex<()>,
    /// Signalled when `num_topologies` falls to zero.
    pub(crate) no_topologies: Condvar,
    /// The lock [`Drop`] reads `num_asyncs` under, and the condvar it is
    /// signalled on. A pair of its own rather than the one above, so that a
    /// run finishing does not wake a shutdown that is waiting for async
    /// tasks and vice versa; `executor::async` holds both ends of it.
    pub(crate) asyncs: Mutex<()>,
    /// Signalled when `num_asyncs` falls to zero.
    pub(crate) no_asyncs: Condvar,
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

    /// Counts one more run -- or one more free async task -- as in flight.
    /// Upstream's `_increment_topology`.
    pub(crate) fn increment_topology(&self) {
        self.num_topologies.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one run -- or one free async task -- as finished, waking
    /// [`Executor::wait_for_all`] if that was the last. Upstream's
    /// `_decrement_topology`.
    pub(crate) fn decrement_topology(&self) {
        if self.num_topologies.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Under the lock: a waiter that has read a non-zero count but not
            // yet reached `wait` still holds it, so taking it here is what
            // stops this notification from arriving before the waiter is
            // listening and being lost.
            let _idle = self
                .topologies
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.no_topologies.notify_all();
        }
    }

    /// Queues `jobs` from a thread that has no queue of its own.
    ///
    /// Upstream's `_bulk_schedule(I first, size_t)`: everything goes to the
    /// shared buffers, and the notifier is told how many workers are worth
    /// waking. A worker scheduling its own successors uses
    /// [`WorkerThread::schedule`](crate::executor::worker::WorkerThread) and
    /// its own queue instead.
    ///
    /// The batch is dealt round-robin by [`Buffers::push_bulk`] rather than
    /// hashed item by item, which is upstream's `_bulk_spill_round_robin`
    /// rather than its `_bulk_spill`; that method records what #60 measured,
    /// and `BENCHMARKS.md` records what remains. For a graph of 65536 sources
    /// this call *is* the run's critical path -- the producer materialises and
    /// pushes every job while 24 workers wait on it -- so the per-item cost
    /// here is worth more than it looks.
    pub(crate) fn bulk_schedule(&self, jobs: impl IntoIterator<Item = Job>) {
        let num_jobs = self.buffers.push_bulk(jobs);

        // Notified after the pushes, as upstream notifies: a worker woken
        // before its work is visible would sweep the queues, find nothing and
        // go back to sleep. Upstream's commented-out experiment in
        // `_bulk_schedule` is exactly this trade, and it records that waking
        // first bought nothing. #60 measured it here too, on 65536 sources and
        // 24 workers, both as upstream wrote it and as a `notify_all` every
        // 4096 pushes: either way the run took close to twice as long, because
        // the woken workers sweep the queues and the cache lines they sweep are
        // the ones the producer is still pushing into.
        if num_jobs > 0 {
            self.notifier.notify_n(num_jobs);
        }
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
        Self::spawn_pool(num_workers, None)
    }

    /// Creates an executor with `num_workers` worker threads, calling
    /// `interface` on each of them as it enters and leaves the scheduling loop.
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

    /// The number of queues a worker can steal from: one per worker plus the
    /// shared buffers. Upstream's `num_queues`.
    #[must_use]
    pub fn num_queues(&self) -> usize {
        self.shared.num_queues()
    }

    /// How many workers the notifier is accounting for right now.
    ///
    /// Upstream's `num_waiters`, and worth exactly what
    /// [`Notifier::num_waiters`] says it is worth: which workers a notifier
    /// counts is the notifier's own business, and the two in this crate count
    /// different things, as upstream's two do. So this is a diagnostic, never
    /// a predicate -- "the pool is idle" is [`Executor::num_topologies`], and
    /// deciding whether to sleep is the notifier's two-phase guard.
    #[must_use]
    pub fn num_waiters(&self) -> usize {
        self.shared.notifier.num_waiters()
    }

    /// How many topologies are running. Upstream's `num_topologies`.
    ///
    /// One per [`RunHandle`](crate::RunHandle) whose run has not finished,
    /// plus one per *free* async task queued or running -- upstream counts
    /// those here too, and `executor::async` has why (#86, #88). A child of a
    /// [`Runtime`](crate::Runtime) body or of a
    /// [`TaskGroup`](crate::TaskGroup) is in neither count, because the
    /// parent waiting for it is what accounts for it (#115).
    ///
    /// This is the count [`Executor::wait_for_all`] blocks on reaching zero.
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
    ///
    /// `spawn_pool` rather than `spawn`, which is what it was called until
    /// #88: [`Executor::spawn`] is now the public submission of an async
    /// task, upstream's `async` under the name Rust leaves available for it.
    fn spawn_pool(num_workers: usize, interface: Option<Arc<dyn WorkerInterface>>) -> Self {
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
    /// Upstream's `_shutdown`: wait for the work that has nobody else to wait
    /// for it, tell every worker to stop, wake the ones that are asleep, and
    /// join them all.
    ///
    /// Upstream's first line is `wait_for_all`, so that nothing still running
    /// is left without a pool. This waits for **free async tasks only** --
    /// `num_asyncs` rather than `num_topologies` -- and the split is the one
    /// `executor::async` argues for:
    ///
    /// - an async task submitted with [`Executor::spawn`] or
    ///   [`Executor::silent_async`] is waited for, because nothing else can
    ///   be. Its handle is `'static` and dropping one detaches, and a
    ///   `silent_async` has no handle at all, so without this line a queued
    ///   body would simply be dropped unrun -- which is the one thing the
    ///   submitting call promises not to do;
    /// - a **run** is not waited for, and that keeps the documented behaviour
    ///   of a leaked [`RunHandle`](crate::RunHandle) true. A handle that was
    ///   dropped has already blocked until its run was over, so a run in
    ///   flight here is one whose handle was leaked: the workers stop where
    ///   they are, the jobs still queued are dropped, and the run never
    ///   finishes -- which nothing is waiting for, the thing that would have
    ///   waited being the handle that was leaked. See [`run_handle`] for the
    ///   rest of what a leaked handle does. It is also what keeps a worker
    ///   that died of a bug from turning shutdown into a hang.
    ///
    /// One foot-gun comes with the wait, and it is upstream's too: dropping
    /// the last share of an executor *from inside one of its own async
    /// tasks* deadlocks, because the task doing the dropping is one the wait
    /// is waiting for. Upstream's destructor has the same property for the
    /// same reason.
    fn drop(&mut self) {
        // Upstream's `wait_for_all()`, narrowed to the count above for the
        // reason the doc comment gives. Before the `done` flags, not after: a
        // worker told to stop leaves its loop with whatever is still queued
        // unrun, so the flags cannot be set while an async task is waiting
        // for a worker to pick it up.
        self.shared.wait_for_asyncs();

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

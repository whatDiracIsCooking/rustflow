//! Translated from `taskflow/core/worker.hpp` (Taskflow v4.1.0).
//!
//! The scheduling loop itself -- upstream's `Executor::_spawn`,
//! `_exploit_task`, `_explore_task`, `_wait_for_task`, `_schedule` and
//! `_invoke`, from `taskflow/core/executor.hpp` of the same version -- is
//! translated here too, rather than into the parent module, because every line
//! of it either reads or writes one worker's own state or runs on that worker's
//! thread. The parent module keeps what the *executor* owns: the worker array,
//! the buffers, the notifier and the thread-id map. `_tear_down_topology`,
//! which a worker calls but which is all about the run, lives in
//! [`run_handle`](crate::executor::run_handle) with the rest of the run family.
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
//! - `WorkerThread` is the part only the worker's own thread touches: the
//!   owning end of its queue, its sticky victim and its generator. It is
//!   created inside the spawned thread and moves nowhere, so the "only the
//!   owner pushes or pops" rule that upstream writes in a comment is here a
//!   property of where the value lives.
//!
//! That split is also why a `WorkerThread` does not always hold the owning
//! end of its queue. A corun loop -- `WorkerThread::corun_until`, the loop
//! `Executor::corun` runs -- starts from *inside* a task body, which is
//! inside the `WorkerThread` frame already running that worker's loop, and a
//! frame cannot be named from the code it called. So a corun loop gets a
//! second `WorkerThread` over the same worker, built by
//! `WorkerThread::corun_view`, and reaches the queue the only way a value
//! without the owning end can: see `LocalQueue` below.
//!
//! The one upstream field with no equivalent is `_thread`: a `JoinHandle` is
//! owned by whoever will join it, so the executor keeps the handles and the
//! worker does not. That costs the port upstream's `WorkerInterface` trick of
//! setting thread affinity from the prologue, which needs the handle; a
//! `Worker` here cannot hand one out.
//!
//! # Conditional scheduling
//!
//! A conditioner -- [`Handle::Condition`] or [`Handle::MultiCondition`] -- is
//! the one node kind that picks its own successors, and that single fact is
//! what the strong/weak dependency split exists for. Upstream's `_invoke` has
//! two switches over the handle because of it, and so does
//! `WorkerThread::invoke`: the first runs the work and keeps what a
//! conditioner returned, the second decides what this node's successors are
//! owed.
//!
//! - An edge out of a *non*-conditioner is a **strong** dependency. It is
//!   counted into the successor's join counter by `set_up_graph` in the
//!   crate-private `executor::topology`, and finishing the predecessor
//!   decrements that counter; the successor becomes ready when the count
//!   reaches zero, which is when every strong predecessor has run.
//! - An edge out of a conditioner is a **weak** dependency. It is not counted,
//!   so it gates nothing. The conditioner's return value instead names
//!   successor *positions* -- one `i32` for a condition task, any number of
//!   them for a multi-condition one -- and each named successor is scheduled
//!   outright. An index that is negative or past the last successor names
//!   nothing, which is how a branch ends, and so does a conditioner whose body
//!   panics: the panic is delivered to whatever is waiting for this node's
//!   graph -- the run, for a top-level conditioner -- and the selection stays
//!   empty. A panic also cancels the rest of that graph, so for a conditioner
//!   the empty selection is belt and braces: there is nothing left to select
//!   into.
//!
//! A selected successor has its join counter *stored to zero* rather than
//! decremented. That is upstream's "zeroing the join counter for invariant",
//! and the invariant is that a node's counter reads zero exactly when the node
//! is ready to run. A node entered through a weak edge cannot get there by
//! arithmetic: a cycle may re-enter it before its strong predecessors run
//! again -- in upstream's `loop_cond` shape the conditioner's weak successor is
//! the conditioner itself, re-armed to its strong count a few lines earlier by
//! the `fetch_add` that makes cycles work at all -- and nothing else is going
//! to decrement it. So the counter is told.
//!
//! That is also where upstream's one rule for writing a conditional graph comes
//! from (its `ConditionalTasking` page): **a task reached through a condition
//! must be reachable only through that condition.** A weak successor that also
//! has a strong predecessor can be started by the conditioner before that
//! predecessor has run, since the zeroed counter says "ready" either way; and a
//! weak successor two conditioners can both select may be started twice at
//! once, which [`Work::invoke`](crate::Work::invoke) reports as a violated
//! one-worker-per-node promise. Neither is diagnosed when the graph is built,
//! and upstream does not diagnose them either.
//!
//! None of this state outlives a run. `run_n`/`run_until` re-arm every node
//! through `set_up_graph` before queueing the next run's sources, so the
//! counters a cycle drove to zero start the next run at their strong counts and
//! two runs of one taskflow cannot see each other's arithmetic.

use std::any::Any;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use super::Shared;
use crate::executor::notifier::Notifier;
use crate::executor::queue::{Thief, WorkerQueue};
use crate::executor::rng::Xorshift32;
use crate::executor::topology::Job;
use crate::executor::{module, runtime, subflow};
use crate::graph::{Handle, Node};

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
    /// Where this loop takes its local work from, and where it puts the work
    /// it makes ready. See [`LocalQueue`].
    pub(crate) queue: LocalQueue,
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

/// Which end of a worker's queue a scheduling loop holds.
///
/// Upstream needs no equivalent: its `_corun_until` takes the `Worker&` the
/// executor hands out and pops that worker's queue through it. Here the owning
/// end of a queue is a `!Sync` value that lives in the [`WorkerThread`] on the
/// worker's own stack frame, and a task body -- which is what calls
/// `Executor::corun` -- is called *by* that frame and cannot name it.
///
/// A corun loop therefore runs on a [`WorkerThread::corun_view`] of the same
/// worker, which has no owning end, and the two accesses it needs are both
/// reachable from the state every thread shares:
///
/// - taking local work becomes a steal from the worker's own [`Thief`] -- the
///   same queue, from the first-in end rather than the last-in one;
/// - publishing newly ready work goes to the shared buffers instead of the
///   queue.
///
/// Both are a little slower than the owned end -- a steal is the contended
/// side of the deque, and a buffer costs the pusher a hash and the taker a
/// sweep -- and neither changes which jobs run or in what order they become
/// ready. The whole trade is locality, and what it buys is that no part of
/// this port needs a raw pointer to another thread's stack frame.
pub(crate) enum LocalQueue {
    /// The owning end, held by the thread running this worker's own scheduling
    /// loop: pops are LIFO and uncontended, and nothing else may touch it.
    Owned(WorkerQueue<Job>),
    /// No owning end, because another [`WorkerThread`] on this thread has it.
    Corun,
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
            queue: LocalQueue::Owned(queue),
            // Upstream starts every worker on its own queue. It is empty at
            // that point, so the first steal is wasted; what it buys is that a
            // worker which is handed work directly finds it without a draw.
            sticky_victim: id,
            rng: Xorshift32::new(seed_from_current_thread()),
        }
    }

    /// A second view of worker `id`, for a corun loop that starts inside one
    /// of that worker's task bodies.
    ///
    /// Call it only from worker `id`'s own thread -- `Executor::corun` checks
    /// that before it gets here. The two views share the worker's queue, but
    /// not the sticky victim and not the generator: those are a `&mut` each,
    /// and the owned one is borrowed by the frame below. Sharing them would
    /// buy a warm first steal and cost the only thing that keeps the two views
    /// apart, which is that only one of them is ever running.
    pub(crate) fn corun_view(shared: Arc<Shared>, id: usize) -> Self {
        // Where `new` starts a worker on its own queue, a corun view starts on
        // the first shared buffer: [`WorkerThread::pop`] has already looked at
        // this worker's own queue by the time a steal is drawn, and the inner
        // run this loop is waiting for was queued through the buffers.
        let sticky_victim = shared.workers.len();
        Self {
            shared,
            id,
            queue: LocalQueue::Corun,
            sticky_victim,
            rng: Xorshift32::new(seed_from_current_thread()),
        }
    }

    /// Takes the next job from this worker's own queue, or `None` if it held
    /// none. Upstream's `w._wsq.pop()`.
    ///
    /// `pub(crate)` for the unit tests in `executor::topology`, which drive
    /// the scheduler one job at a time: what a worker's queue holds after a
    /// job has been invoked is how the preemption protocol is observed.
    pub(crate) fn pop(&self) -> Option<Job> {
        match &self.queue {
            LocalQueue::Owned(queue) => queue.pop(),
            // The same queue through the other end, which is what a view
            // without the owning end can reach. It matters that this covers
            // the worker's own queue at all: with one worker, a job left there
            // by the frame below has nobody else to run it, and a corun loop
            // that could not take it would wait for a job it is sitting on.
            LocalQueue::Corun => self.worker().thief.steal(),
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
        //
        // The stop condition never fires: this worker leaves the loop the way
        // upstream's does, through `_wait_for_task` reporting that the `done`
        // flag is set. `run_until`'s docs name who passes one that does fire.
        let panicked = panic::catch_unwind(AssertUnwindSafe(|| self.run_until(|| false))).err();

        if let Some(interface) = interface {
            interface.scheduler_epilogue(self.worker(), panicked);
        }
        // Without an interface the payload is dropped here, which is upstream's
        // behaviour too: it stores the exception pointer and, with no interface
        // to hand it to, lets it go. The default panic hook has already printed
        // it by this point.
    }

    /// The work-stealing loop: drain the local queue, then steal or sleep until
    /// there is something to drain again, and stop when `stop` says so or when
    /// the executor shuts this worker down.
    ///
    /// Upstream's `while(1)` over `_exploit_task` and `_wait_for_task`, which
    /// breaks when the second one says the worker is done.
    ///
    /// The stop condition is a parameter rather than the `done` flag because
    /// this loop was factored out for `corun`. It turned out not to be the
    /// loop `corun` can use, and the reason is the one sentence worth keeping
    /// from that attempt: [`WorkerThread::wait_for_task`] parks the worker,
    /// and nothing wakes the notifier when a *topology* finishes -- only a
    /// push does -- so a corun loop that reached the sleep path would wait for
    /// a notification that is never sent, having parked the very thread the
    /// run it is waiting for may need. [`WorkerThread::corun_until`] is
    /// therefore its own loop, over [`WorkerThread::explore_task`], and this
    /// one keeps the parameter for the only caller it has: `|| false`, the
    /// worker loop that leaves only on the `done` flag.
    pub(crate) fn run_until(&mut self, mut stop: impl FnMut() -> bool) {
        let mut job = None;
        loop {
            self.exploit_task(job.take(), &mut stop);
            if stop() {
                return;
            }
            match self.wait_for_task() {
                Some(next) => job = Some(next),
                None => return,
            }
        }
    }

    /// The corun loop: keep running jobs -- this worker's own first, then
    /// anybody's -- until `stop` says that whatever this thread is waiting for
    /// has happened.
    ///
    /// Upstream's `_corun_until`, which is `goto exploit`/`goto explore` over
    /// the same two moves this makes. Three things differ from
    /// [`WorkerThread::run_until`], and all three are upstream's too:
    ///
    /// - one local job per round, not a drain: a job run here may itself
    ///   corun, so each one costs stack, and upstream's comment says why it
    ///   takes a single task -- draining "can potentially enter a very deep
    ///   recursive corun, causing stack overflow".
    /// - never sleeps. A worker that parks here is a worker the run it is
    ///   waiting for cannot use, and nothing would wake it: see
    ///   [`WorkerThread::run_until`]. So an empty sweep yields the core
    ///   instead, as upstream's does.
    /// - ignores the `done` flag, as upstream's loop does by never reading it.
    ///   A corun is always nested inside a task body, so the run that task
    ///   belongs to is in flight, so its handle is alive, so the executor
    ///   cannot be dropped and no worker's flag can be set. Treating a
    ///   shutdown as "found nothing" is therefore unreachable rather than
    ///   wrong -- and leaving the loop on it would be wrong: the jobs of the
    ///   inner run name a topology the corunning frame owns.
    pub(crate) fn corun_until(&mut self, mut stop: impl FnMut() -> bool) {
        while !stop() {
            if let Some(job) = self.pop() {
                self.invoke(job);
                continue;
            }

            match self.explore_task() {
                Stolen::Job(job) => self.invoke(job),
                Stolen::Nothing | Stolen::Shutdown => thread::yield_now(),
            }
        }
    }

    /// Runs `job`, then drains this worker's own queue, stopping early if
    /// `stop` says to.
    ///
    /// Upstream's `_exploit_task`. Local work comes first and in LIFO order:
    /// the job pushed most recently is the one most likely to still be in
    /// cache, and taking it keeps the queue's older, coarser work available for
    /// thieves. Jobs left in the queue by an early stop are not lost -- they
    /// are picked up by the next drain, or stolen.
    fn exploit_task(&mut self, first: Option<Job>, stop: &mut impl FnMut() -> bool) {
        let mut job = first;
        while let Some(next) = job {
            self.invoke(next);
            if stop() {
                return;
            }
            job = self.pop();
        }
    }

    /// Queues `job` on this worker's own queue. Upstream's
    /// `_schedule(Worker&, Node*)`.
    ///
    /// Upstream can fail this push -- its per-worker queue is bounded -- and
    /// spills into the shared buffers when it does. The deque behind
    /// [`WorkerQueue`] grows instead, so the spill path has no equivalent
    /// here; see that module for the trade. A corun view has no queue of its
    /// own and goes to the buffers for a different reason, which is
    /// [`LocalQueue`]'s.
    pub(crate) fn schedule(&self, job: Job) {
        match &self.queue {
            LocalQueue::Owned(queue) => queue.push(job),
            LocalQueue::Corun => self.shared.buffers.push(job),
        }
        self.shared.notifier.notify_one();
    }

    /// Runs one job: the task's work, then the bookkeeping that decides what
    /// becomes runnable next.
    ///
    /// Upstream's `_invoke`. The four node kinds it can run itself are static,
    /// placeholder and the two conditioners; the four dynamic-tasking kinds
    /// each get one line here into the module that owns them. [`module`] runs
    /// a composed taskflow as of #49 and refuses an adopted graph until #74;
    /// [`subflow`] and [`runtime`] refuse the run for now. Upstream's three
    /// remaining kinds, its
    /// semaphores and its observers are each a later milestone, and none of
    /// them is silently absent: the `match` below is exhaustive over
    /// [`Handle`], so a new kind arrives as a compile error here rather than
    /// as a task that mysteriously does nothing.
    ///
    /// The bookkeeping is upstream's two switches over the handle, in its
    /// order: run the work, re-arm this node's own join counter, then release
    /// successors -- every one of them for an ordinary task, only the selected
    /// ones for a conditioner, and each one counted against this node's parent
    /// before it is queued. The module docs set out the semantics; [`Release`]
    /// is what carries the first switch's answer to the second.
    ///
    /// Before any of that come the two things upstream does at the top of
    /// `_invoke`: the `PREEMPTED` fast path and the cancellation check.
    ///
    /// Upstream's `_update_cache`/`TF_INVOKE_CONTINUATION` is not ported
    /// either: it keeps one ready successor out of the queue and jumps
    /// straight back to the top of `_invoke` with it. That is a worthwhile
    /// optimisation and a measurable one, so it belongs with the benchmarks
    /// rather than with the first correct version.
    pub(crate) fn invoke(&mut self, job: Job) {
        // The job owns a share of the run and of the taskflow's storage, so
        // this needs no `unsafe` and no argument about who outlives whom: both
        // are alive because the job that named them is. See
        // `executor::topology`.
        let (node, graph, topology) = job.parts();

        // Upstream's `if(node->_nstate & NSTATE::PREEMPTED) goto invoke_task`:
        // this is the *second* entry into a node whose nested graph has just
        // finished, so the work below is a resumption rather than a start, and
        // the cancellation check is deliberately skipped. A parent that was
        // cancelled while its children ran still has to come back through the
        // dispatch, because that is where it clears its own `PREEMPTED` bit
        // and releases what the run still owes.
        if node.nstate().load(Ordering::Relaxed) & Node::PREEMPTED == 0
            // Upstream's `_is_parent_cancelled()` early-out: a cancelled run
            // runs none of the work it had not started, and the job is torn
            // down all the same -- the join counters still have to reach zero
            // for the run to finish at all. The tail of this method says what
            // spending the job here does; it is the same line.
            && job.is_parent_cancelled()
        {
            self.finish_job(job.finish());
            return;
        }

        let release = match node.handle() {
            // Upstream's `_invoke_static_task`, whose
            // `TF_EXECUTOR_EXCEPTION_HANDLER` is this `catch_unwind` and whose
            // `_process_exception` is `Job::record_panic`: the failure is
            // delivered to whatever is waiting for the graph this node is
            // part of -- the run, for a top-level task -- cancels the rest of
            // that graph, and is reported from `wait` or from the frame that
            // was waiting, never raised on a worker thread. `AssertUnwindSafe`
            // is the honest annotation -- the state a panicking body can leave
            // half-written is the user's own, reachable only through what the
            // closure captured.
            //
            // The successors below are still released, as upstream's are: its
            // exception handler is the body of the first switch and does not
            // skip the second. What stops them is the early-out above, when
            // the worker that dequeues one finds the run cancelled.
            Handle::Static(work) => {
                if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| work.invoke())) {
                    job.record_panic(payload);
                }
                Some(Release::Strong)
            }
            // Upstream's `std::monostate` case: a placeholder has no work, and
            // it still has successors to release.
            Handle::Placeholder => Some(Release::Strong),
            // Upstream's `_invoke_condition_task`, which is the static invoker
            // plus a return value: the same exception handler, and `conds = {
            // work() }` where the static one discards what it called. A panic
            // is therefore recorded exactly as a static task's is -- rest of
            // the run cancelled and all -- and the selection it leaves behind
            // is empty, which selects no successor and ends the branch.
            Handle::Condition(work) => Some(Release::Selected(
                panic::catch_unwind(AssertUnwindSafe(|| work.invoke()))
                    .map_err(|payload| job.record_panic(payload))
                    .ok(),
            )),
            // Upstream's `_invoke_multi_condition_task`, the same again over
            // however many indices the body returns.
            Handle::MultiCondition(work) => Some(Release::SelectedMany(
                panic::catch_unwind(AssertUnwindSafe(|| work.invoke()))
                    .map_err(|payload| job.record_panic(payload))
                    .unwrap_or_default(),
            )),
            // The four dynamic-tasking handles, each upstream's
            // `_invoke_*_task` and each one line so that the three modules
            // below are the only ones #50 and #51 have to edit. Every one of
            // them may preempt this node rather than finish it, which is what
            // the `None` under the match is about.
            //
            // The two module arms are upstream's two forwarders, and each
            // hands over what its handle *owns* rather than the `Graph&`
            // upstream resolves here: a nested graph is scheduled through the
            // share that owns it, which only one of the two has. See
            // [`module`].
            Handle::Subflow { work, subgraph } => subflow::invoke(self, &job, work, subgraph),
            Handle::Runtime(work) => runtime::invoke(self, &job, work),
            Handle::Module(target) => module::invoke(self, &job, target),
            Handle::AdoptedModule(adopted) => module::invoke_adopted(self, &job, adopted),
        };

        // Upstream's `return true` out of `_invoke_subflow_task`,
        // `_invoke_module_task_impl` and `_invoke_runtime_task`, which its
        // `_invoke` turns into a bare `return`: this node is the parent of a
        // graph that is now running, and it owes nothing until that graph is
        // done. Its own join counter is not re-armed and its successors are
        // not released, because the node has not finished -- the last job of
        // the nested graph re-queues it, and the second entry takes the fast
        // path above and falls through to here.
        let Some(release) = release else {
            return;
        };

        // Upstream's comment, which is the whole reason this is a `fetch_add`
        // of the strong-dependency count rather than a `store`: the counter
        // has to be re-armed before any successor is scheduled, so that a
        // cycle -- which only a conditioner can create -- finds this node
        // ready again, and it has to be an add because the body just run may
        // itself have scheduled this node.
        node.join_counter()
            .fetch_add(strong_dependencies(node), Ordering::Relaxed);

        // Upstream's second switch over the handle, which [`Release`] has
        // already answered. Either arm counts a successor against this node's
        // parent before queueing it, or the run could look finished with a job
        // still in flight.
        match release.selected() {
            // Upstream's `default` arm: every successor, each the moment its
            // last strong dependency is spent.
            None => {
                for &successor in node.successors() {
                    let next = graph.node(successor);
                    if next.join_counter().fetch_sub(1, Ordering::AcqRel) == 1 {
                        job.parent().count_queued(topology, 1);
                        self.schedule(job.successor(successor));
                    }
                }
            }
            // Upstream's `CONDITION`/`MULTI_CONDITION` arm: only the
            // successors the body named, each scheduled outright rather than
            // waited for. An index naming no successor is upstream's
            // `cond >= 0 && static_cast<size_t>(cond) < _num_successors`
            // failing, and is how a branch ends.
            Some(conditions) => {
                let num_successors = node.num_successors();
                for &condition in conditions {
                    let Some(index) = selected_successor(condition, num_successors) else {
                        continue;
                    };
                    let successor = node.successors()[index];
                    // Upstream's "zeroing the join counter for invariant": a
                    // weak successor may be re-entered on a cycle before its
                    // strong predecessors run again, so nothing is going to
                    // decrement its counter to the zero that means ready. See
                    // the module docs.
                    graph
                        .node(successor)
                        .join_counter()
                        .store(0, Ordering::Relaxed);
                    job.parent().count_queued(topology, 1);
                    self.schedule(job.successor(successor));
                }
            }
        }

        // Upstream's `_tear_down_nonasync`. Spending the job here is what
        // releases its share of the taskflow's storage, and it has to happen
        // before the join counter below may end the run -- `Job::finish`
        // says why. It also ends the borrows this method has been working
        // through, so the compiler is what stops anything from touching
        // `node`, `graph` or the run afterwards.
        self.finish_job(job.finish());
    }

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
            // The sticky victim goes to the buffer that answered, not to the
            // first one, which is upstream's behaviour as it walks the buffers
            // one at a time: the next thing this worker does is steal from its
            // sticky victim, so pointing it anywhere else spends an attempt on
            // a queue already known to be empty.
            if let Some(buffer) = self.shared.buffers.first_nonempty() {
                self.shared.notifier.cancel_wait(self.id);
                self.sticky_victim = self.shared.workers.len() + buffer;
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

/// What the work a worker has just run leaves its node's successors owed.
///
/// Upstream needs no such type: it declares one `SmallVector<int> conds` above
/// its invoke `switch`, fills it in the two conditioner arms only, and then
/// keys a second `switch` off the handle again to decide whether to read it --
/// so "not a conditioner" and "a conditioner that selected nothing" are told
/// apart by re-examining the handle rather than by the value. Here the value
/// says which it is, which is what keeps the two passes over the handle from
/// being able to disagree: a conditioner that panicked selects nothing, and
/// that is a `Selected(None)`, never a `Strong`.
///
/// The three modules that run a nested graph answer with one of these too, or
/// with `None` for a node they have preempted: see [`WorkerThread::invoke`].
pub(crate) enum Release {
    /// Every successor, each once its own strong dependencies are spent: the
    /// rule for every handle that is not a conditioner.
    Strong,
    /// Only the successor a [`Handle::Condition`] named, and none if it
    /// panicked. An [`Option`] rather than a one-element `Vec` because this is
    /// the arm on a cycle's hot path -- upstream's `SmallVector` holds its one
    /// index inline, and a heap allocation per iteration of a loop would not.
    Selected(Option<i32>),
    /// Only the successors a [`Handle::MultiCondition`] named, empty if it
    /// panicked.
    SelectedMany(Vec<i32>),
}

impl Release {
    /// The successor positions this release names, or `None` if it names no
    /// positions at all because every successor is to be released instead.
    ///
    /// The two cases cannot be confused for one another, which is the whole
    /// point of the type: `Some(&[])` is a branch that ended, `None` is an
    /// ordinary task.
    fn selected(&self) -> Option<&[i32]> {
        match self {
            Self::Strong => None,
            Self::Selected(condition) => Some(condition.as_slice()),
            Self::SelectedMany(conditions) => Some(conditions),
        }
    }
}

/// How many strong dependencies `node` has, out of the state word that holds
/// the count and the scheduler's state bits together.
///
/// Upstream's `node->_nstate & NSTATE::STRONG_DEPENDENCIES_MASK`, and the mask
/// is not decoration: the three state bits live above the count in the same
/// word, so re-arming a join counter from the unmasked word would add
/// 536,870,912 or more to it. The node would then never reach zero again and
/// the failure would be a hang rather than a wrong answer -- which is why the
/// mask arrived in the same change as `PREEMPTED`, the first bit the scheduler
/// itself sets on a node it means to come back to.
fn strong_dependencies(node: &Node) -> usize {
    usize::try_from(node.nstate().load(Ordering::Relaxed) & Node::STRONG_DEPENDENCIES_MASK)
        .expect("a u32 always fits in a usize on a 32-bit target or wider")
}

/// Narrows one index a conditioner returned onto a successor position, or
/// `None` if it names no successor.
///
/// Upstream's `cond >= 0 && static_cast<size_t>(cond) < node->_num_successors`,
/// which is two guards rather than one because its cast would otherwise turn a
/// negative index into a huge positive one. `usize::try_from` is the same two
/// guards with the cast removed: a negative `i32` fails the conversion and an
/// out-of-range one fails the filter. That a bad index quietly selects nothing
/// is upstream's behaviour and is load-bearing -- `return -1` is how a
/// conditional branch is written to end.
fn selected_successor(condition: i32, num_successors: usize) -> Option<usize> {
    usize::try_from(condition)
        .ok()
        .filter(|index| *index < num_successors)
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
    use crate::graph::Graph;

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

    /// The index filter is upstream's two guards, and the range it has to
    /// cover is `test_control_flow.cpp`'s own: `multiple_branches` draws its
    /// branch indices from `-10..=110` against a node with 100 successors, so
    /// both ends of that range have to select nothing.
    #[test]
    fn only_an_in_range_index_selects_a_successor() {
        for condition in -10..=110 {
            let selected = selected_successor(condition, 100);
            let expected = (0..100)
                .contains(&condition)
                .then(|| usize::try_from(condition).expect("a non-negative i32 fits in a usize"));
            assert_eq!(selected, expected, "index {condition}");
        }
    }

    /// The extremes, which the range above does not reach: upstream's
    /// `return -1` ends a branch, and no index selects a successor of a node
    /// that has none.
    #[test]
    fn no_index_selects_anything_without_successors() {
        assert_eq!(selected_successor(-1, 2), None);
        assert_eq!(selected_successor(i32::MIN, 2), None);
        assert_eq!(selected_successor(i32::MAX, 2), None);
        for condition in [i32::MIN, -1, 0, 1, i32::MAX] {
            assert_eq!(selected_successor(condition, 0), None, "index {condition}");
        }
    }

    /// `Release` exists to keep "release everything" and "release nothing"
    /// apart, so that is the property worth pinning: a conditioner that
    /// selected nothing must not read as an ordinary task.
    #[test]
    fn a_conditioner_that_selected_nothing_is_not_a_strong_release() {
        assert_eq!(Release::Strong.selected(), None);
        assert_eq!(Release::Selected(None).selected(), Some(&[][..]));
        assert_eq!(Release::SelectedMany(Vec::new()).selected(), Some(&[][..]));
        assert_eq!(Release::Selected(Some(3)).selected(), Some(&[3][..]));
        assert_eq!(
            Release::SelectedMany(vec![3, -1]).selected(),
            Some(&[3, -1][..])
        );
    }

    /// The mask is what keeps a scheduler state bit out of a join counter,
    /// and this is the case that fails *fast* without it: a node carrying one
    /// bit must re-arm from its count alone. Unmasked, each of these would
    /// read as hundreds of millions of dependencies, the node would never
    /// become ready again, and the run would hang with no assertion to point
    /// at it.
    #[test]
    fn a_state_bit_is_not_counted_as_a_strong_dependency() {
        let mut graph = Graph::new();
        let task = graph.add_node(Handle::Placeholder);
        let node = graph.node(task);

        for count in [0_u32, 1, 2, Node::STRONG_DEPENDENCIES_MASK] {
            for state in [
                0,
                Node::PREEMPTED,
                Node::RETAIN_SUBFLOW,
                Node::JOINED_SUBFLOW,
                Node::PREEMPTED | Node::RETAIN_SUBFLOW | Node::JOINED_SUBFLOW,
            ] {
                node.nstate().store(count | state, Ordering::Relaxed);
                assert_eq!(
                    strong_dependencies(node),
                    usize::try_from(count).expect("a u32 fits in a usize"),
                    "count {count:#x} with state {state:#x}"
                );
            }
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

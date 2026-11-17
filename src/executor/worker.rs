//! Translated from `taskflow/core/worker.hpp` (Taskflow v4.1.0).
//!
//! The scheduling loop itself -- upstream's `Executor::_spawn`,
//! `_exploit_task`, `_explore_task`, `_wait_for_task`, `_schedule`, `_invoke`
//! and `_tear_down_topology`, from `taskflow/core/executor.hpp` of the same
//! version -- is translated here too, rather than into the parent module,
//! because every line of it either reads or writes one worker's own state or
//! runs on that worker's thread. The parent module keeps what the *executor*
//! owns: the worker array, the buffers, the notifier and the thread-id map.
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
//! That split is also why a [`WorkerThread`] does not always hold the owning
//! end of its queue. A corun loop -- [`WorkerThread::corun_until`], the loop
//! `Executor::corun` runs -- starts from *inside* a task body, which is inside
//! the [`WorkerThread`] frame already running that worker's loop, and a frame
//! cannot be named from the code it called. So a corun loop gets a second
//! [`WorkerThread`] over the same worker, built by
//! [`WorkerThread::corun_view`], and reaches the queue the only way a value
//! without the owning end can: see [`LocalQueue`].
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

use super::Shared;
use crate::executor::notifier::Notifier;
use crate::executor::queue::{Thief, WorkerQueue};
use crate::executor::rng::Xorshift32;
use crate::executor::topology::{Job, Topology};
use crate::graph::Handle;
use crate::taskflow::Taskflow;

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
    fn pop(&self) -> Option<Job> {
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
    /// Upstream's `_invoke`, restricted to the two node kinds it can run --
    /// static and placeholder. Upstream's eight other kinds, its semaphores,
    /// its observers and its cancellation check are each a later milestone,
    /// and none of them is silently absent: the `match` below is exhaustive
    /// over [`Handle`], so a new kind arrives as a compile error here rather
    /// than as a task that mysteriously does nothing. The two conditioner
    /// handles are what that caught first -- they are graph citizens as of
    /// #28, with nothing to branch on them until #31 -- and their arm refuses
    /// the run instead of guessing at the scheduling.
    ///
    /// Upstream's `_update_cache`/`TF_INVOKE_CONTINUATION` is not ported
    /// either: it keeps one ready successor out of the queue and jumps
    /// straight back to the top of `_invoke` with it. That is a worthwhile
    /// optimisation and a measurable one, so it belongs with the benchmarks
    /// rather than with the first correct version.
    pub(crate) fn invoke(&mut self, job: Job) {
        // SAFETY: a job reaches a worker only through a queue, and the only
        // things that put one there are a run whose `RunHandle` is alive and a
        // corun whose frame has not returned. Both own the topology and both
        // block until its join counter reaches zero, and the worker that
        // drives it to zero does nothing with the topology, the taskflow or
        // the node afterwards except `Topology::finish`, which is the release
        // of that very owner. So every job that can be dequeued names a live
        // node of a borrowed graph and a live topology. See
        // `executor::topology` for the full argument and `executor::corun` for
        // the corun half of it.
        let (node, taskflow, topology) = unsafe { job.parts() };
        let graph = taskflow.graph();

        match node.handle() {
            // Upstream's `_invoke_static_task`, whose
            // `TF_EXECUTOR_EXCEPTION_HANDLER` is this `catch_unwind`: the
            // failure is stored on the topology and reported from `wait`,
            // never raised on a worker thread. `AssertUnwindSafe` is the
            // honest annotation -- the state a panicking body can leave
            // half-written is the user's own, reachable only through what the
            // closure captured.
            Handle::Static(work) => {
                if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| work.invoke())) {
                    topology.record_panic(payload);
                }
            }
            // Upstream's `std::monostate` case: a placeholder has no work, and
            // it still has successors to release.
            Handle::Placeholder => {}
            // Upstream's `_invoke_condition_task`/`_invoke_multi_condition_task`
            // invoke the work and then schedule only the successors its return
            // value names. That is issue #31's, and it cannot be half-done
            // here: a conditioner's edge is deliberately *not* counted into
            // the join counter by `Topology::set_up`, so the unconditional
            // release below is wrong for precisely these two handles. So the
            // run is refused rather than approximated -- the work is not
            // invoked at all -- and refused through the channel the static arm
            // above already uses. A bare `panic!` would be the worse failure:
            // it would kill the worker with this job unfinished, and
            // `RunHandle::drop` would then block for ever instead of saying
            // anything.
            Handle::Condition(_) | Handle::MultiCondition(_) => {
                topology.record_panic(Box::new(
                    "running a condition task is not supported yet: \
                     conditional scheduling is issue #31",
                ));
            }
        }

        // Upstream's comment, which is the whole reason this is a `fetch_add`
        // of the strong-dependency count rather than a `store`: the counter
        // has to be re-armed before any successor is scheduled, so that a
        // cycle -- which only a conditioner can create -- finds this node
        // ready again, and it has to be an add because the body just run may
        // itself have scheduled this node.
        node.join_counter().fetch_add(
            usize::try_from(node.nstate().load(Ordering::Relaxed))
                .expect("a u32 always fits in a usize on a 32-bit target or wider"),
            Ordering::Relaxed,
        );

        for &successor in node.successors() {
            let next = graph.node(successor);
            if next.join_counter().fetch_sub(1, Ordering::AcqRel) == 1 {
                // Counted on the topology before it is queued, or the run
                // could look finished with a job still in flight.
                topology.job_queued();
                self.schedule(Job::new(successor, taskflow, topology));
            }
        }

        // Upstream's `_tear_down_nonasync`. Nothing below may touch `node`,
        // `graph`, `taskflow` or `topology`: the tear-down releases the thread
        // that owns all four.
        if topology.job_finished() {
            self.tear_down_topology(taskflow, topology);
        }
    }

    /// Ends a run whose last job has just finished. Upstream's
    /// `_tear_down_topology`.
    ///
    /// Only the final-run half of upstream's is here, because only a topology
    /// that runs once can exist yet. The rest of it hooks in at the top of
    /// this method, before either line below, and in this order: evaluate the
    /// run predicate and, if it says run again, re-arm the graph with
    /// [`Topology::set_up`](crate::executor::topology::Topology::set_up) and
    /// queue its sources through [`WorkerThread::schedule`] -- a re-run is the
    /// same topology, so it must neither be counted again nor finish -- and
    /// otherwise call the per-run callback and start the next topology of the
    /// taskflow's queue. `taskflow` is a parameter for exactly that: re-arming
    /// the graph needs it, and the topology deliberately holds no reference to
    /// it.
    pub(crate) fn tear_down_topology(&mut self, _taskflow: &Taskflow, topology: &Topology) {
        // The accounting first and the completion slot last. This is
        // deliberately *not* upstream's order -- its `_tear_down_topology`
        // calls `_carry_out_promise()` and only then `_decrement_topology()`,
        // so a caller whose `tf::Future` has just come back can still see
        // `num_topologies()` counting the run it has already been handed the
        // result of. The window is small, real and free to close, and worth
        // closing because that count is what `wait_for_all` blocks on and what
        // the worker loop's fast path reads: here, once a `wait` returns, the
        // run is fully accounted for. The release in `decrement_topology`'s
        // `fetch_sub` and the lock `finish` takes are what carry that to the
        // waiting thread, whose own read of the count is relaxed.
        //
        // The order is also the only one the port's lifetimes allow, which is
        // what makes it free: `finish` releases the thread that owns the
        // topology *and* the taskflow's borrow, so it has to be this worker's
        // last touch of either, while `decrement_topology` reads only the
        // executor's shared state -- which every worker holds a share of, and
        // which therefore outlives all of them.
        self.shared.decrement_topology();
        topology.finish();
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

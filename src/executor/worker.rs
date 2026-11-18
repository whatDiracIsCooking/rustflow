//! Translated from `taskflow/core/worker.hpp` (Taskflow v4.1.0).
//!
//! The scheduling loop itself -- upstream's `Executor::_spawn`,
//! `_exploit_task`, `_explore_task`, `_wait_for_task`, `_schedule` and
//! `_invoke`, from `taskflow/core/executor.hpp` of the same version -- is
//! translated here rather than into the parent module, because every line of
//! it either reads or writes one worker's own state or runs on that worker's
//! thread. The parent module keeps what the *executor* owns, and
//! `_tear_down_topology`, which a worker calls but which is all about the run,
//! lives in [`run_handle`](crate::executor::run_handle).
//!
//! # Design
//!
//! **A worker is two types here and one upstream.** Upstream's `Worker` holds
//! the id, the sticky victim, the generator, the queue, the thread handle and
//! the `done` flag in one struct, because C++ lets the executor reach into a
//! worker from any thread and leaves "only the owner pops its own queue" as a
//! comment. Here the compiler asks which fields are shared: [`Worker`] is the
//! part other threads touch and is `Sync`, and `WorkerThread` is the part only
//! the worker's own thread touches, created inside the spawned thread and
//! moving nowhere -- so that rule is a property of where the value lives. A
//! corun loop starts inside a task body, itself inside the `WorkerThread`
//! frame already running that worker's loop, which the code it calls cannot
//! name, so it gets a second view: `LocalQueue`. Upstream's `_thread` has no
//! equivalent, a `JoinHandle` being owned by whoever will join it, which costs
//! the port its trick of setting thread affinity from the prologue.
//!
//! **Conditional scheduling.** A conditioner -- [`Handle::Condition`] or
//! [`Handle::MultiCondition`] -- is the one node kind that picks its own
//! successors, and that single fact is what the strong/weak dependency split
//! exists for. An edge out of a *non*-conditioner is a **strong** dependency,
//! counted into the successor's join counter by `set_up_graph` and spent as
//! the predecessor finishes. An edge out of a conditioner is **weak**: it
//! gates nothing, and the body's return value names successor *positions* --
//! one `i32` for a condition task, any number for a multi-condition one --
//! which are scheduled outright. An index that is negative or past the last
//! successor names nothing, which is how a branch ends, and so does a
//! conditioner whose body panics.
//!
//! A selected successor has its join counter *stored to zero* rather than
//! decremented: upstream's "zeroing the join counter for invariant", the
//! invariant being that a counter reads zero exactly when the node is ready to
//! run, which a node entered through a weak edge cannot reach by arithmetic --
//! a cycle may re-enter it before its strong predecessors run again. Hence
//! upstream's one rule for writing a conditional graph (its
//! `ConditionalTasking` page): **a task reached through a condition must be
//! reachable only through that condition.** A weak successor with a strong
//! predecessor can start before that predecessor has run, and one two
//! conditioners both select may start twice at once, which
//! [`Work::invoke`](crate::Work::invoke) reports as a violated
//! one-worker-per-node promise; neither is diagnosed when the graph is built,
//! as upstream does not diagnose them either. None of this state outlives a
//! run, every node being re-armed before the next run's sources are queued.

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
use crate::executor::{r#async, dependent_async, module, runtime, subflow};
use crate::graph::{Handle, Node};
use crate::observer::{TaskView, WorkerView};

/// What a strong release that lands on a node some bypass has already entered
/// reports, instead of resuming that node: the successor arm of
/// [`WorkerThread::invoke_one`] is where it is read. A `&'static str` payload,
/// which is what a `panic!` with a literal message produces and what
/// [`RunError`](crate::RunError) renders first.
const REENTERED: &str = "a task was released while it was already running: a \
                         bypassed dependency may not re-enter a task the run \
                         has already entered";

/// A worker of an [`Executor`](crate::Executor), as every thread but its own
/// sees it. A worker id is an index in `0..num_workers`, fixed when the
/// executor is built. The fields are `pub(crate)` rather than private because
/// the scheduling loop here and the run methods later modules add to `Executor`
/// are one scheduler spread over several files.
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
    /// call. Upstream exposes `queue_size()` and `queue_capacity()` here; a
    /// `crossbeam` deque reports neither, and its capacity would be a moving
    /// target where upstream's is bounded, so this is the part of that
    /// interface the port can answer.
    #[must_use]
    pub fn is_queue_empty(&self) -> bool {
        self.thief.is_empty()
    }

    /// Returns `true` once the executor has asked this worker to stop.
    pub(crate) fn is_done(&self) -> bool {
        self.done.load(Ordering::Relaxed)
    }
}

/// A hook that runs on every worker thread, around the scheduling loop:
/// upstream's `tf::WorkerInterface`, whose stated purpose is to let a caller
/// configure a worker thread -- its name, its priority, its CPU affinity -- from
/// inside that thread, at the only two moments the executor offers. Both methods
/// take `&self` and run on every worker at once, so an implementation that
/// accumulates anything needs its own synchronisation, which is why the trait
/// requires `Send + Sync`.
pub trait WorkerInterface: Send + Sync {
    /// Runs on the worker thread before it enters the scheduling loop.
    fn scheduler_prologue(&self, worker: &Worker);

    /// Runs on the worker thread after it leaves the scheduling loop, with the
    /// payload of the panic that ended the loop if one did: upstream's
    /// `std::exception_ptr`, `None` on every normal shutdown.
    fn scheduler_epilogue(&self, worker: &Worker, panicked: Option<Box<dyn Any + Send>>);
}

/// The worker state that belongs to the worker's own thread, and the scheduling
/// loop over it: created inside the spawned thread, dropped when it ends.
pub(crate) struct WorkerThread {
    /// Everything the executor shares with its workers.
    pub(crate) shared: Arc<Shared>,
    /// This worker's id, the same as `self.worker().id`, kept here so that the
    /// hot path indexes the worker array instead of chasing it.
    pub(crate) id: usize,
    /// Where this loop takes its local work from, and where it puts the work
    /// it makes ready. See [`LocalQueue`].
    pub(crate) queue: LocalQueue,
    /// The queue this worker tries to steal from first: upstream's
    /// `_sticky_victim`, and the reason the steal loop is not purely random --
    /// a worker that stole from somebody once is likely to again.
    pub(crate) sticky_victim: usize,
    /// This worker's victim-picking generator. Per-worker, a shared one being a
    /// contended cache line on the hottest path there is.
    pub(crate) rng: Xorshift32,
}

/// Which end of a worker's queue a scheduling loop holds.
///
/// Upstream needs no equivalent: its `_corun_until` takes the `Worker&` the
/// executor hands out and pops that worker's queue through it. Here the owning
/// end is a `!Sync` value in the [`WorkerThread`] on the worker's own stack
/// frame, which a task body cannot name, so a corun loop runs on a
/// [`WorkerThread::corun_view`] with no owning end: taking local work becomes a
/// steal from the worker's own [`Thief`], and newly ready work goes to the
/// shared buffers. Both are slower, and neither changes which jobs run.
pub(crate) enum LocalQueue {
    /// The owning end, held by the thread running this worker's own scheduling
    /// loop: pops are LIFO and uncontended, and nothing else may touch it.
    Owned(WorkerQueue<Job>),
    /// No owning end, because another [`WorkerThread`] on this thread has it.
    Corun,
}

/// What one round of stealing came back with. Upstream's `_explore_task`
/// returns a `bool` and writes the stolen task through a reference, which
/// leaves "returned true and the task is still null" as a third case the
/// caller has to remember; naming the three is the same control flow.
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

    /// A second view of worker `id`, for a corun loop that starts inside one of
    /// that worker's task bodies. Call it only from worker `id`'s own thread --
    /// `Executor::corun` checks that before it gets here. The two views share the
    /// worker's queue but not the sticky victim and not the generator, those being
    /// a `&mut` each and the owned one borrowed by the frame below.
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

    /// Takes the next job from this worker's own queue, or `None` if it held none:
    /// upstream's `w._wsq.pop()`. `pub(crate)` for the unit tests in
    /// `executor::topology`, which drive the scheduler one job at a time.
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
        // exception to the epilogue; `AssertUnwindSafe` is the honest
        // annotation, the state a panicking task could leave half-written being
        // this worker's own. The stop condition never fires: this worker leaves
        // the loop as upstream's does, on `_wait_for_task` reading `done`.
        let panicked = panic::catch_unwind(AssertUnwindSafe(|| self.run_until(|| false))).err();

        if let Some(interface) = interface {
            interface.scheduler_epilogue(self.worker(), panicked);
        }
        // Without an interface the payload is dropped here, as upstream drops
        // its exception pointer; the default panic hook has already printed it.
    }

    /// The work-stealing loop: drain the local queue, then steal or sleep until
    /// there is something to drain again, and stop when `stop` says so or when the
    /// executor shuts this worker down. Upstream's `while(1)` over `_exploit_task`
    /// and `_wait_for_task`. The stop condition is a parameter because this loop was
    /// factored out for `corun`, and why it turned out not to be the loop `corun`
    /// can use is worth keeping: [`WorkerThread::wait_for_task`] parks the worker,
    /// and nothing wakes the notifier when a *topology* finishes, so a corun loop
    /// reaching the sleep path would wait for a notification never sent, having
    /// parked the thread the run it waits for may need.
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
    /// anybody's -- until `stop` says that whatever this thread is waiting for has
    /// happened. Upstream's `_corun_until`. Three things differ from
    /// [`WorkerThread::run_until`], and all three are upstream's too: one local job
    /// per round rather than a drain, a job run here being able to corun itself and
    /// upstream's comment warning of "a very deep recursive corun"; never sleeping,
    /// a worker parked here being one the run it waits for cannot use; and ignoring
    /// the `done` flag, a corun being nested inside a task body, so that run is in
    /// flight and the executor cannot be dropped.
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

    /// Runs `job`, then drains this worker's own queue, stopping early if `stop`
    /// says to: upstream's `_exploit_task`. Local work comes first and in LIFO
    /// order, the job pushed most recently being the one most likely to still be in
    /// cache. Jobs left by an early stop are picked up by the next drain, or
    /// stolen.
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

    /// Queues `job` on this worker's own queue: upstream's
    /// `_schedule(Worker&, Node*)`. Upstream can fail this push, its per-worker
    /// queue being bounded, and spills into the shared buffers; the deque behind
    /// [`WorkerQueue`] grows instead, and a corun view goes to the buffers for
    /// [`LocalQueue`]'s reason. Two kinds of caller, which is upstream's two callers
    /// of the same overload: this worker releasing the successors of a job it has
    /// just finished, and a free async task submitted from inside a runtime body.
    pub(crate) fn schedule(&self, job: Job) {
        match &self.queue {
            LocalQueue::Owned(queue) => queue.push(job),
            LocalQueue::Corun => self.shared.buffers.push(job),
        }
        self.shared.notifier.notify_one();
    }

    /// Runs `job`, and then whatever that job hands straight back to this worker,
    /// until one of them hands back nothing.
    ///
    /// Upstream's `_invoke` is one function with a label at the top and a `goto` at
    /// the bottom: `TF_INVOKE_CONTINUATION()` jumps back to `begin_invoke:` with the
    /// one successor `_update_cache` kept out of the queue. That is this loop, and
    /// [`WorkerThread::invoke_one`] is the body it jumps back to the top of. What it
    /// buys is that a chain runs end to end on the worker that started it, where
    /// without it every link may be stolen first; `BENCHMARKS.md` has the figures,
    /// and two of upstream's own cases in `tests/work_stealing.rs` assert the
    /// mechanism. The loop is here rather than in
    /// [`WorkerThread::exploit_task`], which would run the same jobs in the same
    /// order, because a job may only be held while the run it belongs to cannot
    /// finish -- and what makes holding one across the tear-down of the job that
    /// produced it sound is that it was counted against its parent first, which is
    /// [`WorkerThread::update_cache`]'s argument to make.
    pub(crate) fn invoke(&mut self, job: Job) {
        let mut job = job;
        while let Some(continuation) = self.invoke_one(job) {
            job = continuation;
        }
    }

    /// Runs one job: the task's work, then the bookkeeping that decides what becomes
    /// runnable next. Returns the one successor kept out of the queue for this
    /// worker to run next, if there was one.
    ///
    /// Upstream's `_invoke` body, from `begin_invoke:` to the
    /// `TF_INVOKE_CONTINUATION()` at the end of it; the `goto` itself is
    /// [`WorkerThread::invoke`]. The four kinds it runs itself are static,
    /// placeholder and the two conditioners; the four dynamic-tasking kinds each get
    /// one line into the module that owns them, and the two async kinds have arms
    /// that refuse a handle somebody built into a taskflow. The `match` is
    /// exhaustive over [`Handle`], so a new kind arrives as a compile error rather
    /// than as a task that does nothing. The bookkeeping is upstream's two switches
    /// over the handle, in its order: run the work, re-arm this node's own join
    /// counter, then release successors, each counted against this node's parent
    /// before it is queued. First, though, come the three things upstream does at
    /// the top of `_invoke`: the `PREEMPTED` fast path, the cancellation check and
    /// the acquire, the last two *inside* the fast path.
    fn invoke_one(&mut self, job: Job) -> Option<Job> {
        // The job owns a share of the run and of the taskflow's storage, so
        // this needs no `unsafe` and no argument about who outlives whom: both
        // are alive because the job that named them is.
        let (node, graph, topology) = job.parts();

        // Upstream's `if(node->_nstate & NSTATE::PREEMPTED) goto invoke_task`:
        // the *second* entry into a node whose nested graph has just finished,
        // so the cancellation check -- and the acquire with it -- is skipped. A
        // parent cancelled while its children ran still comes back through the
        // dispatch, that being where it clears its own `PREEMPTED` bit.
        if node.nstate().load(Ordering::Relaxed) & Node::PREEMPTED == 0 {
            // Upstream's `_is_parent_cancelled()` early-out: a cancelled run
            // runs none of the work it had not started, and the job is torn
            // down all the same, the join counters still having to reach zero.
            if job.is_parent_cancelled() {
                // Upstream's cancelled branch goes to `_tear_down_invoke`,
                // which dispatches on the handle: a skipped dependent-async
                // node still owes `FINISHED` and the tasks waiting for it,
                // each counted against this job's parent at submission.
                dependent_async::release_skipped(self, &job);
                self.finish_job(job.finish());
                return None;
            }

            // Upstream's `to_acquire` block, the first semaphore hook: a node
            // that cannot have every count it asked for does not run, and this
            // job is parked on the semaphore that refused, which owns a clone
            // of it. The job is *not* finished here, and that is the
            // accounting: the clone inherits the count this job holds against
            // its parent, so the run cannot finish while a task of it is
            // parked.
            let mut woken = Vec::new();
            if !node.acquire_all(&job, &mut woken) {
                self.shared.bulk_schedule(woken);
                // Nothing is kept: the only jobs made ready belong to
                // semaphores rather than to this node.
                return None;
            }
        }

        self.observer_prologue(&job);

        // Every arm below states its own epilogue behaviour as part of the
        // value it returns, through `Epilogue::Pending` or
        // `Epilogue::AlreadyRun`: whether this match's generic
        // `observer_epilogue` call still has to run is folded into the same
        // `match` that dispatches on the handle, rather than re-derived from
        // it separately, so a new arm cannot compile without choosing.
        let dispatch = match node.handle() {
            // Upstream's `_invoke_static_task`, whose
            // `TF_EXECUTOR_EXCEPTION_HANDLER` is this `catch_unwind` and whose
            // `_process_exception` is `Job::record_panic`: the failure goes to
            // whatever is waiting for this node's graph and is reported from
            // `wait` rather than raised on a worker. The successors below are
            // still released, as upstream's are; the early-out stops them.
            Handle::Static(work) => {
                if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| work.invoke())) {
                    job.record_panic(payload);
                }
                Epilogue::Pending(Some(Release::Strong))
            }
            // Upstream's `std::monostate` case: a placeholder has no work, and
            // it still has successors to release.
            Handle::Placeholder => Epilogue::Pending(Some(Release::Strong)),
            // Upstream's `_invoke_condition_task`: the static invoker plus a
            // return value, so a panic is recorded as a static task's is and
            // leaves an empty selection, which ends the branch.
            Handle::Condition(work) => Epilogue::Pending(Some(Release::Selected(
                panic::catch_unwind(AssertUnwindSafe(|| work.invoke()))
                    .map_err(|payload| job.record_panic(payload))
                    .ok(),
            ))),
            // Upstream's `_invoke_multi_condition_task`, over however many
            // indices the body returns.
            Handle::MultiCondition(work) => Epilogue::Pending(Some(Release::SelectedMany(
                panic::catch_unwind(AssertUnwindSafe(|| work.invoke()))
                    .map_err(|payload| job.record_panic(payload))
                    .unwrap_or_default(),
            ))),
            // The four dynamic-tasking handles, each upstream's
            // `_invoke_*_task` and each one line into the module that owns it.
            // Every one may preempt this node rather than finish it, which the
            // `None` under the match is about, and the two module arms hand
            // over what their handle *owns* rather than a `Graph&`. None of
            // the four signals a waiter from inside its own tear-down, so the
            // generic call after this match is still the one that runs it.
            Handle::Subflow { work, subgraph } => {
                Epilogue::Pending(subflow::invoke(self, &job, work, subgraph))
            }
            Handle::Runtime(work) => Epilogue::Pending(runtime::invoke(self, &job, work)),
            Handle::Module(target) => Epilogue::Pending(module::invoke(self, &job, target)),
            Handle::AdoptedModule(adopted) => {
                Epilogue::Pending(module::invoke_adopted(self, &job, adopted))
            }
            // The two async handles, the one pair of arms a graph is not
            // supposed to reach: an async task's node is free-standing and
            // dispatched through a job of its own arena, so what gets here is
            // a handle somebody put in a taskflow. Both refuse. Both also tear
            // themselves down inside their own dispatch function --
            // `Shared::finish_async`, a dependent task's successor release --
            // which can signal a waiter (`Executor::wait_for_all`,
            // `Executor::drop`) before this match produces a value at all, so
            // each calls `observer_epilogue` itself, right before that signal,
            // on every return path; see `executor::async::invoke` and
            // `executor::dependent_async::invoke`.
            Handle::Async(work) => Epilogue::AlreadyRun(r#async::invoke(self, &job, work)),
            Handle::DependentAsync(work) => {
                Epilogue::AlreadyRun(dependent_async::invoke(self, &job, work))
            }
        };

        let release = match dispatch {
            Epilogue::Pending(release) => {
                self.observer_epilogue(&job);
                release
            }
            Epilogue::AlreadyRun(release) => release,
        };

        // Upstream's `return true` out of its three preempting invokers, which
        // its `_invoke` turns into a bare `return`: this node is the parent of
        // a graph that is now running and owes nothing until that graph is
        // done. The last job of that graph re-queues it, and the second entry
        // takes the fast path above and falls through to here.
        let release = release?;

        // Upstream's `to_release` block, the second hook, here -- after the
        // dispatch and before the counters -- because this is the entry on
        // which the node *finished*: an arm that preempted returned through the
        // `?` above, holding what it acquired. The woken jobs go to the shared
        // buffers rather than into `cache`, which is for a job of *this* node's
        // graph under *this* node's parent: a woken job may be of another run.
        let mut woken = Vec::new();
        if let Err(error) = node.release_all(&mut woken) {
            // Upstream's `TF_THROW` out of `Semaphore::_release`, which escapes
            // the worker loop; here it is a failure of the task that
            // over-released, surfaced from `wait` as `RunError::Panicked`. A
            // `String` payload because that is what a `panic!` of a formatted
            // message leaves and what `RunError`'s `Display` reads back.
            job.record_panic(Box::new(error.to_string()));
        }
        // Outside the `if`, because the releases before the failing one
        // succeeded: dropping the jobs their waiting lists put in `woken` would
        // leave those counts outstanding for ever.
        self.shared.bulk_schedule(woken);

        // Upstream's comment, and the reason this is a `fetch_add` of the
        // strong-dependency count rather than a `store`: the counter is re-armed
        // before any successor is scheduled, so that a cycle finds this node
        // ready, and it is an add because the body may have scheduled it.
        node.join_counter()
            .fetch_add(strong_dependencies(node), Ordering::Relaxed);

        // Upstream's `Node* cache {nullptr}`: the one successor this worker
        // keeps for itself rather than queueing.
        let mut cache = None;

        // Upstream's second switch over the handle, which `Release` has already
        // answered. Either arm counts a successor against this node's parent
        // before queueing it, or the run could look finished with a job in
        // flight.
        match release.selected() {
            // Upstream's `default` arm: every successor, each the moment its
            // last strong dependency is spent. The decrement floors at zero,
            // which is this port's one departure here: a successor's counter
            // can *already* be zero, something having bypassed its dependencies
            // -- `Runtime::schedule` stores a zero, as does the arm below -- and
            // upstream's bare `fetch_sub` on a `size_t` wraps it to `SIZE_MAX`,
            // which no waiter matches, so a bypassed node that later coruns
            // hangs. The floor loses no release upstream delivers.
            None => {
                for &successor in node.successors() {
                    let next = graph.node(successor);
                    // The other half of the bypass, and the one the floor
                    // cannot reach: an *entered* successor is a node something
                    // bypassed the dependencies of, and the count it holds is
                    // its own outstanding work's. Spending one would resume the
                    // node before that work is done, so the release is reported
                    // through the path a panicking body takes, which leaves the
                    // node's references intact. The read is before the
                    // decrement, as `finish_job`'s is.
                    if is_entered(next) {
                        job.record_panic(Box::new(REENTERED));
                        continue;
                    }
                    // `update` is a compare-exchange loop where upstream has
                    // one `fetch_sub`, and that cost is deliberate: the cheaper
                    // shape -- subtract, then add the one back on a zero -- is
                    // not sound, two predecessors of one bypassed node being
                    // able to sit in that window at once.
                    let outstanding = next.join_counter().update(
                        Ordering::AcqRel,
                        Ordering::Acquire,
                        |outstanding| outstanding.saturating_sub(1),
                    );
                    if outstanding == 1 {
                        job.parent().count_queued(topology, 1);
                        self.update_cache(&mut cache, job.successor(successor));
                    }
                }
            }
            // Upstream's `CONDITION`/`MULTI_CONDITION` arm: only the
            // successors the body named, each scheduled outright. An index
            // naming no successor is how a branch ends.
            Some(conditions) => {
                let num_successors = node.num_successors();
                for &condition in conditions {
                    let Some(index) = selected_successor(condition, num_successors) else {
                        continue;
                    };
                    let successor = node.successors()[index];
                    // Upstream's "zeroing the join counter for invariant": a
                    // weak successor may be re-entered on a cycle before its
                    // strong predecessors run again, so nothing else would
                    // decrement its counter to the zero that means ready.
                    graph
                        .node(successor)
                        .join_counter()
                        .store(0, Ordering::Relaxed);
                    job.parent().count_queued(topology, 1);
                    self.update_cache(&mut cache, job.successor(successor));
                }
            }
        }

        // Upstream's `_tear_down_nonasync`. Spending the job here releases its
        // share of the taskflow's storage, which has to happen before the join
        // counter may end the run -- `Job::finish` says why -- and ends the
        // borrows this method works through, so the compiler is what stops
        // anything touching `node`, `graph` or the run afterwards. The
        // continuation held across this call was counted against this job's
        // parent before it was kept, so the counter spent here cannot be the
        // one that ends the run.
        self.finish_job(job.finish());

        // Upstream's `TF_INVOKE_CONTINUATION()`, which is a `goto` where this
        // is a return to the `while let` in `invoke`.
        cache
    }

    /// The point at which this worker is about to run `job`'s work: upstream's
    /// `Executor::_observer_prologue`, which notifies every registered observer of
    /// the entry. Upstream calls its own from inside each `_invoke_*_task`, so its
    /// window is the body alone; the one call site here is around the whole
    /// dispatch, which puts the scheduling of a nested graph inside the window of
    /// the kind that preempts for it.
    ///
    /// The empty-list check is the one thing this does with no observers
    /// registered: see the [`observer`](crate::observer) module docs' "Design"
    /// section for why that is lock-free and allocates nothing.
    fn observer_prologue(&self, job: &Job) {
        if self.shared.observers.is_empty() {
            return;
        }
        let (node, graph, _topology) = job.parts();
        let worker = WorkerView::new(self.worker());
        let task = TaskView::new(node, graph);
        for observer in self.shared.observers.snapshot() {
            observer.on_entry(worker, task);
        }
    }

    /// The point right after that: upstream's `Executor::_observer_epilogue`, which
    /// notifies every registered observer of the exit. Reached on the entry that
    /// *finishes* the node and on every entry that preempts it, the dispatch arm
    /// having returned either way.
    ///
    /// Call this one yourself, from inside a dispatch arm, if that arm's own
    /// tear-down can wake a waiter before this method's caller gets a chance
    /// to -- `executor::async` and `executor::dependent_async` do, around
    /// `Shared::finish_async` and a dependent task's successor release, each
    /// of which can let `Executor::wait_for_all` or `Executor::drop` return
    /// before the on_exit this call is for. Such an arm reports
    /// [`Epilogue::AlreadyRun`] from [`WorkerThread::invoke_one`]'s dispatch
    /// `match`, which is what stops that match's generic call from running a
    /// second time. `pub(crate)` for exactly that: every other dispatch arm
    /// reports [`Epilogue::Pending`] and is finished by that generic call
    /// instead, and has no reason to call this directly.
    pub(crate) fn observer_epilogue(&self, job: &Job) {
        if self.shared.observers.is_empty() {
            return;
        }
        let (node, graph, _topology) = job.parts();
        let worker = WorkerView::new(self.worker());
        let task = TaskView::new(node, graph);
        for observer in self.shared.observers.snapshot() {
            observer.on_exit(worker, task);
        }
    }

    /// Keeps `job` for this worker to run next, queueing whatever was being kept
    /// before it: upstream's `_update_cache`, line for line. Called once per
    /// successor found ready, so a node that readies `n` queues `n - 1`; the last
    /// wins the cache, which is the job this worker's own LIFO queue would have
    /// handed back first anyway. **The counting is the caller's**, and it has to
    /// happen before the job gets here: a cached job belongs to the same graph under
    /// the same parent as the one being torn down, so the `count_queued` before this
    /// call is what stops that tear-down from driving the parent's counter to zero
    /// while this worker still holds a job of it.
    fn update_cache(&self, cache: &mut Option<Job>, job: Job) {
        if let Some(previous) = cache.replace(job) {
            self.schedule(previous);
        }
    }

    /// Steals until something turns up, the worker is told to stop, or the
    /// attempts run out. Upstream's `_explore_task`.
    pub(crate) fn explore_task(&mut self) -> Stolen {
        // Fast path, upstream's: with no topology running every queue is empty
        // by the executor's own invariant, so the caller should sleep. Relaxed
        // is enough because this is a hint -- a topology that starts between
        // this load and the sleep is caught by the notifier's two-phase guard.
        if self.shared.num_topologies() == 0 {
            return Stolen::Nothing;
        }

        let max_victim = self.shared.num_queues();
        // Upstream's `MAX_STEALS`: a couple of passes over the queues before
        // admitting that there is nothing to steal.
        let max_steals = (max_victim + 1) * 2;

        let mut num_steals = 0;
        let mut victim = self.sticky_victim;

        loop {
            if let Some(job) = self.steal_from(victim) {
                self.sticky_victim = victim;
                return Stolen::Job(job);
            }

            // Empty: draw a new victim, never ourselves -- our own queue is the
            // one just drained.
            let draw = usize::try_from(self.rng.next_u32())
                .expect("a u32 fits in a usize on any target this crate supports");
            victim = victim_excluding(draw, self.id, max_victim);

            num_steals += 1;
            if num_steals > max_steals {
                // Still nothing: upstream yields on every attempt past this
                // point and gives up 150 attempts later, by which time the
                // notifier is the cheaper place to wait.
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
    /// down: upstream's `_wait_for_task`, whose `goto explore_task` is this
    /// loop. Returns the job to run next, or `None` when the worker should
    /// leave the scheduling loop.
    pub(crate) fn wait_for_task(&mut self) -> Option<Job> {
        loop {
            match self.explore_task() {
                Stolen::Job(job) => return Some(job),
                Stolen::Shutdown => return None,
                Stolen::Nothing => {}
            }

            // Everything below is inside the notifier's two-phase guard: the
            // predicate is "some queue is non-empty, or we are done", and
            // `prepare_wait` comes first so that a notification arriving while
            // we check cannot be lost, so each check that finds work cancels
            // the wait before it leaves.
            self.shared.notifier.prepare_wait(self.id);

            // Fast path, upstream's: with no topology running there is nothing
            // to scan for, so skip both sweeps and sleep.
            if self.shared.num_topologies() == 0 {
                if self.worker().is_done() {
                    self.shared.notifier.cancel_wait(self.id);
                    return None;
                }
                self.shared.notifier.commit_wait(self.id);
                continue;
            }

            // First condition: the shared buffers must be empty. Checked first
            // because a job in a buffer was pushed by a thread with no queue of
            // its own, and nobody else will come looking. The sticky victim
            // goes to the buffer that answered, that being where this worker
            // steals from next.
            if let Some(buffer) = self.shared.buffers.first_nonempty() {
                self.shared.notifier.cancel_wait(self.id);
                self.sticky_victim = self.shared.workers.len() + buffer;
                continue;
            }

            // Second: every other worker's queue must be empty. Our own is
            // skipped, as upstream skips it -- it was drained on the way here,
            // and anything pushed to it since was pushed by us.
            if let Some(victim) = self.first_nonempty_sibling() {
                self.shared.notifier.cancel_wait(self.id);
                self.sticky_victim = victim;
                continue;
            }

            // Third: we must still be wanted.
            if self.worker().is_done() {
                self.shared.notifier.cancel_wait(self.id);
                return None;
            }

            self.shared.notifier.commit_wait(self.id);
        }
    }

    /// Steals from queue `victim`, which is a worker queue below `num_workers`
    /// and a shared buffer above it. The two kinds being one index space is
    /// upstream's arrangement, and is what lets a victim be drawn with a single
    /// remainder.
    fn steal_from(&self, victim: usize) -> Option<Job> {
        let num_workers = self.shared.workers.len();
        if victim < num_workers {
            self.shared.workers[victim].thief.steal()
        } else {
            self.shared.buffers.steal_from(victim - num_workers)
        }
    }

    /// Returns the index of a sibling worker whose queue is not empty. Indexed
    /// rather than iterated, as upstream is: that race -- its `_spawn` still
    /// writing other workers' fields -- is gone here, every [`Worker`] being built
    /// before any thread starts, but `k -> k + (k >= id)` is still the cheapest way
    /// to walk every worker but oneself.
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

/// What one arm of [`WorkerThread::invoke_one`]'s dispatch `match` leaves for
/// that method to finish: the [`Release`] the dispatch computed, if the node
/// did not preempt itself, and whether the arm has already called
/// [`WorkerThread::observer_epilogue`] for this job on every path through it.
///
/// No arm may skip the choice -- there is no third constructor and no `Deref`
/// to a `bool` a caller could forget to read -- which is the point: a new
/// [`Handle`] variant is a new arm of that `match`, and every existing arm
/// already has to produce one of these two to type-check, so the arm cannot
/// compile without picking one.
pub(crate) enum Epilogue {
    /// This arm's own tear-down cannot wake a waiter, so
    /// [`WorkerThread::invoke_one`]'s generic call after the `match` still has
    /// to run.
    Pending(Option<Release>),
    /// This arm already called [`WorkerThread::observer_epilogue`] itself,
    /// from inside the dispatch function it delegates to, before a signal
    /// that tear-down sends can let a waiter (`Executor::wait_for_all`,
    /// `Executor::drop`) return -- [`Handle::Async`] and
    /// [`Handle::DependentAsync`] are the two arms that do. The generic call
    /// after the `match` must not run a second time.
    AlreadyRun(Option<Release>),
}

/// What the work a worker has just run leaves its node's successors owed.
///
/// Upstream needs no such type: it declares one `SmallVector<int> conds` above
/// its invoke `switch`, fills it in the two conditioner arms only, then keys a
/// second `switch` off the handle to decide whether to read it -- so "not a
/// conditioner" and "a conditioner that selected nothing" are told apart by
/// re-examining the handle rather than by the value. Here the value says so.
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
    /// The successor positions this release names, or `None` if it names none at
    /// all because every successor is to be released instead. The two cannot be
    /// confused, which is the point of the type: `Some(&[])` is a branch that
    /// ended, `None` an ordinary task.
    fn selected(&self) -> Option<&[i32]> {
        match self {
            Self::Strong => None,
            Self::Selected(condition) => Some(condition.as_slice()),
            Self::SelectedMany(conditions) => Some(conditions),
        }
    }
}

/// How many strong dependencies `node` has, out of the state word that holds
/// the count and the scheduler's state bits together: upstream's
/// `node->_nstate & NSTATE::STRONG_DEPENDENCIES_MASK`. The mask is not
/// decoration -- the state bits live above the count in the same word, so
/// re-arming a join counter from the unmasked word would add half a billion to
/// it, and the node would never reach zero again.
fn strong_dependencies(node: &Node) -> usize {
    usize::try_from(node.nstate().load(Ordering::Relaxed) & Node::STRONG_DEPENDENCIES_MASK)
        .expect("a u32 always fits in a usize on a 32-bit target or wider")
}

/// Whether `node` has been **entered**: a node of a kind that preempts itself,
/// carrying the `PREEMPTED` bit that kind's arm sets while its own nested work
/// is outstanding. The one thing a strong release or a
/// [`Runtime::schedule`](crate::Runtime::schedule) must know before it touches
/// a join counter: whether the count is a dependency count, which those two may
/// spend and zero, or one reference per piece of outstanding work, which they
/// may not. Both halves, because neither answers alone: `Node::nstate` is
/// public, and a subflow or module task holds nobody's reference until the entry
/// that preempts it sets the bit.
pub(crate) fn is_entered(node: &Node) -> bool {
    node.nstate().load(Ordering::Relaxed) & Node::PREEMPTED != 0 && preempts_itself(node.handle())
}

/// Whether a node of this kind is one the scheduler may leave `PREEMPTED`: the
/// four dynamic-tasking handles, which are the four arms of the dispatch that
/// can answer `None`. Exhaustive rather than one arm and a wildcard, for
/// [`WorkerThread::invoke_one`]'s own reason.
fn preempts_itself(handle: &Handle) -> bool {
    match handle {
        Handle::Subflow { .. }
        | Handle::Runtime(_)
        | Handle::Module(_)
        | Handle::AdoptedModule(_) => true,
        Handle::Static(_)
        | Handle::Placeholder
        | Handle::Condition(_)
        | Handle::MultiCondition(_)
        | Handle::Async(_)
        | Handle::DependentAsync(_) => false,
    }
}

/// Narrows one index a conditioner returned onto a successor position, or `None`
/// if it names no successor: upstream's
/// `cond >= 0 && static_cast<size_t>(cond) < node->_num_successors`, which is
/// two guards because its cast would turn a negative index into a huge positive
/// one, and `usize::try_from` is the same two with the cast removed. That a bad
/// index quietly selects nothing is load-bearing: `return -1` is how a
/// conditional branch is written to end.
fn selected_successor(condition: i32, num_successors: usize) -> Option<usize> {
    usize::try_from(condition)
        .ok()
        .filter(|index| *index < num_successors)
}

/// Maps a raw draw onto the queues other than `id`: upstream's trick, in two
/// lines -- a remainder over one fewer queue than there are, then a shift past
/// the hole left by `id`. Every victim but `id` is equally likely, and no
/// branch is needed to reject a self-steal.
fn victim_excluding(draw: usize, id: usize, max_victim: usize) -> usize {
    debug_assert!(
        max_victim >= 2,
        "an executor always has at least one worker queue and one buffer"
    );
    debug_assert!(id < max_victim - 1, "a worker id is below the queue count");

    let victim = draw % (max_victim - 1);
    victim + usize::from(victim >= id)
}

/// A seed for one worker's generator, drawn from the calling thread's identity:
/// upstream seeds from `std::hash<std::thread::id>`, which gives every worker of
/// every executor in the process a different stream without a shared counter.
/// The two halves of the hash are folded together rather than one of them being
/// dropped, a `ThreadId` being a small counter whose hash can carry what little
/// entropy it has in either half.
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
    use crate::{Executor, FlowBuilder, RunError, Semaphore, Taskflow};

    /// Upstream's `TF_THROW("can't release the semaphore more than its
    /// maximum value: ", _max_value)`, which escapes its worker loop: here
    /// the over-release is the failing *task's* own, so it is recorded like a
    /// panic in its body and reported from `wait`.
    ///
    /// The second half is the part a weaker implementation would get wrong by
    /// being helpful: the counter is left exactly as it was, so a release
    /// that was refused has not quietly happened anyway.
    #[test]
    fn an_over_release_fails_the_run_and_leaves_the_counter_alone() {
        let semaphore = Arc::new(Semaphore::new(1));
        let executor = Executor::new(2);
        let mut taskflow = Taskflow::new();

        // Releasing without acquiring, which upstream allows -- it is how a
        // task hands out a count another task will take -- so the failure is
        // the *maximum*, not the missing acquire.
        let task = taskflow.emplace(|| {});
        taskflow.task(task).release(&semaphore);

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("an over-release fails its run");
        assert!(matches!(error, RunError::Panicked(_)), "{error}");
        assert_eq!(
            error.to_string(),
            "a task panicked: can't release the semaphore more than its maximum value: 1"
        );

        assert_eq!(semaphore.value(), 1);
        assert_eq!(semaphore.max_value(), 1);
    }

    /// The same node, releasing a count it did take: the run succeeds and the
    /// counter comes back. Without this the case above would pass just as
    /// well against a hook that failed *every* release.
    #[test]
    fn a_task_that_acquires_and_releases_leaves_the_counter_as_it_found_it() {
        let semaphore = Arc::new(Semaphore::new(1));
        let executor = Executor::new(2);
        let mut taskflow = Taskflow::new();

        let task = taskflow.emplace(|| {});
        taskflow.task(task).acquire(&semaphore).release(&semaphore);

        executor.run(&taskflow).wait().expect("the run succeeds");
        assert_eq!(semaphore.value(), 1);
    }

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

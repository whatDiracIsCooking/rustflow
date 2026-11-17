//! Translated from `taskflow/core/runtime.hpp` (Taskflow v4.1.0).
//!
//! The executor's half of runtime tasking: upstream's `_invoke_runtime_task`
//! and the `std::function<void(Runtime&)>` overload of
//! `_invoke_runtime_task_impl`, which that header defines out of line, plus
//! the scheduling methods of `tf::Runtime` -- `schedule`, `corun`,
//! `corun_all` and `is_cancelled`. The `RUNTIME` arm of `_invoke` in
//! `taskflow/core/executor.hpp` is one line into [`invoke`] below, as the
//! module arm is into [`module`](crate::executor::module). The shell of
//! [`Runtime`](crate::Runtime) is `src/runtime.rs`, which says what the
//! handle holds and why it cannot hand back an executor.
//!
//! Upstream's `void(Runtime&, bool)` overload is the async shape -- the
//! second entry calls the body again with `true` so it can clean up after
//! its children -- and belongs with the `rt.async` family in milestone 5.
//!
//! # A runtime task is the parent of its own children
//!
//! The protocol is the preemption protocol the [`executor`](crate::executor)
//! module docs set out, with one difference from a module or subflow task:
//! what the node waits for is not a *graph* but however much asynchronous
//! work the body chose to spawn, so the count cannot be known before the
//! body has run. Upstream's answer is to make the body itself one reference:
//!
//! 1. the first entry sets `PREEMPTED | IMPLICITLY_ANCHORED` and bumps the
//!    node's own join counter by one, **before** the body runs -- a child the
//!    body spawns raises that same counter, and the bits are what tell the
//!    last child to re-queue this node;
//! 2. the body runs, under the `catch_unwind` that every other task body runs
//!    under;
//! 3. the first entry then spends its own reference. If it was the last one,
//!    nothing the body spawned is outstanding, so the bits come off and the
//!    node finishes like a static task. If it was not, the children still
//!    running own the node: it stays preempted, and the last of them to
//!    finish re-queues the job it was dequeued as.
//!
//! The `IMPLICITLY_ANCHORED` bit is what makes the node an anchor of last
//! resort while that is going on -- `Job::record_panic` in the crate-private
//! `executor::topology` is the walk that reads it, and issue #48 ported it
//! for this arm to be the first to set it. A payload that reaches the node's
//! slot is taken by [`Runtime::try_corun`]; one that nobody takes is dropped
//! by the next `set_up_graph` of that graph, which is upstream's behaviour
//! too.
//!
//! # Why the arm answers with a boolean rather than the state word
//!
//! Upstream returns a `bool` from `_invoke_runtime_task_impl` and says why in
//! a comment: the caller cannot read `node->_nstate` to find out whether the
//! node was preempted, because the instant the join counter above drops to
//! something non-zero a child may finish, re-queue this node and have another
//! worker clear those very bits -- so the caller's read would race the
//! second entry's write. The answer therefore has to be a value the first
//! entry computed while it still held a reference, which is exactly what the
//! [`Option<Release>`](Release) this arm returns is: `None` is upstream's
//! `true`.
//!
//! # What a corun waits for, and why nothing waits yet
//!
//! [`Runtime::corun`] waits for the node's join counter to come back to
//! **one** -- the body's own reference, the one step 1 added -- which is
//! upstream's predicate exactly. Until milestone 5 gives a `Runtime` the
//! `async` family, nothing can leave a child under a runtime node: a task
//! this body `schedule`s is counted against the node's *parent*, not against
//! the node, so it is a sibling rather than a child. So in this milestone
//! every corun returns at once and the preemption branch of [`invoke`] is
//! unreachable from the public API. Both are ported in full all the same, and
//! the cases at the bottom of this file drive the counter by hand to reach
//! them: an unreachable branch that is never exercised is how a latent hang
//! gets shipped.
//!
//! # Scheduling a sibling, and the same-graph limit
//!
//! [`Runtime::schedule`] is upstream's `schedule`: it puts an *active* task
//! -- a task of a running taskflow -- straight onto this worker's queue,
//! bypassing the graph's dependencies. Upstream takes a `tf::Task`, which is
//! a pointer to the node, and reaches the node's parent through it. A
//! [`Task`] here is an index into one arena, so the method is limited to a
//! task of **the same graph as the runtime task**, and that limit is what
//! makes the parent question answerable: a sibling's parent is this node's
//! own parent, which the job in hand already holds. See the method's own
//! docs, and the README's design decisions.

use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::Runtime;
use crate::executor::run_handle::RunError;
use crate::executor::topology::Job;
use crate::executor::worker::{Release, Worker, WorkerThread};
use crate::graph::{Node, RuntimeWork, Task};

/// The two bits a runtime task holds while its own work is outstanding:
/// upstream's `NSTATE::PREEMPTED | NSTATE::IMPLICITLY_ANCHORED`, which it
/// sets and clears together in all three of its runtime invokers.
const ENTERED: u32 = Node::PREEMPTED | Node::IMPLICITLY_ANCHORED;

/// Runs the runtime task `job` is for: upstream's `_invoke_runtime_task` and
/// the `void(Runtime&)` overload of `_invoke_runtime_task_impl`.
///
/// `None` means the node has been preempted -- work the body spawned is still
/// running, and the last of it to finish will re-queue this job -- and is
/// what keeps `WorkerThread::invoke` from re-arming the node or releasing
/// successors that have the body's own children still to wait for. The module
/// docs say why that answer is a return value rather than a bit the caller
/// could read back.
///
/// A panic out of the body is recorded exactly as a static task's is, through
/// [`Job::record_panic`]: delivered to whatever is waiting for the graph this
/// node is part of, and cancelling the rest of that graph. The bookkeeping
/// below still runs, because the node has been entered and the counters have
/// to balance whatever the body did -- which is upstream's arrangement too,
/// its exception handler being a statement inside the first entry rather than
/// a return.
pub(crate) fn invoke(worker: &mut WorkerThread, job: &Job, work: &RuntimeWork) -> Option<Release> {
    let node = job.node();

    // Upstream's `if((node->_nstate & NSTATE::PREEMPTED) == 0)`: the first
    // entry, which is the one that runs the body.
    if node.nstate().load(Ordering::Relaxed) & Node::PREEMPTED == 0 {
        // Both before the body, as upstream sets them: a child the body
        // spawns may finish before the body returns, and these are what tell
        // it that this node is waiting to be resumed. Relaxed, as upstream's
        // plain `|=` is -- what orders a child's read of them is the queue it
        // came out of.
        node.nstate().fetch_or(ENTERED, Ordering::Relaxed);

        // The body's own reference, upstream's `fetch_add(1, release)`: it is
        // what keeps the counter off zero while the body is still spawning,
        // so that a child which finishes early cannot resume a node that has
        // not finished starting.
        node.join_counter().fetch_add(1, Ordering::Release);

        run_body(worker, job, node, work);

        // Upstream's `fetch_sub(1, acq_rel) == 1`: this reference spent, and
        // the question of whether anything else holds one. The acquire half
        // is what makes everything the children published visible to the
        // frame that falls through here.
        if node.join_counter().fetch_sub(1, Ordering::AcqRel) != 1 {
            // Upstream's `return true`. The node keeps both bits until the
            // second entry clears them.
            return None;
        }
    }

    // Upstream's two `_nstate &= ~(PREEMPTED | IMPLICITLY_ANCHORED)` sites,
    // which are the same line twice: the first entry that found itself last
    // clears the bits it set, and the second entry clears the ones the first
    // one left behind.
    node.nstate().fetch_and(!ENTERED, Ordering::Relaxed);
    Some(Release::Strong)
}

/// Runs the body against a handle over `node`, recording a panic out of it as
/// this job's failure.
///
/// Upstream's `TF_EXECUTOR_EXCEPTION_HANDLER(worker, node, { work(rt); })`,
/// which `WorkerThread::invoke` spells the same way for a static task:
/// `AssertUnwindSafe` is the honest annotation, since the state a panicking
/// body can leave half-written is the caller's own, reachable only through
/// what the closure captured.
///
/// A function of its own so that the mutable borrow the handle takes of the
/// worker's scheduling loop ends where the body does, which is before the arm
/// above spends the node's own reference.
fn run_body(worker: &mut WorkerThread, job: &Job, node: &Node, work: &RuntimeWork) {
    // The worker record the handle hands back, taken through a share of the
    // executor's state rather than through `worker`: the handle holds that
    // value mutably, so a borrow of the record *out of* it would alias the
    // handle's own. One atomic pair per runtime task, which is what keeps
    // `Runtime::worker` able to lend the record out for the whole invocation.
    let shared = Arc::clone(&worker.shared);
    let record: &Worker = &shared.workers[worker.id];

    let mut runtime = Runtime::new(record, node, job, worker);
    if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| work.invoke(&mut runtime))) {
        job.record_panic(payload);
    }
}

impl Runtime<'_> {
    /// Schedules `task` onto this worker's queue at once, bypassing the
    /// graph's dependencies: upstream's `Runtime::schedule`.
    ///
    /// `task` must be an *active* task -- a task of the taskflow this runtime
    /// task is itself part of, which is to say of a run that is in flight. It
    /// may or may not be running already; what this does is put it in the
    /// queue of the worker running this body, which is why upstream describes
    /// it as scheduling "immediately".
    ///
    /// # The same-graph limit
    ///
    /// `task` has to be a task of **the same graph as the runtime task**.
    /// Upstream's `tf::Task` is a pointer to a node, so its `schedule` can
    /// reach any node of any graph and read the parent and topology stored on
    /// it; a [`Task`] here is an index into one arena, and the thing that
    /// resolves it is the graph this job already holds. The limit is
    /// therefore what makes the accounting answerable at all: the parent to
    /// count the scheduled task against is this node's own parent -- the run,
    /// for a top-level task, and the enclosing node for a task of a nested
    /// graph -- which is upstream's `node->_parent ? .. : node->_topology`
    /// with the two cases decided by the job rather than by a null check.
    ///
    /// The limit cannot be fully checked, and that is worth being plain
    /// about: a `Task` is a bare index, so a task of *another* graph whose
    /// index happens to be live in this one would schedule this graph's node
    /// at that index. What is checked is every index this graph does not
    /// have, which panics as any stale index does, and the README records the
    /// decision. Upstream has two unchecked caveats of its own here -- that
    /// only the parent worker of the runtime may call this, which the
    /// `&mut self` on a handle that cannot leave its own invocation is this
    /// port's answer to, and that scheduling a *runtime* task is "currently
    /// not supported", which this port does not diagnose either.
    ///
    /// # Panics
    ///
    /// Panics if `task` is not a live task of this runtime task's own graph
    /// -- out of range, erased, or a task of another graph with an index this
    /// one does not have. The panic is
    /// [`Graph::node`](crate::Graph::node)'s, and reaches the caller as the
    /// failure of this runtime task, since it happens inside the body.
    pub fn schedule(&mut self, task: Task) {
        // Upstream's comment, and the invariant it names: "when scheduling a
        // task, the task must have zero dependency (join counter is 0) or we
        // can encounter bug when inserting a nested flow (e.g., module
        // task)". A stored zero rather than an arithmetic one, for the reason
        // a conditioner's selected successor gets one -- nothing is going to
        // decrement this counter to the zero that means ready.
        let target = self.job.graph().node(task);
        target.join_counter().store(0, Ordering::Relaxed);

        // Before the push, as every other count of a queued job is: the job
        // may run and finish before this call returns, and a parent that had
        // not been told about it could reach zero with the job still in
        // flight.
        self.job.parent().count_queued(self.job.topology(), 1);

        // Upstream's `_executor._schedule(_worker, node)`: this worker's own
        // queue. `Job::successor` is the job of a task of the same graph,
        // same run and same parent -- which is what a sibling is, whatever
        // the edges say.
        self.thread.schedule(self.job.successor(task));
    }

    /// Waits for everything this runtime task has spawned, reporting a
    /// failure under it rather than raising it: upstream's `corun`, in the
    /// `try_` shape issue #48 established.
    ///
    /// The calling worker stays in the work-stealing loop while it waits, for
    /// the reason the [`corun`](crate::executor::corun) module docs give: a
    /// worker that parked here would be a worker the work it is waiting for
    /// cannot use. The predicate is upstream's -- the node's join counter
    /// back to one, that one being this body's own reference -- so a body
    /// that has spawned nothing returns immediately, which in this milestone
    /// is every body there is.
    ///
    /// The node is explicitly anchored for the length of the wait, which is
    /// what makes a failure under it *this frame's*: the panic walk in
    /// `Job::record_panic` stops at the first explicit anchor it meets, so
    /// the payload is handed to the thread that is about to look for it
    /// rather than to the run. Taking the payload clears the node's slot, as
    /// upstream's `_rethrow_exception` does, so a second corun of the same
    /// body starts clean.
    ///
    /// # Errors
    ///
    /// [`RunError::Panicked`] if work spawned by this runtime task panicked,
    /// carrying what it panicked with. Only the first such failure is
    /// reported, as everywhere else in the crate.
    pub fn try_corun(&mut self) -> Result<(), RunError> {
        let node = self.node;
        {
            // Taken before the wait, and dropped before the payload is read
            // -- `corun_graph` in `executor::corun` takes the same guard in
            // the same order, and says why it has to come first.
            let _anchor = node.base().anchor_explicitly();
            self.thread
                .corun_until(|| node.join_counter().load(Ordering::Acquire) == 1);
        }

        match node.base().take_panic() {
            Some(payload) => Err(RunError::Panicked(payload)),
            None => Ok(()),
        }
    }

    /// Waits for everything this runtime task has spawned, re-raising a
    /// failure under it on this worker: upstream's `corun`.
    ///
    /// [`Runtime::try_corun`] with the failure raised instead of returned,
    /// which is the pair [`Executor::corun`](crate::Executor::corun) and
    /// [`Executor::try_corun`](crate::Executor::try_corun) established and
    /// the shape that composes: a body that wrapped this call in nothing has
    /// the failure propagate out of itself, where the executor catches it as
    /// this task's own failure and the run reports it.
    ///
    /// # Panics
    ///
    /// Panics with the payload of the first piece of work spawned by this
    /// runtime task to panic. The payload keeps its identity, because it goes
    /// to [`std::panic::resume_unwind`] rather than to a fresh `panic!`.
    pub fn corun(&mut self) {
        match self.try_corun() {
            Ok(()) => {}
            // Upstream's `_node->_rethrow_exception()`, which is the last
            // thing its `corun` does for the same reason it is the last thing
            // here: the anchor guard is gone and the slot is empty, so
            // nothing this frame owed is still outstanding when the unwind
            // starts.
            Err(RunError::Panicked(payload)) => panic::resume_unwind(payload),
        }
    }

    /// [`Runtime::corun`], under upstream's older name for it.
    ///
    /// Upstream's `corun_all` is one line -- `corun();` -- kept "just an
    /// alias for legacy purpose", in its words, and it is ported because its
    /// own tests and documentation still use it.
    pub fn corun_all(&mut self) {
        self.corun();
    }

    /// Whether this runtime task's run has been cancelled or something above
    /// it has failed: upstream's `is_cancelled`, which is
    /// `Node::_is_parent_cancelled`.
    ///
    /// The one query a long-running body can use to stop early, and the whole
    /// of what cancellation can do to a task that is already running: nothing
    /// interrupts a body, so a body that means to be cancellable has to ask.
    /// Both terms of upstream's condition are here, as
    /// `Job::is_parent_cancelled` has them -- the run's own cancellation
    /// bits, and the estate of every parent above this job -- which is why a
    /// runtime task inside a nested graph sees a failure in that graph as
    /// well as a cancel of the whole run.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.job.is_parent_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, OnceLock};

    use super::*;
    use crate::Executor;
    use crate::executor::queue::WorkerQueue;
    use crate::executor::topology::{Cancellable, GraphOwner, Parent, Topology, set_up_graph};
    use crate::prelude::*;

    /// A worker thread with a queue nobody else holds the other end of, for
    /// driving the scheduler one job at a time from the test thread.
    ///
    /// The same helper the cases in `executor::topology` take, for the reason
    /// they give: the pool's real threads must not be able to steal what
    /// these cases queue, and they would not look anyway, since nothing here
    /// counts a topology.
    fn test_worker(executor: &Executor) -> WorkerThread {
        WorkerThread::new(Arc::clone(&executor.shared), 0, WorkerQueue::new())
    }

    /// A run of `taskflow`'s graph, armed and accounted for but with nothing
    /// queued: what a case here invokes a job of.
    ///
    /// `on_finish` is the run policy's callback, which the tear-down calls
    /// once the run's join counter reaches zero -- the one observable a
    /// hand-driven run has for "the accounting balanced".
    fn run_of(taskflow: &Taskflow, on_finish: impl FnOnce() + Send + 'static) -> Arc<Topology> {
        let flow = taskflow.flow();
        let topology = Arc::new(Topology::new(|| true, on_finish, Arc::clone(flow)));
        let sources = set_up_graph(flow.graph());
        Parent::Run.count_queued(&topology, sources);
        topology
    }

    /// The job of `taskflow`'s one source task, under the run itself.
    ///
    /// Every case here builds a graph whose runtime task is the only source,
    /// which is what makes this unambiguous: a [`Job`] holds its task index
    /// privately, so the only way to name one is to leave the scheduler one
    /// choice.
    fn source_job(taskflow: &Taskflow, topology: &Arc<Topology>) -> Job {
        let owner = GraphOwner::flow(taskflow.flow());
        let parent = Parent::Run;
        let mut sources = Job::sources(&owner, topology, &parent);
        let job = sources.next().expect("the graph has a source task");
        assert!(sources.next().is_none(), "the cases here have one source");
        job
    }

    /// The state bits of `task`, masked to the two this arm sets.
    fn entered(taskflow: &Taskflow, task: Task) -> u32 {
        taskflow.graph().node(task).nstate().load(Ordering::Relaxed) & ENTERED
    }

    /// A body that returns without spawning anything is the whole of a
    /// runtime task in this milestone: both bits come off on the first entry
    /// and the node finishes like a static task, with its successors released
    /// exactly once.
    ///
    /// "Released" is counted by the successor itself rather than found on the
    /// queue, which is what #62 changed about every case in this module: a
    /// released successor is kept out of the queue and run as the worker's
    /// continuation, so by the time `invoke` returns the release has already
    /// become a run.
    #[test]
    fn a_body_that_spawns_nothing_clears_its_bits_and_releases_once() {
        let runs = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();
        let rt = {
            let runs = Arc::clone(&runs);
            taskflow.emplace_runtime(move |_: &mut Runtime<'_>| {
                runs.fetch_add(1, Ordering::Relaxed);
            })
        };
        let after = {
            let released = Arc::clone(&released);
            taskflow.emplace(move || {
                released.fetch_add(1, Ordering::Relaxed);
            })
        };
        taskflow.task(rt).precede([after]);

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, || {});
        worker.invoke(source_job(&taskflow, &topology));

        assert_eq!(runs.load(Ordering::Relaxed), 1, "the body ran once");
        assert_eq!(entered(&taskflow, rt), 0, "both bits came off");
        assert_eq!(
            taskflow
                .graph()
                .node(rt)
                .join_counter()
                .load(Ordering::Relaxed),
            0,
            "the node was re-armed to its own dependency count"
        );
        assert_eq!(
            released.load(Ordering::Relaxed),
            1,
            "the one successor was released, and released once"
        );
        assert!(
            worker.pop().is_none(),
            "and nothing was left queued: the successor ran here, as this \
             worker's continuation"
        );
    }

    /// The preemption branch, which the public API cannot reach until
    /// milestone 5 gives a `Runtime` the `async` family: the body raises its
    /// own node's join counter, exactly as a child spawned under the node
    /// would, so the first entry is not the last reference and the node is
    /// left preempted with nothing released. A hand-driven decrement to zero
    /// is the last child's tear-down, and the second entry clears the bits
    /// and finishes the node.
    #[test]
    fn a_body_that_leaves_work_outstanding_is_preempted_and_resumed() {
        let runs = Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();
        let rt = {
            let runs = Arc::clone(&runs);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                runs.fetch_add(1, Ordering::Relaxed);
                // What `rt.silent_async` will do: one more reference to the
                // node, held by work that has not finished.
                runtime.node.join_counter().fetch_add(1, Ordering::Relaxed);
            })
        };
        let released = Arc::new(AtomicUsize::new(0));
        let after = {
            let released = Arc::clone(&released);
            taskflow.emplace(move || {
                released.fetch_add(1, Ordering::Relaxed);
            })
        };
        taskflow.task(rt).precede([after]);

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, || {});
        let job = source_job(&taskflow, &topology);
        // The job the node was dequeued as, which is what the last child
        // re-queues to resume it.
        let resume = job.clone();

        worker.invoke(job);

        let node = taskflow.graph().node(rt);
        assert_eq!(runs.load(Ordering::Relaxed), 1, "the body ran once");
        assert_eq!(entered(&taskflow, rt), ENTERED, "the node is preempted");
        assert_eq!(
            node.join_counter().load(Ordering::Relaxed),
            1,
            "one piece of spawned work is still outstanding"
        );
        assert!(
            worker.pop().is_none(),
            "a preempted node releases nothing: its successors wait for the \
             work the body spawned"
        );
        assert_eq!(
            released.load(Ordering::Relaxed),
            0,
            "and it released nothing to run here either"
        );

        // The tear-down of the last child, by hand: `WorkerThread::finish_job`
        // reads the parent's state word before the decrement and, finding
        // `PREEMPTED`, re-queues the job above.
        assert_eq!(
            node.join_counter().fetch_sub(1, Ordering::AcqRel),
            1,
            "the last child drives the counter to zero"
        );
        worker.invoke(resume);

        assert_eq!(
            runs.load(Ordering::Relaxed),
            1,
            "the second entry resumes the node rather than running the body \
             again"
        );
        assert_eq!(
            entered(&taskflow, rt),
            0,
            "the second entry clears both bits"
        );
        assert_eq!(
            released.load(Ordering::Relaxed),
            1,
            "and releases the successor, once -- which since #62 means it ran \
             here as the continuation rather than went on the queue"
        );
        assert!(worker.pop().is_none(), "so nothing was left queued");
    }

    /// A corun with nothing spawned is over before it starts: the counter is
    /// already at the body's own one reference. That is every corun in this
    /// milestone, which is why the branch above is tested by hand rather than
    /// through one of these.
    ///
    /// The explicit anchor is a guard, so it has to be gone by the time the
    /// call returns -- a node left anchored would catch a failure of a
    /// *later* nested graph that nobody is waiting for.
    #[test]
    fn a_corun_with_nothing_spawned_returns_at_once() {
        let mut taskflow = Taskflow::new();
        let rt = taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            runtime.try_corun().expect("nothing was spawned to fail");
            // Both shapes, and upstream's alias for the second.
            runtime.corun();
            runtime.corun_all();
            assert!(
                !runtime.node.base().is_explicitly_anchored(),
                "a corun that has returned holds no anchor, so the body is \
                 free to corun again"
            );
        });

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, || {});
        worker.invoke(source_job(&taskflow, &topology));

        assert_eq!(entered(&taskflow, rt), 0, "the node finished");
    }

    /// A payload in the node's own slot comes back from `try_corun`, which is
    /// what the implicit anchor is *for*: the walk in `Job::record_panic`
    /// leaves a failure there when the node that preempted itself is the
    /// closest thing to it with something to look at, and this is the frame
    /// that looks.
    ///
    /// The payload is put there the way that walk puts one -- claim the slot,
    /// then fill it -- because the walk cannot be driven from here without
    /// the `async` family that gives a runtime node children at all: a chain
    /// rooted at a run hands the payload to the run, which is anchored by
    /// definition.
    #[test]
    fn a_panic_under_an_implicit_anchor_comes_back_from_a_corun() {
        let mut taskflow = Taskflow::new();
        let rt = taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let base = runtime.node.base();
            assert!(
                base.is_implicitly_anchored(),
                "a running runtime task is an anchor of last resort"
            );

            assert!(base.claim_panic(), "the slot is this caller's");
            base.keep_panic(Box::new("boom"));

            let error = runtime
                .try_corun()
                .expect_err("the payload under this anchor is this frame's");
            assert_eq!(error.to_string(), "a task panicked: boom");

            // Taken, so the slot is empty again -- upstream's
            // `_rethrow_exception` clears it for the same reason.
            runtime.try_corun().expect("the slot was emptied");
        });

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, || {});
        worker.invoke(source_job(&taskflow, &topology));

        assert_eq!(entered(&taskflow, rt), 0, "the node finished");
    }

    /// `is_cancelled` reads the run, which is the first term of upstream's
    /// `_is_parent_cancelled`: a body sees `false` while the run is live and
    /// `true` the moment it is cancelled, with no scheduling in between --
    /// cancellation is not preemptive, so asking is the only way a running
    /// body learns of it.
    #[test]
    fn a_body_sees_its_run_cancelled_while_it_runs() {
        let state: Arc<OnceLock<Arc<Cancellable>>> = Arc::default();
        let seen = Arc::new(AtomicBool::new(false));

        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime({
            let state = Arc::clone(&state);
            let seen = Arc::clone(&seen);
            move |runtime: &mut Runtime<'_>| {
                assert!(!runtime.is_cancelled(), "the run is live");
                let state = state.get().expect("filled before the job is run");
                assert!(state.cancel(), "the run had not finished");
                seen.store(runtime.is_cancelled(), Ordering::Relaxed);
            }
        });

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, || {});
        assert!(
            state.set(Arc::clone(topology.cancellable())).is_ok(),
            "the slot is filled once"
        );

        worker.invoke(source_job(&taskflow, &topology));
        assert!(
            seen.load(Ordering::Relaxed),
            "the body saw the cancellation"
        );
    }

    /// `schedule` puts a sibling on *this worker's* queue with a zeroed join
    /// counter and counts it against this node's parent -- and the last of
    /// those is what the run policy's callback pins: a scheduled job the
    /// parent was never told about would let the run finish while that job
    /// was still queued.
    ///
    /// The shape is upstream's `Runtime.Schedule.ModuleTask` reduced to the
    /// scheduler: `other` has a strong dependency on `gate`, and the body
    /// schedules it anyway, which is the dependency bypass the method is for.
    ///
    /// The zeroed counter is the one thing this case no longer reads for
    /// itself, because since #62 `gate` has already run by the time the case
    /// looks: the comment at the drop site has why, and issue #98 has the
    /// wrap that makes the value unreadable.
    #[test]
    fn a_scheduled_sibling_is_queued_here_and_counted_against_the_parent() {
        let order = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicBool::new(false));
        let mut taskflow = Taskflow::new();

        let other = {
            let order = Arc::clone(&order);
            taskflow.emplace(move || {
                order.fetch_add(1, Ordering::Relaxed);
            })
        };
        let gate = {
            let order = Arc::clone(&order);
            taskflow.emplace(move || {
                order.fetch_add(1, Ordering::Relaxed);
            })
        };
        let rt = taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            runtime.schedule(other);
        });
        taskflow.task(rt).precede([gate]);
        taskflow.task(gate).precede([other]);

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, {
            let finished = Arc::clone(&finished);
            move || finished.store(true, Ordering::Relaxed)
        });

        worker.invoke(source_job(&taskflow, &topology));

        // `other`'s own join counter is not read here any more. Since #62 the
        // released successor `gate` runs as this worker's continuation, and
        // `gate` then decrements the counter `schedule` had zeroed, which
        // wraps it: that wrap is issue #98's and is not this case's subject.
        assert_eq!(
            order.load(Ordering::Relaxed),
            1,
            "the released successor ran here as the continuation, and the \
             scheduled sibling did not: it went on the queue"
        );
        assert!(!finished.load(Ordering::Relaxed), "the run is not over");

        // What is left on this worker's queue is the job the body scheduled.
        // Draining it is the rest of the run.
        let mut drained = 0;
        while let Some(job) = worker.pop() {
            worker.invoke(job);
            drained += 1;
        }

        assert_eq!(drained, 1, "the scheduled sibling, and nothing else");
        assert_eq!(order.load(Ordering::Relaxed), 2, "both bodies ran");
        assert!(
            finished.load(Ordering::Relaxed),
            "the run finished exactly when its last job did, so the \
             scheduled job was counted against the parent"
        );
    }
}

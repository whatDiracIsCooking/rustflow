//! Translated from `Executor::_invoke_subflow_task` and `Subflow::join` in
//! `taskflow/core/executor.hpp` (Taskflow v4.1.0).
//!
//! The executor's half of subflow tasking: the dispatch arm that runs a
//! subflow task, the explicit join the body can ask for, and the one function
//! that decides what becomes of a subgraph once its run is over. The builder
//! half of [`Subflow`] is `src/subflow.rs`, and the two meet at the lease --
//! see below.
//!
//! Upstream's shape, which [`invoke`] follows: build the handle, run the body,
//! and -- if the body left the subflow joinable and its graph non-empty --
//! set `PREEMPTED` on the node and schedule the subgraph under it. The second
//! entry clears the bit, and the subgraph is cleared unless the body asked to
//! retain it. [`Subflow::join`] is the same nested run started from inside the
//! body instead, through [`corun_graph`](crate::executor::corun) rather than
//! through the preemption protocol, which is why upstream's comment says the
//! implicit join is the faster of the two.
//!
//! # The lease, and who puts the subgraph back
//!
//! Scheduling a graph needs whatever *owns* it, because the jobs of a graph
//! own what they name; a subgraph is owned by the `Mutex` in
//! [`Handle::Subflow`](crate::Handle::Subflow), and a `MutexGuard` is not
//! something a job can own. Issue #74 settled that for all four kinds of
//! nested graph at once: the subgraph is **moved out** of its lock for the
//! length of its run -- the *lease* -- and held by every job of it through one
//! shared allocation, so no lock is on the scheduler's hot path at all. The
//! `executor::topology` docs have the three options that were on the table and
//! why a lock on that path is not one of them.
//!
//! What was left for this issue is the policy: who puts the graph back, and
//! whether it goes back at all. The answer is **whoever holds the last share
//! of the lease**, through [`return_subgraph`] -- called from `Inside`'s
//! `Drop` in `executor::topology` for a graph that was scheduled, and
//! directly by [`invoke`] for a graph that was not -- and the reason it cannot
//! be the frame that is waiting for the nested run is worth writing down,
//! because the obvious design does not work:
//!
//! > A job of a graph nested *inside* the subgraph decrements the join counter
//! > of its parent node, and that node is a node **of the subgraph** -- so its
//! > counter lives inside the leased graph, and the job must hold the lease to
//! > reach it. It releases that hold *after* the decrement, which is the
//! > decrement that can end the subgraph's own run. So at the instant a
//! > nested run is over, a straggling share of the lease may still exist for a
//! > few instructions, and no holder can prove it is the last one:
//! > `Arc::into_inner` by the waiting frame would fail at random.
//!
//! A `Drop` has no such problem -- the last share is whichever one it turns
//! out to be -- and three consequences are worth naming:
//!
//! - **a subgraph that is not retained never touches a lock.** Dropping the
//!   leased graph *is* upstream's `g.clear()`, so the default path is a free
//!   function of ownership rather than a second trip through the `Mutex`;
//! - **a retained subgraph goes back under `try_lock`**, never a blocking
//!   lock, because the thread holding the last share is a worker: it may be
//!   about to run anything, and a worker that waits on a lock a task body
//!   holds is a worker that run cannot use. A retained subgraph is therefore
//!   lost if the node's slot happens to be busy at that instant -- a
//!   concurrent [`dump`](crate::Taskflow::dump), in practice;
//! - **an explicit join takes the lease back itself** where it can
//!   (`GraphOwner::take_lease`, which is the `Arc::into_inner` above with the
//!   failure allowed for), so that the body sees the graph it built once
//!   `join` returns and the policy is applied once, by [`invoke`], with the
//!   retain bit the body finally settled on. When the straggler above wins
//!   that race -- which takes a subgraph with a nested graph of its own in it
//!   -- the `Drop` applies the policy early instead and the body finds an
//!   empty graph; [`Subflow::set_retain`] says what that means for a caller.
//!
//! Which leaves the question that reading the two paths side by side raises:
//! an **implicit** join recovers the lease through the `Drop` alone, because
//! there is no frame left to recover it with -- the arm set `PREEMPTED` and
//! returned long before the subgraph finished -- where an explicit join also
//! has `take_lease`. That does not make the implicit join the riskier of the
//! two, and it is worth saying which way round it is, because the
//! `take_lease` on the explicit path is not there for retention at all: it is
//! there so the *body* can go on reading the graph it built. Both paths pass
//! through exactly one [`return_subgraph`] per entry, and therefore one
//! `try_lock` that a retained graph can lose -- the `Drop`'s, around the
//! moment the node is resumed, for the implicit join, and [`invoke`]'s tail,
//! the instant the body returns, for the explicit one.
//!
//! # Only the spawning worker may join
//!
//! Upstream states the rule and leaves it at that. Here it is a type: a
//! [`Subflow`] holds `&mut WorkerThread`, which only the dispatch arm running
//! the body can lend, and the guard it holds besides is not [`Send`]. So the
//! handle cannot reach another thread to be joined from one, and the corun
//! loop a join enters is the very loop that worker was already in.
//!
//! # A failure under a join
//!
//! A task of the subgraph that panics reports to whatever is waiting for the
//! graph it belongs to, which for an explicit join is the node itself: the
//! join anchors it for the length of the corun, so the payload comes back out
//! of [`Subflow::join`] -- re-raised -- or out of [`Subflow::try_join`] as an
//! error, which is the pair #48 established for `corun`. For the implicit
//! join nothing is waiting, so the walk marks the node on its way past and
//! carries the payload up to the run, whose `wait` reports it; the rest of the
//! subgraph is cancelled either way. Upstream's `Exception.SubflowTask` and
//! `Exception.JoinedSubflow1` are ported in `tests/exceptions.rs`.

use std::any::Any;
use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Mutex;
use std::sync::TryLockError;
use std::sync::atomic::Ordering;

use crate::executor::corun::corun_graph;
use crate::executor::run_handle::RunError;
use crate::executor::topology::{Job, Nested};
use crate::executor::worker::{Release, WorkerThread};
use crate::graph::{Graph, Handle, Node, SubflowWork};
use crate::subflow::Subflow;

/// Runs the subflow task `job` is for: upstream's `_invoke_subflow_task`.
///
/// `None` means the node has been preempted -- the graph its body built is
/// running, and the last job of that graph will resume it -- which is what
/// keeps `WorkerThread::invoke` from releasing successors that have a whole
/// subgraph still to wait for. Upstream says the same thing by returning
/// `true`.
///
/// `work` and `subgraph` are the two halves of the node's own handle, which
/// the dispatch arm already had in hand: upstream reaches them as
/// `std::get_if<Node::Subflow>(&node->_handle)`.
pub(crate) fn invoke(
    worker: &mut WorkerThread,
    job: &Job,
    work: &SubflowWork,
    subgraph: &Mutex<Graph>,
) -> Option<Release> {
    let node = job.node();

    // Upstream's `else { node->_nstate &= ~NSTATE::PREEMPTED; }`: the second
    // entry, with the subgraph finished and this node free to finish too.
    // Upstream clears the subgraph here as well; this port has already done
    // it, because the lease went home the moment the nested run ended -- see
    // the module docs.
    if node.nstate().load(Ordering::Relaxed) & Node::PREEMPTED != 0 {
        node.nstate().fetch_and(!Node::PREEMPTED, Ordering::Relaxed);
        return Some(Release::Strong);
    }

    // Upstream's `Subflow sf(*this, worker, node, g)` and the body under its
    // `TF_EXECUTOR_EXCEPTION_HANDLER`, whose `_process_exception` is
    // `Job::record_panic`: the failure goes to whatever is waiting for the
    // graph this node is part of, cancels the rest of it, and is never raised
    // on a worker thread. `AssertUnwindSafe` is the honest annotation, as it
    // is for a static task: the state a panicking body can leave half-written
    // is the user's own.
    //
    // A panic does *not* skip what follows, because upstream's handler is the
    // body of its first switch and does not return: a body that panicked
    // after emplacing tasks still has them scheduled, and they are then
    // skipped one by one as the children of a node the failure marked. That
    // is what makes upstream's `Exception.SubflowTask` report the body's
    // failure rather than its children's.
    let (joinable, graph) = {
        let mut subflow = Subflow::new(worker, job, subgraph);
        if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| work.invoke(&mut subflow))) {
            job.record_panic(payload);
        }

        // Upstream reads `sf.joinable()` and `g.empty()` with the handle still
        // alive, which this port cannot: taking the lease needs the guard that
        // handle is holding. So the lease is taken first and the questions are
        // asked of what came out of it.
        (subflow.joinable(), subflow.take_subgraph())
    };

    // Upstream's `if(sf.joinable() && !g.empty())`, widened to the graph that
    // has nothing to *start* at -- an empty one, or one whose every node is in
    // a cycle -- for the reason `executor::module` gives at length: scheduling
    // a sourceless graph queues no job, so nothing would ever resume a node
    // left `PREEMPTED` for it and the run would hang.
    if joinable && has_source_task(&graph) {
        let owner = job.graph_inside(Some(graph));

        // Before the graph is scheduled, because a child may finish before
        // this call returns and the bit is what tells the last one to re-queue
        // this node.
        node.nstate().fetch_or(Node::PREEMPTED, Ordering::Relaxed);

        // Upstream's `_schedule_graph(worker, g, node->_topology, node)`,
        // whose `node` argument is a `Nested` here: the subgraph's jobs count
        // this node's join counter down, and what the last of them re-queues
        // is the *job* the node was dequeued as rather than the node itself.
        let parent = Nested::new(Some(job.clone()));
        worker.shared.schedule_graph(
            Some(worker),
            &owner,
            job.topology_share(),
            &parent.as_parent(),
        );
        return None;
    }

    // Nothing to run: a body that built nothing, or one that joined its
    // subflow itself and so has already had its nested run. Either way the
    // graph is in hand rather than in the node, so upstream's
    // clear-unless-retain is the same call the lease makes when it goes home.
    return_subgraph(node, graph);
    Some(Release::Strong)
}

/// Puts a leased subgraph back into `node`, or drops it: upstream's
/// `if((node->_nstate & NSTATE::RETAIN_SUBFLOW) == 0) g.clear();`.
///
/// Called by whoever holds the last share of a lease -- `Inside`'s `Drop` in
/// `executor::topology` for a subgraph that was scheduled, [`invoke`] for one
/// that was not -- which the module docs argue is the only code that *can* be
/// sure the graph is free to move.
///
/// Dropping the graph is upstream's `clear()`, which is why the default needs
/// no lock: the storage a recursive subflow would otherwise accumulate is
/// freed by the lease ending, not by a second trip through the `Mutex`.
///
/// # Panics
///
/// Panics if `node` is not a subflow node, which cannot happen: the only
/// graph this port leases is a subflow's subgraph.
pub(crate) fn return_subgraph(node: &Node, graph: Graph) {
    if node.nstate().load(Ordering::Relaxed) & Node::RETAIN_SUBFLOW == 0 {
        // Upstream's `g.clear()`, as a drop.
        return;
    }

    let Handle::Subflow { subgraph, .. } = node.handle() else {
        unreachable!("only a subflow node's subgraph is ever leased");
    };

    match subgraph.try_lock() {
        Ok(mut slot) => *slot = graph,
        // Poison means some body panicked, not that the slot is unusable --
        // the reading `Subflow::new` and `Taskflow::dump` both take of it.
        Err(TryLockError::Poisoned(poisoned)) => *poisoned.into_inner() = graph,
        // Never block here: this may be running on a worker that something
        // else is waiting for, and the only things that hold this lock are a
        // dump and a later entry to the same node -- the second of which owns
        // the slot now anyway. The module docs say what is lost.
        Err(TryLockError::WouldBlock) => drop(graph),
    }
}

/// Whether `graph` has a task a run could start at.
///
/// `GraphOwner::has_source_task` asks the same question of a graph that has an
/// owner; this one is asked *before* there is one, so that the common case --
/// a body that emplaced nothing, which every `join`-only subflow is -- costs
/// no allocation at all.
fn has_source_task(graph: &Graph) -> bool {
    graph.iter().any(|(_, node)| node.num_predecessors() == 0)
}

/// Why a [`Subflow::join`] or [`Subflow::try_join`] call did not do what it
/// was asked.
///
/// [`CorunError`](crate::CorunError)'s shape, for the same reason and with the
/// same pair of calls behind it: a join *is* a corun of the node's own
/// subgraph. The two differ only in what can go wrong -- a body always runs on
/// the worker that may join it, so there is no `NotAWorker` here, and a
/// subflow can be joined only once, which a corun has no equivalent of.
///
/// `#[non_exhaustive]` for the reason [`RunError`] is: later milestones have
/// more ways for a nested run to fail.
#[non_exhaustive]
pub enum JoinError {
    /// This subflow has already been joined.
    ///
    /// Upstream throws here -- *"subflow already joined"* -- and the shape of
    /// the failure is all this port changes. It is an error rather than a
    /// panic because it is the one thing a caller can act on, and because a
    /// body that joins twice has usually joined once in a branch it forgot
    /// about: [`Subflow::joinable`] is the question, and this is the answer
    /// for a caller who did not ask it.
    AlreadyJoined,
    /// A task of the subgraph panicked.
    ///
    /// The same failure [`Executor::run`](crate::Executor::run) reports,
    /// carried unchanged so that there is one answer in the crate to "a task
    /// panicked" and one place ([`RunError::Panicked`]) that says what to do
    /// with the payload.
    ///
    /// Returned by [`Subflow::try_join`] only: [`Subflow::join`] re-raises
    /// that failure instead of reporting it.
    Run(RunError),
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyJoined => f.write_str("subflow already joined"),
            Self::Run(error) => error.fmt(f),
        }
    }
}

impl fmt::Debug for JoinError {
    /// Hand-written because [`RunError`]'s is: the panic payload is a
    /// `Box<dyn Any + Send>`, which has no `Debug`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyJoined => f.write_str("AlreadyJoined"),
            Self::Run(error) => write!(f, "Run({error:?})"),
        }
    }
}

impl std::error::Error for JoinError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::AlreadyJoined => None,
            Self::Run(error) => Some(error),
        }
    }
}

impl Subflow<'_> {
    /// Runs this subflow's graph and waits for it on the worker running the
    /// body, re-raising what the first task of it to fail panicked with:
    /// upstream's `Subflow::join`.
    ///
    /// The subflow is not joinable afterwards, and the executor therefore
    /// performs no second, implicit join when the body returns. What the body
    /// emplaces *after* a join is kept or cleared with the rest of the
    /// subgraph and never runs, which is upstream's behaviour too.
    ///
    /// The calling worker stays in the work-stealing loop for the whole of the
    /// nested run -- it is [`Executor::corun`](crate::Executor::corun)'s loop,
    /// over this node's own subgraph -- so a join costs the pool no thread and
    /// a recursive subflow cannot deadlock itself however deep it goes. The
    /// `executor::corun` module docs are the long form of that argument.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::Executor;
    /// use rustflow::prelude::*;
    ///
    /// let counter = Arc::new(AtomicUsize::new(0));
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_subflow({
    ///     let counter = Arc::clone(&counter);
    ///     move |subflow: &mut Subflow<'_>| {
    ///         let counter = Arc::clone(&counter);
    ///         subflow.emplace(move || {
    ///             counter.fetch_add(1, Ordering::Relaxed);
    ///         });
    ///         assert!(subflow.joinable());
    ///
    ///         // Unlike the implicit join, this one happens *here*: the rest
    ///         // of the body can read what the children computed.
    ///         subflow.join().expect("a fresh subflow is joinable");
    ///         assert!(!subflow.joinable());
    ///         assert_eq!(subflow.num_workers(), 2);
    ///     }
    /// });
    ///
    /// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(counter.load(Ordering::Relaxed), 1);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics with the payload of the first task of the subgraph to panic,
    /// which is upstream's rethrow: a failure of the subgraph propagates on
    /// the calling worker, where the body that joined can catch it -- or not,
    /// in which case the executor catches it where it catches any other
    /// failing task and the run reports it. The payload keeps its identity,
    /// because it is handed to [`std::panic::resume_unwind`] rather than
    /// re-panicked.
    ///
    /// [`Subflow::try_join`] is the same call with that failure as an `Err`
    /// instead.
    ///
    /// # Errors
    ///
    /// [`JoinError::AlreadyJoined`] if this subflow has been joined already,
    /// which is the only error this shape returns -- a failing task is the
    /// panic above, not an error.
    pub fn join(&mut self) -> Result<(), JoinError> {
        match self.join_payload()? {
            // Upstream's `p->_rethrow_exception()`, and the last thing this
            // call does: everything the nested run owed has been paid by the
            // time the unwind starts.
            Some(payload) => panic::resume_unwind(payload),
            None => Ok(()),
        }
    }

    /// Runs this subflow's graph and waits for it, reporting a failing task as
    /// an error rather than propagating it.
    ///
    /// [`Subflow::join`] with the one difference the name says: a task of the
    /// subgraph that panicked comes back as
    /// [`JoinError::Run`]`(`[`RunError::Panicked`]`)` carrying the payload, in
    /// the shape [`RunHandle::wait`](crate::RunHandle::wait) reports a failing
    /// run in. Everything else -- the loop, the anchor, what happens to the
    /// rest of the subgraph -- is that method's, and its docs are where the
    /// explanation lives.
    ///
    /// A join that *failed* leaves the subflow joinable, which is upstream's
    /// behaviour and not an accident: its own `join` sets the bit only after
    /// the corun, so a throw skips the line. The consequence is worth
    /// knowing, because it is the executor's and not this call's -- when the
    /// body returns, the implicit join finds a joinable subflow with a
    /// non-empty graph and **runs the subgraph again**. A body that catches a
    /// failure and means that to be the end of it has to leave nothing to
    /// run: [`FlowBuilder::erase`](crate::FlowBuilder::erase) the children, or
    /// let the panic through as [`Subflow::join`] does.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::Executor;
    /// use rustflow::prelude::*;
    ///
    /// let attempts = Arc::new(AtomicUsize::new(0));
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_subflow({
    ///     let attempts = Arc::clone(&attempts);
    ///     move |subflow: &mut Subflow<'_>| {
    ///         let attempts = Arc::clone(&attempts);
    ///         subflow.emplace(move || {
    ///             // Fails the first time only, which is what makes the
    ///             // second attempt below visible.
    ///             if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
    ///                 panic!("x");
    ///             }
    ///         });
    ///
    ///         // The failure is this body's to deal with, so the subflow task
    ///         // survives it and the run completes.
    ///         let error = subflow.try_join().expect_err("`x` panicked");
    ///         assert_eq!(error.to_string(), "a task panicked: x");
    ///         assert!(subflow.joinable(), "a failed join joins nothing");
    ///     }
    /// });
    ///
    /// Executor::new(2)
    ///     .run(&taskflow)
    ///     .wait()
    ///     .expect("the body caught it");
    ///
    /// // Once for the join that failed, once for the implicit join that
    /// // followed it.
    /// assert_eq!(attempts.load(Ordering::Relaxed), 2);
    /// ```
    ///
    /// # Errors
    ///
    /// [`JoinError::AlreadyJoined`] if this subflow has been joined already,
    /// and [`JoinError::Run`] if a task of the subgraph panicked. Only the
    /// first panicking task is reported, as with
    /// [`Executor::run`](crate::Executor::run).
    pub fn try_join(&mut self) -> Result<(), JoinError> {
        match self.join_payload()? {
            Some(payload) => Err(JoinError::Run(RunError::Panicked(payload))),
            None => Ok(()),
        }
    }

    /// Both shapes of join, up to what is done with the payload.
    ///
    /// Upstream's `Subflow::join` in full: refuse a second join, corun the
    /// subgraph under this node, and set `JOINED_SUBFLOW` *after* the corun,
    /// which upstream's own comment explains -- "join here since corun graph
    /// may throw exception". That ordering is load-bearing in both ports: a
    /// join that failed leaves the subflow joinable, so the body may look at
    /// the state and the executor's implicit join still sees a subflow nobody
    /// has joined.
    fn join_payload(&mut self) -> Result<Option<Box<dyn Any + Send>>, JoinError> {
        // Upstream's `if(!joinable()) TF_THROW("subflow already joined")`.
        if !self.joinable() {
            return Err(JoinError::AlreadyJoined);
        }

        let job = self.job;

        // The lease, which is this port's whole departure from upstream's
        // `_corun_graph(_worker, _graph, ...)`: the graph comes out of the
        // node's lock, because the jobs that run it cannot hold a guard. The
        // module docs say who puts it back, and `take_subgraph` is where the
        // body's guard is given up.
        let owner = job.graph_inside(Some(self.take_subgraph()));

        // Upstream passes `_node` as the parent: the subgraph's jobs count
        // this node's own join counter down, and the node is the anchor this
        // frame waits on, so a failure under it is delivered here rather than
        // to the run. `resume` is this node's job all the same -- it is where
        // `Nested` finds the node's state words -- but nothing re-queues it,
        // because a joining node is not `PREEMPTED`.
        let parent = Nested::new(Some(job.clone()));
        let payload = corun_graph(self.worker, &owner, job.topology_share(), &parent);

        // The graph goes back into the node before this returns, so that the
        // body can read what it built and `set_retain` still has something to
        // keep. `take_lease` is the `Arc::into_inner` the module docs allow
        // for failing: when a straggling job of a nested graph still holds a
        // share, that share's `Drop` applies the policy instead and the body
        // finds an empty graph.
        self.relock(owner.take_lease());

        // Upstream's `_node->_nstate |= NSTATE::JOINED_SUBFLOW`, after the
        // corun and only if it brought nothing back: upstream's throw skips
        // the line, and this is the same control flow written out.
        if payload.is_none() {
            self.node
                .nstate()
                .fetch_or(Node::JOINED_SUBFLOW, Ordering::Relaxed);
        }

        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
    use std::sync::{Arc, OnceLock, Weak};

    use super::*;
    use crate::Executor;
    use crate::graph::Task;
    use crate::prelude::*;
    use crate::task::TaskMut;

    /// The subgraph a subflow node kept, as a dump sees it.
    fn subgraph_len(taskflow: &Taskflow, task: Task) -> usize {
        let Handle::Subflow { subgraph, .. } = taskflow.graph().node(task).handle() else {
            panic!("the node is not a subflow node");
        };
        subgraph.lock().expect("the run is over").len()
    }

    /// The whole subgraph runs before the subflow node's successors do, which
    /// is what the implicit join is: the successor sees every child counted.
    #[test]
    fn a_subflow_runs_its_whole_subgraph_before_its_successors() {
        for workers in 1..=3 {
            let counter = Arc::new(AtomicUsize::new(0));
            let seen = Arc::new(AtomicUsize::new(usize::MAX));

            let mut taskflow = Taskflow::new();
            let spawner = taskflow.emplace_subflow({
                let counter = Arc::clone(&counter);
                move |subflow: &mut Subflow<'_>| {
                    let children: Vec<Task> = (0..5)
                        .map(|_| {
                            let counter = Arc::clone(&counter);
                            subflow.emplace(move || {
                                counter.fetch_add(1, Ordering::Relaxed);
                            })
                        })
                        .collect();
                    subflow.linearize(children);
                }
            });
            let after = taskflow.emplace({
                let counter = Arc::clone(&counter);
                let seen = Arc::clone(&seen);
                move || seen.store(counter.load(Ordering::Relaxed), Ordering::Relaxed)
            });
            taskflow.task(spawner).precede([after]);

            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");

            assert_eq!(counter.load(Ordering::Relaxed), 5, "{workers} worker(s)");
            assert_eq!(
                seen.load(Ordering::Relaxed),
                5,
                "{workers} worker(s): a successor ran before the subgraph was done"
            );
        }
    }

    /// The `PREEMPTED` protocol from the inside: the bit is set while the
    /// subgraph runs and cleared by the second entry, so the node is left
    /// carrying nothing but its own dependency count.
    ///
    /// A child of the subgraph is the only place the bit can be seen set, and
    /// reaching the taskflow from there takes a slot filled in after it is
    /// built -- a `Weak`, so that the two do not hold each other alive, which
    /// is `executor::module`'s trick for the same observation.
    #[test]
    fn a_subflow_node_is_preempted_while_its_subgraph_runs() {
        let observed = Arc::new(AtomicU32::new(u32::MAX));
        let spawning: Arc<OnceLock<(Weak<Taskflow>, Task)>> = Arc::default();

        let mut taskflow = Taskflow::new();
        let spawner = taskflow.emplace_subflow({
            let observed = Arc::clone(&observed);
            let spawning = Arc::clone(&spawning);
            move |subflow: &mut Subflow<'_>| {
                let observed = Arc::clone(&observed);
                let spawning = Arc::clone(&spawning);
                subflow.emplace(move || {
                    let (taskflow, spawner) = spawning.get().expect("filled before the run");
                    let taskflow = taskflow.upgrade().expect("alive for the whole run");
                    observed.store(
                        taskflow
                            .graph()
                            .node(*spawner)
                            .nstate()
                            .load(Ordering::Relaxed),
                        Ordering::Relaxed,
                    );
                });
            }
        });
        let taskflow = Arc::new(taskflow);
        spawning
            .set((Arc::downgrade(&taskflow), spawner))
            .expect("the slot is filled once");

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(
            observed.load(Ordering::Relaxed) & Node::PREEMPTED,
            Node::PREEMPTED,
            "the subflow node is preempted while its subgraph runs"
        );
        assert_eq!(
            taskflow
                .graph()
                .node(spawner)
                .nstate()
                .load(Ordering::Relaxed),
            0,
            "the second entry clears the bit it set"
        );
    }

    /// A body that emplaces nothing is a task like any other: it never takes
    /// the `PREEMPTED` bit, and its successors run.
    ///
    /// The sourceless case is this port's widening of upstream's
    /// `g.empty()`: a subgraph whose every node is in a cycle has nothing to
    /// start at either, and preempting for it would hang the run.
    #[test]
    fn an_empty_or_sourceless_subflow_releases_its_successors_without_preempting() {
        for sourceless in [false, true] {
            for workers in 1..=3 {
                let context = format!("sourceless {sourceless}, {workers} worker(s)");
                let counter = Arc::new(AtomicUsize::new(0));

                let mut taskflow = Taskflow::new();
                let spawner = taskflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
                    if sourceless {
                        let (a, b) = subflow.emplace_n((|| {}, || {}));
                        TaskMut::new(subflow.graph_mut(), a).precede([b]);
                        TaskMut::new(subflow.graph_mut(), b).precede([a]);
                    }
                });
                let after = {
                    let counter = Arc::clone(&counter);
                    taskflow.emplace(move || {
                        counter.fetch_add(1, Ordering::Relaxed);
                    })
                };
                taskflow.task(spawner).precede([after]);

                Executor::new(workers)
                    .run(&taskflow)
                    .wait()
                    .unwrap_or_else(|error| panic!("{context}: {error}"));

                assert_eq!(
                    counter.load(Ordering::Relaxed),
                    1,
                    "{context}: the successor runs"
                );
                assert_eq!(
                    taskflow
                        .graph()
                        .node(spawner)
                        .nstate()
                        .load(Ordering::Relaxed),
                    0,
                    "{context}: the state word is a source's count and no bits"
                );
            }
        }
    }

    /// The default is upstream's: a subgraph is cleared once its run is over,
    /// so a recursive subflow does not accumulate every generation it
    /// spawned. `retain(true)` is what keeps it, and the lease going home is
    /// where both happen.
    #[test]
    fn a_subgraph_is_kept_only_when_the_body_retains_it() {
        for retain in [false, true] {
            for joined in [false, true] {
                let context = format!("retain {retain}, explicit join {joined}");
                let mut taskflow = Taskflow::new();
                let spawner = taskflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
                    subflow.emplace(|| {});
                    subflow.emplace(|| {});
                    if joined {
                        subflow.join().expect("a fresh subflow is joinable");
                    }
                    subflow.set_retain(retain);
                });

                Executor::new(2)
                    .run(&taskflow)
                    .wait()
                    .unwrap_or_else(|error| panic!("{context}: {error}"));

                // Two nodes if the body asked to keep them, nothing if it
                // did not -- and the same answer either way the nested run
                // happened, which is what `join`'s `take_lease` buys: the
                // graph is back in the node before the body goes on, so a
                // `set_retain` after a join still has something to keep.
                let kept = subgraph_len(&taskflow, spawner);
                assert_eq!(kept, usize::from(retain) * 2, "{context}");
            }
        }
    }

    /// Joining twice is upstream's "subflow already joined", as an error.
    #[test]
    fn a_second_join_is_refused() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
            subflow.emplace(|| {});
            subflow.join().expect("a fresh subflow is joinable");

            let error = subflow.try_join().expect_err("already joined");
            assert!(matches!(error, JoinError::AlreadyJoined));
            assert_eq!(error.to_string(), "subflow already joined");
            assert_eq!(format!("{error:?}"), "AlreadyJoined");

            assert!(matches!(subflow.join(), Err(JoinError::AlreadyJoined)));
        });

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }

    /// An explicit join puts the graph back where the body can see it, which
    /// is what lets the rest of the body read what its children built -- and
    /// what makes `retain` after a join a question at all.
    #[test]
    fn an_explicit_join_hands_the_subgraph_back_to_the_body() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
            subflow.emplace(|| {});
            subflow.join().expect("a fresh subflow is joinable");

            assert_eq!(subflow.graph().len(), 1, "the lease came home");
            // And the handle is a builder again, though what it builds now
            // will not run: the subflow has been joined.
            subflow.emplace(|| unreachable!("emplaced after the join"));
            assert_eq!(subflow.graph().len(), 2);
        });

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }

    /// A task of a subgraph resolves its own indices in that subgraph, which
    /// is the half of the lease a path alone could not answer: both arenas
    /// number from zero, so a job built over the outer owner would run
    /// whatever the taskflow's own graph has at those indices.
    ///
    /// The shape is the collision, as `executor::module`'s is: three children
    /// at indices 0, 1 and 2 of the subgraph, and three decoys at the same
    /// indices of the taskflow.
    #[test]
    fn a_child_of_a_subflow_resolves_its_task_in_the_subgraph() {
        let inside = Arc::new(AtomicUsize::new(0));
        let outside = Arc::new(AtomicUsize::new(0));

        let mut taskflow = Taskflow::new();
        // Index 0 of the taskflow is the subflow node itself, which a
        // resolution against the wrong arena would re-enter.
        let spawner = taskflow.emplace_subflow({
            let inside = Arc::clone(&inside);
            move |subflow: &mut Subflow<'_>| {
                for _ in 0..3 {
                    let inside = Arc::clone(&inside);
                    subflow.emplace(move || {
                        inside.fetch_add(1, Ordering::Relaxed);
                    });
                }
            }
        });
        assert_eq!(spawner, Task::new(0), "the subflow node is index 0 of both");
        for _ in 0..3 {
            let outside = Arc::clone(&outside);
            taskflow.emplace(move || {
                outside.fetch_add(1, Ordering::Relaxed);
            });
        }

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(
            inside.load(Ordering::Relaxed),
            3,
            "the subgraph's own three tasks ran"
        );
        assert_eq!(
            outside.load(Ordering::Relaxed),
            3,
            "each decoy ran once, as the task of the run that it is"
        );
    }
}

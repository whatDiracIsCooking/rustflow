//! Translated from `Executor::_invoke_subflow_task` and `Subflow::join` in
//! `taskflow/core/executor.hpp` (Taskflow v4.1.0).
//!
//! The executor's half of subflow tasking: the arm that runs a subflow task,
//! the explicit join its body can ask for, and the one function that decides
//! what becomes of a subgraph once its run is over. The builder half of
//! [`Subflow`] is `src/subflow.rs`, and the two meet at the lease. An implicit
//! join preempts the node and schedules the subgraph under it; an explicit one
//! starts the same nested run from inside the body, through
//! [`corun_graph`](crate::executor::corun), which is why upstream calls the
//! implicit join the faster.
//!
//! # Design
//!
//! Scheduling a graph needs whatever *owns* it, and a subgraph is owned by the
//! `Mutex` in [`Handle::Subflow`](crate::Handle::Subflow), which a job cannot
//! hold: `DESIGN.md` has the lease that answers that, and what it costs a
//! retained subgraph. Only the spawning worker may join, which upstream states
//! and this port makes a type -- a [`Subflow`] holds `&mut WorkerThread` and a
//! guard that is not [`Send`]. A failing task of the subgraph reports to
//! whatever waits for the graph it is in: this node for an explicit join,
//! anchored for the length of the corun, and otherwise the run.

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
/// running, and the last job of it will resume the node -- where upstream
/// returns `true`. `work` and `subgraph` are upstream's
/// `std::get_if<Node::Subflow>(&node->_handle)`.
pub(crate) fn invoke(
    worker: &mut WorkerThread,
    job: &Job,
    work: &SubflowWork,
    subgraph: &Mutex<Graph>,
) -> Option<Release> {
    let node = job.node();

    // Upstream's `else { node->_nstate &= ~NSTATE::PREEMPTED; }`: the second
    // entry. Upstream clears the subgraph here; the lease went home already.
    if node.nstate().load(Ordering::Relaxed) & Node::PREEMPTED != 0 {
        node.nstate().fetch_and(!Node::PREEMPTED, Ordering::Relaxed);

        // This is the entry that finishes the node, so it is the one whose
        // `Some` lets the caller's dispatch claim go -- held, since the
        // first entry, straight through the preempted branch at the bottom
        // of this function. A `schedule` bypass that landed anywhere in
        // between, including the window before `PREEMPTED` was ever set,
        // found the claim held and deferred onto it, and the node is
        // genuinely idle again only here rather than mid-preemption.
        return Some(Release::Strong);
    }

    // The body under upstream's `TF_EXECUTOR_EXCEPTION_HANDLER`, whose
    // `_process_exception` is `Job::record_panic`; `AssertUnwindSafe` is the
    // honest annotation, as for a static task. A panic does *not* skip what
    // follows, that handler being a switch body rather than a return -- tasks
    // emplaced before it are scheduled, then skipped as the children of a
    // node the failure marked.
    let (joinable, graph) = {
        let mut subflow = Subflow::new(worker, job, subgraph);
        if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| work.invoke(&mut subflow))) {
            job.record_panic(payload);
        }

        // Upstream asks `sf.joinable()` and `g.empty()` with the handle
        // alive; taking the lease needs the guard it holds.
        (subflow.joinable(), subflow.take_subgraph())
    };

    // Upstream's `if(sf.joinable() && !g.empty())`, widened to the graph with
    // nothing to *start* at: a sourceless one would never resume this node.
    if joinable && has_source_task(&graph) {
        let owner = job.graph_inside(Some(graph));

        // Before the graph is scheduled: a child may finish first, and the
        // bit tells the last one to re-queue this node.
        node.nstate().fetch_or(Node::PREEMPTED, Ordering::Relaxed);

        // The `None` below is what holds the dispatch claim across this
        // preemption: `work`'s guard is already long dropped, but the node
        // is about to be entered, and telling a concurrent `schedule`
        // bypass it is free to run again while its nested graph is still
        // outstanding is exactly the double dispatch the claim exists to
        // prevent. The second-entry branch above is where it is let go.
        //
        // Upstream's `_schedule_graph(worker, g, node->_topology, node)`,
        // whose `node` is a `Nested`: the subgraph's last job re-queues the
        // *job* this node was dequeued as.
        let parent = Nested::new(Some(job.clone()));
        worker.shared.schedule_graph(
            Some(worker),
            &owner,
            job.topology_share(),
            &parent.as_parent(),
        );
        return None;
    }

    // Nothing to run: a body that built nothing, or one that joined already;
    // the graph is in hand, so this is upstream's clear-unless-retain.
    return_subgraph(node, graph);

    // This entry finishes the node without ever preempting it, so -- unlike
    // the branch above -- the caller's claim is let go on this very entry,
    // the same instant `Handle::Static`'s dispatch lets go of its own.
    Some(Release::Strong)
}

/// Puts a leased subgraph back into `node`, or drops it: upstream's
/// `if((node->_nstate & NSTATE::RETAIN_SUBFLOW) == 0) g.clear();`.
///
/// Called by whoever holds the last share of a lease -- `Inside`'s `Drop` for
/// a scheduled subgraph, [`invoke`] for one that was not -- being the only
/// code that can be sure the graph is free to move.
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
        // Never block: this may run on a worker something else waits for.
        Err(TryLockError::WouldBlock) => drop(graph),
    }
}

/// Whether `graph` has a task a run could start at, asked *before* it has an
/// owner (where `GraphOwner::has_source_task` asks after), so that a body
/// which emplaced nothing costs no allocation.
fn has_source_task(graph: &Graph) -> bool {
    graph.iter().any(|(_, node)| node.num_predecessors() == 0)
}

/// Why a [`Subflow::join`] or [`Subflow::try_join`] call did not do what it
/// was asked: [`CorunError`](crate::CorunError)'s shape minus its
/// `NotAWorker`, plus the one-join rule. `#[non_exhaustive]` as that is.
#[non_exhaustive]
pub enum JoinError {
    /// This subflow has already been joined: upstream's throw, *"subflow
    /// already joined"*, as an error rather than a panic, being something a
    /// caller can act on. [`Subflow::joinable`] is the question.
    AlreadyJoined,
    /// A task of the subgraph panicked: the failure
    /// [`Executor::run`](crate::Executor::run) reports, so that
    /// [`RunError::Panicked`] alone says what to do with the payload.
    /// Returned by [`Subflow::try_join`] only.
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
    /// Hand-written because [`RunError`]'s is: the payload has no `Debug`.
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
    /// The subflow is not joinable afterwards, so no implicit join follows,
    /// and what the body emplaces *after* a join is kept or cleared with the
    /// subgraph and never runs, as upstream has it. The calling worker stays
    /// in [`Executor::corun`](crate::Executor::corun)'s loop, so a join costs
    /// the pool no thread however deep it goes.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    /// use rustflow::{Executor, prelude::*};
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
    ///         // Here, not when the body returns, so what follows can read
    ///         // what the children computed.
    ///         subflow.join().expect("a fresh subflow is joinable");
    ///         assert!(!subflow.joinable());
    ///     }
    /// });
    /// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(counter.load(Ordering::Relaxed), 1);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics with the payload of the first task of the subgraph to panic,
    /// which is upstream's rethrow: the body that joined can catch it, or let
    /// it become that task's own failure. Handed to
    /// [`std::panic::resume_unwind`], so it keeps its identity.
    ///
    /// # Errors
    ///
    /// [`JoinError::AlreadyJoined`] if already joined; [`Subflow::try_join`]
    /// is the same call with the panic above as an `Err`.
    pub fn join(&mut self) -> Result<(), JoinError> {
        match self.join_payload()? {
            // Upstream's `p->_rethrow_exception()`, last of all: the nested
            // run has paid what it owed.
            Some(payload) => panic::resume_unwind(payload),
            None => Ok(()),
        }
    }

    /// Runs this subflow's graph and waits for it, reporting a failing task as
    /// an error rather than propagating it: [`Subflow::join`] with the task's
    /// payload coming back as
    /// [`JoinError::Run`]`(`[`RunError::Panicked`]`)`, in the shape
    /// [`RunHandle::wait`](crate::RunHandle::wait) reports a failing run in.
    ///
    /// A join that *failed* leaves the subflow joinable, upstream's own `join`
    /// setting the bit only after the corun so that a throw skips the line.
    /// The consequence is the executor's: the implicit join then finds a
    /// joinable subflow with a non-empty graph and **runs the subgraph
    /// again**. A body that means a caught failure to be the end of it must
    /// leave nothing to run -- erase the children, or let the panic through.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    /// use rustflow::{Executor, prelude::*};
    ///
    /// let attempts = Arc::new(AtomicUsize::new(0));
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_subflow({
    ///     let attempts = Arc::clone(&attempts);
    ///     move |subflow: &mut Subflow<'_>| {
    ///         let attempts = Arc::clone(&attempts);
    ///         // Fails the first time only, so the second run shows.
    ///         subflow.emplace(move || {
    ///             assert_ne!(attempts.fetch_add(1, Ordering::Relaxed), 0, "x");
    ///         });
    ///         let error = subflow.try_join().expect_err("`x` panicked");
    ///         assert!(error.to_string().starts_with("a task panicked"));
    ///         assert!(subflow.joinable(), "a failed join joins nothing");
    ///     }
    /// });
    /// Executor::new(2).run(&taskflow).wait().expect("the body caught it");
    /// // Once for the join that failed, once for the implicit join after it.
    /// assert_eq!(attempts.load(Ordering::Relaxed), 2);
    /// ```
    ///
    /// # Errors
    ///
    /// [`JoinError::AlreadyJoined`] if already joined, and [`JoinError::Run`]
    /// if a task of the subgraph panicked -- the first only, as with
    /// [`Executor::run`](crate::Executor::run).
    pub fn try_join(&mut self) -> Result<(), JoinError> {
        match self.join_payload()? {
            Some(payload) => Err(JoinError::Run(RunError::Panicked(payload))),
            None => Ok(()),
        }
    }

    /// Both shapes of join, up to what is done with the payload: upstream's
    /// `Subflow::join` in full -- refuse a second join, corun the subgraph
    /// under this node, set `JOINED_SUBFLOW` *after* the corun, "since corun
    /// graph may throw exception".
    fn join_payload(&mut self) -> Result<Option<Box<dyn Any + Send>>, JoinError> {
        // Upstream's `if(!joinable()) TF_THROW("subflow already joined")`.
        if !self.joinable() {
            return Err(JoinError::AlreadyJoined);
        }

        let job = self.job;

        // The lease, this port's one departure from upstream's
        // `_corun_graph(_worker, _graph, ...)`: the graph comes out of the
        // node's lock, the jobs that run it being unable to hold a guard.

        let owner = job.graph_inside(Some(self.take_subgraph()));

        // Upstream passes `_node` as the parent: the subgraph's jobs count
        // this node's counter down and the node is this frame's anchor, so a
        // failure under it is delivered here. Nothing re-queues the job.
        let parent = Nested::new(Some(job.clone()));
        let payload = corun_graph(self.worker, &owner, job.topology_share(), &parent);

        // Back into the node before this returns, so that the body can read
        // what it built; `take_lease` may fail -- see the module docs.
        self.relock(owner.take_lease());

        // Upstream's `_node->_nstate |= NSTATE::JOINED_SUBFLOW`, after the
        // corun and only if it brought nothing back: a throw skips the line.
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
                        subflow.task(a).precede([b]);
                        subflow.task(b).precede([a]);
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

//! Translated from `taskflow/core/flow_builder.hpp` (Taskflow v4.1.0).
//!
//! The builder half of upstream's `class Subflow`: the handle a subflow
//! task's body is given, its two state-bit queries and the constructor that
//! resets them. A `Subflow` *is* a [`FlowBuilder`] -- upstream says so by
//! inheritance, this port by implementing the trait over the subgraph -- so
//! the whole `emplace` family builds into the child graph.
//!
//! `join` is not here: joining means running the subgraph and waiting for it,
//! which is the nested-graph core, so it is a second `impl Subflow` block in
//! `src/executor/subflow.rs`. What is below needs no scheduler.
//!
//! # Design
//!
//! `DESIGN.md` has the subgraph's `Mutex<Graph>` and the lease that moves it
//! out for the length of a run, the module task that refuses to re-enter a
//! node its own run is already inside of, and why there is no `executor()`
//! accessor, [`Subflow::num_workers`] standing in its place. The `Option`
//! guard here is the lease's shape on this side: taking it needs
//! `&mut Graph`, so the guard goes first, and a `join` takes a fresh one
//! before returning while the implicit join the dispatch arm performs takes
//! it for good.

use core::fmt;
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::{Mutex, MutexGuard, PoisonError, TryLockError};

use crate::executor::topology::Job;
use crate::executor::worker::{Worker, WorkerThread};
use crate::flow_builder::FlowBuilder;
use crate::graph::{Graph, Node};

/// The graph a subflow task's body builds, as that body sees it: upstream's
/// `tf::Subflow`.
///
/// A subflow task is a task that creates tasks while it runs. The body is
/// handed one of these, emplaces whatever the run-time data calls for through
/// the [`FlowBuilder`] methods, and the executor runs that child graph before
/// the subflow node's own successors -- either because the body asked with
/// [`Subflow::join`], or because it returned and the executor performed the
/// implicit join. The handle borrows everything it names, so it lives exactly
/// as long as that one invocation, as upstream's stack object does, and it
/// cannot leave the thread that made it: the guard it holds is not [`Send`].
///
/// ```
/// use rustflow::prelude::*;
///
/// let mut taskflow = Taskflow::new();
/// let (a, c) = taskflow.emplace_n((|| {}, || {}));
/// // The body runs when the task does, so what it emplaces may depend on
/// // anything known only by then.
/// let b = taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
///     let b1 = subflow.emplace(|| {});
///     let b2 = subflow.emplace(|| {});
///     subflow.linearize([b1, b2]);
/// });
/// taskflow.linearize([a, b, c]);
///
/// assert!(taskflow.graph().node(b).handle().is_subflow());
/// ```
pub struct Subflow<'a> {
    /// The worker running the body, upstream's `_worker`: the thread-side
    /// state a corun loop needs. The `&mut` is the whole of "only the
    /// worker that spawns this subflow can join it".
    pub(crate) worker: &'a mut WorkerThread,
    /// The job the worker dequeued to run this subflow task, which owns the
    /// run, the graph the node lives in and the parent chain above it.
    /// Upstream needs nothing here, reaching both through its `Node*`.
    pub(crate) job: &'a Job,
    /// The node the body is running on, upstream's `_node`. The two subflow
    /// state bits live in its `nstate` word.
    pub(crate) node: &'a Node,
    /// Where the subgraph lives when nothing holds it: the `Mutex` in this
    /// node's own handle, kept beside the guard because a join gives the
    /// guard up and has to take another.
    pub(crate) slot: &'a Mutex<Graph>,
    /// The subgraph, locked for as long as the body is building it:
    /// upstream's `FlowBuilder::_graph`, which it gets by inheritance.
    /// `None` only while the graph is out on lease, which is why every
    /// accessor below expects it to be there: no user code runs then.
    pub(crate) subgraph: Option<MutexGuard<'a, Graph>>,
}

impl<'a> Subflow<'a> {
    /// Builds the handle for a subflow task running on `job`'s node, and
    /// clears the state the last entry to that node left behind.
    ///
    /// Upstream's constructor does three things and so does this one: take
    /// the subgraph, clear `JOINED_SUBFLOW | RETAIN_SUBFLOW` and empty the
    /// graph, because a conditioner can send the run back round to this node
    /// and the second entry must not inherit the first one's answers
    /// (upstream's `Subflow.BitState`, ported in `tests/subflows.rs`).
    /// `pub(crate)` where upstream's is private with `friend class Executor`.
    ///
    /// # Panics
    ///
    /// Panics if the subgraph is already locked, which means two workers are
    /// running one subflow node -- [`crate::Work::invoke`]'s rule, and the
    /// same report.
    pub(crate) fn new(worker: &'a mut WorkerThread, job: &'a Job, slot: &'a Mutex<Graph>) -> Self {
        let mut subgraph = match slot.try_lock() {
            Ok(subgraph) => subgraph,
            // A body that panics poisons its node's subgraph. The graph is
            // cleared below anyway, and the panic itself is already on its way
            // to the topology, so taking the contents back is both harmless
            // and the only way not to replace that report with a lock error.
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => {
                panic!("subflow is already running: one worker per node was violated")
            }
        };

        let node = job.node();
        node.nstate().fetch_and(
            !(Node::JOINED_SUBFLOW | Node::RETAIN_SUBFLOW),
            Ordering::Relaxed,
        );
        subgraph.clear();

        Self {
            worker,
            job,
            node,
            slot,
            subgraph: Some(subgraph),
        }
    }

    /// Returns `true` if this subflow has not been joined yet, upstream's
    /// `joinable()`.
    ///
    /// Reads [`Node::JOINED_SUBFLOW`]. A fresh subflow is joinable; joining
    /// it, explicitly or by the implicit join the executor performs when the
    /// body returns, is what makes it not. A join that *failed* leaves it
    /// joinable, as upstream's does: its `join` sets the bit after the corun.
    #[must_use]
    pub fn joinable(&self) -> bool {
        self.node.nstate().load(Ordering::Relaxed) & Node::JOINED_SUBFLOW == 0
    }

    /// Returns `true` if this subflow's graph will survive the join,
    /// upstream's `retain()`. Reads [`Node::RETAIN_SUBFLOW`]; the default is
    /// `false`, so a recursive subflow does not accumulate every generation
    /// of children it ever spawned. [`Subflow::set_retain`] sets it.
    #[must_use]
    pub fn retain(&self) -> bool {
        self.node.nstate().load(Ordering::Relaxed) & Node::RETAIN_SUBFLOW != 0
    }

    /// Chooses whether this subflow's graph survives the join, upstream's
    /// `retain(bool)`.
    ///
    /// Renamed where upstream overloads one name for getter and setter, as
    /// [`crate::TaskRef::name`] and [`crate::TaskMut::name`] are. `DESIGN.md`
    /// has the two rules that come with it -- set before the join it is
    /// meant to keep, and best-effort since the lease goes back under
    /// [`Mutex::try_lock`] -- under "A subgraph is leased for the length of
    /// its run".
    pub fn set_retain(&mut self, retain: bool) {
        if retain {
            self.node
                .nstate()
                .fetch_or(Node::RETAIN_SUBFLOW, Ordering::Relaxed);
        } else {
            self.node
                .nstate()
                .fetch_and(!Node::RETAIN_SUBFLOW, Ordering::Relaxed);
        }
    }

    /// How many workers the executor running this subflow has: upstream's
    /// `sf.executor().num_workers()`, that accessor being absent here.
    #[must_use]
    pub fn num_workers(&self) -> usize {
        self.worker.shared.workers.len()
    }

    /// Returns the worker running this subflow's body -- an accessor where
    /// upstream keeps `_worker` private, the identity of the one worker that
    /// may join being something a body may legitimately want.
    #[must_use]
    pub fn worker(&self) -> &Worker {
        &self.worker.shared.workers[self.worker.id]
    }

    /// Returns the graph this subflow is building, upstream's `graph()`. The
    /// mutable form is [`FlowBuilder::graph_mut`], the seam every builder
    /// method goes through.
    ///
    /// # Panics
    ///
    /// Panics if the subgraph is out on lease, which no caller of this can
    /// observe: no user code runs in either window.
    #[must_use]
    pub fn graph(&self) -> &Graph {
        self.subgraph
            .as_ref()
            .expect("a subflow body always holds its subgraph")
    }

    /// Takes the graph the body has built out of the node, leaving the
    /// node's slot empty and this handle without a guard: the **lease**. A
    /// missing guard is tolerated -- a body unwound out of a `join` leaves
    /// one -- by taking the lock itself.
    pub(crate) fn take_subgraph(&mut self) -> Graph {
        let mut subgraph = match self.subgraph.take() {
            Some(subgraph) => subgraph,
            None => lock_subgraph(self.slot),
        };
        mem::take(&mut *subgraph)
    }

    /// Takes a guard on the node's subgraph again, putting `graph` back in
    /// the node's slot if the lease came home with it: `Some` is `join`
    /// taking its own lease back, `None` a straggling job's `Drop` leaving
    /// the body an empty graph instead. Blocking rather than
    /// [`Mutex::try_lock`], the opposite of [`Subflow::new`]'s choice: a
    /// contended lock here is a dump or a previous lease going home,
    /// neither of which can wait on this worker.
    pub(crate) fn relock(&mut self, graph: Option<Graph>) {
        let mut subgraph = lock_subgraph(self.slot);
        if let Some(graph) = graph {
            *subgraph = graph;
        }
        self.subgraph = Some(subgraph);
    }
}

impl FlowBuilder for Subflow<'_> {
    /// The inherent [`Subflow::graph`], which is the borrow the trait asks
    /// for. This method and the one below are the whole of what upstream
    /// gets by deriving `Subflow` from `FlowBuilder`.
    fn graph(&self) -> &Graph {
        Subflow::graph(self)
    }

    /// The subgraph, through the guard this handle holds.
    ///
    /// # Panics
    ///
    /// Panics if the subgraph is out on lease, as [`Subflow::graph`] does.
    fn graph_mut(&mut self) -> &mut Graph {
        self.subgraph
            .as_mut()
            .expect("a subflow body always holds its subgraph")
    }
}

impl fmt::Debug for Subflow<'_> {
    /// Names the node, the two state bits, the worker's id and how many
    /// tasks the body has emplaced so far; a graph out on lease counts as
    /// none, which is a state no caller can print from.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subflow")
            .field("node", &self.node.name())
            .field("worker", &self.worker.id)
            .field("joinable", &self.joinable())
            .field("retain", &self.retain())
            .field(
                "tasks",
                &self.subgraph.as_ref().map_or(0, |subgraph| subgraph.len()),
            )
            .finish_non_exhaustive()
    }
}

/// Locks a subflow node's subgraph, taking a poisoned lock's contents
/// rather than failing on them: poison means some body panicked, not that
/// the graph is unreadable.
fn lock_subgraph(slot: &Mutex<Graph>) -> MutexGuard<'_, Graph> {
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::Executor;
    use crate::executor::queue::WorkerQueue;
    use crate::executor::topology::{GraphOwner, Parent, Topology};
    use crate::graph::{Handle, Task};
    use crate::prelude::*;
    use crate::taskflow::Flow;

    /// A one-node taskflow whose node is a subflow task running `body`.
    ///
    /// A real taskflow rather than a bare graph, because a handle needs the
    /// *job* the scheduler would have dequeued and a job is built over a
    /// taskflow's storage.
    fn spawner<C>(body: C) -> Taskflow
    where
        C: for<'a> FnMut(&mut Subflow<'a>) + Send + 'static,
    {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_subflow(body);
        taskflow.task(Task::new(0)).name("spawner");
        taskflow
    }

    /// The subgraph of a subflow node. The variant's fields are public, so
    /// this is only the `let ... else` dance these cases would repeat.
    fn subgraph_of(node: &Node) -> &Mutex<Graph> {
        let Handle::Subflow { subgraph, .. } = node.handle() else {
            panic!("the node is not a subflow node");
        };
        subgraph
    }

    /// Runs `body` against a subflow handle over the one node of `taskflow`,
    /// standing in for the dispatch arm in `executor::subflow`.
    ///
    /// The job is the one that arm would have been handed: the single source
    /// of a run of this taskflow. Nothing here schedules anything, so the run
    /// is never started and never torn down -- what is under test is the
    /// handle around the body, not the body.
    fn with_subflow<F>(taskflow: &Taskflow, body: F)
    where
        F: FnOnce(&mut Subflow<'_>),
    {
        let executor = Executor::new(1);
        let mut worker = WorkerThread::new(Arc::clone(&executor.shared), 0, WorkerQueue::new());
        let topology = run_of(taskflow.flow());
        let owner = GraphOwner::flow(taskflow.flow());
        let job = Job::sources(&owner, &topology, &Parent::Run)
            .next()
            .expect("the subflow node is the one source of this taskflow");

        let mut subflow = Subflow::new(&mut worker, &job, subgraph_of(job.node()));
        body(&mut subflow);
    }

    /// A run of `flow`'s graph, shared as the scheduling path needs it.
    fn run_of(flow: &Arc<Flow>) -> Arc<Topology> {
        Arc::new(Topology::new(|| true, || {}, Arc::clone(flow)))
    }

    /// Upstream's `Subflow.BitState`, on the builder alone: however the last
    /// entry left the node, a fresh handle over it reports clean state and an
    /// empty graph. The run that loops a real subflow node through a condition
    /// task is in `tests/subflows.rs`.
    #[test]
    fn a_fresh_subflow_resets_the_bits_the_last_entry_left() {
        let taskflow = spawner(|_| {});

        // What the end of a previous entry looks like: joined, and asked to
        // keep its graph. The graph has a node in it for the same reason.
        let node = taskflow.graph().node(Task::new(0));
        node.nstate().store(
            Node::JOINED_SUBFLOW | Node::RETAIN_SUBFLOW,
            Ordering::Relaxed,
        );
        subgraph_of(node)
            .lock()
            .expect("uncontended")
            .add_node(Handle::Placeholder);

        with_subflow(&taskflow, |subflow| {
            assert!(subflow.joinable());
            assert!(!subflow.retain());
            assert!(subflow.graph().is_empty());
        });

        // And the reset is in the node's own word, not in the handle: both
        // bits are gone from the state the next entry will read.
        assert_eq!(node.nstate().load(Ordering::Relaxed), 0);
    }

    #[test]
    fn set_retain_toggles_its_own_bit_and_nothing_else() {
        let taskflow = spawner(|_| {});
        let node = taskflow.graph().node(Task::new(0));

        with_subflow(&taskflow, |subflow| {
            subflow.set_retain(true);
            assert!(subflow.retain());
            assert_eq!(
                node.nstate().load(Ordering::Relaxed),
                Node::RETAIN_SUBFLOW,
                "retaining a subflow may not touch another bit"
            );
            // Still joinable: the two bits are independent, which is the half
            // of upstream's reset that a setter could get wrong.
            assert!(subflow.joinable());

            subflow.set_retain(false);
            assert!(!subflow.retain());
            assert_eq!(node.nstate().load(Ordering::Relaxed), 0);
            assert!(subflow.joinable());
        });
    }

    /// `joinable` reads the node's bit rather than any state of its own, which
    /// is what lets `join` -- in another module -- turn it off.
    #[test]
    fn joinable_follows_the_nodes_bit() {
        let taskflow = spawner(|_| {});
        let node = taskflow.graph().node(Task::new(0));

        with_subflow(&taskflow, |subflow| {
            assert!(subflow.joinable());
            node.nstate()
                .fetch_or(Node::JOINED_SUBFLOW, Ordering::Relaxed);
            assert!(!subflow.joinable());
            assert!(!subflow.retain());
        });
    }

    /// The acceptance bar for implementing [`FlowBuilder`]: a subflow builds
    /// into its own subgraph with the same API a taskflow uses, and the
    /// taskflow's graph is not where any of it lands.
    #[test]
    fn a_subflow_builds_into_its_subgraph_through_the_builder_api() {
        let taskflow = spawner(|_| {});

        with_subflow(&taskflow, |subflow| {
            let (b1, b2) = subflow.emplace_n((|| {}, || {}));
            let b3 = subflow.emplace(|| {});
            let b4 = subflow.placeholder();

            // `precede` is on the task handle, as upstream's `B1.precede(B3)`
            // is; the builder's own chaining method is `linearize`.
            subflow.task(b1).precede([b3]);
            subflow.task(b2).precede([b3]);
            subflow.linearize([b3, b4]);

            assert_eq!(subflow.graph().len(), 4);
            assert_eq!(subflow.graph().successors(b1), [b3].as_slice());
            // b2 before b1 in the predecessor half: pushing b4 as a
            // successor later swaps the first predecessor to the back, which
            // is the upstream edge-list trick `graph.rs` documents.
            assert_eq!(subflow.graph().predecessors(b3), [b2, b1].as_slice());
            assert_eq!(subflow.graph().successors(b3), [b4].as_slice());

            // Both halves of the task handle come off the builder, which is
            // what makes a subflow body read like upstream's: naming `B3` and
            // asking after it is the same call here as on a taskflow (#120).
            subflow.task(b3).name("B3");
            assert_eq!(subflow.task_ref(b3).name(), "B3");
            assert_eq!(subflow.task_ref(b3).num_predecessors(), 2);

            assert!(format!("{subflow:?}").contains("tasks: 4"));
            assert_eq!(subflow.num_workers(), 1);
            assert_eq!(subflow.worker().id(), 0);
        });

        // The subgraph outlives the handle -- it belongs to the node -- so
        // what the body built is still there for the executor to run.
        assert_eq!(
            subgraph_of(taskflow.graph().node(Task::new(0)))
                .lock()
                .expect("uncontended")
                .len(),
            4
        );
        // And none of it went into the graph the subflow node lives in.
        assert_eq!(taskflow.graph().len(), 1);
    }

    /// The lease, from this file's side: taking the subgraph empties the
    /// node's slot and leaves the handle without a guard, and re-locking after
    /// the graph has gone home gives the body its builder back.
    #[test]
    fn taking_the_subgraph_empties_the_slot_and_relocking_finds_it_again() {
        let taskflow = spawner(|_| {});
        let slot = subgraph_of(taskflow.graph().node(Task::new(0)));

        with_subflow(&taskflow, |subflow| {
            subflow.emplace(|| {});
            let leased = subflow.take_subgraph();
            assert_eq!(leased.len(), 1);

            // The slot is empty and -- this is the half the jobs of a nested
            // run need -- unlocked, so the scheduler is never behind a guard.
            assert!(
                slot.try_lock()
                    .expect("the guard went with the lease")
                    .is_empty(),
                "the leased graph is out of the node"
            );

            // What the lease going home looks like, done here by hand.
            subflow.relock(Some(leased));
            assert_eq!(subflow.graph().len(), 1);
        });
    }

    /// The seam the dispatch arm is written against, end to end: build the
    /// handle over the node, hand it to the node's own body, and find
    /// afterwards that what the body built is in the node's subgraph.
    #[test]
    fn the_nodes_own_body_builds_through_the_handle_it_is_given() {
        let taskflow = spawner(|subflow: &mut Subflow<'_>| {
            let child = subflow.emplace(|| {});
            let grandchild = subflow.emplace_subflow(|_| {});
            subflow.linearize([child, grandchild]);
        });
        let node = taskflow.graph().node(Task::new(0));
        let Handle::Subflow { work, subgraph } = node.handle() else {
            panic!("the node is not a subflow node");
        };

        with_subflow(&taskflow, |subflow| {
            work.invoke(subflow);
            // The implicit join reads these two, which is why they are the
            // state the body leaves behind rather than anything the handle
            // owns.
            assert!(subflow.joinable());
            assert_eq!(subflow.graph().len(), 2);
        });

        // The guard is gone with the handle, so the subgraph can be locked
        // again -- which is what taking the lease and clearing the graph both
        // need.
        let built = subgraph.lock().expect("uncontended");
        assert_eq!(built.len(), 2);
        assert!(built.node(Task::new(1)).handle().is_subflow());
        drop(built);

        // A second entry to the same node starts over, body and all: nothing
        // the first entry built is still in the way.
        with_subflow(&taskflow, |subflow| {
            assert!(subflow.graph().is_empty());
            work.invoke(subflow);
            assert_eq!(subflow.graph().len(), 2);
        });
    }

    #[test]
    #[should_panic(expected = "one worker per node")]
    fn a_second_handle_over_one_node_panics() {
        let taskflow = spawner(|_| {});
        with_subflow(&taskflow, |_| {
            // Re-entering from this thread is the cheap stand-in for two
            // workers racing: `try_lock` cannot tell the two apart.
            with_subflow(&taskflow, |_| {});
        });
    }
}

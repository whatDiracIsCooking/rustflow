//! Translated from `taskflow/core/flow_builder.hpp` (Taskflow v4.1.0).
//!
//! The builder half of upstream's `class Subflow`, which is the end of that
//! header: the handle a subflow task's body is given, its two state-bit
//! queries and the constructor that resets them. A `Subflow` *is* a
//! [`FlowBuilder`] -- upstream says so by inheritance, this port by
//! implementing the trait over the subgraph -- so the whole `emplace` family
//! builds into the child graph with no second API to learn.
//!
//! `join` is not here. Joining a subflow means running its graph and waiting
//! for it, which is the nested-graph core, so it is a second `impl Subflow`
//! block in `src/executor/subflow.rs` (issue #50) beside the dispatch arm
//! that runs a subflow task. What is below is everything that needs no
//! scheduler.
//!
//! # The subgraph, its lock and the lease
//!
//! Upstream's handle holds a `Graph&` into the node's own
//! `Node::Subflow::subgraph` and reaches it through a `Node*` the scheduler
//! hands around. A worker here reaches its node as `&Node`, so the subgraph
//! carries its own `Mutex` (see [`crate::Handle::Subflow`]) and the handle
//! holds the guard for as long as the body runs. That cannot deadlock a body:
//! the only graph a body locks is its *own* node's, and a subflow nested
//! inside a subflow is a different node, so no body holds two of these locks
//! and none waits for one.
//!
//! What this lock is *not* is the thing that keeps one node to one entry.
//! `Subflow::new` takes it with [`Mutex::try_lock`] and panics on a
//! contended one, which catches two workers running one node while a body is
//! building -- [`crate::Work::invoke`]'s rule, with the same report -- and
//! that is all it catches. The lease below is why: a graph out on lease
//! leaves the node's slot **empty and unlocked**, so a second entry to a
//! node whose subgraph is still running finds the lock free, takes it, and
//! runs the body again over an empty graph. Issue #82 is where that was
//! reached -- through a taskflow composing itself, which re-armed a node this
//! run had preempted -- and it is refused by `executor::module` rather than
//! by this `Mutex`, because the scheduler is what knows that the node is
//! mid-entry. The invariant that holds here is the narrow one stated above,
//! and the "one entry per node" rule is the preemption protocol's.
//!
//! The guard is an [`Option`] because the body is not the only thing that
//! wants the subgraph. Running it -- the implicit join the dispatch arm
//! performs, or [`Subflow::join`](crate::Subflow::join) -- *moves the graph
//! out* of that `Mutex` for the length of the nested run, because the jobs of
//! a graph own what they name and a `MutexGuard` is not something a job can
//! own. Issue #74 calls that the **lease**, `executor::topology` has the three
//! options that were on the table, and the one consequence for this file is
//! the one that cannot be hidden: taking the lease needs `&mut Graph`, so the
//! guard has to be given up first. So `join` hands the guard over, coruns the
//! graph, and takes a fresh guard before it returns; the arm takes the guard
//! for good, after the body is done with it.
//!
//! # The two state bits
//!
//! `joinable()` and `retain()` read one bit each out of the node's `nstate`
//! word -- [`Node::JOINED_SUBFLOW`] and [`Node::RETAIN_SUBFLOW`] -- and
//! `Subflow::new` clears both, because a conditioner can send the run back
//! round to the same subflow node and the second entry must not inherit the
//! first one's answers. That reset is what upstream's
//! `unittests/test_subflows.cpp` `Subflow.BitState` asserts: it loops a
//! subflow task a hundred times through a condition task and requires every
//! entry to see `joinable()` true and `retain()` false, however the previous
//! entry left the node. It is ported in `tests/subflows.rs`.
//!
//! `retain()` has one rule upstream does not need, and it comes straight out
//! of the lease: **set it before the join it is meant to keep.** The graph
//! goes home when the lease is released, and whoever releases it reads this
//! bit then -- which is after the body in every case the lease comes back the
//! usual way, and inside the `join` call in the one case it does not.
//! Retention is also best-effort, because putting the graph back must not
//! park a worker on the node's lock. [`Subflow::set_retain`] has both
//! conditions exactly.
//!
//! # Why there is no `executor()` accessor
//!
//! Upstream's `Subflow` holds an `Executor&` and hands it back, which this
//! port cannot do and does not pretend to. Nothing on the worker path has an
//! `&Executor`: a `WorkerThread` holds an `Arc<Shared>` -- the worker array,
//! the buffers, the notifier, the topology count -- and `Shared` has no
//! back-reference to the `Executor`, which is itself owned by value, so there
//! is no share to clone and no `Weak` to upgrade. Reproducing the member
//! would take a raw back-pointer, which is the `unsafe` the executor has not
//! had since #40.
//!
//! So the accessor is [`Subflow::num_workers`] instead, which is the one
//! thing upstream's callers actually ask the executor for -- its partitioners
//! read `rt.executor().num_workers()` -- and which the shared state can
//! answer. A body that genuinely needs the executor captures an
//! `Arc<Executor>`, as this crate's own `corun` documentation examples do.

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
/// the [`FlowBuilder`] methods, and the executor then runs that child graph
/// before the subflow node's own successors -- either because the body asked
/// for it with [`Subflow::join`], or because the body returned and the
/// executor performed the implicit join.
///
/// The handle borrows everything it names -- the worker running the body, the
/// job that worker dequeued, the node the body runs on and the node's
/// subgraph -- so it lives exactly as long as the one invocation that was
/// given it. Upstream's is a stack object passed by reference for the same
/// reason; neither is copyable, and this one cannot leave the thread that made
/// it either, because the guard it holds is not [`Send`].
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
    /// The worker running the body, upstream's `_worker` -- the thread-side
    /// state of it rather than the record every thread shares, because that is
    /// what a corun loop needs and [`Subflow::join`] is a corun.
    ///
    /// It is also what "only the worker that spawns this subflow can join it"
    /// is about: the `&mut` is the whole of that rule, since the one frame
    /// that can lend it is the dispatch arm on this worker's own stack.
    pub(crate) worker: &'a mut WorkerThread,
    /// The job the worker dequeued to run this subflow task.
    ///
    /// Upstream needs nothing here: its `Subflow` holds a `Node*` and reaches
    /// the run through `_node->_topology` and the parent through
    /// `_node->_parent`. Neither link is on a node in this port -- see
    /// `executor::topology` -- so joining needs the job, which is the value
    /// that owns the run, the graph the node lives in and the parent chain
    /// above it.
    pub(crate) job: &'a Job,
    /// The node the body is running on, upstream's `_node`. The two subflow
    /// state bits live in its `nstate` word.
    pub(crate) node: &'a Node,
    /// Where the subgraph lives when nothing holds it: the `Mutex` in this
    /// node's own handle. Kept beside the guard because a join gives the guard
    /// up and has to take another one.
    pub(crate) slot: &'a Mutex<Graph>,
    /// The subgraph, locked for as long as the body is building it. This is
    /// upstream's `FlowBuilder::_graph` member, which it gets by inheritance.
    ///
    /// `None` only while the graph is out on lease -- inside a `join`, and
    /// after the arm has taken it to schedule -- which is why every accessor
    /// below expects it to be there: no user code runs in either window.
    pub(crate) subgraph: Option<MutexGuard<'a, Graph>>,
}

impl<'a> Subflow<'a> {
    /// Builds the handle for a subflow task running on `job`'s node, and
    /// clears the state the last entry to that node left behind.
    ///
    /// Upstream's constructor does three things and so does this one: take the
    /// subgraph, clear `JOINED_SUBFLOW | RETAIN_SUBFLOW`, and empty the graph
    /// -- all three because "there could have iterative control flow", in
    /// upstream's words, which is to say the same node may be entered again
    /// and has to look untouched when it is.
    ///
    /// Upstream's constructor is private, with `friend class Executor` to let
    /// the scheduler reach it; here it is `pub(crate)`, which is the same
    /// statement in the vocabulary this crate has. It was `pub` until #50 gave
    /// it the job and the worker thread it needs, neither of which is a type
    /// outside the crate can name.
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
    /// A fresh subflow is joinable; joining it -- explicitly with
    /// [`Subflow::join`], or by the implicit join the executor performs when
    /// the body returns -- is what makes it not. A join that *failed* leaves
    /// it joinable, which is upstream's behaviour too: its `join` sets the bit
    /// after the corun, so an exception from the corun skips the line.
    #[must_use]
    pub fn joinable(&self) -> bool {
        self.node.nstate().load(Ordering::Relaxed) & Node::JOINED_SUBFLOW == 0
    }

    /// Returns `true` if this subflow's graph will survive the join,
    /// upstream's `retain()`.
    ///
    /// Default is `false`: the runtime clears a subflow's graph once it has
    /// been joined, so that a recursive subflow does not accumulate every
    /// generation of children it ever spawned. Set it to keep the graph for
    /// something that wants to look at it afterwards, a DOT dump above all --
    /// bearing in mind that keeping it is best-effort, which
    /// [`Subflow::set_retain`] says in full.
    ///
    /// This is what the *node* currently says, not a promise about what will
    /// be in it: a `true` here means the graph is to be kept if it can be,
    /// and a dump is still the only way to find out whether it was.
    #[must_use]
    pub fn retain(&self) -> bool {
        self.node.nstate().load(Ordering::Relaxed) & Node::RETAIN_SUBFLOW != 0
    }

    /// Chooses whether this subflow's graph survives the join, upstream's
    /// `retain(bool)`.
    ///
    /// Upstream overloads one name for the getter and the setter, which one
    /// Rust type cannot do, so the setter is renamed -- the same split that
    /// [`crate::TaskRef::name`] and [`crate::TaskMut::name`] made of upstream's
    /// `name()` pair.
    ///
    /// **Set it before the join it is meant to keep**, which is the one rule
    /// upstream does not need. The subgraph goes back into the node when the
    /// nested run's lease is released, and whoever releases it reads this bit
    /// then. Both joins normally leave that until after the body is done with
    /// the handle -- the implicit one by construction, an explicit
    /// [`Subflow::join`] by taking the lease back itself -- so anywhere in the
    /// body will do. The exception is a subflow whose subgraph contained a
    /// *nested* graph of its own: a straggling job of that graph can still
    /// hold a share of the lease when `join` returns, and the graph then goes
    /// home by the other route, reading this bit at that moment. A
    /// `set_retain(true)` after the join is therefore honoured almost always
    /// and not reliably; before the join it always is. `executor::subflow` has
    /// the full argument.
    ///
    /// **Keeping the graph is best-effort.** Putting it back takes the node's
    /// own lock, and the code that does it uses [`Mutex::try_lock`] rather
    /// than waiting, for the reason [`crate::Taskflow::dump`] gives for its
    /// own `try_lock`-and-skip read from the other side: the thread releasing
    /// a lease is a worker, and a worker parked on a lock is a worker the rest
    /// of the run cannot use. So a lock held at exactly that moment -- a
    /// concurrent `dump` reading the cluster being the realistic case -- costs
    /// the graph, which is dropped as an unretained one would be, and the dump
    /// can come back without the cluster it asked for. There is one such
    /// attempt per entry either way the subflow was joined: when the last job
    /// of the subgraph is accounted for, for the implicit join, and when the
    /// body returns, for [`Subflow::join`]. Nothing reports the loss, so a
    /// dump that has to see the subgraph is a dump taken when the taskflow is
    /// idle -- which is the advice upstream's own `dump` documentation gives
    /// for a subflow anyway. The module docs' lease section is the argument
    /// behind all of it.
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

    /// How many workers the executor running this subflow has.
    ///
    /// Upstream reaches this as `sf.executor().num_workers()`, which is the
    /// one thing its own callers ask a `Subflow`'s executor for -- the
    /// partitioners in `taskflow/algorithm/` size their chunks by it. The
    /// module docs say why the accessor it goes through is not here.
    #[must_use]
    pub fn num_workers(&self) -> usize {
        self.worker.shared.workers.len()
    }

    /// Returns the worker running this subflow's body.
    ///
    /// Upstream keeps `_worker` private and reaches it through `friend class
    /// Executor`; here it is an accessor, because the identity of the worker
    /// is something a body may legitimately want to know -- it is the one
    /// worker that may join this subflow.
    #[must_use]
    pub fn worker(&self) -> &Worker {
        &self.worker.shared.workers[self.worker.id]
    }

    /// Returns the graph this subflow is building, upstream's `graph()`.
    ///
    /// Upstream hands back a mutable `Graph&`; here the mutable form is
    /// [`FlowBuilder::graph_mut`], which is the same seam every builder method
    /// goes through.
    ///
    /// # Panics
    ///
    /// Panics if the subgraph is out on lease, which no caller of this can
    /// observe: the only windows are inside [`Subflow::join`] and after the
    /// body has returned, and no user code runs in either.
    #[must_use]
    pub fn graph(&self) -> &Graph {
        self.subgraph
            .as_ref()
            .expect("a subflow body always holds its subgraph")
    }

    /// Takes the graph the body has built out of the node, leaving the node's
    /// slot empty and this handle without a guard: the **lease** issue #74
    /// settled the shape of.
    ///
    /// The one operation the lease needs from this file, and the reason the
    /// guard is an `Option`: a leased graph is owned by the jobs that run it,
    /// so it cannot be left where a `MutexGuard` is the only way to reach it.
    /// `Subflow::relock` is how a `join` gets a guard back afterwards.
    ///
    /// Tolerates a missing guard, which is the state a body that unwound out
    /// of a `join` leaves behind: the graph is in the slot either way, so this
    /// takes the lock itself.
    pub(crate) fn take_subgraph(&mut self) -> Graph {
        let mut subgraph = match self.subgraph.take() {
            Some(subgraph) => subgraph,
            None => lock_subgraph(self.slot),
        };
        mem::take(&mut *subgraph)
    }

    /// Takes a guard on the node's subgraph again, putting `graph` back in the
    /// node's slot if the lease came home with it.
    ///
    /// What `Subflow::join` does on its way out: `Some` is the lease it took
    /// back itself, `None` the rare case where a straggling job of a nested
    /// graph still held a share and will apply the clear-unless-retain policy
    /// through its own `Drop` instead -- `executor::subflow` has that
    /// argument. The body sees an empty graph in the `None` case, which is why
    /// it is worth distinguishing at all.
    ///
    /// Blocking rather than [`Mutex::try_lock`], which is the opposite of
    /// [`Subflow::new`]'s choice and is right for the opposite reason: a
    /// contended lock here is not a second worker on this node -- this worker
    /// is the one running it -- but a [`crate::Taskflow::dump`] reading the
    /// cluster, or the lease of a previous entry going home, and both let go
    /// at once. Neither can wait on this worker, so neither can deadlock it.
    pub(crate) fn relock(&mut self, graph: Option<Graph>) {
        let mut subgraph = lock_subgraph(self.slot);
        if let Some(graph) = graph {
            *subgraph = graph;
        }
        self.subgraph = Some(subgraph);
    }
}

impl FlowBuilder for Subflow<'_> {
    /// The subgraph, read-only: the inherent [`Subflow::graph`], which is the
    /// same borrow the trait asks for.
    fn graph(&self) -> &Graph {
        Subflow::graph(self)
    }

    /// The subgraph, through the guard this handle holds. This method and the
    /// one above are the whole of what upstream gets by deriving `Subflow`
    /// from `FlowBuilder`.
    ///
    /// # Panics
    ///
    /// Panics if the subgraph is out on lease, as [`Subflow::graph`] does and
    /// for the same reason.
    fn graph_mut(&mut self) -> &mut Graph {
        self.subgraph
            .as_mut()
            .expect("a subflow body always holds its subgraph")
    }
}

impl fmt::Debug for Subflow<'_> {
    /// Names the node, the two state bits and how many tasks the body has
    /// emplaced so far, which is what a subflow is. The worker behind it has
    /// no small rendering -- it is one thread of a pool -- so it is the id
    /// that is printed, which is what identifies it anyway.
    ///
    /// A graph out on lease counts as no tasks, which is a state no caller
    /// can print from: see [`Subflow::graph`].
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

/// Locks a subflow node's subgraph, taking a poisoned lock's contents rather
/// than failing on them.
///
/// Poison means some body panicked, not that the graph is unreadable, which is
/// the reading [`Subflow::new`] and [`crate::Taskflow::dump`] both already
/// take of it.
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

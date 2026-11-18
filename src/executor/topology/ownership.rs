//! Translated from the `Graph` and `NodeBase` estate of
//! `taskflow/core/graph.hpp` (Taskflow v4.1.0).
//!
//! Who owns the graph a [`Job`] walks, and whose join counter that job counts
//! down: upstream's `Graph&`, `Topology::_taskflow` and `NodeBase* _parent`,
//! as owning handles.
//!
//! # Design
//!
//! Each of those three is a non-owning pointer upstream, which is sound there
//! because its documentation forbids destroying or editing a running taskflow
//! -- a rule a safe Rust API cannot rest on. So each is owning here, which is
//! `DESIGN.md`'s "A run owns the graph it walks"; that file also has why a
//! parent travels with the job rather than sitting on the node, and why a
//! subgraph is leased out of its lock to be run. Three merges would each take
//! these five types to four, and each is declined:
//!
//! - **[`Storage`] into [`Inside`]** share only "an `Arc` with a `Drop`": one
//!   is a share of a taskflow's storage whose last holder wakes a waiting
//!   thread, the other a path -- plus, for a subflow, the graph itself --
//!   whose last holder puts that graph back in a node's `Mutex`.
//! - **[`GraphOwner::Run`] into [`GraphOwner::Flow`]** is possible, an ungated
//!   hold behaving exactly as a bare share does, and is declined on cost: a
//!   module task would allocate a hold per nested run that gates nothing, and
//!   [`GraphOwner::graph`] would grow a pointer hop.
//! - **[`Nested`] into [`Parent`]** is forbidden by the invariant that the
//!   state words a nested graph's jobs count down are **one allocation shared
//!   by every job under that parent**: `Parent` is `Clone` and travels by
//!   value on every job, so an inline payload would hand each child a counter
//!   of its own. `Parent` stays an enum rather than an `Option<Arc<Nested>>`
//!   because every job has a parent and the run is one of the two kinds.
//!
//! ## The locks, and the order they are taken in
//!
//! One list, which the field docs here and in `run` point at:
//!
//! - A run's share slot (`Topology::storage`) is never locked while that run's
//!   completion or policy lock is held, and the share is never *dropped* under
//!   it, [`Drop for Storage`](Storage) reaching [`Topology::finish`] and its
//!   completion lock. The other two are never held at once either, nor across
//!   user code.
//! - A subflow's `Mutex<Graph>` is taken by [`Drop for Inside`](Inside) with
//!   no lock of a run held, and with `try_lock`: the thread dropping the last
//!   share of a lease is a worker something else may be waiting for.
//! - **A thread is woken only by the drop of the last hold on whatever it will
//!   touch next.** `DESIGN.md` has why that one rule replaced two that did not
//!   add up, `tests/loom_storage.rs` is the model of it, and
//!   [`Topology::finish`] asserts it in debug builds. The waits it covers: a
//!   `RunHandle`'s `wait`, `Future` and `Drop` sleep in the completion slot
//!   and `Executor::wait_for_all` on the executor's run count, both woken by
//!   [`Drop for Storage`](Storage); `Executor::drop` is woken by the last free
//!   async task's tear-down; a `corun` frame sleeps nowhere and polls for
//!   uniqueness the hold it owns; and a `Subflow::join` waits on an anchor,
//!   taking its lease back through [`Drop for Inside`](Inside).
//!   `AsyncHandle::wait` is the one wait behind which nothing needs it.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

use super::job::Job;
use super::run::{Cancellable, Topology};
use crate::executor::Shared;
use crate::executor::async_node::AsyncNode;
use crate::executor::subflow;
use crate::graph::{Graph, Handle, NodeBase, Task};
use crate::taskflow::Flow;

/// Which graph a [`Job`]'s [`Task`] indexes, and what keeps that graph alive:
/// upstream's `Graph&`, as a choice of owner rather than a reference. `Clone`
/// because every job of a graph holds an owner of its own.
#[derive(Clone)]
pub(crate) enum GraphOwner {
    /// The graph of the run itself, held through the run's [`Storage`] hold
    /// rather than through a bare share of the storage: these are the shares a
    /// waiting thread is released by.
    Run(Arc<Storage>),
    /// A taskflow's storage reached without such a hold, which is the target
    /// of a module task composed of an `Arc<Taskflow>`.
    Flow(Arc<Flow>),
    /// A graph that one node of another graph owns: an adopted module's graph,
    /// and a subflow's subgraph.
    Inside(Arc<Inside>),
    /// The one-node arena an async task is, which belongs to no taskflow and to
    /// no node. The share is upstream's reference count on the node, held while
    /// the job is queued.
    Async(Arc<AsyncNode>),
}

/// A run's hold on the storage of the taskflow it is a run of: the share every
/// job of that run's own graph owns, and -- when the last of those shares goes
/// -- what releases the thread waiting on the run. Upstream has nothing here,
/// its `Topology` holding a `Taskflow&`. One allocation per run.
pub(crate) struct Storage {
    /// The storage itself: the graph and the queue of runs of it. An `Option`
    /// for the length of this type's [`Drop`] only, the share having to be
    /// released *before* the waiter where drop glue would run after.
    flow: Option<Arc<Flow>>,
    /// The run whose waiter the last share of this hold releases, or an empty
    /// [`Weak`] for a hold that releases nobody: a share back would be a cycle
    /// only a completing run would break.
    run: Weak<Topology>,
    /// The executor whose run count this run is counted in, so that the last
    /// share can take it back out; `None` for a hold whose run was never counted,
    /// or accounts for itself. Here because `Executor::wait_for_all` sleeps on
    /// that count, which the wake rule puts behind the last drop.
    executor: Option<Arc<Shared>>,
}

/// A graph owned by one node of another graph, named by that node: the path
/// that stands in for upstream's `Graph&` when the graph is inside a handle.
/// Every link up to the run's own storage is owning, and it cannot be a
/// cycle, a graph a node owns not being able to contain that node.
pub(crate) struct Inside {
    /// Who owns the graph the owning node lives in: the next link up.
    owner: GraphOwner,
    /// The owning node's index in that graph.
    node: Task,
    /// The graph itself, when it could not be left where it lies: a subflow's
    /// subgraph, moved out of the `Mutex` the body built it behind so that its
    /// jobs need no guard. `None` for an adopted module's graph, which the
    /// path above reads in place.
    leased: Option<Graph>,
}

/// What the jobs of one graph count down to: upstream's `NodeBase* _parent`,
/// as the two cases its comparison against `node->_topology` tells apart.
#[derive(Clone, Debug)]
pub(crate) enum Parent {
    /// The run itself, which is every top-level node's parent: upstream's
    /// `parent == node->_topology`, carrying nothing because the job holds the
    /// topology already. A free node with nothing above it carries this too, the
    /// run it names then being the sentinel [`Topology::free_standing`] --
    /// upstream's *null* `_parent` over its null `_topology`.
    Run,
    /// A graph nested inside the run: a node whose own graph is running, or
    /// the anchor a [`corun`](crate::Executor::corun) waits on.
    Nested(Arc<Nested>),
}

/// The parent of a nested graph, shared by every job of that graph: upstream's
/// `NodeBase*` pointing at something that is not the topology, which is a node
/// whose own graph is running or the anchor a `corun` keeps on its frame. This
/// is both, the jobs under it treating the two identically -- they decrement a
/// join counter, and the last to drive it to zero looks at what to do next.
#[derive(Debug)]
pub(crate) struct Nested {
    /// The state words when there is no node, which is upstream's local
    /// `NodeBase anchor` in `corun`: a counter for the frame waiting to poll, the
    /// explicit-anchor bit it holds while it waits, and the payload slot a
    /// failure under it reaches. Unused when `resume` is set.
    anchor: NodeBase,
    /// The job of the node this graph hangs off, which is both where the real
    /// state words are and what the last child to finish re-queues to resume it.
    /// `None` for a corun anchor, which has no node.
    resume: Option<Job>,
}

impl Parent {
    /// Accounts for `jobs` more jobs of this parent's graph becoming ready,
    /// before any of them is queued: upstream's
    /// `parent->_join_counter.fetch_add(num_srcs)` in `_schedule_graph`, and
    /// its `fetch_add(1)` in `_invoke`'s two successor loops.
    pub(crate) fn count_queued(&self, topology: &Topology, jobs: usize) {
        match self {
            Self::Run => topology.jobs_queued(jobs),
            Self::Nested(nested) => {
                nested
                    .base()
                    .join_counter()
                    .fetch_add(jobs, Ordering::Relaxed);
            }
        }
    }

    /// Whether any parent above this one has been cancelled or has failed, or the
    /// run the chain roots at has: the ancestor half of upstream's
    /// `Node::_is_parent_cancelled`. `topology` is the run of the job whose parent
    /// this is, and it travels with the walk because the root's run is not always
    /// that one: a free node spawned under a node of a real run holds the sentinel
    /// on its own job while the chain above it roots at the run that is waiting.
    /// Reading every level, where upstream reads the immediate parent only,
    /// catches a failure in a *sibling* subtree.
    pub(super) fn ancestor_cancelled(&self, topology: &Topology) -> bool {
        let mut parent = self;
        // Re-borrowed rather than taken by `mut` reference, which is what
        // lets the walk step onto a run held by a job further up the chain.
        let mut topology: &Topology = topology;
        loop {
            let Self::Nested(nested) = parent else {
                // The root: the run somebody started, or the sentinel, which
                // is never cancelled.
                return topology.cancelled();
            };

            // Upstream's `ESTATE::CANCELLED | ESTATE::EXCEPTION` on a
            // `NodeBase`, which is the pair `Cancellable::cancelled` reads on
            // the topology: the two mean the same thing to the scheduler.
            let estate = nested.base().estate().load(Ordering::Relaxed);
            if estate & (Cancellable::CANCELLED | Cancellable::EXCEPTION) != 0 {
                return true;
            }

            match nested.resume() {
                Some(job) => {
                    parent = job.parent();
                    topology = job.topology();
                }
                // A corun anchor, whose chain names no run at all: the frame
                // that is waiting holds the only state above this.
                None => return false,
            }
        }
    }
}

impl Nested {
    /// The parent a nested graph is to be scheduled under: `resume` is the job of
    /// the node whose graph it is, and `None` is the anchor case, a
    /// [`corun`](crate::Executor::corun) with no node to resume. Shared from the
    /// start: every job of the graph holds a share, and so may its scheduler.
    pub(crate) fn new(resume: Option<Job>) -> Arc<Self> {
        Arc::new(Self {
            anchor: NodeBase::new(),
            resume,
        })
    }

    /// This parent as a [`Parent`], to schedule a graph under.
    pub(crate) fn as_parent(self: &Arc<Self>) -> Parent {
        Parent::Nested(Arc::clone(self))
    }

    /// The state words this parent's children count down: the parent node's
    /// own, or the anchor's when there is no node.
    pub(crate) fn base(&self) -> &NodeBase {
        match &self.resume {
            Some(job) => job.node().base(),
            None => &self.anchor,
        }
    }

    /// The job that resumes the parent node, if there is one.
    pub(crate) fn resume(&self) -> Option<&Job> {
        self.resume.as_ref()
    }

    /// How many jobs of this parent's graph are still outstanding, read without
    /// blocking: upstream's `p->_join_counter.load(acquire)`, the predicate its
    /// `_corun_graph` waits on. Zero means every job of it has torn down.
    pub(crate) fn outstanding(&self) -> usize {
        self.base().join_counter().load(Ordering::Acquire)
    }
}

impl GraphOwner {
    /// The graph of the run itself, held through that run's hold on the
    /// taskflow's storage. A share rather than the caller's borrow of it,
    /// because every job built over it keeps one.
    pub(crate) fn run(storage: &Arc<Storage>) -> Self {
        Self::Run(Arc::clone(storage))
    }

    /// The graph of a taskflow's storage reached without a run's hold on it: a
    /// module task's target.
    pub(crate) fn flow(flow: &Arc<Flow>) -> Self {
        Self::Flow(Arc::clone(flow))
    }

    /// The graph the node `node` of `owner`'s graph owns, which is the only
    /// thing that names an adopted module's graph or a subflow's subgraph.
    /// `leased` is the graph itself for a subgraph that had to come out of its
    /// lock, and `None` for one read where it lies.
    pub(crate) fn inside(owner: &Self, node: Task, leased: Option<Graph>) -> Self {
        Self::Inside(Arc::new(Inside {
            owner: owner.clone(),
            node,
            leased,
        }))
    }

    /// The graph this owner names: upstream's `Graph&`, resolved rather than
    /// passed. Every borrow it hands out is derived from `&self`, which is
    /// what keeps the whole of this safe.
    ///
    /// # Panics
    ///
    /// Panics if a node on the path is not a live node of its graph, which an
    /// owner the scheduler built cannot have -- see [`Job::node`].
    pub(crate) fn graph(&self) -> &Graph {
        match self {
            Self::Run(storage) => storage.graph(),
            Self::Flow(flow) => flow.graph(),
            Self::Inside(inside) => inside.graph(),
            Self::Async(node) => node.graph(),
        }
    }

    /// Whether the graph this names has a task a run could start at:
    /// `Flow::has_source_task` over any owner. Every arm that runs a nested graph
    /// asks first: one with nothing to start at queues no job, so a node preempted
    /// for it would never be resumed.
    pub(crate) fn has_source_task(&self) -> bool {
        self.graph()
            .iter()
            .any(|(_, node)| node.num_predecessors() == 0)
    }

    /// Takes the leased graph back out of this owner, if this is the last share of
    /// it: `Subflow::join`'s way home, and the one caller that *can* ask, owning a
    /// share and not being a job. `None` means a straggling job of a graph nested
    /// inside this one still holds a share, or that this is no lease.
    pub(crate) fn take_lease(self) -> Option<Graph> {
        match self {
            Self::Run(_) | Self::Flow(_) | Self::Async(_) => None,
            // Taking the field rather than moving it out, because `Inside`
            // implements `Drop` -- and leaving `None` behind is exactly what
            // keeps that `Drop` from returning the graph a second time.
            Self::Inside(inside) => {
                let mut inside = Arc::into_inner(inside)?;
                inside.leased.take()
            }
        }
    }

    /// The address that identifies the graph this owner names, for the hash
    /// and the `Debug` a job needs: the allocation it holds a share of, one
    /// per graph either way, so every job of one scheduled graph agrees on it.
    /// A pointer rather than the graph's own address, a path walk not being
    /// what a hash is for.
    pub(super) fn identity(&self) -> *const () {
        match self {
            Self::Run(storage) => Arc::as_ptr(storage).cast(),
            Self::Flow(flow) => Arc::as_ptr(flow).cast(),
            Self::Inside(inside) => Arc::as_ptr(inside).cast(),
            Self::Async(node) => Arc::as_ptr(node).cast(),
        }
    }
}

impl Storage {
    /// A hold on `flow` whose last share takes `run` out of `executor`'s count
    /// and then releases `run`'s waiter: what [`Topology::for_run`] gives a
    /// run somebody may be waiting on, through either of the two waits.
    pub(super) fn gating(
        flow: Arc<Flow>,
        run: &Weak<Topology>,
        executor: Arc<Shared>,
    ) -> Arc<Self> {
        Arc::new(Self {
            flow: Some(flow),
            run: run.clone(),
            executor: Some(executor),
        })
    }

    /// A hold on `flow` that releases nobody and accounts for no run: what a
    /// topology built by [`Topology::new`] gets, neither kind of run that reaches
    /// that constructor being waited for through a completion slot. A
    /// [`corun`](crate::Executor::corun) frame spins rather than parking, so what
    /// it needs is not a release but a *question it can ask*: it takes the share
    /// out of its run, schedules its graph through it and waits for it to become
    /// unique.
    pub(super) fn ungated(flow: Arc<Flow>) -> Arc<Self> {
        Arc::new(Self {
            flow: Some(flow),
            run: Weak::new(),
            executor: None,
        })
    }

    /// The storage this is a hold on. Panics once [`Drop`] has released it, which
    /// no caller can observe: the only `None` is the one the destructor leaves
    /// behind.
    pub(crate) fn flow(&self) -> &Arc<Flow> {
        self.flow
            .as_ref()
            .expect("a run's hold on its storage is released only as it is dropped")
    }

    /// The graph, which is what the jobs holding this resolve their task index
    /// in.
    fn graph(&self) -> &Graph {
        self.flow().graph()
    }

    /// Whether dropping the last share of this hold is what releases the
    /// thread waiting on the run: `false` for an [`ungated`](Storage::ungated)
    /// hold, whose `run` is an empty [`Weak`]. Asked by
    /// [`Topology::release_flow`] on a live topology, so a gating hold cannot
    /// answer `false` by outliving its run.
    pub(super) fn gates_completion(&self) -> bool {
        self.run.strong_count() != 0
    }
}

impl Drop for Storage {
    /// Releases the storage, then the run's place in the executor's count,
    /// then the thread waiting on the run, in that order -- the module docs'
    /// wake rule, and the whole of the guarantee
    /// [`Taskflow::graph_mut`](crate::Taskflow::graph_mut) makes to both of
    /// the threads that can be waiting. Whichever thread drops the last share
    /// runs it: the tear-down's [`Topology::release_flow`], or a worker still
    /// dropping a copy of a preempted node's job.
    fn drop(&mut self) {
        drop(self.flow.take());

        // `None` for a run that accounts for itself or was never counted: see
        // the field.
        if let Some(executor) = self.executor.take() {
            executor.decrement_topology();
        }

        // `None` only for a run nobody can be waiting on: see the field.
        if let Some(run) = self.run.upgrade() {
            run.finish();
        }
    }
}

impl Inside {
    /// The graph this node owns. Panics if the owning node is not a live node
    /// of its graph, as [`GraphOwner::graph`] says.
    fn graph(&self) -> &Graph {
        // The lease first: a graph that had to come out of its lock to be run
        // is the one thing the path below cannot read in place.
        if let Some(leased) = &self.leased {
            return leased;
        }

        match self.owner.graph().node(self.node).handle() {
            Handle::AdoptedModule(graph) => graph,
            // Unreachable for an owner the scheduler built: it was made from
            // the very handle the dispatch arm was holding, and a handle can
            // only be replaced through `&mut Taskflow`, which no run allows.
            // A subflow's handle does not reach here either -- its graph is
            // the lease above.
            handle => unreachable!("a nested graph hangs off {handle:?}, which owns none"),
        }
    }
}

impl Drop for Inside {
    /// Sends a leased subgraph home, which is the only reason this type has a
    /// destructor: the last share of a lease runs it, for the reason the
    /// module docs' wake rule gives. What "home" means -- back into the node's
    /// `Mutex`, or dropped, which is upstream's `g.clear()` -- is the
    /// subflow's policy, in
    /// [`subflow::return_subgraph`](crate::executor::subflow::return_subgraph).
    /// Panics if the owning node is not a live subflow node of its graph,
    /// which a lease the scheduler built cannot have: the owners above it are
    /// owning.
    fn drop(&mut self) {
        if let Some(graph) = self.leased.take() {
            subflow::return_subgraph(self.owner.graph().node(self.node), graph);
        }
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}

    // One variant owns a `Graph` outright -- a leased subgraph -- so the
    // graph itself has to be shareable, not merely reachable through storage
    // that is.
    assert_send_sync::<GraphOwner>();
    assert_send_sync::<Graph>();
    assert_send_sync::<Flow>();
    assert_send_sync::<AsyncNode>();
    // A `Nested` is shared with every job under it, so `Arc<Nested>` is
    // `Send` only if this is both.
    assert_send_sync::<Nested>();
};

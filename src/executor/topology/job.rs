//! Translated from the queued `Node*` of `_schedule` and `_invoke` in
//! `taskflow/core/executor.hpp` (Taskflow v4.1.0).
//!
//! One queued task of one run: upstream's `Node*`, plus what upstream reaches
//! through the node's `_topology` and `_parent` pointers.
//!
//! # Design
//!
//! Upstream's queues hold a bare `Node*`, a `Node` of its graph knowing its
//! own topology and its own parent. A node here is a slot in an arena that
//! knows nothing about the run it is taking part in, so the job carries both
//! instead -- four handles, each owning what it names, which is `DESIGN.md`'s
//! "A run owns the graph it walks" at the one value the scheduler passes
//! around; `ownership` has the shapes those handles are.
//!
//! Two consequences are this module's own. A job addresses its node by
//! [`Task`] index rather than by pointer, so the node comes out of its graph
//! through safe code and a graph nobody can edit while a job names it cannot
//! invalidate an index either; and [`Job::finish`] *consumes* the job, which
//! is how the compiler enforces that a worker gives up its share of the graph
//! before the decrement that may end the run.

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::ownership::{GraphOwner, Parent};
use super::run::{Cancellable, Topology};
use crate::executor::async_node::AsyncNode;
use crate::graph::{Graph, Node, NodeBase, Task};

/// One unit of work in a worker's queue: upstream's `Node*`, plus what upstream
/// reaches through the node's `_topology` and `_parent` pointers.
///
/// `Clone` because resuming a preempted node means queueing the job it was
/// dequeued as a second time, and because the parent chain shares the job of
/// each parent with every child under it; every field is a handle, so a clone
/// is an index and two or three atomic increments. Five words, the owner being
/// two of them: a tag and one pointer.
#[derive(Clone)]
pub(crate) struct Job {
    /// Which task of the graph to run.
    pub(super) task: Task,
    /// Who owns the graph `task` indexes, which is the whole of what makes
    /// that index resolvable.
    ///
    /// The job holds its own owner rather than reading the run's, and for a
    /// top-level node the two are the same `Arc<Flow>`: the run's share has to
    /// be *takeable* -- see [`Topology::release_flow`] -- and therefore lives
    /// behind a mutex, which is no place for a hot path to go.
    owner: GraphOwner,
    /// The run `task` is part of.
    topology: Arc<Topology>,
    /// What this job's graph was scheduled under: the run itself for a
    /// top-level node, or a `Nested` parent for a node of a nested graph or of
    /// a corun.
    parent: Parent,
}

impl Job {
    /// Builds the job that runs `task` of `owner`'s graph as part of
    /// `topology`, under `parent`.
    pub(super) fn new(
        task: Task,
        owner: &GraphOwner,
        topology: &Arc<Topology>,
        parent: &Parent,
    ) -> Self {
        Self {
            task,
            owner: owner.clone(),
            topology: Arc::clone(topology),
            parent: parent.clone(),
        }
    }

    /// The job that runs the free-standing node of `node`, which belongs to no run
    /// and to no graph the caller built: upstream's `_schedule(Worker&, Node*)`
    /// over an async node. `parent` is whatever scheduled this task --
    /// [`Parent::Run`] over the sentinel for a task submitted to the executor,
    /// which is upstream's null parent, or a [`Parent::Nested`] for one spawned by
    /// a runtime body or owned by a task group -- and the run is the sentinel
    /// [`Topology::free_standing`]. The share of the node is upstream's reference
    /// count on the same node for the same reason: a caller who drops the last
    /// handle on a task that has not run yet must not free the node under the
    /// worker that is about to run it.
    pub(crate) fn free(node: &Arc<AsyncNode>, parent: &Parent) -> Self {
        Self {
            task: AsyncNode::TASK,
            owner: GraphOwner::Async(Arc::clone(node)),
            topology: Arc::clone(Topology::free_standing()),
            parent: parent.clone(),
        }
    }

    /// The jobs that start a graph: one per source task of it. Call it only
    /// after [`set_up_graph`](super::set_up_graph) has returned a non-zero
    /// count for the same graph and `parent` has been told about those
    /// sources, since that count is what the parent's join counter was raised
    /// by. An associated function rather than a method on [`Topology`] because
    /// what it needs is the topology's *share*, and `self: &Arc<Self>` is not
    /// a receiver Rust has.
    pub(crate) fn sources<'a>(
        owner: &'a GraphOwner,
        topology: &'a Arc<Topology>,
        parent: &'a Parent,
    ) -> impl Iterator<Item = Self> + 'a {
        owner
            .graph()
            .iter()
            .filter(|(_, node)| node.num_predecessors() == 0)
            .map(move |(task, _)| Self::new(task, owner, topology, parent))
    }

    /// The graph this job's node lives in, whichever of the five kinds it is.
    pub(crate) fn graph(&self) -> &Graph {
        self.owner.graph()
    }

    /// The owner of a graph this job's *own* node owns: this job's owner plus
    /// its task index, which is [`GraphOwner::inside`].
    ///
    /// What a dispatch arm hands to `Shared::schedule_graph` for the two kinds
    /// whose graph is not a share of anything, where the `Handle::Module` arm
    /// hands over the `Arc<Taskflow>` its own handle holds.
    pub(crate) fn graph_inside(&self, leased: Option<Graph>) -> GraphOwner {
        GraphOwner::inside(&self.owner, self.task, leased)
    }

    /// The node to run.
    ///
    /// # Panics
    ///
    /// Panics if the job's task is not a live node of the graph, which cannot
    /// happen for a job the scheduler built: a node can only be erased through
    /// `&mut Taskflow`, and `Taskflow::graph_mut` refuses while a share of the
    /// storage -- this job's own, for one -- is outstanding. The same holds of
    /// every node on the path to a nested graph.
    pub(crate) fn node(&self) -> &Node {
        self.graph().node(self.task)
    }

    /// The free-standing node this job runs, or `None` for a job of a graph
    /// somebody built: upstream's
    /// `std::get_if<Node::DependentAsync>(&node->_handle)`, which is how both of
    /// its async tear-downs reach the state that is a *free* node's. The share
    /// rather than a borrow because the one caller, the dependent-async tear-down,
    /// may need to keep the node -- to name it as a dependency of something else.
    pub(crate) fn async_node(&self) -> Option<&Arc<AsyncNode>> {
        match &self.owner {
            GraphOwner::Async(node) => Some(node),
            GraphOwner::Run(_) | GraphOwner::Flow(_) | GraphOwner::Inside(_) => None,
        }
    }

    /// What this job's graph was scheduled under.
    pub(crate) fn parent(&self) -> &Parent {
        &self.parent
    }

    /// The run this job is part of.
    pub(crate) fn topology(&self) -> &Topology {
        &self.topology
    }

    /// This job's *share* of the run, which is what scheduling a graph needs:
    /// every job of a nested graph holds one of its own, so
    /// `Shared::schedule_graph` takes the share rather than a reference.
    pub(crate) fn topology_share(&self) -> &Arc<Topology> {
        &self.topology
    }

    /// Records that the work of this job's node failed with `payload`, delivering
    /// it to whatever is waiting for the graph that node is part of: upstream's
    /// `Executor::_process_exception`.
    ///
    /// From the failing node upwards, every base on the parent chain is marked
    /// `EXCEPTION` -- which is what cancels the rest of each nested graph the
    /// failure is inside of -- and the walk stops at the first base that is
    /// explicitly anchored, failing that at the innermost implicitly anchored one,
    /// failing that at the failing node itself. The run a chain roots at is an
    /// anchor by definition, where upstream constructs its topology
    /// `EXPLICITLY_ANCHORED`; the sentinel [`Topology::free_standing`] is the one
    /// root that is not, so a chain rooted there ends like upstream's null
    /// `_topology`, with the payload on the node where
    /// [`AsyncTask::take_panic`](crate::AsyncTask::take_panic) reads it.
    /// `DESIGN.md` has the priority among the three and what the one departure
    /// from upstream costs. The run the walk carries is the one of the job at the
    /// *root* rather than this job's own, the two differing for exactly one shape:
    /// a free node spawned under a node of a real run.
    ///
    /// Nothing here can dangle and nothing here is a pointer walk: a
    /// [`Parent::Nested`] owns the job of the parent node, which owns its own
    /// parent up to the root, so every base the walk touches is alive because the
    /// walk's own job is. That is stronger than it looks -- a job of a nested graph
    /// has not yet decremented its parent's join counter, so no ancestor can be
    /// finished either, and no frame can have stopped waiting for one.
    pub(crate) fn record_panic(&self, payload: Box<dyn Any + Send>) {
        let node = self.node().base();
        let mut base = node;
        let mut parent = Some(&self.parent);
        // The run of the job whose parent `parent` is, which is the run a
        // `Parent::Run` at the end of the walk names. It travels with the
        // chain rather than being read off `self` because the two are not
        // always the same run -- see the docs above.
        let mut topology: &Topology = &self.topology;
        let mut implicit: Option<Anchor<'_>> = None;

        // Upstream's `while(ea && !(ea->_estate & EXPLICITLY_ANCHORED))`, with
        // its `ea = ea->_parent` spelled as a step along this job's chain and
        // its null pointer as the `None` two of the arms below produce.
        let explicit = loop {
            if base.is_explicitly_anchored() {
                break Some(Anchor::Base(base));
            }

            base.estate()
                .fetch_or(Cancellable::EXCEPTION, Ordering::Relaxed);
            // Only the innermost one, as upstream's comment says: the first
            // implicit anchor the walk meets is the closest to the failure.
            if implicit.is_none() && base.is_implicitly_anchored() {
                implicit = Some(Anchor::Base(base));
            }

            match parent {
                // Upstream's `ea == nullptr`: the chain is over, with no
                // frame on it waiting. A corun anchor whose guard is not held
                // ends a chain this way.
                None => break None,
                // Upstream's other null, `_topology == nullptr` on a free
                // node: the sentinel is a run nothing can wait for, so a
                // chain rooted at it ends with no anchor rather than with one
                // nobody can read.
                Some(Parent::Run) if topology.is_free_standing() => break None,
                // The root, anchored for the length of the run.
                Some(Parent::Run) => break Some(Anchor::Run(topology)),
                Some(Parent::Nested(nested)) => {
                    base = nested.base();
                    match nested.resume() {
                        Some(job) => {
                            parent = Some(job.parent());
                            topology = job.topology();
                        }
                        None => parent = None,
                    }
                }
            }
        };

        // Upstream's `if(ea) ... else if(ia)`, as one choice rather than two
        // tests: an explicit anchor that has *already* caught a payload does
        // not hand this one on to an implicit anchor, which two `if let`s in a
        // row would lose.
        let anchor = match (explicit, implicit) {
            (Some(anchor), _) | (None, Some(anchor)) => anchor,
            // Upstream's last line: no anchor anywhere on the chain, so there
            // is nothing waiting -- an execution with no external control over
            // it, which is upstream's own description. A *silent* async task
            // submitted to the executor is exactly that.
            (None, None) => Anchor::Base(node),
        };

        // The first failure under this anchor is the one it reports. A later
        // one is dropped, which is the one place this walk does not follow
        // upstream; `DESIGN.md` has why a payload nothing can read back is not
        // parked here.
        if anchor.claim_panic() {
            anchor.keep_panic(payload);
        }
    }

    /// Resolves this job into the node to run, the graph holding it and the run it
    /// is part of. Safe, and that is the point of the type: the job owns a share of
    /// the storage and of the run, so neither can have gone away, and the node
    /// comes out of the graph by index. Panics as [`Job::node`] does.
    pub(crate) fn parts(&self) -> (&Node, &Graph, &Topology) {
        (self.node(), self.graph(), &self.topology)
    }

    /// Whether this job may be skipped because its run or some parent above it has
    /// been cancelled or failed: upstream's `Node::_is_parent_cancelled`, both
    /// halves of its condition. A job of a cancelled run is skipped by the first
    /// term, and a job whose *parent* failed -- a subflow whose body panicked, say
    /// -- by the second, even though the run as a whole is still going. A free
    /// async node answers through the same two terms: the first reads the sentinel
    /// run, which nothing ever cancels, and the second walks whatever chain is
    /// above it, which is how a task group cancels the children it owns.
    pub(crate) fn is_parent_cancelled(&self) -> bool {
        self.topology.cancelled() || self.parent.ancestor_cancelled(&self.topology)
    }

    /// Whether `graph` is a graph this job is already running inside of: the one
    /// its own node lives in, or the one any node above it on the parent chain
    /// lives in. Upstream has nothing to compare, its `_set_up_graph` re-arming
    /// whatever it is handed; `executor::module` is the one caller and has what
    /// arming a graph a run is inside of would cost.
    ///
    /// The **parent** chain rather than the owner chain is the subtlety: a composed
    /// `Arc<Taskflow>`'s graph names nothing above it, so two taskflows composing
    /// each other are a cycle no [`GraphOwner`] link records, where each parent
    /// link is the job of a node whose graph is still in flight. A corun's anchor
    /// ends a chain without naming the job that started it, so a graph enclosing a
    /// corun is not seen -- which is the limit of what this answers, and
    /// [`crate::Work::invoke`]'s `try_lock` is what reports that case instead.
    pub(crate) fn runs_inside(&self, graph: &Graph) -> bool {
        let mut job = self;
        loop {
            if std::ptr::eq(job.graph(), graph) {
                return true;
            }

            // Upstream's `parent == node->_topology` as the end of the walk:
            // the run's own graph is the graph of the job the chain ends at,
            // so it has been compared by the time this returns.
            let Parent::Nested(nested) = job.parent() else {
                return false;
            };
            match nested.resume() {
                Some(parent) => job = parent,
                None => return false,
            }
        }
    }

    /// The job that runs `task` as part of the same run of the same graph,
    /// under the same parent: what a worker releasing a successor queues.
    pub(crate) fn successor(&self, task: Task) -> Self {
        Self::new(task, &self.owner, self.topology_share(), &self.parent)
    }

    /// Spends this job, handing back what the worker still owes: the parent to
    /// count down and the run to account the job to.
    ///
    /// The job's own share of whatever owns its graph is released here, and *here*
    /// is the point: the caller's next move is to decrement the join counter that
    /// may end the run, so this keeps the window a share is outstanding for as
    /// narrow as it can be -- one job, from the moment it is counted to the moment
    /// it is spent. Consuming the job is what makes the compiler enforce it, the
    /// node and the graph a worker was holding being borrows of this value.
    /// Narrowing that window is not the whole of the editable-taskflow guarantee,
    /// which rests on the wake rule in `ownership`'s docs instead. For a job of a
    /// nested graph the owner released here is the path or the lease rather than a
    /// share of the taskflow, the share it keeps through its [`Parent`] going
    /// later.
    pub(crate) fn finish(self) -> Finished {
        let Self {
            task: _,
            owner,
            topology,
            parent,
        } = self;

        // Before the caller's `job_finished`, which is the whole reason this
        // method exists rather than letting the job fall out of scope.
        drop(owner);
        Finished { parent, topology }
    }
}

/// Where a failure found by [`Job::record_panic`] is delivered.
///
/// Upstream needs no such type: every link of its parent chain is a
/// `NodeBase*`, its topology included, so the anchor it stops at is one
/// pointer whatever kind of thing it turned out to be. Here the chain's root
/// is a run rather than a base and keeps its payload in the completion slot a
/// `wait` reads, so this names the two cases and gives them the one pair of
/// operations the walk needs.
enum Anchor<'a> {
    /// A run somebody started, which is anchored by definition for as long as
    /// it is running. Never the sentinel: nobody is waiting on that one.
    Run(&'a Topology),
    /// A node waiting for its own nested graph, or the anchor a `corun` keeps
    /// on its frame.
    Base(&'a NodeBase),
}

impl Anchor<'_> {
    /// Claims this anchor's one payload slot, reporting whether this caller
    /// was the first to do so.
    fn claim_panic(&self) -> bool {
        match self {
            Self::Run(topology) => topology.claim_panic(),
            Self::Base(base) => base.claim_panic(),
        }
    }

    /// Puts `payload` in the slot this caller has just claimed.
    fn keep_panic(&self, payload: Box<dyn Any + Send>) {
        match self {
            Self::Run(topology) => topology.keep_panic(payload),
            Self::Base(base) => base.keep_panic(payload),
        }
    }
}

/// What a job still owes once its work is done: upstream's two arguments to
/// `_tear_down_nonasync`, minus the node it no longer needs.
///
/// Built by [`Job::finish`] and spent by `WorkerThread::finish_job`. A type
/// rather than a pair so that the order [`Job::finish`] documents cannot be
/// got wrong by a caller holding the pieces: the only way to reach the
/// counters is to have given the graph up first.
pub(crate) struct Finished {
    /// The parent whose join counter this job decrements.
    parent: Parent,
    /// The run the job belonged to.
    topology: Arc<Topology>,
}

impl Finished {
    /// The parent to count down and the run to account the job to.
    pub(crate) fn into_parts(self) -> (Parent, Arc<Topology>) {
        (self.parent, self.topology)
    }
}

impl core::fmt::Debug for Job {
    /// Hand-written because the derived one would print the whole graph: a
    /// job's owner is an owning handle, not a field worth expanding. The two
    /// addresses are what identifies a job, as upstream's `Node*` identifies
    /// its own.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Job")
            .field("task", &self.task)
            .field("owner", &self.owner.identity())
            .field("topology", &Arc::as_ptr(&self.topology))
            .field("parent", &self.parent)
            .finish()
    }
}

impl std::hash::Hash for Job {
    /// Hand-written for the same reason [`Debug`](core::fmt::Debug) is, and it
    /// has one caller: `Buffers::push` routes a job to one of the shared
    /// overflow buffers by its hash, where upstream hashes the `Node*` it is
    /// about to push. The owner's allocation and the run's stand in for that
    /// pointer, and the task index is what separates two jobs of one run.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.task.hash(state);
        self.owner.identity().hash(state);
        Arc::as_ptr(&self.topology).hash(state);
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}

    // A job is built on the thread that schedules a task and dequeued by
    // whichever worker takes it, so it has to be `Send` -- which, now that it
    // owns its shares rather than pointing at them, is derived rather than
    // asserted: `Arc<T>` is `Send` exactly when `T` is `Send + Sync`. It has
    // to be `Sync` too, because the parent chain shares the job of a parent
    // with every job under it.
    assert_send_sync::<Job>();
    // A job hands out a `&Node` on the worker that runs it.
    assert_send_sync::<Node>();
};

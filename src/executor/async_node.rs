//! The free-standing node an async task is: a node that belongs to no
//! taskflow, no graph the caller built and no run.
//!
//! Upstream has no such type, which is why this module names no source: its
//! async node is an ordinary `Node` out of the object pool with
//! `_topology == nullptr`, held alive by a reference count in the low 24 bits
//! of `_estate`. A `Node` here is a slot in a [`Graph`]'s arena and nothing
//! else, so what this module answers is where the arena for a node that
//! belongs to nobody comes from: the task owns one, of one node, and the `Arc`
//! around it is the task's lifetime.
//!
//! # Design
//!
//! `DESIGN.md` has that decision -- why the `Arc` is upstream's refcount, why
//! a free node's job names a sentinel run, and what the shape costs. What is
//! left to this file is the two pieces of state a free node needs and a graph
//! node does not: upstream's `ESTATE::FINISHED` bit and, for a dependent-async
//! node, the successor list it keeps in `_edges` under its `ESTATE::LOCKED`
//! spin bit. Both live behind one lock because the two operations on them
//! have to be atomic with respect to *each other*: a lost race between naming
//! a dependency ([`AsyncNode::push_successor`]) and finishing one
//! ([`AsyncNode::finish`]) is a task that never runs or one that runs twice,
//! each of those calls deciding what the other will find. Nothing is
//! scheduled under that lock, a woken job being free to run before the call
//! that woke it returns; upstream releases successors under its spin bit,
//! which it can afford, its scheduling path being lock-free.

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::executor::topology::Job;
use crate::graph::{Graph, Handle, Node, Task};

/// The one-node graph an async or dependent-async task is, plus the two pieces
/// of state a node outside any run needs. Crate-private -- upstream's `friend
/// class Executor` -- so a caller reaches one only through
/// [`AsyncTask`](crate::AsyncTask).
pub(crate) struct AsyncNode {
    /// The arena the node lives in, with one live node in it --
    /// [`AsyncNode::TASK`] -- never edited after the constructor: a
    /// dependent-async task's dependencies are the successor list below, not
    /// edges, a dependency of a free node being free itself.
    graph: Graph,
    /// Upstream's `ESTATE::FINISHED`: whether the one run of this task is
    /// over, which is what [`AsyncTask::is_done`](crate::AsyncTask::is_done)
    /// reports. Written under `successors`, read without it, and set in
    /// `_tear_down_dependent_async` alone upstream, a plain async node having
    /// no handle to be asked through.
    finished: AtomicBool,
    /// The jobs to release when this task finishes: upstream's `_edges` on a
    /// `DEPENDENT_ASYNC` node, under its `ESTATE::LOCKED` spin bit, and always
    /// empty for a [`Handle::Async`] node, which nothing can depend on. Jobs
    /// rather than nodes, so that what is kept is the value the scheduler
    /// would queue; a `Mutex` where upstream spins, since the critical section
    /// is never user code and parking is better when a holder can be
    /// descheduled.
    successors: Mutex<Vec<Job>>,
}

impl AsyncNode {
    /// The one node of the arena, which every job of a free node names: a
    /// constant, [`Graph::add_node`] handing out indices from zero.
    pub(crate) const TASK: Task = Task::new(0);

    /// The free-standing node of an async task whose body is `handle`, shared
    /// from the start as upstream's `AsyncTask(Node*)` takes the first
    /// reference count in its constructor: a dependency may be handed out
    /// before the task is queued. Panics in a debug build if `handle` is
    /// neither async handle -- a free node with (say) a subflow handle is one
    /// whose arm preempts it with nothing above it to resume it.
    pub(crate) fn new(name: impl Into<String>, handle: Handle) -> Arc<Self> {
        debug_assert!(
            handle.is_async() || handle.is_dependent_async(),
            "a free-standing node carries an async handle, not {handle:?}"
        );

        let mut graph = Graph::new();
        let task = graph.add_named_node(name, handle);
        debug_assert_eq!(task, Self::TASK, "the one node of a fresh arena is 0");

        Arc::new(Self {
            graph,
            finished: AtomicBool::new(false),
            successors: Mutex::new(Vec::new()),
        })
    }

    /// The arena, which `GraphOwner::Async` resolves a job's index against.
    pub(crate) fn graph(&self) -> &Graph {
        &self.graph
    }

    /// The node itself, for the one caller that has the `AsyncNode` rather
    /// than a job of it: the handle's payload slot. Cannot panic, the
    /// constructor having put the node at [`AsyncNode::TASK`].
    pub(crate) fn node(&self) -> &Node {
        self.graph.node(Self::TASK)
    }

    /// Whether the one run of this task is over: upstream's
    /// `_estate.load(acquire) & ESTATE::FINISHED`. Acquire, as upstream's is,
    /// and lock-free -- `true` pairs with [`AsyncNode::finish`]'s release
    /// store, `false` says only that it had not finished during the call.
    pub(crate) fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }

    /// Takes the payload of a failure parked on this node, out of the
    /// `NodeBase` slot every node has: upstream's `exception_ptr()`, and what
    /// [`AsyncTask::take_panic`](crate::AsyncTask::take_panic) hands back. A
    /// silent task's parent chain roots at no waiter, so this is where its
    /// failure stays.
    pub(crate) fn take_panic(&self) -> Option<Box<dyn Any + Send>> {
        self.node().base().take_panic()
    }

    /// Leaves `job` to be released when this task finishes, reporting `false`
    /// if it has finished already and the caller must release it itself:
    /// upstream's `_process_dependent_async`, which takes the lock, tests
    /// `ESTATE::FINISHED` and either pushes or reports the dependency met.
    pub(crate) fn push_successor(&self, job: Job) -> bool {
        let mut successors = self.lock_successors();
        if self.is_finished() {
            return false;
        }
        successors.push(job);
        true
    }

    /// Marks this task finished and hands back the jobs that were waiting for
    /// it, for the caller to release once this lock is gone: upstream's
    /// `_tear_down_dependent_async`, minus the scheduling. The flag is written
    /// under the lock, as the other half of what
    /// [`AsyncNode::push_successor`] decides, and with release ordering, which
    /// makes [`AsyncNode::is_finished`]'s acquire load a happens-before for a
    /// handle holder reading what the body wrote.
    pub(crate) fn finish(&self) -> Vec<Job> {
        let mut successors = self.lock_successors();
        self.finished.store(true, Ordering::Release);
        std::mem::take(&mut *successors)
    }

    /// The successor list. Poisoning is taken rather than propagated, as
    /// everywhere else in the executor: refusing to hand the list back would
    /// strand every task waiting for this one.
    fn lock_successors(&self) -> MutexGuard<'_, Vec<Job>> {
        self.successors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl core::fmt::Debug for AsyncNode {
    /// Hand-written: the derived one would print every job in the list.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AsyncNode")
            .field("node", self.node())
            .field("finished", &self.is_finished())
            .field(
                "successors",
                &self.successors.try_lock().map_or(0, |jobs| jobs.len()),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::topology::Parent;
    use crate::graph::AsyncWork;

    /// A dependent-async node, which is the kind with a successor list.
    fn node() -> Arc<AsyncNode> {
        AsyncNode::new("task", Handle::DependentAsync(AsyncWork::plain(|| {})))
    }

    /// The constructor's half of the module docs: one node, at
    /// [`AsyncNode::TASK`], carrying the handle it was given, with no
    /// dependencies of its own and nothing finished or waiting yet.
    #[test]
    fn a_free_node_is_a_one_node_graph_carrying_its_handle() {
        let free = node();

        assert_eq!(free.graph().len(), 1);
        assert_eq!(free.node().name(), "task");
        assert!(free.node().handle().is_dependent_async());
        assert_eq!(free.node().num_predecessors(), 0, "a free node is a source");
        assert_eq!(free.node().num_successors(), 0);
        assert!(!free.is_finished());
        assert!(free.take_panic().is_none());
    }

    /// The pair the module docs argue has to be atomic, in the order that
    /// makes the successor run: a dependency named *before* the dependency
    /// finished is kept, and finishing hands it back exactly once.
    #[test]
    fn a_successor_named_before_the_finish_is_handed_back_by_it() {
        let dependency = node();
        let successor = Job::free(&node(), &Parent::Run);

        assert!(
            dependency.push_successor(successor.clone()),
            "the dependency has not finished, so the job is kept"
        );

        let released = dependency.finish();
        assert_eq!(released.len(), 1);
        assert!(
            dependency.is_finished(),
            "finishing is what the next namer sees"
        );
        assert!(
            dependency.finish().is_empty(),
            "the list is taken, not copied"
        );
    }

    /// And the other order, which is the one a lost race would turn into a
    /// task that never runs: a dependency named *after* the dependency
    /// finished is refused, so the caller releases the job itself instead of
    /// leaving it on a list nothing will drain again.
    #[test]
    fn a_successor_named_after_the_finish_is_refused() {
        let dependency = node();
        assert!(dependency.finish().is_empty());

        let successor = Job::free(&node(), &Parent::Run);
        assert!(
            !dependency.push_successor(successor),
            "a finished dependency keeps nothing"
        );
        assert!(
            dependency.finish().is_empty(),
            "and the refused job was not kept after all"
        );
    }
}

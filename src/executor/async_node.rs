//! The free-standing node an async task is: a node that belongs to no
//! taskflow, no graph the caller built and no run.
//!
//! Upstream has no such type, which is why this module names no source. Its
//! async node is an ordinary `Node` out of the object pool with
//! `_topology == nullptr`, held alive by a reference count packed into the
//! low 24 bits of `_estate` and handed back to the pool by whichever of the
//! worker and the last `tf::AsyncTask` releases the last count. A `Node` in
//! this port is a slot in a [`Graph`]'s arena and nothing else -- it has no
//! count of its own and no way to exist outside an arena -- so the question
//! this module answers is where the arena for a node that belongs to nobody
//! comes from.
//!
//! The answer is: the async task owns one, with exactly one node in it, and
//! the task's lifetime is the `Arc` around that. Three things fall out of
//! that and all three are the reason it was preferred to the alternatives:
//!
//! - **The `Arc` *is* upstream's refcount.** Handles share it
//!   ([`AsyncTask`](crate::AsyncTask), which is an `Option<Arc<AsyncNode>>`),
//!   and so does every queued [`Job`] of the node, which is what upstream's
//!   count also includes while the task is queued or running. So there is no
//!   second counter to keep in step with it, and no recycle call to get
//!   right: the node goes when the last share goes, whether that is a
//!   handle's or the worker's.
//! - **The node resolves like any other.** A [`Job`] names its node by
//!   [`Task`] index into a graph its [`GraphOwner`](super::topology) resolves,
//!   so a one-node graph makes a free node a job the scheduler already knows
//!   how to run: `Job::parts()` hands the worker a `&Node`, a `&Graph` and a
//!   run, and the dispatch arm that reaches it is a `match` arm on the
//!   handle like the other nine. Nothing in the hot path learns that this
//!   node is special.
//! - **One allocation per async task, beyond the body.** Upstream's cost
//!   model for the async family is one pooled node per task and no topology,
//!   and that is what this is: the `Arc<AsyncNode>` holds the arena inline,
//!   and `Topology::free_standing` -- the run every free node's job claims to
//!   be part of -- is one sentinel shared by every async task in the process.
//!   `executor::topology` has that decision in full.
//!
//! The node's own state words are the `NodeBase` it has as a node, so
//! upstream's `_join_counter`, its `_nstate` preemption bit and the payload
//! slot a silent failure is parked in all come for free and are read by the
//! same code that reads a graph node's. The two things a free node needs that
//! a graph node does not are here instead: the `FINISHED` bit upstream keeps
//! in `_estate`, and -- for a dependent-async node -- the successor list
//! upstream keeps in `_edges` under the `ESTATE::LOCKED` spin bit.
//!
//! Both of those live behind one `Mutex`, and that is deliberate rather than
//! incidental: upstream's spin bit protects exactly the pair, because the
//! only two operations on them have to be atomic *with respect to each
//! other*. Naming a dependency ([`AsyncNode::push_successor`]) must either
//! find the dependency unfinished and leave a successor behind for it, or
//! find it finished and leave nothing; finishing
//! ([`AsyncNode::finish`]) must either see a successor and take it, or set
//! the bit that sends the next namer down the other branch. A lost race
//! either way is a task that never runs or one that runs twice.

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::executor::topology::Job;
use crate::graph::{Graph, Handle, Node, Task};

/// The one-node graph an async or dependent-async task is, plus the two
/// pieces of state a node outside any run needs.
///
/// Crate-private: a caller reaches one only through the public
/// [`AsyncTask`](crate::AsyncTask) handle, which is what upstream's `friend
/// class Executor` plus a private `Node*` member amount to.
pub(crate) struct AsyncNode {
    /// The arena the node lives in, with exactly one live node in it --
    /// [`AsyncNode::TASK`].
    ///
    /// A whole [`Graph`] for one node is three empty `Vec`s and a counter
    /// more than the node itself, and what it buys is that every piece of the
    /// scheduler that resolves a node by index resolves this one too. The
    /// graph is never edited after the constructor: it has no second node to
    /// add an edge to, and a dependent-async task's dependencies are *not*
    /// edges of it -- they are the successor list below, because a dependency
    /// of a free node is another free node in another arena.
    graph: Graph,
    /// Upstream's `ESTATE::FINISHED`: whether the one run of this task is
    /// over, which is what [`AsyncTask::is_done`](crate::AsyncTask::is_done)
    /// reports.
    ///
    /// Written under `successors`, read without it -- see the module docs for
    /// why the write is under a lock at all, and [`AsyncNode::is_finished`]
    /// for why the read need not be.
    finished: AtomicBool,
    /// The jobs to release when this task finishes: upstream's `_edges` on a
    /// `DEPENDENT_ASYNC` node, under its `ESTATE::LOCKED` spin bit.
    ///
    /// Always empty for a [`Handle::Async`] node, which nothing can depend
    /// on. Jobs rather than nodes, because a successor of a free node may be
    /// a free node of its own -- so what is kept is the value the scheduler
    /// would queue, which already owns everything it names.
    ///
    /// A `Mutex` where upstream spins on a bit of the estate word: the
    /// critical section is a push or a `mem::take` and never user code, so
    /// what the two differ in is which of them parks a thread that loses the
    /// race, and parking is the better answer when the holder can be
    /// descheduled.
    successors: Mutex<Vec<Job>>,
}

impl AsyncNode {
    /// The one node of the arena, which every job of a free node names.
    ///
    /// A constant because the arena has exactly one node and
    /// [`Graph::add_node`] hands out indices from zero: the first node of a
    /// fresh graph is index 0, and this arena never gets a second.
    pub(crate) const TASK: Task = Task::new(0);

    /// The free-standing node of an async task whose body is `handle`.
    ///
    /// Shared from the start, because every holder of one is a share:
    /// upstream's `AsyncTask(Node*)` takes the first reference count in its
    /// constructor for the same reason, and for the same purpose -- so that
    /// the submitting call can hand a dependency out before the task it
    /// belongs to has been queued, let alone run.
    ///
    /// # Panics
    ///
    /// Panics in a debug build if `handle` is not one of the two async
    /// handles. A free node with, say, a subflow handle would be a node whose
    /// dispatch arm preempts it with no parent above it to resume it, so the
    /// assertion is about the scheduler and not about tidiness.
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

    /// The arena, which is what a job of this node resolves its index
    /// against: `GraphOwner::Async`'s half of `GraphOwner::graph`.
    pub(crate) fn graph(&self) -> &Graph {
        &self.graph
    }

    /// The node itself, for the one caller that has the `AsyncNode` rather
    /// than a job of it: the handle's payload slot.
    ///
    /// # Panics
    ///
    /// Cannot: the constructor put the node at [`AsyncNode::TASK`] and
    /// nothing erases it -- the graph is not reachable mutably from anywhere.
    pub(crate) fn node(&self) -> &Node {
        self.graph.node(Self::TASK)
    }

    /// Whether the one run of this task is over: upstream's
    /// `_estate.load(acquire) & ESTATE::FINISHED`.
    ///
    /// Acquire, as upstream's load is, and read without the lock the write
    /// takes: a reader that sees `true` has to see everything the body wrote,
    /// which the release store below pairs with, and a reader that sees
    /// `false` has learned only that the task had not finished at some point
    /// during the call -- which is all upstream's load promises either.
    pub(crate) fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }

    /// Takes the payload of a failure parked on this node, if there was one:
    /// what [`AsyncTask::take_panic`](crate::AsyncTask::take_panic) hands
    /// back.
    ///
    /// The node's own slot, which is the `NodeBase` one every node has. For a
    /// free node it holds what upstream's `exception_ptr()` reads: the one
    /// failure of the one task this node is, parked here because a silent
    /// task's chain of parents roots at neither a run nor a waiting frame and
    /// there is nothing above it to deliver to -- `Job::record_panic`'s last
    /// case, which was ported before anything could reach it.
    pub(crate) fn take_panic(&self) -> Option<Box<dyn Any + Send>> {
        self.node().base().take_panic()
    }

    /// Leaves `job` to be released when this task finishes, reporting
    /// `false` if it has finished already and the caller must therefore
    /// release it itself.
    ///
    /// Upstream's `_process_dependent_async`, which takes the spin lock,
    /// tests `ESTATE::FINISHED` and either pushes onto `_edges` or counts the
    /// dependency as already met. The two branches have to be one atomic
    /// step, and the module docs say what a lost race would cost.
    // Nothing in the library names a dependency yet: #91's
    // `silent_dependent_async` is the call that will. `expect` rather than
    // `allow`, and `not(test)` rather than unconditional, so that the
    // attribute cannot outlive the gap -- the build fails the moment the item
    // is used, in the test build today and in #91's library code tomorrow.
    // The one on `finish` below and the one on `AsyncTask::new` are the same
    // statement about the same gap, which #88 retired for the two items it
    // supplied a caller for.
    #[cfg_attr(not(test), expect(dead_code, reason = "naming a dependency is #91"))]
    pub(crate) fn push_successor(&self, job: Job) -> bool {
        let mut successors = self.lock_successors();
        if self.is_finished() {
            return false;
        }
        successors.push(job);
        true
    }

    /// Marks this task finished and hands back the jobs that were waiting for
    /// it: upstream's `_tear_down_dependent_async`, minus the scheduling.
    ///
    /// The successors come *out* rather than being released here, so that
    /// nothing is scheduled with the lock held -- a job pushed to a worker's
    /// queue can be stolen and run before this call returns, and whatever it
    /// does must not be able to wait on this lock. Upstream releases them
    /// under its spin bit, which it can afford because its scheduling path is
    /// lock-free.
    ///
    /// The flag is written under the lock for the reason the module docs give
    /// -- it is the other half of what [`AsyncNode::push_successor`] decides
    /// -- and with release ordering, which is what makes the acquire load in
    /// [`AsyncNode::is_finished`] a real happens-before for a handle holder
    /// reading the body's results.
    // #91's rather than #88's, which is worth saying because the plain async
    // tear-down landed without calling this: upstream sets `ESTATE::FINISHED`
    // in `_tear_down_dependent_async` only, since a `Handle::Async` node has
    // no handle through which anybody could ask whether it is done.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the dependent-async tear-down is #91")
    )]
    pub(crate) fn finish(&self) -> Vec<Job> {
        let mut successors = self.lock_successors();
        self.finished.store(true, Ordering::Release);
        std::mem::take(&mut *successors)
    }

    /// The successor list. Poisoning is taken rather than propagated, as it
    /// is everywhere else in the executor: nothing under this lock does more
    /// than push or move a vector, so a poisoned list means a panic
    /// elsewhere, and refusing to hand it back would strand every task that
    /// is waiting for this one.
    fn lock_successors(&self) -> MutexGuard<'_, Vec<Job>> {
        self.successors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl core::fmt::Debug for AsyncNode {
    /// Hand-written because the derived one would print the successor list,
    /// which is a list of jobs each owning the graph it names; the node's own
    /// `Debug` is what identifies this one.
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

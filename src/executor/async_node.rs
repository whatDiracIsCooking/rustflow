//! The free-standing node an async task is: a node that belongs to no
//! taskflow, no graph the caller built and no run.
//!
//! Upstream has no such type, which is why this module names no source: its
//! async node is an ordinary `Node` out of the object pool with
//! `_topology == nullptr`, held alive by a reference count in the low 24 bits
//! of `_estate`. A `Node` of a taskflow here is a slot in a
//! [`Graph`](crate::graph::Graph)'s arena, reached by index; this type is
//! where a node that belongs to nobody lives instead -- one [`Node`] held
//! inline, and the `Arc` around it is the task's lifetime.
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
//!
//! A free node is **one heap block beyond its body**, the `Arc`, the node
//! sitting in it rather than in an arena of its own. The price of that is
//! paid by every caller that used to resolve this node by index into a
//! graph, [`AsyncNode::node`] being where the index goes instead;
//! `DESIGN.md` has the bill for both shapes.

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::executor::topology::Job;
use crate::graph::{Handle, Node, Task};

/// The one free-standing node an async or dependent-async task is, plus the
/// two pieces of state a node outside any run needs. Crate-private --
/// upstream's `friend class Executor` -- so a caller reaches one only through
/// [`AsyncTask`](crate::AsyncTask).
pub(crate) struct AsyncNode {
    /// The node itself, owned outright rather than held in an arena, and with
    /// no edges ever added to it: a dependent-async task's dependencies are
    /// the successor list below, not edges, a dependency of a free node being
    /// free itself.
    node: Node,
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
    /// The index every job of a free node carries, which resolves to
    /// [`AsyncNode::node`] rather than into any graph: a constant, so that a
    /// job of a free node has the same five fields as a job of a taskflow.
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

        Arc::new(Self {
            node: Node::new(name.into(), handle),
            finished: AtomicBool::new(false),
            successors: Mutex::new(Vec::new()),
        })
    }

    /// The node itself: what `GraphOwner::Async` resolves a job's
    /// [`AsyncNode::TASK`] to, and what a caller holding the `AsyncNode`
    /// rather than a job of it reads the handle's payload slot through.
    pub(crate) fn node(&self) -> &Node {
        &self.node
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
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    use super::*;
    use crate::executor::topology::{Parent, Topology};
    use crate::graph::AsyncWork;

    /// The system allocator plus a count of the allocations the calling
    /// thread asks it for, so that the blocks a free node costs can be
    /// counted instead of read off the code.
    ///
    /// Per thread rather than per process, because this is the allocator of
    /// the whole test binary and the harness runs the rest of its cases
    /// beside this one: a free node's blocks are allocated by the thread that
    /// submits the task, which is the thread running the case below.
    struct CountingAllocator;

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    thread_local! {
        /// How many allocations this thread has made since it started.
        /// Const-initialised and `Drop`-free on purpose: such a thread-local
        /// needs no lazy initialisation, so reading it inside the allocator
        /// cannot allocate.
        static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    }

    // SAFETY: every request is forwarded to `System` with the caller's own
    // layout and pointer and nothing else is done with either, so this
    // allocator is sound exactly as far as `System` is -- including the
    // `realloc` and `alloc_zeroed` this does not override, whose default
    // bodies forward to the two methods below. The count it adds is a
    // `Cell<usize>` in const-initialised thread-local storage, which
    // allocates nothing and so cannot re-enter.
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            ALLOCATIONS.with(|allocations| allocations.set(allocations.get() + 1));
            // SAFETY: `layout` is the caller's, forwarded unchanged.
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            // SAFETY: `ptr` and `layout` are the caller's, forwarded
            // unchanged, and every pointer a caller has came from the
            // `System` allocation above.
            unsafe { System.dealloc(ptr, layout) }
        }
    }

    /// How many heap allocations `body` made on this thread, and whatever it
    /// returned.
    fn allocations_of<T>(body: impl FnOnce() -> T) -> (usize, T) {
        let before = ALLOCATIONS.with(Cell::get);
        let value = body();
        (ALLOCATIONS.with(Cell::get) - before, value)
    }

    /// A dependent-async node, which is the kind with a successor list.
    fn node() -> Arc<AsyncNode> {
        AsyncNode::new("task", Handle::DependentAsync(AsyncWork::plain(|| {})))
    }

    /// The constructor's half of the module docs: one node carrying the
    /// handle it was given, with no edges of its own -- it indexes no arena,
    /// so it may have none -- and nothing finished or waiting yet.
    #[test]
    fn a_free_node_is_one_node_carrying_its_handle() {
        let free = node();

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

    /// The module docs' count, counted: a free async task is its body's box
    /// and the `Arc` the node now sits in, and nothing else. Two rather than
    /// three is the whole of this change -- the one-node `Graph` the node
    /// used to live in was the third -- and `DESIGN.md`'s entry on what a
    /// free node is has what a block costs.
    ///
    /// The queueing is outside the count on purpose, in the second half
    /// below: a job of a free node is clones of two shares, where the queue
    /// it goes into allocates per batch of pushes rather than per task.
    #[test]
    fn a_free_async_task_is_two_heap_blocks() {
        // Captured, so that boxing the body is an allocation at all: a
        // zero-sized closure's box is not one.
        let captured = 7_usize;

        // The sentinel run every free node's job shares is allocated once for
        // the process, by whichever job asks for it first; asking here keeps
        // that one-off out of the counts below.
        let _ = Topology::free_standing();

        let (blocks, free) = allocations_of(|| {
            AsyncNode::new(
                String::new(),
                Handle::Async(AsyncWork::plain(move || {
                    assert_eq!(captured, 7, "the body is one of the two blocks");
                })),
            )
        });
        assert_eq!(blocks, 2, "the `Arc` the node is in, and the body's box");

        let (queueing, job) = allocations_of(|| Job::free(&free, &Parent::Run));
        assert_eq!(
            queueing, 0,
            "and the job that queues it is two clones of two shares"
        );
        drop(job);
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

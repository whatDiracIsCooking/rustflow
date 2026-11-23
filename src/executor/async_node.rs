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
//!
//! That one block is what [`FreeNodes`] recycles: a worker that finished a
//! free node and holds the last share of it parks the allocation instead of
//! returning it to the allocator, and the next submission that worker makes
//! writes a new task into it. Only a worker some submission has already
//! drawn from parks anything, a list nothing draws from being one whose
//! blocks nobody can take out again. `DESIGN.md` has why the list is per
//! worker and unshared where upstream's `NodePool` is one sharded pool per
//! process, why the hand-back is a *check* here and an obligation there, and
//! what the draw condition was measured to be worth.

use std::any::Any;
use std::cell::{Cell, RefCell};
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
    /// neither async handle, for [`debug_assert_free_handle`]'s reason.
    ///
    /// Allocates: [`FreeNodes::animate`] is the same node out of a block the
    /// calling worker recycled, and the two are interchangeable.
    pub(crate) fn new(name: impl Into<String>, handle: Handle) -> Arc<Self> {
        debug_assert_free_handle(&handle);

        Arc::new(Self {
            node: Node::new(name.into(), handle),
            finished: AtomicBool::new(false),
            successors: Mutex::new(Vec::new()),
        })
    }

    /// Writes this node's whole state afresh, as the free node of the task
    /// `handle` is the body of: upstream's `ObjectPool::animate`, which
    /// constructs a `Node` into a block it recycled earlier. Everything the
    /// previous task left -- its body, its name, its finished flag and
    /// anything still on its successor list -- is dropped here, so a reused
    /// block carries nothing of it.
    ///
    /// Takes `&mut self`, which is the whole of what makes it sound to call
    /// on a shared node: only [`FreeNodes`] has one, and only through
    /// `Arc::get_mut`.
    fn reset(&mut self, name: String, handle: Handle) {
        self.node = Node::new(name, handle);
        *self.finished.get_mut() = false;
        self.successors
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
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

    /// Drops the body of a task that will never run, leaving the handle's
    /// slots as a run would have left them: [`AsyncWork::discard`], on
    /// whichever of the two free-node handles this node carries.
    ///
    /// What the cancellation early-out in `WorkerThread::invoke_one` calls
    /// **before** it counts the skipped job off. The body is the one thing a
    /// task owns that a caller can observe being dropped -- a `Runtime::scope`
    /// child's captures a borrow of the frame that opened the scope, and a
    /// guard in it may write through that borrow from its `Drop` -- and the
    /// node it sits in outlives the count: it is parked by
    /// [`FreeNodes::recycle`] or kept by whatever else still names it, and
    /// either way the body would be dropped after `Runtime::scope` had been
    /// told every child was joined. Issue #416 is where that order was found;
    /// `WorkerThread::spend_job`'s docs state the order every path keeps now.
    pub(crate) fn discard_body(&self) {
        match self.node().handle() {
            Handle::Async(work) | Handle::DependentAsync(work) => work.discard(),
            // Not a free node's handle: `debug_assert_free_handle` has why
            // only the two above are, and a parked node's `Placeholder` is
            // never dispatched.
            _ => {}
        }
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

/// Debug-asserts that `handle` is one a free-standing node may carry: a free
/// node with (say) a subflow handle is one whose arm preempts it with nothing
/// above it to resume it. Shared by the two ways such a node is built, the
/// allocation and the recycled block.
fn debug_assert_free_handle(handle: &Handle) {
    debug_assert!(
        handle.is_async() || handle.is_dependent_async(),
        "a free-standing node carries an async handle, not {handle:?}"
    );
}

/// The free [`AsyncNode`] blocks one worker has finished with and may write
/// its next submission into: upstream's `NodePool`, the sharded `ObjectPool`
/// every `Node` of a `TF_ENABLE_TASK_POOL` build is animated from and
/// recycled to, as one unshared stack per worker.
///
/// Reached only from the thread of the worker whose frame holds it -- the
/// submission that draws from it names that worker, and the tear-down that
/// parks a block runs on it -- which is what the [`RefCell`] rests on.
/// `DESIGN.md` has why the list is per worker rather than sharded, and why
/// nothing recycled here is pooled beyond the one block.
pub(crate) struct FreeNodes {
    /// The parked blocks, most recently freed first. Each is the only share
    /// of itself, which [`FreeNodes::recycle`] is what establishes, and each
    /// holds no task: a parked node was reset to an empty name and
    /// [`Handle::Placeholder`], so nothing a finished body captured stays
    /// alive by sitting here.
    parked: RefCell<Vec<Arc<AsyncNode>>>,
    /// Whether a submission has ever drawn from this list, which is the
    /// condition [`FreeNodes::recycle`] parks under: a list nothing draws
    /// from would park blocks no submission can reach again. Set by
    /// [`FreeNodes::animate`] whether or not the list had a block to give,
    /// that call being what proves a submission names this list at all.
    /// `DESIGN.md` has the measurement this condition came from.
    drawn_from: Cell<bool>,
}

impl FreeNodes {
    /// How many blocks one worker parks before it starts handing them back to
    /// the allocator. A bound on the memory a burst of async submissions can
    /// leave parked for the life of the executor, per worker; upstream's pool
    /// is unbounded until its `release`.
    const CAPACITY: usize = 32;

    /// An empty list. The backing `Vec` is left unallocated, a worker that
    /// finishes no free node -- and a corun view, which gets its own list for
    /// the frame it lives in -- paying nothing for having one.
    pub(crate) fn new() -> Self {
        Self {
            parked: RefCell::new(Vec::new()),
            drawn_from: Cell::new(false),
        }
    }

    /// The free-standing node of an async task whose body is `handle`, in a
    /// recycled block if this list has one: upstream's
    /// `ObjectPool::animate`, which is what its `animate(ArgsT&&...)` calls
    /// in a pooled build and `new Node` in every other.
    ///
    /// The node comes out indistinguishable from [`AsyncNode::new`]'s, which
    /// is [`AsyncNode::reset`]'s job, and the caller cannot tell which it
    /// got.
    pub(crate) fn animate(&self, name: String, handle: Handle) -> Arc<AsyncNode> {
        debug_assert_free_handle(&handle);

        self.drawn_from.set(true);
        let recycled = self.parked.borrow_mut().pop();
        let Some(mut node) = recycled else {
            return AsyncNode::new(name, handle);
        };

        Arc::get_mut(&mut node)
            .expect("a parked node is the only share of itself, and the list is this worker's")
            .reset(name, handle);
        node
    }

    /// Parks the block `node` sits in for this worker's next submission, or
    /// drops the share and lets the allocator have it: upstream's
    /// `ObjectPool::recycle`, called where it calls its own, as the node of a
    /// finished task is given up.
    ///
    /// Nothing is parked unless `node` is the **last** share of that node,
    /// which `Arc::get_mut` is what decides: an `AsyncTask` handle or the
    /// parent chain of something still queued may name the same node, and a
    /// block somebody can still reach is not this worker's to write a new
    /// task into. Upstream needs no such test, its own reference count
    /// having reached zero before `_tear_down_dependent_async` recycles.
    ///
    /// Nothing is parked either until [`FreeNodes::animate`] has drawn from
    /// this list once, a list no submission names being one whose blocks
    /// nothing can take out again.
    pub(crate) fn recycle(&self, mut node: Arc<AsyncNode>) {
        if !self.drawn_from.get() {
            return;
        }

        let mut parked = self.parked.borrow_mut();
        if parked.len() >= Self::CAPACITY {
            return;
        }

        let Some(unique) = Arc::get_mut(&mut node) else {
            return;
        };
        unique.reset(String::new(), Handle::Placeholder);
        parked.push(node);
    }

    /// How many blocks are parked.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.parked.borrow().len()
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
    use crate::Executor;
    use crate::executor::r#async::{Spawn, Submission};
    use crate::executor::queue::WorkerQueue;
    use crate::executor::topology::{Parent, Topology};
    use crate::executor::worker::WorkerThread;
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
    ///
    /// The dump is read at each of the three states this case already builds,
    /// because those states are exactly what it reports, and because its two
    /// derived fields are the reason it is hand-written: a scheduler log wants
    /// the node and *how many* jobs are waiting for it, where a derive would
    /// print every one of them.
    #[test]
    fn a_successor_named_before_the_finish_is_handed_back_by_it() {
        let dependency = node();
        let successor = Job::free(&node(), &Parent::Run);

        assert_eq!(
            format!("{dependency:?}"),
            format!(
                "AsyncNode {{ node: {:?}, finished: false, successors: 0 }}",
                dependency.node()
            ),
            "a fresh free node's dump names its node and waits for nothing"
        );

        assert!(
            dependency.push_successor(successor.clone()),
            "the dependency has not finished, so the job is kept"
        );
        let waiting = format!("{dependency:?}");
        assert!(
            waiting.ends_with("finished: false, successors: 1 }"),
            "a kept job is counted rather than expanded: {waiting}"
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
        let finished = format!("{dependency:?}");
        assert!(
            finished.ends_with("finished: true, successors: 0 }"),
            "and the dump follows both halves of the finish: {finished}"
        );
    }

    /// Only the two async handles may sit in a free-standing node, which is
    /// what both constructors debug-assert: a node carrying any other kind is
    /// one whose dispatch arm preempts it with nothing above it to resume it,
    /// and the message names the handle it was given.
    ///
    /// A debug build only, as the assertion is -- `cargo test` is one, and the
    /// release build this port ships deliberately pays nothing for the check.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "a free-standing node carries an async handle, not Placeholder")]
    fn a_free_node_refuses_a_handle_it_could_not_be_resumed_under() {
        let _refused = AsyncNode::new("not an async task", Handle::Placeholder);
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

    /// A silent body with a capture of its own, so that the box it goes in
    /// is an allocation and the two blocks a free task costs can be told
    /// apart: the node's, and this one's.
    fn silent_body() -> impl FnOnce() + Send + 'static {
        let captured = 7_usize;
        move || assert_eq!(captured, 7, "the body is its own block")
    }

    /// That body as the node's handle holds it.
    fn body() -> AsyncWork {
        AsyncWork::plain(silent_body())
    }

    /// The list's whole point, counted: a node recycled to it and then
    /// animated out of it again is the *same allocation*, and the second
    /// task costs one block where the first cost two.
    #[test]
    fn an_animated_node_reuses_the_block_a_recycled_one_parked() {
        let free = FreeNodes::new();

        let (first_blocks, first) =
            allocations_of(|| free.animate(String::new(), Handle::Async(body())));
        assert_eq!(
            first_blocks, 2,
            "an empty list allocates: the `Arc` and the body's box"
        );

        let address = Arc::as_ptr(&first);
        free.recycle(first);
        assert_eq!(free.len(), 1, "the last share of a finished node is parked");

        let (second_blocks, second) =
            allocations_of(|| free.animate(String::new(), Handle::Async(body())));
        assert_eq!(
            second_blocks, 1,
            "the body's box alone: the node's block was the parked one"
        );
        assert!(
            std::ptr::eq(address, Arc::as_ptr(&second)),
            "and it is that block"
        );
        assert_eq!(free.len(), 0, "which the list no longer holds");

        // The task written into it is a task like any other, carrying
        // nothing of the one before it.
        assert_eq!(second.node().name(), "");
        assert!(second.node().handle().is_async());
        assert!(!second.is_finished());
        assert!(second.take_panic().is_none());
    }

    /// The rule that makes recycling safe without `unsafe`: a node somebody
    /// else still names -- an `AsyncTask` handle, a job of it still queued --
    /// is not parked, so no second task is ever written over a node a caller
    /// can still read.
    #[test]
    fn a_node_somebody_still_names_is_not_parked() {
        let free = FreeNodes::new();
        let node = free.animate("task".to_string(), Handle::DependentAsync(body()));
        let handle = Arc::clone(&node);

        free.recycle(node);
        assert_eq!(free.len(), 0, "a shared node is dropped, not reused");
        assert_eq!(
            handle.node().name(),
            "task",
            "and the holder's node is untouched"
        );

        free.recycle(handle);
        assert_eq!(
            free.len(),
            1,
            "the last share of it is the one that parks it"
        );
    }

    /// The bound: a burst of finished tasks parks blocks up to the list's
    /// capacity and hands every one after that back to the allocator, so a
    /// worker cannot hold an unbounded number of nodes for the life of the
    /// executor.
    #[test]
    fn the_list_parks_no_more_than_its_capacity() {
        let free = FreeNodes::new();
        // The draw that opens the list, without which the loop below parks
        // nothing: the case the test after this one is about.
        drop(free.animate(String::new(), Handle::Async(body())));

        for _ in 0..FreeNodes::CAPACITY + 10 {
            free.recycle(AsyncNode::new(String::new(), Handle::Async(body())));
        }

        assert_eq!(free.len(), FreeNodes::CAPACITY);
    }

    /// The other bound, which #397 measured the want of: a list nothing has
    /// drawn from parks nothing, so a worker running children that were
    /// submitted through an `Executor` -- a route that names no worker and
    /// so never draws -- holds no block it cannot hand out again.
    #[test]
    fn a_list_nothing_has_drawn_from_parks_nothing() {
        let free = FreeNodes::new();

        free.recycle(AsyncNode::new(String::new(), Handle::Async(body())));
        assert_eq!(free.len(), 0, "nothing has drawn from this list");

        // One draw opens it, whether or not the list had a block to give:
        // this one had none, and the node below is parked all the same.
        drop(free.animate(String::new(), Handle::Async(body())));
        free.recycle(AsyncNode::new(String::new(), Handle::Async(body())));
        assert_eq!(free.len(), 1, "and a list a submission names parks one");
    }

    /// The submission half, through the path a task body takes: a free node
    /// finished by a worker is parked on *that worker's* list, and the next
    /// submission naming the same worker is written into the block it
    /// parked.
    ///
    /// The worker is driven on this thread, as the other cases that step the
    /// scheduler by hand are, which is also what puts the allocations on the
    /// thread the counter counts. One run, no timing: the claim is which
    /// block the second node is in, not what it saved.
    #[test]
    fn a_worker_submitted_node_is_recycled_for_the_next_submission() {
        let executor = Executor::new(1);
        let mut worker = WorkerThread::new(Arc::clone(&executor.shared), 0, WorkerQueue::new());
        // The job of the body doing the submitting, which is what
        // `Submission::under_node` counts the child against.
        let submitter = Job::free(&node(), &Parent::Run);

        let (first_blocks, first) = {
            let to = Submission::under_node(&submitter, &worker);
            let (blocks, ()) = allocations_of(|| {
                executor
                    .shared
                    .schedule_async_task(Spawn::silent(silent_body()), &to);
            });
            (
                blocks,
                worker
                    .pop()
                    .expect("the child went onto this worker's own queue"),
            )
        };
        assert_eq!(
            first_blocks, 2,
            "an empty list allocates both of a free task's blocks"
        );
        let address = Arc::as_ptr(first.async_node().expect("a free node's job"));

        worker.invoke(first);
        assert_eq!(
            worker.free_nodes().len(),
            1,
            "the worker that finished the task parked its node"
        );

        let (second_blocks, second) = {
            let to = Submission::under_node(&submitter, &worker);
            let (blocks, ()) = allocations_of(|| {
                executor
                    .shared
                    .schedule_async_task(Spawn::silent(silent_body()), &to);
            });
            (blocks, worker.pop().expect("and so did the next child"))
        };

        assert!(
            std::ptr::eq(
                address,
                Arc::as_ptr(second.async_node().expect("a free node's job"))
            ),
            "the second submission is in the block the first one's tear-down parked"
        );
        assert_eq!(
            second_blocks, 1,
            "so it allocated the body's box and nothing else"
        );
        assert_eq!(worker.free_nodes().len(), 0, "and the list is empty again");

        worker.invoke(second);
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

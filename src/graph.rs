//! Translated from `taskflow/core/graph.hpp` (Taskflow v4.1.0).
//!
//! The one structural departure from upstream is how a node is addressed. In
//! C++ a `tf::Task` wraps a raw `Node*` into a graph the caller keeps mutating,
//! which does not survive translation to Rust's aliasing rules; here [`Graph`]
//! owns its nodes by value in a `Vec` and a [`Task`] is an index into it.
//! Upstream's object pool (`TF_ENABLE_TASK_POOL`) is therefore not ported, and
//! neither is the `NodeBase`/`Node` split: the base class exists upstream so
//! that `Topology` can share the three state words, which nothing in this port
//! needs yet, so the words keep their upstream names in a single struct.
//!
//! That choice decides what erasing a node has to do. Upstream's `Graph::_erase`
//! compacts its `_nodes` vector, which it can afford because a `tf::Task` holds
//! a `Node*` that compaction does not move. An index handle is not so lucky: any
//! shuffle would silently re-point live [`Task`]s at other nodes. So
//! [`Graph::erase`] *tombstones* -- it empties the slot and leaves it empty
//! forever. Indices stay stable, which is the promise a `Copy` handle makes, at
//! the cost of a slot that is never reused: a graph that erases N nodes keeps N
//! empty slots until [`Graph::clear`]. A tombstoned handle then behaves exactly
//! like one that was never valid -- [`Graph::contains`] is `false`,
//! [`Graph::get`] is `None`, [`Graph::node`] panics -- and [`Graph::len`] counts
//! live nodes only, so the three iterators skip the dead.

use core::fmt;
use core::num::TryFromIntError;
use std::sync::atomic::{AtomicU32, AtomicUsize};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use crate::runtime::Runtime;
use crate::subflow::Subflow;
use crate::taskflow::Taskflow;

/// A handle to one node of a [`Graph`], as an index into that graph's arena.
///
/// A `Task` carries no reference to the graph it indexes, so it is `Copy` and
/// cheap to pass around, but it is only meaningful together with the `Graph`
/// that produced it -- using one against a different graph addresses an
/// unrelated node, exactly as a stale `Node*` would upstream.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Task(u32);

impl Task {
    /// Wraps a raw arena index.
    #[must_use]
    pub const fn new(index: u32) -> Self {
        Self(index)
    }

    /// Returns the raw arena index.
    #[must_use]
    pub const fn id(self) -> u32 {
        self.0
    }

    /// Returns the arena index as a `usize`, for indexing a node vector.
    ///
    /// # Panics
    ///
    /// Panics on a target whose `usize` is narrower than 32 bits, when the
    /// index does not fit. No such target can hold a graph that large anyway.
    #[must_use]
    pub fn index(self) -> usize {
        usize::try_from(self.0).expect("task index does not fit in a usize on this target")
    }
}

impl TryFrom<usize> for Task {
    type Error = TryFromIntError;

    fn try_from(index: usize) -> Result<Self, Self::Error> {
        Ok(Self(u32::try_from(index)?))
    }
}

impl From<Task> for u32 {
    fn from(task: Task) -> Self {
        task.0
    }
}

/// The callable behind a task, returning `R`.
///
/// Upstream stores a different `std::function` per node kind and the return
/// type is the only thing that differs between three of them -- `void()` for a
/// static task, `int()` for a condition, `SmallVector<int>()` for a
/// multi-condition -- so one type parameter covers all three. It defaults to
/// `()`, which keeps `Work` on its own meaning the static case.
///
/// Upstream calls the function through a `Node*`. A Rust closure needs `&mut`
/// to be called, so the `Mutex` is what lets a `&Node` shared across workers
/// invoke the work at all. It is an assertion rather than a synchronisation
/// primitive: the scheduler promises one worker per node, so [`Work::invoke`]
/// takes the lock with `try_lock` and panics instead of blocking if that
/// promise is ever broken. Whether to trade it for an `UnsafeCell` is for the
/// M2 benchmark to decide.
pub struct Work<R = ()>(Mutex<Box<dyn FnMut() -> R + Send + 'static>>);

impl<R> Work<R> {
    /// Wraps a callable as the work of a task.
    pub fn new<C>(callable: C) -> Self
    where
        C: FnMut() -> R + Send + 'static,
    {
        Self(Mutex::new(Box::new(callable)))
    }

    /// Runs the callable and hands back what it returned, which for a
    /// conditioner is the successors it selects.
    ///
    /// # Panics
    ///
    /// Panics if the work is already running on another thread, which means
    /// the scheduler's one-worker-per-node invariant was violated.
    pub fn invoke(&self) -> R {
        lock_callable(&self.0)()
    }
}

/// Takes the lock the callable of a work handle sits behind.
///
/// The three work handles -- [`Work`], [`SubflowWork`] and [`RuntimeWork`] --
/// differ only in what they call and share this discipline, so it is written
/// once here rather than three times.
///
/// # Panics
///
/// Panics if the work is already running on another thread, which means the
/// scheduler's one-worker-per-node invariant was violated.
fn lock_callable<F: ?Sized>(callable: &Mutex<Box<F>>) -> MutexGuard<'_, Box<F>> {
    match callable.try_lock() {
        Ok(callable) => callable,
        // A task that panics poisons its own work. Handing the closure back
        // out keeps the failure reported as that original panic rather than as
        // a lock error raised by whoever touches the node next; what to do
        // about the panic itself is the executor's call.
        Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
        Err(TryLockError::WouldBlock) => {
            panic!("work is already running: one worker per node was violated")
        }
    }
}

/// The callable behind a subflow task, which is handed the [`Subflow`] it
/// builds into.
///
/// Upstream's `std::function<void(tf::Subflow&)>`. The body builds a graph
/// while it runs, so it cannot be a [`Work`]: what it returns is nothing and
/// what it needs is the handle. The `Mutex` is [`Work`]'s, for [`Work`]'s
/// reason and under the same discipline -- a second worker on one node is a
/// reported panic, not a race.
///
/// The bound is higher-ranked over the subflow's own lifetime because the
/// handle borrows the node the body is running on, which is a lifetime no
/// stored closure can name.
pub struct SubflowWork(Mutex<SubflowBody>);

/// The boxed body of a subflow task. Named rather than written inline because
/// the lock, the box and the higher-ranked bound together are past what
/// clippy's `type_complexity` admits in a field.
type SubflowBody = Box<dyn for<'a> FnMut(&mut Subflow<'a>) + Send + 'static>;

impl SubflowWork {
    /// Wraps a callable as the work of a subflow task.
    pub fn new<C>(callable: C) -> Self
    where
        C: for<'a> FnMut(&mut Subflow<'a>) + Send + 'static,
    {
        Self(Mutex::new(Box::new(callable)))
    }

    /// Runs the callable against `subflow`, which is what the body builds its
    /// child graph into.
    ///
    /// # Panics
    ///
    /// Panics if the work is already running on another thread, which means
    /// the scheduler's one-worker-per-node invariant was violated.
    pub fn invoke(&self, subflow: &mut Subflow<'_>) {
        lock_callable(&self.0)(subflow);
    }
}

impl fmt::Debug for SubflowWork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SubflowWork(..)")
    }
}

/// The callable behind a runtime task, which is handed the [`Runtime`] of the
/// node it runs on.
///
/// Upstream's `std::function<void(tf::Runtime&)>`, under [`SubflowWork`]'s
/// discipline and for the same reasons. Upstream's second runtime shape,
/// `std::function<void(tf::NonpreemptiveRuntime&)>`, is excluded from this
/// milestone.
pub struct RuntimeWork(Mutex<RuntimeBody>);

/// The boxed body of a runtime task, named for [`SubflowBody`]'s reason.
type RuntimeBody = Box<dyn for<'a> FnMut(&mut Runtime<'a>) + Send + 'static>;

impl RuntimeWork {
    /// Wraps a callable as the work of a runtime task.
    pub fn new<C>(callable: C) -> Self
    where
        C: for<'a> FnMut(&mut Runtime<'a>) + Send + 'static,
    {
        Self(Mutex::new(Box::new(callable)))
    }

    /// Runs the callable against `runtime`, which is the body's handle on the
    /// scheduler.
    ///
    /// # Panics
    ///
    /// Panics if the work is already running on another thread, which means
    /// the scheduler's one-worker-per-node invariant was violated.
    pub fn invoke(&self, runtime: &mut Runtime<'_>) {
        lock_callable(&self.0)(runtime);
    }
}

impl fmt::Debug for RuntimeWork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RuntimeWork(..)")
    }
}

impl<R> fmt::Debug for Work<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Work(..)")
    }
}

/// What a node does when the executor reaches it.
///
/// Upstream is a `std::variant` of eleven alternatives (subflow, module,
/// async, ...). This port grows one variant per milestone, so the enum is
/// `#[non_exhaustive]`: adding the rest must not break callers outside the
/// crate.
#[derive(Debug, Default)]
#[non_exhaustive]
pub enum Handle {
    /// A task with no work attached yet, upstream's `std::monostate`.
    #[default]
    Placeholder,
    /// A plain callable, upstream's `Node::Static`.
    Static(Work),
    /// A callable whose return value picks which one of this node's successors
    /// runs next, upstream's `Node::Condition`.
    ///
    /// The index is an `i32` because upstream's is, and that is load-bearing
    /// rather than incidental: a negative or out-of-range value selects
    /// nothing, which is how a condition task ends a branch. Narrowing it to a
    /// successor position is the scheduler's job, through `usize::try_from`
    /// plus a range check and never an `as` cast.
    Condition(Work<i32>),
    /// A callable whose return value picks any number of this node's
    /// successors to run next, upstream's `Node::MultiCondition`.
    ///
    /// `Vec<i32>` stands in for upstream's inline `SmallVector<int>`: the
    /// allocation per invocation is a known cost, to revisit with a bench
    /// before hand-rolling a small-vector type for it.
    MultiCondition(Work<Vec<i32>>),
    /// A callable that builds a graph of its own while it runs, which the
    /// executor then runs before this node's successors: upstream's
    /// `Node::Subflow`.
    ///
    /// `subgraph` is upstream's `Graph subgraph` field, behind a `Mutex`
    /// because the body mutates it through a node the run only *borrows*: a
    /// worker reaches the node as `&Node`, so the graph it writes into has to
    /// carry its own interior mutability. It cannot deadlock a body either,
    /// since a nested subflow locks its own node's graph and never this one
    /// again. See [`crate::subflow`] for what holds the guard.
    Subflow {
        /// The body, which is handed the [`Subflow`] it builds into.
        work: SubflowWork,
        /// The graph the body builds, upstream's `Node::Subflow::subgraph`.
        subgraph: Mutex<Graph>,
    },
    /// A callable handed the scheduler itself, upstream's `Node::Runtime`.
    Runtime(RuntimeWork),
    /// A task that stands for another whole taskflow, upstream's
    /// `Node::Module`.
    ///
    /// Upstream stores a bare `Graph&` and leaves it to the caller to keep the
    /// target alive for as long as any graph composed of it may run; a safe
    /// Rust API cannot state that obligation, so the module *shares* ownership
    /// of its target instead. The run needs no more than that: the `Arc` hands
    /// out `&Graph` through [`Taskflow::graph`], and running the target writes
    /// its nodes only through their atomic state words.
    Module(Arc<Taskflow>),
    /// A task that stands for a graph it owns, upstream's
    /// `Node::AdoptedModule` -- the `adopt(Graph&&)` form of the variant
    /// above, which moves the graph in rather than sharing it.
    AdoptedModule(Graph),
}

impl Handle {
    /// Returns `true` for [`Handle::Placeholder`].
    #[must_use]
    pub fn is_placeholder(&self) -> bool {
        matches!(self, Self::Placeholder)
    }

    /// Returns `true` for [`Handle::Static`].
    #[must_use]
    pub fn is_static(&self) -> bool {
        matches!(self, Self::Static(_))
    }

    /// Returns `true` for [`Handle::Condition`].
    #[must_use]
    pub fn is_condition(&self) -> bool {
        matches!(self, Self::Condition(_))
    }

    /// Returns `true` for [`Handle::MultiCondition`].
    #[must_use]
    pub fn is_multi_condition(&self) -> bool {
        matches!(self, Self::MultiCondition(_))
    }

    /// Returns `true` for [`Handle::Subflow`].
    #[must_use]
    pub fn is_subflow(&self) -> bool {
        matches!(self, Self::Subflow { .. })
    }

    /// Returns `true` for [`Handle::Runtime`].
    #[must_use]
    pub fn is_runtime(&self) -> bool {
        matches!(self, Self::Runtime(_))
    }

    /// Returns `true` for either module handle, [`Handle::Module`] and
    /// [`Handle::AdoptedModule`].
    ///
    /// The two are one task kind as far as anything outside the executor is
    /// concerned -- upstream's `TaskType` has no separate name for the adopted
    /// form either -- and they differ only in who owns the target graph, which
    /// is a question only the arm that runs one asks. Match the handle to tell
    /// them apart.
    #[must_use]
    pub fn is_module(&self) -> bool {
        matches!(self, Self::Module(_) | Self::AdoptedModule(_))
    }

    /// Returns `true` if this handle picks its own successors at runtime,
    /// either kind of condition task. Upstream's `Node::_is_conditioner`.
    ///
    /// This is the predicate that makes a dependency *weak*: a task preceded
    /// by a conditioner may never run at all, so it cannot be waited for the
    /// way a strong dependency is.
    #[must_use]
    pub fn is_conditioner(&self) -> bool {
        self.is_condition() || self.is_multi_condition()
    }
}

/// A single task of a [`Graph`]: its name, its work, its edges and the state
/// words a run mutates.
///
/// The state words are atomic even where upstream's are plain (`_nstate` is a
/// bare `int` there) so that a run can hold a `&Graph` across worker threads
/// instead of handing out `&mut` to nodes it does not exclusively own.
#[derive(Debug)]
pub struct Node {
    name: String,
    handle: Handle,
    /// Successors occupy `edges[..num_successors]` and predecessors the rest,
    /// which is upstream's `_edges`/`_num_successors` layout exactly. One
    /// vector per node instead of two keeps a node's degree to a single
    /// allocation, and keeps the order of each half observable to tests.
    num_successors: usize,
    edges: Vec<Task>,
    nstate: AtomicU32,
    estate: AtomicU32,
    join_counter: AtomicUsize,
}

impl Node {
    /// Creates a node outside any graph. Private because the edge lists are
    /// arena indices, which are meaningless until [`Graph`] owns the node.
    fn new(name: String, handle: Handle) -> Self {
        Self {
            name,
            handle,
            num_successors: 0,
            edges: Vec::new(),
            nstate: AtomicU32::new(0),
            estate: AtomicU32::new(0),
            join_counter: AtomicUsize::new(0),
        }
    }

    /// Returns the name of this node, empty if it was never named.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Renames this node.
    pub fn set_name(&mut self, name: impl Into<String>) {
        self.name = name.into();
    }

    /// Returns the work this node performs.
    #[must_use]
    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    /// Returns the work this node performs, mutably.
    pub fn handle_mut(&mut self) -> &mut Handle {
        &mut self.handle
    }

    /// Replaces the work this node performs, dropping the previous handle.
    pub fn set_handle(&mut self, handle: Handle) {
        self.handle = handle;
    }

    /// Returns the number of outgoing edges, counting duplicates.
    #[must_use]
    pub fn num_successors(&self) -> usize {
        self.num_successors
    }

    /// Returns the number of incoming edges, counting duplicates.
    #[must_use]
    pub fn num_predecessors(&self) -> usize {
        self.edges.len() - self.num_successors
    }

    /// Returns the successor half of the edge list, in insertion order modulo
    /// the swap [`Graph::add_edge`] performs.
    #[must_use]
    pub fn successors(&self) -> &[Task] {
        &self.edges[..self.num_successors]
    }

    /// Returns the predecessor half of the edge list, in insertion order.
    #[must_use]
    pub fn predecessors(&self) -> &[Task] {
        &self.edges[self.num_successors..]
    }

    /// Upstream's `NSTATE::RETAIN_SUBFLOW`: set on a subflow node whose body
    /// asked, through [`Subflow::set_retain`], for its subgraph to survive
    /// the join instead of being cleared.
    pub const RETAIN_SUBFLOW: u32 = 0x4000_0000;

    /// Upstream's `NSTATE::JOINED_SUBFLOW`: set on a subflow node whose
    /// subgraph has been joined, which is what makes [`Subflow::joinable`]
    /// answer `false`.
    pub const JOINED_SUBFLOW: u32 = 0x8000_0000;

    /// Returns the scheduler state word, upstream's `_nstate`.
    ///
    /// The two constants above are the only bits of upstream's `NSTATE`
    /// vocabulary this port has; `IMPLICITLY_ANCHORED`, `PREEMPTED` and the
    /// `STRONG_DEPENDENCIES_MASK` arrive with the executor half of dynamic
    /// tasking. The mask is the one that matters for the pair above: upstream
    /// keeps the strong-dependency count in the low 28 bits and masks it out
    /// of the word on every read, where this port's run stores and re-arms
    /// from the whole 32 bits. Nothing can set these two bits on a node a run
    /// will reach yet -- only [`crate::subflow`] writes them, from a path the
    /// executor still refuses -- but the mask has to come back in that same
    /// change, or a node carrying one of them would re-arm its join counter
    /// with the bit's value.
    #[must_use]
    pub fn nstate(&self) -> &AtomicU32 {
        &self.nstate
    }

    /// Returns the exception state word, upstream's `_estate`.
    #[must_use]
    pub fn estate(&self) -> &AtomicU32 {
        &self.estate
    }

    /// Returns the join counter a run decrements to decide when this node is
    /// ready, upstream's `_join_counter`.
    #[must_use]
    pub fn join_counter(&self) -> &AtomicUsize {
        &self.join_counter
    }

    /// Appends `to` to the successor half, upstream's `Node::_precede` minus
    /// the predecessor push (the caller owns the other endpoint).
    ///
    /// Upstream's trick, kept verbatim: push to the back, then swap the new
    /// entry with the first predecessor so the halves stay contiguous.
    fn push_successor(&mut self, to: Task) {
        self.edges.push(to);
        let last = self.edges.len() - 1;
        self.edges.swap(self.num_successors, last);
        self.num_successors += 1;
    }

    /// Appends `from` to the predecessor half.
    fn push_predecessor(&mut self, from: Task) {
        self.edges.push(from);
    }

    /// Drops every occurrence of `target` from the successor half, returning
    /// how many went. Upstream's `Node::_remove_successors`.
    fn remove_successors(&mut self, target: Task) -> usize {
        let old_num_successors = self.num_successors;
        let mut kept = 0;
        for read in 0..old_num_successors {
            if self.edges[read] != target {
                self.edges[kept] = self.edges[read];
                kept += 1;
            }
        }
        // Close the gap the compaction left between the halves; the drain
        // shifts the predecessors down, as upstream's std::move does.
        self.edges.drain(kept..old_num_successors);
        self.num_successors = kept;
        old_num_successors - kept
    }

    /// Drops every occurrence of `target` from the predecessor half, returning
    /// how many went. Upstream's `Node::_remove_predecessors`.
    fn remove_predecessors(&mut self, target: Task) -> usize {
        let old_len = self.edges.len();
        let mut kept = self.num_successors;
        for read in self.num_successors..old_len {
            if self.edges[read] != target {
                self.edges[kept] = self.edges[read];
                kept += 1;
            }
        }
        self.edges.truncate(kept);
        old_len - kept
    }
}

/// The arena a task dependency graph lives in.
///
/// Nodes are owned by value and addressed by [`Task`]. Unlike upstream's
/// `tf::Graph` this type is movable *and* copy-free in the same sense -- there
/// are no owning pointers to fix up -- but it is deliberately not `Clone`,
/// since the work closures are not.
#[derive(Debug, Default)]
pub struct Graph {
    /// Slots in creation order. A `None` is a tombstone left by
    /// [`Graph::erase`]; it is never refilled, so a [`Task`] cannot come to
    /// address a different node than the one it was handed out for.
    nodes: Vec<Option<Node>>,
    /// How many slots hold a node, so [`Graph::len`] stays O(1).
    num_live: usize,
}

impl Graph {
    /// Creates an empty graph.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends an unnamed node and returns its handle.
    ///
    /// This is the low-level arena primitive; the fluent `emplace` family that
    /// upstream exposes through `tf::FlowBuilder` is built on top of it.
    ///
    /// # Panics
    ///
    /// Panics if the graph already holds `u32::MAX` nodes, the most a [`Task`]
    /// can address.
    pub fn add_node(&mut self, handle: Handle) -> Task {
        self.add_named_node(String::new(), handle)
    }

    /// Appends a named node and returns its handle.
    ///
    /// # Panics
    ///
    /// Panics if the graph already holds `u32::MAX` nodes.
    pub fn add_named_node(&mut self, name: impl Into<String>, handle: Handle) -> Task {
        let task = task_at(self.nodes.len());
        self.nodes.push(Some(Node::new(name.into(), handle)));
        self.num_live += 1;
        task
    }

    /// Returns `true` if `task` addresses a live node of this graph.
    ///
    /// An erased handle is indistinguishable here from one that was never
    /// valid: both are `false`.
    #[must_use]
    pub fn contains(&self, task: Task) -> bool {
        self.get(task).is_some()
    }

    /// Returns the node `task` addresses, or `None` if it addresses nothing
    /// live in this graph -- out of range, or erased.
    #[must_use]
    pub fn get(&self, task: Task) -> Option<&Node> {
        self.nodes.get(task.index()).and_then(Option::as_ref)
    }

    /// Returns the node `task` addresses mutably, or `None`.
    pub fn get_mut(&mut self, task: Task) -> Option<&mut Node> {
        self.nodes.get_mut(task.index()).and_then(Option::as_mut)
    }

    /// Returns the node `task` addresses.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a live node of this graph, whether
    /// because it is out of range or because it was erased.
    #[must_use]
    pub fn node(&self, task: Task) -> &Node {
        let slots = self.nodes.len();
        let live = self.num_live;
        self.get(task).unwrap_or_else(|| {
            panic!("{task:?} is out of range or erased: {live} live nodes in {slots} slots")
        })
    }

    /// Returns the node `task` addresses, mutably.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a live node of this graph, whether
    /// because it is out of range or because it was erased.
    pub fn node_mut(&mut self, task: Task) -> &mut Node {
        let slots = self.nodes.len();
        let live = self.num_live;
        self.get_mut(task).unwrap_or_else(|| {
            panic!("{task:?} is out of range or erased: {live} live nodes in {slots} slots")
        })
    }

    /// Returns the successors of `task`.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a node of this graph.
    #[must_use]
    pub fn successors(&self, task: Task) -> &[Task] {
        self.node(task).successors()
    }

    /// Returns the predecessors of `task`.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a node of this graph.
    #[must_use]
    pub fn predecessors(&self, task: Task) -> &[Task] {
        self.node(task).predecessors()
    }

    /// Adds a `from -> to` dependency, upstream's `Node::_precede`.
    ///
    /// Duplicate edges are kept, as upstream keeps them: a task preceded twice
    /// has a join counter of two.
    ///
    /// # Panics
    ///
    /// Panics if either handle is out of range for this graph.
    pub fn add_edge(&mut self, from: Task, to: Task) {
        // Check both ends before touching either, so a stale handle cannot
        // leave a one-sided edge behind.
        assert!(
            self.contains(from) && self.contains(to),
            "{from:?} -> {to:?} is out of range or erased: {} live nodes in {} slots",
            self.num_live,
            self.nodes.len()
        );
        self.node_mut(from).push_successor(to);
        self.node_mut(to).push_predecessor(from);
    }

    /// Removes every `from -> to` dependency and returns how many were
    /// removed. Upstream's `Taskflow::remove_dependency`.
    ///
    /// # Panics
    ///
    /// Panics if either handle is out of range for this graph.
    pub fn remove_edge(&mut self, from: Task, to: Task) -> usize {
        let removed_successors = self.node_mut(from).remove_successors(to);
        let removed_predecessors = self.node_mut(to).remove_predecessors(from);
        debug_assert_eq!(
            removed_successors, removed_predecessors,
            "edge lists disagree on the multiplicity of {from:?} -> {to:?}"
        );
        removed_successors
    }

    /// Removes the node `task` addresses together with every edge touching it,
    /// and returns `true` if there was one. Upstream's `Graph::_erase`, plus
    /// the neighbour patch-up that upstream leaves to `FlowBuilder::erase`.
    ///
    /// A handle that addresses nothing live -- out of range, or erased already
    /// -- is a no-op returning `false`, matching upstream's "if the task does
    /// not belong to the graph, nothing will happen".
    ///
    /// The slot becomes a tombstone and is never reused, so every other
    /// [`Task`] keeps addressing the node it always did. See the module docs
    /// for why that is worth a permanently empty slot.
    pub fn erase(&mut self, task: Task) -> bool {
        if !self.contains(task) {
            return false;
        }

        // Copy the endpoints out first: patching a neighbour borrows the graph
        // mutably, which the edge lists being read cannot survive. The erased
        // node's own lists need no patching -- they die with it.
        let successors = self.node(task).successors().to_vec();
        let predecessors = self.node(task).predecessors().to_vec();
        for successor in successors {
            self.node_mut(successor).remove_predecessors(task);
        }
        for predecessor in predecessors {
            self.node_mut(predecessor).remove_successors(task);
        }

        self.nodes[task.index()] = None;
        self.num_live -= 1;
        true
    }

    /// Returns the number of live nodes in the graph, not counting the
    /// tombstones [`Graph::erase`] leaves behind.
    ///
    /// This is the single definition of "how many tasks": [`crate::Taskflow`]
    /// defers to it.
    #[must_use]
    pub fn len(&self) -> usize {
        self.num_live
    }

    /// Returns `true` if the graph holds no live nodes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drops every node and every tombstone, invalidating every [`Task`]
    /// handed out so far. Handles start over from zero afterwards.
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.num_live = 0;
    }

    /// Iterates over the handles of every live node, in creation order.
    pub fn tasks(&self) -> impl Iterator<Item = Task> {
        self.iter().map(|(task, _)| task)
    }

    /// Iterates over every live node together with its handle, in creation
    /// order.
    pub fn iter(&self) -> impl Iterator<Item = (Task, &Node)> {
        self.nodes
            .iter()
            .enumerate()
            .filter_map(|(index, slot)| slot.as_ref().map(|node| (task_at(index), node)))
    }

    /// Iterates mutably over every live node together with its handle.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (Task, &mut Node)> {
        self.nodes
            .iter_mut()
            .enumerate()
            .filter_map(|(index, slot)| slot.as_mut().map(|node| (task_at(index), node)))
    }
}

/// Converts an arena index back into a handle.
///
/// [`Graph::add_named_node`] refuses to grow past `u32::MAX` nodes, so every
/// index that names a live node fits, and the conversion only fails on the
/// push that would have overflowed.
fn task_at(index: usize) -> Task {
    Task::try_from(index).expect("a graph cannot hold more than u32::MAX nodes")
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Task>();
    assert_send_sync::<Work>();
    assert_send_sync::<SubflowWork>();
    assert_send_sync::<RuntimeWork>();
    assert_send_sync::<Handle>();
    assert_send_sync::<Node>();
    assert_send_sync::<Graph>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    /// Builds `n` placeholder nodes named `a`, `b`, ... for edge tests.
    fn graph_of(n: usize) -> (Graph, Vec<Task>) {
        let mut graph = Graph::new();
        let tasks = (0..n)
            .map(|i| {
                let name = char::from(b'a' + u8::try_from(i).unwrap()).to_string();
                graph.add_named_node(name, Handle::Placeholder)
            })
            .collect();
        (graph, tasks)
    }

    #[test]
    fn task_index_round_trips() {
        let task = Task::new(7);
        assert_eq!(task.id(), 7);
        assert_eq!(task.index(), 7);
        assert_eq!(u32::from(task), 7);
        assert_eq!(Task::try_from(7usize), Ok(task));
        assert!(Task::try_from(usize::MAX).is_err());
    }

    #[test]
    fn add_node_hands_out_dense_handles() {
        let (graph, tasks) = graph_of(3);
        assert_eq!(tasks, vec![Task::new(0), Task::new(1), Task::new(2)]);
        assert_eq!(graph.len(), 3);
        assert_eq!(graph.node(tasks[1]).name(), "b");
        assert!(graph.contains(tasks[2]));
        assert!(!graph.contains(Task::new(3)));
        assert!(graph.get(Task::new(3)).is_none());
        assert_eq!(graph.tasks().collect::<Vec<_>>(), tasks);
        let names: Vec<&str> = graph.iter().map(|(_, node)| node.name()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn add_edge_splits_successors_from_predecessors() {
        let (mut graph, t) = graph_of(4);
        graph.add_edge(t[0], t[1]);
        graph.add_edge(t[0], t[2]);
        graph.add_edge(t[0], t[3]);

        assert_eq!(graph.node(t[0]).num_successors(), 3);
        assert_eq!(graph.node(t[0]).num_predecessors(), 0);
        assert_eq!(graph.successors(t[0]), [t[1], t[2], t[3]].as_slice());
        for successor in &t[1..] {
            assert_eq!(graph.node(*successor).num_predecessors(), 1);
            assert_eq!(graph.predecessors(*successor), [t[0]].as_slice());
            assert_eq!(graph.node(*successor).num_successors(), 0);
        }
    }

    #[test]
    fn edge_halves_stay_separate_when_a_node_has_both() {
        let (mut graph, t) = graph_of(4);
        // a -> b, b -> c, b -> d: b holds one predecessor and two successors.
        graph.add_edge(t[0], t[1]);
        graph.add_edge(t[1], t[2]);
        graph.add_edge(t[1], t[3]);

        assert_eq!(graph.successors(t[1]), [t[2], t[3]].as_slice());
        assert_eq!(graph.predecessors(t[1]), [t[0]].as_slice());

        // Removing a successor must shift the predecessor half down without
        // losing it -- the upstream std::move/resize path.
        assert_eq!(graph.remove_edge(t[1], t[2]), 1);
        assert_eq!(graph.successors(t[1]), [t[3]].as_slice());
        assert_eq!(graph.predecessors(t[1]), [t[0]].as_slice());
        assert_eq!(graph.node(t[2]).num_predecessors(), 0);

        // And removing the predecessor leaves the successor half alone.
        assert_eq!(graph.remove_edge(t[0], t[1]), 1);
        assert!(graph.predecessors(t[1]).is_empty());
        assert_eq!(graph.successors(t[1]), [t[3]].as_slice());
        assert_eq!(graph.node(t[0]).num_successors(), 0);
    }

    /// Mirrors upstream's `Taskflow.RemoveDependency` test case, which is the
    /// acceptance oracle for duplicate edges.
    #[test]
    fn remove_edge_drops_every_duplicate() {
        let (mut graph, t) = graph_of(4);
        for _ in 0..2 {
            graph.add_edge(t[0], t[1]);
            graph.add_edge(t[0], t[2]);
            graph.add_edge(t[0], t[3]);
        }
        assert_eq!(graph.node(t[0]).num_successors(), 6);
        assert_eq!(graph.node(t[1]).num_predecessors(), 2);

        assert_eq!(graph.remove_edge(t[0], t[1]), 2);
        assert_eq!(graph.node(t[0]).num_successors(), 4);
        assert_eq!(graph.node(t[1]).num_predecessors(), 0);

        assert_eq!(graph.remove_edge(t[0], t[2]), 2);
        assert_eq!(graph.node(t[0]).num_successors(), 2);
        assert_eq!(graph.node(t[2]).num_predecessors(), 0);

        assert_eq!(graph.remove_edge(t[0], t[3]), 2);
        assert_eq!(graph.node(t[0]).num_successors(), 0);
        assert_eq!(graph.node(t[3]).num_predecessors(), 0);

        // Removing an edge that was never there is a no-op, not a panic.
        assert_eq!(graph.remove_edge(t[0], t[1]), 0);
    }

    #[test]
    fn remove_edge_handles_a_self_loop() {
        let (mut graph, t) = graph_of(1);
        graph.add_edge(t[0], t[0]);
        assert_eq!(graph.node(t[0]).num_successors(), 1);
        assert_eq!(graph.node(t[0]).num_predecessors(), 1);
        assert_eq!(graph.remove_edge(t[0], t[0]), 1);
        assert_eq!(graph.node(t[0]).num_successors(), 0);
        assert_eq!(graph.node(t[0]).num_predecessors(), 0);
    }

    /// The bar issue #10 sets for erase: a middle node goes, its neighbours
    /// lose exactly the edges that touched it, and every other handle still
    /// resolves to the node it always named.
    #[test]
    fn erase_detaches_a_middle_node_and_leaves_the_rest_addressable() {
        let (mut graph, t) = graph_of(4);
        // a -> b -> c -> d, plus a -> c so c has two predecessors and the
        // erased node is not the only route into it.
        graph.add_edge(t[0], t[1]);
        graph.add_edge(t[1], t[2]);
        graph.add_edge(t[2], t[3]);
        graph.add_edge(t[0], t[2]);

        assert!(graph.erase(t[1]));

        // The neighbours lost b and nothing else.
        assert_eq!(graph.node(t[0]).num_successors(), 1);
        assert_eq!(graph.successors(t[0]), [t[2]].as_slice());
        assert_eq!(graph.node(t[2]).num_predecessors(), 1);
        assert_eq!(graph.predecessors(t[2]), [t[0]].as_slice());
        assert_eq!(graph.node(t[2]).num_successors(), 1);
        assert_eq!(graph.node(t[3]).num_predecessors(), 1);

        // Every other handle resolves to the same node as before.
        assert_eq!(graph.node(t[0]).name(), "a");
        assert_eq!(graph.node(t[2]).name(), "c");
        assert_eq!(graph.node(t[3]).name(), "d");

        // The erased handle is now indistinguishable from a stale one.
        assert!(!graph.contains(t[1]));
        assert!(graph.get(t[1]).is_none());
        assert!(graph.get_mut(t[1]).is_none());

        // `len` counts the live nodes, and the iterators skip the tombstone.
        assert_eq!(graph.len(), 3);
        assert_eq!(graph.tasks().collect::<Vec<_>>(), vec![t[0], t[2], t[3]]);
        let names: Vec<&str> = graph.iter().map(|(_, node)| node.name()).collect();
        assert_eq!(names, vec!["a", "c", "d"]);
        for (_, node) in graph.iter_mut() {
            node.set_name("live");
        }
        assert_eq!(graph.iter().count(), 3);

        // The slot is not reused: the next node gets a fresh index.
        assert_eq!(graph.add_node(Handle::Placeholder), Task::new(4));
        assert_eq!(graph.len(), 4);
    }

    #[test]
    fn erase_takes_duplicate_edges_and_a_self_loop_with_it() {
        let (mut graph, t) = graph_of(3);
        graph.add_edge(t[0], t[1]);
        graph.add_edge(t[0], t[1]);
        graph.add_edge(t[1], t[1]);
        graph.add_edge(t[1], t[2]);
        graph.add_edge(t[1], t[2]);

        assert!(graph.erase(t[1]));
        assert_eq!(graph.node(t[0]).num_successors(), 0);
        assert_eq!(graph.node(t[2]).num_predecessors(), 0);
    }

    #[test]
    fn erase_is_a_no_op_for_a_handle_that_names_nothing_live() {
        let (mut graph, t) = graph_of(1);
        assert!(!graph.erase(Task::new(1)));
        assert!(graph.erase(t[0]));
        // Erasing the same handle twice is the "task does not belong to the
        // graph" case upstream documents as doing nothing.
        assert!(!graph.erase(t[0]));
        assert_eq!(graph.len(), 0);
        assert!(graph.is_empty());
    }

    #[test]
    #[should_panic(expected = "out of range or erased")]
    fn add_edge_panics_on_an_erased_handle() {
        let (mut graph, t) = graph_of(2);
        graph.erase(t[0]);
        graph.add_edge(t[0], t[1]);
    }

    #[test]
    fn clear_drops_the_tombstones_too() {
        let (mut graph, t) = graph_of(2);
        graph.erase(t[0]);
        graph.clear();
        assert!(graph.is_empty());
        assert_eq!(graph.add_node(Handle::Placeholder), Task::new(0));
    }

    #[test]
    fn clear_empties_the_arena() {
        let (mut graph, t) = graph_of(2);
        graph.add_edge(t[0], t[1]);
        assert!(!graph.is_empty());
        graph.clear();
        assert!(graph.is_empty());
        assert_eq!(graph.len(), 0);
        assert_eq!(graph.tasks().count(), 0);
        // The arena is reusable afterwards, with handles starting over.
        assert_eq!(graph.add_node(Handle::Placeholder), Task::new(0));
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn node_panics_on_a_stale_handle() {
        let graph = Graph::new();
        let _ = graph.node(Task::new(0));
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn add_edge_panics_on_a_stale_handle() {
        let (mut graph, t) = graph_of(1);
        graph.add_edge(t[0], Task::new(1));
    }

    #[test]
    fn node_name_and_handle_are_mutable() {
        let mut graph = Graph::new();
        let task = graph.add_node(Handle::Placeholder);
        assert_eq!(graph.node(task).name(), "");
        assert!(graph.node(task).handle().is_placeholder());

        graph.node_mut(task).set_name("worker");
        let counter = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&counter);
        graph
            .node_mut(task)
            .set_handle(Handle::Static(Work::new(move || {
                seen.fetch_add(1, Ordering::Relaxed);
            })));

        assert_eq!(graph.node(task).name(), "worker");
        assert!(graph.node(task).handle().is_static());
        if let Handle::Static(work) = graph.node(task).handle() {
            work.invoke();
            work.invoke();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn state_words_are_writable_through_a_shared_node() {
        let (graph, t) = graph_of(1);
        let node = graph.node(t[0]);
        node.nstate().store(3, Ordering::Relaxed);
        node.estate().store(5, Ordering::Relaxed);
        node.join_counter().store(7, Ordering::Relaxed);
        assert_eq!(node.nstate().load(Ordering::Relaxed), 3);
        assert_eq!(node.estate().load(Ordering::Relaxed), 5);
        assert_eq!(node.join_counter().fetch_sub(1, Ordering::Relaxed), 7);
        assert_eq!(node.join_counter().load(Ordering::Relaxed), 6);
    }

    /// The reason the state words are atomic and `Work` holds a `Mutex`: a run
    /// gets `&Graph` and nothing more.
    #[test]
    fn a_shared_graph_can_be_run_from_several_threads() {
        let counter = Arc::new(AtomicUsize::new(0));
        let mut graph = Graph::new();
        for _ in 0..4 {
            let seen = Arc::clone(&counter);
            graph.add_node(Handle::Static(Work::new(move || {
                seen.fetch_add(1, Ordering::Relaxed);
            })));
        }

        let graph = &graph;
        std::thread::scope(|scope| {
            for task in graph.tasks() {
                scope.spawn(move || {
                    let node = graph.node(task);
                    if let Handle::Static(work) = node.handle() {
                        work.invoke();
                    }
                    node.join_counter().fetch_add(1, Ordering::Relaxed);
                });
            }
        });

        assert_eq!(counter.load(Ordering::Relaxed), 4);
        for (_, node) in graph.iter() {
            assert_eq!(node.join_counter().load(Ordering::Relaxed), 1);
        }
    }

    /// A conditioner's work is only useful if the value comes back out, so
    /// this is the bar issue #28 sets for making [`Work`] generic.
    #[test]
    fn invoke_returns_what_the_callable_returned() {
        let mut next = 0;
        let counting = Work::new(move || {
            next += 1;
            next - 1
        });
        assert_eq!(counting.invoke(), 0);
        assert_eq!(counting.invoke(), 1);

        // Upstream's `-10..=110` case is only expressible because the index is
        // signed and unbounded: out of range is the caller's prerogative, not
        // an error the handle rejects.
        let branch_nowhere = Work::new(|| -1);
        assert_eq!(branch_nowhere.invoke(), -1);

        let selection = Work::new(|| vec![-10, 0, 2, 110]);
        assert_eq!(selection.invoke(), vec![-10, 0, 2, 110]);
        // And a second invocation gets its own vector, not a drained one.
        assert_eq!(selection.invoke(), vec![-10, 0, 2, 110]);
    }

    #[test]
    fn the_handle_predicates_split_conditioners_from_the_rest() {
        let placeholder = Handle::Placeholder;
        let still = Handle::Static(Work::new(|| {}));
        let one = Handle::Condition(Work::new(|| 0));
        let many = Handle::MultiCondition(Work::new(Vec::new));

        assert!(placeholder.is_placeholder());
        assert!(still.is_static());
        assert!(one.is_condition());
        assert!(many.is_multi_condition());

        // `is_conditioner` is either of the two and nothing else, which is the
        // whole point: it is what the dependency counts key on.
        assert!(one.is_conditioner());
        assert!(many.is_conditioner());
        assert!(!placeholder.is_conditioner());
        assert!(!still.is_conditioner());

        assert!(!one.is_multi_condition());
        assert!(!many.is_condition());
        assert!(!one.is_static());
        assert!(!one.is_placeholder());
    }

    #[test]
    fn a_node_carries_either_conditioner_handle() {
        let mut graph = Graph::new();
        let one = graph.add_named_node("one", Handle::Condition(Work::new(|| 7)));
        let many = graph.add_named_node("many", Handle::MultiCondition(Work::new(|| vec![1, 3])));

        let Handle::Condition(work) = graph.node(one).handle() else {
            panic!("the condition handle did not survive add_named_node");
        };
        assert_eq!(work.invoke(), 7);
        let Handle::MultiCondition(work) = graph.node(many).handle() else {
            panic!("the multi-condition handle did not survive add_named_node");
        };
        assert_eq!(work.invoke(), vec![1, 3]);

        // And a conditioner replaces a static handle in place, which is what
        // `TaskMut::condition_work` is built on.
        graph
            .node_mut(one)
            .set_handle(Handle::Static(Work::new(|| {})));
        assert!(graph.node(one).handle().is_static());
        assert!(!graph.node(one).handle().is_conditioner());
    }

    /// The four dynamic-tasking handles answer their own predicate and
    /// nobody else's. Both module forms answer [`Handle::is_module`], which is
    /// the one predicate that deliberately covers two variants.
    #[test]
    fn the_handle_predicates_cover_the_dynamic_tasking_kinds() {
        let spawner = Handle::Subflow {
            work: SubflowWork::new(|_| {}),
            subgraph: Mutex::new(Graph::new()),
        };
        let runtime = Handle::Runtime(RuntimeWork::new(|_| {}));
        let module = Handle::Module(Arc::new(Taskflow::new()));
        let adopted = Handle::AdoptedModule(Graph::new());

        assert!(spawner.is_subflow());
        assert!(runtime.is_runtime());
        assert!(module.is_module());
        assert!(adopted.is_module());

        for handle in [&spawner, &runtime, &module, &adopted] {
            // A dynamic task is none of the static kinds, and above all it is
            // not a conditioner: a dependency on one of these is strong.
            assert!(!handle.is_placeholder());
            assert!(!handle.is_static());
            assert!(!handle.is_conditioner());
        }
        assert!(!spawner.is_runtime());
        assert!(!spawner.is_module());
        assert!(!runtime.is_subflow());
        assert!(!runtime.is_module());
        assert!(!module.is_subflow());
        assert!(!module.is_runtime());
        assert!(!Handle::Placeholder.is_subflow());
        assert!(!Handle::Static(Work::new(|| {})).is_module());
    }

    /// A node carries each of the four whole, which is what the builders and
    /// the executor arms rely on: the graph a subflow owns, the taskflow a
    /// module shares and the graph an adopted module took over all come back
    /// out of the node unchanged.
    #[test]
    fn a_node_carries_each_dynamic_tasking_handle() {
        let mut target = Taskflow::with_name("target");
        target.graph_mut().add_node(Handle::Placeholder);
        let target = Arc::new(target);

        let mut adopted = Graph::new();
        adopted.add_named_node("inner", Handle::Placeholder);

        let mut graph = Graph::new();
        let spawner = graph.add_named_node(
            "spawner",
            Handle::Subflow {
                work: SubflowWork::new(|_| {}),
                subgraph: Mutex::new(Graph::new()),
            },
        );
        let module = graph.add_named_node("module", Handle::Module(Arc::clone(&target)));
        let owned = graph.add_named_node("owned", Handle::AdoptedModule(adopted));

        let Handle::Subflow { subgraph, .. } = graph.node(spawner).handle() else {
            panic!("the subflow handle did not survive add_named_node");
        };
        // The subgraph is the node's own, reachable through a shared borrow of
        // the graph -- which is the whole reason it is behind a `Mutex`.
        subgraph
            .lock()
            .expect("uncontended")
            .add_node(Handle::Placeholder);
        assert_eq!(subgraph.lock().expect("uncontended").len(), 1);

        let Handle::Module(shared) = graph.node(module).handle() else {
            panic!("the module handle did not survive add_named_node");
        };
        assert!(Arc::ptr_eq(shared, &target));
        assert_eq!(shared.graph().len(), 1);
        assert_eq!(Arc::strong_count(&target), 2);

        let Handle::AdoptedModule(owned) = graph.node(owned).handle() else {
            panic!("the adopted module handle did not survive add_named_node");
        };
        assert_eq!(owned.node(Task::new(0)).name(), "inner");
    }

    /// The two `nstate` bits are upstream's, which means two distinct top
    /// bits: a value that is both of them is neither of anything else, and
    /// neither can be confused with the strong-dependency count the port
    /// keeps in the same word.
    #[test]
    fn the_subflow_state_bits_are_upstreams_two_top_bits() {
        assert_eq!(Node::RETAIN_SUBFLOW, 0x4000_0000);
        assert_eq!(Node::JOINED_SUBFLOW, 0x8000_0000);
        assert_eq!(Node::RETAIN_SUBFLOW & Node::JOINED_SUBFLOW, 0);
        assert_eq!(Node::RETAIN_SUBFLOW.count_ones(), 1);
        assert_eq!(Node::JOINED_SUBFLOW.count_ones(), 1);

        // And they are bits of a node's own word, writable through a shared
        // node like every other piece of run state.
        let (graph, t) = graph_of(1);
        let node = graph.node(t[0]);
        node.nstate()
            .fetch_or(Node::JOINED_SUBFLOW, Ordering::Relaxed);
        assert_eq!(
            node.nstate().load(Ordering::Relaxed) & Node::RETAIN_SUBFLOW,
            0
        );
        assert_ne!(
            node.nstate().load(Ordering::Relaxed) & Node::JOINED_SUBFLOW,
            0
        );
    }

    #[test]
    #[should_panic(expected = "one worker per node")]
    fn invoking_work_twice_at_once_panics() {
        let work = Work::new(|| {});
        // Re-entering from this thread is the cheap stand-in for two workers
        // racing: `try_lock` cannot tell the two apart.
        let _guard = work.0.try_lock().expect("uncontended");
        work.invoke();
    }
}

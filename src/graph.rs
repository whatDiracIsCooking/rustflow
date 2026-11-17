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

use core::fmt;
use core::num::TryFromIntError;
use std::sync::atomic::{AtomicU32, AtomicUsize};
use std::sync::{Mutex, TryLockError};

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

/// The callable behind a static task.
///
/// Upstream keeps a `std::function<void()>` in the node and calls it through a
/// `Node*`. A Rust closure needs `&mut` to be called, so the `Mutex` is what
/// lets a `&Node` shared across workers invoke the work at all. It is an
/// assertion rather than a synchronisation primitive: the scheduler promises
/// one worker per node, so [`Work::invoke`] takes the lock with `try_lock` and
/// panics instead of blocking if that promise is ever broken. Whether to trade
/// it for an `UnsafeCell` is for the M2 benchmark to decide.
pub struct Work(Mutex<Box<dyn FnMut() + Send + 'static>>);

impl Work {
    /// Wraps a callable as the work of a static task.
    pub fn new<C>(callable: C) -> Self
    where
        C: FnMut() + Send + 'static,
    {
        Self(Mutex::new(Box::new(callable)))
    }

    /// Runs the callable.
    ///
    /// # Panics
    ///
    /// Panics if the work is already running on another thread, which means
    /// the scheduler's one-worker-per-node invariant was violated.
    pub fn invoke(&self) {
        let mut callable = match self.0.try_lock() {
            Ok(callable) => callable,
            // A task that panics poisons its own work. Handing the closure
            // back out keeps the failure reported as that original panic
            // rather than as a lock error raised by whoever touches the node
            // next; what to do about the panic itself is the executor's call.
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => {
                panic!("work is already running: one worker per node was violated")
            }
        };
        callable();
    }
}

impl fmt::Debug for Work {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Work(..)")
    }
}

/// What a node does when the executor reaches it.
///
/// Upstream is a `std::variant` of eleven alternatives (subflow, condition,
/// module, async, ...). This port grows one variant per milestone, so the enum
/// is `#[non_exhaustive]`: adding the rest must not break callers outside the
/// crate.
#[derive(Debug, Default)]
#[non_exhaustive]
pub enum Handle {
    /// A task with no work attached yet, upstream's `std::monostate`.
    #[default]
    Placeholder,
    /// A plain callable, upstream's `Node::Static`.
    Static(Work),
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

    /// Returns the scheduler state word, upstream's `_nstate`.
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
    nodes: Vec<Node>,
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
        self.nodes.push(Node::new(name.into(), handle));
        task
    }

    /// Returns `true` if `task` addresses a node of this graph.
    #[must_use]
    pub fn contains(&self, task: Task) -> bool {
        task.index() < self.nodes.len()
    }

    /// Returns the node `task` addresses, or `None` if it addresses nothing in
    /// this graph.
    #[must_use]
    pub fn get(&self, task: Task) -> Option<&Node> {
        self.nodes.get(task.index())
    }

    /// Returns the node `task` addresses mutably, or `None`.
    pub fn get_mut(&mut self, task: Task) -> Option<&mut Node> {
        self.nodes.get_mut(task.index())
    }

    /// Returns the node `task` addresses.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a node of this graph.
    #[must_use]
    pub fn node(&self, task: Task) -> &Node {
        let len = self.nodes.len();
        self.get(task)
            .unwrap_or_else(|| panic!("{task:?} is out of range for a graph of {len} nodes"))
    }

    /// Returns the node `task` addresses, mutably.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a node of this graph.
    pub fn node_mut(&mut self, task: Task) -> &mut Node {
        let len = self.nodes.len();
        self.get_mut(task)
            .unwrap_or_else(|| panic!("{task:?} is out of range for a graph of {len} nodes"))
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
            "{from:?} -> {to:?} is out of range for a graph of {} nodes",
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

    /// Returns the number of nodes in the graph.
    ///
    /// This is the single definition of "how many tasks": [`crate::Taskflow`]
    /// defers to it, and so should erase-by-tombstone when it lands.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns `true` if the graph holds no nodes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drops every node, invalidating every [`Task`] handed out so far.
    pub fn clear(&mut self) {
        self.nodes.clear();
    }

    /// Iterates over the handles of every node, in creation order.
    pub fn tasks(&self) -> impl Iterator<Item = Task> {
        (0..self.nodes.len()).map(task_at)
    }

    /// Iterates over every node together with its handle, in creation order.
    pub fn iter(&self) -> impl Iterator<Item = (Task, &Node)> {
        self.nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (task_at(index), node))
    }

    /// Iterates mutably over every node together with its handle.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (Task, &mut Node)> {
        self.nodes
            .iter_mut()
            .enumerate()
            .map(|(index, node)| (task_at(index), node))
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

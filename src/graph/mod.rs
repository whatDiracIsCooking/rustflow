//! Translated from `taskflow/core/graph.hpp` (Taskflow v4.1.0).
//!
//! [`Graph`] owns its nodes by value and a [`Task`] is an index into that
//! arena. Upstream's `NodeBase`/`Node` split is kept: the base holds the
//! state words a run writes, so that a node whose nested graph is running and
//! the anchor a `corun` waits on are one kind of thing to the scheduler.
//!
//! # Design
//!
//! The arena and the index handle are the crate-wide decision `DESIGN.md`
//! records as "Graph as an arena", which is also why upstream's object pool
//! (`TF_ENABLE_TASK_POOL`) is not ported. Two members of upstream's
//! `NodeBase` are placed by decisions of their own, both in `DESIGN.md`:
//! `_parent` is not stored on a node but travels with the *job* that names
//! one, which is what keeps the scheduler free of raw pointers, and
//! `_exception_ptr` is on the node because an anchor is a `NodeBase`.
//!
//! [`Graph::erase`] **tombstones** a slot where upstream's `Graph::_erase`
//! compacts its `_nodes` vector. Upstream can compact because a `tf::Task`
//! holds a `Node*` that compaction does not move; a shuffle here would
//! silently re-point live [`Task`]s at other nodes. The price of an index
//! that stays stable is a slot that is never reused.

mod handle;
mod node;
mod work;

pub use handle::Handle;
pub use node::Node;
pub use work::{AsyncWork, NonpreemptiveRuntimeWork, OnceWork, RuntimeWork, SubflowWork, Work};

pub(crate) use node::NodeBase;

use core::num::TryFromIntError;

/// A handle to one node of a [`Graph`], as an index into that graph's arena.
///
/// A `Task` carries no reference to the graph it indexes, so it is `Copy`,
/// but it is only meaningful together with the `Graph` that produced it:
/// using one against a different graph addresses an unrelated node, exactly
/// as a stale `Node*` would upstream.
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

/// The arena a task dependency graph lives in.
///
/// Nodes are owned by value and addressed by [`Task`]. Moving one fixes up no
/// owning pointers, where upstream's `tf::Graph` has them, and it is
/// deliberately not `Clone`, the work closures not being.
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

    /// Creates an empty graph whose arena holds `capacity` nodes before it
    /// reallocates.
    ///
    /// Crate-private, because the only caller that can name its final size is
    /// a free async node, whose arena is one node for the life of the node:
    /// a graph a [`FlowBuilder`](crate::FlowBuilder) is filling cannot, and
    /// [`Graph::new`] leaves the `Vec` to pick its own first capacity.
    /// `DESIGN.md`'s entry on the `Arc`-owned free node says what sizing that
    /// one case saves.
    #[must_use]
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(capacity),
            num_live: 0,
        }
    }

    /// Appends an unnamed node and returns its handle: the arena primitive
    /// the `emplace` family of [`FlowBuilder`](crate::FlowBuilder) is built
    /// on.
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
    /// A handle that addresses nothing live -- out of range, or erased
    /// already -- is a no-op returning `false`, matching upstream's "if the
    /// task does not belong to the graph, nothing will happen". The slot
    /// becomes a tombstone and is never reused, so every other [`Task`] keeps
    /// addressing the node it always did; the module docs say why.
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
    /// tombstones [`Graph::erase`] leaves behind. This is the one definition
    /// of "how many tasks", which [`crate::Taskflow`] defers to.
    #[must_use]
    pub fn len(&self) -> usize {
        self.num_live
    }

    /// Returns `true` if the graph holds no live nodes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many slots the arena holds before it reallocates, which is not
    /// how many nodes are in it. Test-only: the one thing a caller of
    /// [`Graph::with_capacity`] can assert about the sizing it asked for.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn capacity(&self) -> usize {
        self.nodes.capacity()
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

/// Converts an arena index back into a handle. [`Graph::add_named_node`]
/// refuses to grow past `u32::MAX` nodes, so the conversion can only fail on
/// the push that would have overflowed.
fn task_at(index: usize) -> Task {
    Task::try_from(index).expect("a graph cannot hold more than u32::MAX nodes")
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Task>();
    assert_send_sync::<Work>();
    assert_send_sync::<SubflowWork>();
    assert_send_sync::<RuntimeWork>();
    assert_send_sync::<NonpreemptiveRuntimeWork>();
    assert_send_sync::<Handle>();
    assert_send_sync::<NodeBase>();
    assert_send_sync::<Node>();
    assert_send_sync::<Graph>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use crate::executor::topology::{Cancellable, GraphOwner, Job, Parent, Topology};
    use crate::semaphore::Semaphore;
    use crate::taskflow::Taskflow;

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

    /// The `nstate` bits are upstream's, which means distinct top bits: a
    /// value that is one of them is none of the others, and none of them can
    /// be confused with the strong-dependency count the port keeps in the
    /// same word.
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

    /// `PREEMPTED` and the mask are the other half of upstream's `NSTATE`,
    /// and the one property that matters is that the state bits and the
    /// count cannot reach into each other: the count is masked out of the
    /// word on every read, so a bit set on a node must not be able to look
    /// like 536,870,912 dependencies, and a count must not be able to look
    /// like a state bit.
    #[test]
    fn the_state_bits_and_the_dependency_count_do_not_overlap() {
        assert_eq!(Node::PREEMPTED, 0x2000_0000);
        assert_eq!(Node::STRONG_DEPENDENCIES_MASK, 0x0FFF_FFFF);
        assert_eq!(Node::PREEMPTED.count_ones(), 1);

        // All four state bits, which is the whole of upstream's vocabulary:
        // disjoint from each other and from the count field.
        assert_eq!(Node::IMPLICITLY_ANCHORED, 0x1000_0000);
        assert_eq!(Node::IMPLICITLY_ANCHORED.count_ones(), 1);
        let state = Node::PREEMPTED
            | Node::RETAIN_SUBFLOW
            | Node::JOINED_SUBFLOW
            | Node::IMPLICITLY_ANCHORED;
        assert_eq!(state & Node::STRONG_DEPENDENCIES_MASK, 0);
        assert_eq!(state.count_ones(), 4);

        // The mask is contiguous from bit zero, so masking a word is the
        // count and nothing else, and it stops just below the lowest state
        // bit -- `IMPLICITLY_ANCHORED`, which is why the count field stops
        // one bit short of `PREEMPTED` rather than at it.
        assert_eq!(Node::STRONG_DEPENDENCIES_MASK.count_ones(), 28);
        assert_eq!(Node::STRONG_DEPENDENCIES_MASK.trailing_ones(), 28);
        const { assert!(Node::STRONG_DEPENDENCIES_MASK < Node::IMPLICITLY_ANCHORED) };
        const { assert!(Node::IMPLICITLY_ANCHORED < Node::PREEMPTED) };
        let (graph, t) = graph_of(1);
        let node = graph.node(t[0]);
        node.nstate()
            .store(Node::STRONG_DEPENDENCIES_MASK | state, Ordering::Relaxed);
        assert_eq!(
            node.nstate().load(Ordering::Relaxed) & Node::STRONG_DEPENDENCIES_MASK,
            Node::STRONG_DEPENDENCIES_MASK
        );
    }

    /// A node's state words are its [`NodeBase`], which is the type the
    /// scheduler holds when it does not care whether what it is counting
    /// down is a node or an anchor.
    #[test]
    fn a_nodes_state_words_are_its_base() {
        let (graph, t) = graph_of(1);
        let node = graph.node(t[0]);
        let base = node.base();

        base.nstate().store(2, Ordering::Relaxed);
        base.estate().store(4, Ordering::Relaxed);
        base.join_counter().store(6, Ordering::Relaxed);
        assert_eq!(node.nstate().load(Ordering::Relaxed), 2);
        assert_eq!(node.estate().load(Ordering::Relaxed), 4);
        assert_eq!(node.join_counter().load(Ordering::Relaxed), 6);

        // And a base of its own starts clear, which is what an anchor needs:
        // nothing outstanding until a graph is scheduled under it.
        let anchor = NodeBase::new();
        assert_eq!(anchor.nstate().load(Ordering::Relaxed), 0);
        assert_eq!(anchor.estate().load(Ordering::Relaxed), 0);
        assert_eq!(anchor.join_counter().load(Ordering::Relaxed), 0);
    }

    /// The explicit-anchor bit is upstream's `ExplicitAnchorGuard`, so what
    /// has to hold is that it is set for exactly as long as the guard lives
    /// -- an unwind through the waiting frame included -- and that it does
    /// not disturb the other estate bits.
    #[test]
    fn an_explicit_anchor_is_set_while_its_guard_lives() {
        let anchor = NodeBase::new();
        assert!(!anchor.is_explicitly_anchored());

        anchor
            .estate()
            .store(Cancellable::CANCELLED, Ordering::Relaxed);
        {
            let _guard = anchor.anchor_explicitly();
            assert!(anchor.is_explicitly_anchored());
            // Set beside whatever the word already held rather than over it,
            // which is why both the guard's halves are read-modify-write.
            assert_eq!(
                anchor.estate().load(Ordering::Relaxed),
                Cancellable::CANCELLED | Cancellable::EXPLICITLY_ANCHORED
            );
        }

        assert!(!anchor.is_explicitly_anchored());
        assert_eq!(
            anchor.estate().load(Ordering::Relaxed),
            Cancellable::CANCELLED,
            "the guard cleared a bit that was not its own"
        );

        // And on an unwind, which is the case the guard exists for: a frame
        // waiting for a nested graph must not leave the anchor set behind.
        let anchor = NodeBase::new();
        let unwound = std::panic::catch_unwind(|| {
            let _guard = anchor.anchor_explicitly();
            panic!("the waiting frame failed");
        });
        assert!(unwound.is_err());
        assert!(!anchor.is_explicitly_anchored());
    }

    /// The payload slot, which is upstream's `_exception_ptr` plus the
    /// `CAUGHT` arbiter that says who may write it: the first claimant keeps
    /// its payload, a later one is turned away, and taking the payload leaves
    /// the base ready to anchor another graph.
    #[test]
    fn the_payload_slot_keeps_the_first_claimant_and_is_taken_once() {
        let base = NodeBase::new();
        assert!(base.take_panic().is_none());

        // The walk marks every base it passes with `EXCEPTION`, so a base can
        // arrive at the claim already carrying it and must still be owed the
        // payload: the test is `CAUGHT` alone.
        base.estate()
            .fetch_or(Cancellable::EXCEPTION, Ordering::Relaxed);
        assert!(base.claim_panic(), "the first claimant");
        base.keep_panic(Box::new("first"));
        assert!(!base.claim_panic(), "the second claimant");

        assert_eq!(
            base.estate().load(Ordering::Relaxed),
            Cancellable::EXCEPTION | Cancellable::CAUGHT
        );

        let payload = base.take_panic().expect("a payload was kept");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"first"));
        // Taken, not cloned, and the base is no longer a failed one: both
        // bits are gone, so a graph scheduled under it next is not born
        // cancelled.
        assert!(base.take_panic().is_none());
        assert_eq!(base.estate().load(Ordering::Relaxed), 0);

        // Clearing is what a set-up does to a node: whatever was there goes,
        // and a slot that was empty stays empty.
        assert!(base.claim_panic());
        base.keep_panic(Box::new("dropped by the next set-up"));
        base.clear_panic();
        assert!(base.take_panic().is_none());
        base.clear_panic();
    }

    #[test]
    #[should_panic(expected = "locked twice at once")]
    fn invoking_work_twice_at_once_panics() {
        let work = Work::new(|| {});
        // Re-entering from this thread is the cheap stand-in for two workers
        // racing: `try_lock` cannot tell the two apart.
        let _guard = work.0.try_lock().expect("uncontended");
        work.invoke();
    }

    /// The job of the one source task of `taskflow`, which is how a case here
    /// gets hold of something a semaphore can park.
    ///
    /// The taskflow must be finished with by the time this is called:
    /// `Taskflow::graph_mut` refuses once a share of the storage is
    /// outstanding, and the job holds one.
    fn source_job(taskflow: &Taskflow) -> Job {
        let flow = Arc::clone(taskflow.flow());
        let topology = Arc::new(Topology::new(|| true, || {}, Arc::clone(&flow)));
        let owner = GraphOwner::flow(&flow);
        let mut sources = Job::sources(&owner, &topology, &Parent::Run);
        let job = sources.next().expect("the graph has a source task");
        assert!(sources.next().is_none(), "the cases here have one source");
        job
    }

    /// A node that acquires nothing may run, and asks no semaphore anything:
    /// every node of every other case in this crate is this one.
    #[test]
    fn acquire_all_of_a_node_with_no_semaphores_succeeds() {
        let mut taskflow = Taskflow::new();
        taskflow
            .graph_mut()
            .add_named_node("a", Handle::Placeholder);
        let job = source_job(&taskflow);

        let mut woken = Vec::new();
        assert!(job.node().acquire_all(&job, &mut woken));
        assert!(job.node().release_all(&mut woken).is_ok());
        assert!(woken.is_empty());
    }

    /// Upstream's `_acquire_all` loop exactly: the first semaphore that cannot
    /// be had parks the job, and the ones already taken are given back rather
    /// than held by a task that is not going to run.
    #[test]
    fn acquire_all_rolls_back_what_it_took_before_it_parked() {
        let free = Arc::new(Semaphore::new(1));
        let exhausted = Arc::new(Semaphore::new(0));

        let mut taskflow = Taskflow::new();
        let a = taskflow
            .graph_mut()
            .add_named_node("a", Handle::Placeholder);
        let semaphores = taskflow.graph_mut().node_mut(a).semaphores_mut();
        semaphores.to_acquire.push(Arc::clone(&free));
        semaphores.to_acquire.push(Arc::clone(&exhausted));
        let job = source_job(&taskflow);

        let mut woken = Vec::new();
        assert!(!job.node().acquire_all(&job, &mut woken));

        // Given back, and parked on the one that could not be had.
        assert_eq!(free.value(), 1);
        assert_eq!(free.num_waiters(), 0);
        assert_eq!(exhausted.num_waiters(), 1);
        // The rollback woke nothing, because nothing was waiting on `free`.
        assert!(woken.is_empty());
    }

    /// The whole of what the executor will do with a node of a semaphore of
    /// one: acquire, run, release -- and the release hands back whoever was
    /// parked in the meantime.
    #[test]
    fn release_all_gives_back_every_semaphore_and_collects_the_woken() {
        let semaphore = Arc::new(Semaphore::new(1));

        let mut taskflow = Taskflow::new();
        let a = taskflow
            .graph_mut()
            .add_named_node("a", Handle::Placeholder);
        let semaphores = taskflow.graph_mut().node_mut(a).semaphores_mut();
        semaphores.to_acquire.push(Arc::clone(&semaphore));
        semaphores.to_release.push(Arc::clone(&semaphore));
        let job = source_job(&taskflow);

        let mut woken = Vec::new();
        assert!(job.node().acquire_all(&job, &mut woken));
        assert_eq!(semaphore.value(), 0);

        // A second task of the same set, parked while the first holds the one
        // count there is.
        assert!(!semaphore.try_acquire_or_wait(&job));

        job.node()
            .release_all(&mut woken)
            .expect("the node holds the count it is giving back");
        assert_eq!(semaphore.value(), 1);
        assert_eq!(woken.len(), 1, "the parked job came back to be scheduled");
    }

    /// A node may release a semaphore it never acquired -- upstream's way of
    /// opening a gate for another task -- and that is the one way a release
    /// can fail.
    #[test]
    fn release_all_reports_an_over_release() {
        let semaphore = Arc::new(Semaphore::new(1));

        let mut taskflow = Taskflow::new();
        let a = taskflow
            .graph_mut()
            .add_named_node("a", Handle::Placeholder);
        taskflow
            .graph_mut()
            .node_mut(a)
            .semaphores_mut()
            .to_release
            .push(Arc::clone(&semaphore));
        let job = source_job(&taskflow);

        let error = job
            .node()
            .release_all(&mut Vec::new())
            .expect_err("the semaphore is already at its maximum");
        assert_eq!(error.max_value(), 1);
    }

    /// The two async handles answer their own predicate and nobody else's.
    /// They do *not* fold together the way the two module forms do, and
    /// [`Handle::is_dependent_async`] says why.
    #[test]
    fn the_handle_predicates_split_the_two_async_kinds() {
        let free = Handle::Async(AsyncWork::plain(|| {}));
        let dependent = Handle::DependentAsync(AsyncWork::plain(|| {}));

        assert!(free.is_async());
        assert!(dependent.is_dependent_async());
        assert!(!free.is_dependent_async());
        assert!(!dependent.is_async());

        for handle in [&free, &dependent] {
            assert!(!handle.is_placeholder());
            assert!(!handle.is_static());
            assert!(!handle.is_module());
            assert!(!handle.is_subflow());
            assert!(!handle.is_runtime());
            // Above all not a conditioner: an async node picks no successors.
            assert!(!handle.is_conditioner());
        }
        assert!(!Handle::Placeholder.is_async());
        assert!(!Handle::Static(Work::new(|| {})).is_dependent_async());
    }

    /// An async body runs **once**, which is the whole of what separates
    /// [`AsyncWork`] from [`Work`]: the one worker that gets there takes the
    /// body out of the slot, and every caller after it finds nothing.
    #[test]
    fn an_async_body_is_taken_out_of_its_slot_once() {
        let AsyncWork::Plain(slot) = AsyncWork::plain(|| {}) else {
            panic!("`plain` builds the plain shape");
        };

        assert!(!slot.is_spent());
        let body = slot.take().expect("the first taker gets the body");
        assert!(slot.is_spent(), "and the slot is empty behind it");
        assert!(slot.take().is_none(), "so a second worker runs nothing");

        // The body is a `FnOnce`, so running it is what consumes it: there is
        // no second call to be had even for the taker.
        body();
    }

    /// The reentrant shape's two halves are taken independently, which is the
    /// port's one departure from upstream's single `void(Runtime&, bool)`
    /// lambda: the first entry takes the body and the second, which may be on
    /// another worker, takes what delivers the node's payload.
    #[test]
    fn a_reentrant_body_and_its_completion_are_two_separate_onces() {
        let delivered = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&delivered);
        let work =
            AsyncWork::reentrant(|_| {}, move |payload| *slot.lock().expect("free") = payload);

        let AsyncWork::Reentrant { body, complete } = work else {
            panic!("`reentrant` builds the reentrant shape");
        };

        // The first entry spends the body and leaves the completion alone.
        assert!(body.take().is_some());
        assert!(body.is_spent());
        assert!(!complete.is_spent(), "the second entry still has its half");

        // The second entry hands over whatever the node's slot held, which is
        // upstream's `eptr ? set_exception : set_value`.
        complete.take().expect("the completion")(Some(Box::new("oops")));
        assert!(complete.is_spent());
        let payload = delivered.lock().expect("free").take().expect("delivered");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"oops"));
    }
}

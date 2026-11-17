//! Translated from `taskflow/core/task.hpp` (Taskflow v4.1.0).
//!
//! Upstream's `tf::Task` is a copyable wrapper around a `Node*` that keeps
//! aliasing a graph the caller goes on mutating, which is exactly the shape
//! Rust's borrow rules exist to forbid. The handle is therefore split in two
//! short-lived borrows of the owning [`Taskflow`]:
//!
//! - [`TaskMut`], from [`Taskflow::task`], is the editing half -- every method
//!   returns `&mut Self` so that `tf.task(a).name("a").precede([b, c])` reads
//!   the way upstream's chain does.
//! - [`TaskRef`], from [`Taskflow::task_ref`], is the query half. It doubles
//!   as the port of upstream's `tf::TaskView`, which differs from `tf::Task`
//!   only in leaving the editing methods out.
//!
//! The lasting handle is the [`Task`] index from [`crate::graph`]; these two
//! borrow types are the fluent sugar over it and are not meant to be stored.
//! That split is also why upstream's `name()` overload pair cannot survive:
//! one type cannot have `name` as both getter and setter, so [`TaskMut::name`]
//! is the setter and the getter is [`TaskRef::name`].
//!
//! # Departures from upstream
//!
//! - `data()`/`data(void*)` are **not** ported. A raw `void*` side channel on a
//!   node has no Rust shape, and it exists upstream only so that a lambda can
//!   reach mutable state it does not own; here the closure captures that state.
//! - `reset()` means something different. Upstream's nulls the aliasing
//!   pointer -- `_node = nullptr` -- which an index handle has no analogue for,
//!   so what is ported is the node-level reset upstream performs on reuse:
//!   drop the work and drop the edges. Upstream's own `Node::reset` does that
//!   second half with a bare `_edges.clear()`, which is one-sided and leaves
//!   every neighbour still listing this node; that is safe there only because
//!   reset is reached from the object pool, where the neighbours are being
//!   recycled too. [`TaskMut::reset`] therefore goes through
//!   [`Graph::remove_edge`] in both directions instead, so a reset in the
//!   middle of a live graph cannot leave a half-edge behind.
//! - `empty()` follows from that: with no null handle to ask about, the ported
//!   [`TaskRef::is_empty`] asks whether the node is indistinguishable from a
//!   freshly created placeholder -- no work, no edges -- which keeps upstream's
//!   documented pair `task.reset(); assert(task.empty())` true.
//! - Semaphores, `exception_ptr()`, `dump()` and `for_each_subflow_task()`
//!   wait for the milestones that give them meaning, and so does the
//!   `composed_of()`/`adopt()` pair that upstream has on `tf::Task` as well as
//!   on its builder: the builder forms are
//!   [`FlowBuilder::composed_of`](crate::FlowBuilder::composed_of) and
//!   [`FlowBuilder::adopt`](crate::FlowBuilder::adopt), and turning an
//!   existing task into a module has no caller in this port yet.
//! - Upstream dispatches one `work()` over five callable shapes, and with no
//!   overloading here each shape is its own method: [`TaskMut::work`],
//!   [`TaskMut::condition_work`], [`TaskMut::multi_condition_work`],
//!   [`TaskMut::subflow_work`] and [`TaskMut::runtime_work`] are all five.

use core::fmt;
use core::hash::{Hash, Hasher};
use std::hash::DefaultHasher;

use std::sync::Mutex;

use crate::graph::{Graph, Handle, RuntimeWork, SubflowWork, Task, Work};
use crate::runtime::Runtime;
use crate::subflow::Subflow;
use crate::taskflow::Taskflow;

/// What a task does when the executor reaches it, as a flat enumeration.
///
/// This is upstream's `tf::TaskType`, kept complete even though
/// [`TaskType::Async`] cannot be built yet: the variants name the port's own
/// roadmap, and the DOT output and the profiler want the whole set. Upstream's
/// `UNDEFINED` is kept for the same reason, though nothing can produce it
/// while every [`Handle`] variant maps to a type of its own.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum TaskType {
    /// A task with no work attached, upstream's `PLACEHOLDER`.
    Placeholder,
    /// A plain callable, upstream's `STATIC`.
    Static,
    /// A callable taking the runtime handle, upstream's `RUNTIME`.
    Runtime,
    /// A callable that spawns a subflow, upstream's `SUBFLOW`.
    Subflow,
    /// A callable whose return value picks the successors to run, upstream's
    /// `CONDITION`. Upstream folds `MULTI_CONDITION` in here too.
    Condition,
    /// A task that runs another graph, upstream's `MODULE`.
    Module,
    /// A task submitted outside any graph, upstream's `ASYNC`.
    Async,
    /// Upstream's `UNDEFINED`, for internal use only.
    Undefined,
}

impl TaskType {
    /// Returns the lower-case name of this task type, upstream's
    /// `tf::to_string(TaskType)`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Placeholder => "placeholder",
            Self::Static => "static",
            Self::Runtime => "runtime",
            Self::Subflow => "subflow",
            Self::Condition => "condition",
            Self::Module => "module",
            Self::Async => "async",
            Self::Undefined => "undefined",
        }
    }
}

impl fmt::Display for TaskType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The task type a given handle implies.
///
/// Upstream switches over `_handle.index()` and ends in `default: UNDEFINED`.
/// Here the match is exhaustive on purpose: [`Handle`] gains a variant per
/// milestone, and a new variant should stop the compiler here rather than be
/// silently reported as [`TaskType::Undefined`] by a wildcard arm.
fn task_type_of(handle: &Handle) -> TaskType {
    match handle {
        Handle::Placeholder => TaskType::Placeholder,
        Handle::Static(_) => TaskType::Static,
        // Upstream folds both conditioners into one task type, so a
        // multi-condition task reports `condition` as well. The handle is what
        // tells the two apart; see [`Handle::is_multi_condition`].
        Handle::Condition(_) | Handle::MultiCondition(_) => TaskType::Condition,
        Handle::Subflow { .. } => TaskType::Subflow,
        Handle::Runtime(_) => TaskType::Runtime,
        // And both module forms fold together the same way, for the same
        // reason: upstream's `TaskType` has no name for the adopted one, so
        // `ADOPTED_MODULE` reports `MODULE` there too.
        Handle::Module(_) | Handle::AdoptedModule(_) => TaskType::Module,
    }
}

/// A read-only handle to one task of a [`Taskflow`].
///
/// Handed out by [`Taskflow::task_ref`]. This is a borrow, not the task's
/// identity: the identity is the [`Task`] index it carries, which outlives it.
///
/// # Examples
///
/// ```
/// use rustflow::{Handle, Taskflow};
///
/// let mut taskflow = Taskflow::new();
/// let a = taskflow.graph_mut().add_named_node("a", Handle::Placeholder);
///
/// let handle = taskflow.task_ref(a);
/// assert_eq!(handle.name(), "a");
/// assert!(handle.is_empty());
/// ```
#[derive(Clone, Copy, Debug)]
pub struct TaskRef<'a> {
    graph: &'a Graph,
    task: Task,
}

impl<'a> TaskRef<'a> {
    /// Borrows `task` out of `graph`.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a node of `graph`. Upstream's `Task`
    /// may hold a null or stale `Node*` and dereferences it anyway; checking
    /// once here is what lets every query below be infallible.
    #[must_use]
    pub fn new(graph: &'a Graph, task: Task) -> Self {
        assert!(
            graph.contains(task),
            "{task:?} is out of range for a graph of {} nodes",
            graph.len()
        );
        Self { graph, task }
    }

    /// Returns the index this handle borrows, the part of it that outlives the
    /// borrow.
    #[must_use]
    pub const fn task(self) -> Task {
        self.task
    }

    /// Returns the name of this task, empty if it was never named.
    #[must_use]
    pub fn name(self) -> &'a str {
        self.graph.node(self.task).name()
    }

    /// Returns the number of successors, counting duplicates.
    #[must_use]
    pub fn num_successors(self) -> usize {
        self.graph.node(self.task).num_successors()
    }

    /// Returns the number of predecessors, counting duplicates.
    #[must_use]
    pub fn num_predecessors(self) -> usize {
        self.graph.node(self.task).num_predecessors()
    }

    /// Returns how many predecessors are *not* conditioners, which is how many
    /// of them must finish before this task may run.
    #[must_use]
    pub fn num_strong_dependencies(self) -> usize {
        self.num_predecessors() - self.num_weak_dependencies()
    }

    /// Returns how many predecessors are conditioners, any one of which may
    /// instead branch away from this task.
    ///
    /// It is upstream's count exactly: over the predecessors, keyed on each
    /// predecessor's own type rather than on anything stored here, which is why
    /// adding the two conditioner handles brought it to life without touching
    /// a line of it.
    #[must_use]
    pub fn num_weak_dependencies(self) -> usize {
        self.graph
            .predecessors(self.task)
            .iter()
            .filter(|predecessor| {
                let handle = self.graph.node(**predecessor).handle();
                task_type_of(handle) == TaskType::Condition
            })
            .count()
    }

    /// Returns `true` if this task is indistinguishable from a freshly created
    /// placeholder: no work and no edges.
    ///
    /// This is the ported reading of upstream's `empty()`, which asks whether
    /// the handle is null -- see the module docs. It is the postcondition of
    /// [`TaskMut::reset`].
    #[must_use]
    pub fn is_empty(self) -> bool {
        let node = self.graph.node(self.task);
        node.handle().is_placeholder() && node.num_successors() == 0 && node.num_predecessors() == 0
    }

    /// Returns `true` if this task has work attached, upstream's
    /// `_handle.index() != 0`.
    #[must_use]
    pub fn has_work(self) -> bool {
        !self.graph.node(self.task).handle().is_placeholder()
    }

    /// Returns the task type, upstream's `type()` (`type` is a keyword here).
    #[must_use]
    pub fn task_type(self) -> TaskType {
        task_type_of(self.graph.node(self.task).handle())
    }

    /// Returns a hash of this task's identity.
    ///
    /// Upstream hashes the `Node*`, so its value is unique across every live
    /// taskflow. This hashes the arena index, so two tasks of *different*
    /// graphs can collide -- which costs nothing, because a [`Task`] is only
    /// meaningful together with the graph that produced it in the first place.
    /// Within one graph the guarantee is upstream's: equal handles hash equal,
    /// distinct ones hash distinctly.
    #[must_use]
    pub fn hash_value(self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.task.hash(&mut hasher);
        hasher.finish()
    }

    /// Applies `visitor` to each successor, in edge order.
    ///
    /// The visitor takes the [`Task`] index rather than a handle: a handle
    /// would borrow the graph for as long as the walk, which is precisely what
    /// upstream's aliasing visitor does and what cannot be reproduced here.
    /// Collect the indices and edit after the walk.
    pub fn for_each_successor<V>(self, mut visitor: V)
    where
        V: FnMut(Task),
    {
        for successor in self.graph.successors(self.task) {
            visitor(*successor);
        }
    }

    /// Applies `visitor` to each predecessor, in edge order.
    pub fn for_each_predecessor<V>(self, mut visitor: V)
    where
        V: FnMut(Task),
    {
        for predecessor in self.graph.predecessors(self.task) {
            visitor(*predecessor);
        }
    }
}

/// An editing handle to one task of a [`Taskflow`].
///
/// Handed out by [`Taskflow::task`]. Every method returns `&mut Self`, which is
/// what keeps upstream's fluent chain readable; the queries live on
/// [`TaskRef`], reachable with [`TaskMut::to_ref`], because one Rust type
/// cannot carry upstream's getter/setter `name()` pair at once.
///
/// # Examples
///
/// ```
/// use rustflow::{Handle, Taskflow};
///
/// let mut taskflow = Taskflow::new();
/// let a = taskflow.graph_mut().add_node(Handle::Placeholder);
/// let b = taskflow.graph_mut().add_node(Handle::Placeholder);
/// let c = taskflow.graph_mut().add_node(Handle::Placeholder);
///
/// // One chain, as upstream reads: name it, give it work, wire it up.
/// taskflow.task(a).name("a").work(|| println!("a")).precede([b, c]);
/// taskflow.task(c).succeed([b]);
///
/// assert_eq!(taskflow.task_ref(a).name(), "a");
/// assert!(taskflow.task_ref(a).has_work());
/// assert_eq!(taskflow.task_ref(a).num_successors(), 2);
/// assert_eq!(taskflow.task_ref(c).num_predecessors(), 2);
///
/// // Dropping the work leaves the edges; a full reset drops both halves of
/// // every edge as well.
/// taskflow.task(a).reset();
/// assert!(taskflow.task_ref(a).is_empty());
/// assert_eq!(taskflow.task_ref(b).num_predecessors(), 0);
/// ```
#[derive(Debug)]
pub struct TaskMut<'a> {
    graph: &'a mut Graph,
    task: Task,
}

impl<'a> TaskMut<'a> {
    /// Borrows `task` out of `graph` for editing.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a node of `graph`.
    #[must_use]
    pub fn new(graph: &'a mut Graph, task: Task) -> Self {
        assert!(
            graph.contains(task),
            "{task:?} is out of range for a graph of {} nodes",
            graph.len()
        );
        Self { graph, task }
    }

    /// Returns the index this handle borrows.
    #[must_use]
    pub const fn task(&self) -> Task {
        self.task
    }

    /// Reborrows this handle as a read-only one, which is where the queries
    /// live.
    ///
    /// Not `as_ref`: [`AsRef`] hands back a reference, and this hands back a
    /// fresh borrow by value.
    #[must_use]
    pub fn to_ref(&self) -> TaskRef<'_> {
        TaskRef {
            graph: self.graph,
            task: self.task,
        }
    }

    /// Renames this task. Upstream overloads `name()` for this.
    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.graph.node_mut(self.task).set_name(name);
        self
    }

    /// Attaches `callable` as this task's work, replacing whatever was there.
    ///
    /// Upstream's `work()` dispatches over five callable shapes at compile
    /// time; this is its static-task arm, and the two below are its
    /// conditioner arms.
    pub fn work<C>(&mut self, callable: C) -> &mut Self
    where
        C: FnMut() + Send + 'static,
    {
        self.graph
            .node_mut(self.task)
            .set_handle(Handle::Static(Work::new(callable)));
        self
    }

    /// Makes this task a condition task running `callable`, replacing whatever
    /// work was there.
    ///
    /// Upstream's `work()` arm for a `ConditionTaskLike` callable, which is how
    /// `test_control_flow.cpp` turns a placeholder into a conditioner after the
    /// branches it selects have been wired up. The returned `i32` indexes this
    /// task's successor list; negative or out of range selects nothing.
    pub fn condition_work<C>(&mut self, callable: C) -> &mut Self
    where
        C: FnMut() -> i32 + Send + 'static,
    {
        self.graph
            .node_mut(self.task)
            .set_handle(Handle::Condition(Work::new(callable)));
        self
    }

    /// Makes this task a multi-condition task running `callable`, replacing
    /// whatever work was there.
    ///
    /// Upstream's `work()` arm for a `MultiConditionTaskLike` callable. Each
    /// index in the returned list names a successor to run, under the same
    /// out-of-range rule as [`TaskMut::condition_work`].
    pub fn multi_condition_work<C>(&mut self, callable: C) -> &mut Self
    where
        C: FnMut() -> Vec<i32> + Send + 'static,
    {
        self.graph
            .node_mut(self.task)
            .set_handle(Handle::MultiCondition(Work::new(callable)));
        self
    }

    /// Makes this task a subflow task running `callable`, replacing whatever
    /// work was there.
    ///
    /// Upstream's `work()` arm for a `SubflowTaskLike` callable. The body is
    /// handed the [`Subflow`] it builds into; see
    /// [`FlowBuilder::emplace_subflow`](crate::FlowBuilder::emplace_subflow)
    /// for what that means and what still refuses to run one.
    ///
    /// The node gets a fresh, empty subgraph: replacing the work of a task
    /// that was already a subflow task drops the graph the previous body
    /// built, which is the same thing the next entry to that node would have
    /// done anyway.
    pub fn subflow_work<C>(&mut self, callable: C) -> &mut Self
    where
        C: for<'b> FnMut(&mut Subflow<'b>) + Send + 'static,
    {
        self.graph.node_mut(self.task).set_handle(Handle::Subflow {
            work: SubflowWork::new(callable),
            subgraph: Mutex::new(Graph::new()),
        });
        self
    }

    /// Makes this task a runtime task running `callable`, replacing whatever
    /// work was there.
    ///
    /// Upstream's `work()` arm for a `RuntimeTaskLike` callable. The body is
    /// handed the [`Runtime`] it runs on; see
    /// [`FlowBuilder::emplace_runtime`](crate::FlowBuilder::emplace_runtime).
    pub fn runtime_work<C>(&mut self, callable: C) -> &mut Self
    where
        C: for<'b> FnMut(&mut Runtime<'b>) + Send + 'static,
    {
        self.graph
            .node_mut(self.task)
            .set_handle(Handle::Runtime(RuntimeWork::new(callable)));
        self
    }

    /// Makes this task precede each of `tasks`, upstream's variadic
    /// `precede(Ts&&...)`.
    ///
    /// Duplicates are kept, as upstream keeps them.
    ///
    /// # Panics
    ///
    /// Panics if any of `tasks` is out of range for this graph.
    pub fn precede(&mut self, tasks: impl IntoIterator<Item = Task>) -> &mut Self {
        for other in tasks {
            self.graph.add_edge(self.task, other);
        }
        self
    }

    /// Makes this task succeed each of `tasks`, upstream's `succeed(Ts&&...)`.
    ///
    /// # Panics
    ///
    /// Panics if any of `tasks` is out of range for this graph.
    pub fn succeed(&mut self, tasks: impl IntoIterator<Item = Task>) -> &mut Self {
        for other in tasks {
            self.graph.add_edge(other, self.task);
        }
        self
    }

    /// Removes each `tasks -> this` dependency, both halves of it.
    ///
    /// A task listed that is not actually a predecessor is a no-op, as it is
    /// upstream.
    ///
    /// # Panics
    ///
    /// Panics if any of `tasks` is out of range for this graph.
    pub fn remove_predecessors(&mut self, tasks: impl IntoIterator<Item = Task>) -> &mut Self {
        for other in tasks {
            self.graph.remove_edge(other, self.task);
        }
        self
    }

    /// Removes each `this -> tasks` dependency, both halves of it.
    ///
    /// # Panics
    ///
    /// Panics if any of `tasks` is out of range for this graph.
    pub fn remove_successors(&mut self, tasks: impl IntoIterator<Item = Task>) -> &mut Self {
        for other in tasks {
            self.graph.remove_edge(self.task, other);
        }
        self
    }

    /// Returns this task to its just-created state: no work, no edges.
    ///
    /// The name is kept, as [`Taskflow::clear`] keeps the taskflow's. See the
    /// module docs for why this is not upstream's `_node = nullptr`, and why
    /// the edges go through [`Graph::remove_edge`] rather than upstream's
    /// one-sided `_edges.clear()`.
    pub fn reset(&mut self) -> &mut Self {
        // The neighbour lists have to be cloned out first: removing an edge
        // needs the graph mutably, and the slices borrow it.
        let successors = self.graph.successors(self.task).to_vec();
        let predecessors = self.graph.predecessors(self.task).to_vec();
        for successor in successors {
            self.graph.remove_edge(self.task, successor);
        }
        for predecessor in predecessors {
            self.graph.remove_edge(predecessor, self.task);
        }
        self.reset_work()
    }

    /// Drops this task's work, leaving it a placeholder with its edges intact.
    pub fn reset_work(&mut self) -> &mut Self {
        self.graph
            .node_mut(self.task)
            .set_handle(Handle::Placeholder);
        self
    }
}

/// The two accessors that hand out the handles above.
///
/// Issue #9 puts these in `src/taskflow.rs`; they are here instead so that
/// this change touches no file a sibling change is editing. Rust allows an
/// inherent `impl` anywhere in the defining crate, and both bodies go through
/// the public [`Taskflow::graph`]/[`Taskflow::graph_mut`], so nothing about
/// the split is load-bearing.
impl Taskflow {
    /// Returns an editing handle to `task`.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a task of this taskflow.
    pub fn task(&mut self, task: Task) -> TaskMut<'_> {
        TaskMut::new(self.graph_mut(), task)
    }

    /// Returns a read-only handle to `task`.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a task of this taskflow.
    #[must_use]
    pub fn task_ref(&self, task: Task) -> TaskRef<'_> {
        TaskRef::new(self.graph(), task)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Builds a taskflow of `n` placeholders named `a`, `b`, ...
    fn taskflow_of(n: usize) -> (Taskflow, Vec<Task>) {
        let mut taskflow = Taskflow::new();
        let tasks = (0..n)
            .map(|i| {
                let name = char::from(b'a' + u8::try_from(i).unwrap()).to_string();
                taskflow
                    .graph_mut()
                    .add_named_node(name, Handle::Placeholder)
            })
            .collect();
        (taskflow, tasks)
    }

    #[test]
    fn task_type_names_match_upstream_to_string() {
        let types = [
            TaskType::Placeholder,
            TaskType::Static,
            TaskType::Runtime,
            TaskType::Subflow,
            TaskType::Condition,
            TaskType::Module,
            TaskType::Async,
            TaskType::Undefined,
        ];
        let names: Vec<String> = types.iter().map(ToString::to_string).collect();
        assert_eq!(
            names,
            [
                "placeholder",
                "static",
                "runtime",
                "subflow",
                "condition",
                "module",
                "async",
                "undefined"
            ]
        );
        assert_eq!(TaskType::Static.as_str(), "static");
    }

    #[test]
    fn the_fluent_chain_edits_one_task() {
        let (mut taskflow, t) = taskflow_of(3);
        let counter = Arc::new(AtomicUsize::new(0));
        let bumped = Arc::clone(&counter);

        taskflow
            .task(t[0])
            .name("renamed")
            .work(move || {
                bumped.fetch_add(1, Ordering::Relaxed);
            })
            .precede([t[1], t[2]]);

        let handle = taskflow.task_ref(t[0]);
        assert_eq!(handle.name(), "renamed");
        assert_eq!(handle.task(), t[0]);
        assert!(handle.has_work());
        assert_eq!(handle.task_type(), TaskType::Static);
        assert_eq!(handle.num_successors(), 2);

        // The work is the closure that was handed over, not a copy of it.
        let Handle::Static(work) = taskflow.graph().node(t[0]).handle() else {
            panic!("work() left a placeholder behind");
        };
        work.invoke();
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn reset_work_keeps_the_edges_and_reset_drops_them() {
        let (mut taskflow, t) = taskflow_of(3);
        taskflow.task(t[1]).succeed([t[0]]).precede([t[2]]);
        taskflow.task(t[1]).work(|| {});

        assert!(!taskflow.task_ref(t[1]).is_empty());

        taskflow.task(t[1]).reset_work();
        assert!(!taskflow.task_ref(t[1]).has_work());
        assert_eq!(taskflow.task_ref(t[1]).task_type(), TaskType::Placeholder);
        // Still not "empty": the edges are untouched by reset_work.
        assert!(!taskflow.task_ref(t[1]).is_empty());
        assert_eq!(taskflow.task_ref(t[1]).num_predecessors(), 1);
        assert_eq!(taskflow.task_ref(t[1]).num_successors(), 1);

        taskflow.task(t[1]).work(|| {}).reset();
        assert!(taskflow.task_ref(t[1]).is_empty());
        // And the neighbours lost their half of each edge, which upstream's
        // one-sided `_edges.clear()` would have left dangling.
        assert_eq!(taskflow.task_ref(t[0]).num_successors(), 0);
        assert_eq!(taskflow.task_ref(t[2]).num_predecessors(), 0);
        assert!(taskflow.task_ref(t[0]).is_empty());
        assert!(taskflow.task_ref(t[2]).is_empty());
        // The name survives a reset, as the taskflow's own survives `clear`.
        assert_eq!(taskflow.task_ref(t[1]).name(), "b");
    }

    #[test]
    fn reset_removes_duplicate_and_self_edges() {
        let (mut taskflow, t) = taskflow_of(2);
        taskflow
            .task(t[0])
            .precede([t[1], t[1], t[0]])
            .succeed([t[1]]);
        assert_eq!(taskflow.task_ref(t[0]).num_successors(), 3);
        assert_eq!(taskflow.task_ref(t[0]).num_predecessors(), 2);

        taskflow.task(t[0]).reset();
        assert!(taskflow.task_ref(t[0]).is_empty());
        assert!(taskflow.task_ref(t[1]).is_empty());
    }

    #[test]
    fn a_predecessor_that_is_not_a_conditioner_is_a_strong_dependency() {
        let (mut taskflow, t) = taskflow_of(4);
        taskflow.task(t[3]).succeed([t[0], t[1], t[2]]);

        let handle = taskflow.task_ref(t[3]);
        assert_eq!(handle.num_predecessors(), 3);
        assert_eq!(handle.num_strong_dependencies(), 3);
        assert_eq!(handle.num_weak_dependencies(), 0);
        // The counts are over predecessors only, as upstream's are.
        let source = taskflow.task_ref(t[0]);
        assert_eq!(source.num_successors(), 1);
        assert_eq!(source.num_strong_dependencies(), 0);
        assert_eq!(source.num_weak_dependencies(), 0);
    }

    /// Both conditioners report one task type, as upstream's `type()` switch
    /// maps `CONDITION` and `MULTI_CONDITION` to the same `TaskType`.
    #[test]
    fn both_conditioners_report_one_task_type() {
        let (mut taskflow, t) = taskflow_of(2);
        taskflow.task(t[0]).condition_work(|| 0);
        taskflow.task(t[1]).multi_condition_work(|| vec![0, 1]);

        for task in &t {
            let handle = taskflow.task_ref(*task);
            assert_eq!(handle.task_type(), TaskType::Condition);
            assert_eq!(handle.task_type().as_str(), "condition");
            assert!(handle.has_work());
            assert!(!handle.is_empty());
        }
        // The task type cannot tell the two apart; the handle can.
        assert!(taskflow.graph().node(t[0]).handle().is_condition());
        assert!(taskflow.graph().node(t[1]).handle().is_multi_condition());
    }

    /// Every dynamic-tasking handle reports the task type upstream's `type()`
    /// switch gives it, with the two module forms sharing one name because
    /// upstream's `TaskType` has no second one.
    #[test]
    fn the_dynamic_tasking_handles_report_their_task_types() {
        let (mut taskflow, t) = taskflow_of(4);
        taskflow.task(t[0]).subflow_work(|_| {});
        taskflow.task(t[1]).runtime_work(|_| {});
        taskflow
            .graph_mut()
            .node_mut(t[2])
            .set_handle(Handle::Module(Arc::new(Taskflow::new())));
        taskflow
            .graph_mut()
            .node_mut(t[3])
            .set_handle(Handle::AdoptedModule(Graph::new()));

        let types: Vec<TaskType> = t
            .iter()
            .map(|task| taskflow.task_ref(*task).task_type())
            .collect();
        assert_eq!(
            types,
            vec![
                TaskType::Subflow,
                TaskType::Runtime,
                TaskType::Module,
                TaskType::Module
            ]
        );
        let names: Vec<&str> = types.iter().map(|kind| kind.as_str()).collect();
        assert_eq!(names, vec!["subflow", "runtime", "module", "module"]);

        for task in &t {
            let handle = taskflow.task_ref(*task);
            assert!(handle.has_work());
            assert!(!handle.is_empty());
            // None of the four is a conditioner, so none of them makes a
            // dependency on it weak.
            assert_eq!(handle.num_weak_dependencies(), 0);
        }
    }

    /// The two new setters are upstream's remaining `work()` arms: they
    /// replace whatever was there, in either direction, and leave the graph
    /// around the node alone.
    #[test]
    fn the_dynamic_tasking_setters_replace_the_work_in_place() {
        let (mut taskflow, t) = taskflow_of(3);
        taskflow.task(t[1]).succeed([t[0]]).precede([t[2]]);

        taskflow.task(t[1]).work(|| {}).subflow_work(|_| {});
        assert_eq!(taskflow.task_ref(t[1]).task_type(), TaskType::Subflow);
        let Handle::Subflow { subgraph, .. } = taskflow.graph().node(t[1]).handle() else {
            panic!("subflow_work left something else behind");
        };
        // A fresh subflow task comes with an empty graph of its own.
        assert!(subgraph.lock().expect("uncontended").is_empty());

        taskflow.task(t[1]).runtime_work(|_| {});
        assert_eq!(taskflow.task_ref(t[1]).task_type(), TaskType::Runtime);
        assert!(taskflow.graph().node(t[1]).handle().is_runtime());

        // Back to a static task, with the edges and the name where they were.
        taskflow.task(t[1]).work(|| {});
        assert_eq!(taskflow.task_ref(t[1]).task_type(), TaskType::Static);
        assert_eq!(taskflow.task_ref(t[1]).name(), "b");
        assert_eq!(taskflow.task_ref(t[1]).num_predecessors(), 1);
        assert_eq!(taskflow.task_ref(t[1]).num_successors(), 1);

        // And `reset_work` drops either of them as readily as static work,
        // subgraph and all.
        taskflow.task(t[1]).subflow_work(|_| {}).reset();
        assert!(taskflow.task_ref(t[1]).is_empty());
    }

    /// The setters are upstream's `work()` arms, so they replace whatever was
    /// there -- in either direction -- and keep the edges and the name.
    #[test]
    fn the_conditioner_setters_replace_the_work_in_place() {
        let (mut taskflow, t) = taskflow_of(3);
        taskflow.task(t[1]).succeed([t[0]]).precede([t[2]]);

        taskflow.task(t[1]).work(|| {}).condition_work(|| 1);
        assert_eq!(taskflow.task_ref(t[1]).task_type(), TaskType::Condition);
        let Handle::Condition(work) = taskflow.graph().node(t[1]).handle() else {
            panic!("condition_work left something else behind");
        };
        assert_eq!(work.invoke(), 1);

        taskflow.task(t[1]).multi_condition_work(|| vec![0]);
        let Handle::MultiCondition(work) = taskflow.graph().node(t[1]).handle() else {
            panic!("multi_condition_work left something else behind");
        };
        assert_eq!(work.invoke(), vec![0]);

        // Back to a static task, and the graph around it never moved.
        taskflow.task(t[1]).work(|| {});
        assert_eq!(taskflow.task_ref(t[1]).task_type(), TaskType::Static);
        assert_eq!(taskflow.task_ref(t[1]).name(), "b");
        assert_eq!(taskflow.task_ref(t[1]).num_predecessors(), 1);
        assert_eq!(taskflow.task_ref(t[1]).num_successors(), 1);

        // And `reset_work` drops a conditioner as readily as it drops static
        // work, which is what keeps `is_empty` the postcondition of `reset`.
        taskflow.task(t[1]).condition_work(|| 0).reset();
        assert!(taskflow.task_ref(t[1]).is_empty());
    }

    /// A conditioner predecessor is a *weak* dependency and every other one is
    /// strong, which is the split `num_weak_dependencies` was written for
    /// before either conditioner handle existed.
    #[test]
    fn a_conditioner_predecessor_is_a_weak_dependency() {
        let (mut taskflow, t) = taskflow_of(4);
        taskflow.task(t[0]).condition_work(|| 0);
        taskflow.task(t[1]).multi_condition_work(|| vec![0]);
        // d succeeds a condition, a multi-condition and a plain placeholder.
        taskflow.task(t[3]).succeed([t[0], t[1], t[2]]);

        let handle = taskflow.task_ref(t[3]);
        assert_eq!(handle.num_predecessors(), 3);
        assert_eq!(handle.num_weak_dependencies(), 2);
        assert_eq!(handle.num_strong_dependencies(), 1);

        // Duplicates count twice, as they do in the join counter.
        taskflow.task(t[3]).succeed([t[0], t[2]]);
        let handle = taskflow.task_ref(t[3]);
        assert_eq!(handle.num_predecessors(), 5);
        assert_eq!(handle.num_weak_dependencies(), 3);
        assert_eq!(handle.num_strong_dependencies(), 2);

        // A conditioner's *successors* say nothing about its own counts.
        let source = taskflow.task_ref(t[0]);
        assert_eq!(source.num_successors(), 2);
        assert_eq!(source.num_weak_dependencies(), 0);
        assert_eq!(source.num_strong_dependencies(), 0);
    }

    #[test]
    fn a_handle_refuses_a_stale_index() {
        let (mut taskflow, _) = taskflow_of(1);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = taskflow.task_ref(Task::new(1));
            }))
            .is_err()
        );
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = taskflow.task(Task::new(7));
            }))
            .is_err()
        );
    }
}

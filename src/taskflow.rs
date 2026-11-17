//! Translated from `taskflow/core/taskflow.hpp` (Taskflow v4.1.0).
//!
//! Only the half of `tf::Taskflow` that owns a graph is here. The `emplace`
//! family, dependency editing, DOT output and everything about running a
//! taskflow arrive in their own modules; upstream's `_topologies` queue and
//! `_mutex` belong to the executor and are not ported yet.

use crate::graph::Graph;

/// A named task dependency graph.
///
/// `Taskflow` owns its [`Graph`] outright. Upstream hands out `tf::Task`
/// objects that alias into the graph while the caller keeps mutating it; here
/// the graph stays behind [`Taskflow::graph`]/[`Taskflow::graph_mut`] and tasks
/// are addressed by [`crate::Task`] index, so nothing aliases.
#[derive(Debug, Default)]
pub struct Taskflow {
    name: String,
    graph: Graph,
}

impl Taskflow {
    /// Creates an empty, unnamed taskflow.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty taskflow with the given name.
    ///
    /// Upstream spells this as a second constructor overload, which Rust has
    /// no equivalent of.
    pub fn with_name(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            graph: Graph::new(),
        }
    }

    /// Returns the name of this taskflow, empty unless one was set.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Renames this taskflow. Upstream overloads `name()` for this.
    pub fn set_name(&mut self, name: impl Into<String>) {
        self.name = name.into();
    }

    /// Returns the number of tasks in the graph.
    #[must_use]
    pub fn num_tasks(&self) -> usize {
        self.graph.len()
    }

    /// Returns `true` if the graph holds no tasks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.graph.is_empty()
    }

    /// Drops every task, invalidating every [`crate::Task`] handed out so far.
    ///
    /// The name is kept, as upstream keeps it.
    pub fn clear(&mut self) {
        self.graph.clear();
    }

    /// Returns the underlying graph.
    #[must_use]
    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    /// Returns the underlying graph, mutably.
    ///
    /// This is the seam the task-building API is written against.
    pub fn graph_mut(&mut self) -> &mut Graph {
        &mut self.graph
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Taskflow>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Handle, Task, Work};

    #[test]
    fn a_new_taskflow_is_empty_and_unnamed() {
        let taskflow = Taskflow::new();
        assert_eq!(taskflow.name(), "");
        assert_eq!(taskflow.num_tasks(), 0);
        assert!(taskflow.is_empty());
        assert!(taskflow.graph().is_empty());
    }

    #[test]
    fn name_round_trips() {
        let mut taskflow = Taskflow::with_name("My Taskflow");
        assert_eq!(taskflow.name(), "My Taskflow");
        taskflow.set_name(String::from("renamed"));
        assert_eq!(taskflow.name(), "renamed");
    }

    #[test]
    fn num_tasks_follows_the_graph() {
        let mut taskflow = Taskflow::with_name("counting");
        let a = taskflow
            .graph_mut()
            .add_named_node("a", Handle::Placeholder);
        let b = taskflow
            .graph_mut()
            .add_named_node("b", Handle::Static(Work::new(|| {})));
        taskflow.graph_mut().add_edge(a, b);

        assert_eq!(taskflow.num_tasks(), 2);
        assert!(!taskflow.is_empty());
        assert_eq!(taskflow.graph().node(a).num_successors(), 1);
        assert_eq!(taskflow.graph().node(b).num_predecessors(), 1);

        taskflow.clear();
        assert_eq!(taskflow.num_tasks(), 0);
        assert!(taskflow.is_empty());
        // Clearing keeps the name, and the arena starts over.
        assert_eq!(taskflow.name(), "counting");
        assert_eq!(
            taskflow.graph_mut().add_node(Handle::Placeholder),
            Task::new(0)
        );
    }
}

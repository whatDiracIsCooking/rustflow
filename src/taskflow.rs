//! Translated from `taskflow/core/taskflow.hpp` (Taskflow v4.1.0).
//!
//! Only the half of `tf::Taskflow` that owns a graph is here, plus upstream's
//! `_topologies` queue and the `_mutex` guarding it -- `_fetch_enqueue` and
//! the half of `_tear_down_topology` that rotates the queue.
//!
//! # Design
//!
//! A taskflow is a name plus a shared `Flow`, so that a queued job cannot
//! outlive the graph and the run it names, where upstream reaches a running
//! graph through raw references and leaves *"do not destroy or modify a
//! taskflow while it is running"* to its documentation: `DESIGN.md`'s "A run
//! owns the graph it walks" has the argument, `executor::topology` the order
//! the shares are released in. The name is outside the allocation because
//! nothing in a run reads it, which keeps [`Taskflow::set_name`] always
//! available.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::executor::topology::Topology;
use crate::graph::Graph;

/// A named task dependency graph.
///
/// `Taskflow` owns its [`Graph`] outright, behind
/// [`Taskflow::graph`]/[`Taskflow::graph_mut`], and tasks are addressed by
/// [`crate::Task`] index rather than by a handle that aliases into it. It
/// also owns the queue of runs of itself, as upstream does: runs of one
/// taskflow serialise rather than going at once, which makes
/// `executor.run(&taskflow)` twice in a row safe with no wait between. Both
/// live in the crate-private `Flow` the taskflow shares with its runs.
#[derive(Debug, Default)]
pub struct Taskflow {
    name: String,
    /// The graph and the runs of it, shared with those runs. A `Taskflow`
    /// holds the one share that can hand out `&mut`: [`Taskflow::flow_mut`].
    flow: Arc<Flow>,
}

/// A taskflow's storage: the graph a run walks and the queue of runs waiting
/// to walk it.
///
/// Everything a run needs is in here and nothing else is: a [`Topology`]
/// holds a share of it for the length of the run, as does each queued
/// [`Job`](crate::executor::topology::Job) of that run.
#[derive(Debug, Default)]
pub(crate) struct Flow {
    graph: Graph,
    /// The runs handed out and not yet finished, oldest first: upstream's
    /// `_topologies` and `_mutex`. The front one is in flight and the rest
    /// wait for it, pushed by [`Flow::enqueue_run`] and popped by
    /// [`Flow::pop_finished_run`].
    runs: Mutex<VecDeque<Arc<Topology>>>,
}

impl Taskflow {
    /// Creates an empty, unnamed taskflow.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty taskflow with the given name: upstream's second
    /// constructor overload.
    pub fn with_name(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            flow: Arc::default(),
        }
    }

    /// Returns the name of this taskflow, empty unless one was set.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Renames this taskflow. Upstream overloads `name()` for this, and the
    /// name being no part of what a run reads, it is the one edit that is
    /// always available -- even, in principle, while a run is in flight.
    pub fn set_name(&mut self, name: impl Into<String>) {
        self.name = name.into();
    }

    /// Returns the number of tasks in the graph.
    #[must_use]
    pub fn num_tasks(&self) -> usize {
        self.graph().len()
    }

    /// Returns `true` if the graph holds no tasks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.graph().is_empty()
    }

    /// Drops every task, invalidating every [`crate::Task`] handed out so
    /// far. The name is kept, as upstream keeps it.
    ///
    /// # Panics
    ///
    /// Panics if a run is still in flight, as [`Taskflow::graph_mut`] does.
    pub fn clear(&mut self) {
        self.graph_mut().clear();
    }

    /// Returns the underlying graph.
    #[must_use]
    pub fn graph(&self) -> &Graph {
        self.flow.graph()
    }

    /// Returns the underlying graph, mutably: the seam the task-building API
    /// is written against.
    ///
    /// # Panics
    ///
    /// Panics if a run of this taskflow is still in flight. A
    /// [`RunHandle`](crate::RunHandle) borrows the taskflow while it lives,
    /// so ordinary code cannot reach this during a run at all; what can is
    /// code that *leaks* a handle, ending the borrow while workers are still
    /// walking the graph.
    pub fn graph_mut(&mut self) -> &mut Graph {
        self.flow_mut().graph_mut()
    }

    /// The storage this taskflow shares with the runs of it.
    pub(crate) fn flow(&self) -> &Arc<Flow> {
        &self.flow
    }

    /// The storage, mutably, which is only possible while nothing is running.
    ///
    /// The uniqueness check is the whole of "do not modify a running
    /// taskflow" for the leaked handle [`Taskflow::graph_mut`] describes: a
    /// run in flight is a run whose topology and queued jobs hold shares of
    /// this allocation. It cannot fire for a run that has *finished*, however
    /// recently, the waiter being released by whoever drops the **last** such
    /// share (`DESIGN.md` on how a run's waiter is released); a second run
    /// queued behind it holds its own, and editing while *that* runs is what
    /// this refuses.
    ///
    /// # Panics
    ///
    /// Panics if a run of this taskflow is still in flight.
    fn flow_mut(&mut self) -> &mut Flow {
        Arc::get_mut(&mut self.flow)
            .expect("a taskflow cannot be edited while a run of it is in flight")
    }
}

impl Flow {
    pub(crate) fn graph(&self) -> &Graph {
        &self.graph
    }

    /// The graph, mutably: exclusive because reachable only through
    /// `&mut Taskflow` -- see [`Taskflow::flow_mut`].
    fn graph_mut(&mut self) -> &mut Graph {
        &mut self.graph
    }

    /// Whether this graph has a task a run could start at: upstream's
    /// `f.empty()` check at the top of `run_until`, widened by one case -- a
    /// graph whose every node is in a cycle has no source either. Asking
    /// before a run is queued keeps the tear-down from having to deal with a
    /// queued run that would schedule nothing; the walk is one `set_up_graph`
    /// then repeats, cheaper than remembering the answer.
    pub(crate) fn has_source_task(&self) -> bool {
        self.graph
            .iter()
            .any(|(_, node)| node.num_predecessors() == 0)
    }

    /// Queues `run` as a run of this taskflow and reports how many runs were
    /// already queued ahead of it: upstream's `_fetch_enqueue`. A zero means
    /// the caller must start this run; anything else means an earlier run
    /// will start it from its tear-down.
    pub(crate) fn enqueue_run(&self, run: Arc<Topology>) -> usize {
        let mut runs = self.lock_runs();
        let queued_ahead = runs.len();
        runs.push_back(run);
        queued_ahead
    }

    /// Drops the finished run at the front of the queue and hands back the
    /// one behind it, which stays queued and is the next to be started: the
    /// queue half of upstream's `_tear_down_topology`, which moves the front
    /// out, pops it and reads the new front under the same lock.
    ///
    /// A topology that is not the front of this queue is not in it at all, so
    /// the identity check is a condition rather than an assertion: that is a
    /// [`corun`](crate::Executor::corun), whose topology the corunning
    /// worker's frame owns and does not queue -- a corun cannot wait its
    /// turn, the frame it blocks being the one that would run the graph ahead.
    pub(crate) fn pop_finished_run(&self, finished: &Topology) -> Option<Arc<Topology>> {
        let mut runs = self.lock_runs();
        if !runs
            .front()
            .is_some_and(|front| std::ptr::eq(Arc::as_ptr(front), finished))
        {
            return None;
        }

        // The share this queue held is dropped here, never the last one: the
        // thread tearing the run down holds one of its own.
        drop(runs.pop_front());
        runs.front().map(Arc::clone)
    }

    /// The run queue. A poisoned lock is taken rather than propagated, as
    /// the executor's are: nothing under it can leave the queue half-updated.
    fn lock_runs(&self) -> MutexGuard<'_, VecDeque<Arc<Topology>>> {
        self.runs.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Taskflow>();
    // A job owns an `Arc<Flow>` and hands out a `&Flow` on whichever worker
    // dequeues it.
    assert_send_sync::<Flow>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Handle, Task, Work};
    use crate::prelude::*;

    /// A run of nothing, which is all these cases need of a topology: the
    /// queue holds shares and never looks inside one.
    fn run() -> Arc<Topology> {
        Arc::new(Topology::new(|| true, || {}, Arc::default()))
    }

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

    /// The three shapes the scheduler cares about: a graph with a task to start
    /// at, an empty graph, and a non-empty graph with no start -- every node of
    /// which is in a cycle, so no run of it could ever begin.
    #[test]
    fn only_a_graph_with_a_predecessorless_task_can_be_run() {
        assert!(!Taskflow::new().flow().has_source_task());

        let mut chain = Taskflow::new();
        let (a, b) = chain.emplace_n((|| {}, || {}));
        chain.task(a).precede([b]);
        assert!(chain.flow().has_source_task());

        chain.task(b).precede([a]);
        assert!(
            !chain.flow().has_source_task(),
            "a cycle through every node leaves nowhere to start"
        );
    }

    /// The queue is upstream's `_topologies`: a run is told how many are ahead
    /// of it, which is what decides whether its caller starts it or the run in
    /// front does.
    #[test]
    fn the_run_queue_reports_how_many_runs_are_ahead() {
        let taskflow = Taskflow::new();
        let flow = taskflow.flow();
        let (first, second, third) = (run(), run(), run());

        assert_eq!(flow.enqueue_run(Arc::clone(&first)), 0);
        assert_eq!(flow.enqueue_run(Arc::clone(&second)), 1);
        assert_eq!(flow.enqueue_run(Arc::clone(&third)), 2);

        // Popping the front hands back the next, which stays queued -- so a
        // run arriving now is still told there is one ahead of it.
        let next = flow
            .pop_finished_run(&first)
            .expect("two runs were behind the first");
        assert!(Arc::ptr_eq(&next, &second));
        assert_eq!(flow.enqueue_run(run()), 2);

        assert!(
            flow.pop_finished_run(&second)
                .is_some_and(|next| Arc::ptr_eq(&next, &third))
        );
        assert!(flow.pop_finished_run(&third).is_some());

        // And the last run of all has nobody behind it.
        let last = flow.lock_runs().front().map(Arc::clone).expect("one left");
        assert!(flow.pop_finished_run(&last).is_none());
        assert!(flow.lock_runs().is_empty());
    }

    /// The uniqueness check behind `graph_mut`: a taskflow whose storage
    /// someone else holds a share of cannot be edited. Nothing in safe code
    /// can hold one except a run -- this case stands in for one by cloning the
    /// share itself, which is what a queued job does.
    #[test]
    #[should_panic(expected = "a taskflow cannot be edited while a run of it is in flight")]
    fn a_taskflow_whose_storage_is_shared_cannot_be_edited() {
        let mut taskflow = Taskflow::new();
        let _run_would_hold_this = Arc::clone(taskflow.flow());
        taskflow.graph_mut();
    }
}

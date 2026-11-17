//! Translated from `taskflow/core/taskflow.hpp` (Taskflow v4.1.0).
//!
//! Only the half of `tf::Taskflow` that owns a graph is here, plus upstream's
//! `_topologies` queue and the `_mutex` that guards it -- `_fetch_enqueue` and
//! the half of `_tear_down_topology` that rotates the queue. The `emplace`
//! family, dependency editing and DOT output arrive in their own modules.
//!
//! # Why a taskflow is a name and a shared `Flow`
//!
//! Upstream reaches a running graph through raw references: a queued `Node`
//! holds a `Topology*`, a `Topology` holds a `Taskflow&`, and *"do not destroy
//! or modify a taskflow while it is running"* is a rule its documentation
//! states and its types cannot. The one `shared_ptr<Topology>` it does keep
//! lives in the taskflow's own queue, so even that share is gone once the
//! taskflow is.
//!
//! A port cannot inherit that, because safe Rust has to stay safe when the
//! caller does something the documentation told it not to. So the storage a
//! run reaches -- the graph and the queue of runs -- is split out into a
//! `Flow`, one reference-counted allocation that every queued job owns a
//! share of. A job cannot outlive what it names, whatever the caller does
//! with the handle it was given; see `executor::topology` for the whole
//! argument and issue #40 for what it replaced.
//!
//! The name stays outside that allocation because nothing in a run reads it,
//! and because [`Taskflow::set_name`] is then still an edit a caller can make
//! at any time.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::executor::topology::Topology;
use crate::graph::Graph;

/// A named task dependency graph.
///
/// `Taskflow` owns its [`Graph`] outright. Upstream hands out `tf::Task`
/// objects that alias into the graph while the caller keeps mutating it; here
/// the graph stays behind [`Taskflow::graph`]/[`Taskflow::graph_mut`] and tasks
/// are addressed by [`crate::Task`] index, so nothing aliases.
///
/// `Taskflow` also owns the queue of runs of itself, as upstream does: several
/// runs of one taskflow line up behind each other instead of running at once,
/// and each is started by the tear-down of the one before it. That is what
/// makes `executor.run(&taskflow)` twice in a row safe without a wait in
/// between -- the two runs share one graph, whose per-node counters only one of
/// them may be using.
///
/// Both of those live in one allocation -- the crate-private `Flow` -- that
/// the taskflow shares with the runs of it, which is what makes a queued
/// job's reach into a graph an *owning* reach. The module docs say why that
/// is not upstream's arrangement.
#[derive(Debug, Default)]
pub struct Taskflow {
    name: String,
    /// The graph and the runs of it, shared with those runs.
    ///
    /// A `Taskflow` holds the one share that can hand out `&mut`, which is
    /// what keeps editing a graph ordinary safe code: see
    /// [`Taskflow::flow_mut`].
    flow: Arc<Flow>,
}

/// A taskflow's storage: the graph a run walks, and the queue of runs waiting
/// to walk it.
///
/// Upstream has no equivalent, because it has no need of one -- its nodes and
/// topologies point at each other and at the taskflow, and the rule that keeps
/// those pointers valid is a sentence of documentation. This is the shape that
/// rule takes in Rust: one allocation behind an [`Arc`], shared by the
/// taskflow, by every run of it and by every job of those runs, so that
/// nothing a worker is walking can be freed under it.
///
/// Everything a run needs is in here and nothing else is. A [`Topology`] holds
/// a share of it for the length of the run, and so does each queued
/// [`Job`](crate::executor::topology::Job) of that run; both are released in
/// an order that leaves the taskflow's own share the only one left the moment the
/// run's last job is accounted for, which is what keeps
/// [`Taskflow::graph_mut`] working the instant a `wait` returns.
#[derive(Debug, Default)]
pub(crate) struct Flow {
    /// The graph itself.
    graph: Graph,
    /// The runs of this taskflow that have been handed out and not yet
    /// finished, oldest first: upstream's `_topologies` and `_mutex`.
    ///
    /// The front one is the run in flight; the rest are waiting for it. A run
    /// is pushed by [`Flow::enqueue_run`] when its handle is created and
    /// popped by [`Flow::pop_finished_run`] when its last graph run is over,
    /// which is also where the next one is started.
    runs: Mutex<VecDeque<Arc<Topology>>>,
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
            flow: Arc::default(),
        }
    }

    /// Returns the name of this taskflow, empty unless one was set.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Renames this taskflow. Upstream overloads `name()` for this.
    ///
    /// The name is not part of what a run reads, so this is the one edit that
    /// is always available -- even, in principle, while a run is in flight.
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

    /// Drops every task, invalidating every [`crate::Task`] handed out so far.
    ///
    /// The name is kept, as upstream keeps it.
    ///
    /// # Panics
    ///
    /// Panics if a run of this taskflow is still in flight, for the reason
    /// [`Taskflow::graph_mut`] gives.
    pub fn clear(&mut self) {
        self.graph_mut().clear();
    }

    /// Returns the underlying graph.
    #[must_use]
    pub fn graph(&self) -> &Graph {
        self.flow.graph()
    }

    /// Returns the underlying graph, mutably.
    ///
    /// This is the seam the task-building API is written against.
    ///
    /// # Panics
    ///
    /// Panics if a run of this taskflow is still in flight. A
    /// [`RunHandle`](crate::RunHandle) borrows the taskflow for as long as it
    /// lives, so ordinary code cannot reach this method during a run at all --
    /// the borrow is a compile error. What can reach it is code that *leaks* a
    /// handle: [`std::mem::forget`] and friends end the borrow while the run
    /// goes on, and the graph is then still being walked by workers. Editing
    /// it would race them, so this panics instead. See issue #40.
    pub fn graph_mut(&mut self) -> &mut Graph {
        self.flow_mut().graph_mut()
    }

    /// The storage this taskflow shares with the runs of it.
    pub(crate) fn flow(&self) -> &Arc<Flow> {
        &self.flow
    }

    /// The storage, mutably, which is only possible while nothing is running.
    ///
    /// The `expect` is the whole of "do not modify a running taskflow" for
    /// code that got past the borrow checker by leaking its handle: a run in
    /// flight is a run whose topology and whose queued jobs hold shares of
    /// this allocation, so the uniqueness check is exactly the question "is
    /// anything still running". It cannot fire for a run that has *finished*,
    /// however recently: the tear-down releases the run's share of the storage
    /// before it releases the thread waiting on the run, and each job releases
    /// its own before the join counter that ends the run reaches zero.
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
    /// The graph.
    pub(crate) fn graph(&self) -> &Graph {
        &self.graph
    }

    /// The graph, mutably. Reachable only through `&mut Taskflow`, which is
    /// what makes it exclusive -- see [`Taskflow::flow_mut`].
    fn graph_mut(&mut self) -> &mut Graph {
        &mut self.graph
    }

    /// Whether this graph has a task a run could start at.
    ///
    /// Upstream's `f.empty()` check at the top of `run_until` is this, widened
    /// by one case: an empty graph has no source, and neither has a graph whose
    /// every node is in a cycle. Both answer "nothing to run", and asking here
    /// -- before a run is ever queued -- is what keeps the tear-down from
    /// having to deal with a queued run that would schedule nothing.
    ///
    /// It walks the graph, which `set_up_graph` then walks again. That is
    /// the same trade the port already makes for the sources, and the same
    /// answer: a walk is cheaper than an allocation, and it buys a scheduling
    /// path with no empty case in it.
    pub(crate) fn has_source_task(&self) -> bool {
        self.graph
            .iter()
            .any(|(_, node)| node.num_predecessors() == 0)
    }

    /// Queues `run` as a run of this taskflow and reports how many runs were
    /// already queued ahead of it. Upstream's `_fetch_enqueue`.
    ///
    /// A zero means this run is the front of the queue and its caller must
    /// start it; anything else means an earlier run is in flight and will start
    /// this one from its tear-down.
    pub(crate) fn enqueue_run(&self, run: Arc<Topology>) -> usize {
        let mut runs = self.lock_runs();
        let queued_ahead = runs.len();
        runs.push_back(run);
        queued_ahead
    }

    /// Drops the finished run at the front of the queue and hands back the one
    /// behind it, which stays queued.
    ///
    /// The queue half of upstream's `_tear_down_topology`, which moves the
    /// front out, pops it, and reads the new front under the same lock.
    ///
    /// A topology that is not the front of this queue is not in it at all, and
    /// then there is nothing to pop and nobody to hand over to: that is a
    /// [`corun`](crate::Executor::corun), whose topology is owned by the
    /// corunning worker's frame and is deliberately not queued -- a corun
    /// cannot wait for its turn, since the frame it blocks is the one that
    /// would run the graph ahead of it. So the identity check is a condition
    /// rather than an assertion.
    pub(crate) fn pop_finished_run(&self, finished: &Topology) -> Option<Arc<Topology>> {
        let mut runs = self.lock_runs();
        if !runs
            .front()
            .is_some_and(|front| std::ptr::eq(Arc::as_ptr(front), finished))
        {
            return None;
        }

        // The share this queue held is dropped here, which cannot be the last
        // one: the thread tearing the run down holds one of its own, and so
        // does the run's handle until it is dropped.
        drop(runs.pop_front());
        runs.front().map(Arc::clone)
    }

    /// The run queue. A poisoned lock is taken rather than propagated, as the
    /// executor's locks are: the queue is a `VecDeque` of shares and nothing
    /// under the lock can leave it half-updated.
    fn lock_runs(&self) -> MutexGuard<'_, VecDeque<Arc<Topology>>> {
        self.runs.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Taskflow>();
    // A job owns an `Arc<Flow>` and hands out a `&Flow` on whichever worker
    // dequeues it, so the storage has to be shareable across threads for that
    // to mean anything.
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

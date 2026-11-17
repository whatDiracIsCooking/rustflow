//! Translated from `taskflow/core/flow_builder.hpp` (Taskflow v4.1.0).
//!
//! The builder half of upstream's `class Subflow`, which is the end of that
//! header: the handle a subflow task's body is given, its two state-bit
//! queries and the constructor that resets them. A `Subflow` *is* a
//! [`FlowBuilder`] -- upstream says so by inheritance, this port by
//! implementing the trait over the subgraph -- so the whole `emplace` family
//! builds into the child graph with no second API to learn.
//!
//! `join` is not here. Joining a subflow means running its graph and waiting
//! for it, which is the nested-graph core, so it lands as a second
//! `impl Subflow` block in `src/executor/subflow.rs` (issue #50); until it
//! does, the executor refuses a subflow task rather than running one. Nothing
//! below needs the executor for anything but to say where it is running,
//! which is why the two halves split where they do.
//!
//! # The subgraph and its lock
//!
//! Upstream's handle holds a `Graph&` into the node's own
//! `Node::Subflow::subgraph` and reaches it through a `Node*` the scheduler
//! hands around. A worker here reaches its node as `&Node`, so the subgraph
//! carries its own `Mutex` (see [`crate::Handle::Subflow`]) and the handle
//! holds the guard for as long as the body runs. That cannot deadlock a body:
//! the only graph a body locks is its *own* node's, so a subflow nested
//! inside a subflow takes a different lock, and no body ever takes one
//! twice.
//!
//! # The two state bits
//!
//! `joinable()` and `retain()` read one bit each out of the node's `nstate`
//! word -- [`Node::JOINED_SUBFLOW`] and [`Node::RETAIN_SUBFLOW`] -- and
//! [`Subflow::new`] clears both, because a conditioner can send the run back
//! round to the same subflow node and the second entry must not inherit the
//! first one's answers. That reset is what upstream's
//! `unittests/test_subflows.cpp` `Subflow.BitState` asserts: it loops a
//! subflow task a hundred times through a condition task and requires every
//! entry to see `joinable()` true and `retain()` false, however the previous
//! entry left the node.

use core::fmt;
use std::sync::atomic::Ordering;
use std::sync::{Mutex, MutexGuard, TryLockError};

use crate::executor::Executor;
use crate::executor::worker::Worker;
use crate::flow_builder::FlowBuilder;
use crate::graph::{Graph, Node};

/// The graph a subflow task's body builds, as that body sees it: upstream's
/// `tf::Subflow`.
///
/// A subflow task is a task that creates tasks while it runs. The body is
/// handed one of these, emplaces whatever the run-time data calls for through
/// the [`FlowBuilder`] methods, and the executor then runs that child graph
/// before the subflow node's own successors.
///
/// The handle borrows everything it names -- the executor, the worker running
/// the body, the node the body runs on and the node's subgraph -- so it lives
/// exactly as long as the one invocation that was given it. Upstream's is a
/// stack object passed by reference for the same reason; neither is copyable,
/// and this one cannot leave the thread that made it either, because the
/// guard it holds is not [`Send`].
///
/// ```
/// use rustflow::prelude::*;
///
/// let mut taskflow = Taskflow::new();
/// let (a, c) = taskflow.emplace_n((|| {}, || {}));
/// // The body runs when the task does, so what it emplaces may depend on
/// // anything known only by then.
/// let b = taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
///     let b1 = subflow.emplace(|| {});
///     let b2 = subflow.emplace(|| {});
///     subflow.linearize([b1, b2]);
/// });
/// taskflow.linearize([a, b, c]);
///
/// assert!(taskflow.graph().node(b).handle().is_subflow());
/// ```
pub struct Subflow<'a> {
    /// The executor running the taskflow this node belongs to.
    pub(crate) executor: &'a Executor,
    /// The worker running the body, upstream's `_worker`. It is the only
    /// worker allowed to join this subflow -- a rule #50 states, and one the
    /// subgraph guard below already enforces as far as types can, since a
    /// `MutexGuard` is not [`Send`].
    pub(crate) worker: &'a Worker,
    /// The node the body is running on, upstream's `_node`. The two subflow
    /// state bits live in its `nstate` word.
    pub(crate) node: &'a Node,
    /// The node's subgraph, locked for as long as this handle lives. This is
    /// upstream's `FlowBuilder::_graph` member, which it gets by inheritance.
    pub(crate) subgraph: MutexGuard<'a, Graph>,
}

impl<'a> Subflow<'a> {
    /// Builds the handle for a subflow task running on `node`, and clears the
    /// state the last entry to that node left behind.
    ///
    /// Upstream's constructor does three things and so does this one: take the
    /// subgraph, clear `JOINED_SUBFLOW | RETAIN_SUBFLOW`, and empty the graph
    /// -- all three because "there could have iterative control flow", in
    /// upstream's words, which is to say the same node may be entered again
    /// and has to look untouched when it is.
    ///
    /// Upstream's constructor is private, with `friend class Executor` to let
    /// the scheduler reach it. Rust has no `friend`: the executor half of this
    /// type is another module, so the constructor has to be visible beyond
    /// this one, and nothing in the crate calls it before #50 gives the
    /// executor an arm that runs a subflow task. It lends out no more than the
    /// caller already holds, so being public costs nothing but a name.
    ///
    /// # Panics
    ///
    /// Panics if the subgraph is already locked, which means two workers are
    /// running one subflow node -- [`crate::Work::invoke`]'s rule, and the
    /// same report.
    #[must_use]
    pub fn new(
        executor: &'a Executor,
        worker: &'a Worker,
        node: &'a Node,
        subgraph: &'a Mutex<Graph>,
    ) -> Self {
        let mut subgraph = match subgraph.try_lock() {
            Ok(subgraph) => subgraph,
            // A body that panics poisons its node's subgraph. The graph is
            // cleared below anyway, and the panic itself is already on its way
            // to the topology, so taking the contents back is both harmless
            // and the only way not to replace that report with a lock error.
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => {
                panic!("subflow is already running: one worker per node was violated")
            }
        };

        node.nstate().fetch_and(
            !(Node::JOINED_SUBFLOW | Node::RETAIN_SUBFLOW),
            Ordering::Relaxed,
        );
        subgraph.clear();

        Self {
            executor,
            worker,
            node,
            subgraph,
        }
    }

    /// Returns `true` if this subflow has not been joined yet, upstream's
    /// `joinable()`.
    ///
    /// A fresh subflow is joinable; joining it -- explicitly, or by the
    /// implicit join the executor performs when the body returns -- is what
    /// makes it not. Both of those are #50's, so every subflow in this
    /// milestone is joinable for the whole of its body.
    #[must_use]
    pub fn joinable(&self) -> bool {
        self.node.nstate().load(Ordering::Relaxed) & Node::JOINED_SUBFLOW == 0
    }

    /// Returns `true` if this subflow's graph will survive the join,
    /// upstream's `retain()`.
    ///
    /// Default is `false`: the runtime clears a subflow's graph once it has
    /// been joined, so that a recursive subflow does not accumulate every
    /// generation of children it ever spawned. Set it to keep the graph for
    /// something that wants to look at it afterwards, a DOT dump above all.
    #[must_use]
    pub fn retain(&self) -> bool {
        self.node.nstate().load(Ordering::Relaxed) & Node::RETAIN_SUBFLOW != 0
    }

    /// Chooses whether this subflow's graph survives the join, upstream's
    /// `retain(bool)`.
    ///
    /// Upstream overloads one name for the getter and the setter, which one
    /// Rust type cannot do, so the setter is renamed -- the same split that
    /// [`crate::TaskRef::name`] and [`crate::TaskMut::name`] made of upstream's
    /// `name()` pair.
    pub fn set_retain(&mut self, retain: bool) {
        if retain {
            self.node
                .nstate()
                .fetch_or(Node::RETAIN_SUBFLOW, Ordering::Relaxed);
        } else {
            self.node
                .nstate()
                .fetch_and(!Node::RETAIN_SUBFLOW, Ordering::Relaxed);
        }
    }

    /// Returns the executor running the taskflow this subflow belongs to,
    /// upstream's `executor()`.
    #[must_use]
    pub fn executor(&self) -> &'a Executor {
        self.executor
    }

    /// Returns the worker running this subflow's body.
    ///
    /// Upstream keeps `_worker` private and reaches it through `friend class
    /// Executor`; here it is an accessor for the same reason the constructor
    /// is public. It is also what "only the worker that spawns this subflow
    /// can join it" is about -- the rule #50 inherits.
    #[must_use]
    pub fn worker(&self) -> &'a Worker {
        self.worker
    }

    /// Returns the graph this subflow is building, upstream's `graph()`.
    ///
    /// Upstream hands back a mutable `Graph&`; here the mutable form is
    /// [`FlowBuilder::graph_mut`], which is the same seam every builder method
    /// goes through.
    #[must_use]
    pub fn graph(&self) -> &Graph {
        &self.subgraph
    }
}

impl FlowBuilder for Subflow<'_> {
    /// The subgraph, through the guard this handle holds. This one method is
    /// the whole of what upstream gets by deriving `Subflow` from
    /// `FlowBuilder`.
    fn graph_mut(&mut self) -> &mut Graph {
        &mut self.subgraph
    }
}

impl fmt::Debug for Subflow<'_> {
    /// Names the node, the two state bits and how many tasks the body has
    /// emplaced so far, which is what a subflow is. The executor behind it has
    /// no small rendering -- it is a thread pool -- and a handle that printed
    /// one would be unreadable where it is most useful, inside a failing body.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subflow")
            .field("node", &self.node.name())
            .field("worker", &self.worker.id())
            .field("joinable", &self.joinable())
            .field("retain", &self.retain())
            .field("tasks", &self.subgraph.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Handle, SubflowWork, Task};
    use crate::task::TaskMut;

    /// The subgraph of a subflow node. The variant's fields are public, so
    /// this is only the `let ... else` dance these cases would repeat.
    fn subgraph_of(node: &Node) -> &Mutex<Graph> {
        let Handle::Subflow { subgraph, .. } = node.handle() else {
            panic!("the node is not a subflow node");
        };
        subgraph
    }

    /// Builds a one-node graph holding a subflow handle whose body does
    /// nothing, which is all these cases need: what is under test is the
    /// handle around the body, not the body.
    fn subflow_graph() -> (Graph, Task) {
        let mut graph = Graph::new();
        let task = graph.add_named_node(
            "spawner",
            Handle::Subflow {
                work: SubflowWork::new(|_| {}),
                subgraph: Mutex::new(Graph::new()),
            },
        );
        (graph, task)
    }

    /// Runs `body` against a subflow handle over `task`'s node, standing in
    /// for the executor arm that #50 writes.
    ///
    /// The worker is the executor's own record for worker 0, since a
    /// [`Worker`] is only ever created by an executor; which worker it is
    /// makes no difference to a handle that cannot schedule yet.
    fn with_subflow<F>(graph: &Graph, task: Task, body: F)
    where
        F: FnOnce(&mut Subflow<'_>),
    {
        let executor = Executor::new(1);
        let node = graph.node(task);
        let mut subflow = Subflow::new(
            &executor,
            &executor.shared.workers[0],
            node,
            subgraph_of(node),
        );
        body(&mut subflow);
    }

    /// Upstream's `Subflow.BitState`, on the builder alone: however the last
    /// entry left the node, a fresh handle over it reports clean state and an
    /// empty graph. The run that loops a real subflow node through a condition
    /// task is #50's.
    #[test]
    fn a_fresh_subflow_resets_the_bits_the_last_entry_left() {
        let (graph, task) = subflow_graph();

        // What the end of a previous entry looks like: joined, and asked to
        // keep its graph. The graph has a node in it for the same reason.
        let node = graph.node(task);
        node.nstate().store(
            Node::JOINED_SUBFLOW | Node::RETAIN_SUBFLOW,
            Ordering::Relaxed,
        );
        subgraph_of(node)
            .lock()
            .expect("uncontended")
            .add_node(Handle::Placeholder);

        with_subflow(&graph, task, |subflow| {
            assert!(subflow.joinable());
            assert!(!subflow.retain());
            assert!(subflow.graph().is_empty());
        });

        // And the reset is in the node's own word, not in the handle: both
        // bits are gone from the state the next entry will read.
        assert_eq!(node.nstate().load(Ordering::Relaxed), 0);
    }

    #[test]
    fn set_retain_toggles_its_own_bit_and_nothing_else() {
        let (graph, task) = subflow_graph();
        let node = graph.node(task);

        with_subflow(&graph, task, |subflow| {
            subflow.set_retain(true);
            assert!(subflow.retain());
            assert_eq!(
                node.nstate().load(Ordering::Relaxed),
                Node::RETAIN_SUBFLOW,
                "retaining a subflow may not touch another bit"
            );
            // Still joinable: the two bits are independent, which is the half
            // of upstream's reset that a setter could get wrong.
            assert!(subflow.joinable());

            subflow.set_retain(false);
            assert!(!subflow.retain());
            assert_eq!(node.nstate().load(Ordering::Relaxed), 0);
            assert!(subflow.joinable());
        });
    }

    /// `joinable` reads the node's bit rather than any state of its own, which
    /// is what lets #50's join -- in another module -- turn it off.
    #[test]
    fn joinable_follows_the_nodes_bit() {
        let (graph, task) = subflow_graph();
        let node = graph.node(task);

        with_subflow(&graph, task, |subflow| {
            assert!(subflow.joinable());
            node.nstate()
                .fetch_or(Node::JOINED_SUBFLOW, Ordering::Relaxed);
            assert!(!subflow.joinable());
            assert!(!subflow.retain());
        });
    }

    /// The acceptance bar for implementing [`FlowBuilder`]: a subflow builds
    /// into its own subgraph with the same API a taskflow uses, and the
    /// taskflow's graph is not where any of it lands.
    #[test]
    fn a_subflow_builds_into_its_subgraph_through_the_builder_api() {
        let (graph, task) = subflow_graph();

        with_subflow(&graph, task, |subflow| {
            let (b1, b2) = subflow.emplace_n((|| {}, || {}));
            let b3 = subflow.emplace(|| {});
            let b4 = subflow.placeholder();

            // `precede` is on the task handle, as upstream's `B1.precede(B3)`
            // is; the builder's own chaining method is `linearize`.
            TaskMut::new(subflow.graph_mut(), b1).precede([b3]);
            TaskMut::new(subflow.graph_mut(), b2).precede([b3]);
            subflow.linearize([b3, b4]);

            assert_eq!(subflow.graph().len(), 4);
            assert_eq!(subflow.graph().successors(b1), [b3].as_slice());
            // b2 before b1 in the predecessor half: pushing b4 as a
            // successor later swaps the first predecessor to the back, which
            // is the upstream edge-list trick `graph.rs` documents.
            assert_eq!(subflow.graph().predecessors(b3), [b2, b1].as_slice());
            assert_eq!(subflow.graph().successors(b3), [b4].as_slice());
            assert!(format!("{subflow:?}").contains("tasks: 4"));
        });

        // The subgraph outlives the handle -- it belongs to the node -- so
        // what the body built is still there for the executor to run.
        assert_eq!(
            subgraph_of(graph.node(task))
                .lock()
                .expect("uncontended")
                .len(),
            4
        );
        // And none of it went into the graph the subflow node lives in.
        assert_eq!(graph.len(), 1);
    }

    /// The seam #50 is written against, end to end: destructure the handle,
    /// build the handle over the node, hand it to the node's own body, and
    /// find afterwards that what the body built is in the node's subgraph.
    /// Everything in this case but the implicit join is what
    /// `_invoke_subflow_task` will do.
    #[test]
    fn the_nodes_own_body_builds_through_the_handle_it_is_given() {
        let mut graph = Graph::new();
        let task = graph.add_named_node(
            "spawner",
            Handle::Subflow {
                work: SubflowWork::new(|subflow: &mut Subflow<'_>| {
                    let child = subflow.emplace(|| {});
                    let grandchild = subflow.emplace_subflow(|_| {});
                    subflow.linearize([child, grandchild]);
                }),
                subgraph: Mutex::new(Graph::new()),
            },
        );

        let executor = Executor::new(1);
        let node = graph.node(task);
        let Handle::Subflow { work, subgraph } = node.handle() else {
            panic!("the node is not a subflow node");
        };
        {
            let mut subflow = Subflow::new(&executor, &executor.shared.workers[0], node, subgraph);
            work.invoke(&mut subflow);
            // The implicit join reads these two, which is why they are the
            // state the body leaves behind rather than anything the handle
            // owns.
            assert!(subflow.joinable());
            assert_eq!(subflow.graph().len(), 2);
        }

        // The guard is gone with the handle, so the subgraph can be locked
        // again -- which is what scheduling the children and clearing the
        // graph afterwards both need.
        let built = subgraph.lock().expect("uncontended");
        assert_eq!(built.len(), 2);
        assert!(built.node(Task::new(1)).handle().is_subflow());

        // A second entry to the same node starts over, body and all: nothing
        // the first entry built is still in the way.
        drop(built);
        with_subflow(&graph, task, |subflow| {
            assert!(subflow.graph().is_empty());
            work.invoke(subflow);
            assert_eq!(subflow.graph().len(), 2);
        });
    }

    #[test]
    #[should_panic(expected = "one worker per node")]
    fn a_second_handle_over_one_node_panics() {
        let (graph, task) = subflow_graph();
        with_subflow(&graph, task, |_| {
            // Re-entering from this thread is the cheap stand-in for two
            // workers racing: `try_lock` cannot tell the two apart.
            with_subflow(&graph, task, |_| {});
        });
    }
}

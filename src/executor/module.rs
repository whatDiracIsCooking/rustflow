//! Translated from `Executor::_invoke_module_task`,
//! `_invoke_adopted_module_task` and `_invoke_module_task_impl` in
//! `taskflow/core/executor.hpp` (Taskflow v4.1.0).
//!
//! A module task stands for another whole graph: running the node runs that
//! graph, and the node's successors wait for all of it.
//!
//! # Design
//!
//! Two handles, two ways to reach the target graph -- a share of a
//! [`Taskflow`] for [`Handle::Module`](crate::Handle::Module), the path
//! `Job::graph_inside` builds for
//! [`Handle::AdoptedModule`](crate::Handle::AdoptedModule) -- and one refusal,
//! [`CYCLE`], for a target this job is already inside of. `DESIGN.md` has both
//! decisions, the second of them with upstream's `composed_of` obligation that
//! a target is not run by two threads at once. The rest is the executor's own
//! machinery, in the [`executor`](crate::executor) module docs: the preemption
//! protocol the two entries below are, the sourceless graph upstream's
//! `graph.empty()` early return widens to, and the anchor a failing task of
//! the target reports to.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::executor::topology::{GraphOwner, Job, Nested};
use crate::executor::worker::{Release, WorkerThread};
use crate::graph::{Graph, Node};
use crate::taskflow::Taskflow;

/// What a module task composed of a graph its own run is inside of reports
/// instead of hanging it: a `&'static str`, as `panic!` with a literal
/// produces and [`RunError`](crate::RunError) renders first.
const CYCLE: &str = "a module task may not run a graph it is already \
     inside of: a taskflow composed of itself, directly or through a cycle \
     of compositions, has no execution order";

/// Runs the module task `job` is for, against the taskflow it is composed of:
/// upstream's `_invoke_module_task`, reaching as [`Taskflow::flow`] what
/// upstream reaches as `std::get_if<Node::Module>(&_handle)->graph`. `None`
/// means the node has been preempted -- its target graph is running and the
/// last job of that graph resumes it -- which keeps `WorkerThread::invoke`
/// from releasing successors with a whole graph still to wait for.
pub(crate) fn invoke(worker: &WorkerThread, job: &Job, target: &Arc<Taskflow>) -> Option<Release> {
    invoke_impl(worker, job, &GraphOwner::flow(target.flow()))
}

/// Runs the adopted module task `job` is for, against the graph its own handle
/// owns: upstream's `_invoke_adopted_module_task`, reaching as the path
/// `Job::graph_inside` builds what upstream reaches as
/// `std::get_if<Node::AdoptedModule>(&_handle)->graph`. `target` is taken for
/// the assertion rather than for the run: the path has to resolve to exactly
/// the graph the dispatch arm already had in hand.
pub(crate) fn invoke_adopted(worker: &WorkerThread, job: &Job, target: &Graph) -> Option<Release> {
    // `None`: an adopted graph is read where it lies, needing no lease.
    let owner = job.graph_inside(None);
    debug_assert!(
        std::ptr::eq(owner.graph(), target),
        "an adopted module's path resolves to the graph its own handle owns"
    );

    invoke_impl(worker, job, &owner)
}

/// Upstream's `_invoke_module_task_impl`, over whatever owns the target graph
/// rather than over a `Graph&`. The three answers are upstream's three, in its
/// order: nothing to do, the first entry, the second entry.
fn invoke_impl(worker: &WorkerThread, job: &Job, target: &GraphOwner) -> Option<Release> {
    // Upstream's `if(graph.empty()) return false`, widened: a sourceless graph
    // would queue no job, so nothing would drive this node's counter off zero.
    if !target.has_source_task() {
        return Some(Release::Strong);
    }

    let node = job.node();

    // Upstream's `if((node->_nstate & NSTATE::PREEMPTED) == 0)`: the first
    // entry, which leaves the node owing its successors. The refusal is ours.
    if node.nstate().load(Ordering::Relaxed) & Node::PREEMPTED == 0 {
        if job.runs_inside(target.graph()) {
            job.record_panic(Box::new(CYCLE));
            return Some(Release::Strong);
        }

        // Set before the graph is scheduled: a child may finish before this
        // call returns, and the bit is what tells the last one to re-queue
        // this node. Relaxed, as upstream's plain `|=` is -- a child's read of
        // it is ordered by the queue the child came out of.
        node.nstate().fetch_or(Node::PREEMPTED, Ordering::Relaxed);

        // Upstream's `_schedule_graph(w, graph, node->_topology, node)`,
        // whose `node` is a `Nested` here: what the last job of the target
        // graph re-queues is the *job* this node was dequeued as.
        let parent = Nested::new(Some(job.clone()));
        worker.shared.schedule_graph(
            Some(worker),
            target,
            job.topology_share(),
            &parent.as_parent(),
        );
        return None;
    }

    // Upstream's `node->_nstate &= ~NSTATE::PREEMPTED`: the second entry.
    node.nstate().fetch_and(!Node::PREEMPTED, Ordering::Relaxed);
    Some(Release::Strong)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
    use std::sync::{Arc, OnceLock, Weak};

    use crate::Executor;
    use crate::graph::Node;
    use crate::prelude::*;

    /// A taskflow of `tasks` tasks in a chain, each bumping `counter`.
    fn chain(tasks: usize, counter: &Arc<AtomicUsize>) -> Taskflow {
        let mut taskflow = Taskflow::new();
        let tasks: Vec<Task> = (0..tasks)
            .map(|_| {
                let counter = Arc::clone(counter);
                taskflow.emplace(move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                })
            })
            .collect();
        taskflow.linearize(tasks.iter().copied());
        taskflow
    }

    /// The target graph runs before the module node's successors do, which is
    /// the whole point of the preemption: the successor sees every task of the
    /// target already counted.
    #[test]
    fn a_module_runs_its_whole_target_before_its_successors() {
        for workers in 1..=3 {
            let counter = Arc::new(AtomicUsize::new(0));
            let target = Arc::new(chain(5, &counter));

            let seen = Arc::new(AtomicUsize::new(usize::MAX));
            let mut taskflow = Taskflow::new();
            let module = taskflow.composed_of(Arc::clone(&target));
            let after = taskflow.emplace({
                let counter = Arc::clone(&counter);
                let seen = Arc::clone(&seen);
                move || seen.store(counter.load(Ordering::Relaxed), Ordering::Relaxed)
            });
            taskflow.task(module).precede([after]);

            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");

            assert_eq!(counter.load(Ordering::Relaxed), 5, "{workers} worker(s)");
            assert_eq!(
                seen.load(Ordering::Relaxed),
                5,
                "{workers} worker(s): the successor ran before the target was done"
            );
        }
    }

    /// The `PREEMPTED` protocol from the inside: the bit is set while the
    /// target graph runs and cleared by the second entry, so the node is left
    /// carrying nothing but its own dependency count.
    ///
    /// The only place the bit can be seen set is a task of the target graph,
    /// and reaching the composing taskflow from there takes a slot filled in
    /// after both are built -- a `Weak`, so that the two do not hold each
    /// other alive.
    #[test]
    fn a_module_node_is_preempted_while_its_target_runs() {
        let observed = Arc::new(AtomicU32::new(u32::MAX));
        let composing: Arc<OnceLock<(Weak<Taskflow>, Task)>> = Arc::default();

        let mut target = Taskflow::new();
        target.emplace({
            let composing = Arc::clone(&composing);
            let observed = Arc::clone(&observed);
            move || {
                let (taskflow, module) = composing.get().expect("filled before the run");
                let taskflow = taskflow.upgrade().expect("alive for the whole run");
                observed.store(
                    taskflow
                        .graph()
                        .node(*module)
                        .nstate()
                        .load(Ordering::Relaxed),
                    Ordering::Relaxed,
                );
            }
        });

        let mut taskflow = Taskflow::new();
        let module = taskflow.composed_of(Arc::new(target));
        let taskflow = Arc::new(taskflow);
        composing
            .set((Arc::downgrade(&taskflow), module))
            .expect("the slot is filled once");

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(
            observed.load(Ordering::Relaxed) & Node::PREEMPTED,
            Node::PREEMPTED,
            "the module node is preempted while its target graph runs"
        );
        assert_eq!(
            taskflow
                .graph()
                .node(module)
                .nstate()
                .load(Ordering::Relaxed),
            0,
            "the second entry clears the bit it set"
        );
    }

    /// An empty target is nothing to do: the node releases its successors like
    /// a static task, and never takes the `PREEMPTED` bit -- which is what
    /// keeps a module of an empty taskflow from hanging the run.
    ///
    /// The sourceless case is the same claim over this port's widening of
    /// upstream's `graph.empty()`: a graph whose every node is in a cycle has
    /// nothing to start at either.
    #[test]
    fn an_empty_module_releases_its_successors_without_preempting() {
        for (what, target) in [("empty", empty_target()), ("sourceless", cyclic_target())] {
            for workers in 1..=3 {
                let context = format!("{what} target, {workers} worker(s)");
                let counter = Arc::new(AtomicUsize::new(0));
                let mut taskflow = Taskflow::new();
                let module = taskflow.composed_of(Arc::clone(&target));
                let after = {
                    let counter = Arc::clone(&counter);
                    taskflow.emplace(move || {
                        counter.fetch_add(1, Ordering::Relaxed);
                    })
                };
                taskflow.task(module).precede([after]);

                Executor::new(workers)
                    .run(&taskflow)
                    .wait()
                    .unwrap_or_else(|error| panic!("{context}: {error}"));

                assert_eq!(
                    counter.load(Ordering::Relaxed),
                    1,
                    "{context}: the successor of an empty module runs"
                );
                assert_eq!(
                    taskflow
                        .graph()
                        .node(module)
                        .nstate()
                        .load(Ordering::Relaxed),
                    0,
                    "{context}: the state word is a source's count and no bits"
                );
            }
        }
    }

    /// A target with no tasks at all.
    fn empty_target() -> Arc<Taskflow> {
        Arc::new(Taskflow::new())
    }

    /// A target with tasks and nowhere to start: two tasks preceding each
    /// other.
    fn cyclic_target() -> Arc<Taskflow> {
        let mut taskflow = Taskflow::new();
        let (a, b) = taskflow.emplace_n((|| {}, || {}));
        taskflow.task(a).precede([b]);
        taskflow.task(b).precede([a]);
        Arc::new(taskflow)
    }

    /// An adopted graph runs the way a composed taskflow's does -- the whole
    /// of it before the module node's successors -- which is what #74
    /// replaced the refusal with. The same claim as
    /// `a_module_runs_its_whole_target_before_its_successors`, over the
    /// handle that owns its graph instead of sharing one.
    #[test]
    fn an_adopted_module_runs_its_whole_graph_before_its_successors() {
        for workers in 1..=3 {
            let counter = Arc::new(AtomicUsize::new(0));
            let seen = Arc::new(AtomicUsize::new(usize::MAX));

            let mut taskflow = Taskflow::new();
            let module = taskflow.adopt(adopted_chain(5, &counter));
            let after = taskflow.emplace({
                let counter = Arc::clone(&counter);
                let seen = Arc::clone(&seen);
                move || seen.store(counter.load(Ordering::Relaxed), Ordering::Relaxed)
            });
            taskflow.task(module).precede([after]);

            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");

            assert_eq!(counter.load(Ordering::Relaxed), 5, "{workers} worker(s)");
            assert_eq!(
                seen.load(Ordering::Relaxed),
                5,
                "{workers} worker(s): the successor ran before the graph was done"
            );
        }
    }

    /// The adopted graph's tasks are resolved in *its* arena and not in the
    /// composing graph's, which is the defect the refusal stood in for: both
    /// arenas number from zero, so a job built over the outer owner would run
    /// whatever the composing graph has at those indices -- starting with the
    /// module node itself -- and would say nothing about it.
    ///
    /// The shape is the collision: three tasks in the adopted graph at
    /// indices 0, 1 and 2, and three decoys in the composing graph at the
    /// same three indices, each counting somewhere else. Every decoy runs
    /// exactly once because it is a task of the run, and the adopted graph's
    /// own three are what the module node runs.
    #[test]
    fn an_adopted_module_resolves_its_tasks_in_its_own_graph() {
        let inside = Arc::new(AtomicUsize::new(0));
        let outside = Arc::new(AtomicUsize::new(0));

        let mut taskflow = Taskflow::new();
        // Index 0 of the composing graph is the module node itself, which a
        // resolution against the wrong arena would re-enter.
        let module = taskflow.adopt(fan_of(3, &inside));
        for _ in 0..3 {
            let outside = Arc::clone(&outside);
            taskflow.emplace(move || {
                outside.fetch_add(1, Ordering::Relaxed);
            });
        }

        assert_eq!(module, Task::new(0), "the module node is index 0 of both");

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(
            inside.load(Ordering::Relaxed),
            3,
            "the adopted graph's own three tasks ran"
        );
        assert_eq!(
            outside.load(Ordering::Relaxed),
            3,
            "each decoy ran once, as the task of the run that it is"
        );
    }

    /// A graph of `tasks` tasks in a chain, each bumping `counter`: the
    /// adopted form of [`chain`], built through the arena because
    /// `FlowBuilder` is implemented for the types that *own* a graph and an
    /// adopted one is owned by the node it is moved into.
    fn adopted_chain(tasks: usize, counter: &Arc<AtomicUsize>) -> Graph {
        let mut graph = Graph::new();
        let tasks: Vec<Task> = (0..tasks)
            .map(|_| {
                let counter = Arc::clone(counter);
                graph.add_node(Handle::Static(Work::new(move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                })))
            })
            .collect();
        for pair in tasks.windows(2) {
            graph.add_edge(pair[0], pair[1]);
        }
        graph
    }

    /// A graph of `tasks` independent tasks, each bumping `counter`, so that
    /// every one of them is a source and the indices are 0..tasks.
    fn fan_of(tasks: usize, counter: &Arc<AtomicUsize>) -> Graph {
        let mut graph = Graph::new();
        for _ in 0..tasks {
            let counter = Arc::clone(counter);
            graph.add_node(Handle::Static(Work::new(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            })));
        }
        graph
    }
}

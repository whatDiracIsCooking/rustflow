//! Translated from `Executor::_invoke_module_task`,
//! `_invoke_adopted_module_task` and `_invoke_module_task_impl` in
//! `taskflow/core/executor.hpp` (Taskflow v4.1.0).
//!
//! A module task stands for another whole graph: running the node runs that
//! graph, and the node's successors wait for all of it. Upstream's three
//! functions are two one-line forwarders onto one `_impl` over a `Graph&`,
//! and the `_impl` is the whole of the semantics -- an empty target is
//! nothing to do, and a non-empty one preempts the node and schedules the
//! target graph under it.
//!
//! # Why the two handles are not one line each here
//!
//! The handles differ only in who owns the target graph, which upstream can
//! forget about the moment it has a `Graph&`. This port cannot: the jobs of a
//! graph own what they name, so scheduling one needs whatever *owns* the
//! graph rather than a reference into it -- `Shared::schedule_graph` takes a
//! `GraphOwner`, and `Job::graph` resolves an index against it.
//!
//! [`Handle::Module`](crate::Handle::Module) owns a share: its target is an
//! `Arc<Taskflow>`, whose storage is exactly what a run of that taskflow
//! would be scheduled over, so [`invoke`] is upstream's forwarder with
//! `Taskflow::flow` where upstream writes `.graph`.
//! [`Handle::AdoptedModule`](crate::Handle::AdoptedModule) owns the graph
//! itself: it sits inline in the handle, inside the arena of the graph the
//! module node is in, so the only thing that names it is the *outer* owner
//! plus that node's own index as a path to follow. `Job::graph_inside` is
//! that path, which is issue #74 -- the widening of a job's graph owner to
//! all four kinds of nested graph at once, rather than the module, subflow
//! and runtime arms each reshaping the field in turn -- and [`invoke_adopted`]
//! is a one-line forwarder over it, as upstream's is over its `Graph&`.
//!
//! What the path has to be right about is which arena an index means. A job
//! of an adopted graph built over the *outer* owner would resolve its tasks
//! against the composing taskflow's own nodes -- silently, against whatever
//! sits at those indices -- which is why #49 refused the run rather than
//! running one that way, and it is what
//! `an_adopted_module_resolves_its_tasks_in_its_own_graph` below pins.
//!
//! # What running the target costs the node
//!
//! Upstream's protocol, which #47 ported and which this module is the first
//! caller of. A module node is entered twice:
//!
//! - the **first** entry sets `NSTATE::PREEMPTED` on the node and schedules
//!   the target graph under it. The node has not finished -- its join counter
//!   is not re-armed and its successors are not released -- so the arm
//!   answers `None` and `WorkerThread::invoke` returns without either;
//! - the target graph's jobs count down the module node rather than the run,
//!   and the last of them to finish finds the bit set and re-queues the job
//!   the node was dequeued as;
//! - the **second** entry clears the bit and answers [`Release::Strong`], so
//!   the node finishes exactly as a static task does, with every successor
//!   released once the whole target graph is done.
//!
//! The bit is set before the graph is scheduled because a child may finish
//! before this call returns, and the child's read of it is ordered by the
//! queue it came out of rather than by the bit's own ordering -- which is why
//! both can be `Relaxed`, as upstream's plain `|=` is.
//!
//! # The empty target, widened by one case
//!
//! Upstream returns early on `graph.empty()`. This port asks
//! `GraphOwner::has_source_task` instead -- `Flow::has_source_task` over
//! whichever kind of owner the target is -- which is the same question
//! widened to the graph that has nothing to *start* at: an empty one, or one
//! whose every node is in a cycle. The widening is not cosmetic here. Scheduling a
//! sourceless graph queues no job at all, so nothing would ever drive the
//! module node's join counter off zero, and a node left `PREEMPTED` with no
//! child to resume it hangs the run. [`Executor::run`](crate::Executor::run)
//! and `corun_graph` ask the same question for the same reason.
//!
//! # A failure inside a module, and cancelling the run
//!
//! Neither needs a line of code here, and both are worth saying out loud
//! because that is a claim about #47's and #48's machinery rather than an
//! absence.
//!
//! A task of the target graph that panics reports through
//! [`Job::record_panic`], whose walk goes up the parent chain from the
//! failing node. A module node is no anchor -- it preempted itself, it did
//! not undertake to look at a payload slot -- so the walk marks it on the way
//! past and carries the payload to the first ancestor that is anchored, which
//! for a composed taskflow is the run. So the failure comes out of the
//! composing run's `wait`, as upstream's comes out of its `get()`, and the
//! marks the walk left behind are what skip the rest of each graph the
//! failure was inside of. That is upstream's `Exception.ModuleTask`, and it
//! is ported in `tests/exceptions.rs`.
//!
//! Cancelling the run skips the module's remaining tasks through the same
//! early-out as any other task's: every job of the target graph holds a share
//! of the *run*'s topology, so `Job::is_parent_cancelled` answers yes for all
//! of them on the first term alone, without the chain walk being needed. A
//! cancelled module is still entered twice, because the early-out tears the
//! job down rather than dropping it: the children that were already queued
//! are skipped one by one, the last of them resumes the node, and the second
//! entry releases successors that are themselves skipped. Nothing is left
//! outstanding, which is what upstream's `CancelComposition` -- a hundred
//! cancelled runs of a four-level composition, asserting only that they all
//! finish -- is really checking.
//!
//! # One target, one run at a time
//!
//! The obligation upstream states for `composed_of` ("thread-safe as long as
//! the target is not concurrently ran by two or more threads") survives
//! translation unchanged, because what makes it an obligation is the graph's
//! per-node state rather than anything about C++: a join counter and a state
//! word belong to one run at a time, and two runs walking one graph would
//! spend each other's counters.
//!
//! Sharing the target as an `Arc<Taskflow>` buys the caller the *lifetime*
//! half of upstream's rule and not this half. What it does change is the
//! failure: two workers reaching one node find the `try_lock` in
//! [`Work::invoke`](crate::Work::invoke) already taken and panic with "work
//! is already running: one worker per node was violated", which is reported
//! as the run's failure like any other panic. A silent race on a counter
//! would be the alternative, and it is the worse one.
//!
//! Two shapes that look like the forbidden one are not, which is why
//! upstream's own tests queue a hundred runs of a composition without a wait
//! between them:
//!
//! - **runs of one taskflow serialise.** Every run of a taskflow is queued on
//!   that taskflow's own storage (#15) and started by the tear-down of the
//!   one before it, so a hundred `run`s of a composing taskflow are a hundred
//!   runs in a row -- and therefore a hundred runs in a row of each target
//!   below it;
//! - **two module tasks, one target, ordered.** A target composed twice into
//!   one taskflow is one graph run twice in that taskflow's run, which is
//!   fine as long as the two module nodes cannot overlap. Upstream's
//!   `Module1`, `Module2` and `Module3` all order theirs with an edge, and
//!   `tests/modules.rs` says so where it ports them.
//!
//! What is left is the real error, and it is the caller's: composing one
//! target into two module tasks that can overlap, or running a taskflow
//! directly while a composition of it is in flight. Neither is something this
//! port can refuse without a lock on every run of every graph, and the most
//! it can do with an obligation no type here expresses is to make breaking it
//! report itself -- which the `try_lock` above does.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::executor::topology::{GraphOwner, Job, Nested};
use crate::executor::worker::{Release, WorkerThread};
use crate::graph::{Graph, Node};
use crate::taskflow::Taskflow;

/// Runs the module task `job` is for, against the taskflow it is composed of:
/// upstream's `_invoke_module_task`.
///
/// `None` means the node has been preempted -- its target graph is running,
/// and the last job of that graph will resume it -- and is what keeps
/// `WorkerThread::invoke` from releasing successors that have a whole graph
/// still to wait for.
///
/// Upstream reaches the target as `std::get_if<Node::Module>(&_handle)->graph`
/// and this reaches it as [`Taskflow::flow`], which is the same graph plus the
/// share that owns it; the module docs say why the share is what the
/// scheduling path needs.
pub(crate) fn invoke(worker: &WorkerThread, job: &Job, target: &Arc<Taskflow>) -> Option<Release> {
    invoke_impl(worker, job, &GraphOwner::flow(target.flow()))
}

/// Runs the adopted module task `job` is for, against the graph its own
/// handle owns: upstream's `_invoke_adopted_module_task`.
///
/// Upstream reaches the target as
/// `std::get_if<Node::AdoptedModule>(&_handle)->graph` and this reaches it as
/// the path `Job::graph_inside` builds -- this job's own owner plus this
/// job's task index -- because a graph the handle owns inline is owned by
/// nothing else. The module docs say why that is the whole difference between
/// this forwarder and [`invoke`], and the refusal it replaces (#47's stub,
/// kept by #49) is gone.
///
/// `target` is the graph the dispatch arm already had in hand, and it is
/// taken as an argument for the assertion below rather than for the run: the
/// path has to resolve to exactly that graph, and a port of a pointer walk
/// should say so somewhere other than in prose. The check is a
/// `debug_assert`, so the release build pays the one `Arc` the path costs and
/// nothing else.
pub(crate) fn invoke_adopted(worker: &WorkerThread, job: &Job, target: &Graph) -> Option<Release> {
    // `None`: an adopted graph is read where it lies, which is the half of
    // #74's widening that needs no lease -- see `executor::topology` for the
    // half that does.
    let owner = job.graph_inside(None);
    debug_assert!(
        std::ptr::eq(owner.graph(), target),
        "an adopted module's path resolves to the graph its own handle owns"
    );

    invoke_impl(worker, job, &owner)
}

/// Upstream's `_invoke_module_task_impl`, over whatever owns the target graph
/// rather than over a `Graph&`.
///
/// The three answers are upstream's three, in its order: nothing to do, the
/// first entry, the second entry.
fn invoke_impl(worker: &WorkerThread, job: &Job, target: &GraphOwner) -> Option<Release> {
    // Upstream's `if(graph.empty()) return false`, widened to the graph with
    // nothing to start at -- see the module docs, which say why scheduling
    // one instead would hang the run rather than merely waste a walk.
    if !target.has_source_task() {
        return Some(Release::Strong);
    }

    let node = job.node();

    // Upstream's `if((node->_nstate & NSTATE::PREEMPTED) == 0)`: the first
    // entry, which starts the target graph and leaves the node owing its
    // successors until that graph is done.
    if node.nstate().load(Ordering::Relaxed) & Node::PREEMPTED == 0 {
        // Before the graph is scheduled, because a child may finish before
        // this call returns and the bit is what tells the last one to re-queue
        // this node.
        node.nstate().fetch_or(Node::PREEMPTED, Ordering::Relaxed);

        // Upstream's `_schedule_graph(w, graph, node->_topology, node)`, whose
        // `node` argument is a `Nested` here: the target graph's jobs count
        // that node's join counter down, and what the last of them re-queues
        // is the *job* the node was dequeued as rather than the node itself.
        // That is #47's "the parent travels with the job", and it is why the
        // run has to be passed as a share -- a nested job owns one of its own.
        let parent = Nested::new(Some(job.clone()));
        worker.shared.schedule_graph(
            Some(worker),
            target,
            job.topology_share(),
            &parent.as_parent(),
        );
        return None;
    }

    // Upstream's `node->_nstate &= ~NSTATE::PREEMPTED`: the second entry, with
    // the target graph finished and this node free to finish too.
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

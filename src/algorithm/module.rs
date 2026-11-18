//! Translated from `Algorithm::make_module_task` and `make_module_task` in
//! `taskflow/algorithm/module.hpp` (Taskflow v4.1.0).
//!
//! A module task with no handle of its own: a runtime-shaped body that
//! stands for a whole [`Taskflow`], for the callers that have a [`Runtime`]
//! to hand it to but no graph to build one into -- an async task, a
//! [`TaskGroup`](crate::TaskGroup), or
//! [`FlowBuilder::emplace_runtime`](crate::FlowBuilder::emplace_runtime)
//! itself.
//!
//! # Design
//!
//! Upstream templates over any `GraphLike T`; this port has exactly one
//! owned, shareable graph type, so [`make_module_task`] is over
//! [`Arc<Taskflow>`](Taskflow) rather than over a trait, the same choice
//! [`FlowBuilder::composed_of`](crate::FlowBuilder::composed_of) makes and
//! for the reason `DESIGN.md`'s "A module task reaches its target through
//! whatever owns it" gives.
//!
//! The body is the one line upstream's lambda is -- an empty-graph check,
//! then `Shared::schedule_graph` -- with none of `executor::module`'s own
//! `PREEMPTED`/`CYCLE` handling, and that is not an omission: those exist
//! because `Handle::Module`'s first and second entries *are* the whole of
//! how a module node is invoked, where here the node is a runtime task, and
//! `executor::runtime`'s `enter` has already set the bits, taken the node's
//! own reference and will clear them again once the scheduled graph's last
//! job re-queues this one. The sourceless case upstream's `graph.empty()` is
//! widened to, as `executor::module`'s is, for the same reason: a graph with
//! nothing to start at would queue no job, and this node has nothing else
//! that would ever be re-queued.
//!
//! What upstream's obligation for `composed_of` -- "thread-safe as long as
//! the target is not concurrently ran by two or more threads" -- means here
//! is unchanged from there too, and deliberately not re-derived: a body that
//! calls [`make_module_task`] on a target already running under another
//! module task, or under a second call of its own, shares upstream's answer
//! of a caller error rather than `executor::module`'s `CYCLE` refusal, which
//! is specific to a node reachable by walking `Handle::Module`'s own parent
//! chain -- a chain a free-standing runtime task is not on.

use std::sync::Arc;

use crate::executor::topology::{GraphOwner, Nested};
use crate::runtime::Runtime;
use crate::taskflow::Taskflow;

/// Builds a runtime-shaped body that runs the whole of `target`'s graph:
/// upstream's `make_module_task` and `Algorithm::make_module_task`.
///
/// The body schedules `target`'s graph under the node and the run of
/// whichever [`Runtime`] it is handed -- a node of a taskflow's own graph via
/// [`FlowBuilder::emplace_runtime`](crate::FlowBuilder::emplace_runtime), or a
/// free-standing node via
/// [`Executor::spawn_runtime`](crate::Executor::spawn_runtime),
/// [`Executor::silent_async_runtime`](crate::Executor::silent_async_runtime),
/// [`Executor::dependent_async_runtime`](crate::Executor::dependent_async_runtime)
/// or a [`TaskGroup`](crate::TaskGroup)'s own async family -- so its
/// successors or its handle wait for the whole composition, the same promise
/// [`FlowBuilder::composed_of`](crate::FlowBuilder::composed_of) makes for a
/// node built into a graph. An empty or sourceless `target` is a no-op,
/// upstream's `graph.empty()` early return.
///
/// `FnMut` rather than `FnOnce`, because the body may run more than once --
/// a node of a taskflow run with [`Executor::run_n`](crate::Executor::run_n),
/// not merely submitted once. Calling this on a `target` that is already
/// running is a caller error with no refusal of its own, which the module
/// docs' `# Design` section says more about.
///
/// ```
/// use std::sync::Arc;
///
/// use rustflow::algorithm::module::make_module_task;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// // Upstream's own example: four taskflows launched in parallel, each a
/// // module task of its own rather than a node built into one graph.
/// let executor = Executor::new(4);
///
/// let mut a = Taskflow::new();
/// a.emplace(|| println!("Taskflow A"));
/// let mut b = Taskflow::new();
/// b.emplace(|| println!("Taskflow B"));
/// let mut c = Taskflow::new();
/// c.emplace(|| println!("Taskflow C"));
/// let mut d = Taskflow::new();
/// d.emplace(|| println!("Taskflow D"));
///
/// executor.silent_async_runtime(make_module_task(Arc::new(a)));
/// executor.silent_async_runtime(make_module_task(Arc::new(b)));
/// executor.silent_async_runtime(make_module_task(Arc::new(c)));
/// executor.silent_async_runtime(make_module_task(Arc::new(d)));
/// executor.wait_for_all();
/// ```
pub fn make_module_task(
    target: Arc<Taskflow>,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static {
    move |runtime: &mut Runtime<'_>| {
        let owner = GraphOwner::flow(target.flow());
        if !owner.has_source_task() {
            return;
        }

        // Upstream's `rt._executor._schedule_graph(rt._worker, graph,
        // rt._node->_topology, rt._node)`: a fresh `Nested` over this job, so
        // the last job of `target`'s graph re-queues the job this body is
        // running as.
        let parent = Nested::new(Some(runtime.job.clone()));
        let thread = &*runtime.thread;
        thread.shared.schedule_graph(
            Some(thread),
            &owner,
            runtime.job.topology_share(),
            &parent.as_parent(),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::make_module_task;
    use crate::Executor;
    use crate::prelude::*;

    /// A taskflow of `tasks` tasks in a chain, each bumping `counter`: the
    /// same shape `executor::module`'s own cases build their targets with.
    fn chain(tasks: usize, counter: &Arc<AtomicUsize>) -> Taskflow {
        let mut taskflow = Taskflow::new();
        let nodes: Vec<Task> = (0..tasks)
            .map(|_| {
                let counter = Arc::clone(counter);
                taskflow.emplace(move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                })
            })
            .collect();
        taskflow.linearize(nodes.iter().copied());
        taskflow
    }

    /// The body runs the whole of its target from a graph's own runtime task,
    /// so the task's successor sees every task of the target already
    /// counted: the same claim `executor::module`'s
    /// `a_module_runs_its_whole_target_before_its_successors` makes for a
    /// `Handle::Module` node, here over the runtime-shaped body instead.
    #[test]
    fn a_module_task_runs_its_whole_target_before_its_successors() {
        let counter = Arc::new(AtomicUsize::new(0));
        let target = Arc::new(chain(5, &counter));

        let seen = Arc::new(AtomicUsize::new(usize::MAX));
        let mut taskflow = Taskflow::new();
        let module = taskflow.emplace_runtime(make_module_task(target));
        let after = taskflow.emplace({
            let counter = Arc::clone(&counter);
            let seen = Arc::clone(&seen);
            move || seen.store(counter.load(Ordering::Relaxed), Ordering::Relaxed)
        });
        taskflow.task(module).precede([after]);

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(counter.load(Ordering::Relaxed), 5);
        assert_eq!(
            seen.load(Ordering::Relaxed),
            5,
            "the successor ran before the target was done"
        );
    }

    /// A node of a taskflow run more than once runs the body again each
    /// time, which is what the body being `FnMut` buys over upstream's
    /// one-shot closure semantics meaning nothing different here.
    #[test]
    fn a_module_task_runs_again_on_a_second_run() {
        let counter = Arc::new(AtomicUsize::new(0));
        let target = Arc::new(chain(3, &counter));

        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(make_module_task(target));

        let executor = Executor::new(2);
        executor
            .run_n(&taskflow, 4)
            .wait()
            .expect("no task panicked");

        assert_eq!(counter.load(Ordering::Relaxed), 12, "four runs of three");
    }

    /// An empty target is a no-op: the node finishes like a static task and
    /// never schedules anything, which is what keeps a module task of an
    /// empty taskflow from hanging the run.
    #[test]
    fn an_empty_target_is_a_no_op() {
        let mut taskflow = Taskflow::new();
        let module = taskflow.emplace_runtime(make_module_task(Arc::new(Taskflow::new())));
        let ran = Arc::new(AtomicUsize::new(0));
        let after = {
            let ran = Arc::clone(&ran);
            taskflow.emplace(move || {
                ran.fetch_add(1, Ordering::Relaxed);
            })
        };
        taskflow.task(module).precede([after]);

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(ran.load(Ordering::Relaxed), 1, "the successor still ran");
    }

    /// A sourceless target -- this port's widening of upstream's
    /// `graph.empty()`, over a graph with tasks and nowhere to start -- is
    /// the same no-op.
    #[test]
    fn a_sourceless_target_is_also_a_no_op() {
        let mut target = Taskflow::new();
        let (a, b) = target.emplace_n((|| {}, || {}));
        target.task(a).precede([b]);
        target.task(b).precede([a]);

        let mut taskflow = Taskflow::new();
        let module = taskflow.emplace_runtime(make_module_task(Arc::new(target)));
        let ran = Arc::new(AtomicUsize::new(0));
        let after = {
            let ran = Arc::clone(&ran);
            taskflow.emplace(move || {
                ran.fetch_add(1, Ordering::Relaxed);
            })
        };
        taskflow.task(module).precede([after]);

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(ran.load(Ordering::Relaxed), 1);
    }

    /// The body is also a plain runtime-shaped closure, so it runs the same
    /// way from a free-standing async task with no graph around it at all:
    /// upstream's `Module.AsyncLaunch`.
    #[test]
    fn a_module_task_runs_from_an_async_body() {
        let counter = Arc::new(AtomicUsize::new(0));
        let target = Arc::new(chain(5, &counter));

        let executor = Executor::new(2);
        executor
            .spawn_runtime(make_module_task(target))
            .wait()
            .expect("no task panicked");

        assert_eq!(counter.load(Ordering::Relaxed), 5);
    }

    /// And from a dependent-async task, waited for through the pair's own
    /// handle: upstream's `Module.DependentAsyncLaunch`.
    #[test]
    fn a_module_task_runs_from_a_dependent_async_body() {
        let counter = Arc::new(AtomicUsize::new(0));
        let target = Arc::new(chain(5, &counter));

        let executor = Executor::new(2);
        let (_task, done) = executor.dependent_async_runtime(make_module_task(target), []);
        done.wait().expect("no task panicked");

        assert_eq!(counter.load(Ordering::Relaxed), 5);
    }
}

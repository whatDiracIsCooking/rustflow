//! Translated from `taskflow/core/runtime.hpp` (Taskflow v4.1.0).
//!
//! Only the shell of upstream's `tf::Runtime` is here: what the handle holds,
//! and the two things it can answer without reaching into the scheduler.
//! `schedule`, `corun` and `is_cancelled` need the executor and live in a
//! second `impl Runtime` block in `src/executor/runtime.rs`, an inherent
//! `impl` being able to sit anywhere in the defining crate.
//!
//! # Design
//!
//! There is no `executor()`; `DESIGN.md` has why a runtime or subflow handle
//! cannot reproduce upstream's member, and [`Runtime::num_workers`] stands
//! in its place.
//!
//! Which nodes are handed one is `executor::runtime`, and the eight forms a
//! body has for spawning work under its own node are `executor::runtime_async`.
//!
//! Upstream's second handle, `NonpreemptiveRuntime`, is ported in full; it
//! lives whole in `executor::runtime`, with no shell here to split it from.
//! The node kind that is handed one is built by
//! [`FlowBuilder::emplace_nonpreemptive_runtime`](crate::FlowBuilder::emplace_nonpreemptive_runtime),
//! a public method whose closure meets the handle by inference, so what that
//! crate-private module withholds is the type's *name* rather than the handle
//! itself; its `# Design` section has why the keyword on the type is `pub`
//! all the same.

use core::fmt;

use crate::executor::topology::Job;
use crate::executor::worker::{Worker, WorkerThread};
use crate::graph::Node;

/// The scheduler, as a runtime task's body sees it: upstream's `tf::Runtime`.
///
/// A runtime task is a task handed the thing that is running it, which is
/// what lets a body schedule work of its own and wait for it. The handle
/// borrows everything it names, so it lives exactly as long as the one
/// invocation and is not meant to be stored -- upstream's is a stack object
/// its body is handed by reference, for the same reason. What this module
/// answers is where the body is running.
pub struct Runtime<'a> {
    /// The worker running the body, upstream's `_worker`.
    pub(crate) worker: &'a Worker,
    /// The node the body is running on, upstream's `_node`: the anchor a
    /// `corun` waits on and the node whose join counter spawned work counts
    /// down. `job`'s own node, resolved once by whoever builds the handle.
    pub(crate) node: &'a Node,
    /// The job the node was dequeued as, which is what upstream reaches
    /// through `_node->_parent` and `_node->_topology`: the run, the graph
    /// its siblings live in, and the parent a scheduled task is counted
    /// against.
    pub(crate) job: &'a Job,
    /// The scheduling loop of the worker running the body: upstream's
    /// `Worker&` wherever it hands one to `_schedule` or `_corun_until`, held
    /// mutably because a corun re-enters that loop.
    pub(crate) thread: &'a mut WorkerThread,
}

impl<'a> Runtime<'a> {
    /// Builds the handle for a runtime task running on `node`, the node
    /// `job` is for. `pub(crate)` where upstream's constructor is private
    /// with `friend class Executor`.
    pub(crate) fn new(
        worker: &'a Worker,
        node: &'a Node,
        job: &'a Job,
        thread: &'a mut WorkerThread,
    ) -> Self {
        Self {
            worker,
            node,
            job,
            thread,
        }
    }

    /// Returns the worker running this runtime task's body, upstream's
    /// `worker()`.
    #[must_use]
    pub fn worker(&self) -> &'a Worker {
        self.worker
    }

    /// Returns the id of the worker running this runtime task's body, an
    /// index in `0..`[`Runtime::num_workers`]: [`Runtime::worker`]`().id()`
    /// in one hop.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::{Executor, Runtime};
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     assert_eq!(runtime.worker_id(), runtime.worker().id());
    ///     assert!(runtime.worker_id() < runtime.num_workers());
    /// });
    ///
    /// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
    /// ```
    #[must_use]
    pub fn worker_id(&self) -> usize {
        self.worker.id()
    }

    /// How many workers the executor running this task has: the part of
    /// upstream's `rt.executor().num_workers()` a runtime body can see.
    #[must_use]
    pub fn num_workers(&self) -> usize {
        self.thread.shared.workers.len()
    }
}

impl fmt::Debug for Runtime<'_> {
    /// Names the node and the worker, which is the whole of a runtime task's
    /// identity; the run behind it has no small rendering.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("node", &self.node.name())
            .field("worker", &self.worker.id())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::Executor;
    use crate::executor::queue::WorkerQueue;
    use crate::executor::topology::{GraphOwner, Parent, Topology, set_up_graph};
    use crate::prelude::*;

    /// Runs `body` against a runtime handle over `task`'s node, standing in
    /// for the dispatch arm in the crate-private `executor::runtime` -- which
    /// is where the handle is built for real, and where the cases that need
    /// the scheduler to *do* something live.
    ///
    /// `task` has to be `taskflow`'s one source task, since the job the
    /// handle is given is the one the scheduler would have dequeued for it.
    fn with_runtime<F>(taskflow: &Taskflow, task: Task, body: F)
    where
        F: FnOnce(&mut Runtime<'_>),
    {
        let executor = Executor::new(1);
        let flow = taskflow.flow();
        let topology = Arc::new(Topology::new(|| true, || {}, Arc::clone(flow)));
        let owner = GraphOwner::flow(flow);
        let parent = Parent::Run;

        set_up_graph(flow.graph());
        let job = Job::sources(&owner, &topology, &parent)
            .next()
            .expect("the taskflow has a source task");

        // A queue of its own rather than the worker's, as the cases in
        // `executor::topology` take one: nothing here is queued, and the
        // pool's real threads must not be able to steal what is.
        let mut thread = WorkerThread::new(Arc::clone(&executor.shared), 0, WorkerQueue::new());
        let mut runtime = Runtime::new(
            &executor.shared.workers[0],
            taskflow.graph().node(task),
            &job,
            &mut thread,
        );
        body(&mut runtime);
    }

    #[test]
    fn a_runtime_handle_names_where_it_is_running() {
        let mut taskflow = Taskflow::new();
        let task = taskflow.emplace_runtime(|_: &mut Runtime<'_>| {});
        taskflow.task(task).name("rt");

        with_runtime(&taskflow, task, |runtime| {
            assert_eq!(runtime.worker().id(), 0);
            assert_eq!(runtime.worker_id(), runtime.worker().id());
            // The worker count is what a body can see of the executor running
            // it, and it is the number upstream's partitioners ask for; the
            // module docs say why the executor itself is not here.
            assert_eq!(runtime.num_workers(), 1);
            // The node is what `Debug` reports, since it is the only part of
            // the handle with a name of its own.
            assert!(format!("{runtime:?}").contains("rt"));
        });
    }

    /// The body is handed the handle, which is the whole point of
    /// `RuntimeWork`: a runtime task's callable cannot be a plain
    /// [`Work`], because what it needs is an argument rather than a return
    /// value.
    #[test]
    fn the_work_handle_passes_the_runtime_to_the_body() {
        let seen = Arc::new(AtomicUsize::new(usize::MAX));
        let reported = Arc::clone(&seen);
        let mut taskflow = Taskflow::new();
        let task = taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            reported.store(runtime.worker().id(), Ordering::Relaxed);
        });

        let Handle::Runtime(work) = taskflow.graph().node(task).handle() else {
            panic!("the runtime handle did not survive emplace_runtime");
        };
        with_runtime(&taskflow, task, |runtime| work.invoke(runtime));

        assert_eq!(seen.load(Ordering::Relaxed), 0);
    }
}

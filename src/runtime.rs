//! Translated from `taskflow/core/runtime.hpp` (Taskflow v4.1.0).
//!
//! Only the shell of upstream's `tf::Runtime` is here: what the handle holds,
//! and the two things it can answer without reaching into the scheduler.
//! Everything the type exists *for* -- `schedule`, `corun` and
//! `is_cancelled` -- needs the executor, and lives in a second
//! `impl Runtime` block in `src/executor/runtime.rs`, beside the dispatch arm
//! that builds one. An inherent `impl` may sit anywhere in the defining
//! crate, which is how this port keeps the graph-side and executor-side
//! halves of one type in the modules that own them.
//!
//! # Why there is no `executor()`
//!
//! Upstream's `Runtime` holds an `Executor&` and hands it back. This port
//! cannot, and the reason is ownership rather than taste: a `Runtime` is
//! built by the runtime dispatch arm, which runs on a worker thread, and **a
//! worker thread holds the executor's shared state rather than the
//! `Executor` value**. Upstream's executor *is* that state; here an
//! [`Executor`](crate::Executor) is a handle over an `Arc` of it plus the
//! join handles of the pool, and it is owned by value, with no
//! `Arc<Executor>` anywhere in the crate for a worker to keep a `Weak` of.
//! So reproducing the member would take a raw back-pointer from the shared
//! state to the value that owns it, and the executor has had no `unsafe`
//! since issue #40.
//!
//! What upstream's own callers ask `rt.executor()` for is nearly always the
//! worker count -- `taskflow/algorithm/find.hpp` asks three times, which is
//! milestone 6 here -- and that *is* reachable from a worker, so
//! [`Runtime::num_workers`] stands in its place. A body that genuinely needs
//! the executor captures an `Arc<Executor>`, which is how upstream's own
//! `Runtime.ExternalGraph.Simple` reaches it -- by reference, out of the
//! enclosing scope -- and what this crate's
//! [`corun`](crate::Executor::corun) doctests already do.
//!
//! # Which bodies are handed one, and what they can do with it
//!
//! Two kinds of node, and `executor::runtime` runs both: a runtime task of a
//! graph, through
//! [`FlowBuilder::emplace_runtime`](crate::FlowBuilder::emplace_runtime), and
//! a *free* node whose body is runtime-shaped, which is upstream's
//! `is_runtime_task_v<F>` branch of `_async`, `_silent_async`,
//! `_dependent_async` and `_silent_dependent_async`. There are rather more
//! ways to submit one of those than there are kinds of them: four on the
//! [`Executor`](crate::Executor) --
//! [`spawn_runtime`](crate::Executor::spawn_runtime),
//! [`silent_async_runtime`](crate::Executor::silent_async_runtime) and the
//! two dependent forms -- and the same four on a `Runtime`, which is what
//! makes a child's body able to spawn children of its own. What differs
//! between them is who waits for the task and what the task waits for, never
//! what its body is handed.
//!
//! The second of upstream's two body shapes, `void(Runtime&, bool)`, is the
//! handle-returning one of those: it is called again when the node finishes
//! so that it can report what failed under it, and `executor::runtime` is
//! where that second entry is argued.
//!
//! What a body can do in the other direction is spawn work *under* its own
//! node, which is upstream's `rt.async`, `rt.silent_async`,
//! `rt.dependent_async` and `rt.silent_dependent_async` and is
//! `executor::runtime_async` (#92): eight methods, because each of the four
//! takes either body shape. Such a child is the only work a
//! [`Runtime::corun`] can wait for and the only thing the implicit join at
//! the end of the body is a join *of* -- where a task a body
//! [`schedule`](Runtime::schedule)s is a sibling, counted against this node's
//! parent rather than against the node, which is why `schedule` is not a way
//! to spawn anything. Upstream's `NonpreemptiveRuntime` is outside this
//! milestone altogether.

use core::fmt;

use crate::executor::topology::Job;
use crate::executor::worker::{Worker, WorkerThread};
use crate::graph::Node;

/// The scheduler, as a runtime task's body sees it: upstream's `tf::Runtime`.
///
/// A runtime task is a task handed the thing that is running it, which is what
/// lets a body schedule work of its own and wait for it. The handle borrows
/// the worker running the body, the node the body is running on and the job
/// that node was dequeued as, so it lives exactly as long as that one
/// invocation and is not meant to be stored -- upstream's is a stack object
/// its body is handed by reference, for the same reason.
///
/// What this module can answer is where the body is running. The scheduling
/// half -- `schedule`, `corun`, `corun_all` and `is_cancelled` -- is the
/// `impl` block in `src/executor/runtime.rs`, which is also where the arm
/// that builds one of these lives.
pub struct Runtime<'a> {
    /// The worker running the body, upstream's `_worker`.
    pub(crate) worker: &'a Worker,
    /// The node the body is running on, upstream's `_node`. It is the anchor
    /// a `corun` waits on and the node whose join counter work spawned by the
    /// body counts down, which is why the handle holds it before anything
    /// reads it.
    ///
    /// It is `job`'s own node, resolved once by whoever builds the handle
    /// rather than on each access: a job of a nested graph resolves its task
    /// index along a path of owners, so that lookup is a walk and not a field
    /// read.
    pub(crate) node: &'a Node,
    /// The job the node was dequeued as, which is what upstream reaches
    /// through `_node->_parent` and `_node->_topology`: the run this task is
    /// part of, the graph its siblings live in, and the parent that a task
    /// this body schedules is counted against. The crate-private
    /// `executor::topology` says why all of that travels with the job here
    /// rather than sitting on the node.
    pub(crate) job: &'a Job,
    /// The scheduling loop of the worker running the body: upstream's
    /// `Worker&` wherever it hands one to `_schedule` or `_corun_until`.
    ///
    /// Held mutably because a corun re-enters that loop, and the owning end
    /// of a worker's queue is the one thing a shared [`Worker`] cannot lend
    /// out -- the crate-private `executor::worker` says why a worker is two
    /// types here and one upstream.
    pub(crate) thread: &'a mut WorkerThread,
}

impl<'a> Runtime<'a> {
    /// Builds the handle for a runtime task running on `node`, which is the
    /// node `job` is for.
    ///
    /// Upstream's constructor is private, with `friend class Executor` to let
    /// the scheduler reach it; `pub(crate)` is the same statement in the one
    /// vocabulary Rust has for it. It was public while the handle held
    /// nothing but shared borrows a caller already had. What it holds now is
    /// the job and the worker's own scheduling loop, both crate-private, so a
    /// public constructor could not be written without leaking them into the
    /// public API -- which the `private_interfaces` lint is right to refuse.
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

    /// How many workers the executor running this task has: the part of
    /// upstream's `executor()` that a runtime body can actually see.
    ///
    /// Upstream writes `rt.executor().num_workers()`, and the callers that
    /// matter are its partitioners -- `taskflow/algorithm/find.hpp` sizes its
    /// chunks with exactly this number. The module docs say why the executor
    /// itself is not reachable from a worker and this is.
    ///
    /// The same answer [`Executor::num_workers`](crate::Executor::num_workers)
    /// gives, read out of the state the worker shares with it.
    #[must_use]
    pub fn num_workers(&self) -> usize {
        self.thread.shared.workers.len()
    }
}

impl fmt::Debug for Runtime<'_> {
    /// Names the node and the worker, which is the whole of a runtime task's
    /// identity. The run behind it has no small rendering and a handle that
    /// printed one would be unreadable where it is most useful, inside a
    /// failing body.
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

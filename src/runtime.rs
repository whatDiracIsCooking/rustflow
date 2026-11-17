//! Translated from `taskflow/core/runtime.hpp` (Taskflow v4.1.0).
//!
//! Only the shell of upstream's `tf::Runtime` is here: the three references it
//! holds, and the two accessors that hand two of them back. Everything the
//! type exists *for* needs the scheduler -- `schedule`, `corun` and
//! `is_cancelled` -- and arrives with the executor half of runtime tasking in
//! issue #51, as a second `impl Runtime` block in `src/executor/runtime.rs`.
//! An inherent `impl` may sit anywhere in the defining crate, which is how
//! this port keeps the graph-side and executor-side halves of one type in the
//! modules that own them.
//!
//! Upstream's `NonpreemptiveRuntime`, its `rt.async` / `silent_async` /
//! `dependent_async` family and its `void(Runtime&, bool)` body shape are all
//! outside this milestone.

use core::fmt;

use crate::executor::Executor;
use crate::executor::worker::Worker;
use crate::graph::Node;

/// The scheduler, as a runtime task's body sees it: upstream's `tf::Runtime`.
///
/// A runtime task is a task handed the thing that is running it, which is what
/// lets a body schedule work of its own and wait for it. The handle borrows
/// the executor, the worker running the body and the node the body is running
/// on, so it lives exactly as long as that one invocation and is not meant to
/// be stored -- upstream's is a stack object its body is handed by reference,
/// for the same reason.
///
/// What this can do today is say where it is running. The scheduling half is
/// issue #51's; until it lands the executor refuses a runtime task rather than
/// running it.
pub struct Runtime<'a> {
    /// The executor running the taskflow this node belongs to.
    pub(crate) executor: &'a Executor,
    /// The worker running the body, upstream's `_worker`.
    pub(crate) worker: &'a Worker,
    /// The node the body is running on, upstream's `_node`. It is the anchor a
    /// `corun` waits on and the parent a scheduled task is counted against,
    /// which is why the handle holds it before anything reads it.
    pub(crate) node: &'a Node,
}

impl<'a> Runtime<'a> {
    /// Builds the handle for a runtime task running on `node`.
    ///
    /// Upstream's constructor is private, with `friend class Executor` to let
    /// the scheduler reach it. Rust has no `friend`: the executor half of this
    /// type is another module, so the constructor has to be visible beyond
    /// this one, and nothing in the crate calls it before #51 gives the
    /// executor an arm that runs a runtime task. It lends out no more than the
    /// caller already holds -- three shared borrows, each behind an accessor
    /// -- so being public costs nothing but a name.
    #[must_use]
    pub fn new(executor: &'a Executor, worker: &'a Worker, node: &'a Node) -> Self {
        Self {
            executor,
            worker,
            node,
        }
    }

    /// Returns the executor running the taskflow this runtime task belongs to,
    /// upstream's `executor()`.
    #[must_use]
    pub fn executor(&self) -> &'a Executor {
        self.executor
    }

    /// Returns the worker running this runtime task's body, upstream's
    /// `worker()`.
    #[must_use]
    pub fn worker(&self) -> &'a Worker {
        self.worker
    }
}

impl fmt::Debug for Runtime<'_> {
    /// Names the node and the worker, which is the whole of a runtime task's
    /// identity. The executor behind it has no small rendering -- it is a
    /// thread pool -- and a handle that printed one would be unreadable where
    /// it is most useful, inside a failing body.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("node", &self.node.name())
            .field("worker", &self.worker.id())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::graph::{Graph, Handle, RuntimeWork, Task};

    /// Runs `body` against a runtime handle over `task`'s node, standing in
    /// for the executor arm that #51 writes.
    ///
    /// The worker is the executor's own record for worker 0, since a
    /// [`Worker`] is only ever created by an executor; which worker it is
    /// makes no difference to a handle that cannot schedule yet.
    fn with_runtime<F>(graph: &Graph, task: Task, body: F)
    where
        F: FnOnce(&mut Runtime<'_>),
    {
        let executor = Executor::new(1);
        let mut runtime = Runtime::new(&executor, &executor.shared.workers[0], graph.node(task));
        body(&mut runtime);
    }

    #[test]
    fn a_runtime_handle_names_where_it_is_running() {
        let mut graph = Graph::new();
        let task = graph.add_named_node("rt", Handle::Runtime(RuntimeWork::new(|_| {})));

        with_runtime(&graph, task, |runtime| {
            assert_eq!(runtime.worker().id(), 0);
            assert_eq!(runtime.executor().num_workers(), 1);
            // The node is what `Debug` reports, since it is the only part of
            // the handle with a name of its own.
            assert!(format!("{runtime:?}").contains("rt"));
        });
    }

    /// The body is handed the handle, which is the whole point of
    /// [`RuntimeWork`]: a runtime task's callable cannot be a plain
    /// [`crate::Work`], because what it needs is an argument rather than a
    /// return value.
    #[test]
    fn the_work_handle_passes_the_runtime_to_the_body() {
        let seen = Arc::new(AtomicUsize::new(usize::MAX));
        let reported = Arc::clone(&seen);
        let mut graph = Graph::new();
        let task = graph.add_named_node(
            "rt",
            Handle::Runtime(RuntimeWork::new(move |runtime: &mut Runtime<'_>| {
                reported.store(runtime.worker().id(), Ordering::Relaxed);
            })),
        );

        let Handle::Runtime(work) = graph.node(task).handle() else {
            panic!("the runtime handle did not survive add_named_node");
        };
        with_runtime(&graph, task, |runtime| work.invoke(runtime));

        assert_eq!(seen.load(Ordering::Relaxed), 0);
    }
}

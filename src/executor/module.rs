//! The executor's half of module tasking, which is issue #49. Upstream's
//! `_invoke_module_task`, `_invoke_adopted_module_task` and
//! `_invoke_module_task_impl` from `taskflow/core/executor.hpp` (Taskflow
//! v4.1.0) land here; nothing in this module is translated from them yet.
//!
//! What is here is the seam they land on: one function, called from the two
//! module arms of `WorkerThread::invoke`, that refuses the run. The file
//! exists so that #49 is a change to this module and not to the worker --
//! the same reason [`subflow`](crate::executor::subflow) and
//! [`runtime`](crate::executor::runtime) are their own files, which is what
//! lets the three land independently.
//!
//! When it does land, upstream's shape is short: an empty target graph is
//! nothing to do, and a non-empty one sets `PREEMPTED` on the module node and
//! schedules the target graph under it. Both module handles go through it --
//! upstream forwards `MODULE` and `ADOPTED_MODULE` to one `_impl` over a
//! `Graph&` -- and the only thing they differ in is who owns the target.

use crate::executor::topology::Job;
use crate::executor::worker::{Release, WorkerThread};
use crate::graph::Graph;

/// Runs the module task `job` is for, or refuses to.
///
/// `None` would mean the node has been preempted: its target graph is
/// running, and the last job of that graph will resume it. Nothing here
/// preempts yet, so the answer is always a strong release of the node's
/// successors -- after the run has been failed.
///
/// The refusal goes through the channel a panicking task body uses, which is
/// the louder of the two options rather than the quieter one. Since #32,
/// `Topology::record_panic` sets `EXCEPTION` beside `CAUGHT`, and `EXCEPTION`
/// is half of `Topology::cancelled`: refusing one node therefore cancels the
/// rest of the run, so every task the run had not started is skipped and
/// `wait` hands the message back. A bare `panic!` would be worse rather than
/// louder -- it would unwind with this job never counted as finished, so the
/// run's join counter would never reach zero and a `wait` would block for
/// ever. A hang says less than an error does.
pub(crate) fn invoke(_worker: &mut WorkerThread, job: &Job, _graph: &Graph) -> Option<Release> {
    job.topology().record_panic(Box::new(
        "running a module task is not supported yet: module tasks are issue #49",
    ));
    Some(Release::Strong)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::Executor;
    use crate::prelude::*;

    /// Both module handles refuse, and the refusal is reported as the run's
    /// failure rather than raised on a worker thread or hung on.
    #[test]
    fn a_module_task_refuses_the_run() {
        for adopted in [false, true] {
            let executor = Executor::new(2);
            let mut taskflow = Taskflow::new();
            if adopted {
                let mut target = Graph::new();
                target.add_node(Handle::Static(Work::new(|| {})));
                taskflow.adopt(target);
            } else {
                let mut target = Taskflow::new();
                target.emplace(|| {});
                taskflow.composed_of(Arc::new(target));
            }

            let error = executor
                .run(&taskflow)
                .wait()
                .expect_err("a module task is refused");
            assert_eq!(
                error.to_string(),
                "a task panicked: running a module task is not supported yet: \
                 module tasks are issue #49"
            );
        }
    }
}

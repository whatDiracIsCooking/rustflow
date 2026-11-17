//! The executor's half of runtime tasking, which is issue #51. Upstream's
//! `_invoke_runtime_task`, `_invoke_runtime_task_impl` and the scheduling
//! methods of `tf::Runtime` from `taskflow/core/executor.hpp` (Taskflow
//! v4.1.0) land here; nothing in this module is translated from them yet.
//!
//! What is here is the seam they land on: one function, called from the
//! runtime arm of `WorkerThread::invoke`, that refuses the run. The file
//! exists so that #51 is a change to this module and not to the worker --
//! the same reason [`module`](crate::executor::module) and
//! [`subflow`](crate::executor::subflow) are their own files, which is what
//! lets the three land independently. The shell of
//! [`Runtime`](crate::Runtime) is already in `src/runtime.rs`, and the
//! `impl Runtime` block that gives it `schedule`, `corun` and `is_cancelled`
//! belongs here beside the arm.
//!
//! When it does land, upstream's shape is: hand the body a `Runtime` over
//! this node, and preempt the node if the body left work of its own
//! outstanding -- the node's join counter is what that work counts down, so
//! the node is the parent of its own asynchronous children.

use crate::executor::topology::Job;
use crate::executor::worker::{Release, WorkerThread};
use crate::graph::RuntimeWork;

/// Runs the runtime task `job` is for, or refuses to.
///
/// `None` would mean the node has been preempted: work its body scheduled is
/// still running, and the last of it to finish will resume the node. Nothing
/// here preempts yet, so the answer is always a strong release of the node's
/// successors -- after the run has been failed, and without the body being
/// invoked at all.
///
/// The refusal goes through the channel a panicking task body uses, and
/// [`module::invoke`](crate::executor::module::invoke) says what that costs:
/// since #32 it cancels the rest of the run, which is still the better of the
/// two failures available here.
pub(crate) fn invoke(
    _worker: &mut WorkerThread,
    job: &Job,
    _work: &RuntimeWork,
) -> Option<Release> {
    job.topology().record_panic(Box::new(
        "running a runtime task is not supported yet: runtime tasks are issue #51",
    ));
    Some(Release::Strong)
}

#[cfg(test)]
mod tests {
    use crate::Executor;
    use crate::prelude::*;

    /// The refusal is reported as the run's failure rather than raised on a
    /// worker thread or hung on -- and the body is not run.
    #[test]
    fn a_runtime_task_refuses_the_run() {
        let executor = Executor::new(2);
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|_: &mut Runtime<'_>| panic!("the body must not run"));

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("a runtime task is refused");
        assert_eq!(
            error.to_string(),
            "a task panicked: running a runtime task is not supported yet: \
             runtime tasks are issue #51"
        );
    }
}

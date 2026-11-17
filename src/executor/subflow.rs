//! The executor's half of subflow tasking, which is issue #50. Upstream's
//! `_invoke_subflow_task` and `Subflow::join` from
//! `taskflow/core/executor.hpp` (Taskflow v4.1.0) land here; nothing in this
//! module is translated from them yet.
//!
//! What is here is the seam they land on: one function, called from the
//! subflow arm of `WorkerThread::invoke`, that refuses the run. The file
//! exists so that #50 is a change to this module and not to the worker --
//! the same reason [`module`](crate::executor::module) and
//! [`runtime`](crate::executor::runtime) are their own files, which is what
//! lets the three land independently. The builder half of
//! [`Subflow`](crate::Subflow) is already in `src/subflow.rs`, and the
//! `impl Subflow` block that gives it `join` belongs here beside the arm.
//!
//! When it does land, upstream's shape is: build the handle, run the body,
//! and -- if the body left the subflow joinable and its graph non-empty --
//! set `PREEMPTED` on the node and schedule the subgraph under it. The second
//! entry clears the bit and clears the subgraph unless the body asked to
//! retain it.

use std::sync::Mutex;

use crate::executor::topology::Job;
use crate::executor::worker::{Release, WorkerThread};
use crate::graph::{Graph, SubflowWork};

/// Runs the subflow task `job` is for, or refuses to.
///
/// `None` would mean the node has been preempted: the graph its body built is
/// running, and the last job of that graph will resume it. Nothing here
/// preempts yet, so the answer is always a strong release of the node's
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
    _work: &SubflowWork,
    _subgraph: &Mutex<Graph>,
) -> Option<Release> {
    job.topology().record_panic(Box::new(
        "running a subflow task is not supported yet: subflow tasks are issue #50",
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
    fn a_subflow_task_refuses_the_run() {
        let executor = Executor::new(2);
        let mut taskflow = Taskflow::new();
        taskflow.emplace_subflow(|_: &mut Subflow<'_>| panic!("the body must not run"));

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("a subflow task is refused");
        assert_eq!(
            error.to_string(),
            "a task panicked: running a subflow task is not supported yet: \
             subflow tasks are issue #50"
        );
    }
}

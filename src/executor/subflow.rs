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
//!
//! # How the subgraph is named, which is settled
//!
//! One question that shape raises is not this issue's, because scheduling a
//! graph needs whatever *owns* it and a subgraph is owned by the `Mutex` in
//! [`Handle::Subflow`](crate::Handle::Subflow). Issue #74 answered it for all
//! four kinds of nested graph at once: the subgraph is moved out of its lock
//! for the length of its run and held by every job of it through one shared
//! allocation, which `Job::graph_inside` builds from this node's own job and
//! a `Some(graph)` where the adopted-module arm passes `None`. The
//! `executor::topology` module docs have the three options that were on the
//! table and why a lock on the hot path is not one of them -- it would
//! serialise the jobs of one subgraph and deadlock a body that coruns its
//! own.
//!
//! What is left for this issue is the policy rather than the shape: taking
//! the lease needs `&mut Graph`, so the body's guard has to be given up or
//! handed over first, and putting the graph back -- or not, which is
//! upstream's clear-unless-`RETAIN_SUBFLOW` default -- happens once the
//! nested run is over and the last share of the lease is the one the restorer
//! holds.

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

//! The executor's half of async tasking, which is issue #88. Upstream's
//! `_schedule_async_task`, `_tear_down_async`, `_async` and `_silent_async`
//! from `taskflow/core/async.hpp` (Taskflow v4.1.0) land here; nothing in
//! this module is translated from them yet.
//!
//! What is here is the seam they land on: one function, called from the
//! [`Handle::Async`](crate::Handle) arm of `WorkerThread::invoke`, that
//! refuses the run. The file exists so that #88 is a change to this module
//! and not to the worker -- the same arrangement
//! [`module`](crate::executor::module), [`subflow`](crate::executor::subflow)
//! and [`runtime`](crate::executor::runtime) were given by #47, which is what
//! let the three of them land independently.
//!
//! The types #88 needs are all in place as of #86: [`AsyncWork`] is the body,
//! in the three shapes upstream's variant has; `executor::async_node` is the
//! free-standing node an async task is; `Job::free` is the job that runs one,
//! and `Topology::free_standing` is the run it claims to be part of. What is
//! left for #88 is the scheduling -- the submitting calls on
//! [`Executor`](crate::Executor), the executor's count of outstanding work
//! and the tear-down that is upstream's `_tear_down_async` rather than its
//! `_tear_down_nonasync`.
//!
//! # What this arm is for
//!
//! A node reached through *this* function is not an async task being
//! submitted: it is an [`Handle::Async`](crate::Handle) handle somebody put
//! in a taskflow's graph, which [`Graph::add_node`](crate::Graph::add_node)
//! permits because it takes any handle. Upstream permits the same thing and
//! runs the body, because a `Node` is a `Node` to its `_invoke`; this port
//! refuses it, and will go on refusing it after #88 lands.
//!
//! The reason is that an async body is a `FnOnce` -- [`AsyncWork`] says so --
//! and a graph may be run any number of times. The second run of a taskflow
//! holding such a node would find the body gone, so the choice is between a
//! task that silently does nothing on every run but the first and a run that
//! says what is wrong. An async task belongs to the executor, not to a graph.
//!
//! The refusal is also the only reason this module has a `#[cfg(test)]`
//! block today, and the case there is the one #47 wrote for its own stubs.

use crate::executor::topology::Job;
use crate::executor::worker::{Release, WorkerThread};
use crate::graph::AsyncWork;

/// Refuses to run the async node `job` is for.
///
/// `Some(Release::Strong)` rather than `None`: `None` would mean the node has
/// been preempted and something will resume it, which nothing here does. So
/// the node finishes and releases its successors, after the run has been
/// failed -- and a cancelled run skips them anyway, which is what the
/// refusal makes of the rest of the graph.
///
/// The refusal goes through the channel a panicking task body uses, for the
/// reason #47 gives in [`module`](crate::executor::module): `record_panic`
/// sets `EXCEPTION` beside `CAUGHT`, and `EXCEPTION` is half of
/// `Topology::cancelled`, so refusing one node cancels the rest of the run
/// and `wait` hands the message back. A bare `panic!` would be worse rather
/// than louder -- it would unwind with this job never counted as finished, so
/// the run's join counter would never reach zero and a `wait` would block for
/// ever.
pub(crate) fn invoke(_worker: &mut WorkerThread, job: &Job, _work: &AsyncWork) -> Option<Release> {
    job.topology().record_panic(Box::new(
        "an async node cannot be run as part of a taskflow: async tasks are issue #88",
    ));
    Some(Release::Strong)
}

#[cfg(test)]
mod tests {
    use crate::Executor;
    use crate::graph::AsyncWork;
    use crate::prelude::*;

    /// An async handle in a taskflow's graph refuses the run, and the refusal
    /// is reported as the run's failure rather than raised on a worker thread
    /// or hung on. #47's case for its own stubs, over the handle #86 added.
    #[test]
    fn an_async_node_in_a_graph_refuses_the_run() {
        let executor = Executor::new(2);
        let mut taskflow = Taskflow::new();
        taskflow
            .graph_mut()
            .add_named_node("free", Handle::Async(AsyncWork::plain(|| {})));

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("an async node is refused");
        assert_eq!(
            error.to_string(),
            "a task panicked: an async node cannot be run as part of a taskflow: \
             async tasks are issue #88"
        );
    }
}

//! The executor's half of dependent-async tasking, which is issue #91.
//! Upstream's `_schedule_dependent_async_task`, `_process_dependent_async`
//! and `_tear_down_dependent_async` from `taskflow/core/async.hpp` and
//! `taskflow/core/executor.hpp` (Taskflow v4.1.0) land here; nothing in this
//! module is translated from them yet.
//!
//! What is here is the seam they land on: one function, called from the
//! [`Handle::DependentAsync`](crate::Handle) arm of `WorkerThread::invoke`,
//! that refuses the run -- [`async`](crate::executor::r#async) is the same
//! arrangement for the other async handle, and its docs have why a stub
//! module per node kind is what lets the waves that fill them land
//! independently.
//!
//! The types #91 needs are in place as of #86, and two of them are this
//! kind's rather than the plain async one's: `AsyncNode::push_successor` and
//! `AsyncNode::finish` are the pair upstream performs under its
//! `ESTATE::LOCKED` spin bit, and [`AsyncTask`](crate::AsyncTask) is the
//! handle a caller names a dependency with. What is left for #91 is the
//! submitting calls, the join counter arithmetic over however many
//! dependencies were named, and the tear-down that releases the successors
//! `finish` hands back.
//!
//! A node reached through *this* function is an
//! [`Handle::DependentAsync`](crate::Handle) handle somebody put in a
//! taskflow's graph, which is refused for the reason
//! [`async`](crate::executor::r#async) sets out: an async body runs once and
//! a graph runs any number of times, so such a node is a mistake worth
//! reporting rather than a task worth running. It is doubly so here, because
//! a dependency of a dependent-async task is another free node and not an
//! edge of any graph -- a node of this kind inside a taskflow could not have
//! its dependencies honoured even in principle.

use crate::executor::topology::Job;
use crate::executor::worker::{Release, WorkerThread};
use crate::graph::AsyncWork;

/// Refuses to run the dependent-async node `job` is for.
///
/// The refusal and the `Some(Release::Strong)` are
/// [`async::invoke`](crate::executor::r#async::invoke)'s, for its reasons:
/// the run is failed through the channel a panicking body uses, and the node
/// is counted as finished so that the run's join counter can still reach
/// zero.
pub(crate) fn invoke(_worker: &mut WorkerThread, job: &Job, _work: &AsyncWork) -> Option<Release> {
    job.topology().record_panic(Box::new(
        "a dependent-async node cannot be run as part of a taskflow: \
         dependent-async tasks are issue #91",
    ));
    Some(Release::Strong)
}

#[cfg(test)]
mod tests {
    use crate::Executor;
    use crate::graph::AsyncWork;
    use crate::prelude::*;

    /// A dependent-async handle in a taskflow's graph refuses the run, as the
    /// plain async handle does and through the same channel.
    #[test]
    fn a_dependent_async_node_in_a_graph_refuses_the_run() {
        let executor = Executor::new(2);
        let mut taskflow = Taskflow::new();
        taskflow
            .graph_mut()
            .add_named_node("free", Handle::DependentAsync(AsyncWork::plain(|| {})));

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("a dependent-async node is refused");
        assert_eq!(
            error.to_string(),
            "a task panicked: a dependent-async node cannot be run as part of a taskflow: \
             dependent-async tasks are issue #91"
        );
    }
}

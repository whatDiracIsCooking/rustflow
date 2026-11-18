//! A Rust port of [Taskflow](https://github.com/taskflow/taskflow), a C++
//! task-parallel programming system built around task dependency graphs.
//!
//! # What the crate exposes
//!
//! A [`Taskflow`] owns a [`Graph`] of [`Node`]s, each addressed by a [`Task`]
//! index and built through the [`FlowBuilder`] API; [`dump`] writes the result
//! out as Graphviz DOT. Condition tasks are graph citizens too -- a
//! [`Handle::Condition`] or [`Handle::MultiCondition`] holds the callable
//! whose return value picks successors, and a dependency on one is *weak*: a
//! task a conditioner may never choose cannot be waited for.
//!
//! [`Executor`] is a fixed pool of worker threads over per-worker queues,
//! with shared overflow buffers for the threads outside the pool and a
//! notifier that idles a worker with nowhere left to steal from.
//! [`Executor::run`], [`Executor::run_n`] and [`Executor::run_until`] submit a
//! graph for one run, a fixed number of them, or as many as a predicate asks
//! for, and each hands back a [`RunHandle`]: wait on it for a [`RunError`],
//! poll it as a [`Future`], or drop it, which blocks until the run is over.
//! The `_with` variant of each attaches a callback the last job of a run
//! calls, the `run_owned` family takes the taskflow by value,
//! [`Executor::corun`] and [`Executor::corun_until`] let a worker wait on a
//! nested run by working on it rather than parking, and
//! [`Executor::wait_for_all`] blocks until the executor is carrying neither a
//! run nor a free async task. A run under way can be cancelled through
//! [`RunHandle::cancel`] or the lifetime-free [`Canceller`] it hands out,
//! which is not preemptive and leaves the run *completing* rather than
//! failing; a panicking task cancels the rest of its own run on those terms
//! and adds the payload, which `wait` hands back as [`RunError::Panicked`].
//!
//! Work needs no graph at all. [`Executor::spawn`] hands a closure straight to
//! the pool and gives back an [`AsyncHandle`] on its result,
//! [`Executor::silent_async`] does the same for a body nobody is waiting on,
//! and [`Executor::spawn_runtime`] and [`Executor::silent_async_runtime`] are
//! those two over a body handed a [`Runtime`], so that it can spawn work of
//! its own and is not done until that work is. [`Executor::dependent_async`]
//! and [`Executor::silent_dependent_async`] put edges between async tasks,
//! each handing back an [`AsyncTask`] a later submission can name as a
//! dependency -- a graph built one task at a time while the tasks already
//! submitted run, which no [`Taskflow`] can do. Both have a
//! `_runtime`-bodied twin too, and any thread may submit any of them.
//!
//! A task can also be the thing that builds the graph.
//! [`FlowBuilder::emplace_subflow`] hands its body a [`Subflow`], itself a
//! [`FlowBuilder`] over a subgraph the node owns, which runs when the body
//! calls [`Subflow::join`] or, if it never does, once the body has returned.
//! [`FlowBuilder::emplace_runtime`] hands its body a [`Runtime`] instead,
//! whose [`schedule`](Runtime::schedule) puts a task of its own graph straight
//! onto the worker's queue past that task's dependencies and whose
//! [`corun`](Runtime::corun) waits for whatever the body spawned under the
//! node. [`FlowBuilder::composed_of`] runs another taskflow's graph in place
//! of a task, in as many taskflows as the caller likes since the module task
//! owns a share of its target, and [`FlowBuilder::adopt`] is the same for a
//! graph with no other owner. [`Executor::task_group`] is the last of them: a
//! scope a worker can spawn into and wait for with no graph and no node of
//! its own, whose children are counted by the group rather than by the
//! executor, and dropping one with children outstanding is *not* a join.
//!
//! One mechanism carries all of those, and it is upstream's: a node with work
//! of its own still outstanding does not finish when its body returns but is
//! entered a second time, by the last job of that work to count it down, and
//! only then releases its successors. So a failure inside a nested graph is
//! not automatically the run's -- it goes to whoever is *waiting* for that
//! graph, a [`Subflow::join`], a [`Runtime::corun`], a [`TaskGroup::corun`] or
//! an [`Executor::corun`], and reaches `wait` only when nobody is.
//!
//! [`Semaphore`] is the one constraint that spans tasks instead of following
//! the edges: [`TaskMut::acquire`] and [`TaskMut::release`] name the
//! semaphores a task takes before its body and gives back after it, and a
//! task that cannot have every count it asked for is parked rather than run,
//! to be rescheduled by whoever releases.
//!
//! # Status
//!
//! Steps 1 to 5 of the README's "Order of work" are in: everything above is
//! built and tested against the upstream unit tests it was translated from.
//! The consolidation milestone that ran next changed nothing a caller can
//! see and three things a reader of the source meets -- the 24 public methods
//! of the async family are one-line delegations onto a single core on the
//! executor's shared state, the run's machinery is the `executor::topology`
//! module tree rather than one file, and every doc comment in the crate is
//! held to the documentation contract in `DESIGN.md`. Steps 6 and 7 --
//! observers and algorithms, then pipelines -- are not started.
//!
//! The crate's only `unsafe` blocks are in
//! [`notifier`](executor::notifier), in the lock-free waiter stack.
//! `DESIGN.md` is why the port is shaped the way it is, and its "Decisions
//! taken in M6" section is the consolidation's record.
//!
//! The port follows **Taskflow v4.1.0** (commit `45366fe5`, 2026-06-19). See
//! the README's "Upstream pin" section before translating anything.
//!
//! # Relationship to upstream
//!
//! Taskflow is MIT licensed, as is this crate. This is a reimplementation in
//! Rust, not a binding: the C++ API leans on raw pointers and handle objects
//! that alias into a graph the user still owns, which does not survive
//! translation to Rust's aliasing rules unchanged. Upstream's copyright notice
//! is carried verbatim in `LICENSE-TASKFLOW` at the repository root, alongside
//! this port's own `LICENSE`.
//!
//! Any module translated closely from upstream **must** name its source on the
//! first line of its own `//!` doc comment:
//!
//! ```text
//! //! Translated from `taskflow/core/executor.hpp` (Taskflow v4.1.0).
//! ```
//!
//! Silence therefore means "this module is our own design", which is what
//! makes the attribution auditable. See the README's "Provenance and
//! attribution" section.

#![doc(html_root_url = "https://docs.rs/rustflow/0.0.0")]

pub mod async_task;
pub mod dump;
pub mod executor;
pub mod flow_builder;
pub mod graph;
pub mod runtime;
pub mod semaphore;
pub mod subflow;
pub mod task;
pub mod task_group;
pub mod taskflow;

pub use crate::async_task::AsyncTask;
pub use crate::executor::Executor;
pub use crate::executor::r#async::AsyncHandle;
pub use crate::executor::corun::CorunError;
pub use crate::executor::queue::{Buffers, Thief, WorkerQueue};
pub use crate::executor::rng::{Xorshift32, Xorshift64};
pub use crate::executor::run_handle::{Canceller, RunError, RunHandle};
pub use crate::executor::subflow::JoinError;
pub use crate::executor::worker::{Worker, WorkerInterface};
pub use crate::flow_builder::{Callables, FlowBuilder};
pub use crate::graph::{
    AsyncWork, Graph, Handle, Node, OnceWork, RuntimeWork, SubflowWork, Task, Work,
};
pub use crate::runtime::Runtime;
pub use crate::semaphore::Semaphore;
pub use crate::subflow::Subflow;
pub use crate::task::{TaskMut, TaskRef, TaskType};
pub use crate::task_group::TaskGroup;
pub use crate::taskflow::Taskflow;

/// The names you need in scope to build a graph.
///
/// [`FlowBuilder`] is a trait, so its methods -- `emplace`, `placeholder`,
/// `linearize` and the rest -- are only callable where it is imported. Upstream
/// gets them from a base class and needs no equivalent.
///
/// ```
/// use rustflow::prelude::*;
///
/// let mut taskflow = Taskflow::with_name("example");
/// let task = taskflow.emplace(|| println!("work"));
/// assert!(taskflow.graph().node(task).handle().is_static());
/// ```
pub mod prelude {
    pub use crate::flow_builder::FlowBuilder;
    pub use crate::graph::{Graph, Handle, Task, Work};
    pub use crate::runtime::Runtime;
    pub use crate::subflow::Subflow;
    pub use crate::taskflow::Taskflow;
}

/// The crate version, as reported by Cargo at compile time.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_wired_to_cargo() {
        assert_eq!(VERSION, "0.0.0");
    }
}

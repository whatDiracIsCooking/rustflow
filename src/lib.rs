//! A Rust port of [Taskflow](https://github.com/taskflow/taskflow), a C++
//! task-parallel programming system built around task dependency graphs.
//!
//! # Status
//!
//! What the crate exposes is a static task graph and the work-stealing
//! executor that runs it. A [`Taskflow`] owns a [`Graph`] of [`Node`]s, each
//! addressed by a [`Task`] index into that graph's arena and built through
//! the [`FlowBuilder`] API, and [`dump`] writes the result out as Graphviz
//! DOT. Condition tasks are graph citizens too -- a [`Handle::Condition`] or
//! [`Handle::MultiCondition`] holds the callable whose return value picks
//! successors, and a dependency on one counts as weak, because a task
//! preceded by a conditioner may never be chosen and so cannot be waited for
//! the way a strong dependency is. The executor branches on them: it runs the
//! body like any other task's and then schedules only the successors the
//! return value names, outright and with their join counters zeroed rather
//! than decremented, since a branch can re-enter a successor long before its
//! strong predecessors come round again. An index naming no successor at all
//! selects nothing, which is upstream's behaviour and is how a branch is
//! written to end, and a node's own counter is re-armed before any successor
//! is queued, which is what lets a conditioner point back at itself and loop.
//!
//! [`Executor`] is a fixed pool of worker threads over per-worker queues,
//! with shared overflow buffers for the threads outside the pool that submit
//! to it and a notifier that idles a worker which has run out of anywhere to
//! steal from, which is
//! [`NonblockingNotifier`](executor::notifier::NonblockingNotifier) unless a
//! caller asks for the other one. [`Executor::run`], [`Executor::run_n`] and
//! [`Executor::run_until`] submit a graph for one run, a fixed number of
//! them, or as many as a predicate keeps asking for, and each hands back a
//! [`RunHandle`]: wait on it for a [`RunError`], poll it as a [`Future`], or
//! just drop it, which blocks until the run is over so that a queued job can
//! never outlive the borrow it was scheduled under. The `_with` variant of
//! each attaches a callback that the last job of a run calls, the `run_owned`
//! family takes the taskflow by value for a caller with no borrow to lend,
//! [`Executor::corun`] and [`Executor::corun_until`] let a worker wait on a
//! nested run by working on it rather than parking -- which is what keeps
//! nesting from starving the pool of the very threads the inner run needs --
//! and [`Executor::wait_for_all`] blocks until the executor is carrying no
//! run at all.
//!
//! Interfering with a run already under way is where the port is still
//! part-built. Cancelling one works: [`RunHandle::cancel`], or a
//! [`Canceller`] -- the lifetime-free token the handle hands out, which is
//! what lets a task of another taskflow cancel this run -- stops the executor
//! scheduling that run onwards. It is not preemptive, so a task already
//! running is left to finish, and the run then *completes* rather than
//! failing: `wait` hands back `Ok(())` as upstream's `get()` does not throw,
//! the callback still fires, and a cancelled `run_n` does not go round again.
//! A panic still interferes with nothing: a panicking task is caught where
//! upstream catches an exception and reported through the handle, while the
//! rest of that run executes to the end, so a failure is something a caller
//! is told about afterwards rather than something that stops the graph.
//! The README's "Order of work" section is the sequence the whole port
//! follows, and reading what is next there beats a list here that would go
//! stale the week after it was written.
//!
//! The port follows **Taskflow v4.1.0** (commit `45366fe5`, 2026-06-19). See
//! the README's "Upstream pin" section before translating anything.
//!
//! # Relationship to upstream
//!
//! Taskflow is MIT licensed, as is this crate. This is a reimplementation in
//! Rust, not a binding: the C++ API leans on raw pointers and handle objects
//! that alias into a graph the user still owns, which does not survive
//! translation to Rust's aliasing rules unchanged.
//!
//! Upstream's copyright notice is carried verbatim in `LICENSE-TASKFLOW` at
//! the repository root, alongside this port's own `LICENSE`.
//!
//! # Attribution convention
//!
//! Any module translated closely from upstream **must** name its source on the
//! first line of its own `//!` doc comment:
//!
//! ```text
//! //! Translated from `taskflow/core/executor.hpp` (Taskflow v4.1.0).
//! ```
//!
//! Silence therefore means "this module is our own design". Keeping that
//! distinction per-module is what makes the attribution auditable later; a
//! blanket notice in a license file cannot tell you which files it earned.
//! See the README's "Provenance and attribution" section.

#![doc(html_root_url = "https://docs.rs/rustflow/0.0.0")]

pub mod dump;
pub mod executor;
pub mod flow_builder;
pub mod graph;
pub mod task;
pub mod taskflow;

pub use crate::executor::Executor;
pub use crate::executor::corun::CorunError;
pub use crate::executor::queue::{Buffers, Thief, WorkerQueue};
pub use crate::executor::rng::{Xorshift32, Xorshift64};
pub use crate::executor::run_handle::{Canceller, RunError, RunHandle};
pub use crate::executor::worker::{Worker, WorkerInterface};
pub use crate::flow_builder::{Callables, FlowBuilder};
pub use crate::graph::{Graph, Handle, Node, Task, Work};
pub use crate::task::{TaskMut, TaskRef, TaskType};
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

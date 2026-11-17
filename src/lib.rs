//! A Rust port of [Taskflow](https://github.com/taskflow/taskflow), a C++
//! task-parallel programming system built around task dependency graphs.
//!
//! # Status
//!
//! What the crate exposes is a task dependency graph and the work-stealing
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
//! just drop it, which blocks until the run is over. That borrow is what
//! makes "do not mutate a running taskflow" a compile error, and a handle a
//! caller *leaks* rather than drops ends it early -- which is a leak and not
//! a use-after-free, because every queued job owns a share of the run and of
//! the taskflow's storage. The `_with` variant of
//! each attaches a callback that the last job of a run calls, the `run_owned`
//! family takes the taskflow by value for a caller with no borrow to lend,
//! [`Executor::corun`] and [`Executor::corun_until`] let a worker wait on a
//! nested run by working on it rather than parking -- which is what keeps
//! nesting from starving the pool of the very threads the inner run needs --
//! and [`Executor::wait_for_all`] blocks until the executor is carrying
//! neither a run nor one of the free async tasks described below.
//!
//! Interfering with a run already under way happens two ways, and both go
//! through one word of state. Cancelling one works: [`RunHandle::cancel`], or
//! a [`Canceller`] -- the lifetime-free token the handle hands out, which is
//! what lets a task of another taskflow cancel this run -- stops the executor
//! scheduling that run onwards. It is not preemptive, so a task already
//! running is left to finish, and the run then *completes* rather than
//! failing: `wait` hands back `Ok(())` as upstream's `get()` does not throw,
//! the callback still fires, and a cancelled `run_n` does not go round again.
//! A panicking task cancels the rest of its own run on exactly those terms,
//! through the other bit of that word: it is caught where upstream catches an
//! exception, so no worker dies of it, and what it adds to a cancellation is
//! the payload, which `wait` hands back as [`RunError::Panicked`]. Only the
//! first panicking task of a run is reported, and a later run of the same
//! taskflow starts clean.
//!
//! Work needs no graph at all to be run. [`Executor::spawn`] hands a closure
//! straight to the pool and gives back an [`AsyncHandle`] on its result --
//! wait on it, poll it as a [`Future`], or drop it, which detaches -- and
//! [`Executor::silent_async`] does the same for a body nobody is waiting on.
//! [`Executor::spawn_runtime`] and [`Executor::silent_async_runtime`] are
//! those two over a body that is handed a [`Runtime`] rather than nothing, so
//! that it can spawn work of its own; such a task is not done until that work
//! is. [`Executor::dependent_async`] and
//! [`Executor::silent_dependent_async`] put edges between async tasks: each
//! hands back an [`AsyncTask`], a shared handle on the one node the task is,
//! and naming such a handle as a dependency of a later submission is how that
//! graph is built -- one task at a time, while the tasks already submitted
//! run, which no [`Taskflow`] can do. An `AsyncTask` answers only what is
//! about identity and state, upstream's own limit:
//! [`is_done`](AsyncTask::is_done),
//! [`use_count`](AsyncTask::use_count) -- which counts the executor's shares
//! as well as the caller's handles, since a queued job is a share -- and
//! [`take_panic`](AsyncTask::take_panic). Both of the dependent forms have a
//! `_runtime`-bodied twin as well.
//!
//! Any thread may submit any of them, a task body already running on one of
//! the executor's own workers included, which is how async tasks nest. A
//! *free* async task -- one submitted to the executor rather than under
//! something -- is counted in [`Executor::num_topologies`] like a run, so
//! [`Executor::wait_for_all`] covers it, and in a second count of its own
//! that dropping the executor waits for, because nothing else can: a detached
//! handle borrows nothing, where a leaked [`RunHandle`] still takes the pool
//! down with it. A panic out of a `spawn` body is reported from its handle as
//! [`RunError::Panicked`] and cancels nothing, since an async task is part of
//! nothing to cancel; a `silent_dependent_async`'s stays on the node, where
//! `take_panic` reads it back, and a `silent_async`'s, which no handle names,
//! is dropped with the node.
//!
//! A task can also be the thing that builds the graph.
//! [`FlowBuilder::emplace_subflow`] hands its body a [`Subflow`], itself a
//! [`FlowBuilder`] over a subgraph the node owns, and that subgraph runs
//! when the body calls [`Subflow::join`] or, if it never does, once the body
//! has returned. [`FlowBuilder::emplace_runtime`] hands its body a
//! [`Runtime`] instead, whose [`schedule`](Runtime::schedule) puts a task of
//! its own graph straight onto the worker's queue past that task's
//! dependencies, and whose [`corun`](Runtime::corun) waits for whatever the
//! body left outstanding under the node -- which is the async tasks it
//! spawned, through the same eight forms the executor has, counted against
//! the node rather than against the run and queued on the very worker the
//! body is running on. A body that spawns and never coruns is joined all the
//! same, once it has returned. And [`FlowBuilder::composed_of`] runs another
//! taskflow's graph in place of a task, as many times and in as many
//! taskflows as the caller likes, since the module task owns a share of its
//! target; [`FlowBuilder::adopt`] is the same thing for a graph that has no
//! other owner.
//!
//! One mechanism carries all of those, and it is upstream's: a node with work
//! of its own still outstanding -- a subgraph, a composed graph, or whatever
//! a runtime body spawned -- does not finish when its body returns but is
//! entered a second time, by the last job of that work to count it down, and
//! only then releases its successors. A runtime-bodied *async* task is
//! entered twice for the same reason, which is what makes its handle's `wait`
//! an answer about the children as well as the body. So a failure inside a
//! nested graph is not automatically the run's. It goes to whoever is
//! *waiting* for that graph -- a [`Subflow::join`], a [`Runtime::corun`], a
//! [`TaskGroup::corun`], an [`Executor::corun`] -- and reaches `wait` only
//! when nobody is, which is what lets the task that asked for the nested work
//! be the task that answers for it.
//!
//! A worker can open a scope to spawn into and wait for with no graph and no
//! node of its own: [`Executor::task_group`] hands back a [`TaskGroup`] with
//! the same eight submitting forms, and [`TaskGroup::corun`] waits for its
//! children by staying in the work-stealing loop, which is why only a worker
//! may open one. Dropping a group with children still outstanding is *not* a
//! join -- upstream has no destructor on it either -- and the children of a
//! group are counted by the group rather than by the executor, so
//! `wait_for_all` does not cover them. Recursive parallelism is what it is
//! for: a plain recursive function rather than a graph somebody has to build.
//!
//! [`Semaphore`] is the one constraint that spans tasks instead of following
//! the edges. [`TaskMut::acquire`] and [`TaskMut::release`] name the
//! semaphores a task takes before its body runs and gives back after it, and
//! a task that cannot have every count it asked for is parked in the
//! semaphore rather than run, to be rescheduled by whoever releases -- so a
//! semaphore of one serialises a set of tasks that have no dependency on each
//! other at all. Releasing past the maximum is the releasing task's own
//! failure, delivered where a panic in its body would go, and a task parked
//! when its run is cancelled *stays* parked, which is upstream's behaviour
//! and the one way a cancelled run does not finish promptly.
//!
//! None of that is paid for with `unsafe`: the executor has none at all, and
//! the crate's only `unsafe` blocks are in [`notifier`](executor::notifier),
//! in the lock-free waiter stack, where a worker about to park publishes its
//! [`Thread`](std::thread::Thread) handle into a slot only the notifier that
//! has popped it may read. A scheduler of this shape usually has more; this
//! one does without because a queued job *owns* everything it names rather
//! than pointing at it.
//!
//! What is still out is the two steps that follow: observers and the
//! algorithms -- partitioners, `for_each`, `reduce`, `sort` -- and then
//! pipelines, the one family with no Rust equivalent. The README's "Order of
//! work" section is the sequence the whole port follows, and reading what is
//! next there beats a list here that would go stale the week after it was
//! written.
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

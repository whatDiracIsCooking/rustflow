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
//! Nothing so far watches the run itself, and [`observer`] is where a caller
//! does: an [`Observer`](observer::Observer) is a hook the executor calls on
//! the worker about to run a task and again on the one that has just run it,
//! [`Executor::make_observer`] registers one and hands back the share the
//! executor keeps, and the callbacks are given a
//! [`WorkerView`](observer::WorkerView) and a
//! [`TaskView`](observer::TaskView) rather than the worker and the node
//! themselves, so an observer cannot reach into a run under way. Two are
//! built in over one shared timeline:
//! [`ChromeObserver`](observer::chrome::ChromeObserver) writes the
//! trace-event JSON `about://tracing` loads directly, and
//! [`TfProfObserver`](observer::tfprof::TfProfObserver) the binary block
//! upstream's own profiler viewer reads, plus a text summary of the same
//! timeline.
//!
//! [`algorithm`] is the parallel algorithm family -- `for_each`,
//! `for_each_index`, `transform`, `fill`, `generate`, `reduce`, `find_if`,
//! `min_element`, `sort`, the inclusive and exclusive scans, and `merge` --
//! and it reaches a caller in layers rather than in one shape. A method on
//! [`Runtime`] is the borrowed form, called from a body that already holds
//! the `&mut [T]`; a wrapper on [`FlowBuilder`], carried by the one
//! [`AlgorithmBuilder`](algorithm::AlgorithmBuilder) extension trait, makes
//! a task of the same algorithm over data it owns; and a `make_*_task`
//! factory hands back the `'static` callable upstream treats as the
//! primitive, for a submission with no builder behind it such as a
//! dependent-async task. All of them carve `[0, n)` into chunks through a
//! [`Partitioner`](algorithm::partitioner::Partitioner) -- static, guided,
//! dynamic or random -- and spawn those chunks under a [`Scope`] rather than
//! handing out aliasing indices into one `&mut`, which is where this port
//! and upstream's raw iterators part company; an index-driven one walks an
//! [`IndexRange`](algorithm::index_range::IndexRange) in place of a slice.
//! `DESIGN.md`'s "Three layers, not two forms" entry, under "Algorithms",
//! has why each layer stays.
//!
//! [`pipeline`] is the family with no Rust equivalent to lean on. A
//! [`Pipeline`](pipeline::Pipeline) is a fixed chain of
//! [`Pipe`](pipeline::Pipe)s, each serial or parallel, run over a bounded
//! number of lines: a token enters at the first pipe and travels the chain,
//! every pipe callable is handed a [`Pipeflow`](pipeline::Pipeflow) naming
//! the token, the pipe and the line it is on, and the first pipe ends the
//! stream by calling [`stop`](pipeline::Pipeflow::stop) on it.
//! [`DataPipeline`](pipeline::DataPipeline) is a typed builder over the same
//! scheduler for pipes that pass data from each to the next instead of only
//! a token. Neither runs itself: both compose into a graph as a module task
//! through [`Composable`], which is also how one pipeline nests inside
//! another.
//!
//! # Where the names live
//!
//! Every name here has a module that owns it, and the crate root re-exports
//! the ones a caller writes: a type a caller constructs, a type a public API
//! hands them or asks of them, and a trait whose methods they call or
//! implement. A *family* -- `algorithm`, `observer`, `pipeline` -- is not
//! flattened here but at its own front door, so [`pipeline::Pipeline`] and
//! `observer`'s types are reached through their module; a part of the
//! scheduler no program outside the crate drives, such as a
//! [`Notifier`](executor::notifier::Notifier), keeps its module path, where
//! the docs explaining it are. [`prelude`] is the smaller list inside that
//! one: the names a program's first page spells, which [`prelude`]'s own docs
//! state as a rule. `DESIGN.md`'s entry on the prelude and the crate root has
//! why both lists are drawn where they are, and which re-export it leaves to
//! be withdrawn.
//!
//! # Status
//!
//! All seven steps of the README's "Order of work" are in: everything above
//! is built and tested against the upstream unit tests it was translated
//! from, the observers, the algorithm family and the pipelines included. The
//! port is at parity with upstream's user-facing surface at the pin -- every
//! public class, free function and example `taskflow/` ships there has a
//! counterpart here or a recorded reason it does not, which the README
//! lists. Two consolidation milestones punctuate the steps, one between
//! steps 5 and 6 and one after the parity pass, and both have run. Most of
//! what they change is what a reader of the source meets: the async family's
//! public methods are one-line delegations onto a single core on the
//! executor's shared state, the run's machinery is the `executor::topology`
//! module tree rather than one file, the graph is the [`graph`] module tree,
//! and every doc comment in the crate is held to the documentation contract
//! in `DESIGN.md`. The second reaches the surface as well: it settles this
//! crate root's own namespace and [`prelude`]'s, which "Where the names live"
//! above states as a rule; the algorithm family's eight extension traits are
//! one [`AlgorithmBuilder`](algorithm::AlgorithmBuilder) with a `make_*_task`
//! factory behind every wrapper, so a dependent-async body can reach the
//! algorithms too; every async submission has a `_named` twin; and two
//! scheduler defects its own panic matrix found -- a dispatch claim a bypass
//! could leak, and an observer hook whose panic killed a worker -- are fixed.
//! `DESIGN.md`'s entries for those are under "Documentation and tooling",
//! "Algorithms" and "Executor core".
//!
//! The crate's `unsafe` blocks live in two modules and nowhere else:
//! [`notifier`](executor::notifier), in the lock-free waiter stack, and
//! [`scope`](executor::scope), where [`Runtime::scope`] erases a borrowed
//! child closure's lifetime and is sound only because it joins every child
//! before returning. The scheduler itself has none. `DESIGN.md` is why the
//! port is shaped the way it is: one entry per decision, grouped by the part
//! of the port it decides and each naming the issue that took it, the two
//! consolidations' among them.
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

pub mod algorithm;
pub mod async_task;
pub mod dump;
pub mod executor;
pub mod flow_builder;
pub mod graph;
pub mod observer;
pub mod pipeline;
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
pub use crate::executor::rng::{Xorshift32, Xorshift64};
pub use crate::executor::run_handle::{Canceller, RunError, RunHandle};
pub use crate::executor::scope::Scope;
pub use crate::executor::subflow::JoinError;
pub use crate::executor::worker::{Worker, WorkerInterface};
pub use crate::flow_builder::{Callables, Composable, FlowBuilder};
pub use crate::graph::{
    AsyncWork, Graph, Handle, Node, OnceWork, RuntimeWork, SubflowWork, Task, Work,
};
pub use crate::runtime::Runtime;
pub use crate::semaphore::{Semaphore, SemaphoreError};
pub use crate::subflow::Subflow;
pub use crate::task::{TaskMut, TaskRef, TaskType};
pub use crate::task_group::TaskGroup;
pub use crate::taskflow::Taskflow;

/// The names a program writes on its first page: everything it takes to build
/// a taskflow, submit it to an executor and read the run's outcome.
///
/// That is the whole of the rule, and it is a boundary as much as a list: a
/// name is in the prelude if a program spells it while building a graph with
/// [`Taskflow`] and running it with [`Executor::run`] -- the graph vocabulary,
/// the two task handles, the body parameters the dynamic task kinds hand their
/// closures, the run's outcome, and every trait whose methods that program
/// calls, since Rust will not call a trait method whose trait is not in scope.
/// Any other entry point into the crate is a feature a program chooses -- the
/// async family, task groups, semaphores, scoped spawns, observers, pipelines,
/// the algorithms' free-function and partitioner forms -- and its names are
/// imported by name from the crate root or from the family's own module at the
/// point the program chooses it. `DESIGN.md`'s entry on the prelude and the
/// crate root has why this reading and not the narrower one.
///
/// ```
/// use rustflow::prelude::*;
///
/// // Building: `Taskflow` implements `FlowBuilder`, and every algorithm's
/// // wrapper comes from one extension trait over the same builder.
/// fn builds_everything<B>(_builder: &B)
/// where
///     B: FlowBuilder + AlgorithmBuilder,
/// {
/// }
///
/// let mut taskflow = Taskflow::with_name("first page");
/// builds_everything(&taskflow);
///
/// let a: Task = taskflow.emplace(|| println!("A"));
/// let b = taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
///     subflow.emplace(|| println!("B1"));
/// });
/// let c = taskflow.emplace_runtime(|_runtime: &mut Runtime<'_>| println!("C"));
/// let mut handle: TaskMut<'_> = taskflow.task(b);
/// handle.name("B").succeed([a]).precede([c]);
///
/// // Inspecting what was built.
/// let view: TaskRef<'_> = taskflow.task_ref(a);
/// assert_eq!(view.task_type(), TaskType::Static);
/// let graph: &Graph = taskflow.graph();
/// let node: &Node = graph.node(a);
/// let Handle::Static(work) = node.handle() else { unreachable!() };
/// let _: &Work = work;
///
/// // Running it, and reading the outcome.
/// let executor = Executor::new(2);
/// let run: RunHandle<'_> = executor.run(&taskflow);
/// let _: Canceller = run.canceller();
/// let outcome: Result<(), RunError> = run.wait();
/// assert!(outcome.is_ok());
/// ```
pub mod prelude {
    pub use crate::algorithm::AlgorithmBuilder;
    pub use crate::executor::Executor;
    pub use crate::executor::run_handle::{Canceller, RunError, RunHandle};
    pub use crate::flow_builder::FlowBuilder;
    pub use crate::graph::{Graph, Handle, Node, Task, Work};
    pub use crate::runtime::Runtime;
    pub use crate::subflow::Subflow;
    pub use crate::task::{TaskMut, TaskRef, TaskType};
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

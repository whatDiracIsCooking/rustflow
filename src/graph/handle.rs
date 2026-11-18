//! Translated from `taskflow/core/graph.hpp` (Taskflow v4.1.0).
//!
//! Upstream's `Node::handle_t`: the `std::variant` of what a node does when
//! the executor reaches it.

use std::sync::{Arc, Mutex};

use super::{AsyncWork, Graph, NonpreemptiveRuntimeWork, RuntimeWork, SubflowWork, Work};
use crate::taskflow::Taskflow;

/// What a node does when the executor reaches it.
///
/// Upstream is a `std::variant` of eleven alternatives (subflow, module,
/// async, ...) and this port now has all eleven: [`Handle::NonpreemptiveRuntime`]
/// is upstream's `NonpreemptiveRuntime`, the second of its two runtime
/// shapes, which arrived separately from [`Handle::Runtime`] rather than at
/// the same time -- `DESIGN.md`'s "Decisions taken in M8" has why a
/// pipeline line forced a question the single `Handle::Runtime` protocol
/// could not answer. The enum is `#[non_exhaustive]` so that a twelfth
/// variant, should upstream ever grow one, does not break callers outside
/// the crate.
#[derive(Debug, Default)]
#[non_exhaustive]
pub enum Handle {
    /// A task with no work attached yet, upstream's `std::monostate`.
    #[default]
    Placeholder,
    /// A plain callable, upstream's `Node::Static`.
    Static(Work),
    /// A callable whose return value picks which one of this node's successors
    /// runs next, upstream's `Node::Condition`.
    ///
    /// The index is an `i32` because upstream's is, and that is load-bearing
    /// rather than incidental: a negative or out-of-range value selects
    /// nothing, which is how a condition task ends a branch. Narrowing it to a
    /// successor position is the scheduler's job, through `usize::try_from`
    /// plus a range check and never an `as` cast.
    Condition(Work<i32>),
    /// A callable whose return value picks any number of this node's
    /// successors to run next, upstream's `Node::MultiCondition`.
    ///
    /// `Vec<i32>` stands in for upstream's inline `SmallVector<int>`, the
    /// allocation per invocation being a known cost not yet worth a
    /// small-vector type of our own.
    MultiCondition(Work<Vec<i32>>),
    /// A callable that builds a graph of its own while it runs, which the
    /// executor then runs before this node's successors: upstream's
    /// `Node::Subflow`.
    ///
    /// `subgraph` is upstream's `Graph subgraph` field, behind a `Mutex`
    /// because the body mutates it through a node the run only *borrows*.
    /// [`crate::subflow`] has what holds the guard and why no body can
    /// deadlock on it.
    Subflow {
        /// The body, which is handed the [`Subflow`](crate::Subflow) it
        /// builds into.
        work: SubflowWork,
        /// The graph the body builds, upstream's `Node::Subflow::subgraph`.
        subgraph: Mutex<Graph>,
    },
    /// A callable handed the scheduler itself, upstream's `Node::Runtime`.
    Runtime(RuntimeWork),
    /// A callable handed a restricted scheduler handle that cannot spawn
    /// nested work, upstream's `Node::NonpreemptiveRuntime`.
    ///
    /// Where [`Handle::Runtime`]'s dispatch arm sets `PREEMPTED` before the
    /// body runs so that a child the body spawns can resume it,
    /// this one never sets the bit at all and always finishes on its first
    /// entry -- there is no protocol to resume, because the handle the body
    /// is given has no way to spawn anything under this node.
    /// `preempts_itself` answers `false` for it accordingly, which is what
    /// makes [`Runtime::schedule`](crate::Runtime::schedule) targeting one
    /// safe at any point in its body's run; `DESIGN.md`'s "Decisions taken in
    /// M8" has the failure that this variant exists to close.
    ///
    /// Built through [`FlowBuilder::emplace_nonpreemptive_runtime`](crate::FlowBuilder::emplace_nonpreemptive_runtime).
    /// The restricted handle itself is not part of this crate's public
    /// surface -- a caller's body closure is handed one without ever
    /// needing to name its type, the same way a closure passed to a generic
    /// `Fn` bound never needs to.
    NonpreemptiveRuntime(NonpreemptiveRuntimeWork),
    /// A task that stands for another whole taskflow, upstream's
    /// `Node::Module`.
    ///
    /// The module *shares* ownership of its target where upstream stores a
    /// bare `Graph&` -- `DESIGN.md` has why. The run needs no more than the
    /// share: the `Arc` hands out `&Graph` through [`Taskflow::graph`], and
    /// running the target writes its nodes only through their atomic state
    /// words.
    Module(Arc<Taskflow>),
    /// A task that stands for a graph it owns, upstream's
    /// `Node::AdoptedModule`: the `adopt(Graph&&)` form of the variant above,
    /// which moves the graph in rather than sharing it.
    AdoptedModule(Graph),
    /// A task submitted outside any graph, upstream's `Node::Async`: the body
    /// a `silent_async` or `async` call hands the pool, which has no
    /// dependencies and no successors.
    ///
    /// The node such a task is lives in a one-node graph of its own rather
    /// than in a taskflow's arena -- `executor::async_node` owns it -- so
    /// this variant is the *handle* of an async task and not a way to build
    /// one: nothing on [`FlowBuilder`](crate::FlowBuilder) produces it. One
    /// put in a taskflow's graph through [`Graph::add_node`] anyway dumps as
    /// a plain node, and the worker that reaches it refuses the run.
    Async(AsyncWork),
    /// A task submitted outside any graph that other such tasks can depend
    /// on, upstream's `Node::DependentAsync`.
    ///
    /// The same body as [`Handle::Async`] over the same free-standing node,
    /// differing in who may wait for it and in what the node keeps besides
    /// its body: a dependent-async node holds the successors upstream keeps
    /// in `_edges` under its `ESTATE::LOCKED` spin bit, and hands out an
    /// [`AsyncTask`](crate::AsyncTask) handle so that a later submission can
    /// name it as a dependency. It is no more a taskflow's citizen than
    /// [`Handle::Async`] is.
    DependentAsync(AsyncWork),
}

impl Handle {
    /// Returns `true` for [`Handle::Placeholder`].
    #[must_use]
    pub fn is_placeholder(&self) -> bool {
        matches!(self, Self::Placeholder)
    }

    /// Returns `true` for [`Handle::Static`].
    #[must_use]
    pub fn is_static(&self) -> bool {
        matches!(self, Self::Static(_))
    }

    /// Returns `true` for [`Handle::Condition`].
    #[must_use]
    pub fn is_condition(&self) -> bool {
        matches!(self, Self::Condition(_))
    }

    /// Returns `true` for [`Handle::MultiCondition`].
    #[must_use]
    pub fn is_multi_condition(&self) -> bool {
        matches!(self, Self::MultiCondition(_))
    }

    /// Returns `true` for [`Handle::Subflow`].
    #[must_use]
    pub fn is_subflow(&self) -> bool {
        matches!(self, Self::Subflow { .. })
    }

    /// Returns `true` for [`Handle::Runtime`].
    #[must_use]
    pub fn is_runtime(&self) -> bool {
        matches!(self, Self::Runtime(_))
    }

    /// Returns `true` for [`Handle::NonpreemptiveRuntime`].
    #[must_use]
    pub fn is_nonpreemptive_runtime(&self) -> bool {
        matches!(self, Self::NonpreemptiveRuntime(_))
    }

    /// Returns `true` for either module handle, [`Handle::Module`] and
    /// [`Handle::AdoptedModule`].
    ///
    /// The two are one task kind outside the executor, as upstream's
    /// `TaskType` has no separate name for the adopted form either: they
    /// differ only in who owns the target graph. Match the handle to tell
    /// them apart.
    #[must_use]
    pub fn is_module(&self) -> bool {
        matches!(self, Self::Module(_) | Self::AdoptedModule(_))
    }

    /// Returns `true` for [`Handle::Async`].
    #[must_use]
    pub fn is_async(&self) -> bool {
        matches!(self, Self::Async(_))
    }

    /// Returns `true` for [`Handle::DependentAsync`].
    ///
    /// Separate from [`Handle::is_async`], where
    /// [`is_module`](Handle::is_module) folds its two variants together: the
    /// two async forms differ in whether anything can depend on them, which
    /// is a question a caller holding an [`AsyncTask`](crate::AsyncTask)
    /// asks. The flat [`TaskType`](crate::TaskType) folds them all the same,
    /// as upstream's does.
    #[must_use]
    pub fn is_dependent_async(&self) -> bool {
        matches!(self, Self::DependentAsync(_))
    }

    /// Returns `true` if this handle picks its own successors at runtime,
    /// either kind of condition task. Upstream's `Node::_is_conditioner`.
    ///
    /// This is the predicate that makes a dependency *weak*: a task preceded
    /// by a conditioner may never run at all, so it cannot be waited for the
    /// way a strong dependency is.
    #[must_use]
    pub fn is_conditioner(&self) -> bool {
        self.is_condition() || self.is_multi_condition()
    }
}

//! Translated from `taskflow/core/graph.hpp` (Taskflow v4.1.0).
//!
//! Upstream's `Node::handle_t`: the `std::variant` of what a node does when
//! the executor reaches it.
//!
//! # Design
//!
//! **All eleven of upstream's alternatives are here, and the enum is
//! `#[non_exhaustive]` anyway.** [`Handle::NonpreemptiveRuntime`] is
//! upstream's `NonpreemptiveRuntime`, the second of its two runtime shapes,
//! which arrived separately from [`Handle::Runtime`] rather than at the same
//! time -- `DESIGN.md`'s "Pipelines" section has why a pipeline line
//! forced a question the single [`Handle::Runtime`] protocol could not
//! answer. A twelfth variant, should upstream ever grow one, therefore
//! breaks no caller outside the crate.
//!
//! **A conditioner's index is an `i32`.** Upstream's is, and that is
//! load-bearing rather than incidental: a negative or out-of-range value
//! selects nothing, which is how a condition task ends a branch. Narrowing
//! it to a successor position is the scheduler's job, through
//! `usize::try_from` plus a range check and never an `as` cast.
//! [`Handle::MultiCondition`]'s `Vec<i32>` stands in for upstream's inline
//! `SmallVector<int>`, the allocation per invocation being a known cost not
//! yet worth a small-vector type of our own.
//!
//! **A subflow node's subgraph sits behind a `Mutex`** because the body
//! mutates it through a node the run only *borrows*. [`crate::subflow`] has
//! what holds the guard and why no body can deadlock on it.
//!
//! **[`Handle::NonpreemptiveRuntime`] never preempts its own node.** Where
//! [`Handle::Runtime`]'s dispatch arm sets `PREEMPTED` before the body runs
//! so that a child the body spawns can resume it, this one never sets the
//! bit at all and always finishes on its first entry -- there is no protocol
//! to resume, because the handle the body is given has no way to spawn
//! anything under this node. `preempts_itself` answers `false` for it
//! accordingly, which is what makes
//! [`Runtime::schedule`](crate::Runtime::schedule) targeting one safe at any
//! point in its body's run. That handle's own type is not part of this
//! crate's public surface: a caller's body closure is handed one without
//! ever naming its type, the same way a closure passed to a generic `Fn`
//! bound never needs to.
//!
//! **A module task shares its target where upstream stores a bare
//! `Graph&`** -- `DESIGN.md` has why, under "A module task reaches its
//! target through whatever owns it". The run needs no more than the share:
//! the `Arc` hands out `&Graph` through [`Taskflow::graph`], and running the
//! target writes its nodes only through their atomic state words.
//!
//! **The two async variants are a node's *handle*, not a way to build one.**
//! The node such a task is lives in a one-node graph of its own rather than
//! in a taskflow's arena -- `executor::async_node` owns it -- so nothing on
//! [`FlowBuilder`](crate::FlowBuilder) produces either. They differ in who
//! may wait for the task and in what the node keeps besides its body: a
//! dependent-async node holds the successors upstream keeps in `_edges`
//! under its `ESTATE::LOCKED` spin bit, and hands out an
//! [`AsyncTask`](crate::AsyncTask) handle so that a later submission can
//! name it as a dependency. That is why [`Handle::is_dependent_async`] is
//! separate where [`Handle::is_module`] folds its two variants together; the
//! flat [`TaskType`](crate::TaskType) folds all four, as upstream's does.

use std::sync::{Arc, Mutex};

use super::{AsyncWork, Graph, NonpreemptiveRuntimeWork, RuntimeWork, SubflowWork, Work};
use crate::taskflow::Taskflow;

/// What a node does when the executor reaches it: upstream's `Node::handle_t`,
/// a `std::variant` of eleven alternatives, all of which this port has, plus
/// `#[non_exhaustive]` -- so a match outside the crate needs a wildcard arm.
#[derive(Debug, Default)]
#[non_exhaustive]
pub enum Handle {
    /// A task with no work attached yet, upstream's `std::monostate`.
    #[default]
    Placeholder,
    /// A plain callable, upstream's `Node::Static`.
    Static(Work),
    /// A callable whose return value picks which one of this node's successors
    /// runs next, upstream's `Node::Condition`. The index is an `i32`, for
    /// the reason the module's `# Design` section gives.
    Condition(Work<i32>),
    /// A callable whose return value picks any number of this node's
    /// successors to run next, upstream's `Node::MultiCondition`.
    MultiCondition(Work<Vec<i32>>),
    /// A callable that builds a graph of its own while it runs, which the
    /// executor then runs before this node's successors: upstream's
    /// `Node::Subflow`.
    Subflow {
        /// The body, handed the [`Subflow`](crate::Subflow) it builds into.
        work: SubflowWork,
        /// The graph the body builds, upstream's `Node::Subflow::subgraph`,
        /// behind the `Mutex` the module's `# Design` section explains.
        subgraph: Mutex<Graph>,
    },
    /// A callable handed the scheduler itself, upstream's `Node::Runtime`.
    Runtime(RuntimeWork),
    /// A callable handed a restricted scheduler handle that cannot spawn
    /// nested work, upstream's `Node::NonpreemptiveRuntime`: it never
    /// preempts its own node, which the module's `# Design` section is on.
    ///
    /// Built through [`FlowBuilder::emplace_nonpreemptive_runtime`](crate::FlowBuilder::emplace_nonpreemptive_runtime).
    NonpreemptiveRuntime(NonpreemptiveRuntimeWork),
    /// A task that stands for another whole taskflow, upstream's
    /// `Node::Module`, of which it *shares* ownership: the module's
    /// `# Design` section has what the share does and does not buy.
    Module(Arc<Taskflow>),
    /// A task that stands for a graph it owns, upstream's
    /// `Node::AdoptedModule`: the `adopt(Graph&&)` form of the variant above,
    /// which moves the graph in rather than sharing it.
    AdoptedModule(Graph),
    /// A task submitted outside any graph, upstream's `Node::Async`: the body
    /// a `silent_async` or `async` call hands the pool, which has no
    /// dependencies and no successors. Nothing on
    /// [`FlowBuilder`](crate::FlowBuilder) builds one, and one put in a
    /// taskflow's graph through [`Graph::add_node`] anyway dumps as a plain
    /// node and is refused by the worker that reaches it.
    Async(AsyncWork),
    /// A task submitted outside any graph that other such tasks can depend
    /// on, upstream's `Node::DependentAsync`: [`Handle::Async`]'s body over
    /// the same free-standing node, differing in who may wait for it and in
    /// what the node keeps besides it. No more a taskflow's citizen either.
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
    /// [`Handle::AdoptedModule`]: one task kind outside the executor, as
    /// upstream's `TaskType` has no separate name for the adopted form
    /// either. Match the handle to tell the two apart.
    #[must_use]
    pub fn is_module(&self) -> bool {
        matches!(self, Self::Module(_) | Self::AdoptedModule(_))
    }

    /// Returns `true` for [`Handle::Async`].
    #[must_use]
    pub fn is_async(&self) -> bool {
        matches!(self, Self::Async(_))
    }

    /// Returns `true` for [`Handle::DependentAsync`] alone, and not for
    /// [`Handle::Async`]: the module's `# Design` section has why.
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

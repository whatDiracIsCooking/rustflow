//! A Rust port of [Taskflow](https://github.com/taskflow/taskflow), a C++
//! task-parallel programming system built around task dependency graphs.
//!
//! # Status
//!
//! The static task graph is the first slice: a [`Taskflow`] owns a [`Graph`]
//! of [`Node`]s, each addressed by a [`Task`] index into that graph's arena.
//! The task-building API and the work-stealing executor that runs the graph
//! are still to come.
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
pub use crate::executor::queue::{Buffers, Thief, WorkerQueue};
pub use crate::executor::rng::{Xorshift32, Xorshift64};
pub use crate::executor::run_handle::{RunError, RunHandle};
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

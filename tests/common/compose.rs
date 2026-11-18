//! Translated from `unittests/test_pipelines.cpp` (Taskflow v4.1.0).
//!
//! Putting a pipeline on a graph: upstream's
//! `taskflow.composed_of(pl).name("module_of_pipeline")`, which every one of
//! its pipeline cases -- untyped, typed and scalable alike -- opens with.
//!
//! A topic of its own for the reason [`super`]'s docs give: this is the one
//! thing all six pipeline suites here do, where the buffer and the pipe
//! builders beside it belong to the untyped families only.

use rustflow::flow_builder::Composable;
use rustflow::prelude::*;

/// Composes `target` into `builder` as a module task called `name`, and hands
/// back the task so that a case can wire it to the rest of its graph.
///
/// Generic over the builder, not a `&mut Taskflow`, because one of these
/// pipelines is composed into a `Subflow` instead -- which is the whole point
/// of upstream's `FlowBuilder` being the base of both.
///
/// The name is the caller's: upstream's composition families number theirs
/// -- `module_of_pipeline1` and so on -- and a dump of the graph is read by
/// those names.
pub fn compose_module(builder: &mut impl FlowBuilder, target: impl Composable, name: &str) -> Task {
    let module = builder.composed_of(target);
    builder.task(module).name(name);
    module
}

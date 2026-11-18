//! The pipeline family's value types: the scheduling token a pipe callable
//! is handed ([`pipe::Pipeflow`]), a pipe's direction ([`pipe::PipeType`])
//! and a pipe itself ([`pipe::Pipe`]).
//!
//! This is the crate's own module tree rather than a translation of a
//! single upstream header; [`pipe`]'s own `//!` doc says which upstream file
//! it translates. The scheduler that drives tokens through a sequence of
//! pipes is built on these shapes and is not here yet.

pub mod pipe;
// Declared under a name other than `pipeline`, despite living in
// `pipeline.rs`: the obvious `pub mod pipeline;` here names a module
// `pipeline` directly inside a module ALSO named `pipeline`, which is
// exactly `clippy::module_inception` -- warn-by-default and so a failure
// under this crate's `-D warnings` gate, with no `#[allow]` admitted for it
// (the crate's one sanctioned exception is `src/algorithm/partitioner.rs`,
// documented in `DESIGN.md`, and this is not it).
#[path = "pipeline.rs"]
pub mod scheduler;

//! The pipeline family's module tree: [`pipe`]'s value types -- the
//! scheduling token a pipe callable is handed ([`pipe::Pipeflow`]), a pipe's
//! direction ([`pipe::PipeType`]) and a pipe itself ([`pipe::Pipe`]) --
//! [`scheduler`]'s [`scheduler::Pipeline`], which drives a stream of tokens
//! through a chain of them, and [`data_pipeline`]'s
//! [`data_pipeline::DataPipeline`], a typed builder over that same scheduler
//! for pipes that pass data from each to the next instead of only a
//! [`pipe::Pipeflow`].
//!
//! This is the crate's own module tree rather than a translation of a
//! single upstream header; each module's own `//!` doc names the upstream
//! file it translates.
//!
//! The names a pipeline program writes are re-exported here, flat, so that a
//! call site spells [`Pipeline`] and [`DataPipeline`] rather than the module
//! each lives in: this module is the family's front door, which is where the
//! crate flattens a family instead of at the crate root. `DESIGN.md`'s
//! "Decisions taken in M10" entry has the rule and why the root is not the
//! place for it.

pub mod data_pipeline;
pub mod pipe;
pub mod scheduler;

pub use crate::pipeline::data_pipeline::{
    DataPipe, DataPipeline, DataPipelineBuilder, parallel_pipe, parallel_pipe_with_flow,
    serial_pipe, serial_pipe_with_flow,
};
pub use crate::pipeline::pipe::{Pipe, PipeType, Pipeflow};
pub use crate::pipeline::scheduler::Pipeline;

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

pub mod data_pipeline;
pub mod pipe;
pub mod scheduler;

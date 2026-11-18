//! The pipeline family's value types: the scheduling token a pipe callable
//! is handed ([`pipe::Pipeflow`]), a pipe's direction ([`pipe::PipeType`])
//! and a pipe itself ([`pipe::Pipe`]).
//!
//! This is the crate's own module tree rather than a translation of a
//! single upstream header; [`pipe`]'s own `//!` doc says which upstream file
//! it translates. [`scheduler`] is the scheduler that drives tokens through
//! a sequence of pipes, built on these shapes.

pub mod pipe;
pub mod scheduler;

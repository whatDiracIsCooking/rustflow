//! Index-driven algorithms: the index-range types they partition work over
//! and the partitioning strategies that drive them.
//!
//! This is the crate's own module tree, not a translation of a single
//! upstream header; each submodule's own `//!` doc says which upstream file
//! it translates, if any.
//!
//! - [`index_range`] the one-dimensional index range an algorithm walks and
//!   partitions, `[begin, end)` in steps of `step_size`.
//! - [`partitioner`] the strategies that split `[0, n)` into chunks for a
//!   parallel loop -- static, guided, dynamic and random -- and the closure
//!   wrapper a caller can run around every chunk's body.

pub mod index_range;
pub mod partitioner;

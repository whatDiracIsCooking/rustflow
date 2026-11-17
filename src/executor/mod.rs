//! The work-stealing executor that runs a [`Taskflow`](crate::Taskflow).
//!
//! The layer is built bottom-up, one submodule at a time: [`queue`] holds the
//! queues a worker owns and the overflow buffers non-worker threads push into,
//! and [`notifier`] the sleep/wake protocol that idles a worker which finds no
//! work. Upstream keeps the whole scheduler in `taskflow/core/executor.hpp`
//! with the queues in `taskflow/core/wsq.hpp`; splitting it into submodules
//! here is our own arrangement, so see each submodule for what it does and does
//! not owe to upstream.

pub mod notifier;
pub mod queue;

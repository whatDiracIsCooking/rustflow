//! What the benchmark targets share: the worker counts every table is run at
//! and the executor each row of one is measured on. The rayon baseline's pool
//! and the per-task body live in the topic files beside this one, which a
//! target pulls by path.
//!
//! # Design
//!
//! Every file directly under `benches/` is its own crate, so each target
//! compiles its own copy of whatever it declares here -- and an item a crate
//! compiles but never calls is dead code, which this crate's lints make a
//! build failure in the gate rather than a warning to live with.
//! `#[allow(dead_code)]` over a shared module would hide the one thing such a
//! module most needs to stay honest about: whether a helper still has users.
//!
//! So the split is by what a target *needs*, not by what a helper *is* --
//! `tests/common/mod.rs` settled that shape for the integration suites and
//! this module keeps it. This file holds only what every target that declares
//! it uses, and each other helper lives in a topic file pulled by path:
//!
//! ```ignore
//! mod common;
//! #[path = "common/baseline.rs"]
//! mod baseline;
//! ```
//!
//! `linear_chain.rs` has no rayon column and `sort.rs`'s elements are not
//! tasks with a body, so neither target compiles the other's helper. A new
//! helper belongs in the file whose every user wants all of it; when that is
//! no existing file, it is a new one.
//!
//! What is deliberately *not* here is anything that would move a measurement:
//! each target keeps its own sizes, its own criterion sample settings and its
//! own one-off setup (`sort.rs`'s `iter_batched` shuffle,
//! `data_pipeline.rs`'s per-line slot), because those are what its table
//! reports rather than what its neighbours share.

use std::num::NonZeroUsize;
use std::thread::available_parallelism;

use rustflow::Executor;

/// The worker counts every shape is measured at: one, and the machine's own.
///
/// The two ends of upstream's `-t` option, which defaults to `1`. They
/// collapse to a single measurement on a single-core box, so a table has one
/// row per entry rather than always two.
pub fn worker_counts() -> Vec<usize> {
    let parallel = available_parallelism().map_or(1, NonZeroUsize::get);
    if parallel > 1 {
        vec![1, parallel]
    } else {
        vec![1]
    }
}

/// The executor every `rustflow` id in one row of a table is measured on.
///
/// One per worker count, built *outside* the measured region as upstream's
/// function-static `tf::Executor` is: a caller holds it across the whole row
/// rather than building one per id, so that no id pays for starting a pool.
pub fn executor(workers: usize) -> Executor {
    Executor::new(workers)
}

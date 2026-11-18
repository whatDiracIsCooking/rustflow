//! The pool the rayon baseline of a row runs on.
//!
//! A topic of its own rather than part of [`super`], for the reason that
//! module's docs give: `linear_chain.rs` has no rayon column at all and the
//! two pipeline shapes compare against `std::sync::mpsc` and against each
//! other instead, so compiling a pool builder they never call would be dead
//! code.

/// A rayon pool of exactly `workers` threads.
///
/// rayon's answer to TBB's `global_control(max_allowed_parallelism, n)`: it is
/// what holds the baseline to the same worker count the `rustflow` ids beside
/// it are measured at. A caller runs its baseline inside this pool's
/// `install`, since without it the work would go to rayon's global pool and
/// ignore the count the row is measuring.
pub fn rayon_pool(workers: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .expect("a rayon pool of the requested size")
}

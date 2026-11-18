//! Translated from `benchmarks/embarrassing_parallelism/embarrassing_parallelism.hpp` (Taskflow v4.1.0).
//!
//! Upstream's `dummy`, the per-task floating-point body the shapes that need
//! one measure. Upstream carries the same loop twice -- `levelgraph.hpp`'s
//! `Node::mark` is `dummy` keyed on `_idx`, with a `_visited` flag and a BFS
//! around it -- so `graph_traversal.rs` reads it from here too.
//!
//! A topic of its own rather than part of [`super`], for the reason that
//! module's docs give: `linear_chain.rs`'s tasks are a counter increment and
//! nothing else, and `sort.rs` has no per-task body at all, so compiling this
//! loop into them would be dead code.

use std::hint::black_box;

/// Upstream's `dummy`: a small, non-foldable amount of FP work per task.
///
/// The `volatile double sink` upstream uses to keep the compiler honest is a
/// [`black_box`] here -- the loop is 32 transcendental calls whose result
/// nothing reads, which both compilers are entitled to delete outright.
pub fn dummy(i: u64) -> f64 {
    // Upstream's mix, including its wrap: `i * 1315423911u` on a `size_t`.
    // The `as f64` loses nothing a benchmark needs -- the value exists only so
    // that the loop below cannot be constant-folded.
    let mut x = (i.wrapping_mul(1_315_423_911) ^ (i >> 3)) as f64 + 1.0;
    let mut sink = 0.0_f64;

    for _ in 0..32 {
        x = x.sin() * 1.000_000_1 + x.cos();
        sink = black_box(sink + x);
    }

    sink
}

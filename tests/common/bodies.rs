//! Task bodies that do nothing but count themselves, which is what most of
//! upstream's cases attach to a task.
//!
//! Separate from `common/counters.rs` because a suite can want one without
//! the other: `tests/semaphores.rs` counts, but every body it attaches also
//! raises and lowers its gauge, so it has no use for [`bump`] -- and
//! [`super`]'s docs say why compiling an unused helper is not an option here.
//!
//! The counter is spelled `Arc<AtomicUsize>` rather than `counters::Counter`
//! so that each topic file stands alone and a suite can pull this one
//! without the other; the two are the same type.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A body that counts itself: upstream's `[&counter](){ ++counter; }`.
pub fn bump(counter: &Arc<AtomicUsize>) -> impl FnMut() + Send + 'static {
    let counter = Arc::clone(counter);
    move || {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

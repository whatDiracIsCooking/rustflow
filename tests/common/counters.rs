//! Counting what a graph ran: the one thing nearly every case here asserts.
//!
//! A topic of its own rather than part of [`super`], for the reason that
//! module's docs give: a suite that counts does not necessarily want the
//! bodies in `common/bodies.rs`, and compiling what it does not call would
//! be dead code.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// How many times a task body has run, shared with that body.
///
/// An atomic where upstream counts with a plain `int`: what makes upstream's
/// sound is the very scheduling the case is testing, so a defect there would
/// be undefined behaviour rather than a failed assertion. Here it is a wrong
/// number, and the assertion is what reports it.
pub type Counter = Arc<AtomicUsize>;

/// A fresh counter, at zero.
pub fn counter() -> Counter {
    Arc::new(AtomicUsize::new(0))
}

/// What `counter` has counted.
///
/// `Relaxed`, as the bodies' increments are: nothing reads a count until the
/// run that produced it has been waited for, and that wait is the ordering.
pub fn count(counter: &Counter) -> usize {
    counter.load(Ordering::Relaxed)
}

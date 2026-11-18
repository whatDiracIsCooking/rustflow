//! Reading a panic back out of a failed run, which is how every case here
//! that expects one asserts that it was *its* panic and not another.
//!
//! A topic of its own for the reason [`super`]'s docs give: a suite either
//! has a case that expects a failure or it does not, and most do not.
//!
//! No provenance line: upstream's own cases catch a `std::exception` by
//! reference and compare its `what()`, where a failure here is a payload
//! `wait` hands back as an `Any`, so what this asserts is upstream's intent
//! in this port's own terms rather than a translation of its text.

use rustflow::RunError;

/// Asserts `error` is a panicked run whose payload is exactly `message`.
///
/// A `match` rather than a destructuring because `RunError` is
/// `#[non_exhaustive]` outside the crate, and the catch-all arm panics with
/// the variant it did get so that a new variant reaching a case that expects
/// a panic reads as that and not as a wrong payload.
pub fn assert_panicked_with(error: RunError, message: &str) {
    match error {
        RunError::Panicked(payload) => {
            assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&message),
                "the payload keeps its identity"
            );
        }
        error => panic!("unexpected failure: {error:?}"),
    }
}

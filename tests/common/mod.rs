//! What the integration suites share: the worker-matrix macro here, and the
//! helpers the suites that need them pull from the topic files beside this
//! one.
//!
//! # Why this is split into topic files
//!
//! Every file directly under `tests/` is its own crate, so each suite
//! compiles its own copy of whatever it declares here -- and an item a crate
//! compiles but never calls is dead code. The crate's lints make that a
//! build failure in the gate rather than a warning to live with, and
//! `#[allow(dead_code)]` over a shared module would hide the one thing this
//! module most needs to stay honest about: whether a helper still has users.
//!
//! So the split is by what a suite *needs*, not by what a helper *is*. This
//! file holds only what every suite that declares it uses -- today the macro
//! below -- and each other helper lives in a topic file a suite pulls by
//! path:
//!
//! ```ignore
//! #[macro_use]
//! mod common;
//! #[path = "common/counters.rs"]
//! mod counters;
//! ```
//!
//! A suite therefore compiles the topics it calls and nothing else, which is
//! why the topics are as narrow as they are: `tests/semaphores.rs` counts
//! what its graphs ran but builds its task bodies around a gauge, so
//! counting (`common/counters.rs`) and body-making (`common/bodies.rs`) are
//! two topics rather than one. A new helper belongs in the file whose every
//! user wants all of it; when that is no existing file, it is a new one.
//!
//! # Why a macro rather than one test over a loop
//!
//! The suites run their case bodies at a sweep of worker counts, which
//! upstream does by registering one `TEST_CASE` per count. A single
//! `#[test]` looping over `[1, 2, 4, 8]` would be fewer lines than either,
//! and is the wrong shape twice over:
//!
//! - **Names.** Each generated test keeps the name upstream's case has, so a
//!   name in a failure report, a `--list` output or a `cargo test <filter>`
//!   means the same thing here as it does in the file this was ported from.
//! - **Attribution.** One test per count fails per count: the report names
//!   the size that broke, the sizes that passed still run, and re-running
//!   just the one is a filter rather than an edit. A loop stops at the first
//!   count that fails and says nothing about the rest.

/// Writes one `#[test]` per entry of a size matrix, each calling `$body`.
///
/// `$body` is the case function the suite already has -- the one upstream
/// takes a worker count or a notifier size -- and each entry is the test's
/// name and the arguments that case takes at that size:
///
/// ```ignore
/// worker_matrix! {
///     starvation_loop {
///         starvation_loop_1_worker(1),
///         starvation_loop_2_workers(2),
///     }
/// }
/// ```
///
/// Each entry spells its test name out rather than having the macro build it
/// from the body's name and the count, because neither half of that is
/// available: `macro_rules!` cannot concatenate identifiers (`concat_idents!`
/// is unstable, and `paste` would be a dependency the README's dependency
/// policy does not admit for this), and upstream's own naming is not a
/// pattern anyway -- `serial_runs_1_thread`, `critical_section_1_worker` and
/// `atomic_notifier_notify_before_commit_5threads` are three spellings of
/// one idea, and the names are upstream's to choose. Writing them out also
/// makes the thing a reviewer has to check -- that no name moved -- visible
/// in the diff rather than inferable from an expansion.
///
/// The arguments are an expression list rather than a bare count for the same
/// reason: a case's size is sometimes the count itself
/// (`sequential_runs(4)`), sometimes a value built from it
/// (`notify_before_commit(&AtomicNotifier::new(4))`), and sometimes a size
/// plus the rounds that size is run for.
macro_rules! worker_matrix {
    (
        $body:ident {
            $( $case:ident ( $( $arg:expr ),* $(,)? ) ),* $(,)?
        }
    ) => {
        $(
            #[test]
            fn $case() {
                $body($($arg),*);
            }
        )*
    };
}

//! Running one case at a time, which is what the pipeline suites need of the
//! test harness and nothing else here does.
//!
//! A topic of its own for the reason [`super`]'s docs give: six suites want
//! this gate and none of them wants the counting, body-making or spinning the
//! other topics beside it carry. `tests/pipelines.rs`'s own module docs have
//! why the gate exists at all -- a case there builds its own multi-worker
//! `Executor` and reruns it a hundred times, and `cargo test` runs dozens of
//! such cases at once -- and this file is only where it lives.

use std::sync::{Mutex, PoisonError};

/// Runs `body` with every other case in the calling suite's own binary held
/// back.
///
/// The gate is one per test binary, as the copy this replaced in each suite
/// was: a suite serialises against itself, never against another suite's
/// process.
///
/// Tolerates a poisoned gate rather than propagating it: the gate protects no
/// data of its own, only serialising, so a case that panicked while holding
/// it must not cascade into every later case failing on the gate itself
/// rather than on whatever it was actually testing.
pub fn serialized<R>(body: impl FnOnce() -> R) -> R {
    static GATE: Mutex<()> = Mutex::new(());
    let _guard = GATE.lock().unwrap_or_else(PoisonError::into_inner);
    body()
}

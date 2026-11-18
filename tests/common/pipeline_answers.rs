//! Translated from `unittests/test_pipelines.cpp` (Taskflow v4.1.0).
//!
//! The two pure functions upstream's `Ifelse` and `PipelineinPipeline`
//! families check a pipeline's output against, worked out independently of
//! the pipeline that produced it. `unittests/test_data_pipelines.cpp` carries
//! its own `ifelse_pipe_ans` with the same three branches, so the untyped and
//! the typed nesting suites share this file.
//!
//! A topic of its own for the reason [`super`]'s docs give: only the two
//! suites that port those families have anything to check against these, and
//! `tests/scalable_pipelines.rs`'s own answer function is upstream's
//! `ifelse_spipe_ans`, phrased as three `let` bindings rather than these
//! three mutations, so it stays where it is.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Upstream's `ifelse_pipe_ans(a)`: the value a three-pipe `Ifelse` pipeline
/// produces for one source value.
///
/// Recomputed here rather than accumulated from the pipeline so that the
/// assertion is on the *branch* each pipe actually took -- the output differs
/// by which arm of each `if` ran -- rather than merely on the run finishing.
pub fn ifelse_pipe_ans(value: i32) -> i32 {
    let mut a = value;
    if a / 2 != 0 {
        a += 8;
    }
    if a > 4897 {
        a -= 1834;
    } else {
        a += 3;
    }
    if (a + 9) / 4 < 50 {
        a += 1;
    } else {
        a += 17;
    }
    a
}

/// `0 + 1 + ... + (sub_n - 1)`: upstream's
/// `std::accumulate(source[token].begin(), source[token].begin() + subN, 0)`.
///
/// Independent of which outer token's row of `source` is read, since every
/// row is upstream's own `std::iota(..., 0)` -- `0, 1, 2, ...` -- so a nested
/// case needs no source data of its own to reproduce one.
pub fn partial_sum(sub_n: usize) -> i32 {
    let sub_n = i32::try_from(sub_n).expect("a sub-token count fits in an i32");
    sub_n * (sub_n - 1) / 2
}

/// Asserts that `collected` is one [`ifelse_pipe_ans`] per source value, in
/// token order: upstream's
/// `for(n) REQUIRE(collection[n] == ifelse_pipe_ans(source[n]))`.
///
/// By index rather than as a sorted set, which the last pipe of an `Ifelse`
/// pipeline being serial is what earns: its tokens reach it in order, so a
/// value landing against the wrong source value is a failure and not a race.
pub fn assert_ifelse_answers(collected: &Mutex<Vec<i32>>, source: &[i32], n: usize) {
    let collected = collected.lock().expect("uncontended");
    assert_eq!(collected.len(), n);
    for (token, &value) in collected.iter().enumerate() {
        assert_eq!(value, ifelse_pipe_ans(source[token]), "token {token}");
    }
}

/// Asserts that each of a sub-pipeline's three pipes ran `sub_n` tokens and
/// that the last one collected `sub_n` values, and hands back their sum:
/// upstream's own checks inside the outer pipe that built and `corun`-ed the
/// sub-pipeline, run before its caller reads the sum back.
pub fn sub_pipeline_sum(
    counters: [&Arc<AtomicUsize>; 3],
    collected: &Mutex<Vec<i32>>,
    sub_n: usize,
) -> i32 {
    for (pipe, counter) in counters.iter().enumerate() {
        assert_eq!(counter.load(Ordering::Relaxed), sub_n, "sub-pipe {pipe}");
    }
    let collected = collected.lock().expect("uncontended");
    assert_eq!(collected.len(), sub_n);
    collected.iter().sum()
}

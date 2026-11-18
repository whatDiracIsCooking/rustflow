//! Translated from `unittests/test_pipelines.cpp` (Taskflow v4.1.0).
//!
//! The graph upstream's `Looping.Pipelines` builds around its one pipeline
//! module, and the bookkeeping its condition task does once a round's
//! assertions have passed: clear what this round counted, grow the next
//! round's token count by one, and answer which successor to take.
//! `unittests/test_data_pipelines.cpp` repeats both unchanged in
//! `Looping.DataPipelines`.
//!
//! A topic of its own for the reason [`super`]'s docs give: the two suites
//! with a looping family are the only callers, and neither wants the other
//! topics' pipe builders or token sweep through this file.
//!
//! # Design
//!
//! **The round's assertions stay in the case; only the clearing moves.**
//! What those seven assertions check -- each pipe ran the round's own number
//! of tokens, each collection holds the right set, and the pipeline's running
//! `num_tokens` is the sum of every round so far -- is the case itself, and a
//! fixture that took them over would have had to take their expected values
//! as arguments to do it. What the two cases genuinely share is the reset,
//! which asserts nothing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::prelude::*;

/// Zeroes this round's counters and collections, advances `round` by one and
/// `cnt` by the round it advanced to, and answers `1` once `rounds` rounds
/// have been run: upstream's own `cnt += ++round; return round >= maxN;`.
///
/// `cnt` is upstream's `cnt`, the running sum of every round's token count,
/// which is what a pipeline that is never reset between rounds has for its
/// `num_tokens` -- the caller asserts that before calling this, since after
/// it the sum is the next round's.
///
/// The return is the index of the successor the condition task takes, so `0`
/// loops back to the pipeline module and `1` falls through to the terminal
/// task.
pub fn advance_round(
    round: &Arc<AtomicUsize>,
    counters: [&Arc<AtomicUsize>; 4],
    collections: [&Arc<Mutex<Vec<i32>>>; 2],
    cnt: &mut usize,
    rounds: usize,
) -> i32 {
    for counter in counters {
        counter.store(0, Ordering::Relaxed);
    }
    for collection in collections {
        collection.lock().expect("uncontended").clear();
    }

    let next_round = round.load(Ordering::Relaxed) + 1;
    round.store(next_round, Ordering::Relaxed);
    *cnt += next_round;

    i32::from(next_round >= rounds)
}

/// Wires upstream's `Looping.*Pipelines` graph around a pipeline's `module`
/// task and the `cond` task that asserts and advances each round: an entry
/// task into the module, and `cond` after the module with the module itself
/// as its first successor and a terminal task as its second.
///
/// `cond` preceding `module` as well as succeeding it is the loop, and the
/// order of its two successors is what [`advance_round`]'s return picks
/// between.
pub fn wire_loop(taskflow: &mut Taskflow, module: Task, cond: Task) {
    let initial = taskflow.emplace(|| {});
    taskflow.task(initial).name("initial").precede([module]);

    let terminal = taskflow.emplace(|| {});
    taskflow.task(terminal).name("terminal");

    taskflow
        .task(cond)
        .name("cond")
        .succeed([module])
        .precede([module, terminal]);
}

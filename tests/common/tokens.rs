//! Translated from `unittests/test_pipelines.cpp` (Taskflow v4.1.0).
//!
//! The token sweep every pipeline family runs, and the one thing a parallel
//! pipe's collected values can be asserted about.
//! `unittests/test_data_pipelines.cpp` repeats both unchanged for its own
//! `DataPipeline` families, so the four suites that port those two files
//! share this file.
//!
//! A topic of its own for the reason [`super`]'s docs give: the suites that
//! nest or rescale a pipeline sweep their own sizes and collect in token
//! order, so neither item here would have a caller in them.

use std::sync::Mutex;

/// Upstream's `maxN`: the largest token count a case sweeps up to.
pub const MAX_N: usize = 100;

/// Asserts that `collected` holds exactly `n` values and, once sorted, is
/// `start, start + 1, ..., start + n - 1`: upstream's `std::sort` plus its
/// loop of `REQUIRE(collection[i] == ...)`.
///
/// Sorting first is what makes this the right check for a parallel pipe: its
/// tokens have no order among themselves, so what a case can pin is the
/// *set* of values that came out, not the sequence.
pub fn assert_consecutive_from(collected: &Mutex<Vec<i32>>, start: i32, n: usize) {
    let mut values = collected.lock().expect("uncontended");
    assert_eq!(values.len(), n);
    values.sort_unstable();
    for (index, &value) in values.iter().enumerate() {
        let offset = i32::try_from(index).expect("a case's token count fits in an i32");
        assert_eq!(value, start + offset);
    }
}

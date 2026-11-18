//! Translated from `examples/parallel_sort.cpp` (Taskflow v4.1.0): upstream
//! picks sequential or parallel from `argv[1]` and only times the one it
//! runs; this example runs both and checks they agree, the way every other
//! port in this milestone turns a command-line choice into a sequence run
//! once.
//!
//! [`SortBuilder::sort`] is the owned-data `FlowBuilder` wrapper, closest to
//! upstream's own `taskflow.sort(strings.begin(), strings.end())`, over an
//! `Arc<Mutex<Vec<String>>>` rather than a borrowed iterator pair.
//! [`Runtime::sort_by`] runs second, from inside a runtime task body, over a
//! second copy of the same strings sorted in reverse by an explicit `cmp` --
//! the slice-borrowing form with no `Arc` or lock, since the data never
//! leaves that body's own frame.
//!
//! Upstream's program only *prints* its timings. This milestone's examples
//! print *and* **assert**: `strings.begin(), strings.end()` sorted is
//! checked against `std::sort`'s own result with `assert_eq!`, so a
//! non-zero exit from the binary is itself the test. See the README's
//! "Examples" section.

use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::prelude::*;

/// Past `rustflow::algorithm::sort`'s own cutoff for a `String`-sized
/// element (2048, per that module's `# Design` table), so the sort actually
/// runs the parallel path -- large enough for that, small enough to keep
/// `run-examples.sh`'s serial suite fast.
const N: usize = 5_000;

/// A tiny xorshift64 generator, standing in for upstream's
/// `std::default_random_engine` -- this crate's dependency policy admits no
/// `rand`-family crate for an example that only needs *varied* strings to
/// sort, not real randomness.
fn next_u64(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// Upstream's `random_string(len)`, over the same alphabet.
fn random_string(state: &mut u64, len: usize) -> String {
    const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let alphabet_len = u64::try_from(ALPHABET.len()).expect("alphabet length fits in u64");
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        let idx = usize::try_from(next_u64(state) % alphabet_len).expect("remainder fits");
        out.push(char::from(ALPHABET[idx]));
    }
    out
}

/// Upstream's `random_strings()`.
fn random_strings(seed: u64, count: usize) -> Vec<String> {
    let mut state = seed;
    (0..count).map(|_| random_string(&mut state, 32)).collect()
}

fn main() {
    sort_builder_matches_std_sort();
    runtime_sort_by_matches_std_sort_unstable_by();
}

/// Upstream's `argv[1][0] == 'p'` branch, via [`SortBuilder::sort`].
fn sort_builder_matches_std_sort() {
    let strings = random_strings(0x5EED_0001, N);
    let mut expected = strings.clone();
    expected.sort();

    let data = Arc::new(Mutex::new(strings));
    let mut taskflow = Taskflow::new();
    taskflow.sort(Arc::clone(&data));
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    assert_eq!(*data.lock().expect("uncontended"), expected);
}

/// The same strings sorted in reverse, via [`Runtime::sort_by`] called
/// directly from a runtime task body on data that body owns outright.
fn runtime_sort_by_matches_std_sort_unstable_by() {
    let strings = random_strings(0x5EED_0002, N);
    let mut expected = strings.clone();
    expected.sort_unstable_by(|a: &String, b: &String| b.cmp(a));

    let mut taskflow = Taskflow::new();
    taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
        // Cloned again rather than moved: `emplace_runtime` wants `FnMut`,
        // so a revisited task needs `strings` to still be here next time.
        let mut local = strings.clone();
        runtime.sort_by(&mut local, |a: &String, b: &String| b.cmp(a));
        assert_eq!(local, expected);
    });
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("no task panicked");
}

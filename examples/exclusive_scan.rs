//! Translated from `examples/exclusive_scan.cpp` (Taskflow v4.1.0), with the
//! same change `examples/inclusive_scan.rs` makes and explains in its own
//! doc comment: upstream's `bop` is `std::plus<int>{}`, associative *and*
//! commutative, so this example scans strings by concatenation instead --
//! associative, not commutative, the honest witness for what
//! [`Runtime::exclusive_scan`] actually relies on.
//!
//! [`ScanBuilder::exclusive_scan`] runs first, the owned-data `FlowBuilder`
//! wrapper closest to upstream's own `taskflow.exclusive_scan(beg, end,
//! d_first, init, bop)`. [`Runtime::transform_exclusive_scan`] runs second,
//! from inside a runtime task body, over slices it borrows from its own
//! frame -- upstream's `transform_exclusive_scan(first, last, d_first, init,
//! bop, uop)`, the shape with no `Arc` or lock at all.
//!
//! Upstream's program only *prints* its timings and a correctness line. This
//! milestone's examples print *and* **assert**: the parallel result is
//! checked against the sequential one with `assert_eq!`, so a non-zero exit
//! from the binary is itself the test. See the README's "Examples" section.

use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::prelude::*;

/// Small enough to keep `run-examples.sh`'s serial suite fast: scanning by
/// concatenation grows each output string, so this stays far smaller than
/// upstream's own element counts.
const N: usize = 80;

/// Concatenation: associative, not commutative -- see this file's own doc
/// comment. A closure literal at each call site, rather than one shared
/// `fn`, the same way upstream passes a fresh `std::plus<int>{}` at each of
/// its own two call sites -- and a plain top-level `fn(&String, &String) ->
/// String` is exactly what every `bop` here must be, so naming one would
/// read as `&str`-worthy to clippy's `ptr_arg` without actually being able
/// to take it.
fn concat_closure() -> impl Fn(&String, &String) -> String + Clone {
    |a: &String, b: &String| {
        let mut out = a.clone();
        out.push_str(b);
        out
    }
}

fn main() {
    scan_builder_matches_the_sequential_fold();
    runtime_transform_exclusive_scan_matches_the_sequential_fold();
}

/// Upstream's plain `exclusive_scan`, via [`ScanBuilder::exclusive_scan`].
fn scan_builder_matches_the_sequential_fold() {
    let elements: Vec<String> = (0..N).map(|i| i.to_string()).collect();
    let init = String::from("INIT-");
    let expected = sequential_exclusive_scan(&elements, &init, concat_closure());

    let input = Arc::new(elements);
    let out = Arc::new(Mutex::new(vec![String::new(); N]));
    let mut taskflow = Taskflow::new();
    taskflow.exclusive_scan(Arc::clone(&input), Arc::clone(&out), init, concat_closure());
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    assert_eq!(*out.lock().expect("uncontended"), expected);
}

/// Upstream's `transform_exclusive_scan`, via
/// [`Runtime::transform_exclusive_scan`] called directly from a runtime task
/// body on slices it owns outright. `uop` wraps every element in brackets
/// before the fold ever sees it.
fn runtime_transform_exclusive_scan_matches_the_sequential_fold() {
    let elements: Vec<String> = (0..N).map(|i| i.to_string()).collect();
    let init = String::from("INIT-");
    let bracket = |x: &String| format!("[{x}]");
    let bracketed: Vec<String> = elements.iter().map(bracket).collect();
    let expected = sequential_exclusive_scan(&bracketed, &init, concat_closure());

    let mut taskflow = Taskflow::new();
    taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
        let mut out = vec![String::new(); N];
        runtime.transform_exclusive_scan(
            &elements,
            &mut out,
            init.clone(),
            concat_closure(),
            bracket,
        );
        assert_eq!(out, expected);
    });
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("no task panicked");
}

/// The `std` computation both forms above are checked against: `out[0] =
/// init`, `out[i] = bop(out[i - 1], input[i - 1])` for `i >= 1`.
fn sequential_exclusive_scan<F>(input: &[String], init: &str, bop: F) -> Vec<String>
where
    F: Fn(&String, &String) -> String,
{
    let mut out = Vec::with_capacity(input.len());
    let mut running = init.to_owned();
    for item in input {
        out.push(running.clone());
        running = bop(&running, item);
    }
    out
}

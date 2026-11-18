//! Translated from `examples/inclusive_scan.cpp` (Taskflow v4.1.0), with one
//! deliberate change: upstream's `bop` is `std::multiplies<int>{}`, which is
//! both associative *and* commutative, so it cannot by itself tell a reader
//! whether the parallel scan below only preserves associativity or leans on
//! commutativity too. [`Runtime::inclusive_scan`]'s own contract is
//! associativity alone (`src/algorithm/scan.rs`'s own `# Design` section has
//! the two-phase shape that rests on), so this example scans strings by
//! concatenation instead -- associative, and *not* commutative, the honest
//! witness for the property actually being relied on.
//!
//! [`AlgorithmBuilder::inclusive_scan`] runs first, the owned-data
//! `FlowBuilder` wrapper closest to upstream's own
//! `taskflow.inclusive_scan(beg, end, d_first, bop)`.
//! [`Runtime::inclusive_scan_with_init`] runs second, from inside a runtime
//! task body, over slices it borrows from its own frame -- the shape with
//! no `Arc` or lock at all.
//!
//! Upstream's program only *prints* its timings and a correctness line. This
//! milestone's examples print *and* **assert**: the parallel result is
//! checked against the sequential one with `assert_eq!`, so a non-zero exit
//! from the binary is itself the test. See the README's "Examples" section.
//!
//! [`AlgorithmBuilder::inclusive_scan`]: rustflow::algorithm::AlgorithmBuilder::inclusive_scan

use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::prelude::*;

/// Small enough to keep `run-examples.sh`'s serial suite fast: scanning by
/// concatenation grows each output string, so this stays far smaller than
/// upstream's own element counts.
const N: usize = 80;

/// Concatenation: associative, not commutative -- see this file's own doc
/// comment. A closure literal at each call site, rather than one shared
/// `fn`, the same way upstream passes a fresh `std::multiplies<int>{}` at
/// each of its own two call sites -- and a plain top-level `fn(&String,
/// &String) -> String` is exactly what every `bop` here must be, so naming
/// one would read as `&str`-worthy to clippy's `ptr_arg` without actually
/// being able to take it.
fn concat_closure() -> impl Fn(&String, &String) -> String + Clone {
    |a: &String, b: &String| {
        let mut out = a.clone();
        out.push_str(b);
        out
    }
}

fn main() {
    scan_builder_matches_the_sequential_fold();
    runtime_inclusive_scan_with_init_matches_the_sequential_fold();
}

/// Upstream's plain `inclusive_scan`, via `AlgorithmBuilder::inclusive_scan`.
fn scan_builder_matches_the_sequential_fold() {
    let elements: Vec<String> = (0..N).map(|i| i.to_string()).collect();
    let expected = sequential_inclusive_scan(&elements, None);

    let input = Arc::new(elements);
    let out = Arc::new(Mutex::new(vec![String::new(); N]));
    let mut taskflow = Taskflow::new();
    taskflow.inclusive_scan(Arc::clone(&input), Arc::clone(&out), concat_closure());
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    assert_eq!(*out.lock().expect("uncontended"), expected);
}

/// Upstream's `inclusive_scan(..., init)` overload, via
/// [`Runtime::inclusive_scan_with_init`] called directly from a runtime task
/// body on slices it owns outright.
fn runtime_inclusive_scan_with_init_matches_the_sequential_fold() {
    let elements: Vec<String> = (0..N).map(|i| format!("#{i}")).collect();
    let init = String::from("INIT-");
    let expected = sequential_inclusive_scan(&elements, Some(&init));

    let mut taskflow = Taskflow::new();
    taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
        let mut out = vec![String::new(); N];
        runtime.inclusive_scan_with_init(&elements, &mut out, concat_closure(), &init);
        assert_eq!(out, expected);
    });
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("no task panicked");
}

/// The `std` computation [`Runtime::inclusive_scan`] and
/// [`Runtime::inclusive_scan_with_init`] are both checked against: a plain
/// left fold, seeded by `init` when given one and by `input`'s own first
/// element otherwise.
fn sequential_inclusive_scan(input: &[String], init: Option<&String>) -> Vec<String> {
    let bop = concat_closure();
    let mut out = Vec::with_capacity(input.len());
    let mut running: Option<String> = init.cloned();
    for item in input {
        running = Some(match running {
            None => item.clone(),
            Some(acc) => bop(&acc, item),
        });
        out.push(running.clone().expect("just set"));
    }
    out
}

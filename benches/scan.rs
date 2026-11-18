//! Translated from `benchmarks/scan/` (Taskflow v4.1.0).
//!
//! An inclusive scan of `N` `i32`s under multiplication: upstream's
//! `scan_taskflow` builds a fresh `tf::Taskflow`, calls
//! `inclusive_scan(input.begin(), input.end(), output.begin(),
//! std::multiplies<int>{})` and runs it, timing both -- "ours includes
//! building the graph" from `BENCHMARKS.md`'s own header applies here
//! unchanged.
//!
//! **There is no rayon baseline, because rayon has no parallel scan.** Its
//! parallel iterators express map, filter, reduce and sort; a prefix scan,
//! whose every output depends on every earlier input, is not among them, and
//! `fold` + `collect` would be a different algorithm wearing the name. So
//! the baseline column here is a sequential `Iterator::scan` over the same
//! `bop`, labelled as such -- the way `linear_chain`'s section records why
//! *its* rayon column is empty instead of putting a loop in it and calling
//! that a parallel baseline. It has no worker axis, so it is measured once
//! per size and read against both of that size's rows, exactly as
//! `benches/pipeline.rs`'s `mpsc` chain is.
//!
//! Two notes on the arithmetic, both upstream's behaviour made explicit.
//! Products of `int`s overflow within a handful of elements at any size this
//! benchmark runs, which upstream's `std::multiplies<int>` leaves to
//! undefined behaviour and both columns here spell `i32::wrapping_mul`: the
//! point of the shape is the scan's dependency structure, not the value it
//! ends on, and a wrap keeps every element contributing to its output. And
//! the inputs are odd, where upstream's are `::rand()`: a single zero
//! anywhere in the input makes every later output zero, which would make the
//! check below blind to anything that went wrong past that point, while odd
//! numbers are units modulo `2^32` and never collapse.
//!
//! That check is the untimed part of the run: before any id of a size is
//! timed, the `rustflow` variant runs once at that size and worker count and
//! its whole output buffer is compared, slot by slot, against the sequential
//! baseline's, so a block whose carry-in was never applied fails the
//! benchmark rather than being timed.

use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustflow::Executor;
use rustflow::prelude::*;

mod common;

/// Element counts, two points off upstream's sweep (`N = 10..=1e8`, ×10 each
/// step): one where the fixed cost of building and running a one-task
/// taskflow still shows, one where the two passes over the data dominate it.
const SIZES: [usize; 2] = [1_000, 1_000_000];

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// `n` pseudo-random odd `i32`s in `[1, 1999]`: upstream's `d = ::rand()`
/// loop, over a linear congruential generator rather than `rand`, so a run
/// is reproducible, and odd for the reason the module docs give.
fn inputs(n: usize) -> Vec<i32> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..n)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let magnitude = i32::try_from((state >> 33) % 1_000).expect("below 1000 fits an i32");
            2 * magnitude + 1
        })
        .collect()
}

/// Upstream's `scan_taskflow`: a fresh [`Taskflow`], one `inclusive_scan`
/// task over the whole input, run to completion.
///
/// `out` is allocated by the caller and reused across samples, as upstream's
/// `output.resize(N)` in `main.cpp` is: every slot is overwritten by every
/// run, and the allocation is not part of what upstream times.
fn scan_rustflow(input: &Arc<Vec<i32>>, out: &Arc<Mutex<Vec<i32>>>, executor: &Executor) {
    let mut taskflow = Taskflow::new();
    taskflow.inclusive_scan(Arc::clone(input), Arc::clone(out), |a: &i32, b: &i32| {
        a.wrapping_mul(*b)
    });
    executor.run(&taskflow).wait().expect("no task panicked");
}

/// The same scan as a sequential `Iterator::scan`, which is the baseline for
/// the reason the module docs give.
///
/// Seeded with `1` rather than with `input[0]`: it is the identity of
/// `wrapping_mul`, so `out[0]` is `input[0]` exactly as an inclusive scan
/// with no init requires.
fn scan_sequential(input: &[i32], out: &mut [i32]) {
    let running = input.iter().scan(1_i32, |acc, x| {
        *acc = acc.wrapping_mul(*x);
        Some(*acc)
    });
    for (slot, value) in out.iter_mut().zip(running) {
        *slot = value;
    }
}

/// Runs the `rustflow` variant once, outside any measured region, and
/// compares its whole output against the sequential baseline's.
///
/// The untimed half of this target's acceptance: a block whose carry-in was
/// never applied, or a tail never written, fails here rather than producing
/// a figure.
fn verify(input: &Arc<Vec<i32>>, workers: usize, executor: &Executor) {
    let size = input.len();
    let mut expected = vec![0_i32; size];
    scan_sequential(input, &mut expected);

    let out = Arc::new(Mutex::new(vec![0_i32; size]));
    scan_rustflow(input, &out, executor);

    let actual = out.lock().expect("uncontended once the run has joined");
    let mismatch = actual.iter().zip(&expected).position(|(a, b)| a != b);
    assert_eq!(
        mismatch, None,
        "rustflow/{workers}w at size {size}: first output slot differs from the sequential scan"
    );
}

/// Registers the `rustflow` variant at every size and worker count, and the
/// sequential baseline once per size.
fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    // Registered outside the worker loop: a sequential scan has no worker
    // count to be held to, so a per-worker id would measure the same thing
    // twice. `BENCHMARKS.md` reads this one figure against both of its
    // size's rows.
    for size in SIZES {
        let input = Arc::new(inputs(size));
        let mut out = vec![0_i32; size];
        group.bench_function(BenchmarkId::new("sequential", size), |b| {
            b.iter(|| scan_sequential(black_box(&input), &mut out));
        });
    }

    for workers in common::worker_counts() {
        let executor = common::executor(workers);

        for size in SIZES {
            // The input is read-only and every slot of `out` is overwritten
            // by every run, so both serve every sample of this id.
            let input = Arc::new(inputs(size));
            verify(&input, workers, &executor);
            let out = Arc::new(Mutex::new(vec![0_i32; size]));

            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| {
                    b.iter(|| scan_rustflow(black_box(&input), &out, &executor));
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);

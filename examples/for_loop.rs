//! Translated from `examples/parallel_for.cpp` (Taskflow v4.1.0): its
//! `demo_for_each_index` and `demo_for_each_by_index_1d`/`demo_2d`, the
//! scalar-index and range-based halves of that upstream program --
//! `examples/parallel_for.rs`'s own doc comment carries the STL-iterator
//! half of the same file, and says why the one upstream source splits into
//! two ports here: this crate keeps the slice-based and index-based loops in
//! separate modules (`src/algorithm/for_each.rs` vs
//! `src/algorithm/for_each_index.rs`), so one example per module reads the
//! same way the port itself is organised.
//!
//! Three shapes, one per section below: a scalar index one at a time
//! ([`AlgorithmBuilder::for_each_index`]), a 1D sub-range per chunk
//! ([`Runtime::for_each_by_index`]), and an N-dimensional sub-box per chunk
//! ([`Runtime::for_each_by_index_nd`]). The first uses the owned-data
//! `FlowBuilder` wrapper, closest to upstream's own
//! `taskflow.for_each_index(beg, end, step, c)`; the other two use
//! [`Runtime`] directly, from inside a runtime task body, to show the
//! slice-borrowing form upstream has no equivalent of.
//!
//! Upstream's program only *prints* as it goes. This milestone's examples
//! print *and* **assert**: every line upstream shows is also checked with
//! `assert!` or `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section.
//!
//! [`AlgorithmBuilder::for_each_index`]: rustflow::algorithm::AlgorithmBuilder::for_each_index

use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::algorithm::index_range::{IndexRange, IndexRanges};
use rustflow::algorithm::partitioner::DefaultPartitioner;
use rustflow::prelude::*;

/// Scalar range for the first two sections: small enough to keep
/// `run-examples.sh`'s serial suite fast.
const N: i32 = 400;

/// Grid dimensions for the N-dimensional section, deliberately non-square
/// like upstream's own `demo_2d(N, N + 3)` call.
const HEIGHT: i32 = 20;
const WIDTH: i32 = 23;

fn main() {
    for_each_index_visits_every_even_index_forward();
    for_each_by_index_sums_a_1d_range_in_chunks();
    for_each_by_index_nd_fills_a_2d_grid_exactly_once();
}

/// Upstream's `demo_for_each_index`'s forward traversal: indices `0, 2, 4,
/// ..., < N`, via `AlgorithmBuilder::for_each_index` queued onto a
/// `Taskflow` -- the owned-data wrapper, closest to upstream's own call.
fn for_each_index_visits_every_even_index_forward() {
    let visited = Arc::new(Mutex::new(vec![false; usize::try_from(N).expect("N >= 0")]));

    let mut taskflow = Taskflow::new();
    let marked = Arc::clone(&visited);
    taskflow.for_each_index(
        0_i32,
        N,
        2,
        move |i: i32| {
            let slot = usize::try_from(i).expect("i is in [0, N)");
            marked.lock().expect("uncontended")[slot] = true;
        },
        DefaultPartitioner::default(),
    );
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    let seen = visited.lock().expect("uncontended");
    let expected: Vec<bool> = (0..N).map(|i| i % 2 == 0).collect();
    assert_eq!(*seen, expected, "only the even indices below N are visited");
}

/// Upstream's `demo_for_each_by_index_1d`'s forward range: `[0, N)` step 1,
/// delivered one sub-range per chunk via [`Runtime::for_each_by_index`],
/// called directly from a runtime task body rather than queued through a
/// `FlowBuilder` wrapper.
fn for_each_by_index_sums_a_1d_range_in_chunks() {
    let total = Arc::new(AtomicI64::new(0));

    let mut taskflow = Taskflow::new();
    let total_body = Arc::clone(&total);
    taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
        // Cloned again rather than moved: `emplace_runtime` wants `FnMut`,
        // so a revisited task needs `total_body` to still be here next time.
        let total_body = Arc::clone(&total_body);
        runtime.for_each_by_index(
            IndexRange::new(0_i32, N, 1),
            move |sub: IndexRange<i32>| {
                let mut local = 0_i64;
                let mut i = sub.begin();
                while i < sub.end() {
                    local += i64::from(i);
                    i += sub.step_size();
                }
                total_body.fetch_add(local, Ordering::Relaxed);
            },
            DefaultPartitioner::default(),
        );
    });
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    let expected: i64 = (0..N).map(i64::from).sum();
    assert_eq!(total.load(Ordering::Relaxed), expected);
}

/// Upstream's `demo_2d`: a `HEIGHT x WIDTH` grid partitioned into rectangular
/// tiles via [`Runtime::for_each_by_index_nd`], each tile written by exactly
/// one call -- verified the same way upstream does, by re-reading every cell
/// afterwards.
///
/// Every tile writes disjoint cells, but nothing in the type of
/// `IndexRanges<i32, 2>` tells the compiler that, so the grid is a slice of
/// atomics rather than a plain `&mut [i32]` -- the same reason
/// `Runtime::for_each_index`'s own doctest keeps one.
fn for_each_by_index_nd_fills_a_2d_grid_exactly_once() {
    let height = usize::try_from(HEIGHT).expect("HEIGHT >= 0");
    let width = usize::try_from(WIDTH).expect("WIDTH >= 0");
    let grid: Vec<AtomicI32> = (0..height * width).map(|_| AtomicI32::new(-1)).collect();
    let grid = Arc::new(grid);

    let range = IndexRanges::from_ranges([
        IndexRange::new(0_i32, HEIGHT, 1),
        IndexRange::new(0_i32, WIDTH, 1),
    ]);

    let mut taskflow = Taskflow::new();
    let grid_body = Arc::clone(&grid);
    taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
        // Cloned again rather than moved: `emplace_runtime` wants `FnMut`,
        // so a revisited task needs `grid_body` to still be here next time.
        let grid_body = Arc::clone(&grid_body);
        runtime.for_each_by_index_nd(
            range,
            move |tile: IndexRanges<i32, 2>| {
                let (r0, r1, rs) = tile.dim(0);
                let (c0, c1, cs) = tile.dim(1);
                let mut r = r0;
                while r < r1 {
                    let mut c = c0;
                    while c < c1 {
                        let row = usize::try_from(r).expect("row is in [0, HEIGHT)");
                        let col = usize::try_from(c).expect("col is in [0, WIDTH)");
                        grid_body[row * width + col].store(r * WIDTH + c, Ordering::Relaxed);
                        c += cs;
                    }
                    r += rs;
                }
            },
            DefaultPartitioner::default(),
        );
    });
    Executor::default()
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    let actual: Vec<i32> = grid
        .iter()
        .map(|cell| cell.load(Ordering::Relaxed))
        .collect();
    let expected: Vec<i32> = (0..i32::try_from(height * width).expect("fits in i32")).collect();
    assert_eq!(actual, expected, "every cell is written exactly once");
}

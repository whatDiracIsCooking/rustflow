//! Translated from `benchmarks/wavefront/` (Taskflow v4.1.0).
//!
//! An `MB × NB` grid of block tasks, each preceding its right and its down
//! neighbour, so the ready frontier sweeps the grid diagonally: one source at
//! the top left, one sink at the bottom right, a join at every interior node
//! and `2·MB·NB` edges less the two boundary rows' worth. Each task is a
//! `B × B` block update reading the block above it and the block to its left,
//! which is why those are exactly the two edges it has.
//!
//! **This is the first shape in this directory built with
//! [`FlowBuilder::placeholder`] and [`TaskMut::work`] rather than
//! [`FlowBuilder::emplace`]**, and it is upstream's own construction: the grid
//! is filled with placeholders first, then walked backwards attaching each
//! block's body and its two edges. The edges of this graph are known before
//! its bodies are -- node `(i, j)` precedes `(i, j + 1)` and `(i + 1, j)`,
//! which do not exist yet when it is reached -- and a placeholder is what
//! lets an edge name a node whose work has not been written.
//!
//! **No rayon variant.** A wavefront is a dependency pattern, not a data
//! decomposition: the frontier is an anti-diagonal whose length changes every
//! step, and rayon's shapes -- `par_iter`, `join`, `scope` -- express "these
//! may run at the same time" about a collection that is known up front.
//! Driving the anti-diagonals by hand over `par_iter` would measure the
//! hand-written sweep and its barriers rather than a baseline's scheduler, for
//! the reason `linear_chain.rs` declines a `for` loop. Upstream compares
//! against a TBB flow graph here, which we have no equivalent of. What stands
//! in its place is upstream's own `seq.cpp`: a sequential sweep in row-major
//! order, which is both the baseline column of this shape's table and -- see
//! [`verify`] -- the reference every `rustflow` id's answer is checked against
//! before anything is timed.
//!
//! # Design
//!
//! Three departures from upstream, each forced by something it does rather
//! than chosen:
//!
//! - **The body is the block update upstream has commented out.** At the pin,
//!   `matrix.hpp`'s `block_computation(i, j)` is `return i + j;` with the
//!   `B × B` update sitting in comments directly below it, so the published
//!   benchmark times a graph whose nodes do nothing. `i + j` is also not a
//!   body a Rust benchmark can keep: nothing reads the result, so it compiles
//!   to nothing at all. This file ports the commented update instead -- it is
//!   the computation the grid's two edges per node exist for, and the only
//!   version of this shape whose answer can be checked.
//! - **The matrix is a flat `Vec<AtomicU64>` of [`f64::to_bits`] values.**
//!   Upstream's is a global `double**` written by concurrent blocks; the
//!   graph's edges are what keep two writers off one cell, and a C++ global
//!   needs nothing from the type system to be shared. A Rust task body is
//!   `Send + 'static`, so the port shares an [`Arc`] instead, and the cells
//!   are atomics so that the port has no data race *at all* rather than one
//!   the edges happen to rule out. The accesses are [`Ordering::Relaxed`]:
//!   the happens-before a block needs against the blocks it reads comes from
//!   the executor completing them, not from these loads and stores, so a
//!   stronger ordering would buy nothing and would show up in the figures.
//!   A [`Mutex`](std::sync::Mutex) per block would measure the lock instead
//!   of the shape.
//! - **Each sample reuses the matrix the previous one left**, as upstream's
//!   rounds do: `main.cpp` calls `init_matrix()` once per size and runs every
//!   round of that size against whatever the last round wrote. That is sound
//!   here because the sweep's answer is a fixed point -- every cell ends at
//!   `1.0`, so a second sweep over a finished matrix performs the same loads,
//!   branches and stores as the first -- which is why this file needs no
//!   `iter_batched` setup the way `sort.rs` does.
//!
//! The measured region is upstream's `wavefront_taskflow`: the placeholders,
//! the bodies, the edges and the run, with "ours includes building the graph"
//! from `BENCHMARKS.md` applying to the grid as to every other shape. The
//! matrix is allocated outside it, as upstream allocates it in `main`, and the
//! executor is built once per worker count, as upstream's function-static one
//! is.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustflow::Executor;
use rustflow::prelude::*;

mod common;

/// Matrix dimensions, as upstream's square `M = N = S`.
///
/// Upstream's `main.cpp` sweeps `S` over the powers of two from `8` to
/// `1 << 14`, whose last matrix is 2048 × 2048 blocks. These are two points
/// *on* that sweep: at `S = 256` the grid is 32 × 32 blocks and the fixed cost
/// of a run still shows, at `S = 1024` it is 128 × 128 and the per-block cost
/// dominates it.
const DIMENSIONS: [usize; 2] = [256, 1024];

/// Upstream's `B`: the side of one block, fixed at `8` in `main.cpp` for every
/// size of its sweep.
const BLOCK: usize = 8;

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// Upstream's `matrix`, a global `double**`, as one shareable allocation.
///
/// See the module docs for why the cells are atomics and why their accesses
/// are relaxed.
struct Matrix {
    /// Row-major cells, each an [`f64`] held as its bit pattern.
    cells: Vec<AtomicU64>,
    /// Rows: upstream's `M`.
    rows: usize,
    /// Columns: upstream's `N`.
    cols: usize,
}

impl Matrix {
    /// Allocates an `rows × cols` matrix and fills it as upstream's
    /// `init_matrix` does, with `i * N + j`.
    fn new(rows: usize, cols: usize) -> Self {
        let cells = (0..rows)
            .flat_map(|i| (0..cols).map(move |j| i * cols + j))
            .map(|value| {
                // The index fits an `f64` exactly for every size in
                // `DIMENSIONS`, and the value exists only to give the sweep
                // something to read: upstream's own is this same product.
                AtomicU64::new((value as f64).to_bits())
            })
            .collect();

        Self { cells, rows, cols }
    }

    /// The cell at `(i, j)`.
    fn get(&self, i: usize, j: usize) -> f64 {
        f64::from_bits(self.bits(i, j))
    }

    /// The bit pattern of the cell at `(i, j)`, which is what [`verify`]
    /// compares: two sweeps that respect the grid's order compute every cell
    /// by the same operations on the same inputs, so they agree bit for bit.
    fn bits(&self, i: usize, j: usize) -> u64 {
        self.cells[i * self.cols + j].load(Ordering::Relaxed)
    }

    /// Writes `value` into the cell at `(i, j)`.
    fn set(&self, i: usize, j: usize, value: f64) {
        self.cells[i * self.cols + j].store(value.to_bits(), Ordering::Relaxed);
    }

    /// Block rows: upstream's `MB = (M / B) + (M % B > 0)`.
    fn block_rows(&self) -> usize {
        self.rows.div_ceil(BLOCK)
    }

    /// Block columns: upstream's `NB`.
    fn block_cols(&self) -> usize {
        self.cols.div_ceil(BLOCK)
    }
}

/// Upstream's `calc`, the per-cell operation of the block update.
///
/// `f64::max` rather than `std::max` -- they differ only on NaN, which this
/// sweep never produces: the equality branch is reached with both arguments
/// equal and non-zero, so its division is never `0.0 / 0.0`.
fn calc(v0: f64, v1: f64) -> f64 {
    if v0 == v1 {
        // Upstream's `std::pow(v0/v1, 4.0f)`, exponent and all.
        (v0 / v1).powf(4.0)
    } else {
        v0.max(v1)
    }
}

/// Upstream's `block_computation(i, j)`: the `B × B` update of block
/// `(i, j)`, reading the cell above and the cell to the left of each of its
/// own cells.
///
/// Within the block those two neighbours are earlier iterations of these
/// loops; on its top row and left column they belong to blocks `(i - 1, j)`
/// and `(i, j - 1)`, which is exactly the pair of edges the grid gives this
/// node. The module docs say why this is the commented-out form of upstream's
/// function rather than the one it compiles.
fn block_computation(matrix: &Matrix, i: usize, j: usize) {
    let start_i = i * BLOCK;
    let end_i = (start_i + BLOCK).min(matrix.rows);
    let start_j = j * BLOCK;
    let end_j = (start_j + BLOCK).min(matrix.cols);

    for ii in start_i..end_i {
        for jj in start_j..end_j {
            let v0 = if ii == 0 { 0.0 } else { matrix.get(ii - 1, jj) };
            let v1 = if jj == 0 { 0.0 } else { matrix.get(ii, jj - 1) };
            let value = if ii == 0 && jj == 0 {
                1.0
            } else {
                calc(v0, v1)
            };
            matrix.set(ii, jj, value);
        }
    }
}

/// Upstream's `wavefront_taskflow`: a grid of placeholders, then each block's
/// work and its two edges, then one run.
///
/// Upstream's backwards walk is kept, because it is what makes the
/// placeholders necessary: at `(i, j)` the successors `(i, j + 1)` and
/// `(i + 1, j)` have already had their work attached, but the forward walk
/// would reach them only later.
fn wavefront_rustflow(matrix: &Arc<Matrix>, executor: &Executor) {
    let (block_rows, block_cols) = (matrix.block_rows(), matrix.block_cols());

    let mut taskflow = Taskflow::new();
    let node: Vec<Vec<Task>> = (0..block_rows)
        .map(|_| (0..block_cols).map(|_| taskflow.placeholder()).collect())
        .collect();

    // Upstream's `matrix[M-1][N-1] = 0` inside the measured region. The cell is
    // overwritten by the sink block before anything reads it, so it changes no
    // answer; it is here because upstream's measured region has it.
    matrix.set(matrix.rows - 1, matrix.cols - 1, 0.0);

    for i in (0..block_rows).rev() {
        for j in (0..block_cols).rev() {
            let cells = Arc::clone(matrix);
            let mut task = taskflow.task(node[i][j]);
            task.work(move || block_computation(&cells, i, j));

            if j + 1 < block_cols {
                task.precede([node[i][j + 1]]);
            }
            if i + 1 < block_rows {
                task.precede([node[i + 1][j]]);
            }
        }
    }

    // One block per node and nothing left a placeholder: the only thing
    // standing between this id and timing a run of empty tasks, which
    // `TaskMut::work` on the wrong handle would do quietly. It is O(1) and
    // asserted in a release build, as `linear_chain.rs`'s count is.
    assert_eq!(
        taskflow.num_tasks(),
        block_rows * block_cols,
        "one task per block of the grid"
    );

    executor.run(&taskflow).wait().expect("no task panicked");
}

/// Upstream's `wavefront_seq`: the same blocks in row-major order, which
/// respects every edge of the grid without declaring any.
///
/// The baseline column of this shape's table, and the reference [`verify`]
/// checks the graph's answer against. It has no worker axis and no graph to
/// build, which is the asymmetry `BENCHMARKS.md`'s "What is measured" names
/// for every table in it.
fn wavefront_sequential(matrix: &Matrix) {
    matrix.set(matrix.rows - 1, matrix.cols - 1, 0.0);

    for i in 0..matrix.block_rows() {
        for j in 0..matrix.block_cols() {
            block_computation(matrix, i, j);
        }
    }
}

/// Runs the graph once on a fresh matrix and compares every cell against the
/// sequential sweep's, before anything is timed.
///
/// The sweep's answer is a deterministic function of the grid -- the sink cell
/// most of all, since every path through the grid reaches it -- so a schedule
/// that runs a block before the block above it or to its left reads a cell
/// still holding its initial `i * N + j` and writes a value the reference
/// never produces. Checking it here rather than inside the timed closure keeps
/// a wrong answer a failure instead of a measurement.
fn verify(dimension: usize, executor: &Executor) {
    let ours = Arc::new(Matrix::new(dimension, dimension));
    wavefront_rustflow(&ours, executor);

    let reference = Matrix::new(dimension, dimension);
    wavefront_sequential(&reference);

    let mismatch = (0..reference.rows)
        .flat_map(|i| (0..reference.cols).map(move |j| (i, j)))
        .find(|&(i, j)| ours.bits(i, j) != reference.bits(i, j));

    assert!(
        mismatch.is_none(),
        "the graph's sweep of {dimension}x{dimension} disagrees with the sequential one \
         at {mismatch:?}: {:?} against {:?}",
        mismatch.map(|(i, j)| ours.get(i, j)),
        mismatch.map(|(i, j)| reference.get(i, j)),
    );
}

/// Registers the graph at every size and worker count, and the sequential
/// baseline once per size.
fn bench_wavefront(c: &mut Criterion) {
    let mut group = c.benchmark_group("wavefront");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    // The sequential sweep has no worker axis -- it is one thread by
    // construction -- so it is registered once per size, and its figure is read
    // against both rows of that size in the table, the way `pipeline.rs`'s
    // `mpsc` baseline is.
    for dimension in DIMENSIONS {
        let matrix = Matrix::new(dimension, dimension);
        let size = matrix.block_rows() * matrix.block_cols();

        group.bench_function(BenchmarkId::new("sequential", size), |b| {
            b.iter(|| wavefront_sequential(black_box(&matrix)));
        });
    }

    for workers in common::worker_counts() {
        let executor = common::executor(workers);

        for dimension in DIMENSIONS {
            verify(dimension, &executor);

            let matrix = Arc::new(Matrix::new(dimension, dimension));
            // Upstream reports `MB * NB`, the number of blocks, as the size of
            // the problem rather than `M` or `S`.
            let size = matrix.block_rows() * matrix.block_cols();

            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| {
                    b.iter(|| wavefront_rustflow(black_box(&matrix), &executor));
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_wavefront);
criterion_main!(benches);

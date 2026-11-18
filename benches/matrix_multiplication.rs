//! Translated from `benchmarks/matrix_multiplication/` (Taskflow v4.1.0).
//!
//! `N × N` `f64` matrices, `c = a * b`, and the only shape in this family
//! with a graph around its algorithms: upstream's
//! `matrix_multiplication_taskflow` builds four `for_each_index` tasks over
//! `[0, N)` -- `init_a`, `init_b`, `init_c` and `comp_c` -- and joins them
//! with `comp_c.succeed(init_a, init_b, init_c)`, three sources into one
//! sink. The edges are ported as upstream has them, the partitioner is the
//! default upstream leaves unspecified, and each task's body is upstream's:
//! `a[i][j] = i + j`, `b[i][j] = i * j`, `c[i][j] = 0`, then the `ijk` triple
//! loop. The matrices are flat `Vec`s indexed `i * n + j`, which is the
//! layout `matrix_multiplication.hpp`'s `allocate_matrix` builds row by row,
//! with no numeric library involved on either side.
//!
//! The baseline is three `par_iter_mut` inits followed by a `par_chunks_mut`
//! product, in the role TBB's `parallel_for` plays in upstream's own
//! `tbb.cpp`. **That baseline has no graph, so its three inits are three
//! barriers where ours are three concurrent sources** -- the one place in
//! this suite where "ours includes building the graph" buys something back
//! rather than only costing, and the thing to read the table for.
//!
//! Two departures, both forced by Rust's aliasing rules rather than chosen:
//!
//! - **Our cells are `AtomicU64` bit patterns; the baseline's are plain
//!   `f64`s.** Upstream writes `double**` globals from several threads with
//!   no synchronisation at all, relying on its edges for ordering. A task
//!   body here is `'static` and `comp_c` reads *all* of `b` while another
//!   row's body may still be writing `c`, so there is no safe way to hand
//!   out `&mut` rows the way `rayon`'s borrowing closures get them -- and
//!   the port's posture is a safe shape over an `unsafe` reproduction of C++
//!   pointer semantics. `Relaxed` is the ordering because the graph, not the
//!   cell, is what orders the writes before the reads: `comp_c`'s join is
//!   the release/acquire pair, exactly as upstream's edge is. On x86-64 a
//!   relaxed load or store of a `u64` is a plain `mov`, but it is not
//!   vectorisable or hoistable, so part of any gap this table shows is the
//!   representation and not the scheduler.
//! - **`c[i][j] +=` accumulates in a local.** Upstream's inner loop
//!   read-modify-writes `c[i][j]` on every `k`, which its compiler is free to
//!   keep in a register; ours cannot be, being an atomic cell, so the body
//!   sums into a local and stores once per `j`. The additions and their order
//!   are unchanged, so the result is identical down to the bit.
//!
//! Both columns are asserted against a single-threaded reference product
//! before anything is timed. That comparison is exact rather than
//! approximate, and legitimately so: every value here is a whole number --
//! `c[i][j] = Σ (i + k) * (k * j)` -- and the largest of them at the sizes
//! below is under 2^53, so every partial sum is exactly representable and the
//! summation order cannot change the answer.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;
use rustflow::Executor;
use rustflow::algorithm::partitioner::DefaultPartitioner;
use rustflow::prelude::*;

#[path = "common/baseline.rs"]
mod baseline;
mod common;

/// Matrix dimensions, two points off upstream's sweep (`N = 128..=1024`, step
/// `32`): one where the `O(N²)` inits and the graph are still a visible share
/// of the run, one where the `O(N³)` product dominates them.
const SIZES: [usize; 2] = [128, 512];

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// One of upstream's `double**` matrices: `n * n` cells indexed `i * n + j`.
///
/// The cells are bit patterns in atomic words for the reason the module docs
/// give. Every access is `Relaxed`; the graph's edges are what order them.
struct Matrix {
    /// Row-major cells, `n * n` of them.
    cells: Vec<AtomicU64>,
    /// The row length, upstream's `N`.
    n: usize,
}

impl Matrix {
    /// An `n` by `n` matrix of zeroes: upstream's `allocate_matrix`, which
    /// also runs outside the measured region.
    fn new(n: usize) -> Self {
        Self {
            cells: (0..n * n).map(|_| AtomicU64::new(0)).collect(),
            n,
        }
    }

    /// The value at `(i, j)`: upstream's `m[i][j]` read.
    fn get(&self, i: usize, j: usize) -> f64 {
        f64::from_bits(self.cells[i * self.n + j].load(Ordering::Relaxed))
    }

    /// Writes `value` at `(i, j)`: upstream's `m[i][j]` assignment.
    fn set(&self, i: usize, j: usize, value: f64) {
        self.cells[i * self.n + j].store(value.to_bits(), Ordering::Relaxed);
    }

    /// The whole matrix as plain values, for the untimed comparison against
    /// the reference product.
    fn to_vec(&self) -> Vec<f64> {
        self.cells
            .iter()
            .map(|cell| f64::from_bits(cell.load(Ordering::Relaxed)))
            .collect()
    }
}

/// Upstream's `a`, `b` and `c` globals, shared with the task bodies that
/// write them.
struct Matrices {
    /// Upstream's `a`, written by `init_a`.
    a: Arc<Matrix>,
    /// Upstream's `b`, written by `init_b`.
    b: Arc<Matrix>,
    /// Upstream's `c`, zeroed by `init_c` and summed into by `comp_c`.
    c: Arc<Matrix>,
}

impl Matrices {
    /// Three `n` by `n` matrices of zeroes.
    fn new(n: usize) -> Self {
        Self {
            a: Arc::new(Matrix::new(n)),
            b: Arc::new(Matrix::new(n)),
            c: Arc::new(Matrix::new(n)),
        }
    }
}

/// The baseline's three matrices, plain `Vec`s in the same `i * n + j`
/// layout: `rayon`'s closures borrow, so nothing here needs a cell.
struct PlainMatrices {
    /// Upstream's `a`.
    a: Vec<f64>,
    /// Upstream's `b`.
    b: Vec<f64>,
    /// Upstream's `c`.
    c: Vec<f64>,
}

impl PlainMatrices {
    /// Three `n` by `n` matrices of zeroes.
    fn new(n: usize) -> Self {
        Self {
            a: vec![0.0; n * n],
            b: vec![0.0; n * n],
            c: vec![0.0; n * n],
        }
    }
}

/// Upstream's `matrix_multiplication_taskflow`: four `for_each_index` tasks,
/// `comp_c` succeeding the three inits, run to completion.
fn matmul_rustflow(n: usize, m: &Matrices, executor: &Executor) {
    let mut taskflow = Taskflow::new();

    let a = Arc::clone(&m.a);
    let init_a = taskflow.for_each_index(
        0_usize,
        n,
        1,
        move |i: usize| {
            for j in 0..n {
                // `usize as f64` is exact for every size on upstream's sweep.
                a.set(i, j, (i + j) as f64);
            }
        },
        DefaultPartitioner::default(),
    );

    let b = Arc::clone(&m.b);
    let init_b = taskflow.for_each_index(
        0_usize,
        n,
        1,
        move |i: usize| {
            for j in 0..n {
                b.set(i, j, (i * j) as f64);
            }
        },
        DefaultPartitioner::default(),
    );

    let c = Arc::clone(&m.c);
    let init_c = taskflow.for_each_index(
        0_usize,
        n,
        1,
        move |i: usize| {
            for j in 0..n {
                c.set(i, j, 0.0);
            }
        },
        DefaultPartitioner::default(),
    );

    let (a, b, c) = (Arc::clone(&m.a), Arc::clone(&m.b), Arc::clone(&m.c));
    let comp_c = taskflow.for_each_index(
        0_usize,
        n,
        1,
        move |i: usize| {
            for j in 0..n {
                // Upstream's `c[i][j] += a[i][k] * b[k][j]`, summed in a
                // local for the reason the module docs give.
                let mut sum = c.get(i, j);
                for k in 0..n {
                    sum += a.get(i, k) * b.get(k, j);
                }
                c.set(i, j, sum);
            }
        },
        DefaultPartitioner::default(),
    );

    taskflow.task(comp_c).succeed([init_a, init_b, init_c]);

    executor.run(&taskflow).wait().expect("no task panicked");
}

/// The same product as three rayon inits and a parallel product, in the role
/// TBB's `parallel_for` plays upstream. The three inits are sequential
/// phases here, which is the asymmetry the module docs name.
fn matmul_rayon(n: usize, m: &mut PlainMatrices, pool: &rayon::ThreadPool) {
    pool.install(|| {
        m.a.par_iter_mut().enumerate().for_each(|(idx, cell)| {
            let (i, j) = (idx / n, idx % n);
            *cell = (i + j) as f64;
        });
        m.b.par_iter_mut().enumerate().for_each(|(idx, cell)| {
            let (i, j) = (idx / n, idx % n);
            *cell = (i * j) as f64;
        });
        m.c.par_iter_mut().for_each(|cell| *cell = 0.0);

        let (a, b) = (&m.a, &m.b);
        m.c.par_chunks_mut(n).enumerate().for_each(|(i, row)| {
            for (j, cell) in row.iter_mut().enumerate() {
                let mut sum = *cell;
                for k in 0..n {
                    sum += a[i * n + k] * b[k * n + j];
                }
                *cell = sum;
            }
        });
    });
}

/// The same product on one thread: the matrix both measured variants have to
/// produce.
fn matmul_sequential(n: usize) -> Vec<f64> {
    let mut a = vec![0.0; n * n];
    let mut b = vec![0.0; n * n];
    let mut c = vec![0.0; n * n];

    for i in 0..n {
        for j in 0..n {
            a[i * n + j] = (i + j) as f64;
            b[i * n + j] = (i * j) as f64;
        }
    }

    for i in 0..n {
        for j in 0..n {
            let mut sum = 0.0;
            for k in 0..n {
                sum += a[i * n + k] * b[k * n + j];
            }
            c[i * n + j] = sum;
        }
    }

    c
}

/// Registers both variants at every size and worker count.
fn bench_matrix_multiplication(c: &mut Criterion) {
    let mut group = c.benchmark_group("matrix_multiplication");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    // Untimed, once per size: the reference product every id below is
    // checked against.
    let references = SIZES.map(matmul_sequential);

    for workers in common::worker_counts() {
        let executor = common::executor(workers);
        let pool = baseline::rayon_pool(workers);

        for (size, expected) in SIZES.into_iter().zip(&references) {
            // Untimed, and once per worker count as well as per size, since a
            // product that is only right on one thread is still wrong. Each
            // check runs against matrices of its own, so a variant that
            // skipped a row fails here rather than reading the other's work.
            let checked = Matrices::new(size);
            matmul_rustflow(size, &checked, &executor);
            assert_eq!(
                checked.c.to_vec(),
                *expected,
                "rustflow multiplies {size}x{size} at {workers} worker(s)"
            );

            let mut checked = PlainMatrices::new(size);
            matmul_rayon(size, &mut checked, &pool);
            assert_eq!(
                checked.c, *expected,
                "rayon multiplies {size}x{size} at {workers} worker(s)"
            );

            // Allocated outside the measured region, as upstream calls
            // `allocate_matrix` outside its own: `init_c` is what makes
            // reusing the buffers across samples correct, on both sides.
            let shared = Matrices::new(size);
            let mut plain = PlainMatrices::new(size);

            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| {
                    b.iter(|| matmul_rustflow(black_box(size), &shared, &executor));
                },
            );
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), size), |b| {
                b.iter(|| matmul_rayon(black_box(size), &mut plain, &pool));
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_matrix_multiplication);
criterion_main!(benches);

//! Translated from `benchmarks/mandelbrot/` (Taskflow v4.1.0).
//!
//! An escape-time render of the Mandelbrot set: upstream's
//! `mandelbrot_taskflow` is one `for_each_index(0, H, 1, .., DynamicPartitioner(1))`
//! whose body walks `W` points of one index's line, calling `mandel.hpp`'s
//! `escape_time` on each. A chunk of one is the point of the shape -- the
//! cost of a line varies by two orders of magnitude between the edge of the
//! image and the interior of the set, where every point runs the full
//! `MAX_ITERATION`, so this is the suite's load-imbalance benchmark rather
//! than another uniform `for_each`. `D`, `H` and `W` stay the size knobs they
//! are upstream, with `H == W == N` as upstream's `main.cpp` sets them.
//!
//! The baseline is `par_iter` over the same lines, in the role TBB's
//! `parallel_for` plays in upstream's own `tbb.cpp`. Both columns write into
//! the same buffer type, so what a row of the table compares is the two
//! schedulers' handling of that imbalance and nothing else.
//!
//! Three departures from upstream, none of them in the kernel:
//!
//! - **The image is escape iterations, not colours.** Upstream's body ends in
//!   `std::tie(RGB[k], RGB[k+1], RGB[k+2]) = get_color(value)`, a lookup in a
//!   sixteen-entry palette whose only consumer is the `dump_tga` call that
//!   `main.cpp` leaves commented out. What is kept here is `value` itself,
//!   one `u32` a point, which is both the whole measured kernel's output and
//!   the thing this benchmark asserts: an image that differs from the
//!   sequential reference fails before anything is timed.
//! - **One line per index, contiguous.** Upstream's `escape_time` for index
//!   `i` and point `j` lands at `RGB[3 * (j * W + i)]`, so a task writes a
//!   *column* of the picture, striding `W` pixels at a time through one
//!   buffer every other task is also writing. The port keeps the values and
//!   the loop, and lays each index's `W` of them out contiguously instead --
//!   the transpose of upstream's picture, which matters only to the TGA dump
//!   that is not ported, and which is what lets one task own its own storage.
//! - **A `Mutex` per line, uncontended by construction.** Upstream relies on
//!   those writes being to disjoint addresses and synchronises nothing. A
//!   task body here is `'static` and cannot hold a `&mut [u32]` into a
//!   caller's buffer, and the port's posture is to reach for a safe shape
//!   rather than an `unsafe` reproduction of C++ pointer semantics, so the
//!   image is one `Mutex<Vec<u32>>` a line. Both `for_each_index` and
//!   `par_iter` visit each line exactly once, so every lock is taken by its
//!   line's only writer -- two uncontended atomics a line, against the
//!   hundreds of transcendental calls a line of this kernel costs.
//!
//! A third column, `rustflow-split`, runs the identical task under
//! `SplittingPartitioner` at the same chunk of one line -- the port's own
//! chunk scheme, whose share re-splits when another worker steals it. No
//! figure for it is claimed here: the milestone's A/B issue is what measures
//! the pair.

use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use rustflow::Executor;
use rustflow::algorithm::for_each_index::IndexDispatch;
use rustflow::algorithm::partitioner::{
    DefaultClosureWrapper, DynamicPartitioner, SplittingPartitioner,
};
use rustflow::prelude::*;

#[path = "common/baseline.rs"]
mod baseline;
mod common;

/// Image dimensions, two points off upstream's sweep (`N = 100..=1000`, step
/// `100`, with `W = H = N`): one where the per-line cost is small enough for
/// the fixed cost of a run to show, one where the kernel dominates it.
const SIZES: [usize; 2] = [100, 500];

/// Upstream's `D`, the power the iteration is taken to: `z -> z^D + c`.
const D: i32 = 2;
/// Upstream's `MAX_ITERATION`, the escape-time cutoff.
const MAX_ITERATION: u32 = 200;
/// Upstream's `XL`, `XR`, `YB`, `YT`: the complex rectangle the image covers.
const XL: f64 = -2.5;
/// See [`XL`].
const XR: f64 = 1.0;
/// See [`XL`].
const YB: f64 = -1.0;
/// See [`XL`].
const YT: f64 = 1.0;

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// Upstream's `RGB` buffer, as the module docs describe it: `n` lines of `n`
/// escape iterations, one `Mutex` a line so that a `'static` task body can
/// reach its own.
type Image = Arc<Vec<Mutex<Vec<u32>>>>;

/// An `n` by `n` image of zeroes.
///
/// Allocated outside every measured region, as upstream `malloc`s `RGB` in
/// `main` once per size and reuses it for every round: each run overwrites
/// every point of it.
fn image(n: usize) -> Image {
    Arc::new((0..n).map(|_| Mutex::new(vec![0; n])).collect())
}

/// Upstream's `escape_time`: how many iterations of `z -> z^n + c` the point
/// `px + i*py` survives before `|z| > 2`, up to [`MAX_ITERATION`].
///
/// The duplicated `pow` and `atan2` of upstream's two assignments are kept
/// rather than hoisted into a shared temporary: whether a compiler folds
/// those two calls into one is the compiler's decision here exactly as it is
/// upstream, and hoisting them by hand would halve the per-point
/// transcendental count this benchmark exists to measure.
fn escape_time(px: f64, py: f64, n: i32) -> u32 {
    // `x` and `y` represent a complex number (x + i*y).
    let mut x = px;
    let mut y = py;

    // Square both components.
    let mut x2 = x * x;
    let mut y2 = y * y;

    // Mandelbrot set: the constant is the point itself.
    let cr = px;
    let ci = py;

    // Upstream's `int n` reaches the kernel only as `n / 2.0` and `n * theta`.
    let n = f64::from(n);

    let mut i = 0;
    while x2 + y2 <= 4.0 && i < MAX_ITERATION {
        let xtmp = (x * x + y * y).powf(n / 2.0) * (n * y.atan2(x)).cos() + cr;
        // Upstream's own order: this line still reads the pre-iteration `x`.
        y = (x * x + y * y).powf(n / 2.0) * (n * y.atan2(x)).sin() + ci;
        x = xtmp;

        // Update our temp variables.
        x2 = x * x;
        y2 = y * y;

        i += 1;
    }

    i
}

/// Upstream's `scale_xy`: maps the integer point `(x, y)` of an `n` by `n`
/// image into `[XL, XR] × [YB, YT]`.
///
/// `usize as f64` is exact for every size on upstream's sweep, and for any
/// image either column could render in a benchmark's lifetime.
fn scale_xy(x: usize, y: usize, n: usize) -> (f64, f64) {
    let xx = XL + (XR - XL) / n as f64 * x as f64;
    let yy = YB + (YT - YB) / n as f64 * y as f64;
    (xx, yy)
}

/// One line of the image: upstream's `for(int j=0; j<W; j++)` body, writing
/// escape iterations instead of palette entries.
fn escape_line(i: usize, n: usize, line: &mut [u32]) {
    for (j, point) in line.iter_mut().enumerate() {
        let (x, y) = scale_xy(i, j, n);
        *point = escape_time(x, y, D);
    }
}

/// Upstream's `mandelbrot_taskflow`: one `for_each_index` over the `H` lines
/// with a dynamic chunk of one, run to completion.
fn mandelbrot_rustflow<P>(n: usize, image: &Image, executor: &Executor, part: P)
where
    P: IndexDispatch + Clone + Send + 'static,
{
    let mut taskflow = Taskflow::new();
    let lines = Arc::clone(image);

    taskflow.for_each_index(
        0_usize,
        n,
        1,
        move |i: usize| {
            let mut line = lines[i].lock().expect("this line's only writer");
            escape_line(i, n, &mut line);
        },
        part,
    );

    executor.run(&taskflow).wait().expect("no task panicked");
}

/// Upstream's own chunk scheme for this shape, behind the `rustflow` ids:
/// one line a chunk.
fn dynamic() -> DynamicPartitioner<DefaultClosureWrapper> {
    DynamicPartitioner::new(1)
}

/// The partitioner behind the `rustflow-split` ids: the port's own splitting
/// scheme at the same chunk of one line, so the two ids differ in how a
/// chunk reaches a worker and in nothing else -- which is the whole question
/// for an image whose lines cost wildly different amounts. No figure is
/// claimed for either here; the milestone's A/B issue measures them.
fn splitting() -> SplittingPartitioner<DefaultClosureWrapper> {
    SplittingPartitioner::new(1)
}

/// The same render as a rayon parallel iterator over the same lines, in the
/// role TBB's `parallel_for` plays upstream.
fn mandelbrot_rayon(n: usize, image: &Image, pool: &rayon::ThreadPool) {
    pool.install(|| {
        image.par_iter().enumerate().for_each(|(i, line)| {
            let mut line = line.lock().expect("this line's only writer");
            escape_line(i, n, &mut line);
        });
    });
}

/// The same render on one thread, by the same kernel: the image both measured
/// variants have to produce.
fn mandelbrot_sequential(n: usize) -> Vec<Vec<u32>> {
    (0..n)
        .map(|i| {
            let mut line = vec![0; n];
            escape_line(i, n, &mut line);
            line
        })
        .collect()
}

/// Panics unless `image` holds exactly `expected`, naming `label` and the
/// first line that differs.
///
/// The comparison is exact because the quantity is an iteration count, not a
/// floating-point result: both columns run the same kernel over the same
/// points, so the counts agree bit for bit or one of them visited a point it
/// should not have.
fn assert_image(image: &Image, expected: &[Vec<u32>], label: &str) {
    assert_eq!(image.len(), expected.len(), "{label}: line count");
    for (i, (line, want)) in image.iter().zip(expected).enumerate() {
        let line = line.lock().expect("uncontended after the run");
        assert_eq!(&*line, want, "{label}: line {i}");
    }
}

/// Registers both variants at every size and worker count.
fn bench_mandelbrot(c: &mut Criterion) {
    let mut group = c.benchmark_group("mandelbrot");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    // Untimed, once per size: the reference image every id below is checked
    // against.
    let references = SIZES.map(mandelbrot_sequential);

    for workers in common::worker_counts() {
        let executor = common::executor(workers);
        let pool = baseline::rayon_pool(workers);

        for (size, expected) in SIZES.into_iter().zip(&references) {
            // Untimed, and once per worker count as well as per size, since
            // an image that is only right on one thread is still wrong. Each
            // check renders into a buffer of its own so that a variant which
            // skips a line fails here rather than reading the other's work.
            let checked = image(size);
            mandelbrot_rustflow(size, &checked, &executor, dynamic());
            assert_image(&checked, expected, &format!("rustflow at {workers}w"));

            let checked = image(size);
            mandelbrot_rustflow(size, &checked, &executor, splitting());
            assert_image(&checked, expected, &format!("rustflow-split at {workers}w"));

            let checked = image(size);
            mandelbrot_rayon(size, &checked, &pool);
            assert_image(&checked, expected, &format!("rayon at {workers}w"));

            // Allocated outside the measured region, as upstream allocates
            // `RGB` outside its own: every sample overwrites all of it.
            let buffer = image(size);
            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| {
                    b.iter(|| mandelbrot_rustflow(black_box(size), &buffer, &executor, dynamic()));
                },
            );
            group.bench_function(
                BenchmarkId::new(format!("rustflow-split/{workers}w"), size),
                |b| {
                    b.iter(|| {
                        mandelbrot_rustflow(black_box(size), &buffer, &executor, splitting());
                    });
                },
            );
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), size), |b| {
                b.iter(|| mandelbrot_rayon(black_box(size), &buffer, &pool));
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_mandelbrot);
criterion_main!(benches);

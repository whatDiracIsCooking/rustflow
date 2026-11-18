//! Translated from `benchmarks/integrate/` (Taskflow v4.1.0).
//!
//! Adaptive quadrature of `f(x) = (x^2 + 1) x` over `[0, V]`: upstream's
//! `spawn_async` bisects the interval, compares the two-trapezoid estimate
//! against the one it was handed, and if they differ by at least `epsilon`
//! submits the left half as a `silent_async` child of a
//! [`TaskGroup`](rustflow::TaskGroup) of that interval's own, refines the
//! right half on the calling worker, `corun`s, and returns the sum of the two
//! areas. The root is one `Executor::spawn` whose handle is waited, because
//! the recursion has to start on a worker. The bisection, the error bound and
//! the `f64` arithmetic are upstream's unchanged, down to `epsilon = 1.0e-9`
//! and the two one-sided comparisons it is tested with.
//!
//! The baseline is **`rayon::join`**, the same recursion with the fork
//! replaced -- which is what upstream's `tbb.cpp` writes on a
//! `tbb::task_group`, and what anyone would reach for here.
//!
//! # Design
//!
//! **What the two branches have to share, and how.** Nothing: unlike
//! `benches/merge_sort.rs`, whose branches write into one array, each half of
//! an interval produces a single `f64`. Upstream's child writes `area_x1x0`,
//! a local of the frame that spawned it, through a captured reference; a
//! [`TaskGroup::silent_async`](rustflow::TaskGroup::silent_async) body is
//! `FnOnce() + Send + 'static` and cannot borrow a frame, so the slot is an
//! `Arc<AtomicU64>` holding an `f64::to_bits` pattern -- the slot
//! `benches/fibonacci.rs`'s group recursion already uses, widened from `u64`
//! to a bit pattern. `Relaxed` on both ends is sound for its reason: what
//! orders the child's store against this frame's load is the `corun`. No
//! `unsafe` and no shared mutable range anywhere in the shape.
//!
//! **What bounds the depth is the error bound; what bounds the size is the
//! stack.** The recursion stops splitting an interval once two trapezoids
//! agree with one to within `epsilon`, which for this cubic happens at a
//! width of about `(2 * epsilon / x)^(1/3)` near `x`: the depth grows as
//! `log(V)` -- 10 levels at `V = 1`, 11 at `V = 2` -- while the *number* of
//! intervals grows as `V^(4/3)`, which is 828 spawned tasks at `V = 1` and
//! 1852 at `V = 2`. The task count is what binds here, and it binds well
//! below upstream's own sweep: `V = 3` is 3478 spawned tasks and **overflows
//! the stack of the one worker** that has to pick them up. That is the
//! nested-corun stack bound, which `DESIGN.md`'s "Executor core" states once
//! for the port and [`TaskGroup::corun`](rustflow::TaskGroup::corun)'s docs
//! name for a caller; where it bites is a property of the tree and the frame
//! rather than one task count, and `BENCHMARKS.md`'s "The #359 measurement"
//! has this shape's own demand, 2.04 MiB of a worker's two at `V = 3`. #342
//! is where the resume that would raise it is measured.
//!
//! **So the sizes are not points on upstream's sweep at all, and that is the
//! finding rather than a shrink.** Upstream's `main.cpp` walks
//! `v = 0, 100, .., 2000`, whose first non-degenerate point, `V = 100`, is
//! some 390_000 spawned tasks (`v = 0` has `x1 == x2`, accepts immediately
//! and integrates nothing). `V = 1` and `V = 2` are two orders of magnitude
//! short of it, and they are what this shape completes at today: measured in
//! a release build at one worker, which is the binding end, with `V = 3` the
//! first that aborts there. `BENCHMARKS.md`'s section says it again where a
//! reader of the table will be looking, including what the other end does --
//! at 24 workers this shape reaches `V = 24`. **Neither size is chosen for
//! speed**, and if the cliff moves, these move with it.
//!
//! Correctness is checked twice before any id is timed, both outside every
//! measured region, because the two checks catch different failures:
//!
//! - **Against the closed form.** `integrate.hpp` carries `integral_fn`, the
//!   exact integral, and upstream never compares against it; each column's
//!   answer here is held to an absolute `1e-5` of it. That bounds the
//!   quadrature, not the schedule, and the bound is `epsilon`'s own: an
//!   accepted interval's trapezoid sum sits within `epsilon / 3` of the true
//!   area over it (the Richardson estimate for a trapezoid), so a tree of `K`
//!   accepted intervals is within `K * epsilon / 3` -- `2.8e-7` at `V = 1`
//!   over its 829 leaves and `6.2e-7` at `V = 2` over its 1853, against a
//!   measured `8.4e-8` and `2.6e-7`. An absolute tolerance rather than a
//!   relative one because `epsilon` is absolute: the areas here are `0.75`
//!   and `6`, so a relative bound would tighten as `V` falls for no reason
//!   the method has.
//! - **Against a sequential reference.** The same recursion with the fork
//!   taken out, through the same [`bisect`] helper, so that both columns
//!   evaluate identical `f64` expressions in an identical association order
//!   -- the tree's shape depends only on each node's own values, never on
//!   which worker ran it, so the sum is schedule-independent to the last bit.
//!   That makes this the check that catches a *dropped* subtree, however
//!   small: the tolerance is a relative `1e-15`, about five ULP, which is
//!   slack for two inlinings of the same helper and nothing else.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustflow::Executor;

#[path = "common/baseline.rs"]
mod baseline;
mod common;

/// The right end of the interval, `[0, V]`. Two orders of magnitude below
/// upstream's own sweep (`v = 0, 100, .., 2000`), because that is where this
/// shape still completes: the module docs have the measured cliff and why it
/// is where it is.
const SIZES: [f64; 2] = [1.0, 2.0];

/// Upstream's `epsilon`: the absolute agreement between one trapezoid and
/// two that accepts an interval instead of splitting it. Unchanged, since it
/// is what shapes the tree this benchmark measures.
const EPSILON: f64 = 1.0e-9;

/// The absolute tolerance each column's answer is held to against the closed
/// form. The module docs derive it from `epsilon` and the interval count.
const CLOSED_FORM_TOLERANCE: f64 = 1e-5;
/// The relative slack the two parallel columns are allowed against the
/// sequential reference, which the module docs derive separately: the sum is
/// schedule-independent, so this is five ULP for two inlinings of one helper
/// and nothing more.
const REFERENCE_TOLERANCE: f64 = 1e-15;

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// Upstream's `fn`, the integrand: `(x * x + 1.0) * x`, in its own
/// association order.
fn integrand(x: f64) -> f64 {
    (x * x + 1.0) * x
}

/// Upstream's `integral_fn`: the exact integral of [`integrand`] over
/// `[a, b]`, through the indefinite `0.25 x^2 (x^2 + 2)`.
fn integral(a: f64, b: f64) -> f64 {
    let indefinite = |x: f64| 0.25 * x * x * (x * x + 2.0);
    indefinite(b) - indefinite(a)
}

/// What one bisection of `[x1, x2]` produces: the midpoint, the integrand
/// there, and the trapezoid area of each half.
///
/// Upstream computes these inline at the top of every `spawn_async` frame;
/// pulling them into one helper is what lets the three variants below share
/// the arithmetic exactly, which the module docs' reference check rests on.
struct Bisection {
    /// The midpoint `x0 = x1 + (x2 - x1) / 2`.
    x0: f64,
    /// `fn(x0)`, upstream's `y0`.
    y0: f64,
    /// Upstream's `area_x1x0`, the left half's trapezoid.
    left: f64,
    /// Upstream's `area_x0x2`, the right half's trapezoid.
    right: f64,
}

/// Upstream's first ten lines of `spawn_async`: bisect, and report whether
/// the two-trapezoid estimate is within `epsilon` of the `area` this frame
/// was handed.
///
/// `Ok(area_x1x2)` is upstream's early return, `Err(bisection)` the branch
/// that spawns. Both of upstream's one-sided comparisons are kept rather than
/// folded into an `abs()`: they are not the same test when a difference is a
/// `NaN`, and the bound is upstream's to port rather than to tidy.
fn bisect(x1: f64, y1: f64, x2: f64, y2: f64, area: f64) -> Result<f64, Bisection> {
    let half = (x2 - x1) / 2.0;
    let x0 = x1 + half;
    let y0 = integrand(x0);

    let left = (y1 + y0) / 2.0 * half;
    let right = (y0 + y2) / 2.0 * half;
    let whole = left + right;

    if whole - area < EPSILON && area - whole < EPSILON {
        Ok(whole)
    } else {
        Err(Bisection {
            x0,
            y0,
            left,
            right,
        })
    }
}

/// Upstream's `spawn_async`: the left half into a group of this interval's
/// own, the right half on this very worker, a `corun` to join them, and the
/// sum of the two areas.
fn integrate_rustflow(
    x1: f64,
    y1: f64,
    x2: f64,
    y2: f64,
    area: f64,
    executor: &Arc<Executor>,
) -> f64 {
    let split = match bisect(x1, y1, x2, y2, area) {
        Ok(whole) => return whole,
        Err(split) => split,
    };

    let group = executor
        .task_group()
        .expect("the recursion runs on a worker, which may open a group");

    let (x0, y0) = (split.x0, split.y0);
    let left_slot = Arc::new(AtomicU64::new(0));
    group.silent_async({
        let executor = Arc::clone(executor);
        let left_slot = Arc::clone(&left_slot);
        let left = split.left;
        move || {
            let area = integrate_rustflow(x1, y1, x0, y0, left, &executor);
            left_slot.store(area.to_bits(), Ordering::Relaxed);
        }
    });

    let right = integrate_rustflow(x0, y0, x2, y2, split.right, executor);

    group.corun();

    f64::from_bits(left_slot.load(Ordering::Relaxed)) + right
}

/// Upstream's `integrate_taskflow`: the recursion has to start on a worker,
/// so the whole of it runs inside one `spawn` whose handle is waited.
fn integrate_rustflow_on(value: f64, executor: &Arc<Executor>) -> f64 {
    executor
        .spawn({
            let executor = Arc::clone(executor);
            move || integrate_rustflow(0.0, integrand(0.0), value, integrand(value), 0.0, &executor)
        })
        .wait()
        .expect("no task under the recursion panicked")
}

/// The same recursion on `rayon::join`, in the role TBB's `task_group` plays
/// upstream. The left half goes first because `join` runs its *second*
/// closure on the forking thread when nobody steals it, which is where
/// upstream's right half runs.
fn integrate_rayon(x1: f64, y1: f64, x2: f64, y2: f64, area: f64) -> f64 {
    let split = match bisect(x1, y1, x2, y2, area) {
        Ok(whole) => return whole,
        Err(split) => split,
    };

    let (left, right) = rayon::join(
        || integrate_rayon(x1, y1, split.x0, split.y0, split.left),
        || integrate_rayon(split.x0, split.y0, x2, y2, split.right),
    );

    left + right
}

/// The baseline's root. `install` holds the recursion to the worker count
/// this row is measuring, as in every other shape here.
fn integrate_rayon_on(value: f64, pool: &rayon::ThreadPool) -> f64 {
    pool.install(|| integrate_rayon(0.0, integrand(0.0), value, integrand(value), 0.0))
}

/// The same recursion with no fork at all, for the untimed comparison: the
/// reference both parallel columns are held to.
fn integrate_sequential(x1: f64, y1: f64, x2: f64, y2: f64, area: f64) -> f64 {
    match bisect(x1, y1, x2, y2, area) {
        Ok(whole) => whole,
        Err(split) => {
            let left = integrate_sequential(x1, y1, split.x0, split.y0, split.left);
            let right = integrate_sequential(split.x0, split.y0, x2, y2, split.right);
            left + right
        }
    }
}

/// Asserts `area` is within [`CLOSED_FORM_TOLERANCE`] of `expected` in
/// absolute terms, which is the units `epsilon` itself is in.
fn assert_near_exact(area: f64, expected: f64, what: &str) {
    let error = (area - expected).abs();
    assert!(
        error <= CLOSED_FORM_TOLERANCE,
        "{what}: {area} is {error:e} from the exact {expected}, \
         over the {CLOSED_FORM_TOLERANCE:e} allowed"
    );
}

/// Asserts `area` is within [`REFERENCE_TOLERANCE`] of the sequential
/// `reference`, relatively.
fn assert_near_reference(area: f64, reference: f64, what: &str) {
    let error = (area - reference).abs() / reference.abs();
    assert!(
        error <= REFERENCE_TOLERANCE,
        "{what}: {area} is a relative {error:e} from the sequential {reference}, \
         over the {REFERENCE_TOLERANCE:e} allowed"
    );
}

/// Runs both variants once, outside any measured region, and checks each
/// against the closed form and against the sequential reference.
///
/// The untimed half of this target's acceptance: a child whose area never
/// reached its parent's slot fails here rather than producing a figure. The
/// module docs say what each of the two checks is for.
fn verify(value: f64, workers: usize, executor: &Arc<Executor>, pool: &rayon::ThreadPool) {
    let exact = integral(0.0, value);
    let reference = integrate_sequential(0.0, integrand(0.0), value, integrand(value), 0.0);
    assert_near_exact(
        reference,
        exact,
        &format!("the sequential reference at V = {value}"),
    );

    let ours = integrate_rustflow_on(value, executor);
    assert_near_exact(ours, exact, &format!("rustflow/{workers}w at V = {value}"));
    assert_near_reference(
        ours,
        reference,
        &format!("rustflow/{workers}w at V = {value}"),
    );

    let theirs = integrate_rayon_on(value, pool);
    assert_near_exact(theirs, exact, &format!("rayon/{workers}w at V = {value}"));
    assert_near_reference(
        theirs,
        reference,
        &format!("rayon/{workers}w at V = {value}"),
    );
}

/// Registers both variants at every size and worker count.
fn bench_integrate(c: &mut Criterion) {
    let mut group = c.benchmark_group("integrate");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in common::worker_counts() {
        // An `Arc` because every interval reaches the executor from a
        // `'static` closure to open a group of its own, as
        // `benches/fibonacci.rs`'s group recursion does.
        let executor = Arc::new(common::executor(workers));
        let pool = baseline::rayon_pool(workers);

        for value in SIZES {
            verify(value, workers, &executor, &pool);

            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), value),
                |b| b.iter(|| integrate_rustflow_on(black_box(value), &executor)),
            );
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), value), |b| {
                b.iter(|| integrate_rayon_on(black_box(value), &pool));
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_integrate);
criterion_main!(benches);

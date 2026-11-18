//! Translated from `benchmarks/black_scholes/` (Taskflow v4.1.0).
//!
//! The one upstream benchmark whose graph is a **loop** rather than a
//! fan-out. `bs_taskflow` builds three nodes: a `placeholder`, a
//! `for_each_index(0, numOptions, 1, ..., StaticPartitioner())` over the
//! `BlkSchlsEqEuroNoDiv` kernel, and a condition task that returns `0` to
//! select the loop again and `-1` to select nothing and so end the run. One
//! run of the taskflow therefore prices every option `NUM_RUNS` times, and
//! what is measured on top of the arithmetic is the condition task
//! re-dispatching a `for_each_index` module that many times.
//!
//! Upstream's `generate_options` couples the two counts --
//! `NUM_RUNS = numOptions` -- so the kernel runs `N * N` times for `N`
//! options. This port keeps the coupling; [`num_runs`] is where it is named.
//!
//! **The rayon baseline is this loop with the graph taken out of it**: a
//! `for` over `NUM_RUNS` around `par_iter_mut`, which is exactly what
//! upstream's own `tbb.cpp` writes with `tbb::parallel_for`. The two sides do
//! the same arithmetic the same number of times; what differs is that a lap
//! boundary costs rayon one implicit barrier at the end of its parallel
//! iterator, where it costs this crate a condition task, an index-range
//! module re-dispatched from it and the join that ends the module.
//!
//! Four departures from upstream, each because the benchmark is about the
//! executor and not about option pricing:
//!
//! - **The options are generated, not read.** Upstream's comment says "Read
//!   input option data from file", but the function under it generates them
//!   from `::rand()` and no file is involved. `rand()` is libc's, so
//!   upstream's data is not reproducible off its own machine anyway; these
//!   come from the port's own [`Xorshift32`] with a fixed [`SEED`], in
//!   upstream's own ranges, the way `graph_traversal.rs` owns its random
//!   graph. [`Options::generate`] states the distribution.
//! - **No degenerate draw.** Upstream draws a spot and a strike price from
//!   `rand() % 200` and a volatility and a time to expiry from
//!   `rand() % 100 / 100.0f`, so zero is in range for all four -- and each
//!   zero puts the kernel somewhere it has no answer (`log(0)`, or a
//!   division by `volatility * sqrt(time)`), which upstream never notices
//!   because it compares its prices against a `DGrefval` that is itself a
//!   random integer. Every draw here is shifted up by one, which keeps the
//!   span of each range and makes a price comparison meaningful.
//! - **The shared price array is a slice of [`AtomicU32`]** holding
//!   [`f32::to_bits`], because the index body a `for_each_index` task takes
//!   is `Fn(usize)` and cannot be handed a disjoint `&mut f32` the way
//!   `par_iter_mut` hands one to the baseline. That is one relaxed 32-bit
//!   store per option against a kernel of two `exp`s, a `ln` and a `sqrt`,
//!   and it is the one asymmetry between the two columns.
//! - **The kernel computes in `f32` throughout.** Upstream's `float`
//!   variables are assigned from `double` literals, so each statement there
//!   rounds a `double` product back to `float`; a few ULP either way is
//!   invisible to a benchmark, and both columns here read the same
//!   [`Options::price`].

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator};
use rustflow::algorithm::partitioner::{DefaultClosureWrapper, StaticPartitioner};
use rustflow::prelude::*;
use rustflow::{Executor, Xorshift32};

#[path = "common/baseline.rs"]
mod baseline;
mod common;

/// Option counts, two points off upstream's own sweep (`main.cpp` walks
/// `N = 1000..=10000` in steps of 1000): one where the loop's own per-lap
/// cost still shows, one where the per-option arithmetic dominates it.
///
/// The coupling [`num_runs`] names is what makes those two the same sweep
/// read from both ends -- work grows as `N * N` while the number of
/// re-dispatches grows as `N`, so the larger the size, the smaller the share
/// of a run that the condition task's loop can account for. Upstream's last
/// point, `N = 10000`, is `10^8` kernel evaluations in a single sample and is
/// left off for that reason alone.
const SIZES: [usize; 2] = [1_000, 4_000];

/// The generator seed. Upstream seeds nothing (`::rand()` without `srand`);
/// this exists so the options are the same options on every run.
const SEED: u32 = 1;

/// How far a measured variant's price may sit from the sequential
/// reference's: upstream's own `ERR_CHK` threshold, which rejects a
/// `priceDelta` of `1e-4` or more.
///
/// Both sides evaluate the same [`Options::price`] on the same inputs, so the
/// only difference either is entitled to is a last-bit one between two
/// inlined copies of the kernel -- an `f32` ULP at the largest price these
/// options can reach (a spot price of 200) is about `1.5e-5`, so this leaves
/// room for a handful of them. What it does not leave room for is a
/// scheduling fault: an option no lap reached keeps the [`f32::NAN`] the
/// array is filled with, and a comparison against a NaN is false whatever the
/// tolerance.
const TOLERANCE: f32 = 1e-4;

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// `inv_sqrt_2xPI`, upstream's macro: `1 / sqrt(2 * PI)`.
const INV_SQRT_2XPI: f32 = 0.398_942_3;

/// Upstream's `NUM_RUNS`, which `generate_options` sets to `numOptions`: one
/// run of the graph prices every option this many times.
///
/// A named function rather than an inlined `size`, because the coupling is
/// what the two entries of [`SIZES`] are read against.
fn num_runs(num_options: usize) -> usize {
    num_options
}

/// Upstream's `CNDF`: the cumulative normal distribution function, by Hull's
/// five-term rational approximation of its tail.
fn cndf(input: f32) -> f32 {
    let negative = input < 0.0;
    let x = input.abs();

    let n_prime_of_x = (-0.5 * x * x).exp() * INV_SQRT_2XPI;

    let k2 = 1.0 / (1.0 + 0.231_641_9 * x);
    let k2_2 = k2 * k2;
    let k2_3 = k2_2 * k2;
    let k2_4 = k2_3 * k2;
    let k2_5 = k2_4 * k2;

    // Upstream's summation order: the four higher terms first, into
    // `xLocal_2`, then the linear one.
    let tail = k2_2 * -0.356_563_8 + k2_3 * 1.781_477_9 + k2_4 * -1.821_256 + k2_5 * 1.330_274_4;
    let local = (tail + k2 * 0.319_381_53) * n_prime_of_x;

    let output = 1.0 - local;
    if negative { 1.0 - output } else { output }
}

/// Upstream's `BlkSchlsEqEuroNoDiv`: the Black-Scholes price of one European
/// option paying no dividend. `otype` is upstream's, `0` for a call and
/// anything else for a put; upstream's trailing unused `float` parameter is
/// dropped.
fn blk_schls_eq_euro_no_div(
    spot: f32,
    strike: f32,
    rate: f32,
    volatility: f32,
    time: f32,
    otype: i32,
) -> f32 {
    let sqrt_time = time.sqrt();
    let log_term = (spot / strike).ln();
    let power_term = volatility * volatility * 0.5;
    let den = volatility * sqrt_time;

    let d1 = ((rate + power_term) * time + log_term) / den;
    let d2 = d1 - den;

    let n_of_xd1 = cndf(d1);
    let n_of_xd2 = cndf(d2);
    let future_value_x = strike * (-rate * time).exp();

    if otype == 0 {
        spot * n_of_xd1 - future_value_x * n_of_xd2
    } else {
        future_value_x * (1.0 - n_of_xd2) - spot * (1.0 - n_of_xd1)
    }
}

/// A draw in `1..=bound`, as an `f32`: upstream's `::rand() % bound` with
/// zero excluded, for the reason the module docs give.
///
/// `u16` is the widest integer `f32` converts from without loss, so this is a
/// [`From`] and not an `as` cast; every `bound` passed below is far inside it.
fn draw(rng: &mut Xorshift32, bound: u32) -> f32 {
    let value = 1 + rng.next_u32() % bound;
    f32::from(u16::try_from(value).expect("a draw at most `bound` fits in a u16"))
}

/// Upstream's `OptionData`, in the struct-of-arrays shape upstream's own
/// `BUFFER` slicing leaves it in: the kernel reads index `i` out of parallel
/// arrays rather than a struct out of one.
struct Options {
    /// Upstream's `sptprice`: the spot price.
    spot: Vec<f32>,
    /// Upstream's `strike`: the strike price.
    strike: Vec<f32>,
    /// Upstream's `rate`: the risk-free interest rate.
    rate: Vec<f32>,
    /// Upstream's `volatility`.
    volatility: Vec<f32>,
    /// Upstream's `otime`: years to expiry, `1.0` being one year.
    time: Vec<f32>,
    /// Upstream's `otype`: `0` for a call ('C'), `1` for a put ('P').
    otype: Vec<i32>,
}

impl Options {
    /// Generates `n` options from [`SEED`], in upstream's own ranges shifted
    /// off zero: a spot and a strike price uniform on the integers `1..=200`,
    /// a volatility and a time to expiry uniform on the hundredths
    /// `0.01..=1.00`, a risk-free rate fixed at upstream's `0.1`, and a call
    /// or a put with even odds.
    ///
    /// Upstream's `divq`, `divs` and `DGrefval` are dropped with the draw
    /// that fills the last of them: the kernel reads none of the three, and
    /// the only thing `DGrefval` feeds is an `ERR_CHK` comparison against a
    /// random integer.
    fn generate(n: usize) -> Self {
        let mut rng = Xorshift32::new(SEED);
        let mut options = Self {
            spot: Vec::with_capacity(n),
            strike: Vec::with_capacity(n),
            rate: Vec::with_capacity(n),
            volatility: Vec::with_capacity(n),
            time: Vec::with_capacity(n),
            otype: Vec::with_capacity(n),
        };

        for _ in 0..n {
            options.spot.push(draw(&mut rng, 200));
            options.strike.push(draw(&mut rng, 200));
            options.rate.push(0.1);
            options.volatility.push(draw(&mut rng, 100) / 100.0);
            options.time.push(draw(&mut rng, 100) / 100.0);
            options.otype.push(i32::from(rng.next_u32() % 2 == 1));
        }

        options
    }

    /// Upstream's `numOptions`.
    fn len(&self) -> usize {
        self.otype.len()
    }

    /// The price upstream's `BlkSchlsEqEuroNoDiv` gives option `i`.
    ///
    /// Every variant below prices through this one method, so a disagreement
    /// between two of them is a scheduling fault and never an arithmetic
    /// difference.
    fn price(&self, i: usize) -> f32 {
        blk_schls_eq_euro_no_div(
            self.spot[i],
            self.strike[i],
            self.rate[i],
            self.volatility[i],
            self.time[i],
            self.otype[i],
        )
    }

    /// How many of these options are puts, which is what decides which branch
    /// of the kernel's own `otype` test each one takes.
    fn puts(&self) -> usize {
        self.otype.iter().filter(|&&otype| otype != 0).count()
    }
}

/// Upstream's `bs_taskflow`: the three-node graph with the condition task's
/// loop in it, built and run once. Returns the number of laps the condition
/// task counted, which [`check`] holds to [`num_runs`].
///
/// The taskflow is constructed inside the measured region because upstream's
/// `measure_time_taskflow` constructs it inside its own -- BENCHMARKS.md's
/// "ours includes building the graph".
fn price_rustflow(
    options: &Arc<Options>,
    prices: &Arc<Vec<AtomicU32>>,
    executor: &Executor,
) -> usize {
    let num_options = options.len();
    let laps = Arc::new(AtomicUsize::new(0));
    let mut taskflow = Taskflow::new();

    let init = taskflow.placeholder();
    let loop_task = {
        let options = Arc::clone(options);
        let prices = Arc::clone(prices);
        taskflow.for_each_index(
            0_usize,
            num_options,
            1,
            move |i| prices[i].store(options.price(i).to_bits(), Ordering::Relaxed),
            // Upstream's `tf::StaticPartitioner()`: equal chunks decided
            // before the loop starts, which is what an iteration of uniform
            // cost wants.
            StaticPartitioner::<DefaultClosureWrapper>::default(),
        )
    };
    let cond = {
        let laps = Arc::clone(&laps);
        let runs = num_runs(num_options);
        // Upstream's `[i=0]() mutable { return ++i == NUM_RUNS ? -1 : 0; }`,
        // whose counter is this closure's own state rather than a shared one:
        // the store below happens once per run, not once per lap, so the
        // check this feeds costs the measured region one relaxed store.
        let mut i = 0_usize;
        taskflow.emplace_condition(move || {
            i += 1;
            if i == runs {
                laps.store(i, Ordering::Relaxed);
                -1
            } else {
                0
            }
        })
    };

    taskflow.task(init).precede([loop_task]);
    taskflow.task(loop_task).precede([cond]);
    // `cond`'s only successor, selected by its `0` and left unselected by its
    // `-1`: the loop edge that makes this graph cyclic.
    taskflow.task(cond).precede([loop_task]);

    executor.run(&taskflow).wait().expect("no task panicked");

    laps.load(Ordering::Relaxed)
}

/// Upstream's `bs_tbb` with rayon in TBB's place: the same loop once the
/// graph is gone, a `for` over `NUM_RUNS` around a parallel iterator over the
/// options, with one implicit barrier a lap.
fn price_rayon(options: &Arc<Options>, prices: &mut [f32], pool: &rayon::ThreadPool) {
    let runs = num_runs(options.len());
    pool.install(|| {
        for _ in 0..runs {
            prices
                .par_iter_mut()
                .enumerate()
                .for_each(|(i, price)| *price = options.price(i));
        }
    });
}

/// Upstream's `bs_seq`, over a single lap: every lap of it writes the same
/// value into the same slot, so `NUM_RUNS` of them say nothing one pass does
/// not.
fn price_sequential(options: &Options) -> Vec<f32> {
    (0..options.len()).map(|i| options.price(i)).collect()
}

/// The untimed check each size runs before it is measured: upstream's
/// `-s,--seq` comparison, which upstream leaves commented out in `main`.
///
/// Both measured variants price the options once here and both outputs are
/// compared against [`price_sequential`], element by element, within
/// [`TOLERANCE`] -- so an option no lap reached, a lap the condition task
/// skipped or a price written to the wrong slot fails the benchmark rather
/// than making it look fast.
fn check(
    options: &Arc<Options>,
    prices: &Arc<Vec<AtomicU32>>,
    rayon_prices: &mut [f32],
    executor: &Executor,
    pool: &rayon::ThreadPool,
) {
    let reference = price_sequential(options);
    let puts = options.puts();
    assert!(
        puts > 0 && puts < options.len(),
        "the generated options reach both branches of the kernel's `otype` test: \
         {puts} puts out of {}",
        options.len()
    );

    let laps = price_rustflow(options, prices, executor);
    assert_eq!(
        laps,
        num_runs(options.len()),
        "the condition task looped `NUM_RUNS` times"
    );
    for (i, &want) in reference.iter().enumerate() {
        let got = f32::from_bits(prices[i].load(Ordering::Relaxed));
        assert!(
            (got - want).abs() <= TOLERANCE,
            "option {i}: the taskflow priced it {got}, the sequential reference {want}"
        );
    }

    price_rayon(options, rayon_prices, pool);
    for (i, &want) in reference.iter().enumerate() {
        let got = rayon_prices[i];
        assert!(
            (got - want).abs() <= TOLERANCE,
            "option {i}: the baseline priced it {got}, the sequential reference {want}"
        );
    }
}

/// Registers both variants at every size and worker count.
fn bench_black_scholes(c: &mut Criterion) {
    let mut group = c.benchmark_group("black_scholes");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in common::worker_counts() {
        let executor = common::executor(workers);
        let pool = baseline::rayon_pool(workers);

        for size in SIZES {
            let options = Arc::new(Options::generate(size));
            // NaN rather than zero, so that a slot no lap wrote fails
            // [`check`]'s comparison instead of reading as a price.
            let prices: Arc<Vec<AtomicU32>> = Arc::new(
                (0..size)
                    .map(|_| AtomicU32::new(f32::NAN.to_bits()))
                    .collect(),
            );
            let mut rayon_prices = vec![f32::NAN; size];

            check(&options, &prices, &mut rayon_prices, &executor, &pool);

            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| {
                    b.iter(|| price_rustflow(black_box(&options), &prices, &executor));
                },
            );
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), size), |b| {
                b.iter(|| price_rayon(black_box(&options), &mut rayon_prices, &pool));
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_black_scholes);
criterion_main!(benches);

//! Translated from `benchmarks/data_pipeline/` (Taskflow v4.1.0).
//!
//! **Two measurements live in this file, and only the first is a
//! translation.** The provenance line above belongs to the `data_pipeline`
//! group alone -- upstream's own shape, a [`DataPipeline`] over typed pipes.
//! The `data_pipeline_slot` group below it translates nothing: it is #188's
//! per-decision measurement of this port's own per-line slot, a shape
//! upstream has no equivalent of, and it keeps its ids where
//! `BENCHMARKS.md`'s #186/#188 section already quotes them rather than moving
//! to a file of its own.
//!
//! # Upstream's shape: a typed chain whose pipes change type
//!
//! `benchmarks/data_pipeline/taskflow.cpp`'s
//! `parallel_pipeline_taskflow_4_pipes` is a `tf::DataPipeline` of
//! [`UPSTREAM_PIPES`] `tf::make_data_pipe`s over [`UPSTREAM_LINES`] lines --
//! its `-l` default -- composed into a `tf::Taskflow` and run once, which is
//! what `measure_time_taskflow` times and so what the measured region here
//! contains. The chain's point is that each pipe's output type differs from
//! its input type: `Pipeflow& -> int`, then upstream's `int2float`,
//! `float2int` and `int2void`, so every value crossing a pipe boundary needs
//! somewhere typed to live. Every pipe is [`PipeType::Serial`], which is
//! upstream's `-p` default (`"ssssssss"`) and not an omission; the
//! concurrency a serial chain still has is across stages, one line's token in
//! the last pipe while the next line's is still in the first.
//!
//! Two departures, which `BENCHMARKS.md`'s own section repeats next to the
//! figures:
//!
//! - **The per-pipe work is the shared [`body::dummy`], not upstream's
//!   `work_int`/`work_float`.** Upstream's are 100-iteration `cos`/`pow`
//!   loops that also transform the value they are handed; the rest of this
//!   suite measures a pipe body with the 32-iteration floating-point loop
//!   `common/body.rs` holds, folded into the value so the chain still carries
//!   something every pipe before it contributed to.
//! - **`float2int` reinterprets rather than casts.** Upstream's body ends in
//!   a C `(int)` cast of a `float`, which in Rust is the saturating `as` cast
//!   this crate's `cast_possible_truncation` lint exists to catch and which
//!   no benchmark may `#[allow]` away. [`float_to_int`] takes the `f64`'s
//!   bits instead -- total, deterministic and `as`-free -- because what the
//!   measurement needs from that pipe is that its output type differs from
//!   its input type and that its value still depends on every pipe before
//!   it, not the particular arithmetic C performs.
//!
//! There is no rayon baseline, for the reason `benches/pipeline.rs` gives at
//! length: rayon has no pipeline primitive. The baseline is
//! [`UPSTREAM_PIPES`] `std::thread`s chained by [`std::sync::mpsc`] channels
//! typed as the pipes are -- `i64`, then `f64`, then `i64` -- in the role
//! `tbb::parallel_pipeline` plays upstream's own `tbb.cpp`. It has no
//! `lines`/`workers` axis, an mpsc stage being one thread whatever
//! `PipeType` the pipe it stands in for would have been, so it is measured
//! once per size and read against every row of that size. Both sides' answer
//! is checked against [`reference_checksum`], a sequential fold of the same
//! chain, in the *untimed* part of this benchmark, before either id is
//! registered.
//!
//! # The #188 measurement: a per-line slot against a user-owned buffer
//!
//! The per-decision measurement issue #188 adds: #186's own per-line slot
//! (`Mutex<Option<Box<dyn Any + Send>>>`, 24 bytes on a 64-bit target --
//! `src/pipeline/data_pipeline.rs`'s own module docs give the layout), timed
//! against a [`Pipeline`] of the same shape carrying its value through a
//! user-owned buffer instead of a typed chain. The chain is
//! `benches/pipeline.rs`'s own `S, P, S, P` and the per-token work is the
//! shared [`body::dummy`], so the one thing the two variants differ in is
//! where a pipe's output goes.
//!
//! **The allocation count is stated, not measured by a counting allocator.**
//! A `#[global_allocator]` wrapping `std::alloc::System` is the only way to
//! count heap operations from inside a benchmark, and implementing
//! `GlobalAlloc` is `unsafe` -- out of scope for this change. The count
//! below instead reads off `DataPipelineBuilder::pipe`'s own body
//! (`src/pipeline/data_pipeline.rs`): every pipe invocation ends by writing
//! `Some(Box::new(output))` into its line's slot, one allocation, and every
//! invocation but a chain's first also opens by `take`ing and downcasting
//! the predecessor's box, which drops -- deallocates -- it once the value is
//! moved out. So a chain of `P` pipes costs exactly `P` allocations and `P`
//! deallocations per token in steady state: `P - 1` of each within the one
//! token's own lap through the chain, plus the one that frees the previous
//! token's last-pipe box when the first pipe of the next lap overwrites its
//! line's slot. For [`NUM_PIPES`] `= 4` that is **4 allocations and 4
//! deallocations a token**, independent of `lines` or `workers`: the
//! `Pipeline` variant below does zero of either, since its buffer is
//! allocated once, outside the measured loop.

use std::hint::black_box;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustflow::Executor;
use rustflow::pipeline::data_pipeline::{
    DataPipeline, parallel_pipe_with_flow, serial_pipe, serial_pipe_with_flow,
};
use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
use rustflow::pipeline::scheduler::Pipeline;
use rustflow::prelude::*;

#[path = "common/body.rs"]
mod body;
mod common;

/// The number of pipes in the chain both `data_pipeline_slot` variants
/// measure: serial first, then alternating serial/parallel --
/// `benches/pipeline.rs`'s own shape, duplicated here so a value carries
/// through it rather than being ignored.
const NUM_PIPES: usize = 4;

/// Line counts both `data_pipeline_slot` variants sweep:
/// `benches/pipeline.rs`'s own `LINES`, trimmed to the two ends -- that
/// measurement is about the slot's cost, not a fresh sweep over how many
/// lines help.
const LINES: [usize; 2] = [1, 8];

/// Token counts both groups in this file sweep, the same pair
/// `benches/pipeline.rs`'s own `SIZES` sweeps and two points off upstream's
/// own `L = 1 << i` for `i` in `1..=23`: one where fixed cost still shows,
/// one where the per-token cost dominates it.
const SIZES: [usize; 2] = [1024, 16384];

/// Pipes in upstream's own chain: `parallel_pipeline_taskflow_4_pipes`, so
/// `Pipeflow& -> int`, `int2float`, `float2int` and `int2void`.
const UPSTREAM_PIPES: usize = 4;

/// Lines upstream's own chain runs over: its `-l` default.
const UPSTREAM_LINES: usize = 8;

/// Samples per measurement, and the time criterion may spend reaching them --
/// `benches/pipeline.rs`'s own values.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// Upstream's `int2float`, the chain's `int -> float` pipe: its own
/// 100-iteration `work_int` loop is the shared [`body::dummy`] here, folded
/// into the value the pipe hands on.
fn int_to_float(input: i64) -> f64 {
    black_box(body::dummy(input.unsigned_abs())) + input as f64
}

/// Upstream's `float2int`, the chain's `float -> int` pipe, which closes its
/// type cycle.
///
/// The bit reinterpretation rather than a cast is this file's second
/// documented departure -- see the module docs. It is total, so no input can
/// make it panic or saturate, and deterministic, which is what
/// [`reference_checksum`] needs of it.
fn float_to_int(value: f64) -> i64 {
    let worked = value + black_box(body::dummy(value.to_bits()));
    i64::from_ne_bytes(worked.to_bits().to_ne_bytes())
}

/// Upstream's `int2void`, the chain's last pipe, plus the fold that makes a
/// run's answer checkable against [`reference_checksum`].
fn int_to_void(input: i64, checksum: &AtomicI64) {
    black_box(body::dummy(input.unsigned_abs()));
    // `fetch_add` on an `AtomicI64` wraps, which is what makes the sum
    // independent of the order the lines happen to finish their tokens in
    // and so comparable to a sequential fold of the same chain.
    checksum.fetch_add(input, Ordering::Relaxed);
}

/// The same chain folded sequentially, with no pipeline at all: the answer
/// both measured variants of upstream's shape are checked against.
fn reference_checksum(num_tokens: usize) -> i64 {
    let mut checksum = 0_i64;

    for token in 0..num_tokens {
        let as_int = i64::try_from(token).expect("a token index fits in an i64");
        checksum = checksum.wrapping_add(float_to_int(int_to_float(as_int)));
    }

    checksum
}

/// Runs `num_tokens` through a freshly built [`DataPipeline`] of `num_lines`
/// typed pipes in upstream's own shape -- `Pipeflow& -> int`, `int2float`,
/// `float2int`, `int2void`, every pipe serial -- composed into a freshly
/// built [`Taskflow`]. Hands back the chain's checksum.
fn run_upstream(num_lines: usize, num_tokens: usize, executor: &Executor) -> i64 {
    let checksum = Arc::new(AtomicI64::new(0));

    let pipeline = DataPipeline::builder(num_lines)
        .pipe(serial_pipe_with_flow::<(), i64>(move |(), pf| {
            if pf.token() == num_tokens {
                pf.stop();
                return 0;
            }
            i64::try_from(pf.token()).expect("a token index fits in an i64")
        }))
        .pipe(serial_pipe::<i64, f64>(int_to_float))
        .pipe(serial_pipe::<f64, i64>(float_to_int))
        .pipe(serial_pipe::<i64, ()>({
            let checksum = Arc::clone(&checksum);
            move |input| int_to_void(input, &checksum)
        }))
        .build();

    let mut taskflow = Taskflow::new();
    let module = taskflow.composed_of(&pipeline);
    taskflow.task(module).name("module_of_data_pipeline");

    executor.run(&taskflow).wait().expect("no task panicked");

    // The only thing standing between this benchmark and timing a pipeline
    // that stopped before its first pipe ever ran -- and, for the second,
    // keeping `UPSTREAM_PIPES` honest about the chain built above.
    assert_eq!(
        pipeline.num_tokens(),
        num_tokens,
        "every token reaches the last pipe exactly once"
    );
    assert_eq!(
        pipeline.num_pipes(),
        UPSTREAM_PIPES,
        "upstream's four-pipe chain"
    );

    checksum.load(Ordering::Relaxed)
}

/// The same chain as [`UPSTREAM_PIPES`] freshly spawned threads chained by
/// [`std::sync::mpsc`] channels typed as the pipes are -- the baseline's
/// whole shape, in the role `tbb::parallel_pipeline` plays upstream's own
/// `tbb.cpp`. Hands back the chain's checksum, as [`run_upstream`] does.
///
/// The stages are written out rather than looped the way
/// `benches/pipeline.rs`'s untyped chain is: each channel here carries a
/// different type, which is the property of upstream's shape this baseline
/// exists to pay for too.
fn run_mpsc_chain(num_tokens: usize) -> i64 {
    let (int_tx, int_rx) = mpsc::channel::<i64>();
    let (float_tx, float_rx) = mpsc::channel::<f64>();
    let (back_tx, back_rx) = mpsc::channel::<i64>();

    let input = thread::spawn(move || {
        for token in 0..num_tokens {
            let as_int = i64::try_from(token).expect("a token index fits in an i64");
            int_tx
                .send(as_int)
                .expect("the next stage is still receiving");
        }
    });

    let to_float = thread::spawn(move || {
        while let Ok(input) = int_rx.recv() {
            float_tx
                .send(int_to_float(input))
                .expect("the next stage is still receiving");
        }
    });

    let to_int = thread::spawn(move || {
        while let Ok(value) = float_rx.recv() {
            back_tx
                .send(float_to_int(value))
                .expect("the next stage is still receiving");
        }
    });

    let sink = thread::spawn(move || {
        let checksum = AtomicI64::new(0);
        let mut count = 0_usize;
        while let Ok(input) = back_rx.recv() {
            int_to_void(input, &checksum);
            count += 1;
        }
        (checksum.load(Ordering::Relaxed), count)
    });

    for handle in [input, to_float, to_int] {
        handle.join().expect("a stage thread does not panic");
    }
    let (checksum, count) = sink.join().expect("a stage thread does not panic");

    assert_eq!(
        count, num_tokens,
        "every token reaches the last stage exactly once"
    );

    checksum
}

/// Registers upstream's own shape: the `mpsc` baseline once per size and the
/// `rustflow` variant at every size and worker count.
fn bench_data_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_pipeline");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    // Computed once per size and outside every measured region: a chain that
    // drops or double-counts a token fails the benchmark rather than running
    // faster than one that does not.
    let expected: Vec<i64> = SIZES.iter().map(|&size| reference_checksum(size)).collect();

    // The mpsc baseline has no `lines` or `workers` axis -- see the module
    // docs -- so it is registered once per size rather than once per row of
    // the table `BENCHMARKS.md` reads this against.
    for (size, &expected) in SIZES.iter().zip(&expected) {
        assert_eq!(
            run_mpsc_chain(*size),
            expected,
            "the mpsc chain folds every token exactly as the sequential chain does"
        );
        group.bench_function(BenchmarkId::new("mpsc", size), |b| {
            b.iter(|| run_mpsc_chain(black_box(*size)));
        });
    }

    for workers in common::worker_counts() {
        let executor = common::executor(workers);

        for (size, &expected) in SIZES.iter().zip(&expected) {
            assert_eq!(
                run_upstream(UPSTREAM_LINES, *size, &executor),
                expected,
                "the data pipeline folds every token exactly as the sequential chain does"
            );
            group.bench_function(
                BenchmarkId::new(format!("rustflow/{UPSTREAM_LINES}l_{workers}w"), size),
                |b| b.iter(|| run_upstream(UPSTREAM_LINES, black_box(*size), &executor)),
            );
        }
    }

    group.finish();
}

/// Runs `num_tokens` through a freshly built [`DataPipeline`] of `num_lines`
/// lines, carrying an `f64` through its typed per-line slot from one pipe to
/// the next: `benches/pipeline.rs`'s own `S, P, S, P` shape, over
/// [`NUM_PIPES`] pipes.
fn run_typed(num_lines: usize, num_tokens: usize, executor: &Executor) {
    let processed = Arc::new(AtomicUsize::new(0));

    let pipe0 = serial_pipe_with_flow::<(), f64>(move |(), pf| {
        if pf.token() == num_tokens {
            pf.stop();
            return 0.0;
        }
        let token = u64::try_from(pf.token()).expect("a token index fits in a u64");
        value_after(0.0, token)
    });

    let pipe1 = parallel_pipe_with_flow::<f64, f64>(|value, pf| {
        let token = u64::try_from(pf.token()).expect("a token index fits in a u64");
        value_after(value, token)
    });

    let pipe2 = serial_pipe_with_flow::<f64, f64>(|value, pf| {
        let token = u64::try_from(pf.token()).expect("a token index fits in a u64");
        value_after(value, token)
    });

    let pipe3 = {
        let processed = Arc::clone(&processed);
        parallel_pipe_with_flow::<f64, ()>(move |value, pf| {
            let token = u64::try_from(pf.token()).expect("a token index fits in a u64");
            black_box(value_after(value, token));
            processed.fetch_add(1, Ordering::Relaxed);
        })
    };

    let pipeline = DataPipeline::builder(num_lines)
        .pipe(pipe0)
        .pipe(pipe1)
        .pipe(pipe2)
        .pipe(pipe3)
        .build();

    let mut taskflow = Taskflow::new();
    let module = taskflow.composed_of(&pipeline);
    taskflow.task(module).name("module_of_data_pipeline");

    executor.run(&taskflow).wait().expect("no task panicked");

    assert_eq!(
        processed.load(Ordering::Relaxed),
        num_tokens,
        "every token reaches the last pipe exactly once"
    );
}

/// [`body::dummy`]'s cost folded into a running value, so the typed
/// variant's slot carries something that depends on every pipe's own work
/// rather than a constant the optimizer could hoist out of the chain.
fn value_after(value: f64, token: u64) -> f64 {
    value + black_box(body::dummy(token))
}

/// A pipeline's `lines * pipes` scratch buffer: `tests/pipelines_compose.rs`'s
/// own `Buffer`, narrowed to `f64` and a fixed [`NUM_PIPES`] width, standing
/// in for the typed slot [`run_typed`] measures against.
#[derive(Clone)]
struct Buffer {
    rows: Arc<Vec<Mutex<[f64; NUM_PIPES]>>>,
}

impl Buffer {
    fn new(num_lines: usize) -> Self {
        Self {
            rows: Arc::new(
                (0..num_lines)
                    .map(|_| Mutex::new([0.0; NUM_PIPES]))
                    .collect(),
            ),
        }
    }

    fn set(&self, line: usize, pipe: usize, value: f64) {
        self.rows[line].lock().expect("uncontended")[pipe] = value;
    }

    fn get(&self, line: usize, pipe: usize) -> f64 {
        self.rows[line].lock().expect("uncontended")[pipe]
    }
}

/// Runs `num_tokens` through a freshly built untyped [`Pipeline`] of
/// `num_lines` lines, carrying its value through a user-owned [`Buffer`]
/// rather than the typed slot [`run_typed`] measures: the same `S, P, S, P`
/// shape and the same per-token [`body::dummy`] cost.
fn run_buffered(num_lines: usize, num_tokens: usize, executor: &Executor) {
    let processed = Arc::new(AtomicUsize::new(0));
    let buffer = Buffer::new(num_lines);

    let mut pipes = Vec::with_capacity(NUM_PIPES);
    pipes.push(Pipe::new(PipeType::Serial, {
        let buffer = buffer.clone();
        move |pf: &mut Pipeflow| {
            if pf.token() == num_tokens {
                pf.stop();
                return;
            }
            let token = u64::try_from(pf.token()).expect("a token index fits in a u64");
            buffer.set(pf.line(), 0, value_after(0.0, token));
        }
    }));

    for stage in 1..NUM_PIPES {
        let kind = if stage % 2 == 1 {
            PipeType::Parallel
        } else {
            PipeType::Serial
        };
        let is_last = stage == NUM_PIPES - 1;
        let buffer = buffer.clone();
        let processed = Arc::clone(&processed);
        pipes.push(Pipe::new(kind, move |pf: &mut Pipeflow| {
            let token = u64::try_from(pf.token()).expect("a token index fits in a u64");
            let previous = buffer.get(pf.line(), stage - 1);
            let value = value_after(previous, token);
            if is_last {
                black_box(value);
                processed.fetch_add(1, Ordering::Relaxed);
            } else {
                buffer.set(pf.line(), stage, value);
            }
        }));
    }

    let pipeline = Pipeline::new(num_lines, pipes);
    let mut taskflow = Taskflow::new();
    let module = taskflow.composed_of(&pipeline);
    taskflow.task(module).name("module_of_pipeline");

    executor.run(&taskflow).wait().expect("no task panicked");

    assert_eq!(
        processed.load(Ordering::Relaxed),
        num_tokens,
        "every token reaches the last pipe exactly once"
    );
}

/// Registers the `typed` ([`DataPipeline`]) and `buffer` ([`Pipeline`])
/// variants at every line count, size and worker count.
fn bench_data_pipeline_slot(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_pipeline_slot");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in common::worker_counts() {
        let executor = common::executor(workers);

        for lines in LINES {
            for size in SIZES {
                group.bench_function(
                    BenchmarkId::new(format!("typed/{lines}l_{workers}w"), size),
                    |b| b.iter(|| run_typed(lines, black_box(size), &executor)),
                );
                group.bench_function(
                    BenchmarkId::new(format!("buffer/{lines}l_{workers}w"), size),
                    |b| b.iter(|| run_buffered(lines, black_box(size), &executor)),
                );
            }
        }
    }

    group.finish();
}

criterion_group!(benches, bench_data_pipeline, bench_data_pipeline_slot);
criterion_main!(benches);

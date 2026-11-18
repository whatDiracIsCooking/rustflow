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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustflow::Executor;
use rustflow::pipeline::data_pipeline::{
    DataPipeline, parallel_pipe_with_flow, serial_pipe_with_flow,
};
use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
use rustflow::pipeline::scheduler::Pipeline;
use rustflow::prelude::*;

#[path = "common/body.rs"]
mod body;
mod common;

/// The number of pipes in the chain both variants measure: serial first,
/// then alternating serial/parallel -- `benches/pipeline.rs`'s own shape,
/// duplicated here so a value carries through it rather than being ignored.
const NUM_PIPES: usize = 4;

/// Line counts both variants sweep: `benches/pipeline.rs`'s own `LINES`,
/// trimmed to the two ends -- this measurement is about the slot's cost, not
/// a fresh sweep over how many lines help.
const LINES: [usize; 2] = [1, 8];

/// Token counts, the same pair `benches/pipeline.rs`'s own `SIZES` sweeps:
/// one where fixed cost still shows, one where the per-token cost dominates
/// it.
const SIZES: [usize; 2] = [1024, 16384];

/// Samples per measurement, and the time criterion may spend reaching them --
/// `benches/pipeline.rs`'s own values.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// Runs `num_tokens` through a freshly built [`DataPipeline`] of `num_lines`
/// lines, carrying an `f64` through its typed per-line slot from one pipe to
/// the next: upstream's `S, P, S, P` shape, over [`NUM_PIPES`] pipes.
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

criterion_group!(benches, bench_data_pipeline_slot);
criterion_main!(benches);

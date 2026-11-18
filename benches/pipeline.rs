//! Translated from `benchmarks/linear_pipeline/` (Taskflow v4.1.0).
//!
//! Upstream's `linear_pipeline` sweeps a chain of pipes over `num_lines`
//! lines and a token count it confusingly spells `L` (not to be confused
//! with the *lines* parameter, which is `num_lines`); this file keeps `L`
//! for lines and `N` for tokens, which is what issue #187 and the rest of
//! this crate's docs call them. A chain here is [`NUM_PIPES`] [`Pipe`]s,
//! serial first and then alternating serial/parallel -- `S, P, S, P` --
//! rather than upstream's own default of eight pipes all serial
//! (`pipes = "ssssssss"`), so that both [`PipeType`]s are exercised in one
//! shape instead of swept separately the way upstream's `-p` option does.
//!
//! **No rayon baseline.** rayon has no pipeline primitive -- `par_iter`,
//! `join` and `scope` all express data or fork-join parallelism, neither of
//! which is a bounded chain of stages passing a stream of tokens through
//! itself. The baseline here is [`NUM_PIPES`] `std::thread`s chained by
//! [`std::sync::mpsc`] channels, each thread one stage, in the role TBB's
//! `tbb::parallel_pipeline` plays upstream's own `tbb.cpp` -- the same role
//! the module docs of the other benches in this directory give rayon, just
//! with a different crate standing in because there is no rayon pipeline to
//! reach for.
//!
//! # What the mpsc baseline does and does not share with the pipeline
//!
//! The comparison is honest about two things and not a third, and the table
//! in `BENCHMARKS.md` says so again next to the numbers:
//!
//! - **The same per-token work, [`NUM_PIPES`] times over.** Every stage of
//!   the mpsc chain and every [`Pipe`] of the [`Pipeline`] below calls
//!   [`dummy`] once per token -- the same 32-iteration floating-point body
//!   `embarrassing_parallelism.rs` and `graph_traversal.rs` use, rather than
//!   upstream's own `work()`, a 10 microsecond `sleep_for`. A sleep would
//!   make every size in [`SIZES`] take minutes once lines and workers
//!   multiply it, without measuring anything this crate's scheduler does;
//!   the fixed FP body costs the same amount of CPU and lets the chain
//!   actually contend for it.
//! - **The same number of hops.** A token crosses [`NUM_PIPES`] pipes in the
//!   [`Pipeline`] and [`NUM_PIPES`] channel sends in the mpsc chain, so
//!   neither side is given a shorter path to the answer.
//! - **Not the same concurrency per stage.** [`LINES`] is a knob the
//!   [`Pipeline`] has and the mpsc chain does not: a [`PipeType::Parallel`]
//!   pipe may run several lines' tokens through it at once, up to
//!   [`Pipeline::num_lines`], while an mpsc stage is always exactly one
//!   `std::thread`, whatever `PipeType` the pipe it stands in for would have
//!   been. So the mpsc baseline is measured once per size, not once per
//!   `(lines, workers)` row -- it has neither axis -- and the same number is
//!   read against every row of that size in the table. **`L = 1` is the
//!   row this matters most for**: with one line, the pipeline has no
//!   concurrency of its own at all (`src/pipeline/scheduler.rs`'s `run_line`
//!   advances one token through every pipe before the next one starts), so
//!   that row measures this crate's per-pipe dispatch cost against a
//!   baseline that keeps [`NUM_PIPES`] threads genuinely busy regardless of
//!   `L`. A large gap there is not a bug in the comparison; it is the
//!   degenerate case the module docs above promise to call out.
//!
//! Both sides' measured region includes its own setup, the same way every
//! other benchmark in this directory includes building the graph: a
//! `rustflow` iteration builds a fresh [`Pipeline`] and composes it into a
//! fresh [`Taskflow`] before running it, and an `mpsc` iteration spawns
//! [`NUM_PIPES`] fresh threads and [`NUM_PIPES`]` - 1` fresh channels before
//! feeding them. Neither side reuses a warm pool of its own stages.

use std::collections::VecDeque;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::thread::available_parallelism;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustflow::Executor;
use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
use rustflow::pipeline::scheduler::Pipeline;
use rustflow::prelude::*;

/// The number of pipes in the chain every variant measures: serial first,
/// then alternating serial/parallel, so a chain of four is `S, P, S, P`.
const NUM_PIPES: usize = 4;

/// Line counts the [`Pipeline`] variant sweeps: upstream's own `-l` option,
/// which defaults to `8`. `1` is the degenerate case the module docs above
/// single out -- no pipeline parallelism at all, so it is also the row that
/// says the most about what this crate's per-pipe dispatch costs on its own.
const LINES: [usize; 3] = [1, 4, 8];

/// Token counts, two points off upstream's own sweep (its confusingly named
/// `L`, `1 << i` for `i` in `1..=23`): one where the fixed cost of building
/// a [`Pipeline`] or spawning [`NUM_PIPES`] threads still shows, one where
/// the per-token cost dominates it.
const SIZES: [usize; 2] = [1024, 16384];

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// The worker counts every shape is measured at: one, and the machine's own.
///
/// Only the `rustflow` ids vary by this: the mpsc baseline's thread count is
/// [`NUM_PIPES`], fixed, which is the asymmetry the module docs above name.
fn worker_counts() -> Vec<usize> {
    let parallel = available_parallelism().map_or(1, NonZeroUsize::get);
    if parallel > 1 {
        vec![1, parallel]
    } else {
        vec![1]
    }
}

/// Upstream's `dummy`, reused unchanged from `embarrassing_parallelism.rs`:
/// a small, non-foldable amount of FP work per token, run once per pipe or
/// per mpsc stage so that both sides pay the same per-token cost.
fn dummy(i: u64) -> f64 {
    let mut x = (i.wrapping_mul(1_315_423_911) ^ (i >> 3)) as f64 + 1.0;
    let mut sink = 0.0_f64;

    for _ in 0..32 {
        x = x.sin() * 1.000_000_1 + x.cos();
        sink = black_box(sink + x);
    }

    sink
}

/// Builds the [`NUM_PIPES`]-pipe chain this benchmark measures and the
/// counter its last pipe advances: `S, P, S, P`, matching the mpsc chain's
/// own stage count and per-token cost.
fn build_pipeline(num_lines: usize, num_tokens: usize) -> (Pipeline, Arc<AtomicUsize>) {
    let processed = Arc::new(AtomicUsize::new(0));
    let mut pipes = Vec::with_capacity(NUM_PIPES);

    // The first pipe is upstream's `Input`: it both generates the token
    // stream and stops it, so it must be serial -- `Pipeline::new` panics
    // on any chain whose first pipe is not.
    pipes.push(Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
        if pf.token() == num_tokens {
            pf.stop();
            return;
        }
        let token = u64::try_from(pf.token()).expect("a token index fits in a u64");
        black_box(dummy(token));
    }));

    for stage in 1..NUM_PIPES {
        let kind = if stage % 2 == 1 {
            PipeType::Parallel
        } else {
            PipeType::Serial
        };
        let is_last = stage == NUM_PIPES - 1;
        let processed = Arc::clone(&processed);
        pipes.push(Pipe::new(kind, move |pf: &mut Pipeflow| {
            let token = u64::try_from(pf.token()).expect("a token index fits in a u64");
            black_box(dummy(token));
            if is_last {
                processed.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    (Pipeline::new(num_lines, pipes), processed)
}

/// Runs `num_tokens` through a freshly built [`Pipeline`] of `num_lines`
/// lines, composed into a freshly built [`Taskflow`]: both are part of the
/// measured region, the same "ours includes building the graph" convention
/// every other benchmark in this directory follows.
fn run_pipeline(num_lines: usize, num_tokens: usize, executor: &Executor) {
    let (pipeline, processed) = build_pipeline(num_lines, num_tokens);

    let mut taskflow = Taskflow::new();
    let module = taskflow.composed_of(&pipeline);
    taskflow.task(module).name("module_of_pipeline");

    executor.run(&taskflow).wait().expect("no task panicked");

    // The only thing standing between this benchmark and timing a pipeline
    // that stopped before its first pipe ever ran.
    assert_eq!(
        processed.load(Ordering::Relaxed),
        num_tokens,
        "every token reaches the last pipe exactly once"
    );
}

/// Chains [`NUM_PIPES`] freshly spawned threads with
/// [`NUM_PIPES`]` - 1` fresh [`std::sync::mpsc`] channels, the first thread
/// generating `num_tokens` tokens and the rest receiving, doing [`dummy`]'s
/// work and forwarding -- the baseline's whole shape, in the role TBB's
/// `parallel_pipeline` plays upstream. Returns each stage's own count of
/// tokens it handled, in stage order.
fn run_mpsc(num_tokens: usize) -> Vec<usize> {
    let channels: Vec<(mpsc::Sender<usize>, mpsc::Receiver<usize>)> =
        (0..NUM_PIPES - 1).map(|_| mpsc::channel()).collect();
    let (txs, rxs): (Vec<_>, Vec<_>) = channels.into_iter().unzip();
    let mut txs: VecDeque<_> = txs.into();
    let mut rxs: VecDeque<_> = rxs.into();

    let handles: Vec<thread::JoinHandle<usize>> = (0..NUM_PIPES)
        .map(|stage| {
            // Only the first stage has no upstream channel and only the
            // last has no downstream one; every stage in between both
            // receives and sends, which is the chain's one hop per pipe.
            let input = if stage == 0 { None } else { rxs.pop_front() };
            let output = if stage == NUM_PIPES - 1 {
                None
            } else {
                txs.pop_front()
            };

            thread::spawn(move || {
                let mut count: usize = 0;

                let mut handle = |token: usize| {
                    let as_u64 = u64::try_from(token).expect("a token index fits in a u64");
                    black_box(dummy(as_u64));
                    count += 1;
                    if let Some(tx) = &output {
                        tx.send(token).expect("the next stage is still receiving");
                    }
                };

                if let Some(rx) = input {
                    while let Ok(token) = rx.recv() {
                        handle(token);
                    }
                } else {
                    for token in 0..num_tokens {
                        handle(token);
                    }
                }

                count
            })
        })
        .collect();

    handles
        .into_iter()
        .map(|handle| {
            handle
                .join()
                .expect("a pipeline stage thread does not panic")
        })
        .collect()
}

/// Runs [`run_mpsc`] and checks every token reached the last stage, the
/// mpsc chain's equivalent of [`run_pipeline`]'s own assertion.
fn mpsc_chain(num_tokens: usize) {
    let counts = run_mpsc(num_tokens);
    assert_eq!(
        counts[NUM_PIPES - 1],
        num_tokens,
        "every token reaches the last stage exactly once"
    );
}

/// Registers the `mpsc` baseline once per size and the `rustflow` variant at
/// every line count, size and worker count.
fn bench_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    // The mpsc baseline has no `lines` or `workers` axis -- see the module
    // docs -- so it is registered once per size rather than once per row of
    // the table `BENCHMARKS.md` reads this against.
    for size in SIZES {
        group.bench_function(BenchmarkId::new("mpsc", size), |b| {
            b.iter(|| mpsc_chain(black_box(size)));
        });
    }

    for workers in worker_counts() {
        let executor = Executor::new(workers);

        for lines in LINES {
            for size in SIZES {
                group.bench_function(
                    BenchmarkId::new(format!("rustflow/{lines}l_{workers}w"), size),
                    |b| b.iter(|| run_pipeline(lines, black_box(size), &executor)),
                );
            }
        }
    }

    group.finish();
}

criterion_group!(benches, bench_pipeline);
criterion_main!(benches);

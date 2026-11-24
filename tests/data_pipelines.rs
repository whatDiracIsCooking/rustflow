//! Translated from `unittests/test_data_pipelines.cpp` (Taskflow v4.1.0).
//!
//! The same seven structural families `tests/pipelines.rs` ports from
//! `unittests/test_pipelines.cpp` -- `DataPipeline.1P(S)`, `2P(SS)`,
//! `2P(SP)`, `3P(SSS)`, `3P(SSP)`, `3P(SPS)` and `3P(SPP)` -- over lines
//! 1..=4 and workers 1..=4, 112 cases through [`worker_matrix!`]. Every case
//! sweeps upstream's own `N` from `0` to 100 tokens.
//!
//! The adaptations are the same three `tests/pipelines.rs` already states
//! and for the same reasons, plus one more specific to this file:
//!
//! - **The value each pipe checks is the chain's own typed value, not a
//!   manual buffer.** `tests/pipelines.rs` writes into and reads out of its
//!   own `Buffer` because a plain `Pipe` callable only ever sees a
//!   `Pipeflow`, with nowhere of its own to carry a value between pipes. A
//!   `DataPipe`'s callable is handed the value the pipe before it just
//!   returned, so every case here reads that value directly as its own
//!   typed argument instead of indexing a side buffer -- which is the
//!   difference `DataPipeline` exists to make, and the one this file's
//!   cases are checking.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::pipeline::data_pipeline::{
    DataPipeline, parallel_pipe, serial_pipe, serial_pipe_with_flow,
};
use rustflow::pipeline::pipe::PipeType;
use rustflow::prelude::*;

#[macro_use]
mod common;
#[path = "common/compose.rs"]
mod compose;
#[path = "common/data_pipeline.rs"]
mod data_pipeline;
#[path = "common/panics.rs"]
mod panics;
#[path = "common/serialized.rs"]
mod serialized;
#[path = "common/tokens.rs"]
mod tokens;

use compose::compose_module;
use data_pipeline::{collecting_pipe, counting_source, propagating_pipe};
use panics::assert_panicked_with;
use serialized::serialized;
use tokens::{MAX_N, assert_consecutive_from};

// ----------------------------------------------------------------------------
// DataPipeline.1P(S): one serial pipe, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `data_pipeline_1P(L, w, tf::PipeType::SERIAL)`: a single
/// `void -> void` serial pipe that stops after `N` tokens, checking each one
/// lands on the line its token number says it should.
fn data_pipeline_1p_s(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let j = Arc::new(AtomicUsize::new(0));

            let pipe0 = {
                let j = Arc::clone(&j);
                serial_pipe_with_flow::<(), ()>(move |(), pf| {
                    if j.load(Ordering::Relaxed) == n {
                        pf.stop();
                        return;
                    }
                    assert_eq!(pf.token() % num_lines, pf.line());
                    j.fetch_add(1, Ordering::Relaxed);
                })
            };

            let pipeline = DataPipeline::builder(num_lines).pipe(pipe0).build();

            compose_module(&mut taskflow, &pipeline, "module_of_data_pipeline");

            for attempt in 0..2 {
                executor.run(&taskflow).wait().expect("no task panicked");
                assert_eq!(pipeline.num_tokens(), n);

                if attempt == 0 {
                    j.store(0, Ordering::Relaxed);
                    pipeline.reset();
                }
            }
        }
    });
}

worker_matrix! {
    data_pipeline_1p_s {
        data_pipeline_1p_s_1l_1w(1, 1),
        data_pipeline_1p_s_1l_2w(1, 2),
        data_pipeline_1p_s_1l_3w(1, 3),
        data_pipeline_1p_s_1l_4w(1, 4),
        data_pipeline_1p_s_2l_1w(2, 1),
        data_pipeline_1p_s_2l_2w(2, 2),
        data_pipeline_1p_s_2l_3w(2, 3),
        data_pipeline_1p_s_2l_4w(2, 4),
        data_pipeline_1p_s_3l_1w(3, 1),
        data_pipeline_1p_s_3l_2w(3, 2),
        data_pipeline_1p_s_3l_3w(3, 3),
        data_pipeline_1p_s_3l_4w(3, 4),
        data_pipeline_1p_s_4l_1w(4, 1),
        data_pipeline_1p_s_4l_2w(4, 2),
        data_pipeline_1p_s_4l_3w(4, 3),
        data_pipeline_1p_s_4l_4w(4, 4),
    }
}

// ----------------------------------------------------------------------------
// DataPipeline.2P(SS): two serial pipes, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `data_pipeline_2P_SS(L, w)`: a `void -> i32` serial pipe that
/// stops after `N` tokens, and a second, `i32 -> void` serial pipe that
/// checks the value it is handed is exactly what the first just returned.
fn data_pipeline_2p_ss(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let j1 = Arc::new(AtomicUsize::new(0));
            let j2 = Arc::new(AtomicUsize::new(0));

            let pipe0 = counting_source(num_lines, move || n, &j1);

            let pipe1 = {
                let j2 = Arc::clone(&j2);
                serial_pipe_with_flow::<i32, ()>(move |value, pf| {
                    let current = j2.load(Ordering::Relaxed);
                    assert!(current < n);
                    assert_eq!(pf.token() % num_lines, pf.line());
                    let expected =
                        i32::try_from(current + 1).expect("a token count fits in an i32");
                    assert_eq!(expected, value);
                    j2.fetch_add(1, Ordering::Relaxed);
                })
            };

            let pipeline = DataPipeline::builder(num_lines)
                .pipe(pipe0)
                .pipe(pipe1)
                .build();

            compose_module(&mut taskflow, &pipeline, "module_of_data_pipeline");

            for attempt in 0..2 {
                executor.run(&taskflow).wait().expect("no task panicked");
                assert_eq!(pipeline.num_tokens(), n);

                if attempt == 0 {
                    j1.store(0, Ordering::Relaxed);
                    j2.store(0, Ordering::Relaxed);
                    pipeline.reset();
                }
            }
        }
    });
}

worker_matrix! {
    data_pipeline_2p_ss {
        data_pipeline_2p_ss_1l_1w(1, 1),
        data_pipeline_2p_ss_1l_2w(1, 2),
        data_pipeline_2p_ss_1l_3w(1, 3),
        data_pipeline_2p_ss_1l_4w(1, 4),
        data_pipeline_2p_ss_2l_1w(2, 1),
        data_pipeline_2p_ss_2l_2w(2, 2),
        data_pipeline_2p_ss_2l_3w(2, 3),
        data_pipeline_2p_ss_2l_4w(2, 4),
        data_pipeline_2p_ss_3l_1w(3, 1),
        data_pipeline_2p_ss_3l_2w(3, 2),
        data_pipeline_2p_ss_3l_3w(3, 3),
        data_pipeline_2p_ss_3l_4w(3, 4),
        data_pipeline_2p_ss_4l_1w(4, 1),
        data_pipeline_2p_ss_4l_2w(4, 2),
        data_pipeline_2p_ss_4l_3w(4, 3),
        data_pipeline_2p_ss_4l_4w(4, 4),
    }
}

// ----------------------------------------------------------------------------
// DataPipeline.2P(SP): one serial, one parallel pipe, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `data_pipeline_2P_SP(L, w)`: a serial pipe exactly as
/// [`data_pipeline_2p_ss`]'s first, and a parallel pipe that collects the
/// value it is handed rather than checking it against a line-local counter
/// -- a parallel pipe gives tokens no order among themselves, so the check
/// is that the right *set* of values came out, not the right sequence.
fn data_pipeline_2p_sp(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let j1 = Arc::new(AtomicUsize::new(0));
            let j2 = Arc::new(AtomicUsize::new(0));
            let collection: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

            let pipe0 = counting_source(num_lines, move || n, &j1);

            let pipe1 = collecting_pipe(num_lines, move || n, &j2, &collection);

            let pipeline = DataPipeline::builder(num_lines)
                .pipe(pipe0)
                .pipe(pipe1)
                .build();

            compose_module(&mut taskflow, &pipeline, "module_of_data_pipeline");

            for attempt in 0..2 {
                executor.run(&taskflow).wait().expect("no task panicked");
                assert_eq!(pipeline.num_tokens(), n);
                assert_consecutive_from(&collection, 1, n);

                if attempt == 0 {
                    j1.store(0, Ordering::Relaxed);
                    j2.store(0, Ordering::Relaxed);
                    collection.lock().expect("uncontended").clear();
                    pipeline.reset();
                }
            }
        }
    });
}

worker_matrix! {
    data_pipeline_2p_sp {
        data_pipeline_2p_sp_1l_1w(1, 1),
        data_pipeline_2p_sp_1l_2w(1, 2),
        data_pipeline_2p_sp_1l_3w(1, 3),
        data_pipeline_2p_sp_1l_4w(1, 4),
        data_pipeline_2p_sp_2l_1w(2, 1),
        data_pipeline_2p_sp_2l_2w(2, 2),
        data_pipeline_2p_sp_2l_3w(2, 3),
        data_pipeline_2p_sp_2l_4w(2, 4),
        data_pipeline_2p_sp_3l_1w(3, 1),
        data_pipeline_2p_sp_3l_2w(3, 2),
        data_pipeline_2p_sp_3l_3w(3, 3),
        data_pipeline_2p_sp_3l_4w(3, 4),
        data_pipeline_2p_sp_4l_1w(4, 1),
        data_pipeline_2p_sp_4l_2w(4, 2),
        data_pipeline_2p_sp_4l_3w(4, 3),
        data_pipeline_2p_sp_4l_4w(4, 4),
    }
}

// ----------------------------------------------------------------------------
// DataPipeline.3P(SSS): three serial pipes, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `data_pipeline_3P_SSS(L, w)`: a chain of three serial pipes,
/// each checking the value it is handed is exactly what the one before it
/// just returned, and passing it on unchanged.
fn data_pipeline_3p_sss(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let j1 = Arc::new(AtomicUsize::new(0));
            let j2 = Arc::new(AtomicUsize::new(0));
            let j3 = Arc::new(AtomicUsize::new(0));

            let pipe0 = counting_source(num_lines, move || n, &j1);

            let pipe1 = {
                let j2 = Arc::clone(&j2);
                serial_pipe_with_flow::<i32, i32>(move |value, pf| {
                    let current = j2.load(Ordering::Relaxed);
                    assert!(current < n);
                    let expected =
                        i32::try_from(current + 1).expect("a token count fits in an i32");
                    assert_eq!(expected, value);
                    assert_eq!(pf.token() % num_lines, pf.line());
                    j2.fetch_add(1, Ordering::Relaxed);
                    value
                })
            };

            let pipe2 = {
                let j3 = Arc::clone(&j3);
                serial_pipe_with_flow::<i32, ()>(move |value, pf| {
                    let current = j3.load(Ordering::Relaxed);
                    assert!(current < n);
                    let expected =
                        i32::try_from(current + 1).expect("a token count fits in an i32");
                    assert_eq!(expected, value);
                    assert_eq!(pf.token() % num_lines, pf.line());
                    j3.fetch_add(1, Ordering::Relaxed);
                })
            };

            let pipeline = DataPipeline::builder(num_lines)
                .pipe(pipe0)
                .pipe(pipe1)
                .pipe(pipe2)
                .build();

            compose_module(&mut taskflow, &pipeline, "module_of_data_pipeline");

            for attempt in 0..2 {
                executor.run(&taskflow).wait().expect("no task panicked");
                assert_eq!(pipeline.num_tokens(), n);

                if attempt == 0 {
                    j1.store(0, Ordering::Relaxed);
                    j2.store(0, Ordering::Relaxed);
                    j3.store(0, Ordering::Relaxed);
                    pipeline.reset();
                }
            }
        }
    });
}

worker_matrix! {
    data_pipeline_3p_sss {
        data_pipeline_3p_sss_1l_1w(1, 1),
        data_pipeline_3p_sss_1l_2w(1, 2),
        data_pipeline_3p_sss_1l_3w(1, 3),
        data_pipeline_3p_sss_1l_4w(1, 4),
        data_pipeline_3p_sss_2l_1w(2, 1),
        data_pipeline_3p_sss_2l_2w(2, 2),
        data_pipeline_3p_sss_2l_3w(2, 3),
        data_pipeline_3p_sss_2l_4w(2, 4),
        data_pipeline_3p_sss_3l_1w(3, 1),
        data_pipeline_3p_sss_3l_2w(3, 2),
        data_pipeline_3p_sss_3l_3w(3, 3),
        data_pipeline_3p_sss_3l_4w(3, 4),
        data_pipeline_3p_sss_4l_1w(4, 1),
        data_pipeline_3p_sss_4l_2w(4, 2),
        data_pipeline_3p_sss_4l_3w(4, 3),
        data_pipeline_3p_sss_4l_4w(4, 4),
    }
}

// ----------------------------------------------------------------------------
// DataPipeline.3P(SSP): two serial pipes then a parallel one, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `data_pipeline_3P_SSP(L, w)`: two serial pipes exactly as
/// [`data_pipeline_3p_sss`]'s first two, then a parallel pipe that collects
/// the value it is handed rather than checking it against a line-local
/// counter.
fn data_pipeline_3p_ssp(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let j1 = Arc::new(AtomicUsize::new(0));
            let j2 = Arc::new(AtomicUsize::new(0));
            let j3 = Arc::new(AtomicUsize::new(0));
            let collection: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

            let pipe0 = counting_source(num_lines, move || n, &j1);

            let pipe1 = {
                let j2 = Arc::clone(&j2);
                serial_pipe_with_flow::<i32, i32>(move |value, pf| {
                    let current = j2.load(Ordering::Relaxed);
                    assert!(current < n);
                    let expected =
                        i32::try_from(current + 1).expect("a token count fits in an i32");
                    assert_eq!(expected, value);
                    assert_eq!(pf.token() % num_lines, pf.line());
                    j2.fetch_add(1, Ordering::Relaxed);
                    value
                })
            };

            let pipe2 = collecting_pipe(num_lines, move || n, &j3, &collection);

            let pipeline = DataPipeline::builder(num_lines)
                .pipe(pipe0)
                .pipe(pipe1)
                .pipe(pipe2)
                .build();

            compose_module(&mut taskflow, &pipeline, "module_of_data_pipeline");

            for attempt in 0..2 {
                executor.run(&taskflow).wait().expect("no task panicked");
                assert_eq!(pipeline.num_tokens(), n);
                assert_consecutive_from(&collection, 1, n);

                if attempt == 0 {
                    j1.store(0, Ordering::Relaxed);
                    j2.store(0, Ordering::Relaxed);
                    j3.store(0, Ordering::Relaxed);
                    collection.lock().expect("uncontended").clear();
                    pipeline.reset();
                }
            }
        }
    });
}

worker_matrix! {
    data_pipeline_3p_ssp {
        data_pipeline_3p_ssp_1l_1w(1, 1),
        data_pipeline_3p_ssp_1l_2w(1, 2),
        data_pipeline_3p_ssp_1l_3w(1, 3),
        data_pipeline_3p_ssp_1l_4w(1, 4),
        data_pipeline_3p_ssp_2l_1w(2, 1),
        data_pipeline_3p_ssp_2l_2w(2, 2),
        data_pipeline_3p_ssp_2l_3w(2, 3),
        data_pipeline_3p_ssp_2l_4w(2, 4),
        data_pipeline_3p_ssp_3l_1w(3, 1),
        data_pipeline_3p_ssp_3l_2w(3, 2),
        data_pipeline_3p_ssp_3l_3w(3, 3),
        data_pipeline_3p_ssp_3l_4w(3, 4),
        data_pipeline_3p_ssp_4l_1w(4, 1),
        data_pipeline_3p_ssp_4l_2w(4, 2),
        data_pipeline_3p_ssp_4l_3w(4, 3),
        data_pipeline_3p_ssp_4l_4w(4, 4),
    }
}

// ----------------------------------------------------------------------------
// DataPipeline.3P(SPS): serial, parallel, serial, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `data_pipeline_3P_SPS(L, w)`: a serial pipe, a parallel pipe
/// that both collects and advances the value it is handed, and a serial pipe
/// that checks the advanced value -- the middle pipe being parallel does not
/// relax the serial pipes around it, which this case's last-pipe check
/// pins.
fn data_pipeline_3p_sps(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let j1 = Arc::new(AtomicUsize::new(0));
            let j2 = Arc::new(AtomicUsize::new(0));
            let j3 = Arc::new(AtomicUsize::new(0));
            let collection: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

            let pipe0 = counting_source(num_lines, move || n, &j1);

            let pipe1 = propagating_pipe(num_lines, move || n, &j2, &collection);

            let pipe2 = {
                let j3 = Arc::clone(&j3);
                serial_pipe_with_flow::<i32, ()>(move |value, pf| {
                    let current = j3.load(Ordering::Relaxed);
                    assert!(current < n);
                    assert_eq!(pf.token() % num_lines, pf.line());
                    let expected =
                        i32::try_from(current + 2).expect("a token count fits in an i32");
                    assert_eq!(expected, value);
                    j3.fetch_add(1, Ordering::Relaxed);
                })
            };

            let pipeline = DataPipeline::builder(num_lines)
                .pipe(pipe0)
                .pipe(pipe1)
                .pipe(pipe2)
                .build();

            compose_module(&mut taskflow, &pipeline, "module_of_data_pipeline");

            for attempt in 0..2 {
                executor.run(&taskflow).wait().expect("no task panicked");
                assert_eq!(pipeline.num_tokens(), n);
                assert_consecutive_from(&collection, 1, n);

                if attempt == 0 {
                    j1.store(0, Ordering::Relaxed);
                    j2.store(0, Ordering::Relaxed);
                    j3.store(0, Ordering::Relaxed);
                    collection.lock().expect("uncontended").clear();
                    pipeline.reset();
                }
            }
        }
    });
}

worker_matrix! {
    data_pipeline_3p_sps {
        data_pipeline_3p_sps_1l_1w(1, 1),
        data_pipeline_3p_sps_1l_2w(1, 2),
        data_pipeline_3p_sps_1l_3w(1, 3),
        data_pipeline_3p_sps_1l_4w(1, 4),
        data_pipeline_3p_sps_2l_1w(2, 1),
        data_pipeline_3p_sps_2l_2w(2, 2),
        data_pipeline_3p_sps_2l_3w(2, 3),
        data_pipeline_3p_sps_2l_4w(2, 4),
        data_pipeline_3p_sps_3l_1w(3, 1),
        data_pipeline_3p_sps_3l_2w(3, 2),
        data_pipeline_3p_sps_3l_3w(3, 3),
        data_pipeline_3p_sps_3l_4w(3, 4),
        data_pipeline_3p_sps_4l_1w(4, 1),
        data_pipeline_3p_sps_4l_2w(4, 2),
        data_pipeline_3p_sps_4l_3w(4, 3),
        data_pipeline_3p_sps_4l_4w(4, 4),
    }
}

// ----------------------------------------------------------------------------
// DataPipeline.3P(SPP): serial, parallel, parallel, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `data_pipeline_3P_SPP(L, w)`: a serial pipe followed by two
/// parallel pipes in a row, each collecting into its own set -- the only
/// family where two pipes with no ordering between their own tokens also
/// have none between each other's, which is what the two independent
/// collections check.
fn data_pipeline_3p_spp(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let j1 = Arc::new(AtomicUsize::new(0));
            let j2 = Arc::new(AtomicUsize::new(0));
            let j3 = Arc::new(AtomicUsize::new(0));
            let collection2: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
            let collection3: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

            let pipe0 = counting_source(num_lines, move || n, &j1);

            let pipe1 = propagating_pipe(num_lines, move || n, &j2, &collection2);

            let pipe2 = collecting_pipe(num_lines, move || n, &j3, &collection3);

            let pipeline = DataPipeline::builder(num_lines)
                .pipe(pipe0)
                .pipe(pipe1)
                .pipe(pipe2)
                .build();

            compose_module(&mut taskflow, &pipeline, "module_of_data_pipeline");

            for attempt in 0..2 {
                executor.run(&taskflow).wait().expect("no task panicked");
                assert_eq!(pipeline.num_tokens(), n);
                assert_consecutive_from(&collection2, 1, n);
                assert_consecutive_from(&collection3, 2, n);

                if attempt == 0 {
                    j1.store(0, Ordering::Relaxed);
                    j2.store(0, Ordering::Relaxed);
                    j3.store(0, Ordering::Relaxed);
                    collection2.lock().expect("uncontended").clear();
                    collection3.lock().expect("uncontended").clear();
                    pipeline.reset();
                }
            }
        }
    });
}

worker_matrix! {
    data_pipeline_3p_spp {
        data_pipeline_3p_spp_1l_1w(1, 1),
        data_pipeline_3p_spp_1l_2w(1, 2),
        data_pipeline_3p_spp_1l_3w(1, 3),
        data_pipeline_3p_spp_1l_4w(1, 4),
        data_pipeline_3p_spp_2l_1w(2, 1),
        data_pipeline_3p_spp_2l_2w(2, 2),
        data_pipeline_3p_spp_2l_3w(2, 3),
        data_pipeline_3p_spp_2l_4w(2, 4),
        data_pipeline_3p_spp_3l_1w(3, 1),
        data_pipeline_3p_spp_3l_2w(3, 2),
        data_pipeline_3p_spp_3l_3w(3, 3),
        data_pipeline_3p_spp_3l_4w(3, 4),
        data_pipeline_3p_spp_4l_1w(4, 1),
        data_pipeline_3p_spp_4l_2w(4, 2),
        data_pipeline_3p_spp_4l_3w(4, 3),
        data_pipeline_3p_spp_4l_4w(4, 4),
    }
}

// ----------------------------------------------------------------------------
// The panic-path cell: a typed pipe callable that fails
// ----------------------------------------------------------------------------
//
// Not upstream's: `test_data_pipelines.cpp` has no failing pipe anywhere.
// One chain rather than `tests/pipelines.rs`'s three, because the three
// places a line task can be when a pipe panics are the scheduler's and are
// pinned there; what is this file's own is the typed slot each pipe hands
// the next, which a failed run leaves empty for the line it failed on. Two
// cases over that one chain, the second of them `#[ignore]`d against the
// defect it found until that defect was fixed -- its own doc comment says
// which.

/// How many tokens the chain below lets through when nothing panics.
const TOKENS: usize = 5;

/// The token the middle pipe panics on.
const FAILS_AT: usize = 2;

/// How many lines the chain runs over.
const LINES: usize = 4;

/// What a case needs of the failing chain: the pipeline, the arming switch
/// its middle pipe reads, and the counters its three pipes write.
///
/// A struct because the two cases below build the same chain and a
/// `DataPipe` is consumed by [`DataPipelineBuilder::pipe`], so there is no
/// chain value to hand to a second case -- each builds its own, and this is
/// what "its own" is.
struct FailingChain {
    pipeline: DataPipeline,
    /// `1` while the middle pipe is to panic on token [`FAILS_AT`].
    armed: Arc<AtomicUsize>,
    /// How many tokens the source let through.
    generated: Arc<AtomicUsize>,
    /// How many the middle pipe was handed.
    middle_seen: Arc<AtomicUsize>,
    /// How many the last pipe was handed.
    tail_seen: Arc<AtomicUsize>,
    /// The highest token the last pipe was handed, plus one, so that zero
    /// means "none at all".
    tail_reach: Arc<AtomicUsize>,
    /// How many times the last pipe was handed a value the two pipes before
    /// it could not have produced. Recorded rather than asserted in the
    /// body, since a panic in a pipe callable is a failed run and would be
    /// read as the failure these cases are about.
    mismatches: Arc<AtomicUsize>,
}

impl FailingChain {
    /// Builds the chain, armed: a `() -> i32` source that stops after
    /// [`TOKENS`] tokens, a `i32 -> i32` middle pipe that panics with `"x"`
    /// on token [`FAILS_AT`], and a `i32 -> ()` pipe that checks the value
    /// the two before it produced.
    fn new() -> Self {
        let armed = Arc::new(AtomicUsize::new(1));
        let generated = Arc::new(AtomicUsize::new(0));
        let middle_seen = Arc::new(AtomicUsize::new(0));
        let tail_seen = Arc::new(AtomicUsize::new(0));
        let tail_reach = Arc::new(AtomicUsize::new(0));
        let mismatches = Arc::new(AtomicUsize::new(0));

        let source = {
            let generated = Arc::clone(&generated);
            serial_pipe_with_flow::<(), i32>(move |(), pf| {
                let token = pf.token();
                if token == TOKENS {
                    pf.stop();
                    return 0;
                }
                generated.fetch_add(1, Ordering::Relaxed);
                i32::try_from(token + 1).expect("a token count fits in an i32")
            })
        };

        let middle = {
            let armed = Arc::clone(&armed);
            let middle_seen = Arc::clone(&middle_seen);
            serial_pipe_with_flow::<i32, i32>(move |value, pf| {
                if pf.token() == FAILS_AT && armed.load(Ordering::SeqCst) == 1 {
                    panic!("x");
                }
                middle_seen.fetch_add(1, Ordering::Relaxed);
                value + 1
            })
        };

        let tail = {
            let tail_seen = Arc::clone(&tail_seen);
            let tail_reach = Arc::clone(&tail_reach);
            let mismatches = Arc::clone(&mismatches);
            serial_pipe_with_flow::<i32, ()>(move |value, pf| {
                tail_seen.fetch_add(1, Ordering::Relaxed);
                tail_reach.fetch_max(pf.token() + 1, Ordering::Relaxed);
                let expected = i32::try_from(pf.token() + 2).expect("a token count fits in an i32");
                if value != expected {
                    mismatches.fetch_add(1, Ordering::Relaxed);
                }
            })
        };

        Self {
            pipeline: DataPipeline::builder(LINES)
                .pipe(source)
                .pipe(middle)
                .pipe(tail)
                .build(),
            armed,
            generated,
            middle_seen,
            tail_seen,
            tail_reach,
            mismatches,
        }
    }

    /// Composes this chain into a fresh taskflow, runs it, and asserts the
    /// middle pipe's panic is what the run reports.
    fn fails_the_composing_run(&self, executor: &Executor) -> Taskflow {
        let mut taskflow = Taskflow::new();
        compose_module(&mut taskflow, &self.pipeline, "module_of_data_pipeline");

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("the middle pipe panicked, so the run failed");
        assert_panicked_with(error, "x");
        taskflow
    }

    /// Disarms the middle pipe and zeroes the per-run counters, leaving the
    /// mismatch count alone: a mismatch from either run is a mismatch.
    fn disarm(&self) {
        self.armed.store(0, Ordering::SeqCst);
        for counter in [&self.generated, &self.middle_seen, &self.tail_seen] {
            counter.store(0, Ordering::Relaxed);
        }
    }
}

/// Runs a one-task taskflow on `executor` and asserts it ran: the third
/// claim of each case here, that a failed run left the executor usable.
fn assert_executor_is_reusable(executor: &Executor) {
    let ran = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&ran);
    let mut taskflow = Taskflow::new();
    taskflow.emplace(move || {
        counted.fetch_add(1, Ordering::Relaxed);
    });
    executor
        .run(&taskflow)
        .wait()
        .expect("the executor is reusable after a failed run");
    assert_eq!(ran.load(Ordering::Relaxed), 1);
}

/// A panic in the middle pipe of a three-pipe typed chain fails the
/// composing run, and the pipe after it is handed nothing for the failing
/// token: the unwind skips the dependency the line would have released, so
/// no line ever carries that token on.
#[test]
fn a_panicking_data_pipe_fails_the_composing_run() {
    serialized(|| {
        let executor = Executor::new(LINES);
        let chain = FailingChain::new();
        let _ = chain.fails_the_composing_run(&executor);

        assert_eq!(
            chain.middle_seen.load(Ordering::Relaxed),
            FAILS_AT,
            "a serial pipe had the tokens before the failing one and no more"
        );
        assert!(
            chain.tail_reach.load(Ordering::Relaxed) <= FAILS_AT,
            "the pipe after the failing one was never handed the failing token"
        );
        assert_eq!(chain.mismatches.load(Ordering::Relaxed), 0);

        chain.pipeline.reset();
        assert_eq!(
            chain.pipeline.num_tokens(),
            0,
            "the reset gave the counter back"
        );
        assert_executor_is_reusable(&executor);
    });
}

/// And the chain `reset` and run again takes the whole token stream
/// through, every slot carrying the value the pipe before it produced --
/// which is what makes the failing pipe's own empty slot a state `reset`
/// clears rather than one the next run trips over.
///
/// `#[ignore]`d for #320 until it was fixed, which
/// `tests/pipelines.rs`'s own cell found and states in full: a line task is
/// bypass-scheduled (`Runtime::schedule`), and a panic that left another
/// line queued and then skipped by the cancellation left that line's node
/// claimed for ever, so the next run's bypass of it queued nothing and the
/// run never finished. Nothing about the typed chain was particular to that
/// -- this cell hung on the same window, 7 runs of 40 pinned to two cores --
/// so the one fix retired both.
#[test]
fn a_reset_data_pipeline_runs_again_after_a_panicking_pipe() {
    serialized(|| {
        let executor = Executor::new(LINES);
        let chain = FailingChain::new();
        let taskflow = chain.fails_the_composing_run(&executor);

        chain.disarm();
        chain.pipeline.reset();

        executor
            .run(&taskflow)
            .wait()
            .expect("nothing panics once the middle pipe is disarmed");

        assert_eq!(chain.pipeline.num_tokens(), TOKENS);
        assert_eq!(chain.generated.load(Ordering::Relaxed), TOKENS);
        assert_eq!(chain.middle_seen.load(Ordering::Relaxed), TOKENS);
        assert_eq!(
            chain.tail_seen.load(Ordering::Relaxed),
            TOKENS,
            "every token reached the end of the chain the second time"
        );
        assert_eq!(
            chain.mismatches.load(Ordering::Relaxed),
            0,
            "every slot carried the value the pipe before it produced"
        );
        assert_executor_is_reusable(&executor);
    });
}

/// `DataPipeline::taskflow` hands back the share its inner [`Pipeline`]
/// does: upstream's `graph()`, widened to the [`Taskflow`] that owns it.
///
/// `the_taskflow_a_pipeline_hands_back_is_its_own_live_graph` in
/// `tests/pipelines.rs` is the same three claims for the untyped pipeline;
/// what this adds is the one that is `DataPipeline`'s own -- the graph it
/// hands back carries the typed chain's slots too, so a run of it moves the
/// values between the pipes and not only the token count.
#[test]
fn the_taskflow_a_data_pipeline_hands_back_runs_the_typed_chain() {
    /// Lines, so that `cond` has a choice of line task to make.
    const LINES: usize = 2;
    /// Tokens the source lets through before it stops.
    const TOKENS: usize = 5;

    serialized(|| {
        let produced = Arc::new(AtomicUsize::new(0));
        let source = {
            let produced = Arc::clone(&produced);
            serial_pipe_with_flow::<(), usize>(move |(), pf| {
                let token = produced.load(Ordering::Relaxed);
                if token == TOKENS {
                    pf.stop();
                    return 0;
                }
                produced.fetch_add(1, Ordering::Relaxed);
                token * 10
            })
        };
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = {
            let seen = Arc::clone(&seen);
            serial_pipe_with_flow::<usize, ()>(move |value, _pf| {
                seen.lock().expect("no pipe panicked").push(value);
            })
        };
        let pipeline = DataPipeline::builder(LINES).pipe(source).pipe(sink).build();

        let handed = pipeline.taskflow();
        assert!(
            Arc::ptr_eq(&handed, &pipeline.taskflow()),
            "two calls name one taskflow"
        );
        assert_eq!(
            handed.graph().len(),
            LINES + 1,
            "one conditioner over one line task per line"
        );

        Executor::new(2)
            .run(&handed)
            .wait()
            .expect("no task panicked");

        assert_eq!(pipeline.num_tokens(), TOKENS);
        assert_eq!(
            *seen.lock().expect("the run is over"),
            vec![0, 10, 20, 30, 40],
            "every token's value reached the second pipe through the slots"
        );
    });
}

// ----------------------------------------------------------------------------
// The typed chain's own renderings (not upstream's)
// ----------------------------------------------------------------------------

/// `DataPipe`'s and [`DataPipeline`]'s `Debug` name the chain's shape and
/// nothing of the data moving through it.
///
/// Both impls are hand-written rather than derived because neither a user
/// callable nor a `Box<dyn Any>` has a `Debug` of its own, and what that
/// leaves is a choice. A `DataPipe` prints its direction and not what its
/// callable closed over; a `DataPipeline` prints the `Pipeline` it wraps
/// and not the per-line slots, which is where every value in flight between
/// two pipes lives -- the strongest case in the crate for a `Debug` that
/// keeps a caller's own data out of a log.
///
/// The absences are made real rather than vacuous: the marker is read inside
/// the source's body, and the second rendering is taken after a run, when
/// the last pipe's own tagged output is what each line's slot is left
/// holding. The `num_tokens` readings either side of the run are the other
/// half of that -- what a `DataPipeline` prints is live state, not a
/// snapshot taken at `build`.
#[test]
fn the_debug_output_of_a_typed_chain_names_its_shape_and_never_the_values_it_carries() {
    /// Lines, so that there is more than one slot to leave a value in.
    const LINES: usize = 2;
    /// Tokens the source lets through before it stops.
    const TOKENS: usize = 4;
    /// A value the source's body closes over, distinctive enough that its
    /// appearance in a rendering could not be a coincidence.
    const MARKER: &str = "a-data-pipes-own-captured-marker";
    /// The same for a value the chain carries between its two pipes.
    const VALUE_TAG: &str = "a-value-in-flight-between-two-pipes";

    serialized(|| {
        let produced = Arc::new(AtomicUsize::new(0));
        let source = {
            let captured = MARKER.to_string();
            let produced = Arc::clone(&produced);
            serial_pipe_with_flow::<(), usize>(move |(), pf| {
                // Read, so that the body really does hold the marker the
                // renderings below are asserted not to carry.
                assert_eq!(captured, MARKER);
                let token = produced.load(Ordering::Relaxed);
                if token == TOKENS {
                    pf.stop();
                    return 0;
                }
                produced.fetch_add(1, Ordering::Relaxed);
                token
            })
        };
        assert_eq!(format!("{source:?}"), "DataPipe { type: Serial }");

        let seen = Arc::new(Mutex::new(Vec::new()));
        let tail = {
            let seen = Arc::clone(&seen);
            parallel_pipe::<usize, String>(move |value| {
                let tagged = format!("{VALUE_TAG}-{value}");
                seen.lock().expect("uncontended").push(tagged.clone());
                tagged
            })
        };
        assert_eq!(format!("{tail:?}"), "DataPipe { type: Parallel }");
        assert!(
            !format!("{tail:#?}").contains(VALUE_TAG),
            "the alternate form prints no more of a pipe than the plain one"
        );

        let pipeline = DataPipeline::builder(LINES).pipe(source).pipe(tail).build();

        let built = format!("{pipeline:?}");
        assert!(
            built.starts_with("DataPipeline { pipeline: Pipeline {"),
            "the rendering is the wrapped scheduler's own: {built}"
        );
        assert!(
            built.contains(&format!("num_lines: {LINES}")),
            "with the shape it was built for: {built}"
        );
        assert!(
            built.contains("Pipe { type: Serial }") && built.contains("Pipe { type: Parallel }"),
            "and each lowered pipe's direction: {built}"
        );
        assert!(
            built.contains("num_tokens: 0"),
            "nothing has run yet: {built}"
        );
        assert!(
            !built.contains("slots"),
            "the per-line slots are no part of the rendering: {built}"
        );
        assert!(
            !built.contains(MARKER),
            "nor is what the source's body closed over: {built}"
        );

        let mut taskflow = Taskflow::new();
        compose_module(&mut taskflow, &pipeline, "module_of_pipeline");
        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(pipeline.num_tokens(), TOKENS);
        let mut carried = seen.lock().expect("the run is over").clone();
        carried.sort();
        assert_eq!(
            carried,
            (0..TOKENS)
                .map(|token| format!("{VALUE_TAG}-{token}"))
                .collect::<Vec<String>>(),
            "every token's value reached the tail pipe, which left it in its line's slot"
        );

        let run = format!("{pipeline:?}");
        assert!(
            run.contains(&format!("num_tokens: {TOKENS}")),
            "the rendering reports live state: {run}"
        );
        assert!(
            !run.contains(VALUE_TAG),
            "and a populated slot is still printed nowhere: {run}"
        );
    });
}

// ----------------------------------------------------------------------------
// A `DataPipe`'s own setters (upstream's own tests call neither)
// ----------------------------------------------------------------------------

/// `DataPipe::set_callable` is what the next run of the chain the pipe was
/// added to carries out: the first run's values come from the callable the
/// middle pipe was built with, the second run's from the one that replaced
/// it.
///
/// The swap happens on a pipe value before it reaches a builder, because
/// `DataPipelineBuilder::pipe` consumes each `DataPipe` it is handed -- so
/// the second run is over the chain `DataPipeline::reset_pipes` builds from
/// the swapped pipe, composed again for the reason that method's own docs
/// give.
#[test]
fn a_replaced_data_pipe_callable_is_what_the_next_run_carries_out() {
    /// Lines the chain runs over, so that more than one slot carries a
    /// value through the swapped pipe.
    const LINES: usize = 2;
    /// Tokens the source lets through before it stops, both runs.
    const TOKENS: usize = 6;
    /// What the callable the middle pipe is built with multiplies by.
    const SCALE: i32 = 10;
    /// What the callable that replaces it adds instead, far enough from
    /// `SCALE`'s own outputs that no value is in both runs' answers.
    const OFFSET: i32 = 1000;

    serialized(|| {
        let seen: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
        let source = || {
            serial_pipe_with_flow::<(), i32>(|(), pf| {
                if pf.token() == TOKENS {
                    pf.stop();
                    return 0;
                }
                i32::try_from(pf.token()).expect("a token count fits in an i32")
            })
        };
        let sink = || {
            let seen = Arc::clone(&seen);
            serial_pipe_with_flow::<i32, ()>(move |value, _pf| {
                seen.lock().expect("no pipe panicked").push(value);
            })
        };
        let token_values: Vec<i32> = (0..TOKENS)
            .map(|token| i32::try_from(token).expect("a token count fits in an i32"))
            .collect();

        let mut pipeline = DataPipeline::builder(LINES)
            .pipe(source())
            .pipe(serial_pipe::<i32, i32>(|value| value * SCALE))
            .pipe(sink())
            .build();

        let executor = Executor::new(2);
        let mut taskflow = Taskflow::new();
        compose_module(&mut taskflow, &pipeline, "module_of_data_pipeline");
        executor.run(&taskflow).wait().expect("no task panicked");

        assert_eq!(pipeline.num_tokens(), TOKENS);
        {
            let mut carried = seen.lock().expect("the run is over");
            assert_eq!(
                *carried,
                token_values
                    .iter()
                    .map(|value| value * SCALE)
                    .collect::<Vec<i32>>(),
                "the first run's values came from the callable the middle pipe was built with"
            );
            carried.clear();
        }

        let replaced_sink = sink();
        pipeline.reset_pipes(|builder| {
            let mut middle = serial_pipe::<i32, i32>(|value| value * SCALE);
            middle.set_callable(|value| value + OFFSET);
            assert_eq!(
                middle.pipe_type(),
                PipeType::Serial,
                "a callable swap is no part of the direction"
            );
            builder.pipe(source()).pipe(middle).pipe(replaced_sink)
        });

        let mut rebuilt = Taskflow::new();
        compose_module(&mut rebuilt, &pipeline, "module_of_the_swapped_chain");
        executor.run(&rebuilt).wait().expect("no task panicked");

        assert_eq!(pipeline.num_tokens(), TOKENS);
        assert_eq!(
            *seen.lock().expect("the run is over"),
            token_values
                .iter()
                .map(|value| value + OFFSET)
                .collect::<Vec<i32>>(),
            "the second run's values came from the callable that replaced it"
        );
    });
}

/// `DataPipe::set_type` flips a pipe `serial_pipe` built over to
/// `PipeType::Parallel`, and the chain it sits in still carries every token's
/// value through it: what the flip changes is the order that pipe's own
/// tokens may run in, not which values reach it.
///
/// The flip is read back three ways -- off the pipe, off the built
/// pipeline's rendering of the `Pipe` it was lowered into, and off the
/// values the run carried -- because the first two alone would not say the
/// scheduler ran the chain under the new direction. The chain's other two
/// pipes stay serial so that one `Parallel` in that rendering can only be
/// this pipe, and so that the sink collects in token order.
#[test]
fn a_data_pipe_flipped_to_parallel_still_carries_every_token() {
    /// Lines the chain runs over, so that the flipped pipe has more than one
    /// line's token to run at once.
    const LINES: usize = 4;
    /// Tokens the source lets through before it stops.
    const TOKENS: usize = 8;

    serialized(|| {
        let mut middle = serial_pipe::<i32, i32>(|value| value + 1);
        middle.set_type(PipeType::Parallel);
        assert_eq!(middle.pipe_type(), PipeType::Parallel);

        let seen: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
        let collected = Arc::clone(&seen);
        let pipeline = DataPipeline::builder(LINES)
            .pipe(serial_pipe_with_flow::<(), i32>(|(), pf| {
                if pf.token() == TOKENS {
                    pf.stop();
                    return 0;
                }
                i32::try_from(pf.token()).expect("a token count fits in an i32")
            }))
            .pipe(middle)
            .pipe(serial_pipe::<i32, ()>(move |value| {
                collected.lock().expect("no pipe panicked").push(value);
            }))
            .build();

        let built = format!("{pipeline:?}");
        assert_eq!(
            built.matches("type: Parallel").count(),
            1,
            "the flipped pipe is the one parallel pipe the scheduler was given: {built}"
        );

        let mut taskflow = Taskflow::new();
        compose_module(&mut taskflow, &pipeline, "module_of_data_pipeline");
        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(pipeline.num_tokens(), TOKENS);
        assert_eq!(
            *seen.lock().expect("the run is over"),
            (1..=TOKENS)
                .map(|value| i32::try_from(value).expect("a token count fits in an i32"))
                .collect::<Vec<i32>>(),
            "every token's value reached the serial sink through the flipped pipe"
        );
    });
}

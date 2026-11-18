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
use std::sync::{Arc, Mutex, PoisonError};

use rustflow::Executor;
use rustflow::pipeline::data_pipeline::{
    DataPipeline, parallel_pipe_with_flow, serial_pipe_with_flow,
};
use rustflow::prelude::*;

#[macro_use]
mod common;

/// Upstream's `maxN`: the largest token count any case sweeps up to.
const MAX_N: usize = 100;

/// Runs `body` with every other case in this suite held back:
/// `tests/pipelines.rs`'s own `serialized` and its doc comment's reasoning,
/// unchanged here.
fn serialized<R>(body: impl FnOnce() -> R) -> R {
    static GATE: Mutex<()> = Mutex::new(());
    let _guard = GATE.lock().unwrap_or_else(PoisonError::into_inner);
    body()
}

/// Asserts that `collected` holds exactly `n` values and, once sorted, is
/// `start, start + 1, ..., start + n - 1`: `tests/pipelines.rs`'s own helper
/// of the same name, unchanged here.
fn assert_consecutive_from(collected: &Mutex<Vec<i32>>, start: i32, n: usize) {
    let mut values = collected.lock().expect("uncontended");
    assert_eq!(values.len(), n);
    values.sort_unstable();
    for (index, &value) in values.iter().enumerate() {
        let offset = i32::try_from(index).expect("a case's token count fits in an i32");
        assert_eq!(value, start + offset);
    }
}

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

            let module = taskflow.composed_of(&pipeline);
            taskflow.task(module).name("module_of_data_pipeline");

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

            let pipe0 = {
                let j1 = Arc::clone(&j1);
                serial_pipe_with_flow::<(), i32>(move |(), pf| {
                    let current = j1.load(Ordering::Relaxed);
                    if current == n {
                        pf.stop();
                        return 0;
                    }
                    assert_eq!(pf.token() % num_lines, pf.line());
                    j1.fetch_add(1, Ordering::Relaxed);
                    i32::try_from(current + 1).expect("a token count fits in an i32")
                })
            };

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

            let module = taskflow.composed_of(&pipeline);
            taskflow.task(module).name("module_of_data_pipeline");

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

            let pipe0 = {
                let j1 = Arc::clone(&j1);
                serial_pipe_with_flow::<(), i32>(move |(), pf| {
                    let current = j1.load(Ordering::Relaxed);
                    if current == n {
                        pf.stop();
                        return 0;
                    }
                    assert_eq!(pf.token() % num_lines, pf.line());
                    j1.fetch_add(1, Ordering::Relaxed);
                    i32::try_from(current + 1).expect("a token count fits in an i32")
                })
            };

            let pipe1 = {
                let j2 = Arc::clone(&j2);
                let collection = Arc::clone(&collection);
                parallel_pipe_with_flow::<i32, ()>(move |value, pf| {
                    let ticket = j2.fetch_add(1, Ordering::Relaxed);
                    assert!(ticket < n);
                    assert_eq!(pf.token() % num_lines, pf.line());
                    collection.lock().expect("uncontended").push(value);
                })
            };

            let pipeline = DataPipeline::builder(num_lines)
                .pipe(pipe0)
                .pipe(pipe1)
                .build();

            let module = taskflow.composed_of(&pipeline);
            taskflow.task(module).name("module_of_data_pipeline");

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

            let pipe0 = {
                let j1 = Arc::clone(&j1);
                serial_pipe_with_flow::<(), i32>(move |(), pf| {
                    let current = j1.load(Ordering::Relaxed);
                    if current == n {
                        pf.stop();
                        return 0;
                    }
                    assert_eq!(pf.token() % num_lines, pf.line());
                    j1.fetch_add(1, Ordering::Relaxed);
                    i32::try_from(current + 1).expect("a token count fits in an i32")
                })
            };

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

            let module = taskflow.composed_of(&pipeline);
            taskflow.task(module).name("module_of_data_pipeline");

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

            let pipe0 = {
                let j1 = Arc::clone(&j1);
                serial_pipe_with_flow::<(), i32>(move |(), pf| {
                    let current = j1.load(Ordering::Relaxed);
                    if current == n {
                        pf.stop();
                        return 0;
                    }
                    assert_eq!(pf.token() % num_lines, pf.line());
                    j1.fetch_add(1, Ordering::Relaxed);
                    i32::try_from(current + 1).expect("a token count fits in an i32")
                })
            };

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
                let collection = Arc::clone(&collection);
                parallel_pipe_with_flow::<i32, ()>(move |value, pf| {
                    let ticket = j3.fetch_add(1, Ordering::Relaxed);
                    assert!(ticket < n);
                    assert_eq!(pf.token() % num_lines, pf.line());
                    collection.lock().expect("uncontended").push(value);
                })
            };

            let pipeline = DataPipeline::builder(num_lines)
                .pipe(pipe0)
                .pipe(pipe1)
                .pipe(pipe2)
                .build();

            let module = taskflow.composed_of(&pipeline);
            taskflow.task(module).name("module_of_data_pipeline");

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

            let pipe0 = {
                let j1 = Arc::clone(&j1);
                serial_pipe_with_flow::<(), i32>(move |(), pf| {
                    let current = j1.load(Ordering::Relaxed);
                    if current == n {
                        pf.stop();
                        return 0;
                    }
                    assert_eq!(pf.token() % num_lines, pf.line());
                    j1.fetch_add(1, Ordering::Relaxed);
                    i32::try_from(current + 1).expect("a token count fits in an i32")
                })
            };

            let pipe1 = {
                let j2 = Arc::clone(&j2);
                let collection = Arc::clone(&collection);
                parallel_pipe_with_flow::<i32, i32>(move |value, pf| {
                    let ticket = j2.fetch_add(1, Ordering::Relaxed);
                    assert!(ticket < n);
                    assert_eq!(pf.token() % num_lines, pf.line());
                    collection.lock().expect("uncontended").push(value);
                    value + 1
                })
            };

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

            let module = taskflow.composed_of(&pipeline);
            taskflow.task(module).name("module_of_data_pipeline");

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

            let pipe0 = {
                let j1 = Arc::clone(&j1);
                serial_pipe_with_flow::<(), i32>(move |(), pf| {
                    let current = j1.load(Ordering::Relaxed);
                    if current == n {
                        pf.stop();
                        return 0;
                    }
                    assert_eq!(pf.token() % num_lines, pf.line());
                    j1.fetch_add(1, Ordering::Relaxed);
                    i32::try_from(current + 1).expect("a token count fits in an i32")
                })
            };

            let pipe1 = {
                let j2 = Arc::clone(&j2);
                let collection2 = Arc::clone(&collection2);
                parallel_pipe_with_flow::<i32, i32>(move |value, pf| {
                    let ticket = j2.fetch_add(1, Ordering::Relaxed);
                    assert!(ticket < n);
                    assert_eq!(pf.token() % num_lines, pf.line());
                    collection2.lock().expect("uncontended").push(value);
                    value + 1
                })
            };

            let pipe2 = {
                let j3 = Arc::clone(&j3);
                let collection3 = Arc::clone(&collection3);
                parallel_pipe_with_flow::<i32, ()>(move |value, pf| {
                    let ticket = j3.fetch_add(1, Ordering::Relaxed);
                    assert!(ticket < n);
                    assert_eq!(pf.token() % num_lines, pf.line());
                    collection3.lock().expect("uncontended").push(value);
                })
            };

            let pipeline = DataPipeline::builder(num_lines)
                .pipe(pipe0)
                .pipe(pipe1)
                .pipe(pipe2)
                .build();

            let module = taskflow.composed_of(&pipeline);
            taskflow.task(module).name("module_of_data_pipeline");

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

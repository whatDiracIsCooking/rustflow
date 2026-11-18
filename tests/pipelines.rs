//! Translated from `unittests/test_pipelines.cpp` (Taskflow v4.1.0).
//!
//! Seven structural families of that file -- `Pipeline.1P(S)`, `2P(SS)`,
//! `2P(SP)`, `3P(SSS)`, `3P(SSP)`, `3P(SPS)` and `3P(SPP)`, the letters
//! spelling out each pipe's direction in order -- over lines 1..=4 and
//! workers 1..=4, 112 cases through [`worker_matrix!`]. Every case sweeps
//! upstream's own `N` from `0` to 100 tokens.
//!
//! Three adaptations, every one forced by the port rather than chosen:
//!
//! - **Every piece of state a pipe callable closes over is shared through an
//!   `Arc`, not captured by reference.** A [`Pipe`] callable is
//!   `Fn(&mut Pipeflow) + Send + Sync + 'static` -- `DESIGN.md`'s "Decisions
//!   taken in M8" has why -- where upstream's `mutable` lambda captures its
//!   counters and buffers by reference and mutates them directly. So a
//!   counter here is an `AtomicUsize`, upstream's `std::mutex`-guarded
//!   collections are `Mutex`-guarded here too, and [`Buffer`] stands in for
//!   upstream's `std::vector<std::array<int, P>>` for the same reason.
//! - **No in-graph "test" task.** Upstream wires its assertions into a task
//!   that runs after the module and reruns with the graph; here they run in
//!   the test function itself, directly after
//!   [`RunHandle::wait`](rustflow::RunHandle::wait) returns -- the same
//!   choice `tests/modules.rs`'s `run_n_counts_per_run` makes, and for the
//!   same reason: nothing a task closure could read here is unavailable to
//!   the thread that is already blocked on the run finishing.
//! - **The "reset and rerun" upstream's own `run_n`/callback shape performs
//!   is [`Pipeline::reset`] directly**, rather than upstream's
//!   `executor.run_n(taskflow, 3, callback)` -- which, read closely, resets
//!   the test's own counters only *once*, after all three runs, so two of
//!   upstream's three are a no-op rerun of an exhausted pipe stopping again
//!   immediately rather than a check of [`Pipeline::reset`] at all. Every
//!   case here instead runs once, asserts, calls [`Pipeline::reset`]
//!   explicitly, and runs and asserts a second time -- which is both a more
//!   direct translation of "reset" and the one upstream never actually
//!   exercises in this file for the non-scalable `Pipeline`.
//! - **Every case's own body runs behind [`serialized`].** Each one spins
//!   up its own multi-worker `Executor` and reruns it up to [`MAX_N`]
//!   times; `cargo test`'s default harness runs many `#[test]` functions
//!   at once, so on a many-core box this suite alone can have dozens of
//!   such `Executor`s, none sharing a line with any other, contending for
//!   the same handful of cores at once. That is not concurrency any one
//!   case's own lines and workers produce -- it is the test harness
//!   oversubscribing the machine -- and it is wide enough to turn a
//!   scheduling window this crate's own concurrency notes already call a
//!   false negative's shape (`taskset -c 0,1`, or `--test-threads=1`/`2`,
//!   make every case here pass every time) into an occasional, real
//!   failure of this suite under the gate's own unpinned `cargo test`.
//!   [`serialized`] -- shared with every other pipeline suite, which has
//!   the same problem -- is the fix, not a correctness requirement on the
//!   scheduler: it runs one case's full sweep at a time,
//!   while every worker *inside* that one case's own `Executor` still runs
//!   fully concurrently, which is the only concurrency a pipeline's own
//!   correctness is actually about.
//!
//! The file's last section is not upstream's: `Pipe::set_callable` is the one
//! member of the pipe's own surface whose effect only a run can show, so the
//! case that names it is here rather than in a doc example.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
use rustflow::pipeline::scheduler::Pipeline;
use rustflow::prelude::*;

#[macro_use]
mod common;
#[path = "common/compose.rs"]
mod compose;
#[path = "common/panics.rs"]
mod panics;
#[path = "common/pipeline.rs"]
mod pipeline;
#[path = "common/serialized.rs"]
mod serialized;
#[path = "common/tokens.rs"]
mod tokens;

use compose::compose_module;
use panics::assert_panicked_with;
use pipeline::{Buffer, collecting_pipe, counting_source, propagating_pipe};
use serialized::serialized;
use tokens::{MAX_N, assert_consecutive_from};

// ----------------------------------------------------------------------------
// Pipeline.1P(S): one serial pipe, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `pipeline_1P(L, w, tf::PipeType::SERIAL)`: a single serial pipe
/// that stops after `N` tokens, checking each one lands on the line its
/// token number says it should.
fn pipeline_1p_s(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let j = Arc::new(AtomicUsize::new(0));

            let pipe = {
                let j = Arc::clone(&j);
                Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                    if j.load(Ordering::Relaxed) == n {
                        pf.stop();
                        return;
                    }
                    assert_eq!(pf.token() % num_lines, pf.line());
                    j.fetch_add(1, Ordering::Relaxed);
                })
            };
            let pipeline = Pipeline::new(num_lines, vec![pipe]);

            compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

            for attempt in 0..2 {
                executor.run(&taskflow).wait().expect("no task panicked");
                assert_eq!(j.load(Ordering::Relaxed), n);
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
    pipeline_1p_s {
        pipeline_1p_s_1l_1w(1, 1),
        pipeline_1p_s_1l_2w(1, 2),
        pipeline_1p_s_1l_3w(1, 3),
        pipeline_1p_s_1l_4w(1, 4),
        pipeline_1p_s_2l_1w(2, 1),
        pipeline_1p_s_2l_2w(2, 2),
        pipeline_1p_s_2l_3w(2, 3),
        pipeline_1p_s_2l_4w(2, 4),
        pipeline_1p_s_3l_1w(3, 1),
        pipeline_1p_s_3l_2w(3, 2),
        pipeline_1p_s_3l_3w(3, 3),
        pipeline_1p_s_3l_4w(3, 4),
        pipeline_1p_s_4l_1w(4, 1),
        pipeline_1p_s_4l_2w(4, 2),
        pipeline_1p_s_4l_3w(4, 3),
        pipeline_1p_s_4l_4w(4, 4),
    }
}

// ----------------------------------------------------------------------------
// Pipeline.2P(SS): two serial pipes, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `pipeline_2P_SS(L, w)`: a serial pipe that stops after `N`
/// tokens and writes each into the buffer, and a second serial pipe that
/// checks what it reads there is what the first pipe just wrote.
fn pipeline_2p_ss(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let j1 = Arc::new(AtomicUsize::new(0));
            let j2 = Arc::new(AtomicUsize::new(0));
            let buffer = Buffer::new(num_lines, 2);

            let pipe0 = counting_source(num_lines, move || n, &j1, &buffer);

            let pipe1 = {
                let j2 = Arc::clone(&j2);
                let buffer = buffer.clone();
                Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                    let current = j2.load(Ordering::Relaxed);
                    assert!(current < n);
                    assert_eq!(pf.token() % num_lines, pf.line());
                    let expected =
                        i32::try_from(current + 1).expect("a token count fits in an i32");
                    assert_eq!(expected, buffer.get(pf.line(), pf.pipe() - 1));
                    j2.fetch_add(1, Ordering::Relaxed);
                })
            };

            let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1]);
            compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

            for attempt in 0..2 {
                executor.run(&taskflow).wait().expect("no task panicked");
                assert_eq!(j1.load(Ordering::Relaxed), n);
                assert_eq!(j2.load(Ordering::Relaxed), n);
                assert_eq!(pipeline.num_tokens(), n);

                if attempt == 0 {
                    j1.store(0, Ordering::Relaxed);
                    j2.store(0, Ordering::Relaxed);
                    buffer.clear();
                    pipeline.reset();
                }
            }
        }
    });
}

worker_matrix! {
    pipeline_2p_ss {
        pipeline_2p_ss_1l_1w(1, 1),
        pipeline_2p_ss_1l_2w(1, 2),
        pipeline_2p_ss_1l_3w(1, 3),
        pipeline_2p_ss_1l_4w(1, 4),
        pipeline_2p_ss_2l_1w(2, 1),
        pipeline_2p_ss_2l_2w(2, 2),
        pipeline_2p_ss_2l_3w(2, 3),
        pipeline_2p_ss_2l_4w(2, 4),
        pipeline_2p_ss_3l_1w(3, 1),
        pipeline_2p_ss_3l_2w(3, 2),
        pipeline_2p_ss_3l_3w(3, 3),
        pipeline_2p_ss_3l_4w(3, 4),
        pipeline_2p_ss_4l_1w(4, 1),
        pipeline_2p_ss_4l_2w(4, 2),
        pipeline_2p_ss_4l_3w(4, 3),
        pipeline_2p_ss_4l_4w(4, 4),
    }
}

// ----------------------------------------------------------------------------
// Pipeline.2P(SP): one serial, one parallel pipe, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `pipeline_2P_SP(L, w)`: a serial pipe exactly as
/// [`pipeline_2p_ss`]'s first, and a parallel pipe that collects what it
/// reads from the buffer rather than checking it against a line-local
/// counter -- a parallel pipe gives tokens no order among themselves, so the
/// check is that the right *set* of values came out, not the right
/// sequence.
fn pipeline_2p_sp(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let j1 = Arc::new(AtomicUsize::new(0));
            let j2 = Arc::new(AtomicUsize::new(0));
            let buffer = Buffer::new(num_lines, 2);
            let collection: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

            let pipe0 = counting_source(num_lines, move || n, &j1, &buffer);

            let pipe1 = collecting_pipe(num_lines, move || n, &j2, &buffer, &collection);

            let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1]);
            compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

            for attempt in 0..2 {
                executor.run(&taskflow).wait().expect("no task panicked");
                assert_eq!(j1.load(Ordering::Relaxed), n);
                assert_eq!(j2.load(Ordering::Relaxed), n);
                assert_consecutive_from(&collection, 1, n);
                assert_eq!(pipeline.num_tokens(), n);

                if attempt == 0 {
                    j1.store(0, Ordering::Relaxed);
                    j2.store(0, Ordering::Relaxed);
                    buffer.clear();
                    collection.lock().expect("uncontended").clear();
                    pipeline.reset();
                }
            }
        }
    });
}

worker_matrix! {
    pipeline_2p_sp {
        pipeline_2p_sp_1l_1w(1, 1),
        pipeline_2p_sp_1l_2w(1, 2),
        pipeline_2p_sp_1l_3w(1, 3),
        pipeline_2p_sp_1l_4w(1, 4),
        pipeline_2p_sp_2l_1w(2, 1),
        pipeline_2p_sp_2l_2w(2, 2),
        pipeline_2p_sp_2l_3w(2, 3),
        pipeline_2p_sp_2l_4w(2, 4),
        pipeline_2p_sp_3l_1w(3, 1),
        pipeline_2p_sp_3l_2w(3, 2),
        pipeline_2p_sp_3l_3w(3, 3),
        pipeline_2p_sp_3l_4w(3, 4),
        pipeline_2p_sp_4l_1w(4, 1),
        pipeline_2p_sp_4l_2w(4, 2),
        pipeline_2p_sp_4l_3w(4, 3),
        pipeline_2p_sp_4l_4w(4, 4),
    }
}

// ----------------------------------------------------------------------------
// Pipeline.3P(SSS): three serial pipes, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `pipeline_3P_SSS(L, w)`: a chain of three serial pipes, each
/// checking what it reads is what the one before it just wrote.
fn pipeline_3p_sss(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let j1 = Arc::new(AtomicUsize::new(0));
            let j2 = Arc::new(AtomicUsize::new(0));
            let j3 = Arc::new(AtomicUsize::new(0));
            let buffer = Buffer::new(num_lines, 3);

            let pipe0 = counting_source(num_lines, move || n, &j1, &buffer);

            let pipe1 = {
                let j2 = Arc::clone(&j2);
                let buffer = buffer.clone();
                Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                    let current = j2.load(Ordering::Relaxed);
                    assert!(current < n);
                    let expected =
                        i32::try_from(current + 1).expect("a token count fits in an i32");
                    assert_eq!(expected, buffer.get(pf.line(), pf.pipe() - 1));
                    assert_eq!(pf.token() % num_lines, pf.line());
                    buffer.set(pf.line(), pf.pipe(), expected);
                    j2.fetch_add(1, Ordering::Relaxed);
                })
            };

            let pipe2 = {
                let j3 = Arc::clone(&j3);
                let buffer = buffer.clone();
                Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                    let current = j3.load(Ordering::Relaxed);
                    assert!(current < n);
                    let expected =
                        i32::try_from(current + 1).expect("a token count fits in an i32");
                    assert_eq!(expected, buffer.get(pf.line(), pf.pipe() - 1));
                    assert_eq!(pf.token() % num_lines, pf.line());
                    j3.fetch_add(1, Ordering::Relaxed);
                })
            };

            let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1, pipe2]);
            compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

            for attempt in 0..2 {
                executor.run(&taskflow).wait().expect("no task panicked");
                assert_eq!(j1.load(Ordering::Relaxed), n);
                assert_eq!(j2.load(Ordering::Relaxed), n);
                assert_eq!(j3.load(Ordering::Relaxed), n);
                assert_eq!(pipeline.num_tokens(), n);

                if attempt == 0 {
                    j1.store(0, Ordering::Relaxed);
                    j2.store(0, Ordering::Relaxed);
                    j3.store(0, Ordering::Relaxed);
                    buffer.clear();
                    pipeline.reset();
                }
            }
        }
    });
}

worker_matrix! {
    pipeline_3p_sss {
        pipeline_3p_sss_1l_1w(1, 1),
        pipeline_3p_sss_1l_2w(1, 2),
        pipeline_3p_sss_1l_3w(1, 3),
        pipeline_3p_sss_1l_4w(1, 4),
        pipeline_3p_sss_2l_1w(2, 1),
        pipeline_3p_sss_2l_2w(2, 2),
        pipeline_3p_sss_2l_3w(2, 3),
        pipeline_3p_sss_2l_4w(2, 4),
        pipeline_3p_sss_3l_1w(3, 1),
        pipeline_3p_sss_3l_2w(3, 2),
        pipeline_3p_sss_3l_3w(3, 3),
        pipeline_3p_sss_3l_4w(3, 4),
        pipeline_3p_sss_4l_1w(4, 1),
        pipeline_3p_sss_4l_2w(4, 2),
        pipeline_3p_sss_4l_3w(4, 3),
        pipeline_3p_sss_4l_4w(4, 4),
    }
}

// ----------------------------------------------------------------------------
// Pipeline.3P(SSP): two serial pipes then a parallel one, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `pipeline_3P_SSP(L, w)`: two serial pipes exactly as
/// [`pipeline_3p_sss`]'s first two, then a parallel pipe that collects what
/// it reads rather than checking it against a line-local counter.
fn pipeline_3p_ssp(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let j1 = Arc::new(AtomicUsize::new(0));
            let j2 = Arc::new(AtomicUsize::new(0));
            let j3 = Arc::new(AtomicUsize::new(0));
            let buffer = Buffer::new(num_lines, 3);
            let collection: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

            let pipe0 = counting_source(num_lines, move || n, &j1, &buffer);

            let pipe1 = {
                let j2 = Arc::clone(&j2);
                let buffer = buffer.clone();
                Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                    let current = j2.load(Ordering::Relaxed);
                    assert!(current < n);
                    let expected =
                        i32::try_from(current + 1).expect("a token count fits in an i32");
                    assert_eq!(expected, buffer.get(pf.line(), pf.pipe() - 1));
                    assert_eq!(pf.token() % num_lines, pf.line());
                    buffer.set(pf.line(), pf.pipe(), expected);
                    j2.fetch_add(1, Ordering::Relaxed);
                })
            };

            let pipe2 = collecting_pipe(num_lines, move || n, &j3, &buffer, &collection);

            let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1, pipe2]);
            compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

            for attempt in 0..2 {
                executor.run(&taskflow).wait().expect("no task panicked");
                assert_eq!(j1.load(Ordering::Relaxed), n);
                assert_eq!(j2.load(Ordering::Relaxed), n);
                assert_eq!(j3.load(Ordering::Relaxed), n);
                assert_consecutive_from(&collection, 1, n);
                assert_eq!(pipeline.num_tokens(), n);

                if attempt == 0 {
                    j1.store(0, Ordering::Relaxed);
                    j2.store(0, Ordering::Relaxed);
                    j3.store(0, Ordering::Relaxed);
                    buffer.clear();
                    collection.lock().expect("uncontended").clear();
                    pipeline.reset();
                }
            }
        }
    });
}

worker_matrix! {
    pipeline_3p_ssp {
        pipeline_3p_ssp_1l_1w(1, 1),
        pipeline_3p_ssp_1l_2w(1, 2),
        pipeline_3p_ssp_1l_3w(1, 3),
        pipeline_3p_ssp_1l_4w(1, 4),
        pipeline_3p_ssp_2l_1w(2, 1),
        pipeline_3p_ssp_2l_2w(2, 2),
        pipeline_3p_ssp_2l_3w(2, 3),
        pipeline_3p_ssp_2l_4w(2, 4),
        pipeline_3p_ssp_3l_1w(3, 1),
        pipeline_3p_ssp_3l_2w(3, 2),
        pipeline_3p_ssp_3l_3w(3, 3),
        pipeline_3p_ssp_3l_4w(3, 4),
        pipeline_3p_ssp_4l_1w(4, 1),
        pipeline_3p_ssp_4l_2w(4, 2),
        pipeline_3p_ssp_4l_3w(4, 3),
        pipeline_3p_ssp_4l_4w(4, 4),
    }
}

// ----------------------------------------------------------------------------
// Pipeline.3P(SPS): serial, parallel, serial, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `pipeline_3P_SPS(L, w)`: a serial pipe, a parallel pipe that
/// both collects and propagates what it read, and a serial pipe that checks
/// the propagated value -- the middle pipe being parallel does not relax the
/// serial pipes around it, which this case's last-pipe check pins.
fn pipeline_3p_sps(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let j1 = Arc::new(AtomicUsize::new(0));
            let j2 = Arc::new(AtomicUsize::new(0));
            let j3 = Arc::new(AtomicUsize::new(0));
            let buffer = Buffer::new(num_lines, 3);
            let collection: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

            let pipe0 = counting_source(num_lines, move || n, &j1, &buffer);

            let pipe1 = propagating_pipe(num_lines, move || n, &j2, &buffer, &collection);

            let pipe2 = {
                let j3 = Arc::clone(&j3);
                let buffer = buffer.clone();
                Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                    let current = j3.load(Ordering::Relaxed);
                    assert!(current < n);
                    assert_eq!(pf.token() % num_lines, pf.line());
                    let expected =
                        i32::try_from(current + 2).expect("a token count fits in an i32");
                    assert_eq!(expected, buffer.get(pf.line(), pf.pipe() - 1));
                    j3.fetch_add(1, Ordering::Relaxed);
                })
            };

            let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1, pipe2]);
            compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

            for attempt in 0..2 {
                executor.run(&taskflow).wait().expect("no task panicked");
                assert_eq!(j1.load(Ordering::Relaxed), n);
                assert_eq!(j2.load(Ordering::Relaxed), n);
                assert_eq!(j3.load(Ordering::Relaxed), n);
                assert_consecutive_from(&collection, 1, n);
                assert_eq!(pipeline.num_tokens(), n);

                if attempt == 0 {
                    j1.store(0, Ordering::Relaxed);
                    j2.store(0, Ordering::Relaxed);
                    j3.store(0, Ordering::Relaxed);
                    buffer.clear();
                    collection.lock().expect("uncontended").clear();
                    pipeline.reset();
                }
            }
        }
    });
}

worker_matrix! {
    pipeline_3p_sps {
        pipeline_3p_sps_1l_1w(1, 1),
        pipeline_3p_sps_1l_2w(1, 2),
        pipeline_3p_sps_1l_3w(1, 3),
        pipeline_3p_sps_1l_4w(1, 4),
        pipeline_3p_sps_2l_1w(2, 1),
        pipeline_3p_sps_2l_2w(2, 2),
        pipeline_3p_sps_2l_3w(2, 3),
        pipeline_3p_sps_2l_4w(2, 4),
        pipeline_3p_sps_3l_1w(3, 1),
        pipeline_3p_sps_3l_2w(3, 2),
        pipeline_3p_sps_3l_3w(3, 3),
        pipeline_3p_sps_3l_4w(3, 4),
        pipeline_3p_sps_4l_1w(4, 1),
        pipeline_3p_sps_4l_2w(4, 2),
        pipeline_3p_sps_4l_3w(4, 3),
        pipeline_3p_sps_4l_4w(4, 4),
    }
}

// ----------------------------------------------------------------------------
// Pipeline.3P(SPP): serial, parallel, parallel, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `pipeline_3P_SPP(L, w)`: a serial pipe followed by two parallel
/// pipes in a row, each collecting into its own set -- the only family
/// where two pipes with no ordering between their own tokens also have none
/// between each other's, which is what the two independent collections
/// check.
fn pipeline_3p_spp(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let j1 = Arc::new(AtomicUsize::new(0));
            let j2 = Arc::new(AtomicUsize::new(0));
            let j3 = Arc::new(AtomicUsize::new(0));
            let buffer = Buffer::new(num_lines, 3);
            let collection2: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
            let collection3: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

            let pipe0 = counting_source(num_lines, move || n, &j1, &buffer);

            let pipe1 = propagating_pipe(num_lines, move || n, &j2, &buffer, &collection2);

            let pipe2 = collecting_pipe(num_lines, move || n, &j3, &buffer, &collection3);

            let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1, pipe2]);
            compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

            for attempt in 0..2 {
                executor.run(&taskflow).wait().expect("no task panicked");
                assert_eq!(j1.load(Ordering::Relaxed), n);
                assert_eq!(j2.load(Ordering::Relaxed), n);
                assert_eq!(j3.load(Ordering::Relaxed), n);
                assert_consecutive_from(&collection2, 1, n);
                assert_consecutive_from(&collection3, 2, n);
                assert_eq!(pipeline.num_tokens(), n);

                if attempt == 0 {
                    j1.store(0, Ordering::Relaxed);
                    j2.store(0, Ordering::Relaxed);
                    j3.store(0, Ordering::Relaxed);
                    buffer.clear();
                    collection2.lock().expect("uncontended").clear();
                    collection3.lock().expect("uncontended").clear();
                    pipeline.reset();
                }
            }
        }
    });
}

worker_matrix! {
    pipeline_3p_spp {
        pipeline_3p_spp_1l_1w(1, 1),
        pipeline_3p_spp_1l_2w(1, 2),
        pipeline_3p_spp_1l_3w(1, 3),
        pipeline_3p_spp_1l_4w(1, 4),
        pipeline_3p_spp_2l_1w(2, 1),
        pipeline_3p_spp_2l_2w(2, 2),
        pipeline_3p_spp_2l_3w(2, 3),
        pipeline_3p_spp_2l_4w(2, 4),
        pipeline_3p_spp_3l_1w(3, 1),
        pipeline_3p_spp_3l_2w(3, 2),
        pipeline_3p_spp_3l_3w(3, 3),
        pipeline_3p_spp_3l_4w(3, 4),
        pipeline_3p_spp_4l_1w(4, 1),
        pipeline_3p_spp_4l_2w(4, 2),
        pipeline_3p_spp_4l_3w(4, 3),
        pipeline_3p_spp_4l_4w(4, 4),
    }
}

// ----------------------------------------------------------------------------
// The panic-path cells: a pipe callable that fails
// ----------------------------------------------------------------------------
//
// Not upstream's: `test_pipelines.cpp` has no failing pipe anywhere. A pipe
// callable runs inside a line's nonpreemptive runtime task, so the three
// cases here are one per place that task can be when the panic happens --
// the first serial pipe, which is the one generating tokens, a serial pipe
// after it, and a parallel one. The reset-and-rerun half of the row is the
// fourth case, which was `#[ignore]`d against the defect it found until that
// defect was fixed; its own doc comment says why it is one case for the three
// chains rather than three.

/// How many tokens each case below lets through when nothing panics.
const TOKENS: usize = 5;

/// The token the armed pipe panics on, which is far enough in that the
/// tokens before it have been through the whole chain.
const FAILS_AT: usize = 2;

/// How many lines each case runs over: more than [`TOKENS`], so that every
/// line is in use and a failing token is never alone on the pipeline.
const LINES: usize = 4;

/// The first pipe of each case: stops the pipeline after [`TOKENS`] tokens
/// and counts the rest, panicking with `"x"` on token [`FAILS_AT`] if
/// `armed` is the counter holding `1` -- which is how the first case makes
/// the token *source* the pipe that fails.
fn stopping_source(seen: &Arc<AtomicUsize>, armed: Option<&Arc<AtomicUsize>>) -> Pipe {
    let seen = Arc::clone(seen);
    let armed = armed.cloned();
    Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
        if pf.token() == TOKENS {
            pf.stop();
            return;
        }
        fail_if_armed(armed.as_ref(), pf.token());
        seen.fetch_add(1, Ordering::Relaxed);
    })
}

/// A pipe after the source that counts the tokens it is handed and fails on
/// token [`FAILS_AT`] while `armed` holds `1`.
fn failing_pipe(kind: PipeType, seen: &Arc<AtomicUsize>, armed: &Arc<AtomicUsize>) -> Pipe {
    let seen = Arc::clone(seen);
    let armed = Arc::clone(armed);
    Pipe::new(kind, move |pf: &mut Pipeflow| {
        fail_if_armed(Some(&armed), pf.token());
        seen.fetch_add(1, Ordering::Relaxed);
    })
}

/// Panics with `"x"` if `armed` holds `1` and `token` is the one to fail on.
///
/// A bare `panic!` of a literal rather than an `assert!`, so that the
/// payload is the `&str` [`assert_panicked_with`] reads back.
fn fail_if_armed(armed: Option<&Arc<AtomicUsize>>, token: usize) {
    if token == FAILS_AT && armed.is_some_and(|armed| armed.load(Ordering::SeqCst) == 1) {
        panic!("x");
    }
}

/// Runs `taskflow` and asserts the armed pipe's panic is what the run
/// reports.
fn assert_the_armed_pipe_failed_the_run(executor: &Executor, taskflow: &Taskflow) {
    let error = executor
        .run(taskflow)
        .wait()
        .expect_err("a pipe callable panicked, so the run failed");
    assert_panicked_with(error, "x");
}

/// Asserts `reset` put a failed run's pipeline back to its just-built
/// progress, which is the half of that claim no second run is needed for:
/// the token counter is what a caller can read of it.
fn assert_the_reset_cleared_the_token_count(pipeline: &Pipeline) {
    pipeline.reset();
    assert_eq!(pipeline.num_tokens(), 0, "the reset gave the counter back");
}

/// Disarms the failing pipe, resets `pipeline` and runs `taskflow` again,
/// asserting the whole token stream got through this time: every counter in
/// `seen` is [`TOKENS`], as is the pipeline's own token count.
fn assert_it_runs_again_after_reset(
    executor: &Executor,
    taskflow: &Taskflow,
    pipeline: &Pipeline,
    armed: &Arc<AtomicUsize>,
    seen: &[&Arc<AtomicUsize>],
) {
    armed.store(0, Ordering::SeqCst);
    for counter in seen {
        counter.store(0, Ordering::Relaxed);
    }
    pipeline.reset();
    assert_eq!(pipeline.num_tokens(), 0, "the reset gave the counter back");

    executor
        .run(taskflow)
        .wait()
        .expect("nothing panics once the pipe is disarmed");

    assert_eq!(pipeline.num_tokens(), TOKENS);
    for counter in seen {
        assert_eq!(counter.load(Ordering::Relaxed), TOKENS);
    }
}

/// Runs a one-task taskflow on `executor` and asserts it ran: the third
/// claim of each case in this section, that a failed run left the executor
/// usable.
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

/// A panic in the *first* serial pipe -- the one that generates tokens --
/// fails the composing run, and the token counter stops exactly where the
/// panic did: a serial first pipe runs one token at a time, so the two
/// tokens before the failing one are the whole of what was generated.
#[test]
fn a_panicking_first_serial_pipe_fails_the_composing_run() {
    serialized(|| {
        let executor = Executor::new(LINES);
        let armed = Arc::new(AtomicUsize::new(1));
        let seen = Arc::new(AtomicUsize::new(0));

        let pipeline = Pipeline::new(LINES, vec![stopping_source(&seen, Some(&armed))]);
        let mut taskflow = Taskflow::new();
        compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

        assert_the_armed_pipe_failed_the_run(&executor, &taskflow);
        assert_eq!(
            pipeline.num_tokens(),
            FAILS_AT,
            "the tokens before the failing one were generated and no more"
        );
        assert_eq!(seen.load(Ordering::Relaxed), FAILS_AT);

        assert_the_reset_cleared_the_token_count(&pipeline);
        assert_executor_is_reusable(&executor);
    });
}

/// A panic in a serial pipe *after* the source fails the run too, and that
/// pipe has seen exactly the tokens before the failing one: a serial pipe
/// takes its tokens in order whatever line each arrives on.
///
/// What the source had generated by then is deliberately not asserted --
/// it runs ahead of the chain by as many tokens as there are lines, so the
/// number is a race rather than a claim.
#[test]
fn a_panicking_inner_serial_pipe_fails_the_composing_run() {
    serialized(|| {
        let executor = Executor::new(LINES);
        let armed = Arc::new(AtomicUsize::new(1));
        let generated = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(AtomicUsize::new(0));

        let pipeline = Pipeline::new(
            LINES,
            vec![
                stopping_source(&generated, None),
                failing_pipe(PipeType::Serial, &seen, &armed),
            ],
        );
        let mut taskflow = Taskflow::new();
        compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

        assert_the_armed_pipe_failed_the_run(&executor, &taskflow);
        assert_eq!(
            seen.load(Ordering::Relaxed),
            FAILS_AT,
            "a serial pipe had the tokens before the failing one and no more"
        );

        assert_the_reset_cleared_the_token_count(&pipeline);
        assert_executor_is_reusable(&executor);
    });
}

/// A panic in a parallel pipe fails the run the same way, which is the case
/// where the failing token is one of several of that pipe in flight at once.
///
/// Nothing is asserted about how many tokens that pipe had seen when it
/// failed: a parallel pipe has no order of its own, so the tokens that got
/// through before the failing one are not a number this case can name. What
/// the run returning at all says instead is that none of those in-flight
/// tokens is still owed anything.
#[test]
fn a_panicking_parallel_pipe_fails_the_composing_run() {
    serialized(|| {
        let executor = Executor::new(LINES);
        let armed = Arc::new(AtomicUsize::new(1));
        let generated = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(AtomicUsize::new(0));

        let pipeline = Pipeline::new(
            LINES,
            vec![
                stopping_source(&generated, None),
                failing_pipe(PipeType::Parallel, &seen, &armed),
            ],
        );
        let mut taskflow = Taskflow::new();
        compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

        assert_the_armed_pipe_failed_the_run(&executor, &taskflow);

        assert_the_reset_cleared_the_token_count(&pipeline);
        assert_executor_is_reusable(&executor);
    });
}

/// A pipeline whose pipe panicked, `reset` and run a second time, takes the
/// whole token stream through -- which is the rest of the row the three
/// cases above cover: the tokens in flight are not merely dropped, the
/// pipeline is runnable again afterwards.
///
/// This cell found #320 and was `#[ignore]`d against it, which is also the
/// reason it is one case rather than three. A line task is bypass-scheduled
/// (`Runtime::schedule`), and every line but the one the conditioner selects
/// is reachable *only* that way, so a panic that left another line queued
/// and then skipped by the cancellation left that line's node claimed for
/// ever: the next run's bypass of it queued nothing, having already told the
/// parent to expect it, and the run never finished. The chain here is the
/// parallel one because it is the shape that reproduced most readily, but
/// all three chains above hung on the same window when rerun, and the one
/// fix -- the claim handed back by every job dispatch, skipped or not --
/// retired all of it.
#[test]
fn a_reset_pipeline_runs_again_after_a_panicking_pipe() {
    serialized(|| {
        let executor = Executor::new(LINES);
        let armed = Arc::new(AtomicUsize::new(1));
        let generated = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(AtomicUsize::new(0));

        let pipeline = Pipeline::new(
            LINES,
            vec![
                stopping_source(&generated, None),
                failing_pipe(PipeType::Parallel, &seen, &armed),
            ],
        );
        let mut taskflow = Taskflow::new();
        compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

        assert_the_armed_pipe_failed_the_run(&executor, &taskflow);

        assert_it_runs_again_after_reset(
            &executor,
            &taskflow,
            &pipeline,
            &armed,
            &[&generated, &seen],
        );
        assert_executor_is_reusable(&executor);
    });
}

// ----------------------------------------------------------------------------
// The pipe's own setter (not upstream's)
// ----------------------------------------------------------------------------

/// `Pipe::set_callable` -- upstream's `Pipe::callable(U&&)` -- replaces the
/// body the scheduler runs for every token that reaches the pipe, and leaves
/// the pipe's direction where it was.
///
/// Upstream's suite calls neither setter. `Pipe::set_type` is observable
/// without running anything and its doc example asserts it there; a replaced
/// *callable* is observable only through a run, so the case for it is here,
/// and it is this file's smallest pipeline: one serial pipe over two lines,
/// stopping after four tokens the way `pipeline_1p_s` does.
#[test]
fn set_callable_replaces_the_body_a_pipe_runs() {
    /// Tokens the replaced body lets through before it stops the pipeline.
    const REPLACED_TOKENS: usize = 4;

    serialized(|| {
        let original = Arc::new(AtomicUsize::new(0));
        let replaced = Arc::new(AtomicUsize::new(0));

        let mut pipe = {
            let original = Arc::clone(&original);
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                original.fetch_add(1, Ordering::Relaxed);
                pf.stop();
            })
        };
        assert_eq!(pipe.pipe_type(), PipeType::Serial);

        pipe.set_callable({
            let replaced = Arc::clone(&replaced);
            move |pf: &mut Pipeflow| {
                if replaced.load(Ordering::Relaxed) == REPLACED_TOKENS {
                    pf.stop();
                    return;
                }
                replaced.fetch_add(1, Ordering::Relaxed);
            }
        });
        assert_eq!(
            pipe.pipe_type(),
            PipeType::Serial,
            "a replaced callable leaves the direction alone"
        );

        let pipeline = Pipeline::new(2, vec![pipe]);
        let mut taskflow = Taskflow::new();
        compose_module(&mut taskflow, &pipeline, "module_of_pipeline");
        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(replaced.load(Ordering::Relaxed), REPLACED_TOKENS);
        assert_eq!(pipeline.num_tokens(), REPLACED_TOKENS);
        assert_eq!(
            original.load(Ordering::Relaxed),
            0,
            "the body that was replaced never ran, so it never stopped the run"
        );
    });
}

/// `Pipeline::taskflow` hands back a share of the taskflow this pipeline
/// composes into a module task: upstream's `graph()`, widened from a
/// `Graph&` to the [`Taskflow`] that owns it.
///
/// Three claims. It is a *share*, so two calls name one taskflow rather than
/// a copy of it. It is this pipeline's own scheduling graph, which is one
/// conditioner picking a line over one line task per line, under the names
/// the constructor gave them. And it is live rather than a snapshot: run
/// directly, with no module task in between, it moves this pipeline's own
/// tokens exactly as composing it does -- which is what makes the handle
/// worth having to a caller who is not implementing [`Composable`].
#[test]
fn the_taskflow_a_pipeline_hands_back_is_its_own_live_graph() {
    /// Lines, so that the line tasks are more than one and `cond` has a
    /// choice to make.
    const LINES: usize = 3;
    /// Tokens the pipe lets through before it stops.
    const TOKENS: usize = 7;

    serialized(|| {
        let seen = Arc::new(AtomicUsize::new(0));
        let pipe = {
            let seen = Arc::clone(&seen);
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                if seen.load(Ordering::Relaxed) == TOKENS {
                    pf.stop();
                    return;
                }
                seen.fetch_add(1, Ordering::Relaxed);
            })
        };
        let pipeline = Pipeline::new(LINES, vec![pipe]);

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
        let names: Vec<&str> = handed
            .graph()
            .tasks()
            .map(|task| handed.task_ref(task).name())
            .collect();
        assert_eq!(names, vec!["cond", "rt-0", "rt-1", "rt-2"]);

        Executor::new(2)
            .run(&handed)
            .wait()
            .expect("no task panicked");

        assert_eq!(seen.load(Ordering::Relaxed), TOKENS);
        assert_eq!(
            pipeline.num_tokens(),
            TOKENS,
            "the run moved this pipeline's own token count"
        );
    });
}

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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
use rustflow::pipeline::scheduler::Pipeline;
use rustflow::prelude::*;
use rustflow::Executor;

#[macro_use]
mod common;

/// Upstream's `maxN`: the largest token count any case sweeps up to.
const MAX_N: usize = 100;

/// A pipeline's `lines * pipes` scratch buffer: upstream's
/// `std::vector<std::array<int, P>> mybuffer(L)`, made safe for a `Fn` pipe
/// callable to share across lines by putting one `Mutex` around the whole
/// thing rather than reaching for `unsafe` -- correctness here, not
/// contention, since every case's bodies touch it only a handful of times
/// per token.
#[derive(Clone)]
struct Buffer {
    rows: Arc<Mutex<Vec<Vec<i32>>>>,
}

impl Buffer {
    /// A buffer of `num_lines` rows of `num_pipes` zeroed slots.
    fn new(num_lines: usize, num_pipes: usize) -> Self {
        Self {
            rows: Arc::new(Mutex::new(vec![vec![0; num_pipes]; num_lines])),
        }
    }

    /// Writes `value` into `line`'s `pipe` slot.
    fn set(&self, line: usize, pipe: usize, value: i32) {
        self.rows.lock().expect("uncontended")[line][pipe] = value;
    }

    /// Reads `line`'s `pipe` slot.
    fn get(&self, line: usize, pipe: usize) -> i32 {
        self.rows.lock().expect("uncontended")[line][pipe]
    }

    /// Zeroes every slot, between a case's two runs.
    fn clear(&self) {
        for row in self.rows.lock().expect("uncontended").iter_mut() {
            row.iter_mut().for_each(|slot| *slot = 0);
        }
    }
}

/// Asserts that `collected` holds exactly `n` values and, once sorted, is
/// `start, start + 1, ..., start + n - 1`: every parallel-pipe case's
/// `std::sort` plus loop of `REQUIRE(collection[i] == ...)`.
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
// Pipeline.1P(S): one serial pipe, L lines, W workers
// ----------------------------------------------------------------------------

/// Upstream's `pipeline_1P(L, w, tf::PipeType::SERIAL)`: a single serial pipe
/// that stops after `N` tokens, checking each one lands on the line its
/// token number says it should.
fn pipeline_1p_s(num_lines: usize, num_workers: usize) {
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

        let module = taskflow.composed_of(&pipeline);
        taskflow.task(module).name("module_of_pipeline");

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
    let executor = Executor::new(num_workers);

    for n in 0..=MAX_N {
        let mut taskflow = Taskflow::new();
        let j1 = Arc::new(AtomicUsize::new(0));
        let j2 = Arc::new(AtomicUsize::new(0));
        let buffer = Buffer::new(num_lines, 2);

        let pipe0 = {
            let j1 = Arc::clone(&j1);
            let buffer = buffer.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j1.load(Ordering::Relaxed);
                if current == n {
                    pf.stop();
                    return;
                }
                assert_eq!(pf.token() % num_lines, pf.line());
                let value = i32::try_from(current + 1).expect("a token count fits in an i32");
                buffer.set(pf.line(), pf.pipe(), value);
                j1.fetch_add(1, Ordering::Relaxed);
            })
        };

        let pipe1 = {
            let j2 = Arc::clone(&j2);
            let buffer = buffer.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j2.load(Ordering::Relaxed);
                assert!(current < n);
                assert_eq!(pf.token() % num_lines, pf.line());
                let expected = i32::try_from(current + 1).expect("a token count fits in an i32");
                assert_eq!(expected, buffer.get(pf.line(), pf.pipe() - 1));
                j2.fetch_add(1, Ordering::Relaxed);
            })
        };

        let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1]);
        let module = taskflow.composed_of(&pipeline);
        taskflow.task(module).name("module_of_pipeline");

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
    let executor = Executor::new(num_workers);

    for n in 0..=MAX_N {
        let mut taskflow = Taskflow::new();
        let j1 = Arc::new(AtomicUsize::new(0));
        let j2 = Arc::new(AtomicUsize::new(0));
        let buffer = Buffer::new(num_lines, 2);
        let collection: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

        let pipe0 = {
            let j1 = Arc::clone(&j1);
            let buffer = buffer.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j1.load(Ordering::Relaxed);
                if current == n {
                    pf.stop();
                    return;
                }
                assert_eq!(pf.token() % num_lines, pf.line());
                let value = i32::try_from(current + 1).expect("a token count fits in an i32");
                buffer.set(pf.line(), pf.pipe(), value);
                j1.fetch_add(1, Ordering::Relaxed);
            })
        };

        let pipe1 = {
            let j2 = Arc::clone(&j2);
            let buffer = buffer.clone();
            let collection = Arc::clone(&collection);
            Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
                let ticket = j2.fetch_add(1, Ordering::Relaxed);
                assert!(ticket < n);
                let mut collected = collection.lock().expect("uncontended");
                assert_eq!(pf.token() % num_lines, pf.line());
                collected.push(buffer.get(pf.line(), pf.pipe() - 1));
            })
        };

        let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1]);
        let module = taskflow.composed_of(&pipeline);
        taskflow.task(module).name("module_of_pipeline");

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
    let executor = Executor::new(num_workers);

    for n in 0..=MAX_N {
        let mut taskflow = Taskflow::new();
        let j1 = Arc::new(AtomicUsize::new(0));
        let j2 = Arc::new(AtomicUsize::new(0));
        let j3 = Arc::new(AtomicUsize::new(0));
        let buffer = Buffer::new(num_lines, 3);

        let pipe0 = {
            let j1 = Arc::clone(&j1);
            let buffer = buffer.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j1.load(Ordering::Relaxed);
                if current == n {
                    pf.stop();
                    return;
                }
                assert_eq!(pf.token() % num_lines, pf.line());
                let value = i32::try_from(current + 1).expect("a token count fits in an i32");
                buffer.set(pf.line(), pf.pipe(), value);
                j1.fetch_add(1, Ordering::Relaxed);
            })
        };

        let pipe1 = {
            let j2 = Arc::clone(&j2);
            let buffer = buffer.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j2.load(Ordering::Relaxed);
                assert!(current < n);
                let expected = i32::try_from(current + 1).expect("a token count fits in an i32");
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
                let expected = i32::try_from(current + 1).expect("a token count fits in an i32");
                assert_eq!(expected, buffer.get(pf.line(), pf.pipe() - 1));
                assert_eq!(pf.token() % num_lines, pf.line());
                j3.fetch_add(1, Ordering::Relaxed);
            })
        };

        let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1, pipe2]);
        let module = taskflow.composed_of(&pipeline);
        taskflow.task(module).name("module_of_pipeline");

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
    let executor = Executor::new(num_workers);

    for n in 0..=MAX_N {
        let mut taskflow = Taskflow::new();
        let j1 = Arc::new(AtomicUsize::new(0));
        let j2 = Arc::new(AtomicUsize::new(0));
        let j3 = Arc::new(AtomicUsize::new(0));
        let buffer = Buffer::new(num_lines, 3);
        let collection: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

        let pipe0 = {
            let j1 = Arc::clone(&j1);
            let buffer = buffer.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j1.load(Ordering::Relaxed);
                if current == n {
                    pf.stop();
                    return;
                }
                assert_eq!(pf.token() % num_lines, pf.line());
                let value = i32::try_from(current + 1).expect("a token count fits in an i32");
                buffer.set(pf.line(), pf.pipe(), value);
                j1.fetch_add(1, Ordering::Relaxed);
            })
        };

        let pipe1 = {
            let j2 = Arc::clone(&j2);
            let buffer = buffer.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j2.load(Ordering::Relaxed);
                assert!(current < n);
                let expected = i32::try_from(current + 1).expect("a token count fits in an i32");
                assert_eq!(expected, buffer.get(pf.line(), pf.pipe() - 1));
                assert_eq!(pf.token() % num_lines, pf.line());
                buffer.set(pf.line(), pf.pipe(), expected);
                j2.fetch_add(1, Ordering::Relaxed);
            })
        };

        let pipe2 = {
            let j3 = Arc::clone(&j3);
            let buffer = buffer.clone();
            let collection = Arc::clone(&collection);
            Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
                let ticket = j3.fetch_add(1, Ordering::Relaxed);
                assert!(ticket < n);
                let mut collected = collection.lock().expect("uncontended");
                assert_eq!(pf.token() % num_lines, pf.line());
                collected.push(buffer.get(pf.line(), pf.pipe() - 1));
            })
        };

        let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1, pipe2]);
        let module = taskflow.composed_of(&pipeline);
        taskflow.task(module).name("module_of_pipeline");

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
    let executor = Executor::new(num_workers);

    for n in 0..=MAX_N {
        let mut taskflow = Taskflow::new();
        let j1 = Arc::new(AtomicUsize::new(0));
        let j2 = Arc::new(AtomicUsize::new(0));
        let j3 = Arc::new(AtomicUsize::new(0));
        let buffer = Buffer::new(num_lines, 3);
        let collection: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

        let pipe0 = {
            let j1 = Arc::clone(&j1);
            let buffer = buffer.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j1.load(Ordering::Relaxed);
                if current == n {
                    pf.stop();
                    return;
                }
                assert_eq!(pf.token() % num_lines, pf.line());
                let value = i32::try_from(current + 1).expect("a token count fits in an i32");
                buffer.set(pf.line(), pf.pipe(), value);
                j1.fetch_add(1, Ordering::Relaxed);
            })
        };

        let pipe1 = {
            let j2 = Arc::clone(&j2);
            let buffer = buffer.clone();
            let collection = Arc::clone(&collection);
            Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
                let ticket = j2.fetch_add(1, Ordering::Relaxed);
                assert!(ticket < n);
                let previous = buffer.get(pf.line(), pf.pipe() - 1);
                buffer.set(pf.line(), pf.pipe(), previous + 1);
                assert_eq!(pf.token() % num_lines, pf.line());
                collection.lock().expect("uncontended").push(previous);
            })
        };

        let pipe2 = {
            let j3 = Arc::clone(&j3);
            let buffer = buffer.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j3.load(Ordering::Relaxed);
                assert!(current < n);
                assert_eq!(pf.token() % num_lines, pf.line());
                let expected = i32::try_from(current + 2).expect("a token count fits in an i32");
                assert_eq!(expected, buffer.get(pf.line(), pf.pipe() - 1));
                j3.fetch_add(1, Ordering::Relaxed);
            })
        };

        let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1, pipe2]);
        let module = taskflow.composed_of(&pipeline);
        taskflow.task(module).name("module_of_pipeline");

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
    let executor = Executor::new(num_workers);

    for n in 0..=MAX_N {
        let mut taskflow = Taskflow::new();
        let j1 = Arc::new(AtomicUsize::new(0));
        let j2 = Arc::new(AtomicUsize::new(0));
        let j3 = Arc::new(AtomicUsize::new(0));
        let buffer = Buffer::new(num_lines, 3);
        let collection2: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
        let collection3: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

        let pipe0 = {
            let j1 = Arc::clone(&j1);
            let buffer = buffer.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j1.load(Ordering::Relaxed);
                if current == n {
                    pf.stop();
                    return;
                }
                assert_eq!(pf.token() % num_lines, pf.line());
                let value = i32::try_from(current + 1).expect("a token count fits in an i32");
                buffer.set(pf.line(), pf.pipe(), value);
                j1.fetch_add(1, Ordering::Relaxed);
            })
        };

        let pipe1 = {
            let j2 = Arc::clone(&j2);
            let buffer = buffer.clone();
            let collection2 = Arc::clone(&collection2);
            Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
                let ticket = j2.fetch_add(1, Ordering::Relaxed);
                assert!(ticket < n);
                let previous = buffer.get(pf.line(), pf.pipe() - 1);
                buffer.set(pf.line(), pf.pipe(), previous + 1);
                assert_eq!(pf.token() % num_lines, pf.line());
                collection2.lock().expect("uncontended").push(previous);
            })
        };

        let pipe2 = {
            let j3 = Arc::clone(&j3);
            let buffer = buffer.clone();
            let collection3 = Arc::clone(&collection3);
            Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
                let ticket = j3.fetch_add(1, Ordering::Relaxed);
                assert!(ticket < n);
                let previous = buffer.get(pf.line(), pf.pipe() - 1);
                assert_eq!(pf.token() % num_lines, pf.line());
                collection3.lock().expect("uncontended").push(previous);
            })
        };

        let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1, pipe2]);
        let module = taskflow.composed_of(&pipeline);
        taskflow.task(module).name("module_of_pipeline");

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

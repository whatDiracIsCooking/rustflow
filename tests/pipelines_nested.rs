//! Translated from `unittests/test_pipelines.cpp` (Taskflow v4.1.0).
//!
//! The two composition families sibling `tests/pipelines_compose.rs` (#183)
//! does not cover: `Ifelse.Pipelines` (16 cases over lines `1, 3, 5, 7` and
//! workers `1..=4`) and `PipelineinPipeline.Pipelines` (18 cases over lines
//! `1, 3, 5`, workers `1, 2` and sub-lines `1, 3, 4`), through
//! [`worker_matrix!`]. `tests/pipelines.rs`'s own docs cover the adaptations
//! common to every file in this family -- `Arc`s standing in for upstream's
//! by-reference captures, [`Buffer`] for its `std::vector<std::array<int,
//! P>>`, no in-graph "test" task, and [`serialized`] against the harness
//! running every other `#[test]` in the same process.
//!
//! # Design
//!
//! **`Ifelse.Pipelines` is not a condition task.** Upstream's own case of
//! that name has no `tf::Task::precede` branch at all: it is a three-pipe
//! (`SPS`) pipeline whose three pipes each take a data-dependent `if`/`else`
//! over a token's running value, and [`ifelse_pipe_ans`] is the same three
//! branches worked out independently for the check. [`ifelse_pipeline`]
//! translates that, not a control-flow choice between two successors --
//! there being none in the file this is translated from. Upstream's own
//! source values come from an unseeded `rand()`, which (with no `srand`
//! call) is itself deterministic from one process to the next; this crate
//! takes on no RNG dependency for one file's input data, so
//! [`next_source_value`] is a fixed-seed xorshift generator standing in for
//! that same property.
//!
//! **`PipelineinPipeline.Pipelines` drives its inner pipeline through
//! [`Executor::corun`].** Upstream's own pipe bodies build a fresh
//! `tf::Pipeline` and `tf::Taskflow` on every call and run them with
//! `executor.corun(taskflow)` before returning -- a worker that is itself
//! inside one of this executor's own tasks stays in the work-stealing loop
//! until the inner graph finishes, rather than being asked to `run` a graph
//! it cannot wait on without parking. [`run_sub_pipeline`] is that
//! translated directly: a [`Pipe`] callable is `Fn(&mut Pipeflow) + Send +
//! Sync`, handed no runtime handle at all, so the only way its body reaches
//! the executor running it is the `Arc<Executor>` it captures, exactly as
//! [`run_sub_pipeline`]'s own caller does. Upstream's outer pipeline is
//! `SPSS` -- serial, parallel, serial, serial, read off its own
//! `tf::PipeType` arguments rather than its stale `// SPPS` comment -- and
//! [`pipeline_in_pipeline`] keeps that order.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use rustflow::Executor;
use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
use rustflow::pipeline::scheduler::Pipeline;
use rustflow::prelude::*;

#[macro_use]
mod common;

/// Runs `body` with every other case in this suite held back:
/// `tests/pipelines.rs`'s own `serialized` helper, duplicated here because
/// each file under `tests/` is its own crate -- its module docs give the
/// full reasoning.
fn serialized<R>(body: impl FnOnce() -> R) -> R {
    static GATE: Mutex<()> = Mutex::new(());
    let _guard = GATE.lock().unwrap_or_else(PoisonError::into_inner);
    body()
}

/// A pipeline's `lines * pipes` scratch buffer: see `tests/pipelines.rs`'s
/// own copy for why this is a `Mutex` rather than `unsafe`. Used here for
/// both an outer pipeline's own buffer and, inside [`run_sub_pipeline`], an
/// inner one's -- the same shape either way.
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
}

// ----------------------------------------------------------------------------
// Ifelse.Pipelines: a three-pipe (SPS) pipeline whose pipes take a
// data-dependent if/else over each token's running value -- see this file's
// own `# Design` section for why this is not a condition task.
// ----------------------------------------------------------------------------

/// A fixed-seed 64-bit xorshift generator: this file's module docs say why
/// one is needed at all in place of upstream's unseeded `rand()`.
fn next_source_value(state: &mut u64) -> i32 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    let value = *state % 9962;
    i32::try_from(value).expect("a value under 9962 fits in an i32")
}

/// Upstream's `ifelse_pipe_ans(a)`: the same three branches
/// [`ifelse_pipeline`]'s own three pipes take, worked out independently on
/// one value for that function's own check.
fn ifelse_pipe_ans(value: i32) -> i32 {
    let mut a = value;
    if a / 2 != 0 {
        a += 8;
    }
    if a > 4897 {
        a -= 1834;
    } else {
        a += 3;
    }
    if (a + 9) / 4 < 50 {
        a += 1;
    } else {
        a += 17;
    }
    a
}

/// Upstream's `ifelse_pipeline(L, w)`: a serial pipe that stops after `N`
/// tokens and takes the first branch on each one's source value, a parallel
/// pipe that takes the second branch on what the first wrote, and a serial
/// pipe that takes the third and collects the result -- tokens reach this
/// last pipe in order, so [`ifelse_pipe_ans`] is checked against the
/// collected values by index, not as a sorted set.
fn ifelse_pipeline(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        const MAX_N: usize = 200;

        let mut seed = 0x1234_5678_9abc_def1_u64;
        let source: Vec<i32> = (0..MAX_N).map(|_| next_source_value(&mut seed)).collect();
        let buffer = Buffer::new(num_lines, 3);

        for n in 1..MAX_N {
            let mut taskflow = Taskflow::new();
            let collection: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::with_capacity(n)));

            let pipe0 = {
                let source = source.clone();
                let buffer = buffer.clone();
                Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                    if pf.token() == n {
                        pf.stop();
                        return;
                    }
                    let value = source[pf.token()];
                    let written = if value / 2 == 0 { value } else { value + 8 };
                    buffer.set(pf.line(), pf.pipe(), written);
                })
            };

            let pipe1 = {
                let buffer = buffer.clone();
                Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
                    let previous = buffer.get(pf.line(), pf.pipe() - 1);
                    let written = if previous > 4897 {
                        previous - 1834
                    } else {
                        previous + 3
                    };
                    buffer.set(pf.line(), pf.pipe(), written);
                })
            };

            let pipe2 = {
                let buffer = buffer.clone();
                let collection = Arc::clone(&collection);
                Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                    let previous = buffer.get(pf.line(), pf.pipe() - 1);
                    let written = if (previous + 9) / 4 < 50 {
                        previous + 1
                    } else {
                        previous + 17
                    };
                    buffer.set(pf.line(), pf.pipe(), written);
                    collection.lock().expect("uncontended").push(written);
                })
            };

            let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1, pipe2]);
            let module = taskflow.composed_of(&pipeline);
            taskflow.task(module).name("module_of_pipeline");

            executor.run(&taskflow).wait().expect("no task panicked");

            let collected = collection.lock().expect("uncontended");
            assert_eq!(collected.len(), n);
            for (token, &value) in collected.iter().enumerate() {
                assert_eq!(value, ifelse_pipe_ans(source[token]));
            }
        }
    });
}

worker_matrix! {
    ifelse_pipeline {
        ifelse_pipelines_1l_1w(1, 1),
        ifelse_pipelines_1l_2w(1, 2),
        ifelse_pipelines_1l_3w(1, 3),
        ifelse_pipelines_1l_4w(1, 4),
        ifelse_pipelines_3l_1w(3, 1),
        ifelse_pipelines_3l_2w(3, 2),
        ifelse_pipelines_3l_3w(3, 3),
        ifelse_pipelines_3l_4w(3, 4),
        ifelse_pipelines_5l_1w(5, 1),
        ifelse_pipelines_5l_2w(5, 2),
        ifelse_pipelines_5l_3w(5, 3),
        ifelse_pipelines_5l_4w(5, 4),
        ifelse_pipelines_7l_1w(7, 1),
        ifelse_pipelines_7l_2w(7, 2),
        ifelse_pipelines_7l_3w(7, 3),
        ifelse_pipelines_7l_4w(7, 4),
    }
}

// ----------------------------------------------------------------------------
// PipelineinPipeline.Pipelines: an outer SPSS pipeline whose first three
// pipes each run a fresh SPS sub-pipeline to completion through
// `Executor::corun` before producing the value the next outer pipe checks.
// ----------------------------------------------------------------------------

/// `0 + 1 + ... + (sub_n - 1)`: upstream's
/// `std::accumulate(source[token].begin(), source[token].begin() + subN, 0)`,
/// independent of which outer token's row of `source` is read since every
/// row is upstream's own `std::iota(..., 0)` -- `0, 1, 2, ...` -- regardless
/// of which row.
fn partial_sum(sub_n: usize) -> i32 {
    let sub_n = i32::try_from(sub_n).expect("a sub-token count fits in an i32");
    sub_n * (sub_n - 1) / 2
}

/// Builds a fresh three-pipe (`SPS`) sub-pipeline of `sub_lines` lines over
/// `sub_n` tokens, runs it to completion through [`Executor::corun`], and
/// returns the sum of what its last pipe collected: upstream's own
/// sub-pipeline, its enclosing `tf::Taskflow`, and the
/// `std::accumulate(subcollection...)` its caller reads back, all in one
/// call since nothing outside this function's caller ever looks at the
/// sub-pipeline's own intermediate state.
///
/// `third_offset` is the one difference between upstream's three calls of
/// this shape, inlined there as a literal `+ 3`, `+ 13` or `+ 7` in the
/// third subpipe -- [`pipeline_in_pipeline`]'s own three outer pipes pass
/// the three values upstream's own three sub-pipelines use.
fn run_sub_pipeline(executor: &Executor, sub_lines: usize, sub_n: usize, third_offset: i32) -> i32 {
    let subj1 = Arc::new(AtomicUsize::new(0));
    let subj2 = Arc::new(AtomicUsize::new(0));
    let subj3 = Arc::new(AtomicUsize::new(0));
    let sub_buffer = Buffer::new(sub_lines, 3);
    let subcollection: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::with_capacity(sub_n)));

    let subpipe0 = {
        let subj1 = Arc::clone(&subj1);
        let sub_buffer = sub_buffer.clone();
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            let current = subj1.load(Ordering::Relaxed);
            if current == sub_n {
                pf.stop();
                return;
            }
            assert_eq!(pf.token() % sub_lines, pf.line());
            let value = i32::try_from(current + 1).expect("a sub-token count fits in an i32");
            sub_buffer.set(pf.line(), pf.pipe(), value);
            subj1.fetch_add(1, Ordering::Relaxed);
        })
    };

    let subpipe1 = {
        let subj2 = Arc::clone(&subj2);
        let sub_buffer = sub_buffer.clone();
        Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
            let ticket = subj2.fetch_add(1, Ordering::Relaxed);
            assert!(ticket < sub_n);
            assert_eq!(pf.token() % sub_lines, pf.line());
            let expected = i32::try_from(pf.token() + 1).expect("a sub-token count fits in an i32");
            let previous = sub_buffer.get(pf.line(), pf.pipe() - 1);
            assert_eq!(previous, expected);
            sub_buffer.set(pf.line(), pf.pipe(), previous);
        })
    };

    let subpipe2 = {
        let subj3 = Arc::clone(&subj3);
        let sub_buffer = sub_buffer.clone();
        let subcollection = Arc::clone(&subcollection);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            let current = subj3.load(Ordering::Relaxed);
            assert!(current < sub_n);
            assert_eq!(pf.token() % sub_lines, pf.line());
            let expected = i32::try_from(current + 1).expect("a sub-token count fits in an i32");
            let previous = sub_buffer.get(pf.line(), pf.pipe() - 1);
            assert_eq!(previous, expected);
            // Upstream writes `source[pf.token()][subj3] + ADD`, not
            // `previous + ADD`: `previous` is only this pipe's own
            // cross-check that the pipe before it wrote what the running
            // index says it should have, and `source[token][k] == k` for
            // every row -- see `partial_sum`'s own doc for why.
            let value =
                i32::try_from(current).expect("a sub-token count fits in an i32") + third_offset;
            sub_buffer.set(pf.line(), pf.pipe(), value);
            subcollection.lock().expect("uncontended").push(value);
            subj3.fetch_add(1, Ordering::Relaxed);
        })
    };

    let subpipeline = Pipeline::new(sub_lines, vec![subpipe0, subpipe1, subpipe2]);
    let mut inner = Taskflow::new();
    let module = inner.composed_of(&subpipeline);
    inner.task(module).name("module_of_subpipeline");

    executor
        .corun(&inner)
        .expect("a worker of this executor may corun the sub-pipeline");

    assert_eq!(subj1.load(Ordering::Relaxed), sub_n);
    assert_eq!(subj2.load(Ordering::Relaxed), sub_n);
    assert_eq!(subj3.load(Ordering::Relaxed), sub_n);
    let collected = subcollection.lock().expect("uncontended");
    assert_eq!(collected.len(), sub_n);
    collected.iter().sum()
}

/// Upstream's `pipeline_in_pipeline(L, w, subL)`: an outer `SPSS` pipeline
/// whose first three pipes each hand `sub_n` tokens to [`run_sub_pipeline`]
/// with the `+3`, `+13` and `+7` offsets upstream's own three sub-pipelines
/// use, and whose fourth checks the last one's output without running a
/// sub-pipeline of its own.
fn pipeline_in_pipeline(num_lines: usize, num_workers: usize, sub_lines: usize) {
    serialized(|| {
        let executor = Arc::new(Executor::new(num_workers));

        const MAX_N: usize = 5;
        const MAX_SUB_N: usize = 4;

        for n in 1..MAX_N {
            for sub_n in 1..MAX_SUB_N {
                let mut taskflow = Taskflow::new();

                let j1 = Arc::new(AtomicUsize::new(0));
                let j2 = Arc::new(AtomicUsize::new(0));
                let j3 = Arc::new(AtomicUsize::new(0));
                let j4 = Arc::new(AtomicUsize::new(0));
                let buffer = Buffer::new(num_lines, 4);

                let pipe0 = {
                    let executor = Arc::clone(&executor);
                    let j1 = Arc::clone(&j1);
                    let buffer = buffer.clone();
                    Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                        let current = j1.load(Ordering::Relaxed);
                        if current == n {
                            pf.stop();
                            return;
                        }
                        let sum = run_sub_pipeline(&executor, sub_lines, sub_n, 3);
                        buffer.set(pf.line(), pf.pipe(), sum);
                        j1.fetch_add(1, Ordering::Relaxed);
                    })
                };

                let pipe1 = {
                    let executor = Arc::clone(&executor);
                    let j2 = Arc::clone(&j2);
                    let buffer = buffer.clone();
                    Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
                        let ticket = j2.fetch_add(1, Ordering::Relaxed);
                        assert!(ticket < n);
                        let offset =
                            i32::try_from(sub_n).expect("a sub-token count fits in an i32");
                        let expected = partial_sum(sub_n) + 3 * offset;
                        assert_eq!(buffer.get(pf.line(), pf.pipe() - 1), expected);
                        let sum = run_sub_pipeline(&executor, sub_lines, sub_n, 13);
                        buffer.set(pf.line(), pf.pipe(), sum);
                    })
                };

                let pipe2 = {
                    let executor = Arc::clone(&executor);
                    let j3 = Arc::clone(&j3);
                    let buffer = buffer.clone();
                    Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                        let ticket = j3.fetch_add(1, Ordering::Relaxed);
                        assert!(ticket < n);
                        let offset =
                            i32::try_from(sub_n).expect("a sub-token count fits in an i32");
                        let expected = partial_sum(sub_n) + 13 * offset;
                        assert_eq!(buffer.get(pf.line(), pf.pipe() - 1), expected);
                        let sum = run_sub_pipeline(&executor, sub_lines, sub_n, 7);
                        buffer.set(pf.line(), pf.pipe(), sum);
                    })
                };

                let pipe3 = {
                    let j4 = Arc::clone(&j4);
                    let buffer = buffer.clone();
                    Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                        let offset =
                            i32::try_from(sub_n).expect("a sub-token count fits in an i32");
                        let expected = partial_sum(sub_n) + 7 * offset;
                        assert_eq!(buffer.get(pf.line(), pf.pipe() - 1), expected);
                        j4.fetch_add(1, Ordering::Relaxed);
                    })
                };

                let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1, pipe2, pipe3]);
                let module = taskflow.composed_of(&pipeline);
                taskflow.task(module).name("module_of_pipeline");

                executor.run(&taskflow).wait().expect("no task panicked");

                assert_eq!(j1.load(Ordering::Relaxed), n);
                assert_eq!(j4.load(Ordering::Relaxed), n);
            }
        }
    });
}

worker_matrix! {
    pipeline_in_pipeline {
        pipeline_in_pipeline_1l_1w_1subl(1, 1, 1),
        pipeline_in_pipeline_1l_1w_3subl(1, 1, 3),
        pipeline_in_pipeline_1l_1w_4subl(1, 1, 4),
        pipeline_in_pipeline_1l_2w_1subl(1, 2, 1),
        pipeline_in_pipeline_1l_2w_3subl(1, 2, 3),
        pipeline_in_pipeline_1l_2w_4subl(1, 2, 4),
        pipeline_in_pipeline_3l_1w_1subl(3, 1, 1),
        pipeline_in_pipeline_3l_1w_3subl(3, 1, 3),
        pipeline_in_pipeline_3l_1w_4subl(3, 1, 4),
        pipeline_in_pipeline_3l_2w_1subl(3, 2, 1),
        pipeline_in_pipeline_3l_2w_3subl(3, 2, 3),
        pipeline_in_pipeline_3l_2w_4subl(3, 2, 4),
        pipeline_in_pipeline_5l_1w_1subl(5, 1, 1),
        pipeline_in_pipeline_5l_1w_3subl(5, 1, 3),
        pipeline_in_pipeline_5l_1w_4subl(5, 1, 4),
        pipeline_in_pipeline_5l_2w_1subl(5, 2, 1),
        pipeline_in_pipeline_5l_2w_3subl(5, 2, 3),
        pipeline_in_pipeline_5l_2w_4subl(5, 2, 4),
    }
}

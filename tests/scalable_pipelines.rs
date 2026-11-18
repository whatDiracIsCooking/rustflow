//! Translated from `unittests/test_scalable_pipelines.cpp` (Taskflow v4.1.0).
//!
//! Upstream's `tf::ScalablePipeline` is this port's own [`Pipeline`] --
//! `DESIGN.md`'s "Pipelines" section says why one type stands
//! for both of upstream's templates -- so what this file actually exercises
//! is [`Pipeline::reset_pipes`] and [`Pipeline::reset_lines`]: `Basics`,
//! `Reset`, `LinesReset`, `IterativeReset`, and then four matrices that
//! build and reset real pipelines through nested compositions --
//! `Ifelse` (16), `PipelineinPipeline` (18), `SNIG` (12) and
//! `Subflow.{1,3,5,7}R` (24).
//!
//! Two cases at the end run `Reset` and `LinesReset` through a
//! [`DataPipeline`] instead, for [`DataPipeline::reset_pipes`] and
//! [`DataPipeline::reset_lines`]; their own section comment says what
//! upstream does and does not have to translate there.
//!
//! `ScalablePipeline.move` is not ported: there is nothing for a move test
//! to prove once a [`Pipeline`] is an ordinary owned value, as `DESIGN.md`
//! says.
//!
//! Four adaptations, every one forced by the port rather than chosen:
//!
//! - **Every piece of state a pipe callable closes over is shared through
//!   an `Arc`, not captured by reference**, for the reason
//!   `tests/pipelines.rs`'s own module doc gives: a [`Pipe`] callable is
//!   `Fn + Send + Sync + 'static`, where upstream's `mutable` lambda
//!   captures and mutates its counters and buffers by reference.
//! - **A reset always runs before the one `composed_of` of a given
//!   pipeline, never after.** [`Pipeline::reset_pipes`] and
//!   [`Pipeline::reset_lines`] rebuild the whole of the taskflow a
//!   [`Pipeline`] composes into, rather than mutating the one already
//!   built -- `src/pipeline/scheduler.rs`'s own `# Design` section has why
//!   -- so composing first and resetting second, which upstream's own
//!   aliasing lets a handful of its cases do from inside a task that runs
//!   before the pipeline's first use, has no translation here. Every call
//!   site in upstream's own test file resets before its one `composed_of`
//!   regardless, so this is not a loss against how upstream is actually
//!   used, only against a handful of cases that reset from *inside* an
//!   already-composed graph's own init task; those are rewritten to reset
//!   before building the graph that composes the result, which is the same
//!   state by the time either composes. Upstream's own case that mutates a
//!   pipe vector *after* constructing the pipeline over it (`ScalablePipeline.Reset`'s
//!   `init` task) accordingly becomes "build the new `Vec<Pipe>` and call
//!   [`Pipeline::reset_pipes`] with it", not a mutation of anything already
//!   handed to [`Pipeline::new`].
//! - **A pipeline always starts from one throwaway pipe.** This port has no
//!   default-constructed, zero-pipe [`Pipeline`] the way
//!   `tf::ScalablePipeline`'s default constructor gives upstream one --
//!   [`Pipeline::new`] always needs at least one pipe, upstream's own
//!   `static_assert` turned into a runtime check. Every case below that
//!   wants to show a reset form building a pipeline's *real* shape from
//!   nothing therefore starts with `Pipeline::new(1, vec![dummy_pipe()])`
//!   and calls [`Pipeline::reset_pipes`] or [`Pipeline::reset_lines`]
//!   immediately, never running the throwaway shape.
//! - **No in-graph "test" task, and `SNIG`'s own scale is cut down.** Every
//!   assertion below runs in the test function itself, directly after
//!   [`RunHandle::wait`](rustflow::RunHandle::wait) or
//!   [`Executor::corun`](rustflow::Executor::corun) returns, for the reason
//!   `tests/pipelines.rs`'s own module doc gives for the non-scalable
//!   family. `SNIG`'s device-count sweep (upstream's `{1, 2, 4, 6, 9, 13,
//!   17}` on top of its own 12-case `L`/`w` matrix) is one fixed count of
//!   three devices instead: three independent pipelines racing one shared
//!   counter is the thing "the multi-pipeline example" names, and a sweep
//!   over how many of them race is not part of what reset forms this issue
//!   is about.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use rustflow::Executor;
use rustflow::pipeline::data_pipeline::{
    DataPipeline, DataPipelineBuilder, serial_pipe, serial_pipe_with_flow,
};
use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
use rustflow::pipeline::scheduler::Pipeline;
use rustflow::prelude::*;

#[macro_use]
mod common;
#[path = "common/compose.rs"]
mod compose;
// Only the gate and the composition scaffold: this suite's own cases spin up several multi-worker
// `Executor`s each, and `cargo test`'s default harness runs many `#[test]`s
// at once.
#[path = "common/serialized.rs"]
mod serialized;

use compose::compose_module;
use serialized::serialized;

/// A single serial pipe that does nothing: the throwaway shape every case
/// below builds a [`Pipeline`] over before its first [`Pipeline::reset_pipes`]
/// or [`Pipeline::reset_lines`] call, standing in for the zero-pipe pipeline
/// upstream's default constructor gives it -- this module's own doc says why
/// there is no equivalent here.
fn dummy_pipe() -> Pipe {
    Pipe::new(PipeType::Serial, |_: &mut Pipeflow| {})
}

// ----------------------------------------------------------------------------
// ScalablePipeline.Basics
// ----------------------------------------------------------------------------

/// Upstream's `ScalablePipeline.Basics`, minus the move-constructor and
/// move-assignment checks this module's own doc says are not ported:
/// [`Pipeline::reset_lines`] from the one throwaway pipe every case starts
/// from reports back the shape it was just given.
#[test]
fn basics() {
    let num_pipes = 10;
    let pipes: Vec<Pipe> = (0..num_pipes).map(|_| dummy_pipe()).collect();

    let mut pipeline = Pipeline::new(1, vec![dummy_pipe()]);
    assert_eq!(pipeline.num_lines(), 1);
    assert_eq!(pipeline.num_pipes(), 1);
    assert_eq!(pipeline.num_tokens(), 0);

    pipeline.reset_lines(1, pipes);
    assert_eq!(pipeline.num_lines(), 1);
    assert_eq!(pipeline.num_pipes(), num_pipes);
    assert_eq!(pipeline.num_tokens(), 0);
}

// ----------------------------------------------------------------------------
// ScalablePipeline.Reset / IterativeReset / LinesReset
//
// All three share one pipe chain: a serial chain that stops pipe 0 at
// `STOP_AT` tokens and otherwise, at every pipe, checks that each line's own
// running total is exactly `token * num_pipes + pipe` -- upstream's own
// check in `scalable_pipeline_reset` -- and counts how many tokens reached
// the last pipe.
// ----------------------------------------------------------------------------

/// Upstream's hard-coded token count every one of `Reset`, `IterativeReset`
/// and `LinesReset`'s pipe chains stops at.
const STOP_AT: usize = 1111;

/// Builds one pipe of a `num_pipes`-long serial chain, every instance
/// running the same body and differing only in what [`Pipeflow::pipe`]
/// reports at run time -- upstream builds its chain the same way, one
/// `emplace_back` of the same lambda per pipe.
fn counting_pipe(
    num_pipes: usize,
    data: Arc<Vec<AtomicUsize>>,
    reached_last: Arc<AtomicUsize>,
) -> Pipe {
    Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
        if pf.pipe() == 0 {
            if pf.token() == STOP_AT {
                pf.stop();
                return;
            }
            data[pf.line()].store(num_pipes * pf.token(), Ordering::Relaxed);
        } else {
            data[pf.line()].fetch_add(1, Ordering::Relaxed);
        }
        assert_eq!(
            data[pf.line()].load(Ordering::Relaxed),
            pf.token() * num_pipes + pf.pipe()
        );
        if pf.pipe() == num_pipes - 1 {
            reached_last.fetch_add(1, Ordering::Relaxed);
        }
    })
}

/// Upstream's `scalable_pipeline_reset(num_lines, num_pipes)`: builds the
/// chain, [`Pipeline::reset_pipes`]es a throwaway pipeline over it, and
/// checks every one of `STOP_AT` tokens reached the last pipe.
fn scalable_pipeline_reset(num_lines: usize, num_pipes: usize) {
    let executor = Executor::new(2);
    let data: Arc<Vec<AtomicUsize>> =
        Arc::new((0..num_lines).map(|_| AtomicUsize::new(0)).collect());
    let reached_last = Arc::new(AtomicUsize::new(0));
    let pipes: Vec<Pipe> = (0..num_pipes)
        .map(|_| counting_pipe(num_pipes, Arc::clone(&data), Arc::clone(&reached_last)))
        .collect();

    let mut pipeline = Pipeline::new(num_lines, vec![dummy_pipe()]);
    pipeline.reset_pipes(pipes);

    let mut taskflow = Taskflow::new();
    compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

    executor.run(&taskflow).wait().expect("no task panicked");

    assert_eq!(reached_last.load(Ordering::Relaxed), STOP_AT);
    assert_eq!(pipeline.num_tokens(), STOP_AT);
}

#[test]
fn reset() {
    serialized(|| {
        for num_lines in 1..=10 {
            for num_pipes in 1..=10 {
                scalable_pipeline_reset(num_lines, num_pipes);
            }
        }
    });
}

/// Upstream's `scalable_pipeline_iterative_reset(num_lines, num_pipes)`:
/// [`Pipeline::reset_pipes`] once, then loop plain [`Pipeline::reset`]
/// between four runs of the same composed module, checking the cumulative
/// "reached the last pipe" count after each one.
fn scalable_pipeline_iterative_reset(num_lines: usize, num_pipes: usize) {
    let executor = Executor::new(2);
    let data: Arc<Vec<AtomicUsize>> =
        Arc::new((0..num_lines).map(|_| AtomicUsize::new(0)).collect());
    let reached_last = Arc::new(AtomicUsize::new(0));
    let pipes: Vec<Pipe> = (0..num_pipes)
        .map(|_| counting_pipe(num_pipes, Arc::clone(&data), Arc::clone(&reached_last)))
        .collect();

    let mut pipeline = Pipeline::new(num_lines, vec![dummy_pipe()]);
    pipeline.reset_pipes(pipes);
    let pipeline = Arc::new(pipeline);

    let mut taskflow = Taskflow::new();
    let module = compose_module(&mut taskflow, &*pipeline, "module_of_pipeline");

    let iteration = Arc::new(AtomicUsize::new(0));
    let cond = {
        let reached_last = Arc::clone(&reached_last);
        let pipeline = Arc::clone(&pipeline);
        let iteration = Arc::clone(&iteration);
        taskflow.emplace_condition(move || {
            let i = iteration.load(Ordering::Relaxed);
            assert_eq!(reached_last.load(Ordering::Relaxed), STOP_AT * (i + 1));
            pipeline.reset();
            iteration.store(i + 1, Ordering::Relaxed);
            if i < 3 { 0 } else { -1 }
        })
    };
    taskflow.task(cond).name("cond");
    taskflow.task(module).precede([cond]);
    taskflow.task(cond).precede([module]);

    executor.run(&taskflow).wait().expect("no task panicked");
}

#[test]
fn iterative_reset() {
    serialized(|| {
        for num_lines in 1..=10 {
            for num_pipes in 1..=10 {
                scalable_pipeline_iterative_reset(num_lines, num_pipes);
            }
        }
    });
}

/// Upstream's `scalable_pipeline_lines_reset(num_lines, num_pipes)`: for
/// every `l` up to `num_lines`, build a fresh outer taskflow,
/// [`Pipeline::reset_lines`] a throwaway pipeline to `l` lines over a fresh
/// chain, compose and run it. Every run processes exactly [`STOP_AT`]
/// tokens regardless of `l` -- the stop condition is on the token count, not
/// on any one line -- so `reached_last` is upstream's own `N`, never reset
/// between values of `l`: after the `l`'th run it has counted `l * STOP_AT`
/// tokens in total, which is upstream's own `REQUIRE(N == 1111 * l)`.
fn scalable_pipeline_lines_reset(num_lines: usize, num_pipes: usize) {
    let executor = Executor::new(2);
    let reached_last = Arc::new(AtomicUsize::new(0));

    for l in 1..=num_lines {
        let data: Arc<Vec<AtomicUsize>> = Arc::new((0..l).map(|_| AtomicUsize::new(0)).collect());
        let pipes: Vec<Pipe> = (0..num_pipes)
            .map(|_| counting_pipe(num_pipes, Arc::clone(&data), Arc::clone(&reached_last)))
            .collect();

        let mut pipeline = Pipeline::new(1, vec![dummy_pipe()]);
        pipeline.reset_lines(l, pipes);

        let mut taskflow = Taskflow::new();
        compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

        executor.run(&taskflow).wait().expect("no task panicked");

        assert_eq!(reached_last.load(Ordering::Relaxed), STOP_AT * l);
    }
}

#[test]
fn lines_reset() {
    serialized(|| {
        for num_pipes in 1..=10 {
            scalable_pipeline_lines_reset(10, num_pipes);
        }
    });
}

// ----------------------------------------------------------------------------
// The `DataPipeline` shape of Reset / LinesReset
//
// Upstream has no scalable `tf::DataPipeline` and no reset case for one --
// its `DataPipeline` exposes `reset()` alone -- so these two cases are
// `ScalablePipeline.Reset` and `ScalablePipeline.LinesReset` run through
// this port's own [`DataPipeline::reset_pipes`] and
// [`DataPipeline::reset_lines`], one case per method. The chain is a fixed
// three pipes rather than upstream's sweep over `num_pipes`: a typed chain's
// length is its type, so there is no loop that builds one of a runtime
// length.
// ----------------------------------------------------------------------------

/// The length of [`counting_chain`], which upstream's own `num_pipes`
/// parameter stands for.
const DATA_PIPES: usize = 3;

/// The typed form of [`counting_pipe`]'s chain: the running total upstream
/// keeps in `data[pf.line()]` is the value the slots carry from one pipe to
/// the next, and every pipe past the first checks it is exactly
/// `token * DATA_PIPES + pipe` before passing it on. Pipe `0`'s own copy of
/// that check is vacuous once the total is a value rather than a cell -- it
/// stores `DATA_PIPES * token` and would compare it against itself -- so
/// only the two that add to it carry one.
fn counting_chain(
    builder: DataPipelineBuilder<()>,
    reached_last: Arc<AtomicUsize>,
) -> DataPipelineBuilder<()> {
    builder
        .pipe(serial_pipe_with_flow::<(), usize>(|(), pf| {
            if pf.token() == STOP_AT {
                pf.stop();
                return 0;
            }
            DATA_PIPES * pf.token()
        }))
        .pipe(serial_pipe_with_flow::<usize, usize>(|total, pf| {
            let total = total + 1;
            assert_eq!(total, pf.token() * DATA_PIPES + pf.pipe());
            total
        }))
        .pipe(serial_pipe_with_flow::<usize, ()>(move |total, pf| {
            let total = total + 1;
            assert_eq!(total, pf.token() * DATA_PIPES + pf.pipe());
            reached_last.fetch_add(1, Ordering::Relaxed);
        }))
}

/// [`dummy_pipe`]'s typed counterpart: the throwaway one-pipe shape a
/// `DataPipeline` starts from, for the reason this module's own doc gives
/// for the untyped one.
fn dummy_data_pipeline(num_lines: usize) -> DataPipeline {
    DataPipeline::builder(num_lines)
        .pipe(serial_pipe::<(), ()>(|()| {}))
        .build()
}

/// `ScalablePipeline.Reset` through a [`DataPipeline`]: for every line count
/// up to ten, [`DataPipeline::reset_pipes`] a throwaway pipeline onto the
/// typed counting chain, compose and run it, and check all [`STOP_AT`]
/// tokens reached the last pipe while the line count stayed the one the
/// pipeline was built with.
#[test]
fn data_pipeline_reset_pipes() {
    serialized(|| {
        for num_lines in 1..=10 {
            let executor = Executor::new(2);
            let reached_last = Arc::new(AtomicUsize::new(0));

            let mut pipeline = dummy_data_pipeline(num_lines);
            pipeline.reset_pipes(|builder| counting_chain(builder, Arc::clone(&reached_last)));

            let mut taskflow = Taskflow::new();
            compose_module(&mut taskflow, &pipeline, "module_of_data_pipeline");

            executor.run(&taskflow).wait().expect("no task panicked");

            assert_eq!(pipeline.num_lines(), num_lines);
            assert_eq!(pipeline.num_pipes(), DATA_PIPES);
            assert_eq!(pipeline.num_tokens(), STOP_AT);
            assert_eq!(reached_last.load(Ordering::Relaxed), STOP_AT);
        }
    });
}

/// `ScalablePipeline.LinesReset` through a [`DataPipeline`]: one pipeline
/// object, [`DataPipeline::reset_lines`]ed to each line count in turn and
/// composed into a fresh outer taskflow after each reset. `reached_last` is
/// upstream's own `N`, never reset between iterations, so after the `l`'th
/// run it has counted `l * STOP_AT` tokens -- upstream's
/// `REQUIRE(N == 1111 * l)`.
#[test]
fn data_pipeline_lines_reset() {
    serialized(|| {
        let executor = Executor::new(2);
        let reached_last = Arc::new(AtomicUsize::new(0));
        let mut pipeline = dummy_data_pipeline(1);

        for l in 1..=10 {
            pipeline.reset_lines(l, |builder| {
                counting_chain(builder, Arc::clone(&reached_last))
            });

            let mut taskflow = Taskflow::new();
            compose_module(&mut taskflow, &pipeline, "module_of_data_pipeline");

            executor.run(&taskflow).wait().expect("no task panicked");

            assert_eq!(pipeline.num_lines(), l);
            assert_eq!(pipeline.num_tokens(), STOP_AT);
            assert_eq!(reached_last.load(Ordering::Relaxed), STOP_AT * l);
        }
    });
}

// ----------------------------------------------------------------------------
// ScalablePipeline.Ifelse: three pipes (serial, parallel, serial), L lines,
// W workers.
// ----------------------------------------------------------------------------

/// Upstream's `ifelse_spipe_ans`: the pure function every token's answer is
/// checked against, over a deterministic stand-in for upstream's
/// `rand() % 9962` source -- this crate admits no random-number dependency,
/// and the point of the case is the branches inside the pipeline, not the
/// distribution of its input.
fn ifelse_answer(source_value: i32) -> i32 {
    let value = if source_value / 2 == 0 {
        source_value
    } else {
        source_value + 8
    };
    let value = if value > 4897 {
        value - 1834
    } else {
        value + 3
    };
    if (value + 9) / 4 < 50 {
        value + 1
    } else {
        value + 17
    }
}

/// Upstream's three `ifelse_spipeline` pipes: a serial pipe that stops at
/// `n` tokens and writes the first branch's result, a parallel pipe that
/// reads it and writes the second branch's, and a serial pipe that reads
/// that and collects the third.
fn ifelse_pipes(
    n: usize,
    source: Arc<Vec<i32>>,
    buffer: Arc<Mutex<Vec<[i32; 2]>>>,
    collected: Arc<Mutex<Vec<i32>>>,
) -> Vec<Pipe> {
    let pipe0 = {
        let source = Arc::clone(&source);
        let buffer = Arc::clone(&buffer);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            if pf.token() == n {
                pf.stop();
                return;
            }
            let value = source[pf.token()];
            let value = if value / 2 == 0 { value } else { value + 8 };
            buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()][0] = value;
        })
    };
    let pipe1 = {
        let buffer = Arc::clone(&buffer);
        Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
            let previous = buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()][0];
            let value = if previous > 4897 {
                previous - 1834
            } else {
                previous + 3
            };
            buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()][1] = value;
        })
    };
    let pipe2 = {
        let buffer = Arc::clone(&buffer);
        let collected = Arc::clone(&collected);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            let previous = buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()][1];
            let value = if (previous + 9) / 4 < 50 {
                previous + 1
            } else {
                previous + 17
            };
            collected
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(value);
        })
    };
    vec![pipe0, pipe1, pipe2]
}

/// Upstream's `ifelse_spipeline(L, w)`: sweeps `n` from `1` to `199`,
/// rebuilding the chain and calling [`Pipeline::reset_pipes`] every time --
/// the line count stays fixed for the whole sweep, matching upstream's own
/// reuse of one `ScalablePipeline` object across every `n`.
fn ifelse_spipeline(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);
        const MAX_N: usize = 200;
        let source: Arc<Vec<i32>> = Arc::new(
            (0..MAX_N)
                .map(|i| i32::try_from((i * 37 + 3) % 9962).expect("source values fit in an i32"))
                .collect(),
        );

        let mut pipeline = Pipeline::new(num_lines, vec![dummy_pipe()]);

        for n in 1..MAX_N {
            let buffer = Arc::new(Mutex::new(vec![[0i32; 2]; num_lines]));
            let collected = Arc::new(Mutex::new(Vec::with_capacity(n)));
            let pipes = ifelse_pipes(n, Arc::clone(&source), buffer, Arc::clone(&collected));
            pipeline.reset_pipes(pipes);

            let mut taskflow = Taskflow::new();
            compose_module(&mut taskflow, &pipeline, "pipeline");

            executor.run(&taskflow).wait().expect("no task panicked");

            let collected = collected.lock().unwrap_or_else(PoisonError::into_inner);
            assert_eq!(collected.len(), n);
            for (index, &value) in collected.iter().enumerate() {
                assert_eq!(value, ifelse_answer(source[index]));
            }
        }
    });
}

worker_matrix! {
    ifelse_spipeline {
        ifelse_1l_1w(1, 1),
        ifelse_1l_2w(1, 2),
        ifelse_1l_3w(1, 3),
        ifelse_1l_4w(1, 4),
        ifelse_3l_1w(3, 1),
        ifelse_3l_2w(3, 2),
        ifelse_3l_3w(3, 3),
        ifelse_3l_4w(3, 4),
        ifelse_5l_1w(5, 1),
        ifelse_5l_2w(5, 2),
        ifelse_5l_3w(5, 3),
        ifelse_5l_4w(5, 4),
        ifelse_7l_1w(7, 1),
        ifelse_7l_2w(7, 2),
        ifelse_7l_3w(7, 3),
        ifelse_7l_4w(7, 4),
    }
}

// ----------------------------------------------------------------------------
// ScalablePipeline.PipelineinPipeline: a pipeline whose first three pipes
// each build, reset_lines and corun a fresh sub-pipeline, L lines, W
// workers, subL sub-lines.
// ----------------------------------------------------------------------------

/// What a nested sub-pipeline's own chain should sum to: `sub_n` tokens,
/// each `source[outer_token][k] + 1 + offset`, matching exactly what
/// [`nested_pipeline_result`]'s chain computes -- the pure check this
/// case's three outer pipes run against what the corun actually produced.
fn nested_pipeline_expected_sum(
    sub_n: usize,
    outer_token: usize,
    source: &[Vec<i32>],
    offset: i32,
) -> i32 {
    (0..sub_n)
        .map(|k| source[outer_token][k] + 1 + offset)
        .sum()
}

/// Builds, [`Pipeline::reset_lines`]es and [`Executor::corun`]s a fresh
/// sub-pipeline of `sub_l` lines over `sub_n` tokens: upstream's own
/// `tf::ScalablePipeline<...> subpl;` declared fresh inside each outer
/// pipe's callback, `subpl.reset(subL, ...)`, composed into a fresh
/// `tf::Taskflow` and run with `executor.corun(taskflow)` -- the same
/// `executor` the outer pipeline's own run is on, called from inside one of
/// its own workers, which is exactly [`Executor::corun`]'s contract.
///
/// Its three pipes are serial, parallel, serial: the first writes
/// `source[outer_token][token] + 1`, the second reads and passes it on
/// unchanged -- a parallel pipe gives no order among its own tokens, so this
/// one asserts nothing about what order it ran in, only that a later stage
/// still reads what an earlier one wrote -- and the third adds `offset` and
/// collects.
fn nested_pipeline_result(
    sub_l: usize,
    sub_n: usize,
    outer_token: usize,
    source: &Arc<Vec<Vec<i32>>>,
    offset: i32,
    executor: &Arc<Executor>,
) -> i32 {
    let buffer: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(vec![0; sub_l]));
    let collected: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::with_capacity(sub_n)));

    let subpipe0 = {
        let source = Arc::clone(source);
        let buffer = Arc::clone(&buffer);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            if pf.token() == sub_n {
                pf.stop();
                return;
            }
            let value = source[outer_token][pf.token()] + 1;
            buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()] = value;
        })
    };
    let subpipe1 = {
        let buffer = Arc::clone(&buffer);
        Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
            let value = buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()];
            buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()] = value;
        })
    };
    let subpipe2 = {
        let buffer = Arc::clone(&buffer);
        let collected = Arc::clone(&collected);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            let value = buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()] + offset;
            collected
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(value);
        })
    };

    let mut sub_pipeline = Pipeline::new(1, vec![dummy_pipe()]);
    sub_pipeline.reset_lines(sub_l, vec![subpipe0, subpipe1, subpipe2]);

    let mut sub_taskflow = Taskflow::new();
    compose_module(&mut sub_taskflow, &sub_pipeline, "module_of_subpipeline");

    executor
        .corun(&sub_taskflow)
        .expect("called from one of this executor's own workers");

    assert_eq!(sub_pipeline.num_tokens(), sub_n);
    let collected = collected.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(collected.len(), sub_n);
    collected.iter().sum()
}

/// Upstream's three nesting `spipeline_in_spipeline` pipes: a serial pipe
/// that stops at `n` tokens, a parallel pipe and a second serial pipe,
/// every one of the three running its own nested sub-pipeline over a
/// different `offset` and asserting the sum it got back against
/// [`nested_pipeline_expected_sum`].
fn pipeline_in_pipeline_pipes(
    n: usize,
    sub_l: usize,
    sub_n: usize,
    source: Arc<Vec<Vec<i32>>>,
    executor: Arc<Executor>,
) -> Vec<Pipe> {
    let token_counter = Arc::new(AtomicUsize::new(0));

    let mut pipes = Vec::with_capacity(3);
    for (index, offset) in [(0, 3), (1, 13), (2, 7)] {
        let source = Arc::clone(&source);
        let executor = Arc::clone(&executor);
        let token_counter = Arc::clone(&token_counter);
        let pipe_type = if index == 1 {
            PipeType::Parallel
        } else {
            PipeType::Serial
        };
        pipes.push(Pipe::new(pipe_type, move |pf: &mut Pipeflow| {
            if index == 0 {
                let current = token_counter.load(Ordering::Relaxed);
                if current == n {
                    pf.stop();
                    return;
                }
                token_counter.fetch_add(1, Ordering::Relaxed);
            }
            let value =
                nested_pipeline_result(sub_l, sub_n, pf.token(), &source, offset, &executor);
            assert_eq!(
                value,
                nested_pipeline_expected_sum(sub_n, pf.token(), &source, offset)
            );
        }));
    }
    pipes
}

/// Upstream's `spipeline_in_spipeline(L, w, subL)`: sweeps `N` and `subN`
/// over `1..7`, rebuilding and [`Pipeline::reset_lines`]ing both the outer
/// pipeline and (inside each of its first three pipes) a fresh nested one
/// for every combination.
fn spipeline_in_spipeline(num_lines: usize, num_workers: usize, sub_lines: usize) {
    serialized(|| {
        let executor = Arc::new(Executor::new(num_workers));
        const MAX_N: usize = 7;
        const MAX_SUB_N: usize = 7;
        let source: Arc<Vec<Vec<i32>>> = Arc::new(
            (0..MAX_N)
                .map(|_| {
                    (0..MAX_SUB_N)
                        .map(|k| i32::try_from(k).expect("fits"))
                        .collect()
                })
                .collect(),
        );

        for n in 1..MAX_N {
            for sub_n in 1..MAX_SUB_N {
                let pipes = pipeline_in_pipeline_pipes(
                    n,
                    sub_lines,
                    sub_n,
                    Arc::clone(&source),
                    Arc::clone(&executor),
                );

                let mut pipeline = Pipeline::new(1, vec![dummy_pipe()]);
                pipeline.reset_lines(num_lines, pipes);

                let mut taskflow = Taskflow::new();
                compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

                executor.run(&taskflow).wait().expect("no task panicked");
            }
        }
    });
}

worker_matrix! {
    spipeline_in_spipeline {
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

// ----------------------------------------------------------------------------
// ScalablePipeline.SNIG: three independent device pipelines (serial,
// parallel, serial, parallel, serial), each L lines, racing one shared
// counter over W workers.
// ----------------------------------------------------------------------------

/// How many independent device pipelines race the shared counter: this
/// module's own doc says why this is one fixed count rather than upstream's
/// own device-count sweep.
const NUM_DEVICES: usize = 3;

/// Upstream's `NUM_SOURCE`/`BATCH_SIZE`, cut down for a fast gate -- chosen
/// so that `NUM_SOURCE` is an exact multiple of `BATCH_SIZE`, which is what
/// keeps every batch a device grabs fully in bounds no matter how the three
/// devices interleave their `fetch_add`s, exactly as upstream's own
/// `70000`/`100` are.
const NUM_SOURCE: usize = 300;
const BATCH_SIZE: usize = 25;

/// Upstream's five `snig_spipeline` pipes for one device: serial, parallel,
/// serial, parallel, serial, each checking it reads exactly what the pipe
/// before it wrote and adding its own offset, collecting the final value at
/// the last pipe.
fn snig_device_pipes(
    num_lines: usize,
    source: Arc<Vec<i32>>,
    dev_begin: Arc<AtomicUsize>,
    collected: Arc<Mutex<Vec<i32>>>,
) -> Vec<Pipe> {
    let buffer: Arc<Mutex<Vec<[i32; 4]>>> = Arc::new(Mutex::new(vec![[0; 4]; num_lines]));

    let pipe0 = {
        let source = Arc::clone(&source);
        let dev_begin = Arc::clone(&dev_begin);
        let buffer = Arc::clone(&buffer);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            if pf.token() == BATCH_SIZE {
                pf.stop();
                return;
            }
            let value = source[dev_begin.load(Ordering::Relaxed) + pf.token()] + 1;
            buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()][0] = value;
        })
    };
    let pipe1 = {
        let buffer = Arc::clone(&buffer);
        Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
            let previous = buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()][0];
            buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()][1] = previous + 2;
        })
    };
    let pipe2 = {
        let buffer = Arc::clone(&buffer);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            let previous = buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()][1];
            buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()][2] = previous + 5;
        })
    };
    let pipe3 = {
        let buffer = Arc::clone(&buffer);
        Pipe::new(PipeType::Parallel, move |pf: &mut Pipeflow| {
            let previous = buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()][2];
            buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()][3] = previous + 1;
        })
    };
    let pipe4 = {
        let buffer = Arc::clone(&buffer);
        let collected = Arc::clone(&collected);
        Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            let previous = buffer.lock().unwrap_or_else(PoisonError::into_inner)[pf.line()][3];
            collected
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(previous + 2);
        })
    };
    vec![pipe0, pipe1, pipe2, pipe3, pipe4]
}

/// Upstream's `snig_spipeline(L, w)`: builds [`NUM_DEVICES`] device
/// pipelines, each [`Pipeline::reset_lines`]-built before it is composed,
/// and wires a `first_fetch`/`fetch` pair of condition tasks per device
/// around its module task, racing [`AtomicUsize::fetch_add`] on one shared
/// counter for the next batch exactly as upstream's own `finished` does.
fn snig_spipeline(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);
        let source: Arc<Vec<i32>> = Arc::new(
            (0..NUM_SOURCE)
                .map(|i| i32::try_from(i).expect("fits"))
                .collect(),
        );
        let finished = Arc::new(AtomicUsize::new(0));

        let mut taskflow = Taskflow::new();
        let start = taskflow.emplace(|| {});
        let end = taskflow.emplace(|| {});

        for _ in 0..NUM_DEVICES {
            let dev_begin = Arc::new(AtomicUsize::new(0));
            let collected: Arc<Mutex<Vec<i32>>> =
                Arc::new(Mutex::new(Vec::with_capacity(BATCH_SIZE)));
            let pipes = snig_device_pipes(
                num_lines,
                Arc::clone(&source),
                Arc::clone(&dev_begin),
                Arc::clone(&collected),
            );

            let mut pipeline = Pipeline::new(1, vec![dummy_pipe()]);
            pipeline.reset_lines(num_lines, pipes);
            let pipeline = Arc::new(pipeline);

            let dev_end = taskflow.emplace(|| {});

            let first_fetch = {
                let finished = Arc::clone(&finished);
                let dev_begin = Arc::clone(&dev_begin);
                taskflow.emplace_condition(move || {
                    let num = finished.fetch_add(BATCH_SIZE, Ordering::Relaxed);
                    dev_begin.store(num, Ordering::Relaxed);
                    i32::from(num >= NUM_SOURCE)
                })
            };

            let module = compose_module(&mut taskflow, &*pipeline, "pipeline");

            let fetch = {
                let finished = Arc::clone(&finished);
                let dev_begin = Arc::clone(&dev_begin);
                let pipeline = Arc::clone(&pipeline);
                let collected = Arc::clone(&collected);
                let source = Arc::clone(&source);
                taskflow.emplace_condition(move || {
                    let begin = dev_begin.load(Ordering::Relaxed);
                    {
                        let seen = collected.lock().unwrap_or_else(PoisonError::into_inner);
                        assert_eq!(seen.len(), BATCH_SIZE);
                        for (offset, &value) in seen.iter().enumerate() {
                            assert_eq!(value, source[begin + offset] + 11);
                        }
                    }
                    collected
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .clear();

                    let num = finished.fetch_add(BATCH_SIZE, Ordering::Relaxed);
                    dev_begin.store(num, Ordering::Relaxed);
                    pipeline.reset();
                    i32::from(num >= NUM_SOURCE)
                })
            };

            taskflow.task(start).precede([first_fetch]);
            taskflow.task(first_fetch).precede([module, dev_end]);
            taskflow.task(module).precede([fetch]);
            taskflow.task(fetch).precede([module, dev_end]);
            taskflow.task(dev_end).precede([end]);
        }

        executor.run(&taskflow).wait().expect("no task panicked");
    });
}

worker_matrix! {
    snig_spipeline {
        snig_1l_1w(1, 1),
        snig_1l_2w(1, 2),
        snig_1l_3w(1, 3),
        snig_3l_1w(3, 1),
        snig_3l_2w(3, 2),
        snig_3l_3w(3, 3),
        snig_5l_1w(5, 1),
        snig_5l_2w(5, 2),
        snig_5l_3w(5, 3),
        snig_7l_1w(7, 1),
        snig_7l_2w(7, 2),
        snig_7l_3w(7, 3),
    }
}

// ----------------------------------------------------------------------------
// ScalablePipeline.Subflow: a pipeline built and run recursively inside a
// subflow, R recursion levels, W workers, L lines.
// ----------------------------------------------------------------------------

/// Upstream's `maxN`/`NUM_PIPES` every `subflow_spipeline` recursion level
/// builds its chain over.
const SUBFLOW_MAX_N: usize = 1123;
const SUBFLOW_NUM_PIPES: usize = 5;

/// Builds one recursion level's chain: a serial chain over
/// [`SUBFLOW_NUM_PIPES`] pipes that stops at [`SUBFLOW_MAX_N`] tokens,
/// counting `completed` once when it does -- upstream's `++counter` --
/// and otherwise checking each line's running total is `token + pipe`,
/// upstream's own `source[token] + pf.pipe()` with `source` the identity
/// sequence `std::iota` builds.
fn subflow_pipes(num_lines: usize, completed: Arc<AtomicUsize>) -> Vec<Pipe> {
    let data: Arc<Vec<AtomicUsize>> =
        Arc::new((0..num_lines).map(|_| AtomicUsize::new(0)).collect());
    (0..SUBFLOW_NUM_PIPES)
        .map(|_| {
            let data = Arc::clone(&data);
            let completed = Arc::clone(&completed);
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                if pf.pipe() == 0 {
                    if pf.token() == SUBFLOW_MAX_N {
                        pf.stop();
                        completed.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                    data[pf.line()].store(pf.token(), Ordering::Relaxed);
                } else {
                    data[pf.line()].fetch_add(1, Ordering::Relaxed);
                }
                assert_eq!(
                    data[pf.line()].load(Ordering::Relaxed),
                    pf.token() + pf.pipe()
                );
            })
        })
        .collect()
}

/// Upstream's `spawn`: builds this recursion level's pipeline, composes it
/// into `subflow`, and -- unless this is the last level -- emplaces a
/// nested subflow task for the next level that precedes this level's own
/// module task, exactly as upstream's `spawn_t.precede(spl_t)` does.
///
/// The [`Pipeline`] built here is dropped at the end of this call without
/// anywhere to keep it: `Composable::taskflow` has already handed the
/// module task its own `Arc` share of the graph by then, which is what
/// keeps the run itself sound -- `src/pipeline/scheduler.rs`'s `# Design`
/// section is the longer version of why an `Arc` share outliving the value
/// that handed it out is exactly the shape this port is built on.
fn spawn_subflow_pipeline(
    subflow: &mut Subflow<'_>,
    num_lines: usize,
    num_recurs: usize,
    level: usize,
    completed: Arc<AtomicUsize>,
) {
    let pipes = subflow_pipes(num_lines, Arc::clone(&completed));

    let mut pipeline = Pipeline::new(1, vec![dummy_pipe()]);
    pipeline.reset_lines(num_lines, pipes);

    let module = compose_module(subflow, &pipeline, "module_of_pipeline");

    if level + 1 < num_recurs {
        let next = subflow.emplace_subflow(move |nested: &mut Subflow<'_>| {
            spawn_subflow_pipeline(
                nested,
                num_lines,
                num_recurs,
                level + 1,
                Arc::clone(&completed),
            );
        });
        subflow.task(next).precede([module]);
    }
}

/// Upstream's `subflow_spipeline(NUM_RECURS, w, L)`: one subflow task whose
/// body recurses [`spawn_subflow_pipeline`] `NUM_RECURS` deep, checking
/// every level's pipeline stopped exactly once.
fn subflow_spipeline(num_recurs: usize, num_workers: usize, num_lines: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);
        let completed = Arc::new(AtomicUsize::new(0));

        let mut taskflow = Taskflow::new();
        let spawner = {
            let completed = Arc::clone(&completed);
            taskflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
                spawn_subflow_pipeline(subflow, num_lines, num_recurs, 0, Arc::clone(&completed));
            })
        };
        taskflow.task(spawner).name("subflows");

        executor.run(&taskflow).wait().expect("no task panicked");

        assert_eq!(completed.load(Ordering::Relaxed), num_recurs);
    });
}

worker_matrix! {
    subflow_spipeline {
        subflow_1r_1w_1l(1, 1, 1),
        subflow_1r_1w_3l(1, 1, 3),
        subflow_1r_1w_4l(1, 1, 4),
        subflow_1r_2w_1l(1, 2, 1),
        subflow_1r_2w_3l(1, 2, 3),
        subflow_1r_2w_4l(1, 2, 4),
        subflow_3r_1w_1l(3, 1, 1),
        subflow_3r_1w_3l(3, 1, 3),
        subflow_3r_1w_4l(3, 1, 4),
        subflow_3r_2w_1l(3, 2, 1),
        subflow_3r_2w_3l(3, 2, 3),
        subflow_3r_2w_4l(3, 2, 4),
        subflow_5r_1w_1l(5, 1, 1),
        subflow_5r_1w_3l(5, 1, 3),
        subflow_5r_1w_4l(5, 1, 4),
        subflow_5r_2w_1l(5, 2, 1),
        subflow_5r_2w_3l(5, 2, 3),
        subflow_5r_2w_4l(5, 2, 4),
        subflow_7r_1w_1l(7, 1, 1),
        subflow_7r_1w_3l(7, 1, 3),
        subflow_7r_1w_4l(7, 1, 4),
        subflow_7r_2w_1l(7, 2, 1),
        subflow_7r_2w_3l(7, 2, 3),
        subflow_7r_2w_4l(7, 2, 4),
    }
}

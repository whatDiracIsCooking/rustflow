//! Translated from `unittests/test_pipelines.cpp` (Taskflow v4.1.0).
//!
//! The first three composition families of that file: `Three.Parallel.Pipelines`
//! (three independent pipelines composed into one taskflow), `Three.Concatenated.Pipelines`
//! (the same three chained by `precede`), and `Looping.Pipelines` (one
//! pipeline module behind a condition task that loops it) -- 192 cases over
//! lines 1..=8 and workers 1..=8, through [`worker_matrix!`]. `tests/pipelines.rs`
//! covers the structural families this file does not; sibling `tests/pipelines_nested.rs`
//! (#184) covers nesting.
//!
//! Every adaptation `tests/pipelines.rs`'s own docs explain applies here too
//! -- `Arc`s standing in for upstream's by-reference captures, and no
//! in-graph "test" task, with assertions run directly after
//! [`RunHandle::wait`](rustflow::RunHandle::wait) returns instead. Two more are
//! specific to this file:
//!
//! - **Neither family here ever calls [`Pipeline::reset`].** Upstream's own
//!   three source functions do not either: `Three.Parallel.Pipelines` and
//!   `Three.Concatenated.Pipelines` drive their three runs through
//!   `executor.run_n(taskflow, 3, callback)`, whose callback clears only the
//!   *test's own* counters, buffers and collections, never the pipeline's
//!   internal state -- so each pipeline's `num_tokens` keeps growing across
//!   the three runs, which is exactly what `cnt * n` in [`ThreePipelines::assert_round`]
//!   checks. `Looping.Pipelines` is the same story over a single run: its
//!   condition task loops the one pipeline module back on itself a hundred
//!   times, asserting and then clearing the test's own state before each
//!   round, while the pipeline's own `num_tokens` accumulates round over
//!   round -- `cnt` there is the running sum of every round's token count,
//!   not a per-round value. The thing this file's own per-round assertions
//!   pin is therefore that *this* local reset -- the one every one of these
//!   families performs on its own counters and collections, not on the
//!   pipeline -- actually clears what it claims to: a round whose previous
//!   leftover state survives shows up as a wrong second round, not a
//!   coincidentally-right final count.
//! - **The token sweep of the two `Three.*` families is subsampled.**
//!   Upstream sweeps every token count in `0..=maxN` there, and a round's
//!   work grows with the count, so those 101 counts were most of this
//!   suite's wall clock. What a count decides is how a round fills the
//!   lines -- fewer tokens than lines, exactly as many, enough to wrap
//!   round them and come back -- and a pipe's own behaviour cycles with
//!   `pf.token() % num_lines`, so every fill shape the 101 hold between
//!   them is settled by the counts up to two full passes over the widest
//!   line count this file sweeps. [`token_sweep`] is therefore every count
//!   up to 18 and then every tenth up to [`MAX_N`]: 28 of the 101,
//!   upstream's own largest among them. `Looping.Pipelines` keeps the whole
//!   of its own sweep, which is a different quantity -- the number of times
//!   its condition task loops the one pipeline module -- and costs well
//!   under a tenth of what the two families above did.

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
#[path = "common/looping.rs"]
mod looping;
#[path = "common/pipeline.rs"]
mod pipeline;
#[path = "common/serialized.rs"]
mod serialized;
#[path = "common/tokens.rs"]
mod tokens;

use compose::compose_module;
use looping::{advance_round, wire_loop};
use pipeline::{Buffer, collecting_pipe, counting_source, propagating_pipe};
use serialized::serialized;
// `MAX_N` is this file's round count as well as its token count: the
// condition task of `Looping.Pipelines` loops that many times.
use tokens::{MAX_N, assert_consecutive_from};

/// The token counts a round of the two three-pipeline families runs: every
/// count up to twice the widest line sweep (`2 * 8 + 2`), then every tenth
/// up to [`MAX_N`], which is the last count upstream sweeps.
///
/// The module docs above say why this is a subsample of upstream's own
/// `0..=maxN` where every other sweep in these suites is not.
fn token_sweep() -> impl Iterator<Item = usize> {
    (0..=18).chain((20..=MAX_N).step_by(10))
}

// ----------------------------------------------------------------------------
// Three.Parallel.Pipelines / Three.Concatenated.Pipelines: the three
// pipelines both families build are identical -- `SSSS`, `SSP` and `SP` --
// and only the wiring between their module tasks differs, so `ThreePipelines`
// is the one builder both `three_parallel_pipelines` and
// `three_concatenated_pipelines` call.
// ----------------------------------------------------------------------------

/// The three pipelines (`SSSS`, `SSP`, `SP`) both composition families
/// build, composed into `taskflow` fresh for one token count `n`.
///
/// Every pipe checks `pf.token() % num_lines == pf.line()`, a serial pipe's
/// defining property -- tokens reach it in order -- and each pipeline's own
/// last pipe either checks what the one before it just wrote (`SSSS`'s
/// fourth pipe, `SSP`'s and `SP`'s own serial second-to-last) or collects it
/// into a `Vec` a parallel pipe gives no promise about the order of
/// ([`assert_consecutive_from`] is the right check there, not a sequence
/// one).
struct ThreePipelines {
    pl1: Pipeline,
    pl2: Pipeline,
    pl3: Pipeline,
    module1: Task,
    module2: Task,
    module3: Task,
    j1_1: Arc<AtomicUsize>,
    j1_2: Arc<AtomicUsize>,
    j1_3: Arc<AtomicUsize>,
    j1_4: Arc<AtomicUsize>,
    j2_1: Arc<AtomicUsize>,
    j2_2: Arc<AtomicUsize>,
    j2_3: Arc<AtomicUsize>,
    j3_1: Arc<AtomicUsize>,
    j3_2: Arc<AtomicUsize>,
    buffer1: Buffer,
    buffer2: Buffer,
    buffer3: Buffer,
    collection2: Arc<Mutex<Vec<i32>>>,
    collection3: Arc<Mutex<Vec<i32>>>,
}

impl ThreePipelines {
    /// Builds and composes all three pipelines into `taskflow`, for a round
    /// that stops each pipeline's first pipe after `n` tokens.
    fn build(taskflow: &mut Taskflow, num_lines: usize, n: usize) -> Self {
        // Pipeline 1: SSSS. Three serial pipes each check what the one
        // before it wrote; the fourth checks only, writing nothing further.
        let j1_1 = Arc::new(AtomicUsize::new(0));
        let j1_2 = Arc::new(AtomicUsize::new(0));
        let j1_3 = Arc::new(AtomicUsize::new(0));
        let j1_4 = Arc::new(AtomicUsize::new(0));
        let buffer1 = Buffer::new(num_lines, 4);

        let pipe1_0 = counting_source(num_lines, move || n, &j1_1, &buffer1);
        let pipe1_1 = {
            let j1_2 = Arc::clone(&j1_2);
            let buffer1 = buffer1.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j1_2.load(Ordering::Relaxed);
                assert!(current < n);
                assert_eq!(pf.token() % num_lines, pf.line());
                let value = i32::try_from(current + 1).expect("a token count fits in an i32");
                assert_eq!(value, buffer1.get(pf.line(), pf.pipe() - 1));
                buffer1.set(pf.line(), pf.pipe(), value);
                j1_2.fetch_add(1, Ordering::Relaxed);
            })
        };
        let pipe1_2 = {
            let j1_3 = Arc::clone(&j1_3);
            let buffer1 = buffer1.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j1_3.load(Ordering::Relaxed);
                assert!(current < n);
                assert_eq!(pf.token() % num_lines, pf.line());
                let value = i32::try_from(current + 1).expect("a token count fits in an i32");
                assert_eq!(value, buffer1.get(pf.line(), pf.pipe() - 1));
                buffer1.set(pf.line(), pf.pipe(), value);
                j1_3.fetch_add(1, Ordering::Relaxed);
            })
        };
        let pipe1_3 = {
            let j1_4 = Arc::clone(&j1_4);
            let buffer1 = buffer1.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j1_4.load(Ordering::Relaxed);
                assert!(current < n);
                assert_eq!(pf.token() % num_lines, pf.line());
                let value = i32::try_from(current + 1).expect("a token count fits in an i32");
                assert_eq!(value, buffer1.get(pf.line(), pf.pipe() - 1));
                j1_4.fetch_add(1, Ordering::Relaxed);
            })
        };
        let pl1 = Pipeline::new(num_lines, vec![pipe1_0, pipe1_1, pipe1_2, pipe1_3]);
        let module1 = compose_module(taskflow, &pl1, "module_of_pipeline1");

        // Pipeline 2: SSP. Two serial pipes as pipeline 1's own first two,
        // then a parallel pipe that collects rather than checking a
        // line-local counter.
        let j2_1 = Arc::new(AtomicUsize::new(0));
        let j2_2 = Arc::new(AtomicUsize::new(0));
        let j2_3 = Arc::new(AtomicUsize::new(0));
        let buffer2 = Buffer::new(num_lines, 3);
        let collection2: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

        let pipe2_0 = counting_source(num_lines, move || n, &j2_1, &buffer2);
        let pipe2_1 = {
            let j2_2 = Arc::clone(&j2_2);
            let buffer2 = buffer2.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j2_2.load(Ordering::Relaxed);
                assert!(current < n);
                assert_eq!(pf.token() % num_lines, pf.line());
                let value = i32::try_from(current + 1).expect("a token count fits in an i32");
                assert_eq!(value, buffer2.get(pf.line(), pf.pipe() - 1));
                buffer2.set(pf.line(), pf.pipe(), value);
                j2_2.fetch_add(1, Ordering::Relaxed);
            })
        };
        let pipe2_2 = collecting_pipe(num_lines, move || n, &j2_3, &buffer2, &collection2);
        let pl2 = Pipeline::new(num_lines, vec![pipe2_0, pipe2_1, pipe2_2]);
        let module2 = compose_module(taskflow, &pl2, "module_of_pipeline2");

        // Pipeline 3: SP. One serial pipe as the other two's own first,
        // then a parallel pipe that collects.
        let j3_1 = Arc::new(AtomicUsize::new(0));
        let j3_2 = Arc::new(AtomicUsize::new(0));
        let buffer3 = Buffer::new(num_lines, 2);
        let collection3: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

        let pipe3_0 = counting_source(num_lines, move || n, &j3_1, &buffer3);
        let pipe3_1 = collecting_pipe(num_lines, move || n, &j3_2, &buffer3, &collection3);
        let pl3 = Pipeline::new(num_lines, vec![pipe3_0, pipe3_1]);
        let module3 = compose_module(taskflow, &pl3, "module_of_pipeline3");

        Self {
            pl1,
            pl2,
            pl3,
            module1,
            module2,
            module3,
            j1_1,
            j1_2,
            j1_3,
            j1_4,
            j2_1,
            j2_2,
            j2_3,
            j3_1,
            j3_2,
            buffer1,
            buffer2,
            buffer3,
            collection2,
            collection3,
        }
    }

    /// Asserts round `n`'s invariants, given the attempt number (`1`-based,
    /// since every one of the three pipelines reaches its `attempt`-th run
    /// in lockstep): `attempt * n` is upstream's own `cnt * N` check, sound
    /// only because none of the three is ever reset between runs.
    fn assert_round(&self, n: usize, attempt: usize) {
        assert_eq!(self.j1_1.load(Ordering::Relaxed), n);
        assert_eq!(self.j1_2.load(Ordering::Relaxed), n);
        assert_eq!(self.j1_3.load(Ordering::Relaxed), n);
        assert_eq!(self.j1_4.load(Ordering::Relaxed), n);
        assert_eq!(self.pl1.num_tokens(), attempt * n);

        assert_eq!(self.j2_1.load(Ordering::Relaxed), n);
        assert_eq!(self.j2_2.load(Ordering::Relaxed), n);
        assert_eq!(self.j2_3.load(Ordering::Relaxed), n);
        assert_consecutive_from(&self.collection2, 1, n);
        assert_eq!(self.pl2.num_tokens(), attempt * n);

        assert_eq!(self.j3_1.load(Ordering::Relaxed), n);
        assert_eq!(self.j3_2.load(Ordering::Relaxed), n);
        assert_consecutive_from(&self.collection3, 1, n);
        assert_eq!(self.pl3.num_tokens(), attempt * n);
    }

    /// Clears every pipeline's own local counters, buffers and collections
    /// between runs -- never a pipeline's own `num_tokens`, which this
    /// file's module docs say is deliberate.
    fn reset_round(&self) {
        self.j1_1.store(0, Ordering::Relaxed);
        self.j1_2.store(0, Ordering::Relaxed);
        self.j1_3.store(0, Ordering::Relaxed);
        self.j1_4.store(0, Ordering::Relaxed);
        self.buffer1.clear();

        self.j2_1.store(0, Ordering::Relaxed);
        self.j2_2.store(0, Ordering::Relaxed);
        self.j2_3.store(0, Ordering::Relaxed);
        self.buffer2.clear();
        self.collection2.lock().expect("uncontended").clear();

        self.j3_1.store(0, Ordering::Relaxed);
        self.j3_2.store(0, Ordering::Relaxed);
        self.buffer3.clear();
        self.collection3.lock().expect("uncontended").clear();
    }
}

/// Upstream's `three_parallel_pipelines(L, w)`: the same three pipelines as
/// [`three_concatenated_pipelines`], wired as three independent subgraphs of
/// one taskflow rather than a chain.
fn three_parallel_pipelines(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in token_sweep() {
            let mut taskflow = Taskflow::new();
            let pipelines = ThreePipelines::build(&mut taskflow, num_lines, n);

            let initial = taskflow.emplace(|| {});
            taskflow.task(initial).name("initial").precede([
                pipelines.module1,
                pipelines.module2,
                pipelines.module3,
            ]);
            let terminal = taskflow.emplace(|| {});
            taskflow.task(terminal).name("terminal").succeed([
                pipelines.module1,
                pipelines.module2,
                pipelines.module3,
            ]);

            for attempt in 1..=3 {
                executor.run(&taskflow).wait().expect("no task panicked");
                pipelines.assert_round(n, attempt);
                pipelines.reset_round();
            }
        }
    });
}

worker_matrix! {
    three_parallel_pipelines {
        three_parallel_pipelines_1l_1w(1, 1),
        three_parallel_pipelines_1l_2w(1, 2),
        three_parallel_pipelines_1l_3w(1, 3),
        three_parallel_pipelines_1l_4w(1, 4),
        three_parallel_pipelines_1l_5w(1, 5),
        three_parallel_pipelines_1l_6w(1, 6),
        three_parallel_pipelines_1l_7w(1, 7),
        three_parallel_pipelines_1l_8w(1, 8),
        three_parallel_pipelines_2l_1w(2, 1),
        three_parallel_pipelines_2l_2w(2, 2),
        three_parallel_pipelines_2l_3w(2, 3),
        three_parallel_pipelines_2l_4w(2, 4),
        three_parallel_pipelines_2l_5w(2, 5),
        three_parallel_pipelines_2l_6w(2, 6),
        three_parallel_pipelines_2l_7w(2, 7),
        three_parallel_pipelines_2l_8w(2, 8),
        three_parallel_pipelines_3l_1w(3, 1),
        three_parallel_pipelines_3l_2w(3, 2),
        three_parallel_pipelines_3l_3w(3, 3),
        three_parallel_pipelines_3l_4w(3, 4),
        three_parallel_pipelines_3l_5w(3, 5),
        three_parallel_pipelines_3l_6w(3, 6),
        three_parallel_pipelines_3l_7w(3, 7),
        three_parallel_pipelines_3l_8w(3, 8),
        three_parallel_pipelines_4l_1w(4, 1),
        three_parallel_pipelines_4l_2w(4, 2),
        three_parallel_pipelines_4l_3w(4, 3),
        three_parallel_pipelines_4l_4w(4, 4),
        three_parallel_pipelines_4l_5w(4, 5),
        three_parallel_pipelines_4l_6w(4, 6),
        three_parallel_pipelines_4l_7w(4, 7),
        three_parallel_pipelines_4l_8w(4, 8),
        three_parallel_pipelines_5l_1w(5, 1),
        three_parallel_pipelines_5l_2w(5, 2),
        three_parallel_pipelines_5l_3w(5, 3),
        three_parallel_pipelines_5l_4w(5, 4),
        three_parallel_pipelines_5l_5w(5, 5),
        three_parallel_pipelines_5l_6w(5, 6),
        three_parallel_pipelines_5l_7w(5, 7),
        three_parallel_pipelines_5l_8w(5, 8),
        three_parallel_pipelines_6l_1w(6, 1),
        three_parallel_pipelines_6l_2w(6, 2),
        three_parallel_pipelines_6l_3w(6, 3),
        three_parallel_pipelines_6l_4w(6, 4),
        three_parallel_pipelines_6l_5w(6, 5),
        three_parallel_pipelines_6l_6w(6, 6),
        three_parallel_pipelines_6l_7w(6, 7),
        three_parallel_pipelines_6l_8w(6, 8),
        three_parallel_pipelines_7l_1w(7, 1),
        three_parallel_pipelines_7l_2w(7, 2),
        three_parallel_pipelines_7l_3w(7, 3),
        three_parallel_pipelines_7l_4w(7, 4),
        three_parallel_pipelines_7l_5w(7, 5),
        three_parallel_pipelines_7l_6w(7, 6),
        three_parallel_pipelines_7l_7w(7, 7),
        three_parallel_pipelines_7l_8w(7, 8),
        three_parallel_pipelines_8l_1w(8, 1),
        three_parallel_pipelines_8l_2w(8, 2),
        three_parallel_pipelines_8l_3w(8, 3),
        three_parallel_pipelines_8l_4w(8, 4),
        three_parallel_pipelines_8l_5w(8, 5),
        three_parallel_pipelines_8l_6w(8, 6),
        three_parallel_pipelines_8l_7w(8, 7),
        three_parallel_pipelines_8l_8w(8, 8),
    }
}

/// Upstream's `three_concatenated_pipelines(L, w)`: the same three pipelines
/// as [`three_parallel_pipelines`], chained `module1.precede(module2)`,
/// `module2.precede(module3)` rather than run independently.
fn three_concatenated_pipelines(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in token_sweep() {
            let mut taskflow = Taskflow::new();
            let pipelines = ThreePipelines::build(&mut taskflow, num_lines, n);

            let initial = taskflow.emplace(|| {});
            taskflow
                .task(initial)
                .name("initial")
                .precede([pipelines.module1]);
            taskflow
                .task(pipelines.module1)
                .precede([pipelines.module2]);
            taskflow
                .task(pipelines.module2)
                .precede([pipelines.module3]);
            let terminal = taskflow.emplace(|| {});
            taskflow
                .task(terminal)
                .name("terminal")
                .succeed([pipelines.module3]);

            for attempt in 1..=3 {
                executor.run(&taskflow).wait().expect("no task panicked");
                pipelines.assert_round(n, attempt);
                pipelines.reset_round();
            }
        }
    });
}

worker_matrix! {
    three_concatenated_pipelines {
        three_concatenated_pipelines_1l_1w(1, 1),
        three_concatenated_pipelines_1l_2w(1, 2),
        three_concatenated_pipelines_1l_3w(1, 3),
        three_concatenated_pipelines_1l_4w(1, 4),
        three_concatenated_pipelines_1l_5w(1, 5),
        three_concatenated_pipelines_1l_6w(1, 6),
        three_concatenated_pipelines_1l_7w(1, 7),
        three_concatenated_pipelines_1l_8w(1, 8),
        three_concatenated_pipelines_2l_1w(2, 1),
        three_concatenated_pipelines_2l_2w(2, 2),
        three_concatenated_pipelines_2l_3w(2, 3),
        three_concatenated_pipelines_2l_4w(2, 4),
        three_concatenated_pipelines_2l_5w(2, 5),
        three_concatenated_pipelines_2l_6w(2, 6),
        three_concatenated_pipelines_2l_7w(2, 7),
        three_concatenated_pipelines_2l_8w(2, 8),
        three_concatenated_pipelines_3l_1w(3, 1),
        three_concatenated_pipelines_3l_2w(3, 2),
        three_concatenated_pipelines_3l_3w(3, 3),
        three_concatenated_pipelines_3l_4w(3, 4),
        three_concatenated_pipelines_3l_5w(3, 5),
        three_concatenated_pipelines_3l_6w(3, 6),
        three_concatenated_pipelines_3l_7w(3, 7),
        three_concatenated_pipelines_3l_8w(3, 8),
        three_concatenated_pipelines_4l_1w(4, 1),
        three_concatenated_pipelines_4l_2w(4, 2),
        three_concatenated_pipelines_4l_3w(4, 3),
        three_concatenated_pipelines_4l_4w(4, 4),
        three_concatenated_pipelines_4l_5w(4, 5),
        three_concatenated_pipelines_4l_6w(4, 6),
        three_concatenated_pipelines_4l_7w(4, 7),
        three_concatenated_pipelines_4l_8w(4, 8),
        three_concatenated_pipelines_5l_1w(5, 1),
        three_concatenated_pipelines_5l_2w(5, 2),
        three_concatenated_pipelines_5l_3w(5, 3),
        three_concatenated_pipelines_5l_4w(5, 4),
        three_concatenated_pipelines_5l_5w(5, 5),
        three_concatenated_pipelines_5l_6w(5, 6),
        three_concatenated_pipelines_5l_7w(5, 7),
        three_concatenated_pipelines_5l_8w(5, 8),
        three_concatenated_pipelines_6l_1w(6, 1),
        three_concatenated_pipelines_6l_2w(6, 2),
        three_concatenated_pipelines_6l_3w(6, 3),
        three_concatenated_pipelines_6l_4w(6, 4),
        three_concatenated_pipelines_6l_5w(6, 5),
        three_concatenated_pipelines_6l_6w(6, 6),
        three_concatenated_pipelines_6l_7w(6, 7),
        three_concatenated_pipelines_6l_8w(6, 8),
        three_concatenated_pipelines_7l_1w(7, 1),
        three_concatenated_pipelines_7l_2w(7, 2),
        three_concatenated_pipelines_7l_3w(7, 3),
        three_concatenated_pipelines_7l_4w(7, 4),
        three_concatenated_pipelines_7l_5w(7, 5),
        three_concatenated_pipelines_7l_6w(7, 6),
        three_concatenated_pipelines_7l_7w(7, 7),
        three_concatenated_pipelines_7l_8w(7, 8),
        three_concatenated_pipelines_8l_1w(8, 1),
        three_concatenated_pipelines_8l_2w(8, 2),
        three_concatenated_pipelines_8l_3w(8, 3),
        three_concatenated_pipelines_8l_4w(8, 4),
        three_concatenated_pipelines_8l_5w(8, 5),
        three_concatenated_pipelines_8l_6w(8, 6),
        three_concatenated_pipelines_8l_7w(8, 7),
        three_concatenated_pipelines_8l_8w(8, 8),
    }
}

// ----------------------------------------------------------------------------
// Looping.Pipelines: one pipeline module behind a condition task that loops
// it a hundred rounds, growing the token count it stops at by one each round.
// ----------------------------------------------------------------------------

/// Upstream's `looping_pipelines(L, w)`: a single `SPSP` pipeline composed
/// once, with a condition task after it that asserts the round just run,
/// clears the test's own per-round state, grows the round's own token count
/// by one, and either loops back to the pipeline module or (once the round
/// count reaches [`MAX_N`]) falls through to a terminal task.
fn looping_pipelines(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);
        let mut taskflow = Taskflow::new();

        // The round's own token count: read by every pipe (to know when to
        // stop or how many tokens are in flight) and advanced by the
        // condition task below, so it is shared the same way every other
        // piece of a pipe's captured state in this file is.
        let round = Arc::new(AtomicUsize::new(0));

        let j1 = Arc::new(AtomicUsize::new(0));
        let j2 = Arc::new(AtomicUsize::new(0));
        let j3 = Arc::new(AtomicUsize::new(0));
        let j4 = Arc::new(AtomicUsize::new(0));
        let buffer = Buffer::new(num_lines, 4);
        let collection2: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
        let collection4: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

        // Every pipe reads the round's own token count rather than a fixed
        // `n`, which is what the builders' closure argument is for.
        let this_round = {
            let round = Arc::clone(&round);
            move || round.load(Ordering::Relaxed)
        };

        let pipe0 = counting_source(num_lines, this_round.clone(), &j1, &buffer);

        let pipe1 = propagating_pipe(num_lines, this_round.clone(), &j2, &buffer, &collection2);

        let pipe2 = {
            let round = Arc::clone(&round);
            let j3 = Arc::clone(&j3);
            let buffer = buffer.clone();
            Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
                let current = j3.load(Ordering::Relaxed);
                assert!(current < round.load(Ordering::Relaxed));
                assert_eq!(pf.token() % num_lines, pf.line());
                let expected = i32::try_from(current + 2).expect("a token count fits in an i32");
                assert_eq!(expected, buffer.get(pf.line(), pf.pipe() - 1));
                buffer.set(pf.line(), pf.pipe(), expected + 1);
                j3.fetch_add(1, Ordering::Relaxed);
            })
        };

        let pipe3 = collecting_pipe(num_lines, this_round, &j4, &buffer, &collection4);

        let pl = Arc::new(Pipeline::new(num_lines, vec![pipe0, pipe1, pipe2, pipe3]));
        let module = compose_module(&mut taskflow, &*pl, "module_of_pipeline");

        let cond = {
            let round = Arc::clone(&round);
            let j1 = Arc::clone(&j1);
            let j2 = Arc::clone(&j2);
            let j3 = Arc::clone(&j3);
            let j4 = Arc::clone(&j4);
            let buffer = buffer.clone();
            let collection2 = Arc::clone(&collection2);
            let collection4 = Arc::clone(&collection4);
            let pl = Arc::clone(&pl);
            // Upstream's `cnt`: the running sum of every round's own token
            // count, since the pipeline itself is never reset -- this
            // file's module docs say why.
            let mut cnt = 0_usize;
            taskflow.emplace_condition(move || {
                let this_round = round.load(Ordering::Relaxed);
                assert_eq!(j1.load(Ordering::Relaxed), this_round);
                assert_eq!(j2.load(Ordering::Relaxed), this_round);
                assert_eq!(j3.load(Ordering::Relaxed), this_round);
                assert_eq!(j4.load(Ordering::Relaxed), this_round);
                assert_consecutive_from(&collection2, 1, this_round);
                assert_consecutive_from(&collection4, 3, this_round);
                assert_eq!(pl.num_tokens(), cnt);

                buffer.clear();
                advance_round(
                    &round,
                    [&j1, &j2, &j3, &j4],
                    [&collection2, &collection4],
                    &mut cnt,
                    MAX_N,
                )
            })
        };
        wire_loop(&mut taskflow, module, cond);

        executor.run(&taskflow).wait().expect("no task panicked");
    });
}

worker_matrix! {
    looping_pipelines {
        looping_pipelines_1l_1w(1, 1),
        looping_pipelines_1l_2w(1, 2),
        looping_pipelines_1l_3w(1, 3),
        looping_pipelines_1l_4w(1, 4),
        looping_pipelines_1l_5w(1, 5),
        looping_pipelines_1l_6w(1, 6),
        looping_pipelines_1l_7w(1, 7),
        looping_pipelines_1l_8w(1, 8),
        looping_pipelines_2l_1w(2, 1),
        looping_pipelines_2l_2w(2, 2),
        looping_pipelines_2l_3w(2, 3),
        looping_pipelines_2l_4w(2, 4),
        looping_pipelines_2l_5w(2, 5),
        looping_pipelines_2l_6w(2, 6),
        looping_pipelines_2l_7w(2, 7),
        looping_pipelines_2l_8w(2, 8),
        looping_pipelines_3l_1w(3, 1),
        looping_pipelines_3l_2w(3, 2),
        looping_pipelines_3l_3w(3, 3),
        looping_pipelines_3l_4w(3, 4),
        looping_pipelines_3l_5w(3, 5),
        looping_pipelines_3l_6w(3, 6),
        looping_pipelines_3l_7w(3, 7),
        looping_pipelines_3l_8w(3, 8),
        looping_pipelines_4l_1w(4, 1),
        looping_pipelines_4l_2w(4, 2),
        looping_pipelines_4l_3w(4, 3),
        looping_pipelines_4l_4w(4, 4),
        looping_pipelines_4l_5w(4, 5),
        looping_pipelines_4l_6w(4, 6),
        looping_pipelines_4l_7w(4, 7),
        looping_pipelines_4l_8w(4, 8),
        looping_pipelines_5l_1w(5, 1),
        looping_pipelines_5l_2w(5, 2),
        looping_pipelines_5l_3w(5, 3),
        looping_pipelines_5l_4w(5, 4),
        looping_pipelines_5l_5w(5, 5),
        looping_pipelines_5l_6w(5, 6),
        looping_pipelines_5l_7w(5, 7),
        looping_pipelines_5l_8w(5, 8),
        looping_pipelines_6l_1w(6, 1),
        looping_pipelines_6l_2w(6, 2),
        looping_pipelines_6l_3w(6, 3),
        looping_pipelines_6l_4w(6, 4),
        looping_pipelines_6l_5w(6, 5),
        looping_pipelines_6l_6w(6, 6),
        looping_pipelines_6l_7w(6, 7),
        looping_pipelines_6l_8w(6, 8),
        looping_pipelines_7l_1w(7, 1),
        looping_pipelines_7l_2w(7, 2),
        looping_pipelines_7l_3w(7, 3),
        looping_pipelines_7l_4w(7, 4),
        looping_pipelines_7l_5w(7, 5),
        looping_pipelines_7l_6w(7, 6),
        looping_pipelines_7l_7w(7, 7),
        looping_pipelines_7l_8w(7, 8),
        looping_pipelines_8l_1w(8, 1),
        looping_pipelines_8l_2w(8, 2),
        looping_pipelines_8l_3w(8, 3),
        looping_pipelines_8l_4w(8, 4),
        looping_pipelines_8l_5w(8, 5),
        looping_pipelines_8l_6w(8, 6),
        looping_pipelines_8l_7w(8, 7),
        looping_pipelines_8l_8w(8, 8),
    }
}

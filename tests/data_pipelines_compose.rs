//! Translated from `unittests/test_data_pipelines.cpp` (Taskflow v4.1.0).
//!
//! The composition families that file adds on top of the structural ones
//! `tests/data_pipelines.rs` (#186) already covers: `Three.Parallel.DataPipelines`,
//! `Three.Concatenated.DataPipelines` and `Looping.DataPipelines` (64 cases
//! each, lines 1..=8 and workers 1..=8), `Ifelse.DataPipelines` (16 cases,
//! lines `1,3,5,7` and workers 1..=4) and `PipelineinPipeline.DataPipelines`
//! (18 cases, lines `1,3,5`, workers `1,2` and sub-lines `1,3,4`) -- 226 in
//! all, through [`worker_matrix!`]. `tests/pipelines_compose.rs` (#183),
//! `tests/pipelines_nested.rs` (#184) and `tests/data_pipelines.rs` (#186)
//! are the model: every adaptation their own module docs state -- `Arc`s for
//! upstream's by-reference captures, no in-graph "test" task, a fixed-seed
//! generator standing in for an unseeded `rand()` -- applies here too, plus
//! two specific to this file:
//!
//! - **`Three.Parallel.DataPipelines` and `Three.Concatenated.DataPipelines`
//!   do not share one pipeline builder the way `tests/pipelines_compose.rs`'s
//!   `ThreePipelines` does.** Upstream's own first pipeline (`SSSS`) is typed
//!   `i32 -> i32 -> i32 -> void` in the parallel family and
//!   `i32 -> String -> i32 -> void` in the concatenated one -- a deliberate
//!   round trip through a typed intermediate value that the untyped
//!   [`Pipe`](rustflow::pipeline::pipe::Pipe) this crate's sibling files use
//!   has no way to need. [`build_pl1_int`] and [`build_pl1_string_roundtrip`]
//!   are the two typings; [`DataPipelineTail`] is the second and third
//!   pipeline (`SSP`, `SP`), identical in both families, factored out once.
//! - **One correction from upstream's own `pipeline_in_pipeline`.** Its
//!   second outer pipe builds a sub-pipeline whose own middle stage checks
//!   `source[j2]` -- the *outer* pipe's shared atomic ticket counter, read
//!   from inside a closure running on a different, unsynchronized outer
//!   token -- rather than the sub-pipeline's own running index, which every
//!   other copy of the same check, in every other outer pipe, already uses.
//!   With more than one outer line in flight, two outer tokens can reach
//!   that parallel pipe out of order, and the ticket read there then belongs
//!   to whichever outer token is fastest rather than the one running the
//!   closure: a race baked into the upstream test itself, not into the
//!   library. [`run_subpipeline`] below needs no outer-token data at all --
//!   upstream's own `source` is `0, 1, 2, ...` on every row regardless of
//!   which outer token it belongs to, the same simplification
//!   `tests/pipelines_nested.rs`'s own `partial_sum` makes -- so every
//!   sub-pipe checks the sub-pipeline's own index consistently instead.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::pipeline::data_pipeline::{
    DataPipeline, parallel_pipe, parallel_pipe_with_flow, serial_pipe, serial_pipe_with_flow,
};
use rustflow::prelude::*;

#[macro_use]
mod common;
#[path = "common/compose.rs"]
mod compose;
#[path = "common/data_pipeline.rs"]
mod data_pipeline;
#[path = "common/looping.rs"]
mod looping;
#[path = "common/pipeline_answers.rs"]
mod pipeline_answers;
#[path = "common/serialized.rs"]
mod serialized;
#[path = "common/tokens.rs"]
mod tokens;

use compose::compose_module;
use data_pipeline::{collecting_pipe, counting_source, propagating_pipe};
use looping::{advance_round, wire_loop};
use pipeline_answers::{assert_ifelse_answers, partial_sum, sub_pipeline_sum};
use serialized::serialized;
// `MAX_N` is the sweep for every family here but `Ifelse.DataPipelines` and
// `PipelineinPipeline.DataPipelines`, which each sweep a range of their own.
use tokens::{MAX_N, assert_consecutive_from};

// ----------------------------------------------------------------------------
// Three.Parallel.DataPipelines / Three.Concatenated.DataPipelines
// ----------------------------------------------------------------------------

/// The second and third of the three data pipelines both composition
/// families build -- `SSP` and `SP` -- identical between the two families;
/// only the first (typed differently per family, built by [`build_pl1_int`]
/// or [`build_pl1_string_roundtrip`]) and the wiring between all three
/// differ.
struct DataPipelineTail {
    pl2: DataPipeline,
    pl3: DataPipeline,
    module2: Task,
    module3: Task,
    j2_1: Arc<AtomicUsize>,
    j2_2: Arc<AtomicUsize>,
    j2_3: Arc<AtomicUsize>,
    j3_1: Arc<AtomicUsize>,
    j3_2: Arc<AtomicUsize>,
    collection2: Arc<Mutex<Vec<i32>>>,
    collection3: Arc<Mutex<Vec<i32>>>,
}

impl DataPipelineTail {
    /// Builds and composes `pl2` (`SSP`) and `pl3` (`SP`) into `taskflow`,
    /// for a round that stops each pipeline's first pipe after `n` tokens.
    fn build(taskflow: &mut Taskflow, num_lines: usize, n: usize) -> Self {
        let j2_1 = Arc::new(AtomicUsize::new(0));
        let j2_2 = Arc::new(AtomicUsize::new(0));
        let j2_3 = Arc::new(AtomicUsize::new(0));
        let collection2: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

        let pipe2_0 = counting_source(num_lines, move || n, &j2_1);
        let pipe2_1 = {
            let j2_2 = Arc::clone(&j2_2);
            serial_pipe_with_flow::<i32, i32>(move |value, pf| {
                let current = j2_2.load(Ordering::Relaxed);
                assert!(current < n);
                assert_eq!(pf.token() % num_lines, pf.line());
                let expected = i32::try_from(current + 1).expect("a token count fits in an i32");
                assert_eq!(expected, value);
                j2_2.fetch_add(1, Ordering::Relaxed);
                value
            })
        };
        let pipe2_2 = collecting_pipe(num_lines, move || n, &j2_3, &collection2);
        let pl2 = DataPipeline::builder(num_lines)
            .pipe(pipe2_0)
            .pipe(pipe2_1)
            .pipe(pipe2_2)
            .build();
        let module2 = compose_module(taskflow, &pl2, "module_of_data_pipeline2");

        let j3_1 = Arc::new(AtomicUsize::new(0));
        let j3_2 = Arc::new(AtomicUsize::new(0));
        let collection3: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

        let pipe3_0 = counting_source(num_lines, move || n, &j3_1);
        let pipe3_1 = collecting_pipe(num_lines, move || n, &j3_2, &collection3);
        let pl3 = DataPipeline::builder(num_lines)
            .pipe(pipe3_0)
            .pipe(pipe3_1)
            .build();
        let module3 = compose_module(taskflow, &pl3, "module_of_data_pipeline3");

        Self {
            pl2,
            pl3,
            module2,
            module3,
            j2_1,
            j2_2,
            j2_3,
            j3_1,
            j3_2,
            collection2,
            collection3,
        }
    }

    /// Asserts round `n`'s invariants for `pl2` and `pl3`, given the attempt
    /// number (`1`-based): `attempt * n` is sound only because neither
    /// pipeline's own `num_tokens` is ever reset between runs -- the
    /// `tests/pipelines_compose.rs` module docs' own reasoning, unchanged
    /// here.
    fn assert_round(&self, n: usize, attempt: usize) {
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

    /// Clears every local counter and collection between runs -- never
    /// `pl2`'s or `pl3`'s own `num_tokens`.
    fn reset_round(&self) {
        self.j2_1.store(0, Ordering::Relaxed);
        self.j2_2.store(0, Ordering::Relaxed);
        self.j2_3.store(0, Ordering::Relaxed);
        self.collection2.lock().expect("uncontended").clear();

        self.j3_1.store(0, Ordering::Relaxed);
        self.j3_2.store(0, Ordering::Relaxed);
        self.collection3.lock().expect("uncontended").clear();
    }
}

/// Builds and composes the `SSSS` first pipeline -- every stage `i32`,
/// upstream's own typing in `three_parallel_data_pipelines` -- into
/// `taskflow`. Returns the pipeline, its module task and its four counters
/// in pipe order.
fn build_pl1_int(
    taskflow: &mut Taskflow,
    num_lines: usize,
    n: usize,
) -> (DataPipeline, Task, [Arc<AtomicUsize>; 4]) {
    let j1_1 = Arc::new(AtomicUsize::new(0));
    let j1_2 = Arc::new(AtomicUsize::new(0));
    let j1_3 = Arc::new(AtomicUsize::new(0));
    let j1_4 = Arc::new(AtomicUsize::new(0));

    let pipe1_0 = counting_source(num_lines, move || n, &j1_1);
    let pipe1_1 = {
        let j1_2 = Arc::clone(&j1_2);
        serial_pipe_with_flow::<i32, i32>(move |value, pf| {
            let current = j1_2.load(Ordering::Relaxed);
            assert!(current < n);
            assert_eq!(pf.token() % num_lines, pf.line());
            let expected = i32::try_from(current + 1).expect("a token count fits in an i32");
            assert_eq!(expected, value);
            j1_2.fetch_add(1, Ordering::Relaxed);
            value
        })
    };
    let pipe1_2 = {
        let j1_3 = Arc::clone(&j1_3);
        serial_pipe_with_flow::<i32, i32>(move |value, pf| {
            let current = j1_3.load(Ordering::Relaxed);
            assert!(current < n);
            assert_eq!(pf.token() % num_lines, pf.line());
            let expected = i32::try_from(current + 1).expect("a token count fits in an i32");
            assert_eq!(expected, value);
            j1_3.fetch_add(1, Ordering::Relaxed);
            value
        })
    };
    let pipe1_3 = {
        let j1_4 = Arc::clone(&j1_4);
        serial_pipe_with_flow::<i32, ()>(move |value, pf| {
            let current = j1_4.load(Ordering::Relaxed);
            assert!(current < n);
            assert_eq!(pf.token() % num_lines, pf.line());
            let expected = i32::try_from(current + 1).expect("a token count fits in an i32");
            assert_eq!(expected, value);
            j1_4.fetch_add(1, Ordering::Relaxed);
        })
    };

    let pl1 = DataPipeline::builder(num_lines)
        .pipe(pipe1_0)
        .pipe(pipe1_1)
        .pipe(pipe1_2)
        .pipe(pipe1_3)
        .build();
    let module1 = compose_module(taskflow, &pl1, "module_of_data_pipeline1");

    (pl1, module1, [j1_1, j1_2, j1_3, j1_4])
}

/// Builds and composes the `SSSS` first pipeline with upstream's
/// `three_concatenated_data_pipelines` own typing -- `i32 -> String -> i32`
/// through its middle two stages, rather than [`build_pl1_int`]'s all-`i32`
/// one -- into `taskflow`.
fn build_pl1_string_roundtrip(
    taskflow: &mut Taskflow,
    num_lines: usize,
    n: usize,
) -> (DataPipeline, Task, [Arc<AtomicUsize>; 4]) {
    let j1_1 = Arc::new(AtomicUsize::new(0));
    let j1_2 = Arc::new(AtomicUsize::new(0));
    let j1_3 = Arc::new(AtomicUsize::new(0));
    let j1_4 = Arc::new(AtomicUsize::new(0));

    let pipe1_0 = counting_source(num_lines, move || n, &j1_1);
    let pipe1_1 = {
        let j1_2 = Arc::clone(&j1_2);
        serial_pipe_with_flow::<i32, String>(move |value, pf| {
            let current = j1_2.load(Ordering::Relaxed);
            assert!(current < n);
            assert_eq!(pf.token() % num_lines, pf.line());
            let expected = i32::try_from(current + 1).expect("a token count fits in an i32");
            assert_eq!(expected, value);
            j1_2.fetch_add(1, Ordering::Relaxed);
            value.to_string()
        })
    };
    let pipe1_2 = {
        let j1_3 = Arc::clone(&j1_3);
        serial_pipe_with_flow::<String, i32>(move |text, pf| {
            let current = j1_3.load(Ordering::Relaxed);
            assert!(current < n);
            assert_eq!(pf.token() % num_lines, pf.line());
            let value: i32 = text
                .parse()
                .expect("the previous pipe emits a valid integer string");
            let expected = i32::try_from(current + 1).expect("a token count fits in an i32");
            assert_eq!(expected, value);
            j1_3.fetch_add(1, Ordering::Relaxed);
            value
        })
    };
    let pipe1_3 = {
        let j1_4 = Arc::clone(&j1_4);
        serial_pipe_with_flow::<i32, ()>(move |value, pf| {
            let current = j1_4.load(Ordering::Relaxed);
            assert!(current < n);
            assert_eq!(pf.token() % num_lines, pf.line());
            let expected = i32::try_from(current + 1).expect("a token count fits in an i32");
            assert_eq!(expected, value);
            j1_4.fetch_add(1, Ordering::Relaxed);
        })
    };

    let pl1 = DataPipeline::builder(num_lines)
        .pipe(pipe1_0)
        .pipe(pipe1_1)
        .pipe(pipe1_2)
        .pipe(pipe1_3)
        .build();
    let module1 = compose_module(taskflow, &pl1, "module_of_data_pipeline1");

    (pl1, module1, [j1_1, j1_2, j1_3, j1_4])
}

/// Upstream's `three_parallel_data_pipelines(L, w)`: three independent data
/// pipelines (`SSSS`, `SSP`, `SP`) composed into one taskflow as three
/// parallel subgraphs, run three times with none of the three ever reset.
fn three_parallel_data_pipelines(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let (pl1, module1, [j1_1, j1_2, j1_3, j1_4]) =
                build_pl1_int(&mut taskflow, num_lines, n);
            let tail = DataPipelineTail::build(&mut taskflow, num_lines, n);

            let initial = taskflow.emplace(|| {});
            taskflow
                .task(initial)
                .name("initial")
                .precede([module1, tail.module2, tail.module3]);
            let terminal = taskflow.emplace(|| {});
            taskflow
                .task(terminal)
                .name("terminal")
                .succeed([module1, tail.module2, tail.module3]);

            for attempt in 1..=3 {
                executor.run(&taskflow).wait().expect("no task panicked");

                assert_eq!(j1_1.load(Ordering::Relaxed), n);
                assert_eq!(j1_2.load(Ordering::Relaxed), n);
                assert_eq!(j1_3.load(Ordering::Relaxed), n);
                assert_eq!(j1_4.load(Ordering::Relaxed), n);
                assert_eq!(pl1.num_tokens(), attempt * n);
                tail.assert_round(n, attempt);

                j1_1.store(0, Ordering::Relaxed);
                j1_2.store(0, Ordering::Relaxed);
                j1_3.store(0, Ordering::Relaxed);
                j1_4.store(0, Ordering::Relaxed);
                tail.reset_round();
            }
        }
    });
}

worker_matrix! {
    three_parallel_data_pipelines {
        three_parallel_data_pipelines_1l_1w(1, 1),
        three_parallel_data_pipelines_1l_2w(1, 2),
        three_parallel_data_pipelines_1l_3w(1, 3),
        three_parallel_data_pipelines_1l_4w(1, 4),
        three_parallel_data_pipelines_1l_5w(1, 5),
        three_parallel_data_pipelines_1l_6w(1, 6),
        three_parallel_data_pipelines_1l_7w(1, 7),
        three_parallel_data_pipelines_1l_8w(1, 8),
        three_parallel_data_pipelines_2l_1w(2, 1),
        three_parallel_data_pipelines_2l_2w(2, 2),
        three_parallel_data_pipelines_2l_3w(2, 3),
        three_parallel_data_pipelines_2l_4w(2, 4),
        three_parallel_data_pipelines_2l_5w(2, 5),
        three_parallel_data_pipelines_2l_6w(2, 6),
        three_parallel_data_pipelines_2l_7w(2, 7),
        three_parallel_data_pipelines_2l_8w(2, 8),
        three_parallel_data_pipelines_3l_1w(3, 1),
        three_parallel_data_pipelines_3l_2w(3, 2),
        three_parallel_data_pipelines_3l_3w(3, 3),
        three_parallel_data_pipelines_3l_4w(3, 4),
        three_parallel_data_pipelines_3l_5w(3, 5),
        three_parallel_data_pipelines_3l_6w(3, 6),
        three_parallel_data_pipelines_3l_7w(3, 7),
        three_parallel_data_pipelines_3l_8w(3, 8),
        three_parallel_data_pipelines_4l_1w(4, 1),
        three_parallel_data_pipelines_4l_2w(4, 2),
        three_parallel_data_pipelines_4l_3w(4, 3),
        three_parallel_data_pipelines_4l_4w(4, 4),
        three_parallel_data_pipelines_4l_5w(4, 5),
        three_parallel_data_pipelines_4l_6w(4, 6),
        three_parallel_data_pipelines_4l_7w(4, 7),
        three_parallel_data_pipelines_4l_8w(4, 8),
        three_parallel_data_pipelines_5l_1w(5, 1),
        three_parallel_data_pipelines_5l_2w(5, 2),
        three_parallel_data_pipelines_5l_3w(5, 3),
        three_parallel_data_pipelines_5l_4w(5, 4),
        three_parallel_data_pipelines_5l_5w(5, 5),
        three_parallel_data_pipelines_5l_6w(5, 6),
        three_parallel_data_pipelines_5l_7w(5, 7),
        three_parallel_data_pipelines_5l_8w(5, 8),
        three_parallel_data_pipelines_6l_1w(6, 1),
        three_parallel_data_pipelines_6l_2w(6, 2),
        three_parallel_data_pipelines_6l_3w(6, 3),
        three_parallel_data_pipelines_6l_4w(6, 4),
        three_parallel_data_pipelines_6l_5w(6, 5),
        three_parallel_data_pipelines_6l_6w(6, 6),
        three_parallel_data_pipelines_6l_7w(6, 7),
        three_parallel_data_pipelines_6l_8w(6, 8),
        three_parallel_data_pipelines_7l_1w(7, 1),
        three_parallel_data_pipelines_7l_2w(7, 2),
        three_parallel_data_pipelines_7l_3w(7, 3),
        three_parallel_data_pipelines_7l_4w(7, 4),
        three_parallel_data_pipelines_7l_5w(7, 5),
        three_parallel_data_pipelines_7l_6w(7, 6),
        three_parallel_data_pipelines_7l_7w(7, 7),
        three_parallel_data_pipelines_7l_8w(7, 8),
        three_parallel_data_pipelines_8l_1w(8, 1),
        three_parallel_data_pipelines_8l_2w(8, 2),
        three_parallel_data_pipelines_8l_3w(8, 3),
        three_parallel_data_pipelines_8l_4w(8, 4),
        three_parallel_data_pipelines_8l_5w(8, 5),
        three_parallel_data_pipelines_8l_6w(8, 6),
        three_parallel_data_pipelines_8l_7w(8, 7),
        three_parallel_data_pipelines_8l_8w(8, 8),
    }
}

/// Upstream's `three_concatenated_data_pipelines(L, w)`: the same three data
/// pipelines as [`three_parallel_data_pipelines`] (pipeline 1 typed by
/// [`build_pl1_string_roundtrip`] rather than [`build_pl1_int`]), chained
/// `module1.precede(module2)`, `module2.precede(module3)` rather than run
/// independently.
fn three_concatenated_data_pipelines(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);

        for n in 0..=MAX_N {
            let mut taskflow = Taskflow::new();
            let (pl1, module1, [j1_1, j1_2, j1_3, j1_4]) =
                build_pl1_string_roundtrip(&mut taskflow, num_lines, n);
            let tail = DataPipelineTail::build(&mut taskflow, num_lines, n);

            let initial = taskflow.emplace(|| {});
            taskflow.task(initial).name("initial").precede([module1]);
            taskflow.task(module1).precede([tail.module2]);
            taskflow.task(tail.module2).precede([tail.module3]);
            let terminal = taskflow.emplace(|| {});
            taskflow
                .task(terminal)
                .name("terminal")
                .succeed([tail.module3]);

            for attempt in 1..=3 {
                executor.run(&taskflow).wait().expect("no task panicked");

                assert_eq!(j1_1.load(Ordering::Relaxed), n);
                assert_eq!(j1_2.load(Ordering::Relaxed), n);
                assert_eq!(j1_3.load(Ordering::Relaxed), n);
                assert_eq!(j1_4.load(Ordering::Relaxed), n);
                assert_eq!(pl1.num_tokens(), attempt * n);
                tail.assert_round(n, attempt);

                j1_1.store(0, Ordering::Relaxed);
                j1_2.store(0, Ordering::Relaxed);
                j1_3.store(0, Ordering::Relaxed);
                j1_4.store(0, Ordering::Relaxed);
                tail.reset_round();
            }
        }
    });
}

worker_matrix! {
    three_concatenated_data_pipelines {
        three_concatenated_data_pipelines_1l_1w(1, 1),
        three_concatenated_data_pipelines_1l_2w(1, 2),
        three_concatenated_data_pipelines_1l_3w(1, 3),
        three_concatenated_data_pipelines_1l_4w(1, 4),
        three_concatenated_data_pipelines_1l_5w(1, 5),
        three_concatenated_data_pipelines_1l_6w(1, 6),
        three_concatenated_data_pipelines_1l_7w(1, 7),
        three_concatenated_data_pipelines_1l_8w(1, 8),
        three_concatenated_data_pipelines_2l_1w(2, 1),
        three_concatenated_data_pipelines_2l_2w(2, 2),
        three_concatenated_data_pipelines_2l_3w(2, 3),
        three_concatenated_data_pipelines_2l_4w(2, 4),
        three_concatenated_data_pipelines_2l_5w(2, 5),
        three_concatenated_data_pipelines_2l_6w(2, 6),
        three_concatenated_data_pipelines_2l_7w(2, 7),
        three_concatenated_data_pipelines_2l_8w(2, 8),
        three_concatenated_data_pipelines_3l_1w(3, 1),
        three_concatenated_data_pipelines_3l_2w(3, 2),
        three_concatenated_data_pipelines_3l_3w(3, 3),
        three_concatenated_data_pipelines_3l_4w(3, 4),
        three_concatenated_data_pipelines_3l_5w(3, 5),
        three_concatenated_data_pipelines_3l_6w(3, 6),
        three_concatenated_data_pipelines_3l_7w(3, 7),
        three_concatenated_data_pipelines_3l_8w(3, 8),
        three_concatenated_data_pipelines_4l_1w(4, 1),
        three_concatenated_data_pipelines_4l_2w(4, 2),
        three_concatenated_data_pipelines_4l_3w(4, 3),
        three_concatenated_data_pipelines_4l_4w(4, 4),
        three_concatenated_data_pipelines_4l_5w(4, 5),
        three_concatenated_data_pipelines_4l_6w(4, 6),
        three_concatenated_data_pipelines_4l_7w(4, 7),
        three_concatenated_data_pipelines_4l_8w(4, 8),
        three_concatenated_data_pipelines_5l_1w(5, 1),
        three_concatenated_data_pipelines_5l_2w(5, 2),
        three_concatenated_data_pipelines_5l_3w(5, 3),
        three_concatenated_data_pipelines_5l_4w(5, 4),
        three_concatenated_data_pipelines_5l_5w(5, 5),
        three_concatenated_data_pipelines_5l_6w(5, 6),
        three_concatenated_data_pipelines_5l_7w(5, 7),
        three_concatenated_data_pipelines_5l_8w(5, 8),
        three_concatenated_data_pipelines_6l_1w(6, 1),
        three_concatenated_data_pipelines_6l_2w(6, 2),
        three_concatenated_data_pipelines_6l_3w(6, 3),
        three_concatenated_data_pipelines_6l_4w(6, 4),
        three_concatenated_data_pipelines_6l_5w(6, 5),
        three_concatenated_data_pipelines_6l_6w(6, 6),
        three_concatenated_data_pipelines_6l_7w(6, 7),
        three_concatenated_data_pipelines_6l_8w(6, 8),
        three_concatenated_data_pipelines_7l_1w(7, 1),
        three_concatenated_data_pipelines_7l_2w(7, 2),
        three_concatenated_data_pipelines_7l_3w(7, 3),
        three_concatenated_data_pipelines_7l_4w(7, 4),
        three_concatenated_data_pipelines_7l_5w(7, 5),
        three_concatenated_data_pipelines_7l_6w(7, 6),
        three_concatenated_data_pipelines_7l_7w(7, 7),
        three_concatenated_data_pipelines_7l_8w(7, 8),
        three_concatenated_data_pipelines_8l_1w(8, 1),
        three_concatenated_data_pipelines_8l_2w(8, 2),
        three_concatenated_data_pipelines_8l_3w(8, 3),
        three_concatenated_data_pipelines_8l_4w(8, 4),
        three_concatenated_data_pipelines_8l_5w(8, 5),
        three_concatenated_data_pipelines_8l_6w(8, 6),
        three_concatenated_data_pipelines_8l_7w(8, 7),
        three_concatenated_data_pipelines_8l_8w(8, 8),
    }
}

// ----------------------------------------------------------------------------
// Looping.DataPipelines: one `SPSP` data pipeline module behind a condition
// task that loops it a hundred rounds, growing the token count it stops at
// by one each round.
// ----------------------------------------------------------------------------

/// Upstream's `looping_data_pipelines(L, w)`: a single `SPSP` data pipeline
/// composed once, with a condition task after it that asserts the round just
/// run *per round* -- so a reset that fails to clear state shows up as a
/// wrong round, not a coincidentally-right final count -- clears the test's
/// own per-round state, grows the round's own token count by one, and either
/// loops back to the pipeline module or (once the round count reaches
/// [`MAX_N`]) falls through to a terminal task.
fn looping_data_pipelines(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);
        let mut taskflow = Taskflow::new();

        // The round's own token count: read by every pipe and advanced by
        // the condition task below.
        let round = Arc::new(AtomicUsize::new(0));

        let j1 = Arc::new(AtomicUsize::new(0));
        let j2 = Arc::new(AtomicUsize::new(0));
        let j3 = Arc::new(AtomicUsize::new(0));
        let j4 = Arc::new(AtomicUsize::new(0));
        let collection2: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
        let collection4: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

        // Every pipe reads the round's own token count rather than a fixed
        // `n`, which is what the builders' closure argument is for.
        let this_round = {
            let round = Arc::clone(&round);
            move || round.load(Ordering::Relaxed)
        };

        let pipe0 = counting_source(num_lines, this_round.clone(), &j1);

        let pipe1 = propagating_pipe(num_lines, this_round.clone(), &j2, &collection2);

        let pipe2 = {
            let round = Arc::clone(&round);
            let j3 = Arc::clone(&j3);
            serial_pipe_with_flow::<i32, i32>(move |value, pf| {
                let current = j3.load(Ordering::Relaxed);
                assert!(current < round.load(Ordering::Relaxed));
                assert_eq!(pf.token() % num_lines, pf.line());
                let expected = i32::try_from(current + 2).expect("a token count fits in an i32");
                assert_eq!(expected, value);
                j3.fetch_add(1, Ordering::Relaxed);
                value + 1
            })
        };

        let pipe3 = collecting_pipe(num_lines, this_round, &j4, &collection4);

        let pipeline = Arc::new(
            DataPipeline::builder(num_lines)
                .pipe(pipe0)
                .pipe(pipe1)
                .pipe(pipe2)
                .pipe(pipe3)
                .build(),
        );
        let module = compose_module(&mut taskflow, &*pipeline, "module_of_data_pipeline");

        let cond = {
            let round = Arc::clone(&round);
            let j1 = Arc::clone(&j1);
            let j2 = Arc::clone(&j2);
            let j3 = Arc::clone(&j3);
            let j4 = Arc::clone(&j4);
            let collection2 = Arc::clone(&collection2);
            let collection4 = Arc::clone(&collection4);
            let pipeline = Arc::clone(&pipeline);
            // Upstream's `cnt`: the running sum of every round's own token
            // count, since the pipeline itself is never reset.
            let mut cnt = 0_usize;
            taskflow.emplace_condition(move || {
                let this_round = round.load(Ordering::Relaxed);
                assert_eq!(j1.load(Ordering::Relaxed), this_round);
                assert_eq!(j2.load(Ordering::Relaxed), this_round);
                assert_eq!(j3.load(Ordering::Relaxed), this_round);
                assert_eq!(j4.load(Ordering::Relaxed), this_round);
                assert_consecutive_from(&collection2, 1, this_round);
                assert_consecutive_from(&collection4, 3, this_round);
                assert_eq!(pipeline.num_tokens(), cnt);

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
    looping_data_pipelines {
        looping_data_pipelines_1l_1w(1, 1),
        looping_data_pipelines_1l_2w(1, 2),
        looping_data_pipelines_1l_3w(1, 3),
        looping_data_pipelines_1l_4w(1, 4),
        looping_data_pipelines_1l_5w(1, 5),
        looping_data_pipelines_1l_6w(1, 6),
        looping_data_pipelines_1l_7w(1, 7),
        looping_data_pipelines_1l_8w(1, 8),
        looping_data_pipelines_2l_1w(2, 1),
        looping_data_pipelines_2l_2w(2, 2),
        looping_data_pipelines_2l_3w(2, 3),
        looping_data_pipelines_2l_4w(2, 4),
        looping_data_pipelines_2l_5w(2, 5),
        looping_data_pipelines_2l_6w(2, 6),
        looping_data_pipelines_2l_7w(2, 7),
        looping_data_pipelines_2l_8w(2, 8),
        looping_data_pipelines_3l_1w(3, 1),
        looping_data_pipelines_3l_2w(3, 2),
        looping_data_pipelines_3l_3w(3, 3),
        looping_data_pipelines_3l_4w(3, 4),
        looping_data_pipelines_3l_5w(3, 5),
        looping_data_pipelines_3l_6w(3, 6),
        looping_data_pipelines_3l_7w(3, 7),
        looping_data_pipelines_3l_8w(3, 8),
        looping_data_pipelines_4l_1w(4, 1),
        looping_data_pipelines_4l_2w(4, 2),
        looping_data_pipelines_4l_3w(4, 3),
        looping_data_pipelines_4l_4w(4, 4),
        looping_data_pipelines_4l_5w(4, 5),
        looping_data_pipelines_4l_6w(4, 6),
        looping_data_pipelines_4l_7w(4, 7),
        looping_data_pipelines_4l_8w(4, 8),
        looping_data_pipelines_5l_1w(5, 1),
        looping_data_pipelines_5l_2w(5, 2),
        looping_data_pipelines_5l_3w(5, 3),
        looping_data_pipelines_5l_4w(5, 4),
        looping_data_pipelines_5l_5w(5, 5),
        looping_data_pipelines_5l_6w(5, 6),
        looping_data_pipelines_5l_7w(5, 7),
        looping_data_pipelines_5l_8w(5, 8),
        looping_data_pipelines_6l_1w(6, 1),
        looping_data_pipelines_6l_2w(6, 2),
        looping_data_pipelines_6l_3w(6, 3),
        looping_data_pipelines_6l_4w(6, 4),
        looping_data_pipelines_6l_5w(6, 5),
        looping_data_pipelines_6l_6w(6, 6),
        looping_data_pipelines_6l_7w(6, 7),
        looping_data_pipelines_6l_8w(6, 8),
        looping_data_pipelines_7l_1w(7, 1),
        looping_data_pipelines_7l_2w(7, 2),
        looping_data_pipelines_7l_3w(7, 3),
        looping_data_pipelines_7l_4w(7, 4),
        looping_data_pipelines_7l_5w(7, 5),
        looping_data_pipelines_7l_6w(7, 6),
        looping_data_pipelines_7l_7w(7, 7),
        looping_data_pipelines_7l_8w(7, 8),
        looping_data_pipelines_8l_1w(8, 1),
        looping_data_pipelines_8l_2w(8, 2),
        looping_data_pipelines_8l_3w(8, 3),
        looping_data_pipelines_8l_4w(8, 4),
        looping_data_pipelines_8l_5w(8, 5),
        looping_data_pipelines_8l_6w(8, 6),
        looping_data_pipelines_8l_7w(8, 7),
        looping_data_pipelines_8l_8w(8, 8),
    }
}

// ----------------------------------------------------------------------------
// Ifelse.DataPipelines: three pipes, each branching internally on the value
// it is handed, over a sweep of token counts.
// ----------------------------------------------------------------------------

/// Upstream's unseeded `rand() % 9962`, stood in for by a linear congruential
/// generator -- `tests/sort.rs`'s own `scattered` and its doc comment's
/// reasoning, over the same range upstream draws from -- so a failure here
/// reproduces instead of depending on which process-wide seed the C library
/// happened to pick.
fn pseudo_random_sources(n: usize) -> Vec<i32> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..n)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let unsigned = (state >> 33) % 9962;
            i32::try_from(unsigned).expect("9962 fits in an i32")
        })
        .collect()
}

/// Upstream's `ifelse_data_pipeline(L, w)`: a serial pipe that branches on
/// its own source value, a parallel pipe that branches on what the first
/// just produced, and a serial pipe that branches on what the second
/// produced and collects the result -- every stage's output recomputed by
/// [`ifelse_pipe_ans`] and checked against what the pipeline actually
/// collected, over `N` from `1` to upstream's own `maxN - 1`.
fn ifelse_data_pipelines(num_lines: usize, num_workers: usize) {
    serialized(|| {
        let executor = Executor::new(num_workers);
        const IFELSE_MAX_N: usize = 200;
        let source = Arc::new(pseudo_random_sources(IFELSE_MAX_N));

        for n in 1..IFELSE_MAX_N {
            let mut taskflow = Taskflow::new();
            let collection: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

            let pipe0 = {
                let source = Arc::clone(&source);
                serial_pipe_with_flow::<(), i32>(move |(), pf| {
                    if pf.token() == n {
                        pf.stop();
                        return 0;
                    }
                    let value = source[pf.token()];
                    if value / 2 == 0 { value } else { value + 8 }
                })
            };

            let pipe1 = parallel_pipe::<i32, i32>(|value| {
                if value > 4897 {
                    value - 1834
                } else {
                    value + 3
                }
            });

            let pipe2 = {
                let collection = Arc::clone(&collection);
                serial_pipe::<i32, ()>(move |value| {
                    let tmp = if (value + 9) / 4 < 50 {
                        value + 1
                    } else {
                        value + 17
                    };
                    collection.lock().expect("uncontended").push(tmp);
                })
            };

            let pipeline = DataPipeline::builder(num_lines)
                .pipe(pipe0)
                .pipe(pipe1)
                .pipe(pipe2)
                .build();

            compose_module(&mut taskflow, &pipeline, "module_of_data_pipeline");

            executor.run(&taskflow).wait().expect("no task panicked");

            assert_ifelse_answers(&collection, &source, n);
        }
    });
}

worker_matrix! {
    ifelse_data_pipelines {
        ifelse_data_pipelines_1l_1w(1, 1),
        ifelse_data_pipelines_1l_2w(1, 2),
        ifelse_data_pipelines_1l_3w(1, 3),
        ifelse_data_pipelines_1l_4w(1, 4),
        ifelse_data_pipelines_3l_1w(3, 1),
        ifelse_data_pipelines_3l_2w(3, 2),
        ifelse_data_pipelines_3l_3w(3, 3),
        ifelse_data_pipelines_3l_4w(3, 4),
        ifelse_data_pipelines_5l_1w(5, 1),
        ifelse_data_pipelines_5l_2w(5, 2),
        ifelse_data_pipelines_5l_3w(5, 3),
        ifelse_data_pipelines_5l_4w(5, 4),
        ifelse_data_pipelines_7l_1w(7, 1),
        ifelse_data_pipelines_7l_2w(7, 2),
        ifelse_data_pipelines_7l_3w(7, 3),
        ifelse_data_pipelines_7l_4w(7, 4),
    }
}

// ----------------------------------------------------------------------------
// PipelineinPipeline.DataPipelines: a 4-pipe (`SPSS`) outer data pipeline
// whose first three pipes each build, run and tear down a fresh 3-pipe
// (`SPS`) inner data pipeline of their own, through `Executor::corun`.
// ----------------------------------------------------------------------------

/// Builds, runs through `executor.corun` and tears down one `SPS` sub-data
/// pipeline of `sub_n` tokens, checking every value a sub-pipe reads is
/// exactly what the one before it just produced, then returns the sum of
/// what its last pipe collected, each one `third_offset` more than its own
/// running index.
///
/// Upstream's own three outer pipes that each build one of these differ
/// only in that offset (`3`, `13`, `7`), the same three values
/// `pipeline_in_pipeline`'s own checks multiply by `sub_n` -- this file's
/// module docs say why its own middle sub-pipe check reads `subpf.token()`
/// rather than upstream's `source[j2]` in the second of the three.
fn run_subpipeline(executor: &Executor, sub_lines: usize, sub_n: usize, third_offset: i32) -> i32 {
    let subj1 = Arc::new(AtomicUsize::new(0));
    let subj2 = Arc::new(AtomicUsize::new(0));
    let subj3 = Arc::new(AtomicUsize::new(0));
    let subcollection: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

    let subpipe0 = {
        let subj1 = Arc::clone(&subj1);
        serial_pipe_with_flow::<(), i32>(move |(), subpf| {
            let current = subj1.load(Ordering::Relaxed);
            if current == sub_n {
                subpf.stop();
                return 0;
            }
            assert_eq!(subpf.token() % sub_lines, subpf.line());
            subj1.fetch_add(1, Ordering::Relaxed);
            i32::try_from(current + 1).expect("a sub-token count fits in an i32")
        })
    };

    let subpipe1 = {
        let subj2 = Arc::clone(&subj2);
        parallel_pipe_with_flow::<i32, i32>(move |value, subpf| {
            let ticket = subj2.fetch_add(1, Ordering::Relaxed);
            assert!(ticket < sub_n);
            assert_eq!(subpf.token() % sub_lines, subpf.line());
            let expected =
                i32::try_from(subpf.token() + 1).expect("a sub-token count fits in an i32");
            assert_eq!(expected, value);
            value
        })
    };

    let subpipe2 = {
        let subj3 = Arc::clone(&subj3);
        let subcollection = Arc::clone(&subcollection);
        serial_pipe_with_flow::<i32, ()>(move |value, subpf| {
            let current = subj3.load(Ordering::Relaxed);
            assert!(current < sub_n);
            assert_eq!(subpf.token() % sub_lines, subpf.line());
            let expected = i32::try_from(current + 1).expect("a sub-token count fits in an i32");
            assert_eq!(expected, value);
            let value =
                i32::try_from(current).expect("a sub-token count fits in an i32") + third_offset;
            subcollection.lock().expect("uncontended").push(value);
            subj3.fetch_add(1, Ordering::Relaxed);
        })
    };

    let subpipeline = DataPipeline::builder(sub_lines)
        .pipe(subpipe0)
        .pipe(subpipe1)
        .pipe(subpipe2)
        .build();

    let mut inner_taskflow = Taskflow::new();
    compose_module(&mut inner_taskflow, &subpipeline, "module_of_subpipeline");

    executor
        .corun(&inner_taskflow)
        .expect("a worker of this executor may corun the sub-pipeline");

    sub_pipeline_sum([&subj1, &subj2, &subj3], &subcollection, sub_n)
}

/// Upstream's `pipeline_in_pipeline(L, w, subL)`: an outer `SPSS` data
/// pipeline of `L` lines over `N` tokens, whose first three pipes each
/// [`run_subpipeline`] -- a fresh `SPS` inner data pipeline of `subL` lines
/// over `subN` tokens, run through `executor.corun` from inside the outer
/// pipe's own callable, with the `3`, `13` and `7` offsets upstream's own
/// three sub-pipelines use -- and whose fourth checks the last one's output
/// against the independently recomputed [`partial_sum`].
fn pipeline_in_pipeline(num_lines: usize, num_workers: usize, sub_lines: usize) {
    serialized(|| {
        let executor = Arc::new(Executor::new(num_workers));

        const PIP_MAX_N: usize = 5;
        const PIP_MAX_SUB_N: usize = 4;

        for n in 1..PIP_MAX_N {
            for sub_n in 1..PIP_MAX_SUB_N {
                let j1 = Arc::new(AtomicUsize::new(0));
                let j2 = Arc::new(AtomicUsize::new(0));
                let j3 = Arc::new(AtomicUsize::new(0));
                let j4 = Arc::new(AtomicUsize::new(0));

                let pipe0 = {
                    let j1 = Arc::clone(&j1);
                    let executor = Arc::clone(&executor);
                    serial_pipe_with_flow::<(), i32>(move |(), pf| {
                        let current = j1.load(Ordering::Relaxed);
                        if current == n {
                            pf.stop();
                            return 0;
                        }
                        let sum = run_subpipeline(&executor, sub_lines, sub_n, 3);
                        j1.fetch_add(1, Ordering::Relaxed);
                        sum
                    })
                };

                let pipe1 = {
                    let j2 = Arc::clone(&j2);
                    let executor = Arc::clone(&executor);
                    parallel_pipe_with_flow::<i32, i32>(move |input, _pf| {
                        let ticket = j2.fetch_add(1, Ordering::Relaxed);
                        assert!(ticket < n);
                        let offset =
                            i32::try_from(sub_n).expect("a sub-token count fits in an i32");
                        assert_eq!(input, partial_sum(sub_n) + 3 * offset);
                        run_subpipeline(&executor, sub_lines, sub_n, 13)
                    })
                };

                let pipe2 = {
                    let j3 = Arc::clone(&j3);
                    let executor = Arc::clone(&executor);
                    serial_pipe_with_flow::<i32, i32>(move |input, _pf| {
                        let ticket = j3.fetch_add(1, Ordering::Relaxed);
                        assert!(ticket < n);
                        let offset =
                            i32::try_from(sub_n).expect("a sub-token count fits in an i32");
                        assert_eq!(input, partial_sum(sub_n) + 13 * offset);
                        run_subpipeline(&executor, sub_lines, sub_n, 7)
                    })
                };

                let pipe3 = {
                    let j4 = Arc::clone(&j4);
                    serial_pipe::<i32, ()>(move |input| {
                        let offset =
                            i32::try_from(sub_n).expect("a sub-token count fits in an i32");
                        assert_eq!(input, partial_sum(sub_n) + 7 * offset);
                        j4.fetch_add(1, Ordering::Relaxed);
                    })
                };

                let pipeline = DataPipeline::builder(num_lines)
                    .pipe(pipe0)
                    .pipe(pipe1)
                    .pipe(pipe2)
                    .pipe(pipe3)
                    .build();

                let mut taskflow = Taskflow::new();
                compose_module(&mut taskflow, &pipeline, "module_of_pipeline");

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

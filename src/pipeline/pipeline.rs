//! Translated from `taskflow/algorithm/pipeline.hpp` (Taskflow v4.1.0).
//!
//! [`Pipeline`] is upstream's `tf::Pipeline`: a fixed chain of [`Pipe`]s run
//! over `num_lines` parallel lines, composed into another graph as a module
//! task. `DESIGN.md`'s "Decisions taken in M8" section covers the three
//! choices the whole family is built on -- why a line is an ordinary runtime
//! task, why a pipe callable's bound forces the pipeline's state to live
//! behind an `Arc` of atomics, and why this one type stands for both of
//! upstream's `Pipeline` and `ScalablePipeline`.
//!
//! # Design
//!
//! **A `Pipeflow` per line sits behind an uncontended `Mutex`, not an
//! `UnsafeCell`.** Upstream reaches every line's `Pipeflow` through a raw
//! pointer `_build`'s closure captures, sound there because the join-counter
//! protocol below -- the two `fetch_sub`s a line task does before it loops or
//! returns -- already guarantees that at most one line task is ever reading
//! or writing a given line's token at a time; nothing upstream's compiler
//! enforces, only the protocol. [`Pipeflow`] already carries its fields
//! `pub(crate)` for exactly this scheduler, so a `Mutex<Pipeflow>` per line
//! costs nothing a `&mut` through an `UnsafeCell` would not also have paid in
//! review effort, and it costs it to the type system instead of to a reader
//! re-deriving the protocol: every access is an ordinary `lock()`, poisoning
//! is tolerated the way every other shared lock in this crate tolerates it,
//! and the lock is never contended because the protocol that makes an
//! `UnsafeCell` sound here is the same protocol that keeps two line tasks
//! from ever locking the same line at once. The crate's two `unsafe` modules
//! are named in the README and neither is this one.
//!
//! **The line body is upstream's `_build` loop, with the `goto` as a
//! `loop`.** A line task's closure starts at whichever line it was built
//! for and may, on the loop's "downward" branch, carry on processing a
//! *different* line's token in the same call -- upstream's `pf = &_pipeflows[n_l]`
//! reassigning the pointer the loop walks rather than the task that is
//! walking it. [`run_line`] below is that function; `active` is upstream's
//! `pf->_line`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use crate::flow_builder::{Composable, FlowBuilder};
use crate::graph::Task;
use crate::pipeline::pipe::{Pipe, PipeType, Pipeflow};
use crate::runtime::Runtime;
use crate::taskflow::Taskflow;

/// A pipeline scheduling framework: upstream's `tf::Pipeline` (and, this
/// port's one type standing for both, `tf::ScalablePipeline`).
///
/// A pipeline schedules a stream of tokens through a fixed chain of [`Pipe`]s
/// over up to [`Pipeline::num_lines`] of them in flight at once, each line a
/// runtime task of the graph [`Pipeline::taskflow`] returns. Composing that
/// graph into another taskflow -- `taskflow.composed_of(&pipeline)` -- is how
/// a pipeline runs; a [`Pipeline`] built and never composed runs nothing.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicUsize, Ordering};
///
/// use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
/// use rustflow::pipeline::scheduler::Pipeline;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// // Two lines, one serial pipe that stops after five tokens and records
/// // each one it let through.
/// let seen = Arc::new(AtomicUsize::new(0));
/// let counted = Arc::clone(&seen);
/// let pipe = Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
///     if pf.token() == 5 {
///         pf.stop();
///         return;
///     }
///     counted.fetch_add(1, Ordering::Relaxed);
/// });
/// let pipeline = Pipeline::new(2, vec![pipe]);
///
/// let mut taskflow = Taskflow::new();
/// let module = taskflow.composed_of(&pipeline);
/// let after = taskflow.emplace(|| {});
/// taskflow.task(module).precede([after]);
///
/// let executor = Executor::new(2);
/// executor.run(&taskflow).wait().expect("no task panicked");
///
/// assert_eq!(pipeline.num_tokens(), 5);
/// assert_eq!(seen.load(Ordering::Relaxed), 5);
/// ```
#[derive(Debug)]
pub struct Pipeline {
    taskflow: Arc<Taskflow>,
    state: Arc<State>,
}

/// A pipeline's state, shared by every line task of its graph: upstream's
/// `Pipeline::_lines`, `_pipeflows`, `_num_tokens` and `_pipes` together,
/// behind the one `Arc` [`Pipeline`] and every line's closure hold a share
/// of -- `DESIGN.md`'s "Decisions taken in M8" says why one allocation
/// rather than upstream's `this`.
#[derive(Debug)]
struct State {
    /// The number of parallel lines: upstream's `_pipeflows.size()`.
    num_lines: usize,
    /// The pipeline's own chain of pipes: upstream's `_pipes` (and, since
    /// there is one type for both of upstream's templates, also its
    /// `_meta`, read off each pipe's own [`PipeType`] rather than kept
    /// twice).
    pipes: Vec<Pipe>,
    /// The number of tokens generated so far: upstream's `_num_tokens`.
    num_tokens: AtomicUsize,
    /// The `lines * pipes` join counters, flattened row-major by line:
    /// upstream's `_lines[line][pipe].join_counter`.
    join_counters: Vec<AtomicUsize>,
    /// One scheduling token per line: upstream's `_pipeflows`.
    pipeflows: Vec<Mutex<Pipeflow>>,
    /// The line tasks of the graph built for this state, in line order:
    /// upstream's `_tasks[1..]`. Filled once, after the graph is built and
    /// before the first run can reach [`State::line_task`] -- a run cannot
    /// start before [`Pipeline::new`] returns the value that starts it.
    line_tasks: OnceLock<Vec<Task>>,
}

impl State {
    /// The number of pipes in the chain: upstream's `num_pipes()`.
    fn num_pipes(&self) -> usize {
        self.pipes.len()
    }

    /// The join counter for `line`'s `pipe`: upstream's
    /// `_lines[line][pipe].join_counter`.
    fn join_counter(&self, line: usize, pipe: usize) -> &AtomicUsize {
        &self.join_counters[line * self.num_pipes() + pipe]
    }

    /// The runtime task that runs `line`: upstream's `_tasks[line + 1]`.
    ///
    /// # Panics
    ///
    /// Panics if called before [`Pipeline::new`] has recorded the line
    /// tasks it built, which cannot happen through any path a caller of
    /// this module reaches -- every [`State`] a line task's closure can run
    /// against is one [`Pipeline::new`] has already finished building.
    fn line_task(&self, line: usize) -> Task {
        self.line_tasks
            .get()
            .expect("a pipeline's line tasks are set before any of them can run")[line]
    }

    /// Resets this pipeline's progress to its just-built state: upstream's
    /// `reset()`.
    ///
    /// The token counter goes to zero, every line's [`Pipeflow`] goes back
    /// to pipe `0` on its own line, and the join counters go back to the
    /// values that let line `0` start at pipe `0` immediately while every
    /// other line and pipe waits on exactly what upstream's own table says
    /// it should.
    fn reset(&self) {
        self.num_tokens.store(0, Ordering::Relaxed);

        for (line, slot) in self.pipeflows.iter().enumerate() {
            let mut pipeflow = lock_pipeflow(slot);
            pipeflow.pipe = 0;
            pipeflow.line = line;
        }

        self.join_counter(0, 0).store(0, Ordering::Relaxed);

        for line in 1..self.num_lines {
            for pipe in 1..self.num_pipes() {
                self.join_counter(line, pipe)
                    .store(self.pipes[pipe].pipe_type().join_counter(), Ordering::Relaxed);
            }
        }

        for pipe in 1..self.num_pipes() {
            self.join_counter(0, pipe).store(1, Ordering::Relaxed);
        }

        for line in 1..self.num_lines {
            // Upstream's `static_cast<size_t>(_meta[0].type) - 1`.
            // `Pipeline::new` has already checked the first pipe is serial,
            // so this is always `2 - 1 == 1`; written as the subtraction
            // anyway, to read the same as the value it is standing for.
            self.join_counter(line, 0)
                .store(self.pipes[0].pipe_type().join_counter() - 1, Ordering::Relaxed);
        }
    }
}

/// Locks `slot`, tolerating poison: every lock in this module is uncontended
/// by the protocol [`State`]'s docs give, so a poisoned lock holds a value no
/// panic ever left mid-update.
fn lock_pipeflow(slot: &Mutex<Pipeflow>) -> MutexGuard<'_, Pipeflow> {
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Pipeline {
    /// Builds a pipeline of up to `num_lines` parallel lines over `pipes`:
    /// upstream's two constructors, collapsed into one since a `Vec<Pipe>`
    /// already covers both a fixed list and a runtime-sized one.
    ///
    /// # Panics
    ///
    /// Panics with upstream's own messages: if `num_lines` is `0` ("must
    /// have at least one line"), if `pipes` is empty ("must have at least
    /// one pipe" -- upstream's `static_assert`, a runtime check here since
    /// the chain's length is no longer known at compile time), or if the
    /// first pipe is not [`PipeType::Serial`] ("first pipe must be
    /// serial").
    #[must_use]
    pub fn new(num_lines: usize, pipes: Vec<Pipe>) -> Self {
        assert!(num_lines > 0, "must have at least one line");
        assert!(!pipes.is_empty(), "must have at least one pipe");
        assert!(
            pipes[0].pipe_type() == PipeType::Serial,
            "first pipe must be serial"
        );

        let num_pipes = pipes.len();
        let state = Arc::new(State {
            num_lines,
            pipes,
            num_tokens: AtomicUsize::new(0),
            join_counters: (0..num_lines * num_pipes).map(|_| AtomicUsize::new(0)).collect(),
            pipeflows: (0..num_lines).map(|_| Mutex::new(Pipeflow::default())).collect(),
            line_tasks: OnceLock::new(),
        });
        state.reset();

        let mut taskflow = Taskflow::new();

        let cond = {
            let state = Arc::clone(&state);
            taskflow.emplace_condition(move || {
                let selected = state.num_tokens.load(Ordering::Relaxed) % state.num_lines;
                i32::try_from(selected).expect("a line index fits in an i32")
            })
        };
        taskflow.task(cond).name("cond");

        let line_tasks: Vec<Task> = (0..num_lines)
            .map(|line| {
                let state = Arc::clone(&state);
                let task = taskflow
                    .emplace_runtime(move |rt: &mut Runtime<'_>| run_line(&state, line, rt));
                taskflow.task(task).name(format!("rt-{line}"));
                task
            })
            .collect();

        taskflow.task(cond).precede(line_tasks.iter().copied());

        state
            .line_tasks
            .set(line_tasks)
            .expect("a pipeline's line tasks are set exactly once, during construction");

        Self {
            taskflow: Arc::new(taskflow),
            state,
        }
    }

    /// The number of parallel lines this pipeline was built with: upstream's
    /// `num_lines`.
    #[must_use]
    pub fn num_lines(&self) -> usize {
        self.state.num_lines
    }

    /// The number of pipes in this pipeline's chain: upstream's
    /// `num_pipes`.
    #[must_use]
    pub fn num_pipes(&self) -> usize {
        self.state.num_pipes()
    }

    /// The number of tokens this pipeline has generated so far: upstream's
    /// `num_tokens`.
    #[must_use]
    pub fn num_tokens(&self) -> usize {
        self.state.num_tokens.load(Ordering::Relaxed)
    }

    /// Resets this pipeline's progress, as if it had just been built:
    /// upstream's `reset`. The token counter returns to `0` and every
    /// line's scheduling token returns to pipe `0` on its own line.
    ///
    /// Not safe to call while a run of a taskflow this pipeline is composed
    /// into is in flight, which upstream's own `reset` does not guard
    /// against either.
    pub fn reset(&self) {
        self.state.reset();
    }

    /// Returns a share of the taskflow this pipeline composes into a module
    /// task: upstream's `graph()`, widened from a `Graph&` to the
    /// [`Taskflow`] that owns it, which is what [`Composable`] needs.
    #[must_use]
    pub fn taskflow(&self) -> Arc<Taskflow> {
        Arc::clone(&self.taskflow)
    }
}

impl Composable for &Pipeline {
    /// Delegates to [`Pipeline::taskflow`]; implemented for a reference
    /// rather than for [`Pipeline`] itself so that composing one does not
    /// consume it -- a caller reads [`Pipeline::num_tokens`] after the run
    /// that produced them, as every structural test in `tests/pipelines.rs`
    /// does.
    fn taskflow(&self) -> Arc<Taskflow> {
        Arc::clone(&self.taskflow)
    }
}

/// Runs the line task built for `line`: upstream's `_build`'s per-line
/// lambda, with the `goto pipeline` as the `loop` below.
///
/// `active` is upstream's `pf->_line` -- the line whose [`Pipeflow`] this
/// call is currently advancing, which starts as `line` and may become a
/// different one on the "downward" branch, exactly as upstream's `pf`
/// pointer may come to alias a different line's token without this call
/// ever becoming a different *task*.
fn run_line(state: &Arc<State>, line: usize, rt: &mut Runtime<'_>) {
    let mut active = line;

    loop {
        let current_pipe;
        let next_pipe;
        let next_line;

        {
            let mut pipeflow = lock_pipeflow(&state.pipeflows[active]);
            current_pipe = pipeflow.pipe();

            // Upstream sets this line's own join counter for the pipe it is
            // about to run back to its type's value *before* running the
            // pipe, so the next token to reach this (line, pipe) pair has
            // something to count down from.
            state
                .join_counter(active, current_pipe)
                .store(state.pipes[current_pipe].pipe_type().join_counter(), Ordering::Relaxed);

            if current_pipe == 0 {
                let token = state.num_tokens.load(Ordering::Relaxed);
                pipeflow.token = token;
                pipeflow.stop = false;
                state.pipes[0].invoke(&mut pipeflow);
                if pipeflow.stop {
                    // Other lines may still be finishing their own last
                    // stages, so this is this line's own task returning,
                    // not the pipeline stopping outright.
                    return;
                }
                state.num_tokens.fetch_add(1, Ordering::Relaxed);
            } else {
                state.pipes[current_pipe].invoke(&mut pipeflow);
            }

            next_pipe = (current_pipe + 1) % state.num_pipes();
            next_line = (active + 1) % state.num_lines;
            pipeflow.pipe = next_pipe;
        }

        // The shared `current_pipe`/`next_pipe`/`next_line` above must not
        // change past this point, upstream's own comment on `f`: whichever
        // of the two dependencies below is spawned first may run to
        // completion, including its own read of these values for its own
        // next token, before the other dependency is even computed.
        let mut ready = 0_u8;
        let mut downward_ready = false;

        if state.pipes[current_pipe].pipe_type() == PipeType::Serial
            && state
                .join_counter(next_line, current_pipe)
                .fetch_sub(1, Ordering::AcqRel)
                == 1
        {
            downward_ready = true;
            ready += 1;
        }

        if state
            .join_counter(active, next_pipe)
            .fetch_sub(1, Ordering::AcqRel)
            == 1
        {
            ready += 1;
        }

        match ready {
            0 => return,
            1 => {
                if downward_ready {
                    active = next_line;
                }
                // Otherwise the forward dependency is the one that is
                // ready, and `active` stays put: `pipeflow.pipe` was
                // already advanced to `next_pipe` above.
            }
            2 => rt.schedule(state.line_task(next_line)),
            _ => unreachable!("at most two dependencies become ready per token"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::flow_builder::FlowBuilder;
    use crate::pipeline::pipe::{Pipe, PipeType, Pipeflow};
    use crate::taskflow::Taskflow;
    use crate::Executor;

    #[test]
    #[should_panic(expected = "must have at least one line")]
    fn zero_lines_panics() {
        let _ = Pipeline::new(0, vec![Pipe::new(PipeType::Serial, |_: &mut Pipeflow| {})]);
    }

    #[test]
    #[should_panic(expected = "must have at least one pipe")]
    fn zero_pipes_panics() {
        let _ = Pipeline::new(1, vec![]);
    }

    #[test]
    #[should_panic(expected = "first pipe must be serial")]
    fn a_parallel_first_pipe_panics() {
        let _ = Pipeline::new(1, vec![Pipe::new(PipeType::Parallel, |_: &mut Pipeflow| {})]);
    }

    #[test]
    fn accessors_read_back_what_new_was_given() {
        let pipeline = Pipeline::new(
            3,
            vec![
                Pipe::new(PipeType::Serial, |_: &mut Pipeflow| {}),
                Pipe::new(PipeType::Parallel, |_: &mut Pipeflow| {}),
            ],
        );
        assert_eq!(pipeline.num_lines(), 3);
        assert_eq!(pipeline.num_pipes(), 2);
        assert_eq!(pipeline.num_tokens(), 0);
    }

    #[test]
    fn running_the_composed_module_advances_token_count_then_reset_returns_to_zero() {
        let seen = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&seen);
        let pipe = Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            if pf.token() == 4 {
                pf.stop();
                return;
            }
            counted.fetch_add(1, Ordering::Relaxed);
        });
        let pipeline = Pipeline::new(2, vec![pipe]);

        let mut taskflow = Taskflow::new();
        let module = taskflow.composed_of(&pipeline);
        taskflow.task(module).name("module_of_pipeline");

        let executor = Executor::new(2);
        executor.run(&taskflow).wait().expect("no task panicked");

        assert_eq!(pipeline.num_tokens(), 4);
        assert_eq!(seen.load(Ordering::Relaxed), 4);

        pipeline.reset();
        assert_eq!(pipeline.num_tokens(), 0);

        seen.store(0, Ordering::Relaxed);
        executor.run(&taskflow).wait().expect("no task panicked");
        assert_eq!(pipeline.num_tokens(), 4);
        assert_eq!(seen.load(Ordering::Relaxed), 4);
    }
}

//! Translated from `taskflow/algorithm/pipeline.hpp` (Taskflow v4.1.0).
//!
//! [`Pipeline`] is upstream's `tf::Pipeline`: a fixed chain of [`Pipe`]s run
//! over `num_lines` parallel lines, composed into another graph as a module
//! task. `DESIGN.md`'s "Pipelines" section covers the three
//! choices the whole family is built on -- why a line is a
//! [`Handle::NonpreemptiveRuntime`](crate::graph::Handle::NonpreemptiveRuntime)
//! task rather than an ordinary [`Handle::Runtime`](crate::graph::Handle::Runtime)
//! one, why a pipe callable's bound forces the pipeline's state to live
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
//! walking it. `run_line` below is that function; `active` is upstream's
//! `pf->_line`.
//!
//! **[`Pipeline::reset_pipes`] and [`Pipeline::reset_lines`] rebuild the
//! whole of [`Pipeline::taskflow`] and the `State` behind it, rather than
//! mutating the one already built.** Upstream's two `reset` overloads
//! -- `reset(first, last)`, `reset(num_lines, first, last)` -- read and
//! write `_pipes`, `_lines` and, for the second, `_graph`/`_tasks` straight
//! through the `this` pointer every already-built line task's lambda
//! captures, so a reset upstream performs is visible to a module task that
//! composed this same object *before* the reset ran, with no rebuild of
//! that module task needed. A line task's closure here instead captures an
//! `Arc<State>` by value, and [`FlowBuilder::composed_of`]
//! hands out a clone of [`Pipeline::taskflow`]'s `Arc` that outlives the
//! call -- so there is no single allocation this port could mutate in place
//! that both forms could reach through every already-composed module task
//! the way upstream's `this` does, not without `unsafe` aliasing of exactly
//! the kind `DESIGN.md`'s porting posture asks a port to design around
//! rather than reproduce.
//!
//! The safe shape both methods take instead: build a fresh graph and a
//! fresh `State` -- [`Pipeline::new`]'s own construction, factored out as
//! `Pipeline::build` -- and replace `self.taskflow`/`self.state` with them.
//! `reset_pipes` passes the current [`Pipeline::num_lines`] through
//! unchanged; `reset_lines` passes the new one. Composing this pipeline
//! (`taskflow.composed_of(&pipeline)`) *before* calling either keeps that
//! composition pointing at the pre-reset graph, which keeps running exactly
//! as it did; compose again afterwards to run the new shape. Every call site
//! in upstream's own `test_scalable_pipelines.cpp` resets before its one
//! `composed_of` of a given object, never after, so this is not a loss
//! against how upstream is actually used -- `tests/scalable_pipelines.rs`'s
//! own module doc says so again, for the tests that exercise it.
//!
//! This also settles what a resized line count does to `State::line_tasks`:
//! nothing, because `reset_lines` never reuses the `State` that field is
//! on. Each call to `Pipeline::build` makes its own `OnceLock`, set exactly
//! once during that call, same as [`Pipeline::new`]'s always has been.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use crate::executor::runtime::NonpreemptiveRuntime;
use crate::flow_builder::{Composable, FlowBuilder};
use crate::graph::Task;
use crate::pipeline::pipe::{Pipe, PipeType, Pipeflow};
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
/// of -- `DESIGN.md`'s "Pipelines" section says why one allocation
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
                self.join_counter(line, pipe).store(
                    self.pipes[pipe].pipe_type().join_counter(),
                    Ordering::Relaxed,
                );
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
            self.join_counter(line, 0).store(
                self.pipes[0].pipe_type().join_counter() - 1,
                Ordering::Relaxed,
            );
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
    /// Builds a fresh graph and [`State`] of `num_lines` parallel lines over
    /// `pipes`: the shared substance of [`Pipeline::new`],
    /// [`Pipeline::reset_pipes`] and [`Pipeline::reset_lines`], none of which
    /// differ in anything below this -- only in whether `num_lines` is the
    /// caller's own [`Pipeline::num_lines`] or a new one, and in what they do
    /// with the pair this returns.
    ///
    /// # Panics
    ///
    /// Panics with upstream's own messages: if `num_lines` is `0` ("must
    /// have at least one line"), if `pipes` is empty ("must have at least
    /// one pipe" -- upstream's `static_assert`, a runtime check here since
    /// the chain's length is no longer known at compile time), or if the
    /// first pipe is not [`PipeType::Serial`] ("first pipe must be
    /// serial").
    fn build(num_lines: usize, pipes: Vec<Pipe>) -> (Arc<Taskflow>, Arc<State>) {
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
            join_counters: (0..num_lines * num_pipes)
                .map(|_| AtomicUsize::new(0))
                .collect(),
            pipeflows: (0..num_lines)
                .map(|_| Mutex::new(Pipeflow::default()))
                .collect(),
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
                let task =
                    taskflow.emplace_nonpreemptive_runtime(move |rt| run_line(&state, line, rt));
                taskflow.task(task).name(format!("rt-{line}"));
                task
            })
            .collect();

        taskflow.task(cond).precede(line_tasks.iter().copied());

        state
            .line_tasks
            .set(line_tasks)
            .expect("a pipeline's line tasks are set exactly once, during construction");

        (Arc::new(taskflow), state)
    }

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
        let (taskflow, state) = Self::build(num_lines, pipes);
        Self { taskflow, state }
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

    /// Rebuilds this pipeline over a new chain of pipes, keeping its number
    /// of lines: upstream's `reset(first, last)`, folded with its sibling
    /// `ScalablePipeline(num_lines, first, last)` constructor the way
    /// [`Pipeline::new`] already folds upstream's two. `pipes` is upstream's
    /// `[first, last)`, already collected -- the port owns its pipes rather
    /// than aliasing a caller's range, so there is no separate "assign a new
    /// range" step for this method to need. Every line starts again at pipe
    /// `0` and the token counter returns to `0`, as a fresh [`Pipeline::new`]
    /// would report.
    ///
    /// Composing this pipeline into a graph (`taskflow.composed_of(&pipeline)`)
    /// before calling this method leaves that composition pointing at the
    /// pre-reset graph; this module's `# Design` section says why and what
    /// to do about it.
    ///
    /// # Panics
    ///
    /// Panics with upstream's own messages: if `pipes` is empty ("must have
    /// at least one pipe") or if the first pipe is not [`PipeType::Serial`]
    /// ("first pipe must be serial"). [`Pipeline::num_lines`] is not
    /// re-checked, already having been checked nonzero by whichever call
    /// established it.
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
    /// // Compose, run, reset the chain, then compose *again*: upstream's
    /// // `scalable_pipeline.cpp` runs its one already-composed task a
    /// // second time instead, which is the line a faithful translation has
    /// // to be rewritten at.
    ///
    /// // One serial pipe that stops after three tokens, counting each one
    /// // it lets through into `counter`.
    /// fn counting_pipe(counter: &Arc<AtomicUsize>) -> Pipe {
    ///     let counted = Arc::clone(counter);
    ///     Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
    ///         if pf.token() == 3 {
    ///             pf.stop();
    ///             return;
    ///         }
    ///         counted.fetch_add(1, Ordering::Relaxed);
    ///     })
    /// }
    ///
    /// let first = Arc::new(AtomicUsize::new(0));
    /// let second = Arc::new(AtomicUsize::new(0));
    /// let executor = Executor::new(2);
    ///
    /// let mut pipeline = Pipeline::new(2, vec![counting_pipe(&first)]);
    ///
    /// let mut taskflow = Taskflow::new();
    /// let module = taskflow.composed_of(&pipeline);
    /// taskflow.task(module).name("module_of_pipeline");
    /// executor.run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(first.load(Ordering::Relaxed), 3);
    ///
    /// // The reset builds a fresh graph, so `taskflow` above still runs the
    /// // old chain: a second composition is what reaches the new one.
    /// pipeline.reset_pipes(vec![counting_pipe(&second)]);
    ///
    /// let mut again = Taskflow::new();
    /// let module = again.composed_of(&pipeline);
    /// again.task(module).name("module_of_reset_pipeline");
    /// executor.run(&again).wait().expect("no task panicked");
    ///
    /// assert_eq!(second.load(Ordering::Relaxed), 3);
    /// assert_eq!(first.load(Ordering::Relaxed), 3);
    /// assert_eq!(pipeline.num_tokens(), 3);
    /// ```
    pub fn reset_pipes(&mut self, pipes: Vec<Pipe>) {
        let (taskflow, state) = Self::build(self.state.num_lines, pipes);
        self.taskflow = taskflow;
        self.state = state;
    }

    /// Rebuilds this pipeline over a new number of lines and a new chain of
    /// pipes, as if it had just been built: upstream's
    /// `reset(num_lines, first, last)`. `pipes` is upstream's `[first,
    /// last)`, already collected, for the same reason
    /// [`Pipeline::reset_pipes`]'s is.
    ///
    /// Composing this pipeline into a graph (`taskflow.composed_of(&pipeline)`)
    /// before calling this method leaves that composition pointing at the
    /// pre-reset graph; this module's `# Design` section says why and what
    /// to do about it.
    ///
    /// # Panics
    ///
    /// Panics with upstream's own messages: if `num_lines` is `0` ("must
    /// have at least one line"), if `pipes` is empty ("must have at least
    /// one pipe"), or if the first pipe is not [`PipeType::Serial`] ("first
    /// pipe must be serial").
    pub fn reset_lines(&mut self, num_lines: usize, pipes: Vec<Pipe>) {
        let (taskflow, state) = Self::build(num_lines, pipes);
        self.taskflow = taskflow;
        self.state = state;
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
fn run_line(state: &Arc<State>, line: usize, rt: &mut NonpreemptiveRuntime<'_>) {
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
            state.join_counter(active, current_pipe).store(
                state.pipes[current_pipe].pipe_type().join_counter(),
                Ordering::Relaxed,
            );

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
    use crate::Executor;
    use crate::flow_builder::FlowBuilder;
    use crate::pipeline::pipe::{Pipe, PipeType, Pipeflow};
    use crate::taskflow::Taskflow;

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
        let _ = Pipeline::new(
            1,
            vec![Pipe::new(PipeType::Parallel, |_: &mut Pipeflow| {})],
        );
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

    #[test]
    #[should_panic(expected = "must have at least one pipe")]
    fn reset_pipes_on_an_empty_list_panics() {
        let mut pipeline =
            Pipeline::new(1, vec![Pipe::new(PipeType::Serial, |_: &mut Pipeflow| {})]);
        pipeline.reset_pipes(vec![]);
    }

    #[test]
    #[should_panic(expected = "first pipe must be serial")]
    fn reset_pipes_onto_a_parallel_first_pipe_panics() {
        let mut pipeline =
            Pipeline::new(1, vec![Pipe::new(PipeType::Serial, |_: &mut Pipeflow| {})]);
        pipeline.reset_pipes(vec![Pipe::new(PipeType::Parallel, |_: &mut Pipeflow| {})]);
    }

    #[test]
    #[should_panic(expected = "must have at least one line")]
    fn reset_lines_to_zero_panics() {
        let mut pipeline =
            Pipeline::new(1, vec![Pipe::new(PipeType::Serial, |_: &mut Pipeflow| {})]);
        pipeline.reset_lines(0, vec![Pipe::new(PipeType::Serial, |_: &mut Pipeflow| {})]);
    }

    /// [`Pipeline::reset_pipes`] replaces the chain a pipeline runs while
    /// keeping its line count: the new chain's shape is what
    /// [`Pipeline::num_pipes`] reports, and composing *after* the call runs
    /// the new chain, not the one the pipeline was built with.
    #[test]
    fn reset_pipes_keeps_the_line_count_and_runs_the_new_chain() {
        let seen = Arc::new(AtomicUsize::new(0));

        let mut pipeline =
            Pipeline::new(3, vec![Pipe::new(PipeType::Serial, |_: &mut Pipeflow| {})]);
        assert_eq!(pipeline.num_lines(), 3);
        assert_eq!(pipeline.num_pipes(), 1);

        let counted = Arc::clone(&seen);
        let stop_after_six = Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            if pf.token() == 6 {
                pf.stop();
                return;
            }
            counted.fetch_add(1, Ordering::Relaxed);
        });
        let double = Pipe::new(PipeType::Serial, |_: &mut Pipeflow| {});
        pipeline.reset_pipes(vec![stop_after_six, double]);

        assert_eq!(pipeline.num_lines(), 3);
        assert_eq!(pipeline.num_pipes(), 2);
        assert_eq!(pipeline.num_tokens(), 0);

        let mut taskflow = Taskflow::new();
        let module = taskflow.composed_of(&pipeline);
        taskflow.task(module).name("module_of_pipeline");

        let executor = Executor::new(2);
        executor.run(&taskflow).wait().expect("no task panicked");

        assert_eq!(pipeline.num_tokens(), 6);
        assert_eq!(seen.load(Ordering::Relaxed), 6);
    }

    /// [`Pipeline::reset_lines`] replaces both the line count and the chain:
    /// the rebuilt pipeline's accessors report the new shape, and a module
    /// composed from it after the call runs across the new number of lines.
    #[test]
    fn reset_lines_changes_both_the_line_count_and_the_chain() {
        let seen = Arc::new(AtomicUsize::new(0));

        let mut pipeline =
            Pipeline::new(1, vec![Pipe::new(PipeType::Serial, |_: &mut Pipeflow| {})]);
        assert_eq!(pipeline.num_lines(), 1);

        let counted = Arc::clone(&seen);
        let stop_after_nine = Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            if pf.token() == 9 {
                pf.stop();
                return;
            }
            assert_eq!(pf.token() % 5, pf.line());
            counted.fetch_add(1, Ordering::Relaxed);
        });
        pipeline.reset_lines(5, vec![stop_after_nine]);

        assert_eq!(pipeline.num_lines(), 5);
        assert_eq!(pipeline.num_pipes(), 1);
        assert_eq!(pipeline.num_tokens(), 0);

        let mut taskflow = Taskflow::new();
        let module = taskflow.composed_of(&pipeline);
        taskflow.task(module).name("module_of_pipeline");

        let executor = Executor::new(3);
        executor.run(&taskflow).wait().expect("no task panicked");

        assert_eq!(pipeline.num_tokens(), 9);
        assert_eq!(seen.load(Ordering::Relaxed), 9);
    }

    /// A pipeline composed into a graph before [`Pipeline::reset_lines`] is
    /// called leaves that composition running the pre-reset graph, exactly
    /// as the module doc says: this is the one place that claim is checked
    /// directly, with the second run's lines and token count coming out
    /// unchanged by a reset the already-composed module never sees.
    #[test]
    fn reset_lines_after_composing_does_not_affect_the_existing_composition() {
        let seen = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&seen);
        let pipe = Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            if pf.token() == 3 {
                pf.stop();
                return;
            }
            counted.fetch_add(1, Ordering::Relaxed);
        });
        let mut pipeline = Pipeline::new(2, vec![pipe]);

        let mut taskflow = Taskflow::new();
        let module = taskflow.composed_of(&pipeline);
        taskflow.task(module).name("module_of_pipeline");

        // Reset *after* composing: the module above keeps the taskflow it
        // was handed at compose time, not whatever `pipeline` points to now.
        pipeline.reset_lines(4, vec![Pipe::new(PipeType::Serial, |_: &mut Pipeflow| {})]);
        assert_eq!(pipeline.num_lines(), 4);

        let executor = Executor::new(2);
        executor.run(&taskflow).wait().expect("no task panicked");

        assert_eq!(seen.load(Ordering::Relaxed), 3);
    }
}

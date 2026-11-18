//! Translated from `taskflow/algorithm/data_pipeline.hpp` (Taskflow v4.1.0).
//!
//! [`DataPipe`] is a typed stage of a data-parallel pipeline, built by
//! [`serial_pipe`], [`serial_pipe_with_flow`], [`parallel_pipe`] or
//! [`parallel_pipe_with_flow`]: upstream's `tf::DataPipe` and
//! `tf::make_data_pipe` together. [`DataPipeline`] assembles a chain of them
//! through [`DataPipeline::builder`]'s typed [`DataPipelineBuilder`], which
//! checks each pipe's input type against its predecessor's output type at
//! compile time, then hands the chain to the [`Pipeline`] scheduler that
//! actually runs it.
//!
//! # Design
//!
//! `DESIGN.md`'s "Decisions taken in M8" section has why this chain is built
//! from two callable shapes rather than upstream's three, and what the slot
//! that carries a token's value from one pipe to the next costs.

use std::any::Any;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::flow_builder::Composable;
use crate::pipeline::pipe::{Pipe, PipeType, Pipeflow};
use crate::pipeline::scheduler::Pipeline;
use crate::taskflow::Taskflow;

/// What a line's slot holds between two pipes: `None` before the first pipe
/// of a token has run on that line, `Some` holding whichever pipe just ran
/// boxed up, for whichever pipe reads it next to downcast.
type SlotValue = Option<Box<dyn Any + Send>>;

/// One line's slot: upstream's `CachelineAligned<data_t>`, a `std::variant`
/// over every pipe's own output type. Rust has no anonymous union of an open
/// type list without a caller-supplied enum, so a type-erased box stands in
/// -- `DESIGN.md`'s "Decisions taken in M8" section has what that costs.
type Slot = Mutex<SlotValue>;

/// Locks `slot`, tolerating poison: a pipeline's own line protocol already
/// guarantees at most one pipe is ever touching a given line's slot at a
/// time -- the same guarantee [`scheduler`](crate::pipeline::scheduler)'s own
/// per-line `Mutex<Pipeflow>` rests on -- so contention never happens and a
/// poisoned lock never holds a half-written value.
fn lock_slot(slot: &Slot) -> MutexGuard<'_, SlotValue> {
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A [`DataPipe`]'s own callable, already normalized to the one shape every
/// constructor below builds: `DESIGN.md`'s "Decisions taken in M8" section
/// says why there is one shape here rather than upstream's three.
type Callable<I, O> = Box<dyn Fn(I, &mut Pipeflow) -> O + Send + Sync>;

/// A typed stage of a [`DataPipeline`]: upstream's `tf::DataPipe`.
///
/// Built by [`serial_pipe`], [`serial_pipe_with_flow`], [`parallel_pipe`] or
/// [`parallel_pipe_with_flow`] -- never directly, since its fields are
/// private -- and consumed by [`DataPipelineBuilder::pipe`], which checks `I`
/// against the chain's previous pipe's own output type before it will accept
/// one.
pub struct DataPipe<I, O> {
    kind: PipeType,
    callable: Callable<I, O>,
}

impl<I, O> DataPipe<I, O> {
    /// This pipe's direction: upstream's `type()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rustflow::pipeline::data_pipeline::serial_pipe;
    /// use rustflow::pipeline::pipe::PipeType;
    ///
    /// let pipe = serial_pipe::<i32, i32>(|value| value + 1);
    /// assert_eq!(pipe.pipe_type(), PipeType::Serial);
    /// ```
    #[must_use]
    pub fn pipe_type(&self) -> PipeType {
        self.kind
    }
}

impl<I, O> fmt::Debug for DataPipe<I, O> {
    /// The callable has no `Debug`, so only the direction is printed --
    /// [`Pipe`]'s own `Debug` is the same shape, for the same reason.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataPipe")
            .field("type", &self.kind)
            .finish()
    }
}

/// Builds a [`PipeType::Serial`] [`DataPipe`] from a callable that only needs
/// its input value: upstream's `make_data_pipe` paired with a
/// `(input_t&) -> output_t` callable.
///
/// `I` is `()` for a pipeline's first pipe, which has no predecessor to read
/// an input from; use [`serial_pipe_with_flow`] instead if that first pipe
/// also needs to call [`Pipeflow::stop`] or read where its token is.
///
/// # Examples
///
/// ```
/// use rustflow::pipeline::data_pipeline::serial_pipe;
///
/// let pipe = serial_pipe::<i32, String>(|value| value.to_string());
/// assert_eq!(pipe.pipe_type().join_counter(), 2);
/// ```
pub fn serial_pipe<I, O>(callable: impl Fn(I) -> O + Send + Sync + 'static) -> DataPipe<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    DataPipe {
        kind: PipeType::Serial,
        callable: Box::new(move |input, _pf: &mut Pipeflow| callable(input)),
    }
}

/// Builds a [`PipeType::Serial`] [`DataPipe`] from a callable that also needs
/// the running token's [`Pipeflow`]: upstream's `make_data_pipe` paired with a
/// `(input_t&, Pipeflow&) -> output_t` callable, and -- with `I = ()` -- its
/// `(Pipeflow&) -> output_t` one too. [`DataPipeline`]'s own docs say why
/// Rust has no need to keep that a third, separate shape: `()` already
/// stands for "no input" at the type level, so a pipeline's first pipe is
/// simply this function with `I = ()`, its callable's first parameter
/// pattern-matched away.
///
/// # Examples
///
/// ```
/// use rustflow::pipeline::data_pipeline::serial_pipe_with_flow;
///
/// let pipe = serial_pipe_with_flow::<(), i32>(|(), pf| {
///     if pf.token() == 5 {
///         pf.stop();
///         return 0;
///     }
///     1
/// });
/// assert_eq!(pipe.pipe_type().join_counter(), 2);
/// ```
pub fn serial_pipe_with_flow<I, O>(
    callable: impl Fn(I, &mut Pipeflow) -> O + Send + Sync + 'static,
) -> DataPipe<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    DataPipe {
        kind: PipeType::Serial,
        callable: Box::new(callable),
    }
}

/// Builds a [`PipeType::Parallel`] [`DataPipe`] from a callable that only
/// needs its input value: [`serial_pipe`]'s own docs, with the direction
/// swapped.
///
/// # Examples
///
/// ```
/// use rustflow::pipeline::data_pipeline::parallel_pipe;
///
/// let pipe = parallel_pipe::<i32, String>(|value| value.to_string());
/// assert_eq!(pipe.pipe_type().join_counter(), 1);
/// ```
pub fn parallel_pipe<I, O>(callable: impl Fn(I) -> O + Send + Sync + 'static) -> DataPipe<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    DataPipe {
        kind: PipeType::Parallel,
        callable: Box::new(move |input, _pf: &mut Pipeflow| callable(input)),
    }
}

/// Builds a [`PipeType::Parallel`] [`DataPipe`] from a callable that also
/// needs the running token's [`Pipeflow`]: [`serial_pipe_with_flow`]'s own
/// docs, with the direction swapped.
///
/// # Examples
///
/// ```
/// use rustflow::pipeline::data_pipeline::parallel_pipe_with_flow;
///
/// let pipe = parallel_pipe_with_flow::<i32, i32>(|value, pf| value + i32::try_from(pf.line()).unwrap());
/// assert_eq!(pipe.pipe_type().join_counter(), 1);
/// ```
pub fn parallel_pipe_with_flow<I, O>(
    callable: impl Fn(I, &mut Pipeflow) -> O + Send + Sync + 'static,
) -> DataPipe<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    DataPipe {
        kind: PipeType::Parallel,
        callable: Box::new(callable),
    }
}

/// A typed builder for a [`DataPipeline`]: checks every [`DataPipe`]'s input
/// type against the chain's previous output type at compile time, returned
/// by [`DataPipeline::builder`] and advanced one pipe at a time by
/// [`DataPipelineBuilder::pipe`].
///
/// `Prev` is the output type the next [`DataPipelineBuilder::pipe`] call must
/// accept as its input -- `()` on the builder [`DataPipeline::builder`]
/// itself returns, since a pipeline's first pipe has no predecessor.
pub struct DataPipelineBuilder<Prev> {
    /// The number of parallel lines the finished pipeline will have.
    num_lines: usize,
    /// The chain built so far, already type-erased: upstream's
    /// `std::tuple<Ps...>`, which this builder has no need to keep typed
    /// since every pipe's callable already closes over [`Self::slots`].
    pipes: Vec<Pipe>,
    /// One slot per line, shared with every pipe's own erased closure and,
    /// later, with the [`DataPipeline`] this builder finishes into.
    slots: Arc<Vec<Slot>>,
    /// Carries `Prev` with no runtime cost: nothing here stores a `Prev`
    /// value, only the type-check [`DataPipelineBuilder::pipe`] performs on
    /// its own input.
    _prev: PhantomData<Prev>,
}

impl<Prev: Send + 'static> DataPipelineBuilder<Prev> {
    /// Appends `next` to the chain, advancing what the following
    /// [`DataPipelineBuilder::pipe`] call must accept to `next`'s own output
    /// type: upstream's variadic constructor, one pipe at a time rather than
    /// through a parameter pack.
    ///
    /// # Examples
    ///
    /// ```
    /// use rustflow::pipeline::data_pipeline::{DataPipeline, serial_pipe};
    ///
    /// let pipeline = DataPipeline::builder(2)
    ///     .pipe(serial_pipe::<(), i32>(|()| 1))
    ///     .pipe(serial_pipe::<i32, String>(|value| value.to_string()))
    ///     .build();
    /// assert_eq!(pipeline.num_pipes(), 2);
    /// ```
    ///
    /// A pipe whose input does not match the chain's previous output is
    /// rejected at compile time, not accepted and wrong at run time:
    ///
    /// ```compile_fail
    /// use rustflow::pipeline::data_pipeline::{DataPipeline, serial_pipe};
    ///
    /// let _ = DataPipeline::builder(1)
    ///     .pipe(serial_pipe::<(), u32>(|()| 7_u32))
    ///     .pipe(serial_pipe::<String, ()>(|_: String| {}));
    /// ```
    #[must_use]
    pub fn pipe<O: Send + 'static>(mut self, next: DataPipe<Prev, O>) -> DataPipelineBuilder<O> {
        let is_first = self.pipes.is_empty();
        let slots = Arc::clone(&self.slots);
        let DataPipe { kind, callable } = next;

        let erased = move |pf: &mut Pipeflow| {
            let input: Prev = if is_first {
                // INVARIANT: `is_first` is true only for the very first
                // `pipe` call reachable from `DataPipeline::builder`, whose
                // return type fixes `Prev = ()` -- the only way to reach
                // this closure with an empty `pipes` vector. The round trip
                // through `Any` is still needed because this method's own
                // body is generic over every later pipe's `Prev` too, and
                // skips the slot entirely: there is no predecessor to have
                // left a value in it.
                *(Box::new(()) as Box<dyn Any + Send>)
                    .downcast::<Prev>()
                    .expect("a pipeline's first pipe always has an input type of `()`")
            } else {
                let boxed = lock_slot(&slots[pf.line()])
                    .take()
                    .expect("a pipe always finds the value its predecessor just left for it");
                *boxed.downcast::<Prev>().expect(
                    "DataPipelineBuilder::pipe checked this pipe's input against its \
                     predecessor's output type",
                )
            };
            let output = callable(input, pf);
            *lock_slot(&slots[pf.line()]) = Some(Box::new(output) as Box<dyn Any + Send>);
        };

        self.pipes.push(Pipe::new(kind, erased));
        DataPipelineBuilder {
            num_lines: self.num_lines,
            pipes: self.pipes,
            slots: self.slots,
            _prev: PhantomData,
        }
    }

    /// Finishes the chain, building the [`DataPipeline`]: upstream's
    /// constructor body, run once every pipe has been added.
    ///
    /// # Panics
    ///
    /// Panics with [`Pipeline::new`]'s own messages: no lines, no pipes, or a
    /// first pipe that is not [`PipeType::Serial`].
    ///
    /// # Examples
    ///
    /// ```
    /// use rustflow::pipeline::data_pipeline::{DataPipeline, serial_pipe};
    ///
    /// let pipeline = DataPipeline::builder(3).pipe(serial_pipe::<(), ()>(|()| {})).build();
    /// assert_eq!(pipeline.num_lines(), 3);
    /// assert_eq!(pipeline.num_tokens(), 0);
    /// ```
    #[must_use]
    pub fn build(self) -> DataPipeline {
        DataPipeline {
            pipeline: Pipeline::new(self.num_lines, self.pipes),
            slots: self.slots,
        }
    }
}

/// A data-parallel pipeline scheduling framework: upstream's
/// `tf::DataPipeline`, built through [`DataPipeline::builder`]'s typed
/// [`DataPipelineBuilder`] rather than a variadic constructor, since Rust has
/// no parameter pack to check one pipe's input against the pipe before it.
///
/// Wraps the [`Pipeline`] scheduler: every [`DataPipe`] in the chain becomes
/// one [`Pipe`] whose callable moves a boxed value through the user's typed
/// callable and into the next pipe's slot, but the token loop that runs
/// those `Pipe`s is [`Pipeline`]'s own one loop, not a second copy of it.
/// Composing that graph into another taskflow -- `taskflow.composed_of(&data_pipeline)`
/// -- is how a `DataPipeline` runs.
///
/// # Examples
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::pipeline::data_pipeline::{DataPipeline, serial_pipe, serial_pipe_with_flow};
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// // void -> i32 -> String -> void, over four lines, five tokens.
/// let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
/// let collected = Arc::clone(&seen);
///
/// let pipeline = DataPipeline::builder(4)
///     .pipe(serial_pipe_with_flow::<(), i32>(|(), pf| {
///         if pf.token() == 5 {
///             pf.stop();
///             return 0;
///         }
///         i32::try_from(pf.token()).expect("a token count fits in an i32")
///     }))
///     .pipe(serial_pipe::<i32, String>(|value| (value + 100).to_string()))
///     .pipe(serial_pipe_with_flow::<String, ()>(move |text, _pf| {
///         collected.lock().expect("uncontended").push(text);
///     }))
///     .build();
///
/// let mut taskflow = Taskflow::new();
/// let module = taskflow.composed_of(&pipeline);
/// taskflow.task(module).name("module_of_pipeline");
///
/// let executor = Executor::new(2);
/// executor.run(&taskflow).wait().expect("no task panicked");
///
/// assert_eq!(pipeline.num_tokens(), 5);
/// let mut values = seen.lock().expect("uncontended").clone();
/// values.sort();
/// assert_eq!(values, vec!["100", "101", "102", "103", "104"]);
/// ```
pub struct DataPipeline {
    pipeline: Pipeline,
    slots: Arc<Vec<Slot>>,
}

impl fmt::Debug for DataPipeline {
    /// A slot's contents have no `Debug` ([`DataPipe`]'s own doc says why),
    /// so only what [`Pipeline`] itself already reports is printed.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataPipeline")
            .field("pipeline", &self.pipeline)
            .finish()
    }
}

impl DataPipeline {
    /// Starts a typed builder for a pipeline of up to `num_lines` parallel
    /// lines: upstream's constructor, entered one pipe at a time rather than
    /// through a variadic pack.
    ///
    /// # Examples
    ///
    /// ```
    /// use rustflow::pipeline::data_pipeline::{DataPipeline, serial_pipe};
    ///
    /// let pipeline = DataPipeline::builder(5).pipe(serial_pipe::<(), ()>(|()| {})).build();
    /// assert_eq!(pipeline.num_lines(), 5);
    /// ```
    #[must_use]
    pub fn builder(num_lines: usize) -> DataPipelineBuilder<()> {
        DataPipelineBuilder {
            num_lines,
            pipes: Vec::new(),
            slots: Arc::new((0..num_lines).map(|_| Mutex::new(None)).collect()),
            _prev: PhantomData,
        }
    }

    /// The number of parallel lines this pipeline was built with: upstream's
    /// `num_lines`.
    #[must_use]
    pub fn num_lines(&self) -> usize {
        self.pipeline.num_lines()
    }

    /// The number of pipes in this pipeline's chain: upstream's `num_pipes`.
    #[must_use]
    pub fn num_pipes(&self) -> usize {
        self.pipeline.num_pipes()
    }

    /// The number of tokens this pipeline has generated so far: upstream's
    /// `num_tokens`.
    #[must_use]
    pub fn num_tokens(&self) -> usize {
        self.pipeline.num_tokens()
    }

    /// Resets this pipeline's progress, as if it had just been built:
    /// upstream's `reset`. Delegates to [`Pipeline::reset`] and drops
    /// whatever value was left in each line's slot.
    ///
    /// Not safe to call while a run of a taskflow this pipeline is composed
    /// into is in flight, which upstream's own `reset` does not guard
    /// against either.
    pub fn reset(&self) {
        for slot in self.slots.iter() {
            *lock_slot(slot) = None;
        }
        self.pipeline.reset();
    }

    /// Returns a share of the taskflow this pipeline composes into a module
    /// task: upstream's `graph()`, widened to the [`Taskflow`] that owns it,
    /// which is what [`Composable`] needs.
    #[must_use]
    pub fn taskflow(&self) -> Arc<Taskflow> {
        self.pipeline.taskflow()
    }
}

impl Composable for &DataPipeline {
    /// Delegates to [`DataPipeline::taskflow`]; implemented for a reference
    /// rather than for [`DataPipeline`] itself so that composing one does not
    /// consume it -- [`Pipeline`]'s own `Composable` impl's reasoning.
    fn taskflow(&self) -> Arc<Taskflow> {
        self.pipeline.taskflow()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::Executor;
    use crate::flow_builder::FlowBuilder;
    use crate::taskflow::Taskflow;

    /// The per-line slot's own cost, measured rather than guessed: a
    /// `Mutex<Option<Box<dyn Any + Send>>>` is 24 bytes on a 64-bit target --
    /// a 16-byte fat pointer for the `Box<dyn Any + Send>` (data pointer plus
    /// vtable pointer), `Option`'s own tag folded into the data pointer's
    /// niche since a `Box`'s data pointer is never null, plus the `Mutex`'s
    /// own lock word rounded up to the slot's 8-byte alignment.
    #[test]
    #[cfg(target_pointer_width = "64")]
    fn the_per_line_slot_is_24_bytes_on_a_64_bit_target() {
        assert_eq!(std::mem::size_of::<Slot>(), 24);
    }

    #[test]
    fn serial_and_parallel_constructors_record_the_right_direction() {
        assert_eq!(
            serial_pipe::<i32, i32>(|value| value).pipe_type(),
            PipeType::Serial
        );
        assert_eq!(
            serial_pipe_with_flow::<i32, i32>(|value, _pf| value).pipe_type(),
            PipeType::Serial
        );
        assert_eq!(
            parallel_pipe::<i32, i32>(|value| value).pipe_type(),
            PipeType::Parallel
        );
        assert_eq!(
            parallel_pipe_with_flow::<i32, i32>(|value, _pf| value).pipe_type(),
            PipeType::Parallel
        );
    }

    #[test]
    #[should_panic(expected = "must have at least one line")]
    fn zero_lines_panics() {
        let _ = DataPipeline::builder(0)
            .pipe(serial_pipe::<(), ()>(|()| {}))
            .build();
    }

    #[test]
    #[should_panic(expected = "must have at least one pipe")]
    fn zero_pipes_panics() {
        let _ = DataPipeline::builder(1).build();
    }

    #[test]
    #[should_panic(expected = "first pipe must be serial")]
    fn a_parallel_first_pipe_panics() {
        let _ = DataPipeline::builder(1)
            .pipe(parallel_pipe::<(), ()>(|()| {}))
            .build();
    }

    #[test]
    fn accessors_read_back_what_builder_was_given() {
        let pipeline = DataPipeline::builder(3)
            .pipe(serial_pipe::<(), i32>(|()| 1))
            .pipe(serial_pipe::<i32, i32>(|value| value))
            .build();
        assert_eq!(pipeline.num_lines(), 3);
        assert_eq!(pipeline.num_pipes(), 2);
        assert_eq!(pipeline.num_tokens(), 0);
    }

    /// A three-pipe chain, run then reset then rerun: every value a pipe
    /// receives is exactly what its predecessor just produced, and
    /// [`DataPipeline::reset`] returns both the token count and every line's
    /// slot to a fresh run's starting state.
    #[test]
    fn a_chain_carries_each_pipes_output_to_the_next_and_resets_cleanly() {
        let produced = Arc::new(AtomicUsize::new(0));
        let last_seen: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));

        let source = Arc::clone(&produced);
        let sink = Arc::clone(&last_seen);
        let pipeline = DataPipeline::builder(2)
            .pipe(serial_pipe_with_flow::<(), i32>(move |(), pf| {
                let current = source.load(Ordering::Relaxed);
                if current == 6 {
                    pf.stop();
                    return 0;
                }
                source.fetch_add(1, Ordering::Relaxed);
                i32::try_from(current).expect("a token count fits in an i32")
            }))
            .pipe(serial_pipe::<i32, i32>(|value| value * 10))
            .pipe(serial_pipe::<i32, ()>(move |value| {
                sink.lock().expect("uncontended").push(value);
            }))
            .build();

        let mut taskflow = Taskflow::new();
        let module = taskflow.composed_of(&pipeline);
        taskflow.task(module).name("module_of_data_pipeline");

        let executor = Executor::new(2);
        executor.run(&taskflow).wait().expect("no task panicked");

        assert_eq!(pipeline.num_tokens(), 6);
        {
            let mut seen = last_seen.lock().expect("uncontended");
            seen.sort_unstable();
            assert_eq!(seen.as_slice(), [0, 10, 20, 30, 40, 50]);
            seen.clear();
        }

        produced.store(0, Ordering::Relaxed);
        pipeline.reset();
        assert_eq!(pipeline.num_tokens(), 0);

        executor.run(&taskflow).wait().expect("no task panicked");
        assert_eq!(pipeline.num_tokens(), 6);
        let mut seen = last_seen.lock().expect("uncontended");
        seen.sort_unstable();
        assert_eq!(seen.as_slice(), [0, 10, 20, 30, 40, 50]);
    }
}

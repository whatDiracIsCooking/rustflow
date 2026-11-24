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
//! `DESIGN.md`'s "Pipelines" section has why this chain is built
//! from two callable shapes rather than upstream's three, and what the slot
//! that carries a token's value from one pipe to the next costs.
//!
//! **The two scalable reset forms take a closure over a builder this type
//! starts, not an already-built chain.** There is no upstream shape to
//! follow -- `tf::DataPipeline` exposes only `reset()` -- and the obvious
//! alternative, a chain value a caller builds and hands over, gives up what
//! [`DataPipelineBuilder`] exists for: its per-line slots are sized by
//! whichever line count built it, so a chain built over a different count
//! would turn [`DataPipelineBuilder::pipe`]'s compile-time type check into a
//! runtime index out of bounds. Handing the closure a builder over the line
//! count the reset is about keeps the check and leaves the count the
//! method's own to decide.
//!
//! **Replacing a pipe's callable is one method per callable shape, where
//! upstream's `callable(U&&)` is one method deducing which shape it was
//! handed.** [`DataPipe::set_callable`] takes the `Fn(I) -> O` shape and
//! [`DataPipe::set_callable_with_flow`] the `Fn(I, &mut Pipeflow) -> O` one.
//! Covering both from a single method would need `impl<F: Fn(I) -> O> ..`
//! beside `impl<F: Fn(I, &mut Pipeflow) -> O> ..`, which coherence rejects
//! as overlapping (E0119) even though no closure satisfies both -- the
//! limitation `DESIGN.md` already records for `emplace`'s task kinds and for
//! the static and dynamic loop traits. A marker type parameter routes around
//! it, at the price of a public trait and a marker type per shape in a
//! surface that is otherwise four free functions, and of a closure whose
//! shape the compiler can no longer infer from its own argument list.
//! Splitting by shape is what those four constructors already do, one name
//! per shape, so a setter is the pair of whichever one built the pipe.

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
/// -- `DESIGN.md`'s "Pipelines" section has what that costs.
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
/// constructor below builds: `DESIGN.md`'s "Pipelines" section
/// says why there is one shape here rather than upstream's three.
type Callable<I, O> = Box<dyn Fn(I, &mut Pipeflow) -> O + Send + Sync>;

/// A typed stage of a [`DataPipeline`]: upstream's `tf::DataPipe`.
///
/// Built by [`serial_pipe`], [`serial_pipe_with_flow`], [`parallel_pipe`] or
/// [`parallel_pipe_with_flow`] -- never directly, since its fields are
/// private -- and consumed by [`DataPipelineBuilder::pipe`], which checks `I`
/// against the chain's previous pipe's own output type before it will accept
/// one.
///
/// The callable bound is `Fn(..) + Send + Sync`, both kinds of pipe paying it
/// alike; `DESIGN.md`'s "A pipe callable is `Fn + Send + Sync`" entry has why
/// `Fn` rather than `FnMut`. [`DataPipe::set_type`],
/// [`DataPipe::set_callable`] and [`DataPipe::set_callable_with_flow`] change
/// a pipe under that same bound before it is added to a chain.
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

    /// Changes this pipe's direction: upstream's `type(PipeType)`.
    ///
    /// A chain's first pipe must still be [`PipeType::Serial`] when
    /// [`DataPipelineBuilder::build`] runs, which is where a flipped first
    /// pipe is caught.
    ///
    /// # Examples
    ///
    /// ```
    /// use rustflow::pipeline::data_pipeline::serial_pipe;
    /// use rustflow::pipeline::pipe::PipeType;
    ///
    /// let mut pipe = serial_pipe::<i32, i32>(|value| value + 1);
    /// pipe.set_type(PipeType::Parallel);
    /// assert_eq!(pipe.pipe_type(), PipeType::Parallel);
    /// ```
    pub fn set_type(&mut self, pipe_type: PipeType) {
        self.kind = pipe_type;
    }

    /// Replaces this pipe's callable with one that only needs its input
    /// value: upstream's `callable(U&&)` handed [`serial_pipe`]'s own
    /// callable shape, leaving the direction alone. This module's `# Design`
    /// section has why the other shape is [`DataPipe::set_callable_with_flow`]
    /// rather than this method again.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::{Arc, Mutex};
    ///
    /// use rustflow::Executor;
    /// use rustflow::pipeline::data_pipeline::{
    ///     DataPipeline, serial_pipe, serial_pipe_with_flow,
    /// };
    /// use rustflow::prelude::*;
    ///
    /// let seen: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    /// let collected = Arc::clone(&seen);
    /// let mut sink = serial_pipe::<i32, ()>(|_value| unreachable!("replaced below"));
    /// sink.set_callable(move |value| collected.lock().expect("uncontended").push(value));
    ///
    /// let pipeline = DataPipeline::builder(1)
    ///     .pipe(serial_pipe_with_flow::<(), i32>(|(), pf| {
    ///         if pf.token() == 3 {
    ///             pf.stop();
    ///             return 0;
    ///         }
    ///         i32::try_from(pf.token()).expect("a token count fits in an i32")
    ///     }))
    ///     .pipe(sink)
    ///     .build();
    ///
    /// let mut taskflow = Taskflow::new();
    /// let module = taskflow.composed_of(&pipeline);
    /// taskflow.task(module).name("module_of_data_pipeline");
    /// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
    ///
    /// assert_eq!(*seen.lock().expect("the run is over"), vec![0, 1, 2]);
    /// ```
    pub fn set_callable(&mut self, callable: impl Fn(I) -> O + Send + Sync + 'static)
    where
        I: Send + 'static,
        O: Send + 'static,
    {
        self.callable = Box::new(move |input, _pf: &mut Pipeflow| callable(input));
    }

    /// Replaces this pipe's callable with one that also needs the running
    /// token's [`Pipeflow`]: upstream's `callable(U&&)` handed
    /// [`serial_pipe_with_flow`]'s own callable shape, leaving the direction
    /// alone.
    ///
    /// # Examples
    ///
    /// ```
    /// use rustflow::pipeline::data_pipeline::parallel_pipe_with_flow;
    /// use rustflow::pipeline::pipe::PipeType;
    ///
    /// let mut pipe = parallel_pipe_with_flow::<i32, i32>(|value, _pf| value);
    /// pipe.set_callable_with_flow(|value, pf| {
    ///     value + i32::try_from(pf.line()).expect("a line count fits in an i32")
    /// });
    /// assert_eq!(pipe.pipe_type(), PipeType::Parallel);
    /// ```
    pub fn set_callable_with_flow(
        &mut self,
        callable: impl Fn(I, &mut Pipeflow) -> O + Send + Sync + 'static,
    ) where
        I: Send + 'static,
        O: Send + 'static,
    {
        self.callable = Box::new(callable);
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

    /// Rebuilds this pipeline over a new typed chain, keeping its number of
    /// lines: [`Pipeline::reset_pipes`] behind the compile-time type check
    /// [`DataPipelineBuilder::pipe`] performs. `chain` is handed a fresh
    /// builder over this pipeline's own [`DataPipeline::num_lines`] lines and
    /// returns it with the new chain appended. Every line starts again at
    /// pipe `0`, each line's slot is empty again and the token counter
    /// returns to `0`, as a freshly built pipeline would report.
    ///
    /// Upstream's `tf::DataPipeline` has no counterpart -- it exposes only
    /// `reset()` -- so the shape is this port's own; this module's `# Design`
    /// section says why it takes a closure rather than an already-built
    /// chain.
    ///
    /// Composing this pipeline into a graph
    /// (`taskflow.composed_of(&pipeline)`) before calling this method leaves
    /// that composition pointing at the pre-reset graph, for the reason
    /// [`Pipeline::reset_pipes`] and `src/pipeline/scheduler.rs`'s own
    /// `# Design` section give; compose again afterwards to run the new
    /// chain.
    ///
    /// # Panics
    ///
    /// Panics with [`Pipeline::reset_pipes`]'s own messages: no pipes ("must
    /// have at least one pipe"), or a first pipe that is not
    /// [`PipeType::Serial`] ("first pipe must be serial"). Panics too if
    /// `chain` returns a builder of its own making rather than the one it
    /// was handed, whose per-line slots would be sized for a different line
    /// count.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::pipeline::data_pipeline::{DataPipeline, serial_pipe, serial_pipe_with_flow};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let sum = Arc::new(AtomicUsize::new(0));
    /// let mut pipeline = DataPipeline::builder(2)
    ///     .pipe(serial_pipe::<(), ()>(|()| {}))
    ///     .build();
    ///
    /// // A void -> i32 -> void chain replaces the throwaway one above.
    /// let counted = Arc::clone(&sum);
    /// pipeline.reset_pipes(|builder| {
    ///     builder
    ///         .pipe(serial_pipe_with_flow::<(), i32>(|(), pf| {
    ///             if pf.token() == 4 {
    ///                 pf.stop();
    ///                 return 0;
    ///             }
    ///             i32::try_from(pf.token()).expect("a token count fits in an i32")
    ///         }))
    ///         .pipe(serial_pipe::<i32, ()>(move |value| {
    ///             counted.fetch_add(usize::try_from(value).expect("nonnegative"), Ordering::Relaxed);
    ///         }))
    /// });
    /// assert_eq!(pipeline.num_lines(), 2);
    /// assert_eq!(pipeline.num_pipes(), 2);
    ///
    /// let mut taskflow = Taskflow::new();
    /// let module = taskflow.composed_of(&pipeline);
    /// taskflow.task(module).name("module_of_data_pipeline");
    ///
    /// let executor = Executor::new(2);
    /// executor.run(&taskflow).wait().expect("no task panicked");
    ///
    /// assert_eq!(pipeline.num_tokens(), 4);
    /// assert_eq!(sum.load(Ordering::Relaxed), 0 + 1 + 2 + 3);
    /// ```
    pub fn reset_pipes<Last>(
        &mut self,
        chain: impl FnOnce(DataPipelineBuilder<()>) -> DataPipelineBuilder<Last>,
    ) {
        let num_lines = self.num_lines();
        let built = chain(Self::builder(num_lines));
        assert_eq!(
            built.num_lines, num_lines,
            "a reset chain must be built on the builder this method hands it"
        );
        // The rebuild is the step that can panic, so the new slots replace
        // this pipeline's own only once it has gone through: a caller that
        // catches the panic is then left with the chain and the slots it
        // already had, still each other's.
        self.pipeline.reset_pipes(built.pipes);
        self.slots = built.slots;
    }

    /// Rebuilds this pipeline over a new number of lines and a new typed
    /// chain: [`Pipeline::reset_lines`] behind the same compile-time type
    /// check, with `chain` handed a fresh builder over `num_lines` lines
    /// rather than this pipeline's current count.
    ///
    /// [`DataPipeline::reset_pipes`]'s docs cover the rest: what has no
    /// upstream counterpart, and what an already-composed graph keeps
    /// running.
    ///
    /// # Panics
    ///
    /// Panics with [`Pipeline::reset_lines`]'s own messages: `num_lines` of
    /// `0` ("must have at least one line"), no pipes ("must have at least
    /// one pipe"), or a first pipe that is not [`PipeType::Serial`] ("first
    /// pipe must be serial"). Panics too on a `chain` that returns a builder
    /// of its own making, as [`DataPipeline::reset_pipes`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use rustflow::pipeline::data_pipeline::{DataPipeline, serial_pipe};
    ///
    /// let mut pipeline = DataPipeline::builder(2)
    ///     .pipe(serial_pipe::<(), ()>(|()| {}))
    ///     .build();
    ///
    /// pipeline.reset_lines(5, |builder| {
    ///     builder
    ///         .pipe(serial_pipe::<(), u8>(|()| 7))
    ///         .pipe(serial_pipe::<u8, String>(|value| value.to_string()))
    /// });
    /// assert_eq!(pipeline.num_lines(), 5);
    /// assert_eq!(pipeline.num_pipes(), 2);
    /// assert_eq!(pipeline.num_tokens(), 0);
    /// ```
    pub fn reset_lines<Last>(
        &mut self,
        num_lines: usize,
        chain: impl FnOnce(DataPipelineBuilder<()>) -> DataPipelineBuilder<Last>,
    ) {
        let built = chain(Self::builder(num_lines));
        assert_eq!(
            built.num_lines, num_lines,
            "a reset chain must be built on the builder this method hands it"
        );
        // [`DataPipeline::reset_pipes`]'s own comment: the slots follow the
        // rebuild rather than lead it.
        self.pipeline.reset_lines(num_lines, built.pipes);
        self.slots = built.slots;
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

    /// Both setters reach the field the constructors set, and a callable
    /// swap leaves the direction where [`DataPipe::set_type`] left it.
    ///
    /// Calls the stored callable directly, as the erased closure
    /// [`DataPipelineBuilder::pipe`] builds does: what a run of a chain
    /// makes of a swapped pipe is `tests/data_pipelines.rs`'s own pair of
    /// cases.
    #[test]
    fn a_data_pipes_type_and_both_callable_shapes_can_be_replaced() {
        let mut pipeflow = Pipeflow {
            line: 3,
            ..Pipeflow::default()
        };
        let mut pipe = serial_pipe::<i32, i32>(|value| value + 1);
        assert_eq!((pipe.callable)(1, &mut pipeflow), 2);

        pipe.set_type(PipeType::Parallel);
        assert_eq!(pipe.pipe_type(), PipeType::Parallel);

        pipe.set_callable(|value| value * 10);
        assert_eq!((pipe.callable)(2, &mut pipeflow), 20);
        assert_eq!(
            pipe.pipe_type(),
            PipeType::Parallel,
            "a callable swap is no part of the direction"
        );

        pipe.set_callable_with_flow(|value, pf| {
            value + i32::try_from(pf.line()).expect("a line count fits in an i32")
        });
        assert_eq!((pipe.callable)(5, &mut pipeflow), 8);
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

    /// A chain built on a builder of the caller's own making is rejected:
    /// its per-line slots are sized for its own line count, not this
    /// pipeline's, so accepting one would reach past the end of them at run
    /// time.
    #[test]
    #[should_panic(expected = "must be built on the builder this method hands it")]
    fn a_reset_chain_from_a_foreign_builder_panics() {
        let mut pipeline = DataPipeline::builder(2)
            .pipe(serial_pipe::<(), ()>(|()| {}))
            .build();
        pipeline
            .reset_pipes(|_builder| DataPipeline::builder(7).pipe(serial_pipe::<(), ()>(|()| {})));
    }

    /// Both reset forms leave the pipeline reporting the shape they were
    /// just given, the token counter back at `0`, and -- for
    /// [`DataPipeline::reset_lines`] -- one slot per new line.
    #[test]
    fn the_reset_forms_read_back_the_shape_they_were_given() {
        let mut pipeline = DataPipeline::builder(2)
            .pipe(serial_pipe::<(), ()>(|()| {}))
            .build();

        pipeline.reset_pipes(|builder| {
            builder
                .pipe(serial_pipe::<(), i32>(|()| 1))
                .pipe(serial_pipe::<i32, i32>(|value| value))
        });
        assert_eq!(pipeline.num_lines(), 2);
        assert_eq!(pipeline.num_pipes(), 2);
        assert_eq!(pipeline.num_tokens(), 0);
        assert_eq!(pipeline.slots.len(), 2);

        pipeline.reset_lines(4, |builder| {
            builder.pipe(serial_pipe::<(), String>(|()| String::new()))
        });
        assert_eq!(pipeline.num_lines(), 4);
        assert_eq!(pipeline.num_pipes(), 1);
        assert_eq!(pipeline.num_tokens(), 0);
        assert_eq!(pipeline.slots.len(), 4);
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

//! Translated from `taskflow/algorithm/pipeline.hpp` (Taskflow v4.1.0).
//!
//! [`Pipeflow`] is the scheduling token a pipe callable is handed; [`PipeType`]
//! is a pipe's direction; [`Pipe`] pairs a direction with the callable the
//! scheduler runs for every token that reaches it. The scheduler that drives
//! a token through a sequence of these is a later addition on top of them.
//!
//! # Design
//!
//! `DESIGN.md`'s "Pipelines" section has why, the pipe's own entries among
//! the family's: the `Handle` variant a pipeline's lines needed after all,
//! the pipe callable's `Fn + Send + Sync` bound, one `Pipeline` type for
//! upstream's two, and what stayed out of scope.

use std::fmt;

/// A scheduling token's own view of its place in a pipeline: upstream's
/// `tf::Pipeflow`.
///
/// The scheduler hands a running pipe's callable a `&mut Pipeflow` so it can
/// read where the token is -- [`Pipeflow::line`], [`Pipeflow::pipe`] and
/// [`Pipeflow::token`] -- and, from the first pipe, end further scheduling
/// with [`Pipeflow::stop`]. The fields are `pub(crate)`: the scheduler builds
/// and advances every token directly, and a pipe callable only ever reads it
/// through the four methods below.
///
/// # Examples
///
/// ```
/// use rustflow::pipeline::pipe::Pipeflow;
///
/// let mut pipeflow = Pipeflow::default();
/// assert_eq!(pipeflow.line(), 0);
/// assert_eq!(pipeflow.pipe(), 0);
/// assert_eq!(pipeflow.token(), 0);
///
/// // The first pipe may stop the token; this does not panic.
/// pipeflow.stop();
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct Pipeflow {
    /// The line this token is running on: upstream's `_line`.
    pub(crate) line: usize,
    /// The pipe this token is at: upstream's `_pipe`.
    pub(crate) pipe: usize,
    /// This token's own identifier, stable across every pipe it runs
    /// through: upstream's `_token`.
    pub(crate) token: usize,
    /// Set by [`Pipeflow::stop`]: upstream's `_stop`.
    pub(crate) stop: bool,
}

impl Pipeflow {
    /// The line this token is running on: upstream's `line()`.
    #[must_use]
    pub const fn line(&self) -> usize {
        self.line
    }

    /// The pipe this token is at: upstream's `pipe()`.
    #[must_use]
    pub const fn pipe(&self) -> usize {
        self.pipe
    }

    /// This token's own identifier, stable across every pipe it runs
    /// through: upstream's `token()`.
    #[must_use]
    pub const fn token(&self) -> usize {
        self.token
    }

    /// Ends the pipeline's scheduling of further tokens: upstream's
    /// `stop()`.
    ///
    /// # Panics
    ///
    /// Panics if called from any pipe but the first, upstream's
    /// `TF_THROW("only the first pipe can stop the token")`: only the first
    /// pipe decides whether a new token enters the pipeline at all.
    pub fn stop(&mut self) {
        if self.pipe != 0 {
            panic!("only the first pipe can stop the token");
        }
        self.stop = true;
    }
}

/// A pipe's direction: upstream's `tf::PipeType`.
///
/// A [`PipeType::Serial`] pipe runs one token at a time, in token order; a
/// [`PipeType::Parallel`] pipe places no such ordering on the tokens running
/// through it. The discriminants match upstream's
/// `static_cast<size_t>(type)`, the join-counter value the scheduler stores
/// before a pipe may run a token.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum PipeType {
    /// Upstream's `PipeType::PARALLEL`, discriminant `1`.
    Parallel = 1,
    /// Upstream's `PipeType::SERIAL`, discriminant `2`.
    Serial = 2,
}

impl PipeType {
    /// This direction's join-counter value: upstream's
    /// `static_cast<size_t>(type)`, as a method rather than an `as usize`
    /// cast so the crate's cast lints have nothing to warn about.
    ///
    /// # Examples
    ///
    /// ```
    /// use rustflow::pipeline::pipe::PipeType;
    ///
    /// assert_eq!(PipeType::Parallel.join_counter(), 1);
    /// assert_eq!(PipeType::Serial.join_counter(), 2);
    /// ```
    #[must_use]
    pub const fn join_counter(self) -> usize {
        match self {
            PipeType::Parallel => 1,
            PipeType::Serial => 2,
        }
    }
}

impl From<PipeType> for usize {
    /// Upstream's `static_cast<size_t>(type)`; see
    /// [`PipeType::join_counter`].
    fn from(pipe_type: PipeType) -> Self {
        pipe_type.join_counter()
    }
}

/// A pipeline stage: upstream's `tf::Pipe`.
///
/// Pairs a [`PipeType`] with the callable the pipeline scheduler runs for
/// every token that reaches this pipe, handing it a `&mut Pipeflow` so it
/// can read where the running token is and, from the first pipe, stop the
/// pipeline. The callable bound is `Fn(&mut Pipeflow) + Send + Sync`, both
/// kinds of pipe paying it alike, and upstream's second callable shape,
/// `(Pipeflow&, NonpreemptiveRuntime&)`, is not ported; `DESIGN.md`'s
/// "Pipelines" section has why for both.
pub struct Pipe {
    kind: PipeType,
    callable: Box<dyn Fn(&mut Pipeflow) + Send + Sync>,
}

impl Pipe {
    /// Builds a pipe of `pipe_type` running `callable`: upstream's
    /// `Pipe(PipeType, C&&)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
    ///
    /// let pipe = Pipe::new(PipeType::Serial, |pipeflow: &mut Pipeflow| {
    ///     pipeflow.stop();
    /// });
    /// assert_eq!(pipe.pipe_type(), PipeType::Serial);
    /// ```
    pub fn new<C>(pipe_type: PipeType, callable: C) -> Self
    where
        C: Fn(&mut Pipeflow) + Send + Sync + 'static,
    {
        Self {
            kind: pipe_type,
            callable: Box::new(callable),
        }
    }

    /// This pipe's direction: upstream's `type()`.
    #[must_use]
    pub fn pipe_type(&self) -> PipeType {
        self.kind
    }

    /// Changes this pipe's direction: upstream's `type(PipeType)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
    ///
    /// let mut pipe = Pipe::new(PipeType::Serial, |_: &mut Pipeflow| {});
    /// pipe.set_type(PipeType::Parallel);
    /// assert_eq!(pipe.pipe_type(), PipeType::Parallel);
    /// ```
    pub fn set_type(&mut self, pipe_type: PipeType) {
        self.kind = pipe_type;
    }

    /// Replaces this pipe's callable: upstream's `callable(U&&)`.
    pub fn set_callable<C>(&mut self, callable: C)
    where
        C: Fn(&mut Pipeflow) + Send + Sync + 'static,
    {
        self.callable = Box::new(callable);
    }

    /// Runs this pipe's callable against `pipeflow`: what the scheduler
    /// calls for every token that reaches this pipe. `pub` stands in for
    /// upstream's friend access, as
    /// [`Work::invoke`](crate::graph::Work::invoke) does for the same shape
    /// of problem.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
    ///
    /// let seen = Arc::new(AtomicUsize::new(0));
    /// let captured = Arc::clone(&seen);
    /// let pipe = Pipe::new(PipeType::Serial, move |pipeflow: &mut Pipeflow| {
    ///     captured.store(pipeflow.token() + 1, Ordering::Relaxed);
    /// });
    ///
    /// let mut pipeflow = Pipeflow::default();
    /// pipe.invoke(&mut pipeflow);
    /// assert_eq!(seen.load(Ordering::Relaxed), 1);
    /// ```
    pub fn invoke(&self, pipeflow: &mut Pipeflow) {
        (self.callable)(pipeflow);
    }
}

impl fmt::Debug for Pipe {
    /// The callable has no `Debug`, so only the direction is printed --
    /// [`graph::OnceWork`](crate::graph::OnceWork)'s same shape, for the
    /// same reason.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pipe").field("type", &self.kind).finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn pipeflow_accessors_read_the_fields_the_scheduler_sets() {
        let pipeflow = Pipeflow {
            line: 3,
            pipe: 2,
            token: 7,
            stop: false,
        };
        assert_eq!(pipeflow.line(), 3);
        assert_eq!(pipeflow.pipe(), 2);
        assert_eq!(pipeflow.token(), 7);
    }

    #[test]
    fn stop_on_the_first_pipe_sets_the_flag() {
        let mut pipeflow = Pipeflow {
            line: 0,
            pipe: 0,
            token: 0,
            stop: false,
        };
        pipeflow.stop();
        assert!(pipeflow.stop);
    }

    #[test]
    #[should_panic(expected = "only the first pipe can stop the token")]
    fn stop_on_any_other_pipe_panics() {
        let mut pipeflow = Pipeflow {
            line: 0,
            pipe: 1,
            token: 0,
            stop: false,
        };
        pipeflow.stop();
    }

    #[test]
    fn pipe_type_discriminants_are_upstreams_join_counter_values() {
        assert_eq!(PipeType::Parallel.join_counter(), 1);
        assert_eq!(PipeType::Serial.join_counter(), 2);
        assert_eq!(usize::from(PipeType::Parallel), 1);
        assert_eq!(usize::from(PipeType::Serial), 2);
    }

    #[test]
    fn a_pipes_type_and_callable_can_both_be_replaced() {
        let seen = Arc::new(AtomicUsize::new(0));
        let first = Arc::clone(&seen);
        let mut pipe = Pipe::new(PipeType::Serial, move |_: &mut Pipeflow| {
            first.store(1, Ordering::Relaxed);
        });
        assert_eq!(pipe.pipe_type(), PipeType::Serial);

        let mut pipeflow = Pipeflow::default();
        pipe.invoke(&mut pipeflow);
        assert_eq!(seen.load(Ordering::Relaxed), 1);

        pipe.set_type(PipeType::Parallel);
        assert_eq!(pipe.pipe_type(), PipeType::Parallel);

        let second = Arc::clone(&seen);
        pipe.set_callable(move |_: &mut Pipeflow| {
            second.store(2, Ordering::Relaxed);
        });
        pipe.invoke(&mut pipeflow);
        assert_eq!(seen.load(Ordering::Relaxed), 2);
    }
}

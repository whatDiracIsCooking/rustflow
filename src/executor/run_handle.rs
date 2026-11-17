//! Translated from `taskflow/core/executor.hpp` (Taskflow v4.1.0): `run`,
//! `_set_up_topology`, `_increment_topology` and `wait_for_all`.
//!
//! # `RunHandle` where upstream has `tf::Future`
//!
//! Upstream's `run` hands back a `tf::Future<void>` that shares ownership of
//! the topology with every queued node, and documents a rule it cannot
//! enforce: *do not mutate a taskflow while it is running*. Break it and the
//! scheduler walks edges the user is editing.
//!
//! Here the rule is the type system's. [`Executor::run`] borrows the executor
//! and the taskflow for the same lifetime and puts that lifetime on the
//! handle, so a taskflow cannot be edited, dropped or run again -- nor the
//! executor dropped -- while a handle naming them is alive. That leaves one
//! hole, the one `std::thread::scope` has: a handle the caller simply forgets
//! about would end the borrow while the run is still going. So [`Drop`]
//! blocks, exactly as a scope's does, and the borrow becomes airtight.
//!
//! Two things follow from that, and both are deliberate:
//!
//! - There is no detached run and no `std::future`-style transfer of the
//!   result to somewhere else in the program. A topology outliving its handle
//!   is the one thing the design does not allow.
//! - [`Executor::wait_for_all`] cannot be the thing that makes dropping an
//!   executor safe, because it is already impossible to drop an executor with
//!   a run in flight. It is here because upstream has it and because the
//!   async run handle will need the count it reads.

use std::any::Any;
use std::fmt;
use std::marker::PhantomData;
use std::sync::PoisonError;

use crate::executor::Executor;
use crate::executor::topology::Topology;
use crate::taskflow::Taskflow;

/// Why a run did not finish cleanly.
///
/// One variant so far, and the enum is `#[non_exhaustive]` because
/// cancellation is a later milestone and will be the second.
#[non_exhaustive]
pub enum RunError {
    /// A task panicked, carrying what it panicked with.
    ///
    /// This is the port's reading of upstream's `std::exception_ptr`: a panic
    /// escaping a task body is caught where upstream catches an exception,
    /// stored in the topology, and surfaced here. The payload is whatever the
    /// task panicked with, so a caller that wants upstream's behaviour --
    /// the failure continuing to propagate on *this* thread -- can hand it to
    /// [`std::panic::resume_unwind`].
    ///
    /// Only the first panicking task of a run is reported; the rest of the
    /// run still completes, since cancelling the remaining tasks is a later
    /// milestone.
    Panicked(Box<dyn Any + Send>),
}

impl RunError {
    /// The panic message, when the payload is one of the two types a
    /// `panic!` produces.
    fn message(&self) -> Option<&str> {
        let Self::Panicked(payload) = self;
        payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
    }
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.message() {
            Some(message) => write!(f, "a task panicked: {message}"),
            None => f.write_str("a task panicked"),
        }
    }
}

impl fmt::Debug for RunError {
    /// Hand-written because the payload is a `Box<dyn Any + Send>`, which has
    /// no `Debug` -- and because this is what `unwrap` prints.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Panicked({self})")
    }
}

impl std::error::Error for RunError {}

/// A run of a taskflow in progress, and the right to wait for it.
///
/// Returned by [`Executor::run`]. Upstream's `tf::Future<void>`, with the
/// differences the module docs set out: the handle borrows the executor and
/// the taskflow for as long as it lives, and dropping it blocks until the run
/// is over.
///
/// ```
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let executor = Executor::new(4);
///
/// let mut taskflow = Taskflow::new();
/// let (a, b) = taskflow.emplace_n((|| println!("a"), || println!("b")));
/// taskflow.task(a).precede([b]);
///
/// executor.run(&taskflow).wait().expect("no task panicked");
///
/// // The borrow is over, so the taskflow can be edited and run again.
/// taskflow.emplace(|| println!("c"));
/// executor.run(&taskflow).wait().expect("no task panicked");
/// ```
pub struct RunHandle<'a> {
    /// The run. Boxed because every queued job holds its address, so it has
    /// to stay put while this handle moves -- out of [`Executor::run`], for
    /// one.
    topology: Box<Topology>,
    /// The borrows that make "do not mutate a running taskflow" a compile
    /// error. A marker rather than two reference fields because nothing reads
    /// either one: their whole job is to be held.
    borrow: PhantomData<(&'a Executor, &'a Taskflow)>,
}

impl<'a> RunHandle<'a> {
    /// Blocks until the run finishes, and reports whether a task panicked.
    ///
    /// Upstream's `tf::Future::get`, which rethrows the stored exception on
    /// the calling thread; a `Result` says the same thing without unwinding
    /// somebody else's stack for them. See [`RunError::Panicked`] for how to
    /// get upstream's behaviour back.
    ///
    /// Waiting from inside a task body works, but it blocks the worker running
    /// that task, so the run being waited for needs another worker to make
    /// progress on. `corun`, which lends the blocked worker to the inner run
    /// instead, is the tool for that and is a later milestone.
    pub fn wait(self) -> Result<(), RunError> {
        match self.topology.join() {
            Some(payload) => Err(RunError::Panicked(payload)),
            None => Ok(()),
        }
    }
}

impl Drop for RunHandle<'_> {
    /// Blocks until the run finishes, as [`std::thread::scope`] does.
    ///
    /// This is what makes the handle's borrow mean something: a run can never
    /// outlive it, so a queued job can never outlive what it points at. After
    /// [`RunHandle::wait`] it finds the run already over and returns at once.
    fn drop(&mut self) {
        // A panic payload nobody waited for is dropped here rather than
        // re-raised: this `Drop` may itself be running during an unwind, and a
        // panic from there aborts the process. Upstream's `Future` likewise
        // lets an unobserved exception go.
        drop(self.topology.join());
    }
}

impl fmt::Debug for RunHandle<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunHandle")
            .field("topology", &self.topology)
            .finish()
    }
}

impl Executor {
    /// Runs `taskflow` and returns a handle to wait on.
    ///
    /// Upstream's `run`, which is `run_n(taskflow, 1)`; `run_n` and
    /// `run_until` are a later issue, and so is the per-taskflow queue that
    /// lets several runs of the *same* taskflow line up behind each other.
    /// Until that lands, two overlapping runs of one taskflow would have two
    /// workers on one node, which [`Work::invoke`](crate::Work::invoke)
    /// reports as a panic rather than corrupting the run.
    ///
    /// A taskflow with no source task -- an empty one, or one whose every
    /// node is in a cycle -- schedules nothing and the returned handle is
    /// already finished. Upstream returns a ready future for the empty case
    /// and blocks forever on the cyclic one; a static graph with a cycle has
    /// no execution order, and reporting that as "nothing ran" beats reporting
    /// it as a hang.
    pub fn run<'a>(&'a self, taskflow: &'a Taskflow) -> RunHandle<'a> {
        let topology = Box::new(Topology::new());

        if !self.set_up_topology(taskflow, &topology) {
            topology.finish();
        }

        RunHandle {
            topology,
            borrow: PhantomData,
        }
    }

    /// Blocks until no run is in flight on this executor. Upstream's
    /// `wait_for_all`.
    ///
    /// Upstream's destructor calls this so that a detached future cannot
    /// outlive the pool. Nothing here needs it for that -- a [`RunHandle`]
    /// borrows the executor and blocks on drop, so a run in flight is already
    /// a run whose handle is alive and whose borrow keeps the executor around.
    /// What it answers is the question upstream's `num_topologies` answers:
    /// is the executor idle.
    pub fn wait_for_all(&self) {
        let mut idle = self
            .shared
            .topologies
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        while self.shared.num_topologies() != 0 {
            idle = self
                .shared
                .no_topologies
                .wait(idle)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Arms `topology` over `taskflow`'s graph and queues its source tasks,
    /// reporting whether there was anything to queue.
    ///
    /// Upstream's `_set_up_topology` for a caller that is not a worker, which
    /// is the only caller there can be today: `run` is the one entry point and
    /// it reaches the queues through the shared buffers. A separate method
    /// rather than four lines inside [`Executor::run`] because `run_n` and
    /// `run_until` need to start the *next* topology of a taskflow's queue
    /// from the tear-down of the previous one, where there is no handle to
    /// build. That path re-arms the graph with [`Topology::set_up`] and
    /// schedules into the finishing worker's own queue instead, and it does
    /// not touch the topology count -- a re-run is the same topology.
    pub(crate) fn set_up_topology(&self, taskflow: &Taskflow, topology: &Topology) -> bool {
        if topology.set_up(taskflow) == 0 {
            return false;
        }

        // Counted before the first job is queued, and for a reason that is
        // easy to get backwards: a worker skips the whole steal loop while
        // `num_topologies` is zero, so a worker woken by the push below has to
        // be able to see a non-zero count by the time it looks.
        self.shared.increment_topology();
        self.shared.bulk_schedule(topology.sources(taskflow));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

    #[test]
    fn an_empty_taskflow_runs_to_completion_without_a_topology() {
        let executor = Executor::new(2);
        let taskflow = Taskflow::new();

        let handle = executor.run(&taskflow);
        assert_eq!(executor.num_topologies(), 0, "nothing was scheduled");
        handle.wait().expect("an empty run cannot panic");

        executor.wait_for_all();
    }

    /// A static graph with a cycle has no source and therefore no execution
    /// order. Upstream blocks forever on it; this port finishes the run
    /// without having scheduled anything, which is what `run`'s docs promise.
    #[test]
    fn a_taskflow_with_no_source_runs_nothing_rather_than_blocking() {
        let executor = Executor::new(2);
        let mut taskflow = Taskflow::new();
        let (a, b) = taskflow.emplace_n((|| panic!("never reached"), || panic!("never reached")));
        taskflow.task(a).precede([b]);
        taskflow.task(b).precede([a]);

        executor.run(&taskflow).wait().expect("nothing ran");
        assert_eq!(executor.num_topologies(), 0);
    }

    #[test]
    fn a_run_error_says_what_the_task_panicked_with() {
        let from_str = RunError::Panicked(Box::new("boom"));
        assert_eq!(from_str.to_string(), "a task panicked: boom");
        assert_eq!(format!("{from_str:?}"), "Panicked(a task panicked: boom)");

        let from_string = RunError::Panicked(Box::new(String::from("boom")));
        assert_eq!(from_string.to_string(), "a task panicked: boom");

        let from_other = RunError::Panicked(Box::new(7_u8));
        assert_eq!(from_other.to_string(), "a task panicked");
    }

    /// The borrow is the whole design, so it is worth a compile-time check
    /// that the handle really does carry both halves of it.
    #[test]
    fn a_handle_borrows_the_executor_and_the_taskflow() {
        fn send<T: Send>() {}
        send::<RunHandle<'static>>();

        let executor = Executor::new(1);
        let taskflow = Taskflow::new();
        let handle = executor.run(&taskflow);
        assert!(format!("{handle:?}").contains("RunHandle"));
    }
}

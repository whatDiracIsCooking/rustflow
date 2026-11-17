//! Translated from `taskflow/core/runtime.hpp` (Taskflow v4.1.0): the `async`
//! family of `tf::Runtime` -- `async`, `silent_async`, `dependent_async` and
//! `silent_dependent_async`, in both of upstream's body shapes -- plus the
//! runtime-shaped branches of `_dependent_async` and
//! `_silent_dependent_async` in `taskflow/core/async.hpp` of the same
//! release, which are the two [`Executor`] forms #91 left out.
//!
//! This is what a runtime task's handle has been waiting for since #51. A
//! body can now spawn work **under its own node**, which is the only work a
//! [`Runtime::corun`] can wait for and the only thing that makes the implicit
//! join at the end of the body mean anything: until now a task a body
//! [`schedule`](Runtime::schedule)d was a *sibling*, counted against the
//! node's own parent, so the join counter a corun watches never rose and
//! every corun returned at once.
//!
//! # A child is a free node with a parent
//!
//! Upstream's four methods are one line each -- raise `_node->_join_counter`,
//! then call `_executor._silent_async(..., _node->_topology, _node)` -- and
//! the whole of what they do is pass the two arguments that say *where this
//! task belongs*: the run, and the node to count it against. The port's
//! answer to those two arguments is a [`Parent::Nested`] over the node's own
//! job, and it is one value rather than two because this port's run travels
//! with the job rather than with the node:
//!
//! - **the counting** is [`Parent::count_queued`], which for a
//!   [`Parent::Nested`] is a `fetch_add` on the parent node's join counter --
//!   upstream's own `_node->_join_counter.fetch_add(1)`, in the submission
//!   rather than in the caller. So the methods below do *not* raise the
//!   counter themselves: `Shared::schedule_async_task` and
//!   `Shared::schedule_dependent_async_task` take exactly one count per
//!   child, before the job can be queued, and a second one here would be a
//!   count nothing ever spends;
//! - **the run** stays the sentinel on the child's own job, which is
//!   [`Job::free`]'s only choice and is deliberate: `Topology::is_free_standing`
//!   is what both async dispatch arms use to refuse a handle somebody built
//!   into a taskflow's graph, so a child carrying the real run would be
//!   refused outright. The run that is actually waiting is reached through
//!   the chain instead -- #105 for a failure, #115 for a cancellation -- and
//!   both of those walks carry the run of the job at the chain's *root*
//!   precisely so that this shape answers for it.
//!
//! # What this module is not: the tear-down
//!
//! The parent branches of `_tear_down_async` and
//! `_tear_down_dependent_async` -- read the parent's `nstate` *before* the
//! `fetch_sub`, decrement, and if that reached zero and the parent was
//! `PREEMPTED` re-queue it -- are one function in this port,
//! `WorkerThread::finish_job`, and it has been there since #51 because every
//! nested graph needs it. So nothing here implements the other end of the
//! protocol: what this module adds is the first caller that can *reach* that
//! branch with a free node, and the `Parent::Nested` it hands the submission
//! is the whole of how.
//!
//! The order in `finish_job` is upstream's and is what makes a child safe to
//! finish at any moment: the bit is read before the decrement, because the
//! instant the counter reaches zero another worker may already be running
//! the resumed node and clearing it.
//!
//! # Where a child is queued, and the stack-depth cliff it costs (#119)
//!
//! In the shared buffers, like every other free node, and that is a place
//! this port and upstream differ with a consequence worth measuring.
//! Upstream's `_schedule_async_task` pushes onto the *submitting worker's*
//! queue when the submitter is a worker -- its `if(auto w = this_worker(); w)`
//! branch -- and this port collapses both branches onto the buffers because
//! the owning end of a worker's queue belongs to the `WorkerThread` frame
//! that called the body, which the body cannot name. `executor::async`'s
//! module docs have that argument in full.
//!
//! A runtime body is the **one** submitter in the crate for which that
//! argument does not hold: the handle it is given holds `&mut WorkerThread`,
//! because a corun needs it, so these four forms could take upstream's other
//! branch. They do not, and that is issue #119 rather than a decision made
//! here -- the destination belongs to the submission, which is one function
//! for every free node, and a second one differing only in where it pushes
//! would be a second copy of #91's dependency protocol and #115's counting
//! rule. #119 is blocked on this issue because this is where its route
//! lives.
//!
//! What it costs is **stack depth**, and the mechanism is worth stating
//! because it is not obvious: a worker's own queue is LIFO, so upstream's
//! corun pops the child it has just spawned -- the one it is waiting for --
//! while the buffers hand back the oldest outstanding job in the pool, which
//! is somebody else's unfinished subtree. A corun that picks that up coruns
//! *inside* a corun whose predicate is still unsatisfied, so recursive
//! spawning nests one frame per outstanding child rather than one per level
//! of recursion. In a debug build that overflows a worker's 2 MiB stack at
//! `fib(15)`, where the same recursion through a subflow -- a *graph*,
//! scheduled by the worker-aware path -- reaches `fib(20)`.
//! `tests/runtimes.rs` has the measurement and runs its recursive cases
//! inside it.
//!
//! None of that is a correctness claim: the work all runs, in any order, at
//! any worker count. What is bounded by the cliff is how deep a recursion
//! may go before the stack rather than the scheduler gives out.
//!
//! # What a failure under a child reaches
//!
//! The walk in `Job::record_panic`, from the child's node up its chain, which
//! for these four shapes ends at the runtime node itself:
//!
//! - an **entered** runtime node carries `IMPLICITLY_ANCHORED` (#51), so a
//!   child that panics leaves its payload in that node's slot and
//!   [`Runtime::try_corun`] hands it back -- or [`Runtime::corun`] re-raises
//!   it on this worker, which is upstream's `_rethrow_exception`;
//! - a node that is *also* explicitly anchored stops the walk one step
//!   earlier and in the same slot: a body inside a [`Runtime::corun`] holds
//!   that bit for the length of the wait, and an
//!   [`Executor::spawn_runtime`] node holds it for its whole life so that
//!   its `complete` can report the failure to the handle;
//! - and because the walk *marks* every base it passes, the first child to
//!   fail cancels its own siblings: `Job::is_parent_cancelled` reads the same
//!   bit one link down.
//!
//! A body that never coruns is the case worth being plain about: the payload
//! sits in the node's slot, the implicit join still waits for every child,
//! and the node then finishes **normally** -- a failure under a runtime task
//! of a graph that nobody collected is dropped by the next `set_up_graph` of
//! that graph, which is upstream's behaviour too. Corun if you want to know.
//!
//! # What cancelling the run reaches
//!
//! The children, through the chain described above: `Parent::ancestor_cancelled`
//! reads the estate of every parent and then the run of the job at the root
//! (#115), so a child queued before a cancellation is dequeued, skipped and
//! torn down without running -- which still counts it off its parent, so the
//! node is still resumed and the run still finishes. That is upstream's
//! `_is_parent_cancelled` over `_topology` and `_parent`, reached the long
//! way round because the child's own job holds the sentinel.
//!
//! # The cost: one parent per child
//!
//! Upstream passes a `Node*`. Each child here gets an `Arc<Nested>` holding a
//! clone of the runtime node's job -- one allocation and three atomic
//! increments -- because the parent has to own the job that resumes the node,
//! and because a [`Runtime`] is built fresh for each invocation and has
//! nowhere to cache one: its fields are borrows of the frame that built it.
//! A body that spawns a thousand children pays a thousand of those, which is
//! the same order as the node and the body it is already paying for, and the
//! alternative -- a field on the handle, built whether or not the body
//! spawns -- is a cost on every runtime task to save one on some.

use std::any::Any;

use crate::async_task::AsyncTask;
use crate::executor::Executor;
use crate::executor::r#async::{AsyncHandle, ESTATE_NONE};
use crate::executor::run_handle::RunError;
use crate::executor::topology::{Cancellable, Nested, Parent};
use crate::graph::AsyncWork;
use crate::runtime::Runtime;

/// The one argument a finishing entry hands `complete`: the node's payload
/// slot, which is upstream's `rt._node->_exception_ptr` and is `None` for a
/// task that did not fail.
///
/// Named because it is spelled out three times otherwise -- in the signature
/// below, in the closure it builds and in `graph`'s own alias for the boxed
/// body, which is private to that module.
type Payload = Option<Box<dyn Any + Send>>;

/// An empty [`AsyncHandle`] and the `complete` half of an
/// [`AsyncWork::Reentrant`] body that fills it: upstream's
/// `std::promise<void> p; auto fu{p.get_future()};` together with the
/// `reentered` branch that reads `rt._node->_exception_ptr` into it.
///
/// The pair every handle-returning *runtime-shaped* submission needs, and
/// there are four of them -- [`Executor::spawn_runtime`],
/// [`Executor::dependent_async_runtime`], [`Runtime::spawn_runtime`] and
/// [`Runtime::dependent_async_runtime`] -- which differ in the node kind and
/// the parent and in nothing else. Written once here rather than four times,
/// for the reason [`AsyncHandle::for_body`] is written once: the promise and
/// the future are two ends of one slot.
///
/// It is not a third construction site for a handle. [`AsyncHandle::pending`]
/// is the seam, and this is its one caller.
pub(crate) fn completion() -> (AsyncHandle<()>, impl FnOnce(Payload) + Send + 'static) {
    let (handle, delivery) = AsyncHandle::pending();

    // Upstream's `reentered` branch in full: `eptr ?
    // p.object.set_exception(eptr) : p.object.set_value()`.
    let complete = move |payload: Payload| {
        delivery.deliver(match payload {
            Some(payload) => Err(RunError::Panicked(payload)),
            None => Ok(()),
        });
    };

    (handle, complete)
}

impl Runtime<'_> {
    /// Spawns `body` under this runtime task and hands back a handle on its
    /// result: upstream's `Runtime::async`, under
    /// [`Executor::spawn`](crate::Executor::spawn)'s name for the reason
    /// `executor::async` gives.
    ///
    /// The child is counted against this runtime task's node, which is the
    /// whole difference from the executor's own `spawn`: it is **joined**
    /// before this task's successors run, whether the body waits for it with
    /// [`Runtime::corun`] or simply returns and leaves it to the implicit
    /// join. Dropping the handle therefore detaches the *result*, not the
    /// task -- unlike an executor-level `spawn`, the work is still waited for.
    ///
    /// A panic out of `body` is caught by the wrapper this call builds and
    /// reported from [`AsyncHandle::wait`] as
    /// [`RunError::Panicked`]`(payload)`, exactly as the executor's `spawn`
    /// reports one. So it does *not* fail this runtime task and does not
    /// cancel its siblings: a caught panic never reaches the walk the module
    /// docs describe. [`Runtime::silent_async`] is the form whose failure
    /// does.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let six = runtime.spawn(|| 6);
    ///     let seven = runtime.spawn(|| 7);
    ///     // Waiting on a handle blocks this worker, so the children need
    ///     // one of their own; `corun` is the shape that does not.
    ///     assert_eq!(six.wait().expect("no panic") * seven.wait().expect("no panic"), 42);
    /// });
    ///
    /// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn spawn<R, F>(&mut self, body: F) -> AsyncHandle<R>
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        // Upstream's `std::packaged_task<R()>` and the `get_future()` beside
        // it, which is the crate's one construction site for a handle on a
        // value: the node runs a plain body whatever `R` is.
        let (wrapper, handle) = AsyncHandle::for_body(body);
        self.spawn_child(AsyncWork::plain(wrapper), ESTATE_NONE);
        handle
    }

    /// Spawns `body` under this runtime task, handing it a [`Runtime`] of its
    /// own, and hands back a handle on its completion: upstream's
    /// `Runtime::async` over a `[](tf::Runtime&){}` body.
    ///
    /// The child is a runtime task in its own right, so it can spawn children
    /// of *its* own and corun on them -- which is how recursive parallelism
    /// is written, and what upstream's `Runtime.Fibonacci` is built out of.
    /// Everything [`Runtime::spawn`] says about the join applies here too,
    /// and the handle carries no value for
    /// [`Executor::spawn_runtime`](crate::Executor::spawn_runtime)'s reason:
    /// what there is to report about a runtime-shaped task is whether
    /// anything under it failed.
    ///
    /// # What a caller sees
    ///
    /// [`AsyncHandle::wait`] answers `Ok(())` once the child *and everything
    /// it spawned* are done, and [`RunError::Panicked`] if the child's body
    /// or anything under it panicked. The child's node is an explicit anchor
    /// from the moment it is created, so a failure under it stops there
    /// rather than reaching this runtime task -- which is also what keeps a
    /// failing grandchild from cancelling this task's other children.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let child = runtime.spawn_runtime(|child: &mut Runtime<'_>| {
    ///         child.silent_async(|| {});
    ///         child.corun();
    ///     });
    ///     runtime.corun();
    ///     // The implicit join has already happened: the corun above waited
    ///     // for the child, which waited for its own.
    ///     assert!(child.is_done());
    ///     child.wait().expect("nothing under the child failed");
    /// });
    ///
    /// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn spawn_runtime<F>(&mut self, body: F) -> AsyncHandle<()>
    where
        F: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static,
    {
        let (handle, complete) = completion();
        self.spawn_child(
            AsyncWork::reentrant(body, complete),
            // Upstream's `ESTATE::EXPLICITLY_ANCHORED`: there is a waiter --
            // whoever holds the handle -- for as long as the node exists.
            Cancellable::EXPLICITLY_ANCHORED,
        );
        handle
    }

    /// Spawns `body` under this runtime task with no handle at all:
    /// upstream's `Runtime::silent_async`.
    ///
    /// The cheapest of the four and the one its own tests are written with:
    /// there is no slot to deliver into, so a child costs the node, the body
    /// and the parent. It is still joined -- [`Runtime::spawn`]'s paragraph
    /// on that is the same here -- and what "silent" drops is the *result*,
    /// not the wait.
    ///
    /// A panic out of `body` is this runtime task's failure, which is the one
    /// place the silent form says more than the handle-returning one: the
    /// payload goes up the chain to the nearest anchor, which is this node,
    /// where [`Runtime::try_corun`] returns it and [`Runtime::corun`]
    /// re-raises it. It also cancels this task's other children, as a failure
    /// inside any nested graph cancels the rest of that graph. A body that
    /// never coruns drops it, which the module docs set out.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let counter = Arc::new(AtomicUsize::new(0));
    /// let spawned = Arc::clone(&counter);
    ///
    /// let mut taskflow = Taskflow::new();
    /// let a = taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
    ///     for _ in 0..100 {
    ///         let counter = Arc::clone(&spawned);
    ///         runtime.silent_async(move || {
    ///             counter.fetch_add(1, Ordering::Relaxed);
    ///         });
    ///     }
    ///     // No corun: the implicit join at the end of the body is what the
    ///     // successor below waits on.
    /// });
    /// let b = {
    ///     let counter = Arc::clone(&counter);
    ///     taskflow.emplace(move || assert_eq!(counter.load(Ordering::Relaxed), 100))
    /// };
    /// taskflow.task(a).precede([b]);
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn silent_async<F>(&mut self, body: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.spawn_child(AsyncWork::plain(body), ESTATE_NONE);
    }

    /// Spawns `body` under this runtime task, handing it a [`Runtime`] of its
    /// own, with no handle at all: upstream's `Runtime::silent_async` over a
    /// `[](tf::Runtime&){}` body.
    ///
    /// The form recursive parallelism is actually written with, because the
    /// recursion needs the child's `Runtime` and nothing needs the child's
    /// handle: upstream's `Runtime.Fibonacci`, `MergeSort` and `QuickSort`
    /// all spawn this way and let the join -- explicit through
    /// [`Runtime::corun`], or implicit at the end of the body -- do the
    /// waiting. [`Runtime::silent_async`]'s paragraph on where a panic goes
    /// holds here too, for the child's body and for anything under it.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let counter = Arc::new(AtomicUsize::new(0));
    /// let spawned = Arc::clone(&counter);
    ///
    /// let mut taskflow = Taskflow::new();
    /// let a = taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
    ///     for _ in 0..10 {
    ///         let counter = Arc::clone(&spawned);
    ///         runtime.silent_async_runtime(move |child: &mut Runtime<'_>| {
    ///             for _ in 0..10 {
    ///                 let counter = Arc::clone(&counter);
    ///                 child.silent_async(move || {
    ///                     counter.fetch_add(1, Ordering::Relaxed);
    ///                 });
    ///             }
    ///         });
    ///     }
    /// });
    /// let b = {
    ///     let counter = Arc::clone(&counter);
    ///     taskflow.emplace(move || assert_eq!(counter.load(Ordering::Relaxed), 100))
    /// };
    /// taskflow.task(a).precede([b]);
    ///
    /// // Each of the ten children is joined by its own implicit join, and
    /// // all ten by this task's -- so `b` sees all hundred.
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn silent_async_runtime<F>(&mut self, body: F)
    where
        F: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static,
    {
        self.spawn_child(AsyncWork::runtime(body), ESTATE_NONE);
    }

    /// Spawns `body` under this runtime task once every task in
    /// `dependencies` has finished, and hands back a handle on the task and a
    /// handle on its result: upstream's `Runtime::dependent_async`.
    ///
    /// [`Executor::dependent_async`](crate::Executor::dependent_async) with
    /// the task parented to this runtime node, so everything that call says
    /// about naming dependencies -- by handle, from any thread, an empty
    /// handle skipped, a finished one not waited for -- holds here, and
    /// everything [`Runtime::spawn`] says about the join holds too. The
    /// dependencies may be tasks this body spawned or tasks from anywhere
    /// else; what the parent decides is who waits for *this* one.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let order = Arc::new(AtomicUsize::new(0));
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
    ///     let order = Arc::clone(&order);
    ///     let (first, one) = runtime.dependent_async(
    ///         {
    ///             let order = Arc::clone(&order);
    ///             move || order.fetch_add(1, Ordering::Relaxed)
    ///         },
    ///         [],
    ///     );
    ///     let (_second, two) = runtime.dependent_async(
    ///         move || order.fetch_add(1, Ordering::Relaxed),
    ///         [&first],
    ///     );
    ///     assert_eq!(one.wait().expect("no panic"), 0);
    ///     assert_eq!(two.wait().expect("no panic"), 1);
    /// });
    ///
    /// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn dependent_async<'t, R, F>(
        &mut self,
        body: F,
        dependencies: impl IntoIterator<Item = &'t AsyncTask>,
    ) -> (AsyncTask, AsyncHandle<R>)
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        let (wrapper, handle) = AsyncHandle::for_body(body);
        let task = self.spawn_dependent_child(AsyncWork::plain(wrapper), ESTATE_NONE, dependencies);
        (task, handle)
    }

    /// Spawns `body` under this runtime task once every task in
    /// `dependencies` has finished, handing it a [`Runtime`] of its own, and
    /// hands back a handle on the task and one on its completion: upstream's
    /// `Runtime::dependent_async` over a `[](tf::Runtime&){}` body.
    ///
    /// [`Runtime::dependent_async`] and [`Runtime::spawn_runtime`] in one
    /// call: the task waits for its dependencies, is joined by this runtime
    /// task, and is itself a runtime task that can spawn and corun.
    /// `AsyncHandle<()>` for the reason that method gives.
    pub fn dependent_async_runtime<'t, F>(
        &mut self,
        body: F,
        dependencies: impl IntoIterator<Item = &'t AsyncTask>,
    ) -> (AsyncTask, AsyncHandle<()>)
    where
        F: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static,
    {
        let (handle, complete) = completion();
        let task = self.spawn_dependent_child(
            AsyncWork::reentrant(body, complete),
            Cancellable::EXPLICITLY_ANCHORED,
            dependencies,
        );
        (task, handle)
    }

    /// Spawns `body` under this runtime task once every task in
    /// `dependencies` has finished, with no handle on its result: upstream's
    /// `Runtime::silent_dependent_async`.
    ///
    /// The [`AsyncTask`] is still handed back, because that is what names
    /// this task as a dependency of the next one -- which is how a chain is
    /// built from inside a body, and what upstream's
    /// `SilentDependentAsync.FromRuntime` does a thousand times over.
    /// [`Runtime::silent_async`]'s paragraph on where a panic goes holds
    /// here, with one addition that is
    /// [`Executor::silent_dependent_async`](crate::Executor::silent_dependent_async)'s
    /// too: the payload is also readable through
    /// [`AsyncTask::take_panic`](crate::AsyncTask::take_panic), since the
    /// handle names the node it was parked on.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let order = Arc::new(AtomicUsize::new(0));
    /// let counted = Arc::clone(&order);
    ///
    /// let mut taskflow = Taskflow::new();
    /// let a = taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
    ///     // A chain of ten, each submitted while the one before it may
    ///     // already be running, and each asserting that it ran after it.
    ///     let mut previous = None;
    ///     for step in 0..10 {
    ///         let order = Arc::clone(&counted);
    ///         let task = runtime.silent_dependent_async(
    ///             move || assert_eq!(order.fetch_add(1, Ordering::Relaxed), step),
    ///             previous.as_ref(),
    ///         );
    ///         previous = Some(task);
    ///     }
    ///     // The implicit join waits for the whole chain.
    /// });
    /// let b = {
    ///     let order = Arc::clone(&order);
    ///     taskflow.emplace(move || assert_eq!(order.load(Ordering::Relaxed), 10))
    /// };
    /// taskflow.task(a).precede([b]);
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn silent_dependent_async<'t, F>(
        &mut self,
        body: F,
        dependencies: impl IntoIterator<Item = &'t AsyncTask>,
    ) -> AsyncTask
    where
        F: FnOnce() + Send + 'static,
    {
        self.spawn_dependent_child(AsyncWork::plain(body), ESTATE_NONE, dependencies)
    }

    /// Spawns `body` under this runtime task once every task in
    /// `dependencies` has finished, handing it a [`Runtime`] of its own and
    /// no handle on its result: upstream's `Runtime::silent_dependent_async`
    /// over a `[](tf::Runtime&){}` body.
    ///
    /// [`Runtime::silent_dependent_async`] whose body can spawn and corun,
    /// which is the fourth corner of the family. Upstream's own
    /// `FromRuntime` cases alternate between this and the plain form along
    /// one chain, which is what pins that the node kind decides the protocol
    /// and the body shape decides only who is handed what.
    pub fn silent_dependent_async_runtime<'t, F>(
        &mut self,
        body: F,
        dependencies: impl IntoIterator<Item = &'t AsyncTask>,
    ) -> AsyncTask
    where
        F: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static,
    {
        self.spawn_dependent_child(AsyncWork::runtime(body), ESTATE_NONE, dependencies)
    }

    /// Submits one free node under this runtime task's own node: upstream's
    /// `_executor._silent_async(..., _node->_topology, _node)`, which its
    /// four methods differ in nothing but the arguments to.
    ///
    /// Through [`Shared`](crate::executor::Shared) rather than through an
    /// [`Executor`], which is the one plumbing fact about this whole module:
    /// a [`Runtime`] holds the worker's share of the shared state and no
    /// `Executor` at all -- `src/runtime.rs` says why there is none to hold
    /// -- so the submission hangs off what a worker has.
    ///
    /// Where that submission *queues* the child is the buffers rather than
    /// this worker's own queue, which the module docs' last section measures:
    /// it is upstream's other branch, it is reachable from here and from
    /// nowhere else in the crate, and it is #119's to take.
    fn spawn_child(&self, work: AsyncWork, estate: u32) {
        self.thread
            .shared
            .schedule_async_task(work, &self.child_parent(), estate);
    }

    /// [`Runtime::spawn_child`] for a node with dependencies: upstream's
    /// `_executor._silent_dependent_async(..., _node->_topology, _node)`.
    fn spawn_dependent_child<'t>(
        &self,
        work: AsyncWork,
        estate: u32,
        dependencies: impl IntoIterator<Item = &'t AsyncTask>,
    ) -> AsyncTask {
        self.thread.shared.schedule_dependent_async_task(
            work,
            &self.child_parent(),
            estate,
            dependencies,
        )
    }

    /// The parent one child of this runtime task is scheduled under:
    /// upstream's `_node` as the `NodeBase* parent` argument.
    ///
    /// The job is cloned into it because that is what resumes the node -- the
    /// last child to finish re-queues exactly this value -- and a fresh
    /// [`Nested`] per child because there is nowhere to keep one; the module
    /// docs have what that costs and why the alternative costs more. Every
    /// one of them resolves to the same node, so they are the same parent in
    /// every way the scheduler reads: [`Nested::base`] is that node's state
    /// words, which is where the join counter and the `PREEMPTED` bit are.
    fn child_parent(&self) -> Parent {
        Nested::new(Some(self.job.clone())).as_parent()
    }
}

impl Executor {
    /// Runs `body` on this executor once every task in `dependencies` has
    /// finished, handing it the scheduler, and hands back a handle on the
    /// task and one on its completion.
    ///
    /// Upstream's `dependent_async` over a `[](tf::Runtime&){}` body -- the
    /// `is_runtime_task_v<F>` branch of `_dependent_async` -- which is
    /// [`Executor::dependent_async`](crate::Executor::dependent_async)'s
    /// dependencies and [`Executor::spawn_runtime`](crate::Executor::spawn_runtime)'s
    /// body and second entry. It lives here rather than beside the plain form
    /// because what is particular to it is the protocol that module docs set
    /// out, and #91 left the pair for this issue for that reason.
    ///
    /// # What a caller sees
    ///
    /// [`AsyncHandle::wait`] answers `Ok(())` once the body and everything it
    /// spawned are done, and [`RunError::Panicked`] if any of that panicked:
    /// the node is an explicit anchor for its whole life, so the walk stops
    /// there and `complete` hands the slot to the handle. The [`AsyncTask`]
    /// is what names this task as a dependency of a later one, and
    /// [`AsyncTask::is_done`](crate::AsyncTask::is_done) is true of it only
    /// once the second entry has run -- which is to say once the children
    /// are done too, since that is what the second entry waits for.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::{Executor, Runtime};
    ///
    /// let executor = Executor::new(4);
    /// let counter = Arc::new(AtomicUsize::new(0));
    ///
    /// let first = executor.silent_dependent_async(|| {}, []);
    /// let (_task, done) = executor.dependent_async_runtime(
    ///     {
    ///         let counter = Arc::clone(&counter);
    ///         move |runtime: &mut Runtime<'_>| {
    ///             for _ in 0..10 {
    ///                 let counter = Arc::clone(&counter);
    ///                 runtime.silent_async(move || {
    ///                     counter.fetch_add(1, Ordering::Relaxed);
    ///                 });
    ///             }
    ///         }
    ///     },
    ///     [&first],
    /// );
    ///
    /// // `Ok(())` is "the body and its children are done", so the count is
    /// // settled by the time this returns.
    /// done.wait().expect("nothing under it failed");
    /// assert_eq!(counter.load(Ordering::Relaxed), 10);
    /// ```
    pub fn dependent_async_runtime<'t, F>(
        &self,
        body: F,
        dependencies: impl IntoIterator<Item = &'t AsyncTask>,
    ) -> (AsyncTask, AsyncHandle<()>)
    where
        F: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static,
    {
        let (handle, complete) = completion();
        let task = self.schedule_dependent_async_task(
            AsyncWork::reentrant(body, complete),
            &Parent::Run,
            // Upstream's `ESTATE::EXPLICITLY_ANCHORED | ESTATE::REFCOUNT_ONE`,
            // minus the count the `Arc` already is: a failure under this node
            // is the handle's to report, so the node is the anchor.
            Cancellable::EXPLICITLY_ANCHORED,
            dependencies,
        );
        (task, handle)
    }

    /// Runs `body` on this executor once every task in `dependencies` has
    /// finished, handing it the scheduler, with no handle on its completion.
    ///
    /// Upstream's `silent_dependent_async` over a `[](tf::Runtime&){}` body,
    /// which is one call with one `ESTATE::NONE` there -- its
    /// `_silent_dependent_async` does not branch on the body's shape at all,
    /// because the shape decides only who is handed what and
    /// [`AsyncWork`] is where that is said.
    ///
    /// # What a caller sees
    ///
    /// The [`AsyncTask`], and nothing else. A panic out of `body` or out of
    /// anything it spawned lands in the node's own slot, where
    /// [`AsyncTask::take_panic`](crate::AsyncTask::take_panic) reads it back
    /// -- the node is *not* an explicit anchor here, so the walk stops at it
    /// only because an entered runtime node is an implicit one, and nothing
    /// is waiting to collect it. [`Executor::wait_for_all`] is what waits for
    /// such a task, as it is for every free node.
    pub fn silent_dependent_async_runtime<'t, F>(
        &self,
        body: F,
        dependencies: impl IntoIterator<Item = &'t AsyncTask>,
    ) -> AsyncTask
    where
        F: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static,
    {
        self.schedule_dependent_async_task(
            AsyncWork::runtime(body),
            &Parent::Run,
            ESTATE_NONE,
            dependencies,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::executor::queue::WorkerQueue;
    use crate::executor::topology::{GraphOwner, Job, Topology, set_up_graph};
    use crate::executor::worker::{WorkerThread, is_entered};
    use crate::prelude::*;

    /// How long a case waits for a worker to deliver before it gives up.
    ///
    /// The deadline `executor::runtime` and `tests/basics.rs` both keep, for
    /// the reason they give: nothing here passes because of this number, and
    /// the only thing that reaches it is a side that is never going to
    /// arrive.
    const GIVE_UP_AFTER: Duration = Duration::from_secs(120);

    /// Waits for `handle` to have an outcome, failing rather than hanging if
    /// it never gets one.
    ///
    /// [`AsyncHandle::wait`] has no deadline of its own, which matters more in
    /// this module than anywhere else: the one case below that waits inside a
    /// *body* would park a worker for ever on a regression -- and with it the
    /// executor's own `Drop`, which joins that worker -- where this fails the
    /// case instead. `executor::runtime` keeps the same helper for the test
    /// thread's side of it.
    fn spin_until_done<R>(what: &str, handle: &AsyncHandle<R>) {
        let deadline = Instant::now() + GIVE_UP_AFTER;
        while !handle.is_done() {
            assert!(Instant::now() < deadline, "gave up waiting for {what}");
            thread::yield_now();
        }
    }

    /// A worker thread with a queue nobody else holds the other end of, for
    /// driving the scheduler one job at a time from the test thread.
    ///
    /// The helper `executor::runtime` and `executor::topology` both take, for
    /// the reason they give: the pool's real threads must not be able to
    /// steal what these cases queue.
    fn test_worker(executor: &Executor) -> WorkerThread {
        WorkerThread::new(Arc::clone(&executor.shared), 0, WorkerQueue::new())
    }

    /// A run of `taskflow`'s graph, armed and accounted for but with nothing
    /// queued: what a case here invokes a job of.
    ///
    /// `on_finish` is the run policy's callback, which the tear-down calls
    /// once the run's join counter reaches zero -- the one observable a
    /// hand-driven run has for "the accounting balanced", and the assertion
    /// three of the cases below turn on.
    fn run_of(taskflow: &Taskflow, on_finish: impl FnOnce() + Send + 'static) -> Arc<Topology> {
        let flow = taskflow.flow();
        let topology = Arc::new(Topology::new(|| true, on_finish, Arc::clone(flow)));
        let sources = set_up_graph(flow.graph());
        Parent::Run.count_queued(&topology, sources);
        topology
    }

    /// The job of `taskflow`'s one source task, under the run itself.
    ///
    /// Every case here builds a graph whose runtime task is the only source,
    /// which is what makes this unambiguous: a [`Job`] holds its task index
    /// privately, so the only way to name one is to leave the scheduler one
    /// choice.
    fn source_job(taskflow: &Taskflow, topology: &Arc<Topology>) -> Job {
        let owner = GraphOwner::flow(taskflow.flow());
        let parent = Parent::Run;
        let mut sources = Job::sources(&owner, topology, &parent);
        let job = sources.next().expect("the graph has a source task");
        assert!(sources.next().is_none(), "the cases here have one source");
        job
    }

    /// Takes one child's job back out of the shared buffers, for a case that
    /// runs it by hand.
    ///
    /// # Why the pool cannot race this
    ///
    /// A worker skips the steal loop entirely while `num_topologies` is zero
    /// -- `WorkerThread::explore_task`'s fast path -- and a hand-driven run
    /// leaves it at zero, because nothing here goes through
    /// `Executor::run`. A parented child does not raise it either, which is
    /// #115's rule. So every job these cases queue stays queued until the
    /// test thread takes it, and the orderings they are about are fixed
    /// rather than raced for. `executor::async`'s own helper says the same
    /// thing from the other side.
    ///
    /// **The flip side, for whoever adds a case here:** that same fast path
    /// is why a hand-driven case must not call [`Runtime::corun`] with a
    /// child outstanding. The corun loop would find nothing to steal, yield,
    /// and go round again for ever -- a hang rather than a failure. Drive the
    /// children with this instead, and leave the corun cases to
    /// `tests/runtimes.rs`, where a real run holds the count above zero.
    fn take_child_job(executor: &Executor) -> Job {
        let buffer = executor
            .shared
            .buffers
            .first_nonempty()
            .expect("the submission pushed a job");
        executor
            .shared
            .buffers
            .steal_from(buffer)
            .expect("the job the submission pushed")
    }

    /// A child raises its parent's join counter, and the **last** child to
    /// finish re-queues the preempted node exactly once: upstream's
    /// `_node->_join_counter.fetch_add(1)` per child against the parent
    /// branch of `_tear_down_async`.
    ///
    /// This is the case `executor::runtime`'s
    /// `a_body_that_leaves_work_outstanding_is_preempted_and_resumed` used to
    /// drive by hand, now driven through `silent_async`: the counter is
    /// raised by a real submission, spent by a real tear-down, and the
    /// resumption is the scheduler's rather than the test's.
    ///
    /// **How a regression surfaces**: as a failed count. A child that was not
    /// counted leaves the node finishing on its first entry, so the
    /// successor runs while the child is still in flight; a resumption queued
    /// twice enters the node a third time, which re-runs the body and
    /// releases the successor again. Both are assertions below rather than
    /// timing, because every job is driven from this thread -- the helper
    /// above has why the pool cannot interfere.
    #[test]
    fn a_child_raises_its_parents_counter_and_the_last_child_resumes_it_once() {
        let runs = Arc::new(AtomicUsize::new(0));
        let children = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicBool::new(false));

        let mut taskflow = Taskflow::new();
        let rt = {
            let runs = Arc::clone(&runs);
            let children = Arc::clone(&children);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                runs.fetch_add(1, Ordering::Relaxed);
                for _ in 0..2 {
                    let children = Arc::clone(&children);
                    runtime.silent_async(move || {
                        children.fetch_add(1, Ordering::Relaxed);
                    });
                }

                // Read from inside the body, which is the only place it can
                // be read: three references -- the two children and the
                // body's own, which the first entry spends on the way out.
                assert_eq!(
                    runtime.node.join_counter().load(Ordering::Relaxed),
                    3,
                    "each child raised the node's counter, and the body still \
                     holds its own reference"
                );
            })
        };
        let after = {
            let released = Arc::clone(&released);
            taskflow.emplace(move || {
                released.fetch_add(1, Ordering::Relaxed);
            })
        };
        taskflow.task(rt).precede([after]);

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, {
            let finished = Arc::clone(&finished);
            move || finished.store(true, Ordering::Relaxed)
        });

        worker.invoke(source_job(&taskflow, &topology));

        let node = taskflow.graph().node(rt);
        assert_eq!(runs.load(Ordering::Relaxed), 1, "the body ran once");
        assert!(is_entered(node), "and the node preempted itself");
        assert_eq!(
            node.join_counter().load(Ordering::Relaxed),
            2,
            "the two children are outstanding, the body's reference spent"
        );
        assert_eq!(
            released.load(Ordering::Relaxed),
            0,
            "a preempted node releases nothing: its successor waits for the \
             children"
        );
        assert!(worker.pop().is_none(), "and queued nothing of its own");

        // The first child: counted off, nothing resumed.
        worker.invoke(take_child_job(&executor));
        assert_eq!(children.load(Ordering::Relaxed), 1, "one child has run");
        assert_eq!(
            node.join_counter().load(Ordering::Relaxed),
            1,
            "and was counted off its parent"
        );
        assert!(
            worker.pop().is_none(),
            "but it was not the last, so nothing was resumed"
        );

        // The last child: the one that re-queues the node.
        worker.invoke(take_child_job(&executor));
        assert_eq!(children.load(Ordering::Relaxed), 2, "both children ran");
        assert_eq!(
            node.join_counter().load(Ordering::Relaxed),
            0,
            "and the node's counter reached zero"
        );

        let resume = worker
            .pop()
            .expect("the last child re-queued the preempted node");
        assert!(
            worker.pop().is_none(),
            "exactly once: one resumption, not one per child"
        );

        worker.invoke(resume);

        assert_eq!(
            runs.load(Ordering::Relaxed),
            1,
            "the second entry resumes the node rather than running the body \
             again"
        );
        assert!(!is_entered(node), "it clears both bits");
        assert_eq!(
            released.load(Ordering::Relaxed),
            1,
            "and releases the successor, once -- which since #62 means it ran \
             here as the continuation"
        );
        assert!(worker.pop().is_none(), "so nothing was left queued");
        assert!(
            finished.load(Ordering::Relaxed),
            "the run finished exactly when its last job did, so every child \
             was accounted for"
        );
    }

    /// A child of a **cancelled run** is torn down without running, and the
    /// tear-down still counts it off its parent -- which is what lets the
    /// preempted node resume and the run finish rather than hang.
    ///
    /// Upstream's `_is_parent_cancelled` over `_topology` and `_parent`,
    /// which for a child reached this way is the chain walk #115 fixed: the
    /// child's own job holds the sentinel run, so the run that is actually
    /// waiting is read off the job at the chain's root.
    ///
    /// **How a regression surfaces**: as a non-zero `children` count, which
    /// is work done on behalf of a run that has already been given up on. A
    /// regression in the *tear-down* half shows up as the missing resumption
    /// or the missing callback below instead -- a cancelled run that never
    /// finishes is upstream's own worst case and this port's too.
    #[test]
    fn a_child_of_a_cancelled_run_is_torn_down_without_running() {
        let children = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicBool::new(false));

        let mut taskflow = Taskflow::new();
        let rt = {
            let children = Arc::clone(&children);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let children = Arc::clone(&children);
                runtime.silent_async(move || {
                    children.fetch_add(1, Ordering::Relaxed);
                });
            })
        };
        let after = {
            let released = Arc::clone(&released);
            taskflow.emplace(move || {
                released.fetch_add(1, Ordering::Relaxed);
            })
        };
        taskflow.task(rt).precede([after]);

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, {
            let finished = Arc::clone(&finished);
            move || finished.store(true, Ordering::Relaxed)
        });

        worker.invoke(source_job(&taskflow, &topology));

        let node = taskflow.graph().node(rt);
        let child = take_child_job(&executor);
        assert!(is_entered(node), "the node is waiting for its child");

        // Cancelled with the child queued and not yet dequeued, which is the
        // only window in which a child can be skipped at all: cancelling
        // earlier would have skipped the runtime task itself.
        assert!(topology.cancel(), "the run had not finished");

        worker.invoke(child);

        assert_eq!(
            children.load(Ordering::Relaxed),
            0,
            "the child was skipped rather than run"
        );
        assert_eq!(
            node.join_counter().load(Ordering::Relaxed),
            0,
            "and still counted off its parent: a skipped job owes the same \
             tear-down as one that ran"
        );

        let resume = worker
            .pop()
            .expect("the skipped child still re-queued the preempted node");
        worker.invoke(resume);

        assert!(!is_entered(node), "the node finished");
        assert_eq!(
            released.load(Ordering::Relaxed),
            0,
            "its successor was released and then skipped, as every task of a \
             cancelled run is"
        );
        assert!(
            finished.load(Ordering::Relaxed),
            "and the run finished, promptly, which is the whole of what \
             cancelling promises"
        );
    }

    /// A child spawned with [`Runtime::spawn`] delivers its value to a
    /// [`AsyncHandle::wait`] **inside the body**: upstream's
    /// `fu1.get()` in the `Runtime::async` documentation.
    ///
    /// Two workers, and the count is the point rather than a detail: a `wait`
    /// inside a body blocks the worker running that body, so the child needs
    /// a worker of its own to run on. That is why upstream's
    /// `Exception.RuntimeAsync` starts at two threads, and why this case
    /// would deadlock at one -- [`Runtime::corun`] is the shape that does
    /// not, and `tests/runtimes.rs` exercises it from one worker upwards.
    ///
    /// **How a regression surfaces**: as the deadline inside the body, not as
    /// a hang. The spin before the `wait` is what turns a child that never
    /// ran into a failed assertion -- the panic goes into the task's own
    /// handle, which the assertion at the end of the case reads back.
    #[test]
    fn a_waited_child_hands_its_value_back_inside_the_body() {
        let executor = Executor::new(2);
        let seen = Arc::new(AtomicI32::new(0));

        let task = executor.spawn_runtime({
            let seen = Arc::clone(&seen);
            move |runtime: &mut Runtime<'_>| {
                let child = runtime.spawn(|| 6 * 7);
                spin_until_done("the child spawned under this runtime task", &child);
                seen.store(
                    child.wait().expect("the child did not panic"),
                    Ordering::Relaxed,
                );
            }
        });

        spin_until_done("the runtime task itself", &task);
        task.wait().expect("nothing under it failed");

        assert_eq!(
            seen.load(Ordering::Relaxed),
            42,
            "the handle handed back what the child's body returned"
        );

        executor.wait_for_all();
        assert_eq!(
            executor.num_topologies(),
            0,
            "and both tasks were accounted for exactly once"
        );
    }

    /// A **skipped** dependent child still releases what was waiting for it:
    /// upstream's `_tear_down_invoke`, whose cancelled early-out dispatches
    /// on the handle and so reaches the whole of
    /// `_tear_down_dependent_async` -- `FINISHED` set, the successor list
    /// drained -- rather than only the parent branch.
    ///
    /// This is the one invariant #91 could state and #92 cannot. Its module
    /// docs say "a dependent-async task submitted to the executor is never
    /// skipped", which was true while every free node carried
    /// `Parent::Run` over the sentinel: nothing can cancel the sentinel. A
    /// child of a *runtime task* has a real run above it, so the early-out at
    /// the top of the dispatch can now skip one -- and a skipped job that
    /// counted itself off its parent without draining its list would strand
    /// every task waiting on it. Those tasks were counted against the runtime
    /// node when they were submitted, so the node's join counter never
    /// reaches zero: the node is never resumed, the run never finishes, and a
    /// `wait` on it never returns.
    ///
    /// **How a regression surfaces**: as the missing resumption and the
    /// missing callback below, not as a hang -- every job is driven from this
    /// thread, so the run that would never finish simply has not finished
    /// when the assertions look.
    #[test]
    fn a_skipped_dependent_child_still_releases_what_was_waiting_for_it() {
        let ran = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicBool::new(false));

        let mut taskflow = Taskflow::new();
        let rt = {
            let ran = Arc::clone(&ran);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let first = runtime.silent_dependent_async(
                    {
                        let ran = Arc::clone(&ran);
                        move || {
                            ran.fetch_add(1, Ordering::Relaxed);
                        }
                    },
                    [],
                );
                let ran = Arc::clone(&ran);
                runtime.silent_dependent_async(
                    move || {
                        ran.fetch_add(1, Ordering::Relaxed);
                    },
                    [&first],
                );
            })
        };

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, {
            let finished = Arc::clone(&finished);
            move || finished.store(true, Ordering::Relaxed)
        });

        worker.invoke(source_job(&taskflow, &topology));

        let node = taskflow.graph().node(rt);
        assert_eq!(
            node.join_counter().load(Ordering::Relaxed),
            2,
            "both links of the chain are counted against the node, including \
             the one that is still waiting for its dependency"
        );

        // The second link is not queued -- it owes its dependency -- so the
        // one job in the buffers is the first.
        let first = take_child_job(&executor);
        assert!(topology.cancel(), "the run had not finished");

        worker.invoke(first);

        assert_eq!(
            ran.load(Ordering::Relaxed),
            0,
            "the first link was skipped rather than run"
        );

        // What the skipped job still owes: the list it was holding. Draining
        // it is what queues the second link, whose own tear-down is the only
        // thing that can spend the count it took at submission.
        let second = worker
            .pop()
            .expect("the skipped link released the task waiting on it");
        worker.invoke(second);

        assert_eq!(
            ran.load(Ordering::Relaxed),
            0,
            "and the second link was skipped too"
        );
        assert_eq!(
            node.join_counter().load(Ordering::Relaxed),
            0,
            "both links counted themselves off the node"
        );

        let resume = worker
            .pop()
            .expect("the last of them re-queued the preempted node");
        worker.invoke(resume);

        assert!(!is_entered(node), "the node finished");
        assert!(
            finished.load(Ordering::Relaxed),
            "and the run finished rather than waiting for ever on a task \
             that nothing was ever going to queue"
        );
    }
}

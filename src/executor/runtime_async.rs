//! Translated from `taskflow/core/runtime.hpp` (Taskflow v4.1.0): the `async`
//! family of `tf::Runtime` -- `async`, `silent_async`, `dependent_async` and
//! `silent_dependent_async`, in both of upstream's body shapes -- plus the
//! runtime-shaped branches of `_dependent_async` and
//! `_silent_dependent_async` in `taskflow/core/async.hpp` of the same
//! release, which are the two [`Executor`] forms at the end of this module.
//!
//! A body can spawn work **under its own node**, which is the only work a
//! [`Runtime::corun`] can wait for and the only thing that makes the implicit
//! join at the end of the body mean anything: a task a body
//! [`schedule`](Runtime::schedule)s is a *sibling*, counted against the
//! node's own parent.
//!
//! # Design
//!
//! Upstream's four methods are one line each -- raise `_node->_join_counter`,
//! then call `_executor._silent_async(..., _node->_topology, _node)` -- and
//! the ten here are one line each for the same reason: the submission core in
//! [`async`](crate::executor::r#async) takes everything that varies, so a
//! method of this module decides only which [`Spawn`] shape the body is and
//! which [`Submission`] it goes under.
//!
//! [`Submission::under_node`] answers upstream's two trailing arguments in
//! one value, and three things follow. The counting is
//! `Parent::count_queued`, which the submission itself takes -- upstream's
//! own `fetch_add(1)`, moved one call inwards -- so nothing here raises a
//! counter and a second count would be one nothing ever spends. The run stays
//! the sentinel on the child's own job, which is what both async dispatch
//! arms test to refuse a handle somebody built into a graph, so the run that
//! is actually waiting is reached up the parent chain instead. And the worker
//! it names is this body's own, upstream's `if(auto w = this_worker(); w)`
//! branch and the one branch only a runtime handle can take, a [`Runtime`]
//! holding `&mut WorkerThread` because a corun needs it. `DESIGN.md` has what
//! that destination buys -- a recursion bounded by its own depth rather than
//! by the size of its task tree -- and `tests/runtimes.rs` carries the
//! measurement. One case keeps the shared buffers and is the same rule rather
//! than an exception: a runtime body invoked by a *corun loop* runs on a
//! `WorkerThread::corun_view`, which has no owning end to lend.
//!
//! The tear-down is not here. The parent branches of `_tear_down_async` and
//! `_tear_down_dependent_async` are `WorkerThread::finish_job`, which every
//! nested graph needs; what this module adds is the first caller that can
//! *reach* that branch with a free node.
//!
//! A failure under a child takes the walk in `Job::record_panic` up its
//! chain, which for these shapes ends at the runtime node itself: an entered
//! runtime node is an implicit anchor, so the payload lands in that node's
//! slot, where [`Runtime::try_corun`] hands it back and [`Runtime::corun`]
//! re-raises it -- and because the walk marks every base it passes, the first
//! child to fail cancels its siblings. A child that is *itself* explicitly
//! anchored stops the walk one step earlier, in its own slot. A body that
//! never coruns is the case worth being plain about: the implicit join still
//! waits for every child and the node then finishes **normally**, so a
//! failure nobody collected is dropped by the next `set_up_graph` of that
//! graph, which is upstream's behaviour too. Cancelling the run reaches the
//! children through the same chain, and a child skipped that way is still
//! counted off its parent, so the node is still resumed and the run still
//! finishes.

use crate::async_task::AsyncTask;
use crate::executor::Executor;
use crate::executor::r#async::{AsyncHandle, Spawn, Submission};
use crate::runtime::Runtime;

impl Runtime<'_> {
    /// Spawns `body` under this runtime task and hands back a handle on its
    /// result: upstream's `Runtime::async`, under
    /// [`Executor::spawn`](crate::Executor::spawn)'s name.
    ///
    /// The child is counted against this runtime task's node, which is the
    /// whole difference from the executor's own `spawn`: it is **joined**
    /// before this task's successors run, whether the body waits for it with
    /// [`Runtime::corun`] or leaves it to the implicit join. Dropping the
    /// handle therefore detaches the *result*, not the task.
    ///
    /// A panic out of `body` is reported from [`AsyncHandle::wait`] as the
    /// executor's `spawn` reports one, so it does *not* fail this runtime
    /// task and does not cancel its siblings -- a caught panic never reaches
    /// the walk. [`Runtime::silent_async`] is the form whose failure does.
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
        self.thread.shared.schedule_async_task(
            Spawn::handled(body),
            &Submission::under_node(self.job, &*self.thread),
        )
    }

    /// Spawns `body` under this runtime task, handing it a [`Runtime`] of its
    /// own, and hands back a handle on its completion: upstream's
    /// `Runtime::async` over a `[](tf::Runtime&){}` body.
    ///
    /// The child is a runtime task in its own right, so it can spawn children
    /// of *its* own and corun on them -- which is how recursive parallelism
    /// is written, and what upstream's `Runtime.Fibonacci` is built out of.
    /// Everything [`Runtime::spawn`] says about the join applies here too.
    ///
    /// [`AsyncHandle::wait`] answers `Ok(())` once the child *and everything
    /// it spawned* are done, and an error if any of that panicked: the
    /// child's node is an explicit anchor from the moment it is created, so a
    /// failure under it stops there rather than reaching this runtime task or
    /// cancelling its siblings.
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
        self.thread.shared.schedule_async_task(
            Spawn::handled_runtime(body),
            &Submission::under_node(self.job, &*self.thread),
        )
    }

    /// Spawns `body` under this runtime task with no handle at all:
    /// upstream's `Runtime::silent_async`.
    ///
    /// The cheapest of the four and the one its own tests are written with.
    /// It is still joined -- [`Runtime::spawn`]'s paragraph on that is the
    /// same here -- and what "silent" drops is the *result*, not the wait.
    ///
    /// A panic out of `body` is this runtime task's failure, which is the one
    /// place the silent form says more than the handle-returning one: the
    /// payload goes up the chain to the nearest anchor, this node, where
    /// [`Runtime::try_corun`] returns it and [`Runtime::corun`] re-raises it,
    /// and it cancels this task's other children. A body that never coruns
    /// drops it.
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
        self.thread.shared.schedule_async_task(
            Spawn::silent(body),
            &Submission::under_node(self.job, &*self.thread),
        );
    }

    /// Spawns `body` under this runtime task, handing it a [`Runtime`] of its
    /// own, with no handle at all: upstream's `Runtime::silent_async` over a
    /// `[](tf::Runtime&){}` body.
    ///
    /// The form recursive parallelism is actually written with, because the
    /// recursion needs the child's `Runtime` and nothing needs its handle:
    /// upstream's `Runtime.Fibonacci`, `MergeSort` and `QuickSort` all spawn
    /// this way and let the join -- explicit through [`Runtime::corun`], or
    /// implicit at the end of the body -- do the waiting.
    /// [`Runtime::silent_async`] on where a panic goes holds here too, for
    /// the child's body and for anything under it.
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
        self.thread.shared.schedule_async_task(
            Spawn::silent_runtime(body),
            &Submission::under_node(self.job, &*self.thread),
        );
    }

    /// Spawns `body` under this runtime task once every task in
    /// `dependencies` has finished, and hands back a handle on the task and a
    /// handle on its result: upstream's `Runtime::dependent_async`.
    ///
    /// [`Executor::dependent_async`](crate::Executor::dependent_async) with
    /// the task parented to this runtime node, so what that call says about
    /// naming dependencies and what [`Runtime::spawn`] says about the join
    /// both hold. The dependencies may be tasks this body spawned or tasks
    /// from anywhere else; the parent decides who waits for *this* one.
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
        self.thread.shared.schedule_dependent_async_task(
            Spawn::handled(body),
            &Submission::under_node(self.job, &*self.thread),
            dependencies,
        )
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
        self.thread.shared.schedule_dependent_async_task(
            Spawn::handled_runtime(body),
            &Submission::under_node(self.job, &*self.thread),
            dependencies,
        )
    }

    /// Spawns `body` under this runtime task once every task in
    /// `dependencies` has finished, with no handle on its result: upstream's
    /// `Runtime::silent_dependent_async`.
    ///
    /// The [`AsyncTask`] is still handed back, because that is what names
    /// this task as a dependency of the next one -- how a chain is built from
    /// inside a body, and what upstream's `SilentDependentAsync.FromRuntime`
    /// does a thousand times over. [`Runtime::silent_async`] on where a panic
    /// goes holds here, with one addition: the payload is also readable
    /// through [`AsyncTask::take_panic`](crate::AsyncTask::take_panic), the
    /// handle naming the node it was parked on.
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
        self.thread
            .shared
            .schedule_dependent_async_task(
                Spawn::silent(body),
                &Submission::under_node(self.job, &*self.thread),
                dependencies,
            )
            .0
    }

    /// Spawns `body` under this runtime task once every task in
    /// `dependencies` has finished, handing it a [`Runtime`] of its own and
    /// no handle on its result: upstream's `Runtime::silent_dependent_async`
    /// over a `[](tf::Runtime&){}` body.
    ///
    /// [`Runtime::silent_dependent_async`] whose body can spawn and corun,
    /// the fourth corner of the family. Upstream's own `FromRuntime` cases
    /// alternate between this and the plain form along one chain, which pins
    /// that the node kind decides the protocol and the body shape decides
    /// only who is handed what.
    pub fn silent_dependent_async_runtime<'t, F>(
        &mut self,
        body: F,
        dependencies: impl IntoIterator<Item = &'t AsyncTask>,
    ) -> AsyncTask
    where
        F: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static,
    {
        self.thread
            .shared
            .schedule_dependent_async_task(
                Spawn::silent_runtime(body),
                &Submission::under_node(self.job, &*self.thread),
                dependencies,
            )
            .0
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
    /// dependencies and
    /// [`Executor::spawn_runtime`](crate::Executor::spawn_runtime)'s body and
    /// second entry.
    ///
    /// [`AsyncHandle::wait`] answers `Ok(())` once the body and everything it
    /// spawned are done, and an error if any of that panicked: the node is an
    /// explicit anchor for its whole life, so the walk stops there and the
    /// finishing entry hands the slot to the handle.
    /// [`AsyncTask::is_done`](crate::AsyncTask::is_done) is true of the
    /// [`AsyncTask`] only once that second entry has run, which is to say
    /// once the children are done too.
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
        self.shared.schedule_dependent_async_task(
            Spawn::handled_runtime(body),
            &Submission::parentless(),
            dependencies,
        )
    }

    /// Runs `body` on this executor once every task in `dependencies` has
    /// finished, handing it the scheduler, with no handle on its completion.
    ///
    /// Upstream's `silent_dependent_async` over a `[](tf::Runtime&){}` body,
    /// which is one call with one `ESTATE::NONE` there -- its
    /// `_silent_dependent_async` does not branch on the body's shape at all.
    ///
    /// The [`AsyncTask`] is what a caller sees, and nothing else. A panic out
    /// of `body` or out of anything it spawned lands in the node's own slot,
    /// where [`AsyncTask::take_panic`](crate::AsyncTask::take_panic) reads it
    /// back: the node is *not* an explicit anchor here, so the walk stops at
    /// it only because an entered runtime node is an implicit one.
    /// [`Executor::wait_for_all`] is what waits for such a task.
    pub fn silent_dependent_async_runtime<'t, F>(
        &self,
        body: F,
        dependencies: impl IntoIterator<Item = &'t AsyncTask>,
    ) -> AsyncTask
    where
        F: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static,
    {
        self.shared
            .schedule_dependent_async_task(
                Spawn::silent_runtime(body),
                &Submission::parentless(),
                dependencies,
            )
            .0
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
    use crate::executor::topology::{GraphOwner, Job, Parent, Topology, set_up_graph};
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

    /// Takes one child's job back off the submitting worker's own queue, for
    /// a case that runs it by hand.
    ///
    /// **Where it looks is half the assertion.** A child submitted from a
    /// runtime body goes onto the queue of the worker running that body
    /// (#119), which is upstream's `if(auto w = this_worker(); w)` branch, so
    /// a submission that went to the shared buffers instead fails every case
    /// that calls this -- and the queue is the one `test_worker` built, which
    /// nothing else in the process holds an end of.
    ///
    /// # Why the pool cannot race this
    ///
    /// It never could: the queue above is not a real worker's, so no thief
    /// can reach it. The buffers these cases used to go through needed an
    /// argument for the same claim, and it still holds for whatever a case
    /// leaves there -- a worker skips the steal loop entirely while
    /// `num_topologies` is zero (`WorkerThread::explore_task`'s fast path),
    /// a hand-driven run leaves it at zero because nothing here goes through
    /// `Executor::run`, and a parented child does not raise it either, which
    /// is #115's rule.
    ///
    /// **The flip side, for whoever adds a case here:** that same fast path
    /// is why a hand-driven case must not call [`Runtime::corun`] with a
    /// child outstanding. The corun loop would find nothing to steal, yield,
    /// and go round again for ever -- a hang rather than a failure. Drive the
    /// children with this instead, and leave the corun cases to
    /// `tests/runtimes.rs`, where a real run holds the count above zero.
    fn take_child_job(worker: &WorkerThread) -> Job {
        worker
            .pop()
            .expect("the submission pushed a job onto this worker's queue")
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
        // The two children, taken off this worker's own queue -- which is
        // where #119 puts them -- before either runs, so that every `pop`
        // below is about a resumption and nothing else.
        let first_child = take_child_job(&worker);
        let second_child = take_child_job(&worker);
        assert!(
            worker.pop().is_none(),
            "its two children and nothing else: a preempted node releases no              successor of its own"
        );

        // The first child: counted off, nothing resumed.
        worker.invoke(first_child);
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
        worker.invoke(second_child);
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
        let child = take_child_job(&worker);
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
        // one job on this worker's queue is the first.
        let first = take_child_job(&worker);
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

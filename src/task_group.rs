//! Translated from `taskflow/core/task_group.hpp` (Taskflow v4.1.0).
//!
//! A task group is a scope a worker opens to spawn asynchronous work into and
//! then wait for. There is no graph, no taskflow and no runtime task around
//! it: a worker asks the executor for one, submits as many async tasks as it
//! likes, and coruns until they are done -- which is what makes recursive
//! parallelism a plain recursive function rather than a graph somebody has to
//! build. Only a worker can open one, because the only thing a group can do
//! to *wait* is corun; [`Executor::task_group`] is the one constructor and
//! [`CorunError::NotAWorker`] is its refusal.
//!
//! # Design
//!
//! The eight spawning forms below are one delegation each onto the submission
//! core in `executor::async`, under a `Submission` built from the group's own
//! anchor. `DESIGN.md` has the anchor's `Arc` shape and the `Drop` this type
//! does not have, where a child is queued and the stack-depth ceiling that
//! follows from it, which anchor a child's failure reaches depending on
//! which of the eight forms raised it, and the naming of the eight forms
//! over upstream's four templates -- all under "Rationale moved out", which
//! also has the one limit this port adds under cancellation: a cancelled
//! group whose outstanding children are dependent-async tasks should be
//! corun to completion rather than cancelled mid-chain if any of them still
//! has a successor to release.

use std::any::Any;
use std::fmt;
use std::marker::PhantomData;
use std::panic;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::async_task::AsyncTask;
use crate::executor::Executor;
use crate::executor::r#async::{AsyncHandle, Spawn, Submission};
use crate::executor::corun::CorunError;
use crate::executor::run_handle::RunError;
use crate::executor::topology::{Cancellable, Nested};
use crate::executor::worker::WorkerThread;
use crate::graph::Node;
use crate::runtime::Runtime;

impl Executor {
    /// Opens a task group on the calling worker: a scope to spawn async work
    /// into and wait for with [`TaskGroup::corun`].
    ///
    /// Upstream's `Executor::task_group`. The group borrows this executor for
    /// as long as it lives, submits its children to it, and is **not**
    /// counted in [`Executor::wait_for_all`]'s work.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::Executor;
    ///
    /// let executor = Arc::new(Executor::new(4));
    /// let counter = Arc::new(AtomicUsize::new(0));
    ///
    /// executor
    ///     .spawn({
    ///         let executor = Arc::clone(&executor);
    ///         let counter = Arc::clone(&counter);
    ///         move || {
    ///             let group = executor.task_group().expect("a worker may open a group");
    ///             assert_eq!(group.size(), 0);
    ///
    ///             for _ in 0..100 {
    ///                 let counter = Arc::clone(&counter);
    ///                 group.silent_async(move || {
    ///                     counter.fetch_add(1, Ordering::Relaxed);
    ///                 });
    ///             }
    ///
    ///             // The one thing that waits for a group's children.
    ///             group.corun();
    ///             assert_eq!(group.size(), 0);
    ///         }
    ///     })
    ///     .wait()
    ///     .expect("no task panicked");
    ///
    /// assert_eq!(counter.load(Ordering::Relaxed), 100);
    ///
    /// // From a thread that is not a worker of this executor there is no
    /// // group to be had, because there would be nothing to corun on.
    /// assert!(executor.task_group().is_err());
    /// ```
    ///
    /// # Errors
    ///
    /// [`CorunError::NotAWorker`] if the calling thread is not a worker of
    /// this executor, which is upstream's *"task_group can only created by a
    /// worker of the executor"*. It is the only error this call has, and it
    /// is the same one [`Executor::corun`] returns: a group whose `corun`
    /// could not run would be a group that cannot wait.
    pub fn task_group(&self) -> Result<TaskGroup<'_>, CorunError> {
        // Upstream's `Worker* w = this_worker(); if(w == nullptr) TF_THROW`,
        // and the id is its `Worker&`: what `corun` needs is the worker whose
        // scheduling loop to re-enter, which `WorkerThread::corun_view`
        // builds from the id.
        let worker = self.this_worker_id().ok_or(CorunError::NotAWorker)?;
        Ok(TaskGroup::new(self, worker))
    }
}

/// A scope one worker spawns asynchronous tasks into and waits for:
/// upstream's `tf::TaskGroup`.
///
/// Opened with [`Executor::task_group`], which is the only way to get one and
/// refuses any thread that is not a worker. The eight spawning forms put a
/// task under the group's anchor; [`TaskGroup::corun`] waits for all of them
/// on the opening worker without taking that worker out of the pool;
/// [`TaskGroup::cancel`] stops the ones that have not started.
///
/// **Dropping a group does not join it.** Children outstanding at that point
/// keep running, with nothing waiting for them and a failure of one of them
/// reported to nobody -- upstream's behaviour, minus the undefined behaviour,
/// for the reason the module docs give. A caller who wants the join calls
/// [`TaskGroup::corun`] before the group goes out of scope, which is also the
/// only place a child's panic can be reported.
///
/// Neither [`Send`] nor [`Sync`], which is upstream's deleted copy and move
/// constructors: a group belongs to the worker that opened it.
pub struct TaskGroup<'a> {
    /// The executor that opened this group, upstream's `Executor& _executor`:
    /// where every child is submitted and whose pool the corun runs on.
    executor: &'a Executor,
    /// The id of the worker that opened the group, upstream's `Worker&
    /// _worker`: the worker whose scheduling loop [`TaskGroup::corun`]
    /// re-enters.
    worker: usize,
    /// What the children count down and what a failure under them lands on:
    /// upstream's `NodeBase _node_base`, shared rather than owned by this
    /// value because every job of every child holds a share.
    anchor: Arc<Nested>,
    /// What makes the group neither [`Send`] nor [`Sync`]: upstream's deleted
    /// move and copy constructors, in the one vocabulary Rust has for it.
    ///
    /// A raw pointer rather than a thread-id check at run time, because the
    /// mistake it prevents -- coruning on a worker that is not the calling
    /// thread -- is one the compiler can simply refuse.
    _not_send: PhantomData<*const ()>,
}

impl<'a> TaskGroup<'a> {
    /// Opens a group on `worker` of `executor`: upstream's private
    /// constructor, with `friend class Executor` spelled as the module
    /// boundary.
    fn new(executor: &'a Executor, worker: usize) -> Self {
        let anchor = Nested::new(None);

        // Upstream's `NSTATE::IMPLICITLY_ANCHORED` constructor argument, and
        // not an afterthought: a child can fail before this call's caller has
        // done anything with the group, and the bit is what decides whether
        // that payload has somewhere to go. In `nstate` rather than `estate`
        // because that is the word upstream puts it in and the word
        // `Job::record_panic` reads it from.
        anchor
            .base()
            .nstate()
            .fetch_or(Node::IMPLICITLY_ANCHORED, Ordering::Relaxed);

        Self {
            executor,
            worker,
            anchor,
            _not_send: PhantomData,
        }
    }

    /// The executor that opened this group: upstream's
    /// `TaskGroup::executor`. `DESIGN.md` has why a `Runtime` has no
    /// equivalent accessor.
    #[must_use]
    pub fn executor(&self) -> &'a Executor {
        self.executor
    }

    /// Runs `body` as a task of this group, and hands back a handle on its
    /// result.
    ///
    /// Upstream's `TaskGroup::async`, under [`Executor::spawn`]'s name: same
    /// submission, parented to the anchor so [`TaskGroup::corun`] waits for
    /// it and [`TaskGroup::cancel`] can stop it unstarted. The
    /// [`AsyncHandle`] behaves as [`Executor::spawn`]'s does, a panic out of
    /// `body` being **this handle's** failure rather than the group's.
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use rustflow::Executor;
    ///
    /// let executor = Arc::new(Executor::new(2));
    ///
    /// let sum = executor
    ///     .spawn({
    ///         let executor = Arc::clone(&executor);
    ///         move || {
    ///             let group = executor.task_group().expect("a worker may open a group");
    ///             let six = group.spawn(|| 6);
    ///             let seven = group.spawn(|| 7);
    ///
    ///             // The corun is what makes the two `wait`s below free:
    ///             // both tasks have finished by the time it returns.
    ///             group.corun();
    ///
    ///             six.wait().expect("no panic") + seven.wait().expect("no panic")
    ///         }
    ///     })
    ///     .wait()
    ///     .expect("no task panicked");
    ///
    /// assert_eq!(sum, 13);
    /// ```
    pub fn spawn<R, F>(&self, body: F) -> AsyncHandle<R>
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        self.executor.shared.schedule_async_task(
            Spawn::handled(body),
            &Submission::under_anchor(&self.anchor),
        )
    }

    /// Runs `body` as a task of this group, with no handle on it at all.
    ///
    /// Upstream's `TaskGroup::silent_async`, and the cheaper half of the
    /// pair: no slot to deliver into, so a submission costs only the node
    /// and the body -- the form a recursive algorithm wants, with the group
    /// rather than a handle waiting for the child.
    ///
    /// A panic out of `body` lands on the **group's anchor**, where
    /// [`TaskGroup::corun`] re-raises it and [`TaskGroup::try_corun`] reports
    /// it, and it stops the children that had not started -- the one way a
    /// group is louder than [`Executor::silent_async`].
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::Executor;
    ///
    /// let executor = Arc::new(Executor::new(4));
    /// let counter = Arc::new(AtomicUsize::new(0));
    ///
    /// executor
    ///     .spawn({
    ///         let executor = Arc::clone(&executor);
    ///         let counter = Arc::clone(&counter);
    ///         move || {
    ///             let group = executor.task_group().expect("a worker may open a group");
    ///             for _ in 0..1000 {
    ///                 let counter = Arc::clone(&counter);
    ///                 group.silent_async(move || {
    ///                     counter.fetch_add(1, Ordering::Relaxed);
    ///                 });
    ///             }
    ///             group.corun();
    ///             assert_eq!(counter.load(Ordering::Relaxed), 1000);
    ///
    ///             // A group is reusable: spawn another round and wait again.
    ///             for _ in 0..1000 {
    ///                 let counter = Arc::clone(&counter);
    ///                 group.silent_async(move || {
    ///                     counter.fetch_add(1, Ordering::Relaxed);
    ///                 });
    ///             }
    ///             group.corun();
    ///         }
    ///     })
    ///     .wait()
    ///     .expect("no task panicked");
    ///
    /// assert_eq!(counter.load(Ordering::Relaxed), 2000);
    /// ```
    pub fn silent_async<F>(&self, body: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.executor
            .shared
            .schedule_async_task(Spawn::silent(body), &Submission::under_anchor(&self.anchor));
    }

    /// Runs `body` as a task of this group, handing it the scheduler, and
    /// hands back a handle on its completion.
    ///
    /// The runtime-shaped half of [`TaskGroup::spawn`]: upstream's
    /// `TaskGroup::async` over a `[](tf::Runtime&){}` body. Everything
    /// [`Executor::spawn_runtime`] says about the body and about the handle
    /// carrying no value holds here, and what the group adds is the parent.
    /// The child's node is **explicitly anchored**, upstream's estate
    /// argument for this form, so a failure under the child -- its own body,
    /// or anything it spawned -- is reported through the handle rather than to
    /// the group.
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use rustflow::{Executor, Runtime};
    ///
    /// let executor = Arc::new(Executor::new(2));
    ///
    /// executor
    ///     .spawn({
    ///         let executor = Arc::clone(&executor);
    ///         move || {
    ///             let group = executor.task_group().expect("a worker may open a group");
    ///             let task = group.spawn_runtime(|runtime: &mut Runtime<'_>| {
    ///                 assert_eq!(runtime.num_workers(), 2);
    ///             });
    ///             group.corun();
    ///             task.wait().expect("the body did not panic");
    ///         }
    ///     })
    ///     .wait()
    ///     .expect("no task panicked");
    /// ```
    pub fn spawn_runtime<F>(&self, body: F) -> AsyncHandle<()>
    where
        F: for<'r> FnOnce(&mut Runtime<'r>) + Send + 'static,
    {
        self.executor.shared.schedule_async_task(
            Spawn::handled_runtime(body),
            &Submission::under_anchor(&self.anchor),
        )
    }

    /// Runs `body` as a task of this group, handing it the scheduler, with no
    /// handle on it at all.
    ///
    /// The runtime-shaped half of [`TaskGroup::silent_async`]: upstream's
    /// `TaskGroup::silent_async` over a `[](tf::Runtime&){}` body. There is
    /// no second entry, as [`Executor::silent_async_runtime`] explains, and a
    /// panic out of `body` or out of anything it spawned lands on the
    /// **group's anchor** -- this is the runtime-shaped form a `corun`
    /// reports for.
    pub fn silent_async_runtime<F>(&self, body: F)
    where
        F: for<'r> FnOnce(&mut Runtime<'r>) + Send + 'static,
    {
        self.executor.shared.schedule_async_task(
            Spawn::silent_runtime(body),
            &Submission::under_anchor(&self.anchor),
        );
    }

    /// Runs `body` as a task of this group once every task in `dependencies`
    /// has finished, and hands back a handle on the task and a handle on its
    /// result.
    ///
    /// Upstream's `TaskGroup::dependent_async`. Everything
    /// [`Executor::dependent_async`] says about naming dependencies holds,
    /// and what the group adds is the parent, so `corun` covers the whole
    /// chain. A panic out of `body` goes into the [`AsyncHandle`] as
    /// [`TaskGroup::spawn`]'s does.
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use rustflow::Executor;
    ///
    /// let executor = Arc::new(Executor::new(4));
    ///
    /// executor
    ///     .spawn({
    ///         let executor = Arc::clone(&executor);
    ///         move || {
    ///             let group = executor.task_group().expect("a worker may open a group");
    ///
    ///             let first = group.silent_dependent_async(|| {}, []);
    ///             let (_second, answer) = group.dependent_async(|| 42, [&first]);
    ///
    ///             // Waits for both links, whichever workers took them.
    ///             group.corun();
    ///             assert_eq!(answer.wait().expect("no panic"), 42);
    ///         }
    ///     })
    ///     .wait()
    ///     .expect("no task panicked");
    /// ```
    pub fn dependent_async<'t, R, F>(
        &self,
        body: F,
        dependencies: impl IntoIterator<Item = &'t AsyncTask>,
    ) -> (AsyncTask, AsyncHandle<R>)
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        self.executor.shared.schedule_dependent_async_task(
            Spawn::handled(body),
            &Submission::under_anchor(&self.anchor),
            dependencies,
        )
    }

    /// Runs `body` as a task of this group once every task in `dependencies`
    /// has finished, with no handle on its result.
    ///
    /// Upstream's `TaskGroup::silent_dependent_async`, the form a chain is
    /// built out of: the [`AsyncTask`] it returns names this task as the
    /// next one's dependency. A panic out of `body` is kept on the task for
    /// [`AsyncTask::take_panic`], **and** reaches the group's anchor.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::Executor;
    ///
    /// let executor = Arc::new(Executor::new(4));
    /// let order = Arc::new(AtomicUsize::new(0));
    ///
    /// executor
    ///     .spawn({
    ///         let executor = Arc::clone(&executor);
    ///         let order = Arc::clone(&order);
    ///         move || {
    ///             let group = executor.task_group().expect("a worker may open a group");
    ///
    ///             // A chain of a hundred, each submitted while the one
    ///             // before it may already be running.
    ///             let mut previous = None;
    ///             for step in 0..100 {
    ///                 let order = Arc::clone(&order);
    ///                 let task = group.silent_dependent_async(
    ///                     move || assert_eq!(order.fetch_add(1, Ordering::Relaxed), step),
    ///                     previous.as_ref(),
    ///                 );
    ///                 previous = Some(task);
    ///             }
    ///
    ///             group.corun();
    ///         }
    ///     })
    ///     .wait()
    ///     .expect("no task panicked");
    ///
    /// assert_eq!(order.load(Ordering::Relaxed), 100);
    /// ```
    pub fn silent_dependent_async<'t, F>(
        &self,
        body: F,
        dependencies: impl IntoIterator<Item = &'t AsyncTask>,
    ) -> AsyncTask
    where
        F: FnOnce() + Send + 'static,
    {
        self.executor
            .shared
            .schedule_dependent_async_task(
                Spawn::silent(body),
                &Submission::under_anchor(&self.anchor),
                dependencies,
            )
            .0
    }

    /// Runs `body` as a task of this group once every task in `dependencies`
    /// has finished, handing it the scheduler, and hands back a handle on the
    /// task and one on its completion.
    ///
    /// The runtime-shaped half of [`TaskGroup::dependent_async`]: upstream's
    /// `is_runtime_task_v<F>` branch of `_dependent_async`, which it reaches
    /// through the same name. The node is explicitly anchored for
    /// [`TaskGroup::spawn_runtime`]'s reason, so what failed under the child
    /// is the handle's to report.
    pub fn dependent_async_runtime<'t, F>(
        &self,
        body: F,
        dependencies: impl IntoIterator<Item = &'t AsyncTask>,
    ) -> (AsyncTask, AsyncHandle<()>)
    where
        F: for<'r> FnOnce(&mut Runtime<'r>) + Send + 'static,
    {
        self.executor.shared.schedule_dependent_async_task(
            Spawn::handled_runtime(body),
            &Submission::under_anchor(&self.anchor),
            dependencies,
        )
    }

    /// Runs `body` as a task of this group once every task in `dependencies`
    /// has finished, handing it the scheduler, with no handle on its result.
    ///
    /// The runtime-shaped half of [`TaskGroup::silent_dependent_async`], and
    /// the last of the eight. A panic out of `body` or out of anything it
    /// spawned reaches the group's anchor, as the other silent forms' does.
    pub fn silent_dependent_async_runtime<'t, F>(
        &self,
        body: F,
        dependencies: impl IntoIterator<Item = &'t AsyncTask>,
    ) -> AsyncTask
    where
        F: for<'r> FnOnce(&mut Runtime<'r>) + Send + 'static,
    {
        self.executor
            .shared
            .schedule_dependent_async_task(
                Spawn::silent_runtime(body),
                &Submission::under_anchor(&self.anchor),
                dependencies,
            )
            .0
    }

    /// Waits for every task of this group on the opening worker, without
    /// taking that worker out of the pool.
    ///
    /// Upstream's `TaskGroup::corun`: the worker stays in the work-stealing
    /// loop, running this group's children and anybody else's it can steal,
    /// until the anchor's join counter is back to zero -- nothing parks, for
    /// [`Executor::corun`]'s reason. May be called any number of times, each
    /// waiting for whatever is outstanding when it starts.
    ///
    /// # Panics
    ///
    /// Panics with the payload of the first child of this group to fail with
    /// no handle of its own, through [`resume_unwind`](panic::resume_unwind)
    /// so it keeps its identity. Taking the payload clears the anchor, so a
    /// group whose failure has been reported is usable again.
    ///
    /// [`TaskGroup::try_corun`] is the same wait with that failure as an
    /// `Err` instead.
    pub fn corun(&self) {
        if let Some(payload) = self.corun_payload() {
            // Upstream's `_node_base._rethrow_exception()`, and the last line
            // of the call for `Executor::corun`'s reason: the unwind starts
            // here, with nothing of the wait left on the frame.
            panic::resume_unwind(payload);
        }
    }

    /// Waits for every task of this group, reporting a failed child as an
    /// error rather than raising it: [`TaskGroup::corun`] with the one
    /// difference the name says.
    ///
    /// # Errors
    ///
    /// [`RunError::Panicked`] carrying the payload of the first child of this
    /// group to fail with no handle of its own. There is no `NotAWorker`
    /// error to return: holding a [`TaskGroup`] is proof the calling thread
    /// is the worker that opened it.
    pub fn try_corun(&self) -> Result<(), RunError> {
        match self.corun_payload() {
            Some(payload) => Err(RunError::Panicked(payload)),
            None => Ok(()),
        }
    }

    /// Both shapes of corun, up to what is done with the payload: the
    /// explicit-anchor guard, the wait on the anchor's counter, then the
    /// payload, in that order -- upstream's `corun` body. The guard releases
    /// before the payload is taken, so the bit is already clear when the
    /// caller decides what to do with it.
    fn corun_payload(&self) -> Option<Box<dyn Any + Send>> {
        {
            let _anchor = self.anchor.base().anchor_explicitly();
            let mut worker =
                WorkerThread::corun_view(Arc::clone(&self.executor.shared), self.worker);
            worker.corun_until(|| self.anchor.outstanding() == 0);
        }

        self.anchor.base().take_panic()
    }

    /// Stops the tasks of this group that have not started.
    ///
    /// Upstream's `TaskGroup::cancel`, one `fetch_or` of `ESTATE::CANCELLED`
    /// on the anchor: a child not yet picked up does not run, and one
    /// already running runs to completion -- cooperative, as everywhere else
    /// in the crate. It is **not** a wait, so a caller still coruns
    /// afterwards to synchronise with the children already in flight, and
    /// the bit is never cleared: a cancelled group stays cancelled.
    pub fn cancel(&self) {
        self.anchor
            .base()
            .estate()
            .fetch_or(Cancellable::CANCELLED, Ordering::Relaxed);
    }

    /// Whether [`TaskGroup::cancel`] has been called on this group.
    ///
    /// Upstream's `TaskGroup::is_cancelled`, reading only the `CANCELLED` bit
    /// of upstream's mask, not the `EXCEPTION` one: a group whose silent
    /// child panicked answers `false` here, that being [`TaskGroup::corun`]'s
    /// failure to report rather than a cancellation. It says nothing about
    /// whether anything has finished.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.anchor.base().estate().load(Ordering::Relaxed) & Cancellable::CANCELLED != 0
    }

    /// How many tasks of this group are still outstanding.
    ///
    /// Upstream's `TaskGroup::size`: a snapshot that may already be stale by
    /// the time it is read, for diagnostics rather than synchronisation --
    /// [`TaskGroup::corun`] is that. Zero before anything is spawned and zero
    /// again once a `corun` returns.
    #[must_use]
    pub fn size(&self) -> usize {
        self.anchor.outstanding()
    }
}

impl fmt::Debug for TaskGroup<'_> {
    /// Hand-written, because what is worth printing about a group is what it
    /// is waiting for rather than the executor it borrows.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskGroup")
            .field("worker", &self.worker)
            .field("size", &self.size())
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;

    /// How long a case waits for the other side of a handshake before it
    /// gives up.
    ///
    /// The deadline every module in the crate keeps, for the reason they
    /// give: nothing here passes because of this number, and the only thing
    /// that reaches it is a side that is never going to arrive. What it buys
    /// in *this* module is that a child which spins for a flag fails the case
    /// instead of wedging a worker for ever.
    const GIVE_UP_AFTER: Duration = Duration::from_secs(120);

    /// The payload a failing child panics with, so that a case can assert it
    /// got back *that* payload rather than merely some panic.
    ///
    /// A type of its own rather than a string, because identity is the claim:
    /// `corun` hands the payload to `resume_unwind`, so what a caller catches
    /// must downcast to exactly what the child panicked with.
    #[derive(Debug, PartialEq, Eq)]
    struct Marker;

    /// Spins until `flag` is set, failing rather than hanging if it never is.
    ///
    /// A spin with `yield_now` rather than a condvar for the crate's usual
    /// reason -- one end of every use of it is a task body, and a worker that
    /// parked would be a worker not running the rest of the work -- and with a
    /// deadline because the other end of these handshakes is a *child* of a
    /// task group: a release that never arrives would otherwise leave a
    /// worker spinning for ever instead of failing the case.
    fn spin_until(what: &str, flag: &AtomicBool) {
        spin_while(what, || !flag.load(Ordering::Acquire));
    }

    /// Spins while `again` says to, failing rather than hanging if it never
    /// stops saying it.
    ///
    /// [`spin_until`] over a predicate, for the one handshake in this module
    /// that is not a flag: "the group has nothing outstanding", which a case
    /// needs in order to reach the panic walk's implicit branch -- the child
    /// has to be *over* before any corun starts.
    fn spin_while(what: &str, mut again: impl FnMut() -> bool) {
        let deadline = Instant::now() + GIVE_UP_AFTER;
        while again() {
            assert!(Instant::now() < deadline, "gave up waiting for {what}");
            thread::yield_now();
        }
    }

    /// The acceptance case this issue names: a group can only be opened by a
    /// worker, because the only thing it can do to wait is corun.
    ///
    /// Upstream throws here; the port reports the same error
    /// [`Executor::corun`] reports for the same mistake, and the executor is
    /// left untouched by the refusal.
    #[test]
    fn a_group_cannot_be_opened_off_a_worker() {
        let executor = Arc::new(Executor::new(2));

        assert!(matches!(executor.task_group(), Err(CorunError::NotAWorker)));

        // The refusal is the whole of it: nothing was scheduled and the
        // executor is still idle.
        assert_eq!(executor.num_topologies(), 0);
        executor.wait_for_all();

        // A worker of *another* executor is not a worker of this one either,
        // which is what the per-executor thread-id map buys over a
        // thread-local.
        let other = Executor::new(1);
        let refused = Arc::new(AtomicBool::new(false));
        other
            .spawn({
                let executor = Arc::clone(&executor);
                let refused = Arc::clone(&refused);
                move || {
                    refused.store(
                        matches!(executor.task_group(), Err(CorunError::NotAWorker)),
                        Ordering::Release,
                    );
                }
            })
            .wait()
            .expect("no task panicked");
        assert!(refused.load(Ordering::Acquire));
    }

    /// The acceptance case this issue names: a silent child's panic comes
    /// back out of `corun` as a `resume_unwind` of the same payload.
    ///
    /// A regression surfaces as an assertion rather than a hang either way:
    /// a payload that never reached the anchor makes `catch_unwind` answer
    /// `Ok`, and one that reached it as something else fails the downcast.
    /// What the second half of the case pins is upstream's
    /// `_rethrow_exception` clearing the slot -- a group whose failure has
    /// been reported is usable again, which is also what keeps the anchor
    /// from skipping every child spawned after the failure.
    #[test]
    fn a_silent_childs_panic_is_raised_by_corun() {
        let executor = Arc::new(Executor::new(2));
        let ran = Arc::new(AtomicUsize::new(0));

        executor
            .spawn({
                let executor = Arc::clone(&executor);
                let ran = Arc::clone(&ran);
                move || {
                    let group = executor.task_group().expect("a worker may open a group");
                    group.silent_async(|| panic::panic_any(Marker));

                    let payload = panic::catch_unwind(AssertUnwindSafe(|| group.corun()))
                        .expect_err("the child panicked, so the corun raised it");
                    assert_eq!(
                        payload.downcast_ref::<Marker>(),
                        Some(&Marker),
                        "the payload the child panicked with, by identity"
                    );

                    // The slot was cleared by the report, so the group takes
                    // another child and the next corun is quiet.
                    group.silent_async({
                        let ran = Arc::clone(&ran);
                        move || {
                            ran.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                    group.corun();
                }
            })
            .wait()
            .expect("the body caught the child's panic itself");

        assert_eq!(ran.load(Ordering::Relaxed), 1);
    }

    /// The acceptance case this issue names: the same failure out of
    /// `try_corun` is an `Err` carrying the payload, not an unwind.
    ///
    /// The pair with the case above is the point -- one implementation, two
    /// shapes -- so a regression that reported the failure through the wrong
    /// one fails exactly one of the two.
    #[test]
    fn a_silent_childs_panic_is_reported_by_try_corun() {
        let executor = Arc::new(Executor::new(2));

        executor
            .spawn({
                let executor = Arc::clone(&executor);
                move || {
                    let group = executor.task_group().expect("a worker may open a group");
                    group.silent_async(|| panic::panic_any(Marker));

                    let error = group.try_corun().expect_err("the child panicked");
                    // One variant, so the binding is irrefutable: what is
                    // being read is the payload, not which failure it was.
                    let RunError::Panicked(payload) = error;
                    assert_eq!(payload.downcast_ref::<Marker>(), Some(&Marker));

                    // And nothing was left behind: the group is empty and
                    // quiet.
                    assert_eq!(group.size(), 0);
                    group.corun();
                }
            })
            .wait()
            .expect("the body took the failure as a value");
    }

    /// A handle-returning child's panic is the handle's and not the group's,
    /// which is upstream's split between its `async` and `silent_async`
    /// forms and the one asymmetry a caller has to know about.
    ///
    /// It is here rather than in `tests/task_group.rs` because what it pins
    /// is which *slot* the payload went into, and the pair of waits is the
    /// only way to say that: a regression that routed a `spawn`ed body's
    /// panic to the anchor would make the corun below raise instead of
    /// returning, failing the case rather than hanging.
    #[test]
    fn a_spawned_childs_panic_goes_to_its_handle_and_not_to_the_group() {
        let executor = Arc::new(Executor::new(2));

        executor
            .spawn({
                let executor = Arc::clone(&executor);
                move || {
                    let group = executor.task_group().expect("a worker may open a group");
                    let failed = group.spawn(|| -> i32 { panic::panic_any(Marker) });

                    // The group is quiet about it, because the wrapper the
                    // submission built caught the payload on its way to the
                    // handle.
                    group.try_corun().expect("the group saw no failure");

                    let error = failed.wait().expect_err("the body panicked");
                    let RunError::Panicked(payload) = error;
                    assert_eq!(payload.downcast_ref::<Marker>(), Some(&Marker));
                }
            })
            .wait()
            .expect("no task panicked");
    }

    /// The acceptance case this issue names: a group dropped with a child
    /// still running leaves the executor usable, and the child runs.
    ///
    /// The Drop decision in one case. The child is already *running* when the
    /// group goes out of scope -- the handshake is what makes that true
    /// rather than likely -- so what is being pinned is that nothing waits
    /// for it, nothing refuses it, and the anchor it is still counting down
    /// outlives the group that owned it. A regression that dangled the anchor
    /// or that cancelled the child on drop fails on the flags; the child's
    /// own spin has a deadline, so a release that never arrived would fail
    /// the case rather than wedge the worker.
    #[test]
    fn a_group_dropped_with_a_child_running_leaves_the_executor_usable() {
        let executor = Arc::new(Executor::new(2));
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));

        executor
            .spawn({
                let executor = Arc::clone(&executor);
                let started = Arc::clone(&started);
                let release = Arc::clone(&release);
                let finished = Arc::clone(&finished);
                move || {
                    let group = executor.task_group().expect("a worker may open a group");
                    group.silent_async({
                        let started = Arc::clone(&started);
                        move || {
                            started.store(true, Ordering::Release);
                            spin_until("the test thread to release the child", &release);
                            finished.store(true, Ordering::Release);
                        }
                    });

                    // Dropped at the end of this scope with the child still
                    // in its body, which is the whole of the case.
                    spin_until("the child to start", &started);
                }
            })
            .wait()
            .expect("no task panicked");

        release.store(true, Ordering::Release);
        spin_until("the orphaned child to finish", &finished);

        // And the executor is unharmed by it: it still runs work, and still
        // joins its pool on the way out of this case.
        assert_eq!(executor.spawn(|| 7).wait().expect("no panic"), 7);
    }

    /// A group's children are accounted for by the anchor and by nothing
    /// else, which is upstream's split and issue #115's fix.
    ///
    /// White-box because the claim is about counters no public call reports
    /// together: the anchor's own counter rises per child and comes back to
    /// zero, while the executor's `num_topologies` -- the count
    /// `wait_for_all` and `Executor::drop` block on -- never sees a child at
    /// all. A regression here is the one shape that *would* hang, in `Drop`
    /// rather than in a test body, so it is worth an assertion that fails
    /// first.
    #[test]
    fn a_groups_children_are_counted_by_the_anchor_alone() {
        let executor = Arc::new(Executor::new(2));
        let counted = Arc::new(AtomicUsize::new(usize::MAX));

        executor
            .spawn({
                let executor = Arc::clone(&executor);
                let counted = Arc::clone(&counted);
                move || {
                    let group = executor.task_group().expect("a worker may open a group");
                    let blocked = Arc::new(AtomicBool::new(false));

                    // Spawned but held, so that the counter can be read with
                    // the children provably outstanding.
                    for _ in 0..8 {
                        let blocked = Arc::clone(&blocked);
                        group.silent_async(move || {
                            spin_until("the group to release its children", &blocked);
                        });
                    }
                    counted.store(group.size(), Ordering::Relaxed);

                    blocked.store(true, Ordering::Release);
                    group.corun();
                    assert_eq!(group.size(), 0);

                    // One topology: the `spawn` this body is, which is the
                    // only thing above the group that the executor counts.
                    assert_eq!(executor.num_topologies(), 1);
                }
            })
            .wait()
            .expect("no task panicked");

        assert_eq!(counted.load(Ordering::Relaxed), 8);

        // The counter `Executor::drop` blocks on is balanced, which is what
        // this case is really about: `wait_for_all` returning is the same
        // claim as the drop below not hanging.
        executor.wait_for_all();

        // Read here and not straight after the `wait` above, because the
        // `wait` does not order the test thread against the tear-down that
        // spends this counter: the body's outcome is delivered into the
        // handle's slot *inside* the wrapper `AsyncHandle::for_body` builds,
        // so it is part of the body, and the dispatch arm calls
        // `Shared::finish_async` -- which decrements `num_topologies` and
        // `num_asyncs` -- only once that body returns. The window between
        // the two is by construction, so an early read of 1 is correct
        // behaviour rather than a flake (#136); `wait_for_all` is the call
        // that actually guarantees the zero.
        assert_eq!(executor.num_topologies(), 0);
    }

    /// The anchor is an implicit anchor for its whole life, so a child that
    /// fails while nobody is coruning still has somewhere to put its payload.
    ///
    /// Upstream's `NSTATE::IMPLICITLY_ANCHORED` constructor argument, and the
    /// one thing the group's anchor has that a corun's stack anchor does not.
    /// The case drives the ordering by hand -- the child is made to finish
    /// *before* any corun starts -- because that is the only way to reach the
    /// implicit branch of the panic walk from the public API: a regression
    /// that relied on the explicit guard alone would drop the payload, and
    /// the corun below would return quietly instead of raising.
    #[test]
    fn a_child_that_fails_before_any_corun_is_reported_by_the_next_one() {
        let executor = Arc::new(Executor::new(2));

        executor
            .spawn({
                let executor = Arc::clone(&executor);
                move || {
                    let group = executor.task_group().expect("a worker may open a group");
                    let failed = Arc::new(AtomicBool::new(false));

                    group.silent_async({
                        let failed = Arc::clone(&failed);
                        move || {
                            // Set before the panic, so the handshake below
                            // cannot be satisfied by a child that has not
                            // reached its body yet.
                            failed.store(true, Ordering::Release);
                            panic::panic_any(Marker);
                        }
                    });

                    // No guard is held while this spins, so the payload has
                    // only the implicit anchor to land on.
                    spin_until("the child to fail", &failed);
                    spin_while("the failed child to be torn down", || group.size() != 0);

                    let error = group.try_corun().expect_err("the child panicked");
                    // One variant, so the binding is irrefutable: what is
                    // being read is the payload, not which failure it was.
                    let RunError::Panicked(payload) = error;
                    assert_eq!(payload.downcast_ref::<Marker>(), Some(&Marker));
                }
            })
            .wait()
            .expect("no task panicked");
    }

    /// The four runtime-shaped forms put a child under the group just as the
    /// four plain ones do, and one corun waits for all of them.
    ///
    /// Not upstream's case, because upstream has no such forms: its four
    /// spawning templates dispatch on the shape of the callable, so this is
    /// the half of each of them that `if constexpr(is_runtime_task_v<F>)`
    /// selects. It is here because a group is the first thing in the crate to
    /// submit a *reentrant* node under a parent -- the handle-returning two
    /// are explicitly anchored and finish on a second entry -- so what the
    /// case pins is that the second entry still delivers when the first entry
    /// counted a parent down: the two handles answer `Ok`, the silent two
    /// reach their bodies, and the anchor comes back to zero.
    #[test]
    fn the_runtime_shaped_forms_put_their_children_under_the_group() {
        let executor = Arc::new(Executor::new(2));
        let ran = Arc::new(AtomicUsize::new(0));

        executor
            .spawn({
                let executor = Arc::clone(&executor);
                let ran = Arc::clone(&ran);
                move || {
                    let group = executor.task_group().expect("a worker may open a group");

                    // A body that uses the scheduler it was handed and then
                    // says it ran. The corun inside returns at once, because
                    // the body spawned nothing of its own.
                    let body = |ran: &Arc<AtomicUsize>| {
                        let ran = Arc::clone(ran);
                        move |runtime: &mut Runtime<'_>| {
                            runtime.corun();
                            ran.fetch_add(1, Ordering::Relaxed);
                        }
                    };

                    let spawned = group.spawn_runtime(body(&ran));
                    group.silent_async_runtime(body(&ran));
                    let (first, dependent) = group.dependent_async_runtime(body(&ran), []);
                    let last = group.silent_dependent_async_runtime(body(&ran), [&first]);

                    group.corun();
                    assert_eq!(group.size(), 0);

                    spawned.wait().expect("the body did not panic");
                    dependent.wait().expect("the body did not panic");
                    assert!(first.is_done() && last.is_done());
                    assert_eq!(ran.load(Ordering::Relaxed), 4);
                }
            })
            .wait()
            .expect("no task panicked");

        assert_eq!(ran.load(Ordering::Relaxed), 4);
    }

    /// A group names the executor that opened it: upstream's
    /// `assert(&(tg.executor()) == &executor)`, which is an identity rather
    /// than an equality there and here.
    #[test]
    fn a_group_names_the_executor_that_opened_it() {
        let executor = Arc::new(Executor::new(1));
        let same = Arc::new(AtomicBool::new(false));

        executor
            .spawn({
                let executor = Arc::clone(&executor);
                let same = Arc::clone(&same);
                move || {
                    let group = executor.task_group().expect("a worker may open a group");
                    same.store(
                        std::ptr::eq(group.executor(), &*executor),
                        Ordering::Release,
                    );
                }
            })
            .wait()
            .expect("no task panicked");

        assert!(same.load(Ordering::Acquire));
    }

    /// A cancelled group runs none of the children it had not started, and
    /// `is_cancelled` reports the bit either side of the call.
    ///
    /// The ordering is driven by the cancel coming *first*, which is the
    /// hazard-free half of upstream's `Cancellation` case: there is no
    /// wedging and no handshake, so a regression -- a child that ran anyway,
    /// or a counter the skip path failed to spend -- is a failed assertion
    /// rather than anything that can hang. `tests/task_group.rs` has
    /// upstream's own shape, where the children are spawned before the
    /// cancel.
    #[test]
    fn a_child_spawned_into_a_cancelled_group_does_not_run() {
        let executor = Arc::new(Executor::new(2));
        let ran = Arc::new(AtomicUsize::new(0));

        executor
            .spawn({
                let executor = Arc::clone(&executor);
                let ran = Arc::clone(&ran);
                move || {
                    let group = executor.task_group().expect("a worker may open a group");

                    assert!(!group.is_cancelled());
                    group.cancel();
                    assert!(group.is_cancelled());

                    for _ in 0..100 {
                        let ran = Arc::clone(&ran);
                        group.silent_async(move || {
                            ran.fetch_add(1, Ordering::Relaxed);
                        });
                    }

                    // Every one of them is counted and then skipped, which is
                    // what brings the counter back to zero: a skip that did
                    // not tear the job down would hang here instead.
                    group.corun();
                    assert_eq!(group.size(), 0);
                }
            })
            .wait()
            .expect("no task panicked");

        assert_eq!(ran.load(Ordering::Relaxed), 0);
    }
}

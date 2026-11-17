//! Translated from `taskflow/core/async.hpp` (Taskflow v4.1.0): `async` --
//! [`spawn`](Executor::spawn) here -- [`silent_async`](Executor::silent_async),
//! the closure branches of `_async` and `_silent_async`,
//! `_schedule_async_task` and the parentless branch of `_tear_down_async`.
//! Case 0 of `_invoke_async_task`, the `ASYNC` arm of `_invoke` and
//! `_increment_topology`/`_decrement_topology` come from
//! `taskflow/core/executor.hpp` of the same release and land in [`invoke`]
//! and in the two counters below.
//!
//! An async task is the one piece of work the executor runs that belongs to
//! no taskflow: a closure handed straight to the pool, from any thread, with
//! or without a handle to its result. `executor::async_node` is the node such
//! a task is and `executor::topology` is the run it claims to be part of;
//! what this module adds is the submitting calls, the counting that makes
//! [`Executor::wait_for_all`] and [`Executor::drop`](Executor) cover that
//! work, and the dispatch arm that runs it.
//!
//! # `spawn`, not `async`
//!
//! Upstream's name for the value-returning form is `async`, which is a
//! reserved word in Rust 2024. The port calls it `spawn` on every surface
//! that has it -- the [`Executor`] here, later a `Runtime` (#90) and a
//! `TaskGroup` (#94) -- and keeps the other three names verbatim, since
//! `silent_async`, `dependent_async` and `silent_dependent_async` are all
//! legal identifiers. A runtime-shaped body gets a `_runtime` suffix, as
//! `emplace_runtime` already does.
//!
//! Upstream's `TaskParams` overloads collapse onto the unnamed forms: all
//! three of its `async` overloads differ only in the name they give the
//! node, an async node has no name worth reading in this milestone, and the
//! observers that would read one are a later step.
//!
//! # The handle, where upstream has a `std::future`
//!
//! [`AsyncHandle`] is what `spawn` hands back, and it is upstream's
//! `std::future<R>` down to who catches what. Upstream wraps the body in a
//! `std::packaged_task<R()>`, which is itself what turns a thrown exception
//! into a `set_exception` on the future; here the wrapper closure the
//! submission builds does the same job with [`catch_unwind`](panic::catch_unwind),
//! so the result -- or the panic payload -- goes into the handle's slot and
//! the executor's own handler never sees a `spawn` body fail. That is
//! upstream's arrangement exactly: its `TF_EXECUTOR_EXCEPTION_HANDLER` never
//! sees one either, because the packaged task has already swallowed it.
//!
//! So the node that runs a `spawn` body carries a *plain* body like a
//! `silent_async`'s, whatever `R` is. That is what keeps one node type -- and
//! therefore one queue of jobs -- able to hold async tasks that disagree
//! about their return type, and it is the reason [`AsyncWork`] is not generic
//! over `R`.
//!
//! # Where a free async job is queued
//!
//! Upstream's `_schedule_async_task` branches on whether the submitting
//! thread is a worker of this executor, and its comment on the first branch
//! is the reason that branch is a plain `_schedule` rather than the
//! per-worker cache the rest of its scheduler uses:
//!
//! > We don't do per-worker cache as it can cause bugs in corun that are very
//! > difficult to track. For example, when a worker invokes an async task and
//! > then immediately enter corun, it becomes very difficult to get the task
//! > out of its cache correctly.
//!
//! The two branches collapse into one here, and what collapses them is where
//! the owning end of a worker's queue lives. A submission from a worker of
//! this executor is a submission from inside a task *body*, which is called
//! by the `WorkerThread` frame that holds that end -- and a frame cannot be
//! named from the code it called. `LocalQueue` in `executor::worker` has that
//! argument in full, for the corun loop that met it first; the consequence
//! here is that a free async job goes to the shared buffers whichever thread
//! submitted it, exactly as a corun loop's newly ready jobs do.
//!
//! What that costs is locality: the job is taken out of a buffer by whichever
//! worker sweeps it up rather than by the submitting worker's next pop. What
//! it does not cost is progress -- every worker steals from the buffers, and
//! a worker with one job in its own queue and one in a buffer finds both --
//! and it does not cost correctness at a single worker either: the submitting
//! worker's own sweep reaches the buffers before it sleeps.
//!
//! # What the executor counts, and what its `Drop` waits for
//!
//! Every async task increments `num_topologies` on creation and decrements it
//! at tear-down, which is upstream's `_increment_topology` in `async` and
//! `silent_async` and the parentless branch of its `_tear_down_async`. So
//! [`Executor::wait_for_all`] covers async work without changing a line: the
//! count it blocks on reaching zero is runs *and* free async tasks, which is
//! what makes "wait until this executor is carrying nothing" true of the only
//! kind of work that has no handle to wait on.
//!
//! There is a second counter, `num_asyncs`, and it counts free async tasks
//! only. It exists because [`Executor`]'s `Drop` has to wait for this kind of
//! work and must *not* wait for a run: a run in flight is a run whose
//! [`RunHandle`](crate::RunHandle) is alive and borrows the executor, except
//! when that handle was leaked -- and the documented behaviour of a leaked
//! handle is that the pool still comes down. An async task is the other way
//! round: nothing borrows the executor for it, because
//! [`AsyncHandle`] is `'static` and dropping it detaches, so the only thing
//! that can keep the pool alive long enough to run a queued `silent_async` is
//! the executor's own destructor. Waiting on `num_asyncs` gives each of them
//! its own answer; waiting on `num_topologies` would have cost the leaked
//! handle its one.
//!
//! # A failure with nothing above it
//!
//! A `spawn` body cannot fail as far as the executor is concerned -- the
//! wrapper catches it -- so the only failure this module records is a
//! `silent_async`'s, and for a task submitted to the executor there is
//! nowhere to deliver it: no future, no frame waiting, no run that anybody
//! holds. Upstream parks the payload on the node, where its `_tear_down_async`
//! then recycles the node and nothing can ever read it; here it is
//! **dropped**, which is the same observable with the payload's destructor
//! run promptly instead of at the next node reuse.
//!
//! What it must *not* do is go through [`Job::record_panic`], and that is
//! worth stating because it is the obvious thing to call. The walk that
//! function performs stops at the first anchor above the failing node, and
//! the root of a parentless free node's chain is [`Parent::Run`] over the
//! *sentinel* run -- the one `Topology::free_standing` hands to every async
//! task in the process. Claiming the sentinel's payload slot would set
//! `EXCEPTION` in the estate word of a run that every later free node reads
//! through `Job::is_parent_cancelled`, so one `silent_async` that panicked
//! would cancel every async task submitted after it, for the life of the
//! process. [`record_failure`] is therefore a choice between the walk and the
//! floor, and `Parent::Run` means the floor.
//!
//! # An async handle inside a graph is still refused
//!
//! [`Graph::add_node`](crate::Graph::add_node) takes any [`Handle`], so a
//! caller can put a [`Handle::Async`] into a taskflow's graph. Upstream runs
//! such a node, because a `Node` is a `Node` to its `_invoke`; this port
//! refuses it, before and after this issue, and the refusal is the first
//! thing [`invoke`] does.
//!
//! The reason is that an async body is a `FnOnce` -- [`AsyncWork`] says so --
//! and a graph may be run any number of times. The second run of a taskflow
//! holding such a node would find the body gone, so the choice is between a
//! task that silently does nothing on every run but the first and a run that
//! says what is wrong. An async task belongs to the executor, not to a graph,
//! and the submitting calls above are how it is said.

use core::fmt;
use std::any::Any;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::executor::async_node::AsyncNode;
use crate::executor::run_handle::RunError;
use crate::executor::runtime;
use crate::executor::topology::{Job, Parent, Topology};
use crate::executor::worker::{Release, WorkerThread};
use crate::executor::{Executor, Shared};
use crate::graph::{AsyncWork, Handle};

impl Executor {
    /// Runs `body` on this executor as soon as a worker picks it up, and
    /// hands back a handle on its result.
    ///
    /// Upstream's `Executor::async`, renamed for the reason the module docs
    /// give. The body runs exactly once, on whichever worker takes it, and
    /// needs no taskflow, no graph and no run: it is submitted straight to
    /// the pool, from any thread -- including from a task body already
    /// running on one of this executor's workers, which is how async tasks
    /// nest.
    ///
    /// The [`AsyncHandle`] is upstream's `std::future<R>`. Wait on it for the
    /// value, poll it as a [`Future`], or drop it, which **detaches**: the
    /// task still runs and its result is thrown away, exactly as a discarded
    /// `std::future` behaves upstream. Nothing is cancelled by dropping a
    /// handle, and nothing blocks.
    ///
    /// A panic out of `body` is caught by the wrapper this call builds and
    /// reported from [`AsyncHandle::wait`] as
    /// [`RunError::Panicked`]`(payload)`, so no worker dies of it and no
    /// other work is cancelled by it. That is the one real difference from a
    /// task of a graph, whose panic cancels the rest of its run: an async
    /// task is not part of anything to cancel.
    ///
    /// ```
    /// use rustflow::Executor;
    ///
    /// let executor = Executor::new(2);
    ///
    /// let answer = executor.spawn(|| 6 * 7);
    /// assert_eq!(answer.wait().expect("the body did not panic"), 42);
    ///
    /// // A body that panics is the handle's failure and nobody else's.
    /// let failed = executor.spawn(|| -> i32 { panic!("not today") });
    /// let error = failed.wait().expect_err("the body panicked");
    /// assert_eq!(error.to_string(), "a task panicked: not today");
    ///
    /// // And the executor is unharmed by it.
    /// assert_eq!(executor.spawn(|| 1).wait().expect("no panic"), 1);
    /// ```
    pub fn spawn<R, F>(&self, body: F) -> AsyncHandle<R>
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        let delivery = Arc::new(Delivery::new());

        // Upstream's `std::packaged_task<R()>`, which is what catches the
        // exception on its way to the future: the node runs a plain body
        // whatever `R` is, and this closure is the whole of the difference
        // between `spawn` and `silent_async`. `AssertUnwindSafe` is the
        // honest annotation -- the state a panicking body can leave
        // half-written is the caller's own, reachable only through what the
        // closure captured.
        let wrapper = {
            let delivery = Arc::clone(&delivery);
            move || {
                let outcome =
                    panic::catch_unwind(AssertUnwindSafe(body)).map_err(RunError::Panicked);
                delivery.deliver(outcome);
            }
        };

        self.schedule_async_task(AsyncWork::plain(wrapper), &Parent::Run);
        AsyncHandle { delivery }
    }

    /// Runs `body` on this executor as soon as a worker picks it up, with no
    /// handle on it at all.
    ///
    /// Upstream's `Executor::silent_async`, and the cheaper half of the pair:
    /// there is no slot to deliver into, so a submission costs the node and
    /// the body and nothing else. Everything [`Executor::spawn`] says about
    /// where the body runs and when applies here too.
    ///
    /// A panic out of `body` is **dropped**. Nothing is waiting for this task
    /// -- that is what "silent" means -- so there is nobody to report it to,
    /// and the module docs say what upstream does with the same payload and
    /// why this port does not route it through the failure machinery
    /// instead. The default panic hook still prints it, so a silent task that
    /// fails is not an invisible one.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::Executor;
    ///
    /// let executor = Executor::new(4);
    /// let counter = Arc::new(AtomicUsize::new(0));
    ///
    /// for _ in 0..100 {
    ///     let counter = Arc::clone(&counter);
    ///     executor.silent_async(move || {
    ///         counter.fetch_add(1, Ordering::Relaxed);
    ///     });
    /// }
    ///
    /// // The one thing that waits for work with no handle: the count every
    /// // async task is in.
    /// executor.wait_for_all();
    /// assert_eq!(counter.load(Ordering::Relaxed), 100);
    /// ```
    pub fn silent_async<F>(&self, body: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.schedule_async_task(AsyncWork::plain(body), &Parent::Run);
    }

    /// Creates the free-standing node `work` is the body of and queues it:
    /// upstream's `_schedule_async_task` plus the `_increment_topology` its
    /// two public callers make first.
    ///
    /// `parent` is upstream's `NodeBase* parent`, which is null for a
    /// submission to the executor and a node or an anchor for one from a
    /// runtime body (#90) or a task group (#94). [`Parent::Run`] stands in
    /// for the null, as it does for every free node: `executor::topology` has
    /// why the sentinel run is a better answer there than an `Option`.
    ///
    /// The order is upstream's and all three steps are load-bearing. The
    /// counters go up before anything is queued, because a worker skips the
    /// steal loop entirely while `num_topologies` is zero and because a job
    /// that ran and tore down before this call returned would otherwise
    /// decrement a count it was never in. The parent is told about the job
    /// next, for the same reason `Shared::schedule_graph` tells it before
    /// queueing a graph's sources. Only then is the job pushed, after which it
    /// may already be running.
    fn schedule_async_task(&self, work: AsyncWork, parent: &Parent) {
        self.shared.increment_async();

        let node = AsyncNode::new("", Handle::Async(work));
        let job = Job::free(&node, parent);
        parent.count_queued(Topology::free_standing(), 1);

        // Upstream's two branches, collapsed for the reason the module docs
        // give: the owning end of the submitting worker's queue belongs to
        // the frame below this call, so there is one destination and it is
        // the one every thread outside the pool uses.
        self.shared.bulk_schedule([job]);
    }
}

/// A handle on one [`Executor::spawn`]ed task, and the right to its result:
/// upstream's `std::future<R>`.
///
/// Dropping it detaches -- the task runs, its result is discarded -- so a
/// caller who wants the value waits for it with [`AsyncHandle::wait`] or
/// awaits the handle as a [`Future`].
///
/// What it deliberately is not is a cancellation token or a way to reach the
/// task's node. Upstream's future is neither: the one thing that can be said
/// about an async task once it is submitted is what it produced, and
/// [`AsyncTask`](crate::AsyncTask) -- the handle a dependent-async task hands
/// back (#91) -- is where identity and state questions live.
pub struct AsyncHandle<R> {
    /// The slot the wrapper closure delivers the outcome into, shared with
    /// that closure and therefore with the node that holds it.
    ///
    /// A share rather than a borrow for the reason every other handle in the
    /// crate is one: the task outlives this value whenever the handle is
    /// dropped early, and the body has to have somewhere to write even then.
    delivery: Arc<Delivery<R>>,
}

impl<R> AsyncHandle<R> {
    /// Blocks until the task has run, and hands back what its body returned.
    ///
    /// Upstream's `std::future::get`, which rethrows a stored exception on
    /// the calling thread; a `Result` says the same thing without unwinding
    /// somebody else's stack. [`RunError::Panicked`] carries the payload, so
    /// a caller who wants upstream's behaviour hands it to
    /// [`resume_unwind`](std::panic::resume_unwind).
    ///
    /// Waiting from inside a task body works and **blocks that worker**, as
    /// `std::future::get` does upstream: the pool loses a thread for the
    /// duration, so the task being waited for needs another worker to run on.
    /// [`Executor::corun_until`] is the non-blocking shape -- it keeps the
    /// calling worker in the work-stealing loop until a predicate says to
    /// stop -- and [`AsyncHandle::is_done`] is the predicate to give it.
    ///
    /// # Panics
    ///
    /// Panics if this handle's outcome has already been taken, which takes
    /// polling the handle as a [`Future`] to completion and then calling
    /// this: the value is handed over exactly once, because it is a value and
    /// not a copy.
    pub fn wait(self) -> Result<R, RunError> {
        self.delivery.join()
    }

    /// Whether the task has finished.
    ///
    /// Upstream has no accessor of its own -- `fu.wait_for(0s) ==
    /// future_status::ready` is how it is asked there -- and the answer means
    /// the same thing: the body has run and its outcome is in the slot, so a
    /// [`AsyncHandle::wait`] now returns without blocking. It says nothing
    /// about whether the body *succeeded*.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.delivery.is_done()
    }
}

/// A spawned task can be awaited instead of waited for, on any runtime and
/// without one: the slot holds the [`Waker`] of whoever is polling, and the
/// worker that finishes the task wakes it where it would otherwise signal the
/// condvar. This is [`RunHandle`](crate::RunHandle)'s arrangement, built the
/// same way and with the same two things worth knowing -- nothing here
/// spawns, blocks or sleeps, so there is no runtime to agree with and no
/// feature flag to turn on, and dropping the handle rather than polling it to
/// the end detaches the task instead of blocking the dropping thread, which
/// is the one way this differs from a run handle.
///
/// ```
/// use std::future::Future;
/// use std::pin::pin;
/// use std::sync::Arc;
/// use std::task::{Context, Poll, Wake, Waker};
///
/// use rustflow::Executor;
///
/// /// The whole runtime this needs: unpark the thread that is waiting.
/// struct Unpark(std::thread::Thread);
///
/// impl Wake for Unpark {
///     fn wake(self: Arc<Self>) {
///         self.0.unpark();
///     }
/// }
///
/// let executor = Executor::new(2);
///
/// let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
/// let mut context = Context::from_waker(&waker);
/// let mut task = pin!(executor.spawn(|| "done"));
///
/// let outcome = loop {
///     if let Poll::Ready(outcome) = task.as_mut().poll(&mut context) {
///         break outcome;
///     }
///     std::thread::park();
/// };
/// assert_eq!(outcome.expect("the body did not panic"), "done");
/// ```
///
/// # Panics
///
/// Polling a handle again after it has returned [`Poll::Ready`] panics, for
/// [`AsyncHandle::wait`]'s reason: the output is a value handed over once.
/// Upstream's future has the same rule and answers a second `get()` with a
/// `future_error` instead.
impl<R> Future for AsyncHandle<R> {
    type Output = Result<R, RunError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.delivery.poll_join(context.waker())
    }
}

impl<R> fmt::Debug for AsyncHandle<R> {
    /// Hand-written because the derived one would need `R: Debug` and would
    /// print the slot, which holds whatever the body returned. Whether the
    /// task is over is the whole of what identifies a handle's state.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncHandle")
            .field("done", &self.is_done())
            .finish()
    }
}

/// Where a [`spawn`](Executor::spawn)ed body leaves its outcome, and how
/// whoever is waiting for it is woken.
///
/// This is [`Topology`]'s `Completion` slot over one value rather than over a
/// run: the same two ways of waiting -- a condvar for a blocked thread, a
/// [`Waker`] for a polling future -- for the same reason that type keeps them
/// apart. A task is waited for one way or the other, never both, and keeping
/// the slots separate is what lets the blocking path stay a condvar instead
/// of a hand-rolled futex over the waker.
struct Delivery<R> {
    /// The outcome and whoever is to be woken by it.
    slot: Mutex<Slot<R>>,
    /// Signalled by [`Delivery::deliver`], under the lock.
    delivered: Condvar,
}

/// The guarded half of a [`Delivery`].
struct Slot<R> {
    /// Whether the body has run. The condvar's predicate, and a separate
    /// field from the outcome below because the outcome is *taken* by
    /// whoever waits: "finished" has to stay true afterwards.
    done: bool,
    /// What the body returned, or the payload it panicked with. `None`
    /// before the body runs and again once a waiter has taken it.
    outcome: Option<Result<R, RunError>>,
    /// A waker left by a future polling this task, woken instead of -- and
    /// as well as -- the condvar.
    waker: Option<Waker>,
}

impl<R> Delivery<R> {
    /// An empty slot, for a task that has not run.
    fn new() -> Self {
        Self {
            slot: Mutex::new(Slot {
                done: false,
                outcome: None,
                waker: None,
            }),
            delivered: Condvar::new(),
        }
    }

    /// Puts `outcome` in the slot and releases whoever is waiting for it:
    /// upstream's `set_value`/`set_exception` on the promise behind the
    /// future, which its `std::packaged_task` calls.
    ///
    /// Called from the wrapper closure, on the worker running the task, and
    /// therefore *before* the tear-down that decrements the counters -- which
    /// is what makes [`Executor::wait_for_all`] enough to guarantee that
    /// every handle of every finished task has its outcome.
    ///
    /// The notification is sent under the lock and the waker woken after it,
    /// as [`Topology::finish`] does and for its reason: a waiter cannot
    /// return from the condvar until it has the mutex back, so the unlock is
    /// this thread's last touch of the slot either way, and the waker is
    /// moved out first so that waking it touches nothing a released waiter
    /// may already have dropped.
    fn deliver(&self, outcome: Result<R, RunError>) {
        let mut slot = self.lock();
        slot.done = true;
        slot.outcome = Some(outcome);
        let waker = slot.waker.take();
        self.delivered.notify_all();
        drop(slot);

        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Whether the body has run.
    fn is_done(&self) -> bool {
        self.lock().done
    }

    /// Blocks until the body has run and takes its outcome.
    fn join(&self) -> Result<R, RunError> {
        let mut slot = self.lock();
        while !slot.done {
            slot = self
                .delivered
                .wait(slot)
                .unwrap_or_else(PoisonError::into_inner);
        }
        slot.outcome.take().expect(OUTCOME_TAKEN_TWICE)
    }

    /// Takes the outcome if the body has run, and otherwise leaves `waker` to
    /// be woken when it does.
    ///
    /// Registering the waker and reading the flag happen under one lock,
    /// which is what stops a task finishing between the two and leaving
    /// nobody to wake.
    fn poll_join(&self, waker: &Waker) -> Poll<Result<R, RunError>> {
        let mut slot = self.lock();
        if slot.done {
            return Poll::Ready(slot.outcome.take().expect(OUTCOME_TAKEN_TWICE));
        }

        // Replaced rather than kept, because a future may be polled by a
        // different task each time; `will_wake` is the cheap way to skip the
        // clone in the common case where it is not.
        if !slot
            .waker
            .as_ref()
            .is_some_and(|current| current.will_wake(waker))
        {
            slot.waker = Some(waker.clone());
        }
        Poll::Pending
    }

    /// The slot. Poisoning is taken rather than propagated, as it is
    /// everywhere else in the executor: the body's own panic is caught by the
    /// wrapper and delivered rather than unwound through this lock, so a
    /// poisoned slot means a panic somewhere that has nothing to do with the
    /// value in it, and refusing to hand it back would turn that into a stuck
    /// `wait`.
    fn lock(&self) -> MutexGuard<'_, Slot<R>> {
        self.slot.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What a second taker of one outcome is told. A value can be handed over
/// once, so this is a caller that polled the handle to [`Poll::Ready`] and
/// then waited on it as well.
const OUTCOME_TAKEN_TWICE: &str = "the outcome of an async task is handed over exactly once";

impl Shared {
    /// Counts one more free async task, before it is queued.
    ///
    /// Upstream's `_increment_topology`, called by `async` and `silent_async`
    /// before `_schedule_async_task`, plus this port's second counter -- the
    /// one `Executor::drop` waits on, which the module docs argue for.
    ///
    /// Relaxed, as the run count's own increment is: what publishes the job
    /// to another worker is the queue it goes into, not either of these
    /// words.
    pub(crate) fn increment_async(&self) {
        self.increment_topology();
        self.num_asyncs.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one free async task as finished, waking a
    /// [`wait_for_all`](Executor::wait_for_all) or an
    /// [`Executor::drop`](Executor) that was waiting for the last of either
    /// kind.
    ///
    /// Upstream's `_decrement_topology` in the parentless branch of
    /// `_tear_down_async`, over both counters. The run count goes first so
    /// that a `Drop` released by the second one cannot find the executor
    /// still claiming to carry a topology it has already finished.
    pub(crate) fn finish_async(&self) {
        self.decrement_topology();

        if self.num_asyncs.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Under the lock, for `decrement_topology`'s reason: a waiter
            // that has read a non-zero count but not yet reached `wait` still
            // holds it, so taking it here is what keeps this notification
            // from arriving before the waiter is listening.
            let _idle = self.asyncs.lock().unwrap_or_else(PoisonError::into_inner);
            self.no_asyncs.notify_all();
        }
    }

    /// Blocks until no free async task is queued or running on this executor.
    ///
    /// The half of upstream's `wait_for_all` that its destructor needs, over
    /// the counter that excludes runs. [`Executor::wait_for_all`] is the
    /// whole of it and stays the public call.
    ///
    /// Acquire, as upstream's `_num_topologies.load` is and where this
    /// port's `num_topologies` read is relaxed: a caller released by this
    /// goes on to observe whatever the last async body wrote, and the
    /// release half of [`Shared::finish_async`]'s `fetch_sub` is what it
    /// pairs with. The join of every worker thread that follows in
    /// `Executor::drop` would carry the same ordering, but a counter whose
    /// only correct read is "the one with a thread join after it" is not a
    /// counter worth having.
    pub(crate) fn wait_for_asyncs(&self) {
        let mut idle = self.asyncs.lock().unwrap_or_else(PoisonError::into_inner);
        while self.num_asyncs.load(Ordering::Acquire) != 0 {
            idle = self
                .no_asyncs
                .wait(idle)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

/// Runs the async task `job` is for: upstream's `ASYNC` arm of `_invoke`,
/// which is case 0 of `_invoke_async_task` followed by `_tear_down_async`.
///
/// `Some(Release::Strong)` for a task that is over -- a free node has no
/// successors to release, so what the answer buys is the tear-down
/// `WorkerThread::invoke` performs on the way out, which is the *other*
/// branch of upstream's `_tear_down_async`: a job whose parent is a node or
/// an anchor counts that parent down and resumes it if it was preempted, and
/// `WorkerThread::finish_job` already does exactly that for every other node
/// kind.
/// A free node with no parent reaches `Parent::Run` over the sentinel run
/// instead, whose counter can never reach zero, so that tear-down correctly
/// does nothing and the counters below are the whole of what such a job owes.
///
/// `None` is upstream's `if(_invoke_async_task(...)) return;` -- a
/// runtime-shaped body that preempted its own node -- and the counters are
/// deliberately *not* touched then: the second entry is what finishes the
/// task. Nothing can produce that answer until #90 fills in
/// [`runtime::invoke_async`].
pub(crate) fn invoke(worker: &mut WorkerThread, job: &Job, work: &AsyncWork) -> Option<Release> {
    // Upstream has no equivalent and needs none; the module docs say why a
    // handle in a graph is refused rather than run, and `Topology` identity
    // is what tells the two apart -- the sentinel run is reachable only
    // through `Job::free`, which only a submitting call above makes.
    //
    // The refusal goes through the channel a panicking task body uses, for
    // the reason `executor::module` gives: `record_panic` sets `EXCEPTION`
    // beside `CAUGHT`, and `EXCEPTION` is half of `Topology::cancelled`, so
    // refusing one node cancels the rest of the run and `wait` hands the
    // message back. A bare `panic!` would be worse rather than louder -- it
    // would unwind with this job never counted as finished, so the run's join
    // counter would never reach zero and a `wait` would block for ever.
    if !is_free_standing(job) {
        job.topology().record_panic(Box::new(
            "an async node cannot be run as part of a taskflow: \
             an async task is submitted to the executor, not built into a graph",
        ));
        return Some(Release::Strong);
    }

    let release = match work {
        // Upstream's case 0, `void()`: the body of a `silent_async` and the
        // wrapper a `spawn` submits, under the `catch_unwind` that is its
        // `TF_EXECUTOR_EXCEPTION_HANDLER`. `AssertUnwindSafe` is the honest
        // annotation, for `Handle::Static`'s reason.
        //
        // Taken out of the slot rather than called through it, because an
        // async body is a `FnOnce` and a `FnOnce` cannot be called through a
        // shared reference -- `OnceWork` is that slot. `None` would mean the
        // body had already been taken, which no async node can reach: one
        // submission makes one job, and only a job can take the body. Doing
        // nothing is the one answer to it that cannot run a body twice.
        AsyncWork::Plain(body) => {
            if let Some(body) = body.take()
                && let Err(payload) = panic::catch_unwind(AssertUnwindSafe(body))
            {
                record_failure(job, payload);
            }
            Some(Release::Strong)
        }
        // Upstream's cases 1 and 2, `void(Runtime&)` and `void(Runtime&,
        // bool)`, which both go to its `_invoke_runtime_task_impl` and are
        // #90's here. One line out to the module that owns runtime tasking,
        // so that #90 edits that file and not this one.
        AsyncWork::Runtime(_) | AsyncWork::Reentrant { .. } => {
            runtime::invoke_async(worker, job, work)
        }
    };

    // Upstream's parentless branch of `_tear_down_async`:
    // `if(node->_parent == nullptr) _decrement_topology();`, where this
    // port's null parent is `Parent::Run` on a free node. A parented one --
    // #90's runtime child, #94's task-group member -- is not counted here
    // because it was not counted on submission either: only the executor's
    // own `spawn` and `silent_async` increment, as only upstream's `async`
    // and `silent_async` do.
    //
    // The job itself is spent by `WorkerThread::invoke` immediately after
    // this returns, which is what drops the node and the body with it --
    // upstream's `recycle(node)`, as the last share going rather than a call.
    // That it happens *after* these decrements is what makes an
    // `Executor::drop` released by them safe: the thread it releases goes on
    // to join every worker, so the worker still holding the node finishes
    // with it first.
    if release.is_some() && matches!(job.parent(), Parent::Run) {
        worker.shared.finish_async();
    }

    release
}

/// Whether `job` is a job of a free-standing async node rather than of a
/// handle somebody put in a taskflow's graph.
///
/// The run is the discriminator because it is the one field only a free node
/// can have: `Job::free` is the only way to get the sentinel
/// `Topology::free_standing` onto a job, and every other constructor carries
/// the topology of a real run. `executor::topology` has why a free node
/// claims to be part of a run at all.
fn is_free_standing(job: &Job) -> bool {
    std::ptr::eq(job.topology(), &**Topology::free_standing())
}

/// Delivers `payload` -- what a free node's body panicked with -- to whatever
/// is waiting above that node, or drops it if nothing is.
///
/// Upstream's `_process_exception`, with the parentless case answered
/// differently and on purpose; the module docs have the argument in full. The
/// short of it is that a parentless free node's chain roots at the sentinel
/// run, so walking it would record the failure of one `silent_async` against
/// a run that every later async task of the process reads as its own.
///
/// A free node *under* a parent is a different question and gets the walk:
/// its chain is a chain of jobs the failing job owns, and the anchor at the
/// end of it -- a runtime node that preempted itself, a task group's anchor,
/// or the run a parented chain reaches -- is something that can be waited on
/// and therefore something a failure is worth marking. #90 and #94 are where
/// such a node first becomes creatable, and where the one chain this walk
/// still cannot help with lands: a free node under a *free* parent that is
/// itself parentless roots at the sentinel again, which is why upstream
/// anchors the parent of such a chain explicitly.
fn record_failure(job: &Job, payload: Box<dyn Any + Send>) {
    match job.parent() {
        // Upstream's `nullptr | nullptr` row: `exe.silent_async()`, with no
        // future, no frame and no run above it. There is nowhere to deliver,
        // so the payload is dropped here rather than parked where nothing can
        // read it.
        Parent::Run => drop(payload),
        Parent::Nested(_) => job.record_panic(payload),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::prelude::*;

    /// How long a handshake waits for the other side before it gives up.
    ///
    /// The helper `tests/basics.rs` and `executor::run_handle` both keep, for
    /// the reason they give: nothing here passes because of this number, and
    /// the only thing that reaches it is a side that is never going to
    /// arrive.
    const GIVE_UP_AFTER: Duration = Duration::from_secs(120);

    /// Spins until `flag` is set, failing rather than hanging if it never is.
    fn spin_until(what: &str, flag: &AtomicBool) {
        let deadline = Instant::now() + GIVE_UP_AFTER;
        while !flag.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "gave up waiting for {what}");
            thread::yield_now();
        }
    }

    /// A `spawn` body that panics is the handle's failure and nobody else's:
    /// the payload comes back from `wait` with its identity intact, and the
    /// executor goes on running work. Upstream's `packaged_task` promises the
    /// same thing through `set_exception`.
    #[test]
    fn a_spawned_body_that_panics_is_reported_from_its_handle() {
        let executor = Executor::new(2);

        let error = executor
            .spawn(|| panic!("boom"))
            .wait()
            .expect_err("the body panicked");
        assert_eq!(error.to_string(), "a task panicked: boom");

        let RunError::Panicked(payload) = error;
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"boom"),
            "the payload keeps its identity, as a run's does"
        );

        assert_eq!(
            executor.spawn(|| 7).wait().expect("no panic"),
            7,
            "a failed async task is not the executor's failure"
        );
    }

    /// A `silent_async` that panics has nowhere to report, and the thing that
    /// matters is what it must *not* reach: the sentinel run every free node
    /// shares. A failure recorded there would set the `EXCEPTION` bit that
    /// `Job::is_parent_cancelled` reads, and every async task submitted after
    /// it -- for the life of the process -- would be skipped unrun.
    #[test]
    fn a_silent_async_that_panics_leaves_the_executor_usable() {
        let executor = Executor::new(2);
        let counter = Arc::new(AtomicUsize::new(0));

        executor.silent_async(|| panic!("dropped on the floor"));
        executor.wait_for_all();

        executor.silent_async({
            let counter = Arc::clone(&counter);
            move || {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        assert_eq!(executor.spawn(|| 1).wait().expect("no panic"), 1);
        executor.wait_for_all();

        assert_eq!(
            counter.load(Ordering::Relaxed),
            1,
            "a silent failure must not cancel the next async task"
        );
    }

    /// An executor dropped with async tasks still queued runs them before
    /// `drop` returns: upstream's destructor calls `wait_for_all` for exactly
    /// this, and nothing else can cover a `silent_async` -- there is no
    /// handle to borrow the executor and none to block on.
    ///
    /// A thousand of them rather than one, so that the race the guarantee is
    /// about is actually run: at two workers, the tail of that queue cannot
    /// have been picked up by the time the drop starts.
    #[test]
    fn dropping_the_executor_runs_the_async_tasks_still_queued() {
        const TASKS: usize = 1000;

        let counter = Arc::new(AtomicUsize::new(0));
        {
            let executor = Executor::new(2);
            for _ in 0..TASKS {
                let counter = Arc::clone(&counter);
                executor.silent_async(move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                });
            }
            // Deliberately no `wait_for_all`: `Drop` is what has to cover
            // them, and `num_asyncs` is the counter it waits on.
        }

        assert_eq!(counter.load(Ordering::Relaxed), TASKS);
    }

    /// Where a free async job is queued, from both sides of the question
    /// upstream's `_schedule_async_task` branches on. It is the shared
    /// buffers either way, and the body below is where that is observable:
    /// the submitting worker's own queue stays empty, because the frame
    /// running this body holds the only end of it that can be pushed.
    ///
    /// One worker, so that nothing can steal the job out of the buffers
    /// before it has been looked for: the one worker there is is the one
    /// running this body.
    #[test]
    fn a_free_async_job_is_queued_through_the_shared_buffers() {
        let executor = Arc::new(Executor::new(1));
        let running = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));

        let mut taskflow = Taskflow::new();
        taskflow.emplace({
            let executor = Arc::clone(&executor);
            let running = Arc::clone(&running);
            let release = Arc::clone(&release);
            move || {
                executor.silent_async(|| {});
                assert!(
                    !executor.shared.buffers.is_empty(),
                    "a submission from a worker goes to the buffers"
                );
                assert!(
                    executor.shared.workers[0].is_queue_empty(),
                    "and not onto the queue this frame holds the owning end of"
                );

                running.store(true, Ordering::Release);
                spin_until("the test thread to let the worker go", &release);
            }
        });

        let run = executor.run(&taskflow);
        spin_until("the worker to reach the task body", &running);

        // And the other branch: from a thread that is no worker of this
        // executor, with the only worker there is parked in the body above so
        // that nothing can empty the buffers under this assertion.
        executor.silent_async(|| {});
        assert!(
            !executor.shared.buffers.is_empty(),
            "a submission from outside the pool goes to the buffers"
        );

        release.store(true, Ordering::Release);
        run.wait().expect("no task panicked");
        executor.wait_for_all();
    }

    /// `wait_for_all` covers async tasks, which is the whole point of
    /// counting them in `num_topologies`: when it returns, every handle has
    /// its outcome and reports it without blocking.
    #[test]
    fn wait_for_all_covers_an_async_task() {
        let executor = Executor::new(1);

        let handle = executor.spawn(|| 11);
        assert_eq!(executor.num_topologies(), 1, "one async task, counted");

        executor.wait_for_all();
        assert_eq!(executor.num_topologies(), 0);
        assert!(handle.is_done());
        assert_eq!(handle.wait().expect("no panic"), 11);
    }

    /// An async handle in a taskflow's graph refuses the run, and the refusal
    /// is reported as the run's failure rather than raised on a worker thread
    /// or hung on. #47's case for its own stubs, kept because the refusal is
    /// kept: the module docs say why a `FnOnce` body is not a graph citizen.
    #[test]
    fn an_async_node_in_a_graph_refuses_the_run() {
        let executor = Executor::new(2);
        let mut taskflow = Taskflow::new();
        taskflow
            .graph_mut()
            .add_named_node("free", Handle::Async(AsyncWork::plain(|| {})));

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("an async node is refused");
        assert_eq!(
            error.to_string(),
            "a task panicked: an async node cannot be run as part of a taskflow: \
             an async task is submitted to the executor, not built into a graph"
        );
    }
}

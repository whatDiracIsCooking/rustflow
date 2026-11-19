//! Translated from `taskflow/core/async.hpp` (Taskflow v4.1.0): `async` --
//! [`spawn`](Executor::spawn) here -- `silent_async`, the closure branches of
//! `_async` and `_silent_async`, `_schedule_async_task` and the parentless
//! branch of `_tear_down_async`. Case 0 of `_invoke_async_task`, the `ASYNC`
//! arm of `_invoke` and `_increment_topology`/`_decrement_topology` come from
//! `taskflow/core/executor.hpp` of the same release and land in [`invoke`]
//! and in the two counters below.
//!
//! An async task is the one piece of work the executor runs that belongs to
//! no taskflow: a closure handed straight to the pool, from any thread, with
//! or without a handle on its result. `executor::async_node` is the node such
//! a task is and `executor::topology` is the sentinel run it claims to be
//! part of; this module holds the submission every receiver goes through, the
//! handle a submitted body delivers into, the counting that makes
//! [`Executor::wait_for_all`] and [`Executor::drop`](Executor) cover async
//! work, and the dispatch arm that runs a free node.
//!
//! # Design
//!
//! The family is upstream's overload set: three receivers -- [`Executor`], a
//! [`Runtime`] body and a [`TaskGroup`](crate::TaskGroup) -- times
//! dependent-or-free, times handle-or-silent, times a plain or a
//! runtime-shaped body, times named-or-not, which is 48 public methods over
//! upstream's four names. Three things vary across them and the core names
//! each once. [`Submission`] is the receiver: the [`Parent`] the child is
//! accounted to and the worker whose own queue its job goes onto. [`Spawn`]
//! is the body and its parameters: the [`AsyncWork`] the node runs, the
//! estate word it is constructed with, the name the node carries and the
//! handle -- if any -- the caller is given back. The node kind is which
//! submission is called, [`Shared::schedule_async_task`] for a free node and
//! `Shared::schedule_dependent_async_task` in `executor::dependent_async` for
//! one with a dependency list. Every public method is one delegation that
//! builds those two and calls one of them, a `_named` form adding one
//! [`Spawn::named`] step to its twin's; `DESIGN.md` records the placement,
//! why the receiver is [`Shared`] and why the name is a method suffix rather
//! than a submission builder.
//!
//! Three invariants live in that core and in no receiver, each with its
//! decision in `DESIGN.md`. A submission made from inside a task body goes
//! onto the submitting worker's own LIFO queue rather than into the shared
//! buffers, which is [`Submission::queue`] and what bounds the stack of a
//! recursive body. The executor's two counters are raised for a
//! **parentless** submission only, a parented child being accounted for by
//! its parent's join counter and by nothing else, which is
//! [`Shared::new_free_node`] and the condition [`invoke`]'s tear-down spends
//! under. And a dependent submission's join counter starts at one, as the
//! submitting thread's own guard.
//!
//! What a submission costs was measured rather than argued: most of an
//! externally submitted `silent_async` is the free node's heap blocks, and 2
//! to 4% of it the counters below. `BENCHMARKS.md` has the ablations and what
//! a block cost; `DESIGN.md` has why the node is two blocks and not one. The
//! node's own block is recycled rather than freed when the worker that
//! finished the task holds the last share of it and a submission has drawn
//! from its list before, so a submission that names a worker writes into a
//! block that worker parked whenever it has one -- [`FreeNodes`], and
//! `DESIGN.md` for why that list is one per worker, what it deliberately
//! does not pool and what the draw condition is worth.

use core::fmt;
use std::any::Any;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::executor::async_node::{AsyncNode, FreeNodes};
use crate::executor::run_handle::RunError;
use crate::executor::runtime;
use crate::executor::topology::{Cancellable, Job, Nested, Parent, Topology};
use crate::executor::worker::{Release, WorkerThread};
use crate::executor::{Executor, Shared};
use crate::graph::{AsyncWork, Handle};
use crate::runtime::Runtime;

/// Upstream's `ESTATE::NONE`, the estate word a free node is constructed with
/// unless something is waiting for what runs under it.
///
/// A named zero because a bare `0` says nothing about which word it is the
/// absence of bits in.
pub(crate) const ESTATE_NONE: u32 = 0;

/// What a submitted body decides, as one value: the work a free node runs,
/// the estate word its node is constructed with, the name that node carries
/// and the handle the submitting call hands back.
///
/// Upstream's leading `(params, f)` pair of `_async(params, f, tpg, parent)`,
/// [`Submission`] being the `(tpg, parent)` pair after it: the
/// `if constexpr(is_runtime_task_v<F>)` branch together with the
/// `std::packaged_task` or `std::promise` its handle-returning forms build,
/// and the `TaskParamsLike` argument its named overloads take. `H` is `()`
/// for a silent form and an [`AsyncHandle`] for one that reports, so the four
/// constructors are the four body shapes and [`Spawn::named`] is the one axis
/// across all four.
pub(crate) struct Spawn<H> {
    /// The body as the node's [`Handle`] will hold it.
    work: AsyncWork,
    /// Upstream's `ESTATE` argument to its internal forms.
    estate: u32,
    /// The name the node is constructed with, which an observer reads back
    /// through `TaskView::name`: upstream's `TaskParams::name`, and empty
    /// unless [`Spawn::named`] was called -- upstream's `DefaultTaskParams`,
    /// whose `Node` constructor leaves `_name` default-constructed.
    name: String,
    /// What the submitting call returns.
    handle: H,
}

impl<H> Spawn<H> {
    /// The same submission with the node named `name`: upstream's
    /// `TaskParams{name}` where the forms below pass its
    /// `DefaultTaskParams{}`.
    pub(crate) fn named(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// The four pieces, for the submission about to place them: the work into
    /// the node's handle, the estate word into its state, the name into the
    /// node, the handle back to the caller.
    pub(crate) fn into_parts(self) -> (AsyncWork, u32, String, H) {
        (self.work, self.estate, self.name, self.handle)
    }
}

impl Spawn<()> {
    /// A `void()` body with nothing waiting for it: upstream's
    /// `silent_async`.
    pub(crate) fn silent<F>(body: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Self {
            work: AsyncWork::plain(body),
            estate: ESTATE_NONE,
            name: String::new(),
            handle: (),
        }
    }

    /// A `void(tf::Runtime&)` body with nothing waiting for it: upstream's
    /// `silent_async` over a runtime-shaped callable, which is its case 1 and
    /// has no second entry to report from.
    pub(crate) fn silent_runtime<F>(body: F) -> Self
    where
        F: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static,
    {
        Self {
            work: AsyncWork::runtime(body),
            estate: ESTATE_NONE,
            name: String::new(),
            handle: (),
        }
    }
}

impl<R: Send + 'static> Spawn<AsyncHandle<R>> {
    /// A body whose return value a handle is waiting for: upstream's
    /// `std::packaged_task<R()>` plus the `get_future()` beside it.
    ///
    /// The wrapper is what catches a panic out of `body`, so the node runs a
    /// plain body whatever `R` is and the executor's own failure machinery
    /// never sees such a failure -- upstream's arrangement exactly.
    pub(crate) fn handled<F>(body: F) -> Self
    where
        F: FnOnce() -> R + Send + 'static,
    {
        let (wrapper, handle) = AsyncHandle::for_body(body);
        Self {
            work: AsyncWork::plain(wrapper),
            estate: ESTATE_NONE,
            name: String::new(),
            handle,
        }
    }
}

impl Spawn<AsyncHandle<()>> {
    /// A `void(tf::Runtime&, bool)` body: upstream's `std::promise<void>`
    /// with the `reentered` branch that delivers `rt._node->_exception_ptr`
    /// into it, so the handle answers for the body *and* for everything it
    /// spawned.
    ///
    /// The node is an explicit anchor from the moment it is created, which is
    /// upstream's own estate argument for this shape: the waiter -- whoever
    /// holds the handle -- exists for the whole life of the node.
    pub(crate) fn handled_runtime<F>(body: F) -> Self
    where
        F: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static,
    {
        let (handle, delivery) = AsyncHandle::pending();
        let complete = move |payload: Option<Box<dyn Any + Send>>| {
            // Upstream's `eptr ? p.object.set_exception(eptr) :
            // p.object.set_value()`.
            delivery.deliver(match payload {
                Some(payload) => Err(RunError::Panicked(payload)),
                None => Ok(()),
            });
        };

        Self {
            work: AsyncWork::reentrant(body, complete),
            estate: Cancellable::EXPLICITLY_ANCHORED,
            name: String::new(),
            handle,
        }
    }
}

/// Where a submitted task belongs: the parent whose join counter accounts for
/// it, and the worker whose own queue its job is pushed onto.
///
/// Upstream's two trailing arguments to `_async` and `_silent_async` -- a
/// `Topology*` and a `NodeBase* parent` -- as one value, the run travelling
/// with the job rather than with the node here. The constructors are the
/// receivers of the family, a task group having one of each kind because it
/// has two routes to open it through.
pub(crate) struct Submission<'a> {
    /// What the child is counted against.
    parent: Parent,
    /// The worker whose queue the job goes onto, or `None` for the shared
    /// buffers.
    worker: Option<&'a WorkerThread>,
}

impl<'a> Submission<'a> {
    /// A submission to the executor itself, parented to nothing: upstream's
    /// null `parent` over its null `_topology`, which is [`Parent::Run`] over
    /// the sentinel run here. The job goes to the shared buffers whichever
    /// thread submits it, because a method on [`Executor`] cannot name the
    /// frame that called it.
    pub(crate) fn parentless() -> Self {
        Self {
            parent: Parent::Run,
            worker: None,
        }
    }

    /// A child of the node `parent` was dequeued as, on the queue of the
    /// worker running that node's body: upstream's
    /// `_executor._silent_async(.., _node->_topology, _node)` out of
    /// `tf::Runtime`, whose `this_worker()` is that worker by construction.
    /// A fresh [`Nested`] per child, holding a clone of the job, because that
    /// job is what the last child to finish re-queues to resume the node.
    pub(crate) fn under_node(parent: &Job, worker: &'a WorkerThread) -> Self {
        Self {
            parent: Nested::new(Some(parent.clone())).as_parent(),
            worker: Some(worker),
        }
    }

    /// A child of a task group's anchor, for a group that holds its worker's
    /// *id*: upstream's `&_node_base` over the buffers, because a `corun`
    /// that has no owning end of the queue pops the other one.
    /// `DESIGN.md`'s "Dynamic tasking" has the pair of routes.
    pub(crate) fn under_anchor(anchor: &Arc<Nested>) -> Self {
        Self {
            parent: anchor.as_parent(),
            worker: None,
        }
    }

    /// A child of a task group's anchor, for a group that holds the worker
    /// itself: the same `&_node_base` over upstream's `this_worker()` branch
    /// of `_schedule_async_task`, which is the branch every one of its
    /// groups takes.
    pub(crate) fn under_anchor_on(anchor: &Arc<Nested>, worker: &'a WorkerThread) -> Self {
        Self {
            parent: anchor.as_parent(),
            worker: Some(worker),
        }
    }

    /// What this submission's child is accounted to.
    pub(crate) fn parent(&self) -> &Parent {
        &self.parent
    }

    /// Whether nothing above the child is waiting for it, which is the one
    /// condition the executor's own counters are kept under.
    fn is_parentless(&self) -> bool {
        matches!(self.parent, Parent::Run)
    }

    /// The free list this submission's node may be written into: the list of
    /// the worker whose queue the job is going onto, which is a worker
    /// running the body that submits. `None` for a submission that goes to
    /// the shared buffers instead, which allocates its node as it always did
    /// -- no worker's list is that job's, the buffers being everybody's.
    fn free_nodes(&self) -> Option<&FreeNodes> {
        self.worker.map(WorkerThread::free_nodes)
    }

    /// Queues one job of this submission: upstream's two branches of
    /// `_schedule_async_task`, over the argument rather than over a
    /// thread-local lookup, since the owning end of a worker's queue lives in
    /// the [`WorkerThread`] frame below the task body and a lookup could not
    /// hand it out. That queue is LIFO and a [`Runtime::corun`] -- or the
    /// `corun` of a group opened from one -- pops the same end, so a body
    /// that spawns and then coruns gets *that* child back where the buffers
    /// hand back the oldest job in the pool; `DESIGN.md` has what the
    /// difference buys.
    pub(crate) fn queue(&self, shared: &Shared, job: Job) {
        match self.worker {
            Some(worker) => worker.schedule(job),
            None => shared.bulk_schedule([job]),
        }
    }
}

impl Executor {
    /// Runs `body` on this executor as soon as a worker picks it up, and
    /// hands back a handle on its result.
    ///
    /// Upstream's `Executor::async`, renamed for the reason `DESIGN.md`
    /// gives. The body runs exactly once, on whichever worker takes it, and
    /// needs no taskflow, no graph and no run: it goes straight to the pool,
    /// from any thread -- including from a task body already running on one
    /// of this executor's workers, which is how async tasks nest.
    ///
    /// The [`AsyncHandle`] is upstream's `std::future<R>`. Wait on it for the
    /// value, poll it as a [`Future`], or drop it, which **detaches**: the
    /// task still runs and its result is thrown away, as a discarded
    /// `std::future` behaves upstream. Nothing is cancelled by dropping a
    /// handle, and nothing blocks.
    ///
    /// A panic out of `body` is reported from [`AsyncHandle::wait`] as
    /// [`RunError::Panicked`]`(payload)`, so no worker dies of it and no
    /// other work is cancelled by it -- an async task is not part of anything
    /// to cancel.
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
        self.shared
            .schedule_async_task(Spawn::handled(body), &Submission::parentless())
    }

    /// [`Executor::spawn`] with the free node this task is named `name`:
    /// upstream's `async(params, f)`, over the `StringLike` reading of its
    /// `TaskParamsLike`.
    ///
    /// The name is what an [`Observer`](crate::observer::Observer) is handed
    /// as [`TaskView::name`](crate::observer::TaskView::name) when a worker
    /// enters the task, and so what a profiler trace labels the segment
    /// with. It is the only thing it changes: where and when the body runs,
    /// what the handle delivers and where a panic goes are
    /// [`Executor::spawn`]'s, and an unnamed submission is one whose node
    /// carries the empty name. Every form of this family -- on [`Executor`],
    /// on a [`Runtime`] and on a [`TaskGroup`](crate::TaskGroup) -- has a
    /// `_named` twin that says exactly this about its own name.
    ///
    /// ```
    /// use std::sync::Mutex;
    ///
    /// use rustflow::Executor;
    /// use rustflow::observer::{Observer, TaskView, WorkerView};
    ///
    /// /// Records the name of every task a worker enters.
    /// struct Names(Mutex<Vec<String>>);
    ///
    /// impl Observer for Names {
    ///     fn set_up(&self, _num_workers: usize) {}
    ///
    ///     fn on_entry(&self, _worker: WorkerView<'_>, task: TaskView<'_>) {
    ///         self.0.lock().expect("no body panicked").push(task.name().to_string());
    ///     }
    ///
    ///     fn on_exit(&self, _worker: WorkerView<'_>, _task: TaskView<'_>) {}
    /// }
    ///
    /// let executor = Executor::new(2);
    /// let seen = executor.make_observer(Names(Mutex::new(Vec::new())));
    ///
    /// let answer = executor.spawn_named("answer", || 6 * 7);
    /// assert_eq!(answer.wait().expect("the body did not panic"), 42);
    /// executor.wait_for_all();
    ///
    /// // `on_entry` runs on the worker before the body does, so the name is
    /// // recorded by the time the handle has a value.
    /// assert_eq!(*seen.0.lock().expect("no body panicked"), ["answer"]);
    /// ```
    pub fn spawn_named<R, F>(&self, name: impl Into<String>, body: F) -> AsyncHandle<R>
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        self.shared
            .schedule_async_task(Spawn::handled(body).named(name), &Submission::parentless())
    }

    /// Runs `body` on this executor as soon as a worker picks it up, with no
    /// handle on it at all.
    ///
    /// Upstream's `Executor::silent_async`, and the cheaper half of the pair:
    /// there is no slot to deliver into, so a submission costs the node and
    /// the body and nothing else. Everything [`Executor::spawn`] says about
    /// where the body runs and when applies here too.
    ///
    /// A panic out of `body` is **dropped**: nothing is waiting for this task
    /// -- that is what "silent" means -- so the payload lands on the node the
    /// task is, where upstream parks it too, and goes with the node one share
    /// later. The default panic hook still prints it.
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
        self.shared
            .schedule_async_task(Spawn::silent(body), &Submission::parentless());
    }

    /// [`Executor::silent_async`] with the free node this task is named
    /// `name`: upstream's `silent_async(params, f)`.
    ///
    /// [`Executor::spawn_named`] says what the name is and what reads it.
    pub fn silent_async_named<F>(&self, name: impl Into<String>, body: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.shared
            .schedule_async_task(Spawn::silent(body).named(name), &Submission::parentless());
    }
}

/// A handle on one [`Executor::spawn`]ed task, and the right to its result:
/// upstream's `std::future<R>`.
///
/// Dropping it detaches -- the task runs, its result is discarded -- so a
/// caller who wants the value waits for it with [`AsyncHandle::wait`] or
/// awaits the handle as a [`Future`]. What it deliberately is not is a
/// cancellation token or a way to reach the task's node, upstream's future
/// being neither; [`AsyncTask`](crate::AsyncTask) is where identity and state
/// questions live.
pub struct AsyncHandle<R> {
    /// The slot the body delivers its outcome into, shared with whatever
    /// writes it: a share rather than a borrow, because the task outlives
    /// this value whenever the handle is dropped early.
    delivery: Arc<Delivery<R>>,
}

impl<R> AsyncHandle<R> {
    /// Wraps `body` as the plain closure a free node runs, and hands back
    /// that closure together with the handle its outcome will arrive at:
    /// upstream's `std::packaged_task<R()>` plus the `get_future()` beside
    /// it. A pair rather than two calls for the reason `DESIGN.md` records,
    /// and `AssertUnwindSafe` is the honest annotation on the `catch_unwind`
    /// inside -- the state a panicking body can leave half-written is the
    /// caller's own.
    fn for_body<F>(body: F) -> (impl FnOnce() + Send + 'static, Self)
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        let (handle, delivery) = Self::pending();
        let wrapper = move || {
            let outcome = panic::catch_unwind(AssertUnwindSafe(body)).map_err(RunError::Panicked);
            delivery.deliver(outcome);
        };
        (wrapper, handle)
    }

    /// An empty handle and the slot that fills it: upstream's
    /// `std::promise<void> p; auto fu{p.get_future()};` at the top of the
    /// runtime-shaped branch of `_async`, which is the half of
    /// [`AsyncHandle::for_body`] for a task that does **not** deliver by
    /// returning a value -- a runtime-shaped task's outcome is decided on the
    /// entry that finishes the node, out of that node's payload slot.
    fn pending() -> (Self, Arc<Delivery<R>>) {
        let delivery = Arc::new(Delivery::new());
        (
            Self {
                delivery: Arc::clone(&delivery),
            },
            delivery,
        )
    }

    /// Blocks until the task has run, and hands back what its body returned.
    ///
    /// Upstream's `std::future::get`, which rethrows a stored exception on
    /// the calling thread; a `Result` says the same thing without unwinding
    /// somebody else's stack, and [`RunError::Panicked`] carries the payload
    /// for a caller who wants upstream's behaviour from
    /// [`resume_unwind`](std::panic::resume_unwind).
    ///
    /// Waiting from inside a task body works and **blocks that worker**, as
    /// `std::future::get` does upstream, so the task being waited for needs
    /// another worker to run on; [`Executor::corun_until`] over
    /// [`AsyncHandle::is_done`] is the non-blocking shape.
    ///
    /// What a returned `wait` does not order is the executor's accounting:
    /// the counters are spent by the tear-down a few instructions after the
    /// outcome is delivered, so [`Executor::num_topologies`] may still count
    /// this task for a moment. Nothing a caller can edit sits behind that
    /// count, `wait_for_all` is the call that promises zero, and upstream's
    /// `std::future::get` has the identical window.
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
    /// the same thing: the body has run and its outcome is in the slot, so an
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
/// condvar. [`RunHandle`](crate::RunHandle)'s arrangement, built the same way
/// -- nothing here spawns, blocks or sleeps, so there is no runtime to agree
/// with -- and differing from it in one thing: dropping the handle rather
/// than polling it to the end detaches the task instead of blocking.
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
    /// print the slot, which holds whatever the body returned.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncHandle")
            .field("done", &self.is_done())
            .finish()
    }
}

/// Where a submitted body leaves its outcome, and how whoever is waiting for
/// it is woken: [`Topology`]'s `Completion` slot over one value rather than
/// over a run, and for that type's reason -- a task is waited for by a
/// blocked thread or by a polling future, never both, and keeping the two
/// apart is what lets the blocking path stay a condvar.
struct Delivery<R> {
    /// The outcome and whoever is to be woken by it.
    slot: Mutex<Slot<R>>,
    /// Signalled by [`Delivery::deliver`], under the lock.
    delivered: Condvar,
}

/// The guarded half of a [`Delivery`].
struct Slot<R> {
    /// Whether the body has run: a separate field from the outcome because
    /// the outcome is *taken* by whoever waits, and "finished" has to stay
    /// true afterwards.
    done: bool,
    /// What the body returned, or the payload it panicked with.
    outcome: Option<Result<R, RunError>>,
    /// A waker left by a future polling this task, woken as well as the
    /// condvar.
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
    /// future. Called on the worker running the task and therefore *before*
    /// the tear-down that spends the counters, which is what makes
    /// [`Executor::wait_for_all`] enough to guarantee that every handle of
    /// every finished task has its outcome.
    ///
    /// The notification is sent under the lock and the waker woken after it,
    /// as [`Topology::finish`] does and for its reason: the waker is moved
    /// out first, so waking it touches nothing a released waiter may already
    /// have dropped.
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
    /// everywhere else in the executor: a panicking body is caught and
    /// delivered rather than unwound through this lock, so a poisoned slot
    /// means a panic that has nothing to do with the value in it and refusing
    /// to hand it back would turn that into a stuck `wait`.
    fn lock(&self) -> MutexGuard<'_, Slot<R>> {
        self.slot.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What a second taker of one outcome is told. A value can be handed over
/// once, so this is a caller that polled the handle to [`Poll::Ready`] and
/// then waited on it as well.
const OUTCOME_TAKEN_TWICE: &str = "the outcome of an async task is handed over exactly once";

impl Shared {
    /// Creates the free-standing node `spawn` is the body of and queues it:
    /// upstream's `_schedule_async_task`, plus the `_increment_topology` its
    /// two public callers make first.
    ///
    /// The order is upstream's and all three steps are load-bearing. The
    /// counters go up before anything is queued, because a worker skips the
    /// steal loop entirely while `num_topologies` is zero and because a job
    /// that ran and tore down before this call returned would otherwise
    /// decrement a count it was never in. The parent is told about the job
    /// next, for the same reason `Shared::schedule_graph` tells it before
    /// queueing a graph's sources. Only then is the job pushed, after which it
    /// may already be running.
    pub(crate) fn schedule_async_task<H>(&self, spawn: Spawn<H>, to: &Submission<'_>) -> H {
        let (work, estate, name, handle) = spawn.into_parts();
        let node = self.new_free_node(name, Handle::Async(work), estate, to);
        let job = Job::free(&node, to.parent());
        to.parent().count_queued(Topology::free_standing(), 1);
        to.queue(self, job);

        handle
    }

    /// The free-standing node a submission is about to queue, counted in
    /// whatever accounts for it: the half the two submissions share, plus the
    /// `_increment_topology` that is a line of upstream's public `async` and
    /// `silent_async` and not of the `_async(..., tpg, parent)` a runtime body
    /// or a task group calls.
    ///
    /// That condition is the whole of the counting rule, and [`invoke`]'s
    /// tear-down asks it again: a parented child is accounted for by the join
    /// counter of the parent waiting on it, so counting it here as well
    /// leaves `num_asyncs` and `num_topologies` carrying a job nothing ever
    /// spends -- a [`wait_for_all`](Executor::wait_for_all) and an
    /// [`Executor::drop`](Executor) that never return. `DESIGN.md` has what
    /// the two conditions disagreeing cost.
    ///
    /// The estate word is set before any job of the node exists, because a
    /// node that is to be an anchor has to be one before anything under it
    /// can fail. Where the node itself comes from -- a block the submitting
    /// worker parked, or the allocator -- is [`FreeNodes`]'s, and changes
    /// nothing a caller can see.
    pub(crate) fn new_free_node(
        &self,
        name: String,
        handle: Handle,
        estate: u32,
        to: &Submission<'_>,
    ) -> Arc<AsyncNode> {
        if to.is_parentless() {
            self.increment_async();
        }

        let node = match to.free_nodes() {
            Some(free) => free.animate(name, handle),
            None => AsyncNode::new(name, handle),
        };
        node.node()
            .base()
            .estate()
            .fetch_or(estate, Ordering::Relaxed);

        node
    }

    /// Counts one more free async task, before it is queued.
    ///
    /// Upstream's `_increment_topology`, plus this port's second counter --
    /// the one [`Executor::drop`](Executor) waits on, which `DESIGN.md`
    /// argues for. Relaxed, as the run count's own increment is: what
    /// publishes the job to another worker is the queue it goes into, not
    /// either of these words.
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

    /// Blocks until no free async task is queued or running on this executor:
    /// the half of upstream's `wait_for_all` that its destructor needs, over
    /// the counter that excludes runs.
    ///
    /// Acquire, as upstream's `_num_topologies.load` is and where this port's
    /// `num_topologies` read is relaxed: a caller released by this goes on to
    /// observe whatever the last async body wrote, and it pairs with the
    /// release half of [`Shared::finish_async`]'s `fetch_sub`. The worker
    /// joins that follow in `Executor::drop` would carry the same ordering,
    /// but a counter whose only correct read is "the one with a thread join
    /// after it" is not a counter worth having.
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
/// `Some(Release::Strong)` for a task that is over. A free node has no
/// successors to release, so what the answer buys is the tear-down
/// `WorkerThread::invoke` performs on the way out -- the *other* branch of
/// upstream's `_tear_down_async`, which counts a parent down and resumes it
/// if it was preempted. A parentless job reaches [`Parent::Run`] over the
/// sentinel run, whose counter can never reach zero, so that tear-down
/// correctly does nothing. `None` is upstream's
/// `if(_invoke_async_task(...)) return;` -- a runtime-shaped body that
/// preempted its own node -- and the counters are deliberately *not* touched
/// then: the second entry is what finishes the task.
pub(crate) fn invoke(worker: &mut WorkerThread, job: &Job, work: &AsyncWork) -> Option<Release> {
    // A `Handle::Async` somebody built into a taskflow's graph, which
    // upstream runs because a `Node` is a `Node` to its `_invoke` and this
    // port refuses: an async body is a `FnOnce` and a graph may be run any
    // number of times, so the alternative is a task that silently does
    // nothing on every run but the first. The identity of the run tells the
    // two apart, the sentinel being reachable only through `Job::free`.
    //
    // The refusal goes through the channel a panicking task body uses, for
    // the reason `executor::module` gives: a bare `panic!` would unwind with
    // this job never counted as finished, so a `wait` would block for ever.
    if !job.topology().is_free_standing() {
        job.topology().record_panic(Box::new(
            "an async node cannot be run as part of a taskflow: \
             an async task is submitted to the executor, not built into a graph",
        ));
        // `WorkerThread::invoke_one` skips its own generic call for this
        // handle kind -- see its `async_dispatch` -- so every return out of
        // this function makes its own, this one included.
        worker.observer_epilogue(job);
        return Some(Release::Strong);
    }

    let release = match work {
        // Upstream's case 0, `void()`: the body of a `silent_async` and the
        // wrapper a `spawn` submits, under the `catch_unwind` that is its
        // `TF_EXECUTOR_EXCEPTION_HANDLER`; `AssertUnwindSafe` is the honest
        // annotation, for `Handle::Static`'s reason. Taken out of the slot
        // rather than called through it, because an async body is a `FnOnce`.
        // `None` would mean the body had already been taken, which no async
        // node can reach: one submission makes one job, and only a job can
        // take the body.
        AsyncWork::Plain(body) => {
            if let Some(body) = body.take()
                && let Err(payload) = panic::catch_unwind(AssertUnwindSafe(body))
            {
                // Upstream's `TF_EXECUTOR_EXCEPTION_HANDLER` calling
                // `_process_exception`, which is the walk and nothing special
                // for an async node: where the payload lands is whatever the
                // chain above this job roots at, and `DESIGN.md` has the four
                // destinations and why the sentinel is not one of them.
                job.record_panic(payload);
            }
            Some(Release::Strong)
        }
        // Upstream's cases 1 and 2, `void(Runtime&)` and `void(Runtime&,
        // bool)`, which both go to its `_invoke_runtime_task_impl`: one line
        // out to the module that owns everything handed a `Runtime`.
        AsyncWork::Runtime(_) | AsyncWork::Reentrant { .. } => {
            runtime::invoke_async(worker, job, work)
        }
    };

    // Before `finish_async` below, not after: that call can wake an
    // `Executor::wait_for_all` or an `Executor::drop` waiting on this being
    // the last free task, on a thread with nothing else ordering it against
    // this one's own observer call -- #167's `tests/observers.rs` caught
    // this the other way round. Covers both outcomes of the match above:
    // `None` (a runtime-shaped body that preempted its own node, the second
    // entry still to come) gets exactly the one call `WorkerThread::invoke_one`
    // would have, and `Some` (finished) gets it before the signal.
    worker.observer_epilogue(job);

    // Upstream's parentless branch of `_tear_down_async`:
    // `if(node->_parent == nullptr) _decrement_topology();`, under the
    // condition `Shared::new_free_node` increments under -- the two are one
    // claim. The job itself is spent by `WorkerThread::invoke` immediately
    // after this returns, which gives up the node and the body with it --
    // to this worker's free list if nothing else still names the node, and
    // to the allocator otherwise; that it happens *after* these decrements
    // is what makes an `Executor::drop` released by them safe, the thread it
    // releases going on to join every worker, free list and all.
    if release.is_some() && matches!(job.parent(), Parent::Run) {
        worker.shared.finish_async();
    }

    release
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::executor::queue::WorkerQueue;
    use crate::executor::topology::Nested;
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

    /// The two counters a free async task's submission and tear-down have to
    /// balance: `(num_asyncs, num_topologies)`.
    fn counters(executor: &Executor) -> (usize, usize) {
        (
            executor.shared.num_asyncs.load(Ordering::Acquire),
            executor.num_topologies(),
        )
    }

    /// Spends whatever the counters are still carrying, so that a case which
    /// found them unbalanced fails instead of hanging.
    ///
    /// [`Executor`]'s own `Drop` waits for `num_asyncs` to reach zero, so a
    /// leaked submission would park the test thread for ever -- in the
    /// unwinding of the very assertion that caught it. One
    /// [`Shared::finish_async`] spends one of each counter, which is what one
    /// leaked submission costs, so the loop is over `num_asyncs`.
    fn drain_counters(executor: &Executor) {
        for _ in 0..executor.shared.num_asyncs.load(Ordering::Acquire) {
            executor.shared.finish_async();
        }
    }

    /// Takes the one job a submission pushed back out of the shared buffers,
    /// for a case that runs it by hand.
    ///
    /// The pool cannot run a *parented* submission: a worker skips the steal
    /// loop entirely while `num_topologies` is zero, which is what a parented
    /// submission correctly leaves it at. A real parented child is submitted
    /// from inside a running task, whose own run is counted -- #92 is where
    /// that caller arrives -- so driving the job from the test thread is what
    /// stands in for it here.
    fn take_submitted_job(executor: &Executor) -> Job {
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

    /// A `silent_async` that panics has nowhere to report its failure to, and
    /// the thing that matters is what it must *not* reach: the sentinel run
    /// every free node shares. A failure recorded there would set the
    /// `EXCEPTION` bit that `Job::is_parent_cancelled` reads, and every async
    /// task submitted after it -- for the life of the process -- would be
    /// skipped unrun.
    ///
    /// #88's case, re-expressed for the truth #105 moved it to: the payload
    /// is no longer dropped on the floor by this module but walked by
    /// `Job::record_panic` like any other task's, which parks it on the node
    /// the task is -- gone with the node's last share, since a `silent_async`
    /// hands out no handle to read it from. What the sentinel holds
    /// afterwards is the half the walk had to get right, so it is asserted
    /// here rather than inferred from the tasks that follow.
    #[test]
    fn a_silent_async_that_panics_leaves_the_executor_usable() {
        let executor = Executor::new(2);
        let counter = Arc::new(AtomicUsize::new(0));

        executor.silent_async(|| panic!("parked on the node"));
        executor.wait_for_all();

        assert!(
            !Topology::free_standing().cancelled(),
            "the run every free node shares took no part in that failure"
        );

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

    /// Issue #105's regression, at the level a caller meets it: one failure
    /// *recorded* on a parentless free node, and then an ordinary
    /// `silent_async` that has to run to completion.
    ///
    /// The failure goes through [`Job::record_panic`] rather than through a
    /// panicking body, because that is the walk the bug was in and because
    /// #90, #91 and #93 all reach it with a node this test can hold. A
    /// payload claimed on the sentinel run would set `EXCEPTION` on the one
    /// topology every free node of the process shares, and the second task
    /// below would be torn down unrun by the early-out at the top of
    /// `WorkerThread::invoke` -- which never reaches
    /// [`Shared::finish_async`], so `num_asyncs` would never fall to zero
    /// and neither the `wait_for_all` below nor [`Executor`]'s own `Drop`
    /// would ever return. That is why the slot is asserted on first and the
    /// spin has a deadline: everything downstream of a poisoned sentinel
    /// hangs rather than fails.
    #[test]
    fn a_recorded_failure_on_a_free_node_leaves_later_async_tasks_runnable() {
        let executor = Executor::new(2);
        let failed = AsyncNode::new("failed", Handle::Async(AsyncWork::plain(|| {})));

        Job::free(&failed, &Parent::Run).record_panic(Box::new("boom"));
        assert!(
            failed.take_panic().is_some(),
            "the payload belongs to the node that failed"
        );

        let ran = Arc::new(AtomicBool::new(false));
        executor.silent_async({
            let ran = Arc::clone(&ran);
            move || ran.store(true, Ordering::Release)
        });

        spin_until("the async task submitted after a free node's failure", &ran);
        executor.wait_for_all();
        assert_eq!(
            executor.num_topologies(),
            0,
            "and the task that ran was accounted for"
        );
    }

    /// A free node submitted **under a parent** is accounted for by that
    /// parent and by nothing else: the executor's two counters are untouched
    /// by the submission and still zero after the tear-down, which is what
    /// lets [`Executor::wait_for_all`] and [`Executor`]'s own `Drop` return
    /// at all.
    ///
    /// Issue #115's first half. The submission used to increment both
    /// counters whatever the parent was, while the tear-down decrements them
    /// only for a [`Parent::Run`], so one parented child leaked one of each
    /// -- upstream increments in its public `async`/`silent_async` and not in
    /// the `_async(..., tpg, parent)` its runtime and task-group forms call,
    /// which is exactly this condition.
    ///
    /// **How a regression surfaces.** As a failed assertion rather than a
    /// hang, which takes some care: a leak is a `Drop` that never returns, so
    /// the counters are read into locals and then *spent* before anything is
    /// asserted. Reading them rather than calling `wait_for_all` is the same
    /// precaution one line earlier.
    #[test]
    fn a_parented_async_task_is_counted_by_its_parent_and_not_by_the_executor() {
        let executor = Executor::new(1);
        let mut worker = WorkerThread::new(Arc::clone(&executor.shared), 0, WorkerQueue::new());

        // An anchor with no node to resume -- a corun's shape, and a task
        // group's -- which is the parent a child is counted against.
        let parent = Nested::new(None);
        let ran = Arc::new(AtomicBool::new(false));

        executor.shared.schedule_async_task(
            Spawn::silent({
                let ran = Arc::clone(&ran);
                move || ran.store(true, Ordering::Release)
            }),
            &Submission::under_anchor(&parent),
        );
        let submitted = counters(&executor);
        assert_eq!(
            parent.outstanding(),
            1,
            "the child is outstanding against its parent"
        );

        worker.invoke(take_submitted_job(&executor));
        let torn_down = counters(&executor);

        drain_counters(&executor);

        assert_eq!(
            submitted,
            (0, 0),
            "a parented submission counts nothing in the executor's own counters"
        );
        assert_eq!(torn_down, (0, 0), "and its tear-down spends nothing");
        assert!(ran.load(Ordering::Acquire), "the body ran");
        assert_eq!(
            parent.outstanding(),
            0,
            "the parent's counter is where the child was counted, and the \
             tear-down is what spent it"
        );
    }

    /// A name given to a submission reaches the node the task is, and a
    /// submission given none leaves that node's name empty: upstream's
    /// `TaskParams{name}` against the `DefaultTaskParams{}` its unnamed
    /// overloads pass.
    ///
    /// Read at the core rather than through [`Executor::spawn_named`],
    /// because one `Spawn::named` step is the whole of what a `_named` form
    /// adds to its twin and this is where that step lands; `tests/asyncs.rs`
    /// reads the same name back through an observer, which is what a caller
    /// has to read it with. Both submissions go under an anchor so that the
    /// job can be taken and inspected before it runs, for
    /// [`take_submitted_job`]'s reason.
    #[test]
    fn a_submission_names_its_node_and_an_unnamed_one_leaves_it_empty() {
        let executor = Executor::new(1);
        let mut worker = WorkerThread::new(Arc::clone(&executor.shared), 0, WorkerQueue::new());
        let parent = Nested::new(None);

        executor.shared.schedule_async_task(
            Spawn::silent(|| {}).named("my task"),
            &Submission::under_anchor(&parent),
        );
        let job = take_submitted_job(&executor);
        assert_eq!(job.parts().0.name(), "my task");
        worker.invoke(job);

        executor
            .shared
            .schedule_async_task(Spawn::silent(|| {}), &Submission::under_anchor(&parent));
        let job = take_submitted_job(&executor);
        assert_eq!(
            job.parts().0.name(),
            "",
            "an unnamed submission is upstream's `DefaultTaskParams`"
        );
        worker.invoke(job);

        assert_eq!(parent.outstanding(), 0, "both children were accounted for");
    }
}

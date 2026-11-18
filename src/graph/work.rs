//! Translated from `taskflow/core/graph.hpp` (Taskflow v4.1.0).
//!
//! The callable each handle of a node holds: the `work` member of upstream's
//! `Node::Static`, `Node::Condition`, `Node::MultiCondition`, `Node::Subflow`,
//! `Node::Runtime`, `Node::NonpreemptiveRuntime`, `Node::Async` and
//! `Node::DependentAsync`.
//!
//! # Design
//!
//! The work handles lock their callable for two different reasons. [`Work`],
//! [`SubflowWork`] and [`RuntimeWork`] hold a `Mutex` because a closure needs
//! `&mut` to be called and a worker reaches its node as `&Node`; that lock is
//! an *assertion* on the scheduler's one-worker-per-node promise, taken with
//! `try_lock` so that a contended one panics rather than waits, and trading
//! it for an `UnsafeCell` was left to the benchmarks and not taken.
//! [`OnceWork`] locks to *arbitrate* instead: a free async node is reachable
//! through an [`AsyncTask`](crate::AsyncTask) handle a caller holds, so two
//! callers can race to take the one body and exactly one is given it.
//!
//! **One [`Work`] covers three of upstream's node kinds.** Upstream stores a
//! different `std::function` per kind, and the return type is all that
//! differs between three of them -- `void()` for a static task, `int()` for
//! a condition, `SmallVector<int>()` for a multi-condition -- so one type
//! parameter covers all three.
//!
//! **An async body is moved out of its node to be run.** Upstream's async
//! bodies are a `std::packaged_task` or a `mutable` lambda that is moved
//! into the node and called once, where a static task's `std::function` is
//! called afresh on every run of its graph; a `FnOnce` says that, and cannot
//! be called through a shared reference, so [`OnceWork::take`] hands the
//! body out instead. Upstream needs no equivalent, its async node being
//! recycled by the worker that ran it; here the node lives as long as the
//! last handle on it, and the taking is what keeps a body from running
//! twice.

use core::fmt;
use std::any::Any;
use std::sync::{Mutex, MutexGuard, PoisonError, TryLockError};

use crate::executor::runtime::NonpreemptiveRuntime;
use crate::runtime::Runtime;
use crate::subflow::Subflow;

/// The callable behind a task, returning `R`, which defaults to `()` so that
/// `Work` on its own means the static case: upstream's per-node-kind
/// `std::function`, three of which this one type covers -- the module's
/// `# Design` section says which three.
///
/// The `Mutex` is what lets a `&Node` shared across workers invoke the work
/// at all, where upstream calls the function through a `Node*`; the module
/// docs say why it is an assertion rather than a synchronisation primitive.
pub struct Work<R = ()>(pub(super) Mutex<Box<dyn FnMut() -> R + Send + 'static>>);

impl<R> Work<R> {
    /// Wraps a callable as the work of a task.
    pub fn new<C>(callable: C) -> Self
    where
        C: FnMut() -> R + Send + 'static,
    {
        Self(Mutex::new(Box::new(callable)))
    }

    /// Runs the callable and hands back what it returned, which for a
    /// conditioner is the successors it selects.
    ///
    /// # Panics
    ///
    /// Panics if the work is already running on another thread, which means
    /// the scheduler's one-worker-per-node invariant was violated.
    pub fn invoke(&self) -> R {
        lock_callable(&self.0)()
    }
}

/// Takes the lock the callable of a work handle sits behind: the one
/// discipline [`Work`], [`SubflowWork`] and [`RuntimeWork`] share.
///
/// # Panics
///
/// Panics if the work is already running on another thread. Every dispatch
/// arm a [`Runtime::schedule`](crate::Runtime::schedule) bypass can target
/// takes that node's own dispatch claim before it gets here, and the bypass
/// defers onto a held claim instead of queueing, so a trip here is the
/// scheduler's own one-worker-per-node invariant failing -- a dispatch arm
/// that forgot to claim, not a caller of `schedule`.
fn lock_callable<F: ?Sized>(callable: &Mutex<Box<F>>) -> MutexGuard<'_, Box<F>> {
    match callable.try_lock() {
        Ok(callable) => callable,
        // A task that panics poisons its own work. Handing the closure back
        // out keeps the failure reported as that original panic rather than as
        // a lock error raised by whoever touches the node next; what to do
        // about the panic itself is the executor's call.
        Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
        Err(TryLockError::WouldBlock) => {
            panic!(
                "a node's own callable was locked twice at once: the \
                 scheduler's own dispatch claim should have made this \
                 unreachable"
            )
        }
    }
}

/// The callable behind a subflow task, which is handed the [`Subflow`] it
/// builds into.
///
/// Upstream's `std::function<void(tf::Subflow&)>`. The body builds a graph
/// while it runs, so what it needs is the handle rather than a return value
/// and it cannot be a [`Work`]; the `Mutex` is [`Work`]'s, under the same
/// discipline. The bound is higher-ranked over the subflow's own lifetime
/// because the handle borrows the node the body is running on, which is a
/// lifetime no stored closure can name.
pub struct SubflowWork(Mutex<SubflowBody>);

/// The boxed body of a subflow task, named rather than written inline because
/// the lock, the box and the bound together are past what clippy's
/// `type_complexity` admits in a field.
type SubflowBody = Box<dyn for<'a> FnMut(&mut Subflow<'a>) + Send + 'static>;

impl SubflowWork {
    /// Wraps a callable as the work of a subflow task.
    pub fn new<C>(callable: C) -> Self
    where
        C: for<'a> FnMut(&mut Subflow<'a>) + Send + 'static,
    {
        Self(Mutex::new(Box::new(callable)))
    }

    /// Runs the callable against `subflow`, which is what the body builds its
    /// child graph into.
    ///
    /// # Panics
    ///
    /// Panics if the work is already running on another thread, which means
    /// the scheduler's one-worker-per-node invariant was violated.
    pub fn invoke(&self, subflow: &mut Subflow<'_>) {
        lock_callable(&self.0)(subflow);
    }
}

impl fmt::Debug for SubflowWork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SubflowWork(..)")
    }
}

/// The callable behind a runtime task, which is handed the [`Runtime`] of the
/// node it runs on.
///
/// Upstream's `std::function<void(tf::Runtime&)>`, under [`SubflowWork`]'s
/// discipline. Upstream's second runtime shape,
/// `std::function<void(tf::NonpreemptiveRuntime&)>`, is
/// [`NonpreemptiveRuntimeWork`] rather than a second case of this type: the
/// two run under different protocols -- `DESIGN.md`'s "Decisions taken in
/// M8" has why -- and `preempts_itself` tells them apart by
/// [`Handle`](super::Handle) variant rather than by a flag on a shared one.
pub struct RuntimeWork(Mutex<RuntimeBody>);

/// The boxed body of a runtime task, named for [`SubflowBody`]'s reason.
type RuntimeBody = Box<dyn for<'a> FnMut(&mut Runtime<'a>) + Send + 'static>;

impl RuntimeWork {
    /// Wraps a callable as the work of a runtime task.
    pub fn new<C>(callable: C) -> Self
    where
        C: for<'a> FnMut(&mut Runtime<'a>) + Send + 'static,
    {
        Self(Mutex::new(Box::new(callable)))
    }

    /// Runs the callable against `runtime`, which is the body's handle on the
    /// scheduler.
    ///
    /// # Panics
    ///
    /// Panics if the work is already running on another thread, which means
    /// the scheduler's one-worker-per-node invariant was violated.
    pub fn invoke(&self, runtime: &mut Runtime<'_>) {
        lock_callable(&self.0)(runtime);
    }
}

impl fmt::Debug for RuntimeWork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RuntimeWork(..)")
    }
}

/// The callable behind a nonpreemptive runtime task, which is handed a
/// restricted `NonpreemptiveRuntime` rather than the full [`Runtime`].
///
/// Upstream's `std::function<void(tf::NonpreemptiveRuntime&)>`, under
/// [`SubflowWork`]'s locking discipline. The handle type is not part of this
/// crate's public surface -- see
/// [`Handle::NonpreemptiveRuntime`](super::Handle::NonpreemptiveRuntime) for
/// why a caller still names no type to use one -- so there is nothing here
/// for this doc comment to link to.
pub struct NonpreemptiveRuntimeWork(Mutex<NonpreemptiveRuntimeBody>);

/// The boxed body of a nonpreemptive runtime task, named for [`SubflowBody`]'s
/// reason.
type NonpreemptiveRuntimeBody =
    Box<dyn for<'a> FnMut(&mut NonpreemptiveRuntime<'a>) + Send + 'static>;

impl NonpreemptiveRuntimeWork {
    /// Wraps a callable as the work of a nonpreemptive runtime task.
    pub(crate) fn new<C>(callable: C) -> Self
    where
        C: for<'a> FnMut(&mut NonpreemptiveRuntime<'a>) + Send + 'static,
    {
        Self(Mutex::new(Box::new(callable)))
    }

    /// Runs the callable against `line`, the body's restricted handle on the
    /// scheduler.
    ///
    /// # Panics
    ///
    /// Panics if the work is already running on another thread, which means
    /// the scheduler's one-worker-per-node invariant was violated.
    pub(crate) fn invoke(&self, line: &mut NonpreemptiveRuntime<'_>) {
        lock_callable(&self.0)(line);
    }
}

impl fmt::Debug for NonpreemptiveRuntimeWork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NonpreemptiveRuntimeWork(..)")
    }
}

impl<R> fmt::Debug for Work<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Work(..)")
    }
}

/// A body that runs **once**, in the slot the one worker that runs it takes
/// it out of.
///
/// The three [`AsyncWork`] shapes are built out of this; the module's
/// `# Design` section has why the body is moved out of the node to be run,
/// and why this lock arbitrates where [`Work`]'s asserts.
pub struct OnceWork<F: ?Sized>(Mutex<Option<Box<F>>>);

impl<F: ?Sized> OnceWork<F> {
    /// Fills the slot with `body`, which is the one call of it there will be.
    /// Takes the box rather than the callable, `F` being a `dyn FnOnce` of
    /// one of three shapes whose coercion belongs at the call sites in
    /// [`AsyncWork`].
    #[must_use]
    pub fn new(body: Box<F>) -> Self {
        Self(Mutex::new(Some(body)))
    }

    /// Takes the body out, leaving the slot empty: `Some` for the one caller
    /// that got there first, `None` for every caller after it.
    pub fn take(&self) -> Option<Box<F>> {
        self.lock().take()
    }

    /// Whether the body has been taken, the taker possibly still running it.
    #[must_use]
    pub fn is_spent(&self) -> bool {
        self.lock().is_none()
    }

    /// The slot. Poisoning is taken rather than propagated, as it is
    /// everywhere else in the crate: nothing under this lock does more than
    /// move a box, so a poisoned slot means a panic in the body after it was
    /// taken, which is already on its way to being reported.
    fn lock(&self) -> MutexGuard<'_, Option<Box<F>>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<F: ?Sized> fmt::Debug for OnceWork<F> {
    /// Whether the body is still there is the whole of what is printable.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The lock rather than a `try_lock`, where a `NodeBase`'s `Debug`
        // declines to wait: nothing holds this one across anything but an
        // `Option::take`.
        f.debug_tuple("OnceWork")
            .field(&if self.is_spent() { "spent" } else { "pending" })
            .finish()
    }
}

/// The body of an async or dependent-async task, in one of upstream's three
/// shapes.
///
/// Upstream's `Node::Async::work` and `Node::DependentAsync::work`, which are
/// the same `std::variant<std::function<void()>,
/// std::function<void(tf::Runtime&)>, std::function<void(tf::Runtime&, bool)>>`
/// twice over -- so this is one type used by both
/// [`Handle::Async`](super::Handle::Async) and
/// [`Handle::DependentAsync`](super::Handle::DependentAsync), as the two
/// upstream structs are one layout under two names.
///
/// Not generic over a return value, where upstream's third `async` form is a
/// `std::packaged_task<R()>`: a value-returning body is submitted as a
/// `Plain` wrapper closure that captures the slot it will put the value in,
/// which is what lets one queue hold jobs of async tasks that disagree about
/// `R`.
#[derive(Debug)]
pub enum AsyncWork {
    /// A plain callable, upstream's `std::function<void()>` alternative: the
    /// body of a `silent_async`, and the wrapper a value-returning `async`
    /// submits.
    Plain(OnceWork<PlainAsyncBody>),
    /// A callable handed the scheduler, upstream's
    /// `std::function<void(tf::Runtime&)>` alternative: a *silent*
    /// runtime-shaped body, which nobody is waiting on and which therefore
    /// needs no second entry.
    Runtime(OnceWork<AsyncRuntimeBody>),
    /// A runtime-shaped body somebody *is* waiting on, upstream's
    /// `std::function<void(tf::Runtime&, bool reentered)>` alternative.
    ///
    /// Two closures rather than one closure and a flag. Upstream's lambda is
    /// called twice: once with `reentered == false`, when it runs the user's
    /// body, and once with `true`, when its only job is to deliver the node's
    /// payload to whoever holds the handle (`_async` in
    /// `taskflow/core/async.hpp`, which reads `rt._node->_exception_ptr` on
    /// that second entry). Each half runs exactly once, and two `FnOnce`s say
    /// so where one `FnMut` and a `bool` leave it to be taken on trust.
    Reentrant {
        /// The user's body, run on the first entry.
        body: OnceWork<AsyncRuntimeBody>,
        /// What delivers the node's one failure, if there was one, on the
        /// second entry: upstream's `eptr ? set_exception : set_value`.
        complete: OnceWork<AsyncCompleteBody>,
    },
}

/// The boxed body of a plain async task, named rather than written inline for
/// [`SubflowBody`]'s reason.
type PlainAsyncBody = dyn FnOnce() + Send + 'static;

/// The boxed body of a runtime-shaped async task.
///
/// Higher-ranked over the runtime's own lifetime, as [`RuntimeBody`] is and
/// for its reason: the handle borrows the node the body is running on.
type AsyncRuntimeBody = dyn for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static;

/// The boxed second half of an [`AsyncWork::Reentrant`] body: what hands the
/// node's payload -- `None` for a task that did not fail -- to whoever is
/// waiting on the task.
type AsyncCompleteBody = dyn FnOnce(Option<Box<dyn Any + Send>>) + Send + 'static;

impl AsyncWork {
    /// Wraps a plain callable as the body of an async task: upstream's
    /// `std::function<void()>` alternative.
    pub fn plain<C>(body: C) -> Self
    where
        C: FnOnce() + Send + 'static,
    {
        Self::Plain(OnceWork::new(Box::new(body)))
    }

    /// Wraps a runtime-shaped callable nobody is waiting on: upstream's
    /// `std::function<void(tf::Runtime&)>` alternative.
    pub fn runtime<C>(body: C) -> Self
    where
        C: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static,
    {
        Self::Runtime(OnceWork::new(Box::new(body)))
    }

    /// Wraps a runtime-shaped callable somebody is waiting on, plus what
    /// delivers the node's payload to them: upstream's
    /// `std::function<void(tf::Runtime&, bool)>` alternative, as two
    /// `FnOnce`s.
    pub fn reentrant<B, C>(body: B, complete: C) -> Self
    where
        B: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static,
        C: FnOnce(Option<Box<dyn Any + Send>>) + Send + 'static,
    {
        Self::Reentrant {
            body: OnceWork::new(Box::new(body)),
            complete: OnceWork::new(Box::new(complete)),
        }
    }
}

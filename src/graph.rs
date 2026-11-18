//! Translated from `taskflow/core/graph.hpp` (Taskflow v4.1.0).
//!
//! [`Graph`] owns its nodes by value and a [`Task`] is an index into that
//! arena. Upstream's `NodeBase`/`Node` split is kept: the base holds the
//! state words a run writes, so that a node whose nested graph is running and
//! the anchor a `corun` waits on are one kind of thing to the scheduler.
//!
//! # Design
//!
//! The arena and the index handle are the crate-wide decision `DESIGN.md`
//! records as "Graph as an arena", which is also why upstream's object pool
//! (`TF_ENABLE_TASK_POOL`) is not ported. Two members of upstream's
//! `NodeBase` are placed by decisions of their own, both in `DESIGN.md`:
//! `_parent` is not stored on a node but travels with the *job* that names
//! one, which is what keeps the scheduler free of raw pointers, and
//! `_exception_ptr` is on the node because an anchor is a `NodeBase`.
//!
//! [`Graph::erase`] **tombstones** a slot where upstream's `Graph::_erase`
//! compacts its `_nodes` vector. Upstream can compact because a `tf::Task`
//! holds a `Node*` that compaction does not move; a shuffle here would
//! silently re-point live [`Task`]s at other nodes. The price of an index
//! that stays stable is a slot that is never reused.
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

use core::fmt;
use core::num::TryFromIntError;
use std::any::Any;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, TryLockError};

use crate::executor::runtime::NonpreemptiveRuntime;
use crate::executor::topology::{Cancellable, Job};
use crate::runtime::Runtime;
use crate::semaphore::{Semaphore, SemaphoreError};
use crate::subflow::Subflow;
use crate::taskflow::Taskflow;

/// A handle to one node of a [`Graph`], as an index into that graph's arena.
///
/// A `Task` carries no reference to the graph it indexes, so it is `Copy`,
/// but it is only meaningful together with the `Graph` that produced it:
/// using one against a different graph addresses an unrelated node, exactly
/// as a stale `Node*` would upstream.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Task(u32);

impl Task {
    /// Wraps a raw arena index.
    #[must_use]
    pub const fn new(index: u32) -> Self {
        Self(index)
    }

    /// Returns the raw arena index.
    #[must_use]
    pub const fn id(self) -> u32 {
        self.0
    }

    /// Returns the arena index as a `usize`, for indexing a node vector.
    ///
    /// # Panics
    ///
    /// Panics on a target whose `usize` is narrower than 32 bits, when the
    /// index does not fit. No such target can hold a graph that large anyway.
    #[must_use]
    pub fn index(self) -> usize {
        usize::try_from(self.0).expect("task index does not fit in a usize on this target")
    }
}

impl TryFrom<usize> for Task {
    type Error = TryFromIntError;

    fn try_from(index: usize) -> Result<Self, Self::Error> {
        Ok(Self(u32::try_from(index)?))
    }
}

impl From<Task> for u32 {
    fn from(task: Task) -> Self {
        task.0
    }
}

/// The callable behind a task, returning `R`.
///
/// Upstream stores a different `std::function` per node kind, and the return
/// type is all that differs between three of them -- `void()` for a static
/// task, `int()` for a condition, `SmallVector<int>()` for a multi-condition
/// -- so one type parameter covers all three, defaulting to `()` so that
/// `Work` on its own means the static case.
///
/// The `Mutex` is what lets a `&Node` shared across workers invoke the work
/// at all, where upstream calls the function through a `Node*`; the module
/// docs say why it is an assertion rather than a synchronisation primitive.
pub struct Work<R = ()>(Mutex<Box<dyn FnMut() -> R + Send + 'static>>);

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
/// Panics if the work is already running on another thread. Before the
/// per-node dispatch claim closed `Runtime::schedule`'s hole, this could be
/// tripped from outside the scheduler: a
/// [`Runtime::schedule`](crate::Runtime::schedule) bypass had no way to see
/// that a non-preempting target -- or a preempting one still building what
/// would make it preempt -- was genuinely running, and could queue a second
/// job onto a callable the first had not finished with. Every dispatch arm
/// that can be such a target now takes this node's own [`DispatchClaim`]
/// before it gets here, and `schedule`'s bypass defers onto a held claim
/// instead of queueing, so a trip here is the scheduler's own invariant
/// failing -- a dispatch arm that forgot to claim, not a caller of
/// `schedule`.
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
/// M8" has why -- and `preempts_itself` tells them apart by [`Handle`]
/// variant rather than by a flag on a shared one.
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
/// crate's public surface -- see [`Handle::NonpreemptiveRuntime`] for why a
/// caller still names no type to use one -- so there is nothing here for this
/// doc comment to link to.
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
/// The three [`AsyncWork`] shapes are built out of this. Upstream's async
/// bodies are a `std::packaged_task` or a `mutable` lambda that is moved into
/// the node and called once, where a static task's `std::function` is called
/// afresh on every run of its graph; a `FnOnce` says that, and cannot be
/// called through a shared reference, so the body is *moved out* of the node
/// to be run -- which is what [`OnceWork::take`] is. The module docs say why
/// this lock arbitrates where [`Work`]'s asserts.
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
    ///
    /// Upstream needs no equivalent, its async node being recycled by the
    /// worker that ran it. Here the node lives as long as the last handle on
    /// it, and this is what keeps a body from being run twice.
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
/// twice over -- so this is one type used by both [`Handle::Async`] and
/// [`Handle::DependentAsync`], as the two upstream structs are one layout
/// under two names.
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

/// What a node does when the executor reaches it.
///
/// Upstream is a `std::variant` of eleven alternatives (subflow, module,
/// async, ...) and this port now has all eleven: [`Handle::NonpreemptiveRuntime`]
/// is upstream's `NonpreemptiveRuntime`, the second of its two runtime
/// shapes, which arrived separately from [`Handle::Runtime`] rather than at
/// the same time -- `DESIGN.md`'s "Decisions taken in M8" has why a
/// pipeline line forced a question the single `Handle::Runtime` protocol
/// could not answer. The enum is `#[non_exhaustive]` so that a twelfth
/// variant, should upstream ever grow one, does not break callers outside
/// the crate.
#[derive(Debug, Default)]
#[non_exhaustive]
pub enum Handle {
    /// A task with no work attached yet, upstream's `std::monostate`.
    #[default]
    Placeholder,
    /// A plain callable, upstream's `Node::Static`.
    Static(Work),
    /// A callable whose return value picks which one of this node's successors
    /// runs next, upstream's `Node::Condition`.
    ///
    /// The index is an `i32` because upstream's is, and that is load-bearing
    /// rather than incidental: a negative or out-of-range value selects
    /// nothing, which is how a condition task ends a branch. Narrowing it to a
    /// successor position is the scheduler's job, through `usize::try_from`
    /// plus a range check and never an `as` cast.
    Condition(Work<i32>),
    /// A callable whose return value picks any number of this node's
    /// successors to run next, upstream's `Node::MultiCondition`.
    ///
    /// `Vec<i32>` stands in for upstream's inline `SmallVector<int>`, the
    /// allocation per invocation being a known cost not yet worth a
    /// small-vector type of our own.
    MultiCondition(Work<Vec<i32>>),
    /// A callable that builds a graph of its own while it runs, which the
    /// executor then runs before this node's successors: upstream's
    /// `Node::Subflow`.
    ///
    /// `subgraph` is upstream's `Graph subgraph` field, behind a `Mutex`
    /// because the body mutates it through a node the run only *borrows*.
    /// [`crate::subflow`] has what holds the guard and why no body can
    /// deadlock on it.
    Subflow {
        /// The body, which is handed the [`Subflow`] it builds into.
        work: SubflowWork,
        /// The graph the body builds, upstream's `Node::Subflow::subgraph`.
        subgraph: Mutex<Graph>,
    },
    /// A callable handed the scheduler itself, upstream's `Node::Runtime`.
    Runtime(RuntimeWork),
    /// A callable handed a restricted scheduler handle that cannot spawn
    /// nested work, upstream's `Node::NonpreemptiveRuntime`.
    ///
    /// Where [`Handle::Runtime`]'s dispatch arm sets `PREEMPTED` before the
    /// body runs so that a child the body spawns can resume it,
    /// this one never sets the bit at all and always finishes on its first
    /// entry -- there is no protocol to resume, because the handle the body
    /// is given has no way to spawn anything under this node.
    /// `preempts_itself` answers `false` for it accordingly, which is what
    /// makes [`Runtime::schedule`] targeting one safe at any point in its
    /// body's run; `DESIGN.md`'s "Decisions taken in M8" has the failure
    /// that this variant exists to close.
    ///
    /// Built through [`FlowBuilder::emplace_nonpreemptive_runtime`](crate::FlowBuilder::emplace_nonpreemptive_runtime).
    /// The restricted handle itself is not part of this crate's public
    /// surface -- a caller's body closure is handed one without ever
    /// needing to name its type, the same way a closure passed to a generic
    /// `Fn` bound never needs to.
    NonpreemptiveRuntime(NonpreemptiveRuntimeWork),
    /// A task that stands for another whole taskflow, upstream's
    /// `Node::Module`.
    ///
    /// The module *shares* ownership of its target where upstream stores a
    /// bare `Graph&` -- `DESIGN.md` has why. The run needs no more than the
    /// share: the `Arc` hands out `&Graph` through [`Taskflow::graph`], and
    /// running the target writes its nodes only through their atomic state
    /// words.
    Module(Arc<Taskflow>),
    /// A task that stands for a graph it owns, upstream's
    /// `Node::AdoptedModule`: the `adopt(Graph&&)` form of the variant above,
    /// which moves the graph in rather than sharing it.
    AdoptedModule(Graph),
    /// A task submitted outside any graph, upstream's `Node::Async`: the body
    /// a `silent_async` or `async` call hands the pool, which has no
    /// dependencies and no successors.
    ///
    /// The node such a task is lives in a one-node graph of its own rather
    /// than in a taskflow's arena -- `executor::async_node` owns it -- so
    /// this variant is the *handle* of an async task and not a way to build
    /// one: nothing on [`FlowBuilder`](crate::FlowBuilder) produces it. One
    /// put in a taskflow's graph through [`Graph::add_node`] anyway dumps as
    /// a plain node, and the worker that reaches it refuses the run.
    Async(AsyncWork),
    /// A task submitted outside any graph that other such tasks can depend
    /// on, upstream's `Node::DependentAsync`.
    ///
    /// The same body as [`Handle::Async`] over the same free-standing node,
    /// differing in who may wait for it and in what the node keeps besides
    /// its body: a dependent-async node holds the successors upstream keeps
    /// in `_edges` under its `ESTATE::LOCKED` spin bit, and hands out an
    /// [`AsyncTask`](crate::AsyncTask) handle so that a later submission can
    /// name it as a dependency. It is no more a taskflow's citizen than
    /// [`Handle::Async`] is.
    DependentAsync(AsyncWork),
}

impl Handle {
    /// Returns `true` for [`Handle::Placeholder`].
    #[must_use]
    pub fn is_placeholder(&self) -> bool {
        matches!(self, Self::Placeholder)
    }

    /// Returns `true` for [`Handle::Static`].
    #[must_use]
    pub fn is_static(&self) -> bool {
        matches!(self, Self::Static(_))
    }

    /// Returns `true` for [`Handle::Condition`].
    #[must_use]
    pub fn is_condition(&self) -> bool {
        matches!(self, Self::Condition(_))
    }

    /// Returns `true` for [`Handle::MultiCondition`].
    #[must_use]
    pub fn is_multi_condition(&self) -> bool {
        matches!(self, Self::MultiCondition(_))
    }

    /// Returns `true` for [`Handle::Subflow`].
    #[must_use]
    pub fn is_subflow(&self) -> bool {
        matches!(self, Self::Subflow { .. })
    }

    /// Returns `true` for [`Handle::Runtime`].
    #[must_use]
    pub fn is_runtime(&self) -> bool {
        matches!(self, Self::Runtime(_))
    }

    /// Returns `true` for [`Handle::NonpreemptiveRuntime`].
    #[must_use]
    pub fn is_nonpreemptive_runtime(&self) -> bool {
        matches!(self, Self::NonpreemptiveRuntime(_))
    }

    /// Returns `true` for either module handle, [`Handle::Module`] and
    /// [`Handle::AdoptedModule`].
    ///
    /// The two are one task kind outside the executor, as upstream's
    /// `TaskType` has no separate name for the adopted form either: they
    /// differ only in who owns the target graph. Match the handle to tell
    /// them apart.
    #[must_use]
    pub fn is_module(&self) -> bool {
        matches!(self, Self::Module(_) | Self::AdoptedModule(_))
    }

    /// Returns `true` for [`Handle::Async`].
    #[must_use]
    pub fn is_async(&self) -> bool {
        matches!(self, Self::Async(_))
    }

    /// Returns `true` for [`Handle::DependentAsync`].
    ///
    /// Separate from [`Handle::is_async`], where
    /// [`is_module`](Handle::is_module) folds its two variants together: the
    /// two async forms differ in whether anything can depend on them, which
    /// is a question a caller holding an [`AsyncTask`](crate::AsyncTask)
    /// asks. The flat [`TaskType`](crate::TaskType) folds them all the same,
    /// as upstream's does.
    #[must_use]
    pub fn is_dependent_async(&self) -> bool {
        matches!(self, Self::DependentAsync(_))
    }

    /// Returns `true` if this handle picks its own successors at runtime,
    /// either kind of condition task. Upstream's `Node::_is_conditioner`.
    ///
    /// This is the predicate that makes a dependency *weak*: a task preceded
    /// by a conditioner may never run at all, so it cannot be waited for the
    /// way a strong dependency is.
    #[must_use]
    pub fn is_conditioner(&self) -> bool {
        self.is_condition() || self.is_multi_condition()
    }
}

/// The state a run writes on something it can schedule a graph under:
/// upstream's `NodeBase`.
///
/// A [`Node`] embeds one, and so does the anchor a
/// [`corun`](crate::Executor::corun) waits on, because the scheduler counts
/// both down the same way: the jobs of a graph decrement the join counter of
/// whatever they were scheduled under, and the last of them to reach zero
/// ends that nested run. Upstream says the same by inheritance, reaching
/// either through a `NodeBase*`.
///
/// The words are atomic even where upstream's are plain -- `_nstate` is a
/// bare `int` there -- so that a run can hold a `&Graph` across worker
/// threads instead of handing out `&mut` to nodes it does not exclusively
/// own. Upstream's one remaining member is not here: `_parent` travels with
/// the job.
///
/// The payload slot is a field of the node rather than of the nested run,
/// which is `DESIGN.md`'s entry on where a failure inside a nested graph is
/// delivered; the one anchor without a `NodeBase` is the run itself, whose
/// payload slot is the completion slot a `wait` already reads. The `Mutex`
/// the slot sits behind is uncontended by construction: at most one writer
/// arrives, [`NodeBase::claim_panic`] having arbitrated on the estate word
/// before the lock is taken, and the only readers are the frame or the second
/// entry that waited for the nested graph, which run when no job of it is
/// left.
#[derive(Default)]
pub(crate) struct NodeBase {
    /// Upstream's `_nstate`: the strong-dependency count in the low 28 bits
    /// -- [`Node::STRONG_DEPENDENCIES_MASK`] -- and the scheduler's state
    /// bits above it.
    nstate: AtomicU32,
    /// Upstream's `_estate`: the cancellation and exception bits, which for a
    /// parent are what makes the whole chain under it cancelled, plus the
    /// explicit-anchor bit an [`ExplicitAnchor`] guard sets.
    estate: AtomicU32,
    /// Upstream's `_join_counter`: what is still outstanding, be that this
    /// node's own strong dependencies or the jobs of the graph it is the
    /// parent of.
    join_counter: AtomicUsize,
    /// Upstream's `_exception_ptr`: what the first task to fail under this
    /// anchor panicked with, until whoever waited for that graph takes it.
    /// Empty on every node that is not an anchor, which is most of them.
    panic: Mutex<Option<Box<dyn Any + Send>>>,
}

impl NodeBase {
    /// A base with every word clear: no dependencies, no payload, nothing
    /// outstanding.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The scheduler state word, upstream's `_nstate`.
    pub(crate) fn nstate(&self) -> &AtomicU32 {
        &self.nstate
    }

    /// The exception state word, upstream's `_estate`.
    pub(crate) fn estate(&self) -> &AtomicU32 {
        &self.estate
    }

    /// The join counter, upstream's `_join_counter`.
    pub(crate) fn join_counter(&self) -> &AtomicUsize {
        &self.join_counter
    }

    /// Marks this base as the blocking point of a nested graph until the
    /// guard is dropped: upstream's `ExplicitAnchorGuard`.
    ///
    /// Taken by whoever is about to wait for a graph scheduled under this
    /// base -- `corun_graph` in `executor::corun`, upstream's `_corun_graph`
    /// -- and taken **before** the graph is scheduled: a source of that graph
    /// can fail before the call that scheduled it has returned, and a failure
    /// arriving while the bit is clear walks straight past the frame that is
    /// waiting for it.
    pub(crate) fn anchor_explicitly(&self) -> ExplicitAnchor<'_> {
        self.estate
            .fetch_or(Cancellable::EXPLICITLY_ANCHORED, Ordering::Relaxed);
        ExplicitAnchor(self)
    }

    /// Whether some frame is waiting for a graph scheduled under this base,
    /// which is where the panic walk stops.
    pub(crate) fn is_explicitly_anchored(&self) -> bool {
        self.estate.load(Ordering::Relaxed) & Cancellable::EXPLICITLY_ANCHORED != 0
    }

    /// Whether this base is an anchor of last resort: upstream's
    /// `NSTATE::IMPLICITLY_ANCHORED`, which marks a node that preempted
    /// itself and will look at its slot when it is resumed.
    ///
    /// `executor::runtime` is what sets it, taking
    /// `PREEMPTED | IMPLICITLY_ANCHORED` together on a first entry, so it is
    /// on a runtime task and on a free node whose body is handed a `Runtime`.
    /// A `TaskGroup`'s anchor carries it for its whole life instead, rather
    /// than for one nested run. A node that a subflow or a module merely
    /// preempted is no anchor, which is upstream's arrangement too.
    pub(crate) fn is_implicitly_anchored(&self) -> bool {
        self.nstate.load(Ordering::Relaxed) & Node::IMPLICITLY_ANCHORED != 0
    }

    /// Claims the one payload slot of this base, reporting whether this
    /// caller was the first to do so.
    ///
    /// Upstream's `(_estate.fetch_or(EXCEPTION | CAUGHT) & CAUGHT) == 0`: the
    /// atomic is the arbiter, so a later failure under the same anchor is
    /// turned away here rather than at the lock. `EXCEPTION` comes with it
    /// because an anchor with a failure to report is an anchor whose
    /// remaining children are to be skipped, which is the bit cancellation
    /// reads.
    ///
    /// Masking `CAUGHT` alone rather than the pair is upstream's and is
    /// load-bearing on a node: the panic walk sets `EXCEPTION` on every base
    /// it passes, so a base can arrive here already carrying it and still be
    /// owed the payload.
    pub(crate) fn claim_panic(&self) -> bool {
        let flag = Cancellable::EXCEPTION | Cancellable::CAUGHT;
        self.estate.fetch_or(flag, Ordering::Relaxed) & Cancellable::CAUGHT == 0
    }

    /// Puts `payload` in the slot this caller has just claimed.
    ///
    /// **Call it only after [`NodeBase::claim_panic`] has answered `true`**,
    /// which is what makes the write unopposed -- and is load-bearing beyond
    /// that: `set_up_graph` in `executor::topology` reads `CAUGHT` to decide
    /// whether a node's slot is worth locking at all, so a payload put here
    /// without claiming the slot first would be invisible to the clearing a
    /// re-run does and would surface as the *next* run's failure.
    pub(crate) fn keep_panic(&self, payload: Box<dyn Any + Send>) {
        debug_assert!(
            self.estate.load(Ordering::Relaxed) & Cancellable::CAUGHT != 0,
            "a payload was kept in a slot nobody claimed"
        );
        *self.lock_panic() = Some(payload);
    }

    /// Takes the payload out of the slot and un-claims it: upstream's
    /// `NodeBase::_rethrow_exception`, minus the rethrow -- the payload is
    /// handed back so that the caller decides, which gives
    /// [`Executor::corun`](crate::Executor::corun) and its `try_` form one
    /// primitive instead of two. Clearing `EXCEPTION | CAUGHT` is upstream's
    /// too and only happens when there was something to take, so that a graph
    /// scheduled under this anchor next is not born cancelled.
    pub(crate) fn take_panic(&self) -> Option<Box<dyn Any + Send>> {
        let payload = self.lock_panic().take();
        if payload.is_some() {
            let flag = Cancellable::EXCEPTION | Cancellable::CAUGHT;
            self.estate.fetch_and(!flag, Ordering::Relaxed);
        }
        payload
    }

    /// Empties the slot, which is what arming a graph does to a node whose
    /// slot it finds claimed (`set_up_graph` in `executor::topology`): a
    /// payload nobody came for must not be reported by the next run.
    pub(crate) fn clear_panic(&self) {
        drop(self.lock_panic().take());
    }

    /// The payload slot. Poisoning is taken rather than propagated, as the
    /// executor's locks are: nothing under this lock does more than move a
    /// box, so a poisoned slot means a panic elsewhere and refusing to hand
    /// it back would turn a reported failure into a stuck wait.
    fn lock_panic(&self) -> MutexGuard<'_, Option<Box<dyn Any + Send>>> {
        self.panic.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl fmt::Debug for NodeBase {
    /// Hand-written because the payload is a `Box<dyn Any + Send>`, which has
    /// no [`Debug`](fmt::Debug); whether there is one is what gets printed.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeBase")
            .field("nstate", &self.nstate)
            .field("estate", &self.estate)
            .field("join_counter", &self.join_counter)
            .field(
                "panicked",
                &self.panic.try_lock().is_ok_and(|payload| payload.is_some()),
            )
            .finish()
    }
}

/// Holds a `NodeBase` explicitly anchored for as long as it lives:
/// upstream's `ExplicitAnchorGuard`.
///
/// Taken by [`NodeBase::anchor_explicitly`] and released on drop, including
/// on an unwind through the frame that is waiting -- which is why it is a
/// guard rather than two calls, exactly as upstream's is. The bit is in
/// `estate` because that is the word the panic walk reads.
pub(crate) struct ExplicitAnchor<'a>(&'a NodeBase);

impl Drop for ExplicitAnchor<'_> {
    fn drop(&mut self) {
        self.0
            .estate
            .fetch_and(!Cancellable::EXPLICITLY_ANCHORED, Ordering::Relaxed);
    }
}

/// What a node acquires before its work runs and releases after it:
/// upstream's `Node::Semaphores`.
///
/// These are `Arc<Semaphore>` where upstream stores `Semaphore*` -- the
/// decision is `DESIGN.md`'s, on semaphore waiters -- so a node's claim on a
/// semaphore is a share of it, and every node of one constrained set holds a
/// clone of one `Arc`: see [`TaskMut::acquire`](crate::TaskMut::acquire).
///
/// Crate-private, like upstream's nested struct: what a caller edits is the
/// task handle's `acquire`/`release`, and what a caller can ask is how many
/// of each there are.
#[derive(Debug, Default)]
pub(crate) struct Semaphores {
    /// Acquired in this order before the node's work, and given back in
    /// reverse if one of them cannot be had -- see [`Node::acquire_all`].
    pub(crate) to_acquire: Vec<Arc<Semaphore>>,
    /// Released in this order after the node's work.
    pub(crate) to_release: Vec<Arc<Semaphore>>,
}

/// Whether a node's own callable is locked by a dispatch in progress, and
/// whether a [`Runtime::schedule`](crate::Runtime::schedule) bypass asked --
/// while it was -- to run the node again: the dispatch claim every node
/// carries to close a hole `Node::nstate`'s `PREEMPTED` bit cannot, that bit
/// being set only by the four handle kinds `preempts_itself` answers `true`
/// for. A node of any other kind -- [`Handle::Static`], either conditioner,
/// [`Handle::NonpreemptiveRuntime`] -- is never "entered" by that reckoning
/// even while its body is genuinely running, and [`Handle::Subflow`] is not
/// either for as long as its body is still *building* the subgraph that
/// would make it preempt. `schedule`'s own guard, `is_entered`, answers both
/// cases `false`; this claim is what answers the question `is_entered` is
/// not asking -- not "is this node's nested run outstanding" but "is this
/// node's own callable locked right now" -- and `DESIGN.md`'s "Decisions
/// taken in M8" has the caller-side fix that was tried first and could not
/// close this from outside the scheduler.
///
/// One byte per node: an `AtomicU8` beside [`NodeBase`]'s own three words,
/// which [`Semaphores`] and [`Node`]'s own data slot already account for
/// themselves in their own doc comments.
///
/// # Ordering
///
/// [`DispatchClaim::release`]'s successful store and the successful load
/// half of [`DispatchClaim::claim`] or [`DispatchClaim::wake`] are the pair.
/// A caller may call `release` only once its own callable's guard has
/// already dropped -- every caller here does, since a `MutexGuard` drops at
/// the end of the statement that locked it, strictly before the next
/// statement runs, so there is nothing left to publish by the time `release`
/// is reached. `release`'s `AcqRel` store therefore makes that drop, and
/// everything the callable wrote before it, visible to whichever `claim` or
/// `wake` is the first to read the bit clear with `Acquire`; that pairing is
/// what makes the clear bit *reliable* to the thread that locks the same
/// callable next, rather than a read a compiler or a core is otherwise free
/// to reorder ahead of the unlock it is reporting on.
pub(crate) struct DispatchClaim(AtomicU8);

impl DispatchClaim {
    /// Set while a dispatch holds this node's callable locked.
    const CLAIMED: u8 = 0b01;
    /// Set alongside `CLAIMED`: a [`Runtime::schedule`](crate::Runtime::schedule)
    /// bypass asked to run this node again while the current dispatch held
    /// it, for that dispatch's own [`DispatchClaim::release`] to read.
    const PENDING: u8 = 0b10;

    /// A fresh claim: nothing locked, nothing pending.
    fn new() -> Self {
        Self(AtomicU8::new(0))
    }

    /// Takes the claim, before the dispatch arm that calls this locks this
    /// node's callable. Idempotent: a dispatch that was already claimed by
    /// [`DispatchClaim::wake`]'s own first branch finds the bit already set
    /// and changes nothing, which is how that branch and this call come to
    /// share one claim rather than racing for two.
    pub(crate) fn claim(&self) {
        self.0.fetch_or(Self::CLAIMED, Ordering::AcqRel);
    }

    /// Releases the claim, once the caller's own callable's guard has
    /// dropped (the type's own `# Ordering` section), and reports whether a
    /// `schedule` bypass asked to run this node again while it was held --
    /// which the caller must then do itself, by queueing the node afresh:
    /// nothing else is watching for that flag.
    ///
    /// Leaves the claim held with pending cleared if it reports `true`: the
    /// caller's own requeue is what the next dispatch's
    /// [`DispatchClaim::claim`] will find already set, the same way
    /// [`DispatchClaim::wake`]'s first branch leaves it. Leaves the claim
    /// fully idle if it reports `false`.
    pub(crate) fn release(&self) -> bool {
        let previous = self.0.update(Ordering::AcqRel, Ordering::Acquire, |state| {
            if state & Self::PENDING == 0 {
                0
            } else {
                Self::CLAIMED
            }
        });
        previous & Self::PENDING != 0
    }

    /// What [`Runtime::schedule`](crate::Runtime::schedule)'s bypass calls on
    /// its target instead of queueing it outright: claims the node for a
    /// fresh dispatch if nothing holds it, or marks one pending -- rather
    /// than racing a second worker onto a callable the current dispatch has
    /// not finished with -- if something does.
    ///
    /// Reports which: `true` means the caller must queue the node now,
    /// exactly as `schedule` always did before this claim existed; `false`
    /// means the current claim holder's own [`DispatchClaim::release`] will
    /// queue it once it is done.
    ///
    /// Several concurrent calls are safe: only the one whose update is first
    /// to see the bit clear claims it and is told to queue, and every call
    /// after that, for as long as the claim is held, finds `PENDING` already
    /// where it left it and is told to defer -- which is what keeps two
    /// bypasses from ever queueing two jobs for the same dispatch.
    pub(crate) fn wake(&self) -> bool {
        let previous = self.0.update(Ordering::AcqRel, Ordering::Acquire, |state| {
            if state & Self::CLAIMED == 0 {
                Self::CLAIMED
            } else {
                state | Self::PENDING
            }
        });
        previous & Self::CLAIMED == 0
    }
}

impl fmt::Debug for DispatchClaim {
    /// The claim word, read with the same `Relaxed` load `AtomicU32`'s own
    /// [`Debug`](fmt::Debug) would use: a diagnostic, not a synchronisation
    /// point.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("DispatchClaim")
            .field(&self.0.load(Ordering::Relaxed))
            .finish()
    }
}

/// A single task of a [`Graph`]: its name, its work, its edges and the state
/// words a run mutates.
///
/// The state words are a `NodeBase` -- crate-private, so it is named rather
/// than linked here -- which is upstream's arrangement and the reason a parent
/// can be a node or an anchor indifferently.
pub struct Node {
    name: String,
    handle: Handle,
    /// Successors occupy `edges[..num_successors]` and predecessors the rest,
    /// which is upstream's `_edges`/`_num_successors` layout exactly: one
    /// allocation per node's degree, with the order of each half observable.
    num_successors: usize,
    edges: Vec<Task>,
    /// The state words a run writes, upstream's `NodeBase` subobject.
    base: NodeBase,
    /// What this node acquires and releases around its work, `None` until a
    /// caller asks for either: upstream's `unique_ptr<Semaphores>`, boxed for
    /// its reason -- a graph that acquires nothing pays one pointer per node.
    semaphores: Option<Box<Semaphores>>,
    /// The user data slot, `None` until a caller sets one: upstream's
    /// `void* _data {nullptr}`. `Send + Sync` where upstream's pointer is
    /// neither, because a node is reachable from any worker -- see
    /// [`TaskMut::set_data`](crate::TaskMut::set_data) for the rest of what
    /// the type change buys. A node that never sets one pays for the
    /// `Option` alone, there being no second allocation behind a `None`.
    data: Option<Box<dyn Any + Send + Sync>>,
    /// Whether this node's own callable is locked by a dispatch in progress,
    /// and whether a [`Runtime::schedule`](crate::Runtime::schedule) bypass
    /// asked to run it again while it was: see [`DispatchClaim`]'s own docs
    /// for the hole this closes and the ordering argument behind it.
    dispatch_claim: DispatchClaim,
}

impl Node {
    /// Creates a node outside any graph. Private because the edge lists are
    /// arena indices, which are meaningless until [`Graph`] owns the node.
    fn new(name: String, handle: Handle) -> Self {
        Self {
            name,
            handle,
            num_successors: 0,
            edges: Vec::new(),
            base: NodeBase::new(),
            semaphores: None,
            data: None,
            dispatch_claim: DispatchClaim::new(),
        }
    }

    /// Returns the name of this node, empty if it was never named.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Renames this node.
    pub fn set_name(&mut self, name: impl Into<String>) {
        self.name = name.into();
    }

    /// Returns the work this node performs.
    #[must_use]
    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    /// Returns the work this node performs, mutably.
    pub fn handle_mut(&mut self) -> &mut Handle {
        &mut self.handle
    }

    /// Replaces the work this node performs, dropping the previous handle.
    pub fn set_handle(&mut self, handle: Handle) {
        self.handle = handle;
    }

    /// Returns the number of outgoing edges, counting duplicates.
    #[must_use]
    pub fn num_successors(&self) -> usize {
        self.num_successors
    }

    /// Returns the number of incoming edges, counting duplicates.
    #[must_use]
    pub fn num_predecessors(&self) -> usize {
        self.edges.len() - self.num_successors
    }

    /// Returns the successor half of the edge list, in insertion order modulo
    /// the swap [`Graph::add_edge`] performs.
    #[must_use]
    pub fn successors(&self) -> &[Task] {
        &self.edges[..self.num_successors]
    }

    /// Returns the predecessor half of the edge list, in insertion order.
    #[must_use]
    pub fn predecessors(&self) -> &[Task] {
        &self.edges[self.num_successors..]
    }

    /// Upstream's `NSTATE::RETAIN_SUBFLOW`: set on a subflow node whose body
    /// asked, through [`Subflow::set_retain`], for its subgraph to survive
    /// the join instead of being cleared.
    pub const RETAIN_SUBFLOW: u32 = 0x4000_0000;

    /// Upstream's `NSTATE::JOINED_SUBFLOW`: set on a subflow node whose
    /// subgraph has been joined, which is what makes [`Subflow::joinable`]
    /// answer `false`.
    pub const JOINED_SUBFLOW: u32 = 0x8000_0000;

    /// Upstream's `NSTATE::PREEMPTED`: set on a node whose nested graph is
    /// running, and cleared when the scheduler comes back to it.
    ///
    /// A node carrying this bit has been *entered* and not finished: the
    /// executor scheduled the graph the node is the parent of and left the
    /// node itself to be resumed by the last of that graph's jobs to finish.
    /// See the [`executor`](crate::executor) module docs for the protocol.
    pub const PREEMPTED: u32 = 0x2000_0000;

    /// Upstream's `NSTATE::IMPLICITLY_ANCHORED`: set on a node that preempted
    /// itself and means to look at its own payload slot when it is resumed.
    ///
    /// It is the anchor of *lowest* priority: a failure under this node is
    /// delivered here only if no frame between the failure and the run is
    /// explicitly waiting for a graph -- see the
    /// [`executor`](crate::executor) module docs for the three kinds and the
    /// order they are tried in. Upstream sets it on a runtime task's node for
    /// as long as that body's own children are running, and on a task group's
    /// anchor for the whole life of the group.
    pub const IMPLICITLY_ANCHORED: u32 = 0x1000_0000;

    /// Upstream's `NSTATE::STRONG_DEPENDENCIES_MASK`: which bits of the
    /// `nstate` word hold the strong-dependency count rather than scheduler
    /// state.
    ///
    /// The count lives in the low 28 bits and the state bits above it are the
    /// three constants above, so every read of the count masks and every
    /// write of it is checked against this. Without the mask a node carrying
    /// one state bit would re-arm its join counter with that bit's value --
    /// hundreds of millions -- and never become ready again, which is a hang
    /// rather than a wrong answer.
    pub const STRONG_DEPENDENCIES_MASK: u32 = 0x0FFF_FFFF;

    /// The state words a run writes on this node, upstream's `NodeBase`
    /// subobject: what the scheduler holds when it does not care whether the
    /// thing it counts down is a node or the anchor of a nested run.
    pub(crate) fn base(&self) -> &NodeBase {
        &self.base
    }

    /// Returns the scheduler state word, upstream's `_nstate`: the
    /// strong-dependency count with the four state bits above it, so the
    /// count is read through [`Node::STRONG_DEPENDENCIES_MASK`] and nothing
    /// else.
    #[must_use]
    pub fn nstate(&self) -> &AtomicU32 {
        self.base.nstate()
    }

    /// Returns the exception state word, upstream's `_estate`.
    ///
    /// Three kinds of thing live in it, all the scheduler's: whether the
    /// graph under this node has been cancelled or has failed, whether this
    /// node's one payload slot is taken, and whether something is blocked
    /// waiting for a graph scheduled under it. The bit values are upstream's
    /// `ESTATE` and crate-private; what a caller can do is read the word.
    #[must_use]
    pub fn estate(&self) -> &AtomicU32 {
        self.base.estate()
    }

    /// Returns the join counter a run decrements to decide when this node is
    /// ready, upstream's `_join_counter`.
    ///
    /// It counts two things, as upstream's does: the strong dependencies this
    /// node is still waiting for, and -- while the node is the parent of a
    /// running graph -- the jobs of that graph which have not finished. They
    /// never overlap, a node whose nested graph is running having had its own
    /// dependencies spent already.
    #[must_use]
    pub fn join_counter(&self) -> &AtomicUsize {
        self.base.join_counter()
    }

    /// What this node acquires and releases around its work, `None` if nothing
    /// ever asked it to.
    pub(crate) fn semaphores(&self) -> Option<&Semaphores> {
        self.semaphores.as_deref()
    }

    /// The same, for editing, allocating the lists on first use: upstream's
    /// `if(!_semaphores) _semaphores = make_unique<Semaphores>()`, which it
    /// repeats in each of the four `Task` methods.
    pub(crate) fn semaphores_mut(&mut self) -> &mut Semaphores {
        self.semaphores.get_or_insert_default()
    }

    /// The user data slot, upstream's `_data`, read-only and untyped: the
    /// downcast lives on the task handles that call this, not here.
    pub(crate) fn data(&self) -> Option<&(dyn Any + Send + Sync)> {
        self.data.as_deref()
    }

    /// The same, mutably.
    pub(crate) fn data_mut(&mut self) -> Option<&mut (dyn Any + Send + Sync)> {
        self.data.as_deref_mut()
    }

    /// Replaces the user data slot, dropping whatever was there: upstream's
    /// `_data = data`.
    pub(crate) fn set_data(&mut self, data: Box<dyn Any + Send + Sync>) {
        self.data = Some(data);
    }

    /// Takes the user data slot, leaving it empty.
    pub(crate) fn take_data(&mut self) -> Option<Box<dyn Any + Send + Sync>> {
        self.data.take()
    }

    /// The dispatch claim [`Runtime::schedule`](crate::Runtime::schedule)'s
    /// bypass and this node's own dispatch arm share: see [`DispatchClaim`]'s
    /// docs for what it is for.
    pub(crate) fn dispatch_claim(&self) -> &DispatchClaim {
        &self.dispatch_claim
    }

    /// Acquires every semaphore this node asked for, in order, and answers
    /// whether the node may run: upstream's `Node::_acquire_all`.
    ///
    /// `false` means a clone of `job` is parked on the first semaphore that
    /// could not be had and the caller must not run the node -- whoever
    /// releases that semaphore will schedule the job. The ones already taken
    /// are given back before returning, newest first, which is upstream's
    /// inner loop: a node holding half of what it needs would otherwise
    /// deadlock the rest of its set. Anything those releases wake goes into
    /// `woken`, for the caller to schedule.
    ///
    /// `&self`, because a worker reaches its node through a shared graph: it
    /// is each semaphore's own lock that makes the counter and the waiting
    /// list sound under that sharing, and nothing is written here. The one
    /// caller is `WorkerThread::invoke_one`, on a first entry into a node.
    pub(crate) fn acquire_all(&self, job: &Job, woken: &mut Vec<Job>) -> bool {
        // Upstream asserts `_semaphores != nullptr` (commented out) because
        // only its executor calls this, and only for a node that has them. A
        // node with nothing to acquire trivially may run.
        let Some(semaphores) = self.semaphores() else {
            return true;
        };

        let to_acquire = &semaphores.to_acquire;
        for (taken, semaphore) in to_acquire.iter().enumerate() {
            if !semaphore.try_acquire_or_wait(job) {
                for held in to_acquire[..taken].iter().rev() {
                    // This cannot be an over-release: the count being given
                    // back is one this loop took a moment ago and still
                    // holds. Only a `reset_to` racing a run of this graph
                    // could make it one, which that method documents as not
                    // allowed -- and a rollback has nobody to report it to
                    // anyway.
                    let _ = held.release(woken);
                }
                return false;
            }
        }

        true
    }

    /// Releases every semaphore this node asked to release, in order:
    /// upstream's `Node::_release_all`.
    ///
    /// Whatever those releases wake goes into `woken`, for the caller to
    /// schedule.
    ///
    /// # Errors
    ///
    /// Returns [`SemaphoreError`] from the first semaphore that was already at
    /// its maximum, leaving the ones after it unreleased -- which is where
    /// upstream's throw leaves the loop too. What a task that over-releases
    /// has done is the executor's decision, not this module's. The one caller
    /// is `WorkerThread::invoke_one`, on the entry that finishes the node.
    pub(crate) fn release_all(&self, woken: &mut Vec<Job>) -> Result<(), SemaphoreError> {
        let Some(semaphores) = self.semaphores() else {
            return Ok(());
        };

        for semaphore in &semaphores.to_release {
            semaphore.release(woken)?;
        }

        Ok(())
    }

    /// Appends `to` to the successor half, upstream's `Node::_precede` minus
    /// the predecessor push (the caller owns the other endpoint).
    ///
    /// Upstream's trick, kept verbatim: push to the back, then swap the new
    /// entry with the first predecessor so the halves stay contiguous.
    fn push_successor(&mut self, to: Task) {
        self.edges.push(to);
        let last = self.edges.len() - 1;
        self.edges.swap(self.num_successors, last);
        self.num_successors += 1;
    }

    /// Appends `from` to the predecessor half.
    fn push_predecessor(&mut self, from: Task) {
        self.edges.push(from);
    }

    /// Drops every occurrence of `target` from the successor half, returning
    /// how many went. Upstream's `Node::_remove_successors`.
    fn remove_successors(&mut self, target: Task) -> usize {
        let old_num_successors = self.num_successors;
        let mut kept = 0;
        for read in 0..old_num_successors {
            if self.edges[read] != target {
                self.edges[kept] = self.edges[read];
                kept += 1;
            }
        }
        // Close the gap the compaction left between the halves; the drain
        // shifts the predecessors down, as upstream's std::move does.
        self.edges.drain(kept..old_num_successors);
        self.num_successors = kept;
        old_num_successors - kept
    }

    /// Drops every occurrence of `target` from the predecessor half, returning
    /// how many went. Upstream's `Node::_remove_predecessors`.
    fn remove_predecessors(&mut self, target: Task) -> usize {
        let old_len = self.edges.len();
        let mut kept = self.num_successors;
        for read in self.num_successors..old_len {
            if self.edges[read] != target {
                self.edges[kept] = self.edges[read];
                kept += 1;
            }
        }
        self.edges.truncate(kept);
        old_len - kept
    }
}

impl fmt::Debug for Node {
    /// Hand-written because the data slot is a `Box<dyn Any + Send + Sync>`,
    /// which has no [`Debug`](fmt::Debug); whether one is set is what gets
    /// printed, as `NodeBase`'s panic slot already does.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("name", &self.name)
            .field("handle", &self.handle)
            .field("num_successors", &self.num_successors)
            .field("edges", &self.edges)
            .field("base", &self.base)
            .field("semaphores", &self.semaphores)
            .field("data", &self.data.is_some())
            .field("dispatch_claim", &self.dispatch_claim)
            .finish()
    }
}

/// The arena a task dependency graph lives in.
///
/// Nodes are owned by value and addressed by [`Task`]. Moving one fixes up no
/// owning pointers, where upstream's `tf::Graph` has them, and it is
/// deliberately not `Clone`, the work closures not being.
#[derive(Debug, Default)]
pub struct Graph {
    /// Slots in creation order. A `None` is a tombstone left by
    /// [`Graph::erase`]; it is never refilled, so a [`Task`] cannot come to
    /// address a different node than the one it was handed out for.
    nodes: Vec<Option<Node>>,
    /// How many slots hold a node, so [`Graph::len`] stays O(1).
    num_live: usize,
}

impl Graph {
    /// Creates an empty graph.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty graph whose arena holds `capacity` nodes before it
    /// reallocates.
    ///
    /// Crate-private, because the only caller that can name its final size is
    /// a free async node, whose arena is one node for the life of the node:
    /// a graph a [`FlowBuilder`](crate::FlowBuilder) is filling cannot, and
    /// [`Graph::new`] leaves the `Vec` to pick its own first capacity.
    /// `DESIGN.md`'s entry on the `Arc`-owned free node says what sizing that
    /// one case saves.
    #[must_use]
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(capacity),
            num_live: 0,
        }
    }

    /// Appends an unnamed node and returns its handle: the arena primitive
    /// the `emplace` family of [`FlowBuilder`](crate::FlowBuilder) is built
    /// on.
    ///
    /// # Panics
    ///
    /// Panics if the graph already holds `u32::MAX` nodes, the most a [`Task`]
    /// can address.
    pub fn add_node(&mut self, handle: Handle) -> Task {
        self.add_named_node(String::new(), handle)
    }

    /// Appends a named node and returns its handle.
    ///
    /// # Panics
    ///
    /// Panics if the graph already holds `u32::MAX` nodes.
    pub fn add_named_node(&mut self, name: impl Into<String>, handle: Handle) -> Task {
        let task = task_at(self.nodes.len());
        self.nodes.push(Some(Node::new(name.into(), handle)));
        self.num_live += 1;
        task
    }

    /// Returns `true` if `task` addresses a live node of this graph.
    ///
    /// An erased handle is indistinguishable here from one that was never
    /// valid: both are `false`.
    #[must_use]
    pub fn contains(&self, task: Task) -> bool {
        self.get(task).is_some()
    }

    /// Returns the node `task` addresses, or `None` if it addresses nothing
    /// live in this graph -- out of range, or erased.
    #[must_use]
    pub fn get(&self, task: Task) -> Option<&Node> {
        self.nodes.get(task.index()).and_then(Option::as_ref)
    }

    /// Returns the node `task` addresses mutably, or `None`.
    pub fn get_mut(&mut self, task: Task) -> Option<&mut Node> {
        self.nodes.get_mut(task.index()).and_then(Option::as_mut)
    }

    /// Returns the node `task` addresses.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a live node of this graph, whether
    /// because it is out of range or because it was erased.
    #[must_use]
    pub fn node(&self, task: Task) -> &Node {
        let slots = self.nodes.len();
        let live = self.num_live;
        self.get(task).unwrap_or_else(|| {
            panic!("{task:?} is out of range or erased: {live} live nodes in {slots} slots")
        })
    }

    /// Returns the node `task` addresses, mutably.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a live node of this graph, whether
    /// because it is out of range or because it was erased.
    pub fn node_mut(&mut self, task: Task) -> &mut Node {
        let slots = self.nodes.len();
        let live = self.num_live;
        self.get_mut(task).unwrap_or_else(|| {
            panic!("{task:?} is out of range or erased: {live} live nodes in {slots} slots")
        })
    }

    /// Returns the successors of `task`.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a node of this graph.
    #[must_use]
    pub fn successors(&self, task: Task) -> &[Task] {
        self.node(task).successors()
    }

    /// Returns the predecessors of `task`.
    ///
    /// # Panics
    ///
    /// Panics if `task` does not address a node of this graph.
    #[must_use]
    pub fn predecessors(&self, task: Task) -> &[Task] {
        self.node(task).predecessors()
    }

    /// Adds a `from -> to` dependency, upstream's `Node::_precede`.
    ///
    /// Duplicate edges are kept, as upstream keeps them: a task preceded twice
    /// has a join counter of two.
    ///
    /// # Panics
    ///
    /// Panics if either handle is out of range for this graph.
    pub fn add_edge(&mut self, from: Task, to: Task) {
        // Check both ends before touching either, so a stale handle cannot
        // leave a one-sided edge behind.
        assert!(
            self.contains(from) && self.contains(to),
            "{from:?} -> {to:?} is out of range or erased: {} live nodes in {} slots",
            self.num_live,
            self.nodes.len()
        );
        self.node_mut(from).push_successor(to);
        self.node_mut(to).push_predecessor(from);
    }

    /// Removes every `from -> to` dependency and returns how many were
    /// removed. Upstream's `Taskflow::remove_dependency`.
    ///
    /// # Panics
    ///
    /// Panics if either handle is out of range for this graph.
    pub fn remove_edge(&mut self, from: Task, to: Task) -> usize {
        let removed_successors = self.node_mut(from).remove_successors(to);
        let removed_predecessors = self.node_mut(to).remove_predecessors(from);
        debug_assert_eq!(
            removed_successors, removed_predecessors,
            "edge lists disagree on the multiplicity of {from:?} -> {to:?}"
        );
        removed_successors
    }

    /// Removes the node `task` addresses together with every edge touching it,
    /// and returns `true` if there was one. Upstream's `Graph::_erase`, plus
    /// the neighbour patch-up that upstream leaves to `FlowBuilder::erase`.
    ///
    /// A handle that addresses nothing live -- out of range, or erased
    /// already -- is a no-op returning `false`, matching upstream's "if the
    /// task does not belong to the graph, nothing will happen". The slot
    /// becomes a tombstone and is never reused, so every other [`Task`] keeps
    /// addressing the node it always did; the module docs say why.
    pub fn erase(&mut self, task: Task) -> bool {
        if !self.contains(task) {
            return false;
        }

        // Copy the endpoints out first: patching a neighbour borrows the graph
        // mutably, which the edge lists being read cannot survive. The erased
        // node's own lists need no patching -- they die with it.
        let successors = self.node(task).successors().to_vec();
        let predecessors = self.node(task).predecessors().to_vec();
        for successor in successors {
            self.node_mut(successor).remove_predecessors(task);
        }
        for predecessor in predecessors {
            self.node_mut(predecessor).remove_successors(task);
        }

        self.nodes[task.index()] = None;
        self.num_live -= 1;
        true
    }

    /// Returns the number of live nodes in the graph, not counting the
    /// tombstones [`Graph::erase`] leaves behind. This is the one definition
    /// of "how many tasks", which [`crate::Taskflow`] defers to.
    #[must_use]
    pub fn len(&self) -> usize {
        self.num_live
    }

    /// Returns `true` if the graph holds no live nodes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many slots the arena holds before it reallocates, which is not
    /// how many nodes are in it. Test-only: the one thing a caller of
    /// [`Graph::with_capacity`] can assert about the sizing it asked for.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn capacity(&self) -> usize {
        self.nodes.capacity()
    }

    /// Drops every node and every tombstone, invalidating every [`Task`]
    /// handed out so far. Handles start over from zero afterwards.
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.num_live = 0;
    }

    /// Iterates over the handles of every live node, in creation order.
    pub fn tasks(&self) -> impl Iterator<Item = Task> {
        self.iter().map(|(task, _)| task)
    }

    /// Iterates over every live node together with its handle, in creation
    /// order.
    pub fn iter(&self) -> impl Iterator<Item = (Task, &Node)> {
        self.nodes
            .iter()
            .enumerate()
            .filter_map(|(index, slot)| slot.as_ref().map(|node| (task_at(index), node)))
    }

    /// Iterates mutably over every live node together with its handle.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (Task, &mut Node)> {
        self.nodes
            .iter_mut()
            .enumerate()
            .filter_map(|(index, slot)| slot.as_mut().map(|node| (task_at(index), node)))
    }
}

/// Converts an arena index back into a handle. [`Graph::add_named_node`]
/// refuses to grow past `u32::MAX` nodes, so the conversion can only fail on
/// the push that would have overflowed.
fn task_at(index: usize) -> Task {
    Task::try_from(index).expect("a graph cannot hold more than u32::MAX nodes")
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Task>();
    assert_send_sync::<Work>();
    assert_send_sync::<SubflowWork>();
    assert_send_sync::<RuntimeWork>();
    assert_send_sync::<NonpreemptiveRuntimeWork>();
    assert_send_sync::<Handle>();
    assert_send_sync::<NodeBase>();
    assert_send_sync::<Node>();
    assert_send_sync::<Graph>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    // `Job` is in scope from the module above; these three are the rest of
    // what it takes to build one.
    use crate::executor::topology::{GraphOwner, Parent, Topology};

    /// Builds `n` placeholder nodes named `a`, `b`, ... for edge tests.
    fn graph_of(n: usize) -> (Graph, Vec<Task>) {
        let mut graph = Graph::new();
        let tasks = (0..n)
            .map(|i| {
                let name = char::from(b'a' + u8::try_from(i).unwrap()).to_string();
                graph.add_named_node(name, Handle::Placeholder)
            })
            .collect();
        (graph, tasks)
    }

    #[test]
    fn task_index_round_trips() {
        let task = Task::new(7);
        assert_eq!(task.id(), 7);
        assert_eq!(task.index(), 7);
        assert_eq!(u32::from(task), 7);
        assert_eq!(Task::try_from(7usize), Ok(task));
        assert!(Task::try_from(usize::MAX).is_err());
    }

    #[test]
    fn add_node_hands_out_dense_handles() {
        let (graph, tasks) = graph_of(3);
        assert_eq!(tasks, vec![Task::new(0), Task::new(1), Task::new(2)]);
        assert_eq!(graph.len(), 3);
        assert_eq!(graph.node(tasks[1]).name(), "b");
        assert!(graph.contains(tasks[2]));
        assert!(!graph.contains(Task::new(3)));
        assert!(graph.get(Task::new(3)).is_none());
        assert_eq!(graph.tasks().collect::<Vec<_>>(), tasks);
        let names: Vec<&str> = graph.iter().map(|(_, node)| node.name()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn add_edge_splits_successors_from_predecessors() {
        let (mut graph, t) = graph_of(4);
        graph.add_edge(t[0], t[1]);
        graph.add_edge(t[0], t[2]);
        graph.add_edge(t[0], t[3]);

        assert_eq!(graph.node(t[0]).num_successors(), 3);
        assert_eq!(graph.node(t[0]).num_predecessors(), 0);
        assert_eq!(graph.successors(t[0]), [t[1], t[2], t[3]].as_slice());
        for successor in &t[1..] {
            assert_eq!(graph.node(*successor).num_predecessors(), 1);
            assert_eq!(graph.predecessors(*successor), [t[0]].as_slice());
            assert_eq!(graph.node(*successor).num_successors(), 0);
        }
    }

    #[test]
    fn edge_halves_stay_separate_when_a_node_has_both() {
        let (mut graph, t) = graph_of(4);
        // a -> b, b -> c, b -> d: b holds one predecessor and two successors.
        graph.add_edge(t[0], t[1]);
        graph.add_edge(t[1], t[2]);
        graph.add_edge(t[1], t[3]);

        assert_eq!(graph.successors(t[1]), [t[2], t[3]].as_slice());
        assert_eq!(graph.predecessors(t[1]), [t[0]].as_slice());

        // Removing a successor must shift the predecessor half down without
        // losing it -- the upstream std::move/resize path.
        assert_eq!(graph.remove_edge(t[1], t[2]), 1);
        assert_eq!(graph.successors(t[1]), [t[3]].as_slice());
        assert_eq!(graph.predecessors(t[1]), [t[0]].as_slice());
        assert_eq!(graph.node(t[2]).num_predecessors(), 0);

        // And removing the predecessor leaves the successor half alone.
        assert_eq!(graph.remove_edge(t[0], t[1]), 1);
        assert!(graph.predecessors(t[1]).is_empty());
        assert_eq!(graph.successors(t[1]), [t[3]].as_slice());
        assert_eq!(graph.node(t[0]).num_successors(), 0);
    }

    /// Mirrors upstream's `Taskflow.RemoveDependency` test case, which is the
    /// acceptance oracle for duplicate edges.
    #[test]
    fn remove_edge_drops_every_duplicate() {
        let (mut graph, t) = graph_of(4);
        for _ in 0..2 {
            graph.add_edge(t[0], t[1]);
            graph.add_edge(t[0], t[2]);
            graph.add_edge(t[0], t[3]);
        }
        assert_eq!(graph.node(t[0]).num_successors(), 6);
        assert_eq!(graph.node(t[1]).num_predecessors(), 2);

        assert_eq!(graph.remove_edge(t[0], t[1]), 2);
        assert_eq!(graph.node(t[0]).num_successors(), 4);
        assert_eq!(graph.node(t[1]).num_predecessors(), 0);

        assert_eq!(graph.remove_edge(t[0], t[2]), 2);
        assert_eq!(graph.node(t[0]).num_successors(), 2);
        assert_eq!(graph.node(t[2]).num_predecessors(), 0);

        assert_eq!(graph.remove_edge(t[0], t[3]), 2);
        assert_eq!(graph.node(t[0]).num_successors(), 0);
        assert_eq!(graph.node(t[3]).num_predecessors(), 0);

        // Removing an edge that was never there is a no-op, not a panic.
        assert_eq!(graph.remove_edge(t[0], t[1]), 0);
    }

    #[test]
    fn remove_edge_handles_a_self_loop() {
        let (mut graph, t) = graph_of(1);
        graph.add_edge(t[0], t[0]);
        assert_eq!(graph.node(t[0]).num_successors(), 1);
        assert_eq!(graph.node(t[0]).num_predecessors(), 1);
        assert_eq!(graph.remove_edge(t[0], t[0]), 1);
        assert_eq!(graph.node(t[0]).num_successors(), 0);
        assert_eq!(graph.node(t[0]).num_predecessors(), 0);
    }

    /// The bar issue #10 sets for erase: a middle node goes, its neighbours
    /// lose exactly the edges that touched it, and every other handle still
    /// resolves to the node it always named.
    #[test]
    fn erase_detaches_a_middle_node_and_leaves_the_rest_addressable() {
        let (mut graph, t) = graph_of(4);
        // a -> b -> c -> d, plus a -> c so c has two predecessors and the
        // erased node is not the only route into it.
        graph.add_edge(t[0], t[1]);
        graph.add_edge(t[1], t[2]);
        graph.add_edge(t[2], t[3]);
        graph.add_edge(t[0], t[2]);

        assert!(graph.erase(t[1]));

        // The neighbours lost b and nothing else.
        assert_eq!(graph.node(t[0]).num_successors(), 1);
        assert_eq!(graph.successors(t[0]), [t[2]].as_slice());
        assert_eq!(graph.node(t[2]).num_predecessors(), 1);
        assert_eq!(graph.predecessors(t[2]), [t[0]].as_slice());
        assert_eq!(graph.node(t[2]).num_successors(), 1);
        assert_eq!(graph.node(t[3]).num_predecessors(), 1);

        // Every other handle resolves to the same node as before.
        assert_eq!(graph.node(t[0]).name(), "a");
        assert_eq!(graph.node(t[2]).name(), "c");
        assert_eq!(graph.node(t[3]).name(), "d");

        // The erased handle is now indistinguishable from a stale one.
        assert!(!graph.contains(t[1]));
        assert!(graph.get(t[1]).is_none());
        assert!(graph.get_mut(t[1]).is_none());

        // `len` counts the live nodes, and the iterators skip the tombstone.
        assert_eq!(graph.len(), 3);
        assert_eq!(graph.tasks().collect::<Vec<_>>(), vec![t[0], t[2], t[3]]);
        let names: Vec<&str> = graph.iter().map(|(_, node)| node.name()).collect();
        assert_eq!(names, vec!["a", "c", "d"]);
        for (_, node) in graph.iter_mut() {
            node.set_name("live");
        }
        assert_eq!(graph.iter().count(), 3);

        // The slot is not reused: the next node gets a fresh index.
        assert_eq!(graph.add_node(Handle::Placeholder), Task::new(4));
        assert_eq!(graph.len(), 4);
    }

    #[test]
    fn erase_takes_duplicate_edges_and_a_self_loop_with_it() {
        let (mut graph, t) = graph_of(3);
        graph.add_edge(t[0], t[1]);
        graph.add_edge(t[0], t[1]);
        graph.add_edge(t[1], t[1]);
        graph.add_edge(t[1], t[2]);
        graph.add_edge(t[1], t[2]);

        assert!(graph.erase(t[1]));
        assert_eq!(graph.node(t[0]).num_successors(), 0);
        assert_eq!(graph.node(t[2]).num_predecessors(), 0);
    }

    #[test]
    fn erase_is_a_no_op_for_a_handle_that_names_nothing_live() {
        let (mut graph, t) = graph_of(1);
        assert!(!graph.erase(Task::new(1)));
        assert!(graph.erase(t[0]));
        // Erasing the same handle twice is the "task does not belong to the
        // graph" case upstream documents as doing nothing.
        assert!(!graph.erase(t[0]));
        assert_eq!(graph.len(), 0);
        assert!(graph.is_empty());
    }

    #[test]
    #[should_panic(expected = "out of range or erased")]
    fn add_edge_panics_on_an_erased_handle() {
        let (mut graph, t) = graph_of(2);
        graph.erase(t[0]);
        graph.add_edge(t[0], t[1]);
    }

    #[test]
    fn clear_drops_the_tombstones_too() {
        let (mut graph, t) = graph_of(2);
        graph.erase(t[0]);
        graph.clear();
        assert!(graph.is_empty());
        assert_eq!(graph.add_node(Handle::Placeholder), Task::new(0));
    }

    #[test]
    fn clear_empties_the_arena() {
        let (mut graph, t) = graph_of(2);
        graph.add_edge(t[0], t[1]);
        assert!(!graph.is_empty());
        graph.clear();
        assert!(graph.is_empty());
        assert_eq!(graph.len(), 0);
        assert_eq!(graph.tasks().count(), 0);
        // The arena is reusable afterwards, with handles starting over.
        assert_eq!(graph.add_node(Handle::Placeholder), Task::new(0));
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn node_panics_on_a_stale_handle() {
        let graph = Graph::new();
        let _ = graph.node(Task::new(0));
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn add_edge_panics_on_a_stale_handle() {
        let (mut graph, t) = graph_of(1);
        graph.add_edge(t[0], Task::new(1));
    }

    #[test]
    fn node_name_and_handle_are_mutable() {
        let mut graph = Graph::new();
        let task = graph.add_node(Handle::Placeholder);
        assert_eq!(graph.node(task).name(), "");
        assert!(graph.node(task).handle().is_placeholder());

        graph.node_mut(task).set_name("worker");
        let counter = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&counter);
        graph
            .node_mut(task)
            .set_handle(Handle::Static(Work::new(move || {
                seen.fetch_add(1, Ordering::Relaxed);
            })));

        assert_eq!(graph.node(task).name(), "worker");
        assert!(graph.node(task).handle().is_static());
        if let Handle::Static(work) = graph.node(task).handle() {
            work.invoke();
            work.invoke();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn state_words_are_writable_through_a_shared_node() {
        let (graph, t) = graph_of(1);
        let node = graph.node(t[0]);
        node.nstate().store(3, Ordering::Relaxed);
        node.estate().store(5, Ordering::Relaxed);
        node.join_counter().store(7, Ordering::Relaxed);
        assert_eq!(node.nstate().load(Ordering::Relaxed), 3);
        assert_eq!(node.estate().load(Ordering::Relaxed), 5);
        assert_eq!(node.join_counter().fetch_sub(1, Ordering::Relaxed), 7);
        assert_eq!(node.join_counter().load(Ordering::Relaxed), 6);
    }

    /// The reason the state words are atomic and `Work` holds a `Mutex`: a run
    /// gets `&Graph` and nothing more.
    #[test]
    fn a_shared_graph_can_be_run_from_several_threads() {
        let counter = Arc::new(AtomicUsize::new(0));
        let mut graph = Graph::new();
        for _ in 0..4 {
            let seen = Arc::clone(&counter);
            graph.add_node(Handle::Static(Work::new(move || {
                seen.fetch_add(1, Ordering::Relaxed);
            })));
        }

        let graph = &graph;
        std::thread::scope(|scope| {
            for task in graph.tasks() {
                scope.spawn(move || {
                    let node = graph.node(task);
                    if let Handle::Static(work) = node.handle() {
                        work.invoke();
                    }
                    node.join_counter().fetch_add(1, Ordering::Relaxed);
                });
            }
        });

        assert_eq!(counter.load(Ordering::Relaxed), 4);
        for (_, node) in graph.iter() {
            assert_eq!(node.join_counter().load(Ordering::Relaxed), 1);
        }
    }

    /// A conditioner's work is only useful if the value comes back out, so
    /// this is the bar issue #28 sets for making [`Work`] generic.
    #[test]
    fn invoke_returns_what_the_callable_returned() {
        let mut next = 0;
        let counting = Work::new(move || {
            next += 1;
            next - 1
        });
        assert_eq!(counting.invoke(), 0);
        assert_eq!(counting.invoke(), 1);

        // Upstream's `-10..=110` case is only expressible because the index is
        // signed and unbounded: out of range is the caller's prerogative, not
        // an error the handle rejects.
        let branch_nowhere = Work::new(|| -1);
        assert_eq!(branch_nowhere.invoke(), -1);

        let selection = Work::new(|| vec![-10, 0, 2, 110]);
        assert_eq!(selection.invoke(), vec![-10, 0, 2, 110]);
        // And a second invocation gets its own vector, not a drained one.
        assert_eq!(selection.invoke(), vec![-10, 0, 2, 110]);
    }

    #[test]
    fn the_handle_predicates_split_conditioners_from_the_rest() {
        let placeholder = Handle::Placeholder;
        let still = Handle::Static(Work::new(|| {}));
        let one = Handle::Condition(Work::new(|| 0));
        let many = Handle::MultiCondition(Work::new(Vec::new));

        assert!(placeholder.is_placeholder());
        assert!(still.is_static());
        assert!(one.is_condition());
        assert!(many.is_multi_condition());

        // `is_conditioner` is either of the two and nothing else, which is the
        // whole point: it is what the dependency counts key on.
        assert!(one.is_conditioner());
        assert!(many.is_conditioner());
        assert!(!placeholder.is_conditioner());
        assert!(!still.is_conditioner());

        assert!(!one.is_multi_condition());
        assert!(!many.is_condition());
        assert!(!one.is_static());
        assert!(!one.is_placeholder());
    }

    #[test]
    fn a_node_carries_either_conditioner_handle() {
        let mut graph = Graph::new();
        let one = graph.add_named_node("one", Handle::Condition(Work::new(|| 7)));
        let many = graph.add_named_node("many", Handle::MultiCondition(Work::new(|| vec![1, 3])));

        let Handle::Condition(work) = graph.node(one).handle() else {
            panic!("the condition handle did not survive add_named_node");
        };
        assert_eq!(work.invoke(), 7);
        let Handle::MultiCondition(work) = graph.node(many).handle() else {
            panic!("the multi-condition handle did not survive add_named_node");
        };
        assert_eq!(work.invoke(), vec![1, 3]);

        // And a conditioner replaces a static handle in place, which is what
        // `TaskMut::condition_work` is built on.
        graph
            .node_mut(one)
            .set_handle(Handle::Static(Work::new(|| {})));
        assert!(graph.node(one).handle().is_static());
        assert!(!graph.node(one).handle().is_conditioner());
    }

    /// The four dynamic-tasking handles answer their own predicate and
    /// nobody else's. Both module forms answer [`Handle::is_module`], which is
    /// the one predicate that deliberately covers two variants.
    #[test]
    fn the_handle_predicates_cover_the_dynamic_tasking_kinds() {
        let spawner = Handle::Subflow {
            work: SubflowWork::new(|_| {}),
            subgraph: Mutex::new(Graph::new()),
        };
        let runtime = Handle::Runtime(RuntimeWork::new(|_| {}));
        let module = Handle::Module(Arc::new(Taskflow::new()));
        let adopted = Handle::AdoptedModule(Graph::new());

        assert!(spawner.is_subflow());
        assert!(runtime.is_runtime());
        assert!(module.is_module());
        assert!(adopted.is_module());

        for handle in [&spawner, &runtime, &module, &adopted] {
            // A dynamic task is none of the static kinds, and above all it is
            // not a conditioner: a dependency on one of these is strong.
            assert!(!handle.is_placeholder());
            assert!(!handle.is_static());
            assert!(!handle.is_conditioner());
        }
        assert!(!spawner.is_runtime());
        assert!(!spawner.is_module());
        assert!(!runtime.is_subflow());
        assert!(!runtime.is_module());
        assert!(!module.is_subflow());
        assert!(!module.is_runtime());
        assert!(!Handle::Placeholder.is_subflow());
        assert!(!Handle::Static(Work::new(|| {})).is_module());
    }

    /// A node carries each of the four whole, which is what the builders and
    /// the executor arms rely on: the graph a subflow owns, the taskflow a
    /// module shares and the graph an adopted module took over all come back
    /// out of the node unchanged.
    #[test]
    fn a_node_carries_each_dynamic_tasking_handle() {
        let mut target = Taskflow::with_name("target");
        target.graph_mut().add_node(Handle::Placeholder);
        let target = Arc::new(target);

        let mut adopted = Graph::new();
        adopted.add_named_node("inner", Handle::Placeholder);

        let mut graph = Graph::new();
        let spawner = graph.add_named_node(
            "spawner",
            Handle::Subflow {
                work: SubflowWork::new(|_| {}),
                subgraph: Mutex::new(Graph::new()),
            },
        );
        let module = graph.add_named_node("module", Handle::Module(Arc::clone(&target)));
        let owned = graph.add_named_node("owned", Handle::AdoptedModule(adopted));

        let Handle::Subflow { subgraph, .. } = graph.node(spawner).handle() else {
            panic!("the subflow handle did not survive add_named_node");
        };
        // The subgraph is the node's own, reachable through a shared borrow of
        // the graph -- which is the whole reason it is behind a `Mutex`.
        subgraph
            .lock()
            .expect("uncontended")
            .add_node(Handle::Placeholder);
        assert_eq!(subgraph.lock().expect("uncontended").len(), 1);

        let Handle::Module(shared) = graph.node(module).handle() else {
            panic!("the module handle did not survive add_named_node");
        };
        assert!(Arc::ptr_eq(shared, &target));
        assert_eq!(shared.graph().len(), 1);
        assert_eq!(Arc::strong_count(&target), 2);

        let Handle::AdoptedModule(owned) = graph.node(owned).handle() else {
            panic!("the adopted module handle did not survive add_named_node");
        };
        assert_eq!(owned.node(Task::new(0)).name(), "inner");
    }

    /// The `nstate` bits are upstream's, which means distinct top bits: a
    /// value that is one of them is none of the others, and none of them can
    /// be confused with the strong-dependency count the port keeps in the
    /// same word.
    #[test]
    fn the_subflow_state_bits_are_upstreams_two_top_bits() {
        assert_eq!(Node::RETAIN_SUBFLOW, 0x4000_0000);
        assert_eq!(Node::JOINED_SUBFLOW, 0x8000_0000);
        assert_eq!(Node::RETAIN_SUBFLOW & Node::JOINED_SUBFLOW, 0);
        assert_eq!(Node::RETAIN_SUBFLOW.count_ones(), 1);
        assert_eq!(Node::JOINED_SUBFLOW.count_ones(), 1);

        // And they are bits of a node's own word, writable through a shared
        // node like every other piece of run state.
        let (graph, t) = graph_of(1);
        let node = graph.node(t[0]);
        node.nstate()
            .fetch_or(Node::JOINED_SUBFLOW, Ordering::Relaxed);
        assert_eq!(
            node.nstate().load(Ordering::Relaxed) & Node::RETAIN_SUBFLOW,
            0
        );
        assert_ne!(
            node.nstate().load(Ordering::Relaxed) & Node::JOINED_SUBFLOW,
            0
        );
    }

    /// `PREEMPTED` and the mask are the other half of upstream's `NSTATE`,
    /// and the one property that matters is that the state bits and the
    /// count cannot reach into each other: the count is masked out of the
    /// word on every read, so a bit set on a node must not be able to look
    /// like 536,870,912 dependencies, and a count must not be able to look
    /// like a state bit.
    #[test]
    fn the_state_bits_and_the_dependency_count_do_not_overlap() {
        assert_eq!(Node::PREEMPTED, 0x2000_0000);
        assert_eq!(Node::STRONG_DEPENDENCIES_MASK, 0x0FFF_FFFF);
        assert_eq!(Node::PREEMPTED.count_ones(), 1);

        // All four state bits, which is the whole of upstream's vocabulary:
        // disjoint from each other and from the count field.
        assert_eq!(Node::IMPLICITLY_ANCHORED, 0x1000_0000);
        assert_eq!(Node::IMPLICITLY_ANCHORED.count_ones(), 1);
        let state = Node::PREEMPTED
            | Node::RETAIN_SUBFLOW
            | Node::JOINED_SUBFLOW
            | Node::IMPLICITLY_ANCHORED;
        assert_eq!(state & Node::STRONG_DEPENDENCIES_MASK, 0);
        assert_eq!(state.count_ones(), 4);

        // The mask is contiguous from bit zero, so masking a word is the
        // count and nothing else, and it stops just below the lowest state
        // bit -- `IMPLICITLY_ANCHORED`, which is why the count field stops
        // one bit short of `PREEMPTED` rather than at it.
        assert_eq!(Node::STRONG_DEPENDENCIES_MASK.count_ones(), 28);
        assert_eq!(Node::STRONG_DEPENDENCIES_MASK.trailing_ones(), 28);
        const { assert!(Node::STRONG_DEPENDENCIES_MASK < Node::IMPLICITLY_ANCHORED) };
        const { assert!(Node::IMPLICITLY_ANCHORED < Node::PREEMPTED) };
        let (graph, t) = graph_of(1);
        let node = graph.node(t[0]);
        node.nstate()
            .store(Node::STRONG_DEPENDENCIES_MASK | state, Ordering::Relaxed);
        assert_eq!(
            node.nstate().load(Ordering::Relaxed) & Node::STRONG_DEPENDENCIES_MASK,
            Node::STRONG_DEPENDENCIES_MASK
        );
    }

    /// A node's state words are its [`NodeBase`], which is the type the
    /// scheduler holds when it does not care whether what it is counting
    /// down is a node or an anchor.
    #[test]
    fn a_nodes_state_words_are_its_base() {
        let (graph, t) = graph_of(1);
        let node = graph.node(t[0]);
        let base = node.base();

        base.nstate().store(2, Ordering::Relaxed);
        base.estate().store(4, Ordering::Relaxed);
        base.join_counter().store(6, Ordering::Relaxed);
        assert_eq!(node.nstate().load(Ordering::Relaxed), 2);
        assert_eq!(node.estate().load(Ordering::Relaxed), 4);
        assert_eq!(node.join_counter().load(Ordering::Relaxed), 6);

        // And a base of its own starts clear, which is what an anchor needs:
        // nothing outstanding until a graph is scheduled under it.
        let anchor = NodeBase::new();
        assert_eq!(anchor.nstate().load(Ordering::Relaxed), 0);
        assert_eq!(anchor.estate().load(Ordering::Relaxed), 0);
        assert_eq!(anchor.join_counter().load(Ordering::Relaxed), 0);
    }

    /// The explicit-anchor bit is upstream's `ExplicitAnchorGuard`, so what
    /// has to hold is that it is set for exactly as long as the guard lives
    /// -- an unwind through the waiting frame included -- and that it does
    /// not disturb the other estate bits.
    #[test]
    fn an_explicit_anchor_is_set_while_its_guard_lives() {
        let anchor = NodeBase::new();
        assert!(!anchor.is_explicitly_anchored());

        anchor
            .estate()
            .store(Cancellable::CANCELLED, Ordering::Relaxed);
        {
            let _guard = anchor.anchor_explicitly();
            assert!(anchor.is_explicitly_anchored());
            // Set beside whatever the word already held rather than over it,
            // which is why both the guard's halves are read-modify-write.
            assert_eq!(
                anchor.estate().load(Ordering::Relaxed),
                Cancellable::CANCELLED | Cancellable::EXPLICITLY_ANCHORED
            );
        }

        assert!(!anchor.is_explicitly_anchored());
        assert_eq!(
            anchor.estate().load(Ordering::Relaxed),
            Cancellable::CANCELLED,
            "the guard cleared a bit that was not its own"
        );

        // And on an unwind, which is the case the guard exists for: a frame
        // waiting for a nested graph must not leave the anchor set behind.
        let anchor = NodeBase::new();
        let unwound = std::panic::catch_unwind(|| {
            let _guard = anchor.anchor_explicitly();
            panic!("the waiting frame failed");
        });
        assert!(unwound.is_err());
        assert!(!anchor.is_explicitly_anchored());
    }

    /// The payload slot, which is upstream's `_exception_ptr` plus the
    /// `CAUGHT` arbiter that says who may write it: the first claimant keeps
    /// its payload, a later one is turned away, and taking the payload leaves
    /// the base ready to anchor another graph.
    #[test]
    fn the_payload_slot_keeps_the_first_claimant_and_is_taken_once() {
        let base = NodeBase::new();
        assert!(base.take_panic().is_none());

        // The walk marks every base it passes with `EXCEPTION`, so a base can
        // arrive at the claim already carrying it and must still be owed the
        // payload: the test is `CAUGHT` alone.
        base.estate()
            .fetch_or(Cancellable::EXCEPTION, Ordering::Relaxed);
        assert!(base.claim_panic(), "the first claimant");
        base.keep_panic(Box::new("first"));
        assert!(!base.claim_panic(), "the second claimant");

        assert_eq!(
            base.estate().load(Ordering::Relaxed),
            Cancellable::EXCEPTION | Cancellable::CAUGHT
        );

        let payload = base.take_panic().expect("a payload was kept");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"first"));
        // Taken, not cloned, and the base is no longer a failed one: both
        // bits are gone, so a graph scheduled under it next is not born
        // cancelled.
        assert!(base.take_panic().is_none());
        assert_eq!(base.estate().load(Ordering::Relaxed), 0);

        // Clearing is what a set-up does to a node: whatever was there goes,
        // and a slot that was empty stays empty.
        assert!(base.claim_panic());
        base.keep_panic(Box::new("dropped by the next set-up"));
        base.clear_panic();
        assert!(base.take_panic().is_none());
        base.clear_panic();
    }

    #[test]
    #[should_panic(expected = "locked twice at once")]
    fn invoking_work_twice_at_once_panics() {
        let work = Work::new(|| {});
        // Re-entering from this thread is the cheap stand-in for two workers
        // racing: `try_lock` cannot tell the two apart.
        let _guard = work.0.try_lock().expect("uncontended");
        work.invoke();
    }

    /// The job of the one source task of `taskflow`, which is how a case here
    /// gets hold of something a semaphore can park.
    ///
    /// The taskflow must be finished with by the time this is called:
    /// `Taskflow::graph_mut` refuses once a share of the storage is
    /// outstanding, and the job holds one.
    fn source_job(taskflow: &Taskflow) -> Job {
        let flow = Arc::clone(taskflow.flow());
        let topology = Arc::new(Topology::new(|| true, || {}, Arc::clone(&flow)));
        let owner = GraphOwner::flow(&flow);
        let mut sources = Job::sources(&owner, &topology, &Parent::Run);
        let job = sources.next().expect("the graph has a source task");
        assert!(sources.next().is_none(), "the cases here have one source");
        job
    }

    /// A node that acquires nothing may run, and asks no semaphore anything:
    /// every node of every other case in this crate is this one.
    #[test]
    fn acquire_all_of_a_node_with_no_semaphores_succeeds() {
        let mut taskflow = Taskflow::new();
        taskflow
            .graph_mut()
            .add_named_node("a", Handle::Placeholder);
        let job = source_job(&taskflow);

        let mut woken = Vec::new();
        assert!(job.node().acquire_all(&job, &mut woken));
        assert!(job.node().release_all(&mut woken).is_ok());
        assert!(woken.is_empty());
    }

    /// Upstream's `_acquire_all` loop exactly: the first semaphore that cannot
    /// be had parks the job, and the ones already taken are given back rather
    /// than held by a task that is not going to run.
    #[test]
    fn acquire_all_rolls_back_what_it_took_before_it_parked() {
        let free = Arc::new(Semaphore::new(1));
        let exhausted = Arc::new(Semaphore::new(0));

        let mut taskflow = Taskflow::new();
        let a = taskflow
            .graph_mut()
            .add_named_node("a", Handle::Placeholder);
        let semaphores = taskflow.graph_mut().node_mut(a).semaphores_mut();
        semaphores.to_acquire.push(Arc::clone(&free));
        semaphores.to_acquire.push(Arc::clone(&exhausted));
        let job = source_job(&taskflow);

        let mut woken = Vec::new();
        assert!(!job.node().acquire_all(&job, &mut woken));

        // Given back, and parked on the one that could not be had.
        assert_eq!(free.value(), 1);
        assert_eq!(free.num_waiters(), 0);
        assert_eq!(exhausted.num_waiters(), 1);
        // The rollback woke nothing, because nothing was waiting on `free`.
        assert!(woken.is_empty());
    }

    /// The whole of what the executor will do with a node of a semaphore of
    /// one: acquire, run, release -- and the release hands back whoever was
    /// parked in the meantime.
    #[test]
    fn release_all_gives_back_every_semaphore_and_collects_the_woken() {
        let semaphore = Arc::new(Semaphore::new(1));

        let mut taskflow = Taskflow::new();
        let a = taskflow
            .graph_mut()
            .add_named_node("a", Handle::Placeholder);
        let semaphores = taskflow.graph_mut().node_mut(a).semaphores_mut();
        semaphores.to_acquire.push(Arc::clone(&semaphore));
        semaphores.to_release.push(Arc::clone(&semaphore));
        let job = source_job(&taskflow);

        let mut woken = Vec::new();
        assert!(job.node().acquire_all(&job, &mut woken));
        assert_eq!(semaphore.value(), 0);

        // A second task of the same set, parked while the first holds the one
        // count there is.
        assert!(!semaphore.try_acquire_or_wait(&job));

        job.node()
            .release_all(&mut woken)
            .expect("the node holds the count it is giving back");
        assert_eq!(semaphore.value(), 1);
        assert_eq!(woken.len(), 1, "the parked job came back to be scheduled");
    }

    /// A node may release a semaphore it never acquired -- upstream's way of
    /// opening a gate for another task -- and that is the one way a release
    /// can fail.
    #[test]
    fn release_all_reports_an_over_release() {
        let semaphore = Arc::new(Semaphore::new(1));

        let mut taskflow = Taskflow::new();
        let a = taskflow
            .graph_mut()
            .add_named_node("a", Handle::Placeholder);
        taskflow
            .graph_mut()
            .node_mut(a)
            .semaphores_mut()
            .to_release
            .push(Arc::clone(&semaphore));
        let job = source_job(&taskflow);

        let error = job
            .node()
            .release_all(&mut Vec::new())
            .expect_err("the semaphore is already at its maximum");
        assert_eq!(error.max_value(), 1);
    }

    /// The two async handles answer their own predicate and nobody else's.
    /// They do *not* fold together the way the two module forms do, and
    /// [`Handle::is_dependent_async`] says why.
    #[test]
    fn the_handle_predicates_split_the_two_async_kinds() {
        let free = Handle::Async(AsyncWork::plain(|| {}));
        let dependent = Handle::DependentAsync(AsyncWork::plain(|| {}));

        assert!(free.is_async());
        assert!(dependent.is_dependent_async());
        assert!(!free.is_dependent_async());
        assert!(!dependent.is_async());

        for handle in [&free, &dependent] {
            assert!(!handle.is_placeholder());
            assert!(!handle.is_static());
            assert!(!handle.is_module());
            assert!(!handle.is_subflow());
            assert!(!handle.is_runtime());
            // Above all not a conditioner: an async node picks no successors.
            assert!(!handle.is_conditioner());
        }
        assert!(!Handle::Placeholder.is_async());
        assert!(!Handle::Static(Work::new(|| {})).is_dependent_async());
    }

    /// An async body runs **once**, which is the whole of what separates
    /// [`AsyncWork`] from [`Work`]: the one worker that gets there takes the
    /// body out of the slot, and every caller after it finds nothing.
    #[test]
    fn an_async_body_is_taken_out_of_its_slot_once() {
        let AsyncWork::Plain(slot) = AsyncWork::plain(|| {}) else {
            panic!("`plain` builds the plain shape");
        };

        assert!(!slot.is_spent());
        let body = slot.take().expect("the first taker gets the body");
        assert!(slot.is_spent(), "and the slot is empty behind it");
        assert!(slot.take().is_none(), "so a second worker runs nothing");

        // The body is a `FnOnce`, so running it is what consumes it: there is
        // no second call to be had even for the taker.
        body();
    }

    /// The reentrant shape's two halves are taken independently, which is the
    /// port's one departure from upstream's single `void(Runtime&, bool)`
    /// lambda: the first entry takes the body and the second, which may be on
    /// another worker, takes what delivers the node's payload.
    #[test]
    fn a_reentrant_body_and_its_completion_are_two_separate_onces() {
        let delivered = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&delivered);
        let work =
            AsyncWork::reentrant(|_| {}, move |payload| *slot.lock().expect("free") = payload);

        let AsyncWork::Reentrant { body, complete } = work else {
            panic!("`reentrant` builds the reentrant shape");
        };

        // The first entry spends the body and leaves the completion alone.
        assert!(body.take().is_some());
        assert!(body.is_spent());
        assert!(!complete.is_spent(), "the second entry still has its half");

        // The second entry hands over whatever the node's slot held, which is
        // upstream's `eptr ? set_exception : set_value`.
        complete.take().expect("the completion")(Some(Box::new("oops")));
        assert!(complete.is_spent());
        let payload = delivered.lock().expect("free").take().expect("delivered");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"oops"));
    }
}

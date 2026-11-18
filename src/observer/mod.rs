//! Translated from `taskflow/observer/interface.hpp` and the "Observer
//! methods" section of `taskflow/core/executor.hpp` (Taskflow v4.1.0).
//!
//! An [`Observer`] is a hook an [`Executor`](crate::Executor) calls around
//! every task it runs, so that a caller can watch which worker ran what and
//! when without reaching into the scheduler itself.
//! [`Executor::make_observer`](crate::Executor::make_observer) registers
//! one, [`Executor::remove_observer`](crate::Executor::remove_observer)
//! un-registers it, and
//! [`Executor::num_observers`](crate::Executor::num_observers) counts how
//! many are live. Upstream hands the
//! callbacks a `WorkerView` and a `TaskView`, immutable wrappers so that an
//! observer cannot reach into the worker or the node it is told about and
//! disturb a run under way; this port keeps the same split, as [`WorkerView`]
//! and [`TaskView`].
//!
//! # Design
//!
//! **`make_observer` takes a built value, not constructor arguments.**
//! Upstream's is a variadic template; the natural Rust shape is the other
//! half of that call -- the caller builds an `O` and hands it over, and
//! [`Executor::make_observer`](crate::Executor::make_observer) does the
//! `Arc::new` and the `set_up` upstream's does.
//!
//! **A worker's queue answers `is_queue_empty` and `queue_size`, but not
//! `queue_capacity`,** and **the registered list is one atomic check on the
//! path with no observers, and a mutex only when there is work to do** --
//! `DESIGN.md` has both, the first split across "Decisions taken in M7" (why
//! `is_queue_empty` alone shipped first) and "Decisions taken in M9" (why
//! `queue_size` joined it and `queue_capacity` still has not), the second
//! under "Decisions taken in M7".
//!
//! **A hook may panic, and the run reports it rather than the worker dying
//! of it.** [`Observer::on_entry`] and [`Observer::on_exit`] are called
//! under a `catch_unwind` in `WorkerThread::observer_prologue` and
//! `WorkerThread::observer_epilogue`, and a payload goes to
//! `Job::record_panic` -- the same channel a panicking task body uses, so
//! the run fails with [`RunError::Panicked`](crate::RunError::Panicked)
//! carrying the hook's own payload and the executor stays usable. The
//! alternative was to forbid a panicking hook in the obligations this
//! trait's docs already carry and leave the behaviour alone, and it was
//! rejected on what the violation costs: the hook calls bracket the
//! dispatch inside the worker's scheduling loop, so an escaping payload
//! kills the worker thread with the job in flight unfinished, which leaves
//! the run's join counter short and `RunHandle::wait` blocked for ever --
//! an unenforceable rule whose breach is a silent hang, against a rule the
//! scheduler can simply keep. Upstream has the same hang and is departed
//! from knowingly: its `_observer_prologue` and `_observer_epilogue` calls
//! sit *outside* the `TF_EXECUTOR_EXCEPTION_HANDLER` that wraps each body,
//! so a throwing hook unwinds into the `catch(...)` around its `_spawn`
//! loop and that worker exits. The catch is per observer rather than around
//! the loop over them, so one failing hook does not cost the observers
//! after it their callback, and every payload is offered to the run, which
//! keeps the first. The third hook, [`Observer::set_up`], needs none of
//! this: `Executor::make_observer` calls it on the registering thread
//! before the observer is inserted, so its panic propagates to that caller
//! and the observer is simply not registered.
//!
//! **`TaskView` borrows a node and the graph it lives in, not a node and the
//! index that names it.** Upstream's holds a `const Node&` alone; a
//! predecessor here is a [`Task`](crate::Task) index, so answering "is this
//! predecessor a conditioner" -- what [`TaskView::num_weak_dependencies`]
//! needs -- means resolving that index back into a node, which takes the
//! graph too.
//!
pub mod chrome;
pub mod tfprof;
mod timeline;

use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crate::executor::worker::Worker;
use crate::graph::{Graph, Node};
use crate::task::{TaskType, task_type_of};

/// A hook an [`Executor`](crate::Executor) calls around every task a worker
/// runs, upstream's `ObserverInterface`.
///
/// Every method takes `&self` and may be called from any worker at once, a
/// single registered observer being shared by the whole pool: an
/// implementation that accumulates anything needs its own synchronisation,
/// as [`WorkerInterface`](crate::WorkerInterface)'s doc comment says for the
/// same reason. `Send + Sync` is required so that an `Arc<dyn Observer>` can
/// be called from every worker thread.
pub trait Observer: Send + Sync {
    /// Called once, when this observer is registered: upstream's
    /// constructor-like `set_up`, told how many workers the executor has.
    ///
    /// Runs on the thread that registers the observer, before it is
    /// inserted, so a panic here is raised to that caller and leaves nothing
    /// registered.
    fn set_up(&self, num_workers: usize);

    /// Called on the worker about to run `task`'s work, before it does.
    ///
    /// A panic out of this hook fails the run the call belongs to, through
    /// the channel a panicking task body uses; it does not stop the task
    /// from running, the dispatch this brackets being already under way. The
    /// module docs' "Design" section has why it is caught at all.
    fn on_entry(&self, worker: WorkerView<'_>, task: TaskView<'_>);

    /// Called on the worker that just ran `task`'s work, right after.
    ///
    /// Reached once per dispatch, including one that preempts the node for a
    /// nested graph: the scheduling loop's `observer_epilogue` seam runs on
    /// every return out of the dispatch, whether or not it finished the
    /// node.
    ///
    /// A panic here fails the run as one out of
    /// [`on_entry`](Self::on_entry) does, and the node's own tear-down --
    /// the semaphores it holds, the successors it releases -- still runs.
    fn on_exit(&self, worker: WorkerView<'_>, task: TaskView<'_>);
}

/// An immutable view of a worker, handed to an [`Observer`] rather than the
/// [`Worker`] itself so that nothing it is told can be mutated through it.
/// Upstream's `WorkerView`.
#[derive(Clone, Copy, Debug)]
pub struct WorkerView<'a> {
    worker: &'a Worker,
}

impl<'a> WorkerView<'a> {
    /// Borrows `worker`. Crate-private, as upstream's constructor is
    /// `private` with `Executor` the one `friend`: only the scheduler builds
    /// one of these.
    pub(crate) fn new(worker: &'a Worker) -> Self {
        Self { worker }
    }

    /// This worker's id, an index in `0..num_workers`. Upstream's
    /// `WorkerView::id`.
    #[must_use]
    pub fn id(&self) -> usize {
        self.worker.id()
    }

    /// Whether this worker's queue held nothing at the moment of the call.
    /// Upstream's `WorkerView::empty`-style read beside
    /// [`queue_size`](Self::queue_size), rather than defined in terms of it.
    #[must_use]
    pub fn is_queue_empty(&self) -> bool {
        self.worker.is_queue_empty()
    }

    /// How many items this worker's queue held at the moment of the call.
    /// Upstream's `WorkerView::queue_size`.
    ///
    /// [`Worker::queue_size`]'s doc has the staleness caveat that applies
    /// here too: a sibling worker may steal from this queue, and the worker
    /// itself may push or pop it, at any point after this returns, so the
    /// number is not a fact this method's caller can act on, only a
    /// snapshot of a moment already past. The module docs' "Design" section
    /// is why there is no `queue_capacity` beside it.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::Executor;
    /// use rustflow::observer::{Observer, TaskView, WorkerView};
    /// use rustflow::prelude::*;
    ///
    /// struct Peek(Arc<AtomicUsize>);
    ///
    /// impl Observer for Peek {
    ///     fn set_up(&self, _num_workers: usize) {}
    ///
    ///     fn on_entry(&self, worker: WorkerView<'_>, _task: TaskView<'_>) {
    ///         self.0.store(worker.queue_size(), Ordering::SeqCst);
    ///     }
    ///
    ///     fn on_exit(&self, _worker: WorkerView<'_>, _task: TaskView<'_>) {}
    /// }
    ///
    /// let seen = Arc::new(AtomicUsize::new(usize::MAX));
    /// let executor = Executor::new(1);
    /// let _observer = executor.make_observer(Peek(Arc::clone(&seen)));
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace(|| {});
    /// executor.run(&taskflow).wait().unwrap();
    ///
    /// // The one worker's only task was already taken off the queue before
    /// // `on_entry` runs, so there is nothing left in it to see.
    /// assert_eq!(seen.load(Ordering::SeqCst), 0);
    /// ```
    #[must_use]
    pub fn queue_size(&self) -> usize {
        self.worker.queue_size()
    }
}

/// An immutable view of one task, handed to an [`Observer`] instead of a
/// handle that could edit the graph mid-run. Upstream's `TaskView`.
#[derive(Clone, Copy, Debug)]
pub struct TaskView<'a> {
    node: &'a Node,
    graph: &'a Graph,
}

impl<'a> TaskView<'a> {
    /// Borrows `node` out of `graph`. Crate-private, as upstream's
    /// constructor is `private` with `Executor` the one `friend`.
    pub(crate) fn new(node: &'a Node, graph: &'a Graph) -> Self {
        Self { node, graph }
    }

    /// The name of this task, empty if it was never named. Upstream's
    /// `TaskView::name`.
    #[must_use]
    pub fn name(&self) -> &'a str {
        self.node.name()
    }

    /// What kind of task this is. Upstream's `TaskView::type`.
    #[must_use]
    pub fn task_type(&self) -> TaskType {
        task_type_of(self.node.handle())
    }

    /// How many successors this task has, counting duplicates. Upstream's
    /// `TaskView::num_successors`.
    #[must_use]
    pub fn num_successors(&self) -> usize {
        self.node.num_successors()
    }

    /// How many predecessors this task has, counting duplicates. Upstream's
    /// `TaskView::num_predecessors`.
    #[must_use]
    pub fn num_predecessors(&self) -> usize {
        self.node.num_predecessors()
    }

    /// How many predecessors are *not* conditioners, which is how many of
    /// them must finish before this task may run. Upstream's
    /// `TaskView::num_strong_dependencies`.
    #[must_use]
    pub fn num_strong_dependencies(&self) -> usize {
        self.num_predecessors() - self.num_weak_dependencies()
    }

    /// How many predecessors are conditioners, any one of which may instead
    /// branch away from this task. Upstream's
    /// `TaskView::num_weak_dependencies`.
    #[must_use]
    pub fn num_weak_dependencies(&self) -> usize {
        self.node
            .predecessors()
            .iter()
            .filter(|&&predecessor| self.graph.node(predecessor).handle().is_conditioner())
            .count()
    }

    /// A hash of this task's identity, distinct from every other live
    /// task's. Upstream's `TaskView::hash_value` hashes the `Node*`; this
    /// hashes the address `self.node` borrows, which is as stable for as
    /// long as this view can exist -- the node does not move while a run can
    /// reach it.
    #[must_use]
    pub fn hash_value(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        std::ptr::from_ref(self.node).hash(&mut hasher);
        hasher.finish()
    }
}

/// The executor's registered observers: upstream's
/// `std::unordered_set<std::shared_ptr<ObserverInterface>>`.
///
/// The module docs' "Design" section has the shape and why: a lock-free
/// empty check for the two scheduling seams, a mutex taken only to register,
/// un-register, or snapshot the list for a seam that found it non-empty.
pub(crate) struct ObserverList {
    /// How many observers are registered, kept beside the list rather than
    /// read from it so that the empty check -- read on every dispatch -- is
    /// one relaxed load and nothing else.
    count: AtomicUsize,
    /// The observers themselves, written only by `insert` and `remove`.
    observers: Mutex<Vec<Arc<dyn Observer>>>,
}

impl ObserverList {
    /// An empty list.
    pub(crate) fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
            observers: Mutex::new(Vec::new()),
        }
    }

    /// Whether no observer is registered. The one check both scheduling
    /// seams open with, and the reason the zero-observer path allocates and
    /// locks nothing.
    pub(crate) fn is_empty(&self) -> bool {
        self.count.load(Ordering::Relaxed) == 0
    }

    /// How many observers are registered.
    pub(crate) fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Registers `observer`.
    pub(crate) fn insert(&self, observer: Arc<dyn Observer>) {
        let mut observers = self.lock();
        observers.push(observer);
        self.count.store(observers.len(), Ordering::Relaxed);
    }

    /// Un-registers whichever registered observer is `target`, identified by
    /// pointer rather than by value: upstream's `unordered_set::erase` on a
    /// `shared_ptr`, which likewise compares the pointee's address.
    pub(crate) fn remove(&self, target: &Arc<dyn Observer>) {
        let mut observers = self.lock();
        observers.retain(|observer| !Arc::ptr_eq(observer, target));
        self.count.store(observers.len(), Ordering::Relaxed);
    }

    /// Clones every registered observer's `Arc` into a `Vec` of its own, so
    /// that a seam which found the list non-empty can call into each one
    /// without holding the lock across the call -- see the module docs'
    /// "Design" section.
    pub(crate) fn snapshot(&self) -> Vec<Arc<dyn Observer>> {
        self.lock().clone()
    }

    /// The list, locked. A poisoned lock carries no information beyond "a
    /// writer panicked mid-edit", so it is taken rather than propagated, as
    /// every other lock in the scheduler is.
    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Arc<dyn Observer>>> {
        self.observers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl fmt::Debug for ObserverList {
    /// Reports the count rather than the observers themselves, which name no
    /// useful state of their own to print.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObserverList")
            .field("len", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct Counting {
        entries: AtomicUsize,
        exits: AtomicUsize,
    }

    impl Observer for Counting {
        fn set_up(&self, _num_workers: usize) {}

        fn on_entry(&self, _worker: WorkerView<'_>, _task: TaskView<'_>) {
            self.entries.fetch_add(1, Ordering::Relaxed);
        }

        fn on_exit(&self, _worker: WorkerView<'_>, _task: TaskView<'_>) {
            self.exits.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A fresh list is empty and stays that way until something is inserted.
    #[test]
    fn a_fresh_list_is_empty() {
        let list = ObserverList::new();
        assert!(list.is_empty());
        assert_eq!(list.len(), 0);
        assert!(list.snapshot().is_empty());
    }

    /// Inserting makes the list non-empty and visible in a snapshot;
    /// removing the same `Arc` reverses both.
    #[test]
    fn insert_and_remove_round_trip() {
        let list = ObserverList::new();
        let observer: Arc<Counting> = Arc::new(Counting {
            entries: AtomicUsize::new(0),
            exits: AtomicUsize::new(0),
        });
        let registered: Arc<dyn Observer> = Arc::clone(&observer) as Arc<dyn Observer>;

        list.insert(Arc::clone(&registered));
        assert!(!list.is_empty());
        assert_eq!(list.len(), 1);
        assert_eq!(list.snapshot().len(), 1);

        list.remove(&registered);
        assert!(list.is_empty());
        assert_eq!(list.len(), 0);
    }

    /// Removing an `Arc` that was never registered is a no-op, not a panic.
    #[test]
    fn removing_an_unregistered_observer_is_a_no_op() {
        let list = ObserverList::new();
        let observer: Arc<dyn Observer> = Arc::new(Counting {
            entries: AtomicUsize::new(0),
            exits: AtomicUsize::new(0),
        }) as Arc<dyn Observer>;

        list.remove(&observer);
        assert!(list.is_empty());
    }
}

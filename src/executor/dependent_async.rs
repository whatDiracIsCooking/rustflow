//! Translated from `taskflow/core/async.hpp` (Taskflow v4.1.0):
//! [`dependent_async`](Executor::dependent_async) --
//! [`silent_dependent_async`](Executor::silent_dependent_async), the closure
//! branches of `_dependent_async` and `_silent_dependent_async`,
//! `_schedule_dependent_async_task`, `_process_dependent_async` and
//! `_tear_down_dependent_async`. Case 0 of `_invoke_dependent_async_task`
//! and the `DEPENDENT_ASYNC` arm of `_invoke` come from
//! `taskflow/core/executor.hpp` of the same release and land in [`invoke`].
//!
//! A dependent-async task is an async task with edges to other async tasks:
//! the same free-standing node `executor::async_node` is, submitted the same
//! way from any thread, counted in the same two counters, and -- for the
//! handle-returning form -- delivering into the same [`AsyncHandle`]. The
//! other half of the header is [`async`](crate::executor::r#async) and this
//! module leans on it rather than repeating it. What is particular to this
//! half is that the graph is built **one task at a time, while the tasks
//! already submitted are running**, which no `Taskflow` can do and which is
//! the whole of the protocol below.
//!
//! # One iterator where upstream has four overloads
//!
//! Upstream names dependencies either variadically --
//! `silent_dependent_async(f, t1, t2)`, which packs `&t1, &t2` into a
//! `std::array` and forwards -- or as an iterator pair, `(first, last)` over
//! anything dereferencing to an `AsyncTask`. Both collapse onto one
//! `IntoIterator<Item = &AsyncTask>` parameter here, which is what makes
//! `[&a, &b]`, `tasks.iter()`, `&tasks[k..k + 1]` and `[]` all read naturally
//! at the call site, and which is also the one thing about the signature that
//! is not upstream's: `std::distance` on a random-access pair is how upstream
//! knows how many dependencies there are before it looks at any of them, and
//! a Rust iterator may be single-pass. The guard below is what replaces it.
//!
//! The `TaskParams` overloads collapse onto the unnamed forms for
//! [`async`](crate::executor::r#async)'s reason: they differ only in the name
//! given to the node, and nothing in this milestone reads one.
//!
//! # The protocol
//!
//! Upstream guards a predecessor's `_edges` with the `ESTATE::LOCKED` spin
//! bit and marks a finished task with `ESTATE::FINISHED` in the same word, so
//! that its two operations cannot interleave: naming a dependency has to
//! either find it unfinished and leave a successor behind, or find it
//! finished and leave nothing, and finishing has to either see a successor
//! and take it, or set the bit that sends the next namer down the other
//! branch. Here the successor list is the `Mutex<Vec<Job>>` on `AsyncNode`
//! and `FINISHED` is set *under that lock* -- `AsyncNode::push_successor` and
//! `AsyncNode::finish` are the pair, and `executor::async_node` has why one
//! lock over both is the same statement as upstream's one word.
//!
//! So a submission does this, per dependency named:
//!
//! - an **empty** handle is skipped, which is upstream's `task.empty()`
//!   branch of `_process_dependent_async` and the case
//!   `DependentAsync.NullDependency` is about: a default-constructed
//!   [`AsyncTask`](crate::AsyncTask) names no task, so there is nothing to
//!   wait for and nothing to push onto;
//! - a dependency that has **already finished** is counted off the new node's
//!   join counter at once, because nothing will ever drain its list again;
//! - otherwise the new node's [`Job`] goes onto that dependency's list, and
//!   the dependency's own tear-down is what counts it off.
//!
//! A node whose counter reaches zero is queued, by whichever of the two
//! arrives last -- the submitting thread or a predecessor's tear-down. Which
//! of them it is cannot be predicted and does not matter: exactly one
//! decrement drives the counter to zero, because every one of them is a
//! `fetch_sub` of its own.
//!
//! # The guard, where upstream has `std::distance`
//!
//! Upstream initialises the new node's `_join_counter` to the number of
//! dependencies and then decrements it for each one already met, scheduling
//! if the last decrement left zero. That needs the count *before* the first
//! dependency is looked at, which a single-pass iterator cannot give.
//!
//! This port starts the counter at [`SUBMITTING`] -- one outstanding
//! dependency, which is the submitting call itself -- raises it by one as each
//! dependency is named, and spends the guard together with everything it
//! found already met in one final `fetch_sub`. The guard is not a trick to
//! avoid counting: it is load-bearing, and the thing it buys is that the
//! counter **cannot reach zero while the submission is still in progress**.
//! Without it a task whose first dependency finished early could be queued,
//! run and *finish* before its second dependency had been named -- and naming
//! a dependency of a node that has already finished would leave a job on a
//! list that nothing drains again, which is a task that never runs, or push
//! it onto a predecessor whose tear-down then schedules a node that has
//! already run. Upstream is safe from both for the same reason by
//! construction, since its counter starts at the full count.
//!
//! # A submission of its own, not `schedule_async_task`
//!
//! `Executor::schedule_async_task` in [`async`](crate::executor::r#async) is
//! the submission for the other half of the header and cannot serve this one,
//! which is upstream's split exactly -- `_schedule_dependent_async_task` is a
//! function of its own there too. Two things differ and both are structural:
//! the node carries a [`Handle::DependentAsync`](crate::Handle), so that the
//! dispatch reaches the arm that releases successors, and the job must
//! **not** be queued until every dependency has been named, which is the
//! guard's whole point. What is shared is shared rather than copied:
//! `Shared::increment_async`, `Parent::count_queued` and
//! `Shared::bulk_schedule` are the same three calls in the same order, and
//! `Shared::finish_async` is the same tear-down counter.
//!
//! One of those three lands at a different *point*, which is the only other
//! thing this submission does differently. `Parent::count_queued` is "tell
//! the parent before you queue the job", and for a plain async task the
//! submission and the queueing are one instant, so counting beside the push
//! is counting at submission. Here they are not: the job may be queued by a
//! predecessor's tear-down, on another thread, at any time after the first
//! dependency is named. So the count is taken once at submission, for the
//! one job that will run the task, and neither the push below nor the
//! tear-down takes it again. Upstream agrees and says so from the other
//! side: its `Runtime::dependent_async` raises `_node->_join_counter` itself
//! before calling `_dependent_async` at all.
//!
//! # Ownership is the `Arc`
//!
//! `async_task.hpp`'s argument for a reference-counted handle is the ABA
//! problem: a task named as the dependency of a *later* submission has to
//! still be there when that submission looks at it, and a count held by the
//! caller is what guarantees it. Here the count is an `Arc<AsyncNode>`, held
//! by every [`AsyncTask`](crate::AsyncTask) handle, by every [`Job`] of the
//! node, and by every predecessor's successor list until that list is
//! drained. Upstream's `_incref`/`_decref` pairs, its tear-down
//! `fetch_sub(ESTATE::REFCOUNT_ONE)` and its `recycle(node)` therefore have
//! no equivalent at all: the node goes when the last share goes, whether that
//! is a handle's, a worker's or a list's.
//!
//! # Panics
//!
//! A [`dependent_async`](Executor::dependent_async) body's panic goes into
//! its handle, because the body the node runs is the wrapper
//! `AsyncHandle::for_body` built -- upstream's `std::packaged_task`, and the
//! same wrapper [`Executor::spawn`] submits. The executor's own failure
//! machinery never sees it.
//!
//! A [`silent_dependent_async`](Executor::silent_dependent_async) body's
//! panic has no handle to go into, so it goes through `Job::record_panic`
//! like any other task's -- and this is the one place in the crate where that
//! walk's last case is both reached *and* readable. The chain is one link
//! long and roots at the sentinel run every free node shares, which is not an
//! anchor (#105), so the payload stays on the node the task is; and unlike a
//! `silent_async`'s, that node is still named by a handle, so
//! [`AsyncTask::take_panic`](crate::AsyncTask::take_panic) hands it back --
//! upstream's `exception_ptr()` on the same node. The order the tear-down
//! keeps is what makes it worth reading: the payload is parked *before*
//! `FINISHED` is set, so a caller that sees
//! [`is_done`](crate::AsyncTask::is_done) can take the payload.
//!
//! Neither kind of failure cancels anything. A free node has nothing above it
//! to cancel, and the sentinel run is never marked -- which is what keeps a
//! failed task from skipping every async task submitted after it.
//!
//! # Where a released successor is queued
//!
//! Upstream's tear-down releases through `_update_cache`, so the last
//! successor it finds ready is run by this worker on the spot and the rest go
//! on its queue. The cache cannot be reached from here -- it is a local of
//! `WorkerThread::invoke_one`, filled from the *graph* successors a free node
//! does not have, and [`invoke`] answers with a [`Release`] rather than a job
//! -- so every released successor goes onto this worker's own queue instead.
//! That is the same jobs in the same order: the queue is LIFO, so the last
//! one pushed is the first one popped, and it is popped by this worker unless
//! a thief gets there first. The cost is one push and one pop per chain link,
//! which is what `BENCHMARKS.md` measures the continuation against for a
//! graph's chain.
//!
//! # Why nothing here can hang
//!
//! A dependent-async task submitted to the executor is **never skipped**,
//! which is what makes the successor release unconditional. The early-out at
//! the top of `WorkerThread::invoke_one` skips a job whose run is cancelled or
//! whose parent chain is marked, and a free node submitted here has neither:
//! its run is the sentinel, which nothing ever records anything on, and its
//! parent is `Parent::Run` rather than a chain. So every queued
//! dependent-async job reaches the dispatch, every dispatch reaches the
//! tear-down below, and every tear-down sets `FINISHED` and drains the list
//! -- including the job of a task whose body panicked, since a panic is
//! caught rather than unwound through the tear-down.
//!
//! # A dependent-async handle inside a graph is still refused
//!
//! [`Graph::add_node`](crate::Graph::add_node) takes any
//! [`Handle`](crate::Handle), so a caller can put a
//! [`Handle::DependentAsync`](crate::Handle) into a taskflow's graph. It is
//! refused for the reason [`async`](crate::executor::r#async) gives -- an
//! async body is a `FnOnce` and a graph may be run any number of times -- and
//! doubly so here: the dependencies of a dependent-async task are other free
//! nodes rather than edges of any graph, so such a node could not have them
//! honoured even in principle.

use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::Ordering;

use crate::async_task::AsyncTask;
use crate::executor::Executor;
use crate::executor::r#async::{AsyncHandle, ESTATE_NONE};
use crate::executor::async_node::AsyncNode;
use crate::executor::runtime;
use crate::executor::topology::{Job, Parent, Topology};
use crate::executor::worker::{Release, WorkerThread};
use crate::graph::{AsyncWork, Handle};

/// The one dependency a submission in progress counts as outstanding on its
/// own behalf: the guard the module docs argue for.
///
/// A named one because it appears twice -- raised before the dependencies are
/// named and spent with the last of them -- and because a bare `1` at either
/// site says nothing about which of the counter's two readings it is.
const SUBMITTING: usize = 1;

impl Executor {
    /// Runs `body` on this executor once every task in `dependencies` has
    /// finished, and hands back a handle on the task and a handle on its
    /// result.
    ///
    /// Upstream's `Executor::dependent_async`. The [`AsyncTask`] is what
    /// names this task as a dependency of a later one; the [`AsyncHandle`] is
    /// upstream's `std::future<R>` and behaves exactly as
    /// [`Executor::spawn`]'s does, including that dropping it detaches.
    ///
    /// Dependencies are named by handle and may be named from any thread,
    /// including from a task body already running on one of this executor's
    /// workers -- which is how a graph of async tasks is built while it runs.
    /// A task already finished is not waited for, and an **empty** handle is
    /// skipped: it names no task.
    ///
    /// A panic out of `body` is caught by the wrapper this call builds and
    /// reported from [`AsyncHandle::wait`] as
    /// [`RunError::Panicked`](crate::RunError::Panicked)`(payload)`. It does
    /// **not** cancel the tasks that depend on this one -- upstream's
    /// `std::future` makes the same promise, since the exception it stores
    /// was never thrown as far as the executor is concerned -- so a successor
    /// of a failed task runs, and a caller who wants otherwise reads the
    /// handle and decides.
    ///
    /// ```
    /// use rustflow::Executor;
    ///
    /// let executor = Executor::new(4);
    ///
    /// let (first, six) = executor.dependent_async(|| 6, []);
    /// let (second, seven) = executor.dependent_async(|| 7, []);
    ///
    /// // Runs only after both of the above, whichever workers took them.
    /// let (_product, answer) = executor.dependent_async(|| 42, [&first, &second]);
    ///
    /// assert_eq!(six.wait().expect("no panic"), 6);
    /// assert_eq!(seven.wait().expect("no panic"), 7);
    /// assert_eq!(answer.wait().expect("no panic"), 42);
    /// ```
    pub fn dependent_async<'a, R, F>(
        &self,
        body: F,
        dependencies: impl IntoIterator<Item = &'a AsyncTask>,
    ) -> (AsyncTask, AsyncHandle<R>)
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        // Upstream's `std::packaged_task<R()>` and the `get_future()` beside
        // it, which is the one construction site for a handle in the crate:
        // the node runs a plain body whatever `R` is, exactly as a `spawn`ed
        // one does.
        let (wrapper, handle) = AsyncHandle::for_body(body);
        let task = self.schedule_dependent_async_task(
            AsyncWork::plain(wrapper),
            &Parent::Run,
            // Upstream's `ESTATE::REFCOUNT_ONE` and nothing else: the
            // reference count is the `Arc` here, and the anchor bit its
            // runtime-shaped branch also sets is #92's, where there is a
            // second entry that reads the node's payload slot. A plain
            // body's failure is caught by the wrapper above instead.
            ESTATE_NONE,
            dependencies,
        );
        (task, handle)
    }

    /// Runs `body` on this executor once every task in `dependencies` has
    /// finished, with no handle on its result.
    ///
    /// Upstream's `Executor::silent_dependent_async`, and the cheaper half of
    /// the pair: there is no slot to deliver into, so a submission costs the
    /// node and the body and nothing else. Everything
    /// [`Executor::dependent_async`] says about naming dependencies applies
    /// here too, and the [`AsyncTask`] it hands back is the same handle.
    ///
    /// A panic out of `body` is **kept on the task**, where
    /// [`AsyncTask::take_panic`] hands it back to whoever still holds a
    /// handle -- upstream's `exception_ptr()`, and the one place in this port
    /// a free node's own payload slot is both written and readable. It
    /// cancels nothing, here or in the tasks that depend on this one. The
    /// default panic hook still prints it, so a silent task that fails is not
    /// an invisible one.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::Executor;
    ///
    /// let executor = Executor::new(4);
    /// let order = Arc::new(AtomicUsize::new(0));
    ///
    /// // A chain of a hundred, each submitted while the one before it may
    /// // already be running, and each asserting that it ran after it.
    /// let mut previous = None;
    /// for step in 0..100 {
    ///     let order = Arc::clone(&order);
    ///     let task = executor.silent_dependent_async(
    ///         move || assert_eq!(order.fetch_add(1, Ordering::Relaxed), step),
    ///         previous.as_ref(),
    ///     );
    ///     previous = Some(task);
    /// }
    ///
    /// executor.wait_for_all();
    /// assert_eq!(order.load(Ordering::Relaxed), 100);
    /// ```
    pub fn silent_dependent_async<'a, F>(
        &self,
        body: F,
        dependencies: impl IntoIterator<Item = &'a AsyncTask>,
    ) -> AsyncTask
    where
        F: FnOnce() + Send + 'static,
    {
        self.schedule_dependent_async_task(
            AsyncWork::plain(body),
            &Parent::Run,
            ESTATE_NONE,
            dependencies,
        )
    }

    /// Creates the free-standing node `work` is the body of, counts the
    /// dependencies named against it, and queues it if there are none left to
    /// wait for: upstream's `_schedule_dependent_async_task` plus the
    /// `_process_dependent_async` it calls per dependency and the
    /// `_increment_topology` its public callers make first, which as there is
    /// taken only for a parentless submission.
    ///
    /// `parent` and `estate` are upstream's own two arguments and mean what
    /// they mean in `Executor::schedule_async_task`: [`Parent::Run`] is its
    /// null parent, and the estate word is the bits to start the node with.
    /// Both are parameters rather than constants because the runtime-shaped
    /// forms (#92) need the other value of each, as they do for the plain
    /// async node.
    ///
    /// The order of the three steps that are not the dependency loop is
    /// `schedule_async_task`'s, for its reasons: the counters go up before
    /// anything can be queued, the parent is told about the job before it is
    /// pushed, and the push is last because the job may already be running
    /// when it returns. What this adds in the middle is the guard, which the
    /// module docs argue for -- and note that the guard is raised *before*
    /// `increment_async` could matter, since a node nobody can reach yet
    /// cannot be scheduled by anybody.
    pub(crate) fn schedule_dependent_async_task<'a>(
        &self,
        work: AsyncWork,
        parent: &Parent,
        estate: u32,
        dependencies: impl IntoIterator<Item = &'a AsyncTask>,
    ) -> AsyncTask {
        // Parentless only, which is the condition the tear-down below
        // decrements under and upstream's split between its public
        // `dependent_async` and the `_dependent_async(..., tpg, parent)` its
        // runtime and task-group forms call: `Executor::schedule_async_task`
        // has the whole of the argument, and #115 is what the two conditions
        // disagreeing cost.
        if matches!(parent, Parent::Run) {
            self.shared.increment_async();
        }

        let node = AsyncNode::new("", Handle::DependentAsync(work));
        node.node()
            .base()
            .estate()
            .fetch_or(estate, Ordering::Relaxed);

        // Upstream's `num_predecessors` as the counter's initial value, with
        // the guard standing in for a count it cannot take in advance.
        // Relaxed, and nothing else is possible to get wrong here: no other
        // thread can name this node yet, because no other thread has it.
        let outstanding = node.node().join_counter();
        outstanding.store(SUBMITTING, Ordering::Relaxed);

        // Upstream's `_node->_join_counter.fetch_add(1)`, which is a line of
        // `Runtime::dependent_async` rather than of
        // `_schedule_dependent_async_task` -- the parent is told by whoever
        // submits. It is here instead, as `schedule_async_task` has it, and
        // *this early* rather than beside the push below, which is the one
        // ordering question this call raises for this node kind: a plain
        // async task is queued by the thread that submits it, so the two
        // points are the same instant, but a dependent-async task may be
        // queued by a predecessor's tear-down on another thread the moment
        // the first `push_successor` returns -- and may be queued however
        // long after this call. One count per *task* and not per job: the
        // several jobs the loop below may create are copies of the one job
        // that will run it, the rest of which are dropped undequeued, and
        // `WorkerThread::finish_job` spends this count once when that job
        // finishes.
        parent.count_queued(Topology::free_standing(), 1);

        // Upstream's `_process_dependent_async` loop. `met` is the
        // dependencies that turned out to be over already -- its
        // `num_predecessors = node->_join_counter.fetch_sub(1) - 1` branch,
        // deferred to one decrement below rather than taken per dependency,
        // because under the guard there is nothing to be gained by taking it
        // early and one less atomic to pay for.
        let mut met = 0;
        for dependency in dependencies {
            // Upstream's `if(task.empty())` branch: an empty handle names no
            // task, so it is neither counted nor pushed onto. Skipping it
            // outright is the same arithmetic as upstream's count-then-cancel
            // and is `DependentAsync.NullDependency`.
            let Some(predecessor) = dependency.node() else {
                continue;
            };

            // Before the push, and that order is the whole of why the guard
            // exists: the predecessor's tear-down may run this job's
            // decrement the instant the push returns, so the increment it is
            // paying for has to be in the counter first.
            outstanding.fetch_add(1, Ordering::Relaxed);

            // Upstream's spin-locked `_edges.push_back(node)` against its
            // `ESTATE::FINISHED` test, which `AsyncNode::push_successor`
            // performs under one lock: `false` is the dependency that had
            // already finished, whose list nothing will drain again, so this
            // submission counts it off itself.
            if !predecessor.push_successor(Job::free(&node, parent)) {
                met += 1;
            }
        }

        // Upstream's `if(num_predecessors == 0) _schedule(...)`, which is
        // this decrement reaching zero. One `fetch_sub` for the guard and
        // every met dependency together, so that the counter is driven to
        // zero exactly once however many of each there were: every other
        // decrement of it is a predecessor's tear-down spending one.
        //
        // `AcqRel`, as upstream's is: the acquire half is what makes a
        // predecessor's writes visible to whoever is released by this, and
        // the release half publishes the pushes above to the tear-down that
        // is released by the predecessor's own decrement instead.
        if outstanding.fetch_sub(met + SUBMITTING, Ordering::AcqRel) == met + SUBMITTING {
            self.shared.bulk_schedule([Job::free(&node, parent)]);
        }

        // Upstream's `AsyncTask task(animate(...))` is taken at the *top* of
        // its function, because the count it takes is what keeps the node
        // alive while the loop runs. Here the local `node` is that share and
        // this is it being handed over -- the node has been shared from its
        // constructor on, which `executor::async_node` says is why.
        AsyncTask::new(node)
    }
}

/// Runs the dependent-async task `job` is for and releases whatever was
/// waiting for it: upstream's `DEPENDENT_ASYNC` arm of `_invoke`, which is
/// `_invoke_dependent_async_task` followed by
/// `_tear_down_dependent_async`.
///
/// `Some(Release::Strong)` for a task that is over. A free node has no graph
/// successors to release, so what that answer buys is the tear-down
/// `WorkerThread::invoke_one` performs on the way out -- which for a job
/// under a parent (#92's runtime child, #94's task-group member) counts that
/// parent down, and for a parentless one reaches the sentinel run, whose
/// counter can never hit zero, so it correctly does nothing.
///
/// `None` is upstream's `if(_invoke_dependent_async_task(...)) return;` -- a
/// runtime-shaped body that preempted its own node -- and neither the
/// successors nor the counters are touched then: the second entry is what
/// finishes the task, and a task that has not finished must not release what
/// is waiting for it.
pub(crate) fn invoke(worker: &mut WorkerThread, job: &Job, work: &AsyncWork) -> Option<Release> {
    // Upstream has no equivalent and needs none, for the reason
    // `executor::async` gives: a handle somebody built into a graph is told
    // apart from a free node's job by the identity of the run, and the
    // refusal goes through the channel a panicking body uses so that the run
    // is failed rather than hung.
    if !job.topology().is_free_standing() {
        job.topology().record_panic(Box::new(
            "a dependent-async node cannot be run as part of a taskflow: \
             a dependent-async task is submitted to the executor, and its \
             dependencies are other async tasks rather than edges of a graph",
        ));
        return Some(Release::Strong);
    }

    let release = match work {
        // Upstream's case 0, `void()`: a `silent_dependent_async` body or the
        // wrapper a `dependent_async` submits, under the `catch_unwind` that
        // is its `TF_EXECUTOR_EXCEPTION_HANDLER`. Taken out of the slot
        // rather than called through it, because an async body is a `FnOnce`;
        // `None` would be a body already spent, which one submission and one
        // queued job cannot reach, and doing nothing is the one answer to it
        // that cannot run a body twice. `AssertUnwindSafe` is the honest
        // annotation, for `Handle::Static`'s reason.
        AsyncWork::Plain(body) => {
            if let Some(body) = body.take()
                && let Err(payload) = panic::catch_unwind(AssertUnwindSafe(body))
            {
                // Upstream's `_process_exception`, and for this node kind the
                // walk's last case is the useful one: nothing is anchored
                // above a task submitted here, so the payload stays on the
                // node, where `AsyncTask::take_panic` reads it back. Before
                // `finish` below, which is what lets a caller that has seen
                // `is_done` take it.
                job.record_panic(payload);
            }
            Some(Release::Strong)
        }
        // Upstream's cases 1 and 2, which are its two
        // `_invoke_runtime_task_impl` overloads and this port's
        // `executor::runtime`. Nothing submits a runtime-shaped
        // dependent-async body yet -- `Runtime::dependent_async` is #92's --
        // so the only way to such a node today is to build one by hand
        // inside the crate; the arm is here rather than `unreachable!`
        // because the node kind is what decides the protocol, not the shape
        // of the body, and #92 needs this line to say exactly this.
        AsyncWork::Runtime(_) | AsyncWork::Reentrant { .. } => {
            runtime::invoke_async(worker, job, work)
        }
    };

    // Upstream's bare `return` out of the arm: the node is preempted, so it
    // has not finished and owes nothing yet.
    let release = release?;

    release_successors(worker, job);

    // Upstream's parentless branch of `_tear_down_dependent_async`:
    // `if(node->_parent == nullptr) _decrement_topology()`, where this port's
    // null parent is `Parent::Run` on a free node. A parented one is not
    // counted here because the submission does not count it either, and what
    // accounts for it instead is its parent's join counter (#115). The
    // counters go *after* the release above, as upstream orders them: a
    // successor is already counted in them on its own behalf, so the order
    // cannot let `wait_for_all` return early either way, and keeping
    // upstream's is one less thing for a reader to check.
    if matches!(job.parent(), Parent::Run) {
        worker.shared.finish_async();
    }

    Some(release)
}

/// Marks the task finished and queues whatever that made ready: the rest of
/// upstream's `_tear_down_dependent_async`, minus the parent branch its
/// caller keeps.
///
/// The two steps are `AsyncNode::finish`, which sets `FINISHED` under the
/// successor lock and hands the list back, and then the release -- outside
/// the lock, which is this port's one departure from upstream's shape here.
/// Upstream releases under its spin bit because its scheduling path is
/// lock-free; a job pushed to a queue may be stolen and run before this call
/// returns, and whatever it does must not be able to wait on the lock it was
/// released under. `executor::async_node` has that argument in full.
fn release_successors(worker: &mut WorkerThread, job: &Job) {
    // Upstream's `std::get_if<Node::DependentAsync>(&node->_handle)`, which
    // cannot fail for the same reason: the dispatch arm this runs under is
    // chosen by the node's own handle, and only a free node carries one.
    let node = job
        .async_node()
        .expect("a dependent-async job is a job of a free-standing node");

    // Upstream's loop over `node->_edges`, which holds the *successors*
    // rather than the edges a graph node's list holds.
    for successor in node.finish() {
        // Upstream's `s->_join_counter.fetch_sub(1, acq_rel) == 1`, over the
        // node of the job that was parked here rather than over a node
        // pointer. It cannot wrap, which is why this is a plain `fetch_sub`
        // where the successor loops in `WorkerThread::invoke_one` floor at
        // zero (#98): every decrement of this counter was paid for by an
        // increment the submitting call made before pushing the job, and
        // nothing can bypass a free node's dependencies -- there is no
        // `Runtime::schedule` for a node that is not part of a graph. The
        // assertion is in debug builds, where the crate's own tests run, so
        // a protocol that broke that invariant would fail rather than hang.
        let outstanding = successor
            .node()
            .join_counter()
            .fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(
            outstanding, 0,
            "a dependent-async task's join counter is decremented once per dependency named"
        );

        if outstanding == 1 {
            // Nothing is counted here, which is the one place this port's
            // "count a job before you queue it" rule is kept by somebody
            // else: the submission that created this job counted it against
            // its parent then, because it could not know which thread would
            // queue it or when. Counting again here would leave the parent
            // waiting for a job that no longer exists.
            //
            // This worker's own queue, where upstream's `_update_cache`
            // would have kept one of them for the frame below this one. The
            // module docs have why the cache cannot be reached from here and
            // why the difference is one push and one pop rather than a
            // change in which worker runs what.
            worker.schedule(successor);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::executor::queue::WorkerQueue;
    use crate::executor::topology::Nested;
    use crate::prelude::*;

    /// How long a handshake waits for the other side before it gives up.
    ///
    /// The number `executor::async` and `tests/basics.rs` keep, for their
    /// reason: nothing here passes because of it, and the only thing that
    /// reaches it is a side that is never going to arrive. A deadline rather
    /// than an open loop matters more in this module than in most, because
    /// the regression it guards against -- a successor left on a list nothing
    /// drains -- is a task that never runs, and a task that never runs is a
    /// counter that never reaches zero.
    const GIVE_UP_AFTER: Duration = Duration::from_secs(120);

    /// Spins until `flag` is set, failing rather than hanging if it never is.
    fn spin_until(what: &str, flag: &AtomicBool) {
        let deadline = Instant::now() + GIVE_UP_AFTER;
        while !flag.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "gave up waiting for {what}");
            thread::yield_now();
        }
    }

    /// A fresh flag, and the two ends of it.
    fn flag() -> (Arc<AtomicBool>, Arc<AtomicBool>) {
        let flag = Arc::new(AtomicBool::new(false));
        (Arc::clone(&flag), flag)
    }

    /// The two counters a free async task's submission and tear-down have to
    /// balance: `(num_asyncs, num_topologies)`.
    ///
    /// This helper and the two below are `executor::async`'s, kept here as
    /// `spin_until` above is: the imbalance they read was one line in each
    /// module, so each module asserts its own.
    fn counters(executor: &Executor) -> (usize, usize) {
        (
            executor.shared.num_asyncs.load(Ordering::Acquire),
            executor.num_topologies(),
        )
    }

    /// Spends whatever the counters are still carrying, so that a case which
    /// found them unbalanced fails instead of hanging in [`Executor`]'s own
    /// `Drop`, which waits for `num_asyncs` to reach zero.
    fn drain_counters(executor: &Executor) {
        for _ in 0..executor.shared.num_asyncs.load(Ordering::Acquire) {
            executor.shared.finish_async();
        }
    }

    /// Takes the one job a submission pushed back out of the shared buffers,
    /// for a case that runs it by hand because the pool cannot: a worker
    /// skips the steal loop while `num_topologies` is zero, which is what a
    /// parented submission correctly leaves it at.
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

    /// A dependency that had already finished when the dependent was created
    /// is counted off at submission and **nothing is pushed onto it**: its
    /// list is empty afterwards, so it holds no share of the task that named
    /// it and nothing is waiting to be drained by a tear-down that has
    /// already happened.
    ///
    /// Upstream's other branch of `_process_dependent_async`, and the half
    /// that a lost race would turn into a task that never runs.
    #[test]
    fn a_dependency_that_already_finished_does_not_hold_its_dependent() {
        let executor = Executor::new(2);

        let first = executor.silent_dependent_async(|| {}, []);
        executor.wait_for_all();
        assert!(
            first.is_done(),
            "the dependency is over before the next call"
        );

        let (ran, set_ran) = flag();
        let second = executor
            .silent_dependent_async(move || set_ran.store(true, Ordering::Release), [&first]);
        spin_until("the dependent of a finished dependency", &ran);

        // Destructive, and inert for exactly the reason this asserts: the
        // node is finished, so `finish` is the `std::mem::take` of a list
        // nothing can push onto again -- `executor::async_node`'s own case
        // asserts the same pair.
        assert!(
            first
                .node()
                .expect("a submitted task names a node")
                .finish()
                .is_empty(),
            "a finished dependency keeps no successor"
        );
        executor.wait_for_all();
        assert!(
            second.is_done(),
            "the flag the spin waited for is set by the body and `is_done` by the \
             tear-down after it, so this is asserted after the wait rather than \
             after the spin"
        );
    }

    /// A dependent of a task that is still **running** is held by it -- the
    /// job is on that task's list, which is a share of the node beside the
    /// handle's -- and released exactly once when it finishes.
    ///
    /// The count is the assertion rather than a timing claim: two shares
    /// while the dependency is parked, one body run after it is let go.
    #[test]
    fn a_dependent_on_a_running_dependency_is_held_and_released_once() {
        let executor = Executor::new(2);
        let (started, set_started) = flag();
        let (release, let_go) = flag();
        let runs = Arc::new(AtomicUsize::new(0));

        let parked = executor.silent_dependent_async(
            move || {
                set_started.store(true, Ordering::Release);
                spin_until("the test thread to let the dependency go", &release);
            },
            [],
        );
        spin_until("the dependency to start running", &started);

        let dependent = executor.silent_dependent_async(
            {
                let runs = Arc::clone(&runs);
                move || {
                    runs.fetch_add(1, Ordering::Release);
                }
            },
            [&parked],
        );

        assert!(!dependent.is_done(), "its dependency is still running");
        assert_eq!(
            dependent.use_count(),
            2,
            "the handle, plus the job parked on the dependency's list"
        );
        assert_eq!(
            runs.load(Ordering::Acquire),
            0,
            "a dependent does not run before its dependency"
        );

        let_go.store(true, Ordering::Release);
        executor.wait_for_all();

        assert!(dependent.is_done());
        assert_eq!(
            runs.load(Ordering::Acquire),
            1,
            "released exactly once, by the tear-down that drained the list"
        );
    }

    /// The mixed case the guard exists for: one dependency over and one still
    /// running. The task waits for the running one, which is what a counter
    /// that could reach zero mid-submission would get wrong.
    #[test]
    fn a_dependent_waits_for_the_last_of_its_dependencies() {
        let executor = Executor::new(2);
        let (started, set_started) = flag();
        let (release, let_go) = flag();

        let over = executor.silent_dependent_async(|| {}, []);
        executor.wait_for_all();
        assert!(over.is_done());

        let parked = executor.silent_dependent_async(
            move || {
                set_started.store(true, Ordering::Release);
                spin_until("the test thread to let the dependency go", &release);
            },
            [],
        );
        spin_until("the dependency to start running", &started);

        let (ran, set_ran) = flag();
        let dependent = executor.silent_dependent_async(
            move || set_ran.store(true, Ordering::Release),
            [&over, &parked],
        );

        assert!(
            !ran.load(Ordering::Acquire),
            "a dependency already met does not make the task ready on its own"
        );
        assert!(!dependent.is_done());

        let_go.store(true, Ordering::Release);
        spin_until("the dependent of the parked dependency", &ran);
        executor.wait_for_all();
    }

    /// A `silent_dependent_async` body's panic is **kept on the task**: there
    /// is no handle on a result to deliver it to and nothing anchored above a
    /// free node, so `Job::record_panic`'s last case parks it on the node,
    /// where the handle reads it back exactly once.
    ///
    /// Upstream's `exception_ptr()`, and #105's half of it: what the payload
    /// must *not* reach is the sentinel run every free node shares, since a
    /// failure recorded there would skip every async task submitted
    /// afterwards for the life of the process. The dependent at the end is
    /// the other half of the claim -- a failed task is still a finished task,
    /// so what was waiting for it runs.
    #[test]
    fn a_silent_dependent_async_that_panics_keeps_its_payload_on_the_task() {
        let executor = Executor::new(2);

        let failed = executor.silent_dependent_async(|| panic!("boom"), []);

        // On `is_done` rather than on `wait_for_all`, because the order that
        // makes the payload readable is what this asserts: the tear-down
        // parks it and only then sets `FINISHED`, so a caller released by
        // that flag can take it. A deadline rather than an open spin, for
        // `GIVE_UP_AFTER`'s reason.
        let deadline = Instant::now() + GIVE_UP_AFTER;
        while !failed.is_done() {
            assert!(
                Instant::now() < deadline,
                "gave up waiting for the failed task to finish"
            );
            thread::yield_now();
        }
        assert!(
            !Topology::free_standing().cancelled(),
            "the run every free node shares took no part in that failure"
        );

        let payload = failed.take_panic().expect("the task's own failure");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
        assert!(failed.take_panic().is_none(), "the payload is taken once");

        let (ran, set_ran) = flag();
        let _after = executor
            .silent_dependent_async(move || set_ran.store(true, Ordering::Release), [&failed]);
        spin_until("the dependent of a failed task", &ran);
        executor.wait_for_all();
    }

    /// Once the task has run and every job of it is gone, the handle is the
    /// last share: upstream's `use_count()` of 1, which is what its own docs
    /// mean by the worker "removing the task from the executor" when it
    /// completes.
    ///
    /// The executor is dropped rather than waited on, and that is the whole
    /// of why this is deterministic: `wait_for_all` returns when the counters
    /// reach zero, which is a line *before* the worker spends the job that
    /// holds the node, while `Drop` joins every worker thread.
    #[test]
    fn the_last_share_of_a_finished_task_is_its_handle() {
        let (task, handle) = {
            let executor = Executor::new(2);
            let task = executor.silent_dependent_async(|| {}, []);
            let (task, handle) = executor.dependent_async(|| 7, [&task]);
            executor.wait_for_all();
            (task, handle)
        };

        assert!(task.is_done());
        assert_eq!(task.use_count(), 1, "the handle is the last share");
        assert_eq!(
            handle.wait().expect("no panic"),
            7,
            "and the result survived the task's node"
        );
    }

    /// A dependent-async handle in a taskflow's graph refuses the run, as the
    /// plain async handle does and through the same channel. #47's case for
    /// the stub this module replaced, kept because the refusal is kept: the
    /// module docs say why a `FnOnce` body with dependencies that are not
    /// edges is not a graph citizen.
    #[test]
    fn a_dependent_async_node_in_a_graph_refuses_the_run() {
        let executor = Executor::new(2);
        let mut taskflow = Taskflow::new();
        taskflow
            .graph_mut()
            .add_named_node("free", Handle::DependentAsync(AsyncWork::plain(|| {})));

        let error = executor
            .run(&taskflow)
            .wait()
            .expect_err("a dependent-async node is refused");
        assert_eq!(
            error.to_string(),
            "a task panicked: a dependent-async node cannot be run as part of a taskflow: \
             a dependent-async task is submitted to the executor, and its dependencies are \
             other async tasks rather than edges of a graph"
        );
    }

    /// The same balance `executor::async` asserts for a plain async task, for
    /// a dependent-async one: a submission under a parent counts nothing in
    /// the executor's own counters and its tear-down spends nothing, so the
    /// parent's join counter is the whole of the accounting.
    ///
    /// Issue #115's first half in this module, where #92's two runtime-shaped
    /// forms and #93's four spawning forms all arrive. The imbalance was the
    /// same line in both places.
    ///
    /// **How a regression surfaces.** As a failed assertion rather than a
    /// hang -- `executor::async`'s case says why the counters are read and
    /// spent rather than waited on, and why the job is driven by hand.
    #[test]
    fn a_parented_dependent_async_task_is_counted_by_its_parent_only() {
        let executor = Executor::new(1);
        let mut worker = WorkerThread::new(Arc::clone(&executor.shared), 0, WorkerQueue::new());

        let parent = Nested::new(None);
        let (ran, set_ran) = flag();

        let task = executor.schedule_dependent_async_task(
            AsyncWork::plain(move || set_ran.store(true, Ordering::Release)),
            &parent.as_parent(),
            ESTATE_NONE,
            // No dependencies, so the submission queues the job itself: a
            // dependency would park it on a node this case would then have to
            // run as well, and the counters are the subject here.
            [],
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
        assert!(task.is_done(), "and the task is finished");
        assert_eq!(
            parent.outstanding(),
            0,
            "the parent's counter is where the child was counted, and the \
             tear-down is what spent it"
        );
    }
}

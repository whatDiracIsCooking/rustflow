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
//! the same free-standing node, submitted the same way from any thread,
//! counted in the same two counters and -- for a handle-returning form --
//! delivering into the same [`AsyncHandle`]. What is particular to this half
//! of the header is that the graph is built **one task at a time, while the
//! tasks already submitted are running**, which no `Taskflow` can do.
//!
//! # Design
//!
//! [`async`](crate::executor::r#async) holds the submission core this module
//! is the dependent half of; the body shapes and the receivers are its
//! business rather than this one's. Two things are this one's.
//!
//! The **protocol**, per dependency named: an *empty* handle is skipped,
//! upstream's `task.empty()` branch of `_process_dependent_async`; a
//! dependency that has already **finished** is counted off the new node's
//! join counter at once, nothing ever draining its list again; and otherwise
//! the new node's [`Job`] goes onto that dependency's list, whose tear-down
//! counts it off. A node whose counter reaches zero is queued by whichever of
//! the submitting thread and a predecessor's tear-down arrives last, which
//! does not matter: every decrement is a `fetch_sub` of its own, so exactly
//! one drives the counter to zero. `AsyncNode::push_successor` and
//! `AsyncNode::finish` are the pair that makes naming a dependency and
//! finishing one atomic against each other, where upstream uses its
//! `ESTATE::LOCKED` spin bit and `ESTATE::FINISHED` in one word.
//!
//! The **guard**, which replaces upstream's `std::distance`: a Rust
//! `IntoIterator` may be single-pass, so the counter cannot start at the
//! number of dependencies. It starts at [`SUBMITTING`] instead, and that one
//! outstanding dependency is spent together with everything found already met
//! in one final `fetch_sub`. Without it a task whose first dependency
//! finished early could run and *finish* before its second was named, leaving
//! a job on a list nothing drains again. `DESIGN.md` has that decision and
//! with it why `Parent::count_queued` is taken at submission here, where a
//! plain async task's is taken beside the push.
//!
//! Nothing here can hang because every queued dependent-async job sets
//! `FINISHED` and drains its list whatever happens to it -- a task left on a
//! list nothing drains again is a count against its parent that is never
//! spent. A parentless job cannot even be skipped, its run being the sentinel
//! and its parent no chain; a job under a parent can, and
//! [`release_skipped`] is what the early-out at the top of
//! `WorkerThread::invoke_one` calls instead. That is upstream's arrangement
//! exactly: its cancelled branch goes to `_tear_down_invoke`, which
//! dispatches on the handle and so runs the *whole* of
//! `_tear_down_dependent_async` where every other node kind owes only the
//! parent branch.
//!
//! A [`Handle::DependentAsync`] somebody built into a taskflow's graph is
//! refused for the reason [`async`](crate::executor::r#async) gives, and
//! doubly so here: the dependencies of such a task are other free nodes
//! rather than edges of any graph.

use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::Ordering;

use crate::async_task::AsyncTask;
use crate::executor::r#async::{AsyncHandle, Spawn, Submission};
use crate::executor::runtime;
use crate::executor::topology::{Job, Parent, Topology};
use crate::executor::worker::{Release, WorkerThread};
use crate::executor::{Executor, Shared};
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
    /// names this task as a dependency of a later one; the [`AsyncHandle`]
    /// is upstream's `std::future<R>` and behaves exactly as
    /// [`Executor::spawn`]'s does, including that dropping it detaches.
    ///
    /// Dependencies are named by handle and from any thread, including from
    /// a task body already running on one of this executor's workers -- which
    /// is how a graph of async tasks is built while it runs. A task already
    /// finished is not waited for, and an **empty** handle is skipped: it
    /// names no task.
    ///
    /// A panic out of `body` is reported from that handle as
    /// [`RunError::Panicked`](crate::RunError::Panicked)`(payload)` and does
    /// **not** cancel the tasks that depend on this one, as upstream's
    /// `std::future` promises too.
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
        self.shared.schedule_dependent_async_task(
            Spawn::handled(body),
            &Submission::parentless(),
            dependencies,
        )
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
    /// [`AsyncTask::take_panic`] hands it back -- upstream's
    /// `exception_ptr()`, and the one place in this port a free node's own
    /// payload slot is both written and readable. It cancels nothing, and the
    /// default panic hook still prints it.
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
        self.shared
            .schedule_dependent_async_task(
                Spawn::silent(body),
                &Submission::parentless(),
                dependencies,
            )
            .0
    }
}

impl Shared {
    /// Creates the free-standing node `spawn` is the body of, counts the
    /// dependencies named against it, and queues it if there are none left to
    /// wait for: upstream's `_schedule_dependent_async_task` plus the
    /// `_process_dependent_async` it calls per dependency.
    ///
    /// The steps that are not the dependency loop are
    /// [`Shared::schedule_async_task`]'s, in that order and for its reasons;
    /// what this adds in the middle is the guard the module docs argue for.
    /// The [`Submission`] places only the push *this* call may make, which is
    /// the one for a task whose dependencies were all met already: a task
    /// released later is queued by whichever worker ran its last predecessor
    /// and was never this submission's to place, which is why upstream's own
    /// branch sits inside its `if(num_predecessors == 0)`.
    pub(crate) fn schedule_dependent_async_task<'a, H>(
        &self,
        spawn: Spawn<H>,
        to: &Submission<'_>,
        dependencies: impl IntoIterator<Item = &'a AsyncTask>,
    ) -> (AsyncTask, H) {
        let (work, estate, handle) = spawn.into_parts();
        let node = self.new_free_node(Handle::DependentAsync(work), estate, to);

        // Upstream's `num_predecessors` as the counter's initial value, with
        // the guard standing in for a count it cannot take in advance.
        // Relaxed, and nothing else is possible to get wrong here: no other
        // thread can name this node yet, because no other thread has it.
        let outstanding = node.node().join_counter();
        outstanding.store(SUBMITTING, Ordering::Relaxed);

        // Upstream's `_node->_join_counter.fetch_add(1)`, which is a line of
        // its `Runtime::dependent_async` rather than of
        // `_schedule_dependent_async_task` -- the parent is told by whoever
        // submits. One count per *task* and not per job: the jobs the loop
        // below may create are copies of the one job that will run it, the
        // rest dropped undequeued, and `WorkerThread::finish_job` spends this
        // count once when that job finishes.
        to.parent().count_queued(Topology::free_standing(), 1);

        // Upstream's `_process_dependent_async` loop. `met` is the
        // dependencies that turned out to be over already -- its
        // `num_predecessors = node->_join_counter.fetch_sub(1) - 1` branch,
        // deferred to one decrement below because under the guard there is
        // nothing to be gained by taking it early.
        let mut met = 0;
        for dependency in dependencies {
            // An empty handle names no task, so it is neither counted nor
            // pushed onto. Skipping it outright is the same arithmetic as
            // upstream's count-then-cancel.
            let Some(predecessor) = dependency.node() else {
                continue;
            };

            // Before the push, and that order is the whole of why the guard
            // exists: the predecessor's tear-down may run this job's
            // decrement the instant the push returns, so the increment it is
            // paying for has to be in the counter first.
            outstanding.fetch_add(1, Ordering::Relaxed);

            // Upstream's spin-locked `_edges.push_back(node)` against its
            // `ESTATE::FINISHED` test, under one lock: `false` is the
            // dependency that had already finished, whose list nothing will
            // drain again, so this submission counts it off itself.
            if !predecessor.push_successor(Job::free(&node, to.parent())) {
                met += 1;
            }
        }

        // Upstream's `if(num_predecessors == 0) _schedule(...)`, which is
        // this decrement reaching zero. One `fetch_sub` for the guard and
        // every met dependency together, so the counter is driven to zero
        // exactly once however many of each there were.
        //
        // `AcqRel`, as upstream's is: the acquire half makes a predecessor's
        // writes visible to whoever is released by this, and the release half
        // publishes the pushes above to the tear-down released by the
        // predecessor's own decrement instead.
        if outstanding.fetch_sub(met + SUBMITTING, Ordering::AcqRel) == met + SUBMITTING {
            to.queue(self, Job::free(&node, to.parent()));
        }

        // Upstream's `AsyncTask task(animate(...))` is taken at the *top* of
        // its function, because the count it takes is what keeps the node
        // alive while the loop runs. Here the local `node` is that share and
        // this is it being handed over.
        (AsyncTask::new(node), handle)
    }
}

/// Runs the dependent-async task `job` is for and releases whatever was
/// waiting for it: upstream's `DEPENDENT_ASYNC` arm of `_invoke`, which is
/// `_invoke_dependent_async_task` followed by `_tear_down_dependent_async`.
///
/// `Some(Release::Strong)` for a task that is over, which buys the tear-down
/// `WorkerThread::invoke_one` performs on the way out, as the plain async arm
/// does. `None` is upstream's `if(_invoke_dependent_async_task(...)) return;`
/// -- a runtime-shaped body that preempted its own node -- and neither the
/// successors nor the counters are touched then: a task that has not finished
/// must not release what is waiting for it.
pub(crate) fn invoke(worker: &mut WorkerThread, job: &Job, work: &AsyncWork) -> Option<Release> {
    // A handle somebody built into a graph, told apart from a free node's job
    // by the identity of the run and refused through the channel a panicking
    // body uses, so that the run is failed rather than hung: `executor::async`
    // has the whole of that argument.
    if !job.topology().is_free_standing() {
        job.topology().record_panic(Box::new(
            "a dependent-async node cannot be run as part of a taskflow: \
             a dependent-async task is submitted to the executor, and its \
             dependencies are other async tasks rather than edges of a graph",
        ));
        // `WorkerThread::invoke_one` skips its own generic call for this
        // handle kind -- see its `async_dispatch` -- so every return out of
        // this function makes its own, this one included.
        worker.observer_epilogue(job);
        return Some(Release::Strong);
    }

    let release = match work {
        // Upstream's case 0, `void()`: a `silent_dependent_async` body or the
        // wrapper a `dependent_async` submits, under the `catch_unwind` that
        // is its `TF_EXECUTOR_EXCEPTION_HANDLER`, and taken out of the slot
        // for the reason `executor::async`'s own arm gives.
        AsyncWork::Plain(body) => {
            if let Some(body) = body.take()
                && let Err(payload) = panic::catch_unwind(AssertUnwindSafe(body))
            {
                // Upstream's `_process_exception`, and for this node kind the
                // walk's last case is the useful one: nothing is anchored
                // above a task submitted to the executor, so the payload
                // stays on the node, where `AsyncTask::take_panic` reads it
                // back -- before `finish` below, which is what lets a caller
                // that has seen `is_done` take it.
                job.record_panic(payload);
            }
            Some(Release::Strong)
        }
        // Upstream's cases 1 and 2, its two `_invoke_runtime_task_impl`
        // overloads and this port's `executor::runtime`: the node kind
        // decides the protocol, not the shape of the body.
        AsyncWork::Runtime(_) | AsyncWork::Reentrant { .. } => {
            runtime::invoke_async(worker, job, work)
        }
    };

    // Upstream's bare `return` out of the arm: the node is preempted, so it
    // has not finished and owes nothing yet. Still makes the one call this
    // function owes `WorkerThread::invoke_one` -- see `async_dispatch`
    // there -- the second entry being what actually releases anything.
    let Some(release) = release else {
        worker.observer_epilogue(job);
        return None;
    };

    // Before `release_successors` and `finish_async` below, not after:
    // both can let something waiting on this task -- a successor, an
    // `Executor::wait_for_all`, an `Executor::drop` -- run before this call
    // would otherwise have, on a thread with nothing else ordering it
    // against this one's own. #167's `tests/observers.rs` caught the
    // `finish_async` half of this the other way round.
    worker.observer_epilogue(job);

    release_successors(worker, job);

    // Upstream's parentless branch of `_tear_down_dependent_async`, under the
    // condition `Shared::new_free_node` increments under. The counters go
    // *after* the release above, as upstream orders them: a successor is
    // already counted in them on its own behalf, so the order cannot let
    // `wait_for_all` return early either way, and keeping upstream's is one
    // less thing for a reader to check.
    if matches!(job.parent(), Parent::Run) {
        worker.shared.finish_async();
    }

    Some(release)
}

/// Releases whatever was waiting for `job`'s task when that job is one the
/// dispatch **skipped**, and does nothing for a job of any other kind.
///
/// The half of upstream's `_tear_down_invoke` that is not
/// `WorkerThread::finish_job`. Called by that early-out for every job it
/// skips, which is why the two questions are asked here rather than there:
/// the node kind, and whether this is a free node at all -- a
/// [`Handle::DependentAsync`] built into a taskflow's graph has no successor
/// list to drain, the list being a *free* node's state. Why it matters is the
/// module docs' paragraph on hanging: the tasks waiting on a skipped one were
/// counted against its parent when they were submitted, so leaving them there
/// means a parent whose join counter never reaches zero.
pub(crate) fn release_skipped(worker: &mut WorkerThread, job: &Job) {
    if job.async_node().is_some() && matches!(job.node().handle(), Handle::DependentAsync(_)) {
        release_successors(worker, job);
    }
}

/// Marks the task finished and queues whatever that made ready: the rest of
/// upstream's `_tear_down_dependent_async`, minus the parent branch its
/// caller keeps.
///
/// The two steps are `AsyncNode::finish`, which sets `FINISHED` under the
/// successor lock and hands the list back, and then the release -- outside
/// the lock, which is this port's one departure from upstream's shape here:
/// upstream releases under its spin bit because its scheduling path is
/// lock-free, and a job pushed to a queue may be stolen and run before this
/// call returns, so whatever it does must not be able to wait on the lock it
/// was released under.
///
/// A released successor goes onto *this* worker's queue, where upstream's
/// `_update_cache` would have kept one for the frame below: that cache is a
/// local of `WorkerThread::invoke_one` filled from graph successors a free
/// node does not have, so the difference is one push and one pop per chain
/// link.
pub(crate) fn release_successors(worker: &mut WorkerThread, job: &Job) {
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
        // zero: every decrement of this counter was paid for by an increment
        // the submitting call made before pushing the job, and nothing can
        // bypass a free node's dependencies. The assertion is in debug
        // builds, where the crate's own tests run, so a protocol that broke
        // that invariant would fail rather than hang.
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
            // else: the submission counted it against its parent then,
            // because it could not know which thread would queue it or when.
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

        let (task, ()) = executor.shared.schedule_dependent_async_task(
            Spawn::silent(move || set_ran.store(true, Ordering::Release)),
            &Submission::under_anchor(&parent),
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

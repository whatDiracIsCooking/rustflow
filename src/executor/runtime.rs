//! Translated from `taskflow/core/runtime.hpp` (Taskflow v4.1.0).
//!
//! The executor's half of runtime tasking: upstream's `_invoke_runtime_task`
//! and **both** overloads of `_invoke_runtime_task_impl`, which that header
//! defines out of line, plus the scheduling methods of `tf::Runtime` --
//! `schedule`, `corun`, `corun_all` and `is_cancelled`. [`Runtime::task_group`]
//! is here as well and is **not** one of them: upstream opens a group on its
//! executor, which reaches the submitting worker through a thread-local
//! `this_worker()` where this port cannot, so the branch that pushes onto
//! that worker's own queue is reachable only from the handle holding its
//! scheduling loop. `src/task_group.rs` has the two routes. The `RUNTIME`
//! arm of `_invoke` in `taskflow/core/executor.hpp` is one line into [`invoke`]
//! below. The shell of [`Runtime`] is `src/runtime.rs`, which says what the
//! handle holds and why it cannot hand back an executor.
//!
//! Upstream's second class, `NonpreemptiveRuntime`, is also here, in full:
//! [`invoke_nonpreemptive`] is `_invoke_nonpreemptive_runtime_task`, and
//! `NonpreemptiveRuntime` itself -- the restricted handle, not part of this
//! crate's public surface -- is this module's own, with no shell in
//! `src/runtime.rs` to split it from. `DESIGN.md`'s "Pipelines" section has
//! why it took a second milestone to arrive rather than the first.
//!
//! # Design
//!
//! Three body shapes share one protocol. A runtime task of a graph is one of
//! them; the other two are the runtime-shaped *async* bodies, upstream's
//! `void(tf::Runtime&)` and `void(tf::Runtime&, bool)` alternatives, which
//! are cases 1 and 2 of `_invoke_async_task` and which it dispatches to the
//! very same pair of overloads. [`invoke_async`] is that dispatch, and the
//! submitting calls for those two shapes are the `is_runtime_task_v<F>`
//! branches of `_async` and `_silent_async` -- here rather than beside the
//! plain forms in `executor::async` because what is particular to them is
//! this protocol and not the submission, which is one delegation like every
//! other in the family.
//!
//! The protocol is the preemption protocol the [`executor`](crate::executor)
//! module docs set out, with one difference from a module or a subflow task:
//! what the node waits for is not a *graph* but however much asynchronous
//! work the body chose to spawn, so the count cannot be known before the body
//! has run. Upstream's answer is to make the body itself one reference. The
//! first entry sets `PREEMPTED | IMPLICITLY_ANCHORED` and raises the node's
//! own join counter **before** the body runs, because a child the body spawns
//! raises that same counter and the bits are what tell the last child to
//! re-queue this node; then it spends its own reference, which either finds
//! nothing else outstanding -- the bits come off and the node finishes like a
//! static task -- or leaves the node owned by children that are still
//! running, the last of which re-queues the job it was dequeued as.
//!
//! The `void(Runtime&, bool)` shape is the one that is *called* twice:
//! upstream hands one lambda a `reentered` flag, `false` to run the user's
//! body and `true` to read `rt._node->_exception_ptr` into the promise behind
//! the future. This port splits that into the two `FnOnce`s
//! [`AsyncWork::Reentrant`] holds, and [`invoke_async`] runs `complete` on
//! the entry that **finishes** the node -- the second one when the body left
//! work outstanding, the first one when it did not, which is upstream's
//! `work(rt, true)` after its `if`/`else`: exactly once, whichever branch got
//! there.
//!
//! Two consequences are worth stating where no signature shows them. The arm
//! answers with a value rather than letting its caller read `nstate` back,
//! which is upstream's own comment on returning a `bool`: the instant the
//! join counter drops to something non-zero a child may finish, re-queue this
//! node and have another worker clear those very bits, so the answer has to
//! be what the first entry computed while it still held a reference. And
//! [`Runtime::try_corun`] waits for the counter to come back to **one**, that
//! one being the body's own reference, which is upstream's predicate exactly
//! -- so a body that spawned nothing returns at once, and what raises the
//! counter above one is the `async` family in `executor::runtime_async`,
//! where a task the body [`schedule`](Runtime::schedule)s is counted against
//! the node's own parent and is a sibling rather than a child.
//!
//! Where a failure under each of the three shapes goes is one argument in
//! `DESIGN.md` and a few lines of difference here. All three are implicit
//! anchors for exactly as long as they are entered -- the bit goes on before
//! the body runs and comes off only once everything the body spawned has been
//! counted off -- so a failure under any of them lands in the node's own
//! slot, which is the slot [`Runtime::try_corun`] reads. What differs is what
//! becomes of a payload the body never collected, and [`enter`] settles it on
//! the way out: the handle-returning form is explicitly anchored from
//! construction as well, so the payload stays on its node and `complete`
//! hands it to the handle; for the other two the walk is run again from a
//! node that is no longer an anchor, which delivers the payload to whatever
//! is waiting for the node's own graph -- the run, for a runtime task of a
//! graph -- or leaves it on the node to be dropped with it, for a free silent
//! one whose chain roots at the sentinel.
//!
//! [`invoke_nonpreemptive`] shares none of this. It sets no bit, takes no
//! reference, and never returns `None`, so it is not a fourth shape of the
//! protocol above but the deliberate absence of one -- upstream's own
//! `NonpreemptiveRuntime` is built the same way, for the same reason: a node
//! with no protocol to resume it can be the target of
//! [`Runtime::schedule`] at any point in its body's run, which is what the
//! family above cannot promise.
//!
//! [`NonpreemptiveRuntime`]'s declared visibility is `pub` rather than the
//! `pub(crate)` its real reachability would suggest, because
//! [`FlowBuilder::emplace_nonpreemptive_runtime`](crate::FlowBuilder::emplace_nonpreemptive_runtime)
//! is a genuinely public method whose bound names the type: `rustc`'s
//! `private_bounds` lint checks a bound's own declared visibility against its
//! caller's, not the full path's reachability, and a body closure never needs
//! to write the type's path to satisfy that bound regardless -- the same way
//! a closure passed to a bound of `Fn(&mut SomeType)` never has to spell
//! `SomeType` to compile against it.

use core::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::Runtime;
use crate::executor::Executor;
use crate::executor::r#async::{AsyncHandle, Spawn, Submission};
use crate::executor::run_handle::RunError;
use crate::executor::topology::{Cancellable, Job};
use crate::executor::worker::{Release, Worker, WorkerThread, is_entered};
use crate::graph::{AsyncWork, Node, NonpreemptiveRuntimeWork, OnceWork, RuntimeWork, Task};
use crate::task_group::TaskGroup;

/// The two bits a runtime task holds while its own work is outstanding:
/// upstream's `NSTATE::PREEMPTED | NSTATE::IMPLICITLY_ANCHORED`, which it
/// sets and clears together in all three of its runtime invokers.
const ENTERED: u32 = Node::PREEMPTED | Node::IMPLICITLY_ANCHORED;

/// Runs the runtime task `job` is for: upstream's `_invoke_runtime_task` and
/// the `void(Runtime&)` overload of `_invoke_runtime_task_impl`.
///
/// `None` means the node has been preempted -- work the body spawned is still
/// running, and the last of it to finish will re-queue this job -- which is
/// what keeps `WorkerThread::invoke` from re-arming the node or releasing a
/// successor that has the body's own children still to wait for. A panic out
/// of the body is recorded as a static task's is, through
/// [`Job::record_panic`], and the bookkeeping in [`enter`] still runs.
pub(crate) fn invoke(worker: &mut WorkerThread, job: &Job, work: &RuntimeWork) -> Option<Release> {
    // Called *through* the slot rather than taken out of it, which is the one
    // way a graph's runtime task differs from the two async shapes below: a
    // graph may be run any number of times, so its body is a `FnMut` that
    // stays where it is. `RuntimeWork::invoke` is what arbitrates the one
    // worker per node that promise rests on.
    enter(worker, job, |runtime| work.invoke(runtime))
}

/// Runs a runtime-shaped async body: upstream's cases 1 and 2 of
/// `_invoke_async_task`, which are the two `_invoke_runtime_task_impl`
/// overloads over the body of a free-standing node.
///
/// The answer is [`invoke`]'s and means the same thing.
pub(crate) fn invoke_async(
    worker: &mut WorkerThread,
    job: &Job,
    work: &AsyncWork,
) -> Option<Release> {
    match work {
        // Upstream's case 0, which is `executor::async`'s own arm: both
        // callers match the two runtime-shaped alternatives before calling
        // here, exactly as upstream's two switches do. Named rather than
        // quietly handled, because a third caller that forgot to would
        // otherwise run a plain body under a protocol that preempts its node.
        AsyncWork::Plain(_) => {
            unreachable!("a plain async body is not a runtime-shaped one: see `executor::async`")
        }
        // Upstream's case 1, the silent shape: one entry's worth of body and
        // no second call, because nobody is waiting for an answer.
        AsyncWork::Runtime(body) => enter(worker, job, |runtime| take_and_run(body, runtime)),
        // Upstream's case 2. The `complete` half is its `work(rt, true)`,
        // after the `if`/`else` and therefore on the entry that finished the
        // node -- never on an entry that preempted it, which is what the `?`
        // says: a promise fulfilled then would be fulfilled before the work
        // it is a promise about was done.
        AsyncWork::Reentrant { body, complete } => {
            let release = enter(worker, job, |runtime| take_and_run(body, runtime))?;

            // Upstream's `auto& eptr = rt._node->_exception_ptr; eptr ?
            // p.set_exception(eptr) : p.set_value()`, with the node reached
            // through the job because that is the one thing the second entry
            // has. `None` would mean the slot had already been spent, which
            // takes a node entered twice without being preempted in between;
            // doing nothing is the one answer to that which cannot fulfil
            // one promise twice.
            if let Some(complete) = complete.take() {
                complete(job.node().base().take_panic());
            }
            Some(release)
        }
    }
}

/// Runs `body` on the node `job` is for under upstream's preemption protocol:
/// the shared half of both `_invoke_runtime_task_impl` overloads, which differ
/// in nothing else. The module docs' `# Design` section is the argument for
/// every line of it; the answer is the `bool` upstream returns, inverted into
/// the `Option<Release>` the dispatch wants, `None` being upstream's `true`.
fn enter(
    worker: &mut WorkerThread,
    job: &Job,
    body: impl FnOnce(&mut Runtime<'_>),
) -> Option<Release> {
    let node = job.node();

    // Upstream's `if((node->_nstate & NSTATE::PREEMPTED) == 0)`: the first
    // entry, which is the one that runs the body.
    if node.nstate().load(Ordering::Relaxed) & Node::PREEMPTED == 0 {
        // Both before the body, as upstream sets them: a child the body
        // spawns may finish before the body returns, and these are what tell
        // it that this node is waiting to be resumed. Relaxed, as upstream's
        // plain `|=` is -- what orders a child's read of them is the queue it
        // came out of.
        node.nstate().fetch_or(ENTERED, Ordering::Relaxed);

        // The body's own reference, upstream's `fetch_add(1, release)`: it is
        // what keeps the counter off zero while the body is still spawning,
        // so that a child which finishes early cannot resume a node that has
        // not finished starting.
        node.join_counter().fetch_add(1, Ordering::Release);

        run_body(worker, job, node, body);

        // Upstream's `fetch_sub(1, acq_rel) == 1`: this reference spent, and
        // the question of whether anything else holds one. The acquire half
        // is what makes everything the children published visible to the
        // frame that falls through here.
        if node.join_counter().fetch_sub(1, Ordering::AcqRel) != 1 {
            // Upstream's `return true`. The node keeps both bits until the
            // second entry clears them.
            return None;
        }
    }

    // Upstream's two `_nstate &= ~(PREEMPTED | IMPLICITLY_ANCHORED)` sites,
    // which are the same line twice: the first entry that found itself last
    // clears the bits it set, and the second entry clears the ones the first
    // one left behind. It is also the one exit this node has from being an
    // implicit anchor, which is what the forward below is about.
    node.nstate().fetch_and(!ENTERED, Ordering::Relaxed);

    // A payload nobody came for goes on up, the walk asked again from a node
    // that has stopped being an anchor -- the `# Design` section has where
    // each of the three shapes delivers it. *After* the
    // bits come off, which is why it is not two lines earlier: a walk that
    // still found this node implicitly anchored would hand the payload
    // straight back.
    //
    // `CAUGHT` before the lock, which is `set_up_graph`'s trick in
    // `executor::topology`: a slot is only ever filled by a claimant that set
    // that bit first, so a runtime task that failed at nothing pays a load
    // rather than a mutex. Relaxed because the bit was published before the
    // decrement this frame has already observed -- the `AcqRel` `fetch_sub`
    // above on the first entry, the queue the resumed job came out of on the
    // second.
    if node.base().estate().load(Ordering::Relaxed) & Cancellable::CAUGHT != 0
        && let Some(payload) = node.base().take_panic()
    {
        job.record_panic(payload);
    }

    Some(Release::Strong)
}

/// Runs the body against a handle over `node`, recording a panic out of it as
/// this job's failure: upstream's `TF_EXECUTOR_EXCEPTION_HANDLER(worker,
/// node, { work(rt); })`.
///
/// A function of its own so that the mutable borrow the handle takes of the
/// worker's scheduling loop ends where the body does, which is before
/// [`enter`] spends the node's own reference.
fn run_body(
    worker: &mut WorkerThread,
    job: &Job,
    node: &Node,
    body: impl FnOnce(&mut Runtime<'_>),
) {
    // The worker record the handle hands back, taken through a share of the
    // executor's state rather than through `worker`: the handle holds that
    // value mutably, so a borrow of the record *out of* it would alias the
    // handle's own. One atomic pair per runtime task, which is what keeps
    // `Runtime::worker` able to lend the record out for the whole invocation.
    let shared = Arc::clone(&worker.shared);
    let record: &Worker = &shared.workers[worker.id];

    let mut runtime = Runtime::new(record, node, job, worker);
    // `AssertUnwindSafe` is the honest annotation: the state a panicking body
    // can leave half-written is the caller's own, reachable only through what
    // the closure captured.
    if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| body(&mut runtime))) {
        job.record_panic(payload);
    }
}

/// The slot an async task's runtime-shaped body sits in until the one worker
/// that runs it takes it out: [`AsyncWork::Runtime`]'s payload and the `body`
/// half of [`AsyncWork::Reentrant`]. Spelled out rather than named, `graph`'s
/// own alias for it being private to that module.
type AsyncRuntimeSlot = OnceWork<dyn for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static>;

/// Takes an async task's body out of `slot` and runs it against `runtime`.
///
/// Taken rather than called through, an async body being a `FnOnce`, as the
/// plain arm in `executor::async` takes it: `None` is a body already spent,
/// which one submission and one job cannot reach.
fn take_and_run(slot: &AsyncRuntimeSlot, runtime: &mut Runtime<'_>) {
    if let Some(body) = slot.take() {
        body(runtime);
    }
}

impl Executor {
    /// Runs `body` on this executor as soon as a worker picks it up, handing
    /// it the scheduler, and hands back a handle on its completion.
    ///
    /// Upstream's `async` over a `[](tf::Runtime&){}` body -- the
    /// `is_runtime_task_v<F>` branch of `_async` -- under
    /// [`Executor::spawn`]'s name, whose docs hold here too: where the body
    /// runs, dropping the handle to detach, awaiting it as a [`Future`].
    ///
    /// The body is handed a [`Runtime`], so it can do what a runtime task of
    /// a graph can do: spawn children of its own and [`Runtime::corun`] on
    /// them, with [`Runtime::schedule`]'s one limit. The handle therefore
    /// carries no value -- upstream's own `std::promise<void>` -- what there
    /// is to report about a runtime-shaped task being **whether anything
    /// under it failed**. [`AsyncHandle::wait`] answers `Ok(())` once the
    /// body *and* everything it spawned are done, and
    /// [`RunError::Panicked`], the first such failure only, if any of that
    /// panicked.
    ///
    /// ```
    /// use rustflow::{Executor, Runtime};
    ///
    /// let executor = Executor::new(2);
    ///
    /// // The body is handed the scheduler rather than returning a value.
    /// let task = executor.spawn_runtime(|runtime: &mut Runtime<'_>| {
    ///     assert_eq!(runtime.num_workers(), 2);
    ///     runtime.corun();
    /// });
    /// task.wait().expect("the body did not panic");
    ///
    /// // A panic out of it is this handle's failure and nobody else's.
    /// let failed = executor.spawn_runtime(|_: &mut Runtime<'_>| panic!("not today"));
    /// let error = failed.wait().expect_err("the body panicked");
    /// assert_eq!(error.to_string(), "a task panicked: not today");
    ///
    /// // And the executor is unharmed by it.
    /// assert_eq!(executor.spawn(|| 1).wait().expect("no panic"), 1);
    /// ```
    pub fn spawn_runtime<F>(&self, body: F) -> AsyncHandle<()>
    where
        F: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static,
    {
        self.shared
            .schedule_async_task(Spawn::handled_runtime(body), &Submission::parentless())
    }

    /// Runs `body` on this executor as soon as a worker picks it up, handing
    /// it the scheduler, with no handle on it at all.
    ///
    /// Upstream's `silent_async` over a `[](tf::Runtime&){}` body -- the
    /// `is_runtime_task_v<F>` branch of `_silent_async` -- and the cheaper
    /// half of the pair, for
    /// [`Executor::silent_async`](crate::Executor::silent_async)'s reason.
    /// There is no second entry either, upstream's case 1 against case 2.
    ///
    /// A panic out of `body` -- or out of anything it spawned -- is
    /// **dropped**: the payload lands in the node's own slot and goes with
    /// the node, no handle having been handed out to read it from, though the
    /// default panic hook still prints it. [`Executor::wait_for_all`] is the
    /// one thing that waits for such a task.
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
    /// for _ in 0..100 {
    ///     let counter = Arc::clone(&counter);
    ///     executor.silent_async_runtime(move |runtime: &mut Runtime<'_>| {
    ///         // Nothing was spawned, so this returns at once -- and the
    ///         // node finishes on its first entry.
    ///         runtime.corun();
    ///         counter.fetch_add(1, Ordering::Relaxed);
    ///     });
    /// }
    ///
    /// executor.wait_for_all();
    /// assert_eq!(counter.load(Ordering::Relaxed), 100);
    /// ```
    pub fn silent_async_runtime<F>(&self, body: F)
    where
        F: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static,
    {
        self.shared
            .schedule_async_task(Spawn::silent_runtime(body), &Submission::parentless());
    }
}

impl Runtime<'_> {
    /// Schedules `task` onto this worker's queue at once, bypassing the
    /// graph's dependencies: upstream's `Runtime::schedule`.
    ///
    /// `task` must be an *active* task -- a task of the taskflow this runtime
    /// task is itself part of -- and of **the same graph as the runtime
    /// task**, the limit `DESIGN.md` argues for: the parent to count it
    /// against is this node's own parent. Not fully checkable, a [`Task`]
    /// being a bare index; what is checked is every index this graph lacks.
    ///
    /// Zeroing the target's join counter bypasses what it owes and does not
    /// remove the edges that made it owe anything, so a strong predecessor
    /// that has yet to run still releases the target onto that zero. The
    /// decrement floors at zero rather than wrapping, and a release onto a
    /// node that has already *preempted* itself is refused -- `DESIGN.md` has
    /// both halves of that, and the third ordering it leaves alone.
    ///
    /// # Panics
    ///
    /// Panics if `task` is not a live task of this runtime task's own graph
    /// -- out of range, erased, or a task of another graph with an index this
    /// one does not have. The panic is
    /// [`Graph::node`](crate::Graph::node)'s.
    ///
    /// Panics if `task` names a node that has already preempted itself, which
    /// is the refusal above. An *ordinary* `Handle::Runtime` task is not safe
    /// against this call for as long as its body merely runs, with no child
    /// spawned at all, `enter` setting the bit before the body runs whether
    /// the body ever spawns one or not; [`Handle::NonpreemptiveRuntime`] is
    /// the one callee that is always safe, whatever its body is doing.
    /// `DESIGN.md`'s entry on `Runtime::schedule`'s hole has why.
    ///
    /// Either reaches the caller as the failure of this runtime task, since
    /// it happens inside the body.
    ///
    /// [`Handle::NonpreemptiveRuntime`]: crate::graph::Handle::NonpreemptiveRuntime
    pub fn schedule(&mut self, task: Task) {
        schedule_bypassing(self.job, self.thread, task);
    }

    /// Waits for everything this runtime task has spawned, reporting a
    /// failure under it rather than raising it: upstream's `corun`, in the
    /// `try_` shape the crate's other waiting surfaces have.
    ///
    /// The calling worker stays in the work-stealing loop while it waits, for
    /// the reason the [`corun`](crate::executor::corun) module docs give. The
    /// predicate is the `# Design` section's -- the node's join counter back
    /// to one -- so a body that has spawned nothing returns immediately; what
    /// keeps that *equality* safe is the floored decrement and the refused
    /// release [`Runtime::schedule`] points at.
    ///
    /// Taking the payload clears the node's slot, as upstream's
    /// `_rethrow_exception` does, so a second corun of the same body starts
    /// clean.
    ///
    /// A deep recursion is the caller's bound to respect, since an overflow of
    /// the worker's stack aborts the process rather than reaching this `Err`:
    /// a job picked up while waiting runs on the waiting frame, and this route
    /// nests one such frame per level of *recursion*, a child submitted here
    /// going onto this worker's own queue for the wait to pop straight back.
    /// `DESIGN.md`'s "Executor core" has the bound, and what makes
    /// [`TaskGroup::corun`](crate::TaskGroup::corun)'s a much lower one on
    /// the route that holds a worker id rather than the worker.
    ///
    /// # Errors
    ///
    /// [`RunError::Panicked`] if work spawned by this runtime task panicked,
    /// carrying what it panicked with. Only the first such failure is
    /// reported, as everywhere else in the crate.
    pub fn try_corun(&mut self) -> Result<(), RunError> {
        let node = self.node;
        {
            // Taken before the wait, and dropped before the payload is read
            // -- `corun_graph` in `executor::corun` takes the same guard in
            // the same order, and says why it has to come first.
            let _anchor = node.base().anchor_explicitly();
            self.thread
                .corun_until(|| node.join_counter().load(Ordering::Acquire) == 1);
        }

        match node.base().take_panic() {
            Some(payload) => Err(RunError::Panicked(payload)),
            None => Ok(()),
        }
    }

    /// Waits for everything this runtime task has spawned, re-raising a
    /// failure under it on this worker: upstream's `corun`.
    ///
    /// [`Runtime::try_corun`] with the failure raised instead of returned,
    /// which is the shape that composes: the failure propagates out of the
    /// body, where the executor catches it as this task's own. The stack bound
    /// it states holds here too.
    ///
    /// # Panics
    ///
    /// Panics with the payload of the first piece of work spawned by this
    /// runtime task to panic. The payload keeps its identity, going to
    /// [`std::panic::resume_unwind`] rather than to a fresh `panic!`.
    pub fn corun(&mut self) {
        match self.try_corun() {
            Ok(()) => {}
            // Upstream's `_node->_rethrow_exception()`, which is the last
            // thing its `corun` does for the same reason it is the last thing
            // here: the anchor guard is gone and the slot is empty, so
            // nothing this frame owed is still outstanding when the unwind
            // starts.
            Err(RunError::Panicked(payload)) => panic::resume_unwind(payload),
        }
    }

    /// [`Runtime::corun`], under upstream's older name for it: `corun_all` is
    /// one line there, kept "just an alias for legacy purpose", and ported
    /// because upstream's own tests and documentation still use it.
    pub fn corun_all(&mut self) {
        self.corun();
    }

    /// Whether this runtime task's run has been cancelled or something above
    /// it has failed: upstream's `is_cancelled`, which is
    /// `Node::_is_parent_cancelled`.
    ///
    /// The one query a long-running body can use to stop early, and the whole
    /// of what cancellation can do to a task that is already running: nothing
    /// interrupts a body, so a body that means to be cancellable has to ask.
    /// Both terms of upstream's condition are here -- the run's own
    /// cancellation bits and the estate of every parent above this job -- so
    /// a runtime task inside a nested graph sees a failure in that graph too.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.job.is_parent_cancelled()
    }

    /// Keeps the worker running this runtime task's body in the
    /// work-stealing loop until `predicate` returns `true`.
    ///
    /// Upstream's `rt.executor().corun_until(predicate)`: the same primitive
    /// [`Executor::corun_until`] gives any thread, with that method's one
    /// check dropped -- a runtime task's body is already running on a worker,
    /// so it cannot be the non-worker caller
    /// [`CorunError::NotAWorker`](crate::CorunError::NotAWorker) is for,
    /// which is what makes this infallible rather than a `Result`.
    ///
    /// `scan.hpp`'s own comment on why the method is public rather than
    /// private to one algorithm is the reason this exists at all: "need to
    /// do public corun because multiple workers can call this".
    ///
    /// `predicate` runs on the calling worker between jobs and may run many
    /// times; the two rules [`Executor::corun_until`] states hold here too --
    /// it must not block, and it must eventually return `true`.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::prelude::*;
    /// use rustflow::{Executor, Runtime};
    ///
    /// // A barrier every one of four runtime tasks reaches before any of
    /// // them is allowed past it: `scan.hpp`'s own shape for a prefix pass.
    /// const WORKERS: usize = 4;
    /// let arrived = Arc::new(AtomicUsize::new(0));
    /// let past = Arc::new(AtomicUsize::new(0));
    ///
    /// let mut taskflow = Taskflow::new();
    /// for _ in 0..WORKERS {
    ///     let arrived = Arc::clone(&arrived);
    ///     let past = Arc::clone(&past);
    ///     taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
    ///         if arrived.fetch_add(1, Ordering::AcqRel) + 1 == WORKERS {
    ///             // The last one through releases everybody else.
    ///             arrived.store(0, Ordering::Release);
    ///         } else {
    ///             runtime.corun_until(|| arrived.load(Ordering::Acquire) == 0);
    ///         }
    ///         past.fetch_add(1, Ordering::Relaxed);
    ///     });
    /// }
    ///
    /// Executor::new(WORKERS)
    ///     .run(&taskflow)
    ///     .wait()
    ///     .expect("no task panicked");
    /// assert_eq!(past.load(Ordering::Relaxed), WORKERS);
    /// ```
    pub fn corun_until(&mut self, predicate: impl FnMut() -> bool) {
        self.thread.corun_until(predicate);
    }

    /// Opens a task group that holds this worker's queue: a scope to spawn
    /// asynchronous work into and wait for with
    /// [`TaskGroup::corun`](crate::TaskGroup::corun), whose children go onto
    /// the queue of the worker running this body and whose corun pops that
    /// same end.
    ///
    /// Upstream has one group constructor and this is its mechanism: its
    /// `_schedule_async_task` finds the submitting worker through
    /// `this_worker()` and pushes onto `w._wsq`, which a method on
    /// [`Executor`] cannot do here, the owning end of that queue living in
    /// the frame below the body. A [`Runtime`] is what holds that frame, so
    /// it is the surface the branch is reachable from;
    /// [`Executor::task_group`] stays the route for a body that was handed no
    /// `Runtime`, and `DESIGN.md`'s "Dynamic tasking" has what separates the
    /// two -- a recursion of groups through this one nests a stack frame per
    /// level of recursion rather than per outstanding task.
    ///
    /// The group borrows this handle for as long as it lives, which is the
    /// one thing a caller has to arrange around: while one is open, the way
    /// to this handle is
    /// [`TaskGroup::with_runtime`](crate::TaskGroup::with_runtime), which
    /// lends it back for a scope, and that is where a body spawns siblings,
    /// [`schedule`](Runtime::schedule)s, coruns its own node or runs the
    /// inline branch of a recursion. There is no error to
    /// handle, where
    /// [`Executor::task_group`] has one: holding a `Runtime` is already proof
    /// of a worker. [`TaskGroup::executor`](crate::TaskGroup::executor)
    /// answers `None` on a group opened here.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::prelude::*;
    /// use rustflow::{Executor, Runtime};
    ///
    /// let counter = Arc::new(AtomicUsize::new(0));
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime({
    ///     let counter = Arc::clone(&counter);
    ///     move |runtime: &mut Runtime<'_>| {
    ///         let group = runtime.task_group();
    ///
    ///         for _ in 0..100 {
    ///             let counter = Arc::clone(&counter);
    ///             group.silent_async(move || {
    ///                 counter.fetch_add(1, Ordering::Relaxed);
    ///             });
    ///         }
    ///
    ///         // The one thing that waits for a group's children, here on
    ///         // the very worker whose queue they were pushed onto.
    ///         group.corun();
    ///         assert_eq!(group.size(), 0);
    ///     }
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(counter.load(Ordering::Relaxed), 100);
    /// ```
    #[must_use]
    pub fn task_group(&mut self) -> TaskGroup<'_> {
        // Upstream's `TaskGroup(*this, *w)` with its `this_worker()` lookup
        // answered by the handle: this *is* worker `w`, and the loop the
        // group needs to push onto and pop is the one below this body. A
        // reborrow of the whole handle rather than of its worker alone,
        // because the group lends it back to `TaskGroup::with_runtime`.
        TaskGroup::on_worker(Runtime::new(
            self.worker,
            self.node,
            self.job,
            &mut *self.thread,
        ))
    }
}

/// The shared half of [`Runtime::schedule`] and [`NonpreemptiveRuntime::schedule`]:
/// upstream keeps two copies of this body, one on each class, and this port
/// keeps one function both call instead, so the assert's wording and the
/// accounting cannot drift between the two callers.
fn schedule_bypassing(job: &Job, thread: &mut WorkerThread, task: Task) {
    // Upstream's comment, and the invariant it names: "when scheduling a
    // task, the task must have zero dependency (join counter is 0) or we
    // can encounter bug when inserting a nested flow (e.g., module task)". A
    // stored zero rather than an arithmetic one, for the reason a
    // conditioner's selected successor gets one: the count the target is
    // carrying is exactly what this call means to bypass.
    let target = job.graph().node(task);

    // The one case this call can see for itself: the store below would wipe
    // a count that is not a dependency count at all -- a preempted node's
    // counter holds one reference per piece of nested work still
    // outstanding, and work that finished against a zeroed counter would
    // drive it past zero rather than resume the node. Refused before
    // anything is written, so a body that schedules a node it should not
    // have fails *as a body* rather than leaving the hang the store would
    // have.
    assert!(
        !is_entered(target),
        "a task that is already running cannot be scheduled: a bypassed \
         dependency may not re-enter a task the run has already entered"
    );

    target.join_counter().store(0, Ordering::Relaxed);

    // Before the push, as every other count of a queued job is: the job may
    // run and finish before this call returns, and a parent that had not
    // been told about it could reach zero with the job still in flight. True
    // regardless of whether the push below happens now or is deferred to a
    // claim holder's own release -- the count is owed the instant this call
    // commits to scheduling the target at all.
    job.parent().count_queued(job.topology(), 1);

    // `is_entered` above answers only "is this node's nested run
    // outstanding", which says nothing about a target a job has already been
    // dequeued for -- `DESIGN.md` has the handle kinds that leaves out.
    // `wake` answers that question instead: `true` means nothing holds the
    // target's claim, so queueing now is what this call always did; `false`
    // means a job for this node is already in `WorkerThread::invoke_one`,
    // which hands the claim back exactly once -- after the dispatch, on the
    // entry that finishes the node, or straight away if cancellation makes
    // it skip the dispatch -- and queues this same job as it does. See
    // `DispatchClaim`'s own docs for the ordering that makes a `false` here
    // never a lost wake.
    if target.dispatch_claim().wake() {
        // Upstream's `_executor._schedule(_worker, node)`: this worker's own
        // queue. `Job::successor` is the job of a task of the same graph,
        // same run and same parent -- which is what a sibling is, whatever
        // the edges say.
        thread.schedule(job.successor(task));
    }
}

/// The scheduler, as a nonpreemptive runtime task's body sees it: upstream's
/// `NonpreemptiveRuntime`.
///
/// [`Runtime`] minus everything that could spawn work under this node --
/// `silent_async`, `scope`, `corun_until` and the rest of
/// [`crate::executor::runtime_async`]'s family -- which this type has no
/// inherent `impl` block borrowing from, so a body cannot reach them: nothing
/// ever sets this node `PREEMPTED`, so there is no protocol here to resume
/// one. What is left is [`NonpreemptiveRuntime::schedule`] and the read-only
/// accessors a body needs to tell which of a pool's workers it is running on.
///
/// Not nameable from outside the crate even though the keyword below is
/// `pub`, `executor::runtime` being `pub(crate)`; the `# Design` section has
/// why the keyword is `pub` all the same.
pub struct NonpreemptiveRuntime<'a> {
    /// The worker running the body, upstream's `_worker`.
    worker: &'a Worker,
    /// The job the node was dequeued as: what [`NonpreemptiveRuntime::schedule`]
    /// reaches the graph, the parent and the topology through.
    job: &'a Job,
    /// The scheduling loop of the worker running the body, for
    /// [`NonpreemptiveRuntime::schedule`]'s one push onto it.
    thread: &'a mut WorkerThread,
}

impl<'a> NonpreemptiveRuntime<'a> {
    /// Builds the handle for a nonpreemptive runtime task on `job`'s node.
    pub(crate) fn new(worker: &'a Worker, job: &'a Job, thread: &'a mut WorkerThread) -> Self {
        Self {
            worker,
            job,
            thread,
        }
    }

    /// Returns the worker running this task's body, upstream's `_worker` read
    /// back: [`Runtime::worker`]'s read-only half, with no `executor()`.
    #[must_use]
    pub fn worker(&self) -> &'a Worker {
        self.worker
    }

    /// Returns the id of the worker running this task's body:
    /// [`NonpreemptiveRuntime::worker`]`().id()` in one hop.
    #[must_use]
    pub fn worker_id(&self) -> usize {
        self.worker.id()
    }

    /// How many workers the executor running this task has, as
    /// [`Runtime::num_workers`] answers for the full handle.
    #[must_use]
    pub fn num_workers(&self) -> usize {
        self.thread.shared.workers.len()
    }

    /// Schedules `task` onto this worker's queue at once, bypassing the
    /// graph's dependencies: upstream's `NonpreemptiveRuntime::schedule`,
    /// which is [`Runtime::schedule`]'s body under a second name. Every word
    /// of that method's doc applies here unchanged -- `task` must be an
    /// active task of this runtime task's own graph -- with one difference in
    /// what makes the call safe: this node is never `PREEMPTED`, so a caller
    /// never has to reason about whether this node's own body has spawned
    /// anything before scheduling it.
    ///
    /// # Panics
    ///
    /// The same two panics [`Runtime::schedule`] has:
    /// [`Graph::node`](crate::Graph::node)'s if `task` is not a live task of
    /// this graph, and the refusal if `task` names a node that has already
    /// preempted itself -- which a nonpreemptive node never is itself.
    pub fn schedule(&mut self, task: Task) {
        schedule_bypassing(self.job, self.thread, task);
    }
}

impl fmt::Debug for NonpreemptiveRuntime<'_> {
    /// Names the worker, this handle's whole identity: unlike [`Runtime`] it
    /// holds no node of its own to name, as upstream's class holds no
    /// `Node*`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NonpreemptiveRuntime")
            .field("worker", &self.worker.id())
            .finish_non_exhaustive()
    }
}

/// Runs the nonpreemptive-runtime task `job` is for: upstream's
/// `_invoke_nonpreemptive_runtime_task`.
///
/// Unlike [`invoke`], there is no preemption protocol here at all: the body
/// is simply called, with no bit set before it and no reference taken on the
/// join counter, so this call always finishes the node on this one entry --
/// hence the bare `Release::Strong` rather than the `Option<Release>`
/// [`invoke`] and [`invoke_async`] return, there being no `None` case for the
/// dispatch arm to fold in. A panic out of the body is recorded as a static
/// task's is, through [`Job::record_panic`].
///
/// The dispatch claim's clearest case, and the caller's to hold: `work.invoke`
/// both takes and drops the one lock the claim stands in for, in one
/// statement, so the `Release` returned here is already the point the node is
/// idle again.
pub(crate) fn invoke_nonpreemptive(
    worker: &mut WorkerThread,
    job: &Job,
    work: &NonpreemptiveRuntimeWork,
) -> Release {
    // The same construction `run_body` gives a full `Runtime`, over the
    // restricted handle instead: the mutable borrow of the worker's
    // scheduling loop still has to end where the body does, which is a
    // promise this function keeps on its own since there is no `enter`
    // wrapping it to keep it for.
    let shared = Arc::clone(&worker.shared);
    let record: &Worker = &shared.workers[worker.id];
    let mut line = NonpreemptiveRuntime::new(record, job, worker);

    // `AssertUnwindSafe` for the reason `run_body` takes it: what a panic
    // here can leave half-written is reachable only through what the
    // closure captured.
    if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| work.invoke(&mut line))) {
        job.record_panic(payload);
    }

    Release::Strong
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, OnceLock};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::executor::r#async::ESTATE_NONE;
    use crate::executor::async_node::AsyncNode;
    use crate::executor::queue::WorkerQueue;
    use crate::executor::topology::{Cancellable, GraphOwner, Parent, Topology, set_up_graph};
    use crate::prelude::*;

    /// How long a case waits for a worker to deliver before it gives up.
    ///
    /// The deadline `tests/basics.rs` and `executor::async` both keep, for
    /// the reason they give: nothing here passes because of this number, and
    /// the only thing that reaches it is a side that is never going to
    /// arrive.
    const GIVE_UP_AFTER: Duration = Duration::from_secs(120);

    /// A free-standing async node of `work` and the job that runs it,
    /// accounted for as a submission accounts for one but **not** queued.
    ///
    /// `Shared::schedule_async_task` is the real submission and takes the
    /// same three steps before its push; what the cases below need that it
    /// cannot give them is the node itself, so that the state words can be
    /// read after the entry that finished it. The `increment_async` is not
    /// optional bookkeeping: the tear-down in `executor::async` decrements
    /// both counters for a parentless free node, and a job invoked without
    /// that increment would drive `num_asyncs` below zero and leave the
    /// executor's own `Drop` waiting for ever.
    fn free_async(executor: &Executor, work: AsyncWork, estate: u32) -> (Arc<AsyncNode>, Job) {
        executor.shared.increment_async();

        let node = AsyncNode::new("", Handle::Async(work));
        node.node()
            .base()
            .estate()
            .fetch_or(estate, Ordering::Relaxed);
        let job = Job::free(&node, &Parent::Run);
        Parent::Run.count_queued(Topology::free_standing(), 1);

        (node, job)
    }

    /// The state bits of a free node, masked to the two an entry sets.
    fn free_entered(node: &Arc<AsyncNode>) -> u32 {
        node.node().base().nstate().load(Ordering::Relaxed) & ENTERED
    }

    /// Waits for `handle` to have an outcome, failing rather than hanging if
    /// it never gets one.
    ///
    /// [`AsyncHandle::wait`] has no deadline of its own, so a regression in
    /// the delivery below -- a `complete` that never ran, or ran on the wrong
    /// entry -- would park this thread for ever instead of failing the case.
    /// Spinning on `is_done` first is what turns that into an assertion.
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
    /// The same helper the cases in `executor::topology` take, for the reason
    /// they give: the pool's real threads must not be able to steal what
    /// these cases queue, and they would not look anyway, since nothing here
    /// counts a topology.
    fn test_worker(executor: &Executor) -> WorkerThread {
        WorkerThread::new(Arc::clone(&executor.shared), 0, WorkerQueue::new())
    }

    /// A run of `taskflow`'s graph, armed and accounted for but with nothing
    /// queued: what a case here invokes a job of.
    ///
    /// `on_finish` is the run policy's callback, which the tear-down calls
    /// once the run's join counter reaches zero -- the one observable a
    /// hand-driven run has for "the accounting balanced".
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
    /// `Runtime::silent_async` queues a child on the queue of the worker
    /// running the body -- upstream's `if(auto w = this_worker(); w)` branch,
    /// which #119 is about and `executor::runtime_async` argues -- so where
    /// this looks is itself part of what the case asserts. The queue is the
    /// one `test_worker` built and nothing else holds an end of it, so a job
    /// left there cannot be stolen while the case walks over it.
    fn child_job(worker: &WorkerThread) -> Job {
        worker
            .pop()
            .expect("the submission pushed a job onto this worker's queue")
    }

    /// The state bits of `task`, masked to the two this arm sets.
    fn entered(taskflow: &Taskflow, task: Task) -> u32 {
        taskflow.graph().node(task).nstate().load(Ordering::Relaxed) & ENTERED
    }

    /// A body that returns without spawning anything is the simplest shape
    /// a runtime task takes: both bits come off on the first entry and the
    /// node finishes like a static task, with its successors released
    /// exactly once.
    ///
    /// "Released" is counted by the successor itself rather than found on the
    /// queue, which is what #62 changed about every case in this module: a
    /// released successor is kept out of the queue and run as the worker's
    /// continuation, so by the time `invoke` returns the release has already
    /// become a run.
    #[test]
    fn a_body_that_spawns_nothing_clears_its_bits_and_releases_once() {
        let runs = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();
        let rt = {
            let runs = Arc::clone(&runs);
            taskflow.emplace_runtime(move |_: &mut Runtime<'_>| {
                runs.fetch_add(1, Ordering::Relaxed);
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
        let topology = run_of(&taskflow, || {});
        worker.invoke(source_job(&taskflow, &topology));

        assert_eq!(runs.load(Ordering::Relaxed), 1, "the body ran once");
        assert_eq!(entered(&taskflow, rt), 0, "both bits came off");
        assert_eq!(
            taskflow
                .graph()
                .node(rt)
                .join_counter()
                .load(Ordering::Relaxed),
            0,
            "the node was re-armed to its own dependency count"
        );
        assert_eq!(
            released.load(Ordering::Relaxed),
            1,
            "the one successor was released, and released once"
        );
        assert!(
            worker.pop().is_none(),
            "and nothing was left queued: the successor ran here, as this \
             worker's continuation"
        );
    }

    /// The preemption branch, reached the way a caller reaches it: the body
    /// spawns one child, so the first entry is not the last reference and the
    /// node is left preempted with nothing released. The child's own
    /// tear-down drives the counter to zero and re-queues the node, and the
    /// second entry clears the bits and finishes it.
    ///
    /// Driven one job at a time from this thread, which is what fixes the
    /// order: the child is taken back out of the shared buffers rather than
    /// left for the pool, and `executor::runtime_async` -- which has the
    /// cases about the child's own accounting -- says why nothing can race
    /// that. A regression therefore fails an assertion here instead of
    /// wedging the suite.
    #[test]
    fn a_body_that_leaves_work_outstanding_is_preempted_and_resumed() {
        let runs = Arc::new(AtomicUsize::new(0));
        let children = Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();
        let rt = {
            let runs = Arc::clone(&runs);
            let children = Arc::clone(&children);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                runs.fetch_add(1, Ordering::Relaxed);
                // One more reference to the node, held by work that has not
                // finished: upstream's `rt.silent_async`, which is what the
                // counter below is counting.
                let children = Arc::clone(&children);
                runtime.silent_async(move || {
                    children.fetch_add(1, Ordering::Relaxed);
                });
            })
        };
        let released = Arc::new(AtomicUsize::new(0));
        let after = {
            let released = Arc::clone(&released);
            taskflow.emplace(move || {
                released.fetch_add(1, Ordering::Relaxed);
            })
        };
        taskflow.task(rt).precede([after]);

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, || {});

        worker.invoke(source_job(&taskflow, &topology));

        let node = taskflow.graph().node(rt);
        assert_eq!(runs.load(Ordering::Relaxed), 1, "the body ran once");
        assert_eq!(entered(&taskflow, rt), ENTERED, "the node is preempted");
        assert_eq!(
            node.join_counter().load(Ordering::Relaxed),
            1,
            "one piece of spawned work is still outstanding"
        );
        // The child, taken off this worker's own queue -- which is where
        // #119 puts it -- before the assertion that nothing *else* is there.
        let child = child_job(&worker);
        assert!(
            worker.pop().is_none(),
            "a preempted node releases nothing: its successors wait for the \
             work the body spawned"
        );
        assert_eq!(
            released.load(Ordering::Relaxed),
            0,
            "and it released nothing to run here either"
        );

        // Run here: its tear-down is `WorkerThread::finish_job`, which reads
        // the parent's state word before the decrement and, finding
        // `PREEMPTED`, re-queues the job the node was dequeued as.
        worker.invoke(child);
        assert_eq!(children.load(Ordering::Relaxed), 1, "the child ran");
        assert_eq!(
            node.join_counter().load(Ordering::Relaxed),
            0,
            "and drove the counter to zero"
        );

        let resume = worker
            .pop()
            .expect("the last child re-queued the preempted node");
        worker.invoke(resume);

        assert_eq!(
            runs.load(Ordering::Relaxed),
            1,
            "the second entry resumes the node rather than running the body \
             again"
        );
        assert_eq!(
            entered(&taskflow, rt),
            0,
            "the second entry clears both bits"
        );
        assert_eq!(
            released.load(Ordering::Relaxed),
            1,
            "and releases the successor, once -- which since #62 means it ran \
             here as the continuation rather than went on the queue"
        );
        assert!(worker.pop().is_none(), "so nothing was left queued");
    }

    /// A corun with nothing spawned is over before it starts: the counter is
    /// already at the body's own one reference. A corun that does wait needs
    /// a real run behind it -- a hand-driven one would spin in a steal loop
    /// that can find nothing to take -- so those cases live in
    /// `tests/runtimes.rs` and the branch above is stepped by hand.
    ///
    /// The explicit anchor is a guard, so it has to be gone by the time the
    /// call returns -- a node left anchored would catch a failure of a
    /// *later* nested graph that nobody is waiting for.
    #[test]
    fn a_corun_with_nothing_spawned_returns_at_once() {
        let mut taskflow = Taskflow::new();
        let rt = taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            runtime.try_corun().expect("nothing was spawned to fail");
            // Both shapes, and upstream's alias for the second.
            runtime.corun();
            runtime.corun_all();
            assert!(
                !runtime.node.base().is_explicitly_anchored(),
                "a corun that has returned holds no anchor, so the body is \
                 free to corun again"
            );
        });

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, || {});
        worker.invoke(source_job(&taskflow, &topology));

        assert_eq!(entered(&taskflow, rt), 0, "the node finished");
    }

    /// A payload in the node's own slot comes back from `try_corun`, which is
    /// what the implicit anchor is *for*: the walk in `Job::record_panic`
    /// leaves a failure there when the node that preempted itself is the
    /// closest thing to it with something to look at, and this is the frame
    /// that looks.
    ///
    /// The payload is put there the way that walk puts one -- claim the slot,
    /// then fill it -- rather than by a child that panics, because a corun is
    /// what collects it and a hand-driven case must not corun: nothing here
    /// counts a topology, so the corun loop would find nothing to steal and
    /// spin rather than fail. `a_failing_child_is_reported_from_its_parents_corun`
    /// in `tests/runtimes.rs` is the same claim through the public API, where
    /// a real run holds that count above zero.
    #[test]
    fn a_panic_under_an_implicit_anchor_comes_back_from_a_corun() {
        let mut taskflow = Taskflow::new();
        let rt = taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let base = runtime.node.base();
            assert!(
                base.is_implicitly_anchored(),
                "a running runtime task is an anchor of last resort"
            );

            assert!(base.claim_panic(), "the slot is this caller's");
            base.keep_panic(Box::new("boom"));

            let error = runtime
                .try_corun()
                .expect_err("the payload under this anchor is this frame's");
            assert_eq!(error.to_string(), "a task panicked: boom");

            // Taken, so the slot is empty again -- upstream's
            // `_rethrow_exception` clears it for the same reason.
            runtime.try_corun().expect("the slot was emptied");
        });

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, || {});
        worker.invoke(source_job(&taskflow, &topology));

        assert_eq!(entered(&taskflow, rt), 0, "the node finished");
    }

    /// `is_cancelled` reads the run, which is the first term of upstream's
    /// `_is_parent_cancelled`: a body sees `false` while the run is live and
    /// `true` the moment it is cancelled, with no scheduling in between --
    /// cancellation is not preemptive, so asking is the only way a running
    /// body learns of it.
    #[test]
    fn a_body_sees_its_run_cancelled_while_it_runs() {
        let state: Arc<OnceLock<Arc<Cancellable>>> = Arc::default();
        let seen = Arc::new(AtomicBool::new(false));

        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime({
            let state = Arc::clone(&state);
            let seen = Arc::clone(&seen);
            move |runtime: &mut Runtime<'_>| {
                assert!(!runtime.is_cancelled(), "the run is live");
                let state = state.get().expect("filled before the job is run");
                assert!(state.cancel(), "the run had not finished");
                seen.store(runtime.is_cancelled(), Ordering::Relaxed);
            }
        });

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, || {});
        assert!(
            state.set(Arc::clone(topology.cancellable())).is_ok(),
            "the slot is filled once"
        );

        worker.invoke(source_job(&taskflow, &topology));
        assert!(
            seen.load(Ordering::Relaxed),
            "the body saw the cancellation"
        );
    }

    /// `schedule` puts a sibling on *this worker's* queue with a zeroed join
    /// counter and counts it against this node's parent -- and the last of
    /// those is what the run policy's callback pins: a scheduled job the
    /// parent was never told about would let the run finish while that job
    /// was still queued.
    ///
    /// The shape is upstream's `Runtime.Schedule.ModuleTask` reduced to the
    /// scheduler: `other` has a strong dependency on `gate`, and the body
    /// schedules it anyway, which is the dependency bypass the method is for.
    ///
    /// The zeroed counter is read here too, and since #62 it is read *after*
    /// `gate` has run -- the released successor is this worker's
    /// continuation, so the release of `other` that bypasses nothing has
    /// already happened by the time the case looks. Zero is what it finds,
    /// because that release is floored rather than wrapped; issue #98 has
    /// why that floor is in the successor arm at all.
    #[test]
    fn a_scheduled_sibling_is_queued_here_and_counted_against_the_parent() {
        let order = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicBool::new(false));
        let mut taskflow = Taskflow::new();

        let other = {
            let order = Arc::clone(&order);
            taskflow.emplace(move || {
                order.fetch_add(1, Ordering::Relaxed);
            })
        };
        let gate = {
            let order = Arc::clone(&order);
            taskflow.emplace(move || {
                order.fetch_add(1, Ordering::Relaxed);
            })
        };
        let rt = taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            runtime.schedule(other);
        });
        taskflow.task(rt).precede([gate]);
        taskflow.task(gate).precede([other]);

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, {
            let finished = Arc::clone(&finished);
            move || finished.store(true, Ordering::Relaxed)
        });

        worker.invoke(source_job(&taskflow, &topology));

        let other_node = taskflow.graph().node(other);
        assert_eq!(
            other_node.join_counter().load(Ordering::Relaxed),
            0,
            "the scheduled sibling is queued with the zero `schedule` stored, \
             and `gate` -- which has already run here as the continuation, \
             and which `other` has a strong dependency on -- decremented \
             that zero without taking it below zero"
        );
        assert_eq!(
            order.load(Ordering::Relaxed),
            1,
            "the released successor ran here as the continuation, and the \
             scheduled sibling did not: it went on the queue"
        );
        assert!(!finished.load(Ordering::Relaxed), "the run is not over");

        // What is left on this worker's queue is the job the body scheduled.
        // Draining it is the rest of the run.
        let mut drained = 0;
        while let Some(job) = worker.pop() {
            worker.invoke(job);
            drained += 1;
        }

        assert_eq!(drained, 1, "the scheduled sibling, and nothing else");
        assert_eq!(order.load(Ordering::Relaxed), 2, "both bodies ran");
        assert_eq!(
            other_node.join_counter().load(Ordering::Relaxed),
            1,
            "and its own arm re-armed it to its one strong dependency, which \
             it could not have reached from a wrapped counter"
        );
        assert!(
            finished.load(Ordering::Relaxed),
            "the run finished exactly when its last job did, so the \
             scheduled job was counted against the parent"
        );
    }

    /// Issue #98's hazard, reached the way a caller reaches it and closed:
    /// `schedule` zeroes its target's join counter, a strong predecessor of
    /// that target decrements it anyway, and the target is a *runtime* task
    /// whose body coruns.
    ///
    /// The decrement of a counter already at zero is floored in
    /// `WorkerThread::invoke_one`'s successor arm rather than left to wrap,
    /// so the scheduled node is dequeued holding the zero `schedule` stored
    /// and its own first entry raises that to the one reference
    /// [`Runtime::try_corun`]'s predicate waits for. Before the floor the
    /// counter was `usize::MAX`, the entry's `fetch_add(1)` carried it to
    /// zero rather than to one, and the corun below never terminated -- a
    /// hang reachable from `emplace_runtime`, `precede` and `schedule`
    /// alone.
    ///
    /// Two things keep that hang out of this suite if the floor ever goes
    /// away. The jobs are driven by hand, one at a time, so the order the
    /// counter is touched in is fixed rather than raced for -- a real
    /// `Executor::run` of this graph reaches the same state at one worker,
    /// but a regression there would wedge the run instead of failing it. And
    /// the body coruns only if its counter is the one a corun can wait for,
    /// which turns a regression into the failed assertion below.
    #[test]
    fn a_scheduled_runtime_task_released_by_its_predecessor_can_still_corun() {
        let seen = Arc::new(AtomicUsize::new(usize::MAX));
        let coruned = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        let mut taskflow = Taskflow::new();

        let other = {
            let seen = Arc::clone(&seen);
            let coruned = Arc::clone(&coruned);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let counter = runtime.node.join_counter().load(Ordering::Relaxed);
                seen.store(counter, Ordering::Relaxed);
                if counter == 1 {
                    runtime.try_corun().expect("nothing was spawned to fail");
                    coruned.store(true, Ordering::Relaxed);
                }
            })
        };
        let gate = taskflow.emplace(|| {});
        let rt = taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            runtime.schedule(other);
        });
        taskflow.task(rt).precede([gate]);
        taskflow.task(gate).precede([other]);

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, {
            let finished = Arc::clone(&finished);
            move || finished.store(true, Ordering::Relaxed)
        });

        worker.invoke(source_job(&taskflow, &topology));

        let other_node = taskflow.graph().node(other);
        assert_eq!(
            other_node.join_counter().load(Ordering::Relaxed),
            0,
            "`gate` released a counter that `schedule` had already zeroed, \
             and the release left it at zero rather than at `usize::MAX`"
        );

        let job = worker.pop().expect("the scheduled runtime task is queued");
        worker.invoke(job);

        assert_eq!(
            seen.load(Ordering::Relaxed),
            1,
            "the body's own reference is the whole of the counter, which is \
             what the corun predicate waits for"
        );
        assert!(coruned.load(Ordering::Relaxed), "and the corun returned");
        assert!(
            worker.pop().is_none(),
            "the runtime task has no successors, so nothing was left queued"
        );
        assert!(
            finished.load(Ordering::Relaxed),
            "the node finished like a static task, so the run's accounting \
             balanced"
        );
    }

    /// The job that runs `task` under the same run as `source`, counted
    /// against that run but **not** queued: how the three cases below hold a
    /// predecessor's job back until the order they are about has been
    /// reached.
    ///
    /// Counted here rather than at the invoke because that is the order every
    /// real queueing takes, and because a held-back job the run was never
    /// told about would let the run finish -- and tear its own topology down
    /// -- halfway through the case.
    fn held_back_job(source: &Job, topology: &Arc<Topology>, task: Task) -> Job {
        Parent::Run.count_queued(topology, 1);
        source.successor(task)
    }

    /// Issue #107's third ordering, which is the one no check sees: a plain
    /// task the graph says runs once, run twice.
    ///
    /// `target` is bypassed by `schedule` *and* has a strong predecessor the
    /// bypass does not remove, and here that predecessor runs **after** the
    /// scheduled task has finished and re-armed its own counter. The release
    /// is then an ordinary one -- the counter is at its armed value, the
    /// decrement reaches zero legitimately -- so the target is queued and run
    /// again. This case pins that second run as what the caller gets, which
    /// is what [`Runtime::schedule`]'s docs now say it is: the caller's own
    /// doing, for a graph upstream's rule says not to build.
    ///
    /// It is also the answer to #107's first question, which was whether
    /// `Work::invoke`'s `try_lock` already reports this. It cannot, and the
    /// two runs here are why: the second starts after the first has
    /// returned, so the lock is free and there is nothing to contend for.
    /// The ordering that *would* contend -- a release arriving while the body
    /// runs -- is the one #98's floor turns into no release at all, since a
    /// bypassed node's counter is the stored zero for as long as its body
    /// runs and a floored decrement of zero queues nothing.
    ///
    /// The order is fixed rather than raced for: every job is invoked from
    /// this thread, and the conditioner is what keeps `gate` out of the
    /// scheduler until the case wants it -- it selects the runtime task and
    /// never `gate`, whose job is built by hand and invoked last. Nothing
    /// here waits for anything, so a regression fails an assertion rather
    /// than wedging the suite.
    #[test]
    fn a_predecessor_releasing_after_the_scheduled_task_ran_runs_it_twice() {
        let runs = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicBool::new(false));
        let mut taskflow = Taskflow::new();

        let target = {
            let runs = Arc::clone(&runs);
            taskflow.emplace(move || {
                runs.fetch_add(1, Ordering::Relaxed);
            })
        };
        let gate = taskflow.emplace(|| {});
        let rt = taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            runtime.schedule(target);
        });
        // The conditioner selects its first successor and nothing else, so
        // `gate` is reachable only through the job this case builds by hand.
        let entry = taskflow.emplace_condition(|| 0);
        taskflow.task(entry).precede([rt, gate]);
        taskflow.task(gate).precede([target]);

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, {
            let finished = Arc::clone(&finished);
            move || finished.store(true, Ordering::Relaxed)
        });
        let entry_job = source_job(&taskflow, &topology);
        let gate_job = held_back_job(&entry_job, &topology, gate);

        worker.invoke(entry_job);

        let target_node = taskflow.graph().node(target);
        assert_eq!(
            runs.load(Ordering::Relaxed),
            0,
            "the scheduled task went on the queue rather than running as the \
             conditioner's continuation, which is the runtime task"
        );

        let job = worker.pop().expect("the scheduled task is queued");
        worker.invoke(job);

        assert_eq!(runs.load(Ordering::Relaxed), 1, "the bypass ran it once");
        assert_eq!(
            target_node.join_counter().load(Ordering::Relaxed),
            1,
            "and its own arm re-armed it to its one strong dependency, which \
             is the value that makes the release below an ordinary one"
        );
        assert!(worker.pop().is_none(), "nothing else is queued");

        worker.invoke(gate_job);

        assert_eq!(
            runs.load(Ordering::Relaxed),
            2,
            "the predecessor's release reached zero legitimately, so the \
             target ran a second time -- `try_lock` had nothing to report, \
             because the first run was over"
        );
        assert_eq!(
            target_node.join_counter().load(Ordering::Relaxed),
            1,
            "the second run re-armed it again"
        );
        assert!(
            worker.pop().is_none(),
            "the second run was the continuation"
        );
        assert!(
            finished.load(Ordering::Relaxed),
            "and it was accounted for like any other released successor, so \
             the run finished rather than losing a count to it"
        );
    }

    /// Issue #107's dangerous ordering, refused: the bypassed task has
    /// *preempted itself* by the time its strong predecessor releases it, so
    /// the count that release would spend is a reference its own outstanding
    /// work is holding.
    ///
    /// Delivering it would resume the node with that work still running --
    /// successors released early, and the counter past the zero the work
    /// needs to resume it, which is a run that never finishes. `try_lock`
    /// cannot report that one either, and this is the shape that settles it:
    /// the body has already returned, so no lock is held, and the resumption
    /// would take the dispatch's `PREEMPTED` fast path, which does not touch
    /// the body at all. So the successor arm drops the release and records it
    /// as the failure of the task that released, leaving every reference the
    /// node holds intact -- which is what lets the outstanding work still
    /// resume it and the run still finish, with the failure rather than
    /// without it.
    ///
    /// The body stands its outstanding work up the way
    /// `a_body_that_leaves_work_outstanding_is_preempted_and_resumed` does,
    /// by raising the node's own counter, and the case spends that reference
    /// by hand afterwards. So the order is fixed, nothing waits, and the
    /// regression this guards against fails an assertion instead of wedging
    /// the suite: the resumption that would hang the run is the one the case
    /// drives itself.
    #[test]
    fn a_release_onto_a_task_that_has_preempted_itself_is_refused() {
        let runs = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicBool::new(false));
        let mut taskflow = Taskflow::new();

        let other = {
            let runs = Arc::clone(&runs);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                runs.fetch_add(1, Ordering::Relaxed);
                // A child, stood in for by hand: one more reference to the
                // node, held by work that has not finished. By hand because
                // this case is about the *order* the bypass arrives in, and
                // spends that reference itself further down.
                runtime.node.join_counter().fetch_add(1, Ordering::Relaxed);
            })
        };
        let after = {
            let released = Arc::clone(&released);
            taskflow.emplace(move || {
                released.fetch_add(1, Ordering::Relaxed);
            })
        };
        let gate = taskflow.emplace(|| {});
        let rt = taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            runtime.schedule(other);
        });
        let entry = taskflow.emplace_condition(|| 0);
        taskflow.task(entry).precede([rt, gate]);
        taskflow.task(gate).precede([other]);
        taskflow.task(other).precede([after]);

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, {
            let finished = Arc::clone(&finished);
            move || finished.store(true, Ordering::Relaxed)
        });
        let entry_job = source_job(&taskflow, &topology);
        let gate_job = held_back_job(&entry_job, &topology, gate);

        worker.invoke(entry_job);

        let other_node = taskflow.graph().node(other);
        let job = worker.pop().expect("the scheduled runtime task is queued");
        // The job the node was dequeued as, which is what the outstanding
        // work re-queues to resume it.
        let resume = job.clone();
        worker.invoke(job);

        assert_eq!(runs.load(Ordering::Relaxed), 1, "the body ran once");
        assert_eq!(entered(&taskflow, other), ENTERED, "and preempted itself");
        assert_eq!(
            other_node.join_counter().load(Ordering::Relaxed),
            1,
            "holding the one reference its outstanding work owns"
        );

        worker.invoke(gate_job);

        assert_eq!(
            other_node.join_counter().load(Ordering::Relaxed),
            1,
            "the strong release was dropped rather than delivered, so the \
             reference the outstanding work owns is still there"
        );
        assert_eq!(
            entered(&taskflow, other),
            ENTERED,
            "and the node is still preempted, since nothing resumed it"
        );
        assert_eq!(
            released.load(Ordering::Relaxed),
            0,
            "its successor was not released early"
        );
        assert!(worker.pop().is_none(), "nor was a resumption queued");
        assert!(
            !finished.load(Ordering::Relaxed),
            "and the run is not over: the preempted node still owes it"
        );

        // The outstanding work finishing, by hand, as the preemption cases
        // above drive it: `WorkerThread::finish_job` reads the parent's state
        // word before this decrement and, finding `PREEMPTED`, re-queues the
        // job the node was dequeued as.
        assert_eq!(
            other_node.join_counter().fetch_sub(1, Ordering::AcqRel),
            1,
            "the refused release left this the last reference"
        );
        worker.invoke(resume);

        assert_eq!(
            runs.load(Ordering::Relaxed),
            1,
            "the resumption is the entry the node was waiting for, and it \
             runs no body"
        );
        assert_eq!(entered(&taskflow, other), 0, "it clears both bits");
        assert_eq!(
            released.load(Ordering::Relaxed),
            0,
            "the successor it releases is skipped, because the refusal \
             cancelled the run as any task's failure does"
        );
        assert!(
            finished.load(Ordering::Relaxed),
            "and the run finished rather than hanging, which is the whole \
             point of dropping that release instead of spending it"
        );
        assert!(
            payload_of(&topology).contains("already running"),
            "the run reports the invariant by name"
        );
    }

    /// The same hazard from the other direction, and the one `schedule`
    /// itself can see: a target that has already preempted itself is refused
    /// before the zero is stored.
    ///
    /// Storing it would wipe the references the node's outstanding work is
    /// holding rather than bypass a dependency count, which is the hang
    /// upstream's own "we currently do not support scheduling a runtime
    /// task" leaves to the caller. The panic is the body's, so it is
    /// reported the way any failing task is -- as the run's failure, with
    /// the runtime task still finishing -- and the target is left exactly as
    /// it was found.
    ///
    /// The target's state is set up by hand rather than reached through a
    /// second worker, which is what keeps the order fixed: a case that raced
    /// a live preemption against a `schedule` could only ever *sometimes*
    /// reach the state it is about. `set_up_graph` clears both words at the
    /// start of every run, so this is the state a node of a run that reached
    /// the preemption branch is in, written directly.
    #[test]
    fn scheduling_a_task_that_has_preempted_itself_is_refused() {
        let runs = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicBool::new(false));
        let mut taskflow = Taskflow::new();

        let other = {
            let runs = Arc::clone(&runs);
            taskflow.emplace_runtime(move |_: &mut Runtime<'_>| {
                runs.fetch_add(1, Ordering::Relaxed);
            })
        };
        let rt = taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            runtime.schedule(other);
        });
        let entry = taskflow.emplace_condition(|| 0);
        taskflow.task(entry).precede([rt, other]);

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, {
            let finished = Arc::clone(&finished);
            move || finished.store(true, Ordering::Relaxed)
        });

        // A node that has reached the preemption branch: both bits, and one
        // reference per piece of work still outstanding.
        let other_node = taskflow.graph().node(other);
        other_node.nstate().fetch_or(ENTERED, Ordering::Relaxed);
        other_node.join_counter().fetch_add(1, Ordering::Relaxed);

        worker.invoke(source_job(&taskflow, &topology));

        assert_eq!(
            runs.load(Ordering::Relaxed),
            0,
            "the target was not entered a second time"
        );
        assert_eq!(
            other_node.join_counter().load(Ordering::Relaxed),
            1,
            "and the refusal wrote nothing: the reference its outstanding \
             work owns is untouched"
        );
        assert!(
            worker.pop().is_none(),
            "nothing was queued, so the count `schedule` takes against the \
             parent was not taken either"
        );
        assert!(
            finished.load(Ordering::Relaxed),
            "the runtime task finished through the panic path, so the run's \
             accounting balanced and `wait` would answer"
        );
        assert!(
            payload_of(&topology).contains("already running"),
            "with the invariant named in the payload"
        );
    }

    /// What a hand-driven run reports, as the string `RunError` would render.
    ///
    /// The `join` cannot block: the tear-down fires the run's callback and
    /// fills the completion slot in that order, both inside the `invoke` that
    /// ended the run and therefore on this thread, so a case that has
    /// asserted the callback ran is asking for a slot that is already full.
    fn payload_of(topology: &Arc<Topology>) -> String {
        let payload = topology.join().expect("the run reports a failure");
        payload
            .downcast_ref::<&str>()
            .expect("both refusals are literal messages")
            .to_string()
    }

    /// A payload in a `spawn_runtime` node's own slot is what that task
    /// reports, which is the whole of what its second entry is for: the node
    /// is an explicit anchor for its whole life, so the walk in
    /// `Job::record_panic` leaves a failure under it right there, and
    /// `complete` hands that slot to the handle.
    ///
    /// The payload is put there the way
    /// `a_panic_under_an_implicit_anchor_comes_back_from_a_corun` puts one --
    /// claim the slot, then fill it -- rather than by a real child, because
    /// this node's own submission is counted in `num_topologies` and the pool
    /// would race this thread for any child it spawned (`free_async` has that
    /// count). What it stands in for is exactly that: a child's failure,
    /// delivered here by the walk, collected by the entry that finishes the
    /// node.
    #[test]
    fn a_payload_on_a_spawn_runtime_node_comes_back_from_its_handle() {
        let executor = Executor::new(2);

        let handle = executor.spawn_runtime(|runtime: &mut Runtime<'_>| {
            let base = runtime.node.base();
            assert!(
                base.is_explicitly_anchored(),
                "upstream constructs this node ESTATE::EXPLICITLY_ANCHORED, so \
                 a failure under it stops here rather than going up the chain"
            );
            assert!(
                base.is_implicitly_anchored(),
                "and it is the entered runtime node it also is"
            );

            assert!(base.claim_panic(), "the slot is this caller's");
            base.keep_panic(Box::new("boom"));
        });

        spin_until_done("the spawn_runtime task to deliver", &handle);
        let error = handle
            .wait()
            .expect_err("the payload under this anchor is the task's failure");
        assert_eq!(error.to_string(), "a task panicked: boom");

        let RunError::Panicked(payload) = error;
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"boom"),
            "the payload keeps its identity, as a run's does"
        );

        executor.wait_for_all();
        assert_eq!(
            executor.num_topologies(),
            0,
            "and the task was accounted for exactly once"
        );
    }

    /// A `silent_async_runtime` body that spawns nothing finishes on its
    /// first entry: both bits come off, the node is torn down like a plain
    /// async task's, and the counters a submission raised come back down
    /// without a second entry ever happening.
    ///
    /// Driven by hand rather than submitted, for the reason `free_async`
    /// gives: a submission hands back no node, and the node's state words
    /// after the entry are the observable. The estate is left clear, which is
    /// upstream's `ESTATE::NONE` for this form and is asserted from inside
    /// the body -- it is what makes the difference between this shape and
    /// the one above.
    #[test]
    fn a_silent_async_runtime_body_that_spawns_nothing_finishes_on_its_first_entry() {
        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let runs = Arc::new(AtomicUsize::new(0));

        let work = AsyncWork::runtime({
            let runs = Arc::clone(&runs);
            move |runtime: &mut Runtime<'_>| {
                runs.fetch_add(1, Ordering::Relaxed);

                let base = runtime.node.base();
                assert!(
                    base.is_implicitly_anchored(),
                    "an entered runtime node is an anchor of last resort, \
                     whichever of the three shapes it is"
                );
                assert!(
                    !base.is_explicitly_anchored(),
                    "but nobody is waiting for this one: upstream constructs \
                     the silent form ESTATE::NONE"
                );

                runtime.try_corun().expect("nothing was spawned to fail");
            }
        });
        let (node, job) = free_async(&executor, work, ESTATE_NONE);

        worker.invoke(job);

        assert_eq!(runs.load(Ordering::Relaxed), 1, "the body ran once");
        assert_eq!(
            free_entered(&node),
            0,
            "both bits came off on the first entry: nothing was outstanding"
        );
        assert_eq!(
            node.node().join_counter().load(Ordering::Relaxed),
            0,
            "and the body's own reference was the last one"
        );
        assert!(
            worker.pop().is_none(),
            "a free node has no successors, so nothing was left queued"
        );
        assert_eq!(
            executor.num_topologies(),
            0,
            "the tear-down ran on that one entry, so the counters the \
             submission raised are back down"
        );
    }

    /// A `silent_async_runtime` body that panics is reported to nobody and
    /// must cost nobody: the payload is parked on the node and goes with it,
    /// the executor is still usable, and -- the part that matters -- the
    /// sentinel run every free node's job claims to be part of takes no part
    /// in it.
    ///
    /// That last one is issue #105's, re-asserted here because this
    /// submission is a new way onto that walk: a payload claimed on the
    /// sentinel would set the `EXCEPTION` bit `Job::is_parent_cancelled`
    /// reads, so every async task submitted afterwards -- for the life of the
    /// process -- would be skipped unrun, and a skipped job never reaches the
    /// counters, so no later `wait_for_all` would return either. Everything
    /// downstream of that hangs rather than fails, which is why the sentinel
    /// is asserted on directly rather than inferred from the task below.
    #[test]
    fn a_silent_async_runtime_body_that_panics_leaves_the_executor_usable() {
        let executor = Executor::new(2);

        executor.silent_async_runtime(|_: &mut Runtime<'_>| panic!("parked on the node"));
        executor.wait_for_all();

        assert!(
            !Topology::free_standing().cancelled(),
            "the run every free node shares took no part in that failure"
        );

        let ran = Arc::new(AtomicUsize::new(0));
        executor.silent_async_runtime({
            let ran = Arc::clone(&ran);
            move |_: &mut Runtime<'_>| {
                ran.fetch_add(1, Ordering::Relaxed);
            }
        });
        executor.wait_for_all();

        assert_eq!(
            ran.load(Ordering::Relaxed),
            1,
            "a silent failure must not cancel the next async task"
        );
        assert_eq!(executor.num_topologies(), 0, "and both were accounted for");
    }

    /// The preemption branch of the reentrant shape: the body raises its own
    /// node's join counter, exactly as a child spawned under the node does
    /// (#92), so the first entry is not the last reference. The
    /// node is left preempted with nothing delivered and nothing accounted
    /// for; a hand-driven decrement to zero is the last child's tear-down,
    /// and the second entry clears the bits, runs `complete` and finishes
    /// the task.
    ///
    /// That `complete` runs **once** is the half of upstream's
    /// `work(rt, true)` that is easiest to get wrong in this shape: a second
    /// call would fulfil one promise twice, and a call on the preempted
    /// entry would fulfil it before the work it is a promise about was done.
    #[test]
    fn a_reentrant_body_that_leaves_work_outstanding_completes_on_its_second_entry() {
        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let runs = Arc::new(AtomicUsize::new(0));
        let completions = Arc::new(AtomicUsize::new(0));

        let work = AsyncWork::reentrant(
            {
                let runs = Arc::clone(&runs);
                move |runtime: &mut Runtime<'_>| {
                    runs.fetch_add(1, Ordering::Relaxed);
                    // A child, stood in for by hand: one more reference to
                    // the node, held by work that has not finished. By hand
                    // because a free node's submission is counted in
                    // `num_topologies`, so the pool would race this thread
                    // for a real child -- and because the case spends that
                    // reference itself below, which fixes the order.
                    runtime.node.join_counter().fetch_add(1, Ordering::Relaxed);
                }
            },
            {
                let completions = Arc::clone(&completions);
                move |payload: Option<Box<dyn Any + Send>>| {
                    assert!(payload.is_none(), "nothing failed under this node");
                    completions.fetch_add(1, Ordering::Relaxed);
                }
            },
        );
        // Explicitly anchored, as `spawn_runtime` constructs it: this is that
        // node, minus the submission that would have hidden it.
        let (node, job) = free_async(&executor, work, Cancellable::EXPLICITLY_ANCHORED);
        // The job the node was dequeued as, which is what the last child
        // re-queues to resume it.
        let resume = job.clone();

        worker.invoke(job);

        assert_eq!(runs.load(Ordering::Relaxed), 1, "the body ran once");
        assert_eq!(free_entered(&node), ENTERED, "the node is preempted");
        assert_eq!(
            node.node().join_counter().load(Ordering::Relaxed),
            1,
            "one piece of spawned work is still outstanding"
        );
        assert_eq!(
            completions.load(Ordering::Relaxed),
            0,
            "a preempted entry delivers nothing: the task is not over"
        );
        assert_eq!(
            executor.num_topologies(),
            1,
            "and it is not accounted for either -- the second entry is what \
             finishes the task"
        );
        assert!(worker.pop().is_none(), "nothing was queued by that entry");

        // The tear-down of the last child, by hand: `WorkerThread::finish_job`
        // reads the parent's state word before the decrement and, finding
        // `PREEMPTED`, re-queues the job above.
        assert_eq!(
            node.node().join_counter().fetch_sub(1, Ordering::AcqRel),
            1,
            "the last child drives the counter to zero"
        );
        worker.invoke(resume);

        assert_eq!(
            runs.load(Ordering::Relaxed),
            1,
            "the second entry resumes the node rather than running the body \
             again"
        );
        assert_eq!(
            completions.load(Ordering::Relaxed),
            1,
            "and runs the completion, once"
        );
        assert_eq!(free_entered(&node), 0, "the second entry clears both bits");
        assert_eq!(
            executor.num_topologies(),
            0,
            "the counters the submission raised came back down on that entry"
        );
        assert!(worker.pop().is_none(), "so nothing was left queued");
    }

    /// A nonpreemptive runtime task's body finishes on its first entry and
    /// leaves no `PREEMPTED` bit behind, which is the whole of what
    /// distinguishes [`Handle::NonpreemptiveRuntime`] from
    /// [`Handle::Runtime`]: there is no second branch to take here, unlike
    /// `a_body_that_spawns_nothing_clears_its_bits_and_releases_once` above,
    /// because no entry of this dispatch arm ever sets the bit in the first
    /// place.
    ///
    /// Driven by hand, one job at a time, for
    /// `a_body_that_spawns_nothing_clears_its_bits_and_releases_once`'s
    /// reason: the order is fixed rather than raced for, and a regression
    /// fails an assertion instead of wedging the suite.
    #[test]
    fn a_nonpreemptive_body_finishes_on_its_first_entry_with_no_bit_set() {
        let runs = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicUsize::new(0));
        let mut taskflow = Taskflow::new();
        let line = {
            let runs = Arc::clone(&runs);
            taskflow.emplace_nonpreemptive_runtime(move |_| {
                runs.fetch_add(1, Ordering::Relaxed);
            })
        };
        let after = {
            let released = Arc::clone(&released);
            taskflow.emplace(move || {
                released.fetch_add(1, Ordering::Relaxed);
            })
        };
        taskflow.task(line).precede([after]);

        let executor = Executor::new(1);
        let mut worker = test_worker(&executor);
        let topology = run_of(&taskflow, || {});
        worker.invoke(source_job(&taskflow, &topology));

        assert_eq!(runs.load(Ordering::Relaxed), 1, "the body ran once");
        assert_eq!(
            entered(&taskflow, line),
            0,
            "no bit was ever set, so there is none to clear"
        );
        assert_eq!(
            taskflow
                .graph()
                .node(line)
                .join_counter()
                .load(Ordering::Relaxed),
            0,
            "the node was re-armed to its own dependency count, having taken \
             no reference on it the way a preempting kind's first entry does"
        );
        assert_eq!(
            released.load(Ordering::Relaxed),
            1,
            "the one successor was released, and released once"
        );
        assert!(
            worker.pop().is_none(),
            "and nothing was left queued: the successor ran here, as this \
             worker's continuation"
        );
    }
}

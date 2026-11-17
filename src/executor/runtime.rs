//! Translated from `taskflow/core/runtime.hpp` (Taskflow v4.1.0).
//!
//! The executor's half of runtime tasking: upstream's `_invoke_runtime_task`
//! and **both** overloads of `_invoke_runtime_task_impl`, which that header
//! defines out of line, plus the scheduling methods of `tf::Runtime` --
//! `schedule`, `corun`, `corun_all` and `is_cancelled`. The `RUNTIME` arm of
//! `_invoke` in `taskflow/core/executor.hpp` is one line into [`invoke`]
//! below, as the module arm is into [`module`](crate::executor::module). The
//! shell of [`Runtime`](crate::Runtime) is `src/runtime.rs`, which says what
//! the handle holds and why it cannot hand back an executor.
//!
//! # Three body shapes, one protocol
//!
//! A runtime task of a graph is one of them. The other two are the
//! runtime-shaped *async* bodies: upstream's
//! `std::function<void(tf::Runtime&)>` and
//! `std::function<void(tf::Runtime&, bool)>` alternatives, which are cases 1
//! and 2 of `_invoke_async_task` in `taskflow/core/executor.hpp` and which it
//! dispatches to the very same pair of `_invoke_runtime_task_impl`
//! overloads. [`invoke_async`] is that dispatch, and the submitting calls are
//! the `is_runtime_task_v<F>` branches of `_async` and `_silent_async` in
//! `taskflow/core/async.hpp` -- ported as [`Executor::spawn_runtime`] and
//! [`Executor::silent_async_runtime`] in the second `impl Executor` block
//! below rather than beside the plain forms in `executor::async`, because
//! what is interesting about them is the protocol in the next section and not
//! the submission, which is `executor::async`'s own
//! `Executor::schedule_async_task` called with one more argument.
//!
//! The `void(Runtime&, bool)` shape is the one that is *called* twice.
//! Upstream hands one lambda a `reentered` flag: `false` to run the user's
//! body, `true` to read `rt._node->_exception_ptr` into the promise behind
//! the future. This port splits that into the two `FnOnce`s
//! [`AsyncWork::Reentrant`] holds, for the reason that variant's own docs
//! give, and [`invoke_async`] runs `body` on the first entry and `complete`
//! on the entry that **finishes** the node -- the second one when the body
//! left work outstanding, the first one when it did not. That is upstream's
//! `work(rt, true)` after its `if`/`else`: exactly once, whichever branch
//! got there.
//!
//! # A runtime task is the parent of its own children
//!
//! The protocol is the preemption protocol the [`executor`](crate::executor)
//! module docs set out, with one difference from a module or subflow task:
//! what the node waits for is not a *graph* but however much asynchronous
//! work the body chose to spawn, so the count cannot be known before the
//! body has run. Upstream's answer is to make the body itself one reference:
//!
//! 1. the first entry sets `PREEMPTED | IMPLICITLY_ANCHORED` and bumps the
//!    node's own join counter by one, **before** the body runs -- a child the
//!    body spawns raises that same counter, and the bits are what tell the
//!    last child to re-queue this node;
//! 2. the body runs, under the `catch_unwind` that every other task body runs
//!    under;
//! 3. the first entry then spends its own reference. If it was the last one,
//!    nothing the body spawned is outstanding, so the bits come off and the
//!    node finishes like a static task. If it was not, the children still
//!    running own the node: it stays preempted, and the last of them to
//!    finish re-queues the job it was dequeued as.
//!
//! The `IMPLICITLY_ANCHORED` bit is what makes the node an anchor of last
//! resort while that is going on -- `Job::record_panic` in the crate-private
//! `executor::topology` is the walk that reads it, and issue #48 ported it
//! for this arm to be the first to set it. A payload that reaches the node's
//! slot is taken by [`Runtime::try_corun`]; one that nobody takes is dropped
//! by the next `set_up_graph` of that graph, which is upstream's behaviour
//! too.
//!
//! # What a failure under each of the three shapes reaches
//!
//! Upstream constructs the node of the handle-returning async form
//! `ESTATE::EXPLICITLY_ANCHORED` and the silent one `ESTATE::NONE`, and that
//! one argument is the whole of the difference in what a caller sees:
//!
//! - [`Executor::spawn_runtime`]'s node is explicitly anchored from the
//!   moment it is constructed, rather than for the length of a wait as
//!   [`Runtime::try_corun`]'s guard is. So the panic walk stops *there* for
//!   anything that fails under it -- the body itself, and a child the body
//!   spawned (#92) -- and leaves the payload in the node's own slot.
//!   `complete` reads that slot on the finishing entry and hands it to
//!   [`AsyncHandle::wait`](crate::AsyncHandle::wait) as
//!   [`RunError::Panicked`], which is upstream's `set_exception`.
//!
//!   One wrinkle, and it is upstream's too: a [`Runtime::corun`] inside such
//!   a body takes the explicit-anchor *guard* over the same bit, and that
//!   guard clears the bit on the way out -- upstream's
//!   `ExplicitAnchorGuard` destructor is an unconditional
//!   `fetch_and(~EXPLICITLY_ANCHORED)`, not a restore. It costs nothing,
//!   because the node that has just lost the explicit bit still carries the
//!   implicit one for as long as it is entered, and both anchors are the
//!   same `NodeBase`: a later failure under it lands in the same slot, which
//!   is the slot `complete` reads.
//! - [`Executor::silent_async_runtime`]'s node carries no estate bit and is
//!   an anchor only in the implicit sense above, only while it is entered.
//!   A payload that lands in its slot is dropped with the node, because
//!   there is no handle to read it from -- the answer
//!   [`Executor::silent_async`](crate::Executor::silent_async) gives for a
//!   plain body, and upstream's.
//! - A runtime task of a graph is neither: its failure goes up the chain to
//!   whatever is waiting for that graph, which for a top-level task is the
//!   run.
//!
//! What a free node must never do is claim the sentinel run at the root of
//! its chain, which is issue #105 and is `Job::record_panic`'s to get right
//! rather than this module's: the two submitting calls below pass
//! [`Parent::Run`] over that sentinel, exactly as the plain forms do.
//!
//! # Why the arm answers with a boolean rather than the state word
//!
//! Upstream returns a `bool` from `_invoke_runtime_task_impl` and says why in
//! a comment: the caller cannot read `node->_nstate` to find out whether the
//! node was preempted, because the instant the join counter above drops to
//! something non-zero a child may finish, re-queue this node and have another
//! worker clear those very bits -- so the caller's read would race the
//! second entry's write. The answer therefore has to be a value the first
//! entry computed while it still held a reference, which is exactly what the
//! [`Option<Release>`](Release) this arm returns is: `None` is upstream's
//! `true`.
//!
//! # What a corun waits for
//!
//! [`Runtime::corun`] waits for the node's join counter to come back to
//! **one** -- the body's own reference, the one step 1 added -- which is
//! upstream's predicate exactly. What raises it above one is the `async`
//! family of [`Runtime`], which is `executor::runtime_async` (#92): each
//! child a body spawns is counted against *this* node, where a task the body
//! [`schedule`](Runtime::schedule)s is counted against the node's own parent
//! and is therefore a sibling rather than a child. So the preemption branch
//! above is reached by any body that spawns and does not wait, and the corun
//! predicate has something to wait for in any body that spawns and does.
//!
//! The cases at the bottom of this file still drive one thing by hand, and
//! the reason has moved: a free node's own submission is counted in
//! `num_topologies`, so the pool's workers sweep the buffers while such a
//! case runs and would race it for the child. A case about a *graph's*
//! runtime node has no such count and drives real children --
//! `executor::runtime_async` has those, and the helper there says why the
//! pool cannot interfere.
//!
//! # Scheduling a sibling, and the same-graph limit
//!
//! [`Runtime::schedule`] is upstream's `schedule`: it puts an *active* task
//! -- a task of a running taskflow -- straight onto this worker's queue,
//! bypassing the graph's dependencies. Upstream takes a `tf::Task`, which is
//! a pointer to the node, and reaches the node's parent through it. A
//! [`Task`] here is an index into one arena, so the method is limited to a
//! task of **the same graph as the runtime task**, and that limit is what
//! makes the parent question answerable: a sibling's parent is this node's
//! own parent, which the job in hand already holds. See the method's own
//! docs, and the README's design decisions.
//!
//! What `schedule` bypasses is the target's dependency *count*, not its
//! edges: a strong predecessor of a scheduled task still releases it when it
//! finishes, onto the zero `schedule` stored. Upstream's release arm wraps
//! that zero to `SIZE_MAX`, which is a counter no corun predicate and no
//! resume ever matches again; this port floors the decrement at zero
//! instead, the one departure its successor arm makes from upstream's bare
//! `fetch_sub`. That is issue #98. What the floor cannot make harmless is the
//! *order*, which is issue #107: the same arm now refuses a strong release
//! onto a node that has already preempted itself, as [`Runtime::schedule`]
//! refuses a target that has, and the method's docs work through all three
//! orderings -- the two that are reported and the one second run that stays
//! the caller's own doing.

use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::Runtime;
use crate::executor::Executor;
use crate::executor::r#async::{AsyncHandle, ESTATE_NONE};
use crate::executor::run_handle::RunError;
use crate::executor::runtime_async::completion;
use crate::executor::topology::{Cancellable, Job, Parent};
use crate::executor::worker::{Release, Worker, WorkerThread, is_entered};
use crate::graph::{AsyncWork, Node, OnceWork, RuntimeWork, Task};

/// The two bits a runtime task holds while its own work is outstanding:
/// upstream's `NSTATE::PREEMPTED | NSTATE::IMPLICITLY_ANCHORED`, which it
/// sets and clears together in all three of its runtime invokers.
const ENTERED: u32 = Node::PREEMPTED | Node::IMPLICITLY_ANCHORED;

/// Runs the runtime task `job` is for: upstream's `_invoke_runtime_task` and
/// the `void(Runtime&)` overload of `_invoke_runtime_task_impl`.
///
/// `None` means the node has been preempted -- work the body spawned is still
/// running, and the last of it to finish will re-queue this job -- and is
/// what keeps `WorkerThread::invoke` from re-arming the node or releasing
/// successors that have the body's own children still to wait for. The module
/// docs say why that answer is a return value rather than a bit the caller
/// could read back.
///
/// A panic out of the body is recorded exactly as a static task's is, through
/// [`Job::record_panic`]: delivered to whatever is waiting for the graph this
/// node is part of, and cancelling the rest of that graph. The bookkeeping in
/// [`enter`] still runs, because the node has been entered and the counters
/// have to balance whatever the body did -- which is upstream's arrangement
/// too, its exception handler being a statement inside the first entry rather
/// than a return.
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
/// The answer is [`invoke`]'s answer and means the same thing, which is why
/// the `ASYNC` arm in `executor::async` can hand both shapes straight here:
/// `Some` for a node that is finished, `None` for one that is preempted and
/// whose counters its second entry will settle. The module docs' first
/// section has which upstream function each arm is, and its anchor section
/// has what a failure under each reaches.
pub(crate) fn invoke_async(
    worker: &mut WorkerThread,
    job: &Job,
    work: &AsyncWork,
) -> Option<Release> {
    match work {
        // Upstream's case 0, which is `executor::async`'s own arm: both
        // callers -- the `ASYNC` arm and #91's `DEPENDENT_ASYNC` one -- match
        // the two runtime-shaped alternatives before calling here, exactly as
        // upstream's two switches do. Named rather than quietly handled,
        // because a third caller that forgot to would otherwise run a plain
        // body under a protocol that preempts its node.
        AsyncWork::Plain(_) => {
            unreachable!("a plain async body is not a runtime-shaped one: see `executor::async`")
        }
        // Upstream's case 1, the silent shape: one entry's worth of body and
        // no second call, because nobody is waiting for an answer.
        AsyncWork::Runtime(body) => enter(worker, job, |runtime| take_and_run(body, runtime)),
        // Upstream's case 2. The `complete` half is its `work(rt, true)`,
        // after the `if`/`else` and therefore on the entry that finished the
        // node -- never on an entry that preempted it, which is what the `?`
        // says: `None` is a node whose children are still running, and a
        // promise fulfilled then would be fulfilled before the work it is a
        // promise about was done.
        AsyncWork::Reentrant { body, complete } => {
            let release = enter(worker, job, |runtime| take_and_run(body, runtime))?;

            // Upstream's `auto& eptr = rt._node->_exception_ptr; eptr ?
            // p.set_exception(eptr) : p.set_value()`, with the node reached
            // through the job because that is the one thing the second entry
            // has -- upstream reaches it through the `Runtime` it rebuilds,
            // which this half of the body needs for nothing else.
            //
            // `None` would mean the slot had already been spent, which takes
            // a node entered twice without being preempted in between; doing
            // nothing is the one answer to that which cannot fulfil one
            // promise twice.
            if let Some(complete) = complete.take() {
                complete(job.node().base().take_panic());
            }
            Some(release)
        }
    }
}

/// Runs `body` on the node `job` is for under upstream's preemption protocol:
/// the shared half of both `_invoke_runtime_task_impl` overloads, which differ
/// in nothing else.
///
/// The module docs' protocol section is the argument for every line of this,
/// and the answer is the `bool` upstream returns inverted into the
/// `Option<Release>` the dispatch wants: `None` is upstream's `true`.
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
    // one left behind.
    node.nstate().fetch_and(!ENTERED, Ordering::Relaxed);
    Some(Release::Strong)
}

/// Runs the body against a handle over `node`, recording a panic out of it as
/// this job's failure.
///
/// Upstream's `TF_EXECUTOR_EXCEPTION_HANDLER(worker, node, { work(rt); })`,
/// which `WorkerThread::invoke` spells the same way for a static task:
/// `AssertUnwindSafe` is the honest annotation, since the state a panicking
/// body can leave half-written is the caller's own, reachable only through
/// what the closure captured.
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
    if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| body(&mut runtime))) {
        job.record_panic(payload);
    }
}

/// The slot an async task's runtime-shaped body sits in until the one worker
/// that runs it takes it out: [`AsyncWork::Runtime`]'s payload and the `body`
/// half of [`AsyncWork::Reentrant`].
///
/// Spelled out rather than named, because `graph`'s own alias for it is
/// private to that module.
type AsyncRuntimeSlot = OnceWork<dyn for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static>;

/// Takes an async task's body out of `slot` and runs it against `runtime`.
///
/// Taken rather than called through, because an async body is a `FnOnce` and
/// a `FnOnce` cannot be called through a shared reference -- the same step
/// the plain arm in `executor::async` takes, and `None` means the same thing
/// there as here: a body already spent, which one submission and one job
/// cannot reach, and which doing nothing is the only answer to that cannot
/// run a body twice.
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
    /// [`Executor::spawn`]'s name, for the reason `executor::async` gives.
    /// Everything that call says about where the body runs, about dropping
    /// the handle to detach and about awaiting it as a
    /// [`Future`] holds here too.
    ///
    /// What is different is the body and the answer. The body is handed a
    /// [`Runtime`], so it can do what a runtime task of a graph can do --
    /// [`Runtime::schedule`] has the one limit, and since #92 it can also
    /// spawn children of its own and [`Runtime::corun`] on them. The
    /// handle therefore carries no value: `AsyncHandle<()>` where
    /// [`Executor::spawn`] carries whatever the body returned, because what
    /// there is to report about a runtime-shaped task is **whether anything
    /// under it failed**. That is upstream's own shape -- a
    /// `std::promise<void>` -- and it is what the second entry exists for.
    ///
    /// # What a caller sees
    ///
    /// [`AsyncHandle::wait`] answers `Ok(())` once the body *and* everything
    /// it spawned are done, and `Err(`[`RunError::Panicked`]`(payload))` if
    /// the body panicked or anything it spawned did (#92). Only the
    /// first such failure is reported, as everywhere else in the crate; the
    /// node is an explicit anchor from the moment it is created, which is
    /// what makes it the thing the panic walk stops at, and the module docs'
    /// anchor section says why that is a construction argument rather than a
    /// guard -- and what one `corun` inside the body does to it.
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
        // Upstream's `std::promise<void> p; auto fu{p.get_future()};` and
        // the `reentered` branch that fills it, which is
        // `runtime_async::completion` because four submissions need exactly
        // that pair (#92). The *wrapper* half of the handle seam --
        // `AsyncHandle::for_body`, which is what `Executor::spawn` uses -- is
        // deliberately not used: a runtime-shaped task's outcome is decided
        // on its finishing entry out of the node's payload slot, not by what
        // the body returned, so what this needs is the writing end of the
        // slot and no wrapper at all.
        let (handle, complete) = completion();

        self.schedule_async_task(
            AsyncWork::reentrant(body, complete),
            &Parent::Run,
            // Upstream's `ESTATE::EXPLICITLY_ANCHORED` argument, and the one
            // place in the crate that bit is set other than by a waiting
            // frame's guard: there *is* a waiter here -- whoever holds the
            // handle -- and it is waiting for the whole of what runs under
            // this node, for as long as the node exists.
            Cancellable::EXPLICITLY_ANCHORED,
        );
        handle
    }

    /// Runs `body` on this executor as soon as a worker picks it up, handing
    /// it the scheduler, with no handle on it at all.
    ///
    /// Upstream's `silent_async` over a `[](tf::Runtime&){}` body -- the
    /// `is_runtime_task_v<F>` branch of `_silent_async` -- and the cheaper
    /// half of the pair, for
    /// [`Executor::silent_async`](crate::Executor::silent_async)'s reason:
    /// there is no slot to deliver into, so a submission costs the node and
    /// the body and nothing else. There is no second entry either, which is
    /// upstream's case 1 against case 2: the node finishes when the body and
    /// everything under it are done, and nothing is reported.
    ///
    /// # What a caller sees
    ///
    /// Nothing, which is what "silent" means. A panic out of `body` -- or out
    /// of anything it spawned (#92) -- is **dropped**, as a
    /// [`Executor::silent_async`](crate::Executor::silent_async) body's is
    /// and for the same reason: the payload lands in the node's own slot and
    /// goes with the node, because no handle was ever handed out to read it
    /// from. The default panic hook still prints it, so a silent task that
    /// fails is not an invisible one. [`Executor::wait_for_all`] is the one
    /// thing that waits for such a task, as it is for every free node.
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
        // Upstream's `_silent_async`, whose runtime branch and whose static
        // branch are one call with one `ESTATE::NONE`: the shape of the body
        // is the whole difference, and `AsyncWork` is where it is said.
        self.schedule_async_task(AsyncWork::runtime(body), &Parent::Run, ESTATE_NONE);
    }
}

impl Runtime<'_> {
    /// Schedules `task` onto this worker's queue at once, bypassing the
    /// graph's dependencies: upstream's `Runtime::schedule`.
    ///
    /// `task` must be an *active* task -- a task of the taskflow this runtime
    /// task is itself part of, which is to say of a run that is in flight. It
    /// may or may not be running already; what this does is put it in the
    /// queue of the worker running this body, which is why upstream describes
    /// it as scheduling "immediately".
    ///
    /// # The same-graph limit
    ///
    /// `task` has to be a task of **the same graph as the runtime task**.
    /// Upstream's `tf::Task` is a pointer to a node, so its `schedule` can
    /// reach any node of any graph and read the parent and topology stored on
    /// it; a [`Task`] here is an index into one arena, and the thing that
    /// resolves it is the graph this job already holds. The limit is
    /// therefore what makes the accounting answerable at all: the parent to
    /// count the scheduled task against is this node's own parent -- the run,
    /// for a top-level task, and the enclosing node for a task of a nested
    /// graph -- which is upstream's `node->_parent ? .. : node->_topology`
    /// with the two cases decided by the job rather than by a null check.
    ///
    /// The limit cannot be fully checked, and that is worth being plain
    /// about: a `Task` is a bare index, so a task of *another* graph whose
    /// index happens to be live in this one would schedule this graph's node
    /// at that index. What is checked is every index this graph does not
    /// have, which panics as any stale index does, and the README records the
    /// decision. Upstream has one more unchecked caveat of its own here --
    /// that only the parent worker of the runtime may call this, which the
    /// `&mut self` on a handle that cannot leave its own invocation is this
    /// port's answer to. Its other, that scheduling a *runtime* task is
    /// "currently not supported", is half-diagnosed as of issue #107: a
    /// runtime task that is **entered** is refused below, and one that is
    /// not is an ordinary target.
    ///
    /// # A bypassed dependency is still an edge
    ///
    /// Zeroing the target's join counter bypasses what it owes; it does not
    /// remove the edges that made it owe anything. A strong predecessor of
    /// the target that has yet to run therefore still releases it when it
    /// finishes, and that release lands on the zero stored here. Issue #98
    /// is what used to happen then: upstream's release arm is a bare
    /// `fetch_sub` on a `size_t`, so the counter wrapped to `SIZE_MAX`, and
    /// a counter past zero never again equals the one
    /// [`Runtime::try_corun`] waits for nor the zero a preempted node is
    /// resumed on -- a silent wrap one `schedule` and one `precede` away
    /// from a hang. This port floors that decrement at zero instead, so the
    /// zero stored here is the zero the target is dequeued with, and the
    /// departure is noted where the floor is.
    ///
    /// What the floor does not and cannot do is make the *order* harmless,
    /// and the order has three outcomes. Which one a caller gets turns on
    /// what the target was doing when the bypass reached it a second time --
    /// the second `schedule`, or the predecessor's release:
    ///
    /// 1. **Its body is running.** Two workers in one body is the one class
    ///    of this the work handles arbitrate themselves:
    ///    [`Work::invoke`](crate::Work::invoke) takes the body with
    ///    `try_lock` and panics rather than blocking, naming the invariant.
    ///    A bypassed *dependency* cannot reach it -- the target's counter is
    ///    the zero stored here for as long as its body runs, so the
    ///    floored decrement queues nothing -- but a second `schedule`
    ///    racing the first one's run can, and that is what the panic is for.
    /// 2. **It has preempted itself**: a runtime, module or subflow task
    ///    whose nested work is outstanding, which is upstream's dangerous
    ///    half. The body has returned, so no lock is held and nothing
    ///    arbitrates; what the node is holding instead is one join-counter
    ///    reference per piece of outstanding work, and a bypass that spends
    ///    or zeroes those resumes the node before its own children are done.
    ///    Both bypasses are therefore **refused and reported** rather than
    ///    delivered: this method panics on an *entered* target, and the
    ///    successor arm of the dispatch drops a strong release onto one,
    ///    recording it as the failure of the task that released. "Entered"
    ///    is the `PREEMPTED` bit on a node of a kind whose arm sets it,
    ///    which is `executor::worker`'s `is_entered` and which that function
    ///    argues is both halves of one question. Each asks it before it
    ///    touches the counter, so a node that preempts itself in the window
    ///    after that read is not seen -- and nothing turns a node that was
    ///    never entered into a refusal, since a node still owing a strong
    ///    dependency cannot have been entered by anything but a bypass.
    /// 3. **It has finished and re-armed** its counter to its strong
    ///    dependency count. Then the predecessor's release is an *ordinary*
    ///    one: the decrement reaches zero legitimately and the target runs
    ///    **a second time**, which is the caller's own doing and is not
    ///    diagnosed. Nothing on a plain node records that it has already
    ///    run, the second run is strictly after the first, and so the lock
    ///    in (1) is free by then and has nothing to report. Upstream neither
    ///    diagnoses this nor survives it, and its own answer is the rule its
    ///    `ConditionalTasking` page states for the other bypass:
    ///    **schedule a task the graph cannot otherwise reach.** Upstream's
    ///    example gates the target's one strong predecessor behind a
    ///    condition that is never taken, and this crate's
    ///    `Runtime.Schedule.ModuleTask` port is built the same way.
    ///
    /// A conditioner is the other bypass and reaches (2) and (3) the same
    /// way, since the arm that selects a successor stores the same zero this
    /// method does. The refusal above covers the release it is given, not
    /// the selection: a conditioner that selects a node of its own nested
    /// work stays upstream's rule rather than this port's check.
    ///
    /// # Panics
    ///
    /// Panics if `task` is not a live task of this runtime task's own graph
    /// -- out of range, erased, or a task of another graph with an index this
    /// one does not have. The panic is
    /// [`Graph::node`](crate::Graph::node)'s.
    ///
    /// Panics if `task` names a node that has already preempted itself,
    /// which is case (2) above.
    ///
    /// Either reaches the caller as the failure of this runtime task, since
    /// it happens inside the body.
    pub fn schedule(&mut self, task: Task) {
        // Upstream's comment, and the invariant it names: "when scheduling a
        // task, the task must have zero dependency (join counter is 0) or we
        // can encounter bug when inserting a nested flow (e.g., module
        // task)". A stored zero rather than an arithmetic one, for the reason
        // a conditioner's selected successor gets one: the count the target
        // is carrying is exactly what this call means to bypass, so there is
        // no arithmetic that would arrive at zero. What may still decrement
        // the zero afterwards is a strong predecessor of the target, which
        // the docs above work through -- it is issue #98, and the release arm
        // is where the answer to it lives.
        let target = self.job.graph().node(task);

        // Issue #107, and the one case this call can see for itself: the
        // store below would wipe a count that is not a dependency count at
        // all -- a preempted node's counter holds one reference per piece of
        // nested work still outstanding, and work that finished against a
        // zeroed counter would drive it past zero rather than resume the
        // node. Refused before anything is written, so a body that schedules
        // a node it should not have scheduled fails *as a body*: the panic is
        // this runtime task's, through `run_body`'s handler, rather than the
        // hang the store would have left. `is_entered` is the same question
        // the successor arm asks before its own decrement, asked here of the
        // same node; the docs above have the two orderings neither of them
        // can see.
        assert!(
            !is_entered(target),
            "a task that is already running cannot be scheduled: a bypassed \
             dependency may not re-enter a task the run has already entered"
        );

        target.join_counter().store(0, Ordering::Relaxed);

        // Before the push, as every other count of a queued job is: the job
        // may run and finish before this call returns, and a parent that had
        // not been told about it could reach zero with the job still in
        // flight.
        self.job.parent().count_queued(self.job.topology(), 1);

        // Upstream's `_executor._schedule(_worker, node)`: this worker's own
        // queue. `Job::successor` is the job of a task of the same graph,
        // same run and same parent -- which is what a sibling is, whatever
        // the edges say.
        self.thread.schedule(self.job.successor(task));
    }

    /// Waits for everything this runtime task has spawned, reporting a
    /// failure under it rather than raising it: upstream's `corun`, in the
    /// `try_` shape issue #48 established.
    ///
    /// The calling worker stays in the work-stealing loop while it waits, for
    /// the reason the [`corun`](crate::executor::corun) module docs give: a
    /// worker that parked here would be a worker the work it is waiting for
    /// cannot use. The predicate is upstream's -- the node's join counter
    /// back to one, that one being this body's own reference -- so a body
    /// that has spawned nothing returns immediately, which in this milestone
    /// is every body there is.
    ///
    /// Being an *equality* rather than a `<=`, the predicate is only as good
    /// as the guarantee that nothing drives the counter below the reference
    /// the first entry added. Two things in the successor arm of
    /// `WorkerThread::invoke_one` make that guarantee. It floors its
    /// decrement at zero, which is issue #98, so a node whose counter was
    /// zeroed by [`Runtime::schedule`] or by a conditioner still enters its
    /// body at exactly one rather than at `usize::MAX + 1`; and it drops
    /// altogether a strong release onto a node that has already preempted
    /// itself, which is issue #107 and the one remaining way to lose the
    /// reference --
    /// a predecessor releasing this node while this body runs, after
    /// something bypassed the dependency it was owed. That release is
    /// reported as the releasing task's failure instead, so what used to be
    /// a corun that never terminated is a run that answers.
    ///
    /// The node is explicitly anchored for the length of the wait, which is
    /// what makes a failure under it *this frame's*: the panic walk in
    /// `Job::record_panic` stops at the first explicit anchor it meets, so
    /// the payload is handed to the thread that is about to look for it
    /// rather than to the run. Taking the payload clears the node's slot, as
    /// upstream's `_rethrow_exception` does, so a second corun of the same
    /// body starts clean.
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
    /// which is the pair [`Executor::corun`](crate::Executor::corun) and
    /// [`Executor::try_corun`](crate::Executor::try_corun) established and
    /// the shape that composes: a body that wrapped this call in nothing has
    /// the failure propagate out of itself, where the executor catches it as
    /// this task's own failure and the run reports it.
    ///
    /// # Panics
    ///
    /// Panics with the payload of the first piece of work spawned by this
    /// runtime task to panic. The payload keeps its identity, because it goes
    /// to [`std::panic::resume_unwind`] rather than to a fresh `panic!`.
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

    /// [`Runtime::corun`], under upstream's older name for it.
    ///
    /// Upstream's `corun_all` is one line -- `corun();` -- kept "just an
    /// alias for legacy purpose", in its words, and it is ported because its
    /// own tests and documentation still use it.
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
    /// Both terms of upstream's condition are here, as
    /// `Job::is_parent_cancelled` has them -- the run's own cancellation
    /// bits, and the estate of every parent above this job -- which is why a
    /// runtime task inside a nested graph sees a failure in that graph as
    /// well as a cancel of the whole run.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.job.is_parent_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, OnceLock};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::executor::async_node::AsyncNode;
    use crate::executor::queue::WorkerQueue;
    use crate::executor::topology::{GraphOwner, Topology, set_up_graph};
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
    /// `Executor::schedule_async_task` is the real submission and takes the
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

    /// A body that returns without spawning anything is the whole of a
    /// runtime task in this milestone: both bits come off on the first entry
    /// and the node finishes like a static task, with its successors released
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
    /// already at the body's own one reference. That is every corun in this
    /// milestone, which is why the branch above is tested by hand rather than
    /// through one of these.
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
}

//! Translated from `taskflow/core/graph.hpp` (Taskflow v4.1.0).
//!
//! Upstream's `class NodeBase` and `class Node`, together with the
//! `Node::Semaphores` and `ExplicitAnchorGuard` that belong to them.

use core::fmt;
use std::any::Any;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use super::{Handle, Task};
use crate::executor::topology::{Cancellable, Job};
use crate::semaphore::{Semaphore, SemaphoreError};

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

/// A single task of a [`Graph`](super::Graph): its name, its work, its edges
/// and the state words a run mutates.
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
    /// Creates a node outside any graph. Private to the `graph` module because
    /// the edge lists are arena indices, which are meaningless until
    /// [`Graph`](super::Graph) owns the node.
    pub(super) fn new(name: String, handle: Handle) -> Self {
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
    /// the swap [`Graph::add_edge`](super::Graph::add_edge) performs.
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
    /// asked, through [`Subflow::set_retain`](crate::Subflow::set_retain),
    /// for its subgraph to survive the join instead of being cleared.
    pub const RETAIN_SUBFLOW: u32 = 0x4000_0000;

    /// Upstream's `NSTATE::JOINED_SUBFLOW`: set on a subflow node whose
    /// subgraph has been joined, which is what makes
    /// [`Subflow::joinable`](crate::Subflow::joinable) answer `false`.
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
    pub(super) fn push_successor(&mut self, to: Task) {
        self.edges.push(to);
        let last = self.edges.len() - 1;
        self.edges.swap(self.num_successors, last);
        self.num_successors += 1;
    }

    /// Appends `from` to the predecessor half.
    pub(super) fn push_predecessor(&mut self, from: Task) {
        self.edges.push(from);
    }

    /// Drops every occurrence of `target` from the successor half, returning
    /// how many went. Upstream's `Node::_remove_successors`.
    pub(super) fn remove_successors(&mut self, target: Task) -> usize {
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
    pub(super) fn remove_predecessors(&mut self, target: Task) -> usize {
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

#[cfg(test)]
mod tests {
    use super::DispatchClaim;

    /// The claim's transitions in isolation, where no race is needed to see
    /// them: `tests/loom_dispatch_claim.rs` models the concurrent half, and a
    /// run shows only the job a bypass queued or did not. #310 set the bar for
    /// these four cases.
    #[test]
    fn the_first_bypass_takes_the_claim_and_is_told_to_queue() {
        let claim = DispatchClaim::new();

        assert!(claim.wake(), "nothing holds a fresh claim");
        // The dispatch that bypass queues finds the claim already taken, so
        // its own `claim` changes nothing and its `release` has no requeue to
        // report: one bypass, one job.
        claim.claim();
        assert!(!claim.release());
        assert!(claim.wake(), "the claim is idle again");
    }

    #[test]
    fn a_bypass_onto_a_held_claim_defers_to_that_dispatch() {
        let claim = DispatchClaim::new();
        claim.claim();

        assert!(!claim.wake(), "the holder owes the requeue, not the bypass");
        assert!(claim.release(), "and `release` is where it learns that");
    }

    #[test]
    fn two_bypasses_onto_a_held_claim_owe_exactly_one_requeue() {
        let claim = DispatchClaim::new();
        claim.claim();

        assert!(!claim.wake());
        assert!(!claim.wake(), "the second defers to the same release");
        assert!(
            claim.release(),
            "the second bypass left the first one's pending flag where \
             `release` finds it"
        );
        // Once, not twice: the dispatch the requeue queues finds nothing
        // pending, so the two bypasses cost one extra run of the node.
        claim.claim();
        assert!(!claim.release());
    }

    #[test]
    fn a_reported_requeue_leaves_the_claim_held() {
        let claim = DispatchClaim::new();
        claim.claim();
        assert!(!claim.wake());
        assert!(claim.release());

        // Held rather than idle, so a bypass arriving between the requeue and
        // the dispatch it queues is told to defer in its turn instead of
        // queueing a second job for the same dispatch.
        assert!(!claim.wake());
        assert!(claim.release());
        assert!(!claim.release(), "nothing is pending now");
    }
}

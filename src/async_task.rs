//! Translated from `taskflow/core/async_task.hpp` (Taskflow v4.1.0).
//!
//! One type, and it is a smart pointer: upstream's `tf::AsyncTask` is a
//! `Node*` plus a hand-rolled reference count packed into the low 24 bits of
//! the node's `_estate`, with `_incref`/`_decref` on every copy, move and
//! destruction and a `recycle(_node)` when the count reaches zero. The whole
//! of that is an `Option<Arc<AsyncNode>>` here, which is why this module is
//! short where upstream's header is not: `Clone` and `Drop` are the counting,
//! and the node going back to its allocator when the last share goes is
//! `Arc`'s own business.
//!
//! What the packing bought upstream is worth saying, because the port gives
//! it up on purpose. Sharing the estate word means a count that costs no
//! extra cache line and needs no `std::get_if` on the handle's hot path, at
//! the price of a 24-bit ceiling on owners and of a count only a
//! `DEPENDENT_ASYNC` node may be asked for. An `Arc`'s count is a word of its
//! own in the same allocation as the node, which is the same cache line in
//! practice, has no ceiling, and is the one mechanism in the crate every
//! reader already knows.
//!
//! The handle exists for the ABA problem upstream's own docs name: a
//! dependent-async task named as the dependency of a *later* submission has
//! to still be there when the later one looks at it, and a count held by the
//! caller is what guarantees that. [`AsyncTask::use_count`] is therefore the
//! number of shares and not the number of handles -- see its docs.

use core::fmt;
use core::hash::{Hash, Hasher};
use std::any::Any;
use std::hash::DefaultHasher;
use std::sync::Arc;

use crate::executor::async_node::AsyncNode;

/// A handle with shared ownership of one async task: upstream's
/// `tf::AsyncTask`.
///
/// Empty by default, which is upstream's `_node == nullptr`, and otherwise a
/// share of the free-standing node the task is. Cheap to clone -- one atomic
/// increment -- and compares, hashes and prints by node identity, so two
/// clones of one handle are one task and two handles on two tasks are never
/// equal.
///
/// What it is *not* is a way to reach the task's body or its result. Upstream
/// has no such accessor either: a value-returning `async` hands back a
/// `std::future` and this handle is what `silent_dependent_async` hands back,
/// so the handle answers only the questions that are about the task's
/// *identity and state* -- is it done, how many owners are there, and did it
/// fail silently.
#[derive(Default)]
pub struct AsyncTask(Option<Arc<AsyncNode>>);

impl AsyncTask {
    /// The handle on `node`, taking a share of it.
    ///
    /// Crate-private, as upstream's `explicit AsyncTask(Node*)` is private to
    /// its `friend class Executor`: a handle is only ever minted by the
    /// submission that created the task, because a handle on a node nobody
    /// submitted would be a task that is never going to finish and whose
    /// [`AsyncTask::is_done`] would never become `true`.
    // Nothing mints a handle yet; `executor::async_node` says what the
    // attribute is and why it is `expect` under `not(test)`.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the call that mints one is #91")
    )]
    pub(crate) fn new(node: Arc<AsyncNode>) -> Self {
        Self(Some(node))
    }

    /// Whether this handle is associated with no task at all: upstream's
    /// `empty()`.
    #[must_use]
    pub fn empty(&self) -> bool {
        self.0.is_none()
    }

    /// Releases the task this handle manages, if any: upstream's `reset()`.
    ///
    /// Afterwards the handle is [`AsyncTask::empty`] and the share is gone,
    /// which for the last owner of a task that has already run is what frees
    /// the node. A task that has *not* run yet is not cancelled by this and
    /// is not freed either -- the job queued for it holds a share of its own.
    pub fn reset(&mut self) {
        self.0 = None;
    }

    /// How many shares of this task are outstanding: upstream's
    /// `use_count()`, which is `0` for an empty handle.
    ///
    /// **Handles plus the executor's own shares**, and the distinction
    /// matters for anyone reading the number: a task that is queued or
    /// running is named by a [`Job`](crate::executor) the scheduler holds,
    /// which is a share like any other. Upstream's count includes exactly the
    /// same thing -- it is incremented when the node is created for the queue
    /// and decremented by the worker that finishes it, which is what its own
    /// docs mean by "when a worker completes an async task, it will remove the
    /// task from the executor, decrementing the number of shared owners by
    /// one". So two clones of a handle on a task still in the queue read
    /// three, not two, and the number is a liveness fact rather than a count
    /// of your own variables.
    ///
    /// Relaxed, as upstream's load is.
    #[must_use]
    pub fn use_count(&self) -> usize {
        self.0.as_ref().map_or(0, Arc::strong_count)
    }

    /// Whether the task has finished: upstream's `is_done()`, which is `true`
    /// for an empty handle.
    ///
    /// `true` for an empty handle is upstream's answer and is the useful one:
    /// a handle that has been [`reset`](AsyncTask::reset) is no longer
    /// waiting for anything, so a caller spinning on this gets out rather
    /// than spinning for ever. It says nothing about whether the task
    /// *succeeded* -- [`AsyncTask::take_panic`] is that question.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.0.as_ref().is_none_or(|node| node.is_finished())
    }

    /// The hash of this handle's task identity: upstream's `hash_value()`,
    /// which is `std::hash<Node*>{}(_node)` and therefore hashes the null
    /// pointer for an empty handle.
    ///
    /// A `u64` where upstream's is a `size_t`, because that is what Rust's
    /// [`Hasher`] produces on every target. The value is this port's
    /// [`Hash`] impl run through the default hasher, so it agrees with
    /// whatever a [`HashMap`](std::collections::HashMap) of handles computes
    /// -- which is the only promise a hash of an address can keep, since
    /// neither upstream's nor this one is stable across runs.
    #[must_use]
    pub fn hash_value(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    /// Takes the payload of the task's own failure, if it failed and nothing
    /// above it collected the failure: upstream's `exception_ptr()`.
    ///
    /// This is the one accessor on the handle that reads the *node* rather
    /// than its state word, and the case it exists for is upstream's own: a
    /// **silent** async task has no future and no frame waiting on it, so a
    /// panic out of its body has nowhere to be delivered and is parked on the
    /// node -- `Job::record_panic`'s last case, which such a task is the one
    /// thing that reaches. This hands it back to whoever still holds a
    /// handle.
    ///
    /// It is not the `Task::exception_ptr` the README declines to port, and
    /// the difference is which failure the slot holds. A *graph* node's slot
    /// is an anchor's: several tasks under one anchor can fail at once, only
    /// the first claims it, and handing back a race loser would be reporting
    /// an arbitrary one of several failures. A free node's slot holds the one
    /// failure of the one task that node is, so there is no race to lose and
    /// nothing arbitrary to report.
    ///
    /// Takes rather than peeks, as [`RunHandle::wait`](crate::RunHandle)
    /// does: the payload is a `Box<dyn Any + Send>` that only one caller can
    /// own, so a second call hands back `None`. `None` is also the answer for
    /// an empty handle and for a task that has not failed.
    #[must_use]
    pub fn take_panic(&self) -> Option<Box<dyn Any + Send>> {
        self.0.as_ref().and_then(|node| node.take_panic())
    }

    /// The address this handle's identity is: the node, or null for an empty
    /// handle.
    ///
    /// Upstream compares and hashes `Node*` directly, and a null pointer for
    /// an empty handle is what makes two empty handles equal to each other
    /// without a case of its own.
    fn identity(&self) -> *const AsyncNode {
        self.0.as_ref().map_or(core::ptr::null(), Arc::as_ptr)
    }
}

impl Clone for AsyncTask {
    /// Upstream's copy constructor, whose `_incref` this is.
    ///
    /// Hand-written rather than derived only so that this doc comment can say
    /// that; `Option<Arc<_>>` clones exactly the same way.
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl PartialEq for AsyncTask {
    /// By node identity, which is upstream's `_node == rhs._node` -- the
    /// comparison its `std::hash<Node*>` is the hash of.
    ///
    /// Upstream declares no `operator==` at all; the port has one because a
    /// handle that hashes has to agree with how it compares, and because
    /// naming dependencies means putting handles in sets.
    fn eq(&self, other: &Self) -> bool {
        core::ptr::eq(self.identity(), other.identity())
    }
}

impl Eq for AsyncTask {}

impl Hash for AsyncTask {
    /// By node identity, so that it agrees with [`PartialEq`]: upstream's
    /// `hash_value()`, which is where this is read back out.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.identity().hash(state);
    }
}

impl fmt::Debug for AsyncTask {
    /// Hand-written because the derived one would print the node, and through
    /// it the one-node graph and whatever the body captured. The address and
    /// the two state questions are what identifies a handle.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("AsyncTask");
        match &self.0 {
            None => debug.field("node", &Option::<&AsyncNode>::None).finish(),
            Some(node) => debug
                .field("node", &Arc::as_ptr(node))
                .field("done", &node.is_finished())
                .field("use_count", &Arc::strong_count(node))
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{AsyncWork, Handle};

    /// A dependent-async task's node, which is what a handle is handed out
    /// for: upstream mints one from `silent_dependent_async`.
    fn node() -> Arc<AsyncNode> {
        AsyncNode::new("task", Handle::DependentAsync(AsyncWork::plain(|| {})))
    }

    /// Upstream's documented pair, from the `empty()` and `use_count()`
    /// examples: `tf::AsyncTask task; assert(task.empty());
    /// assert(task.use_count() == 0);` -- plus `is_done()`, whose own example
    /// asserts that a reset handle reports finished.
    #[test]
    fn an_empty_handle_owns_nothing_and_is_done() {
        let empty = AsyncTask::default();

        assert!(empty.empty());
        assert!(empty.is_done(), "nothing is pending for an empty handle");
        assert_eq!(empty.use_count(), 0);
        assert!(empty.take_panic().is_none());

        // Two empty handles are one thing, which is upstream's null pointer
        // compared against itself.
        let other = AsyncTask::default();
        assert_eq!(empty, other);
        assert_eq!(empty.hash_value(), other.hash_value());
    }

    /// Two clones of one handle are one task: equal, equally hashed, and two
    /// shares of the node. The count is what upstream's `_incref` keeps and
    /// is the whole reason the handle exists -- a task named as another's
    /// dependency has to be kept alive by the namer.
    #[test]
    fn two_clones_of_one_handle_are_one_task_counted_twice() {
        let task = AsyncTask::new(node());
        let clone = task.clone();

        assert!(!task.empty());
        assert_eq!(task, clone);
        assert_eq!(task.hash_value(), clone.hash_value());
        assert_eq!(task.use_count(), 2);
        assert_eq!(clone.use_count(), 2);

        // And a handle on a *different* task is a different thing, however
        // identical the two bodies are.
        let other = AsyncTask::new(node());
        assert_ne!(task, other);
        assert_eq!(other.use_count(), 1);
    }

    /// Upstream's `reset()` example: the handle empties and gives its share
    /// up, which the other clone's count is what observes.
    #[test]
    fn resetting_a_handle_drops_its_share() {
        let mut task = AsyncTask::new(node());
        let clone = task.clone();
        assert_eq!(clone.use_count(), 2);

        task.reset();

        assert!(task.empty());
        assert!(task.is_done(), "an emptied handle waits for nothing");
        assert_eq!(task.use_count(), 0);
        assert_eq!(clone.use_count(), 1, "the share went with the reset");
        assert_ne!(task, clone, "an empty handle is not the task it dropped");
    }

    /// `is_done` is the node's `FINISHED` bit, and `take_panic` is the node's
    /// own payload slot: the two questions a handle answers about a *silent*
    /// task, which has no future to raise through.
    #[test]
    fn a_handle_reports_its_tasks_state_and_takes_its_one_failure() {
        let node = node();
        let task = AsyncTask::new(Arc::clone(&node));
        assert!(!task.is_done());
        assert!(task.take_panic().is_none(), "nothing has failed");

        // What a failing body leaves behind, through the one channel that may
        // write the slot: claim it, then keep the payload.
        assert!(node.node().base().claim_panic());
        node.node().base().keep_panic(Box::new("oops"));

        assert!(node.finish().is_empty(), "nothing was waiting on it");
        assert!(task.is_done());

        let payload = task.take_panic().expect("the task's own failure");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"oops"));
        assert!(task.take_panic().is_none(), "the payload is taken once");
    }
}

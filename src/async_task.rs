//! Translated from `taskflow/core/async_task.hpp` (Taskflow v4.1.0).
//!
//! One type, and it is a smart pointer: upstream's `tf::AsyncTask` is a
//! `Node*` plus a reference count packed into the low 24 bits of the node's
//! `_estate`, with `_incref`/`_decref` on every copy, move and destruction
//! and a `recycle(_node)` when it reaches zero. All of that is an
//! `Option<Arc<AsyncNode>>` here -- `Clone` and `Drop` are the counting.
//!
//! The handle exists for the ABA problem upstream's own docs name: a
//! dependent-async task named as the dependency of a *later* submission has
//! to still be there when the later one looks at it.
//!
//! # Design
//!
//! What the packing bought upstream and what giving it up costs is
//! `DESIGN.md`'s entry on the handle being that `Arc`; the consequence a
//! caller sees is [`AsyncTask::use_count`].

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
/// share of the free-standing node the task is. It compares, hashes and
/// prints by node identity, so two clones of one handle are one task.
///
/// What it is *not* is a way to reach the task's body or its result, and
/// upstream has no such accessor either: it answers only what is about
/// *identity and state* -- is it done, how many owners, did it fail silently.
#[derive(Default)]
pub struct AsyncTask(Option<Arc<AsyncNode>>);

impl AsyncTask {
    /// The handle on `node`, taking a share of it. Crate-private, as
    /// upstream's `explicit AsyncTask(Node*)` is to its `friend class
    /// Executor`: only the submission that created a task mints a handle on
    /// it, one on an unsubmitted node never becoming [`AsyncTask::is_done`].
    pub(crate) fn new(node: Arc<AsyncNode>) -> Self {
        Self(Some(node))
    }

    /// The free-standing node this handle shares, `None` for an empty handle:
    /// upstream's `task._node`, reached from `_process_dependent_async`
    /// through its `friend class Executor`.
    ///
    /// The one caller is `executor::dependent_async`, pushing the job of a
    /// new task onto the successor list of every dependency named, and `None`
    /// is upstream's `task.empty()` branch -- the dependency it counts off
    /// instead of waiting for. A share rather than a borrow, that caller
    /// building a [`Job`](crate::executor) that keeps the node alive.
    pub(crate) fn node(&self) -> Option<&Arc<AsyncNode>> {
        self.0.as_ref()
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
    /// which for the last owner of a task that has run frees the node. A task
    /// that has *not* run is neither cancelled nor freed: the job queued for
    /// it holds a share of its own.
    pub fn reset(&mut self) {
        self.0 = None;
    }

    /// How many shares of this task are outstanding: upstream's
    /// `use_count()`, which is `0` for an empty handle.
    ///
    /// **Handles plus the executor's own shares**: a task that is queued or
    /// running is named by a [`Job`](crate::executor) the scheduler holds,
    /// which is a share like any other, so two clones of a handle on a task
    /// still in the queue read three. Upstream's count includes the same
    /// thing, which its own docs say from the other side. The number is a
    /// liveness fact rather than a count of your own variables.
    #[must_use]
    pub fn use_count(&self) -> usize {
        self.0.as_ref().map_or(0, Arc::strong_count)
    }

    /// Whether the task has finished: upstream's `is_done()`, which is `true`
    /// for an empty handle.
    ///
    /// `true` for an empty handle is upstream's answer and the useful one, a
    /// handle that has been [`reset`](AsyncTask::reset) waiting for nothing.
    /// It says nothing about whether the task *succeeded* --
    /// [`AsyncTask::take_panic`] is that question.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.0.as_ref().is_none_or(|node| node.is_finished())
    }

    /// The hash of this handle's task identity: upstream's `hash_value()`,
    /// which is `std::hash<Node*>{}(_node)` and so hashes the null pointer
    /// for an empty handle. A `u64` where upstream's is a `size_t`, that
    /// being what Rust's [`Hasher`] produces, and it is this port's [`Hash`]
    /// impl run through the default hasher -- so it agrees with what a
    /// [`HashMap`](std::collections::HashMap) of handles computes, which is
    /// the only promise a hash of an address can keep.
    #[must_use]
    pub fn hash_value(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    /// Takes the payload of the task's own failure, if it failed and nothing
    /// above it collected the failure: upstream's `exception_ptr()`.
    ///
    /// The one accessor that reads the *node* rather than its state word, for
    /// upstream's own case: a **silent** async task has no future and no
    /// frame waiting on it, so a panic out of its body is parked on the node,
    /// and this hands it back to whoever still holds a handle.
    ///
    /// It is not the `Task::exception_ptr` the README declines to port: a
    /// *graph* node's slot is an anchor's, where only the first of several
    /// failures claims it, so handing one back would report an arbitrary one
    /// of them. A free node's slot holds the one failure of the one task it
    /// is.
    ///
    /// Takes rather than peeks, as [`RunHandle::wait`](crate::RunHandle)
    /// does, the payload being a `Box<dyn Any + Send>` only one caller can
    /// own: a second call hands back `None`, as do an empty handle and a task
    /// that has not failed.
    #[must_use]
    pub fn take_panic(&self) -> Option<Box<dyn Any + Send>> {
        self.0.as_ref().and_then(|node| node.take_panic())
    }

    /// The address this handle's identity is: the node, or null for an empty
    /// handle, which is what makes two empty handles equal without a case of
    /// their own. Upstream compares and hashes `Node*` directly.
    fn identity(&self) -> *const AsyncNode {
        self.0.as_ref().map_or(core::ptr::null(), Arc::as_ptr)
    }
}

impl Clone for AsyncTask {
    /// Upstream's copy constructor, whose `_incref` this is; hand-written
    /// only so that the comment can say so, `Option<Arc<_>>` cloning the
    /// same way.
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl PartialEq for AsyncTask {
    /// By node identity, upstream's `_node == rhs._node`. Upstream declares
    /// no `operator==`; this one exists because a handle that hashes has to
    /// agree with how it compares.
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
    /// Hand-written because the derived one would print the node, and
    /// through it whatever the body captured.
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

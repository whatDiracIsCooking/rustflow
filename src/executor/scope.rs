//! A scoped spawn: [`Runtime::scope`] hands its body a [`Scope`] whose
//! children may borrow from the frame that opened it, and every one of them
//! is joined -- run to completion, or dropped unstarted -- before `scope`
//! returns.
//!
//! No `//!` provenance line: upstream has no equivalent. `tf::Runtime`'s own
//! `async` family already lets a body spawn work *under its own node*
//! (`executor::runtime_async`), but every body this port hands to it is
//! `Send + 'static`, so a body that wants to fan a borrowed slice out to a
//! handful of children has had nowhere to put them. `Scope` is that place,
//! in Rayon's shape: `rt.scope(|s| { s.spawn(|| ..); s.spawn_runtime(|rt|
//! ..); })`, where each child may capture a `&mut` into the enclosing frame
//! instead of only owning what it needs.
//!
//! # Design
//!
//! What makes a borrowed child sound to hand to
//! [`Runtime::silent_async`](crate::Runtime::silent_async), whose own bound
//! is `Send + 'static`, is one `unsafe` erasure plus a guarantee that holds
//! on every path out of [`Runtime::scope`]: the body `f` runs first, under
//! `catch_unwind` so a panic out of it does not skip the join that follows;
//! then every child `f` spawned is waited on through
//! [`Runtime::try_corun`](crate::Runtime::try_corun), the crate's *existing*
//! implicit-join primitive -- the same wait a runtime task's own body already
//! gets for free at the end of its node (`executor::runtime`'s module docs),
//! pulled one frame earlier so a borrow can end there too. Only once that
//! wait has returned does `scope` propagate whichever failure came first,
//! `f`'s own panic or a child's. Three paths, each ending in the join before
//! anything a child borrowed is freed:
//!
//! - **Normal completion.** `f` returns, every spawned child is run and
//!   joined by the wait, and `scope` returns `f`'s value.
//! - **A panic**, in `f` itself or in a child. The wait still runs --
//!   `catch_unwind` around `f` and the unconditional call after it see to
//!   that -- so every child is joined before the first failure is re-raised
//!   on this frame, through [`std::panic::resume_unwind`] rather than a fresh
//!   `panic!`, which keeps the original payload.
//! - **A cancelled run.** A child queued but never picked up takes the
//!   cancellation early-out `executor::worker`'s dispatch already has for
//!   every async task: its body is never called, and is dropped instead --
//!   *first*, and only then is the job counted off the very join counter the
//!   wait above reads (issue #416: a `Drop` in what the body captured can
//!   write through the borrow as freely as the body could, so the count
//!   that releases the wait has to come after it). "Dropped, not leaked into
//!   a queue" is that drop, not a second mechanism built for this module.
//!
//! The `unsafe` is the two [`core::mem::transmute`] calls in
//! [`Scope::spawn`] and [`Scope::spawn_runtime`], each erasing a boxed
//! closure's `'scope` bound to `'static` so
//! [`Runtime::silent_async`](crate::Runtime::silent_async) and
//! [`Runtime::silent_async_runtime`](crate::Runtime::silent_async_runtime)
//! will take it. A trait object's boxed representation carries no run-time
//! trace of the lifetime in its bound -- the vtable and the data pointer are
//! the same either way -- so the two types have identical layout and the
//! transmute changes nothing a caller could observe except which bound the
//! type checker reads afterwards. What keeps that erased bound from ever
//! being acted on dishonestly is everything above: the real `'scope` outlives
//! every use the erased `'static` ever gets, because `scope` does not
//! return -- and so does not let `'scope` end -- until the wait above has.
//!
//! `Scope` cannot reach the unsoundness [`std::thread::scope`]'s own docs
//! warn against: a child that captures the scope handle itself and spawns a
//! second child borrowing a local of the *first* child, racing the join that
//! is supposed to guard it. [`Scope::spawn`]'s body is a bare `FnOnce()` with
//! no handle back to the scope at all, and [`Scope::spawn_runtime`]'s is
//! handed its *own* fresh [`Runtime`], never this scope's; a `Send` closure
//! cannot capture the scope handle either way, `Scope` holding a
//! [`RefCell`] rather than being `Sync`. A grandchild spawned through that
//! fresh `Runtime` is already joined before its own parent's node is -- the
//! crate's existing preemption protocol, which `executor::runtime`'s module
//! docs set out -- so recursion through `spawn_runtime` costs this module
//! nothing new. `tests/loom_scope.rs` models the join itself: the mutation
//! named in its own doc comment returns from a scope before the wait, and
//! loom is shown to catch the child it leaves dangling.

use std::cell::RefCell;
use std::marker::PhantomData;
use std::panic::{self, AssertUnwindSafe};

use crate::Runtime;
use crate::executor::run_handle::RunError;

/// The borrowed-spawn half of [`Runtime::scope`]: every [`Scope::spawn`] or
/// [`Scope::spawn_runtime`] call made against one of these is joined before
/// the `scope` call that built it returns.
///
/// Three lifetimes answer three different questions. `'scope` bounds what a
/// child may capture, and is deliberately *not* tied to this value's own
/// borrow: a child may capture a local `f` itself declares, one that drops
/// when `f` returns and well before the scope call that owns `'call` does.
/// `'call` is this borrow's own -- the duration of the enclosing `scope`
/// call, which the module docs' join rests on lasting at least that long.
/// `'rt` is the enclosed [`Runtime`]'s own lifetime, carried through
/// unchanged.
pub struct Scope<'scope, 'call, 'rt: 'call> {
    /// The runtime this scope's children are spawned under. A [`RefCell`]
    /// rather than a plain borrow, because `f` is handed `&Scope` -- so that
    /// `Scope::spawn` may be called more than once without `f` needing `&mut`
    /// -- and because a [`RefCell`] is what makes `Scope` `!Sync`, which the
    /// module docs' soundness argument leans on: nothing `Send` can capture a
    /// reference to this value at all.
    runtime: RefCell<&'call mut Runtime<'rt>>,
    /// Invariant over `'scope`. Nothing here can actually reach the
    /// cross-thread attack [`std::thread::Scope`]'s own identical marker
    /// defends against -- the module docs say why -- but the marker costs
    /// nothing, and it keeps this type shaped the way every other
    /// scoped-spawn is rather than asking a reader to re-derive why a weaker
    /// one would still be sound here.
    _scope: PhantomData<&'scope mut &'scope ()>,
}

impl<'rt> Runtime<'rt> {
    /// Opens a scope: runs `f` with a [`Scope`] whose children may borrow
    /// from this call's own frame, and waits for every one of them -- whether
    /// `f` did or not -- before returning.
    ///
    /// Upstream has no equivalent; Rayon's `scope` is the shape this follows,
    /// and the module docs have why it is sound over this crate's own
    /// primitives rather than upstream's.
    ///
    /// # Panics
    ///
    /// Panics with whichever payload came first: `f`'s own panic, or the
    /// first panic among the children it spawned -- with every child still
    /// joined first either way. Re-raised through
    /// [`std::panic::resume_unwind`], so the payload keeps its identity.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     // A local the body owns, not an `Arc`: `Runtime::silent_async`
    ///     // could not take a borrow of it, and `Runtime::scope` can.
    ///     let mut total = 0u32;
    ///     let answer = runtime.scope(|s| {
    ///         s.spawn(|| total = 6 * 7);
    ///         // `f`'s own return value, handed straight back by `scope`.
    ///         "done"
    ///     });
    ///     assert_eq!(answer, "done");
    ///     // The child is joined before `scope` returned the line above.
    ///     assert_eq!(total, 42);
    /// });
    ///
    /// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn scope<'call, 'scope, F, R>(&'call mut self, f: F) -> R
    where
        F: FnOnce(&Scope<'scope, 'call, 'rt>) -> R,
    {
        let scope = Scope {
            runtime: RefCell::new(self),
            _scope: PhantomData,
        };

        // `AssertUnwindSafe`: the honest annotation, as `run_body` in
        // `executor::runtime` takes it for the identical shape of call --
        // the state a panicking `f` can leave half-written is reachable only
        // through what it captured.
        let result = panic::catch_unwind(AssertUnwindSafe(|| f(&scope)));

        // Unconditional: every one of the module docs' three paths reaches
        // this line, which is the join each of them rests on.
        let joined = scope.runtime.borrow_mut().try_corun();

        match result {
            Err(payload) => panic::resume_unwind(payload),
            Ok(value) => match joined {
                Ok(()) => value,
                Err(RunError::Panicked(payload)) => panic::resume_unwind(payload),
            },
        }
    }
}

impl<'scope, 'call, 'rt> Scope<'scope, 'call, 'rt> {
    /// Spawns `body` under the enclosing [`Runtime::scope`] call, joined
    /// before it returns: upstream has no equivalent, and this is the shape
    /// [`Runtime::silent_async`](crate::Runtime::silent_async) cannot be --
    /// `body` may borrow from the frame that opened this scope instead of
    /// only owning `'static` data.
    ///
    /// A panic out of `body` is this scope's failure, exactly as
    /// [`Runtime::silent_async`](crate::Runtime::silent_async)'s is the
    /// enclosing runtime task's: it is recorded against the node
    /// [`Runtime::scope`] is running on and re-raised from there, after every
    /// sibling has still been joined.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let mut numbers = [0u64; 10];
    ///     runtime.scope(|s| {
    ///         let (left, right) = numbers.split_at_mut(5);
    ///         s.spawn(|| left.fill(1));
    ///         s.spawn(|| right.fill(2));
    ///     });
    ///     assert_eq!(numbers.iter().sum::<u64>(), 5 + 10);
    /// });
    ///
    /// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn spawn<F>(&self, body: F)
    where
        F: FnOnce() + Send + 'scope,
    {
        let boxed: Box<dyn FnOnce() + Send + 'scope> = Box::new(body);
        // SAFETY: `boxed`'s `'scope` bound is erased to `'static` so
        // `Runtime::silent_async` will take it. The two boxed trait object
        // types have the same layout -- a lifetime bound carries no runtime
        // representation -- so nothing about the value itself changes. The
        // erased bound never has to be true on its own: `Runtime::scope`
        // does not return, and so never lets the real `'scope` end, until
        // this body has been run to completion or dropped unstarted, which
        // is the join the module docs argue for on every one of its three
        // paths.
        let erased: Box<dyn FnOnce() + Send + 'static> = unsafe { core::mem::transmute(boxed) };
        self.runtime.borrow_mut().silent_async(erased);
    }

    /// Spawns `body` under the enclosing [`Runtime::scope`] call, handing it
    /// a [`Runtime`] of its own, joined before this scope returns: upstream
    /// has no equivalent, and this is the shape
    /// [`Runtime::silent_async_runtime`](crate::Runtime::silent_async_runtime)
    /// cannot be, for [`Scope::spawn`]'s reason.
    ///
    /// `body` gets a *fresh* runtime handle rather than this scope's own, so
    /// it can spawn and corun under its own node exactly as any runtime task
    /// can; it has no way to reach back into this scope by construction --
    /// the module docs say why that is what keeps the erasure below sound
    /// even for a `body` that spawns further children of its own.
    ///
    /// ```
    /// use std::sync::atomic::{AtomicU64, Ordering};
    ///
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let total = AtomicU64::new(0);
    ///     runtime.scope(|s| {
    ///         // The child is itself a runtime task, so it can open a scope
    ///         // of its own -- a fresh one, over its own children, not this
    ///         // one's -- and still borrow `total` from further up.
    ///         s.spawn_runtime(|child: &mut Runtime<'_>| {
    ///             child.scope(|inner| {
    ///                 inner.spawn(|| {
    ///                     total.fetch_add(1, Ordering::Relaxed);
    ///                 });
    ///                 inner.spawn(|| {
    ///                     total.fetch_add(1, Ordering::Relaxed);
    ///                 });
    ///             });
    ///         });
    ///     });
    ///     assert_eq!(total.load(Ordering::Relaxed), 2);
    /// });
    ///
    /// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn spawn_runtime<F>(&self, body: F)
    where
        F: for<'a> FnOnce(&mut Runtime<'a>) + Send + 'scope,
    {
        let boxed: Box<dyn for<'a> FnOnce(&mut Runtime<'a>) + Send + 'scope> = Box::new(body);
        // SAFETY: the same erasure as `Scope::spawn`, over the runtime-shaped
        // body instead of the plain one -- the justification is identical,
        // and is argued once in the module docs rather than twice here.
        let erased: Box<dyn for<'a> FnOnce(&mut Runtime<'a>) + Send + 'static> =
            unsafe { core::mem::transmute(boxed) };
        self.runtime.borrow_mut().silent_async_runtime(erased);
    }
}

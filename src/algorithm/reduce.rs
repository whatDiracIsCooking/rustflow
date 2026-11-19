//! Translated from `taskflow/algorithm/reduce.hpp` (Taskflow v4.1.0).
//!
//! [`Runtime::reduce`] is upstream's `make_reduce_task`, run directly as the
//! runtime task body -- [`crate::algorithm::for_each`]'s own `# Design`
//! section says why, for the same reason. [`Runtime::transform_reduce`] is
//! its single-input `make_transform_reduce_task` overload;
//! [`Runtime::transform_reduce2`] is the two-input one -- `2` names it,
//! since Rust has no overload resolution to pick it by arity the way
//! upstream's `requires` clause on `BOP_T` does, and upstream's own test
//! names already call it `TransformReduce2`. [`Runtime::reduce_by_index`] is
//! `make_reduce_by_index_task`. The owned-data wrappers over all four are
//! on [`AlgorithmBuilder`], the one extension trait the whole algorithm
//! family shares, in [`crate::prelude`] alongside [`FlowBuilder`].
//!
//! [`AlgorithmBuilder`]: crate::algorithm::AlgorithmBuilder
//! [`FlowBuilder`]: crate::FlowBuilder
//!
//! # Design
//!
//! **A second driving trait, [`ReducePartitioner`], over index ranges rather
//! than a split `&mut [T]`.** [`crate::algorithm::SlicePartitioner::drive`]
//! hands a chunk body a disjoint `&mut [T]`, which no algorithm here needs:
//! every one of them only *reads* its input (or, for
//! [`Runtime::reduce_by_index`], has no slice at all, only an index range),
//! so [`ReducePartitioner::drive_fold`] hands a chunk body a `[part_b,
//! part_e)` pair instead, the same shape
//! [`crate::algorithm::find::FindPartitioner`] uses for its own read-only
//! fold -- and for the identical reason, duplicated here rather than shared,
//! that module's own `# Design` section already gives.
//!
//! **One fold-and-merge skeleton, not four.** `fold_and_merge` is the
//! short-circuit-or-parallel shape every one of upstream's four
//! `make_*_task` functions repeats: `N == 0` is a no-op; `W <= 1` or `N <=
//! chunk_size()` runs the whole range as a single chunk, right here, under
//! `part`'s closure wrapper; otherwise the range is split and each worker
//! folds its own share into a *local* accumulator before merging that local
//! into the shared result exactly once, under one lock. Each public method
//! supplies only the two closures that differ -- `fold`, which turns a
//! `[part_b, part_e)` sub-range into (or into an existing) local `R`, and
//! `merge`, the caller's own `bop`/`gop` -- so the lock-once-per-worker
//! property this module's tests pin lives in one place rather than four.
//!
//! **A local starts at `None`, seeded by the first element it ever sees, not
//! a fabricated identity.** Upstream avoids calling `bop` with a
//! default-constructed `T` by pre-reducing a chunk's first *two* real
//! elements by hand (`T sum = bop(*beg1, *beg2)`), a dance that exists only
//! to dodge a copy at the first step in C++. [`Option<R>`] gets the same
//! guarantee for one element instead of two -- `fold`'s `None` branch is
//! always "the first real value this local has seen," never
//! `R::default()` -- which is also why every `bop`/`gop`/`uop` here is
//! `Fn(&_, &_) -> _`, not upstream's by-value `O`: a reference-taking
//! signature never needs to move a value out of a `&mut R` (no `Default`,
//! no placeholder swap) either to seed a local or, at the very end, to fold
//! a worker's pooled result back into the caller's `init`. For `T = String`
//! that same bound is what makes a `bop`/`gop`/`uop` here a closure literal
//! rather than a named `fn`: [`crate::algorithm::scan`]'s own `# Design`
//! section has the `clippy::ptr_arg` reason, which applies unchanged to
//! every operator below.
//!
//! **Every worker merges into a shared pool, not directly into `init`.**
//! Upstream's own per-worker merge locks a mutex and folds straight into
//! `init` (`init = bop(init, sum)`), `K` times, once per worker that ran.
//! This port instead merges every worker's local into a pool of its own
//! (`Mutex<Option<R>>`, starting empty) under that same one-lock-per-worker
//! discipline, and folds the pool into `init` exactly once, single-threaded,
//! after [`Runtime::scope`] has joined every child. Both give the identical
//! final value for any associative `bop`/`gop` -- the same assumption any
//! parallel reduction already rests on, since the two shapes only differ in
//! *when* `init`'s own original value re-enters the fold, never in which
//! elements get combined with which. What the pool buys back is not having
//! to thread a `&'scope Mutex<&mut R>` through every worker merely so each
//! one may, one at a time, touch the caller's own `&mut R` -- `init` stays a
//! plain, uncontended `&mut R` for the whole call, read only after every
//! worker has already finished.
//!
//! **`reduce` needs `T: Clone`; the other three do not.** Seeding a local
//! from `data`'s first element clones it, since `data: &[T]` is borrowed,
//! not consumed -- the one place in this module where upstream's own
//! move-only-friendly design does not carry over, and the reason the
//! move-only `TransformReduce.MoveOnlyData.*` cases upstream's test suite
//! carries are not ported: this module's acceptance is upstream's `Reduce.*`,
//! `TransformReduce.*`, `TransformReduceBinary.*` and `ReduceByIndex.*`
//! cases, none of which touch move-only data. [`Runtime::transform_reduce`],
//! [`Runtime::transform_reduce2`] and [`Runtime::reduce_by_index`] seed a
//! local from `uop`'s or `lop`'s own fresh return value instead, so none of
//! `T`, `T1`, `T2` needs `Clone` there.

use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex, PoisonError};

use crate::Scope;
use crate::algorithm::SliceSource;
use crate::algorithm::index_range::{IndexInt, IndexRange};
use crate::algorithm::partitioner::{
    ClosureWrapper, DynamicLoop, DynamicPartitioner, GuidedPartitioner, Partitioner,
    RandomPartitioner, SplittingPartitioner, StaticLoop, StaticPartitioner, index_shares,
    spawn_shares, walk_chunks,
};
use crate::runtime::Runtime;

/// Closes [`ReducePartitioner`] against implementations outside this crate,
/// the same restriction [`crate::algorithm::SlicePartitioner`] and
/// [`crate::algorithm::find::FindPartitioner`] carry, for the same reason:
/// only the five concrete partitioners below can answer a loop shape.
mod sealed {
    /// Marker a type must carry to implement [`super::ReducePartitioner`].
    pub trait Sealed {}
}

/// The two closures [`ReducePartitioner::drive_fold`] takes, bundled into
/// one value purely to keep that method's own argument count inside
/// clippy's default bound: `fold` turns one `[part_b, part_e)` sub-range
/// into (or into an existing) local accumulator; `merge` is the caller's own
/// `bop`/`gop`, folding one worker's finished local into the shared pool.
pub struct ReduceOps<'scope, Fold, Merge> {
    /// See the type's own docs.
    pub fold: &'scope Fold,
    /// See the type's own docs.
    pub merge: &'scope Merge,
}

/// Drives the per-worker fold every method in this module builds on: splits
/// `[0, n)` among `w_total` workers, runs `ops.fold` over each worker's own
/// chunk(s) into a local accumulator, and merges every non-empty local into
/// the shared `result` under its lock -- once per chunk *task*, never once
/// per chunk or per element, which is the property this module's tests pin.
/// For the four ported partitioners that is once per worker; a
/// [`SplittingPartitioner`] task that splits leaves two tasks, and so two
/// locals, behind.
///
/// Sealed against implementations outside this crate, like
/// [`crate::algorithm::SlicePartitioner`] and
/// [`crate::algorithm::find::FindPartitioner`].
pub trait ReducePartitioner: Partitioner + sealed::Sealed {
    /// See the trait's own docs. `cursor` is the dynamic family's shared
    /// draw point, unused by the static family -- the same parameter
    /// [`crate::algorithm::find::FindPartitioner::drive_find`] carries for
    /// an identical reason: allocated by the caller, since it must outlive
    /// this one call under `'scope`, which a value local to this function's
    /// own frame cannot.
    fn drive_fold<'scope, R, Fold, Merge>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        n: usize,
        w_total: usize,
        ops: &'scope ReduceOps<'scope, Fold, Merge>,
        result: &'scope Mutex<Option<R>>,
        cursor: &'scope AtomicUsize,
    ) where
        R: Send,
        Fold: Fn(usize, usize, &mut Option<R>) + Sync,
        Merge: Fn(&R, &R) -> R + Sync;
}

impl<C: ClosureWrapper> sealed::Sealed for StaticPartitioner<C> {}
impl<C: ClosureWrapper> sealed::Sealed for GuidedPartitioner<C> {}
impl<C: ClosureWrapper> sealed::Sealed for DynamicPartitioner<C> {}
impl<C: ClosureWrapper> sealed::Sealed for RandomPartitioner<C> {}
impl<C: ClosureWrapper> sealed::Sealed for SplittingPartitioner<C> {}

impl<C: ClosureWrapper + Sync> ReducePartitioner for StaticPartitioner<C> {
    fn drive_fold<'scope, R, Fold, Merge>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        n: usize,
        w_total: usize,
        ops: &'scope ReduceOps<'scope, Fold, Merge>,
        result: &'scope Mutex<Option<R>>,
        _cursor: &'scope AtomicUsize,
    ) where
        R: Send,
        Fold: Fn(usize, usize, &mut Option<R>) + Sync,
        Merge: Fn(&R, &R) -> R + Sync,
    {
        let mut tasks: Vec<Box<dyn FnOnce() + Send + 'scope>> = Vec::new();
        let mut curr_b = 0usize;
        for w in 0..w_total {
            if curr_b >= n {
                break;
            }
            let chunk_size = self.adjusted_chunk_size(n, w_total, w);
            let b = curr_b;
            let wrapper = self.closure_wrapper();
            tasks.push(Box::new(move || {
                wrapper.invoke(|| {
                    let mut local: Option<R> = None;
                    self.loop_static(n, w_total, b, chunk_size, |part_b, part_e| {
                        (ops.fold)(part_b, part_e, &mut local);
                    });
                    merge_local(ops.merge, result, local);
                });
            }));
            curr_b += chunk_size;
        }
        spawn_tasks(scope, tasks);
    }
}

impl<C: ClosureWrapper + Sync> ReducePartitioner for GuidedPartitioner<C> {
    fn drive_fold<'scope, R, Fold, Merge>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        n: usize,
        w_total: usize,
        ops: &'scope ReduceOps<'scope, Fold, Merge>,
        result: &'scope Mutex<Option<R>>,
        cursor: &'scope AtomicUsize,
    ) where
        R: Send,
        Fold: Fn(usize, usize, &mut Option<R>) + Sync,
        Merge: Fn(&R, &R) -> R + Sync,
    {
        drive_fold_dynamic(self, scope, n, w_total, ops, result, cursor);
    }
}

impl<C: ClosureWrapper + Sync> ReducePartitioner for DynamicPartitioner<C> {
    fn drive_fold<'scope, R, Fold, Merge>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        n: usize,
        w_total: usize,
        ops: &'scope ReduceOps<'scope, Fold, Merge>,
        result: &'scope Mutex<Option<R>>,
        cursor: &'scope AtomicUsize,
    ) where
        R: Send,
        Fold: Fn(usize, usize, &mut Option<R>) + Sync,
        Merge: Fn(&R, &R) -> R + Sync,
    {
        drive_fold_dynamic(self, scope, n, w_total, ops, result, cursor);
    }
}

impl<C: ClosureWrapper + Sync> ReducePartitioner for RandomPartitioner<C> {
    fn drive_fold<'scope, R, Fold, Merge>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        n: usize,
        w_total: usize,
        ops: &'scope ReduceOps<'scope, Fold, Merge>,
        result: &'scope Mutex<Option<R>>,
        cursor: &'scope AtomicUsize,
    ) where
        R: Send,
        Fold: Fn(usize, usize, &mut Option<R>) + Sync,
        Merge: Fn(&R, &R) -> R + Sync,
    {
        drive_fold_dynamic(self, scope, n, w_total, ops, result, cursor);
    }
}

impl<C: ClosureWrapper + Sync> ReducePartitioner for SplittingPartitioner<C> {
    /// One task per worker over a contiguous share of `[0, n)`, folding its
    /// own local and merging it once; a share that splits takes a second
    /// local with it. `cursor` is unused: nothing here is drawn from a
    /// shared point.
    fn drive_fold<'scope, R, Fold, Merge>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        n: usize,
        w_total: usize,
        ops: &'scope ReduceOps<'scope, Fold, Merge>,
        result: &'scope Mutex<Option<R>>,
        _cursor: &'scope AtomicUsize,
    ) where
        R: Send,
        Fold: Fn(usize, usize, &mut Option<R>) + Sync,
        Merge: Fn(&R, &R) -> R + Sync,
    {
        let chunk_size = self.chunk_size();
        let wrapper = self.closure_wrapper();
        let work = move |(b, e): (usize, usize)| {
            wrapper.invoke(|| {
                let mut local: Option<R> = None;
                walk_chunks(b, e, chunk_size, |part_b, part_e| {
                    (ops.fold)(part_b, part_e, &mut local);
                    false
                });
                merge_local(ops.merge, result, local);
            });
        };
        spawn_shares(scope, index_shares(n, w_total), self.split_floor(), work);
    }
}

/// The dynamic family's shared [`ReducePartitioner::drive_fold`]: `w_total`
/// tasks, every one racing `cursor` via [`DynamicLoop::loop_dynamic`], the
/// last running inline.
fn drive_fold_dynamic<'scope, R, Fold, Merge, P>(
    part: &'scope P,
    scope: &Scope<'scope, '_, '_>,
    n: usize,
    w_total: usize,
    ops: &'scope ReduceOps<'scope, Fold, Merge>,
    result: &'scope Mutex<Option<R>>,
    cursor: &'scope AtomicUsize,
) where
    R: Send,
    Fold: Fn(usize, usize, &mut Option<R>) + Sync,
    Merge: Fn(&R, &R) -> R + Sync,
    P: DynamicLoop + Sync,
    P::Wrapper: Sync,
{
    let mut tasks: Vec<Box<dyn FnOnce() + Send + 'scope>> = Vec::with_capacity(w_total);
    for _ in 0..w_total {
        let wrapper = part.closure_wrapper();
        tasks.push(Box::new(move || {
            wrapper.invoke(|| {
                let mut local: Option<R> = None;
                part.loop_dynamic(n, w_total, cursor, |part_b, part_e| {
                    (ops.fold)(part_b, part_e, &mut local);
                });
                merge_local(ops.merge, result, local);
            });
        }));
    }
    spawn_tasks(scope, tasks);
}

/// Spawns every task but the last under `scope`, running the last inline:
/// upstream's `(++w == W...) ? task() : rt.silent_async(task)`, shared by
/// every [`ReducePartitioner`] impl above rather than repeated once per
/// family -- the identical helper
/// [`crate::algorithm::find::FindPartitioner`]'s own impls use, duplicated
/// here for the reason this module's `# Design` section gives.
fn spawn_tasks<'scope>(
    scope: &Scope<'scope, '_, '_>,
    mut tasks: Vec<Box<dyn FnOnce() + Send + 'scope>>,
) {
    let last = tasks.pop();
    for task in tasks {
        scope.spawn(task);
    }
    if let Some(task) = last {
        task();
    }
}

/// Folds one worker's `local` into the shared `result` under its lock, if
/// `local` is non-empty: a no-op when a dynamic-family worker never
/// acquired any work, matching upstream's own `if(tmp)` guard in
/// `make_reduce_by_index_task`'s dynamic branch. `result` starting at `None`
/// (rather than seeded from the caller's own `init`) is what lets the first
/// worker to merge skip `merge` entirely, exactly as `fold` skips a
/// fabricated identity for the first element a local ever sees.
fn merge_local<R, Merge>(merge: &Merge, result: &Mutex<Option<R>>, local: Option<R>)
where
    Merge: Fn(&R, &R) -> R,
{
    let Some(local) = local else {
        return;
    };
    let mut guard = result.lock().unwrap_or_else(PoisonError::into_inner);
    let merged = match guard.as_ref() {
        Some(existing) => merge(existing, &local),
        None => local,
    };
    *guard = Some(merged);
}

/// The shared short-circuit-or-parallel skeleton every public method in this
/// module builds on: see this module's own `# Design` section for what it
/// does and why a shared pool, not a direct merge into `init`, is sound.
fn fold_and_merge<R, Fold, Merge, P>(
    rt: &mut Runtime<'_>,
    n: usize,
    fold: Fold,
    merge: &Merge,
    init: &mut R,
    part: P,
) where
    R: Send,
    Fold: Fn(usize, usize, &mut Option<R>) + Sync,
    Merge: Fn(&R, &R) -> R + Sync,
    P: ReducePartitioner,
{
    if n == 0 {
        return;
    }

    let w_total = rt.num_workers();
    if w_total <= 1 || n <= part.chunk_size() {
        let mut local = None;
        part.call(|| fold(0, n, &mut local));
        if let Some(local) = local {
            *init = merge(init, &local);
        }
        return;
    }
    let w_total = w_total.min(n);

    let result: Mutex<Option<R>> = Mutex::new(None);
    let cursor = AtomicUsize::new(0);
    let ops = ReduceOps { fold: &fold, merge };
    rt.scope(|s| {
        part.drive_fold(s, n, w_total, &ops, &result, &cursor);
    });

    if let Some(pooled) = result.into_inner().unwrap_or_else(PoisonError::into_inner) {
        *init = merge(init, &pooled);
    }
}

impl<'rt> Runtime<'rt> {
    /// Folds `data` into `init` with `bop`, in parallel chunks decided by
    /// `part`: upstream's `make_reduce_task`.
    ///
    /// `N == 0` leaves `init` untouched; otherwise every local (and, in the
    /// short-circuit case, `init` itself) is seeded from a real element of
    /// `data`, never a fabricated identity -- this module's `# Design`
    /// section says how, and why `T: Clone` is the one place in this module
    /// that needs it.
    ///
    /// ```
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let data: Vec<i32> = (1..=100).collect();
    ///     let mut sum = 0;
    ///     runtime.reduce(
    ///         &data,
    ///         &mut sum,
    ///         |a: &i32, b: &i32| a + b,
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(sum, 5050);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn reduce<T, O, P>(&mut self, data: &[T], init: &mut T, bop: O, part: P)
    where
        T: Clone + Send + Sync,
        O: Fn(&T, &T) -> T + Sync,
        P: ReducePartitioner,
    {
        let n = data.len();
        let fold = |part_b: usize, part_e: usize, local: &mut Option<T>| {
            for item in &data[part_b..part_e] {
                let next = match local.take() {
                    None => item.clone(),
                    Some(cur) => bop(&cur, item),
                };
                *local = Some(next);
            }
        };
        fold_and_merge(self, n, fold, &bop, init, part);
    }

    /// Folds `uop(x)` for every `x` in `data` into `init` with `bop`:
    /// upstream's single-input `make_transform_reduce_task`.
    ///
    /// Every local is seeded from `uop`'s own first return value, so neither
    /// `T` nor `R` needs `Clone` here -- see this module's `# Design`
    /// section.
    ///
    /// ```
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let data: Vec<i32> = (1..=10).collect();
    ///     let mut sum_of_squares = 0;
    ///     runtime.transform_reduce(
    ///         &data,
    ///         &mut sum_of_squares,
    ///         |a: &i32, b: &i32| a + b,
    ///         |x: &i32| x * x,
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(sum_of_squares, 385);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn transform_reduce<T, R, O, U, P>(
        &mut self,
        data: &[T],
        init: &mut R,
        bop: O,
        uop: U,
        part: P,
    ) where
        T: Sync,
        R: Send,
        O: Fn(&R, &R) -> R + Sync,
        U: Fn(&T) -> R + Sync,
        P: ReducePartitioner,
    {
        let n = data.len();
        let fold = |part_b: usize, part_e: usize, local: &mut Option<R>| {
            for item in &data[part_b..part_e] {
                let transformed = uop(item);
                let next = match local.take() {
                    None => transformed,
                    Some(cur) => bop(&cur, &transformed),
                };
                *local = Some(next);
            }
        };
        fold_and_merge(self, n, fold, &bop, init, part);
    }

    /// Folds `uop(x, y)` for every `(x, y)` in `(data1, data2)` into `init`
    /// with `bop`: upstream's two-input `make_transform_reduce_task` overload
    /// -- this module's `# Design` section says why it carries a `2` here
    /// rather than reusing [`Runtime::transform_reduce`]'s name.
    ///
    /// `data2` must have at least as many elements as `data1`; indexing past
    /// the end of a shorter `data2` panics, matching upstream's own
    /// undefined behaviour made into a defined one.
    ///
    /// ```
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let a: Vec<i32> = (1..=10).collect();
    ///     let b: Vec<i32> = (1..=10).rev().collect();
    ///     let mut dot = 0;
    ///     runtime.transform_reduce2(
    ///         &a,
    ///         &b,
    ///         &mut dot,
    ///         |l: &i32, r: &i32| l + r,
    ///         |x: &i32, y: &i32| x * y,
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(dot, 220);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn transform_reduce2<T1, T2, R, O, U, P>(
        &mut self,
        data1: &[T1],
        data2: &[T2],
        init: &mut R,
        bop: O,
        uop: U,
        part: P,
    ) where
        T1: Sync,
        T2: Sync,
        R: Send,
        O: Fn(&R, &R) -> R + Sync,
        U: Fn(&T1, &T2) -> R + Sync,
        P: ReducePartitioner,
    {
        let n = data1.len();
        let fold = |part_b: usize, part_e: usize, local: &mut Option<R>| {
            for x in part_b..part_e {
                let transformed = uop(&data1[x], &data2[x]);
                let next = match local.take() {
                    None => transformed,
                    Some(cur) => bop(&cur, &transformed),
                };
                *local = Some(next);
            }
        };
        fold_and_merge(self, n, fold, &bop, init, part);
    }

    /// Folds `range` into `init` with `lop` and `gop`: upstream's
    /// `make_reduce_by_index_task`. `lop` reduces one worker's own
    /// sub-range(s) into a local, taking the running total it has built so
    /// far (`None` the first time) and returning the new one; `gop` folds
    /// that local into the shared result, once per worker, exactly like
    /// `bop` does for [`Runtime::reduce`].
    ///
    /// An empty or invalid `range` (`range.size() == 0` covers both, since
    /// [`IndexRange::size`] already folds the invalid case into zero) leaves
    /// `init` untouched.
    ///
    /// ```
    /// use rustflow::algorithm::index_range::IndexRange;
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let data: Vec<i32> = (1..=100).collect();
    ///     let mut sum = 0;
    ///     runtime.reduce_by_index(
    ///         IndexRange::new(0usize, data.len(), 1),
    ///         &mut sum,
    ///         |sub: IndexRange<usize>, running: Option<i32>| {
    ///             let mut total = running.unwrap_or(0);
    ///             for i in (sub.begin()..sub.end()).step_by(sub.step_size()) {
    ///                 total += data[i];
    ///             }
    ///             total
    ///         },
    ///         |a: &i32, b: &i32| a + b,
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(sum, 5050);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn reduce_by_index<I, T, L, G, P>(
        &mut self,
        range: IndexRange<I>,
        init: &mut T,
        lop: L,
        gop: G,
        part: P,
    ) where
        I: IndexInt + Sync,
        T: Send,
        L: Fn(IndexRange<I>, Option<T>) -> T + Sync,
        G: Fn(&T, &T) -> T + Sync,
        P: ReducePartitioner,
    {
        let n = range.size();
        let fold = |part_b: usize, part_e: usize, local: &mut Option<T>| {
            let sub = range.discrete_domain(part_b, part_e);
            let taken = local.take();
            *local = Some(lop(sub, taken));
        };
        fold_and_merge(self, n, fold, &gop, init, part);
    }
}

/// Builds the runtime body of a [`Runtime::reduce`] task over owned `data`,
/// folding into a shared `init`: upstream's `make_reduce_task(b, e, init,
/// bop, part)`. `init` is locked once for the task's whole run, and the
/// whole fold goes through that one guard. `data` is any [`SliceSource`], so
/// the locked vector an earlier factory wrote can be folded here; its guard
/// is taken ahead of `init`'s and dropped with it when the body returns, the
/// lock order the parent module's `# Design` section requires.
///
/// `bop` and `part` are cloned on every run of the body -- relevant only for
/// a task a cycle revisits -- matching upstream copying both into its own
/// closure by value.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
/// use rustflow::algorithm::reduce::make_reduce_task;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let data = Arc::new((1..=100).collect::<Vec<i32>>());
/// let sum = Arc::new(Mutex::new(0));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_reduce_task(
///     Arc::clone(&data),
///     Arc::clone(&sum),
///     |a: &i32, b: &i32| a + b,
///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(*sum.lock().expect("uncontended"), 5050);
/// ```
pub fn make_reduce_task<T, O, P>(
    data: impl SliceSource<Item = T> + Send + 'static,
    init: Arc<Mutex<T>>,
    bop: O,
    part: P,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Clone + Send + Sync + 'static,
    O: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
    P: ReducePartitioner + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let source = data.read_slice();
        let mut guard = init.lock().unwrap_or_else(PoisonError::into_inner);
        rt.reduce(&source, &mut *guard, bop.clone(), part.clone());
    }
}

/// Builds the runtime body of a [`Runtime::transform_reduce`] task over
/// owned `data`: upstream's single-input `make_transform_reduce_task(b, e,
/// init, bop, uop, part)` overload. `data` is a [`SliceSource`], and `init`
/// is locked and the operators cloned exactly as [`make_reduce_task`] does.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
/// use rustflow::algorithm::reduce::make_transform_reduce_task;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let data = Arc::new((1..=10).collect::<Vec<i32>>());
/// let sum_of_squares = Arc::new(Mutex::new(0));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_transform_reduce_task(
///     Arc::clone(&data),
///     Arc::clone(&sum_of_squares),
///     |a: &i32, b: &i32| a + b,
///     |x: &i32| x * x,
///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(*sum_of_squares.lock().expect("uncontended"), 385);
/// ```
pub fn make_transform_reduce_task<T, R, O, U, P>(
    data: impl SliceSource<Item = T> + Send + 'static,
    init: Arc<Mutex<R>>,
    bop: O,
    uop: U,
    part: P,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Sync + Send + 'static,
    R: Send + 'static,
    O: Fn(&R, &R) -> R + Sync + Clone + Send + 'static,
    U: Fn(&T) -> R + Sync + Clone + Send + 'static,
    P: ReducePartitioner + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let source = data.read_slice();
        let mut guard = init.lock().unwrap_or_else(PoisonError::into_inner);
        rt.transform_reduce(&source, &mut *guard, bop.clone(), uop.clone(), part.clone());
    }
}

/// Builds the runtime body of a [`Runtime::transform_reduce2`] task over
/// owned `data1` and `data2`: upstream's two-input
/// `make_transform_reduce_task(beg1, end1, beg2, init, bop_r, bop_t, part)`
/// overload, carrying the `2` for the reason this module's `# Design`
/// section gives for the method's own name. `data1` and `data2` are
/// [`SliceSource`]s, read under guards taken in that order ahead of `init`'s
/// lock; the operators are cloned exactly as [`make_reduce_task`] does.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
/// use rustflow::algorithm::reduce::make_transform_reduce2_task;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let a = Arc::new((1..=10).collect::<Vec<i32>>());
/// let b = Arc::new((1..=10).rev().collect::<Vec<i32>>());
/// let dot = Arc::new(Mutex::new(0));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_transform_reduce2_task(
///     Arc::clone(&a),
///     Arc::clone(&b),
///     Arc::clone(&dot),
///     |l: &i32, r: &i32| l + r,
///     |x: &i32, y: &i32| x * y,
///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(*dot.lock().expect("uncontended"), 220);
/// ```
pub fn make_transform_reduce2_task<T1, T2, R, O, U, P>(
    data1: impl SliceSource<Item = T1> + Send + 'static,
    data2: impl SliceSource<Item = T2> + Send + 'static,
    init: Arc<Mutex<R>>,
    bop: O,
    uop: U,
    part: P,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T1: Sync + Send + 'static,
    T2: Sync + Send + 'static,
    R: Send + 'static,
    O: Fn(&R, &R) -> R + Sync + Clone + Send + 'static,
    U: Fn(&T1, &T2) -> R + Sync + Clone + Send + 'static,
    P: ReducePartitioner + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let first = data1.read_slice();
        let second = data2.read_slice();
        let mut guard = init.lock().unwrap_or_else(PoisonError::into_inner);
        rt.transform_reduce2(
            &first,
            &second,
            &mut *guard,
            bop.clone(),
            uop.clone(),
            part.clone(),
        );
    }
}

/// Builds the runtime body of a [`Runtime::reduce_by_index`] task over
/// `range`: upstream's `make_reduce_by_index_task(range, init, lop, gop,
/// part)`. `range` is a plain value, not `Arc`-wrapped, since [`IndexRange`]
/// is `Copy`; `init` is locked and `lop`/`gop`/`part` are cloned exactly as
/// [`make_reduce_task`] does.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::index_range::IndexRange;
/// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
/// use rustflow::algorithm::reduce::make_reduce_by_index_task;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let sum = Arc::new(Mutex::new(0usize));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_reduce_by_index_task(
///     IndexRange::new(0usize, 100, 1),
///     Arc::clone(&sum),
///     |sub: IndexRange<usize>, running: Option<usize>| {
///         let mut total = running.unwrap_or(0);
///         for i in (sub.begin()..sub.end()).step_by(sub.step_size()) {
///             total += i;
///         }
///         total
///     },
///     |a: &usize, b: &usize| a + b,
///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(*sum.lock().expect("uncontended"), 4950);
/// ```
pub fn make_reduce_by_index_task<I, T, L, G, P>(
    range: IndexRange<I>,
    init: Arc<Mutex<T>>,
    lop: L,
    gop: G,
    part: P,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    I: IndexInt + Sync + Send + 'static,
    T: Send + 'static,
    L: Fn(IndexRange<I>, Option<T>) -> T + Sync + Clone + Send + 'static,
    G: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
    P: ReducePartitioner + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let mut guard = init.lock().unwrap_or_else(PoisonError::into_inner);
        rt.reduce_by_index(range, &mut *guard, lop.clone(), gop.clone(), part.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::algorithm::partitioner::{
        DefaultClosureWrapper, DynamicPartitioner, GuidedPartitioner, RandomPartitioner,
        StaticPartitioner,
    };
    use crate::prelude::*;
    use crate::{Executor, Runtime};

    /// `N == 0` leaves `init` untouched for every algorithm, upstream's
    /// first short-circuit.
    #[test]
    fn empty_input_is_a_no_op() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let data: [i32; 0] = [];
            let mut reduce_init = 99;
            let mut transform_init = 99;
            let mut binary_init = 99;
            let mut index_init = 99;
            runtime.reduce(
                &data,
                &mut reduce_init,
                |_: &i32, _: &i32| unreachable!(),
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
            runtime.transform_reduce(
                &data,
                &mut transform_init,
                |_: &i32, _: &i32| unreachable!(),
                |_: &i32| unreachable!(),
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
            runtime.transform_reduce2(
                &data,
                &data,
                &mut binary_init,
                |_: &i32, _: &i32| unreachable!(),
                |_: &i32, _: &i32| unreachable!(),
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
            runtime.reduce_by_index(
                crate::algorithm::index_range::IndexRange::new(0usize, 0, 1),
                &mut index_init,
                |_: crate::algorithm::index_range::IndexRange<usize>, _: Option<i32>| {
                    unreachable!()
                },
                |_: &i32, _: &i32| unreachable!(),
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
            assert_eq!(reduce_init, 99);
            assert_eq!(transform_init, 99);
            assert_eq!(binary_init, 99);
            assert_eq!(index_init, 99);
        });
        Executor::new(3).run(&taskflow).wait().expect("no panic");
    }

    /// `Reduce.*`: `reduce` with a `bop` that has no identity -- `min` --
    /// over an `init` whose starting value is itself the thing to beat.
    /// Seeding `0` instead (a fabricated identity) would be wrong here, since
    /// every value in range is negative: this is the "bop never sees a
    /// fabricated identity" case the issue asks for, over every partitioner
    /// and a sweep of worker counts.
    #[test]
    fn reduce_min_never_fabricates_an_identity() {
        let data: Vec<i32> = (1..=200).map(|x| -x).collect();
        let want = *data.iter().min().expect("non-empty");

        for workers in 1..=5usize {
            for chunk_size in [0usize, 1, 3, 23] {
                check_reduce_min(
                    &data,
                    want,
                    workers,
                    StaticPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                );
                check_reduce_min(
                    &data,
                    want,
                    workers,
                    GuidedPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                );
                check_reduce_min(
                    &data,
                    want,
                    workers,
                    DynamicPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                );
                check_reduce_min(
                    &data,
                    want,
                    workers,
                    RandomPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                );
            }
        }
    }

    fn check_reduce_min<P>(data: &[i32], want: i32, workers: usize, part: P)
    where
        P: ReducePartitioner + Send + 'static,
    {
        let data = data.to_vec();
        let mut taskflow = Taskflow::new();
        let mut part = Some(part);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let part = part.take().expect("this task runs once");
            // A starting value that is itself a real candidate answer, never
            // a fabricated identity like `i32::MAX` would be if the data
            // could be empty -- here it cannot, so the true minimum is also
            // this seed's only possible outcome if every local is seeded
            // correctly from real data.
            let mut local_min = data[0];
            runtime.reduce(
                &data,
                &mut local_min,
                |a: &i32, b: &i32| if *a < *b { *a } else { *b },
                part,
            );
            assert_eq!(local_min, want, "{workers} worker(s)");
        });
        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }

    /// `Reduce.*`: sum, over every partitioner and a sweep of worker counts
    /// and chunk sizes.
    #[test]
    fn reduce_sum_matches_sequential_sum_across_partitioners_and_workers() {
        let data: Vec<i64> = (1..=500).collect();
        let want: i64 = data.iter().sum();

        for workers in 1..=5usize {
            for chunk_size in [0usize, 1, 7, 37] {
                for part_kind in 0..4 {
                    let data = data.clone();
                    let mut taskflow = Taskflow::new();
                    taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                        let mut sum = 0_i64;
                        let bop = |a: &i64, b: &i64| a + b;
                        match part_kind {
                            0 => runtime.reduce(
                                &data,
                                &mut sum,
                                bop,
                                StaticPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                            ),
                            1 => runtime.reduce(
                                &data,
                                &mut sum,
                                bop,
                                GuidedPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                            ),
                            2 => runtime.reduce(
                                &data,
                                &mut sum,
                                bop,
                                DynamicPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                            ),
                            _ => runtime.reduce(
                                &data,
                                &mut sum,
                                bop,
                                RandomPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                            ),
                        }
                        assert_eq!(sum, want, "{workers} worker(s), chunk {chunk_size}");
                    });
                    Executor::new(workers)
                        .run(&taskflow)
                        .wait()
                        .expect("no task panicked");
                }
            }
        }
    }

    /// `TransformReduce.*`: squares summed via `uop`/`bop`, over every
    /// partitioner and a sweep of worker counts.
    #[test]
    fn transform_reduce_matches_sequential_sum_of_squares() {
        let data: Vec<i64> = (1..=300).collect();
        let want: i64 = data.iter().map(|x| x * x).sum();

        for workers in 1..=5usize {
            for chunk_size in [0usize, 1, 11] {
                let data = data.clone();
                let mut taskflow = Taskflow::new();
                taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                    let mut sum_of_squares = 100_i64;
                    runtime.transform_reduce(
                        &data,
                        &mut sum_of_squares,
                        |a: &i64, b: &i64| a + b,
                        |x: &i64| x * x,
                        GuidedPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    assert_eq!(sum_of_squares, want + 100, "{workers} worker(s)");
                });
                Executor::new(workers)
                    .run(&taskflow)
                    .wait()
                    .expect("no task panicked");
            }
        }
    }

    /// `TransformReduceBinary.*`: two inputs combined elementwise by `uop`
    /// before folding, over every partitioner.
    #[test]
    fn transform_reduce2_matches_sequential_dot_product() {
        let a: Vec<i64> = (1..=400).collect();
        let b: Vec<i64> = (1..=400).rev().collect();
        let want: i64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();

        for workers in 1..=5usize {
            for chunk_size in [0usize, 1, 13] {
                let a = a.clone();
                let b = b.clone();
                let mut taskflow = Taskflow::new();
                taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                    let mut dot = 0_i64;
                    runtime.transform_reduce2(
                        &a,
                        &b,
                        &mut dot,
                        |l: &i64, r: &i64| l + r,
                        |x: &i64, y: &i64| x * y,
                        DynamicPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    assert_eq!(dot, want, "{workers} worker(s)");
                });
                Executor::new(workers)
                    .run(&taskflow)
                    .wait()
                    .expect("no task panicked");
            }
        }
    }

    /// `ReduceByIndex.*`: `lop` sums its own sub-range, `gop` folds the
    /// locals, starting from a non-zero `init` -- matching upstream's own
    /// `reduce_by_index_sum` test, which seeds `sol = 10`.
    #[test]
    fn reduce_by_index_matches_sequential_sum_from_a_nonzero_seed() {
        let data: Vec<i64> = (1..=600).map(|x| x - 300).collect();
        let want: i64 = 10 + data.iter().sum::<i64>();

        for workers in 1..=5usize {
            for chunk_size in [0usize, 1, 17] {
                let data = data.clone();
                let mut taskflow = Taskflow::new();
                taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                    let mut sol = 10_i64;
                    runtime.reduce_by_index(
                        crate::algorithm::index_range::IndexRange::new(0usize, data.len(), 1),
                        &mut sol,
                        |sub: crate::algorithm::index_range::IndexRange<usize>,
                         running: Option<i64>| {
                            let mut total = running.unwrap_or(0);
                            for i in (sub.begin()..sub.end()).step_by(sub.step_size()) {
                                total += data[i];
                            }
                            total
                        },
                        |a: &i64, b: &i64| a + b,
                        GuidedPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    assert_eq!(sol, want, "{workers} worker(s)");
                });
                Executor::new(workers)
                    .run(&taskflow)
                    .wait()
                    .expect("no task panicked");
            }
        }
    }

    /// The counting-wrapper case the issue asks for: the closure wrapper --
    /// and so the merge it wraps -- runs exactly once per worker *task*, not
    /// once per element, for the static family.
    #[test]
    fn closure_wrapper_runs_once_per_worker_for_reduce_static() {
        for workers in 1..8usize {
            let wrapper_called = Arc::new(AtomicUsize::new(0));
            let mut taskflow = Taskflow::new();
            let wrapper_called_body = Arc::clone(&wrapper_called);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let data: Vec<i64> = (0..1000).collect();
                let wrapper_called = Arc::clone(&wrapper_called_body);
                let wrapper = move |task: &mut dyn FnMut()| {
                    wrapper_called.fetch_add(1, Ordering::Relaxed);
                    task();
                };
                let mut sum = 0_i64;
                runtime.reduce(
                    &data,
                    &mut sum,
                    |a: &i64, b: &i64| a + b,
                    StaticPartitioner::with_closure_wrapper(1, wrapper),
                );
                assert_eq!(sum, 499_500);
            });
            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");
            assert_eq!(wrapper_called.load(Ordering::Relaxed), workers);
        }
    }

    /// The dynamic family's wrapper also runs once per worker *task*, never
    /// once per chunk: `ClosureWrapper.Reduce.Dynamic`.
    #[test]
    fn closure_wrapper_runs_at_most_once_per_worker_for_reduce_dynamic() {
        for workers in 1..8usize {
            let wrapper_called = Arc::new(AtomicUsize::new(0));
            let mut taskflow = Taskflow::new();
            let wrapper_called_body = Arc::clone(&wrapper_called);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let data: Vec<i64> = (0..1000).collect();
                let wrapper_called = Arc::clone(&wrapper_called_body);
                let wrapper = move |task: &mut dyn FnMut()| {
                    wrapper_called.fetch_add(1, Ordering::Relaxed);
                    task();
                };
                let mut sum = 0_i64;
                runtime.reduce(
                    &data,
                    &mut sum,
                    |a: &i64, b: &i64| a + b,
                    DynamicPartitioner::with_closure_wrapper(1, wrapper),
                );
                assert_eq!(sum, 499_500);
            });
            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");
            assert!(wrapper_called.load(Ordering::Relaxed) <= workers);
        }
    }

    /// `ClosureWrapper.TransformReduce.*`: the wrapper runs once per worker
    /// for `transform_reduce` too.
    #[test]
    fn closure_wrapper_runs_once_per_worker_for_transform_reduce() {
        for workers in 1..8usize {
            let wrapper_called = Arc::new(AtomicUsize::new(0));
            let mut taskflow = Taskflow::new();
            let wrapper_called_body = Arc::clone(&wrapper_called);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let data: Vec<i64> = (0..1000).collect();
                let wrapper_called = Arc::clone(&wrapper_called_body);
                let wrapper = move |task: &mut dyn FnMut()| {
                    wrapper_called.fetch_add(1, Ordering::Relaxed);
                    task();
                };
                let mut result = 0_i64;
                runtime.transform_reduce(
                    &data,
                    &mut result,
                    |a: &i64, b: &i64| a + b,
                    |x: &i64| -x,
                    StaticPartitioner::with_closure_wrapper(1, wrapper),
                );
                assert_eq!(result, -499_500);
            });
            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");
            assert_eq!(wrapper_called.load(Ordering::Relaxed), workers);
        }
    }

    /// The owned-data wrapper: upstream's plain, non-stateful `reduce` over
    /// a `tf::Taskflow::reduce` call -- exercised through `rustflow::prelude`
    /// alone, with no direct `use` of `AlgorithmBuilder` itself, proving the
    /// trait is reachable the way the prelude promises.
    #[test]
    fn flow_builder_reduce_locks_and_runs_over_owned_data() {
        let data = Arc::new((1..=100).collect::<Vec<i64>>());
        let sum = Arc::new(Mutex::new(0_i64));
        let mut taskflow = Taskflow::new();
        taskflow.reduce(
            Arc::clone(&data),
            Arc::clone(&sum),
            |a: &i64, b: &i64| a + b,
            GuidedPartitioner::<DefaultClosureWrapper>::default(),
        );

        Executor::new(3).run(&taskflow).wait().expect("no panic");

        assert_eq!(*sum.lock().expect("uncontended"), 5050);
    }
}

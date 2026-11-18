//! Translated from `taskflow/algorithm/find.hpp` (Taskflow v4.1.0).
//!
//! [`Runtime::find_if`] and [`Runtime::find_if_not`] are upstream's
//! `make_find_if_task`/`make_find_if_not_task`, run directly rather than
//! building a second closure, as [`crate::algorithm::for_each`] already does
//! for the same reason. [`Runtime::min_element`] and [`Runtime::max_element`]
//! are their `make_min_element_task`/`make_max_element_task`.
//! [`FindBuilder`] is the owned-data wrapper over all four, in
//! [`crate::prelude`] alongside [`crate::algorithm::for_each::ForEachBuilder`].
//!
//! # Design
//!
//! **A second driving trait, not [`crate::algorithm::SlicePartitioner`].**
//! That trait's `drive` runs a chunk body to completion over every chunk --
//! its own signature says `Body: Fn(&mut [T])`, returning nothing, which
//! cannot express "stop this worker's own scan as soon as it finds a
//! match". [`FindPartitioner::drive_find`] is the `bool`-returning shape
//! instead, built on [`crate::algorithm::partitioner::StaticLoop::loop_static_until`]
//! and [`crate::algorithm::partitioner::DynamicLoop::loop_dynamic_until`] --
//! the two methods the partitioners already added for exactly this -- the
//! same sealed, four-times-implemented shape `SlicePartitioner` uses, duplicated here
//! rather than shared, since the two traits answer different questions over
//! different element types (`&mut [T]` there, `&[T]` here) and a single
//! trait trying to cover both would need the same `if constexpr` Rust has
//! no equivalent for that `partitioner`'s own `# Design` section already
//! describes. [`FindPartitioner::drive_reduce`] is the plain, non-breaking
//! shape [`Runtime::min_element`]/[`Runtime::max_element`] build on, folding
//! a local index under each worker's own [`crate::algorithm::partitioner::StaticLoop::loop_static`]
//! or [`crate::algorithm::partitioner::DynamicLoop::loop_dynamic`] rather than
//! breaking early -- upstream's own `loop` call there is the `void`-returning
//! branch too.
//!
//! **No `split_at_mut`.** Every one of the four algorithms here only reads
//! `data`, so every worker closure captures the same `&'scope [T]` directly:
//! unlike [`crate::algorithm::SlicePartitioner::drive`]'s `&mut [T]`, nothing
//! needs carving into disjoint pieces up front to stay sound, since a shared
//! reference may be copied into as many closures as a partitioner spawns.
//!
//! **The earliest match wins via [`std::sync::atomic::AtomicUsize::fetch_min`],
//! not a mutex.** Upstream's `make_find_if_task` locks a `std::mutex` around
//! a plain `std::distance`/compare/assign every time a chunk matches; a
//! shared `AtomicUsize`, seeded to `data.len()` as the sentinel for "nothing
//! yet" (matching upstream's own `result = std::next(origin, N)`), gets the
//! identical "only the smallest index ever written survives" guarantee from
//! one lock-free instruction. A later chunk finishing first can only ever
//! move the atomic *down*, never up, which is the property
//! `find_if_reports_the_earliest_match_not_merely_any_match` below pins.
//! Stopping *this* worker's own further chunks once it has found one match
//! is still exactly upstream's shape: nothing here stops a *sibling* worker
//! already racing a different part of `data`, which is also upstream's
//! shape -- the earliest-wins atomic is what makes that safe to leave
//! unsynchronized.
//!
//! **[`Runtime::min_element`]'s fold needs no "chunk size at least two"
//! floor.** Upstream pre-reduces a chunk's first two elements by hand so its
//! `T` (an iterator) is never copied from a single element alone; the local
//! fold here is a plain `usize` index, trivially copied, so every chunk
//! (even a chunk of one) folds through the same `Option<usize>` --
//! `None` until the first index a worker ever sees sets it, lazily, since
//! the dynamic family does not know its own first index up front the way
//! the static family's `curr_b` does.
//!
//! **[`Runtime::max_element`] is [`Runtime::min_element`] with `cmp`
//! flipped**, not a second copy of the fold: `max_element(cmp)` and
//! `min_element(|a, b| cmp(b, a))` ask the identical question, since
//! "the smallest element under `|a, b| cmp(b, a)`" is exactly "the largest
//! element under `cmp`".

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crate::Scope;
use crate::algorithm::partitioner::{
    ClosureWrapper, DynamicLoop, DynamicPartitioner, GuidedPartitioner, Partitioner,
    RandomPartitioner, StaticLoop, StaticPartitioner,
};
use crate::flow_builder::FlowBuilder;
use crate::graph::Task;
use crate::runtime::Runtime;

/// Closes [`FindPartitioner`] against implementations outside this crate,
/// the same restriction [`crate::algorithm::SlicePartitioner`] carries, for
/// the same reason: only the four concrete partitioners below can answer
/// either loop shape.
mod sealed {
    /// Marker a type must carry to implement [`super::FindPartitioner`].
    pub trait Sealed {}
}

/// Drives the breaking search [`Runtime::find_if`]/[`Runtime::find_if_not`]
/// build on, and the plain fold [`Runtime::min_element`]/[`Runtime::max_element`]
/// build on -- each over a shared, read-only `data: &[T]` rather than
/// [`crate::algorithm::SlicePartitioner::drive`]'s `&mut [T]`. This module's
/// `# Design` section says why find needs a trait of its own rather than
/// reusing that one.
///
/// Sealed against implementations outside this crate, like
/// [`crate::algorithm::SlicePartitioner`].
pub trait FindPartitioner: Partitioner + sealed::Sealed {
    /// Splits `data` among `w_total` workers and runs
    /// `predicate` over every chunk, stopping each worker's own scan as soon
    /// as it sees a match: upstream's `make_find_if_task`'s breaking branch.
    /// The earliest matching index any worker sees is folded into `best` via
    /// [`AtomicUsize::fetch_min`]; `cursor` is the shared draw point the
    /// dynamic family races over, unused by the static family.
    fn drive_find<'scope, T, Pred>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope [T],
        w_total: usize,
        predicate: &'scope Pred,
        best: &'scope AtomicUsize,
        cursor: &'scope AtomicUsize,
    ) where
        T: Sync,
        Pred: Fn(&T) -> bool + Sync;

    /// Splits `data` among `w_total` workers and folds each worker's own
    /// share into a local index under `cmp`, merging every worker's local
    /// into the shared `best` under its lock: upstream's
    /// `make_min_element_task` (and `make_max_element_task`, `cmp` flipped).
    /// `cursor` is the dynamic family's shared draw point, as in
    /// [`FindPartitioner::drive_find`].
    fn drive_reduce<'scope, T, Cmp>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope [T],
        w_total: usize,
        cmp: &'scope Cmp,
        best: &'scope Mutex<usize>,
        cursor: &'scope AtomicUsize,
    ) where
        T: Sync,
        Cmp: Fn(&T, &T) -> bool + Sync;
}

impl<C: ClosureWrapper> sealed::Sealed for StaticPartitioner<C> {}
impl<C: ClosureWrapper> sealed::Sealed for GuidedPartitioner<C> {}
impl<C: ClosureWrapper> sealed::Sealed for DynamicPartitioner<C> {}
impl<C: ClosureWrapper> sealed::Sealed for RandomPartitioner<C> {}

impl<C: ClosureWrapper + Sync> FindPartitioner for StaticPartitioner<C> {
    fn drive_find<'scope, T, Pred>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope [T],
        w_total: usize,
        predicate: &'scope Pred,
        best: &'scope AtomicUsize,
        _cursor: &'scope AtomicUsize,
    ) where
        T: Sync,
        Pred: Fn(&T) -> bool + Sync,
    {
        let n = data.len();
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
                    self.loop_static_until(n, w_total, b, chunk_size, |part_b, part_e| {
                        scan_chunk(data, part_b, part_e, predicate, best)
                    });
                });
            }));
            curr_b += chunk_size;
        }
        spawn_tasks(scope, tasks);
    }

    fn drive_reduce<'scope, T, Cmp>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope [T],
        w_total: usize,
        cmp: &'scope Cmp,
        best: &'scope Mutex<usize>,
        _cursor: &'scope AtomicUsize,
    ) where
        T: Sync,
        Cmp: Fn(&T, &T) -> bool + Sync,
    {
        let n = data.len();
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
                    let mut local: Option<usize> = None;
                    self.loop_static(n, w_total, b, chunk_size, |part_b, part_e| {
                        fold_chunk(data, part_b, part_e, cmp, &mut local);
                    });
                    if let Some(candidate) = local {
                        merge_best(data, cmp, best, candidate);
                    }
                });
            }));
            curr_b += chunk_size;
        }
        spawn_tasks(scope, tasks);
    }
}

impl<C: ClosureWrapper + Sync> FindPartitioner for GuidedPartitioner<C> {
    fn drive_find<'scope, T, Pred>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope [T],
        w_total: usize,
        predicate: &'scope Pred,
        best: &'scope AtomicUsize,
        cursor: &'scope AtomicUsize,
    ) where
        T: Sync,
        Pred: Fn(&T) -> bool + Sync,
    {
        drive_find_dynamic(self, scope, data, w_total, predicate, best, cursor);
    }

    fn drive_reduce<'scope, T, Cmp>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope [T],
        w_total: usize,
        cmp: &'scope Cmp,
        best: &'scope Mutex<usize>,
        cursor: &'scope AtomicUsize,
    ) where
        T: Sync,
        Cmp: Fn(&T, &T) -> bool + Sync,
    {
        drive_reduce_dynamic(self, scope, data, w_total, cmp, best, cursor);
    }
}

impl<C: ClosureWrapper + Sync> FindPartitioner for DynamicPartitioner<C> {
    fn drive_find<'scope, T, Pred>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope [T],
        w_total: usize,
        predicate: &'scope Pred,
        best: &'scope AtomicUsize,
        cursor: &'scope AtomicUsize,
    ) where
        T: Sync,
        Pred: Fn(&T) -> bool + Sync,
    {
        drive_find_dynamic(self, scope, data, w_total, predicate, best, cursor);
    }

    fn drive_reduce<'scope, T, Cmp>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope [T],
        w_total: usize,
        cmp: &'scope Cmp,
        best: &'scope Mutex<usize>,
        cursor: &'scope AtomicUsize,
    ) where
        T: Sync,
        Cmp: Fn(&T, &T) -> bool + Sync,
    {
        drive_reduce_dynamic(self, scope, data, w_total, cmp, best, cursor);
    }
}

impl<C: ClosureWrapper + Sync> FindPartitioner for RandomPartitioner<C> {
    fn drive_find<'scope, T, Pred>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope [T],
        w_total: usize,
        predicate: &'scope Pred,
        best: &'scope AtomicUsize,
        cursor: &'scope AtomicUsize,
    ) where
        T: Sync,
        Pred: Fn(&T) -> bool + Sync,
    {
        drive_find_dynamic(self, scope, data, w_total, predicate, best, cursor);
    }

    fn drive_reduce<'scope, T, Cmp>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope [T],
        w_total: usize,
        cmp: &'scope Cmp,
        best: &'scope Mutex<usize>,
        cursor: &'scope AtomicUsize,
    ) where
        T: Sync,
        Cmp: Fn(&T, &T) -> bool + Sync,
    {
        drive_reduce_dynamic(self, scope, data, w_total, cmp, best, cursor);
    }
}

/// The dynamic family's shared [`FindPartitioner::drive_find`]: `w_total`
/// tasks, every one racing `cursor` via
/// [`DynamicLoop::loop_dynamic_until`], the last running inline.
fn drive_find_dynamic<'scope, T, Pred, P>(
    part: &'scope P,
    scope: &Scope<'scope, '_, '_>,
    data: &'scope [T],
    w_total: usize,
    predicate: &'scope Pred,
    best: &'scope AtomicUsize,
    cursor: &'scope AtomicUsize,
) where
    T: Sync,
    Pred: Fn(&T) -> bool + Sync,
    P: DynamicLoop + Sync,
    P::Wrapper: Sync,
{
    let n = data.len();
    let mut tasks: Vec<Box<dyn FnOnce() + Send + 'scope>> = Vec::with_capacity(w_total);
    for _ in 0..w_total {
        let wrapper = part.closure_wrapper();
        tasks.push(Box::new(move || {
            wrapper.invoke(|| {
                part.loop_dynamic_until(n, w_total, cursor, |part_b, part_e| {
                    scan_chunk(data, part_b, part_e, predicate, best)
                });
            });
        }));
    }
    spawn_tasks(scope, tasks);
}

/// The dynamic family's shared [`FindPartitioner::drive_reduce`]: `w_total`
/// tasks, every one drawing from `cursor` via [`DynamicLoop::loop_dynamic`],
/// the last running inline.
fn drive_reduce_dynamic<'scope, T, Cmp, P>(
    part: &'scope P,
    scope: &Scope<'scope, '_, '_>,
    data: &'scope [T],
    w_total: usize,
    cmp: &'scope Cmp,
    best: &'scope Mutex<usize>,
    cursor: &'scope AtomicUsize,
) where
    T: Sync,
    Cmp: Fn(&T, &T) -> bool + Sync,
    P: DynamicLoop + Sync,
    P::Wrapper: Sync,
{
    let n = data.len();
    let mut tasks: Vec<Box<dyn FnOnce() + Send + 'scope>> = Vec::with_capacity(w_total);
    for _ in 0..w_total {
        let wrapper = part.closure_wrapper();
        tasks.push(Box::new(move || {
            wrapper.invoke(|| {
                let mut local: Option<usize> = None;
                part.loop_dynamic(n, w_total, cursor, |part_b, part_e| {
                    fold_chunk(data, part_b, part_e, cmp, &mut local);
                });
                if let Some(candidate) = local {
                    merge_best(data, cmp, best, candidate);
                }
            });
        }));
    }
    spawn_tasks(scope, tasks);
}

/// Spawns every task but the last under `scope`, running the last inline:
/// upstream's `(++w == W...) ? task() : rt.silent_async(task)`, shared by
/// every [`FindPartitioner`] impl above rather than repeated once per family.
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

/// Scans `data[part_b..part_e)` for the first index satisfying `predicate`,
/// folding a match into `best` with [`AtomicUsize::fetch_min`] -- this
/// module's `# Design` section says why that alone gives the earliest-wins
/// guarantee -- and stopping (returning `true`) as soon as one is seen:
/// upstream's own early return, once its lock-guarded compare is folded into
/// a single atomic instead.
fn scan_chunk<T, Pred>(
    data: &[T],
    part_b: usize,
    part_e: usize,
    predicate: &Pred,
    best: &AtomicUsize,
) -> bool
where
    Pred: Fn(&T) -> bool,
{
    for (x, item) in data.iter().enumerate().take(part_e).skip(part_b) {
        if predicate(item) {
            best.fetch_min(x, Ordering::Relaxed);
            return true;
        }
    }
    false
}

/// Folds `data[part_b..part_e)` into `local` under `cmp`: upstream's
/// per-chunk "loop reduce". `local` starts at the first index this chunk
/// loop ever sees -- lazily, since the dynamic family does not know its own
/// first index up front the way the static family's `curr_b` does -- and
/// only ever moves to an index `cmp` prefers over the one it already holds.
fn fold_chunk<T, Cmp>(
    data: &[T],
    part_b: usize,
    part_e: usize,
    cmp: &Cmp,
    local: &mut Option<usize>,
) where
    Cmp: Fn(&T, &T) -> bool,
{
    let mut candidate = local.unwrap_or(part_b);
    for x in part_b..part_e {
        if cmp(&data[x], &data[candidate]) {
            candidate = x;
        }
    }
    *local = Some(candidate);
}

/// Folds one worker's local best into the shared `best`, under its lock:
/// upstream's "final reduce".
fn merge_best<T, Cmp>(data: &[T], cmp: &Cmp, best: &Mutex<usize>, candidate: usize)
where
    Cmp: Fn(&T, &T) -> bool,
{
    let mut guard = best.lock().unwrap_or_else(PoisonError::into_inner);
    if cmp(&data[candidate], &data[*guard]) {
        *guard = candidate;
    }
}

/// The shared body of [`Runtime::find_if`] and [`Runtime::find_if_not`]:
/// `predicate` already encodes which of the two is being asked.
fn find_impl<T, F, P>(rt: &mut Runtime<'_>, data: &[T], predicate: F, part: P) -> Option<usize>
where
    T: Sync,
    F: Fn(&T) -> bool + Sync,
    P: FindPartitioner,
{
    let n = data.len();
    if n == 0 {
        return None;
    }

    let w_total = rt.num_workers();
    if w_total <= 1 || n <= part.chunk_size() {
        let mut found = None;
        part.call(|| {
            found = data.iter().position(&predicate);
        });
        return found;
    }
    let w_total = w_total.min(n);

    // The sentinel for "nothing found yet": no real index can ever reach
    // `n`, matching upstream's `result = std::next(origin, N)`.
    let best = AtomicUsize::new(n);
    let cursor = AtomicUsize::new(0);
    rt.scope(|s| {
        part.drive_find(s, data, w_total, &predicate, &best, &cursor);
    });

    let found = best.load(Ordering::Relaxed);
    (found < n).then_some(found)
}

/// The shared body of [`Runtime::min_element`] and [`Runtime::max_element`]:
/// `cmp` already encodes which of the two is being asked, this module's
/// `# Design` section says how.
fn reduce_impl<T, C, P>(rt: &mut Runtime<'_>, data: &[T], cmp: C, part: P) -> Option<usize>
where
    T: Sync,
    C: Fn(&T, &T) -> bool + Sync,
    P: FindPartitioner,
{
    let n = data.len();
    if n == 0 {
        return None;
    }

    let w_total = rt.num_workers();
    if w_total <= 1 || n <= part.chunk_size() {
        let mut best = 0usize;
        part.call(|| {
            for x in 1..n {
                if cmp(&data[x], &data[best]) {
                    best = x;
                }
            }
        });
        return Some(best);
    }
    let w_total = w_total.min(n);

    let best = Mutex::new(0usize);
    let cursor = AtomicUsize::new(0);
    rt.scope(|s| {
        part.drive_reduce(s, data, w_total, &cmp, &best, &cursor);
    });

    Some(*best.lock().unwrap_or_else(PoisonError::into_inner))
}

impl<'rt> Runtime<'rt> {
    /// Finds the earliest index in `data` for which `predicate` holds,
    /// writing it into `result`, or `None` if nothing matches: upstream's
    /// `make_find_if_task`, with the output parameter [`Option<usize>`]
    /// standing in for upstream's "iterator, or `end`".
    ///
    /// `N == 0` leaves `result` as `None`, matching upstream's `result =
    /// end`; `W <= 1` or `data.len() <= part.chunk_size()` runs `predicate`
    /// right here with [`Iterator::position`], under `part`'s closure
    /// wrapper, with no [`Runtime::scope`] at all. Otherwise `data` is split
    /// into chunks and searched in parallel, each worker's own scan
    /// stopping as soon as it sees a match -- this module's `# Design`
    /// section says how the earliest one still wins.
    ///
    /// ```
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let data: Vec<i32> = (0..100).collect();
    ///     let mut result = None;
    ///     runtime.find_if(
    ///         &data,
    ///         &mut result,
    ///         |&x| x == 42,
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(result, Some(42));
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn find_if<T, F, P>(
        &mut self,
        data: &[T],
        result: &mut Option<usize>,
        predicate: F,
        part: P,
    ) where
        T: Sync,
        F: Fn(&T) -> bool + Sync,
        P: FindPartitioner,
    {
        *result = find_impl(self, data, predicate, part);
    }

    /// Finds the earliest index in `data` for which `predicate` does *not*
    /// hold, writing it into `result`, or `None` if `predicate` holds
    /// everywhere: upstream's `make_find_if_not_task`.
    ///
    /// ```
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let data = vec![5; 50];
    ///     let mut result = None;
    ///     runtime.find_if_not(
    ///         &data,
    ///         &mut result,
    ///         |&x: &i32| x == 5,
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(result, None);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn find_if_not<T, F, P>(
        &mut self,
        data: &[T],
        result: &mut Option<usize>,
        predicate: F,
        part: P,
    ) where
        T: Sync,
        F: Fn(&T) -> bool + Sync,
        P: FindPartitioner,
    {
        *result = find_impl(self, data, move |item: &T| !predicate(item), part);
    }

    /// Finds the index of the smallest element of `data` under `cmp`,
    /// writing it into `result`, or `None` if `data` is empty: upstream's
    /// `make_min_element_task`.
    ///
    /// `W <= 1` or `data.len() <= part.chunk_size()` folds `data` right here
    /// under `part`'s closure wrapper; otherwise every worker folds its own
    /// share into a local index first, and the local indices are merged
    /// under a lock -- this module's `# Design` section says why the fold
    /// needs no pre-reduce of the first two elements, unlike upstream's.
    ///
    /// ```
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let data = vec![4, 1, 7, 9];
    ///     let mut result = None;
    ///     runtime.min_element(
    ///         &data,
    ///         &mut result,
    ///         |a: &i32, b: &i32| a < b,
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(result, Some(1));
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn min_element<T, C, P>(&mut self, data: &[T], result: &mut Option<usize>, cmp: C, part: P)
    where
        T: Sync,
        C: Fn(&T, &T) -> bool + Sync,
        P: FindPartitioner,
    {
        *result = reduce_impl(self, data, cmp, part);
    }

    /// Finds the index of the largest element of `data` under `cmp`,
    /// writing it into `result`, or `None` if `data` is empty: upstream's
    /// `make_max_element_task`, [`Runtime::min_element`] with `cmp` flipped
    /// -- this module's `# Design` section says why that is the identical
    /// question.
    ///
    /// ```
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let data = vec![4, 1, 7, 9];
    ///     let mut result = None;
    ///     runtime.max_element(
    ///         &data,
    ///         &mut result,
    ///         |a: &i32, b: &i32| a < b,
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(result, Some(3));
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn max_element<T, C, P>(&mut self, data: &[T], result: &mut Option<usize>, cmp: C, part: P)
    where
        T: Sync,
        C: Fn(&T, &T) -> bool + Sync,
        P: FindPartitioner,
    {
        *result = reduce_impl(self, data, move |a: &T, b: &T| cmp(b, a), part);
    }
}

/// The owned-data half of [`Runtime::find_if`], [`Runtime::find_if_not`],
/// [`Runtime::min_element`] and [`Runtime::max_element`]: upstream's
/// `FlowBuilder::find_if` and friends. See
/// [`crate::algorithm::for_each::ForEachBuilder`]'s own `# Design` section
/// for why this is a separate, blanket-implemented trait rather than a
/// second `trait FlowBuilder` block, and [`crate::prelude`] for why it sits
/// in the prelude alongside [`FlowBuilder`] itself.
pub trait FindBuilder: FlowBuilder {
    /// Creates a runtime task that runs [`Runtime::find_if`] over `data` and
    /// writes its answer into `result`: upstream's `find_if(beg, end,
    /// result, predicate, part)`, over a shared owned `Vec` rather than an
    /// iterator pair a caller must otherwise keep alive.
    ///
    /// `predicate` and `part` are cloned on every run of the task --
    /// relevant only for a task revisited by a cycle -- matching upstream
    /// copying both into the closure by value.
    ///
    /// `rustflow::prelude::*` is the only import this example needs beyond
    /// the partitioner and the data it builds the task over: `FindBuilder`
    /// itself is one of the prelude's names, not imported here by name.
    ///
    /// ```
    /// use std::sync::{Arc, Mutex};
    ///
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let data = Arc::new((0..100).collect::<Vec<i32>>());
    /// let result = Arc::new(Mutex::new(None));
    /// taskflow.find_if(
    ///     Arc::clone(&data),
    ///     Arc::clone(&result),
    ///     |&x| x == 42,
    ///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
    /// );
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(*result.lock().expect("uncontended"), Some(42));
    /// ```
    fn find_if<T, F, P>(
        &mut self,
        data: Arc<Vec<T>>,
        result: Arc<Mutex<Option<usize>>>,
        predicate: F,
        part: P,
    ) -> Task
    where
        T: Sync + Send + 'static,
        F: Fn(&T) -> bool + Sync + Clone + Send + 'static,
        P: FindPartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut found = None;
            runtime.find_if(&data, &mut found, predicate.clone(), part.clone());
            *result.lock().unwrap_or_else(PoisonError::into_inner) = found;
        })
    }

    /// The `find_if_not` half of the pair: see [`FindBuilder::find_if`].
    fn find_if_not<T, F, P>(
        &mut self,
        data: Arc<Vec<T>>,
        result: Arc<Mutex<Option<usize>>>,
        predicate: F,
        part: P,
    ) -> Task
    where
        T: Sync + Send + 'static,
        F: Fn(&T) -> bool + Sync + Clone + Send + 'static,
        P: FindPartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut found = None;
            runtime.find_if_not(&data, &mut found, predicate.clone(), part.clone());
            *result.lock().unwrap_or_else(PoisonError::into_inner) = found;
        })
    }

    /// The owned-data half of [`Runtime::min_element`]: see
    /// [`FindBuilder::find_if`] for why this is a separate trait.
    fn min_element<T, C, P>(
        &mut self,
        data: Arc<Vec<T>>,
        result: Arc<Mutex<Option<usize>>>,
        cmp: C,
        part: P,
    ) -> Task
    where
        T: Sync + Send + 'static,
        C: Fn(&T, &T) -> bool + Sync + Clone + Send + 'static,
        P: FindPartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut found = None;
            runtime.min_element(&data, &mut found, cmp.clone(), part.clone());
            *result.lock().unwrap_or_else(PoisonError::into_inner) = found;
        })
    }

    /// The `max_element` half of the pair: see [`FindBuilder::min_element`].
    fn max_element<T, C, P>(
        &mut self,
        data: Arc<Vec<T>>,
        result: Arc<Mutex<Option<usize>>>,
        cmp: C,
        part: P,
    ) -> Task
    where
        T: Sync + Send + 'static,
        C: Fn(&T, &T) -> bool + Sync + Clone + Send + 'static,
        P: FindPartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut found = None;
            runtime.max_element(&data, &mut found, cmp.clone(), part.clone());
            *result.lock().unwrap_or_else(PoisonError::into_inner) = found;
        })
    }
}

impl<B: FlowBuilder + ?Sized> FindBuilder for B {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::algorithm::partitioner::{
        DefaultClosureWrapper, DynamicPartitioner, GuidedPartitioner, StaticPartitioner,
    };
    use crate::prelude::*;
    use crate::{Executor, Runtime};

    /// `N == 0` leaves every result `None`, upstream's first short-circuit,
    /// across all four algorithms.
    #[test]
    fn empty_slice_leaves_every_result_none() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let data: [i32; 0] = [];
            let mut find_result = Some(99);
            let mut not_result = Some(99);
            let mut min_result = Some(99);
            let mut max_result = Some(99);
            runtime.find_if(
                &data,
                &mut find_result,
                |_: &i32| unreachable!(),
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
            runtime.find_if_not(
                &data,
                &mut not_result,
                |_: &i32| unreachable!(),
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
            runtime.min_element(
                &data,
                &mut min_result,
                |_: &i32, _: &i32| unreachable!(),
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
            runtime.max_element(
                &data,
                &mut max_result,
                |_: &i32, _: &i32| unreachable!(),
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
            assert_eq!(find_result, None);
            assert_eq!(not_result, None);
            assert_eq!(min_result, None);
            assert_eq!(max_result, None);
        });
        Executor::new(3).run(&taskflow).wait().expect("no panic");
    }

    /// A single-element slice: found when it matches, `None` when it does
    /// not, and trivially both the min and the max.
    #[test]
    fn single_element_slice() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let data = [7i32];
            let mut hit = None;
            let mut miss = None;
            let mut min_result = None;
            let mut max_result = None;
            runtime.find_if(
                &data,
                &mut hit,
                |&x| x == 7,
                DynamicPartitioner::<DefaultClosureWrapper>::default(),
            );
            runtime.find_if(
                &data,
                &mut miss,
                |&x| x == 8,
                DynamicPartitioner::<DefaultClosureWrapper>::default(),
            );
            runtime.min_element(
                &data,
                &mut min_result,
                |a: &i32, b: &i32| a < b,
                DynamicPartitioner::<DefaultClosureWrapper>::default(),
            );
            runtime.max_element(
                &data,
                &mut max_result,
                |a: &i32, b: &i32| a < b,
                DynamicPartitioner::<DefaultClosureWrapper>::default(),
            );
            assert_eq!(hit, Some(0));
            assert_eq!(miss, None);
            assert_eq!(min_result, Some(0));
            assert_eq!(max_result, Some(0));
        });
        Executor::new(2).run(&taskflow).wait().expect("no panic");
    }

    /// The correctness point this issue is about: with more than one match
    /// in the input, `find_if` reports the earliest one, not merely one a
    /// fast chunk happened to see first. Sweeping worker counts inside one
    /// test is a stress case over how the chunks land, not a repeat-count
    /// argument that the race is handled.
    #[test]
    fn find_if_reports_the_earliest_match_not_merely_any_match() {
        let n = 2000;
        let first_match = 17;
        let second_match = 1500;
        let mut data = vec![0i32; n];
        data[first_match] = 1;
        data[second_match] = 1;

        for workers in 1..=8 {
            for chunk_size in [0usize, 1, 3, 7] {
                let mut taskflow = Taskflow::new();
                let data = data.clone();
                taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                    let mut result = None;
                    runtime.find_if(
                        &data,
                        &mut result,
                        |&x| x == 1,
                        DynamicPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    assert_eq!(
                        result,
                        Some(first_match),
                        "workers={workers} chunk_size={chunk_size}"
                    );
                });
                Executor::new(workers)
                    .run(&taskflow)
                    .wait()
                    .expect("no task panicked");
            }
        }
    }

    /// The closure wrapper is applied exactly once per worker task for the
    /// static family, over that worker's whole share: `ClosureWrapper.find.Static`.
    #[test]
    fn closure_wrapper_runs_once_per_worker_for_find_static() {
        for workers in 1..8usize {
            let wrapper_called = Arc::new(AtomicUsize::new(0));
            let mut taskflow = Taskflow::new();
            let wrapper_called_body = Arc::clone(&wrapper_called);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let data: Vec<i32> = (0..1000).collect();
                let wrapper_called = Arc::clone(&wrapper_called_body);
                let wrapper = move |task: &mut dyn FnMut()| {
                    wrapper_called.fetch_add(1, Ordering::Relaxed);
                    task();
                };
                let mut result = None;
                runtime.find_if(
                    &data,
                    &mut result,
                    |&x| x == 500,
                    StaticPartitioner::with_closure_wrapper(1, wrapper),
                );
                assert_eq!(result, Some(500));
            });
            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");
            assert_eq!(wrapper_called.load(Ordering::Relaxed), workers);
        }
    }

    /// The dynamic family's wrapper also runs once per worker *task*, but
    /// never more than once per worker: `ClosureWrapper.find.Dynamic`.
    #[test]
    fn closure_wrapper_runs_at_most_once_per_worker_for_find_dynamic() {
        for workers in 1..8usize {
            let wrapper_called = Arc::new(AtomicUsize::new(0));
            let mut taskflow = Taskflow::new();
            let wrapper_called_body = Arc::clone(&wrapper_called);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let data: Vec<i32> = (0..1000).collect();
                let wrapper_called = Arc::clone(&wrapper_called_body);
                let wrapper = move |task: &mut dyn FnMut()| {
                    wrapper_called.fetch_add(1, Ordering::Relaxed);
                    task();
                };
                let mut result = None;
                runtime.find_if(
                    &data,
                    &mut result,
                    |&x| x == 500,
                    DynamicPartitioner::with_closure_wrapper(1, wrapper),
                );
                assert_eq!(result, Some(500));
            });
            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");
            assert!(wrapper_called.load(Ordering::Relaxed) <= workers);
        }
    }

    /// The owned-data wrapper: upstream's plain, non-stateful `find_if` over
    /// a `tf::Taskflow::find_if` call.
    #[test]
    fn flow_builder_find_if_locks_and_runs_over_owned_data() {
        let data = Arc::new((0..64).collect::<Vec<i32>>());
        let result = Arc::new(Mutex::new(None));
        let mut taskflow = Taskflow::new();
        taskflow.find_if(
            Arc::clone(&data),
            Arc::clone(&result),
            |&x| x == 40,
            GuidedPartitioner::<DefaultClosureWrapper>::default(),
        );

        Executor::new(3).run(&taskflow).wait().expect("no panic");

        assert_eq!(*result.lock().expect("uncontended"), Some(40));
    }
}

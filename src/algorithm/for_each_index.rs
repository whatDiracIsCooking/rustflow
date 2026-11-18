//! Translated from `taskflow/algorithm/for_each.hpp` (Taskflow v4.1.0): the
//! two index-driven halves, `make_for_each_index_task` and
//! `make_for_each_by_index_task`. The slice-based `make_for_each_task` is
//! `crate::algorithm::for_each`, a sibling module ported separately -- the
//! two share nothing but the partitioner layer underneath them.
//!
//! `for_each_index` (free function [`make_for_each_index_task`]) walks an
//! integer range `[beg, end)` in steps of `step`, calling `f` once per
//! index. `for_each_by_index` (free function
//! [`make_for_each_by_index_task`]) walks an [`IndexRange<T>`] one chunk at
//! a time, calling `f` with each chunk's own sub-range rather than one
//! index at a time -- the shape a caller wants when the body itself knows
//! how to walk a contiguous run faster than one call per element would.
//! Each is available three ways: the `make_*_task` free function above,
//! queued into a graph through [`ForEachIndexBuilder`]'s `FlowBuilder`
//! wrapper; and a [`Runtime`] method built on [`Runtime::scope`], for a
//! runtime task's body that wants `f` to borrow from its own frame instead
//! of owning everything it touches.
//!
//! `for_each_by_index_nd` (free function
//! [`make_for_each_by_index_nd_task`]) is `for_each_by_index`'s own
//! counterpart for an
//! [`IndexRanges<T, N>`](crate::algorithm::index_range::IndexRanges) of more
//! than one dimension, walking its Cartesian product one dimension-0 row at
//! a time and handing `f` each row's own N-dimensional sub-range, available
//! the same three ways.
//!
//! # Design
//!
//! **No deferred `beg`/`end`/`step`/`range`.** Upstream's `B`, `E`, `S` and
//! `R` are each `std::decay_t<std::unwrap_ref_decay_t<...>>`, so a caller
//! may pass `std::ref(x)` and have the task re-read `x`'s current value on
//! every run -- `unittests/test_for_each.cpp`'s `StatefulParallelFor.*` and
//! `StatefulForEach.NegativeIndexRange.*` cases exist only to exercise that.
//! Rust has no equivalent primitive standing in for a C++ reference wrapper
//! here, and the acceptance this module is held to names exactly the cases
//! that do not need one -- positive and negative steps, invalid ranges, the
//! closure-wrapper cases -- so this port takes `beg`, `end`, `step` and
//! `range` by value, fixed when the task is built. A caller who wants the
//! range to vary between runs can read it from a `Runtime` body with
//! [`Runtime::for_each_index`] / [`Runtime::for_each_by_index`] instead,
//! computing it fresh each call.
//!
//! **One dispatch trait, four concrete impls, no blanket.** Upstream's
//! `make_*_task` branches `if constexpr(part.type() == PartitionerType::STATIC)`
//! between [`StaticLoop`]'s strided walk and [`DynamicLoop`]'s shared-cursor
//! race. A caller generic over an arbitrary `P: Partitioner` cannot make
//! that choice in Rust -- neither loop trait is implemented by every
//! partitioner, by design (`partitioner`'s own `# Design` section) -- so
//! [`IndexDispatch`] is the per-concrete-type answer: one `dispatch` method,
//! implemented once each for [`StaticPartitioner`] (over `spawn_static`)
//! and for [`GuidedPartitioner`], [`DynamicPartitioner`] and
//! [`RandomPartitioner`] (over `spawn_dynamic`, since all three are
//! [`DynamicLoop`]s). Two blanket impls keyed on `P: StaticLoop` and
//! `P: DynamicLoop` were the first shape tried instead, and do not compile:
//! nothing stops a future type from implementing both loop traits, so
//! rustc's coherence check cannot rule out the overlap, unlike the four
//! concrete impls here, which name a closed, non-overlapping set of types.
//!
//! **A chunk body is a factory's product, not a cloned chunk.** A worker's
//! own chunk body is called once per chunk that worker's task walks --
//! several, under [`StaticLoop::loop_static`]'s stride or a busy
//! [`DynamicLoop::loop_dynamic`] cursor -- and upstream's own per-worker
//! lambda is `[=](...) mutable`, so the one copy of `f` it captures persists
//! (and may mutate) across every chunk that single invocation walks.
//! [`IndexDispatch::dispatch`]'s `make_body` is called once per worker for
//! the same reason: the `B` it returns owns that worker's own state across
//! every chunk, rather than a fresh instance being cloned in per chunk,
//! which would silently reset anything a stateful `f` mutates between
//! chunks.
//!
//! **`dispatch` submits through a callback, not a shared spawner trait.**
//! The owned path ([`make_for_each_index_task`], run from
//! [`FlowBuilder::emplace_runtime`]) submits through
//! [`Runtime::silent_async`], which needs `&mut Runtime`; the borrowed path
//! ([`Runtime::for_each_index`]) submits through
//! [`Scope::spawn`](crate::executor::scope::Scope::spawn), which needs only
//! `&Scope`. A trait abstracting over "submit a task" would have
//! to name one receiver mutability for both, which neither call site
//! actually has in common. A plain `&mut dyn FnMut(bool, Box<dyn FnOnce() +
//! Send + 'env>)` sidesteps the mismatch instead: each call site closes over
//! whichever of `rt` or `s` it holds, in its own way, outside
//! [`IndexDispatch`]'s own bounds entirely. The `bool` is upstream's own
//! `(++w == W || ...) ? task() : rt.silent_async(task)` choice, carried
//! across the callback rather than decided on either side of it.
//!
//! **A closure wrapper's body is `FnMut`, not `FnOnce`, on an honest
//! assumption.** [`ClosureWrapper::invoke`] demands
//! `F: FnMut()`, so `spawn_static` and `spawn_dynamic` build a body that
//! satisfies that signature by taking its one chunk-walking `B` through an
//! `Option::take`, which would panic were the wrapper to call it a second
//! time. No wrapper in this crate or its tests ever does -- `partitioner`'s
//! own tests call a wrapper's body zero or one times, and
//! `tf::PartitionerBase::operator()`'s only real-world implementations are
//! the same shape -- so this documents the assumption rather than defending
//! against a caller this module has no test for.
//!
//! **The N-dimensional overload is its own name, not the same
//! `for_each_by_index` dispatching on rank.** Upstream's
//! `for_each_by_index<R>` is one template over `R: IndexRangesLike`,
//! `if constexpr`-branching inside on `R::rank == 1` between the 1-D
//! unraveling fast path and an N-D box partition. Rust's const generics let
//! [`index_range::IndexRanges<T, N>`](crate::algorithm::index_range::IndexRanges)
//! serve the same range of `N`, but a function generic over `N` has no
//! equivalent branch: it type-checks one body for every `N`, so it cannot
//! call [`IndexRange`]'s own 1-D-only methods the way upstream's discarded
//! branch can. [`make_for_each_by_index_nd_task`] is therefore a second,
//! explicitly-named entry point rather than a second case of
//! [`make_for_each_by_index_task`] -- [`Runtime::for_each_by_index_nd`] and
//! [`ForEachIndexBuilder::for_each_by_index_nd`] alongside it, the same three
//! ways the 1-D form is already available.
//!
//! **Partitioning an N-D range only ever splits dimension 0.** A worker's
//! chunk of an `IndexRanges<T, N>` has to stay a single rectangular sub-box
//! for [`IndexRanges::discrete_domain`](crate::algorithm::index_range::IndexRanges::discrete_domain)
//! to hand it to `f` in one call, and only a whole number of dimension 0's
//! own "rows" (one row being every other dimension's own extent) is
//! guaranteed to form one. So [`make_for_each_by_index_nd_task`] dispatches
//! over `range.dim_size(0)` rather than `range.size()`, and multiplies each
//! worker's chunk bounds by the row size before calling `discrete_domain` --
//! `index_range`'s own `# Design` section says why a finer, any-dimension
//! split like upstream's `ceil`/`floor`/`upper_slice`/`lower_slice` is out of
//! this issue's scope.

use std::boxed::Box;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crate::algorithm::index_range::{
    IndexInt, IndexRange, IndexRanges, distance, is_index_range_invalid,
};
use crate::algorithm::partitioner::{
    ClosureWrapper, DynamicLoop, DynamicPartitioner, GuidedPartitioner, Partitioner,
    RandomPartitioner, StaticLoop, StaticPartitioner,
};
use crate::flow_builder::FlowBuilder;
use crate::graph::Task;
use crate::runtime::Runtime;

/// Dispatches the chunked per-worker tasks for this partitioner's own
/// scheduling family: upstream's `if constexpr(part.type() == ...)` branch
/// in each `make_*_task`, resolved per concrete partitioner type -- see the
/// module's `# Design` section for why a blanket impl over [`StaticLoop`] or
/// [`DynamicLoop`] cannot stand in for the four below.
///
/// `make_body` is called once per worker, never re-cloned across the
/// chunks that same worker's own task may walk, so a chunk body that
/// mutates its own state does so across every chunk one worker processes,
/// matching a chunk-walking closure captured once into upstream's per-worker
/// lambda. `submit` decides, for each worker's task, whether it runs inline
/// or is handed off -- the module docs say why that is a callback rather
/// than a shared trait.
pub trait IndexDispatch: Partitioner {
    /// See the trait's own docs.
    fn dispatch<'env, B, Make>(
        &self,
        n: usize,
        w_total: usize,
        make_body: Make,
        submit: &mut dyn FnMut(bool, Box<dyn FnOnce() + Send + 'env>),
    ) where
        B: FnMut(usize, usize) + Send + 'env,
        Make: Fn() -> B;
}

impl<CW> IndexDispatch for StaticPartitioner<CW>
where
    CW: ClosureWrapper + Clone + Send + 'static,
{
    fn dispatch<'env, B, Make>(
        &self,
        n: usize,
        w_total: usize,
        make_body: Make,
        submit: &mut dyn FnMut(bool, Box<dyn FnOnce() + Send + 'env>),
    ) where
        B: FnMut(usize, usize) + Send + 'env,
        Make: Fn() -> B,
    {
        spawn_static(self, n, w_total, make_body, submit);
    }
}

impl<CW> IndexDispatch for GuidedPartitioner<CW>
where
    CW: ClosureWrapper + Clone + Send + 'static,
{
    fn dispatch<'env, B, Make>(
        &self,
        n: usize,
        w_total: usize,
        make_body: Make,
        submit: &mut dyn FnMut(bool, Box<dyn FnOnce() + Send + 'env>),
    ) where
        B: FnMut(usize, usize) + Send + 'env,
        Make: Fn() -> B,
    {
        spawn_dynamic(self, n, w_total, make_body, submit);
    }
}

impl<CW> IndexDispatch for DynamicPartitioner<CW>
where
    CW: ClosureWrapper + Clone + Send + 'static,
{
    fn dispatch<'env, B, Make>(
        &self,
        n: usize,
        w_total: usize,
        make_body: Make,
        submit: &mut dyn FnMut(bool, Box<dyn FnOnce() + Send + 'env>),
    ) where
        B: FnMut(usize, usize) + Send + 'env,
        Make: Fn() -> B,
    {
        spawn_dynamic(self, n, w_total, make_body, submit);
    }
}

impl<CW> IndexDispatch for RandomPartitioner<CW>
where
    CW: ClosureWrapper + Clone + Send + 'static,
{
    fn dispatch<'env, B, Make>(
        &self,
        n: usize,
        w_total: usize,
        make_body: Make,
        submit: &mut dyn FnMut(bool, Box<dyn FnOnce() + Send + 'env>),
    ) where
        B: FnMut(usize, usize) + Send + 'env,
        Make: Fn() -> B,
    {
        spawn_dynamic(self, n, w_total, make_body, submit);
    }
}

/// The static branch of [`IndexDispatch::dispatch`]: one task per worker,
/// each walking its own strided share of `[0, n)` via
/// [`StaticLoop::loop_static`] -- upstream's `curr_b`/`chunk_size`
/// bookkeeping around `part.loop`, deciding inline-or-submit exactly as
/// upstream's own short-circuited `(++w == W || (curr_b += chunk_size) >=
/// N))` does: the `curr_b` update is skipped, not merely unobserved, when
/// `w` alone already ends the loop.
fn spawn_static<'env, CW, B, Make>(
    part: &StaticPartitioner<CW>,
    n: usize,
    w_total: usize,
    make_body: Make,
    submit: &mut dyn FnMut(bool, Box<dyn FnOnce() + Send + 'env>),
) where
    CW: ClosureWrapper + Clone + Send + 'static,
    B: FnMut(usize, usize) + Send + 'env,
    Make: Fn() -> B,
{
    let mut w = 0_usize;
    let mut curr_b = 0_usize;
    while w < w_total && curr_b < n {
        let chunk_size = part.adjusted_chunk_size(n, w_total, w);
        let task_curr_b = curr_b;
        let p_call = part.clone();
        let p_loop = part.clone();
        let mut body = Some(make_body());
        let task: Box<dyn FnOnce() + Send + 'env> = Box::new(move || {
            p_call.call(move || {
                let body = body
                    .take()
                    .expect("a closure wrapper runs a partitioned chunk body at most once");
                p_loop.loop_static(n, w_total, task_curr_b, chunk_size, body);
            });
        });

        w += 1;
        let run_inline = if w == w_total {
            true
        } else {
            curr_b += chunk_size;
            curr_b >= n
        };

        submit(run_inline, task);
    }
}

/// The dynamic branch of [`IndexDispatch::dispatch`]: one task per worker,
/// all racing a shared cursor via [`DynamicLoop::loop_dynamic`] -- upstream's
/// `*next` atomic, shared across every task through an [`Arc`] where
/// upstream's is a `std::shared_ptr`.
fn spawn_dynamic<'env, P, B, Make>(
    part: &P,
    n: usize,
    w_total: usize,
    make_body: Make,
    submit: &mut dyn FnMut(bool, Box<dyn FnOnce() + Send + 'env>),
) where
    P: DynamicLoop + Clone + Send + 'static,
    B: FnMut(usize, usize) + Send + 'env,
    Make: Fn() -> B,
{
    let next = Arc::new(AtomicUsize::new(0));
    for w in 0..w_total {
        let p_call = part.clone();
        let p_loop = part.clone();
        let next_loop = Arc::clone(&next);
        let mut body = Some(make_body());
        let task: Box<dyn FnOnce() + Send + 'env> = Box::new(move || {
            p_call.call(move || {
                let body = body
                    .take()
                    .expect("a closure wrapper runs a partitioned chunk body at most once");
                p_loop.loop_dynamic(n, w_total, &next_loop, body);
            });
        });

        submit(w + 1 == w_total, task);
    }
}

/// Builds the runtime body of a `for_each_index` task: upstream's
/// `make_for_each_index_task`. Walks `[beg, end)` in steps of `step`,
/// calling `f` once per index, split across `rt`'s workers by `part`.
///
/// An invalid range (see [`is_index_range_invalid`]) or an empty one makes
/// the body a no-op, matching upstream exactly. See the module's `# Design`
/// section for why `beg`, `end` and `step` are fixed values rather than
/// upstream's deferred providers.
///
/// ```
/// use std::sync::atomic::{AtomicI64, Ordering};
/// use std::sync::Arc;
///
/// use rustflow::algorithm::for_each_index::make_for_each_index_task;
/// use rustflow::algorithm::partitioner::DefaultPartitioner;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let sum = Arc::new(AtomicI64::new(0));
/// let counted = Arc::clone(&sum);
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_for_each_index_task(
///     0_i64,
///     10,
///     1,
///     move |i| {
///         counted.fetch_add(i, Ordering::Relaxed);
///     },
///     DefaultPartitioner::default(),
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(sum.load(Ordering::Relaxed), 45);
/// ```
pub fn make_for_each_index_task<T, C, P>(
    beg: T,
    end: T,
    step: T,
    f: C,
    part: P,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: IndexInt + Send + 'static,
    C: Fn(T) + Clone + Send + 'static,
    P: IndexDispatch + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        if is_index_range_invalid(beg, end, step) {
            return;
        }
        let n = distance(beg, end, step);
        if n == 0 {
            return;
        }
        let mut w = rt.num_workers();
        if w <= 1 || n <= part.chunk_size() {
            let f = f.clone();
            part.call(|| {
                let mut idx = beg;
                for _ in 0..n {
                    f(idx);
                    idx = idx + step;
                }
            });
            return;
        }
        if n < w {
            w = n;
        }
        let f = f.clone();
        part.dispatch(
            n,
            w,
            || {
                let f = f.clone();
                move |part_b: usize, part_e: usize| {
                    let mut idx = beg + step * T::from_position(part_b);
                    for _ in part_b..part_e {
                        f(idx);
                        idx = idx + step;
                    }
                }
            },
            &mut |run_inline, task| {
                if run_inline {
                    task();
                } else {
                    rt.silent_async(task);
                }
            },
        );
    }
}

/// Builds the runtime body of a `for_each_by_index` task: upstream's
/// `make_for_each_by_index_task`. Walks `range` one chunk at a time, calling
/// `f` with each chunk's own sub-range (its [`IndexRange::discrete_domain`]),
/// split across `rt`'s workers by `part`.
///
/// An invalid `range` or an empty one makes the body a no-op, matching
/// upstream exactly. See the module's `# Design` section for why `range` is
/// a fixed value rather than upstream's deferred provider.
///
/// ```
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::Arc;
///
/// use rustflow::algorithm::for_each_index::make_for_each_by_index_task;
/// use rustflow::algorithm::index_range::IndexRange;
/// use rustflow::algorithm::partitioner::DefaultPartitioner;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let total = Arc::new(AtomicUsize::new(0));
/// let counted = Arc::clone(&total);
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_for_each_by_index_task(
///     IndexRange::new(0_i32, 10, 1),
///     move |chunk: IndexRange<i32>| {
///         counted.fetch_add(chunk.size(), Ordering::Relaxed);
///     },
///     DefaultPartitioner::default(),
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(total.load(Ordering::Relaxed), 10);
/// ```
pub fn make_for_each_by_index_task<T, C, P>(
    range: IndexRange<T>,
    f: C,
    part: P,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: IndexInt + Send + 'static,
    C: Fn(IndexRange<T>) + Clone + Send + 'static,
    P: IndexDispatch + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        if is_index_range_invalid(range.begin(), range.end(), range.step_size()) {
            return;
        }
        let n = range.size();
        if n == 0 {
            return;
        }
        let mut w = rt.num_workers();
        if w <= 1 || n <= part.chunk_size() {
            let f = f.clone();
            part.call(|| f(range));
            return;
        }
        if n < w {
            w = n;
        }
        let f = f.clone();
        part.dispatch(
            n,
            w,
            || {
                let f = f.clone();
                move |part_b: usize, part_e: usize| {
                    f(range.discrete_domain(part_b, part_e));
                }
            },
            &mut |run_inline, task| {
                if run_inline {
                    task();
                } else {
                    rt.silent_async(task);
                }
            },
        );
    }
}

/// Builds the runtime body of the N-dimensional `for_each_by_index`: upstream's
/// `make_for_each_by_index_task` for `R::rank > 1`. Walks `range`'s Cartesian
/// product one dimension-0 "row" at a time, calling `f` with each row's own
/// sub-range (its [`IndexRanges::discrete_domain`]), split across `rt`'s
/// workers by `part`.
///
/// An empty `range` (any dimension's own range invalid or zero-size) makes
/// the body a no-op. See the module's `# Design` section for why this is a
/// separate entry point from [`make_for_each_by_index_task`] rather than a
/// second case of it, and for why partitioning only ever splits dimension 0.
///
/// ```
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::Arc;
///
/// use rustflow::algorithm::for_each_index::make_for_each_by_index_nd_task;
/// use rustflow::algorithm::index_range::{IndexRange, IndexRanges};
/// use rustflow::algorithm::partitioner::DefaultPartitioner;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let total = Arc::new(AtomicUsize::new(0));
/// let counted = Arc::clone(&total);
///
/// let range = IndexRanges::from_ranges([IndexRange::new(0_i32, 4, 1), IndexRange::new(0, 5, 1)]);
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_for_each_by_index_nd_task(
///     range,
///     move |chunk: IndexRanges<i32, 2>| {
///         counted.fetch_add(chunk.size(), Ordering::Relaxed);
///     },
///     DefaultPartitioner::default(),
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(total.load(Ordering::Relaxed), 20);
/// ```
pub fn make_for_each_by_index_nd_task<T, C, P, const N: usize>(
    range: IndexRanges<T, N>,
    f: C,
    part: P,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: IndexInt + Send + 'static,
    C: Fn(IndexRanges<T, N>) + Clone + Send + 'static,
    P: IndexDispatch + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let n = range.size();
        if n == 0 {
            return;
        }
        let n_outer = range.dim_size(0);
        let mut w = rt.num_workers();
        if w <= 1 || n <= part.chunk_size() {
            let f = f.clone();
            part.call(|| f(range));
            return;
        }
        if n_outer < w {
            w = n_outer;
        }
        // `n` is nonzero, so every dimension's own count is nonzero (`size`
        // is their plain product), including `n_outer` itself -- safe to
        // divide by.
        let inner_volume = n / n_outer;
        let f = f.clone();
        part.dispatch(
            n_outer,
            w,
            || {
                let f = f.clone();
                move |part_b: usize, part_e: usize| {
                    f(range.discrete_domain(part_b * inner_volume, part_e * inner_volume));
                }
            },
            &mut |run_inline, task| {
                if run_inline {
                    task();
                } else {
                    rt.silent_async(task);
                }
            },
        );
    }
}

impl<'rt> Runtime<'rt> {
    /// Walks `[beg, end)` in steps of `step` on this runtime task's own
    /// node, calling `f` once per index: this module's own addition, built
    /// on [`Runtime::scope`] so `f` may borrow from the calling body's own
    /// frame instead of needing `Send + 'static` ownership the way
    /// [`make_for_each_index_task`] does. See the module's `# Design`
    /// section.
    ///
    /// ```
    /// use std::sync::atomic::{AtomicI64, Ordering};
    ///
    /// use rustflow::algorithm::partitioner::DefaultPartitioner;
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     // Borrowed from this body's own frame -- never an `Arc` -- which
    ///     // is what `Runtime::scope` underneath makes sound: every worker
    ///     // this spawns is joined before `for_each_index` returns.
    ///     let squares: [AtomicI64; 10] = std::array::from_fn(|_| AtomicI64::new(0));
    ///     runtime.for_each_index(
    ///         0_i64,
    ///         10,
    ///         1,
    ///         |i| {
    ///             let slot = usize::try_from(i).expect("non-negative index");
    ///             squares[slot].store(i * i, Ordering::Relaxed);
    ///         },
    ///         DefaultPartitioner::default(),
    ///     );
    ///     for (slot, square) in squares.iter().enumerate() {
    ///         let i = i64::try_from(slot).expect("slot fits in i64");
    ///         assert_eq!(square.load(Ordering::Relaxed), i * i);
    ///     }
    /// });
    ///
    /// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn for_each_index<T, C, P>(&mut self, beg: T, end: T, step: T, f: C, part: P)
    where
        T: IndexInt + Send + 'static,
        C: Fn(T) + Clone + Send,
        P: IndexDispatch + Clone + Send + 'static,
    {
        if is_index_range_invalid(beg, end, step) {
            return;
        }
        let n = distance(beg, end, step);
        if n == 0 {
            return;
        }
        let mut w = self.num_workers();
        if w <= 1 || n <= part.chunk_size() {
            let f = f.clone();
            part.call(|| {
                let mut idx = beg;
                for _ in 0..n {
                    f(idx);
                    idx = idx + step;
                }
            });
            return;
        }
        if n < w {
            w = n;
        }
        self.scope(move |s| {
            part.dispatch(
                n,
                w,
                || {
                    let f = f.clone();
                    move |part_b: usize, part_e: usize| {
                        let mut idx = beg + step * T::from_position(part_b);
                        for _ in part_b..part_e {
                            f(idx);
                            idx = idx + step;
                        }
                    }
                },
                &mut |run_inline, task| {
                    if run_inline {
                        task();
                    } else {
                        s.spawn(task);
                    }
                },
            );
        });
    }

    /// Walks `range` one chunk at a time on this runtime task's own node,
    /// calling `f` with each chunk's own sub-range: this module's own
    /// addition, built on [`Runtime::scope`] for [`Runtime::for_each_index`]'s
    /// reason. See the module's `# Design` section.
    ///
    /// ```
    /// use rustflow::algorithm::index_range::IndexRange;
    /// use rustflow::algorithm::partitioner::DefaultPartitioner;
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let mut seen = 0_usize;
    ///     runtime.for_each_by_index(
    ///         IndexRange::new(0_i32, 10, 1),
    ///         |chunk: IndexRange<i32>| {
    ///             let _ = chunk.size();
    ///         },
    ///         DefaultPartitioner::default(),
    ///     );
    ///     seen += 1;
    ///     assert_eq!(seen, 1);
    /// });
    ///
    /// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn for_each_by_index<T, C, P>(&mut self, range: IndexRange<T>, f: C, part: P)
    where
        T: IndexInt + Send + 'static,
        C: Fn(IndexRange<T>) + Clone + Send,
        P: IndexDispatch + Clone + Send + 'static,
    {
        if is_index_range_invalid(range.begin(), range.end(), range.step_size()) {
            return;
        }
        let n = range.size();
        if n == 0 {
            return;
        }
        let mut w = self.num_workers();
        if w <= 1 || n <= part.chunk_size() {
            let f = f.clone();
            part.call(|| f(range));
            return;
        }
        if n < w {
            w = n;
        }
        self.scope(move |s| {
            part.dispatch(
                n,
                w,
                || {
                    let f = f.clone();
                    move |part_b: usize, part_e: usize| {
                        f(range.discrete_domain(part_b, part_e));
                    }
                },
                &mut |run_inline, task| {
                    if run_inline {
                        task();
                    } else {
                        s.spawn(task);
                    }
                },
            );
        });
    }

    /// Walks `range`'s Cartesian product one dimension-0 row at a time on
    /// this runtime task's own node, calling `f` with each row's own
    /// sub-range: this module's own addition, built on [`Runtime::scope`]
    /// for [`Runtime::for_each_index`]'s reason. See the module's `# Design`
    /// section for why this is a distinct method from
    /// [`Runtime::for_each_by_index`] rather than a second case of it.
    ///
    /// ```
    /// use rustflow::algorithm::index_range::{IndexRange, IndexRanges};
    /// use rustflow::algorithm::partitioner::DefaultPartitioner;
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let mut seen = 0_usize;
    ///     let range =
    ///         IndexRanges::from_ranges([IndexRange::new(0_i32, 4, 1), IndexRange::new(0, 3, 1)]);
    ///     runtime.for_each_by_index_nd(
    ///         range,
    ///         |chunk: IndexRanges<i32, 2>| {
    ///             let _ = chunk.size();
    ///         },
    ///         DefaultPartitioner::default(),
    ///     );
    ///     seen += 1;
    ///     assert_eq!(seen, 1);
    /// });
    ///
    /// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn for_each_by_index_nd<T, C, P, const N: usize>(
        &mut self,
        range: IndexRanges<T, N>,
        f: C,
        part: P,
    ) where
        T: IndexInt + Send + 'static,
        C: Fn(IndexRanges<T, N>) + Clone + Send,
        P: IndexDispatch + Clone + Send + 'static,
    {
        let n = range.size();
        if n == 0 {
            return;
        }
        let n_outer = range.dim_size(0);
        let mut w = self.num_workers();
        if w <= 1 || n <= part.chunk_size() {
            let f = f.clone();
            part.call(|| f(range));
            return;
        }
        if n_outer < w {
            w = n_outer;
        }
        let inner_volume = n / n_outer;
        self.scope(move |s| {
            part.dispatch(
                n_outer,
                w,
                || {
                    let f = f.clone();
                    move |part_b: usize, part_e: usize| {
                        f(range.discrete_domain(part_b * inner_volume, part_e * inner_volume));
                    }
                },
                &mut |run_inline, task| {
                    if run_inline {
                        task();
                    } else {
                        s.spawn(task);
                    }
                },
            );
        });
    }
}

/// [`FlowBuilder`] wrappers for [`for_each_index`](make_for_each_index_task)
/// and [`for_each_by_index`](make_for_each_by_index_task), queued into the
/// graph as a runtime task.
///
/// A second trait rather than two more methods on [`FlowBuilder`] itself:
/// this module owns only `src/algorithm/for_each_index.rs` this wave, and a
/// default method must live in the trait that declares it, which rules out
/// a second `impl FlowBuilder` block the way a second `impl Runtime` block
/// (above) is possible for an inherent type. Blanket-implemented for every
/// [`FlowBuilder`], so `taskflow.for_each_index(...)` reads the same at the
/// call site as every other builder method once this trait is in scope --
/// [`crate::prelude`] carries it for the same reason it carries
/// [`FlowBuilder`] itself.
///
/// ```
/// // Only the prelude, exactly as a caller would write it -- no direct
/// // `use` of `ForEachIndexBuilder` itself.
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let mut taskflow = Taskflow::new();
/// taskflow.for_each_index(
///     0_i32,
///     5,
///     1,
///     |_| {},
///     rustflow::algorithm::partitioner::DefaultPartitioner::default(),
/// );
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// ```
pub trait ForEachIndexBuilder: FlowBuilder {
    /// Creates a runtime task that walks `[beg, end)` in steps of `step`,
    /// calling `f` once per index: upstream's `FlowBuilder::for_each_index`,
    /// over [`make_for_each_index_task`].
    fn for_each_index<T, C, P>(&mut self, beg: T, end: T, step: T, f: C, part: P) -> Task
    where
        T: IndexInt + Send + 'static,
        C: Fn(T) + Clone + Send + 'static,
        P: IndexDispatch + Clone + Send + 'static,
    {
        self.emplace_runtime(make_for_each_index_task(beg, end, step, f, part))
    }

    /// Creates a runtime task that walks `range` one chunk at a time,
    /// calling `f` with each chunk's own sub-range: upstream's
    /// `FlowBuilder::for_each_by_index`, over [`make_for_each_by_index_task`].
    fn for_each_by_index<T, C, P>(&mut self, range: IndexRange<T>, f: C, part: P) -> Task
    where
        T: IndexInt + Send + 'static,
        C: Fn(IndexRange<T>) + Clone + Send + 'static,
        P: IndexDispatch + Clone + Send + 'static,
    {
        self.emplace_runtime(make_for_each_by_index_task(range, f, part))
    }

    /// Creates a runtime task that walks `range`'s Cartesian product one
    /// dimension-0 row at a time, calling `f` with each row's own sub-range:
    /// upstream's `FlowBuilder::for_each_by_index` for `R::rank > 1`, over
    /// [`make_for_each_by_index_nd_task`]. See the module's `# Design`
    /// section for why this is a distinct method rather than a second case
    /// of [`Self::for_each_by_index`].
    fn for_each_by_index_nd<T, C, P, const N: usize>(
        &mut self,
        range: IndexRanges<T, N>,
        f: C,
        part: P,
    ) -> Task
    where
        T: IndexInt + Send + 'static,
        C: Fn(IndexRanges<T, N>) + Clone + Send + 'static,
        P: IndexDispatch + Clone + Send + 'static,
    {
        self.emplace_runtime(make_for_each_by_index_nd_task(range, f, part))
    }
}

impl<B: FlowBuilder + ?Sized> ForEachIndexBuilder for B {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    use super::*;
    use crate::Executor;
    use crate::algorithm::partitioner::{DefaultClosureWrapper, DefaultPartitioner};
    use crate::taskflow::Taskflow;

    #[test]
    fn for_each_index_visits_every_index_exactly_once() {
        for workers in 1..=4 {
            let seen: Arc<[AtomicUsize; 50]> =
                Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
            let marked = Arc::clone(&seen);

            let mut taskflow = Taskflow::new();
            taskflow.for_each_index(
                0_i64,
                50,
                1,
                move |i: i64| {
                    let idx = usize::try_from(i).expect("non-negative in this test");
                    marked[idx].fetch_add(1, Ordering::Relaxed);
                },
                DefaultPartitioner::default(),
            );

            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");

            for (i, count) in seen.iter().enumerate() {
                assert_eq!(
                    count.load(Ordering::Relaxed),
                    1,
                    "index {i}, {workers} worker(s)"
                );
            }
        }
    }

    #[test]
    fn for_each_index_negative_step_walks_backward() {
        let sum = Arc::new(AtomicI64::new(0));
        let counted = Arc::clone(&sum);

        let mut taskflow = Taskflow::new();
        taskflow.for_each_index(
            10_i64,
            0,
            -1,
            move |i| {
                counted.fetch_add(i, Ordering::Relaxed);
            },
            DefaultPartitioner::default(),
        );

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        // 10 + 9 + ... + 1 = 55.
        assert_eq!(sum.load(Ordering::Relaxed), 55);
    }

    #[test]
    fn for_each_index_invalid_range_is_a_no_op() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&counter);

        let mut taskflow = Taskflow::new();
        taskflow.for_each_index(
            0_i32,
            -1,
            1,
            move |_| {
                counted.fetch_add(1, Ordering::Relaxed);
            },
            DefaultPartitioner::default(),
        );

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn for_each_by_index_visits_every_chunk_sub_range_once() {
        for workers in 1..=4 {
            let total = Arc::new(AtomicUsize::new(0));
            let counted = Arc::clone(&total);

            let mut taskflow = Taskflow::new();
            taskflow.for_each_by_index(
                IndexRange::new(0_i32, 97, 1),
                move |chunk: IndexRange<i32>| {
                    counted.fetch_add(chunk.size(), Ordering::Relaxed);
                },
                DefaultPartitioner::default(),
            );

            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");

            assert_eq!(total.load(Ordering::Relaxed), 97, "{workers} worker(s)");
        }
    }

    #[test]
    fn for_each_by_index_nd_visits_every_cell_exactly_once() {
        use crate::algorithm::index_range::IndexRanges;

        for workers in 1..=4 {
            let rows = 7_usize;
            let cols = 11_usize;
            let seen: Arc<Vec<AtomicUsize>> =
                Arc::new((0..rows * cols).map(|_| AtomicUsize::new(0)).collect());
            let marked = Arc::clone(&seen);

            let range = IndexRanges::from_ranges([
                IndexRange::new(0_i32, 7, 1),
                IndexRange::new(0_i32, 11, 1),
            ]);

            let mut taskflow = Taskflow::new();
            taskflow.for_each_by_index_nd(
                range,
                move |chunk: IndexRanges<i32, 2>| {
                    let (i0, i1, _) = chunk.dim(0);
                    let (j0, j1, _) = chunk.dim(1);
                    for i in i0..i1 {
                        for j in j0..j1 {
                            let row = usize::try_from(i).expect("non-negative in this test");
                            let col = usize::try_from(j).expect("non-negative in this test");
                            marked[row * cols + col].fetch_add(1, Ordering::Relaxed);
                        }
                    }
                },
                DefaultPartitioner::default(),
            );

            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");

            for (idx, count) in seen.iter().enumerate() {
                assert_eq!(
                    count.load(Ordering::Relaxed),
                    1,
                    "cell {idx}, {workers} worker(s)"
                );
            }
        }
    }

    #[test]
    fn runtime_for_each_index_lets_the_body_borrow_its_own_frame() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let mut hits = [0_u32; 20];
            runtime.for_each_index(
                0_i64,
                20,
                1,
                |i| {
                    let _ = i;
                },
                DefaultPartitioner::default(),
            );
            // Nothing above actually needed to mutate `hits` to prove the
            // bound compiles without `Send + 'static`; touch it so the
            // borrow is real rather than merely legal.
            hits[0] = 1;
            assert_eq!(hits[0], 1);
        });

        Executor::new(3)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }

    #[test]
    fn runtime_for_each_by_index_lets_the_body_borrow_its_own_frame() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let mut total = 0_usize;
            runtime.for_each_by_index(
                IndexRange::new(0_i32, 40, 1),
                |chunk: IndexRange<i32>| {
                    let _ = chunk.size();
                },
                DefaultPartitioner::default(),
            );
            total += 1;
            assert_eq!(total, 1);
        });

        Executor::new(3)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }

    #[test]
    fn a_closure_wrapper_runs_once_per_worker_task_for_the_static_family() {
        let wrapper_calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&wrapper_calls);
        let element_calls = Arc::new(AtomicUsize::new(0));
        let counted_elements = Arc::clone(&element_calls);

        let part =
            StaticPartitioner::<_>::with_closure_wrapper(1, move |task: &mut dyn FnMut()| {
                counted.fetch_add(1, Ordering::Relaxed);
                task();
            });

        let mut taskflow = Taskflow::new();
        taskflow.for_each_index(
            0_i32,
            100,
            1,
            move |_| {
                counted_elements.fetch_add(1, Ordering::Relaxed);
            },
            part,
        );

        let workers = 8;
        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(element_calls.load(Ordering::Relaxed), 100);
        assert_eq!(wrapper_calls.load(Ordering::Relaxed), workers);
    }

    #[test]
    fn a_closure_wrapper_runs_at_most_once_per_worker_task_for_a_dynamic_family() {
        let wrapper_calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&wrapper_calls);
        let element_calls = Arc::new(AtomicUsize::new(0));
        let counted_elements = Arc::clone(&element_calls);

        let part =
            DynamicPartitioner::<_>::with_closure_wrapper(4, move |task: &mut dyn FnMut()| {
                counted.fetch_add(1, Ordering::Relaxed);
                task();
            });

        let mut taskflow = Taskflow::new();
        taskflow.for_each_index(
            0_i32,
            1000,
            1,
            move |_| {
                counted_elements.fetch_add(1, Ordering::Relaxed);
            },
            part,
        );

        let workers = 8;
        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(element_calls.load(Ordering::Relaxed), 1000);
        assert!(wrapper_calls.load(Ordering::Relaxed) <= workers);
    }

    /// The identity wrapper is the default -- no special setup needed for
    /// the common case.
    #[test]
    fn default_closure_wrapper_needs_no_special_setup() {
        let part = StaticPartitioner::<DefaultClosureWrapper>::default();
        let counter = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&counter);

        let mut taskflow = Taskflow::new();
        taskflow.for_each_index(
            0_i32,
            10,
            1,
            move |_| {
                counted.fetch_add(1, Ordering::Relaxed);
            },
            part,
        );

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert_eq!(counter.load(Ordering::Relaxed), 10);
    }
}

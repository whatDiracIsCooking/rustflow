//! Index-driven algorithms: the index-range types they partition work over
//! and the partitioning strategies that drive them.
//!
//! This is the crate's own module tree, not a translation of a single
//! upstream header; each submodule's own `//!` doc says which upstream file
//! it translates, if any.
//!
//! - [`for_each_index`] `for_each_index` and `for_each_by_index`, over an
//!   integer range and an [`index_range::IndexRange`] respectively.
//! - [`index_range`] the one-dimensional index range an algorithm walks and
//!   partitions, `[begin, end)` in steps of `step_size`.
//! - [`merge`] a parallel merge of two sorted slices into a third, by
//!   co-rank chunking over [`Runtime::scope`](crate::Runtime::scope).
//! - [`partitioner`] the strategies that split `[0, n)` into chunks for a
//!   parallel loop -- static, guided, dynamic and random -- and the closure
//!   wrapper a caller can run around every chunk's body.
//! - [`for_each`] the first algorithm built on the two above plus
//!   [`crate::Runtime::scope`]: a parallel loop over a mutable slice.
//!
//! [`AlgorithmBuilder`] is the family's front door, one
//! blanket-implemented extension trait carrying every algorithm's
//! owned-data [`FlowBuilder`] wrapper, beside the partitioners,
//! [`IndexRange`], [`IndexRanges`] and the `make_*_task` factories this
//! module re-exports from the submodules that define them.
//!
//! # Design
//!
//! **One extension trait for the whole family.** [`FlowBuilder`] is a
//! *trait* -- `src/flow_builder.rs`'s own `# Design` section says why --
//! implemented by `Taskflow` and `Subflow` rather than a single struct a
//! second `impl` block could extend, so an algorithm's owned-data wrapper
//! cannot be an inherent method and lives on a blanket-implemented
//! extension trait instead: [`AlgorithmBuilder`], whose every method is one
//! [`FlowBuilder::emplace_runtime`] call over the [`Runtime`] method or the
//! `make_*_task` factory of the same name. One trait carries all of them
//! rather than one trait per algorithm, because a per-algorithm trait holds
//! no concept of its own -- the family shares one vocabulary of owned
//! parameters, one partitioner set and one lock-once-per-invocation
//! ownership rule -- and each would cost a caller another name in scope and
//! another prelude line for nothing. `DESIGN.md`'s "Decisions taken in M10"
//! entry has the rest, including why the wrappers are not moved into
//! [`FlowBuilder`] itself.
//!
//! **A shared chunk-loop driver.** Every one of upstream's `make_*_task`
//! functions re-derives the same two shapes over `[0, n)` -- a fixed share
//! per worker for a [`partitioner::StaticLoop`] partitioner, one shared pool
//! every worker races to drain for a [`partitioner::DynamicLoop`] one -- and
//! differs only in what runs over each chunk once it is handed out.
//! [`SlicePartitioner::drive`] is that shared shape, over a `&mut [T]`
//! carved up and spawned under a [`crate::Scope`] by the `pub(crate)`
//! helpers below it -- `drive_per_worker`, `drive_shared` and what feeds
//! them -- never by indices into a shared `&mut`, which is the whole reason
//! `Scope` and `split_at_mut` replace upstream's raw-pointer aliasing here.
//! [`for_each`] is the first caller and the reason the helpers live here
//! rather than there: the sort, transform/fill/generate, reduce, find and
//! scan algorithms still to come all drive the identical two shapes over a
//! `&mut [T]` and differ from `for_each` only in what a chunk's body does
//! with it, which is exactly what staying generic over `Body: Fn(&mut
//! [T])` buys them.
//!
//! **One trait, implemented four times rather than twice with a blanket
//! impl.** [`SlicePartitioner::drive`] needs
//! [`partitioner::StaticPartitioner`]'s own branch to call
//! [`partitioner::StaticLoop::loop_static`] and the other three's to call
//! [`partitioner::DynamicLoop::loop_dynamic`], which a single generic
//! `impl<P: StaticLoop> SlicePartitioner for P` and
//! `impl<P: DynamicLoop> SlicePartitioner for P` cannot express: Rust's
//! coherence check rejects two blanket impls keyed on different trait
//! bounds even when, as here, no type could ever implement both bounds at
//! once -- that is exactly the "if constexpr" branch `partitioner`'s own
//! `# Design` section says Rust has no equivalent for. Four direct impls
//! over the four concrete partitioners -- three of them one-line forwards to
//! a shared `partition_dynamic` -- are what is left, which is also why
//! [`SlicePartitioner`] is `pub` rather than `pub(crate)`: a trait bound on
//! a `pub fn` has to be at least as visible as the function is, and it is
//! *sealed* (implementable only inside this crate, [`index_range::IndexInt`]'s
//! own pattern) rather than opened up, since nothing outside the four
//! partitioners above can answer either branch.
//!
//! **The dynamic family's shared pool is a lock, not upstream's atomic.**
//! `partition_dynamic` runs a partitioner's own `loop_dynamic` exactly
//! once, single-threaded, over a *local* cursor purely to enumerate the
//! chunk boundaries -- a pure function of `n`, `w` and (for
//! [`partitioner::RandomPartitioner`]) its seed, independent of which worker
//! would have asked -- then carves every chunk out of the slice up front in
//! that order. What a real race over upstream's atomic buys instead, a
//! worker that is running fast claiming more of the remaining work, is
//! regained by `drive_shared` handing the pre-cut pieces out of one
//! [`std::sync::Mutex`]-guarded queue: the chunk *boundaries* are decided
//! before any worker runs, same as upstream's, but which worker is handed
//! which chunk is still resolved at run time.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use crate::Scope;
use crate::algorithm::find::FindPartitioner;
use crate::algorithm::for_each_index::IndexDispatch;
use crate::algorithm::index_range::IndexInt;
use crate::algorithm::partitioner::{DynamicLoop, Partitioner, StaticLoop};
use crate::algorithm::reduce::ReducePartitioner;
use crate::flow_builder::FlowBuilder;
use crate::graph::Task;
use crate::runtime::Runtime;

pub mod fill;
pub mod find;
pub mod for_each;
pub mod for_each_index;
pub mod generate;
pub mod index_range;
pub mod merge;
pub mod module;
pub mod partitioner;
pub mod reduce;
pub mod scan;
pub mod sort;
pub mod transform;

pub use crate::algorithm::fill::{make_fill_n_task, make_fill_task};
pub use crate::algorithm::find::{
    make_find_if_not_task, make_find_if_task, make_max_element_task, make_min_element_task,
};
pub use crate::algorithm::for_each::make_for_each_task;
pub use crate::algorithm::for_each_index::{
    make_for_each_by_index_nd_task, make_for_each_by_index_task, make_for_each_index_task,
};
pub use crate::algorithm::generate::{make_generate_n_task, make_generate_task};
pub use crate::algorithm::index_range::{IndexRange, IndexRanges};
pub use crate::algorithm::merge::{make_merge_task, make_merge_task_by};
pub use crate::algorithm::module::make_module_task;
pub use crate::algorithm::partitioner::{
    ClosureWrapper, DefaultClosureWrapper, DefaultPartitioner, DynamicPartitioner,
    GuidedPartitioner, RandomPartitioner, StaticPartitioner,
};
pub use crate::algorithm::scan::{
    make_exclusive_scan_task, make_inclusive_scan_task, make_inclusive_scan_with_init_task,
    make_transform_exclusive_scan_task, make_transform_inclusive_scan_task,
    make_transform_inclusive_scan_with_init_task,
};
pub use crate::algorithm::sort::{make_sort_by_task, make_sort_task};
pub use crate::algorithm::transform::{make_transform_task, make_transform2_task};

/// Every algorithm's owned-data [`FlowBuilder`] wrapper: upstream's
/// `FlowBuilder::for_each`, `for_each_index`, `transform`, `fill`,
/// `generate`, `reduce`, `find_if`, `sort`, `*_scan` and `merge` members,
/// each queueing the [`Runtime`] method of the same name into the graph as a
/// runtime task over data it owns a share of.
///
/// Blanket-implemented for every [`FlowBuilder`], so a wrapper reads at the
/// call site exactly as [`Taskflow`](crate::Taskflow)'s own methods do, and
/// in [`crate::prelude`] beside [`FlowBuilder`] itself for the reason that
/// module's docs give: a trait's methods are only callable where the trait
/// is imported. This module's `# Design` section says why one trait carries
/// the whole family, and why the wrappers are an extension trait rather than
/// [`FlowBuilder`] methods.
///
/// ```
/// // Only the prelude, exactly as a caller would write it -- no direct
/// // `use` of `AlgorithmBuilder` itself, and one trait for all three
/// // families called here.
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let mut taskflow = Taskflow::new();
/// taskflow.for_each_index(
///     0_i32,
///     5,
///     1,
///     |_| {},
///     rustflow::algorithm::DefaultPartitioner::default(),
/// );
/// taskflow.sort(std::sync::Arc::new(std::sync::Mutex::new(vec![2, 1])));
/// let (_merged, _slot) = taskflow.merge(vec![1, 4], vec![2, 3]);
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// ```
pub trait AlgorithmBuilder: FlowBuilder {
    /// Creates a runtime task that locks `data` and runs [`Runtime::for_each`]
    /// over it: upstream's `for_each(beg, end, c, part)`, over a shared
    /// owned `Vec` rather than an iterator pair a caller must otherwise keep
    /// alive.
    ///
    /// `f` and `part` are cloned on every run of the task -- relevant only
    /// for a task revisited by a cycle -- matching upstream copying `c` and
    /// `part` into the closure by value.
    ///
    /// `rustflow::prelude::*` is the only import this example needs beyond
    /// the partitioner and the data it builds the task over: `AlgorithmBuilder`
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
    /// let data = Arc::new(Mutex::new(vec![0u32; 10]));
    /// taskflow.for_each(
    ///     Arc::clone(&data),
    ///     |x: &mut u32| *x = 1,
    ///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
    /// );
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(data.lock().expect("uncontended").iter().sum::<u32>(), 10);
    /// ```
    fn for_each<T, F, P>(&mut self, data: Arc<Mutex<Vec<T>>>, f: F, part: P) -> Task
    where
        T: Send + 'static,
        F: Fn(&mut T) + Sync + Clone + Send + 'static,
        P: SlicePartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(make_for_each_task(data, f, part))
    }

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
    /// [`make_for_each_by_index_nd_task`]. See
    /// [`for_each_index`]'s own `# Design` section for
    /// why this is a distinct method rather than a second case of
    /// [`Self::for_each_by_index`].
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

    /// Creates a runtime task that locks `out`, borrows `input`, and runs
    /// [`Runtime::transform`] over them: upstream's one-input
    /// `FlowBuilder::transform`, over owned, `Arc`-shared data rather than an
    /// iterator pair a caller must otherwise keep alive.
    ///
    /// `f` and `part` are cloned on every run of the task, matching upstream
    /// copying `c` and `part` into the closure by value.
    ///
    /// ```
    /// use std::sync::{Arc, Mutex};
    ///
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let input = Arc::new(vec![1, 2, 3, 4]);
    /// let out = Arc::new(Mutex::new(vec![0; 4]));
    /// taskflow.transform(
    ///     Arc::clone(&input),
    ///     Arc::clone(&out),
    ///     |x: &i32| x * 10,
    ///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
    /// );
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(*out.lock().expect("uncontended"), vec![10, 20, 30, 40]);
    /// ```
    fn transform<A, B, F, P>(
        &mut self,
        input: Arc<Vec<A>>,
        out: Arc<Mutex<Vec<B>>>,
        f: F,
        part: P,
    ) -> Task
    where
        A: Sync + Send + 'static,
        B: Send + 'static,
        F: Fn(&A) -> B + Sync + Clone + Send + 'static,
        P: SlicePartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(make_transform_task(input, out, f, part))
    }

    /// The two-input half of the owned-data wrapper: upstream's two-input
    /// `FlowBuilder::transform` overload, named `transform2` for the reason
    /// [`Runtime::transform2`]'s own doc gives.
    ///
    /// ```
    /// use std::sync::{Arc, Mutex};
    ///
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let a = Arc::new(vec![1, 2, 3]);
    /// let b = Arc::new(vec![100, 200, 300]);
    /// let out = Arc::new(Mutex::new(vec![0; 3]));
    /// taskflow.transform2(
    ///     Arc::clone(&a),
    ///     Arc::clone(&b),
    ///     Arc::clone(&out),
    ///     |x: &i32, y: &i32| x + y,
    ///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
    /// );
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(*out.lock().expect("uncontended"), vec![101, 202, 303]);
    /// ```
    fn transform2<A, B, C, F, P>(
        &mut self,
        a: Arc<Vec<A>>,
        b: Arc<Vec<B>>,
        out: Arc<Mutex<Vec<C>>>,
        f: F,
        part: P,
    ) -> Task
    where
        A: Sync + Send + 'static,
        B: Sync + Send + 'static,
        C: Send + 'static,
        F: Fn(&A, &B) -> C + Sync + Clone + Send + 'static,
        P: SlicePartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(make_transform2_task(a, b, out, f, part))
    }

    /// Creates a runtime task that locks `out` and runs
    /// [`Runtime::fill`](self::fill) over it: upstream's
    /// `FlowBuilder::fill`.
    ///
    /// ```
    /// use std::sync::{Arc, Mutex};
    ///
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let out = Arc::new(Mutex::new(vec![0; 5]));
    /// taskflow.fill(
    ///     Arc::clone(&out),
    ///     7,
    ///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
    /// );
    ///
    /// Executor::new(3).run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(*out.lock().expect("uncontended"), vec![7; 5]);
    /// ```
    fn fill<T, P>(&mut self, out: Arc<Mutex<Vec<T>>>, value: T, part: P) -> Task
    where
        T: Clone + Send + Sync + 'static,
        P: SlicePartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(make_fill_task(out, value, part))
    }

    /// Creates a runtime task that locks `out` and runs
    /// [`Runtime::fill_n`](self::fill) over its first `count` elements:
    /// upstream's `FlowBuilder::fill_n`.
    ///
    /// ```
    /// use std::sync::{Arc, Mutex};
    ///
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let out = Arc::new(Mutex::new(vec![0; 5]));
    /// taskflow.fill_n(
    ///     Arc::clone(&out),
    ///     3,
    ///     7,
    ///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
    /// );
    ///
    /// Executor::new(3).run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(*out.lock().expect("uncontended"), vec![7, 7, 7, 0, 0]);
    /// ```
    fn fill_n<T, P>(&mut self, out: Arc<Mutex<Vec<T>>>, count: usize, value: T, part: P) -> Task
    where
        T: Clone + Send + Sync + 'static,
        P: SlicePartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(make_fill_n_task(out, count, value, part))
    }

    /// Creates a runtime task that locks `out` and runs
    /// [`Runtime::generate`](self::generate) over it: upstream's
    /// `FlowBuilder::generate`.
    ///
    /// ```
    /// use std::sync::{Arc, Mutex};
    /// use std::sync::atomic::{AtomicI32, Ordering};
    ///
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let out = Arc::new(Mutex::new(vec![0; 4]));
    /// let next = Arc::new(AtomicI32::new(1));
    /// taskflow.generate(
    ///     Arc::clone(&out),
    ///     move || next.fetch_add(1, Ordering::Relaxed),
    ///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
    /// );
    ///
    /// Executor::new(3).run(&taskflow).wait().expect("no task panicked");
    /// let guard = out.lock().expect("uncontended");
    /// let mut sorted = guard.clone();
    /// sorted.sort_unstable();
    /// assert_eq!(sorted, vec![1, 2, 3, 4]);
    /// ```
    fn generate<T, G, P>(&mut self, out: Arc<Mutex<Vec<T>>>, generator: G, part: P) -> Task
    where
        T: Send + 'static,
        G: FnMut() -> T + Send + Clone + 'static,
        P: SlicePartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(make_generate_task(out, generator, part))
    }

    /// Creates a runtime task that locks `out` and runs
    /// [`Runtime::generate_n`](self::generate) over its first `count`
    /// elements: upstream's `FlowBuilder::generate_n`.
    ///
    /// ```
    /// use std::sync::{Arc, Mutex};
    ///
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let out = Arc::new(Mutex::new(vec![0; 5]));
    /// taskflow.generate_n(
    ///     Arc::clone(&out),
    ///     3,
    ///     || 9,
    ///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
    /// );
    ///
    /// Executor::new(3).run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(*out.lock().expect("uncontended"), vec![9, 9, 9, 0, 0]);
    /// ```
    fn generate_n<T, G, P>(
        &mut self,
        out: Arc<Mutex<Vec<T>>>,
        count: usize,
        generator: G,
        part: P,
    ) -> Task
    where
        T: Send + 'static,
        G: FnMut() -> T + Send + Clone + 'static,
        P: SlicePartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(make_generate_n_task(out, count, generator, part))
    }

    /// Creates a runtime task that runs [`Runtime::reduce`] over `data`,
    /// locking `init` for the task's whole run: upstream's `reduce(beg, end,
    /// init, bop, part)`, over a shared owned `Vec` and a shared,
    /// lock-guarded `init` rather than iterator and reference parameters a
    /// caller must otherwise keep alive.
    ///
    /// `bop` and `part` are cloned on every run of the task -- relevant only
    /// for a task revisited by a cycle -- matching upstream copying both into
    /// the closure by value.
    ///
    /// ```
    /// use std::sync::{Arc, Mutex};
    ///
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let data = Arc::new((1..=100).collect::<Vec<i32>>());
    /// let sum = Arc::new(Mutex::new(0));
    /// taskflow.reduce(
    ///     Arc::clone(&data),
    ///     Arc::clone(&sum),
    ///     |a: &i32, b: &i32| a + b,
    ///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
    /// );
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(*sum.lock().expect("uncontended"), 5050);
    /// ```
    fn reduce<T, O, P>(&mut self, data: Arc<Vec<T>>, init: Arc<Mutex<T>>, bop: O, part: P) -> Task
    where
        T: Clone + Send + Sync + 'static,
        O: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
        P: ReducePartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut guard = init.lock().unwrap_or_else(PoisonError::into_inner);
            runtime.reduce(&data, &mut *guard, bop.clone(), part.clone());
        })
    }

    /// The owned-data half of [`Runtime::transform_reduce`]: see
    /// [`AlgorithmBuilder::reduce`] for what is cloned on every run.
    fn transform_reduce<T, R, O, U, P>(
        &mut self,
        data: Arc<Vec<T>>,
        init: Arc<Mutex<R>>,
        bop: O,
        uop: U,
        part: P,
    ) -> Task
    where
        T: Sync + Send + 'static,
        R: Send + 'static,
        O: Fn(&R, &R) -> R + Sync + Clone + Send + 'static,
        U: Fn(&T) -> R + Sync + Clone + Send + 'static,
        P: ReducePartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut guard = init.lock().unwrap_or_else(PoisonError::into_inner);
            runtime.transform_reduce(&data, &mut *guard, bop.clone(), uop.clone(), part.clone());
        })
    }

    /// The owned-data half of [`Runtime::transform_reduce2`]: see
    /// [`AlgorithmBuilder::reduce`] for what is cloned on every run.
    fn transform_reduce2<T1, T2, R, O, U, P>(
        &mut self,
        data1: Arc<Vec<T1>>,
        data2: Arc<Vec<T2>>,
        init: Arc<Mutex<R>>,
        bop: O,
        uop: U,
        part: P,
    ) -> Task
    where
        T1: Sync + Send + 'static,
        T2: Sync + Send + 'static,
        R: Send + 'static,
        O: Fn(&R, &R) -> R + Sync + Clone + Send + 'static,
        U: Fn(&T1, &T2) -> R + Sync + Clone + Send + 'static,
        P: ReducePartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut guard = init.lock().unwrap_or_else(PoisonError::into_inner);
            runtime.transform_reduce2(
                &data1,
                &data2,
                &mut *guard,
                bop.clone(),
                uop.clone(),
                part.clone(),
            );
        })
    }

    /// The owned-data half of [`Runtime::reduce_by_index`]: see
    /// [`AlgorithmBuilder::reduce`] for what is cloned on every run. `range`
    /// is a plain value, not `Arc`-wrapped, since [`IndexRange`] is `Copy` --
    /// [`for_each_index`]'s own wrapper over an `IndexRange` takes it the
    /// same way.
    fn reduce_by_index<I, T, L, G, P>(
        &mut self,
        range: IndexRange<I>,
        init: Arc<Mutex<T>>,
        lop: L,
        gop: G,
        part: P,
    ) -> Task
    where
        I: IndexInt + Sync + Send + 'static,
        T: Send + 'static,
        L: Fn(IndexRange<I>, Option<T>) -> T + Sync + Clone + Send + 'static,
        G: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
        P: ReducePartitioner + Clone + Send + 'static,
    {
        self.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let mut guard = init.lock().unwrap_or_else(PoisonError::into_inner);
            runtime.reduce_by_index(range, &mut *guard, lop.clone(), gop.clone(), part.clone());
        })
    }

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
    /// the partitioner and the data it builds the task over: `AlgorithmBuilder`
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
        self.emplace_runtime(make_find_if_task(data, result, predicate, part))
    }

    /// The `find_if_not` half of the pair: see [`AlgorithmBuilder::find_if`].
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
        self.emplace_runtime(make_find_if_not_task(data, result, predicate, part))
    }

    /// The owned-data half of [`Runtime::min_element`]: see
    /// [`AlgorithmBuilder::find_if`].
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
        self.emplace_runtime(make_min_element_task(data, result, cmp, part))
    }

    /// The `max_element` half of the pair: see
    /// [`AlgorithmBuilder::min_element`].
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
        self.emplace_runtime(make_max_element_task(data, result, cmp, part))
    }

    /// Creates a runtime task that locks `out` and runs
    /// [`Runtime::inclusive_scan`] over it and `input`: upstream's
    /// `FlowBuilder::inclusive_scan(first, last, d_first, bop)`.
    ///
    /// `rustflow::prelude::*` is the only import this example needs:
    /// `AlgorithmBuilder` itself is one of the prelude's names, not imported
    /// here by name.
    ///
    /// ```
    /// use std::sync::{Arc, Mutex};
    ///
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let input = Arc::new(vec![1, 2, 3, 4, 5]);
    /// let out = Arc::new(Mutex::new(vec![0; 5]));
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.inclusive_scan(Arc::clone(&input), Arc::clone(&out), |a: &i32, b: &i32| a + b);
    ///
    /// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(*out.lock().expect("uncontended"), vec![1, 3, 6, 10, 15]);
    /// ```
    fn inclusive_scan<T, F>(&mut self, input: Arc<Vec<T>>, out: Arc<Mutex<Vec<T>>>, bop: F) -> Task
    where
        T: Send + Sync + Clone + 'static,
        F: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
    {
        self.emplace_runtime(make_inclusive_scan_task(input, out, bop))
    }

    /// Creates a runtime task that locks `out` and runs
    /// [`Runtime::inclusive_scan_with_init`] over it and `input`: upstream's
    /// `FlowBuilder::inclusive_scan(first, last, d_first, bop, init)`.
    fn inclusive_scan_with_init<T, F>(
        &mut self,
        input: Arc<Vec<T>>,
        out: Arc<Mutex<Vec<T>>>,
        bop: F,
        init: T,
    ) -> Task
    where
        T: Send + Sync + Clone + 'static,
        F: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
    {
        self.emplace_runtime(make_inclusive_scan_with_init_task(input, out, bop, init))
    }

    /// Creates a runtime task that locks `out` and runs
    /// [`Runtime::exclusive_scan`] over it and `input`: upstream's
    /// `FlowBuilder::exclusive_scan(first, last, d_first, init, bop)`.
    fn exclusive_scan<T, F>(
        &mut self,
        input: Arc<Vec<T>>,
        out: Arc<Mutex<Vec<T>>>,
        init: T,
        bop: F,
    ) -> Task
    where
        T: Send + Sync + Clone + 'static,
        F: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
    {
        self.emplace_runtime(make_exclusive_scan_task(input, out, init, bop))
    }

    /// Creates a runtime task that locks `out` and runs
    /// [`Runtime::transform_inclusive_scan`] over it and `input`: upstream's
    /// `FlowBuilder::transform_inclusive_scan(first, last, d_first, bop,
    /// uop)`.
    fn transform_inclusive_scan<T, F, Uop>(
        &mut self,
        input: Arc<Vec<T>>,
        out: Arc<Mutex<Vec<T>>>,
        bop: F,
        uop: Uop,
    ) -> Task
    where
        T: Send + Sync + Clone + 'static,
        F: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
        Uop: Fn(&T) -> T + Sync + Clone + Send + 'static,
    {
        self.emplace_runtime(make_transform_inclusive_scan_task(input, out, bop, uop))
    }

    /// Creates a runtime task that locks `out` and runs
    /// [`Runtime::transform_inclusive_scan_with_init`] over it and `input`:
    /// upstream's `FlowBuilder::transform_inclusive_scan(first, last,
    /// d_first, bop, uop, init)`.
    fn transform_inclusive_scan_with_init<T, F, Uop>(
        &mut self,
        input: Arc<Vec<T>>,
        out: Arc<Mutex<Vec<T>>>,
        bop: F,
        uop: Uop,
        init: T,
    ) -> Task
    where
        T: Send + Sync + Clone + 'static,
        F: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
        Uop: Fn(&T) -> T + Sync + Clone + Send + 'static,
    {
        self.emplace_runtime(make_transform_inclusive_scan_with_init_task(
            input, out, bop, uop, init,
        ))
    }

    /// Creates a runtime task that locks `out` and runs
    /// [`Runtime::transform_exclusive_scan`] over it and `input`: upstream's
    /// `FlowBuilder::transform_exclusive_scan(first, last, d_first, init,
    /// bop, uop)`.
    fn transform_exclusive_scan<T, F, Uop>(
        &mut self,
        input: Arc<Vec<T>>,
        out: Arc<Mutex<Vec<T>>>,
        init: T,
        bop: F,
        uop: Uop,
    ) -> Task
    where
        T: Send + Sync + Clone + 'static,
        F: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
        Uop: Fn(&T) -> T + Sync + Clone + Send + 'static,
    {
        self.emplace_runtime(make_transform_exclusive_scan_task(
            input, out, init, bop, uop,
        ))
    }

    /// Creates a runtime task that locks `data` and runs [`Runtime::sort_by`]
    /// over it: upstream's `sort(beg, end, cmp)`, over a shared owned `Vec`
    /// rather than an iterator pair a caller must otherwise keep alive.
    ///
    /// `cmp` is cloned on every run of the task -- relevant only for a task
    /// revisited by a cycle -- matching upstream copying `cmp` into the
    /// closure by value.
    ///
    /// ```
    /// use std::sync::{Arc, Mutex};
    ///
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let data = Arc::new(Mutex::new(vec![5i32, 3, 1, 4, 1, 5, 9, 2, 6]));
    /// taskflow.sort_by(Arc::clone(&data), |a: &i32, b: &i32| b.cmp(a));
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(
    ///     *data.lock().expect("uncontended"),
    ///     vec![9, 6, 5, 5, 4, 3, 2, 1, 1]
    /// );
    /// ```
    fn sort_by<T, C>(&mut self, data: Arc<Mutex<Vec<T>>>, cmp: C) -> Task
    where
        T: Send + 'static,
        C: Fn(&T, &T) -> Ordering + Sync + Clone + Send + 'static,
    {
        self.emplace_runtime(make_sort_by_task(data, cmp))
    }

    /// Creates a runtime task that locks `data` and runs [`Runtime::sort`]
    /// over it: upstream's `sort(beg, end)`.
    ///
    /// ```
    /// use std::sync::{Arc, Mutex};
    ///
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let data = Arc::new(Mutex::new(vec![5i32, 3, 1, 4, 1, 5, 9, 2, 6]));
    /// taskflow.sort(Arc::clone(&data));
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// assert_eq!(
    ///     *data.lock().expect("uncontended"),
    ///     vec![1, 1, 2, 3, 4, 5, 5, 6, 9]
    /// );
    /// ```
    fn sort<T>(&mut self, data: Arc<Mutex<Vec<T>>>) -> Task
    where
        T: Ord + Send + 'static,
    {
        self.emplace_runtime(make_sort_task(data))
    }

    /// Emplaces a runtime task that merges owned `a` and `b`, by `T`'s own
    /// [`Ord`]: upstream's `std::less<>` overload of `FlowBuilder::merge`,
    /// over owned inputs. Returns the task and the slot its body publishes
    /// the merged `Vec<T>` into once the run reaches it --
    /// [`merge`]'s own `# Design` section says why a slot,
    /// where upstream returns only the `Task`. See
    /// [`AlgorithmBuilder::merge_by`] for a custom comparator.
    ///
    /// `use rustflow::prelude::*;` brings this trait's methods into scope
    /// along with [`Taskflow`](crate::Taskflow)'s own -- it is in the
    /// prelude exactly because [`FlowBuilder`] is too, for the reason the
    /// prelude's own docs give.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let (_task, result) = taskflow.merge(vec![1, 4, 6], vec![2, 3, 5]);
    ///
    /// Executor::new(2)
    ///     .run(&taskflow)
    ///     .wait()
    ///     .expect("no task panicked");
    ///
    /// assert_eq!(*result.lock().expect("uncontended"), [1, 2, 3, 4, 5, 6]);
    /// ```
    fn merge<T>(&mut self, a: Vec<T>, b: Vec<T>) -> (Task, Arc<Mutex<Vec<T>>>)
    where
        T: Ord + Clone + Send + Sync + 'static,
    {
        let (body, result) = make_merge_task(a, b);
        (self.emplace_runtime(body), result)
    }

    /// Emplaces a runtime task that merges owned `a` and `b` under `cmp`:
    /// upstream's general `FlowBuilder::merge` overload, over owned inputs.
    /// See [`AlgorithmBuilder::merge`] for the `Ord`-only form and what the
    /// returned slot is for.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// let (_task, result) = taskflow.merge_by(vec![6, 4, 1], vec![5, 3, 2], |x, y| x > y);
    ///
    /// Executor::new(2)
    ///     .run(&taskflow)
    ///     .wait()
    ///     .expect("no task panicked");
    ///
    /// assert_eq!(*result.lock().expect("uncontended"), [6, 5, 4, 3, 2, 1]);
    /// ```
    fn merge_by<T, C>(&mut self, a: Vec<T>, b: Vec<T>, cmp: C) -> (Task, Arc<Mutex<Vec<T>>>)
    where
        T: Clone + Send + Sync + 'static,
        C: Fn(&T, &T) -> bool + Send + Sync + 'static,
    {
        let (body, result) = make_merge_task_by(a, b, cmp);
        (self.emplace_runtime(body), result)
    }
}

impl<B: FlowBuilder + ?Sized> AlgorithmBuilder for B {}

/// Closes [`SlicePartitioner`] against implementations outside this crate --
/// this module's `# Design` section says why, and
/// [`index_range::IndexInt`]'s own `sealed` module is the identical pattern.
mod sealed {
    /// Marker a type must carry to implement [`super::SlicePartitioner`].
    pub trait Sealed {}
}

/// Drives a parallel loop over a mutable slice, the shape every one of
/// upstream's `make_*_task` functions shares once its own short-circuits
/// have been ruled out: split `[0, n)` into chunks by this partitioner's own
/// scheme, then run `body` once per chunk under [`Runtime::scope`], wrapped
/// once per worker by this partitioner's closure wrapper.
///
/// Sealed against implementations outside this crate -- see this module's
/// `# Design` section for why a `pub` bound is needed at all, and why only
/// the four concrete partitioners below can answer it.
///
/// [`Runtime::scope`]: crate::Runtime::scope
pub trait SlicePartitioner: Partitioner + sealed::Sealed {
    /// Splits `data` (`n` elements, matching `data.len()`) among `w_total`
    /// workers and runs `body` over every chunk.
    fn drive<'scope, T, Body>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope mut [T],
        n: usize,
        w_total: usize,
        body: &'scope Body,
    ) where
        T: Send,
        Body: Fn(&mut [T]) + Sync;
}

impl<C: ClosureWrapper> sealed::Sealed for StaticPartitioner<C> {}
impl<C: ClosureWrapper> sealed::Sealed for GuidedPartitioner<C> {}
impl<C: ClosureWrapper> sealed::Sealed for DynamicPartitioner<C> {}
impl<C: ClosureWrapper> sealed::Sealed for RandomPartitioner<C> {}

impl<C: ClosureWrapper + Sync> SlicePartitioner for StaticPartitioner<C> {
    fn drive<'scope, T, Body>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope mut [T],
        n: usize,
        w_total: usize,
        body: &'scope Body,
    ) where
        T: Send,
        Body: Fn(&mut [T]) + Sync,
    {
        // One worker, one entry in `worker_ranges`; `loop_static`'s own
        // stride can still land several non-contiguous chunks in it when
        // the configured chunk size is smaller than `n / w_total`; see
        // `carve_round_robin` for why increasing-round, increasing-worker
        // order is also increasing-index order.
        let mut worker_ranges: Vec<Vec<(usize, usize)>> = vec![Vec::new(); w_total];
        let mut curr_b = 0usize;
        for (w, ranges) in worker_ranges.iter_mut().enumerate() {
            if curr_b >= n {
                break;
            }
            let chunk_size = self.adjusted_chunk_size(n, w_total, w);
            self.loop_static(n, w_total, curr_b, chunk_size, |b, e| ranges.push((b, e)));
            curr_b += chunk_size;
        }
        let per_worker = carve_round_robin(data, worker_ranges);
        drive_per_worker(scope, per_worker, self.closure_wrapper(), body);
    }
}

impl<C: ClosureWrapper + Sync> SlicePartitioner for GuidedPartitioner<C> {
    fn drive<'scope, T, Body>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope mut [T],
        n: usize,
        w_total: usize,
        body: &'scope Body,
    ) where
        T: Send,
        Body: Fn(&mut [T]) + Sync,
    {
        let pieces = partition_dynamic(self, data, n, w_total);
        drive_shared(scope, pieces, w_total, self.closure_wrapper(), body);
    }
}

impl<C: ClosureWrapper + Sync> SlicePartitioner for DynamicPartitioner<C> {
    fn drive<'scope, T, Body>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope mut [T],
        n: usize,
        w_total: usize,
        body: &'scope Body,
    ) where
        T: Send,
        Body: Fn(&mut [T]) + Sync,
    {
        let pieces = partition_dynamic(self, data, n, w_total);
        drive_shared(scope, pieces, w_total, self.closure_wrapper(), body);
    }
}

impl<C: ClosureWrapper + Sync> SlicePartitioner for RandomPartitioner<C> {
    fn drive<'scope, T, Body>(
        &'scope self,
        scope: &Scope<'scope, '_, '_>,
        data: &'scope mut [T],
        n: usize,
        w_total: usize,
        body: &'scope Body,
    ) where
        T: Send,
        Body: Fn(&mut [T]) + Sync,
    {
        let pieces = partition_dynamic(self, data, n, w_total);
        drive_shared(scope, pieces, w_total, self.closure_wrapper(), body);
    }
}

/// Runs `part`'s own `loop_dynamic` once, single-threaded over a local
/// cursor, purely to enumerate the chunk boundaries it would hand out --
/// this module's `# Design` section says why that is the same sequence a
/// real race over the cursor produces -- then carves `data` into exactly
/// those pieces, in order.
fn partition_dynamic<'a, T, P: DynamicLoop>(
    part: &P,
    data: &'a mut [T],
    n: usize,
    w_total: usize,
) -> Vec<&'a mut [T]> {
    let cursor = std::sync::atomic::AtomicUsize::new(0);
    let mut boundaries = Vec::new();
    part.loop_dynamic(n, w_total, &cursor, |b, e| boundaries.push((b, e)));

    let mut remaining = data;
    let mut pieces = Vec::with_capacity(boundaries.len());
    for (b, e) in boundaries {
        let (head, tail) = remaining.split_at_mut(e - b);
        pieces.push(head);
        remaining = tail;
    }
    pieces
}

/// Carves `data` into `worker_ranges[w]`'s chunks, one [`std::ops::IndexMut`]
/// split per chunk via `split_at_mut`, visited in increasing-round,
/// increasing-worker order.
///
/// That order is increasing-index order too, which is what makes a plain
/// sequential carve correct: within one round every worker's chunk size is
/// `StaticPartitioner::adjusted_chunk_size`'s (at most one apart, when the
/// configured size is `0`, or identical otherwise), so the round's chunks
/// tile `[0, n)` left to right in worker order; across rounds, a worker's
/// *own* stride is `w_total` times its chunk size, which is exactly what one
/// full round advances the shared cursor by. Debug builds check the
/// invariant rather than assume it.
fn carve_round_robin<'a, T>(
    data: &'a mut [T],
    worker_ranges: Vec<Vec<(usize, usize)>>,
) -> Vec<Vec<&'a mut [T]>> {
    let max_rounds = worker_ranges.iter().map(Vec::len).max().unwrap_or(0);
    let mut per_worker: Vec<Vec<&'a mut [T]>> = worker_ranges.iter().map(|_| Vec::new()).collect();
    let mut remaining = data;
    let mut consumed = 0usize;
    for round in 0..max_rounds {
        for (w, ranges) in worker_ranges.iter().enumerate() {
            if let Some(&(b, e)) = ranges.get(round) {
                debug_assert_eq!(b, consumed, "a static partition left a gap or an overlap");
                let (head, tail) = remaining.split_at_mut(e - b);
                per_worker[w].push(head);
                remaining = tail;
                consumed = e;
            }
        }
    }
    per_worker
}

/// Spawns one child per non-empty entry of `per_worker` under `scope`,
/// running the last inline on the calling worker: upstream's `(++w == W...)
/// ? task() : rt.silent_async(task)` for the static family. `body` runs
/// once per chunk, wrapped once per *worker* -- `wrapper.invoke` sees a
/// whole worker's chunk list, not one chunk -- matching upstream's
/// `part([=]{ part.loop(...) })` wrapping the whole per-worker closure.
pub(crate) fn drive_per_worker<'scope, T, W, Body>(
    scope: &Scope<'scope, '_, '_>,
    per_worker: Vec<Vec<&'scope mut [T]>>,
    wrapper: &'scope W,
    body: &'scope Body,
) where
    T: Send,
    W: ClosureWrapper + Sync,
    Body: Fn(&mut [T]) + Sync,
{
    let mut tasks: Vec<Vec<&'scope mut [T]>> = per_worker
        .into_iter()
        .filter(|chunks| !chunks.is_empty())
        .collect();
    let last = tasks.pop();

    for mut chunks in tasks {
        scope.spawn(move || {
            // `ClosureWrapper::invoke` takes `FnMut`, so the chunk list is
            // drained through a `&mut` borrow rather than moved by value --
            // the same chunk list is never visited twice regardless.
            wrapper.invoke(|| {
                for chunk in chunks.drain(..) {
                    body(chunk);
                }
            });
        });
    }

    if let Some(mut chunks) = last {
        wrapper.invoke(|| {
            for chunk in chunks.drain(..) {
                body(chunk);
            }
        });
    }
}

/// Spawns `w_total` children under `scope`, each draining a shared pool of
/// pre-cut chunks until it is empty, the last running inline: upstream's
/// `(++w == W) ? task() : rt.silent_async(task)` for the dynamic family,
/// over a [`std::sync::Mutex`]-guarded queue rather than upstream's atomic
/// cursor -- this module's `# Design` section says why that still lets the
/// faster worker claim more of the remaining work. `body` is wrapped once
/// per worker, like [`drive_per_worker`].
pub(crate) fn drive_shared<'scope, T, W, Body>(
    scope: &Scope<'scope, '_, '_>,
    pieces: Vec<&'scope mut [T]>,
    w_total: usize,
    wrapper: &'scope W,
    body: &'scope Body,
) where
    T: Send,
    W: ClosureWrapper + Sync,
    Body: Fn(&mut [T]) + Sync,
{
    let queue = Arc::new(Mutex::new(VecDeque::from(pieces)));

    for _ in 1..w_total {
        let queue = Arc::clone(&queue);
        scope.spawn(move || drain_queue(&queue, wrapper, body));
    }

    // The last worker runs on the calling thread, matching the static
    // family's own last-inline dispatch.
    drain_queue(&queue, wrapper, body);
}

/// Pops chunks off `queue` one at a time, under `wrapper`, until it is
/// empty: the body every [`drive_shared`] child and its inline last worker
/// share.
fn drain_queue<T, W, Body>(queue: &Mutex<VecDeque<&mut [T]>>, wrapper: &W, body: &Body)
where
    W: ClosureWrapper,
    Body: Fn(&mut [T]),
{
    wrapper.invoke(|| {
        loop {
            let next = queue
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .pop_front();
            match next {
                Some(chunk) => body(chunk),
                None => break,
            }
        }
    });
}

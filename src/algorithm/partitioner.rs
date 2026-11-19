//! Translated from `taskflow/algorithm/partitioner.hpp` (Taskflow v4.1.0).
//!
//! A partitioner decides how a parallel loop over `[0, n)` is cut into
//! chunks and handed to workers. Four strategies ship here:
//! [`StaticPartitioner`] pre-assigns a strided share to each worker,
//! [`GuidedPartitioner`] (this module's [`DefaultPartitioner`]) and
//! [`DynamicPartitioner`] hand chunks out from a shared cursor -- guided
//! shrinking the chunk as the remaining work does, dynamic keeping it fixed
//! -- and [`RandomPartitioner`] draws the chunk size itself from a range.
//! A fifth, [`SplittingPartitioner`], is the port's own: its chunk re-splits
//! when another worker steals it, so a worker that misses the first fan-out
//! can still take a share.
//!
//! # Design
//!
//! **One trait, two loop shapes, and why they stay apart.** Upstream's
//! `PartitionerBase` declares neither `loop` nor `adjusted_chunk_size`; each
//! concrete partitioner adds its own, and the two call shapes -- a static
//! stride over an explicit `curr_b`, a dynamic race over a shared
//! `AtomicUsize` -- never appear on the same type. A caller generic over
//! `P` picks the right one with `if constexpr(P::type() == STATIC)`, a
//! branch the compiler only instantiates one side of. Rust has no such
//! branch: a generic function type-checks every method it calls for every
//! instantiation, so a single trait carrying both shapes would force
//! [`StaticPartitioner`] to grow a `loop_dynamic` it cannot run and
//! [`GuidedPartitioner`] a `loop_static` it cannot run, each a panic
//! waiting for a caller that forgot to check. [`StaticLoop`] and
//! [`DynamicLoop`] are separate traits instead, implemented only by the
//! partitioners the shape fits, so a caller generic over `P: StaticLoop`
//! can no more reach `loop_dynamic` than `StaticPartitioner` can implement
//! it -- the `if constexpr` branch becomes a trait bound the compiler
//! checks once, at the call site, rather than a check every call would
//! otherwise need to repeat at runtime. [`Partitioner`] itself carries only
//! what every strategy shares: the chunk size, which family it belongs to,
//! and the closure wrapper.
//!
//! **The closure wrapper needs no `is_default_wrapper_v`.** Upstream's
//! `PartitionerBase::operator()` branches at compile time on whether its
//! closure wrapper type is the empty `DefaultClosureWrapper`, so that the
//! common case costs nothing. [`ClosureWrapper::invoke`] is a generic method
//! instead: [`DefaultClosureWrapper`]'s implementation runs the body
//! directly with no indirection, and a custom wrapper is reached by the
//! same call through monomorphization, not a runtime check -- the
//! specialization upstream writes by hand is what choosing an
//! implementation already gives here.
//!
//! **`RandomPartitioner` is seeded, not reseeded from the OS.** Upstream
//! builds a fresh `std::default_random_engine` from `std::random_device`
//! on every call to `loop`, so two runs never draw the same chunk sizes.
//! This port stores a `seed` field instead and (re)builds a
//! [`crate::Xorshift64`] from it on every call -- the crate's own
//! generator, not a `rand` dependency, for the reason `tests/work_stealing.rs`
//! gives for the same choice there: a twelve-line recurrence with a
//! known-answer test does not need to out-test a runtime crate it would
//! otherwise have to trust. Determinism costs nothing a chunk partitioner
//! needs: coverage -- every index visited exactly once -- comes from the
//! shared `AtomicUsize` cursor every worker races for, not from the chunk
//! sizes being unpredictable, and a fixed seed is what lets a test assert
//! on one run rather than sampling many.
//!
//! **`SplittingPartitioner` answers a measured fan-out, and is ours.** The
//! four types above hand a fixed number of chunk tasks -- one per worker --
//! to a single deque, and `BENCHMARKS.md`'s own fan-out measurement found
//! that reaching only 15 to 19 of 24 workers: a worker that misses the steal
//! window has no way to take a share afterwards, because nothing a running
//! chunk does makes more work visible. Rayon's splitter has no such window,
//! and [`SplittingPartitioner`] is that property ported to this scheduler
//! rather than to upstream's: a chunk task that finds itself running on a
//! thread other than the one that spawned it hands half of its remaining
//! share to a fresh task under its own node before walking the rest, so each
//! steal makes one more share stealable. A chunk running where it was
//! spawned does not split, which is what bounds the extra tasks by the
//! number of steals rather than by the size of the range. The default stays
//! [`GuidedPartitioner`], upstream's own.
//!
//! **A share's home is a [`std::thread::ThreadId`], not a worker index.**
//! The split test wants "was this task stolen", and the natural spelling is
//! [`Runtime::worker_id`](crate::Runtime::worker_id) against an id recorded
//! where the task was spawned. Half the spawn sites cannot reach one:
//! [`crate::Scope`] -- all the slice, reduce and find algorithms have at the
//! point they fan out -- exposes `spawn` and `spawn_runtime` and nothing
//! about the worker it would spawn from, and an executor's own worker-id
//! lookup is a map keyed by [`std::thread::ThreadId`] to begin with. So the
//! home recorded at spawn and the identity compared at entry are both thread
//! ids, one hop before the index upstream's `this_worker_id` would hand
//! back; a pool thread is a worker for the executor's whole life, so the two
//! comparisons answer the same question. The split *half* still needs a
//! [`Runtime`], which is why a splitting chunk task is
//! spawned runtime-bodied where the ported families' are not.
//!
//! **The chunk size is the splitting floor, and `0` means one call per
//! share.** A share stops splitting once either half would fall below the
//! configured chunk size (`0` meaning one index), which is the only bound a
//! steal-driven split needs: nothing splits without a steal, so the extra
//! tasks are counted by contention rather than by `n`. The same number is
//! how far a task's own walk sub-divides its share before calling the chunk
//! body -- and `0`, which asks the ported families to work a size out from
//! `n` and `w`, asks this one for one call per share instead, since the
//! balance it offers comes from re-splitting rather than from small chunks.
//!
//! **The default floor of one index stays, and what it used to cost has
//! gone.** `BENCHMARKS.md`'s splitting measurements all run a
//! `for_each_by_index` over a million indices at 24 workers, body one `tan`
//! an index. #381 and #401 read the default there at **1.90-2.86 ms** against
//! 0.72-0.85 for the same partitioner with its splits off -- 9 to 13 µs of
//! wall a re-split, 2.0x to 2.5x of the guided default -- and #400 attributed
//! the whole of that to the join a split opens rather than to the split: the
//! far half goes to a [`Runtime`] scope, and the scope's corun could not
//! notice the half finishing until its own 211-attempt search had failed.
//! Issue #412 gave a corun upstream's loop, which re-reads what it waits for
//! after every failed steal, and #409 re-took both measurements on that tree:
//!
//! | row, one process | chunk tasks | distinct workers | wall |
//! |---|---|---|---|
//! | floor of one -- [`SplittingPartitioner::default()`] | 113-132 | 21-24 / 24 | 0.893-1.275 ms |
//! | a floor above `n / w`, so nothing splits | 23 | 22-23 / 24 | 0.914-1.219 ms |
//! | [`GuidedPartitioner`], this crate's [`DefaultPartitioner`] | 23 | 14-18 / 24 | 1.211-1.399 ms |
//!
//! A re-split prices between **−1.9 and +1.2 µs** over the four processes
//! #409 measured -- a quantity whose sign the harness cannot fix -- so the
//! default is level with its own non-splitting control on wall and on CPU,
//! where it used to be 2 to 4x it. Against the *guided* default the same row
//! reads 0.88x in the process above and 1.19x and 1.18x in the two sweep
//! processes, this box's processes differing by up to 1.4x about any one row;
//! what is stable across all four is the comparison with the non-splitting
//! control, and that the floor of one is the only row of the sweep's seven
//! that ever reaches 24 of 24 workers. The floor stays at one for the reason
//! #401 kept it: a seven-row sweep taken in both row orders separates no
//! floor from another, or from splitting nothing, by more than either side's
//! own spread, and what the uncapped chain buys -- depth, so that a worker
//! arriving late to an already-stolen share still finds half of it to take --
//! is permanent where the cost was not. A caller who wants a coarser setting
//! asks for it with [`SplittingPartitioner::new`].
//!
//! **It implements neither [`StaticLoop`] nor [`DynamicLoop`].** Both
//! describe a walk a worker performs alone, over a `curr_b` or a shared
//! cursor; a splitting walk is neither, because it needs a handle on the
//! scheduler to hand its far half to. What it implements instead is the
//! per-algorithm driving trait each family already has --
//! [`crate::algorithm::SlicePartitioner`],
//! [`crate::algorithm::reduce::ReducePartitioner`],
//! [`crate::algorithm::find::FindPartitioner`] and
//! [`crate::algorithm::for_each_index::IndexDispatch`] -- so every algorithm
//! takes it unchanged -- each of them separately sealed in its own module,
//! so a fifth partitioner is an impl in each.
//!
//! **Omitted:** upstream's `PartitionerLike` concept and `is_partitioner_v`
//! check that a type derives from `PartitionerBase`. A `P: Partitioner`
//! bound already is that check; nothing else needs to restate it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{self, ThreadId};

use crate::runtime::Runtime;
use crate::{Scope, Xorshift64};

/// Which of the two scheduling families a partitioner belongs to: upstream's
/// `PartitionerType`. [`StaticLoop`] is implemented only by a
/// [`PartitionerType::Static`] partitioner, [`DynamicLoop`] only by a
/// [`PartitionerType::Dynamic`] one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PartitionerType {
    /// Each worker is pre-assigned a strided share of the range: upstream's
    /// `PartitionerType::STATIC`.
    Static,
    /// Workers race a shared cursor for the next chunk: upstream's
    /// `PartitionerType::DYNAMIC`.
    Dynamic,
}

/// Wraps a partitioned chunk body before it runs: upstream's
/// `PartitionerBase::operator()`, generalized over any wrapper rather than
/// compile-time-specialized on [`DefaultClosureWrapper`] alone -- see the
/// module's `# Design` section for why that costs nothing.
///
/// A closure `Fn(&mut dyn FnMut())` already implements this trait: it is
/// called with the chunk body and decides when, or whether, to run it.
pub trait ClosureWrapper {
    /// Runs `body` under this wrapper. Named `invoke`, not `call`: a closure
    /// implements this trait by the blanket impl below, and `call` is one of
    /// the names `rustc`'s `unstable_name_collisions` lint reserves against a
    /// future inherent `Fn::call`.
    fn invoke<F: FnMut()>(&self, body: F);
}

/// The identity closure wrapper: upstream's `DefaultClosureWrapper`. Runs a
/// chunk body with nothing before or after it.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultClosureWrapper;

impl ClosureWrapper for DefaultClosureWrapper {
    fn invoke<F: FnMut()>(&self, mut body: F) {
        body();
    }
}

impl<T: Fn(&mut dyn FnMut())> ClosureWrapper for T {
    fn invoke<F: FnMut()>(&self, mut body: F) {
        self(&mut body);
    }
}

/// The surface every partitioner shares: upstream's `PartitionerBase<C>`.
///
/// What is specific to one scheduling family -- `adjusted_chunk_size` on
/// [`StaticPartitioner`], the `loop` shapes on [`StaticLoop`] and
/// [`DynamicLoop`] -- is not here; see the module's `# Design` section for
/// why.
pub trait Partitioner {
    /// The closure wrapper type this partitioner's chunks run through.
    type Wrapper: ClosureWrapper;

    /// Which scheduling family this partitioner belongs to: upstream's
    /// `type()`.
    const PARTITIONER_TYPE: PartitionerType;

    /// The configured chunk size: upstream's `chunk_size()`. `0` asks a
    /// caller to work one out from the iteration count and worker count.
    fn chunk_size(&self) -> usize;

    /// Sets the chunk size: upstream's `chunk_size(size_t)` setter.
    fn set_chunk_size(&mut self, chunk_size: usize);

    /// An immutable reference to the closure wrapper: upstream's
    /// `closure_wrapper() const`.
    fn closure_wrapper(&self) -> &Self::Wrapper;

    /// A mutable reference to the closure wrapper: upstream's
    /// `closure_wrapper()`.
    fn closure_wrapper_mut(&mut self) -> &mut Self::Wrapper;

    /// Runs `body` through the closure wrapper: upstream's `operator()`.
    fn call<F: FnMut()>(&self, body: F) {
        self.closure_wrapper().invoke(body);
    }
}

/// The static scheduling loop: a worker strides through `[0, n)` in steps of
/// `w * chunk_size`, starting from its own pre-assigned `curr_b`. Implemented
/// only by [`StaticPartitioner`] -- see the module's `# Design` section.
pub trait StaticLoop: Partitioner {
    /// Runs every chunk from `curr_b` to `n`, stopping as soon as `f`
    /// returns `true`: upstream's `StaticPartitioner::loop`, the
    /// `bool`-returning branch of its `if constexpr`. What a `find` builds
    /// on.
    fn loop_static_until(
        &self,
        n: usize,
        w: usize,
        curr_b: usize,
        chunk_size: usize,
        f: impl FnMut(usize, usize) -> bool,
    );

    /// The same loop, running every chunk to completion: upstream's `loop`,
    /// the `void`-returning branch.
    fn loop_static(
        &self,
        n: usize,
        w: usize,
        curr_b: usize,
        chunk_size: usize,
        mut f: impl FnMut(usize, usize),
    ) {
        self.loop_static_until(n, w, curr_b, chunk_size, move |b, e| {
            f(b, e);
            false
        });
    }
}

/// The dynamic scheduling loop: workers race a shared `next` cursor for the
/// next chunk, each strategy choosing its own chunk size. Implemented by
/// [`GuidedPartitioner`], [`DynamicPartitioner`] and [`RandomPartitioner`] --
/// see the module's `# Design` section.
pub trait DynamicLoop: Partitioner {
    /// Runs chunks drawn from `next` until `n` is exhausted or `f` returns
    /// `true`: the `bool`-returning branch of each concrete `loop`. What a
    /// `find` builds on.
    fn loop_dynamic_until(
        &self,
        n: usize,
        w: usize,
        next: &AtomicUsize,
        f: impl FnMut(usize, usize) -> bool,
    );

    /// The same loop, running every chunk to completion: the `void`-returning
    /// branch.
    fn loop_dynamic(
        &self,
        n: usize,
        w: usize,
        next: &AtomicUsize,
        mut f: impl FnMut(usize, usize),
    ) {
        self.loop_dynamic_until(n, w, next, move |b, e| {
            f(b, e);
            false
        });
    }
}

/// A partitioner that divides `[0, n)` into chunks of a fixed size and
/// assigns them to workers round-robin, worker `w` starting at `w *
/// chunk_size` and striding `w_total * chunk_size` past it: upstream's
/// `StaticPartitioner`.
///
/// Best suited to a workload whose cost per iteration does not vary much --
/// [`GuidedPartitioner`] or [`DynamicPartitioner`] answer an unbalanced one
/// better, at the cost of the coordination a shared cursor needs.
///
/// ```
/// use rustflow::algorithm::partitioner::{
///     DefaultClosureWrapper, Partitioner, StaticLoop, StaticPartitioner,
/// };
///
/// let p = StaticPartitioner::<DefaultClosureWrapper>::new(3);
/// let mut seen = Vec::new();
/// p.loop_static(10, 1, 0, p.chunk_size(), |b, e| seen.push((b, e)));
/// assert_eq!(seen, vec![(0, 3), (3, 6), (6, 9), (9, 10)]);
/// ```
#[derive(Clone, Debug)]
pub struct StaticPartitioner<C: ClosureWrapper = DefaultClosureWrapper> {
    chunk_size: usize,
    closure_wrapper: C,
}

impl<C: ClosureWrapper + Default> Default for StaticPartitioner<C> {
    fn default() -> Self {
        Self {
            chunk_size: 0,
            closure_wrapper: C::default(),
        }
    }
}

impl<C: ClosureWrapper> StaticPartitioner<C> {
    /// A static partitioner with the given chunk size (`0` to let a caller
    /// work one out via [`StaticPartitioner::adjusted_chunk_size`]) and a
    /// default-constructed closure wrapper: upstream's
    /// `explicit StaticPartitioner(size_t)`.
    #[must_use]
    pub fn new(chunk_size: usize) -> Self
    where
        C: Default,
    {
        Self {
            chunk_size,
            closure_wrapper: C::default(),
        }
    }

    /// A static partitioner with the given chunk size and closure wrapper:
    /// upstream's `explicit StaticPartitioner(size_t, C&&)`.
    #[must_use]
    pub fn with_closure_wrapper(chunk_size: usize, closure_wrapper: C) -> Self {
        Self {
            chunk_size,
            closure_wrapper,
        }
    }

    /// The chunk size worker `i` of `w` would use for `n` iterations: the
    /// configured chunk size if it is non-zero, or `n / w`, with one extra
    /// iteration for each of the lowest-numbered `n % w` workers so every
    /// iteration is still covered. Upstream's `adjusted_chunk_size`.
    #[must_use]
    pub fn adjusted_chunk_size(&self, n: usize, w: usize, i: usize) -> usize {
        if self.chunk_size != 0 {
            self.chunk_size
        } else {
            n / w + usize::from(i < n % w)
        }
    }
}

impl<C: ClosureWrapper> Partitioner for StaticPartitioner<C> {
    type Wrapper = C;
    const PARTITIONER_TYPE: PartitionerType = PartitionerType::Static;

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn set_chunk_size(&mut self, chunk_size: usize) {
        self.chunk_size = chunk_size;
    }

    fn closure_wrapper(&self) -> &C {
        &self.closure_wrapper
    }

    fn closure_wrapper_mut(&mut self) -> &mut C {
        &mut self.closure_wrapper
    }
}

impl<C: ClosureWrapper> StaticLoop for StaticPartitioner<C> {
    fn loop_static_until(
        &self,
        n: usize,
        w: usize,
        curr_b: usize,
        chunk_size: usize,
        mut f: impl FnMut(usize, usize) -> bool,
    ) {
        let stride = w * chunk_size;
        let mut curr_b = curr_b;
        while curr_b < n {
            let curr_e = (curr_b + chunk_size).min(n);
            if f(curr_b, curr_e) {
                return;
            }
            curr_b += stride;
        }
    }
}

/// A partitioner whose chunk size shrinks from a large fraction of the
/// remaining work down to the configured floor as workers drain a shared
/// cursor, the last chunk possibly smaller still: upstream's
/// `GuidedPartitioner`, and this module's [`DefaultPartitioner`].
#[derive(Clone, Debug)]
pub struct GuidedPartitioner<C: ClosureWrapper = DefaultClosureWrapper> {
    chunk_size: usize,
    closure_wrapper: C,
}

impl<C: ClosureWrapper + Default> Default for GuidedPartitioner<C> {
    fn default() -> Self {
        Self {
            chunk_size: 0,
            closure_wrapper: C::default(),
        }
    }
}

impl<C: ClosureWrapper> GuidedPartitioner<C> {
    /// A guided partitioner with the given floor chunk size (`0` meaning
    /// `1`) and a default-constructed closure wrapper: upstream's
    /// `explicit GuidedPartitioner(size_t)`.
    #[must_use]
    pub fn new(chunk_size: usize) -> Self
    where
        C: Default,
    {
        Self {
            chunk_size,
            closure_wrapper: C::default(),
        }
    }

    /// A guided partitioner with the given floor chunk size and closure
    /// wrapper: upstream's `explicit GuidedPartitioner(size_t, C&&)`.
    #[must_use]
    pub fn with_closure_wrapper(chunk_size: usize, closure_wrapper: C) -> Self {
        Self {
            chunk_size,
            closure_wrapper,
        }
    }
}

impl<C: ClosureWrapper> Partitioner for GuidedPartitioner<C> {
    type Wrapper = C;
    const PARTITIONER_TYPE: PartitionerType = PartitionerType::Dynamic;

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn set_chunk_size(&mut self, chunk_size: usize) {
        self.chunk_size = chunk_size;
    }

    fn closure_wrapper(&self) -> &C {
        &self.closure_wrapper
    }

    fn closure_wrapper_mut(&mut self) -> &mut C {
        &mut self.closure_wrapper
    }
}

impl<C: ClosureWrapper> DynamicLoop for GuidedPartitioner<C> {
    fn loop_dynamic_until(
        &self,
        n: usize,
        w: usize,
        next: &AtomicUsize,
        mut f: impl FnMut(usize, usize) -> bool,
    ) {
        let chunk_size = if self.chunk_size == 0 {
            1
        } else {
            self.chunk_size
        };
        let p1 = 2 * w * (chunk_size + 1);
        let mut curr_b = next.load(Ordering::Relaxed);

        while curr_b < n {
            let r = n - curr_b;

            if r < p1 {
                loop {
                    curr_b = next.fetch_add(chunk_size, Ordering::Relaxed);
                    if curr_b >= n {
                        return;
                    }
                    if f(curr_b, (curr_b + chunk_size).min(n)) {
                        return;
                    }
                }
            } else {
                // Upstream's `p2 * r` is `(0.5 / w) * r`, i.e. `r / (2 * w)`:
                // integer division gives the same floor exactly, with no
                // float round-trip to be off by one for a large `r`.
                let mut q = r / (2 * w);
                if q < chunk_size {
                    q = chunk_size;
                }
                let curr_e = (curr_b + q).min(n);
                match next.compare_exchange(curr_b, curr_e, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => {
                        if f(curr_b, curr_e) {
                            return;
                        }
                        curr_b = curr_e;
                    }
                    Err(actual) => curr_b = actual,
                }
            }
        }
    }
}

/// A partitioner that splits `[0, n)` into equal chunks of the configured
/// size (`0` meaning `1`) and hands them out from a shared cursor in no
/// particular order: upstream's `DynamicPartitioner`.
#[derive(Clone, Debug)]
pub struct DynamicPartitioner<C: ClosureWrapper = DefaultClosureWrapper> {
    chunk_size: usize,
    closure_wrapper: C,
}

impl<C: ClosureWrapper + Default> Default for DynamicPartitioner<C> {
    fn default() -> Self {
        Self {
            chunk_size: 0,
            closure_wrapper: C::default(),
        }
    }
}

impl<C: ClosureWrapper> DynamicPartitioner<C> {
    /// A dynamic partitioner with the given chunk size (`0` meaning `1`) and
    /// a default-constructed closure wrapper: upstream's
    /// `explicit DynamicPartitioner(size_t)`.
    #[must_use]
    pub fn new(chunk_size: usize) -> Self
    where
        C: Default,
    {
        Self {
            chunk_size,
            closure_wrapper: C::default(),
        }
    }

    /// A dynamic partitioner with the given chunk size and closure wrapper:
    /// upstream's `explicit DynamicPartitioner(size_t, C&&)`.
    #[must_use]
    pub fn with_closure_wrapper(chunk_size: usize, closure_wrapper: C) -> Self {
        Self {
            chunk_size,
            closure_wrapper,
        }
    }
}

impl<C: ClosureWrapper> Partitioner for DynamicPartitioner<C> {
    type Wrapper = C;
    const PARTITIONER_TYPE: PartitionerType = PartitionerType::Dynamic;

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn set_chunk_size(&mut self, chunk_size: usize) {
        self.chunk_size = chunk_size;
    }

    fn closure_wrapper(&self) -> &C {
        &self.closure_wrapper
    }

    fn closure_wrapper_mut(&mut self) -> &mut C {
        &mut self.closure_wrapper
    }
}

impl<C: ClosureWrapper> DynamicLoop for DynamicPartitioner<C> {
    fn loop_dynamic_until(
        &self,
        n: usize,
        _w: usize,
        next: &AtomicUsize,
        mut f: impl FnMut(usize, usize) -> bool,
    ) {
        let chunk_size = if self.chunk_size == 0 {
            1
        } else {
            self.chunk_size
        };
        let mut curr_b = next.fetch_add(chunk_size, Ordering::Relaxed);
        while curr_b < n {
            if f(curr_b, (curr_b + chunk_size).min(n)) {
                return;
            }
            curr_b = next.fetch_add(chunk_size, Ordering::Relaxed);
        }
    }
}

/// The seed every [`RandomPartitioner`] starts from unless
/// [`RandomPartitioner::set_seed`] overrides it: an arbitrary nonzero
/// constant, [`crate::Xorshift64::new`] folding zero to a different one.
const DEFAULT_SEED: u64 = 0x5EED_5EED_5EED_5EED;

/// A partitioner that splits `[0, n)` into chunks whose size is drawn
/// uniformly from `[alpha * n * w, beta * n * w]` on every draw, then hands
/// them out from a shared cursor like [`DynamicPartitioner`]: upstream's
/// `RandomPartitioner`. See the module's `# Design` section for why the
/// random source is a seed this port stores rather than the OS.
#[derive(Clone, Debug)]
pub struct RandomPartitioner<C: ClosureWrapper = DefaultClosureWrapper> {
    chunk_size: usize,
    closure_wrapper: C,
    alpha: f32,
    beta: f32,
    seed: u64,
}

impl<C: ClosureWrapper + Default> Default for RandomPartitioner<C> {
    fn default() -> Self {
        Self {
            chunk_size: 0,
            closure_wrapper: C::default(),
            alpha: 0.01,
            beta: 0.5,
            seed: DEFAULT_SEED,
        }
    }
}

impl<C: ClosureWrapper> RandomPartitioner<C> {
    /// A random partitioner with the given chunk size (kept for parity with
    /// the other partitioners' constructors; [`DynamicLoop::loop_dynamic`]
    /// never reads it here) and the default `alpha` (`0.01`) and `beta`
    /// (`0.5`): upstream's `explicit RandomPartitioner(size_t)`.
    #[must_use]
    pub fn new(chunk_size: usize) -> Self
    where
        C: Default,
    {
        Self {
            chunk_size,
            closure_wrapper: C::default(),
            alpha: 0.01,
            beta: 0.5,
            seed: DEFAULT_SEED,
        }
    }

    /// The same, with an explicit closure wrapper: upstream's
    /// `explicit RandomPartitioner(size_t, C&&)`.
    #[must_use]
    pub fn with_closure_wrapper(chunk_size: usize, closure_wrapper: C) -> Self {
        Self {
            chunk_size,
            closure_wrapper,
            alpha: 0.01,
            beta: 0.5,
            seed: DEFAULT_SEED,
        }
    }

    /// A random partitioner with the given `alpha`/`beta` chunk-size
    /// fractions: upstream's `RandomPartitioner(float, float)`.
    #[must_use]
    pub fn with_alpha_beta(alpha: f32, beta: f32) -> Self
    where
        C: Default,
    {
        Self {
            chunk_size: 0,
            closure_wrapper: C::default(),
            alpha,
            beta,
            seed: DEFAULT_SEED,
        }
    }

    /// The same, with an explicit closure wrapper: upstream's
    /// `RandomPartitioner(float, float, C&&)`.
    #[must_use]
    pub fn with_alpha_beta_and_closure_wrapper(alpha: f32, beta: f32, closure_wrapper: C) -> Self {
        Self {
            chunk_size: 0,
            closure_wrapper,
            alpha,
            beta,
            seed: DEFAULT_SEED,
        }
    }

    /// The `alpha` fraction of `n * w` a drawn chunk size floors to: upstream's
    /// `alpha()`.
    #[must_use]
    pub fn alpha(&self) -> f32 {
        self.alpha
    }

    /// The `beta` fraction of `n * w` a drawn chunk size is capped to:
    /// upstream's `beta()`.
    #[must_use]
    pub fn beta(&self) -> f32 {
        self.beta
    }

    /// The seed [`DynamicLoop::loop_dynamic`] (re)builds a
    /// [`crate::Xorshift64`] from on every call: this port's replacement for
    /// upstream's per-call reseed from `std::random_device`. See the
    /// module's `# Design` section.
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Sets the seed.
    pub fn set_seed(&mut self, seed: u64) {
        self.seed = seed;
    }

    /// The inclusive range a chunk size is drawn from for `n` iterations over
    /// `w` workers: `floor(alpha * n * w)` and `floor(beta * n * w)`, swapped
    /// if out of order, the lower bound floored at `1` and the upper bound
    /// raised to be at least one past it. Upstream's `chunk_size_range`.
    #[must_use]
    pub fn chunk_size_range(&self, n: usize, w: usize) -> (usize, usize) {
        let nw = n as f32 * w as f32;

        // Unlike every other float-to-int conversion this module needed,
        // this one has no integer-arithmetic equivalent to fall back on:
        // `alpha` and `beta` are arbitrary, user-supplied tunables (upstream
        // takes the same `float` parameters), not a ratio of two integers
        // already in hand. `std` has no `TryFrom<f32> for usize` either, so
        // the cast below is the one conversion left. It is sound regardless
        // of what `alpha`/`beta` are: Rust's `as` from a float to an
        // integer is a *total*, saturating operation since 1.45 (`NaN` and
        // a negative product floor to `0`, anything past `usize::MAX`
        // saturates to it), unlike C++'s `static_cast`, which is undefined
        // outside the target type's range -- the lint is flagging a cast
        // that cannot be proven safe from its syntax alone, not one that is
        // unsound here.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let (mut b1, mut b2) = ((self.alpha * nw) as usize, (self.beta * nw) as usize);

        if b1 > b2 {
            std::mem::swap(&mut b1, &mut b2);
        }
        b1 = b1.max(1);
        b2 = b2.max(b1 + 1);
        (b1, b2)
    }
}

impl<C: ClosureWrapper> Partitioner for RandomPartitioner<C> {
    type Wrapper = C;
    const PARTITIONER_TYPE: PartitionerType = PartitionerType::Dynamic;

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn set_chunk_size(&mut self, chunk_size: usize) {
        self.chunk_size = chunk_size;
    }

    fn closure_wrapper(&self) -> &C {
        &self.closure_wrapper
    }

    fn closure_wrapper_mut(&mut self) -> &mut C {
        &mut self.closure_wrapper
    }
}

impl<C: ClosureWrapper> DynamicLoop for RandomPartitioner<C> {
    fn loop_dynamic_until(
        &self,
        n: usize,
        w: usize,
        next: &AtomicUsize,
        mut f: impl FnMut(usize, usize) -> bool,
    ) {
        let (lo, hi) = self.chunk_size_range(n, w);
        let mut rng = Xorshift64::new(self.seed);
        let mut chunk_size = draw_in_range(&mut rng, lo, hi);
        let mut curr_b = next.fetch_add(chunk_size, Ordering::Relaxed);
        while curr_b < n {
            if f(curr_b, (curr_b + chunk_size).min(n)) {
                return;
            }
            chunk_size = draw_in_range(&mut rng, lo, hi);
            curr_b = next.fetch_add(chunk_size, Ordering::Relaxed);
        }
    }
}

/// The default partitioner, matching upstream's choice: upstream's
/// `DefaultPartitioner`. A guided schedule gives decent, stable performance
/// across most workloads without a caller choosing a chunk size.
pub type DefaultPartitioner = GuidedPartitioner<DefaultClosureWrapper>;

/// Draws a value uniformly from the inclusive range `[lo, hi]`, matching
/// upstream's `std::uniform_int_distribution<size_t>(lo, hi)` closely enough
/// for a chunk-size draw: the modulo bias is negligible next to the chunk
/// sizes this module produces, and reproducibility -- not statistical
/// perfection -- is what [`RandomPartitioner`] needs to be tested.
fn draw_in_range(rng: &mut Xorshift64, lo: usize, hi: usize) -> usize {
    let span = hi - lo + 1;
    let draw = usize::try_from(rng.next_u64()).unwrap_or(usize::MAX);
    lo + draw % span
}

/// A partitioner whose chunk re-splits when it is stolen: a chunk task that
/// starts on a thread other than the one that spawned it hands half of its
/// remaining share to a new task before walking the rest, down to a floor of
/// one chunk size.
///
/// No upstream counterpart: the four partitioners above are
/// `taskflow/algorithm/partitioner.hpp`'s, and this one is the port's own.
/// The module's `# Design` section has the fan-out measurement it answers,
/// why a share's home is a [`std::thread::ThreadId`], why the chunk size is
/// read as a splitting floor, and what the default floor of one costs.
///
/// ```
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::Arc;
///
/// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, SplittingPartitioner};
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let visits = Arc::new(AtomicUsize::new(0));
/// let counted = Arc::clone(&visits);
///
/// let mut taskflow = Taskflow::new();
/// taskflow.for_each_index(
///     0_usize,
///     10_000,
///     1,
///     move |_| {
///         counted.fetch_add(1, Ordering::Relaxed);
///     },
///     // Every algorithm takes it where it takes one of the ported four.
///     SplittingPartitioner::<DefaultClosureWrapper>::new(64),
/// );
///
/// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(visits.load(Ordering::Relaxed), 10_000);
/// ```
#[derive(Clone, Debug)]
pub struct SplittingPartitioner<C: ClosureWrapper = DefaultClosureWrapper> {
    chunk_size: usize,
    closure_wrapper: C,
}

/// A floor of one index and one chunk-body call per share. The module's
/// `# Design` section has what that costs and why it is the default anyway.
impl<C: ClosureWrapper + Default> Default for SplittingPartitioner<C> {
    fn default() -> Self {
        Self {
            chunk_size: 0,
            closure_wrapper: C::default(),
        }
    }
}

impl<C: ClosureWrapper> SplittingPartitioner<C> {
    /// A splitting partitioner whose shares stop splitting at `chunk_size`
    /// and whose chunk body is called once per `chunk_size` indices (`0`
    /// meaning a floor of one index and one call per share), with a
    /// default-constructed closure wrapper.
    #[must_use]
    pub fn new(chunk_size: usize) -> Self
    where
        C: Default,
    {
        Self {
            chunk_size,
            closure_wrapper: C::default(),
        }
    }

    /// [`SplittingPartitioner::new`] with an explicit closure wrapper, which
    /// wraps each chunk task -- including each task a split created.
    ///
    /// ```
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    /// use std::sync::Arc;
    ///
    /// use rustflow::algorithm::partitioner::SplittingPartitioner;
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let tasks = Arc::new(AtomicUsize::new(0));
    /// let counted = Arc::clone(&tasks);
    /// let part = SplittingPartitioner::with_closure_wrapper(16, move |body: &mut dyn FnMut()| {
    ///     counted.fetch_add(1, Ordering::Relaxed);
    ///     body();
    /// });
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.for_each_index(0_usize, 1_000, 1, |_| {}, part);
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    ///
    /// // One per worker's own share, and one more for every share a steal
    /// // re-split -- which is how many there are is up to the run.
    /// assert!(tasks.load(Ordering::Relaxed) >= 4);
    /// ```
    #[must_use]
    pub fn with_closure_wrapper(chunk_size: usize, closure_wrapper: C) -> Self {
        Self {
            chunk_size,
            closure_wrapper,
        }
    }

    /// The size below which neither half of a split may fall: the configured
    /// chunk size, or `1` when none was configured.
    pub(crate) fn split_floor(&self) -> usize {
        self.chunk_size.max(1)
    }
}

impl<C: ClosureWrapper> Partitioner for SplittingPartitioner<C> {
    type Wrapper = C;
    /// Work is assigned while the loop runs rather than before it, which is
    /// the dynamic family's own property; the shared cursor is not, and the
    /// module's `# Design` section says why no loop trait fits.
    const PARTITIONER_TYPE: PartitionerType = PartitionerType::Dynamic;

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn set_chunk_size(&mut self, chunk_size: usize) {
        self.chunk_size = chunk_size;
    }

    fn closure_wrapper(&self) -> &C {
        &self.closure_wrapper
    }

    fn closure_wrapper_mut(&mut self) -> &mut C {
        &mut self.closure_wrapper
    }
}

/// One chunk task's own share of a parallel loop, as [`SplittingPartitioner`]
/// halves it: an index range for the index-driven algorithms, a `&mut [T]`
/// for the slice-driven ones.
pub(crate) trait SplitShare: Sized {
    /// How many indices, or elements, this share covers.
    fn size(&self) -> usize;

    /// Splits this share into its first `mid` and the rest. `mid` is always
    /// in `1..size()`.
    fn split(self, mid: usize) -> (Self, Self);
}

impl SplitShare for (usize, usize) {
    fn size(&self) -> usize {
        self.1 - self.0
    }

    fn split(self, mid: usize) -> (Self, Self) {
        let at = self.0 + mid;
        ((self.0, at), (at, self.1))
    }
}

impl<T> SplitShare for &mut [T] {
    fn size(&self) -> usize {
        // Spelled through the slice rather than as `self.len()`, which would
        // resolve back to this method on `&&mut [T]`.
        <[T]>::len(self)
    }

    fn split(self, mid: usize) -> (Self, Self) {
        self.split_at_mut(mid)
    }
}

/// Where a share of `size` splits, or `None` when either half would fall
/// below `floor` (itself read as at least `1`).
pub(crate) fn split_point(size: usize, floor: usize) -> Option<usize> {
    let smallest = floor.max(1).checked_mul(2)?;
    (size >= smallest).then_some(size / 2)
}

/// The contiguous shares `[0, n)` starts out as, one per worker, in index
/// order and never empty: the same spread
/// [`StaticPartitioner::adjusted_chunk_size`] computes, which is where a
/// splitting run begins rather than where it ends.
pub(crate) fn index_shares(n: usize, w: usize) -> Vec<(usize, usize)> {
    if w == 0 {
        return Vec::new();
    }
    let mut shares = Vec::with_capacity(w.min(n));
    let mut begin = 0usize;
    for i in 0..w {
        let size = n / w + usize::from(i < n % w);
        if size == 0 {
            break;
        }
        shares.push((begin, begin + size));
        begin += size;
    }
    shares
}

/// [`index_shares`]'s carve of a `&mut [T]`: the same contiguous spread, as
/// sub-slices rather than index pairs.
pub(crate) fn slice_shares<T>(data: &mut [T], w: usize) -> Vec<&mut [T]> {
    let sizes = index_shares(data.len(), w);
    let mut shares = Vec::with_capacity(sizes.len());
    let mut rest = data;
    for (b, e) in sizes {
        let (head, tail) = rest.split_at_mut(e - b);
        shares.push(head);
        rest = tail;
    }
    shares
}

/// Walks `[b, e)` in steps of `chunk_size`, calling `f` once per chunk and
/// stopping as soon as it returns `true`; a `chunk_size` of `0` hands the
/// whole range to one call. [`SplittingPartitioner`]'s own chunk loop, where
/// the ported families have [`StaticLoop`] and [`DynamicLoop`].
pub(crate) fn walk_chunks(
    b: usize,
    e: usize,
    chunk_size: usize,
    mut f: impl FnMut(usize, usize) -> bool,
) {
    if chunk_size == 0 {
        if b < e {
            f(b, e);
        }
        return;
    }
    let mut curr = b;
    while curr < e {
        let next = curr.saturating_add(chunk_size).min(e);
        if f(curr, next) {
            return;
        }
        curr = next;
    }
}

/// Runs one chunk task's `share`, splitting it first if this thread is not
/// `home` -- the thread the task was spawned on: the far half goes to a task
/// of its own under this task's node, joined before this call returns, and
/// `work` walks the near half here. `work` is called exactly once per share
/// the split leaves behind, and every index of `share` is covered by exactly
/// one of them.
pub(crate) fn run_share<'a, S, W>(
    rt: &mut Runtime<'_>,
    home: ThreadId,
    share: S,
    floor: usize,
    work: W,
) where
    S: SplitShare + Send + 'a,
    W: Fn(S) + Clone + Send + 'a,
{
    let here = thread::current().id();
    let mid = if here == home {
        None
    } else {
        split_point(share.size(), floor)
    };

    match mid {
        None => work(share),
        Some(mid) => {
            let (near, far) = share.split(mid);
            let far_work = work.clone();
            rt.scope(move |s: &Scope<'a, '_, '_>| {
                // `here`, not `home`: the far half's own home is wherever it
                // is being handed out from, so the worker that steals it is
                // the one that splits it again.
                s.spawn_runtime(move |rt: &mut Runtime<'_>| {
                    run_share(rt, here, far, floor, far_work);
                });
                work(near);
            });
        }
    }
}

/// Spawns one chunk task per share under `scope`, each recording this thread
/// as its home, and runs the last share inline: upstream's `(++w == W) ?
/// task() : rt.silent_async(task)` for [`SplittingPartitioner`]. The inline
/// share is already running on its home thread, so it never splits.
pub(crate) fn spawn_shares<'scope, S, W>(
    scope: &Scope<'scope, '_, '_>,
    shares: impl IntoIterator<Item = S>,
    floor: usize,
    work: W,
) where
    S: SplitShare + Send + 'scope,
    W: Fn(S) + Clone + Send + 'scope,
{
    let home = thread::current().id();
    let mut pending: Option<S> = None;
    for share in shares {
        if share.size() == 0 {
            continue;
        }
        if let Some(previous) = pending.replace(share) {
            let work = work.clone();
            scope.spawn_runtime(move |rt: &mut Runtime<'_>| {
                run_share(rt, home, previous, floor, work);
            });
        }
    }
    if let Some(last) = pending {
        work(last);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, Mutex, PoisonError};
    use std::time::Duration;

    use super::*;
    use crate::algorithm::AlgorithmBuilder;
    use crate::observer::{Observer, TaskView, WorkerView};
    use crate::{Executor, FlowBuilder, TaskType, Taskflow};

    /// Upstream's formula: the given chunk size when non-zero, or `n / w`
    /// with the `n % w` leftover iterations spread one each over the
    /// lowest-numbered workers.
    #[test]
    fn adjusted_chunk_size_spreads_the_remainder_over_the_first_workers() {
        let configured = StaticPartitioner::<DefaultClosureWrapper>::new(5);
        assert_eq!(configured.adjusted_chunk_size(100, 4, 0), 5);
        assert_eq!(configured.adjusted_chunk_size(100, 4, 3), 5);

        let auto = StaticPartitioner::<DefaultClosureWrapper>::new(0);
        // 10 iterations over 3 workers: 4, 3, 3.
        assert_eq!(auto.adjusted_chunk_size(10, 3, 0), 4);
        assert_eq!(auto.adjusted_chunk_size(10, 3, 1), 3);
        assert_eq!(auto.adjusted_chunk_size(10, 3, 2), 3);
    }

    /// `PartitionerType` separates the two families the loop traits are
    /// implemented against.
    #[test]
    fn each_concrete_partitioner_reports_its_family() {
        assert_eq!(
            StaticPartitioner::<DefaultClosureWrapper>::PARTITIONER_TYPE,
            PartitionerType::Static
        );
        assert_eq!(
            GuidedPartitioner::<DefaultClosureWrapper>::PARTITIONER_TYPE,
            PartitionerType::Dynamic
        );
        assert_eq!(
            DynamicPartitioner::<DefaultClosureWrapper>::PARTITIONER_TYPE,
            PartitionerType::Dynamic
        );
        assert_eq!(
            RandomPartitioner::<DefaultClosureWrapper>::PARTITIONER_TYPE,
            PartitionerType::Dynamic
        );
    }

    /// Upstream's formula, with `alpha > beta` swapped back into order and
    /// both bounds floored so the range is never empty.
    #[test]
    fn chunk_size_range_orders_and_floors_its_bounds() {
        let p = RandomPartitioner::<DefaultClosureWrapper>::with_alpha_beta(0.01, 0.5);
        let (lo, hi) = p.chunk_size_range(1000, 4);
        assert_eq!(lo, 40); // floor(0.01 * 1000 * 4)
        assert_eq!(hi, 2000); // floor(0.5 * 1000 * 4)
        assert!(lo < hi);

        // alpha > beta: the pair comes back the other way round, not
        // reversed.
        let swapped = RandomPartitioner::<DefaultClosureWrapper>::with_alpha_beta(0.5, 0.01);
        assert_eq!(swapped.chunk_size_range(1000, 4), (lo, hi));

        // n * w so small that both bounds floor to 0: the range still has to
        // hold at least one value, and a non-empty span.
        let tiny = RandomPartitioner::<DefaultClosureWrapper>::with_alpha_beta(0.01, 0.01);
        let (lo, hi) = tiny.chunk_size_range(1, 1);
        assert_eq!(lo, 1);
        assert!(hi > lo);
    }

    /// The identity wrapper runs the body exactly once and returns nothing
    /// around it.
    #[test]
    fn the_default_closure_wrapper_is_the_identity() {
        let mut calls = 0;
        DefaultClosureWrapper.invoke(|| calls += 1);
        assert_eq!(calls, 1);
    }

    /// A closure wrapper is free to run code on either side of the body, and
    /// to choose not to run the body at all.
    #[test]
    fn a_closure_wrapper_runs_around_the_body() {
        let trace = std::cell::RefCell::new(Vec::new());
        let wrapper = |body: &mut dyn FnMut()| {
            trace.borrow_mut().push("before");
            body();
            trace.borrow_mut().push("after");
        };

        wrapper.invoke(|| trace.borrow_mut().push("body"));
        assert_eq!(*trace.borrow(), vec!["before", "body", "after"]);

        let skip = |_body: &mut dyn FnMut()| {
            trace.borrow_mut().push("skipped");
        };
        skip.invoke(|| trace.borrow_mut().push("never"));
        assert_eq!(*trace.borrow(), vec!["before", "body", "after", "skipped"]);
    }

    /// A share stops splitting once either half would fall below the floor,
    /// and the floor is at least one index however the chunk size is set.
    #[test]
    fn a_share_splits_in_half_down_to_the_floor() {
        assert_eq!(split_point(64, 1), Some(32));
        assert_eq!(split_point(2, 1), Some(1));
        assert_eq!(split_point(1, 1), None);
        // A floor of 8 needs 16 to split, and the halves are 8 each.
        assert_eq!(split_point(16, 8), Some(8));
        assert_eq!(split_point(15, 8), None);
        // `0` is read as `1`, and a floor past half of `usize` can never be
        // doubled, so nothing splits rather than wrapping.
        assert_eq!(split_point(4, 0), Some(2));
        assert_eq!(split_point(usize::MAX, usize::MAX), None);
    }

    /// The shares a run starts from tile `[0, n)` in order, never empty, and
    /// stop at `n` when there are more workers than indices.
    #[test]
    fn the_starting_shares_tile_the_range() {
        assert_eq!(index_shares(10, 3), vec![(0, 4), (4, 7), (7, 10)]);
        assert_eq!(index_shares(2, 5), vec![(0, 1), (1, 2)]);
        assert_eq!(index_shares(0, 4), Vec::new());
        assert_eq!(index_shares(4, 0), Vec::new());

        let shares = index_shares(1000, 24);
        assert_eq!(shares.first(), Some(&(0, 42)));
        assert_eq!(shares.last().map(|&(_, e)| e), Some(1000));
        for pair in shares.windows(2) {
            assert_eq!(pair[0].1, pair[1].0);
        }
    }

    /// Runs `body` as the one runtime task of a fresh taskflow on a fresh
    /// `workers`-wide executor, the shape every case below that needs a real
    /// [`Runtime`] takes.
    fn in_runtime<F>(workers: usize, body: F)
    where
        F: FnMut(&mut Runtime<'_>) + Send + 'static,
    {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(body);
        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }

    /// The shares one [`run_share`] call ended up handing to its work
    /// closure, sorted. A `home` of `None` is the worker the call runs on --
    /// a task on its home thread -- which the test thread's own id is not.
    fn record_shares(
        home: Option<ThreadId>,
        share: (usize, usize),
        floor: usize,
    ) -> Vec<(usize, usize)> {
        let seen: Arc<Mutex<Vec<(usize, usize)>>> = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&seen);
        in_runtime(1, move |rt: &mut Runtime<'_>| {
            let recorded = Arc::clone(&recorded);
            let work = move |range: (usize, usize)| {
                recorded
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(range);
            };
            let home = home.unwrap_or_else(|| thread::current().id());
            run_share(rt, home, share, floor, work);
        });

        let mut got = std::mem::take(&mut *seen.lock().unwrap_or_else(PoisonError::into_inner));
        got.sort_unstable();
        got
    }

    /// An id no task will ever run on: a thread's own, read once and then
    /// joined. `std` does not reuse a `ThreadId`, so it stays foreign.
    fn a_foreign_thread_id() -> ThreadId {
        thread::spawn(|| thread::current().id())
            .join()
            .expect("the probe thread does not panic")
    }

    /// A chunk that starts where it was spawned walks its whole share: the
    /// half of the mechanism that keeps the extra tasks counted by steals
    /// rather than by the size of the range.
    #[test]
    fn a_share_running_on_its_home_thread_does_not_split() {
        assert_eq!(record_shares(None, (0, 64), 1), vec![(0, 64)]);
    }

    /// A chunk that starts anywhere else hands half of its share to a task
    /// of its own, and the two halves still cover the share exactly.
    #[test]
    fn a_share_running_on_a_foreign_thread_splits_once() {
        let foreign = Some(a_foreign_thread_id());
        assert_eq!(record_shares(foreign, (0, 64), 1), vec![(0, 32), (32, 64)]);
        // The far half's own home is wherever it was handed out from, so it
        // does not split again on the worker that is already running it.
        assert_eq!(record_shares(foreign, (0, 4), 1), vec![(0, 2), (2, 4)]);
        // Nothing splits below the floor, foreign thread or not.
        assert_eq!(record_shares(foreign, (0, 64), 64), vec![(0, 64)]);
    }

    /// Counts the chunk tasks a run spawns: an [`Observer`] sees one
    /// [`TaskType::Async`] entry per chunk task a partitioner hands to the
    /// pool, and none for the share that runs inline or for the runtime task
    /// driving them.
    #[derive(Default)]
    struct ChunkCounter {
        /// Entries seen so far.
        entries: AtomicUsize,
    }

    impl Observer for ChunkCounter {
        fn set_up(&self, _num_workers: usize) {}

        fn on_entry(&self, _worker: WorkerView<'_>, task: TaskView<'_>) {
            if task.task_type() == TaskType::Async {
                self.entries.fetch_add(1, Ordering::Relaxed);
            }
        }

        fn on_exit(&self, _worker: WorkerView<'_>, _task: TaskView<'_>) {}
    }

    /// Runs `visits` indices through `for_each_index` on a `workers`-wide
    /// executor under a [`SplittingPartitioner`], and reports how many chunk
    /// tasks the run spawned. `pause` is how long each index takes, which is
    /// what decides whether the worker holding the shares runs them itself
    /// or another worker steals one.
    fn count_chunk_tasks(workers: usize, visits: usize, pause: Duration) -> usize {
        let executor = Executor::new(workers);
        let counter = executor.make_observer(ChunkCounter::default());

        let seen = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&seen);
        let mut taskflow = Taskflow::new();
        taskflow.for_each_index(
            0_usize,
            visits,
            1,
            move |_| {
                counted.fetch_add(1, Ordering::Relaxed);
                thread::sleep(pause);
            },
            SplittingPartitioner::<DefaultClosureWrapper>::new(1),
        );
        executor.run(&taskflow).wait().expect("no task panicked");

        assert_eq!(
            seen.load(Ordering::Relaxed),
            visits,
            "every index is visited"
        );
        counter.entries.load(Ordering::Relaxed)
    }

    /// The property the whole type exists for: at 24 workers over a range
    /// too long for one worker to walk alone, the chunk tasks outnumber the
    /// workers, which only a share that was stolen and re-split can do --
    /// the four ported partitioners spawn exactly `w - 1`, one per worker
    /// bar the inline one, however the run goes.
    ///
    /// The sleep is the forcing: it keeps the worker that submitted the
    /// shares busy inside its own, so the other 23 reach theirs by stealing
    /// rather than by waiting for it.
    #[test]
    fn a_chunk_stolen_by_another_worker_splits() {
        const WORKERS: usize = 24;
        let spawned = count_chunk_tasks(WORKERS, WORKERS * 64, Duration::from_micros(100));
        assert!(
            spawned > WORKERS,
            "{spawned} chunk tasks at {WORKERS} workers: no share was stolen and re-split"
        );
    }

    /// One worker has nobody to steal from it, so nothing splits -- and at
    /// one worker the algorithms' own `w <= 1` short-circuit walks the range
    /// in the calling task, so no chunk task is spawned at all.
    #[test]
    fn one_worker_splits_nothing() {
        assert_eq!(count_chunk_tasks(1, 256, Duration::ZERO), 0);
    }
}

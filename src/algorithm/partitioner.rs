//! Translated from `taskflow/algorithm/partitioner.hpp` (Taskflow v4.1.0).
//!
//! A partitioner decides how a parallel loop over `[0, n)` is cut into
//! chunks and handed to workers. Four strategies ship here:
//! [`StaticPartitioner`] pre-assigns a strided share to each worker,
//! [`GuidedPartitioner`] (this module's [`DefaultPartitioner`]) and
//! [`DynamicPartitioner`] hand chunks out from a shared cursor -- guided
//! shrinking the chunk as the remaining work does, dynamic keeping it fixed
//! -- and [`RandomPartitioner`] draws the chunk size itself from a range.
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
//! **Omitted:** upstream's `PartitionerLike` concept and `is_partitioner_v`
//! check that a type derives from `PartitionerBase`. A `P: Partitioner`
//! bound already is that check; nothing else needs to restate it.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::Xorshift64;

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
        let p2 = 0.5_f32 / w as f32;
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
                let mut q = floor_to_usize(p2 * r as f32);
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
        let mut b1 = floor_to_usize(self.alpha * nw);
        let mut b2 = floor_to_usize(self.beta * nw);
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

/// Floors a `f32` into a `usize`, matching upstream's
/// `static_cast<size_t>(x)` at each of this module's call sites.
///
/// `std` has no `TryFrom<f32> for usize`, and the cast below is the
/// conversion upstream performs too -- except Rust's `as` from a float to an
/// integer is a *total*, saturating operation since 1.45 (`NaN` and a
/// negative input floor to `0`, anything past `usize::MAX` saturates to it),
/// unlike C++'s `static_cast`, which is undefined outside the target type's
/// range. That already is the floor every caller here wants, so the lint is
/// silenced once, at this single conversion, instead of reasoned about at
/// each call site.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn floor_to_usize(x: f32) -> usize {
    x as usize
}

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

#[cfg(test)]
mod tests {
    use super::*;

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
}

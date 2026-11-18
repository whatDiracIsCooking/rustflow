//! Translated from `taskflow/utility/iterator.hpp` (Taskflow v4.1.0).
//!
//! A one-dimensional index range is a half-open interval `[begin, end)`
//! walked in steps of `step_size`: the unit the index-driven algorithms
//! partition and hand out to workers one chunk at a time.
//! [`IndexRange<T>`] is that range; the two free functions below,
//! [`is_index_range_invalid`] and [`distance`], are the rule it is built on
//! and the element count the rule implies.
//!
//! [`IndexRanges<T, N>`] is the Cartesian product of `N` of these — upstream's
//! own general template, of which `IndexRange<T>` is the `N == 1` case
//! (`using IndexRange = IndexRanges<T, 1>`). This port keeps the two as
//! independent types rather than aliasing one to the other; see the
//! `# Design` section below for why.
//!
//! # Design
//!
//! - **A closed set of index types, not a single one.** Upstream's `T` is
//!   any `std::integral`; here [`IndexInt`] is implemented for the ten
//!   built-in integer primitives up to 64 bits wide (`i8`..`i64`, `isize`,
//!   `u8`..`u64`, `usize`) and is sealed against outside implementations, so
//!   [`IndexRange`] stays generic over "an index type this crate knows how
//!   to drive" rather than opening onto an arbitrary caller type whose
//!   arithmetic this module has never seen. 128-bit integers are left out:
//!   nothing an index range addresses needs them, and admitting them would
//!   mean widening every `usize` conversion below past what `usize` itself
//!   can hold.
//! - **No `as` casts.** Every place upstream writes a `static_cast` — `T` to
//!   `size_t` in `distance`, `size_t` to `T` in `discrete_domain` — this port
//!   writes a `TryFrom` and a documented `expect`, since a silent wrap on an
//!   index is exactly the bug the crate's cast lints exist to catch. A
//!   position outside the index type's own range is a programmer error, not
//!   a value this module tries to represent.
//! - **A guarded zero step.** Upstream's `distance` divides by `step`
//!   unconditionally, trusting the caller never to pass one that makes the
//!   range invalid; this port checks for a zero step itself and returns 0,
//!   so a bad call panics nowhere instead of dividing by zero. The returned
//!   value does not change either way: an invalid range already documents 0
//!   as its answer.
//! - **`discrete_domain`, not `unravel`.** Same mapping — a contiguous slice
//!   of this range's zero-based element positions back to the sub-range that
//!   covers exactly those elements — under the name that says what the
//!   result is rather than the operation performed to get it.
//! - **`reset` is the only mutator.** Upstream also exposes `begin(T)`,
//!   `end(T)` and `step_size(T)` as individually fluent setters overloaded
//!   against their own getters, which Rust has no way to name the same way;
//!   this port keeps the getters and the all-three-at-once `reset`, and
//!   leaves a per-field setter unported until something other than a test
//!   needs one.
//! - **`IndexRanges<T, N>` stays its own struct, not `IndexRange<T>` aliased
//!   to `N == 1`, and a const generic rather than a trait.** Upstream's
//!   one-dimensional-only members (`begin`, `end`, `step_size`, `reset`) are
//!   gated by a `requires (N == 1)` clause C++ checks per instantiation, so a
//!   single class body serves both shapes. Rust has no equivalent gate
//!   inside one generic-over-`N` impl block: a function generic over `N`
//!   type-checks every branch for every `N`, so it cannot call a method only
//!   `IndexRanges<T, 1>` has, the way `if constexpr` lets upstream's own
//!   `for_each_by_index` do. Aliasing the two here would still need two
//!   call-site-visible shapes wherever that distinction matters, for no
//!   simplification, so [`IndexRange<T>`] stays exactly the type it already
//!   was and [`IndexRanges<T, N>`] is its own const-generic sibling. A trait
//!   standing in for the `N == 1` / `N > 1` distinction was the other option
//!   this module could have taken; it would only pay off if a caller needed
//!   one call site generic over an `N` it does not already know at that
//!   site, which none here does, so the const generic is the whole answer.
//! - **`discrete_domain` on `IndexRanges<T, N>` only ever narrows dimension
//!   0.** A `[part_beg, part_end)` slice of the fully flattened, row-major
//!   space is a valid axis-aligned box only when it is a whole number of
//!   "rows" of the inner dimensions — cutting across a row boundary produces
//!   a staircase, not a rectangle, which is exactly why upstream's own
//!   N-dimensional story (`ceil`/`floor`/`upper_slice`/`lower_slice`) grows a
//!   box dimension by dimension under a "trailing zeros" rule instead of
//!   unraveling an arbitrary chunk. Those four methods partition any
//!   dimension and are out of this issue's scope; this port's
//!   `discrete_domain` instead keeps the 1-D method's own contract — the
//!   caller picks a valid `part_beg`/`part_end` — extended to say they must
//!   also be multiples of the product of every dimension after the first,
//!   so narrowing dimension 0 alone is always exact.
//! - **`size()` is the plain product, not upstream's zero-stops-accumulation
//!   rule.** Upstream's `size()` treats a zero-size dimension as stopping a
//!   nested loop's own dimension and everything inside it, while leaving
//!   outer dimensions unaffected — a rule `ceil`/`floor`/`upper_slice`/
//!   `lower_slice` need in order to know which dimensions are still
//!   "active". Without those methods this port has no caller for that
//!   distinction, so `size()` here is the straightforward product of every
//!   dimension's own count, zero if any one of them is.

use core::fmt;
use core::ops::{Add, Mul};

/// Closes [`IndexInt`] against implementations outside this module.
mod sealed {
    /// Marker a type must carry to implement [`super::IndexInt`].
    pub trait Sealed {}
}

/// An integer type [`IndexRange`] and its free helper functions can be built
/// over.
///
/// Implemented for `i8`, `i16`, `i32`, `i64`, `isize`, `u8`, `u16`, `u32`,
/// `u64` and `usize`, and sealed against any other implementation — see the
/// module's `# Design` section for why that set and not upstream's unbounded
/// `std::integral`. Most callers want [`distance`], [`is_index_range_invalid`]
/// or an [`IndexRange`] method instead of these associated items directly.
pub trait IndexInt:
    sealed::Sealed + Copy + Ord + fmt::Debug + Add<Output = Self> + Mul<Output = Self>
{
    /// This type's additive identity.
    const ZERO: Self;
    /// This type's multiplicative identity, used by [`distance`]'s
    /// ceiling/flooring adjustment.
    const ONE: Self;

    /// Type-specific core of [`distance`] for an already-known-nonzero
    /// `step`.
    fn raw_distance(beg: Self, end: Self, step: Self) -> usize;

    /// Converts a value already known to be non-negative to `usize`.
    fn to_usize_checked(self) -> usize;

    /// Converts a zero-based element position into this index type, for
    /// [`IndexRange::discrete_domain`].
    fn from_position(pos: usize) -> Self;
}

macro_rules! impl_unsigned_index_int {
    ($($t:ty),+ $(,)?) => {$(
        impl sealed::Sealed for $t {}

        impl IndexInt for $t {
            const ZERO: Self = 0;
            const ONE: Self = 1;

            fn raw_distance(beg: Self, end: Self, step: Self) -> usize {
                if end > beg {
                    // Ceiling division: (end - beg) steps of size `step`,
                    // rounded up.
                    ((end - beg + step - Self::ONE) / step).to_usize_checked()
                } else {
                    0
                }
            }

            fn to_usize_checked(self) -> usize {
                usize::try_from(self)
                    .expect("a validated range's distance fits in a usize on this target")
            }

            fn from_position(pos: usize) -> Self {
                Self::try_from(pos)
                    .expect("a discrete_domain position fits in this range's index type")
            }
        }
    )+};
}

macro_rules! impl_signed_index_int {
    ($($t:ty),+ $(,)?) => {$(
        impl sealed::Sealed for $t {}

        impl IndexInt for $t {
            const ZERO: Self = 0;
            const ONE: Self = 1;

            fn raw_distance(beg: Self, end: Self, step: Self) -> usize {
                // Ceiling division away from zero: a positive step rounds up,
                // a negative one rounds down, by nudging the numerator by
                // one before the truncating division.
                let adjust = if step > Self::ZERO { -Self::ONE } else { Self::ONE };
                let steps = (end - beg + step + adjust) / step;
                let steps = if steps > Self::ZERO { steps } else { Self::ZERO };
                steps.to_usize_checked()
            }

            fn to_usize_checked(self) -> usize {
                usize::try_from(self)
                    .expect("a validated range's distance fits in a usize on this target")
            }

            fn from_position(pos: usize) -> Self {
                Self::try_from(pos)
                    .expect("a discrete_domain position fits in this range's index type")
            }
        }
    )+};
}

impl_unsigned_index_int!(u8, u16, u32, u64, usize);
impl_signed_index_int!(i8, i16, i32, i64, isize);

/// Reports whether `(beg, end, step)` is not a usable index range.
///
/// Mirrors upstream's `is_index_range_invalid`: a range is invalid when its
/// step is zero while `beg != end`, when it runs forward (`beg < end`) on a
/// non-positive step, or backward (`beg > end`) on a non-negative step. An
/// empty range (`beg == end`) is valid for any step, including zero.
///
/// ```
/// use rustflow::algorithm::index_range::is_index_range_invalid;
///
/// assert!(!is_index_range_invalid(0, 10, 2));
/// assert!(!is_index_range_invalid(10, 0, -2));
/// assert!(is_index_range_invalid(0, 10, 0));
/// assert!(is_index_range_invalid(0, 10, -1));
/// ```
pub fn is_index_range_invalid<T: IndexInt>(beg: T, end: T, step: T) -> bool {
    (step == T::ZERO && beg != end)
        || (beg < end && step <= T::ZERO)
        || (beg > end && step >= T::ZERO)
}

/// Number of steps needed to walk from `beg` to `end` (exclusive) by `step`.
///
/// Mirrors upstream's `distance`. An invalid range (see
/// [`is_index_range_invalid`]) returns 0, including the zero-step case, which
/// this function checks for explicitly rather than dividing by it.
///
/// ```
/// use rustflow::algorithm::index_range::distance;
///
/// assert_eq!(distance(0, 10, 2), 5);
/// assert_eq!(distance(10, 0, -2), 5);
/// assert_eq!(distance(0, 10, 0), 0);
/// ```
pub fn distance<T: IndexInt>(beg: T, end: T, step: T) -> usize {
    if step == T::ZERO || is_index_range_invalid(beg, end, step) {
        return 0;
    }
    T::raw_distance(beg, end, step)
}

/// A one-dimensional half-open index range `[begin, end)` walked in steps of
/// `step_size`, upstream's `IndexRanges<T, 1>` (aliased there as
/// `IndexRange<T>`).
///
/// `step_size` may be negative, walking from `begin` down to `end`; see
/// [`is_index_range_invalid`] for which `(begin, end, step_size)`
/// combinations are usable — this type does not check that for itself, any
/// more than upstream's does.
///
/// ```
/// use rustflow::algorithm::index_range::IndexRange;
///
/// let r = IndexRange::new(0, 10, 2);
/// assert_eq!(r.size(), 5);
/// assert_eq!(r.discrete_domain(1, 4), IndexRange::new(2, 8, 2));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexRange<T: IndexInt> {
    begin: T,
    end: T,
    step_size: T,
}

impl<T: IndexInt> IndexRange<T> {
    /// Builds a range over `[begin, end)` stepped by `step_size`.
    pub fn new(begin: T, end: T, step_size: T) -> Self {
        Self {
            begin,
            end,
            step_size,
        }
    }

    /// The range's starting index.
    pub fn begin(&self) -> T {
        self.begin
    }

    /// The range's ending index (exclusive).
    pub fn end(&self) -> T {
        self.end
    }

    /// The step between consecutive indices of the range.
    pub fn step_size(&self) -> T {
        self.step_size
    }

    /// Rebinds the range to a new `(begin, end, step_size)` in place.
    ///
    /// Useful when the bounds are only known at run time, for example inside
    /// a task that initialises a range a later task iterates.
    pub fn reset(&mut self, begin: T, end: T, step_size: T) -> &mut Self {
        self.begin = begin;
        self.end = end;
        self.step_size = step_size;
        self
    }

    /// The number of elements in the range.
    pub fn size(&self) -> usize {
        distance(self.begin, self.end, self.step_size)
    }

    /// Maps a contiguous slice `[part_beg, part_end)` of this range's
    /// zero-based element positions back to the sub-range that covers
    /// exactly those elements (upstream's `unravel`).
    ///
    /// The caller must ensure `part_end <= self.size()`; this is the
    /// primitive a partitioner uses to turn a chunk of flat positions into
    /// the sub-range a worker iterates directly.
    pub fn discrete_domain(&self, part_beg: usize, part_end: usize) -> Self {
        let beg = self.begin + self.step_size * T::from_position(part_beg);
        let end = self.begin + self.step_size * T::from_position(part_end);
        Self::new(beg, end, self.step_size)
    }
}

/// The Cartesian product of `N` [`IndexRange<T>`]s, upstream's general
/// `IndexRanges<T, N>` template (of which `IndexRange<T>` is upstream's own
/// `N == 1` case, aliased there but not here — see the module's `# Design`
/// section for why).
///
/// Row-major: dimension `0` is outermost and varies slowest, dimension
/// `N - 1` innermost and fastest, matching the natural nesting of `for`
/// loops written outer-to-inner in that order. Dimensions are independent:
/// any combination of positive, negative or mixed step sizes across them is
/// fine, as long as each dimension's own `(begin, end, step)` is individually
/// valid (see [`is_index_range_invalid`]).
///
/// ```
/// use rustflow::algorithm::index_range::{IndexRange, IndexRanges};
///
/// // 3D: mixed step sizes, upstream's own `r3.size() == 120` example.
/// let r3 = IndexRanges::from_ranges([
///     IndexRange::new(0, 4, 1),   // dim 0: 4 elements
///     IndexRange::new(0, 10, 2),  // dim 1: 5 elements (0,2,4,6,8)
///     IndexRange::new(0, 6, 1),   // dim 2: 6 elements
/// ]);
/// assert_eq!(r3.size(), 120);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexRanges<T: IndexInt, const N: usize> {
    dims: [(T, T, T); N],
}

impl<T: IndexInt, const N: usize> IndexRanges<T, N> {
    /// This instantiation's dimensionality: upstream's `rank`.
    pub const RANK: usize = N;

    /// Builds an `N`-dimensional range from `N` one-dimensional ones, one per
    /// dimension in order from outermost (dimension 0) to innermost
    /// (dimension `N - 1`): upstream's constructor from `N` `IndexRange<T>`s.
    ///
    /// ```
    /// use rustflow::algorithm::index_range::{IndexRange, IndexRanges};
    ///
    /// let r = IndexRanges::from_ranges([IndexRange::new(0, 4, 1), IndexRange::new(0, 6, 1)]);
    /// assert_eq!(r.size(), 24);
    /// ```
    pub fn from_ranges(ranges: [IndexRange<T>; N]) -> Self {
        Self {
            dims: ranges.map(|r| (r.begin(), r.end(), r.step_size())),
        }
    }

    /// The `(begin, end, step_size)` tuple for dimension `d`: upstream's
    /// `dim(d) const`.
    ///
    /// # Panics
    ///
    /// If `d >= N`.
    pub fn dim(&self, d: usize) -> (T, T, T) {
        self.dims[d]
    }

    /// A mutable reference to the `(begin, end, step_size)` tuple for
    /// dimension `d`: upstream's `dim(d)`, used to rebind a stateful range's
    /// bounds at run time the way [`IndexRange::reset`] does for `N == 1`.
    ///
    /// # Panics
    ///
    /// If `d >= N`.
    pub fn dim_mut(&mut self, d: usize) -> &mut (T, T, T) {
        &mut self.dims[d]
    }

    /// The number of elements along dimension `d`, independent of every
    /// other dimension: upstream's `size(d)`.
    ///
    /// # Panics
    ///
    /// If `d >= N`.
    pub fn dim_size(&self, d: usize) -> usize {
        let (beg, end, step) = self.dims[d];
        distance(beg, end, step)
    }

    /// The total number of elements in the Cartesian product: the plain
    /// product of every dimension's own [`Self::dim_size`], zero if any one
    /// of them is. See the module's `# Design` section for how this differs
    /// from upstream's `size()`.
    ///
    /// ```
    /// use rustflow::algorithm::index_range::{IndexRange, IndexRanges};
    ///
    /// let r = IndexRanges::from_ranges([IndexRange::new(0, 4, 1), IndexRange::new(0, 0, 1)]);
    /// assert_eq!(r.size(), 0);
    /// ```
    pub fn size(&self) -> usize {
        (0..N).map(|d| self.dim_size(d)).product()
    }

    /// Maps a contiguous slice `[part_beg, part_end)` of this range's
    /// zero-based flattened element positions back to the sub-range that
    /// covers exactly those elements, narrowing dimension 0 and copying
    /// every other dimension through unchanged.
    ///
    /// The caller must ensure `part_end <= self.size()` (as [`IndexRange`]'s
    /// own `discrete_domain` already requires) and that `part_beg` and
    /// `part_end` are each a multiple of the product of every dimension's
    /// own [`Self::dim_size`] after dimension 0 — see the module's `# Design`
    /// section for why only a slice aligned that way is a valid rectangular
    /// sub-range.
    ///
    /// ```
    /// use rustflow::algorithm::index_range::{IndexRange, IndexRanges};
    ///
    /// let r = IndexRanges::from_ranges([IndexRange::new(0, 4, 1), IndexRange::new(0, 3, 1)]);
    /// // dim 1 has 3 elements per row of dim 0, so position 3 is row 1.
    /// let sub = r.discrete_domain(3, 6);
    /// assert_eq!(sub.dim(0), (1, 2, 1));
    /// assert_eq!(sub.dim(1), (0, 3, 1));
    /// ```
    pub fn discrete_domain(&self, part_beg: usize, part_end: usize) -> Self {
        let inner_volume = self.inner_volume();
        let outer_beg = part_beg.checked_div(inner_volume).unwrap_or(0);
        let outer_end = part_end.checked_div(inner_volume).unwrap_or(0);
        let (beg0, _end0, step0) = self.dims[0];
        let mut dims = self.dims;
        dims[0] = (
            beg0 + step0 * T::from_position(outer_beg),
            beg0 + step0 * T::from_position(outer_end),
            step0,
        );
        Self { dims }
    }

    /// The product of every dimension's own [`Self::dim_size`] after
    /// dimension 0 -- the number of flattened positions one step along
    /// dimension 0 covers, and the granularity [`Self::discrete_domain`]
    /// requires its arguments to respect.
    fn inner_volume(&self) -> usize {
        (1..N).map(|d| self.dim_size(d)).product()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_ranges_nd_from_ranges_and_dim_round_trip() {
        let r = IndexRanges::from_ranges([IndexRange::new(0, 4, 1), IndexRange::new(10, 0, -2)]);
        assert_eq!(IndexRanges::<i32, 2>::RANK, 2);
        assert_eq!(r.dim(0), (0, 4, 1));
        assert_eq!(r.dim(1), (10, 0, -2));
        assert_eq!(r.size(), 4 * 5);
    }

    #[test]
    fn index_ranges_nd_dim_mut_rebinds_in_place() {
        let mut r = IndexRanges::from_ranges([IndexRange::new(0, 4, 1), IndexRange::new(0, 5, 1)]);
        *r.dim_mut(1) = (0, 10, 2);
        assert_eq!(r.dim(1), (0, 10, 2));
        assert_eq!(r.size(), 4 * 5);
    }

    #[test]
    fn index_int_from_position_round_trips_through_to_usize_checked() {
        for pos in [0_usize, 1, 2, 100] {
            assert_eq!(i32::from_position(pos).to_usize_checked(), pos);
            assert_eq!(u32::from_position(pos).to_usize_checked(), pos);
            assert_eq!(i64::from_position(pos).to_usize_checked(), pos);
            assert_eq!(usize::from_position(pos).to_usize_checked(), pos);
        }
    }

    #[test]
    fn distance_never_divides_by_a_zero_step() {
        // beg == end with a zero step is a *valid* empty range (see
        // `is_index_range_invalid.empty_range` in `tests/index_range.rs`);
        // this exercises every signed/unsigned pairing of the guard that
        // keeps `distance` from dividing by that step anyway.
        assert_eq!(distance(5_i32, 5, 0), 0);
        assert_eq!(distance(5_u32, 5, 0), 0);
        assert_eq!(distance(0_i64, 0, 0), 0);
        assert_eq!(distance(0_usize, 0, 0), 0);
    }

    #[test]
    fn reset_rebinds_all_three_fields_and_chains() {
        let mut r = IndexRange::new(0, 10, 1);
        r.reset(5, 20, 3).reset(1, 2, 1);
        assert_eq!(r, IndexRange::new(1, 2, 1));
    }
}

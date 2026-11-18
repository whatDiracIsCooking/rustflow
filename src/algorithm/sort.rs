//! Translated from `taskflow/algorithm/sort.hpp` (Taskflow v4.1.0).
//!
//! [`Runtime::sort_by`] is upstream's `make_sort_task(b, e, cmp)`, called
//! directly rather than building a second closure -- [`for_each`]'s own
//! module docs give the reason, and it holds here unchanged. [`Runtime::sort`]
//! is the `T: Ord` overload upstream spells with no `cmp` argument at all,
//! and [`AlgorithmBuilder::sort_by`]/[`AlgorithmBuilder::sort`] are the
//! owned-data wrappers over both: upstream's `FlowBuilder::sort` overloads.
//!
//! [`for_each`]: crate::algorithm::for_each
//! [`AlgorithmBuilder::sort`]: crate::algorithm::AlgorithmBuilder::sort
//! [`AlgorithmBuilder::sort_by`]: crate::algorithm::AlgorithmBuilder::sort_by
//!
//! # Design
//!
//! **Two overloads become `sort` and `sort_by`.** Upstream names both
//! `make_sort_task`/`FlowBuilder::sort`, picked apart by whether a third,
//! comparator argument is given; Rust has no overloading on arity, so this
//! port follows `[T]::sort`/`[T]::sort_by`'s own split of exactly the same
//! two signatures rather than inventing a shape of its own.
//!
//! **The 3-way partition this issue asks for is not the one upstream's
//! `make_sort_task` runs.** The pinned header defines two sort cores side by
//! side -- `parallel_pdqsort` and this algorithm's own `parallel_3wqsort` --
//! but `make_sort_task` only ever calls the first; the call to
//! `parallel_3wqsort` is commented out. That leaves `parallel_3wqsort`'s own
//! pivot picker, `pseudo_median_of_nine`, calling a `sort3` helper that is
//! not defined anywhere in the pinned tree. Nothing upstream catches this:
//! a function template's body is not checked for a name two-phase lookup
//! cannot resolve until something instantiates the template, and nothing
//! does. This port implements the behaviour the name promises -- plain
//! median-of-three at or below `NINTHER_THRESHOLD`, Tukey's ninther above it
//! -- in exactly the shape `parallel_pdqsort`'s *own* pivot selection already
//! uses a few lines above the dead call: the one version of this choice the
//! pinned header does compile and run. `choose_pivot` is that shape,
//! standing in for the uninstantiable `pseudo_median_of_nine`.
//!
//! **Tail-recursion elimination survives as one [`Runtime::scope`] call per
//! stack frame, not a C++ `goto`.** Upstream's `while(true)` spawns the left
//! partition through `rt.silent_async` and loops back onto the right one in
//! the same frame, so only the left side ever grows the call stack.
//! `sort_range` keeps exactly that shape: one `scope` wraps the whole loop,
//! [`crate::Scope::spawn_runtime`] fires the left half into a fresh node
//! every time the loop spawns one, and the loop itself reassigns its own
//! `data` binding to the right half and continues -- so the join
//! [`Runtime::scope`] performs happens once per frame, after every left
//! child the loop ever spawned, exactly where upstream's implicit per-node
//! join falls.

use std::cmp::Ordering;
use std::mem::size_of;

use crate::Scope;
use crate::runtime::Runtime;

/// Below this many elements a partition is sorted serially rather than split
/// further: upstream's `parallel_sort_cutoff<I>()`, over `size_of::<T>()`
/// rather than an iterator's value type. Upstream special-cases
/// `std::string`; Rust's `String` has no equivalent layout quirk to guard
/// against, so the generic buckets below are all this port needs.
const fn parallel_sort_cutoff<T>() -> usize {
    let object_size = size_of::<T>();
    if object_size < 16 {
        4096
    } else if object_size < 32 {
        2048
    } else if object_size < 64 {
        1024
    } else if object_size < 128 {
        768
    } else if object_size < 256 {
        512
    } else if object_size < 512 {
        256
    } else {
        128
    }
}

/// Above this many elements, pivot selection widens from a plain
/// median-of-three to Tukey's ninther: `parallel_pdqsort`'s own
/// `ninther_threshold`, this module's `# Design` section says why it is this
/// module's pivot threshold too.
const NINTHER_THRESHOLD: usize = 128;

/// Returns whichever of `a`, `b`, `c` indexes the median value, without
/// moving anything: the three-comparison branch every median-of-three
/// implementation shares.
fn median3_index<T, C>(data: &[T], a: usize, b: usize, c: usize, cmp: &C) -> usize
where
    C: Fn(&T, &T) -> Ordering,
{
    if cmp(&data[a], &data[b]) == Ordering::Less {
        if cmp(&data[b], &data[c]) == Ordering::Less {
            b
        } else if cmp(&data[a], &data[c]) == Ordering::Less {
            c
        } else {
            a
        }
    } else if cmp(&data[a], &data[c]) == Ordering::Less {
        a
    } else if cmp(&data[b], &data[c]) == Ordering::Less {
        c
    } else {
        b
    }
}

/// Picks the pivot index for `data`'s next partition: a plain median-of-three
/// at or below [`NINTHER_THRESHOLD`], Tukey's ninther above it -- the module
/// docs' `# Design` section says why this, rather than upstream's own dead
/// `pseudo_median_of_nine`, is what this port runs. `data` must be non-empty.
///
/// [`parallel_sort_cutoff`]'s smallest bucket is exactly
/// [`NINTHER_THRESHOLD`], and [`sort_range`] only ever calls this once
/// `data.len()` has already cleared that cutoff -- so the plain
/// median-of-three branch below is never actually taken, in this port or in
/// `parallel_pdqsort`'s identical two-branch pivot selection upstream. It
/// stays, matching the shape upstream's own live code has.
fn choose_pivot<T, C>(data: &[T], cmp: &C) -> usize
where
    C: Fn(&T, &T) -> Ordering,
{
    let n = data.len();
    let mid = n / 2;
    if n > NINTHER_THRESHOLD {
        let eighth = n / 8;
        let quarter = n / 4;
        let low = median3_index(data, 0, eighth, quarter, cmp);
        let middle = median3_index(data, mid - eighth, mid, mid + eighth, cmp);
        let high = median3_index(data, n - 1 - quarter, n - 1 - eighth, n - 1, cmp);
        median3_index(data, low, middle, high, cmp)
    } else {
        median3_index(data, 0, mid, n - 1, cmp)
    }
}

/// Three-way-partitions `data` around `data[0]`: upstream's
/// `parallel_3wqsort`'s own inline loop. On return, every element less than
/// the pivot is in `data[..l)`, every element equal to it is in
/// `data[l..=r]`, and every element greater than it is in `data[r + 1..]`.
/// The two `bool`s report whether a less-than or a greater-than element was
/// ever found, upstream's `is_swapped_l`/`is_swapped_r`.
///
/// `data` must have at least two elements.
fn three_way_partition<T, C>(data: &mut [T], cmp: &C) -> (usize, usize, bool, bool)
where
    C: Fn(&T, &T) -> Ordering,
{
    let n = data.len();
    let mut l = 0usize;
    let mut r = n - 1;
    let mut f = 1usize;
    let mut found_less = false;
    let mut found_greater = false;

    while f <= r {
        if cmp(&data[f], &data[l]) == Ordering::Less {
            data.swap(l, f);
            found_less = true;
            l += 1;
            f += 1;
        } else if cmp(&data[l], &data[f]) == Ordering::Less {
            data.swap(r, f);
            found_greater = true;
            // Sound: the loop condition just above requires `f <= r`, and
            // neither bound has moved yet this iteration, so `r >= f >= 1`
            // and `r - 1` cannot underflow.
            r -= 1;
        } else {
            f += 1;
        }
    }

    // `f - l >= 1` is a loop invariant -- a "less" step moves both `l` and
    // `f` by one, a "greater" step moves neither, and the remaining branch
    // moves only `f` -- and the loop exits with `f == r + 1` exactly, since
    // each step changes `f` or `r` by at most one. Together: `l <= r`, so
    // `r - l` below never underflows.
    (l, r, found_less, found_greater)
}

/// Sorts `data` in place: upstream's `parallel_3wqsort(rt, first, last,
/// compare)`, over a half-open `&mut [T]` rather than a closed iterator
/// pair. Only called once `data.len()` is already known to exceed the
/// cutoff and there is more than one worker to spend on it --
/// [`Runtime::sort_by`]'s own short-circuit is the entry upstream's
/// `make_sort_task` runs first.
fn sort_range<T, C>(rt: &mut Runtime<'_>, data: &mut [T], cmp: &C)
where
    T: Send,
    C: Fn(&T, &T) -> Ordering + Sync,
{
    rt.scope(|s| sort_loop(s, data, cmp));
}

/// The body of [`sort_range`]'s `rt.scope`, pulled out to its own function
/// rather than left as a closure over `data`: a closure capturing `data` by
/// `move` ties every reassignment of it inside the loop to one fixed
/// lifetime fixed at the closure's creation, which is shorter than the
/// reborrow the last iteration needs; a plain function parameter with its
/// own named lifetime does not have that problem; this is the same
/// "iterative `&mut` reborrow" shape `std`'s own `split_first_mut`-driven
/// loops use.
fn sort_loop<'scope, T, C>(s: &Scope<'scope, '_, '_>, mut data: &'scope mut [T], cmp: &'scope C)
where
    T: Send,
    C: Fn(&T, &T) -> Ordering + Sync,
{
    let cutoff = parallel_sort_cutoff::<T>();

    loop {
        let n = data.len();
        if n <= cutoff {
            data.sort_unstable_by(cmp);
            return;
        }

        let pivot = choose_pivot(data, cmp);
        data.swap(0, pivot);

        let (l, r, found_less, found_greater) = three_way_partition(data, cmp);

        let (left, rest) = data.split_at_mut(l);
        if l > 1 && found_less {
            s.spawn_runtime(move |child: &mut Runtime<'_>| {
                sort_range(child, left, cmp);
            });
        }

        // `rest` starts at the pivot's final resting place (global index
        // `l`), so the greater-than section begins `r - l + 1` elements into
        // it -- `r >= l` is `three_way_partition`'s own invariant.
        let right_start = r - l + 1;
        let right_len = rest.len() - right_start;
        if right_len > 1 && found_greater {
            data = &mut rest[right_start..];
        } else {
            return;
        }
    }
}

impl<'rt> Runtime<'rt> {
    /// Sorts `data` in place using `cmp`: upstream's `make_sort_task(b, e,
    /// cmp)`.
    ///
    /// `data.len() <= 1` is trivially sorted. Otherwise, one worker or
    /// `data.len()` at or below this module's cutoff (its `# Design` section
    /// has the table) runs `data.sort_unstable_by(cmp)` directly, with no
    /// [`Runtime::scope`] at all -- upstream's own `W <= 1 || N <=
    /// parallel_sort_cutoff<...>()` short-circuit. Above both, the sort runs
    /// through `sort_range`'s parallel three-way partition.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let mut data = [5i32, 3, 1, 4, 1, 5, 9, 2, 6];
    ///     // Descending, to show `cmp` rather than `T`'s own order decides it.
    ///     runtime.sort_by(&mut data, |a: &i32, b: &i32| b.cmp(a));
    ///     assert_eq!(data, [9, 6, 5, 5, 4, 3, 2, 1, 1]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn sort_by<T, C>(&mut self, data: &mut [T], cmp: C)
    where
        T: Send,
        C: Fn(&T, &T) -> Ordering + Sync,
    {
        if data.len() <= 1 {
            return;
        }

        let cutoff = parallel_sort_cutoff::<T>();
        if self.num_workers() <= 1 || data.len() <= cutoff {
            data.sort_unstable_by(&cmp);
            return;
        }

        sort_range(self, data, &cmp);
    }

    /// Sorts `data` in place using `T`'s own order: upstream's
    /// `make_sort_task(beg, end)`, which plugs in `std::less<value_type>{}`
    /// -- [`Ord::cmp`], made explicit, here.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let mut data = [5i32, 3, 1, 4, 1, 5, 9, 2, 6];
    ///     runtime.sort(&mut data);
    ///     assert_eq!(data, [1, 1, 2, 3, 4, 5, 5, 6, 9]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn sort<T>(&mut self, data: &mut [T])
    where
        T: Ord + Send,
    {
        self.sort_by(data, T::cmp);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::prelude::*;
    use crate::{Executor, Runtime};

    /// Deterministic but scattered: not sorted, not reverse-sorted, with
    /// repeats -- a linear congruential generator rather than `rand`, so the
    /// sequence (and therefore which branch of [`three_way_partition`] each
    /// element takes) is the same on every run.
    fn scattered(n: usize, modulus: i64) -> Vec<i64> {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        (0..n)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let unsigned = (state >> 33) % u64::try_from(modulus).expect("modulus fits a u64");
                i64::try_from(unsigned).expect("modulus is small enough to fit an i64")
                    - modulus / 2
            })
            .collect()
    }

    /// `parallel_sort_cutoff`'s own table, against every boundary it names:
    /// `object_size`s one byte below a bucket edge, on it, and one above.
    #[test]
    fn parallel_sort_cutoff_matches_every_bucket() {
        assert_eq!(parallel_sort_cutoff::<[u8; 1]>(), 4096);
        assert_eq!(parallel_sort_cutoff::<[u8; 15]>(), 4096);
        assert_eq!(parallel_sort_cutoff::<[u8; 16]>(), 2048);
        assert_eq!(parallel_sort_cutoff::<[u8; 31]>(), 2048);
        assert_eq!(parallel_sort_cutoff::<[u8; 32]>(), 1024);
        assert_eq!(parallel_sort_cutoff::<[u8; 63]>(), 1024);
        assert_eq!(parallel_sort_cutoff::<[u8; 64]>(), 768);
        assert_eq!(parallel_sort_cutoff::<[u8; 127]>(), 768);
        assert_eq!(parallel_sort_cutoff::<[u8; 128]>(), 512);
        assert_eq!(parallel_sort_cutoff::<[u8; 255]>(), 512);
        assert_eq!(parallel_sort_cutoff::<[u8; 256]>(), 256);
        assert_eq!(parallel_sort_cutoff::<[u8; 511]>(), 256);
        assert_eq!(parallel_sort_cutoff::<[u8; 512]>(), 128);
        assert_eq!(parallel_sort_cutoff::<[u8; 4096]>(), 128);
    }

    /// Every ordering of three distinct values, plus a tie: the three
    /// comparisons [`median3_index`] branches on, each exercised both ways.
    #[test]
    fn median3_index_returns_the_middle_value() {
        let cmp = i64::cmp;
        for &(x, y, z) in &[
            (1i64, 2, 3),
            (1, 3, 2),
            (2, 1, 3),
            (2, 3, 1),
            (3, 1, 2),
            (3, 2, 1),
            (5, 5, 1),
            (5, 1, 5),
            (1, 5, 5),
        ] {
            let data = [x, y, z];
            let median = data[median3_index(&data, 0, 1, 2, &cmp)];
            let mut sorted = data;
            sorted.sort_unstable();
            assert_eq!(median, sorted[1], "data = {data:?}");
        }
    }

    /// [`three_way_partition`]'s three boundary shapes: every element less
    /// than the pivot, every element greater, and every element equal --
    /// each of which leaves one of its two `bool`s `false` and the
    /// corresponding section empty, per the function's own doc comment.
    #[test]
    fn three_way_partition_handles_every_element_on_one_side() {
        let cmp = i64::cmp;

        // Pivot (`5`) is the maximum: nothing is greater.
        let mut all_less = [5i64, 1, 2, 3, 4];
        let (l, r, found_less, found_greater) = three_way_partition(&mut all_less, &cmp);
        assert!(found_less);
        assert!(!found_greater);
        assert!(all_less[..l].iter().all(|&v| v < 5));
        assert!(all_less[l..=r].iter().all(|&v| v == 5));
        assert!(all_less[r + 1..].is_empty());

        // Pivot (`1`) is the minimum: nothing is less.
        let mut all_greater = [1i64, 5, 4, 3, 2];
        let (l, r, found_less, found_greater) = three_way_partition(&mut all_greater, &cmp);
        assert!(!found_less);
        assert!(found_greater);
        assert!(all_greater[..l].is_empty());
        assert!(all_greater[l..=r].iter().all(|&v| v == 1));
        assert!(all_greater[r + 1..].iter().all(|&v| v > 1));

        // Every element equals the pivot: the whole range is the middle
        // zone, and neither `bool` is set.
        let mut all_equal = [7i64; 6];
        let (l, r, found_less, found_greater) = three_way_partition(&mut all_equal, &cmp);
        assert!(!found_less);
        assert!(!found_greater);
        assert_eq!((l, r), (0, all_equal.len() - 1));

        // A mix, with ties on both sides of the pivot, is the shape the
        // public entry points exercise at scale; this is the hand-checkable
        // size of it.
        let mut mixed = [4i64, 2, 4, 6, 1, 4, 6, 3];
        let (l, r, found_less, found_greater) = three_way_partition(&mut mixed, &cmp);
        assert!(found_less);
        assert!(found_greater);
        assert!(mixed[..l].iter().all(|&v| v < 4));
        assert!(mixed[l..=r].iter().all(|&v| v == 4));
        assert!(mixed[r + 1..].iter().all(|&v| v > 4));
    }

    /// [`choose_pivot`] always returns an in-bounds index, below and above
    /// [`NINTHER_THRESHOLD`] -- [`sort_range`]'s own callers exercise the
    /// above-threshold case at scale (its doc comment says why the
    /// below-threshold branch is otherwise dead), so this is the one direct
    /// check of the plain median-of-three branch on its own.
    #[test]
    fn choose_pivot_stays_in_bounds_on_both_sides_of_the_ninther_threshold() {
        let cmp = i64::cmp;
        for &n in &[3, NINTHER_THRESHOLD, NINTHER_THRESHOLD + 1, 1000] {
            let data = scattered(n, 1000);
            let pivot = choose_pivot(&data, &cmp);
            assert!(pivot < n, "n = {n}");
        }
    }

    /// A smoke test over [`Runtime::sort`] and [`Runtime::sort_by`] through
    /// [`sort_range`]'s full recursive path, well above every bucket
    /// `parallel_sort_cutoff` can return: `tests/sort.rs` is upstream's own
    /// named cases and the worker matrix, this is one direct check that the
    /// two entry points wire to it at all.
    #[test]
    fn sort_and_sort_by_match_std_above_every_cutoff() {
        let raw = scattered(50_000, 1000);
        let mut expected = raw.clone();
        expected.sort_unstable();

        let sorted = Arc::new(Mutex::new(raw.clone()));
        let sorted_by = Arc::new(Mutex::new(raw));

        let mut taskflow = Taskflow::new();
        {
            let sorted = Arc::clone(&sorted);
            let sorted_by = Arc::clone(&sorted_by);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                runtime.sort(&mut sorted.lock().expect("uncontended"));
                runtime.sort_by(&mut sorted_by.lock().expect("uncontended"), i64::cmp);
            });
        }

        Executor::new(4)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(*sorted.lock().expect("uncontended"), expected);
        assert_eq!(*sorted_by.lock().expect("uncontended"), expected);
    }
}

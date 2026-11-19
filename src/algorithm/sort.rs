//! Translated from `taskflow/algorithm/sort.hpp` (Taskflow v4.1.0).
//!
//! [`Runtime::sort_by`] is upstream's `make_sort_task(b, e, cmp)` over a
//! borrowed slice, called directly rather than building a second closure --
//! [`for_each`]'s own module docs give the reason, and it holds here
//! unchanged. [`Runtime::sort`] is the `T: Ord` overload upstream spells
//! with no `cmp` argument at all; [`make_sort_by_task`] and
//! [`make_sort_task`] are those same two overloads over owned data; and
//! [`AlgorithmBuilder::sort_by`]/[`AlgorithmBuilder::sort`] are the one
//! line over each: upstream's `FlowBuilder::sort` overloads.
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
//! **The partition is `parallel_pdqsort`'s, and the three-way split this
//! port used to run is this port's departure from it.** The pinned header
//! defines two sort cores side by side -- `parallel_pdqsort` and
//! `parallel_3wqsort` -- and `make_sort_task` only ever calls the first;
//! the call to the second is commented out. A single-pivot three-way split
//! costs strictly more comparisons and swaps per level than a
//! pattern-defeating partition does on the same input, so what the loop
//! runs now is upstream's live core: `choose_pivot`'s ninther,
//! `partition_right` with the elements equal to the pivot going right,
//! `partition_left` instead when the pivot repeats the element before the
//! range, `insertion_sort` below a small threshold and again as the partial
//! pass that checks a partition which came back already partitioned,
//! `break_patterns`' swaps when one comes back badly unbalanced, and a
//! budget of such partitions past which the range is finished sequentially.
//! The sequential cutoff and the fork structure are the ones already
//! ported, unchanged.
//!
//! The rest of this section is where that core departs from a transcription
//! of upstream's.
//!
//! **`*(begin - 1)` becomes the first element of the slice.** Upstream
//! reads the element *before* the range twice: to find that the pivot
//! repeats it, so the block of elements equal to the pivot can be dropped
//! rather than recursed into, and to drop a bounds check from its insertion
//! sort. A `&mut [T]` cannot be indexed below zero, and the element cannot
//! be copied out either -- `T` need not be [`Clone`], and the suite's own
//! move-only case is not -- so a range that is not the leftmost one is
//! carried as a slice whose element `0` *is* that predecessor, with the
//! range itself starting at `1`. One split at the end of each frame hands
//! both halves what they need: the left child keeps this frame's
//! predecessor, and the right half's is the pivot the partition just
//! placed.
//!
//! **The pivot stays in the slice.** Upstream's `T pivot(std::move(*begin))`
//! both speeds up the comparisons against it and leaves a hole at `*begin`
//! to write into; moving a value out of a slice needs `unsafe` or [`Clone`]
//! in Rust, so every comparison reads element `0` in place and the pivot
//! reaches its final index by one [`<[T]>::swap`] rather than two moves.
//!
//! **Pivot selection picks an index instead of sorting the sample.**
//! Upstream's four `sort3` calls choose the ninther and leave the nine
//! sampled elements partly sorted on the way out; `choose_pivot` computes
//! the median index and its caller swaps that one element to the front,
//! which moves two elements rather than up to twelve and still leaves the
//! one thing `partition_right` needs of it -- an element not less than the
//! pivot somewhere above it, which is what makes its first scan stop in
//! bounds.
//!
//! **One insertion sort, with the move budget as an argument.** Upstream's
//! `insertion_sort` and `partial_insertion_sort` differ only in whether
//! they give up once eight elements have been shifted, so this port takes
//! that budget as a parameter rather than writing the loop twice. The shift
//! itself is [`<[T]>::rotate_right`] over the elements the new one passes,
//! where upstream moves the element into a local and walks the hole it
//! leaves down: one `memmove` either way, and the same comparisons in the
//! same order. `unguarded_insertion_sort` is not ported at all -- it is
//! that same loop minus one bounds check.
//!
//! **The guaranteed fallback is `[T]::sort_unstable_by`.** Once the budget
//! of badly unbalanced partitions runs out, upstream finishes the range
//! with `std::make_heap` and `std::sort_heap`, holding the worst case at
//! `O(n log n)`. `[T]::sort_unstable_by` documents that same worst case, so
//! this port spends the budget exactly where upstream spends it and lets
//! `std` be the floor under it, rather than carrying a hand-rolled heap
//! sort whose only caller is one branch.
//!
//! **Tail-recursion elimination survives as one [`Runtime::scope`] call per
//! stack frame, not a C++ `goto`.** Upstream's `while(true)` spawns the left
//! partition through `rt.silent_async` and loops back onto the right one in
//! the same frame, so only the left side ever grows the call stack. The loop
//! keeps exactly that shape: one `scope` wraps it, [`crate::Scope::spawn_runtime`]
//! fires the left half into a fresh node every time the loop spawns one, and
//! the loop itself reassigns its own `data` binding to the right half and
//! continues -- so the join [`Runtime::scope`] performs happens once per
//! frame, after every left child the loop ever spawned, exactly where
//! upstream's implicit per-node join falls. Upstream's two other loop-carried
//! values travel the same way: the `leftmost` flag and the remaining budget
//! of bad partitions are copied into each spawned child and updated in place
//! for the next turn of the loop.

use std::cmp::Ordering;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

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

/// Above this many elements pivot selection widens from a plain
/// median-of-three to Tukey's ninther, and [`break_patterns`] widens from
/// two swaps a side to six: upstream's `ninther_threshold`.
const NINTHER_THRESHOLD: usize = 128;

/// Below this many elements a range is insertion-sorted rather than
/// partitioned, and a side shorter than this is left out of
/// [`break_patterns`]' shuffle: upstream's `insertion_sort_threshold`.
const INSERTION_SORT_THRESHOLD: usize = 24;

/// How many elements the partial pass of [`insertion_sort`] may shift before
/// giving up: upstream's `partial_insertion_sort_limit`.
const PARTIAL_INSERTION_SORT_LIMIT: usize = 8;

/// Returns whichever of `a`, `b`, `c` indexes the median value, without
/// moving anything: the three-comparison branch every median-of-three
/// implementation shares.
///
/// Whichever index it returns, one of the other two holds an element not
/// less than that one -- the guarantee [`choose_pivot`] passes on to
/// [`partition_right`].
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
/// at or below [`NINTHER_THRESHOLD`], Tukey's ninther above it --
/// `parallel_pdqsort`'s own pivot selection, as an index rather than a
/// sorted sample for the reason the module's `# Design` section gives.
/// `data` must be non-empty.
///
/// The three finalists the last [`median3_index`] compares are at distinct
/// indices, so swapping the median it returns to the front can only move
/// its not-less-than witness to another index above the front:
/// [`partition_right`]'s precondition, established here.
///
/// [`parallel_sort_cutoff`]'s smallest bucket is exactly
/// [`NINTHER_THRESHOLD`], and [`sort_loop`] only ever calls this once
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

/// Insertion-sorts `data`, giving up and returning `false` once more than
/// `move_budget` elements have been shifted: upstream's
/// `partial_insertion_sort` at [`PARTIAL_INSERTION_SORT_LIMIT`], and its
/// plain `insertion_sort` at [`usize::MAX`], where the budget cannot run out
/// and the answer is always `true`. `data` is sorted when it returns `true`
/// and a permutation of what went in either way.
fn insertion_sort<T, C>(data: &mut [T], cmp: &C, move_budget: usize) -> bool
where
    C: Fn(&T, &T) -> Ordering,
{
    let mut shifted = 0usize;

    for cur in 1..data.len() {
        if shifted > move_budget {
            return false;
        }

        // Compared first so that an element already in place costs one
        // comparison and no move, as upstream's own loop does.
        if cmp(&data[cur], &data[cur - 1]) == Ordering::Less {
            let mut pos = cur - 1;
            while pos > 0 && cmp(&data[cur], &data[pos - 1]) == Ordering::Less {
                pos -= 1;
            }
            data[pos..=cur].rotate_right(1);
            shifted += cur - pos;
        }
    }

    true
}

/// Partitions `data` around the pivot at `data[0]`, elements equal to the
/// pivot going right: upstream's `partition_right`. Returns the pivot's
/// final index `p` -- `data[..p]` is then less than `data[p]` and
/// `data[p + 1..]` is not -- and whether `data` was already partitioned
/// that way on entry, upstream's `already_partitioned`.
///
/// Some element of `data[1..]` must be not less than `data[0]`, which is
/// what [`choose_pivot`] leaves behind and what stops the first scan below
/// in bounds. `data` must have at least two elements.
fn partition_right<T, C>(data: &mut [T], cmp: &C) -> (usize, bool)
where
    C: Fn(&T, &T) -> Ordering,
{
    // The first element not less than the pivot. The precondition is what
    // keeps this index inside `data`.
    let mut first = 1;
    while cmp(&data[first], &data[0]) == Ordering::Less {
        first += 1;
    }

    // The last element less than the pivot, searched leftwards. With
    // `first == 1` nothing below it is known to be less than the pivot, so
    // that search needs the bound upstream guards it with; above it,
    // `data[first - 1]` is itself less than the pivot and stops the scan.
    let mut last = data.len();
    if first == 1 {
        while first < last {
            last -= 1;
            if cmp(&data[last], &data[0]) == Ordering::Less {
                break;
            }
        }
    } else {
        loop {
            last -= 1;
            if cmp(&data[last], &data[0]) == Ordering::Less {
                break;
            }
        }
    }

    let already_partitioned = first >= last;

    // Each swap puts an element less than the pivot at `first` and one not
    // less than it at `last`, so the two scans that follow guard each other
    // and neither can leave `1..data.len()`.
    while first < last {
        data.swap(first, last);
        loop {
            first += 1;
            if cmp(&data[first], &data[0]) != Ordering::Less {
                break;
            }
        }
        loop {
            last -= 1;
            if cmp(&data[last], &data[0]) == Ordering::Less {
                break;
            }
        }
    }

    // `first >= 1` throughout, so the pivot's resting place is in bounds,
    // and the element sitting there is less than the pivot: one swap does
    // what upstream's two moves through its local copy do.
    let pivot_pos = first - 1;
    data.swap(0, pivot_pos);
    (pivot_pos, already_partitioned)
}

/// Partitions `data` around the pivot at `data[0]`, elements equal to the
/// pivot going *left*: upstream's `partition_left`. Returns the pivot's
/// final index `p`, with `data[..=p]` then not greater than `data[p]` and
/// `data[p + 1..]` greater than it.
///
/// `data` must have at least two elements, and nothing else: the pivot is
/// still in the slice, so the leftward scan below stops at element `0` at
/// the latest, where upstream's wants the pivot to be a median instead.
fn partition_left<T, C>(data: &mut [T], cmp: &C) -> usize
where
    C: Fn(&T, &T) -> Ordering,
{
    let n = data.len();

    // The last element not greater than the pivot.
    let mut last = n;
    loop {
        last -= 1;
        if cmp(&data[0], &data[last]) != Ordering::Less {
            break;
        }
    }

    // The first element greater than the pivot. With `last` at the end
    // nothing above it is known to be greater, so that search takes the
    // bound; otherwise `data[last + 1]` is greater and stops the scan.
    let mut first = 0;
    if last == n - 1 {
        while first < last {
            first += 1;
            if cmp(&data[0], &data[first]) == Ordering::Less {
                break;
            }
        }
    } else {
        loop {
            first += 1;
            if cmp(&data[0], &data[first]) == Ordering::Less {
                break;
            }
        }
    }

    // As in `partition_right`, each swap leaves the pair that guards the two
    // scans behind it, and `first >= 1` keeps both off the pivot.
    while first < last {
        data.swap(first, last);
        loop {
            last -= 1;
            if cmp(&data[0], &data[last]) != Ordering::Less {
                break;
            }
        }
        loop {
            first += 1;
            if cmp(&data[0], &data[first]) == Ordering::Less {
                break;
            }
        }
    }

    data.swap(0, last);
    last
}

/// Swaps a few elements on each side of the pivot at `data[pivot]` to break
/// the input pattern a badly unbalanced partition is evidence of: the
/// shuffle upstream runs inline under `highly_unbalanced`.
///
/// Every swap stays on one side of `data[pivot]` and the pivot itself does
/// not move, so the partition survives and the caller recurses into both
/// halves without redoing it. `pivot` must be an index of `data`.
fn break_patterns<T>(data: &mut [T], pivot: usize) {
    let n = data.len();
    let left_len = pivot;
    let right_len = n - pivot - 1;

    // Each `>= INSERTION_SORT_THRESHOLD` guard is what keeps the arithmetic
    // below in bounds: a quarter of a side that long is at least 6, so no
    // index reaches past the pivot or past either end, and the wider set of
    // swaps needs the far longer side `NINTHER_THRESHOLD` asks for.
    if left_len >= INSERTION_SORT_THRESHOLD {
        let quarter = left_len / 4;
        data.swap(0, quarter);
        data.swap(pivot - 1, pivot - quarter);
        if left_len > NINTHER_THRESHOLD {
            data.swap(1, quarter + 1);
            data.swap(2, quarter + 2);
            data.swap(pivot - 2, pivot - (quarter + 1));
            data.swap(pivot - 3, pivot - (quarter + 2));
        }
    }

    if right_len >= INSERTION_SORT_THRESHOLD {
        let quarter = right_len / 4;
        data.swap(pivot + 1, pivot + 1 + quarter);
        data.swap(n - 1, n - quarter);
        if right_len > NINTHER_THRESHOLD {
            data.swap(pivot + 2, pivot + 2 + quarter);
            data.swap(pivot + 3, pivot + 3 + quarter);
            data.swap(n - 2, n - (1 + quarter));
            data.swap(n - 3, n - (2 + quarter));
        }
    }
}

/// Sorts a range in place: upstream's `parallel_pdqsort(rt, begin, end,
/// comp, bad_allowed, leftmost)`, over a `&mut [T]` rather than an iterator
/// pair. Only called once the range is already known to exceed the cutoff
/// and there is more than one worker to spend on it --
/// [`Runtime::sort_by`]'s own short-circuit is the entry upstream's
/// `make_sort_task` runs first.
///
/// `data` is that range when `leftmost`; otherwise `data[0]` is the element
/// before it -- not greater than any of `data[1..]`, which is the range --
/// per the module's `# Design` section.
fn sort_range<T, C>(rt: &mut Runtime<'_>, data: &mut [T], leftmost: bool, bad_allowed: u32, cmp: &C)
where
    T: Send,
    C: Fn(&T, &T) -> Ordering + Sync,
{
    rt.scope(|s| sort_loop(s, data, leftmost, bad_allowed, cmp));
}

/// The body of [`sort_range`]'s `rt.scope`, pulled out to its own function
/// rather than left as a closure over `data`: a closure capturing `data` by
/// `move` ties every reassignment of it inside the loop to one fixed
/// lifetime fixed at the closure's creation, which is shorter than the
/// reborrow the last iteration needs; a plain function parameter with its
/// own named lifetime does not have that problem; this is the same
/// "iterative `&mut` reborrow" shape `std`'s own `split_first_mut`-driven
/// loops use.
///
/// `data`, `leftmost` and `bad_allowed` are [`sort_range`]'s own, with the
/// same meaning.
fn sort_loop<'scope, T, C>(
    s: &Scope<'scope, '_, '_>,
    mut data: &'scope mut [T],
    mut leftmost: bool,
    mut bad_allowed: u32,
    cmp: &'scope C,
) where
    T: Send,
    C: Fn(&T, &T) -> Ordering + Sync,
{
    let cutoff = parallel_sort_cutoff::<T>();

    loop {
        // Upstream's `begin`, as an index: `data[..begin]` is the
        // predecessor this frame was handed, empty on the leftmost range.
        let begin = usize::from(!leftmost);
        let n = data.len() - begin;

        if n < INSERTION_SORT_THRESHOLD {
            // Always `true` at this budget; the range is sorted.
            insertion_sort(&mut data[begin..], cmp, usize::MAX);
            return;
        }

        if n <= cutoff {
            data[begin..].sort_unstable_by(cmp);
            return;
        }

        let pivot = choose_pivot(&data[begin..], cmp);
        data.swap(begin, begin + pivot);

        // Upstream's `!leftmost && !comp(*(begin - 1), *begin)`: the pivot
        // is not greater than a predecessor that is not greater than
        // anything in the range, so every element equal to the pivot is a
        // sorted block this frame drops rather than recurses into.
        if !leftmost && cmp(&data[begin - 1], &data[begin]) != Ordering::Less {
            let pivot_pos = begin + partition_left(&mut data[begin..], cmp);
            data = &mut data[pivot_pos..];
            continue;
        }

        let (pivot_rel, already_partitioned) = partition_right(&mut data[begin..], cmp);
        let left_len = pivot_rel;
        let right_len = n - pivot_rel - 1;
        let pivot_pos = begin + pivot_rel;

        if left_len < n / 8 || right_len < n / 8 {
            // Upstream's `if (--bad_allowed == 0)`, which cannot underflow
            // here because the fallback is taken at one rather than at zero.
            if bad_allowed <= 1 {
                data[begin..].sort_unstable_by(cmp);
                return;
            }
            bad_allowed -= 1;
            break_patterns(&mut data[begin..], pivot_rel);
        } else if already_partitioned
            && insertion_sort(
                &mut data[begin..pivot_pos],
                cmp,
                PARTIAL_INSERTION_SORT_LIMIT,
            )
            && insertion_sort(
                &mut data[pivot_pos + 1..],
                cmp,
                PARTIAL_INSERTION_SORT_LIMIT,
            )
        {
            return;
        }

        // One split for both halves: `left` keeps this frame's predecessor
        // ahead of its own range, and `right` begins at the pivot, which is
        // the predecessor of the range the next turn of the loop sorts.
        let (left, right) = data.split_at_mut(pivot_pos);
        if left_len > 1 {
            s.spawn_runtime(move |child: &mut Runtime<'_>| {
                sort_range(child, left, leftmost, bad_allowed, cmp);
            });
        }
        data = right;
        leftmost = false;
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
    /// through the parallel pattern-defeating partition.
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

        // Upstream's `static_cast<int>(log2(size_t(end - beg)))`: how many
        // badly unbalanced partitions the recursion tolerates before it
        // falls back. The length cleared the cutoff above, so it is not 0.
        let bad_allowed = data.len().ilog2();

        sort_range(self, data, true, bad_allowed, &cmp);
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

/// Builds the runtime body of a [`Runtime::sort_by`] task over owned `data`:
/// upstream's `make_sort_task(b, e, cmp)`. `data` is locked once per run of
/// the body and sorted in place through that one guard.
///
/// `cmp` is cloned on every run of the body -- relevant only for a task a
/// cycle revisits -- matching upstream copying `cmp` into its own closure by
/// value.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::sort::make_sort_by_task;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let data = Arc::new(Mutex::new(vec![5i32, 3, 1, 4, 1, 5, 9, 2, 6]));
///
/// let mut taskflow = Taskflow::new();
/// // Descending, to show `cmp` rather than `T`'s own order decides it.
/// taskflow.emplace_runtime(make_sort_by_task(Arc::clone(&data), |a: &i32, b: &i32| {
///     b.cmp(a)
/// }));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(
///     *data.lock().expect("uncontended"),
///     vec![9, 6, 5, 5, 4, 3, 2, 1, 1]
/// );
/// ```
pub fn make_sort_by_task<T, C>(
    data: Arc<Mutex<Vec<T>>>,
    cmp: C,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Send + 'static,
    C: Fn(&T, &T) -> Ordering + Sync + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let mut guard = data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.sort_by(&mut guard, cmp.clone());
    }
}

/// Builds the runtime body of a [`Runtime::sort`] task over owned `data`:
/// upstream's `make_sort_task(beg, end)`, which plugs
/// `std::less<value_type>{}` into the comparator overload -- [`Ord::cmp`]
/// into [`make_sort_by_task`], here.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::sort::make_sort_task;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let data = Arc::new(Mutex::new(vec![5i32, 3, 1, 4, 1, 5, 9, 2, 6]));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_sort_task(Arc::clone(&data)));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(
///     *data.lock().expect("uncontended"),
///     vec![1, 1, 2, 3, 4, 5, 5, 6, 9]
/// );
/// ```
pub fn make_sort_task<T>(
    data: Arc<Mutex<Vec<T>>>,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Ord + Send + 'static,
{
    make_sort_by_task(data, T::cmp)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::prelude::*;
    use crate::{Executor, Runtime};

    /// Deterministic but scattered: not sorted, not reverse-sorted, with
    /// repeats -- a linear congruential generator rather than `rand`, so the
    /// sequence (and therefore which scan of [`partition_right`] each
    /// element stops) is the same on every run.
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
    /// comparisons [`median3_index`] branches on, each exercised both ways,
    /// and the not-less-than witness it promises in every one of them.
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
            let median_index = median3_index(&data, 0, 1, 2, &cmp);
            let mut sorted = data;
            sorted.sort_unstable();
            assert_eq!(data[median_index], sorted[1], "data = {data:?}");
            assert!(
                (0..3).any(|i| i != median_index && data[i] >= data[median_index]),
                "data = {data:?}"
            );
        }
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

    /// Checks [`partition_right`]'s postcondition and returns the pivot
    /// value, so that each case below states only what is specific to it.
    fn check_partition_right(data: &mut [i64], expected_pivot: i64) -> (usize, bool) {
        let before = {
            let mut sorted = data.to_vec();
            sorted.sort_unstable();
            sorted
        };

        let (pivot_pos, already_partitioned) = partition_right(data, &i64::cmp);

        assert_eq!(data[pivot_pos], expected_pivot);
        assert!(data[..pivot_pos].iter().all(|&v| v < expected_pivot));
        assert!(data[pivot_pos + 1..].iter().all(|&v| v >= expected_pivot));

        let mut after = data.to_vec();
        after.sort_unstable();
        assert_eq!(after, before);

        (pivot_pos, already_partitioned)
    }

    /// [`partition_right`] over the shapes its two scans and its
    /// `already_partitioned` answer turn on: a range already split that way,
    /// one that is not, a pivot that is the minimum (nothing goes left) and
    /// a range of one repeated value. Each case puts a median at `data[0]`
    /// first, which is the precondition the function states.
    #[test]
    fn partition_right_splits_around_the_pivot() {
        // Already partitioned: `4` is the median of `4`, `1` and `5`, and
        // everything below it already precedes everything above.
        let (pivot_pos, already_partitioned) =
            check_partition_right(&mut [4i64, 1, 2, 3, 9, 5, 7], 4);
        assert_eq!(pivot_pos, 3);
        assert!(already_partitioned);

        // Not partitioned: the same values, interleaved.
        let (pivot_pos, already_partitioned) =
            check_partition_right(&mut [4i64, 9, 1, 7, 2, 5, 3], 4);
        assert_eq!(pivot_pos, 3);
        assert!(!already_partitioned);

        // The pivot is the minimum: nothing is less than it, so it stays at
        // the front and the whole rest of the range is the right side.
        let (pivot_pos, already_partitioned) =
            check_partition_right(&mut [1i64, 9, 4, 7, 2, 5, 3], 1);
        assert_eq!(pivot_pos, 0);
        assert!(already_partitioned);

        // Every element equal to the pivot: equal goes right, so this is the
        // minimum case again, and it is the shape `partition_left` exists to
        // stop the recursion repeating.
        let (pivot_pos, already_partitioned) = check_partition_right(&mut [7i64; 6], 7);
        assert_eq!(pivot_pos, 0);
        assert!(already_partitioned);

        // Ties on both sides of the pivot, the shape the public entry points
        // meet at scale, at a hand-checkable size.
        let (pivot_pos, _) = check_partition_right(&mut [4i64, 2, 4, 6, 1, 4, 6, 3], 4);
        assert_eq!(pivot_pos, 3);
    }

    /// [`partition_left`] over the two shapes its guarded first scan turns
    /// on -- an element not greater than the pivot at the very end of the
    /// range, and one further in -- plus the all-equal range that is the
    /// reason upstream has this partition at all. Equal elements go left,
    /// so the pivot's resting place is the *last* of them.
    #[test]
    fn partition_left_puts_the_equal_elements_on_the_left() {
        let cmp = i64::cmp;

        // `2` repeats, with greater elements interleaved: the equal block
        // ends up below the pivot's index and nothing equal is left above.
        let mut interleaved = [2i64, 2, 2, 5, 2, 7];
        let pivot_pos = partition_left(&mut interleaved, &cmp);
        assert!(interleaved[..=pivot_pos].iter().all(|&v| v == 2));
        assert!(interleaved[pivot_pos + 1..].iter().all(|&v| v > 2));
        assert_eq!(pivot_pos, 3);

        // The last element is itself equal to the pivot, which is the case
        // upstream guards the first scan for.
        let mut equal_at_the_end = [2i64, 9, 2, 5, 2, 2];
        let pivot_pos = partition_left(&mut equal_at_the_end, &cmp);
        assert!(equal_at_the_end[..=pivot_pos].iter().all(|&v| v == 2));
        assert!(equal_at_the_end[pivot_pos + 1..].iter().all(|&v| v > 2));
        assert_eq!(pivot_pos, 3);

        // All equal: the whole range is the left side, and the frame that
        // called this has nothing left to recurse into.
        let mut all_equal = [4i64; 5];
        let pivot_pos = partition_left(&mut all_equal, &cmp);
        assert_eq!(pivot_pos, all_equal.len() - 1);
    }

    /// [`insertion_sort`] at both budgets: unlimited, where it matches
    /// `std`; at [`PARTIAL_INSERTION_SORT_LIMIT`] over a range one adjacent
    /// swap away from sorted, where it stays inside the budget and sorts;
    /// and at that budget over a reversed range, where it gives up and
    /// leaves a permutation of the input behind.
    #[test]
    fn insertion_sort_sorts_within_its_budget_and_gives_up_past_it() {
        let cmp = i64::cmp;

        let mut unlimited = scattered(40, 100);
        let mut expected = unlimited.clone();
        expected.sort_unstable();
        assert!(insertion_sort(&mut unlimited, &cmp, usize::MAX));
        assert_eq!(unlimited, expected);

        let mut nearly_sorted: Vec<i64> = (0..40).collect();
        nearly_sorted.swap(10, 11);
        assert!(insertion_sort(
            &mut nearly_sorted,
            &cmp,
            PARTIAL_INSERTION_SORT_LIMIT
        ));
        assert_eq!(nearly_sorted, (0..40).collect::<Vec<i64>>());

        let mut reversed: Vec<i64> = (0..40).rev().collect();
        assert!(!insertion_sort(
            &mut reversed,
            &cmp,
            PARTIAL_INSERTION_SORT_LIMIT
        ));
        let mut recovered = reversed;
        recovered.sort_unstable();
        assert_eq!(recovered, (0..40).collect::<Vec<i64>>());
    }

    /// [`break_patterns`] over every branch of its index arithmetic -- a
    /// side too short to shuffle, one long enough for two swaps, one long
    /// enough for six -- checking what the function promises its caller: the
    /// pivot does not move, no element crosses it, and a side that is long
    /// enough really is shuffled.
    #[test]
    fn break_patterns_shuffles_each_side_without_crossing_the_pivot() {
        for &(n, pivot) in &[
            (20usize, 10usize),
            (50, 10),
            (50, 40),
            (100, 50),
            (400, 200),
        ] {
            let original: Vec<i64> = (0..n)
                .map(|v| i64::try_from(v).expect("n is small"))
                .collect();
            let mut data = original.clone();
            break_patterns(&mut data, pivot);

            let boundary = i64::try_from(pivot).expect("n is small");
            assert_eq!(data[pivot], boundary, "(n, pivot) = ({n}, {pivot})");
            assert!(
                data[..pivot].iter().all(|&v| v < boundary),
                "(n, pivot) = ({n}, {pivot})"
            );
            assert!(
                data[pivot + 1..].iter().all(|&v| v > boundary),
                "(n, pivot) = ({n}, {pivot})"
            );

            let mut recovered = data.clone();
            recovered.sort_unstable();
            assert_eq!(recovered, original, "(n, pivot) = ({n}, {pivot})");

            let left_len = pivot;
            let right_len = n - pivot - 1;
            let shuffled = data != original;
            assert_eq!(
                shuffled,
                left_len >= INSERTION_SORT_THRESHOLD || right_len >= INSERTION_SORT_THRESHOLD,
                "(n, pivot) = ({n}, {pivot})"
            );
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

    /// The four input shapes a pattern-defeating quicksort exists for, each
    /// through the parallel path at a length that recurses several levels
    /// deep: an already sorted range, a reversed one, one of three distinct
    /// values -- which is what sends the recursion through
    /// [`partition_left`] -- and an organ pipe (#375).
    #[test]
    fn sort_matches_std_on_the_patterns_the_partition_defeats() {
        let n = 50_000usize;
        let index = |i: usize| i64::try_from(i).expect("n is small");
        let patterns: [(&str, Vec<i64>); 4] = [
            ("sorted", (0..n).map(index).collect()),
            ("reversed", (0..n).rev().map(index).collect()),
            ("few distinct", (0..n).map(|i| index(i % 3)).collect()),
            (
                "organ pipe",
                (0..n).map(|i| index(i.min(n - 1 - i))).collect(),
            ),
        ];

        for (name, raw) in patterns {
            let mut expected = raw.clone();
            expected.sort_unstable();

            let data = Arc::new(Mutex::new(raw));
            let mut taskflow = Taskflow::new();
            {
                let data = Arc::clone(&data);
                taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                    runtime.sort(&mut data.lock().expect("uncontended"));
                });
            }

            Executor::new(4)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");

            assert_eq!(*data.lock().expect("uncontended"), expected, "{name}");
        }
    }

    /// The pattern defeat itself, as a comparison count rather than a time:
    /// an already sorted range costs one partition plus the partial
    /// insertion sort over each of its halves, which is linear in the length
    /// and spawns nothing, where falling through to the recursion would
    /// descend to the cutoff and compare `n log n` times (#375).
    #[test]
    fn an_already_sorted_range_costs_a_linear_number_of_comparisons() {
        let n = 50_000usize;
        let raw: Vec<i64> = (0..n)
            .map(|i| i64::try_from(i).expect("n is small"))
            .collect();

        let comparisons = Arc::new(AtomicUsize::new(0));
        let data = Arc::new(Mutex::new(raw));

        let mut taskflow = Taskflow::new();
        {
            let comparisons = Arc::clone(&comparisons);
            let data = Arc::clone(&data);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let counted = |a: &i64, b: &i64| {
                    comparisons.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    a.cmp(b)
                };
                runtime.sort_by(&mut data.lock().expect("uncontended"), counted);
            });
        }

        Executor::new(4)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        let counted = comparisons.load(std::sync::atomic::Ordering::Relaxed);
        assert!(counted < 4 * n, "{counted} comparisons for {n} elements");
        assert!(data.lock().expect("uncontended").is_sorted());
    }
}

//! Translated from `taskflow/algorithm/merge.hpp` (Taskflow v4.1.0).
//!
//! A parallel merge of two sorted slices into a third: upstream's
//! `make_merge_task`, `tf::Runtime::merge` and the two `FlowBuilder::merge`
//! overloads, over a comparator or `T`'s own [`Ord`].
//!
//! Upstream's algorithm is *co-rank*, not a recursive divide at the longer
//! input's midpoint: the merged output of length `N = a.len() + b.len()` is
//! split into `W` equal contiguous chunks, one per worker. For each chunk
//! boundary `rank`, a binary search -- `co_rank` -- finds how many of the
//! first `rank` merged elements came from `a` (call it `i`) and how many
//! from `b` (`j = rank - i`), by searching for the `i` at which
//! `a[i - 1] <= b[j]` and `b[j - 1] <= a[i]` both hold; both sequences being
//! sorted is what makes that condition monotonic in `i`. Once a chunk's
//! `(i, j)` bounds on both sides are known, its slice of `a`, its slice of
//! `b` and its slice of `out` are independent of every other chunk's, so
//! each chunk's own serial merge runs with no synchronization at all beyond
//! the join at the end. A chunk of size `K` always costs `O(K)` regardless
//! of where the split fell, so there is no load imbalance a partitioner
//! could do anything about -- upstream's own "why no partitioner" --
//! and this port takes none either.
//!
//! # Design
//!
//! - **`Runtime::scope`, not a naked `silent_async`.** Upstream's lambda
//!   spawns `W - 1` chunks with `rt.silent_async` and runs the last inline,
//!   relying on the enclosing runtime node not finishing until every
//!   `silent_async` child does. [`Runtime::scope`](crate::Runtime::scope)
//!   is this port's replacement for that reliance (its own module docs say
//!   why), and it buys back something upstream's shape cannot type-check in
//!   Rust: each chunk's closure borrows its slice of `a`, `b` and `out`
//!   straight from the caller's stack frame, through `split_at_mut` on
//!   `out`, rather than needing every input behind an `Arc`.
//! - **`_by` suffixes, not overloading.** Upstream names both the
//!   default-ordered and custom-comparator forms `merge` and lets overload
//!   resolution pick; Rust has no such resolution, so the comparator form is
//!   named `merge_by` throughout -- [`Runtime::merge_by`], the free function
//!   [`make_merge_task_by`] and the [`AlgorithmBuilder::merge_by`] wrapper --
//!   the same split [`<[T]>::sort`] and [`<[T]>::sort_by`] use, and every
//!   `Ord`-only form calls straight through to its `_by` sibling with
//!   `|x, y| x < y`, upstream's own `std::less<>`.
//! - **`Clone`, not `Copy` or a move.** A chunk's merge reads `a[i]` or
//!   `b[j]` and writes the smaller into `out`, same as upstream's
//!   `std::merge`; `a` and `b` are shared slices here; there is no way to
//!   move an element out of one, so every writer-side element is a `.clone()`
//!   of whichever input held it, exactly upstream's requirement that `O`
//!   `OutputIterator`'s value type be copy-assignable.
//! - **A result the owned forms publish through a shared slot, not a
//!   `Task`.** Upstream's own `FlowBuilder::merge` overloads take an output
//!   iterator the caller already owns storage behind, and return nothing but
//!   a `Task` -- fitting C++'s pointer-like iterators, where the caller's
//!   storage already exists and outlives the call. [`make_merge_task`] and
//!   [`AlgorithmBuilder::merge`] take owned `Vec<T>` inputs instead, because
//!   a graph node's body must be `Send + 'static`, so the inputs cannot stay
//!   borrowed from the caller's frame the way [`Runtime::merge`]'s can; with
//!   no caller-owned destination to write into, each publishes its merged
//!   `Vec<T>` through an `Arc<Mutex<Vec<T>>>` returned alongside the
//!   [`Task`], a shape this port invents and upstream has no equivalent of. The mutex is locked only once per run, after the
//!   parallel merge itself has already finished, so it carries none of the
//!   per-element contention `Runtime::merge`'s `split_at_mut` chunks exist to
//!   avoid.
//!
//! `tests/merge.rs` is `unittests/test_merge.cpp` translated: the serial
//! cutoff, both input orderings, the custom-comparator case and empty
//! inputs, each checked against a `std`-sorted concatenation, over the
//! worker matrix.
//!
//! [`AlgorithmBuilder::merge`]: crate::algorithm::AlgorithmBuilder::merge
//! [`AlgorithmBuilder::merge_by`]: crate::algorithm::AlgorithmBuilder::merge_by
//! [`Task`]: crate::Task

use std::sync::{Arc, Mutex, PoisonError};

use crate::algorithm::SliceSource;
use crate::runtime::Runtime;

// Upstream's inner `std::merge` call, over a comparator rather than its own
// overload set: the serial merge each chunk -- or the whole input, below the
// worker-count cutoff -- runs with no further splitting. `a[i]`/`b[j]` are
// `.clone()`d rather than moved, since both are shared slices here; the
// module docs' `# Design` section says why `Clone` rather than `Copy`.
fn merge_serial<T: Clone>(a: &[T], b: &[T], out: &mut [T], cmp: &impl Fn(&T, &T) -> bool) {
    debug_assert_eq!(out.len(), a.len() + b.len());
    let mut i = 0_usize;
    let mut j = 0_usize;
    for slot in out.iter_mut() {
        if j >= b.len() || (i < a.len() && !cmp(&b[j], &a[i])) {
            *slot = a[i].clone();
            i += 1;
        } else {
            *slot = b[j].clone();
            j += 1;
        }
    }
}

/// `co_rank`: for `rank` output positions, how many came from `a` (the
/// return's first element) and how many from `b` (its second) -- upstream's
/// `co_rank` lambda, binary-searching `a`'s index range for the split that
/// makes both `a[i - 1] <= b[j]` and `b[j - 1] <= a[i]` hold, skipping the
/// `b[j - 1]` read when `j == 0` the same way upstream's guard against the
/// `seq2[-1]` dereference does.
fn co_rank<T>(a: &[T], b: &[T], cmp: &impl Fn(&T, &T) -> bool, rank: usize) -> (usize, usize) {
    let mut low = rank.saturating_sub(b.len());
    let mut high = a.len().min(rank);

    while low < high {
        let i = low + (high - low) / 2;
        let j = rank - i;

        if j == 0 || !cmp(&b[j - 1], &a[i]) {
            low = i + 1;
        } else {
            high = i;
        }
    }
    (low, rank - low)
}

impl<'rt> Runtime<'rt> {
    /// Merges sorted `a` and `b` into `out`, by `T`'s own [`Ord`]: upstream's
    /// `std::less<>` overload of `Runtime::merge`. Calls
    /// [`Runtime::merge_by`] with `|x, y| x < y`; see there for a custom
    /// comparator.
    ///
    /// `out.len()` must equal `a.len() + b.len()`; the module docs' `# Design`
    /// section has the co-rank chunking this performs and why it needs no
    /// partitioner.
    ///
    /// # Panics
    ///
    /// Panics if `out.len() != a.len() + b.len()`.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let a = [1, 3, 5, 7];
    ///     let b = [2, 4, 6];
    ///     let mut out = [0; 7];
    ///     runtime.merge(&a, &b, &mut out);
    ///     assert_eq!(out, [1, 2, 3, 4, 5, 6, 7]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn merge<T>(&mut self, a: &[T], b: &[T], out: &mut [T])
    where
        T: Ord + Clone + Send + Sync,
    {
        self.merge_by(a, b, out, |x, y| x < y);
    }

    /// Merges sorted `a` and `b` into `out` under `cmp`: upstream's
    /// `make_merge_task`'s body, as a method on the [`Runtime`] a `scope`
    /// call needs to fan the chunks below out across workers.
    ///
    /// `a` and `b` must already be sorted with respect to `cmp`, and
    /// `out.len()` must equal `a.len() + b.len()`; the module docs' `# Design`
    /// section has the co-rank chunking this performs and why it needs no
    /// partitioner.
    ///
    /// # Panics
    ///
    /// Panics if `out.len() != a.len() + b.len()`.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let a = [7, 5, 3, 1];
    ///     let b = [6, 4, 2];
    ///     let mut out = [0; 7];
    ///     runtime.merge_by(&a, &b, &mut out, |x, y| x > y);
    ///     assert_eq!(out, [7, 6, 5, 4, 3, 2, 1]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn merge_by<T, C>(&mut self, a: &[T], b: &[T], out: &mut [T], cmp: C)
    where
        T: Clone + Send + Sync,
        C: Fn(&T, &T) -> bool + Sync,
    {
        let total = a.len() + b.len();
        assert_eq!(
            out.len(),
            total,
            "merge: out.len() ({}) must equal a.len() + b.len() ({total})",
            out.len(),
        );
        if total == 0 {
            return;
        }

        let workers = self.num_workers();
        if workers <= 1 || total <= workers {
            merge_serial(a, b, out, &cmp);
            return;
        }

        // Upstream's `chunk_size = (N + W - 1) / W`, written with `div_ceil`
        // rather than the overflow-prone `+ W - 1` form.
        let chunk_size = total.div_ceil(workers);

        self.scope(|s| {
            let mut rest = out;
            let mut curr_b = 0_usize;
            let mut i_beg = 0_usize;
            let mut j_beg = 0_usize;

            while curr_b < total {
                let curr_e = (curr_b + chunk_size).min(total);
                let (i_end, j_end) = co_rank(a, b, &cmp, curr_e);

                let len = curr_e - curr_b;
                let (chunk, remainder) = rest.split_at_mut(len);
                rest = remainder;

                let a_part = &a[i_beg..i_end];
                let b_part = &b[j_beg..j_end];

                if curr_e >= total {
                    // Upstream's last-worker case: run inline rather than
                    // spawning a child only to join it immediately after.
                    merge_serial(a_part, b_part, chunk, &cmp);
                } else {
                    let cmp = &cmp;
                    s.spawn(move || merge_serial(a_part, b_part, chunk, cmp));
                }

                i_beg = i_end;
                j_beg = j_end;
                curr_b = curr_e;
            }
        });
    }
}

/// Locks `slot`, taking a poisoned lock's contents rather than failing on
/// them: the one panic that could poison it is inside the merge itself, in
/// which case the node this body runs on has already failed and carries that
/// panic back to its anchor, and the half-written `Vec` this would otherwise
/// refuse to hand back is simply never looked at by a caller who checked the
/// run's result first.
fn lock_result<T>(slot: &Mutex<Vec<T>>) -> std::sync::MutexGuard<'_, Vec<T>> {
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Builds a runtime-shaped body that merges owned `a` and `b`, by `T`'s own
/// [`Ord`], publishing the result through the returned slot: upstream's
/// `make_merge_task`'s `std::less<>` overload, over owned inputs rather than
/// iterators into storage the caller keeps alive -- the module docs'
/// `# Design` section says why.
///
/// `a` and `b` are [`SliceSource`]s, so each may be an owned `Vec<T>`, a
/// shared `Arc<Vec<T>>` or the `Arc<Mutex<Vec<T>>>` an earlier factory
/// wrote; both are read under guards taken in that order and released
/// before the result slot is locked.
///
/// Returns a body suitable for
/// [`Executor::spawn_runtime`](crate::Executor::spawn_runtime),
/// [`Executor::silent_async_runtime`](crate::Executor::silent_async_runtime),
/// [`Executor::dependent_async_runtime`](crate::Executor::dependent_async_runtime)
/// or [`FlowBuilder::emplace_runtime`](crate::FlowBuilder::emplace_runtime),
/// and the `Arc<Mutex<Vec<T>>>` that body locks once, after its merge is
/// done, to publish the result into. See [`make_merge_task_by`] for a custom
/// comparator, and [`AlgorithmBuilder::merge`] for the graph-node wrapper
/// built on this.
///
/// [`AlgorithmBuilder::merge`]: crate::algorithm::AlgorithmBuilder::merge
///
/// `FnMut` rather than `FnOnce`, as [`make_module_task`](super::module::make_module_task)
/// is, since the node this becomes may run more than once.
///
/// ```
/// use rustflow::algorithm::merge::make_merge_task;
/// use rustflow::Executor;
///
/// let executor = Executor::new(2);
/// let (body, result) = make_merge_task(vec![1, 3, 5], vec![2, 4, 6]);
/// executor.silent_async_runtime(body);
/// executor.wait_for_all();
///
/// assert_eq!(*result.lock().expect("uncontended"), [1, 2, 3, 4, 5, 6]);
/// ```
pub fn make_merge_task<T>(
    a: impl SliceSource<Item = T> + Send + 'static,
    b: impl SliceSource<Item = T> + Send + 'static,
) -> (
    impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static,
    Arc<Mutex<Vec<T>>>,
)
where
    T: Ord + Clone + Send + Sync + 'static,
{
    make_merge_task_by(a, b, |x, y| x < y)
}

/// Builds a runtime-shaped body that merges owned `a` and `b` under `cmp`,
/// publishing the result through the returned slot: upstream's
/// `make_merge_task`, over owned inputs -- see [`make_merge_task`] for the
/// `Ord`-only form, the shapes `a` and `b` accept and the order their guards
/// are taken in, and the module docs' `# Design` section for why this
/// returns a slot rather than writing through an iterator the caller
/// already owns storage behind.
///
/// ```
/// use rustflow::algorithm::merge::make_merge_task_by;
/// use rustflow::Executor;
///
/// let executor = Executor::new(2);
/// let (body, result) = make_merge_task_by(vec![5, 3, 1], vec![6, 4, 2], |x, y| x > y);
/// executor.silent_async_runtime(body);
/// executor.wait_for_all();
///
/// assert_eq!(*result.lock().expect("uncontended"), [6, 5, 4, 3, 2, 1]);
/// ```
pub fn make_merge_task_by<T, C>(
    a: impl SliceSource<Item = T> + Send + 'static,
    b: impl SliceSource<Item = T> + Send + 'static,
    cmp: C,
) -> (
    impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static,
    Arc<Mutex<Vec<T>>>,
)
where
    T: Clone + Send + Sync + 'static,
    C: Fn(&T, &T) -> bool + Send + Sync + 'static,
{
    let result = Arc::new(Mutex::new(Vec::new()));
    let published = Arc::clone(&result);

    let body = move |rt: &mut Runtime<'_>| {
        // Both read guards end with this block, so the slot below is locked
        // with neither of them held: `algorithm`'s own `# Design` section
        // states that discipline for the whole family.
        let buf = {
            let first = a.read_slice();
            let second = b.read_slice();
            // A scratch buffer of the right length and already-valid `T`s,
            // built from the very elements the merge below overwrites
            // wholesale -- the module docs' `# Design` section says why this
            // needs `Clone` rather than, say, `Default`.
            let mut buf: Vec<T> = first.iter().chain(second.iter()).cloned().collect();
            rt.merge_by(&first, &second, &mut buf, &cmp);
            buf
        };
        *lock_result(&published) = buf;
    };

    (body, result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Executor;
    use crate::prelude::*;

    /// A fully serial case -- one worker -- still produces a correctly
    /// merged, interleaved result: the fallback upstream takes for `W <= 1`.
    #[test]
    fn merge_on_one_worker_is_still_correct() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|rt: &mut Runtime<'_>| {
            let a = [1, 4, 7, 10];
            let b = [2, 3, 8, 9, 20];
            let mut out = [0; 9];
            rt.merge(&a, &b, &mut out);
            assert_eq!(out, [1, 2, 3, 4, 7, 8, 9, 10, 20]);
        });

        Executor::new(1)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }

    /// Two empty inputs merge to an empty output and touch nothing: the one
    /// case [`Runtime::merge_by`] returns from before ever reading
    /// `num_workers`.
    #[test]
    fn two_empty_inputs_merge_to_nothing() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|rt: &mut Runtime<'_>| {
            let a: [i32; 0] = [];
            let b: [i32; 0] = [];
            let mut out: [i32; 0] = [];
            rt.merge(&a, &b, &mut out);
        });

        Executor::new(4)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }

    /// A length mismatch between `out` and `a.len() + b.len()` panics rather
    /// than silently truncating or reading past the end of `out`.
    #[test]
    #[should_panic(expected = "out.len()")]
    fn a_mismatched_out_length_panics() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|rt: &mut Runtime<'_>| {
            let a = [1, 2];
            let b = [3, 4];
            let mut out = [0; 3];
            rt.merge(&a, &b, &mut out);
        });

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }

    /// `AlgorithmBuilder::merge_by` from a [`Subflow`] body: the extension
    /// trait reaches every [`FlowBuilder`] implementor, not only
    /// [`Taskflow`].
    #[test]
    fn merge_by_works_from_a_subflow_too() {
        type SlotHolder = Arc<Mutex<Option<Arc<Mutex<Vec<i32>>>>>>;

        let mut taskflow = Taskflow::new();
        let slot_holder: SlotHolder = Arc::new(Mutex::new(None));
        let published = Arc::clone(&slot_holder);
        taskflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
            let (_task, slot) = subflow.merge_by(vec![9, 5, 1], vec![7, 3], |x, y| x > y);
            *published.lock().expect("uncontended") = Some(slot);
        });

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        let slot = slot_holder
            .lock()
            .expect("uncontended")
            .clone()
            .expect("the subflow task ran");
        assert_eq!(*slot.lock().expect("uncontended"), [9, 7, 5, 3, 1]);
    }

    /// Both merge inputs are [`SliceSource`]s, so a merge can consume what
    /// two earlier factories sorted in place rather than only a `Vec` the
    /// caller built itself. The sorts are linearized ahead of the merge for
    /// the reason `algorithm`'s `# Design` section gives; they could run
    /// together, each holding a different lock, but the merge reads both and
    /// so must follow both.
    #[test]
    fn a_merge_factory_reads_two_locked_sources() {
        let a = Arc::new(Mutex::new(vec![5, 1, 3]));
        let b = Arc::new(Mutex::new(vec![6, 2, 4]));

        let mut taskflow = Taskflow::new();
        let sorted_a =
            taskflow.emplace_runtime(crate::algorithm::sort::make_sort_task(Arc::clone(&a)));
        let sorted_b =
            taskflow.emplace_runtime(crate::algorithm::sort::make_sort_task(Arc::clone(&b)));
        let (body, merged) = make_merge_task(Arc::clone(&a), Arc::clone(&b));
        let merge = taskflow.emplace_runtime(body);
        taskflow.linearize([sorted_a, sorted_b, merge]);

        Executor::new(2)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");

        assert_eq!(*merged.lock().expect("uncontended"), [1, 2, 3, 4, 5, 6]);
    }
}

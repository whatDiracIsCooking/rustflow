//! Translated from `taskflow/algorithm/scan.hpp` (Taskflow v4.1.0).
//!
//! Six entry points, all over a borrowed `input: &[T]` written into a
//! same-length `out: &mut [T]` -- no in-place aliasing, upstream's own
//! `std::ref(sbeg)` form that lets a single iterator be both source and
//! destination has no translation here, since `&[T]` and `&mut [T]` cannot
//! borrow the same slice at once. [`Runtime::inclusive_scan`] and
//! [`Runtime::inclusive_scan_with_init`] are upstream's two
//! `make_inclusive_scan_task` overloads; [`Runtime::exclusive_scan`] is
//! `make_exclusive_scan_task`; [`Runtime::transform_inclusive_scan`] and
//! [`Runtime::transform_inclusive_scan_with_init`] are the two
//! `make_transform_inclusive_scan_task` overloads; and
//! [`Runtime::transform_exclusive_scan`] is `make_transform_exclusive_scan_task`.
//! Each also has a `make_*_task` free function building the owned-data
//! runtime body `emplace_runtime` wants, and a [`ScanBuilder`] method
//! queuing that body as a task.
//!
//! # Design
//!
//! **Two generic cores, not six.** Upstream's six `make_*_task` functions
//! already share one block-parallel shape and one `detail::scan_loop` for
//! the "add this block's offset" step; this port goes one step further and
//! shares the *whole* two-phase body across every inclusive-family variant,
//! because a plain inclusive scan is exactly a transform-scan whose `uop`
//! is [`Clone::clone`]: [`Runtime::inclusive_scan`] and
//! [`Runtime::inclusive_scan_with_init`] call
//! [`Runtime::transform_inclusive_scan`] / `_with_init` with `T::clone` as
//! the transform, rather than re-deriving the block loop. `scan_two_phase`,
//! below, is the one function that actually runs it, taking an `Option<&T>`
//! for upstream's two-overloads-by-arity: `None` where upstream has no
//! `init` parameter at all.
//!
//! **Exclusive is inclusive, shifted by one.** Upstream's
//! `make_exclusive_scan_task` pre-fetches each block's raw boundary element
//! into the reductions buffer before any block runs, so that the same
//! `detail::scan_loop` add-up it already has for the inclusive family also
//! finishes an exclusive scan. That trick earns its complexity in C++,
//! where `buf[w].data = *s_beg_temp` is a plain iterator read; porting it
//! as written would mean carrying a second, exclusive-only shape through
//! `scan_two_phase`'s split-and-spawn plumbing for no asymptotic gain, since
//! either shape is one pass to size the blocks and one to fill them. A
//! cleaner identity does the same job: exclusive scan's definition is
//! `out[0] = init` and `out[i] = fold(init, input[0..i))` for `i >= 1`,
//! which is precisely `out[i] = inclusive_scan_with_init(input[0..n-1])[i-1]`
//! -- an inclusive scan *with the same `init`*, run over every input
//! element except the last (whose value an exclusive scan of length `n`
//! never needs), written one slot to the right of where it would otherwise
//! land. [`Runtime::exclusive_scan`] and [`Runtime::transform_exclusive_scan`]
//! are exactly that: `out[0] = init.clone()`, then one call to
//! [`Runtime::transform_inclusive_scan_with_init`] over `&input[..n - 1]`
//! and `&mut out[1..]`. `tests/scan.rs`'s non-commutative case is what
//! this identity is checked against, since a commutative `bop` cannot tell
//! a correctly-shifted exclusive scan from an incorrectly-ordered one.
//!
//! **What makes the second round sound.** `scan_two_phase` owns the
//! `totals: Vec<T>` reductions buffer and the block boundaries; both
//! rounds of children borrow out of them through [`Runtime::scope`], never
//! an `Arc` or a lock. The first round writes every block's own local total
//! into `totals[w]` and nowhere else; what makes the second round's read of
//! `totals[w - 1]` -- after the serial prefix-sum between the two scope
//! calls has combined it with every earlier block's own total -- safe to
//! read without any atomic or lock of its own is that first [`Runtime::scope`]
//! call not returning until every one of its children has: its join is
//! built on [`Runtime::try_corun`], which is the same `corun_until` seam
//! `scan.hpp`'s own comment calls out as "public ... because multiple
//! workers can call this". The prefix-sum that follows runs after that join
//! and before the second `scope` call opens, entirely on the worker that is
//! running this body, so nothing is racing it either. The second round
//! therefore needs no synchronization of its own beyond the two scope
//! calls bracketing it -- not a counter, not a lock -- because by the time
//! it can run, the write it depends on already happened-before it, through
//! that join.
//!
//! **`T: Clone`.** Every variant below writes the same running value to an
//! output slot and carries it forward to combine with the next element, so
//! unlike [`Runtime::for_each`](crate::algorithm::for_each), which only ever
//! mutates a slot once, a scan cannot avoid asking for [`Clone`] -- there is
//! no way to both keep a value and hand a copy of it to the output slice
//! with only [`Send`]. `bop`'s own signature, `Fn(&T, &T) -> T`, is taken
//! by reference on both sides rather than upstream's by-value `BOP(T, T)`,
//! so a non-commutative, non-`Copy` `bop` -- string concatenation, say --
//! never has to clone an operand just to be allowed to read it twice.

use std::sync::{Arc, Mutex};

use crate::flow_builder::FlowBuilder;
use crate::graph::Task;
use crate::runtime::Runtime;

impl<'rt> Runtime<'rt> {
    /// Scans `input` into `out`, upstream's `inclusive_scan(first, last,
    /// d_first, bop)`: `out[i] = bop(bop(...bop(input[0], input[1]), ...),
    /// input[i])`.
    ///
    /// Splits `input` into one block per worker and runs two rounds of
    /// children under [`Runtime::scope`] when there is more than one
    /// worker and `input.len() > 2`; otherwise runs the whole scan right
    /// here. The module's `# Design` section has the two-phase shape and
    /// why its ordering is sound without an atomic or lock of its own.
    ///
    /// # Panics
    ///
    /// Panics if `out.len() != input.len()`.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let input = [1, 2, 3, 4, 5, 6, 7, 8];
    ///     let mut out = [0; 8];
    ///     runtime.inclusive_scan(&input, &mut out, |a: &i32, b: &i32| a + b);
    ///     assert_eq!(out, [1, 3, 6, 10, 15, 21, 28, 36]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn inclusive_scan<T, F>(&mut self, input: &[T], out: &mut [T], bop: F)
    where
        T: Send + Sync + Clone,
        F: Fn(&T, &T) -> T + Sync,
    {
        self.transform_inclusive_scan(input, out, bop, T::clone);
    }

    /// [`Runtime::inclusive_scan`], seeded with `init`: upstream's
    /// `inclusive_scan(first, last, d_first, bop, init)`. `out[0] =
    /// bop(init, input[0])`, and every later slot folds in one more element
    /// the same way `inclusive_scan`'s does.
    ///
    /// # Panics
    ///
    /// Panics if `out.len() != input.len()`.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let input = [1, 2, 3, 4, 5, 6, 7, 8];
    ///     let mut out = [0; 8];
    ///     runtime.inclusive_scan_with_init(&input, &mut out, |a: &i32, b: &i32| a + b, &100);
    ///     assert_eq!(out, [101, 103, 106, 110, 115, 121, 128, 136]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn inclusive_scan_with_init<T, F>(&mut self, input: &[T], out: &mut [T], bop: F, init: &T)
    where
        T: Send + Sync + Clone,
        F: Fn(&T, &T) -> T + Sync,
    {
        self.transform_inclusive_scan_with_init(input, out, bop, T::clone, init);
    }

    /// Scans `input` into `out` starting from `init`, upstream's
    /// `exclusive_scan(first, last, d_first, init, bop)`: `out[0] = init`
    /// and `out[i] = bop(out[i - 1], input[i - 1])` for `i >= 1`, so
    /// `input[i]` never contributes to `out[i]` itself.
    ///
    /// Built over [`Runtime::transform_inclusive_scan_with_init`], not a
    /// dedicated block loop -- the module's `# Design` section has the
    /// identity this rests on.
    ///
    /// # Panics
    ///
    /// Panics if `out.len() != input.len()`.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let input = [1, 2, 3, 4, 5, 6, 7, 8];
    ///     let mut out = [0; 8];
    ///     runtime.exclusive_scan(&input, &mut out, 100, |a: &i32, b: &i32| a + b);
    ///     assert_eq!(out, [100, 101, 103, 106, 110, 115, 121, 128]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn exclusive_scan<T, F>(&mut self, input: &[T], out: &mut [T], init: T, bop: F)
    where
        T: Send + Sync + Clone,
        F: Fn(&T, &T) -> T + Sync,
    {
        self.transform_exclusive_scan(input, out, init, bop, T::clone);
    }

    /// Scans `uop(input[i])` into `out`, upstream's
    /// `transform_inclusive_scan(first, last, d_first, bop, uop)`: `out[i] =
    /// bop(bop(...bop(uop(input[0]), uop(input[1])), ...), uop(input[i]))`.
    /// `uop` runs on every element exactly once, before `bop` ever sees it,
    /// matching upstream applying it inside the fold rather than as a
    /// separate pass.
    ///
    /// # Panics
    ///
    /// Panics if `out.len() != input.len()`.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let input = [1, 2, 3, 4, 5, 6, 7, 8];
    ///     let mut out = [0; 8];
    ///     runtime.transform_inclusive_scan(
    ///         &input,
    ///         &mut out,
    ///         |a: &i32, b: &i32| a + b,
    ///         |x: &i32| x * 2,
    ///     );
    ///     assert_eq!(out, [2, 6, 12, 20, 30, 42, 56, 72]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn transform_inclusive_scan<T, F, Uop>(
        &mut self,
        input: &[T],
        out: &mut [T],
        bop: F,
        uop: Uop,
    ) where
        T: Send + Sync + Clone,
        F: Fn(&T, &T) -> T + Sync,
        Uop: Fn(&T) -> T + Sync,
    {
        scan_two_phase(self, input, out, &bop, &uop, None);
    }

    /// [`Runtime::transform_inclusive_scan`], seeded with `init`: upstream's
    /// `transform_inclusive_scan(first, last, d_first, bop, uop, init)`.
    /// `out[0] = bop(init, uop(input[0]))`.
    ///
    /// # Panics
    ///
    /// Panics if `out.len() != input.len()`.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let input = [1, 2, 3, 4, 5, 6, 7, 8];
    ///     let mut out = [0; 8];
    ///     runtime.transform_inclusive_scan_with_init(
    ///         &input,
    ///         &mut out,
    ///         |a: &i32, b: &i32| a + b,
    ///         |x: &i32| x * 2,
    ///         &100,
    ///     );
    ///     assert_eq!(out, [102, 106, 112, 120, 130, 142, 156, 172]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn transform_inclusive_scan_with_init<T, F, Uop>(
        &mut self,
        input: &[T],
        out: &mut [T],
        bop: F,
        uop: Uop,
        init: &T,
    ) where
        T: Send + Sync + Clone,
        F: Fn(&T, &T) -> T + Sync,
        Uop: Fn(&T) -> T + Sync,
    {
        scan_two_phase(self, input, out, &bop, &uop, Some(init));
    }

    /// Scans `uop(input[i])` into `out` starting from `init`, upstream's
    /// `transform_exclusive_scan(first, last, d_first, init, bop, uop)`:
    /// `out[0] = init` and `out[i] = bop(out[i - 1], uop(input[i - 1]))` for
    /// `i >= 1`.
    ///
    /// Built over [`Runtime::transform_inclusive_scan_with_init`], exactly
    /// as [`Runtime::exclusive_scan`] is -- the module's `# Design` section
    /// has why.
    ///
    /// # Panics
    ///
    /// Panics if `out.len() != input.len()`.
    ///
    /// ```
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let input = [1, 2, 3, 4, 5, 6, 7, 8];
    ///     let mut out = [0; 8];
    ///     runtime.transform_exclusive_scan(
    ///         &input,
    ///         &mut out,
    ///         100,
    ///         |a: &i32, b: &i32| a + b,
    ///         |x: &i32| x * 2,
    ///     );
    ///     assert_eq!(out, [100, 102, 106, 112, 120, 130, 142, 156]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn transform_exclusive_scan<T, F, Uop>(
        &mut self,
        input: &[T],
        out: &mut [T],
        init: T,
        bop: F,
        uop: Uop,
    ) where
        T: Send + Sync + Clone,
        F: Fn(&T, &T) -> T + Sync,
        Uop: Fn(&T) -> T + Sync,
    {
        let n = input.len();
        assert_eq!(
            out.len(),
            n,
            "scan's output slice must match its input's length"
        );
        if n == 0 {
            return;
        }
        out[0] = init.clone();
        if n > 1 {
            self.transform_inclusive_scan_with_init(
                &input[..n - 1],
                &mut out[1..],
                bop,
                uop,
                &init,
            );
        }
    }
}

/// The boundaries of `w_total` blocks tiling `[0, n)`: the first `n % w_total`
/// are one element wider than the rest, matching upstream's `Q = N/W, R =
/// N%W` split.
fn block_bounds(n: usize, w_total: usize) -> Vec<(usize, usize)> {
    let q = n / w_total;
    let r = n % w_total;
    let mut bounds = Vec::with_capacity(w_total);
    let mut start = 0;
    for w in 0..w_total {
        let len = if w < r { q + 1 } else { q };
        bounds.push((start, len));
        start += len;
    }
    bounds
}

/// Carves `data` into one mutable slice per entry of `bounds`, left to
/// right: the mutable half of [`block_bounds`]'s split, via
/// [`<[T]>::split_at_mut`] rather than any raw-pointer aliasing. Taking
/// `data` as a reborrow (`&mut *out` at the call site) rather than owning
/// `out` outright is what lets `scan_two_phase` call this twice -- once per
/// round -- over the same slice.
fn split_blocks<'a, T>(data: &'a mut [T], bounds: &[(usize, usize)]) -> Vec<&'a mut [T]> {
    let mut remaining = data;
    let mut pieces = Vec::with_capacity(bounds.len());
    for &(_, len) in bounds {
        let (head, tail) = remaining.split_at_mut(len);
        pieces.push(head);
        remaining = tail;
    }
    pieces
}

/// Runs the two-phase block-parallel scan every inclusive-family method
/// above shares: upstream's `make_inclusive_scan_task` /
/// `make_transform_inclusive_scan_task`, unified over `uop` (identity is
/// [`Clone::clone`] for the two plain-`bop` variants) and over `init` being
/// absent (`None`) where upstream has no such overload. The module's own
/// `# Design` section is the argument for why this needs no synchronization
/// beyond the two [`Runtime::scope`] calls below.
fn scan_two_phase<T, F, Uop>(
    rt: &mut Runtime<'_>,
    input: &[T],
    out: &mut [T],
    bop: &F,
    uop: &Uop,
    init: Option<&T>,
) where
    T: Send + Sync + Clone,
    F: Fn(&T, &T) -> T + Sync,
    Uop: Fn(&T) -> T + Sync,
{
    let n = input.len();
    assert_eq!(
        out.len(),
        n,
        "scan's output slice must match its input's length"
    );
    if n == 0 {
        return;
    }

    let w_total = rt.num_workers();
    // Only myself -- no need to spawn another graph, upstream's `W <= 1 ||
    // N <= 2`.
    if w_total <= 1 || n <= 2 {
        let mut iter = input.iter().zip(out.iter_mut());
        let (first_in, first_out) = iter.next().expect("n > 0 was just checked");
        let mut local = match init {
            Some(seed) => bop(seed, &uop(first_in)),
            None => uop(first_in),
        };
        *first_out = local.clone();
        for (elem, slot) in iter {
            local = bop(&local, &uop(elem));
            *slot = local.clone();
        }
        return;
    }
    let w_total = w_total.min(n);

    let bounds = block_bounds(n, w_total);
    let in_blocks: Vec<&[T]> = bounds
        .iter()
        .map(|&(start, len)| &input[start..start + len])
        .collect();

    // Round 1: each block scans itself locally, writing provisional output
    // and publishing its own total into `totals[w]`.
    let mut totals: Vec<Option<T>> = (0..w_total).map(|_| None).collect();
    {
        let out_blocks = split_blocks(&mut *out, &bounds);
        let total_slots: Vec<&mut Option<T>> = totals.iter_mut().collect();
        rt.scope(|s| {
            for (w, ((in_block, out_block), slot)) in in_blocks
                .iter()
                .copied()
                .zip(out_blocks)
                .zip(total_slots)
                .enumerate()
            {
                s.spawn(move || {
                    let seed = if w == 0 { init } else { None };
                    let mut iter = in_block.iter().zip(out_block.iter_mut());
                    let (first_in, first_out) = iter.next().expect("every block is non-empty");
                    let mut local = match seed {
                        Some(i) => bop(i, &uop(first_in)),
                        None => uop(first_in),
                    };
                    *first_out = local.clone();
                    for (elem, out_slot) in iter {
                        local = bop(&local, &uop(elem));
                        *out_slot = local.clone();
                    }
                    *slot = Some(local);
                });
            }
        });
    }

    // Serial prefix-sum over the published totals: upstream's `detail::
    // make_mscan_task`, run right here rather than by whichever worker
    // finished last, since the join above already means every total is in.
    let mut totals: Vec<T> = totals
        .into_iter()
        .map(|t| t.expect("round 1's scope joined every block before returning"))
        .collect();
    for w in 1..w_total {
        totals[w] = bop(&totals[w - 1], &totals[w]);
    }

    // Round 2: every block but the first adds its running offset to what it
    // already wrote -- upstream's `detail::scan_loop` add-up phase.
    let out_blocks = split_blocks(&mut *out, &bounds);
    rt.scope(|s| {
        for (w, out_block) in out_blocks.into_iter().enumerate().skip(1) {
            let offset = &totals[w - 1];
            s.spawn(move || {
                for slot in out_block.iter_mut() {
                    *slot = bop(offset, slot);
                }
            });
        }
    });
}

/// Builds the runtime body of an [`Runtime::inclusive_scan`] task: upstream's
/// `make_inclusive_scan_task`. `out` is locked once and scanned in place;
/// `input` is read without locking, since nothing here ever writes to it.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::scan::make_inclusive_scan_task;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let input = Arc::new(vec![1, 2, 3, 4, 5]);
/// let out = Arc::new(Mutex::new(vec![0; 5]));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_inclusive_scan_task(
///     Arc::clone(&input),
///     Arc::clone(&out),
///     |a: &i32, b: &i32| a + b,
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(*out.lock().expect("uncontended"), vec![1, 3, 6, 10, 15]);
/// ```
pub fn make_inclusive_scan_task<T, F>(
    input: Arc<Vec<T>>,
    out: Arc<Mutex<Vec<T>>>,
    bop: F,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Send + Sync + Clone + 'static,
    F: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let mut guard = out
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.inclusive_scan(&input, &mut guard, bop.clone());
    }
}

/// Builds the runtime body of a [`Runtime::inclusive_scan_with_init`] task:
/// upstream's `make_inclusive_scan_task` overload that takes `init`.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::scan::make_inclusive_scan_with_init_task;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let input = Arc::new(vec![1, 2, 3, 4, 5]);
/// let out = Arc::new(Mutex::new(vec![0; 5]));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_inclusive_scan_with_init_task(
///     Arc::clone(&input),
///     Arc::clone(&out),
///     |a: &i32, b: &i32| a + b,
///     100,
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(*out.lock().expect("uncontended"), vec![101, 103, 106, 110, 115]);
/// ```
pub fn make_inclusive_scan_with_init_task<T, F>(
    input: Arc<Vec<T>>,
    out: Arc<Mutex<Vec<T>>>,
    bop: F,
    init: T,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Send + Sync + Clone + 'static,
    F: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let mut guard = out
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.inclusive_scan_with_init(&input, &mut guard, bop.clone(), &init);
    }
}

/// Builds the runtime body of a [`Runtime::exclusive_scan`] task: upstream's
/// `make_exclusive_scan_task`.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::scan::make_exclusive_scan_task;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let input = Arc::new(vec![1, 2, 3, 4, 5]);
/// let out = Arc::new(Mutex::new(vec![0; 5]));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_exclusive_scan_task(
///     Arc::clone(&input),
///     Arc::clone(&out),
///     100,
///     |a: &i32, b: &i32| a + b,
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(*out.lock().expect("uncontended"), vec![100, 101, 103, 106, 110]);
/// ```
pub fn make_exclusive_scan_task<T, F>(
    input: Arc<Vec<T>>,
    out: Arc<Mutex<Vec<T>>>,
    init: T,
    bop: F,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Send + Sync + Clone + 'static,
    F: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let mut guard = out
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.exclusive_scan(&input, &mut guard, init.clone(), bop.clone());
    }
}

/// Builds the runtime body of a [`Runtime::transform_inclusive_scan`] task:
/// upstream's `make_transform_inclusive_scan_task`.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::scan::make_transform_inclusive_scan_task;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let input = Arc::new(vec![1, 2, 3, 4, 5]);
/// let out = Arc::new(Mutex::new(vec![0; 5]));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_transform_inclusive_scan_task(
///     Arc::clone(&input),
///     Arc::clone(&out),
///     |a: &i32, b: &i32| a + b,
///     |x: &i32| x * 2,
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(*out.lock().expect("uncontended"), vec![2, 6, 12, 20, 30]);
/// ```
pub fn make_transform_inclusive_scan_task<T, F, Uop>(
    input: Arc<Vec<T>>,
    out: Arc<Mutex<Vec<T>>>,
    bop: F,
    uop: Uop,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Send + Sync + Clone + 'static,
    F: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
    Uop: Fn(&T) -> T + Sync + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let mut guard = out
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.transform_inclusive_scan(&input, &mut guard, bop.clone(), uop.clone());
    }
}

/// Builds the runtime body of a [`Runtime::transform_inclusive_scan_with_init`]
/// task: upstream's `make_transform_inclusive_scan_task` overload that
/// takes `init`.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::scan::make_transform_inclusive_scan_with_init_task;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let input = Arc::new(vec![1, 2, 3, 4, 5]);
/// let out = Arc::new(Mutex::new(vec![0; 5]));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_transform_inclusive_scan_with_init_task(
///     Arc::clone(&input),
///     Arc::clone(&out),
///     |a: &i32, b: &i32| a + b,
///     |x: &i32| x * 2,
///     100,
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(*out.lock().expect("uncontended"), vec![102, 106, 112, 120, 130]);
/// ```
pub fn make_transform_inclusive_scan_with_init_task<T, F, Uop>(
    input: Arc<Vec<T>>,
    out: Arc<Mutex<Vec<T>>>,
    bop: F,
    uop: Uop,
    init: T,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Send + Sync + Clone + 'static,
    F: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
    Uop: Fn(&T) -> T + Sync + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let mut guard = out
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.transform_inclusive_scan_with_init(&input, &mut guard, bop.clone(), uop.clone(), &init);
    }
}

/// Builds the runtime body of a [`Runtime::transform_exclusive_scan`] task:
/// upstream's `make_transform_exclusive_scan_task`.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::scan::make_transform_exclusive_scan_task;
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let input = Arc::new(vec![1, 2, 3, 4, 5]);
/// let out = Arc::new(Mutex::new(vec![0; 5]));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_transform_exclusive_scan_task(
///     Arc::clone(&input),
///     Arc::clone(&out),
///     100,
///     |a: &i32, b: &i32| a + b,
///     |x: &i32| x * 2,
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(*out.lock().expect("uncontended"), vec![100, 102, 106, 112, 120]);
/// ```
pub fn make_transform_exclusive_scan_task<T, F, Uop>(
    input: Arc<Vec<T>>,
    out: Arc<Mutex<Vec<T>>>,
    init: T,
    bop: F,
    uop: Uop,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Send + Sync + Clone + 'static,
    F: Fn(&T, &T) -> T + Sync + Clone + Send + 'static,
    Uop: Fn(&T) -> T + Sync + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let mut guard = out
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.transform_exclusive_scan(&input, &mut guard, init.clone(), bop.clone(), uop.clone());
    }
}

/// The owned-data half of every scan method on [`Runtime`]: upstream's six
/// `FlowBuilder::*_scan` members, over the six `make_*_task` functions
/// above. A blanket-implemented extension trait rather than a second
/// `trait FlowBuilder` block, for the reason [`FlowBuilder`]'s own `# Design`
/// section gives: [`FlowBuilder`] is a trait, implemented by `Taskflow` and
/// `Subflow`, with no single struct a second `impl` block could extend.
///
/// In [`crate::prelude`] alongside [`FlowBuilder`] itself, since a trait's
/// methods are only callable where the trait is imported.
pub trait ScanBuilder: FlowBuilder {
    /// Creates a runtime task that locks `out` and runs
    /// [`Runtime::inclusive_scan`] over it and `input`: upstream's
    /// `FlowBuilder::inclusive_scan(first, last, d_first, bop)`.
    ///
    /// `rustflow::prelude::*` is the only import this example needs:
    /// `ScanBuilder` itself is one of the prelude's names, not imported
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
}

impl<B: FlowBuilder + ?Sized> ScanBuilder for B {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::prelude::*;
    use crate::{Executor, Runtime};

    /// `N == 0` is a no-op for every variant, upstream's first
    /// short-circuit. The acceptance-level sweep over sizes, the worker
    /// matrix and the non-commutative case lives in `tests/scan.rs`
    /// instead, alongside `tests/for_each.rs`'s own translation of its
    /// upstream file.
    #[test]
    fn empty_input_is_a_no_op() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let input: [u32; 0] = [];
            let mut out: [u32; 0] = [];
            runtime.inclusive_scan(&input, &mut out, |a: &u32, b: &u32| a + b);
            runtime.exclusive_scan(&input, &mut out, 0, |a: &u32, b: &u32| a + b);
        });
        Executor::new(2).run(&taskflow).wait().expect("no panic");
    }

    /// A sanity check on the shape of the two-phase scan itself, on top of
    /// the arithmetic below: a counter incremented inside `bop` counts
    /// exactly the three contributions `scan_two_phase`'s own doc comment
    /// names. 30 elements over 4 workers split into blocks of `[8, 8, 7,
    /// 7]` (`block_bounds(30, 4)`): round 1 calls `bop` once per non-first
    /// element of each block (`7 + 7 + 6 + 6 == 26`, the first slot of each
    /// block coming from `uop` alone); the serial prefix-sum between the
    /// two scope calls folds the four totals together (`4 - 1 == 3`);
    /// round 2 calls `bop` once per element of every block but the first
    /// (`8 + 7 + 7 == 22`). A double-written or skipped slot would move
    /// this count before the sum itself is even checked.
    #[test]
    fn bop_call_count_matches_the_two_phase_shape() {
        let counter = Arc::new(AtomicUsize::new(0));
        let touched = Arc::clone(&counter);

        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let input: Vec<i64> = (0..30).collect();
            let mut out = vec![0; 30];
            let touched = Arc::clone(&touched);
            runtime.inclusive_scan(&input, &mut out, move |a: &i64, b: &i64| {
                touched.fetch_add(1, Ordering::Relaxed);
                a + b
            });
            // Triangular numbers: sum(0..=i) == i * (i + 1) / 2.
            let golden: Vec<i64> = (0..30i64).map(|i| i * (i + 1) / 2).collect();
            assert_eq!(out, golden);
        });

        Executor::new(4)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert_eq!(counter.load(Ordering::Relaxed), 26 + 3 + 22);
    }
}

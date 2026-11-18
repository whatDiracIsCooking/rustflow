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
//! # Design
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

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use crate::Scope;
use crate::algorithm::partitioner::{
    ClosureWrapper, DynamicLoop, DynamicPartitioner, GuidedPartitioner, Partitioner,
    RandomPartitioner, StaticLoop, StaticPartitioner,
};

pub mod fill;
pub mod find;
pub mod for_each;
pub mod for_each_index;
pub mod generate;
pub mod index_range;
pub mod merge;
pub mod module;
pub mod partitioner;
pub mod transform;

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

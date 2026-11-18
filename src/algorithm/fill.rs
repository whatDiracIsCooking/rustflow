//! Translated from `taskflow/algorithm/fill.hpp` (Taskflow v4.1.0).
//!
//! [`Runtime::fill`] is upstream's `make_fill_task`/`FlowBuilder::fill` over
//! a borrowed slice; [`Runtime::fill_n`] is the `_n` sibling, upstream's
//! `make_fill_n_task`/`FlowBuilder::fill_n`. [`make_fill_task`] and
//! [`make_fill_n_task`] are those same two over owned data, and the
//! wrappers one line over each -- [`AlgorithmBuilder::fill`] and
//! [`AlgorithmBuilder::fill_n`], on the one extension trait the whole
//! algorithm family shares. `DESIGN.md`'s "Three layers, not two forms" entry
//! says why the port needs all three where upstream needs
//! two.
//!
//! [`AlgorithmBuilder::fill`]: crate::algorithm::AlgorithmBuilder::fill
//! [`AlgorithmBuilder::fill_n`]: crate::algorithm::AlgorithmBuilder::fill_n
//!
//! # Design
//!
//! **The chunk body writes `value.clone()`, matching upstream's own
//! per-element copy-assignment.** Upstream's inner loop is `*beg++ = value;`
//! -- a copy-assignment into every slot from one captured `value`, repeated
//! once per element. `*slot = value.clone()` is the direct equivalent, not a
//! simplification of it: `T: Clone + Sync` lets every worker clone the same
//! shared `value` concurrently, with no need to give each worker its own
//! up-front copy first.
//!
//! **`fill_n`'s `count` is a slice length, not an unchecked iterator
//! advance.** Upstream takes `count` on faith -- advancing an iterator
//! `count` times past where a container actually ends is undefined
//! behaviour the caller is on the hook for. [`Runtime::fill_n`] instead
//! slices `out` to `&mut out[..count]` and defers to [`Runtime::fill`]: a
//! `count` the destination cannot hold is a clean, loud panic at the slice
//! index, not a silent out-of-bounds write. The same holds for
//! [`crate::Runtime::generate_n`].
//!
//! **No `N == 0` special case needed beyond the one upstream writes.**
//! Upstream's `fill`/`fill_n` both check `N == 0` explicitly before
//! `N <= part.chunk_size()`; this port keeps that check too, even though
//! `0 <= chunk_size()` already holds for every chunk size, because the
//! explicit branch is what upstream's own structure reads as and a reader
//! comparing the two should see the same shape.

use std::sync::{Arc, Mutex};

use crate::algorithm::SlicePartitioner;
use crate::runtime::Runtime;

impl<'rt> Runtime<'rt> {
    /// Writes `value.clone()` into every element of `out`, in parallel
    /// chunks decided by `part`: upstream's `make_fill_task`, called
    /// directly rather than building a second closure.
    ///
    /// ```
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let mut out = [0u32; 10];
    ///     runtime.fill(
    ///         &mut out,
    ///         12,
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(out, [12; 10]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn fill<T, P>(&mut self, out: &mut [T], value: T, part: P)
    where
        T: Clone + Send + Sync,
        P: SlicePartitioner,
    {
        let n = out.len();
        if n == 0 {
            return;
        }

        let w_total = self.num_workers();
        if w_total <= 1 || n <= part.chunk_size() {
            part.call(|| {
                for slot in out.iter_mut() {
                    *slot = value.clone();
                }
            });
            return;
        }
        let w_total = w_total.min(n);

        let apply = |chunk: &mut [T]| {
            for slot in chunk.iter_mut() {
                *slot = value.clone();
            }
        };

        self.scope(|s| {
            part.drive(s, out, n, w_total, &apply);
        });
    }

    /// Writes `value.clone()` into the first `count` elements of `out`,
    /// leaving the rest untouched: upstream's `make_fill_n_task`.
    ///
    /// # Panics
    ///
    /// If `count > out.len()`: see this module's `# Design` section for why
    /// that is a slice-index panic here rather than upstream's unchecked
    /// iterator advance.
    ///
    /// ```
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let mut out = [0u32; 10];
    ///     runtime.fill_n(
    ///         &mut out,
    ///         4,
    ///         12,
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(out, [12, 12, 12, 12, 0, 0, 0, 0, 0, 0]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn fill_n<T, P>(&mut self, out: &mut [T], count: usize, value: T, part: P)
    where
        T: Clone + Send + Sync,
        P: SlicePartitioner,
    {
        self.fill(&mut out[..count], value, part);
    }
}

/// Builds the runtime body of a [`Runtime::fill`] task over owned `out`:
/// upstream's `make_fill_task`. `out` is locked once per run of the body,
/// not once per element, and the whole slice is filled through that one
/// guard.
///
/// `value` and `part` are cloned on every run of the body -- relevant only
/// for a task a cycle revisits -- matching upstream copying `value` and
/// `part` into its own closure by value.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::fill::make_fill_task;
/// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let out = Arc::new(Mutex::new(vec![0; 5]));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_fill_task(
///     Arc::clone(&out),
///     7,
///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(*out.lock().expect("uncontended"), vec![7; 5]);
/// ```
pub fn make_fill_task<T, P>(
    out: Arc<Mutex<Vec<T>>>,
    value: T,
    part: P,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Clone + Send + Sync + 'static,
    P: SlicePartitioner + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let mut guard = out
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.fill(&mut guard, value.clone(), part.clone());
    }
}

/// Builds the runtime body of a [`Runtime::fill_n`] task over owned `out`:
/// upstream's `make_fill_n_task`. A `count` past the end of `out` panics
/// when the body runs, for the reason this module's `# Design` section
/// gives.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::fill::make_fill_n_task;
/// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let out = Arc::new(Mutex::new(vec![0; 5]));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_fill_n_task(
///     Arc::clone(&out),
///     3,
///     7,
///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(*out.lock().expect("uncontended"), vec![7, 7, 7, 0, 0]);
/// ```
pub fn make_fill_n_task<T, P>(
    out: Arc<Mutex<Vec<T>>>,
    count: usize,
    value: T,
    part: P,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Clone + Send + Sync + 'static,
    P: SlicePartitioner + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let mut guard = out
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.fill_n(&mut guard, count, value.clone(), part.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::algorithm::partitioner::{
        DefaultClosureWrapper, DynamicPartitioner, GuidedPartitioner, RandomPartitioner,
        StaticPartitioner,
    };
    use crate::prelude::*;
    use crate::{Executor, Runtime};

    /// `N == 0` is a no-op, upstream's first short-circuit.
    #[test]
    fn empty_slice_is_a_no_op() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let mut out: [u32; 0] = [];
            runtime.fill(
                &mut out,
                1,
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
        });
        Executor::new(2).run(&taskflow).wait().expect("no panic");
    }

    /// Every element ends up `value`, across every partitioner and a sweep
    /// of worker counts, chunk sizes and lengths: `ParallelFill.*`.
    #[test]
    fn every_element_is_filled_exactly_once() {
        for workers in 1..=5 {
            for chunk_size in [0usize, 1, 3, 7, 23] {
                for len in [0usize, 1, 2, 7, 37, 150] {
                    check_fill(
                        workers,
                        len,
                        StaticPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    check_fill(
                        workers,
                        len,
                        GuidedPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    check_fill(
                        workers,
                        len,
                        DynamicPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    check_fill(
                        workers,
                        len,
                        RandomPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                }
            }
        }
    }

    fn check_fill<P>(workers: usize, len: usize, part: P)
    where
        P: SlicePartitioner + Send + 'static,
    {
        let mut taskflow = Taskflow::new();
        let mut part = Some(part);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let part = part.take().expect("this task runs once");
            let mut out = vec![-1i64; len];
            runtime.fill(&mut out, 12, part);
            assert!(out.iter().all(|&v| v == 12), "every element of {len}");
        });
        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }

    /// `fill_n` touches only the first `count` elements, leaving the rest at
    /// their initial value: `ParallelFillN.*`.
    #[test]
    fn fill_n_touches_only_the_requested_prefix() {
        for workers in 1..=4 {
            for len in [0usize, 1, 10, 100] {
                let count = len / 2;
                let mut taskflow = Taskflow::new();
                taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                    let mut out = vec![0i64; len];
                    runtime.fill_n(
                        &mut out,
                        count,
                        12,
                        GuidedPartitioner::<DefaultClosureWrapper>::default(),
                    );
                    assert!(out[..count].iter().all(|&v| v == 12));
                    assert!(out[count..].iter().all(|&v| v == 0));
                });
                Executor::new(workers)
                    .run(&taskflow)
                    .wait()
                    .expect("no task panicked");
            }
        }
    }

    /// `fill_n` with `count` past the slice panics rather than writing out
    /// of bounds: this module's `# Design` section.
    #[test]
    #[should_panic(expected = "range end index")]
    fn fill_n_panics_when_count_exceeds_the_slice() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let mut out = vec![0i64; 4];
            runtime.fill_n(
                &mut out,
                5,
                12,
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
        });
        Executor::new(1).run(&taskflow).wait().expect("no panic");
    }

    /// A borrowed-slice stand-in for upstream's `ParallelFillStateful`: the
    /// slice is computed by the same runtime body, from state an earlier
    /// task set up.
    #[test]
    fn stateful_fill_reads_a_slice_computed_by_an_earlier_task() {
        for workers in 1..=4u32 {
            let mut taskflow = Taskflow::new();
            let backing: Arc<std::sync::Mutex<Vec<i64>>> =
                Arc::new(std::sync::Mutex::new(Vec::new()));

            let setup = {
                let backing = Arc::clone(&backing);
                taskflow.emplace(move || {
                    *backing.lock().expect("uncontended") = vec![0; 10000];
                })
            };

            let runtime_task = {
                let backing = Arc::clone(&backing);
                taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                    let mut guard = backing.lock().expect("uncontended");
                    runtime.fill(
                        &mut guard,
                        12,
                        GuidedPartitioner::<DefaultClosureWrapper>::default(),
                    );
                })
            };

            taskflow.task(setup).precede([runtime_task]);

            Executor::new(workers as usize)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");

            let guard = backing.lock().expect("uncontended");
            assert!(guard.iter().all(|&v| v == 12));
        }
    }

    /// The closure wrapper is applied once per worker task over that
    /// worker's whole share, not once per element.
    #[test]
    fn closure_wrapper_runs_once_per_worker_for_the_static_partitioner() {
        for workers in 1..8usize {
            let wrapper_called = Arc::new(AtomicUsize::new(0));
            let mut taskflow = Taskflow::new();

            let wrapper_called_body = Arc::clone(&wrapper_called);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let mut out = vec![0u32; 1000];
                let wrapper_called = Arc::clone(&wrapper_called_body);
                let wrapper = move |task: &mut dyn FnMut()| {
                    wrapper_called.fetch_add(1, Ordering::Relaxed);
                    task();
                };
                runtime.fill(
                    &mut out,
                    1,
                    StaticPartitioner::with_closure_wrapper(1, wrapper),
                );
                assert!(out.iter().all(|&v| v == 1));
            });

            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");

            assert_eq!(wrapper_called.load(Ordering::Relaxed), workers);
        }
    }
}

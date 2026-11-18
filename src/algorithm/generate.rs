//! Translated from `taskflow/algorithm/generate.hpp` (Taskflow v4.1.0).
//!
//! [`Runtime::generate`] is upstream's `make_generate_task`/
//! `FlowBuilder::generate` over a borrowed slice; [`Runtime::generate_n`] is
//! the `_n` sibling, upstream's `make_generate_n_task`/
//! `FlowBuilder::generate_n`. [`make_generate_task`] and
//! [`make_generate_n_task`] are those same two over owned data, and the
//! wrappers one line over each -- [`AlgorithmBuilder::generate`] and
//! [`AlgorithmBuilder::generate_n`], on the one extension trait the whole
//! algorithm family shares. `DESIGN.md`'s "Three layers, not two forms" entry
//! says why the port needs all three where upstream needs
//! two.
//!
//! [`AlgorithmBuilder::generate`]: crate::algorithm::AlgorithmBuilder::generate
//! [`AlgorithmBuilder::generate_n`]: crate::algorithm::AlgorithmBuilder::generate_n
//!
//! # Design
//!
//! **One generator behind a `Mutex`, not upstream's copy per worker.**
//! Upstream's `[=]` capture copies `generator` into every per-worker task closure
//! from the *same* pristine value, so a stateful generator -- a counter, say
//! -- restarts from the same point on every worker rather than advancing
//! through one shared sequence; only a generator whose output does not
//! depend on how many times it has already run, which is what every case in
//! `unittests/test_generate.cpp` happens to pass, is unaffected by the
//! difference. [`Runtime::generate`] instead locks one shared `Mutex<G>`
//! for the duration of each chunk and calls `generator` under it: every element
//! still gets a distinct call, but of one evolving generator, which is
//! closer to what a caller reading the name `generate` over `fill` expects
//! in the first place. The cost is real -- every chunk, on every worker,
//! now serializes through one lock around the calls to `generator` itself (not
//! around writing the result, which still goes straight to that worker's
//! own slice) -- and it is upstream's own design that avoids paying it, at
//! the price of the duplicated-state behaviour above.
//!
//! **No explicit `N == 0` branch**, matching upstream's own
//! `make_generate_task`/`make_generate_n_task`: unlike `fill.hpp`, upstream
//! writes no separate check here, because `N <= part.chunk_size()` already
//! holds whenever `N == 0`, which routes it into the sequential branch like
//! every other small input. [`Runtime::fill`] keeps upstream's explicit
//! check because `fill.hpp` has one; this file has none because
//! `generate.hpp` has none.
//!
//! **`generate_n`'s `count` is a slice length**, for the same reason
//! [`crate::Runtime::fill_n`]'s is -- see that module's
//! `# Design` section.

use std::sync::{Arc, Mutex};

use crate::algorithm::SlicePartitioner;
use crate::runtime::Runtime;

impl<'rt> Runtime<'rt> {
    /// Writes `generator()` into every element of `out`, in parallel chunks
    /// decided by `part`: upstream's `make_generate_task`, called directly
    /// rather than building a second closure. See this module's `# Design`
    /// section for how the one `generator` this method owns is shared, rather
    /// than copied, across the workers that call it.
    ///
    /// ```
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let mut out = [0u32; 10];
    ///     runtime.generate(
    ///         &mut out,
    ///         || 7,
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(out, [7; 10]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn generate<T, G, P>(&mut self, out: &mut [T], mut generator: G, part: P)
    where
        T: Send,
        G: FnMut() -> T + Send,
        P: SlicePartitioner,
    {
        let n = out.len();

        let w_total = self.num_workers();
        if w_total <= 1 || n <= part.chunk_size() {
            part.call(|| {
                for slot in out.iter_mut() {
                    *slot = generator();
                }
            });
            return;
        }
        let w_total = w_total.min(n);

        let generator = Mutex::new(generator);
        let apply = |chunk: &mut [T]| {
            let mut guard = generator
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for slot in chunk.iter_mut() {
                *slot = (*guard)();
            }
        };

        self.scope(|s| {
            part.drive(s, out, n, w_total, &apply);
        });
    }

    /// Writes `generator()` into the first `count` elements of `out`, leaving the
    /// rest untouched: upstream's `make_generate_n_task`.
    ///
    /// # Panics
    ///
    /// If `count > out.len()`: see
    /// [`crate::Runtime::fill_n`]'s `# Design` section for
    /// why that is a slice-index panic here rather than upstream's
    /// unchecked iterator advance.
    ///
    /// ```
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let mut out = [0u32; 10];
    ///     runtime.generate_n(
    ///         &mut out,
    ///         4,
    ///         || 7,
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(out, [7, 7, 7, 7, 0, 0, 0, 0, 0, 0]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn generate_n<T, G, P>(&mut self, out: &mut [T], count: usize, generator: G, part: P)
    where
        T: Send,
        G: FnMut() -> T + Send,
        P: SlicePartitioner,
    {
        self.generate(&mut out[..count], generator, part);
    }
}

/// Builds the runtime body of a [`Runtime::generate`] task over owned
/// `out`: upstream's `make_generate_task`. `out` is locked once per run of
/// the body, not once per element, and the whole slice is generated through
/// that one guard.
///
/// `generator` and `part` are cloned on every run of the body -- relevant
/// only for a task a cycle revisits -- matching upstream copying `gen` and
/// `part` into its own closure by value. Within one run, the clone is the
/// single generator every worker shares, which is this module's `# Design`
/// section's departure from upstream.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::generate::make_generate_task;
/// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let out = Arc::new(Mutex::new(vec![0; 5]));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_generate_task(
///     Arc::clone(&out),
///     || 9,
///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(*out.lock().expect("uncontended"), vec![9; 5]);
/// ```
pub fn make_generate_task<T, G, P>(
    out: Arc<Mutex<Vec<T>>>,
    generator: G,
    part: P,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Send + 'static,
    G: FnMut() -> T + Send + Clone + 'static,
    P: SlicePartitioner + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let mut guard = out
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.generate(&mut guard, generator.clone(), part.clone());
    }
}

/// Builds the runtime body of a [`Runtime::generate_n`] task over owned
/// `out`: upstream's `make_generate_n_task`. A `count` past the end of `out`
/// panics when the body runs, for the reason
/// [`crate::Runtime::fill_n`]'s `# Design` section gives.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::generate::make_generate_n_task;
/// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let out = Arc::new(Mutex::new(vec![0; 5]));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_generate_n_task(
///     Arc::clone(&out),
///     3,
///     || 9,
///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(*out.lock().expect("uncontended"), vec![9, 9, 9, 0, 0]);
/// ```
pub fn make_generate_n_task<T, G, P>(
    out: Arc<Mutex<Vec<T>>>,
    count: usize,
    generator: G,
    part: P,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Send + 'static,
    G: FnMut() -> T + Send + Clone + 'static,
    P: SlicePartitioner + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let mut guard = out
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.generate_n(&mut guard, count, generator.clone(), part.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    use super::*;
    use crate::algorithm::partitioner::{
        DefaultClosureWrapper, DynamicPartitioner, GuidedPartitioner, RandomPartitioner,
        StaticPartitioner,
    };
    use crate::prelude::*;
    use crate::{Executor, Runtime};

    /// `N == 0` is a no-op, matching upstream's implicit handling (this
    /// module's `# Design` section).
    #[test]
    fn empty_slice_is_a_no_op() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let mut out: [u32; 0] = [];
            runtime.generate(
                &mut out,
                || unreachable!(),
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
        });
        Executor::new(2).run(&taskflow).wait().expect("no panic");
    }

    /// Every element is generated exactly once, across every partitioner and
    /// a sweep of worker counts, chunk sizes and lengths: `ParallelGenerate.*`.
    #[test]
    fn every_element_is_generated_exactly_once() {
        for workers in 1..=5 {
            for chunk_size in [0usize, 1, 3, 7, 23] {
                for len in [0usize, 1, 2, 7, 37, 150] {
                    check_generate(
                        workers,
                        len,
                        StaticPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    check_generate(
                        workers,
                        len,
                        GuidedPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    check_generate(
                        workers,
                        len,
                        DynamicPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    check_generate(
                        workers,
                        len,
                        RandomPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                }
            }
        }
    }

    fn check_generate<P>(workers: usize, len: usize, part: P)
    where
        P: SlicePartitioner + Send + 'static,
    {
        let mut taskflow = Taskflow::new();
        let mut part = Some(part);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let part = part.take().expect("this task runs once");
            let mut out = vec![-1i64; len];
            runtime.generate(&mut out, || 12, part);
            assert!(out.iter().all(|&v| v == 12), "every element of {len}");
        });
        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
    }

    /// `generate_n` touches only the first `count` elements, leaving the
    /// rest at their initial value: `ParallelGenerateN.*`.
    #[test]
    fn generate_n_touches_only_the_requested_prefix() {
        for workers in 1..=4 {
            for len in [0usize, 1, 10, 100] {
                let count = len / 2;
                let mut taskflow = Taskflow::new();
                taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                    let mut out = vec![0i64; len];
                    runtime.generate_n(
                        &mut out,
                        count,
                        || 12,
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

    /// `generate_n` with `count` past the slice panics rather than writing
    /// out of bounds.
    #[test]
    #[should_panic(expected = "range end index")]
    fn generate_n_panics_when_count_exceeds_the_slice() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let mut out = vec![0i64; 4];
            runtime.generate_n(
                &mut out,
                5,
                || 12,
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
        });
        Executor::new(1).run(&taskflow).wait().expect("no panic");
    }

    /// Every slot is visited by a distinct call of one shared, evolving
    /// generator rather than a duplicated-per-worker copy: the behaviour
    /// this module's `# Design` section chose over upstream's.
    #[test]
    fn generate_calls_a_single_shared_generator_once_per_element() {
        for workers in 1..=6usize {
            let mut taskflow = Taskflow::new();
            let next = Arc::new(AtomicI64::new(0));
            let calls = Arc::new(AtomicUsize::new(0));
            let next_body = Arc::clone(&next);
            let calls_body = Arc::clone(&calls);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let mut out = vec![-1i64; 500];
                let next_body = Arc::clone(&next_body);
                let calls_body = Arc::clone(&calls_body);
                runtime.generate(
                    &mut out,
                    move || {
                        calls_body.fetch_add(1, Ordering::Relaxed);
                        next_body.fetch_add(1, Ordering::Relaxed)
                    },
                    GuidedPartitioner::<DefaultClosureWrapper>::default(),
                );

                let mut seen = out.clone();
                seen.sort_unstable();
                seen.dedup();
                assert_eq!(seen.len(), out.len(), "no two slots see the same call");
            });

            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");

            assert_eq!(calls.load(Ordering::Relaxed), 500);
        }
    }

    /// A borrowed-slice stand-in for upstream's `ParallelGenerateStateful`:
    /// the slice is computed by the same runtime body, from state an
    /// earlier task set up.
    #[test]
    fn stateful_generate_reads_a_slice_computed_by_an_earlier_task() {
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
                    runtime.generate(
                        &mut guard,
                        || 12,
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
}

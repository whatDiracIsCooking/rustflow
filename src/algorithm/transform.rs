//! Translated from `taskflow/algorithm/transform.hpp` (Taskflow v4.1.0).
//!
//! [`Runtime::transform`] is the one-input half of upstream's overloaded
//! `make_transform_task`/`FlowBuilder::transform`; [`Runtime::transform2`] is
//! the two-input half, upstream's `requires (!PartitionerLike<...>)` overload
//! on the same two names. Rust has no overload resolution, so the two keep
//! upstream's own test-suite names (`ParallelTransform` vs
//! `ParallelTransform2`) rather than sharing one.
//!
//! The owned-data wrappers over every method this module,
//! [`fill`](super::fill) and [`generate`](super::generate) add are on
//! [`AlgorithmBuilder`], the one extension trait the whole algorithm family
//! shares.
//!
//! [`AlgorithmBuilder`]: crate::algorithm::AlgorithmBuilder
//!
//! # Design
//!
//! **Borrowed slices, not iterator pairs** -- for the same reason
//! [`for_each`](super::for_each)'s own `# Design` section gives: a slice a
//! caller computes in the same runtime body gets upstream's lazy-fetch
//! behaviour for free, with no second task and no `std::ref` equivalent
//! needed. The chunk loop itself is likewise
//! [`crate::algorithm::SlicePartitioner::drive`], not repeated here.
//!
//! **A chunk is a slice of paired references, not an index into two
//! slices.** [`SlicePartitioner::drive`] only knows how to split *one*
//! `&mut [T]`; [`Runtime::transform`] has a read-only input slice and a
//! mutable output slice that must be split at the same boundaries. Rather
//! than add a second entry point to `drive` for two slices (and a third for
//! [`Runtime::transform2`]'s three), both methods first zip their slices
//! into one `Vec` of `(&A, &mut B)` pairs -- `(&A, &B, &mut C)` triples for
//! `transform2` -- and hand `drive` *that* to split: a pair is `Send`
//! whenever `A: Sync` and `B: Send`, which is exactly the bound a chunk
//! handed to another thread already needs, so no new capability is spent on
//! the zip. The extra `Vec` is `n` pointer-sized tuples, paid once per call,
//! not per element.
//!
//! **`fill_n`/`generate_n`'s `count` panics past the slice, rather than
//! being upstream's unchecked iterator advance.** See
//! [`fill`](super::fill)'s own `# Design` section.

use crate::algorithm::SlicePartitioner;
use crate::runtime::Runtime;

impl<'rt> Runtime<'rt> {
    /// Writes `f(&input[i])` into `out[i]` for every `i`, in parallel chunks
    /// decided by `part`: upstream's one-input `make_transform_task`, called
    /// directly rather than building a second closure.
    ///
    /// # Panics
    ///
    /// If `input.len() != out.len()`.
    ///
    /// ```
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let input = [1, 2, 3, 4, 5];
    ///     let mut out = [0; 5];
    ///     runtime.transform(
    ///         &input,
    ///         &mut out,
    ///         |x: &i32| x * 2,
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(out, [2, 4, 6, 8, 10]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn transform<A, B, F, P>(&mut self, input: &[A], out: &mut [B], f: F, part: P)
    where
        A: Sync,
        B: Send,
        F: Fn(&A) -> B + Sync,
        P: SlicePartitioner,
    {
        assert_eq!(
            input.len(),
            out.len(),
            "transform's input and output slices must have the same length"
        );
        let n = out.len();
        if n == 0 {
            return;
        }

        let w_total = self.num_workers();
        let mut paired: Vec<(&A, &mut B)> = input.iter().zip(out.iter_mut()).collect();
        if w_total <= 1 || n <= part.chunk_size() {
            part.call(|| {
                for pair in &mut paired {
                    *pair.1 = f(pair.0);
                }
            });
            return;
        }
        let w_total = w_total.min(n);

        let apply = |chunk: &mut [(&A, &mut B)]| {
            for pair in chunk.iter_mut() {
                *pair.1 = f(pair.0);
            }
        };

        self.scope(|s| {
            part.drive(s, paired.as_mut_slice(), n, w_total, &apply);
        });
    }

    /// Writes `f(&a[i], &b[i])` into `out[i]` for every `i`: upstream's
    /// two-input `make_transform_task` overload, named `transform2` here --
    /// upstream's own test suite calls it `ParallelTransform2` for the same
    /// reason this port needs a second name: Rust has no overload
    /// resolution to pick it by arity.
    ///
    /// # Panics
    ///
    /// If `a.len() != out.len()` or `b.len() != out.len()`.
    ///
    /// ```
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let a = [1, 2, 3, 4];
    ///     let b = [10, 20, 30, 40];
    ///     let mut out = [0; 4];
    ///     runtime.transform2(
    ///         &a,
    ///         &b,
    ///         &mut out,
    ///         |x: &i32, y: &i32| x + y,
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(out, [11, 22, 33, 44]);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn transform2<A, B, C, F, P>(&mut self, a: &[A], b: &[B], out: &mut [C], f: F, part: P)
    where
        A: Sync,
        B: Sync,
        C: Send,
        F: Fn(&A, &B) -> C + Sync,
        P: SlicePartitioner,
    {
        assert_eq!(
            a.len(),
            out.len(),
            "transform2's first input and output slices must have the same length"
        );
        assert_eq!(
            b.len(),
            out.len(),
            "transform2's second input and output slices must have the same length"
        );
        let n = out.len();
        if n == 0 {
            return;
        }

        let w_total = self.num_workers();
        let mut triples: Vec<(&A, &B, &mut C)> = a
            .iter()
            .zip(b.iter())
            .zip(out.iter_mut())
            .map(|((x, y), z)| (x, y, z))
            .collect();
        if w_total <= 1 || n <= part.chunk_size() {
            part.call(|| {
                for t in &mut triples {
                    *t.2 = f(t.0, t.1);
                }
            });
            return;
        }
        let w_total = w_total.min(n);

        let apply = |chunk: &mut [(&A, &B, &mut C)]| {
            for t in chunk.iter_mut() {
                *t.2 = f(t.0, t.1);
            }
        };

        self.scope(|s| {
            part.drive(s, triples.as_mut_slice(), n, w_total, &apply);
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::algorithm::partitioner::{
        DefaultClosureWrapper, DynamicPartitioner, GuidedPartitioner, RandomPartitioner,
        StaticPartitioner,
    };
    use crate::prelude::*;
    use crate::{Executor, Runtime};

    /// `N == 0` is a no-op for `transform`, upstream's first short-circuit.
    #[test]
    fn empty_slices_are_a_no_op_for_transform() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let input: [i32; 0] = [];
            let mut out: [i32; 0] = [];
            runtime.transform(
                &input,
                &mut out,
                |_: &i32| unreachable!(),
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
        });
        Executor::new(2).run(&taskflow).wait().expect("no panic");
    }

    /// Every output is `f(input[i])` exactly once, across every partitioner
    /// and a sweep of worker counts and chunk sizes: `ParallelTransform.*`.
    #[test]
    fn every_element_is_transformed_exactly_once() {
        for workers in 1..=5 {
            for chunk_size in [0usize, 1, 3, 7, 23] {
                for len in [0usize, 1, 2, 7, 37, 150] {
                    check_transform(
                        workers,
                        len,
                        StaticPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    check_transform(
                        workers,
                        len,
                        GuidedPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    check_transform(
                        workers,
                        len,
                        DynamicPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    check_transform(
                        workers,
                        len,
                        RandomPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                }
            }
        }
    }

    fn check_transform<P>(workers: usize, len: usize, part: P)
    where
        P: SlicePartitioner + Send + 'static,
    {
        let mut taskflow = Taskflow::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_body = Arc::clone(&counter);
        let mut part = Some(part);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let part = part.take().expect("this task runs once");
            let input: Vec<i64> = (0..i64::try_from(len).expect("len fits in an i64")).collect();
            let mut out = vec![-1i64; len];
            runtime.transform(
                &input,
                &mut out,
                |x: &i64| {
                    counter_body.fetch_add(1, Ordering::Relaxed);
                    x * 2
                },
                part,
            );
            assert!(out.iter().enumerate().all(|(i, &v)| v == 2 * input[i]));
        });
        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert_eq!(counter.load(Ordering::Relaxed), len);
    }

    /// `N == 0` is a no-op for `transform2` too.
    #[test]
    fn empty_slices_are_a_no_op_for_transform2() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let a: [i32; 0] = [];
            let b: [i32; 0] = [];
            let mut out: [i32; 0] = [];
            runtime.transform2(
                &a,
                &b,
                &mut out,
                |_: &i32, _: &i32| unreachable!(),
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
        });
        Executor::new(2).run(&taskflow).wait().expect("no panic");
    }

    /// `transform2`'s two inputs are read element-by-element in lockstep:
    /// `ParallelTransform2.*`.
    #[test]
    fn transform2_combines_both_inputs_elementwise() {
        for workers in 1..=4 {
            for len in [0usize, 1, 7, 100] {
                let mut taskflow = Taskflow::new();
                taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                    let a: Vec<i64> =
                        (0..i64::try_from(len).expect("len fits in an i64")).collect();
                    let b: Vec<i64> = a.iter().map(|x| x * 10).collect();
                    let mut out = vec![0i64; len];
                    runtime.transform2(
                        &a,
                        &b,
                        &mut out,
                        |x: &i64, y: &i64| x + y,
                        GuidedPartitioner::<DefaultClosureWrapper>::default(),
                    );
                    assert!(out.iter().enumerate().all(|(i, &v)| v == a[i] + b[i]));
                });
                Executor::new(workers)
                    .run(&taskflow)
                    .wait()
                    .expect("no task panicked");
            }
        }
    }

    /// A borrowed-slice stand-in for upstream's stateful transform cases, the
    /// same shape `for_each.rs`'s own stateful test uses: the slices are
    /// computed by the *same* runtime body, from state an earlier task set
    /// up.
    #[test]
    fn stateful_transform_reads_slices_computed_by_an_earlier_task() {
        for workers in 1..=4u32 {
            let mut taskflow = Taskflow::new();
            let src: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
            let dst: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));

            let setup = {
                let src = Arc::clone(&src);
                let dst = Arc::clone(&dst);
                taskflow.emplace(move || {
                    *src.lock().expect("uncontended") = (0..37).collect();
                    *dst.lock().expect("uncontended") = vec![-1; 37];
                })
            };

            let runtime_task = {
                let src = Arc::clone(&src);
                let dst = Arc::clone(&dst);
                taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                    let src_guard = src.lock().expect("uncontended");
                    let mut dst_guard = dst.lock().expect("uncontended");
                    runtime.transform(
                        &src_guard,
                        &mut dst_guard,
                        |x: &i64| x + 100,
                        GuidedPartitioner::<DefaultClosureWrapper>::default(),
                    );
                })
            };

            taskflow.task(setup).precede([runtime_task]);

            Executor::new(workers as usize)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");

            let src_guard = src.lock().expect("uncontended");
            let dst_guard = dst.lock().expect("uncontended");
            assert!(
                dst_guard
                    .iter()
                    .enumerate()
                    .all(|(i, &v)| v == src_guard[i] + 100)
            );
        }
    }

    /// The closure wrapper is applied once per worker task over that
    /// worker's whole share, not once per element: `ClosureWrapper.transform.Static`.
    #[test]
    fn closure_wrapper_runs_once_per_worker_for_the_static_partitioner() {
        for workers in 1..8usize {
            let wrapper_called = Arc::new(AtomicUsize::new(0));
            let touched = Arc::new(AtomicUsize::new(0));
            let mut taskflow = Taskflow::new();

            let wrapper_called_body = Arc::clone(&wrapper_called);
            let touched_body = Arc::clone(&touched);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let input = vec![0i32; 1000];
                let mut out = vec![0i32; 1000];
                let wrapper_called = Arc::clone(&wrapper_called_body);
                let wrapper = move |task: &mut dyn FnMut()| {
                    wrapper_called.fetch_add(1, Ordering::Relaxed);
                    task();
                };
                let touched = Arc::clone(&touched_body);
                runtime.transform(
                    &input,
                    &mut out,
                    move |_: &i32| {
                        touched.fetch_add(1, Ordering::Relaxed);
                        1
                    },
                    StaticPartitioner::with_closure_wrapper(1, wrapper),
                );
                assert!(out.iter().all(|&v| v == 1));
            });

            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");

            assert_eq!(touched.load(Ordering::Relaxed), 1000);
            assert_eq!(wrapper_called.load(Ordering::Relaxed), workers);
        }
    }

    /// The dynamic family's wrapper also runs once per worker *task*, but
    /// may spawn fewer tasks than there are workers:
    /// `ClosureWrapper.transform.Dynamic`.
    #[test]
    fn closure_wrapper_runs_at_most_once_per_worker_for_the_dynamic_partitioner() {
        for workers in 1..8usize {
            let wrapper_called = Arc::new(AtomicUsize::new(0));
            let touched = Arc::new(AtomicUsize::new(0));
            let mut taskflow = Taskflow::new();

            let wrapper_called_body = Arc::clone(&wrapper_called);
            let touched_body = Arc::clone(&touched);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let input = vec![0i32; 1000];
                let mut out = vec![0i32; 1000];
                let wrapper_called = Arc::clone(&wrapper_called_body);
                let wrapper = move |task: &mut dyn FnMut()| {
                    wrapper_called.fetch_add(1, Ordering::Relaxed);
                    task();
                };
                let touched = Arc::clone(&touched_body);
                runtime.transform(
                    &input,
                    &mut out,
                    move |_: &i32| {
                        touched.fetch_add(1, Ordering::Relaxed);
                        1
                    },
                    DynamicPartitioner::with_closure_wrapper(1, wrapper),
                );
                assert!(out.iter().all(|&v| v == 1));
            });

            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");

            assert_eq!(touched.load(Ordering::Relaxed), 1000);
            assert!(wrapper_called.load(Ordering::Relaxed) <= workers);
        }
    }

    /// The owned-data wrapper, reached through `rustflow::prelude::*` alone
    /// -- no direct `AlgorithmBuilder` import -- proving
    /// `taskflow.fill(...)` resolves the way the module docs claim.
    #[test]
    fn flow_builder_wrappers_resolve_through_the_prelude_alone() {
        let data = Arc::new(Mutex::new(vec![0u32; 64]));
        let mut taskflow = Taskflow::new();
        taskflow.fill(
            Arc::clone(&data),
            9,
            GuidedPartitioner::<DefaultClosureWrapper>::default(),
        );

        Executor::new(3).run(&taskflow).wait().expect("no panic");

        assert!(data.lock().expect("uncontended").iter().all(|&v| v == 9));
    }
}

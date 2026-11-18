//! Translated from `taskflow/algorithm/for_each.hpp` (Taskflow v4.1.0).
//!
//! The slice half of `make_for_each_task` and `FlowBuilder::for_each` --
//! `make_for_each_index_task`/`for_each_index` and
//! `make_for_each_by_index_task`/`for_each_by_index` are a separate port,
//! not here.
//!
//! [`Runtime::for_each`] is the runtime-task body upstream builds as a
//! closure over `(b, e, c, part)`; [`make_for_each_task`] is upstream's
//! factory of the same name, over owned data; and
//! [`AlgorithmBuilder::for_each`] is the one line over that factory --
//! `DESIGN.md`'s "Decisions taken in M10" entry on the three layers says
//! why the port needs all three where upstream needs two.
//!
//! [`AlgorithmBuilder::for_each`]: crate::algorithm::AlgorithmBuilder::for_each
//!
//! # Design
//!
//! **A borrowed slice, not an iterator pair.** Upstream's `b`/`e` are a
//! generic iterator pair, copied into the returned closure and re-read
//! every time the runtime task body runs -- which is also how its
//! "stateful" tests pass `std::ref(beg)`/`std::ref(end)` and have the body
//! see whatever `beg`/`end` hold *when the task runs*, set by an earlier
//! task in the same graph. [`Runtime::for_each`] takes `data: &mut [T]`
//! instead, which gets the identical lazy-fetch behaviour for free: a
//! caller that needs to compute the slice first does so in the same runtime
//! body, before calling this method, rather than in an earlier graph task
//! connected by an edge. `tests/for_each.rs`'s stateful cases are exactly
//! that shape.
//!
//! **The chunk loop itself lives in `algorithm::mod`.** Splitting `data`
//! and spawning one child per chunk under [`Runtime::scope`] is
//! [`crate::algorithm::SlicePartitioner::drive`], in the parent module
//! rather than here -- its own `# Design` section says why, and names the
//! five issues that are expected to share its `pub(crate)` helpers.
//!
//! **`Arc<Mutex<Vec<T>>>`, not `Arc<[T]>` + `Fn(&T)`.** The owned-data
//! wrapper exists so a body that does not already have a `&mut [T]` to hand
//! [`Runtime::for_each`] still gets one: a `Mutex` is what turns the `Arc`'s
//! shared ownership back into the exclusive `&mut` access the method
//! needs, locked once per invocation rather than once per element.

use std::sync::{Arc, Mutex};

use crate::algorithm::SlicePartitioner;
use crate::runtime::Runtime;

impl<'rt> Runtime<'rt> {
    /// Runs `f` over every element of `data`, in parallel chunks decided by
    /// `part`: upstream's `make_for_each_task`, called directly rather than
    /// building a second closure, since the runtime task body *is* this
    /// call for a caller that already has one.
    ///
    /// `N == 0` is a no-op; `W <= 1` or `data.len() <= part.chunk_size()`
    /// runs `f` over every element right here, under `part`'s closure
    /// wrapper, with no [`Runtime::scope`] at all; otherwise `data` is split
    /// into chunks and run under one, each worker's whole share wrapped
    /// once -- this module's `# Design` section, and the parent module's,
    /// say how.
    ///
    /// ```
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
    /// use rustflow::prelude::*;
    /// use rustflow::Executor;
    ///
    /// let mut taskflow = Taskflow::new();
    /// taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
    ///     let mut data = [0u32; 10];
    ///     let counter = AtomicUsize::new(0);
    ///     runtime.for_each(
    ///         &mut data,
    ///         |x: &mut u32| {
    ///             *x = 1;
    ///             counter.fetch_add(1, Ordering::Relaxed);
    ///         },
    ///         GuidedPartitioner::<DefaultClosureWrapper>::default(),
    ///     );
    ///     assert_eq!(counter.load(Ordering::Relaxed), 10);
    ///     assert_eq!(data.iter().sum::<u32>(), 10);
    /// });
    ///
    /// Executor::new(4).run(&taskflow).wait().expect("no task panicked");
    /// ```
    pub fn for_each<T, F, P>(&mut self, data: &mut [T], f: F, part: P)
    where
        T: Send,
        F: Fn(&mut T) + Sync,
        P: SlicePartitioner,
    {
        let n = data.len();
        if n == 0 {
            return;
        }

        let w_total = self.num_workers();
        if w_total <= 1 || n <= part.chunk_size() {
            part.call(|| {
                for item in data.iter_mut() {
                    f(item);
                }
            });
            return;
        }
        let w_total = w_total.min(n);

        let apply = |chunk: &mut [T]| {
            for item in chunk.iter_mut() {
                f(item);
            }
        };

        self.scope(|s| {
            part.drive(s, data, n, w_total, &apply);
        });
    }
}

/// Builds the runtime body of a [`Runtime::for_each`] task over owned
/// `data`: upstream's `make_for_each_task`. `data` is locked once per run of
/// the body, not once per element, and the whole slice is run through that
/// one guard.
///
/// `f` and `part` are cloned on every run of the body -- relevant only for a
/// task a cycle revisits -- matching upstream copying `c` and `part` into its
/// own closure by value.
///
/// ```
/// use std::sync::{Arc, Mutex};
///
/// use rustflow::algorithm::for_each::make_for_each_task;
/// use rustflow::algorithm::partitioner::{DefaultClosureWrapper, GuidedPartitioner};
/// use rustflow::prelude::*;
/// use rustflow::Executor;
///
/// let data = Arc::new(Mutex::new(vec![0u32; 10]));
///
/// let mut taskflow = Taskflow::new();
/// taskflow.emplace_runtime(make_for_each_task(
///     Arc::clone(&data),
///     |x: &mut u32| *x = 1,
///     GuidedPartitioner::<DefaultClosureWrapper>::default(),
/// ));
///
/// Executor::new(2).run(&taskflow).wait().expect("no task panicked");
/// assert_eq!(data.lock().expect("uncontended").iter().sum::<u32>(), 10);
/// ```
pub fn make_for_each_task<T, F, P>(
    data: Arc<Mutex<Vec<T>>>,
    f: F,
    part: P,
) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static
where
    T: Send + 'static,
    F: Fn(&mut T) + Sync + Clone + Send + 'static,
    P: SlicePartitioner + Clone + Send + 'static,
{
    move |rt: &mut Runtime<'_>| {
        let mut guard = data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rt.for_each(&mut guard, f.clone(), part.clone());
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

    /// `N == 0` is a no-op for every partitioner, upstream's first
    /// short-circuit.
    #[test]
    fn empty_slice_is_a_no_op() {
        let mut taskflow = Taskflow::new();
        taskflow.emplace_runtime(|runtime: &mut Runtime<'_>| {
            let mut data: [u32; 0] = [];
            runtime.for_each(
                &mut data,
                |_: &mut u32| unreachable!(),
                GuidedPartitioner::<DefaultClosureWrapper>::default(),
            );
        });
        Executor::new(2).run(&taskflow).wait().expect("no panic");
    }

    /// Every element is visited exactly once, across every partitioner and a
    /// sweep of worker counts and chunk sizes: `ParallelFor.*`.
    #[test]
    fn every_element_is_visited_exactly_once() {
        for workers in 1..=5 {
            for chunk_size in [0usize, 1, 3, 7, 23] {
                for len in [0usize, 1, 2, 7, 37, 150] {
                    check_coverage(
                        workers,
                        len,
                        StaticPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    check_coverage(
                        workers,
                        len,
                        GuidedPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    check_coverage(
                        workers,
                        len,
                        DynamicPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                    check_coverage(
                        workers,
                        len,
                        RandomPartitioner::<DefaultClosureWrapper>::new(chunk_size),
                    );
                }
            }
        }
    }

    fn check_coverage<P>(workers: usize, len: usize, part: P)
    where
        P: SlicePartitioner + Send + 'static,
    {
        let mut taskflow = Taskflow::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_body = Arc::clone(&counter);
        // `emplace_runtime` wants `FnMut`, so the captured `part` is taken
        // out of an `Option` rather than moved directly -- this task never
        // runs twice, but the closure's type still has to allow it.
        let mut part = Some(part);
        taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
            let part = part.take().expect("this task runs once");
            let mut data = vec![-1i64; len];
            runtime.for_each(
                &mut data,
                |slot: &mut i64| {
                    counter_body.fetch_add(1, Ordering::Relaxed);
                    *slot += 1;
                },
                part,
            );
            assert!(
                data.iter().all(|&v| v == 0),
                "every element was touched exactly once"
            );
        });
        Executor::new(workers)
            .run(&taskflow)
            .wait()
            .expect("no task panicked");
        assert_eq!(counter.load(Ordering::Relaxed), len);
    }

    /// A borrowed-slice stand-in for upstream's `StatefulParallelFor`: the
    /// slice is computed by the *same* runtime body, from state an earlier
    /// task in the graph set up, rather than read via `std::ref` from a
    /// pair of iterators a second task mutates -- this module's `# Design`
    /// section says why that is the direct equivalent here.
    #[test]
    fn stateful_for_each_reads_a_slice_computed_by_an_earlier_task() {
        for workers in 1..=4u32 {
            let mut taskflow = Taskflow::new();
            let backing: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
            let half = Arc::new(Mutex::new(0usize));

            let setup = {
                let backing = Arc::clone(&backing);
                let half = Arc::clone(&half);
                taskflow.emplace(move || {
                    let n = 37;
                    *backing.lock().expect("uncontended") = vec![-1; n];
                    *half.lock().expect("uncontended") = n / 2;
                })
            };

            let counter = Arc::new(AtomicUsize::new(0));
            let runtime_task = {
                let backing = Arc::clone(&backing);
                let half = Arc::clone(&half);
                let counter = Arc::clone(&counter);
                taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                    let mut guard = backing.lock().expect("uncontended");
                    let at = *half.lock().expect("uncontended");
                    let (left, _right) = guard.split_at_mut(at);
                    runtime.for_each(
                        left,
                        |slot: &mut i64| {
                            counter.fetch_add(1, Ordering::Relaxed);
                            *slot = 8;
                        },
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
            let at = *half.lock().expect("uncontended");
            assert_eq!(counter.load(Ordering::Relaxed), at);
            assert!(guard[..at].iter().all(|&v| v == 8));
            assert!(guard[at..].iter().all(|&v| v == -1));
        }
    }

    /// The closure wrapper is applied exactly once per worker task, over
    /// that worker's *whole* share, not once per element:
    /// `ClosureWrapper.for_each.Static`.
    #[test]
    fn closure_wrapper_runs_once_per_worker_for_the_static_partitioner() {
        for workers in 1..8usize {
            let wrapper_called = Arc::new(AtomicUsize::new(0));
            let touched = Arc::new(AtomicUsize::new(0));
            let mut taskflow = Taskflow::new();

            let wrapper_called_body = Arc::clone(&wrapper_called);
            let touched_body = Arc::clone(&touched);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let mut data = vec![0u32; 1000];
                let wrapper_called = Arc::clone(&wrapper_called_body);
                let wrapper = move |task: &mut dyn FnMut()| {
                    wrapper_called.fetch_add(1, Ordering::Relaxed);
                    task();
                };
                runtime.for_each(
                    &mut data,
                    |x: &mut u32| {
                        touched_body.fetch_add(1, Ordering::Relaxed);
                        *x = 1;
                    },
                    StaticPartitioner::with_closure_wrapper(1, wrapper),
                );
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
    /// `ClosureWrapper.for_each.Dynamic`.
    #[test]
    fn closure_wrapper_runs_at_most_once_per_worker_for_the_dynamic_partitioner() {
        for workers in 1..8usize {
            let wrapper_called = Arc::new(AtomicUsize::new(0));
            let touched = Arc::new(AtomicUsize::new(0));
            let mut taskflow = Taskflow::new();

            let wrapper_called_body = Arc::clone(&wrapper_called);
            let touched_body = Arc::clone(&touched);
            taskflow.emplace_runtime(move |runtime: &mut Runtime<'_>| {
                let mut data = vec![0u32; 1000];
                let wrapper_called = Arc::clone(&wrapper_called_body);
                let wrapper = move |task: &mut dyn FnMut()| {
                    wrapper_called.fetch_add(1, Ordering::Relaxed);
                    task();
                };
                runtime.for_each(
                    &mut data,
                    |x: &mut u32| {
                        touched_body.fetch_add(1, Ordering::Relaxed);
                        *x = 1;
                    },
                    DynamicPartitioner::with_closure_wrapper(1, wrapper),
                );
            });

            Executor::new(workers)
                .run(&taskflow)
                .wait()
                .expect("no task panicked");

            assert_eq!(touched.load(Ordering::Relaxed), 1000);
            assert!(wrapper_called.load(Ordering::Relaxed) <= workers);
        }
    }

    /// The owned-data wrapper: upstream's plain, non-stateful `for_each`
    /// over a `tf::Taskflow::for_each` call.
    #[test]
    fn flow_builder_for_each_locks_and_runs_over_owned_data() {
        let data = Arc::new(Mutex::new(vec![0u32; 64]));
        let mut taskflow = Taskflow::new();
        taskflow.for_each(
            Arc::clone(&data),
            |x: &mut u32| *x += 1,
            GuidedPartitioner::<DefaultClosureWrapper>::default(),
        );

        Executor::new(3).run(&taskflow).wait().expect("no panic");

        assert!(data.lock().expect("uncontended").iter().all(|&v| v == 1));
    }
}

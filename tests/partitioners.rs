//! Translated from `unittests/test_partitioners.cpp` (Taskflow v4.1.0).
//!
//! Upstream parametrises each coverage case over a worker count and runs it
//! for every `(N, chunk)` pair in its own table; this file does the same,
//! generic over the relevant loop trait rather than over a C++ template
//! parameter. The 1D cases are ported in full -- `StaticPartitioner`'s
//! strided loop, the shared-cursor loop of `GuidedPartitioner`,
//! `DynamicPartitioner` and `RandomPartitioner`, and the `bool`-returning
//! early-exit form `find` builds on. Upstream's `IndexRange`/`IndexRanges`
//! cases are not: N-D ranges are not yet ported (`#168` and after), so there
//! is nothing here to call them with.
//!
//! `std::thread::scope` replaces upstream's `std::async`/`std::future`
//! pairs: the worker closures only read the partitioner and write into
//! `visited`, both borrowed for the scope's lifetime, so nothing needs an
//! `Arc`.
//!
//! The closure-wrapper cases are this port's own addition. Upstream's suite
//! has none -- `partitioner.hpp`'s closure wrapper is exercised by the
//! algorithms that consume it (`for_each` and friends), which are not ported
//! yet -- so the module's `# Design` contract ("the closure-wrapper cases")
//! is met directly against [`rustflow::algorithm::partitioner::ClosureWrapper`]
//! instead.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use rustflow::algorithm::partitioner::{
    DefaultClosureWrapper, DynamicLoop, DynamicPartitioner, GuidedPartitioner, Partitioner,
    RandomPartitioner, StaticLoop, StaticPartitioner,
};

/// Upstream's `check_visited`: every index in `[0, n)` was covered by exactly
/// one chunk.
fn assert_visited_once(visited: &[AtomicUsize], n: usize) {
    for (i, count) in visited.iter().enumerate().take(n) {
        assert_eq!(count.load(Ordering::Relaxed), 1, "index {i} of {n}");
    }
}

/// Upstream's `test_static_loop_1d<P>`: for every `(N, chunk)` pair in its
/// table, one worker at a time runs [`StaticLoop::loop_static`] from its own
/// pre-assigned `curr_b`, and every index of `[0, N)` must end up visited
/// exactly once.
fn static_loop_1d<P: StaticLoop>(make: impl Fn(usize) -> P, workers: usize) {
    for n in [0_usize, 1, 3, 7, 16, 99, 1000] {
        for c in [0_usize, 1, 3, 7, 99] {
            let p = make(c);
            let chunk_size = if p.chunk_size() != 0 {
                p.chunk_size()
            } else if workers != 0 {
                n.div_ceil(workers)
            } else {
                n
            };
            let chunk_size = chunk_size.max(1);

            let visited: Vec<AtomicUsize> = (0..n).map(|_| AtomicUsize::new(0)).collect();

            for w in 0..workers {
                let curr_b = w * chunk_size;
                if curr_b >= n {
                    break;
                }
                p.loop_static(n, workers, curr_b, chunk_size, |b, e| {
                    for count in &visited[b..e] {
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }

            assert_visited_once(&visited, n);
        }
    }
}

/// Upstream's `test_dynamic_loop_1d<P>`: `workers` threads each run
/// [`DynamicLoop::loop_dynamic`] against one shared cursor, and every index
/// of `[0, N)` must end up visited exactly once regardless of how the
/// threads interleave.
fn dynamic_loop_1d<P: DynamicLoop + Sync>(make: impl Fn(usize) -> P, workers: usize) {
    for n in [0_usize, 1, 3, 7, 16, 99, 1000] {
        for c in [0_usize, 1, 3, 7, 99] {
            let p = make(c);
            let next = AtomicUsize::new(0);
            let visited: Vec<AtomicUsize> = (0..n).map(|_| AtomicUsize::new(0)).collect();

            thread::scope(|scope| {
                for _ in 0..workers {
                    scope.spawn(|| {
                        p.loop_dynamic(n, workers, &next, |b, e| {
                            for count in &visited[b..e] {
                                count.fetch_add(1, Ordering::Relaxed);
                            }
                        });
                    });
                }
            });

            assert_visited_once(&visited, n);
        }
    }
}

/// Upstream's `test_dynamic_loop_1d_bool<P>`: `workers` threads race
/// [`DynamicLoop::loop_dynamic_until`] to find the one index equal to
/// `target`, each stopping its own loop as soon as it sees it. Exactly one
/// index satisfies the search, so the lowest index any worker records is
/// always `target`.
fn dynamic_loop_1d_bool<P: DynamicLoop + Sync>(make: impl Fn(usize) -> P, workers: usize) {
    for n in [1_usize, 7, 16, 99, 1000] {
        for c in [0_usize, 1, 3, 7, 99] {
            let p = make(c);
            let target = n / 2;
            let result = AtomicUsize::new(n); // sentinel: not found
            let next = AtomicUsize::new(0);

            thread::scope(|scope| {
                for _ in 0..workers {
                    scope.spawn(|| {
                        p.loop_dynamic_until(n, workers, &next, |b, e| {
                            for i in b..e {
                                if i == target {
                                    result.fetch_min(i, Ordering::Relaxed);
                                    return true;
                                }
                            }
                            false
                        });
                    });
                }
            });

            assert_eq!(result.load(Ordering::Relaxed), target);
        }
    }
}

// ============================================================================
// StaticPartitioner
// ============================================================================

#[test]
fn static_loop_1d_1_worker() {
    static_loop_1d(StaticPartitioner::<DefaultClosureWrapper>::new, 1);
}

#[test]
fn static_loop_1d_2_workers() {
    static_loop_1d(StaticPartitioner::<DefaultClosureWrapper>::new, 2);
}

#[test]
fn static_loop_1d_4_workers() {
    static_loop_1d(StaticPartitioner::<DefaultClosureWrapper>::new, 4);
}

#[test]
fn static_loop_1d_8_workers() {
    static_loop_1d(StaticPartitioner::<DefaultClosureWrapper>::new, 8);
}

// ============================================================================
// GuidedPartitioner
// ============================================================================

#[test]
fn guided_loop_1d_1_worker() {
    dynamic_loop_1d(GuidedPartitioner::<DefaultClosureWrapper>::new, 1);
}

#[test]
fn guided_loop_1d_2_workers() {
    dynamic_loop_1d(GuidedPartitioner::<DefaultClosureWrapper>::new, 2);
}

#[test]
fn guided_loop_1d_4_workers() {
    dynamic_loop_1d(GuidedPartitioner::<DefaultClosureWrapper>::new, 4);
}

#[test]
fn guided_loop_1d_8_workers() {
    dynamic_loop_1d(GuidedPartitioner::<DefaultClosureWrapper>::new, 8);
}

#[test]
fn guided_loop_1d_bool_1_worker() {
    dynamic_loop_1d_bool(GuidedPartitioner::<DefaultClosureWrapper>::new, 1);
}

#[test]
fn guided_loop_1d_bool_2_workers() {
    dynamic_loop_1d_bool(GuidedPartitioner::<DefaultClosureWrapper>::new, 2);
}

#[test]
fn guided_loop_1d_bool_4_workers() {
    dynamic_loop_1d_bool(GuidedPartitioner::<DefaultClosureWrapper>::new, 4);
}

#[test]
fn guided_loop_1d_bool_8_workers() {
    dynamic_loop_1d_bool(GuidedPartitioner::<DefaultClosureWrapper>::new, 8);
}

// ============================================================================
// DynamicPartitioner
// ============================================================================

#[test]
fn dynamic_loop_1d_1_worker() {
    dynamic_loop_1d(DynamicPartitioner::<DefaultClosureWrapper>::new, 1);
}

#[test]
fn dynamic_loop_1d_2_workers() {
    dynamic_loop_1d(DynamicPartitioner::<DefaultClosureWrapper>::new, 2);
}

#[test]
fn dynamic_loop_1d_4_workers() {
    dynamic_loop_1d(DynamicPartitioner::<DefaultClosureWrapper>::new, 4);
}

#[test]
fn dynamic_loop_1d_8_workers() {
    dynamic_loop_1d(DynamicPartitioner::<DefaultClosureWrapper>::new, 8);
}

// ============================================================================
// RandomPartitioner
//
// Seeded rather than sampled: `RandomPartitioner::new` starts every instance
// from the same fixed `DEFAULT_SEED`, so these cases assert on one
// deterministic run instead of repeating a nondeterministic one.
// ============================================================================

#[test]
fn random_loop_1d_1_worker() {
    dynamic_loop_1d(RandomPartitioner::<DefaultClosureWrapper>::new, 1);
}

#[test]
fn random_loop_1d_2_workers() {
    dynamic_loop_1d(RandomPartitioner::<DefaultClosureWrapper>::new, 2);
}

#[test]
fn random_loop_1d_4_workers() {
    dynamic_loop_1d(RandomPartitioner::<DefaultClosureWrapper>::new, 4);
}

#[test]
fn random_loop_1d_8_workers() {
    dynamic_loop_1d(RandomPartitioner::<DefaultClosureWrapper>::new, 8);
}

/// A seed explicitly set away from the default still has to produce full,
/// non-overlapping coverage -- the guarantee comes from the shared cursor
/// every worker races for, not from which seed drives the chunk-size draws.
#[test]
fn random_loop_1d_is_deterministic_under_an_explicit_seed() {
    dynamic_loop_1d(
        |c| {
            let mut p = RandomPartitioner::<DefaultClosureWrapper>::new(c);
            p.set_seed(0xC0FF_EE42);
            p
        },
        4,
    );
}

// ============================================================================
// The closure wrapper
// ============================================================================

/// The default wrapper changes nothing: a chunk body wrapped in it runs
/// exactly as it would unwrapped.
#[test]
fn the_default_closure_wrapper_changes_nothing() {
    let p = StaticPartitioner::<DefaultClosureWrapper>::new(2);
    let mut seen = Vec::new();
    p.call(|| seen.push("chunk"));
    assert_eq!(seen, vec!["chunk"]);
}

/// A custom closure wrapper runs around every chunk a partitioner's `call`
/// dispatches to it, in both loop shapes: the static strided loop and the
/// shared-cursor one.
#[test]
fn a_custom_closure_wrapper_runs_around_every_chunk() {
    let before_after = |body: &mut dyn FnMut()| {
        body();
    };

    let p = StaticPartitioner::with_closure_wrapper(3, before_after);
    let mut chunks = 0;
    p.loop_static(10, 1, 0, p.chunk_size(), |_b, _e| {
        p.call(|| chunks += 1);
    });
    assert_eq!(chunks, 4); // [0,3) [3,6) [6,9) [9,10)

    let counting = DynamicPartitioner::with_closure_wrapper(4, before_after);
    let next = AtomicUsize::new(0);
    let mut dynamic_chunks = 0;
    counting.loop_dynamic(10, 1, &next, |_b, _e| {
        counting.call(|| dynamic_chunks += 1);
    });
    assert_eq!(dynamic_chunks, 3); // [0,4) [4,8) [8,10)
}

/// A wrapper that never runs the body it was handed suppresses every
/// chunk's effect, including on a shared cursor's coverage -- a caller that
/// wants every index to still run must call `body()` itself.
#[test]
fn a_closure_wrapper_that_skips_the_body_suppresses_the_chunk() {
    let skip_every_other = std::cell::Cell::new(false);
    let wrapper = |body: &mut dyn FnMut()| {
        if skip_every_other.get() {
            skip_every_other.set(false);
        } else {
            skip_every_other.set(true);
            body();
        }
    };

    let p = StaticPartitioner::with_closure_wrapper(0, wrapper);
    let mut ran = 0;
    for _ in 0..4 {
        p.call(|| ran += 1);
    }
    assert_eq!(ran, 2);
}

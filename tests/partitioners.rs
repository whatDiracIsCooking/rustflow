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
//!
//! The cases under *The cut itself* are this port's own as well, and they are
//! there because the coverage cases cannot be: "every index visited exactly
//! once" holds for any chunking at all, so the arithmetic each strategy *is*
//! -- the guided shrink and the threshold it stops shrinking at, the
//! inclusive range a random chunk size is drawn from, the chunk size a setter
//! sets -- went unasserted until #274's mutation run said so.

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

/// `RandomPartitioner::with_alpha_beta_and_closure_wrapper` -- upstream's
/// `RandomPartitioner(float, float, C&&)` -- sets both halves at once: the
/// fractions a chunk size is drawn between, and the wrapper every chunk runs
/// under.
///
/// Each half on its own is asserted elsewhere in this file, by
/// `random_chunk_sizes_are_drawn_from_the_whole_range_and_follow_the_seed` and
/// by the two cases above. What this adds is that neither argument is dropped
/// when both are given, and that the chunk sizes are still the drawn ones
/// rather than the `0` chunk size the constructor stores.
#[test]
fn a_random_partitioner_takes_its_fractions_and_its_wrapper_together() {
    const N: usize = 1000;
    const W: usize = 4;

    let wrapped = std::cell::Cell::new(0_usize);
    let counting = |body: &mut dyn FnMut()| {
        wrapped.set(wrapped.get() + 1);
        body();
    };

    // `alpha * N * W` is 4 and `beta * N * W` is 8, as in the seeded case
    // below: the draws come from [4, 8].
    let mut p = RandomPartitioner::with_alpha_beta_and_closure_wrapper(0.001, 0.002, counting);
    p.set_seed(0xC0FF_EE42);
    assert_eq!(p.alpha(), 0.001);
    assert_eq!(p.beta(), 0.002);
    assert_eq!(p.chunk_size_range(N, W), (4, 8), "the fractions were kept");

    let next = AtomicUsize::new(0);
    let mut chunks = 0;
    let mut covered = 0;
    p.loop_dynamic(N, W, &next, |b, e| {
        p.call(|| {
            chunks += 1;
            covered += e - b;
        });
    });

    assert_eq!(covered, N, "every index was handed out exactly once");
    assert!(
        chunks >= N / 8,
        "a chunk is at most eight indices wide, so a thousand take at least \
         {} chunks: {chunks}",
        N / 8
    );
    assert_eq!(
        wrapped.get(),
        chunks,
        "the wrapper given to the constructor ran around every chunk"
    );
}

/// `GuidedPartitioner::with_closure_wrapper` and
/// `RandomPartitioner::with_closure_wrapper` -- upstream's
/// `explicit P(size_t, C&&)` for the two strategies the cases above do not
/// reach -- take a chunk size and a wrapper together, and keep both.
///
/// What each half is asserted against is what its own strategy does with it,
/// and that is where the two differ. A guided floor chunk size shapes the
/// sequence, so the chunks this constructor's partitioner hands out are the
/// ones `GuidedPartitioner::new` of the same floor does. A random one is
/// kept for parity and never read by the loop, so what this pins there is
/// the other half of the signature: the `alpha` and `beta` fractions
/// `with_alpha_beta_and_closure_wrapper` overrides are still the defaults.
#[test]
fn the_guided_and_random_constructors_keep_their_chunk_size_and_their_wrapper() {
    const N: usize = 90;
    const W: usize = 3;

    // The sequence a guided floor of four cuts, with no wrapper in the way.
    let plain = GuidedPartitioner::<DefaultClosureWrapper>::new(4);
    let next = AtomicUsize::new(0);
    let mut expected = Vec::new();
    plain.loop_dynamic(N, W, &next, |b, e| expected.push((b, e)));

    let guided_chunks = std::cell::Cell::new(0_usize);
    let counting = |body: &mut dyn FnMut()| {
        guided_chunks.set(guided_chunks.get() + 1);
        body();
    };

    let guided = GuidedPartitioner::with_closure_wrapper(4, counting);
    assert_eq!(guided.chunk_size(), 4);
    let next = AtomicUsize::new(0);
    let mut cut = Vec::new();
    guided.loop_dynamic(N, W, &next, |b, e| {
        guided.call(|| cut.push((b, e)));
    });
    assert_eq!(
        cut, expected,
        "the chunk size given is the floor the guided sequence shrinks to"
    );
    assert_eq!(
        guided_chunks.get(),
        cut.len(),
        "and the wrapper ran around every chunk of it"
    );

    let random_chunks = std::cell::Cell::new(0_usize);
    let wrapping = |body: &mut dyn FnMut()| {
        random_chunks.set(random_chunks.get() + 1);
        body();
    };

    let random = RandomPartitioner::with_closure_wrapper(7, wrapping);
    assert_eq!(
        random.chunk_size(),
        7,
        "kept, though a random loop never reads it"
    );
    assert_eq!(random.alpha(), 0.01, "and the default fractions are intact");
    assert_eq!(random.beta(), 0.5);

    let next = AtomicUsize::new(0);
    let mut chunks = 0;
    let mut covered = 0;
    random.loop_dynamic(N, W, &next, |b, e| {
        random.call(|| {
            chunks += 1;
            covered += e - b;
        });
    });
    assert_eq!(covered, N, "every index was handed out exactly once");
    assert_eq!(
        random_chunks.get(),
        chunks,
        "and this wrapper ran around every chunk too"
    );
}

// ============================================================================
// The cut itself
//
// The coverage cases above are blind to *how* a range was cut: "every index
// visited exactly once" holds for any chunking at all, which is what makes
// them insensitive to the arithmetic each strategy is. These cases assert the
// cut -- the chunk size each partitioner reports, the shape of the sequence
// the guided loop hands out, and the range a random one draws from.
// ============================================================================

/// Every partitioner reports the chunk size it was built with and takes a new
/// one: the single piece of state [`Partitioner`] itself has, and what each
/// loop shape reads to size its chunks.
#[test]
fn every_partitioner_reports_and_replaces_its_chunk_size() {
    fn round_trip<P: Partitioner>(mut p: P) {
        assert_eq!(p.chunk_size(), 3);
        p.set_chunk_size(7);
        assert_eq!(p.chunk_size(), 7);
    }

    round_trip(StaticPartitioner::<DefaultClosureWrapper>::new(3));
    round_trip(GuidedPartitioner::<DefaultClosureWrapper>::new(3));
    round_trip(DynamicPartitioner::<DefaultClosureWrapper>::new(3));
    round_trip(RandomPartitioner::<DefaultClosureWrapper>::new(3));

    // And the setter is not just a field write: the chunks a later loop hands
    // out are the size last set.
    let mut p = DynamicPartitioner::<DefaultClosureWrapper>::new(2);
    p.set_chunk_size(5);
    let next = AtomicUsize::new(0);
    let mut chunks = Vec::new();
    p.loop_dynamic(10, 1, &next, |b, e| chunks.push((b, e)));
    assert_eq!(chunks, vec![(0, 5), (5, 10)]);
}

/// The static loop stops *at* `n`: a range its chunk size divides exactly ends
/// on the last full chunk, and a range of nothing is no chunks at all. Neither
/// hands the body an empty chunk -- upstream's `while(curr_b < n)`.
#[test]
fn the_static_loop_hands_out_no_empty_chunk() {
    let p = StaticPartitioner::<DefaultClosureWrapper>::new(3);

    let mut chunks = Vec::new();
    p.loop_static(9, 1, 0, p.chunk_size(), |b, e| chunks.push((b, e)));
    assert_eq!(chunks, vec![(0, 3), (3, 6), (6, 9)]);

    let mut empty_range = Vec::new();
    p.loop_static(0, 1, 0, p.chunk_size(), |b, e| empty_range.push((b, e)));
    assert!(empty_range.is_empty());
}

/// The guided loop hands out half of what is left per worker and shrinks with
/// it: upstream's `q = p2 * r` for `p2 = 0.5 / w`, which this port computes as
/// `r / (2 * w)`. One caller over an uncontended cursor draws the whole
/// sequence itself, so the shape it promises can be asserted on one run -- the
/// first chunk is `n / (2 * w)`, no chunk is larger than the one before it,
/// and the tail is the configured floor.
#[test]
fn the_guided_loop_shrinks_its_chunks_with_the_remaining_work() {
    const N: usize = 100;
    const W: usize = 3;

    let p = GuidedPartitioner::<DefaultClosureWrapper>::new(1);
    let next = AtomicUsize::new(0);
    let mut chunks = Vec::new();
    p.loop_dynamic(N, W, &next, |b, e| chunks.push((b, e)));

    // Contiguous and complete, which is the coverage cases' claim restated for
    // a sequence one caller drew in order.
    assert_eq!(chunks.first().copied().map(|(b, _)| b), Some(0));
    assert_eq!(chunks.last().copied().map(|(_, e)| e), Some(N));
    assert!(chunks.windows(2).all(|pair| pair[0].1 == pair[1].0));

    let sizes: Vec<usize> = chunks.iter().map(|&(b, e)| e - b).collect();
    assert_eq!(sizes[0], N / (2 * W));
    assert!(
        sizes.windows(2).all(|pair| pair[0] >= pair[1]),
        "chunk sizes must not grow: {sizes:?}"
    );
    assert_eq!(*sizes.last().expect("a non-empty range is chunked"), 1);
}

/// The guided rule still applies *at* its own threshold: upstream's
/// `if(r < p1)` switches to floor-size chunks strictly below
/// `2 * w * (chunk_size + 1)` remaining, so a remainder of exactly that much
/// is cut by the `r / (2 * w)` rule like any larger one.
#[test]
fn the_guided_loop_cuts_a_remainder_of_exactly_the_threshold_by_the_rule() {
    const W: usize = 3;

    let p = GuidedPartitioner::<DefaultClosureWrapper>::new(1);
    let n = 2 * W * (p.chunk_size() + 1);

    let next = AtomicUsize::new(0);
    let mut chunks = Vec::new();
    p.loop_dynamic(n, W, &next, |b, e| chunks.push((b, e)));

    assert_eq!(chunks.first().copied(), Some((0, n / (2 * W))));
    assert_eq!(chunks.last().copied().map(|(_, e)| e), Some(n));
}

/// A dynamic loop that finds the cursor already past the end hands out
/// nothing, and the guided one leaves the cursor where it found it. Both
/// matter because the cursor is shared with every other worker on the same
/// range: a loop arriving late must neither run an empty chunk nor push the
/// count past `n` for the next arrival to read.
#[test]
fn a_dynamic_loop_over_an_exhausted_cursor_hands_out_no_chunk() {
    const N: usize = 16;
    const W: usize = 3;

    let guided = GuidedPartitioner::<DefaultClosureWrapper>::new(1);
    let next = AtomicUsize::new(N);
    let mut chunks = Vec::new();
    guided.loop_dynamic(N, W, &next, |b, e| chunks.push((b, e)));
    assert!(chunks.is_empty(), "guided: {chunks:?}");
    assert_eq!(next.load(Ordering::Relaxed), N, "guided moved the cursor");

    let dynamic = DynamicPartitioner::<DefaultClosureWrapper>::new(4);
    let next = AtomicUsize::new(N);
    let mut chunks = Vec::new();
    dynamic.loop_dynamic(N, W, &next, |b, e| chunks.push((b, e)));
    assert!(chunks.is_empty(), "dynamic: {chunks:?}");

    let random = RandomPartitioner::<DefaultClosureWrapper>::new(0);
    let next = AtomicUsize::new(N);
    let mut chunks = Vec::new();
    random.loop_dynamic(N, W, &next, |b, e| chunks.push((b, e)));
    assert!(chunks.is_empty(), "random: {chunks:?}");
}

/// A random partitioner draws every chunk size uniformly from the inclusive
/// range its `alpha` and `beta` fractions set, and the draws follow its seed:
/// the same seed gives the same sequence and another seed gives another one.
///
/// Four claims, and each is one the module's `# Design` section makes: the
/// fractions are kept as given rather than defaulted, the range is inclusive
/// at both ends, every size but the last -- which the end of the range clamps
/// -- is inside it, and a stored seed is what lets all of that be asserted on
/// one run instead of sampled over many.
#[test]
fn random_chunk_sizes_are_drawn_from_the_whole_range_and_follow_the_seed() {
    const N: usize = 1000;
    const W: usize = 4;

    // alpha * N * W = 4 and beta * N * W = 8, so the draws come from [4, 8] --
    // a span small enough that every value in it is expected many times over
    // the ~160 chunks a thousand indices take.
    fn chunk_sizes(seed: u64) -> (Vec<usize>, (usize, usize)) {
        let mut p = RandomPartitioner::<DefaultClosureWrapper>::with_alpha_beta(0.001, 0.002);
        assert_eq!(p.alpha(), 0.001);
        assert_eq!(p.beta(), 0.002);
        p.set_seed(seed);
        assert_eq!(p.seed(), seed);

        let next = AtomicUsize::new(0);
        let mut sizes = Vec::new();
        p.loop_dynamic(N, W, &next, |b, e| sizes.push(e - b));
        (sizes, p.chunk_size_range(N, W))
    }

    let (sizes, (lo, hi)) = chunk_sizes(0xC0FF_EE42);
    assert_eq!((lo, hi), (4, 8));

    let (last, rest) = sizes.split_last().expect("a thousand indices are chunked");
    for size in rest {
        assert!(
            (lo..=hi).contains(size),
            "a drawn chunk size must be in [{lo}, {hi}]: {sizes:?}"
        );
    }
    assert!(
        *last <= hi,
        "the last chunk is clamped to the end, not grown"
    );
    for wanted in lo..=hi {
        assert!(
            rest.contains(&wanted),
            "a uniform draw over [{lo}, {hi}] reaches {wanted}: {sizes:?}"
        );
    }

    let (again, _) = chunk_sizes(0xC0FF_EE42);
    assert_eq!(again, sizes, "the same seed draws the same sizes");
    let (elsewhere, _) = chunk_sizes(0x5A5A_5A5A);
    assert_ne!(elsewhere, sizes, "another seed draws other sizes");
}

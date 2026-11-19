//! Property tests for the algorithm family: random inputs, every
//! partitioner, the worker counts the integration suites sweep, and `std`'s
//! own answer to the same question as the oracle (#269).
//!
//! Upstream has no equivalent suite -- its algorithm tests are the fixed
//! `(N, chunk)` tables `tests/reduce.rs`, `tests/find.rs` and the rest
//! translate -- so nothing here is a port, and the file carries no
//! provenance line. What it adds over those tables is the input *shape*: a
//! length, a chunk size, a partitioner family and a worker count drawn per
//! case rather than listed, so a boundary nobody thought to tabulate (a
//! chunk size larger than the range, a negative step whose last index lands
//! exactly on `end`) is reached by generation instead of by hand.
//!
//! # What the oracle can and cannot be
//!
//! Most of these algorithms are deterministic: a scan, a merge, a `find_if`
//! and a sort all have exactly one right answer, so the property is plain
//! equality with the `std` computation of it. Three are not, and the
//! assertions below are the strongest true statement rather than the
//! strongest wished-for one:
//!
//! - **`reduce`/`transform_reduce` do not promise left-to-right order.**
//!   `src/algorithm/reduce.rs`'s `# Design` section says each worker folds
//!   its own share into a local and merges that local into a shared pool
//!   *under a lock*, so with a non-commutative `bop` the order the locals
//!   combine in is decided by the race, not by the indices. String
//!   concatenation is still the right `bop` to generate over -- it makes
//!   every combination visible, where `+` on integers would hide a
//!   duplicated or dropped chunk behind arithmetic -- so the property is
//!   that every element is combined exactly once, with `init` as the
//!   leading operand, plus exact equality with `Iterator::fold` on the
//!   single-chunk path the docs do pin (`W <= 1` or `n <= chunk_size`).
//! - **`sort` is not stable.** `Runtime::sort_by` ends in
//!   `[T]::sort_unstable_by` at the leaves, and the partition above it
//!   reorders equal keys freely, so no case here checks the order of equal
//!   elements. The permutation check on the tags replaces it: the keys must
//!   match `[T]::sort_by`'s, and the elements must still be the ones that
//!   went in.
//! - **`min_element`/`max_element` return an index, and ties are not
//!   ordered.** The locals merge under the same lock `reduce`'s do, so the
//!   case checks the *value* at the index against `Iterator::min`/`max`.
//!
//! # Design
//!
//! **One executor per worker count, shared by every case.** A generated
//! case is cheap and there are thousands of them, where spawning and
//! joining a pool is neither; [`executor`] therefore hands out four
//! process-lifetime executors instead of building one per case, which is
//! also why a case that needs its own pool size (`Executor::new(0)` panics,
//! an observer-carrying pool) does not belong in this file. Several cases
//! submit to the same executor at once, since `cargo test` runs the test
//! functions below in parallel -- that is the external-submission path the
//! crate already supports, not a special arrangement.
//!
//! **Worker counts are drawn, not written out.** The suites that translate
//! an upstream `TEST_CASE` use `tests/common`'s `worker_matrix!` so that
//! each count keeps its own test name and fails on its own; a generated
//! case has no name to keep, and the count that broke is in the shrunk
//! input proptest prints. So the matrix appears here as the [`WORKERS`]
//! range the macro's entries spell out, and `tests/common` is not declared.

use std::ops::RangeInclusive;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::thread;

use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;
use rustflow::Executor;
use rustflow::algorithm::index_range::{IndexRange, IndexRanges};
use rustflow::algorithm::partitioner::{
    DefaultClosureWrapper, DynamicLoop, DynamicPartitioner, GuidedPartitioner, RandomPartitioner,
    StaticLoop, StaticPartitioner,
};
use rustflow::prelude::*;

/// The input lengths every case draws over: #269's `0..=256`, which spans
/// the empty input, the single-chunk short-circuits every algorithm here
/// has, and -- for an element wide enough to land in the smallest sort
/// bucket -- the parallel sort path above its 128-element cutoff.
const LENGTHS: RangeInclusive<usize> = 0..=256;

/// The worker counts `tests/common`'s `worker_matrix!` entries sweep in the
/// algorithm suites, as a strategy; this file's `# Design` section says why
/// it is a range here rather than that macro.
const WORKERS: RangeInclusive<usize> = 1..=4;

/// Chunk sizes drawn per case, wide enough to include `0` (ask the
/// partitioner to work one out) and sizes past the longest input (one chunk
/// for the whole range).
const CHUNKS: RangeInclusive<usize> = 0..=300;

/// Element values drawn per case, a range narrow enough that ties, runs and
/// repeated keys are common rather than vanishingly unlikely.
const VALUES: RangeInclusive<i64> = -1000..=1000;

/// A `workers`-wide executor shared by every case in this file -- see the
/// module's `# Design` section.
///
/// # Panics
///
/// Panics if `workers` is outside [`WORKERS`].
fn executor(workers: usize) -> &'static Executor {
    static POOL: [OnceLock<Executor>; 4] = [
        OnceLock::new(),
        OnceLock::new(),
        OnceLock::new(),
        OnceLock::new(),
    ];
    POOL[workers - 1].get_or_init(|| Executor::new(workers))
}

/// Runs `body` as the single runtime task of a fresh taskflow on the shared
/// `workers`-wide executor and hands back what it returned.
///
/// `body` is `'static` because [`FlowBuilder::emplace_runtime`]'s callable
/// is: a case's data is moved in rather than borrowed, and the answer comes
/// back through the slot both ends hold.
fn in_runtime<R, F>(workers: usize, body: F) -> R
where
    F: FnOnce(&mut Runtime<'_>) -> R + Send + 'static,
    R: Send + 'static,
{
    let slot: Arc<Mutex<Option<R>>> = Arc::new(Mutex::new(None));
    let filled = Arc::clone(&slot);
    let mut body = Some(body);
    let mut taskflow = Taskflow::new();
    taskflow.emplace_runtime(move |rt: &mut Runtime<'_>| {
        let body = body.take().expect("this task runs once per case");
        let answer = body(rt);
        *filled.lock().unwrap_or_else(PoisonError::into_inner) = Some(answer);
    });

    executor(workers)
        .run(&taskflow)
        .wait()
        .expect("no task panicked");

    slot.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
        .expect("the runtime task ran")
}

/// Which partitioner family a case runs under: the four
/// `src/algorithm/partitioner.rs` ships, picked per case so that every
/// property below is checked against all of them.
#[derive(Debug, Clone, Copy)]
enum Family {
    /// [`StaticPartitioner`].
    Static,
    /// [`GuidedPartitioner`], the default.
    Guided,
    /// [`DynamicPartitioner`].
    Dynamic,
    /// [`RandomPartitioner`].
    Random,
}

/// Draws one of the four partitioner families.
fn family() -> impl Strategy<Value = Family> {
    prop_oneof![
        Just(Family::Static),
        Just(Family::Guided),
        Just(Family::Dynamic),
        Just(Family::Random),
    ]
}

/// Binds `$part` to a `$family` partitioner of chunk size `$chunk` and runs
/// `$body` under it.
///
/// A macro rather than a function generic over the partitioner: the four
/// families are four distinct types and the trait a given algorithm wants
/// of them differs per algorithm (`ReducePartitioner`, `FindPartitioner`,
/// `IndexDispatch`), so the one thing that generalises is the `match`
/// itself, instantiated afresh at each use.
macro_rules! with_family {
    ($family:expr, $chunk:expr, |$part:ident| $body:block) => {
        match $family {
            Family::Static => {
                let $part = StaticPartitioner::<DefaultClosureWrapper>::new($chunk);
                $body
            }
            Family::Guided => {
                let $part = GuidedPartitioner::<DefaultClosureWrapper>::new($chunk);
                $body
            }
            Family::Dynamic => {
                let $part = DynamicPartitioner::<DefaultClosureWrapper>::new($chunk);
                $body
            }
            Family::Random => {
                let $part = RandomPartitioner::<DefaultClosureWrapper>::new($chunk);
                $body
            }
        }
    };
}

/// Whether a `(workers, n, chunk)` case takes the single-chunk
/// short-circuit every algorithm in `reduce`, `find` and `for_each_index`
/// documents -- `W <= 1 || n <= part.chunk_size()`, with `0` meaning "no
/// configured size" and so never short-circuiting.
fn folds_as_one_chunk(workers: usize, n: usize, chunk: usize) -> bool {
    workers <= 1 || (chunk != 0 && n <= chunk)
}

/// Every index of `[0, n)` is covered by exactly one of `chunks`.
fn covers_range_once(chunks: &[(usize, usize)], n: usize) -> TestCaseResult {
    let mut visits = vec![0_usize; n];
    for &(b, e) in chunks {
        prop_assert!(b <= e, "chunk ({b}, {e}) runs backwards");
        prop_assert!(e <= n, "chunk ({b}, {e}) runs past n = {n}");
        for slot in &mut visits[b..e] {
            *slot += 1;
        }
    }
    for (i, &visited) in visits.iter().enumerate() {
        // Positional rather than inline captures: `prop_assert_eq!` builds
        // its format string with `concat!`, which `format_args!` will not
        // capture a variable through.
        prop_assert_eq!(
            visited,
            1,
            "index {} of {} was visited {} time(s)",
            i,
            n,
            visited
        );
    }
    Ok(())
}

/// The indices an [`IndexRange`] stands for, in order: the sequential walk
/// every `for_each_*` case below is checked against.
fn walk(range: IndexRange<i64>) -> Vec<i64> {
    let (begin, end, step) = (range.begin(), range.end(), range.step_size());
    let mut seen = Vec::new();
    let mut i = begin;
    while if step > 0 { i < end } else { i > end } {
        seen.push(i);
        i += step;
    }
    seen
}

// ----------------------------------------------------------------------------
// sort / sort_by against `[T]::sort_by`
// ----------------------------------------------------------------------------

/// An element wide enough (528 bytes) to land in `sort`'s smallest cutoff
/// bucket, so that an input of 129 elements or more takes the parallel path
/// rather than the serial short-circuit -- `src/algorithm/sort.rs`'s
/// `parallel_sort_cutoff` has the table.
#[derive(Clone, Copy, Debug)]
struct Wide {
    /// What the case sorts on.
    key: i64,
    /// Where this element started, so the sort can be checked for being a
    /// permutation without assuming the stability it does not promise.
    tag: usize,
    /// Payload: every byte of it is `tag`'s own low byte, so that a case
    /// can tell an element that moved from one the sort rebuilt out of two
    /// others. 512 bytes is also what puts this type in the bucket the
    /// struct's own docs name.
    pad: [u8; 512],
}

/// The byte a [`Wide`]'s payload is filled with for a given tag.
fn tag_byte(tag: usize) -> u8 {
    u8::try_from(tag % 256).expect("a remainder mod 256 fits a u8")
}

proptest! {
    /// `Runtime::sort` over `T: Ord` agrees with `[T]::sort_by` using the
    /// same order, at every length and worker count.
    #[test]
    fn sort_matches_std_sort(values in prop::collection::vec(VALUES, LENGTHS), workers in WORKERS) {
        let mut want = values.clone();
        want.sort_by(i64::cmp);

        let got = in_runtime(workers, move |rt: &mut Runtime<'_>| {
            let mut data = values;
            rt.sort(&mut data);
            data
        });

        prop_assert_eq!(got, want);
    }

    /// `Runtime::sort_by` over a wide element and a key comparator leaves
    /// the keys in `[T]::sort_by`'s order and the elements a permutation of
    /// the input, payloads intact -- never a check on the order of equal
    /// keys, which this sort does not promise; the module docs above say
    /// why.
    #[test]
    fn sort_by_matches_std_sort_by(
        keys in prop::collection::vec(VALUES, LENGTHS),
        workers in WORKERS,
    ) {
        let n = keys.len();
        let mut want_keys = keys.clone();
        want_keys.sort_by(i64::cmp);

        let data: Vec<Wide> = keys
            .into_iter()
            .enumerate()
            .map(|(tag, key)| Wide {
                key,
                tag,
                pad: [tag_byte(tag); 512],
            })
            .collect();

        let got = in_runtime(workers, move |rt: &mut Runtime<'_>| {
            let mut data = data;
            rt.sort_by(&mut data, |a: &Wide, b: &Wide| a.key.cmp(&b.key));
            data
        });

        let got_keys: Vec<i64> = got.iter().map(|w| w.key).collect();
        prop_assert_eq!(got_keys, want_keys);

        let mut tags: Vec<usize> = got.iter().map(|w| w.tag).collect();
        tags.sort_unstable();
        prop_assert_eq!(tags, (0..n).collect::<Vec<usize>>());

        for element in &got {
            let want = tag_byte(element.tag);
            prop_assert!(
                element.pad.iter().all(|&byte| byte == want),
                "element tagged {} came back with a different payload",
                element.tag
            );
        }
    }
}

/// The input shapes a pattern-defeating quicksort exists for: a partition
/// that mishandles one of these is slow on it rather than wrong, so what the
/// case below checks is that the pivot selection, the pattern-breaking swaps
/// and the bad-partition fallback `src/algorithm/sort.rs` runs over them
/// still come back with `[T]::sort_by`'s answer (#375).
#[derive(Debug, Clone, Copy)]
enum Pattern {
    /// Already in the order the sort is asked for.
    Sorted,
    /// Exactly reversed.
    Reversed,
    /// Three distinct values, so most of a partition is equal to its pivot.
    FewDistinct,
    /// Ascending to the middle, then descending.
    OrganPipe,
}

/// Draws one of the four pattern shapes.
fn pattern() -> impl Strategy<Value = Pattern> {
    prop_oneof![
        Just(Pattern::Sorted),
        Just(Pattern::Reversed),
        Just(Pattern::FewDistinct),
        Just(Pattern::OrganPipe),
    ]
}

/// The `n` keys a [`Pattern`] stands for.
fn pattern_keys(pattern: Pattern, n: usize) -> Vec<i64> {
    let key = |i: usize| i64::try_from(i).expect("a pattern length fits an i64");
    (0..n)
        .map(|i| match pattern {
            Pattern::Sorted => key(i),
            Pattern::Reversed => key(n - 1 - i),
            Pattern::FewDistinct => key(i % 3),
            Pattern::OrganPipe => key(i.min(n - 1 - i)),
        })
        .collect()
}

/// The lengths the pattern cases draw over, longer than [`LENGTHS`]: a
/// [`Wide`] input this long clears the 128-element cutoff several times
/// over, so a case reaches the partitions that inherit a pivot from the one
/// above them and not just the first.
const PATTERN_LENGTHS: RangeInclusive<usize> = 0..=600;

proptest! {
    /// `Runtime::sort_by` over each [`Pattern`] agrees with `[T]::sort_by`
    /// and brings back the elements that went in. What this adds over
    /// `sort_by_matches_std_sort_by` is the input shape: a drawn vector is
    /// almost never sorted, reversed or an organ pipe, and those are the
    /// shapes the partition treats specially (#375).
    #[test]
    fn sort_by_matches_std_sort_by_on_pattern_inputs(
        pattern in pattern(),
        n in PATTERN_LENGTHS,
        workers in WORKERS,
    ) {
        let keys = pattern_keys(pattern, n);
        let mut want_keys = keys.clone();
        want_keys.sort_by(i64::cmp);

        let data: Vec<Wide> = keys
            .into_iter()
            .enumerate()
            .map(|(tag, key)| Wide {
                key,
                tag,
                pad: [tag_byte(tag); 512],
            })
            .collect();

        let got = in_runtime(workers, move |rt: &mut Runtime<'_>| {
            let mut data = data;
            rt.sort_by(&mut data, |a: &Wide, b: &Wide| a.key.cmp(&b.key));
            data
        });

        let got_keys: Vec<i64> = got.iter().map(|w| w.key).collect();
        prop_assert_eq!(got_keys, want_keys);

        let mut tags: Vec<usize> = got.iter().map(|w| w.tag).collect();
        tags.sort_unstable();
        prop_assert_eq!(tags, (0..n).collect::<Vec<usize>>());
    }
}

// ----------------------------------------------------------------------------
// reduce / transform_reduce against `Iterator::fold`
// ----------------------------------------------------------------------------

/// Splits what a concatenating `bop` built back into the operands that went
/// into it, `;` being the separator every token below carries.
fn tokens(folded: &str) -> Vec<&str> {
    let mut tokens: Vec<&str> = folded.split(';').filter(|t| !t.is_empty()).collect();
    tokens.sort_unstable();
    tokens
}

proptest! {
    /// `Runtime::reduce` with string concatenation -- associative, not
    /// commutative -- combines `init` and every element exactly once, with
    /// `init` leading; on the single-chunk path it is exactly
    /// `Iterator::fold`. The module docs above say why the parallel path
    /// cannot be held to the order `fold` produces.
    #[test]
    fn reduce_combines_init_and_every_element_once(
        values in prop::collection::vec(any::<u32>(), LENGTHS),
        seed in 0_usize..5,
        chunk in CHUNKS,
        family in family(),
        workers in WORKERS,
    ) {
        let len = values.len();
        let data: Vec<String> = values.iter().map(|v| format!("{v};")).collect();
        let init = format!("i{seed};");
        let want_sequential = data.iter().fold(init.clone(), |acc, item| acc + item);

        let moved = (data.clone(), init.clone());
        let got = in_runtime(workers, move |rt: &mut Runtime<'_>| {
            let (data, mut acc) = moved;
            with_family!(family, chunk, |part| {
                rt.reduce(&data, &mut acc, |a: &String, b: &String| format!("{a}{b}"), part);
            });
            acc
        });

        prop_assert!(got.starts_with(&init), "{got} does not lead with {init}");
        prop_assert_eq!(got.len(), want_sequential.len());

        prop_assert_eq!(tokens(&got), tokens(&want_sequential));

        if folds_as_one_chunk(workers, len, chunk) {
            prop_assert_eq!(got, want_sequential);
        }
    }

    /// `Runtime::transform_reduce` under the same concatenating `bop`: every
    /// element is transformed and combined exactly once, and the
    /// single-chunk path is exactly `Iterator::fold` over `uop`'s values.
    #[test]
    fn transform_reduce_combines_init_and_every_element_once(
        values in prop::collection::vec(any::<u32>(), LENGTHS),
        seed in 0_usize..5,
        chunk in CHUNKS,
        family in family(),
        workers in WORKERS,
    ) {
        let len = values.len();
        let data = values;
        let init = format!("i{seed};");
        let want_sequential = data
            .iter()
            .fold(init.clone(), |acc, item| acc + &format!("{item};"));

        let moved = (data, init.clone());
        let got = in_runtime(workers, move |rt: &mut Runtime<'_>| {
            let (data, mut acc) = moved;
            with_family!(family, chunk, |part| {
                rt.transform_reduce(
                    &data,
                    &mut acc,
                    |a: &String, b: &String| format!("{a}{b}"),
                    |item: &u32| format!("{item};"),
                    part,
                );
            });
            acc
        });

        prop_assert!(got.starts_with(&init), "{got} does not lead with {init}");
        prop_assert_eq!(tokens(&got), tokens(&want_sequential));

        if folds_as_one_chunk(workers, len, chunk) {
            prop_assert_eq!(got, want_sequential);
        }
    }
}

// ----------------------------------------------------------------------------
// the four scans against a sequential prefix scan
// ----------------------------------------------------------------------------

proptest! {
    /// `inclusive_scan`, `inclusive_scan_with_init`, `exclusive_scan` and
    /// `transform_inclusive_scan` each match the sequential prefix scan of
    /// the same `bop` (and, for the last, the same `uop`), run over one
    /// input in one task so that a case compares all four on the same
    /// partition of the same data.
    #[test]
    fn the_four_scans_match_a_sequential_prefix_scan(
        input in prop::collection::vec(VALUES, LENGTHS),
        init in VALUES,
        workers in WORKERS,
    ) {
        let n = input.len();

        let mut want_inclusive = Vec::with_capacity(n);
        let mut want_with_init = Vec::with_capacity(n);
        let mut want_exclusive = Vec::with_capacity(n);
        let mut want_transform = Vec::with_capacity(n);
        let mut inclusive = 0;
        let mut with_init = init;
        let mut exclusive = init;
        let mut transform = 0;
        for (i, &x) in input.iter().enumerate() {
            inclusive = if i == 0 { x } else { inclusive + x };
            with_init += x;
            transform = if i == 0 { 2 * x } else { transform + 2 * x };
            want_inclusive.push(inclusive);
            want_with_init.push(with_init);
            want_exclusive.push(exclusive);
            want_transform.push(transform);
            exclusive += x;
        }

        let moved = input;
        let got = in_runtime(workers, move |rt: &mut Runtime<'_>| {
            let input = moved;
            let add = |a: &i64, b: &i64| a + b;

            let mut inclusive = vec![0_i64; input.len()];
            rt.inclusive_scan(&input, &mut inclusive, add);

            let mut with_init = vec![0_i64; input.len()];
            rt.inclusive_scan_with_init(&input, &mut with_init, add, &init);

            let mut exclusive = vec![0_i64; input.len()];
            rt.exclusive_scan(&input, &mut exclusive, init, add);

            let mut transform = vec![0_i64; input.len()];
            rt.transform_inclusive_scan(&input, &mut transform, add, |x: &i64| 2 * x);

            (inclusive, with_init, exclusive, transform)
        });

        prop_assert_eq!(got.0, want_inclusive);
        prop_assert_eq!(got.1, want_with_init);
        prop_assert_eq!(got.2, want_exclusive);
        prop_assert_eq!(got.3, want_transform);
    }
}

// ----------------------------------------------------------------------------
// merge against a sorted concatenation, find against `Iterator`'s
// ----------------------------------------------------------------------------

proptest! {
    /// `Runtime::merge` of two sorted inputs is their concatenation sorted:
    /// each side is drawn up to half of [`LENGTHS`]' longest, so that the
    /// output spans the same lengths the other cases do.
    #[test]
    fn merge_matches_a_sorted_concatenation(
        left in prop::collection::vec(VALUES, 0..=128_usize),
        right in prop::collection::vec(VALUES, 0..=128_usize),
        workers in WORKERS,
    ) {
        let mut a = left;
        let mut b = right;
        a.sort_by(i64::cmp);
        b.sort_by(i64::cmp);

        let mut want: Vec<i64> = a.iter().chain(b.iter()).copied().collect();
        want.sort_by(i64::cmp);

        let moved = (a, b);
        let got = in_runtime(workers, move |rt: &mut Runtime<'_>| {
            let (a, b) = moved;
            let mut out = vec![0_i64; a.len() + b.len()];
            rt.merge(&a, &b, &mut out);
            out
        });

        prop_assert_eq!(got, want);
    }

    /// `find_if` and `find_if_not` report exactly what
    /// `Iterator::position` does over the same predicate -- the *earliest*
    /// matching index, not merely a matching one -- for every partitioner,
    /// chunk size and worker count.
    #[test]
    fn find_if_and_find_if_not_match_iterator_position(
        data in prop::collection::vec(0_i64..=7, LENGTHS),
        needle in 0_i64..=7,
        chunk in CHUNKS,
        family in family(),
        workers in WORKERS,
    ) {
        let want_if = data.iter().position(|x| *x == needle);
        let want_if_not = data.iter().position(|x| *x != needle);

        let moved = data;
        let got = in_runtime(workers, move |rt: &mut Runtime<'_>| {
            let data = moved;
            let mut matched = None;
            let mut unmatched = None;
            with_family!(family, chunk, |part| {
                rt.find_if(&data, &mut matched, |x: &i64| *x == needle, part);
            });
            with_family!(family, chunk, |part| {
                rt.find_if_not(&data, &mut unmatched, |x: &i64| *x == needle, part);
            });
            (matched, unmatched)
        });

        prop_assert_eq!(got.0, want_if);
        prop_assert_eq!(got.1, want_if_not);
    }

    /// `min_element` and `max_element` index an element whose value is
    /// `Iterator::min`'s and `Iterator::max`'s, and index nothing at all for
    /// an empty input. The value rather than the index, because ties are
    /// broken by the same race `reduce`'s pool is -- the module docs above
    /// say so.
    #[test]
    fn min_and_max_element_match_iterator_min_and_max(
        data in prop::collection::vec(VALUES, LENGTHS),
        chunk in CHUNKS,
        family in family(),
        workers in WORKERS,
    ) {
        let want_min = data.iter().copied().min();
        let want_max = data.iter().copied().max();

        let moved = data.clone();
        let got = in_runtime(workers, move |rt: &mut Runtime<'_>| {
            let data = moved;
            let mut smallest = None;
            let mut largest = None;
            with_family!(family, chunk, |part| {
                rt.min_element(&data, &mut smallest, |a: &i64, b: &i64| a < b, part);
            });
            with_family!(family, chunk, |part| {
                rt.max_element(&data, &mut largest, |a: &i64, b: &i64| a < b, part);
            });
            (smallest, largest)
        });

        prop_assert_eq!(got.0.map(|i| data[i]), want_min);
        prop_assert_eq!(got.1.map(|i| data[i]), want_max);
    }
}

// ----------------------------------------------------------------------------
// for_each_index over 1-D and N-D ranges: every index exactly once
// ----------------------------------------------------------------------------

proptest! {
    /// `for_each_index` visits exactly the indices the sequential walk from
    /// `begin` in steps of `step` does, each exactly once, for a positive
    /// and a negative step alike.
    #[test]
    fn for_each_index_visits_every_index_exactly_once(
        begin in -100_i64..=100,
        len in 0_i64..=256,
        step in prop_oneof![-8_i64..=-1, 1_i64..=8],
        chunk in CHUNKS,
        family in family(),
        workers in WORKERS,
    ) {
        let end = begin + len * step;
        let want = walk(IndexRange::new(begin, end, step));

        let seen: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
        let visited = Arc::clone(&seen);
        in_runtime(workers, move |rt: &mut Runtime<'_>| {
            with_family!(family, chunk, |part| {
                rt.for_each_index(
                    begin,
                    end,
                    step,
                    move |i: i64| {
                        visited
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .push(i);
                    },
                    part,
                );
            });
        });

        let mut got = std::mem::take(&mut *seen.lock().unwrap_or_else(PoisonError::into_inner));
        got.sort_unstable();
        let mut want = want;
        want.sort_unstable();
        prop_assert_eq!(got, want);
    }

    /// `for_each_by_index` hands out sub-ranges that, walked, are exactly
    /// the indices of the range it was given, each exactly once.
    #[test]
    fn for_each_by_index_covers_its_range_exactly_once(
        begin in -100_i64..=100,
        len in 0_i64..=256,
        step in prop_oneof![-8_i64..=-1, 1_i64..=8],
        chunk in CHUNKS,
        family in family(),
        workers in WORKERS,
    ) {
        let end = begin + len * step;
        let range = IndexRange::new(begin, end, step);
        let mut want = walk(range);
        want.sort_unstable();

        let seen: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
        let visited = Arc::clone(&seen);
        in_runtime(workers, move |rt: &mut Runtime<'_>| {
            with_family!(family, chunk, |part| {
                rt.for_each_by_index(
                    range,
                    move |piece: IndexRange<i64>| {
                        visited
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .extend(walk(piece));
                    },
                    part,
                );
            });
        });

        let mut got = std::mem::take(&mut *seen.lock().unwrap_or_else(PoisonError::into_inner));
        got.sort_unstable();
        prop_assert_eq!(got, want);
    }

    /// `for_each_by_index_nd` hands out sub-ranges whose Cartesian products
    /// are exactly the cells of the 2-D range it was given, each exactly
    /// once, with either dimension's step pointing either way.
    #[test]
    fn for_each_by_index_nd_covers_its_product_exactly_once(
        rows in 0_i64..=16,
        cols in 0_i64..=16,
        row_step in prop_oneof![-3_i64..=-1, 1_i64..=3],
        col_step in prop_oneof![-3_i64..=-1, 1_i64..=3],
        chunk in CHUNKS,
        family in family(),
        workers in WORKERS,
    ) {
        let row_range = IndexRange::new(0, rows * row_step, row_step);
        let col_range = IndexRange::new(0, cols * col_step, col_step);
        let range = IndexRanges::from_ranges([row_range, col_range]);

        let mut want: Vec<(i64, i64)> = walk(row_range)
            .into_iter()
            .flat_map(|i| walk(col_range).into_iter().map(move |j| (i, j)))
            .collect();
        want.sort_unstable();

        let seen: Arc<Mutex<Vec<(i64, i64)>>> = Arc::new(Mutex::new(Vec::new()));
        let visited = Arc::clone(&seen);
        in_runtime(workers, move |rt: &mut Runtime<'_>| {
            with_family!(family, chunk, |part| {
                rt.for_each_by_index_nd(
                    range,
                    move |piece: IndexRanges<i64, 2>| {
                        let (i_beg, i_end, i_step) = piece.dim(0);
                        let (j_beg, j_end, j_step) = piece.dim(1);
                        let cells: Vec<(i64, i64)> = walk(IndexRange::new(i_beg, i_end, i_step))
                            .into_iter()
                            .flat_map(|i| {
                                walk(IndexRange::new(j_beg, j_end, j_step))
                                    .into_iter()
                                    .map(move |j| (i, j))
                            })
                            .collect();
                        visited
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .extend(cells);
                    },
                    part,
                );
            });
        });

        let mut got = std::mem::take(&mut *seen.lock().unwrap_or_else(PoisonError::into_inner));
        got.sort_unstable();
        prop_assert_eq!(got, want);
    }
}

// ----------------------------------------------------------------------------
// every partitioner's chunk sequence covers [0, n) exactly once
// ----------------------------------------------------------------------------

/// The chunk sequence a [`StaticPartitioner`] hands out for `n` iterations
/// over `w` workers: the loop `SlicePartitioner::drive` runs, each worker
/// striding from its own `curr_b` under its own `adjusted_chunk_size`.
fn static_chunks(
    part: &StaticPartitioner<DefaultClosureWrapper>,
    n: usize,
    w: usize,
) -> Vec<(usize, usize)> {
    let mut chunks = Vec::new();
    let mut curr_b = 0_usize;
    for i in 0..w {
        if curr_b >= n {
            break;
        }
        let chunk_size = part.adjusted_chunk_size(n, w, i);
        part.loop_static(n, w, curr_b, chunk_size, |b, e| chunks.push((b, e)));
        curr_b += chunk_size;
    }
    chunks
}

/// The chunk sequence a dynamic-family partitioner hands out for `n`
/// iterations: `w` threads racing one shared cursor, as the algorithms'
/// workers do, so the sequence collected is one real interleaving rather
/// than a replay of the boundaries.
fn dynamic_chunks<P: DynamicLoop + Sync>(part: &P, n: usize, w: usize) -> Vec<(usize, usize)> {
    let chunks: Mutex<Vec<(usize, usize)>> = Mutex::new(Vec::new());
    let cursor = AtomicUsize::new(0);
    thread::scope(|scope| {
        for _ in 0..w {
            scope.spawn(|| {
                part.loop_dynamic(n, w, &cursor, |b, e| {
                    chunks
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push((b, e));
                });
            });
        }
    });
    chunks.into_inner().unwrap_or_else(PoisonError::into_inner)
}

proptest! {
    /// Whichever family it belongs to, a partitioner's chunks tile `[0, n)`:
    /// no index is skipped and none is handed out twice, for a chunk size of
    /// `0`, one larger than `n`, and everything in between.
    #[test]
    fn every_partitioner_covers_its_range_exactly_once(
        n in LENGTHS,
        chunk in CHUNKS,
        family in family(),
        workers in WORKERS,
    ) {
        let chunks = match family {
            Family::Static => {
                static_chunks(&StaticPartitioner::new(chunk), n, workers)
            }
            Family::Guided => {
                dynamic_chunks(&GuidedPartitioner::<DefaultClosureWrapper>::new(chunk), n, workers)
            }
            Family::Dynamic => {
                dynamic_chunks(&DynamicPartitioner::<DefaultClosureWrapper>::new(chunk), n, workers)
            }
            Family::Random => {
                dynamic_chunks(&RandomPartitioner::<DefaultClosureWrapper>::new(chunk), n, workers)
            }
        };

        covers_range_once(&chunks, n)?;
    }
}

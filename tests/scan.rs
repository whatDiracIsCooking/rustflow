//! Translated from `unittests/test_scan.cpp` (Taskflow v4.1.0): the six
//! `test_*_scan` groups, each a `TEST_CASE` per worker count upstream
//! (`InclusiveScan.int+.1thread` through `.12threads`, and so on for
//! `*`, `string`, the `Initialized.*` variants and the two `Transformed*`
//! groups).
//!
//! Two adaptations, each forced by the port rather than chosen:
//!
//! - **One sweep per worker count, not 72 `TEST_CASE`s.** Upstream reruns
//!   every `n` in `0, 1, 3, 7, ..` (`n = n*2+1`) up to 250000 (2500 for
//!   strings) on a *fresh* `tf::Executor(W)` per `TEST_CASE`. The shape
//!   survives -- one [`Executor`] per worker count, [`Taskflow::clear`]
//!   between sizes -- but `N_SIZES` below is the representative subset
//!   `[0, 1, 2, 3, 7, 8, 23, 37, 100]`: small enough to run the worker
//!   matrix in well under upstream's `doctest::timeout(300)`, while still
//!   covering `N <= 2` (the serial fallback), `N == W` and `N` well above
//!   and well below every `W` in [`WORKERS`].
//! - **No in-place case.** Upstream's `task2` scans `std::ref(sbeg)` into
//!   itself, exercising that a deferred iterator pair is re-read when the
//!   task runs. `&[T]` and `&mut [T]` cannot alias the same slice in Rust,
//!   and this port's own signature -- `src/algorithm/scan.rs`'s own `#
//!   Design` section -- never offers one; only the out-of-place half
//!   (`task1` upstream) is here.
//!
//! The non-commutative case upstream exercises through `std::string` --
//! `std::plus<std::string>` concatenates, which does not commute -- is
//! every variant's own witness here too, `*_strings_pin_the_order` below.
//! `(a, b) -> a * 10 + b`, the acceptance this file is held to also names,
//! is not actually usable for this: it is non-commutative but not
//! *associative* (`bop(bop(x, y), z) != bop(x, bop(y, z))` once `y` or `z`
//! is itself a compound value rather than a raw single digit), and
//! associativity is what every block-decomposable scan -- upstream's own
//! included -- requires of `bop` before it may regroup the fold at a
//! block boundary at all. Concatenation has no such defect.

use std::sync::{Arc, Mutex};

use rustflow::Executor;
use rustflow::prelude::*;

/// Upstream's own worker counts: `1, 2, 3, 4, 8, 12`.
const WORKERS: [usize; 6] = [1, 2, 3, 4, 8, 12];

/// A representative subset of upstream's `n = 0, 1, 3, 7, 15, ..`, chosen
/// for this file's own doc comment's reasons.
const N_SIZES: [usize; 9] = [0, 1, 2, 3, 7, 8, 23, 37, 100];

fn serial_inclusive<T, F, Uop>(input: &[T], out: &mut [T], bop: &F, uop: &Uop)
where
    T: Clone,
    F: Fn(&T, &T) -> T,
    Uop: Fn(&T) -> T,
{
    let mut iter = input.iter().zip(out.iter_mut());
    let Some((first_in, first_out)) = iter.next() else {
        return;
    };
    let mut local = uop(first_in);
    *first_out = local.clone();
    for (elem, slot) in iter {
        local = bop(&local, &uop(elem));
        *slot = local.clone();
    }
}

fn serial_inclusive_with_init<T, F, Uop>(input: &[T], out: &mut [T], bop: &F, uop: &Uop, init: &T)
where
    T: Clone,
    F: Fn(&T, &T) -> T,
    Uop: Fn(&T) -> T,
{
    let mut iter = input.iter().zip(out.iter_mut());
    let Some((first_in, first_out)) = iter.next() else {
        return;
    };
    let mut local = bop(init, &uop(first_in));
    *first_out = local.clone();
    for (elem, slot) in iter {
        local = bop(&local, &uop(elem));
        *slot = local.clone();
    }
}

fn serial_exclusive<T, F, Uop>(input: &[T], out: &mut [T], init: &T, bop: &F, uop: &Uop)
where
    T: Clone,
    F: Fn(&T, &T) -> T,
    Uop: Fn(&T) -> T,
{
    let mut local = init.clone();
    for (elem, slot) in input.iter().zip(out.iter_mut()) {
        *slot = local.clone();
        local = bop(&local, &uop(elem));
    }
}

// ----------------------------------------------------------------------------
// Testcase: inclusive_scan -- upstream's `InclusiveScan.int+`/`.int*`/`.string`
// ----------------------------------------------------------------------------

#[test]
fn inclusive_scan_matches_a_serial_scan() {
    for &workers in &WORKERS {
        let executor = Executor::new(workers);
        for &n in &N_SIZES {
            let input: Vec<i64> = (0..n).map(|i| i64::try_from(i % 10).unwrap()).collect();

            let mut golden = vec![0i64; n];
            serial_inclusive(
                &input,
                &mut golden,
                &|a: &i64, b: &i64| a + b,
                &|x: &i64| *x,
            );

            let input_arc = Arc::new(input);
            let out = Arc::new(Mutex::new(vec![0i64; n]));

            let mut taskflow = Taskflow::new();
            taskflow.inclusive_scan(
                Arc::clone(&input_arc),
                Arc::clone(&out),
                |a: &i64, b: &i64| a + b,
            );

            executor.run(&taskflow).wait().expect("no task panicked");
            assert_eq!(
                *out.lock().expect("uncontended"),
                golden,
                "n={n}, workers={workers}"
            );
        }
    }
}

/// Upstream's `InclusiveScan.string`: `std::plus<std::string>`
/// concatenates, which does not commute, so this is also this port's own
/// non-commutative witness for `inclusive_scan`.
#[test]
fn inclusive_scan_strings_pin_the_order() {
    for &workers in &[1, 2, 4, 8] {
        let executor = Executor::new(workers);
        for &n in &N_SIZES {
            let input: Vec<String> = (0..n).map(|i| (i % 10).to_string()).collect();

            let mut golden = vec![String::new(); n];
            serial_inclusive(
                &input,
                &mut golden,
                &|a: &String, b: &String| format!("{a}{b}"),
                &String::clone,
            );

            let input_arc = Arc::new(input);
            let out = Arc::new(Mutex::new(vec![String::new(); n]));

            let mut taskflow = Taskflow::new();
            taskflow.inclusive_scan(
                Arc::clone(&input_arc),
                Arc::clone(&out),
                |a: &String, b: &String| format!("{a}{b}"),
            );

            executor.run(&taskflow).wait().expect("no task panicked");
            assert_eq!(
                *out.lock().expect("uncontended"),
                golden,
                "n={n}, workers={workers}"
            );
        }
    }
}

// ----------------------------------------------------------------------------
// Testcase: inclusive_scan with init -- upstream's `Initialized.InclusiveScan.*`
// ----------------------------------------------------------------------------

#[test]
fn inclusive_scan_with_init_matches_a_serial_scan() {
    for &workers in &WORKERS {
        let executor = Executor::new(workers);
        for &n in &N_SIZES {
            let input: Vec<i64> = (0..n).map(|i| i64::try_from(i % 10).unwrap()).collect();
            let init = 7i64;

            let mut golden = vec![0i64; n];
            serial_inclusive_with_init(
                &input,
                &mut golden,
                &|a: &i64, b: &i64| a + b,
                &|x: &i64| *x,
                &init,
            );

            let input_arc = Arc::new(input);
            let out = Arc::new(Mutex::new(vec![0i64; n]));

            let mut taskflow = Taskflow::new();
            taskflow.inclusive_scan_with_init(
                Arc::clone(&input_arc),
                Arc::clone(&out),
                |a: &i64, b: &i64| a + b,
                init,
            );

            executor.run(&taskflow).wait().expect("no task panicked");
            assert_eq!(
                *out.lock().expect("uncontended"),
                golden,
                "n={n}, workers={workers}"
            );
        }
    }
}

/// This port's own non-commutative witness for `inclusive_scan_with_init`:
/// upstream's `Initialized.InclusiveScan.*` only ever runs `int`, never
/// `std::string`, so there is no upstream case to translate here --
/// concatenation, `inclusive_scan_strings_pin_the_order`'s own operator,
/// is what pins it. `(a, b) -> a * 10 + b`, the other form this module's
/// own acceptance names, is not actually usable here: it is not
/// associative (`bop(bop(x, y), z) != bop(x, bop(y, z))` in general, since
/// the left operand's own magnitude leaks into the result), and
/// associativity is the one property every block-decomposable scan --
/// upstream's included -- requires of `bop` to be allowed to regroup the
/// fold at block boundaries at all.
#[test]
fn inclusive_scan_with_init_strings_pin_the_order() {
    for &workers in &[1, 2, 4, 8] {
        let executor = Executor::new(workers);
        for &n in &N_SIZES {
            let input: Vec<String> = (0..n).map(|i| (i % 10).to_string()).collect();
            let init = "init-".to_string();

            let mut golden = vec![String::new(); n];
            serial_inclusive_with_init(
                &input,
                &mut golden,
                &|a: &String, b: &String| format!("{a}{b}"),
                &String::clone,
                &init,
            );

            let input_arc = Arc::new(input);
            let out = Arc::new(Mutex::new(vec![String::new(); n]));

            let mut taskflow = Taskflow::new();
            taskflow.inclusive_scan_with_init(
                Arc::clone(&input_arc),
                Arc::clone(&out),
                |a: &String, b: &String| format!("{a}{b}"),
                init,
            );

            executor.run(&taskflow).wait().expect("no task panicked");
            assert_eq!(
                *out.lock().expect("uncontended"),
                golden,
                "n={n}, workers={workers}"
            );
        }
    }
}

// ----------------------------------------------------------------------------
// Testcase: exclusive_scan -- upstream's `ExclusiveScan.int+`/`.int*`/`.string`
// ----------------------------------------------------------------------------

#[test]
fn exclusive_scan_matches_a_serial_scan() {
    for &workers in &WORKERS {
        let executor = Executor::new(workers);
        for &n in &N_SIZES {
            let input: Vec<i64> = (0..n).map(|i| i64::try_from(i % 10).unwrap()).collect();
            let init = 7i64;

            let mut golden = vec![0i64; n];
            serial_exclusive(
                &input,
                &mut golden,
                &init,
                &|a: &i64, b: &i64| a + b,
                &|x: &i64| *x,
            );

            let input_arc = Arc::new(input);
            let out = Arc::new(Mutex::new(vec![0i64; n]));

            let mut taskflow = Taskflow::new();
            taskflow.exclusive_scan(
                Arc::clone(&input_arc),
                Arc::clone(&out),
                init,
                |a: &i64, b: &i64| a + b,
            );

            executor.run(&taskflow).wait().expect("no task panicked");
            assert_eq!(
                *out.lock().expect("uncontended"),
                golden,
                "n={n}, workers={workers}"
            );
        }
    }
}

/// Upstream's `ExclusiveScan.string`, and this port's non-commutative
/// witness for `exclusive_scan`: the case that actually exercises the
/// `# Design`-section identity `Runtime::exclusive_scan` is built on, since
/// a shifted-by-one *inclusive* scan only reproduces an exclusive scan
/// exactly when `bop` cares about the order it is folded in.
#[test]
fn exclusive_scan_strings_pin_the_order() {
    for &workers in &[1, 2, 4, 8] {
        let executor = Executor::new(workers);
        for &n in &N_SIZES {
            let input: Vec<String> = (0..n).map(|i| (i % 10).to_string()).collect();
            let init = "init-".to_string();

            let mut golden = vec![String::new(); n];
            serial_exclusive(
                &input,
                &mut golden,
                &init,
                &|a: &String, b: &String| format!("{a}{b}"),
                &String::clone,
            );

            let input_arc = Arc::new(input);
            let out = Arc::new(Mutex::new(vec![String::new(); n]));

            let mut taskflow = Taskflow::new();
            taskflow.exclusive_scan(
                Arc::clone(&input_arc),
                Arc::clone(&out),
                init,
                |a: &String, b: &String| format!("{a}{b}"),
            );

            executor.run(&taskflow).wait().expect("no task panicked");
            assert_eq!(
                *out.lock().expect("uncontended"),
                golden,
                "n={n}, workers={workers}"
            );
        }
    }
}

// ----------------------------------------------------------------------------
// Testcase: transform_inclusive_scan -- upstream's `TransformedInclusiveScan.*`
// ----------------------------------------------------------------------------

#[test]
fn transform_inclusive_scan_matches_a_serial_scan() {
    for &workers in &WORKERS {
        let executor = Executor::new(workers);
        for &n in &N_SIZES {
            let input: Vec<i64> = (0..n).map(|i| i64::try_from(i % 10).unwrap()).collect();

            let mut golden = vec![0i64; n];
            serial_inclusive(
                &input,
                &mut golden,
                &|a: &i64, b: &i64| a + b,
                &|x: &i64| x * 2,
            );

            let input_arc = Arc::new(input);
            let out = Arc::new(Mutex::new(vec![0i64; n]));

            let mut taskflow = Taskflow::new();
            taskflow.transform_inclusive_scan(
                Arc::clone(&input_arc),
                Arc::clone(&out),
                |a: &i64, b: &i64| a + b,
                |x: &i64| x * 2,
            );

            executor.run(&taskflow).wait().expect("no task panicked");
            assert_eq!(
                *out.lock().expect("uncontended"),
                golden,
                "n={n}, workers={workers}"
            );
        }
    }
}

/// This port's own non-commutative witness; see
/// `inclusive_scan_with_init_strings_pin_the_order` for why concatenation,
/// not `(a, b) -> a * 10 + b`, is what pins it.
#[test]
fn transform_inclusive_scan_strings_pin_the_order() {
    let bop = |a: &String, b: &String| format!("{a}{b}");
    let uop = |s: &String| format!("[{s}]");

    for &workers in &[1, 2, 4, 8] {
        let executor = Executor::new(workers);
        for &n in &N_SIZES {
            let input: Vec<String> = (0..n).map(|i| (i % 10).to_string()).collect();

            let mut golden = vec![String::new(); n];
            serial_inclusive(&input, &mut golden, &bop, &uop);

            let input_arc = Arc::new(input);
            let out = Arc::new(Mutex::new(vec![String::new(); n]));

            let mut taskflow = Taskflow::new();
            taskflow.transform_inclusive_scan(Arc::clone(&input_arc), Arc::clone(&out), bop, uop);

            executor.run(&taskflow).wait().expect("no task panicked");
            assert_eq!(
                *out.lock().expect("uncontended"),
                golden,
                "n={n}, workers={workers}"
            );
        }
    }
}

// ----------------------------------------------------------------------------
// Testcase: initialized transform_inclusive_scan -- upstream's
// `Initialized.TransformedInclusiveScan.*`
// ----------------------------------------------------------------------------

#[test]
fn transform_inclusive_scan_with_init_matches_a_serial_scan() {
    for &workers in &WORKERS {
        let executor = Executor::new(workers);
        for &n in &N_SIZES {
            let input: Vec<i64> = (0..n).map(|i| i64::try_from(i % 10).unwrap()).collect();
            let init = 7i64;

            let mut golden = vec![0i64; n];
            serial_inclusive_with_init(
                &input,
                &mut golden,
                &|a: &i64, b: &i64| a + b,
                &|x: &i64| x * 2,
                &init,
            );

            let input_arc = Arc::new(input);
            let out = Arc::new(Mutex::new(vec![0i64; n]));

            let mut taskflow = Taskflow::new();
            taskflow.transform_inclusive_scan_with_init(
                Arc::clone(&input_arc),
                Arc::clone(&out),
                |a: &i64, b: &i64| a + b,
                |x: &i64| x * 2,
                init,
            );

            executor.run(&taskflow).wait().expect("no task panicked");
            assert_eq!(
                *out.lock().expect("uncontended"),
                golden,
                "n={n}, workers={workers}"
            );
        }
    }
}

/// This port's own non-commutative witness; see
/// `inclusive_scan_with_init_strings_pin_the_order` for why concatenation,
/// not `(a, b) -> a * 10 + b`, is what pins it.
#[test]
fn transform_inclusive_scan_with_init_strings_pin_the_order() {
    let bop = |a: &String, b: &String| format!("{a}{b}");
    let uop = |s: &String| format!("[{s}]");

    for &workers in &[1, 2, 4, 8] {
        let executor = Executor::new(workers);
        for &n in &N_SIZES {
            let input: Vec<String> = (0..n).map(|i| (i % 10).to_string()).collect();
            let init = "init-".to_string();

            let mut golden = vec![String::new(); n];
            serial_inclusive_with_init(&input, &mut golden, &bop, &uop, &init);

            let input_arc = Arc::new(input);
            let out = Arc::new(Mutex::new(vec![String::new(); n]));

            let mut taskflow = Taskflow::new();
            taskflow.transform_inclusive_scan_with_init(
                Arc::clone(&input_arc),
                Arc::clone(&out),
                bop,
                uop,
                init,
            );

            executor.run(&taskflow).wait().expect("no task panicked");
            assert_eq!(
                *out.lock().expect("uncontended"),
                golden,
                "n={n}, workers={workers}"
            );
        }
    }
}

// ----------------------------------------------------------------------------
// Testcase: transform_exclusive_scan -- upstream's `TransformedExclusiveScan.*`
// ----------------------------------------------------------------------------

#[test]
fn transform_exclusive_scan_matches_a_serial_scan() {
    for &workers in &WORKERS {
        let executor = Executor::new(workers);
        for &n in &N_SIZES {
            let input: Vec<i64> = (0..n).map(|i| i64::try_from(i % 10).unwrap()).collect();
            let init = 7i64;

            let mut golden = vec![0i64; n];
            serial_exclusive(
                &input,
                &mut golden,
                &init,
                &|a: &i64, b: &i64| a + b,
                &|x: &i64| x * 2,
            );

            let input_arc = Arc::new(input);
            let out = Arc::new(Mutex::new(vec![0i64; n]));

            let mut taskflow = Taskflow::new();
            taskflow.transform_exclusive_scan(
                Arc::clone(&input_arc),
                Arc::clone(&out),
                init,
                |a: &i64, b: &i64| a + b,
                |x: &i64| x * 2,
            );

            executor.run(&taskflow).wait().expect("no task panicked");
            assert_eq!(
                *out.lock().expect("uncontended"),
                golden,
                "n={n}, workers={workers}"
            );
        }
    }
}

/// This port's own non-commutative witness for `transform_exclusive_scan`,
/// the variant resting most directly on the shift-by-one identity: see
/// `exclusive_scan_strings_pin_the_order`'s own doc comment, and
/// `inclusive_scan_with_init_strings_pin_the_order`'s for why concatenation,
/// not `(a, b) -> a * 10 + b`, is what pins it.
#[test]
fn transform_exclusive_scan_strings_pin_the_order() {
    let bop = |a: &String, b: &String| format!("{a}{b}");
    let uop = |s: &String| format!("[{s}]");

    for &workers in &[1, 2, 4, 8] {
        let executor = Executor::new(workers);
        for &n in &N_SIZES {
            let input: Vec<String> = (0..n).map(|i| (i % 10).to_string()).collect();
            let init = "init-".to_string();

            let mut golden = vec![String::new(); n];
            serial_exclusive(&input, &mut golden, &init, &bop, &uop);

            let input_arc = Arc::new(input);
            let out = Arc::new(Mutex::new(vec![String::new(); n]));

            let mut taskflow = Taskflow::new();
            taskflow.transform_exclusive_scan(
                Arc::clone(&input_arc),
                Arc::clone(&out),
                init,
                bop,
                uop,
            );

            executor.run(&taskflow).wait().expect("no task panicked");
            assert_eq!(
                *out.lock().expect("uncontended"),
                golden,
                "n={n}, workers={workers}"
            );
        }
    }
}

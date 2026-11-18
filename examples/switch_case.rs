//! Translated from `examples/switch_case.cpp` (Taskflow v4.1.0).
//!
//! Switch-case control flow: `swcond` draws a value in `0..3` and selects one
//! of `case1`, `case2` or `case3`; whichever runs releases `target` through
//! its own, single-successor edge.
//!
//! Upstream's `case1`/`case2`/`case3` lambdas each return `int` (`0`), so
//! upstream's overload-based `emplace` treats them as condition tasks too,
//! not statics, even though each has only one successor -- porting with the
//! same method (`emplace_condition`, always returning `0`) preserves that
//! rather than flattening it to `emplace`.
//!
//! Upstream draws the switch value with `rand() % 3`; the README's dependency
//! policy keeps `rand` out, so this port draws from the crate's own
//! [`Xorshift32`](rustflow::Xorshift32) instead, seeded so the branch taken is
//! reproducible and the assertion below is not guessing at the draw.
//!
//! Upstream's program only *prints* as it runs. This milestone's examples
//! print *and* **assert**: every line upstream shows is also checked with
//! `assert!` or `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rustflow::Executor;
use rustflow::Xorshift32;
use rustflow::prelude::*;

/// The seed the switch condition's generator draws from, named so the
/// expected branch below is computed from the same value rather than copied
/// by hand.
const SEED: u32 = 0x5C17_C45E;

fn main() {
    let executor = Executor::default();
    let mut taskflow = Taskflow::new();

    // Computed from a generator seeded identically to the one the condition
    // task below owns, so this is the branch the run *will* take rather than
    // a guess at `rand() % 3`'s output.
    let expected = {
        let mut rng = Xorshift32::new(SEED);
        i32::try_from(rng.next_u32() % 3).expect("a value in 0..3 fits in i32")
    };

    let source = taskflow.emplace(|| println!("source"));
    let swcond = {
        let mut rng = Xorshift32::new(SEED);
        taskflow.emplace_condition(move || {
            println!("switch");
            i32::try_from(rng.next_u32() % 3).expect("a value in 0..3 fits in i32")
        })
    };

    let ran_case1 = Arc::new(AtomicBool::new(false));
    let ran_case2 = Arc::new(AtomicBool::new(false));
    let ran_case3 = Arc::new(AtomicBool::new(false));
    let ran_target = Arc::new(AtomicBool::new(false));

    let case1 = {
        let ran = Arc::clone(&ran_case1);
        taskflow.emplace_condition(move || {
            println!("case 1");
            ran.store(true, Ordering::Relaxed);
            0
        })
    };
    let case2 = {
        let ran = Arc::clone(&ran_case2);
        taskflow.emplace_condition(move || {
            println!("case 2");
            ran.store(true, Ordering::Relaxed);
            0
        })
    };
    let case3 = {
        let ran = Arc::clone(&ran_case3);
        taskflow.emplace_condition(move || {
            println!("case 3");
            ran.store(true, Ordering::Relaxed);
            0
        })
    };
    let target = {
        let ran = Arc::clone(&ran_target);
        taskflow.emplace(move || {
            println!("target");
            ran.store(true, Ordering::Relaxed);
        })
    };

    taskflow.task(source).precede([swcond]);
    taskflow.task(swcond).precede([case1, case2, case3]);
    taskflow.task(target).succeed([case1, case2, case3]);

    assert!(executor.run(&taskflow).wait().is_ok());

    // The path taken: the case at `expected`'s index ran and the other two
    // did not, and whichever one ran released `target` through its weak
    // edge.
    let ran = [&ran_case1, &ran_case2, &ran_case3];
    for (index, flag) in ran.iter().enumerate() {
        let index = i32::try_from(index).expect("a small index");
        assert_eq!(
            flag.load(Ordering::Relaxed),
            index == expected,
            "seed {SEED:#x}: case {} ran exactly when selected",
            index + 1
        );
    }
    assert!(
        ran_target.load(Ordering::Relaxed),
        "the selected case's weak edge released target"
    );
}

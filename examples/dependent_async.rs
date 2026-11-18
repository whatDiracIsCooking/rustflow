//! Translated from `examples/dependent_async.cpp` (Taskflow v4.1.0).
//!
//! Demonstrates building a graph of free-standing async tasks one task at a
//! time: `D` depends on `B` and `C`, both of which depend on `A`, named as
//! dependencies by the handle each submission hands back rather than by any
//! edge of a `Taskflow`. The same shape is run twice -- once with
//! [`Executor::dependent_async`], once with
//! [`Executor::silent_dependent_async`] -- and each run asserts the only
//! thing upstream's printed order shows a human: `A` finishes before `B` and
//! `C`, and both of those finish before `D`.
//!
//! Upstream's program only *prints* as it goes. This milestone's examples
//! print *and* **assert**: every line upstream shows is also checked with
//! `assert!` or `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustflow::Executor;

/// Where in finish order each of `A`, `B`, `C` and `D` landed: upstream shows
/// this only as the order four `printf`s happen to appear in, which this
/// reads back as four positions instead.
struct FinishOrder {
    a: AtomicUsize,
    b: AtomicUsize,
    c: AtomicUsize,
    d: AtomicUsize,
}

impl FinishOrder {
    fn new() -> Self {
        Self {
            a: AtomicUsize::new(usize::MAX),
            b: AtomicUsize::new(usize::MAX),
            c: AtomicUsize::new(usize::MAX),
            d: AtomicUsize::new(usize::MAX),
        }
    }

    /// Stamps `slot` with the next value of `sequence`: upstream's `printf`
    /// of the task's name, read back as a position rather than by eye.
    fn record(slot: &AtomicUsize, sequence: &AtomicUsize) {
        slot.store(sequence.fetch_add(1, Ordering::Relaxed), Ordering::Relaxed);
    }

    /// Checks that `A` finished before `B` and `C`, and both of those
    /// finished before `D`: the dependency graph the submissions below built,
    /// read back from the order their tasks actually finished in.
    fn assert_respects_dependencies(&self) {
        let a = self.a.load(Ordering::Relaxed);
        let b = self.b.load(Ordering::Relaxed);
        let c = self.c.load(Ordering::Relaxed);
        let d = self.d.load(Ordering::Relaxed);
        assert!(a < b, "A must finish before B");
        assert!(a < c, "A must finish before C");
        assert!(b < d, "B must finish before D");
        assert!(c < d, "C must finish before D");
    }
}

fn main() {
    let executor = Executor::default();
    let sequence = Arc::new(AtomicUsize::new(0));

    // demonstration of dependent async (with a handle on the result)
    println!("Dependent Async");
    let order = Arc::new(FinishOrder::new());

    let (a, fu_a) = executor.dependent_async(
        {
            let order = Arc::clone(&order);
            let sequence = Arc::clone(&sequence);
            move || {
                println!("A");
                FinishOrder::record(&order.a, &sequence);
            }
        },
        [],
    );
    let (b, fu_b) = executor.dependent_async(
        {
            let order = Arc::clone(&order);
            let sequence = Arc::clone(&sequence);
            move || {
                println!("B");
                FinishOrder::record(&order.b, &sequence);
            }
        },
        [&a],
    );
    let (c, fu_c) = executor.dependent_async(
        {
            let order = Arc::clone(&order);
            let sequence = Arc::clone(&sequence);
            move || {
                println!("C");
                FinishOrder::record(&order.c, &sequence);
            }
        },
        [&a],
    );
    let (_d, fu_d) = executor.dependent_async(
        {
            let order = Arc::clone(&order);
            let sequence = Arc::clone(&sequence);
            move || {
                println!("D");
                FinishOrder::record(&order.d, &sequence);
            }
        },
        [&b, &c],
    );

    fu_d.wait().expect("no task in the chain panicked");
    fu_a.wait().expect("no task in the chain panicked");
    fu_b.wait().expect("no task in the chain panicked");
    fu_c.wait().expect("no task in the chain panicked");
    order.assert_respects_dependencies();

    // demonstration of silent dependent async (no handle on the result)
    println!("Silent Dependent Async");
    sequence.store(0, Ordering::Relaxed);
    let order = Arc::new(FinishOrder::new());

    let a = executor.silent_dependent_async(
        {
            let order = Arc::clone(&order);
            let sequence = Arc::clone(&sequence);
            move || {
                println!("A");
                FinishOrder::record(&order.a, &sequence);
            }
        },
        [],
    );
    let b = executor.silent_dependent_async(
        {
            let order = Arc::clone(&order);
            let sequence = Arc::clone(&sequence);
            move || {
                println!("B");
                FinishOrder::record(&order.b, &sequence);
            }
        },
        [&a],
    );
    let c = executor.silent_dependent_async(
        {
            let order = Arc::clone(&order);
            let sequence = Arc::clone(&sequence);
            move || {
                println!("C");
                FinishOrder::record(&order.c, &sequence);
            }
        },
        [&a],
    );
    let _d = executor.silent_dependent_async(
        {
            let order = Arc::clone(&order);
            let sequence = Arc::clone(&sequence);
            move || {
                println!("D");
                FinishOrder::record(&order.d, &sequence);
            }
        },
        [&b, &c],
    );

    executor.wait_for_all();
    order.assert_respects_dependencies();
}

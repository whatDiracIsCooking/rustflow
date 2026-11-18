//! Translated from the `IndexRange<T>` sections of `unittests/test_iterators.cpp`
//! (Taskflow v4.1.0): `is_index_range_invalid`, `distance`, and the
//! one-dimensional `IndexRange1D` and `NegativeStep.IndexRange1D` cases.
//!
//! Three upstream cases are deliberately not here, all for the same reason:
//! `src/algorithm/index_range.rs`'s `# Design` section narrows the ported
//! surface on purpose, and a case that exercises what was left out would be
//! testing a feature this port does not have.
//!
//! - `IndexRange1D.CTAD` tests class template argument deduction, a C++
//!   mechanism this port has no equivalent of: `IndexRange::new(0, 10, 2)`
//!   already infers its index type from the arguments.
//! - `IndexRange1D.fluent_setters` and `IndexRange1D.rank` exercise
//!   `begin(T)`/`end(T)`/`step_size(T)` as overloaded setters and the `rank`
//!   constant, neither of which this port carries; see the module doc for
//!   why.
//!
//! Every other upstream `IndexRange<T>`/`IndexRanges<T, 1>` case is here,
//! `unravel` renamed to `discrete_domain` throughout to match the port.
//! `IndexRanges<T, N>` for `N > 1` is a different, not-yet-ported type and
//! has no cases in this file.

use rustflow::algorithm::index_range::{IndexRange, distance, is_index_range_invalid};

// Enumerates every element an `IndexRange<i32>` produces, by explicit loop --
// the direct Rust equivalent of upstream's `enumerate_1d` test helper.
fn enumerate(r: &IndexRange<i32>) -> Vec<i32> {
    let mut out = Vec::new();
    let step = r.step_size();
    let mut v = r.begin();
    if step > 0 {
        while v < r.end() {
            out.push(v);
            v += step;
        }
    } else {
        while v > r.end() {
            out.push(v);
            v += step;
        }
    }
    out
}

// ============================================================================
// is_index_range_invalid
// ============================================================================

#[test]
fn is_index_range_invalid_positive_range() {
    assert!(!is_index_range_invalid(0, 10, 1));
    assert!(!is_index_range_invalid(0, 10, 2));
    assert!(!is_index_range_invalid(-5, 5, 1));
    // zero step with beg != end
    assert!(is_index_range_invalid(0, 10, 0));
    // positive range with non-positive step
    assert!(is_index_range_invalid(0, 10, -1));
}

#[test]
fn is_index_range_invalid_negative_range() {
    assert!(!is_index_range_invalid(10, 0, -1));
    assert!(!is_index_range_invalid(10, 0, -2));
    // negative range with non-negative step
    assert!(is_index_range_invalid(10, 0, 1));
    assert!(is_index_range_invalid(10, 0, 0));
}

#[test]
fn is_index_range_invalid_empty_range() {
    // beg == end is always valid regardless of step
    assert!(!is_index_range_invalid(5, 5, 0));
    assert!(!is_index_range_invalid(5, 5, 1));
    assert!(!is_index_range_invalid(5, 5, -1));
}

#[test]
fn is_index_range_invalid_unsigned() {
    assert!(!is_index_range_invalid(0_usize, 10, 1));
    assert!(!is_index_range_invalid(0_usize, 10, 3));
    assert!(is_index_range_invalid(0_usize, 10, 0));
}

// ============================================================================
// distance
// ============================================================================

#[test]
fn distance_positive_step() {
    assert_eq!(distance(0, 10, 1), 10);
    assert_eq!(distance(0, 10, 2), 5);
    assert_eq!(distance(0, 10, 3), 4); // ceil(10/3)
    assert_eq!(distance(0, 9, 3), 3);
    assert_eq!(distance(5, 20, 5), 3);
    assert_eq!(distance(0, 1, 1), 1);
    assert_eq!(distance(0, 0, 1), 0); // empty
}

#[test]
fn distance_negative_step() {
    assert_eq!(distance(10, 0, -1), 10);
    assert_eq!(distance(10, 0, -2), 5);
    assert_eq!(distance(10, 0, -3), 4);
    assert_eq!(distance(10, 1, -3), 3);
}

#[test]
fn distance_unsigned() {
    assert_eq!(distance(0_usize, 10, 1), 10);
    assert_eq!(distance(0_usize, 10, 2), 5);
    assert_eq!(distance(0_usize, 10, 3), 4);
}

#[test]
fn distance_matches_size() {
    // distance() and IndexRange<T>::size() must agree for every valid range.
    for beg in -5..=5 {
        for end in (beg + 1)..=10 {
            for step in 1..=4 {
                let r = IndexRange::new(beg, end, step);
                assert_eq!(distance(beg, end, step), r.size());
            }
        }
    }
}

// ============================================================================
// IndexRange<T> -- construction & accessors
// ============================================================================

#[test]
fn index_range_1d_basic_construction() {
    let r = IndexRange::new(0, 10, 2);
    assert_eq!(r.begin(), 0);
    assert_eq!(r.end(), 10);
    assert_eq!(r.step_size(), 2);
    assert_eq!(r.size(), 5);
}

#[test]
fn index_range_1d_negative_step() {
    let r = IndexRange::new(10, 0, -2);
    assert_eq!(r.begin(), 10);
    assert_eq!(r.end(), 0);
    assert_eq!(r.step_size(), -2);
    assert_eq!(r.size(), 5);
}

#[test]
fn index_range_1d_unit_step() {
    let r = IndexRange::new(3, 8, 1);
    assert_eq!(r.size(), 5);
    assert_eq!(enumerate(&r), vec![3, 4, 5, 6, 7]);
}

#[test]
fn index_range_1d_reset() {
    let mut r = IndexRange::new(0, 10, 1);
    r.reset(5, 20, 3);
    assert_eq!(r.begin(), 5);
    assert_eq!(r.end(), 20);
    assert_eq!(r.step_size(), 3);
    assert_eq!(r.size(), 5);
}

// ============================================================================
// IndexRange<T>::discrete_domain (upstream: unravel)
// ============================================================================

#[test]
fn index_range_1d_discrete_domain_basic() {
    let r = IndexRange::new(0, 10, 2); // elements: 0,2,4,6,8
    let sub = r.discrete_domain(1, 4); // positions [1,4) -> elements 2,4,6
    assert_eq!(sub.begin(), 2);
    assert_eq!(sub.end(), 8);
    assert_eq!(sub.step_size(), 2);
    assert_eq!(sub.size(), 3);
}

#[test]
fn index_range_1d_discrete_domain_full_range() {
    let r = IndexRange::new(0, 10, 1);
    let sub = r.discrete_domain(0, 10);
    assert_eq!(sub, r);
}

#[test]
fn index_range_1d_discrete_domain_single_element() {
    let r = IndexRange::new(0, 10, 2);
    let sub = r.discrete_domain(3, 4); // element at position 3 -> value 6
    assert_eq!(sub.begin(), 6);
    assert_eq!(sub.size(), 1);
    assert_eq!(sub.step_size(), 2);
}

#[test]
fn index_range_1d_discrete_domain_negative_step() {
    let r = IndexRange::new(10, 0, -2); // elements: 10,8,6,4,2
    let sub = r.discrete_domain(1, 4); // positions [1,4) -> 8,6,4
    assert_eq!(sub.begin(), 8);
    assert_eq!(sub.end(), 2);
    assert_eq!(sub.step_size(), -2);
    assert_eq!(sub.size(), 3);
}

#[test]
fn index_range_1d_discrete_domain_covers_partition() {
    // All partitions from a sweep must together cover the full range exactly
    // once.
    let r = IndexRange::new(0, 30, 3);
    let n = r.size(); // 10
    let mut visited = vec![0; n];

    let chunk = 3;
    let mut b = 0;
    while b < n {
        let e = (b + chunk).min(n);
        let sub = r.discrete_domain(b, e);
        for v in enumerate(&sub) {
            let pos = usize::try_from((v - r.begin()) / r.step_size()).unwrap();
            visited[pos] += 1;
        }
        b += chunk;
    }
    assert!(visited.iter().all(|&v| v == 1));
}

// ============================================================================
// Negative step
// ============================================================================

#[test]
fn negative_step_index_range_1d_size_and_elements() {
    // beg=10, end=0, step=-2 -> 10,8,6,4,2 (5 elements)
    let r = IndexRange::new(10, 0, -2);
    assert_eq!(r.size(), 5);
    assert_eq!(enumerate(&r), vec![10, 8, 6, 4, 2]);
}

#[test]
fn negative_step_index_range_1d_discrete_domain() {
    let r = IndexRange::new(10, 0, -2); // elements: 10,8,6,4,2
    // positions [1,4) -> elements 8,6,4
    let sub = r.discrete_domain(1, 4);
    assert_eq!(sub.begin(), 8);
    assert_eq!(sub.end(), 2);
    assert_eq!(sub.step_size(), -2);
    assert_eq!(sub.size(), 3);
    assert_eq!(enumerate(&sub), vec![8, 6, 4]);
}

#[test]
fn negative_step_index_range_1d_discrete_domain_full_coverage() {
    // Same sweep as the positive-step discrete_domain coverage test.
    let mut beg = 10;
    while beg >= 2 {
        for s in 1..=3 {
            let r = IndexRange::new(beg, 0, -s);
            let n = r.size();
            let mut visited = vec![0; n];
            let chunk = 3;
            let mut b = 0;
            while b < n {
                let e = (b + chunk).min(n);
                let sub = r.discrete_domain(b, e);
                for v in enumerate(&sub) {
                    let pos = usize::try_from((r.begin() - v) / s).unwrap();
                    visited[pos] += 1;
                }
                b += chunk;
            }
            assert!(visited.iter().all(|&v| v == 1));
        }
        beg -= 2;
    }
}

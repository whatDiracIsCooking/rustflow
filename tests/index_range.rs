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
//!
//! The `IndexRangeND.*` and `upper_slice.*`/`lower_slice.*` sections at the
//! bottom of this file cover `IndexRanges<T, N>` for `N > 1`: construction,
//! `size`, the per-dimension accessors, and `discrete_domain`'s own
//! generalization -- narrowing dimension 0 only, as
//! `src/algorithm/index_range.rs`'s `# Design` section explains. Upstream's
//! own `ceil`/`floor`/`upper_slice`/`lower_slice` tests have no cases here,
//! for the same reason the three 1-D cases above do not: this port has none
//! of those four methods.

use rustflow::algorithm::index_range::{IndexRange, IndexRanges, distance, is_index_range_invalid};

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

// ============================================================================
// IndexRanges<T, N> -- construction, size, rank, dim
// ============================================================================

// Position of `value` along a dimension with the given `(begin, step)`:
// the N-D analogue of the `(v - r.begin()) / r.step_size()` arithmetic the
// 1-D coverage tests above already use, generalized to a negative step too.
fn pos_of(beg: i32, step: i32, value: i32) -> usize {
    if step > 0 {
        usize::try_from((value - beg) / step).expect("value is on this dimension's own grid")
    } else {
        usize::try_from((beg - value) / (-step)).expect("value is on this dimension's own grid")
    }
}

// Enumerates every `(i, j)` an `IndexRanges<i32, 2>` produces, row-major --
// the direct Rust equivalent of upstream's `enumerate_2d` test helper.
fn enumerate_2d(r: &IndexRanges<i32, 2>) -> Vec<(i32, i32)> {
    let (b0, e0, s0) = r.dim(0);
    let (b1, e1, s1) = r.dim(1);
    let mut out = Vec::new();
    let mut i = b0;
    while if s0 > 0 { i < e0 } else { i > e0 } {
        let mut j = b1;
        while if s1 > 0 { j < e1 } else { j > e1 } {
            out.push((i, j));
            j += s1;
        }
        i += s0;
    }
    out
}

#[test]
fn index_ranges_nd_construction_2d() {
    let r = IndexRanges::from_ranges([IndexRange::new(0, 4, 1), IndexRange::new(0, 6, 1)]);
    assert_eq!(r.dim_size(0), 4);
    assert_eq!(r.dim_size(1), 6);
    assert_eq!(r.size(), 24);
}

#[test]
fn index_ranges_nd_construction_3d_non_unit_steps() {
    // dim 0: 4 elements, dim 1: 5 elements (step 2), dim 2: 3 elements (step 3).
    let r = IndexRanges::from_ranges([
        IndexRange::new(0, 4, 1),
        IndexRange::new(0, 10, 2),
        IndexRange::new(0, 9, 3),
    ]);
    assert_eq!(r.dim_size(0), 4);
    assert_eq!(r.dim_size(1), 5);
    assert_eq!(r.dim_size(2), 3);
    assert_eq!(r.size(), 4 * 5 * 3);
}

#[test]
fn index_ranges_nd_rank() {
    assert_eq!(IndexRanges::<i32, 2>::RANK, 2);
    assert_eq!(IndexRanges::<i32, 3>::RANK, 3);
    assert_eq!(IndexRanges::<i32, 4>::RANK, 4);
}

#[test]
fn index_ranges_nd_dim_accessor() {
    let r = IndexRanges::from_ranges([IndexRange::new(2, 8, 2), IndexRange::new(1, 7, 3)]);
    assert_eq!(r.dim(0), (2, 8, 2));
    assert_eq!(r.dim(1), (1, 7, 3));
}

// `dim_mut` rebinds one dimension's bounds in place: upstream's non-const
// `dim(d)`, which is what `IndexRange::reset` is for `N == 1`. Upstream's own
// suite has no case for either -- `index_range_1d_reset` above is this port's
// too -- so what is asserted is that the rebinding reaches every derived
// quantity and no other dimension.
#[test]
fn index_ranges_nd_dim_mut_rebinds_one_dimension() {
    let mut r = IndexRanges::from_ranges([IndexRange::new(0, 4, 1), IndexRange::new(0, 10, 2)]);
    assert_eq!((r.dim_size(0), r.dim_size(1)), (4, 5));
    assert_eq!(r.size(), 20);

    *r.dim_mut(1) = (6, 0, -2);

    assert_eq!(r.dim(1), (6, 0, -2), "the rebound dimension reads back");
    assert_eq!(r.dim(0), (0, 4, 1), "and no other dimension moved");
    assert_eq!(r.dim_size(1), 3, "6, 4 and 2 under a step of -2");
    assert_eq!(r.size(), 12, "the product follows the rebinding");

    let elements = enumerate_2d(&r);
    assert_eq!(elements.len(), 12, "and so does what the range enumerates");
    assert!(
        elements.iter().all(|&(_, j)| [6, 4, 2].contains(&j)),
        "every element of the rebound dimension is one of its new ones: {elements:?}"
    );
}

// Upstream's own worked `r3.size() == 120` example, named directly rather
// than only through the doctest that also carries it.
#[test]
fn index_ranges_nd_r3_size_is_120() {
    let r3 = IndexRanges::from_ranges([
        IndexRange::new(0, 4, 1),  // dim 0: 4 elements
        IndexRange::new(0, 10, 2), // dim 1: 5 elements (0,2,4,6,8)
        IndexRange::new(0, 6, 1),  // dim 2: 6 elements
    ]);
    assert_eq!(r3.size(), 120);
}

// ============================================================================
// IndexRanges<T, N>::discrete_domain
// ============================================================================

#[test]
fn index_ranges_nd_discrete_domain_basic() {
    let r = IndexRanges::from_ranges([IndexRange::new(0, 4, 1), IndexRange::new(0, 3, 1)]);
    // 3 elements per row of dim 0 -- position 3 is the start of row 1.
    let sub = r.discrete_domain(3, 9);
    assert_eq!(sub.dim(0), (1, 3, 1));
    assert_eq!(sub.dim(1), (0, 3, 1));
    assert_eq!(sub.size(), 6);
}

#[test]
fn index_ranges_nd_discrete_domain_full_range_round_trips() {
    let r = IndexRanges::from_ranges([IndexRange::new(0, 5, 1), IndexRange::new(0, 4, 1)]);
    let sub = r.discrete_domain(0, r.size());
    assert_eq!(sub, r);
}

#[test]
fn index_ranges_nd_discrete_domain_negative_step_on_the_split_dimension() {
    // dim 0 itself walks backward: 10, 7, 4, 1 (step -3).
    let r = IndexRanges::from_ranges([IndexRange::new(10, 0, -3), IndexRange::new(0, 4, 1)]);
    assert_eq!(r.dim_size(0), 4);
    assert_eq!(r.size(), 16);

    // positions [0, 8) are dim-0 outer positions 0 and 1 -> values 10, 7.
    let sub = r.discrete_domain(0, 8);
    assert_eq!(sub.dim(0), (10, 4, -3));
    assert_eq!(sub.dim(1), (0, 4, 1));
    assert_eq!(sub.size(), 8);
}

#[test]
fn index_ranges_nd_discrete_domain_mixed_steps_across_dimensions() {
    // dim 0 forward (step 1), dim 1 backward (step -2): a genuinely mixed
    // pair of signs, inner dimension untouched by the split.
    let r = IndexRanges::from_ranges([IndexRange::new(0, 5, 1), IndexRange::new(10, 0, -2)]);
    assert_eq!(r.dim_size(0), 5);
    assert_eq!(r.dim_size(1), 5);

    let sub = r.discrete_domain(5, 10); // dim-0 outer position 1 only.
    assert_eq!(sub.dim(0), (1, 2, 1));
    assert_eq!(sub.dim(1), (10, 0, -2)); // copied through unchanged.
    assert_eq!(sub.size(), 5);
}

#[test]
fn index_ranges_nd_discrete_domain_covers_partition_2d_mixed_steps() {
    // dim 0 forward, dim 1 backward: every (row, col) position is covered
    // by exactly one of the swept partitions.
    let r = IndexRanges::from_ranges([IndexRange::new(0, 5, 1), IndexRange::new(10, 0, -2)]);
    let (b0, _e0, s0) = r.dim(0);
    let (b1, _e1, s1) = r.dim(1);
    let rows = r.dim_size(0);
    let cols = r.dim_size(1);
    let mut visited = vec![0; rows * cols];

    let chunk_rows = 2; // does not evenly divide `rows` (5).
    let mut row_beg = 0;
    while row_beg < rows {
        let row_end = (row_beg + chunk_rows).min(rows);
        let sub = r.discrete_domain(row_beg * cols, row_end * cols);
        for (i, j) in enumerate_2d(&sub) {
            let row = pos_of(b0, s0, i);
            let col = pos_of(b1, s1, j);
            visited[row * cols + col] += 1;
        }
        row_beg = row_end;
    }
    assert!(visited.iter().all(|&v| v == 1));
}

#[test]
fn index_ranges_nd_discrete_domain_covers_partition_3d() {
    let r = IndexRanges::from_ranges([
        IndexRange::new(0, 4, 1),
        IndexRange::new(6, 0, -2),
        IndexRange::new(0, 5, 1),
    ]);
    let dims: [(i32, i32, i32); 3] = [r.dim(0), r.dim(1), r.dim(2)];
    let sizes = [r.dim_size(0), r.dim_size(1), r.dim_size(2)];
    let inner_volume = sizes[1] * sizes[2];
    let mut visited = vec![0; sizes[0] * sizes[1] * sizes[2]];

    let chunk_rows = 3; // does not evenly divide `sizes[0]` (4).
    let mut row_beg = 0;
    while row_beg < sizes[0] {
        let row_end = (row_beg + chunk_rows).min(sizes[0]);
        let sub = r.discrete_domain(row_beg * inner_volume, row_end * inner_volume);

        let (b0, e0, s0) = sub.dim(0);
        let (b1, e1, s1) = sub.dim(1);
        let (b2, e2, s2) = sub.dim(2);
        let mut i = b0;
        while if s0 > 0 { i < e0 } else { i > e0 } {
            let mut j = b1;
            while if s1 > 0 { j < e1 } else { j > e1 } {
                let mut k = b2;
                while if s2 > 0 { k < e2 } else { k > e2 } {
                    let row = pos_of(dims[0].0, dims[0].2, i);
                    let col = pos_of(dims[1].0, dims[1].2, j);
                    let dep = pos_of(dims[2].0, dims[2].2, k);
                    visited[(row * sizes[1] + col) * sizes[2] + dep] += 1;
                    k += s2;
                }
                j += s1;
            }
            i += s0;
        }
        row_beg = row_end;
    }
    assert!(visited.iter().all(|&v| v == 1));
}

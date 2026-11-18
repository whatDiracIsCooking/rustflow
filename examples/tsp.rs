//! Translated from `examples/tsp.cpp` (Taskflow v4.1.0).
//!
//! A parallel branch-and-bound solver for the travelling-salesman problem
//! over a hand-built four-city instance, using an `Executor::task_group` to
//! spawn one branch per unvisited city and `TaskGroup::corun` to wait on
//! them. Upstream shows the answer only as a printed number next to a comment
//! claiming it is optimal; the port additionally solves the same instance by
//! brute force and asserts the two agree, which is what #197 asks for in
//! place of a number taken on faith.
//!
//! Upstream's program only *prints* as it goes. This milestone's examples
//! print *and* **assert**: every line upstream shows is also checked with
//! `assert!` or `assert_eq!`, so a non-zero exit from the binary is itself
//! the test. See the README's "Examples" section.

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use rustflow::Executor;

/// Upstream's own name, kept for the same reason its comment gives: `N` is a
/// template parameter deep in this crate's own internals too, and the name
/// reads better next to the distance matrix below regardless.
const NUM_CITIES: usize = 4;

/// Upstream's `DIST`: city `A` through `D`, indices 0 through 3. The
/// optimal tour of this instance is `A -> B -> D -> C -> A`, cost 80 --
/// upstream's own comment, and what `main` checks for below.
const DIST: [[i32; NUM_CITIES]; NUM_CITIES] = [
    [0, 10, 15, 20],
    [10, 0, 35, 25],
    [15, 35, 0, 30],
    [20, 25, 30, 0],
];

/// A floor on the best possible completion of a partial tour: upstream's
/// `lower_bound`. See its own comment for why summing these two components
/// is a valid lower bound: the cheapest edge out of `current`, plus the
/// cheapest edge touching each city not yet visited.
fn lower_bound(current: usize, visited: &[bool], cost_so_far: i32) -> i32 {
    let mut bound = cost_so_far;

    let Some(cheapest_from_current) = (0..NUM_CITIES)
        .filter(|&v| !visited[v])
        .map(|v| DIST[current][v])
        .min()
    else {
        // no unvisited cities left: the tour is already complete
        return bound;
    };
    bound += cheapest_from_current;

    for u in 0..NUM_CITIES {
        if visited[u] {
            continue;
        }
        let cheapest_from_u = (0..NUM_CITIES)
            .filter(|&v| v != u)
            .map(|v| DIST[u][v])
            .min()
            .expect("NUM_CITIES > 1, so every city has another to go to");
        bound += cheapest_from_u;
    }
    bound
}

/// One node of the search tree: upstream's `branch_and_bound`.
///
/// Spawns one branch per unvisited city except the first, which runs
/// in-line on this worker to avoid task overhead for a branch with nothing
/// else outstanding yet, and `TaskGroup::corun`s to wait for the rest --
/// upstream's own comments give both reasons. `executor` and `best_cost` are
/// `Arc`s rather than upstream's file-scope globals captured by reference,
/// which is this port's answer to the same sharing.
fn branch_and_bound(
    executor: &Arc<Executor>,
    best_cost: &Arc<AtomicI32>,
    current: usize,
    visited: Vec<bool>,
    cost_so_far: i32,
    depth: usize,
) {
    // compute the lower bound and prune if it cannot improve on best_cost
    if lower_bound(current, &visited, cost_so_far) >= best_cost.load(Ordering::Relaxed) {
        return;
    }

    // all cities visited: close the tour by returning to city 0
    if depth == NUM_CITIES {
        let total = cost_so_far + DIST[current][0];
        let mut observed = best_cost.load(Ordering::Relaxed);
        while total < observed {
            match best_cost.compare_exchange_weak(
                observed,
                total,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current_value) => observed = current_value,
            }
        }
        return;
    }

    // `branch_and_bound` only ever runs inside a task body -- the root call
    // in `main` is submitted through `Executor::spawn` for exactly this
    // reason -- so opening a group here never fails.
    let group = executor
        .task_group()
        .expect("branch_and_bound only ever runs on a worker");

    let mut first = true;
    for v in 0..NUM_CITIES {
        if visited[v] {
            continue;
        }

        let mut new_visited = visited.clone();
        new_visited[v] = true;
        let new_cost = cost_so_far + DIST[current][v];

        if first {
            // run the first unvisited branch directly on this worker to
            // avoid unnecessary task creation overhead
            first = false;
            branch_and_bound(executor, best_cost, v, new_visited, new_cost, depth + 1);
        } else {
            // spawn the remaining branches as independent tasks of the
            // group; all of them share `best_cost`, so a better solution
            // found in any branch immediately tightens the pruning bound
            // for every other branch.
            let executor = Arc::clone(executor);
            let best_cost = Arc::clone(best_cost);
            group.silent_async(move || {
                branch_and_bound(&executor, &best_cost, v, new_visited, new_cost, depth + 1);
            });
        }
    }

    // cooperatively wait for all spawned branches: corun re-enters the
    // executor's work-stealing loop rather than blocking, which is what
    // keeps deep recursion from starving the pool.
    group.corun();
}

/// Every permutation of `remaining`, each handed to `visit` once appended to
/// `chosen`: a small hand-rolled permutation walk, since the crate has no
/// dependency that would do it and `NUM_CITIES - 1` is three.
fn permute(remaining: &[usize], chosen: &mut Vec<usize>, visit: &mut impl FnMut(&[usize])) {
    if remaining.is_empty() {
        visit(chosen);
        return;
    }
    for (i, &city) in remaining.iter().enumerate() {
        let mut rest = remaining.to_vec();
        rest.remove(i);
        chosen.push(city);
        permute(&rest, chosen, visit);
        chosen.pop();
    }
}

/// The same instance, solved by brute force over every permutation of the
/// three cities other than the start: the serial oracle the parallel search
/// above is checked against.
fn serial_best() -> i32 {
    let others: Vec<usize> = (1..NUM_CITIES).collect();
    let mut best = i32::MAX;

    permute(&others, &mut Vec::new(), &mut |order: &[usize]| {
        let mut cost = 0;
        let mut current = 0;
        for &next in order {
            cost += DIST[current][next];
            current = next;
        }
        cost += DIST[current][0];
        best = best.min(cost);
    });

    best
}

fn main() {
    let executor = Arc::new(Executor::new(4));
    let best_cost = Arc::new(AtomicI32::new(i32::MAX));

    let mut visited = vec![false; NUM_CITIES];
    visited[0] = true; // start from city A (index 0)

    // the root call must be submitted as an async task so it runs inside a
    // worker thread, which is required for `Executor::task_group` to be valid
    executor
        .spawn({
            let executor = Arc::clone(&executor);
            let best_cost = Arc::clone(&best_cost);
            move || branch_and_bound(&executor, &best_cost, 0, visited, 0, 1)
        })
        .wait()
        .expect("no branch panicked");

    let found = best_cost.load(Ordering::Relaxed);
    println!("Optimal tour cost: {found}");

    // the tour length checked against a serial solve of the same instance,
    // rather than against a number taken on faith from upstream's comment
    assert_eq!(found, serial_best());
    assert_eq!(found, 80);
}

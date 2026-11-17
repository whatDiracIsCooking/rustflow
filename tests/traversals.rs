//! Translated from `unittests/test_traversals.cpp` (Taskflow v4.1.0).
//!
//! Upstream's file is a thousand-node DAG walked three ways. Only the first of
//! the three is a static graph, and it is the one translated here:
//! `StaticTraversal` at one through four workers, upstream's own sweep.
//!
//! - **`DynamicTraversal`** discovers the graph as it runs, emplacing one
//!   subflow task per node whose last dependency it just released, so it needs
//!   subflows: milestone 4 (#50), with the stress half of that milestone at
//!   #54.
//! - **`ParallelTraversal`** is `DynamicTraversal` again from `W` concurrent
//!   threads over one shared executor -- and it is commented out in upstream's
//!   own file, every case of it, so it is not a case that passes there either.
//!   Its traversal body is the same subflow recursion, so it waits on the same
//!   milestone. What is here under that name instead is labelled for what it
//!   is: not a translation, but the static traversal driven from `W` threads at
//!   once, which is the half of upstream's case that this port can run today.
//!   [`parallel_static_traversal`] says what it does and does not cover.
//!
//! Two notes on the translation itself:
//!
//! - **Upstream's `Node` is a raw-pointer graph beside the taskflow**, and its
//!   task bodies write `visited` and `level` through those pointers while
//!   other bodies read them. The port keeps the second graph -- it is what the
//!   case checks the taskflow *against* -- as a shared slice of nodes holding
//!   atomics, with successors as indices rather than pointers. The index is
//!   also upstream's own `idx` field, which exists precisely so a successor
//!   pointer can be turned back into a task position.
//! - **Relaxed is enough for all three fields.** The level tickets come from
//!   one `fetch_add` on one counter, so they are totally ordered however they
//!   are read; what makes a predecessor's ticket *smaller* than its
//!   successor's is the dependency edge between the two bodies, which is the
//!   executor's own synchronisation, and what makes the final assertions see
//!   every write is the end of the run. Upstream writes two of the three
//!   fields non-atomically for the same reason.
//!
//! `make_chain` from upstream's header is not ported: nothing in the file uses
//! it, in upstream or here.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

use rustflow::Executor;
use rustflow::prelude::*;

/// Upstream's `struct Node`: the graph the taskflow is checked against.
#[derive(Debug, Default)]
struct Node {
    /// Indices into the same slice, upstream's `std::vector<Node*>`.
    successors: Vec<usize>,
    /// Upstream's `dependents`, counted up as edges are added and back down
    /// by the task bodies.
    dependents: AtomicUsize,
    /// The order this node's body ran in, one-based. Upstream's `level`.
    level: AtomicUsize,
    /// Whether this node's body ran at all. Upstream's `visited`.
    visited: AtomicBool,
}

/// Upstream's `make_dag(num_nodes, max_degree)`.
///
/// Deterministic, in upstream too: node `i` precedes the first `max_degree`
/// odd-numbered nodes after it, which is why half the nodes are sources and
/// the levels fan out the way they do. No seed to carry, and nothing for the
/// crate's `Xorshift` to replace.
fn make_dag(num_nodes: usize, max_degree: usize) -> Arc<Vec<Node>> {
    let mut nodes: Vec<Node> = (0..num_nodes).map(|_| Node::default()).collect();

    for i in 0..num_nodes {
        let mut degree = 0;
        for j in i + 1..num_nodes {
            if degree >= max_degree {
                break;
            }
            if !j.is_multiple_of(2) {
                nodes[i].successors.push(j);
                nodes[j].dependents.fetch_add(1, Ordering::Relaxed);
                degree += 1;
            }
        }
    }

    Arc::new(nodes)
}

// ----------------------------------------------------------------------------
// Testcase: StaticTraversal
// ----------------------------------------------------------------------------

/// Upstream's `static_traversal(W)`, less the executor it builds for itself:
/// one task per node, one edge per node edge, and the node's own body as the
/// only thing that records what ran when.
///
/// The assertions are upstream's three, and the third is the one that matters:
/// a node's level has to be strictly below every successor's, so a single
/// task released before one of its dependencies -- the failure a work-stealing
/// scheduler can produce out of nothing but a mis-ordered counter -- shows up
/// as a pair of levels in the wrong order rather than as a crash.
fn traverse(executor: &Executor) {
    const MAX_DEGREE: usize = 4;
    const NUM_NODES: usize = 1000;

    let nodes = make_dag(NUM_NODES, MAX_DEGREE);

    let mut taskflow = Taskflow::new();
    let level = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::with_capacity(NUM_NODES);

    for i in 0..NUM_NODES {
        let task = {
            let nodes = Arc::clone(&nodes);
            let level = Arc::clone(&level);
            taskflow.emplace(move || {
                let node = &nodes[i];
                node.level
                    .store(level.fetch_add(1, Ordering::Relaxed) + 1, Ordering::Relaxed);
                node.visited.store(true, Ordering::Relaxed);
                for &successor in &node.successors {
                    nodes[successor].dependents.fetch_sub(1, Ordering::Relaxed);
                }
            })
        };
        taskflow.task(task).name(i.to_string());
        tasks.push(task);
    }

    for (i, node) in nodes.iter().enumerate() {
        let successors: Vec<Task> = node.successors.iter().map(|&j| tasks[j]).collect();
        taskflow.task(tasks[i]).precede(successors);
    }

    executor
        .run(&taskflow)
        .wait()
        .expect("a task of this taskflow panicked");

    for (i, node) in nodes.iter().enumerate() {
        assert!(node.visited.load(Ordering::Relaxed), "node {i} never ran");
        assert_eq!(
            node.dependents.load(Ordering::Relaxed),
            0,
            "node {i} was left with unreleased dependencies"
        );
        for &successor in &node.successors {
            assert!(
                node.level.load(Ordering::Relaxed) < nodes[successor].level.load(Ordering::Relaxed),
                "node {i} ran at level {} but its successor {successor} ran at level {}",
                node.level.load(Ordering::Relaxed),
                nodes[successor].level.load(Ordering::Relaxed)
            );
        }
    }
}

/// Upstream's `static_traversal(W)`.
fn static_traversal(workers: usize) {
    traverse(&Executor::new(workers));
}

#[test]
fn static_traversal_1_worker() {
    static_traversal(1);
}

#[test]
fn static_traversal_2_workers() {
    static_traversal(2);
}

#[test]
fn static_traversal_3_workers() {
    static_traversal(3);
}

#[test]
fn static_traversal_4_workers() {
    static_traversal(4);
}

// ----------------------------------------------------------------------------
// Not a testcase of upstream's: the static half of ParallelTraversal
// ----------------------------------------------------------------------------

/// `W` threads, one shared executor, and a whole thousand-node traversal each.
///
/// **This is not a translation.** Upstream's `ParallelTraversal` runs
/// `DynamicTraversal`'s subflow recursion from `W` threads, and every case of
/// it is commented out in upstream's file. The recursion is milestone 4; what
/// this keeps is the other thing that case does, which is to put `W`
/// concurrent runs of an independent large graph on one pool and let them
/// compete -- each thread building its own DAG, its own taskflow and its own
/// level counter, and checking its own graph afterwards. A run that leaked a
/// job into another run's topology, or a submitter that stole from a queue it
/// had no business in, fails somebody's level ordering here.
///
/// It is therefore weaker than upstream's case in the obvious way and worth
/// replacing with the real thing at #54 rather than keeping both.
fn parallel_static_traversal(threads: usize) {
    let executor = Executor::new(threads);

    thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| traverse(&executor));
        }
    });
}

#[test]
fn parallel_static_traversal_1_thread() {
    parallel_static_traversal(1);
}

#[test]
fn parallel_static_traversal_2_threads() {
    parallel_static_traversal(2);
}

#[test]
fn parallel_static_traversal_3_threads() {
    parallel_static_traversal(3);
}

#[test]
fn parallel_static_traversal_4_threads() {
    parallel_static_traversal(4);
}

#[test]
fn parallel_static_traversal_5_threads() {
    parallel_static_traversal(5);
}

#[test]
fn parallel_static_traversal_6_threads() {
    parallel_static_traversal(6);
}

#[test]
fn parallel_static_traversal_7_threads() {
    parallel_static_traversal(7);
}

#[test]
fn parallel_static_traversal_8_threads() {
    parallel_static_traversal(8);
}

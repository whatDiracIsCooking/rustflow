//! Translated from `unittests/test_traversals.cpp` (Taskflow v4.1.0).
//!
//! Upstream's file is a thousand-node DAG walked three ways, and all three are
//! here: `StaticTraversal` at one through four workers, `DynamicTraversal` at
//! the same four, and `ParallelTraversal` -- the dynamic walk from `W`
//! concurrent submitter threads over one shared executor -- at one through
//! eight, which are upstream's own sweeps. The two dynamic ones arrived with
//! #54 once subflows existed (#50); only the static walk was portable before.
//!
//! Two notes about what the dynamic cases are, because neither is obvious from
//! the shape:
//!
//! - **`DynamicTraversal` discovers the graph as it runs.** There is one task
//!   per *source* node in the taskflow and nothing else: a node's body
//!   decrements each successor's dependency count, and the body that takes a
//!   count to zero emplaces the successor as a subflow task of its own
//!   subflow. So the taskflow holds five hundred tasks and the run nests
//!   graphs as deep as the order the predecessors happened to finish in puts
//!   it -- measured here, by counting the nesting from inside the bodies:
//!   around four hundred and twenty graphs deep at two workers and close to
//!   five hundred at four, which is the DAG's longest path, and one single
//!   level at one worker, where each node is released by the source just
//!   below it before the level above has a chance to. Every level is joined
//!   implicitly by the level above, and the three assertions at the end are
//!   the same three the static walk makes: a level that is not strictly below
//!   its successor's is a subflow that was not joined before its parent node
//!   finished.
//! - **`ParallelTraversal` is that walk from `W` threads at once**, each with
//!   its own DAG, taskflow and level counter, on one executor they share. It
//!   is commented out in upstream's file, every case of it -- which is why a
//!   stand-in stood here until #54 -- and nothing in the shape needs the
//!   comment: a failed assertion inside a scoped thread is a failed test
//!   here, where doctest's `REQUIRE` is not safe to call from the threads
//!   upstream's version spawns.
//!
//! What stood here under `ParallelTraversal`'s name until #54 -- the *static*
//! traversal driven from `W` submitter threads, labelled as not a translation
//! -- is gone rather than kept as an extra. It was the half of upstream's
//! case this port could run before subflows, and the real translation is
//! strictly stronger: the same `W` concurrent submitters over one executor,
//! with a payload that also nests hundreds of graphs deep per thread.
//!
//! Three notes on the translation itself:
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
//! - **`REQUIRE(!n->visited)` becomes a `swap`.** Upstream's dynamic walk
//!   checks that it has not been here before by reading a `bool` it is about
//!   to write; one atomic `swap` says the same thing and says it without a
//!   race, which matters because the whole question is whether two bodies can
//!   reach one node.
//!
//! `make_chain` from upstream's header is not ported: nothing in the file uses
//! it, in upstream or here.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

use rustflow::Executor;
use rustflow::prelude::*;

// Only the matrix macro: every counter here is a field of the node struct the
// cases walk rather than a share handed to a body, so there is nothing for
// `common/counters.rs` or `common/bodies.rs` to carry.
#[macro_use]
mod common;

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

/// Upstream's three closing `REQUIRE`s, which are the same three for all
/// three walks.
///
/// The third is the one that matters: a node's level has to be strictly below
/// every successor's, so a single task released before one of its
/// dependencies -- the failure a work-stealing scheduler can produce out of
/// nothing but a mis-ordered counter -- shows up as a pair of levels in the
/// wrong order rather than as a crash.
fn assert_walked_in_order(nodes: &[Node]) {
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

/// Upstream's `max_degree` and `num_nodes`, shared by all three walks.
const MAX_DEGREE: usize = 4;
/// How many nodes the DAG has, upstream's `num_nodes`.
const NUM_NODES: usize = 1000;

// ----------------------------------------------------------------------------
// Testcase: StaticTraversal
// ----------------------------------------------------------------------------

/// Upstream's `static_traversal(W)`, less the executor it builds for itself:
/// one task per node, one edge per node edge, and the node's own body as the
/// only thing that records what ran when.
fn traverse(executor: &Executor) {
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

    assert_walked_in_order(&nodes);
}

/// Upstream's `static_traversal(W)`.
fn static_traversal(workers: usize) {
    traverse(&Executor::new(workers));
}

worker_matrix! {
    static_traversal {
        static_traversal_1_worker(1),
        static_traversal_2_workers(2),
        static_traversal_3_workers(3),
        static_traversal_4_workers(4),
    }
}

// ----------------------------------------------------------------------------
// Testcase: DynamicTraversal
// ----------------------------------------------------------------------------

/// Upstream's `traverse` lambda, which is a `std::function` precisely so that
/// it can hand itself to the subflow it spawns. A free function here, for the
/// same reason the bodies are `'static`: there is nothing to capture but the
/// two shares, and a recursive closure would need a name of its own anyway.
fn traverse_from(
    nodes: &Arc<Vec<Node>>,
    index: usize,
    level: &Arc<AtomicUsize>,
    subflow: &mut Subflow<'_>,
) {
    assert!(
        !nodes[index].visited.swap(true, Ordering::Relaxed),
        "node {index} was reached twice"
    );

    for &successor in &nodes[index].successors {
        // The body that takes a successor's count to zero is the one that
        // owns it from here: it stamps the level and spawns the walk.
        if nodes[successor].dependents.fetch_sub(1, Ordering::Relaxed) == 1 {
            nodes[successor]
                .level
                .store(level.fetch_add(1, Ordering::Relaxed) + 1, Ordering::Relaxed);

            let nodes = Arc::clone(nodes);
            let level = Arc::clone(level);
            subflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
                traverse_from(&nodes, successor, &level, subflow);
            });
        }
    }
}

/// Upstream's `dynamic_traversal(W)`, less the executor it builds for itself.
///
/// The taskflow holds one subflow task per source node of the DAG and no
/// edges at all; everything else is discovered. The sources keep level 0,
/// which they do upstream too -- nobody stamps them -- and the tickets start
/// at 1, so the ordering assertion holds for them as well.
fn traverse_dynamically(executor: &Executor) {
    let nodes = make_dag(NUM_NODES, MAX_DEGREE);
    let level = Arc::new(AtomicUsize::new(0));

    let mut taskflow = Taskflow::new();
    for (i, node) in nodes.iter().enumerate() {
        if node.dependents.load(Ordering::Relaxed) != 0 {
            continue;
        }
        let nodes = Arc::clone(&nodes);
        let level = Arc::clone(&level);
        let source = taskflow.emplace_subflow(move |subflow: &mut Subflow<'_>| {
            traverse_from(&nodes, i, &level, subflow);
        });
        taskflow.task(source).name(i.to_string());
    }

    executor
        .run(&taskflow)
        .wait()
        .expect("a task of this taskflow panicked");

    assert_walked_in_order(&nodes);
}

/// Upstream's `dynamic_traversal(W)`.
fn dynamic_traversal(workers: usize) {
    traverse_dynamically(&Executor::new(workers));
}

worker_matrix! {
    dynamic_traversal {
        dynamic_traversal_1_worker(1),
        dynamic_traversal_2_workers(2),
        dynamic_traversal_3_workers(3),
        dynamic_traversal_4_workers(4),
    }
}

// ----------------------------------------------------------------------------
// Testcase: ParallelTraversal
// ----------------------------------------------------------------------------

/// Upstream's `parallel_traversal(W)`: `W` threads, one shared executor, and
/// a whole thousand-node dynamic walk each.
///
/// A run that leaked a job into another run's topology, or a submitter that
/// stole from a queue it had no business in, fails somebody's level ordering
/// here; a subgraph that went home to the wrong node fails the `visited`
/// swap. Upstream spawns `std::thread`s and joins them; a scoped thread is
/// the same thing with the join guaranteed, which is what lets each thread
/// keep its DAG on its own stack as upstream does.
fn parallel_traversal(threads: usize) {
    let executor = Executor::new(threads);

    thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| traverse_dynamically(&executor));
        }
    });
}

worker_matrix! {
    parallel_traversal {
        parallel_traversal_1_thread(1),
        parallel_traversal_2_threads(2),
        parallel_traversal_3_threads(3),
        parallel_traversal_4_threads(4),
        parallel_traversal_5_threads(5),
        parallel_traversal_6_threads(6),
        parallel_traversal_7_threads(7),
        parallel_traversal_8_threads(8),
    }
}

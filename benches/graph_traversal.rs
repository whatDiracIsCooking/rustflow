//! Translated from `benchmarks/graph_traversal/` (Taskflow v4.1.0).
//!
//! Upstream's `LevelGraph`: `levels` rows of `length` nodes, every edge going
//! from a node in one row to a node in the next, one to four of them per node.
//! Unlike the chain and the tree it is an irregular DAG with real joins -- a
//! node may have several predecessors -- so it is the shape that exercises the
//! join counter rather than the queues.
//!
//! Two deliberate departures from upstream's generator, both because the
//! benchmark is about the executor and not about the graph:
//!
//! - **The random graph is ours.** Upstream seeds `std::mt19937` and
//!   `std::rand()` with 0, and `std::rand()` is libc's, so its graph is not
//!   reproducible off its own machine in the first place. This uses the port's
//!   own [`Xorshift64`] with a fixed seed, which makes *this* column
//!   reproducible, and follows upstream's construction exactly otherwise: a
//!   shuffled permutation of the next row, sliced into runs of 1 to 4, with a
//!   reshuffle whenever a slice reaches its end. Both give every node in rows
//!   1.. at least one predecessor and leave row 0 as the sources.
//! - **No `BFS` and no `_chosen`.** Upstream's `mark()` runs a backward BFS on
//!   a node drawn with probability `threshold`, and `main.cpp` leaves
//!   `threshold` at its default `0.0f` -- on top of which upstream's draw,
//!   `std::rand()/(RAND_MAX + 1u)`, is integer division and so always 0. No
//!   node is ever chosen in the benchmark as published, so the BFS is dead
//!   code there and absent here. What is left of `mark()` is its
//!   floating-point loop, which upstream carries a second time as
//!   `embarrassing_parallelism.hpp`'s `dummy` and this file reads from
//!   `common/body.rs`.
//!
//! **The rayon variant is level-synchronous**, which is what rayon has for
//! this shape: `par_iter` over a row, then the next row. It respects every
//! dependency, since edges only ever go one row forward, but it synchronises
//! more than the graph demands -- the last node of row `l` gates the first
//! node of row `l + 1` even with no edge between them. That is the comparison
//! on offer: a task graph against the barrier you would write without one.
//! The graph is generated outside the measured region, as upstream generates
//! it in `main`, while the taskflow is built inside it, as upstream builds its
//! `TF` struct inside `measure_time_taskflow`.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rustflow::prelude::*;
use rustflow::{Executor, Xorshift64};

#[path = "common/baseline.rs"]
mod baseline;
#[path = "common/body.rs"]
mod body;
mod common;

/// Graph dimensions, as upstream's `LevelGraph(i, i)`: `i` rows of `i` nodes.
///
/// Upstream sweeps `i` over the powers of two up to 2048, whose last graph is
/// four million nodes. These are two points on that sweep.
const DIMENSIONS: [usize; 2] = [32, 128];

/// Upstream's `_edge_max`: a node gets `1 + rand() % 4` out-edges.
const EDGE_MAX: u64 = 4;

/// The generator seed. Upstream's is 0 for both of its two generators; this one
/// exists so the graph is the same graph on every run of this benchmark.
const SEED: u64 = 1;

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// Upstream's `LevelGraph`, reduced to the only thing either variant reads:
/// `out_edges[level][node]`, the indices in row `level + 1` that this node
/// precedes. The last row's entry is generated, as upstream generates it, and
/// used by neither variant.
struct LevelGraph {
    /// Rows of nodes, each holding its out-edges into the next row.
    out_edges: Vec<Vec<Vec<usize>>>,
    /// Nodes per row.
    length: usize,
}

impl LevelGraph {
    /// Generates a graph of `levels` rows of `length` nodes, following
    /// upstream's constructor.
    fn new(levels: usize, length: usize) -> Self {
        let mut rng = Xorshift64::new(SEED);
        let mut out_edges = Vec::with_capacity(levels);

        for _ in 0..levels {
            let mut next_row: Vec<usize> = (0..length).collect();
            shuffle(&mut next_row, &mut rng);

            let mut row = Vec::with_capacity(length);
            let mut start = 0;

            for _ in 0..length {
                let edges = 1 + usize::try_from(rng.next_u64() % EDGE_MAX)
                    .expect("a value below EDGE_MAX fits a usize");

                // Upstream's rule, including the part that makes the slice
                // cover the whole permutation before reshuffling: a run that
                // would overshoot the row takes the rest of it instead.
                let (end, reshuffle) = if start + edges >= length {
                    (length, true)
                } else {
                    (start + edges, false)
                };

                row.push(next_row[start..end].to_vec());

                if reshuffle {
                    shuffle(&mut next_row, &mut rng);
                    start = 0;
                } else {
                    start = end;
                }
            }

            out_edges.push(row);
        }

        Self { out_edges, length }
    }

    /// Rows in the graph.
    fn levels(&self) -> usize {
        self.out_edges.len()
    }

    /// Nodes in the graph: upstream's `|V|`.
    fn num_nodes(&self) -> usize {
        self.levels() * self.length
    }

    /// Edges the taskflow actually declares: upstream's `|E|` minus the last
    /// row's, which nothing links.
    fn num_edges(&self) -> usize {
        self.out_edges
            .iter()
            .take(self.levels().saturating_sub(1))
            .flatten()
            .map(Vec::len)
            .sum()
    }
}

/// Fisher-Yates over `items`, standing in for `std::shuffle` with a seeded
/// `std::mt19937`.
fn shuffle(items: &mut [usize], rng: &mut Xorshift64) {
    for i in (1..items.len()).rev() {
        let bound = u64::try_from(i).expect("an index fits a u64") + 1;
        let j = usize::try_from(rng.next_u64() % bound).expect("a value below bound fits a usize");
        items.swap(i, j);
    }
}

/// Emplaces one row of marking tasks -- upstream's `mark()`, which is
/// [`body::dummy`] keyed on the node's index within its row as upstream
/// keys it on `_idx` -- and hands back their handles.
fn emplace_row(taskflow: &mut Taskflow, length: usize, counter: &Arc<AtomicUsize>) -> Vec<Task> {
    (0..length)
        .map(|i| {
            let counter = Arc::clone(counter);
            let index = u64::try_from(i).expect("a node index fits a u64");
            taskflow.emplace(move || {
                black_box(body::dummy(index));
                counter.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect()
}

/// Builds the graph as a taskflow and traverses it once. Upstream's `TF`
/// struct and `traverse_level_graph_taskflow`.
fn traverse_rustflow(graph: &LevelGraph, executor: &Executor) {
    let counter = Arc::new(AtomicUsize::new(0));
    let mut taskflow = Taskflow::new();

    // Upstream's order: the last row first, then each row above it, so that a
    // node's successors already exist when its edges are added.
    let mut next = emplace_row(&mut taskflow, graph.length, &counter);
    for level in (0..graph.levels().saturating_sub(1)).rev() {
        let current = emplace_row(&mut taskflow, graph.length, &counter);
        for (i, task) in current.iter().enumerate() {
            let successors = graph.out_edges[level][i].iter().map(|&k| next[k]);
            taskflow.task(*task).precede(successors);
        }
        next = current;
    }

    executor.run(&taskflow).wait().expect("no task panicked");

    // Upstream's `validate_result()`, which checks every node's `_visited`
    // flag; counting the visits answers the same question, since each task
    // runs exactly once per run.
    assert_eq!(
        counter.load(Ordering::Relaxed),
        graph.num_nodes(),
        "every node in the graph is visited exactly once"
    );
}

/// The same traversal as a row-at-a-time rayon iterator. See the module docs
/// for why that is a different schedule and still a fair baseline.
fn traverse_rayon(graph: &LevelGraph, pool: &rayon::ThreadPool) {
    let counter = AtomicUsize::new(0);
    let length = u64::try_from(graph.length).expect("a row length fits a u64");

    pool.install(|| {
        for _ in 0..graph.levels() {
            (0..length).into_par_iter().for_each(|i| {
                black_box(body::dummy(i));
                counter.fetch_add(1, Ordering::Relaxed);
            });
        }
    });

    assert_eq!(
        counter.load(Ordering::Relaxed),
        graph.num_nodes(),
        "every node in the graph is visited exactly once"
    );
}

/// Registers both variants at every size and worker count.
fn bench_graph_traversal(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_traversal");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    for workers in common::worker_counts() {
        let executor = common::executor(workers);
        let pool = baseline::rayon_pool(workers);

        for dimension in DIMENSIONS {
            let graph = LevelGraph::new(dimension, dimension);
            // Upstream reports `|V| + |E|` as the size of the problem.
            let size = graph.num_nodes() + graph.num_edges();

            group.bench_function(
                BenchmarkId::new(format!("rustflow/{workers}w"), size),
                |b| {
                    b.iter(|| traverse_rustflow(black_box(&graph), &executor));
                },
            );
            group.bench_function(BenchmarkId::new(format!("rayon/{workers}w"), size), |b| {
                b.iter(|| traverse_rayon(black_box(&graph), &pool));
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_graph_traversal);
criterion_main!(benches);

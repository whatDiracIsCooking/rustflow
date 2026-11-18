//! Translated from `benchmarks/graph_pipeline/` (Taskflow v4.1.0).
//!
//! A [`Pipeline`] run over a `LevelGraph` rather than over a token stream of
//! its own: upstream's three pipe roles are `Input`, which walks the graph's
//! nodes level by level and writes the node it is on into the running line's
//! slot of a `num_lines`-wide buffer, `Filter`, which re-does that node's
//! work, and `FilterFinal`, which adds up the node's predecessors' values
//! from the level above and writes the node's final value back. The chain is
//! `composed_of` into a [`Taskflow`] and run once, which is what
//! `measure_time_taskflow` times and so what the measured region here
//! contains.
//!
//! The chain is [`NUM_PIPES`] pipes -- upstream's
//! `graph_pipeline_taskflow_4_pipes`, one of the sixteen pipe counts its `-p`
//! option selects between -- over [`NUM_LINES`] lines, upstream's own `-l`
//! default. Four rather than upstream's default of eight because it is the
//! count `benches/pipeline.rs` and `benches/data_pipeline.rs` already measure,
//! so the three pipeline tables in `BENCHMARKS.md` are read against the same
//! chain length.
//!
//! **Every pipe is [`PipeType::Serial`], which is upstream's shape and not an
//! omission.** `taskflow.cpp` builds all sixteen of its variants out of
//! `tf::PipeType::SERIAL` pipes alone, and `tbb.cpp` matches it with
//! `tbb::filter::serial_in_order` throughout, so there is no parallel middle
//! pipe anywhere in this benchmark upstream. The concurrency a serial chain
//! still has is across *stages*: `src/pipeline/scheduler.rs`'s `run_line`
//! hands the next line's token to the pipe it has just left, so one line's
//! token may be in `FilterFinal` while the next line's is still in `Input`.
//! That is also what makes the shape correct with a serial `FilterFinal`:
//! tokens reach it in token order, the walk is level-major, and so every node
//! of level `l - 1` has its final value before any node of level `l` asks for
//! it.
//!
//! Two departures from upstream, both of which `BENCHMARKS.md`'s own section
//! repeats next to the figures:
//!
//! - **The per-node work is the shared [`body::dummy`], not upstream's
//!   `work(int)`.** Upstream's is a 16x16 integer matrix multiply that
//!   allocates three `std::vector<std::vector<int>>` per call, so most of what
//!   it measures is an allocator; the rest of this suite measures a pipe body
//!   with the 32-iteration floating-point loop `common/body.rs` holds, and
//!   this file reads it from there too. A node's value is therefore an `f64`
//!   where upstream's is an `int`, keyed -- as upstream keys its own -- on the
//!   node's `uid`.
//! - **The graph generator is this file's own, not `benches/graph_traversal.rs`'s.**
//!   Upstream ships `levelgraph.hpp` twice, once per benchmark directory, and
//!   the two copies differ: this one adds `uid()`, `get_value`/`set_value` and
//!   the `_length` those need, which is exactly what the pipeline reads off a
//!   node and the traversal does not. The node generation is identical in both
//!   copies, and this file's [`LevelGraph`] keeps `benches/graph_traversal.rs`'s
//!   two documented departures from it -- a seeded [`Xorshift64`] in place of
//!   `std::mt19937` plus `std::rand()`, and no `_chosen`/`BFS`, which
//!   `threshold`'s default of `0.0f` makes dead code upstream -- while
//!   carrying the in-edges per node that `FilterFinal` needs and the
//!   traversal's copy drops. A shared generator in `benches/common` would
//!   have to be the union of the two upstream headers, which is a third shape
//!   neither benchmark has.
//!
//! # What the mpsc baseline does and does not share with the pipeline
//!
//! There is no rayon baseline, for the reason `benches/pipeline.rs` gives at
//! length: rayon has no pipeline primitive. The baseline is [`NUM_PIPES`]
//! `std::thread`s chained by [`std::sync::mpsc`] channels, one thread per pipe
//! role, in the role `tbb::parallel_pipeline` plays upstream's own `tbb.cpp`.
//! Both sides walk the same graph in the same order, call [`body::dummy`] once
//! per node per stage, and write the same values into the same per-node cells,
//! so the two differ in their scheduling and in nothing else. What they do not
//! share is a `lines`/`workers` axis: an mpsc stage is one thread whatever
//! `PipeType` the pipe it stands in for would have been, so `mpsc` is measured
//! once per graph size and read against every row of that size.
//!
//! Both sides' answer is checked against upstream's own sequential model --
//! `gold.cpp`'s `measure_time_gold`, [`reference`] here -- in the *untimed*
//! part of this benchmark, before either id is registered, so a schedule that
//! sums the wrong predecessors fails the benchmark instead of winning it.

use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow};
use rustflow::pipeline::scheduler::Pipeline;
use rustflow::prelude::*;
use rustflow::{Executor, Xorshift64};

#[path = "common/body.rs"]
mod body;
mod common;

/// Graph dimensions, as upstream's `LevelGraph(i, i)`: `i` levels of `i`
/// nodes, and so `i * i` tokens through the pipeline.
///
/// Upstream's `main.cpp` sweeps `i` over `1, 16, 31, ..., 451`. These are two
/// points on that sweep: one where building the [`Pipeline`] and composing it
/// still shows against 256 tokens, one where 14641 tokens' per-node cost
/// dominates it.
const DIMENSIONS: [usize; 2] = [16, 121];

/// Pipes in the chain: upstream's `graph_pipeline_taskflow_4_pipes`, so
/// `Input`, two `Filter`s and `FilterFinal`.
const NUM_PIPES: usize = 4;

/// Lines the [`Pipeline`] runs over: upstream's `-l` default.
const NUM_LINES: usize = 8;

/// Upstream's `_edge_max`: a node gets `1 + rand() % 4` out-edges.
const EDGE_MAX: u64 = 4;

/// The generator seed. Upstream's is 0 for both of its two generators; this
/// one exists so the graph is the same graph on every run of this benchmark.
const SEED: u64 = 1;

/// Samples per measurement, and the time criterion may spend reaching them.
/// Below criterion's defaults for the reason `linear_chain.rs` gives.
const SAMPLES: usize = 20;
/// Warm-up time per measurement. See [`SAMPLES`].
const WARM_UP: Duration = Duration::from_secs(1);
/// Measurement time per measurement. See [`SAMPLES`].
const MEASUREMENT: Duration = Duration::from_secs(3);

/// Upstream's `LevelGraph`, as `benchmarks/graph_pipeline/levelgraph.hpp`
/// spells it: `levels` rows of `length` nodes, every edge running from a node
/// in one row to a node in the next, one to [`EDGE_MAX`] of them per node.
///
/// Both edge directions are kept, unlike `benches/graph_traversal.rs`'s copy:
/// the out-edges because they are what the generator produces, the in-edges
/// because `FilterFinal` reads them.
struct LevelGraph {
    /// `out_edges[level][node]`: the indices in row `level + 1` this node
    /// precedes. The last row's entry is generated, as upstream generates it,
    /// and linked to nothing.
    out_edges: Vec<Vec<Vec<usize>>>,
    /// `in_edges[level][node]`: the indices in row `level - 1` that precede
    /// this node -- upstream's `_in_edges`, whose `first` is all either
    /// `FilterFinal` or `gold.cpp` reads of a pair.
    in_edges: Vec<Vec<Vec<usize>>>,
    /// Nodes per row: upstream's `_length_num`.
    length: usize,
}

impl LevelGraph {
    /// Generates a graph of `levels` rows of `length` nodes, following
    /// upstream's constructor and then its in-edge pass.
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

        // Upstream's second constructor loop: every out-edge of a node in row
        // `l` becomes an in-edge of its destination in row `l + 1`.
        let mut in_edges = vec![vec![Vec::new(); length]; levels];
        for level in 0..levels.saturating_sub(1) {
            for (node, edges) in out_edges[level].iter().enumerate() {
                for &destination in edges {
                    in_edges[level + 1][destination].push(node);
                }
            }
        }

        Self {
            out_edges,
            in_edges,
            length,
        }
    }

    /// Rows in the graph: upstream's `level()`.
    fn levels(&self) -> usize {
        self.out_edges.len()
    }

    /// Nodes per row: upstream's `length()`.
    fn length(&self) -> usize {
        self.length
    }

    /// Nodes in the graph, which is also the number of tokens one run pushes
    /// through the pipeline: upstream's `get_node_count()`.
    fn num_nodes(&self) -> usize {
        self.levels() * self.length
    }

    /// Upstream's `uid()`, `_idx + _level * _length`: the node's index into
    /// [`Values`], and the one thing a pipe passes down the chain.
    fn uid(&self, level: usize, index: usize) -> usize {
        index + level * self.length
    }

    /// Upstream's `graph_size()`, the `|V| + |E|` its table reports: one per
    /// node plus one per out-edge, the last row's included even though nothing
    /// links them.
    fn graph_size(&self) -> usize {
        self.num_nodes() + self.out_edges.iter().flatten().map(Vec::len).sum::<usize>()
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

/// Upstream's per-node `_value`, one cell per node: `set_value` and
/// `get_value`, shared between the pipes that write one node's value and the
/// pipes that read its successors'.
struct Values {
    /// One `f64`, as its bits, per node -- upstream reaches a node's `int
    /// _value` through a `LevelGraph&` several pipes hold at once, which in
    /// Rust is a shared cell per node rather than a shared `&mut` graph.
    cells: Vec<AtomicU64>,
}

impl Values {
    /// Zeroed cells for a graph of `num_nodes` nodes, as upstream's
    /// `_value {0}`.
    fn new(num_nodes: usize) -> Self {
        Self {
            cells: (0..num_nodes).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// Upstream's `set_value`.
    fn set(&self, uid: usize, value: f64) {
        // Release/Acquire rather than Relaxed: the scheduler's own join
        // counters already order one pipe's write before the next pipe's read
        // of the same line, and these pair state that rather than lean on it.
        self.cells[uid].store(value.to_bits(), Ordering::Release);
    }

    /// Upstream's `get_value`.
    fn get(&self, uid: usize) -> f64 {
        f64::from_bits(self.cells[uid].load(Ordering::Acquire))
    }

    /// Every node's value in `uid` order, which is what a run's answer is
    /// compared against [`reference`]'s.
    fn snapshot(&self) -> Vec<f64> {
        (0..self.cells.len()).map(|uid| self.get(uid)).collect()
    }
}

/// Upstream's `work(uid)`, keyed on the node the way upstream keys its own,
/// but the suite's shared [`body::dummy`] rather than its matrix multiply --
/// see this file's module docs.
fn work(uid: usize) -> f64 {
    let key = u64::try_from(uid).expect("a node uid fits a u64");
    body::dummy(key)
}

/// Upstream's `FilterFinal` body, with the walk from a `uid` back to its
/// `(level, index)` that upstream writes twice over: a node's final value is
/// its own work plus every predecessor's value from the level above, or just
/// its own work in level 0.
///
/// Upstream spells the level `uid / graph.level()` and the index
/// `uid % graph.length()`, which agree with `uid / length` and `uid % length`
/// only because `main.cpp` builds nothing but square graphs; this uses the
/// length for both, which is what `uid()` inverts.
fn finalize(graph: &LevelGraph, values: &Values, uid: usize) {
    let level = uid / graph.length();
    let index = uid % graph.length();
    let own = work(uid);

    if level == 0 {
        values.set(uid, own);
        return;
    }

    let from_previous_level: f64 = graph.in_edges[level][index]
        .iter()
        .map(|&source| values.get(graph.uid(level - 1, source)))
        .sum();

    values.set(uid, own + from_previous_level);
}

/// Upstream's `measure_time_gold` (`gold.cpp`): the same per-node arithmetic
/// in one level-major loop, with no pipeline at all. Both measured variants'
/// answers are checked against this one.
fn reference(graph: &LevelGraph) -> Vec<f64> {
    let values = Values::new(graph.num_nodes());

    for level in 0..graph.levels() {
        for index in 0..graph.length() {
            let uid = graph.uid(level, index);
            // The `Input` pipe's write and each `Filter`'s, which
            // `FilterFinal` then overwrites -- upstream's `pipe_helper`.
            values.set(uid, work(uid));
            for _ in 1..NUM_PIPES - 1 {
                values.set(uid, work(uid));
            }
            finalize(graph, &values, uid);
        }
    }

    values.snapshot()
}

/// Builds the [`NUM_PIPES`]-pipe chain upstream's
/// `graph_pipeline_taskflow_4_pipes` builds, over `num_lines` lines: a serial
/// `Input` walking the graph into the running line's buffer slot, `Filter`s
/// re-doing that node's work, and a `FilterFinal` summing the level above.
fn build_pipeline(graph: &Arc<LevelGraph>, values: &Arc<Values>, num_lines: usize) -> Pipeline {
    // Upstream's `std::vector<int> mybuffer(num_lines)`: the one node a line
    // is carrying, written by `Input` and read by every pipe after it.
    let buffer: Arc<Vec<AtomicUsize>> =
        Arc::new((0..num_lines).map(|_| AtomicUsize::new(0)).collect());
    // Upstream's `Input{0, 0, ...}`, its `lev` and `len` members: the walk's
    // own cursor, which only the serial first pipe ever touches.
    let cursor = Arc::new(Mutex::new((0_usize, 0_usize)));

    let mut pipes = Vec::with_capacity(NUM_PIPES);

    pipes.push(Pipe::new(PipeType::Serial, {
        let graph = Arc::clone(graph);
        let values = Arc::clone(values);
        let buffer = Arc::clone(&buffer);
        move |pf: &mut Pipeflow| {
            let mut cursor = cursor
                .lock()
                .expect("the serial first pipe owns the cursor");
            let (level, index) = *cursor;

            if index == 0 && level == graph.levels() {
                pf.stop();
                return;
            }

            let uid = graph.uid(level, index);
            buffer[pf.line()].store(uid, Ordering::Release);
            values.set(uid, work(uid));

            if index == graph.length() - 1 {
                *cursor = (level + 1, 0);
            } else {
                *cursor = (level, index + 1);
            }
        }
    }));

    for stage in 1..NUM_PIPES {
        let is_last = stage == NUM_PIPES - 1;
        let graph = Arc::clone(graph);
        let values = Arc::clone(values);
        let buffer = Arc::clone(&buffer);
        pipes.push(Pipe::new(PipeType::Serial, move |pf: &mut Pipeflow| {
            let uid = buffer[pf.line()].load(Ordering::Acquire);
            if is_last {
                finalize(&graph, &values, uid);
            } else {
                values.set(uid, work(uid));
            }
        }));
    }

    Pipeline::new(num_lines, pipes)
}

/// Runs the whole graph through a freshly built [`Pipeline`] composed into a
/// freshly built [`Taskflow`], and hands back every node's final value.
///
/// Both constructions are inside the measured region, as upstream's
/// `graph_pipeline_taskflow_4_pipes` has them inside its own timer.
fn run_pipeline(graph: &Arc<LevelGraph>, num_lines: usize, executor: &Executor) -> Vec<f64> {
    let values = Arc::new(Values::new(graph.num_nodes()));
    let pipeline = build_pipeline(graph, &values, num_lines);

    let mut taskflow = Taskflow::new();
    let module = taskflow.composed_of(&pipeline);
    taskflow.task(module).name("module_of_graph_pipeline");

    executor.run(&taskflow).wait().expect("no task panicked");

    // The only thing standing between this benchmark and timing a pipeline
    // that stopped before its first pipe ever ran.
    assert_eq!(
        pipeline.num_tokens(),
        graph.num_nodes(),
        "every node in the graph passes through the pipeline exactly once"
    );

    values.snapshot()
}

/// The same walk as [`NUM_PIPES`] freshly spawned threads chained by
/// [`std::sync::mpsc`] channels, one thread per pipe role -- the baseline's
/// whole shape, in the role `tbb::parallel_pipeline` plays upstream's own
/// `tbb.cpp`. Hands back every node's final value, as [`run_pipeline`] does.
fn run_mpsc(graph: &Arc<LevelGraph>) -> Vec<f64> {
    let values = Arc::new(Values::new(graph.num_nodes()));
    let channels: Vec<(mpsc::Sender<usize>, mpsc::Receiver<usize>)> =
        (0..NUM_PIPES - 1).map(|_| mpsc::channel()).collect();
    let (txs, rxs): (Vec<_>, Vec<_>) = channels.into_iter().unzip();
    let mut txs: VecDeque<_> = txs.into();
    let mut rxs: VecDeque<_> = rxs.into();

    let handles: Vec<thread::JoinHandle<usize>> = (0..NUM_PIPES)
        .map(|stage| {
            // Only `Input` has no upstream channel and only `FilterFinal` has
            // no downstream one; the `Filter`s in between both receive and
            // send, which is the chain's one hop per pipe.
            let input = if stage == 0 { None } else { rxs.pop_front() };
            let output = if stage == NUM_PIPES - 1 {
                None
            } else {
                txs.pop_front()
            };
            let is_last = stage == NUM_PIPES - 1;
            let graph = Arc::clone(graph);
            let values = Arc::clone(&values);

            thread::spawn(move || {
                let mut count = 0_usize;

                let mut handle = |uid: usize| {
                    if is_last {
                        finalize(&graph, &values, uid);
                    } else {
                        values.set(uid, work(uid));
                    }
                    count += 1;
                    if let Some(tx) = &output {
                        tx.send(uid).expect("the next stage is still receiving");
                    }
                };

                if let Some(rx) = input {
                    while let Ok(uid) = rx.recv() {
                        handle(uid);
                    }
                } else {
                    // Upstream's `Input`: the level-major walk itself.
                    for level in 0..graph.levels() {
                        for index in 0..graph.length() {
                            handle(graph.uid(level, index));
                        }
                    }
                }

                count
            })
        })
        .collect();

    let counts: Vec<usize> = handles
        .into_iter()
        .map(|handle| handle.join().expect("a stage thread does not panic"))
        .collect();

    assert_eq!(
        counts[NUM_PIPES - 1],
        graph.num_nodes(),
        "every node in the graph reaches the last stage exactly once"
    );

    values.snapshot()
}

/// Checks a variant's answer against upstream's sequential `gold` model.
///
/// Called once per id *before* that id is registered, so the comparison is
/// outside every measured region: a schedule that sums the wrong level fails
/// the benchmark rather than running faster than one that does not.
fn assert_matches_reference(answer: &[f64], expected: &[f64], variant: &str) {
    assert_eq!(
        answer, expected,
        "{variant} computes upstream's own `gold` values for every node"
    );
}

/// Registers the `mpsc` baseline once per graph size and the `rustflow`
/// variant at every size and worker count.
fn bench_graph_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_pipeline");
    group
        .sample_size(SAMPLES)
        .warm_up_time(WARM_UP)
        .measurement_time(MEASUREMENT);

    let graphs: Vec<(Arc<LevelGraph>, Vec<f64>)> = DIMENSIONS
        .iter()
        .map(|&dimension| {
            // Generated outside every measured region, as upstream generates
            // it in `main` rather than in `measure_time_taskflow`.
            let graph = Arc::new(LevelGraph::new(dimension, dimension));
            let expected = reference(&graph);
            (graph, expected)
        })
        .collect();

    // The mpsc baseline has no `lines` or `workers` axis -- see the module
    // docs -- so it is registered once per size rather than once per row of
    // the table `BENCHMARKS.md` reads this against.
    for (graph, expected) in &graphs {
        assert_matches_reference(&run_mpsc(graph), expected, "the mpsc chain");
        group.bench_function(BenchmarkId::new("mpsc", graph.graph_size()), |b| {
            b.iter(|| run_mpsc(black_box(graph)));
        });
    }

    for workers in common::worker_counts() {
        let executor = common::executor(workers);

        for (graph, expected) in &graphs {
            assert_matches_reference(
                &run_pipeline(graph, NUM_LINES, &executor),
                expected,
                "the pipeline",
            );
            group.bench_function(
                BenchmarkId::new(
                    format!("rustflow/{NUM_LINES}l_{workers}w"),
                    graph.graph_size(),
                ),
                |b| {
                    b.iter(|| run_pipeline(black_box(graph), NUM_LINES, &executor));
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_graph_pipeline);
criterion_main!(benches);

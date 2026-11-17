# Benchmarks

Four of upstream's benchmarks, ported to criterion with a rayon baseline in the
role TBB's flow graph plays upstream: `linear_chain`, `binary_tree`,
`graph_traversal` and `embarrassing_parallelism`, each translated from the
matching directory under Taskflow v4.1.0's `benchmarks/`. The shapes and the
measured regions are upstream's; what the sizes, the baseline and the harness
are is set out below, because a number whose provenance is unstated is not a
result.

```sh
devtools/devcontainer.sh bench                        # the whole suite
devtools/devcontainer.sh bench --bench linear_chain   # one shape
devtools/devcontainer.sh bench -- --save-baseline x   # keep it to compare against
```

Benchmarks are deliberately **not** part of the gate (`fmt`, `clippy`, `test`,
`deny`): a benchmark answers "how fast", which is not a pass or a fail. What CI
holds the suite to is that it still compiles, which the gate's
`cargo clippy --all-targets` already does.

## The machine, and what it was doing

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id |
| Date | 2026-11-17 |
| **Load average** | **7.9 rising to 10.6 over the run** |

That last row is the one that matters. This box was running three other agents'
`cargo` builds throughout, and the numbers below are not what a quiet machine
would say: the same `linear_chain/rustflow/1w` ids measured earlier the same
hour at a load average of 1.4 came out at 114 µs and 1.65 ms, against the 136 µs
and 2.05 ms in the table. So treat the **absolute** figures as good to about
±25%, and the **within-row** comparisons as much better than that -- the
`rustflow` and `rayon` ids of one row are measured seconds apart in the same
process under the same load, which is the one thing this environment does
protect. Each cell carries criterion's own 95% confidence interval for the mean
as a ± percentage; where that interval is tight and the row still looks odd, the
noise is between runs rather than within one.

## What is measured

- **Ours includes building the graph.** Upstream's `measure_time_taskflow`
  wraps constructing the `tf::Taskflow` as well as running it, so the ported
  benchmarks do too. rayon has no graph to construct, which is the asymmetry to
  keep in mind in every table below -- upstream compares against a TBB flow
  graph, which pays a construction cost of its own, and we have no such
  baseline available. The `embarrassing_parallelism` table quantifies the
  difference with a declaration-only id.
- **Sizes are points on upstream's sweeps, not sizes of our own.** Upstream's
  `main.cpp` walks a geometric sweep per shape (`L = 2^1..2^25` for the chain,
  `1..2^20` tasks for embarrassing parallelism, `LevelGraph(i, i)` for `i` up to
  2048). Criterion samples one size a hundredfold rather than sweeping, so each
  table takes two points off upstream's sweep: one where the fixed cost of a run
  still shows, one where the per-task cost dominates it.
- **Workers are 1 and `available_parallelism`**, the two ends of upstream's
  `-t` option, which defaults to 1. rayon is held to the same count with a
  `ThreadPoolBuilder::num_threads` pool, which is what upstream's
  `global_control(max_allowed_parallelism, n)` does for TBB.

## The upstream C++ column is empty, and that is the finding

Upstream's `benchmarks/benchmarks.md` publishes **no figures**. It documents how
to build the suite and how to drive it with `benchmarks.py`, and then shows one
plot (`image/benchmarks.svg`) with no tabulated numbers behind it. There is
therefore nothing to quote for any of these four shapes, and the column below
says so rather than carrying a number from somewhere else. Had there been one,
it would still not have been comparable: it would be a different CPU, a
different compiler and a TBB baseline rather than a rayon one.

## linear_chain

`L` tasks, each a dependency of the next: no parallelism at all, so this
measures the scheduler's per-task cost on the critical path.

**No rayon baseline.** rayon's shapes -- `par_iter`, `join`, `scope` -- all
express "these may run at the same time", and a chain says the opposite about
every pair of tasks. The only rayon program with this shape is a sequential
loop, which would measure the loop and not a scheduler, so this shape is
reported on its own rather than against a baseline dressed up as one.

| size | workers | rustflow | rayon | upstream C++ |
|---|---|---|---|---|
| 1024 | 1 | 136.1 µs ±6.9% | n/a | not published |
| 1024 | 24 | 217.9 µs ±1.9% | n/a | not published |
| 16384 | 1 | 2.046 ms ±4.6% | n/a | not published |
| 16384 | 24 | 2.872 ms ±3.9% | n/a | not published |

Per task at the larger size that is 125 ns at one worker against 175 ns at 24,
which is the shape's whole point: 23 workers with nothing to do cost the one
worker that has something to do 40% more time per task, because every completed
task is handed to whichever worker wakes up first rather than staying on the
queue it was pushed to.

## binary_tree

A complete binary tree of `N` slots, node `i` preceding `2i` and `2i + 1`: one
source, then parallelism doubling with every level. The rayon baseline is the
recursive `rayon::join` that shape asks for.

| size | workers | rustflow | rayon | upstream C++ |
|---|---|---|---|---|
| 4096 | 1 | 505.1 µs ±0.4% | 35.6 µs ±1.4% | not published |
| 4096 | 24 | 739.8 µs ±1.1% | 91.6 µs ±1.0% | not published |
| 65536 | 1 | 8.075 ms ±9.4% | 500.8 µs ±0.2% | not published |
| 65536 | 24 | 10.33 ms ±0.8% | 995.9 µs ±0.5% | not published |

A 14x gap at 4096 nodes and 16x at 65536, and it is the honest one to expect:
`rayon::join` on a single thread degenerates into direct recursion -- two
closure calls, no atomics, no allocation -- while our run allocates 65535 boxed
closures and nodes, then drives 65535 queue pushes and join-counter decrements
through them.
The task body here is a single relaxed increment, so essentially all of what is
measured is that machinery. This is the shape where a task graph buys nothing
and pays for everything: the graph is built, run once and dropped, and nothing
reuses the structure that cost the construction.

## graph_traversal

Upstream's `LevelGraph`: `levels` rows of `length` nodes, one to four edges from
each node into the next row, and a fixed 32-iteration floating-point body per
node. Sizes are upstream's `|V| + |E|`, from `LevelGraph(32, 32)` and
`LevelGraph(128, 128)`.

The rayon baseline is level-synchronous -- `par_iter` over a row, then the next
row. It respects every dependency, since edges only ever go one row forward,
but it synchronises more than the graph demands. That is the comparison on
offer: a task graph against the barrier you would write without one.

| size | workers | rustflow | rayon | upstream C++ |
|---|---|---|---|---|
| 3476 (32x32) | 1 | 748.4 µs ±0.2% | 555.4 µs ±0.7% | not published |
| 3476 (32x32) | 24 | 333.2 µs ±0.4% | 334.4 µs ±1.5% | not published |
| 56759 (128x128) | 1 | 12.35 ms ±0.3% | 8.764 ms ±0.1% | not published |
| 56759 (128x128) | 24 | 4.643 ms ±0.8% | 2.271 ms ±0.7% | not published |

The first shape where the two draw level: at 1024 nodes and 24 workers the
barrier costs rayon what the graph costs us. At 16384 nodes rayon pulls ahead
again -- a row is then 128 nodes of 0.5 µs each, enough work per barrier to
amortise it, while our per-node overhead does not amortise against anything.

## embarrassing_parallelism

`N` independent tasks, no edges, each doing the same 32-iteration floating-point
body. The shape a scheduler should be best at, and the fairest comparison here:
`(0..n).into_par_iter().for_each(..)` is exactly this program and is what
anyone would write.

| size | workers | rustflow | rayon | upstream C++ |
|---|---|---|---|---|
| 4096 | 1 | 2.637 ms ±0.4% | 2.262 ms ±0.2% | not published |
| 4096 | 24 | 816.0 µs ±0.9% | 153.1 µs ±0.5% | not published |
| 65536 | 1 | 42.99 ms ±0.1% | 35.57 ms ±0.6% | not published |
| 65536 | 24 | 11.54 ms ±1.7% | 1.900 ms ±0.7% | not published |

Declaring the taskflow, with nothing run, is the `rustflow-build` id: 120.5 µs
±0.7% for 4096 nodes and 1.999 ms ±0.3% for 65536, or about 31 ns a node. So of
the 11.54 ms at 24 workers, 2.0 ms is construction that rayon never pays and
9.5 ms is the run -- still 5x rayon's 1.9 ms, and the real finding of this
table. The body is ~543 ns (rayon's 35.57 ms over 65536 at one worker), so the
total work is 36 ms of CPU; rayon turns that into 1.9 ms, a 19x speed-up on 24
threads, and we turn it into 9.5 ms, a 3.8x one. Every task here is pushed by
the calling thread into the shared overflow buffers rather than onto a worker's
own queue, so all 65536 of them are distributed by stealing; that is the first
thing to look at, and it is a scheduling question rather than a `Work` one.

## The `Work` decision: `Mutex::try_lock` stays

`Work` holds its callable in a `Mutex` and `invoke` takes it with `try_lock`,
panicking rather than blocking if the lock is held, since the scheduler promises
one worker per node at a time. Issue #5 deferred the question of trading that
for an `UnsafeCell`, and #18 is where it was to be measured: `linear_chain` and
`embarrassing_parallelism`, at 1 and 24 workers.

**Measured, and the answer is to keep the `Mutex`.**

The first attempt was the obvious one -- `--save-baseline` with the `Mutex`,
patch, `--baseline` with the `UnsafeCell` -- and it is worth recording why it
was thrown away: the `rayon` ids, which do not touch `Work` at all, reported
"performance has regressed" by 6.7% and 9.3% with `p = 0.00`. On this box, a
comparison between two criterion runs minutes apart measures the other agents'
builds. So the measurement was redone as three interleaved rounds, rebuilding
the library between each half, with both variants of each round measured within
a minute of each other. Point estimates of the mean, in the order the rounds ran:

| id | `Mutex` | `UnsafeCell` | per task |
|---|---|---|---|
| `linear_chain/1w/1024` | 117.9, 110.4, 114.7 µs | 114.3, 107.1, 113.4 µs | −2.7 ns |
| `linear_chain/1w/16384` | 1.697, 1.608, 1.645 ms | 1.645, 1.600, 1.570 ms | −2.7 ns |
| `linear_chain/24w/1024` | 220.2, 219.7, 223.2 µs | 221.9, 228.2, 227.8 µs | **+4.8 ns** |
| `linear_chain/24w/16384` | 2.862, 3.031, 2.785 ms | 2.768, 2.890, 2.806 ms | −4.4 ns |
| `embarrassing/1w/65536` | 53.94, 44.46, 52.56 ms | 53.87, 50.83, 52.58 ms | **+32 ns** |

What that says:

- Where the effect is consistent, it is **−2.7 ns per invocation**, and the
  two chain sizes at one worker agree on that figure to within 0.1 ns from
  two independent measurements -- which is exactly the uncontended
  lock-plus-unlock pair the swap removes. It is real, and it is 2.4% of the
  ~110 ns per task that shape cost during these rounds.
- Two rows come out with the **wrong sign**. Removing an uncontended atomic
  cannot make a program slower, so `+4.8 ns` and `+32 ns` are the noise floor
  announcing itself: the same binary varies by 5% run to run on the chain and by
  21% on `embarrassing_parallelism` (44.5 ms against 53.9 ms for the `Mutex`,
  twice, unchanged).
- So the swap is worth about 2.5% on the cheapest-bodied shape we measure,
  nothing measurable on a shape with a real task body, and the box cannot
  resolve either claim to better than the effect itself.

Against that, the cost is concrete rather than stylistic. The crate has exactly
one sanctioned `unsafe` boundary today (`Job`, in `src/executor/topology.rs`);
this would be the second, and it needs two -- the `unsafe` deref in `invoke` and
an `unsafe impl Sync for Work<R>`, since `UnsafeCell` is not `Sync` on its own.
It would also delete the assertion that pays for the `Mutex`: `try_lock` turns a
broken one-worker-per-node invariant into an immediate panic naming it, and the
`UnsafeCell` turns the same bug into a data race on a `dyn FnMut`, which is
undefined behaviour that may not surface for a milestone. `graph.rs`'s
`invoking_work_twice_at_once_panics` test exists for that and would have to go.
Trading a live check on the scheduler's central invariant for 2.5% of one
pathological shape is the wrong trade at this stage of the port -- and the
shapes above say the scheduler's problem is 5x on `embarrassing_parallelism`,
not 2.5% on a chain.

The decision is reversible and the measurement is cheap to redo: if a later
milestone brings the per-task cost down far enough that 2.7 ns is a visible
fraction of it, or an `UnsafeCell` with a `debug_assert`-grade check gets the
assertion back, the question is open again. The patch used here is 20 lines
against `Work` alone; nothing in `src/executor/` had to change for it, which is
the one genuinely reassuring thing the experiment established.

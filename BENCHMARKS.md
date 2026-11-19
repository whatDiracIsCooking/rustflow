# Benchmarks

**Twenty-two of upstream's twenty-five benchmarks**, ported to criterion with
a rayon baseline in the role TBB's flow graph plays upstream, each translated
from the matching directory under Taskflow v4.1.0's `benchmarks/`. The shapes
and the measured regions are upstream's; what the sizes, the baseline and the
harness are is set out below, because a number whose provenance is unstated is
not a result.

`benchmarks/CMakeLists.txt` at the pin declares twenty-five `bench_*` targets
-- twenty-four host ones, numbered 1 to 24 in its own comments, and
`bench_hetero_traversal` behind `TF_BUILD_CUDA` -- and the twenty-two ported
are these, by the issue that ported each: `linear_chain`, `binary_tree`,
`graph_traversal` and `embarrassing_parallelism` (#18); `async_task` and
`fibonacci` (#96); `sort` (#174); `pipeline`, which is upstream's
`linear_pipeline` (#187); `for_each`, `reduce_sum` and `scan` (#335);
`primes`, `mandelbrot` and `matrix_multiplication` (#336); `black_scholes`
(#337); `wavefront` (#338); `graph_pipeline` and `data_pipeline` (#339, the
second landing as a second criterion group in the file #188 had opened for the
per-line slot measurement); `skynet` and `nqueens` (#340); and `merge_sort`
and `integrate` (#341). That is 22 `[[bench]]` targets in `Cargo.toml` and 232
criterion ids, which `cargo bench --all-features -- --list` counts.

**Three are declined, and this is the whole of that list:**

- **`hetero_traversal`** walks a graph whose tasks are a mix of CPU and GPU
  work, and is upstream's one CUDA benchmark: `.cu` sources, built only under
  `TF_BUILD_CUDA`. `cuda/` has been out of scope from the start -- the
  README's "Upstream pin" says so -- so there is no port of the thing this
  shape measures to measure.
- **`mnist`** trains a DNN over the MNIST dataset. Its own `readme.md` asks
  the caller to download four files from Kaggle, and its `dnn.hpp` includes
  `Eigen/Dense`, which `benchmarks/CMakeLists.txt` resolves against
  `3rd-party/eigen-3.3.7`. A downloaded dataset and a linear-algebra library
  are both outside the pinned tree's own code, and `3rd-party/` carries
  separate terms besides.
- **`thread_pool`** compares `tf::Executor` against the hand-rolled
  `ThreadPool.hpp` singleton sitting beside it in the same directory: 1000
  independent tasks each way, through `pool->enqueue` on one side and a
  1000-task `tf::Taskflow` on the other. That baseline has no counterpart
  here -- the rayon pool stands in for TBB, not for a C++ singleton -- and the
  shape itself is `embarrassing_parallelism`, which is ported. What the target
  measures is a comparison between two C++ libraries rather than a shape of
  the executor.

**How this file is laid out.** `grep -c "^## " BENCHMARKS.md` counts 48
headings, and they are three kinds: the three method sections that follow this
preamble, "The machine" through "The upstream C++ column is empty"; 21 shape
sections carrying the 22 shapes, since `async_task` and `fibonacci` share "The
two shapes #96 added", which measured the two together in a run of their own;
and 24 per-decision measurement sections, each named for the issue that took
it. A shape's section is its table, its sizes and its departures from
upstream. A measurement section is one question, with the machine, the load
and the method of its own run in it, and it ends either in a change that
shipped or in a change declined with what it would have cost.

```sh
devtools/devcontainer.sh bench                        # the whole suite
devtools/devcontainer.sh bench --bench linear_chain   # one shape
devtools/devcontainer.sh bench -- --save-baseline x   # keep it to compare against
```

Benchmarks are deliberately **not** part of the gate (`fmt`, `clippy`, `test`,
`deny`, `doc`): a benchmark answers "how fast", which is not a pass or a fail.
What CI holds the suite to is that it still compiles, which the gate's
`cargo clippy --all-targets` already does, and that it still *runs*: the
`examples` job of `.github/workflows/ci.yml` executes every one of the 232 ids
once in criterion's untimed `--test` mode, which is a smoke test and not a
measurement -- nothing is timed, saved or compared against a baseline.

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

**The fourteen sections this milestone added were measured in one pass on a
quiet box**, a condition only the #359 measurement below has had before: no
sibling agent, no other `cargo` anywhere, and `/proc/loadavg`'s runnable-task
field at 1 or 2 throughout, so the load each shape ran under is its own 24
threads and the decay of the shape before it rather than another workload.
Those sections carry a figures table and no machine table of their own,
because the machine is this one:

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id -- every one of the fourteen targets, unchanged |
| Date | 2026-11-19 |
| **Load average** | **each shape's own: 3.2 to 11.1 before a shape and 5.6 to 13.0 after it, with nothing else on the box** |

The shapes ran back to back, one `cargo bench --bench <name>` each, which is
why a shape's "before" reading is its predecessor's tail rather than zero; the
pass's own PR records the pair around each shape. No sample count was reduced:
the caveat for a shape too slow to measure in one pass did not apply to any of
the fourteen, the longest single id being `black_scholes/rustflow/1w/4000` at
350 ms a sample. Every figure is **one** run of one id -- criterion's twenty
samples are the statistic, so there is no best-of-N anywhere below -- quoted as
the point estimate of the mean with half the width of criterion's 95%
confidence interval as a plus-or-minus percentage, the convention every table
in this file uses. A wide interval or a high outlier count is named under the table
that has one rather than smoothed away. The one group this pass deliberately
did *not* re-measure is `data_pipeline_slot`: those figures belong to the
#186 decision below and re-taking them was not this pass's job, so
`benches/data_pipeline.rs`'s two groups carry figures from two different runs.

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
- **What every target shares is `benches/common/`.** That worker pair, the
  executor a row is measured on, the rayon pool holding the baseline to the
  same count and upstream's `dummy` body are each defined there once -- so a
  section below saying "the shared body" names one function -- while every
  target keeps its own sizes, its own criterion sample settings and its own
  one-off setup, which are what its table reports rather than what its
  neighbours share.
- **An A/B between two builds is measured in interleaved rounds**, by
  `devtools/bench-ab.sh <ref-A> <ref-B> --bench <name> [--rounds N]`, which is
  the method every A/B section below hand-rolled and the one they use from here
  on. It builds each side into its own worktree and its own
  `CARGO_TARGET_DIR`, then alternates rounds -- A, B, A, B, ... -- so that a
  load which drifts over the run lands on both sides instead of on whichever
  was measured second, the mistake the `Work` decision and #102 each record
  having made and undone. It prints each side's spread against its own rounds
  before it prints the A-to-B ratio of medians, because the rule those sections
  settled on is that a difference inside either side's own spread is not a
  result; and it records `/proc/loadavg` at the start, after every round and at
  the end, together with `nproc`, so that a table built from it can carry the
  load it ran under the way the ones below do.

## The upstream C++ column is empty, and that is the finding

Upstream's `benchmarks/benchmarks.md` publishes **no figures**. It documents how
to build the suite and how to drive it with `benchmarks.py`, and then shows one
plot (`image/benchmarks.svg`) with no tabulated numbers behind it. There is
therefore nothing to quote for any of the twenty-two shapes, and the
`upstream C++` column each of their twenty-two tables carries says so rather
than a number from somewhere else. Had there been one,
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

**These four rows predate #40**, which made a queued job own a share of its run
and of the taskflow's storage and cost this shape about 19% at 24 workers --
see "The #40 decision" below for the measurement. The rows are left as measured
rather than scaled. They also predate **#62**, which ported upstream's
continuation optimisation and took 40 to 50% off this shape at 24 workers --
far more than #40 and #74 together had added -- so the paragraph above about
23 idle workers costing the busy one 40% is the old scheduler's behaviour.
"The #62 measurement" below has the figures, and "The #373 measurement" at the
end of this file re-takes these four ids on a quiet box -- 131 ns a task at one
worker against 128 at 24 -- and counts what the 23 workers actually do.

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
**#60 took it apart** -- see "The #60 finding" below for where the 9.5 ms goes,
which of it came back, and why the rest is the architecture rather than a
tuning gap.

These rows predate #40 too, which cost this shape 2.4% at 65536 tasks and 24
workers and 7% at 4096 -- an order of magnitude less than the chain, because
here there is a task body for the scheduler's overhead to hide behind.

## The #40 decision: a job owns what it names, at 4 atomics a task

Issue #40 found a use-after-free reachable from safe code: `mem::forget` on a
`RunHandle` ends the borrow the scheduler's raw pointers leaned on, and the
taskflow could then be freed under the workers. The fix that was taken -- the
issue's option 1 -- gives every queued `Job` an `Arc<Topology>` and an
`Arc<Flow>` instead of two `NonNull`s, so a job's targets are alive *because
the job holds them*. That is four atomic read-modify-writes per task that the
pointers did not pay: two clones when the job is built, two drops when it is
spent. `Taskflow::graph_mut` also became an `Arc::get_mut`, which is one more
on the graph-building path.

Measured the same way the `Work` question above was, and for the same reason --
cross-run criterion comparison on this box measures the other agents' builds,
not the patch. Three interleaved rounds, both variants of each round within a
minute of each other, `-- rustflow` so the rayon ids are not even built; load
average 2.0 to 5.8 throughout. Point estimates of the mean, in the order the
rounds ran, with the per-task figure taken from the mean delta:

| id | before (`NonNull`) | after (`Arc`) | per task |
|---|---|---|---|
| `linear_chain/1w/16384` | 1.706, 1.611, *4.636* ms | 1.765, 1.626, 1.791 ms | +1 to +4 ns |
| `linear_chain/24w/1024` | 221.4, 227.8, 224.7 µs | 264.4, 268.7, 266.2 µs | **+41 ns** |
| `linear_chain/24w/16384` | 3.115, 3.159, 3.098 ms | 3.696, 3.684, 3.735 ms | **+35 ns** |
| `embarrassing/24w/4096` | 784.5, 777.4, 755.3 µs | 828.0, 835.0, 820.3 µs | +13 ns |
| `embarrassing/24w/65536` | 11.93, 11.84, 11.65 ms | 12.11, 12.06, 12.09 ms | +4 ns |
| `embarrassing/rustflow-build/65536` | 2.215, 2.067, 2.070 ms | 2.273, 2.215, 2.256 ms | +2 ns |

Two ids are left out of the table because this box cannot resolve them:
`linear_chain/1w/1024` reported a 95% interval wider than ±15% in four of its
six measurements, and `embarrassing/1w/65536` -- whose task body is ~543 ns --
swung between 43.2 and 48.0 ms *before* the change and 41.7 to 53.4 ms after
it, which is ±10% against an effect of under 1%. The italicised 4.636 ms is an
outlier of the same kind, 2.8x its own neighbours, and is excluded from the
`1w/16384` delta.

What the usable rows say:

- **At 24 workers the cost is 35 to 41 ns per task**, and the three rounds of
  each row agree on it to within 10% (+43.0/+40.9/+41.5 µs over 1024 tasks;
  +581/+524/+637 µs over 16384). That is about 9 ns per refcount operation,
  which is what a contended cache line costs: the whole pool shares two of
  them, one per allocation, and a chain hands every task to whichever worker
  wakes first, so both lines bounce from core to core for the length of the
  run.
- **At one worker it is 1 to 4 ns per task**, i.e. the four uncontended
  RMWs and nothing else. The lines stay in one core's cache, and this is the
  figure the design would cost on a pool that kept work where it was pushed.
- **It is a fraction of a shape with a real task body**: +4 ns against 543 ns
  of work is 2.4% at 65536 tasks, and +13 ns at 4096 is 7% -- that id is
  fixed-cost heavy, 190 ns a task against the same 543 ns of work.
- **Building a graph costs +2 ns per `emplace`**, which is the `Arc::get_mut`
  uniqueness check behind `graph_mut`. It buys the other half of the issue: an
  edit of a running taskflow panics instead of reallocating a vector the
  workers are walking.

So the worst case is +19% on the one shape built to measure nothing but
scheduler overhead, and ~2% on the shape with work in it. **That is accepted,
and it is the whole point of the issue**: the alternative is a safe API with a
reachable use-after-free, which is not a thing to trade nanoseconds against.

Two things would get most of it back, and neither is this PR. **The first of
them has since landed** -- #62 -- and got back rather more than #40 cost; "The
#62 measurement" below is that measurement, and the prediction it confirms is
the one in the first bullet:

- **Upstream's `_update_cache`/`TF_INVOKE_CONTINUATION`**, which `worker.rs`
  recorded as unported until #62: it keeps one ready successor out of the queue
  and jumps back to the top of `_invoke` with it. Under the old pointers that
  saved a queue push; under these shares it also saves a clone *and* a drop,
  which is exactly the 4 atomics -- on a chain, all of them. The shape it helps
  most is the shape that regressed most.
- **One allocation instead of two**, putting the `Topology` and the share of
  the storage in the same `Arc` so a job clones one refcount rather than two.
  It was not taken, because the release of that share and the end of the run
  have to happen in that order and in that order only -- otherwise a `wait`
  can return while a job still holds the taskflow's storage and the caller's
  next `taskflow.emplace()` panics. `tests/basics.rs`'s
  `a_taskflow_is_editable_the_instant_its_run_reports_done` is the case that
  pins it.

  **#109 changed which way round that ordering is enforced, and the
  conclusion survives it twice over.** The share is no longer required to be
  given up *before* the decrement that ends the run: #109 found that a job
  copied out of a preempted node cannot do that, and so made the run end
  *after* the share instead. `Drop for Storage` in `executor::topology` is the
  whole of it -- the last share of the run's hold drops the storage and
  *then* releases the waiter, in one destructor, whichever thread runs it. So
  the hold has to be a thing whose last share can go away while the run is
  still live. A hold merged into the `Topology` allocation could not be: every
  job holds an `Arc<Topology>` beside its share, and since #109 so does the
  thread being released, so the merged refcount could never reach the point
  that releases anything -- the waiter would be waiting on itself. #112 then
  made the same hold what a `corun` frame waits to become unique, which a
  pool-wide `Topology` could not answer either.

  The trade is therefore the same one and it is still wrong: half the atomics
  for a `wait` that cannot say when a run is over. The continuation
  optimisation was the one to do first, and the bullet above is the one that
  landed.

## The #74 decision: a job names which of four graphs it walks, at one word

Issue #74 widened the field #40 had just introduced. A queued `Job` held an
`Arc<Flow>` -- a share of the taskflow's storage -- which names two of the four
graphs the scheduler can run: the run's own graph, and the target of a module
task composed of an `Arc<Taskflow>`. The other two are owned *inside* the
handle of the node they hang off, an adopted module's `Graph` and a subflow's
`Mutex<Graph>`, and nothing names one but the owner of the outer graph plus
that node's index as a path. So the field became a two-variant enum -- a share
or a path -- and `size_of::<Job>()` went from **32 bytes to 40**: two pointer
variants need a tag, and there is no niche to put it in. The
`a_job_is_five_words` case in `executor::topology` pins both numbers.

One job is built per task on the hot path and travels through a work-stealing
deque by value, so the fifth word is a cost worth having a number for.
Measured the way #40's was, and for the same reason -- cross-run criterion
comparison on this box measures whatever else the box is doing: interleaved
rounds, both variants of a round within a minute of each other, `-- rustflow`
so the rayon ids are not built. Six rounds for `linear_chain` and four for the
two `embarrassing_parallelism` ids that resolve at all; load average 4.2 to 6.6
throughout, with no other agent on the box. Point estimates of the mean, in
the order the rounds ran:

| id | before (one `Arc<Flow>`) | after (tag + pointer) | delta | per task |
|---|---|---|---|---|
| `linear_chain/1w/1024` | 158.6, 161.7, 159.6, 160.0, 161.2, 147.0 µs | 169.6, 164.3, 147.8, 150.1, 166.4, 165.5 µs | +1.6% | +2.5 ns |
| `linear_chain/1w/16384` | 2.101, 2.250, 2.242, 2.260, 2.230, 2.249 ms | 2.326, 2.351, 2.348, 2.341, 2.018, 2.362 ms | +3.1% | +4.2 ns |
| `linear_chain/24w/1024` | 297.7, 297.6, 307.0, 299.6, 300.0, 310.2 µs | 315.4, 317.4, 315.0, 314.4, 302.6, 312.1 µs | +3.6% | +10.6 ns |
| `linear_chain/24w/16384` | 4.282, 4.225, 4.262, 4.279, 4.350, 4.176 ms | 4.335, 4.394, 4.410, 4.337, 4.356, 4.343 ms | +2.3% | +6.1 ns |
| `embarrassing/24w/4096` | 955.6, 974.9, 977.0, 973.1 µs | 976.7, 982.3, 972.0, 981.3 µs | +0.8% | +1.9 ns |
| `embarrassing/24w/65536` | 13.70, 13.35, 13.64, 13.72 ms | 13.71, 13.73, 13.48, 14.23 ms | +1.3% | +2.8 ns |

So **2 to 4% on the shapes built to measure nothing but the scheduler, and
about 1% on the shape with a task body in it** -- an order of magnitude less
than #40's own 19%, and in the same place: a chain at 24 workers, where every
job is handed to whichever worker wakes first and the deque slot it travels in
is cold when it arrives. A 32-byte job fits a cache line twice over and a
40-byte one does not, which is what the +10.6 ns on `24w/1024` looks like. Two
caveats on reading the table: the two single-worker `linear_chain` ids swing
by ±9% round to round, so their deltas are the mean of six rounds and not a
resolved effect; and the first two-way set measured `embarrassing/24w/4096` at
+9%, which four interleaved rounds did not reproduce -- the "before" column of
that earlier set was itself 8% faster than this one. Interleaving within a
round is the only thing this box protects, and these two ids are at the edge
of what it can resolve.

**An alternative that keeps a job at 32 bytes was measured and rejected.**
Boxing the owner -- `GraphOwner(Arc<Owned>)`, one pointer, with the enum
behind it -- costs one extra dependent load per resolution and one allocation
per graph scheduled, and buys the word back. Measured in the same rounds as
the flat enum, it did not win: `linear_chain/24w/1024` came out at +6.2%
against the flat enum's +2.1% over that round-set's own baseline, and
`embarrassing/24w/4096` at +2.1% against +0.8%. The owner allocation is shared
by every job of a graph and is therefore hot, but a job's owner is read once
per invocation and twice more on the queueing path, so the indirection costs
about what the word costs -- and the flat enum is the one with no allocation
and no second load in it. So the extra word is accepted, and what it buys is
not a nanosecond trade at all: before it, a job of an adopted module's graph
resolved its indices against the composing taskflow's arena, and the arm that
ran one refused the run rather than do that silently.

## The #62 measurement: the continuation takes half of a chain away

Issue #62 ported upstream's `_update_cache`/`TF_INVOKE_CONTINUATION`, which
#40's section above had named as the thing that would get its cost back: a
worker keeps one ready successor out of its queue and loops back to the top of
`invoke` with it, so a chain runs end to end on the worker that started it
instead of being pushed, notified and re-stolen per link. `linear_chain` is
the shape built to measure exactly that.

Measured the way #40's and #74's were, and for the same reason -- a
cross-run criterion comparison on this box measures the other agents' builds,
not the patch. Three interleaved rounds, both variants of each round within a
minute of each other, `src/executor/worker.rs` swapped and the library rebuilt
between halves. **The box was carrying three other agents' `cargo` builds and
test runs throughout**: load average 4.0 to 12.0, read at the start of each
half and recorded with it. That is not a quiet machine and these are not
quiet-machine absolutes; what interleaving protects is the comparison, and
every round agrees on it. Point estimates of the mean, in the order the rounds
ran:

| id | before (queue per link) | after (continuation) | delta | per task |
|---|---|---|---|---|
| `linear_chain/rustflow/1w/1024` | 165.3, 162.2, 160.5 µs | 156.2, 126.6, 137.6 µs | **−13.9%** | −22 ns |
| `linear_chain/rustflow/1w/16384` | 2.876, 2.232, 2.264 ms | 1.948, 1.951, 1.785 ms | **−22.9%** | −34 ns |
| `linear_chain/rustflow/24w/1024` | 274.0, 300.1, 309.8 µs | 156.6, 145.2, 188.4 µs | **−44.5%** | −128 ns |
| `linear_chain/rustflow/24w/16384` | 4.002, 4.309, 4.326 ms | 1.888, 1.661, 2.342 ms | **−53.4%** | −137 ns |

What that says:

- **At 24 workers a chain costs half what it did**, −39% to −62% in each of
  the three rounds of each id, and the per-task figure of 128 to 137 ns is
  three times what #40 (35 to 41 ns) and #74 (6 to 11 ns) had added between
  them. It is also the whole of the "40% more per task at 24 workers than at
  1" that the `linear_chain` table above records: after this change the two
  worker counts measure the same thing, because 23 idle workers no longer get
  a say in where the next link runs. What a hand-off cost was a contended
  queue slot, a `notify_one`, a thief's successful steal, and a node and a
  job arriving cold in another core's cache.
- **At one worker it is still 22 to 34 ns per task**, which is the part that
  has nothing to do with stealing: a push and a pop of the local deque, and
  the `notify_one` every push makes. A single worker's queue is LIFO, so it
  already handed back the same job the continuation keeps -- `update_cache`'s
  docs make that equivalence -- and this is purely the queueing round trip
  that is no longer made.
- **The two single-worker ids are the noisy ones**, as they were in #74's
  table: round 1's `1w/16384` "before" reported a 95% interval of 2.80 to
  2.98 ms and the id swings ±14% round to round, so read −22.9% as "a fifth
  or so" and not as a resolved figure. The 24-worker ids are where the effect
  is far larger than the noise: the smallest gap any round produced there is
  39%, against a round-to-round spread of about 12%.

**The optimisation is not only a nanosecond trade**, which is why it is in
`tests/work_stealing.rs` as well as here: `Continuation` and
`Oversubscription` are upstream acceptance cases that assert the locality it
produces, and both failed against this port at every worker count above one
before this change -- `Continuation` on the identity of the worker running
task 87, `Oversubscription` with 4 chains spread over all 8 workers.

**What is still not cached** is the other half of upstream's: a preempted
parent resumed by its last child goes back on a queue rather than being run on
the spot (`WorkerThread::finish_job`). A chain has no preempted parents, so
none of the rows above would move. The nested shapes were the candidates, and
the #342 section below has since counted them rather than guessed: a `corun`
leaves nothing preempted, so **none of the five nested shapes in this file
moves either**, and the whole suite re-queues a parent twelve times over one
run of every id -- all twelve in `pipeline`, which is the only shape here
whose parents are preempted at all.

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

Against that, the cost is concrete rather than stylistic. When this was
measured the crate had exactly one sanctioned `unsafe` boundary in the
scheduler (`Job`, in what is now `src/executor/topology/job.rs`) and this
would have been the second; #40 has since removed that one, so the only
`unsafe` left anywhere in
the crate is the notifier's `ThreadSlot`, and an `UnsafeCell` in `Work` would
now be *reintroducing* a boundary rather than adding to one. It needs two of
them, besides -- the `unsafe` deref in `invoke` and an
`unsafe impl Sync for Work<R>`, since `UnsafeCell` is not `Sync` on its own.
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

## The #60 finding: external submission costs ~100 ns a task, and 14 ns of it was a hash

The `embarrassing_parallelism` table above leaves 9.5 ms of run against rayon's
1.9 ms with no explanation, and #60 is where that was taken apart. The headline:
**the gap is the per-task handoff itself, not the routing, the buffer count or
the notifier**, and of the ~110 ns a source task cost end to end exactly one
component turned out to be reducible -- the `DefaultHasher` run that routed each
job to a buffer, now gone from the bulk path (see `Buffers::push_bulk`).

Everything below was measured with temporary counters and harnesses inside
`src/executor/queue.rs`; none of that instrumentation is in the shipped code,
and the atomics it added are a cost the measured figures carry but the crate
does not. **Three other agents were building and testing on this box
throughout** -- load average 4.7 to 16.0 across the runs below, named per table
-- so every comparison here is either a ratio inside one process or a counter
that does not depend on wall clock at all. #18's figures above are untouched.

### The four hypotheses the issue listed, and what the counters say

**1. "Is the hash distributing at all?" -- yes, and that was the first thing
killed.** 65536 sources of one taskflow, 24 workers, 5 buffers, counting which
buffer each `Buffers::push` landed in. Two runs, i.e. two different `Topology`
and `Flow` allocations:

| | buffer 0 | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|
| run A | 13257 | 13074 | 13058 | 13231 | 12916 |
| run B | 13149 | 13225 | 13144 | 12997 | 13021 |

A perfectly even deal is 13107.2. The worst cell is 1.5% off it, so the derived
`Hash` plus upstream's shift-and-remainder reduction spreads consecutive task
indices as well as anything could -- the concern that two of a `Job`'s three
hashed fields are constant within a run is answered: `DefaultHasher` is
SipHash-1-3 and mixes the index alone fine.

**2. "Does stealing find the work?" -- yes, essentially first time.** The same
runs counted steal outcomes per buffer: 65536 successful steals against 450 to
1121 empty attempts and 3410 to 8597 crossbeam `Steal::Retry`s. That is **1.01
to 1.02 buffer attempts per job and 0.05 to 0.13 retries per job**: the workers
are not thrashing on contended queue heads and they are not sweeping empty
queues. Whatever the cost is, it is not the search.

**3. "Does the notifier wake enough workers?" -- it already wakes everybody**,
and waking them *earlier* is worse. `notify_n(65536)` with 24 waiter slots takes
`NonblockingNotifier::notify_n`'s `n >= self.waiters.len()` branch, which is
`notify_all`. Two variants of waking earlier were measured against the shipped
"push everything, then notify": a `notify_all` before the pushes, and a
`notify_all` every 4096 pushes. Medians of 8 runs, interleaved, 65536 tasks at
24 workers:

| | notify after (shipped) | notify first | notify every 4096 |
|---|---|---|---|
| empty task body, warm pool | 6.02 ms | 13.66 ms | 13.66 ms |
| 543 ns task body, warm pool | 7.40 ms | 13.52 ms | 14.18 ms |
| 543 ns task body, pool parked 20 ms first | 8.82 ms | 18.19 ms | 19.56 ms |

Roughly **twice as slow**, and the mechanism is visible in hypothesis 5 below:
a woken worker's first act is to sweep the buffers, and those are the cache
lines the producer is still pushing into. Upstream's commented-out experiment in
`_bulk_schedule` says waking first "does not show any benefit"; here it is
actively harmful, and the `bulk_schedule` doc now records that.

**4. "How many buffers are there?" -- 5 for 24 workers, and more buffers do help
a little.** 65536 sources at 24 workers, dealt round-robin, medians of 8
interleaved runs:

| buffers | empty task body | 543 ns task body |
|---|---|---|
| 5 (`bit_width(24)`, shipped) | 6.42 ms | 7.44 ms |
| 12 | 5.95 ms | 6.68 ms |
| 24 | 5.90 ms | 6.62 ms |
| 48 | 5.58 ms | 6.41 ms |

**Declined, deliberately.** The buffer count is not a property of this path
alone: `Shared::num_queues` is every worker's victim space in every shape, so
going from 5 buffers to 48 would make 48 of 72 possible victims a queue that
only an external submitter ever fills -- paid by `graph_traversal` and
`binary_tree`, which push their successors onto worker queues and would draw
empty victims instead. Buying 10% on one shape with a slower steal loop
everywhere needs its own issue and its own measurement of the other three
shapes, not a line in this one.

**That issue was #102, and the last section of this file is its measurement**:
all six shapes, the shipped 5 against 12 and 48, on a quiet box. It declines
the change as well -- though not for the reason given here, which it also
measured and found wrong.

### 5. What it actually costs: ~100 ns a task, and more workers do not help

The decisive measurement is the shape with **no task body at all** -- 65536
`emplace`d closures that do nothing -- because then the wall clock is pure
scheduler. Medians of 4 runs each, rayon's `par_iter` over the same count in the
same process as the load control (its body does one `fetch_add`, ours does
nothing, so if anything this flatters neither side):

| workers | rustflow | per job | rayon |
|---|---|---|---|
| 1 | 6.08 ms | 93 ns | 0.16 ms |
| 2 | 22.88 ms | 349 ns | 0.56 ms |
| 4 | 13.54 ms | 207 ns | 1.34 ms |
| 8 | 9.82 ms | 150 ns | 1.29 ms |
| 16 | 6.74 ms | 103 ns | 1.16 ms |
| 24 | 7.32 ms | 112 ns | 1.26 ms |

**That column does not come down.** 24 workers cost *more* per job than one
does, which is the whole finding: the per-task cost of an externally submitted
graph is not work that parallelises, it is cache-coherence traffic that gets
worse as more cores join in. (The 2-worker row is 3x its neighbours and is not
explained; it reproduces, and it deserves its own look.)

Two harnesses split that ~100 ns, both with 65536 items and 24 consumer threads:

- **The bare handoff** -- one producer pushing plain `usize`s into the buffers
  while 24 thieves take them one at a time, no `Job`, no `Arc`, no task body --
  costs **87 to 91 ns an item** (5.7 to 5.9 ms). The executor adds almost
  nothing to it.
- **The shared counters a job carries** -- three `Arc` clone/drop pairs and two
  read-modify-writes on one shared counter, which is what `Job` plus
  `Parent::count_queued` and `Job::finish` do per task -- cost 15 ns a job on
  one thread and **63 ns a job across 24**, i.e. ~8 ns per contended atomic.
  That is the same figure #40 measured for its two refcounts (~9 ns) from the
  other direction.

So the budget is: ~60 ns of contended refcount and join-counter traffic, ~30 ns
of queue traffic moving the job from the producer's cache line to a consumer's,
and 14 ns of hash. The producer side of it alone -- materialise a `Job`, hash
it, push it -- is **36 to 41 ns a job measured at one worker** (the time for
`Executor::run` to return), rising to 46-103 ns at 24 workers as thieves pull on
the same lines. For 65536 sources that is a 2.4 to 6.7 ms single-threaded phase
during which 24 workers, which could retire a 543 ns task every 23 ns between
them, are starved by a producer that emits one every ~77 ns.

### The change: deal the batch, hash nothing

`Buffers::push_bulk` deals a batch round-robin and hashes nothing, which is
upstream's `_bulk_spill_round_robin` rather than its `_bulk_spill`; single
`Buffers::push` still hashes, since one item has nothing to deal. Interleaved
A/B in one process with both paths compiled in, **the order of the two swapped
every round**, 20 samples per id, medians, run only (the taskflow is built once
outside the loop); load average 4.7 to 8.1:

| id | hashed (before) | dealt (after) | change | rayon, same loop | ratio before -> after |
|---|---|---|---|---|---|
| 1 worker, 4096 | 2.768 ms | 2.719 ms | −1.8% | 3.198 ms | 0.87x -> 0.85x |
| 1 worker, 65536 | 42.634 ms | 40.620 ms | −4.7% | 35.984 ms | 1.18x -> 1.13x |
| 24 workers, 4096 | 752 µs | 561 µs | **−25.3%** | 429 µs | 1.75x -> 1.31x |
| 24 workers, 65536 | 7.917 ms | 6.705 ms | **−15.3%** | 3.000 ms | 2.64x -> 2.23x |

A method note worth keeping, because it nearly produced a wrong answer: with
the two paths always measured in the same order within a round, the 4096-task
row read **+61%** instead of −25%. At half a millisecond a run, whichever path
follows rayon's turn finds the pool in a different state from the one that
follows a rustflow run, and only alternating the order cancels it.

The criterion suite was then run before and after on all four shapes, adjacent
runs, load average 9.3 rising to 16.0 -- build *and* run, so these are the ids
the tables above quote:

| id | before | after | rayon before -> after |
|---|---|---|---|
| `embarrassing/24w/65536` | 15.73 ms | 11.81 ms | 2.51 -> 2.04 ms |
| `embarrassing/24w/4096` | 1.025 ms | 916 µs | 304 -> 167 µs |
| `embarrassing/1w/65536` | 46.20 ms | 45.53 ms | 36.08 -> 35.67 ms |
| `linear_chain/24w/16384` | 4.329 ms | 4.330 ms | n/a |
| `binary_tree/24w/65536` | 14.13 ms | 15.26 ms | 1.042 -> 1.022 ms |
| `graph_traversal/24w/56759` | 6.161 ms | 6.873 ms | 2.438 -> 2.387 ms |

The rayon column moved by 5 to 45% between those two runs, which is what this
box's load does over four minutes, and that is the reason the A/B table above is
the one to read. The other three shapes **cannot** be affected by more than a
rounding error and were not expected to be: a chain and a tree have exactly one
source task, and a 128x128 level graph has 128, so the change removes one hash
per run from the first two and 128 from the third -- under 2 µs of a 6 ms run.
Their rows here are the load, not the patch.

### What is left is architectural, and it is not a queue tweak

After the change a 65536-task flat graph is ~2.2x rayon rather than ~2.6x, with
~95 ns of per-task cost left. None of it is reachable from this module:

- **~60 ns is the shared counters a `Job` carries** -- #40's refcounts plus the
  parent's join counter up and down. That section explains why they are there
  and what would get them back (upstream's `_update_cache` continuation, which
  keeps one successor out of the queue entirely).
- **~35 ns is the handoff**, and the producer pays ~40 ns of it before any
  consumer is involved. That is the hard floor: **65536 sources cannot be
  submitted from one thread for less than ~2.6 ms**, which is already more than
  rayon's whole 1.9-3.0 ms run. No consumer-side cleverness can fix a
  producer-bound path, and the measurement says so directly -- replacing the
  one-at-a-time steal with crossbeam's `steal_batch_and_pop` in the bare-handoff
  harness made it *slower* (10.25 ms against 5.94 at 5 buffers, 7.29 against
  5.85 at 24), because batching helps a consumer that is behind and these
  consumers are starved.

The difference from rayon is therefore structural rather than a tuning gap:
`(0..n).into_par_iter()` never materialises n work items. It splits an index
*range* recursively, so the number of cross-thread handoffs is O(log n) and no
per-item write ever touches a line another core owns, while upstream's design --
which this port follows -- queues one node per task and hands each one across a
core boundary. Closing the rest of this gap means giving the scheduler a
splittable source range, i.e. something `tf::Taskflow` does not have and
`tf::TaskGroup`'s `for_each` is the nearest thing to; it is a milestone, not a
patch. **That is the recorded cost of the architecture, with the numbers above
as the evidence.** That milestone has since been measured: `for_each_by_index`
with a partitioner is that splittable range, and the "#343 attribution"
section at the end of this file finds its chunk loop to be rayon's equal and
the remaining gap to be this paragraph's own handoff, reaching only 15 to 19
of 24 workers on a fan-out shape.

## The two shapes #96 added, and how they were measured

`async_task` and `fibonacci` were ported and measured after #18's four, so
their numbers are a **separate run** under a load of their own and are not
comparable with the tables at the top of this file except in the way every
figure here is: within a row.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id |
| Date | 2026-11-17 |
| **Load average, `async_task` run** | **4.7 rising to 15.7 over the run** |
| **Load average, `fibonacci` run** | **25.2 falling to 10.4 over the run** |

At least one sibling agent was building this crate and running its test suite
on this box throughout both runs, which is what those two rows are: not a quiet machine,
and the `fibonacci` run started on the busiest part of it. The load is recorded
per id in the run logs and is why the method below is what it is.

**The method is order-swapped interleaving, and it is not optional here.** The
"#60 finding" section measured the cost of that: the same two ids read **+61%**
in a fixed order and **−25%** when the order was swapped every round, which is
a wrong answer rather than a noisy one. So each id was run in its own criterion
process, the ids of one row back to back, and the order *within* a row reversed
between round 1 and round 2 -- for `async_task` that is
`rustflow, rayon, rustflow-group` and then its reverse, for `fibonacci`
`rustflow-group, rustflow-runtime, rayon` and then its reverse. Every cell
below therefore carries **two point estimates of the mean, round 1 then round
2**, in the style of the #40 and #62 tables rather than the ± of #18's: on this
box the spread between two rounds minutes apart is the larger of the two
uncertainties, and hiding it behind criterion's within-id interval would
overstate what is known. Where that interval is itself the point, it is quoted
in the text.

Every number in the two tables comes from this run, measured on #96's branch
over `3bc9cdf` (#121) and shipped rebased onto `d966b28` (#122) -- which moves
`FlowBuilder`'s task accessors and touches nothing on the submission or
scheduling path these two shapes measure. #18's, #60's and #62's tables are
untouched.

### async_task

`N` independent asynchronous submissions and one wait, with a task body of one
relaxed increment: upstream's `async_task_taskflow` calls `silent_async` `N`
times and then `executor.wait_for_all()`. There is no graph, so both columns
measure the same thing for once -- the per-task cost of getting work to a
worker and the wait that notices it is done.

Three variants, because the submitting *thread* is the whole question here:

- **`rustflow`** submits from the benchmark's own thread, which is not a
  worker, so every job goes into the shared overflow buffers. This is the
  external-submission path, and the "#60 finding" section above is the crate's
  one measurement of it -- the figures below are to be read against it rather
  than to re-derive it.
- **`rustflow-group`** submits the same `N` tasks from *inside* one `spawn`ed
  body, into a `TaskGroup`, so the submitting thread is a worker of the pool.
  It is upstream's `async_task_tbb` -- `tbb::task_group`, `tg.run` `N` times,
  `tg.wait` -- translated literally. Its jobs go into the shared buffers too:
  a task group has no worker to hand the submission, which the "#119
  measurement" section below argues and which that section re-measured as
  unchanged, so the gap between this id and the first is **not** a gap between
  two destinations. (The bullet below said it was; that was wrong, and the
  correction is in that section rather than in these figures, which stand as
  measured.)
- **`rayon`** is `rayon::scope` plus `N` `spawn`s, which is the rayon form that
  matches upstream's TBB variant. `ThreadPool::install` makes the submitting
  thread a worker of the pool, so rayon has no external-submission variant to
  offer: its spawns always go onto a worker's own deque, which is a
  destination neither `rustflow` id here has.

| size | workers | rustflow | rustflow-group | rayon | upstream C++ |
|---|---|---|---|---|---|
| 4096 | 1 | *3.582* / 1.939 ms | 773.5 / 801.6 µs | 178.5 / 182.1 µs | not published |
| 4096 | 24 | 5.134 / 4.741 ms | 4.931 / 5.020 ms | 1.676 / 1.660 ms | not published |
| 65536 | 1 | 28.98 / 29.21 ms | 18.77 / 21.25 ms | 2.074 / 2.118 ms | not published |
| 65536 | 24 | 78.50 / 63.56 ms | 78.23 / 75.99 ms | 26.64 / 22.07 ms | not published |

The italicised 3.582 ms is not a result: criterion put a ±19% interval on it
(2.936 to 4.258 ms) where the same id in round 2 read 1.939 ms ±0.7%, and that
is the same unresolvable `1w/4096` id the #40 table had to drop. Read the
round-2 number; every other cell above agrees between rounds to better than
±12%, and most to better than ±3%.

What the table says, per task at 65536 and one worker, where the ids are
tightest:

- **An externally submitted async task costs ~440 ns**, against the ~100 ns a
  job of an externally submitted *graph* costs on the same path (#60). So the
  shape with no graph in it is four times more expensive per task than the
  shape with one, which is the opposite of what "no graph to build" suggests.
  Reading `Shared::schedule_async_task` says where that goes, and this run
  measured none of it -- no counters were added for it, and that is the
  follow-up: a free node is **allocated per task** where a graph's nodes
  already exist, two counters go up and down per task (`increment_async` is
  both `num_topologies` and `num_asyncs`, and the last decrement of each takes
  a mutex to meet a waiter), and `bulk_schedule` is called with **one** job,
  so the notifier is poked once per task where a run of 65536 sources pokes it
  once for the batch. **That ~440 ns is pre-#144 and pre-#376**: pinned on
  today's `main` the same id reads 344 to 357 ns a task, and "The #382
  measurement" at the end of this file has the two changes that moved it.
- **Submitting from a worker saves 30%** -- 305 ns against 440 -- and the
  mechanism is *not* the one this bullet first gave: both ids push into the
  shared buffers, so the saving is not a worker's own queue against them. What
  differs is the submitting thread, the per-task accounting a **parentless**
  submission takes that a group's child does not (`increment_async`'s two
  counters, and the last decrement of each meeting a waiter under a mutex),
  and the wait -- `wait_for_all` against a `corun` that also runs the tasks.
  This run separated none of those, so the figure stands and the cause is
  open; #119's section below confirms the figure does not move when a
  *runtime* body's destination changes. On today's `main` the pair is **150 ns
  against 350**, the same 57% the "#144 change" section read, and #382's block
  counts say what the gap is not: both ids allocate the same two blocks a
  task, so what differs is which thread frees them.
- **rayon's scope costs ~32 ns a task** at one worker, 14x less. A scope's
  spawn is a push onto the deque of the thread that is already running the
  scope, with no allocation beyond the closure and nothing to notify while the
  pool is awake; there is no graph and no topology to count.
- **More workers make every variant worse**, which is #60's finding reproduced
  on a shape with no graph at all: ours goes from 29 ms to 63-79 ms and
  rayon's from 2.1 ms to 22-27 ms. Nothing in 65536 one-increment tasks
  parallelises, so all 24 workers buy is cache-coherence traffic -- and
  because rayon loses an order of magnitude to it too, the gap *narrows* from
  14x at one worker to ~3x at 24. The honest summary of this shape is that
  neither framework should be handed 65536 tasks this small, and that ours
  pays four times as much as it does for the same tasks in a graph.

### fibonacci

The naive recursion, one branch spawned and the other computed on the calling
worker -- upstream's "tail optimization" -- with a wait that keeps that worker
in the pool. Upstream's `fibonacci_taskflow` wraps the whole recursion in one
`executor.async(...)`, because it has to start on a worker; so does each
`rustflow` id here. `rustflow-group` is upstream's `fibonacci/taskflow.cpp`
line for line; `rustflow-runtime` is the same recursion through a runtime task
(`rt.silent_async` with a runtime-shaped body, which this port spells
`silent_async_runtime`), which is upstream's own `Runtime.Fibonacci` unit test
rather than its benchmark; `rayon` is `rayon::join`.

**`n` is 12 and 15 where upstream's unit test asserts `fib(25)` and its
benchmark sweeps to `fib(40)`, and `rustflow-group` is why.** What bounds it is
the nested-corun stack bound, which the #359 section below measures once for
the whole file: a group's corun nests a frame per outstanding *task* rather
than one per level of recursion. That was both variants when #96 measured them;
#119 gave the **runtime** variant upstream's `if(auto w = this_worker(); w)`
branch, so its children go onto the submitting worker's LIFO queue and its
corun pops them straight back. The cliff was then re-measured in this profile,
with a release build of both recursions driven straight (no criterion):

| variant | workers | largest `n` that passes | smallest `n` that aborts |
|---|---|---|---|
| `rustflow-group` | 1 | 17 | 18 |
| `rustflow-group` | 24 | 23 | 24 |
| `rustflow-runtime` | 1 | 40, the top of upstream's sweep | not reached |
| `rustflow-runtime` | 24 | 40, the top of upstream's sweep | not reached |

The two group rows are #96's figures unchanged, which is the control: the fix
must not move the variant it does not touch, and it does not. The two runtime
rows are the fix -- `fib(40)` is 102334155 spawned children and took 36 s at
one worker, so what stops that variant now is the clock and not the stack. A
recursion through a runtime task nests one frame per level, and a chain of
nested coruns in this profile reaches 2400 levels before a worker's stack
gives out (2800 overflows); the figure for the debug profile the test suite
runs under is 500 levels, which `src/executor/runtime_async.rs` records.

**One worker is the binding end for the group variant**, measured as such in
the #359 section below -- the one worker that coruns is the only worker there
is to pick the outstanding children up, while 24 workers spread the same tree
over 24 stacks and reach six levels further. Since both variants measure the
same `n`, the group's one-worker cliff
is what sets the sizes: `fib(15)` is 986 spawned children and sits three
levels, a factor of four in task count, inside it. `tests/runtimes.rs` -- all
runtime tasks -- now asserts upstream's own `fib(25)` and sorts to ten
thousand elements, while `tests/task_group.rs` keeps the reduced inputs for the
reason `src/task_group.rs`'s module docs give: a group holds a worker *id*, not
the worker, and waits on the stealing end of its queue in any case. Measuring
the runtime id at upstream's `fib(25)` is now possible and is deliberately not
done here, because a row of the table above compares the two variants at one
`n`.

| `n` | workers | rustflow-group | rustflow-runtime | rayon | upstream C++ |
|---|---|---|---|---|---|
| 12 | 1 | 111.2 / 84.93 µs | 108.8 / 82.13 µs | 6.219 / 6.707 µs | not published |
| 12 | 24 | 1.289 / 1.231 ms | 1.262 / 1.302 ms | 16.49 / 16.71 µs | not published |
| 15 | 1 | 454.4 / 331.9 µs | 336.3 / 338.2 µs | 12.73 / 12.95 µs | not published |
| 15 | 24 | 2.880 / 3.664 ms | 2.308 / 2.399 ms | 23.06 / 23.17 µs | not published |

The one-worker rows of the two `rustflow` variants are the pair the load moved:
round 1 ran at a load average above 20 and round 2 below 15, and the `n = 12`
cells fell by 22 to 25% between them while the ratio against rayon held to
within 10%. That is this box, not the variants, and it is why the rows carry
two numbers.

What the table says:

- **The two `rustflow` variants were the same scheduler and measured the
  same**: a group per level and a runtime task per level cost the same thing,
  because what they both did per level was submit one child through
  `schedule_async_task` -- to the same destination -- and corun. At one worker
  they were indistinguishable (332-454 µs against 336-338 at `n = 15`, the
  group's own two rounds disagreeing by more than the two variants did). At 24
  workers the runtime variant read lower in both rounds -- 2.31-2.40 ms against
  2.88-3.66 -- which is the group allocation per level it does not make, but
  the group row's own spread was as wide as the difference, so this run did not
  separate them. **#119 separated them**, and by a lot more than that: the
  `rustflow-runtime` column of this table is pre-#119 and the "#119
  measurement" section below supersedes it.
- **~340 ns per spawned child at one worker** (335 µs over 986 children), which
  is the `async_task` table's worker-submitted figure (305 ns) to within 12% --
  the same path measured from a recursion rather than a loop, and a useful
  cross-check that this shape has no cost of its own at one worker. On today's
  `main` the group route's child is **327 to 334 ns** and the runtime route's
  **178 to 183 ns**, and "The #382 measurement" at the end of this file prices
  the two changes behind that: #376 took 12.5 to 16.5% off both routes at one
  worker, and #379 gave 8.6% of it back on the group route alone.
- **24 workers make it 7x slower**, 335 µs to 2.3 ms for the same 986 children,
  or ~2.4 µs each. This was the worst 1-to-24 scaling in the file and it is the
  same cause as the stack cliff above: every child crossed into the shared
  buffers and had to be stolen back out, and a parent that coruns was competing
  for its own children with 23 other workers. Upstream's LIFO branch is what
  this shape is built to exploit, and #119 gave it to the runtime variant --
  which is where a third of the one-worker time and a quarter to a third of the
  24-worker time went; see below. The group variant still pays this, and the
  buffers turn out to be only half of what it pays: the "#380 measurement"
  section below adds the fourth id, a group opened on the `Runtime`, and that
  id halves the one-worker cost (175 ns a child against 347) while reading
  38.178 ms at 24 workers against the `rustflow-group` id's 2.2967 in the same
  process. What the route costs this shape is upstream's `FibonacciSwapped`
  ordering, which leaves one child outstanding at a time for 23 awake workers
  to race for, and that section's ablation prices the two causes apart.
- **`rayon::join` costs ~13 ns a level at one worker**, 26x less, and that is
  the comparison being made rather than a scheduler being beaten: a `join`
  nobody steals from *is* direct recursion -- two closure calls and one atomic
  -- while a group or a runtime task queues a job per level whatever the pool
  is doing. rayon also barely notices 24 workers (12.7 to 23 µs) because most
  of its levels still run inline. The 100-150x ratio in the 24-worker rows is
  therefore a statement about work granularity: `fib(15)` has no work in it for
  a scheduler to amortise against, and ours schedules every level of it.

## The #119 measurement: a task body's own queue, on a quiet box

#119 gave a free async task submitted from inside a **runtime** body the queue
of the worker running that body -- upstream's `if(auto w = this_worker(); w)`
branch of `_schedule_async_task`, with the thread-local lookup replaced by an
argument the submission is handed. A change on the scheduling path touches
every shape, so all six were measured, not only the recursive one.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id |
| Date | 2026-11-17 |
| **Load average** | **2.9 to 14.3, and all of it this run's own** |

That last row is the first time in this file it can be said: no other agent was
building or testing on the box, and the load figure recorded at the start of
each of the 24 criterion processes is the decay of the previous process's own
24-worker ids. So the between-process noise here is this machine's rather than
somebody else's -- which is visible in the tables below as ids whose two rounds
still disagree by 14 to 66%, on both sides of the A/B, in the same places #96
found them.

**The method is #60's**, unchanged: A is `origin/main` (`1f81ff5`), B is this
branch, each a separate criterion process over the same ids, round 1 A then B
and round 2 **B then A**. Every cell is a point estimate of the mean, round 1
then round 2, per side. The benches were filtered to the `rustflow` ids: the
`rayon` baselines are a property of rayon and this change cannot move them.

### fibonacci, the shape that moved

| `n` | workers | main r1 / r2 | branch r1 / r2 | |
|---|---|---|---|---|
| 12 | 1 | 80.34 / 80.04 µs | **51.38 / 50.77 µs** | `rustflow-runtime`, −36% |
| 15 | 1 | 337.96 / 335.59 µs | **201.18 / 197.89 µs** | `rustflow-runtime`, −41% |
| 12 | 24 | 1.198 / 1.382 ms | **924.1 / 928.8 µs** | `rustflow-runtime`, −28% |
| 15 | 24 | 2.187 / 2.322 ms | **1.378 / 1.367 ms** | `rustflow-runtime`, −39% |
| 12 | 1 | 82.47 / 88.05 µs | 83.41 / 83.70 µs | `rustflow-group`, −2% |
| 15 | 1 | 336.36 / 333.70 µs | 336.36 / 342.87 µs | `rustflow-group`, +1% |
| 12 | 24 | 1.189 / 1.369 ms | 1.178 / 1.250 ms | `rustflow-group`, −5% |
| 15 | 24 | 2.365 / 2.936 ms | 2.568 / 2.421 ms | `rustflow-group`, −6% |

The runtime variant is **28% to 41% faster**, and the four percentages are
larger than any round-to-round spread in the rows they are in -- the branch's
own two rounds agree to within 1.6% on all four. At `n = 15` and one worker
that is 986 spawned children going from 340 ns each to 201, which is the cost
of a buffer push plus a steal back out, replaced by a push and a pop on the
same end of one deque. At 24 workers it is the same thing plus the contention
#96's bullet named: a parent that coruns is no longer competing with 23 other
workers for its own children.

The group variant is the control and does not move: its four cells sit inside
their own rounds' spread, which at 24 workers is 14 to 21% on both sides. It
cannot move, because a task group has no worker to hand the submission -- the
reason is in `src/task_group.rs`, and the cliff rows above are the other half
of the same statement.

### The other five shapes did not move

| shape | ids | largest change | largest spread within one side |
|---|---|---|---|
| `linear_chain` | 4 | +10.8% (`24w/1024`) | 16.9% |
| `binary_tree` | 4 | −14.1% (`1w/65536`) | 28.1% |
| `graph_traversal` | 4 | −15.7% (`1w/56759`) | 32.4% |
| `embarrassing_parallelism` | 6 | +4.6% (`build/65536`) | 6.9% |
| `async_task` | 8 | −25.1% (`rustflow/1w/65536`) | 65.9% |

Every one of those changes is **smaller than the disagreement between the two
rounds of one side** on the same id, which is the test #60's method is for:
the three double-digit figures are all ids whose own rounds disagree by more,
and they are the same ids #18 and #96 flagged as unresolvable on this box.
Four of these five shapes have no async submission in them at all -- a graph's
jobs were always queued by the worker-aware path -- so there is nothing in them
for this change to have moved, and the table says so rather than assuming it.

One id is worth naming because it is *not* inside the noise:
`async_task/rustflow-group/1w/65536` read 19.21 / 19.01 ms on main and 18.35 /
18.39 ms on the branch, a consistent **−4%** where neither side's rounds
disagree by more than 1%. A task group's submission path differs by exactly one
`match` on a `None` after this change and nothing else, so that is a
code-layout effect and not a scheduling one. It is recorded rather than
explained away.

### Where the stack cliff sits now

Re-measured rather than assumed, in both profiles, because they differ:

| profile | route | shape | passes | aborts |
|---|---|---|---|---|
| debug | runtime | `fib` at 1-4 workers | 30 (832040 children, 77 s) | not reached |
| debug | runtime | nested coruns, 1 worker | 500 | 600 |
| debug | group | `fib` at 1-4 workers | 15 | 16 |
| debug | group | both sorts, 1-8 workers | 2000 elements | 3000 |
| release | runtime | `fib` at 1 and 24 workers | 40 (36 s at one worker) | not reached |
| release | runtime | nested coruns, 1 worker | 2400 | 2800 |
| release | group | `fib` at 1 worker | 17 | 18 |
| release | group | `fib` at 24 workers | 23 | 24 |

The "nested coruns" rows are the useful ones for the runtime route, because
`fib` stops being stack-bound there: a body that spawns one child and coruns,
to a given depth, is the shape that measures frames per level directly -- about
4 KiB in debug and 870 bytes in release, bisected in steps of 400 here and
pinned to the byte by the #359 section below. A `fib` recursion nests one such
level per level of recursion now, so its own bound is thousands of levels deep
and its task count (1.6^n) runs out of clock first. The group rows are what
`tests/task_group.rs` and this benchmark's sizes are chosen inside.

## The #359 measurement: a nested corun costs ~1 KiB of stack a level

This is the one place the nested-corun stack bound is measured, and the
sections that used to each explain the cliff in their own words -- `fibonacci`,
`skynet`, `nqueens`, `merge_sort`, `integrate` and the #119 subsection above --
point here instead. `DESIGN.md`'s "Executor core" carries the decision that
rests on it: the stack stays at std's default and the bound is a documented
property of the port, named where a caller writes the wait.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0 |
| Profile | `--release`, and `dev` where a row says so |
| `ulimit -s` | 8192 KiB |
| Date | 2026-11-18 |
| **Load average** | **this run's own: no sibling agent was on the box** |

**The method** is a probe binary outside the repo with a path dependency on the
crate, driving two recursions straight rather than under criterion: a *chain* of
nested coruns, one child per level so that the nesting is the depth and nothing
else, and the three shapes themselves. Three instruments, two of them reading
the same quantity, so that the per-level figure rests on more than one of them:

- **A frame's size, by its address.** Every level records the address of one
  local; the differences are the per-level cost. The chain's deltas are uniform
  to the byte over twelve levels, so this is a frame size rather than an
  average over a mixture.
- **The cliff, by bisection against `RUST_MIN_STACK`.** std takes a spawned
  thread's stack size from that variable when the spawn names none, so the
  deepest chain that completes can be bisected at several stack sizes without
  touching the crate -- and the slope of the cliff against the stack size is an
  independent reading of the frame size above.
- **A shape's demand, by instrumenting the recursion.** A guard in each body
  counts the bodies live on the calling thread and the span from the outermost
  of them down to this frame, both maxed into globals. Driven under a 512 MiB
  stack it reports what a shape *would* need, not only whether it fits.

### What a level costs

| route | profile | bytes/level by frame address | by the cliff |
|---|---|---|---|
| `TaskGroup::corun` | release | 992 | 992.0 |
| `TaskGroup::corun` | debug | 3616 | -- |
| `Runtime::corun` | release | 928 | 931 |
| `Runtime::corun` | debug | 4512 | -- |

The last column is a slope over three stack sizes for the group route and a
single cliff divided by its stack size for the runtime one, which is why the
group figure has the extra digit. **Re-read on today's `main`** by #380's
probe, the same chain measures 1040 bytes a level through `Executor::task_group`
and 832 through `Runtime::corun`, with a third route to read -- 1104 through
`Runtime::task_group`, which did not exist when this section was taken. The
type on the frame and the chain's own body have both moved since, so "The #380
measurement" below carries that table rather than this one being corrected: a
per-level figure belongs to the binary it was measured in.

The group-route cliff rows the slope is taken from, at one worker:

| `RUST_MIN_STACK` | deepest chain that completes | first that aborts |
|---|---|---|
| 1 MiB | 1051 | 1052 |
| 2 MiB, and unset, which is identical | 2108 | 2109 |
| 4 MiB | 4222 | 4223 |

3171 levels over 3 MiB is 992.0 bytes a level, the frame-address figure to four
digits, and the ~6 KiB each row falls short of its stack size by is the fixed
part below the first level: the worker loop's own frames. That unset reads
identically to 2 MiB is the other thing this table pins -- a worker gets std's
default, nothing in the crate names a size, and `RUST_MIN_STACK` is therefore a
live knob for a caller who needs a deeper recursion than the default carries.
These rows also supersede the #119 subsection's coarser 870 bytes and 2400
levels for the runtime route, which were bisected in steps of 400. The debug
column is not a footnote: the test suite runs there, so its ceiling is a
quarter of the release one.

### Why one worker is the binding end, confirmed against the code

The code answers whose stack a stolen sub-task runs on.
`WorkerThread::corun_until` takes a job from `pop` or `explore_task` and calls
`invoke` on it, which runs that body on the waiting frame. A `TaskGroup`'s
children are pushed to the shared buffers rather than onto the submitting
worker's queue -- its corun loop runs on a `corun_view`, which holds no owning
queue end -- so the job a waiting worker is handed is the oldest outstanding one
in the pool, somebody's unfinished subtree, and that job is itself a parent that
submits children and coruns. The instrumentation counts the consequence:

| shape | workers | deepest nesting on one thread | the stack span it cost |
|---|---|---|---|
| `skynet` `MaxDepth = 4` | 1 | 1112 bodies | 1.12 MiB |
| `skynet` `MaxDepth = 4` | 2 | 559 | 576 KiB |
| `skynet` `MaxDepth = 4` | 4 | 344 | 354 KiB |
| `skynet` `MaxDepth = 4` | 24 | 77 | 78 KiB |
| `nqueens` `q = 8` | 1 | 1322 | 1.35 MiB |
| `nqueens` `q = 8` | 24 | 90 | 93 KiB |
| `integrate` `V = 3` | 1 | 3479 | 2.04 MiB |
| `integrate` `V = 3` | 24 | 241 | 134 KiB |

1112 is `(10^4 - 1) / 9` interior nodes plus the root body, and the same run
reports exactly one nesting chain started: at one worker the **whole tree** is
one chain of nested frames and there is nothing else on that stack. #359's
reading -- a stolen sub-task runs on its stealer's stack, so the depth is
spread across workers -- is therefore confirmed rather than assumed, and the
1112 / 559 / 344 / 77 column is the shape of the spreading: roughly 1/N,
because what is spread is the *outstanding set* and not the depth of the
recursion.

### What a shape demands, and why no stack size is a bound

One worker, under a stack large enough that nothing aborts:

| shape | size | bodies live | demand | per body |
|---|---|---|---|---|
| `skynet` | `MaxDepth = 4` | 1112 | 1.12 MiB | 1055 B |
| `skynet` | 5 | 11112 | 11.2 MiB | 1056 B |
| `skynet` | 6 | 111112 | 112 MiB | 1056 B |
| `nqueens` | `q = 8` | 1322 | 1.35 MiB | 1071 B |
| `nqueens` | 9 | 5459 | 5.58 MiB | 1072 B |
| `nqueens` | 10 | 22766 | 23.3 MiB | 1072 B |
| `nqueens` | 11 | 105851 | 108 MiB | 1072 B |
| `integrate` | `V = 1` | 829 | 497 KiB | 614 B |
| `integrate` | 2 | 1853 | 1.09 MiB | 615 B |
| `integrate` | 3 | 3479 | 2.04 MiB | 616 B |
| `integrate` | 4 | 5065 | 2.97 MiB | 616 B |

The demand predicts the cliff, checked three ways: `skynet 5` aborts at an
8 MiB stack and completes at 12, `nqueens 9` aborts at 4 MiB and completes at 6,
and `integrate`'s `V = 3` overshoots the default 2 MiB stack by 44 KiB, which
is the whole of why that size aborts where `V = 2`, at 1.09 MiB, does not. The
per-body column is the chain's 992 bytes plus what each shape's own body holds,
and `integrate`'s is smaller because half of its recursion is the right half it
refines inline rather than a body picked up off a queue.

What the demand column says is that it is **geometric in the shape's size**: ten
times per `skynet` level, four and a half per `nqueens` column. Upstream's own
sweeps top out at `MaxDepth = 8` and `q = 14`, which at one worker through this
route is on the order of 11 GiB and 10 GiB of stack. No constant reaches that,
and the constant a raise would plausibly pick buys one notch of two shapes and
none of the third: 8 MiB -- what `ulimit -s` gives a C++ `std::thread` on this
box, four times what std gives a Rust one -- takes `nqueens` from 8 to 9 and
`integrate` from 2 to 8, and leaves `skynet` at 4. Costing, for every executor
and not only the recursive ones, 6 MiB of address space a worker: measured at 24
workers as `VmSize` 1625600 kB against 1773068 kB, with `VmRSS` moving 2476 kB
to 2568 kB, since the pages are reserved and never touched.

### The route is what the bound is about

The same ten-way tree at one worker, submitted through `Runtime` instead:

| `skynet` `MaxDepth` | deepest nesting on one thread | stack span | time |
|---|---|---|---|
| 4 | 5 bodies | 4 KiB | -- |
| 5 | 6 | 5 KiB | -- |
| 6 | 7 | 6 KiB | -- |
| 7 | 8 | 7 KiB | 2.0 s |
| 8, upstream's top | 9 | 8 KiB | 20.2 s |

A `Runtime`'s child goes onto the submitting worker's own LIFO queue, which is
what #119 changed, so its corun pops the child it is waiting for and nests one
frame per *level of recursion*. The whole of upstream's skynet sweep then runs
on one worker in eight kilobytes of stack -- a hundred million leaves and 11.1
million interior nodes -- where the same tree through a `TaskGroup` wants 11 GiB.
That is the measurement behind what `TaskGroup::corun`'s own docs tell a caller,
and it is why the bound belongs to a queue destination rather than to `corun`.

### The cliffs at the default stack, and one correction

| shape | 1 worker: passes / aborts | 24 workers: passes / aborts |
|---|---|---|
| `skynet` `MaxDepth` | 4 / 5 | 5 / 6 |
| `nqueens` `q` | 8 / 9 | 10 / 11 |
| `integrate` `V` | 2 / 3 | 24 / 32 |

The one-worker column reproduces #340's and #341's figures exactly, which is
what makes the probe trustworthy for the rest of this section. The 24-worker
cell for `integrate` does not reproduce: `V = 3` completed ten times out of ten
here, and so did 4, 6, 8, 12, 16 and 24, with 32 the first to abort (one pass in
three). Three sibling agents were building on this box when #341 took that
cell, which the `merge_sort` and `integrate` sections recorded where their
figures now are, until #352 replaced that note with a table taken on a quiet
box. 24 workers is the end a load moves while one worker is not, and the
demand table says why: a 24-worker run needs 134 KiB for `V = 3` against the
one worker's 2.04 MiB. The one-worker end is what every size in this file is
chosen inside, so no size changed on account of this -- #352 re-took every
figure and resized nothing.

## The #342 decision: the resumed parent stays on a queue, and no shape here resumes one

The #62 section above ends on the half of upstream's locality optimisation
this port did not take: a preempted parent, resumed when its last child
finishes, goes back on a queue rather than being run on the spot
(`WorkerThread::finish_job`), and "the shapes that would [move] are the nested
ones, and nothing here measures those yet". There are five of those shapes in
this file now, so this is that measurement. **The change is declined, and what
declines it is a counter rather than the clock: over one run of every id of
the eleven shapes the suite A/Bs, the re-queue it would replace happens twelve
times, all twelve of them in `pipeline`, and in the five nested shapes it
happens never.**

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id |
| Instrument | `devtools/bench-ab.sh`, 3 interleaved rounds, A then B in each |
| Date | 2026-11-19 |
| **Load average** | **2.8 to 14.2 over the pass, all of it this pass's own** |

That last row is the one the method depends on. No sibling agent was on the
box: every load figure the eleven reports recorded -- start, after each build,
after each round -- is the benchmark itself, which is why it rises with the
24-worker ids and falls back between runs. The whole pass ran 00:45Z to
01:17Z.

### The path, from the last child's tear-down to the parent's next dispatch

`WorkerThread::finish_job` is the whole of it. The last job of a nested
graph reads the parent's state word **before** spending its count, as upstream
reads it, and exactly one child -- the one whose `fetch_sub` returns 1 -- finds
`PREEMPTED` set and calls `Nested::resume`, which hands back the job the
parent node was dequeued as. That job then goes through
`WorkerThread::schedule`: onto **this worker's own LIFO queue** when the worker
holds an owning queue end, into the **shared spill buffers** when it is a corun
view (`LocalQueue::Corun`), and `notify_one` either way. So what a re-queue
costs is a push, a notify, and a pop that may be another worker's steal --
which is the hand-off #62 priced on a chain link: 22 to 34 ns at one worker
and 128 to 137 ns at 24.

The continuation slot #62 added is **not eligible** for the resumed parent as
`main` stands, and not by oversight: `update_cache` is reached only from
`invoke_one`'s two successor loops, and `finish_job` returns `()` to a caller
that has already produced its `cache`.

**Upstream v4.1.0 resumes on the spot**, so the "B" side here is upstream's
behaviour rather than a hypothesis of ours. `taskflow/core/executor.hpp`'s
`_tear_down_nonasync` ends in `_update_cache(worker, cache, parent)` under
exactly this condition -- "this task is spawned from a preempted parent, so we
need to resume it" -- and every caller of it closes with
`TF_INVOKE_CONTINUATION()`, including the cancelled early-out at the top of
`_invoke`. The same `_update_cache(worker, cache, parent)` appears a second
time, in the tear-down of a taskflow module's topology, with the `PREEMPTED`
test commented out around it.

B is that behaviour, in a two-file patch of 28 added lines and 11 removed:
`finish_job` returns `Option<Job>` instead of calling `schedule`, and
`invoke_one`'s two calls to it put what comes back into the slot through
`update_cache` -- which queues a successor already there, as upstream's
`_update_cache` queues it. Nothing else in the scheduler changes.

### What the suite resumes: twelve events in eleven shapes

Before timing a change, count the events it is for. A build of `main`
carrying four counters at that tear-down -- nested tear-downs, those that
drove the parent's counter to zero, those that then found `PREEMPTED`, and
those that re-queued a job -- read at every `Executor::drop`, with each bench
binary run in criterion's `--test` mode over the ids the A/B below uses:

| bench | ids counted | nested tear-downs | drove the counter to zero | found `PREEMPTED` | re-queued |
|---|---|---|---|---|---|
| `fibonacci` | `rustflow-group`, `rustflow-runtime` | 4872 | 2436 | 0 | **0** |
| `skynet` | `rustflow-group` | 48880 | 4915 | 0 | **0** |
| `nqueens` | `rustflow-group` | 8832 | 5703 | 0 | **0** |
| `merge_sort` | `rustflow` | 4152 | 4152 | 0 | **0** |
| `integrate` | `rustflow` | 10720 | 10720 | 0 | **0** |
| `linear_chain` | `rustflow` | 0 | 0 | 0 | **0** |
| `binary_tree` | `rustflow` | 0 | 0 | 0 | **0** |
| `graph_traversal` | `rustflow` | 0 | 0 | 0 | **0** |
| `embarrassing_parallelism` | `rustflow` | 0 | 0 | 0 | **0** |
| `sort` | `rustflow` | 383 | 0 | 0 | **0** |
| `pipeline` | `rustflow` | 80007 | 12 | 12 | **12** |

Cumulative over both worker counts, one run of each id's routine. The counter
is not broken, which the two cases that do fire establish: `pipeline`
re-queues a parent once per pipeline run, twelve ids being twelve events, and
the unit test the issue names --
`executor::runtime::tests::a_body_that_leaves_work_outstanding_is_preempted_and_resumed`
-- reports `nested=1 last=1 preempted=1 resumed=1` under the same build.

**Why the five nested shapes never reach it is the shape of their join, not
their size.** All five wait with a `corun`, and a `corun` leaves nothing
preempted.

- Four of them, and `fibonacci`'s `rustflow-group` id, wait on a
  **`TaskGroup`**, whose children are parented to the group's own anchor:
  `Nested::new(None)`, which carries `IMPLICITLY_ANCHORED`, never carries
  `PREEMPTED`, and has no job to resume. What the last child's decrement
  releases is the *frame* watching that counter inside `TaskGroup::corun`, so
  there is no job to keep out of a queue in the first place. That is the same
  fact the #359 section measures from the other end: the parent is a live frame
  on a worker's stack, which is why the group route costs about a kilobyte of
  stack a level.
- `fibonacci`'s `rustflow-runtime` id does parent its children to the node
  (`Submission::under_node`), and that node *is* `PREEMPTED` for the length of
  its body -- `executor::runtime`'s `enter` sets the bit before the body runs
  and holds a reference of its own on the join counter while it does. But
  `Runtime::corun` waits for that counter to come back to one, so when the body
  returns nothing is outstanding, the body's own `fetch_sub` returns 1, and the
  node finishes on its **first** entry. A node that never leaves the dispatch
  preempted is never resumed.

**This supersedes four sentences in the sections below**, two in `skynet`'s
and two in `nqueens`'s: "every one of the ... interior nodes is a **preempted
parent**", "every parent goes back on a queue instead of being resumed where
it stopped", and the pair that make the re-queue "a larger share of a node's
cost" in the ragged tree than in the regular one. The interior nodes of those
shapes *are* parents, and they are why the shapes cost what they do, but the
mechanism is an anchored frame rather than a preempted node, so none of
`skynet`'s 184 ns a task and none of the `nqueens` figures those sentences
divide is a re-queue a resumed parent could give back. The pair is still
worth measuring together; what distinguishes them is the fan-out, not the
tear-down. It also answers the two forward references this section was
promised to: `merge_sort`'s, which called that shape one of the five #342
could A/B, and `integrate`'s, which expected the resume to raise its own
stack cliff. It raises nothing -- the frames that make the cliff are a
`corun`'s, not a preempted node's -- so **no size in this file moves on
account of #342**.

What does leave a parent preempted is a body that returns with work still
outstanding -- a `silent_async` or a `scope` with no join, a subflow, a module
task -- and `pipeline`'s twelve are the only such events in the file.

### The A/B anyway: 56 ids, and what they can see

The counters say B's branch is never taken in ten of the eleven shapes and
once per run in the eleventh, so what the sweep below can see is code layout
and this box's own noise. It was run all the same, because that is what the
issue asked for and because a sweep of 56 ids is a useful reading of the
resolution this instrument has today. Each row is one `bench-ab.sh` run of
three interleaved rounds; the ratio is of the two sides' medians and the
verdict is the script's own, which divides the change by the wider of the two
within-side spreads.

The five nested shapes, at both worker counts and both sizes:

| id | A spread | B spread | A median | B median | B/A | verdict |
|---|---|---|---|---|---|---|
| `fibonacci/rustflow-group/1w/12` | 1.012x | 1.035x | 84.91 µs | 84.44 µs | 0.994x (-0.6%) | inside |
| `fibonacci/rustflow-group/1w/15` | 1.036x | 1.012x | 335.7 µs | 322.4 µs | 0.960x (-4.0%) | marginal |
| `fibonacci/rustflow-group/24w/12` | 1.153x | 1.106x | 1.172 ms | 1.102 ms | 0.940x (-6.0%) | inside |
| `fibonacci/rustflow-group/24w/15` | 1.106x | 1.144x | 2.296 ms | 2.18 ms | 0.950x (-5.0%) | inside |
| `fibonacci/rustflow-runtime/1w/12` | 1.071x | 1.094x | 54.13 µs | 52.29 µs | 0.966x (-3.4%) | inside |
| `fibonacci/rustflow-runtime/1w/15` | 1.038x | 1.045x | 195.6 µs | 196.4 µs | 1.004x (+0.4%) | inside |
| `fibonacci/rustflow-runtime/24w/12` | 1.048x | 1.042x | 864.9 µs | 872.5 µs | 1.009x (+0.9%) | inside |
| `fibonacci/rustflow-runtime/24w/15` | 1.052x | 1.028x | 1.368 ms | 1.418 ms | 1.037x (+3.7%) | inside |
| `skynet/rustflow-group/1w/3` | 1.037x | 1.018x | 245.8 µs | 242 µs | 0.984x (-1.6%) | inside |
| `skynet/rustflow-group/1w/4` | 1.035x | 1.017x | 2.433 ms | 2.391 ms | 0.983x (-1.7%) | inside |
| `skynet/rustflow-group/24w/3` | 1.036x | 1.030x | 563.3 µs | 558.6 µs | 0.992x (-0.8%) | inside |
| `skynet/rustflow-group/24w/4` | 1.078x | 1.108x | 2.553 ms | 2.674 ms | 1.048x (+4.8%) | inside |
| `nqueens/rustflow-group/1w/6` | 1.123x | 1.011x | 83.3 µs | 86.13 µs | 1.034x (+3.4%) | inside |
| `nqueens/rustflow-group/1w/8` | 1.044x | 1.007x | 1.154 ms | 1.145 ms | 0.993x (-0.7%) | inside |
| `nqueens/rustflow-group/24w/6` | 1.044x | 1.148x | 813.2 µs | 791.3 µs | 0.973x (-2.7%) | inside |
| `nqueens/rustflow-group/24w/8` | 1.066x | 1.039x | 3.023 ms | 2.884 ms | 0.954x (-4.6%) | inside |
| `merge_sort/rustflow/1w/10000` | 1.018x | 1.051x | 278 µs | 277.3 µs | 0.998x (-0.2%) | inside |
| `merge_sort/rustflow/1w/1000000` | 1.026x | 1.012x | 59.62 ms | 56.94 ms | 0.955x (-4.5%) | **a result** |
| `merge_sort/rustflow/24w/10000` | 1.067x | 1.092x | 834.8 µs | 823.4 µs | 0.986x (-1.4%) | inside |
| `merge_sort/rustflow/24w/1000000` | 1.142x | 1.120x | 18.74 ms | 19 ms | 1.014x (+1.4%) | inside |
| `integrate/rustflow/1w/1` | 1.026x | 1.077x | 287.2 µs | 289.1 µs | 1.007x (+0.7%) | inside |
| `integrate/rustflow/1w/2` | 1.029x | 1.018x | 633 µs | 637.4 µs | 1.007x (+0.7%) | inside |
| `integrate/rustflow/24w/1` | 1.068x | 1.159x | 1.31 ms | 1.235 ms | 0.943x (-5.7%) | inside |
| `integrate/rustflow/24w/2` | 1.063x | 1.020x | 2.099 ms | 2.086 ms | 0.994x (-0.6%) | inside |

**Twenty-two of the twenty-four are inside one side's own spread, and the two
that are not have no mechanism behind them.** `merge_sort/rustflow/1w/1000000`
reads -4.5% against sides inside 2.6% and 1.2% of themselves, which the script
calls a result at 1.8x the wider spread -- and the counters above say B's
branch is taken zero times in that shape, so what it can be is code layout or
a spread three rounds underestimated. One id in 56 at 1.8x is also what a
sweep this wide produces on its own. `fibonacci/rustflow-group/1w/15` at -4.0%
and 1.15x is the script's "marginal", which is the same reading with less of
it. No id in the table is the gain the change was proposed for, and none could
have been.

The six graph shapes, which is the regression check #144's section ran:

| id | A spread | B spread | A median | B median | B/A | verdict |
|---|---|---|---|---|---|---|
| `linear_chain/rustflow/1w/1024` | 1.337x | 1.011x | 154.8 µs | 154.3 µs | 0.997x (-0.3%) | inside |
| `linear_chain/rustflow/1w/16384` | 1.178x | 1.038x | 2.161 ms | 2.124 ms | 0.983x (-1.7%) | inside |
| `linear_chain/rustflow/24w/1024` | 1.033x | 1.286x | 162 µs | 148.1 µs | 0.914x (-8.6%) | inside |
| `linear_chain/rustflow/24w/16384` | 1.037x | 1.031x | 2.053 ms | 2.081 ms | 1.014x (+1.4%) | inside |
| `binary_tree/rustflow/1w/4096` | 1.202x | 1.010x | 683.7 µs | 679.9 µs | 0.994x (-0.6%) | inside |
| `binary_tree/rustflow/1w/65536` | 1.369x | 2.332x | 11.31 ms | 10.34 ms | 0.914x (-8.6%) | inside |
| `binary_tree/rustflow/24w/4096` | 1.030x | 1.023x | 1.046 ms | 1.026 ms | 0.981x (-1.9%) | inside |
| `binary_tree/rustflow/24w/65536` | 1.024x | 1.166x | 16.31 ms | 15.72 ms | 0.964x (-3.6%) | inside |
| `graph_traversal/rustflow/1w/3476` | 1.181x | 1.059x | 807.3 µs | 774.2 µs | 0.959x (-4.1%) | inside |
| `graph_traversal/rustflow/1w/56759` | 1.009x | 1.562x | 13.21 ms | 13.12 ms | 0.993x (-0.7%) | inside |
| `graph_traversal/rustflow/24w/3476` | 1.007x | 1.015x | 431.6 µs | 429.6 µs | 0.995x (-0.5%) | inside |
| `graph_traversal/rustflow/24w/56759` | 1.034x | 1.046x | 6.592 ms | 6.609 ms | 1.003x (+0.3%) | inside |
| `embarrassing_parallelism/rustflow/1w/4096` | 1.451x | 1.262x | 3.084 ms | 2.882 ms | 0.934x (-6.6%) | inside |
| `embarrassing_parallelism/rustflow/1w/65536` | 1.323x | 1.290x | 58.73 ms | 48 ms | 0.817x (-18.3%) | inside |
| `embarrassing_parallelism/rustflow/24w/4096` | 1.044x | 1.076x | 961.7 µs | 1.005 ms | 1.045x (+4.5%) | inside |
| `embarrassing_parallelism/rustflow/24w/65536` | 1.007x | 1.040x | 13.78 ms | 13.8 ms | 1.001x (+0.1%) | inside |
| `sort/rustflow/1w/1000` | 1.026x | 1.054x | 20.23 µs | 19.79 µs | 0.979x (-2.1%) | inside |
| `sort/rustflow/1w/1000000` | 1.041x | 1.052x | 37.16 ms | 37.07 ms | 0.997x (-0.3%) | inside |
| `sort/rustflow/24w/1000` | 1.314x | 1.048x | 22.78 µs | 21.87 µs | 0.960x (-4.0%) | inside |
| `sort/rustflow/24w/1000000` | 1.027x | 1.172x | 14.04 ms | 15.18 ms | 1.081x (+8.1%) | inside |
| `pipeline/rustflow/1l_1w/1024` | 1.019x | 1.011x | 2.253 ms | 2.231 ms | 0.990x (-1.0%) | inside |
| `pipeline/rustflow/1l_1w/16384` | 1.023x | 1.022x | 35.25 ms | 35.5 ms | 1.007x (+0.7%) | inside |
| `pipeline/rustflow/1l_24w/1024` | 1.188x | 1.029x | 4.597 ms | 2.321 ms | 0.505x (-49.5%) | **a result** |
| `pipeline/rustflow/1l_24w/16384` | 1.020x | 1.030x | 35.81 ms | 35.54 ms | 0.992x (-0.8%) | inside |
| `pipeline/rustflow/4l_1w/1024` | 1.039x | 1.012x | 2.34 ms | 2.344 ms | 1.001x (+0.1%) | inside |
| `pipeline/rustflow/4l_1w/16384` | 1.029x | 1.012x | 36.87 ms | 37.34 ms | 1.013x (+1.3%) | inside |
| `pipeline/rustflow/4l_24w/1024` | 1.001x | 1.008x | 1.764 ms | 1.785 ms | 1.012x (+1.2%) | **a result** |
| `pipeline/rustflow/4l_24w/16384` | 1.005x | 1.010x | 27.79 ms | 27.95 ms | 1.006x (+0.6%) | inside |
| `pipeline/rustflow/8l_1w/1024` | 1.030x | 1.016x | 2.364 ms | 2.382 ms | 1.008x (+0.8%) | inside |
| `pipeline/rustflow/8l_1w/16384` | 1.037x | 1.006x | 36.5 ms | 37.37 ms | 1.024x (+2.4%) | inside |
| `pipeline/rustflow/8l_24w/1024` | 1.007x | 1.011x | 1.772 ms | 1.792 ms | 1.012x (+1.2%) | marginal |
| `pipeline/rustflow/8l_24w/16384` | 1.005x | 1.008x | 27.93 ms | 28.24 ms | 1.011x (+1.1%) | **a result** |

**Twenty-eight of the thirty-two are inside a spread; all four that are not
are `pipeline`, and they point in both directions at once.** Three of them are
the 24-worker ids whose sides are inside 1% of themselves -- `4l_24w/1024`,
`8l_24w/1024` and `8l_24w/16384`, at +1.2%, +1.2% and +1.1% -- which is the
only consistent signal anywhere in the sweep, and at one re-queue per
pipeline run it cannot be the mechanism either -- a mechanism that fires once
per 1.8 ms cannot cost 1% of 1.8 ms: it is 1% of three ids of the one shape
whose sides are tight enough for 1% to clear them. The fourth,
`1l_24w/1024`, reads -49.5% with both sides' own rounds tight around their
medians, and it is the useful row rather than the striking one: its A side
*is* `origin/main`, the same source the #352 pass measured at 2.524 ms, and it
came out at 3.90, 4.63 and 4.60 ms here. A same-source disagreement of 1.8x is
larger than every A-to-B ratio in these two tables put together, so what that
id resolves is nothing, and B's 2.32 ms agreeing with #352's figure is the
coincidence rather than the finding. A one-line pipeline at 24 workers is the
degenerate id the `pipeline` section already calls out; this run adds that it
is bistable across builds.

### The decision: declined

#342's bar was that the five nested shapes gain outside their spread **and**
the six graph shapes do not lose. Neither half is met, and the first could not
have been: the branch B changes is taken zero times in all five nested shapes
and in four of the six graph shapes, so there was no gain available to measure
there. What is left is three `pipeline` ids reading 1% the wrong way and a
mechanism change to the hottest path in the scheduler, which is not a trade to
take for a branch no benchmarked shape takes more than once a run.

What the change is worth is bounded by the same two measurements rather than
guessed at. In the five nested shapes it is exactly nothing: the branch is
never taken. In `pipeline` it is one hand-off per pipeline run, which at
#62's price -- 128 to 137 ns at 24 workers, 22 to 34 at one -- is 137 ns off
an id that costs 1.76 ms, six thousandths of one percent, and the three
`pipeline` ids that moved at all moved 1% the other way. The port therefore
keeps the queue, and `DESIGN.md`'s "Executor core" carries the decision.

**What would reopen it is a shape, not a better box.** A benchmark that leaves
parents preempted -- a recursion through module tasks, a subflow per level, or
a `silent_async` tree joined once at the root instead of at every level --
would put the path under load for the first time, and the A/B is 28 lines and
half an hour once such a shape exists. The one place this file already touches
it is `pipeline`, at one event per run.

### The invariant this cannot weaken

The port promises **one worker per node at a time**, enforced rather than
assumed, by three things: `graph::work`'s `lock_callable`, a `try_lock` that
panics naming the invariant rather than blocking; `executor::worker`'s
`is_entered`, which reads the `PREEMPTED` bit of the four handle kinds that
can carry it, so that a release landing on a node something has already
entered is reported instead of resuming it; and the `DispatchClaim`
`invoke_one` takes and gives back once per job dispatch (#320). The B side
touches none of the three, and the argument it rests on is the counting rather
than the locks. Exactly one child of a preempted parent sees its
`fetch_sub` return 1, so exactly one resume job exists however it travels;
`invoke_one` takes the claim on entry whether that job arrived from a queue or
from the continuation slot; and the count the resumed job holds against *its
own* parent was taken when it was first queued and is spent only when the
resumed dispatch finishes, which is what makes holding it across the
tear-down of its last child sound -- `update_cache`'s own docs make that
argument for a successor. What B does remove is the `notify_one` that re-queue
performs, which is why a test asserting the wake rather than the resumption
would have to be rewritten against it: five unit tests pop the worker's queue
to prove the re-queue happened, and under B the resumed job never goes there.
They were the only things in the suite that failed against it; the other 2644
tests passed unchanged.

## The #102 decision: 5 spill buffers stay, and here is what 48 costs

`Buffers::new` makes `bit_width(num_workers)` spill buffers -- 5 at 24 workers
-- which is upstream's sizing and its own "log2(N) performs best". The "#60
finding" above measured that 48 buffers are worth about 10% to
`embarrassing_parallelism` and declined to take them, because the count is also
every worker's victim space; #102 is where that decision was measured rather
than argued. **The count stays at `bit_width(num_workers)`**, and this section
is the evidence: on this box 48 buffers buy 4 to 6% on the two shapes that
keep thousands of jobs in the buffers at once and cost 8 to 26% on two of the
four that keep a handful there, and 12 buffers buy a third of the win for most
of the cost.

Nothing in the crate changed to measure it. The two sizings are the same
binary under an environment override read once per `Buffers::new`, and the
counter figures below come from a temporary instrumented build; none of that
instrumentation is in the shipped code.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id |
| Date | 2026-11-17 |
| **Load average** | **1.9 to 17.8, and all of it this run's own** |

**The box was quiet**, as it was for #119 and for no section before that: no
other agent was building or testing on it, and the load figure recorded at the
start of each of the 156 criterion processes is the decay of the previous
process's own 24-worker ids -- it reads 1.9 to 2.3 through the two shapes whose
ids are cheapest and climbs to 17.8 through `async_task` and `fibonacci`, which
is this run's own 24 threads and nobody else's.

### How many of a worker's victims are buffers, and can the two be sized apart

Read against the code, which is what the issue asked for first:

- `Shared::num_queues()` is `workers.len() + buffers.num_buffers()`, and
  `WorkerThread::explore_task` passes exactly that to `victim_excluding`, which
  draws `draw % (num_queues - 1)` and then skips the drawing worker's own
  index. So at 24 workers and 29 queues a worker draws over **28 candidates, of
  which 5 are buffers -- 17.9%**. At 12 buffers it is 12 of 35 (34.3%), and at
  48 it is **48 of 71 (67.6%)**: two draws in three would land on a queue only
  a non-worker submitter ever fills.
- **The loop can tell the two apart where it steals, and cannot where it
  draws.** `WorkerThread::steal_from` branches on `victim < num_workers` and
  calls a different method for each kind; the draw above is one remainder over
  the union, which is the whole reason upstream numbers its victims
  `0..num_workers + num_buffers` in the first place, as `steal_from`'s own doc
  records.
- **So the two spaces could be sized apart**, and the shape it would take is
  clear enough to write down: draw over `num_workers + 1` slots with the last
  one meaning "the buffers", and resolve that slot through
  `Buffers::first_nonempty`. The buffer count would then leave the draw
  entirely.

That is not taken here, and the measurement is why rather than taste. The count
leaks out of the submission path in **three** places, not one: the draw's
range, the spin budget `max_steals = (num_queues + 1) * 2` that decides when a
worker stops stealing and parks, and the `first_nonempty` sweep itself, which
is O(buffers). Separating the draw fixes one of the three. And the draw is not
where the time goes anyway: the dilution arithmetic above predicts 2.5x more
wasted steal attempts at 48 buffers than at 5 (71 candidates against 28 for one
item), where the counters below measure **116x**. A fix aimed at the draw is
therefore aimed at the wrong number, and a worker that resolved "the buffers"
by sweeping 48 of them would pay the sweep instead. Sizing the two spaces apart
is the better change *if the win is ever wanted*; on these figures the win is
not worth wanting, so it stays unbuilt and this paragraph is the record of what
it would be.

### Method

#60's, unchanged, which this file has twice recorded as not optional: a fixed
order read **+61%** on an id that order-swapping read **−25%** on. A is the
shipped `bit_width(num_workers)`, B is `2 * num_workers` buffers -- **48 at 24
workers**, which is #60's best row, and 2 at one worker. Each id ran in its own
criterion process, filtered to the `rustflow` ids because the `rayon`
baselines cannot move, and the four processes of an id ran back to back in the
order **A B B A**, so each side holds positions 1 and 4 or 2 and 3 and neither
is systematically warmer. Every cell is a point estimate of the mean, round 1
then round 2, and the column that decides whether a change is real is the
**spread between the two rounds of one side** on that id.

### 24 workers: 5 buffers against 48

| id | 5 buffers, r1 / r2 | 48 buffers, r1 / r2 | change | widest spread in one side |
|---|---|---|---|---|
| `linear_chain/rustflow/1024` | 145.76 / 146.37 µs | 187.15 / 181.91 µs | **+26.3%** | 2.9% |
| `linear_chain/rustflow/16384` | 1.942 / 1.946 ms | 1.951 / 1.905 ms | −0.8% | 2.4% |
| `binary_tree/rustflow/4096` | 1.143 / 1.141 ms | 1.131 / 1.106 ms | −2.0% | 2.2% |
| `binary_tree/rustflow/65536` | 14.552 / 14.542 ms | 14.493 / 14.796 ms | +0.7% | 2.1% |
| `graph_traversal/rustflow/3476` | 456.54 / 460.60 µs | 460.30 / 456.48 µs | −0.0% | 0.9% |
| `graph_traversal/rustflow/56759` | 5.899 / 5.866 ms | 5.942 / 5.868 ms | +0.4% | 1.3% |
| `embarrassing/rustflow/4096` | 874.96 / 885.65 µs | 855.35 / 871.80 µs | −1.9% | 1.9% |
| `embarrassing/rustflow/65536` | 11.949 / 11.986 ms | 11.287 / 11.115 ms | **−6.4%** | 1.5% |
| `async_task/rustflow/4096` | 4.970 / 4.879 ms | 4.709 / 4.600 ms | −5.5% | 2.4% |
| `async_task/rustflow/65536` | 80.945 / 85.632 ms | 73.286 / 81.793 ms | −6.9% | 11.6% |
| `async_task/rustflow-group/4096` | 5.017 / 4.970 ms | 4.771 / 4.791 ms | **−4.3%** | 1.0% |
| `async_task/rustflow-group/65536` | 77.300 / 75.964 ms | 73.307 / 73.331 ms | **−4.3%** | 1.8% |
| `fibonacci/rustflow-group/12` | 1.228 / 1.295 ms | 1.367 / 1.358 ms | +8.0% | 5.4% |
| `fibonacci/rustflow-group/15` | 2.699 / 2.791 ms | 3.314 / 3.174 ms | **+18.2%** | 4.4% |
| `fibonacci/rustflow-runtime/12` | 932.30 / 922.44 µs | 915.09 / 914.31 µs | −1.4% | 1.1% |
| `fibonacci/rustflow-runtime/15` | 1.466 / 1.427 ms | 1.445 / 1.425 ms | −0.8% | 2.8% |

Six changes are larger than either side's own spread and are therefore results
rather than noise: **−6.4% on `embarrassing/65536`**, −4.3% on both
`async_task-group` ids and −5.5% on `async_task/rustflow/4096`, against
**+26.3% on `linear_chain/1024`** and **+18.2% on `fibonacci-group/15`**
(+8.0% at `n = 12`, where one side's spread is 5.4% and the margin is thin).
`async_task/rustflow/65536` reads −6.9% but its own rounds disagree by 11.6%,
so it is not resolved -- it is the same id #96 and #119 both had to flag.

**The two shapes #102 predicted would pay did not pay.** `binary_tree` and
`graph_traversal` -- the shapes that push successors onto worker queues and
never touch a buffer -- do not move at either size, in any of their eight
cells, and the largest figure in them (−2.0%) sits inside its own side's
spread. The prediction in the #60 section, that a 48-of-72 victim space would
cost the shapes that steal from worker queues, is **measured and wrong**.

What pays instead is the shape with **one source task and nothing to steal**,
and it pays per *run* rather than per task: `linear_chain/1024` costs +26.3% of
146 µs, or about 39 µs a run, while `linear_chain/16384` -- the same scheduler,
the same single source, 16 times the tasks -- does not move, because the same
39 µs is 2% of 1.94 ms and this box cannot see 2%. `fibonacci-group` pays the
same way: a group's children go into the buffers, but only a handful are
outstanding at a time, so each one is a search of a nearly empty space.

So the regime that decides the sign is **how deep the buffers are when a thief
arrives**, not which queue the shape's successors go to. 65536 sources dealt
round-robin leave every one of 48 buffers non-empty, so the first attempt
succeeds whatever the count and all the extra buffers do is spread the
producer's and the consumers' writes over more cache lines -- which is the 4 to
6% win. One source, or 986 children of a recursion, leave 47 of 48 empty, and
then the count is pure search cost.

### One worker: 1 buffer against 2

The same rule at the other end of the sweep. `bit_width(1)` is 1, so A has a
single buffer and B has two, and at one worker *every* candidate victim is a
buffer either way -- so these rows carry no victim dilution at all, only the
cost or benefit of a second injector.

| id | 1 buffer, r1 / r2 | 2 buffers, r1 / r2 | change | widest spread in one side |
|---|---|---|---|---|
| `linear_chain/rustflow/1024` | 196.76 / 132.24 µs | 130.89 / 146.79 µs | not resolved | 48.8% |
| `linear_chain/rustflow/16384` | 1.950 / 1.943 ms | 1.944 / 1.945 ms | −0.1% | 0.4% |
| `binary_tree/rustflow/4096` | 645.13 / 590.24 µs | 674.63 / 674.28 µs | +9.2% | 9.3% |
| `binary_tree/rustflow/65536` | 21.423 / 9.766 ms | 9.888 / 9.777 ms | not resolved | 119.4% |
| `graph_traversal/rustflow/3476` | 796.26 / 792.94 µs | 779.71 / 793.65 µs | −1.0% | 1.8% |
| `graph_traversal/rustflow/56759` | 12.267 / 12.870 ms | 12.893 / 13.028 ms | +3.1% | 4.9% |
| `embarrassing/rustflow/4096` | 2.835 / 2.792 ms | 2.699 / 2.812 ms | −2.0% | 4.2% |
| `embarrassing/rustflow/65536` | 44.558 / 44.946 ms | 44.772 / 45.181 ms | +0.5% | 0.9% |
| `async_task/rustflow/4096` | 1.936 / 1.885 ms | 1.947 / 1.998 ms | +3.2% | 2.7% |
| `async_task/rustflow/65536` | 30.884 / 31.258 ms | 29.379 / 29.533 ms | **−5.2%** | 1.2% |
| `async_task/rustflow-group/4096` | 784.06 / 776.43 µs | 803.24 / 802.65 µs | **+2.9%** | 1.0% |
| `async_task/rustflow-group/65536` | 19.132 / 19.273 ms | 20.038 / 19.777 ms | **+3.7%** | 1.3% |
| `fibonacci/rustflow-group/12` | 82.38 / 84.21 µs | 80.83 / 79.80 µs | −3.6% | 2.2% |
| `fibonacci/rustflow-group/15` | 335.54 / 334.63 µs | 323.90 / 325.52 µs | **−3.1%** | 0.5% |
| `fibonacci/rustflow-runtime/12` | 48.56 / 48.54 µs | 48.34 / 48.55 µs | −0.2% | 0.4% |
| `fibonacci/rustflow-runtime/15` | 188.64 / 188.09 µs | 189.11 / 188.86 µs | +0.3% | 0.3% |

Four of those are resolved and they disagree in sign, which is the regime split
again and nothing else: the **externally** submitted `async_task` id gains 5.2%
from a second buffer, because its producer is a different thread from its one
consumer and two injectors give them a line each, while the **worker**-
submitted `async_task-group` ids *lose* 2.9 to 3.7%, because there the producer
and the consumer are the same thread and the second buffer only has to be
searched. Two ids are unresolvable on this box and are left that way:
`linear_chain/1w/1024`, which the #40 table also had to drop, and
`binary_tree/1w/65536`, whose first process of the shape read 21.4 ms against
9.77 ms in its other three -- a 2.2x first-touch artefact, not a sizing effect.
`binary_tree/1w/4096`'s +9.2% is inside its own 9.3% spread.

### 12 buffers: a third of the win, most of the cost

The obvious compromise, measured the same way on the seven 24-worker ids that
moved at all:

| id | 5 buffers, r1 / r2 | 12 buffers, r1 / r2 | change | widest spread in one side |
|---|---|---|---|---|
| `linear_chain/rustflow/1024` | 137.16 / 132.71 µs | 163.99 / 163.18 µs | **+21.2%** | 3.4% |
| `embarrassing/rustflow/4096` | 886.95 / 864.62 µs | 871.48 / 875.62 µs | −0.3% | 2.6% |
| `embarrassing/rustflow/65536` | 12.214 / 12.264 ms | 12.014 / 11.891 ms | **−2.3%** | 1.0% |
| `async_task/rustflow/4096` | 5.330 / 4.874 ms | 4.997 / 4.880 ms | −3.2% | 9.4% |
| `async_task/rustflow-group/65536` | 74.550 / 77.242 ms | 74.243 / 76.023 ms | −1.0% | 3.6% |
| `fibonacci/rustflow-group/12` | 1.204 / 1.215 ms | 1.223 / 1.244 ms | +2.0% | 1.7% |
| `fibonacci/rustflow-group/15` | 2.607 / 2.536 ms | 2.410 / 2.406 ms | **−6.4%** | 2.8% |

The 5-buffer column is this round-set's own, measured minutes after the one
above, which is why it does not match cell for cell: as everywhere in this
file, the comparison is within a set and never across two.

`linear_chain/1024` loses 21.2% at 12 buffers against 26.3% at 48, so **nearly
the whole cost arrives by 12**, while `embarrassing/65536` gets −2.3% of the
−6.4% that 48 is worth. There is no count between 5 and 48 that is a free win:
the cost comes in first. (`fibonacci-group/15` reads −6.4% here against +18.2%
at 48, which is not monotone and is the one figure in this section with no
mechanism offered for it; both sides' rounds are tight, so it is recorded as
measured rather than averaged away.)

### The steal loop, counted

The number the #60 section said would get worse -- its 1.01 to 1.02 buffer
attempts per job -- measured directly, with temporary counters in
`explore_task` splitting its outcomes by the kind of queue drawn. One
instrumented run per sizing: 100 runs of a 1024-task chain and 4 runs of a
65536-task flat graph, both at 24 workers. The counters are atomics inside the
steal loop, so these are counts and emphatically not timings.

The chain, per run:

| buffers | successful steals | empty buffer draws | empty worker-queue draws |
|---|---|---|---|
| 5 (shipped) | 1.00 | **0.08** | 0.58 |
| 12 | 1.00 | 13.6 | 25.1 |
| 48 | 1.00 | **51.5** | 25.0 |

The flat graph, per job:

| buffers | buffer attempts, successful + empty | empty worker-queue draws |
|---|---|---|
| 5 (shipped) | **1.012** | 0.057 |
| 12 | 1.405 | 0.776 |
| 48 | **1.082** | 0.039 |

- **A chain needs exactly one steal per run** at every sizing -- the source
  task -- because #62's continuation keeps every one of the other 1023 links
  on the worker that readied it. So the +26.3% above buys nothing whatsoever:
  it is the cost of *finding* that one job. The wasted draws that find nothing
  go from 0.66 a run at 5 buffers to 38.7 at 12 and 76.5 at 48, a **116x
  rise** where the draw's dilution predicts 2.5x (71 candidates against 28 for
  one item). Whatever the remaining factor of 46 is, it is not the victim
  draw's probability -- which is what stops "size the two spaces apart" from
  being the answer to this. The likeliest explanation is one this run did not
  measure and so does not claim: at 5 buffers the pool's 24 sticky victims can
  cover every buffer, and past 24 they cannot, so a search that used to end on
  the first attempt starts running for dozens.
- **The park path is not involved.** A fourth counter on the
  `Buffers::first_nonempty` sweep in `wait_for_task` -- the one O(buffers)
  scan on the sleep path, and the obvious suspect -- reads 0.00 sweeps per
  chain run at 5 buffers and 0.04 at 48: between runs a worker parks through
  `wait_for_task`'s `num_topologies() == 0` fast path, which skips the sweep
  entirely. A negative result worth keeping, because it is where the next
  reader would look first.
- **The flat graph reproduces #60 exactly and then barely moves**: 1.012
  buffer attempts per job at the shipped count, against the 1.01 to 1.02 that
  section reported, and 1.082 at 48. That is the other half of the regime
  split -- with 65536 items dealt over 48 buffers the first attempt still
  succeeds, so the count costs the search nothing and the cache-line spread is
  free money. The 12-buffer row of that table (1.405, and 0.776 worker draws
  against 0.039 at 48) does not fit the pattern its neighbours make on either
  side; it comes from a single 4-run instrumented pass, and it is reported as
  measured rather than explained.

### The decision

**`bit_width(num_workers)` stays**, and `src/executor/queue.rs` names this
issue so the 10% is not rediscovered as free. Upstream's "empirically, log2(N)
performs best" turns out to hold for this port too, for a reason upstream does
not state and this run can: the count trades a bulk submitter's cache-line
spread against every thief's search cost in a sparse space, and the shapes in
this file want opposite ends of that trade. What the one shape that benefits
would need is not more buffers: it is not to materialise 65536 jobs and hand
each one across a core boundary, which the end of the "#60 finding" section
argues is a milestone rather than a patch.

## The #125 attribution: an external async task is ~440 ns, and 87% of it is the node it allocates

`async_task`'s `rustflow` id is the external submission path with no graph in
it, and the table above left its per-task cost unexplained: ~440 ns at one
worker, against the ~100 ns the "#60 finding" section measured for a job of an
externally submitted *graph* on the same path. #125 is where that was taken
apart, against the four candidates
its text listed. The answer is the first of them -- **the allocation** -- but
not for the reason "one allocation per task" suggests: a free async task costs
**three** heap blocks and 848 bytes, the producer allocates them and a *worker*
frees them, and two thirds of what they cost is that split rather than the
allocating.

**Three is two on today's `main`, and 848 bytes is 312.** #144 sized the
middle block to the one node it held and #376 took that block away
altogether; "The #382 measurement" at the end of this file counts what is left
by this section's own method and prices both changes, and every "three blocks"
below is this run's reading rather than the shipped one.

**No shipped code changed**, and the decision at the end of this section says
why. Everything below was measured with temporary counters and ablation
harnesses inside `src/executor/async.rs`, `src/executor/mod.rs` and
`src/executor/notifier.rs`; none of that instrumentation is in the shipped
code, and the counter runs and the timing runs are separate builds -- a build
carrying an atomic per notifier call is not a build to time. Because nothing
shipped changed, the six shapes' tables above are untouched and were not
re-run; the two `async_task` ids below were re-measured because the figure
this section attributes had to be the one `main` reads today.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id |
| Date | 2026-11-17 |
| **Load average** | **1.2 to 7.5, and all of it this run's own** |

The box was quiet, as it was for #119 and #102: no other agent was building or
testing on it, and the figure above is the decay of this run's own 24-worker
ids. The criterion ids below ran at a load of 2.1 to 2.4.

### The number, re-measured on today's `main`

Not taken from #96: `main` has had #119 (a runtime body's own queue) and #138
and #141 (`wait_for_all` woken by the last share of the storage hold, and the
debug assertion of that rule) since that table was written, and the second of
those reworked the very machinery candidate 2 names. Each id in its own
criterion process, the two ids back to back, the order reversed in round 2:

| id | round 1 | round 2 | per task |
|---|---|---|---|
| `async_task/rustflow/1w/65536` | 28.730 ms | 31.311 ms | **438 / 478 ns** |
| `async_task/rustflow-group/1w/65536` | 17.954 ms | 18.301 ms | 274 / 279 ns |

So the figure to attribute is unchanged, and **#138's rework did not move it**
-- which the counters below explain rather than leave to inference: the mutex
that rework is about is taken **once per run**, not once per task.

### Where the time is: the producer, not the worker and not the wait

The shape is `N` submissions and one `wait_for_all`, so the first cut is to
time the two apart. Eleven reps of the shipped path at one worker, medians,
two rounds:

| | round 1 | round 2 | share |
|---|---|---|---|
| submission loop | 427.3 ns/task | 418.1 ns/task | **95%** |
| `wait_for_all` | 19.6 ns/task | 21.5 ns/task | 5% |
| whole shape | 450.0 ns/task | 439.2 ns/task | |

That answers candidate 4 on its own -- **the wait is 5% of the shape** -- and
it says where the other 95% has to be looked for: the submitting thread's own
loop, with one worker draining behind it. The same producer loop with *nothing
consuming at all* -- the shipped build of it, pushing into a buffer nobody
steals from -- costs **140 to 153 ns a task**, so roughly two thirds of what
the producer pays it pays for having a consumer.

### The counters: the mutex and the notifier are not it

One instrumented run per worker count, 65536 external async tasks, counts and
emphatically not timings:

| counter | 1 worker | 24 workers |
|---|---|---|
| allocations / task | **3.016** | 3.016 |
| bytes allocated / task | **848** | 848 |
| `num_asyncs` reached zero | **1** per run | 2720 per run |
| `num_topologies` reached zero | **1** per run | 6853 per run |
| notifier poked | 65536 | 65536 |
| ... and found nobody waiting | **65535** | 60375 |
| ... and popped a parked waiter | 1 | 4803 |
| `commit_wait` (a worker parks) | **1** per run | 5026 per run |
| `thread::park()` actually called | **0** | 0 |

- **Candidate 2's mutex is taken once per 65536 tasks at one worker.** Both
  counters fall to zero exactly once -- at the end of the run -- because the
  producer is ahead of the worker the whole way. What a submission pays per
  task is four atomics and nothing else.
- **Candidate 3's notifier poke finds nobody 65535 times out of 65536.** The
  worker parks once, at the start, and then never again: it is never starved,
  so `bulk_schedule`'s `notify_n(1)` is a `SeqCst` fence and a load that
  returns. And `thread::park()` is never reached even at 24 workers, where
  4803 pops signal 4827 waiters between them: every one of those waiters is
  signalled between its own compare-exchange and its first load of the state
  word, so no worker on this shape ever reaches the blocking call -- and
  neither, therefore, does the notifier's `unpark`.
- **Three allocations per task, not one.** #86's cost model says "one
  allocation per async task, beyond its body", and the three are the
  `Arc<AsyncNode>` (88 B), the arena's `Vec<Option<Node>>` (**704 B**: one
  176-byte node in a `Vec` that grew to capacity 4) and the boxed body
  (56 B). The middle figure became 176 B with #144 -- its section below -- and
  then nothing at all with #376, which leaves the node's `Arc` at 256 B and
  the task at two blocks ("The #382 measurement"); #144's section also splits
  this bullet's 56-byte "boxed body" into the two things a per-size histogram
  shows it to be.

### The ablations: what each candidate costs

Each variant is the shipped submission path with one thing taken out of it, in
the shipped executor and against the real worker, timing the producer loop.
Eleven reps per variant per round, medians, and the **order of the variants is
rotated every rep** so none of them is always first; two rounds. Where a
variant takes the counters out, the batch holds one topology instead and
completion is noticed by the task body's own counter.

| variant | 1 worker, r1 / r2 | 24 workers, r1 / r2 |
|---|---|---|
| **A** the shipped path | 444.6 / 434.9 ns | 1132.3 / 1140.9 ns |
| **B** every node and job built before the timer | **52.5 / 51.4** | **224.5 / 226.2** |
| **C** no `increment_async`/`finish_async` | 436.4 / 417.6 | 1123.8 / 1177.5 |
| **D** notifier poked every 256 tasks | 441.5 / 424.6 | 1044.1 / 1060.9 |
| **E** B and C together | 42.2 / 41.7 | 201.0 / 198.8 |
| **F** the build recycled on the producer itself | 171.9 / 169.4 | 543.0 / 543.2 |
| **G** A plus one more 64-byte allocation | 519.6 / 503.9 | 1823.0 / 1878.3 |
| **H** A plus one more 704-byte allocation | 669.9 / 659.1 | 2053.8 / 2151.1 |

Read as differences, at one worker:

- **A − B = 384 to 392 ns: the node and the job.** Building them is 87% of the
  submission loop. Nothing else in the path is within an order of magnitude of
  it, and every other candidate has to be read against B's 52 ns.
- **A − C = 8 to 17 ns: the two counters.** At or below the 2 to 5% a side's
  own rounds disagree by, which is this file's test for a result. Candidate 2
  is four atomics and one mutex per run.
- **A − D = 3 to 10 ns: the notifier.** Likewise inside the spread at one
  worker, and the counter table says why. At 24 workers it is 80 to 88 ns
  (7%), which is the one place candidate 3 shows up at all.
- **E = 42 ns: the handoff.** A push into a spill buffer, the notifier poke,
  the worker stealing it back out and freeing the node. The same harness's
  bare handoff -- plain `usize`s pushed to one thief, no job and no node -- is
  34 to 35 ns, which is #60's experiment at one consumer rather than 24, so
  the executor adds almost nothing to the cost of moving an item across a core
  boundary.
- **F − E = 128 to 130 ns: the build when the producer frees its own
  memory**, against 384 to 392 when the worker does. **A − F = 266 to 273 ns
  is therefore the cost of the split**, not of the work: in F the producer
  allocates, runs and frees each node itself and pushes a prebuilt job
  instead, so the worker's half of the regime is held exactly the same and
  only the allocator's is different. Build it all on one thread with the
  memory recycled straight back and the whole task -- node, job, body and
  retirement -- costs **62 to 65 ns**.
- **G − A = 69 to 75 ns per extra allocation.** One more 64-byte block,
  allocated by the producer and freed by the worker, costs that much on its
  own; three of them is the 266 to 273 ns above, which is the attribution
  closing on itself from the other side. At 24 workers that single block costs
  **691 to 737 ns** and the whole build goes from 388 ns to 911, which is
  three quarters of why this shape is 2.6x worse at 24 workers than at one:
  #96's "more workers make every variant worse" is, on this shape, the
  allocator and not the scheduler.
- **H − A = 224 to 225 ns** for a 704-byte block against G's 69 to 75 for a
  64-byte one -- but H's box is zero-initialised, so what the difference buys
  is 11 cache lines written and not a larger chunk. The A/B below is what
  separates those two.

### Sizing the arena to its one node is worth 3%, and it is not this issue's change

The 704-byte allocation is the arena of a one-node graph: `Vec`'s first push
takes capacity 4, so three quarters of the largest of the three blocks is
never used. Measured as a two-build A/B -- a temporary `Graph` constructor
sized to exactly one node, the two binaries run **A B B A** so neither side is
systematically warmer:

| | round 1 | round 2 | spread within one side |
|---|---|---|---|
| shipped (capacity 4) | 427.6 ns/task | 435.7 ns/task | 1.9% |
| sized to one node | **412.3 ns/task** | **421.2 ns/task** | 2.2% |

**−3.3 to −3.6%**, which is just outside either side's own spread and therefore a
result -- and a small one, because the bytes it saves were never touched. It
is the *count* of blocks that costs, not their size: that is what G against H
looked like it contradicted and what this A/B settles. The change lives in
`src/graph.rs` and `src/executor/async_node.rs`, is out of #125's scope, and
is left for an issue of its own rather than smuggled in here.

That issue is #144 and the last section of this file is its measurement. The
direction holds and the size does not: re-measured against the same id with
more rounds, the change is **−6%**, and on the `rustflow-group` id -- the same
construction submitted from a worker, which this run never A/B'd -- it is
−34%. So "the bytes it saves were never touched" is too strong a reading of
G against H: shrinking this block from 704 bytes to 176 buys about 40% of what
G measured a whole extra 64-byte block to cost in the same regime, and rather
more than one whole block in the regime where a worker submits. A block's size
is worth less than its existence, which is what this paragraph has right; how
much less is what the section below measures instead of inferring.

### A method note: a stand-in consumer is not a worker

The first version of this harness replaced the executor with one producer
thread and one thief spinning on the spill buffer, and staged the path up from
a bare handoff. It had to be abandoned: with the real build in front of it the
same producer loop read **545 to 832 ns** a task against the shipped path's
445, and the two rounds of a stage disagreed by up to 2x. A thief that polls
an empty deque in a tight loop keeps the deque's control words in its own
cache, so every push the producer makes pays a coherence miss -- where a real
worker, which runs a task body between steals and parks exactly once per run,
costs the producer about half that. Every ablation above is therefore measured
in the shipped executor against the real worker, with only the submission path
varied. The one figure of that harness quoted above is its bare handoff of
plain `usize`s (34 to 35 ns a task), which has no build in front of it and so
never starves its thief; the stages that did are not reported, because what
they measure is the stand-in.

### The decision: declined, and this is the cost of the design

**Nothing in this path is worth changing**, and the three candidates that
could be changed from `executor::async` are each inside the noise:

- The two counters are 2 to 4% of the shape at one worker and nothing at 24.
  Folding them into one word would buy half of that, and would entangle the
  two waiters that `executor::async` keeps apart on purpose and that #138 and
  #141 have just finished hardening. Declined: a 1% win is not worth a
  correctness surface that has already produced four race fixes (#109, #112,
  #136, #138).
- The notifier poke is 1 to 2% at one worker and 7% at 24, and it cannot be
  skipped when no worker is waiting: the `is_empty` check that would skip it
  has to follow the `SeqCst` fence, which is the expensive half, or the Dekker
  pair that makes a wake-up unlosable is broken. #60 declined to wake workers
  *earlier* on this path and this declines to wake them less often, for the
  same reason in reverse.
- The wait is 5%, and `wait_for_all` is already a condvar woken once.

What is left is **the three heap blocks a free async task is**, 87% of the
submission loop -- two of them on today's `main`, and "The #382 measurement"
prices the one that went at 7 to 16% of the id at one worker, which over a
344 to 440 ns task is the 69 to 75 ns the G ablation above priced a block at,
measured this time as a shipped change rather than as an added block. They are
a property of how a node that belongs to no taskflow is represented here: `executor::async_node` gives such a task its own
one-node `Graph` inside an `Arc`, so that every piece of the scheduler which
resolves a node by index into a graph resolves this one too, and the body is
boxed because it is type-erased. That decision is #86's and its module docs
argue it; what this run adds is its price, which that module could not have
known: **one allocation per task costs 69 to 75 ns at one worker and 691 to
737 ns at 24 when the producer allocates and a worker frees**, so three of them
is the shape.

Upstream pays this differently rather than not paying it. Its async node is
one `Node` out of `animate` -- a plain `new Node` in a default build, so the
same producer-allocates/worker-frees regime over **one** block instead of
three -- and `TF_ENABLE_TASK_POOL` replaces that with a sharded
`ObjectPool<Node>` whose hot path is a lock-free stack of recycled blocks, so
a freed node goes back to a shard instead of to the allocator -- which is
exactly the defence against the regime measured above. Neither of those is
reachable without changing what a free node *is* in this port, so the
reducible shape here is "fewer blocks per async task", which is a design
change to #86 and a milestone rather than a patch -- the same place the end of
the "#60 finding" section arrived at for the flat graph, and for the same kind
of reason. That milestone is M12: #376 took the arena away and #379 gave a
worker a free list of finished nodes, and "The #382 measurement" is where the
two are priced -- the first worth 7 to 16% a task, the second worth nothing
this box can see. **That is the recorded cost of the architecture, with the ablations
above as the evidence.**

## The #144 change: a free node's arena is sized to its one node, and that is 6% of the submission path

#125's section above ends with one change it named and could not make: a free
async node owns a `Graph` of exactly one node, and `Graph::new` left that
arena to the `Vec`'s own first capacity -- four slots of 176 bytes, three of
which nothing will ever fill. This is that change, which is a crate-private
`Graph::with_capacity` and one call site. It is the first of these six runs to
move a number the shipped code reads.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id |
| Date | 2026-11-18 |
| **Load average** | **1.7 to 3.4, and not this run's own** |

**The box was not quiet, and the load is not the interesting part of it**:
another agent's test binary held one core at 100% for the whole run, and a
sibling agent's `cargo` builds came and went between ids. That is a steady
competitor for one of 24 threads rather than a varying one for all of them,
which is why the one-worker ids below are pinned.

### Method

Two prebuilt criterion binaries -- one from `main`, one from this branch --
run as **blocks of four processes, `A B B A`, with the order of the block
swapped on alternate blocks**, each process its own criterion invocation with
its own `CRITERION_HOME`, two to three blocks an id. Round 1 is the first half
of every block and round 2 the second, so each round holds both orders and
each cell is a median of two or three point estimates with the spread within
that one side reported beside it. #60's rule, which this file has recorded
three times as not optional: a fixed order on this shape has read the sign of
a change wrong.

The one-worker ids are run under `taskset -c 4,5` -- two physical cores of one
CCD, the producer and the one worker -- which is what makes the shape
reproducible with a core of the box occupied: unpinned, the same id's four
processes read 29 to 50 ms where pinned they read 29 to 31. The headline id is
reported **both** ways, and the two agree; the 24-worker id cannot be isolated
that way and is reported with the caveat it earns. The counters below are a
third build, with a counting global allocator in an example that is not part
of this branch -- an instrumented build is not a build to time, and
instrumentation does not ship.

### The counters: three blocks still, 320 bytes instead of 848

One instrumented run per side per worker count, 65536 external `silent_async`s
after a warm-up run of the same shape, counted by block size:

| block | shipped | this branch |
|---|---|---|
| the body's box (a closure capturing one `Arc`) | 8 B x 1.0000 | 8 B x 1.0000 |
| `Arc<AsyncNode>` | 88 B x 1.0000 | 88 B x 1.0000 |
| the arena's `Vec<Option<Node>>` | **704 B** x 1.0000 | **176 B** x 1.0000 |
| the spill buffers growing | 3032 B x 0.0159 | 3032 B x 0.0159 |
| **per task** | **3.0159 blocks, 848.12 B** | **3.0159 blocks, 320.12 B** |

Identical at 1 worker and at 24, and the shipped column reproduces #125's
848 bytes and 3.016 blocks exactly -- which is also what corrects that run's
breakdown of them. Its 56-byte "boxed body" is two things: the body's box is
**8 bytes** here (the benchmark's closure captures one `Arc`, so the box is
one pointer), and the other 48 bytes a task are its share of the 1040
reallocations the shared spill buffers make over the run. Neither is the body
being large; #125 had the residual and not the histogram.

**Three blocks, not two.** The arena cannot share the `Arc<AsyncNode>`'s
block, and `DESIGN.md`'s entry on the `Arc`-owned free node now says why
rather than leaving the next reader to re-derive it: a `Graph` holds its nodes
in a `Vec`, so collapsing the two would take either a graph that can hold one
node inline or an async path that does not resolve a job through a `&Graph` at
all. What is reachable *here* is the size of the second block, which is this
change; #376 then took the second of those two alternatives, and "The #382
measurement" at the end of this file counts what a free async task costs after
it -- **2.0159 blocks and 312.12 bytes** -- by the method this section's
counters use.

### The headline id, both ways

`async_task/rustflow/1w/65536`, medians of each round, per task in brackets:

| protocol | round | shipped | this branch | delta | spread within one side |
|---|---|---|---|---|---|
| unpinned, 3 blocks | 1 | 29.059 ms (443 ns) | **27.280 ms (416 ns)** | **−6.1%** | 5.4% / 4.8% |
| unpinned, 3 blocks | 2 | 29.477 ms (450 ns) | **27.423 ms (418 ns)** | **−7.0%** | 2.6% / 1.0% |
| `taskset -c 4,5`, 3 blocks | 1 | 29.344 ms | **27.956 ms** | −4.7% | 0.5% / 1.5% |
| `taskset -c 4,5`, 3 blocks | 2 | 31.028 ms | **27.718 ms** | −10.7% | 8.7% / 1.2% |
| `taskset -c 4,5`, 2 blocks | 1 | 29.588 ms | **27.474 ms** | −7.2% | 0.2% / 1.5% |
| `taskset -c 4,5`, 2 blocks | 2 | 30.855 ms | **27.295 ms** | −11.5% | 10.5% / 0.1% |

**#125's −3.3 to −3.6% is confirmed in direction and corrected in size: this
id is −6 to −7% on the protocol with the tightest sides**, and the pinned
rounds read −4.7 to −11.5% because the *shipped* side throws occasional 31 to
32 ms processes where this branch's ten processes never leave the 27.3 to
28.0 ms band. That asymmetry is the result underneath the percentages: across
every protocol above, the highest changed reading (28.0 ms) is below the
lowest shipped one (29.0 ms), which no single pair of rounds would establish.

### The other async ids, and where it is worth most

| id | protocol | round 1 | round 2 | spread within one side |
|---|---|---|---|---|
| `async_task/rustflow/1w/4096` | pinned | −12.0% | −8.9% | 0.0% to 2.1% |
| `async_task/rustflow-group/1w/65536` | pinned, 3 blocks | **−34.3%** | **−34.6%** | 0.1% to 10.3% |
| `async_task/rustflow-group/1w/65536` | pinned, 2 blocks | **−33.8%** | **−33.7%** | 0.0% to 0.4% |
| `async_task/rustflow/24w/65536` | unpinned | −1.1% | −8.5% | 1.3% to 3.3% |

The `rustflow-group` id is where this change is worth most, and it is worth
*five times* what the external id gains: 18.0 ms to 11.9 ms, **274 ns a task
to 182**, with both sides inside 0.4% of themselves on the second run. That id
submits the same construction from a worker, so one thread allocates the node
and -- usually -- the same thread frees it, which is #125's variant F regime;
a block that fits a smaller size class is worth more there than in the regime
where a producer allocates and a worker frees, because there is a free list to
come back to. It supersedes the `async_task` table's "submitting from a worker
saves 30%" bullet, which is now 56% at 65536 and one worker.

At **24 workers the id is not resolved**: −1.1% is inside the sides' own 1.3
to 3.3%, −8.5% is well outside it, and 24 workers on a box with a core already
taken is exactly the measurement this run cannot make. The direction is never
positive; the size is not a result.

### All six shapes, which is the regression check

`Graph`'s arena is on every shape's path, so every shape was A/B'd and not
just the one the change is for. One worker, pinned, the larger of each shape's
two sizes:

| shape | id | round 1 | round 2 | spread within one side |
|---|---|---|---|---|
| `linear_chain` | `rustflow/1w/16384` | +0.4% | +0.4% | 0.3% to 1.7% |
| `binary_tree` | `rustflow/1w/65536` | +1.7% / +1.1% | +0.6% / +0.2% | 0.3% to 2.4% |
| `graph_traversal` | `rustflow/1w/56759` | −0.0% | −0.1% | 0.1% to 0.4% |
| `embarrassing_parallelism` | `rustflow/1w/65536` | −0.1% | −0.2% | 0.0% to 0.5% |
| `async_task` | `rustflow/1w/65536` | −6.1% | −7.0% | 1.0% to 5.4% |
| `fibonacci` | `rustflow-group/1w/15` | −4.4% | −4.5% | 0.1% to 1.3% |
| `fibonacci` | `rustflow-runtime/1w/15` | −1.1% | −2.9% | 0.0% to 2.1% |

- **The four graph shapes do not move**, which is what the code says they
  cannot: a graph a `FlowBuilder` fills still goes through `Graph::new`, whose
  `Vec` is untouched, and `with_capacity` is crate-private with one caller.
  `graph_traversal` and `embarrassing_parallelism` read within a fifth of a
  percent either way. The two that read positive are `linear_chain` at +0.4%
  twice and `binary_tree`, which was therefore run a second time: four point
  estimates between +0.2% and +1.7%, against the 0.3 to 2.4% its own sides
  disagree by. Both are at the edge of what this box resolves and neither has
  a mechanism -- nothing on either path goes near the arena's sizing -- so
  what is left for them is code layout.
- **`fibonacci` gains too, and that is not incidental**: both of its `rustflow`
  ids are recursive `spawn`s, so every node of the recursion is a free node
  and pays for its own arena. The `rustflow-group` id's −4.4 / −4.5% against
  sides inside 1.3% of themselves is a result; the `rustflow-runtime` id's −1.1
  / −2.9% straddles its own spread and is not.

So the trade to report is that there is none to report: the two shapes that
allocate a free node per task gain 1 to 34%, and the four that build a graph
read between −0.2% and +1.7% -- inside, or within a few tenths of a percent
of, the spread of their own sides, on paths the change does not touch.

## The #167 note: observer seams cost one relaxed load with none registered

#167 gave the executor's two scheduling seams, `observer_prologue` and
`observer_epilogue`, bodies: an `ObserverList::is_empty` check -- one relaxed
load of an `AtomicUsize`, no lock and no allocation -- around every dispatch,
taken on every seam whether or not anything is registered. This is not the
case for the heavier A/B protocol the sections above use: the claim is
structural (the branch a reader can see in `src/executor/worker.rs`, and the
one-row measurement below with no observer ever registered), not a result
repeated runs would sharpen.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id |
| Date | 2026-11-18 |

One criterion run per side, `linear_chain/rustflow/24w/16384`, no observer
registered either side:

| | load average | time |
|---|---|---|
| before (`main`) | 3.16 | 1.9200 ms – 1.9480 ms |
| after (this branch) | 10.84 | 2.0959 ms – 2.1208 ms |

Criterion itself reports the difference as "+7.9% to +9.3%, regressed" --
but the load average more than tripled between the two runs, three sibling
agents' `cargo` builds having started on this box in between, which is
exactly the kind of swing "The machine, and what it was doing" above says to
read as noise rather than as a result: the `before` row of that section's own
`linear_chain/rustflow/1w` id moved by a similar amount between two runs an
hour apart at a load average of 1.4 versus 7.9. A single relaxed atomic load
added to a dispatch that was already reading several others does not have a
mechanism to cost 9% of a 2 ms run, and this file's standing rule is not to
chase a number by re-running until the load looks better -- so both numbers
are reported as measured, with the caveat that earns them.

## sort

`N` random `f64`s, sorted once: upstream's `measure_time_taskflow` builds a
fresh `tf::Taskflow`, calls `taskflow.sort(vec.begin(), vec.end())` and runs
it, timing both -- "ours includes building the graph" from this file's own
header applies here too. `rayon::par_sort_unstable_by` is the baseline, in
the role TBB's `parallel_sort` plays upstream. Sizes are two points off
upstream's `N = 10..=1e8` sweep: one inside `src/algorithm/sort.rs`'s own
cutoff for `f64` (`4096`, under the `< 16` byte bucket), one two orders of
magnitude past it.

Unlike the six shapes above, this one mutates its input in place, so a plain
`Bencher::iter` would sort already-sorted data on every sample after the
first. Each iteration here is `Bencher::iter_batched_ref` instead, whose
untimed setup re-shuffles the one buffer the id owns -- `benches/sort.rs`'s
own module docs say so, and it is why this table carries no
"construction-only" column the way `embarrassing_parallelism`'s does: the
untimed setup already isolates the one cost this shape has that rayon's does
not. That buffer is allocated before anything is timed, where upstream's
file-level global lives; the "#366 change" section at the end of this file is
why.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id |
| Date | 2026-11-19 |
| **Load average** | **0.21 rising to 4.13 over the run, all of it this run's own** |

The figures below were re-taken in the "#399 change" pass at the end of this
file, on a quiet box: no sibling agent and nothing else building. They
replace the "#381 measurement" pass's set, which read 15.69 µs, 17.34 µs,
21.11 ms and 15.01 ms on the same four ids and predates the comparator's call
shape changing, and that in turn replaced the #366 pass's.

| size | workers | rustflow | rayon | upstream C++ |
|---|---|---|---|---|
| 1000 | 1 | 15.70 µs ±0.5% | 11.88 µs ±0.4% | not published |
| 1000 | 24 | 17.56 µs ±0.3% | 15.96 µs ±1.4% | not published |
| 1000000 | 1 | 21.07 ms ±0.1% | 23.99 ms ±0.1% | not published |
| 1000000 | 24 | 13.32 ms ±11.9% | 3.386 ms ±0.5% | not published |

**`rustflow/24w/1000000` is the one id of this shape too wide to read as a
point**: ±11.9%, where its baseline beside it is ±0.5%. It is quoted rather
than dropped and rather than re-run -- one run per id is the rule every pass
here is measured under -- but it means "about 13 ms" and nothing finer, and
the "#381 measurement"'s own three interleaved rounds put its round-to-round
spread at 15.3%, which is wider than the difference between this reading and
the 15.01 ms of the pass before it.

At 1000 elements -- below this type's parallel cutoff on every worker count --
both columns take their serial path, so the gap is construction and dispatch
rather than sorting: our run builds a `Taskflow`, hands it to an `Executor`
and tears both down, where rayon's `install` has no graph to build at all.
24 workers cost both columns a little at this size, for the reason
`linear_chain`'s own table gives -- there is nothing here for the other 23 to
do.

**The one-worker million-element row is the port ahead of its baseline, and
neither column's sort is this crate's**: `Runtime::sort_by` short-circuits to
`[T]::sort_unstable_by` whenever the pool has one worker or the range is at or
under `parallel_sort_cutoff::<T>()`, which for `f64` is 4096, so three of this
table's four `rustflow` ids measure the standard library's pdqsort with a
taskflow built around it. 21.07 ms against `par_sort_unstable_by`'s 23.99 ms
on one thread is those two sorts. The "#381 measurement" below found this id
swinging by 1.7x on the *call shape* of that short-circuit -- the comparator
reached the sort as a `&C`, whose instantiation optimises in some calling
crates and not others -- and the "#399 change" at the end of this file is that
line fixed: the comparator is passed by value now, the four caller binaries
measured there agree to 1.03x where they had disagreed by 1.86x, and the row
is readable as "the two libraries' serial sorts" rather than as a figure that
depends on the caller.

At 1000000 elements and 24 workers the parallel path runs on both sides and
rayon is 3.9x ahead, the figure as wide as the id it comes from. **What that
row is about is scaling and not the partition**: this column goes 21.07 ms to
13.32 ms over 24 workers, 1.58x, where rayon goes 23.99 ms to 3.386 ms, 7.1x.
Every spawned half here goes through this crate's own node machinery --
`Scope::spawn_runtime`, a fresh arena, a join -- where rayon's split is a
`join` of two closures with one atomic, which is the gap `binary_tree`'s own
table already found for a plain increment. The partition itself is no longer a
candidate: #375 replaced the single-pivot three-way split this section used to
blame with upstream's live `parallel_pdqsort` core, and the A/B in the "#381
measurement" reads this id at 1.035x across that change, inside its own
15.3% round-to-round spread.

## pipeline

A chain of `P = 4` pipes, serial first then alternating serial/parallel
(`S, P, S, P`), over `L` lines passing `N` tokens -- upstream's
`linear_pipeline`, which sweeps its own default eight-pipe, all-serial chain
over a token count it confusingly also spells `L`. `benches/pipeline.rs`
keeps `L` for lines and `N` for tokens instead. There is no rayon baseline:
rayon has no pipeline primitive, so the comparison is against `P` OS threads
chained by `std::sync::mpsc`, in the role TBB's `tbb::parallel_pipeline`
plays upstream's own `tbb.cpp`. Both sides pay the same per-token cost --
the 32-iteration floating-point body `embarrassing_parallelism.rs` and
`graph_traversal.rs` use, in place of upstream's 10 microsecond `sleep_for`,
so the suite finishes in seconds rather than minutes once `L` and the worker
count multiply it -- and the same number of hops, `P` pipes against `P`
channel sends.

**What the two sides do not share is a `lines`/`workers` axis on the mpsc
side.** `L` is a knob the `Pipeline` has and the mpsc chain does not: a
`PipeType::Parallel` pipe may run several lines' tokens through it at once,
up to `L`, while an mpsc stage is always exactly one `std::thread`,
whatever `PipeType` the pipe it stands in for would have been. So `mpsc` is
measured once per size below, not once per `(lines, workers)` row, and the
same figure is read against every row of that size -- `benches/pipeline.rs`'s
own module docs say so again next to the code.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id |
| Date | 2026-11-18 |
| **Load average** | **49.2 rising to 58.7 over the run** |

**This box was not quiet.** Three sibling agents' own `cargo` builds and test
suites were running throughout, at a load average several times anything
else recorded in this file -- "The machine, and what it was doing" above puts
the absolute figures at ±25% on a load of 7 to 11; a load in the 50s is
outside anything this file has had to caveat before, so read the absolute
numbers as indicative at best. What that section's own rule still protects is
the comparison *within* a row: every `mpsc` and `rustflow` id below came from
one criterion process, ids seconds to minutes apart, so a row's own ratio is
far more trustworthy than either of its figures on its own.

| lines | size | workers | rustflow | mpsc | upstream C++ |
|---|---|---|---|---|---|
| 1 | 1024 | 1 | 3.142 ms ±11.3% | 2.489 ms ±8.5% | not published |
| 1 | 1024 | 24 | 2.524 ms ±2.0% | 2.489 ms ±8.5% | not published |
| 1 | 16384 | 1 | 38.95 ms ±0.8% | 28.08 ms ±15.8% | not published |
| 1 | 16384 | 24 | 40.37 ms ±1.6% | 28.08 ms ±15.8% | not published |
| 4 | 1024 | 1 | 2.765 ms ±4.9% | 2.489 ms ±8.5% | not published |
| 4 | 1024 | 24 | 1.878 ms ±4.5% | 2.489 ms ±8.5% | not published |
| 4 | 16384 | 1 | 41.05 ms ±1.0% | 28.08 ms ±15.8% | not published |
| 4 | 16384 | 24 | 27.87 ms ±2.8% | 28.08 ms ±15.8% | not published |
| 8 | 1024 | 1 | 2.748 ms ±4.1% | 2.489 ms ±8.5% | not published |
| 8 | 1024 | 24 | 1.735 ms ±4.8% | 2.489 ms ±8.5% | not published |
| 8 | 16384 | 1 | 43.43 ms ±2.8% | 28.08 ms ±15.8% | not published |
| 8 | 16384 | 24 | 28.47 ms ±4.2% | 28.08 ms ±15.8% | not published |

**`L = 1` is the degenerate case the module docs promise to call out, and the
pipeline loses it plainly at the larger size.** With one line there is no
pipeline parallelism at all -- `run_line` advances one token through all four
pipes before the next token starts -- so `1l_1w/16384` and `1l_24w/16384` cost
39 and 44% more than the mpsc chain's 28.08 ms, which keeps four threads busy
regardless of `L`. At 1024 tokens the gap is present but smaller and, at 24
workers, inside the two ids' own noise (2.524 ms against 2.489 ms): the fixed
cost of building a `Pipeline` and composing it into a `Taskflow` is a bigger
share of a 1024-token run, which blurs the per-token comparison the larger
size makes plainly. Neither row is a bug in the comparison -- it is this
crate's per-pipe dispatch cost against a baseline with nothing else to do,
exactly as the module docs predict.

**At `L = 4` and `L = 8` with 24 workers, the pipeline catches up and passes
the baseline at the smaller size, and roughly ties it at the larger one.**
`4l_24w/1024` and `8l_24w/1024` are 1.878 ms and 1.735 ms against mpsc's
2.489 ms -- 25% and 30% faster -- because a `Parallel` pipe with idle workers
to hand lines to actually overlaps several tokens' work the way the mpsc
chain's one-thread-per-stage design cannot. At 16384 tokens the same two rows
read 27.87 ms and 28.47 ms against 28.08 ms, a tie inside the ids' own ±2.8%
and ±4.2%: the pipeline's per-token machinery (lock a line's `Mutex`, two
`fetch_sub`s, a possible `rt.schedule`) costs enough more than a channel send
that the extra concurrency only closes the gap rather than opening one in its
favour at this size.

**At one worker, more lines buy nothing**, which is the other side of the
same mechanism: one worker thread can run exactly one line task at a time no
matter how many lines the `Pipeline` was built with, so `4l_1w` and `8l_1w`
read within a few percent of `1l_1w` at both sizes (2.77 and 2.75 ms against
3.14 ms at 1024; 41.05 and 43.43 ms against 38.95 ms at 16384 -- the small
rise with more lines at this worker count is the extra `Mutex` and join-
counter traffic `L` adds without a second thread to spend it on). A
`PipeType::Parallel` pipe's extra concurrency is a property of the worker
pool, not of the pipe count or the line count on their own, and this row is
where that shows most directly.

## The #186 decision: a per-line slot's box, measured against a user-owned buffer

#186 gave every line of a `DataPipeline` a `Mutex<Option<Box<dyn Any + Send>>>`
slot -- 24 bytes on a 64-bit target -- so that a chain of typed `DataPipe`s
can move a value from one pipe's callable to the next without upstream's
`std::variant` over every pipe's own output type, which Rust has no anonymous
equivalent of. #188 is where that choice is measured: `benches/data_pipeline.rs`
times a `DataPipeline` against a `Pipeline` of the identical `S, P, S, P`
shape carrying the same `f64` through a user-owned buffer
(`tests/pipelines_compose.rs`'s own `Buffer`, narrowed to this shape) instead
of the typed slot.

**Where those ids live now.** That file also hosts upstream's own
`data_pipeline` shape, which is a different measurement with a section of its
own below. The two are separate criterion groups, and the ids this section
quotes are the `data_pipeline_slot` group's -- `typed/<lines>l_<workers>w` and
`buffer/<lines>l_<workers>w` -- unchanged in name and in what they measure.

**The allocation count is stated by inspection, not by a counting allocator.**
Implementing `GlobalAlloc` to count heap operations is `unsafe`, which is out
of scope for this issue; `src/pipeline/data_pipeline.rs`'s own
`DataPipelineBuilder::pipe` body is read instead. Every pipe invocation ends
by writing `Some(Box::new(output))` into its line's slot -- one allocation --
and every invocation but a chain's first opens by `take`ing and downcasting
the predecessor's box, which drops it once the value is moved out -- one
deallocation. So a chain of `P = 4` pipes costs **4 allocations and 4
deallocations a token**, independent of `lines` or `workers`: 3 of each
within one token's own lap through the chain, plus the allocation that frees
the previous token's last-pipe box when the next lap's first pipe overwrites
the line's slot. The `Pipeline` variant allocates nothing per token; its
buffer is built once, outside the measured loop.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id |
| Date | 2026-11-18 |
| **Load average** | **25.9 rising to 33.7 over the run** |

**This box was not quiet.** Sibling agents' own `cargo` builds and test
suites were running throughout -- "The machine, and what it was doing" above
puts the absolute figures at ±25% on a load of 7 to 11, and the `pipeline`
section above already had to call a load in the 50s "indicative at best"; a
load of 26 to 34 sits between those two and gets the same treatment: read the
absolute numbers as indicative, and the within-row ratio -- `typed` and
`buffer` of one row, measured seconds apart in the same process -- as the
figure worth trusting.

| lines | size | workers | typed (`DataPipeline`) | buffer (`Pipeline`) |
|---|---|---|---|---|
| 1 | 1024 | 1 | 2.507 ms | 2.456 ms |
| 1 | 16384 | 1 | 39.90 ms | 38.91 ms |
| 8 | 1024 | 1 | 2.600 ms | 2.545 ms |
| 8 | 16384 | 1 | 41.37 ms | 40.64 ms |
| 1 | 1024 | 24 | 2.695 ms | 2.561 ms |
| 1 | 16384 | 24 | 40.03 ms | 41.71 ms |
| 8 | 1024 | 24 | 1.978 ms | 1.844 ms |
| 8 | 16384 | 24 | 30.21 ms | 30.28 ms |

**The two single-worker, 16384-token rows are the cleanest read**, since at
one worker there is no cross-core traffic for the slot's extra allocation to
contend with: `1l_1w` reads +0.99 ms over 16384 tokens (+60 ns a token) and
`8l_1w` reads +0.73 ms (+45 ns a token), both in the direction the stated
allocation count predicts -- a `Box::new` and, for three of the four pipes, a
matching drop, that the buffer variant never pays. The two single-worker,
1024-token rows agree in sign and are in the same range once the smaller
token count is accounted for: +50 and +54 ns a token.

**The two 24-worker rows do not agree in sign**, and that is the load talking
rather than the slot: `1l_24w/16384` reads *typed faster* by 1.67 ms and
`8l_24w/16384` reads the two within 0.08 ms of each other, both well inside
what a load average above 25 does to a multi-worker id elsewhere in this
file. So the one claim this table supports is the single-worker one -- **a
per-line slot's `Box::new`-and-drop costs on the order of 45 to 60 ns a
token** on top of a 32-iteration floating-point pipe body, which is small
next to that body but is not nothing, and it is the allocator traffic the
stated 4-per-token count predicts rather than a number pulled from nowhere.
The 24-worker rows would need a quieter box to read past their own noise, and
are left as measured rather than as a resolved claim.

## black_scholes

European option pricing by the Black-Scholes formula, and **the one shape in
this file whose graph is a loop rather than a fan-out**. Upstream's
`bs_taskflow` builds three nodes -- a `placeholder`, a
`for_each_index(0, numOptions, 1, ..., StaticPartitioner())` over the
`BlkSchlsEqEuroNoDiv` kernel, and a condition task returning `0` to select
the loop again and `-1` to select nothing -- so one run of the taskflow
prices every option `NUM_RUNS` times through the *same* three nodes, and what
is measured on top of the arithmetic is the condition task re-dispatching the
`for_each_index` module that many times. Upstream couples the two counts:
`generate_options` sets `NUM_RUNS = numOptions`, so `N` options cost `N * N`
kernel evaluations and `N` laps, and `benches/black_scholes.rs` keeps the
coupling.

**What the loop costs is one condition task and one module dispatch a lap,
and what rayon has to do instead is a barrier.** The baseline is a `for` over
`NUM_RUNS` around `par_iter_mut` -- the loop once the graph is gone, and
exactly what upstream's own `tbb.cpp` writes with `tbb::parallel_for`. Both
sides run the same arithmetic the same number of times; a lap boundary ends a
rayon parallel iterator, which is one implicit barrier, where on our side it
is a condition task running, its `0` selecting the loop edge, the
`for_each_index` runtime task being scheduled again, its chunks being spawned
and joined, and only then the next lap. So this table is not a measure of the
kernel at all -- it is the price of a *re-dispatchable* loop against a
disposable one, which is what buys the graph the ability to decide at run time
whether to take another lap.

Sizes are two points off upstream's own sweep (`main.cpp` walks
`N = 1000..=10000` in steps of 1000), and with this shape the `N * N` coupling
is what makes two points enough: the per-lap cost grows as `N` while the work
it is amortised over grows as `N * N`, so `1000` is the point where the loop's
own overhead still shows -- 1000 laps of 1000 options is 42 options a worker a
lap at 24 workers -- and `4000` the point where the per-option arithmetic
dominates it. Upstream's last point, `N = 10000`, is `10^8` kernel evaluations
in a single criterion sample and is left off for that reason alone.

**The options are generated, not read from a file.** Upstream's `common.hpp`
calls its generator "Read input option data from file" and then draws every
field from `::rand()`, which is libc's and unseeded, so upstream's data is
not reproducible off its own machine. This port draws from the crate's own
`Xorshift32` with a fixed seed, in upstream's ranges shifted off zero: the
spot and strike prices are uniform on the integers `1..=200`, the volatility
and the time to expiry uniform on the hundredths `0.01..=1.00`, the risk-free
rate is upstream's fixed `0.1`, and an option is a put or a call with even
odds. The shift is what makes a price comparison possible at all -- zero is in
range for all four of upstream's draws, and each zero puts the kernel
somewhere it has no answer (`log(0)`, or a division by
`volatility * sqrt(time)`), which upstream never notices because its own
`ERR_CHK` compares against a `DGrefval` that is itself a random integer.

Two asymmetries to read the table against, both stated next to the code as
well. Ours includes building the graph, as every table in this file does --
though here it is three nodes built once against `N` laps of work, so it is
the smallest construction share in the file rather than the largest. And the
array the taskflow writes its prices into is a slice of `AtomicU32` holding
`f32::to_bits`, because a `for_each_index` body is an `Fn(usize)` and cannot
be handed the disjoint `&mut f32` that `par_iter_mut` hands the baseline: one
relaxed 32-bit store an option, against a kernel of two `exp`s, a `ln` and a
`sqrt`.

The correctness check is in the untimed part of the benchmark, before either
id is registered: both variants price the options once, and every price is
compared against a sequential evaluation of the same kernel on the same
options -- upstream's `-s,--seq` comparison, which upstream leaves commented
out in `main`. The tolerance is `1e-4` absolute, upstream's own `ERR_CHK`
threshold: both sides evaluate the same `f32` expression on the same inputs,
so the only difference either is entitled to is a last-bit one between two
inlined copies of the kernel, and an `f32` ULP at the largest price these
options can reach is about `1.5e-5`. A price no lap wrote keeps the `NaN` the
array is filled with and fails whatever the tolerance, and the condition
task's own lap count is asserted too, so a loop that quietly took one lap
instead of `N` fails rather than benchmarks.

The eight ids are `black_scholes/{rustflow,rayon}/{1w,24w}/{1000,4000}`.

| size | workers | rustflow | rayon | upstream C++ |
|---|---|---|---|---|
| 1000 | 1 | 22.38 ms ±1.1% | 23.18 ms ±0.2% | not published |
| 1000 | 24 | 27.87 ms ±0.2% | 25.57 ms ±0.4% | not published |
| 4000 | 1 | 349.6 ms ±0.2% | 354.8 ms ±0.7% | not published |
| 4000 | 24 | 133.7 ms ±0.1% | 160.9 ms ±0.9% | not published |

Criterion flagged 5 of 20 measurements as outliers on `rustflow/1w/1000` and 4
of 20 on `rayon/1w/4000`; both intervals are nonetheless at or under ±1.1%.

**At one worker the re-dispatchable loop is marginally the cheaper of the
two**, by 3.6% at 1000 options and 1.5% at 4000. With one thread in either
pool a lap is a condition task and a module dispatch on our side against
ending and restarting a parallel iterator on rayon's, and those come out close
enough that the row is a tie with a sign rather than a result. **At 1000
options and 24 workers neither column gains, and ours loses** -- 27.87 ms
against its own 22.38 ms at one worker, where rayon goes from 23.18 to
25.57 ms. That is the section's own prediction met: 1000 laps of 42 options a
worker is a lap boundary repeated a thousand times with almost nothing to
amortise it against, so what the row measures is the boundary. At 4000 options
it is amortised enough for both to gain -- 2.6x for ours, 2.2x for rayon's --
and ours comes out 17% ahead, 133.7 ms against 160.9 ms.

Dividing by the lap count puts the whole table on one line: at 4000 options a
lap costs 33.4 µs through the graph against 40.2 µs through the barrier, and at
1000 options 27.9 against 25.6. Both columns are far short of what 24 workers
do to a flat fan-out of this much arithmetic, which is what a shape whose
parallelism is bounded by a serial re-dispatch looks like; the condition task
and the module dispatch are the cheaper boundary of the two at the size where
either boundary is amortised at all.

## for_each

`N` `f64`s, each replaced by its own tangent: upstream's `for_each_taskflow`
builds a fresh `tf::Taskflow`, calls
`for_each_by_index(IndexRange(0, vec.size(), 1), ..)` over a body that walks
one sub-range, and runs it, timing both -- "ours includes building the graph"
from this file's own header applies here too, though what is built is one
runtime task rather than `N` of them. `par_iter_mut().for_each` is the
baseline, in the role TBB's `parallel_for` plays upstream. Sizes are two
points off upstream's `N = 10..=1e8` sweep: `1000`, where the fixed cost of
building and running a one-task taskflow still shows, and `1000000`, where
the per-element `tan` dominates it. Worker counts are this file's usual pair,
`1` and `available_parallelism`, from `benches/common/`.

**This is the first shape in the suite where the baseline is the same program
rather than a different schedule for the same work.** The four flat-graph
shapes compare a graph of `N` nodes against a parallel iterator that has no
graph, which is the asymmetry `embarrassing_parallelism`'s own
declaration-only id exists to quantify. Here both columns take one index
range and split it into chunks at run time -- `for_each_by_index`'s
partitioner against rayon's own range split -- so the construction term is
one node on our side and nothing on rayon's, and what is left is the
partitioners and the per-chunk dispatch. That makes the pair of numbers to
read not within this table but across two: **`for_each` against
`embarrassing_parallelism` at the same worker count** is the difference
between splitting a range and enqueuing one task per element, which is the
measured question this section is the input to.

Two departures from upstream are recorded in `benches/for_each.rs`'s own
module docs and matter to the comparison. The `rustflow` column's vector is a
slice of `AtomicU64` holding `f64` bit patterns, because the ported body is
one `Fn(IndexRange<usize>)` shared by every chunk and so cannot hold a
`&mut [f64]`; the baseline keeps a plain `Vec<f64>`, since `par_iter_mut`
hands each element a real `&mut f64`. And each iteration runs on freshly
generated values, because this shape mutates its input in place and upstream
regenerates `vec` once per round: each id owns **one** buffer, allocated
before anything is timed as upstream allocates `vec` in `main`, which
`iter_batched_ref`'s untimed setup rewrites between iterations. The "#366
change" section at the end of this file is why the harness is that shape
rather than a fresh allocation per iteration.

Both columns are checked before either is timed: `benches/for_each.rs`'s
`verify` runs each variant once per size and worker count, outside any
measured region, and compares every element against a sequential `f64::tan`
reference. The comparison is exact, not a tolerance -- each output depends on
exactly one input element and both columns call the same `f64::tan` on it --
so a chunk the partitioner never handed out fails the run instead of
producing a figure.

| size | workers | rustflow | rayon | upstream C++ |
|---|---|---|---|---|
| 1000 | 1 | 14.40 µs ±0.3% | 10.37 µs ±1.8% | not published |
| 1000 | 24 | 45.12 µs ±3.3% | 28.47 µs ±0.6% | not published |
| 1000000 | 1 | 8.088 ms ±0.1% | 7.875 ms ±0.0% | not published |
| 1000000 | 24 | 622.4 µs ±1.0% | 720.7 µs ±0.9% | not published |

Measured in the "#366 change" pass at the end of this file, on the same quiet
box and under the same conditions "What is measured" names for the pass that
first filled this table. `rustflow/24w/1000` carries the widest interval of
the eight at ±3.3%, with 4 of 20 measurements flagged as outliers; 5 of 20
were flagged on `rayon/1w/1000000`, whose interval is ±0.03%, and 3 of 20 on
`rustflow/24w/1000000`. A ten microsecond run is a thousand `tan` calls and a
pool hand-off, which is little for twenty samples to average over; read the
ratios below against those widths rather than past them.

**This is the closest the port comes to its baseline on a fan-out shape, and
at the largest size it is not behind at all**: 1.39x behind at 1000 and one
worker, 1.58x behind at 1000 and 24, 1.03x behind at a million and one worker,
and 0.86x -- 622.4 µs against 720.7 µs -- at a million and 24. The one-worker
million-element row is a near tie, 8.09 ns an element against 7.87 ns, on a
body that is most of the 7.87 by itself. Both columns then scale about as far
as a memory-bound pass over eight megabytes lets them: rayon turns its
7.875 ms into 720.7 µs, 10.9x, and we turn 8.088 ms into 622.4 µs, 13.0x.
**Read the 24-worker row as a tie and not as a win** -- #343's section below
measured this id's own between-run spread at 1.6x on our side against 1.06x
on rayon's, which is wider than the 14% between the two columns here.

**Nothing in `src/` changed to move that row, and it is not the port getting
faster.** This section used to read 2.17x at a million elements and 24 workers
and used to attribute the factor to the partitioner and the per-chunk
dispatch. #343's counters found both halves of that wrong -- a chunk costs
15 ns and the 547 of them are 1.3% of the run -- and found most of the
published gap to be the benchmark harness's own per-iteration input buffer.
The "#366 change" section took the buffer out of the timed region and
re-measured: of the ~1.0 ms that left this id, 0.73 ms fell inside the clock
as the release of an 8 MB input and 0.28 ms fell around it, both of them
because the setup kept **two** 8 MB blocks live per iteration rather than one.
What is above is what was underneath. The one mechanism those counters did
name -- the fan-out reaching only 15 to 19 of the 24 workers -- is filed
outside this milestone and is unaffected either way.

**A third column, `rustflow-split`, is priced in the "#381 measurement" below
rather than here.** It runs the identical task under `SplittingPartitioner`,
whose share re-splits when another worker steals it, so the pair differ only
in how a chunk reaches a worker. It is 1.00x this column at a thousand
elements and one worker and 0.95x at a million and one -- nothing splits
without a steal, which makes a pool of one that pair's control -- and
**3.40x** at a million and 24 workers, **33.8x** at a thousand and 24. That
pass re-measured this table's own `rustflow` and `rayon` ids in passing and
reproduced seven of the eight within 7%; the exception is `rustflow/24w/1000`,
which read 31.52 µs against the 45.12 µs above, and it is the id this section
already flags as ±3.3% with 4 of 20 outliers and whose between-run spread
#343 put at 1.6x. The "1.58x behind" below is that id's bad day as much as
this shape's cost.

**The cross-table comparison this section was added for.** At 24 workers
`for_each` spends 0.62 ns an element end to end (622.4 µs over a million),
where `embarrassing_parallelism` above spends 176 ns a task (11.54 ms over
65536) of which some 153 ns is not the body. Splitting an index range is
therefore nearly three orders of magnitude cheaper a unit of work than
enqueuing a node per unit -- which is what that table's declaration-only id
was pointing at, and the factor is larger than this section first reported
because the figure it divided was the harness's as much as the schedule's.
Read the order of magnitude and not the digits: those rows come from a
different run, at a load of 7.9 to 10.6, with a different body and a different
size.

## reduce_sum

`N` `f64`s summed once: upstream's `reduce_sum_taskflow` builds a fresh
`tf::Taskflow`, calls `reduce_by_index(IndexRange(0, vec.size(), 1), result,
lop, std::plus<double>())` -- `lop` folding one sub-range into the running
total it is handed, `gop` folding each worker's local into the shared result
-- and runs it, timing both. `par_iter().sum::<f64>()` is the baseline, in
the role TBB's `parallel_reduce` plays upstream. Sizes are two points off
upstream's `N = 10..=1e9` sweep: `1000`, where the fixed cost of the run
dominates the additions, and `1000000`, where the additions dominate it.
Worker counts are the usual pair from `benches/common/`.

The shape is `for_each`'s with the writes replaced by a fold, so the two
sections are worth reading together: the same range, the same partitioner and
the same one-node graph, with a per-worker local and one `gop` merge each in
place of a store per element. What a row says that `for_each`'s does not is
what the merge costs, since an `f64` addition is cheap enough that the fold
itself is close to memory-bound at the larger size.

**The check on this shape is a tolerance, not an equality, and that is the
shape rather than a weakened assertion.** Floating-point addition is not
associative, so this crate's per-worker locals folded by `gop`, rayon's own
tree of partial sums and a sequential `Iterator::sum` add the same `N` values
in three different orders and land on three slightly different `f64`s.
`benches/reduce_sum.rs` therefore asserts a relative distance of `1e-6` from
a sequential sum, before any id is timed, for both columns -- five orders of
magnitude tighter than mis-chunking would be, since dropping or repeating one
chunk of a million-element sum moves the answer by a fraction of a percent.
The module docs say so next to the constant. One departure from upstream
feeds that check: upstream's `main.cpp` only `resize`s its `vec` and never
writes it, so every round there sums zeroes -- an answer a run that skipped
half the range would also reach -- while the port fills the vector from the
same generator `sort`'s does.

| size | workers | rustflow | rayon | upstream C++ |
|---|---|---|---|---|
| 1000 | 1 | 10.22 µs ±22.8% | 4.897 µs ±0.4% | not published |
| 1000 | 24 | 135.4 µs ±1.0% | 28.45 µs ±2.2% | not published |
| 1000000 | 1 | 643.5 µs ±0.1% | 618.7 µs ±0.1% | not published |
| 1000000 | 24 | 283.4 µs ±0.8% | 111.4 µs ±0.9% | not published |

**`rustflow/1w/1000` is the one cell in this pass whose interval is too wide to
read as a point**: ±22.8%, against the ±0.4% of the baseline beside it. It is
quoted rather than dropped and rather than re-run -- one run per id is the
rule this pass is measured under -- but it means "about 10 µs" and nothing
finer. 6 of 20 measurements were flagged as outliers on `rayon/1w/1000000`,
whose interval is nonetheless ±0.1%.

**The million-element one-worker row is a near tie, which is this section's own
prediction**: 643.5 µs against 618.7 µs, 4% apart, for a fold whose body is one
`f64` addition over a pass of eight megabytes -- both columns are reading
memory and a schedule has little room to show. What the merge costs shows at
the other three rows. At a million elements and 24 workers we reach 283.4 µs,
2.3x our own one worker, where rayon reaches 111.4 µs, 5.6x. The per-worker
local and its one `gop` merge cannot be the difference -- there are 24 of them
in a run of a quarter of a millisecond -- so what is left is the per-chunk
dispatch `for_each` isolates, now spread over a fold cheap enough an element
that it hides none of it.

**The `rustflow-split` column of this shape's twelve ids is priced in the
"#381 measurement" below**, where the same `reduce_by_index` runs under
`SplittingPartitioner` at the same chunk size: a tie at one worker, 0.98x at a
million, and **4.95x** this column at a million and 24 workers, the widest
24-worker ratio of the four shapes that carry a split id. A fold of one `f64`
addition an element is the least body in the suite to amortise an extra task
over, which is the same reason this table's own 24-worker rows read the way
they do.

The 1000-element rows say the same thing from the other side. At 24 workers a
thousand-element sum costs us 135.4 µs against 10.2 µs at one worker: a factor
of thirteen **lost** by adding workers, where rayon loses 5.8x. Both columns
agree that the right worker count for a sum this short is one, and this table
puts the crossover somewhere above a thousand elements for either of them.

## scan

An inclusive scan of `N` `i32`s under multiplication: upstream's
`scan_taskflow` builds a fresh `tf::Taskflow`, calls
`inclusive_scan(input.begin(), input.end(), output.begin(),
std::multiplies<int>{})` and runs it, timing both. Sizes are two points off
upstream's `N = 10..=1e8` sweep, `1000` and `1000000`; worker counts are the
usual pair. The output buffer is allocated by the harness and reused across
samples, as upstream's `output.resize(N)` in `main.cpp` is -- every slot is
overwritten by every run, and the allocation is not part of what upstream
times.

**The baseline column is a sequential `Iterator::scan`, not a parallel one,
because rayon has no parallel scan.** Its parallel iterators express map,
filter, reduce and sort; a prefix scan, whose every output depends on every
earlier input, is not among them, and a `fold` plus a `collect` would be a
different algorithm wearing the name. So this column is labelled `sequential`
rather than `rayon`, the way `linear_chain`'s section records that *its*
rayon column is empty instead of putting a loop in it and calling that a
parallel baseline. The difference from `linear_chain` is that here the
sequential program is the one a caller would actually write, so it is a real
baseline and not a stand-in: what a row asks is whether two passes over the
data across several workers beat one pass on one. It has no worker axis --
a sequential scan has no count to be held to -- so it is measured once per
size and the same figure is read against both of that size's rows, exactly as
the `pipeline` section's `mpsc` chain is.

Two notes on the arithmetic, both upstream's behaviour made explicit.
Products of `int`s overflow within a handful of elements at every size here,
which upstream's `std::multiplies<int>` leaves to undefined behaviour and
both columns spell `i32::wrapping_mul`: the point of the shape is the scan's
dependency structure, not the value it ends on. And the generated inputs are
odd where upstream's are `::rand()`, because a single zero anywhere in the
input makes every later output zero and would blind the check below to
anything that went wrong past that point.

That check is `benches/scan.rs`'s `verify`, run before any id of a size is
timed: the `rustflow` variant runs once per size and worker count outside any
measured region and its whole output buffer is compared, slot by slot,
against the sequential baseline's, so a block whose carry-in was never
applied fails the run rather than producing a figure.

| size | workers | rustflow | sequential | upstream C++ |
|---|---|---|---|---|
| 1000 | 1 | 10.53 µs ±0.5% | 630.8 ns ±0.1% | not published |
| 1000 | 24 | 241.7 µs ±0.9% | 630.8 ns ±0.1% | not published |
| 1000000 | 1 | 635.8 µs ±0.1% | 640.5 µs ±0.1% | not published |
| 1000000 | 24 | 343.1 µs ±0.8% | 640.5 µs ±0.1% | not published |

5 of 20 measurements were flagged as outliers on `rustflow/24w/1000000`; every
interval in the table is at or under ±0.9%.

**The question this section asks -- whether two passes over the data across
several workers beat one pass on one -- comes out "only at the larger size, and
then by less than a factor of two."** At a million elements the two columns
tie at a single worker, 635.8 µs against 640.5 µs -- as they must, since
`inclusive_scan` takes upstream's own `W <= 1` single-pass path there and is
then the same one pass through a different loop -- and the parallel scan beats
the sequential one 1.87x at 24 workers. The sequential column is why that win
is so small rather than the parallel column being slow: 640.5 µs for a million
`wrapping_mul`s is 0.64 ns an element, one integer multiply's latency at this
clock, so the baseline is a dependency chain running at the hardware's limit
and the two-pass algorithm has to do twice the multiplies before any of its
workers helps.

**This shape carries no `rustflow-split` id, where `for_each`, `reduce_sum`,
`primes` and `mandelbrot` each gained one with #374.** The scan family takes
no partitioner anywhere in the port, because it takes none upstream:
`src/algorithm/scan.rs`'s provenance names its six entry points as
`make_inclusive_scan_task`, `make_exclusive_scan_task` and the four
`make_transform_*_scan_task` overloads at v4.1.0, and not one of them has a
`part` parameter -- a two-pass scan divides its range by worker count rather
than by a chunk policy. Giving it one would be a departure from upstream's
user-facing surface and is nobody's decision yet; the "#381 measurement"
below records it where the other four shapes' split figures are.

At a thousand elements the parallel scan loses by 17x at one worker and by
383x at 24. A 630 ns sequential scan is shorter than building the one-node
taskflow and handing out its chunks, and at 24 workers it is shorter than
waking the workers that would take them: 241.7 µs for 41 elements a worker is
the clearest statement in this file of what a parallel algorithm's floor costs
when there is nothing above it.

## primes

Counts the primes in `[1, value)` by trial division: upstream's
`primes_taskflow` is one `reduce_by_index` over
`IndexRange<size_t>(1, value, 1)` whose local operator folds `is_prime(i)`
over a chunk and whose global one is `std::plus`, with
`DynamicPartitioner{primes_chunk}` -- a chunk of ten -- deciding how those
chunks are handed out. "Ours includes building the graph" from this file's
own header applies here as it does everywhere, and costs this shape the least
of any section: the graph is one node, so what the `rustflow` ids measure is
the partitioner's cursor and the arithmetic behind it.

The baseline is `(1..value).into_par_iter().filter(is_prime).count()`, in the
role TBB's `parallel_reduce` over a `blocked_range(1, value, primes_chunk)`
plays in upstream's own `tbb.cpp`. **The two sides do not share a chunk
size**, and that is deliberate: upstream hands TBB the same ten it hands the
`DynamicPartitioner`, while a rayon parallel iterator splits adaptively from
the length of the range and takes no chunk size at all. Forcing it to ten
with `with_min_len` would measure a rayon nobody writes, so the baseline is
left as it would be written and the difference is named here instead.

Sizes are two points off upstream's `p = 10, 100, .. 1e8` sweep, ×10 a step:
`10000`, where the fixed cost of a run still shows against a thousand chunks,
and `1000000`, where the trial division dominates it. Upstream's own top point
is minutes of arithmetic a round, which criterion's twenty samples an id would
multiply. The ids are `primes/{rustflow,rayon}/{1,N}w` at each of those two
sizes, with `N` the box's `available_parallelism`.

Before any id is measured, both variants are run once per size *and* per
worker count and their counts asserted against a single-threaded reference
over the same `is_prime` -- a count that is only right on one thread is still
wrong, so each worker count checks its own.

| size | workers | rustflow | rayon | upstream C++ |
|---|---|---|---|---|
| 10000 | 1 | 66.82 µs ±2.9% | 64.79 µs ±1.2% | not published |
| 10000 | 24 | 160.9 µs ±5.3% | 52.20 µs ±0.8% | not published |
| 1000000 | 1 | 29.58 ms ±0.1% | 29.03 ms ±0.1% | not published |
| 1000000 | 24 | 2.925 ms ±0.4% | 2.636 ms ±1.3% | not published |

`rustflow/24w/10000` is the loosest cell at ±5.3%, which is small against the
factor of three that row reports.

**At one worker the two columns are the same program**: 66.82 against 64.79 µs
at 10000 and 29.58 against 29.03 ms at a million, 3.1% and 1.9% apart. That is
what a shape whose body is trial division should say about a scheduler, and it
is the control the other two rows are read against. At a million and 24
workers the gap is 11% -- 2.925 ms against 2.636 ms -- with both columns
reaching 10.1x and 11.0x of their own one worker, which is the range twelve
physical cores with SMT give an ALU-bound body and not a scheduling ceiling
either side is hitting.

**The `rustflow-split` column, priced in the "#381 measurement" below, holds
upstream's chunk of ten and changes only the dispatch**:
`SplittingPartitioner::new(10)` against the `DynamicPartitioner::new(10)` this
table's `rustflow` ids use. It ties at one worker and costs **1.27x** at a
million and 24 workers -- the *smallest* 24-worker penalty of the four shapes
that carry a split id, because trial division is the heaviest body in the
suite to amortise an extra task over -- and **7.02x** at 10000, where the
whole job is under 70 µs of division.

**The 10000-element 24-worker row is the one the two sides' chunk sizes
decide, and it costs us 3.1x**: 160.9 µs against 52.20 µs, and 2.4x our own one
worker. `value = 10000` is a thousand chunks of ten, each about 70 ns of
division at this end of the range, handed out one at a time through one shared
cursor -- a thousand hand-offs inside 160.9 µs is 161 ns of wall clock each,
more than twice the division a chunk contains, which is what a row bounded by
its cursor rather than by its arithmetic looks like. rayon splits the same range adaptively from its
length and hands each worker a contiguous four hundred integers instead. The
section says above that forcing rayon to ten would measure a rayon nobody
writes; this row is what the difference is worth, and it is the dominant term
in it rather than anything about the two schedulers' queues.

## mandelbrot

An escape-time render of the Mandelbrot set: upstream's `mandelbrot_taskflow`
is one `for_each_index(0, H, 1, .., DynamicPartitioner(1))` whose body walks
the `W` points of one index's line through `mandel.hpp`'s `escape_time`. **A
chunk of one is the whole point of the shape.** The cost of a line varies by
two orders of magnitude between the edge of the image and the interior of the
set, where every point runs the full `MAX_ITERATION = 200`, so this is the
suite's load-imbalance benchmark rather than a second uniform `for_each` --
what a row of its table reads is how two schedulers hand out two hundred
unequal pieces of work, not how fast either one dispatches. `D`, `H` and `W`
stay the size knobs they are upstream, with `H == W == N` as `main.cpp` sets
them.

The baseline is `par_iter` over the same lines, in the role TBB's
`parallel_for` plays in upstream's own `tbb.cpp`. Unlike the
`matrix_multiplication` section below, **both columns here write into the same
buffer type**, so the row compares the two schedulers and nothing else: the
image is one `Mutex<Vec<u32>>` a line, and both `for_each_index` and
`par_iter` visit each line exactly once, which makes every lock the lock of
that line's only writer -- two uncontended atomics against the hundreds of
transcendental calls a line of this kernel costs. Upstream synchronises
nothing at all and relies on its writes landing at disjoint addresses, which a
`'static` task body cannot express in safe Rust; `benches/mandelbrot.rs`'s own
module docs say so next to the code, along with the other two departures (the
image holds escape iterations rather than palette entries, since `get_color`'s
only consumer is the `dump_tga` call `main.cpp` leaves commented out; and each
index's line is contiguous rather than a stride-`W` column of upstream's
picture, a transpose only that un-ported dump would notice).

Sizes are two points off upstream's `N = 100..=1000` sweep, step `100`: `100`,
where the per-line cost is small enough for the fixed cost of a run to show,
and `500`, where the kernel dominates it. The ids are
`mandelbrot/{rustflow,rayon}/{1,N}w` at each size.

Before any id is measured, each variant renders into a buffer of its own, once
per size and per worker count, and the whole image is asserted point for point
against a single-threaded render by the same kernel. The comparison is exact
because the quantity is an iteration count rather than a floating-point
result, and the per-variant buffer is what makes a skipped line fail here
instead of silently reading the other column's work.

| size | workers | rustflow | rayon | upstream C++ |
|---|---|---|---|---|
| 100 | 1 | 16.11 ms ±0.1% | 16.38 ms ±0.5% | not published |
| 100 | 24 | 1.042 ms ±1.5% | 1.229 ms ±0.6% | not published |
| 500 | 1 | 405.9 ms ±0.7% | 410.9 ms ±0.7% | not published |
| 500 | 24 | 21.96 ms ±2.1% | 22.67 ms ±0.5% | not published |

**This is the first shape in this file where the port is ahead of its rayon
baseline at every row**, and the margin is small everywhere but one: 1.7% at
100 lines and one worker, 1.2% at 500 and one worker, 3.2% at 500 and 24
workers, and 15% at 100 lines and 24 workers. The two one-worker rows are a
tie by construction -- one thread renders every line whatever order it picks
them in -- and that is what makes them the control: the two columns' buffers,
locks and kernels cost the same to within a percent or two, so the 24-worker
rows can be read as scheduling and nothing else.

**The `rustflow-split` column is this shape's most interesting number and it
is in the "#381 measurement" below**: `SplittingPartitioner::new(1)` against
the `DynamicPartitioner::new(1)` above, same chunk of one, same body. A
partitioner whose share re-splits when it is stolen should have its best case
on the suite's load-imbalance shape, and it does not -- **1.48x** at 100 lines
and **1.70x** at 500, against a column that is itself ahead of rayon at both.
Where the dynamic chunk of one costs one cursor acquisition a line, the split
costs a task.

There the chunk of one earns its keep. 100 unequal lines over 24 workers is
four lines a worker if the deal is even and a visible imbalance if it is
static, and `for_each_index`'s dynamic partitioner at a chunk of one reaches
15.5x of its own one worker where rayon's adaptive split reaches 13.3x. At 500
lines there are twenty lines a worker, enough that either policy averages out,
and the two reach 18.5x and 18.1x -- the imbalance stops being what the row is
about and the row goes back to being a tie.

## matrix_multiplication

`N × N` `f64` matrices, `c = a * b`, and **the one shape in this group with a
graph around its algorithms**: upstream's `matrix_multiplication_taskflow`
builds four `for_each_index` tasks over `[0, N)` -- `init_a`, `init_b`,
`init_c` and `comp_c` -- and joins them with
`comp_c.succeed(init_a, init_b, init_c)`, three sources into one sink. The
edges are ported as upstream has them, the partitioner is the default upstream
leaves unspecified, and the bodies are upstream's: `a[i][j] = i + j`,
`b[i][j] = i * j`, `c[i][j] = 0`, then the `ijk` triple loop. Both sides lay
their matrices out as flat vectors indexed `i * n + j`, which is what
`matrix_multiplication.hpp`'s `allocate_matrix` builds row by row, with no
numeric library on either side of the table.

The baseline is three `par_iter_mut` inits followed by a `par_chunks_mut`
product, in the role TBB's `parallel_for` plays in upstream's own `tbb.cpp`.
**Its three inits are three barriers where ours are three concurrent
sources**, which is the one place in this suite where the construction cost
"ours includes building the graph" charges us buys something back rather than
only costing -- three independent `O(N²)` passes run together against three
run in sequence, and at the smaller size below that is a visible share of the
run.

**The two columns do not share a representation, and part of any gap is
that.** `comp_c` reads *all* of `b` while another row's body may still be
writing `c`, and a task body here is `'static`, so there is no safe way to
hand out `&mut` rows the way rayon's borrowing closures receive them --
upstream's unsynchronised `double**` writes have no safe translation, and this
port's posture is a safe shape over an `unsafe` reproduction of C++ pointer
semantics. Our cells are therefore `AtomicU64` bit patterns accessed
`Relaxed`, with the graph's join rather than the cell providing the ordering,
exactly as upstream's edge does; rayon's are plain `f64`s. On x86-64 a relaxed
load or store of a `u64` is a plain `mov`, but it is neither vectorisable nor
hoistable, which is why `benches/matrix_multiplication.rs` accumulates
upstream's `c[i][j] +=` in a local and stores once per `j` -- the additions and
their order are unchanged, so the product is identical down to the bit, but
the inner loop is not asking the optimiser for something an atomic cell cannot
give. Read a row of this table as scheduler *plus* representation, and the
`mandelbrot` section above as the one with the representation held fixed.

Sizes are two points off upstream's `N = 128..=1024` sweep, step `32`: `128`,
where the `O(N²)` inits and the graph are still a visible share of the run, and
`512`, where the `O(N³)` product dominates them. The ids are
`matrix_multiplication/{rustflow,rayon}/{1,N}w` at each size.

Before any id is measured, each variant multiplies into matrices of its own,
once per size and per worker count, and the whole product is asserted against
a single-threaded reference. That comparison is exact rather than approximate,
and legitimately so: every value is a whole number,
`c[i][j] = Σ (i + k) * (k * j)`, and the largest of them at these sizes is
under `2^53`, so every partial sum is exactly representable and the summation
order cannot change the answer.

| size | workers | rustflow | rayon | upstream C++ |
|---|---|---|---|---|
| 128 | 1 | 1.908 ms ±1.1% | 2.145 ms ±0.5% | not published |
| 128 | 24 | 256.6 µs ±0.3% | 317.8 µs ±1.1% | not published |
| 512 | 1 | 264.1 ms ±0.1% | 266.8 ms ±0.1% | not published |
| 512 | 24 | 14.86 ms ±1.0% | 16.06 ms ±1.7% | not published |

**The port is ahead at all four rows -- 11% and 19% at 128, 1.0% and 7.5% at
512 -- and the representation is why that is a surprise rather than why it
happens.** `AtomicU64` cells accessed `Relaxed` can only cost us against plain
`f64`s: the same `mov`, but neither vectorisable nor hoistable. So the table's
sign is the thing to read first, and its size second.

Two of the four rows are not about the graph at all. At 512 and one worker the
`O(N³)` product is the run and the two inner loops agree to 1.0%, which is
what a relaxed store per `j` against a plain one should cost. At 128 and one
worker nothing runs concurrently on either side -- three init passes and a
product, in order -- so that row's 11% is those passes' own cost and not the
three sources. What the edges are worth is the *change* between the worker
counts: 11% to 19% at 128 and 1.0% to 7.5% at 512, eight and six and a half
points gained by letting `init_a`, `init_b` and `init_c` run together where
rayon's three `par_iter_mut`s are three barriers. That is the one place in this
suite where "ours includes building the graph" buys something back, and six to
eight points of a run is what it buys.

## wavefront

An `MB × NB` grid of block tasks, each preceding its right and its down
neighbour: one source at the top left, one sink at the bottom right, a join at
every interior node, and a ready frontier that sweeps the grid as an
anti-diagonal whose length grows and then shrinks again. The body is
upstream's `block_computation(i, j)` -- a `B × B` block update whose every
cell reads the cell above it and the cell to its left -- which is what those
two edges per node are for: a block's top row needs the block above it and its
left column needs the block to its left, and nothing else.

**That body is the one upstream has commented out**, and the section says so
rather than leaving the provenance line to imply otherwise: at the pin,
`matrix.hpp`'s `block_computation` is `return i + j;` with the `B × B` update
sitting in comments directly below it, so the published benchmark times a
grid whose nodes do nothing. `i + j` is also not a body this port could keep
-- nothing reads the result, so it compiles to nothing at all -- and it is
the only version of this shape whose answer can be checked. The commented
update is what the grid's two edges per node exist for, so that is what runs
here.

**This is the first shape here built with `placeholder` and `work` rather
than `emplace`**, and that is upstream's own construction rather than a
translation choice. The grid's edges are known before its bodies are: the
backwards walk reaches node `(i, j)` and declares edges to `(i, j + 1)` and
`(i + 1, j)`, which is only possible because every node of the grid already
exists as a placeholder with no work attached. The graph shapes above either
build each node and its body in one `emplace` call or, as `black_scholes`
does, leave a `placeholder` a placeholder -- upstream's `init` node, which
never has work attached at all -- so none of them exercises the two-step
form, and this one does nothing else.

**No rayon baseline, and the baseline column is a sequential sweep.** A
wavefront is a dependency pattern rather than a data decomposition -- the set
of blocks that may run at once changes every step and is not a collection
known up front -- so `par_iter`, `join` and `scope` have nothing to say about
it, and driving the anti-diagonals by hand over `par_iter` would measure the
hand-written sweep and its barriers rather than a baseline's scheduler. That
is `linear_chain`'s reason for an empty rayon column, reached from the other
direction: the chain has no parallelism for rayon to express, and this shape
has parallelism no rayon primitive describes. Upstream compares against a TBB
flow graph, which we have no equivalent of. What stands in its place is
upstream's own `seq.cpp`, labelled `sequential`: the same blocks in row-major
order, which respects every edge of the grid without declaring one. It has no
worker axis -- it is one thread by construction -- so it is measured once per
size and its figure is read against both rows of that size, the way
`pipeline`'s `mpsc` baseline is. It also has no graph to build, which is this
file's standing asymmetry: ours includes constructing `MB × NB` placeholders,
bodies and edges.

Sizes are two points off upstream's `main.cpp` sweep of the matrix dimension
`S` over the powers of two from `8` to `1 << 14`, at its fixed block size
`B = 8`: `S = 256`, a 32 × 32 grid of 1024 blocks where the fixed cost of a
run still shows, and `S = 1024`, a 128 × 128 grid of 16384 blocks where the
per-block cost dominates it. The size column is `MB · NB`, the block count,
which is what upstream reports. The six ids are therefore
`wavefront/sequential/{1024, 16384}` and
`wavefront/rustflow/{1w, 24w}/{1024, 16384}`.

| size | workers | rustflow | sequential | upstream C++ |
|---|---|---|---|---|
| 1024 (32x32) | 1 | 1.110 ms ±0.9% | 930.6 µs ±0.1% | not published |
| 1024 (32x32) | 24 | 472.9 µs ±0.2% | 930.6 µs ±0.1% | not published |
| 16384 (128x128) | 1 | 21.04 ms ±7.7% | 14.13 ms ±0.2% | not published |
| 16384 (128x128) | 24 | 5.324 ms ±1.3% | 14.13 ms ±0.2% | not published |

`rustflow/1w/16384` carries ±7.7%, the widest interval in this pass after
`reduce_sum`'s one wide cell, so read it as "40 to 60% over the sequential
sweep" rather than as the 49% its point estimate gives.

**One worker pays for the graph and gets nothing back**: 1.110 ms against the
sequential sweep's 930.6 µs at 1024 blocks and 21.04 ms against 14.13 ms at
16384. That is this file's standing asymmetry with no parallelism to offset
it -- 16384 placeholders, their bodies and 32512 edges are built inside the
measured region, where a row-major sweep builds nothing -- and it is the
reason the one-worker column of a dependency shape is a cost and not a
comparison.

**24 workers buy 2.0x at 1024 blocks and 2.65x at 16384, and the
anti-diagonal is the ceiling rather than the scheduler.** A 128x128 grid's
frontier averages 64 blocks over its 255 steps but begins and ends at one, and
each of those steps is a join the blocks behind it wait on; the sequential
column puts a block's own work at 862 ns, so there is not much for a worker to
do between two joins. Against that, 24 workers turn 862 ns of serial work a
block into 325 ns of wall clock a block: the graph pays for itself by 2.65x
where its one-worker column cost 49%, and what separates those two figures is
the shape's own parallelism rather than anything the scheduler decides.

What the benchmark does assert, with no timing involved, is the answer. The
sweep's result is a deterministic function of the grid, so
`benches/wavefront.rs` runs the graph once on a fresh matrix and compares
every cell against the sequential sweep's, at each worker count, before
criterion measures anything -- a block that runs before the block above it or
to its left reads a cell still holding its initial value and writes one the
reference never produces. Deleting either edge of the grid makes that
comparison fail at the parallel worker count, which is what the check is
there for.

## skynet

A ten-way tree of depth `MaxDepth` whose leaves are the integers below
`10^MaxDepth`, summed back up: upstream's `skynet_one_tf` opens a
`tf::TaskGroup` per interior node, `silent_async`s ten children into it,
`corun`s for them and adds the ten slots, and the root is one
`executor.async(...)` whose handle is waited -- a group may only be opened by
a worker. Both are translated as they stand, so the `rustflow-group` id is
the same port shape `fibonacci`'s id of that name is: `task_group()`,
`silent_async`, `corun`. It carries `group` rather than being plain
`rustflow` because the submission path is the thing this shape measures, and
there is exactly one `rustflow` variant of it here.

**The baseline is `rayon::scope` plus ten `spawn`s per level**, which is the
form upstream's own non-Taskflow column takes: `skynet/tbb.cpp` runs a
`tbb::task_group`, `tg.run` ten times, then `tg.wait`. There is no graph on
either side, so the construction asymmetry "What is measured" warns about is
absent here as it is for `async_task` and `fibonacci`; what the two ids do
not share is upstream's frame-borrowed `std::array<size_t, 10> results`,
which a `Send + 'static` body cannot have. The `rustflow-group` id pays an
`Arc<[AtomicU64; 10]>` per interior node for it -- one allocation and ten
refcount pairs -- where the rayon id splits a plain `[u64; 10]` into ten
disjoint `&mut`, since a scope's closures may borrow the frame the scope is
on. `benches/skynet.rs`'s module docs list that and the two other departures.

**Sizes are `MaxDepth = 3` and `4`** -- 1110 spawned tasks and 11110, a decade
apart, the smaller where the fixed cost of a round still shows against the
tree and the larger where the per-task cost dominates it. **Both are below
upstream's own sweep, which runs `MaxDepth = 1..=8`, and what bounds them is
the stack rather than the clock**: this is the nested-corun stack bound, which
the #359 section above measures once for the whole file, and the rows below are
its reading of this shape -- a release build driven straight rather than under
criterion.

| id | workers | largest `MaxDepth` that passes | smallest that aborts |
|---|---|---|---|
| `rustflow-group` | 1 | 4 | 5 |
| `rustflow-group` | 24 | 5 | 6 |

One worker is the binding end, and #359 counted what that costs here: the
whole tree, all 1111 interior nodes of `MaxDepth = 4`, nests on the one stack
there is, for 1.12 MiB of the 2 MiB a worker gets, where 24 workers spread the
same tree and the deepest of them reaches 78 KiB. `MaxDepth = 4` is therefore a
factor of ten in task count inside the one-worker ceiling -- `MaxDepth = 5`
demands 11.2 MiB -- and each of the four sizes-by-workers combinations above
was run ten times to check that it sits inside it rather than on it.

**The answer is checked outside the measured region.** The leaves are exactly
the integers below `n = 10^MaxDepth`, each counted once, so the sum is
`n * (n - 1) / 2`; each id is run once per row and compared against it before
criterion times anything, which upstream does not do at all --
`skynet(executor, MaxDepth)` discards `skynet_one_tf`'s return value. A
recursion that lost a subtree fails the bench rather than reporting a good
number for doing less work.

| size | workers | rustflow-group | rayon | upstream C++ |
|---|---|---|---|---|
| `MaxDepth = 3` | 1 | 237.1 µs ±0.7% | 29.05 µs ±0.4% | not published |
| `MaxDepth = 3` | 24 | 539.9 µs ±1.5% | 33.08 µs ±1.6% | not published |
| `MaxDepth = 4` | 1 | 2.296 ms ±0.8% | 254.3 µs ±0.3% | not published |
| `MaxDepth = 4` | 24 | 2.465 ms ±1.9% | 119.8 µs ±0.6% | not published |

**The port loses this shape by between 8 and 21x, and what to take from the
table is the per-task constant rather than any of those ratios.** At
`MaxDepth = 4` and one worker, 11110 spawned tasks in 2.296 ms is 207 ns a
task against the rayon scope's 22.9 ns. **That constant is pre-#376**: the
same id reads 2.016 ms on today's `main`, **181 ns a task**, and "The #382
measurement" at the end of this file puts the whole of that 13.7% on one heap
block fewer per free node -- 14.4% for #376 alone against −0.4% for #379. The body adds ten slots and returns, so
essentially the whole 184 ns of difference is the submission path: a
`silent_async` into the shared buffers, an `Arc<[AtomicU64; 10]>` allocated and
dropped per interior node, and a `corun` whose waiting frame the tenth child
releases -- #342 counted this shape and found no parent here re-queued at all.

**24 workers make our column slower and rayon's faster, which is the most
informative pair of rows here.** 237.1 to 539.9 µs at `MaxDepth = 3` and 2.296
to 2.465 ms at 4, against rayon's 29.05 to 33.08 µs and 254.3 to 119.8 µs. A
`TaskGroup`'s children go to the shared overflow buffers rather than to the
submitting worker's own queue, so every one of those tasks is distributed by
stealing; with 23 other workers contending for the buffers that costs more
than the extra hands earn, and a tree of 11110 tasks of nothing never grows
enough to carry it. These four rows price the submission path of the group
route, and they are the baseline #342's A/B was measured against.

**That attribution is confirmed by removing it**, which is what the "#380
measurement" section below did: #377's `rustflow-runtime-group` id is this
same recursion with each node's group opened on the `Runtime` running it, so
its children go onto that worker's own queue instead, and in the same process
it reads 1.0090 ms at `MaxDepth = 4` and 24 workers against this column's
2.5976 -- the first group-route id in this file to get anything at all out of
23 more workers on a tree whose bodies hold no work. The figures for it are in that section rather than in the table
above, which this pass did not re-take.

What the shape is here to measure is the group route's cost per interior node,
and the mechanism is **not** the one this section first claimed. Each of the
`(10^MaxDepth - 1) / 9` interior nodes is a parent, but it is an anchored
*frame* rather than a preempted node: its children count down the group's own
anchor, which never carries `PREEMPTED` and has no job to resume, so what the
last child releases is the frame waiting inside `TaskGroup::corun`. #342
counted exactly that over this shape -- 48880 nested tear-downs, 4915 of them
driving a counter to zero, **none** finding `PREEMPTED` -- so none of the
184 ns a task above is a re-queue that resuming a parent on the spot could
give back. The ids above are still what made the measurement possible; the
#342 section is what it concluded.

## nqueens

The classic backtracking count -- one queen per column, prune the placements
that attack an earlier one, count the complete boards -- with a task per
surviving placement: upstream's `nqueens(j, a, executor)` opens a
`tf::TaskGroup` per level, `silent_async`s one child per legal placement in
column `j`, gathers each child's count through a per-child slot (`parts`),
`corun`s, and returns the sum; the root is one `executor.async(...)` whose
handle is read. The `rustflow-group` id is that, line for line, and is named
for its submission path for the same reason `skynet`'s is.

**The baseline is `rayon::scope` plus one `spawn` per legal placement**,
again the form upstream's own `nqueens/tbb.cpp` takes with `tbb::task_group`:
`g.run` per placement, then `g.wait`. Both sides prune with upstream's
`queens_ok`, so the arithmetic is the same work in both columns; both sides
give a child an owned board, because a scope's `spawn` is `Send` too and
upstream's trick of handing child `i` a reference into the parent's
`N`-by-`N` `buf` is not available to either. What only the `rustflow-group`
id pays is `parts` as an `Arc<Vec<AtomicU64>>` where the rayon id splits a
`Vec<u64>` into disjoint `&mut`. `benches/nqueens.rs`'s module docs list the
departures in full.

What this shape adds over `skynet` is that **the tree is ragged**: a level
spawns between zero and `N` children depending on what the pruning leaves, so
the fan-out is data rather than a constant and most interior nodes wait on a
handful of children rather than on ten. The group route's per-parent cost is
therefore spread over fewer children here than there, which is what makes the
pair worth measuring rather than either one alone. (This section first said
that cost was a preempted parent's re-queue; #342 counted the suite and found
no shape here re-queues one.)

**Sizes are `q = 6` and `8`** -- 152 spawned tasks and 2056, a factor of
thirteen apart, with the same split of roles the other tables use. **Both are
below upstream's own default sweep, which runs `q = 1..=14`, and what bounds
them is the stack rather than the clock**, exactly as for `skynet` and for the
reason the #359 section above measures:

| id | workers | largest `q` that passes | smallest that aborts |
|---|---|---|---|
| `rustflow-group` | 1 | 8 | 9 |
| `rustflow-group` | 24 | 10 | 11 |

`q = 8` is a factor of four in task count inside the one-worker ceiling,
which is the margin `fibonacci`'s own sizes keep against its cliff, and each
size-by-workers combination was run ten times to check it sits inside rather
than on it. One worker is the binding end here too: #359 measured `q = 8`
nesting 1322 bodies and 1.35 MiB onto that one stack against the deepest
worker's 93 KiB at 24, with `q = 9` demanding 5.58 MiB.

**The answer is checked outside the measured region**, against upstream's own
`answers` table from `nqueens.hpp`: `answers[6]` is 4 and `answers[8]` is 92.
Upstream checks the same table after its clock stops; the difference is only
that the check here runs once per id, before criterion times that id a
hundred times.

| size | workers | rustflow-group | rayon | upstream C++ |
|---|---|---|---|---|
| `q = 6` | 1 | 84.09 µs ±0.4% | 31.97 µs ±1.0% | not published |
| `q = 6` | 24 | 829.7 µs ±6.5% | 28.20 µs ±2.0% | not published |
| `q = 8` | 1 | 1.156 ms ±0.5% | 535.8 µs ±0.1% | not published |
| `q = 8` | 24 | 2.946 ms ±4.6% | 184.5 µs ±1.1% | not published |

The two 24-worker `rustflow-group` cells carry ±6.5% and ±4.6%, the loosest in
either recursive section, with 3 of 20 measurements flagged on `rayon/24w/8`.
The gaps below are factors rather than percentages, so those widths change
nothing about any of them.

**The ragged tree costs the port less than the regular one at one worker and
more at 24.** At `q = 8` and one worker it is 2.2x behind -- 1.156 ms against
535.8 µs, or 562 ns a task against 261 over 2056 tasks, a figure the node
changes leave where it was: #376 took 7.0% off it and #379 put 3.7% back, so
"The #382 measurement" at the end of this file reads 556 to 564 ns there and
calls the pair not resolved -- where `skynet` at one
worker is 9.0x behind, because a `nqueens` body copies a board and prunes a
column and so gives the submission path something to hide behind. At 24
workers it goes the other way: 2.946 ms against 184.5 µs is 16x, with our
column 2.5x slower than its own one worker while rayon's scope is 2.9x faster
than its own -- a regression the shared buffers are the whole of, measured as
such in the "#380 measurement" section below: the same recursion with its
groups opened on the `Runtime` reads 897.52 µs at 24 workers against this
column's 3.2976 ms in the same process, the largest of the five improvements
that measurement found. Most interior nodes here wait on a handful of children rather
than on ten, so each parent's own share of the group route -- a `task_group()`
opened, a `parts` vector allocated, a frame parked in `corun` until the anchor
clears -- is amortised over a handful of submissions instead of ten, which is
the reason this section gives above for measuring the pair rather than either
alone, now with a number on it. What that share is *not* is a re-queue: #342
read 8832 nested tear-downs here and `PREEMPTED` on none of them.

## graph_pipeline

A `Pipeline` of `P = 4` pipes over `L = 8` lines run across a
`LevelGraph` rather than a token stream of its own, which is upstream's
`graph_pipeline`: a serial `Input` pipe walks the graph's nodes level by level
and writes the node it is on into the running line's slot of an `L`-wide
buffer, two `Filter` pipes re-do that node's work, and a serial `FilterFinal`
adds up the node's predecessors' values from the level above and writes its
final value back. The pipeline is `composed_of` into a `Taskflow` and run
once, and -- "ours includes building the graph" from this file's own header --
building the `Pipeline` and composing it are inside the measured region while
generating the graph is not, exactly as upstream has them.

**Every pipe is serial, which is upstream's shape and not an omission.**
`taskflow.cpp` builds all sixteen of its pipe-count variants out of
`tf::PipeType::SERIAL` alone and `tbb.cpp` matches it with
`tbb::filter::serial_in_order` throughout, so there is no parallel middle pipe
anywhere in this benchmark upstream. The concurrency a serial chain still has
is across *stages* -- one line's token in `FilterFinal` while the next line's
is still in `Input` -- and that is also what makes a serial `FilterFinal`
correct here: tokens reach it in token order, the walk is level-major, so every
node of level `l - 1` has its final value before any node of level `l` asks for
it.

Sizes are two points off upstream's own `LevelGraph(i, i)` sweep,
`i = 1, 16, 31, ..., 451`: `i = 16`, where building the pipeline still shows
against 256 tokens, and `i = 121`, where 14641 tokens' per-node cost dominates
it. The table reports upstream's own `graph_size()`, `|V| + |E|`, which is 857
and 51055 for those two. Workers are the usual `1` and `available_parallelism`
from `benches/common`, and the per-node work is that module's shared `dummy`
rather than upstream's 16x16 integer matrix multiply -- which allocates three
`std::vector<std::vector<int>>` per call, so most of what it measures is an
allocator. **`benches/graph_pipeline.rs` carries its own `LevelGraph`
generator rather than sharing `benches/graph_traversal.rs`'s:** upstream ships
`levelgraph.hpp` twice, once per benchmark directory, and this copy is the one
with `uid()` and `get_value`/`set_value` on a node, plus the in-edges
`FilterFinal` reads and the traversal's copy drops. Node generation is
identical in both, and this file's copy keeps the traversal's two documented
departures from it (a seeded `Xorshift64`, no `_chosen`/`BFS`).

**The baseline is the `mpsc` thread chain, not rayon**, for the reason the
`pipeline` section above gives at length: rayon has no pipeline primitive. Four
`std::thread`s chained by `std::sync::mpsc`, one per pipe role, in the role
`tbb::parallel_pipeline` plays upstream's own `tbb.cpp`. Both sides walk the
same graph in the same order, call `dummy` once per node per stage and write
the same values into the same per-node cells, so they differ in their
scheduling and in nothing else; and as in the `pipeline` section the baseline
has no `lines`/`workers` axis, so `mpsc` is measured once per graph size and
read against every row of that size. Both sides' answer is checked against
upstream's own sequential model -- `gold.cpp`'s `measure_time_gold` --
*outside* every measured region, before either id is registered, so a schedule
that sums the wrong predecessors fails the benchmark instead of winning it.

The ids are `graph_pipeline/mpsc/<|V|+|E|>` and
`graph_pipeline/rustflow/8l_<workers>w/<|V|+|E|>`.

| graph size | workers | rustflow | mpsc | upstream C++ |
|---|---|---|---|---|
| 857 (16x16) | 1 | 592.4 µs ±0.4% | 277.4 µs ±0.7% | not published |
| 857 (16x16) | 24 | 534.8 µs ±0.3% | 277.4 µs ±0.7% | not published |
| 51055 (121x121) | 1 | 32.77 ms ±0.2% | 11.37 ms ±0.4% | not published |
| 51055 (121x121) | 24 | 28.71 ms ±0.2% | 11.37 ms ±0.4% | not published |

**An all-serial chain has no pipe-level parallelism to win, and the table says
so twice.** 24 workers buy 10% at the smaller graph and 12% at the larger over
our own one worker -- not nothing, since lines do overlap stages -- and the
four-thread mpsc chain is 1.9x and 2.5x ahead of the better of our two rows at
the two sizes. Per token the larger graph is 14641 nodes through four pipes:
1.96 µs a token for us at 24 workers against the chain's 777 ns, or about
300 ns a pipe hop more than a channel send.

That is the `pipeline` section's `L = 1` finding reached from a different
direction -- there one line removed the pipeline parallelism, here every pipe
being serial does -- and it is the comparison the shape offers rather than a
defect in it: four `std::thread`s dedicated to four stages are a very good
implementation of a four-stage serial chain, and what a `Pipeline` adds over
them is the ability to make a middle pipe parallel, which upstream's own
benchmark never does.

## data_pipeline

Upstream's own `data_pipeline`, which is **not** the measurement the #186/#188
section above reports: that one is this port's per-line slot against a
user-owned buffer, invented here, and this one is
`benchmarks/data_pipeline/taskflow.cpp`'s
`parallel_pipeline_taskflow_4_pipes` translated. Both live in
`benches/data_pipeline.rs`, in two criterion groups -- `data_pipeline` for the
translated shape, `data_pipeline_slot` for the #188 measurement, whose ids are
unchanged -- and the file's module docs say in their first lines which of the
two the "Translated from" line covers. **The decision this records:** hosting
both in one file keeps the ids the #186/#188 section quotes exactly where it
quotes them; renaming the #188 file to `data_pipeline_slot.rs` would have
broken them for no gain, since the two shapes share the token counts, the
sample settings and the `benches/common` body anyway.

The translated shape is a `DataPipeline` of `P = 4` typed pipes over `L = 8`
lines -- upstream's `-l` default -- composed into a `Taskflow` and run once.
Its point is that each pipe's output type differs from its input type:
`Pipeflow& -> int`, then upstream's `int2float`, `float2int` and `int2void`, so
every value crossing a pipe boundary needs somewhere typed to live, which on
this port is #186's per-line slot and upstream is a `std::variant`. Every pipe
is serial, upstream's `-p` default (`"ssssssss"`). Sizes are two points off
upstream's `L = 1 << i`, `i` in `1..=23`, and they are the same 1024 and 16384
the `pipeline` and #186/#188 tables use, so the three pipeline shapes are read
against one another at the same token counts.

Two departures from upstream, both in `benches/data_pipeline.rs`'s own module
docs as well. The per-pipe work is `benches/common`'s shared `dummy`, folded
into the value, rather than upstream's 100-iteration `work_int`/`work_float`
loops -- the same substitution every other shape in this file makes. And
`float2int` reinterprets the `f64`'s bits instead of casting it: upstream's
body ends in a C `(int)` cast, which in Rust is the saturating `as` cast this
crate's `cast_possible_truncation` lint exists to catch and which no benchmark
may `#[allow]` away, and what the measurement needs from that pipe is that its
output type differs from its input type and that its value still depends on
every pipe before it, not the particular arithmetic C performs.

**The baseline is an `mpsc` thread chain**, as in the two sections above and
for the same reason, with one difference: its three channels are typed as the
pipes are -- `i64`, `f64`, `i64` -- so the baseline pays for upstream's typed
chain too, in the role `tbb::parallel_pipeline` plays upstream's own `tbb.cpp`.
It has no `lines`/`workers` axis, so it is measured once per size. Both sides'
answer is a wrapping checksum of every token's final value, checked against a
sequential fold of the same chain *outside* every measured region, before
either id is registered.

The ids are `data_pipeline/mpsc/<size>` and
`data_pipeline/rustflow/8l_<workers>w/<size>`.

| size | workers | rustflow | mpsc | upstream C++ |
|---|---|---|---|---|
| 1024 | 1 | 1.838 ms ±1.0% | 929.5 µs ±0.6% | not published |
| 1024 | 24 | 1.980 ms ±0.1% | 929.5 µs ±0.6% | not published |
| 16384 | 1 | 29.30 ms ±0.5% | 12.92 ms ±0.4% | not published |
| 16384 | 24 | 31.32 ms ±0.2% | 12.92 ms ±0.4% | not published |

**Upstream's own typed chain is `graph_pipeline`'s result with even the
24-worker gain removed**: 2.0x and 2.3x behind the mpsc chain at one worker,
2.1x and 2.4x at 24, and 24 workers cost 8% and 7% here rather than buying
anything. Four serial pipes over eight lines are four stages of concurrency
whatever the worker count, so the extra 23 workers have nothing to do but
contend for the line locks -- which is what those two rows are.

Per token at 16384 the chain is 789 ns and our line is 1.91 µs at 24 workers,
so a typed hop costs about 280 ns more than a typed channel send. Against
`graph_pipeline`'s 300 ns on this same pass, the two shapes' per-hop overheads
agree to within what their pipes do differently, which is the thing to want
from two ports of one scheduler: the figure belongs to this crate's per-pipe
dispatch rather than to either benchmark's own arithmetic, and the typed slot
the `DataPipeline` adds over the untyped `Pipeline` is inside that agreement
rather than visible above it.

## merge_sort

`N` random `f64`s sorted by the recursion written out rather than by a
library call: upstream's `merge_sort/taskflow.cpp` splits a range at its
midpoint, `silent_async`s the right half into a `TaskGroup` of that level's
own, sorts the left half on the calling worker, `corun`s, and merges the two
sorted halves through a second array of the same length before copying them
back. Below 1024 elements -- upstream's own cutoff, unchanged -- a range is
sorted in place and nothing is spawned. The root is one `Executor::spawn`
whose handle is waited, because the recursion has to start on a worker;
upstream nests *two* `executor.async` calls around the same work, the inner
one blocking a worker on its `wait()`, and this port submits one.

**The baseline is `rayon::join`, not `par_sort_unstable_by`**, which is the
one thing that keeps this section from duplicating the `sort` one above.
There both columns are a library's own sort -- `Runtime::sort_by` against
pattern-defeating quicksort -- and the table reads as a comparison of two
sorting algorithms that happen to be parallel. Here both columns run the
*same* merge sort with the same cutoff and the same merge, so what a row
isolates is one `silent_async` plus one `corun` per level against one
`rayon::join`, which is what upstream's own `tbb.cpp` writes on a
`tbb::task_group`. The two sections are worth reading in that order: `sort`
says what the shipped algorithm costs, this one says what the fork costs.

Sizes are two points off upstream's `N = 10..=1e8` sweep, `10_000` and
`1_000_000`. The sweep's first three points are below the 1024-element
cutoff, where this shape degenerates into a single `sort_unstable_by` and
spawns nothing at all -- the `sort` section already measures that case at
`N = 1_000` -- so `10_000` is the first point of the sweep with a recursion
in it, four levels deep and 15 spawned tasks, where the fixed cost of a run
still shows; `1_000_000` is ten levels and 1023 spawned tasks, where the
per-task cost dominates.

**What bounds the depth is upstream's cutoff, and the task count that falls
out of it is what makes this shape safe where `fibonacci`'s `rustflow-group`
id is not.** A range is halved until it is at most 1024 long, so the
recursion is `ceil(log2(N / 1024))` levels and the tasks outstanding at any
moment are bounded by the 1023 of the larger size. That is what the #359
section above measures the bound against: a `TaskGroup::corun` nests a frame
per outstanding *task*, about a kilobyte of stack each in a release build, so a
thousand `msort` frames is a megabyte of the 2 MiB a worker gets -- inside it,
checked at both worker counts in a release build rather than assumed -- which
is what made this one of the five nested shapes #342 could A/B the
preempted-parent resume on at its published sizes, where the `integrate`
section below is the one that could not.

**How the two halves reach the two branches, without `unsafe`.** Upstream's
child captures the whole `std::vector<double>` by reference and is trusted to
touch only `[mid, end)`; a `TaskGroup::silent_async` body here is
`FnOnce() + Send + 'static` and so cannot hold the `&mut [f64]` that
`split_at_mut` would give it. The `rustflow` column therefore keeps the array
as a slice of `AtomicU64` holding `f64::to_bits` patterns, shared by `Arc`,
and passes upstream's `begin`/`mid`/`end` indices -- the departure the
`for_each` section above records, for the same reason, and a relaxed load and
store are the instructions a plain load and store are on this target.
Disjointness is then a property of the index arithmetic rather than something
the compiler checks, which is exactly what the untimed assertion below is for.
The `rayon` column does get real `&mut [f64]` halves out of `split_at_mut`,
since a `join` body may borrow the frame that forked it; that asymmetry is in
the baseline's favour, like the `Vec<f64>` the `for_each` baseline keeps.

Each iteration runs on a fresh shuffle through `Bencher::iter_batched_ref`,
reusing the `sort` section's pattern for its reason: the input is mutated in
place, so criterion's plain `iter` would time a sort of already-sorted data
on every sample after the first. Each id owns one array, allocated before
anything is timed and re-shuffled by that untimed setup, for the reason the
"#366 change" section at the end of this file measures. The *merge* buffer is
a different question and is still allocated inside the timed region on both
sides, where upstream's `std::vector<T> buffer(data.size())` is: it is part
of the algorithm being compared rather than part of its input, and both
columns pay it.

The check is in the untimed part of the run, before any id of a size is
timed: both columns sort one shuffle at each size and worker count, and each
output is compared element by element against a copy sorted by
`sort_unstable_by` on one thread. The comparison is exact rather than a
tolerance -- a sort permutes its input and invents no values, so the sorted
sequence of a multiset of `f64`s is unique whatever order the halves were
merged in -- and a mutation confirms it bites: a left branch handed one
element fewer fails at index 762 of the 10_000 rather than producing a figure.

The eight ids are `merge_sort/{rustflow,rayon}/{1w,24w}/{10000,1000000}`.

| size | workers | rustflow | rayon | upstream C++ |
|---|---|---|---|---|
| 10000 | 1 | 258.6 µs ±0.3% | 242.9 µs ±0.1% | not published |
| 10000 | 24 | 869.0 µs ±2.1% | 139.4 µs ±1.4% | not published |
| 1000000 | 1 | 55.33 ms ±0.3% | 55.33 ms ±0.1% | not published |
| 1000000 | 24 | 17.31 ms ±2.3% | 18.91 ms ±2.4% | not published |

Measured in the "#366 change" pass at the end of this file, on the same quiet
box and under the same conditions "What is measured" names for the pass that
first filled this table. `rustflow/24w/10000` is the widest of the eight at
±2.1%, with one of 20 measurements flagged as an outlier.

**The million-element one-worker row is now a dead tie, and the correction is
the harness's rather than the scheduler's.** 55.331 ms against 55.334 ms --
three microseconds apart on a 55 millisecond run. This section used to read
57.14 against 59.86 there and called the 4.6% a merge-loop difference it could
not explain; it did not need explaining, because it was not there. Both
columns were being charged for a per-iteration 8 MB input on top of the 8 MB
merge buffer each of them allocates *inside* the timed region -- the live pair
the "#366 change" section finds to be the expensive case, rather than the free
itself -- and taking the input out moved our id by 5.2% and rayon's by 6.9%,
in the same direction and by amounts that leave the two on top of each other.
What the row says now is that the merge through a slice of `AtomicU64` with
relaxed loads and stores costs the same as a merge through real `&mut [f64]`
halves on this target, which is what the module docs claim for the departure
and is the first row in this file to measure it.

**The 24-worker million-element row still goes the port's way, and by the
margin the replaced table read.** 17.31 ms against 18.91 ms, 8.5% apart with
intervals of ±2.3% and ±2.4%, where the figures this pass replaced were
17.39 and 19.16, 9.3% apart. Only our column moved with the harness change
there (−11.4% against rayon's +3.6%, the latter inside its own interval), so
the agreement of the two ratios is the useful part: at a million elements the
recursion is 1023 spawned tasks against some twenty million compare-and-copy
operations, and whatever this row is reading, it is not the fork.

**The 10_000-element rows are the ones that answer the question this section
exists for, and they go the other way.** 6.5% behind at one worker over 15
spawned tasks, and 6.2x behind at 24 workers, where our 869.0 µs is 3.4x our
own one-worker time and rayon's 139.4 µs is 1.7x faster than its own. Fifteen
tasks whose leaves are 1024-element sorts, distributed by stealing out of the
shared buffers with a frame parked in `corun` at every level, is the `skynet`
and `integrate` finding at a thirtieth of their task count -- #342 counted
4152 nested tear-downs here and no preempted parent among them, so the cost is
the buffers and the frames rather than a re-queue. **The buffers are not what
the 24-worker rows are, though**, and the "#380 measurement" section below is
where that is measured: #377's `rustflow-runtime-group` id takes the buffers
out of this recursion and reads 907.76 µs and 73.408 ms against this column's
535.29 µs and 15.606 ms in the same process. What it cannot keep is the
ordering -- the group holds the `Runtime` the left half needs, so the right
half is submitted after the left is sorted rather than before -- and that
section's ablation puts the whole of the difference on the ordering. What the
million-element rows add is the other half of it: this fork is expensive per
task and the per task cost is all it is, so a shape with enough work per level
buries it entirely. **This is also the shape that resolves nothing about the
node changes**, which is the same statement from the other side: "The #382
measurement" at the end of this file A/B'd all four ids here across #376 and
#379 as the regression check those changes needed, and every reading is inside
one of the two sides' own spreads -- a block off a free node is not visible
behind twenty million compares, or behind fifteen tasks whose leaves are
1024-element sorts.

## integrate

Adaptive quadrature of `f(x) = (x^2 + 1) x` over `[0, V]`, and the second of
the two **data corun recursions**: upstream's `spawn_async` bisects the
interval, compares the two-trapezoid estimate against the one it was handed,
and if they differ by at least `epsilon = 1.0e-9` submits the left half as a
`silent_async` child of a `TaskGroup` of that interval's own, refines the
right half on the calling worker, `corun`s, and returns the sum of the two
areas. The bisection, the error bound and the `f64` arithmetic are upstream's
unchanged, down to the two one-sided comparisons it spells the bound with. As
in `merge_sort`, the root is one `Executor::spawn` whose handle is waited.
The baseline is `rayon::join`, which is the same recursion with the fork
replaced and what upstream's `tbb.cpp` writes on a `tbb::task_group`.

**Nothing is shared between the two branches**, which is what makes this the
cheaper of the two data recursions to port: each half of an interval produces
a single `f64`, so where upstream's child writes `area_x1x0` through a
captured reference into its parent's frame, the port hands it an
`Arc<AtomicU64>` holding an `f64::to_bits` pattern -- the slot `fibonacci`'s
group recursion already uses. `Relaxed` on both ends, ordered by the `corun`.
No `unsafe`, no shared mutable range, and no index discipline for an
assertion to have to check.

**What bounds the depth is the error bound; what bounds the size is the
stack, and the size this shape completes at is two orders of magnitude below
upstream's own sweep.** The recursion stops splitting once two trapezoids
agree with one to within `epsilon`, which for this cubic happens at a width
of about `(2 * epsilon / x)^(1/3)` near `x`: the depth grows as `log(V)`,
which is 10 levels at `V = 1` and 11 at `V = 2`, while the number of
intervals grows as `V^(4/3)` -- 828 spawned tasks at `V = 1`, 1852 at
`V = 2`. It is the task count that binds. Upstream's `main.cpp` walks
`v = 0, 100, .., 2000`, and its first non-degenerate point, `V = 100`, is
some 390_000 spawned tasks; measured in a release build on this box,
**`V = 3` -- 3478 spawned tasks -- overflows the stack of the one worker that
has to pick them up**, and `V = 1` and `V = 2` are what completes there. So the
sizes here are not points on upstream's sweep at all, and that is the finding
rather than a quiet shrink.

The cliff is the nested-corun stack bound the #359 section above measures, and
that section carries this shape's reading of it: `V = 3` demands 2.04 MiB of a
worker's 2 MiB at one worker, which it misses by 45 KiB, where `V = 2` demands
1.09 MiB and fits. At 24 workers the same tree asks 134 KiB of the deepest
stack, and #359 found `V = 3` through `V = 24` all completing there, with 32 the
first to abort -- so **the 24-worker end of this shape is not what the sizes are
chosen against**; the one-worker end is, as it is for every shape in this file.
The demand is a property of the tree and of the frame rather than of one task
count: this recursion carries five `f64`s and a slot and costs 616 bytes a
node, where `skynet`'s ten-slot frame costs 1055. The column in the table
below is a `TaskGroup` column, because upstream's `integrate/taskflow.cpp` is
written on `task_group()` and the port follows it, so the ceiling above is the
id route's; #377 added a second column, `rustflow-runtime-group`, whose groups
are opened on the `Runtime` instead and whose ceiling the "#380 measurement"
section below reports. The resume that was expected to raise that ceiling does not.
#342 measured it: the frames that make this cliff are a `corun`'s rather than
a preempted node's -- 10720 nested tear-downs here and `PREEMPTED` on none of
them -- so **no size in this section moves on account of #342**, and what
lifts the cliff is the route rather than the resume.

The answer is checked twice before any id is timed, both outside every
measured region, because the two checks catch different failures. **Against
the closed form**, `integrate.hpp`'s own `integral_fn`, which upstream never
compares against: the tolerance is an absolute `1e-5`, derived from
`epsilon` rather than picked -- an accepted interval's trapezoid sum sits
within `epsilon / 3` of the true area over it, so a tree of `K` accepted
intervals is within `K * epsilon / 3`, which is `2.8e-7` over the 829 leaves
at `V = 1` and `6.2e-7` over the 1853 at `V = 2` against a measured `8.4e-8`
and `2.6e-7`. Absolute and not relative because `epsilon` is absolute: the
areas here are `0.75` and `6`, and a relative bound would tighten as `V`
falls for no reason the method has. **Against a sequential reference**, the
same recursion with the fork taken out and through the same bisection helper:
the tree's shape depends only on each node's own values and never on which
worker ran it, so the sum is schedule-independent to the last bit, and the
tolerance is a relative `1e-15` -- about five ULP, slack for two inlinings of
one helper and nothing else. That second check is the one that catches a
dropped subtree however small, and a mutation confirms the pair is layered
the way this paragraph claims: a child whose area never reaches its parent's
slot fails the closed-form check outright, while a parent that scales its
child's area by `1 - 1e-12` passes it and fails the reference check at a
relative `4.3e-12`.

The eight ids are `integrate/{rustflow,rayon}/{1w,24w}/{1,2}`.

| size | workers | rustflow | rayon | upstream C++ |
|---|---|---|---|---|
| `V = 1` | 1 | 277.4 µs ±1.5% | 20.02 µs ±0.4% | not published |
| `V = 1` | 24 | 1.287 ms ±2.6% | 30.93 µs ±1.8% | not published |
| `V = 2` | 1 | 636.6 µs ±0.6% | 41.86 µs ±0.6% | not published |
| `V = 2` | 24 | 2.064 ms ±2.5% | 39.24 µs ±1.4% | not published |

4 of 20 measurements were flagged as outliers on `rustflow/1w/1`; no interval
in the table exceeds ±2.6%, which is nothing against the ratios in it.

**This is the widest gap in the file -- 14x and 15x at one worker, 42x and 53x
at 24 -- and it is the per-task constant with nothing in front of it.** A node
bisects an interval, evaluates two trapezoids and compares them: five or six
`f64` operations, where `merge_sort`'s million-element rows hide the same fork
behind twenty million of them. At `V = 2` and one worker, 1852 spawned tasks in
636.6 µs is 344 ns a task against the `rayon::join` column's 22.6 ns. **That
constant is unchanged today, and not because nothing happened to it**: #376
took 9.5% off it and #379 put 12.9% back, which is the largest regression "The
#382 measurement" at the end of this file found and the reason #397 exists.
The same id reads 625.7 µs there (338 ns a task) against the "#380
measurement"'s 648.23 µs (350 ns), which is the few per cent this box moves
between runs on it.

The 24-worker rows then repeat `skynet`'s direction: ours goes from
636.6 µs to 2.064 ms, 3.2x slower for 23 more workers, with each parent holding
a frame in `corun` until its one child lands, while `rayon::join` on the same
tree barely moves (41.86 to 39.24 µs). **What that regression is not is the
shared buffers**, which is this section's one correction from the "#380
measurement" below: the `rustflow-runtime-group` id queues the same children on
the submitting worker's own deque and reads **29.854 ms and 65.129 ms** at 24
workers against this column's 1.4389 and 2.4263 in the same process, where at
one worker it halves it (322.42 µs against 648.23 at `V = 2`). One child
outstanding at a time is what this recursion has, on either route, and giving
23 awake workers a single-job deque to race for is worth twenty-fold here --
the mechanism that section's ablation, width sweep and handoff counts pin. Read with the #359 section above,
the two measurements are one finding from two directions: a nested corun
through this route costs about a kilobyte of stack and a few hundred
nanoseconds a task, and both are the queue destination rather than `corun`
itself.

## The #343 attribution: a chunk costs 15 ns, and the gap is the fan-out

The `for_each` section above reads a 2.17x gap at a million elements and 24
workers and says what to look for: "the factor of nearly two between the two
speed-ups is the partitioner's and the per-chunk dispatch's". #343 took that
apart with counters and ablations, in the shape #125's section is the model
for, and **both halves of that sentence are wrong**. A chunk costs about 15 ns
and there are 547 of them, which is 1.3% of the run; the port's own chunk loop,
fanned out by rayon instead of by this executor, is *faster* than rayon's own
range split on the same body and the same buffer. What is left is two things
that have nothing to do with chunks: **the fan-out reaches only 15 to 19 of the
24 workers** in an uninstrumented-length run, and **most of the published gap
was the benchmark harness's own per-iteration input buffer**, which #366's
section below has since taken out of the timed region and re-measured without.

**No shipped code changed**, and the decision at the end says why. Nothing in
`src/` carries a counter from this run either: everything below was counted
through the public API from a harness outside the repo -- the chunk body counts
its own chunks, and `Executor::make_observer` counts which worker entered each
dispatched task -- so there was nothing a `cfg` was needed for. (A new `cfg`
name would also have to be declared in `Cargo.toml`'s `check-cfg`, since
`unexpected_cfgs` is a warning and the gate denies warnings.) The `for_each`
table above was therefore untouched by *this* run, whose two headline ids were
re-run as published rather than replaced; #366 has since replaced that table
from a pass of its own, and every figure in this section is this run's as
measured.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, where used: 20 samples, 1s warm-up, 3s measurement |
| Date | 2026-11-19 |
| **Load average** | **0.04 to 1.5, and all of it this run's own** |

The box was this run's alone: no sibling agent, nothing else building, and
`/proc/loadavg` was recorded either side of every measurement below (the raw
output names them one by one). Two regimes appear in the tables and the
difference between them turns out to be most of the finding:

- **Regime A**, the one the ablations are read in: the input buffer is
  allocated and written before the timer and freed after it, so what is timed
  is the run. Each variant is one run per rep, variants **rotated** so none is
  always first, medians of 11 to 21 reps, with each side's own min and max
  printed.
- **Regime B**, criterion's: `iter_batched` with `BatchSize::LargeInput` is
  `ceil(iters/1000)`, which is **1** for every sample either id of this shape
  reaches, so each iteration allocates and writes a fresh 8 MB input outside
  the timer and *frees it inside* -- the bench's own closure takes the input by
  value. The `for_each` table was regime B when this run was made; #366 moved
  that shape, `sort` and `merge_sort` to regime A, which is what its section
  below measures.

### The counters: what the chunk loop actually runs

One instrumented run per partitioner, `n = 1000000` at 24 workers, three runs
each, counts and emphatically not timings. "Workers that ran a chunk" is the
number of distinct threads a chunk body was ever called on; "busy / span" is
the sum of the chunk bodies' own durations over the wall span of the whole
dispatch, which is the same quantity read as an effective worker count.

| partitioner (chunk size) | chunks | min / median / max chunk | tasks | workers that ran a chunk | busy / span |
|---|---|---|---|---|---|
| guided, floor 1 (`DefaultPartitioner`) | **547** | 1 / 67 / 20833 | 24 | **17 / 19 / 18** | 12.0 / 16.7 / 13.2 |
| static, chunk 0 | **24** | 41666 / 41667 / 41667 | 24 | 18 / 22 / 18 | 10.7 / 13.0 / 10.8 |
| guided, 4096 | 126 | 1066 / 5647 / 20833 | 24 | 19 / 21 / 17 | 12.8 / 16.9 / 15.3 |
| dynamic, 4096 | 245 | 576 / 4096 / 4096 | 24 | 16 / 20 / 18 | 14.6 / 17.8 / 15.6 |
| dynamic, floor 1 | **1000000** | 1 / 1 / 1 | 24 | 24 | 0.3 |

**A method note that is itself a result.** The first version of this counter
took one shared `Mutex` per chunk, and under it 22 to 24 of the 24 workers ran
a chunk and the dispatch took 1134 to 1342 µs instead of 760 to 1205. The log
is now one `Mutex` *per thread*, so a chunk record is an uncontended lock, and
the worker count fell -- the observer below, the lightest instrument of the
three, reads 15 to 18. The three instruments agree in the direction that
matters: **how many workers arrive is a race against the run finishing**, so a
heavier instrument lengthens the run and lets more of them in. Every count in
this section is from the light log or the observer, and the shared-log figures
are quoted only here, as the control that found the effect.

Three things in that table are the whole section.

- **A chunk allocates nothing.** The arena #144 sized to one node is allocated
  per *task*, not per chunk: `dispatch` builds `w` of them, `w - 1` through
  `Runtime::silent_async` and one run inline, so 547 chunks cost 23 free nodes
  and not 547. Each of those 23 is one `Arc<AsyncNode>`, one one-node arena,
  two boxed closures (the chunk body is boxed by `dispatch` and again by
  `silent_async`) and one `notify_one`. `Scope::spawn` and
  `Scope::spawn_runtime` are called **zero** times on this shape: the bench
  reaches `for_each_by_index` through `AlgorithmBuilder`, whose dispatch
  submits through `Runtime::silent_async`, and `Runtime::scope` is only on the
  borrowed form -- whose ablation below costs nothing measurable anyway.
- **The default partitioner's floor of 1 spends 328 of its 547 chunks on the
  last 1% of the elements.** Guided takes `r / 2w` of whatever remains, so the
  chunk size decays geometrically to the floor; the end of the range is handed
  out a few elements at a time. That end game is 3%, 23% and 26% of the span
  in the three runs. Upstream's own default is the same floor (`chunk_size == 0
  ? 1 : chunk_size`, `DefaultPartitioner = GuidedPartitioner<>`, both confirmed
  against `taskflow/algorithm/partitioner.hpp` at the pinned tag), so this is
  the port reproducing upstream and not a translation slip.
- **`DynamicPartitioner::default()` hands out a million chunks of one
  element.** That is upstream's default too, and it is unusable at this size:
  18.3 ms through rayon's fan-out against 593 µs for the same body under the
  guided cursor. It is in the table because a reader who sets a partitioner but
  not a chunk size needs to see it.

### The fan-out counter: 24 tasks, 15 to 18 workers

The same question asked by a second instrument, with no chunk log at all: an
`Observer` registered on the executor, counting task entries per worker id.
Nine runs, `n = 1000000`, 24 workers:

| | entries | distinct workers | first entries spread over |
|---|---|---|---|
| guided, floor 1 | 25, 25, 25 | **16, 17, 17** / 24 | 90 µs; 85 µs + one at 1.1 ms; 47 µs + two at 1.2 ms |
| static | 25, 25, 24 | **17, 15, 18** / 24 | 83 µs; 74 µs; 72 µs + two at 0.7 ms |
| guided, 4096 | 24, 25, 25 | **15, 15, 17** / 24 | 31 µs + three at 1.2 ms; 90 µs; 57 µs + one at 1.2 ms |

Twenty-five entries is the runtime node plus its 24 chunk tasks, so every task
*is* dispatched and every one does run. But they run on 15 to 18 distinct
workers out of 24, which agrees with the chunk log's own 16 to 22, and the
first-entry times say how: a dozen to seventeen workers enter a task within
30 to 90 µs of each other, and the rest enter one **after the range is already
exhausted** -- the 1.2 ms entries in the table are at the very end of a 1.2 ms
run. A task entered then finds the shared cursor past `n` and returns without
calling the body once.

That is the mechanism, and it is a property of the dispatch shape rather than
of a chunk: there are exactly `w` tasks, all pushed onto *one* worker's queue,
and a worker that misses the steal window has no way to take a share of the
work afterwards. Rayon's split has no such window -- a worker that wakes late
steals half of whatever range is left -- which is why its figures below have a
spread of 1.06x between runs where ours has 1.6x.

### The ablations: what each candidate costs

Regime A, `n = 1000000` at 24 workers, medians of 11 to 21 reps with the
variants rotated. `rayon-*` rows run **the port's own code** under rayon's
thread pool wherever the name says so: `rayon-guided-cursor` is
`GuidedPartitioner::loop_dynamic` over our chunk body, fanned out by
`(0..w).into_par_iter()` instead of by `dispatch`, and `rayon-static-loop` is
`StaticPartitioner::loop_static` with a private cursor per worker. `nobody` is
the shipped dispatch with the body replaced by `black_box(sub.size())`.

| variant | median | min | max |
|---|---|---|---|
| **shipped** (guided, floor 1) | 631 / 607 / 805 / 964 µs | 590 | 1296 |
| static | 786 / 934 µs | 580 | 1598 |
| guided, 4096 | 632 µs | 587 | 804 |
| dynamic, 4096 | 638 µs | 578 | 673 |
| `Runtime::scope` form, guided | 786 µs | 704 | 1002 |
| `Runtime::scope` form, static | 1010 µs | 693 | 1115 |
| **shipped, body removed** | **73.0 µs** | 65 | 80 |
| static, body removed | **65.3 µs** | 59 | 93 |
| one empty runtime node, built and run | **14.3 µs** | 4 | 24 |
| `rayon` (`par_iter_mut`, plain `f64`) | 660 / 647 / 651 / 689 µs | 609 | 1768 |
| `rayon-cells` (rayon's split, our atomics) | 658 / 633 µs | 605 | 889 |
| **`rayon-guided-cursor`** (our cursor, rayon's fan-out) | **610 / 593 / 628 µs** | 561 | 843 |
| `rayon-static-loop` (our static loop, private cursor) | 766 / 558 µs | 543 | 1974 |
| `rayon-dynamic-cursor` (floor 1) | 18295 µs | 17880 | 19005 |
| sequential, our body | 7761 µs | 7525 | 8319 |
| sequential, `*d = d.tan()` | 7634 µs | 7419 | 8228 |

Read as differences:

- **A chunk is 15 ns.** The body-removed pair differ by 547 − 24 = 523 chunks
  and by 7.7 µs: **14.7 ns a chunk**, which cannot contain an allocation
  (#125 measured one 64-byte block allocated by a producer and freed by a
  worker at ~70 ns) and matches a contended cursor acquisition and a closure
  call. Against the 631 µs the shipped path takes, the 547 chunks are **8 µs,
  1.3%**. The issue's premise does not survive this line.
- **The fan-out is 51 µs of the 73.** Take the empty one-node flow off the
  body-removed static run and 24 tasks cost 51 µs of wall time between them --
  about 2 µs a task, of which #125's and #144's numbers say ~0.3 µs is the node
  the producer builds and the rest is the wake-up and the steal.
- **The cursor is not worth removing.** `static` -- 24 chunks, no shared cursor
  at all -- is *slower* than the guided floor-1 cursor with its 547 chunks
  (786/934 against 631/607/805/964), and the counters say why: with 15 to 18
  workers arriving, a fixed `1/24` share each means one worker runs two or
  three shares back to back, and static's own end game starts at 51 to 56% of
  its span. Under rayon's fan-out, where every worker does arrive, the cursor
  costs 610 against the private-cursor 766/558 -- inside that side's own
  spread, so **≤ 60 ns a chunk acquisition and not a result**.
- **The scope's join is free.** 786 against 805 for guided, 1010 against 934
  for static: both inside either side's spread. `Runtime::scope`'s
  `catch_unwind` plus `try_corun` does not show against a 24-way fan-out.
- **Our partitioner beats rayon's splitter.** 610 µs for the port's guided
  cursor under rayon's fan-out against 658 µs for rayon's own split over the
  same atomic cells. **The chunk loop this issue was about is not the gap**;
  the executor-side difference is 631 against 610 in the best-measured run and
  up to 964 against 628 in the worst, which is the same lottery the fan-out
  counter counts.
- **The body is not the gap either.** Our atomic-cell body is 1.7% off
  `*d = d.tan()` sequentially (7761 against 7634 µs), so the `AtomicU64`
  departure `benches/for_each.rs` documents costs the comparison nothing.

### Most of the published gap is the harness's input buffer

Regime B is where the `for_each` table's figures come from, and it carries a
term that has nothing to do with either scheduler. Re-running the two
published ids today reproduces them: 1.6500 ms and 723.7 µs, then 1.6349 ms
and 737.2 µs on a second pass, against the table's 1.658 ms and 765.6 µs. The
same variants under regime B in the harness say where that comes from:

| variant | regime A | B, one fresh buffer per iteration | B, plus the bench's own `cells(&inputs(n))` setup | criterion itself |
|---|---|---|---|---|
| shipped | 631 µs | 949 / 1056 µs | **1558 / 1771 µs** | **1635 / 1650 µs** |
| `rayon` | 660 µs | 706 / 720 µs | 888 / 964 µs | **724 / 737 µs** |
| `rayon-cells` | 658 µs | 775 µs | **1636 µs** | n/a |

The third column is the decisive one: `rayon-cells` runs **no code of this
crate at all** -- rayon's split, rayon's pool, our atomic cells -- and it goes
from 775 µs to 1636 µs when the input is built the way the bench builds the
`rustflow` column's, +861 µs. The `rustflow` column's setup is
`cells(&inputs(size))`: an 8 MB `Vec<f64>` allocated, written and dropped, and
an 8 MB `Vec<AtomicU64>` built from it, **per iteration**, where the `rayon`
column's is `inputs(size)` alone. Churning the second buffer, not running the
chunk loop, is what the extra ~700 µs buys.

Put as two differences: criterion's figure for the `rustflow` column is
**~1000 µs above** the same program in regime A, and its figure for the
`rayon` column is **~80 µs above** it. The published gap of ~900 µs is the
difference between those two harness terms, and what is left in regime A is a
median ratio between **0.93x and 1.40x across four runs** whose own
between-run spread (1.6x on our side, 1.06x on rayon's) is wider than the
difference. **The honest reading of `for_each/24w/1000000` is a tie with a
worse spread, not 2.17x.** `reduce_sum` and `scan` are not affected the same
way -- both use plain `Bencher::iter` over an input allocated once -- so this
is a `for_each`-shaped finding and not a suite-wide correction.

**#366 is where that tie was published.** It audited all 196 ids for the same
pattern, found it in the three shapes that build an input per iteration --
this one, `sort` and `merge_sort` -- moved the input out of the timed region
on each and re-measured: `for_each/24w/1000000` reads **622.4 µs against the
baseline's 720.7 µs**, which is this subsection's own prediction. It also
names the mechanism this one could only point at: what costs is the **pair**
of 8 MB blocks the `rustflow` setup kept live per iteration, 0.73 ms of it
inside the clock as the second block's release and 0.28 ms around it. A single
8 MB input freed inside the timed region costs nothing measurable, which is
why the `rayon` column of the same row never carried the term.

### At a thousand elements the shape is four fifths overhead

The other published row, `n = 1000` at 24 workers, is a different regime and
the counters are blunter about it. The whole job is 1000 `tan` calls: 10 to
12 µs on one thread, measured. Counters, one run each:

| partitioner | chunks | median chunk | workers that ran a chunk | workers that entered a task |
|---|---|---|---|---|
| guided, floor 1 | **218** | 2 | 16 | **7 / 24** |
| static | 24 | 42 | 6 | **5 / 24** |
| dynamic, floor 1 | 1000 | 1 | 13 | -- |
| guided, 4096 | **1** (the `n <= chunk_size` early-out) | 1000 | 1 | -- |

And regime A timings, medians of 15 reps: shipped **63.5 µs**, static 43.1,
guided-4096 **22.2**, shipped body removed **38.2**, static body removed 38.8,
one empty runtime node 7.3, sequential 12.1.

- **218 chunks for 1000 elements**, median width 2, and the two body-removed
  rows are indistinguishable (38.2 against 38.8 for 218 chunks against 24), so
  again the chunks are free and the 24-task fan-out is the cost: of the 63.5 µs
  the shipped path takes, 12 µs is the work and 38 µs is the dispatch with the
  body removed -- 31 µs of that above the empty one-node flow, to hand 24 tasks
  to the 7 of 24 workers that arrive.
- **A chunk size turns the whole thing off and wins.** `guided-4096` takes
  `make_for_each_by_index_task`'s `n <= part.chunk_size()` branch -- one
  sequential call, no tasks, no cursor -- for 22.2 µs against the shipped
  path's 63.5 and the published rayon column's 31.5. A size sweep in the same
  regime puts the crossover between 1000 and 10000 elements: at `n = 10000`
  shipped is 71.6 µs against 109.8 sequential, and at `n = 100000` 149.8
  against 825.0.

### The decision: declined, and what would reopen it

**Nothing ships.** The bar is the issue's own -- a change needs a mechanism the
counters name *and* an A/B across `for_each`, `reduce_sum`, `primes`,
`mandelbrot` and `sort` outside both spreads -- and no candidate reaches the
first half of it:

- The per-chunk cost this issue was about is 15 ns and 1.3% of the run. There
  is nothing to win there, and the counters rather than a clock are what say
  so.
- The one knob that moves the clock is the guided chunk floor, and it is not
  the port's to choose: `chunk_size == 0 ? 1 : chunk_size` and
  `DefaultPartitioner = GuidedPartitioner<>` are upstream's at the pinned tag,
  a caller who wants a wider floor passes a chunk size, and the API to pass one
  is already here.
  Changing a ported default to flatter a benchmark is the wrong trade; what the
  measurement earns is this section saying what the default costs and where the
  crossover is.
- The mechanism the counters *do* name -- `w` tasks on one queue reaching 15 to
  19 of 24 workers -- is not reachable from `src/algorithm/for_each_index.rs`
  at all. It is the steal loop and the notifier, i.e. #60's and #102's
  territory, and the next measurement was theirs: count, inside
  `WorkerThread`'s steal loop, how many of the `w - 1` pokes wake a worker that
  then fails to steal a task that is sitting in the victim's deque, and whether
  a poke spent on an already-awake worker is what the late arrivals are
  missing. **"The #367 measurement" below is that count**, and the answer is
  that the pokes all land: every worker wakes, none parks on available work,
  and what is left is the single queue the `w` tasks are pushed onto.

What this does close is the question #60's last paragraph left open. "A
splittable source range" is exactly what `for_each_by_index` with a
partitioner is, and the measurement says the split itself was never the
problem: this port's chunk loop is rayon's equal on the same fan-out. The gap
that remains on a fan-out shape is **how the first `w` tasks reach `w`
workers**, which is the same handoff that section priced at ~100 ns a task and
not a per-item cost at all.

## The #366 change: three tables stopped timing their own input

The section above found its control by accident. `rayon-cells` runs **no code
of this crate at all** -- rayon's split, rayon's pool, our atomic cells -- and
it read 775 µs on its own against 1636 µs when its input was built the way
`benches/for_each.rs` built the `rustflow` column's. A harness term that large
is not a background detail of a table whose headline gap was 900 µs, so this
pass audited every id in the suite for the pattern, took the input out of the
timed region where it found it, and re-measured.

**No shipped code changed.** Nothing under `src/` is touched by this section,
no bench's size, worker count, sample settings or id name changed, and
`cargo bench -- --list` prints the same 196 ids it did before. What changed is
three files' timed regions. **A figure that moves below is therefore the
artefact leaving, not the port getting faster**, and the sections those
figures live in say so where the move was large enough to change what they
claim.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id -- unchanged, every target |
| Date | 2026-11-19 |
| **Load average, before run** | **`for_each` 0.17, `sort` 5.49, `merge_sort` 3.75** |
| **Load average, after run** | **`for_each` 2.25, `sort` 6.58, `merge_sort` 4.78** |

The box was this pass's alone: no sibling agent, nothing else building, and
`/proc/loadavg` read either side of every run (the raw logs name them one by
one). A shape's "before" reading is its predecessor's decaying tail rather
than zero, exactly as the pass "What is measured" above describes, whose
conditions this one matches: every id is **one** run, criterion's twenty
samples are the statistic, and the quoted ± is half the width of criterion's
95% confidence interval for the mean.

### The audit: 196 ids, and the three shapes that had it

Read "setup inside the timed region" as: the timed closure allocates, fills or
frees a buffer that is the shape's *input* rather than part of the work being
compared. Building the taskflow inside the timed region is not that -- it is
what "ours includes building the graph" means, and it is upstream's own
measured region.

| group | ids | verdict |
|---|---|---|
| `async_task` | 12 | clean -- no input buffer at all; the `N` submissions are the subject |
| `binary_tree` | 8 | clean -- no input buffer; the graph is the measured subject |
| `black_scholes` | 8 | clean -- `Options::generate` and both price buffers allocated once per id |
| `data_pipeline` | 6 | **inside, left** -- the first pipe *is* the token source, and a `DataPipeline` cannot be re-run: its token count is consumed |
| `data_pipeline_slot` | 16 | **inside, left** -- as `data_pipeline`; the per-line slot is the measured subject |
| `embarrassing_parallelism` | 10 | clean -- no input buffer; `rustflow-build` times a build and a drop deliberately |
| `fibonacci` | 12 | clean -- no input buffer |
| `for_each` | 8 | **inside, moved** -- a `Vec<f64>` and a `Vec<AtomicU64>` per iteration, 8 MB each at the larger size |
| `graph_pipeline` | 6 | **inside, left** -- the `LevelGraph` input is outside; the pipeline and its per-run accumulator are inside and cannot be reused, and the `mpsc` baseline spawns its own threads there by design |
| `graph_traversal` | 8 | clean -- `LevelGraph::new` once per id outside |
| `integrate` | 8 | clean -- no buffer; the input is one `f64` |
| `linear_chain` | 4 | clean -- no input buffer |
| `mandelbrot` | 8 | clean -- the image is allocated once per id and every sample overwrites all of it |
| `matrix_multiplication` | 8 | clean -- all three matrices allocated once per id; the init phases inside are upstream's own work |
| `merge_sort` | 8 | **inside, moved** -- one input per iteration, 8 MB at the larger size; the *merge* buffer stays, where upstream's is |
| `nqueens` | 8 | clean -- the board is `side` bytes, upstream's own, and the recursion allocates one per node regardless |
| `pipeline` | 14 | **inside, left** -- as `data_pipeline` |
| `primes` | 8 | clean -- no buffer; the input is one integer |
| `reduce_sum` | 8 | clean -- one read-only vector per id, allocated outside |
| `scan` | 6 | clean -- input and output allocated once per id |
| `skynet` | 8 | clean -- no input buffer |
| `sort` | 8 | **inside, moved** -- one input per iteration, 8 MB at the larger size |
| `wavefront` | 6 | clean -- the matrix is allocated once per id, and its sweep is a fixed point so reuse is sound |

So 24 of the 196 ids had it and all 24 are the three shapes that mutate their
input in place -- which is the same reason they were the three already using
`iter_batched`. The four pipeline groups keep theirs, with the reason in the
rows above: a pipeline's input is the token stream its first pipe produces,
and there is nothing to hoist.

### What changed in the three, and why `PerIteration`

Each of the 24 ids now owns **one** buffer, allocated before anything is timed
-- where `mandelbrot`'s image and `matrix_multiplication`'s matrices already
were, and where upstream allocates `vec` in `main` -- and
`Bencher::iter_batched_ref`'s untimed setup rewrites it from the same
generator between iterations. Two things move out of the timed region
together: the allocation and fill, which `iter_batched` already kept out, and
the **free**, which it did not -- its timed closure takes the input by value,
so the input's destructor runs inside the measured region.
`iter_batched_ref` drops it after the clock stops.

**The batch size is `BatchSize::PerIteration`, and with one buffer per id it
is the only correct one.** `LargeInput` is `ceil(iters/1000)` and `SmallInput`
`ceil(iters/10)`: a batch of `k` would call the setup `k` times before timing
`k` runs, which over one shared buffer means one effective re-shuffle and `k`
sorts of already-sorted data. `PerIteration` pins the batch to 1, so the
guarantee does not depend on how many iterations criterion picks. It costs two
`Instant::now` calls an iteration, about 40 ns, which against the shortest of
these ids -- 10.4 µs -- is under half a percent, and both the before and the
after figures below are flat-sampled means, so the two are comparable.

The one wrinkle is that a `rayon` column needs `&mut [f64]` while its buffer
has to be reachable from the untimed setup as well, which a `RefCell` around
the buffer settles: nothing but the benchmark thread ever borrows it, the
borrow is a flag, and `par_iter_mut` and `par_sort_unstable_by` still get a
real `&mut`. The `rustflow` columns need no such thing -- their arrays are
already shared through an `Arc`, for the reason their module docs give.

### What the artefact was, taken apart

One shape, one id, four harnesses, `for_each/rustflow/24w/1000000`. Each row
is one criterion run of that id on this box; nothing differs between them but
the input's lifetime. The shipped `rustflow` setup was
`cells(&inputs(size))`: an 8 MB `Vec<f64>` allocated, filled and dropped, and
an 8 MB `Vec<AtomicU64>` built from it -- **two** blocks per iteration, where
the `rayon` column's `inputs(size)` is one.

| harness | buffers per iteration | input freed | figure |
|---|---|---|---|
| `iter_batched`, `LargeInput` -- what the table used to be measured through | 2 | inside | 1.634 ms |
| `iter_batched_ref`, `PerIteration` -- same setup, free after the clock stops | 2 | outside | 908.8 µs |
| `iter_batched`, `LargeInput` -- the cells built straight from the generator | 1 | inside | 626.4 µs |
| `iter_batched_ref`, `PerIteration` -- **one** buffer per id, refilled untimed | 0 | n/a | 622.4 µs |

**It is the pair that costs, not the free.** Row 3 is the control that says
so: it frees an 8 MB input inside the timed region exactly as row 1 does and
pays nothing for it, because an allocator handed the same size back and forth
recycles the chunk, where two live 8 MB blocks per iteration leave it
releasing and re-taking pages from the kernel every time. That is also why the
`rayon` column of this shape never moved while ours fell by 62% -- it churns
one block an iteration, which is the cheap case -- and why the 10_000-element
`merge_sort` ids, whose inputs are 80 KB, show nothing at all.

So the ~1.0 ms this id shed is one term with two halves: with the pair in
place, 0.73 ms of it fell inside the clock as the second block's free and
0.28 ms fell around it as pages the timed pass then had to walk. Criterion
keeps a per-iteration allocation out of the *clock*; it cannot keep it out of
the *machine*, which is why rows 3 and 4 and not row 2 are the artefact-free
ones. The three figures that agree -- 626.4 µs, 622.4 µs and #343's own
regime A at 631 µs, measured by three different harnesses -- are the check
that nothing else moved with the harness.

### The before and the after, all 24 ids

Each side is one run of each id, the before on `origin/main` and the after on
this change, back to back on the quiet box above, with `change` as criterion's
own comparison of the two and `p` its own test. Nothing below is a repeat
sweep or a best of several.

| id | before | after | change | p |
|---|---|---|---|---|
| `for_each/rustflow/1w/1000` | 14.35 µs | 14.39 µs | +0.4% | 0.29 |
| `for_each/rayon/1w/1000` | 10.54 µs | 10.37 µs | −0.5% | 0.44 |
| `for_each/rustflow/1w/1000000` | 8.762 ms | 8.088 ms | **−7.7%** | 0.00 |
| `for_each/rayon/1w/1000000` | 7.746 ms | 7.875 ms | +1.3% | 0.00 |
| `for_each/rustflow/24w/1000` | 45.68 µs | 45.12 µs | −1.4% | 0.28 |
| `for_each/rayon/24w/1000` | 30.39 µs | 28.47 µs | −6.2% | 0.00 |
| `for_each/rustflow/24w/1000000` | 1.634 ms | 622.4 µs | **−61.7%** | 0.00 |
| `for_each/rayon/24w/1000000` | 730.4 µs | 720.7 µs | +0.3% | 0.75 |
| `sort/rustflow/1w/1000` | 19.30 µs | 19.51 µs | +1.1% | 0.01 |
| `sort/rayon/1w/1000` | 13.03 µs | 12.38 µs | −5.0% | 0.00 |
| `sort/rustflow/1w/1000000` | 36.12 ms | 36.26 ms | +0.4% | 0.00 |
| `sort/rayon/1w/1000000` | 24.95 ms | 24.84 ms | −0.2% | 0.03 |
| `sort/rustflow/24w/1000` | 21.77 µs | 23.00 µs | +5.6% | 0.00 |
| `sort/rayon/24w/1000` | 16.06 µs | 16.12 µs | +0.7% | 0.45 |
| `sort/rustflow/24w/1000000` | 14.61 ms | 15.51 ms | −1.6% | 0.76 |
| `sort/rayon/24w/1000000` | 3.360 ms | 3.370 ms | +0.8% | 0.09 |
| `merge_sort/rustflow/1w/10000` | 270.7 µs | 258.6 µs | −5.0% | 0.00 |
| `merge_sort/rayon/1w/10000` | 249.0 µs | 242.9 µs | −2.3% | 0.00 |
| `merge_sort/rustflow/1w/1000000` | 58.36 ms | 55.33 ms | **−5.2%** | 0.00 |
| `merge_sort/rayon/1w/1000000` | 59.43 ms | 55.33 ms | **−6.9%** | 0.00 |
| `merge_sort/rustflow/24w/10000` | 840.7 µs | 869.0 µs | +4.4% | 0.04 |
| `merge_sort/rayon/24w/10000` | 142.4 µs | 139.4 µs | −1.6% | 0.05 |
| `merge_sort/rustflow/24w/1000000` | 19.73 ms | 17.31 ms | **−11.4%** | 0.00 |
| `merge_sort/rayon/24w/1000000` | 18.25 ms | 18.91 ms | +3.6% | 0.07 |

`change` is criterion's own figure against the run before it, which is why it
is not always the ratio of the two columns to the digit: it compares sample
distributions, not the two point estimates. A difference inside an id's own
interval is not a result -- the rule this file's "What is measured" states for
every A/B in it -- and `sort/rustflow/24w/1000000` is the one id of the 24 too
wide to read either way, ±7.8% before and after.

Four things in that table are worth naming.

- **Three ids moved by more than 7%**, and all three are a million-element
  input: `for_each`'s two `rustflow` ids and `merge_sort/rustflow/24w/1000000`.
  Three more moved between 5% and 7%, two of them the million-element
  one-worker `merge_sort` pair. A 10_000-element `merge_sort` input is 80 KB,
  served out of the allocator's own arena, and nothing there moves beyond this
  box's drift.
- **The 1000-element ids moved by up to 6% in both directions**, which is that
  drift and not this change: there is no 8 MB block anywhere near them, their
  inputs are 8 KB, and `for_each/rayon/24w/1000` reading 6.2% faster has the
  same standing as `sort/rustflow/24w/1000` reading 5.6% slower.
- **`sort` barely moved anywhere**, which is the control inside this pass. Its
  input is a single `Vec<f64>` on both sides rather than `for_each`'s pair, so
  there is no alternating allocation to pay for, and at a million elements the
  sort itself is 36 ms -- an order of magnitude more than the term that was
  hiding in it. Its table is replaced all the same, because its figures
  predated the quiet box.
- **Three ids read slower by more than 2%**: `sort/rustflow/24w/1000` by 5.6%,
  `merge_sort/rustflow/24w/10000` by 4.4% and
  `merge_sort/rayon/24w/1000000` by 3.6%, the last inside its own ±2.4% at
  p = 0.07. For the two small ones the per-iteration clock pair is a larger
  share of a 20 µs run and criterion's flat sampling has fewer iterations to
  average within a sample, which is the price named above, paid where it shows.

### What this does not say

It does not say the port got faster: `git diff` for this change touches
`benches/` and this file. It does not revisit #343's decision, which stands --
the per-chunk cost is still 15 ns, and the fan-out still reaches 15 to 19 of 24
workers under the instrument that counted them, which "The #367 measurement"
below takes apart. And
it does not make the 24-worker `for_each` row a win for the port: #343
measured that id's between-run spread at 1.6x on our side against 1.06x on
rayon's, so 622 µs against 721 µs is the tie that section predicted, now
published as one.

## The #367 measurement: the wake-ups are not the ceiling, the one queue is

**The notifier wake-ups on the worker path are not the ceiling.** Every one of
the 24 workers is woken on every run of the fan-out, every wake-up is
delivered, and no worker ever parks while a job is queued anywhere -- the
counters below put all three of those at the limit rather than near it. What
is left when the wake-ups are excluded is the **steal** side: the `w - 1`
chunk tasks of a fan-out all go onto **one** worker's deque, pushed one at a
time at about 2 µs each, so each is taken by whichever already-awake thief
wins the race for that single queue, and a worker that loses has nothing left
to find. The number therefore moves with how many threads the box has spare
and with the dispatch's *shape* -- a flat `w`-way push against a tree that
re-splits on the stealing worker -- and not with the partitioner, the chunk
count, the submission path or the wake count.

This is the measurement #343's last subsection asked for, in the same style:
counters, ablations, and a decision at the end. It is also the gate on #378
("One wake per fan-out: a bulk push on the worker path"), and the answer is
that #378 ships no code: the batched poke was implemented and counted here,
and it changes neither the worker count nor the submission's own cost.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | not used: this section is counts and harness wall clocks, as #343's is |
| Date | 2026-11-19 |
| **Load average** | **1.02 to 1.45 before and after every shape below, with `/proc/pressure/cpu`'s `some avg10` at 0.00 and 2 runnable threads at rest** |

The box was this run's alone, and `/proc/loadavg` was read either side of every
shape (the raw logs name each pair). The one-minute figure never fell below 1.0
because it is still decaying from this run's own `cargo build`; the pressure
counter and the runnable-task field are the ones that say the box was idle.
**One shape was measured while a build really was still finishing, at a load
average of 3.90 decaying to 2.07, and it is quoted below because the
difference it makes is itself a result**: the same program that reaches 19 to
21 of 24 workers on the quiet box reached 13 to 17 under that decay.

Every count below is a **delta across one run** of one fan-out: the pool is
quiesced first (`Executor::num_waiters() == 24` confirmed before each window,
and it was 24 every time), both snapshots are taken on the submitting thread,
and the counters are per-thread cache-padded words incremented with a relaxed
`fetch_add` by their owner only. Nothing was committed: `src/probe.rs` and the
call sites in `notifier.rs`, `worker.rs`, `queue.rs`, `runtime.rs` and
`for_each_index.rs` were reverted before the gate ran, which is #343's and
#102's method. The shipped diff of this measurement is this section and a
`DESIGN.md` entry.

### The wake accounting: 24 pokes, 24 unparks, 24 wake-ups, 0 lost

`for_each_by_index` over a million elements at 24 workers, default guided
partitioner, through the same `Taskflow` the `for_each` table's `rustflow` id
builds. Twelve consecutive runs, the pool parked between each:

| counter | per run | what it is |
|---|---|---|
| workers parked when the window opened | **24 / 24** | `Executor::num_waiters()` |
| `notify_one` calls | **24** | 23 from the fan-out's `silent_async` calls, 1 from the tear-down |
| of them, popping a parked waiter | **23 or 24** | the notifier's waiter stack |
| of them, resolving a pre-waiter's ticket | 0 or 1 | a worker caught inside the two-phase window |
| of them, finding the notifier empty | 0 or 1 | a wake spent on nobody |
| `notify_n` calls | 1, asking for 1 | the run's own single source node, through the buffers |
| `unpark` calls issued | **24 or 25** | `Thread::unpark`, counted at the call |
| parks that returned | **23 to 25** | every worker woke |
| **parks taken while any queue held a job** | **0** | summed over every worker queue and buffer, at every park |
| wake-ups whose next sweep found a job | 13 to 21 | |
| wake-ups whose next sweep found nothing | 0 to 4 | |
| steal attempts | 1988 to 4741 | |
| steal attempts that took a job | 21 to 25 | 24 tasks exist |
| `explore_task` calls that spent the whole budget | 0 to 14 | 62 attempts, then 150 more with a yield |

Three of those rows close the question the issue was filed on.

- **The pokes are not lost and they are not wasted.** 24 `notify_one` calls
  produce 24 or 25 `unpark` calls and 23 to 25 returned parks: the submission
  wakes the whole pool, one worker per push, and at most one poke per run is
  spent on an empty notifier. There is no notification to add and none to
  save.
- **No worker ever sleeps on available work.** The "parked while any queue
  held a job" counter sums every worker deque and every buffer at the moment
  each park is committed, and it reads **zero** in every run of every shape in
  this section. A worker that loses is not a worker that missed a wake-up; it
  is a worker that looked, found the queue genuinely empty, and parked again.
  The two-phase guard in `wait_for_task` is doing exactly what it is for.
- **The jobs all get taken, by fewer workers than there are jobs.** Steal hits
  match the task count every run, but the per-worker breakdown shows a handful
  of workers taking two, three or four while four to nine take none, and the
  losers are the ones with the long steal counts: a winner's `sticky_victim`
  already points at the producer's queue and costs it 6 to 100 attempts, while
  a loser draws victims at random out of `num_queues() == 29` and spends 150
  to 285 before giving up.

The observer that counts *which* worker entered each task reads 13 to 21
distinct workers over those twelve runs, with first entries 5 to 92 µs apart
and the occasional straggler at 0.4 to 1.4 ms -- a task entered that late
finds the shared cursor past the end and returns without calling the body
once, which is #343's finding reproduced. Under the decaying build load the
same twelve runs read 13 to 18.

### What the submission costs, and what one wake per fan-out buys

The same question asked of a shape with no cursor in it: a runtime task that
hands the pool `w` bodies of a fixed length and runs the last one itself, with
the body marking its own worker. The runtime body times its own push loop,
which is the producer's serial cost. Four submission paths, three body
lengths, three runs each:

| path | what the submission is | push window, 23-24 tasks | distinct workers, 24 bodies of 20 / 100 / 500 µs |
|---|---|---|---|
| worker, poke per task | 23 × (`push` + `notify_one`) | **45 to 60 µs** | 16-18 / 17-23 / 17-20 |
| worker, one batched poke | 23 × `push`, then one `notify_n(23)` | **44 to 59 µs** | 19-21 / 18-23 / 17-19 |
| external, poke per task | 24 × (`push_bulk(1)` + `notify_n(1)`) | 39 to 58 µs | 18-21 / 18-23 / 17-20 |
| external, one bulk poke | one `push_bulk(24)` + one `notify_n(24)` | **under 1 µs** | 20-21 / 21-23 / 13-19 |

- **A fan-out task costs the submitter about 2 µs**, and that is the same
  whether the wake goes with each push or once at the end: 45-60 µs against
  44-59 µs for the same 23 tasks. The `unpark` is not what the push loop is
  made of -- the node, the two boxed closures and the deque push are -- so
  there is no ~23-syscall saving to collect. For scale, a single job handed to
  a fully parked pool reaches its body in **6 µs** (median of 20; 5 µs best,
  43 µs worst), so the producer finishes the fan-out at about the same time
  the eighth worker arrives.
- **#378's mechanism buys nothing.** One `notify_n(23)` in place of 23
  `notify_one` calls leaves the distinct count inside the unbatched path's own
  run-to-run range at all three body lengths, and it has a failure mode the
  unbatched path never showed: at 500 µs bodies, two of three runs recorded a
  push window of **807 µs and 1062 µs** instead of ~50, with 13 of 24 workers
  reached -- the burst of 23 unparks from one call makes 23 threads runnable at
  once on a 24-thread box and the submitter loses its own core part-way
  through. That is a mechanism the counters name, pointing the other way.
- **The spill buffers do not serialise an external submission.** The external
  path costs the same ~2 µs a task as the worker path when it is one job at a
  time, and a *bulk* external submission -- one `push_bulk` of 24 items dealt
  round-robin over the `bit_width(24) == 5` buffers, one `notify_n(24)` --
  costs **under a microsecond for the whole fan-out** and still reaches the
  same 13 to 23 workers. Dealing the work over five queues instead of one is
  not enough spread to change the count, and nothing in `Buffers` is the
  bottleneck. #102's five buffers are not implicated either way.

### It does not move with the partitioner or the chunk count

The same shape under every partitioner the port has, `n = 1000000` at 24
workers, three runs each. "Tasks" is the observer's own entry count, so for the
splitting rows it counts the re-splits as well:

| partitioner | tasks | distinct workers, run 0 / 1 / 2 | wall, run 1 / 2 |
|---|---|---|---|
| guided, floor 1 (`DefaultPartitioner`, 547 chunks) | 25 | **13** / 23 / 23 | 854 / 817 µs |
| static, chunk 0 (24 chunks) | 24-25 | 17 / 24 / 23 | 1401 / 1063 µs |
| guided, 4096 (126 chunks) | 25 | 17 / 22 / 23 | 859 / 824 µs |
| dynamic, 4096 (245 chunks) | 25 | 16 / 23 / 23 | 781 / 776 µs |
| random, 4096 | 25 | 13 / 10 / 15 | 10518 / 9642 µs |
| splitting, default | 108-138 | 16 / 23 / 23 | 2487 / 2805 µs |
| splitting, 4096 | 85-94 | 19 / 23 / 23 | 1467 / 1591 µs |
| splitting, 65536 | 25 | 23 / 23 / 23 | 1211 / 1017 µs |

- **The chunk count spans 24 to 547 and the worker count does not follow it.**
  Static's 24 chunks and guided's 547 reach the same 23 to 24 workers in the
  steady state, which is what #343 already concluded from the other side: the
  chunk loop is not the gap.
- **Run 0 is the first fan-out that executor ever ran, and it is the low one
  in seven rows of eight.** A cold pool reaches 13 to 19 where the same code
  reaches 22 to 24 one millisecond later. An idle-gap sweep (fresh executor,
  eight runs, gaps of 0 to 50 ms with no quiesce) puts the whole range at 17 to
  23 with no clean trend, so pool temperature moves the number by several
  workers without being the cause of the ceiling.
- **`SplittingPartitioner` is the one knob that moves the count where it is
  low**, and it is not free: 16 and 19 against guided's 13 on the cold run,
  equal at 23 once warm, for a wall of 1.5 to 2.8 ms against 0.8. Its own
  figures belong to #381, which owns the `rustflow-split` ids; what this
  section claims about it is only the mechanism -- its 85 to 138 tasks come
  from shares re-splitting on the worker that stole them, so the fan-out's
  *producers* multiply instead of its tasks all leaving one deque. 60 to 113
  of its `notify_one` calls find the notifier empty, which costs a fence and
  nothing else.
- **`RandomPartitioner` at 4096 is a different animal** -- 9.6 to 11.7 ms, 10
  to 15 workers -- and is in the table only so that the row is not missing.
  Diagnosing it is not this issue's.

### It does move with the width of the pool against the width of the box

The decisive sweep. One executor per worker count, `n = 1000000`, guided and
splitting, two runs each:

| workers | guided: distinct | guided: wall | splitting: distinct |
|---|---|---|---|
| 4 | **4 / 4**, 4 / 4 | 2.43, 3.01 ms | 4 / 4, 4 / 4 |
| 8 | **8 / 8**, 8 / 8 | 1.70, 2.15 ms | 8 / 8, 8 / 8 |
| 12 | **12 / 12**, 12 / 12 | 1.28, 1.42 ms | 12 / 12, 12 / 12 |
| 16 | **16 / 16**, 16 / 16 | 0.93, 1.06 ms | 16 / 16, 16 / 16 |
| 20 | **20 / 20**, 20 / 20 | 0.85, 0.95 ms | 20 / 20, 20 / 20 |
| 24 | **14 / 24**, 16 / 24 | 1.27, 1.56 ms | 18 / 24, 17 / 24 |
| 32 | 15 / 32, 25 / 32 | 1.14, 1.32 ms | 24 / 32, 21 / 32 |

**Up to 20 workers on a 24-thread box the fan-out reaches every worker, every
time, and the shortfall appears exactly when the pool is as wide as
`available_parallelism`.** A pool of 20 has four hardware threads spare for
the submitting worker to keep its core, the waiting thread, the observer and
the kernel; a pool of 24 has none, so four to ten workers do not reach a core
inside the ~50 µs window in which the jobs exist, and by the time they do the
queue is empty -- which is the "parked while work was queued = 0" row again,
seen from the machine's side.

The control confirms it from the other direction: a **20**-worker pool, 20
bodies of 500 µs, with `k` extra threads spinning in the background, counted
with the lightest instrument in this section (one thread-local read per body):

| background spinners | distinct of 20, six runs | rayon's pool, same shape, interleaved |
|---|---|---|
| 0 | 17, 14, 18, 15, 15, 14 | 18, 18, 19, 19, 20, 20 |
| 2 | 14, 16, 17, 14, 14, 17 | 20, 20, 20, 20, 20, 20 |
| 4 | 13, 13, 14, 14, 14, 14 | 18, 19, 19, 19, 19, 19 |
| 8 | 10, 12, 14, 13, 14, 13 | 19, 16, 16, 15, 15, 15 |

Taking the slack away takes workers away from the fan-out, on both sides. What
the right-hand column also says is that **rayon loses fewer of them**, and the
next subsection is why.

### What upstream does, and what rayon does

**Upstream does exactly what this port does, line for line.** At the pinned
tag, `Executor::_schedule(Worker&, Node*)` is one `worker._wsq.try_push(node)`
-- spilling to the buffers if the bounded queue is full, which a crossbeam
deque never is -- followed by one `_notifier.notify_one()`, under a comment
saying that since v3.5 it uses no "complicated notification mechanism" because
"the experimental result has shown no significant advantage".
`_schedule_async_task` in `async.hpp` routes a worker's `silent_async`
straight into that overload, and `for_each.hpp`'s dispatch is
`(++w == W) ? task() : rt.silent_async(task)`. So upstream's own `for_each`
fan-out is `W - 1` individual pushes onto the submitting worker's queue and
`W - 1` individual `notify_one` calls, which is what
`src/algorithm/for_each_index.rs` and `WorkerThread::schedule` do here. The
steal side matches too: `_explore_task` is the same walk from
`w._sticky_victim` with `MAX_STEALS = ((MAX_VICTIM + 1) << 1)` and a
150-attempt yielding tail, and `_wait_for_task` the same two-phase guard with
the buffer sweep before the sibling sweep.

Upstream *has* the bulk path #378 proposed -- `_bulk_schedule(Worker&, I,
size_t)`, one `try_bulk_push` then one `notify_n(num_nodes)` -- and spends it
on a node's released successors and on `_schedule_graph`'s source nodes,
never on a runtime fan-out. Its own commented-out experiment sits in that
function: notifying before spilling, with the note that "the experiment does
not show any benefit for doing this". The port's `Shared::bulk_schedule`
already carries both the path and that finding, measured, in the "#60"
section above. **The ceiling is therefore a shape of the design and not a
port-specific gap.**

**rayon's `ThreadPool` differs in two ways, and only the second one matters.**
When a `par_iter` lands on an idle pool from a non-worker thread,
`Registry::in_worker_cold` pushes **one** job into the injector and blocks the
caller on a latch; `Registry::inject` then calls
`Sleep::new_injected_jobs(1, queue_was_empty)`, and a worker's own
`WorkerThread::push` calls `Sleep::new_internal_jobs(1, queue_was_empty)`.
`Sleep::new_jobs` wakes `min(num_jobs, num_sleepers)` threads when the queue
was already non-empty and **nothing at all** when `num_awake_but_idle >=
num_jobs`: rayon counts its awake-but-idle threads and declines to spend a
wake it does not need, where this port's notifier cannot see an awake worker
at all -- a spinning worker is simply absent from its state word, which is why
`notify_one` is cheap and why one poke per run here lands on nobody. That is a
difference in wake *economy*, and the counters above say the port is not
losing anything to it.

What does matter is that **rayon's fan-out is a tree and ours is flat**.
`bridge_producer_consumer` splits the range in half and runs the two halves
through `join_context`, which pushes the right half onto the *calling*
worker's own deque; `Splitter::try_split` then resets the remaining split
budget to `current_num_threads()` whenever the job it is splitting **was
stolen** ("This job was stolen! Reset the number of desired splits to the
thread count"). So a worker that steals late does not take one of `w` equal
chunks off one queue -- it takes half of what is left and immediately becomes
a producer on a queue of its own. After `k` generations there are up to `2^k`
non-empty deques for a searching thief to find, against this port's one, and a
thief's random draw over `num_queues()` hits a non-empty queue far sooner. A
rayon worker also spins `ROUNDS_UNTIL_SLEEPY = 32` rounds and one more before
it sleeps, where `explore_task` spends 62 attempts and then 150 yielding ones,
so the port is not the impatient one.

That tree is precisely the mechanism `SplittingPartitioner` (#374) ports, and
the partitioner table above is it working: the splitting rows reach more
workers than guided on the cold run and match it warm, with 85 to 138 tasks
where guided has 25. It is also why it is not simply better -- the tree costs
three to four times the dispatch.

### The decision: nothing ships, and what would reopen it

**No change ships, and #378 is the one this closes.** The bar is #342's and
#343's: a mechanism the counters name, then an A/B outside both sides' spread.
The one candidate reaches neither half.

- **One wake per fan-out has no mechanism left to fix.** 24 pokes deliver 24
  unparks and 24 wake-ups; no worker parks on available work; the push loop
  costs the same 2 µs a task with the pokes batched; and the batched variant
  stalled the submitter for 0.8 to 1.1 ms in two runs of three on the longest
  bodies. There is nothing here to A/B, so #381's bulk-wake column has nothing
  to measure and #378 closes with this section as its answer.
- **The ceiling that is left is one queue and one producer**, and both are
  upstream's design at the pin. Changing either is not a fixup: a fan-out that
  pushed into `w` different deques would be writing other workers' queues,
  which the port's `WorkerQueue` is `!Sync` precisely to forbid, and a fan-out
  that built a tree is `SplittingPartitioner`, which already exists, is already
  benched, and costs three to four times the dispatch to run.
- **Four to ten workers of 24 on this box are not the scheduler's to give.**
  The worker-count sweep reaches `w` of `w` at every width up to 20 and the
  background-spinner control takes workers away from rayon too. A pool as wide
  as `available_parallelism` has no thread to spare for the thread that is
  submitting, and that is the shape of the box rather than of the queue.
- **What would reopen it** is a counter this section does not have: the
  distinct-worker count on a box with more hardware threads than the pool has
  workers *and* a fan-out longer than the arrival spread, which would separate
  the two causes that are confounded at 24 workers here. Failing that, a
  measurement that the tree dispatch can be made cheap enough to be the default
  -- which is #381's question about `SplittingPartitioner`, not this one's.

**What this does not say.** It does not say the fan-out reaches all 24 workers:
it reaches 13 to 23, the median of the quiet-box runs being about 20, and the
"15 to 19" that #343 published and #367 was filed on sits inside that range
and is reproducible with the same observer-weight instrument. It does not
revisit #343's attribution, which stands unchanged. And it does not claim a
figure: no criterion id was run for this section, because nothing in `src/`
changed and no table here needed re-taking.

## The #373 measurement: the 23 idle workers never wake, and the 40% was #62's

**A chain at 24 workers costs what a chain at one worker costs, and the 23
workers with nothing to do take no part in it whatsoever.** Over a
16384-task run the counters read **zero** `explore_task` calls, zero steal
attempts, zero `prepare_wait`/`commit_wait` pairs and zero unparks for 23 of
the 24: one worker is woken by the run's single `notify_n(1)`, takes the one
source out of a buffer, runs all 16384 links through #62's continuation, and
parks. The "40% more per task at 24 workers" that the `linear_chain` table
above records and that this issue was filed on is the pre-#62 scheduler's;
re-taking the four ids on today's `main` puts the two worker counts within
3% of each other, with the 24-worker side the *faster* one at the larger
size.

What a chain's per-task figure does move with is **where the box puts the one
busy thread**. Held fixed with `taskset`, nine pool widths from 1 to 32 read
the same 45.2 to 45.7 ns a task; the same program unpinned reads 61 ns a task
when the worker that wakes shares an L3 with the thread that allocated the
graph and 140 ns when it does not. And what the busy worker pays the
*scheduler* turns out to be neither of the two candidates the issue named: not
its own deque's cache line, which no other worker touches even once in a run,
and not the notifier poke in `WorkerThread::schedule`, which is called zero
times a run. It is one shared word per task that nothing reads -- an empty
batch handed to `Buffers::push_bulk` on every task's release path -- and that
is worth 11 to 15% of the run region. It does not reach the bar on the
published ids, so nothing ships here and it is filed as #393 -- **which has
since shipped the guard**, on this section's pinned ablation rather than on
the published ids; the last subsection below is what that cost and bought.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads, two L3 instances (cpus 0-5,12-17 and 6-11,18-23) |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement -- the four `linear_chain` ids, re-taken, and the `devtools/bench-ab.sh` rounds; everything else below is counts and harness wall clocks, as #343's and #367's are |
| Date | 2026-11-19 |
| **Load average** | **1.01 to 2.61 before and after every measurement below, 2 or 3 runnable threads throughout, `/proc/pressure/cpu`'s `some avg10` at 0.00** |

The box was this run's alone -- no sibling agent, nothing else building -- and
`/proc/loadavg` was read either side of every measurement:

| measurement | before | after |
|---|---|---|
| the four criterion ids | 1.87 2.07 2.01 | 1.91 2.06 2.00 |
| the counters, both worker counts | 1.20 1.38 1.68 | 1.20 1.38 1.68 |
| the steal-attempt sweep over chain lengths | 1.13 1.32 1.62 | 1.13 1.32 1.62 |
| the width sweep, unpinned | 1.01 1.20 1.54 | 1.09 1.22 1.54 |
| the width sweep, pinned | 1.10 1.17 1.49 | 2.61 1.49 1.59 |
| the guard ablation, pinned then unpinned | 1.34 1.60 1.62 | 2.19 1.77 1.68 |
| the alignment pair | 1.44 1.62 1.63 | 1.44 1.62 1.63 |
| `bench-ab.sh`, 5 rounds | 1.68 1.45 1.57 | 2.00 1.75 1.67 |

The one-minute figure never falls below 1.0 because it is decaying from this
run's own benchmarks, each of which keeps one or two threads busy for a
minute at a time; the runnable-task field and the pressure counter are the
ones that say the box was otherwise idle.

Every count below is a **delta across one run** of one chain: the pool is
quiesced first (`Executor::num_waiters() == workers` confirmed before each
window, and it was every time), both snapshots are taken on the submitting
thread, and the counters are cache-padded per-worker words incremented with a
relaxed `fetch_add` by their owner only. Nothing was committed: a `src/probe.rs`
and its call sites in `worker.rs`, `notifier.rs` and `queue.rs` were reverted
before the gate ran, which is #343's and #367's method. The shipped diff of
this measurement is this section.

### The counters: one worker, two `explore_task` calls, one park

`linear_chain`'s own graph at its own two lengths, built by the harness the
`rustflow` ids build, run from a fully parked pool. The single-run windows and
a window of 100 back-to-back runs (which is what criterion's `iter` loop does)
agree to the counter on every row, so one column serves for both. `L = 16384`
is quoted; at `L = 1024` every row reads the same but the two timestamps,
+12.1 µs and +100.3 µs, and the two counts that scale with the task count.

| counter, per run | 24 workers | 1 worker | what it is |
|---|---|---|---|
| workers parked when the window opened | **24 / 24** | 1 / 1 | `Executor::num_waiters()` |
| workers that ran a job | **1** | 1 | all 16384 of them on it |
| `explore_task` calls, whole pool | **2** | 2 | one to find the source, one to decide to park |
| steal attempts on a worker deque | **0** | 0 | by victim index: none, against any deque |
| steal attempts on a buffer | **1** | 1 | and it is a hit |
| `explore_task` calls that spent the budget | **0** | 0 | 60 attempts, then 150 yielding ones |
| `yield_now` calls in `explore_task` | **0** | 0 | |
| `prepare_wait` / `commit_wait` | **1 / 1** | 1 / 1 | the busy worker's own park, after the last link |
| `cancel_wait` | 0 | 0 | |
| unparks delivered / parks that returned | 1 / 1 | 1 / 1 | |
| `WorkerThread::schedule` calls | **0** | 0 | nothing is pushed after the source |
| `notify_one` calls | **0** | 0 | |
| `notify_n` calls | 1, asking for 1 | 1, asking for 1 | the external submission |
| `Buffers::push_bulk` calls | **16385** | 16385 | one with an item in it; 16384 empty |
| first job entered | +124.7 µs | +129.1 µs | after `run()`: the `set_up_graph` pass over 16384 nodes |
| the only park committed | +1.485 ms | +1.293 ms | of a 1.489 ms and a 1.296 ms run |

Four of those rows are the whole answer to the question this issue asked.

- **The 23 are not idle-but-searching, they are absent.** Zero
  `explore_task` calls means not one of them so much as reached the
  `num_topologies()` fast path: they were parked before the window opened and
  the run's one `notify_n(1)` woke exactly one of them. The counter the issue
  asked for -- the time from the run's start until the last idle worker parks
  -- therefore has no value to report, because **no idle worker parks inside a
  chain's run at all**. The only `commit_wait` in the window is the busy
  worker's own, at +1.485 ms of a 1.489 ms run, which is the run ending rather
  than an idle worker settling.
- **Nothing is ever pushed, so the notifier poke in `WorkerThread::schedule`
  does not exist here.** That method is called **zero** times over a
  16384-link run at either worker count: #62's continuation keeps every one of
  the 16383 successors out of the queue, and the one job that is queued is
  queued externally, into a *buffer*, by `Shared::bulk_schedule`. The second
  candidate the issue named -- the contended cache line of the busy worker's
  own deque -- is just as empty: the per-victim counters record no steal
  attempt against that deque, or against any other worker's, over the whole
  run. There is nothing for the 23 to contend *on*.
- **One steal a run, exactly as #102's section measured it**, and it is a
  buffer rather than a sibling. Its cost is the next subsection's.
- **The first job enters at +125 µs, and that is not a dispatch latency.**
  It is `set_up_graph`'s pass over the 16384 nodes inside `run()`, which
  scales with the graph (12.1 to 12.9 µs at `L = 1024`) and is the same at both
  worker counts. The dispatch latency proper -- `run()` to the body of a
  one-task graph on a parked pool -- is 3.5 to 6.0 µs median at every pool
  width from 1 to 24, with no trend in the width, which is #367's 6 µs
  reproduced on a different shape.

The first window after a quiesce is the one exception to the steal row, and it
is worth keeping: the woken worker's `sticky_victim` is then whatever it last
stole from, so one measured cold window cost **36 deque draws and 7 buffer
draws** before the source turned up. In the steady state of a criterion
measurement that is 1.00 attempts a run, for the reason the alignment
subsection below gives.

### The four ids, re-taken: 131 ns a task at one worker against 128 at 24

One run of each id on today's `main`, the point estimate of the mean with half
criterion's 95% interval as a ±, against the rows in the `linear_chain` table
above -- which predate #40, #74 and #62 and are left as measured:

| id | today's `main` | per task | the table above |
|---|---|---|---|
| `linear_chain/rustflow/1w/1024` | 158.0 µs ±1.0% | 154 ns | 136.1 µs |
| `linear_chain/rustflow/24w/1024` | 161.9 µs ±0.3% | 158 ns | 217.9 µs |
| `linear_chain/rustflow/1w/16384` | 2.145 ms ±0.6% | 131 ns | 2.046 ms |
| `linear_chain/rustflow/24w/16384` | 2.095 ms ±0.2% | 128 ns | 2.872 ms |

- **The gap the issue was filed on is gone, and #62's section said it would
  be**: "after this change the two worker counts measure the same thing". At
  16384 the 24-worker id is now **2.3% faster** than the one-worker id, at
  1024 it is 2.5% slower, and both differences are a few times each id's own
  interval rather than the 40% the old rows show.
- **The 24-worker row is the one that moved**: −27% at 16384 and −26% at
  1024 against the pre-#62 table, where the one-worker rows are within 5% of
  what they were. That is the shape of a change that removed a hand-off, not one
  that made a task cheaper.
- **These are two runs of a four-id target, not a re-measurement pass.** The
  table above is left alone deliberately: it is the historical row set with the
  paragraph that says what it predates, and replacing it is a docs pass's job
  rather than this section's.

### It is the placement of one thread, not the width of the pool

The decisive control. One chain per round on each of nine pools, **every pool
alive throughout** so that the box carries the same 120 threads in every row
and the only difference between two rows is how wide the pool the chain ran
on was; rounds rotated so no width is always measured first; medians of 101
rounds of the **run region only**, the graph's construction timed separately
and excluded. The left-hand column is the whole process pinned to one CPU with
`taskset -c 3`:

| pool width | pinned: run | ns a task | unpinned: run |
|---|---|---|---|
| 1 | 0.747 ms | 45.6 | 1.022 ms |
| 2 | 0.745 ms | 45.5 | 1.020 ms |
| 4 | 0.746 ms | 45.5 | 1.024 ms |
| 8 | 0.748 ms | 45.6 | 1.006 ms |
| 12 | 0.746 ms | 45.5 | **2.300 ms** |
| 16 | 0.744 ms | 45.4 | 1.010 ms |
| 20 | 0.745 ms | 45.5 | 1.002 ms |
| 24 | 0.745 ms | 45.5 | 1.006 ms |
| 32 | 0.749 ms | 45.7 | 1.010 ms |

- **Pinned, the pool's width is worth 0.7% across nine widths** -- 45.4 to
  45.7 ns a task, every row inside every other row's [5th, 95th] band. A pool
  32 workers wide on a 24-thread box costs a chain the same as a pool of one.
  That is the issue's question answered from the timing side, and it agrees
  with the counters: 23 parked threads that never run cannot cost anything,
  and neither can 31.
- **Unpinned, one row in nine is 2.3x the others, and it stays there** --
  2.300 ms median, [2.230, 2.321] over 101 rounds, 140 ns a task against
  61. The pool width is not what distinguishes it: what distinguishes it is
  which core the notifier's one wake lands on. This part has two L3 instances,
  the graph's 16384 nodes are allocated by the submitting thread, and a worker
  on the far L3 reads every one of them cold. A separate run that records the
  CPU of the first task's body found one 24-worker pool that ran every one of
  201 rounds on `cpu8` at 0.780 ms and a one-worker pool that ran 184 of them
  on `cpu23` at 1.028 ms -- a 30% spread between two placements on the *same*
  L3, which is the rest of this effect and is the frequency and SMT story
  rather than the cache one.
- **So the published 2.1 ms is a placement as much as a measurement.**
  Pinning the whole process to one CPU makes the chain *faster* (45 ns a task
  against 61) because the thread that builds the graph and the worker that runs
  it then share an L1; the three figures this shape can produce on this box --
  45, 61 and 140 ns a task -- differ by where one thread sits and not by
  anything the scheduler does.

### What the busy worker does pay: an empty batch on every task's release path

`WorkerThread::invoke_one`'s release hook runs on every job:

```rust
let mut woken = Vec::new();
if let Err(error) = node.release_all(&mut woken) { /* ... */ }
self.shared.bulk_schedule(woken);
```

`release_all` returns at once when the node has no semaphores, so `woken` is
empty for every task of every shape in this file -- none of the 22 puts a
semaphore on a task.
`Shared::bulk_schedule` is called all the same, and `Buffers::push_bulk`
advances `rotation` -- an atomic read-modify-write on a shared, deliberately
padded cache line -- and divides by a runtime `num_buffers` **before** its loop
discovers there is nothing to deal. The counter row above is that: 16385
`push_bulk` calls over a 16384-task run, 16384 of them empty.

Ablated in one process, with the guard behind a relaxed flag so that rounds
alternate A, B, A, B inside one pool and one placement -- the run region only,
medians of 101 rounds at `L = 16384` and 401 at `L = 1024`, each side's
[5th, 95th] band beside it:

| id, pinned to one CPU | A: as it ships | B: empty batch skipped | B/A | ns a task |
|---|---|---|---|---|
| 1 worker, 16384 | 0.791 ms [0.781, 0.798] | 0.688 ms [0.680, 0.696] | **0.870x** | 48.3 → 42.0 |
| 24 workers, 16384 | 0.800 ms [0.787, 0.838] | 0.698 ms [0.687, 0.725] | **0.872x** | 48.8 → 42.6 |
| 1 worker, 1024 | 0.0580 ms [0.053, 0.060] | 0.0510 ms [0.046, 0.052] | 0.889x | 56.5 → 50.2 |
| 24 workers, 1024 | 0.0540 ms [0.054, 0.062] | 0.0480 ms [0.047, 0.070] | 0.882x | 53.2 → 46.9 |

Two further unpinned repeats of the same four comparisons give 0.872x, 0.875x,
0.884x, 0.887x and 0.854x, 0.855x, 0.870x, 0.863x: **twelve interleaved
comparisons, every one between 0.854x and 0.889x, and 6 to 10 ns a task.** The
bands are disjoint in all twelve. That is an atomic RMW and a 64-bit division
on a path a chain takes 16384 times, and it is the only per-task cost this
measurement found that the port could stop paying.

**It does not reach the bar on the published ids, which is why nothing ships
here.** `devtools/bench-ab.sh origin/main WORKTREE --bench linear_chain
--rounds 5`, all four ids:

| id | A median | B median | B/A | wider within-side spread | verdict |
|---|---|---|---|---|---|
| `1w/1024` | 152.7 µs | 148.2 µs | 0.971x | +23.3% | inside it |
| `1w/16384` | 2.104 ms | 2.021 ms | 0.960x | +7.0% | inside it |
| `24w/1024` | 161.8 µs | 162.8 µs | 1.006x | +25.7% | inside it |
| `24w/16384` | 2.100 ms | 2.011 ms | 0.957x | +18.8% | inside it |

Three of the four move the right way by about 4% and the script's verdict on
every one of them is "inside it, not a result" -- which is the correct reading
and not a formality. An id times the graph's construction and its drop as well
as its run, so 13% of the run region is 4% of the id; and a round of this id
on this box differs from the next round by 7 to 26%, because each round is a
fresh process and gets a fresh placement for the one busy thread. The
mechanism is real, the instrument that can see it is the pinned interleaved
ablation above, and **#393** carries both.

### The one steal, and what a stale sticky victim costs

The rotation that the empty batches advance has a visible second-order effect,
and it is why the steal row above reads 1.00 attempts a run. A chain advances
`rotation` `L + 1` times a run; `(1024 + 1) % 5` and `(16384 + 1) % 5` are both
**zero**, so at both published lengths the rotation comes back to the same
buffer every run, the source lands there every run, and the woken worker's
`sticky_victim` -- set to that buffer the first time -- hits on its first
attempt for ever after. At a length where it does not, the source moves and
the sticky victim misses. Counted over 50 runs at 24 workers, per run:

| chain length | `(L + 1) % 5` | deque draws | buffer draws | `yield_now` calls |
|---|---|---|---|---|
| 1024 | 0 | **0.08** | 1.06 | 0.00 |
| 1020 | 1 | 28.4 | 6.98 | 4.96 |
| 1021 | 2 | 22.3 | 5.88 | 2.90 |
| 1022 | 3 | 29.8 | 7.38 | 6.14 |
| 1023 | 4 | 22.9 | 6.12 | 5.92 |
| 4096 | 2 | 23.0 | 5.66 | 4.90 |

That is one pass over `num_queues() == 29` as the expected draw count predicts,
with the yielding tail entered on the one run in eight where 60 draws were not
enough -- `explore_task` searches the whole victim space at random, so a job
that can only be in one of five buffers still costs about 29 draws to find.
**What it is worth is 0.6 µs a run**: 0.0544 ms against 0.0550 ms for
`L = 1024` and `L = 1023` run on a pool each, interleaved, pinned, 401
rounds. It is in this section because it is the whole of what the one steal of
a chain costs, and it is not worth a change: 0.6 µs is 1% of the smaller id
and 0.03% of the larger, and the random draw is upstream's own.

### What upstream does, and the one place it differs

**`_explore_task` at the pin is the same loop, line for line**: own
`_sticky_victim` first, then a random victim out of
`_workers.size() + _buffers.size()`, `MAX_STEALS = ((MAX_VICTIM + 1) << 1)`
tight attempts and a 150-attempt yielding tail, then back to
`_wait_for_task`'s two-phase guard with the buffer sweep before the sibling
sweep. A chain gives that loop nothing to do at either end: upstream's
`TF_INVOKE_CONTINUATION` keeps the successor exactly as this port's
`update_cache` does, so upstream's 23 idle workers would also be parked for
the whole run and its one woken worker would also draw over 29 queues to find
one source in one of five buffers. Nothing in the steal loop is a port gap.

**The release hook is the one place the port differs, and upstream is the one
that guards it.** At the pin `_invoke` reads

```cpp
// if releasing semaphores exist, release them
if(node->_semaphores && !node->_semaphores->to_release.empty()) {
  SmallVector<Node*> waiters;
  node->_release_all(waiters);
  _bulk_schedule(worker, waiters.begin(), waiters.size());
}
```

-- and the acquire hook above it is guarded the same way. A task with no
semaphores on it therefore never reaches `_bulk_schedule` upstream, where this
port's does on every job. (Upstream's call is also the *worker* overload,
pushing onto the worker's own queue; the port's routes woken jobs to the shared
buffers for the reason `invoke_one` states, and that difference is not this
section's.) The port's guard is one line, it is #393, and this section is its
measurement.

### The decision: nothing ships, and what would reopen it

**No change ships from this issue.** The bar is #342's, #343's and #367's: a
mechanism the counters name, then an A/B outside both sides' spread. The
mechanism is named and the A/B is inside the spread, so the shipped diff is
this section.

- **The question as asked is answered in the negative.** 23 idle workers cost
  a chain nothing -- not a steal attempt, not a wake, not a park -- and a
  pinned sweep over nine pool widths puts their cost at 0.7%, which is the
  measurement's own resolution. The contended deque line and the
  `WorkerThread::schedule` poke, the two candidates the issue offered, are
  called zero times a run between them.
- **The 40% in the table above is a historical figure and this section is
  where that is now measured** rather than inferred from #62's A/B.
- **What the chain's 128 ns a task is made of is not the steal loop.** 45 ns
  of it is reachable by putting the builder and the runner on one core, and the
  gap between 45, 61 and 140 ns a task is cache and placement. A reader looking
  for the next win on this shape should look at what the 16384 nodes cost to
  allocate, touch and free, which is where `binary_tree`'s section already
  points, and not at the queues.
- **What would reopen it** is a shape that leaves the pool's other workers
  *awake* while one worker runs a serial chain -- a long chain next to a live
  fan-out, which no id in this file builds. Every count here is from a pool
  that is parked except for one worker, and a thief that is already awake has
  a cost this section never measures: #367's losing thief, 150 to 285 attempts
  at random over 29 victims, would be drawing on the chain's deque while it
  runs.

### What #393 shipped: the guard is in, and it is worth 16% of the run region

**`if !woken.is_empty()` now guards the release hook's `bulk_schedule` call,
and a chain's run region is 16% shorter for it.** The acquire hook above is
left as it stands -- its `bulk_schedule` is already behind
`if !node.acquire_all(..)`, which is the branch upstream's `to_acquire` guard
exists to avoid taking -- so the shipped diff is the one `if` this section
asked for, with the comment at it naming
`if(node->_semaphores && !node->_semaphores->to_release.empty())`.

One run of the interleaved same-process ablation above, re-taken against the
shipped guard rather than a flag: A is `origin/main`'s unconditional call, B
is the guard, rounds alternate A, B, A, B inside one pool and one placement,
`taskset -c 3`, the run region only, medians of 101 rounds with each side's
[5th, 95th] band beside it:

| id, pinned to one CPU | A: unconditional | B: guarded | B/A | ns a task |
|---|---|---|---|---|
| 24 workers, 16384 | 0.7938 ms [0.7807, 0.8601] | 0.6688 ms [0.6593, 0.7087] | **0.843x** | 48.4 → 40.8 |
| 1 worker, 16384 | 0.7818 ms [0.7781, 0.7918] | 0.6577 ms [0.6521, 0.6657] | **0.841x** | 47.7 → 40.1 |

Both bands are disjoint, and 0.843x is a little better than the 0.872x the
flag-driven ablation above read on the same id: the shipped guard spends one
branch on a local `Vec`'s length where the flag spent that branch plus a
relaxed atomic load per task on *both* sides, and the rest of the gap is
between-run variation of the kind the twelve comparisons above show
(0.854x to 0.889x on one mechanism).

**The second-order effect the section predicted is real, and the 0.843x is
already net of it.** Counted over 50 runs at 24 workers, `L = 16384`,
unpinned, the one steal of the run costs what the rotation decides:

| release hook | `push_bulk` calls a run | deque draws | buffer draws | `yield_now` calls |
|---|---|---|---|---|
| unconditional | 16384, every one empty | 0.00 | **1.00** | 0.00 |
| guarded | 0 | 28.8 | 7.00 | 3.56 |

With the empty batches gone, `rotation` advances once a batch as
`push_bulk`'s own doc says instead of `L + 1` times a run, so the source stops
landing in the same buffer every run and the woken worker's `sticky_victim`
stops hitting on its first attempt: 35.8 draws over `num_queues() == 29`
victims, and the yielding tail entered on the runs where 60 draws were not
enough, 3.56 `yield_now` calls a run. That is the row the "one steal"
subsection above priced at 0.6 µs a run, and the pinned ablation is measured
with it in: 7.6 ns a task won against 0.04 ns a task given back.

The two mean run figures that counting pass printed, 1.02 ms unconditional
against 2.54 ms guarded, are **not** a comparison: they are two processes, two
pools and two placements, which is the 2.300 ms row of the unpinned width
sweep above rather than anything the guard does. That is the whole reason the
ablation alternates inside one pool.

One run of `devtools/bench-ab.sh origin/main WORKTREE --bench linear_chain
--rounds 5` as the no-regression check, all four published ids:

| id | A median | B median | B/A | wider within-side spread | verdict |
|---|---|---|---|---|---|
| `1w/1024` | 153.7 µs | 143.6 µs | 0.934x | +14.1% | inside it |
| `1w/16384` | 2.080 ms | 1.982 ms | 0.953x | +15.3% | inside it |
| `24w/1024` | 155.2 µs | 159.8 µs | 1.030x | +15.4% | inside it |
| `24w/16384` | 2.061 ms | 2.013 ms | 0.977x | +11.1% | inside it |

Nothing is a result on the published ids, which is what this section said
would happen: an id times the graph's construction and its drop as well as
its run, so 16% of the run region is 2 to 5% of the id against a round-to-round
spread of 11 to 15%. The one id that moves *against* the change, `24w/1024` at
+3.0%, is a fifth of its own spread and its A and B rounds interleave. The
measurement that can see this change is the pinned ablation; the bench-ab run
is here to show no id moved outside its spread.

The box was this run's alone, and `/proc/loadavg` either side of each of the
three measurements:

| measurement | before | after |
|---|---|---|
| the draw counts, 50 runs a side | 0.76 1.52 2.59 | 0.76 1.52 2.59 |
| the ablation, pinned, 101 rounds a side | 0.53 1.39 2.51 | 0.53 1.39 2.51 |
| `bench-ab.sh`, 5 rounds | 0.39 1.27 2.44 | 1.12 1.48 2.33 |

The counters and the ablation flag were a `src/probe.rs` and its call sites in
`worker.rs`, reverted before the gate as this section's own were: the shipped
diff is the `if` and these paragraphs.

## The #380 measurement: the route halves a task, the ordering sets the width

**At one worker a group opened on a `Runtime` costs half what a group opened
on the `Executor` costs a task -- 175 ns a spawned child against 347 on
`fibonacci` at `n = 15`, which is #119's 340-against-201 prior reproduced on
today's `main` and a little better.** At 24 workers the route is not what
decides the number. Two of the five shapes improve by 61% and 73%,
and one of them is the first group-route id in this file to *scale* from 1 to
24 workers on a tree whose bodies hold no work to scale with. The other three
get **4.7x to 27x worse** at their larger size and 1.7x to 21x at their
smaller one, and the ablation below puts all of that on the **ordering**
rather than on the queue: `Runtime::task_group`
borrows the runtime for the group's whole life, so a body with an inline
branch of its own has to submit its child *after* that branch instead of
before it, which leaves one child outstanding at a time for 23 awake thieves
to race for. The **id** route under the same ordering is just as slow (44.0 ms
against the runtime route's 35.4 on `fib(15)` at 24 workers, ranges
overlapping, where the id route with the submission first is 2.66 ms), and the
same route with the submission first is the fastest of the five variants
measured. So the route is a per-task win at every width, the forced ordering
is a loss that is worth more than the win on three of the five shapes, and
#395 is where the ordering is taken up.

This is also the shape #373's last bullet asked for and said it had not
measured: a serial spine next to a pool of workers that are **awake** rather
than parked. 23 thieves drawing on one deque that holds a single job cost
59 µs a handoff here, against the 12 µs the same recursion pays when there is
something else in the queue for them to find.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id -- the five shapes' `rustflow*` ids, one `cargo bench --bench <name>` each; the stack, ablation, width and handoff figures below are a probe's own wall clocks and counts, as #343's, #367's and #373's are |
| `ulimit -s` | 8192 KiB |
| Date | 2026-11-19 |
| **Load average** | **1.08 to 9.71 before and after each measurement, 2 or 3 runnable threads throughout, and all of it this run's own** |

The box was this run's alone -- no sibling agent, nothing else building -- and
`/proc/loadavg` was read either side of every measurement:

| measurement | before | after |
|---|---|---|
| `skynet`, the 8 `rustflow*` ids | 1.44 1.44 1.49 | 8.14 2.97 2.00 |
| `nqueens`, 8 ids | 7.04 2.91 1.99 | 9.71 3.93 2.36 |
| `integrate`, 8 ids | 8.37 3.83 2.35 | 9.46 4.58 2.66 |
| `merge_sort`, 8 ids | 7.12 4.36 2.62 | 5.50 4.26 2.65 |
| `fibonacci`, 12 ids | 3.12 3.80 2.57 | 6.93 4.61 2.91 |
| the frame-address and cliff bisections | 1.59 3.22 2.63 | 1.16 2.62 2.48 |
| the skynet tree at one worker, to `MaxDepth = 8` | 1.17 2.65 2.48 | 1.98 2.72 2.51 |
| the five-variant ablation | 2.07 2.73 2.51 | 2.14 2.73 2.52 |
| the pool-width sweep | 1.22 2.21 2.35 | 1.22 2.21 2.35 |
| the handoff counts | 1.08 1.99 2.26 | 1.08 1.99 2.26 |

The one-minute figure reaching 8 and 9 after three of the criterion shapes is
those shapes' own 24-worker ids decaying -- a shape's "before" reading is its
predecessor's tail, which is the condition "What is measured" names -- and the
runnable-task field was 2 or 3 at every one of those twenty readings. The
probe measurements below ran after the criterion ones and read 1.1 to 2.1,
which is this run's own decay and nothing else.

Every criterion figure is **one** run of one id, quoted as the point estimate
of the mean with half the width of criterion's 95% interval as a
plus-or-minus percentage. The two routes' ids are in the **same process** for
each shape, which is what #377 built them for: a row below is two columns
measured seconds apart under the same load.

### The figures: five shapes, two routes, two worker counts

The id route is each file's own column name -- `rustflow-group` in `skynet`,
`nqueens` and `fibonacci`, plain `rustflow` in `merge_sort` and `integrate` --
and the runtime route is `rustflow-runtime-group` everywhere. The sizes and
the bodies are each shape's published ones, unchanged.

| shape | size | workers | id route | runtime route | B/A |
|---|---|---|---|---|---|
| `skynet` | `MaxDepth = 3` | 1 | 209.19 µs ±0.21% | **141.79 µs ±0.22%** | 0.68 |
| `skynet` | `MaxDepth = 3` | 24 | 530.08 µs ±1.38% | **427.70 µs ±0.79%** | 0.81 |
| `skynet` | `MaxDepth = 4` | 1 | 2.0196 ms ±0.28% | **1.3441 ms ±0.63%** | 0.67 |
| `skynet` | `MaxDepth = 4` | 24 | 2.5976 ms ±1.40% | **1.0090 ms ±0.63%** | **0.39** |
| `nqueens` | `q = 6` | 1 | 85.172 µs ±0.17% | **51.502 µs ±0.11%** | 0.60 |
| `nqueens` | `q = 6` | 24 | 846.97 µs ±1.30% | **557.79 µs ±1.12%** | 0.66 |
| `nqueens` | `q = 8` | 1 | 1.1889 ms ±0.14% | **802.39 µs ±0.47%** | 0.67 |
| `nqueens` | `q = 8` | 24 | 3.2976 ms ±4.51% | **897.52 µs ±0.62%** | **0.27** |
| `fibonacci` | `n = 12` | 1 | 88.633 µs ±0.71% | **45.806 µs ±0.30%** | 0.52 |
| `fibonacci` | `n = 12` | 24 | **1.1550 ms ±1.02%** | 9.3732 ms ±2.61% | **8.1** |
| `fibonacci` | `n = 15` | 1 | 342.36 µs ±0.50% | **172.75 µs ±0.36%** | 0.50 |
| `fibonacci` | `n = 15` | 24 | **2.2967 ms ±1.90%** | 38.178 ms ±1.66% | **16.6** |
| `merge_sort` | 10000 | 1 | 254.94 µs ±0.13% | 255.13 µs ±0.21% | 1.00 |
| `merge_sort` | 10000 | 24 | **535.29 µs ±11.5%** | 907.76 µs ±7.35% | 1.70 |
| `merge_sort` | 1000000 | 1 | 55.495 ms ±0.37% | 54.788 ms ±0.12% | 0.99 |
| `merge_sort` | 1000000 | 24 | **15.606 ms ±0.76%** | 73.408 ms ±1.72% | **4.70** |
| `integrate` | `V = 1` | 1 | 297.36 µs ±0.11% | **144.49 µs ±0.15%** | 0.49 |
| `integrate` | `V = 1` | 24 | **1.4389 ms ±2.18%** | 29.854 ms ±2.07% | **20.7** |
| `integrate` | `V = 2` | 1 | 648.23 µs ±0.11% | **322.42 µs ±0.13%** | 0.50 |
| `integrate` | `V = 2` | 24 | **2.4263 ms ±3.78%** | 65.129 ms ±2.59% | **26.8** |

`fibonacci` carries a third `rustflow` column, `rustflow-runtime` -- the same
recursion through a runtime task with **no group**, which is the same queue
destination as the runtime route and the natural ordering -- and it is the
control that makes the ablation below readable: 47.700 µs ±0.32% and
181.33 µs ±0.39% at one worker, 902.13 µs ±0.98% and 1.3767 ms ±1.10% at
24 workers. That column is the fastest of the three at 24 workers and the
second fastest of the three at one.

Every ratio in the table bar one is outside both sides' intervals by a wide
margin. The exception is the pair of `merge_sort` ties at one worker -- 0.99
and 1.00, which is a shape whose timed region is twenty million compares and
fifteen or a thousand spawned tasks, so neither route has anything to show
there and the row says so. `merge_sort/24w/10000` is the loosest cell in the
table at ±11.5% against ±7.35%, and 1.70 still clears it: 590 µs is the id
route's upper bound against the runtime route's 836 µs lower one.

### One worker: 175 ns a child against 347, and a `Route` is all it is

Per spawned task, at one worker, over each shape's published task count:

| shape | size | spawned tasks | id route | runtime route | per task |
|---|---|---|---|---|---|
| `fibonacci` | `n = 15` | 986 | 342.36 µs | 172.75 µs | **347 ns → 175 ns** |
| `skynet` | `MaxDepth = 4` | 11110 | 2.0196 ms | 1.3441 ms | 182 ns → 121 ns |
| `nqueens` | `q = 8` | 2056 | 1.1889 ms | 802.39 µs | 578 ns → 390 ns |
| `integrate` | `V = 2` | 1852 | 648.23 µs | 322.42 µs | 350 ns → 174 ns |
| `merge_sort` | 1000000 | 1023 | 55.495 ms | 54.788 ms | not a submission figure |

The `fibonacci` row is the one to read against the prior, because it is the
shape #119 measured: **347 ns a child on the id route against 175 on the
runtime route**, where #119 read 340 against 201 for the same `n` through
`Runtime::silent_async_runtime`. The same id's `rustflow-runtime` column reads
184 ns a child here, so a group on the runtime route is not more expensive
than a runtime task's own submission -- it is 5% cheaper, outside both
intervals, and the ablation says why: at one worker the ordering the group
forces is itself worth a little (0.269 ms against 0.337 for the id route,
0.183 against 0.185 for the runtime one), because a parent that has nothing
outstanding until it waits pops its one child back with no window for anybody
to take it.

`nqueens` and `integrate` carry real arithmetic in a body, which is why their
per-task figures are larger on both routes; what the route moves is the same
170 to 180 ns of submission on `fibonacci` and `integrate` and 61 ns on
`skynet`, whose groups carry ten children each rather than one, so a level's
fixed cost is spread over ten submissions there. Nothing in these rows is
about the 24-worker tables: at one worker there is no thief, and the whole
difference is a buffer push plus a steal back out against a push and a pop on
the same end of one deque.

### 24 workers: two shapes scale, three fall over

The 1-to-24 ratio of each route against *itself*, which is the question the
issue asked:

| shape | size | id route, 1 → 24 | runtime route, 1 → 24 |
|---|---|---|---|
| `skynet` | `MaxDepth = 3` | 2.53x slower | 3.02x slower |
| `skynet` | `MaxDepth = 4` | 1.29x slower | **1.33x faster** |
| `nqueens` | `q = 6` | 9.94x slower | 10.8x slower |
| `nqueens` | `q = 8` | 2.77x slower | **1.12x slower** |
| `fibonacci` | `n = 12` | 13.0x slower | 205x slower |
| `fibonacci` | `n = 15` | 6.71x slower | 221x slower |
| `merge_sort` | 10000 | 2.10x slower | 3.56x slower |
| `merge_sort` | 1000000 | 3.56x faster | 1.34x slower |
| `integrate` | `V = 1` | 4.84x slower | 207x slower |
| `integrate` | `V = 2` | 3.74x slower | 202x slower |

**`skynet` at `MaxDepth = 4` scales where the id route regresses**: 1.3441 ms
to 1.0090 ms, against the id route's 2.0196 to 2.5976 the other way. That is a
tree of 11110 bodies that add ten slots and return getting something out of 23
more workers, which is a first for a group-route id on a shape with no work in
it -- `merge_sort`'s million-element row scales on the id route too, but what
it has to scale with is twenty million compares rather than a submission path.
`nqueens` at `q = 8`
is the same statement one notch weaker -- a 2.77x regression becomes 1.12x,
which is 898 µs against 802 and is a regression no longer worth the name. Both
are shapes whose parent submits **all** of its children and then waits: ten
per group in `skynet`, one per surviving placement in `nqueens`, with no
inline branch between the submissions and the `corun`. For those two the
attribution the `skynet` and `nqueens` sections above make -- the shared
buffers and the stealing they force -- is **confirmed by removing it**, which
is what those two sections now say.

The small sizes of both shapes still regress on both routes (`MaxDepth = 3` at
3.02x, `q = 6` at 10.8x), and that is #343's and #367's finding rather than
this one's: 1110 and 152 tasks of nothing cannot pay for the arrival spread of
24 workers whatever queue they are pushed onto.

The three shapes that fall over -- `fibonacci`, `integrate`, `merge_sort` --
are exactly the three whose body **computes one branch inline between
submitting the other and waiting for it**: upstream's tail optimisation, which
is what `fibonacci/taskflow.cpp`, `merge_sort/taskflow.cpp` and
`integrate/taskflow.cpp` all write. On the runtime route they cannot write it
that way round. `Runtime::task_group` borrows the `&mut Runtime` for as long
as the group lives and the inline branch needs that same runtime to open a
group of its own, so the branch has to run *before* the group is opened --
upstream's own `FibonacciSwapped` ordering, which `DESIGN.md` records as what
the route costs a body, `benches/fibonacci.rs`'s header names for its own
column, and the recursions in `benches/merge_sort.rs` and
`benches/integrate.rs` each carry a line about. The consequence is one child
outstanding at a time, and the next three subsections are what that costs.

### The ablation: it is the ordering and not the route

Five variants of the same `fib(15)`, driven straight by a probe outside the
crate rather than under criterion, nine rounds each, median of the nine with
the range beside it. The first two are the id route with the submission first
and last; the next two are the runtime route with no group, the same two ways
round; the fifth is the runtime-route group, which can only be written the
second way:

| variant | queue destination | submission | 1 worker | 24 workers |
|---|---|---|---|---|
| `group` | shared buffers | first | 0.337 ms (0.332-0.346) | **2.659 ms** (1.943-6.259) |
| `group-swapped` | shared buffers | last | 0.269 ms (0.256-0.397) | **44.010 ms** (41.911-47.063) |
| `runtime` | worker's own deque | first | 0.185 ms (0.181-0.234) | **1.655 ms** (1.315-3.444) |
| `runtime-swapped` | worker's own deque | last | 0.183 ms (0.182-0.236) | **40.610 ms** (36.092-48.750) |
| `rtgroup`, the measured route | worker's own deque | last, forced | 0.189 ms (0.184-0.249) | **35.433 ms** (31.703-43.365) |

Read down the 24-worker column and the ordering is the only thing that
matters: 2.7 and 1.7 ms with the submission first, 44.0, 40.6 and 35.4 with it
last, on **both** queue destinations. The three slow variants' ranges overlap
each other, so this instrument does not separate them -- what it does say is
that the runtime route is no worse than the id route under the same ordering
and is the faster of the two under either. Read down the one-worker column and
the destination is the only thing that matters: 0.34 and 0.27 ms into the
buffers, 0.18 to 0.19 onto the worker's own deque, with the ordering worth
nothing there or a little in the swapped direction.

That is the whole of the 24-worker table's bad half, and it is why #395 asks
for an API rather than a scheduler change: the figure the runtime route would
read with the submission first is the `runtime` row, 1.655 ms, which is
**below** the id route's own 2.659.

### It grows with the width of the pool, and stops at the cores

The forced ordering against nine pool widths, same probe, seven rounds each,
with the natural ordering through the same queue beside it:

| workers | `rtgroup`, submission last | `runtime`, submission first |
|---|---|---|
| 1 | 0.181 ms | 0.182 ms |
| 2 | 0.371 ms | 0.770 ms |
| 3 | 1.004 ms | -- |
| 4 | 2.168 ms | 0.791 ms |
| 6 | 8.718 ms | -- |
| 8 | 15.003 ms | 0.945 ms |
| 12 | 37.158 ms | -- |
| 16 | 36.606 ms | -- |
| 24 | 36.015 ms | 2.331 ms |

The cost is monotone in the width up to **12**, which is this box's core count
rather than its thread count, and flat from there to 24. That is the signature
of thieves rather than of lost parallelism: the serial time is 0.18 ms, so what
the other 35.8 ms is made of is the work of the workers that have nothing to
do, and adding a worker without a core to put it on adds nothing more. The
natural ordering through the same queue rises from 0.18 to 2.33 ms over the
same sweep, which is the arrival spread every shape in this file pays.

### The handoff, counted: 60% of the children change worker, at 59 µs each

The same recursion with each body asking whether it is running on the worker
that submitted it -- `Runtime::worker_id` against the id its parent passed
down -- summed over the 1973 bodies `fib(15)` enters, 986 of which are
spawned tasks. Three runs each:

| variant | workers | bodies | migrations | wall clock | wall per migration |
|---|---|---|---|---|---|
| `runtime`, submission first | 1 | 1973 | **0** | 0.21-0.31 ms | -- |
| `rtgroup`, submission last | 1 | 1973 | **0** | 0.19-0.28 ms | -- |
| `runtime`, submission first | 24 | 1973 | 99-146 | 1.24-1.75 ms | 12.0-13.6 µs |
| `rtgroup`, submission last | 24 | 1973 | **559-592** | 33.0-35.0 ms | 58.7-61.5 µs |

At one worker neither variant migrates once: the parent pops the child it
pushed, every time, which is the LIFO property the route is for. At 24 workers
the natural ordering migrates 10 to 15% of its children and the forced
ordering migrates **57 to 60%** of them -- the parent pushes its one child and
loses the race for it to a thief more often than not. The last column divides
the whole run by its migrations, so it is an upper bound on a handoff rather
than a breakdown of one; what it bounds is 59 µs against the natural
ordering's 12, and the difference between those two is that a migration in the
natural ordering overlaps with the parent's own inline branch while a
migration in the forced one is the serial spine stopping dead until the thief
finishes the subtree. What a handoff is made of -- failed steal attempts,
`explore_task`'s yield budget, a park that should not have happened -- is not
counted here and is the counter #395 would want.

### Where the new route's stack cliff sits

#359's method, re-run for all three routes in one binary: a chain of nested
coruns, one child a level so that the nesting is the depth and nothing else,
with the per-level cost read twice -- off the frame addresses, and off the
slope of the cliff against `RUST_MIN_STACK`.

| route | bytes/level by frame address | by the cliff slope, 1 → 4 MiB | deepest chain at the default stack | first that aborts |
|---|---|---|---|---|
| `Executor::task_group`, the id route | 1040 | 1040.3 | **2010** | 2011 |
| `Runtime::task_group`, the runtime route | 1104 | 1104.2 | **1893** | 1894 |
| `Runtime::corun`, no group at all | 832 | 832.0 | 2513 | 2514 |

The frame-address column is uniform to the byte over twelve levels on each
route, which is what makes it a frame size rather than an average, and the
slope column is an independent reading of the same quantity over three stack
sizes:

| route | 1 MiB | 2 MiB, and unset, which is identical | 4 MiB |
|---|---|---|---|
| id route | 1002 / 1003 | 2010 / 2011 | 4026 / 4027 |
| runtime route | 944 / 945 | 1893 / 1894 | 3793 / 3794 |
| `Runtime::corun` | 1252 / 1253 | 2513 / 2514 | 5033 / 5034 |

These are measured here rather than quoted from #359's 992 and 928, because
both the type on the frame and the chain's body have moved since: a
`TaskGroup` now carries #372's `Route`, and the runtime route's corun holds
the borrow of the worker across the wait. What the paragraph in
`src/task_group.rs` needs is the two routes measured in one binary, which is
this table. The 6 to 7 KiB each row falls short of its stack size by is the
same fixed part #359 named: the worker loop's own frames below the first
level.

**For a chain the two routes are within 6% of each other, and for a tree they
are not comparable at all**, which is the distinction `TaskGroup::corun`'s
docs are about. A chain has one task outstanding per level, so both routes
nest per level; a tree's id route nests per outstanding *task*. The same
ten-way `skynet` tree at one worker, through each route, run straight:

| route | deepest `MaxDepth` that completes at one worker | first that aborts |
|---|---|---|
| id route | 4 (11110 tasks) | 5 |
| runtime route | **8, the top of upstream's sweep** (111 million tasks, 13.5 s) | not reached |

The id route's row reproduces #340's and #359's figures exactly, which is what
makes the probe trustworthy for the other one; the runtime route's row is
#359's `Runtime::corun` finding holding for a group opened on a `Runtime` as
well -- the whole of upstream's skynet sweep on one worker, in the ten
kilobytes nine levels of 1104 bytes come to, where the id route overflows at
`MaxDepth = 5` and #359 costed upstream's own top at some 11 GiB through it. That is the half of this measurement with no trade-off
in it: on the stack the runtime route is better by orders of magnitude at
every size, and the 6% deeper chain the id route carries is not a size any
shape in this file is chosen against.

### The decision: both routes stay, and the guidance is the ordering

**Both routes stay, and the id route is not "the slow route" -- it is the
route that does not borrow the runtime.** That is what the figures argue for,
and the `DESIGN.md` entry under "Dynamic tasking" is where the decision is.

- **Open the group on a `Runtime` when the parent submits every child before
  it waits.** That is `skynet`, `nqueens`, and any fan-out of a fixed set:
  about a third faster per task at one worker, 61% and 73% faster at 24, the
  only route on which `skynet` gets anything at all out of 23 extra workers,
  and a stack bound per level of recursion rather than per outstanding task,
  which is worth four `skynet` levels and the whole of upstream's sweep.
- **Keep the id route for a body that has work of its own to do between the
  submission and the wait.** On the runtime route it cannot: the group holds
  the runtime, the inline branch needs it, so the submission goes last and
  that costs 4.7x to 27x at 24 workers -- far more than the route's own 50%
  per-task saving is worth. Three of this file's five nested shapes are that
  body, because upstream's tail optimisation is that body.
- **Nothing here is a scheduler change, and the one candidate is an API**
  (#395): a scoped form that hands a body a group and a reborrowed `Runtime`
  together, so that the fast ordering and the fast queue can be had at once.
  The figure it would be measured against is in the ablation above -- the
  `runtime` row's 1.655 ms, below the id route's own 2.659 -- and the bar #395
  carries is the `rustflow-group` id's 2.2967 ms ±1.90%.
- **What this does not say.** It does not say the runtime route is slower at
  24 workers: under the ordering the id route uses it is faster, and under the
  ordering it forces the id route is just as slow. It does not re-publish any
  shape's table, which is why the five sections above point here instead of
  carrying a `rustflow-runtime-group` column of their own. And it does not
  revisit the small sizes' 24-worker regressions (`MaxDepth = 3`, `q = 6`),
  which are #343's and #367's arrival spread rather than either route's.

## The #382 measurement: one block fewer is 7 to 16% a task, and recycling it is not

**Taking the one-node arena out of a free async node (#376) is worth 7.0 to
16.5% a task at one worker on every shape that allocates one, and parking the
node a worker finished on that worker's own free list (#379) is worth nothing
it can prove on the one route that draws from the list.** The two are the first
and the second half of what the "#125 attribution" above called the
irreducible shape of this design -- three heap blocks a free async task, 87%
of the submission loop -- so this is where both are priced, apart and
together, with the blocks counted the same way the "#144 change" counted them.
A free async task is now **2.0159 blocks and 312.12 bytes**, against #125's
3.016 blocks and 848 bytes and #144's 3.0159 and 320.12.

The second finding is the one that needed the pricing. #379 takes the node's
block out of the per-task count of a child submitted through a `Runtime` --
counted below: 4.0000 blocks and 400 bytes a child down to 3.0000 and 144 --
and moves **no time** there: −0.6% and +0.3% on `fibonacci`'s two 24-worker
`rustflow-runtime` ids, against sides inside 1.3% and 1.9% of themselves. Off
that route nothing allocates differently and the readings run from −14.2% to
+15.0% all the same, the two tightest of them pointing opposite ways:
`integrate/rustflow/1w/2` **+12.9%** against sides inside 2.9%, and
`skynet/rustflow-group/24w/3` **−14.2%** against sides inside 3.5%. This run
did not separate the three candidates inside that range and guessed at the
hot path's code; the "#397 attribution" below separated them and found an
allocation after all -- the `Vec` a corun frame's own list parks its blocks
in, one a task on a shape that coruns per child -- which is also where the
decision #379 was left needing is taken.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id -- every id is each shape's own published one, unchanged |
| `ulimit -s` | 8192 KiB |
| Date | 2026-11-19 |
| **Load average** | **0.53 to 14.8 before and after each measurement, 1 or 2 runnable threads at every reading, and all of it this run's own** |

The box was this run's alone -- no sibling agent, nothing else building -- and
the one-minute figure reaching 14 is the run's own: eighteen `bench-ab.sh`
invocations, each of which builds two sides with `CARGO_BUILD_JOBS=4` and then
runs six rounds of 24-worker criterion ids. `/proc/loadavg` was read either
side of every measurement, and its **runnable-task** field -- the one that says
whether anything else wanted a core -- read 1 or 2 at forty-one of the
forty-two readings below, the forty-second reading 6 between two of the runs:

| measurement | before | after |
|---|---|---|
| `async_task`, the span | 0.53 0.75 1.85 | 13.54 7.20 4.22 |
| `fibonacci`, the span | 3.56 5.50 3.87 | 11.64 7.93 5.17 |
| `skynet`, the span | 7.06 7.17 5.00 | 7.75 7.67 5.48 |
| `nqueens`, the span | 6.55 7.42 5.42 | 11.13 8.69 6.13 |
| `integrate`, the span | 10.24 8.55 6.10 | 12.25 9.35 6.66 |
| `merge_sort`, the span | 10.44 9.06 6.60 | 6.76 7.92 6.50 |
| `async_task`, #379 alone | 6.22 7.79 6.46 | 14.83 11.21 8.18 |
| `fibonacci`, #379 alone | 11.55 10.66 8.04 | 12.74 10.77 8.59 |
| `skynet`, #379 alone | 5.53 9.10 8.14 | 7.43 8.50 8.03 |
| `nqueens`, #379 alone | 7.43 8.50 8.03 | 10.79 9.11 8.29 |
| `integrate`, #379 alone | 9.92 8.96 8.25 | 11.90 9.67 8.57 |
| `merge_sort`, #379 alone | 11.90 9.67 8.57 | 3.73 7.44 7.91 |
| `async_task`, #376 alone | 1.62 6.29 7.49 | 12.14 9.28 8.42 |
| `fibonacci`, #376 alone | 10.28 8.97 8.33 | 12.18 9.58 8.64 |
| `skynet`, #376 alone | 11.21 9.42 8.59 | 8.93 9.07 8.56 |
| `nqueens`, #376 alone | 8.93 9.07 8.56 | 10.00 9.31 8.70 |
| `integrate`, #376 alone | 9.20 9.15 8.66 | 13.66 10.47 9.17 |
| `merge_sort`, #376 alone | 13.66 10.47 9.17 | 6.57 8.88 8.78 |
| the block counts, today's `main` | 1.07 0.86 1.97 | 0.77 0.81 1.90 |
| the block counts, the two earlier refs | 1.36 4.23 6.83 | 1.36 4.23 6.83 |
| the two pinned `async_task` absolutes | 2.12 7.05 8.15 | 1.94 6.69 8.00 |

### Method: three spans, because the one the issue names is not two changes

Every A/B below is one `devtools/bench-ab.sh <A> <B> --bench <shape> --rounds 3
-- 'rustflow'` run: two worktrees, two target directories, three interleaved
rounds a side, each side's spread over its own rounds printed before the
B-over-A ratio of medians. Three spans, measured on each of the six shapes --
eighteen runs, 96 matched ids, nothing unmatched:

- **the whole span, `2e90fcb -> 2efc96b`**, which is what the issue asked for.
  It is not #376 plus #379 alone: it also carries #375 (`sort_by`'s
  pattern-defeating partition), #372 (a task group opened from a `Runtime`
  getting its worker's queue) and #374 (the splitting partitioner). None of
  those three is on the async submission path, and no id below reaches the two
  partitioners at all, but #372 rewrites `TaskGroup::submit` into a two-armed
  `Route` that every id below does reach.
- **#376 alone, `2e90fcb -> 127e3b7`.**
- **#379 alone, `da36b9d -> 2efc96b`.**

The two changes are therefore priced apart twice over: once by the two
single-commit spans, and once by whether the three-span row adds up. It
mostly does -- `fibonacci/rustflow-group/1w/15` reads −12.5% for #376 and
+8.6% for #379 against +1.6% for the span, and `integrate/rustflow/1w/2`
−9.5% and +12.9% against +3.0% -- which is the check that no third commit in
the span is doing the work.

A cell below is a **median of three rounds**, which is what `bench-ab`
reports and not criterion's own interval; the ± figures in this section's prose
are each side's spread over its own rounds, by the rule the "What is measured"
section states. The two absolutes at the end are the exception and carry
criterion's point estimate with half its 95% interval, because they are single
criterion runs rather than an A/B. **Bold** marks a change outside both sides'
own spreads by more than the 1.25x `bench-ab` calls resolved, *italic* a
marginal one between 1.00x and 1.25x, and plain text a change inside one of
the two spreads, which is not a result.

### The blocks, counted on both sides of both changes

#144's method, re-run: a counting global allocator in an example that is not
part of this branch, one instrumented run per ref per route, 65536 external
`silent_async`s (or 121392 spawned children for the recursion) after a warm-up
run of the same shape, counted by block size. Three routes, because the
changes do not reach them alike: **external**, which is `async_task`'s
`rustflow` id; the **id route**, which is a `TaskGroup` from
`Executor::task_group` inside a worker -- `async_task`'s `rustflow-group` id,
and the route every one of the five nested shapes opens its groups through;
and the **runtime route**, `benches/fibonacci.rs`'s `rustflow-runtime`
recursion through `Runtime::silent_async_runtime`.

| route | block | `2e90fcb` | after #376 | today's `main` |
|---|---|---|---|---|
| external | the body's box | 8 B x 1.0000 | 8 B x 1.0000 | 8 B x 1.0000 |
| external | `Arc<AsyncNode>` | 88 B x 1.0000 | **256 B x 1.0000** | 256 B x 1.0000 |
| external | the arena's `Vec<Option<Node>>` | **200 B x 1.0000** | -- | -- |
| external | the spill buffers growing | 3032 B x 0.0159 | 3032 B x 0.0159 | 3032 B x 0.0159 |
| external | **per task** | **3.0159 blocks, 344.12 B** | **2.0159 blocks, 312.12 B** | 2.0159 blocks, 312.12 B |
| id route | **per task** | 3.0159 blocks, 344.12 B | 2.0159 blocks, 312.12 B | **2.0160 blocks, 312.13 B** |
| runtime route | the body's box and the slot | 24 B x 2.0000 | 24 B x 2.0000 | 24 B x 2.0000 |
| runtime route | the child's `Nested` | 96 B x 1.0000 | 96 B x 1.0000 | 96 B x 1.0000 |
| runtime route | `Arc<AsyncNode>` | 88 B x 1.0000 | 256 B x 1.0000 | **256 B x 0.0000** |
| runtime route | the arena's `Vec<Option<Node>>` | 200 B x 1.0000 | -- | -- |
| runtime route | **per task** | **5.0000 blocks, 432.00 B** | **4.0000 blocks, 400.00 B** | **3.0000 blocks, 144.00 B** |

- **The count reproduces #144's and corrects one of its figures.** 3.0159
  blocks a task on the external route is that section's number exactly, down
  to the 0.0159 of a spill buffer; the bytes read 344.12 where it read 320.12,
  because the arena it sized to one node is **200 bytes** at the span's A side
  against the 176 it measured the day before -- an `Option<Node>` 24 bytes
  larger, from changes that are nobody's measurement here. Three blocks, in
  other words, was still three blocks on the day #376 started.
- **#376 is one block and 32 bytes a task**, and the second figure is the one
  worth noticing: the `Arc<AsyncNode>` grows from 88 bytes to 256 as it
  swallows the node, so what the change buys is almost entirely the *count*.
  That is #125's "a block's size is worth less than its existence" measured
  from the other end, and the per-task timings below are what it is worth.
- **#379 is one more block, on the runtime route only.** A child there costs
  3.0000 blocks where it cost 4.0000, and the node's 256-byte block is
  allocated **once per run** rather than once per child at one worker (one
  block in 121392) and 490 times in 121392 at 24 workers, where a child
  finished on one worker is sometimes submitted from another. Two of the three
  blocks left are the benchmark's own result slot and the body's box, so what
  the scheduler still pays per child there is the body's box and the
  **`Nested`** that parents it -- 96 bytes, allocated per child on the
  routes that have a parent, and once per group on the route that does not.
- **The id route never collects.** 256 B x 1.0000 on today's `main`: a
  `TaskGroup` opened on an `Executor` submits through
  `Submission::under_anchor`, which names no worker, so there is no list to
  draw from and the node is allocated every task exactly as before. Five of
  the six shapes below are that route, which is why #379's figures on them are
  about something other than allocation.

### The figures: 32 ids, three spans

Before and after are the whole span's own two sides. The per-cent columns are
B-over-A, so a negative number is the change making the id faster.

| shape | id | before | after | the span | #376 | #379 |
|---|---|---|---|---|---|---|
| `async_task` | `rustflow/1w/4096` | 3.536 ms | 2.886 ms | **−18.4%** | −16.0% | −0.4% |
| `async_task` | `rustflow/1w/65536` | 55.36 ms | 45.98 ms | **−17.0%** | **−13.6%** | −3.0% |
| `async_task` | `rustflow/24w/4096` | 5.181 ms | 4.768 ms | *−8.0%* | −5.9% | +5.0% |
| `async_task` | `rustflow/24w/65536` | 86.12 ms | 70.28 ms | **−18.4%** | *−9.8%* | −6.2% |
| `async_task` | `rustflow-group/1w/4096` | 755.9 µs | 628.2 µs | **−16.9%** | **−15.5%** | **−3.9%** |
| `async_task` | `rustflow-group/1w/65536` | 12.32 ms | 10.06 ms | **−18.3%** | **−14.1%** | *−5.1%* |
| `async_task` | `rustflow-group/24w/4096` | 4.922 ms | 4.373 ms | **−11.2%** | **−12.2%** | **+2.1%** |
| `async_task` | `rustflow-group/24w/65536` | 76.35 ms | 68.09 ms | **−10.8%** | **−13.8%** | **+2.5%** |
| `fibonacci` | `rustflow-group/1w/12` | 84.30 µs | 88.06 µs | +4.5% | **−16.5%** | *+8.8%* |
| `fibonacci` | `rustflow-group/1w/15` | 323.9 µs | 329.0 µs | +1.6% | **−12.5%** | **+8.6%** |
| `fibonacci` | `rustflow-group/24w/12` | 1.086 ms | 1.072 ms | −1.3% | −1.6% | **−4.1%** |
| `fibonacci` | `rustflow-group/24w/15` | 2.060 ms | 2.059 ms | −0.0% | +1.2% | −1.0% |
| `fibonacci` | `rustflow-runtime/1w/12` | 50.12 µs | 46.82 µs | −6.6% | *−11.7%* | −0.7% |
| `fibonacci` | `rustflow-runtime/1w/15` | 194.3 µs | 180.8 µs | −6.9% | **−13.6%** | −1.5% |
| `fibonacci` | `rustflow-runtime/24w/12` | 843.9 µs | 831.6 µs | **−1.5%** | *+1.0%* | −0.6% |
| `fibonacci` | `rustflow-runtime/24w/15` | 1.319 ms | 1.300 ms | −1.4% | +2.1% | +0.3% |
| `skynet` | `rustflow-group/1w/3` | 243.1 µs | 212.6 µs | **−12.6%** | **−14.1%** | +0.2% |
| `skynet` | `rustflow-group/1w/4` | 2.336 ms | 2.016 ms | **−13.7%** | **−14.4%** | −0.4% |
| `skynet` | `rustflow-group/24w/3` | 525.6 µs | 442.7 µs | **−15.8%** | **−3.1%** | **−14.2%** |
| `skynet` | `rustflow-group/24w/4` | 2.346 ms | 2.336 ms | −0.4% | *−3.4%* | +1.1% |
| `nqueens` | `rustflow-group/1w/6` | 83.66 µs | 83.14 µs | −0.6% | −5.8% | +7.4% |
| `nqueens` | `rustflow-group/1w/8` | 1.195 ms | 1.159 ms | −2.9% | **−7.0%** | +3.7% |
| `nqueens` | `rustflow-group/24w/6` | 754.2 µs | 742.6 µs | −1.5% | −1.6% | −3.5% |
| `nqueens` | `rustflow-group/24w/8` | 2.751 ms | 2.676 ms | −2.7% | +0.8% | +4.0% |
| `integrate` | `rustflow/1w/1` | 277.2 µs | 292.4 µs | **+5.5%** | **−11.3%** | **+15.0%** |
| `integrate` | `rustflow/1w/2` | 607.8 µs | 625.7 µs | **+3.0%** | **−9.5%** | **+12.9%** |
| `integrate` | `rustflow/24w/1` | 1.235 ms | 1.253 ms | +1.4% | −1.0% | +2.2% |
| `integrate` | `rustflow/24w/2` | 2.177 ms | 2.061 ms | **−5.3%** | −4.1% | −0.1% |
| `merge_sort` | `rustflow/1w/10000` | 257.8 µs | 258.4 µs | +0.3% | −5.4% | −2.0% |
| `merge_sort` | `rustflow/1w/1000000` | 54.85 ms | 55.60 ms | +1.4% | −3.5% | +1.1% |
| `merge_sort` | `rustflow/24w/10000` | 838.4 µs | 710.0 µs | −15.3% | −1.5% | −5.5% |
| `merge_sort` | `rustflow/24w/1000000` | 16.55 ms | 17.20 ms | +3.9% | −0.4% | −3.0% |

The absolutes in the first two columns are **not** comparable with the shape
tables above, and `async_task/rustflow/1w/65536`'s 55.36 ms against that
section's 29 ms is the case the "#144 change" section already recorded:
unpinned, that id's processes read 29 to 50 ms on this box depending on where
the producer and its one worker land, so a figure from it is a ratio's
denominator and nothing else. Within one row the two sides are measured
minutes apart in alternating rounds under the same drift, which is the one
thing the method does protect. The two pinned absolutes below are what the
section quotes for that id instead.

### #376: 7 to 16% a task wherever a submission is most of the shape

At **one worker** the change is a result on every shape whose per-task cost is
a submission, and the sizes agree with each other:

| shape | id | #376 | each side's own spread |
|---|---|---|---|
| `fibonacci` | `rustflow-group/1w/12` | **−16.5%** | 6.4% / 10.8% |
| `async_task` | `rustflow-group/1w/4096` | **−15.5%** | 3.3% / 4.7% |
| `skynet` | `rustflow-group/1w/4` | **−14.4%** | 3.8% / 2.9% |
| `async_task` | `rustflow-group/1w/65536` | **−14.1%** | 2.4% / 3.6% |
| `fibonacci` | `rustflow-runtime/1w/15` | **−13.6%** | 2.5% / 5.4% |
| `async_task` | `rustflow/1w/65536` | **−13.6%** | 1.3% / 1.7% |
| `integrate` | `rustflow/1w/1` | **−11.3%** | 2.4% / 6.3% |
| `integrate` | `rustflow/1w/2` | **−9.5%** | 5.4% / 7.0% |
| `nqueens` | `rustflow-group/1w/8` | **−7.0%** | 2.3% / 3.5% |
| `merge_sort` | `rustflow/1w/1000000` | −3.5% | 2.4% / 4.5% |

**One block in three is worth between 7 and 16% of a free async task**, and
the ordering of that column is the shape's own per-task
arithmetic rather than anything about the change: `fibonacci` and `skynet`
bodies hold nothing, `nqueens` copies a board and prunes a column, and
`merge_sort`'s million-element id is twenty million compares with 1023
submissions hidden behind them, which is why it is the one row that reads
nothing. `integrate`'s two ids are the cross-check that the ordering is
arithmetic and not route: they are the same id route as `skynet`'s and read
two thirds of its figure, over a body of six `f64` operations.

Read against the block counts, that is **−32 bytes and −1 block buying 7 to
16%**, which closes #125's G-against-H question from the other side: that run
measured one extra 64-byte block at 69 to 75 ns and one extra 704-byte block
at 224, and could not tell a count from a size. Here the count falls by one
while the size of what is left *rises* by 168 bytes, and the id gets 13%
faster. A block's existence is the whole of it.

At **24 workers** only the shape that is nothing but submissions keeps it:
`async_task`'s group id reads −12.2% and −13.8%, its external id −5.9% and
−9.8%, and every nested recursion reads between −4.1% and +2.1% with its
sides disagreeing by as much. That is the "#380 measurement" above showing
through -- at 24 workers a nested recursion's cost is the arrival of one
outstanding child among 23 awake thieves, 59 µs a handoff, and a block saved
in the submission disappears into it.

### #379: nothing on the route that draws, and no sign off it

The route the list was built for is `fibonacci`'s `rustflow-runtime` id, and
the block counts above show the list doing exactly what it is for there: the
node's block is drawn from it for all but one child in 121392 at one worker.
The time does not move.

| id | #379 | each side's own spread |
|---|---|---|
| `rustflow-runtime/24w/12` | −0.6% | 1.3% / 1.2% |
| `rustflow-runtime/24w/15` | +0.3% | 1.8% / 1.9% |
| `rustflow-runtime/1w/12` | −0.7% | 9.9% / 11.9% |
| `rustflow-runtime/1w/15` | −1.5% | 14.4% / 3.2% |

The two 24-worker rows are a **resolved no change**: sides inside 1 to 2% of
themselves and a difference of half a percent. The two one-worker rows resolve
nothing either way, their two sides disagreeing by 10 to 14% between their own
rounds. So the honest statement of what recycling a block is worth, where it
is drawn, is **half a percent of a child at 24 workers, and below what this
box can see at one** -- that child being 183 ns at one worker and 1.3 µs at 24
-- against a block count that falls by a quarter and a byte count that falls
by 64%. Which is a statement about `glibc`'s allocator as much as
about this port: a block allocated and freed by the same thread, 121392 times
in a row at the same size, costs about what a `RefCell` borrow, an
`Arc::get_mut` and a `Vec` push cost, so replacing one with the other is a
wash. Upstream's `TF_ENABLE_TASK_POOL` defends against a different regime --
its `ObjectPool` is a cross-thread recycler, and #125 priced *that* regime at
691 to 737 ns a block.

Off that route nothing allocates differently, and the readings still move:

| shape | id | #379 | each side's own spread |
|---|---|---|---|
| `integrate` | `rustflow/1w/1` | **+15.0%** | 2.2% / 2.2% |
| `integrate` | `rustflow/1w/2` | **+12.9%** | 1.6% / 2.9% |
| `fibonacci` | `rustflow-group/1w/15` | **+8.6%** | 2.4% / 0.6% |
| `nqueens` | `rustflow-group/1w/6` | +7.4% | 8.0% / 2.0% |
| `skynet` | `rustflow-group/1w/4` | −0.4% | 5.4% / 4.1% |
| `async_task` | `rustflow-group/1w/65536` | *−5.1%* | 4.7% / 3.3% |
| `skynet` | `rustflow-group/24w/3` | **−14.2%** | 0.7% / 3.5% |

Four of the twelve one-worker ids on routes that cannot draw are resolved, and
three of those four are **regressions**: `integrate`'s two sizes agreeing at
+12.9 and +15.0% against sides inside 3%, and `fibonacci`'s group id at
+8.6%. The two resolved readings in the other direction are
`async_task/rustflow-group/1w/65536` at −5.1% and
`skynet/rustflow-group/24w/3` at −14.2%, and the second of those does not
reproduce at the shape's other size (+1.1%, inside the spread). What the
extra per-task work on these routes is, read off the code, is one `Arc` clone
and drop of every finished free node in `WorkerThread::spend_job` plus a
`RefCell` borrow and a length test in `FreeNodes::recycle` once the 32-block
list is full -- which is a few nanoseconds and not 13% of `integrate`'s
338-ns task, so most of that range looked like code layout and inlining on
the one-worker corun path. **This run did not separate those**, and the "#397
attribution" below is the run that did: the `recycle` call is the whole of
the cost, the clone measures at nothing, and what the call pays on a route
that cannot draw is a heap block a task rather than any of this paragraph's
nanoseconds. #379 stays, parking nothing on a list no submission has drawn
from.

### The two absolutes, pinned, on today's `main`

`async_task`'s two one-worker ids, each in its own criterion process, two
rounds with the order reversed, under `taskset -c 4,5` -- two physical cores of
one CCD for the producer and the one worker, which is #144's protocol for
making this shape's absolute reproducible:

| id | round 1 | round 2 | per task |
|---|---|---|---|
| `async_task/rustflow/1w/65536` | 22.565 ms ±0.22% | 23.424 ms ±0.68% | **344 / 357 ns** |
| `async_task/rustflow-group/1w/65536` | 9.8517 ms ±0.12% | 9.9287 ms ±0.17% | **150 / 152 ns** |

So **an externally submitted free async task is ~350 ns on today's `main`**,
against #144's pinned 27.7 to 28.0 ms (423 to 427 ns) for the same id on the
same protocol and #125's 438 to 478 ns before that change; and the same
construction submitted from a worker through a task group is **~150 ns**,
against #144's 182 and #125's 274. The two protocols agree: −16 to −19% from
the pinned pair against the A/B's −13.6% and −14.1% for #376, with #379
inside the noise on the external id.

### The decision: #376 is settled, #379 is #397's

- **#376 stays, and this is what it bought**: one block in three off every
  free async task, 9.5 to 16.5% a task at one worker on four of the six shapes
  and 7.0% on a fifth, 12 to 14% at 24 workers on the shape that is only
  submissions, and nothing
  at 24 workers on a nested recursion because #380's handoff is what costs
  there instead. No shape regressed on it: the most positive reading in the
  whole #376 column is `fibonacci/rustflow-runtime/24w/15` at +2.1%, inside
  its sides' own 3.5%.
- **#379's win is a footprint**: 400 bytes a child down to 144 on the runtime
  route, with the node's block allocated once a run instead of once a child.
  As a time it is unmeasurable where it applies and unresolved in sign where
  it does not, and the "#397 attribution" below is where the ablation and the
  choice between fixing the cost and reverting the change were taken: the
  cost came off, and the change stays. Nothing in this section is an argument
  for keeping it on the strength of the time it saves, because it saves none
  that this box can see.
- **What would reopen #376's half** is a third block to take out, and the
  counts above name the candidates: the body's box, which is the type erasure
  and cannot go, and the per-child `Nested` at 96 bytes, which is the one
  block a *parented* child pays that the external route does not. That is a
  design question about who owns a child's join counter rather than a tuning
  one, and nothing here proposes it.

## The #381 measurement: the share reaches the pool, not the tree, and the sort's gain is a call shape

**Three A/Bs on the fan-out changes, and none of the three ships a line.** The
bulk wake was answered without code before this run began; the splitting
partitioner reaches 21 to 24 of 24 workers where the guided one reaches 21 to
22, for a wall of 1.9 to 2.9 ms against 0.5 to 0.6 ms, so it stays the option
#374 made it and does not become a default; and the pattern-defeating
partition #375 shipped is *not* what moved the `sort` figures -- three of that
shape's four ids never reach a partition of this crate's at all, and the one
that does moves 3.5% against a side that disagrees with itself by 15%.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id, as every target declares |
| Date | 2026-11-19 |
| **Load average** | **each measurement's own, below; 0.2 to 11.4 before a shape and 0.2 to 12.0 after it, with nothing else on the box** |

The box was this run's alone: no sibling agent, no other `cargo`, and
`/proc/loadavg` read either side of every measurement below --
`for_each` 0.70 to 8.82, `reduce_sum` 7.01 to 10.60, `primes` 9.75 to 11.37,
`mandelbrot` 9.63 to 11.99, the counter sweep 0.29 to 0.29, the `sort` A/B
8.02 through 3.99 round by round, the re-taken `sort` table 0.34 to 2.80 and
the call-shape ablation 0.22 to 0.22 with `/proc/pressure/cpu`'s `some avg10`
at 0.00. A shape's "before" is its predecessor's decay rather than zero,
which is the condition "What is measured" describes; the PR records every
pair. Every criterion figure is **one** run of one id, quoted as the point
estimate of the mean with half the width of the 95% confidence interval as a
plus-or-minus percentage.

### Part 1: the bulk wake, already answered and not re-measured

**#378 closed with no code, so this part is a citation rather than a
measurement.** "The #367 measurement" above is the gate it closed on: 24
`notify_one` calls produce 24 or 25 `unpark` calls and 23 to 25 returned
parks, **zero** parks are taken while a job is queued in any deque or buffer,
and the batched `notify_n` form #378 proposed was implemented, counted and
reverted -- the push loop costs the same 45-60 µs for 23 tasks either way,
the distinct-worker count stays inside the unbatched path's own range at 20,
100 and 500 µs bodies, and at the longest bodies two runs of three recorded a
submitter stall of 0.8 to 1.1 ms. There is no before-and-after to run, so
there is no `bench-ab` table here and no observer count: the mechanism this
part was filed to price does not exist in the tree on either side.

### Part 2: the splitting partitioner against the shipped ids, in one process

Four bench files carry the `rustflow-split` ids #374 added -- `for_each`,
`reduce_sum`, `primes` and `mandelbrot` -- and each runs the *same* task as
its `rustflow` id under `SplittingPartitioner` instead of the partitioner the
shape ships with, **at the same chunk size**: guided floor 1 against
splitting floor 1 on `for_each` and `reduce_sum`, `DynamicPartitioner::new(10)`
against `SplittingPartitioner::new(10)` on `primes`, and `new(1)` against
`new(1)` on `mandelbrot`. So a row below changes how a chunk reaches a worker
and nothing else. Each file's twelve ids were measured in one `cargo bench`
per shape, the three columns of a row seconds apart in the same process,
which is the one thing this environment protects.

**`benches/scan.rs` has no such id, and that is a finding rather than an
omission**: the scan family takes no partitioner anywhere in the port because
it takes none upstream. `src/algorithm/scan.rs`'s provenance names its six
entry points as `make_inclusive_scan_task`, `make_exclusive_scan_task` and
the four `make_transform_*_scan_task` overloads at v4.1.0, and not one of
them has a `part` parameter. Giving scan one would be a departure from
upstream's user-facing surface, which is its own decision and not this
measurement's to take.

| shape | size | workers | `rustflow` | `rustflow-split` | split ÷ shipped | `rayon` |
|---|---|---|---|---|---|---|
| `for_each` | 1000 | 1 | 14.95 µs ±0.6% | 14.94 µs ±0.6% | **1.00x** | 9.858 µs ±1.9% |
| `for_each` | 1000 | 24 | 31.52 µs ±0.2% | 1.065 ms ±0.9% | **33.8x** | 30.45 µs ±2.5% |
| `for_each` | 1000000 | 1 | 8.526 ms ±0.2% | 8.108 ms ±0.6% | **0.95x** | 7.601 ms ±0.2% |
| `for_each` | 1000000 | 24 | 652.8 µs ±1.6% | 2.222 ms ±1.1% | **3.40x** | 754.7 µs ±2.4% |
| `reduce_sum` | 1000 | 1 | 8.874 µs ±12.9% | 7.318 µs ±0.3% | 0.82x | 4.664 µs ±0.2% |
| `reduce_sum` | 1000 | 24 | 113.5 µs ±2.0% | 446.7 µs ±1.4% | **3.94x** | 28.13 µs ±2.3% |
| `reduce_sum` | 1000000 | 1 | 654.8 µs ±0.2% | 643.2 µs ±0.2% | **0.98x** | 618.7 µs ±0.1% |
| `reduce_sum` | 1000000 | 24 | 276.4 µs ±0.8% | 1.369 ms ±1.4% | **4.95x** | 109.2 µs ±0.6% |
| `primes` | 10000 | 1 | 67.73 µs ±0.6% | 67.90 µs ±0.5% | **1.00x** | 60.77 µs ±0.2% |
| `primes` | 10000 | 24 | 138.2 µs ±1.1% | 970.1 µs ±1.2% | **7.02x** | 51.12 µs ±1.0% |
| `primes` | 1000000 | 1 | 30.36 ms ±0.1% | 30.37 ms ±0.1% | **1.00x** | 28.99 ms ±0.1% |
| `primes` | 1000000 | 24 | 2.937 ms ±2.3% | 3.719 ms ±1.2% | **1.27x** | 2.595 ms ±0.5% |
| `mandelbrot` | 100 | 1 | 17.08 ms ±0.1% | 17.09 ms ±0.1% | **1.00x** | 16.10 ms ±0.1% |
| `mandelbrot` | 100 | 24 | 1.012 ms ±0.5% | 1.501 ms ±0.1% | **1.48x** | 1.211 ms ±0.4% |
| `mandelbrot` | 500 | 1 | 424.0 ms ±0.1% | 423.2 ms ±0.0% | **1.00x** | 399.3 ms ±0.0% |
| `mandelbrot` | 500 | 24 | 21.46 ms ±0.3% | 36.42 ms ±0.4% | **1.70x** | 22.33 ms ±0.3% |

Three readings, in the order they decide the question.

- **At one worker the two columns are the same program, and the figures say
  so.** Seven of the eight one-worker rows are inside a percent of each other;
  the two exceptions are `for_each/1w/1000000` at 0.95x, where the split id's
  one unstolen share is a single body call while the guided cursor still walks
  its geometric sequence of chunks, and `reduce_sum/1w/1000`, whose shipped id
  carries a ±12.9% interval and 8 of 20 outliers and so resolves nothing.
  That tie is the mechanism confirming itself: **nothing splits without a
  steal**, so a pool of one is this partitioner's control rather than a row of
  its own.
- **At 24 workers it costs between 1.27x and 33.8x, and the ordering is by how
  much body there is to amortise it over.** `primes` at a million, 2.9 ms of
  trial division over 100000 chunks of ten, pays 1.27x; `mandelbrot` at 500
  lines pays 1.70x; `for_each` and `reduce_sum` at a million, where the body
  is one `tan` or one addition an element, pay 3.40x and 4.95x; and the two
  small sizes, where the whole job is 10 to 60 µs of work, pay 3.94x to 33.8x
  because the dispatch *is* the row.
- **The shape it was built for is the one where it loses most clearly.**
  `mandelbrot` is this suite's load-imbalance benchmark -- 100 to 500 lines
  whose cost varies by two orders of magnitude, handed out one at a time -- so
  a scheme that re-splits on a steal should have its best case here. It is
  1.48x and 1.70x behind the dynamic chunk of one it replaces, and 1.24x and
  1.63x behind rayon, on a shape where the shipped id is *ahead* of rayon at
  both sizes.

### What the split buys, counted: a share per task, and not the tree

The #343 observer, registered on the executor and counting which worker enters
each dispatched task, over the same `for_each_by_index` the `for_each` table
builds: `n = 1000000` at 24 workers, six runs per partitioner with the pool
confirmed fully parked before each window (`Executor::num_waiters() == 24`),
a fresh executor per row so that run 0 is a cold pool. "Tasks" is the
observer's own entry count, which for a splitting row counts the re-splits.

| partitioner | tasks | distinct workers, six runs | wall |
|---|---|---|---|
| guided, floor 1 (`DefaultPartitioner`) | 25 | **21-22** / 24 | 0.48-0.63 ms |
| splitting, floor 1 (`default()`) | **121-157** | **21-24** / 24 | **1.90-2.86 ms** |
| guided, 4096 | 25 | 21-23 / 24 | 0.48-0.71 ms |
| splitting, 4096 | 88-94 | 22-23 / 24 | 1.03-1.28 ms |
| guided, 65536 | 25 | **17** / 24 | 0.73-0.96 ms |
| splitting, 65536 | 25 | 22 / 24 | 0.72-0.85 ms |
| guided, 262144 | 25 | **11-15** / 24 | 2.15-2.38 ms |
| splitting, 262144 | 25 | 21-23 / 24 | 0.72-0.77 ms |

And the same sweep at `n = 1000`, where the whole job is 10 to 12 µs of work:

| partitioner | tasks | distinct workers, six runs | wall |
|---|---|---|---|
| guided, floor 1 | 24 | **13-16** / 24 | 0.05-0.08 ms |
| splitting, floor 1 | 66-82 | **17-21** / 24 | 0.33-0.90 ms |
| guided or splitting, 4096 | **1** | 1 / 24 | 0.02-0.03 ms |

- **The re-split is not what reaches the workers.** The two bottom rows of the
  first table are the splitting partitioner with its re-splits turned off by a
  floor above `n / w`: 25 tasks, no splits, and still **21 to 23** of 24
  workers, which is the count the splitting default reaches *with* 121 to 157
  tasks. What reaches those workers is the share the dispatch hands each task
  -- `n / w` of the range, walked in one body call -- against the guided
  cursor, whose chunk floor decides how much of the range is left by the time
  a late worker arrives. At a floor of 65536 the guided cursor has 16 chunks
  for 24 tasks and reaches 17 workers, and at 262144 it has 4 and reaches 11
  to 15; the splitting walk at those same floors reaches 22 either way.
  #367's reading that `SplittingPartitioner` "is the one knob that moves the
  count where it is low" survives, but the knob turns out to be the static
  share and not the tree.
- **A re-split costs 12 to 16 µs, which is 7x what a fan-out task costs.**
  The splitting default's 1.90-2.86 ms against the same partitioner's
  non-splitting 0.72-0.77 ms is +111 tasks for +1.49 ms at the medians --
  **13 µs a split**, and 12 to 16 µs taken run by run -- where "The #367
  measurement" priced a plain fan-out push, node and two boxed closures and
  deque push and `notify_one`, at about **2 µs**. A split is one
  `Scope::spawn_runtime` of a half share, so the gap is not accounted for by
  the submission. **#400 has since attributed it, and it is none of the four
  candidates this section listed**: it is the join that `Scope::spawn_runtime`
  obliges, whose wait cannot park and costs 143 to 172 µs of CPU a round.
  "The #400 attribution" below is that run.
- **The default floor is the type's worst setting.** 1.90-2.86 ms at floor 1,
  1.03-1.28 at 4096, 0.72-0.85 at 65536, with the distinct-worker count flat
  at 21 to 24 across all three: the floor buys nothing the type exists for and
  costs 2.5 to 3.5x. `SplittingPartitioner` is the port's own, so unlike
  `GuidedPartitioner`'s floor of 1 -- upstream's, which #343 declined to
  change for that reason -- this default is ours to pick, and **#401** was
  that decision with its figures. **It has since been taken, and the floor
  stays**: "The #401 decision" at the end of this file swept the floor alone
  -- holding the chunk walk at one body call a share, which the 4096 and 65536
  rows above do not -- and found the band from `n / (2w)` to `n / (16w)`
  separating neither from its neighbours nor from a floor that splits nothing,
  with no splitting floor inside 1.5x of the guided default. The three rows
  above are now in the type's own docs as the price of the default.
- **The cold-pool advantage #367 read off run 0 does not reproduce here.**
  That section's partitioner table has guided at 13 of 24 on the first fan-out
  an executor ever ran against the splitting rows' 16 to 19, and called the
  splitting dispatch "the one knob that moves the count where it is low". In
  this sweep run 0 reads 21 of 24 for guided and 21 or 22 for splitting, and
  no row's run 0 is its lowest. The two sweeps differ in their harness rather
  than in the code they call, so what this one adds is that the cold run is
  not reliably the low one and the cold-pool margin is not a figure to lean
  on; the static-share mechanism above is, and it is visible warm.
- **At a thousand elements a chunk size still beats both.** The
  `n <= chunk_size` early-out takes the whole job sequentially in
  0.02-0.03 ms, against 0.05-0.08 for the guided default and 0.33-0.90 for the
  splitting one, which is #343's own finding reproduced on both partitioners.

### Part 3: the sort partition, before and after #375

`devtools/bench-ab.sh 127e3b7 a03d9dd --bench sort --rounds 3`: interleaved
rounds over both sizes and both worker counts, each side built into its own
worktree and its own target directory. Medians of three rounds, with each
side's spread against its own rounds -- the rule a difference is read against.

| id | before #375 | after #375 | B/A | A spread | B spread | verdict |
|---|---|---|---|---|---|---|
| `rustflow/1w/1000` | 19.28 µs | 15.11 µs | 0.784x | 1.010x | **1.365x** | inside B's own spread |
| `rustflow/1w/1000000` | 36.62 ms | **20.97 ms** | **0.573x** | 1.006x | 1.049x | a result, 15.3x the spread |
| `rustflow/24w/1000` | 21.99 µs | 18.12 µs | **0.824x** | 1.061x | 1.084x | a result, 2.6x the spread |
| `rustflow/24w/1000000` | 15.41 ms | 15.95 ms | 1.035x | **1.153x** | 1.052x | inside A's own spread |
| `rayon/1w/1000` | 13.03 µs | 13.35 µs | 1.025x | 1.308x | 1.283x | control: inside |
| `rayon/1w/1000000` | 25.25 ms | 23.99 ms | 0.950x | 1.048x | 1.042x | control: marginal |
| `rayon/24w/1000` | 16.43 µs | 15.99 µs | 0.974x | 1.011x | 1.010x | control: "a result" |
| `rayon/24w/1000000` | 3.417 ms | 3.437 ms | 1.006x | 1.042x | 1.051x | control: inside |

**The four `rayon` rows are an A/A control and they set this shape's floor.**
#375 touched `src/algorithm/sort.rs`, `tests/sort.rs` and
`tests/properties.rs` and nothing else -- `git diff --stat` says so -- so the
baseline column runs identical code on both sides, and whatever it reports is
noise. It reports up to 5.0%, and on `rayon/24w/1000` the script's own rule
calls a 2.6% difference "a result" because that id's within-side spread is
1.0%. So read a `rustflow` row's 3.5% as nothing and its 42.7% as something,
and treat anything between 3% and 6% on this shape as unresolved.

### Three of the four sort ids never reach the new partition

`Runtime::sort_by` short-circuits before any partition of this crate's runs:

```rust
let cutoff = parallel_sort_cutoff::<T>();
if self.num_workers() <= 1 || data.len() <= cutoff {
    data.sort_unstable_by(&cmp);
    return;
}
```

`parallel_sort_cutoff::<f64>()` is 4096, so **`1w/1000`, `1w/1000000` and
`24w/1000` all take that line** -- one worker, or a thousand elements -- and
the only id of this shape that reaches `parallel_pdqsort` is
`24w/1000000`, which the table above reads at 1.035x inside a 1.153x spread.
That line is character for character the same in `127e3b7` and `a03d9dd`.

What moved, then, is the codegen of that one call, and three sorts of the same
million `f64`s in one binary isolate it:

| call shape | wall, two runs each |
|---|---|
| `data.sort_unstable_by(&cmp)` -- what the short-circuit writes | **37.76, 39.37 ms** |
| `data.sort_unstable_by(cmp)` | **21.90, 21.88 ms** |
| `data.sort_unstable_by(\|a, b\| cmp(a, b))` | 21.80, 21.81 ms |
| `buffer.sort_unstable_by(f64::total_cmp)`, no generic in the way | 21.88, 21.91 ms |

**Handing the standard library's sort a `&C` costs 1.7x when that
instantiation does not optimise, and whether it does is a property of the
calling crate.** With today's library the bench crate reads
`sort/rustflow/1w/1000000` at 21.67 ms while an example crate in the same
tree, calling `taskflow.sort_by` on the same buffer with the same comparator,
reads 35.4 to 36.1 ms; with `127e3b7`'s library the same two harnesses read
36.62 ms and 19.9 ms. Both versions have both regimes, the regimes are 1.7x
apart, and #375's diff shuffled which crate lands in which. The A/B's
headline figure is that lottery. **#399 is the fix, and it has since
shipped** -- pass the comparator by value or through a closure, both of which
read 21.8 ms above, at all three of the call's sites, the two in
`sort_loop`'s own leaves included. "The #399 change" at the end of this file
is that change, with the ablation re-taken against criterion and four caller
binaries measured either side of it: the regimes are gone and the four agree
to 1.03x.

The `sort` table in that shape's section above has been re-taken again since,
by that change; the one row that is about #375 at all is the 24-worker
million-element one, which neither pass moves outside its own interval.

### The decision: nothing new ships, two defaults stand, three leads filed

The bar is #342's and #343's -- a mechanism the counters name, then an A/B
outside both sides' spread -- and the three parts land in three different
places.

- **Part 1 is closed and this run added nothing to it.** #378 shipped no code
  and "The #367 measurement" is the answer; there was no second build to
  measure.
- **Part 2: `SplittingPartitioner` stays an option and does not become a
  default, at a cost this section now states.** It reaches 21 to 24 of 24
  workers against the guided default's 21 to 22, and the counters say the
  static share rather than the re-split is what does that; the re-splits
  themselves cost 12 to 16 µs each -- which "The #400 attribution" below
  resolves into the join each one opens, at 160 to 476 µs of CPU a split --
  and the whole scheme 1.27x to 33.8x of the
  shipped ids' wall at 24 workers, including 1.48x and 1.70x on `mandelbrot`,
  the shape whose imbalance it was built for.
  `DefaultPartitioner = GuidedPartitioner<>` therefore stays what #343 left
  it. The declined change is "make the
  splitting dispatch the default", and its price is the 24-worker column
  above.
- **Part 3: #375 stays, on the grounds it was taken on rather than on a
  figure.** It replaced a call upstream itself does not make -- the pinned
  header's `make_sort_task` calls `parallel_pdqsort` and its
  `parallel_3wqsort` call is commented out, with a `sort3` its pivot picker
  needs missing from the tree -- so the port now runs upstream's live core.
  What this measurement adds is that the change is **not visible** on this
  shape: the one id that reaches it moves 3.5% inside a 15.3% spread, and the
  42.7% and 17.6% the A/B shows elsewhere belong to a comparator call shape on
  a path neither partition touches.
- **The three leads are filed.** #399 (the `&cmp` call shape, 1.7x on a
  one-worker sort), #400 (what 13 µs a re-split is made of), #401
  (`SplittingPartitioner::default()`'s floor). Each names a bar and none of
  them is this section's to take. All three have since been taken: "The #399
  change", "The #400 attribution" and "The #401 decision" below are their
  sections, and between them they filed three of their own -- #406 and #407
  from the second, #409 from the third.

**What this does not say.** It does not say `SplittingPartitioner` is useless:
a box wider than its pool, a body longer than the arrival spread, or a cursor
whose chunk floor starves late workers -- the guided 262144 row, 11 to 15
workers at 2.2 ms against the splitting walk's 22 at 0.74 -- are all cases
where the static share wins, and the type exists for a caller who has one. It
does not say #375 made the sort slower or faster: on the one id that measures
it, it did neither. And it does not resolve `sort/rustflow/24w/1000000`, whose
±6.9% interval and 15.3% round-to-round spread are wider than any change #375
could plausibly have made; what would resolve it is an id with a tighter
spread, which is a harness question and not this one's.

## The #399 change: the comparator reaches the sort by value, and the 1.8x regime is gone

**The lottery the "#381 measurement" above found is closed by one word on
three lines.** `Runtime::sort_by`'s sequential short-circuit handed
`[T]::sort_unstable_by` a `&C`; it now hands it the `C` the frame owns, and
the two leaves inside `sort_loop`, which hold a reference a whole scope
shares, hand it `|a, b| cmp(a, b)`. In one binary the reference form costs
**1.77x** against all three alternatives, and across four caller binaries the
*same* library used to read 20.06 to 37.23 ms on the same million `f64`s
depending on nothing a caller can see. It now reads 21.86 to 22.54 ms
everywhere, a 1.03x spread.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release`, no `lto`, 16 codegen units |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id, as every target declares |
| Date | 2026-11-19 |
| **Load average** | **each measurement's own, below; 0.19 to 0.97 before and 0.26 to 4.13 after, with nothing else on the box** |

The box was this run's alone: no sibling agent and no other `cargo`.
`/proc/loadavg` read either side of every measurement -- the before pair of
the suite's id 0.97 to 0.97, its after pair 0.19 to 0.26, the example
harness 0.76 to 0.78 and 0.30 to 0.36, the plain harness 0.30 to 0.30 at one
call site, 0.38 to 0.38 at two and 0.33 to 0.33 after the change, the
four-way ablation 0.29 to 0.49, and the re-taken shape
table 0.21 to 4.13, whose tail is its own 24-worker ids. Every criterion
figure is **one** run of one id, quoted as the point estimate of the mean
with half the width of the 95% confidence interval as a plus-or-minus
percentage; the plain harness is twenty timed samples of its own, quoted as
the mean with its min and max, because it carries no criterion at all --
which is the point of it.

### The four call shapes, in one binary

A million `f64`s from `benches/sort.rs`'s own generator, re-shuffled untimed
before every sample, sorted four ways by four generic functions with
`Runtime::sort_by`'s signature shape -- a `C: Fn(&T, &T) -> Ordering` taken
by value -- so that the only thing that differs between the first three is
how `cmp` reaches the standard library's sort.

| call shape | time | against the floor |
|---|---|---|
| `data.sort_unstable_by(&cmp)` -- what the short-circuit wrote | **37.19 ms ±0.3%** | **1.77x** |
| `data.sort_unstable_by(cmp)` -- what it writes now | 21.03 ms ±0.4% | 1.00x |
| `data.sort_unstable_by(\|a, b\| cmp(a, b))` -- what the leaves write now | 20.76 ms ±0.3% | 0.99x |
| `data.sort_unstable_by(f64::total_cmp)`, no generic in the way | 20.97 ms ±0.1% | the floor |

This is the ablation the "#381 measurement" ran, re-taken against criterion
rather than a wall clock, and it reproduces: 37.19 ms against 20.76 to 21.03,
with every interval under half a percent, so the 77% is two orders of
magnitude outside the widest of them. **The two fixed forms are not faster
than a sort with no generic in
the way** -- they are the floor, within 1% -- which is what says the cost is
the forwarding call through `impl Fn for &F` and not the comparator.

### Four caller crates, before and after, on the same library

The rule the "#381 measurement" could only state is here demonstrated:
whether the `&C` instantiation optimises is a property of the **calling**
crate, because that is where a generic instance is compiled. The two columns
below differ in nothing but the three lines of `src/algorithm/sort.rs`.

| caller | what it calls | before | after | ratio |
|---|---|---|---|---|
| `benches/sort.rs` | `sort/rustflow/1w/1000000` | 20.78 ms ±0.2% | 21.86 ms ±0.3% | 1.05x |
| example, criterion | the same work, same comparator | 20.19 ms ±0.2% | 22.54 ms ±0.1% | 1.12x |
| example, no criterion, one call site | the same work, `f64::total_cmp` | 20.06 ms (19.99-20.33) | -- | -- |
| example, no criterion, two call sites | the same work, `f64::total_cmp` | **32.06 ms** (31.89-32.15) | 22.24 ms (22.06-22.53) | **0.69x** |
| the same binary's second site | the same work, a closure comparator | **37.23 ms** (37.03-37.42) | 22.32 ms (22.09-22.79) | **0.60x** |

**The third and fourth rows are the same source file, and the only difference
between them is a second call site.** The plain harness began as one timed
loop over `taskflow.sort_by(.., f64::total_cmp)` and read 20.06 ms; adding a
second loop over the same work with a closure comparator -- a second
instantiation, in the same crate, which the first does not call -- moved that
*first* loop to 32.06 ms and put the new one at 37.23 ms. Nothing else
changed, and no edit to this crate was involved. That is the whole of what
"the instantiation does not optimise" means in practice, and it is why the
published 1.7x of #375's A/B was not the partition.

After the change, the four harnesses read 21.86, 22.54, 22.24 and 22.32 ms:
a 1.03x spread where there had been 1.86x, and every one of them at or under
the floor-plus-10% the acceptance named (23.80 ms).

**What this does not buy is a faster sort on a build that was already
lucky.** The suite's own id goes 20.78 to 21.86 ms and the criterion example
20.19 to 22.54 -- 1.05x and 1.12x the wrong way, both against a shape whose
A/A control in the "#381 measurement" puts its own floor at about 5% and
whose three readings of that one id in this run alone are 20.78, 21.07 and
21.86 ms. The change is not an optimisation of the lucky regime; it deletes
the unlucky one. A caller who had the 37 ms build gains 1.67x, a caller who
had the 20 ms build loses nothing that this shape can resolve, and no caller
is left where the figure depends on how many times its own crate happens to
instantiate the call.

### What was checked beside it, and found not to be the shape

- **`sort_loop`'s two leaves are fixed too**, though no id of this suite
  measures them: three of the `sort` shape's four ids never enter the loop,
  and the fourth, `24w/1000000`, reads 13.32 ms ±11.9% against the 15.01 ms
  the shape's table carried before -- inside its own interval, which is the
  only thing an id this wide can say.
- **`merge_sort` has no `&C` to fix.** Its three leaf sorts
  (`benches/merge_sort.rs`'s two `msort_*` cutoffs and `msort_rayon`'s) pass
  `f64::total_cmp` by value already, which is the ablation's last row -- the
  floor itself.
  There is no `src/algorithm/merge_sort.rs`: the shape is a benchmark over
  `Executor::task_group` and `rayon::join`, not a ported algorithm.
- **`src/algorithm/merge.rs` is a different shape.** Its `&cmp` reaches this
  crate's own `merge_serial` and `co_rank`, which take `&impl Fn` so that one
  comparator can serve every spawned chunk, and never a standard-library
  sort. Nothing here measures it, and it is not what this section's figures
  are about.

### The re-taken shape table

The `sort` section's table is this change's, re-taken in one
`cargo bench --bench sort` with the three lines in place, and the figures
above are the same code read id by id. Its one-worker million-element row
moves 21.11 to 21.07 ms, which is nothing; the shape's own prose, which
carried "within the noise that #399 is filed to remove", now carries the
outcome instead.

## The #397 attribution: the cost is the parking, and the list now waits to be drawn from

**Of the three candidates the "#382 measurement" left, the one that costs is
the `FreeNodes::recycle` call: taking it out alone moves
`integrate/rustflow/1w/2` by −9.6% and `1w/1` by −13.7%, where taking the
`Arc` clone and drop out of `WorkerThread::spend_job` alone moves neither
(+0.2% and +0.6%, inside both sides' own spreads).** So #379 stays, with one
condition added to it: a worker's list is parked into only once a submission
has drawn from it. With that condition the two `integrate` ids read −10.8%
and −12.8% against today's `main` and **+2.0% and +3.2% against `da36b9d`**,
the side before #379, both inside the two sides' spreads -- the pre-#379 cost
is back -- while the route the list was built for is unmoved: `fibonacci`'s
four `rustflow-runtime` ids read −1.7% to +0.7%, every one of them inside a
spread. What the recycling is worth is still the footprint #382 measured, and
it now costs nothing where it cannot be collected.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id -- each shape's own published settings, unchanged |
| Date | 2026-11-19 |
| **Load average** | **each measurement's own, below; the runnable-task field read 1 at every reading but one, which read 2** |

The box was this run's alone -- no sibling agent, nothing else building --
and `/proc/loadavg` was read either side of every measurement:

| measurement | before | after |
|---|---|---|
| `integrate`, the A/A control | 0.49 0.37 0.83 | 1.32 0.75 0.93 |
| `integrate`, both candidates off | 1.12 0.72 0.92 | 1.69 1.09 1.04 |
| `integrate`, the `recycle` call off | 1.43 1.05 1.03 | 1.47 1.21 1.09 |
| `integrate`, the clone off | 0.58 1.00 1.02 | 1.32 1.21 1.10 |
| `integrate`, `main` against the condition | 11.99 4.80 2.37 | 4.74 4.24 2.38 |
| `integrate`, `da36b9d` against the condition | 3.12 3.90 2.31 | 2.21 3.52 2.32 |
| `fibonacci`, `main` against the condition | 0.52 2.55 2.09 | 10.81 6.87 3.94 |

Two of those figures are this run's own and not another workload: the 11.99
one-minute average is the decay of the gate's own `cargo test --all-features`
minutes earlier, with one runnable thread at the reading, and the 10.81 the
`fibonacci` 24-worker ids that had just run. Every A/B below is one
`devtools/bench-ab.sh` run of three interleaved rounds a side, each side's
spread over its own rounds printed before the B-over-A ratio of medians, and
a cell is that median. The ablations are three one-commit branches off
`main`, measured against `main` itself, and the condition is measured as
`WORKTREE`.

### The ablation: three builds, and only one of them moves an id

The per-cent columns are B-over-A, so a negative number is the *removal*
making the id faster -- the cost of what was removed.

| what the B side takes out of today's `main` | `1w/1` | `1w/2` | spreads on `1w/2` | verdict |
|---|---|---|---|---|
| nothing (`main` against `main`, an A/A control) | −0.7% | −1.4% | 6.7% / 2.2% | inside the spread |
| the `FreeNodes::recycle` call, keeping the clone | **−13.7%** | **−9.6%** | 1.8% / 2.0% | a result |
| the `Arc` clone and drop, keeping the `recycle` call | +0.6% | +0.2% | 2.3% / 0.7% | inside the spread |
| both | **−11.9%** | **−7.4%** | 5.2% / 5.7% | a result |

The clone was taken out by handing the node *out* of `Job::finish` -- the one
place a job's share of its owner is dropped -- instead of cloning the share
before the job is spent, which is the only way to keep the `recycle` call
without the two atomics. It costs nothing either way, so the row below the
A/A control is the whole of the finding: the two atomics of an `Arc` clone
and drop are not what a worker pays on this route, and removing both
candidates is no better than removing the call alone.

**What the call costs on a route that cannot draw is a heap block a task**,
which is read off the code rather than measured apart, and the arithmetic
agrees with the figure:

- `FreeNodes::new` leaves the backing `Vec` unallocated, so the first node a
  list parks allocates it -- one block, four slots.
- Every `TaskGroup::corun` runs on a `WorkerThread::corun_view`, which has a
  list of its own and drops it with the frame, and `integrate`'s `rustflow`
  id opens a group and coruns it **once per splitting interval**: one corun
  frame per spawned child.
- So before the condition, each of those frames allocated a `Vec` to park the
  one node it finished in, parked a block nothing could take out again, and
  freed both when the frame died -- one extra heap block a task, which the
  "#125 attribution" priced at ~70 ns on the thread that allocates it. The
  two ids moved 32 and 47 ns a task (9.6% of 622 µs over 1852 children, 13.7%
  of 288 µs over 828), which is that block and no room for much else.
- It is also why the figure followed the *shape* rather than the route.
  #382's block count for the id route reads 2.0160 blocks a task against
  #376's 2.0159 on `async_task/rustflow-group`, whose 65536 children share a
  handful of corun frames: the same `Vec` per frame, amortised over 65536
  tasks there and over one task here. That 0.0001 was this cost, counted,
  before anything timed it.

### The condition, on the two routes it has to tell apart

`FreeNodes::animate` records that this list was drawn from -- whether or not
it had a block to give, that call being what proves a submission names the
list at all -- and `recycle` parks nothing until it has. The route that draws
opens its list with its first child, which is submitted before any child of
it finishes, so nothing about the collection changes there; the route that
cannot draw never opens one.

| shape | id | `main` -> the condition | spreads | verdict |
|---|---|---|---|---|
| `integrate` | `rustflow/1w/1` | **−12.8%** | 2.6% / 5.4% | a result |
| `integrate` | `rustflow/1w/2` | **−10.8%** | 2.5% / 4.6% | a result |
| `fibonacci` | `rustflow-group/1w/15` | **−11.5%** | 2.7% / 4.2% | a result |
| `fibonacci` | `rustflow-group/1w/12` | *−11.9%* | 4.8% / 11.7% | marginal |
| `fibonacci` | `rustflow-group/24w/12` | −3.3% | 7.5% / 1.6% | inside the spread |
| `fibonacci` | `rustflow-group/24w/15` | +3.7% | 2.1% / 4.9% | inside the spread |
| `fibonacci` | `rustflow-runtime/1w/12` | +0.7% | 10.7% / 4.6% | inside the spread |
| `fibonacci` | `rustflow-runtime/1w/15` | −1.7% | 4.1% / 3.2% | inside the spread |
| `fibonacci` | `rustflow-runtime/24w/12` | −0.0% | 1.6% / 0.6% | inside the spread |
| `fibonacci` | `rustflow-runtime/24w/15` | +0.3% | 2.3% / 1.7% | inside the spread |

The four `rustflow-group` rows are #382's regressions answered: that run read
`fibonacci/rustflow-group/1w/15` at **+8.6%** for #379 and this one reads
−11.5% for the condition, on sides inside 4.2%, and the two 24-worker ids say
nothing either way in both runs -- which is the "#380 measurement" showing
through, a nested recursion's cost at 24 workers being the handoff and not
the submission. The four `rustflow-runtime` rows are the control: that is the
one route that draws from the list, and the condition leaves it where #382
left it.

**Against the side before #379**, which is the comparison the acceptance
named, one further run: `da36b9d` against the condition, interleaved, same
three rounds.

| id | `da36b9d` | the condition | ratio | spreads | verdict |
|---|---|---|---|---|---|
| `integrate/rustflow/1w/1` | 252.7 µs | 260.8 µs | +3.2% | 6.6% / 1.3% | inside the spread |
| `integrate/rustflow/1w/2` | 562.0 µs | 573.5 µs | +2.0% | 5.5% / 1.7% | inside the spread |

`da36b9d` reads 562.0 µs for `1w/2` here against the 558.2 µs it read in the
#382 run, which is that absolute reproducing to 0.7% two days later. The
**570 µs** bar the acceptance set -- that figure plus 2% -- sits inside this
side's own spread today, its three rounds being 552.7, 562.0 and 583.2 µs, so
it cannot be read as a pass or a fail at this box's resolution; the
interleaved ratio above is what answers it, and +2.0% against sides that
disagree by 5.5% is not a result. The condition's own median is 573.5 µs in
this run and 575.2 µs in the run against `main`.

One caveat on that span, which no id here reaches: `da36b9d` to today's
`main` now also carries #399's sort fix (PR #403). Nothing in it touches the
async submission path, and neither `integrate` nor `fibonacci` calls
`sort_by` or a partitioner, so it is in the span and not in the figures.

### The decision: #379 stays, with a condition on where it parks

- **The ablation named the `recycle` call**, not the clone, and the call's
  cost is a block a task on every route whose corun frames park blocks they
  cannot draw out again. That is what the −14% to +15% range off the drawing
  route was: not the hot path's code layout, as this section's predecessor
  guessed at, but an allocation after all -- one the block counts had
  already recorded as 0.0001 blocks a task on a shape that amortised it.
- **The clone and drop stay where they are.** They are measurable at nothing
  (+0.2% and +0.6% for removing them), and the alternative shape -- handing
  the node out of `Job::finish` -- costs that method its one call site and
  one return value for no figure, so it was measured and declined.
- **Giving the id route the draw is not reachable**, which was the other
  option #397 carried. A group opened by `Executor::task_group` holds its
  worker's *id*: the owning `WorkerThread` frame is one no task body can
  name, by the same argument that puts a corun on a stealing view of the
  queue, and the list lives in that frame. Reaching it would mean moving the
  list out of the frame -- a thread-local, or a shared per-worker slot behind
  an atomic -- which is upstream's sharded pool and a different design than
  the one #379 argued for.
- **What #379 is worth is unchanged and is still a footprint**: a child
  submitted through a `Runtime` costs 3 heap blocks and 144 bytes where it
  cost 4 and 400, with its node drawn from the list for all but one child in
  121392 at one worker. No time, where it is drawn; nothing, now, where it is
  not.

## The #400 attribution: a re-split costs the join it opens, and the join never parks

**The 14 µs a re-split is not the spawn, not the thread-id check, not a
re-walk and not the migration: it is the explicit join the split opens, and
that join's wait is 143 to 172 µs of CPU a round.** `run_share` hands its far
half to a `Runtime::scope` and walks the near half inside it, so the scope's
own `Runtime::try_corun` has to wait for the far half before the splitting
task can finish; a corun cannot park, by the design its own module docs state,
so the waiting worker searches instead -- 210 steal attempts and 150
`thread::yield_now` calls per failed `WorkerThread::explore_task` -- and does
it again until the half it is waiting for completes. The arithmetic closes
exactly: at `n = 1e6` and 24 workers the splitting default burns **33.4 to
79.1 ms** of CPU against the same partitioner's non-splitting **17.9 to
22.6 ms**, and subtracting the measured spin from each of the six runs leaves
**17.2 to 20.6 ms** every time -- inside the non-splitting row's own range. An
ablation that replaces the scope's join with the node's own preemptive one
brings the wall to **1.01-1.26 ms** against 2.14-4.19, and the CPU to
18.6-21.0 ms, with **zero** corun calls.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| harness | #381's observer harness, not criterion: six runs a row, the pool confirmed fully parked (`Executor::num_waiters() == 24`) before each window, a fresh executor per row |
| Date | 2026-11-19 |
| **Load average** | **0.15 0.30 1.13 before the pass and 0.15 0.30 1.13 after it, with nothing else on the box** |

The box was this run's alone: no sibling agent, no other `cargo`, and
`/proc/loadavg`'s runnable-task field read 1 or 2 at every reading either side
of all three builds. Every figure below is **one** run of one row, quoted as
the range over that row's six runs, which is the convention #381's part 2 set
for this harness; there is no best-of-anything here, and a row is only read
against the control rows measured in the same process, because the three
builds' own controls differ by up to 1.4x between processes and the box's
drift is the reason.

### Method: one harness, three builds, four rows each

The harness is #381's: a `for_each_by_index` over a million `AtomicU64` cells,
each replaced by its own tangent, under a #343-shaped observer that counts
which worker enters each dispatched async task. What is added to it here is
three things. A counter bank in `run_share` and in `WorkerThread::corun_until`
-- splits, entries, body calls, covered indices, and the nanoseconds of each
of the split's three phases on the splitting worker. A CPU reading either side
of the timed region, summing `sum_exec_runtime` out of every
`/proc/self/task/<tid>/schedstat`, which is what separates a wait that costs
cores from a wait that costs only latency. And an untimed correctness check
after every run, comparing all million cells against a sequential `f64::tan`,
so a build that loses an index fails here rather than producing a fast figure
-- every run of every build below passed it.

The three builds are today's tree and two ablations, selected at run time so
that one binary carries all three:

- **A, today's tree.** `run_share` opens a `Runtime::scope` per split.
- **B, the far half detached.** The far half goes to
  `Runtime::silent_async_runtime` under the splitting task's *own* node and the
  body returns: the preemption protocol `executor::runtime`'s module docs set
  out joins it, so the worker goes back to its scheduling loop -- where, with
  nothing to do, it *parks* -- instead of waiting in a corun. This is an
  ablation and not a candidate change: it needs the far half's body to be
  `'static`, which the index family's own three borrowed entry points do not
  give, and the decision below says what shipping it would take.
- **C, the chain capped at one.** Today's scope, but a share that has already
  been split never splits again -- the far half records the thread it is
  handed out *on* as its home, so it reads as un-stolen wherever it lands.

### The counters: 23 shares, 98 to 130 splits, and one body call each

Build A, the `splitting floor 1 (default)` row, six runs. "Entries" is
`run_share` calls, which the observer's own async-task count matches exactly
in every run -- the one inline share runs on the calling worker and is no
task. "Spin" is time in a `corun_until` round that found nothing to steal.

| run | entries | home | splits | bodies | indices covered | spawn, µs | near walk, ms | join, ms | spin, ms | corun rounds / pops / steals / yields | CPU, ms | wall, ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 121 | 23 | 98 | 121 | 1000000 | 116 | 12.4 | 63.1 | 15.7 | 199 / 23 / 66 / 110 | 33.4 | 2.14 |
| 2 | 124 | 23 | 101 | 124 | 1000000 | 107 | 15.9 | 82.4 | 26.5 | 242 / 23 / 65 / 154 | 44.3 | 2.48 |
| 3 | 141 | 23 | 118 | 141 | 1000000 | 873 | 15.8 | 73.5 | 19.8 | 226 / 23 / 85 / 118 | 39.8 | 2.14 |
| 4 | 153 | 23 | 130 | 153 | 1000000 | 111 | 16.2 | 251.9 | 61.9 | 518 / 23 / 95 / 400 | 79.1 | 4.19 |
| 5 | 147 | 23 | 124 | 147 | 1000000 | 102 | 16.3 | 106.1 | 25.9 | 285 / 23 / 96 / 166 | 46.5 | 2.48 |
| 6 | 123 | 23 | 100 | 123 | 1000000 | 70 | 15.2 | 92.4 | 28.6 | 276 / 23 / 63 / 190 | 46.9 | 2.51 |

Four things to read off it before any candidate is priced.

- **Exactly 23 entries of every run find themselves at home, and they are the
  far halves nobody stole.** The dispatch spawns 23 shares and runs the 24th
  inline; all 23 spawned ones are stolen, every run, so all 23 split. The
  other 23 un-split entries are the far halves the splitting worker's own
  corun popped back off its own queue -- `pops` is 23 in all six runs, which is
  the same number from the other side.
- **`corun` calls equal splits, every run.** One scope, one `try_corun`, one
  split: the join is not amortised over anything.
- **Nothing is walked twice.** `bodies` equals `entries` in all six runs, and
  the covered indices sum to `n` exactly, so every index is visited by exactly
  one body call and no share re-walks what it handed off.
- **The chain is deep.** Counted by the depth a split happens at, the six
  runs give 23 / 20-23 / 17-23 / 13-22 / 8-16 / 5-11 / 2-6 over the first
  seven levels, with the eleven-and-deeper bucket non-empty in three of them.
  A share is re-split four or five times over, and each level is a frame
  waiting on the level below it.

### The four candidates the issue listed, priced and dismissed

| candidate | what the counters say | share of the 14 µs |
|---|---|---|
| the runtime-bodied spawn a split needs | the whole scope-open-plus-`spawn_runtime` phase is 70 to 116 µs over 98 to 130 splits -- **0.7 to 1.2 µs a split**, two `Instant::now()` calls of the measurement included -- in five runs of six, and 873 µs in the sixth, where one spawn stalled | under a tenth, and consistent with "The #380 measurement"'s 347 ns a child plus a scope frame |
| the `ThreadId` compare and the `Runtime::worker_id` lookup | the read and the compare total 5.2 to 6.4 µs over 121 to 153 entries -- **41 to 45 ns an entry**, which is two `Instant::now()` calls and no measurable remainder -- and `Runtime::worker_id` is never called on this path at all | nothing |
| the share's own re-walk after the handoff | `bodies == entries` and the covered indices sum to `n` in every run | zero |
| the cache traffic of a half share changing worker | the splitting row's body time is 14.3 to 18.1 ns an index against the non-splitting row's 13.3 to 16.9, which is **inside the control's own range** and so not a result; build B bounds it instead, its body time reading 13.7 to 15.7 ns against its own control's 13.7 to 14.3 -- at most **1.4 ns an index**, 1.3 ms of CPU a run, under 1.2 µs of wall a split | under a tenth |

So the four together account for about 1 to 2 µs of the 14, which leaves the
join.

### Where it goes: a wait that cannot park, and a search that costs 143 µs to fail

The join's own clock is the `join, ms` column, and it is the wrong number to
divide: a parent's join contains its children's, so 251.9 ms over a 4.19 ms
run is nested double counting rather than a thread-time total. What is not
double counted is the CPU, and it attributes the whole of the difference:

| row (build A) | wall, ms | CPU, ms | CPU − spin, ms |
|---|---|---|---|
| guided, floor 1 (`DefaultPartitioner`) | 0.79-0.96 | 18.0-20.4 | -- |
| splitting, floor 1 (`default()`) | **2.14-4.19** | **33.4-79.1** | **17.2-20.6** |
| splitting, 262144 -- above `n/w`, so nothing splits | 1.06-1.34 | 17.9-22.6 | -- |
| guided, 262144 | 3.10-4.89 | 15.5-17.8 | -- |

**The splitting default's CPU minus its own measured spin lands inside the
non-splitting row's CPU in all six runs**, 17.2-20.6 against 17.9-22.6, so the
extra 13 to 59 ms of CPU a run *is* the spin and not work the split added.
Per split that is **160 to 476 µs of CPU**, and the run's own concurrency --
16 to 19 cores' worth, by the CPU-over-wall ratio -- turns it into the 8 to
23 µs of wall a split that the published 14 µs is. Spin time is wall time on
the waiting worker rather than a CPU reading of its own; that the two agree to
the control's range is what says the waiting worker is on a core while it
waits.

Per failed round the spin is **143, 172, 167, 155, 156 and 151 µs** in the six
runs, which is where the cost actually lives, and
`WorkerThread::explore_task`'s own give-up policy is the whole of it. At 24
workers a victim is one of `num_queues()` = 24 queues plus
`bit_width(24)` = 5 spill buffers, so `max_steals` is `(29 + 1) * 2` = 60
attempts; past that it yields on every attempt and gives up 150 attempts
later. A round that finds nothing therefore costs **210 steal attempts and
150 `sched_yield` calls**, each attempt a contended pop on another worker's
deque. `corun_until` then yields once more and runs the round again, because
the one thing it must not do is sleep: "a worker parked here being one the run
it waits for cannot use" is its own comment, and it is right -- but it means a
corun that waits a long time pays this search over and over, and that a worker
which would otherwise have *parked* instead hammers every deque in the pool.
That last part is what the fourth candidate was half-seeing: the splitting
row's per-index body cost is a little above the non-splitting row's, and build
B shows most of that gap is the searching, not the migration.

### The ablation: the join replaced by the node's own, and the cost is gone

Three builds, four rows, six runs a row, all in one pass and each row read
against the controls of its own build.

| row | A: today | B: far half detached | C: chain capped at one |
|---|---|---|---|
| guided, floor 1 | 0.79-0.96 ms | 0.90-1.24 ms | 1.16-1.27 ms |
| **splitting, floor 1** | **2.14-4.19 ms** | **1.01-1.26 ms** | **1.27-1.49 ms** |
| splitting, 262144 | 1.06-1.34 ms | 1.00-1.04 ms | 1.11-1.27 ms |
| guided, 262144 | 3.10-4.89 ms | 2.73-3.82 ms | 2.89-3.36 ms |
| splitting floor 1: CPU | 33.4-79.1 ms | 18.6-21.0 ms | 25.0-29.0 ms |
| splitting floor 1: tasks | 121-153 | 127-175 | 46 |
| splitting floor 1: splits | 98-130 | 59-91 | 23 |
| splitting floor 1: corun calls | 98-130 | **0** | 23 |
| splitting floor 1: spin | 15.7-61.9 ms | **0** | 2.1-3.3 ms |

- **B removes the whole of it.** With the far half joined by its own node the
  splitting default reads 1.01-1.26 ms against its own build's non-splitting
  row at 1.00-1.04 and its guided default at 0.90-1.24 -- level with both,
  where today it is 2 to 4x them -- and its CPU, 18.6-21.0 ms, is its own
  control's. It also *splits more freely* while costing less, because a worker
  that is no longer stuck in a join is a worker that can steal: 127 to 175
  tasks against today's 121 to 153.
- **B's absolute figures do not clear the issue's 1.2 ms on their own**, and
  the controls say why rather than excusing it: that build's own guided
  default read 0.90-1.24 ms where build A's read 0.79-0.96, so the process
  carried about 25% of drift and the bar is inside it. What B clears
  unambiguously is the comparison the bar was a proxy for -- the splitting
  default level with the non-splitting row in the same process.
- **C is a mitigation and not a fix.** Capping the chain at one level cuts
  splits from ~115 to 23 and the spin with them, and lands 1.27-1.49 ms
  against its own controls' 1.11-1.27 -- a residual 1.15x, which is the 23
  joins it still opens. It also takes the property the type exists for away:
  23 splits is one re-split per share and no more, so a worker arriving late
  to a *stolen* share has nothing to take.

### What this leaves for the floor

The cost is **per split**, so the floor is a linear multiplier on it and
nothing else: `default()`'s floor of 1 buys 98 to 130 splits and 98 to 130
joins, a floor above `n / w` buys none, and #381's 4096 row sits in between at
88 to 94 tasks and 1.03-1.28 ms. #401 is the floor's decision and this section
does not take it, but it owes two numbers to it. The first is that the floor
prices nothing except the number of splits, each of which costs 160 to 476 µs
of CPU on today's tree. The second is that build B drops that per-split cost
to the 1 to 2 µs the four candidates bound, which would leave the floor
costing almost nothing -- so a floor chosen against today's join is a floor
chosen against a cost that the join is, and not against the splitting scheme.

**"The #401 decision" below has since taken it, and it refines the first of
those two numbers**: swept floor by floor with the chunk walk held fixed, the
cost is not linear in splits. A chain capped at four levels by a floor of
`n / (16w)` runs 73 to 85 splits for between 0.4 and 9.0 µs of wall each,
where the uncapped chain this section counted runs 98 to 130 for 9 to 13 --
so what the floor of one buys is depth rather than splits, and depth is what
nests one corun's wait inside another's. The floor stays at one all the same,
for the second number's reason.

### The decision: nothing ships here, and the join is two issues of its own

The issue's own gate for a change was that the counters name one of its four
candidates. They name none of them: each of the four is priced above at
between nothing and a tenth of the cost, and what is left is a mechanism the
issue did not list. So this closes as an attribution, with the reason the
cost cannot come off *here* stated rather than asserted.

- **The join is required where it stands.** `Scope`'s soundness argument is
  that `Runtime::scope` does not return until its children are joined -- its
  own module docs rest the two `transmute` calls `Scope::spawn` and
  `Scope::spawn_runtime` make on exactly that -- and `run_share`'s share and
  work are `'a`-bounded, not `'static`, because `spawn_shares` hands the
  slice, reduce and find algorithms shares that borrow their caller's data.
- **The cheaper join needs a `'static` body, and three call sites do not have
  one.** Build B's route is `Runtime::silent_async_runtime`, whose bound is
  `Send + 'static`. The owned entry points (`make_for_each_index_task` and its
  two siblings) do satisfy it; `Runtime::for_each_index`,
  `Runtime::for_each_by_index` and `Runtime::for_each_by_index_nd` do not --
  they submit through a `Scope` precisely so that a body may borrow a local --
  and `IndexDispatch::dispatch` is one method over both, so `spawn_splitting`
  cannot name the detached route from inside it. Giving it one means a handoff
  seam through `dispatch`: a new parameter and a new bound on a public trait
  method, which is a surface change and not a tuning. **#406** is that change,
  with build B's figures as its bar.
- **The search the wait performs is scheduler-wide**, not this partitioner's:
  210 steal attempts and 150 `sched_yield`s per failed round costs 143 to
  172 µs of CPU to establish that a 24-worker pool is busy, and every
  `Runtime::scope`, `Runtime::corun`, `TaskGroup::corun` and algorithm corun
  pays it on the same terms. `src/executor/worker.rs` is where that lives and
  it is outside this issue's files. **#407** is that one.
- **What the splitting scheme is actually paying for** is that it hands work
  out through a *task*, which something has to join, where the four ported
  families hand it out through a shared cursor, which nothing does. That is
  the structural reason the guided default is cheap at 24 workers and not an
  accident of its tuning.

## The #401 decision: the floor stays at one index, and only an uncapped chain separates

**`SplittingPartitioner::default()`'s floor of one index is the type's most
expensive setting, and it stays.** A sweep that holds the chunk walk fixed and
moves only the splitting floor finds exactly one separation in it: an
*uncapped* chain of re-splits costs 9 to 13 µs of wall a split over the same
partitioner with its splits turned off, in all three passes, while every
bounded floor from `n / (2w)` to `n / (16w)` lands in one band that separates
neither from its neighbours nor from not splitting at all. So nothing in the
band is a floor to prefer on a figure, no floor that splits reaches the 1.5x
of the guided default the issue asked for -- the closest row is the
*non-splitting* control at 1.22 to 1.48x -- and the one property this type
exists for reads at or above its own process's guided control in every pass
only at the floor of one. What the saving would buy is the depth of the chain,
which is permanent; what it would save is the join "The #400 attribution"
above measured and **#406** owns, which is not.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| harness | #381's observer harness, not criterion: six runs a row, the pool confirmed fully parked (`Executor::num_waiters() == 24`) before each window, a fresh executor per row |
| Date | 2026-11-19 |
| **Load average** | **0.85 1.01 1.38 either side of pass 1, 0.24 0.71 1.22 either side of pass 2, 0.33 0.54 1.08 either side of pass 3** |

The box was this run's alone: no sibling agent, no other `cargo`, and
`/proc/loadavg`'s runnable-task field read 1 at every reading either side of
all three passes. A figure below is **one** run of one row, quoted as the range
over that row's six runs with that row's median beside it where a ratio is
taken -- the convention #381's part 2 set for this harness. There is no
best-of-anything here: the second pass is the first pass's row order reversed
and is reported in full beside it, not instead of it.

### Method: one process a pass, and the control is in it

The harness is a rebuild of #381's -- a `for_each_by_index` over a million
`AtomicU64` cells, each replaced by its own tangent, under a #343-shaped
observer counting which worker enters each dispatched chunk task, with an
untimed check of all million cells against a sequential `f64::tan` after every
run, which every run of every row below passed. The graph is built outside the
clock and only `Executor::run(..).wait()` is timed.

**Its guided control reads 1.09-1.66 ms where #381's read 0.48-0.63 and
#400's 0.79-0.96, so no row here is read against those.** It is a rebuilt
harness on a box three passes of its own disagree by up to 1.4x about, which
is the drift "The #400 attribution" names, and the answer is the same one:
every row is read only against the control rows measured in the same process,
and the pass's own order is reversed to check that the order is not the
finding.

What the sweep adds to #381's three rows is that it moves the floor **alone**.
A `SplittingPartitioner::new(k)` sets the splitting floor and the chunk walk
together -- `k` is both the size below which a share stops halving and the
number of indices one chunk-body call covers -- so #381's 4096 and 65536 rows
changed two things at once, and the `n <= chunk_size` early-out is a third. The
sweep instead keeps `chunk_size` at `0`, one body call a share as `default()`
has it, and derives the floor from `n` and `w` the way the ported four derive
their chunk size: a temporary `n / (div * w)` in
`SplittingPartitioner::split_floor`, selected per row inside one process. With
`div` at zero that expression is today's floor of one index, which is the row
labelled so; the knob adds one relaxed load per dispatch to the shipped path
and nothing else, and pass 3 below is the shipped tree without it.

Since each initial share is `n / w` of the range, a floor of `n / (div * w)`
bounds the chain at `log2(div)` levels: `n / (2w)` splits a stolen share once
and its halves never again, `n / (16w)` allows four levels, and today's floor
of one allows fifteen, of which #400's depth histogram found five to eleven
occupied.

### The sweep: seven rows, two orders, and one separation

Pass 1 runs the rows top to bottom, pass 2 bottom to top, in two processes of
the same binary. "Tasks" counts the observer's chunk-task entries, which for a
splitting row counts the re-splits; the inline share is no task and is not in
it.

| row | floor | tasks | distinct workers | wall, pass 1 | wall, pass 2 |
|---|---|---|---|---|---|
| guided, floor 1 (`DefaultPartitioner`) | 1 | 23 | 21-23 / 19-20 | 1.09-1.66 ms | 1.17-1.49 ms |
| **splitting, floor 1 (`default()`)** | 1 | **115-141 / 116-149** | **23-24 / 19-24** | **2.06-2.96 ms** | **2.34-4.82 ms** |
| splitting, `n / (2w)` | 20833 | 45-46 | 17-19 / 17-20 | 1.55-1.97 ms | 1.63-2.04 ms |
| splitting, `n / (4w)` | 10416 | 65-69 | 20-21 / 18-20 | 1.62-1.99 ms | 1.76-2.00 ms |
| splitting, `n / (8w)` | 5208 | 80-88 / 78-89 | 17-19 / 17-22 | 1.46-2.18 ms | 1.85-2.26 ms |
| splitting, `n / (16w)` | 2604 | 83-102 / 94-108 | 18-21 / 22-24 | 1.85-2.83 ms | 1.62-1.96 ms |
| splitting, 262144 -- above `n / w`, so nothing splits | 262144 | 23 | 22 / 14-18 | 1.21-1.70 ms | 1.53-1.85 ms |

The same rows as medians, and as a ratio against the guided control measured
in the same process, which is the only comparison this harness supports:

| row | pass 1 median | ÷ guided | pass 2 median | ÷ guided |
|---|---|---|---|---|
| guided, floor 1 | 1.27 ms | 1.00x | 1.22 ms | 1.00x |
| **splitting, floor 1** | **2.53 ms** | **1.99x** | **3.04 ms** | **2.48x** |
| splitting, `n / (2w)` | 1.72 ms | 1.35x | 1.95 ms | 1.59x |
| splitting, `n / (4w)` | 1.73 ms | 1.36x | 1.93 ms | 1.58x |
| splitting, `n / (8w)` | 1.97 ms | 1.55x | 1.95 ms | 1.59x |
| splitting, `n / (16w)` | 2.22 ms | 1.75x | 1.84 ms | 1.50x |
| splitting, nothing splits | 1.55 ms | 1.22x | 1.81 ms | 1.48x |

Four readings, in the order they decide the question.

- **Only the uncapped chain separates, and it separates in both orders.** The
  floor-1 row is the slowest of the seven in pass 1 and in pass 2, by 0.3 to
  1.2 ms of median over the next row and 2.0x and 2.5x over its own process's
  guided control. Against the non-splitting control of its own process that
  is 9.0 µs and 12.6 µs of wall a split -- `(2.53 − 1.55) / 108` and
  `(3.04 − 1.81) / 98` -- which is the 8 to 23 µs "The #400 attribution"
  priced the join at, reproduced from the floor's side.
- **The bounded floors are one band and the band resolves nothing.** Capping
  the chain at one level or at four costs the same: 1.72, 1.73, 1.97 and
  2.22 ms in pass 1 against 1.95, 1.93, 1.95 and 1.84 in pass 2, with each
  row's own six runs spanning 0.4 to 1.0 ms. `n / (16w)` is the worst derived
  row of pass 1 and the best of pass 2, which is the whole of what the band
  says about which of them to prefer. Nor does the band separate from the
  non-splitting control (1.55 and 1.81 ms): the gap is 0.03 to 0.67 ms, inside
  the rows' own spreads either way. **So the cost is not linear in splits** --
  73 splits at `n / (16w)` cost 0.4 µs each in pass 2 and 9.0 in pass 1, where
  the floor-1 row's 98 to 108 cost 9 to 13 in every pass -- and what the
  uncapped chain adds is not splits but *depth*: a parent's join contains its
  children's, so the fifteen levels a floor of one allows is a nest of waits
  where four levels is not.
- **No floor that splits comes within 1.5x of the guided default.** The
  derived band reads 1.35x to 1.75x of its own control in pass 1 and 1.50x to
  1.59x in pass 2, and the closest row of the seven is the one that splits
  *nothing*, at 1.22x and 1.48x. That last figure is the ceiling on what any
  floor can recover: a splitting dispatch with its splits off is still 1.2 to
  1.5x the guided cursor, because it hands each task `n / w` of the range
  outright where the cursor hands out what is left, which is the mechanism
  #381's counters named.
- **The distinct-worker count does not survive the order reversal, and the
  floor of one is the only row it ever favours.** The non-splitting control
  reads 22 of 24 in pass 1 and 14 to 18 in pass 2; `n / (16w)` reads 18-21 and
  then 22-24. A row moving by 8 of 24 between two passes of the same binary is
  a count this harness cannot discriminate floors with, and that is the
  reading, not a tie-break. What is consistent across all three passes is only
  this: the floor-1 row reads 19 to 24 in every pass, at or above its own
  process's guided control each time, and is the only row of the seven that
  ever reaches 24 of 24.

### Pass 3: the shipped tree, without the knob

The same harness built against the tree as it ships -- no `split_floor` knob,
`default()` and `new(262144)` reached through the public API -- three rows, six
runs each, one process:

| row | tasks | distinct workers | wall | median | ÷ guided | CPU |
|---|---|---|---|---|---|---|
| guided, floor 1 | 23 | 20-21 / 24 | 1.09-1.53 ms | 1.27 ms | 1.00x | 25.6-29.2 ms |
| **splitting, floor 1 (`default()`)** | **99-155** | **22-24 / 24** | **1.76-4.08 ms** | **2.85 ms** | **2.25x** | 34.0-84.7 ms |
| splitting, 262144 (nothing splits) | 23 | 20-21 / 24 | 1.48-1.76 ms | 1.67 ms | 1.32x | 26.3-32.8 ms |

It reproduces pass 1 and pass 2 on all three rows -- 2.25x the guided control
against 1.99x and 2.48x, 1.32x for the non-splitting row against 1.22x and
1.48x, and 10.2 µs of wall a split against 9.0 and 12.6 -- so the sweep's knob
was free and the figures above are figures about the shipped default.

### The decision: the documented floor of one stays, with its price in its docs

The issue's own acceptance was one of two outcomes, and the figures pick the
second: no floor the sweep measured reads within 1.5x of the guided default
without also giving up the splitting, so **`SplittingPartitioner::default()`
keeps its floor of one index and `src/algorithm/partitioner.rs`'s `# Design`
section now carries #381's three rows and what they cost.** Nothing in `src/`
changes.

- **The band offers no floor to prefer.** A default has to be one number.
  `n / (2w)` and `n / (16w)` are 1.35x and 1.75x of the control in one pass
  and 1.59x and 1.50x in the other, in opposite orders; picking either would
  be picking inside a spread, which is the one thing this file's own rule
  forbids.
- **What the raise would save is the join, and the join is #406's.** The
  separation the sweep does find -- 9 to 13 µs a split for an uncapped chain
  -- is the wait "The #400 attribution" attributed in full, and that section
  bounded the per-split cost at the 1 to 2 µs its four candidates account for
  once the far half is joined by its own node instead of a `Runtime::scope`.
  A floor raised today is therefore tuned against a cost that change removes,
  and a shipped default is harder to move back than a floor is to raise.
- **What the raise would cost is permanent.** A floor of `n / (2w)` is one
  re-split a share and no more, which is the mitigation "The #400
  attribution" called a mitigation and not a fix for exactly this reason: a
  worker arriving late to an already-stolen share finds nothing to take. The
  floor of one is also the only row above whose worker count reads at or above
  its own guided control in all three passes.
- **The caller who wants the cheap setting has it today.**
  `SplittingPartitioner::new(k)` is a floor of `k`, and the type's docs now
  say what the default costs against it, which is the part of the issue that
  was missing rather than the default itself.
- **#409 is filed to re-take this sweep once #406 lands**, with the same rows
  and the same bar, because that is the change that makes the floor worth
  re-deciding.

**What this does not say.** It does not say the floor of one is good: it is
2.0x, 2.5x and 2.25x its own guided control in three passes and the most
expensive of the seven rows in both orders, and that is now in the type's
docs rather than only in this file. It does not say a derived floor is wrong
-- it says this sweep cannot tell `n / (2w)` from `n / (16w)` or from no
splitting at all, which is a different claim and the reason nothing ships. And
it does not re-measure the `rustflow-split` criterion ids: no code changed, so
#381's part 2 table stands as taken.

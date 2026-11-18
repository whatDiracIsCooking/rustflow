# Benchmarks

Seven of upstream's benchmarks, ported to criterion with a rayon baseline in
the role TBB's flow graph plays upstream: `linear_chain`, `binary_tree`,
`graph_traversal` and `embarrassing_parallelism` from #18, then `async_task`
and `fibonacci` from #96, then `sort` from #174, each translated from the
matching directory under Taskflow v4.1.0's `benchmarks/`. The shapes and the
measured regions are upstream's; what the sizes, the baseline and the
harness are is set out below, because a number whose provenance is unstated
is not a result.

The two shapes #96 added are the two with **no graph in them at all** --
`silent_async` submissions and a recursive spawn -- so the construction
asymmetry the first four have to warn about is absent from their tables, and
they were measured in a separate run with a load of their own. Their section
records both. The third run is #119's: it is the A/B of one change on the
scheduling path across all six shapes, and it supersedes the
`rustflow-runtime` column of the `fibonacci` table. The fourth run is #102's, on the same quiet box: an A/B of the *spill-buffer
count* across all six shapes, which changed no shipped code and is the reason
the count is still `bit_width(num_workers)`. The last section is a fifth run,
#125's, which changed no shipped code either: it takes the per-task cost of
the `async_task` shape's external submission apart with counters and
ablations, and records what it is. The sixth is #144's, which makes the one
change that run named and could not reach: a free async node's arena sized
to the one node it will ever hold. The seventh and last is #174's, which adds
the seventh shape, `sort`, and measures it in a run of its own.

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

**These four rows predate #40**, which made a queued job own a share of its run
and of the taskflow's storage and cost this shape about 19% at 24 workers --
see "The #40 decision" below for the measurement. The rows are left as measured
rather than scaled. They also predate **#62**, which ported upstream's
continuation optimisation and took 40 to 50% off this shape at 24 workers --
far more than #40 and #74 together had added -- so the paragraph above about
23 idle workers costing the busy one 40% is the old scheduler's behaviour.
"The #62 measurement" below has the figures.

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
none of the rows above would move; the shapes that would are the nested ones,
and nothing here measures those yet.

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
as the evidence.**

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
  once for the batch.
- **Submitting from a worker saves 30%** -- 305 ns against 440 -- and the
  mechanism is *not* the one this bullet first gave: both ids push into the
  shared buffers, so the saving is not a worker's own queue against them. What
  differs is the submitting thread, the per-task accounting a **parentless**
  submission takes that a group's child does not (`increment_async`'s two
  counters, and the last decrement of each meeting a waiter under a mutex),
  and the wait -- `wait_for_all` against a `corun` that also runs the tasks.
  This run separated none of those, so the figure stands and the cause is
  open; #119's section below confirms the figure does not move when a
  *runtime* body's destination changes.
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
benchmark sweeps to `fib(40)`, and `rustflow-group` is why.** A corun keeps its
worker in the work-stealing loop, so every job that worker picks up while
waiting nests one frame deeper, and a corun handed the oldest outstanding job
in the pool -- somebody else's unfinished subtree -- rather than its own newest
child nests one frame per outstanding *child* instead of one per level of
recursion, until a worker's 2 MiB stack runs out. That was both variants when
#96 measured them; #119 gave the **runtime** variant upstream's
`if(auto w = this_worker(); w)` branch, so its children go onto the submitting
worker's LIFO queue and its corun pops them straight back. The cliff was then
re-measured in this profile, with a release build of both recursions driven
straight (no criterion):

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

**One worker is the binding end for the group variant, and that is the
mechanism showing itself**: the one worker that coruns is the only worker there
is to pick the outstanding children up, so it nests a frame for every one of
them, while 24 workers spread the same tree over 24 stacks and reach six levels
further. Since both variants measure the same `n`, the group's one-worker cliff
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
  cross-check that this shape has no cost of its own at one worker.
- **24 workers make it 7x slower**, 335 µs to 2.3 ms for the same 986 children,
  or ~2.4 µs each. This was the worst 1-to-24 scaling in the file and it is the
  same cause as the stack cliff above: every child crossed into the shared
  buffers and had to be stolen back out, and a parent that coruns was competing
  for its own children with 23 other workers. Upstream's LIFO branch is what
  this shape is built to exploit, and #119 gave it to the runtime variant --
  which is where a third of the one-worker time and a quarter to a third of the
  24-worker time went; see below. The group variant still pays this.
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
4 KiB in debug and 870 bytes in release. A `fib` recursion nests one such
level per level of recursion now, so its own bound is thousands of levels deep
and its task count (1.6^n) runs out of clock first. The group rows are what
`tests/task_group.rs` and this benchmark's sizes are chosen inside.

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
  (56 B). The middle figure is 176 B on today's `main` -- #144's section below
  -- and that section also splits this bullet's 56-byte "boxed body" into the
  two things a per-size histogram shows it to be.

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
submission loop, and they are a property of how a node that belongs to no
taskflow is represented here: `executor::async_node` gives such a task its own
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
of reason. **That is the recorded cost of the architecture, with the ablations
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
all. What is reachable is the *size* of the second block, which is this
change.

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
first. Each sample here is `Bencher::iter_batched` instead: a fresh shuffle
in the untimed setup, consumed by the timed sort -- `benches/sort.rs`'s own
module docs say so, and it is why this table carries no "construction-only"
column the way `embarrassing_parallelism`'s does: the untimed setup already
isolates the one cost this shape has that rayon's does not.

| | |
|---|---|
| CPU | AMD Ryzen 9 5900X, 12 cores / 24 threads |
| `available_parallelism` | 24 |
| OS / toolchain | Debian 12 (bookworm) container, rustc 1.100.0, `--release` |
| criterion | 0.8.2, 20 samples, 1s warm-up, 3s measurement per id |
| Date | 2026-11-18 |
| **Load average** | **11.3 falling to 8.3 over the run** |

Three sibling agents' `cargo` builds were running on this box throughout, so
this is not a quiet machine -- "The machine, and what it was doing" above has
the same caveat for the original four shapes, and it applies here unchanged:
read the within-row comparison, not the absolute figure.

| size | workers | rustflow | rayon | upstream C++ |
|---|---|---|---|---|
| 1000 | 1 | 21.61 µs ±1.9% | 12.85 µs ±0.5% | not published |
| 1000 | 24 | 23.14 µs ±1.7% | 16.87 µs ±1.4% | not published |
| 1000000 | 1 | 37.12 ms ±0.2% | 25.28 ms ±0.3% | not published |
| 1000000 | 24 | 10.82 ms ±1.4% | 3.428 ms ±1.0% | not published |

At 1000 elements -- below this type's parallel cutoff on every worker count --
both columns take their serial path, so the gap is construction and dispatch
rather than sorting: our run builds a `Taskflow`, hands it to an `Executor`
and tears both down, where rayon's `install` has no graph to build at all.
24 workers cost both columns a little at this size, for the reason
`linear_chain`'s own table gives -- there is nothing here for the other 23 to
do.

At 1000000 elements the parallel path runs on both sides, and the gap widens
rather than closes: rayon is 3.6x faster at one worker and 3.2x at 24. Two
differences compound rather than one. `Runtime::sort_by`'s own partition is a
single-pivot, three-way split per level against `par_sort_unstable`'s own
pattern-defeating quicksort, so this crate does strictly more comparisons and
swaps for the same input; and every spawned half goes through this crate's
own node machinery -- `Scope::spawn_runtime`, a fresh arena, a join -- where
rayon's split is a `join` of two closures with one atomic. Both gaps are the
ones `binary_tree`'s own table already found for a plain increment; sorting
does not change which one dominates, only how much larger the constant is
that it is multiplying.

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

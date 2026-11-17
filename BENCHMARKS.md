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
  It was not taken here because the storage share has to be *released* at a
  particular instant -- before the join counter can end the run -- and a share
  inside the run's own allocation cannot be, which would make
  `taskflow.emplace()` panic at random for a few instructions after a `wait`
  returned. `tests/basics.rs`'s
  `a_taskflow_is_editable_the_instant_its_run_reports_done` is the case that
  would fail. Half the atomics for a racy panic is the wrong trade; the
  continuation optimisation is the one to do first.

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
scheduler (`Job`, in `src/executor/topology.rs`) and this would have been the
second; #40 has since removed that one, so the only `unsafe` left anywhere in
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

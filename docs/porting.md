# How the port was built

The [`README`](../README.md) is the page a user of the crate wants and
[`development.md`](development.md) the page a contributor wants. This is the
page for someone asking how the port got here: the order the work ran in,
what each step covers and deliberately leaves out, where the two `unsafe`
modules came from, and the two performance results worth a paragraph each.
The decisions themselves are in [`DESIGN.md`](../DESIGN.md), one entry per
decision and each naming the issue that took it; this page is the map, not
the record.

## Order of work

Each step is a milestone of PR-sized issues; the upstream unit tests under
`unittests/` are the acceptance oracle, translated per module. The toolchain,
container setup and CI gate the whole thing rides on were in place and
verified before step 1. A milestone still open after the feature it names
works is open on follow-ups rather than on the feature. All seven steps are
in, and the port is at parity with upstream's user-facing surface at v4.1.0.

1. **Static graph** — `Taskflow`, `Task`, the flow builder, DOT dump.
2. **Executor** — workers, deque, notifier, `run`/`run_n`/`wait_for_all`.
3. **Control flow** — condition tasks, cancellation, panic propagation.
4. **Dynamic tasking** — subflows, runtime tasks, module composition.
5. **Async** — async and dependent-async tasks submitted from the executor,
   from a runtime task or from a task group, with and without a handle on the
   result; task groups; and semaphores.
6. **Observers and algorithms** — an `Observer` hook around every task a
   worker runs, with a built-in Chrome trace-event writer and a `tfprof` one
   over the same timeline; and `for_each`, `for_each_index`, `transform`,
   `fill`, `generate`, `reduce`, `find`, `sort`, `scan` and `merge` over a
   borrowed slice or an `IndexRange`, each with a choice of partitioner and a
   `FlowBuilder` wrapper over owned data. This is the step that added
   `Runtime::scope`, which the chunk loop under every one of those algorithms
   spawns through.
7. **Pipelines** — the one family with no Rust equivalent: a fixed chain of
   `Pipe`s run over a bounded number of parallel lines, `DataPipeline`'s
   typed builder over the same scheduler for pipes that pass data from each
   to the next, and the `Composable` trait both compose into another graph
   through. Deferred pipelines are not part of v4.1.0 and so are not ported:
   there is no `defer` in `pipeline.hpp` at the pin, and while upstream's
   `test_deferred_*.cpp` files exist, they are not in its own test
   `CMakeLists`.

**Six milestones have no step number.** A **consolidation** ran between
steps 5 and 6, on what five steps had left behind: five milestones' worth of
rationale collected in doc comments, an async family written three times over
its three receivers, and the run's machinery one 2,352-line file over a dozen
types, six of the previous milestone's seven post-merge bugs having been
counter or share-lifecycle errors in it. It changed no public API; what it
left is `executor::topology` as a module tree, an async family of one-line
delegations onto a single core, every doc comment held to a written
documentation contract, and the integration suites sharing one `tests/common`
module. The **parity pass** after step 7 closed the remaining gap between the
seven steps and upstream's own surface, and recorded what it declined; the
README's "What is not ported" list is that record. A **second consolidation**
ran after that pass, on what its own friction log
([`api-friction.md`](api-friction.md)) collected, and this one did reach the
surface: it settled what the crate root re-exports and what
`rustflow::prelude` is for, collapsed the algorithm family's eight extension
traits into one and completed the `make_*_task` factories behind them, gave
every async submission a `_named` twin, and fixed two scheduler defects its
own panic matrix found. **Benchmarks and performance** ran fourth and is the one
of the five that changed no API at all: it took the suite from eight of
upstream's shapes to twenty-two — thirteen new `[[bench]]` targets for the 22
in `Cargo.toml` and 196 criterion ids, 232 today — on
`benches/common` for the fixtures a target may share,
`devtools/bench-ab.sh` for an interleaved A/B between two refs, and a CI step
that smoke-runs every id. Four of its measurements
overturned something this repo had asserted: a nested `corun` costs about a
kilobyte of stack a level, so what bounds those shapes' published sizes is the
worker's stack rather than the clock (#359); the preempted-parent re-queue
the port declines to optimise fires twelve times over a run of the whole
suite, all twelve in `pipeline` and none in any nested shape, which is not
what the stack entry had expected of it (#342); a chunk of the algorithms'
chunk loop costs 15 ns and 1.3% of its run, leaving a fan-out that reaches 15
to 19 of 24 workers as the open lead (#343); and 24 of the 196 ids were timing
their own input setup (#366). **Scheduler performance** ran fifth, on the worst
thing those tables said: five nested shapes — `skynet`, `nqueens`,
`integrate`, `fibonacci` and `merge_sort` — were *slower* at 24 workers than
at one. Twenty-six issues later they are not, and the two changes that did
it are both places this port had diverged from the design it translates: a
task group opened on a `Runtime` holds the submitting worker's own queue and
coruns on that end (#372), where every group child used to go to the shared
buffers; and `corun_until` re-reads its predicate after every failed steal
(#412), where it used to read it once a 211-attempt search — upstream's
`_schedule_async_task` and `_corun_until` already did both. Each of the five is now faster
at 24 workers than at one, where the worst of them was 6.9x slower. The
milestone's own last act is what makes that a finding rather than a hope: it
put a C++ toolchain in the image and built **upstream's own benchmark
binaries** (#383), so the `upstream C++` column of those five tables is
upstream's executor measured on the same CPU (#384) — and since upstream gains
from the width on every shape and size it can be asked for, the regression was
this port's and not Taskflow's. Three measurements in it ship nothing and are
kept for what they rule out: the fan-out's ceiling is one queue and one
producer rather than a wake-up policy (#367), a chain at 24 workers costs what
a chain at one worker costs and the 40% was the continuation's (#373 — whose
one by-product, a guard upstream has on the semaphore release hook, shipped as
#393 and is worth 16% of a chain's run region), and
`SplittingPartitioner`'s default floor is the one the sweep cannot separate
from any other (#401, re-taken as #409 once #412 had removed the search its
price was mostly made of, which also closed #406 as declined). It also declined two changes outright,
both recorded in [`DESIGN.md`](../DESIGN.md) with their reasons: stack-allocated
jobs for scoped children, and a splittable source range for external
submission.

**Test hardening** ran last and is the only one of the six that changed no
production line of `src/` at all — every line it added there is inside a
`#[cfg(test)] mod tests` — because what it was for is the evidence behind the
suite rather than the code under it: a coverage lane and a Miri lane beside the
gate, the per-file line coverage of `src/` turned into a gate of its own over a
floors file, and the executor core mutation-tested in four runs of some 966
mutants. Those four runs and the two re-measurements that re-took the first
two of them **found no bug in the port**: every finding was a defect in a test,
in an instrument or in the method, most often a test that asserted less than it
appeared to — a routing case measured at a width where the mutated shift is
harmless, an RNG known answer seeded with the one seed where `|` is `^`, an
agreement asserted over a graph on which every observation was empty.
[`development.md`](development.md)'s "What the suite is held to" is the map it
leaves behind: what the suite covers, which modules have been mutated and when,
which have not and how many mutants they hold, and what runs under Miri.

All six keep their decisions in
[`DESIGN.md`](../DESIGN.md), one entry per decision, in the subsystem
section each belongs to rather than under the milestone that took it — the
benchmarks milestone's own under "Benchmarks and performance", and the three
it took about the port itself under "Executor core" and "Algorithms"; the
scheduler milestone's are spread over "Executor core", "Dynamic tasking",
"The async family" and "Algorithms", and the test-hardening milestone's are
nearly all under "Documentation and tooling", that being the section the
instruments and the mutants no test should kill belong to. That file's preamble
lists where each of them is.

So the step-to-milestone mapping is the seven steps with those six among
them, which are milestones 6, 9, 10, 11, 12 and 13: step 6 is the sixth step
and the seventh milestone, step 7 the eighth. A later step may do the same, and
if one does it belongs in this list the same way, named here rather than
renumbering the steps, since the numbers are what every issue and every
`DESIGN.md` entry has been written against.

## The examples, by step

`examples/` is upstream's own directory, one file per example there, each
ported under the same name and made to **print and assert** rather than only
print. They are the first code in this crate shaped the way a caller writes
it rather than the way a translated module does, which is also what they are
*for*: because an example is the whole public surface at once, each example
PR recorded what it found awkward getting there under a "Friction" heading in
its own PR body — an import that had to be spelled out, a turbofish a type
inference could not close, anything that rubbed. M9's closing issue collected
those into [`api-friction.md`](api-friction.md), one entry per distinct item,
as the input to the second consolidation.

The directory's 44 files, by the step that ported them: step 1's static
graph has `simple`, `run` and `visualization`; step 3's control flow has
`condition`, `if_else`, `nested_if_else`, `multi_condition`, `switch_case`,
`while_loop`, `do_while_loop`, `cancel` and `exception`; step 4's dynamic
tasking has `subflow`, `subflow_exception`, `runtime`, `composition`,
`async_module`, `task_visitor`, `attach_data` and `corun`; step 5's async
family has `asyncs`, `dependent_async`, `runtime_async`, `fibonacci`,
`limited_concurrency` and `tsp`; step 6's observers and algorithms have
`observer`, `chrome_trace`, `worker_interface`, `parallel_for`, `for_loop`,
`parallel_reduce`, `parallel_sort`, `inclusive_scan`, `exclusive_scan` and
`dependent_async_algorithm`; and step 7's pipelines have `pipeline`,
`scalable_pipeline`, `taskflow_pipeline`, `parallel_data_pipeline`,
`parallel_graph_pipeline`, `data_scalable_parallel_pipeline` and
`text_pipeline`. `tfprof` stands alone, for the `TFProfManager` decision
under "What is not ported" in the README.

## The two `unsafe` modules

They are `src/executor/notifier.rs` and `src/executor/scope.rs`, and nothing
else in the crate, which is worth saying out loud because a work-stealing
scheduler is not where a reader expects to find that. In the notifier, a
worker about to park publishes its thread handle into the lock-free waiter
stack and the notifier that pops it reads it back. In `scope`, added in step
6, `Runtime::scope` erases a borrowed child closure's lifetime to the
`'static` bound `Runtime::silent_async` needs, sound only because `scope`
joins every child — run to completion, panicked, or dropped unstarted —
before it returns. The scheduler itself needs none: it was the ownership
discipline described under "A run owns the graph it walks" in
[`DESIGN.md`](../DESIGN.md) that removed the last one, and every slice of
dynamic tasking, of the async family and of the two steps after it has
stayed inside it.

## Two performance results

Every measured number is in [`BENCHMARKS.md`](../BENCHMARKS.md) — twenty-two
of upstream's twenty-five benchmarks ported to criterion, sixteen of them
against a rayon baseline in the role TBB's flow graph plays upstream and the
other six against what rayon has no primitive to stand in for: an `mpsc`
thread chain for the three pipeline shapes, in the role TBB's own
`parallel_pipeline` plays there (#187, #339), a sequential program for `scan`
and `wavefront`, and nothing at all for `linear_chain`, whose shape says of
every pair of tasks the opposite of what a rayon primitive expresses — the
machine and the load each ran under recorded beside them, and after the tables
the per-decision measurements, each named for the issue that took it: among
them #40 and #74 on what a job owns, the continuation #62 ported, the `Work`
question, external submission (#60), a task body's own queue (#119), the
spill-buffer count (#102), what a free async task costs (#125), the boxed
per-line slot a `DataPipeline` pays for over a `Pipeline` of the same shape
(#186, measured in #188) and the benchmarks milestone's four (#359, #342,
#343, #366). They are kept there rather than repeated here, so each figure has
one owner. Benchmarks are deliberately outside the gate: a benchmark answers
"how fast", which is not a pass or a fail, so what CI holds the suite to is
that it still compiles — and that it is still *run once*, the `examples` job
executing every benchmark id a single untimed time in criterion's `--test`
mode, which asks only whether it runs.

One of those results is bad, and tracked rather than filed away, because the
honest state of play is more useful than a table of the shapes that happen to
look good. The other named below, #62, used to be the same kind of bad
result and is now the fix for it:

- **External submission** (issue #60). A graph submitted from a thread that is
  not one of the executor's workers reaches the pool only through the shared
  overflow buffers, and from there one steal at a time, so a graph that is
  nothing but independent sources is distributed entirely by stealing:
  `embarrassing_parallelism` at 65536 tasks and 24 workers measures about 5x
  rayon on the same box in the same run. One obvious lever is **settled**:
  more spill buffers are worth 4 to 6% here and the count stays
  `bit_width(num_workers)` all the same, because it is also every worker's
  victim space and the spin budget before one parks — at 48 buffers a
  thousand-link chain costs 26% more and a task group's recursion 18%, and
  there is no count in between that is a free win. #102 is that measurement;
  it also found #60's *stated* reason for declining the change wrong, since
  what pays is not the shapes that steal from worker queues but the shapes
  with one source task and nothing to steal, which pay per run.
  A free async task submitted the same way costs **~350 ns**, most of it the
  node and the job it builds (#125), and that too is the design rather than a
  tuning gap. What *was* a tuning gap is gone twice over: the node's one-node
  arena stopped carrying a `Vec`'s spare capacity (#144, 528 of the 848 bytes
  a task, 6 to 7% of this path and five times that on the one a *worker*
  submits from) and then stopped existing, the node now sitting inline in the
  `Arc` (#376), so a free async task is **two heap blocks and 312 bytes**
  where it was three and 848. #382 counted that and priced the block at 7 to
  16% of a task at one worker on every shape that allocates one; it also
  priced the free list a worker parks its finished nodes on (#379) at a
  footprint rather than a time.
- **The continuation optimisation** (issue #62, landed in PR #99). Upstream's
  `_update_cache` / `TF_INVOKE_CONTINUATION` keeps one ready successor out of
  the queue and resumes it on the worker that just finished its predecessor,
  so a chain runs end to end where it started instead of being pushed and
  re-stolen per link. It is in now, in `src/executor/worker.rs`: at 24 workers
  a `linear_chain` costs half what it did before the change, and at one
  worker the queueing round trip it skips is still 22 to 34 ns a task. "The
  #62 measurement" in [`BENCHMARKS.md`](../BENCHMARKS.md) has the figures.

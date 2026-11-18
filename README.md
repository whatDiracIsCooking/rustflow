# rustflow

A Rust port of [Taskflow](https://github.com/taskflow/taskflow), a C++
task-parallel programming system built around task dependency graphs.

**Status: all seven steps of "Order of work" are in, the port is at parity
with upstream's user-facing surface at v4.1.0, and the consolidation
milestone after that parity pass has closed** — every public class, free
function and example `taskflow/` ships at the pin has a counterpart here, or
a recorded reason it does not, and what the second consolidation settled
about the surface a caller writes is in [`DESIGN.md`](DESIGN.md) under
"Documentation and tooling", "Algorithms" and "Executor core". "Order of
work", at the end of this file, is where that state is kept rather than
duplicated up here: it says what each step covers and what it deliberately
leaves out, and each step is a GitHub milestone, so its issues are what is
merged and what is not.

Two things are worth knowing before reading any of it:

- **No `unsafe` outside two modules** — `src/executor/notifier.rs` and
  `src/executor/scope.rs`. The scheduler itself has none, which is worth
  saying out loud because a work-stealing scheduler is not where a reader
  expects to find that; "Order of work" says what each of the two buys.
- **The decisions are written down.** [`DESIGN.md`](DESIGN.md) has one entry
  per decision, grouped by the part of the port it decides and each naming the
  issue that took it, beside the documentation contract every comment in this
  crate is held to; its index is the ten sections. The measured ones are in
  [`BENCHMARKS.md`](BENCHMARKS.md), which "Performance" indexes.

## Building and testing

[`docs/development.md`](docs/development.md) is the contributor page: the
pinned toolchain, the container both front ends build, the five-command gate
`devtools/devcontainer.sh check` runs, and what CI runs beside it.

Four measurements of this crate's own documentation and tests sit beside that
gate without being part of it, all four **reports** that exit 0 whatever they
find: `devtools/doc-ratio.sh` (the documentation contract's prose ratio),
`devtools/test-names.sh` (the test-name comparison a documentation pass is
held to), `devtools/coverage.sh` (line and region coverage of `src/`) and
`devtools/api-witness.py` (the public declarations no test, example, benchmark
or doc example witnesses). What does decide a pass is the gate above plus
`devtools/run-examples.sh`, which CI's `examples` job runs on every PR.

## Examples

[`examples/`](examples/) is upstream's own directory, one file per example
there, each ported here under the same name: `cargo run --example simple` and
`cargo run --example run` build and run them directly. They are the first
code in this crate shaped the way a caller writes it rather than the way a
translated module does, which is also what they are *for*: upstream's
examples only print, so the port's **print and assert** — every line upstream
shows is checked with `assert!` or `assert_eq!`, making a non-zero exit the
test. `devtools/run-examples.sh` runs every `examples/*.rs` this way, by
glob rather than by a list it would need editing on each addition, and the
`examples` job of [`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs
that script on every PR.

Because an example is the whole public surface at once, each example PR
records what it found awkward getting there under a "Friction" heading in
its own PR body — an import that had to be spelled out, a turbofish a type
inference could not close, anything that rubbed. M9's closing issue
collected those into [`docs/api-friction.md`](docs/api-friction.md), one
entry per distinct item, as the input to the consolidation milestone that
follows it.

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
`text_pipeline`. `tfprof` stands alone, for the profiler decision under
"What parity leaves out" in "Order of work", below.

## Provenance and attribution

This crate is a Rust reimplementation of Taskflow, not a binding. Both projects
are MIT licensed, so there is no compatibility question — only an attribution
one, and MIT answers it: the copyright notice *"shall be included in all copies
or substantial portions of the Software."*

- [`LICENSE`](LICENSE) — this port's own copyright, covering the Rust code.
- [`LICENSE-TASKFLOW`](LICENSE-TASKFLOW) — upstream's notice, byte-for-byte,
  covering the design and any closely translated portions. It is kept as a
  **separate** file on purpose: so that it stays unambiguous whose copyright
  covers what, and so its non-standard `TASKFLOW MIT LICENSE` header does not
  confuse license detection on this repo's own `LICENSE`.

**The per-module rule:** any module translated closely from upstream names its
source in its `//!` doc comment, on the first line:

```rust
//! Translated from `taskflow/core/executor.hpp` (Taskflow v4.1.0).
```

The version is part of the line. Upstream rewrites its scheduler between major
versions, so "translated from `executor.hpp`" without saying which one is not
an auditable claim.

Blanket attribution in a LICENSE file is the legal minimum and tells you
nothing useful a year later. The per-module line is what makes it auditable —
it records which modules were *translated* and which were *invented*, which is
exactly the distinction that gets forgotten first. If a module is your own
design that merely implements the same concept, say nothing; the absence is
then meaningful.

One thing not covered by any of the above: upstream's `3rd-party/` directory
carries its own separate terms. Nothing ported out of there may ride on
`LICENSE-TASKFLOW` — treat it as a fresh licensing question.

## Porting notes

### Upstream pin

This port follows **Taskflow v4.1.0**, tag `v4.1.0`, commit
`45366fe5bc4f2f8ec9aa590b40c504e296886865` (2026-06-19). Every "Translated
from" line, every ported unit test and every benchmark comparison refers to that
tree. Moving the pin is a deliberate, repo-wide change, not something one PR
does on the side.

The v4 line is what we port, not v3: it is C++20, splits `NodeBase` out of
`Node`, schedules in bulk into the work-stealing queues, defaults to the
nonblocking notifier, and adds `TaskGroup` and adopted module tasks. Nothing
from `cuda/` or `3rd-party/` is in scope.

### Dependency policy

Upstream's library core depends on nothing but the C++ standard library; its
`3rd-party/` directory serves only tests, benchmarks and the profiler. We keep
the same split:

- A **runtime** dependency must replace something upstream hand-wrote *and* be
  better tested than our own port of it would be. Today that admits
  `crossbeam-deque` (the Chase-Lev deque behind `wsq.hpp`) and `crossbeam-utils`
  (cache-line padding and backoff), and nothing else — both are in as of the
  work-stealing queue layer, and the only other crate in the tree is
  `crossbeam-epoch`, which `crossbeam-deque` pulls in itself. Anything optional
  goes behind a Cargo feature so the default build stays at those two.
- **Dev** dependencies need only be useful: `loom` for the lock-free parts
  and for the cross-thread rules a machine-checked model states better than a
  paragraph — eight models under `tests/loom_*.rs`, one per protocol or rule
  and each named in [`docs/development.md`](docs/development.md),
  `criterion` for the ported benchmarks, `rayon` as the baseline those
  benchmarks compare against, in the role TBB plays upstream, and `proptest`
  for the generated inputs `tests/properties.rs` checks the algorithms
  against `std`'s own answer over.

### Design decisions already taken

They live in [`DESIGN.md`](DESIGN.md), one entry per decision and each still
naming the issue that took it, grouped by the part of the port it decides --
graph and flow builder, executor core, control flow and cancellation, dynamic
tasking, the async family, observers, algorithms, pipelines, documentation and
tooling -- beside the documentation contract this crate's comments are held
to. The measured ones keep their numbers in
[`BENCHMARKS.md`](BENCHMARKS.md), which "Performance", below, indexes.

### Performance

Every measured number this port has is in [`BENCHMARKS.md`](BENCHMARKS.md) —
upstream's benchmarks ported to criterion with a rayon baseline in the role
TBB's flow graph plays upstream, plus the pipeline (issue #187), which has no
rayon equivalent and compares against an `mpsc` thread chain in the role TBB's
own `parallel_pipeline` plays there — the machine and the load each ran under
recorded beside them, and after the tables the per-decision measurements: #40
and #74 on what a job owns, the continuation #62 ported, the `Work`
question, external submission (#60), a task body's own queue (#119), the
spill-buffer count (#102), what a free async task costs (#125) and the boxed
per-line slot a `DataPipeline` pays for over a `Pipeline` of the same shape
(#186, measured in #188). They are kept there rather than repeated here, so
each figure has one owner. Benchmarks are deliberately outside the gate: a
benchmark answers "how fast", which is not a pass or a fail, so what CI
holds the suite to is that it still compiles.

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
  A free async task submitted the same way costs ~415 ns, 87% of it the node
  it allocates (#125), and that too is the design rather than a tuning gap.
  The one part of it that *was* a tuning gap is gone: the node's one-node
  arena no longer carries a `Vec`'s spare capacity, which was 528 of the 848
  bytes a task, 6 to 7% of this path and five times that on the one a *worker*
  submits from (#144).
- **The continuation optimisation** (issue #62, landed in PR #99). Upstream's
  `_update_cache` / `TF_INVOKE_CONTINUATION` keeps one ready successor out of
  the queue and resumes it on the worker that just finished its predecessor,
  so a chain runs end to end where it started instead of being pushed and
  re-stolen per link. It is in now, in `src/executor/worker.rs`: at 24 workers
  a `linear_chain` costs half what it did before the change, and at one
  worker the queueing round trip it skips is still 22 to 34 ns a task. "The
  #62 measurement" in [`BENCHMARKS.md`](BENCHMARKS.md) has the figures.

### Order of work

Each step is a milestone of PR-sized issues; the upstream unit tests under
`unittests/` are the acceptance oracle, translated per module. The toolchain,
container setup and CI gate the whole thing rides on were in place and
verified before step 1. A milestone still open after the feature it names
works is open on follow-ups rather than on the feature — the performance ones
are named under "Performance", above.

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

**The two `unsafe` modules**, and nothing else in the crate. One is
`src/executor/notifier.rs`, where a worker about to park publishes its thread
handle into the lock-free waiter stack and the notifier that pops it reads it
back. The other is `src/executor/scope.rs`, added in step 6, where
`Runtime::scope` erases a borrowed child closure's lifetime to the `'static`
bound `Runtime::silent_async` needs, sound only because `scope` joins every
child — run to completion, panicked, or dropped unstarted — before it
returns. The scheduler itself needs none: it was the ownership discipline
described under "A run owns the graph it walks" in [`DESIGN.md`](DESIGN.md)
that removed the last one, and every slice of dynamic tasking, of the async
family and of the two steps above has stayed inside it.

**What parity leaves out.** A pass after step 7 closed the remaining gap
between the seven steps and upstream's own surface, and recorded what it
declined:

- `cuda/` and `3rd-party/`, out of scope from the start — see "Upstream pin",
  above.
- Deferred pipelines, since `pipeline.hpp` at the pin has no `defer` (step 7,
  above).
- `NonpreemptiveRuntime` as a user-facing type. The port keeps an internal
  nonpreemptive node kind a pipeline's line task dispatches through instead
  of a type a caller names directly — [`DESIGN.md`](DESIGN.md)'s entry on the
  eleventh `Handle` variant has why.
- `TFProfManager`. A library has no business installing a process-global
  singleton whose destructor writes a file to a path an environment variable
  names — [`DESIGN.md`](DESIGN.md)'s entry on `TFProfManager` has the
  decision and what is kept as documentation instead.
- The serializer and the rest of upstream's `utility/` directory that `std`
  already covers without a port.

**Three milestones have no step number.** A **consolidation** ran between
steps 5 and 6, on what five steps had left behind: five milestones' worth of
rationale collected in doc comments, an async family written three times over
its three receivers, and the run's machinery one 2,352-line file over a dozen
types, six of the previous milestone's seven post-merge bugs having been
counter or share-lifecycle errors in it. It changed no public API; what it
left is `executor::topology` as a module tree, an async family of one-line
delegations onto a single core, every doc comment held to a written
documentation contract, and the integration suites sharing one `tests/common`
module. The **parity pass** after step 7 is where "What parity leaves out",
above, comes from. A **second consolidation** ran after that pass, on what
its own friction log ([`docs/api-friction.md`](docs/api-friction.md))
collected, and this one did reach the surface: it settled what the crate root
re-exports and what `rustflow::prelude` is for, collapsed the algorithm
family's eight extension traits into one and completed the `make_*_task`
factories behind them, gave every async submission a `_named` twin, and fixed
two scheduler defects its own panic matrix found. All three keep their
decisions in [`DESIGN.md`](DESIGN.md), one entry per decision, in the
subsystem section each belongs to rather than under the milestone that took
it.

So the step-to-milestone mapping is the seven steps with those three
interleaved: step 6 is the sixth step and the seventh milestone, step 7 the
eighth. A later step may do the same, and if one does it belongs in this list
the same way, named here rather than renumbering the steps, since the numbers
are what every issue and every `DESIGN.md` entry has been written against.

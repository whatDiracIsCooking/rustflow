# rustflow

[![ci](https://github.com/whatDiracIsCooking/rustflow/actions/workflows/ci.yml/badge.svg)](https://github.com/whatDiracIsCooking/rustflow/actions/workflows/ci.yml)

rustflow is a Rust port of [Taskflow](https://github.com/taskflow/taskflow),
the C++ task-parallel programming system: you describe a program as a graph
of tasks and the dependencies between them, and a work-stealing executor runs
it. Static graphs, subflows spawned from inside a task, condition tasks that
pick the next step at run time, composition of one graph into another, async
tasks with and without dependencies, parallel algorithms over slices and
ranges, and pipelines are all here, each behind the Rust shape of upstream's
API rather than a binding to it.

The port follows **Taskflow v4.1.0** and is at parity with its user-facing
surface: every public class, free function and example upstream ships at that
tag has a counterpart here, or a recorded reason it does not (see "What is
not ported", below). It has two runtime dependencies, no `unsafe` outside two
named modules, and every design decision written down with the issue that
took it.

## Start your first rustflow program

The four-task graph upstream opens with, `A` before `B` and `C`, `D` after
both:

```rust
use rustflow::Executor;
use rustflow::prelude::*;

fn main() {
    let executor = Executor::default();
    let mut taskflow = Taskflow::with_name("simple");

    let a = taskflow.emplace(|| println!("TaskA"));
    let b = taskflow.emplace(|| println!("TaskB"));
    let c = taskflow.emplace(|| println!("TaskC"));
    let d = taskflow.emplace(|| println!("TaskD"));

    taskflow.task(a).name("A").precede([b, c]); // A runs before B and C
    taskflow.task(d).name("D").succeed([b, c]); // D runs after  B and C

    executor.run(&taskflow).wait().expect("no task panicked");
}
```

`emplace` hands back a `Task`, an index into the graph; `taskflow.task(a)`
is how the graph is edited through it. `Executor::run` hands back a
`RunHandle`, and `wait` on it returns `Err(RunError::Panicked)` if any task
panicked, so a run's failure is a value rather than an aborted process. This
is `examples/simple.rs`; `cargo run --example simple` runs it.

## Create a subflow

A task can build and run a graph of its own while the parent graph is
running. The body is handed a `Subflow`, which is a `FlowBuilder` over a
subgraph the task owns, and the subgraph runs when the body returns or when
it calls `join` explicitly:

```rust
let b = taskflow.emplace_subflow(|subflow: &mut Subflow<'_>| {
    let b1 = subflow.emplace(|| println!("TaskB1"));
    let b2 = subflow.emplace(|| println!("TaskB2"));
    let b3 = subflow.emplace(|| println!("TaskB3"));
    subflow.task(b1).precede([b3]);
    subflow.task(b2).precede([b3]);
});
```

`examples/subflow.rs` and `examples/run.rs` have the whole program, and
`examples/fibonacci.rs` is the recursive case.

## Integrate control flow into a task graph

A condition task returns an index, and that index picks which of its
successors runs next. Since a successor may be an earlier task, this is also
how a graph loops:

```rust
let c = taskflow.emplace_condition(move || {
    if counter.load(Ordering::Relaxed) != 5 { 0 } else { 1 }
});
taskflow.task(a).precede([b]);
taskflow.task(b).precede([c]);
taskflow.task(c).precede([b, d]); // 0 loops back to B, 1 goes on to D
```

A dependency on a condition task is *weak*: a task the conditioner may never
choose cannot be waited for, which is what keeps the cycle above from being a
deadlock. `emplace_multi_condition` returns a set of indices instead of one.
`examples/condition.rs`, `if_else.rs`, `switch_case.rs`, `while_loop.rs`
and `do_while_loop.rs` cover the shapes; `cancel.rs` and `exception.rs` show
a run being cancelled and a panic reaching `wait`.

## Compose task graphs

A taskflow can run as a single task of another. The module task owns a
share of its target, so one graph can be composed into as many others as the
caller likes:

```rust
let f1 = Arc::new(f1);
let module = f2.composed_of(Arc::clone(&f1));
f2.task(f2c).precede([module]);
f2.task(module).precede([f2d]);
```

`adopt` does the same for a graph with no other owner, and the `Composable`
trait is what both accept, so a `Pipeline` composes the same way. See
`examples/composition.rs`.

## Launch asynchronous tasks

The executor is also a thread pool. `spawn` hands back a handle on the
result, `silent_async` hands back nothing, and the `dependent_async` family
puts edges between async tasks as they are submitted, building a graph one
task at a time while the tasks already submitted run:

```rust
let fu = executor.spawn(|| 1);
executor.silent_async(|| println!("nobody waits for this"));

let (a, _) = executor.dependent_async(|| println!("A"), []);
let (b, _) = executor.dependent_async(|| println!("B"), [&a]);
let (c, _) = executor.dependent_async(|| println!("C"), [&a]);
let (_, fu_d) = executor.dependent_async(|| println!("D"), [&b, &c]);

assert_eq!(fu.wait().expect("no panic"), 1);
fu_d.wait().expect("no panic");
```

The same forms exist on a `Runtime` handed to a runtime task, on a
`TaskGroup`, and with a `_runtime` body that can spawn work of its own.
`Semaphore` is the one constraint that spans tasks instead of following
edges: a task that cannot acquire every count it asks for is parked rather
than run. See `examples/asyncs.rs`, `dependent_async.rs`,
`runtime_async.rs` and `limited_concurrency.rs`.

## Leverage parallel algorithms

`for_each`, `for_each_index`, `transform`, `fill`, `generate`, `reduce`,
`find_if`, `min_element`, `sort`, the two scans and `merge` come in three
layers: a method on `Runtime` over a borrowed slice, a task on a
`FlowBuilder` over owned data, and a `make_*_task` factory for a submission
with no builder behind it. Each takes a partitioner, static, guided, dynamic
or random:

```rust
use rustflow::algorithm::partitioner::DefaultPartitioner;

let init = Arc::new(Mutex::new(i32::MAX));
taskflow.reduce(
    Arc::clone(&data),
    Arc::clone(&init),
    |a: &i32, b: &i32| (*a).min(*b),
    DefaultPartitioner::default(),
);
```

Pipelines are the family with no Rust equivalent to lean on. A `Pipeline` is
a fixed chain of serial or parallel `Pipe`s run over a bounded number of
lines, and it runs by being composed into a graph:

```rust
let pipeline = Pipeline::new(num_lines, vec![pipe0, pipe1, pipe2]);
let task = taskflow.composed_of(&pipeline);
```

`DataPipeline` is a typed builder over the same scheduler for pipes that pass
a value from each to the next. `examples/parallel_for.rs` through
`text_pipeline.rs` cover both families; the lists under "Examples", below,
name them all.

## Run a taskflow through an executor

`run`, `run_n` and `run_until` submit a graph once, a fixed number of times,
or until a predicate says stop; each has a `_with` twin that takes a callback
the last job of a run calls, and a `run_owned` twin that takes the taskflow
by value. All are safe to call from any thread, workers included:

```rust
executor.run(&taskflow).wait()?;
executor.run_n(&taskflow, 4).wait()?;
executor.run_until(&taskflow, move || counter.fetch_add(1, Ordering::Relaxed) == 3).wait()?;
executor.wait_for_all(); // nothing left in flight, async tasks included
```

`RunHandle` is also a `Future`, and `cancel` on it, or on the `Canceller` it
hands out, stops a run from scheduling further tasks. `Executor::corun` lets
a worker wait on a nested run by working on it rather than parking.
`examples/run.rs` and `corun.rs` have the whole set.

## Visualize a taskflow graph

`dump` writes the graph as Graphviz DOT, with subflows and composed modules
drawn as clusters:

```rust
println!("{}", taskflow.dump());
```

`examples/visualization.rs` writes one out, and the executor's observers do
the same for a *run*: `ChromeObserver` writes the trace-event JSON
`about://tracing` loads, and `TfProfObserver` the binary block upstream's
own profiler viewer reads. See `examples/observer.rs`, `chrome_trace.rs`
and `tfprof.rs`.

## Examples

[`examples/`](examples/) is upstream's own directory ported file for file,
44 of them, under the same names: `cargo run --example <name>` builds and
runs one. Upstream's examples only print; the port's **print and assert**.
Every line upstream shows is checked with `assert!` or `assert_eq!`, so a
non-zero exit is the test, `devtools/run-examples.sh` runs every
`examples/*.rs` that way, and the `examples` job of
[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs that script on
every PR -- followed, in the same job, by every benchmark id once in
criterion's untimed `--test` mode, so the job holds both directories to "does
it still run".

By family: the static graph has `simple`, `run` and `visualization`; control
flow has `condition`, `if_else`, `nested_if_else`, `multi_condition`,
`switch_case`, `while_loop`, `do_while_loop`, `cancel` and `exception`;
dynamic tasking has `subflow`, `subflow_exception`, `runtime`,
`composition`, `async_module`, `task_visitor`, `attach_data` and `corun`;
the async family has `asyncs`, `dependent_async`, `runtime_async`,
`fibonacci`, `limited_concurrency` and `tsp`; observers and algorithms have
`observer`, `chrome_trace`, `worker_interface`, `parallel_for`, `for_loop`,
`parallel_reduce`, `parallel_sort`, `inclusive_scan`, `exclusive_scan` and
`dependent_async_algorithm`; pipelines have `pipeline`, `scalable_pipeline`,
`taskflow_pipeline`, `parallel_data_pipeline`, `parallel_graph_pipeline`,
`data_scalable_parallel_pipeline` and `text_pipeline`; and `tfprof` stands
alone, for the reason under "What is not ported".

## Requirements

- **Rust 1.100, edition 2024.** The toolchain is pinned by `RUST_VERSION` in
  the [`Dockerfile`](Dockerfile), and [`docs/development.md`](docs/development.md)
  is the contributor page: the container both front ends build, the
  five-command gate `devtools/devcontainer.sh check` runs, and what CI runs
  beside it. Four lanes do, and not one of them is a repeat count: the suite a
  second time pinned to two cores with `taskset -c 0,1`, which is how most of
  the lifecycle races this port closed were found; a `coverage` job on every
  pull request, publishing the per-file table and the listing of every function
  the suite never executed; and weekly, the nine `loom` models under
  `tests/loom_*.rs` and a `miri` job on a date-pinned nightly — the unit tests
  of three modules whose discipline the type system is not checking, then a
  four-case executor lifecycle witness set at two workers, which is the only
  thing putting `src/executor/scope.rs` under the interpreter. Nothing in
  either Miri step is ignored. Four measurements of the crate's own
  documentation and tests sit beside that gate:
  `devtools/doc-ratio.sh`, `devtools/test-names.sh`,
  `devtools/coverage.sh` and `devtools/api-witness.py`. Three are reports that
  exit 0 whatever they find, and so is `devtools/coverage.sh` in its default
  and `--functions` modes; its `--check-floors` mode is a gate, and the one
  CI's `coverage` job runs, over the per-file line floors in
  `devtools/coverage-floors.txt`. What decides a pass is the gate, that check,
  and what CI's `examples` job runs: `devtools/run-examples.sh`
  and the benchmarks' untimed smoke run.
- **Two runtime dependencies**, `crossbeam-deque` and `crossbeam-utils`, and
  nothing else in the default build. "Dependency policy", below, is the rule
  that keeps it there.
- **No `unsafe` outside two modules**, `src/executor/notifier.rs` and
  `src/executor/scope.rs`. The scheduler itself has none;
  [`docs/porting.md`](docs/porting.md) says what each of the two buys.

## What is not ported

- `cuda/` and `3rd-party/`, out of scope from the start.
- Deferred pipelines: `pipeline.hpp` at the pin has no `defer`, and
  upstream's `test_deferred_*.cpp` files are not in its own test
  `CMakeLists`.
- `NonpreemptiveRuntime` as a *nameable* type -- the node kind itself is
  ported and callable. `FlowBuilder::emplace_nonpreemptive_runtime` is a
  public method, a pipeline's line task dispatches through the same node
  kind, and a body closure is handed the restricted handle with its
  parameter's type inferred, so it can `schedule` a task of its own graph
  through one. What no public path spells is the type: it lives in the
  crate-private `executor::runtime` module, so it is neither a root
  re-export nor a name a caller can write in an annotation, a field or a
  signature of its own. The handle is restricted because a node with no
  preemption protocol has no way to resume itself if its body spawned nested
  work, and a name is the one thing a body never needs in order to use one;
  [`DESIGN.md`](DESIGN.md)'s entry on the eleventh `Handle` variant has the
  decision.
- `TFProfManager`. A library has no business installing a process-global
  singleton whose destructor writes a file to a path an environment variable
  names; [`DESIGN.md`](DESIGN.md)'s entry on `TFProfManager` has the
  decision, and `TfProfObserver` is what is kept.
- The serializer and the rest of upstream's `utility/` directory that `std`
  already covers.
- Three of upstream's twenty-five benchmarks, where what they measure has no
  counterpart here: `hetero_traversal`, which is the CUDA one; `mnist`, which
  wants a dataset downloaded from Kaggle and an Eigen DNN out of
  `3rd-party/`; and `thread_pool`, which compares `tf::Executor` against a
  hand-rolled C++ thread-pool singleton over a shape
  `embarrassing_parallelism` already covers.
  [`BENCHMARKS.md`](BENCHMARKS.md)'s header and
  [`DESIGN.md`](DESIGN.md)'s "Benchmarks and performance" have the reasons in
  full; the other twenty-two are ported.

Where the port's API differs from upstream's in shape rather than in
coverage, [`DESIGN.md`](DESIGN.md) has one entry per decision, grouped by
the part of the port it decides (graph and flow builder, executor core,
control flow, dynamic tasking, the async family, observers, algorithms,
pipelines, documentation and tooling) and each naming the issue that took
it; [`docs/api-friction.md`](docs/api-friction.md) is
the log of what writing the examples against that API found awkward, and
what became of each item.

## Performance

Every measured number this port has is in [`BENCHMARKS.md`](BENCHMARKS.md):
**twenty-two of upstream's twenty-five benchmarks**, ported to criterion with
a rayon baseline in the role TBB plays upstream, 232 ids in all, each table
beside the machine and the load it ran under, plus the per-decision
measurements behind [`DESIGN.md`](DESIGN.md). Thirteen of the shapes are the
benchmarks milestone's own -- `for_each`, `reduce_sum`, `scan`, `primes`,
`mandelbrot`, `matrix_multiplication`, `black_scholes`, `wavefront`,
`graph_pipeline`, `skynet`, `nqueens`, `merge_sort` and `integrate`, with
upstream's `data_pipeline` shape joining the per-line slot measurement in one
file -- and four of those are nested recursions whose published sizes are
bounded by **the worker's stack rather than the clock**, a ceiling
`TaskGroup::corun` and `Runtime::corun` each name where a caller writes the
wait. Three benchmarks are declined, listed under "What is not ported" above.

Two more of the milestone's measurements overturned what this repo had
assumed, which is the use a measurement is for. "The #342 decision" counted the
preempted-parent re-queue the port declines to optimise and found the suite
performs it twelve times in all, every one of them in `pipeline` and none in
any nested shape, so no size and no gap here turns on it. "The #343
attribution" took the `for_each` gap apart and found the per-chunk cost to be
15 ns and 1.3% of the run, this port's chunk loop rayon's equal, and what is
left a fan-out that reaches only 15 to 19 of 24 workers -- which is the open
lead, filed as an issue of its own rather than answered there. The
continuation optimisation that halved a chain's cost is measured there too,
and [`docs/porting.md`](docs/porting.md) has the summary of both.

A **scheduler performance** milestone ran after that one, on the worst thing
those tables said: five nested shapes -- `skynet`, `nqueens`, `integrate`,
`fibonacci` and `merge_sort` -- were *slower* at 24 workers than at one.
**They are not any more.** A task group opened on a `Runtime` now puts its
children on the submitting worker's own queue and waits on that end, and a
`corun` re-reads what it waits for after every failed steal rather than once a
211-attempt search, which is what upstream's own loop does. Each of the five is
now faster at 24 workers than at one -- 1.7x to 3.4x on the executor-opened
route and 2.2x to 4.9x on the runtime one -- and three of them pass rayon at 24
workers where none did. The milestone also built **upstream's own benchmark
binaries** and ran them on this box, which is what settles whose regression it
was: upstream's executor gains from the width on every shape and size it can be
asked for, so the fault was this port's and not Taskflow's design. The
per-task constant moved too, with one of the three heap blocks a free async
node used to be: an externally submitted free async task is **~350 ns** against
the 438 to 478 first attributed to it, and the same task submitted from inside a
worker through a group **~150 ns** against 274.

**The one result still tracked as bad is external submission.** A graph
submitted from outside the pool reaches it only by stealing, so a graph of
nothing but independent sources runs several times slower than rayon on the
same box -- about 100 ns a task, most of it the producer's. The scheduler
milestone looked at it again and left it: what would fix it is a splittable
source range, which for a loop is `for_each_index` with a partitioner and which
a caller already has, and for a graph of independent sources is a different
design for what a submission is. It is the documented cost of this one, not an
open defect.

Benchmarks are deliberately outside the gate, since "how fast" is not a pass
or a fail; what CI holds the suite to is that it still compiles and still
runs, the `examples` job executing every benchmark id once, untimed, in
criterion's `--test` mode.

## Porting notes

[`docs/porting.md`](docs/porting.md) is the record of how the port was
built: the seven steps, and the two consolidations and the parity pass that
punctuate them, what each covers and what it left out, and where the two
`unsafe` modules came from. A **benchmarks and performance** milestone ran
after all of those, taking the suite from eight of upstream's shapes to
twenty-two, taking three measurements that each corrected something this repo
had asserted, and finding 24 of its 196 ids timing their own input setup. Its
tables are in [`BENCHMARKS.md`](BENCHMARKS.md), and the decisions it took
about the suite itself are in [`DESIGN.md`](DESIGN.md)'s "Benchmarks and
performance" section -- the two it took about the scheduler being filed under
the part of the port they decide, as every entry there is. A **scheduler
performance** milestone ran after it, on what those tables found, and is the
fifth with no step number; "Performance" above is what it changed and
`docs/porting.md` has its paragraph. A **test hardening** milestone is the
sixth and ran last, and it is the one that changed no production line of
`src/`: it added the coverage and Miri lanes named above and mutation-tested
the executor core, finding no bug in the port and a run of defects in the tests
and instruments themselves.
[`docs/development.md`](docs/development.md)'s "What the suite is held to" is
the measured state it leaves. The three rules
below are the ones a contributor needs before touching anything.

### Upstream pin

This port follows **Taskflow v4.1.0**, tag `v4.1.0`, commit
`45366fe5bc4f2f8ec9aa590b40c504e296886865` (2026-06-19). Every "Translated
from" line, every ported unit test and every benchmark comparison refers to
that tree. Moving the pin is a deliberate, repo-wide change, not something
one PR does on the side.

The v4 line is what we port, not v3: it is C++20, splits `NodeBase` out of
`Node`, schedules in bulk into the work-stealing queues, defaults to the
nonblocking notifier, and adds `TaskGroup` and adopted module tasks. Nothing
from `cuda/` or `3rd-party/` is in scope.

### Dependency policy

Upstream's library core depends on nothing but the C++ standard library; its
`3rd-party/` directory serves only tests, benchmarks and the profiler. We
keep the same split:

- A **runtime** dependency must replace something upstream hand-wrote *and*
  be better tested than our own port of it would be. Today that admits
  `crossbeam-deque` (the Chase-Lev deque behind `wsq.hpp`) and
  `crossbeam-utils` (cache-line padding and backoff), and nothing else; the
  only other crate in the tree is `crossbeam-epoch`, which `crossbeam-deque`
  pulls in itself. Anything optional goes behind a Cargo feature so the
  default build stays at those two.
- **Dev** dependencies need only be useful: `loom` for the lock-free parts
  and for the cross-thread rules a machine-checked model states better than
  a paragraph (nine models under `tests/loom_*.rs`, each named in
  [`docs/development.md`](docs/development.md)), `criterion` for the ported
  benchmarks, `rayon` as the baseline those benchmarks compare against, in
  the role TBB plays upstream, and `proptest` for the generated inputs
  `tests/properties.rs` checks the algorithms against `std`'s own answer
  over.

### Provenance and attribution

This crate is a Rust reimplementation of Taskflow, not a binding. Both
projects are MIT licensed, so there is no compatibility question, only an
attribution one, and MIT answers it: the copyright notice *"shall be
included in all copies or substantial portions of the Software."*

- [`LICENSE`](LICENSE) is this port's own copyright, covering the Rust code.
- [`LICENSE-TASKFLOW`](LICENSE-TASKFLOW) is upstream's notice, byte for
  byte, covering the design and any closely translated portions. It is kept
  as a **separate** file on purpose: so that it stays unambiguous whose
  copyright covers what, and so its non-standard `TASKFLOW MIT LICENSE`
  header does not confuse license detection on this repo's own `LICENSE`.

**The per-module rule:** any module translated closely from upstream names
its source in its `//!` doc comment, on the first line:

```rust
//! Translated from `taskflow/core/executor.hpp` (Taskflow v4.1.0).
```

The version is part of the line. Upstream rewrites its scheduler between
major versions, so "translated from `executor.hpp`" without saying which one
is not an auditable claim. Blanket attribution in a LICENSE file is the
legal minimum and tells you nothing useful a year later; the per-module line
records which modules were *translated* and which were *invented*, which is
exactly the distinction that gets forgotten first. If a module is your own
design that merely implements the same concept, say nothing; the absence is
then meaningful.

One thing not covered by any of the above: upstream's `3rd-party/` directory
carries its own separate terms. Nothing ported out of there may ride on
`LICENSE-TASKFLOW`; treat it as a fresh licensing question.

## License

MIT, for both the port ([`LICENSE`](LICENSE)) and the design it follows
([`LICENSE-TASKFLOW`](LICENSE-TASKFLOW)).

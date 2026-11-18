# API friction, collected from M9's example PRs

This is the friction log issue #201 asks for: every "Friction" section from
the eight example PRs that ported `examples/` (#190, #194–#200), collected
into one document, one entry per **distinct** item, grouped by the module it
touches. It is the input to the consolidation milestone that follows M9 —
nothing here is fixed in this document, and nothing here is a complaint
report: a few entries below are findings that nothing is wrong at all, kept
because they bound what the problem actually is.

Each entry states what upstream writes, what the port makes the user write,
the Rust reason for the difference when there is one, and either a proposed
change or "keep, because …". **Status** says where the entry already stands:
*open* (nothing has decided it yet), *keep* (a divergence or a cost this port
accepts, with the reason), or *tracked by #NNN* (an issue already owns it).

A note on sourcing: the themes digest prepared ahead of this document
mis-cited three items to the wrong PR, and double-cited a fourth to a PR
whose own Friction section does not mention it. Both are corrected below,
each at the entry it affects, because an uncorrected citation is worse than
no citation at all for a reader checking the source.

## Prelude

### What is the prelude for?

Three independent examples hit a gap in `rustflow::prelude`, each a
different kind of name:

- `rustflow::Xorshift32`, needed by `switch_case.rs` to stand in for
  upstream's `rand()` (PR #232, closing #195 — the digest attributes this to
  PR #239, which is about the observer example and never mentions
  `Xorshift32`; the corpus supports only #232).
- `Executor` itself, which every example needs and none of the two batches
  that tried first (#190, #194) found in the prelude, alongside `RunError`
  (#194) and `TaskMut`/`TaskRef`/`TaskType` (#194), also absent.
- The per-algorithm `*Builder` extension traits, which are in the prelude —
  but only because the orchestrator required it mid-milestone after one PR
  (#171) came close to shipping `ForEachIndexBuilder` without its prelude
  re-export, which would have made `for_each_index` uncallable through
  `use rustflow::prelude::*;` alone.

Three agents reaching the same gap independently, plus a near-miss that
needed a human to catch it, is one signal rather than three: the prelude's
membership today is accidental, built by "this trait needs to be callable"
rather than by any stated rule. Counter-evidence worth keeping beside it —
`Subflow` and `Runtime` *are* in the prelude and read fine (PR #233, closing
#196) — so the fix is not "add everything public"; it's deciding what the
prelude is actually for. The two readings that would answer differently:
*only the builder traits a `use rustflow::prelude::*;` needs to make
`FlowBuilder`'s algorithm methods callable*, which is what is there today by
accident, or *the names a caller writes on the first page of a program*,
which would add `Executor`, `RunError` and the task handle types too.

- **Status: tracked by #256, and settled there.** The *first page* reading
  won: the prelude is the names a program spells to build a taskflow, submit
  it to an executor and read the run's outcome, which added `Executor`,
  `RunHandle`, `RunError`, `Canceller`, `TaskMut`, `TaskRef`, `TaskType` and
  `Node` to it and took nothing out. The corpus decided it -- all 44 examples
  and 33 of the 34 prelude-using test files write `Executor` in an import of
  their own. `Xorshift32`, the third gap above, is answered the other way: it
  is a utility a program reaches for rather than a first-page name, so it
  stays a crate-root name and is imported by name. The eight `*Builder`
  traits stay in the prelude under the same rule, as the traits whose methods
  a first-page program calls; whether they stay *eight* is #216's. The rule
  itself is in `rustflow::prelude`'s own doc comment and in `DESIGN.md`'s
  "Decisions taken in M10".
- **Reported in:** PR #225 (#190), PR #232 (#195), PR #233 (#196, counter-evidence), PR #235 (#194).

## FlowBuilder and task-building ergonomics

### `emplace_n` only emplaces statics

Upstream's `if_else.cpp`, `nested_if_else.cpp`, `switch_case.cpp` and
`while_loop.cpp` each destructure one `auto [a, b, c, ...] = taskflow.emplace(...)`
call whose closures mix static and condition-task signatures, picked apart by
upstream's overloaded `emplace`. `FlowBuilder::emplace_n` requires every
element to be `FnMut()` (the `Callables` bound), so all four examples needed
one `emplace`/`emplace_condition` call per task instead of upstream's single
destructuring statement — the one place the port reads noticeably more
verbose than upstream line for line.

- **Why:** Rust has no variadics, and the per-kind `emplace_*` methods make a
  mixed-arity `emplace_n` family (one trait per possible arity-and-kind
  combination) an awkward thing to write for a single convenience method.
- **Status: keep, decided by #260.** The PR's own read ("probably the right
  design") is the answer: no mixed-kind `emplace!`. Dispatching on a
  closure's *return* type is not available at all — two blanket impls keyed
  on `FnMut()` and `FnMut() -> i32` are E0119 under coherence — so a macro
  would have to be told each element's kind syntactically, which is what the
  per-kind methods already are, and `DESIGN.md`'s M10 entry has what the four
  call sites would actually have saved (less than the count suggests: most of
  those closures need a block each to clone an `Arc` in).
- **Reported in:** PR #232 (#195).

### `composed_of` returns a bare `Task`, not a chainable handle

Upstream's `f2.composed_of(f1).name("module_of_f1");` chains the name onto
the same statement, because `composed_of` there returns `tf::Task` with its
own setter. The port's `composed_of` returns the same index type every other
`emplace_*` does, so naming a module task is always its own follow-up
statement: `let m = taskflow.composed_of(target); taskflow.task(m).name(...);`.
Reported independently, by two different example batches, as the one place a
module-composing example reads flatter than upstream everywhere it composes
*and* names a task.

- **Why:** the same reason naming any task is two statements rather than one
  (`DESIGN.md`'s entry on it) — `emplace` returning a borrow instead of a
  value would leave the taskflow borrowed across every later statement.
  `composed_of` is not a special case of that rule; it is the same rule,
  independently rediscovered by two examples that happened to compose *and*
  name in the same breath.
- **Status: keep, decided by #260.** Two independent reports of one shape is
  still one rule rather than two: a chainable `composed_of` would have to hand
  back a `&mut` borrow of the builder, and then the handle would hold the
  taskflow borrowed until it died — exactly the argument the "Naming a task is
  two statements" entry in `DESIGN.md` already makes for `emplace`. The M10
  entry there records it.
- **Reported in:** PR #235 (#194), PR #252 (#199, confirming — "same as
  #190's examples already reported for other module tasks").

### A single dependency is always `[&task]`, never a bare reference

Upstream overloads `dependent_async`/`silent_dependent_async` over a
variadic-plus-iterator pack, so `silent_dependent_async(f, A)` passes one
task directly. The port's single `IntoIterator<Item = &AsyncTask>` form means
every call site wraps even a single dependency in a one-element array
literal. Harmless once the signature is known, but the first attempt without
brackets fails with a trait-bound error naming `IntoIterator` rather than
`AsyncTask`, which took a moment to place.

- **Why:** one uniform signature over "a dependency or several" beats a
  second overload for the one-element case, and Rust has no variadic pack to
  give upstream's overload set a literal counterpart.
- **Status: keep, because** the uniform signature is the right call; the
  friction is a diagnostic-quality issue (the error names the wrong trait)
  rather than an API-shape one. No change proposed to the signature itself.
- **Reported in:** PR #236 (#197).

### A closure that must be `FnMut + Send + 'static` forces an `Arc`-wrapped clone

Every subflow/runtime closure is `FnMut + Send + 'static`, so anything
upstream captures by `&`/`&mut` has to become an `Arc`-wrapped value cloned
into each closure (PR #233: `cap` in `subflow.rs`, `res1`/`res2` in
`fibonacci.rs`, four of six files touched). The same root cause shows up from
the owned-data algorithm side too: `emplace_runtime` wanting `FnMut`, not
`FnOnce`, means `let mut local = owned_value;` inside its body does not
compile — moving a captured upvar out of an `FnMut` closure's `&mut self`
receiver is rejected outright rather than silently downgraded — so the fix is
always `.clone()` (PR #241: `for_loop.rs`'s `total_body`/`grid_body`,
`parallel_sort.rs`'s `strings`; three examples hit this the same way).

- **Why:** a condition task's body can run many times per run, so the
  scheduler invokes it `FnMut`, not `FnOnce` — this is the right bound for a
  body that may be revisited by a cycle, not a tuning gap.
- **Status: keep, and the doc note is in as of #260.** The bound is correct.
  `AlgorithmBuilder::for_each`'s own doc comment already said so for the one
  wrapper that owns `f`/`part` — it was `ForEachBuilder`'s until #216
  collapsed the eight extension traits into one — and the gap this entry
  names, that nothing said it at the bodies themselves, is closed:
  `src/flow_builder.rs`'s `# Design` section says what the bound asks of a
  captured value (the `Arc`-wrapped clone, and E0507 on moving an owned
  capture out of an `FnMut` body), with `FlowBuilder::emplace_subflow` and
  `FlowBuilder::emplace_runtime` pointing at it in half a line each.
- **Reported in:** PR #233 (#196), PR #241 (#198).

### `clippy::ptr_arg` forces a closure literal over a named `fn` for a `String` `bop`/`uop`

Every scan/reduce `bop`/`uop` in this crate is `Fn(&T, &T) -> T`, so for
`T = String` the bound is exactly `Fn(&String, &String) -> String` — the one
signature `clippy::ptr_arg` always wants rewritten to `&str`. A
`fn(&str, &str) -> String` item does not implement
`Fn(&String, &String) -> String` (no coercion through a generic `Fn` bound),
so the fix is a closure literal rather than a named `fn` — `ptr_arg` does not
fire on closures, only on `fn` items.

- **Why:** this is a lint-shaped trap rather than an API gap — the crate's
  own bound is correct, and the workaround is ordinary Rust.
- **Status: keep, and the doc note is in as of #260.** Nothing in the API
  needs to change. `src/algorithm/scan.rs`'s `# Design` section carries the
  note and `src/algorithm/reduce.rs` points at it, with the one refinement
  checking the lint showed: `ptr_arg` fires on the `fn` item once its *body*
  shows the parameter would do as `&str`, which a concatenation's body — clone
  the left operand, push the right onto it — does.
- **Reported in:** PR #241 (#198).

## Task introspection and subflows

### `for_each_subflow_task`'s `TaskRef` cannot resolve a second index from inside its own visitor

`for_each_subflow_task` hands a visitor a `TaskRef` scoped to a lock on the
subgraph the call itself holds. Calling `TaskRef::for_each_successor` or
`for_each_predecessor` from inside that visitor gets a bare `Task` index
back, as every such visitor does — but turning that index into a name
requires calling `for_each_subflow_task` again, and doing so from inside the
first call's own visitor locks the same `Mutex` on the same thread, which
hangs rather than panics. The workaround is two passes: one call builds a
`HashMap<Task, String>` of the subflow's own task names, a second resolves
against it.

- **Why:** a subflow's subgraph lives in a `Mutex<Graph>` on the node
  (`DESIGN.md`'s entry on it), so a nested call from inside the body that
  already holds the lock is a self-deadlock, not a borrow error the type
  system catches.
- **Status: keep, and the doc note is in as of #260.**
  `TaskRef::for_each_subflow_task`'s own docs now name the trap: the walk
  holds the subgraph's `Mutex` for its whole length, so calling the method
  again *for the same subflow task* from inside its own visitor re-enters a
  non-reentrant lock on the same thread and hangs rather than panicking, and
  the two-pass shape is the answer. Two different subflow tasks are two locks
  and nest safely, which the note also says.
- **Reported in:** PR #235 (#194).

### `attach_data`'s premise does not survive the port unchanged

Upstream's task body reads `A.data()` back through a captured `tf::Task`
that still aliases the live graph while the body runs. A task body here is
`'static` and owns nothing of the taskflow holding it, so it cannot look
itself up mid-run. `examples/attach_data.rs` sets the value with
`TaskMut::set_data` before the run and reads it back with `TaskRef::data`
after `wait()` returns instead of inside the body — every method's own
behaviour is still on display, but who reads the value and when has to
change.

- **Status: keep, because** this is the Rust ownership model forcing a
  different program shape, not a smaller substitute for upstream's. Recorded
  as a decision in `DESIGN.md`'s "Decisions taken in M9" entry on
  `Task::data` (#191).
- **Reported in:** PR #235 (#194).

### `fibonacci` is `TaskGroup` upstream, not a subflow

Upstream's `fibonacci.cpp` recurses through `tf::TaskGroup`, with no taskflow
graph at all; the port's example instead demonstrates the subflow surface.
The two are different enough designs that "translated from" needed a
paragraph in the module doc rather than the one-line comment the issue
originally anticipated for a difference this size.

- **Status: keep, because** this is an honest divergence in what the example
  demonstrates, not a smaller or incorrect port of upstream's. No change
  proposed.
- **Reported in:** PR #233 (#196).

## Accessors upstream has and the port does not

### No `Runtime::executor()`

There is no way to ask a `Runtime` for the executor it runs on, only
`Runtime::num_workers()`; a body that genuinely wants the executor (to open a
`TaskGroup`, say) has nothing to call. This is the live candidate issue #196
flagged in advance, and it came up exactly as expected in `runtime_async.rs`.

The themes digest prepared ahead of this document cites this twice, against
PR #233 and PR #241; the corpus text for PR #241 does not mention
`Runtime::executor()` anywhere, so only the PR #233 citation is substantiated
here.

- **Status: keep, because — deliberate, tracked by #202.** `Runtime` holds
  `Arc<Shared>` and no `&Executor`, for the reason `DESIGN.md`'s entry on it
  gives; a body that needs the executor captures an `Arc<Executor>` instead.
- **Reported in:** PR #233 (#196).

### `Worker` has no `thread()` accessor

`worker_interface.rs` cannot reproduce upstream's CPU-affinity demonstration
at all — not a smaller substitute, a different example. `src/executor/worker.rs`'s
module docs explain the `Worker`/`WorkerThread` split and why `Worker` cannot
lend out a thread handle; this was simply the first example that wanted to
use one.

- **Status: keep, decided by #260.** No `thread_id()` either, and the
  module-doc explanation is the permanent answer — with the half it was
  missing now written down, since it explained where the thread handle lives
  rather than why no read-only substitute is coming. `DESIGN.md`'s M10 entry
  has the argument: a `ThreadId` cannot set affinity, which is what upstream's
  `Worker::thread()` is for, and every path that hands out a `&Worker` or a
  `WorkerView` hands out the worker of the *calling* thread, so the accessor
  would return `std::thread::current().id()` by construction.
  `tests/workers.rs` pins that premise.
- **Reported in:** PR #233 (#196).

### A free async node cannot be named

Upstream's `executor.async("async1", [](){...})` and
`.silent_async("async2", ...)` give the submitted task a name a printed graph
or a profiler trace can use later. `Executor::spawn`/`silent_async` here take
only the body — there is no parameter to put a name into, not even one to
ignore.

- **Status: tracked by #258, and done there.** Every submitting form of the
  family -- eight each on `Executor`, `Runtime` and `TaskGroup` -- gained a
  `_named` twin that takes the name first and the body after it, so
  `executor.spawn_named("answer", || 6 * 7)` is upstream's
  `executor.async("answer", ...)` and an observer reads the name back as
  `TaskView::name` in `on_entry`. A `_named` suffix rather than an
  `Executor::named(..)` submission builder, and `DESIGN.md`'s M10 entry has
  why; an unnamed submission is unchanged, naming its node the empty string
  upstream's `DefaultTaskParams` leaves it with.
- **Reported in:** PR #236 (#197).

## Error handling and run lifecycle

### `RunError` is `#[non_exhaustive]`

Both `exception.rs` and `subflow_exception.rs` match on
`Err(RunError::Panicked(payload))` and need a trailing wildcard arm even
though `Panicked` is today the type's only variant — correct, given the
type's own docs say later milestones will add more, but a first-time reader
hits `E0004` before learning why, since the non-exhaustiveness is not visible
at the call site that fails.

- **Status: keep, recorded in `DESIGN.md` by #260.** The attribute is
  correct for a type that will grow variants. No change proposed to the type;
  the friction is purely "a reader meets the consequence before the reason",
  and the cost is one match arm.
- **Reported in:** PR #235 (#194).

### `RunHandle::drop` blocks, where upstream's discarded future does not

`limited_concurrency.cpp` is `executor.run(taskflow);` (fire and forget)
followed by a separate `executor.wait_for_all();`. Writing that literally
here still compiles, but reads as a bug: dropping the `RunHandle` from
`executor.run(&taskflow)` already blocks until that run is over, by design,
so the two-call shape no longer buys anything and a reader has to already
know the drop blocks to see it is not a race.

- **Status: keep, recorded in `DESIGN.md` by #260.** The blocking drop is
  the documented design. No change proposed; the example itself resolved it
  with `executor.run(&taskflow).wait().expect(...)` plus a comment at the
  spot, which is the port's spelling of upstream's two calls.
- **Reported in:** PR #236 (#197).

## Tooling and lint interactions

### An intra-doc link needs its target named in code, not just in prose

Writing a module doc with `` [`TaskGroup::corun`] `` and importing
`TaskGroup` solely to make the link resolve gets the import flagged
`unused_imports` under `clippy --all-targets -D warnings`, even though
`cargo doc` itself never touches `examples/*.rs` (confirmed by a clean
`cargo doc` run with the import still present) — nothing in the example ever
names the type directly, since the group's type is inferred from
`executor.task_group()?`. The fix was to drop the import and write the name
in plain backticks instead of an intra-doc link.

- **Status: keep, and the convention note is in as of #260.** clippy is
  correct — the import genuinely has no runtime use in an example, where it
  would have one in `src/`. The note is a bullet in `.claude/agents/rust.md`'s
  "Conventions" section rather than in this document, since it is a rule for
  writing the next example rather than a finding about the API: in `examples/`
  and `tests/`, a type the code never names goes in backticks, `cargo doc`
  not building those targets in the first place.
- **Reported in:** PR #236 (#197).

### `&str` from a local array needs an explicit copy to satisfy a `'static` closure bound

`let names = ["A", "B", ...]; names.iter().map(|name| { ... move || { ...
println!("{name}"); } })` fails to borrow-check: `name: &&'static str`
borrows `names`, and the `move` closure needs `'static`. The fix is
`|&name|`, copying the `&'static str` out of the iterator item instead of
holding a reference to the array — easy once the error is seen, but the
message ("`names` does not live long enough") does not point at the
array-literal-vs-`'static` distinction directly.

- **Status:** informational. This is an ordinary Rust ownership rule, not a
  port API gap; no change proposed.
- **Reported in:** PR #236 (#197).

## Observers

### `Executor::make_observer` takes an already-built value, not a variadic constructor call

Upstream's `make_observer<MyObserver>("MyObserver")` constructs the observer
in place; the port's split (build the value, then hand it to
`make_observer`) is a clean design once the reason for it is read, but it
does mean `observer.rs` needs a second statement — `MyObserver::new(..)`
before `make_observer(..)` — where upstream had one.

- **Status: keep, because** a documented design note already exists for the
  split. Recorded here because, per the PR, every future translated example
  touching an observer or similar factory will hit the same second call.
- **Reported in:** PR #239 (#200).

## Algorithms (step 6): two forms where upstream has one

This is the biggest API-shape finding in the corpus, and the one the
issue's own brief asked to probe directly: every M7 algorithm has a
slice-borrowing `Runtime::` method *and* an owned-data `FlowBuilder::`
wrapper, where upstream has one shape. Five of the seven algorithm examples
run both forms side by side on the same algorithm.

- **The owned-data wrapper reads closest to upstream when the caller already
  owns an `Arc`-friendly value, and noticeably worse otherwise.**
  `taskflow.sort(Arc::clone(&data))` reads almost exactly like upstream's
  `taskflow.sort(beg, end)`, the only ceremony being one `Arc::new(Mutex::new(..))`
  up front. That stops being free the moment the caller's data is not
  theirs alone: `for_loop.rs`'s stateful-range case has no port-side
  equivalent to upstream's `std::ref(range)`, because the wrapper's shape is
  always `Arc<Mutex<_>>` with no reference-wrapper vocabulary standing in for
  it. `Runtime::for_each_by_index_nd`'s own `# Design` section already names
  this gap from the implementation side; this is the same gap felt
  independently from the call-site side.
- **The `Runtime::` methods read better than upstream once inside a runtime
  body already, and that half has no upstream shape to be unfaithful to.**
  `runtime.sort_by(&mut local, cmp)` on a `&mut [T]` the body already owns is
  genuinely nicer than anything upstream can write — upstream's `tf::Runtime`
  has no loop/sort/reduce/scan methods at all, so there is no 1:1 line to
  translate against.
- **Neither form reaches a dependent-async task body — a third, uncovered
  case.** `ForEachBuilder`/`ElementwiseBuilder`/`ReduceBuilder` all build
  through `FlowBuilder::emplace_runtime`, and a dependent-async task
  submitted through `Executor::silent_dependent_async_runtime` is never
  queued into any `Taskflow` for that to call. `dependent_async_algorithm.rs`
  drops straight to the `Runtime::` methods and hand-locks its own
  `Arc<Mutex<Vec<i32>>>` at each of its three stages — exactly what the
  owned-data wrapper would have done, with none of its convenience, since
  the wrapper is unreachable here. Upstream's own version needs no locks at
  all: its three `tf::make_*_task` factories alias the same `std::vector`
  across all three stages, trusted entirely to the dependency edges. This is
  the widest gap the batch found — not between the port's two existing
  forms, but a third case neither covers.
- **`Runtime::transform` always writes into a second slice, where upstream
  transforms in place.** `transform(input: &[A], out: &mut [B], f, part)`
  cannot alias `input` and `out` (an overlapping `&[T]`/`&mut [T]` does not
  borrow-check), so the doubled-value case writes into a second `Vec`.
  `src/algorithm/transform.rs`'s own `# Design` section already gives the
  reason; this is the same constraint confirmed from the call-site side, on
  the simpler one-input case.

- **Status: tracked by #257, and settled there.** The answer to "one shape or
  two" is *three*, because upstream has two of them as well. Reading
  `taskflow/algorithm/*.hpp` at the pin, every `FlowBuilder` algorithm method
  there is one derived line -- `return emplace(make_X_task(args...));` -- over
  a `make_*_task` factory that is upstream's actual primitive, while
  `tf::Runtime` has no algorithm members at all. That is this entry's own
  second bullet read from upstream's side: the `Runtime::` form is the port's
  invention, not an unfaithful translation of anything. Both of the port's
  existing shapes stay, and #257's call-site counts are why neither is the
  redundant one: `find` runs 15 `Runtime::` calls to 0 wrapper calls outside
  `src/`, `scan` runs 2 to 14, and `tests/scan.rs` is wrapper-only. What gets
  completed is the middle layer, the `'static` callable a `FlowBuilder`
  wrapper, an async task *and* a dependent-async task can all accept: 17 new
  factories, one per wrapper, filed as #292, #293, #294 and #295, with #296
  rewriting `dependent_async_algorithm.rs` on them. That closes this entry's
  third bullet, and corrects the reason the example gives for the gap -- a
  `Runtime::scope` underneath an algorithm does not stop a factory from
  handing back a cold callable, and `make_inclusive_scan_task` is the proof
  already in the tree. The first bullet's `Arc<Mutex<_>>` cost is accepted
  rather than fixed, since there is no safe counterpart to `std::ref(range)`,
  whose soundness upstream rests on the programmer's claim about its
  dependency edges rather than on anything a compiler checks; #257 records the
  ownership-shape rule the 17 factories follow so the family reads as one
  vocabulary. The in-place `transform` constraint in the fourth bullet is
  unchanged by all of it.
- **Outcome: the ownership rule's read side is a sealed trait, so the
  factories chain** (#318, found while writing #296). Completing the family
  exposed a second edge in the rule itself: every producing factory writes
  into an `Arc<Mutex<Vec<T>>>` while every consuming one read an
  `Arc<Vec<T>>`, and the two do not convert without copying -- so the longest
  chain the family could express was for-each/sort into for-each/sort, and no
  factory could end a chain by reducing what an earlier stage wrote. The
  three-stage program this entry's third bullet asked for therefore still did
  not compile: stage A did, stages B and C failed with `expected
  &Arc<Vec<_>>, found &Arc<Mutex<Vec<i32>>>`. The fix is one sealed
  `SliceSource` trait -- implemented for `Arc<Vec<T>>`,
  `Arc<Mutex<Vec<T>>>` and an owned `Vec<T>` -- taken by all fifteen
  read-only parameters of the eleven factories that have one, in argument
  position so every existing call site and all 28 wrapper signatures are
  untouched. What it costs is a discipline rather than a copy, and
  `DESIGN.md`'s M10 entry states it: guards in parameter order, none held
  past the body's return, and two stages over one lock need an edge between
  them even when both only read, `Mutex` having no shared mode.
- **Reported in:** PR #241 (#198).

## Pipelines (step 7)

### The pipeline module path reads oddly, and nothing re-exports it flatter

Importing `rustflow::pipeline::scheduler::Pipeline` at a call site that also
names `rustflow::pipeline::pipe::{Pipe, PipeType, Pipeflow}` reads oddly the
first time — the type doing the scheduling is spelled
`pipeline::scheduler::Pipeline`, not the `pipeline::pipeline::Pipeline` the
obvious nesting would suggest. Once #244's `clippy::module_inception` rename
is known it makes sense, but a `pub use` re-export of `Pipeline` and
`DataPipeline` at `rustflow::pipeline::{Pipeline, DataPipeline}` would read
closer to upstream's flat `tf::` namespace, and closer to how these examples
otherwise just open `rustflow::prelude::*`.

- **Status: tracked by #256, and done there.** `src/pipeline/mod.rs` is the
  family's front door and now re-exports the flat names a pipeline program
  writes -- `Pipeline`, `DataPipeline`, `DataPipelineBuilder`, `DataPipe`,
  the four `*_pipe*` constructors, `Pipe`, `PipeType` and `Pipeflow` -- so a
  call site spells `rustflow::pipeline::Pipeline` and the module each type
  happens to live in is an implementation detail again. The root was the
  other candidate and was rejected: #256's root rule flattens a family at its
  own front door rather than at the crate root, since
  `rustflow::pipeline::Pipeline` reads as well as `rustflow::Pipeline` and
  says more.
- **Reported in:** PR #252 (#199).

### Composing before `reset_pipes`/`reset_lines` is a sharp edge a faithful translation runs straight into

Upstream's own `scalable_pipeline.cpp` composes once, runs, calls
`pl.reset(pipes.begin(), pipes.end())`, then runs the *same* composed task
expecting the new chain. The port's `reset_pipes` rebuilds a fresh
`Arc<Taskflow>` rather than mutating the one already composed, so the
already-composed task keeps running the old chain. This is documented in
`src/pipeline/scheduler.rs`'s own `# Design` section and pinned by a test
there — but translating upstream's *example* (rather than its test suite,
which already composes after resetting) is the first place this shows up as
"the upstream example program does not literally port." The example resolves
it by composing a second time into a second `Taskflow` after the reset, with
a comment at the spot.

- **Status: keep, because** the rebuild-rather-than-mutate design is a
  recorded decision (issue #185); the documentation of it is **tracked by
  #259**. Proposed change: a doc example on `Pipeline::reset_pipes` itself,
  next to its existing test, showing the compose-again pattern — the PR that
  found this suggests exactly that.
- **Reported in:** PR #252 (#199).

### `DataPipeline` has no `reset_pipes`/`reset_lines`

`Pipeline` exposes both scalable reset forms (#185); `DataPipeline` wraps a
private `Pipeline` and exposes only `reset()`. There is no way to replace a
`DataPipeline`'s chain or line count in place, so demonstrating the typed
equivalent of `scalable_pipeline` meant building a second `DataPipeline` from
scratch rather than resetting one object twice.

- **Status: tracked by #259.** Proposed change: thread
  `reset_pipes`/`reset_lines` through to the inner `Pipeline` the same way
  `reset()` already delegates.
- **Reported in:** PR #252 (#199).

### Upstream's own data-pipeline example has a live type bug the port's typed builder rejects

Upstream's `data_scalable_parallel_pipeline.cpp` appends two pipes of type
`void -> int` into slots declared `int -> float` and `float -> int`.
`DataPipelineBuilder::pipe` checks exactly this at compile time, so there is
no way to write a faithful port of that chain: any attempt either fails to
compile or silently becomes a differently-typed chain. The example instead
writes a consistently-typed five-stage chain of its own, with a comment
saying so.

- **Status: keep, because** the port's compile-time type checking is correct
  and upstream's own example is the one with the bug. No change proposed;
  the divergence is already documented at the point it occurs.
- **Reported in:** PR #252 (#199).

## Negative findings

Kept deliberately, because a log of only complaints would misrepresent the
port — these bound the problem as much as the entries above describe it:

- **No turbofish needed anywhere**, across four independent batches: every
  closure's bound inferred cleanly from the call site, including `i32::from(bool)`
  and `i32::try_from(usize)` conversions (PR #232), every
  `for<'a> FnMut(&mut Subflow<'a>)` body (PR #233), every `data::<T>()`/
  `take_data::<T>()` call (PR #235), and the five async examples (PR #236).
- **Naming reads right next to upstream's**: `emplace`/`emplace_condition`/
  `precede`/`succeed`/`.task(..).name(..)` all read as the natural
  translation of upstream's own names (PR #233).
- **No `SmallVector`-equivalent, and no friction from its absence.**
  `multi_condition.cpp` returns `tf::SmallVector<int>`; the port's
  `emplace_multi_condition` just wants `Vec<i32>`, a strictly simpler surface
  for the same job (PR #232 — the themes digest prepared ahead of this
  document mis-cites this to PR #233, whose own Friction section does not
  mention `SmallVector`; the corpus supports only #232).
- **`Subflow`/`Runtime` are already in the prelude**, and no example needed
  an explicit `use` for either (PR #233) — the counter-evidence behind the
  prelude entry above.
- **`WorkerView`/`TaskView` matched upstream's `WorkerView`/`TaskView`
  closely enough that the translation of `observer.cpp` was closer to
  mechanical than most of its siblings'** (PR #239).
- **No new friction calling `Executor::corun` from inside a pipe callable
  running on a `NonpreemptiveRuntime` line task** — it behaves exactly like
  calling it from an ordinary task's body, with no special case needed for
  the line task's restricted handle (PR #252).

## Cross-check: every example PR's Friction section against this document

| PR (closes) | Entries drawn from it |
|---|---|
| #225 (#190) | Prelude: `Executor` missing |
| #232 (#195) | Prelude: `Xorshift32` missing; `emplace_n` statics-only; negative: no `SmallVector` equivalent, no turbofish |
| #233 (#196) | Prelude: `Subflow`/`Runtime` present (counter-evidence); `FnMut`-forced clone; `fibonacci` is `TaskGroup` upstream; no `Runtime::executor()`; `Worker` has no `thread()`; negative: no turbofish, naming reads right, prelude counter-evidence |
| #235 (#194) | Prelude: `TaskMut`/`TaskRef`/`TaskType`, `Executor`/`RunError` missing; `composed_of` returns bare `Task`; `for_each_subflow_task` lock-reentrancy; `attach_data`'s premise; `RunError` non-exhaustive; negative: no turbofish |
| #236 (#197) | Single dependency needs `[&task]`; free async node cannot be named; `RunHandle::drop` blocks; intra-doc link vs. `unused_imports`; `&str`/`'static` array copy; negative: no turbofish |
| #239 (#200) | `make_observer` needs a second statement; negative: `WorkerView`/`TaskView` match closely |
| #241 (#198) | `FnMut`-forced clone (`emplace_runtime`); `clippy::ptr_arg` vs. named `fn`; two forms where upstream has one (and its three sub-findings) |
| #252 (#199) | `composed_of` returns bare `Task` (confirming #194); pipeline module path; compose-before-reset sharp edge; `DataPipeline` has no scalable reset; upstream's own example type bug; negative: no new `corun` friction |

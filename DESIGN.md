# Design

Why this port is shaped the way it is. Two kinds of thing live in this file,
and nothing else does:

- **The documentation contract** — what a doc comment in this crate says and
  what it must not say. It is stated here and summarised in
  `.claude/agents/rust.md`, because that rule set is what every PR is reviewed
  against; this file is the authority where the two read differently.
- **The design decisions already taken** — one entry per decision, each
  naming the issue that took it. This is the one place in the repo where an
  issue number belongs in prose that ships; the source comments carry none.

What is deliberately not here: the measured numbers, which are in
[`BENCHMARKS.md`](BENCHMARKS.md) beside the machine and the load they were
taken under, with the performance decisions that rest on them; the per-module
provenance lines, which belong in the modules they describe; and the status of
the work, which is the "Order of work" section of
[`docs/porting.md`](docs/porting.md) and the milestones behind it.

The contract below keeps a rationale in one place, and for a module that is a
dozen lines of dispatch over machinery somebody else owns -- or a handful of
value types whose own doc comments would otherwise retell a decision made
once, elsewhere -- that place cannot be the module itself. Some of the entries
are that place: #130 moved the executor periphery's rationale here first, and
the modules after it that were in the same position add to it the same way,
each naming the issue whose decision it records, as every other entry does.

A decision does not always add an entry of its own either. The first
consolidation ran 17 issues against the 11 it opened with, and the rest of its
decisions extended an entry rather than adding one. Where each is: the anchor
window and the companion walk in `enter` (#157) under "A failure inside a
nested graph is delivered to whoever is waiting for that graph"; the one-node
arena and the heap-block bill it was part of (#144) under "An async task is
one `Arc`-owned free-standing node"; the debug assertion and the loom model
that replaced a paragraph (#138, #141) under "A run's waiter is released by the
last share of its storage hold"; and a task body's own queue (#119) and naming
a task in two statements (#120) in entries of their own. Two issues
deliberately left none: #128 and #151 are the weekly loom workflow, whose own
file and the README say what it runs, and #146 was two module docs still
calling the async family a later milestone.

The entries are grouped by the part of the port they decide rather than by the
milestone that decided them; each still names its issue, which is where the
order they were taken in is recorded.

## Index

- [Documentation contract](#documentation-contract) -- what a doc comment says
- [Graph and flow builder](#graph-and-flow-builder) -- the arena and the builder
- [Executor core](#executor-core) -- what a run owns, and how a job is dispatched
- [Control flow and cancellation](#control-flow-and-cancellation) -- where a failure goes
- [Dynamic tasking](#dynamic-tasking) -- subflows, module tasks and task groups
- [The async family](#the-async-family) -- free nodes, handles and named submissions
- [Observers](#observers) -- the two hook seams and the profiler
- [Algorithms](#algorithms) -- the three layers and the ownership rule they share
- [Pipelines](#pipelines) -- lines, the nonpreemptive node kind and `DataPipeline`
- [Documentation and tooling](#documentation-and-tooling) -- the surface and the instruments
- [Benchmarks and performance](#benchmarks-and-performance) -- the suite and its shapes

## Documentation contract

An item doc (`///`) states **what the item is**, the **upstream name** it
translates (upstream's `Topology::_join_counter`) and any **invariant a caller
must keep** — a locking order, a lifetime, "never held across a drop" —
and nothing else. **Rationale**, meaning why this port differs from upstream
and which alternative was rejected, lives **once**: in the module's `//!` docs
under a `# Design` heading, or in this file when more than one module shares
it, and a field or type doc may point at it in half a line but does not retell
it. **History** — "before #109 this was an `Arc<Flow>`", "found by #93" —
belongs to the PR trail and the git log, so there are **zero `#NNN` citations
in the doc comments of production text**; the entries below name their issue,
and that is the one place, with the one exception the bullets below settle.
A **trait impl** (`Hash`, `Debug`, `Drop`, `Display`) gets one line on
what is non-obvious about it, or nothing. An **inline `//` comment** explains a
line a reader would otherwise question — an `Ordering`, a deliberate leak, a
lock taken out of order — rather than narrating what the line does. The
provenance `//!` line on a translated module is untouched by all of this and
stays. The target is a file's **prose** lines at or under **1.2× its code
lines**, prose being every comment line that is neither part of a doc example
nor inside a module's `# Design` section — the section running from the
`//! # Design` heading to the next top-level `//! #` heading or the end of the
`//!` block, whichever comes first, with a `//! ##` sub-heading staying inside
it (#228).

`devtools/doc-ratio.sh` measures that last part: one row per production file
(`src/**/*.rs`, counting the text above the test module) with its prose lines,
example lines, `# Design` lines, code lines, ratio and
`#NNN`-in-doc-comment count, then a tree total. It is a **report and not a
gate** — it always exits 0 — because which comments go is a reviewer's
judgement, and a ratio cannot make it. The tree
stood at **2.2× overall** when this contract was written (#126) — examples
counted as prose, as they then were — with 2.6× in the single
`executor::topology` file #131 has since split, 4.8× in
`src/executor/runtime.rs` and 210 issue citations in doc comments across 26
files. It closed the consolidation at **1.2× over 29 files** with zero
citations in production text, and the measurement of the same tree the old way
is 1.4×.

Three questions the documentation passes ran into are settled here, so that
the next pass does not answer them a fourth way (issues #129, #130, #133,
#134):

- **The 1.2× target is a guide for prose-heavy implementation files, not a
  gate, and doc examples are not prose.** The examples a public API carries
  are required by this contract, so counting them against a file's budget
  charged it for obeying the rule the ratio exists to serve — and made the
  figure arithmetically unreachable rather than merely hard, `runtime_async`'s
  frozen examples being 189 lines against a budget of 165. They are therefore
  measured in their own column, and a crate root — whose code lines are a
  `pub mod` and `pub use` list — is reported but not held to the figure at
  all. What is left after both exemptions is judgement, not arithmetic: 14 of
  the 28 files that are held to it are over, four of them by under 3% and the
  rest being modules whose rationale is the `# Design` section this contract
  puts there and nowhere else (`src/executor/runtime.rs` at 2.7×,
  `src/task_group.rs`, `src/executor/corun.rs` and
  `src/executor/dependent_async.rs` at 1.9×). Buying the number by cutting a
  `# Design` section or deleting a doctest is refused, since that trades the
  thing the contract is for against the measurement of it; a file over 1.2×
  is one a reviewer looks at, and a reason stated in its PR is an answer.
- **A `#NNN` in a *test's* doc comment is the one allowed citation.** The
  zero above is a rule about the text a reader of the public docs or of the
  production source meets. A test case whose doc says which issue set its bar
  is naming its acceptance rather than telling a story about the past, which
  is the one place that history is load-bearing — 53 of them on 48 lines in
  11 files, and the reason `doc-ratio.sh` deliberately stops at the first
  column-0 `#[cfg(test)]` instead of reading past it.
- **"The same set of test names before and after" means the same set of item
  paths.** A doctest's generated name embeds the line its example starts on
  (`src/task.rs - task::TaskMut (line 349)` today, a different number as soon
  as anything above it is reworded), so moving one paragraph renames every
  doctest below it in the file and a literal comparison of two `--list` outputs
  can never hold for a docs pass. An issue that asks for an unchanged
  test-name set is asking for the item paths with ` (line N)` stripped, and a
  PR that claims one says which it compared.

  `devtools/test-names.sh` is that comparison, so that the issues which
  promise an unchanged set do not each invent one: `snapshot <file>` writes the
  sorted item paths from `cargo test --all-features -- --list` — every test
  binary and the doctests — with ` (line N)` stripped, and
  `diff <before> <after>` prints what was removed and what was added and exits
  non-zero if either list is non-empty. It compares **multisets** rather than
  sets, because an integration test's item path does not name the binary it
  lives in — `basics` and `future` are each several different tests in this
  tree, and one item carrying two examples lists its path twice — so folding
  the duplicates would let a renamed twin cancel against its sibling and read
  as unchanged. Nothing is stored in the repo: a snapshot is of a tree its
  reader has in front of them, and a committed one would be a second copy of
  the test list to keep in step with the tests (#262).

## Graph and flow builder

- **Graph as an arena.** The upstream API hands out `tf::Task` handles that
  alias into a graph the caller still owns and mutates, which does not survive
  translation to Rust's aliasing rules. Here the taskflow owns a vector of
  nodes and a `Task` is a `Copy` index. Dependency edits go through the
  taskflow via a short-lived fluent borrow, so the graph is plain `Send +
  Sync` with no `unsafe`, and a run can hold `&Taskflow` across worker
  threads. Upstream's rule "do not mutate a taskflow while it runs" becomes a
  lifetime on the run handle — which makes the ordinary case a compile error,
  and is not on its own enough; see "A run owns the graph it walks" under
  "Executor core".
- **Naming a task is two statements, not `emplace(..).name("A")`** (issue
  #120). Upstream's `emplace` returns a `tf::Task` by value, so a name chains
  straight onto it; here `emplace` returns the `Copy` index and the fluent
  methods live on `TaskMut`, which is a `&mut` borrow of the builder. Handing
  that borrow back from `emplace` is what the chain would cost: a
  `let a = tf.emplace(..)` would then hold the taskflow borrowed until `a`
  dies, so a second `emplace` on the next line would not borrow-check, and
  every example in the crate would break. So it is `let a = tf.emplace(..);`
  and then `tf.task(a).name("a");`. The two other shapes considered are rejected for
  their own reasons: a consuming `TaskMut::name(self) -> Task` breaks the
  documented `tf.task(a).name("a").work(..).precede([b, c])` chain, and an
  `emplace_named` family needs a twin for each of the five emplace kinds plus
  `placeholder` to save one line. What the port does instead is put `task()`
  and `task_ref()` on `FlowBuilder` rather than on `Taskflow`, so that the
  two-statement form reads the same inside a subflow body as outside one.
- **Task bodies are `FnMut() + Send + 'static`.** Condition tasks create
  cycles, so a body runs many times per run and upstream invokes it mutably;
  the scheduler's one-worker-per-node guarantee is the safety argument.
  Panics take the role of upstream's exceptions: caught per task, stored in the
  topology, surfaced from `wait`.
- **Per-node runtime state stays in the node** as atomics, matching upstream's
  join counter and state words, so runs of one taskflow serialise exactly as
  they do upstream.
- **`FlowBuilder` is a trait with provided methods, not a base class**
  (issue #23). Upstream's `tf::FlowBuilder` is a base class holding a
  `Graph&`, which `tf::Taskflow` and `tf::Subflow` inherit for the
  task-building API; Rust has no inheritance for code reuse, so the trait
  asks an implementor for the one thing it cannot provide itself -- the
  graph it builds into, as `FlowBuilder::graph` and `FlowBuilder::graph_mut`
  -- and supplies everything else as a default method over that. `Runtime`
  deliberately does not implement it: the graph a runtime task runs in is
  the enclosing one, scheduled into but never edited, so it has no `task`
  method to offer.
- **`FlowBuilder::composed_of` takes anything `Composable`, not a concrete
  `Arc<Taskflow>`** (issue #181). Upstream's own `composed_of` is a template
  over its `GraphLike` concept rather than a `Taskflow` overload, because a
  pipeline composes its own graph the same way a plain taskflow does; the
  port gets that for free once the one thing `composed_of` needs -- a share
  of the target's `Taskflow` -- is asked for through a trait instead of a
  concrete type. `Composable` is implemented for `Arc<Taskflow>` so every
  caller from before the trait existed is unaffected; a pipeline's own graph
  is the other implementor.
- **`Task::data` is a typed slot, and `for_each_subflow_task` is ported
  because a retained subflow's graph survives its join** (issue #191).
  Upstream's `Node::_data` is `void* {nullptr}`, read and written through
  `Task::data()`/`data(void*)` with no check that a caller's cast matches
  what was stored. The port's slot is `Option<Box<dyn Any + Send + Sync>>`
  on `Node` instead -- `Send + Sync` because a node is reachable from any
  worker, where upstream's bare pointer needs neither -- and
  `TaskMut::set_data`, `TaskMut::data_mut` and `TaskMut::take_data` write it
  while `TaskRef::data::<T>()` reads it typed, a failed downcast answering
  `None` rather than trusting whatever bytes happen to be there. The type
  change does not rescue upstream's own premise that a task body can read
  its own attached data back through a `tf::Task` that still aliases the
  live graph while the body runs: a body here is `'static` and owns nothing
  of the taskflow holding it, so `examples/attach_data.rs` sets the value
  with `TaskMut::set_data` before a run and reads it back with
  `TaskRef::data` after `wait()` returns, rather than inside the body itself
  -- the one example in #194's batch where the ownership model forces a
  different program shape rather than a different spelling of the same one
  (`docs/api-friction.md` records it as a "keep").

  `for_each_subflow_task` is ported on `TaskRef`, not left absent: a subflow's
  subgraph lives in the node's `Mutex<Graph>` (see `src/subflow.rs` and the
  entry on leasing a subgraph under "Dynamic tasking"), and after a join that
  retained it (`Subflow::set_retain(true)`) the graph is still there for the
  method to walk, exactly as upstream's is. Its visitor hands back a `TaskRef`
  scoped to that subgraph's own lock rather than a bare `Task`, because the
  index space a subflow's tasks live in is not the composing taskflow's own --
  which is also where the trap `docs/api-friction.md` records from #194's own
  example lives: calling `for_each_subflow_task` again from inside the first
  call's own visitor, to resolve a successor's name, re-enters the same
  `Mutex` on the same thread and hangs rather than panics.

## Executor core

- **A run owns the graph it walks** (issue #40). A lifetime on the run handle
  is only as good as the handle: `mem::forget` on one ends the borrow under
  NLL with the run still in flight, and a blocking `Drop` cannot help, because
  `Drop` is not guaranteed to run. That was a use-after-free reachable from
  safe code. So a taskflow is now a name plus an `Arc` of its *storage* — the
  graph and the queue of runs — and every queued job holds a share of that
  storage and of the run it belongs to. Upstream is less careful here than
  this, not more: its nodes hold a raw `Topology*`, its topologies a
  `Taskflow&`, and its `tf::Future` only a `weak_ptr`, so nothing owns a
  taskflow on its behalf at all. Which share a waiting thread is *released by*
  took two more goes and has an entry of its own below (#109, #112).

  The consequences are the point. A leaked handle leaks rather than dangling;
  an edit of a running taskflow panics (`Arc::get_mut` finds the run's shares)
  rather than reallocating a vector workers are walking; and the executor's
  one `unsafe` boundary is gone, leaving the notifier's as the only one in the
  crate. The cost is four atomic refcount operations per task and one
  `Arc::get_mut` per graph edit, and it was measured rather than estimated:
  the figures, the method and the two ids this box could not resolve are in
  [`BENCHMARKS.md`](BENCHMARKS.md) under "The #40 decision", which is the one
  place they live, since a number copied into prose is a number that drifts.
  The worst of them is +19% on `linear_chain` at 24 workers, the shape that
  measures nothing but scheduler overhead. A real regression, and the right
  one to accept: the alternative was a safe API with a reachable
  use-after-free.
- **A run's waiter is released by the last share of its storage hold** (issues
  #109, #112). Two rules used to carry the promise that a `wait` returning
  means the taskflow is editable: a job gives up its share of the storage
  before the decrement that can end the run, and the tear-down drops the run's
  own share before releasing the waiter. The pair does not add up, because a
  node that *preempts* is dequeued as a job and that job is then copied three
  times — onto the frame inside the dispatch arm, into the parent its nested
  graph is scheduled under, and as the job the last child re-queues to resume
  it — and only the third copy is ever finished. The other two are dropped by
  frames that have moved on, and nothing orders those instructions against the
  resumed node ending the run, so a `wait` could return, and on CI did, with a
  copy still holding the storage and the caller's next `emplace` panicking.
  Eliminating the copies is not available: the join counter a child decrements
  lives in the parent node, inside what one of those copies keeps alive. So the
  release of the waiter moved instead — the run's hold on the storage is one
  allocation every job of the run's graph shares, and its `Drop` releases the
  storage, then the executor's count, then the waiter, in that order. One
  waiter cannot be released that way, and takes the same hold from the other
  side: a `corun` frame parks nowhere, so it *owns* the hold and waits for
  `Arc::get_mut` to call it unique, which is the one question no mere holder of
  a share can answer (#112). The rule the two shapes share — a thread is woken
  only by the drop of the last hold on whatever it will touch next — is since
  #138 and #141 a debug assertion and a loom model rather than a paragraph.
- **The run splits into three modules and the ownership quartet stays four**
  (issue #131). `executor::topology` was one 2,352-line file over a dozen
  types, and six of M5's seven post-merge bugs were counter or share-lifecycle
  errors in it, so it is now a module tree: `run` for `Topology`, `RunPolicy`,
  `Completion`, `Cancellable` and `set_up_graph`, `job` for `Job`, `Finished`
  and `Anchor`, and `ownership` for `GraphOwner`, `Storage`, `Inside`,
  `Parent` and `Nested`. The split is by what owns what rather than by size,
  which is why the file's one `//!` became three `# Design` sections and why
  the lock orderings -- spread over field docs before -- are one list in
  `ownership`'s.

  The quartet was the question the issue asked, and the answer is that **four
  stay four**, with one of the three candidate merges available on cost and
  two forbidden outright. `Storage` into `Inside` share only "an `Arc` with a
  `Drop`". `GraphOwner::Run` into `GraphOwner::Flow` *is* possible, an ungated
  hold behaving exactly as a bare share of the storage does, and is declined
  because every module task would then allocate a hold whose only property is
  that it gates nothing, on the hottest resolution in the crate. `Nested` into
  `Parent` is forbidden by an invariant: the state words a nested graph's jobs
  count down have to be one allocation shared by every job under that parent,
  and `Parent` is `Clone` and travels by value on every job, so an inline
  payload would hand each child a counter of its own. Nothing merged, nothing
  weakened, no behaviour changed -- the unchanged test-name set and
  `tests/loom_storage.rs` are what says so -- and `Worker::invoke_one` gained
  two named empty seams, `observer_prologue` and `observer_epilogue`, so that
  the observers of step 6 add a call rather than a reading of that function.
- **A node that parents a graph is entered twice, and the parent travels with
  the job** (issue #47). Every graph the executor schedules is scheduled
  *under* something, whose join counter the jobs of that graph decrement as
  they finish: the run, for the nodes of a taskflow's own graph, whose counter
  reaching zero ends the run; or something nested inside it — a node whose own
  graph is running, the anchor a `corun` schedules a taskflow under, a node
  whose children are async tasks, a `TaskGroup`'s anchor with no run around it
  at all — whose counter reaching zero ends nothing and instead resumes the
  node that was waiting or releases the frame watching it. Upstream stores that
  parent on the node as `node->_parent`; here it is carried by the job, so a
  parent chain is a chain of *jobs* and the parent of a node is reached as the
  job that node was dequeued as — which is exactly what has to be queued again
  to resume it.

  A node that parents a graph cannot finish when its body returns, its
  successors having to wait for that graph, so it is entered twice with
  `NSTATE::PREEMPTED` saying which entry this is. The first entry runs whatever
  the node does, sets the bit and schedules the nested graph under the node,
  then answers "preempted": the node's counter is not re-armed and no successor
  is released. The children are ordinary jobs, each counted against the node.
  The tear-down of the last of them finds the counter at zero and the bit set
  and queues the node's own job a second time — reading the bit *before* the
  decrement, because the instant that counter reaches zero another worker may
  be running the node. The second entry skips the cancellation check, an
  entered node having to come back out through the dispatch whatever happened
  meanwhile, clears the bit and answers with an ordinary release. The protocol
  costs one requeue per nested graph where upstream resumes the parent on the
  worker that finished the last child, its `_update_cache` being unported
  (#62), and all four arms that can preempt a node do: a module task (#49,
  #74), a runtime task that spawned work (#51), a subflow left to its implicit
  join (#50) and a free node whose body is handed a `Runtime` (#90).
- **A worker that waits for a nested graph does not park** (issue #16). A task
  body runs on a worker thread, and an executor's workers are a fixed pool: the
  only threads that will ever take a job out of its queues. So a task that
  calls `Executor::run` and then waits — with `RunHandle::wait`, or by letting
  the handle drop — parks one of those threads in the inner run's completion
  slot while the work it waits for sits in the pool's queues. Nothing undoes
  that: a parked thread is woken by the topology it waited on finishing, and
  that topology finishes when some *other* worker runs its last job, so the
  pool has to have a worker to spare. Four such waits on a four-worker executor
  leave none, and at one worker it takes one. That is the deadlock upstream's
  own documentation warns about, and `Executor::corun` is the answer: the
  calling worker stays in the work-stealing loop, running the inner graph's
  jobs and anybody else's until the inner run's last job has finished, so the
  pool never loses a thread to waiting and no amount of nesting can drain it.
  Three consequences a caller feels. The wait is not ordered, a corunning
  worker running whatever it can steal, so a corun returns no sooner than the
  inner run finishes and no later than the next job it picked up is done —
  a bound on one steal attempt rather than on a search, the loop re-reading
  what it waits for after every attempt that found nothing, which is #412 and
  is upstream's `_corun_until` structure. It is
  for a worker of *that* executor and nothing else, any other thread getting
  `CorunError::NotAWorker` rather than the blocking wait, which would be a
  mistake about which thread the code is on. And nesting a run on a *different*
  executor and blocking on it is still fine, two pools being unable to starve
  each other the way one starves itself. Every blocking wait on a nested graph
  in the crate is this loop: `Subflow::join` (#50), `Runtime::corun` (#51) and
  `TaskGroup::corun` (#93) as much as `Executor::corun` itself.
- **Waiting on a nested graph comes in two shapes, `corun` and `try_corun`**
  (issue #48). Upstream has one `corun`, which rethrows: a failure of the inner
  graph is raised on the calling worker, where the task body that called it can
  catch it — or not, in which case it becomes that task's own failure and the
  outer run reports it. That composition is worth keeping, so `corun` is it:
  the payload goes to `std::panic::resume_unwind`, which keeps its identity, so
  a handler two levels up still downcasts it to what the inner task panicked
  with. `try_corun` is the same call with the failure as an
  `Err(CorunError::Run(RunError::Panicked(..)))` instead, for a caller that
  means to inspect it — upstream's `try`/`catch` around the call. Both still
  report a `corun` from a non-worker thread as `CorunError::NotAWorker`, which
  is the one failure a caller can act on rather than propagate.
- **A task body's async child is queued on the worker running the body**
  (issue #119). Upstream's `_schedule_async_task` branches on whether the
  submitting thread is one of its workers, and the port takes that branch on
  whether the caller was *handed* its worker rather than by looking the thread
  up — the owning end of a worker's queue lives in the `WorkerThread` frame
  that called the body, and a frame cannot be named by the code it called. So
  `Executor::spawn` from inside a task body still goes to the shared buffers,
  and a `Runtime`'s eight forms, which are given the scheduler, push onto the
  worker's own queue. The difference is not a micro-optimisation: that queue is
  LIFO, so a body that spawns a child and then coruns gets *that child* back
  rather than the oldest job in the pool, and each nested corun that picks up
  somebody else's work adds a frame without retiring one. The port's
  `Runtime.Fibonacci` overflowed a worker's stack in under a second with the
  children in the buffers; with this branch it computes upstream's own
  `fib(25)` at one to four workers, which is the input that test ships.

- **Semaphore waiters are jobs, and an over-release is the releasing task's own
  failure** (issues #87, #89). Upstream's waiting list holds `Node*` and leaves
  the caller to keep those nodes alive; here it holds the crate's unit of
  queued work, which owns the run, the graph and the node it names for as long
  as it sits in the list — and a task keeps an `Arc<Semaphore>` where upstream
  stores the address of one, since a graph cannot promise that an object it
  only points at outlives it. Three consequences are decisions in their own
  right. A parked job is *not* finished, so nothing is counted down and the run
  stays exactly as outstanding as it was. The jobs a release wakes go to the
  shared buffers rather than into the worker's continuation slot: that slot is
  for a successor of *this* node's graph counted against *this* node's parent,
  and a woken job may belong to another graph and another run entirely, while a
  release wakes a whole set where the slot holds one. And releasing past the
  maximum, which is upstream's `TF_THROW` out of the worker loop, is here a
  failure of the task that did it — delivered to the same anchor a panic in its
  body would reach, surfaced from `wait` as `RunError::Panicked`, with the
  semaphore's counter left alone and the jobs woken by that task's *earlier*
  successful releases still queued, since dropping those would leave their
  counts outstanding for ever.

  One interaction is **recorded rather than fixed**, because it is upstream's:
  a job parked on a semaphore when its run is cancelled stays parked. Nothing
  sweeps a waiting list, so that job runs only when some other task releases,
  and until then the run's join counter never reaches zero — the one way a
  cancelled run does not complete promptly. The order of the two hooks bounds
  it, the cancellation check coming before the acquire, and `Semaphore::reset`
  is not the rescue either: it drops the waiting list, which unschedules those
  tasks for good. The executor's module docs have the race that is left.
- **`WorkerView` and `Worker` gain `queue_size`, not `queue_capacity`** (issue
  #206, following on from #167's entry on the observer list under
  "Observers"). `crossbeam-deque`'s `Worker::len` and `Stealer::len` read the
  same front/back indices `is_empty` already does, so `queue_size` is as
  honest a read as upstream's `BoundedWSQ::size` -- a live count, stale the
  instant it is returned, which is what `Worker::queue_size` and
  `WorkerView::queue_size`'s own doc comments say rather than letting a caller
  treat the number as a fact to act on.

  `queue_capacity` stays unanswered, because it has no truthful answer to
  give. Upstream's `BoundedWSQ::capacity` returns a fixed bound fixed at
  construction; a `crossbeam-deque` worker queue instead grows its ring
  buffer on demand from a 64-item minimum (per its own source, not part of
  its published API) and publishes that buffer's size through neither
  `Worker` nor `Stealer`. There is no call this port could make that would
  not be inventing a number: a hard-coded constant, `usize::MAX`, or the
  current `queue_size` would each look plausible and each be meaningless,
  which is worse than the method staying absent.
- **`Runtime::schedule`'s hole is closed in the scheduler, not documented and
  left to the caller** (issues #244, #245). M8's original premise is false a
  second time over, and in the opposite direction from the entry on the
  eleventh `Handle` variant under "Pipelines":
  `Handle::NonpreemptiveRuntime`'s dispatch arm never sets `PREEMPTED` so that
  `is_entered` would stop blocking a pipeline's own `schedule` bypass, but
  that same deliberate `false` means `is_entered` also stops answering the
  question it originally existed to answer -- whether this node's callable is
  genuinely running right now. `is_entered` only ever answers `true` for the
  four handle kinds `preempts_itself` does, and only once their dispatch arm
  has actually set `PREEMPTED`. Every other handle kind (`Handle::Static`,
  either conditioner, `Handle::NonpreemptiveRuntime`) is never "entered" by
  that reckoning even while its body is genuinely running, and
  `Handle::Subflow` is not either for as long as its body is still *building*
  the subgraph that would make it preempt -- so a `schedule` bypass targeting
  any of those, while the target's callable is actually locked, found nothing
  to stop it and could queue a second job onto a callable the first had not
  finished with, tripping `lock_callable`'s one-worker-per-node assertion.

  **Issue #244 tried to close this from the caller first, and that attempt
  is why #245 closes it in the scheduler instead.** A per-line
  `{Idle, Running, RunningWithWake}` status in the pipeline, re-armed before
  a line's body returns, fixed the *unbounded* version of the race -- and
  found two further real bugs on the way, a `stop()` early return that
  never released its claim and a `compare_exchange`/`fetch_max` gap that let
  a release land on the `Idle` a raise then overwrote. It still reproduced
  the panic at roughly 5 of 40 full-suite runs under `taskset -c 0,1`, with
  zero instrumentation, because the instant that matters -- the callable's
  `MutexGuard` actually dropping -- is several stack frames below the
  line's own body, inside `NonpreemptiveRuntimeWork::invoke`. A caller can
  flip its own status to `Idle` only *before* it returns; it has no frame to
  run code *after* the guard it does not own has dropped. No caller-side
  scheme can close that window, because no caller is the one that drops the
  guard. The dispatch layer is, so the dispatch layer is where the claim
  lives.

  **The fix is a claim per node, `Node::dispatch_claim` in `src/graph.rs`,
  independent of `PREEMPTED`.** Every dispatch arm that locks a node's
  callable -- `Handle::Static`, both conditioners, `Handle::Subflow`,
  `Handle::NonpreemptiveRuntime` -- claims it immediately before the lock and
  releases it immediately after the guard has dropped, except
  `Handle::Subflow`'s own preempting entry, which defers the release to its
  second entry: the point the node is genuinely idle again, rather than the
  point its body merely returned, since a fresh job racing the node's own
  still-outstanding nested run would be exactly the double dispatch this claim
  exists to prevent. `schedule_bypassing`'s own `assert!(!is_entered(target))`
  is untouched -- #240's reasoning for keeping `is_entered` answering a
  different question than this claim stands -- and what follows it now calls
  `DispatchClaim::wake` instead of queueing outright: a claim already held
  defers (`PENDING` set) rather than races, and the claim holder's own release
  queues the deferred job once it is actually safe to. `Handle::Runtime`'s own
  `enter` and the two module handles were deliberately **not** given a claim,
  on the reasoning that `enter` sets `PREEMPTED` *before* the body it locks
  ever runs, so `is_entered` already covers the entire locked window for an
  ordinary runtime task, and that the two module handles lock no callable at
  all, so `lock_callable` cannot trip on either of them. **That part of this
  entry is wrong, and the entry below on the dispatch claim has the defect it
  caused and the rule that replaces it**: reasoning per handle kind about
  which arms lock a callable missed that `wake` sets the bit for whatever
  target a body names, so a kind whose arm never released it leaked the claim.
  What stands here unchanged is the rest: that the claim lives in the dispatch
  layer rather than in any caller, and that the existing case asserting
  `schedule` refuses an *entered* node is kept exactly as a caller bug, not
  softened into a defer.

  What the claim holds every dispatch arm to is stricter than anything
  upstream promises: upstream's line task is a bare `std::function<void()>`
  call, invoked through whatever pointer `this` happens to be, with nothing
  in the language or the library stopping two threads from calling the same
  one at once -- only the join-counter arithmetic upstream trusts to never
  schedule that. This port's one-worker-per-node invariant is the scheduler's
  own rule rather than an accounting side effect, so the claim polices it
  directly for every dispatch arm that can be asked to run a node twice,
  pipeline or not, rather than trusting each caller's own counting to add up.

  A node queued twice through two *unrelated* channels before either has
  been claimed -- a real predecessor's release and a `schedule` bypass,
  racing to be the one that queues the node at all -- is not this claim's
  concern and is not newly introduced by it: `wake` only answers "is this
  node's callable locked right now", the same question a `claim` already
  held, taken by an in-flight dispatch, asks. `tests/loom_dispatch_claim.rs`
  models the narrower, in-scope property on its own terms and names the
  limit again.
- **The dispatch claim is one pair of calls per job dispatch, not one per
  handle kind** (issue #320). The entry above on `Runtime::schedule`'s hole
  reasoned about which dispatch *arms* lock a node's callable and gave the
  claim to the five that do, withholding it from `Handle::Runtime`, the two
  module handles and -- unexamined -- `Handle::Placeholder`. That reasoning
  asks the wrong question. `DispatchClaim::wake` is what `Runtime::schedule`'s
  bypass calls on its target, and it sets `CLAIMED` on **whatever node a body
  names**, handing the claim to the job it queues; a handle kind whose arm
  never released it therefore left the bit set for the whole life of the
  graph, and the next bypass of that node got `false`, queued nothing, and
  left the `count_queued(+1)` it had already paid outstanding -- a parent
  whose join counter never reaches zero. Three independent witnesses, each
  hanging on the second run of one taskflow on a single worker: a static task
  bypassed by a body that then panics, so the queued job is skipped by
  `WorkerThread::invoke_one`'s cancellation early-out, which returns before
  the dispatch `match` and so claimed and released nothing; a bypassed
  `Handle::Placeholder`; and a bypassed preemptive `Handle::Runtime`. Only the
  first needs a panic anywhere. The realistic witness is a pipeline, whose
  line tasks are all bypass-scheduled and all but one reachable *only* that
  way, which is how `tests/pipelines.rs` and `tests/data_pipelines.rs` came to
  have a cell apiece that hung under `taskset -c 0,1`.

  So the claim is the **job's**, and `WorkerThread::invoke_one` is the one
  place it is taken and given back: one `claim` before the handle `match`,
  one `release` after it, and the requeue of a deferred bypass at that
  release. Three things follow, and they are why this shape is the one taken
  rather than a sixth, seventh and eighth per-arm pair. A kind added to
  `Handle` cannot forget the claim, there being nowhere left to forget it.
  Preemption needs no special case: holding the claim across it *is* the
  existing `Epilogue` `None`, so `src/executor/subflow.rs`'s hand-written
  deferral to its second entry -- the subtle half of #245 -- is now what the
  `None` means for every preempting kind, and the arm just returns.
  And the cancellation early-out takes the same pair before `finish_job`,
  which is what closes the first witness: its `release` hands the claim back
  without a dispatch ever running, requeueing a pending bypass whose own job
  is skipped in turn, with the accounting unchanged because that bypass had
  already paid its count.

  The alternative the issue allows -- a `QUEUED` bit, so that `wake` stops
  setting `CLAIMED` on a node it has merely queued -- was rejected: it still
  needs one generic point that consumes the bit and honours `PENDING` for
  the kinds whose arms do nothing, so it buys a third state and a wider
  protocol for no property the pair above lacks, and #245's lost-wake
  argument would have to be re-answered for the new state rather than kept.

  The audit the fix rests on, over every path that retires a job without
  dispatching it. The cancellation early-out leaked, as above. The four arms
  that never released leaked, as above. `Node::acquire_all`'s refusal -- the
  semaphore park -- does **not**: the job is parked rather than retired, so
  the claim is still owed by a live job, a bypass landing meanwhile defers
  onto `PENDING`, and the eventual dispatch of the parked clone honours it;
  a run cancelled while the job is parked funnels into the early-out and is
  covered there. The dependent-async job copies "dropped undequeued" in
  `src/executor/dependent_async.rs` do **not**: those are free nodes, and
  `Runtime::schedule` takes a `Task` of a graph, so no claim is ever taken
  on them. The `is_entered(next)` branch of the successor-release loop does
  **not**: it declines to queue a successor nothing bypassed, so no `wake`
  ever ran. And the two async arms that refuse a handle built into a graph
  are now covered like every other kind, by the generic pair.
- **An observer hook may panic, and the scheduler catches it** (issue #304).
  The two answers the issue put were to catch the hook or to forbid a
  panicking one in `Observer`'s documented obligations, and the first is
  taken: `WorkerThread::observer_prologue` and
  `WorkerThread::observer_epilogue` call each registered hook under a
  `catch_unwind` and hand a payload to `Job::record_panic`, so the run
  fails with `RunError::Panicked` carrying the hook's own payload and the
  executor stays usable. What decided it is the cost of the violation, not
  the elegance of the rule. The hook calls bracket the dispatch *inside*
  the worker's scheduling loop, so a payload that escapes kills the worker
  thread with the job in flight never finished: the run's join counter
  stays short, `RunHandle::wait` blocks for ever, and the pool is one
  worker down with no diagnostic beyond the default hook's print. A rule
  the library cannot enforce, whose breach is a silent hang, loses to a
  rule the scheduler keeps by construction -- and an `Observer` is
  typically an adapter onto a caller's own logging or accounting, where a
  panic is as reachable as it is in a task body the executor already
  catches.

  Three consequences are worth naming. The catch is **inside** the two
  seam methods rather than at their call sites, which is what covers all
  six `observer_epilogue` callers -- the generic one under
  `WorkerThread::invoke_one`'s dispatch `match` and the five in
  `executor::async` and `executor::dependent_async` -- and what keeps
  #320's one `claim`/`release` pair balanced: a hook that panics now
  cannot unwind out from between the claim and the release, so this fix
  does not trade this hang for that one. It is one `catch_unwind` **per
  observer** rather than one around the loop, matching what the successor
  loop's `is_entered` branch and `Node::release_all` already do with a
  failure mid-iteration: record it and carry on with the rest, so one
  failing hook does not cost the observers after it their callback, with
  the run keeping the first payload offered. And the failure does not
  retract the dispatch the hook brackets: a panicking `on_entry` still
  lets the body run -- this entry's cancellation check is already past --
  and a panicking `on_exit` still leaves the node's tear-down to the
  caller, exactly as a panicking task body still releases its successors.

  Upstream is departed from knowingly here. Its `_observer_prologue` and
  `_observer_epilogue` calls sit *outside* the
  `TF_EXECUTOR_EXCEPTION_HANDLER` wrapping each `_invoke_*_task` body, so a
  throwing hook unwinds into the `catch(...)` around its `_spawn` loop,
  reaches `scheduler_epilogue` and ends that worker -- the same hang, in
  C++. The third hook needs none of this: `Executor::make_observer` calls
  `Observer::set_up` on the registering thread before inserting anything,
  so its panic is raised to that caller and no observer is registered,
  which `tests/observers.rs` pinned before this change and still does.

- **A nested corun is bounded by the worker's stack, and the bound stays the
  OS default** (issue #359). A `corun` keeps its worker in the work-stealing
  loop, and `WorkerThread::corun_until` runs the job it picks up by calling
  `invoke` on it -- on the waiting frame. So a job that is itself a parent
  which submits children and coruns adds another level, and the depth that
  reaches is a property of *which queue a child was pushed to* rather than of
  the recursion: a `TaskGroup`'s children go to the shared buffers, because a
  group's corun loop runs on a `corun_view` that holds no owning queue end, so
  the waiting worker is handed the oldest outstanding job in the pool and nests
  one frame per outstanding **task**; a `Runtime`'s children go onto the
  worker's own LIFO queue (#119), so its corun pops the child it is waiting for
  and nests one frame per level of **recursion**. One worker is therefore the
  binding end of the group route rather than its easy case: with nobody else to
  pick the outstanding children up, the whole subtree becomes one chain of
  frames on one stack, and adding workers spreads that set over their stacks
  instead. All of it is measured in [`BENCHMARKS.md`](BENCHMARKS.md) under "The
  #359 measurement", which is the one place the figures and the method live --
  about a kilobyte a level in a release build, three and a half in a debug one.

  **The stack stays at std's default** rather than being set with
  `thread::Builder::stack_size`, and the measurements are the argument. The
  group route's demand is geometric in the shape's size -- ten times per level
  of the ten-way `skynet` tree -- so no constant is a bound: raising a worker
  to 8 MiB, which is what `ulimit -s` gives a C++ `std::thread` on the
  measurement box and four times what std gives a Rust one, buys one notch of
  two of the three benchmark shapes and none of the third, while upstream's own
  sweeps would want gigabytes. It would also
  cost every executor 6 MiB of reserved address space a worker, recursive or
  not, and take away the one control a caller has today, since an explicit size
  overrides `RUST_MIN_STACK` and the executor has no configuration surface to
  put a replacement knob on. What actually lifts the ceiling is the route, and
  only the route: the runtime route already carries upstream's whole `skynet`
  sweep on one worker in eight kilobytes of stack. This entry expected the
  preempted-parent resume to lower the group route's demand, and #342 measured
  it and found it cannot -- a group's children count down an anchor that never
  carries `PREEMPTED`, so what the last child releases is the waiting frame
  itself, which is the thing the demand is made of. The entry below has the
  counters.

  So the bound is a documented property of the port. An overflow is an abort
  rather than a `RunError`, which is the one failure in the crate a caller
  cannot catch, so `TaskGroup::corun`, `TaskGroup::try_corun`, `Runtime::corun`
  and `Runtime::try_corun` each name it where a caller writes the wait, with
  the three things that keep a recursion inside it: bound the tasks outstanding
  under one corun, submit through a `Runtime` rather than a `TaskGroup` where
  the shape allows, or raise `RUST_MIN_STACK`. Nothing here changes how `corun`
  nests.

- **A preempted parent is resumed through a queue, not the continuation slot**
  (issue #342). Upstream's `_tear_down_nonasync` ends in
  `_update_cache(worker, cache, parent)`: the child that drives a preempted
  parent's join counter to zero keeps that parent out of the queues and
  `TF_INVOKE_CONTINUATION()`s straight into it, on the thread that has just
  finished the last child. `WorkerThread::finish_job` schedules it instead --
  the worker's own LIFO queue, or the shared buffers when the worker is a
  corun view, with a `notify_one` either way. #62 took half of upstream's
  locality optimisation, the one successor a node keeps out of its queue, and
  this entry is why the other half stays untaken.

  What decided it was counting the events rather than timing them. Across the
  eleven shapes the suite A/Bs, that re-queue happens **twelve times**, all
  twelve of them in `pipeline` at one per pipeline run, and in the five nested
  shapes -- `fibonacci`, `skynet`, `nqueens`, `merge_sort`, `integrate` -- it
  happens never. The reason is the shape of a `corun`, and it corrects what
  the #359 entry above expected of this issue. A `TaskGroup`'s children are
  parented to the group's own anchor, `Nested::new(None)`, which carries
  `IMPLICITLY_ANCHORED`, never carries `PREEMPTED` and has no job to resume,
  so what the last child's decrement releases is the waiting *frame* -- the
  very frame whose nesting is that entry's stack bound. Resuming a preempted
  parent on the spot therefore cannot lower the group route's stack demand. A
  `Runtime`'s children do hang off the node, and that node is `PREEMPTED` for
  as long as its body runs, but `Runtime::corun` waits for the join counter to
  come back to one, so the body's own reference is the last one spent and the
  node finishes on its first entry. What leaves a parent preempted is a body
  that returns with work still outstanding: an unjoined `silent_async` or
  `scope`, a subflow, a module task.

  The divergence from upstream is therefore deliberate rather than an
  omission, and it is cheap to revisit: the patch is 28 lines over
  `src/executor/run_handle.rs` and `src/executor/worker.rs`; every test but
  the five that pop a worker's queue to assert the re-queue itself passes
  against it; and the one-worker-per-node invariant is untouched either way,
  since exactly one child sees the counter reach zero, so exactly one resume
  job exists however it travels, and `invoke_one` takes the dispatch claim on
  entry whether that job came from a queue or from the slot. The figures, the
  counters and the 56-id A/B behind all of this are in
  [`BENCHMARKS.md`](BENCHMARKS.md) under "The #342 decision"; what would
  reopen the question is a benchmark shape that leaves parents preempted,
  rather than a quieter box.

- **A fan-out stays `w - 1` individual pushes and `w - 1` pokes, because the
  wake-ups were never the ceiling** (issue #367). A `for_each_by_index` at `w`
  workers reaches 13 to 23 of 24 on this box rather than all of them, and #343
  left the cause open between two candidates: the notifier losing wake-ups, or
  the steal side. Counters settle it. Over twelve runs of the published shape
  the submission issues 24 `notify_one` calls, which produce 24 or 25 `unpark`
  calls and 23 to 25 returned parks -- the whole pool wakes, one worker per
  push, with at most one poke per run spent on an empty notifier -- and a
  counter summing every worker deque and every buffer at the moment each park
  is committed reads **zero** parks taken while a job was queued anywhere. So
  no notification is lost, none is owed, and the two-phase guard in
  `wait_for_task` is doing its job. What is left is that the `w - 1` tasks all
  go onto the submitting worker's single deque at about 2 µs each, so each is
  taken by whichever already-awake thief wins the race for that one queue: a
  winner's `sticky_victim` costs it 6 to 100 steal attempts, a loser draws at
  random over `num_queues() == 29` and spends 150 to 285 before parking again.

  **The candidate that was measured and declined is #378's**, a bulk push with
  one `notify_n(w - 1)` in place of the per-task `notify_one`. It was
  implemented behind a switch, counted, and reverted: the push loop costs the
  same 45-60 µs for 23 tasks either way, because what it is made of is the
  node, the two boxed closures and the deque push rather than the `unpark`;
  the distinct-worker count stays inside the unbatched path's own run-to-run
  range at 20, 100 and 500 µs bodies; and on the longest bodies two runs of
  three recorded a submission stall of 0.8 to 1.1 ms, the burst of 23 unparks
  from one call making 23 threads runnable at once on a 24-thread box and
  costing the submitter its own core. The spill buffers are not implicated
  either: an external bulk submission -- one `push_bulk` of 24, one
  `notify_n(24)`, under a microsecond for the whole fan-out -- reaches the same
  count as the worker path's 50 µs of drip-feeding, so dealing the work over
  five queues instead of one changes nothing.

  Upstream is identical here, which is why this is a shape of the design and
  not a port gap: at the pin `Executor::_schedule(Worker&, Node*)` is one
  `try_push` plus one `notify_one`, `_schedule_async_task` routes a worker's
  `silent_async` straight into it, and `for_each.hpp` dispatches
  `(++w == W) ? task() : rt.silent_async(task)`. Upstream's own bulk overload
  exists and is spent on released successors and graph sources, never on a
  runtime fan-out, and the commented-out experiment inside it records that
  notifying earlier bought nothing. rayon reaches more workers on the same
  shape for a different reason: its fan-out is a tree, `join_context` pushing
  the right half onto the stealing worker's own deque and `Splitter::try_split`
  resetting the split budget to the thread count whenever the job it splits was
  stolen, so a late thief has many non-empty deques to find instead of one.
  `SplittingPartitioner` (#374) is that mechanism ported, and the measurement
  shows it reaching more workers on a cold pool at three to four times the
  dispatch cost -- a trade #381 priced and declined as a default, in the
  entry under "Algorithms" below. The counters, the
  partitioner sweep, the worker-count sweep and the background-spinner control
  are in [`BENCHMARKS.md`](BENCHMARKS.md) under "The #367 measurement"; what
  would reopen the question is a box with more hardware threads than the pool
  has workers, which would separate the arrival race from the machine's own
  width -- at 24 workers on 24 threads they are confounded, and at every width
  up to 20 the fan-out reaches every worker it has.

  **What the partitioner A/B added to this is that the share, not the tree, is
  what reaches a late worker** (issue #381). Turning the re-splits off by
  giving `SplittingPartitioner` a floor above `n / w` leaves 25 tasks -- the
  same count the flat dispatch submits -- and still reaches 21 to 23 of 24
  workers, where the same partitioner with its floor at 1 reaches 21 to 24 for
  121 to 157 tasks. What differs from the guided cursor at those floors is
  only what each task is handed: `n / w` of the range outright, against
  whatever the cursor has left by the time that task is entered. So the
  tree-versus-flat distinction this entry drew from rayon is not the one that
  matters on the arrival race; how much work a task still holds when it is
  finally stolen is. The ceiling itself is unchanged -- one producer, one
  queue, both upstream's -- and the figures are in
  [`BENCHMARKS.md`](BENCHMARKS.md) under "The #381 measurement".

- **A corun learnt its child was done one failed search late, and that search
  was 211 attempts long** (issues #407 and #412, **fixed**).
  `WorkerThread::corun_until` read its
  predicate once a round and spent the round in `explore_task`, the scheduling
  loop's search with the scheduling loop's give-up: `(num_queues() + 1) * 2`
  attempts, then 150 more each after a `thread::yield_now`, 137 to 150 µs on
  this box at 24 workers. A parent whose child was stolen therefore returned no
  sooner than that round ended, however quickly the child finished -- which
  made the entry above's "a corun returns no sooner than the inner run
  finishes and no later than the next job it picked up is done" false by one
  search. Upstream's `_corun_until` at the pin reads `stop_predicate()`
  after **every** failed steal, yields past `MAX_STEALS` and never gives up on
  its own; the three differences `corun_until`'s docs list (one local job a
  round, never sleeping, ignoring `done`) are upstream's too, and this fourth
  one is not. Counters on `fibonacci(15)` at 24 workers put every failed corun
  search at exactly 211 attempts and 151 yields, 100 to 178 of them a run, and
  at **94 to 103% of the run's CPU** -- 13.5 to 27.8 ms of 17.9 to 32.5 for
  169 µs of work -- while the scheduling loop's own thieves fail exactly one
  search each, at the end, on their way to parking. A body-cost sweep says it
  is a defect and not a granularity floor: at 1 µs bodies rayon gains 15x from
  24 workers and the port 1.2x. An ablation that swaps in upstream's loop,
  interleaved with today's in one binary, reads 137 µs against 1.848 ms on the
  zero-work shape at 24 workers, 255 µs against 2.147 ms at 1 µs bodies and
  1.131 ms against 2.733 ms at 10 µs, CPU at 0.22x, 0.25x and 0.50x, and it is
  the first time 24 workers beat one on `fibonacci` or `integrate`.

  **#412 shipped that loop**, as `WorkerThread::steal_until`: a search that
  steals, reads the predicate, yields past `MAX_STEALS` and gives up only when
  the predicate says to, which is a port-fidelity correction rather than a
  scheduler design. `explore_task` and `wait_for_task` keep their 211-attempt
  give-up, that being what lets a worker reach the notifier and park, which a
  corun never does. The A/B moves **20 of the 22** 24-worker `rustflow` ids
  across `fibonacci`, `integrate`, `merge_sort`, `skynet` and `nqueens`
  outside both sides' spreads -- `nqueens/rustflow-group/24w/6` 0.056x,
  `fibonacci/rustflow-runtime-group/24w/15` 0.061x,
  `integrate/rustflow/24w/2` 0.141x -- and leaves 20 of the 22 one-worker ids
  inside them; the two exceptions either end are `merge_sort`'s
  million-element rows, where twenty million compares bury the wait, and two
  `skynet` one-worker cells that read ±4% in opposite directions. Every one of
  the five shapes is now faster at 24 workers than at one, and three of them
  beat rayon at 24 where none did. What it costs is **16 bytes of a worker's
  stack a level**, the `&mut` to the predicate the search leaves addressable:
  the search is a function of its own and `#[inline(never)]` for exactly that,
  which the measurement shows is worth 16 bytes a level against writing it as
  one loop (32) or letting it inline (64). The per-level figures
  `TaskGroup::corun`'s docs carry move with it, and no size in the suite does.
  #407 closed with it. The sweep, the counters, upstream's loop and the
  ablation are in [`BENCHMARKS.md`](BENCHMARKS.md) under "The #407
  measurement", and the A/B, the re-published tables, the stack probe and the
  latency test under "The #412 change". What neither reaches is the per-child
  constant (#382's territory) or the two-worker case, where a stolen child
  still costs the parent a wait with nobody to hand it other work.

## Control flow and cancellation

- **A failure inside a nested graph is delivered to whoever is waiting for
  that graph** (issue #48), not to the run. Upstream calls the thing that is
  waiting an *anchor* and finds it by walking the failing node's parent chain;
  the walk is ported with one change of priority — an explicit anchor (a frame
  blocked right now in an `Executor::corun`, a `Subflow::join`, a
  `Runtime::corun` or a `TaskGroup::corun`, and since the async family also a
  free node whose runtime-shaped body hands back a handle, which is anchored
  for its whole life because the waiter is whoever holds that handle) beats an
  implicit one (a node that preempted itself and means to read its payload
  slot when it is resumed — a runtime task, a free node with a runtime-shaped
  body, or a task group's anchor, which carries the bit from the moment the
  group is opened) beats the run, which is every chain's root. The one root
  that is no destination at all is the sentinel run a free async task's job
  names; the entry on it under "The async family" says why. Two decisions came
  out of #48 that upstream does not have to make, and the change of priority
  is #157's.

  The payload slot is **on the node**, as upstream's `_exception_ptr` is, even
  though a chain of parents in this port is a chain of `Nested` allocations
  rather than of nodes and a slot per nested run would be cheaper. It cannot
  live there: the last job of a nested graph re-queues its parent and then
  drops the `Nested`, so a payload stored on it would be gone exactly when the
  resumed parent needs to read it. The anchor a `corun` waits on gets a slot
  for free, being a `NodeBase` itself; the run is the one anchor without one,
  and uses the completion slot a `wait` already reads.

  The **loser of the one-payload race is dropped** where upstream parks it on
  the failing node. Upstream can read it back through
  `tf::Task::exception_ptr()`, an accessor this port does not have, so parking
  it would hold a caller's payload — destructor and all — until the next run of
  that graph set the node up again. What the port keeps instead is the promise
  its thread-safety case pins: of a thousand tasks failing at once, exactly one
  payload reaches the caller and the rest are dropped by the run.

  An **implicit anchor beats the run** (issue #157), where upstream's chain
  ends at a topology it constructs `ESTATE::EXPLICITLY_ANCHORED` — its
  `Topology` *is* a `NodeBase` — and which therefore outranks everything below
  it. The two priorities differ only for a chain that has both, and that chain
  is a runtime task of a graph whose body spawned silent children. Under
  upstream's order the destination depends on *when* a child failed: the node
  is explicitly anchored only from the moment its `corun` is entered, while a
  child pushed onto a stealable queue can fail before the body gets there, so
  the same program delivers to the body's `corun` or to the run depending on
  which side was descheduled. Upstream has the window too, and does not run
  the case that would see it: its `RuntimeAsync` test is commented out in
  full, no reason given, with a note on its third phase that a `silent_async`
  can finish before the `corun` does. This port declines to inherit the
  window: what delivers is the node's `ENTERED` bit, which is
  set before the body runs and cleared only once every child has been counted
  off, so no child can observe the node unanchored.

  That needs one companion, in `enter`: a node leaving `ENTERED` with a payload
  still in its slot runs the walk again, from a node that is by then no anchor,
  which hands the payload to the enclosing anchor or to the run. So a body that
  never coruns still fails its run, exactly as it did under the other priority
  and as it does upstream, and the two halves together are what make the
  delivery independent of the order the body and its children finish in.
  `tests/loom_anchor.rs` is the model of that pair, with the orderings it was
  checked against in its header; the alternative of giving `ENTERED` the
  explicit bit instead was rejected for needing the same companion while
  blurring what "explicit" means, which is a frame blocked right now.
- **The cancellation early-out reads the whole parent chain** (issue #48).
  Upstream's `Node::_is_parent_cancelled` reads the immediate parent only,
  which it can afford because `_process_exception` marks every ancestor on its
  way up — and the walk here does that too, so one level would be enough. Every
  level is read anyway: it costs one relaxed load per level of nesting and
  answers strictly more, since a job whose own parent is unmarked can still
  have a marked *grandparent*, when the failure was in a sibling subtree under
  the same anchor, and every job under a failed anchor is one whose result
  nobody will collect.
- **`Runtime::schedule` is limited to a task of the runtime task's own
  graph** (issue #51). Upstream's `schedule` takes a `tf::Task`, which is a
  pointer to a node, and reaches that node's parent and topology through it;
  a `Task` here is an index into one arena, so the only graph that can resolve
  one is the graph the scheduling job already holds. That limit is what makes
  the accounting answerable: the parent to count a scheduled task against is
  the runtime node's own parent — the run for a top-level task, the enclosing
  node for a task of a nested graph — which is upstream's
  `node->_parent ? … : node->_topology` with the choice made by the job rather
  than by a null check. It is not fully checkable, and the port does not
  pretend otherwise: an index this graph does not have panics as any stale
  index does, and an index that happens to be live here schedules *this*
  graph's node at it. Scheduling a task of a graph that is not running is the
  caller's error upstream too. One case did become checkable, and the entry on
  bypassed dependencies below has it: a target the run has already *entered*
  is refused rather than resumed early (#107).
- **A bypassed dependency may not re-enter a task the run has entered**
  (issues #98, #107). A join counter can be driven to zero by something other
  than its dependencies — `Runtime::schedule` stores a zero, and a conditioner
  zeroes the successor it selects — so a later strong release can arrive at a
  counter that is already zero. Upstream's bare `fetch_sub` on a `size_t`
  wraps it to `SIZE_MAX`, which is silent and not benign: a wrapped counter
  never equals the one a `Runtime::corun` waits for nor the zero a parent waits
  for, so a bypassed node that later coruns or preempts hangs. The decrement
  therefore **floors at zero**, which keeps the counter's meaning and loses no
  release upstream delivers. The other half the floor cannot reach is a bypass
  that lands on a node already *entered* — one whose count is one reference per
  piece of its own outstanding work rather than a dependency count — because
  spending one of those resumes the node before that work is done. Both
  bypasses ask the question before they touch the counter and both are
  **refused**: `Runtime::schedule` panics on an entered target, and a strong
  release onto one is dropped and recorded as the failure of the task that
  released it, so the run answers with a reported failure where it used to
  hang. A task that has *finished* and re-armed its counter is a different
  case and is not diagnosed: it simply runs a second time, which is the
  caller's own doing and stays upstream's rule to keep rather than this port's
  check — schedule a task the graph cannot otherwise reach.
- **A cancelled task group should not be corun while a dependent-async chain
  behind it still has successors to release** (issue #93). Upstream's skip
  for a cancelled child goes through `_tear_down_invoke`, draining that
  child's successor list as it tears the node down; the port's equivalent,
  the skip at the top of `WorkerThread::invoke_one`, counts the anchor down
  but does not drain it, which is the dependent-async arm's own job and is
  never reached for a child that was skipped rather than run. A chain
  cancelled mid-flight therefore leaves the jobs behind the cancelled link
  unqueued, their counts still outstanding on the anchor, and a `corun`
  after that does not return. The fix is a rule for the caller rather than
  the scheduler: cancel a group whose outstanding children are
  dependent-async tasks only once none of them still has a successor
  waiting to be released.

## Dynamic tasking

- **A module task shares its target, `Arc<Taskflow>`.** Upstream's
  `composed_of` stores a bare `Graph&` and leaves the caller to keep the
  composed taskflow alive for as long as any run of the composing one may be
  in flight — an obligation a safe Rust API cannot state, so the module owns a
  share of its target instead. `adopt(Graph)` is the owned form of the same
  thing, for a graph with no other owner.
- **A module task reaches its target through whatever owns it** (issues #49,
  #74). Upstream's three module functions are two forwarders onto one `_impl`
  over a `Graph&`, which is all they need: a reference reaches the nodes and
  the caller is left to keep them alive. A job of a graph here owns what it
  names, so the forwarders differ in who owns the target.
  `Handle::Module` owns a share and hands over `Taskflow::flow`;
  `Handle::AdoptedModule` owns its graph *inline*, in the arena the module
  node itself is in, so the only thing that names it is the outer owner plus
  that node's index — the path `Job::graph_inside` builds. What the path has to
  be right about is which arena an index means: a job of an adopted graph
  built over the outer owner would resolve its tasks against the composing
  taskflow's own nodes, silently, against whatever sits at those indices.
- **A module task may not run a graph its own run is inside of** (issues #49,
  #82). Upstream's obligation for `composed_of` — "thread-safe as long as the
  target is not concurrently ran by two or more threads" — survives
  translation, because what makes it an obligation is the target's per-node
  state rather than anything about C++: a join counter and a state word belong
  to one run at a time. Sharing the target as an `Arc<Taskflow>` buys the
  caller the lifetime half of that rule and not this half; what it changes is
  the failure, two workers reaching one node finding `Work::invoke`'s
  `try_lock` taken and panicking, which the run reports, where a silent race
  on a counter is the alternative.

  One shape of that error a run *can* see coming is refused rather than left
  to the caller: a target that is a graph the module node is already inside
  of. A taskflow cannot be composed of itself when it is built — `composed_of`
  takes an `Arc<Taskflow>` and the taskflow it builds into needs `&mut` — but a
  subflow body can do it while the taskflow runs, a `Subflow` being a
  `FlowBuilder` and the taskflow a body belongs to being alive behind an `Arc`
  the caller holds. None of what that did is about locking: scheduling a graph
  re-arms every node of it, so the subflow node the module node hangs under
  loses the `PREEMPTED` bit and the join counter of one that `finish_job`
  would have resumed it by; the subgraph then finishes, nothing re-queues that
  node, the run's counter never reaches zero and `wait` never returns — with
  every worker parked, the run being not slow but over and unreported. So a
  module node's first entry asks whether the target is a graph this job is
  already inside of, its own or that of any node on its parent chain, and
  reports a panic payload instead of running it: the payload becomes the run's,
  the rest of the run is cancelled, and `wait` answers `Err`. Two taskflows
  composing each other are the same cycle one link longer, and the walk costs
  one pointer comparison per level of nesting. What it cannot see is a chain a
  `corun`'s anchor ends without naming the job that started it, and that route
  needs no second answer: a corun of the taskflow whose task is coruning
  re-enters a node whose body is still running, which the `try_lock` above
  reports. A preempted node's body is *not* running, which is the whole of why
  this case slipped past it.
- **A subflow's subgraph lives in a `Mutex<Graph>` inside the node's handle,**
  because the body builds it through a node the run only *borrows*: a worker
  reaches its node as `&Node`, so the graph it writes needs interior
  mutability of its own. The `Subflow` handle holds the guard for the body's
  duration, which cannot deadlock one — a nested subflow locks its own node's
  graph, so no body ever takes the same lock twice.
- **A subgraph is *leased* for the length of its run, and the last share of
  the lease is what puts it back** (issues #74, #50). Running a graph needs
  whatever owns it, because every job of a graph owns what it names, and a
  `MutexGuard` is not something a job can own — so the subgraph is moved out
  of that `Mutex` while it runs and held by its jobs through one shared
  allocation. No lock is then on the scheduler's hot path, where upstream has
  a plain `Graph&`; the price is that somebody has to put the graph back, and
  it cannot be the frame waiting for the nested run. A job of a graph nested
  *inside* a leased subgraph decrements the join counter of its parent node,
  which is a node of that subgraph — so the counter is inside the lease, the
  job holds the lease across the decrement that can end the subgraph's run,
  and no holder can prove it is the last one. A `Drop` needs no such proof,
  so that is where the graph goes home, and dropping it instead *is*
  upstream's `clear()` for a subflow that was not retained. Two visible
  consequences: a retained subgraph goes back under `try_lock`, never a
  blocking one, because the thread holding the last share is a worker that
  something else may be waiting for; and `retain(true)` is reliable set
  *before* the join it is meant to keep, not after.
- **A runtime or subflow handle cannot hand back the executor** (issues #50,
  #51). Upstream's `tf::Runtime` and `tf::Subflow` hold an `Executor&`, and
  this port's cannot, because the two are not the same object here: upstream's
  executor *is* the state its workers share, where an `Executor` here is a
  handle over an `Arc` of that state plus the pool's join handles, owned by
  value with no `Arc<Executor>` for a worker to keep a `Weak` of. The handles
  are built on a worker thread, which holds the shared state and nothing else,
  so reproducing the member would take a raw back-pointer from that state to
  the value owning it — and the executor has had no `unsafe` since #40. What
  upstream's own callers ask `rt.executor()` for is almost always the worker
  count, which *is* in the shared state, so `Runtime::num_workers` and
  `Subflow::num_workers` stand in its place; a body that genuinely needs the
  executor captures an `Arc<Executor>`, as upstream's own
  `Runtime.ExternalGraph.Simple` captures a reference and as this crate's
  `corun` doctests already do. "Almost" is the word that matters, and it was
  counted rather than assumed: of the 26 uses of the accessor in upstream's
  `algorithm/` headers at the pinned tag, 25 are `rt.executor().num_workers()`
  in the algorithms of step 6 below, and the twenty-sixth is `scan.hpp`
  calling `rt.executor().corun_until(..)`. So step 6 is the moment to revisit
  this — when that scan is ported, and not before. A `TaskGroup` is the one
  handle that *does* have an `executor()`, and for the reason this entry
  gives rather than against it: a group is opened by a call on the `Executor`
  value itself, so the borrow it keeps is the caller's own and no
  back-pointer is involved. A group opened on a `Runtime` instead answers
  `None` there, which is this entry applied a second time rather than an
  exception to it; the entry below on the two routes has it.
- **A task group has no implicit join on drop** (issue #93). Upstream's
  `TaskGroup` has no destructor, so a group that goes out of scope with work
  outstanding dangles the `NodeBase` its children are still counting down —
  undefined behaviour, which upstream's own documentation says in as many
  words. This port mirrors the *behaviour* without the undefined half: the
  anchor is an `Arc` shared with every child rather than a value on the opening
  frame, so an unjoined group is merely unjoined — the children run, their
  counts land on an anchor that is still alive, and the last share goes with
  the last child, taking a payload nobody came for with it. A `Drop` that
  coruns was the alternative and is declined deliberately: it would put a wait
  on the one path a caller cannot see, cannot opt out of and cannot handle a
  failure from, including an unwind, where coruning inside a `Drop` risks a
  second panic and an abort. `TaskGroup::corun` is where the join and the
  failure both live.

  Where a group's children are queued is a property of the **route** it was
  opened through and not of groups (issue #372). `Executor::task_group` is a
  call on the executor value, which can name the opening worker only by id —
  the owning end of that worker's queue lives in the `WorkerThread` frame
  below the task body, where no method on the `Executor` can reach it — so
  such a group pushes into the shared buffers and coruns on a `corun_view`
  whose pops are steals from the first-in end. Pushing LIFO against that FIFO
  waiter would hand the corun the oldest outstanding subtree rather than the
  child it just spawned, so the buffers are the consistent destination for
  that route. `Runtime::task_group` is the route that *does* hold the frame:
  the group borrows the `&mut WorkerThread` the runtime has, its children go
  onto the worker's own LIFO queue and its corun pops that same end, which is
  what upstream does for every group through the thread-local `this_worker()`
  its `_schedule_async_task` consults. What the route decides is a
  **stack-depth** ceiling rather than correctness — every child runs either
  way: the id route nests one frame per outstanding *task*, which for a
  recursion is most of the task tree, where the runtime route nests one per
  level of *recursion*. `BENCHMARKS.md` has both cliffs, and
  `src/task_group.rs` tells a caller which one the wait they are writing is
  against.

  Holding the worker rather than its id costs one thing, and it is in the
  signature rather than in a second type: `TaskGroup::executor` answers an
  `Option`, `None` on the runtime route, for the reason the entry above on
  handing an executor back gives — a runtime body cannot name the executor
  running it, so neither can a group it opens. A second public type with
  sixteen spawning forms of its own would have bought the accessor back and
  nothing else, so the routes are one type and one enum. It cost a second
  thing until #395: the group borrowed the runtime for its whole life, so a
  body could not schedule, spawn a sibling or corun its own node while one was
  open, and upstream's tail-optimised `fibonacci` — which keeps a group open
  across an inline branch that opens groups of its own — had to be written on
  this route with that branch first, upstream's own `FibonacciSwapped`
  ordering. The entry below on lending the runtime back is what removed it.

- **Both routes stay, and which one a body wants is decided by its own
  *ordering* rather than by the queue** (issue #380). The A/B of the two
  routes over the five nested benchmark shapes says two things that point
  opposite ways, and [`BENCHMARKS.md`](BENCHMARKS.md)'s "The #380 measurement"
  has the figures behind both. The runtime route is the cheaper submission at
  every pool width -- about half the cost of a spawned task at one worker --
  and it is the only route whose stack demand is per level of recursion, so a
  parent that submits all of its children and *then* waits is strictly better
  off on it: upstream's `skynet` and `nqueens` improve on it where the id
  route regresses as workers are added. But a parent that computes one branch
  **inline** between submitting the other and waiting for it -- upstream's
  tail optimisation, which its `fibonacci`, `merge_sort` and `integrate`
  benchmarks all write -- cannot write that on the runtime route at all,
  because the group borrows the runtime that inline branch needs. Its
  submission therefore goes last, one child is outstanding at a time, and at
  24 workers that costs between 1.7 and 27 times the id route depending on the
  shape, which is more than the route's own saving by an order of magnitude.
  The measurement's ablation puts the whole of that on the ordering: the *id*
  route under the same ordering is just as slow, and the runtime route under
  the id route's ordering is the fastest of the five variants it measured.

  So the id route is **kept, and it is not "the slow route"** -- it is the
  route that does not borrow the runtime, which is what a tail-optimised
  recursion needs and is worth more at width than the queue is. That is the
  distinction `src/task_group.rs` draws between the two constructors, rather
  than one of speed, and `TaskGroup::corun` carries the stack half of it with
  both cliffs measured. What would collapse the choice back to a single route
  is an API that lets a body on the runtime route submit *before* its inline
  branch runs -- a scoped form handing out a group and a reborrowed `Runtime`
  together, say -- which was #395's question and which the entry below
  answers: the choice did collapse, and the conclusion of this entry is
  superseded by it.

- **A group opened on a `Runtime` lends it back, in a scope, and that is the
  only shape of it that is sound** (issue #395). Upstream's tail optimisation
  — submit one branch, compute the other on this worker, then wait — is what
  its `fibonacci`, `merge_sort` and `integrate` benchmarks all write, and the
  entry above records that the runtime route could not write it and what the
  forced ordering cost: at 24 workers and their larger size, 4.7x to 26.8x the
  id route. `TaskGroup::with_runtime` is the fix, and it is the port's own —
  upstream's group reaches the submitting worker through a thread-local
  `this_worker()`, so a body there is never handed one back and the header has
  no counterpart to translate. [`BENCHMARKS.md`](BENCHMARKS.md)'s "The #395
  change" has the figures: those three cells go from 4.7x-26.8x against the id
  route to 0.53x-0.87x for it.

  **The shape the issue asked for first is the one Rust does not have.** A
  `Runtime::with_task_group(|group, rt| ..)` handing a body both handles at
  once wants a `&TaskGroup` and a `&mut Runtime` alive together, and both of
  them are paths to the same `WorkerThread`: the group needs it to push a
  child onto the worker's own deque and to re-enter that worker's scheduling
  loop in `corun`, and the runtime needs it mutably for every scheduling
  method it has. That is `&` and `&mut` to one place, which no
  `RefCell` rescues either — a cell makes the *borrows* dynamic but a body
  holding a live `Runtime` out of one would be holding the cell's mutable
  borrow across an arbitrary stretch of its own code, so the group's next
  submission panics instead of compiling. The shape that *is* sound is
  nesting rather than simultaneity: the group owns the reborrowed handle and
  lends it out for a scope. `with_runtime` takes `&mut self` for that reason
  and reads the cell through `RefCell::get_mut` — the compiler then refuses a
  body that tries to submit into the group whose runtime it is holding, and
  there is no dynamic check left to fail. Nothing here is `unsafe`, and the
  one-worker-per-node invariant that `is_entered` and `Work::invoke`'s
  `try_lock` enforce is untouched: the lent handle is this thread's, names the
  node its body is already running, and is a reborrow in a chain the compiler
  checks rather than a second owner of it.

  Two consequences are the signature's. The method answers `Option`, `None`
  and nothing run on the id route, which is the mirror of `TaskGroup::executor`
  rather than a failure — each route hands back the handle it holds, and a
  group opened on the `Executor` borrows no runtime there is to lend. And the
  group grew: it carries a whole reborrowed `Runtime` — worker, node, job and
  worker thread — where it carried a worker, because the node and the job are
  per-invocation and reach it no other way. That is 16 bytes a level of a
  recursion of groups on the id route and 32 on the runtime one, measured
  against both builds in one probe, which is a 1.5% and 2.7% shallower chain
  of nested coruns and no shape's size in the suite. Holding a `&mut Runtime`
  in the group instead would have cost one pointer rather than four, and is
  not available: `Runtime<'r>` is invariant in `'r`, so `TaskGroup` would have
  taken a second lifetime parameter and every signature naming `TaskGroup<'_>`
  would have had to name it too.

  So the two routes collapse to one choice, and it is not speed: **open the
  group on a `Runtime` wherever a body has one**, and keep
  `Executor::task_group` for a body that was handed none — a plain `spawn`, a
  static task, any worker without the scheduler in its hand. `src/task_group.rs` is where a caller is
  told which they are writing.

## The async family

- **`async` is `spawn`, and nothing else in the family is renamed** (issue
  #88). `async` is a reserved word in Rust 2024, so upstream's value-returning
  `async` is `spawn` on every surface that has one — the executor, a
  `Runtime`, a `TaskGroup` — and the other three names are kept verbatim,
  `silent_async`, `dependent_async` and `silent_dependent_async` all being
  legal identifiers. A body that is handed the scheduler rather than nothing
  takes a `_runtime` suffix, as `emplace_runtime` already did, which is how
  eight submitting forms come out of four names. Upstream's `TaskParams`
  overloads collapse onto the unnamed forms: they differ only in the name they
  give the node, and the observers that would read one are step 6.
- **An async task is one `Arc`-owned free-standing node, and its job names a
  sentinel run** (issues #86, #105, #376). Upstream's async node is an ordinary
  `Node` out of its object pool with `_topology == nullptr`, held alive by a
  reference count packed into the low 24 bits of the node's state word. A
  `Node` of a taskflow here is a slot in a `Graph`'s arena, so an async task
  holds its one node *inline* instead — `AsyncNode` owns a `Node` and no
  arena — and the `Arc` around it *is* upstream's count: handles share it,
  every queued job of the node shares it, and the node goes when the last
  share goes — no second counter to keep in step, and the recycle call upstream
  has to get right is a *check* here rather than an obligation (the entry
  below). That leaves the null topology, which
  every job in the scheduler reaches through without asking whether it is
  there. Making the field an `Option` would have put a `None` case into the
  worker's dispatch, into every arm that schedules a nested graph, into the
  corun path and into the tear-down, so a free node's job names one
  process-wide **sentinel** run instead — one that never finishes, is never
  cancelled, is nobody's to wait on and holds no taskflow. The one thing the
  sentinel must not be is an *anchor*: a panic whose parent chain roots there
  ends the walk rather than being delivered, because claiming it would set the
  exception bit of a run that every async task in the process reads on its way
  in. That is #105, where one failed `silent_async` skipped every async task
  submitted after it for the life of the process and the next `wait_for_all`
  never returned.

  What the shape costs was measured rather than argued: an externally
  submitted async task was ~415 ns, of which 87% was the three heap blocks the
  node then was — the `Arc`, the arena inside it and the body's box.
  [`BENCHMARKS.md`](BENCHMARKS.md) has the ablations under "The #125
  attribution", which priced a block at ~70 ns on the thread that allocates it
  and ~700 ns when a worker frees what a producer allocated, and the section
  after them has the first change they named: the arena was sized to the
  single node it held (issue #144), 528 of the 848 bytes a task and 6% of the
  submission path. #376 is the second and takes the arena away altogether, so
  a free node is **two blocks, the `Arc` and the body's box** — the body stays
  because it is type erasure — and the two were measured to be one too many
  rather than argued down. Of the two ways to fold the arena into the `Arc`,
  the one taken is the second: **a free node's job resolves its node through a
  field rather than by index into a graph**, where the rejected alternative
  was a `Graph` that can hold one node inline, which is a discriminant and a
  branch on every resolution by index in every graph in the crate for the one
  case that needs it. What that choice costs is paid by `GraphOwner`: its
  async arm answers `node()` from the field and has no graph to answer with at
  all, so `graph()` is an `Option` on the three paths every job reaches — the
  node's own dispatch, the observer views, and `runs_inside` — and panics on
  the paths that are a graph's alone, which are `set_up_graph`, a successor
  release and `Runtime::schedule`'s bypass target. A free node has no
  successors in a graph and no predecessors to call weak, so the `None` on
  those three paths is the answer an empty graph would have given. The
  allocation count is a test rather than a figure
  (`src/executor/async_node.rs`); what taking the third block away was worth
  is [`BENCHMARKS.md`](BENCHMARKS.md)'s "#382 measurement", which counted the
  blocks either side of the change and A/B'd six shapes across it. A free
  async task is **2.0159 blocks and 312 bytes** where it was 3.0159 and 344,
  an externally submitted one is ~350 ns pinned where it was ~415, and the one
  block is **7 to 16% of a task** at one worker on every shape that
  allocates one — 12 to 14% at 24 workers on the shape that is nothing but
  submissions, and nothing at 24 workers on a nested recursion, where #380's
  handoff is what a task costs instead. The `Arc` grows from 88 bytes to 256
  as it swallows the node, so what the change bought is the block's existence
  and not its bytes, which is the proposition that run was built to
  separate.
- **A worker recycles the free node it finished, on a bounded list of its own**
  (issues #379, #397). Upstream's `TF_ENABLE_TASK_POOL` build allocates every `Node`
  from a process-wide `ObjectPool`: `tf::animate` pops a block off one of its
  shards' lock-free Treiber stacks and constructs into it, `tf::recycle`
  destroys the object and pushes the block back onto the shard its own header
  names, and nothing goes back to the system allocator until the pool is
  released. What that defends against is exactly the regime the async family
  is in — the thread that submits allocates the node and a *worker* frees it —
  which [`BENCHMARKS.md`](BENCHMARKS.md)'s "#125 attribution" priced at ~700
  ns a block at 24 workers against ~70 ns on the thread that allocated it.
  This port gives the one node kind that is allocated per task the same
  defence, and four things about it are its own.

  **The list is one per worker and shared with nobody.** It lives in the
  `WorkerThread` frame behind a `RefCell`, not in a global behind a CAS:
  upstream shards *because* its pool is global and every thread animates out
  of it, where a list only the owning worker's thread can reach needs no
  atomic at all. The submission path that draws from it is the one that
  already names a worker — a body spawning on the worker running it — so
  nothing new had to be threaded through to find the list. The price is that a
  node finished on worker 3 is only ever reused by worker 3, where upstream's
  blocks circulate between threads, and that an external submission
  (`Executor::silent_async`, a task group's) names no worker and allocates as
  it always did.

  **A block is parked only when the `Arc` says the last share has gone**,
  which is the `Arc::get_mut` in `FreeNodes::recycle`. Upstream's
  `recycle(Node*)` is an unchecked promise — the caller swears the node is
  dead, and its `AsyncTask` reference count having reached zero is what makes
  that true. The same node here may still be named by an `AsyncTask` handle or
  by the parent chain of something still queued, so the test is what lets the
  recycling add no `unsafe` and no invariant a reader has to learn: a node
  somebody can still reach is dropped exactly as it was before. The clone that
  is tested is taken before the job is spent and tested after, which is the
  whole protocol (`WorkerThread::spend_job`).

  **A list nothing has drawn from parks nothing**, which is what keeps the
  recycling off the routes it cannot serve. `animate` records the draw
  whether or not the list had a block to give, that call being what proves a
  submission names this list at all, and until it has `recycle` hands every
  node back to the allocator. Without that condition a worker whose children
  came through `Executor::task_group` -- a route that names no worker, so
  never draws -- parked blocks nothing could take out again, and the backing
  `Vec` it parked them in cost a heap block per corun frame, which is a block
  per *task* on a shape that coruns once per child.
  [`BENCHMARKS.md`](BENCHMARKS.md)'s "#397 attribution" priced that at 9.6
  and 13.7% of a task on `integrate`'s two one-worker ids and 11.5% on
  `fibonacci`'s group recursion, against the `Arc` clone the protocol above
  costs in `WorkerThread::spend_job`, which the same ablation priced at
  nothing at all (+0.2% and +0.6% for removing it) and which therefore stays
  as it is.

  **The list is bounded and dies with its worker**, where upstream's pool is
  unbounded until `release` and outlives every executor. 32 blocks is what
  stops a burst of submissions from parking memory for the life of the
  executor, and a node is reset at the moment it is parked rather than at
  reuse, so it holds no body, no name and no successor job while it waits. The
  body's own box is deliberately not pooled: it is the type erasure, so its
  size is the body's and not the node's.

  What the list is worth was then measured
  ([`BENCHMARKS.md`](BENCHMARKS.md)'s "#382 measurement"), and the answer is a
  **footprint and not a time**: a child submitted through a `Runtime` costs 3
  heap blocks and 144 bytes where it cost 4 and 400, its node drawn from the
  list for all but one child in 121392 at one worker, while the two 24-worker
  ids of that recursion read −0.6% and +0.3% against sides inside 2% of
  themselves — a resolved no change. A block allocated and freed by the same
  thread costs about what this list's own `RefCell` borrow, `Arc::get_mut` and
  `Vec` push cost; the regime upstream's pool defends against is the
  cross-thread one, which that file's "#125 attribution" priced at 691 to
  737 ns a block. Off the route that draws the same change moved ids by −14%
  to +15% with no consistent sign until the condition above was added, which
  is what that range was: the parking, on lists that could not be drawn from.
  With it, `integrate`'s two one-worker ids are back inside the spread of the
  side before #379 -- +2.0% and +3.2%, against sides disagreeing by 5.5% and
  6.6% -- and the recursion that does draw is unmoved, its four ids reading
  −1.7% to +0.7%. The tests (`src/executor/async_node.rs`) assert which block
  the second submission landed in, and that a list nothing has drawn from
  parks none, rather than what either saved.
- **`AsyncTask` is that `Arc`, so `use_count` counts the executor's shares
  too** (issues #86, #91). Upstream's handle is a `Node*` plus `_incref` and
  `_decref` on every copy, move and destruction; here it is an
  `Option<Arc<AsyncNode>>`, so `Clone` and `Drop` *are* the counting. What the
  packing bought upstream is given up on purpose: a 24-bit ceiling on owners
  and a count only a dependent-async node may be asked for, against an `Arc`'s
  word in the same allocation, with no ceiling and no new mechanism for a
  reader to learn. The visible consequence is `use_count`, which answers
  handles **plus** the shares the scheduler holds — a queued or running task is
  named by a job, and a job is a share like any other. Upstream's count
  includes exactly the same thing, which its own documentation says from the
  other side, so the number agrees; it is only more obvious here where it
  comes from.
- **A free node's successor list is a `Mutex`, and its join counter starts at
  one** (issue #91). Three decisions one protocol forces, because a
  dependent-async graph is built one task at a time *while the tasks already
  submitted are running*. First, upstream guards a node's `_edges` with the
  `ESTATE::LOCKED` spin bit and sets `ESTATE::FINISHED` in the same word, so
  that naming a dependency and finishing one cannot interleave; here both live
  behind one `Mutex` on the node, which is the same statement — the critical
  section is a push or a `mem::take` and never user code, so what the two
  differ in is only whether the loser of the race spins or parks, and parking
  is the better answer when the holder can be descheduled. Second, upstream
  knows how many dependencies there are before it looks at any of them, by
  `std::distance` on a random-access iterator pair; a Rust `IntoIterator` may
  be single-pass, so the counter starts at one for the submission itself and
  that guard is spent with everything already met in one final `fetch_sub`.
  Without it a task whose first dependency finished early could run and
  *finish* before its second was named. Third, the count against the parent is
  taken once at submission rather than beside the push, because the push may
  happen on another thread at any time after the first dependency is named —
  which is upstream's arrangement too, from the other side: its
  `Runtime::dependent_async` raises the counter itself before calling the
  internal form.
- **`Executor::drop` waits for free async tasks on a second counter** (issues
  #88, #115). Upstream's destructor calls `wait_for_all`, which waits for
  every kind of work at once; this port can tell two kinds apart and must. A
  run in flight is a run some `RunHandle` borrows the executor for, so the only
  way to reach `Drop` with one outstanding is to have *leaked* that handle —
  and the documented answer to a leaked handle is that the pool still comes
  down. An async task is the other way round: its handle is `'static`, dropping
  one detaches and a `silent_async` has none at all, so nothing but the
  executor's own destructor can keep the pool alive long enough to run a queued
  body. Waiting on `num_topologies` would have cost the leaked handle its
  answer, so there are two counts: a free async task is in both, and
  `Executor::drop` waits on the narrower one. The foot-gun that comes with the
  wait is upstream's too — dropping the last share of an executor from inside
  one of its own async tasks deadlocks. A *parented* async task is in neither
  count (#115): a child of a runtime body or of a task group is accounted for
  by the join counter of the parent that is waiting for it, so
  `wait_for_all` does not wait for one. Counting it in both was a real bug
  rather than a tidiness question — a child counted at submission and
  uncounted nowhere left `wait_for_all` and `Drop` waiting for ever.
- **A failure out of a free node goes where the submission promised, and
  nowhere else** (issues #88, #91, #105). A `spawn` body's panic goes into its
  own handle, because the body the node runs is the wrapper the handle was
  built with — upstream's `std::packaged_task`, whose executor's own exception
  handler never sees such a failure either, the packaged task having swallowed
  it first. A `silent_async`'s has no handle and no anchor: the
  payload lands on the node the task is, where upstream parks it too, and goes
  with the node one share later. A *silent dependent-async*'s lands in the same
  place and is readable, because that node is still named by an `AsyncTask`
  handle: `AsyncTask::take_panic` is upstream's `exception_ptr()` on the same
  node, and the tear-down parks the payload before it sets `FINISHED` so that a
  caller who sees `is_done` can take it. None of the three cancels anything,
  which is the sentinel's third property above.

  A free node that has a *parent* — a runtime body's child, a group's — moves
  the destination rather than the mechanism: the walk takes its parent links
  first, and only the root reached through them decides the case. That is also
  why the walk carries a run along rather than reading
  the failing job's own — a child spawned under a node of a real run holds the
  sentinel on its own job while the chain above it roots at the run that is
  actually waiting (#105, #115).
- **One place in the crate builds an `AsyncHandle`, and a submission belongs
  to the shared state rather than to the `Executor`** (issues #90, #92).
  Upstream's handle-returning form is a `std::packaged_task<R()>` plus the
  `get_future()` beside it, and the port keeps them a *pair*: one call wraps
  the body and hands back the wrapper together with the handle its outcome
  will arrive at, because a promise and a future are two ends of one slot and
  handing out either alone is not a thing a caller should have to arrange. A
  runtime-shaped task needs only the handle end — its outcome is decided on
  the entry that *finishes* the node, out of the node's payload slot rather
  than out of a return value — so that half is a second constructor on the
  same seam rather than a second way of making a handle. And the submitting
  code lives on the executor's shared state, with the public `Executor`
  methods two lines over it: a `Runtime` body holds the worker's share of that
  state and no `Executor` at all, for the reason the entry above on
  `rt.executor()` gives, so a submission that lived on the `Executor` would
  have been a submission a task body could not make.
- **One async core on `impl Shared`, and three receivers that delegate**
  (issue #133). The async family is upstream's overload set over three
  receivers, and writing out what varies across it answers where the code
  belongs. The matrix has four axes -- receiver, dependent-or-free,
  handle-or-silent, plain-or-runtime body -- and 24 public methods, which is
  every combination; of those axes only the first three reach the scheduler,
  because the body shape and the handle decide the same thing. So the core is
  **two functions over two parameters**: `Shared::schedule_async_task` and
  `Shared::schedule_dependent_async_task`, each taking a `Spawn<H>` -- the
  work, the estate word and the handle the call hands back, built by one of
  four constructors for the four body shapes -- and a `Submission` -- the
  parent the child is accounted to and the worker whose own queue its job goes
  onto, built by one of three constructors for the three receivers. A third
  function, `Shared::new_free_node`, is the half both submissions share.
  Every `pub fn` of `Executor`, `Runtime` and `TaskGroup` is then one
  delegation, and the layer that was written twice -- `spawn_child`,
  `spawn_dependent_child` and `child_parent` in `executor::runtime_async`
  against their twins in `src/task_group.rs`, plus two spellings of the
  promise-and-future pair -- is gone rather than shared, each of those names
  now appearing nowhere in the crate.

  The receiver is `Shared` because a `Runtime` holds `Arc<Shared>` and no
  `&Executor` by the design the entry above on `rt.executor()` sets out, which
  is also why the two `Executor::schedule_*_async_task` forwarders are gone:
  what they added was a `None` worker, and that is now what
  `Submission::parentless` says. This extends the entry above on one
  `AsyncHandle` construction site rather than replacing it -- both of its
  claims still hold, and the four body shapes are now the only callers of that
  seam.

  The three invariants of submission each live in exactly one function, which
  is the point of the consolidation rather than a side effect: the routing
  decision that puts a child of a task body on the submitting worker's own
  queue (#119) is `Submission::queue`; the rule that only a *parentless*
  submission touches the executor's two counters (#115) is
  `Shared::new_free_node`, and the tear-down in `executor::async` asks the
  same question on the way out; and the join-counter guard a dependent
  submission starts at one (#91) is the one function that has a dependency
  loop. Before this each of the three was open-coded per receiver, which is
  how all three became bugs. No behaviour changed: the identical test-name
  set, the identical `pub fn` list over the five files and the suite are what
  says so.
- **A free async task is named by a `_named` twin of its submitting method,
  and the name rides in `Spawn`** (issue #258).

  Upstream's shape at the pin is an *optional leading argument*, not a second
  verb: `async(f)` forwards to `async(DefaultTaskParams{}, f)`, and
  `TaskParamsLike` admits `TaskParams`, `DefaultTaskParams` or anything
  `StringLike`, so `executor.async("async1", f)` and
  `rt.async("my task", f)` are one overload of the same four names. C++
  spells "optional" as an overload and Rust has no overloading, so the
  rendering is a second method name: `spawn_named`, `silent_async_named`,
  `dependent_async_named` and their `_runtime` twins, on each of the three
  receivers, which is twenty-four new methods beside the twenty-four already
  there. Each is its unnamed twin plus one step, because the name travels in
  `Spawn<H>` rather than as a parameter of `Shared::schedule_async_task` --
  which makes that value upstream's leading `(params, f)` pair exactly as
  `Submission` is its trailing `(tpg, parent)` pair, and leaves the unnamed
  twenty-four, the submission core's signature and every existing call site
  untouched. This supersedes the last sentence of the entry above on `async`
  being `spawn` (#88): the `TaskParams` overloads no longer collapse onto the
  unnamed forms, because the observers it deferred them to arrived in M7 and
  `TaskView::name` is now something to read a name back with.

  The alternative was a submission builder, `executor.named("a").spawn(f)`,
  with one `named` step on each receiver that every existing method is reached
  through. By method count it is the smaller surface and that is the one thing
  it wins: a single public builder holding a private three-variant target --
  parentless, under a node, under an anchor -- carries eight methods where the
  twins carry twenty-four. Three things decide against it. It adds a **public
  type no program using the crate ever writes**, which is precisely the test
  the entry on root re-exports under "Documentation and tooling" applies to
  earn a root line, so the builder would need a root line to be documented at
  all while failing the rule that hands them out; a `_named` method adds no
  type and no name to place. It **separates the name from the call it is an
  argument of**: `spawn`/`spawn_named` and
  `spawn_runtime`/`spawn_runtime_named` sort next to each other on the same
  rustdoc page, so a reader who found the unnamed form has found the name,
  where a builder puts the named forms on a page reached only through a verb
  that sorts nowhere near them. And the thing a builder is actually for --
  more than one optional parameter -- **is not there**: `TaskParams` has two
  fields, and the second, `void* data`, does not survive translation at all,
  user data in Rust travelling in the closure. If a second parameter ever does
  arrive, a builder is the shape to revisit and these twins are its front end
  rather than an obstacle.

  The default is deliberately unchanged: an unnamed submission names its node
  the empty string, which is what `DefaultTaskParams` leaves `Node::_name`
  as upstream, and the node constructors in `src/graph/` already took a name
  — nothing there moved for this.

- **A scoped spawn, and the crate's second `unsafe` module** (issue #164).
  `Runtime::scope` is this port's own addition -- upstream has nothing to
  translate -- because the existing async family
  (`executor::runtime_async`) only ever hands a body `Send + 'static` work,
  so a runtime task that wants a handful of children to write into a slice
  it already owns has had to put that slice behind an `Arc` for no reason
  but the bound. The alternative that stayed on the table was *nothing*:
  keep every body `'static`, as the crate has since M1, and leave a caller
  to reach for `Arc` itself. That is declined because the cost then falls on
  every caller of the pattern rather than on the one place that removes it,
  and because Rayon's `scope` is exactly this shape, already proven in the
  ecosystem it was borrowed from.

  The erasure that makes a borrowed child sound to hand to
  `Runtime::silent_async` is one `unsafe` transmute, in a module of its own
  (`src/executor/scope.rs`) rather than folded into `executor::runtime`, so
  that "no `unsafe` outside N modules" stays a short, auditable list rather
  than a search through every module that touches a `Runtime`. The module's
  own `# Design` section has the argument for why the erasure is sound here
  specifically -- a `RefCell` rather than `Sync` closes off the one
  cross-thread attack `std::thread::scope`'s identical invariant marker
  defends against, so nothing spawned through `Scope` can ever reach the
  same scope a sibling is borrowing from -- and `tests/loom_scope.rs` is the
  model, checked against the mutation of skipping the join `scope` performs
  on every path out of it.

## Observers

- **The observer list is one atomic check on the path with no observers, and a
  mutex only when there is work to do** (issue #167). Upstream's own
  `_observer_prologue`/`_observer_epilogue` iterate `_observers` -- an
  `unordered_set` -- under no lock at all, relying on nothing ever mutating it
  while a run is in flight, which upstream's own docs do not promise and this
  port will not assume. The crate-private `ObserverList` instead keeps a plain
  `AtomicUsize` count beside the list: the empty check both scheduling seams
  open with is one relaxed load, and `ObserverList::insert` and
  `ObserverList::remove` -- called rarely, never from a worker's hot path --
  are the only two callers that take the list's mutex to write it. A non-empty
  seam takes that same mutex once, to clone the registered `Arc`s into a `Vec`
  of its own, and releases it before calling anything, so an observer that
  calls back into `make_observer` or `remove_observer` cannot deadlock against
  the seam that is visiting it. The same issue scopes what a `WorkerView`
  answers about a worker's queue down to `is_queue_empty` alone, repeating
  exactly what `Worker` already exposes rather than a second, inconsistent one
  -- not because upstream's `queue_size()`/`queue_capacity()` pair turns out
  to be unanswerable, but because answering either half needs
  `src/executor/queue.rs` and a wider `Worker` surface, both outside this
  issue's own files. Issue #206 is the follow-up those two files were left
  for, and the two halves turn out to need different answers: see the entry on
  `queue_size` versus `queue_capacity` under "Executor core" for which.
- **An observer epilogue skip is folded into the exhaustive dispatch
  match, not read from a side `matches!`** (issues #211, #212). Two handle
  kinds, `Handle::Async` and `Handle::DependentAsync`, tear themselves down
  and signal a waiter from inside their own dispatch arms, before
  `WorkerThread::invoke_one`'s generic post-match `observer_epilogue` call
  could otherwise run -- so those two arms call it themselves and
  `invoke_one` must skip its own call for them. Spelling that skip as a
  `matches!` beside the exhaustive `match` that already dispatches on the
  handle left the two lists able to drift: a later `Handle` variant that
  also signals a waiter from inside its own tear-down could compile without
  being added to it, silently reintroducing the exact missing-`on_exit`
  defect #167 found. The fix makes each dispatch arm return an `Epilogue`
  value instead -- `Pending` if the generic call still has to run,
  `AlreadyRun` if the arm already made it -- read back from the same
  `match`'s result rather than re-derived from the handle a second time, so
  a new variant is a compile error there until it states which it is. A
  second exhaustive match on `Handle` in `src/graph.rs` was considered and
  declined: it would answer the question in a different place from the one
  that already knows it, which is the two-places-to-keep-in-step problem
  this entry exists to close rather than move.
- **`TFProfManager` is not ported** (issue #193). `tfprof.hpp` at the pinned
  v4.1.0 tag builds a process-global singleton around `TfProfObserver`,
  constructed with `_fpath{get_env(TF_ENABLE_PROFILER)}`; its destructor
  opens that path and, if it opens, writes a complete `.tfp` file there, or
  prints a text summary to stderr otherwise. A library has no business
  installing a process-global singleton whose destructor writes a file to
  a path an environment variable names -- that is an application's choice
  to make, not something this crate should do on a caller's behalf without
  being asked. The port exposes `TfProfObserver::dump` and
  `TfProfObserver::summary` directly and leaves the sink to the caller, as
  every other `Observer` in this crate does.

  Preserved as documentation rather than code: a caller who wants a real,
  multi-executor `.tfp` file -- several `TfProfObserver`s, one file -- can
  still build exactly the one upstream's `TFProfManager::dump` writes
  around this port's own `dump` output. Its 12-byte file header, all
  integers little-endian: `"TFPX"` (4 bytes, the magic), `version` (`u16`,
  `1` at the pin), `flags` (`u16`, `0` at the pin), `num_exec` (`u32`, how
  many executor blocks follow). After that header, each `TfProfObserver`'s
  own self-contained block -- exactly what `TfProfObserver::dump` writes,
  with no shared state between them -- one after another, in whatever order
  the caller chooses.

## Algorithms

- **The index algorithms take a slice or an `IndexRange`, never a generic
  iterator pair** (issues #166, #170, #171). Upstream's `for_each`,
  `for_each_index` and the rest are templates over `B`/`E` iterator pairs
  that may be `std::ref`-wrapped so a task re-reads them on every run.
  `IndexRange<T>` ports upstream's one-dimensional `IndexRanges<T, 1>` for
  the integer domain; `Runtime::for_each` takes a borrowed `&mut [T]`
  instead for the slice domain, each caller's own module arguing why a
  borrowed slice reproduces that same lazy-fetch behaviour without a
  reference-wrapper vocabulary Rust has no equivalent for. Multi-dimensional
  `IndexRanges<T, N>` for `N > 1`, and a signature generic over an arbitrary
  splittable iterator the way Rayon's `IndexedParallelIterator` is, are both
  declined: every algorithm this milestone ports reads one contiguous slice
  or one integer range, and a caller with something else first turns it
  into one of those two shapes. `FlowBuilder`'s owned-data wrapper over each
  (`ForEachBuilder` and the family beside it) takes the ownership question
  off a caller who has no `&mut [T]` or range to hand a runtime task already
  borrowing one.
- **`find_if`, `find_if_not`, `min_element` and `max_element` return an
  index, not an iterator** (issue #177). Upstream returns an iterator into
  the input, `end` meaning "not found"; translating `end` needs the same
  iterator-pair vocabulary the entry above declines, so the result is
  `Option<usize>` instead, `None` where upstream returns `end`. A caller
  that wants the element back indexes `data` with it.
- **`IndexRanges<T, N>` extends the one-dimensional range to `N` dimensions,
  and `discrete_domain` only ever narrows the outermost one** (issue #192).
  `src/algorithm/index_range.rs`'s own `# Design` section has the full
  argument -- why `IndexRanges<T, N>` is its own const-generic struct rather
  than `IndexRange<T>` aliased to `N == 1` (Rust has no `if constexpr`-shaped
  gate inside one generic-over-`N` `impl` block, so a method only the `N == 1`
  case has cannot live in a block that type-checks for every `N`), and why
  narrowing a flattened `[part_beg, part_end)` chunk back to a rectangular
  sub-range only ever touches dimension 0 (any other split produces a
  staircase, not a box). `for_each_by_index_nd` is `for_each_by_index`'s own
  N-dimensional counterpart, dispatching over that same outermost dimension
  so every worker's chunk stays one hyper-rectangle rather than a scattered
  set of cells; `src/algorithm/for_each_index.rs`'s own module docs have the
  rest. Recorded here only because this issue's own acceptance names an
  entry for it -- the rationale itself stays in the two modules that need it,
  per the documentation contract's rule that it lives once.
- **Three layers, not two forms: the `Runtime::` method and the `FlowBuilder`
  wrapper both stay, and the `make_*_task` factory is the middle layer the
  port is missing** (issue #257).

  `docs/api-friction.md`'s "Algorithms (step 6): two forms where upstream has
  one" is the evidence for this entry, and the first thing reading upstream
  does to it is correct its framing. Upstream does not have one shape; it has
  two, and the one it treats as primitive is the one this port lacks. Every
  `FlowBuilder` algorithm method in `taskflow/algorithm/*.hpp` at the pin is
  literally `return emplace(make_X_task(args...));` -- in `for_each.hpp`,
  `transform.hpp`, `fill.hpp`, `generate.hpp`, `reduce.hpp`, `find.hpp`,
  `sort.hpp`, `scan.hpp` and `merge.hpp` alike, without exception. Upstream's
  primitive is therefore the factory, a cold callable over `tf::Runtime&`,
  and its `FlowBuilder` method is one derived line over it. What upstream has
  no counterpart for at all is this port's *other* form: `tf::Runtime`
  carries no loop, sort, reduce or scan members, so `Runtime::for_each` and
  its 27 siblings are this port's own invention.

  The port has upstream's derived layer and a primitive of its own, and is
  missing the layer in between. It needs three where upstream needs two for
  one reason: upstream's `make_for_each_task(b, e, c, part)` is at once a
  `'static` callable and a borrow of the caller's container, which compiles
  only because its iterators are unchecked pointers and nothing verifies the
  container outlives the task. In Rust a `'static` callable cannot hold a
  `&mut [T]`, so the owned/borrowed split is forced and the only open question
  is where the seam falls. It falls here: `Runtime::X(&mut [T], ..)` is the
  borrowed primitive a body that already holds a live slice calls directly,
  `make_X_task(owned, ..) -> impl for<'a> FnMut(&mut Runtime<'a>) + Send +
  'static` is the `'static` callable, and `FlowBuilder::X` is one
  `emplace_runtime` line over the factory. Counting the surface at the time
  of the decision: 28 `Runtime::` methods, 28 `FlowBuilder` wrapper methods
  across eight extension traits, and 11 factories -- three for
  `for_each_index`, two for `merge`, six for `scan` -- so 17 are missing and
  the completed family is 1:1 with the wrappers.

  **Neither existing shape goes.** Both carry real weight in the corpus: of
  the 103 algorithm call sites in `tests/`, `examples/` and `benches/`, 62
  call a `Runtime::` method and 41 call a wrapper, and the split is not
  uniform per algorithm -- `find` is 15 to 0 (its four wrapper methods have
  no call site outside `src/` at all, only a unit test and four doctests),
  while `scan` is 2 to 14 and `tests/scan.rs` is wrapper-only, 12 calls with
  no `Runtime::` call anywhere in the file. Dropping the wrapper would make
  `scan` and `for_each_index` cost a hand-written closure per use at a dozen
  sites that read like upstream today; dropping the `Runtime::` method would
  delete the implementation every wrapper is built on and the only form that
  reaches a `&mut [T]` a body already owns without a lock. The friction log's
  own reading survives intact -- `taskflow.sort(Arc::clone(&data))` reads
  almost exactly like upstream's `taskflow.sort(beg, end)`, and
  `runtime.sort_by(&mut local, cmp)` is nicer than anything upstream can
  write -- and the honest conclusion is that those two sentences describe two
  different jobs rather than two candidates for one.

  **The factory family is how a dependent-async body gets the algorithms**,
  and completing it is the work this decision commissions. The gap the
  friction log calls the widest it found is real: all three stages of
  `examples/dependent_async_algorithm.rs` hand-lock an `Arc<Mutex<_>>` and
  call a `Runtime::` method, because the wrapper builds through
  `FlowBuilder::emplace_runtime` and a dependent-async task is never queued
  into a `Taskflow` for that to call. The factory closes it with nothing new
  invented: `impl for<'a> FnMut(&mut Runtime<'a>) + Send + 'static` is
  accepted by `emplace_runtime`, by `Executor::spawn_runtime` and
  `silent_async_runtime`, and by `dependent_async_runtime` and
  `silent_dependent_async_runtime` whose bound is the weaker `FnOnce`. The
  precedent is already tested: `src/algorithm/module.rs`'s
  `a_module_task_runs_from_a_dependent_async_body` submits
  `make_module_task` straight through `Executor::dependent_async_runtime`.
  The one argument on record against a factory for the other families --
  `examples/dependent_async_algorithm.rs`'s claim that a `Runtime::` method
  is "not a callable a factory can hand back cold, because the chunk loop
  underneath every one of them is built on `Runtime::scope`" -- does not
  hold, and `make_inclusive_scan_task` is the counterexample sitting in the
  tree: it hands back exactly such a callable, and `Runtime::inclusive_scan`
  underneath it calls `rt.scope`. A factory's product is handed its
  `&mut Runtime` when it *runs*; it never needed one to be built. The
  seventeen factories, each beside the `Runtime::` method it wraps and with
  the parameter list its wrapper already has, are filed as #292
  (`make_for_each_task`, `make_sort_task`, `make_sort_by_task`), #293
  (`make_transform_task`, `make_transform2_task`, `make_fill_task`,
  `make_fill_n_task`, `make_generate_task`, `make_generate_n_task`), #294
  (`make_reduce_task`, `make_transform_reduce_task`,
  `make_transform_reduce2_task`, `make_reduce_by_index_task`) and #295
  (`make_find_if_task`, `make_find_if_not_task`, `make_min_element_task`,
  `make_max_element_task`), with #296 rewriting the example on them -- the
  check that the gap actually closed, and the place the stale claim above
  gets corrected. Each of the four also reduces its wrapper methods to the
  `self.emplace_runtime(make_X_task(..))` one-liner `ScanBuilder` and
  `ForEachIndexBuilder` already are, which is what makes the derived layer
  cheap enough to keep.

  **The `Arc<Mutex<_>>` is accepted, and the absence of a `std::ref(range)`
  counterpart is permanent rather than pending.** Upstream's reference wrapper
  is sound there only because its graph edges are trusted by convention:
  `make_for_each_task(std::ref(beg), std::ref(end), ..)` hands the task an
  unchecked alias into a container the caller still mutates, and what keeps
  that from being a data race is the programmer's claim about the dependency
  edges, nothing the compiler reads. Reproducing it would need `unsafe` and
  would hand a caller exactly that footgun, which this port's stated posture
  refuses; a lock taken once per invocation -- not once per element -- is the
  price, and `src/algorithm/for_each_index.rs`'s "No deferred
  `beg`/`end`/`step`/`range`" `# Design` section already names the same gap
  from the implementation side. Two measurements make the cost smaller than
  the friction log's framing suggests. It is a *slice* algorithm's cost, not
  the family's: of the 28 wrappers, 23 take at least one `Arc`, two (`merge`,
  `merge_by`) take owned `Vec`s and return the slot they publish into, and
  three -- the `for_each_index` trio -- take no data at all, since an index
  range is `Copy` and travels by value. And the port has a form upstream does
  not for the case that actually chafes: a caller who wants no lock computes
  the slice in a `Runtime` body and calls the `Runtime::` method on it, which
  is what `tests/for_each.rs`'s stateful cases do in place of upstream's
  `std::ref` ones. The rule the seventeen new factories follow, so the family
  stays one vocabulary rather than three: a value the body only reads is any
  `SliceSource` -- `Arc<Vec<T>>`, `Arc<Mutex<Vec<T>>>` or an owned `Vec<T>`,
  the read side the entry below on a factory's read parameter states and the
  reason it is wider than one `Arc` -- one it writes is `Arc<Mutex<Vec<T>>>`,
  a scalar result is `Arc<Mutex<T>>`, a `Copy` range or index travels by
  value, and a factory that *creates* the output storage returns `(body,
  slot)` as `make_merge_task` does.

  **What #216 inherits.** It is collapsing the *derived* layer -- 28 one-line
  wrappers once #292 through #295 land -- not a primitive, which is why the
  collapse is cheap and why it does not have to wait for those four: a factory
  changes a wrapper's body, never its signature. It should keep all 28 methods
  (none is redundant; the counts above show which algorithms lean on which
  form, not that any method is unused), fold the eight traits into one whose
  every method is a single `emplace_runtime` call, and, per #256's entry under
  "Documentation and tooling", flatten `rustflow::algorithm`'s own front door:
  the collapsed trait, the four partitioners and their `ClosureWrapper`,
  `IndexRange` and `IndexRanges`, and the `make_*_task` family. The collapsed
  trait replaces the eight prelude lines with one; the factories are *not*
  prelude names, which is #256's boundary applied unchanged -- a program
  importing a free-function form is choosing it, and chooses it by name.

- **A factory's read parameter is one sealed source trait, and the family's
  locking discipline is "every guard in parameter order, none past the body's
  return"** (issue #318).

  The ownership rule above gives every *writing* factory an
  `Arc<Mutex<Vec<T>>>` and gave every *reading* one an `Arc<Vec<T>>`, and the
  two are not interconvertible without copying the data. Across the eleven
  factories with a read parameter -- `transform`, `transform2`, the six
  scans, the three `reduce` forms that take a vector, the four finds and the
  two merges -- that made the family unable to express a chain: the longest
  one statable was for-each/sort into for-each/sort, the only factories that
  read and write the same locked vector, and no factory could terminate a
  chain by reducing what an earlier stage wrote. The program #296 specifies
  compiled for its first stage and failed for its second and third with
  `expected &Arc<Vec<_>>, found &Arc<Mutex<Vec<i32>>>`.

  **What lands is `SliceSource`**, a trait sealed the way
  `algorithm::SlicePartitioner` and `index_range::IndexInt` are, with an
  `Item` associated type and a GAT guard that derefs to `Vec<Item>`:
  `&'a Vec<T>` for `Arc<Vec<T>>` and for an owned `Vec<T>`, and
  `MutexGuard<'a, Vec<T>>` for `Arc<Mutex<Vec<T>>>`. Every read parameter of
  those eleven factories takes it -- fifteen parameters, the whole read side
  rather than only the three factories #296 needs, since a half-converted
  family is a worse API than either end state -- as
  `impl SliceSource<Item = T>` in argument position rather than as a named
  type parameter, which is what keeps every call site, doctest and turbofish
  inferring unchanged and leaves all 28 `AlgorithmBuilder` wrapper signatures
  byte for byte as they were. The guard is why this is a trait rather than a
  `From` conversion: a locked source cannot hand out a `&[T]` without
  something owning the lock for the duration of the read, and that something
  is the guard the body holds for its own run.

  **The alternative was a second `_locked` factory per read parameter**, and
  it is rejected: it doubles eleven factories into two families a caller
  picks between by name, each wrapper would still call only one of the two,
  and the pair would have to be kept in step for every later change to
  either. An `Arc<RwLock<Vec<T>>>` source was considered too, since a
  read-write lock would let two *reading* stages run at once where a `Mutex`
  serialises them. It is not taken: every writing factory publishes into an
  `Arc<Mutex<Vec<T>>>`, so a chain built on `RwLock` would have to convert at
  the seam -- the copy this decision exists to remove -- and a second lock
  flavour in the vocabulary buys concurrency only for the reader-reader case
  an edge already orders.

  **The locking discipline**, stated here and in `src/algorithm/mod.rs`'s own
  `# Design` section. A factory body locks each of its sources once per run,
  in the order its signature lists them, and drops every guard before it
  returns; the find family and `make_merge_task_by` release their read guards
  before taking the lock they publish into, so a read guard is never held
  across a write lock the read itself does not need. Two stages joined by a
  dependency edge therefore cannot deadlock over a lock they share, in either
  lock order, since the earlier body has returned -- and dropped its guards
  -- before the later one is scheduled at all. Two stages that share a lock
  and are *not* joined by an edge are a caller error, two readers included: a
  body holds its guard across the chunk loop under it, that loop's wait runs
  other ready work on the same worker (`WorkerThread::corun_until` steals),
  and `Mutex` has no shared mode, so a worker that picks up the unordered
  second stage blocks on a guard only its own stack could release. Handing
  one `Arc<Mutex<Vec<T>>>` to two parameters of a single factory -- two read
  sources, or a read source and the write target -- is that same error inside
  one call, which is also why an in-place scan stays unexpressible: `input`
  and `out` have to be two different locks, exactly as
  `Runtime::inclusive_scan`'s own `&[T]`/`&mut [T]` pair cannot alias.
- **One per-algorithm `FlowBuilder` extension trait per module, rather than
  one shared trait** (issues #170, #171, #173, #174, #175, #176, #177,
  #178). `FlowBuilder` is a trait, not a struct, so an algorithm's
  owned-data wrapper cannot be added as a second inherent `impl` block the
  way a method on a concrete type could be. Each algorithm module therefore
  declares its own blanket-implemented extension trait --
  `impl<B: FlowBuilder + ?Sized> XxxBuilder for B {}` -- re-exported from
  the prelude beside `FlowBuilder` itself, so a caller with
  `use rustflow::prelude::*;` never has to name any of them. The shape is an
  orchestration decision rather than a design one: it let these issues land
  in parallel without several agents conflicting inside one `impl` block in
  `src/flow_builder.rs`, at the cost of one public name per algorithm that
  exists for no reason but where the parallelism pressure put it. Whether
  to collapse them into one `AlgorithmBuilder: FlowBuilder` now that the
  pressure is gone, or keep the split and call it settled, is issue #216's
  to answer, not this entry's.

- **One `AlgorithmBuilder` for all 28 wrappers, declared in
  `src/algorithm/mod.rs` and flattened at `rustflow::algorithm`'s front
  door** (issue #216).

  This supersedes the entry above on one per-algorithm `FlowBuilder` extension
  trait per module, which named the question and left it here. The eight
  traits it created -- `FindBuilder`, `ForEachBuilder`, `ForEachIndexBuilder`,
  `MergeBuilder`, `ReduceBuilder`, `ScanBuilder`, `SortBuilder`,
  `ElementwiseBuilder` -- are one `AlgorithmBuilder: FlowBuilder`,
  blanket-implemented for every `FlowBuilder` exactly as each of them was,
  carrying all 28 methods unchanged in signature and in body. The eight
  prelude lines are one, and the corpus shows what the split was costing: of
  the 38 files in `tests/` that import the prelude, 37 named none of the eight
  at all, and the one that did -- `tests/for_each_index.rs` -- imported it
  beside a partitioner trait rather than for anything the trait itself said;
  no example and no benchmark imported one. A trait per algorithm was never a
  distinction a caller could use, because none of the eight held a concept of
  its own: the family shares one vocabulary of owned parameters, one
  partitioner set and one lock-once-per-invocation ownership rule, all three
  decided in the "Three layers, not two forms" entry above rather than per
  algorithm.

  **Not moved into `src/flow_builder.rs`.** That is the other shape #216
  offered, and it reads better at first -- zero new names, since
  `FlowBuilder` is already a prelude and a root name -- but it puts the
  algorithm family inside the trait that defines what a task *is*. The
  honest test is what each file then has to know: `FlowBuilder` would gain
  28 methods whose signatures name `SlicePartitioner`, `ReducePartitioner`,
  `FindPartitioner`, `IndexDispatch`, `IndexInt`, `IndexRange` and
  `IndexRanges`, so `src/flow_builder.rs` -- the file `src/graph/`,
  `src/taskflow.rs` and `src/subflow.rs` all build against -- would import
  seven types from the algorithm tree to declare a layer *derived* from it.
  The dependency runs the wrong way: an algorithm wrapper is built on
  `FlowBuilder::emplace_runtime`, and nothing in the builder is built on an
  algorithm. Keeping the wrappers in `src/algorithm/` keeps that arrow
  pointing one way, and the extension trait is what Rust charges for it: one
  name, in the prelude, which a caller writing `use rustflow::prelude::*;`
  never spells.

  **The front door.** Per the prelude/root entry under "Documentation and
  tooling", `rustflow::algorithm` is flattened here rather than at the crate
  root: `AlgorithmBuilder`, the four partitioners with `DefaultPartitioner`,
  `ClosureWrapper` and `DefaultClosureWrapper`, `IndexRange` and
  `IndexRanges`, and the twelve `make_*_task` factories are re-exported from
  `src/algorithm/mod.rs`. The factories are *not* prelude names, which is that
  entry's boundary applied unchanged -- a program reaching for a free-function
  form chooses it, and chooses it by name. The submodule paths all still
  resolve, so no caller lost an import.

  **What moved, and what a reader should expect of the diff.** The 28 method
  bodies are untouched: a wrapper is still one `emplace_runtime` call over
  the `Runtime` method or `make_*_task` factory of the same name, and the
  four issues completing that factory family (#292 through #295) change
  bodies, never signatures, so they land on top of this with no
  re-collapsing. The one thing a `--list` comparison does see is that a
  wrapper's doctest is now named `algorithm::AlgorithmBuilder::sort` where it
  was `algorithm::sort::SortBuilder::sort`: 15 doctest item paths moved,
  which is the trait rename itself and not a test gained or lost, and every
  other name in the 2549 is identical. The per-module `# Design` sections
  that each argued for their own trait are one paragraph in
  `src/algorithm/mod.rs`, where the trait now is.

- **The partitioned chunk loop stays as upstream wrote it, including the chunk
  floor of 1** (issue #343). `for_each_by_index` with a partitioner is the
  splittable source range the "#60 finding" in
  [`BENCHMARKS.md`](BENCHMARKS.md) said this port lacked, so it is the first
  shape where the rayon baseline runs the same program; the question was what
  its per-chunk cost is worth. Counted rather than timed, the answer is that
  there is nothing there: a chunk allocates nothing -- `dispatch` builds one
  free node per *worker*, never per chunk, so 547 chunks cost 23 of them --
  and a chunk is about 15 ns, 1.3% of the run it belongs to. The same chunk
  loop driven by rayon's fan-out instead of this executor's is *faster* than
  rayon's own range split on the same body, which is the measurement that
  retires the question.

  So `GuidedPartitioner`'s `chunk_size == 0 ? 1 : chunk_size` and
  `DefaultPartitioner = GuidedPartitioner<>` keep upstream's values, even
  though the floor of 1 is what makes the default partitioner hand out the
  last 1% of a range a few elements at a time, and even though a chunk size
  large enough to take `make_for_each_by_index_task`'s own
  `n <= part.chunk_size()` early-out beats the parallel path outright below
  about ten thousand elements. A caller who wants either already has the API
  to ask for it; changing a ported default to flatter one benchmark row is the
  trade this declines.

  What the measurement did find is not in this subsystem at all: the `w` tasks
  `dispatch` submits all land on one worker's queue, and 15 to 19 of 24
  workers ever run a chunk body, the rest entering their task after the shared
  cursor is exhausted. That is the steal loop and the notifier, so the next
  measurement belongs to #60's and #102's territory rather than to
  `src/algorithm/`. The counters, the ablations and the harness term that
  turns out to be most of the published gap are in
  [`BENCHMARKS.md`](BENCHMARKS.md) under "The #343 attribution".

- **`SplittingPartitioner` stays an option and `GuidedPartitioner` stays the
  default, at a price now measured** (issue #381). #374 added the port's own
  partitioner -- a chunk task that finds itself on a foreign thread hands half
  its remaining share to a task of its own -- against the fan-out the entry
  under "Executor core" describes, and the question this leaves is whether it
  should be what `DefaultPartitioner` names. It should not. On the four shapes
  that carry a `rustflow-split` id it costs **1.27x to 33.8x** of the shipped
  dispatch at 24 workers, including 1.48x and 1.70x on `mandelbrot`, the
  suite's load-imbalance shape and so the one its mechanism was built for; it
  ties on every one-worker row, which is the mechanism confirming itself,
  since nothing splits without a steal. What it buys is 21 to 24 of 24 workers
  against the guided default's 21 to 22, and the counters attribute even that
  to the static share each task is handed rather than to the re-split: with
  the splits turned off by a high floor the count is unchanged and the wall is
  a third of it. A re-split costs 12 to 16 µs where a fan-out task costs
  2 µs, and **#400 has since attributed that gap to the join a split opens**:
  the far half goes to a `Runtime::scope`, whose `try_corun` cannot park, so
  the waiting worker searches instead and the whole of the splitting default's
  extra CPU is that search. `BENCHMARKS.md`'s "The #400 attribution" has the
  counters, the four candidates it dismisses and the two issues it filed; no
  code changed for it, so the figures in this entry stand.

  **The floor of 1 that `default()` sets is the type's worst setting on every
  shape measured, and it stays** (issue #401). A sweep that holds the chunk
  walk at one body call a share and moves only the floor -- which #381's 4096
  and 65536 rows do not, both numbers being the floor and the walk at once --
  finds one separation in it: the *uncapped* chain a floor of 1 allows costs
  9 to 13 µs of wall a split over the same partitioner with its splits off, in
  three passes, while every floor from `n / (2w)` to `n / (16w)` lands in one
  band that separates neither from its neighbours nor from splitting nothing,
  and no floor that splits comes within 1.5x of the guided default. So there
  is no derived floor the figures pick, and the 9 to 13 µs is the join #400
  attributed and #406 removes, which bounds a split at 1 to 2 µs instead. A
  raise today would tune a shipped default against a cost another change
  takes away and give up the depth of the chain, which is what lets a worker
  arriving late to an already-stolen share take anything; a caller who wants
  the cheap setting asks `new(k)` for it. What #401 changed is the price's
  visibility: `src/algorithm/partitioner.rs`'s `# Design` section now carries
  the three floors and what the default costs, and #409 re-takes the sweep
  once #406 lands. The figures are in [`BENCHMARKS.md`](BENCHMARKS.md) under
  "The #401 decision".

  **The floor stays at one, the join it was priced against is gone, and
  `IndexDispatch::dispatch` grows no handoff parameter** (issue #409; issue
  #406 **declined**). #401's sweep and #400's attribution were both taken
  before #412 gave `WorkerThread::corun_until` upstream's search, and the
  cost they measured was that search: re-taken on the post-#412 tree, a
  re-split prices between **−1.9 and +1.2 µs** of wall against 9 to 13 µs,
  a sign the harness cannot fix, and `SplittingPartitioner::default()` is
  level with the same partitioner with its splits off -- 1.12x, 0.85x, 0.84x
  and 0.97x of it in four processes, every difference inside both rows' own
  spreads -- where #400 measured 2 to 4x on wall and up to 3.5x on CPU. The
  counters say the scope still coruns, one `try_corun` a split as before; what
  changed is that a failed search ends in 32 to 43 attempts instead of 211, so
  the whole run's time in one falls from 15.7-61.9 ms to 0.663-3.125 ms. Two
  decisions follow. The floor stays at one, because the re-taken seven-row
  sweep -- both row orders, six runs a row -- separates no floor from another
  or from splitting nothing by more than either side's spread, while
  `default()` now reads **1.19x and 1.18x** of the guided control against
  #401's 1.99x, 2.48x and 2.25x; `src/algorithm/partitioner.rs`'s `# Design`
  section carries the new price. And **#406's handoff seam is declined**: a
  `dispatch` parameter carrying "how this call site hands a far half on", with
  a detached impl for the three owned entry points and a scoped one for the
  three borrowed ones and for `spawn_shares`, was worth a new parameter and a
  new bound on a public trait method while the join it avoided cost 160 to
  476 µs of CPU a split, and is not worth it now that the join costs what
  #400's four candidates already account for. The `Scope` route stays
  everywhere, so the soundness argument `Runtime::scope` rests on is
  untouched. What is left of the splitting default against the guided one is
  the static share each task is handed, which is the entry above and #343's
  fan-out rather than the join. The four passes, their counters and their load
  tables are in [`BENCHMARKS.md`](BENCHMARKS.md) under "The #409 decision".

- **`sort_by` keeps upstream's live partition, and the sort's own figures do
  not show it** (issues #375, #381). The pinned header defines two sort cores
  and `make_sort_task` calls only `parallel_pdqsort`; the `parallel_3wqsort`
  call beside it is commented out and its pivot picker needs a `sort3` the
  tree does not define, so running the three-way split was the port following
  dead code. #375 replaced it with the live core -- ninther pivot, Hoare
  `partition_right`, `partition_left` on a repeated pivot, insertion sort below
  24 elements, `break_patterns` on a bad split -- and that stands on those
  grounds rather than on a measurement: the A/B across it reads the one `sort`
  id that reaches a partition at all (24 workers, a million elements) at
  1.035x, inside its own 15.3% round-to-round spread. The other three ids of
  that shape never reach either partition, because `sort_by` short-circuits to
  `[T]::sort_unstable_by` at one worker or at or under
  `parallel_sort_cutoff::<T>()`, and the 1.7x the A/B shows on them is the
  **call shape** of that short-circuit: handing the standard library's sort a
  `&C` where the comparator is owned measures 37.8 ms against 21.9 ms for the
  same million `f64`s in one binary, and which regime a build lands in is a
  property of the calling crate. The figures, the A/A control that sets this
  shape's floor and the ablation are in
  [`BENCHMARKS.md`](BENCHMARKS.md) under "The #381 measurement".

- **No `&C` reaches `[T]::sort_unstable_by`, at any of the three places the
  sort falls back to it** (issue #399). Upstream takes `Compare` by value and
  its `std::sort(.., comp)` calls hand the sort a copy of the functor;
  `Runtime::sort_by`'s short-circuit owns `cmp` and passes it, and
  `sort_loop`'s two leaves, which hold the `&C` a whole scope shares and
  cannot copy because `C` need not be `Clone`, pass `|a, b| cmp(a, b)`.
  The reference form is the same program at a different cost -- `impl Fn for
  &F` is a forwarding call some instantiations inline and others do not -- and
  the cost is up to 1.8x on a million `f64`s. It is a property of the
  *calling* crate, since that is where a generic instance is compiled, and it
  is not stable under anything a caller would think of as unrelated: one
  harness moved 20.06 ms to 32.06 ms on the same call when a second,
  uncalled instantiation was added to the same file. Four caller binaries
  that disagreed by 1.86x on this crate's own benchmark agree to 1.03x after
  the change; what it does not buy is a faster sort on a build that was
  already lucky, which the shape's own id shows by not moving outside its
  spread. [`BENCHMARKS.md`](BENCHMARKS.md)'s "The #399 change" has the
  four-way ablation and the before and after.

## Pipelines

- **A pipeline's lines need the eleventh `Handle` variant after all**
  (issues #180, #181, #230). The premise M8 was planned on was that a
  pipeline's lines could be ordinary runtime tasks, so that no eleventh
  `Handle` variant was needed: issue #180 read upstream's
  `NonpreemptiveRuntime&` -- the second runtime shape next to its ordinary
  `Runtime`, whose one method, `schedule`, stores a dependency count rather
  than decrementing one -- as exactly the port's own `Runtime::schedule`
  contract, and issue #181 checked that contract against the port's own
  machinery and found the one refusal that could have mattered, "already
  preempted itself" on an entered target, looking unreachable for a line
  task: a line task never spawns a child of its own graph, so -- #181's
  reasoning went -- it never preempts and never reaches that check.
  `Runtime::schedule`'s own doc said so, naming the pipeline as a safe
  caller.

  **That premise is false, and issue #230 is the implementation hitting
  it.** `Runtime::schedule`'s guard does not ask "has this node spawned a
  child"; it asks `is_entered`, which for an ordinary `Handle::Runtime` task
  is simply "has the first entry's `enter` set `PREEMPTED`" -- and `enter`
  sets that bit **before the body runs, unconditionally**, whether or not
  the body ever spawns anything. #181's reading checked the refusal's
  *intent* correctly and missed this: a line task's body does not need to
  spawn a child to be seen as "already entered", it only needs to still be
  running, and two lines with two workers reach that in the plain course of
  scheduling. `tests/pipelines.rs`'s multi-line, multi-worker cases hit the
  assert for exactly this reason once #182 tried to build the pipeline on
  top of an ordinary runtime task.

  **The guard itself is correct and was never the bug.** During `enter`,
  the node's join counter holds the body's own reference; storing zero over
  it from a sibling's `schedule` call would make the body's own closing
  decrement wrap and leave the node `PREEMPTED` with nothing left to
  resume it. Narrowing or removing the assert to let a line task through
  would trade that loud panic for a silent hang, which is strictly worse.

  **The fix is the eleventh `Handle` variant upstream always had two of.**
  `Handle::NonpreemptiveRuntime` ports upstream's `NonpreemptiveRuntime`
  class on its own terms: its dispatch arm never sets `PREEMPTED` and takes
  no reference on the join counter, so it always finishes on its first
  entry and `preempts_itself` answers `false` for it unconditionally --
  `is_entered` is therefore `false` for the whole of its body's run, with
  no dependence on whether the body has "finished" in any sense `schedule`
  needs to ask about. The body is handed a restricted handle rather than
  the full `Runtime`, because a node with no preemption protocol has no way
  to resume itself if its body tried to spawn nested work on it; making
  that unrepresentable in the type was preferred over documenting it as a
  hazard. `FlowBuilder::emplace_nonpreemptive_runtime` builds one.

  The trap worth marking for the next reader: `enter`'s bit is a property
  of the *dispatch arm*, set the instant the first entry begins and cleared
  only once the node is done, not a property of what the body happened to
  do on a given run. Reading `Runtime::schedule`'s refusal as being about
  spawned children -- rather than about which `Handle` variant the target
  is -- is the reading that looked right here and was not.
- **A pipe callable is `Fn + Send + Sync`, and a pipeline's own mutable
  state lives behind an `Arc` of atomics** (issue #180). Upstream's line
  task captures `this` -- the `Pipeline` or `ScalablePipeline` object
  itself -- so every line reads and writes the same counters and token
  buffers through a bare pointer the object's own lifetime backs. `Fn`
  rather than `FnMut` is forced by the same fact `Pipe`'s own docs give for
  the bound: a `PipeType::Parallel` pipe runs its callable on more than one
  line's worker at once, so the callable has to be callable through one
  shared reference from more than one thread at the same time. What
  upstream reaches through `this`, the scheduler reaches instead through an
  `Arc` of atomics shared by every line's runtime task, so that the state a
  pipe callable closes over and the state the scheduler mutates between
  lines are the same shared allocation rather than two different answers
  to "what does a line own".
- **One `Pipeline` type covers upstream's `Pipeline` and
  `ScalablePipeline`** (issue #180). The two upstream class templates
  differ only in template mechanics -- `Pipeline<Ps...>` is a fixed tuple
  of pipe types fixed at compile time, `ScalablePipeline<P>` a
  runtime-sized range of one pipe type -- and in the `reset` overloads that
  follow from that difference, not in anything the scheduling logic itself
  does. A `Vec<Pipe>` covers both shapes here, so one `Pipeline` type is
  what upstream's two become, and `ScalablePipeline`'s `move` test, which
  exists to prove a pipeline's identity survives a C++ move constructor,
  has no Rust counterpart to port: there is nothing left for it to prove
  once a `Pipeline` is an ordinary owned value.

  **Deferred pipelines are out of scope.** `pipeline.hpp` at the pinned
  v4.1.0 tag has no `defer`, and while `test_deferred_*.cpp` exists
  upstream, it is not wired into upstream's own test `CMakeLists` -- so
  there is no upstream test this port is on the hook for, and nothing here
  builds toward one.
- **A pipeline's `Pipeflow` sits behind a per-line `Mutex`, not an
  `UnsafeCell`** (issue #182). Upstream reaches every line's `Pipeflow`
  through a raw pointer `_build`'s closure captures, sound there only
  because the join-counter protocol already guarantees at most one line
  task is ever reading or writing a given line's token at a time -- a
  discipline nothing in the C++ type system checks, only the protocol.
  `Pipeflow`'s fields are already `pub(crate)` for exactly this scheduler,
  so a `Mutex<Pipeflow>` per line costs nothing an `UnsafeCell` would not
  also have paid in review effort, and it pays that cost to the type
  system instead: every access is an ordinary `lock()`, poisoning is
  tolerated as every other shared lock in this crate tolerates it, and the
  lock is never contended because the protocol that would make an
  `UnsafeCell` sound here is the same protocol that keeps two line tasks
  from ever locking the same line at once. `src/pipeline/scheduler.rs`'s
  own `# Design` section has the full argument; the crate's two `unsafe`
  modules, named in the README, are neither this one.
- **Resetting a pipeline rebuilds its graph and state rather than mutating
  them in place** (issue #185). Upstream's `reset` overloads read and write
  `_pipes`, `_lines` and, for the wider overload, `_graph`/`_tasks` straight
  through the `this` pointer every already-built line task's closure
  captures, so a reset upstream performs is visible at once to a module
  task that composed the object *before* the reset ran. A line task's
  closure here instead captures an `Arc<State>` by value, and
  `FlowBuilder::composed_of` hands out a clone of `Pipeline::taskflow`'s own
  `Arc` that outlives the call -- so there is no single allocation this
  port could mutate in place that every already-composed module task would
  see, not without the kind of `unsafe` aliasing this port's porting posture
  asks a port to design around rather than reproduce. `Pipeline::reset_pipes`
  and `Pipeline::reset_lines` build a fresh graph and `State` instead and
  swap them in, which means **a composition made before the call keeps
  pointing at the pre-reset graph and keeps running exactly as it did** --
  compose again after resetting to run the new shape. That is not a loss
  against how upstream is actually used: every call site in upstream's own
  `test_scalable_pipelines.cpp` resets before its one `composed_of` of a
  given object, never after. `src/pipeline/scheduler.rs`'s own `# Design`
  section has the full argument.
- **A `DataPipe`'s callable has two shapes, not upstream's three, and the
  slot between two pipes is a `Mutex<Option<Box<dyn Any + Send>>>`, 24 bytes
  per line** (issue #186). Upstream's `DataPipeline::_on_pipe` picks one of
  three callable shapes by `is_invocable_v`: `(Pipeflow&) -> O`, with no
  input at all; `(I&) -> O`; and `(I&, Pipeflow&) -> O`. The first of those
  is not a shape a pipe's *position* forces so much as a shape its *input
  type* does -- it is only ever reachable for a pipeline's first pipe, the
  one `DataPipe`'s own `input_t` is `void` for -- and Rust already has a
  value for "no input" that upstream's template metaprogramming has to work
  around: `()`. So [`serial_pipe_with_flow`](crate::pipeline::data_pipeline::serial_pipe_with_flow)
  and [`parallel_pipe_with_flow`](crate::pipeline::data_pipeline::parallel_pipe_with_flow)
  are `Fn(I, &mut Pipeflow) -> O`, and a pipeline's first pipe is simply one
  of the two with `I = ()`, its callable's first parameter pattern-matched
  away (`|(), pf| ...`) rather than omitted. [`serial_pipe`](crate::pipeline::data_pipeline::serial_pipe)
  and [`parallel_pipe`](crate::pipeline::data_pipeline::parallel_pipe) are
  the plain `Fn(I) -> O` shape for a pipe that never touches its
  `Pipeflow`. Two shapes rather than three, with no loss of what upstream's
  three let a pipe do.

  The slot a `DataPipelineBuilder::pipe` call's erased closure reads from
  and writes to is one `Mutex<Option<Box<dyn Any + Send>>>` per line --
  upstream's `CachelineAligned<data_t>`, a `std::variant` over every pipe's
  own output type fixed at compile time by the parameter pack. Rust has no
  anonymous union of an open, heterogeneous type list without a
  caller-supplied enum, and asking a `DataPipe` caller to write one defeats
  the point of a typed builder that infers every pipe's types from its
  callable. A type-erased box is what is left, and its cost is measured
  rather than guessed: `size_of::<Mutex<Option<Box<dyn Any + Send>>>>()` is
  24 bytes on a 64-bit target, checked by a test guarded on
  `target_pointer_width = "64"` -- a 16-byte fat pointer for the
  `Box<dyn Any + Send>` (data pointer plus vtable pointer, `Option`'s own
  tag folded into the data pointer's niche since a `Box`'s data pointer is
  never null) plus the `Mutex`'s own lock word rounded up to the slot's
  8-byte alignment. On top of that fixed cost, every pipe invocation pays
  one heap allocation for `Box::new(output)` and frees it when the next
  pipe's `take` (or the following token's own overwrite, for a chain's last
  pipe) drops the box -- the same per-token allocation upstream avoids with
  its compile-time-sized `data_t`, and the one real price this port's typed
  builder pays for not asking a caller to write a variant enum by hand.

  The builder's own type-state, `DataPipelineBuilder<Prev>`, is what makes
  the chain's own types line up without a parameter pack: `Prev` is the
  output type the next `pipe` call must accept as `DataPipe<Prev, _>`'s own
  input, carried in a zero-cost `PhantomData<Prev>` and advanced by each
  `pipe` call's own generic parameter. A mismatched pair is a type error at
  the `pipe` call itself, not a `downcast` failure discovered at run time --
  `DataPipelineBuilder::pipe`'s own `compile_fail` doctest is the
  acceptance check for that. The one place a `Prev`-typed value still has
  to round-trip through `Any` despite the chain being fully typed is a
  pipeline's first pipe: `DataPipelineBuilder::pipe`'s body is generic over
  every pipe's own `Prev`, including the first one's, so it cannot write a
  literal `()` of type `Prev` without a `Default` bound this method has no
  business requiring of every later pipe's input type too. It downcasts a
  freshly boxed `()` to `Prev` instead, sound because `pipes.is_empty()` is
  true if and only if `Prev` is `()` -- the only way to reach that call with
  an empty chain is through `DataPipeline::builder`'s own
  `DataPipelineBuilder<()>` -- and the comment at that call names the
  invariant rather than leaving it to be re-derived.

  `DataPipeline` itself adds nothing to [`Pipeline`](crate::pipeline::scheduler::Pipeline)'s
  own scheduling loop: every `DataPipe` becomes one `Pipe` whose callable
  moves the slot's value through the user's typed callable, and the chain is
  then handed to `Pipeline::new` unchanged, so the token loop upstream
  duplicates between `Pipeline::_build` and `DataPipeline::_build` exists
  here exactly once.
- **A serial pipe pays the same callable bound as a parallel one, and
  upstream's second callable shape is not ported** (issue #180). `Pipe`'s
  bound, `Fn(&mut Pipeflow) + Send + Sync`, is forced by `PipeType::Parallel`
  alone -- a parallel pipe runs its callable on more than one line's worker
  at once, so it must be callable through one shared `&Pipe` from more than
  one thread at the same time, which is exactly `Sync`. A serial pipe never
  needs that on its own, but `Pipe::set_type` lets a caller change a pipe's
  direction after it is built, so a serial pipe pays the bound a parallel
  one would need rather than this type carrying two incompatible shapes that
  a `set_type` call could silently make unsound.

  Upstream's own `Pipe` accepts a second callable shape, `(Pipeflow&,
  NonpreemptiveRuntime&)`, checked for with `is_invocable_v` the same way
  `DataPipeline::_on_pipe` picks between its own three (the entry above on a
  `DataPipe`'s two callable shapes has that one); the port does not add a twin
  for `Pipe`. A pipe that could reach the line's own `NonpreemptiveRuntime`
  could call `schedule` on it directly, racing the very three-way outcome --
  `rt.schedule(next line)` or not -- the pipeline's own line task already
  computes from the two `fetch_sub`s around every pipe call; nothing a user
  pipe body could legitimately schedule that the line task is not already
  scheduling for it, and a seam that let it try would be a second, uncounted
  way to reach exactly the double-dispatch hazard the entry on
  `Runtime::schedule`'s hole under "Executor core" exists to close.

## Documentation and tooling

- **The prelude is a program's first page, and the crate root is the flat
  namespace for the names a caller writes -- with each family flattened at
  its own front door rather than at the root** (issue #256).

  `rustflow::prelude` takes the wider of the two readings
  `docs/api-friction.md` put side by side: it is the first-page vocabulary --
  the names a program spells to build a taskflow, submit it to an executor and
  read the run's outcome. That is `Taskflow`, `FlowBuilder` and the eight
  algorithm extension traits, one `AlgorithmBuilder` since #216; the graph
  vocabulary `Graph`, `Node`, `Task`, `Handle` and `Work`; `TaskMut`,
  `TaskRef` and `TaskType`; the `Subflow` and `Runtime` body parameters the
  dynamic task kinds hand their closures; and `Executor` with `RunHandle`,
  `RunError` and `Canceller`. The narrow reading -- only the traits a glob
  import needs to make `FlowBuilder`'s methods callable -- is what was there
  by accident, and the corpus answers it: all 44 example programs write
  `Executor` in an import of their own, 40 of them on the line after `use
  rustflow::prelude::*;`, as do 33 of the 34 prelude-using files in `tests/`.
  A prelude that leaves the single most written name in the crate out of the
  one import a program makes first is not a prelude; it is a list of trait
  fixups. The boundary is drawn at the point a program *chooses* something,
  which is why the async family, task groups, semaphores, scoped spawns,
  observers, pipelines and the algorithms' free-function and partitioner forms
  stay out: a program imports those by name, where it makes the choice. It
  also stops one step short of a prelude member's own nested-wait outcome --
  `JoinError` from `Subflow::join`, `CorunError` from `Runtime::corun` --
  because a body that joins or coruns is past its first page; both are root
  names. Nothing was taken out of the prelude, so no caller lost an import and
  an example's now-redundant `use rustflow::Executor;` is free to stay.

  The crate root is the port's counterpart of upstream's flat `tf::`
  namespace, and a name earns a line in it by being one a program *using* the
  crate writes: a type it constructs (`Taskflow`, `Executor`, `Semaphore`,
  `Xorshift32`), a type a public API hands it or asks of it (`Task`,
  `RunHandle`, `SemaphoreError`, `Scope`), or a trait whose methods it calls
  or implements (`FlowBuilder`, `Composable`, `WorkerInterface`). Applying
  that added the two names it showed were missing, `Composable` and
  `SemaphoreError`, and it answers the friction log's `Xorshift32` report with
  "already a root name, and not a prelude one" -- upstream documents
  `tf::Xorshift` as a utility with its own examples, and
  `examples/switch_case.rs` uses it as a caller would, standing in for
  upstream's `rand()`. Two kinds of name do not earn a root line. A *family*
  -- `algorithm`, `observer`, `pipeline` -- is flattened at its own front door
  instead: `rustflow::pipeline::{Pipeline, DataPipeline, Pipe, PipeType,
  Pipeflow, ...}` is this issue's own application of that,
  `rustflow::algorithm` is issue #216's to flatten as it collapses the eight
  extension traits, and `rustflow::observer` wants the two lines that would
  put `ChromeObserver` and `TfProfObserver` beside `Observer` in files this
  issue does not own. Flattening all three at the root would roughly double a
  35-name list for no gain: `rustflow::pipeline::Pipeline` reads as well as
  `rustflow::Pipeline` and says more, which is a distinction a single C++
  namespace has nowhere to draw. And a part of the scheduler that no program
  outside the crate can drive is not a root name and ought not be `pub`
  either, which is the arbiter issue #279 asks for: it excludes
  `NonpreemptiveRuntime` and `NonpreemptiveRuntimeWork` (the README already
  lists the first as deliberately not user-facing), the `executor::topology`
  types, and the `executor::queue` trio `Buffers`, `Thief` and `WorkerQueue`,
  whose only caller anywhere is `tests/wsq.rs` -- a test of the queue layer,
  not a program using it. Those three root lines were the rule's first
  outstanding application, and #279 took it: withdrawing them edited
  `tests/wsq.rs` to name `rustflow::executor::queue::...` instead, the same
  module path `tests/notifiers.rs` already uses for the notifiers, which were
  never root names.

  **`SemaphoreError` stopped qualifying, and the rule took the type as well as
  its root line** (issue #347). #329 demoted `SemaphoreError::max_value` to
  `pub(crate)`, and the only release a caller can reach is a task's own,
  through `TaskMut::release`, whose over-release `src/executor/worker.rs`
  turns into the panic payload the run reports -- a `String` of the error's
  `Display` text, so not even a downcast of that payload reaches the type.
  Both producers, `Semaphore::release` and `Node::release_all`, are
  `pub(crate)`, the struct's one field is private and there is no public
  constructor: no program outside the crate can hold, construct or ask for a
  value of this type, and `Semaphore`'s own public API -- `new`, `value`,
  `max_value`, `reset`, `reset_to` -- returns no `Result` at all. So the root
  line is withdrawn and the rule's last clause takes the rest: the type is
  `pub(crate)` outright rather than `pub` behind `rustflow::semaphore`. That
  is where it parts from the queue trio #279 withdrew, which stayed `pub` in
  their module because `tests/wsq.rs` really does drive a `WorkerQueue`
  through that path, while a `rustflow::semaphore::SemaphoreError` would be a
  name with nothing a caller could put behind it. The module's `# Design`
  section names the type in backticks instead of linking it, a public doc
  comment having nothing to link to once the type is crate-private.

- **The documentation contract, and the three questions applying it asked**
  (issues #126, #129, #130, #134). The contract is the section at the top of
  this file, written at the start of the first consolidation against a tree
  measured at 2.2× carrying 210 issue citations in its doc comments (#126).
  Two passes applied it, to the graph side (#129, ten files) and to the
  executor's periphery (#130, nine files, plus the `unsafe` audit that left
  all 15 sites in `src/executor/notifier.rs` carrying a `// SAFETY:` and
  promoted `clippy::undocumented_unsafe_blocks` to `deny`); both met the same
  three questions about the ratio, the citations below a test module and what
  an unchanged test-name set can mean, and the closing pass settled all three
  inside the contract itself (#134) rather than letting three issues answer
  them three ways. One of those answers is a rule about documentation passes
  in general and belongs with the others here: **a pass may not buy the number
  by cutting a `# Design` section or deleting a doctest**, so what moved to
  fit the contract was the measurement.

- **The suites share one test module, and the worker matrices are a macro**
  (issues #127, #132). The suites under `tests/` carried one hand-written
  `fn starvation_loop_4_threads() { starvation_loop(4) }` per worker count per
  family, six private copies of `spin_until` and five each of a counter and a
  body-maker; 489 of their `#[test]` functions across 13 suites now come out
  of 89 `worker_matrix!` blocks, and each helper is defined once under
  `tests/common/`. The test names do not change, which is the constraint the
  shape is built around and the rule every pass after this one inherits: a
  suite's names are upstream's `TEST_CASE` names, so renaming one edits the
  acceptance oracle rather than tidying a file. Why the helpers are topic
  files a suite pulls by path, and why a macro beats a single `#[test]` over
  a loop, are argued once where they are implemented — `tests/common/mod.rs`'s
  own docs — because the reason is that module's own (every file under
  `tests/` is a separate crate) rather than something the port shares.
- **A module's `# Design` section is not prose, and the eleven files still
  over the figure are disposed of here** (issue #228).

  The question the doc-ratio issue asked last -- whether a 1.2x bar measured
  per file is the right instrument at all -- is answered the way the examples
  column answered it once already. This contract sends a module's rationale to
  a `# Design` heading and nowhere else, so a file that obeyed it was charged
  for obeying it: `src/executor/scope.rs` stood at 3.2x and
  `src/algorithm/module.rs` at 3.0x, and in both the section the contract put
  there was most of the numerator. `devtools/doc-ratio.sh` therefore prints a
  fourth column, `design`, and computes `ratio` on what is left.

  The section is the `//! # Design` heading and the lines under it, up to the
  next top-level `//! #` heading or the end of the `//!` block, whichever
  comes first. A `//! ##` sub-heading stays inside it, which is what keeps
  `src/executor/topology/ownership.rs`'s "The locks, and the order they are
  taken in" with the argument it is part of; a fenced example inside the
  section is still an example, the fence being read before the heading; and
  only the heading spelled exactly `# Design` opens one, so `src/dump.rs`'s
  four topic headings and `src/executor/notifier.rs`'s "The one real departure
  from upstream" stay prose. Widening the rule to any module heading would
  make the column mean "module docs" rather than "the rationale this contract
  relocated", and neither of those two files needs it: both are well under the
  figure as prose. Nothing else about the instrument moved -- it still exits 0,
  no CI step reads it, and 1.2x is still a guide for prose-heavy
  implementation files rather than a bar.

  The tree went from **24 files flagged of 55 at a crate total of 1.0** to
  **11 flagged at 0.8**: 1,615 of the 10,371 lines the old column called prose
  are `# Design` sections, and the thirteen files the change clears were over
  by less than their own section. Each disposition below cites the split that
  decided it -- doc prose (item plus module, examples and `# Design` held out)
  against inline `//` prose, both over the file's code lines -- because that
  split is what says whether a pass has anything to move.

  **pass**, meaning a doc pass moves the prose to `# Design` or to this file,
  or cuts it:

  - `src/executor/runtime.rs` (2.3x) -- 305 doc lines over 187 of code is
    1.6x before an inline comment is counted, and what is retold is
    identifiable: `Runtime::schedule`'s `# Panics` re-derives the M8 failure
    the module's own `# Design` and this file already argue, and
    `try_corun`'s "what makes a failure under this node *this frame's*"
    paragraph is the anchor argument a third time. The 130 inline lines at
    the atomics are not the problem and stay.
  - `src/executor/corun.rs` (1.6x) -- 179 doc lines over 133 of code.
    `InnerRun`'s private-method docs restate a step of the module's
    `# Design` each (the hold taken out of the topology, the second wait, the
    idempotent `end`), and `CorunError`'s variant docs carry the cancellation
    rationale that belongs once in the section above them.
  - `src/algorithm/module.rs` (1.6x) -- a 22-line module with one public
    function whose 22-line item doc is half rationale: the
    `FnMut`-not-`FnOnce` paragraph, and the `composed_of` promise restated
    from the `# Design` section twelve lines above it.
  - `src/async_task.rs` (1.4x) -- 97 doc lines over 67 of code, for ten
    accessors of one to three lines each, with a five-line `# Design` section
    nothing was ever put in. `use_count`'s explanation of what the number
    counts, `take_panic`'s comparison with the `Task::exception_ptr` the
    README declines to port, and the three trait-impl notes are what that
    section is for.
  - `src/graph/handle.rs` (2.2x) -- no `# Design` section at all, and 121 doc
    lines over 54 of code: an eleven-variant enum where a variant is one code
    line and its doc up to ten. The per-variant rationale moves (the
    `Vec<i32>` small-vector note, the `NonpreemptiveRuntime` protocol, who
    owns a module's target); the eleven one-line `is_*` docs are the
    `missing_docs` floor and stay.
  - `src/graph/node.rs` (1.2x, twelve lines over) -- also no `# Design`
    section, so the `NodeBase` type doc carries three rationale paragraphs:
    atomic words where upstream's are plain, where a failure inside a nested
    graph is delivered, why the payload `Mutex` is uncontended. The
    per-method invariant statements -- "call it only after `claim_panic` has
    answered `true`" -- stay where the caller meets them.
  - `src/graph/work.rs` (1.3x, nine lines over) -- the `# Design` section is
    eleven lines and the item docs re-derive it: `Work`'s "upstream stores a
    different `std::function` per node kind" paragraph, and `lock_callable`'s
    `# Panics`, seven lines of which are the history of how the dispatch
    claim came to exist and belong to the PR trail rather than to the
    function.
  - `src/flow_builder.rs` (1.2x, under three lines over) -- the marginal
    case, and nothing in it restates a signature. The one movable paragraph
    is `emplace`'s "upstream overloads on the callable's signature"
    rationale, which is a `# Design` line; #216 moves both numbers anyway, so
    a pass that reads through and leaves it at 1.2 has answered this.

  **exempt**, meaning the prose is an invariant or a soundness argument that
  belongs at the code:

  - `src/executor/scope.rs` (2.0x) -- 48 code lines whose whole job is two
    `transmute` erasures and the join that makes them sound. Eighteen prose
    lines are the two `// SAFETY:` blocks and the notes at the `catch_unwind`
    and the unconditional join; of the 77 doc lines, `Scope`'s three-lifetime
    paragraph and its two field docs are the invariants that argument leans
    on (the `RefCell` is what makes `Scope` `!Sync`), and `Runtime::scope`'s
    `# Panics` plus the panic-attribution sentences on `spawn` and
    `spawn_runtime` are caller-facing text a public API owes. What is left to
    move is a few pointer sentences against a gap of 37 lines, so the figure
    is unreachable without moving the soundness argument away from the
    `unsafe` -- which is what #276 says to refuse.
  - `src/executor/dependent_async.rs` (1.3x) -- 118 doc lines over 161 of
    code is 0.7x; the file is over on its 94 inline lines, and those are the
    ordering argument at each atomic: why the `SUBMITTING` guard is raised
    before the dependencies are named, why each increment precedes its push,
    and why one `fetch_sub` pays the guard and every met dependency together.
    That is the contract's own example of what an inline comment is for, and
    there is nowhere else it can be read.
  - `src/pipeline/mod.rs` (2.1x) -- a family front door whose nine code lines
    are three `pub mod` and a `pub use` list: exactly the denominator the
    crate-root exemption exists for, on a file that exemption does not name.
    Its nineteen lines are the family's vocabulary and the re-export rule,
    pointing at #256's entry above, and it has no item doc at all. If a later
    change wants arithmetic here rather than this line, the rule to widen is
    the crate-root one.

  By directory that leaves #275 with `src/algorithm/module.rs` alone -- the
  five candidates its own scope lists fall to one -- #276 with `corun.rs` and
  `runtime.rs` out of twelve, and #277 with `async_task.rs`,
  `flow_builder.rs` and the three `graph/` files. `src/pipeline/mod.rs`
  belongs to none of the three, which the exempt verdict settles rather than
  needing a fourth pass.

- **`src/executor/corun.rs` comes under the 1.2x guide and
  `src/executor/runtime.rs` does not** (issue #276). The executor's
  documentation pass took the two files #228's re-measured disposition marks
  *pass*, and they end in different places.

  `corun.rs` goes from 1.6x to 1.2x by moving three arguments into its own
  module `# Design` section: that cancellation is not one of the ways a corun
  fails, which `CorunError` carried; what `InnerRun`'s `Drop` is for, which
  the struct doc carried; and that the anchor is taken before the graph is
  scheduled, which `corun_graph`'s four-step recital carried. What its
  private methods and that recital said about those a second time is cut.

  `runtime.rs` goes from 2.3x to 2.0x and stops there, which the arithmetic
  settles rather than a judgement about any one paragraph. 123 of its prose
  lines are inline `//` comments at the atomics -- why both state bits go on
  before the body runs, why the body's own reference is a `Release`
  `fetch_add` and its spending an `AcqRel` `fetch_sub`, why the payload
  forward waits for those bits to come off, why `CAUGHT` is read before the
  lock -- and 17 more are the module header above the `# Design` section.
  Against a budget of 1.2 x 187 code lines, that leaves 84 prose lines for 27
  documented items, fourteen of them public and four carrying a `# Panics` or
  `# Errors` section a caller is owed: three lines each. The file reaches the
  figure only by moving an ordering argument away from the atomic it is about,
  which is the move #276 refuses for `src/executor/scope.rs` and the ground on
  which #228 marks `src/executor/dependent_async.rs` exempt. What the pass did
  take are the redundancies that disposition named: `Runtime::schedule`'s `#
  Panics` no longer re-derives the failure the "`Runtime::schedule`'s hole is
  closed in the scheduler" entry under "Executor core" argues,
  `Runtime::try_corun` no longer makes the anchor argument a third time, and
  `NonpreemptiveRuntime`'s `private_bounds` rationale is in the module's `#
  Design` section rather than on the type.
- **The `graph/` submodules keep their own rationale, and this file keeps
  what they share** (issue #277). `handle.rs`, `node.rs` and `work.rs` are
  private modules behind the public `graph` facade, so a `# Design` section
  of one of them is read in the source rather than in the rendered docs.
  That is still where the documentation pass moved the rationale out of
  their item docs, because each piece of it belongs to exactly one of the
  three modules -- what a conditioner's `i32` buys, why a subflow's subgraph
  sits behind a `Mutex`, why the dispatch claim is independent of
  `PREEMPTED`, what one `Work<R>` folds together -- and an entry per variant
  here would split one module's reasoning across two files.
  `src/graph/mod.rs`'s own section carries the tree's shared decisions, the
  arena and the tombstone; what an item doc keeps is the half-line pointer
  the contract allows. The one fact three modules do share is that the
  restricted `NonpreemptiveRuntime` handle is not part of the public
  surface: a body closure is handed one with its parameter's type inferred,
  so `graph::Handle`'s variant, `graph::work`'s `NonpreemptiveRuntimeWork`
  and `FlowBuilder::emplace_nonpreemptive_runtime` each state the one
  consequence they meet -- no type annotation in a doctest, no type to link
  to -- and none of them re-derives the rest.

- **The four files M9 itself pushed over 1.2x stay over it, reviewed rather
  than cut** (issue #201). #192's N-dimensional extension, #191's
  `Task::data` slot and #206's `queue_size` left
  `src/algorithm/index_range.rs` (1.4x), `src/graph.rs` (1.2x),
  `src/executor/queue.rs` (1.2x) and `src/executor/worker.rs` (1.2x) over
  the contract's guide -- the same four issue #228 counts among the tree's
  21 (#228 has the rest, almost all predating M9). None is a
  `src/pipeline/mod.rs`-shaped case: that file's whole purpose is a
  module-tree pointer with nothing to move, where these four carry ordinary
  item docs plus, for `index_range.rs`, a `# Design` section arguing a real
  question (why `IndexRanges<T, N>` is its own type, why `discrete_domain`
  narrows one dimension). Cutting either to buy the number is exactly what
  the documentation contract above refuses, so none of the four were cut.
  They stand in the same bucket the contract already names for
  `src/executor/runtime.rs`, `src/task_group.rs`, `src/executor/corun.rs`
  and `src/executor/dependent_async.rs` -- a file a reviewer looks at, with
  the reason given once here rather than reinvented per PR.

- **`src/pipeline/mod.rs` is exempted from the 1.2× ratio, the same way a
  crate root already is** (issue #189). Its code is three `pub mod`
  declarations; the file's whole purpose is the module-tree overview above
  them, naming what each of `pipe`, `scheduler` and `data_pipeline` is for a
  reader who has not yet opened any of the three. Cutting that overview to
  3 prose lines to match 3 lines of code would not shrink a rationale this
  contract puts elsewhere -- there is none to move, the whole file *is* the
  pointer -- it would just delete the one place a reader meets the tree
  before the modules themselves. The contract already exempts a crate root
  on exactly this reasoning (`src/lib.rs`'s own code is a `pub mod`/`pub use`
  list, so its denominator measures nothing a ratio can judge); a module
  tree's own root is the same shape one level down, and issue #228, which
  asks the more general question of whether a per-file ratio is the right
  instrument for this crate at all, is where that reasoning would extend
  further if it does.

- **A lint policy gets its first exception, and the rule that admits it**
  (issue #209). `RandomPartitioner::chunk_size_range` (PR #203) carries the
  crate's first `#[allow]` anywhere in `src/`:
  `#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]` on the
  `f32`-to-`usize` conversion of `alpha * nw` and `beta * nw`. `Cargo.toml`'s
  policy for those two lints reads "loud from the start rather than
  retrofitted ... over a thousand call sites later", so the crate went from
  zero such attributes to one without the policy gaining a documented
  exception -- and a later contributor reading only `Cargo.toml` would
  reasonably treat the existing attribute as licence for the next one.

  The rule: a float-to-int conversion may carry a statement-scoped
  `#[allow]` with an inline justification only when its input is a
  genuinely user-supplied float with no integer-arithmetic equivalent, the
  way `alpha`/`beta` are arbitrary tunables matching upstream's own `float`
  parameters and `std` has no `TryFrom<f32> for usize` to fall back on. A
  conversion derivable from integers already in hand does not qualify, and
  must be done in integer arithmetic instead -- which is what the PR's
  other candidate site, the guided partitioner's chunk step, turned out to
  be: `r / (2 * w)` is a ratio of two integers already in hand, so integer
  division gives the exact floor with no float round-trip, and the
  `floor_to_usize` helper that had covered both sites was deleted once that
  site moved off it. `chunk_size_range`'s is, for now, the one instance the
  rule admits.
- **The eleven mutants of #274's run that no test should kill are
  dispositioned rather than tested: `.cargo/mutants.toml` skips them and the
  argument for each is here** (issue #309).

  #274's run over `src/graph/`, `src/semaphore.rs` and
  `src/algorithm/partitioner.rs` left 69 survivors, 53 of them killed by the
  twelve tests its own PR added. Eleven of the rest are mutants no test should
  kill: each is the same program as the original, or differs from it only in a
  value nothing promises. A test written for one of those pins the mutation
  instead of the behaviour, which is the one kind of test #274's acceptance
  refuses, so what they are owed is a disposition. Both forms are written.
  `exclude_re` in `.cargo/mutants.toml` keeps the next `cargo mutants` over
  these two files from reporting them at all, and this entry carries the
  reasoning, because the two rot differently: the regex is keyed on
  `file:line:col`, so an edit above any of these lines makes a pattern stop
  matching and the mutant comes back as a survivor pointing at an entry that
  still explains it. That fail-open is the reason the patterns are not keyed on
  the looser pair, function name plus operator, which would hide genuine `+`
  and `*` mutants on `loop_dynamic_until`'s other lines for as long as nobody
  looked. `cargo mutants 27.1.0` does read the key: its listing over the two
  files goes from 242 mutants to 231, and the eleven that disappear are exactly
  these.

  **Two `|`s over disjoint bits.** `NodeBase::claim_panic` and
  `NodeBase::take_panic` each build `Cancellable::EXCEPTION |
  Cancellable::CAUGHT` out of two distinct single-bit constants
  (`0x1000_0000` and `0x2000_0000`), and `a | b == a ^ b` whenever
  `a & b == 0`. Mutating that `|` to `^` leaves the same constant, so it is
  the same program. The `&` mutant of the same two expressions is a different
  program and is killed.

  **A swap guard that fires only on equal bounds.**
  `RandomPartitioner::chunk_size_range` ends with
  `if b1 > b2 { swap(&mut b1, &mut b2) }`, and `>=` for `>` swaps when the
  two bounds are equal as well, which leaves them where they were. The other
  two mutants of that comparison, `==` and `<`, change the range whenever
  `alpha > beta` and are killed.

  **A floor clamp its own branch condition makes unreachable.** The guided
  loop's `if q < chunk_size { q = chunk_size }` sits in the `r >= p1` arm,
  where `p1 == 2 * w * (chunk_size + 1)` and `q == r / (2 * w)`, so
  `q >= chunk_size + 1` on every path that reaches it; `<=` and `==` for `<`
  are false there exactly as `<` is. The third mutant of that comparison,
  `>`, is always *true* there and is killed by
  `the_guided_loop_shrinks_its_chunks_with_the_remaining_work`. The clamp is
  upstream's and stays: it is dead in the unmutated program only, and it is
  what makes the next group equivalent.

  **Six mutants of `p1`, the threshold that picks the atomic rather than the
  chunk size.** `let p1 = 2 * w * (chunk_size + 1)` chooses which arm hands
  out the next chunk: below `p1` remaining, a `fetch_add(chunk_size)` loop; at
  or above it, a compare-exchange of `q = max(r / (2 * w), chunk_size)`. Every
  arithmetic mutant of that line -- `2 + w * (c + 1)`, `(2 / w) * (c + 1)`,
  `2 * w + (c + 1)`, `2 * w / (c + 1)`, `2 * w * (c * 1)` and
  `2 * w * (c - 1)`, writing `c` for the chunk size -- is at most `p1` for
  every `w >= 1` and `c >= 1`, and `c >= 1` is what the loop's own
  `if self.chunk_size == 0` guarantees. So the only remainders that change arm
  are the `r` with `p1' <= r < p1`, and there `r / (2 * w) <= c`: the clamp of
  the group above raises `q` back to `c` and the body is handed a chunk of
  exactly the size the `fetch_add` arm would have handed it, with the same
  boundaries, the same order and the same early exit, for one caller and for
  `w` racing ones alike. Deleting the clamp as dead code would end that, and
  these six patterns with it.

  What does differ is the final value of the shared cursor, the `fetch_add`
  arm overshooting `n` by one chunk where the compare-exchange arm stops at
  `n`. Nothing pins that: every caller in the crate --
  `find::drive_find_dynamic`, `reduce::drive_reduce_dynamic`,
  `for_each_index`'s own loop and the `partition_dynamic` the slice
  algorithms share -- drops its cursor unread once the loops return, and a
  loop only ever compares it against `n`. The one cursor claim that *is* a contract, "a loop that arrives
  at an exhausted cursor hands out no chunk and does not move it", is asserted
  by `a_dynamic_loop_over_an_exhausted_cursor_hands_out_no_chunk`, and no
  mutant of `p1` can reach it, the `while curr_b < n` loop never being
  entered.

  Those last two groups argue over the inputs the unmutated loop is defined
  on: `w >= 1`, and a `2 * w * (chunk_size + 1)` that does not overflow.
  Outside them the original panics -- `r / (2 * w)` divides by zero for
  `w == 0`, `chunk_size + 1` overflows a debug build -- and a mutant that
  moves where the panic happens, or gets past it, differs from the original in
  nothing else. The crate's own callers are well inside the range: an executor
  has at least one worker (`Executor::new` asserts it), and each algorithm's
  driver has already returned for `n == 0` and taken the serial path for
  `w_total <= 1` before a partitioner sees either number.

- **Four API-shape questions the friction log left open are all "keep", and
  the reasons are here rather than in code** (issue #260).

  `docs/api-friction.md` closed M9 with four entries whose status was *open*
  for the same reason: each asks for a decision rather than a fix, and nothing
  was going to decide itself. All four come out the same way, which is worth
  stating plainly -- the log is a record of friction, not a backlog of
  defects, and the most common honest answer to "should the port change
  shape?" is no.

  **`composed_of` returns a bare `Task`, and naming a module task stays a
  second statement.** Two different example batches reported this
  independently (#194's and #199's), which is the only thing that made it a
  question: two reports of one shape look like a stronger signal than one.
  They are the same shape, though, and the rule they rediscovered is the
  "Naming a task is two statements" entry above, not a rule of its own -- a
  chainable `composed_of` would have to hand back a `&mut` borrow of the
  builder, and then `let m = taskflow.composed_of(t);` would hold the taskflow
  borrowed until `m` died, exactly as it would for `emplace`. The module task
  is not even the sharpest case of the rule, since `composed_of`'s argument is
  already an `Arc` the caller built in a statement of its own.

  **`emplace_n` keeps emplacing only statics; there is no mixed-kind
  `emplace!` macro.** Upstream's four conditional examples each destructure
  one overloaded `emplace` call whose callables mix kinds, and the port writes
  one `emplace`/`emplace_condition` call per task instead. Closing that by
  type is not available: dispatching on a closure's *return* type needs
  `impl<F: FnMut()> ..` beside `impl<F: FnMut() -> i32> ..`, which coherence
  rejects as overlapping (E0119) even though no closure can satisfy both --
  the same limitation `src/algorithm/mod.rs`'s own `# Design` section hits
  between its static and dynamic loop traits. So a macro would have to be told
  each element's kind syntactically, `emplace!(tf, static || {}, condition ||
  0)`, which is the kind named per call either way, and it would buy a macro
  that no rustdoc page lists beside the six methods it stands in for. What it
  would actually have saved is also smaller than the four call sites suggest:
  in `examples/if_else.rs` only two of the four tasks are one-liners, the other
  two needing a block each to clone an `Arc` into the body, and
  `examples/nested_if_else.rs`, `switch_case.rs` and `while_loop.rs` are the
  same mixture. Keep.

  **`Worker` gains no `thread_id()`, and `Worker::thread()` has no
  counterpart at all.** Upstream's accessor lends out a mutable
  `std::thread&` for one purpose, which its own class docs and
  `worker.hpp`'s `WorkerInterface` example both name: `native_handle()` into
  `pthread_setaffinity_np`. The port answers neither half. The native handle
  std will part with is on the `JoinHandle` the executor owns
  (`JoinHandleExt::as_pthread_t`), not on a `&Worker` a hook can reach, and it
  is a raw pthread any thread could act on -- the footgun this port's posture
  refuses. The one thing std offers for a thread, a `ThreadId`, cannot set
  affinity, and it would be no news to its reader either: every path that
  hands out a `&Worker` or a `WorkerView` -- `WorkerInterface`'s two hooks,
  `Executor::this_worker`, `Runtime::worker`, `Subflow::worker` and an
  observer's view -- hands out the worker *of the calling thread*, a
  non-worker thread being refused a corun (`CorunError::NotAWorker`) rather
  than running a body, so `worker.thread_id()` would return
  `std::thread::current().id()` by construction. That premise is what
  `tests/workers.rs`'s `a_worker_a_caller_reads_is_the_thread_it_runs_on`
  pins, reading the pair from a worker's prologue and from a
  `Runtime::worker()` in a body and requiring them to agree. The direction
  that does carry information is the reverse one, and the port already has
  upstream's own: `Executor::this_worker`/`this_worker_id` answer "which
  worker is this thread" out of the executor's thread-id map. A caller who
  wants affinity sets it from inside `scheduler_prologue`, which runs on the
  worker's own thread and so needs nothing lent to it; what is left for
  `src/executor/worker.rs`'s module docs to say -- and now does, where before
  they only said a thread handle lives in `WorkerThread` -- is why no
  read-only substitute is coming either. This also decides the shape of
  `examples/worker_interface.rs`'s divergence from upstream's affinity
  program permanently rather than for now.

  **`RunError` stays `#[non_exhaustive]` with its one variant, and
  `RunHandle::drop` goes on blocking.** Both were already "keep" in the log
  with their reasons, and both are recorded here so the log can point at this
  file instead of restating them. The attribute is right for a type the
  milestones ahead will add variants to, and the cost -- a trailing wildcard
  arm in `examples/exception.rs` and `subflow_exception.rs` before a reader
  has met the reason -- is one match arm. The blocking drop is the documented
  design (`src/executor/run_handle.rs`'s module docs and its `Drop` impl,
  under the "A run owns the graph it walks" entry above), which is why
  upstream's fire-and-forget `run` plus a separate `wait_for_all` has no
  literal translation worth writing: `executor.run(&taskflow).wait()` is the
  port's spelling of the same program.

  The doc notes this issue carries alongside the four are at the items
  themselves, each because a caller meets the consequence before the reason:
  what `FnMut + Send + 'static` asks of a captured value
  (`src/flow_builder.rs`'s `# Design` section, pointed at from
  `emplace_subflow` and `emplace_runtime`), why a `String` `bop` is a closure
  literal rather than a named `fn` (`src/algorithm/scan.rs`'s `# Design`
  section, pointed at from `src/algorithm/reduce.rs`), and that re-entering
  `TaskRef::for_each_subflow_task` from inside its own visitor hangs on a
  non-reentrant `Mutex` rather than panicking (`src/task.rs`). The lint
  interaction the log's "Tooling" section reported is a convention rather than
  a crate doc, so it went to `.claude/agents/rust.md`: in `examples/` and
  `tests/`, a type the code never names goes in backticks, since `cargo doc`
  does not build those targets and the `use` an intra-doc link would need is
  `unused_imports` under `clippy --all-targets`.

**The instruments, and which of them decides anything.** Four measurements of
this crate's own documentation and tests live in `devtools/`, and all four are
**reports**: `doc-ratio.sh` (#126, with #228's `design` column) measures the
ratio this contract names, `test-names.sh` (#262) is the test-name comparison a
documentation pass is held to, `coverage.sh` (#263) runs `cargo-llvm-cov` over
`src/` alone, and `api-witness.py` (#325, made type-aware there and precise
about signatures and annotations in #331) counts the public declarations no
test, example, benchmark or doc example witnesses, each call site read against
the type it reaches rather than against a bare name. None of them is a gate:
each exits 0 whatever it finds -- `test-names.sh diff` and `coverage.sh` exit
non-zero only on a difference a PR asked for or a measurement that could not
run -- so a number from one is something a reviewer reads rather than something
that passes or fails. `cargo mutants` is the fifth, run by hand over a module
at a time, with `.cargo/mutants.toml` carrying the equivalent mutants the entry
on them dispositions. What does decide a pass is the gate `docs/development.md`
names, plus the two things CI's `examples` job runs: `devtools/run-examples.sh`
and, since #334, every benchmark id once in criterion's untimed `--test` mode.

## Benchmarks and performance

The benchmark suite is a part of the port with decisions of its own, and this
is where they are: what is ported out of upstream's `benchmarks/` and what is
not, and what the harness under `benches/` is allowed to share. The milestone
that built the suite took three decisions that are *not* here, because they
decide the scheduler rather than the suite and this file groups by the part of
the port a decision decides: the nested-corun stack bound (#359) and the
preempted parent's re-queue (#342), which corrects what that entry expected,
are both under "Executor core", and the `for_each_index` chunk loop (#343) is
under "Algorithms". The figures behind all three are in
[`BENCHMARKS.md`](BENCHMARKS.md), which is where every number in this port
lives.

- **Twenty-two of upstream's twenty-five benchmarks are ported, and the three
  that are not are declined for reasons outside the port's control** (issue
  #345). `benchmarks/CMakeLists.txt` at the pin declares twenty-five `bench_*`
  targets, twenty-four host ones and `bench_hetero_traversal` behind
  `TF_BUILD_CUDA`. `hetero_traversal` is that CUDA target, and `cuda/` has
  been out of scope from the start. `mnist` wants four dataset files
  downloaded from Kaggle and an Eigen DNN its `dnn.hpp` includes out of
  `3rd-party/eigen-3.3.7`, so neither half of what it measures is code in the
  pinned tree's own library -- and `3rd-party/` carries separate terms, which
  is a fresh licensing question rather than a porting one. `thread_pool`
  compares `tf::Executor` against the hand-rolled `ThreadPool.hpp` singleton
  beside it; the rayon pool stands in for TBB rather than for a C++
  singleton, and the shape it submits -- 1000 independent tasks -- is
  `embarrassing_parallelism`, which is ported. The declines are recorded in
  `BENCHMARKS.md`'s header and in the README's "What is not ported", and what
  would reverse any of them is a change of scope rather than a measurement.

- **A benchmark target shares a fixture module, not a library, and the module
  is split by what a target needs** (issue #332). Every file directly under
  `benches/` is its own crate, so a shared helper is compiled separately into
  each target that declares it, and a helper one target does not call is dead
  code -- which this crate's lints make a gate failure rather than a warning.
  So `benches/common/` is `tests/common/`'s shape: `mod.rs` holds only what
  every target that declares it calls, and each further helper is a topic file
  pulled by `#[path]`, so `linear_chain.rs` compiles no rayon pool and
  `sort.rs` no per-task body. Nothing that would move a measurement is shared:
  sizes, criterion sample settings and a shape's own setup stay in the target,
  which is what its table reports. The alternative that was rejected is an
  `#[allow(dead_code)]` over one wide module, which buys convenience by hiding
  exactly the thing a shared fixture most needs to stay honest about -- whether
  a helper still has callers. `devtools/bench-ab.sh` (#333) is the other half
  of the harness and sits outside the crates for the same reason: an
  interleaved A/B builds two refs, which is a script's job and not a
  benchmark's.

- **The benchmarks' CI smoke run stays a step of the `examples` job** (issues
  #334, #345). `cargo bench -- --test` runs every id once, untimed, which is
  "does it still run" rather than "how fast" and so is not a gate command;
  what needed deciding was whether 22 targets had outgrown a step of a job
  built for examples. The timings say no, twice over. On the run that closed
  #366 the job took 1m35s -- 21 s of container setup and checkout, 30 s of
  `run-examples.sh`, 42 s of smoke run -- while `two-core` took 2m14s on the
  same workflow, and all four jobs run in parallel. A job of its own would
  therefore shorten the workflow by nothing, since `examples` is not the
  critical path, and would pay the 21 s of setup a second time. The step also
  wants exactly what that job already has: dev-dependencies built and
  criterion and rayon already compiled under the profile a bench build gives
  them. What would reopen this is `examples` becoming the longest job of the
  four, which is a number the workflow's own run history answers.

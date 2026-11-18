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
the work, which is the README's "Order of work" section and the milestones
behind it.

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
lines**, prose being every comment line that is not part of a doc example.

`devtools/doc-ratio.sh` measures that last part: one row per production file
(`src/**/*.rs`, counting the text above the test module) with its prose lines,
example lines, code lines, ratio and `#NNN`-in-doc-comment count, then a tree
total. It is a **report and not a gate** — it always exits 0 — because which
comments go is a reviewer's judgement, and a ratio cannot make it. The tree
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

## Design decisions already taken

- **Graph as an arena.** The upstream API hands out `tf::Task` handles that
  alias into a graph the caller still owns and mutates, which does not survive
  translation to Rust's aliasing rules. Here the taskflow owns a vector of
  nodes and a `Task` is a `Copy` index. Dependency edits go through the
  taskflow via a short-lived fluent borrow, so the graph is plain `Send + Sync`
  with no `unsafe`, and a run can hold `&Taskflow` across worker threads.
  Upstream's rule "do not mutate a taskflow while it runs" becomes a lifetime
  on the run handle — which makes the ordinary case a compile error, and is
  not on its own enough; see the next entry.
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
- **A module task shares its target, `Arc<Taskflow>`.** Upstream's
  `composed_of` stores a bare `Graph&` and leaves the caller to keep the
  composed taskflow alive for as long as any run of the composing one may be
  in flight — an obligation a safe Rust API cannot state, so the module owns a
  share of its target instead. `adopt(Graph)` is the owned form of the same
  thing, for a graph with no other owner.
- **A failure inside a nested graph is delivered to whoever is waiting for
  that graph** (issue #48), not to the run. Upstream calls the thing that is
  waiting an *anchor* and finds it by walking the failing node's parent chain;
  the walk is ported with one change of priority — an explicit anchor
  (a frame blocked right now in an `Executor::corun`, a `Subflow::join`, a
  `Runtime::corun` or a `TaskGroup::corun`, and since the async family also a
  free node whose runtime-shaped body hands back a handle, which is anchored
  for its whole life because the waiter is whoever holds that handle) beats an
  implicit one (a node that preempted itself and means to read its payload
  slot when it is resumed — a runtime task, a free node with a runtime-shaped
  body, or a task group's anchor, which carries the bit from the moment the
  group is opened) beats the run, which is every chain's root. The one root
  that is no destination at all is the sentinel run a free async task's job
  names; the entry on it below says why. Two decisions came out of #48 that
  upstream does not have to make, and the change of priority is #157's.

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
- **A subflow's subgraph lives in a `Mutex<Graph>` inside the node's handle,**
  because the body builds it through a node the run only *borrows*: a worker
  reaches its node as `&Node`, so the graph it writes needs interior
  mutability of its own. The `Subflow` handle holds the guard for the body's
  duration, which cannot deadlock one — a nested subflow locks its own node's
  graph, so no body ever takes the same lock twice.
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
  back-pointer is involved.
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
  sentinel run** (issues #86, #105). Upstream's async node is an ordinary
  `Node` out of its object pool with `_topology == nullptr`, held alive by a
  reference count packed into the low 24 bits of the node's state word. A
  `Node` here is a slot in a `Graph`'s arena and has no way to exist outside
  one, so an async task owns a one-node arena and the `Arc` around it *is*
  upstream's count: handles share it, every queued job of the node shares it,
  and the node goes when the last share goes — no second counter to keep in
  step and no recycle call to get right. That leaves the null topology, which
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
  submitted async task is ~415 ns, of which 87% is the three heap blocks the
  node is -- the `Arc`, the arena inside it and the body's box.
  [`BENCHMARKS.md`](BENCHMARKS.md) has the ablations under "The #125
  attribution" and, in the section after it, the one change they named: the
  arena is sized to the single node it holds (issue #144), which is 528 of
  the 848 bytes a task and 6% of the submission path. That it is three blocks
  and not two is `Graph`'s doing rather than this entry's — nodes live in a
  `Vec`, so folding the arena into the `Arc` would take either a graph that
  can hold one node inline, which is a discriminant and a branch on every
  resolution by index in every graph in the crate for the one case that needs
  it, or an async path that does not resolve a job through a `&Graph` at all.
  The three are therefore the bill, for the reason this entry gives.
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

  A group's children are also queued in the shared buffers where a runtime
  body's go onto the submitting worker's own queue, and that stays true even
  for a group that could name its worker: `corun` on a group runs without the
  owning end of that queue, so its pops are steals from the first-in end, and
  pushing LIFO against a FIFO waiter would hand the corun the oldest
  outstanding subtree rather than the child it just spawned. What that leaves
  is a **stack-depth** ceiling, not a correctness one — every child still runs
  — and `src/task_group.rs` has where the cliff is, with the test inputs kept a
  factor of two inside it.
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

## Decisions taken in M6

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
  `Submission::parentless` says. This extends the M5 entry on one
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
- **The documentation contract, and the three questions applying it asked**
  (issues #126, #129, #130, #134). The contract is the section at the top of
  this file, written at the start of the milestone against a tree measured at
  2.2× carrying 210 issue citations in its doc comments (#126). Two passes
  applied it, to the graph side (#129, ten files) and to the executor's
  periphery (#130, nine files, plus the `unsafe` audit that left all 15 sites
  in `src/executor/notifier.rs` carrying a `// SAFETY:` and promoted
  `clippy::undocumented_unsafe_blocks` to `deny`); both met the same three
  questions about the ratio, the citations below a test module and what an
  unchanged test-name set can mean, and the closing pass settled all three
  inside the contract itself (#134) rather than letting three issues answer
  them three ways. One of those answers is a rule about documentation passes
  in general and belongs with the others here: **a pass may not buy the number
  by cutting a `# Design` section or deleting a doctest**, so what moved to
  fit the contract was the measurement.

The milestone ran 17 issues against the 11 it opened with, and the rest of its
decisions extended an entry above rather than adding one here. Where each is:
the anchor window and the companion walk in `enter` (#157) under "A failure
inside a nested graph is delivered to whoever is waiting for that graph"; the
one-node arena and why three heap blocks cannot be two (#144) under "An async
task is one `Arc`-owned free-standing node"; the debug assertion and the loom
model that replaced a paragraph (#138, #141) under "A run's waiter is released
by the last share of its storage hold"; and a task body's own queue (#119) and
naming a task in two statements (#120) in entries of their own. Two issues
deliberately left none: #128 and #151 are the weekly loom workflow, whose own
file and the README say what it runs, and #146 was two module docs still
calling the async family a later milestone.

## Rationale moved out of the executor's periphery

The contract at the top of this file keeps a rationale in one place, and for a
module that is a dozen lines of dispatch over machinery somebody else owns,
that place cannot be the module itself. These entries are that place, moved
there by #130, and each names the issue whose decision it records, as every
entry above does.

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
- **The cancellation early-out reads the whole parent chain** (issue #48).
  Upstream's `Node::_is_parent_cancelled` reads the immediate parent only,
  which it can afford because `_process_exception` marks every ancestor on its
  way up — and the walk here does that too, so one level would be enough. Every
  level is read anyway: it costs one relaxed load per level of nesting and
  answers strictly more, since a job whose own parent is unmarked can still
  have a marked *grandparent*, when the failure was in a sibling subtree under
  the same anchor, and every job under a failed anchor is one whose result
  nobody will collect.
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
  inner run finishes and no later than the next job it picked up is done. It is
  for a worker of *that* executor and nothing else, any other thread getting
  `CorunError::NotAWorker` rather than the blocking wait, which would be a
  mistake about which thread the code is on. And nesting a run on a *different*
  executor and blocking on it is still fine, two pools being unable to starve
  each other the way one starves itself. Every blocking wait on a nested graph
  in the crate is this loop: `Subflow::join` (#50), `Runtime::corun` (#51) and
  `TaskGroup::corun` (#93) as much as `Executor::corun` itself.

## Decisions taken in M7

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

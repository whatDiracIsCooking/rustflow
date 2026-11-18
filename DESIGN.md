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

## Rationale moved out

The contract at the top of this file keeps a rationale in one place, and for
a module that is a dozen lines of dispatch over machinery somebody else owns
-- or a handful of value types whose own doc comments would otherwise retell
a decision made once, elsewhere -- that place cannot be the module itself.
These entries are that place: #130 moved the executor periphery's rationale
here first, and the modules after it that were in the same position add to
it the same way, each naming the issue whose decision it records, as every
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

  Upstream's own `Pipe` accepts a second callable shape,
  `(Pipeflow&, NonpreemptiveRuntime&)`, checked for with `is_invocable_v` the
  same way `DataPipeline::_on_pipe` picks between its own three (the M8
  entry on `DataPipe` above has that one); the port does not add a twin for
  `Pipe`. A pipe that could reach the line's own `NonpreemptiveRuntime` could
  call `schedule` on it directly, racing the very three-way outcome --
  `rt.schedule(next line)` or not -- the pipeline's own line task already
  computes from the two `fetch_sub`s around every pipe call; nothing a user
  pipe body could legitimately schedule that the line task is not already
  scheduling for it, and a seam that let it try would be a second, uncounted
  way to reach exactly the double-dispatch hazard the entry below on
  `Runtime::schedule`'s hole exists to close.

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
- **The observer list is one atomic check on the path with no observers,
  and a mutex only when there is work to do** (issue #167). Upstream's own
  `_observer_prologue`/`_observer_epilogue` iterate `_observers` -- an
  `unordered_set` -- under no lock at all, relying on nothing ever mutating
  it while a run is in flight, which upstream's own docs do not promise and
  this port will not assume. The crate-private `ObserverList` instead keeps
  a plain `AtomicUsize` count beside the list: the empty check both
  scheduling seams open with is one relaxed load, and `ObserverList::insert`
  and `ObserverList::remove` -- called rarely, never from a worker's hot
  path -- are the only two callers that take the list's mutex to write it.
  A non-empty seam takes that same mutex once, to clone the registered
  `Arc`s into a `Vec` of its own, and releases it before calling anything,
  so an observer that calls back into `make_observer` or `remove_observer`
  cannot deadlock against the seam that is visiting it. The same issue
  scopes what a `WorkerView` answers about a worker's queue down to
  `is_queue_empty` alone, repeating exactly what `Worker` already exposes
  rather than a second, inconsistent one -- not because upstream's
  `queue_size()`/`queue_capacity()` pair turns out to be unanswerable, but
  because answering either half needs `src/executor/queue.rs` and a wider
  `Worker` surface, both outside this issue's own files. Issue #206 is the
  follow-up those two files were left for, and the two halves turn out to
  need different answers: see "Decisions taken in M9" for which.
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

## Decisions taken in M8

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
- **`Runtime::schedule`'s hole is closed in the scheduler, not documented
  and left to the caller** (issues #244, #245). M8's original premise is
  false a second time over, and in the opposite direction from the bullet
  above: `Handle::NonpreemptiveRuntime`'s dispatch arm never sets
  `PREEMPTED` so that `is_entered` would stop blocking a pipeline's own
  `schedule` bypass, but that same deliberate `false` means `is_entered`
  also stops answering the question it originally existed to answer --
  whether this node's callable is genuinely running right now. `is_entered`
  only ever answers `true` for the four handle kinds `preempts_itself`
  does, and only once their dispatch arm has actually set `PREEMPTED`.
  Every other handle kind
  (`Handle::Static`, either conditioner, `Handle::NonpreemptiveRuntime`) is
  never "entered" by that reckoning even while its body is genuinely
  running, and `Handle::Subflow` is not either for as long as its body is
  still *building* the subgraph that would make it preempt -- so a
  `schedule` bypass targeting any of those, while the target's callable is
  actually locked, found nothing to stop it and could queue a second job
  onto a callable the first had not finished with, tripping `lock_callable`'s
  one-worker-per-node assertion.

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
  `Handle::NonpreemptiveRuntime` -- claims it immediately before the lock
  and releases it immediately after the guard has dropped, except
  `Handle::Subflow`'s own preempting entry, which defers the release to its
  second entry: the point the node is genuinely idle again, rather than the
  point its body merely returned, since a fresh job racing the node's own
  still-outstanding nested run would be exactly the double dispatch this
  claim exists to prevent. `schedule_bypassing`'s own
  `assert!(!is_entered(target))` is untouched -- #240's reasoning for keeping
  `is_entered` answering a different question than this claim stands --
  and what follows it now calls `DispatchClaim::wake` instead of queueing
  outright: a claim already held defers (`PENDING` set) rather than races,
  and the claim holder's own release queues the deferred job once it is
  actually safe to. `Handle::Runtime`'s own `enter` and the two module
  handles are deliberately **not** given a claim: `enter` sets `PREEMPTED`
  *before* the body it locks ever runs, so `is_entered` already covers the
  entire locked window for an ordinary runtime task, and the existing case
  asserting `schedule` refuses an entered one is kept exactly as a caller
  bug, not softened into a defer. The two module handles lock no callable at
  all, so `lock_callable` cannot trip on either of them.

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

## Decisions taken in M9

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

  `for_each_subflow_task` is ported on `TaskRef`, not left absent: a
  subflow's subgraph lives in the node's `Mutex<Graph>` (see `src/subflow.rs`
  and the entry above on leasing a subgraph), and after a join that retained
  it (`Subflow::set_retain(true)`) the graph is still there for the method to
  walk, exactly as upstream's is. Its visitor hands back a `TaskRef` scoped
  to that subgraph's own lock rather than a bare `Task`, because the index
  space a subflow's tasks live in is not the composing taskflow's own --
  which is also where the trap `docs/api-friction.md` records from #194's own
  example lives: calling `for_each_subflow_task` again from inside the first
  call's own visitor, to resolve a successor's name, re-enters the same
  `Mutex` on the same thread and hangs rather than panics.
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
- **`WorkerView` and `Worker` gain `queue_size`, not `queue_capacity`**
  (issue #206, following on from #167's "Decisions taken in M7" entry
  above). `crossbeam-deque`'s `Worker::len` and `Stealer::len` read the same
  front/back indices `is_empty` already does, so `queue_size` is as honest
  a read as upstream's `BoundedWSQ::size` -- a live count, stale the
  instant it is returned, which is what `Worker::queue_size` and
  `WorkerView::queue_size`'s own doc comments say rather than letting a
  caller treat the number as a fact to act on.

  `queue_capacity` stays unanswered, because it has no truthful answer to
  give. Upstream's `BoundedWSQ::capacity` returns a fixed bound fixed at
  construction; a `crossbeam-deque` worker queue instead grows its ring
  buffer on demand from a 64-item minimum (per its own source, not part of
  its published API) and publishes that buffer's size through neither
  `Worker` nor `Stealer`. There is no call this port could make that would
  not be inventing a number: a hard-coded constant, `usize::MAX`, or the
  current `queue_size` would each look plausible and each be meaningless,
  which is worse than the method staying absent.
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

## Decisions taken in M10

- **The prelude is a program's first page, and the crate root is the flat
  namespace for the names a caller writes -- with each family flattened at
  its own front door rather than at the root** (issue #256).

  `rustflow::prelude` takes the wider of the two readings
  `docs/api-friction.md` put side by side: it is the first-page vocabulary --
  the names a program spells to build a taskflow, submit it to an executor
  and read the run's outcome. That is `Taskflow`, `FlowBuilder` and the eight
  algorithm extension traits; the graph vocabulary `Graph`, `Node`, `Task`,
  `Handle` and `Work`; `TaskMut`, `TaskRef` and `TaskType`; the `Subflow` and
  `Runtime` body parameters the dynamic task kinds hand their closures; and
  `Executor` with `RunHandle`, `RunError` and `Canceller`. The narrow reading
  -- only the traits a glob import needs to make `FlowBuilder`'s methods
  callable -- is what was there by accident, and the corpus answers it: all
  44 example programs write `Executor` in an import of their own, 40 of them
  on the line after `use rustflow::prelude::*;`, as do 33 of the 34
  prelude-using files in `tests/`. A prelude that leaves the single most
  written name in the crate out of the one import a program makes first is
  not a prelude; it is a list of trait fixups. The boundary is drawn at the
  point a program *chooses* something, which is why the async family, task
  groups, semaphores, scoped spawns, observers, pipelines and the algorithms'
  free-function and partitioner forms stay out: a program imports those by
  name, where it makes the choice. It also stops one step short of a prelude
  member's own nested-wait outcome -- `JoinError` from `Subflow::join`,
  `CorunError` from `Runtime::corun` -- because a body that joins or coruns
  is past its first page; both are root names. Nothing was taken out of the
  prelude, so no caller lost an import and an example's now-redundant
  `use rustflow::Executor;` is free to stay.

  The crate root is the port's counterpart of upstream's flat `tf::`
  namespace, and a name earns a line in it by being one a program *using* the
  crate writes: a type it constructs (`Taskflow`, `Executor`, `Semaphore`,
  `Xorshift32`), a type a public API hands it or asks of it (`Task`,
  `RunHandle`, `SemaphoreError`, `Scope`), or a trait whose methods it calls
  or implements (`FlowBuilder`, `Composable`, `WorkerInterface`). Applying
  that added the two names it showed were missing, `Composable` and
  `SemaphoreError`, and it answers the friction log's `Xorshift32` report
  with "already a root name, and not a prelude one" -- upstream documents
  `tf::Xorshift` as a utility with its own examples, and
  `examples/switch_case.rs` uses it as a caller would, standing in for
  upstream's `rand()`. Two kinds of name do not earn a root line. A *family*
  -- `algorithm`, `observer`, `pipeline` -- is flattened at its own front
  door instead: `rustflow::pipeline::{Pipeline, DataPipeline, Pipe, PipeType,
  Pipeflow, ...}` is this issue's own application of that, `rustflow::algorithm`
  is issue #216's to flatten as it collapses the eight extension traits, and
  `rustflow::observer` wants the two lines that would put `ChromeObserver`
  and `TfProfObserver` beside `Observer` in files this issue does not own.
  Flattening all three at the root would roughly double a 35-name list for
  no gain: `rustflow::pipeline::Pipeline` reads as well as
  `rustflow::Pipeline` and says more, which is a distinction a single C++
  namespace has nowhere to draw. And a part of the scheduler that no program
  outside the crate can drive is not a root name and ought not be `pub`
  either, which is the arbiter issue #279 asks for: it excludes
  `NonpreemptiveRuntime` and `NonpreemptiveRuntimeWork` (the README already
  lists the first as deliberately not user-facing), the `executor::topology`
  types, and the `executor::queue` trio `Buffers`, `Thief` and `WorkerQueue`,
  whose only caller anywhere is `tests/wsq.rs` -- a test of the queue layer,
  not a program using it. Those three root lines are the rule's one
  outstanding application: withdrawing them edits `tests/wsq.rs` to name
  `rustflow::executor::queue::...` instead, the same module path
  `tests/notifiers.rs` already uses for the notifiers, which were never root
  names, and that edit belongs to #279, which owns the file.

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
  counterpart is permanent rather than pending.** Upstream's reference
  wrapper is sound there only because its graph edges are trusted by
  convention: `make_for_each_task(std::ref(beg), std::ref(end), ..)` hands
  the task an unchecked alias into a container the caller still mutates, and
  what keeps that from being a data race is the programmer's claim about the
  dependency edges, nothing the compiler reads. Reproducing it would need
  `unsafe` and would hand a caller exactly that footgun, which this port's
  stated posture refuses; a lock taken once per invocation -- not once per
  element -- is the price, and `src/algorithm/for_each_index.rs`'s "No
  deferred `beg`/`end`/`step`/`range`" `# Design` section already names the
  same gap from the implementation side. Two measurements make the cost
  smaller than the friction log's framing suggests. It is a *slice*
  algorithm's cost, not the family's: of the 28 wrappers, 23 take at least
  one `Arc`, two (`merge`, `merge_by`) take owned `Vec`s and return the slot
  they publish into, and three -- the `for_each_index` trio -- take no data
  at all, since an index range is `Copy` and travels by value. And the port
  has a form upstream does not for the case that actually chafes: a caller
  who wants no lock computes the slice in a `Runtime` body and calls the
  `Runtime::` method on it, which is what `tests/for_each.rs`'s stateful
  cases do in place of upstream's `std::ref` ones. The rule the seventeen new
  factories follow, so the family stays one vocabulary rather than three: a
  value the body only reads is `Arc<Vec<T>>`, one it writes is
  `Arc<Mutex<Vec<T>>>`, a scalar result is `Arc<Mutex<T>>`, a `Copy` range or
  index travels by value, and a factory that *creates* the output storage
  returns `(body, slot)` as `make_merge_task` does.

  **What #216 inherits.** It is collapsing the *derived* layer -- 28 one-line
  wrappers once #292 through #295 land -- not a primitive, which is why the
  collapse is cheap and why it does not have to wait for those four: a
  factory changes a wrapper's body, never its signature. It should keep all
  28 methods (none is redundant; the counts above show which algorithms lean
  on which form, not that any method is unused), fold the eight traits into
  one whose every method is a single `emplace_runtime` call, and, per #256's
  entry above, flatten `rustflow::algorithm`'s own front door: the collapsed
  trait, the four partitioners and their `ClosureWrapper`, `IndexRange` and
  `IndexRanges`, and the `make_*_task` family. The collapsed trait replaces
  the eight prelude lines with one; the factories are *not* prelude names,
  which is #256's boundary applied unchanged -- a program importing a
  free-function form is choosing it, and chooses it by name.

# rustflow

A Rust port of [Taskflow](https://github.com/taskflow/taskflow), a C++
task-parallel programming system built around task dependency graphs.

**Status: steps 1 to 5 are in** — the static graph, the work-stealing
executor, condition tasks and cancellation, dynamic tasking (subflows,
runtime tasks and module composition), and step 5's async family: async and
dependent-async tasks submitted from the executor, from a runtime task or from
a task group, with and without a handle on the result; task groups; and
semaphores. Those are steps of "Order of work" at
the end of this file, which is where the status is kept rather than duplicated
here: each step is a GitHub milestone, so its issues are what is merged and
what is not, and a milestone still open after the feature it names works is
open on follow-ups rather than on the feature — the performance ones are named
under "Performance", below. The toolchain, container setup and CI gate all of
it rides on were in place and verified first.

All of that is written with **no `unsafe` outside one module**: every `unsafe`
block in the crate is in `src/executor/notifier.rs`, where a worker about to
park publishes its thread handle into the lock-free waiter stack and the
notifier that pops it reads it back. The scheduler itself has none — it was
the ownership discipline described under "A run owns the graph it walks" below
that removed the last one, and every slice of dynamic tasking and of the async
family since has stayed inside it, which is worth saying out loud because a
work-stealing scheduler is not where a reader expects to find that.

## Getting a toolchain

There is no host toolchain requirement — Rust lives in the container, pinned by
`ARG RUST_VERSION` in the root [`Dockerfile`](Dockerfile) (currently
`1.100-slim-bookworm`). Two front ends build that one image, so a result in
either means the same thing:

| | |
|---|---|
| **Interactive / agent** | `devtools/devcontainer.sh` (or open `.devcontainer/` in an IDE) |
| **Batch / CI** | `docker compose -f docker/compose.yaml run --rm check` |

```sh
devtools/devcontainer.sh up             # start the container
devtools/devcontainer.sh check          # the gate: fmt, clippy, test, deny, doc
devtools/devcontainer.sh shell          # bash inside it
devtools/devcontainer.sh claude         # Claude Code inside it
devtools/devcontainer.sh test graph::   # cargo test, filtered
devtools/devcontainer.sh down           # stop it (volumes survive)
```

`check` is the gate: `cargo fmt --all --check`, then
`cargo clippy --all-targets --all-features -- -D warnings`, then
`cargo test --all-features`, then `cargo deny check` — licenses, RustSec
advisories, duplicate crate versions and crate sources, configured in
[`deny.toml`](deny.toml) and pinned by `ARG CARGO_DENY_VERSION` — and last
`RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features`, which holds
the docs to the standard the crate-wide `missing_docs` lint already implies:
a link into a private item renders as nothing for a reader of the public
docs, and `cargo test` only catches a link that is outright broken. Anything
that passes locally should pass in CI, with one wrinkle in the shape rather
than the result: CI runs four of the five commands in this image and
`cargo deny check` as a separate `deny` job on a plain runner, because the
image CI uses is the stock `rust` one and building cargo-deny from source
there would cost every PR minutes.

## Working inside the container

The image carries the agent tooling as well as the toolchain, pinned the same
way `RUST_VERSION` is: `ARG CLAUDE_CODE_VERSION` and `ARG GH_VERSION` in the
[`Dockerfile`](Dockerfile). Bump the arg and rebuild rather than letting either
self-update — an image whose contents depend on the day it was built is the
thing the pins exist to prevent.

```sh
devtools/devcontainer.sh claude         # Claude Code, TTY attached
devtools/devcontainer.sh shell -c 'gh pr checks 12'
```

### Credentials

**Claude Code needs no setup.** The `dev` service bind-mounts your `~/.claude`
as the container's config directory, so the container is logged in because you
are — no token to mint, no `/login` to answer, and `nuke` cannot clear it
(it drops volumes, and this is a bind).

Two details make that safe rather than merely convenient, and both are worth
knowing before changing the mount:

- **A directory, not just `.credentials.json`.** A single-file bind mount can
  be written through but never *replaced*: an atomic write-temp-then-rename —
  how a careful writer updates a credential — fails on one with
  `OSError: [Errno 16] Device or resource busy`. Mounting the directory keeps
  the rename inside the mount. Measured, not assumed: a container session
  refreshes the token and the host's file is updated in place.
- **Shared live, not copied.** That file holds a `refreshToken` rewritten when
  the access token refreshes, so two copies would drift and a container refresh
  could invalidate your host login. One copy cannot.

`~/.claude.json` is deliberately **not** shared. Claude Code keeps it *beside*
the config directory, and that separation is the whole trick: it is one
monolithic JSON holding every project's state, rewritten whole on each change,
so sharing it would be last-writer-wins between host and container. The
container gets its own copy (see the bootstrap, below). For the same reason the
image does **not** set `CLAUDE_CONFIG_DIR` — that would collapse the two back
into one shared directory.

`plugins/` and `backups/` are shadowed back out of the bind with empty volumes,
so a container cannot rewrite plugin state every other session on the host
shares.

**`gh` and `git push` are the ones that need a variable**: `GH_TOKEN`, which
`docker/compose.yaml` forwards from the host only when the host has it set (see
its `environment:` comment for why that distinction is load-bearing). Without
it, push does not work *at all* — the image has no ssh client and the container
has no keys, so this repo's `git@github.com:` remote is unreachable.
`devtools/container-bootstrap.sh` wires `gh`'s credential helper and an
`insteadOf` rewrite into the *container's* global git config, sending that same
remote over HTTPS with the token instead. Your host git config is untouched.

`ANTHROPIC_API_KEY` and `CLAUDE_CODE_OAUTH_TOKEN` are forwarded too, for a host
that has no `~/.claude` to mount (a CI runner): `claude setup-token` produces
the latter. **Export any of them before `up`** — a container's environment is
fixed when it is *created*, so exporting into a shell whose container is already
running changes nothing in there. `down`, then any command, recreates it.

### What the bootstrap does

`devtools/container-bootstrap.sh` runs once per created container — from
`devtools/devcontainer.sh` after `up`, and as the IDE front end's
`postCreateCommand`, so the two cannot drift. Besides the git credentials
above, it seeds `~/.claude.json` from the host's copy. Since credentials come
from the mount, that is not about logging in; it carries onboarding state and
supplies the one thing nothing else can — a minimal `projects["/workspace"]`
trust entry. The host's own entry is keyed by the checkout's *host* path, so
trust cannot carry over on its own, and until it is accepted Claude Code
ignores the whole `permissions.allow` list in `.claude/settings.json`. Two
fixups come with the copy, both explained where they happen in that script.

**One thing the container cannot do, and one trap.** `docker` is not installed
in it, so `devtools/devcontainer.sh` and `docker/compose.yaml` are host-only —
from inside, run the five `cargo` commands of the gate directly. The trap is a
`.claude/worktrees/` checkout created on the **host**: a worktree's `.git` is a
*file* holding the absolute path of its git dir, and a host path resolves to
nothing under `/workspace`, so every git command in there dies with `fatal: not
a git repository`. Create the worktree from *inside* the container and its
`.git` reads `gitdir: /workspace/.git/worktrees/<name>`, which does resolve —
git then works fully in it, `push` and `gh pr create` included, which is how
the PRs so far have been shipped. Which half made a worktree is not visible
from its path, so `cat` that `.git` file when you inherit one.

### Two things that will bite otherwise

- **`HOST_UID`/`HOST_GID` are required, not defaulted**, by
  `docker/compose.yaml`. The wrapper exports the real ones for you; raw
  `docker compose` needs `export HOST_UID=$(id -u) HOST_GID=$(id -g)` (or
  `cp docker/env.example docker/.env`). A default of 1000 on a host where you
  are not uid 1000 silently writes root-owned files into the bind mount.
- **Builds go to `/target` in the container, not `./target`.** That is
  deliberate: sharing one `./target` across a container and a host build has
  them overwrite each other's artifacts. Run `cargo` on the host if you like —
  it gets its own `./target` and the two never meet.

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
  and the wake-ordering protocol (`tests/loom_*.rs`),
  `criterion` for the ported benchmarks, `rayon` as the baseline those
  benchmarks compare against, in the role TBB plays upstream.

### Design decisions already taken

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
  the walk is ported as it stands, including the priority — an explicit anchor
  (a frame blocked right now in an `Executor::corun`, a `Subflow::join`, a
  `Runtime::corun` or a `TaskGroup::corun`, and since the async family also a
  free node whose runtime-shaped body hands back a handle, which is anchored
  for its whole life because the waiter is whoever holds that handle) beats an
  implicit one (a node that preempted itself and means to read its payload
  slot when it is resumed — a runtime task, a free node with a runtime-shaped
  body, or a task group's anchor, which carries the bit from the moment the
  group is opened) beats the run, which is every chain's root and is anchored
  by definition. The one root that is *not* an anchor is the sentinel run a
  free async task's job names; the entry on it below says why. Two decisions
  came out of #48 that upstream does not have to make.

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
  submitted async task is ~440 ns, of which 87% is the three heap blocks the
  node is. [`BENCHMARKS.md`](BENCHMARKS.md) has the ablations under "The #125
  attribution", including the one that would get 3% of it back and why that is
  not this design's bill.
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

### Performance

Every measured number this port has is in [`BENCHMARKS.md`](BENCHMARKS.md) —
six of upstream's benchmarks ported to criterion with a rayon baseline in the
role TBB's flow graph plays upstream, the machine and the load it was under
recorded beside them, and after the tables the per-decision measurements: #40
and #74 on what a job owns, the continuation #62 did not port, the `Work`
question, external submission (#60), a task body's own queue (#119), the
spill-buffer count (#102) and what a free async task costs (#125). They are
kept there rather than repeated here, so each figure has one owner.
Benchmarks are deliberately outside the gate: a benchmark answers "how fast",
which is not a pass or a fail, so what CI holds the suite to is that it still
compiles.

Two of those results are bad, and both are tracked rather than filed away,
because the honest state of play is more useful than a table of the shapes
that happen to look good:

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
  A free async task submitted the same way costs ~440 ns, 87% of it the node
  it allocates (#125), and that too is the design rather than a tuning gap.
- **The continuation optimisation** (issue #62). Upstream's `_update_cache` /
  `TF_INVOKE_CONTINUATION` keeps one ready successor out of the queue and
  resumes it on the worker that just finished its predecessor, so a chain runs
  end to end where it started instead of being pushed and re-stolen per link.
  It is unported, which is also the main lever on the `linear_chain`
  regression above — under a job that owns what it names it saves the clone
  and the drop as well as the queue push — and two of the stress cases
  translated in #17 assert the locality it produces and had to be left out
  without it.

### Order of work

Each step is a milestone of PR-sized issues; the upstream unit tests under
`unittests/` are the acceptance oracle, translated per module.

1. **Static graph** — `Taskflow`, `Task`, the flow builder, DOT dump.
2. **Executor** — workers, deque, notifier, `run`/`run_n`/`wait_for_all`.
3. **Control flow** — condition tasks, cancellation, panic propagation.
4. **Dynamic tasking** — subflows, runtime tasks, module composition.
5. **Async** — async and dependent-async tasks, task groups, semaphores.
6. **Observers and algorithms** — partitioners, `for_each`, `reduce`, `sort`.
7. **Pipelines** — the one family with no Rust equivalent.

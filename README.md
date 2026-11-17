# rustflow

A Rust port of [Taskflow](https://github.com/taskflow/taskflow), a C++
task-parallel programming system built around task dependency graphs.

**Status: the static graph is in; the executor is being ported.** Those are
steps 1 and 2 of "Order of work" at the end of this file, which is where the
status is kept rather than duplicated here: each step is a GitHub milestone, so
the closed ones are what is merged and the open one is where the work is. The
toolchain, container setup and CI gate all of it rides on were in place and
verified first.

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
- **Dev** dependencies need only be useful: `loom` for the lock-free parts,
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
  taskflow on its behalf at all.

  The consequences are the point. A leaked handle leaks rather than dangling;
  an edit of a running taskflow panics (`Arc::get_mut` finds the run's shares)
  rather than reallocating a vector workers are walking; and the executor's
  one `unsafe` boundary is gone, leaving the notifier's as the only one in the
  crate. The cost is four atomic refcount operations per task and one
  `Arc::get_mut` per graph edit, measured in `BENCHMARKS.md`: +35 to +41 ns a
  task at 24 workers on `linear_chain` (+19%, the shape that measures nothing
  but scheduler overhead), +4 ns on `embarrassing_parallelism` at the same
  width (+2.4%), +1 to +4 ns at one worker, and +2 ns per `emplace`. A real
  regression, and the right one to accept: the alternative was a safe API with
  a reachable use-after-free.
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
  (a frame blocked in a `corun` today, a subflow's `join` once #50 lands) beats
  an implicit one (a node that preempted itself and looks when it is resumed,
  which is #51's runtime tasks and nothing yet) beats the run, which is every
  chain's root and is anchored by definition. Two decisions came out of it that
  upstream does not have to make.

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
  caller's error upstream too.
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
  `corun` doctests already do.
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

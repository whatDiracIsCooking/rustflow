# rustflow

A Rust port of [Taskflow](https://github.com/taskflow/taskflow), a C++
task-parallel programming system built around task dependency graphs.

**Status: all seven steps of "Order of work" are in** — the static graph, the
work-stealing executor, condition tasks and cancellation, dynamic tasking
(subflows, runtime tasks and module composition), step 5's async family
(async and dependent-async tasks submitted from the executor, from a runtime
task or from a task group, with and without a handle on the result; task
groups; and semaphores), step 6's observers and algorithms: an `Observer`
hook around every task a worker runs, with a built-in Chrome trace-event
writer; and `for_each`, `for_each_index`, `reduce`, `sort`, `scan`,
`transform`, `merge` and `find` over a borrowed slice or an `IndexRange`,
each with a choice of partitioner and a `FlowBuilder` wrapper over owned
data; and step 7's pipelines: a fixed chain of `Pipe`s run over a bounded
number of parallel lines, `DataPipeline`'s typed builder over the same
scheduler for pipes that pass data from each to the next, and the composition
trait both compose into another graph through. Those are steps of
"Order of work" at the end of this file, which is where the status is kept
rather than duplicated here: each step is a GitHub milestone, so its issues
are what is merged and what is not, and a milestone still open after the
feature it names works is open on follow-ups rather than on the feature —
the performance ones are named under "Performance", below. The toolchain,
container setup and CI gate all of it rides on were in place and verified
first.

A **consolidation milestone** then ran between steps 5 and 6, and it changed
no public API: what it changed is what a reader of the source meets. The async
family's 24 public methods are one-line delegations onto a single core over
two carriers; `executor::topology`, which was one 2,352-line file, is a module
tree split by what owns what; every doc comment in the crate is held to a
written documentation contract, and the integration suites share one test
module and write their worker matrices through a macro. The decisions are in
[`DESIGN.md`](DESIGN.md) under "Decisions taken in M6", one entry each, and
"Order of work" below records where the milestone sits.

All of that is written with **no `unsafe` outside two modules**: one is
`src/executor/notifier.rs`, where a worker about to park publishes its thread
handle into the lock-free waiter stack and the notifier that pops it reads it
back; the other is `src/executor/scope.rs`, added in M7, where
`Runtime::scope` erases a borrowed child closure's lifetime to the `'static`
bound `Runtime::silent_async` needs, sound only because `scope` joins every
child — run to completion, panicked, or dropped unstarted — before it
returns. The scheduler itself has none — it was the ownership discipline
described under "A run owns the graph it walks" in [`DESIGN.md`](DESIGN.md)
that removed the last one, and every slice of dynamic tasking and of the
async family since has stayed inside it, which is worth saying out loud
because a work-stealing scheduler is not where a reader expects to find
that.

M9 closed the remaining gap between the seven steps above and upstream's own
surface: the port is now at **parity with upstream's user-facing surface at
v4.1.0** — every public class, free function and example `taskflow/` ships
at the pin has a counterpart here, or a recorded reason it does not. What is
deliberately absent:

- `cuda/` and `3rd-party/`, out of scope from the start — see "Upstream pin",
  below.
- Deferred pipelines, since `pipeline.hpp` at the pin has no `defer` (see
  step 7 of "Order of work", below).
- `NonpreemptiveRuntime` as a user-facing type. The port keeps an internal
  nonpreemptive node kind a pipeline's line task dispatches through instead
  of a type a caller names directly — [`DESIGN.md`](DESIGN.md)'s M8 entry on
  the eleventh `Handle` variant has why.
- `TFProfManager`. A library has no business installing a process-global
  singleton whose destructor writes a file to a path an environment variable
  names — [`DESIGN.md`](DESIGN.md)'s M9 entry has the decision and what is
  kept as documentation instead.
- The serializer and the rest of upstream's `utility/` directory that `std`
  already covers without a port.

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

The loom models in `tests/loom_*.rs` are the one test target the gate does not
build — they need a `cfg(loom)` rebuild of the whole dependency graph, which
only `RUSTFLAGS` sets — so
[`.github/workflows/loom.yml`](.github/workflows/loom.yml) runs them weekly and
on `workflow_dispatch` instead of per PR.

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
`text_pipeline`. `tfprof` stands alone, for M9's own profiler decision
above.

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
  and for the two orderings a machine-checked model states better than a
  paragraph — the wake-after-last-release rule and the delivery of a failure
  under a runtime task (`tests/loom_*.rs`, one model each),
  `criterion` for the ported benchmarks, `rayon` as the baseline those
  benchmarks compare against, in the role TBB plays upstream.

### Design decisions already taken

They live in [`DESIGN.md`](DESIGN.md), one entry per decision and each still
naming the issue that took it, beside the documentation contract this crate's
comments are held to. The measured ones keep their numbers in
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
`unittests/` are the acceptance oracle, translated per module.

1. **Static graph** — `Taskflow`, `Task`, the flow builder, DOT dump.
2. **Executor** — workers, deque, notifier, `run`/`run_n`/`wait_for_all`.
3. **Control flow** — condition tasks, cancellation, panic propagation.
4. **Dynamic tasking** — subflows, runtime tasks, module composition.
5. **Async** — async and dependent-async tasks, task groups, semaphores.
6. **Observers and algorithms** — partitioners, `for_each`, `reduce`, `sort`.
7. **Pipelines** — the one family with no Rust equivalent. Deferred
   pipelines are not part of v4.1.0 and so are not ported: there is no
   `defer` in `pipeline.hpp` at the pin, and while upstream's
   `test_deferred_*.cpp` files exist, they are not in its own test
   `CMakeLists`.

One milestone has no step number: a **consolidation** ran between 5 and 6,
named in "Status" above and with its decisions in [`DESIGN.md`](DESIGN.md).
What it was for is what five steps had left behind — five milestones' worth of
rationale collected in doc comments, an async family written three times over
its three receivers, and the run's machinery one 2,352-line file over a dozen
types, six of the previous milestone's seven post-merge bugs having been
counter or share-lifecycle errors in it. So the step-to-milestone mapping is
one-to-one with that milestone skipped: step 6 is the sixth step and the
seventh milestone. A later step may do the same, and if one does it belongs in
this list the same way, named here rather than renumbering the steps, since
the numbers are what every issue and every `DESIGN.md` entry has been written
against.

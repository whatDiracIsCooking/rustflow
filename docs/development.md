# Developing rustflow

Everything it takes to build, test and gate this crate. The
[`README`](../README.md) is the page a reader of the crate wants; this is the
page a contributor wants, and nothing in it is needed to *use* rustflow.

## Getting a toolchain

There is no host toolchain requirement — Rust lives in the container, pinned by
`ARG RUST_VERSION` in the root [`Dockerfile`](../Dockerfile) (currently
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
[`deny.toml`](../deny.toml) and pinned by `ARG CARGO_DENY_VERSION` — and last
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
[`.github/workflows/loom.yml`](../.github/workflows/loom.yml) runs them weekly
and on `workflow_dispatch` instead of per PR. There are eight, one per
lock-free protocol or cross-thread rule a machine-checked model states better
than a paragraph: the notifier's wait protocol, the run storage's
wake-after-last-release rule, the delivery of a failure under a runtime task,
`Runtime::scope`'s join rule, a node's dispatch claim, the `AsyncHandle`
delivery slot, the `TaskGroup` counter, and the semaphore's
park-and-reschedule rule.

[`.github/workflows/miri.yml`](../.github/workflows/miri.yml) is the second
weekly lane, and what it checks is the half loom cannot: loom explores every
ordering of a *model* whose atomics it swapped out, while Miri interprets the
real code and objects to what that code does along the way — an aliasing rule
broken, a read of dangling or uninitialised memory, a misaligned access, a
data race. Miri is a nightly-only rustup component, so the lane carries a
second toolchain pinned by date in the two places that have to agree,
`ARG MIRI_NIGHTLY` in the [`Dockerfile`](../Dockerfile) and the toolchain name
in the workflow; a bare `nightly` would make the interpreter's verdict depend
on the day it ran. What runs under it is the unit tests of the three modules
holding most of the crate's `unsafe` — `executor::notifier`,
`executor::async_node` and `pipeline::scheduler` — rather than the whole
`--lib` suite: at roughly a hundredth of native speed, the four-hundred-odd
cases of that suite — many of them a multi-worker executor over thousands of
nodes — are not a job that finishes, and a lane that covers the `unsafe`
weekly is worth more than one that times out. Within those three, **nothing is
`#[cfg_attr(miri, ignore)]`d**: an ignore would owe a reason both here and in
the workflow's reason table, and the job fails on an ignored test that is
missing from that table, so the two lists cannot drift apart.

Two `MIRIFLAGS` are set, and both are there because `crossbeam-epoch` is in
the tree rather than because of anything in this crate — which is also why
each says so out loud in the workflow beside the flag:

- `-Zmiri-tree-borrows` picks Miri's newer aliasing model over the default
  Stacked Borrows. It is not a weakening: the aliasing check and the data-race
  detector both still run, under a different set of rules for what a derived
  pointer may do. Stacked Borrows rejects `crossbeam_epoch`'s `element_of`,
  which derives a pointer to a whole `Local` from a pointer to its list
  `Entry` field — the `container_of` pattern, which Tree Borrows allows — and
  `Stealer::steal` reaches it the moment a second worker steals.
- `-Zmiri-ignore-leaks` turns off the exit-time leak check and nothing else.
  The leaks are that crate's epoch-based reclamation working as designed: a
  worker's `Local` and the sealed garbage bag its thread-local handle pushes
  at thread exit are freed only when some thread pins again, and once an
  `Executor` has joined its workers nothing does. No frame of this crate
  appears in their allocation backtraces, and a leak is not undefined
  behaviour.

The limit that leaves is in the log rather than hidden: the same crate earns
an integer-to-pointer cast warning, and Tree Borrows does not model such a
cast, so a pointer derived that way is one this lane cannot fully vouch for.
Between a model that rejects the dependency outright and one that does not
model one of its casts, the job is as strict as the dependency allows.
`-Zmiri-disable-isolation` is deliberately absent, since none of the three
modules reads the host's clock, environment or filesystem. In the container:

```sh
MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-ignore-leaks" \
  cargo +nightly-2026-11-18 miri test --lib -- executor::notifier::tests
```

That toolchain is in the image rather than in `Cargo.toml`, so, exactly as for
the coverage pins below, a container created before the layer that installs it
has no `+nightly-2026-11-18` to run and needs a **host-side**
`devtools/devcontainer.sh rebuild` first.

CI runs the suite a second time as the `two-core` job of
[`.github/workflows/ci.yml`](../.github/workflows/ci.yml): once, pinned with
`taskset -c 0,1`, because most of the lifecycle races closed in this port were
found that way rather than by a full-width run — two runnable workers sharing
one core interleave a release against the wake that follows it in orders
neither a 24-core box nor a 4-core runner produces. It is one run against the
same bar, not a repeat count, so a failure in that job is a bug to file and
never a test to loosen.

## Working inside the container

The image carries the agent tooling as well as the toolchain, pinned the same
way `RUST_VERSION` is: `ARG CLAUDE_CODE_VERSION` and `ARG GH_VERSION` in the
[`Dockerfile`](../Dockerfile). Bump the arg and rebuild rather than letting
either self-update — an image whose contents depend on the day it was built is
the thing the pins exist to prevent.

```sh
devtools/devcontainer.sh claude         # Claude Code, TTY attached
devtools/devcontainer.sh shell -c 'gh pr checks 12'
```

The image also carries the two measurement tools the gate does not run:
`cargo-llvm-cov` for coverage and `cargo-mutants` for mutation testing, pinned
by `ARG CARGO_LLVM_COV_VERSION` and `ARG CARGO_MUTANTS_VERSION` beside
`CARGO_DENY_VERSION`, with the `llvm-tools` rustup component the first one
drives. `devtools/coverage.sh` runs the suite under `cargo llvm-cov` and prints
the per-file table and total over `src/` alone (`--html` adds a report under
`/target/llvm-cov/html/`); `cargo mutants` is run by hand against the module
whose tests are in question. Both are tools, not dependencies — `Cargo.toml`,
`Cargo.lock` and `deny.toml` know nothing of them. Any change to the
`Dockerfile`, these pins included, takes effect only after a **host-side**
rebuild of the image, `devtools/devcontainer.sh rebuild`: nothing in the gate
or in CI builds the image, so that rebuild is the check that a `Dockerfile`
edit works, and a container that predates a pin simply lacks the tool (the
coverage script says so and names this command).

`--functions` is the mode the coverage dispositions read: it lists every
function the suite never executed, grouped by file, one line per *instantiation
group* — llvm-cov counts a generic function once however many
monomorphisations it emitted — with `file:line:col` and the demangled symbol.
The listing is sorted and carries no timestamp, so two runs differ only where
the coverage did, and the script refuses to print one whose counts disagree
with the table above it. The `coverage` job in
[`.github/workflows/coverage.yml`](../.github/workflows/coverage.yml) runs that
same command on every pull request and every push to `main`, in the image `ci`
uses with `llvm-tools` and the pinned `cargo-llvm-cov` added, and publishes the
table in the job summary and the listing as a `functions-never-executed`
artifact. **It has no floor**: it goes red when the measurement fails, never
when a figure is low.

The baseline that job starts from, measured on 2026-11-21 by
`devtools/coverage.sh --functions` at commit 59cf9e4: **96.20% of 17035 lines**
(647 missed), **89.51% of 2412 functions** (253 never executed, spread over 43
files) and 96.29% of 19746 regions, with the per-file low end `flow_builder.rs`
at 50.55% of its functions (45 of 91) and 84.95% of its lines (56 of 372). The
line and region figures move by a few lines between runs of the same tree —
which worker steals what decides which scheduler paths a run takes, and the
spread behind this entry was 96.17% to 96.21% of lines — so read them to two
significant figures. The function figure is the one the dispositions work from,
and the one the listing is checked against, which is why it is the count stated
per file rather than a percentage. That listing is what the milestone's
disposition issues read, and this is the before their after is set against.

One more thing is in the image for a measurement rather than for the build:
`g++`, `cmake` and `make`, in the last layer of the
[`Dockerfile`](../Dockerfile) and there for `devtools/upstream-bench.sh`, which
builds **Taskflow v4.1.0's own benchmark binaries** and runs them on this box.
That is what filled [`BENCHMARKS.md`](../BENCHMARKS.md)'s `upstream C++`
column for the five nested shapes, which is empty for the other seventeen
because upstream publishes no figures at all — the one comparison that can say
whether a nested shape's scaling is the Taskflow design's or this port's being
upstream's own executor on the same CPU. That run is #384, and "The #384
measurement" at the end of that file is its output and its reading.
The script clones upstream at the pin into a scratch directory
**outside** the repo — never vendored, because `LICENSE-TASKFLOW` covers the
translated code in `src/` and `benches/` and not a checkout — configures it
with the benchmark targets on and tests and examples off, and builds the five
nested shapes' targets: `bench_skynet`, `bench_nqueens`, `bench_integrate`,
`bench_fibonacci` and `bench_merge_sort`.

```sh
devtools/upstream-bench.sh --list        # the plan, with no network and no toolchain
devtools/upstream-bench.sh               # clone, build, run the five
devtools/upstream-bench.sh --build-only  # stop after the build
devtools/upstream-bench.sh --bench nqueens --threads 1
```

**The image has to be rebuilt before it will run.** Exactly as for the
coverage pins above, nothing in the gate or in CI builds the image, so a
container created before that layer existed has none of the three tools; the
script says so and names `devtools/devcontainer.sh rebuild`, which is
host-only. `--list` needs none of them, so it is also how to tell which kind of
container you are sitting in. Three things from its `--help` are worth knowing
before reading a number out of it, all three measured rather than assumed: the
`runtime` column upstream prints is **milliseconds**; upstream's binaries sweep
a size *range* per process, so a cell of our tables is a row of its output and
`integrate`'s two sizes are not on upstream's grid at all; and
`bench_merge_sort` **deadlocks at one worker** in upstream's own harness, which
leaves that one cell unobtainable this way. Like the reports below it is one
run per row with no repeat count, so a quiet box matters as much as it does for
`devtools/bench-ab.sh`.

Three more instruments need nothing from the image. Two are POSIX `sh` and
measure the documentation contract in [`DESIGN.md`](../DESIGN.md) rather than
the code: `devtools/doc-ratio.sh` prints, per production file, how many of its
lines are prose, doc example, `# Design` rationale and code, with the ratio of
the first to the last, and `devtools/test-names.sh` snapshots and compares the
set of test names, which is how a documentation pass shows it renamed no test.
The third is Python 3 with nothing outside its standard library:
`devtools/api-witness.py` counts the public declarations in `src/` that no
test, example, benchmark or doc example witnesses, reading each call site
against the type it reaches rather than against a bare function name.

**Which of these decide anything: none of them.** All five of
`devtools/doc-ratio.sh`, `devtools/test-names.sh`, `devtools/coverage.sh`,
`devtools/api-witness.py` and `devtools/upstream-bench.sh` are **reports** —
each exits 0 whatever it finds, no CI job makes one a pass or a fail, and a
number from one is something a reviewer reads rather than something that
passes or fails.
`test-names.sh diff` and `coverage.sh` exit non-zero only on a difference the
PR running them asked about or a measurement that could not run at all;
`api-witness.py` exits 2 for the same second reason, and `upstream-bench.sh`
for a missing tool or a clone, configure, build or run that failed. `cargo mutants` is in the same class, run by hand over
a module at a time. What **gates** is the five-command `check` above, plus the
two things CI's `examples` job runs: `devtools/run-examples.sh`, and every
benchmark id once in criterion's untimed `--test` mode, which asks whether the
suite still runs and deliberately times nothing.

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

# rustflow

A Rust port of [Taskflow](https://github.com/taskflow/taskflow), a C++
task-parallel programming system built around task dependency graphs.

**Status: scaffolding.** The toolchain, container setup and CI gate are in
place and verified. No upstream API is ported yet.

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
devtools/devcontainer.sh check          # fmt + clippy -D warnings + test
devtools/devcontainer.sh shell          # bash inside it
devtools/devcontainer.sh claude         # Claude Code inside it
devtools/devcontainer.sh test graph::   # cargo test, filtered
devtools/devcontainer.sh down           # stop it (volumes survive)
```

`check` is the gate: `cargo fmt --all --check`, then
`cargo clippy --all-targets --all-features -- -D warnings`, then
`cargo test --all-features`. Anything that passes locally should pass in CI.

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

Nothing is baked into the image. Both tools authenticate from the environment,
which `docker/compose.yaml` forwards from the host **only when the host has it
set** (see its `environment:` comment for why that distinction is load-bearing).

| | |
|---|---|
| **Claude Code** | `CLAUDE_CODE_OAUTH_TOKEN` — `claude setup-token` on the host, once, then export it |
| **`gh`, and git push** | `GH_TOKEN` |

With no `CLAUDE_CODE_OAUTH_TOKEN` (or `ANTHROPIC_API_KEY`), Claude Code falls
back to an interactive `/login`, which persists in the
`rustflow-claude-config` volume and survives `down` and `rebuild` — only `nuke`
clears it. Either way works; the token is just the one that needs no prompt.

**Export it before `up`.** A container's environment is fixed when it is
*created*, so exporting a variable into a shell whose container is already
running changes nothing in there. `devtools/devcontainer.sh down` then any
command recreates it.

`git push` needs `GH_TOKEN` and would not work without it at all: the image has
no ssh client and the container has no keys, so this repo's `git@github.com:`
remote is unreachable from inside. `devtools/container-bootstrap.sh` wires
`gh`'s credential helper and an `insteadOf` rewrite into the *container's*
global git config at create time, which sends that same remote over HTTPS with
the token instead. Your host git config is untouched.

### Two things the bootstrap does for you

`devtools/container-bootstrap.sh` runs once per created container — from
`devtools/devcontainer.sh` after `up`, and as the IDE front end's
`postCreateCommand`, so the two cannot drift. Besides the git credentials
above, it seeds Claude Code's `~/.claude.json` from the host's copy if the
volume has none, because that file sits *beside* the config directory and so is
not carried by the volume — without it a fresh container looks like a fresh
install. It is a copy, never a shared mount: that file is one monolithic JSON
rewritten whole on each change, so sharing it read-write would be
last-writer-wins. Two fixups come with the copy, both explained at the point
they happen in that script, plus a minimal `projects["/workspace"]` trust entry
— the host's own entry is keyed by the checkout's *host* path, so trust cannot
carry over on its own, and without it Claude Code ignores the whole
`permissions.allow` list in `.claude/settings.json`.

**Two things the container cannot do.** `docker` is not installed in it, so
`devtools/devcontainer.sh` and `docker/compose.yaml` are host-only — from
inside, run the three `cargo` commands of the gate directly. And git does not
work inside a `.claude/worktrees/` checkout mounted into the container: a
worktree's `.git` is a *file* holding an absolute host path, which resolves to
nothing under `/workspace`. The devcontainer is for the main checkout; worktree
work does its git from the host.

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
//! Translated from `taskflow/core/executor.hpp`.
```

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

The upstream API hands out `tf::Task` handles that alias into a graph the caller
still owns and mutates, which does not survive translation to Rust's aliasing
rules unchanged — so the graph representation is the first real design decision,
not a transcription step.

Rough order of work, coarsest first:

1. **Static graph** — `Taskflow`, `Task`, dependency edges, topology checks.
2. **Executor** — the work-stealing thread pool and its run/wait semantics.
3. **Control flow** — conditional tasks, module tasks, subflows.
4. **Beyond** — `cudaFlow` and the pipeline/dataflow modules, if ever.

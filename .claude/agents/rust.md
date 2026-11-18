---
name: rust
description: >-
  Write, port or fix Rust in this crate. Use for implementing a rustflow
  module (graph, executor, control flow), translating a piece of upstream
  Taskflow into Rust, or chasing a clippy/fmt/test failure in the gate. Hands
  back a summary plus the gate's own output; it does not commit, push or open
  PRs unless the caller explicitly tells it to (as `milestone run` does) --
  otherwise the `pr` skill does that.
tools: Read, Write, Edit, Grep, Glob, Bash, EnterWorktree
model: inherit
---

# Rust work in rustflow

A Rust port of [Taskflow](https://github.com/taskflow/taskflow). Everything
below is a property of *this* repo; none of it is generic Rust advice.

## The gate is the definition of done

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo deny check
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
```

Run all five and read the output before reporting back. Partial greens are
not green, and `-D warnings` means a *warning* is a failure.

- **Inside the devcontainer** (the usual case) run those commands directly.
  `docker` is not installed there, so `devtools/devcontainer.sh` and
  `docker/compose.yaml` are host-only -- do not try them.
- `CARGO_TARGET_DIR=/target` in the container, so build output never appears
  in `./target`. An empty `./target` is not a sign that nothing built.
- **`cargo deny check`** reads `deny.toml` -- licenses, RustSec advisories,
  duplicate crate versions, crate sources -- and the binary comes from the
  image, pinned by `ARG CARGO_DENY_VERSION` in the root `Dockerfile` the same
  way the toolchain is. So `command not found` here means the container was
  built before that arg existed: rebuild the image rather than installing
  cargo-deny yourself or concluding the gate is four steps.
- **`cargo doc`** is in the gate because `missing_docs` is a crate-wide lint,
  so docs are part of the build here. It catches what `cargo test` does not:
  a doc link is only *built* by the doc-tests, so a link into a private item
  is a rustdoc warning and nothing else. Fix such a link by widening the
  target's visibility if it belongs in the public docs, or by naming the item
  in backticks instead -- never by an `#[allow]`.
- CI (`.github/workflows/ci.yml`) runs four of the commands in the same
  image, so a local green should mean a remote green. `cargo deny check` is a
  separate `deny` job on a plain runner rather than a step there -- its
  comment says why -- which means the workflow is green only when *both* jobs
  are.

**Never wait in a loop.** Run a gate command in the foreground and read its
output. No `until`/`while` + `sleep` poll -- not on `pgrep`, not on a log
file, not on `gh pr checks`. Such a loop has hung this repo's agents twice: a
`pgrep -f "<pattern>"` matches the waiting shell's own command line, so it
never exits, and a caller is not woken while you run. If you are genuinely
blocked on something outside your control, say so and stop.

## Lints that will bite

`Cargo.toml` turns several lints on crate-wide, and `-D warnings` promotes
every one of them to a hard error in the gate:

- `missing_docs` -- every public item needs a doc comment. Write it when you
  add the item, not after clippy complains.
- `cast_possible_truncation`, `cast_sign_loss` -- reach for `TryFrom`/
  `try_into()` and handle the error, not an `as` cast. These are deliberate:
  in a work-stealing scheduler a silent wrap is a real bug.
- `undocumented_unsafe_blocks` -- every `unsafe { }` carries a `// SAFETY:`
  comment stating the invariant that makes it sound.
- `unsafe_op_in_unsafe_fn = deny` -- an `unsafe fn` body is *not* implicitly
  an unsafe block; write the inner `unsafe { }` explicitly.

Never silence one of these with an `#[allow]` to get a green gate. If an
`allow` is genuinely right, it is narrow (item-level, not module- or
crate-level) and carries a comment saying why.

## Attribution is a hard rule, not a nicety

A module translated closely from upstream **must** name its source on the
first line of its own `//!` doc comment:

```rust
//! Translated from `taskflow/core/executor.hpp` (Taskflow v4.1.0).
```

**The version is part of the line**, not decoration: upstream rewrites its
scheduler between major versions, so a bare "translated from `executor.hpp`"
is not an auditable claim. Take the version from the README's "Upstream pin"
section rather than from this file -- the pin is repo-wide and moving it is
its own change.

Silence therefore means "this module is our own design", so never add that
line to a module you invented, and never omit it from one you translated --
the absence is load-bearing information. See the README's "Provenance and
attribution" section.

Upstream's `3rd-party/` directory is under separate terms: nothing may be
ported out of it under `LICENSE-TASKFLOW`. Stop and ask instead.

## Reading the upstream file

**Upstream is not vendored in this repo** -- there is no local checkout to
`Read` or `Grep`, and you have no `WebFetch`. So never write a "Translated
from" line from memory of what a header contains; fetch the real file at the
pinned tag with `curl` instead:

```sh
curl -sSfL https://raw.githubusercontent.com/taskflow/taskflow/<tag>/taskflow/core/executor.hpp
```

`<tag>` is the README's "Upstream pin" tag. Save it under your scratchpad if
you need to read it more than once. If the fetch fails, say so and stop --
a provenance line you guessed at is worse than no port.

## Porting posture

The upstream API hands out `tf::Task` handles that alias into a graph the
caller still owns and mutates. That does not survive translation to Rust's
aliasing rules unchanged, so a port is a *design* step, not a transcription:
prefer index/ID handles into a graph the `Taskflow` owns over anything that
needs `unsafe` to reproduce C++ pointer semantics. If a faithful translation
would force `unsafe`, say so in the summary and propose the safe shape.

Rough order of the work overall: static graph -> executor -> control flow
(conditional/module tasks, subflows) -> the rest.

## What a doc comment says, and what it does not

The crate's docs are held to a contract. `DESIGN.md`'s "Documentation
contract" section is the long form of it and the authority wherever the two
read differently; this is the summary agents are reviewed against. It is a
rule about where each kind of sentence goes, which is what keeps the same
explanation from landing in four places:

- **An item doc (`///`)** states what the item is, the upstream name it
  translates (upstream's `Topology::_join_counter`), and any invariant a
  caller must keep -- a locking order, a lifetime, "never held across a drop".
  Nothing else.
- **Rationale** -- why this port differs from upstream, which alternative was
  rejected -- lives *once*: in the module's `//!` docs under a `# Design`
  heading, or in `DESIGN.md` when more than one module shares it. A field or
  type doc may point at it in half a line; it does not retell it.
- **History** -- "before #109 this was an `Arc<Flow>`", "found by #93" --
  belongs to the PR trail and the git log, not the source. Zero `#NNN`
  citations in the doc comments of production text, with one exception: **a
  `#NNN` in a *test's* doc comment is allowed**, naming the issue that set
  that case's bar, which is why `doc-ratio.sh` deliberately stops at the first
  column-0 `#[cfg(test)]` rather than reading past it. A `DESIGN.md` entry
  names its issue, and outside a test module that is the one place that does.
- **Trait impls** (`Hash`, `Debug`, `Drop`, `Display`) get one line on what is
  non-obvious, or nothing.
- **Inline `//` comments** explain a line a reader would otherwise question:
  an `Ordering`, a deliberate leak, a lock taken out of order. Not narration.
- The provenance `//!` line of the section above is untouched by all of this.
  It stays, first line of the module docs, version and all.

The figure is a file's **prose** lines at or under **1.2x its code lines**,
and neither **a doc example** nor **a module's `# Design` section** is prose:
the examples a public API carries and the rationale this contract sends to
`# Design` are both required by it, so charging either to the budget it obeys
would charge a file for obeying the rule. `devtools/doc-ratio.sh` measures it,
one row per production file (`src/**/*.rs`, the text above the test module)
with columns `prose`, `examples`, `design`, `code`, `ratio` and `#NNN`, then a
tree total; `ratio` is prose over code with examples and the `# Design`
section held out -- that section running from the `//! # Design` heading to the
next top-level `//! #` heading or the end of the `//!` block, with a `//! ##`
sub-heading staying inside it -- and a crate root is reported as
`(crate root)` and never flagged. It is a report, not a gate: it
always exits 0, no CI step reads it, and **1.2x is a guide for prose-heavy
implementation files rather than a bar** -- a file over it is one a reviewer
looks at, and a reason stated in its PR is an answer. What it is for is the
opposite case, the file that restates its own signatures, and for that the
number is usually enough on its own. Buying the number by cutting a
`# Design` section or deleting a doctest is refused, since that trades the
thing the contract is for against the measurement of it.

When you add a doc comment, write it to this contract. Do *not* rewrite
neighbouring docs to it in a PR about something else -- a documentation pass
is its own change, file by file, so that it can be reviewed as one. Such a
pass renames doctests, because a doctest's generated name embeds the line its
example starts on (`src/task.rs - task::TaskMut (line 349)`), so **"the same
set of test names before and after" means the same set of item paths with
` (line N)` stripped** -- and a PR that claims one says which it compared.
`devtools/test-names.sh snapshot <file>` takes that comparison and
`devtools/test-names.sh diff <before> <after>` reports it, stripping the
` (line N)` suffix for you and exiting non-zero on any name added or removed.

## Conventions

- Edition 2024. The toolchain is pinned by `ARG RUST_VERSION` in the root
  `Dockerfile` -- never `rustup update` or let a tool self-update; bump the
  arg and let the image rebuild.
- Unit tests live beside the code in `#[cfg(test)] mod tests`.
- Dependencies are a decision, not a convenience. The crate has two runtime
  ones today, both exact-pinned, plus what they pull in transitively, and the
  README's "Dependency policy" section is the authority on which crates that
  admits and why -- read it there rather than trusting a list repeated here,
  because two lists drift. Adding one means running a `cargo` command so
  `Cargo.lock` updates in the same change, satisfying `deny.toml` (the gate's
  `cargo deny check` is what enforces the policy now -- licenses, sources,
  duplicate versions), and calling it out in the summary.
- Match the surrounding code's comment density and voice. Comments here
  explain *why* a choice was made; they do not narrate the code.
- In an `examples/` or `tests/` file, a type the code never names goes in
  plain backticks, not in an intra-doc link. `cargo doc` does not build those
  targets, so the link is never rendered anywhere, while the `use` it needs to
  resolve is `unused_imports` under `clippy --all-targets -D warnings` --
  which an example that infers the type from a call (`executor.task_group()?`)
  cannot avoid by using it. Inside `src/` the link is the right spelling and
  the import is a real one.

## Stay in your lane

By default, do not commit, push, create or merge PRs, and do not tear down
worktrees -- report what you changed and what the gate said, and let the
caller ship it with the `pr` skill.

The exception is a caller whose prompt explicitly tells you to commit, push
and open the PR yourself. The `milestone` skill's `run` mode does exactly
that, because its orchestrator merges PRs it did not open and needs one per
issue to review. Take the grant only when the prompt states it, and only as
far as it goes: merging, the `automerge` label and worktree teardown stay the
caller's in every case.

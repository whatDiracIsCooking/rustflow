---
name: rust
description: >-
  Write, port or fix Rust in this crate. Use for implementing a rustflow
  module (graph, executor, control flow), translating a piece of upstream
  Taskflow into Rust, or chasing a clippy/fmt/test failure in the gate. Hands
  back a summary plus the gate's own output; it does not commit, push or open
  PRs -- the `pr` skill does that.
tools: Read, Write, Edit, Grep, Glob, Bash
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
```

Run all four and read the output before reporting back. Partial greens are
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
  cargo-deny yourself or concluding the gate is three steps.
- CI (`.github/workflows/ci.yml`) runs the first three commands in the same
  image, so a local green should mean a remote green. The fourth is a separate
  `deny` job on a plain runner rather than a fourth step there -- its comment
  says why -- which means the workflow is green only when *both* jobs are.

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
//! Translated from `taskflow/core/executor.hpp`.
```

Silence therefore means "this module is our own design", so never add that
line to a module you invented, and never omit it from one you translated --
the absence is load-bearing information. See the README's "Provenance and
attribution" section.

Upstream's `3rd-party/` directory is under separate terms: nothing may be
ported out of it under `LICENSE-TASKFLOW`. Stop and ask instead.

## Porting posture

The upstream API hands out `tf::Task` handles that alias into a graph the
caller still owns and mutates. That does not survive translation to Rust's
aliasing rules unchanged, so a port is a *design* step, not a transcription:
prefer index/ID handles into a graph the `Taskflow` owns over anything that
needs `unsafe` to reproduce C++ pointer semantics. If a faithful translation
would force `unsafe`, say so in the summary and propose the safe shape.

Rough order of the work overall: static graph -> executor -> control flow
(conditional/module tasks, subflows) -> the rest.

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

## Stay in your lane

Do not commit, push, create or merge PRs, and do not tear down worktrees --
report what you changed and what the gate said, and let the caller ship it
with the `pr` skill.

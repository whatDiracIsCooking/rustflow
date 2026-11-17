#!/usr/bin/env python3
"""SessionStart hook: announce which half of this repo's split environment the
session is running in -- the devcontainer (container half) or the bare host.

This repo has no host toolchain requirement: `cargo` exists only inside the
image (see the root Dockerfile). So the halves differ in what is runnable,
which is worth knowing up front rather than discovering through a
`cargo: command not found`:

  * On the HOST, `cargo` may well be absent. The entry points are
    `devtools/devcontainer.sh <cmd>` and `docker compose -f
    docker/compose.yaml run --rm check`, both of which drive the container.
  * INSIDE the container, `cargo` is on PATH and builds land in /target (a
    named volume), not ./target. So are `claude` and `gh`, which is what makes
    an in-container agent session possible at all. `docker` is NOT available
    there, so the wrapper and compose cannot be invoked from inside -- run
    them from the host.

Detection: the standard Docker marker file `/.dockerenv`. The result is emitted
as SessionStart `additionalContext`, so it lands in the session's context
rather than being shown once and lost.

Wired from the committed `.claude/settings.json` as a SessionStart hook. Fails
OPEN (silent, exit 0) on any error: a hook bug must never break session startup.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> None:
    if Path("/.dockerenv").exists():
        context = (
            "Environment: inside the devcontainer (the container half of this "
            "repo's split host/container setup). `cargo` is on PATH here and "
            "CARGO_TARGET_DIR=/target, a named volume -- so build output does "
            "NOT appear in ./target in the checkout. `claude` and `gh` are on "
            "PATH too. Both depend on forwarded host credentials: check "
            "`gh auth status` before relying on gh, and note that `git push` "
            "works only when GH_TOKEN was forwarded (no ssh client is "
            "installed, so this repo's SSH remote goes over an HTTPS rewrite "
            "that devtools/container-bootstrap.sh sets up) and never from a "
            "mounted .claude/worktrees/ checkout, whose .git names a host "
            "path. "
            "`docker` is not available here: devtools/devcontainer.sh and "
            "docker/compose.yaml are host-only, so do not try to run them "
            "from in here. The gate "
            "is `cargo fmt --all --check && cargo clippy --all-targets "
            "--all-features -- -D warnings && cargo test --all-features`."
        )
    else:
        context = (
            "Environment: the bare host (the host half of this repo's split "
            "host/container setup). The Rust toolchain lives in the container "
            "image, not here -- `cargo` may not exist on PATH at all. Use "
            "`devtools/devcontainer.sh check|test|shell` (interactive) or "
            "`docker compose -f docker/compose.yaml run --rm check` (batch); "
            "both build the repo's one root Dockerfile. If you do have a host "
            "cargo, it uses its own ./target and never shares with the "
            "container's /target volume."
        )

    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "SessionStart",
            "additionalContext": context,
        }
    }))
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        # Fail open: never let a hook bug break session startup.
        sys.exit(0)

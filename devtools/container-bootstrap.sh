#!/usr/bin/env bash
# Runs INSIDE the container, once per created container, doing the two things
# the image itself cannot: give git a way to push, and give Claude Code a
# config so it does not open on a login prompt.
#
# Both front ends call it, which is the point -- the logic lives here rather
# than twice:
#
#   devtools/devcontainer.sh   after `up` creates the container (see ensure_up)
#   .devcontainer/*.json       as the postCreateCommand
#
# Idempotent and quiet about it: safe to run on every `up`, and it never
# overwrites a config that already exists. Fails SOFT on everything except its
# own bugs -- a container you cannot push from is still a container you can
# build in, so nothing in here is worth refusing to start over.
set -uo pipefail

# Where the host's ~/.claude.json arrives. devcontainer.json bind-mounts it
# here read-only; devtools/devcontainer.sh streams it in to the same path. One
# path, two delivery mechanisms, so this script does not care which front end
# started the container.
SEED="${1:-/tmp/claude-seed.json}"
CONFIG_DIR="${CLAUDE_CONFIG_DIR:-$HOME/.claude}"
DST="$CONFIG_DIR/.claude.json"

# --- git: make `git push` possible ------------------------------------------
# The image has no ssh client and the container has no keys, so an SSH remote
# (git@github.com:...) cannot be pushed to from in here at all -- it fails with
# "ssh: not found". GH_TOKEN plus gh's credential helper is the way through,
# and `insteadOf` is what points the repo's existing SSH remote at it without
# rewriting the remote itself (which lives in the shared .git and would follow
# you back to the host).
#
# Global config here means /home/<user>/.gitconfig, inside the container, NOT
# the bind-mounted checkout -- the host's own git config is untouched.
#
# From `/`, not the workspace. Both commands below only touch --global config
# and want no repository, but git still runs its repo discovery first, and a
# BROKEN .git is a hard error rather than an absent one:
#
#   fatal: not a git repository: /host/path/.git/worktrees/<name>
#
# which is precisely what /workspace is when a .claude/worktrees/ checkout is
# the thing mounted -- its .git is a file naming an absolute HOST path. Setting
# up credentials is not the step that should care, so step out of the repo.
if [ -n "${GH_TOKEN:-}" ]; then
    if (cd / && gh auth setup-git); then
        (cd / && git config --global \
            url."https://github.com/".insteadOf "git@github.com:")
        echo "bootstrap: git will push over https as the GH_TOKEN user"
    else
        # Not silenced: the one time this failed in testing, gh's own message
        # was the whole diagnosis.
        echo "bootstrap: gh auth setup-git failed (above) -- push will not work" >&2
    fi
else
    echo "bootstrap: no GH_TOKEN in env, so git cannot push from in here" \
         "(export it on the host; read-only git still works)"
fi

# --- Claude Code: seed the config so there is no login prompt ----------------
# ~/.claude.json is NOT part of the config directory -- it sits beside it -- so
# the /claude-config volume does not carry it and a fresh container looks like
# a fresh install. Seeding it from the host's copy is what keeps the login.
#
# A COPY, never a shared mount: that file is one monolithic JSON holding every
# project's state, rewritten whole on each change, so host and container
# sharing it read-write is last-writer-wins. The seed mount is read-only and
# this writes only to the volume.
if [ -e "$DST" ]; then
    exit 0
fi
if [ ! -r "$SEED" ]; then
    echo "bootstrap: no readable $SEED, so Claude Code will ask you to /login" \
         "(that answer then persists in the volume)"
    exit 0
fi

# python3 is in the image for the SessionStart hook; reuse it rather than add
# jq for one transform.
python3 - "$SEED" "$DST" <<'PY' || exit 0
import json
import sys

seed, dst = sys.argv[1], sys.argv[2]

try:
    with open(seed) as fh:
        cfg = json.load(fh)
except (OSError, ValueError) as exc:
    print(f"bootstrap: unusable seed ({exc}), skipping", file=sys.stderr)
    raise SystemExit(1)

# The host's installation bookkeeping describes a binary at
# ~/.local/bin/claude. That path does not exist in here -- the Dockerfile puts
# claude at /usr/local/bin/claude -- and the startup health check turns the
# mismatch into "claude command at ... missing or broken". Absent, installMethod
# derives to "unknown" and the check does not run.
cfg.pop("installMethod", None)
cfg.pop("autoUpdatesProtectedForNative", None)

# Pinned false rather than dropped: it defaults to TRUE when absent, and
# /usr/local/bin/claude is root-owned, so an enabled auto-update would fail
# noisily on every start. The image's ARG CLAUDE_CODE_VERSION is the pin; an
# update behind the image's back is exactly what that ARG exists to prevent.
cfg["autoUpdates"] = False

# Trust for the workspace, which the seed cannot carry: the `projects` map is
# keyed by absolute path, the host's key is the checkout's host path, and in
# here the same checkout is /workspace. Without an entry Claude Code opens on
# the trust dialog and -- until it is answered -- ignores the whole
# permissions.allow list in the repo's committed .claude/settings.json.
#
# Deliberately MINIMAL: trust and nothing else. The host's own entry for this
# repo also holds its accumulated allowedTools, and importing those would let
# host-local approvals leak in; .claude/settings.json is the reviewed source
# for what is allowed, and it is already in the checkout.
#
# This answers a safety prompt without a human, so note what it is scoped to:
# one path, in a volume, derived at runtime from the user's own host config --
# never a committed file. No seed, no trust entry.
cfg.setdefault("projects", {}).setdefault("/workspace", {})[
    "hasTrustDialogAccepted"
] = True

with open(dst, "w") as fh:
    json.dump(cfg, fh, indent=2)
PY

echo "bootstrap: seeded $DST from $SEED"

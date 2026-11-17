# The one image for this repo. Both front ends build it, so a result in the
# devcontainer and a result from docker/compose.yaml mean the same thing:
#
#   .devcontainer/devcontainer.json   interactive / IDE / agent-facing
#   docker/compose.yaml               one-shot batch runs (CI, fmt+clippy+test)
#
# RUST_VERSION is the project's toolchain pin -- there is no
# rust-toolchain.toml on purpose. A `channel = "stable"` file would make rustup
# download a second toolchain on first use inside a container that already has
# one, because the official image installs its toolchain under the exact
# version name (1.NN.N-x86_64-...), not under the `stable` alias.
ARG RUST_VERSION=1.100-slim-bookworm
FROM rust:${RUST_VERSION}

# The official rust images install with `--profile minimal`, so clippy and
# rustfmt are NOT in the base -- they have to be added explicitly.
RUN rustup component add clippy rustfmt

# --- What the Rust build needs ----------------------------------------------
# git: the devcontainer runs git against the bind-mounted checkout.
# pkg-config + libssl-dev: the common native-dependency pair, so the first
# crate that needs them doesn't fail a build days from now.
#
# .github/workflows/ci.yml installs THIS list (and only this list) into the
# bare rust image it runs in; keep the two equal. The agent tooling below is
# deliberately not mirrored there -- CI runs cargo, not an agent.
RUN apt-get update && apt-get install -y --no-install-recommends \
        git ca-certificates pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# --- What the in-container agent needs --------------------------------------
# curl: fetches the Claude Code installer below.
# python3: `.claude/settings.json` wires devtools/hooks/session-env.py as a
#   SessionStart hook. Without an interpreter that hook fails open (silently),
#   and a session in here starts with no idea which half of the split
#   host/container setup it is in -- which is precisely the half where it
#   matters, since `docker` is absent.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl python3 \
    && rm -rf /var/lib/apt/lists/*

# GitHub CLI, from the project's own .deb rather than apt: bookworm's archive
# carries no `gh` at all, and the alternative (cli.github.com's apt repo) means
# a signing key plus a sources entry for one package. Pinned for the same
# reason RUST_VERSION is -- `latest` would make the image's contents depend on
# the day it was built.
#
# Auth is NOT baked in: `gh` reads GH_TOKEN from the environment, which
# docker/compose.yaml forwards from the host only when the host has it set.
ARG GH_VERSION=2.104.0
RUN arch="$(dpkg --print-architecture)" \
 && curl -fsSLo /tmp/gh.deb \
      "https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_${arch}.deb" \
 && dpkg -i /tmp/gh.deb \
 && rm -f /tmp/gh.deb \
 && gh --version

# Claude Code, pinned the same way. The official installer puts a versioned
# binary under $HOME/.local/share/claude/versions/ and symlinks
# $HOME/.local/bin/claude at it; that is a per-user layout, and this image's
# runtime user is a build argument whose home directory is therefore not known
# to any ENV line here. So install under a throwaway HOME, copy the single
# self-contained binary the symlink resolves to onto the PATH every user
# already has, and drop the rest -- including the root-owned config the
# installer leaves behind, which must not be mistaken for the real one.
#
# `readlink -f` rather than interpolating ${CLAUDE_CODE_VERSION} into the path:
# it resolves whatever got installed, so `stable` works here too.
ARG CLAUDE_CODE_VERSION=2.1.257
RUN set -eu \
 && export HOME=/tmp/claude-install \
 && curl -fsSL https://claude.ai/install.sh | bash -s "${CLAUDE_CODE_VERSION}" \
 && install -m 0755 "$(readlink -f "$HOME/.local/bin/claude")" /usr/local/bin/claude \
 && rm -rf "$HOME" \
 && claude --version

# /usr/local/bin/claude is root-owned, so the self-updater could not replace it
# even if it tried -- and it should not: the version above is this image's pin,
# and an update would make two containers built from one Dockerfile disagree.
# Bump CLAUDE_CODE_VERSION and rebuild instead.
ENV DISABLE_AUTOUPDATER=1

# Where Claude Code keeps credentials, history and project state. It defaults
# to $HOME/.claude, which lives in the container's writable layer and would be
# thrown away by every `down`/`rebuild` -- i.e. you would log in again on every
# recreate. Pointing it at a fixed path lets docker/compose.yaml and
# devcontainer.json back it with a named volume, exactly as they do for the
# cargo registry: container-side state, deliberately NOT in the bind mount
# (where it would collide with the host's own .claude/ and land in git status).
ENV CLAUDE_CONFIG_DIR=/claude-config

# Run as the host user so files written into the bind mount are owned by you
# and not by root. A default of 1000 is only a default: pass the real values
# (docker/env.example shows how) on a host where you are not uid 1000.
ARG HOST_UID=1000
ARG HOST_GID=1000
RUN if ! getent group ${HOST_GID} >/dev/null; then groupadd -g ${HOST_GID} dev; fi \
 && if ! getent passwd ${HOST_UID} >/dev/null; then \
        useradd -m -u ${HOST_UID} -g ${HOST_GID} -s /bin/bash dev; \
    fi

# CARGO_HOME stays at the image default (/usr/local/cargo) so the rustup
# proxies on PATH keep working; only the registry below is a volume. Builds go
# to /target instead of /workspace/target so a container build and a host build
# can never poison each other's artifacts through the bind mount.
ENV CARGO_TARGET_DIR=/target
RUN mkdir -p /target /usr/local/cargo/registry ${CLAUDE_CONFIG_DIR} \
 && chown -R ${HOST_UID}:${HOST_GID} /target /usr/local/cargo ${CLAUDE_CONFIG_DIR}

USER ${HOST_UID}:${HOST_GID}
WORKDIR /workspace

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

# cargo-deny, the gate's fourth step (see deny.toml). Pinned like everything
# else here, and from source rather than a release tarball because `cargo
# install --locked` is itself the thing being asserted: the binary is built
# from the lockfile the authors published, with this image's toolchain.
#
# It lands in /usr/local/cargo/bin -- CARGO_HOME, which stays at the image
# default, so it is on the PATH of whatever uid runs later. The advisory
# database it fetches at run time goes to $CARGO_HOME/advisory-dbs, outside
# the registry volume, so each container re-clones it once; that is seconds
# and it must be fresh to mean anything.
#
# The crate sources the install downloaded are dropped again: both front ends
# mount a named volume over /usr/local/cargo/registry, so a copy baked into
# the layer is shadowed at run time and pure weight.
#
# CI does NOT use this image (see .github/workflows/ci.yml) and gets the same
# version from a pinned action instead -- keep the two equal.
ARG CARGO_DENY_VERSION=0.21.0
RUN cargo install cargo-deny --locked --version ${CARGO_DENY_VERSION} \
 && cargo deny --version \
 && rm -rf /usr/local/cargo/registry/*

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

# Pin HOME rather than inherit whatever /etc/passwd says for the runtime uid.
# Two reasons, and the second is the load-bearing one:
#
#   * The runtime user is a build ARG. `useradd` below only runs when that uid
#     is not already taken, so on a host uid that collides with a Debian system
#     user (anything under 1000) HOME would be that account's home instead.
#   * docker/compose.yaml and devcontainer.json have to NAME this path to mount
#     Claude Code's config dir at it, and neither can ask the image what the
#     uid's home turned out to be. Pinned here, $HOME/.claude is knowable.
#
# Deliberately NOT CLAUDE_CONFIG_DIR=/claude-config, which an earlier revision
# of this file had. Claude Code keeps .claude.json BESIDE its config directory,
# not in it -- and that separation is exactly what makes the config dir safe to
# share with the host: ~/.claude is mounted from the host (so the container is
# logged in because you are) while ~/.claude.json stays container-private (it
# is one monolithic JSON rewritten whole, so sharing it is last-writer-wins).
# Setting CLAUDE_CONFIG_DIR collapses the two back into one directory and the
# container's copy then lands in the host's config dir.
ENV HOME=/home/dev

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
# $HOME/.claude is created so that an unmounted container still has a writable
# config dir (Claude Code then behaves like a fresh install and asks you to log
# in) rather than failing on a missing path. Both front ends mount the host's
# over it.
RUN mkdir -p /target /usr/local/cargo/registry ${HOME}/.claude \
 && chown -R ${HOST_UID}:${HOST_GID} /target /usr/local/cargo ${HOME}

USER ${HOST_UID}:${HOST_GID}
WORKDIR /workspace

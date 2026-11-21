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
# rustfmt are NOT in the base -- they have to be added explicitly. llvm-tools
# is the llvm-profdata/llvm-cov pair that cargo-llvm-cov (below) drives; it
# must come from the SAME toolchain that compiles the instrumented tests, which
# is why it is a rustup component here and not an apt package.
RUN rustup component add clippy rustfmt llvm-tools

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

# Coverage and mutation tooling, installed the same way for the same reasons.
# Neither is part of the gate: `devtools/coverage.sh` drives cargo-llvm-cov
# (which needs the llvm-tools component added above) and `cargo mutants` is
# run by hand when a test suite's bite is in question. They live in the image
# so that measuring either is a script and not a one-off install -- a number
# from a tool version nobody recorded is not comparable to the next one.
#
# CI does not run them and the stock rust image it uses has neither, so the
# only check that this block builds is a host-side rebuild of the image
# (`devtools/devcontainer.sh rebuild`) after a change here.
ARG CARGO_LLVM_COV_VERSION=0.9.1
ARG CARGO_MUTANTS_VERSION=27.1.0
RUN cargo install cargo-llvm-cov --locked --version ${CARGO_LLVM_COV_VERSION} \
 && cargo install cargo-mutants --locked --version ${CARGO_MUTANTS_VERSION} \
 && cargo llvm-cov --version \
 && cargo mutants --version \
 && rm -rf /usr/local/cargo/registry/*

# Miri, which is a rustup component rather than a crate and nightly-only: on
# this image's stable pin `cargo miri --version` reports that the component
# "is not available for the 1.100.0 toolchain". So a second toolchain is
# installed beside the pinned stable one, used for nothing but Miri, and its
# date is as much a pin as RUST_VERSION is -- a bare `nightly` would make the
# interpreter's verdict depend on the day the image was built. `rust-src` comes
# with it because Miri interprets the standard library rather than linking a
# precompiled one, so `cargo miri` builds a sysroot from source on first use
# and fails without those sources.
#
# `.github/workflows/miri.yml` runs this install line and the command below
# weekly; the date here and the date there are kept equal by hand, exactly as
# RUST_VERSION and the images in ci.yml and loom.yml are, and that workflow is
# the only check that this layer works. In the container:
#
#   cargo +nightly-2026-11-18 miri test --lib -- executor::notifier::tests
#
# docs/development.md says which modules that covers and why not all of them.
ARG MIRI_NIGHTLY=nightly-2026-11-18
RUN rustup toolchain install ${MIRI_NIGHTLY} \
        --profile minimal --component miri --component rust-src \
 && cargo +${MIRI_NIGHTLY} miri --version

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

# --- What the upstream C++ comparison needs ---------------------------------
# `devtools/upstream-bench.sh` builds Taskflow v4.1.0's own benchmark binaries
# from a clone outside this repo, so that BENCHMARKS.md's `upstream C++` column
# can be filled by upstream's executor on this box rather than left empty. That
# needs a C++ toolchain the Rust image has no other use for:
#
#   g++   the base image carries `gcc` but no C++ compiler at all, and upstream
#         is header-only C++20.
#   cmake upstream is built through CMake, 3.18 or newer by its own
#         `cmake_minimum_required`.
#   make  needed TWICE over, which is why it is spelled out rather than left to
#         chance: cmake's default generator is Unix Makefiles, and upstream's
#         `benchmarks/CMakeLists.txt` shells out to `make` at CONFIGURE time to
#         build the TBB 2020.3 it vendors under `3rd-party/tbb` (every
#         benchmark target links it, ours included). Debian only *recommends*
#         make for cmake, and this file installs with --no-install-recommends.
#
# NOT version-pinned, unlike CARGO_DENY_VERSION and friends above, because
# bookworm's archive is itself the pin: a Debian release serves one g++ and one
# cmake for its life, so RUST_VERSION's `-bookworm` suffix already fixes these
# as firmly as an ARG would. As built today that is g++ 12.2.0 and cmake
# 3.25.1; `upstream-bench.sh` prints both, so a run that disagrees with this
# line says the base image moved.
#
# Placed LAST on purpose, after the cargo installs rather than in the toolchain
# layer at the top. Two reasons:
#
#   * The layer at the top is mirrored verbatim by .github/workflows/ci.yml.
#     Adding a C++ toolchain there would cost every PR an apt install of
#     something no CI job runs -- CI runs cargo, and the upstream comparison is
#     a measurement taken by hand on one box.
#   * Everything above this line is expensive to rebuild (three
#     `cargo install`s from source). Appending here leaves all of it cached,
#     so adding these three packages is a rebuild of seconds rather than
#     minutes.
#
# As with the coverage tooling above, nothing in the gate or in CI builds this
# image, so the only check that this layer works is a host-side
# `devtools/devcontainer.sh rebuild`; `upstream-bench.sh` names that command
# when it finds any of the three missing.
RUN apt-get update && apt-get install -y --no-install-recommends \
        g++ cmake make \
    && rm -rf /var/lib/apt/lists/*

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

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

# git: the devcontainer runs git against the bind-mounted checkout.
# pkg-config + libssl-dev: the common native-dependency pair, so the first
# crate that needs them doesn't fail a build days from now.
RUN apt-get update && apt-get install -y --no-install-recommends \
        git ca-certificates pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

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
RUN mkdir -p /target /usr/local/cargo/registry \
 && chown -R ${HOST_UID}:${HOST_GID} /target /usr/local/cargo

USER ${HOST_UID}:${HOST_GID}
WORKDIR /workspace

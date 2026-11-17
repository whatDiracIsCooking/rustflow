#!/usr/bin/env bash
# Thin wrapper over docker/compose.yaml for the interactive container.
#
# It exists for one reason compose cannot handle itself: compose.yaml requires
# HOST_UID/HOST_GID (deliberately, see its header) and this exports the real
# ones, so no invocation can accidentally run as root and leave root-owned
# files in the bind mount.
#
#   devtools/devcontainer.sh up            start the container
#   devtools/devcontainer.sh shell         interactive bash inside it
#   devtools/devcontainer.sh shell -c CMD  run one command inside it
#   devtools/devcontainer.sh claude [ARGS] Claude Code inside it
#   devtools/devcontainer.sh check         the gate: fmt + clippy + test + deny + doc
#   devtools/devcontainer.sh test [ARGS]   cargo test
#   devtools/devcontainer.sh bench [ARGS]  cargo bench (the criterion suite)
#   devtools/devcontainer.sh rebuild       rebuild the image after a Dockerfile edit
#   devtools/devcontainer.sh down          stop it (volumes survive)
#   devtools/devcontainer.sh nuke          stop it AND drop every volume
#   devtools/devcontainer.sh status        what is running
#
# VS Code / IDE users can instead open .devcontainer/devcontainer.json; it
# builds the same root Dockerfile and runs the same
# devtools/container-bootstrap.sh (git push credentials + the Claude Code
# config seed) as its postCreateCommand.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker/compose.yaml"

export HOST_UID="${HOST_UID:-$(id -u)}"
export HOST_GID="${HOST_GID:-$(id -g)}"

dc() { docker compose -f "$COMPOSE_FILE" "$@"; }

# compose binds ~/.claude into the dev container as Claude Code's config dir.
# docker does NOT refuse a missing bind source -- it creates it owned by ROOT,
# and the failure then surfaces later as a permission error that names no
# mount. So make it here, as the user, where it is cheap and obvious.
ensure_claude_config_dir() {
    local src="${CLAUDE_CONFIG_SRC:-$HOME/.claude}"
    [ -d "$src" ] || mkdir -p "$src"
}

# Hand the host's ~/.claude.json to the container, then run the in-container
# bootstrap (git push credentials + the Claude Code config seed; see that
# script's header). Streamed in over `exec` rather than bind-mounted, which the
# IDE front end does instead: compose would have to name the host path in
# compose.yaml, and docker refuses to start a container whose bind source does
# not exist -- so a host that has never run claude could not bring the
# container up at all. Here the file simply may or may not be there.
#
# `exec -T`: no pty, or the redirect below is not a pipe. Both halves are
# allowed to fail; a container you cannot push from still builds.
bootstrap() {
    if [ -r "$HOME/.claude.json" ]; then
        dc exec -T dev bash -c 'cat > /tmp/claude-seed.json' \
            < "$HOME/.claude.json" || true
    fi
    dc exec -T dev bash /workspace/devtools/container-bootstrap.sh || true
}

# Bring `dev` up only if it isn't already; `up -d` on a running container is a
# no-op but still prints noise on every single command.
#
# The bootstrap runs only on the branch that created the container, because
# what it writes (/home/<user>/.gitconfig and the seeded config) lives in the
# container's own filesystem and so is exactly what a recreate loses. It is
# idempotent anyway.
ensure_up() {
    if [ -z "$(dc ps -q dev 2>/dev/null)" ]; then
        ensure_claude_config_dir
        dc up -d dev
        bootstrap
    fi
}

# `bash -c`, never `bash -lc`: a login shell sources /etc/profile, which resets
# PATH and drops the image's /usr/local/cargo/bin.
in_dev() { ensure_up; dc exec dev bash -c "$1"; }

cmd="${1:-shell}"
shift || true

case "$cmd" in
    up)      ensure_claude_config_dir; dc up -d dev; bootstrap ;;
    shell)
        ensure_up
        if [ "${1:-}" = "-c" ]; then
            shift
            dc exec dev bash -c "$*"
        else
            dc exec dev bash
        fi
        ;;
    # Not routed through in_dev: that wraps the command in `bash -c`, and
    # Claude Code is a TUI that needs the terminal handed straight to it.
    # `exec` (no -T) keeps the TTY attached; a bash -c layer in between breaks
    # arrow keys and resize.
    #
    # Credentials come from the rustflow-claude-config volume, so the first run
    # in a fresh volume needs an interactive `/login` once -- and only once,
    # since `down` and `rebuild` both leave the volume standing. Already have
    # ANTHROPIC_API_KEY or CLAUDE_CODE_OAUTH_TOKEN exported on the host?
    # compose.yaml forwards it and there is nothing to log in to.
    claude)
        ensure_up
        dc exec dev claude "$@"
        ;;
    # The fourth command needs cargo-deny, which the image installs pinned by
    # ARG CARGO_DENY_VERSION. CI runs it as a separate job on a plain runner
    # (see .github/workflows/ci.yml) because the image CI uses is the stock
    # rust one; the command itself is the same. The fifth needs nothing but
    # rustdoc, and denies its warnings through RUSTDOCFLAGS so that a plain
    # `cargo doc` stays usable while a doc link is being written.
    check)
        in_dev 'set -e
                cargo fmt --all --check
                cargo clippy --all-targets --all-features -- -D warnings
                cargo test --all-features
                cargo deny check
                RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features'
        ;;
    test)    in_dev "cargo test --all-features $*" ;;
    # Not part of `check`, deliberately: a benchmark answers "how fast", which
    # is not a pass/fail, and criterion's own statistics need a quiet machine
    # to mean anything -- see BENCHMARKS.md. What CI holds the suite to is that
    # it still compiles, which `cargo clippy --all-targets` in the gate above
    # already does.
    #
    # ARGS land where `cargo bench`'s own do: a bare word is forwarded to the
    # criterion harness as an id filter (`bench linear_chain`), and anything
    # after `--` is a harness option -- `bench -- --save-baseline mutex` then
    # `bench -- --baseline mutex` being how a before/after pair is collected.
    bench)   in_dev "cargo bench --all-features $*" ;;
    fmt)     in_dev 'cargo fmt --all' ;;
    rebuild) ensure_claude_config_dir \
                 && dc build --no-cache dev \
                 && dc up -d --force-recreate dev && bootstrap ;;
    down)    dc down ;;
    # Drops the build caches and the two shadow volumes. NOT your Claude Code
    # login: that lives in the host's ~/.claude, which is a bind, and `down -v`
    # does not touch binds.
    nuke)    dc down -v ;;
    status)  dc ps ;;
    *)
        echo "unknown command: $cmd" >&2
        sed -n '3,20p' "${BASH_SOURCE[0]}" >&2
        exit 2
        ;;
esac

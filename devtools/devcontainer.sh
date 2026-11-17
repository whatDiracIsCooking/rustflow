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
#   devtools/devcontainer.sh check         fmt + clippy + test (the CI gate)
#   devtools/devcontainer.sh test [ARGS]   cargo test
#   devtools/devcontainer.sh rebuild       rebuild the image after a Dockerfile edit
#   devtools/devcontainer.sh down          stop it (volumes survive)
#   devtools/devcontainer.sh nuke          stop it AND drop every volume, the
#                                          Claude Code login included
#   devtools/devcontainer.sh status        what is running
#
# VS Code / IDE users can instead open .devcontainer/devcontainer.json; it
# builds the same root Dockerfile.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker/compose.yaml"

export HOST_UID="${HOST_UID:-$(id -u)}"
export HOST_GID="${HOST_GID:-$(id -g)}"

dc() { docker compose -f "$COMPOSE_FILE" "$@"; }

# Bring `dev` up only if it isn't already; `up -d` on a running container is a
# no-op but still prints noise on every single command.
ensure_up() {
    if [ -z "$(dc ps -q dev 2>/dev/null)" ]; then
        dc up -d dev
    fi
}

# `bash -c`, never `bash -lc`: a login shell sources /etc/profile, which resets
# PATH and drops the image's /usr/local/cargo/bin.
in_dev() { ensure_up; dc exec dev bash -c "$1"; }

cmd="${1:-shell}"
shift || true

case "$cmd" in
    up)      dc up -d dev ;;
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
    check)
        in_dev 'set -e
                cargo fmt --all --check
                cargo clippy --all-targets --all-features -- -D warnings
                cargo test --all-features'
        ;;
    test)    in_dev "cargo test --all-features $*" ;;
    fmt)     in_dev 'cargo fmt --all' ;;
    rebuild) dc build --no-cache dev && dc up -d --force-recreate dev ;;
    down)    dc down ;;
    nuke)    dc down -v ;;
    status)  dc ps ;;
    *)
        echo "unknown command: $cmd" >&2
        sed -n '3,20p' "${BASH_SOURCE[0]}" >&2
        exit 2
        ;;
esac

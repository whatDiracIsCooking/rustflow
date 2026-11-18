#!/usr/bin/env bash
# Measures line/region coverage of the test suite with cargo-llvm-cov and
# prints the per-file table plus the TOTAL line.
#
#   devtools/coverage.sh            # the table, on stdout
#   devtools/coverage.sh --html     # the table, then an HTML report as well
#
# `tests/`, `examples/` and `benches/` are excluded from the denominator: the
# question this answers is how much of `src/` the suite exercises, and an
# integration test's own lines are 100% covered by definition of having run.
# The regex matches those three as path components so it holds whether
# llvm-cov sees the path absolute or relative; `src/` has no directory by any
# of those names, so nothing under it is caught by accident.
#
# Output goes under CARGO_TARGET_DIR -- `/target` inside the container, set
# by the Dockerfile, so an empty ./target afterwards is normal: the
# instrumented build lands in $CARGO_TARGET_DIR/llvm-cov-target and the HTML
# report in $CARGO_TARGET_DIR/llvm-cov/html/index.html.
#
# This is a REPORT, not a gate. It exits non-zero only when the measurement
# itself cannot run -- most likely because the image predates the
# `CARGO_LLVM_COV_VERSION` pin in the Dockerfile, which the message below
# says how to fix. Do NOT `cargo install` the tool by hand to get past it:
# the pin exists so that two coverage numbers are from the same tool.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

html=0
for arg in "$@"; do
    case "$arg" in
        --html) html=1 ;;
        -h|--help)
            sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed '$d' | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "coverage: unknown argument '$arg' (only --html is accepted)" >&2
            exit 2
            ;;
    esac
done

if ! cargo llvm-cov --version >/dev/null 2>&1; then
    cat >&2 <<'MSG'
coverage: `cargo llvm-cov` is not installed in this image.

The Dockerfile installs it, pinned by ARG CARGO_LLVM_COV_VERSION, together
with the `llvm-tools` rustup component it drives. An image that lacks it was
built before that pin existed. Rebuild the image from the host --

    devtools/devcontainer.sh rebuild

-- rather than installing the tool in this container by hand, so that the
version measured with is the one the Dockerfile records.
MSG
    exit 1
fi

exclude='(^|/)(tests|examples|benches)/'
target_dir="${CARGO_TARGET_DIR:-$root/target}"

echo "coverage: cargo llvm-cov $(cargo llvm-cov --version | awk '{print $2}'), output under $target_dir"
cargo llvm-cov --all-features --ignore-filename-regex "$exclude"

if [ "$html" -eq 1 ]; then
    # `report` reuses the profiles the run above wrote rather than running the
    # suite a second time; it needs the same exclusion to describe the same
    # denominator as the table.
    cargo llvm-cov report --html --ignore-filename-regex "$exclude"
    echo "coverage: HTML report at $target_dir/llvm-cov/html/index.html"
fi

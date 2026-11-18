#!/usr/bin/env bash
# Builds and runs every examples/*.rs in release mode, one at a time, failing
# on the first non-zero exit.
#
#   devtools/run-examples.sh
#
# Discovers its targets by glob rather than by a list: every example in this
# milestone (#190's own two plus #194-#200's) asserts its own result and
# exits non-zero on failure (see the README's "Examples" section), so a new
# `examples/foo.rs` needs no edit here and none in the `examples` job of
# `.github/workflows/ci.yml` either -- both already run whatever the glob
# below finds.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

shopt -s nullglob
examples=(examples/*.rs)

if [ "${#examples[@]}" -eq 0 ]; then
    echo "run-examples: no examples/*.rs found" >&2
    exit 1
fi

passed=()
for path in "${examples[@]}"; do
    name="$(basename "$path" .rs)"
    echo "run-examples: running $name"
    cargo run --release --example "$name"
    passed+=("$name")
done

echo "run-examples: passed: ${passed[*]}"

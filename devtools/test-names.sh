#!/bin/sh
# Snapshots and compares the set of test item paths, so that a change which
# promises "the same set of test names before and after" is checked one way
# instead of once per issue.
#
#   devtools/test-names.sh snapshot before.txt   # on the base tree
#   # ... make the change ...
#   devtools/test-names.sh snapshot after.txt
#   devtools/test-names.sh diff before.txt after.txt
#
# `snapshot` writes the sorted test item paths that
# `cargo test --all-features -- --list` reports -- every test binary plus the
# doctests -- with the doctest ` (line N)` suffix stripped. That strip is the
# comparison DESIGN.md's documentation contract defines: a doctest's generated
# name embeds the line its example starts on, so rewording one paragraph
# renames every doctest below it in the file and a literal comparison of two
# `--list` outputs can never hold for a docs pass. It is a full test build,
# since `--list` has to link the binaries it asks; nothing is run.
#
# `diff` prints what was removed (`-`) and what was added (`+`) and exits
# non-zero if either list is non-empty, so a PR can quote it as its acceptance
# check. The comparison is a *multiset* one rather than a set one: an
# integration test's item path does not name the binary it lives in, so
# `basics` and `future` are each several different tests in today's tree, and
# one item carrying two examples lists its path twice. Folding the duplicates
# would let a renamed twin cancel against its sibling and read as unchanged.
#
# No snapshot is stored in the repo. One is of a tree its reader has in front
# of them, and a committed one would be a second copy of the test list to keep
# in step with the tests.
#
# Two things a reader comparing counts should know: the `tests/loom_*.rs`
# models list nothing unless `cfg(loom)` is set, and `--all-features` is part
# of the command because the gate's `cargo test` is.
#
# Nothing here is outside POSIX sh plus cargo, awk and sort, so it runs in the
# container, on a host and in a worktree with no setup.
set -u

usage() {
    cat >&2 <<'USAGE'
usage: devtools/test-names.sh snapshot <file>
       devtools/test-names.sh diff <before> <after>
USAGE
}

# The repo root comes from this script's own location rather than from $PWD, so
# a snapshot taken from a subdirectory is of the same tree.
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd) || exit 2

# Names in the first file that the second does not match copy for copy, one
# line per surplus copy, C-sorted. Neither input need be sorted.
only_in() {
    awk '
        NR == FNR { before[$0]++; next }
        { after[$0]++ }
        END {
            for (name in before) {
                surplus = before[name] - after[name]
                for (i = 0; i < surplus; i++) { print name }
            }
        }
    ' "$1" "$2" | LC_ALL=C sort
}

lines() {
    awk 'END { print NR }' "$1"
}

snapshot() {
    out=$1
    # cargo runs in the repo root, so a relative output path -- which belongs
    # to the caller's directory -- is resolved before the cd.
    case $out in
        /*) ;;
        *) out=$PWD/$out ;;
    esac
    raw=$out.list.$$
    part=$out.part.$$

    if ! (CDPATH= cd -- "$root" && cargo test --all-features -- --list) >"$raw"
    then
        echo "test-names: cargo test -- --list failed; $out not written" >&2
        rm -f "$raw"
        return 1
    fi

    awk '
        /: test$/ || /: bench$/ {
            name = $0
            sub(/: (test|bench)$/, "", name)
            sub(/ \(line [0-9]+\)$/, "", name)
            print name
        }
    ' "$raw" | LC_ALL=C sort >"$part"

    # libtest ends each target with "N tests, M benchmarks", so the extraction
    # can be checked against the harness's own count instead of trusted. A
    # mismatch means the list format moved under the pattern above, and a
    # snapshot that is quietly missing names is worse than none.
    want=$(awk '
        /^[0-9]+ tests?, [0-9]+ benchmarks?$/ { tests += $1; benches += $3 }
        END { print tests + benches + 0 }
    ' "$raw")
    got=$(lines "$part")
    if [ "$want" -ne "$got" ]; then
        echo "test-names: extracted $got names but --list counted $want;" \
            "the libtest list format has changed and the snapshot would be" \
            "incomplete -- $out not written" >&2
        rm -f "$raw" "$part"
        return 1
    fi

    mv -- "$part" "$out"
    rm -f "$raw"
    echo "test-names: $got names -> $out"
}

compare() {
    for file in "$1" "$2"; do
        if [ ! -r "$file" ]; then
            echo "test-names: cannot read $file" >&2
            return 2
        fi
    done

    removed=$(only_in "$1" "$2")
    added=$(only_in "$2" "$1")
    if [ -z "$removed" ] && [ -z "$added" ]; then
        echo "test-names: unchanged -- $(lines "$1") names"
        return 0
    fi

    if [ -n "$removed" ]; then
        printf '%s\n' "$removed" | sed 's/^/- /'
    fi
    if [ -n "$added" ]; then
        printf '%s\n' "$added" | sed 's/^/+ /'
    fi
    echo "test-names: changed -- $(lines "$1") names before," \
        "$(lines "$2") after" >&2
    return 1
}

case ${1:-} in
    snapshot)
        [ "$#" -eq 2 ] || { usage; exit 2; }
        snapshot "$2"
        ;;
    diff)
        [ "$#" -eq 3 ] || { usage; exit 2; }
        compare "$2" "$3"
        ;;
    *)
        usage
        exit 2
        ;;
esac

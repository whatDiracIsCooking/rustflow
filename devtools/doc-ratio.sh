#!/bin/sh
# Reports how much of each production source file is prose, how much is doc
# example and how much is code, against the documentation contract in
# DESIGN.md: a file's prose lines at or under 1.2x its code lines.
#
#   devtools/doc-ratio.sh                   # every src/**/*.rs, plus a total
#   devtools/doc-ratio.sh src/executor/runtime.rs src/graph.rs
#
# This is a REPORT, not a gate: it exits 0 whatever it finds, so an acceptance
# check or a PR body can quote its numbers without the script deciding what
# passes. Nothing here is outside POSIX sh plus find/sort/awk, so it runs in
# the container, on a host and in a worktree with no setup at all.
#
# What is counted, per file, in the text ABOVE the test module:
#
#   prose     comment lines (///, //! or //) that are not part of a doc
#             example -- the text a reader reads
#   examples  comment lines inside a ```-fenced block in a doc comment, plus
#             the two fences themselves; prose + examples is every comment
#             line, which is what this column's absence used to report
#   code      every other non-blank line, except a line that is only an
#             attribute (#[...] / #![...]) -- those are neither prose a reader
#             reads nor logic a reader follows, and counting them as code
#             would quietly buy a file comment budget
#   ratio     prose / code, shown to one decimal; the "over 1.2x" flag tests
#             the unrounded value, so a file shown at 1.2 can be flagged
#   #NNN      issue citations in DOC comments (/// and //!), which the
#             contract puts at zero; the same citation in an inline // is not
#             counted, nor is one below the test module
#
# Examples are out of the ratio because the contract requires a public API to
# carry them, so a file that keeps its doctests was being charged for obeying
# the rule the ratio exists to serve (#134 settles that; DESIGN.md's
# "Documentation contract" is where the decision is written). They are counted
# and shown rather than skipped, because a file whose examples have drifted
# from its code is a thing a reader of this report wants to see.
#
# A crate root is reported but never flagged: its code lines are a `pub mod`
# and `pub use` list, so the denominator measures nothing a ratio can judge.
# It stays in the total all the same -- the total is the tree, not a selection.
#
# "Above the test module" means up to the first `#[cfg(test)]` written at
# column 0, not the first one anywhere: a `#[cfg(test)]` indented inside an
# `impl` block gates one test-only helper and production code continues after
# it, so cutting there would charge a file for prose it does not have.
set -u

# Paths are resolved against the repo root, derived from this script's own
# location rather than from $PWD or `git rev-parse`, so the tree walk reports
# the same files from the root, from a subdirectory and from a worktree.
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd) || exit 0

# The file list goes to awk on stdin, one path per line, and awk opens each
# with getline -- rather than as awk arguments, which would split a path on
# whitespace.
if [ "$#" -gt 0 ]; then
    for arg in "$@"; do
        if [ -f "$arg" ]; then
            echo "$arg"
        elif [ -f "$root/$arg" ]; then
            echo "$root/$arg"
        else
            echo "doc-ratio: no such file: $arg" >&2
        fi
    done
else
    find "$root/src" -name '*.rs' -type f | LC_ALL=C sort
fi | awk -v root="$root" '
    { paths[++files] = $0 }

    END {
        fmt = "%8s %9s %8s %7s %6s  %s\n"
        printf fmt, "prose", "examples", "code", "ratio", "#NNN", "file"
        for (i = 1; i <= files; i++) {
            count(paths[i])
            name = shorten(paths[i])
            printf fmt, prose, examples, code, ratio(prose, code), cites,
                name (is_root(name) ? "  (crate root)" : mark(prose, code))
            tp += prose
            tx += examples
            tk += code
            ti += cites
        }
        # A total over one file is that row again, so a single-file report is
        # just the one row.
        if (files > 1) {
            printf fmt, tp, tx, tk, ratio(tp, tk), ti,
                "TOTAL (" files " files)" mark(tp, tk)
        }
    }

    # Sets the globals prose, examples, code and cites for one file.
    function count(path,    line, rest, body, in_example) {
        prose = 0
        examples = 0
        code = 0
        cites = 0
        in_example = 0
        while ((getline line < path) > 0) {
            if (line ~ /^#\[cfg\(test\)\]/) { break }
            sub(/^[ \t]+/, "", line)
            if (line == "") { continue }
            if (line ~ /^\/\/[\/!]/) {
                # The fence opens and closes inside the doc comment, so the
                # marker is read from the line with `///` or `//!` and one
                # space of indentation removed.
                body = substr(line, 4)
                sub(/^ /, "", body)
                if (body ~ /^```/) {
                    in_example = !in_example
                    examples++
                    continue
                }
                if (in_example) {
                    examples++
                    continue
                }
                prose++
                rest = line
                while (match(rest, /#[0-9]+/)) {
                    cites++
                    rest = substr(rest, RSTART + RLENGTH)
                }
            } else if (line ~ /^\/\//) {
                prose++
            } else if (line !~ /^#!?\[/) {
                code++
            }
        }
        close(path)
    }

    function ratio(c, k) {
        return k > 0 ? sprintf("%.1f", c / k) : "-"
    }

    # Over the 1.2x target, flagged so a long report can be skimmed.
    function mark(c, k) {
        return (k > 0 && c > 1.2 * k) ? "  <-- over 1.2x" : ""
    }

    # A crate root, whose code lines are a module list rather than logic.
    function is_root(name) {
        return name == "src/lib.rs" || name == "src/main.rs"
    }

    function shorten(path) {
        return index(path, root "/") == 1 \
            ? substr(path, length(root) + 2) : path
    }
'

# A report never fails its reader.
exit 0

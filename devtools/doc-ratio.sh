#!/bin/sh
# Reports how much of each production source file is comment and how much is
# code, against the documentation contract in DESIGN.md: a file's comment
# lines at or under 1.2x its code lines.
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
#   comments  lines whose first non-blank characters are ///, //! or //
#   code      every other non-blank line, except a line that is only an
#             attribute (#[...] / #![...]) -- those are neither prose a reader
#             reads nor logic a reader follows, and counting them as code
#             would quietly buy a file comment budget
#   ratio     comments / code, shown to one decimal; the "over 1.2x" flag
#             tests the unrounded value, so a file shown at 1.2 can be flagged
#   #NNN      issue citations in DOC comments (/// and //!), which the
#             contract puts at zero; the same citation in an inline // is not
#             counted, nor is one below the test module
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
        fmt = "%8s %8s %7s %6s  %s\n"
        printf fmt, "comments", "code", "ratio", "#NNN", "file"
        for (i = 1; i <= files; i++) {
            count(paths[i])
            printf fmt, comments, code, ratio(comments, code), cites,
                shorten(paths[i]) mark(comments, code)
            tc += comments
            tk += code
            ti += cites
        }
        # A total over one file is that row again, so a single-file report is
        # just the one row.
        if (files > 1) {
            printf fmt, tc, tk, ratio(tc, tk), ti,
                "TOTAL (" files " files)" mark(tc, tk)
        }
    }

    # Sets the globals comments, code and cites for one file.
    function count(path,    line, rest) {
        comments = 0
        code = 0
        cites = 0
        while ((getline line < path) > 0) {
            if (line ~ /^#\[cfg\(test\)\]/) { break }
            sub(/^[ \t]+/, "", line)
            if (line == "") { continue }
            if (line ~ /^\/\/[\/!]/) {
                comments++
                rest = line
                while (match(rest, /#[0-9]+/)) {
                    cites++
                    rest = substr(rest, RSTART + RLENGTH)
                }
            } else if (line ~ /^\/\//) {
                comments++
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

    function shorten(path) {
        return index(path, root "/") == 1 \
            ? substr(path, length(root) + 2) : path
    }
'

# A report never fails its reader.
exit 0

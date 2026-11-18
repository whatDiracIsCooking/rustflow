#!/bin/sh
# Reports how much of each production source file is prose, how much is doc
# example and how much is code, against the documentation contract in
# DESIGN.md: a file's prose lines at or under 1.2x its code lines.
#
#   devtools/doc-ratio.sh                   # every src/**/*.rs, plus a total
#   devtools/doc-ratio.sh src/executor/runtime.rs src/graph/node.rs
#
# This is a REPORT, not a gate: it exits 0 whatever it finds, so an acceptance
# check or a PR body can quote its numbers without the script deciding what
# passes. Nothing here is outside POSIX sh plus find/sort/awk, so it runs in
# the container, on a host and in a worktree with no setup at all.
#
# What is counted, per file, in the text ABOVE the test module:
#
#   prose     comment lines (///, //! or //) that are neither part of a doc
#             example nor inside a module's `# Design` section -- the text a
#             reader reads
#   examples  comment lines inside a ```-fenced block in a doc comment, plus
#             the two fences themselves
#   design    a module's `# Design` section: the `//! # Design` heading itself
#             and the lines under it, up to the next top-level `//! #` heading
#             or the end of the `//!` block, whichever comes first. A `//! ##`
#             sub-heading stays inside the section, and a fenced example
#             inside it counts as an example rather than as design; only the
#             heading spelled exactly `# Design` opens one. prose + examples +
#             design is every comment line, which is what this file reported
#             as prose alone before either column existed
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
# Examples and the `# Design` section are both out of the ratio for the same
# reason, the second following the first (#134, then #228; DESIGN.md's
# "Documentation contract" is where both decisions are written). The contract
# requires a public API to carry examples, and requires a module's rationale
# to live under `# Design` and nowhere else, so charging either to a file's
# budget charged it for obeying a rule the ratio exists to serve --
# `src/executor/scope.rs` at 3.2x and `src/algorithm/module.rs` at 3.0x were
# each over on little else. Both are counted and shown rather than skipped,
# because a file whose examples have drifted from its code, or whose `# Design`
# section has outgrown the module it explains, is a thing a reader of this
# report wants to see.
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
        fmt = "%8s %9s %7s %8s %7s %6s  %s\n"
        printf fmt, "prose", "examples", "design", "code", "ratio", "#NNN",
            "file"
        for (i = 1; i <= files; i++) {
            count(paths[i])
            name = shorten(paths[i])
            printf fmt, prose, examples, design, code, ratio(prose, code),
                cites,
                name (is_root(name) ? "  (crate root)" : mark(prose, code))
            tp += prose
            tx += examples
            td += design
            tk += code
            ti += cites
        }
        # A total over one file is that row again, so a single-file report is
        # just the one row.
        if (files > 1) {
            printf fmt, tp, tx, td, tk, ratio(tp, tk), ti,
                "TOTAL (" files " files)" mark(tp, tk)
        }
    }

    # Sets the globals prose, examples, design, code and cites for one file.
    function count(path,    line, rest, body, in_example, in_design) {
        prose = 0
        examples = 0
        design = 0
        code = 0
        cites = 0
        in_example = 0
        in_design = 0
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
                # A `///` item doc ends the module block the section lives in,
                # and so does any heading that is not `# Design` itself or a
                # sub-heading of it. The heading test runs outside a fence,
                # because a hidden doctest line also starts with `#`.
                if (line !~ /^\/\/!/) {
                    in_design = 0
                } else if (body ~ /^#+([ \t]|$)/) {
                    if (body ~ /^#[ \t]+Design[ \t]*$/) {
                        in_design = 1
                    } else if (body !~ /^##/) {
                        in_design = 0
                    }
                }
                if (in_design) { design++ } else { prose++ }
                rest = line
                while (match(rest, /#[0-9]+/)) {
                    cites++
                    rest = substr(rest, RSTART + RLENGTH)
                }
            } else if (line ~ /^\/\//) {
                in_design = 0
                prose++
            } else {
                in_design = 0
                if (line !~ /^#!?\[/) { code++ }
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

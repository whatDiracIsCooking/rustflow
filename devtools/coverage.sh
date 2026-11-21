#!/usr/bin/env bash
# Measures line/region coverage of the test suite with cargo-llvm-cov and
# prints the per-file table plus the TOTAL line.
#
#   devtools/coverage.sh              # the table, on stdout
#   devtools/coverage.sh --html       # the table, then an HTML report as well
#   devtools/coverage.sh --functions  # the table, then every function that the
#                                     # suite never executed, grouped by file
#
# `tests/`, `examples/` and `benches/` are excluded from the denominator: the
# question this answers is how much of `src/` the suite exercises, and an
# integration test's own lines are 100% covered by definition of having run.
# The regex matches those three as path components so it holds whether
# llvm-cov sees the path absolute or relative; `src/` has no directory by any
# of those names, so nothing under it is caught by accident.
#
# `--functions` is the listing every coverage disposition reads, so it is
# built to be the same text from run to run: sorted by file, then line, then
# column; paths relative to the repository root; no timestamp and no count
# that depends on the box. One line per *instantiation group* -- llvm-cov
# counts a generic function once however many monomorphisations it emitted,
# and calls the group executed when any one of them ran -- which is why the
# listing keys on `file:line:col` and says how many instantiations a group
# holds rather than listing a symbol per copy. The group total is checked
# against the "Missed Functions" column of the table printed above it, and the
# run fails if the two disagree: two numbers for one measurement is the thing
# this script exists to avoid.
#
# Output goes under CARGO_TARGET_DIR -- `/target` inside the container, set
# by the Dockerfile, so an empty ./target afterwards is normal: the
# instrumented build lands in $CARGO_TARGET_DIR/llvm-cov-target, the HTML
# report in $CARGO_TARGET_DIR/llvm-cov/html/index.html, and `--functions`
# leaves its listing and the JSON export it was derived from beside that.
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
functions=0
for arg in "$@"; do
    case "$arg" in
        --html) html=1 ;;
        --functions) functions=1 ;;
        -h|--help)
            sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed '$d' | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "coverage: unknown argument '$arg' (only --html and --functions are accepted)" >&2
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
out_dir="$target_dir/llvm-cov"
table="$out_dir/table.txt"
listing="$out_dir/functions-never-executed.txt"
json="$out_dir/functions.json"

echo "coverage: cargo llvm-cov $(cargo llvm-cov --version | awk '{print $2}'), output under $target_dir"

if [ "$functions" -eq 1 ]; then
    # Through `tee` only in this mode, so that the check at the end reads the
    # TOTAL row the reader has just seen instead of measuring a second time.
    mkdir -p "$out_dir"
    cargo llvm-cov --all-features --ignore-filename-regex "$exclude" | tee "$table"
else
    cargo llvm-cov --all-features --ignore-filename-regex "$exclude"
fi

if [ "$html" -eq 1 ]; then
    # `report` reuses the profiles the run above wrote rather than running the
    # suite a second time; it needs the same exclusion to describe the same
    # denominator as the table.
    cargo llvm-cov report --html --ignore-filename-regex "$exclude"
    echo "coverage: HTML report at $target_dir/llvm-cov/html/index.html"
fi

if [ "$functions" -eq 0 ]; then
    exit 0
fi

# Also a `report`, for the reason above: the same profiles, read a second way.
cargo llvm-cov report --json --ignore-filename-regex "$exclude" --output-path "$json"

work="$(mktemp -d)"
# Expanded now rather than at trap time, deliberately: the directory to remove
# is this run's, whatever $work is by then.
trap "rm -rf '$work'" EXIT

# One record per instantiation group that nothing executed, as
# `path<TAB>line<TAB>col<TAB>instantiations<TAB>symbol`, plus the group totals
# written to $work/totals for the check below.
awk -v root="$root/" -v totals="$work/totals" '
    # The exporter writes the whole report on one line, and a function record
    # contains no nested object, so splitting at every `{` puts each record
    # whole in $0 without needing a JSON parser.
    BEGIN { RS = "{"; OFS = "\t"; groups = 0 }

    # The `files` array, which precedes `functions`, is the set the
    # --ignore-filename-regex left in -- the same denominator the table counts
    # over. A record whose file is outside it is one the table did not count.
    {
        rest = $0
        while (match(rest, /"filename":"[^"]*"/)) {
            reported[substr(rest, RSTART + 12, RLENGTH - 13)] = 1
            rest = substr(rest, RSTART + RLENGTH)
        }
    }

    /"regions":\[\[/ {
        record = $0

        if (!match(record, /"count":[0-9]+/)) next
        count = substr(record, RSTART + 8, RLENGTH - 8) + 0

        if (!match(record, /"name":"[^"]*"/)) next
        symbol = substr(record, RSTART + 8, RLENGTH - 9)

        if (!match(record, /"filenames":\[[^]]*\]/)) next
        files = split(substr(record, RSTART + 13, RLENGTH - 14), file, ",")
        for (i = 1; i <= files; i++)
            file[i] = substr(file[i], 2, length(file[i]) - 2)

        at = index(record, "\"regions\":[[")
        tail = substr(record, at + 12)
        regions = split(substr(tail, 1, index(tail, "]]") - 1), region, "\\],\\[")

        # llvm-cov reports a record under the lowest-numbered of its files that
        # no expansion region expands into (its `findMainViewFileID`). Field 8
        # of a region is its kind, 1 being an expansion, and field 7 the file
        # that one expands into.
        delete expanded
        for (i = 1; i <= regions; i++) {
            split(region[i], field, ",")
            if (field[8] + 0 == 1) expanded[field[7] + 0] = 1
        }
        main = -1
        for (i = 0; i < files; i++)
            if (!(i in expanded)) { main = i; break }
        if (main < 0 || !(file[main + 1] in reported)) next

        # The group is keyed on where the function starts in that file, which
        # is what every monomorphisation of a generic shares.
        found = 0
        for (i = 1; i <= regions; i++) {
            split(region[i], field, ",")
            if (field[6] + 0 == main) {
                line = field[1] + 0
                col = field[2] + 0
                found = 1
                break
            }
        }
        if (!found) next

        key = file[main + 1] SUBSEP line SUBSEP col
        if (!(key in instantiations)) {
            keys[++groups] = key
            path_of[key] = file[main + 1]
            line_of[key] = line
            col_of[key] = col
        }
        instantiations[key]++
        executions[key] += count
        # A group prints one symbol, so it has to pick one of the
        # monomorphisations; the lowest in C order picks the same one each run.
        if (!(key in symbol_of) || symbol < symbol_of[key]) symbol_of[key] = symbol
    }

    END {
        missed = 0
        for (i = 1; i <= groups; i++) {
            key = keys[i]
            if (executions[key] > 0) continue
            missed++
            path = path_of[key]
            if (index(path, root) == 1) path = substr(path, length(root) + 1)
            print path, line_of[key], col_of[key], instantiations[key], symbol_of[key]
        }
        print groups, missed > totals
    }
' "$json" | LC_ALL=C sort -t "$(printf '\t')" -k1,1 -k2,2n -k3,3n > "$work/groups"

read -r total_functions missed_functions < "$work/totals"

# Checked before a line of it is printed: two numbers for one measurement is
# what this guards against. The groups come from the JSON export and the table
# from llvm-cov's own report, so they agree only while the grouping above is
# still the one llvm-cov uses.
table_functions="$(awk '$1 == "TOTAL" { print $5 }' "$table")"
table_missed="$(awk '$1 == "TOTAL" { print $6 }' "$table")"
if [ "$total_functions" != "$table_functions" ] || [ "$missed_functions" != "$table_missed" ]; then
    cat >&2 <<MSG
coverage: the grouping disagrees with the table printed above it.
  table:   $table_missed of $table_functions functions never executed
  listing: $missed_functions of $total_functions

Whatever changed, the listing would not be the table's "Missed Functions"
column broken down by file, so neither number could be quoted and none is
printed. The grouping it reimplements is llvm-cov's own (see the --functions
paragraph at the top of this script); a new cargo-llvm-cov is the likeliest
reason for it to drift.
MSG
    exit 1
fi

# The symbols are v0-mangled in the export. cargo-llvm-cov demangles its own
# HTML report by re-executing itself as llvm-cov's `-Xdemangler`, one symbol
# per line, which is the only demangler the image is guaranteed to have; it is
# not a documented interface, so a version that no longer answers that way
# degrades to mangled symbols rather than failing a measurement over cosmetics.
cut -f 5 "$work/groups" > "$work/mangled"
demangler="$(command -v cargo-llvm-cov || true)"
if [ -n "$demangler" ] &&
    __CARGO_LLVM_COV_DEMANGLER=1 "$demangler" < "$work/mangled" > "$work/demangled" 2>/dev/null &&
    [ "$(wc -l < "$work/demangled")" -eq "$(wc -l < "$work/mangled")" ]; then
    :
else
    echo "coverage: could not demangle; the listing names mangled symbols" >&2
    cp "$work/mangled" "$work/demangled"
fi

paste "$work/groups" "$work/demangled" > "$work/listing"

{
    echo "functions never executed: $missed_functions of $total_functions"
    echo "grouped by file; one line per instantiation group, as llvm-cov counts functions"
    awk -F '\t' '
        NR == FNR { per_file[$1]++; next }
        $1 != current {
            current = $1
            printf "\n%s: %d\n", current, per_file[current]
        }
        {
            line = sprintf("  %s:%s:%s  %s", $1, $2, $3, $6)
            if ($4 + 0 > 1) line = line sprintf("  (%d instantiations)", $4)
            print line
        }
    ' "$work/listing" "$work/listing"
} > "$listing"

cat "$listing"
echo
echo "coverage: listing at $listing, JSON export at $json"

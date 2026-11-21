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
# The listing comes in two sections, production code and `src/`'s own
# `#[cfg(test)]` code, because the exclusion above is a path regex and a path
# cannot see a module: most of what no test runs is unit-test code, and a
# reader looking for a production gap should not have to sieve it out. Both
# sections print their own count and their own denominator, and both are
# checked against the table the same way the total is -- the split is one
# measurement read two ways, not a second measurement, and the total above the
# sections is still the one the table reconciles against.
#
# What the split tests is `::tests::` in the demangled symbol's own path,
# taken up to the first `::<` so that a production generic instantiated with a
# unit test's closure is counted where it is defined rather than where its
# type argument comes from -- and taken over EVERY monomorphisation in the
# group rather than the one whose symbol names it, because a blanket impl
# (`impl Callables for (A, B)`) writes its Self type ahead of that cut, so one
# instantiation of a production group can read as a test symbol. A group is
# test code only when all of its instantiations say so. The one symbol a group
# prints is the lowest mangled name among them, which embeds the crate's
# disambiguator hash and so is not the same choice on two machines; the counts
# must not inherit that, and this is how they do not.
#
# So it matches a module named `tests`, which is a
# convention and not a guarantee: a `#[cfg(test)]` helper declared outside one
# counts as production, as does a `const _` compile-time assertion, which no
# run can execute at all. Symbols that could not be demangled cannot be
# partitioned either -- a mangled symbol does not spell `::tests::`, so the
# test would silently call every group production -- and such a run prints one
# unsplit section and says so. Only the function counts are split: the line
# and region percentages come from llvm-cov's own table and count `src/`'s
# test code in their denominators too.
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

# One record per instantiation group, as
# `path<TAB>line<TAB>col<TAB>instantiations<TAB>never_executed<TAB>symbol`,
# plus the group totals written to $work/totals for the check below. Every
# group and not only the never-executed ones, because each section of the
# listing states a denominator as well as a count. $work/records gets one line
# per monomorphisation instead of one per group, keyed the same way, which is
# what the split is decided over.
awk -v root="$root/" -v totals="$work/totals" -v records="$work/records" '
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

        path = file[main + 1]
        if (index(path, root) == 1) path = substr(path, length(root) + 1)
        key = path SUBSEP line SUBSEP col
        if (!(key in instantiations)) {
            keys[++groups] = key
            path_of[key] = path
            line_of[key] = line
            col_of[key] = col
        }
        instantiations[key]++
        print path, line, col, symbol > records
        executions[key] += count
        # A group prints one symbol, so it has to pick one of the
        # monomorphisations; the lowest in C order picks the same one each run.
        if (!(key in symbol_of) || symbol < symbol_of[key]) symbol_of[key] = symbol
    }

    END {
        missed = 0
        for (i = 1; i <= groups; i++) {
            key = keys[i]
            never = (executions[key] > 0) ? 0 : 1
            missed += never
            print path_of[key], line_of[key], col_of[key],
                instantiations[key], never, symbol_of[key]
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
# Every group's symbol goes through it, not just the never-executed ones,
# because each section of the listing states a denominator too -- and every
# monomorphisation's, because the split is decided over all of them.
cut -f 6 "$work/groups" > "$work/mangled"
cut -f 4 "$work/records" > "$work/records-mangled"
demangler="$(command -v cargo-llvm-cov || true)"
demangled=1
if [ -n "$demangler" ] &&
    __CARGO_LLVM_COV_DEMANGLER=1 "$demangler" < "$work/mangled" > "$work/demangled" 2>/dev/null &&
    [ "$(wc -l < "$work/demangled")" -eq "$(wc -l < "$work/mangled")" ] &&
    __CARGO_LLVM_COV_DEMANGLER=1 "$demangler" < "$work/records-mangled" > "$work/records-demangled" 2>/dev/null &&
    [ "$(wc -l < "$work/records-demangled")" -eq "$(wc -l < "$work/records-mangled")" ]; then
    :
else
    echo "coverage: could not demangle; the listing names mangled symbols" >&2
    cp "$work/mangled" "$work/demangled"
    # And so cannot be partitioned: see the --functions paragraph at the top.
    demangled=0
fi

paste "$work/groups" "$work/demangled" > "$work/listing"

# Which section each group belongs to, as `path<TAB>line<TAB>col<TAB>0|1`,
# decided over all of its monomorphisations: test code only when every one of
# them is in a `tests` module, so that the answer does not depend on which one
# llvm-cov named the group after. Empty when nothing could be demangled, which
# leaves every group production and the listing unsplit.
: > "$work/classes"
if [ "$demangled" -eq 1 ]; then
    paste "$work/records" "$work/records-demangled" | awk -F '\t' '
        # Tested on the path part of the symbol -- everything up to the first
        # `::<` -- rather than on the whole symbol, so that a production
        # generic instantiated with the closure of a unit test is counted
        # where it is defined and not where its type argument comes from.
        function in_test_module(symbol,    cut, path) {
            cut = index(symbol, "::<")
            path = (cut ? substr(symbol, 1, cut - 1) : symbol)
            return index(path, "::tests::") > 0
        }

        {
            key = $1 SUBSEP $2 SUBSEP $3
            if (!(key in all_test)) {
                keys[++groups] = key
                label[key] = $1 "\t" $2 "\t" $3
                all_test[key] = 1
            }
            if (!in_test_module($5)) all_test[key] = 0
        }

        END {
            for (i = 1; i <= groups; i++)
                print label[keys[i]] "\t" all_test[keys[i]]
        }
    ' > "$work/classes"

    classified="$(wc -l < "$work/classes")"
    if [ "$classified" -ne "$total_functions" ]; then
        cat >&2 <<MSG
coverage: the split classified $classified groups, not $total_functions.

The sections would then be over a different set of groups than the table
counts, so neither is printed. The classes are keyed on the same path, line
and column as the listing (see the --functions paragraph at the top of this
script), so a key that stopped matching is the likeliest reason.
MSG
        exit 1
    fi
fi

# Both, even when a section turns out to be empty, so that the breakdown below
# always has a file to read.
: > "$work/production"
: > "$work/test-code"

# The partition, one file per section, with each section's group total and
# never-executed count written to $work/counts for the check below.
awk -F '\t' -v counts="$work/counts" -v classes="$work/classes" \
    -v production="$work/production" -v test_code="$work/test-code" '
    # On FILENAME rather than `NR == FNR`, which is true of every line of the
    # second file when the first one is empty -- and the first one is empty
    # whenever nothing could be demangled.
    FILENAME == classes { section[$1 SUBSEP $2 SUBSEP $3] = $4 + 0; next }

    {
        # `+ 0` so that a group with no class -- every group, when nothing
        # could be demangled -- subscripts the counts below as 0 and not as
        # the empty string.
        is_test = section[$1 SUBSEP $2 SUBSEP $3] + 0
        total[is_test]++
        if ($5 + 0 != 1) next
        missed[is_test]++
        print > (is_test ? test_code : production)
    }

    END {
        print total[0] + 0, missed[0] + 0, total[1] + 0, missed[1] + 0 > counts
    }
' "$work/classes" "$work/listing"

read -r production_total production_missed test_total test_missed < "$work/counts"

# The same guard as above, applied to the split: a partition that drops or
# duplicates a group would make the two sections say something the table does
# not, which is the one thing this listing may not do.
if [ "$((production_total + test_total))" != "$total_functions" ] ||
    [ "$((production_missed + test_missed))" != "$missed_functions" ]; then
    cat >&2 <<MSG
coverage: the production/test split does not add up to the measurement.
  listing:  $missed_functions of $total_functions functions never executed
  sections: $production_missed of $production_total production,
            $test_missed of $test_total in src/'s own test code

Two sections that do not sum to the total are two numbers for one measurement,
so neither is printed. The split is a test on the demangled symbol (see the
--functions paragraph at the top of this script).
MSG
    exit 1
fi

# One section's per-file breakdown: the listing as it read before the split,
# over the subset handed in.
breakdown() {
    awk -F '\t' '
        NR == FNR { per_file[$1]++; next }
        $1 != current {
            current = $1
            printf "\n%s: %d\n", current, per_file[current]
        }
        {
            line = sprintf("  %s:%s:%s  %s", $1, $2, $3, $7)
            if ($4 + 0 > 1) line = line sprintf("  (%d instantiations)", $4)
            print line
        }
    ' "$1" "$1"
}

{
    # First line, and the split inside it: the coverage job lifts this line
    # into its summary by its `functions never executed: ` prefix, and several
    # merged pull requests quote the total, so both stay where they were.
    if [ "$demangled" -eq 1 ]; then
        echo "functions never executed: $missed_functions of $total_functions" \
            "(production $production_missed of $production_total;" \
            "src/'s own #[cfg(test)] code $test_missed of $test_total)"
        echo "the split is of these function counts only; the line and region" \
            "percentages in the table above are llvm-cov's own, and count" \
            "src/'s test code in their denominators as well"
    else
        echo "functions never executed: $missed_functions of $total_functions" \
            "(not split: the symbols could not be demangled)"
        echo "the line and region percentages in the table above are" \
            "llvm-cov's own, and count src/'s test code in their" \
            "denominators as well"
    fi
    echo "grouped by file; one line per instantiation group, as llvm-cov counts functions"

    echo
    if [ "$demangled" -eq 1 ]; then
        echo "== production code: $production_missed of" \
            "$production_total groups never executed =="
    else
        echo "== production and test code together: $production_missed of" \
            "$production_total groups never executed =="
    fi
    breakdown "$work/production"

    if [ "$demangled" -eq 1 ]; then
        echo
        echo "== src/'s own #[cfg(test)] code: $test_missed of" \
            "$test_total groups never executed =="
        breakdown "$work/test-code"
    fi
} > "$listing"

cat "$listing"
echo
echo "coverage: listing at $listing, JSON export at $json"

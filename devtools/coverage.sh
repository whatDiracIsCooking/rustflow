#!/usr/bin/env bash
# Measures line/region coverage of the test suite with cargo-llvm-cov and
# prints the per-file table plus the TOTAL line.
#
#   devtools/coverage.sh              # the table, on stdout
#   devtools/coverage.sh --html       # the table, then an HTML report as well
#   devtools/coverage.sh --functions  # the table, then every function that the
#                                     # suite never executed, grouped by file
#   devtools/coverage.sh --check-floors   # the table, then the per-file line
#                                         # floors; non-zero if one is unmet
#   devtools/coverage.sh --write-floors   # the table, then rewrite the floors
#                                         # file from the run it just did
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
# The two floor modes are the gate half of this script, and the only part of
# it that fails over a number rather than over a measurement.
# `devtools/coverage-floors.txt` holds one `path  percent` line per file,
# sorted, keyed on the repository-relative path. `--check-floors` names every
# file whose LINE coverage is below its floor, with the floor and the measured
# value, and exits non-zero; `--write-floors` rewrites that file from the run
# it has just done. A floor is the measured line coverage less a margin -- the
# larger of two percentage points and four missed lines -- rounded down to a
# whole point. `docs/development.md` records why that margin and what was
# measured to choose it; a floor hand-edited above what the tree measures is
# how the check is demonstrated, so nothing here forbids one.
#
# A file with no line in the floors file has no floor, which is how a new
# module arrives: ungated until someone writes its line, and counted by the
# check so that it is not ungated invisibly. A floor whose file the
# measurement does not report fails the check as well -- a renamed or deleted
# file would otherwise lose its floor in silence -- and says to rewrite the
# file rather than reporting a regression.
#
# Line coverage only, and llvm-cov's line denominators count `src/`'s own
# `#[cfg(test)]` code, because the exclusion above is a path regex and a path
# cannot see a module. A file's floor is therefore partly a floor on its own
# test code, which `docs/development.md` says what to make of.
#
# Both modes work from the per-file rows of the table printed above them,
# reconciled against a second read of the same profiles: the row count, the
# line total and the covered total have to equal the TOTAL row's, and every
# row has to equal its own file's summary in the export, or no floor is
# written and none is judged. One measurement must not be able to produce two
# numbers, here as in the listing above. The table prints each path with the
# prefix every reported file shares removed, so the floors key on the export's
# path instead, relative to the repository root: that is the one spelling that
# is the same in this container and on a CI runner, and the prefix relating
# the two is derived from the rows rather than assumed.
#
# Output goes under CARGO_TARGET_DIR -- `/target` inside the container, set
# by the Dockerfile, so an empty ./target afterwards is normal: the
# instrumented build lands in $CARGO_TARGET_DIR/llvm-cov-target, the HTML
# report in $CARGO_TARGET_DIR/llvm-cov/html/index.html, and `--functions`
# leaves its listing and the JSON export it was derived from beside that.
#
# Without `--check-floors` this is a REPORT, not a gate: the plain run,
# `--html` and `--functions` exit non-zero only when the measurement itself
# cannot run -- most likely because the image predates the
# `CARGO_LLVM_COV_VERSION` pin in the Dockerfile, which the message below
# says how to fix. Do NOT `cargo install` the tool by hand to get past it:
# the pin exists so that two coverage numbers are from the same tool.
# `--check-floors` adds the one figure that fails: a file under its floor.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

html=0
functions=0
write_floors=0
check_floors=0
for arg in "$@"; do
    case "$arg" in
        --html) html=1 ;;
        --functions) functions=1 ;;
        --write-floors) write_floors=1 ;;
        --check-floors) check_floors=1 ;;
        -h|--help)
            sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed '$d' | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "coverage: unknown argument '$arg' (only --html, --functions," \
                "--write-floors and --check-floors are accepted)" >&2
            exit 2
            ;;
    esac
done

# Writing the floors and then checking them would check the numbers against
# themselves, so the two are an either/or rather than an oversight.
if [ "$write_floors" -eq 1 ] && [ "$check_floors" -eq 1 ]; then
    echo "coverage: --write-floors and --check-floors are exclusive; a run" \
        "that writes a floor cannot also be the run that judges it" >&2
    exit 2
fi

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
summary="$out_dir/summary.json"
per_file="$out_dir/per-file-lines.txt"
per_file_totals="$out_dir/per-file-totals.txt"
floors="$root/devtools/coverage-floors.txt"
# What the floors report calls it: the absolute path is this box's, and this
# output is read on another one.
floors_name="devtools/coverage-floors.txt"

echo "coverage: cargo llvm-cov $(cargo llvm-cov --version | awk '{print $2}'), output under $target_dir"

if [ "$functions" -eq 1 ] || [ "$write_floors" -eq 1 ] || [ "$check_floors" -eq 1 ]; then
    # Through `tee` in the modes that go on to read the table back, so that
    # they read the TOTAL row and the rows the reader has just seen instead of
    # measuring a second time.
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

# The floors, as a function because it runs last in either mode: after the
# table on a plain `--check-floors`, and after the never-executed listing when
# `--functions` was asked for too, so that the floors report is always the
# tail of the output. A run that asked for neither floor mode returns at once.
floors() {
    if [ "$write_floors" -eq 0 ] && [ "$check_floors" -eq 0 ]; then
        return 0
    fi
    # Inside the guard, so that a run which asked for no floor prints exactly
    # what it printed before this mode existed.
    echo

    # A third `report` over the profiles the run above wrote -- the same
    # measurement, read a third way -- and `--summary-only` because the
    # per-file totals are all this needs: it keeps the export to tens of
    # kilobytes and leaves nothing to confuse a file's own summary with.
    cargo llvm-cov report --json --summary-only \
        --ignore-filename-regex "$exclude" --output-path "$summary"

    # `path<TAB>lines<TAB>covered` per file, with the counts the reconciliation
    # below judges, from the table and the export together. Every refusal in
    # here is a correspondence that could not be established at all, as
    # against the arithmetic the shell checks afterwards. Removed first so that
    # a previous run's figures cannot be left where this run's are read from.
    rm -f "$per_file" "$per_file_totals"
    if ! awk -v table="$table" -v root="$root/" \
        -v records="$per_file" -v totals="$per_file_totals" '
        function fail(message) {
            printf "coverage: %s\n", message > "/dev/stderr"
            failed = 1
            exit 1
        }

        function number(object, key,   pattern, text) {
            pattern = "\"" key "\":[0-9]+"
            if (!match(object, pattern))
                fail("the summary export gives no " key " for " path)
            text = substr(object, RSTART, RLENGTH)
            return substr(text, length(key) + 4) + 0
        }

        BEGIN { OFS = "\t" }

        # The table, read first and only once: its header names the columns,
        # so the two columns this needs are found by name. A cargo-llvm-cov
        # that adds or reorders one therefore moves nothing silently.
        FILENAME == table {
            if (done) next
            if ($1 == "Filename") {
                for (i = 1; i <= NF; i++) {
                    name = $i
                    # "Missed Regions" and "Missed Lines" are two fields each.
                    if (name == "Missed") { i++; name = name " " $i }
                    columns++
                    if (name == "Lines") lines_at = columns
                    if (name == "Missed Lines") missed_at = columns
                }
                if (!lines_at || !missed_at)
                    fail("the table has no Lines and Missed Lines columns")
                in_table = 1
                next
            }
            if (!in_table) next
            if ($1 ~ /^-+$/) next
            if (NF != columns)
                fail("a row of the table has " NF " fields, not the " columns \
                    " its header names: " $0)
            if ($1 == "TOTAL") {
                total_lines = $lines_at + 0
                total_missed = $missed_at + 0
                in_table = 0
                done = 1
                next
            }
            rows++
            row_name[rows] = $1
            row_lines[rows] = $lines_at + 0
            row_missed[rows] = $missed_at + 0
            next
        }

        # The export, accumulated whole rather than read line by line: it is
        # one line today, and this way a pretty-printed one reads the same.
        { json = json $0 }

        END {
            if (failed) exit 1
            if (!done) fail("the table printed above has no TOTAL row")
            if (rows < 1) fail("the table printed above has no per-file row")

            rest = json
            while (match(rest, /"filename":"[^"]*"/)) {
                path = substr(rest, RSTART + 12, RLENGTH - 13)
                rest = substr(rest, RSTART + RLENGTH)
                # The summary of the file just named: its own `lines` object
                # comes before anything else that has one.
                if (!match(rest, /"lines":\{[^}]*\}/))
                    fail("the summary export gives no lines summary for " path)
                object = substr(rest, RSTART, RLENGTH)
                if (index(path, root) != 1)
                    fail("the summary export reports " path ", which is not " \
                        "under " root " -- a floor keyed on that would be a " \
                        "path of this box alone")
                path = substr(path, length(root) + 1)
                # The floors file is read back as whitespace-separated pairs,
                # so a path it cannot key on has to stop the run rather than
                # be written into it.
                if (path ~ /[ \t]/)
                    fail("the summary export reports " path ", whose path " \
                        "contains whitespace")
                if (path in json_lines)
                    fail("the summary export reports " path " twice")
                files++
                paths[files] = path
                json_lines[path] = number(object, "count")
                json_covered[path] = number(object, "covered")
            }
            if (failed) exit 1

            # The table prints every path with the prefix all of them share
            # removed, so a row has to be matched back to the export path the
            # floors are keyed on. The prefix is DERIVED -- the one candidate
            # every row agrees on -- rather than assumed to be `src/`, which a
            # reported file outside src/ would make wrong for every row at
            # once.
            for (i = 1; i <= rows; i++) {
                for (j = 1; j <= files; j++) {
                    path = paths[j]
                    if (path == row_name[i]) candidate = ""
                    else if (length(path) > length(row_name[i]) &&
                        substr(path, length(path) - length(row_name[i]) + 1) \
                            == row_name[i])
                        candidate = substr(path, 1,
                            length(path) - length(row_name[i]))
                    else continue
                    support[candidate]++
                }
            }
            for (candidate in support)
                if (support[candidate] == rows) {
                    prefixes++
                    prefix = candidate
                }
            if (prefixes != 1)
                fail("no single prefix relates the " rows " rows of the " \
                    "table to the " files " files of the summary export (" \
                    prefixes + 0 " candidates)")

            for (i = 1; i <= rows; i++) {
                path = prefix row_name[i]
                if (!(path in json_lines))
                    fail("the table row " row_name[i] " is no file of the " \
                        "summary export")
                if (used[path]++)
                    fail("two rows of the table name " path)
                if (json_lines[path] != row_lines[i] ||
                    json_lines[path] - json_covered[path] != row_missed[i])
                    fail("the table and the summary export disagree about " \
                        path ": " row_lines[i] " lines and " row_missed[i] \
                        " missed against " json_lines[path] " and " \
                        json_lines[path] - json_covered[path])
            }
            if (failed) exit 1

            for (i = 1; i <= files; i++) {
                path = paths[i]
                if (!(path in used))
                    fail("the summary export reports " path \
                        ", which the table does not")
                sum_lines += json_lines[path]
                sum_covered += json_covered[path]
                print path, json_lines[path], json_covered[path] > records
            }
            if (failed) exit 1

            print files, rows, sum_lines, sum_covered, total_lines, total_missed \
                > totals
        }
    ' "$table" "$summary"; then
        cat >&2 <<MSG

coverage: the per-file figures could not be tied to the table printed above.

No floor is written and none is judged: a floor is only worth something while
it is the table's own number for that file. The two reads are llvm-cov's
report table and its --summary-only export of the same profiles (see the
floors paragraphs at the top of this script).
MSG
        return 1
    fi

    read -r file_count row_count sum_lines sum_covered table_lines table_missed \
        < "$per_file_totals"

    # The same guard the listing applies to its function counts, applied to
    # these line counts: the files have to be the table's rows, and their
    # lines and misses have to add up to the TOTAL row. Checked before a floor
    # is written or judged, because a floor is a number read off this table.
    if [ "$file_count" != "$row_count" ] ||
        [ "$sum_lines" != "$table_lines" ] ||
        [ "$((sum_lines - sum_covered))" != "$table_missed" ]; then
        cat >&2 <<MSG

coverage: the per-file line figures disagree with the table printed above.
  table:    $table_missed of $table_lines lines missed, over $row_count files
  per file: $((sum_lines - sum_covered)) of $sum_lines, over $file_count

A floor measured against a total nobody printed is two numbers for one
measurement, so no floor is written and none is judged.
MSG
        return 1
    fi

    if [ "$write_floors" -eq 1 ]; then
        {
            cat <<MSG
# Per-file line-coverage floors: one \`path  percent\` line per file, sorted,
# keyed on the repository-relative path. \`devtools/coverage.sh --check-floors\`
# fails and names every file whose line coverage is below its floor; a file
# with no line here has no floor, so a new module is ungated until someone
# adds one.
#
# Written by \`devtools/coverage.sh --write-floors\`, which is also how to
# raise them after a tree has gained coverage. Each floor is that run's
# measured line coverage less the larger of two percentage points and four
# missed lines, rounded down to a whole point. \`docs/development.md\` records
# why that margin, what was measured to choose it, and what a line floor does
# and does not say about a file whose own \`#[cfg(test)]\` code is in the
# denominator.
MSG
            awk '
                {
                    count = $2
                    covered = $3
                    # In hundredths of a line, so that the floor comes out of
                    # integer arithmetic: four lines, or two percent of the
                    # file when that is the larger.
                    margin = 400
                    if (2 * count > margin) margin = 2 * count
                    remaining = 100 * covered - margin
                    printf "%-40s %3d\n", $1, \
                        (remaining > 0) ? int(remaining / count) : 0
                }
            ' "$per_file" | LC_ALL=C sort
        } > "$floors"

        awk -v name="$floors_name" '
            /^#/ { next }
            { count++; if (!($2 + 0)) zeros = zeros " " $1 }
            END {
                printf "floors: wrote %d floors to %s, each the line coverage" \
                    " this run measured less the larger of two points and" \
                    " four lines, rounded down\n", count, name
                if (zeros != "")
                    printf "floors: the margin leaves nothing above 0 for%s," \
                        " whose floor therefore gates nothing\n", zeros
            }
        ' "$floors"
        return 0
    fi

    if [ ! -f "$floors" ]; then
        cat >&2 <<MSG

coverage: there is no $floors_name to check against.

Write it from a measured tree with \`devtools/coverage.sh --write-floors\`.
MSG
        return 1
    fi

    # The judgement, in the floors file's own order so that two runs read the
    # same, and in integer arithmetic so that "below its floor" is not a
    # question about a rounded percentage: a file is under only when
    # `covered * 100 < floor * lines`.
    awk -v floors="$floors" -v name="$floors_name" '
        function fail(message) {
            printf "coverage: %s\n", message > "/dev/stderr"
            failed = 1
            exit 1
        }

        FILENAME == floors {
            if ($0 ~ /^[ \t]*#/ || NF == 0) next
            if (NF != 2 || $2 !~ /^[0-9]+$/)
                fail(name " line " FNR " is not a `path percent` pair: " $0)
            if ($2 + 0 > 100)
                fail(name " line " FNR " asks for more than 100%: " $0)
            if ($1 in floor)
                fail(name " names " $1 " twice")
            floor[$1] = $2 + 0
            listed[++listed_n] = $1
            next
        }

        { lines[$1] = $2 + 0; covered[$1] = $3 + 0; measured[++measured_n] = $1 }

        END {
            if (failed) exit 1

            thinnest = 101
            for (i = 1; i <= listed_n; i++) {
                path = listed[i]
                if (!(path in lines)) { stale[++stale_n] = path; continue }
                gated++
                if (100 * covered[path] < floor[path] * lines[path]) {
                    under[++under_n] = path
                    continue
                }
                head = 100.0 * covered[path] / lines[path] - floor[path]
                if (head < thinnest) { thinnest = head; thinnest_at = path }
            }
            for (i = 1; i <= measured_n; i++)
                if (!(measured[i] in floor)) ungated = ungated " " measured[i]

            if (under_n) {
                printf "floors: %d of the %d floors in %s %s not met.\n", \
                    under_n, gated, name, (under_n == 1 ? "is" : "are")
                for (i = 1; i <= under_n; i++) {
                    path = under[i]
                    printf "  %-40s floor %3d  measured %6.2f%%" \
                        "  (%d of %d lines)\n", path, floor[path], \
                        100.0 * covered[path] / lines[path], covered[path], \
                        lines[path]
                }
            }
            if (stale_n) {
                printf "floors: %s names %d file%s the measurement does not.\n", \
                    name, stale_n, (stale_n == 1 ? "" : "s")
                for (i = 1; i <= stale_n; i++)
                    printf "  %-40s floor %3d  not reported\n", \
                        stale[i], floor[stale[i]]
                printf "floors: a renamed or deleted file keeps its floor" \
                    " until someone rewrites the file, so this is a stale" \
                    " floors file rather than a regression: re-run" \
                    " devtools/coverage.sh --write-floors.\n"
            }
            if (!under_n && !stale_n && !gated)
                printf "floors: %s lists no floor for any file the" \
                    " measurement reports, so nothing is gated.\n", name
            else if (!under_n && !stale_n)
                printf "floors: every one of the %d floors in %s is met;" \
                    " thinnest headroom %.2f points, %s (floor %d).\n", \
                    gated, name, thinnest, thinnest_at, floor[thinnest_at]
            if (ungated != "")
                printf "floors: %d measured file%s no floor and %s not gated:%s\n", \
                    measured_n - gated, \
                    (measured_n - gated == 1 ? " has" : "s have"), \
                    (measured_n - gated == 1 ? "is" : "are"), ungated

            exit (under_n + stale_n > 0) ? 1 : 0
        }
    ' "$floors" "$per_file"
}

if [ "$functions" -eq 0 ]; then
    floors
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

# Last, so that the floors report is the tail of the output whichever other
# mode was asked for, and so that a floor this run does not meet fails only
# after everything a reader needs has been printed. The `coverage` job lifts
# the report by that: from its first `floors: ` line to the end of the log.
floors

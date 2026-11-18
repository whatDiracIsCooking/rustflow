#!/bin/sh
# Compares two refs on one benchmark in interleaved rounds -- A, B, A, B, ...
# -- and prints each side's spread against itself before it prints the A->B
# ratio, so that a difference inside either side's own noise is visible as one.
#
#   devtools/bench-ab.sh <ref-A> <ref-B> --bench <name> [--rounds N] \
#                        [-- <criterion args>]
#
#   devtools/bench-ab.sh origin/main WORKTREE --bench linear_chain --rounds 2 \
#       -- 'rustflow/1w/1024'
#
# Either ref may be the literal WORKTREE, which means the tree this script
# lives in exactly as it stands -- the usual case for an unpushed patch. Any
# other value is whatever `git worktree add` accepts: a branch, a tag,
# `origin/main`, a bare commit. Giving WORKTREE as *both* refs measures the
# box against itself, an A/A control: whatever difference that reports is one
# this box cannot tell from noise today. Everything after `--` is handed to the
# compiled bench binary, which takes criterion's own command line; a filter
# (a regex over benchmark ids) is the argument worth giving, since a round
# that measures four ids takes four times as long and a comparison wants the
# ids it is about.
#
# WHY THIS EXISTS. On this box a criterion run measures whatever else the box
# is doing, so two runs minutes apart measure the other agents' builds rather
# than the patch: BENCHMARKS.md's `Work` decision, #40, #102 and #119 each
# record an answer that was wrong rather than noisy when read that way, and
# each worked around it by alternating two builds by hand. This is that method
# as one script.
#
# WHAT IT DOES
#   1. Checks each ref out into its own detached worktree under
#      `.claude/worktrees/bench-ab-<side>` of the primary checkout. A side
#      given as WORKTREE is measured where it already is and is never touched.
#   2. Builds each side once, `cargo bench --no-run`, into its own
#      CARGO_TARGET_DIR (`<base>/bench-ab-<side>`), so neither side's build
#      lands in the other side's timing and the two builds do not serialise
#      behind cargo's exclusive lock on one target directory. `<base>` is
#      $CARGO_TARGET_DIR when set -- `/target` in this repo's container --
#      and `<primary checkout>/target` otherwise.
#   3. Runs the two compiled binaries directly, A then B, for --rounds rounds
#      (default 3). Each run saves its own criterion baseline,
#      `<side>-r<round>`, so no round's data overwrites another's. Where a
#      binary is comes from cargo's own artifact message rather than from a
#      guess at the target directory's layout, which is not stable across
#      cargo versions: this one puts a bench binary under
#      `release/build/<pkg>/<hash>/out/` and has no `release/deps` at all.
#   4. Prints the report below, then removes both worktrees and both target
#      directories -- on failure too, from an EXIT trap. Every run therefore
#      builds both sides from scratch, which is the point: a target directory
#      never survives to taint a later comparison.
#
# WHAT IT REPORTS, in this order, per benchmark id
#   * each side's spread against itself, max/min over that side's own rounds;
#   * each side's median over its rounds, and the B/A ratio of the two;
#   * every round's point estimate, in the order the rounds ran.
#   The spread comes first because it is the rule every A/B table in
#   BENCHMARKS.md is held to: a difference inside either side's own spread is
#   not a result, and the verdict line says which of the two cases the ratio
#   is in. Each number is criterion's own typical estimate -- the middle
#   figure of its `time: [lo est hi]` line, which is its regression slope
#   where it has one and the mean otherwise -- normalised to nanoseconds.
#
#   The verdict line divides the change by the wider of the two within-side
#   spreads: at or under 1.00x the difference is inside one side's own noise
#   and is not a result, up to 1.25x it says `marginal` instead of deciding,
#   and past that it says the change is a result. That 1.25 is the only
#   judgement in this script, and every number it is computed from is printed
#   on the lines above it, so a reader who would draw the line elsewhere can.
#
# WHAT IT CANNOT DO
#   * It cannot make a loaded box quiet. Interleaving cancels drift over the
#     run, not variance: both sides see the same average disturbance, so the
#     absolutes stay as noisy as the box is and only the comparison improves.
#     The load average is recorded at the start, after each round and at the
#     end so that a reader can see which kind of box it was.
#   * It cannot cancel a bias that depends on position within a round. Each
#     round runs A and then B, always that way round, which is how
#     BENCHMARKS.md's two-build A/Bs were run by hand. Every round's number is
#     printed so that a reader can look for such a bias instead of being told
#     there is none.
#   * It says nothing about spread at --rounds 1: one round per side is one
#     number per side, so there is no within-side spread and no verdict.
#   * It matches ids by name. An id only one side has is reported as unmatched
#     and gets no ratio -- if two refs disagree about a benchmark's ids, that
#     is the finding rather than something to average over.
#   * It measures what criterion measures, wall time of the sampled region.
#     Allocations, instruction counts and cache misses are not in it.
#   * It is not a gate. A comparison that completed exits 0 whatever the
#     numbers say; a non-zero exit means the measurement could not be made.
#
# WHEN A SIDE FAILS TO BUILD it stops, and loudly: the whole cargo output goes
# to stderr under a BUILD FAILED banner, both worktrees and both target
# directories are torn down anyway, the log directory is kept and named, and
# the exit status is non-zero. There is no partial report and no falling back
# to a binary from an earlier run -- a one-sided A/B is not a measurement. A
# bench binary that exits non-zero mid-round is treated the same way.
#
# Progress goes to stderr and the report to stdout, so
# `devtools/bench-ab.sh ... >report.txt` keeps the report and still shows the
# run as it happens.
#
# POSIX sh, dash included -- /bin/sh in this repo's container is dash, whose
# `set` has no `-o pipefail`. So this is `set -eu` and no pipeline's exit
# status is load-bearing: every cargo and bench invocation runs on its own and
# is checked. Nothing here is outside sh plus git, cargo, awk and sort.
set -eu

me=bench-ab

note() {
    printf '%s: %s\n' "$me" "$*" >&2
}

banner() {
    printf '\n%s\n%s: %s\n%s\n\n' \
        '===============================================================' \
        "$me" "$*" \
        '===============================================================' >&2
}

die() {
    printf '%s: %s\n' "$me" "$*" >&2
    exit 1
}

# Usage errors exit 2, so a caller can tell "you held it wrong" from "the
# measurement failed".
usage_error() {
    printf '%s: %s\n' "$me" "$*" >&2
    printf 'usage: %s <ref-A> <ref-B> --bench <name> [--rounds N]' "$0" >&2
    printf ' [-- <criterion args>]\n' >&2
    printf '       %s --help\n' "$0" >&2
    exit 2
}

# The header above, with the comment markers taken off: the documentation and
# the help text cannot drift apart if there is only one of them.
help() {
    sed -n '2,/^set -eu$/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'
}

ref_a=''
ref_b=''
bench=''
rounds=3
refs_given=0

while [ "$#" -gt 0 ]; do
    case $1 in
        -h|--help)
            help
            exit 0
            ;;
        --bench)
            [ "$#" -ge 2 ] || usage_error "--bench needs a benchmark name"
            bench=$2
            shift 2
            ;;
        --bench=*)
            bench=${1#--bench=}
            shift
            ;;
        --rounds)
            [ "$#" -ge 2 ] || usage_error "--rounds needs a count"
            rounds=$2
            shift 2
            ;;
        --rounds=*)
            rounds=${1#--rounds=}
            shift
            ;;
        --)
            shift
            break
            ;;
        -*)
            usage_error "unknown option '$1'"
            ;;
        *)
            case $refs_given in
                0)
                    ref_a=$1
                    refs_given=1
                    ;;
                1)
                    ref_b=$1
                    refs_given=2
                    ;;
                *)
                    usage_error "unexpected argument '$1';" \
                        "criterion's own arguments go after --"
                    ;;
            esac
            shift
            ;;
    esac
done
# Whatever is left in "$@" came after `--` and belongs to the bench binary.

[ "$refs_given" -eq 2 ] \
    || usage_error "two refs are required (either may be WORKTREE)"
[ -n "$bench" ] || usage_error "--bench <name> is required"
case $rounds in
    ''|*[!0-9]*) usage_error "--rounds wants a count, got '$rounds'" ;;
esac
[ "$rounds" -ge 1 ] || usage_error "--rounds wants at least 1, got '$rounds'"

command -v git >/dev/null 2>&1 || die "git is not on PATH"
command -v cargo >/dev/null 2>&1 || die "cargo is not on PATH"

# The tree comes from this script's own location rather than from $PWD, so a
# run started from a subdirectory compares the same two things.
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd) \
    || die "cannot resolve the repo root"
git -C "$root" rev-parse --git-dir >/dev/null 2>&1 \
    || die "$root is not a git checkout"

# Worktrees go under the primary checkout, which is the parent of the shared
# git common dir -- the same place this repo's other worktrees live, and a
# path that does not nest a worktree inside a worktree when the script is run
# from one. The common dir is `.git` for the primary checkout itself, so it is
# resolved against $root when it comes back relative.
common_dir=$(git -C "$root" rev-parse --git-common-dir)
case $common_dir in
    /*) ;;
    *) common_dir=$root/$common_dir ;;
esac
primary=$(CDPATH= cd -- "$common_dir/.." && pwd) \
    || die "cannot resolve the primary checkout from $common_dir"
worktrees=$primary/.claude/worktrees

target_base=${CARGO_TARGET_DIR:-$primary/target}
tdir_a=$target_base/bench-ab-a
tdir_b=$target_base/bench-ab-b

dir_a=$worktrees/bench-ab-a
dir_b=$worktrees/bench-ab-b
if [ "$ref_a" = WORKTREE ]; then dir_a=$root; fi
if [ "$ref_b" = WORKTREE ]; then dir_b=$root; fi

work=$(mktemp -d "${TMPDIR:-/tmp}/bench-ab.XXXXXX") \
    || die "cannot create a working directory"
: >"$work/created-worktrees.txt"
: >"$work/created-targets.txt"
: >"$work/samples.tsv"
: >"$work/load.txt"

cleaned=0

# Removes only what this run created, and only paths whose names say so: the
# `bench-ab-*` test below is why a WORKTREE side -- whose directory is the
# caller's real checkout and is never recorded here -- cannot be deleted by a
# bug in the bookkeeping above.
cleanup() {
    status=$?
    if [ "$cleaned" -eq 0 ]; then
        cleaned=1
        while IFS= read -r path; do
            case $path in
                */bench-ab-*) ;;
                *) continue ;;
            esac
            if [ -d "$path" ]; then
                git -C "$primary" worktree remove --force "$path" \
                    >/dev/null 2>&1 || rm -rf -- "$path" || :
            fi
        done <"$work/created-worktrees.txt"
        git -C "$primary" worktree prune >/dev/null 2>&1 || :
        while IFS= read -r path; do
            case $path in
                */bench-ab-*) rm -rf -- "$path" || : ;;
                *) continue ;;
            esac
        done <"$work/created-targets.txt"
        if [ "$status" -eq 0 ]; then
            rm -rf -- "$work"
        else
            note "logs kept in $work"
        fi
    fi
    exit "$status"
}

# The signal traps exit rather than clean up themselves, so that there is one
# teardown path and it is the EXIT one.
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

record_load() {
    if [ -r /proc/loadavg ]; then
        load=$(awk '{ printf "%s %s %s", $1, $2, $3 }' /proc/loadavg)
    else
        load='(no /proc/loadavg on this host)'
    fi
    printf '  %-14s %-22s %s\n' "$1" "$load" "$(date -u '+%H:%M:%SZ')" \
        >>"$work/load.txt"
    note "load average $load ($1)"
}

make_worktree() {
    side=$1
    ref=$2
    dir=$3
    if [ "$ref" = WORKTREE ]; then
        return 0
    fi
    if [ -e "$dir" ]; then
        die "$dir already exists -- another bench-ab run may be in flight;" \
            "remove it (git worktree remove) and start again"
    fi
    git -C "$root" rev-parse --verify --quiet "$ref^{commit}" >/dev/null 2>&1 \
        || die "'$ref' is not a commit this repo knows (fetch it first?)"
    note "side $side: checking $ref out into $dir"
    git -C "$root" worktree add --detach "$dir" "$ref" >&2 \
        || die "git worktree add failed for '$ref'"
    printf '%s\n' "$dir" >>"$work/created-worktrees.txt"
}

# One line naming what a side actually is, for the conditions block. Reads
# only; a failure here degrades the description rather than the run.
describe_side() {
    ref=$1
    dir=$2
    commit=$(git -C "$dir" rev-parse --short HEAD 2>/dev/null) || commit='?'
    if [ "$ref" = WORKTREE ]; then
        dirty=$(git -C "$dir" status --porcelain 2>/dev/null) || dirty=''
        if [ -n "$dirty" ]; then
            printf 'WORKTREE %s (%s + uncommitted changes)\n' "$dir" "$commit"
        else
            printf 'WORKTREE %s (%s, clean)\n' "$dir" "$commit"
        fi
    else
        printf '%s (%s)\n' "$ref" "$commit"
    fi
}

build_side() {
    side=$1
    ref=$2
    dir=$3
    tdir=$4
    log=$work/build-$side.log
    note "side $side ($ref): cargo bench --no-run --bench $bench -> $tdir"
    printf '%s\n' "$tdir" >>"$work/created-targets.txt"
    if ! CARGO_TARGET_DIR=$tdir cargo bench --no-run \
            --manifest-path "$dir/Cargo.toml" --bench "$bench" \
            >"$log" 2>&1; then
        banner "BUILD FAILED on side $side ($ref) -- this run has no numbers"
        cat -- "$log" >&2
        die "side $side did not build; cargo's output is above"
    fi
    # A second, now fully cached, invocation purely to be told where the
    # binary is. Asking the first one in JSON would have turned cargo's own
    # diagnostics into JSON too, and a build failure is the one output here
    # that has to stay readable.
    if ! CARGO_TARGET_DIR=$tdir cargo bench --no-run \
            --manifest-path "$dir/Cargo.toml" --bench "$bench" \
            --message-format=json >"$work/build-$side.json" 2>>"$log"; then
        cat -- "$log" >&2
        die "side $side: cargo could not report its artifacts"
    fi
    awk -v want="\"name\":\"$bench\"" '
        /"reason":"compiler-artifact"/ &&
        index($0, "\"kind\":[\"bench\"]") && index($0, want) {
            parts = split($0, field, /"executable":"/)
            if (parts > 1) {
                path = field[2]
                sub(/".*/, "", path)
                print path
            }
        }
    ' "$work/build-$side.json" >"$work/exe-$side.txt"
    found=$(awk 'END { print NR + 0 }' "$work/exe-$side.txt")
    if [ "$found" -ne 1 ]; then
        die "side $side: cargo's artifact list names $found '$bench' bench" \
            "binaries, not 1"
    fi
}

# Turns one criterion run's stdout into `id<TAB>side<TAB>round<TAB>ns` lines.
#
# criterion writes its progress ("Benchmarking <id>: Warming up ...") to
# stderr and its results to stdout, which is why the two streams are kept
# apart: on stdout a measurement is an id line -- present only when the id is
# too long to fit the column, hence the fallback to the previous line -- and
# then `time:   [lo est hi]`. The middle figure is the point estimate. Zero
# measurements, an unreadable line or an unknown unit are all failures: a
# missing id would otherwise read as a side with fewer rounds.
parse_round() {
    side=$1
    round=$2
    log=$3
    out=$work/round-$round-$side.tsv
    rc=0
    awk -v side="$side" -v round="$round" '
        function to_ns(value, unit) {
            if (unit == "ps") { return value / 1000 }
            if (unit == "ns") { return value }
            if (unit == "\302\265s" || unit == "us") { return value * 1000 }
            if (unit == "ms") { return value * 1000000 }
            if (unit == "s") { return value * 1000000000 }
            return -1
        }
        { sub(/\r$/, "") }
        /time:[ \t]*\[/ {
            head = $0
            sub(/time:[ \t]*\[.*/, "", head)
            gsub(/^[ \t]+|[ \t]+$/, "", head)
            id = (head == "") ? previous : head
            inner = $0
            sub(/.*time:[ \t]*\[/, "", inner)
            sub(/\].*/, "", inner)
            gsub(/^[ \t]+|[ \t]+$/, "", inner)
            fields = split(inner, estimate, /[ \t]+/)
            if (id == "" || fields != 6) {
                printf "unreadable time line: %s\n", $0 >"/dev/stderr"
                bad = 1
                next
            }
            ns = to_ns(estimate[3] + 0, estimate[4])
            if (ns < 0) {
                printf "unknown unit %s: %s\n", estimate[4], $0 >"/dev/stderr"
                bad = 1
                next
            }
            printf "%s\t%s\t%s\t%.6f\n", id, side, round, ns
            seen++
            next
        }
        /^[^ \t]/ && !/^Found / { previous = $0 }
        END {
            if (bad) { exit 3 }
            if (seen == 0) { exit 4 }
        }
    ' "$log" >"$out" || rc=$?
    if [ "$rc" -eq 4 ]; then
        banner "NO MEASUREMENTS: side $side, round $round"
        die "criterion reported no timings -- does the filter match any" \
            "benchmark id of '$bench'?"
    fi
    if [ "$rc" -ne 0 ]; then
        banner "UNREADABLE OUTPUT: side $side, round $round"
        die "could not parse criterion's output; the log is $log"
    fi
    cat -- "$out" >>"$work/samples.tsv"
}

run_round() {
    side=$1
    round=$2
    exe=$3
    tdir=$4
    shift 4
    log=$work/round-$round-$side.out
    note "round $round, side $side: running $exe"
    # `--bench` is not decoration: criterion runs each benchmark once in test
    # mode without it, and reports no timings at all. `--color never` keeps
    # ANSI escapes out of the output being parsed whatever stdout is attached
    # to, and the per-round baseline name stops round 2 overwriting round 1.
    if ! CRITERION_HOME=$tdir/criterion "$exe" --bench --color never \
            --save-baseline "$side-r$round" "$@" \
            >"$log" 2>"$work/round-$round-$side.err"; then
        banner "BENCH RUN FAILED: side $side, round $round"
        cat -- "$log" "$work/round-$round-$side.err" >&2
        die "the bench binary for side $side exited non-zero in round $round"
    fi
    parse_round "$side" "$round" "$log"
}

report() {
    tab=$(printf '\t')
    LC_ALL=C sort -t "$tab" -k1,1 -k2,2 -k3,3n "$work/samples.tsv" \
        >"$work/sorted.tsv"
    # resolve_margin is the one judgement in here: how far past the wider
    # within-side spread a change has to reach before the verdict stops
    # hedging. See the header.
    awk -F "$tab" -v rounds="$rounds" -v resolve_margin=1.25 '
        # The scale criterion prints in, so that a figure here reads against
        # one in its own output or in BENCHMARKS.md.
        function pretty(ns) {
            if (ns < 1) { return sprintf("%.4g ps", ns * 1000) }
            if (ns < 1000) { return sprintf("%.4g ns", ns) }
            if (ns < 1000000) { return sprintf("%.4g \302\265s", ns / 1000) }
            if (ns < 1000000000) { return sprintf("%.4g ms", ns / 1000000) }
            return sprintf("%.4g s", ns / 1000000000)
        }
        function percent(ratio) {
            return sprintf("%+.1f%%", (ratio - 1) * 100)
        }
        # Writes stat_min, stat_max, stat_median and stat_spread, the rounds of
        # one side being few enough that an insertion sort is the whole story.
        function stats(key, count,   i, j, held) {
            for (i = 1; i <= count; i++) { sorted[i] = value[key, i] }
            for (i = 2; i <= count; i++) {
                held = sorted[i]
                j = i - 1
                while (j >= 1 && sorted[j] > held) {
                    sorted[j + 1] = sorted[j]
                    j--
                }
                sorted[j + 1] = held
            }
            stat_min = sorted[1]
            stat_max = sorted[count]
            if (count % 2 == 1) {
                stat_median = sorted[(count + 1) / 2]
            } else {
                stat_median = (sorted[count / 2] + sorted[count / 2 + 1]) / 2
            }
            stat_spread = (stat_min > 0) ? stat_max / stat_min : 0
        }
        function verdict(change, spread, margin, tail,   form) {
            form = "  verdict    change %s vs wider within-side" \
                " spread %s (%.2fx) -- %s\n"
            printf form, percent(change), percent(spread), margin, tail
        }
        function rounds_of(key, count,   i, text) {
            text = ""
            for (i = 1; i <= count; i++) {
                text = text (i > 1 ? ", " : "") pretty(value[key, i])
            }
            return text
        }
        {
            id = $1
            side = $2
            key = id SUBSEP side
            if (!(id in known)) {
                known[id] = 1
                ids[++id_count] = id
            }
            value[key, ++n[key]] = $4 + 0
        }
        END {
            print  ""
            print  "== per id: within-side spread first, then A -> B =="
            for (i = 1; i <= id_count; i++) {
                id = ids[i]
                a = id SUBSEP "a"
                b = id SUBSEP "b"
                print ""
                print id
                if (n[a] == 0 || n[b] == 0) {
                    printf "  UNMATCHED  measured only on side %s" \
                        " -- the two refs do not share this id, so no ratio\n",
                        (n[a] > 0 ? "A" : "B")
                    unmatched++
                }
                if (n[a] > 0) {
                    stats(a, n[a])
                    a_median = stat_median
                    a_spread = stat_spread
                    printf "  A spread   %.3fx (%s) over %d round%s%s\n",
                        a_spread, percent(a_spread), n[a],
                        (n[a] == 1 ? "" : "s"),
                        (n[a] == rounds ? "" : sprintf(" of %d", rounds))
                }
                if (n[b] > 0) {
                    stats(b, n[b])
                    b_median = stat_median
                    b_spread = stat_spread
                    printf "  B spread   %.3fx (%s) over %d round%s%s\n",
                        b_spread, percent(b_spread), n[b],
                        (n[b] == 1 ? "" : "s"),
                        (n[b] == rounds ? "" : sprintf(" of %d", rounds))
                }
                if (n[a] > 0 && n[b] > 0 && a_median > 0) {
                    ratio = b_median / a_median
                    printf "  medians    A %s   B %s   ->  B/A %.3fx (%s)\n",
                        pretty(a_median), pretty(b_median), ratio,
                        percent(ratio)
                    widest = (a_spread > b_spread) ? a_spread : b_spread
                    swing = (ratio > 1) ? ratio : (ratio > 0 ? 1 / ratio : 0)
                    margin = (widest > 1) ? (swing - 1) / (widest - 1) : -1
                    if (n[a] < 2 || n[b] < 2) {
                        print "  verdict    one round per side: no" \
                            " within-side spread to judge it against"
                        unjudged++
                    } else if (margin < 0) {
                        printf "  verdict    change %s, and neither side" \
                            " varied at all -- nothing to judge it against\n",
                            percent(ratio)
                        unjudged++
                    } else if (margin <= 1) {
                        verdict(ratio, widest, margin,
                            "inside it, not a result")
                        inside++
                    } else if (margin <= resolve_margin) {
                        verdict(ratio, widest, margin,
                            sprintf("marginal, under the %.2fx this calls" \
                                " resolved", resolve_margin))
                        marginal++
                    } else {
                        verdict(ratio, widest, margin, "a result")
                        outside++
                    }
                }
                if (n[a] > 0) { printf "  A rounds   %s\n", rounds_of(a, n[a]) }
                if (n[b] > 0) { printf "  B rounds   %s\n", rounds_of(b, n[b]) }
            }
            print ""
            total = "== %d id%s: %d a result, %d marginal, %d inside a" \
                " spread, %d unjudged, %d unmatched ==\n"
            printf total, id_count, (id_count == 1 ? "" : "s"),
                outside + 0, marginal + 0, inside + 0, unjudged + 0,
                unmatched + 0
        }
    ' "$work/sorted.tsv"
}

cargo_version=$(cargo --version 2>/dev/null) || cargo_version='(unknown)'
cores=$(nproc 2>/dev/null) || cores=$(getconf _NPROCESSORS_ONLN 2>/dev/null) \
    || cores='(unknown)'
started=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

note "bench '$bench', $rounds round(s), A then B in each round"
record_load start

make_worktree a "$ref_a" "$dir_a"
make_worktree b "$ref_b" "$dir_b"
desc_a=$(describe_side "$ref_a" "$dir_a")
desc_b=$(describe_side "$ref_b" "$dir_b")

build_side a "$ref_a" "$dir_a" "$tdir_a"
build_side b "$ref_b" "$dir_b" "$tdir_b"
exe_a=$(cat "$work/exe-a.txt")
exe_b=$(cat "$work/exe-b.txt")
[ -x "$exe_a" ] || die "side A's bench binary $exe_a is not executable"
[ -x "$exe_b" ] || die "side B's bench binary $exe_b is not executable"
record_load built

round=1
while [ "$round" -le "$rounds" ]; do
    run_round a "$round" "$exe_a" "$tdir_a" "$@"
    run_round b "$round" "$exe_b" "$tdir_b" "$@"
    record_load "after round $round"
    round=$((round + 1))
done
record_load end

if [ "$rounds" -eq 1 ]; then round_word=round; else round_word=rounds; fi
if [ "$#" -eq 0 ]; then
    criterion_args='(none -- every id in the target)'
else
    # "$*" joins on a space, which is how the arguments were given; an
    # argument with a space of its own is not distinguishable here, and this
    # line is a record of the run rather than something to paste back.
    criterion_args=$*
fi

printf '== bench-ab: an interleaved A/B, %d %s, A then B in each ==\n\n' \
    "$rounds" "$round_word"
printf '  %-14s %s\n' 'started' "$started"
printf '  %-14s %s\n' 'bench' "$bench"
printf '  %-14s %s\n' 'criterion args' "$criterion_args"
printf '  %-14s %s\n' 'side A' "$desc_a"
printf '  %-14s %s\n' 'side B' "$desc_b"
printf '  %-14s %s\n' 'target A' "$tdir_a"
printf '  %-14s %s\n' 'target B' "$tdir_b"
printf '  %-14s %s\n' 'cores' "$cores"
printf '  %-14s %s\n' 'cargo' "$cargo_version"
printf '\n  %-14s %-22s %s\n' 'load average' '1m 5m 15m' 'at (UTC)'
cat -- "$work/load.txt"
report

note "done -- tearing the two worktrees and target dirs down"

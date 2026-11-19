#!/usr/bin/env bash
# Builds Taskflow v4.1.0's OWN benchmark binaries for the five nested shapes
# and runs each at one worker and at all of them, so that BENCHMARKS.md's
# `upstream C++` column can carry upstream's executor measured on this box
# instead of "not published".
#
#   devtools/upstream-bench.sh --list             # the whole plan, no network
#   devtools/upstream-bench.sh                    # clone, build, run all five
#   devtools/upstream-bench.sh --build-only       # stop after the build
#   devtools/upstream-bench.sh --bench nqueens    # one shape (repeatable)
#   devtools/upstream-bench.sh --threads 1,2,24   # thread counts (default 1,nproc)
#   devtools/upstream-bench.sh --rounds 3         # upstream's -r (default 1)
#   devtools/upstream-bench.sh --dir /var/tmp/ub  # where the clone goes
#
# WHY THIS EXISTS. "The upstream C++ column" in BENCHMARKS.md records that
# upstream publishes no figures at all -- its
# `benchmarks/benchmarks.md` documents how to build and drive the suite and
# then shows one plot with no table behind it. So the only way to get a number
# for upstream's own executor is to run it, and the one comparison that settles
# whether a nested shape's scaling is the Taskflow design's or this port's is
# upstream's binary on the same CPU. This script is that build, and it exists
# as a script rather than a paragraph for the reason the coverage and
# documentation instruments do: a figure from a build nobody recorded is not
# comparable with the next one. #384 is the run that filled the five nested
# shapes' column with it; "The #384 measurement" at the end of that file is
# where its output and its reading are.
#
# THE IMAGE MUST BE REBUILT FIRST. The toolchain this needs -- g++, cmake,
# make -- was added to the Dockerfile by the same issue that added this file.
# Nothing in the gate or in CI builds that image, so a container created before
# the rebuild simply lacks all three; the check below says so and names
# `devtools/devcontainer.sh rebuild`, which is host-only. `--list` works
# without any of it.
#
# WHAT IT DOES
#   1. Clones taskflow/taskflow at tag v4.1.0 -- this repo's pin -- into a
#      scratch directory OUTSIDE the repo (default
#      ${TMPDIR:-/tmp}/rustflow-upstream-bench), and refuses a directory inside
#      it. An existing clone at the pin is reused rather than re-fetched, which
#      is what makes a second run cheap; nothing is ever deleted, so remove the
#      directory by hand when done with it -- measured, the shallow clone is
#      117 MB and the build another 23.
#   2. Configures it with `CXXFLAGS=-fpermissive cmake -DTF_BUILD_BENCHMARKS=ON
#      -DTF_BUILD_TESTS=OFF -DTF_BUILD_EXAMPLES=OFF -DCMAKE_BUILD_TYPE=Release`
#      into a build directory beside the clone. Tests and examples off because
#      the benchmark targets are the whole point and upstream's unit tests are
#      a doctest build of their own; `-fpermissive` because of the vendored TBB
#      (see below).
#   3. Builds the five targets below, and only those: `cmake --build --target`
#      with `-j $(nproc)`.
#   4. Runs each built target once per thread count with `-m tf` (upstream's
#      default model, and the only one that is ours to compare -- its `tbb` and
#      `omp` models are upstream's baselines, where BENCHMARKS.md's second
#      column is rayon). /proc/loadavg goes out before the first run and after
#      every one, and the toolchain versions and the clone's commit go out
#      first, so the output states the provenance of every figure in it.
#
# THE FIVE TARGETS, verified against `benchmarks/CMakeLists.txt` at the pin,
# where they are numbered in its own comments -- 15, 19, 20, 21 and 23 of the
# 24 host targets:
#
#   bench_skynet      bench_nqueens   bench_integrate
#   bench_fibonacci   bench_merge_sort
#
# These are the five nested shapes -- the ones whose port recurses through a
# `TaskGroup` and a `corun`, which is where the port's scaling question is.
#
# SIZES, and this is the part that does not line up. Each upstream binary
# sweeps a RANGE in one process and prints a row per point; where it takes a
# size argument at all, that argument is the TOP of the sweep rather than a
# point in it. So a cell of our tables is a row of upstream's output, and what
# this script passes is whatever makes that row appear:
#
#   skynet       `MaxDepth = 1..8`, fixed, no flag. Our rows are 3 and 4.
#   nqueens      `-q N` sweeps `q = 1..N` (upstream default 14). `-q 8` covers
#                our 6 and 8.
#   fibonacci    `-f N` sweeps `n = 1..N` (default 40). `-f 15` covers our
#                12 and 15.
#   merge_sort   `N = 10, 100, .., 1e8`, fixed, no flag. Our rows are 10000
#                and 1000000, and they are the 4th and 6th of eight; the last
#                two rows are most of the run and most of the memory (1e8
#                doubles twice over).
#   integrate    `-v V` walks `v = 0, 100, .., V` (default 2000), so OUR SIZES
#                ARE NOT ON ITS GRID AT ALL: this shape's rows are `V = 1` and
#                `V = 2`, which BENCHMARKS.md's own section explains are two
#                orders of magnitude below upstream's sweep because `V = 3`
#                overflows a one-worker stack in the port. `-v 100` is
#                upstream's first non-degenerate point and is ~390_000 spawned
#                tasks against our 828 and 1852. It is run as the nearest thing
#                there is, and it is NOT a cell of our table.
#
# WHAT THE OUTPUT MEANS. Each binary prints `model=`, `num_threads=` and
# `num_rounds=`, then a `size`/`runtime` table of its sweep. **That runtime
# column is MILLISECONDS** -- `main.cpp` divides a `std::chrono::microseconds`
# count by 1e3, and upstream's own plot labels its y-axis in milliseconds --
# where BENCHMARKS.md's tables are in the unit criterion chose per cell. It is
# one run of upstream's own harness per row, timing what upstream times: the
# whole call, no warm-up, no sample distribution, no outlier handling. Nothing
# here is comparable with a criterion interval; what it is for is the factor
# between two columns at one size, which is how every ratio in BENCHMARKS.md is
# read anyway.
#
# THREE THINGS THAT BITE, all three measured rather than assumed:
#
#   * **The vendored TBB needs `-fpermissive` on g++ 13 and later.** Every
#     benchmark target links TBB, and `benchmarks/CMakeLists.txt` builds the
#     TBB 2020.3 upstream vendors under `3rd-party/tbb` by shelling out to its
#     Makefile at CONFIGURE time -- so when that compile fails,
#     `find_package(TBB REQUIRED)` fails after it and there is no build system
#     to run at all. What fails is `include/tbb/task.h`, which declares
#     `tbb::task& task()` inside `internal::task_prefix`: GCC 13 rejects that
#     as changing the meaning of `task` (`-Wchanges-meaning`), an error rather
#     than a warning. Measured on both compilers this is run with, on a
#     translation unit that merely includes the header: a host g++ 13.3 errors
#     and compiles clean with `-fpermissive`, while bookworm's g++ 12.2 --
#     THIS IMAGE's own -- accepts it either way. The flag is therefore inert
#     where the figures are taken, and carried for the toolchain that needs
#     it: a run against a host compiler, or a base image that moves past
#     bookworm. It reaches that compile because TBB's own build honours
#     CXXFLAGS (`CPLUS_FLAGS += $(WARNING_KEY) $(CXXFLAGS)` in
#     `build/common_rules.inc`); cmake also folds it into the benchmark
#     translation units, which costs nothing -- it permits a diagnostic,
#     changes no code generation, and the TBB model is not measured here in
#     any case.
#   * **The binaries need LD_LIBRARY_PATH even though they carry an rpath.**
#     That TBB build produces shared objects under a directory TBBBuild.cmake
#     names itself, and cmake does point the executable at it -- but with
#     DT_RUNPATH, which the loader uses for that object's OWN dependencies and
#     does not pass on to theirs. Read with `readelf -d`: the binary NEEDs
#     `libtbb.so.2` and `libtbbmalloc_proxy.so.2`, which the runpath resolves,
#     and the proxy then NEEDs `libtbbmalloc.so.2` with no runpath of its own,
#     so a bare `./bench_skynet` dies with
#         error while loading shared libraries: libtbbmalloc.so.2
#     one level down. LD_LIBRARY_PATH covers every level, and it also survives
#     a build directory that has been moved, which that absolute rpath does
#     not. This script finds the directory by looking for the library rather
#     than by spelling its path, which is TBBBuild.cmake's own invention and
#     not an interface.
#   * **`bench_merge_sort -t 1` deadlocks, and is skipped.** Upstream's
#     `msort(executor, data)` calls `executor.async(...).wait()` from inside a
#     worker, so with one worker that worker blocks on a task only it could
#     run: measured, the binary printed not one row of its sweep in 420
#     seconds, where `-t 2` completes the whole sweep in about twelve. So the
#     one-worker cell of our `merge_sort` table has no upstream counterpart
#     from this harness as it stands, which is a finding about the harness and
#     not about either executor.
#
# NEVER VENDORED. The clone goes outside the repo and is not committed:
# LICENSE-TASKFLOW covers the translated code in `src/` and `benches/`, not a
# checkout of upstream, and a second copy of a 40 MB tree in the bind mount
# would be a liability rather than a convenience. The default directory is
# under TMPDIR, which inside the container is container-private and goes away
# with it.
#
# NOT A GATE, and not quick to interpret either. It exits non-zero only when
# the measurement could not be made -- a missing tool, a failed clone,
# configure or build, or a binary that exits non-zero. A completed run exits 0
# whatever the numbers say, nothing reads its output, and no CI job runs it.
# Both this and `devtools/bench-ab.sh` want a quiet box: the figures it prints
# are one run each, so a sibling agent building the crate is in them.
set -euo pipefail

me=upstream-bench

# This repo's upstream pin, the same tag the README and BENCHMARKS.md name.
pin=v4.1.0
url=https://github.com/taskflow/taskflow.git

# name|target|extra arguments|which rows of its sweep our tables use
shapes=(
    "skynet|bench_skynet||MaxDepth = 3 and 4 of its fixed 1..8 sweep"
    "nqueens|bench_nqueens|-q 8|q = 6 and 8"
    "integrate|bench_integrate|-v 100|V = 1 and 2, which are NOT on this grid"
    "fibonacci|bench_fibonacci|-f 15|n = 12 and 15"
    "merge_sort|bench_merge_sort||N = 10000 and 1000000 of its fixed sweep"
)

note() {
    printf '%s: %s\n' "$me" "$*" >&2
}

die() {
    printf '%s: %s\n' "$me" "$*" >&2
    exit 1
}

# Usage errors exit 2, so a caller can tell "you held it wrong" from "the
# measurement failed".
usage_error() {
    printf '%s: %s\n' "$me" "$*" >&2
    printf 'usage: %s [--list] [--build-only] [--bench NAME]...' "$0" >&2
    printf ' [--threads LIST] [--rounds N] [--dir DIR]\n' >&2
    printf '       %s --help\n' "$0" >&2
    exit 2
}

# The header above, with the comment markers taken off: the documentation and
# the help text cannot drift apart if there is only one of them.
help() {
    sed -n '2,/^set -euo pipefail$/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'
}

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

list=0
build_only=0
rounds=1
threads=""
dir="${TMPDIR:-/tmp}/rustflow-upstream-bench"
wanted=()

while [ "$#" -gt 0 ]; do
    case "$1" in
        --list) list=1 ;;
        --build-only) build_only=1 ;;
        --bench)
            [ "$#" -ge 2 ] || usage_error "--bench needs a shape name"
            wanted+=("$2")
            shift
            ;;
        --threads)
            [ "$#" -ge 2 ] || usage_error "--threads needs a list, e.g. 1,24"
            threads="$(printf '%s' "$2" | tr ',' ' ')"
            shift
            ;;
        --rounds)
            [ "$#" -ge 2 ] || usage_error "--rounds needs a count"
            rounds="$2"
            shift
            ;;
        --dir)
            [ "$#" -ge 2 ] || usage_error "--dir needs a directory"
            dir="$2"
            shift
            ;;
        -h|--help) help; exit 0 ;;
        *) usage_error "unknown argument '$1'" ;;
    esac
    shift
done

case "$rounds" in
    ''|*[!0-9]*) usage_error "--rounds wants a positive integer, got '$rounds'" ;;
    0) usage_error "--rounds 0 would measure nothing" ;;
esac

# 1 and all of them, which on the box BENCHMARKS.md is measured on is 1 and 24.
# Derived rather than hardcoded so a run on another box still asks the two
# questions this comparison is about -- the one-worker constant and the
# full-width one -- rather than a 24 that box does not have.
if [ -z "$threads" ]; then
    threads="1 $(nproc)"
fi
for t in $threads; do
    case "$t" in
        ''|*[!0-9]*) usage_error "--threads wants integers, got '$t'" ;;
        0) usage_error "--threads 0 is not a thread count" ;;
    esac
done

# Select the shapes to work on, keeping the order above rather than the order
# the flags came in, so two runs of the same set print in the same order.
names=""
for spec in "${shapes[@]}"; do
    names="$names ${spec%%|*}"
done

selected=()
if [ "${#wanted[@]}" -eq 0 ]; then
    selected=("${shapes[@]}")
else
    for want in "${wanted[@]}"; do
        found=0
        for spec in "${shapes[@]}"; do
            if [ "${spec%%|*}" = "$want" ]; then found=1; fi
        done
        [ "$found" -eq 1 ] || usage_error "no such shape '$want' (one of:$names)"
    done
    for spec in "${shapes[@]}"; do
        for want in "${wanted[@]}"; do
            if [ "${spec%%|*}" = "$want" ]; then selected+=("$spec"); fi
        done
    done
fi

# The scratch directory must not be inside the repo. See "NEVER VENDORED"
# above: the clone is not ours to carry, and .gitignore is not the mechanism
# that should be keeping it out of the tree. `realpath -m` so that the check is
# against a resolved absolute path and does not need the directory to exist.
dir="$(realpath -m "$dir")"
case "$dir" in
    "$root"|"$root"/*)
        die "--dir must be outside the repo ($root); the clone is never vendored"
        ;;
esac

clone="$dir/taskflow-$pin"
build="$dir/build"

tools_line() {
    local line=""
    local tool
    for tool in git cmake make g++; do
        if command -v "$tool" >/dev/null 2>&1; then
            case "$tool" in
                git) line="$line git $(git --version | awk '{print $3}')," ;;
                cmake) line="$line cmake $(cmake --version | head -1 | awk '{print $3}')," ;;
                make) line="$line make $(make --version | head -1 | awk '{print $3}')," ;;
                g++) line="$line g++ $(g++ -dumpfullversion)," ;;
            esac
        else
            line="$line $tool ABSENT,"
        fi
    done
    printf '%s' "${line# }" | sed 's/,$//'
}

if [ "$list" -eq 1 ]; then
    cat <<LIST
$me: --list, so nothing below is run and nothing is fetched.

toolchain here : $(tools_line)
clone          : $url at tag $pin
                 -> $clone  (reused if already at the pin)
build directory: $build
targets        :$(for spec in "${selected[@]}"; do
                      IFS='|' read -r _n t _a _r <<<"$spec"; printf ' %s' "$t"
                  done)
threads        : $threads
rounds         : $rounds

the clone:
  git clone --depth 1 --branch $pin $url $clone

the configure:
  CXXFLAGS=-fpermissive cmake -S $clone -B $build \\
      -DCMAKE_BUILD_TYPE=Release -DTF_BUILD_BENCHMARKS=ON \\
      -DTF_BUILD_TESTS=OFF -DTF_BUILD_EXAMPLES=OFF

the build:
  cmake --build $build --target$(for spec in "${selected[@]}"; do
      IFS='|' read -r _n t _a _r <<<"$spec"; printf ' %s' "$t"
  done) -j $(nproc)

the runs, with LD_LIBRARY_PATH set to the TBB build beside them:
LIST
    for spec in "${selected[@]}"; do
        IFS='|' read -r name target args rows <<<"$spec"
        printf '  %s -- our rows: %s\n' "$name" "$rows"
        for t in $threads; do
            if [ "$name" = merge_sort ] && [ "$t" -eq 1 ]; then
                printf '    (skipped) %s -t 1: upstream deadlocks at one worker\n' \
                    "$target"
                continue
            fi
            printf '    %s/benchmarks/%s -t %s -r %s%s\n' \
                "$build" "$target" "$t" "$rounds" "${args:+ $args}"
        done
    done
    cat <<'LIST'

Read `--help` for what the runtime column is (milliseconds), why
`-fpermissive` is there, and why merge_sort has no one-worker row.
LIST
    exit 0
fi

# --- From here on the tools are needed -------------------------------------
missing=""
for tool in git cmake make g++; do
    command -v "$tool" >/dev/null 2>&1 || missing="$missing $tool"
done
if [ -n "$missing" ]; then
    cat >&2 <<MSG
$me: this environment is missing:$missing

In the container, the Dockerfile installs g++, cmake and make for exactly this
script -- but nothing in the gate or in CI builds the image, so a container
created before that layer existed lacks them. Rebuild from the HOST --

    devtools/devcontainer.sh rebuild

-- rather than apt-getting them in this container by hand, so that the compiler
a figure was measured with is the one the Dockerfile records. On a host they
are yours to install, and the figures are then not the image's. Either way,
\`$0 --list\` shows the plan without them.
MSG
    exit 1
fi

mkdir -p "$dir"

if [ -d "$clone/.git" ]; then
    at="$(git -C "$clone" describe --tags --exact-match 2>/dev/null || echo "")"
    [ "$at" = "$pin" ] || die "$clone exists but is not at $pin (describe: '${at:-none}'); remove it"
    note "reusing the clone at $clone ($pin)"
else
    note "cloning $url at $pin into $clone"
    git clone --depth 1 --branch "$pin" "$url" "$clone"
fi

sha="$(git -C "$clone" rev-parse --short HEAD)"

note "configuring into $build (CXXFLAGS=-fpermissive, see --help)"
CXXFLAGS=-fpermissive cmake -S "$clone" -B "$build" \
    -DCMAKE_BUILD_TYPE=Release \
    -DTF_BUILD_BENCHMARKS=ON \
    -DTF_BUILD_TESTS=OFF \
    -DTF_BUILD_EXAMPLES=OFF

targets=()
for spec in "${selected[@]}"; do
    IFS='|' read -r _name target _args _rows <<<"$spec"
    targets+=("$target")
done

note "building: ${targets[*]}"
cmake --build "$build" --target "${targets[@]}" -j "$(nproc)"

for target in "${targets[@]}"; do
    [ -x "$build/benchmarks/$target" ] \
        || die "built, but $build/benchmarks/$target is not there"
done

if [ "$build_only" -eq 1 ]; then
    note "--build-only: the binaries are in $build/benchmarks"
    exit 0
fi

# The directory TBBBuild.cmake put libtbb.so.2 and libtbbmalloc.so.2 in. Found
# rather than spelled: the name it derives from `tbb_build_prefix` is its own
# business and not an interface. Without it every binary dies on startup.
tbbmalloc="$(find "$build" -name 'libtbbmalloc.so.2' -print -quit)"
[ -n "$tbbmalloc" ] || die "the TBB build produced no libtbbmalloc.so.2 under $build"
tbblib="$(dirname "$tbbmalloc")"

printf '\n'
printf '%s: toolchain   %s\n' "$me" "$(tools_line)"
printf '%s: upstream    %s at %s (%s)\n' "$me" "$url" "$pin" "$sha"
printf '%s: box         nproc %s, loadavg %s\n' \
    "$me" "$(nproc)" "$(cut -d' ' -f1-3 /proc/loadavg)"
printf '%s: runs        -m tf, -r %s, threads: %s\n' "$me" "$rounds" "$threads"
printf '%s: UNITS       the runtime column below is MILLISECONDS\n' "$me"
printf '%s: date        %s\n' "$me" "$(date -u '+%Y-%m-%d %H:%M:%SZ')"
printf '\n'

for spec in "${selected[@]}"; do
    IFS='|' read -r name target args rows <<<"$spec"
    for t in $threads; do
        # See "THREE THINGS THAT BITE": one worker cannot run this one at all.
        if [ "$name" = merge_sort ] && [ "$t" -eq 1 ]; then
            note "$name at 1 thread: skipped, upstream deadlocks there (see --help)"
            continue
        fi
        printf '=== %s, %s thread(s) -- our rows: %s\n' "$name" "$t" "$rows"
        printf '    %s -t %s -r %s%s\n' "$target" "$t" "$rounds" "${args:+ $args}"
        # Unquoted $args on purpose: it is a fixed flag pair from the table at
        # the top of this file, not caller input.
        LD_LIBRARY_PATH="$tbblib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
            "$build/benchmarks/$target" -t "$t" -r "$rounds" $args
        printf '    loadavg after: %s\n\n' "$(cut -d' ' -f1-3 /proc/loadavg)"
    done
done

note "done; the clone and build stay in $dir -- remove it by hand when finished"

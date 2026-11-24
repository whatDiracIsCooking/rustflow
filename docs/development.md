# Developing rustflow

Everything it takes to build, test and gate this crate. The
[`README`](../README.md) is the page a reader of the crate wants; this is the
page a contributor wants, and nothing in it is needed to *use* rustflow.

## Getting a toolchain

There is no host toolchain requirement — Rust lives in the container, pinned by
`ARG RUST_VERSION` in the root [`Dockerfile`](../Dockerfile) (currently
`1.100-slim-bookworm`). Two front ends build that one image, so a result in
either means the same thing:

| | |
|---|---|
| **Interactive / agent** | `devtools/devcontainer.sh` (or open `.devcontainer/` in an IDE) |
| **Batch / CI** | `docker compose -f docker/compose.yaml run --rm check` |

```sh
devtools/devcontainer.sh up             # start the container
devtools/devcontainer.sh check          # the gate: fmt, clippy, test, deny, doc
devtools/devcontainer.sh shell          # bash inside it
devtools/devcontainer.sh claude         # Claude Code inside it
devtools/devcontainer.sh test graph::   # cargo test, filtered
devtools/devcontainer.sh down           # stop it (volumes survive)
```

`check` is the gate: `cargo fmt --all --check`, then
`cargo clippy --all-targets --all-features -- -D warnings`, then
`cargo test --all-features`, then `cargo deny check` — licenses, RustSec
advisories, duplicate crate versions and crate sources, configured in
[`deny.toml`](../deny.toml) and pinned by `ARG CARGO_DENY_VERSION` — and last
`RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features`, which holds
the docs to the standard the crate-wide `missing_docs` lint already implies:
a link into a private item renders as nothing for a reader of the public
docs, and `cargo test` only catches a link that is outright broken. Anything
that passes locally should pass in CI, with one wrinkle in the shape rather
than the result: CI runs four of the five commands in this image and
`cargo deny check` as a separate `deny` job on a plain runner, because the
image CI uses is the stock `rust` one and building cargo-deny from source
there would cost every PR minutes.

The loom models in `tests/loom_*.rs` are the one test target the gate does not
build — they need a `cfg(loom)` rebuild of the whole dependency graph, which
only `RUSTFLAGS` sets — so
[`.github/workflows/loom.yml`](../.github/workflows/loom.yml) runs them weekly
and on `workflow_dispatch` instead of per PR. There are nine, one per
lock-free protocol or cross-thread rule a machine-checked model states better
than a paragraph: the notifier's wait protocol, the run storage's
wake-after-last-release rule, the delivery of a failure under a runtime task,
`Runtime::scope`'s join rule, a node's dispatch claim, the `AsyncHandle`
delivery slot, the `TaskGroup` counter, the semaphore's
park-and-reschedule rule, and the sweep of the other workers' queues a worker
makes between arming its waiter and parking in it.

[`.github/workflows/miri.yml`](../.github/workflows/miri.yml) is the second
weekly lane, and what it checks is the half loom cannot: loom explores every
ordering of a *model* whose atomics it swapped out, while Miri interprets the
real code and objects to what that code does along the way — an aliasing rule
broken, a read of dangling or uninitialised memory, a misaligned access, a
data race. Miri is a nightly-only rustup component, so the lane carries a
second toolchain pinned by date in the two places that have to agree,
`ARG MIRI_NIGHTLY` in the [`Dockerfile`](../Dockerfile) and the toolchain name
in the workflow; a bare `nightly` would make the interpreter's verdict depend
on the day it ran. The lane is **two steps**, and neither is the whole `--lib`
suite: at roughly a hundredth of native speed, the four-hundred-odd cases of
that suite — many of them a multi-worker executor over thousands of nodes —
are not a job that finishes, and a lane that covers the protocols weekly is
worth more than one that times out.

The first step runs the unit tests of the three modules the interpreter is
worth the most over: `executor::notifier`'s lock-free waiter stack,
`executor::async_node`'s recycling of free nodes, and
`pipeline::scheduler`'s line protocol. That is not the same list as "the
modules holding the crate's `unsafe`", and the difference is worth keeping
straight: production `unsafe` is two modules and two only,
`executor::notifier` and `executor::scope`, as the README's own bullet and
[`porting.md`](porting.md)'s "The two `unsafe` modules" both say;
`async_node`'s is a counting allocator inside its own `#[cfg(test)]` module,
and `pipeline::scheduler` has none at all, its docs having argued the
`UnsafeCell` upstream reaches for down to a `Mutex`. What those three share is
a discipline the type system is not checking for them, which is the half of
Miri that is the data-race detector rather than the aliasing model.

The second step is the **lifecycle witness set**, and it is where the lane
reaches `executor::scope`: that module's two lifetime-erasing `transmute`s
appear in no test module of the step above. The unit tests drive one waiter
slot, one free node, one scheduler at a time, from the test's own thread,
while the races those disciplines exist for are in the lifecycle — a worker
parking in the notifier while another wakes it, a free node released by its
last share on a thread that did not allocate it, `Runtime::scope` returning
while a child it spawned is still being dropped. Reaching those takes a real
pool with real work in it, so the step runs four cases from the integration
suites, one per guarded module, each at **two workers**, the smallest pool in
which every ordering those disciplines are for is possible:
`basics::run_and_wait_simple` for the notifier,
`task_group::a_silent_runtime_shaped_child_is_joined_and_reported_by_its_group`
for `async_node`,
`scope::a_cancelled_run_drops_every_unstarted_child_before_scope_returns` for
`scope`, and `pipelines::set_callable_replaces_the_body_a_pipe_runs` for the
pipeline scheduler. Three of those four pin the count; the cancellation case
sweeps one to four workers, `tests/scope.rs` having no two-worker-only
cancellation case, so two is one of the four shapes it runs rather than the
only one. Each runs on its own, with its own wall time written into
the job summary, because the affordability of this step is a measurement and
not a hope: the four cost ten seconds of interpretation between them, seven
and a half of those the cancelled-scope case, and some twenty seconds for the
step including the build of the four test binaries. A `timeout` caps each case
at **five minutes** of interpretation, forty times the slowest of them — a
watchdog on a witness that has stopped making progress, not a target.

**Nothing in either step is `#[cfg_attr(miri, ignore)]`d.** In the first, an
ignore would owe a reason both here and in the workflow's reason table, and
the job fails on an ignored test missing from that table, so the two lists
cannot drift apart. In the second there is no such table and no need of one:
each witness must report exactly one passing case or the step fails, so an
ignore, a rename and a filter that matches nothing all stop the lane instead
of emptying it quietly. Nor is any witness `cfg(miri)`-only — all four are
ordinary members of the gate's `cargo test`, which is what keeps a case the
interpreter depends on from rotting behind a flag nothing else builds.

Two `MIRIFLAGS` are set, and both are there because `crossbeam-epoch` is in
the tree rather than because of anything in this crate — which is also why
each says so out loud in the workflow beside the flag:

- `-Zmiri-tree-borrows` picks Miri's newer aliasing model over the default
  Stacked Borrows. It is not a weakening: the aliasing check and the data-race
  detector both still run, under a different set of rules for what a derived
  pointer may do. Stacked Borrows rejects `crossbeam_epoch`'s `element_of`,
  which derives a pointer to a whole `Local` from a pointer to its list
  `Entry` field — the `container_of` pattern, which Tree Borrows allows — and
  `Stealer::steal` reaches it the moment a second worker steals.
- `-Zmiri-ignore-leaks` turns off the exit-time leak check and nothing else.
  The leaks are that crate's epoch-based reclamation working as designed: a
  worker's `Local` and the sealed garbage bag its thread-local handle pushes
  at thread exit are freed only when some thread pins again, and once an
  `Executor` has joined its workers nothing does. No frame of this crate
  appears in their allocation backtraces, and a leak is not undefined
  behaviour.

The limit that leaves is in the log rather than hidden: the same crate earns
an integer-to-pointer cast warning, and Tree Borrows does not model such a
cast, so a pointer derived that way is one this lane cannot fully vouch for.
Between a model that rejects the dependency outright and one that does not
model one of its casts, the job is as strict as the dependency allows.
`-Zmiri-disable-isolation` is deliberately absent, since nothing either step
runs reads the host's environment or filesystem. The witness set does read a
clock — every rendezvous in `tests/` is a `spin_until` with an
`Instant::now()` deadline — and that is the one isolation answers rather than
refuses: Miri serves the monotonic clock from a virtual one of its own,
advanced by interpreter steps rather than by the host, which is also why the
120-second give-up in `tests/common/spin.rs` is unreachable under it. Only
`SystemTime` would have wanted the host's. Both flags live on the job rather
than on either step, there being two steps that want the same two. In the
container:

```sh
MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-ignore-leaks" \
  cargo +nightly-2026-11-18 miri test --lib -- executor::notifier::tests
MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-ignore-leaks" \
  cargo +nightly-2026-11-18 miri test --test scope -- \
  --exact a_cancelled_run_drops_every_unstarted_child_before_scope_returns
```

That toolchain is in the image rather than in `Cargo.toml`, so, exactly as for
the coverage pins below, a container created before the layer that installs it
has no `+nightly-2026-11-18` to run and needs a **host-side**
`devtools/devcontainer.sh rebuild` first.

CI runs the suite a second time as the `two-core` job of
[`.github/workflows/ci.yml`](../.github/workflows/ci.yml): once, pinned with
`taskset -c 0,1`, because most of the lifecycle races closed in this port were
found that way rather than by a full-width run — two runnable workers sharing
one core interleave a release against the wake that follows it in orders
neither a 24-core box nor a 4-core runner produces. It is one run against the
same bar, not a repeat count, so a failure in that job is a bug to file and
never a test to loosen.

## Working inside the container

The image carries the agent tooling as well as the toolchain, pinned the same
way `RUST_VERSION` is: `ARG CLAUDE_CODE_VERSION` and `ARG GH_VERSION` in the
[`Dockerfile`](../Dockerfile). Bump the arg and rebuild rather than letting
either self-update — an image whose contents depend on the day it was built is
the thing the pins exist to prevent.

```sh
devtools/devcontainer.sh claude         # Claude Code, TTY attached
devtools/devcontainer.sh shell -c 'gh pr checks 12'
```

The image also carries the two measurement tools the gate does not run:
`cargo-llvm-cov` for coverage and `cargo-mutants` for mutation testing, pinned
by `ARG CARGO_LLVM_COV_VERSION` and `ARG CARGO_MUTANTS_VERSION` beside
`CARGO_DENY_VERSION`, with the `llvm-tools` rustup component the first one
drives. `devtools/coverage.sh` runs the suite under `cargo llvm-cov` and prints
the per-file table and total over `src/` alone (`--html` adds a report under
`/target/llvm-cov/html/`); `cargo mutants` is run by hand against the module
whose tests are in question. Both are tools, not dependencies — `Cargo.toml`,
`Cargo.lock` and `deny.toml` know nothing of them. Any change to the
`Dockerfile`, these pins included, takes effect only after a **host-side**
rebuild of the image, `devtools/devcontainer.sh rebuild`: nothing in the gate
or in CI builds the image, so that rebuild is the check that a `Dockerfile`
edit works, and a container that predates a pin simply lacks the tool (the
coverage script says so and names this command).

`--functions` is the mode the coverage dispositions read: it lists every
function the suite never executed, grouped by file, one line per *instantiation
group* — llvm-cov counts a generic function once however many monomorphisations
it emitted — with `file:line:col` and the demangled symbol. It comes in two
sections, **production code** and **`src/`'s own `#[cfg(test)]` code**, because
the exclusion above is a path regex and a path cannot see a module: most of
what no test runs is unit-test code, and without the split a disposition reads
a number that is mostly its own test closures. The partition tests for
`::tests::` in the demangled symbol's path up to the first `::<`, so a
production generic instantiated with a test's closure is counted where it is
defined, and it tests **every** monomorphisation in the group rather than the
one symbol the group prints: a blanket impl writes its Self type ahead of that
cut, and the symbol a group is named after is the lowest mangled name among its
instantiations, which embeds the crate's disambiguator hash and so differs
between this container and the CI runner. Deciding over all of them is what
keeps the two counts the same number on both (31 of today's groups have
instantiations that disagree). It tracks the convention that a `#[cfg(test)]`
module is called `tests` and nothing stronger, so a `#[cfg(test)]` helper
declared outside one — `Semaphore::num_waiters` and `FreeNodes::len` today —
counts as production, as does a `const _` compile-time assertion, which no run
executes at all. **Only the function counts are split**: the line and region
percentages are llvm-cov's own, and count `src/`'s test code in their
denominators too, so those and the production function count do not share a
denominator. Each section states its own count and its own denominator, and
both are checked against the table above them. The listing is sorted and
carries no timestamp, so two runs differ only where the coverage did, and the
script refuses to print either section when the counts disagree with that
table. The `coverage` job in
[`.github/workflows/coverage.yml`](../.github/workflows/coverage.yml) runs that
same command on every pull request and every push to `main`, in the image `ci`
uses with `llvm-tools` and the pinned `cargo-llvm-cov` added, and publishes the
table, the total and the floors report in the job summary and the listing as a
`functions-never-executed` artifact. It runs `--check-floors` in the same step,
after the report and last of all, so that a floor it does not meet fails the
job with the table already in the log — the summary step is `if: always()` and
posts it either way. One step rather than two deliberately: the figures the
check judges have to be the figures the job published, and a second step would
be a second measurement of a tree whose figures move by a few lines between
runs.

The baseline that job starts from, measured on 2026-11-21 by
`devtools/coverage.sh --functions` at commit 59cf9e4: **96.20% of 17035 lines**
(647 missed), **89.51% of 2412 functions** (253 never executed, spread over 43
files) and 96.29% of 19746 regions, with the per-file low end `flow_builder.rs`
at 50.55% of its functions (45 of 91) and 84.95% of its lines (56 of 372). The
line and region figures move by a few lines between runs of the same tree —
which worker steals what decides which scheduler paths a run takes, and the
spread behind this entry was 96.17% to 96.21% of lines — so read them to two
significant figures. The function figure is the one the dispositions work from,
and the one the listing is checked against, which is why it is the count stated
per file rather than a percentage. That listing is what the milestone's
disposition issues read, and this is the before their after is set against.

How those 253 divide, which is why #442 split the listing: measured at commit
5d804e4, where the total was still that same 253 of 2412, **55 of 1267
production groups** and **198 of 1145 groups in `src/`'s own `#[cfg(test)]`
code**. The figure the disposition issues were scoped against was mostly their
own test closures — empty ones filling a handle variant's argument slot in a
test asserting which variant landed there, 45 of them in `flow_builder.rs`
(whose misses are *all* test closures, so the 50.55% above is not a production
gap at all), 27 in `executor/topology/mod.rs`, 18 in `task.rs` and 14 in
`subflow.rs`. Re-measured on 2026-11-21 at commit 3d9a290, with all five of
this milestone's dispositions landed — #424, #425, #426, #427 and #428:
**223 of 2416** never executed, **25 of 1267 production** over 16 files and
**198 of 1149** in test code over 33, at 97.36% of 17072 lines and 97.10% of
19797 regions. Their tests took the production count from 55 to 25 and left
the test-code count exactly where it was, which is the split earning its keep:
the files they covered are out of the production section altogether —
`algorithm/partitioner.rs`'s 11 groups, `algorithm/mod.rs`'s 6, and the five
between `pipeline/` and `observer/` — and the production low end is now
`executor/async.rs` at 4 groups, then `executor/topology/run.rs` at 3. The
253-including-test-code total is what this milestone's merged pull requests
quote and is still the first line of the listing; the production number is the
one a coverage gap can be read off.

**The floors.** [`devtools/coverage-floors.txt`](../devtools/coverage-floors.txt)
holds one `path  percent` line per measured file of `src/`, sorted and keyed on
the repository-relative path. `devtools/coverage.sh --check-floors` measures,
names every file whose **line** coverage is below its floor with the floor and
the measured value, and exits non-zero, which is what the `coverage` job turns
into a red check; `--write-floors` rewrites the file from the run it has just
done, and is how a floor rises once a tree has earned it. **A file with no
line of its own there has no floor**, so a new module is ungated until someone
writes its line — the check counts the files it finds in that state rather than
passing over them in silence, but it does not fail over them. The opposite case
does fail: a floor whose file the measurement does not report, which is a
renamed or deleted file about to lose its floor without a word, and the message
says to re-run `--write-floors` rather than reporting a regression. One of the
crate's 55 `src/` files is in neither state and never will be — `pipeline/mod.rs`
is re-exports only, so llvm-cov reports no line of it at all — which is why
there are 54 floors and not 55.

**The margin is the larger of two percentage points and four missed lines,
rounded down to a whole point**, and the figure behind that is the run-to-run
spread, measured rather than assumed. The same measured tree — `src/` and
`tests/` at commit af137aa, identical there to 3d9a290's and untouched by the
change that added the floors — read sixteen times in this container and once in
the `coverage` job of the same code: the crate line total moved between
**97.34% and 97.39%** here (446 to 454 missed lines of 17072) and read
**97.33%** on the runner (455). Per file, **45 of the 54 are
the same figure in every one of those seventeen measurements**; the nine that
move are the concurrency-sensitive ones, because which worker steals what
decides which scheduler paths a run takes. The largest movement is
`executor/notifier.rs`, **five lines of 535 and 0.93 points** (10 to 15 missed),
then `algorithm/partitioner.rs` at four lines and `algorithm/for_each_index.rs`
at three; the other six move by one. Nothing under 226 lines moved at all, and
no file's figure on the runner is outside the range this box produced — though
`notifier.rs` on the runner sits at the bottom of it.

The issue proposed one point. One point would have survived all seventeen of
those measurements — floors written from any one of them are met by all of
them — but the headroom it leaves is a line or two: **0.55 points** on
`algorithm/partitioner.rs` for the floors as actually written, and **0.20
points, one line of 535**, on `notifier.rs` had they been written from the
luckiest of the seventeen runs. And because it is a percentage, it leaves the
small files tighter still. Six files under 100 lines would tolerate less than
one extra missed line: `lib.rs` 0.03 of a line, `graph/handle.rs` 0.33,
`executor/scope.rs` 0.40, `executor/rng.rs` 0.55, `runtime.rs` 0.85,
`observer/timeline.rs` 0.97. A 33-line file at 100% is the clearest case — one
point below it is a floor that one missed line fails. So two points, which
governs every file where movement was actually observed (10.7 lines on
`notifier.rs`, 12.4 on `partitioner.rs`), and four lines for the files under
200 where two points is less than that and where no measurement moved by even
one line. Over the same seventeen measurements this rule leaves a thinnest
headroom of **1.55 points** (1.20 from the luckiest source run), and no single
interleaving-dependent line can fail the gate on any file.

**The risk it takes is the other one**, deliberately. A margin this wide will
not notice a small loss: four lines on a small file, two points on a large one,
and on the smallest files a floor only a gross regression trips —
`executor/scope.rs` is floored at 80% against a measured 93.33%, and `src/lib.rs`,
three executable lines long, gets a floor of 0, which gates nothing and says so
when it is written. That is the trade taken on purpose. A floor that flakes is
paid for by every pull request, and the first thing it buys is the habit of
ignoring a red `coverage`; a floor that is too low is paid for once, by a
reviewer, and `--write-floors` raises it in one command on a tree that has
earned it. The floors can only rise from here: the mutation chain adds tests,
and the re-measure that closed the milestone raised nine of the 54 on the
strength of them, which "What the suite is held to" below names.

**They are floors on line coverage only, and a line floor is partly a floor on
test code.** Regions and functions are not floored: the never-executed function
count is the figure the dispositions work from and is already split per file
into production and test code, while llvm-cov's line and region denominators
are not split at all, for the reason above — the exclusion is a path regex and
a `#[cfg(test)] mod tests` lives in the same file as the code it tests. So a
file's line percentage counts its own unit tests, and a floor on it gates them
too: **32.4% of the crate's `src/` lines sit below a column-0 `#[cfg(test)]`**,
and `executor/topology/mod.rs` is 1468 of 1484 lines of test module, so its
floor is almost entirely a statement about test code. Two consequences follow.
Adding unit tests raises a file's percentage by itself, since a test's own
lines are covered by the fact of having run — a floor can be met without
covering any more production code. And deleting a test module takes its lines
out of the denominator as well as the numerator, so what a floor catches is
coverage going away, not coverage never written. For production coverage the
figure to read is still the production function count above.

One more thing is in the image for a measurement rather than for the build:
`g++`, `cmake` and `make`, in the last layer of the
[`Dockerfile`](../Dockerfile) and there for `devtools/upstream-bench.sh`, which
builds **Taskflow v4.1.0's own benchmark binaries** and runs them on this box.
That is what filled [`BENCHMARKS.md`](../BENCHMARKS.md)'s `upstream C++`
column for the five nested shapes, which is empty for the other seventeen
because upstream publishes no figures at all — the one comparison that can say
whether a nested shape's scaling is the Taskflow design's or this port's being
upstream's own executor on the same CPU. That run is #384, and "The #384
measurement" at the end of that file is its output and its reading.
The script clones upstream at the pin into a scratch directory
**outside** the repo — never vendored, because `LICENSE-TASKFLOW` covers the
translated code in `src/` and `benches/` and not a checkout — configures it
with the benchmark targets on and tests and examples off, and builds the five
nested shapes' targets: `bench_skynet`, `bench_nqueens`, `bench_integrate`,
`bench_fibonacci` and `bench_merge_sort`.

```sh
devtools/upstream-bench.sh --list        # the plan, with no network and no toolchain
devtools/upstream-bench.sh               # clone, build, run the five
devtools/upstream-bench.sh --build-only  # stop after the build
devtools/upstream-bench.sh --bench nqueens --threads 1
```

**The image has to be rebuilt before it will run.** Exactly as for the
coverage pins above, nothing in the gate or in CI builds the image, so a
container created before that layer existed has none of the three tools; the
script says so and names `devtools/devcontainer.sh rebuild`, which is
host-only. `--list` needs none of them, so it is also how to tell which kind of
container you are sitting in. Three things from its `--help` are worth knowing
before reading a number out of it, all three measured rather than assumed: the
`runtime` column upstream prints is **milliseconds**; upstream's binaries sweep
a size *range* per process, so a cell of our tables is a row of its output and
`integrate`'s two sizes are not on upstream's grid at all; and
`bench_merge_sort` **deadlocks at one worker** in upstream's own harness, which
leaves that one cell unobtainable this way. Like the reports below it is one
run per row with no repeat count, so a quiet box matters as much as it does for
`devtools/bench-ab.sh`.

Three more instruments need nothing from the image. Two are POSIX `sh` and
measure the documentation contract in [`DESIGN.md`](../DESIGN.md) rather than
the code: `devtools/doc-ratio.sh` prints, per production file, how many of its
lines are prose, doc example, `# Design` rationale and code, with the ratio of
the first to the last, and `devtools/test-names.sh` snapshots and compares the
set of test names, which is how a documentation pass shows it renamed no test.
The third is Python 3 with nothing outside its standard library:
`devtools/api-witness.py` counts the public declarations in `src/` that no
test, example, benchmark or doc example witnesses, reading each call site
against the type it reaches rather than against a bare function name.

**Which of these decide anything: one of them, in one mode.** Four of
`devtools/doc-ratio.sh`, `devtools/test-names.sh`, `devtools/coverage.sh`,
`devtools/api-witness.py` and `devtools/upstream-bench.sh` are **reports** —
each exits 0 whatever it finds, no CI job makes one a pass or a fail, and a
number from one is something a reviewer reads rather than something that
passes or fails.
`test-names.sh diff` and a plain or `--functions` run of `coverage.sh` exit
non-zero only on a difference the PR running them asked about or a measurement
that could not run at all; `api-witness.py` exits 2 for the same second reason,
and `upstream-bench.sh` for a missing tool or a clone, configure, build or run
that failed. `cargo mutants` is in the same class, run by hand over
a module at a time. The exception is `coverage.sh --check-floors`, which the
`coverage` job runs on every pull request and which fails over a figure: a file
of `src/` below its line in `devtools/coverage-floors.txt`. What **gates** is
the five-command `check` above, that one check, and the two things CI's
`examples` job runs: `devtools/run-examples.sh`, and every
benchmark id once in criterion's untimed `--test` mode, which asks whether the
suite still runs and deliberately times nothing.

### Credentials

**Claude Code needs no setup.** The `dev` service bind-mounts your `~/.claude`
as the container's config directory, so the container is logged in because you
are — no token to mint, no `/login` to answer, and `nuke` cannot clear it
(it drops volumes, and this is a bind).

Two details make that safe rather than merely convenient, and both are worth
knowing before changing the mount:

- **A directory, not just `.credentials.json`.** A single-file bind mount can
  be written through but never *replaced*: an atomic write-temp-then-rename —
  how a careful writer updates a credential — fails on one with
  `OSError: [Errno 16] Device or resource busy`. Mounting the directory keeps
  the rename inside the mount. Measured, not assumed: a container session
  refreshes the token and the host's file is updated in place.
- **Shared live, not copied.** That file holds a `refreshToken` rewritten when
  the access token refreshes, so two copies would drift and a container refresh
  could invalidate your host login. One copy cannot.

`~/.claude.json` is deliberately **not** shared. Claude Code keeps it *beside*
the config directory, and that separation is the whole trick: it is one
monolithic JSON holding every project's state, rewritten whole on each change,
so sharing it would be last-writer-wins between host and container. The
container gets its own copy (see the bootstrap, below). For the same reason the
image does **not** set `CLAUDE_CONFIG_DIR` — that would collapse the two back
into one shared directory.

`plugins/` and `backups/` are shadowed back out of the bind with empty volumes,
so a container cannot rewrite plugin state every other session on the host
shares.

**`gh` and `git push` are the ones that need a variable**: `GH_TOKEN`, which
`docker/compose.yaml` forwards from the host only when the host has it set (see
its `environment:` comment for why that distinction is load-bearing). Without
it, push does not work *at all* — the image has no ssh client and the container
has no keys, so this repo's `git@github.com:` remote is unreachable.
`devtools/container-bootstrap.sh` wires `gh`'s credential helper and an
`insteadOf` rewrite into the *container's* global git config, sending that same
remote over HTTPS with the token instead. Your host git config is untouched.

`ANTHROPIC_API_KEY` and `CLAUDE_CODE_OAUTH_TOKEN` are forwarded too, for a host
that has no `~/.claude` to mount (a CI runner): `claude setup-token` produces
the latter. **Export any of them before `up`** — a container's environment is
fixed when it is *created*, so exporting into a shell whose container is already
running changes nothing in there. `down`, then any command, recreates it.

### What the bootstrap does

`devtools/container-bootstrap.sh` runs once per created container — from
`devtools/devcontainer.sh` after `up`, and as the IDE front end's
`postCreateCommand`, so the two cannot drift. Besides the git credentials
above, it seeds `~/.claude.json` from the host's copy. Since credentials come
from the mount, that is not about logging in; it carries onboarding state and
supplies the one thing nothing else can — a minimal `projects["/workspace"]`
trust entry. The host's own entry is keyed by the checkout's *host* path, so
trust cannot carry over on its own, and until it is accepted Claude Code
ignores the whole `permissions.allow` list in `.claude/settings.json`. Two
fixups come with the copy, both explained where they happen in that script.

**One thing the container cannot do, and one trap.** `docker` is not installed
in it, so `devtools/devcontainer.sh` and `docker/compose.yaml` are host-only —
from inside, run the five `cargo` commands of the gate directly. The trap is a
`.claude/worktrees/` checkout created on the **host**: a worktree's `.git` is a
*file* holding the absolute path of its git dir, and a host path resolves to
nothing under `/workspace`, so every git command in there dies with `fatal: not
a git repository`. Create the worktree from *inside* the container and its
`.git` reads `gitdir: /workspace/.git/worktrees/<name>`, which does resolve —
git then works fully in it, `push` and `gh pr create` included, which is how
the PRs so far have been shipped. Which half made a worktree is not visible
from its path, so `cat` that `.git` file when you inherit one.

### Two things that will bite otherwise

- **`HOST_UID`/`HOST_GID` are required, not defaulted**, by
  `docker/compose.yaml`. The wrapper exports the real ones for you; raw
  `docker compose` needs `export HOST_UID=$(id -u) HOST_GID=$(id -g)` (or
  `cp docker/env.example docker/.env`). A default of 1000 on a host where you
  are not uid 1000 silently writes root-owned files into the bind mount.
- **Builds go to `/target` in the container, not `./target`.** That is
  deliberate: sharing one `./target` across a container and a host build has
  them overwrite each other's artifacts. Run `cargo` on the host if you like —
  it gets its own `./target` and the two never meet.

## What the suite is held to

Coverage, mutation and the two weekly lanes are the evidence behind the suite,
and exactly one of them fails a pull request: the per-file coverage floors. The
sections above say what each is and how to run it; this one is the **state of
each on a dated measurement of this tree**, which is what the next pass starts
from, and every figure in it names the command it came from. The milestone that
put them there ran 2026-11-21 to 2026-11-24 and is the sixth with no step
number in [`porting.md`](porting.md).

**Coverage.** `devtools/coverage.sh --functions --check-floors`, 2026-11-24, at
commit be63c15: **97.85% of 17178 lines** (370 missed), 97.53% of 19954 regions
and **91.42% of 2424 function groups**, 208 of which the suite never executed —
**10 of 1267 production groups** and 198 of 1157 in `src/`'s own `#[cfg(test)]`
code. The two further runs of the same tree quoted below --
`--functions --write-floors` and `--check-floors` -- read 372 and 373 missed
lines (97.83% both) and the identical 208, 10 and 198, which is the order of
the spread this page already describes: the line figure moves by a line or two
between runs, the function counts do not move at all. The baseline the lane
started from, for comparison, is the one above: 96.20% of 17035 lines and 253
never-executed groups, measured on 2026-11-21 at 59cf9e4.

The production count is the one a coverage gap can be read off, and its
trajectory across the milestone is **55 → 25 → 10**: 55 of 1267 when #442 split
the listing (5d804e4), 25 once the five dispositions had landed (3d9a290), 10
today — with the test-code count at exactly 198 in all three, which is the split
earning its keep. The ten that remain are each a **recorded exception with an
entry in [`DESIGN.md`](../DESIGN.md)**, and no production function the suite
never executes is an owed test:

- five `assert_send_sync` calls in `const _: () = { ... }` blocks —
  `graph/mod.rs`, `taskflow.rs` and `executor/topology/{job,ownership,run}.rs`
  — whose only possible call is the one the compile already made (#425);
- `AtomicWaiter::lock`'s poisoned-lock arm, which nothing under that lock can
  reach (#430);
- the three `|| {}` on-finish callbacks `Topology::over`'s signature asks for
  over runs whose tear-down no path reaches (#432's `InnerRun::new`, #433's
  `free_standing` and `nothing_to_run`);
- pdqsort's bad-partition fallback, which an adversarial input reaches and a
  case does not (#428).

So a new line in that section of a later listing is new, and that is the
comparison to make rather than a percentage.

**The floors stand at 54, all met**, thinnest headroom **1.64 points** —
`algorithm/for_each_index.rs` against a floor of 95 — on the `--check-floors`
run above. This issue's re-measure raised **nine** of them through
`devtools/coverage.sh --write-floors` and the margin rule above:
`executor/async.rs` 92 to 97, `executor/async_node.rs` 92 to 96,
`executor/mod.rs` 94 to 98, `executor/notifier.rs` 95 to 97,
`executor/queue.rs` 96 to 98, `executor/rng.rs` 92 to 93,
`executor/run_handle.rs` 95 to 97, `executor/subflow.rs` 91 to 93 and
`executor/topology/job.rs` 91 to 96. Those nine are exactly the files the four
mutation runs below added tests to, which is the mutation chain paying the
coverage lane. One floor the same run offered was **not** taken:
`algorithm/partitioner.rs` measured 98.71% in it against the 99.04% its
standing floor of 97 was written from, so `--write-floors` proposed 96, and a
floor that moves down is a floor measured on an unlucky run rather than a tree
that has lost coverage. Floors rise; a `--write-floors` diff is read before it
is committed, and a line that falls in it is dropped.

**Mutation.** `cargo mutants --list`, 2026-11-24, generates **2486** mutants
over `src/`, after the 46 `exclude_re` patterns in `.cargo/mutants.toml` —
`--list` applies the config, so every count here is a count of mutants a run
would actually report. Which files have been mutated, and when:

| Files | Mutants today | Run |
|---|---|---|
| `src/graph/` | 208 | #274, 2026-11-18 |
| `src/semaphore.rs` | 26 | #274, 2026-11-18 |
| `src/algorithm/partitioner.rs` | 189 | #274, 2026-11-18 |
| `src/executor/{notifier,rng,queue}.rs` | 229 | #430, 2026-11-21; re-verified #454, 2026-11-24 |
| `src/executor/{worker,mod,scope,corun,module,dependent_async}.rs` | 203 | #432, 2026-11-21; re-verified #457, 2026-11-23 |
| `src/executor/topology/` and `src/executor/run_handle.rs` | 215 | #433, 2026-11-21 |
| `src/executor/{runtime,runtime_async,async,async_node,subflow}.rs` and `src/task_group.rs` | 284 | #434, 2026-11-23 |

A run's own total is larger than its row, because the exclusions it argued for
are in the count above and were not in its: #430 drove 232 mutants, #432 227,
#433 220 and #434 287, 966 between them, and each of those four numbers belongs
to the issue that measured it rather than to this tree. What the four runs
found is in `DESIGN.md`'s mutation entries, and the summary is that **none of
them found a bug in the port**: every finding was a defect in a test, in an
instrument or in the method, most often a test that asserted less than it
appeared to.

The same `--list`, over the files no run has reached yet, is the map the next
pass starts from — **1132 mutants**, which is 46% of the crate's:

| Files | Mutants | Note |
|---|---|---|
| `src/algorithm/` less `partitioner.rs` | 664 | `sort.rs` is 254 of them, `for_each_index.rs` 98, `merge.rs` 65 |
| `src/observer/` | 164 | `tfprof.rs` 75, `mod.rs` 39, `timeline.rs` 35, `chrome.rs` 15 |
| `src/pipeline/` | 99 | `scheduler.rs` 52, `data_pipeline.rs` 31, `pipe.rs` 16 |
| the handle and builder surface | 205 | `task.rs` 62, `dump.rs` 44, `taskflow.rs` 30, `subflow.rs` 25, `flow_builder.rs` 19, `async_task.rs` 19, `runtime.rs` 6 |

Those four groups and the seven rows above account for all 2486, so the map has
no gap in it. Three of the crate's 55 files under `src/` generate no mutant at
all and are in neither table: `executor/topology/mod.rs`, which is 16 lines of
production code in front of the crate's largest test module, and
`pipeline/mod.rs` and `lib.rs`, which are documentation and re-exports.

**How a run of this kind must be driven is itself a finding** (#433, corrected
and widened by #457), and it is not the default. One job at a time, in a
`CARGO_TARGET_DIR` no other build and no other `cargo mutants` process is
using: at `-j N` under one directory the jobs overwrite each other's rlib and
test binaries, so a mutant can be reported MISSED *or* **CAUGHT** when it is
neither, and a survivor reported caught costs a disposition nobody ever
writes. The test selection goes through `--cargo-arg` rather than
`--cargo-test-arg`, which is the difference between the build phase compiling
the one target and all ninety; survivors are then re-measured unfiltered over
the whole suite; and the verdicts are tallied from `outcomes.json`, not from
stdout, which carries only MISSED, TIMEOUT and UNVIABLE when it is not a tty.
`DESIGN.md`'s entry on it has the evidence for each of those, and
[`.cargo/mutants.toml`](../.cargo/mutants.toml) carries the equivalent mutants
a run must not report.

**Miri, and what is ignored under it: nothing.** `grep -rn 'cfg_attr(miri' src
tests` reports no match on this tree (2026-11-24), and neither step of the lane
can be emptied quietly — the unit-test step fails on an ignored test missing
from the workflow's reason table, and each witness of the lifecycle step must
report exactly one passing case. Between them the two steps cover the three
modules whose discipline the type system is not checking and the four lifecycle
races those disciplines exist for; the division worth remembering is that the
crate's production `unsafe` is `executor/notifier.rs` and `executor/scope.rs`
only, the first reached by the unit-test step and the second by **no** test
module in it, so the witness set is the one thing putting `scope.rs`'s two
lifetime-erasing `transmute`s under the interpreter. The costs are #429's
measurement of 2026-11-21 and are not re-taken here, the lane being weekly CI
on a toolchain pinned apart from the gate's: ten seconds of interpretation over
the four cases, seven and a half of them the cancelled-scope case, some twenty
seconds for the step including the four builds, against a `timeout` of 300
seconds per case. "Getting a toolchain" above has both steps in full, the two
`MIRIFLAGS` and why each is `crossbeam-epoch`'s rather than this crate's.

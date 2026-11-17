---
name: milestone
description: >-
  Two modes over one GitHub milestone. `create` turns a plan — a roadmap/design
  .md, or the current conversation — into a milestone plus a DAG of issues,
  each scoped to a single PR, wired with native "blocked by" dependencies and
  labelled (area + wave-N) so independent issues can be worked in parallel.
  `run` executes an existing milestone end to end: read its issue DAG, fan out
  one subagent per ready issue (each in its own .claude/worktrees/ worktree
  running the cargo gate), merge their PRs one at a time, and launch newly
  unblocked issues until the milestone is done. Use when the user asks to
  "make a milestone", "break this into issues", "plan out the work as issues",
  or to "complete / execute / ship this milestone with subagents". Requires gh
  + GH_TOKEN. Creating issues is outward-facing — `create` drafts and confirms
  first; invoking `run` is the merge grant for that milestone's PRs.
---

# `/milestone create` · `/milestone run` — plan → DAG of issues → merged PRs

Two halves of one milestone's life. `create` does the decomposition; `run`
executes it with subagents. They share the issue body format: the `## Depends
on` lines `create` writes are exactly what `run` reads back as the DAG.

## Picking the mode

- `/milestone create [path/to/plan.md]` → **Part A**. No path means "the plan
  is this conversation".
- `/milestone run <milestone URL|#N|N> [max=N] [dry-run]` → **Part B**.
- **No mode word.** A `.md` path or no argument at all → `create`. A milestone
  URL or number → the user almost certainly means `run`, but running is a
  merge grant, so ask in one line before starting rather than inferring one.

Both halves need `gh` with a token: `gh auth status` first. The container has
`gh` (2.104 at the time of writing, new enough for `--blocked-by`) and gets
`GH_TOKEN` only when the host exported it before the container was created.

---

# Part A — `create`: plan → milestone → DAG of PR-sized issues

Take a plan and turn it into one GitHub milestone whose issues each map to a
single PR, wired into a dependency DAG so the independent ones can run at once.
The hard part is not the `gh` calls — it is the decomposition. Do that well,
show it, then create.

## A1. Get the plan

The input is one of:

- **A file** — `/milestone create path/to/plan.md` (a design doc, proposal, or
  roadmap). Read it in full.
- **The conversation** — no path. Reconstruct the plan from what has been
  discussed and decided in this session. If the discussion is thin or
  ambiguous, say so and ask for the file rather than inventing scope.

Extract two things: the **overall goal** (becomes the milestone) and the
**discrete pieces of work** (become the issues). If the plan is one
undifferentiated wall, your job in A2 is to cut it up; if it already has
sections/steps, use them as the starting seams but re-check their size.

For a port, the upstream header is a seam that already exists: one issue per
upstream file or coherent section of one (`taskflow/core/executor.hpp`'s
worker loop, its topology section, …), named in Scope so the implementer does
not have to rediscover it.

## A2. Decompose into PR-sized issues

Each issue must be addressable by **one coherent, reviewable PR**. Calibrate:

- **Too big** if you cannot state the PR's diff in a sentence or two, if it
  touches unrelated subsystems, or if a reviewer would have to context-switch
  mid-review. Split it.
- **Too small** if it cannot stand as its own commit with its own test/check —
  a one-line tweak that only makes sense bundled with its neighbour. Merge it
  up.
- **Just right**: one bounded change, an independently statable acceptance
  check, a named set of files/modules it will touch. That usually means one
  module and its tests, landing via the `pr` skill as one squash-merged PR.

Write each issue with a consistent body so a future implementer (or subagent)
needs nothing else. Issues #5–#18 of M1/M2 are the template of record:

```
## Scope
<one-PR change, stated as the diff it will produce; for a port, the upstream
file/section it is translated from>

## Depends on
- #<n>   (or: "None — ready to start")

## Acceptance
- <the check that proves it done: a test name, a benchmark, a build>
- Gate green.

## Files / area
<paths or module the PR will touch>
```

**Acceptance is one run against a fixed bar.**

- **Name the bar and where it comes from.** In this repo that is usually a
  named upstream unittest translated into Rust (`unittests/test_basics.cpp`:
  `RunAndWait.Simple`), a property that must hold (a panicking task leaves the
  executor usable), or a `cargo bench` comparison — plus `Gate green`, meaning
  `cargo fmt --all --check`, `cargo clippy --all-targets --all-features --
  -D warnings`, `cargo test --all-features`, `cargo deny check`, all four.
- **Never a repeat count.** Not "N consecutive passes", not "replay N×", not a
  spread measured over repeats — a flaky concurrency test is not made sound by
  running it more often. A loop inside one test (`for _ in 0..1000` over a
  diamond chain) is a *stress case*, which is fine; "run the suite 20 times"
  as evidence is not. For a flake issue, acceptance is a one-run test on the
  captured failing input plus the suite that flaked; rerunning to catch the
  failure belongs in Scope, as diagnosis.
- **Necessary and sufficient.** List checks that others imply under
  "Not gated (implied: …)".

Two repo-specific things to put in Scope where they apply, because they are
review gates in the `rust` subagent's rules and a PR that misses them bounces:
the `//!` **provenance line** on a module translated from upstream (and its
deliberate absence on one we invented), and any **new dependency**, which is a
decision to call out, with `Cargo.lock` committed in the same PR.

## A3. Wire the DAG — maximise what can run in parallel

Add an edge **A → B** only when B genuinely cannot start until A lands — B
imports A's new symbol, builds on a file A creates, or asserts against an
interface A defines. Do **not** add edges for mere preference of ordering; a
spurious edge serialises work that could have run at once.

Then compute **waves** (topological layers): wave 0 = every issue with no
unmet dependency, wave 1 = issues whose deps are all in wave 0, and so on.
Wide-and-shallow beats deep-and-narrow — if everything chains off one root,
look for a smaller shared prerequisite to split out so the rest fan out. The
wave map is what you show the user and what tells them what to start today.

One extra constraint `run` will hit: two issues in the same wave that both
edit `src/lib.rs`'s `mod` list or `Cargo.toml`'s `[dependencies]` conflict on
every merge. That is a list conflict and resolvable, but if two same-wave
issues would rewrite the *same module*, they are one issue or they need an
edge — fix it here rather than leaving `run` to serialise them.

## A4. Draft, then get approval — do not create yet

Creating issues is public and awkward to undo. Present the whole plan first:

- the **milestone** title + one-line description;
- a **wave table** — for each issue: proposed title, its wave, its `depends on`
  set, its labels, and a one-line scope;
- any **labels to be created**, with description and colour;
- call out the **wave-0 set** explicitly: "these N can be started in parallel
  immediately."

Get a clear go-ahead (or edits) before any `gh` write. If the user tweaks the
cut, revise the table and re-confirm.

## A5. Create the milestone

No `gh milestone` subcommand exists — use the API. Capture the number.

```bash
gh api repos/{owner}/{repo}/milestones \
  -f title="<milestone title>" \
  -f description="<goal + the wave map, in markdown>" \
  --jq '.number'
```

Put the wave map in the description (markdown renders on the milestone page) so
the DAG is visible without opening every issue.

## A6. Make sure the labels exist

`gh issue create --label` fails on a label that does not exist, so create the
missing ones first. This repo still has only the stock GitHub set plus
`automerge` — `gh label list --limit 100` to confirm — so expect to create
both kinds:

- **`wave-N`** — exactly one per issue, its wave from A3. None exist yet:

  ```bash
  gh label create wave-0 --description "Milestone wave 0 (no blockers)" --color ededed
  ```

- **Area labels** — one or more per issue, naming the part of the crate it
  touches (`graph`, `executor`, `control-flow`, `bench`, `ci`, `docs`), plus
  the stock `bug` / `enhancement` / `documentation` where they fit. Reuse what
  exists before inventing a near duplicate; a new area label gets a one-line
  description naming its module, e.g. `--description "The executor
  (src/executor)"`.

Create only the labels the approved draft listed. **Never put `automerge` on
an issue** — it is a PR label, and `.github/workflows/automerge.yml` keys off
it to squash-merge; on an issue it is just noise, and on an orchestrated PR it
races `run`'s serial merge (see B5).

## A7. Create the issues in topological order

Create dependencies **before** dependents, so every blocker already has a real
number when its dependent is created. Each issue carries three things that
encode the graph:

- the body's `## Depends on` lines — the **source of truth**, which Part B
  reads and which survives any tooling;
- native **"blocked by"** links (`--blocked-by`), which GitHub shows in the
  issue sidebar as *Blocked by* / *Blocking* — set them on every issue with
  dependencies, matching the body exactly;
- its **labels** (`wave-N` + area).

```bash
gh issue create \
  --title "<title>" \
  --milestone "<milestone title>" \
  --label "wave-1,executor" \
  --blocked-by 12,14 \
  --body "$(cat <<'EOF'
## Scope
...
## Depends on
- #12
- #14
## Acceptance
...
## Files / area
...
EOF
)"
```

Keep a title→number map as you go so later issues can reference earlier ones.
Notes:

- `--milestone` takes the milestone **title**, which must match what A5
  created. It is `-m, --milestone name` — a bare number is not accepted.
- A wave-0 issue has no `--blocked-by` and `None — ready to start` in its
  body.
- `--blocked-by` takes issue numbers or URLs, comma-separated. It needs a
  recent gh: the container's 2.104 has it; a distro gh (Ubuntu 24.04 ships
  2.45) does not. Without it, add each link after creation through the REST
  API, which wants the blocker's **database id**, not its number:

  ```bash
  id=$(gh api repos/{owner}/{repo}/issues/<blocker> --jq .id)
  gh api -X POST repos/{owner}/{repo}/issues/<n>/dependencies/blocked_by \
    -F issue_id="$id"
  ```

- A forgotten or wrong link is fixed in place:
  `gh issue edit <n> --add-blocked-by 12` / `--remove-blocked-by 12`.
- `--parent <n>` makes a **sub-issue** — a parent/child tree, not a
  dependency. Use it only when the relationship really is containment, never
  in place of `--blocked-by`.

Verify the graph before reporting. For every issue, the native list must equal
the body's `Depends on` set:

```bash
gh api repos/{owner}/{repo}/issues/<n>/dependencies/blocked_by --jq '[.[].number]'
```

Fix any mismatch with `gh issue edit`. The M1/M2 issues (#5–#18) predate this
step and have **no** native links and no labels — their bodies are the only
graph they have. Backfill them only if asked; `run` reads the bodies anyway.

## A8. Report

Finish with the milestone URL and a short wave summary: which issues (by number
now) are in each wave, and the explicit **"ready now"** list — the wave-0
issues with no blocker. That is the answer to "what can we work on
simultaneously."

## Undo, if it came out wrong

Issue creation is reversible only by hand, so this is the escape hatch:

```bash
gh issue delete <number> --yes                                   # per issue
gh api -X DELETE repos/{owner}/{repo}/milestones/<number>        # the milestone
```

Deleting the milestone does **not** delete its issues — they just detach.
Remove the issues first if the whole plan is being scrapped. Deleting an issue
drops its blocked-by links with it. Labels from A6 stay behind; delete a new
one (`gh label delete <name> --yes`) only if nothing else uses it.

---

# Part B — `run`: milestone DAG → merged PRs

Part A turns a plan into a DAG of PR-sized issues; this half **executes** one.
You are the **orchestrator**: you schedule, create worktrees, review, merge and
tear down. Subagents implement one issue each, ship a PR with their own gate
run reported in it, and **hand it back without merging**.

**Invoking `/milestone run` is the merge grant** — for PRs that close an issue
in *this* milestone, opened by *this* run's subagents, once the B5 gates are
green. It does not cover any other PR, `--admin`, or merging red. `.claude/
settings.json`'s `autoMode.allow` already covers "merge a PR the agent
authored once CI has passed"; if the classifier still blocks a merge, stop
merging and report the green PR links rather than working around it.

Run it **inside the devcontainer**, from the **main checkout** (`/workspace`).
Everything below is a command from that root.

## Arguments

- **Milestone** — a URL (`…/milestone/<N>`), `#N`, or a bare number.
- **`max=N`** — the most subagents in flight at once. Default **3**. The limit
  is the box, not GitHub: each agent runs the full gate (`cargo test
  --all-features` builds every target), so give each its own target dir and
  job count (B4). On this 24-core container 3 agents at `-j 8` is the
  comfortable shape; past ~6, warn the user that the builds will thrash, then
  honour the number they gave.
- **`dry-run`** — do B1–B2, print the schedule, and stop. No worktree, no
  agent, no `gh` write.

## B1. Read the milestone and rebuild the DAG

```bash
gh api repos/{owner}/{repo}/milestones/<N> --jq '{title,state,open_issues,closed_issues}'
gh issue list --milestone "<title>" --state all --limit 200 \
  --json number,title,state,body,labels
```

`--milestone` wants the **title**, not the number. For each issue, take its
blockers from the `## Depends on` lines Part A writes into every body — that is
this repo's source of truth. Native links, where they exist, count too:

```bash
gh api repos/{owner}/{repo}/issues/<n>/dependencies/blocked_by --jq '.[].number'
```

The M1/M2 issues have none, so an empty list there is not "no blockers" —
read the body.

Then classify every issue:

- **done** — closed.
- **ready** — open, every blocker closed, and no open PR already references it
  (`gh pr list --search "<n> in:body" --state open --json number,headRefName`).
- **resumable** — open, with an open PR whose head branch is `issue-<n>-*`
  *and* a local `.claude/worktrees/issue-<n>-*` checkout. An earlier run died
  or was compacted mid-flight: adopt it. A PR whose body reports a green gate
  goes straight to B5. Otherwise give a fresh agent the existing worktree and
  PR ("finish PR #M") instead of a new branch. An open PR that fails the gate
  and was not opened by this run is someone else's: leave it alone and report
  it.
- **blocked** — some blocker still open. A blocker *outside* the milestone that
  is open is a hard stop for that branch: report it, don't work around it.

A cycle, or an issue with no `Acceptance` section, means the milestone is
malformed: stop and say so rather than guess an order.

## B2. Show the schedule, then start

Print the wave table (issue, title, blockers, state), the ready set, and `max`.
On `dry-run`, stop here. Otherwise start without waiting for a reply — the
invocation was the go-ahead. Stop and ask instead only when B1 found something
odd (cycle, external open blocker, an issue that looks bigger than one PR).

## B3. Pre-flight, once

```bash
gh auth status                      # token present? (the container needs the forwarded one)
gh workflow list --all              # ci and automerge both `active`?
git fetch origin main --quiet
git merge --ff-only origin/main     # local main must not be behind: worktrees fork from it
git worktree list                   # stale worktrees from an earlier run?
```

Then warm the shared build once from the main checkout, so three cold agents
do not each compile the world from nothing:

```bash
cargo fmt --all --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features && cargo deny check
```

A red gate on `main` is a stop: every agent would inherit the failure and
report STUCK. Fix or report that first.

Two environment facts that decide whether `run` can work at all:

- **`ci` must be active.** It runs the gate's first three commands in the same
  image on every PR, and its fourth, `cargo deny check`, as a separate `deny`
  job in the same workflow — unlike repos where CI skips the real build, here a
  green `ci` is real evidence, and both of those jobs have to be green. If `ci`
  is disabled (`gh workflow list --all`), `gh pr checks` reports nothing and
  the only evidence is each agent's own gate run; say so in the final report
  and merge on that.
- **Worktree git must work.** A worktree created *in the container* records a
  container gitdir (`.claude/worktrees/<wt>/.git` reads `gitdir:
  /workspace/.git/worktrees/<wt>`) and git works there, including push. One
  created on the **host** records a host absolute path, and every git command
  inside it dies with `fatal: not a git repository` — the trap the `pr` skill
  warns about. Check the `.git` file of any worktree you adopt; if it names a
  host path, remove and recreate it from here.

## B4. Dispatch loop

While any issue is ready or in flight:

1. **Launch** `min(max − in_flight, |ready|)` ready issues. For each, **you**
   create the worktree, one at a time, so parallel agents never race on
   `git fetch` or the ref lock:

   ```bash
   git fetch origin main --quiet
   git worktree add .claude/worktrees/<wt> -b <wt> origin/main
   git branch --unset-upstream <wt>
   ```

   `<wt>` is `issue-<n>-<slug>`. Don't use the Agent tool's
   `isolation: "worktree"`: it lives elsewhere and tears itself down, which
   removes the checkout you need for a rebase. Also hand each agent:

   - **its own target dir.** `CARGO_TARGET_DIR` is `/target` for everything in
     this container, and cargo takes an exclusive lock on a target directory
     for the whole build — three agents sharing it would serialise behind
     "Blocking waiting for file lock on build directory" and `max` would buy
     nothing. Give each `CARGO_TARGET_DIR=/target/<wt>`. The crate has no
     dependencies yet, so a cold target dir costs seconds, not minutes; the
     volume had ~450 GB free at the time of writing, and B5 deletes the
     directory at teardown.
   - **a job count** `J = nproc / max` (24/3 → `-j 8`), passed as
     `CARGO_BUILD_JOBS=<J>`, so three parallel gates do not each try to use
     every core. When the user gives each agent its own thread count, pass it
     whole.
   - **the files it owns.** Two agents editing the same module conflict on
     every merge. Partition the work so each in-flight agent owns disjoint
     files — different modules, different test files — and say which
     explicitly in its prompt. `src/lib.rs`'s `mod` list and `Cargo.toml`'s
     `[dependencies]` are the unavoidable shared edits: tell each agent to
     touch **only its own line** there, which keeps the conflict a list
     conflict you can resolve (B5). If two ready issues cannot be
     partitioned, launch them in sequence, not together.

   Then spawn a **background `rust` subagent** (`subagent_type: "rust"`) with
   the prompt in "Subagent prompt" below. Its own instructions are already
   this repo's rule set — the gate, the crate-wide lints, the provenance line,
   the dependency policy, the porting posture — so the prompt does not restate
   any of that. What the prompt must do, and does, is lift `rust.md`'s "Stay
   in your lane" ban on committing, pushing and opening PRs, which that file
   waives only for a caller that says so explicitly. Record
   `issue → {wt, agent id, files, target dir}`.

2. **Wait for notifications**; never poll on a short timer, and never give an
   agent a wait loop of its own (see the prompt's rules). When an agent
   reports:
   - **"PR #M ready"** → B5. Merges are serial: if one is in progress, queue
     this one.
   - **stuck** (ambiguous issue, a failing test, would need an `#[allow]` or a
     weakened assertion to go green, scope bigger than one PR) → don't merge.
     Mark the issue **stuck**. Its dependents stay blocked. Keep its worktree.
     Carry on with every other branch of the DAG.

3. **Watch for stragglers.** If an agent runs long, check `gh pr checks <M>`
   yourself. A PR whose checks are done but whose agent is still waiting —
   nudge it with `SendMessage`. Use `TaskStop` on one that has already handed
   back.

**Status line.** After every launch, merge, or stuck report, print one line —
`merged #12 → PR #21 (3/10 done) · launched #13 · in flight: #14 #16 · stuck:
none` — so the user can follow a long run without reading the transcript.

## B5. Review and merge — one PR at a time

Merge **sequentially**, never in parallel and never with `--auto` or the
`automerge` label — you need to see `main` move before deciding the next
merge, and the label's workflow would race you. Before each merge:

```bash
gh pr view <M> --json mergeable,body,files,commits
gh pr checks <M> --watch
```

Check that:

- `mergeable` is `MERGEABLE` and `ci` is green (it is the real gate here);
- the body has `Closes #<n>` and, under *What was run*, the four gate commands
  with their results, plus the acceptance check named in the issue;
- the diff did not buy green with an `#[allow]` that silences one of
  `Cargo.toml`'s crate-wide lints (`missing_docs`,
  `cast_possible_truncation`, `cast_sign_loss`, `undocumented_unsafe_blocks`),
  a deleted or weakened assertion, or an `unsafe` block without a `// SAFETY:`
  comment;
- a module translated from upstream carries its `//!` provenance line, and one
  we invented does **not** (the absence is load-bearing — see the README's
  "Provenance and attribution"); nothing came out of upstream's `3rd-party/`;
- a `Cargo.toml` dependency change comes with `Cargo.lock` in the same PR.

**Rebase onto the fresh main before merging.** `ci` ran against the PR's base,
and a symbol an earlier merge in this run renamed does not show up as a text
conflict — GitHub will happily merge a PR that no longer compiles. So if
`origin/main` has moved past the PR's base (it will have, after any earlier
merge in this run), rebase, push, and let `ci` re-run on the new base:

```bash
git -C .claude/worktrees/<wt> fetch origin main
git -C .claude/worktrees/<wt> rebase origin/main
git -C .claude/worktrees/<wt> push --force-with-lease
```

Here the push *is* the re-check: `ci` reruns the whole gate in a few minutes,
so there is no reason to rerun the full gate locally first — do that only when
the rebase pulled in code this PR builds against (`git log <old
base>..origin/main --stat`) and you want the failure sooner than CI.

**Send the merge and the force-push as bare Bash calls** — `gh pr merge <M>
--squash` on its own, `git -C <wt> push --force-with-lease` on its own, from
the main checkout, with no `cd … &&`, no `;` chain, no trailing `sleep`.
`.claude/settings.json` allows `git` and `gh`, but an allow rule applies only
when *every* command in the call matches one; a chained call falls through to
the auto-mode classifier.

Right after a push the checks are not registered yet and `gh pr checks
--watch` exits at once with "no checks reported": wait a few seconds and
re-run it before reading that as green.

Then merge with the repo's convention:

```bash
gh pr merge <M> --squash
```

`delete_branch_on_merge` is on, so GitHub deletes the remote branch. **Never
pass `--delete-branch` locally**: it attempts a local `git checkout main`,
which fails because `main` is held by the primary checkout.

**Conflicts.** Each merge moves `main`, so a later sibling can still conflict
despite the partition. Two cases:

- **A pure list or registry conflict** — both sides added a `mod` line to
  `src/lib.rs`, an entry to `Cargo.toml`'s `[dependencies]`, a row to a README
  table → resolve it yourself in the PR's worktree during the rebase above,
  keeping both sides, then `cargo fmt --all --check` and the push.
- **Anything else** — module code or a test → send it back to the agent that
  wrote the PR (`SendMessage` resumes it with its context): rebase, resolve,
  rerun the gate, push, re-report.

**After each merge:**

- confirm it merged (`gh pr view <M> --json state` reads `MERGED`) and the
  issue closed (`gh issue view <n> --json state`);
- fast-forward local `main` so the next worktree forks from the right commit:
  `git merge --ff-only origin/main` after a `git fetch origin main`
  (`--ff-only`, not `git update-ref`, which would leave the merged files
  reading as reverted on disk);
- tear the worktree down, and reclaim its target dir:

  ```bash
  git -C .claude/worktrees/<wt> status --porcelain
  git worktree remove .claude/worktrees/<wt>
  git branch -D <wt>
  rm -rf /target/<wt>
  ```

  `-D`, because the squash merge gave `main` a different SHA — safe only
  *after* the merge above confirmed the patches landed. `remove` refuses a
  dirty checkout: if it is dirty, leave it and report it;
- print the status line, recompute **ready**, and go back to B4.1.

## B6. Finish

When nothing is ready or in flight:

- if every issue is closed, close the milestone
  (`gh api -X PATCH repos/{owner}/{repo}/milestones/<N> -f state=closed`);
- report one line per issue: `#n → PR #M merged`, or **stuck** with its
  reason, or **blocked** naming its blocker. Spend words only on what
  deviated: blocked merges, rebases, stuck issues, conflicts you resolved,
  decisions made for the user.

Leave the worktrees of stuck issues standing, and say where they are.

## Subagent prompt

Fill in `<…>`. Keep it self-contained — the agent has none of your context.
`<J>` is the job count from B4.1. `<email>` is `git log -1 --format=%ae
origin/main`, the GitHub noreply address.

```
Resolve GitHub issue #<n> (<title>) in this repo. Read it first:
`gh issue view <n>` — its Scope, Acceptance and Files / area sections are your
spec. Your worktree already exists at /workspace/.claude/worktrees/<wt>, on
branch <wt>, forked from origin/main — EnterWorktree {"path":
".claude/worktrees/<wt>"} into it and do not create another.

Your own instructions are this repo's rule set — the gate, the crate-wide
lints you may not #[allow] away, the `//!` provenance line a translated module
must carry and an invented one must not, the dependency policy, the porting
posture on `tf::Task` handles. Follow them as written, with the one exception
below. Also read README.md's "Provenance and attribution" section, and the
upstream file your issue names.

Rules:
- **Exception to "Stay in your lane":** on this run you DO commit, push and
  open the PR yourself — this is the explicit grant that section defers to,
  and the mechanics are spelled out below.
- Do NOT merge, do NOT add the `automerge` label, and do NOT tear down the
  worktree. Your job ends at an open PR with your own gate run reported in it.
  Then reply exactly: "PR #<M> ready" + one line with the gate result and the
  test counts.
- The gate is the definition of done, and all four parts of it:
    CARGO_TARGET_DIR=/target/<wt> CARGO_BUILD_JOBS=<J> cargo fmt --all --check
    CARGO_TARGET_DIR=/target/<wt> CARGO_BUILD_JOBS=<J> cargo clippy --all-targets --all-features -- -D warnings
    CARGO_TARGET_DIR=/target/<wt> CARGO_BUILD_JOBS=<J> cargo test --all-features
    cargo deny check
  Use that CARGO_TARGET_DIR on every cargo command that builds: siblings are
  building in parallel and cargo locks a target directory for the whole build,
  so the shared default /target would serialise you behind them. An empty
  ./target is normal and is not a sign that nothing built. `cargo deny check`
  builds nothing and needs no target dir; it is the image's pinned binary (ARG
  CARGO_DENY_VERSION in the Dockerfile), so if it is missing say so rather than
  installing one or dropping the step. Read the output; a warning is a failure
  under -D warnings, and partial greens are not green.
- Never silence a lint to go green — no crate- or module-level #[allow], no
  weakened or deleted assertion, no `unsafe` without a `// SAFETY:` comment.
  If passing would need one of those, stop and reply "STUCK: <reason>".
- Acceptance is one run against the bar the issue names. Never add or claim a
  repeat count ("N consecutive passes", "replay N×") as evidence a fix works;
  a stress loop inside one test is fine, rerunning the suite is not. Rerun a
  flaky case only to capture a failing input, and report those runs as
  diagnosis.
- Never wait in a loop. No `until`/`while` + `sleep` poll (on pgrep, on a
  log file, on `gh pr checks`), no background wait. Run a command, read its
  output, move on; if you are genuinely blocked on something outside your
  control, reply STUCK and say what.
- Name every scratch or log file you write <your scratchpad>/issue<n>-… :
  sibling agents share the scratchpad, and a bare gate.log gets overwritten.
- Commit and push with `git -C /workspace/.claude/worktrees/<wt> …`, authored
  as the noreply address (`git -c user.email=<email> commit`), never a
  personal one. Never --no-verify. End the commit message with the
  Co-Authored-By line from your system reminder. Commit Cargo.lock in the same
  commit as any Cargo.toml dependency change.
- Open the PR with `gh pr create --body-file <file in your scratchpad>`, with
  `Closes #<n>`, and under "What was run" the four gate commands with their
  results plus the acceptance check from the issue. On the PR, `ci` runs the
  first three and a separate `deny` job runs the fourth; do not claim a check
  you did not see.
- You own these files and ONLY these: <the agent's partition>. Siblings are
  working in parallel — if you need to change a file outside your partition,
  reply "STUCK: <what>" instead. In `src/lib.rs` and `Cargo.toml`, touch only
  your own line.
- Rebase onto origin/main right before you push, and rerun the gate after the
  rebase only if the new commits touch code you build against — the PR's `ci`
  run is the authoritative re-check.
- If the issue is ambiguous in a way the code can't answer, reply
  "STUCK: <question>" rather than guessing.
```

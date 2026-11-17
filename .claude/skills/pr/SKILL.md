---
name: pr
description: >-
  Ship the current work: commit the relevant changes, push the branch, open a
  GitHub PR with gh, and merge it. Use when the user asks to "PR this", "ship
  it", "commit/push/merge", or otherwise wants the working-tree changes landed
  on main end to end. Requires gh + GH_TOKEN on the HOST; the container image
  has neither, so run every step of this skill from the host.
---

# Commit → push → PR → merge

Drive the current changes all the way to `main` in one pass. Each step has a
gotcha; follow them in order.

## 1. Commit the relevant changes

- **Never commit on `main`.** If the session is on `main`, create a branch
  first (`git switch -c <topic>`). If the session is in a `.claude/worktrees/`
  worktree, the branch already exists — use it.
- Stage **relevant** changes, not everything: read `git status` and `git diff`,
  stage what belongs to this task, and leave unrelated untracked files behind.
  No blanket `git add -A` without looking.
- There is **no pre-commit hook in this repo yet**, so nothing lints on your
  behalf at commit time. `devtools/devcontainer.sh check` is the whole gate.
- If `Cargo.toml` gained or bumped a dependency, commit the updated
  `Cargo.lock` with it. The lock is tracked on purpose (see `.gitignore`) —
  it is what makes a CI run reproducible, and a diff that changes deps without
  it will resolve differently in the container than it did for you.

## 2. Check the repo's own invariants

Before pushing, look at what the diff touches and whether this repo guards it:

- a **ratchet** (a test pinning a measured number or a floor) may only move in
  the improving direction, in its own commit, saying what moved. A loosened
  threshold produces a green run — that is exactly why it needs a human.
- the **reference chain** (whatever independently checks correctness here) is
  not something to adjust to make a test pass.

A repo with neither has nothing to do in this step.

## 3. Push

```bash
git push -u origin <branch>
```

- There is **no pre-push hook in this repo yet**, so a successful push implies
  nothing about correctness. Run the gate yourself first and read its output:

  ```bash
  devtools/devcontainer.sh check   # fmt --check, clippy -D warnings, test
  ```

  Pushing on a red gate is how a branch reaches review broken; if `check`
  fails, fix it before step 3 rather than after review.
- If the worktree's git dir has gone missing (`fatal: not a git repository:
  …/worktrees/<name>`), push from the main checkout instead:
  `git -C <repo-root>/main push origin <branch>`.

## 4. Create the PR

```bash
gh pr create --title "<concise title>" --body "<what and why>"
gh pr edit <number> --add-label automerge    # opt this PR into server-side merge
```

Summarise what changed and why, and note any test implications. The harness's
PR-body footer rules apply.

The `automerge` label is the opt-in for `.github/workflows/automerge.yml`: when
`ci` finishes green on this PR, that workflow squash-merges it server-side and
deletes the branch, with no local permission prompt. Add it on every PR you
intend to land unattended; **leave it off** for the "stop after step 4" cases in
"When NOT to merge" below (CI expected red, a ratchet/reference touch, or the
user asked for a PR but not a merge).

**If `ci.yml` is manually disabled** (check with `gh workflow list --all`),
skip the label: `automerge`
fires on `ci` completing, and a disabled workflow never completes, so a
labelled PR would sit open forever with no CI check to satisfy it. Merge by
hand instead, per step 5 below.

## 5. Merge

If you added the `automerge` label in step 4, **you are done** — the
`automerge` workflow squash-merges the PR the moment `ci` goes green and deletes
the branch, no local action needed. Watch it land with `gh pr checks <number>
--watch` and confirm with `gh pr view <number> --json state`; do not also run
`gh pr merge`, which would race the workflow.

To merge by hand instead (label not applied, or you want it merged now):

```bash
gh pr merge <number> --squash
```

This repo has `delete_branch_on_merge` enabled, so GitHub deletes the remote
branch for you. Do **not** follow up with `git push origin --delete <branch>`:
the ref is already gone, so it fails with `remote ref does not exist` and reads
like a broken merge. Verify with `gh repo view --json deleteBranchOnMerge` if a
branch ever does survive a merge.

- **Wait for CI — unless it's disabled.** `.github/workflows/ci.yml` runs
  `cargo fmt --all --check`, `cargo clippy --all-targets --all-features --
  -D warnings`, then `cargo test --all-features` on every PR — the same three
  commands as `devtools/devcontainer.sh check`, in the same container image, so
  a local green should mean a remote green.
  Check with `gh pr checks <number> --watch` before merging, and do not merge
  red. If `ci.yml` is manually disabled (`gh workflow list --all`), there is no
  check to wait for — `gh pr checks` will show nothing — so merge on the
  strength of the local gates alone and say so.
- **Never pass `--delete-branch` to a local `gh pr merge`**: it attempts a local
  `git checkout main`, which fails when `main` is held by another worktree. You
  do not need it anyway -- the repo setting above handles the remote branch.
  (The `automerge` *workflow* does pass `--delete-branch`, which is safe there —
  it runs server-side with no local checkout to disturb.)
- If GitHub reports the PR as not yet mergeable, wait a moment and re-check
  with `gh pr view <number> --json mergeable,mergeStateStatus` before retrying
  — do not force.

## 6. After the merge

**First confirm the PR actually merged** — `gh pr view <number> --json state`
must read `MERGED`. For a labelled PR that means *waiting* for the `automerge`
workflow, which lands it a short while **after** CI goes green; green CI alone is
not "merged". Everything below assumes `origin/main` has already advanced, so
doing it before the merge lands is a no-op at best.

`origin/main` has moved but local `main` has not — and `git worktree add` forks
from local HEAD, so a stale `main` silently seeds stale branches:

```bash
git -C <main-checkout> merge --ff-only origin/main
```

`--ff-only` is the point: it refuses rather than merging if that `main` has
diverged, which is the signal you wanted. Use it — **not**
`git update-ref refs/heads/main origin/main`: `update-ref` moves the
branch pointer without touching the checked-out index or working tree, so the
just-merged files read as *reverted* on disk until a `git reset --hard HEAD`,
and a careless commit then un-does the merge.

If the work happened in a `.claude/worktrees/` worktree, whether to tear it
down depends on how this skill was invoked:

- **Bare `/pr`** — leave the worktree standing. The work is merged, but the
  checkout stays for follow-up commits; do not remove it unless asked.
- **`/pr full`** — reclaim it now, per the `worktree` skill's "`/pr full` —
  ship, then tear the worktree down" section: exit with
  `ExitWorktree {"action": "keep"}` if the session is inside it, then
  `git worktree remove` + `git branch -d <name>` (`-D` after a squash merge).

Otherwise — a plain topic branch on an ordinary checkout — the local branch can
go with `git branch -d <branch>` (or `-D` after a squash merge, since the SHAs
differ; the merge already confirmed the patches landed).

## When NOT to merge

Stop after step 4 and hand the PR to the user instead of merging when:

- CI is red, or the push gate was bypassed with `--no-verify`;
- the diff moves a ratchet floor or touches the correctness references in ways
  only a human should sign off on;
- the user asked for a PR but not a merge.

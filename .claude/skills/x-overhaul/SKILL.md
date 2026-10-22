---
name: x-overhaul
description: Review an MMDB scope, resolve confirmed in-scope findings, commit, and open a PR. Default is the latest commit; pass all for the full repository. One local release if src/ changed.
argument-hint: "[N | all | staged | worktree | <rev> | <rev1>..<rev2>]"
disable-model-invocation: true
allowed-tools: Bash(git add *) Bash(git commit *) Bash(git tag -a *) Bash(git branch --show-current) Bash(git switch -c *) Bash(date *) Bash(git push -u origin x-overhaul/*) Bash(git push origin x-overhaul/*) Bash(gh pr create *)
---

# MMDB Review-Fix-Commit Pipeline

Review the scope, record dispositions, fix in-scope Open, then one local
release if `src/` changed. All commits go on a non-default working branch.
After the local workflow completes, push that branch and open a pull request
as a best-effort step. Never push the default branch; never force-push or
rewrite history.

Do not run `/x-fix` or `/x-review` as nested workflows. Read the sections of
their skill files cited below and follow only those. Their headers (read-only,
no commit, no bump), exit, and release rules do not apply here.

## Invocation state

Captured when this skill loaded. This is the preflight baseline
(`workflow-policy.md` §1) and the ledger's start `HEAD`.

!`git status --short --branch`

HEAD: !`git rev-parse HEAD`

## Input

Parse the user argument with the scope table in `workflow-policy.md`. Empty is
the latest commit, same as `/x-review`. The full repository is `all`. Reject
`--fix` and any other extra token.

Unless the scope is `all`, fix only findings rooted in that diff, re-review
only files this run changed, and leave other Open entries untouched.

## Setup

Read first:
[workflow-policy.md](../../docs/workflow-policy.md),
[commit-protocol.md](../../docs/commit-protocol.md),
[review-core.md](../../docs/review-core.md),
[pragmatic-engineering.md](../../docs/pragmatic-engineering.md),
[technical-patterns.md](../../docs/technical-patterns.md),
[false-positive-guide.md](../../docs/false-positive-guide.md).
Design-shaped or multi-subsystem → also
[design-patterns.md](../../docs/design-patterns.md).

Preflight and ledger before any edit.

## Working branch

Create the branch after the preflight record and before any edit, including
the `docs/audit.md` registry update.

1. Resolve the default branch: `git rev-parse --abbrev-ref origin/HEAD`, then
   strip the `origin/` prefix. If that ref is absent, use `master`, then
   `main`.
2. If the current branch is the default branch, create and switch to
   `x-overhaul/<scope>-<UTC stamp>`:
   `git switch -c x-overhaul/<scope>-$(date -u +%Y%m%d-%H%M%S)`.
   Use `latest` for empty input and a sanitized scope token otherwise. If the
   name already exists, append `-2`, `-3`, ... until creation succeeds.
3. Otherwise keep the current branch as the working branch.

Record the working branch. If branch creation still fails, stop before any
edit and report; do not fall back to committing on the default branch. Never
commit, tag, or release on the default branch.

## 1. Review

Follow [x-review](../x-review/SKILL.md) Protocol §1–§5 (Scope through
Registry). Do not commit yet. `all` re-checks every Won't Fix entry. A narrow
scope must not drop an unrelated Open entry unless the code shows the defect is
gone.

## 2. Resolve

Triage and fix in-scope Open with [x-fix](../x-fix/SKILL.md) Protocol §1–§2:
severity order, one root cause per commit, sequential edits, targeted tests. A
finding ends as a fix, Won't Fix, or Rejected. Do not relabel Open to clear the
list. No release and no final gate in this phase.

## 3. Gate, release, and best-effort PR

The cargo gate in `commit-protocol.md` once if behavior changed. Docs-only →
skip. Then the release step in `commit-protocol.md`, once. Nothing to commit →
no empty commit, bump, tag, push, or PR.

When this invocation created at least one commit, attempt the remote step.
It is optional: a push or PR failure does not fail the local overhaul. Report
the failure and keep the local commits.

1. Confirm `git branch --show-current` is the recorded working branch and is
   not the default branch. If not, skip the remote step and report it.
2. Push the literal branch name: `git push -u origin <working-branch>`.
   Never push the default branch, a tag, `HEAD`, `--all`, or `--mirror`; never
   use a force flag or a refspec with a destination (`src:dst`).
3. Open the pull request against the default branch:
   `gh pr create --base <default-branch> --head <working-branch> --title ... --body ...`.
   Title: `x-overhaul: <scope>`, plus ` (v<version>)` when a release was made.
   Body: scope, disposition counts, fixed findings (IDs and one-line
   descriptions), validation commands/results, version/tag when present, and
   the `docs/audit.md` reference. Follow `workflow-policy.md` wording.
4. Report the PR URL, or the exact failing command and error. Do not retry by
   pushing another ref, and do not treat the failure as an overhaul failure.

## Output

Scope, coverage, dispositions, validations, hashes and subjects, version and
tag if any, the working branch, PR URL or best-effort failure, and the
baseline left alone.

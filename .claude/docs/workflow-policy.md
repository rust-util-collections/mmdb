# Workflow Safety and Atomic Commit Policy

This is the canonical safety policy for `/x-review`, `/x-commit`, `/x-fix`, and
`/x-overhaul`. Skills reference it instead of duplicating or weakening it.

## 1. Preflight

Before any mutation or commit:

1. Record `git status --short`, the current branch, and `HEAD`.
2. Account separately for staged, unstaged, and untracked baseline changes.
3. Stop if a merge, rebase, or cherry-pick is in progress, or if `HEAD` is
   detached, unless the user explicitly resolves that state.
4. Define the files/hunks owned by this invocation. Existing unrelated changes
   remain owned by their author.

Do not require a clean worktree. Do require a clean ownership boundary.

## 2. Preserve existing work

- Never use `git stash`, `git clean`, `git checkout --`, `git restore`, or
  destructive `git reset` to manufacture a clean tree.
- Never revert, overwrite, stage, or commit unrelated baseline changes.
- If a required fix overlaps an existing change and cannot be separated
  confidently, stop and report the conflict.
- Review agents are read-only. Parallel work is allowed only for independent
  investigation or validation; edits and commits in one working tree are
  sequential.

## 3. Atomic commit units

One independent issue, root cause, or behavior change gets one commit.

- Include the tests, public docs, and audit-registry update required to prove
  that unit.
- Multiple symptoms may share a commit only when they have the same root cause.
- Do not mix unrelated cleanup, formatting churn, or opportunistic refactors
  into a fix.
- Stage exact paths or hunks, never `git add -A`. Inspect
  `git diff --cached` immediately before every commit.
- Create new commits only. Never amend, rebase, reset published history,
  filter history, or force-push.
- These workflows create local commits only; they never push any remote.

## 4. Validation and failure handling

- Run the smallest relevant validation before each commit unit.
- Validation against a dirty worktree covers everything currently present. If
  unrelated or later units could affect the result, validate `HEAD` plus only
  the candidate unit in a disposable worktree (or equivalent isolated copy).
  Never use a stash to build that isolation, and remove the temporary worktree
  afterward.
- Run the repository-wide gate once after the final behavior-affecting change.
- A failure caused by the unit must be fixed before committing it.
- A demonstrably pre-existing failure is reported with evidence; it is never
  silently presented as success.
- If the same failure repeats without new evidence or progress, stop and report
  the blocker instead of looping indefinitely.

## 5. Audit dispositions

- `Open`: confirmed, actionable defect.
- `Won't Fix`: confirmed defect or concrete technical debt whose safe fix is
  currently disproportionate.
- `Rejected`: investigated, recurring/material claim disproven by current code;
  it is not a severity. Routine discarded hypotheses do not belong in the
  registry.

Audit entries describe current evidence only. Never add dates, timestamps, or
"last reviewed" markers.

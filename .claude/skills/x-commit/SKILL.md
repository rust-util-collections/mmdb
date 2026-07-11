---
name: x-commit
description: Review, fix, validate, and commit MMDB worktree changes as atomic commits. Use only when the user explicitly invokes /x-commit.
disable-model-invocation: true
---

# Self-Reviewing Commit for MMDB

Review all intended worktree changes, fix confirmed defects, validate them, and
create local commits. Never push.

## Setup

1. Read `.claude/docs/workflow-policy.md`.
2. Read `.claude/docs/commit-protocol.md`.
3. Read `.claude/docs/review-core.md`,
   `.claude/docs/technical-patterns.md`, and
   `.claude/docs/false-positive-guide.md`.
4. Run the workflow-policy preflight and record the invocation ledger required
   by the commit protocol.

## Protocol

### 1. Establish scope

1. Read `git status --short`, staged and unstaged diffs, and every intended
   untracked file. `git diff HEAD` alone is insufficient because it omits
   untracked files.
2. If no intended changes exist, report "nothing to commit" and stop.
3. Separate the worktree into coherent commit units before editing:
   - one independent issue, root cause, or behavior change per unit;
   - required tests, docs, and audit-registry updates stay with that unit;
   - preserve pre-staged boundaries unless the user explicitly asks to change
     them.
4. If unrelated changes overlap the same hunk and cannot be separated safely,
   stop rather than stash, revert, or absorb them.
5. When several pre-existing units coexist, do not claim that a validation run
   against the combined worktree proves each commit independently. Use the
   disposable-worktree procedure in `workflow-policy.md` when isolation matters.

### 2. Review and fix

For each unit:

1. Map changed files through the Subsystem Map and load the relevant guides.
2. Read complete functions, callers, error paths, and tests.
3. Check invariants, boundaries, crash safety, concurrency, unsafe contracts,
   performance hot paths, and public API compatibility.
4. Refute candidates using the false-positive guide; retain only concrete
   defects.
5. Fix every retained defect completely and add focused regression coverage.
6. Re-review the resulting unit until no confirmed finding remains. If the same
   failure repeats without new evidence or progress, stop and report the
   blocker instead of looping.

Read-only investigation may run in parallel. All edits and commits are
sequential.

### 3. Validate and commit each unit

Apply the per-unit procedure in `.claude/docs/commit-protocol.md`: format,
lint, run targeted tests, stage exact paths/hunks, inspect the cached diff, and
create one new commit. Never amend an earlier commit.

### 4. Final gate and version

After all behavior commits, execute the commit protocol's final repository gate
and single version-bump policy. Any regression found after a commit is fixed in
a new focused commit, never by rewriting history.

## Output

Report reviewed files/subsystems, findings fixed, validations run, every commit
hash/subject, the version result, and any baseline changes intentionally left
untouched.

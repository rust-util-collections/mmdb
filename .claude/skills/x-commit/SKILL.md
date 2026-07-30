---
name: x-commit
description: Review, fix, validate, and commit MMDB worktree changes as atomic commits. Use only when the user explicitly invokes /x-commit.
disable-model-invocation: true
---

# Self-Reviewing Commit for MMDB

Review owned worktree changes → fix confirmed defects → validate → local commits.
Never push. User-invoked only. New commits only (no amend/rebase/force-push).

## Setup

Read `workflow-policy.md`, `commit-protocol.md`, `pragmatic-engineering.md`,
`review-core.md`, `technical-patterns.md`, `false-positive-guide.md`;
design-shaped → `design-patterns.md`. Preflight + ledger (incl. **frozen paths**).

## Protocol

### 1. Scope

1. `git status --short`, full diffs, intended untracked (`git diff HEAD` misses untracked).
2. Nothing intended → “nothing to commit”, stop.
3. Freeze owned paths before edit; later stage freeze + this-invocation fix/format only.
4. Split coherent units: one issue/root cause/behavior each; tests/docs/audit stay with unit;
   keep pre-staged boundaries unless user changes them.
5. Unrelated same-hunk overlap → stop (no stash/revert/absorb).
6. Multi-unit tree: combined validation ≠ per-unit proof — disposable worktree when isolation matters.

### 2. Review and fix (per unit)

Map guides → full functions/callers/errors/tests → invariants, crash, concurrency,
unsafe, quantified hot-path perf, design (if any), placeholders, public API →
refute via FP guide → fix completely + regression → re-review until clean. No-progress
loop → stop and report.

Investigate parallel OK; edit/commit sequential.

### 3. Validate and commit

`commit-protocol.md` per unit: format, lint, targeted tests, exact stage, inspect
cached diff, one new commit. Never amend.

### 4. Final gate, version, tag

After behavior commits: full gate + single version-bump-and-tag policy. Post-commit
regression → new focused commit, no history rewrite.

## Output

Files/subsystems, fixes, validations, hashes/subjects, version/tag, untouched baseline.

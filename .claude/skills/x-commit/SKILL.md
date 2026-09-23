---
name: x-commit
description: Check MMDB worktree changes for correctness, fix confirmed issues, validate, and create atomic local commits. Does not bump the version or tag. Use only when the user explicitly invokes /x-commit.
disable-model-invocation: true
---

# Self-Reviewing Commit for MMDB

Review owned worktree changes, fix confirmed defects, validate, and commit.
Never push, bump, or tag. User-invoked only. New commits only.

## Setup

[workflow-policy.md](../../docs/workflow-policy.md) and
[commit-protocol.md](../../docs/commit-protocol.md). Evidence rules:
[review-core.md](../../docs/review-core.md).

Also read `pragmatic-engineering.md`, `technical-patterns.md`, and
`false-positive-guide.md`. Design-shaped → `design-patterns.md`. Preflight,
then freeze owned paths.

## Protocol

### 1. Scope

1. `git status --short`, full diffs, and intended untracked files. `git diff HEAD` misses untracked files.
2. Nothing intended → "nothing to commit", then stop.
3. Freeze owned paths before edits. Later, stage only that set plus this invocation's fix and format paths.
4. Split units by issue, root cause, or behavior. Tests, docs, and audit stay with the unit. Keep a pre-staged boundary unless the user changes it.
5. Unrelated overlap in the same hunk → stop. No stash, revert, or absorb.
6. If unrelated dirty files would change the result, stop and report.

### 2. Review and fix (per unit)

Read the full functions, callers, errors, and tests. Check invariants, crash,
concurrency, unsafe, quantified hot-path cost, design shape, placeholders, and
the public API. Apply `false-positive-guide.md`. Fix the unit completely and
add a regression. Re-review until clean. No progress → stop and report. Do not
edit an unowned caller; report it.

Reads may be parallel. Edits and commits are sequential.

### 3. Validate and commit

Follow `commit-protocol.md` for each unit. Never amend.

### 4. Gate

After the last behavior commit, run the cargo gate in `commit-protocol.md` once. Docs-only → skip. A later
regression is a new commit, then re-run the gate. No version bump or tag.

## Output

Files and subsystems, fixes, validations, hashes and subjects, and the
untouched baseline.

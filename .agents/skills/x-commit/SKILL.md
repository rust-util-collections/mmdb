---
name: x-commit
description: Explicitly invoke $x-commit to review intended MMDB worktree changes, fix confirmed issues, validate and commit locally without a version bump.
---

# Self-Reviewing Commit for MMDB

MMDB is an in-process embedded Rust LSM key-value library operating on
caller-owned local files (`vsdb` backend; no network, daemon, or IPC service).

Resolve repository paths from the Git root; Markdown links are relative to
this file. Read invocation arguments from the user prompt.

Review owned worktree changes, fix confirmed defects, validate, and create
atomic local commits. Never push, bump, or tag. New commits only.

## Preflight

Run `git status --short --branch` and `git rev-parse HEAD` explicitly before
review or edits. Record the staged, unstaged, and untracked baseline, branch,
and starting `HEAD` for `workflow-policy.md` §1 and the invocation ledger.

## Setup

Read first:
[workflow-policy.md](../../../docs/ai/workflow-policy.md),
[commit-protocol.md](../../../docs/ai/commit-protocol.md),
[review-core.md](../../../docs/ai/review-core.md) (evidence rules),
[pragmatic-engineering.md](../../../docs/ai/pragmatic-engineering.md),
[technical-patterns.md](../../../docs/ai/technical-patterns.md),
[false-positive-guide.md](../../../docs/ai/false-positive-guide.md).
Design-shaped → also [design-patterns.md](../../../docs/ai/design-patterns.md).

Preflight, then freeze owned paths before editing.

## Protocol

### 1. Scope

1. Inspect `git status --short`, full diffs, and user-intended untracked
   source/test files and explicitly requested untracked documentation (`git diff HEAD` misses untracked files). Leave untracked
   design drafts or scratch notes (such as `docs/proposals/*`) untouched unless
   the user asked to commit them.
2. Nothing intended → report "nothing to commit" and stop.
3. Freeze owned paths before edits. Stage only that set plus this invocation's
   in-unit fix and rustfmt paths.
4. Split units by independent issue, root cause, or behavior change; keep
   matching tests, docs, and any resolved `docs/audit.md` hunk with the unit.
   Preserve a pre-staged boundary unless the user asks to reorganize it.
5. Unrelated baseline overlap in the same hunk, or unstaged edits on a staged
   file that invalidate worktree Cargo checks → stop and report without
   `stash`, `restore`, or absorbing unowned lines.

### 2. Review and fix (per unit)

For documentation and skill changes, use the [documentation map](../../../docs/ai/review-core.md#documentation-map)
and the [per-unit validation rules](../../../docs/ai/commit-protocol.md#per-unit-validate-and-commit).
For code, trace the complete changed operation and error or cleanup path.
Select the relevant [v5 contract checks](../../../docs/ai/review-core.md#v5-contract-checks)
and subsystem guide. A change in a benchmark driver or validator needs
verification against its input, marker for the measured path and output metric.
Fix confirmed defects in the owned unit and cover behavior changes with a
focused regression test. Report a baseline overlap before editing unowned lines.

Reads may run in parallel; edits and commits are strictly sequential.

### 3. Validate and commit

Follow `commit-protocol.md` per-unit validation and staging for each unit.
Never amend prior commits.

### 4. Gate

After the last behavior commit, run the Final repository gate in
`commit-protocol.md` once (skip for docs-only commits). Fix any gate regression
in a new atomic commit and re-run the gate. No version bump or tag.

## Output

Report committed files and subsystems, defects fixed during review, executed
validation commands, new commit hashes and subjects, and untouched baseline
paths.

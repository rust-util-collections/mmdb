---
name: x-overhaul
description: Review an MMDB scope, resolve confirmed in-scope findings, and commit. Default is the latest commit; pass all for the full repository. One local release if src/ changed.
argument-hint: "[N | all | staged | worktree | <rev> | <rev1>..<rev2>]"
disable-model-invocation: true
allowed-tools: Bash(git add *) Bash(git commit *) Bash(git tag -a *)
---

# MMDB Review-Fix-Commit Pipeline

Review the scope, record dispositions, fix in-scope Open, then one local
release if `src/` changed. Never push. New commits only.

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

## 3. Gate and release

The cargo gate in `commit-protocol.md` once if behavior changed. Docs-only →
skip. Then the release step in `commit-protocol.md`, once. Nothing to commit →
no empty commit, bump, or tag.

## Output

Scope, coverage, dispositions, validations, hashes and subjects, version and
tag if any, and the baseline left alone.

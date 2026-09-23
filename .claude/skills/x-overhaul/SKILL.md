---
name: x-overhaul
description: Review an MMDB scope, resolve confirmed in-scope findings, and commit. Default is the latest commit; pass all for the full repository. One local release if src/ changed. Use only when the user explicitly invokes /x-overhaul.
argument-hint: "[N | all | staged | worktree | <rev> | <rev1>..<rev2>]"
disable-model-invocation: true
---

# MMDB Review-Fix-Commit Pipeline

Review the scope, record dispositions, fix in-scope Open, then one local
release if `src/` changed. Never push. User-invoked only. New commits only.

Do not run `/x-fix` or `/x-review` as nested workflows. Use their evidence and
per-finding commit rules only. Their exit and release rules do not apply.

## Input

Parse the user argument with the scope table in `workflow-policy.md`. Empty is
the latest commit, same as `/x-review`. The full repository is `all`. Reject
`--fix` and any other extra token.

Unless the scope is `all`, fix only findings rooted in that diff, re-review
only files this run changed, and leave other Open entries untouched.

## Setup

[workflow-policy.md](../../docs/workflow-policy.md),
[commit-protocol.md](../../docs/commit-protocol.md),
[review-core.md](../../docs/review-core.md), and `pragmatic-engineering.md`.
Preflight and ledger before any edit.

## 1. Review

Same evidence and registry rules as `/x-review`. Do not commit yet. `all`
re-checks every Won't Fix entry. A narrow scope must not drop an unrelated Open
entry unless the code shows the defect is gone.

## 2. Resolve

Triage and fix in-scope Open with the `/x-fix` per-finding rules: severity
order, one root cause per commit, sequential edits, targeted tests. A finding
ends as a fix, Won't Fix, or Rejected. Do not relabel Open to clear the list.
No release and no final gate in this phase.

## 3. Gate and release

The cargo gate in `commit-protocol.md` once if behavior changed. Docs-only → skip. Then the release step in
`commit-protocol.md`, once. Nothing to commit → no empty commit, bump, or tag.

## Output

Scope, coverage, dispositions, validations, hashes and subjects, version and
tag if any, and the baseline left alone.

---
name: x-overhaul
description: Review an MMDB scope, resolve confirmed in-scope findings, and create atomic local commits. Default is the latest commit; pass all for the full repository. One local release if src/ changed. Never push.
---

# MMDB Review-Fix-Commit Pipeline

Resolve repository paths from the Git root; Markdown links are relative to
this file. Read invocation arguments from the user prompt.

Review the scope, record dispositions, fix in-scope Open, then one local
release if `src/` changed. Keep all work local. Never push, create a pull
request, force-push or rewrite history.

Do not run `$x-fix` or `$x-review` as nested workflows. Read the sections of
their skill files cited below and follow only those. Their headers (read-only,
no commit, no bump), exit, and release rules do not apply here.

## Preflight

Run `git status --short --branch` and `git rev-parse HEAD` explicitly before
review or edits. Record the staged, unstaged and untracked baseline, branch and
starting HEAD for `workflow-policy.md` §1 and the invocation ledger.

## Input

Parse the user argument with the scope table in `workflow-policy.md`. Empty is
the latest commit, same as `$x-review`. The full repository is `all`. Reject
`--fix` and any other extra token.

Unless the scope is `all`, fix only findings rooted in that diff, re-review
only files this run changed, and leave other Open entries untouched.

## Setup

Read first:
[workflow-policy.md](../../../docs/ai/workflow-policy.md),
[commit-protocol.md](../../../docs/ai/commit-protocol.md),
[review-core.md](../../../docs/ai/review-core.md),
[pragmatic-engineering.md](../../../docs/ai/pragmatic-engineering.md),
[technical-patterns.md](../../../docs/ai/technical-patterns.md),
[false-positive-guide.md](../../../docs/ai/false-positive-guide.md).
Design-shaped or multi-subsystem → also
[design-patterns.md](../../../docs/ai/design-patterns.md).

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

## 3. Gate and local release

The cargo gate in `commit-protocol.md` once if behavior changed. Docs-only →
skip. Then the local release step in `commit-protocol.md`, once. Nothing to
commit → no empty commit, bump or tag.

Preserve validated local commits if a later unit or check is blocked, and
report what remains. No remote step belongs to this workflow.

## Output

Scope, coverage, dispositions, validations, hashes and subjects, local version
and tag if any, remaining findings, and the baseline left alone.

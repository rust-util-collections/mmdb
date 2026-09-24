---
name: x-review
description: Review correctness, recovery, and resource behavior in an MMDB scope and update docs/audit.md. Default is the latest commit.
argument-hint: "[N | all | staged | worktree | <rev> | <rev1>..<rev2>]"
disable-model-invocation: true
---

# MMDB Storage-Engine Reliability Review

High-signal review. Code is read-only. May update only `docs/audit.md`. Never
commit, push, bump, or tag. Fixes belong to `/x-fix` or `/x-overhaul`.

## Invocation state

Captured when this skill loaded. This is the worktree baseline
(`workflow-policy.md` §1).

!`git status --short --branch`

HEAD: !`git rev-parse HEAD`

## Setup

Read first:
[workflow-policy.md](../../docs/workflow-policy.md),
[pragmatic-engineering.md](../../docs/pragmatic-engineering.md),
[technical-patterns.md](../../docs/technical-patterns.md),
[review-core.md](../../docs/review-core.md),
[false-positive-guide.md](../../docs/false-positive-guide.md).
Design-shaped or multi-subsystem → also
[design-patterns.md](../../docs/design-patterns.md).

Task/report wording follows `workflow-policy.md`. Review agents use the
handoff in `review-core.md`.

## Input

Parse the user argument with the scope table in `workflow-policy.md`. Reject
anything else, including `--fix`.

## Protocol

### 1. Scope

1. Worktree baseline: the invocation state above.
2. Changed files, full diff, callers, tests. `worktree` includes untracked
   (`git status --short`). `all` → ledger of `src/`, `tests/`, `benches/`,
   build/CI, public docs, `.claude/`.
3. Map via the Subsystem Map. Load concurrency/unsafe guides when relevant.
4. Mark generated, vendored, and out-of-scope rows. Do not silent-drop them.

### 2. Evidence

Small single-subsystem → review directly. Use a read-only agent only when a
fresh context split helps. `all`: disjoint batches, each Rust file one owner.
fmt/compile/clippy are tools, not agents.

Cover what the diff actually touches: correctness, crash, concurrency, unsafe,
design shape, public API, quantified hot-path cost.

Each candidate needs a location, invariant, concrete conditions, expected and
observed results, existing checks, a minimal correction, and a regression test.
Drop style, speculation, feature requests, documented contracts, and anything
an existing check already covers.

### 3. Verify

Re-read the cited code. Keep only code-demonstrable items. One extra reader
only if the result is still ambiguous. Agreement is not proof. Merge one root
cause into one finding.

### 4. Completeness

Diff scope: every changed file, public contract, failure path, and relevant
test. `all`: compare the ledger with the results; review only uncovered files
or invariants. No rework.

### 5. Registry

Update `docs/audit.md` from current code, using the forms in `review-core.md`.

1. Prune fixed or obsolete in-scope Open.
2. Add confirmed Open. Dedupe. Sort CRITICAL → LOW.
3. Re-check intersecting Won't Fix (`all` → all). A stale reason becomes Open
   or Rejected. Do not add Won't Fix.
4. A material disproven claim becomes Rejected. Drop noise and non-defects.
5. No dates.

### 6. Report

Scope, coverage, and findings: severity, location, conditions, expected and
observed results, correction. Zero findings → say so, and say what was covered.

---
name: x-fix
description: Resolve Open findings in docs/audit.md sequentially, one validated local commit each. Does not bump the version or tag.
disable-model-invocation: true
---

# Resolve MMDB Reliability Findings

Clear actionable `docs/audit.md` Open, then self-review and commit. Never push,
bump, or tag. New commits only.

## Invocation state

Captured when this skill loaded. This is the preflight baseline
(`workflow-policy.md` §1) and the ledger's start `HEAD`.

!`git status --short --branch`

HEAD: !`git rev-parse HEAD`

## Setup

Read first:
[workflow-policy.md](../../docs/workflow-policy.md),
[commit-protocol.md](../../docs/commit-protocol.md),
[review-core.md](../../docs/review-core.md),
[pragmatic-engineering.md](../../docs/pragmatic-engineering.md),
[technical-patterns.md](../../docs/technical-patterns.md),
[false-positive-guide.md](../../docs/false-positive-guide.md).

Preflight and freeze paths before editing. Empty Open → "nothing to fix".

## Protocol

### 1. Triage (CRITICAL → LOW)

Before editing, read the code, callers, tests, and guides
([design-patterns.md](../../docs/design-patterns.md) if the shape is a design
issue). Reproduce from current code. Dedupe root causes.

- Ruled out → Rejected.
- Confirmed, but a complete fix has disproportionate cost or regression risk →
  Won't Fix + reason.
- Feature request or documented contract → delete the entry. Do not keep it as
  Won't Fix.
- Do not reclassify an entry just to empty Open.

### 2. One finding → one commit

1. Root-cause fix and a focused regression.
2. Trace the error, crash, concurrency, and cleanup paths.
3. Drop that Open entry in the same commit.
4. Per-unit validation in `commit-protocol.md`.
5. Stage the freeze set plus this fix. Inspect the cached diff. Commit before
   the next finding.

A registry-only disposition is one unit. Batch several of those into one commit
at the end. The same root cause may batch symptoms. Edits and commits are
sequential. Reads and tests may be parallel.

### 3. Self-review

Review `starting_HEAD..HEAD` and the remaining owned worktree with the evidence
and verify rules in [x-review](../x-review/SKILL.md) §2–§3. Do not reopen
unrelated Open. A new confirmed defect in this range goes back through the same
loop. Stop on no progress or on overlap with the baseline.

### 4. Gate

The cargo gate in `commit-protocol.md` once after the last behavior commit.
Docs-only → skip. A remaining Open entry is a valid result; report the blocker.
No version bump or tag.

## Output

Dispositions, fixes, Rejected and Won't Fix, validations, hashes and subjects,
and the baseline left alone.

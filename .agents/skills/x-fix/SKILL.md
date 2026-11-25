---
name: x-fix
description: Resolve Open findings in docs/audit.md sequentially, one validated local commit each. Does not bump the version or tag.
---

# Resolve MMDB Reliability Findings

MMDB is an in-process embedded Rust LSM key-value library operating on
caller-owned local files (`vsdb` backend; no network, daemon, or IPC service).

Resolve repository paths from the Git root; Markdown links are relative to
this file. Read invocation arguments from the user prompt.

Clear actionable `docs/audit.md` `Open` entries sequentially, self-review, and
create atomic local commits. Never push, bump, or tag. New commits only.

## Preflight

Run `git status --short --branch` and `git rev-parse HEAD` explicitly before
review or edits. Record the staged, unstaged, and untracked baseline, branch,
and starting `HEAD` for `workflow-policy.md` §1 and the invocation ledger.

## Setup

Read first:
[workflow-policy.md](../../../docs/ai/workflow-policy.md),
[commit-protocol.md](../../../docs/ai/commit-protocol.md),
[review-core.md](../../../docs/ai/review-core.md),
[pragmatic-engineering.md](../../../docs/ai/pragmatic-engineering.md),
[technical-patterns.md](../../../docs/ai/technical-patterns.md),
[false-positive-guide.md](../../../docs/ai/false-positive-guide.md).

Preflight and freeze owned paths before editing. Empty `Open` → report
"nothing to fix" and stop.

## Protocol

### 1. Triage (CRITICAL → LOW)

Before editing, read the full implementation, callers, tests, and mapped guides
([design-patterns.md](../../../docs/ai/design-patterns.md) when design-shaped).
Verify each finding against current code and dedupe shared root causes:

- Ruled out by code or tests → `Rejected`.
- Confirmed defect whose complete fix has disproportionate cost or regression
  risk → `Won't Fix` + concrete reason.
- Feature request or documented contract → delete the entry (never `Won't Fix`).
- Do not reclassify a valid defect just to empty `Open`.

### 2. One finding → one commit

1. For a behavior defect, implement the root-cause fix and a focused regression
   test in a temporary DB. For a documentation defect, correct the owning page
   in the [documentation map](../../../docs/ai/review-core.md#documentation-map)
   and verify its claim against the relevant code or test.
2. Trace typed `ErrorKind` propagation, post-commit acknowledgement boundaries,
   interrupted-write recovery, lock/claim/fail-stop publication order, and
   resource cleanup paths; check sibling twins (`forward`/`reverse`,
   `partitions`/`range_partitions`, `Snapshot`/`ArcSnapshot`, `WalAsync`/`WalSync`/`DisableWal`).
3. Remove that finding's `Open` entry in the same commit.
4. Run per-unit validation in `commit-protocol.md`.
5. Stage only the unit's owned fix/test/doc/audit paths, inspect
   `git diff --cached`, and commit before starting the next finding.

Batch registry-only dispositions (`Rejected`, `Won't Fix`, non-defect removals)
into one separate documentation commit after code fixes. Multiple symptoms of
the same root cause share one commit. Edits and commits are sequential;
read-only checks may run in parallel.

### 3. Self-review

Review `starting_HEAD..HEAD` and any remaining owned worktree changes with the
evidence and verification rules in [x-review](../x-review/SKILL.md) §2–§3. Do
not reopen unrelated entries. Route any newly introduced defect in this range
through Step 2 as a new commit. Stop on no progress or baseline overlap.

### 4. Gate

Run the Final repository gate in `commit-protocol.md` once after the last
behavior commit (skip for docs-only commits). Remaining blocked `Open` entries
are valid outcomes — report the blocker clearly. No version bump or tag.

## Output

Report dispositions, root-cause fixes, `Rejected` and `Won't Fix` updates,
executed validation commands, new commit hashes and subjects, and untouched
baseline paths.

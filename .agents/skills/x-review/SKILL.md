---
name: x-review
description: Explicitly invoke $x-review to inspect an MMDB scope and update docs/audit.md with verified findings. Default scope is the latest commit.
---

# MMDB Storage-Engine Reliability Review

MMDB is an in-process embedded Rust LSM key-value library operating on
caller-owned local files (`vsdb` backend; no network, daemon, or IPC service).
Review data-structure, MVCC, persistence, and concurrency invariants.

Resolve repository paths from the Git root; Markdown links are relative to
this file. Read invocation arguments from the user prompt.

High-signal review. Source code is read-only; update only `docs/audit.md`.
Never commit, push, bump, or tag. Fixes belong to `$x-fix` or `$x-overhaul`.

## Preflight

Run `git status --short --branch` and `git rev-parse HEAD` explicitly before
review or edits. Record the staged, unstaged, and untracked baseline, branch,
and starting `HEAD` for `workflow-policy.md` §1 and the invocation ledger.

## Setup

Read first:
[workflow-policy.md](../../../docs/ai/workflow-policy.md),
[review-core.md](../../../docs/ai/review-core.md),
[false-positive-guide.md](../../../docs/ai/false-positive-guide.md).
Design-shaped or multi-subsystem → also
[design-patterns.md](../../../docs/ai/design-patterns.md).

## Input

Parse the user argument with the scope table in `workflow-policy.md`. Reject
anything else, including `--fix`.

## Protocol

### 1. Scope

1. Worktree baseline: the recorded preflight above.
2. Changed files, full diff, callers, tests. `worktree` includes intended
   untracked source/test files and explicitly requested untracked documentation (`git status --short`). For `all`,
   use `git ls-files` and the [documentation and subsystem maps](../../../docs/ai/review-core.md#documentation-map).
3. Use the [subsystem map](../../../docs/ai/review-core.md#subsystem-map)
   and [v5 contract checks](../../../docs/ai/review-core.md#v5-contract-checks)
   to locate the current implementation, callers and tests. Load the
   concurrency or `unsafe` guide when the path crosses those boundaries.
4. Mark generated, vendored, and out-of-scope rows explicitly in the ledger.

### 2. Evidence

Small or single-subsystem scope → review directly in the current session.
Delegate read-only subagents only when splitting large context helps (`all` or
multi-subsystem diffs), using the
[scoped handoff](../../../docs/ai/review-core.md#agent-handoff-and-response)
with concrete storage-invariant questions. For `all`, assign disjoint subsystem
batches; large files (`src/db.rs`, `src/compaction/leveled.rs`,
`src/sst/table_reader/{mod.rs,iterator.rs}`, `src/sst/table_builder.rs`,
`src/iterator/db_iter.rs`, `src/manifest/version_set.rs`) may be split into
disjoint line-range sections so every line has one owner in the ledger.
`fmt`, compile, and `clippy` are Cargo tools, not subagent tasks.

Cover what the scope touches: key/value ordering, MVCC visibility,
interrupted-write recovery, thread synchronization, `unsafe` preconditions,
design shape, public API contract, and quantified hot-path cost.

For a measured performance claim, trace the operation actually exercised,
including the benchmark feature configuration, and consult the [profile
evidence](../../../docs/ai/review-core.md#documentation-map).

Each candidate needs a location, invariant, concrete inputs/ordering, expected
and observed results, existing checks, a minimal correction, and a regression
test. For a documentation discrepancy, cite the claim and its verifiable
source using the documentation map. Drop style, speculation, feature requests, documented contracts, and
anything an existing check already guarantees.

### 3. Verify

Re-read the cited code and callers. Keep only code-demonstrable defects. Use at
most one extra reader if a path remains genuinely ambiguous; agreement alone is
not proof. Merge symptoms of one root cause into one finding.

### 4. Completeness

Diff scope: every changed file, public contract, error/recovery path, and
relevant test. `all`: compare the ledger with reviewed files/sections and
complete any uncovered items before updating the registry.

### 5. Registry

Update `docs/audit.md` from current code, using the forms in `review-core.md`:

1. Prune fixed or obsolete in-scope `Open` entries.
2. Add confirmed `Open` entries, deduped and sorted `CRITICAL` → `LOW`.
3. Re-check intersecting `Won't Fix` entries (`all` → all). Move stale entries
   to `Open` or `Rejected` as appropriate. Add a new `Won't Fix` only for a
   confirmed defect confined to unsupported architectures, citing the target
   and the [architecture policy](../../../docs/ai/workflow-policy.md#supported-architectures).
4. Record material disproven claims as `Rejected`; omit routine noise and
   non-defects.
5. No dates.

### 6. Report

Report scope, ledger coverage, and findings (severity, location, concrete
conditions, expected vs observed results, minimal correction). Zero findings →
state that explicitly along with the files and invariants covered.

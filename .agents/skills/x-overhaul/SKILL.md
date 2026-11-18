---
name: x-overhaul
description: Review scoped changes in MMDB, fix confirmed in-scope defects, and create atomic local commits. Invoke explicitly as $x-overhaul; empty scope is the latest commit and all selects the full repository.
---

# MMDB Scoped Review, Fix, and Local Commit

MMDB is an in-process embedded Rust LSM key-value library operating on
caller-owned local files (`vsdb` backend; no network, daemon, or IPC service).
This workflow checks data-structure, MVCC, persistence, and concurrency
invariants, fixes confirmed in-scope defects as sequential atomic local commits,
and finalizes at most one eligible patch version bump with a local version tag.

Resolve repository paths from the Git root; Markdown links below are relative
to this file. Read invocation arguments from user prompt text. Never create a
branch, push, open a PR, publish, or rewrite history.

## Preflight

Run `git status --short --branch` and `git rev-parse HEAD` explicitly before
reviewing or editing. Record the branch, starting `HEAD`, package version in
`Cargo.toml` (at `HEAD` and in the worktree), and the staged, unstaged, and
untracked baseline for the invocation ledger in
[commit-protocol.md](../../../docs/ai/commit-protocol.md#invocation-ledger).

## Setup

Read first:
[workflow-policy.md](../../../docs/ai/workflow-policy.md),
[commit-protocol.md](../../../docs/ai/commit-protocol.md),
[review-core.md](../../../docs/ai/review-core.md),
[pragmatic-engineering.md](../../../docs/ai/pragmatic-engineering.md),
[technical-patterns.md](../../../docs/ai/technical-patterns.md),
[false-positive-guide.md](../../../docs/ai/false-positive-guide.md).
Multi-subsystem or design-shaped scope → also
[design-patterns.md](../../../docs/ai/design-patterns.md).

Compose the cited sections of `x-review`, `x-commit`, and `x-fix` under this
single invocation ledger; do not launch them as nested standalone workflows or
apply their standalone read-only, final-gate, or no-version rules.

## Input

Parse one optional scope with the table in
[workflow-policy.md](../../../docs/ai/workflow-policy.md#scope) (`empty` =
latest commit; `N`; `staged`; `worktree`; `all` = full repository; `<rev>`;
`<rev1>..<rev2>`). Reject `--fix`, multiple scopes, and unknown tokens.

## Protocol

### 1. Scope and ledger

1. Freeze resolved commits and owned paths before any edit.
2. `staged`: owns pre-staged paths/hunks only. Because `cargo` checks compile
   and test the working tree rather than the Git index, stop and report before
   editing if any staged file also has unstaged changes that affect validation.
3. `worktree`: owns tracked staged/unstaged changes and only user-intended
   untracked source/test files. Leave untracked design drafts or scratch notes
   (such as `docs/proposals/*`) untouched unless the user asked to include them.
4. Committed scopes (`empty`, `N`, `<rev>`, `<rev1>..<rev2>`, `all`): leave all
   baseline worktree changes uncommitted and untouched; stop and report if an
   in-scope fix overlaps dirty baseline lines.
5. `all`: build a complete tracked-file ledger across `src/`, `tests/`,
   `benches/`, `examples/`, `Cargo.toml`/`Makefile`/`.github/`, public docs
   (`README.md`, `COMPARISON.md`), `.agents/`, and `docs/ai/`. Large files
   (`src/db.rs`, `src/compaction/leveled.rs`) may be partitioned into disjoint
   line-range sections so each section has a single review owner in the ledger.

### 2. Review (or commit owned worktree)

- **`staged` / `worktree`**: follow [x-commit](../x-commit/SKILL.md) Protocol
  §1–§3 directly on the frozen units (review full functions/callers/tests, fix
  confirmed defects in place with focused regressions, run per-unit validation,
  and create sequential atomic commits while preserving staged/unstaged
  boundaries). Skip `x-commit` §4 here; run the repository gate once in Step 4.
  Do not write fixable findings into `docs/audit.md` ahead of fixing them.
- **Committed scopes**: execute [x-review](../x-review/SKILL.md) Protocol §1–§4
  (`Scope`, `Evidence`, `Verify`, `Completeness`), holding `docs/audit.md`
  updates until Step 3. Review small or single-subsystem diffs directly in the
  current session. Delegate read-only subagents only when splitting large
  context helps (`all` or multi-subsystem ranges), using the
  [scoped handoff](../../../docs/ai/review-core.md#agent-handoff-and-response)
  with concrete storage-invariant questions while keeping the coverage ledger
  in the coordinating session.

### 3. Resolve in-scope findings

1. Historical diff scopes fix only defects rooted in that diff that remain
   present at `HEAD` (callers supply evidence, not unrelated fix scope), plus
   any pre-existing `docs/audit.md` `Open` entry directly inside the frozen
   scope. `all` resolves all confirmed new and existing `Open` entries and
   re-checks every `Won't Fix` entry.
2. Follow [x-fix](../x-fix/SKILL.md) Protocol §1–§2 sequentially (`CRITICAL` →
   `LOW`): one root cause per commit with its focused regression test, matching
   doc updates, and removal of any pre-existing `Open` entry for that issue,
   validated per [commit-protocol.md](../../../docs/ai/commit-protocol.md#per-unit-validate-and-commit).
3. Do not write newly discovered fixable issues to `docs/audit.md` only to
   remove them in the same run. Keep blocked or unaccepted breaking findings
   `Open` without relabeling them to clear the list. Write `docs/audit.md` in
   one separate documentation commit after code fixes only when needed for
   unresolved `Open` blockers, `Won't Fix` / material `Rejected` dispositions,
   or stale-entry cleanup.

### 4. Aggregate check, final gate, and version tag

1. If commits were added (`starting_HEAD..HEAD`), review the aggregate diff
   once ([x-fix](../x-fix/SKILL.md) Protocol §3) for cross-commit interactions.
2. Run the [Final repository gate](../../../docs/ai/commit-protocol.md#final-repository-gate)
   once when Rust or build files changed, when the scope is `all`, or before
   committing an eligible version bump. Reuse passing results when the tree
   state has not changed. Report pre-existing out-of-scope failures separately
   from regressions introduced by this run.
3. Apply [Version and tag](../../../docs/ai/commit-protocol.md#version-and-tag)
   once only when all required checks pass with no in-scope `Open` findings, no
   unreviewed ledger gaps, and no dirty baseline in `Cargo.toml`. Never accept
   a public API or on-disk format break or choose a major bump on the user's
   behalf. No changes and no eligible version bump → no empty commit.

## Output

Report the resolved scope, ledger coverage and any remaining gaps, finding
dispositions, executed validation commands (including any optional host tool not
present locally), new commit hashes and subjects, version and tag outcomes,
and preserved baseline paths. Handle failed or incomplete steps per
[workflow-policy.md](../../../docs/ai/workflow-policy.md#4-validation-and-failure),
keeping validated local commits intact.

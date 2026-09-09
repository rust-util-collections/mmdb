---
name: x-overhaul
description: Review MMDB storage-engine correctness (full repo or scoped range), resolve confirmed findings, and create atomic local commits. Use only when the user explicitly invokes /x-overhaul.
argument-hint: "[N | all | staged | worktree | <hash> | <hash1>..<hash2>]"
disable-model-invocation: true
---

# MMDB Review-Fix-Commit Pipeline

Review scope → record a disposition for every confirmed finding → fix actionable items → local commits.
Unlike `/x-review`, always fixes and commits (no `--fix`). Never push.
User-invoked only. New commits only.

## Input

`$ARGUMENTS` — same scopes as `/x-review` without `--fix`:

| Input | Scope |
|-------|-------|
| *(empty)* or `all` | Full repo (default) |
| `N` / `staged` / `worktree` / hash / range | Diff-bound |

Non-full: fix only findings rooted in that diff. Post-fix re-review = files
this run changed.

## Setup

Review local embedded-storage behavior using the neutral task/report language
in [workflow-policy.md](../../docs/workflow-policy.md). Any review agents use
the scoped handoff and evidence templates in
[review-core.md](../../docs/review-core.md).

Preflight (`workflow-policy.md`); `pragmatic-engineering.md`; read `x-review` +
`x-fix` skills + `commit-protocol.md`; ledger before mutations.

## Phase 1 — Review

`/x-review <scope>` without `--fix`:

1. Coverage: full ledger (`all`) or diff+callers.
2. Read-only agents with disjoint ownership when needed, using the shared handoff template; `all` → each Rust file once in depth.
3. Cross-subsystem / design / completeness only for depth gaps.
4. Verify + dedupe.
5. Update `docs/audit.md` (`all` re-evals all sections; narrow scopes prune/merge
   in-scope without dropping unrelated Open unless proven fixed). No timestamps.
6. If registry changed → docs-only inventory commit before fixes (may list many findings).

## Phase 2 — Resolve

Full `/x-fix` on Phase-1 (and still-applicable Open) findings: severity order;
complete validated fix or evidence-backed Won't Fix/Rejected; one root cause per commit; mutations
sequential; re-review changed files only. Correctness > open-count cosmetics.
Out-of-scope Open untouched.

## Phase 3 — Gate, version, tag

Final gate; regressions in new commits. Rust changed → one patch bump + separate
release commit + annotated tag. Nothing changed → no empty commit/bump/tag.

## Output

Scope, coverage, dispositions, validations, hashes/subjects, version/tag, baseline left alone.

---
name: x-review
description: Review correctness, recovery, and resource behavior in MMDB changes or the full repository. Use only when the user explicitly invokes /x-review.
argument-hint: "[N | all | staged | worktree | <hash> | <hash1>..<hash2>] [--fix]"
disable-model-invocation: true
---

# MMDB Storage-Engine Reliability Review

High-signal review. Code read-only unless `--fix`; may update only
`docs/audit.md`. Never commit or push. User-invoked only.

## Setup

Review local embedded-storage behavior using the neutral task/report language
in [workflow-policy.md](../../docs/workflow-policy.md). Any review agents use
the scoped handoff and evidence templates in
[review-core.md](../../docs/review-core.md).

Read: `workflow-policy.md`, `pragmatic-engineering.md`, `technical-patterns.md`,
`review-core.md` (Subsystem Map), `false-positive-guide.md`. Design-shaped /
multi-subsystem → also `design-patterns.md`.

## Input

`$ARGUMENTS` — one optional scope + optional `--fix`:

| Input | Scope |
|-------|-------|
| *(empty)* | Latest commit |
| `N` | Last N commits (positive int) |
| `staged` | `git diff --cached` |
| `worktree` | Staged + unstaged + untracked |
| `all` | Full repo |
| `<hash>` | One commit |
| `<hash1>..<hash2>` | Range |

Validate revs with Git. Reject bad args; never guess. `--fix`: apply confirmed
fixes after report. Historical scope: only still-present HEAD defects.

## Protocol

### Phase 1 — Scope

1. Worktree baseline (`workflow-policy.md`).
2. Changed files + full diff + callers/tests. `worktree` includes untracked
   (`git status --short`). `all` → ledger: `src/`, `tests/`, `benches/`,
   build/CI, public docs, `.claude/`.
3. Map via Subsystem Map; load guides (concurrency/unsafe when relevant).
4. Mark generated/vendored/out-of-scope in the ledger — do not silent-drop.

### Phase 2 — Evidence

Small single-subsystem → review direct. Agents only if context split helps
(read-only; fresh scoped context + shared handoff template + applicable guides).

Non-trivial dimensions (minimum sufficient):

- correctness / invariants
- crash / concurrency / unsafe
- design shape if locks/resources/bounds/install/failure/API (`design-patterns.md`)
- API / quantified perf / placeholders (`review-core.md`)

`all`: disjoint subsystem batches (each Rust file one owner); cross-subsystem +
design only for gaps. fmt/compile/clippy → tools, not agents.

Each candidate: location + invariant · concrete conditions · expected/observed
results · existing checks · minimal correction + test. Drop style-only notes,
speculation, and candidates already covered by existing checks.

### Phase 3 — Verify

The parent re-reads the relevant code and checks whether existing guards or
caller constraints already explain the result. Use one independent verifier
only if still ambiguous. Agreement is not proof. Keep only code-demonstrable
items; merge findings with the same root cause.

### Phase 4 — Completeness

Diff: every changed file, public contract, failure path, relevant test.
`all`: ledger vs depth results; additional review only for uncovered files/invariants. No rework.

### Phase 5 — Findings registry

Update `docs/audit.md` from current code:

1. Prune fixed/obsolete in-scope Open.
2. Add confirmed Open, dedupe, CRITICAL→LOW.
3. Re-check intersecting Won't Fix (`all` → all).
4. Disproportionate real → Won't Fix + Reason.
5. Material disproven → Rejected (no severity); drop routine noise; re-check only
   if cited code/invariant changed.
6. No dates/freshness markers.

```markdown
## Open
### [SEVERITY] subsystem: summary
- **Where**: file:line_range
- **What**: expected behavior and observed difference
- **Why**: concrete conditions, invariant, and existing checks
- **Suggested fix**: direction

## Won't Fix
### [SEVERITY] subsystem: summary
- **Where** / **What** / **Reason**

## Rejected
### subsystem: claim
- **Where** / **Claim** / **Reason**
```

### Phase 6 — Report

Scope, coverage, findings (severity, location, conditions, expected/observed results, correction). Zero → say so
+ what was covered.

### Phase 7 — `--fix` only

Sequential fixes; preserve baseline; stop when overlap cannot be separated from existing work. Regression tests +
smallest validate per fix; re-review; update audit. No version/commit/push —
user runs `/x-commit` after inspect.

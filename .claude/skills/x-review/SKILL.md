---
name: x-review
description: Deep regression review of MMDB changes or the full repository. Use only when the user explicitly invokes /x-review.
argument-hint: "[N | all | staged | worktree | <hash> | <hash1>..<hash2>] [--fix]"
disable-model-invocation: true
---

# Deep Regression Review for MMDB

Review MMDB changes with high-signal, evidence-based analysis. Source code stays
read-only unless the user supplied `--fix`; the normal workflow may update only
`docs/audit.md`. It never commits or pushes.

## Setup

1. Read `.claude/docs/workflow-policy.md`.
2. Read `.claude/docs/technical-patterns.md`.
3. Read `.claude/docs/review-core.md` and use its Subsystem Map as the canonical
   file-to-guide mapping.
4. Read `.claude/docs/false-positive-guide.md`.

## Input

Arguments: `$ARGUMENTS`

Accept exactly one optional scope plus an optional `--fix`:

| Input | Scope |
|-------|-------|
| *(empty)* | Latest commit |
| `N` | Last N commits; N must be a positive integer |
| `staged` | Staged changes (`git diff --cached`) |
| `worktree` | All staged, unstaged, and untracked worktree changes |
| `all` | Full repository audit |
| `<hash>` | One commit |
| `<hash1>..<hash2>` | Exact commit range |

Validate revisions with Git before reviewing. Reject unknown, ambiguous, or
extra arguments with the usage string; never guess the intended range.

`--fix` means apply confirmed fixes to the current worktree after reporting.
For historical scopes, first verify that each candidate still exists at the
current `HEAD`; never apply or register a bug that is already fixed.

## Protocol

### Phase 1: Scope and coverage

1. Record the pre-existing worktree baseline required by
   `workflow-policy.md`.
2. Build the changed-file list and read the complete diff plus surrounding
   implementation, callers, and relevant tests.
   - For `worktree`, include untracked files from `git status --short`; Git
     diffs do not show them.
   - For `all`, build a tracked-file ledger from `src/`, `tests/`, `benches/`,
     build/CI configuration, public documentation, and `.claude/`.
3. Map every code file to the Subsystem Map and load every mapped pattern
   guide. Apply cross-cutting concurrency and unsafe guides where relevant.
4. Mark generated, vendored, ignored, or explicitly out-of-scope files in the
   ledger instead of silently omitting them.

### Phase 2: Evidence collection

For a small, single-subsystem diff, review directly. Use agents only when
separate context materially improves coverage:

- Agents are read-only and receive an exact scope, relevant guides, and the
  high-signal rule.
- For a non-trivial diff, use no more dimensions than needed: correctness and
  invariants; crash/concurrency/unsafe; API/performance/error paths.
- For `all`, partition the depth pass into disjoint subsystem batches. Every
  tracked Rust source file must have one owner. Run a later cross-subsystem pass
  only for interactions that a file-local pass cannot establish.
- Do not delegate compiler, formatter, or Clippy diagnostics to LLM agents.
  Deterministic tools own those checks.

Every candidate finding must include:

1. exact location and affected invariant;
2. a concrete, realistic trigger;
3. the incorrect observable outcome;
4. the existing guard or protocol that was checked and why it is insufficient;
5. a minimal fix direction and appropriate regression test.

Discard style preferences, unsupported speculation, and findings already
covered by `.claude/docs/false-positive-guide.md`.

### Phase 3: Adversarial verification

The orchestrator re-reads every candidate with full context and actively tries
to refute it. Use one independent read-only verifier only when the control flow
or invariant remains genuinely ambiguous; agent majority voting is not proof.

A finding survives only when its trigger and outcome can be demonstrated from
the current code. Deduplicate multiple symptoms with one root cause.

### Phase 4: Completeness

For diff scopes, account for every changed file, changed public contract,
failure path, and relevant test. For `all`, reconcile the file ledger against
the depth-pass results and run a focused completeness critic over uncovered
files or invariants only. Do not repeat already-owned review work.

### Phase 5: Audit registry

Update `docs/audit.md` from current-code evidence:

1. Prune fixed or obsolete `## Open` entries in scope.
2. Add confirmed actionable findings to `## Open`, deduplicated and sorted
   CRITICAL → HIGH → MEDIUM → LOW.
3. Re-evaluate `## Won't Fix` entries whose files, callers, assumptions, or
   subsystem intersect this review. In `all` mode, re-evaluate every entry.
4. Keep disproportional but real findings under `## Won't Fix` with a concrete
   `**Reason**`.
5. Put disproven claims under `## Rejected`; these are not findings and have no
   severity. Record only an existing or plausibly recurring claim with useful
   counter-evidence; discard routine refuted candidates instead of bloating the
   registry. Re-check recorded claims only when their cited code or invariant
   changed.
6. Never add dates, timestamps, "last reviewed", or similar freshness markers.

Use this shape:

```markdown
## Open

### [SEVERITY] subsystem: summary
- **Where**: file:line_range
- **What**: concrete defect
- **Why**: trigger, outcome, and violated invariant
- **Suggested fix**: minimal safe direction

## Won't Fix

### [SEVERITY] subsystem: summary
- **Where**: file:line_range
- **What**: concrete defect
- **Reason**: why fixing it is currently disproportionate or unsafe

## Rejected

### subsystem: rejected claim
- **Where**: file:line_range
- **Claim**: what was alleged
- **Reason**: evidence showing why it is not a bug
```

### Phase 6: Report

Report scope, covered subsystems/invariants, and confirmed findings. For each
finding, include severity, location, trigger, outcome, and fix direction. If
none survive, state that plainly and list the meaningful coverage performed.

### Phase 7: Fix (`--fix` only)

1. Apply confirmed fixes sequentially; never run mutating agents in parallel.
2. Preserve baseline changes and stop on unsafe overlap.
3. Add focused regression coverage and run the smallest relevant validation
   after each fix.
4. Re-review the changed code and update `docs/audit.md`.
5. Do not bump the version, commit, amend, or push. The user can invoke
   `/x-commit` after inspecting the resulting worktree.

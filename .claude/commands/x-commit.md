---
description: Self-reviewing commit — review uncommitted changes, fix all findings, validate, commit
---

# Self-Reviewing Commit for MMDB

You are performing a self-reviewing commit: review all uncommitted changes, fix every issue found, validate, and commit.

## Setup

1. **MANDATORY**: Read `.claude/docs/technical-patterns.md` — bug pattern reference.
2. Read `.claude/docs/review-core.md` — review methodology and the canonical **Subsystem Map**.
3. Read `.claude/docs/false-positive-guide.md` — consult before reporting any finding.
4. Read `.claude/docs/commit-protocol.md` — validate/bump/commit procedure.

## Execution Protocol

### Task 1: Deep Self-Review

1. Run `git diff HEAD` to collect all uncommitted changes.
2. If the diff is empty, report "nothing to commit" and stop.
3. Map changed files to subsystems via the Subsystem Map; read the pattern guide(s) for each affected subsystem.
4. Perform the full regression analysis from review-core.md:
   - **Classify** each change (Phase 2)
   - **Invariants, boundary conditions, failure paths, concurrency** (Phase 3)
   - **Crash safety, performance, API compatibility** (Phases 4.1–4.3)
   - **Code style rules** (Phase 4.4)
5. Audit any added/modified `unsafe` blocks per `.claude/docs/patterns/unsafe-audit.md`.
6. Cross-reference every finding with `false-positive-guide.md` — retain only findings with **concrete evidence**.

### Task 2: Fix All Findings

For EVERY finding from Task 1 (CRITICAL through LOW):

1. Fix the issue completely — no TODOs, no "fix later", no partial fixes.
2. After all fixes, re-run `git diff HEAD` and repeat Task 1 analysis on the new diff.
3. If new findings emerge, fix those too. Iterate until clean.
4. Report the final list of fixes applied.

### Task 3: Validate, Bump, Commit

Execute `.claude/docs/commit-protocol.md` in full:
1. **Validate** (fmt → lint → scoped tests) — mandatory, never skip.
2. **Bump patch version** if any `.rs` file changed.
3. **Commit** (specific files, HEREDOC message, no co-author line, verify with `git status`).

## Output Format

```
## Self-Review Commit Summary

**Reviewed**: <number of files changed>
**Subsystems**: <list>
**Findings**: <N found, N fixed> (or "0 — clean")
**Validation**: fmt ✓ | lint ✓ | tests <which suites> ✓
**Commit**: <short hash> <subject line>
```

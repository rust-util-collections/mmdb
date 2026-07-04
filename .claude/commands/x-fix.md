---
description: Fix audit backlog — resolve every open finding in docs/audit.md, self-review, commit
---

# Fix Audit Backlog

You are resolving every open finding in `docs/audit.md`, then self-reviewing and committing the result.

**How this differs from `/x-commit`:**
- `/x-commit` = "I've made changes — review them and commit." (starts from uncommitted diff)
- `/x-fix` = "Work through the audit backlog — fix, verify, commit." (starts from `docs/audit.md`)

## Setup

1. **MANDATORY**: Read `.claude/docs/technical-patterns.md` — bug pattern reference.
2. Read `.claude/docs/review-core.md` — review methodology and the canonical **Subsystem Map**.
3. Read `.claude/docs/false-positive-guide.md` — consult before reporting any finding.
4. Read `.claude/docs/commit-protocol.md` — validate/bump/commit procedure.
5. Read `docs/audit.md` — this is your **primary work list**.

## Phase 1: Fix

### Task 1: Triage

1. Read `docs/audit.md`. If no `## Open` entries exist, report "nothing to fix" and stop.
2. Sort open findings by severity: CRITICAL → HIGH → MEDIUM → LOW.
3. For each finding, read the code at the reported location with full context (100+ lines).
4. For each affected subsystem, read its pattern guide(s) per the Subsystem Map.

### Task 2: Fix

For each open finding, in severity order:

1. **Understand** the root cause — read the code, trace call sites, identify the violated invariant.
2. **Implement** a complete fix that:
   - Fully resolves the finding — not a band-aid, not a workaround
   - Introduces no new issues (check boundary conditions, error paths, concurrency)
   - Respects crash safety (WAL durability, fsync ordering), lock ordering, and MVCC invariants
   - Follows existing code style and conventions
3. **Verify** the fix by reading the modified code and tracing its effects.
4. If the finding **cannot be fixed** (technical limitation, disproportionate risk, architectural redesign required), move it to `## Won't Fix` with a clear `**Reason**`.

### Task 3: Update Audit Registry

1. Remove all fixed entries from `## Open`.
2. Add `**Reason**` to entries moved to `## Won't Fix`.
3. Write the updated `docs/audit.md`.
   **NEVER include timestamps, dates, "Last review", "Last sweep", "Last cleared",
   or any time-based markers in the file.** Dates bias future reviews toward
   shallowness ("it was just reviewed, I can skip"). The file must carry
   zero information about *when* it was last touched.

## Phase 2: Self-Review

1. Run `git diff HEAD` to see all changes from audit fixes.
2. If the diff is empty, report "nothing to commit" and stop.
3. Execute the `/x-review` Execution Protocol on the diff — invariant checks, concurrency analysis, crash safety, unsafe audit.
4. Cross-reference every finding with `false-positive-guide.md`.
5. If the review produces **new findings**: fix them immediately, update `docs/audit.md`, and repeat until `## Open` is empty (or only Won't Fix remains).

## Phase 3: Validate, Bump, Commit

Execute `.claude/docs/commit-protocol.md` in full:
1. **Validate** (fmt → lint → scoped tests) — audit fixes span subsystems, so default to the full `cargo test` suite.
2. **Bump patch version** if any `.rs` file changed.
3. **Commit** with a message summarizing the audit fixes (specific files, HEREDOC, no co-author line).

## Output Format

```
## Audit Fix Summary

**Open before**: N findings
**Fixed**: X
**Won't Fix**: Y (moved with reasons)

### Self-Review
**New findings**: N (all resolved)

### Validation
fmt ✓ | lint ✓ | tests <which suites> ✓

### Commit
**Commit**: <short hash> <subject line>
```

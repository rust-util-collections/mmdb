---
description: Full codebase overhaul — audit all source files, fix every finding, commit
---

# Full Codebase Audit-Fix-Commit Pipeline

You are performing a full codebase audit: review ALL source files (not just uncommitted changes), fix every finding, and commit.

## Phase 1: Full Codebase Review

Execute `/x-review all` — the Full Audit Protocol in `.claude/commands/x-review.md`:

1. Complete the Setup section of x-review.md (load technical-patterns, review-core, false-positive-guide).
2. Launch parallel agents per Subsystem Map row; each agent reads all its source files plus its pattern guide(s) and performs the deep analysis (invariants, boundary conditions, failure paths, concurrency, unsafe audit, style rules).
3. Aggregate and deduplicate all findings.
4. Update `docs/audit.md` — prune fixed entries, merge new findings sorted by severity.
   **NEVER include timestamps, dates, "Last review", "Last sweep", "Last cleared",
   or any time-based markers in the file.** Dates bias future reviews toward
   shallowness. The file must carry zero information about *when* it was last touched.

## Phase 2: Fix

Execute the `/x-fix` Phase 1 protocol (`.claude/commands/x-fix.md`):

1. Fix every open finding in `docs/audit.md` — 100% resolution is the goal.
2. Move truly unfixable items to `## Won't Fix` with reasons.
3. Update `docs/audit.md`.

If fixes introduced new issues, re-review the CHANGED files only (not the full codebase again) and fix any new findings. Iterate until `docs/audit.md` has zero open entries (or only Won't Fix).

## Phase 3: Validate, Bump, Commit

Execute `.claude/docs/commit-protocol.md` in full:
1. **Validate** (fmt → lint → tests) — a full overhaul touches many subsystems: run the full `cargo test` suite, plus `cargo test --test crash_recovery` if WAL/flush/MANIFEST changed.
2. **Bump patch version** if any `.rs` file changed.
3. **Commit** with a message covering the audit scope and fixes applied (specific files, HEREDOC, no co-author line).

## Output Format

```
## Full Audit Pipeline Summary

### Review (full codebase)
**Subsystems audited**: <list>
**Total findings**: N (X critical, Y high, Z medium, W low)

### Fix
**Fixed**: X | **Won't Fix**: Y | **Remaining**: 0

### Validation
fmt ✓ | lint ✓ | tests <which suites> ✓

### Commit
**Commit**: <short hash> <subject line>
```

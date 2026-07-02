---
description: Deep regression review of recent changes (or full codebase with `all`); updates docs/audit.md
argument-hint: "[N | all | <hash> | <hash1>..<hash2>]"
---

# Deep Regression Analysis for MMDB

You are performing a deep code review of changes to MMDB, a Rust LSM-Tree storage engine.

## Setup

1. **MANDATORY**: Read `.claude/docs/technical-patterns.md` — your bug pattern reference.
2. Read `.claude/docs/review-core.md` — review methodology, including the canonical **Subsystem Map** (file → subsystem → pattern guide).
3. Read `.claude/docs/false-positive-guide.md` — consult before reporting any finding.

## Input

Arguments: `$ARGUMENTS`

| Input | Scope | How |
|-------|-------|-----|
| *(empty)* | Latest commit | `git diff HEAD~1`, `git log -1` |
| `N` (integer) | Last N commits | `git diff HEAD~N`, `git log -N --oneline` |
| `all` | Full codebase audit | See **Full Audit Protocol** below |
| `<commit hash>` | Specific commit | `git diff <hash>~1 <hash>` |
| `<hash1>..<hash2>` | Commit range | `git diff <hash1> <hash2>` |

For diff-based reviews, run the Execution Protocol. For `all`, skip to the Full Audit Protocol.

## Execution Protocol

### Task 1: Context & Classification

1. Read the full diff carefully.
2. Map every changed file to its subsystem via the **Subsystem Map** in review-core.md.
3. Read the pattern guide(s) for EACH affected subsystem.
4. Classify each change per review-core.md Phase 2 (control flow, resource lifecycle, concurrency, unsafe, etc.).

### Task 2: Deep Regression Analysis

For each HIGH or CRITICAL classified change:

1. **Read the surrounding code** — at least 50 lines of context around each change
2. **Trace call sites** — grep/LSP for all callers of changed functions
3. **Check invariants** — review-core.md Phase 3.1
4. **Boundary conditions** — review-core.md Phase 3.2
5. **Failure paths** — review-core.md Phase 3.3
6. **Concurrency** — if shared state is touched, full analysis per review-core.md Phase 3.4

For each candidate finding:
- Which pattern in `technical-patterns.md` does it match?
- Does `false-positive-guide.md` rule it out?
- Only report with **concrete evidence** — a specific triggering scenario.

### Task 3: Cross-Cutting Analysis

Check every change for:
1. **Crash safety** — what happens if `kill -9` hits at this exact line?
2. **Performance** — does this add latency to hot paths?
3. **API compatibility** — does this change observable behavior?

### Task 4: Code Style Enforcement

Apply the style rules from review-core.md Phase 4.4 to all changed files:
no `#[allow(...)]`, no repeated inline paths (3+ per file), grouped imports, doc-code alignment.

### Task 5: Unsafe Code Audit

If ANY `unsafe` block is added or modified:
1. Read `.claude/docs/patterns/unsafe-audit.md`
2. Verify the SAFETY comment exists, is specific, and its prerequisites actually hold
3. Check for undefined behavior (aliasing, alignment, validity, data races)

### Task 6: Audit Registry (docs/audit.md)

After completing the analysis:

1. Read `docs/audit.md` from the project root (create if absent).
2. **Prune**: verify each `## Open` entry against the current code; remove entries that are 100% fixed.
3. **Merge**: add new findings under `## Open`, deduplicated, sorted CRITICAL → HIGH → MEDIUM → LOW.
4. **Preserve**: leave `## Won't Fix` entries untouched.
5. Write the updated `docs/audit.md`.

File format:

```markdown
# Audit Findings

> Auto-managed by /x-review and /x-fix.

## Open

### [SEVERITY] subsystem: one-line summary
- **Where**: file:line_range
- **What**: description
- **Why**: invariant/pattern violated
- **Suggested fix**: how to fix

---

## Won't Fix

### [SEVERITY] subsystem: one-line summary
- **Where**: file:line_range
- **What**: description
- **Reason**: why this cannot or should not be fixed
```

## Output Format

```
## Review Summary

**Commit**: <hash> <subject>
**Subsystems**: <list of affected subsystems>
**Risk Level**: CRITICAL / HIGH / MEDIUM / LOW

## Findings

### [SEVERITY] subsystem: one-line summary

**Where**: file:line_range
**What**: Description
**Why**: Invariant/pattern violated (cite technical-patterns.md)
**Fix**: Suggested fix or questions

---

## No Issues Found

(list areas checked where no issues were found, to demonstrate coverage)
```

If zero findings after full analysis:
```
## Review Summary
**Result**: LGTM — no regressions found
**Coverage**: <list of subsystems and invariants checked>
```

---

## Full Audit Protocol (for `all` mode)

### Strategy: Parallel Subsystem Audit

Launch **one agent per subsystem row** of the Subsystem Map in review-core.md (9 subsystems; manifest and cache may share one agent). Each agent's prompt must include:
1. The subsystem's file list (from the Subsystem Map)
2. Instructions to read its pattern guide(s), `technical-patterns.md`, and `false-positive-guide.md`
3. The full review-core methodology (invariants, boundary conditions, failure paths, concurrency)
4. The code style rules (review-core.md Phase 4.4)
5. The finding report format above, prefixed with the subsystem name

### Aggregation

After all agents complete:
1. Deduplicate cross-subsystem findings (e.g., a concurrency issue reported by both write-path and compaction agents)
2. Sort by severity: CRITICAL → HIGH → MEDIUM → LOW
3. Update `docs/audit.md` per Task 6
4. Output:

```
## Full Audit Report

**Scope**: All src/ files
**Subsystems Audited**: <list>
**Total Findings**: N (X critical, Y high, Z medium, W low)

## Findings

(sorted by severity, grouped by subsystem)

## Clean Areas

(subsystems with no findings — list what was checked)
```

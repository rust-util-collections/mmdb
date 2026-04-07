# Deep Regression Analysis for MMDB

You are performing a deep code review of changes to MMDB, a Rust LSM-Tree storage engine.

## Setup

1. **MANDATORY**: Read `.claude/docs/technical-patterns.md` first — this is your bug pattern reference.
2. Read `.claude/docs/review-core.md` — this is your review methodology.
3. Read `.claude/docs/false-positive-guide.md` — consult before reporting any finding.

## Input

Analyze the changes specified by the user. If no specific input is given:
- Run `git diff HEAD~1` to get the latest commit diff
- Run `git log -1 --format="%H %s"` to understand the commit intent

If the user provides a commit hash, PR number, or file list, analyze those instead.

## Execution Protocol

### Task 1: Context & Classification

1. Read the full diff carefully
2. Identify ALL affected subsystems by mapping changed files:
   - `src/db.rs` → write-path, read-path
   - `src/memtable/` → memtable
   - `src/wal/` → WAL
   - `src/sst/` → SST
   - `src/iterator/` → iterator
   - `src/compaction/` → compaction
   - `src/manifest/` → manifest
   - `src/cache/` → cache
   - `src/types.rs` → data-encoding
   - `src/options.rs` → configuration
3. For EACH affected subsystem, read the corresponding pattern file:
   - `.claude/docs/patterns/compaction.md`
   - `.claude/docs/patterns/iterator.md`
   - `.claude/docs/patterns/wal.md`
   - `.claude/docs/patterns/sst.md`
   - `.claude/docs/patterns/memtable.md`
   - `.claude/docs/patterns/concurrency.md`
4. Classify each change per the review-core methodology (control flow, resource lifecycle, concurrency, unsafe, etc.)

### Task 2: Deep Regression Analysis

For each HIGH or CRITICAL classified change:

1. **Read the surrounding code** — at least 50 lines of context around each change
2. **Trace call sites** — use grep/LSP to find all callers of changed functions
3. **Check invariants** — verify each invariant from review-core.md Phase 3.1
4. **Boundary conditions** — check edge cases from review-core.md Phase 3.2
5. **Failure paths** — analyze error handling per review-core.md Phase 3.3
6. **Concurrency** — if shared state is touched, perform full concurrency analysis per review-core.md Phase 3.4

For each finding:
- Cross-reference with `technical-patterns.md` — which pattern does it match?
- Cross-reference with `false-positive-guide.md` — is this a known false positive?
- Only report if you have **concrete evidence**

### Task 3: Cross-Cutting Analysis

Check every change for:
1. **Crash safety** — what happens if `kill -9` hits at this exact line?
2. **Performance** — does this add latency to hot paths?
3. **API compatibility** — does this change observable behavior?

### Task 4: Code Style Enforcement

Check changed files against project style rules:

1. **No lint suppression** — `#[allow(clippy::...)]`, `#[allow(unused_...)]`, `#[allow(dead_code)]` etc. are forbidden. All warnings must be fixed at the source, not silenced. Report every `allow(...)` attribute in changed code as a finding.
2. **No inline paths** — Types must be imported via `use` at the top of the file, not referenced inline as `std::collections::HashMap::new()`. The only exception is disambiguation in a single call site where two types share a name.
3. **Import grouping** — Imports with a common prefix must be merged using nested braces: `use std::sync::{Arc, Mutex};` not separate `use std::sync::Arc; use std::sync::Mutex;`.
4. **Doc-code alignment** — If the change modifies a public function signature, struct field, or module-level behavior, verify that the corresponding doc comments, README, and CLAUDE.md still accurately describe the current behavior. Report any stale documentation as a finding.

### Task 5: Unsafe Code Audit

If ANY `unsafe` block is added or modified:
1. Read `.claude/docs/patterns/unsafe-audit.md`
2. Verify SAFETY comment exists and is accurate
3. Check all prerequisites mentioned in the SAFETY comment
4. Verify no undefined behavior (aliasing, alignment, validity)

## Output Format

Report findings as:

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

(repeat for each finding)

## No Issues Found

(list areas checked where no issues were found, to demonstrate coverage)
```

If zero findings after full analysis, report:
```
## Review Summary
**Result**: LGTM — no regressions found
**Coverage**: <list of subsystems and invariants checked>
```

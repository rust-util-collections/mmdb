# Deep Regression Analysis for MMDB

You are performing a deep code review of changes to MMDB, a Rust LSM-Tree storage engine.

## Setup

1. **MANDATORY**: Read `.claude/docs/technical-patterns.md` first — this is your bug pattern reference.
2. Read `.claude/docs/review-core.md` — this is your review methodology.
3. Read `.claude/docs/false-positive-guide.md` — consult before reporting any finding.

## Input

Arguments: `$ARGUMENTS`

Parse the arguments to determine review scope:

| Input | Scope | How |
|-------|-------|-----|
| *(empty)* | Latest commit | `git diff HEAD~1`, `git log -1` |
| `N` (integer) | Last N commits | `git diff HEAD~N`, `git log -N --oneline` |
| `all` | Full codebase audit | Read all `src/` files by subsystem (see Full Audit Protocol below) |
| `<commit hash>` | Specific commit | `git diff <hash>~1 <hash>` |
| `<hash1>..<hash2>` | Commit range | `git diff <hash1> <hash2>` |

For diff-based reviews (everything except `all`), proceed to the Execution Protocol below.
For `all`, skip to the **Full Audit Protocol** section at the end of this document.

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

---

## Full Audit Protocol (for `all` mode)

When `$ARGUMENTS` is `all`, perform a full codebase audit instead of a diff-based review.

### Strategy: Parallel Subsystem Audit

Launch **one Agent per subsystem** in parallel. Each agent receives:
1. The subsystem file list to read
2. The corresponding pattern file from `.claude/docs/patterns/`
3. `technical-patterns.md` and `false-positive-guide.md`
4. The code style rules from Task 4

### Subsystem Partitioning

| Subsystem | Files | Pattern Guide |
|-----------|-------|---------------|
| write-path & read-path | `src/db.rs`, `src/options.rs`, `src/error.rs`, `src/stats.rs` | `concurrency.md` |
| memtable | `src/memtable/mod.rs`, `src/memtable/skiplist.rs`, `src/memtable/skiplist_impl.rs` | `memtable.md`, `unsafe-audit.md` |
| WAL | `src/wal/writer.rs`, `src/wal/reader.rs`, `src/wal/record.rs` | `wal.md` |
| SST | `src/sst/table_builder.rs`, `src/sst/table_reader.rs`, `src/sst/block.rs`, `src/sst/block_builder.rs`, `src/sst/filter.rs`, `src/sst/format.rs` | `sst.md`, `unsafe-audit.md` |
| iterator | `src/iterator/db_iter.rs`, `src/iterator/merge.rs`, `src/iterator/level_iter.rs`, `src/iterator/bidi_iter.rs`, `src/iterator/range_del.rs` | `iterator.md` |
| compaction | `src/compaction/leveled.rs` | `compaction.md` |
| manifest & cache | `src/manifest/version_set.rs`, `src/manifest/version_edit.rs`, `src/manifest/version.rs`, `src/cache/block_cache.rs`, `src/cache/table_cache.rs` | `concurrency.md` |
| types & encoding | `src/types.rs`, `src/lib.rs`, `src/rate_limiter.rs` | (cross-cutting) |

### Per-Subsystem Agent Instructions

Each agent must:
1. Read ALL files in its subsystem
2. Read `technical-patterns.md` and the assigned pattern guide(s)
3. Read `false-positive-guide.md`
4. Perform the **full review-core methodology** (invariants, boundary conditions, failure paths, concurrency)
5. Apply **code style rules** (no `#[allow(...)]`, no inline paths, grouped imports, doc-code alignment)
6. Report findings in the standard format, prefixed with subsystem name

### Aggregation

After all agents complete:
1. Collect all findings
2. Deduplicate cross-subsystem findings (e.g., a concurrency issue reported by both write-path and compaction agents)
3. Sort by severity: CRITICAL → HIGH → MEDIUM → LOW
4. Output a unified audit report:

```
## Full Audit Report

**Scope**: All src/ files (~17K LOC)
**Subsystems Audited**: <list>
**Total Findings**: N (X critical, Y high, Z medium, W low)

## Findings

(sorted by severity, grouped by subsystem)

## Clean Areas

(subsystems with no findings — list what was checked)
```

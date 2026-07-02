# MMDB Review Core Methodology

This document defines the systematic review protocol for MMDB code changes.

---

## Phase 1: Context Gathering

Before analyzing any change, gather context:

1. **Read the diff** — understand every changed line
2. **Identify affected subsystems** — map changed files using the Subsystem Map below
3. **Load subsystem patterns** — read every pattern guide mapped to an affected subsystem
4. **Check call sites** — use grep/LSP to find all callers of changed functions
5. **Check related tests** — identify which test files cover the changed code

### Subsystem Map (canonical — all commands reference this table)

| Subsystem | Files | Pattern guide(s) |
|-----------|-------|------------------|
| write-path & read-path | `src/db.rs`, `src/options.rs`, `src/error.rs`, `src/stats.rs` | `concurrency.md`, `unsafe-audit.md` (db.rs unsafe) |
| memtable | `src/memtable/` (mod.rs, skiplist.rs, skiplist_impl.rs) | `memtable.md`, `unsafe-audit.md` |
| WAL | `src/wal/` (writer.rs, reader.rs, record.rs) | `wal.md` |
| SST | `src/sst/` (table_builder.rs, table_reader.rs, block.rs, block_builder.rs, filter.rs, format.rs) | `sst.md`, `unsafe-audit.md` (table_reader only) |
| iterator | `src/iterator/` (db_iter.rs, merge.rs, level_iter.rs, bidi_iter.rs, range_del.rs) | `iterator.md` |
| compaction | `src/compaction/leveled.rs` | `compaction.md` |
| manifest | `src/manifest/` (version_set.rs, version_edit.rs, version.rs) | `concurrency.md` |
| cache | `src/cache/` (block_cache.rs, table_cache.rs) | `concurrency.md` |
| types & encoding | `src/types.rs`, `src/lib.rs`, `src/rate_limiter.rs` | `technical-patterns.md` §2 (cross-cutting) |

Pattern guides live in `.claude/docs/patterns/`.

## Phase 2: Change Classification

Classify each change into one or more categories:

| Category | Description | Risk Level |
|----------|-------------|------------|
| Control flow | if/else, match, loop, early return changes | HIGH |
| Resource lifecycle | open/close, alloc/dealloc, lock/unlock | HIGH |
| Concurrency | Arc, Mutex, ArcSwap, atomic ops, thread spawn | CRITICAL |
| Unsafe code | Any code in `unsafe {}` blocks | CRITICAL |
| Error handling | Result, Option, unwrap, expect, ? operator | MEDIUM |
| Data encoding | Key/value encoding, checksums, compression | HIGH |
| Configuration | Options, defaults, thresholds | LOW |
| Logging/metrics | tracing calls, stats updates | LOW |
| Test changes | New or modified test cases | LOW |

## Phase 3: Regression Analysis

For each HIGH or CRITICAL change, perform deep analysis:

### 3.1 Invariant Check
Identify the invariants that the changed code must maintain:
- **Ordering invariant**: Keys must be sorted within and across levels
- **Uniqueness invariant**: No duplicate (user_key, seq) pairs in the same level (L1+)
- **Tombstone invariant**: Tombstones retained until bottommost compaction with no covering snapshots
- **WAL invariant**: Every committed write is recoverable from WAL until flushed
- **MANIFEST invariant**: MANIFEST always reflects a consistent file set
- **Sequence invariant**: Sequence numbers are strictly monotonic
- **Iterator invariant**: Forward iteration yields keys in ascending order; no gaps, no duplicates at user-key level

### 3.2 Boundary Condition Analysis
Check edge cases specific to the change:
- Empty database / single entry
- Maximum key/value size (exceeding block size)
- L0 file count at compaction trigger threshold
- MemTable exactly at `write_buffer_size`
- Single-file level vs multi-file level
- First/last key in a block (restart point boundaries)
- Sequence number at u64::MAX

### 3.3 Failure Path Analysis
For every new error path introduced:
- Does the error path clean up all acquired resources?
- Does partial failure leave the database in a consistent state?
- Can the operation be retried safely after failure?
- Is the error propagated with sufficient context?

### 3.4 Concurrency Analysis
For changes touching shared state:
- What lock is held? For how long?
- Can this create a new deadlock cycle?
- Is the lock ordering consistent with existing patterns?
- For lock-free paths: is the happens-before relationship correct?
- For ArcSwap: is the publish order correct (write data first, then publish pointer)?

## Phase 4: Cross-Cutting Concerns

### 4.1 Crash Safety
If the change touches the write path, flush, compaction, or MANIFEST:
- What happens if the process crashes at every point in this code?
- Is the WAL sufficient to recover?
- Are MANIFEST updates atomic (write + fsync + rename)?

### 4.2 Performance Regression
- Does this change add a lock to a previously lock-free path?
- Does this allocate on a hot path?
- Does this change the algorithmic complexity?
- Does this affect cache locality (data structure layout)?

### 4.3 API Contract
- Does the change alter observable behavior for existing users?
- Are new public APIs consistent with existing naming conventions?
- Do new options have sensible defaults?

### 4.4 Code Style Rules
These are enforced project conventions — violations are findings (severity LOW):
- **No lint suppression**: `#[allow(...)]` is forbidden. Warnings must be fixed, not silenced.
- **Prefer imports over inline paths**: Avoid `std::foo::Bar::new()` inline in function bodies when the same path appears 3+ times in a file; add a `use` import at file top instead. Function-body `use` statements (scoped imports) are fine. 1-2 inline uses of common `std::` items are acceptable.
- **Grouped imports**: Common prefixes must be merged — `use std::sync::{Arc, Mutex};` not two separate `use` lines.
- **Doc-code alignment**: Public API changes must have matching doc comment / README / CLAUDE.md updates. Stale docs are a finding. When a change adds, removes, or renames a public type, module, or subsystem path, also verify:
  - `CLAUDE.md` architecture table (paths, type names, dependency info)
  - The Subsystem Map above (canonical file → subsystem → guide mapping)
  - `.claude/docs/patterns/` guides — referenced file lists and invariants

## Phase 5: Audit Registry & Won't Fix Re-evaluation

Before reporting, consult `docs/audit.md`:

1. **Won't Fix re-evaluation** (MANDATORY on every review):
   - Read every `## Won't Fix` entry.
   - Re-assess each against the **current** code state — the original deferral decision was
     based on a snapshot that may no longer be accurate.
   - **Promote** to `## Open` if: the surrounding code has changed, the fix is now
     straightforward, or the severity was underestimated.
   - **Delete** if: the referenced code no longer exists, the pattern guide has been
     updated to cover it, or the entry is objectively no longer applicable.
   - **Keep** only if: the original reasoning still holds against the latest code,
     with a brief note confirming re-verification (e.g. "Re-checked 2026-07-02: still applies").
   - Do NOT treat Won't Fix as a permanent exemption — every entry must earn its
     place again on each audit.

2. **Open entries**: Verify each still exists in the current code; prune fixed ones.

### Finding Format
For each finding, report:

```
[SEVERITY] subsystem: one-line summary

WHERE: file:line_range
WHAT: Description of the issue
WHY: Why this is a problem (reference invariant or pattern from technical-patterns.md)
FIX: Suggested fix (if clear) or questions to resolve
```

### Severity Levels
- **CRITICAL**: Data loss, corruption, undefined behavior, or crash
- **HIGH**: Incorrect results, resource leak, or performance regression in hot path
- **MEDIUM**: Edge case bug, error handling gap, or minor performance issue
- **LOW**: Style, clarity, or non-functional improvement
- **INFO**: Observation or question, not necessarily a bug

### Quality Gate
Only report findings where you have **concrete evidence** from the code. Never report:
- Hypothetical issues without a specific triggering condition
- Style preferences not related to correctness
- "Consider" suggestions without a clear downside to the current code

Consult `.claude/docs/false-positive-guide.md` before finalizing any finding.

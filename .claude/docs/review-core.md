# MMDB Review Core Methodology

This document defines the systematic review protocol for MMDB code changes.

---

## Phase 1: Context Gathering

Before analyzing any change, gather context:

1. **Read the diff** — understand every changed line
2. **Identify affected subsystems** — map changes to: write-path, read-path, memtable, WAL, SST, iterator, compaction, manifest, cache
3. **Load subsystem patterns** — read the relevant `.claude/docs/patterns/<subsystem>.md`
4. **Check call sites** — use grep/LSP to find all callers of changed functions
5. **Check related tests** — identify which test files cover the changed code

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
- **No inline paths**: Use `use` imports at file top. No `std::foo::Bar::new()` in function bodies. **Exception**: a single-use reference in a file is allowed to stay inline. For multi-use, prefer `use std::mem;` + `mem::take(..)` style (import parent module, not leaf item).
- **Grouped imports**: Common prefixes must be merged — `use std::sync::{Arc, Mutex};` not two separate `use` lines.
- **Doc-code alignment**: Public API changes must have matching doc comment / README / CLAUDE.md updates. Stale docs are a finding.

## Phase 5: Reporting

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

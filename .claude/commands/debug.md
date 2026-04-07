# MMDB Crash & Corruption Debugger

You are debugging a crash, data corruption, or incorrect behavior in MMDB.

## Setup

1. **MANDATORY**: Read `.claude/docs/technical-patterns.md` — your bug pattern reference.
2. Read `.claude/docs/review-core.md` — methodology for systematic analysis.
3. After initial analysis, load relevant subsystem patterns from `.claude/docs/patterns/`.

## Input

The user will provide one or more of:
- A panic/crash backtrace
- A failing test case or reproduction steps
- A description of incorrect behavior (e.g., "deleted keys reappear after compaction")
- A corrupted database directory for analysis
- An error message or log output

## Execution Protocol

### Task 1: Symptom Classification

Map the symptom to a bug category from `technical-patterns.md`:

| Symptom | Likely Categories |
|---------|-------------------|
| Panic/crash | 6.x Unsafe, 3.x Resource, 5.x Iterator |
| Wrong data returned | 2.x Data Integrity, 4.x Compaction, 5.x Iterator |
| Deleted keys reappear | 4.1 Premature Tombstone Drop, 4.2 Sequence Zero |
| Data missing after crash | 2.3 CRC, WAL recovery, MANIFEST corruption |
| Deadlock/hang | 1.x Concurrency (lock ordering) |
| Memory growth | 3.3 Block Cache, 3.4 MemTable Size |
| FD exhaustion | 3.1 FD Leak, 3.2 WAL Accumulation |

### Task 2: Root Cause Investigation

Based on classification, investigate systematically:

**For crashes/panics:**
1. Parse the backtrace — identify the exact file:line
2. Read the code at the crash site with full context
3. Identify the immediate cause (unwrap on None, index out of bounds, unsafe UB)
4. Trace backwards: what state led to this condition?
5. Check: is this a single-trigger or requires specific timing?

**For data corruption:**
1. Identify WHICH data is wrong (key, value, metadata)
2. Determine WHEN it went wrong (write, flush, compaction, recovery)
3. Check sequence numbers: is this an MVCC ordering issue?
4. Check key encoding: is this a comparison/sorting issue?
5. Check tombstones: is this a deletion semantics issue?
6. If WAL-related: verify CRC integrity, record type handling, fragmentation

**For concurrency issues:**
1. Identify all threads involved
2. Map the lock acquisition order for each thread
3. Check for lock inversion (deadlock) or missing locks (race)
4. For ArcSwap-based paths: check publish ordering
5. Use `parking_lot` semantics: non-reentrant, no poisoning

**For performance issues:**
1. Profile which subsystem is slow (write path, read path, compaction)
2. Check for lock contention (Mutex vs RwLock vs lock-free)
3. Check for unnecessary allocations in hot loops
4. Check block cache hit rate and configuration
5. Check compaction parallelism and rate limiting

### Task 3: Hypothesis Verification

For each hypothesis:
1. Write a mental test: "If hypothesis X is correct, then Y should be true"
2. Verify Y by reading code, running tests, or checking state
3. If Y is false, discard hypothesis and try next
4. If Y is true, look for additional confirming evidence

### Task 4: Fix Proposal

For the confirmed root cause:
1. Propose a minimal fix with exact code changes
2. Explain why the fix is correct (which invariant it restores)
3. Identify what tests should be added to prevent regression
4. Check if the same bug pattern exists elsewhere in the codebase (grep for similar code)

## Output Format

```
## Debug Report

**Symptom**: <one-line description>
**Root Cause**: <one-line description>
**Category**: <reference to technical-patterns.md pattern>
**Severity**: CRITICAL / HIGH / MEDIUM

### Investigation

<Step-by-step explanation of how you identified the root cause>

### Root Cause Detail

**Where**: file:line_range
**What**: <detailed explanation>
**Trigger**: <exact conditions that trigger the bug>

### Proposed Fix

<Code diff or detailed description>

### Regression Test

<Test case that would catch this bug>

### Related Code

<Other locations where the same pattern might exist>
```

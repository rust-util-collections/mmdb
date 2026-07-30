# MMDB False Positive Guide

Match before reporting. Suppress, or say why the rule does not apply.

## FP-1: Safe Rust ownership

**Skip:** UAF/double-free/dangling in safe Rust (no `unsafe`/raw ptr).
**Keep:** Logical UAF (e.g. index into Vec after mutation).

## FP-2: Continuous lock

**Skip:** Check-then-act while the protecting guard spans the whole sequence.
Trace `MutexGuard`/`RwLockGuard` to end of scope or `drop`.

## FP-3: ArcSwap snapshot staleness

**Skip:** `super_version.load()` lagging a concurrent flush/compact — by design.
Pin/seq makes snapshot reads correct; non-snapshot may see any version current during the call.
**Keep:** SuperVersion used for a latest-state decision (e.g. compact inputs).

## FP-4: unwrap/expect on proven state

**Skip** unless a production path can fail it. Check same-scope population,
caller guards, and tests (panics OK).

## FP-5: Clippy already enforces

CI is `-D warnings`. Skip unused vars, needless clones, redundant closures, etc.
Focus on semantics clippy cannot see.

## FP-6: Advice without downside

No pure “consider”. Need wrong result, crash, or leak scenario.
Bad: “add bounds check”. Good: “corrupt SST prefix_len > key.len() panics on slice”.

## FP-7: Test code standards

Tests may unwrap, Drop cleanup, block, hardcode.
**Keep:** wrong tests, or `test-utils` issues that can reach production.

## FP-8: Documented unsafe

Read `// SAFETY:`. Skip if prereqs hold.
**Keep:** broken prereqs, invalidated assumptions, missing/vague comment.

## FP-9: Perf off hot path

**Hot:** get, iter next/prev, block decode, bloom. **Warm:** flush, compact loop.
**Cold:** open/close, options, error format, WAL recovery.
Report perf only on hot/warm with path evidence.

## FP-10: Incomplete lock analysis

Before races: all locks (`Mutex`/`RwLock`/`ArcSwap`), order, higher serializer
locks, and that parking_lot is non-reentrant.

## FP-11: Tombstone retention by design

Non-bottommost / snapshot-covered / covering lower levels → must keep.
Drop only when still needed → correctness. Bottommost conservative keep → only
with material accumulation, not mere eligibility.

## FP-12: Half-open ranges

MMDB is `[start, end)`. Verify use-site ops (`key < end`, not `<=`) and a concrete
mis-included/excluded key before filing.

## FP-13: Won't Fix without re-check

Not permanent. Re-check when review touches cited code/callers/assumptions/
subsystem; full audit → every entry. Carry-forward without check → LOW process.
No freshness dates in `audit.md`.

## FP-14: Legitimate no-ops

`Ok(())` / `None` / empty vec often correct. Placeholder only if contract requires
work and body is stub (`todo!`, unfinished TODO, temp dummy).

## FP-15: Design ID is not a finding

D-\* labels need trigger + outcome. Prefer LSM protocol guides when they already
catalog the bug.

## FP-16: Cache invalidation window

`insert`/reverse-index and detach vs unpinned insert are cutoff-not-barrier.
IDs never reuse → wrong data cannot swap; cold unreachable until LRU.
**Keep:** wrong-data visibility, ID reuse, unbounded retain, pin-path races.

# MMDB Checks Before Recording a Finding

Check these existing guarantees before reporting. Omit a candidate they already
cover, or explain why the guarantee does not cover the described conditions.

## FP-1: Safe Rust ownership

**Skip:** Lifetime or aliasing failures claimed in 100% safe Rust without an
`unsafe` or raw-pointer path.
**Keep:** Stale logical indices or cached offsets after collection mutation.

## FP-2: Continuous lock

**Skip:** Check-then-act while the protecting guard spans the whole sequence.
Trace `MutexGuard`/`RwLockGuard` to end of scope or `drop`.

## FP-3: ArcSwap snapshot staleness & view-before-sequence pinning

**Skip:** `super_version.load()` lagging a concurrent flush/compaction — by design.
`ReadState::point_lookup` and `scan` pin `SuperVersion` *before* `Acquire`-loading
`committed_sequence`, and `FlushContext::flush` keeps `oldest` in
`immutable_memtables` until *after* `confirm_manifest_durable`, so memtable
tombstone early-exit (`tomb_seq > 0`) never misses newly installed L0 SSTs.
**Keep:** `SuperVersion` used for a latest-state mutation decision without
revalidating under `DB::inner`.

## FP-4: unwrap/expect on proven state

**Skip** unless a production path can fail it. Check same-scope population,
caller guards, and tests (panics OK).

## FP-5: Clippy already enforces

CI is `-D warnings`. Skip unused vars, needless clones, redundant closures, etc.
Focus on semantics clippy cannot see. Check the exporter separately with
`--manifest-path tools/v4-export/Cargo.toml`.

## FP-6: Advice without downside

No unsupported “consider” advice. Name an observable result.
Vague: “add bounds check”. Concrete: “index leaf ending before its directory
separator causes `find_data_block` to return `Ok(None)` instead of `Corruption`”.

## FP-7: Test code standards

Tests may unwrap, Drop cleanup, block, hardcode.
**Keep:** wrong tests, or `test-utils` issues that can reach production.

## FP-8: Documented unsafe

Read `// SAFETY:`. Skip if prereqs (including Stacked/Tree Borrows provenance) hold.
**Keep:** broken prereqs, `Unique` retag invalidating shared pins, invalidated
assumptions, or missing/vague comment.

## FP-9: Perf off hot path

Point reads, iteration, group commit, block decoding and Bloom probes run on
core paths. Flush and compaction affect them when writers wait for capacity
or readers contend for resources. Quantify allocations, lock time, I/O or
throughput at the affected API. For occasional cleanup jobs or a one-time
export, assess the cost within that task. A bounded local cost can be an
acceptable trade for correctness or simplicity. [Performance priorities](pragmatic-engineering.md#performance-priorities)

## FP-10: Incomplete lock analysis

Before races: trace all locks (`lock_file`, `maintenance`, `write_queue`, `inner`,
`compacting_files`, `manifest_writer`, `pin_updates`, `SstBuilds`), lock order,
leader/claim barriers, and that `parking_lot` is non-reentrant.

## FP-11: Tombstone & snapshot-stripe retention by design

Non-bottommost / snapshot-covered tombstones and `RangeFragment` stripes
(`admit_range_stripes` ≤ `MAX_RANGE_STRIPES`), plus fragments carried across SST
splits (`clip_at`), must be kept.
**Keep:** premature drop (resurrection), `entry_seq -> 0` inversion against
surviving stripes, or unbounded meta-block overflow.

## FP-12: Half-open ranges

MMDB is `[start, end)`. Verify use-site ops (`key < end`, not `<=`) and a concrete
mis-included/excluded key before filing.

## FP-13: Won't Fix is not a standing exemption

An existing Won't Fix does not rule a candidate out by itself. Re-check its
reason when the review touches its cited code, callers, assumptions, or
subsystem; full audit → every entry. A stale reason moves the entry to Open or
Rejected (`workflow-policy.md` §5).

## FP-14: Legitimate no-ops

`Ok(())` / `None` / empty vec often correct (e.g., empty/inverted `delete_range`
dropped before WAL/sequence assignment). Placeholder only if contract requires
work and body is stub (`todo!`, unfinished TODO, temp dummy).

## FP-15: Design ID is not a finding

D-\* labels need trigger + outcome. Prefer LSM protocol guides when they already
catalog the bug.

## FP-16: Cache invalidation window

Unpinned `insert`/reverse-index and `detach` vs unpinned insert are cutoff-not-barrier.
`member_id` and `file_number` never reuse → wrong data cannot alias; cold orphans
stay LRU-bounded.
**Keep:** wrong-data visibility, ID reuse, `u32` weight truncation, unbounded
retain, or `pin_updates` races.

## FP-17: Not a defect

A feature request, documented contract, or by-design limit is not a finding.
Do not record it as Open or Won't Fix. A claim that the code disproves is
Rejected. Delete an existing entry when a re-check shows it was never a defect.

## FP-18: `DisableWal` & advisory `wal_bytes_per_sync`

**Skip:** `DurabilityMode::DisableWal` omitting WAL records (losing un-flushed
batches on crash is its contract), or `wal_bytes_per_sync` (`sync_file_range`)
not guaranteeing crash durability (it is an advisory writeback hint).
**Keep:** `WalSync` or `sync_wal()` (`WalFence`) returning `Ok(())` without
`sync_data()` covering prior WAL-enabled batches, or `synced_wal_seq` advancing
past unsynced `WalAsync` records.

## FP-19: `Iterator::next` vs fallible `step_next` / `step_prev` lookahead

**Skip:** `std::iter::Iterator::next` / `valid()` yielding an already-decoded,
MVCC-resolved front entry when readahead into the *following* block fails, then
halting on the next step with `error().is_some()` — documented contract.
**Keep:** `step_next` / `step_prev` returning `Ok(Some(...))` instead of `Err`
when an error is present, re-seeks/direction switches clearing a latched error,
or backward decode failure exposing a shadowed older version.

## FP-20: `ArcSnapshot` after `DB::close()` / `Drop`

**Skip:** `ArcSnapshot` methods returning `Err(ErrorKind::DbClosed)` after the
originating `DB` is closed or dropped — `SnapshotLease` holds `Weak<ReadState>`
by design so surviving handles never pin `TableCache`, `MemTable`s, SST FDs, or
`LOCK`. Cross-DB snapshot use returning `Err(ErrorKind::InvalidArgument)` via
`db_id` is also by design.

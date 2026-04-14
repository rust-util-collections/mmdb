# Audit Findings

> Auto-managed by /x-review and /x-fix.

## Open

(none)

---

## Won't Fix

### [MEDIUM] db: `do_compaction` holds db_mutex during blocking I/O
- **Where**: src/db.rs:2405-2446
- **What**: `do_compaction()` holds the inner mutex across `execute_compaction_with_cache` and `force_merge_level`, blocking all writers for the entire compaction duration.
- **Reason**: Refactoring to a 3-phase pattern (lock → I/O → lock) requires `do_compaction` to not take `&self` with the lock, which is a deep architectural change. Only triggered by explicit `compact()`/`flush()` API calls, not the background compaction path.

### [MEDIUM] db: `freeze_and_flush` holds db_mutex during SST write I/O
- **Where**: src/db.rs:2288-2299
- **What**: Writes an entire SST file while the caller holds the DB lock.
- **Reason**: Only called from `close()` (shutdown path). Refactoring is disproportionate risk for a non-hot path. The background flush path correctly uses the 3-phase pattern.

### [MEDIUM] manifest: `log_and_apply` performs I/O via `maybe_compact_manifest` while caller holds DB mutex
- **Where**: src/manifest/version_set.rs:316, 406-414
- **What**: Every 1000th edit, `maybe_compact_manifest` fires from inside `log_and_apply`, performing file create, write, fsync, rename, directory fsync, and file removal — all while the caller holds the main DB mutex.
- **Reason**: Fixing requires separating `VersionSet` from the DB mutex (new manifest-specific lock), which is a deep architectural refactor. The current behavior causes a periodic write-stall spike (~10-100ms every 1000 edits) but does not affect correctness.

### [LOW] db: `max_immutable_memtables` option declared but not enforced
- **Where**: src/options.rs:18-19
- **What**: Option is documented but no code enforces the limit.
- **Reason**: Feature not yet implemented. Option exists for API forward-compatibility. Not a correctness bug.

### [LOW] types: `from_encoded_slice` appears unused in production code
- **Where**: src/types.rs:88-95
- **What**: Public method with no callers outside tests.
- **Reason**: Public API for external consumers; cannot determine if unused without checking downstream crates.

### [LOW] types: "Safety:" comment in non-unsafe context
- **Where**: src/types.rs:49-52
- **What**: Comment uses "Safety:" but no unsafe block is involved.
- **Reason**: Comment is a design note, not a safety justification. Cosmetic only.

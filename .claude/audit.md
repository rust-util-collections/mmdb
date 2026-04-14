# Audit Findings

> Auto-managed by /x-review and /x-fix.

## Open

### [LOW] compaction: misleading variable name `min_unflushed_seq` for oldest snapshot
- **Where**: src/compaction/leveled.rs:723, 953
- **What**: The variable `min_unflushed_seq` is used to store the sequence number of the oldest active snapshot.
- **Why**: In LSM-Tree terminology, `min_unflushed_seq` typically refers to the sequence of the oldest data in the memtable/WAL. Using it to mean "oldest snapshot" might confuse future maintainers.
- **Suggested fix**: Rename to `oldest_snapshot_seq` or `min_active_snapshot_seq` in a future cleanup.

## Won't Fix

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

# Audit Findings

> Auto-managed by /x-review and /x-fix.

## Open

(none)

---

## Won't Fix

### [HIGH] sst: corrupt block entry could read restart-array bytes / iteration degrades to EOF
- **Where**: src/sst/block.rs:73-97, 411-528; src/sst/table_reader.rs:379-392
- **What**: Entry decoders bound key/value lengths against `data.len()` rather than the data-region end (`restart_offset`), and a mid-block decode failure surfaces as iterator EOF rather than an error.
- **Reason**: Data blocks are CRC32-verified before they are ever decoded (`table_reader.rs:446-452` rejects any block whose checksum does not match), so a corrupt block never reaches the decoders. The decoders already guard against out-of-bounds with `checked_add` + `value_end > data.len()`. Reaching the reported state requires a CRC32 collision (~2^-32). The suggested remedy (thread `restart_offset` through `decode_entry_at`/`decode_entry_reuse` to ~6 call sites and convert block iteration to a `Result`-returning protocol) is a large, hot-path API change disproportionate to a CRC-collision-only scenario.

### [HIGH] types: invalid ValueType byte decodes as Deletion instead of erroring
- **Where**: src/types.rs:54-62, 127-130, 178-181
- **What**: `unpack_sequence_and_type` maps an unknown type tag to `Deletion` (invisible) rather than surfacing corruption.
- **Reason**: This is a documented, intentional defensive choice (src/types.rs:58-60): all persisted internal keys come from CRC32-protected storage (WAL records and SST blocks are checksummed before decode), so an invalid tag is unreachable in practice; when it is reached (memory corruption / future format drift) defaulting to the *invisible* Deletion is strictly safer than the *phantom-data* Value. `value_type()` is on the hot read path (decoded on every key comparison/lookup); making it fallible across all SST/iterator/compaction call sites is disproportionate to a CRC-collision-only scenario.

### [MEDIUM] db: `do_compaction` holds db_mutex during blocking I/O
- **Where**: src/db.rs:2438-2493
- **What**: `do_compaction()` holds the inner mutex across `execute_compaction_with_cache` and `force_merge_level`, blocking all writers for the entire compaction duration.
- **Reason**: Refactoring to a 3-phase pattern (lock → I/O → lock) requires `do_compaction` to not take `&self` with the lock, which is a deep architectural change. Only triggered by explicit `compact()`/`flush()` API calls, not the background compaction path.

### [MEDIUM] db: `freeze_and_flush` holds db_mutex during SST write I/O
- **Where**: src/db.rs:2295-2320
- **What**: Writes an entire SST file while the caller holds the DB lock.
- **Reason**: Only called from `close()` (shutdown path). Refactoring is disproportionate risk for a non-hot path. The background flush path correctly uses the 3-phase pattern.

### [MEDIUM] manifest: `log_and_apply` performs I/O via `maybe_compact_manifest` while caller holds DB mutex
- **Where**: src/manifest/version_set.rs:317, 421-466
- **What**: Every 1000th edit, `maybe_compact_manifest` fires from inside `log_and_apply`, performing file create, write, fsync, rename, directory fsync, and file removal — all while the caller holds the main DB mutex. (`log_and_apply` now also fsyncs the DB directory when the edit adds files, for crash-safety.)
- **Reason**: Fixing requires separating `VersionSet` from the DB mutex (new manifest-specific lock), which is a deep architectural refactor. The current behavior causes a periodic write-stall spike but does not affect correctness.

### [LOW] db: `max_immutable_memtables` option declared but not enforced
- **Where**: src/options.rs:18-19
- **What**: Option is documented but no code enforces the limit.
- **Reason**: Feature not yet implemented. Option exists for API forward-compatibility. Not a correctness bug.

### [LOW] types: `from_encoded_slice` appears unused in production code
- **Where**: src/types.rs:88-95
- **What**: Public method with no callers outside tests.
- **Reason**: Public API for external consumers; cannot determine if unused without checking downstream crates.

### [LOW] types: "Safety:" comment in non-unsafe context
- **Where**: src/types.rs:54-62
- **What**: Comment uses "Safety:" but no unsafe block is involved.
- **Reason**: Comment is a design note, not a safety justification. Cosmetic only.

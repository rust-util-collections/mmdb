# Audit Findings

> Auto-managed by /x-review and /x-fix.
> Last full audit: 2026-07-02 (9 subsystems, 25 findings — 21 fixed, 4 deferred)

## Open

(none)

---

## Won't Fix

> **⚠️ Re-evaluation required on every audit.** Each entry here was deferred based on
> the code state at a specific point in time. Every `/x-review` (including `all` mode)
> MUST re-assess each Won't Fix entry against the **current** code:
> - Has the surrounding code changed enough to invalidate the original reasoning?
> - Is the severity assessment still accurate?
> - If the entry is now fixable with reasonable effort, promote it back to `## Open`.
> - If the entry is no longer relevant (code removed / refactored away), delete it.
> Do NOT treat this section as a permanent exemption list.

### [LOW] sst: pathological same-user-key version runs can still error flush instead of splitting
- **Where**: src/db.rs (write_memtable_ssts), src/sst/table_builder.rs (META_BLOCK_HARD_LIMIT guard)
- **What**: Flush/compaction outputs split at user-key boundaries when projected index/range-del meta blocks reach 32 MiB. A single user key rewritten enough times with multi-MiB keys in one memtable (e.g., 8 versions of an 8 MiB key) cannot be cut mid-run, so the builder's hard guard rejects the entry and flush fail-stops with an explicit `InvalidArgument` instead of silently producing an unreadable SST.
- **Reason**: Cutting mid-user-key across L0 files breaks newest-file-first point lookups, and supporting it safely requires ordered file-number assignment invariants across recovery and in-process installs. The trigger (tens of MiB of same-key versions inside one write buffer) is far outside realistic workloads; write-path key/entry caps (`types::MAX_USER_KEY_SIZE`, `MAX_WRITE_ENTRY_SIZE`) bound the blast radius, and the failure is now loud and attributable rather than a silent read-back wedge.

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

### [LOW] db: point-lookup `get` path scans all L0 + L1+ files with range deletions
- **Where**: src/db.rs (get path), src/iterator/
- **What**: The point-lookup `get` path iterates ALL L0 and L1+ files that have range deletions to check for covering tombstones — O(files_with_range_dels). The iterator path bounds the L1+ portion by checking `smallest_key > upper_bound` to skip files entirely past the range, but the point-lookup path cannot apply the same optimization since range tombstones can extend past a file's largest key. The L0 branch must pre-scan every tombstone-bearing L0 file before any point lookup because a single split flush numbers its files by key, not sequence, so a tombstone can live in a different L0 file than the keys it covers.
- **Reason**: This is an inherent limitation of range tombstones combined with point lookups. The cost is bounded by the number of tombstone-bearing files (typically small in practice), and the alternative (maintaining a separate tombstone index) would add write-path complexity disproportionate to the benefit.

### [LOW] types: `InternalKey` panics on malformed encoded data
- **Where**: src/types.rs (`InternalKey::from_encoded`, `from_encoded_slice`, `InternalKeyRef::new`)
- **What**: Uses `assert!` guards that panic (in both debug and release builds) on encoded keys < 8 bytes, rather than returning a `Result`.
- **Reason**: Design assumption that all encoded keys come from CRC-protected storage (WAL and SST blocks are checksummed before decode), making this condition unreachable in practice. `InternalKey::new()` builds from components and always appends the 8-byte trailer — no guard needed. Converting to fallible returns would touch every callsite on the hot read path.

### [LOW] options: `DbOptions` Debug impl omits ~14 fields
- **Where**: src/options.rs
- **What**: The manual `Debug` implementation omits `l0_slowdown_trigger`, `l0_stop_trigger`, `rate_limiter_bytes_per_sec`, `compression_per_level`, and others.
- **Reason**: Intentional conciseness — use `{:#?}` via derived Debug on inner types for full display.

---

## Known Issues & Limitations

This section provides user-facing context for the audit entries above. See individual entries in the [Won't Fix](#wont-fix) section for technical details.

### Mutex-Held I/O Trade-offs

`compact()` / `flush()` (explicit API) and manifest compaction hold the DB mutex during blocking I/O. Point reads stay lock-free via `ArcSwap<SuperVersion>`. The background flush path correctly releases the mutex during I/O. Manifest compaction fires every 1000th edit — imperceptible for most workloads (~1ms spike).

### Range Tombstone O(N) in Point Lookups

`get()` scans all L1+ files with range deletions to check for covering tombstones. The iterator path bounds this with key-range checks, but point lookups cannot apply the same optimization. Cost is bounded by the count of tombstone-bearing files, which is typically small.

### InternalKey Panics on Malformed Data

Encoded internal keys < 8 bytes cause a panic (debug and release). All stored data is CRC-protected, making corruption unreachable in practice. The design chooses a loud failure (panic with clear message) over silent data loss.

### DbOptions Debug Omits Fields

`DbOptions::fmt` is intentionally concise; use `{:#?}` for full display.

### Invalid ValueType → Deletion (Design Decision)

Unknown value type tags map to `Deletion` rather than erroring. All persisted data is CRC32-protected; when reachable (memory corruption), treating unknown as deletion is safer than returning phantom data.

### [LOW] merge: IterSource::seek_to uses next() instead of next_lazy() (missed zero-copy)
- **Where**: src/iterator/merge.rs:397-427
- **What**: After seek_to(), source branches call `iter.next()` which allocates Vec for value. `next_lazy()` returns LazyValue without allocation.
- **Reason**: Performance optimization on hot path, not a correctness bug. Restructuring to use next_lazy requires borrowing self.peeked_key across the match arms, which needs broader refactoring of the IterSource state machine. The current code is correct; the allocation overhead is marginal.

### [LOW] db_iter: forward iteration level filter may be too strict for same-level entries
- **Where**: src/iterator/db_iter.rs:443-453, src/iterator/range_del.rs:275-280
- **What**: Level filter uses `level >= src_lvl`. A tombstone at level L can never delete entries also at level L.
- **Reason**: Tentative finding — the backward path uses `None` (no level filter, conservative). The forward path's behavior is the RocksDB-derived pattern. No concrete reproduction case was identified. Requires deeper analysis with specific memtable-level tombstone scenarios.

### [LOW] test: test_write_options_no_slowdown discards its behavioral signal
- **Where**: tests/integration.rs:807-849
- **What**: Tracks `hit_error` but discards it. Accepts any outcome as valid.
- **Reason**: Test verifies the no_slowdown API doesn't crash (smoke test). Full behavioral verification (asserting write rejection under L0 pressure) requires careful setup of write buffer sizes and compaction thresholds that would be fragile. The existing test provides basic API safety coverage.

### [LOW] manifest: recovery and log_and_apply handle out-of-range levels inconsistently
- **Where**: src/manifest/version_set.rs:171-183 (recovery) vs. 281 (log_and_apply)
- **What**: Recovery errors on out-of-range levels; log_and_apply silently skips.
- **Reason**: Requires an internal logic bug to trigger (normal compaction/flush never produces out-of-range levels). Making log_and_apply fallible at this point changes its return type and touches all call sites. The recovery path already catches any such MANIFEST corruption on next open.

### Corrupt Block Entry → Restart Array Read (Design Decision)

CRC32 verification (per-block, checked before any decoder runs) blocks corrupt blocks. The window exists only under CRC32 collision (~2^-32 probability).

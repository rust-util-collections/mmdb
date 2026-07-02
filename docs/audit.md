# Audit Findings

> Auto-managed by /x-review and /x-fix.
> Last full audit: 2026-07-02 (9 subsystems, 9 new findings — all fixed; 13 Won't Fix entries re-verified, 3 promoted and fixed)

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
- **Where**: src/db.rs (write_memtable_ssts, ~2911), src/sst/table_builder.rs:243-272 (META_BLOCK_HARD_LIMIT guard)
- **What**: Flush/compaction outputs split at user-key boundaries when projected index/range-del meta blocks reach 32 MiB. A single user key rewritten enough times with multi-MiB keys in one memtable (e.g., 8 versions of an 8 MiB key) cannot be cut mid-run, so the builder's hard guard rejects the entry and flush fail-stops with an explicit `InvalidArgument` instead of silently producing an unreadable SST.
- **Reason**: Cutting mid-user-key across L0 files breaks newest-file-first point lookups, and supporting it safely requires ordered file-number assignment invariants across recovery and in-process installs. The trigger (tens of MiB of same-key versions inside one write buffer) is far outside realistic workloads; write-path key/entry caps (`types::MAX_USER_KEY_SIZE`, `MAX_WRITE_ENTRY_SIZE`) bound the blast radius, and the failure is loud and attributable rather than a silent read-back wedge. Re-checked 2026-07-02: guard and cut behavior unchanged; still applies.

### [HIGH] sst: corrupt block entry could read restart-array bytes / iteration degrades to EOF
- **Where**: src/sst/block.rs:411-528 (decode_entry_at / decode_entry_reuse); src/sst/table_reader/mod.rs:410-488 (CRC verification)
- **What**: Entry decoders bound key/value lengths against `data.len()` rather than the data-region end (`restart_offset`), and a mid-block decode failure surfaces as iterator EOF rather than an error.
- **Reason**: Data blocks are CRC32-verified before they are ever decoded (`table_reader/mod.rs:410-488` rejects any block whose checksum does not match, including on the cache-insert paths), so a corrupt block never reaches the decoders. The decoders already guard against out-of-bounds with `checked_add` + `value_end > data.len()`. Reaching the reported state requires a CRC32 collision (~2^-32). The suggested remedy (thread `restart_offset` through `decode_entry_at`/`decode_entry_reuse` to ~6 call sites and convert block iteration to a `Result`-returning protocol) is a large, hot-path API change disproportionate to a CRC-collision-only scenario. Re-checked 2026-07-02 after the table_reader/ split: all decode paths (including cached blocks) still verify CRC first; still applies.

### [HIGH] types: invalid ValueType byte decodes as Deletion instead of erroring
- **Where**: src/types.rs:72-78 (unpack_sequence_and_type), 143-146, 194-197
- **What**: `unpack_sequence_and_type` maps an unknown type tag to `Deletion` (invisible) rather than surfacing corruption.
- **Reason**: This is a documented, intentional defensive choice (src/types.rs:74-77): all persisted internal keys come from CRC32-protected storage (WAL records and SST blocks are checksummed before decode), so an invalid tag is unreachable in practice; when it is reached (memory corruption / future format drift) defaulting to the *invisible* Deletion is strictly safer than the *phantom-data* Value. `value_type()` is on the hot read path (decoded on every key comparison/lookup); making it fallible across all SST/iterator/compaction call sites is disproportionate to a CRC-collision-only scenario. Re-checked 2026-07-02: still applies.

### [MEDIUM] db: `do_compaction` holds db_mutex during blocking I/O
- **Where**: src/db.rs:2632-2700
- **What**: `do_compaction()` holds the inner mutex across `execute_compaction_with_cache` and `force_merge_level`, blocking all writers for the entire compaction duration.
- **Reason**: Refactoring to a 3-phase pattern (lock → I/O → lock) requires `do_compaction` to not take `&self` with the lock, which is a deep architectural change. Reachable from explicit `compact()`/`flush()` API calls and from the L0 stop-trigger write-stall path (which is already a deliberate stall). The background compaction path correctly releases the lock during I/O. Re-checked 2026-07-02: still applies.

### [MEDIUM] db: `freeze_and_flush` holds db_mutex during SST write I/O
- **Where**: src/db.rs:2454-2486
- **What**: Writes an entire SST file while the caller holds the DB lock.
- **Reason**: Only called from `close()` (shutdown path). Refactoring is disproportionate risk for a non-hot path. The background flush path correctly uses the 3-phase pattern. Re-checked 2026-07-02: still applies.

### [MEDIUM] manifest: `log_and_apply` performs I/O via `maybe_compact_manifest` while caller holds DB mutex
- **Where**: src/manifest/version_set.rs:301 (log_and_apply), 521-633 (maybe_compact_manifest)
- **What**: Every 1000th edit, `maybe_compact_manifest` fires from inside `log_and_apply`, performing file create, write, fsync, rename, directory fsync, and file removal — all while the caller holds the main DB mutex. (`log_and_apply` also fsyncs the DB directory when the edit adds files, for crash-safety.)
- **Reason**: Fixing requires separating `VersionSet` from the DB mutex (new manifest-specific lock), which is a deep architectural refactor. The current behavior causes a periodic write-stall spike but does not affect correctness. Re-checked 2026-07-02: still applies.

### [LOW] db: `max_immutable_memtables` option declared but not enforced
- **Where**: src/options.rs:17-20
- **What**: Option is documented but no code enforces the limit.
- **Reason**: Feature not yet implemented. Option exists for API forward-compatibility. Not a correctness bug. Re-checked 2026-07-02: still applies.

### [LOW] types: `from_encoded_slice` appears unused in production code
- **Where**: src/types.rs:113-120
- **What**: Public method with no callers outside tests.
- **Reason**: Public API for external consumers (`pub mod types`); cannot determine if unused without checking downstream crates. Re-checked 2026-07-02: still applies.

### [LOW] db: point-lookup `get` path scans all L0 + L1+ files with range deletions
- **Where**: src/db.rs (get path), src/iterator/
- **What**: The point-lookup `get` path iterates ALL L0 and L1+ files that have range deletions to check for covering tombstones — O(files_with_range_dels). The iterator path bounds the L1+ portion by checking `smallest_key > upper_bound` to skip files entirely past the range, but the point-lookup path cannot apply the same optimization since range tombstones can extend past a file's largest key. The L0 branch must pre-scan every tombstone-bearing L0 file before any point lookup because a single split flush numbers its files by key, not sequence, so a tombstone can live in a different L0 file than the keys it covers.
- **Reason**: This is an inherent limitation of range tombstones combined with point lookups. The cost is bounded by the number of tombstone-bearing files (typically small in practice), and the alternative (maintaining a separate tombstone index) would add write-path complexity disproportionate to the benefit. Re-checked 2026-07-02: still applies.

### [LOW] types: `InternalKey` panics on malformed encoded data
- **Where**: src/types.rs (`InternalKey::from_encoded`, `from_encoded_slice`, `InternalKeyRef::new`)
- **What**: Uses `assert!` guards that panic (in both debug and release builds) on encoded keys < 8 bytes, rather than returning a `Result`.
- **Reason**: Design assumption that all encoded keys come from CRC-protected storage (WAL and SST blocks are checksummed before decode), making this condition unreachable in practice. `InternalKey::new()` builds from components and always appends the 8-byte trailer — no guard needed. Converting to fallible returns would touch every callsite on the hot read path. Re-checked 2026-07-02: still applies.

### [LOW] options: `DbOptions` Debug impl is manual
- **Where**: src/options.rs (Debug impl)
- **What**: `DbOptions` needs a manual `Debug` because of non-Debug callback fields (`compaction_filter`, `block_property_collectors`); when adding a field, remember to extend the Debug impl. As of v4.0 the impl lists every field.
- **Reason**: Structural constraint of `dyn` callback members. Re-checked 2026-07-02 (v4.0): impl now covers all fields.

### [LOW] db_iter: forward iteration level filter may be too strict for same-level entries
- **Where**: src/iterator/db_iter.rs (forward path level filter), src/iterator/range_del.rs (level-filtered lookup)
- **What**: Level filter uses `level >= src_lvl`, which assumes a tombstone at level L never needs to delete entries also tagged level L.
- **Reason**: Currently dormant: DB-constructed sources are untagged (`IterSource.level` is always `usize::MAX` in production; the `with_level()` setter was removed in v4.0 as dead code), so the filter never actually excludes a tombstone. Naïvely tagging every source level 0 would be incorrect — a same-level delete is real (an active-memtable tombstone `[a,c)@10` must hide an immutable-memtable/L0 entry `b@5`, and distinct L0 files are the same "level"). Any future source-level tagging must assign distinct source indices per memtable/L0 file, not per LSM level. Re-checked 2026-07-02 (v4.0): still applies (dormant).

### [LOW] test: test_write_options_no_slowdown discards its behavioral signal
- **Where**: tests/integration.rs:807-849
- **What**: Tracks `hit_error` but discards it. Accepts any outcome as valid.
- **Reason**: Test verifies the no_slowdown API doesn't crash (smoke test). Full behavioral verification (asserting write rejection under L0 pressure) requires careful setup of write buffer sizes and compaction thresholds that would be fragile. The existing test provides basic API safety coverage. Re-checked 2026-07-02: still applies.

---

## Known Issues & Limitations

This section provides user-facing context for the audit entries above. See individual entries in the [Won't Fix](#wont-fix) section for technical details.

### Mutex-Held I/O Trade-offs

`compact()` / `flush()` (explicit API) and manifest compaction hold the DB mutex during blocking I/O. Point reads stay lock-free via `ArcSwap<SuperVersion>`. The background flush path correctly releases the mutex during I/O. Manifest compaction fires every 1000th edit — imperceptible for most workloads (~1ms spike).

### Range Tombstone O(N) in Point Lookups

`get()` scans all L0 and L1+ files with range deletions to check for covering tombstones. The iterator path bounds this with key-range checks, but point lookups cannot apply the same optimization. Cost is bounded by the count of tombstone-bearing files, which is typically small.

### InternalKey Panics on Malformed Data

Encoded internal keys < 8 bytes cause a panic (debug and release). All stored data is CRC-protected, making corruption unreachable in practice. The design chooses a loud failure (panic with clear message) over silent data loss.

### DbOptions Debug Omits Fields

`DbOptions::fmt` is intentionally concise; use `{:#?}` for full display.

### Invalid ValueType → Deletion (Design Decision)

Unknown value type tags map to `Deletion` rather than erroring. All persisted data is CRC32-protected; when reachable (memory corruption), treating unknown as deletion is safer than returning phantom data.

### Corrupt Block Entry → Restart Array Read (Design Decision)

CRC32 verification (per-block, checked before any decoder runs) blocks corrupt blocks. The window exists only under CRC32 collision (~2^-32 probability).

### WAL Mid-Log Corruption Fails Open (Design Decision)

Recovery tolerates a corrupt WAL record only when nothing but zero padding follows it (torn tail). Corruption followed by real data fails `DB::open` loudly and preserves the WAL file, rather than silently recovering a prefix and deleting later committed writes.

# Audit Findings

> Auto-managed by /x-review and /x-fix.
> Last review: 2026-07-03 (regression review of commit 7087027 / v4.0.2: 3 findings — all 3 fixed same session; all 13 Won't Fix entries re-verified against post-commit code, line refs refreshed)

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

### [HIGH] cache: `max_open_files` is not a hard process-wide file descriptor cap
- **Where**: src/cache/table_cache.rs:20-38; src/manifest/version.rs:8-24; src/manifest/version_set.rs:322-343; src/sst/table_reader/mod.rs:56-64
- **What**: `max_open_files` limits the moka table-cache entries, but live `Version`s and iterators hold `Arc<TableReader>` references; each `TableReader` holds an open SST file descriptor so live readers can keep reading an SST after compaction unlinks it.
- **Reason**: A contained metadata-only `TableReader` change breaks iterator/snapshot safety: once compaction unlinks an obsolete SST and invalidates its blocks, a live iterator over an old `SuperVersion` may need an uncached block and would fail to reopen the deleted path. A correct hard-FD-cap design requires a broader architecture: metadata-only `Version`s, on-demand table handles, and deferred obsolete-file deletion until no live reader can reference the file. Current code keeps stable file handles to preserve snapshot/iterator correctness; docs now state that `max_open_files` is a table-cache entry limit, not a hard process-wide FD cap. Re-checked 2026-07-03: still applies after self-review of the metadata-only attempt.

### [HIGH] sst: corrupt block entry could read restart-array bytes / iteration degrades to EOF
- **Where**: src/sst/block.rs:393-528 (decode_entry_at / decode_entry_reuse); src/sst/table_reader/mod.rs:420-442 (CRC verification)
- **What**: Entry decoders bound key/value lengths against `data.len()` rather than the data-region end (`restart_offset`), and a mid-block decode failure surfaces as iterator EOF rather than an error.
- **Reason**: Data blocks are CRC32-verified before they are ever decoded (`table_reader/mod.rs:420-442` rejects any block whose checksum does not match), so a corrupt block never reaches the decoders. The decoders already guard against out-of-bounds with `checked_add` + `value_end > data.len()`. Reaching the reported state requires a CRC32 collision (~2^-32). The remedy (thread `restart_offset` through the decoders + convert block iteration to `Result`) is a large hot-path API change disproportionate to a CRC-collision-only scenario. Re-checked 2026-07-03: v4.0 added the `fill_cache=false` scan path (`read_block_cached_opt`); it still runs `read_block_data`'s CRC verify before returning, so corrupt blocks are rejected on the new path too — invariant unchanged, still applies.

### [HIGH] types: invalid ValueType byte decodes as Deletion instead of erroring
- **Where**: src/types.rs:180-183 (InternalKeyRef::value_type — production path), 46-53 (ValueType::from_u8 returns Option)
- **What**: `value_type()` maps an unknown type tag to `Deletion` (invisible) rather than surfacing corruption (`from_u8(..).unwrap_or(ValueType::Deletion)`).
- **Reason**: Documented, intentional defensive choice: all persisted internal keys come from CRC32-protected storage (WAL records and SST blocks are checksummed before decode), so an invalid tag is unreachable in practice; when reached (memory corruption / future format drift) defaulting to the *invisible* Deletion is strictly safer than *phantom* Value. `value_type()` is on the hot read path; making it fallible across every SST/iterator/compaction call site is disproportionate to a CRC-collision-only scenario. Re-checked 2026-07-03: v4.0 moved `unpack_sequence_and_type` (the old test-facing decoder, types.rs:72) to `#[cfg(test)]`; the production invalid-tag→Deletion behavior now lives solely in `InternalKeyRef::value_type` (line 182). Reasoning unchanged; still applies.

### [MEDIUM] db: `do_compaction` holds db_mutex during blocking I/O
- **Where**: src/db.rs:2607 (do_compaction)
- **What**: `do_compaction()` holds the inner mutex across `execute_compaction_with_cache` and `force_merge_level`, blocking all writers for the entire compaction duration.
- **Reason**: Refactoring to a 3-phase pattern (lock → I/O → lock) requires `do_compaction` to not take `&self` with the lock — a deep architectural change. Reachable from explicit `compact()`/`flush()` API calls and from the L0 stop-trigger write-stall path (already a deliberate stall). The background compaction path correctly releases the lock during I/O. Re-checked 2026-07-03: function unchanged by v4.0 (only error-call migration); still applies.

### [MEDIUM] db: `freeze_and_flush` holds db_mutex during SST write I/O
- **Where**: src/db.rs:2429 (freeze_and_flush)
- **What**: Writes an entire SST file while the caller holds the DB lock.
- **Reason**: Only called from `close()` (shutdown path). Refactoring is disproportionate risk for a non-hot path. The background flush path correctly uses the 3-phase pattern. Re-checked 2026-07-03: unchanged; still applies.

### [MEDIUM] manifest: `log_and_apply` performs I/O via `maybe_compact_manifest` while caller holds DB mutex
- **Where**: src/manifest/version_set.rs:296 (log_and_apply), 512 (maybe_compact_manifest)
- **What**: Every 1000th edit, `maybe_compact_manifest` fires from inside `log_and_apply`, performing file create, write, fsync, rename, directory fsync, and file removal — all while the caller holds the main DB mutex. (`log_and_apply` also fsyncs the DB directory when the edit adds files, for crash-safety.)
- **Reason**: Fixing requires separating `VersionSet` from the DB mutex (new manifest-specific lock) — a deep architectural refactor. Causes a periodic write-stall spike but does not affect correctness. Re-checked 2026-07-03: v4.0 migrated error calls only; atomicity (build version → dir fsync → MANIFEST append → install) unchanged; still applies.

### [LOW] sst: pathological same-user-key version runs can still error flush instead of splitting
- **Where**: src/db.rs (write_memtable_ssts, ~2880), src/sst/table_builder.rs:242-270 (META_BLOCK_HARD_LIMIT guard)
- **What**: Flush/compaction outputs split at user-key boundaries when projected index/range-del meta blocks reach the split threshold. A single user key rewritten enough times with multi-MiB keys in one memtable cannot be cut mid-run, so the builder's hard guard rejects the entry and flush fail-stops with an explicit `InvalidArgument` instead of silently producing an unreadable SST.
- **Reason**: Cutting mid-user-key across L0 files breaks newest-file-first point lookups, and supporting it safely requires ordered file-number assignment invariants across recovery and in-process installs. The trigger (tens of MiB of same-key versions inside one write buffer) is far outside realistic workloads; write-path key/entry caps (`types::MAX_USER_KEY_SIZE`, `MAX_WRITE_ENTRY_SIZE`) bound the blast radius, and the failure is loud and attributable. Re-checked 2026-07-03: guard (`META_BLOCK_HARD_LIMIT`, table_builder.rs:29) and cut behavior unchanged; still applies.

### [LOW] db: `max_immutable_memtables` option declared but not enforced
- **Where**: src/options.rs:21 (field; default 4 at :93)
- **What**: Option is documented but no code enforces the limit.
- **Reason**: Feature not yet implemented. Option exists for API forward-compatibility. Not a correctness bug. Re-checked 2026-07-03: still declared, still unenforced; survived the v4.0 no-op-knob purge because it is intended to be wired up. Still applies.

### [LOW] db: point-lookup `get` path scans all L0 + L1+ files with range deletions
- **Where**: src/db.rs:971 (L0 tombstone scan), 1004 (L1+ tombstone scan); src/iterator/
- **What**: The point-lookup `get` path iterates ALL L0 and L1+ files that have range deletions to check for covering tombstones — O(files_with_range_dels). The iterator path bounds the L1+ portion by `smallest_key > upper_bound`, but the point-lookup path cannot (range tombstones can extend past a file's largest key). The L0 branch must pre-scan every tombstone-bearing L0 file because a split flush numbers its files by key, not sequence, so a tombstone can live in a different L0 file than the keys it covers.
- **Reason**: Inherent limitation of range tombstones combined with point lookups. Cost is bounded by the number of tombstone-bearing files (typically small); a separate tombstone index would add write-path complexity disproportionate to the benefit. Re-checked 2026-07-03: unchanged; still applies.

### [LOW] types: `InternalKey` panics on malformed encoded data
- **Where**: src/types.rs:159-164 (`InternalKeyRef::new` assert)
- **What**: `InternalKeyRef::new` uses an `assert!` that panics (in debug and release) on encoded keys < 8 bytes, rather than returning a `Result`.
- **Reason**: Design assumption that all encoded keys come from CRC-protected storage (WAL and SST blocks are checksummed before decode), making this unreachable in practice. `InternalKey::new()` builds from components and always appends the 8-byte trailer — no guard needed. Converting to fallible returns would touch every hot-path call site. Re-checked 2026-07-03: v4.0 **deleted** `from_encoded_slice` and moved `from_encoded` to `#[cfg(test)]` (types.rs:106); `compare_internal_key` handles <8-byte keys defensively without panicking (types.rs:201-202). The only remaining production panic path is `InternalKeyRef::new` — entry narrowed accordingly. Still applies.

### [LOW] options: `DbOptions` Debug impl is manual
- **Where**: src/options.rs (Debug impl)
- **What**: `DbOptions` needs a manual `Debug` because of non-Debug callback fields (`compaction_filter`, `block_property_collectors`); when adding a field, remember to extend the Debug impl.
- **Reason**: Structural constraint of `dyn` callback members. Re-checked 2026-07-03 (v4.0): `DbOptions` now `#[derive(Clone)]` (the error-prone manual Clone was removed), but Debug remains manual and, as of v4.0, lists every field. Still applies (maintenance note).

### [LOW] db_iter: forward iteration level filter may be too strict for same-level entries
- **Where**: src/iterator/db_iter.rs (forward path level filter), src/iterator/range_del.rs (level-filtered lookup)
- **What**: Level filter uses `level >= src_lvl`, which assumes a tombstone at level L never needs to delete entries also tagged level L.
- **Reason**: Dormant: DB-constructed sources are untagged (`IterSource.level` is always `usize::MAX` in production; the `with_level()` setter was removed in v4.0 as dead code), so the filter never actually excludes a tombstone. Naïvely tagging every source level 0 would be incorrect — a same-level delete is real (an active-memtable tombstone `[a,c)@10` must hide an immutable-memtable/L0 entry `b@5`, and distinct L0 files are the same "level"). Any future source-level tagging must assign distinct source indices per memtable/L0 file, not per LSM level. Re-checked 2026-07-03 (v4.0.2): the prefix-clamp changes in db_iter.rs touch adjacent code but not the level filter; still dormant, still applies.

### [LOW] test: test_write_options_no_slowdown discards its behavioral signal
- **Where**: tests/integration.rs:808
- **What**: Tracks `hit_error` but discards it. Accepts any outcome as valid.
- **Reason**: Test verifies the no_slowdown API doesn't crash (smoke test). Full behavioral verification (asserting write rejection under L0 pressure) requires fragile setup of write buffer sizes and compaction thresholds. The existing test provides basic API safety coverage. Re-checked 2026-07-03: unchanged; still applies.

---

## Known Issues & Limitations

This section provides user-facing context for the audit entries above. See individual entries in the [Won't Fix](#wont-fix) section for technical details.

### Mutex-Held I/O Trade-offs

`compact()` / `flush()` (explicit API) and manifest compaction hold the DB mutex during blocking I/O. Point reads stay lock-free via `ArcSwap<SuperVersion>`. The background flush path correctly releases the mutex during I/O. Manifest compaction fires every 1000th edit — imperceptible for most workloads (~1ms spike).

### Range Tombstone O(N) in Point Lookups

`get()` scans all L0 and L1+ files with range deletions to check for covering tombstones. The iterator path bounds this with key-range checks, but point lookups cannot apply the same optimization. Cost is bounded by the count of tombstone-bearing files, which is typically small.

### InternalKey Panics on Malformed Data

`InternalKeyRef::new` panics (debug and release) on encoded internal keys < 8 bytes. All stored data is CRC-protected, making corruption unreachable in practice. The design chooses a loud failure (panic with clear message) over silent data loss. `from_encoded` is test-only and `compare_internal_key` degrades gracefully without panicking.

### DbOptions Debug Omits Nothing (but is manual)

`DbOptions` derives `Clone` but keeps a hand-written `Debug` (required by its `dyn` callback fields); as of v4.0 the impl lists every field.

### Invalid ValueType → Deletion (Design Decision)

Unknown value type tags map to `Deletion` rather than erroring (`InternalKeyRef::value_type`). All persisted data is CRC32-protected; when reachable (memory corruption), treating unknown as deletion is safer than returning phantom data.

### Corrupt Block Entry → Restart Array Read (Design Decision)

CRC32 verification (per-block, checked before any decoder runs — including the v4.0 `fill_cache=false` scan path) blocks corrupt blocks. The window exists only under CRC32 collision (~2^-32 probability).

### Table Cache FD Cap Is Best-Effort

`max_open_files` bounds table-cache entries, not every live `TableReader` held by snapshots/iterators. Stable file handles are required so old readers remain valid after compaction unlinks obsolete SST paths.

### WAL Mid-Log Corruption Fails Open (Design Decision)

Recovery tolerates a corrupt WAL record only in the highest non-empty recovered WAL and only when nothing but zero padding follows it (torn active tail). Corruption in earlier WALs, or followed by real data, fails `DB::open` loudly and preserves the WAL file, rather than silently recovering a prefix and deleting later committed writes.

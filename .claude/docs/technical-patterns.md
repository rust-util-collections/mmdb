# MMDB Technical Bug Patterns

This document catalogs known bug categories in LSM-Tree storage engines implemented in Rust.
Load this document FIRST before performing any review or debug analysis.

---

## Category 1: Concurrency & Atomicity Bugs

### 1.1 SuperVersion Stale Read
**Pattern**: Reading from a stale `SuperVersion` snapshot after a flush or compaction has completed but before `ArcSwap::store()` publishes the new version.
**Where**: `db.rs` — any path that reads `self.super_version` without checking if a background job has committed a new `VersionEdit`.
**Impact**: Reads return deleted keys or miss recently flushed data.
**Check**: Verify that every code path calling `super_version.load()` either holds a `Guard` or explicitly accepts point-in-time staleness (e.g., snapshot reads).

### 1.2 Group Commit Ordering
**Pattern**: If a refactor changes the group commit leader to process WAL writes and MemTable inserts in separate passes (e.g., all WAL writes first, then all MemTable inserts), a reordering or partial-visibility window could emerge where batch B is visible in the MemTable before batch A, despite A having a lower sequence number.
**Where**: `db.rs` write path — `write_batch_group()`. Currently correct: processes each batch sequentially (assign seq → WAL write → MemTable insert) in a single forward-order loop.
**Impact**: Would violate linearizability. A reader could see a later write but not an earlier one.
**Check**: Verify the per-batch loop order is maintained: sequence assignment → WAL append → MemTable insert, all in the same forward pass. Watch for refactors that split these into separate passes.

### 1.3 Compaction + Flush Race
**Pattern**: Flush produces a new L0 file while compaction is reading the file list. Compaction's `VersionEdit` overwrites the flush's `VersionEdit`.
**Where**: `compaction/leveled.rs` and `db.rs` flush path — both modify `VersionSet`.
**Impact**: Lost L0 file reference — data silently disappears.
**Check**: Verify that `VersionSet::log_and_apply()` is serialized (single Mutex) and that both flush and compaction go through the same path.

### 1.4 Iterator Invalidation on Drop
**Pattern**: An iterator holds references to MemTable or SST data. If the MemTable is dropped (flush completes) or SST is deleted (compaction completes) while the iterator is live, the references become dangling.
**Where**: `iterator/db_iter.rs`, `iterator/merge.rs` — lifetime of internal iterators.
**Impact**: Use-after-free or reading garbage data.
**Check**: Verify iterators hold `Arc` references that prevent premature cleanup. Check `SuperVersion` pinning.

---

## Category 2: Data Integrity Bugs

### 2.1 Sequence Number Mismanagement
**Pattern**: Assigning duplicate sequence numbers, or failing to increment the global sequence counter after a WriteBatch.
**Where**: `db.rs` write path, `types.rs` sequence handling.
**Impact**: MVCC corruption — snapshot reads return wrong versions.
**Check**: Verify `last_sequence` is atomically incremented by exactly `batch.count()` after each write.

### 2.2 InternalKey Encoding/Decoding Mismatch
**Pattern**: Encoding user_key as `[user_key][!pack(seq, type)]` but decoding with wrong byte order or wrong bit inversion.
**Where**: `types.rs` — `InternalKey::encode()` / `InternalKey::decode()`.
**Impact**: Incorrect key ordering — breaks binary search, iterator merge, compaction.
**Check**: Verify pack/unpack round-trips. Verify bit inversion (`!`) is applied symmetrically.

### 2.3 CRC Checksum Scope Error
**Pattern**: CRC covers only part of the WAL record (e.g., missing the type byte), or CRC is computed before compression but verified after decompression.
**Where**: `wal/record.rs`, `wal/writer.rs`, `wal/reader.rs`.
**Impact**: Silent data corruption goes undetected on recovery.
**Check**: Verify CRC scope matches exactly: `crc32(type_byte || payload)`.

### 2.4 Block Prefix Compression Corruption
**Pattern**: Restart point offset is wrong, causing prefix decompression to use the wrong base key.
**Where**: `sst/block.rs`, `sst/block_builder.rs`.
**Impact**: Keys within a block decode incorrectly — affects seeks and range queries.
**Check**: Verify restart point indices are written at correct intervals. Verify prefix length never exceeds previous key length.

### 2.5 Range Tombstone Boundary Error
**Pattern**: Range deletion `[start, end)` is applied as `[start, end]` (inclusive end) or `(start, end)` (exclusive start).
**Where**: `iterator/range_del.rs`, `compaction/leveled.rs` (tombstone GC).
**Impact**: Deletes too many or too few keys.
**Check**: Verify half-open interval semantics `[start, end)` consistently across all consumers.

---

## Category 3: Resource Management Bugs

### 3.1 File Descriptor Leak on Error Path
**Pattern**: SST file opened for reading but not closed when an error occurs during table_reader construction.
**Where**: `sst/table_reader.rs` — `TableReader::open()` error paths.
**Impact**: FD exhaustion under sustained errors (e.g., corrupted files).
**Check**: Verify all error paths either close the file or rely on RAII (`Drop`).

### 3.2 WAL File Accumulation
**Pattern**: Old WAL files not deleted after successful flush because the deletion is gated on a condition that's never met.
**Where**: `db.rs` — post-flush cleanup, WAL file lifecycle.
**Impact**: Disk space exhaustion.
**Check**: Verify WAL deletion triggers after MemTable flush completes and MANIFEST records the new state.

### 3.3 Block Cache Memory Pressure
**Pattern**: L0 pinned blocks are never unpinned after compaction removes the L0 file.
**Where**: `cache/block_cache.rs` — `insert_pinned()` and compaction cleanup.
**Impact**: Unbounded cache growth during sustained L0 write pressure.
**Check**: Verify compaction completion triggers `unpin()` for all blocks of deleted L0 files.

### 3.4 MemTable Size Tracking Drift
**Pattern**: `approximate_memory_usage()` drifts from actual allocation due to missed accounting of range tombstone entries or skiplist node overhead.
**Where**: `memtable/mod.rs`, `memtable/skiplist.rs`.
**Impact**: MemTable grows larger than `write_buffer_size` before triggering flush.
**Check**: Verify all insert paths (put, delete, delete_range) update the size tracker.

---

## Category 4: Compaction Correctness Bugs

### 4.1 Premature Tombstone Drop
**Pattern**: Compaction at a non-bottommost level drops a tombstone, but the key it covers still exists in a lower level.
**Where**: `compaction/leveled.rs` — tombstone retention logic.
**Impact**: Deleted keys reappear ("zombie keys").
**Check**: Tombstones must be retained unless compacting the bottommost level AND no snapshot references the tombstone's sequence number.

### 4.2 Sequence Zero at Wrong Level
**Pattern**: Sequence numbers zeroed for entries that are not at the bottommost level, breaking snapshot isolation.
**Where**: `compaction/leveled.rs` — sequence zeroing logic.
**Impact**: Snapshots see inconsistent data.
**Check**: Verify `is_bottommost_level` check is correct and accounts for overlapping files in lower levels.

### 4.3 Sub-Compaction Boundary Key Duplication
**Pattern**: When splitting compaction work across threads, a key at the split boundary is processed by both sub-compactions.
**Where**: `compaction/leveled.rs` — sub-compaction boundary calculation.
**Impact**: Duplicate entries in output SSTs — incorrect reads.
**Check**: Verify split boundaries use exclusive-start semantics and no key spans two sub-compaction ranges.

### 4.4 Trivial Move Incorrect Condition
**Pattern**: Trivial move (rename without rewrite) applied when L(n+1) has overlapping files, or when a compaction filter needs to process entries.
**Where**: `compaction/leveled.rs` — trivial move check.
**Impact**: Stale tombstones persist, overlapping key ranges at L(n+1), or compaction filter logic bypassed.
**Check**: Verify trivial move requires ALL three conditions: (1) exactly 1 input file, (2) 0 overlapping files at target level, (3) no compaction filter installed (`compaction_filter.is_none()`).

---

## Category 5: Iterator Protocol Bugs

### 5.1 Heap Invariant Violation After Direction Switch
**Pattern**: Switching from forward to backward iteration without rebuilding the heap, leaving children in inconsistent positions.
**Where**: `iterator/merge.rs`, `iterator/bidi_iter.rs`.
**Impact**: Missed keys or duplicate keys during reverse iteration.
**Check**: Verify direction switch re-positions all child iterators and rebuilds the heap.

### 5.2 Prefix Bound Overshoot
**Pattern**: `iter_with_prefix()` continues past the prefix boundary due to incorrect comparison logic.
**Where**: `iterator/db_iter.rs` — prefix bound checking.
**Impact**: Returns keys outside the requested prefix range.
**Check**: Verify prefix comparison uses byte-wise prefix match, not lexicographic comparison of the full key.

### 5.3 Snapshot Filtering Gap
**Pattern**: Iterator skips a visible version of a key because it encounters a newer (invisible) version and advances past all versions.
**Where**: `iterator/db_iter.rs` — sequence number filtering.
**Impact**: Get returns None for a key that should be visible at the snapshot's sequence.
**Check**: Verify the iterator checks ALL versions of a key until finding one with seq <= snapshot_seq, not just the first.

---

## Category 6: Unsafe Code Bugs

### 6.1 Skiplist Node Lifetime
**Pattern**: Skiplist node contains raw pointers to other nodes. If a node is deallocated while another thread holds a pointer, it's use-after-free.
**Where**: `memtable/skiplist_impl.rs`.
**Impact**: Memory corruption, undefined behavior, potential data loss.
**Check**: Verify epoch-based reclamation or that nodes are never freed while any reader holds a reference.

### 6.2 Block Slice Aliasing
**Pattern**: `&[u8]` slice into block data is used after the block is evicted from cache or the underlying buffer is reallocated.
**Where**: `sst/block.rs`, `sst/table_reader.rs`.
**Impact**: Read garbage data or crash.
**Check**: Verify block data lifetime is tied to cache handle or `Arc<Vec<u8>>`.

### 6.3 Unaligned Read/Write
**Pattern**: `ptr::read_unaligned` or `ptr::write_unaligned` not used when reading packed integer fields from byte slices.
**Where**: `types.rs`, `sst/format.rs`, `wal/record.rs`.
**Impact**: Undefined behavior on architectures requiring alignment.
**Check**: Verify all integer reads from byte slices use `from_le_bytes()` or `read_unaligned`.

# MMDB Technical Bug Patterns

This document catalogs known bug categories in LSM-Tree storage engines implemented in Rust.
Load this document FIRST before performing any review or debug analysis.

---

## Category 1: Concurrency & Atomicity Bugs

### 1.1 SuperVersion Used for a Latest-State Decision
**Pattern**: Using a point-in-time `SuperVersion` for a mutation or maintenance
decision that requires the latest installed Version.
**Where**: `db.rs` — calls to `self.super_version.load()`.
**Impact**: A compaction/cleanup decision can be built from superseded files.
**Check**: Ordinary reads intentionally use a pinned, internally consistent
`ArcSwap` snapshot; do not report that staleness. Paths that require latest
state must revalidate under the DB lock before install (for example,
`install_compaction` stale-output checks).

### 1.2 Group Commit Ordering
**Pattern**: The WAL and MemTable phase split uses different request order, or
publishes `committed_sequence` before every assigned MemTable insert completes.
That can make batch B visible while lower-sequence batch A is absent.
**Where**: `db.rs` write path — `write_batch_group()`. Currently correct:
sequence numbers for the whole group are reserved in bulk upfront, then one
loop appends every WAL-enabled request, a shared fsync/flush runs, and a second
loop inserts assigned requests into the MemTable. Both loops preserve group
order and `committed_sequence` is published after insertion.
**Impact**: Would violate linearizability. A reader could see a later write but not an earlier one.
**Check**: Verify both the WAL-write loop and the MemTable-insert loop iterate the batch group in identical order. Watch for refactors that reorder within either phase, or that make one loop skip/reorder entries relative to the other.

### 1.3 Compaction + Flush Race
**Pattern**: Flush produces a new L0 file while compaction is reading the file list. Compaction's `VersionEdit` overwrites the flush's `VersionEdit`.
**Where**: `compaction/leveled.rs` and `db.rs` flush path — both modify `VersionSet`.
**Impact**: Lost L0 file reference — data silently disappears.
**Check**: Verify that `VersionSet::log_and_apply()` is serialized (single Mutex) and that both flush and compaction go through the same path.

### 1.4 Iterator Ownership Regression
**Pattern**: A future iterator refactor borrows MemTable/SST data without
retaining the owning `Arc` across flush, compaction, or cache eviction.
**Where**: `iterator/db_iter.rs`, `iterator/merge.rs` — lifetime of internal iterators.
**Impact**: Use-after-free or reading garbage data.
**Check**: Current safe-Rust iterators own the required `Arc` references and
pin a SuperVersion. Report only a concrete raw/borrowed ownership regression,
not ordinary pathname deletion or cache eviction while an owner remains live.

---

## Category 2: Data Integrity Bugs

### 2.1 Sequence Number Mismanagement
**Pattern**: Assigning duplicate sequence numbers, or failing to increment the global sequence counter after a WriteBatch.
**Where**: `db.rs` write path, `types.rs` sequence handling.
**Impact**: MVCC corruption — snapshot reads return wrong versions.
**Check**: Verify `write_batch_group()` reserves one contiguous range equal to
the total operation count, assigns it monotonically in group order, and
publishes `committed_sequence` only after the corresponding MemTable inserts.

### 2.2 InternalKey Encoding/Decoding Mismatch
**Pattern**: Encoding user_key as `[user_key][!pack(seq, type)]` but decoding with wrong byte order or wrong bit inversion.
**Where**: `types.rs` — `InternalKey::new()` (encoding from components) / `InternalKeyRef` (decoding from bytes: `user_key()`/`sequence()`/`value_type()`).
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
**Where**: `sst/table_reader/mod.rs` — `TableReader::open_with_all()` error paths.
**Impact**: FD exhaustion under sustained errors (e.g., corrupted files).
**Check**: Verify all error paths either close the file or rely on RAII (`Drop`).

> **Status**: Low current risk. `open_with_all()` owns its `File` locally, so
> every `?` return closes it through RAII. This pattern becomes live only if a
> future refactor leaks/forgets the file or extracts a raw descriptor whose
> ownership is not restored.

### 3.2 WAL File Accumulation
**Pattern**: Old WAL files not deleted after successful flush because the deletion is gated on a condition that's never met.
**Where**: `db.rs` — post-flush cleanup, WAL file lifecycle.
**Impact**: Disk space exhaustion.
**Check**: Verify WAL deletion triggers after MemTable flush completes and MANIFEST records the new state.

### 3.3 Block Cache Memory Pressure
**Pattern**: L0 pinned blocks are never unpinned after compaction removes the L0 file.
**Where**: `cache/block_cache.rs` — `insert_pinned()` and compaction cleanup.
**Impact**: Unbounded cache growth during sustained L0 write pressure.
**Check**: Verify file removal triggers `unpin_file()`/`invalidate_file()` for
every deleted L0 file. See `patterns/cache.md`.

### 3.4 MemTable Size Tracking Drift
**Pattern**: `approximate_size()` drifts from actual allocation due to missed accounting of range tombstone entries or skiplist node overhead.

> **Status**: Resolved in current code. `MemTable::put()` computes `entry_size`
> (encoded key + value + approximate node overhead) and adds duplicated
> range-tombstone storage for `ValueType::RangeDeletion`;
> `approximate_size()` only reads the atomic counter. The check below targets
> future entry/storage changes.

**Where**: `memtable/mod.rs`, `memtable/skiplist.rs`.
**Impact**: MemTable grows larger than `write_buffer_size` before triggering flush.
**Check**: Verify `MemTable::put()` accounts for all `ValueType` variants
(`Value`, `Deletion`, `RangeDeletion`). New entry types or duplicated storage
must add their overhead to the atomic counter.

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
**Pattern**: Trivial move (metadata-only level change) applied when L(n+1) has
overlapping files or when an effective compaction filter must process entries.
**Where**: `compaction/leveled.rs` — trivial move check.
**Impact**: Stale tombstones persist, overlapping key ranges at L(n+1), or compaction filter logic bypassed.
**Check**: Verify the ordinary trivial move requires exactly one input file, no
target-level overlap, and no effective filter (`None` or `is_noop()`). Forced
merge no-op shortcuts may also bypass rewriting when the filter cannot apply
(not bottommost or snapshots are active).

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

> **Note**: Unsafe code is concentrated in the skiplist, DB group-commit/file
> locking, and SST readahead syscall paths. Derive the live inventory during
> review and use `patterns/unsafe-audit.md`; do not rely on copied counts.

### 6.1 Skiplist Node Lifetime
**Pattern**: Skiplist node contains raw pointers to other nodes. If a node is deallocated while another thread holds a pointer, it's use-after-free.
**Where**: `memtable/skiplist_impl.rs`.
**Impact**: Memory corruption, undefined behavior, potential data loss.
**Check**: Verify epoch-based reclamation or that nodes are never freed while any reader holds a reference.

### 6.2 Block Ownership Regression
**Pattern**: A future refactor returns a borrowed block slice without retaining
the owning allocation across cache eviction.
**Where**: `sst/block.rs`, `sst/table_reader/`.
**Impact**: Read garbage data or crash.
**Check**: Current code is safe and `Arc<Vec<u8>>`-backed. Verify any new
borrowed/raw-pointer path retains equivalent ownership; do not report ordinary
cache eviction while an `Arc` is live.

### 6.3 Unaligned Read/Write
**Pattern**: `ptr::read_unaligned` or `ptr::write_unaligned` not used when reading packed integer fields from byte slices.
**Where**: `types.rs`, `sst/format.rs`, `wal/record.rs`.
**Impact**: Undefined behavior on architectures requiring alignment.
**Check**: Verify all integer reads from byte slices use `from_le_bytes()` or `read_unaligned`.

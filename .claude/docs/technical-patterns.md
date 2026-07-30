# MMDB Technical Bug Patterns

LSM-Tree / Rust bug catalog. Load before review or debug.

## 1. Concurrency & atomicity

### 1.1 SuperVersion for latest-state work
**Pattern:** Point-in-time SV used for mutation/maintenance needing latest Version.
**Where:** `db.rs` `super_version.load()`.
**Impact:** Compact/cleanup on superseded files.
**Check:** Ordinary reads may use pinned ArcSwap snapshot — do not flag that.
Latest-state paths must revalidate under DB lock before install (e.g. `install_compaction` stale-output checks).

### 1.2 Group-commit ordering
**Pattern:** WAL vs MemTable phase reorder, or `committed_sequence` before all MemTable inserts → later batch visible without earlier.
**Where:** `write_batch_group()`. Correct today: bulk reserve seq → WAL loop (order preserved) → shared fsync/flush → MemTable loop (same order) → publish seq after inserts.
**Impact:** Linearizability break.
**Check:** Both loops same group order; no skip/reorder; publish after inserts.

### 1.3 Flush vs compaction race
**Pattern:** Flush L0 while compact reads file list; VersionEdit clobber.
**Where:** `leveled.rs`, flush path → `VersionSet`.
**Impact:** Lost L0 ref.
**Check:** `log_and_apply` serialized on one Mutex; both paths use it.

### 1.4 Iterator ownership regression
**Pattern:** Borrow MemTable/SST without holding owning Arc across flush/compact/evict.
**Where:** `db_iter.rs`, `merge.rs`.
**Impact:** UAF / garbage.
**Check:** Safe paths own Arcs + pin SV. Report only real borrow/raw regression — not unlink/evict while Arc lives.

## 2. Data integrity

### 2.1 Sequence mismanagement
**Pattern:** Dup seq or failed counter advance after WriteBatch.
**Where:** write path, `types.rs`.
**Impact:** Bad snapshot reads.
**Check:** Contiguous reserve = op count; monotonic group assign; publish after MemTable inserts.

### 2.2 InternalKey pack/unpack
**Pattern:** Encoding vs decode mismatch (endian / `!` inversion).
**Where:** `InternalKey::new` / `InternalKeyRef`.
**Impact:** Bad order → seek/merge/compact wrong.
**Check:** Round-trip; `!` symmetric.

### 2.3 CRC scope
**Pattern:** CRC misses type byte, or compressbound vs verify-after-decompress mismatch.
**Where:** `wal/record|writer|reader`.
**Impact:** Silent corrupt recovery.
**Check:** `crc32(type || payload)` writer=reader.

### 2.4 Block prefix compression
**Pattern:** Bad restart offset → wrong base key.
**Where:** `block.rs`, `block_builder.rs`.
**Impact:** Wrong keys in block.
**Check:** Restart interval/offsets; shared_len ≤ prev key; shared=0 at restarts.

### 2.5 Range tombstone bounds
**Pattern:** `[start,end)` treated inclusive-end or exclusive-start.
**Where:** `range_del.rs`, compact GC.
**Impact:** Over/under delete.
**Check:** Half-open everywhere.

## 3. Resources

### 3.1 FD leak on open error
**Pattern:** SST open fails mid-construct; FD escapes Drop.
**Where:** `TableReader::open_with_all`.
**Impact:** FD exhaustion under corrupt files.
**Check:** Owned `File` + RAII (current: low risk unless raw-fd refactor).

### 3.2 WAL accumulation
**Pattern:** Old WALs never deleted (wrong gate).
**Where:** post-flush cleanup.
**Impact:** Disk fill.
**Check:** Delete after flush + MANIFEST records new state.

### 3.3 Pinned L0 cache growth
**Pattern:** Pin never unpin after L0 file gone.
**Where:** `block_cache.rs`.
**Impact:** Unbounded cache under L0 pressure.
**Check:** File removal → `unpin_file`/`invalidate_file`. See `patterns/cache.md`.

### 3.4 MemTable size drift
**Pattern:** Miss range-del or node overhead in size (was fixed; watch refactors).
**Where:** `memtable/`.
**Impact:** Late flush / OOM.
**Check:** All `ValueType`s + duplicated range-del storage hit atomic counter.

## 4. Compaction

### 4.1 Premature tombstone drop
**Pattern:** Non-bottommost drops tombstone; key still lower.
**Impact:** Zombie keys.
**Check:** Keep unless bottommost **and** no covering snapshot.

### 4.2 Seq zero wrong level
**Pattern:** Zero seq off bottommost → snapshot break.
**Check:** `is_bottommost` + lower overlaps.

### 4.3 Sub-compaction boundary dups
**Pattern:** Boundary key in two sub-jobs.
**Impact:** Dup SST keys.
**Check:** Exclusive-start; no multi-range keys.

### 4.4 Bad trivial move
**Pattern:** Metadata move with L(n+1) overlap or effective filter.
**Impact:** Stale tombstones / overlap / filter skip.
**Check:** One input, no target overlap, no effective filter (`None`/`is_noop`). Forced no-op only when filter cannot apply.

## 5. Iterators

### 5.1 Heap after direction switch
**Pattern:** Fwd→bwd without rebuild.
**Impact:** Miss/dup keys.
**Check:** Reposition children + rebuild heap.

### 5.2 Prefix overshoot
**Pattern:** Bad prefix end test.
**Impact:** Keys outside prefix.
**Check:** Byte prefix match, not full-key lex of synthetic end alone.

### 5.3 Snapshot filter gap
**Pattern:** Newer invisible version causes skip of all versions.
**Impact:** False miss at snapshot.
**Check:** Scan versions until latest with `seq <= snapshot_seq`.

## 6. Unsafe

Live set: skiplist, group-commit/file lock, SST fadvise — inventory via search; see `patterns/unsafe-audit.md`.

### 6.1 Skiplist node lifetime
Raw nexts live while readers hold pointers → need arena (no free while live) or epoch.
### 6.2 Block ownership regression
Borrowed block without Arc across evict — current is `Arc<Vec<u8>>`.
### 6.3 Unaligned integers
Packed fields: `from_le_bytes` / `read_unaligned`, not plain typed loads.

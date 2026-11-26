# MMDB Storage-Engine Correctness Patterns

LSM-Tree / Rust v5 behavior checks. Load before review or debugging.
Describe findings with the registry fields in `review-core.md`.

## 1. Concurrency & atomicity

### 1.1 View-before-sequence & SuperVersion usage
**Pattern:** Reading `committed_sequence` before pinning `SuperVersion` (GC race), or using a point-in-time `SuperVersion` for latest-state mutation/compaction picks without revalidating under `DB::inner`.
**Where:** [Read view and visibility](../../src/db/read.rs) (`point_lookup`, `grouped_point_lookup`, `scan`, `resolve_read_sequence`) and [sorted ingestion](../../src/db/ingest.rs).
**Impact:** Snapshot/point read misses compacted versions, or compaction acts on superseded files.
**Check:** After pinning the view, latest reads take the minimum of the committed sequence and that view's `ingest_ceiling`. Explicit snapshots supply a DB-checked sequence. A grouped lookup uses the same pair for every slot. A reader paused after pinning an old view must still observe the pre-ingest state after sorted ingestion and a subsequent write. Latest-state mutations revalidate their own inputs under `DB::inner`.

### 1.2 Group-commit ordering & post-commit boundary
**Pattern:** WAL vs MemTable phase reorder, `committed_sequence` stored before all MemTable inserts finish, or post-commit auto-freeze (`submit_active`) / partial-group `append_error` overwriting already-committed callers' results with `Err`.
**Where:** `db.rs` (`enqueue_write`, `write_batch_group`, `submit_active`).
**Impact:** Linearizability break, or callers told a durable/visible write failed.
**Check:** Validate the batch before pressure checks. A full immutable queue or L0 at the slowdown or stop threshold can return `Busy` to a nonempty `no_slowdown` write. The leader rechecks immutable capacity for each queued nonblocking member before range-stripe and same-key admission. Trace admitted sequences through WAL append and flush/sync, ordered memtable insertion and `committed_sequence.store(Release)`. Advance the WAL watermark on a successful sync. When a later append or post-commit auto-freeze fails, preserve each already-committed caller's success. [Group and WAL oracles](../../src/db/flush.rs)

### 1.3 Async `FlushQueue`, claims, & fail-stop wake ordering
**Pattern:** Popping `immutable_memtables` before `confirm_manifest_durable`; signaling `compaction_notify` while still holding `CompactionClaim` on new L0 SSTs; or waking `FlushQueue` waiters (`flush_queue.fail`) before storing `bg_error` + `has_bg_error` (`Release`).
**Where:** `db/flush.rs`, `db.rs` (`set_bg_error`, compaction `set_error`, `LevelRewrite`).
**Impact:** Premature WAL/memtable retirement, stalled L0 compaction wakeup, or woken threads observing `Ok(...)` after an `ErrorKind::Background` return.
**Check:** Retain oldest immutable memtable and its WAL until `confirm_manifest_durable` succeeds; `drop(claim)` before notifying `compaction_notify`; store `bg_error` + `has_bg_error` (`Release`) before `flush_queue.fail`.

### 1.4 Lifetime & callback re-entrancy guards
**Pattern:** Dropping `Arc` backing (`MemTablePin`, `BlockRef`, `SuperVersion`) while slices are live, retaining `TableCache`/`LOCK` inside `ArcSnapshot`, or invoking mutating DB methods / `close()` from `CompactionFilter` / `BlockPropertyCollector` callbacks.
**Where:** `pinned.rs`, `db/snapshot.rs`, `db.rs` (`SstBuilds`).
**Impact:** Dangling slice, leaked `LOCK`/FDs after `close()`, or self-deadlock.
**Check:** Non-empty `PinnedSlice` retains `Arc<MemTable>` / `Arc<Vec<u8>>` (zero-len uses `LazyValue::Inline`); `SnapshotLease` holds `Weak<ReadState>`; `SstBuilds::contains_current_thread()` rejects callback mutations/close with `InvalidArgument`.

## 2. Data integrity

### 2.1 Sequence & `DBHeader` geometry validation
**Pattern:** Non-contiguous sequence reservation, or MANIFEST recovery/repair running before `DBHeader` (`"MMDB"`, v5, `levels` `2..=16`, `prefix_len`) validation.
**Where:** `db.rs`, `manifest/version_set.rs`, `types.rs`.
**Impact:** Snapshot anomalies, or corrupting/truncating a store on format/geometry mismatch.
**Check:** A write batch reserves one sequence per operation. Sorted ingestion assigns one fresh sequence to all rows in a nonempty input. On reopen, `DBHeader::read` + geometry check run before `VersionEdit` replay, `open_append_truncated`, or WAL recovery. Writable locking can create `LOCK` before header validation. Read-only open adopts stored geometry.

For `ingest_sorted`, check strict input order and revalidate the complete
user-key interval against active/immutable memtables, SST point bounds
and range-tombstone extents at installation. Before MANIFEST installation,
failure discards this job's staged SSTs. After installation, a failed
durability confirmation must retain the referenced files and fail-stop.
A successful ingest caps the old views before publishing the new view and
sequence. [Ingestion and its race tests](../../src/db/ingest.rs)

### 2.2 `InternalKey`, `FileMetaData`, & directory↔leaf bounds
**Pattern:** Unvalidated `< 8`-byte or inverted `FileMetaData` bounds in `VersionSet`, or SST v2 partitioned index/`rangedel` leaves whose start/end keys disagree with `TableReader::partitions` / `range_partitions`.
**Where:** `manifest/version_set.rs` (`open_table`, `validate_level_disjointness`), `sst/table_reader/mod.rs` (`index_leaf_block`, `find_data_block`, `covering_sequence`, `visit_ranges`).
**Impact:** Binary-search point reads/iterators silently return `Ok(None)` or compaction drops partitions.
**Check:** Validate `decode_internal_key` + `smallest_key <= largest_key` (no empty-key skips at L1+), and cross-check both index leaves and `rangedel` leaves against their directory partition bounds.

### 2.3 CRC, footer adjacency, & compression caps
**Pattern:** CRC omits record/block type byte, footer index handle points at an earlier directory page, or LZ4/Zstd allocates uncapped decoded buffers before limit check.
**Where:** `wal/{record,writer,reader}.rs`, `sst/{format,table_builder,table_reader}`.
**Impact:** Silent truncation/corruption acceptance or OOM on damaged block headers.
**Check:** Verify WAL CRC over fragment type followed by payload and SST CRC over payload followed by the compression byte. The head index page ends at `file_size - FOOTER_SIZE`. Backward directory offsets strictly decrease and decoded blocks are capped at `64 MiB`.

### 2.4 Block restarts & shortest separators
**Pattern:** Bad restart offset, mid-block decode error treated as clean EOF, or `shorten_separator` shortening across equal user keys.
**Where:** `sst/block.rs`, `sst/table_builder.rs`.
**Impact:** Corrupted tail hidden as EOF, or older snapshot versions skipped across blocks.
**Check:** Checked `num_restarts` math; mid-block errors set `BlockIterator::error()`; `shorten_separator` shortens only when `lo < prefix < hi` strictly as user keys.

### 2.5 Half-open `[start, end)` & range-tombstone stripes
**Pattern:** Inclusive-end comparison, missing L0/L1+ covering tombstones whose `end > meta.largest_key`, or exceeding `META_BLOCK_HARD_LIMIT` under many snapshots.
**Where:** `db/read.rs`, `sst/table_builder.rs`, `iterator/range_del.rs`, `compaction/leveled.rs`.
**Impact:** Over/under-deletion or unreadable `rangedel` meta blocks.
**Check:** Fold covering tombstones from every `has_range_deletions` file at a level *before* `[smallest_key, largest_key]` point pruning; enforce `admit_range_stripes` (`MAX_RANGE_STRIPES`).

## 3. Resources

### 3.1 `PendingSsts`, WAL unlinks, & `DirSynced`
**Pattern:** Unlinking `old_wal` or compaction inputs before `DirSynced` + `confirm_manifest_durable`, or failed SST builds deleting sibling jobs' files.
**Where:** `manifest/file_numbers.rs`, `manifest/version_set.rs`, `db/flush.rs`, `compaction/leveled.rs`.
**Impact:** Unrecoverable missing SSTs after crash or sibling job corruption.
**Check:** Per-job `PendingSsts` unlinks only its own uncommitted numbers; `DirSynced` and `confirm_manifest_durable` serialize `fsync` failures with `VersionSet::poisoned` before any unlink.

### 3.2 L0 first-block pin & `BlockCachePool` accounting
**Pattern:** L0 pin leaked across trivial move / `LevelRewrite` / `detach`, or > `u32::MAX` block under-weighed in moka.
**Where:** `cache/block_cache.rs`, `sst/table_reader/mod.rs`.
**Impact:** Unbounded pinned memory or skewed pool eviction.
**Check:** Every L0 removal calls `unpin_file`/`invalidate_file`; `detach` serializes with `insert_pinned` on `pin_updates`; `u32::try_from(capacity)` overflow invalidates stale key.

### 3.3 MemTable slab & same-key metadata accounting
**Pattern:** Dedicated `> 256 KiB` slab clobbering active slab cursor, uncounted slab tail/range-del capacity, or unbounded single-key version run overflowing SST index/range leaf limits.
**Where:** `memtable/{mod,skiplist,skiplist_impl}.rs`, `types.rs`, `db.rs`.
**Impact:** Arena waste/underflow, late flush, or flush failing `META_BLOCK_HARD_LIMIT`.
**Check:** Dedicated slabs leave active cursor intact; full reserved slab + `MemRangeTombstone` bytes hit `approximate_size`; `same_key_metadata` matches across `WriteBatch`, `metadata_buckets`, and `admit_same_key_runs`.

## 4. Compaction

### 4.1 Bottommost GC, sequence zeroing, & `keep_from`
**Pattern:** Dropping tombstones or zeroing sequences when deeper levels overlap (`range_partition_extents` missed), or mismatching `keep_from` vs `entry_seq < oldest_snapshot_seq`.
**Impact:** Deleted data resurrected or surviving stripe inverted against zeroed point sequence.
**Check:** Close overlaps over resident `range_partition_extents()` across all deeper levels; keep `keep_from(oldest_snapshot_seq)` paired with strict `< oldest_snapshot_seq` zeroing.

### 4.2 Sub-compaction & SST-split clipping
**Pattern:** Non-monotonic target file order breaking `compute_split_points`, `MAX_SEQUENCE_NUMBER` tombstone at split key skipped by both sub-jobs, or `clip_at` leaving gaps across SST cuts.
**Check:** Keep target inputs in level order; seek sub-jobs with `(lo, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion)`; tile `[begin, K_split)` and `[K_split, end)` at user-key boundaries.

### 4.3 Typed merge errors & snapshot-raced filters
**Pattern:** Reclassifying `MergingIterator` `Corruption`/`Io` as `ErrorKind::Background` on synchronous `compact()`, or installing `filter_mutated` output after a snapshot registered concurrently.
**Check:** Preserve `e.clone().context(...)` from `MergingIterator::error()`; discard `filter_mutated && live_snapshots_present` outputs without deleting live trivial-move inputs.

## 5. Iterators

### 5.1 Permanent error latching & lending `Err` precedence
**Pattern:** Re-seek or direction switch clearing a latched decode/CRC error, or `step_next`/`step_prev` returning `Ok(Some(...))` when lookahead into the next block failed.
**Check:** Fuse all movement/seek methods once `has_failed()`; `lend_current(true)` checks `error()` before yielding; backward failure halts before exposing older versions.

### 5.2 Upper-bound partition leaf isolation & post-MVCC filters
**Pattern:** `TableIterator::seek` / `seek_for_prev` / `load_next_block` calling `select_partition` / `ensure_index` before checking `iterate_upper_bound` / exhaustion, or running `BlockPropertyFilter::key_matches` before MVCC resolution.
**Check:** Check `upper` and `index_pos == usize::MAX` before index-leaf I/O; evaluate `IterConfig::matches` post-MVCC in both forward and reverse.

## 6. Unsafe & Provenance

Live set: `memtable/{skiplist_impl,mod,skiplist}.rs`, `db.rs`, `sst/table_reader/mod.rs`, and `#[cfg(test)]` `test_alloc.rs` — derive via `rg '\bunsafe\b' src` and follow `patterns/unsafe-audit.md`.

### 6.1 Stacked/Tree Borrows provenance
Keep raw `NonNull<u8>` in `Slab` (no `Box`/`Vec` `Unique` retag); derive `node_kv` slices from `tower(node)` rather than `&NodeHeader`; project individual raw fields on queued `WriteRequest`s (`&(*ptr).batch`, `(*ptr).result = ...`, never `&mut *ptr`).
### 6.2 `PinnedSlice` & cursor lifetime
`MemTablePin` and `MemTableCursorIter` retain `Arc<MemTable>`; `BlockRef` retains `Arc<Vec<u8>>`; zero-length values use `LazyValue::Inline` without pinning slabs/blocks.

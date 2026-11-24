# MMDB v5 Structural & Wire-Format Roadmap

Status: proposed major-release design (`v5.0.0`), 2026-11-24. Not yet implemented; current `v4.3` contracts remain active until v5 development begins.
Baseline: `cecc9b3` (`v4.3.15`), co-designed with `vsdb` v18 ([vsdb v18 roadmap](../../../vsdb/docs/proposals/v18-roadmap.md)).

## Scope boundary: what genuinely requires v5?

MMDB keeps all internal modules private and exports only the curated root surface in [src/lib.rs](../../src/lib.rs#L58-L71). Several `v4.3` releases already landed major internal wins compatibly (`TableReader::find_data_block` using `cached_index_entries` at [table_reader/mod.rs](../../src/sst/table_reader/mod.rs#L480), `Block::seek_by_reuse`, `ArcSwap` block-pin reads at [block_cache.rs](../../src/cache/block_cache.rs#L210), `Version` per-level `Arc` sharing at [version.rs](../../src/manifest/version.rs#L24-L26), `Cow::Borrowed` uncompressed SST writes at [table_builder.rs](../../src/sst/table_builder.rs#L508-L528), and `RateLimiter`'s zero-rate early return at [rate_limiter.rs](../../src/rate_limiter.rs#L145-L148)). Remaining purely internal tuning stays in `v4.x`.

`v5.0.0` is justified by **six structural changes that either require breaking public-API / on-disk format contracts or co-design core representations with `vsdb` v18**:

1. **Public safety & option-extensibility breaks**: `#[non_exhaustive]` options, borrowed `ReadOptions<'a>` bounds, live DB-bound `Snapshot` handles in `ReadOptions` (closing the `db.snapshot().read_options()` temporary-drop MVCC reclamation hazard), `DurabilityMode` enum (eliminating invalid `sync: true, disable_wal: true`), and **key-only MVCC-safe `BlockPropertyCollector`** (fixing a present value-filter MVCC resurrection flaw);
2. **Zero-copy & zero-fence engine primitives**: `DB::get_pinned` (`PinnedSlice`), value-free `DB::contains_key`, seal-aware `DB::sync_wal`, typed `Option<&Error>` on iterators, and fallible lending cursor steps;
3. **Flat WAL-native `WriteBatch` + scatter-gather prefixed writes**, eliminating per-entry `Vec` allocations and `encode_wal_record` copying under `DB::inner` while replacing `WriteBatchWithIndex`'s owned-tuple accessor;
4. **Inline-arena variable-height `MemTable`**, co-designed with `PinnedSlice` to cut small-KV node overhead by ~50% and drop arena slabs without per-entry `Vec` frees;
5. **Non-blocking FIFO immutable-MemTable flush pipeline** (`max_immutable_memtables` activated with WAL-seal rotation, newest-first read / oldest-first install, and generation-gated `flush`/`close`) + unlocked `compact*` / `lazy_delete` sweeps;
6. **SST v2 & `MANIFEST` v2 on-disk formats**: 64B cache-line-blocked Bloom filters (`filter.format = 2`), internal-key-valid shortest separators + pre-fragmented partitioned metadata blocks, and crash-atomic `MANIFEST` `DBHeader` (`format_version = 5`, exact `num_levels`, `prefix_len`).

*(Note: purely additive methods such as `get_pinned`, `contains_key`, `sync_wal`, and `put_prefixed`, or internal `fdatasync`/arena/flat-batch representations, may be prototyped in `v4.x` ahead of the `v5.0.0` signature and wire-format cutover.)*

### Consolidated v5 feature matrix

| # | Subsystem | Planned v5 change | Audit / cross-repo debt closed | Primary win | Break scope |
|---|-----------|-------------------|--------------------------------|-------------|-------------|
| 1 | Options & MVCC safety | `#[non_exhaustive]` options, borrowed `ReadOptions<'a>` bounds + live `SnapshotRef<'a>`, `DurabilityMode` enum, key-only `BlockPropertyCollector` | Fixes `snapshot().read_options()` use-after-drop, value-filter MVCC resurrection, and exhaustive-struct lock-in ([read-only-mode.md](read-only-mode.md#L83-L85)) | Eliminates MVCC snapshot/filter footguns & bound clones; unlocks minor-version options | Public API |
| 2 | Read, fence, & iter API | `DB::get_pinned` (`PinnedSlice`), `DB::contains_key`, seal-aware `DB::sync_wal`, `Option<&Error>` on iterators, fallible lending cursor | Removes `vsdb` `META_KEY_WAL_FENCE` deletes, `get`-for-`contains_key` value copies, and lossy `Option<String>` iterator errors | 0 result-value copies on cache/memtable hits; 0 WAL writes on `sync_wal`; typed iterator errors | Public API |
| 3 | `WriteBatch` & WAL path | Flat contiguous WAL-frame `WriteBatch` + `put_prefixed` / `delete_prefixed` / `delete_range_prefixed` + raw-place seq patch | Eliminates `Vec<WriteBatchEntry>` per-entry allocs + `encode_wal_record` copy under `inner` lock | 1 buffer per batch; 0 prefix+key concat allocs in `vsdb`; $O(1)$ sequence patch | Public API (`WriteBatchWithIndex`) |
| 4 | `MemTable` (`skiplist`) | `#[repr(C, align(8))]` variable-height tower + inline `[key \| val]` in raw-pointer slabs | Fixes 152B fixed-tower bloat on small KVs and per-node `Vec` destructor walks | ~2–3× higher small-KV write-buffer density; 0 per-entry `Vec` allocs | Internal (co-designed with `PinnedSlice`) |
| 5 | Flush, WAL sync, & sweeps | Newest-first read / oldest-first install `max_immutable_memtables` FIFO, sealed WAL rotation, `sync_data`, `l0_install_epoch`-guarded unlocked sweeps | Activates `max_immutable_memtables` ([options.rs](../../src/options.rs#L24-L30)); removes `write_queue` hold across `compact*` | Removes synchronous 16–64 MiB writer-group flush stalls and snapshot/writer sweep stalls | Behavioral + `DbOptions` |
| 6 | SST v2 & `MANIFEST` v2 | 64B cache-line-blocked Bloom, `InternalKey` shortest separators, pre-fragmented partitioned range-del/index, atomic `MANIFEST` `DBHeader` | Narrows `[MEDIUM] SST` metadata limit & closes `[LOW] options: num_levels` ([audit.md](../audit.md#L21-L35)) | 1 cache-line per filter probe; -50%+ resident index RAM; self-describing store geometry | On-disk (`SST` + `MANIFEST`) |

---

## 1. Public configuration, MVCC snapshot lifetimes, and key-only block properties

Source: [options.rs](../../src/options.rs), [db.rs](../../src/db.rs#L4522-L4550), [table_reader/iterator.rs](../../src/sst/table_reader/iterator.rs#L181-L192), [iterator/db_iter.rs](../../src/iterator/db_iter.rs#L568-L590).

### Current contract hazards and friction

1. **Dangling `Snapshot` sequence footgun (`db.snapshot().read_options()`):**
   [`Snapshot::read_options`](../../src/db.rs#L4538-L4543) borrows `&self` only for the duration of the call and copies a raw `u64` into `ReadOptions::snapshot: Option<SequenceNumber>` ([options.rs](../../src/options.rs#L259)). Writing `let opts = db.snapshot().read_options();` immediately drops the temporary `Snapshot`, unregistering the sequence so concurrent compaction can reclaim historical versions before `opts` is used — and nothing stops passing DB `A`'s sequence number into DB `B`.
2. **MVCC resurrection via value-inspecting `BlockPropertyCollector`:**
   [`BlockPropertyCollector::add(&mut self, key: &[u8], value: &[u8])`](../../src/options.rs#L380) exposes `value` (while `Deletion` tombstones are omitted or carry empty values), and [`TableIterator::seek`](../../src/sst/table_reader/iterator.rs#L312) skips physical data blocks **per SST before `DBIterator` runs cross-level MVCC deduplication**. If `k = "red"` lives in L1 and a newer `k = "blue"` (or `Deletion`) lives in L0 inside a block with no `"red"` values, a filter skipping blocks without `"red"` skips the L0 block and **resurrects stale `k = "red"` from L1**.
3. **Contradictory `WriteOptions` flags, exhaustive option structs, and owned read bounds:**
   - `WriteOptions { sync: true, disable_wal: true }` ([options.rs](../../src/options.rs#L309-L316)) is representable even though `disable_wal` suppresses WAL writes and `sync` performs no fsync when all group members disable WAL ([db.rs](../../src/db.rs#L3616-L3630)).
   - `DbOptions`, `ReadOptions`, and `WriteOptions` lack `#[non_exhaustive]`, making every new option a SemVer major break ([read-only-mode.md](read-only-mode.md#L83-L85)), while `ReadOptions::{iterate_lower_bound, iterate_upper_bound}` (`Option<Vec<u8>>`) force owned `Vec` allocations when callers hold stack/slice bounds.
   - `DbOptions::pin_l0_filter_and_index_blocks_in_cache` ([options.rs](../../src/options.rs#L90-L96)) is explicitly documented as misnamed (index/filter blocks live on `TableReader`, never in `block_cache`; the flag warms `cached_index_entries` and pins each L0 file's first data block).

### v5 design

1. **Live, DB-verified `SnapshotRef<'a>` + `ArcSnapshot`:**
   - Tag each open `DB` with a process-unique `db_id: u64`. Expose borrowed `Snapshot<'a>` and owned `ArcSnapshot` (holding `Arc` to the snapshot list + `db_id`, without pinning `DB` if `close` semantics stay explicit, or pinning read state cleanly), and change `ReadOptions<'a>::snapshot` to `Option<SnapshotRef<'a>>`.
   - Compaction cannot reclaim a snapshot's versions while `ReadOptions<'a>` exists (compile-time lifetime guarantee!), and passing a snapshot from another `DB` (`snapshot.db_id != self.db_id`) returns `Err(ErrorKind::InvalidArgument)`. Add convenience `snapshot.get(key)`, `snapshot.get_pinned(key)`, `snapshot.contains_key(key)`, and `snapshot.iter*()`.
2. **Key-only MVCC-safe `BlockPropertyCollector`:**
   - Change `BlockPropertyCollector::add(&mut self, user_key: &[u8])` to receive **only `user_key`** (invoked for both `Value` and `Deletion` entries in the data block). Because every SST block's key set is monotonic and every version of `user_key` shares the same `user_key`, skipping a block whose `user_key` summary cannot match the query predicate skips *all* versions in that block only when `user_key` itself is out-of-predicate — making physical block skipping **100% MVCC-sound across overwrites and point deletions**. Value-dependent predicates belong in post-MVCC caller filtering.
3. **`DurabilityMode`, `#[non_exhaustive]`, borrowed `ReadOptions<'a>`, and honest naming:**
   - Replace `WriteOptions::{sync, disable_wal}` with `pub enum DurabilityMode { WalAsync, WalSync, DisableWal }`.
   - Add `#[non_exhaustive]` + builder methods to `DbOptions`, `ReadOptions<'a>`, and `WriteOptions` (`pub iterate_lower_bound: Option<&'a [u8]>`, `pub iterate_upper_bound: Option<&'a [u8]>`). Internally, share a single `Arc<IterBounds>` across `DBIterator` and its `LevelIterator` / `TableIterator` sources instead of cloning bound `Vec`s per level ([db.rs](../../src/db.rs#L2007-L2096)).
   - Rename `pin_l0_filter_and_index_blocks_in_cache` to `pin_l0_first_block_and_warm_index: bool` (preserving both behaviors explicitly).

---

## 2. Zero-copy `PinnedSlice`, value-free `contains_key`, seal-aware `sync_wal`, and typed iterator cursors

Source: [db.rs](../../src/db.rs#L1711-L1758), [iterator/db_iter.rs](../../src/iterator/db_iter.rs#L19-L70), [iterator/bidi_iter.rs](../../src/iterator/bidi_iter.rs#L111-L128), [sst/block.rs](../../src/sst/block.rs#L479), [error.rs](../../src/error.rs).

### Current limitations

1. **Forced result-value copy on `DB::get` and `contains_key`:**
   `DB::get_with_options` copies the matched value out of `MemTable` or `BlockCache`'s `Arc<Vec<u8>>` even when `vsdb` only binary-searches a slotted B+ tree page (`NodeView`), computes vector distance, or checks key existence (`MapxRaw::contains_key`).
2. **No native `DB::sync_wal`:**
   `vsdb` currently issues `delete_with_options(sync=true, META_KEY_WAL_FENCE)` to fence prior `WalAsync` commits without flushing memtables.
3. **Stringified iterator errors:**
   While `BlockIterator` already retains `Option<Error>` ([sst/block.rs](../../src/sst/block.rs#L479)), `TableIterator`, `LevelIterator`, `MergingIterator`, `DBIterator::error`, and `BidiIterator::error` stringify failures into `Option<String>`, losing `ErrorKind` and `source()`.

### v5 design

1. **`PinnedSlice` (`DB::get_pinned` / `DB::get_pinned_with_options`):**
   - `pub struct PinnedSlice` implements `Deref<Target = [u8]>`, `AsRef<[u8]>`, and `into_vec(self) -> Vec<u8>`, backed by `(Arc<Vec<u8>>, offset: u32, len: u32)` for SST blocks (both cached and `fill_cache = false` reads already wrap `Arc<Vec<u8>>` at [table_reader/mod.rs](../../src/sst/table_reader/mod.rs#L690)) or `(Arc<MemTable>, NonNull<u8>, len: usize)` (`Send + Sync` proven in §4) for memtable hits — eliminating the result-value copy (stack-buffering short internal seek keys removes the remaining point-get scratch `Vec` for keys `<= 120`B).
   - **Explicit retention warning:** Document that holding a `PinnedSlice` retains its backing `Arc<Vec<u8>>` block or `Arc<MemTable>` (~64 MiB arena) even after compaction, cache eviction, or `DB::close()`; callers retaining values across long lifetimes must call `.into_vec()`.
2. **Value-free `DB::contains_key` / `DB::contains_key_with_options`:**
   - Execute full `SuperVersion` MVCC + range-tombstone visibility resolution inspecting only internal-key trailers (`ValueType::Value` vs `Deletion`), returning `Result<bool>` with zero value copies.
3. **Cross-rotation-safe `DB::sync_wal(&self) -> Result<()>`:**
   - **Critical async-flush interaction:** Once §5 allows background memtable flushes, an earlier `WalAsync` write may live in a rotated WAL `W0` whose SST/MANIFEST install is still in flight while `active_memtable` uses empty `W1`! Because §5 seals (`flush` + `sync_data`) `W0` **before** publishing `W1` for appends, any committed write in a rotated WAL is already durable on disk; `sync_wal()` therefore needs only ensure the current active `WalWriter` is `sync_data()`'d up to the target `committed_sequence` (bypassing `write_batch_group`'s `total_ops == 0` early return at [db.rs](../../src/db.rs#L3573-L3575) so fence-only requests coalesce with active group-commit leaders and honor `set_bg_error` fail-stop on sync failure).
4. **Typed `Option<&Error>` and fallible lending `step_next` / `step_prev`:**
   - Propagate typed `Option<Error>` end-to-end through `TableIterator`, `LevelIterator`, `MergingIterator`, `DBIterator::error(&self) -> Option<&Error>`, and `BidiIterator::error(&self) -> Option<&Error>`.
   - Add lending cursor methods `step_next(&mut self) -> Result<Option<(&[u8], &[u8])>>` and `step_prev(&mut self) -> Result<Option<(&[u8], &[u8])>>` (backed by an internal reusable key/value slot across memtable and SST sources). **Strict error precedence:** if lookahead during deduplication or source advancement encounters an `Error` on the same call that resolved a candidate row, `step_next` / `step_prev` latches the error and returns **`Err(err.clone())` immediately** (never `Ok(Some(row))`), matching `vsdb`'s post-step `CheckedBidiIter` invariant ([vsdb mmdb.rs](../../../vsdb/core/src/common/engine/mmdb.rs#L629-L657)).

---

## 3. Flat WAL-native `WriteBatch` and raw-place group-commit safety

Source: [types.rs](../../src/types.rs#L288-L455), [db.rs](../../src/db.rs#L460-L475), [db.rs](../../src/db.rs#L3409-L3680), [db.rs](../../src/db.rs#L4225-L4248).

### Current costs

`WriteBatch` ([types.rs](../../src/types.rs#L289-L298)) allocates per-entry `key: Vec<u8>` (plus `value: Vec<u8>` on puts/range-deletes), and [`encode_wal_record`](../../src/db.rs#L4225-L4248) allocates and copies the entire batch into a second `Vec<u8>` while holding `DB::inner`.

### v5 design

1. **Contiguous WAL-record `WriteBatch` with checked admission:**
   - Store `buf: Vec<u8>` laid out directly as `[seq_placeholder: u64 LE (8B) | count: u32 LE (4B) | entries...]`, plus `invalid: Option<Error>`.
   - Provide `put`, `delete`, `delete_range` (retaining raw `delete_range(begin, end)` for cross-prefix wipes such as `vsdb`'s `[prefix, prefix_successor(prefix))`), **plus** scatter-gather `put_prefixed(prefix, key, value)`, `delete_prefixed(prefix, key)`, and `delete_range_prefixed(prefix, begin, end)`.
   - Perform checked length arithmetic (`prefix.len().checked_add(key.len())`, charging `2 * prefix.len()` on `delete_range_prefixed`), `MAX_USER_KEY_SIZE` / `MAX_WRITE_ENTRY_SIZE` / `MAX_RANGE_DELETE_SIZE` validation, and `count <= u32::MAX` checks **before** narrowing to `u32`: if violated, latch `self.invalid` (cleared on `WriteBatch::clear()`), and check `self.invalid` in `write_batch_inner` **before** the `batch.is_empty()` fast return ([db.rs](../../src/db.rs#L3420)).
2. **Preserving `WriteRequest` raw-pointer aliasing safety ([db.rs](../../src/db.rs#L460-L469)):**
   - Waiting writer threads may wake spuriously and read `(*req_ptr).done` under `write_queue` while the leader runs unlocked. Forming `&mut WriteRequest` in `write_batch_group` would violate Stacked/Tree Borrows!
   - Instead, patch `first_seq` strictly through a disjoint raw field projection `unsafe { WriteBatch::patch_seq_raw(&raw mut (*req_ptr).batch, first_seq) }` (writing only `buf[0..8]` and reading `&buf[..]` via `&raw const (*req_ptr).batch`), preserving the field-split aliasing proof and per-request partial-append results ([db.rs](../../src/db.rs#L3666-L3680)).
3. **Redesigned `WriteBatchWithIndex`:**
   - Store `(entry_offset: usize, write_pos: u64)` in `BTreeMap` (keyed by user key slice/owned key for $O(\log N)$ point lookup and ordered merge) and range-tombstone `(entry_offset, write_pos)` indices into `batch.buf`, replacing `pub fn range_tombstones(&self) -> &[(Vec<u8>, Vec<u8>, u64)]` ([types.rs](../../src/types.rs#L430)) with an iterator yielding borrowed `(&[u8], &[u8], u64)` triples while preserving exact `write_pos` temporal overlay semantics across mixed puts, deletes, and range deletes.

---

## 4. Inline-arena variable-height `MemTable` (`unsafe` layout & splice specification)

Source: [memtable/skiplist_impl.rs](../../src/memtable/skiplist_impl.rs#L43-L265), [memtable/mod.rs](../../src/memtable/mod.rs#L75-L175).

### v5 design

1. **Checked `Layout` composition and raw-pointer slabs (Miri-clean with `PinnedSlice`):**
   - Replace `Vec<u8>` slabs with raw `*mut u8` allocations (`std::alloc::alloc(slab_layout)`, deallocated in `Arena::drop` via `std::alloc::dealloc`) so inserting later nodes **never** creates a `&mut [u8]` reference spanning already-published bytes that a concurrent `PinnedSlice` is dereferencing (`Send + Sync` on `PinnedSlice` holds `Arc<MemTable>`, keeping every slab live until all readers drop).
   - Lay out each node using `#[repr(C, align(8))] struct NodeHeader { key_len: u32, val_len: u32, height: u8, _pad: [u8; 7] }` (16B, 8-byte aligned), immediately followed by `tower: [AtomicPtr<NodeHeader>; height]` (8-byte aligned), `internal_key: [u8; key_len]`, and `value: [u8; val_len]` (computed via `Layout::extend` with checked overflow, allocating dedicated oversized slabs when `node_size > ARENA_MAX_BLOCK`).
2. **Exact two-step single-writer splice ([skiplist_impl.rs](../../src/memtable/skiplist_impl.rs#L252-L265)):**
   - Initialize `NodeHeader`, `internal_key`, and `value` bytes completely.
   - For each `level in 0..height`: read `old_next` (`self.head[level].load(Relaxed)` if `prev[level].is_null()`, else `(*prev[level]).tower[level].load(Relaxed)`), store `node.tower[level].store(old_next, Relaxed)`, and publish `pred_slot.store(node, Ordering::Release)`. Update `tail` iff `level == 0 && old_next.is_null()`.
3. **Accurate memory accounting:**
   - Account exact arena node bytes + separate range-tombstone endpoint/index bytes ([memtable/mod.rs](../../src/memtable/mod.rs#L77-L165)), and check `write_buffer_size` **before** enqueuing oversized groups when active memtable is already full so batch overshoot stays bounded. Delete `all_nodes: Vec<*mut Node>` (`MemTable::drop` frees ~64 raw slabs + the range-tombstone `Vec`/segment tree).

---

## 5. Non-blocking FIFO flush pipeline, sealed WAL rotation, and unlocked sweeps

Source: [options.rs](../../src/options.rs#L24-L30), [db.rs](../../src/db.rs#L266-L291), [db.rs](../../src/db.rs#L1740-L1750), [db.rs](../../src/db.rs#L2632-L2865), [db.rs](../../src/db.rs#L3130-L3185), [db.rs](../../src/db.rs#L3682-L3995), [tests/crash_recovery.rs](../../tests/crash_recovery.rs#L706-L745).

### Critical invariants verified against MMDB recovery & MVCC

1. **Seal rotated WALs before publishing the new WAL:**
   MMDB recovery rejects torn tails on any WAL older than the newest non-empty WAL ([crash_recovery.rs](../../tests/crash_recovery.rs#L706-L745)). Therefore, rotating `WAL_old -> WAL_new` must `flush()` + `sync_data()` `WAL_old` (and pre-create `WAL_new` + `sync_parent_dir` outside `inner.lock()`) **before** any writer appends to `WAL_new`. This costs only one `fdatasync` per `write_buffer_size` rotation and guarantees both torn-tail recovery safety and §2's `sync_wal()` correctness.
2. **Opposite reader vs installer order on `immutable_memtables`:**
   Point lookups ([db.rs](../../src/db.rs#L1740-L1750)) return the first match in `immutable_memtables` and therefore require **newest-first** order, whereas `MANIFEST` `log_number` installation requires **oldest-first** FIFO order! Store `immutable_memtables: VecDeque<Arc<MemTable>>`: `freeze_memtable` `push_front`s the newly frozen table (`SuperVersion` iterates front-to-back = newest-first), while the background flush worker flushes `back()` (oldest-first).
3. **Generation-gated durable completion & terminal-failure `close()`:**
   - Keep an immutable memtable in `immutable_memtables` (and hold its `flush_gen: u64` incomplete) until **both** `install_flush` **and** `confirm_manifest_durable` succeed (closing the window where `WAL`-disabled writes could be acknowledged by `flush()` before `MANIFEST` sync, [db.rs](../../src/db.rs#L3946-L3995)).
   - If the oldest flush or its `MANIFEST` sync fails, set `has_bg_error`, wake all waiting writers/`flush()`/`close()` callers, and **forbid installing any newer immutable or active memtable** — fixing [`DB::close`](../../src/db.rs#L3142-L3147) so `close()` checks `fail_stop_error()` *before* attempting `freeze_and_flush` and never advances `log_number` past a failed earlier WAL!
   - Fail fast in `write_batch_inner` when `WriteOptions::no_slowdown` is set and `immutable_memtables.len() >= max_immutable_memtables`.
4. **Unlocked sweeps without `write_queue` stalls or L0 file-number inversion:**
   - Replace holding `write_queue` across `compact()` / `compact_range()` ([db.rs](../../src/db.rs#L2635-L2658)) with an `active_maintenance_ops` counter + condvar checked by `close()`, so foreground writers can still enqueue while unlocked compaction I/O runs; move new-SST directory `sync_all()` out of `VersionSet::log_and_apply` (`inner.lock()`) into the unlocked build phase.
   - **L0 ordering guard ([db.rs](../../src/db.rs#L266-L282)):** Track `l0_install_epoch: u64` incremented on every memtable freeze and L0 flush install. An unlocked L0 rewrite (`force_merge_level` / sweep) picks only when `immutable_memtables.is_empty()`, records `l0_install_epoch`, and discards/retries its output if `l0_install_epoch` changed, `immutable_memtables` is non-empty, or a snapshot arrived during filter-mutating I/O ([db.rs](../../src/db.rs#L6428)) — preventing older L0 data from ever receiving a newer L0 position or file number than a concurrent flush.
5. **Distinct `WalWriter::sync_data()` for WAL appends:**
   - Add `WalWriter::sync_data()` (`BufWriter::flush()` + `File::sync_data()`) specifically for ordinary WAL append commits, while keeping full `WalWriter::sync()` (`File::sync_all()`) for `open_append_truncated` size truncation and `MANIFEST` syncs.

---

## 6. SST v2 and `MANIFEST` v2 on-disk formats

Source: [types.rs](../../src/types.rs#L46-L75), [sst/filter.rs](../../src/sst/filter.rs), [sst/format.rs](../../src/sst/format.rs), [sst/table_builder.rs](../../src/sst/table_builder.rs#L284-L410), [manifest/version_set.rs](../../src/manifest/version_set.rs#L160-L410), [audit.md](../audit.md#L21-L35).

### v5 wire specifications (`TABLE_MAGIC_V2`, `MANIFEST` v2)

1. **Aligned 64-byte cache-line-blocked Bloom filter (`FILTER_FORMAT_BLOCKED_64B = 2`):**
   - Store filter payload in a 64-byte-aligned buffer (`AlignedBytes`, 512 bits/block) for both key and prefix filters, identified by explicit metaindex format tag `2` with pinned hash/seed test vectors.
   - One 64-bit hash selects the 64B line via Lemire fastrange (`(h_hi * num_lines) >> 32`) and probes `k = 8` (`bloom_bits_per_key = 12` default, empirically ~0.57% FPR at +20% filter bytes) within that single cache line using odd step `(h_lo.rotate_left(15)) | 1` (`gcd(odd, 512) == 1` — 1 cache line per SST filter probe).
2. **Valid `InternalKey` shortest separators + independent-restart partitioned index:**
   - **Exact internal-key separator:** When closing data block `i` (`last_ikey`) before `next_first_ikey` with `last_uk < next_first_uk`, construct any shortened prefix `s` (`last_uk < s < next_first_uk`) via **`InternalKey::new(s, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion)`** — whose inverted trailer is `[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFD]` (NEVER `[0x00; 8]`, which has invalid `value_type = 255` and fails [`decode_internal_key`](../../src/types.rs#L207-L226)!). Keep `last_ikey` un-shortened when `last_uk == next_first_uk` and on the final **point** block of the SST (separating final point-index boundary from `FileMetaData.largest_key`, which may come from a tombstone-only SST at [table_builder.rs](../../src/sst/table_builder.rs#L284-L302)).
   - **Two-level partitioned index:** Split index entries into ~64 KiB leaf index blocks (allowing single oversized entries up to `MAX_DECOMPRESSED_BLOCK_SIZE` for 8 MiB keys), each starting with a **self-contained full key** (never cross-leaf delta-dependent) and read via `BlockCache` (`fill_cache = false` during compaction). Retain `first_key` inside leaf entries for zero-I/O deferred `TableIterator::seek` positioning, and drop duplicate raw root `Block` bytes once `TableReader` parses the compact top-level partition array. Note: if millions of versions of the *same* 8 MiB user key accumulate in one memtable (cannot split mid-user-key across SSTs), enforce per-memtable/batch metadata admission accounting so oversized same-key floods fail fast at write admission rather than at flush/recovery ([audit.md](../audit.md#L26-L30)).
3. **Pre-fragmented, overlap-free partitioned range-deletion blocks:**
   - Partitioning raw overlapping tombstones (`[a, z)@10` in leaf 0, `[m, n)@11` in leaf 1) by `begin_key` alone would miss `[a, z)@10` when probing `y`!
   - Instead, **fragment range tombstones at SST build time into disjoint elementary intervals `[begin_i, end_i)` (`end_i <= begin_{i+1}`)** carrying only snapshot-visible descending sequence numbers, and partition those disjoint intervals across ~64 KiB `rangedel` leaf blocks indexed by `max_end_key`. Because intervals are disjoint, any point probe `y` (including on tombstone-only SSTs, independent of point prefix-Bloom rejection) binary-searches the top-level `rangedel` index (`first partition with end > y`) and reads **at most one** `rangedel` leaf block! Update recovery and compaction to stream/merge elementary intervals without materializing redundant whole-file `Vec` clones.
4. **Crash-atomic `MANIFEST` `DBHeader` (`format_version = 5`, exact `num_levels` `2..=16`, `prefix_len: u32`):**
   - Fix [`VersionSet::create_with_cache`](../../src/manifest/version_set.rs#L168-L193) to write and `sync_manifest()` the initial snapshot edit containing `DBHeader` **before** publishing `CURRENT` (matching MANIFEST rotation order so a crash during creation never leaves `CURRENT` pointing to a headerless `MANIFEST` that recovery misclassifies as v4!).
   - Require `DBHeader` as the first record of every `MANIFEST` (including rotated snapshots), validate `2 <= num_levels <= 16` (`u32::try_from(prefix_len)`) **before** allocating `Version` level vectors or truncating `MANIFEST`, adopt persisted `(num_levels, prefix_len)` automatically in `open_read_only`, and require exact `num_levels` match on writable `DB::open` (unless an explicit level-count migration API is invoked). Also fix 32-bit `checked_mul` restart validation in [sst/block.rs](../../src/sst/block.rs#L79) ([audit.md](../audit.md#L21-L24)).

---

## Preserved invariants, migration, and staged rollout

- **Exact engine invariants:**
  - `ArcSwap<SuperVersion>` point reads avoid `DB::inner` (note active-memtable range-tombstone checks take a short `RwLock` read guard and first frozen lookup builds its segment tree);
  - WAL-before-MemTable publication holds for `WalAsync` and `WalSync` (`DisableWal` opts out explicitly);
  - WAL recovery tolerates a torn tail **only** on the highest non-empty WAL and fails closed on earlier WAL corruption;
  - `LOCK` is acquired (`LOCK_EX` writable, `LOCK_SH` read-only when `LOCK` exists; zero-syscall unlocked read-only open remains supported on immutable snapshots per [read-only-mode.md](read-only-mode.md#L20-L35)) **before** reading `CURRENT`/`MANIFEST`, and a v4 format mismatch fails closed immediately after `LOCK` without modifying `CURRENT`, `MANIFEST`, WAL, or SST bytes.
- **Stage 0 migration & rollout sequence:**
  1. **Stage 0 — Golden v4 fixtures & exporter:** Capture v4 fixtures (residual WAL with puts/deletes/range-deletes, non-default `num_levels`/`prefix_len`, multi-snapshot crossing range tombstones) and ship a version-pinned v4 reader/exporter (checking `.error()` on completion; `vsdb` v18 uses its own higher-level logical exporter for `VerMap`/`DagMap`/`SlotDex` fidelity).
  2. **Stage 1 — Public options, MVCC contracts, & typed errors:** Land `#[non_exhaustive]` options, `ReadOptions<'a>` borrowed bounds + `SnapshotRef<'a>`/`ArcSnapshot`, `DurabilityMode`, key-only `BlockPropertyCollector`, `Option<&Error>` on iterators + strict-precedence `step_next`/`step_prev`, and `DB::contains_key`.
  3. **Stage 2 — Flat `WriteBatch`, raw-slab `MemTable`, & `PinnedSlice`:** Land contiguous `WriteBatch` (`put_prefixed` family + raw-place sequence patching), 8B-aligned raw-slab variable-height skiplist (verified under Miri with concurrent `PinnedSlice` readers), and `DB::get_pinned`.
  4. **Stage 3 — Sealed-WAL FIFO flush, `sync_wal`, & `l0_install_epoch` sweeps:** Land sealed WAL rotation, `VecDeque` newest-first/oldest-first `max_immutable_memtables` FIFO with generation-gated `flush()` and fail-stop-first `close()`, `DB::sync_wal`, `WalWriter::sync_data()`, and `l0_install_epoch`-guarded unlocked sweeps — gated on deterministic two-generation visibility, oldest-flush/MANIFEST failure, and L0 flush-vs-sweep race tests.
  5. **Stage 4 — `MANIFEST` v2 & SST v2 freeze:** Land crash-atomic `DBHeader` (`format_version = 5`), 64B-aligned blocked Bloom filters, `[0x00..0xFD]` shortest separators + independent-restart partitioned index, and pre-fragmented partitioned `rangedel` blocks — gated on crossing-tombstone snapshot tests, 8 MiB same-key admission/flush tests, and multi-threaded writer p99 / RSS benchmarks.

# Cache Review Patterns

**Files:** `block_cache.rs`, `table_cache.rs`, `mod.rs`; tests: `shared_cache.rs`.

**Arch:** `BlockCachePool` = shared `moka::sync::SegmentedCache` (`lru_segments` scaled to keep ≥ 1 MiB admission per segment, up to 64 segments) + 16-shard reverse index (`FileOffsetsIndex`). Each `BlockCache` is a DB member view keyed `(member_id, file_number, offset)`. L0 data-block-0 pins live member-local in `ArcSwap<PinnedBlocks>` (off LRU); SST v2 point-index leaves and `rangedel` leaves use ordinary `BlockCache` admission (respecting `fill_cache`), while partition directories and 64B-aligned blocked Bloom filters live on `TableReader`. `TableCache` (`ArcSwapOption<Cache>`) coalesces opens via `try_get_with`, preserving typed `Error: Clone`.

## Invariants

**CA1 Member isolation** — same `file_number`/`offset` across DBs never share bytes.
Keys always carry `member_id`; IDs are monotonic and never reused after `detach()`.

**CA2 Pin lifecycle & unlocked L0 warm** — `prepare_first_block_pin` reads L0 index leaf 0 + data block 0 outside `DB::inner`; `publish_prepared_pin` admits leaf 0 to LRU (`cache.insert`) and pins data block 0 (`cache.insert_pinned`). Every L0 exit path (compaction, trivial move, `drain_l0`, `LevelRewrite`, `detach`) calls `unpin_file`/`invalidate_file` only after MANIFEST install succeeds. Replacement pins adjust `pinned.bytes` by exact delta.

**CA3 Pin snapshots** — `get()` loads immutable pins through `ArcSwap` without `pin_updates`. Insert, unpin, and detach serialize on `pin_updates`, load the latest snapshot under the lock, and publish `entries` + `bytes` atomically. In-flight readers finish via retained `Arc<Vec<u8>>`s (including `PinnedSlice` `LazyValue::BlockRef`).

**CA4 Reverse index & weight bounds** — register offset before `inner.insert`; prune on moka eviction listener + bulk invalidate using identical `shard_for` (`take_member` walks all shards). Allocations whose `capacity() > u32::MAX` bypass insertion and invalidate any prior cached entry at that key so moka `u32` weights never truncate.

**CA5 Detach** — idempotent (`SeqCst` swap); own member only; clears `PinnedBlocks` under `pin_updates` (re-checked on `insert_pinned`); batches unpinned invalidations before one `run_pending_tasks()`. Afterwards the view is a permanent uncached pass-through.

**CA6 TableReader & TableCache life** — eviction or `TableCache::close()` (`inner.store(None)` → `ErrorKind::DbClosed` on new misses) must not invalidate `Version` or iterator `Arc<TableReader>`s. `prewarm` runs unlocked before `log_and_apply_synced`; authoritative open errors happen inside `VersionSet::open_table`.

## Accepted races

Unpinned `insert` vs `invalidate_file` / `detach` is a cutoff rather than a barrier: a racing unpinned entry may remain until LRU eviction, bounded by pool capacity and unreachable because `member_id` and `file_number` are never reused.

## Checklist

- [ ] Keys include `(member_id, file_number, offset)` and member IDs never reuse
- [ ] L0 `prepare_first_block_pin` runs unlocked; `unpin_file` runs on every L0 removal/move
- [ ] Pin entry count and `pinned_bytes` stay exact across replace, unpin, and detach
- [ ] `u32::try_from(arc.capacity())` overflow invalidates stale key instead of under-weighing
- [ ] Shard selection + eviction pruning consistent across add/remove/take_file/take_member
- [ ] `detach()` idempotent, serialized with `insert_pinned`, siblings unaffected
- [ ] Capacity `0` disables both LRU and pinned insertion cleanly
- [ ] `TableReader` and `BlockRef` (`PinnedSlice`) `Arc`s outlive cache eviction/close

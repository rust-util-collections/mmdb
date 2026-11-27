# Cache Review Patterns

**Files:** `block_cache.rs`, `table_cache.rs`, `mod.rs`; tests: `shared_cache.rs`.

For store capacities, reader lifetimes, and pin ownership, see
[SST layout and cache ownership](../../architecture/sst-and-cache.md).

## Invariants

**CA1 Member isolation** — same `file_number`/`offset` across DBs never share bytes.
Keys always carry `member_id`; IDs are monotonic and never reused after `detach()`.

**CA2 Pin lifecycle & unlocked L0 warm** — `prepare_first_block_pin` prepares the first index leaf and data block of an eligible L0 SST outside `DB::inner`. `publish_prepared_pin` warms the index leaf when needed and publishes the data pin. Trace `unpin_file` or `invalidate_file` after an installed compaction, trivial move, L0 drain or level rewrite. Closing a member detaches its cache independently of a MANIFEST edit. Replacement pins adjust `pinned.bytes` by the exact payload delta.

**CA3 Pin snapshots** — `get()` loads immutable pins through `ArcSwap` without `pin_updates`. Insert, unpin, and detach serialize on `pin_updates`, load the latest snapshot under the lock, and publish `entries` + `bytes` atomically. In-flight readers finish via retained `Arc<Vec<u8>>`s (including `PinnedSlice` `LazyValue::BlockRef`).

**CA4 Reverse index & weight bounds** — register the offset before insertion. The eviction listener retains the registration on `RemovalCause::Replaced` because the successor occupies that key. On removal it prunes the entry using the same `shard_for` as `take_file`. A detach sweeps all shards. Allocations whose `capacity() > u32::MAX` bypass insertion and invalidate any prior cached entry at that key.

**CA5 Detach** — idempotent (`SeqCst` swap); own member only; clears `PinnedBlocks` under `pin_updates` (re-checked on `insert_pinned`); batches unpinned invalidations before one `run_pending_tasks()`. Afterwards the view is a permanent uncached pass-through.

**CA6 TableReader and TableCache lifetimes** — A live version or iterator keeps its `Arc<TableReader>` across eviction or `TableCache::close()`. After close, `get_reader` returns `ErrorKind::DbClosed`. Trace newly built SSTs through `VersionSet::prepare_tables` outside `DB::inner` and `apply_with_tables` at install. Eviction between these steps must preserve the validated reader held by the job. [Install and eviction test](../../../src/db.rs)

**CA7 Memory gauges** — count `BlockCachePool::usage()` once per shared pool, keeping both stores' capacities and weighted charges separate. Each store charges `capacity().max(1)` per admitted buffer. Test weighted charges after quiescent maintenance. `DB::memory_usage()` samples active and immutable memtables from one published view and the member's L0 pin-map payload independently. The string property `block-cache-usage` reports an entry count. After close the DB gauges reach zero even when a caller still owns an old pin. [Gauge implementation](../../../src/db/memory.rs) · [Shared-pool tests](../../../tests/shared_cache.rs)

## Accepted races

Unpinned `insert` vs `invalidate_file` / `detach` is a cutoff rather than a barrier: a racing unpinned entry may remain until LRU eviction, subject to eviction by the receiving store and unreachable because `member_id` and `file_number` are never reused.

## Checklist

- [ ] Keys include `(member_id, file_number, offset)` and member IDs never reuse
- [ ] L0 `prepare_first_block_pin` runs unlocked; `unpin_file` runs on every L0 removal/move
- [ ] Pin entry count and `pinned_bytes` stay exact across replace, unpin, and detach
- [ ] Same-key replacement followed by invalidate/detach removes the successor's charge while preserving sibling members' data and charges
- [ ] `u32::try_from(arc.capacity())` overflow invalidates stale key instead of under-weighing
- [ ] Shard selection + eviction pruning consistent across add/remove/take_file/take_member
- [ ] `detach()` idempotent, serialized with `insert_pinned`, siblings unaffected
- [ ] Capacity `0` disables admission and pinning. Both weighted stores participate in file invalidation and detach
- [ ] `TableReader` and `BlockRef` (`PinnedSlice`) `Arc`s outlive cache eviction/close

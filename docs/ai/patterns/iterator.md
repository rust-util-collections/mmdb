# Iterator Review Patterns

**Files:** `db_iter.rs`, `bidi_iter.rs`, `level_iter.rs`, `merge.rs`, `range_del.rs`, `source.rs`, `mod.rs`.

**Arch:** `DBIterator` = MVCC cutoff + same-key dedupe + point/range tombstones + `BlockPropertyFilter` row filter + `[lower, upper)` / prefix bounds + lending `step_next`/`step_prev`. `MergingIterator` = binary heap (+ single-source fast path). `IterSource` = peeked `MemTableCursorIter` / `TableIterator` / `LevelIterator` (`Boxed` is `#[cfg(test)]`-only). `BidiIterator::lazy` = O(1)-memory bidirectional wrapper narrowing `IterConfig` bounds monotonically across direction switches. `FragmentedRangeTombstoneList` = pre-loaded interval list with per-level sequence checks.

## Invariants

**I1 Forward & reverse monotonicity** — `step_next`/`next` yields strictly increasing user keys; `step_prev`/`prev`/`next_back` yields strictly decreasing user keys; each visible user key appears once at its highest sequence `<= read_seq`.

**I2 Typed error propagation & permanent latching** — every layer (`SeekableIterator`, `IterSource`, `LevelIterator`, `MergingIterator`, `DBIterator`, `BidiIterator`) exposes `Option<&Error>` and permanently fuses (`has_failed()` / `halt_on_error()`) on the first I/O, CRC, block-entry, or `InternalKey` decode failure. Subsequent `seek*`, `advance`, `prev`, `next_prefix`, or direction switches must never clear a latched error or expose a shadowed older version from a deeper source.

**I3 Lending `step_next`/`step_prev` vs `Iterator::next`** — Lending steps return `Result<Option<(&[u8], &[u8])>>` with an owned `Error`. The `error()` accessor borrows the latched error. Both steps enforce strict `Err` precedence: if lookahead into the next block fails while taking the current entry, they halt and return `Err(error)` immediately. Infallible `Iterator::next` / `valid()` / `BidiIterator::next` (`front(false)`) may yield an already-decoded, MVCC-verified front entry and halt on the next advance with `error().is_some()`.

**I4 Post-MVCC `BlockPropertyFilter` & `SkipPointFn`** — `IterConfig::matches(user_key)` (`BlockPropertyFilter::key_matches`) and `skip_point` run strictly *after* MVCC sequence cutoff, same-key deduplication, point `Deletion`, and `FragmentedRangeTombstoneList` visibility resolution in both forward (`next_visible`) and reverse (`resolve_prev_user_key`) scans (`clean_read` disabled when filters are active). `TableBuilder` feeds both `Value` and `Deletion` user keys to `BlockPropertyCollector` so blocks containing point tombstones for matching keys are never skipped.

**I5 Bounds, prefix, & partitioned SST leaf isolation** — `intersect_iter_bounds` intersects `ReadOptions` (`Cow<'a, [u8]>`), range, and `[prefix, prefix_successor(prefix))` bounds into shared `Arc<IterConfig>` *before* SST pruning and `init_heap` `posix_fadvise`. Prune point sources outside the scan bounds. Collect overlapping range tombstones independently because their ends can extend past `largest_key`. `TableReader::read_tombstones` binary-searches overlapping `range_partitions` and picks at most one stripe `<= read_seq`. `TableIterator` must check `iterate_upper_bound` and `index_pos == usize::MAX` *before* `select_partition` / `ensure_index` so out-of-bounds seeks never read, cache, or fail on excluded ~64 KiB index leaves.

**I6 Cross-level range tombstones & `iter_with_batch`** — a range tombstone at level `L` covers only sources at `>= L` (`level > source_level` ignored, protecting bottommost sequence-zeroed points). `DB::iter_with_batch` validates `batch.check_valid()` upfront, pins `SuperVersion` before `committed_sequence`, and assigns overlay sequences `[MAX_SEQUENCE_NUMBER - (op_count - 1) ..= MAX_SEQUENCE_NUMBER]` strictly above `read_seq`.

## Checklist

- [ ] Errors latch permanently across `seek`, `seek_for_prev`, `seek_to_first`, `seek_to_last`, `next_prefix`, and `BidiIterator` switches
- [ ] `step_next` / `step_prev` return `Err` immediately on lookahead failure; backward decode failure never exposes older versions
- [ ] `BlockPropertyFilter::key_matches` runs post-MVCC in both forward and reverse; `LevelIterator` reverse walks past all-skipped SSTs
- [ ] `TableIterator::seek`, `seek_for_prev`, and `load_next_block` check `upper` / exhaustion before loading index partition leaves
- [ ] `BidiIterator::lazy` narrows `[lower, upper)` monotonically without materializing windows or double-yielding unconsumed `current`
- [ ] Prefix scans enforce exact `key.starts_with(prefix)` (including all-`0xFF` prefix exhaustion)
- [ ] `next_into` / `next_lazy` never alias or dangle across MemTable/SST cursors
- [ ] SST v2 index and `rangedel` leaves respect `fill_cache`; partition directories and blocked Bloom filters stay resident on `TableReader`

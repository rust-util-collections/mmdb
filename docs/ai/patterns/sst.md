# SST v2 Review Patterns

**Files:** `table_builder.rs`, `table_reader/{mod.rs,iterator.rs}`, `block.rs`, `block_builder.rs`, `filter.rs`, `format.rs`, `range.rs`, `mod.rs`.

See [SST v2 physical layout](../../architecture/sst-and-cache.md#sst-v2-physical-layout)
for block encoding and directory traversal.

## Invariants

**S1 Prefix & restarts** — entries share a prefix with their immediate predecessor (`shared = 0` at every restart). `Block::new` uses checked arithmetic on `num_restarts`, requires strictly increasing offsets within `entry_data` (`[0..restart_offset)`), rejects empty trailing segments on non-empty blocks, and surfaces mid-block entry decode errors via `BlockIterator::error()` / `Result` (never silent EOF).

**S2 Footer & paged directories** — decode `TABLE_MAGIC_V2` and reject `metaindex_handle.size == 0` before reading blocks; require the head index directory page to end immediately before `FOOTER_SIZE` (`offset + size + 5 == file_size - 48`). `read_directory` enforces `prev.size > 0` and `prev.offset + prev.size + 5 <= page.offset` (strictly decreasing offsets → loop-free, total bytes ≤ `file_size`), restores ascending order, and verifies strict monotonicity across `partitions` (`p[0].0 < p[1].0`) and `range_partitions` (`p[0].end <= p[1].begin`).

**S3 Shortest separators & final block** — `shorten_separator(lo, hi)` emits `InternalKey::new(&prefix, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion)` (`[0x00..0xFD]`) only when `lo < prefix < hi` strictly as user keys; equal-user-key block boundaries and the final point block (`emit_pending(None, ...)` even when trailing `RangeDeletion`s follow) retain the exact unshortened `last_key`.
At an inclusive scan lower bound equal to a shortened separator, the preceding
block or index partition contains only smaller user keys. An exact point-key
separator remains inclusive.

**S4 Directory↔Leaf consistency** — every leaf `part` in `partitions` must be non-empty, end with `separator_key == partitions[part].0`, and (`part > 0`) start with `first_key > partitions[part - 1].0`; `point_value_with_probe` and `grouped_point_value` must treat `partition_point_index_leaf` reaching `block.num_restarts()` inside a selected partition as `ErrorKind::Corruption` (matching `range_partitions` checks in `covering_sequence` and `visit_ranges`).

**S5 Pre-fragmented `rangedel` & stripe budget** — `write_range_del_block` sweeps `RangeFragment`s (`begin < end`) into disjoint elementary intervals `[prev, end)` with strictly ascending snapshot-pruned stripes (`<= MAX_SEQUENCE_NUMBER`), partitioned into ~64 KiB leaves. `admit_range_stripes` (`MAX_RANGE_STRIPES = 6,290,928`) bounds worst-case single-interval size below `META_BLOCK_HARD_LIMIT`. Point lookups (`covering_sequence`) binary-search `range_partitions` (`end <= user_key`) and read **at most one** `rangedel` leaf; scans (`read_tombstones`) read only partitions overlapping `[lower, upper)`. `FileMetaData` folds extreme `RangeFragment` `begin` keys into `smallest_key`/`largest_key` and sets `has_range_deletions == !range_partitions.is_empty()`.

For point reads, account for covering tombstones in every file marked
`has_range_deletions` at the level before pruning by SST point bounds. A
tombstone can end beyond the file's `largest_key`. [Point lookup](../../../src/db/read.rs)

**S6 64B Blocked Bloom & `BlockPropertyCollector`** — `#[repr(align(64))] Line([u8; 64])` backs safe 64B-aligned filters (`filter.format = 2` required iff key/prefix filter exists; non-64B-multiple payloads rejected as `Corruption`). The whole-key filter hashes each distinct point user key with seed-zero XXH3-64. The high 32 bits select a 64-byte line, and two `mix64` outputs supply eight 9-bit positions within it. Verify the level-dependent whole-key budget separately from the fixed prefix-filter budget. For every point entry, including multiple versions and point deletions, `TableBuilder` passes the user key to each `BlockPropertyCollector::add`.

**S7 Compression & `fill_cache`** — verify `crc32(payload || [compression_byte])` before decompression and cap decoded LZ4/Zstd sizes at `64 MiB` before allocation. Thread `fill_cache` through `index_leaf_block`, `point_value_with_probe` (`point_in_data_block`), `grouped_point_value` (`indexed_point_block`), `covering_sequence`, `visit_ranges`, and `TableIterator`.

**S8 Bounded iterator partition I/O & error latching** — `TableIterator` checks `iterate_upper_bound` and `index_pos == usize::MAX` *before* `select_partition` / `ensure_index` so out-of-bounds `seek` / `seek_for_prev` / `next` never read, cache, or fail on excluded index leaves; once `self.err` is set, every forward/backward/re-seek method stays latched.

### Grouped point reads

A grouped request accepts up to `MAX_GROUPED_READ_KEYS` input slots (32),
including duplicates. Check empty and singleton inputs with a foreign
snapshot, the 33-slot error, and a nonempty pin held after close. The limit
bounds slots. Distinct returned values can retain whole SST blocks or
memtable arenas. A group keeps one published view and one read sequence,
coalesces equal keys and restores the original slot order. Each of the
two request-local file cursors owns an index leaf. Each leaf belongs
to a file number and partition. The cached data slot belongs to that same
leaf. Replacing a file cursor or changing its partition drops the cached data
slot before reuse. A seek past a table's final partition leaves both cursors
intact. Check scalar and grouped results at the same snapshot for covered
keys outside SST point bounds, duplicates, misses and a malformed later
block. A failed request drops its earlier pins and leaves the returned-byte
counter unchanged. Compare the real block acquisitions when two SSTs
alternate, including a third table with only a post-snapshot record.
[Grouped-read regression tests](../../../tests/grouped_reads.rs)

## Checklist

- [ ] `TABLE_MAGIC_V2`, non-zero `metaindex`, head-page footer adjacency, and backward directory offset bounds checked
- [ ] Index and `rangedel` leaves both cross-check start/end keys against their resident directory partition entries
- [ ] `shorten_separator` preserves equal-user-key boundaries and unshortened final point block
- [ ] `covering_sequence` reads ≤ 1 `rangedel` leaf; `admit_range_stripes` + `META_BLOCK_SPLIT_THRESHOLD` keep all meta blocks < 64 MiB
- [ ] `filter.format = 2` + 64B line alignment validated; `Deletion` user keys included in Bloom and `BlockPropertyCollector`
- [ ] `TableIterator` checks `upper` / exhaustion before loading index leaves and latches `self.err` across all movements
- [ ] LZ4/Zstd allocation capped at `MAX_DECOMPRESSED_BLOCK_SIZE` after CRC check

# SST v2 Review Patterns

**Files:** `table_builder.rs`, `table_reader/{mod.rs,iterator.rs}`, `block.rs`, `block_builder.rs`, `filter.rs`, `format.rs`, `range.rs`, `mod.rs`.

**Arch:** SST v2 (`TABLE_MAGIC_V2`) stores prefix-compressed data blocks, 64-byte-aligned XXH3-64 blocked Bloom filters (`filter.format = 2`), ~64 KiB (`PARTITION_BYTES`) point-index leaves (`BlockBuilder::new(1)`) and pre-fragmented `rangedel` leaves, backward-chained ≤ 32 MiB (`META_BLOCK_SPLIT_THRESHOLD`) partition directory pages, a non-empty `metaindex` block, and a 48-byte footer. Every block carries `[payload][compression_type: u8][crc32: u32 LE]` capped at `MAX_DECOMPRESSED_BLOCK_SIZE` (`64 MiB`; meta hard limit `META_BLOCK_HARD_LIMIT = 64 MiB - 4 KiB`).

## Invariants

**S1 Prefix & restarts** — entries share a prefix with their immediate predecessor (`shared = 0` at every restart). `Block::new` uses checked arithmetic on `num_restarts`, requires strictly increasing offsets within `entry_data` (`[0..restart_offset)`), rejects empty trailing segments on non-empty blocks, and surfaces mid-block entry decode errors via `BlockIterator::error()` / `Result` (never silent EOF).

**S2 Footer & paged directories** — decode `TABLE_MAGIC_V2` and reject `metaindex_handle.size == 0` before reading blocks; require the head index directory page to end immediately before `FOOTER_SIZE` (`offset + size + 5 == file_size - 48`). `read_directory` enforces `prev.size > 0` and `prev.offset + prev.size + 5 <= page.offset` (strictly decreasing offsets → loop-free, total bytes ≤ `file_size`), restores ascending order, and verifies strict monotonicity across `partitions` (`p[0].0 < p[1].0`) and `range_partitions` (`p[0].end <= p[1].begin`).

**S3 Shortest separators & final block** — `shorten_separator(lo, hi)` emits `InternalKey::new(&prefix, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion)` (`[0x00..0xFD]`) only when `lo < prefix < hi` strictly as user keys; equal-user-key block boundaries and the final point block (`emit_pending(None, ...)` even when trailing `RangeDeletion`s follow) retain the exact unshortened `last_key`.

**S4 Directory↔Leaf consistency** — every leaf `part` in `partitions` must be non-empty, end with `separator_key == partitions[part].0`, and (`part > 0`) start with `first_key > partitions[part - 1].0`; `find_data_block` must treat `seek_by_reuse == None` inside a selected partition as `ErrorKind::Corruption` (matching `range_partitions` checks in `covering_sequence` and `visit_ranges`).

**S5 Pre-fragmented `rangedel` & stripe budget** — `write_range_del_block` sweeps `RangeFragment`s (`begin < end`) into disjoint elementary intervals `[prev, end)` with strictly ascending snapshot-pruned stripes (`<= MAX_SEQUENCE_NUMBER`), partitioned into ~64 KiB leaves. `admit_range_stripes` (`MAX_RANGE_STRIPES = 6,290,928`) bounds worst-case single-interval size below `META_BLOCK_HARD_LIMIT`. Point lookups (`covering_sequence`) binary-search `range_partitions` (`end <= user_key`) and read **at most one** `rangedel` leaf; scans (`read_tombstones`) read only partitions overlapping `[lower, upper)`. `FileMetaData` folds extreme `RangeFragment` `begin` keys into `smallest_key`/`largest_key` and sets `has_range_deletions == !range_partitions.is_empty()`.

**S6 64B Blocked Bloom & `BlockPropertyCollector`** — `#[repr(align(64))] Line([u8; 64])` backs safe 64B-aligned filters (`filter.format = 2` required iff key/prefix filter exists; non-64B-multiple payloads rejected as `Corruption`). Each distinct user key (both `Value` and `Deletion`, never `RangeDeletion`) is hashed once with seed-0 XXH3-64 (64-bit fastrange line selection + 8 odd-step probes) and passed stripped of its 8B trailer to `BlockPropertyCollector::add`.

**S7 Compression & `fill_cache`** — verify `crc32(payload || [compression_byte])` before decompression and cap decoded LZ4/Zstd sizes at `64 MiB` before allocation. Thread `fill_cache` through `index_leaf`, `find_data_block`, `covering_sequence`, `visit_ranges`, and `TableIterator`.

**S8 Bounded iterator partition I/O & error latching** — `TableIterator` checks `iterate_upper_bound` and `index_pos == usize::MAX` *before* `select_partition` / `ensure_index` so out-of-bounds `seek` / `seek_for_prev` / `next` never read, cache, or fail on excluded index leaves; once `self.err` is set, every forward/backward/re-seek method stays latched.

## Checklist

- [ ] `TABLE_MAGIC_V2`, non-zero `metaindex`, head-page footer adjacency, and backward directory offset bounds checked
- [ ] Index and `rangedel` leaves both cross-check start/end keys against their resident directory partition entries
- [ ] `shorten_separator` preserves equal-user-key boundaries and unshortened final point block
- [ ] `covering_sequence` reads ≤ 1 `rangedel` leaf; `admit_range_stripes` + `META_BLOCK_SPLIT_THRESHOLD` keep all meta blocks < 64 MiB
- [ ] `filter.format = 2` + 64B line alignment validated; `Deletion` user keys included in Bloom and `BlockPropertyCollector`
- [ ] `TableIterator` checks `upper` / exhaustion before loading index leaves and latches `self.err` across all movements
- [ ] LZ4/Zstd allocation capped at `MAX_DECOMPRESSED_BLOCK_SIZE` after CRC check

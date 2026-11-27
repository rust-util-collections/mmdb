# SST layout and cache ownership

A table reader keeps its directories and filters resident. The cache pool
and caller-held pins have separate lifetimes. Each memory gauge counts the
allocations of a particular owner. [Read path](reads.md#point-lookups) · [Architecture overview](../architecture.md)

## SST v2 physical layout

The reader starts at the fixed 48-byte footer. It decodes a 16-byte
metaindex handle and a 16-byte point-index handle, skips eight padding
bytes, and verifies the eight-byte magic `TABLE_MAGIC_V2`
(`0x4D4D_4442_5353_5402`). The head point-index directory page ends
immediately before the footer. Each block has a five-byte trailer containing
a compression byte and a little-endian CRC32 over the stored payload and
that byte. The decompressed payload is limited to 64 MiB.
[Footer and block format](../../src/sst/format.rs) · [Reader validation](../../src/sst/table_reader/mod.rs)

| Structure | What is stored and how it is read |
|---|---|
| Point data | Prefix-compressed keys and values with full keys at restart points (default interval 16). Data blocks support uncompressed, LZ4, and Zstd encodings. |
| Point index | Leaves target 64 KiB. An entry contains a separator, a data-block handle, the block's first key, and any block properties. The builder shortens a separator only if the new user key lies strictly between adjacent blocks' user keys. The final separator retains the last point key. |
| Range deletions | Separate `rangedel` leaves target 64 KiB. Each stores disjoint half-open intervals and their ascending sequence lists. To resolve one key, search the directory by exclusive end, then read at most one candidate leaf per table. |
| Partition directories | Point-index and range-deletion directories use checksummed pages targeting 32 MiB. The reader follows backward handles with decreasing offsets, then restores directory entries in ascending order in memory. Loaded leaves are checked against their directory bounds. |
| Metaindex and Bloom filters | The metaindex locates whole-key, prefix, and range-deletion metadata when present. Current writers also record `table.max_sequence`. Blocked Bloom format 2 uses seed-zero XXH3-64 and eight bit probes in one 64-byte-aligned line. |

Whole-key filters have a level-scaled bits-per-key budget. With
`optimize_filters_for_hits`, new bottom-level files can omit the whole-key
filter. Configured prefix filters retain their base budget. Block-property
collectors see the user keys of both point values and point deletions.
[SST builder](../../src/sst/table_builder.rs) · [Bloom filter](../../src/sst/filter.rs)

The MANIFEST records the extreme internal keys actually emitted across
point data and range-fragment starts. A range's exclusive end can be far
beyond the last such key. Point lookup and compaction therefore consult
the range-partition extents separately. [File-bound validation](../../src/sst/table_reader/mod.rs) ·
[Recovery of older SSTs](recovery.md#validate-the-manifest-and-live-ssts)

## Cache ownership

| Owner | Contents and lifetime |
|---|---|
| `TableCache` and live versions | The cache coalesces opens of the same SST. A `TableReader` retains its file, Bloom filters, and partition directories. Live versions and iterators can keep readers beyond the table-cache entry limit. |
| `BlockCachePool` | One segmented store and one overflow store hold evictable blocks. Keys include the DB member ID, file number, and block offset, so different databases can share a pool even when their file numbers match. Validated blocks retain the metadata needed to reuse them. |
| Per-DB `BlockCache` | When `pin_l0_first_block_and_warm_index` is enabled and caching is active, flush can warm the first index leaf and pin the first data block of a new L0 SST. A range-only table has no point block to pin. The member's pin map is separate from the pool's weighted stores. |
| Returned `PinnedSlice` | A nonempty pin keeps an SST data buffer or memtable arena alive. Evicting the cache entry, compacting the file, or closing the DB releases only the engine's references. The allocation can be freed after the last owner drops it. |

A pool with configured capacity `C` gives **each** evictable store a weighted
capacity of `C`. A block heavier than one segment's share and within `C` goes
to overflow. In a one-segment pool this interval is empty, so overflow usage
can be zero even though the reported capacity is `C`. Weights use the backing
buffer capacity. An allocation exceeding the `u32` weight range bypasses
admission and invalidates a previous entry for that key.

A 16-shard reverse index tracks offsets by member and file. Replacing SSTs
invalidates their cached blocks, and closing a DB detaches its member without
purging other members' entries. [Block cache and pin map](../../src/cache/block_cache.rs) ·
[Table cache](../../src/cache/table_cache.rs) · [Shared cache tests](../../tests/shared_cache.rs)

## Memory accounting

| Measurement | What to count |
|---|---|
| `BlockCachePool::usage()` | Sample once per shared pool. Read `segmented_capacity_bytes` and `overflow_capacity_bytes` for configured limits, and the corresponding `*_weighted_bytes` for approximate evictable charges. Concurrent cache maintenance can make a charge lag. |
| `DB::memory_usage()` | Sample each DB. `active_memtable_bytes`, `immutable_memtable_count`, and `immutable_memtable_bytes` describe the published view. `l0_pin_bytes` samples the member pin map separately. A flush removes a retired generation from this view after durable install, even if a reader still owns it. |
| `get_property("block-cache-usage")` | An approximate **entry count** including pool-wide evictable entries and this member's pins. Adding this property across members would count shared entries more than once. |
| Process resident set size (RSS) | Includes resident memory outside those gauges, such as live table readers, retired blocks or arenas held by callers, and allocator overhead. The operating system's file-page residency is a separate measurement. |

Pin a nonempty SST value with `get_pinned`, then call `close()`.
`db.memory_usage()` now reports zero for every gauge. The pin still keeps
its block alive, and other open DBs can still use the shared pool. [DB memory gauges](../../src/db/memory.rs) ·
[Cache capacity experiment](../benchmarks/cache-capacity/README.md)

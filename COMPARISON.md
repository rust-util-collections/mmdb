# mmdb vs RocksDB / Pebble Feature Comparison

## Iterator / Range

| Feature | RocksDB | Pebble | mmdb | Notes |
|---------|---------|--------|------|-------|
| MergingIterator (heap-based) | Yes | Yes | Yes | O(N log K) merge |
| Direction switch optimization (in-heap tracking) | Yes | Yes | Yes | Only re-seek off-heap sources |
| TrySeekUsingNext | Yes | Yes | Yes | Step-forward instead of full seek when target >= current |
| NextPrefix skip | No | Yes | Yes | O(log N) inter-prefix jump |
| SeekGEWithLimit / IterAtLimit | No | Yes | No | Soft limit for distributed shard scanning |
| LazyValue / deferred value loading | Partial (BlobDB) | Yes | Yes | Zero-copy within SST blocks, no value alloc on skip path |
| Iterator object pool | Yes | Yes | Partial | Provides reset(), no global pool |
| SetBounds propagation to sub-iterators | Yes | Yes | Yes | upper_bound propagated to TableIterator/LevelIterator |
| SkipPoint callback | No | Yes | Yes | ReadOptions.skip_point callback filtering |
| Iterator error propagation | Yes | Yes | Yes | DBIterator::error() exposes I/O errors |

## Range Tombstone

| Feature | RocksDB | Pebble | mmdb | Notes |
|---------|---------|--------|------|-------|
| Range Deletion (RANGEDEL) | Yes | Yes | Yes | Stored in separate SST block |
| FragmentedRangeTombstone O(log T) | Yes | Yes | Yes | Pre-fragmented + binary search |
| Cross-level tombstone pruning | Yes | Yes | Yes | Shallow tombstones don't mis-delete deeper keys |
| Point query tombstone O(log T) | Yes | Yes | Yes | Replaces original O(T) linear scan |
| Range Key (RANGEKEYSET/UNSET) | No | Yes | No | CRDB-specific, not needed for general KV |

## Memtable

| Feature | RocksDB | Pebble | mmdb | Notes |
|---------|---------|--------|------|-------|
| SkipList (lock-free) | Yes | Yes | Yes | Single-writer multi-reader, arena allocation |
| Backward O(1) (prev pointer) | Yes | Yes | Yes | Level-0 doubly-linked list + cached tail |
| Multiple MemTable implementations | Yes (HashSkipList, Vector) | No | No | SkipList is sufficient |

## SST / Block

| Feature | RocksDB | Pebble | mmdb | Notes |
|---------|---------|--------|------|-------|
| BlockPropertyFilter | Yes | Yes | Yes | User-defined block-level property collection + filtering |
| Bloom Filter (whole-key) | Yes | Yes | Yes | Configurable bits_per_key |
| Prefix Bloom Filter | Yes | Yes | Yes | prefix_len configurable |
| Block compression (LZ4/Zstd) | Yes | Yes | Yes | Per-level configurable |
| BlockCache (LRU) | Yes | Yes | Yes | moka concurrent cache + L0 pinning |
| Value Block separation | Partial (BlobDB/Titan) | Yes | No | LazyValue provides zero-copy within blocks |

## Compaction

| Feature | RocksDB | Pebble | mmdb | Notes |
|---------|---------|--------|------|-------|
| Leveled Compaction | Yes | Yes | Yes | Core strategy |
| Sequence number zeroing | Yes | Yes | Yes | Bottommost + snapshot-aware |
| Range tombstone GC | Yes | Yes | Yes | Discard tombstones at bottommost level |
| Compaction Filter | Yes | Yes | Yes | Keep/Remove/ChangeValue |
| Read-triggered compaction | Yes | Yes | Yes | Hot key sampling, hint-driven |
| Sub-compaction parallelism | Yes | Yes | No | Single-threaded compaction |
| CompactionIter snapshot boundary awareness | Yes | Yes | Partial | Zeroing logic, no multi-version retention |
| Tiered/Universal Compaction | Yes | No | No | Leveled only |

## Write Path

| Feature | RocksDB | Pebble | mmdb | Notes |
|---------|---------|--------|------|-------|
| WAL (Write-Ahead Log) | Yes | Yes | Yes | Group commit |
| WriteBatch | Yes | Yes | Yes | Atomic batch writes |
| WriteBatchWithIndex (batch iteration) | Yes | Yes | Yes | Uncommitted writes are iterable |
| Pipeline Write | Yes | No | No | Group commit serves as alternative |
| Rate Limiter | Yes | Yes | Yes | Compaction write rate limiting |

## Read Path

| Feature | RocksDB | Pebble | mmdb | Notes |
|---------|---------|--------|------|-------|
| Point Get (multi-level lookup) | Yes | Yes | Yes | memtable -> L0 -> L1+ |
| SuperVersion (lock-free reads) | Yes | Yes | Yes | Arc<RwLock<SuperVersion>> |
| Prefix Iterator | Yes | Yes | Yes | Bloom filtering + prefix stop |
| Bidirectional Iterator | Yes | Yes | Yes | Lazy streaming, forward/backward |
| Parallel init_heap I/O | No | Partial (goroutine) | Yes | std::thread::scope parallel peek |

## Key Gaps Summary

| Category | Missing Feature | Impact |
|----------|----------------|--------|
| Feature | Range Key (RANGEKEYSET/RANGEUNSET) | CRDB-specific, not needed for general use |
| Feature | SeekGEWithLimit (soft limit) | Needed for distributed shard scanning |
| Performance | Full value block separation | Further optimization for large values (>4KB) |
| Performance | Sub-compaction parallelism | Large compaction tasks can be split |
| Performance | Tiered/Universal Compaction | Alternative strategy for write-heavy workloads |
| Performance | Global iterator object pool | Avoids allocation for high-frequency short iterations |

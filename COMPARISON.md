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
| Iterator object pool | Yes | Yes | Yes | Global lock-free pool + PooledIterator RAII wrapper |
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
| Sub-compaction parallelism | Yes | Yes | Yes | std::thread::scope, split on Ln+1 file boundaries |
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
| SuperVersion (lock-free reads) | Yes | Yes | Yes | ArcSwap\<SuperVersion\> (single atomic load, no RwLock) |
| Prefix Iterator | Yes | Yes | Yes | Bloom filtering + prefix stop |
| Bidirectional Iterator | Yes | Yes | Yes | Lazy streaming, forward/backward |
| Prefetch-based init_heap I/O | No | Partial (goroutine) | Yes | Sequential prefetch hints + page-cache-overlapped peek (not thread-parallel) |

## Key Gaps Summary

| Category | Missing Feature | Impact |
|----------|----------------|--------|
| Feature | Range Key (RANGEKEYSET/RANGEUNSET) | CRDB-specific, not needed for general use |
| Feature | SeekGEWithLimit (soft limit) | Needed for distributed shard scanning |
| Performance | Full value block separation | Further optimization for large values (>4KB) |
| Performance | Tiered/Universal Compaction | Alternative strategy for write-heavy workloads |

## Gap Analysis: What Is Worth Implementing?

Below is an objective assessment of each missing feature, ordered by value/effort ratio.

### Implemented

**Sub-compaction parallelism** — Implemented via `max_subcompactions` option.
Split points derived from target-level file boundaries. Uses `std::thread::scope` for
parallel execution with shared `AtomicU64` file number counter and pre-populated
`RangeTombstoneTracker` for cross-boundary tombstone correctness.

**Global iterator object pool** — Replaced thread-local pool with global
`parking_lot::Mutex<Vec<DBIterator>>`. Added `PooledIterator` RAII wrapper for
automatic return on drop. Pool capacity adapts to `num_cpus * 4`.

### High Value

**Value Block separation (BlobDB / value separation)**
- **What**: Store large values (>4KB) in separate blob files; SST data blocks contain only keys + value handles.
- **Why it matters**: With inline values, compaction must read/write all value bytes even when only keys are relevant. For workloads with large values (>4KB), this causes 5–10x write amplification compared to a value-separated design. Additionally, the block cache becomes more effective when data blocks contain only keys + small pointers — cache hit rates increase significantly.
- **Effort**: High. Requires a new blob file format, GC mechanism for stale blobs, and integration with the compaction and read paths. The existing `LazyValue::BlockRef` provides zero-copy within blocks, but this is not the same as cross-file separation.
- **Verdict**: **Recommended if mmdb targets workloads with values >4KB.** If the primary use case is small-value KV (chat metadata, indexes, config), the existing LazyValue is sufficient and the complexity is not justified.

### Medium Value

**Tiered/Universal Compaction**
- **What**: An alternative compaction strategy that merges sorted runs of similar size, optimizing for write amplification at the cost of read amplification and space amplification.
- **Why it matters**: For write-heavy workloads (logging, time-series, bulk ingestion), leveled compaction's O(10x) write amplification is the dominant cost. Universal compaction reduces write amplification to O(1x–3x) in exchange for higher read amplification.
- **Effort**: High. Requires a fundamentally different compaction picker, file management strategy, and configuration surface. Pebble chose not to implement this — they solve the write-heavy case with MVCC + range keys instead.
- **Verdict**: **Defer unless there is a concrete write-heavy use case.** Sub-compaction parallelism addresses the most acute write stall problem at far lower cost. Universal compaction is a separate architectural bet.

### Low Value

**SeekGEWithLimit (soft limit for distributed scanning)**
- **What**: A seek variant that returns `IterAtLimit` when the next key exceeds a boundary, allowing the caller to redistribute work across shards without scanning past the boundary.
- **Why it matters**: This is a Pebble-specific optimization for CockroachDB's distributed SQL layer where scan requests are split across range replicas. It avoids scanning beyond the replica's key range and paying unnecessary I/O.
- **Effort**: Low (a few dozen lines in DBIterator).
- **Verdict**: **Only implement if mmdb is embedded in a distributed system.** For standalone or single-node use, `iterate_upper_bound` already provides hard boundary enforcement. The "soft limit" semantic is only valuable when a higher layer wants to distinguish "exhausted within range" from "hit boundary".

**Range Key (RANGEKEYSET/RANGEUNSET)**
- **What**: Key-range-scoped metadata that can be associated with a span of keys without touching each individual key.
- **Why it matters**: Pebble uses this for CockroachDB MVCC range tombstones and import-time key rewriting. It is not part of RocksDB's feature set either.
- **Effort**: High (new value type, SST block format, iterator integration, compaction handling).
- **Verdict**: **Not recommended.** This is highly specialized for CockroachDB's requirements and adds significant complexity for no benefit in a general-purpose KV engine.

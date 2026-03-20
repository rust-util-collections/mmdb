# MMDB

**A Modern LSM-Tree Storage Engine!**

[![CI](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml/badge.svg)](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024_edition-orange.svg)](https://www.rust-lang.org/)
[![Tests](https://img.shields.io/badge/tests-250_passing-brightgreen.svg)]()

A pure-Rust, synchronous LSM-Tree key-value storage engine with competitive scan and point-read performance.

---

## Performance

In typical configurations, MMDB's scan throughput and point-read latency are comparable to RocksDB. The engine uses the same core optimizations (bloom filters, block cache, prefix compression, leveled compaction) and is designed to perform well across both warm-cache and cold-cache workloads. Run `make bench` to evaluate performance under your specific hardware and data profile.

---

## Features

| Feature | Status |
|---------|--------|
| Put / Get / Delete | Implemented |
| WriteBatch (atomic multi-key writes) | Implemented |
| WAL with group commit & crash recovery | Implemented |
| SST files with prefix-compressed blocks | Implemented |
| Bloom filters (per-key + prefix bloom) | Implemented |
| Block cache (moka LRU) with L0 pinning (insert_pinned) | Implemented |
| Table cache (open file handles) | Implemented |
| MANIFEST version tracking + compaction | Implemented |
| Leveled compaction with trivial move | Implemented |
| Multi-threaded background compaction | Implemented |
| Streaming compaction (O(block) memory) | Implemented |
| MVCC snapshots via sequence numbers | Implemented |
| Forward/backward/prefix/range iterators | Implemented |
| Compression: None, LZ4, Zstd (per-level) | Implemented |
| Write backpressure (slowdown/stop) | Implemented |
| DeleteRange (range tombstones) | Implemented |
| CompactRange API (with range filtering) | Implemented |
| Compaction filter | Implemented |
| Rate limiter (token bucket, wired to compaction) | Implemented |
| DB properties/statistics (wired to all paths) | Implemented |
| Configurable options (RocksDB parity) | Implemented |

---

## Architecture

```
                      +-------------+
                      |   DB (API)  |   get / put / delete / write_batch
                      +------+------+
                             |
                +------------+------------+
                v            v            v
          +----------+  +---------+  +-----------+
          | MemTable |  |  WAL    |  | MANIFEST  |
          | (active) |  | (append)|  | (versions)|
          +----+-----+  +---------+  +-----------+
               | freeze
               v
          +----------+
          | MemTable |
          | (immut.) |
          +----+-----+
               | flush
               v
    +------------------------------------+
    |      SST Layer (L0 .. Ln)          |
    |  L0: overlapping files             |
    |  L1+: sorted, non-overlapping      |
    +------------------------------------+
               ^
               | background compaction (N threads)
    +----------+-----------+
    | Leveled Compaction   |
    +----------------------+
```

### Write Path

1. Encode as InternalKey (`user_key + !pack(sequence, type)` — inverted big-endian for descending sequence order)
2. Append to WAL (group commit: leader batches multiple writers into one fsync)
3. Insert into active MemTable (concurrent skiplist)
4. When MemTable exceeds `write_buffer_size`, freeze and flush to L0 SST
5. Signal background compaction threads if L0 file count exceeds threshold

### Read Path

1. Active MemTable (newest writes)
2. Immutable MemTables (newest first)
3. L0 SST files (newest first, per-file bloom filter + range check)
4. L1+ SST files (LevelIterator with lazy file opening, binary search by key range)

### Iterator Architecture

The scan path uses a **MergingIterator** (min-heap) that merges entries from:
- MemTable cursors (O(1) per entry via skiplist level-0 chain)
- L0 TableIterators (one per overlapping SST file)
- LevelIterators (one per L1+ level — lazy two-level iterator)

Key optimizations:
- **Single-source bypass**: When only one source exists, `MergingIterator` bypasses heap machinery — direct delegation
- **Lock-free reads**: Read path uses `ArcSwap<SuperVersion>` snapshot instead of locking the inner mutex
- **Buffer-reusing iteration**: `next_into()` copies directly into caller buffers — minimizes heap allocation per entry
- **In-place key truncation**: Truncates internal keys to user keys in-place instead of allocating
- **Block cache**: `Block` stores `Arc<Vec<u8>>` — cache hits avoid memcpy
- **Cursor-based block iteration**: Decodes entries one-at-a-time from raw block data — no per-block Vec allocation
- **Deferred block read**: SST index stores `first_key` per block; Seek positions without reading data blocks
- **Sequential readahead**: `posix_fadvise(WILLNEED)` after detecting sequential block access
- **L0 metadata pinning**: Index/data blocks pinned for L0 files via `insert_pinned`; unpinned on compaction
- **Sweep-line range tombstone tracking**: O(1) amortized per key
- **Lazy index loading**: Index parsed on first use
- **Atomic L0 counter**: Write-throttle checks use an atomic counter, avoiding mutex contention on writes

### SST Format

```
Data Block 0: [entries with prefix compression + restart points]
Data Block 1: ...
...
Filter Block: [bloom filter bits]
Prefix Filter Block: [prefix bloom bits]
Metaindex Block: ["filter.bloom" -> handle, "filter.prefix" -> handle]
Index Block: [last_key -> BlockHandle + first_key per block]
Footer (48 bytes): [metaindex_handle, index_handle, magic]
```

---

## Source Code Structure

```
src/
+-- lib.rs                  # Public API re-exports
+-- db.rs                   # DB: open/get/put/delete/write/flush/compact/close
+-- options.rs              # DbOptions, ReadOptions, WriteOptions (RocksDB-compatible)
+-- types.rs                # InternalKey, ValueType, SequenceNumber, WriteBatch
+-- error.rs                # Error types
+-- rate_limiter.rs         # Token-bucket rate limiter
+-- stats.rs                # Database statistics
+-- memtable/
|   +-- mod.rs              # MemTable (put/get/iter with approximate_size tracking)
|   +-- skiplist.rs         # OrdInternalKey + skiplist (O(1) iteration)
+-- wal/
|   +-- writer.rs           # WAL append with block-based fragmentation
|   +-- reader.rs           # WAL replay for crash recovery
|   +-- record.rs           # Record format: checksum + length + type + payload
+-- sst/
|   +-- block.rs            # Data block: prefix compression + restart points + seek
|   +-- block_builder.rs    # Block construction
|   +-- table_builder.rs    # SST writer (data + filter + index with first_key + footer)
|   +-- table_reader.rs     # SST reader + TableIterator (cursor-based, deferred block read)
|   +-- filter.rs           # Bloom filter (double hashing)
|   +-- format.rs           # Footer, BlockHandle, CompressionType, IndexEntry encoding
+-- compaction/
|   +-- leveled.rs          # Leveled compaction: streaming merge, trivial move, filter
+-- manifest/
|   +-- version_edit.rs     # VersionEdit encode/decode
|   +-- version.rs          # Version (immutable SST file set snapshot)
|   +-- version_set.rs      # VersionSet (MANIFEST management + version chain)
+-- cache/
|   +-- block_cache.rs      # Block LRU cache (moka) with L0 pinning support
|   +-- table_cache.rs      # SST reader cache
+-- iterator/
    +-- merge.rs            # Min-heap MergingIterator with buffer reuse + prefetch
    +-- db_iter.rs          # DBIterator: dedup, snapshot, tombstone/range-del filtering
    +-- level_iter.rs       # LevelIterator: lazy two-level iterator for L1+
    +-- range_del.rs        # RangeTombstoneTracker: sweep-line O(1) amortized
    +-- bidi_iter.rs        # BidiIterator: bidirectional iteration
```

---

## Public API

```rust
impl DB {
    pub fn open(options: DbOptions, path: impl AsRef<Path>) -> Result<Self>;
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub fn put_with_options(&self, options: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()>;
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    pub fn get_with_options(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>>;
    pub fn delete(&self, key: &[u8]) -> Result<()>;
    pub fn delete_with_options(&self, options: &WriteOptions, key: &[u8]) -> Result<()>;
    pub fn delete_range(&self, begin: &[u8], end: &[u8]) -> Result<()>;
    pub fn delete_range_with_options(&self, options: &WriteOptions, begin: &[u8], end: &[u8]) -> Result<()>;
    pub fn write(&self, batch: WriteBatch) -> Result<()>;
    pub fn write_with_options(&self, batch: WriteBatch, options: &WriteOptions) -> Result<()>;
    pub fn iter(&self) -> Result<DBIterator>;

    /// Prefix-optimized iteration. Uses prefix bloom filters to skip entire SST
    /// files that don't contain `prefix`. Supports bounds via ReadOptions.
    pub fn iter_with_prefix(&self, prefix: &[u8], options: &ReadOptions) -> Result<DBIterator>;

    /// Full-scan iteration with optional key-range bounds.
    /// WARNING: Does NOT use prefix bloom filters. For prefix-scoped queries,
    /// prefer `iter_with_prefix()` which is significantly faster.
    pub fn iter_with_range(&self, options: &ReadOptions, lower: Option<&[u8]>, upper: Option<&[u8]>) -> Result<DBIterator>;

    /// RAII snapshot — automatically released on drop.
    pub fn snapshot(&self) -> Snapshot<'_>;

    pub fn flush(&self) -> Result<()>;
    pub fn compact(&self) -> Result<()>;
    pub fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()>;
    pub fn get_property(&self, name: &str) -> Option<String>;
    pub fn close(&self) -> Result<()>;
}

impl DBIterator {
    pub fn valid(&mut self) -> bool;
    pub fn key(&mut self) -> Option<&[u8]>;     // None if invalid (no panic)
    pub fn value(&mut self) -> Option<&[u8]>;   // None if invalid (no panic)
    pub fn advance(&mut self);
    pub fn seek(&mut self, target: &[u8]);       // first key >= target
    pub fn seek_for_prev(&mut self, target: &[u8]); // last key <= target
    pub fn seek_to_first(&mut self);
    pub fn seek_to_last(&mut self);
    pub fn prev(&mut self);
}

impl Snapshot<'_> {
    pub fn sequence(&self) -> SequenceNumber;
    pub fn read_options(&self) -> ReadOptions;  // pre-configured for this snapshot
}

struct ReadOptions {
    pub snapshot: Option<u64>,
    pub iterate_lower_bound: Option<Vec<u8>>,  // inclusive
    pub iterate_upper_bound: Option<Vec<u8>>,  // exclusive
    // ... other options
}
```

---

## Configuration

```rust
let opts = DbOptions {
    write_buffer_size: 64 * 1024 * 1024,    // 64 MB memtable
    block_cache_capacity: 256 * 1024 * 1024, // 256 MB block cache
    block_size: 16384,                        // 16 KB blocks
    l0_compaction_trigger: 8,
    compression: CompressionType::Lz4,
    bloom_bits_per_key: 10,
    max_background_compactions: 4,            // parallel compaction
    pin_l0_filter_and_index_blocks_in_cache: true,
    ..Default::default()
};

// Or use a preset profile:
let opts = DbOptions::write_heavy();  // 128MB memtable, 4 compaction threads, LZ4
let opts = DbOptions::read_heavy();   // large cache, 14 bits/key bloom
```

---

## Feature Comparison

See [COMPARISON.md](COMPARISON.md) for a detailed feature-by-feature comparison with RocksDB and Pebble, including gap analysis and recommendations.

---

## Build & Test

```bash
cargo build
cargo test               # 250+ tests (unit + integration + e2e + proptest)
make all                 # fmt + lint + check + test
make bench               # criterion benchmarks (warm + cold cache scenarios)
cargo bench -- "cold"    # cold-cache benchmarks only
cargo bench -- "warm"    # warm-cache benchmarks only
```

Benchmarks test both warm-cache (256MB, data in memory) and cold-cache (256KB, I/O-bound) scenarios. Cold-cache tests use smaller datasets to keep runtime reasonable.

---

## License

MIT

# MMDB

**A Modern LSM-Tree Storage Engine!**

[![CI](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml/badge.svg)](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024_edition-orange.svg)](https://www.rust-lang.org/)
[![Tests](https://img.shields.io/badge/tests-220_passing-brightgreen.svg)]()

A pure-Rust, synchronous LSM-Tree key-value storage engine. Designed as a native Rust alternative to RocksDB with competitive performance — **scan throughput matches or exceeds RocksDB** on equivalent configurations.

---

## Performance

Benchmarked via [vsdb](https://github.com/rust-util-collections/vsdb) (500K entries, 128-byte values, 64MB write buffer, 256MB block cache):

| Operation | RocksDB | MMDB | Result |
|-----------|---------|------|--------|
| scan 100 entries | 14.1 us | **11.7 us** | MMDB 17% faster |
| scan 1000 entries | 112 us | **108 us** | MMDB 4% faster |
| scan 10000 entries | 1.10 ms | **1.09 ms** | MMDB 1% faster |
| full scan 500K | 54.5 ms | **52.4 ms** | MMDB 4% faster |
| point read | 405 ns | **168 ns** | MMDB 2.4x faster |

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

### Iterator Architecture (key performance innovation)

The scan path uses a **MergingIterator** (min-heap) that merges entries from:
- MemTable cursors (O(1) per entry via skiplist level-0 chain)
- L0 TableIterators (one per overlapping SST file)
- LevelIterators (one per L1+ level — lazy two-level iterator)

Key optimizations that match/exceed RocksDB:
- **Zero-copy block cache**: `Block` stores `Arc<Vec<u8>>` — cache hits avoid memcpy
- **Cursor-based block iteration**: Decodes entries one-at-a-time from raw block data (`decode_entry_reuse`) — no per-block Vec allocation
- **Deferred block read**: SST index stores `first_key` per block; Seek positions without reading data blocks
- **Buffer reuse**: `IterSource` reuses key/value buffers; `next_into()` decodes directly into caller buffers
- **Sequential readahead**: `posix_fadvise(WILLNEED)` after detecting sequential block access
- **L0 metadata pinning**: Index entries and first data blocks pinned (non-evictable) for L0 files via `insert_pinned`; unpinned on compaction
- **Sweep-line range tombstone tracking**: O(1) amortized instead of O(T) per key
- **Lazy index loading**: `TableIterator::new()` is O(1) — index parsed on first use

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
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    pub fn delete(&self, key: &[u8]) -> Result<()>;
    pub fn delete_range(&self, begin: &[u8], end: &[u8]) -> Result<()>;
    pub fn write(&self, batch: WriteBatch) -> Result<()>;
    pub fn iter(&self) -> Result<DBIterator>;
    pub fn iter_with_prefix(&self, prefix: &[u8]) -> Result<DBIterator>;
    pub fn flush(&self) -> Result<()>;
    pub fn compact(&self) -> Result<()>;
    pub fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()>;
    pub fn get_property(&self, name: &str) -> Option<String>;
    pub fn close(&self) -> Result<()>;
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

## Build & Test

```bash
cargo build
cargo test               # 220+ tests (unit + integration + e2e + proptest)
make all                 # fmt + lint + check + test
make bench               # criterion benchmarks (warm + cold cache scenarios)
cargo bench -- "cold"    # cold-cache benchmarks only
cargo bench -- "warm"    # warm-cache benchmarks only
```

Benchmarks test both warm-cache (256MB, data in memory) and cold-cache (256KB, I/O-bound) scenarios. Cold-cache tests use smaller datasets to keep runtime reasonable.

---

## License

MIT

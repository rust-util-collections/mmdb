# MMDB — A Modern LSM-Tree Storage Engine

[![CI](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml/badge.svg)](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024_edition-orange.svg)](https://www.rust-lang.org/)
[![Tests](https://img.shields.io/badge/tests-159_passing-brightgreen.svg)]()

A pure-Rust, synchronous LSM-Tree key-value storage engine. Designed for correctness, simplicity, and competitive performance.

---

## Features

| Feature | Status |
|---------|--------|
| Put / Get / Delete | Implemented |
| WriteBatch (atomic multi-key writes) | Implemented |
| WAL with group commit & crash recovery | Implemented |
| SST files with prefix-compressed blocks | Implemented |
| Bloom filters per SST | Implemented |
| Block cache (moka LRU) | Implemented |
| Table cache (open file handles) | Implemented |
| MANIFEST version tracking | Implemented |
| Leveled compaction (L0->L1, Ln->Ln+1) | Implemented |
| Trivial move optimization | Implemented |
| Streaming compaction (O(block) memory) | Implemented |
| MVCC snapshots via sequence numbers | Implemented |
| Forward iterator with seek | Implemented |
| Compression: None, LZ4, Zstd | Implemented |
| Write backpressure (slowdown/stop) | Implemented |
| DeleteRange (range tombstones) | Implemented |
| CompactRange API | Implemented |
| Compaction filter | Implemented |
| Rate limiter (token bucket) | Implemented |
| DB properties/statistics | Implemented |
| Preset profiles (balanced/write-heavy/read-heavy) | Implemented |
| MANIFEST compaction (periodic rewrite) | Implemented |

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
               | compaction
    +----------+-----------+
    | Leveled Compaction   |
    +----------------------+
```

### Write Path

1. Encode as InternalKey (user_key + sequence + type)
2. Append to WAL (group commit: leader batches multiple writers into one fsync)
3. Insert into active MemTable (lock-free crossbeam-skiplist with `OrdInternalKey`)
4. When MemTable exceeds `write_buffer_size`, freeze and flush to L0 SST
5. Trigger compaction if L0 file count exceeds threshold

### Read Path

1. Active MemTable (newest writes)
2. Immutable MemTables (newest first)
3. L0 SST files (newest first, may overlap) with bloom filter
4. L1+ SST files (binary search by key range, no overlap within level)

### InternalKey Encoding

```
[user_key bytes][!packed.to_be_bytes()]
```

Where `packed = (sequence << 8) | value_type`. The bit-inversion ensures that for the
same user key, higher sequence numbers sort first in lexicographic order.

Variable-length user keys require `compare_internal_key()` for correct ordering —
raw byte comparison is insufficient.

---

## Source Code Structure

```
src/
+-- lib.rs                  # Public API re-exports
+-- db.rs                   # DB struct: open/get/put/delete/write/flush/compact/close
+-- options.rs              # DbOptions, ReadOptions, WriteOptions, CompactionFilter
+-- types.rs                # InternalKey, ValueType, SequenceNumber, WriteBatch
+-- error.rs                # Error types
+-- rate_limiter.rs         # Token-bucket rate limiter
+-- stats.rs                # Database statistics
+-- memtable/
|   +-- mod.rs              # MemTable (put/get/iter with approximate_size tracking)
|   +-- skiplist.rs         # OrdInternalKey + crossbeam-skiplist (O(1) iteration)
+-- wal/
|   +-- mod.rs
|   +-- writer.rs           # WAL append with block-based fragmentation
|   +-- reader.rs           # WAL replay for crash recovery
|   +-- record.rs           # Record format: checksum + length + type + payload
+-- sst/
|   +-- mod.rs
|   +-- block.rs            # Data block: prefix compression + restart points + seek
|   +-- block_builder.rs    # Block construction
|   +-- table_builder.rs    # SST file writer (data + filter + index + footer)
|   +-- table_reader.rs     # SST file reader + TableIterator (streaming)
|   +-- filter.rs           # Bloom filter (double hashing)
|   +-- format.rs           # Footer, BlockHandle, CompressionType
+-- compaction/
|   +-- mod.rs
|   +-- leveled.rs          # Leveled compaction with streaming merge + trivial move
+-- manifest/
|   +-- mod.rs
|   +-- version_edit.rs     # VersionEdit encode/decode
|   +-- version.rs          # Version (immutable SST file set snapshot)
|   +-- version_set.rs      # VersionSet (MANIFEST management + version chain)
+-- cache/
|   +-- mod.rs
|   +-- block_cache.rs      # Block-level LRU cache (moka)
|   +-- table_cache.rs      # SST reader cache
+-- iterator/
    +-- mod.rs
    +-- merge.rs            # Min-heap MergingIterator (O(N log K))
    +-- db_iter.rs          # DBIterator (dedup, snapshot, tombstone/range-del filtering)
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
    pub fn snapshot_seq(&self) -> SequenceNumber;
    pub fn iter(&self) -> Result<DBIterator>;
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
    write_buffer_size: 64 * 1024 * 1024,  // 64 MB memtable
    l0_compaction_trigger: 4,              // compact when 4 L0 files
    compression: CompressionType::Lz4,     // or None, Zstd
    bloom_bits_per_key: 10,                // ~1% false positive rate
    block_cache_capacity: 64 * 1024 * 1024,
    ..Default::default()
};

// Or use a preset profile:
let opts = DbOptions::write_heavy();  // 128MB memtable, LZ4
let opts = DbOptions::read_heavy();   // large cache, more bloom bits
```

---

## Dependencies

```
crossbeam-skiplist  — Lock-free concurrent MemTable
moka                — High-performance LRU block cache
lz4_flex            — LZ4 compression
zstd                — Zstd compression
crc32fast           — CRC32 checksums
parking_lot         — Fast mutexes and condvars
thiserror           — Error types
tracing             — Structured logging
```

---

## Build & Test

```bash
cargo build
cargo test               # ~95+ unit/integration/proptest tests
cargo bench              # criterion benchmarks (when configured)
make all                 # fmt + lint + check + test
```

---

## License

MIT OR Apache-2.0

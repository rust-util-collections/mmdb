# MMDB

**The Storage Engine Behind [vsdb](https://github.com/rust-util-collections/vsdb).**

[![CI](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml/badge.svg)](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024_edition-orange.svg)](https://www.rust-lang.org/)
[![Tests](https://img.shields.io/badge/tests-250_passing-brightgreen.svg)]()

A pure-Rust LSM-Tree key-value storage engine, purpose-built for
[vsdb](https://github.com/rust-util-collections/vsdb).

> **Scope** — MMDB is designed and optimised exclusively for vsdb's
> workload: prefix-partitioned keys, bulk COW mutations, and
> compaction-driven garbage collection. While the API is generic enough
> for other key-value use cases, **optimisation efforts focus solely on
> the vsdb scenario**. If you need a general-purpose embedded KV store,
> consider RocksDB, sled, or redb.

---

## Performance

In typical configurations, MMDB's scan throughput and point-read latency are comparable to RocksDB. The engine uses the same core optimizations (bloom filters, block cache, prefix compression, leveled compaction) and is designed to perform well across both warm-cache and small-block-cache workloads. Run `make bench` to evaluate performance under your specific hardware and data profile.

---

## Features

| Feature | Status |
|---------|--------|
| Put / Get / Delete | Implemented |
| WriteBatch (atomic multi-key writes) | Implemented |
| WAL with group commit & crash recovery | Implemented |
| SST files with prefix-compressed blocks | Implemented |
| Bloom filters (per-key + prefix bloom) | Implemented |
| Block cache (moka LRU) with L0 first-block pinning (insert_pinned) | Implemented |
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
| Typed errors with full propagation traces | Implemented |

`max_immutable_memtables` is retained as an accepted-but-unused compatibility
option. MMDB flushes each frozen MemTable synchronously, so there is no
immutable-MemTable queue for that value to bound; other options are wired to
their documented behavior.

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
- **L0 first-block pinning**: Each L0 file's first data block (smallest key) is pinned via `insert_pinned` so `init_heap`'s first `peek()` is always a cache hit; unpinned on compaction. Index and filter blocks are held directly as `TableReader` fields and never go through `block_cache`
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
|   +-- skiplist.rs         # OrdInternalKey + skiplist + MemTableCursorIter (O(1) iteration)
|   +-- skiplist_impl.rs    # Lock-free skiplist implementation (raw pointers, arena allocation)
+-- wal/
|   +-- writer.rs           # WAL append with block-based fragmentation
|   +-- reader.rs           # WAL replay for crash recovery
|   +-- record.rs           # Record format: checksum + length + type + payload
+-- sst/
|   +-- block.rs            # Data block: prefix compression + restart points + seek
|   +-- block_builder.rs    # Block construction
|   +-- table_builder.rs    # SST writer (data + filter + index with first_key + footer)
|   +-- table_reader/
|   |   +-- mod.rs           # SST reader: open, block read/cache, point lookup
|   |   +-- iterator.rs      # TableIterator (cursor-based, deferred block read)
|   +-- filter.rs            # Bloom filter (double hashing)
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
    +-- source.rs           # IterSource (peeked-entry adapter) + SeekableIterator trait
    +-- db_iter.rs          # DBIterator: dedup, snapshot, tombstone/range-del filtering
    +-- level_iter.rs       # LevelIterator: lazy two-level iterator for L1+
    +-- range_del.rs        # FragmentedRangeTombstoneList (O(log T) lookup) + sweep-line RangeTombstoneTracker
    +-- bidi_iter.rs        # BidiIterator: bidirectional iteration
```

---

## Selected Public API

The complete supported API is the set of crate-root re-exports documented on
[docs.rs/mmdb](https://docs.rs/mmdb). In addition to the selected methods
below, that includes `BidiIterator`, `WriteBatchWithIndex`, `BlockCache`,
`BlockCachePool`, `CompressionType`, compaction/property filter traits, and
the public size-limit constants.

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
    pub fn write_with_options(&self, options: &WriteOptions, batch: WriteBatch) -> Result<()>;
    pub fn iter(&self) -> Result<DBIterator>;
    pub fn iter_with_options(&self, options: &ReadOptions) -> Result<DBIterator>;
    pub fn iter_with_batch(&self, batch: &WriteBatchWithIndex) -> Result<DBIterator>;

    /// Prefix-bounded iteration — the fastest option for prefix-scoped queries.
    ///
    /// Applies two levels of SST pruning:
    ///   1. Range pruning  — skips files whose key range doesn't overlap the prefix.
    ///   2. Bloom filter   — skips files that don't contain the prefix (finer-grained).
    ///
    /// Use this when all target keys share a common prefix (e.g. `b"orders:"`).
    /// For a sub-range within the prefix, set ReadOptions bounds.
    pub fn iter_with_prefix(&self, prefix: &[u8], options: &ReadOptions) -> Result<DBIterator>;

    /// Arbitrary key-range iteration with SST range pruning.
    ///
    /// Skips SST files whose `[smallest, largest]` key range does not overlap
    /// `[lower, upper)` — but does NOT use bloom filters.
    ///
    /// Use this when the scan range spans multiple prefixes and cannot be
    /// expressed as a single `iter_with_prefix()` call (e.g. `[b"m", b"z")`).
    /// For full-database scans, prefer `iter()` / `iter_with_options()`.
    ///
    /// Explicit bounds are merged with any bounds in ReadOptions (tighter wins).
    ///
    /// ## Pruning comparison
    ///
    /// | Method               | SST range pruning | Bloom filter pruning |
    /// |----------------------|:-----------------:|:--------------------:|
    /// | `iter()`             | ✗                 | ✗                    |
    /// | `iter_with_range()`  | ✓                 | ✗                    |
    /// | `iter_with_prefix()` | ✓                 | ✓                    |
    pub fn iter_with_range(&self, options: &ReadOptions, lower_bound: Option<&[u8]>, upper_bound: Option<&[u8]>) -> Result<DBIterator>;

    /// RAII snapshot — automatically released on drop.
    pub fn snapshot(&self) -> Snapshot<'_>;

    pub fn flush(&self) -> Result<()>;
    pub fn compact(&self) -> Result<()>;
    pub fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()>;
    pub fn get_property(&self, name: &str) -> Option<String>;
    pub fn path(&self) -> &Path;
    pub fn lazy_delete(&self, key: &[u8]);
    pub fn lazy_delete_batch(&self, keys: impl IntoIterator<Item = impl AsRef<[u8]>>);
    pub fn dead_key_count(&self) -> usize;
    pub fn clear_dead_keys(&self);
    pub fn close(&self) -> Result<()>;
}

impl DBIterator {
    // Also implements Iterator<Item = (Vec<u8>, Vec<u8>)>.
    pub fn valid(&mut self) -> bool;
    pub fn key(&mut self) -> Option<&[u8]>;     // None if invalid (no panic)
    pub fn value(&mut self) -> Option<&[u8]>;   // None if invalid (no panic)
    pub fn advance(&mut self);
    pub fn seek(&mut self, target: &[u8]);       // first key >= target
    pub fn seek_for_prev(&mut self, target: &[u8]); // last key <= target
    pub fn seek_to_first(&mut self);
    pub fn seek_to_last(&mut self);
    pub fn prev(&mut self);
    pub fn next_prefix(&mut self, prefix_len: usize); // O(log N) jump
    pub fn error(&self) -> Option<String>;       // I/O errors swallowed by Iterator::next
}

impl Snapshot<'_> {
    // RAII guard: released automatically on drop.
    pub fn sequence(&self) -> SequenceNumber;
    pub fn read_options(&self) -> ReadOptions;  // pre-configured for this snapshot
}

struct ReadOptions {
    pub snapshot: Option<SequenceNumber>,      // from Snapshot::sequence()
    pub fill_cache: bool,                      // false = scans don't evict hot blocks
    pub skip_point: Option<SkipPointFn>,
    pub block_property_filters: Vec<Arc<dyn BlockPropertyFilter>>,
    pub iterate_lower_bound: Option<Vec<u8>>,  // inclusive
    pub iterate_upper_bound: Option<Vec<u8>>,  // exclusive
}

struct WriteOptions {
    pub sync: bool,        // fsync WAL before acknowledging
    pub disable_wal: bool, // skip WAL (data may be lost on crash)
    pub no_slowdown: bool, // error instead of sleeping when throttled
}
```

---

## Error Handling

All fallible APIs return `mmdb::Result<T> = Result<T, mmdb::Error>`.

`Error` carries a typed [`ErrorKind`] (`Io`, `Corruption`, `InvalidArgument`,
`DbClosed`, `Background`) for programmatic matching, plus a full propagation
trace — the origin `file:line:column` and one frame per `.ctx()` /
`.with_ctx(..)` hop — and preserves the underlying source error (e.g.
`std::io::Error`) for `std::error::Error::source()` downcasting.
`Display`/`Debug` render the complete chain:

```text
Corruption: WAL 000012 CRC mismatch
    at src/wal/reader.rs:116:38
    at src/db.rs:410:36 -- refusing prefix recovery
caused by: unexpected end of file
```

```rust
match db.get(b"key") {
    Err(e) if e.kind() == mmdb::ErrorKind::Corruption => { /* quarantine */ }
    other => { /* ... */ }
}
```

`Error` is `Clone + Send + Sync`, so one failure can be shared across
group-commit waiters without stringification.

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

## Migrating 3.x → 4.0

v4.0 is a design-level cleanup release. Breaking changes:

1. **Typed errors replace `ruc` chains.** `Result<T>` is now
   `Result<T, mmdb::Error>`; match on `err.kind()` (`ErrorKind`). Errors still
   carry a full per-hop `file:line:column` propagation trace (rendered by
   `Display`/`Debug`), plus `std::error::Error::source()`, `Clone`, and `Sync`
   — a strict superset of the old `Box<dyn RucError>` payload. Code that only
   propagates/prints errors (incl. `ruc`'s `.c(d!())` on the caller side)
   keeps working since `Error: Display + Debug + Send + Sync`.
2. **Internal modules are private.** Import everything from the crate root
   (e.g. `mmdb::CompressionType`, not `mmdb::sst::format::CompressionType`).
3. **No-op "compatibility" options removed** from `DbOptions`
   (`cache_index_and_filter_blocks`, `max_write_buffer_number`,
   `level_compaction_dynamic_level_bytes`, `allow_concurrent_memtable_write`,
   `memtable_prefix_bloom_ratio`), `ReadOptions` (`verify_checksums`,
   `readahead_size`, `total_order_seek`, `pin_data`) and `WriteOptions`
   (`low_pri`). Delete them from struct literals. `ReadOptions::fill_cache`
   remains and is now **actually implemented** (checksums are always
   verified).
4. **Iterator pool removed.** `pool_return()` and `PooledIterator` are gone —
   just drop iterators.
5. **RAII-only snapshots.** `snapshot_seq()`/`release_snapshot()` are private;
   use `let snap = db.snapshot();` and `snap.read_options()` /
   `snap.sequence()`.
6. **`write_with_options(&WriteOptions, WriteBatch)`** — options now come
   first, consistent with every other `*_with_options` method.

---

## Build & Test

```bash
cargo build
cargo test               # 250+ tests (unit + integration + e2e + proptest)
make all                 # fmt + lint + test
make bench               # criterion benchmarks (warm + small-cache scenarios)
cargo bench -- "small_cache" # small block-cache benchmarks only
cargo bench -- "warm"    # warm-cache benchmarks only
```

Benchmarks cover warm-cache (256MB block cache, data in memory) and
small-cache (256KB block cache, block-cache-miss plus decode overhead)
scenarios. Small-cache reads still use the same-process OS page cache, so they
do not measure storage latency; those cases use smaller datasets to keep
runtime reasonable.

---

## License

MIT

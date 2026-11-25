# MMDB

**The Storage Engine Behind [vsdb](https://github.com/rust-util-collections/vsdb).**

[![CI](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml/badge.svg)](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/mmdb.svg)](https://crates.io/crates/mmdb)
[![Docs.rs](https://docs.rs/mmdb/badge.svg)](https://docs.rs/mmdb)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024_edition-orange.svg)](https://www.rust-lang.org/)

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
| Read-only open for immutable/permission-restricted stores | Implemented |
| SST files with prefix-compressed blocks | Implemented |
| Bloom filters (Monkey level-proportional 64B blocked + prefix bloom) | Implemented |
| Block cache (moka LRU) with L0 first-block pinning & >1 MiB pool overflow | Implemented |
| Table cache (open file handles) | Implemented |
| MANIFEST version tracking with atomic version edits | Implemented |
| Leveled compaction with trivial move | Implemented |
| Multi-threaded background compaction | Implemented |
| Streaming compaction of point entries | Implemented |
| MVCC snapshots with DB-bound leases | Implemented |
| Forward/backward/prefix/range iterators | Implemented |
| Compression: None, LZ4, Zstd (per-level) | Implemented |
| Write backpressure (slowdown/stop) | Implemented |
| DeleteRange (range tombstones) | Implemented |
| CompactRange API (with range filtering) | Implemented |
| Sorted bottommost bulk ingestion (`DB::ingest_sorted`) | Implemented |
| Compaction filter | Implemented |
| Rate limiter (token bucket, wired to compaction) | Implemented |
| DB properties/statistics (reads, writes, flushes, compaction) | Implemented |
| Typed errors with full propagation traces | Implemented |

`max_immutable_memtables` bounds the number of frozen write buffers awaiting
durable installation (default 4). Reads check the newest buffer first. A
dedicated worker writes the oldest buffer to L0, syncs its MANIFEST edit and
retires its WAL. A full queue blocks normal writes and returns `ErrorKind::Busy`
to writes with `no_slowdown`. `DB::flush()` waits for all writes preceding
its generation to reach SSTs. `DB::sync_wal()` makes prior WAL-enabled writes
durable without creating SSTs or appending a fence record. On Linux,
`wal_bytes_per_sync` controls asynchronous writeback before WAL rotation
(default 2 MiB, 0 disables hints).

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
          | (frozen) |
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
2. Append to WAL (group commit: the leader batches multiple writers into one append and one flush, using a single data sync when a grouped write requests `WalSync`)
3. Insert into active MemTable (concurrent skiplist)
4. When MemTable exceeds `write_buffer_size`, freeze and flush to L0 SST
5. Signal background compaction threads so L0 is drained when it reaches `l0_compaction_trigger`

WriteBatch keeps its entries in one reusable WAL-encoded buffer.
`WriteBatch::with_capacity(bytes)` reserves space for the body.
`put_prefixed`, `delete_prefixed`, and `delete_range_prefixed` append the
prefix and suffix directly. Each range endpoint obeys `MAX_USER_KEY_SIZE`.
Submitting an invalid batch returns `InvalidArgument` before any entry is
written. The indexed batch's `into_batch()` also reports that error.

MemTable nodes store their variable-height tower, internal key, and value
in aligned slabs. A node above 256 KiB gets a dedicated slab. The flush
threshold counts allocated slab bytes and range-tombstone endpoint storage.
Writes to a single user key in one active memtable share a 32 MiB metadata
budget, charged per version as the separator length plus 40 bytes. A range
deletion uses its longer endpoint for this charge. Flush releases the budget
for subsequent writes to that key.

### Read Path

1. Active MemTable (newest writes)
2. Immutable MemTables, newest generation first
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
- With L0 pinning and caching enabled, a flush reads and pins the first data block of each new L0 SST. Compaction releases those pins. Each table reader holds the index and range-deletion partition directories and the aligned Bloom filters.
- **Range tombstones**: Iterators query a prebuilt interval index. Compaction tracks overlapping deletions in a heap ordered by sequence.
- **Partitioned index**: An iterator holds its current index leaf. Point reads binary-search the directory and fetch one leaf through the block cache.
- **Atomic L0 counter**: Write-throttle checks use an atomic counter, avoiding mutex contention on writes

### SST Format

```
Data Block 0: [entries with prefix compression + restart points]
Data Block 1: ...
...
Filter Block: [whole-key bloom filter bits]        (when bloom filters are enabled and the file has keys)
Prefix Filter Block: [prefix bloom bits]           (when prefix blooms are configured and the file has keys)
Range deletion leaves: exclusive end -> interval begin and visible sequence numbers
Range deletion directory: maximum end -> leaf handle and minimum begin
Metaindex Block: ["filter.bloom" -> handle,
                  "filter.format" -> 2,
                  "filter.prefix" -> handle,
                  "filter.prefix_len" -> prefix length,
                  "rangedelblock" -> range deletion directory]
Index leaves: separator -> data block handle, first key and block properties
Index directory: upper separator -> index leaf handle
Footer (48 bytes): [metaindex_handle, index_directory_handle, padding, TABLE_MAGIC_V2]
```

The key and prefix Bloom filters use XXH3-64 with seed zero and derive eight independent 9-bit bit probes within one aligned 64-byte cache line (`filter.format = 2`), sized per level with a Monkey bits-per-key schedule (with optional bottommost point-filter elision via `DbOptions::optimize_filters_for_hits`). Index and range-deletion leaves target 64 KiB and can grow to fit a single large entry. Large directories span checksummed pages that the reader loads at open. Adjacent index partitions for one user key share the user-key bytes in memory. A point query checks at most one range-deletion leaf per SST. Flush and compaction clip crossing tombstones at output boundaries and preserve a sequence for each live snapshot stripe.


---

## Source Code Structure

Selected modules. Directory `mod.rs` files are listed only when they contain the
subsystem implementation; pure re-export `mod.rs` files are omitted.

```
src/
+-- lib.rs                  # Public API re-exports
+-- db.rs                   # DB: open/get/put/delete/write/flush/compact/close
+-- db/
|   +-- flush.rs            # FIFO FlushQueue worker & async 0-byte standby WAL rotation
|   +-- ingest.rs           # Bottommost sorted bulk ingestion (DB::ingest_sorted)
|   +-- read.rs             # Lock-free SuperVersion point-read state
|   +-- snapshot.rs         # Registered Snapshot / ArcSnapshot handles
+-- options.rs              # DbOptions, ReadOptions, WriteOptions (RocksDB-compatible)
+-- types.rs                # InternalKey, ValueType, SequenceNumber, WriteBatch
+-- error.rs                # Error types
+-- rate_limiter.rs         # Token-bucket rate limiter
+-- stats.rs                # Database statistics
+-- memtable/
|   +-- mod.rs              # MemTable (put/get/iter with approximate_size tracking)
|   +-- skiplist.rs         # Borrowed internal-key lookup and streaming MemTable cursors
|   +-- skiplist_impl.rs    # Lock-free skiplist implementation (raw pointers, arena allocation)
+-- wal/
|   +-- writer.rs           # WAL append with block-based fragmentation
|   +-- reader.rs           # WAL replay for crash recovery
|   +-- record.rs           # Record format: checksum + length + type + payload
+-- sst/
|   +-- block.rs            # Data block: prefix compression + restart points + seek
|   +-- block_builder.rs    # Block construction
|   +-- table_builder.rs    # SST writer (data + filter + range-del + index + footer)
|   +-- table_reader/
|   |   +-- mod.rs           # SST reader: open, block read/cache, point lookup
|   |   +-- iterator.rs      # TableIterator (cursor-based, deferred block read)
|   +-- filter.rs            # Bloom filter (double hashing)
|   +-- format.rs           # Footer, BlockHandle, CompressionType, index-value encoding
+-- compaction/
|   +-- leveled.rs          # Leveled compaction: streaming merge, trivial move, filter
+-- manifest/
|   +-- version_edit.rs     # VersionEdit encode/decode
|   +-- version.rs          # Version (immutable SST file set snapshot)
|   +-- version_set.rs      # VersionSet (MANIFEST management + version chain)
+-- cache/
|   +-- block_cache.rs      # Block cache (moka) with shared pools + L0 pinning
|   +-- table_cache.rs      # SST reader cache
+-- iterator/
    +-- merge.rs            # Min-heap MergingIterator with buffer reuse + prefetch
    +-- source.rs           # IterSource (peeked-entry adapter) + SeekableIterator trait
    +-- db_iter.rs          # DBIterator: dedup, snapshot, tombstone/range-del filtering
    +-- level_iter.rs       # LevelIterator: lazy two-level iterator for L1+
    +-- range_del.rs        # FragmentedRangeTombstoneList (per-level sequence searches) + sweep-line RangeTombstoneTracker
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
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self>;
    pub fn open_read_only_with_options(options: DbOptions, path: impl AsRef<Path>) -> Result<Self>;
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub fn put_with_options(&self, options: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()>;
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    pub fn get_with_options(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>>;
    pub fn get_pinned(&self, key: &[u8]) -> Result<Option<PinnedSlice>>;
    pub fn get_pinned_with_options(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<PinnedSlice>>;
    pub fn contains_key(&self, key: &[u8]) -> Result<bool>;
    pub fn contains_key_with_options(&self, options: &ReadOptions, key: &[u8]) -> Result<bool>;
    pub fn sync_wal(&self) -> Result<()>;
    pub fn delete(&self, key: &[u8]) -> Result<()>;
    pub fn delete_with_options(&self, options: &WriteOptions, key: &[u8]) -> Result<()>;
    pub fn delete_range(&self, begin: &[u8], end: &[u8]) -> Result<()>;
    pub fn delete_range_with_options(&self, options: &WriteOptions, begin: &[u8], end: &[u8]) -> Result<()>;
    pub fn write(&self, batch: WriteBatch) -> Result<()>;
    pub fn write_with_options(&self, options: &WriteOptions, batch: WriteBatch) -> Result<()>;
    pub fn ingest_sorted<I, K, V>(&self, iter: I) -> Result<u64>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>;
    pub fn iter(&self) -> Result<DBIterator>;
    /// ReadOptions bounds prune SST point sources before iteration starts;
    /// range tombstones that can cover the scan are retained independently.
    pub fn iter_with_options(&self, options: &ReadOptions) -> Result<DBIterator>;
    pub fn iter_with_batch(&self, batch: &WriteBatchWithIndex) -> Result<DBIterator>;

    /// Prefix-bounded iteration — the fastest option for prefix-scoped queries.
    ///
    /// Applies two levels of SST pruning:
    ///   1. Range pruning  — skips files whose key range doesn't overlap the prefix.
    ///   2. Bloom filter   — when `DbOptions::prefix_len > 0`, skips files whose
    ///                       prefix bloom reports the prefix absent (finer-grained).
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
    /// | Method               | SST range pruning | Bloom filter pruning        |
    /// |----------------------|:-----------------:|:---------------------------:|
    /// | `iter()`             | ✗                 | ✗                           |
    /// | `iter_with_range()`  | ✓                 | ✗                           |
    /// | `iter_with_prefix()` | ✓                 | ✓ (requires `prefix_len > 0`) |
    pub fn iter_with_range(&self, options: &ReadOptions, lower_bound: Option<&[u8]>, upper_bound: Option<&[u8]>) -> Result<DBIterator>;

    /// RAII snapshot — automatically released on drop.
    pub fn snapshot(&self) -> Snapshot<'_>;
    pub fn arc_snapshot(&self) -> ArcSnapshot;

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
    pub fn error(&self) -> Option<&Error>;
    pub fn step_next(&mut self) -> Result<Option<(&[u8], &[u8])>>;
    pub fn step_prev(&mut self) -> Result<Option<(&[u8], &[u8])>>;
}

impl Snapshot<'_> {
    pub fn sequence(&self) -> SequenceNumber;
    pub fn read_options(&self) -> ReadOptions<'_>;
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    pub fn get_pinned(&self, key: &[u8]) -> Result<Option<PinnedSlice>>;
    pub fn contains_key(&self, key: &[u8]) -> Result<bool>;
    pub fn iter(&self) -> Result<DBIterator>;
}
```

A borrowed `Snapshot` keeps its registered versions available until drop.
Use `DB::arc_snapshot()` to share that registration across threads. Read
through either snapshot directly or pass `snapshot.read_options()` to its database. The last clone of an
`ArcSnapshot` releases the registration. Reading from a closed database
returns `DbClosed`, and reading an open snapshot on another database returns
`InvalidArgument`.

`get_pinned` returns a borrowed view backed by a retained SST block or
memtable arena. Pins can outlive cache eviction and database close. A small
value can keep an entire block (up to 64 MiB) or arena alive outside the
normal storage budgets. Call `into_vec()` to retain only the value bytes.

Both iterator types support lending `step_next` and `step_prev`. Each step
returns a typed error immediately if reading or resolving the row fails,
including lookahead into the following block. On a `BidiIterator` the two
steps consume opposite ends of the remaining range.

## Migrating a v4 store

v5 reads the database format, level count (2 through 16), and prefix length
from the first MANIFEST record. A writable reopen requires the same geometry.
To migrate a stable v4 store into a fresh v5 directory, run the exporter
from this checkout. Choose a destination outside the source directory.
The arguments below use three levels and two prefix
bytes. Omit both numbers for the v4 defaults of seven and zero.

```sh
cargo +stable run --manifest-path tools/v4-export/Cargo.toml -- /data/v4 /data/v5 3 2
```

The exporter opens the source read-only with MMDB 4.3.15, replays residual
WALs, streams visible key/value pairs directly into bottommost SSTs via
`DB::ingest_sorted`, checks the final iterator error, and closes the v5
database before publishing the destination. For v4 stores with more than
sixteen levels it writes sixteen v5 levels. Snapshot registrations and
physical SST/WAL history live only in the source. Use the vsdb logical
exporter when migrating vsdb collection formats.

For Rust callers moving to v5:

- Construct options with `Default` or a preset and chain `with_*` setters.
  Iteration bounds accept owned buffers and borrowed slices.
- Select `DurabilityMode::{WalAsync, WalSync, DisableWal}` on `WriteOptions`.
  `DB::sync_wal()` fences preceding WAL writes without adding a record.
- Pass a live snapshot handle to `with_snapshot`. Both snapshot types also
  provide `read_options()`.
- Collect block properties from the user keys of all values and deletions.
  Keep each collector name's meaning stable. A filter may skip a block only
  when every collected key fails its `key_matches` predicate. The iterator
  checks that predicate on the resolved row. Point reads ignore scan filters.
- Use the prefixed batch methods to append a prefix and key directly to the
  WAL payload. `WriteBatchWithIndex::range_tombstones()` iterates borrowed
  endpoints and write positions. Propagate `into_batch()?` to check admission.
- Use `pin_l0_first_block_and_warm_index` for L0 pinning, and
  `optimize_filters_for_hits` to omit bottommost point Bloom filters on
  hit-dominated stores.

## Read-only databases

`DB::open_read_only` opens an existing store without modifying its directory.
Valid records from residual WAL files are replayed into memory, so reads see
the same recovered state as a writable open while `CURRENT`, MANIFEST, WAL,
SST, and `LOCK` files remain unchanged through open, reads, `close`, and drop.

```rust
use std::path::Path;

use mmdb::{DB, ErrorKind};

fn inspect(path: &Path) -> mmdb::Result<()> {
    let db = DB::open_read_only(path)?;

    if let Some(value) = db.get(b"key")? {
        println!("{}", String::from_utf8_lossy(&value));
    }

    // A read-only handle reports mutation attempts explicitly.
    let err = db.put(b"key", b"new value").unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ReadOnly);

    db.close()
}
```

Use `DB::open_read_only_with_options` to configure read and cache behavior.
Read-only recovery adopts the stored level count and prefix length.

```rust
use std::path::Path;

use mmdb::{DB, DbOptions};

fn open_custom(path: &Path) -> mmdb::Result<DB> {
    DB::open_read_only_with_options(
        DbOptions::default().with_block_cache_capacity(256 * 1024 * 1024),
        path,
    )
}
```

| Operation on a read-only handle | Behavior |
|---------------------------------|----------|
| `get*`, iterators, snapshots, properties | Fully supported |
| `close` and drop | Release resources without changing the store |
| `put*`, `delete*`, `write*`, `ingest_sorted`, `sync_wal`, `flush`, `compact*` | Return `ErrorKind::ReadOnly` before side effects |
| `lazy_delete*` | No-op (the existing infallible API cannot return an error) |

On Unix, an existing `LOCK` file is opened without write access and held with
a shared, non-blocking lock. Multiple read-only handles can coexist, while a
writer is rejected. If `LOCK` is absent, no file is created and the handle
proceeds unlocked; the caller must keep the directory stable for the handle's
entire lifetime. Platforms without Unix `flock` should also be treated as
unlocked. Never use an unlocked read-only handle alongside a live writer,
because recovery could mix MANIFEST, SST, and WAL states and produce
incomplete reads.

---

## Error Handling

All fallible APIs return `mmdb::Result<T> = Result<T, mmdb::Error>`.

`Error` carries a typed [`ErrorKind`] (`Io`, `Corruption`, `InvalidArgument`,
`DbClosed`, `ReadOnly`, `Busy`, `Background`) for programmatic matching, plus a full
propagation trace — the origin `file:line:column` and one frame per `.ctx()` /
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
let opts = DbOptions::default()
    .with_write_buffer_size(64 * 1024 * 1024)
    .with_block_cache_capacity(256 * 1024 * 1024)
    .with_block_size(16384)
    .with_l0_compaction_trigger(8)
    .with_compression(CompressionType::Lz4)
    .with_bloom_bits_per_key(12)
    .with_max_background_compactions(4);

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
cargo test               # unit + crash-recovery + integration + e2e + proptest suites
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

## AI maintenance

[AGENTS.md](AGENTS.md) contains the shared project guide and links to the four
explicit `.agents/skills/` workflows. Their
[workflow policy](docs/ai/workflow-policy.md) defines review scope, worktree
ownership and local commit rules; subsystem guides live in `docs/ai/`.

## License

MIT

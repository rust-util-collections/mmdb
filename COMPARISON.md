# MMDB, RocksDB, and Pebble

## Common operations

These are entry points for readers familiar with the RocksDB 9.10 C++ or
Pebble 1.1 Go APIs. MMDB v5 operates on a single local byte-key space. Its
batch format, SST format, and configuration belong to MMDB.

| Operation | MMDB v5 | RocksDB 9.10.0 | Pebble 1.1.5 |
|-----------|---------|----------------|--------------|
| Read a value | `DB::get` returns owned bytes. `get_pinned` can retain the SST block or memtable | `DB::Get` can fill a string or a `PinnableSlice` | `DB.Get` returns bytes and a closer |
| Group mutations | `WriteBatch` and `DB::write` | `WriteBatch` and `DB::Write` | `Batch` and `DB.Apply` |
| Delete a key range | `DB::delete_range` covers `[begin, end)` | `DB::DeleteRange` | `DB.DeleteRange` |
| Read a stable view | `DB::snapshot` or `DB::arc_snapshot` | `DB::GetSnapshot` | `DB.NewSnapshot` |
| Position a cursor | `seek` (≥), `seek_for_prev` (≤) | `Seek` (≥), `SeekForPrev` (≤) | `SeekGE` (≥), `SeekLT` (<) |
| Report an iterator failure | Lending steps return `Result`. Owned iteration exposes `error()` | `Iterator::status` | `Iterator.Error` |

For signatures and lifetimes, see the [MMDB API](src/lib.rs), [RocksDB database](https://github.com/facebook/rocksdb/blob/v9.10.0/include/rocksdb/db.h) and [iterator](https://github.com/facebook/rocksdb/blob/v9.10.0/include/rocksdb/iterator_base.h) interfaces, and the [Pebble database](https://github.com/cockroachdb/pebble/blob/v1.1.5/db.go) and [iterator](https://github.com/cockroachdb/pebble/blob/v1.1.5/iterator.go) implementations.

## MMDB storage choices

| Area | What v5 implements |
|------|--------------------|
| Scans | Range and prefix bounds, prefix Bloom pruning, lending forward/backward steps, block-property filters, and a lazy double-ended iterator |
| Values | SST data blocks hold inline values. Pinned reads retain the block or memtable that supplies the bytes |
| Deletions | Point deletions and half-open range tombstones participate in MVCC. Range tombstones have their own partitioned SST index. The API has no range-key set/unset operations |
| Writes | A group-commit leader serializes WAL append and memtable application. Sorted disjoint input can instead be streamed directly to the bottommost level |
| Compaction | Leveled compaction with optional parallel subcompactions, read-triggered level hints, snapshot-aware deletion handling, and a user compaction filter |
| Cache and compression | Shareable block-cache pools, member-local L0 pins, resident point-index and range-deletion directories and Bloom filters, and per-level choice of uncompressed, LZ4, or Zstd data blocks |

For file operations and resource bounds, see the [v5 architecture](docs/architecture.md). The [design record](docs/proposals/v5-roadmap.md) covers the proposed write pipeline and large-value separation. The repository's [benchmarks](benches/benchmarks.rs) measure MMDB workloads.

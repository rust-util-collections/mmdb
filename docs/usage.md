# Using MMDB v6

A `DB` handle operates on a local directory. Keys and values are byte
strings. For a complete first program, see the [getting-started example](../README.md#getting-started).

## Open and configure a store

`DB::open(options, path)` opens a writable store. `CURRENT` identifies an
existing database. The default, `create_if_missing = true`, also creates a
missing store. Set `with_create_if_missing(false)` when a typo in the path
should fail. To create a new store only, set `with_error_if_exists(true)`.

Creation records two settings in the MANIFEST. Supply the same `num_levels`
and `prefix_len` on each writable reopen. A mismatch returns
`ErrorKind::InvalidArgument`. If `CURRENT` is missing alongside an SST or WAL,
creation returns `ErrorKind::Corruption`. MMDB syncs the parent of each
directory it creates. If you supply an existing directory under a search-only
parent, its entry must already be durable. [Creation and recovery](architecture/recovery.md)

### Read-only databases

`DB::open_read_only(path)` requires an existing store with `CURRENT`. Use
`DB::open_read_only_with_options(options, path)` to customize read and cache
settings. Both adopt the persisted level count and prefix length and replay
eligible WAL records into memory. A large residual WAL can therefore require
more RAM than `write_buffer_size`. Opening, reading, and closing leave the
database files intact.

On Unix, read-only handles share a nonblocking lock on the existing `LOCK`
file. A writable handle requires an exclusive lock. If `LOCK` is missing,
read-only open proceeds unlocked. In that case, and on platforms without Unix
`flock`, the caller must keep the directory stable until the handle closes.

Reads, scans, snapshots, and properties are available. On a usable read-only
handle, fallible disk mutations, including an empty `write`, `ingest_sorted`,
`flush`, `sync_wal`, and `compact_range`, return `ErrorKind::ReadOnly`.
`lazy_delete` and `lazy_delete_batch` have no effect. [Read-only tests](../tests/read_only.rs)

### Common settings

Start with `DbOptions::default()` (also named `balanced()`) or a preset such
as `DbOptions::read_heavy()` or `DbOptions::write_heavy()`. Apply overrides
with `with_*` setters. The values below are for `DbOptions::default()`.
[All options and presets](../src/options.rs)

| Option | Default | Meaning |
|---|---|---|
| `num_levels`, `prefix_len` | 7, 0 | Persisted level count including L0 (2–16) and fixed prefix length in bytes. Zero disables prefix Bloom filters. |
| `write_buffer_size`, `max_immutable_memtables` | 64 MiB, 4 | Active memtable freeze threshold and maximum frozen generations awaiting durable installation. |
| `wal_bytes_per_sync` | 2 MiB | Interval for advisory Linux WAL writeback. Zero disables hints. Write durability is selected per batch. |
| `block_size`, `compression` | 4 KiB, `None` | SST data-block target and default codec. `compression_per_level` selects per-level codecs. |
| `block_cache_capacity`, `max_open_files` | 64 MiB, 1,000 | Byte capacity **per evictable store** in a private pool, and maximum entries in the table cache. Live versions and iterators may hold additional open readers. |
| `bloom_bits_per_key` | 12 | Base SST filter budget in bits per key. Zero disables Bloom filters. |
| `l0_compaction_trigger`, `l0_slowdown_trigger`, `l0_stop_trigger` | 4, 8, 12 | Installed L0 file counts for compaction, progressive write delay, and write stalls. |
| `max_background_compactions`, `max_subcompactions` | 1, 1 | Background compaction workers and maximum parallel ranges per eligible job. L0 compaction runs serially. |

To share a block cache across databases, pass the same pool using
`with_block_cache(Some(pool))`. The pool sets both stores' capacities.
The four-host study measured the effects of cache size, compression, and
write-buffer settings under specific workloads. [Tuning measurements](benchmarks/four-host/README.md)

## Write and choose durability

`DB::write(batch)` makes all mutations in a successful `WriteBatch` visible
together. A batch can put values, delete individual keys, or delete a half-open
range `[begin, end)`. The prefixed methods concatenate prefix and suffix. For
`delete_range_prefixed` the prefix applies to both endpoints.

| On successful return from… | What the caller can rely on |
|---|---|
| `DB::write`, or a write using `WalAsync` | The batch is visible. MMDB has flushed its WAL buffer to the OS. A power loss can still lose the batch. |
| A write with `WalSync` | The batch is visible and MMDB has confirmed WAL data durability. |
| A write with `DisableWal` | The batch is visible in memory. An interruption before durable SST installation can lose it. |
| `DB::sync_wal()` | Preceding WAL-enabled writes are durable. The fence adds no record or sequence. For `DisableWal` data, use `flush`. |
| `DB::flush()` | Writes through the generation captured by the call are in durably installed SSTs, including `DisableWal` writes. Concurrent later writes may still be in memory. |
| `DB::close()` | A healthy writable handle has flushed its final generation, stopped its workers, and released its directory lock. Explicit close reports flush, sync, or earlier background failures. |

For a single batch, pass `WriteOptions::default().with_durability(DurabilityMode::WalSync)`
to `write_with_options(&options, batch)`. Use `close()` when the caller needs
the final write or shutdown error. `Drop` attempts a WAL sync and cannot
report failure. [Write and flush implementation](architecture/writes.md)

### Batch limits

A write validates the batch before applying it. Each key and each range
endpoint must fit `MAX_USER_KEY_SIZE` (8 MiB). The combined key and value
must fit `MAX_WRITE_ENTRY_SIZE`, or the two endpoints must fit
`MAX_RANGE_DELETE_SIZE`. Within one active memtable, versions at the same
key share a 32 MiB metadata budget. Each version charges the key length plus
40 bytes, using the longer endpoint for a range deletion.
[Batch encoding and public limits](../src/types.rs)

### Sorted ingestion

`DB::ingest_sorted` bulk-loads strictly increasing, unique byte keys directly
into bottom-level SSTs. The **entire span** from the first key to the last
must be disjoint from existing memtable entries, live SST key ranges, and
range deletions. For example, a stored key `b` between input keys `a` and `c`
makes that input invalid even if neither input key exists. A successful call
returns the row count and publishes all rows at one sequence. An empty input
returns zero on a usable writable handle. [Ingestion and snapshot visibility](architecture/writes.md#sorted-ingestion)

## Read, pin, and iterate

| Need | API and result |
|---|---|
| Own a copy of a value | `get` returns `Option<Vec<u8>>`. A missing or deleted key yields `None`. |
| Test whether a key is visible | `contains_key` checks existence without copying the value. |
| Borrow stored bytes beyond the read call | `get_pinned` returns a `PinnedSlice`. A nonempty pin can retain a whole SST block or memtable through eviction, flush, and close. `into_vec()` lets the caller keep only the value bytes. |
| Read several keys at one sequence | `get_many_pinned` returns one slot per input key in input order. Duplicates occupy separate slots. Any lookup error fails the call. |

### Snapshots and grouped reads

`DB::snapshot()` borrows the handle. `DB::arc_snapshot()` returns a cloneable
snapshot. Both register a sequence with the originating DB until the borrowed
snapshot or the last owned clone drops. Read through the snapshot or pass its
`read_options()` to the same DB. A different open DB rejects the snapshot with
`InvalidArgument`. New reads through a snapshot after its DB closes return `DbClosed`.

A grouped call accepts up to `MAX_GROUPED_READ_KEYS` (32) input slots. To
read more keys at one sequence, reuse a snapshot across chunks. This example
counts visible input slots, including repeated hits, and releases each chunk's
pins before requesting the next one.

```rust
fn count_hits(db: &mmdb::DB, keys: &[&[u8]]) -> mmdb::Result<usize> {
    let snapshot = db.snapshot();
    let options = snapshot.read_options();
    let mut hits = 0;
    for chunk in keys.chunks(mmdb::MAX_GROUPED_READ_KEYS) {
        let values = db.get_many_pinned_with_options(&options, chunk)?;
        hits += values.iter().flatten().count();
    }
    Ok(hits)
}
```

### Range and prefix scans

`iter_with_range` intersects its explicit bounds with those in `ReadOptions`.
The lower bound is inclusive and the upper bound exclusive. `iter_with_prefix`
matches the full requested prefix. Both scans honor visible range deletions.
For a one-off scan, `with_fill_cache(false)` serves cached hits and skips cache
insertion on misses. Scan predicates apply to visible rows. `iter_with_batch`
also overlays pending mutations from a `WriteBatchWithIndex`. [Scan behavior](architecture/reads.md#range-and-prefix-scans)

`step_next()` and `step_prev()` lend slices and return a typed error on the
failing step, including a lookahead failure. The standard `Iterator` yields
owned pairs. After consuming it, check `error()` to distinguish exhaustion
from failure. `BidiIterator::lazy` consumes the remaining scan from either
end, starting at the wrapped iterator's current position. [Iterator example](../examples/basic.rs)

## Compaction and properties

`DB::compact()` waits for already queued flushes and compacts the SST levels.
The current active memtable can still contain writes. For a nonempty
`[begin, end)`, `DB::compact_range(begin, end)` first flushes the current
generation, then compacts overlapping files. A selected file can include keys
outside the interval. `None` means unbounded on that side. A positive
`rate_limiter_bytes_per_sec` paces compaction writes.

`lazy_delete` and `lazy_delete_batch` register keys in memory for removal by
a later compaction. A registered key can remain readable until the rewrite,
and restarting loses pending registrations. Use a point or range delete for a
deletion that must be visible when the call returns. [Compaction and snapshots](architecture/compaction.md)

### Measuring storage and memory

| API | Scope and unit |
|---|---|
| `get_property("num-files-at-level0")`, `get_property("total-sst-size")` | Number of installed L0 files, and total bytes in live SSTs. `stats.flushes_completed` counts completed memtable flushes. |
| `get_property("block-cache-usage")` | Approximate **entry count** across the pool's evictable stores plus this DB's L0 pins. |
| `db.block_cache_pool().usage()` | Per-store byte capacities and approximate evictable weighted bytes. Sample once per shared pool. |
| `db.memory_usage()` | Memtable byte estimates, immutable memtable count, and L0 pin payload bytes for this DB. These gauges reach zero after close. |

A returned `PinnedSlice` or iterator can still retain memory after close.
Measure process RSS separately to include those allocations, live table
readers, and allocator overhead. [Cache ownership and gauge definitions](architecture/sst-and-cache.md#memory-accounting)

## Errors

Fallible operations return `mmdb::Result<T>`. Match `err.kind()` for handling,
and use the displayed message and propagation frames for diagnosis. An I/O
error retains its underlying source. [Error types](../src/error.rs)

| `ErrorKind` | Example and caller action |
|---|---|
| `InvalidArgument` | A writable reopen used a different level count or prefix length. Supply the geometry used at creation. |
| `Busy` | A valid nonempty `no_slowdown` write met a full immutable queue or the L0 slowdown threshold. That batch consumed no WAL record or sequence. Retry after the backlog drains. An admitted request can still wait for its group. |
| `ReadOnly` | A disk mutation was requested on a usable read-only handle. Use a writable handle for the change. |
| `DbClosed` | A new read or write used a closed handle. Open the store again to start a new session. |
| `Corruption` | Stored bytes or metadata fail an integrity check, such as an unsupported MANIFEST header. The error identifies the failed check. |
| `Io` | A filesystem operation failed. Inspect the underlying I/O error and path. |
| `Background` | The open handle latched a storage failure. Further reads and writes fail. Close the handle, inspect the failure, and reopen to recover. |

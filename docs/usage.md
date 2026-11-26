# Using MMDB v6

## Open and configure a store

`DB::open(options, path)` opens a writable store. By default it creates a
missing database. MMDB uses the presence of `CURRENT` to identify an existing
store; if `CURRENT` is missing from a directory that already contains persisted
store files, writable creation returns `ErrorKind::Corruption` rather than
overwriting them. When creating missing directories, the opener needs access to sync
their containing directories before publishing the store. A caller supplying
an existing directory under a search-only parent is responsible for the
durability of that existing directory entry. At creation MMDB records `num_levels` and `prefix_len` in the MANIFEST.
Supply the same values on every writable reopen. [Upgrading to v6](migration.md)

| Option | Default | What it controls |
|--------|---------|------------------|
| `num_levels`, `prefix_len` | 7, 0 | Level count including L0 (2–16), and fixed prefix Bloom length in bytes (0 disables prefix Bloom filters) |
| `write_buffer_size`, `max_immutable_memtables` | 64 MiB, 4 | Active memtable freeze threshold and number of frozen buffers awaiting installation |
| `wal_bytes_per_sync` | 2 MiB | Interval for advisory Linux WAL writeback. Zero disables hints |
| `block_size`, `compression` | 4 KiB, `None` | Target size and compression of SST data blocks. LZ4 and Zstd are available per level |
| `block_cache_capacity`, `max_open_files` | 64 MiB, 1000 | Private block-cache sizing and table-cache entry limit. Live versions and iterators can retain additional table readers |
| `bloom_bits_per_key` | 12 | Base bits per key for SST filters. Zero disables them |
| `l0_compaction_trigger`, `l0_slowdown_trigger`, `l0_stop_trigger` | 4, 8, 12 | L0 file counts for compaction, write slowdown, and write stalls |
| `max_background_compactions`, `max_subcompactions` | 1, 1 | Compaction worker count and maximum parallel ranges within an eligible compaction |

Start with `DbOptions::default()` or the `balanced()`, `read_heavy()`, and
`write_heavy()` presets. Chain `with_*` setters for overrides. Supplying
`with_block_cache(Some(pool))` shares the pool across handles and gives the
pool control over cache sizing. [Options and presets](../src/options.rs) · [Cache accounting](architecture.md#sst-v2-and-cache-memory)

For a machine with ample RAM, budget the decoded working set and the
kernel's resident file pages separately. If several DBs share a pool,
configure its capacity once: each of its two weighted stores gets that
capacity. For latency-sensitive reads, measure the effective hit fraction
and process RSS before increasing it. When values contain repeated data,
`with_compression(CompressionType::Lz4)` can save substantial SST space
at the cost of decoding on misses. The four-host trials give paired
measurements for these choices on APFS, Btrfs, ext4 and ZFS.
[Measured tuning examples](benchmarks/four-host/README.md)

## Write and choose durability

| Operation | Guarantee on success |
|-----------|----------------------|
| `DB::write(batch)` or `WalAsync` (default) | The whole batch is visible. WAL bytes have reached the OS and may still need a data sync to survive a power loss |
| `WalSync` | MMDB confirms WAL data durability before acknowledging the batch |
| `DisableWal` | The batch is visible in memory. A crash before durable flush can lose it |
| `DB::sync_wal()` | Fences preceding WAL-enabled writes. Adds no record and consumes no MVCC sequence |
| `DB::flush()` | Waits for writes through the captured flush generation to reach durably installed SSTs, including writes made with `DisableWal` |
| `DB::close()` | On a healthy writable handle, drains the final flush and shuts down workers. Returns flush, sync, or background errors. An implicit drop cannot report such errors |

`WriteOptions::default().with_durability(DurabilityMode::WalSync)` selects
per-batch sync for `write_with_options` and the other option-taking writers.
`WriteBatch` accepts puts, point deletes, and half-open range deletes. Its
prefixed variants append the prefix and suffix directly. Batch submission
reports invalid entry sizes before applying the batch. Keys and both range
endpoints are limited by `MAX_USER_KEY_SIZE` (8 MiB). The combined payload
must also fit `MAX_WRITE_ENTRY_SIZE` for a point entry or `MAX_RANGE_DELETE_SIZE`
for a range deletion. In one active memtable, successive versions at the same
user key share a 32 MiB metadata budget. Each version consumes the key length
plus 40 bytes, using the longer endpoint length for range deletions. `WriteBatchWithIndex`
also supports reading pending changes through `DB::iter_with_batch`.
[Write options](../src/options.rs) · [Batch encoding and limits](../src/types.rs)

## Read, pin, and iterate

- `get` returns owned bytes. `contains_key` checks visibility without copying the result value. A nonempty `get_pinned` result keeps the underlying SST block or memtable alive across eviction and database close. Use `into_vec()` when a small retained value would otherwise keep a large allocation alive.
- `DB::get_many_pinned` accepts up to `MAX_GROUPED_READ_KEYS` (32) keys. It captures one read view and one sequence, returning one result per input position. Duplicate keys keep both positions, a miss yields `None`, and any read failure returns a typed error for the request. For more than 32 keys, use the same snapshot in `db.get_many_pinned_with_options(&snapshot.read_options(), chunk)` for every chunk when the caller needs a common sequence. Groups with nearby keys can reuse SST index leaves and data blocks. On successful return, duplicate hits each contribute their key and value length to `stats.bytes_read`. A failed group adds zero returned bytes. The request can keep a leaf and data block for each of two recent files, in addition to the returned pins. Each result pin can retain a whole block or memtable after eviction or close.
- `DB::snapshot()` creates a borrowed handle. `DB::arc_snapshot()` creates a cloneable one. Both register a sequence with the originating database. Read through the handle or pass its `read_options()` to that database. The registration ends when the borrowed snapshot or last owned clone drops. Using an open snapshot with another database returns `InvalidArgument`. Reads through a snapshot after database close return `DbClosed`.
- `iter_with_range` intersects its explicit bounds with the bounds in `ReadOptions`. The lower bound is inclusive and the upper bound exclusive. `iter_with_prefix` also enforces the full requested prefix. An SST with a prefix Bloom filter can prune point entries when the requested prefix is at least as long as the stored prefix length. Both scans still account for overlapping range tombstones.
- `step_next()` and `step_prev()` lend key and value slices and return a typed error on the failing step, including a lookahead failure. The standard `Iterator` yields owned pairs. After consuming it, check `error()` to distinguish exhaustion from failure. `BidiIterator::lazy` consumes the remaining scan from either end starting at the wrapped iterator's current position.
- Use `ReadOptions::with_fill_cache(false)` for a one-off scan to read cached hits and avoid inserting misses. Block-property and skip-point filters apply to scans. The engine checks the user-key predicate on the resolved row after applying snapshot visibility and deletions.

[Point and scan paths](../src/db/read.rs) · [Snapshot handles](../src/db/snapshot.rs) · [Iterators](../src/iterator/)

## Read-only databases

`DB::open_read_only(path)` requires an existing store with `CURRENT`.
`DB::open_read_only_with_options(options, path)` also accepts read/cache tuning.
Both adopt the persisted level count and prefix length. They replay eligible
WAL records into memory and retain that recovered state for the handle's
lifetime. A large residual WAL set can therefore require more memory than the
configured write buffer. Open, reads, close, and drop leave store files intact.

On Unix an existing `LOCK` file is opened read-only and held with a
nonblocking shared lock. Multiple readers can coexist. An exclusive writer
conflicts with those readers. If `LOCK` is absent, read-only open proceeds
unlocked. In that case, and on platforms without Unix `flock`, the caller
must keep the directory stable for the entire handle lifetime.

Point reads, scans, snapshots, and properties work normally. Disk mutations,
including an empty `write`, `ingest_sorted`, `flush`, `sync_wal`, and
`compact_range`, return `ErrorKind::ReadOnly` on a usable read-only handle.
The infallible `lazy_delete` and `lazy_delete_batch` methods are no-ops.
[Open and close paths](../src/db.rs) · [Read-only acceptance tests](../tests/read_only.rs)

## Compaction and properties

`DB::compact()` waits for previously queued flushes and compacts the SST
levels. For a nonempty half-open range, `DB::compact_range(begin, end)`
first flushes the current generation, then selects overlapping files. Files
can contain keys outside the requested interval. Use `None` for an open end.
`rate_limiter_bytes_per_sec` limits compaction writes when set above zero.

`lazy_delete` and `lazy_delete_batch` register keys in memory for removal by
compaction. The key can remain readable until the rewrite. A restart loses
registrations that have not yet taken effect. Use an ordinary point or range
delete when reads must observe the deletion immediately.

`get_property("num-files-at-level0")` reports the L0 file count.
`total-sst-size` reports bytes and `stats.flushes_completed` counts flushes.
`block-cache-usage` reports an approximate **entry count**. For a shared pool,
it includes unpinned entries across all members plus this member's pins.
[Maintenance APIs and property names](../src/db.rs)

Use `db.block_cache_pool().usage()` for the **byte** capacity and approximate
evictable charge of each of the segmented and overflow stores. Sample a shared
pool once across its DB handles. If the entire capacity fits in one segment,
no block is eligible for the overflow store and its weighted charge stays
zero even though its configured capacity is reported. For each DB, `db.memory_usage()` reports the
active memtable estimate, immutable table count and byte estimate, and the
payload bytes in that DB's L0 pin map. After close, the DB-local gauges are
zero. A caller can still hold bytes through a previously returned
`PinnedSlice`. Measure process RSS independently to include retained readers,
allocator overhead and memory outside these gauges.
[Pool usage](../src/cache/block_cache.rs) · [DB memory usage](../src/db/memory.rs)

## Errors

Fallible operations return `mmdb::Result<T>`. Match `err.kind()` for
programmatic handling. For example, a writable reopen with incompatible
geometry returns `InvalidArgument`, an unsupported MANIFEST header returns
`Corruption`, and a valid nonempty write with `no_slowdown` returns `Busy` if the
immutable queue is full or the L0 file count reaches the slowdown threshold.
This admission refusal leaves the batch unapplied and consumes no WAL record
or sequence. Retry after the backlog drains. Concurrent requests can still
wait for their write group to complete. A latched background failure
causes subsequent reads and writes on that handle to return `Background`. The error's display includes its
origin and propagation frames. An I/O error retains its underlying source.
[Error kinds](../src/error.rs) · [Write admission](../src/db.rs)

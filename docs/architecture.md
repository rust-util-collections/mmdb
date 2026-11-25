# MMDB v5 storage architecture

## From a write batch to a durable SST

1. **Group commit.** A leader admits queued batches and assigns a contiguous sequence range to each. It appends one logical WAL record per WAL-enabled batch, using the batch header and encoded body as separate slices. The group shares a WAL flush, or a data sync when required. The leader then inserts admitted entries into the active memtable in sequence order and publishes the committed sequence. Readers see the batch atomically. Group WAL I/O and memtable insertion are serialized under `DB::inner`.
2. **Freeze.** When the write buffer reaches its size threshold, the leader seals the active WAL with `sync_data()` outside `inner`. It takes a ready zero-length standby WAL if available, or creates a new WAL. It publishes a new active memtable and puts the frozen one at the front of the immutable list. The skiplist stores point entries in arena slabs. The flush threshold accounts for allocated slab capacity and range-deletion storage.
3. **Flush.** A dedicated worker builds L0 SSTs for the oldest frozen generation. It syncs the files and their directory, installs the MANIFEST edit, and confirms MANIFEST durability before retiring the immutable memtable and its WAL. A bounded queue allows later writes to proceed while earlier generations flush. `DB::flush()` captures a generation and waits for it.
4. **Compaction.** L0 files may overlap. At L1 and deeper, files are ordered with disjoint point-key ranges. Compaction claims inputs, builds outputs outside the DB mutex, then validates the install against current state. An eligible file can move levels with its bytes intact. Non-L0 compactions can split work at target-level file boundaries. Superseded files become eligible for removal after the replacement is durable.

A regular write that has already been published keeps its successful
acknowledgement if a later automatic freeze fails. The database latches the
failure for subsequent operations. Compaction preserves versions needed by
registered snapshots and clips crossing range tombstones at output boundaries.
Bottommost garbage collection accounts for overlapping deeper data and
snapshot cutoffs. A lazy-delete registration takes effect through compaction.

[Group commit and freeze](../src/db.rs) · [Flush queue](../src/db/flush.rs) · [Memtable](../src/memtable/) · [Leveled compaction](../src/compaction/leveled.rs)

### Sorted ingestion

`DB::ingest_sorted` takes strictly ascending key/value pairs and returns a row
count. It serializes against writers and builds SSTs directly at the bottommost
level, bypassing the WAL and memtable. The entire span from the first key to
the last must be disjoint from existing memtable entries and range tombstones, SST key ranges, and the
range-deletion partition extents of every live SST. Even distinct input keys can therefore overlap an
existing file. Duplicates, out-of-order input, excessive entry sizes, and
overlap return `InvalidArgument`. On a usable writable handle empty input
returns zero.

All ingested rows share one fresh sequence. Ingest holds a compaction claim
on its new SSTs until it publishes that sequence and the read view. The files
are installed in one MANIFEST edit so earlier snapshots keep their previous
view. Before install, failed builds are cleaned up through `PendingSsts`.
If the edit has been applied but its sync fails, MMDB retains the referenced
files and latches the failure. [Implementation and tests](../src/db/ingest.rs)

## Reading a point and scanning a range

A reader pins the `ArcSwap<SuperVersion>` view before choosing its read
sequence. This keeps its memtables and SST version alive across a concurrent
flush or compaction. Pinning the view avoids taking the writer's `inner`
mutex. Internal keys order by user key first, then by descending packed
sequence and value type.

| Read operation | Sources and visibility |
|----------------|------------------------|
| Point lookup | Probe the active memtable and then immutable memtables newest first. Search overlapping L0 files newest first. Binary-search L1+ file ranges for at most one point candidate per level. A covering range deletion can extend beyond a file's point bounds, so the reader checks those tombstones independently |
| Range or prefix scan | Merge memtable cursors, overlapping L0 table cursors, and a lazy per-level cursor for each nonempty deeper level. Each deeper cursor prunes files and creates per-file iterators on demand. Resolve duplicate versions and deletions at the read sequence, then apply user-key predicates |
| Snapshot read | Resolve visibility at the registered sequence. Compaction keeps the versions needed by live snapshots. A borrowed snapshot holds a reference to its DB. An owned snapshot shares a lease with a weak reference to the read state |

The active memtable keeps range deletions in a separate list. Frozen
memtables build a fragmented coverage index on demand. An SST point lookup
uses the range-deletion directory and reads at most one range-deletion leaf
in that file. Each Version tracks the readers containing range deletions
separately for each level. A point-only level skips this part of the lookup.
Scan setup reads the overlapping ranges even when Bloom or file
bounds prune the corresponding point source.

[Point lookup and scan setup](../src/db/read.rs) · [Snapshot leases](../src/db/snapshot.rs) · [Merge and visibility](../src/iterator/)

## Database files and recovery

| File | Role in v5 |
|------|------------|
| `LOCK` | Cooperative directory lock on Unix. A writer takes an exclusive lock before recovery. Read-only handles can share an existing lock |
| `CURRENT` | Names the active MANIFEST. MANIFEST rotation publishes the replacement using a synced temporary file, rename, and directory sync |
| `MANIFEST-*` | First WAL-framed record is the 16-byte `DBHeader` containing `MMDB`, format version 5, level count, and prefix length (three little-endian `u32` fields after the magic). Subsequent `VersionEdit` records describe live tables and recovery metadata |
| `*.wal` | Batch records split into checksummed fragments within 32 KiB blocks. The batch header contains the starting sequence and entry count |
| `*.sst` | Immutable SST v2 tables with a 48-byte footer ending in `TABLE_MAGIC_V2` |

On store creation, MMDB syncs each new directory entry through its containing
directory before descending to the next path component. [Directory creation](../src/db/directory.rs)

Open replays eligible WALs in file-number order, starting at the MANIFEST log
number. A nonzero log number names a required WAL. If it is absent, both
open modes return `Corruption`. Only the highest **nonempty** eligible WAL may have an incomplete
tail followed by zero padding or EOF. A higher empty standby WAL does not
change that choice. Checksum errors and malformed batch contents fail
recovery. Writable recovery can spill to staged L0 files during replay and
at WAL boundaries. It installs the recovered files and fresh WAL number in a
single edit, syncs the MANIFEST, then removes obsolete files. Read-only
recovery retains the replayed entries in memory.

[WAL framing](../src/wal/) · [MANIFEST and CURRENT](../src/manifest/version_set.rs) · [Recovery tests](../tests/crash_recovery.rs)

## SST v2 and cache memory

- **Data and index.** Prefix-compressed data blocks use restart points. A partitioned point index maps separators to data-block handles, first keys, and optional block properties. Index and range-deletion leaves target 64 KiB. Linked, checksummed directory pages allow larger indexes. Each live table reader retains the directories and any Bloom filters.
- **Filters.** With `filter.format = 2`, a seed-zero XXH3-64 hash selects one aligned 64-byte line. Two mixed hash words provide eight 9-bit probe positions in that line. Newly built whole-key filters receive a level-dependent bits-per-key budget. `optimize_filters_for_hits` omits the whole-key filter on newly built bottommost SSTs. Configured prefix filters keep their base bits-per-key budget.
- **Cached blocks.** A private or shared `BlockCachePool` keys entries by member, file number, and block offset. Data and index blocks can carry validated restart metadata so a hit can reuse it. An unpinned allocation larger than one segment's share but within the configured capacity uses an unsegmented overflow cache. Each cache has its own weighted capacity limit. The configured value therefore does not cap their combined memory.
- **Pins and handles.** With L0 pinning and caching enabled, flush attempts to warm the first index leaf and pin the first data block. Range-deletion-only tables have no data block to pin. Pins are member-local and charged separately. Versions and iterators retain table readers. A caller holding a `PinnedSlice` can retain an entire SST block or memtable after eviction or close.

[SST builder and reader](../src/sst/) · [Bloom probes](../src/sst/filter.rs) · [Block and table caches](../src/cache/) · [Shared-cache tests](../tests/shared_cache.rs)

`BlockCachePool::usage` samples the evictable stores' weighted byte counts,
which can lag insertions and evictions while cache maintenance runs.
`DB::memory_usage` samples the active and immutable memtables from one
`SuperVersion` and reads the member's L0 pin-map payload length separately.
It takes no writer lock. Flush retires a generation from the reported
immutable set after durable installation, even if a reader still owns that
generation. [DB memory gauges](../src/db/memory.rs)

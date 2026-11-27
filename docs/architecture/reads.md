# Reads and snapshots

Point reads, grouped reads, and scans first acquire their storage sources
and then choose the sequence at which to interpret the entries they find.
[Caller-facing read APIs](../usage.md#read-pin-and-iterate) · [Architecture overview](../architecture.md)

## Pin the view, then choose the sequence

1. `ReadState` pins the current `SuperVersion` through `ArcSwap`. Its
   references keep the active and frozen memtables and the installed
   `Version` alive. Each live SST file in that version holds a `TableReader`.
2. The read checks that the handle is usable. A closed handle returns
   `DbClosed`. A fail-stopped open handle returns `Background`.
3. A latest read loads `min(committed_sequence, view.ingest_ceiling)` with
   `Acquire` ordering. The writer publishes the committed sequence with
   `Release` after inserting all entries. The ceiling protects an old view
   held across sorted ingestion. A snapshot read checks the originating
   database ID and uses the snapshot's registered sequence.

This order allows flush and compaction to replace the installed file set
while a read continues using its original sources. [Read path](../../src/db/read.rs) ·
[Ingest publication and race tests](../../src/db/ingest.rs)

## Point lookups

`get`, `get_pinned`, and `contains_key` use the same visibility rules.

1. Find the highest visible range-deletion sequence covering the key in the
   active and immutable memtables. Probe their point entries newest memtable
   first. A point deletion hides the key, as does a covering range deletion
   newer than the point. A covering memtable deletion with no newer point
   there also rules out a value in an SST.
2. Visit SST levels from L0 down. At each level consult the range-deletion
   readers whose extents cover the key, including readers whose last stored
   key is smaller than the lookup key. A covering range can extend beyond
   the file's last point or fragment start.
3. Probe candidate point files. L0 can have multiple overlapping candidates,
   searched newest first. A binary search in each deeper level selects at
   most one point candidate. A whole-key Bloom filter can rule out a miss.
   Otherwise the reader follows the resident directory to an index leaf and
   data block. Hits at L2 or deeper can feed read-compaction sampling.

For example, a range deletion `[b, d)` at sequence 12 hides an older `c` in
a deeper level for reads at sequence 12 or later. A snapshot at sequence 11
can still see that value, and the deletion does not cover `d`.
[Memtable and SST lookup](../../src/db/read.rs) · [Table reader](../../src/sst/table_reader/mod.rs)

### Grouped point reads

`get_many_pinned_with_options` resolves up to 32 input slots against one
view and sequence. A singleton uses the scalar pinned path. For a larger
group, the engine sorts slot positions, probes each distinct key once, and
restores input order. Duplicate hits get separate pins to the same backing
bytes. A miss yields `None` in its slot.

During the call, `GroupedPointBlocks` can retain an index leaf and a data
block for each of two recently probed SSTs, in addition to the returned
pins. On success, `stats.bytes_read` includes the key and value length for
every hit slot, including duplicates. An error drops partial results and
adds zero returned bytes. [Grouped-read tests](../../tests/grouped_reads.rs) ·
[Backing storage for returned values](../../src/pinned.rs)

## Range and prefix scans

A scan intersects the explicit bounds with `ReadOptions`. The lower bound
is inclusive, the upper exclusive. A prefix scan also enforces the full
requested prefix. If the prefix is at least as long as the file's configured
Bloom prefix, a negative probe skips that file's point entries. Overlapping
range deletions still participate in visibility.

`DBIterator` merges cursors over the memtables, separate L0 tables, and a
lazy `LevelIterator` for each nonempty deeper level. A `MergingIterator`
combines the ordered sources. For each user key the scan resolves versions
at the captured sequence, point deletions, and covering range deletions.
A tombstone from SST level `L` applies to point sources at level `L` or
deeper when its visible sequence is newer than the point.

A block-property collector sees keys for both point values and point
deletions. A filter can skip a block only if all collected keys fail its
predicate. The iterator applies `key_matches` and `skip_point` to the
resolved user key before yielding it. With `fill_cache = false`, scans still
use existing cached blocks. [Scan setup](../../src/db/read.rs) ·
[Iterator implementation](../../src/iterator/) · [Filter contract](../../src/options.rs)

`iter_with_batch` adds a `WriteBatchWithIndex` as the newest source.
Pending mutations occupy synthetic sequences above the captured read
sequence, preserving their order without exposing later committed writes.

The lending `step_next` and `step_prev` methods return a typed error on
the failing step, including lookahead. The owned `Iterator` exposes a
latched failure through `error()`. `BidiIterator::lazy` narrows the remaining
window on direction changes so the same row is consumed only once.
[Iterator steps](../../src/iterator/db_iter.rs) · [Bidirectional scans](../../src/iterator/bidi_iter.rs)

## Snapshot and pin lifetimes

| Object | What keeps it valid |
|---|---|
| `Snapshot` | Borrows the DB and registers a sequence under `DB::inner`. Compaction retains versions needed at that sequence until the registration ends. |
| `ArcSnapshot` | Clones share one `SnapshotLease`. The lease holds a weak reference to `ReadState`. The last clone releases the registration. After DB close, a new read through a surviving clone returns `DbClosed`. |
| `DBIterator` | Holds references to its scan sources. An already-created iterator can finish reading those sources after close. |
| `PinnedSlice` | A nonempty result owns a reference to its SST data block or memtable arena. Its bytes survive flush, eviction, compaction, and close. An empty value uses inline empty bytes. |

Snapshot lifetime controls which versions a compaction must retain. Pin
lifetime controls when the backing bytes can be freed.
[Snapshot handles](../../src/db/snapshot.rs) · [Cache ownership](sst-and-cache.md#cache-ownership)

# Writes, flush, and sorted ingestion

An acknowledged batch has a visibility sequence. Its durability depends on
the requested WAL mode and whether its memtable generation has reached a
durable SST. [Caller-facing durability guarantees](../usage.md#write-and-choose-durability) ·
[Architecture overview](../architecture.md)

## Admission and group commit

1. `DB::write_with_options` checks that the handle is writable and the batch
   encoding is valid. A valid empty batch returns immediately. A nonempty
   batch checks the immutable queue and the installed L0 file count.
2. With `no_slowdown`, a full queue or L0 at the slowdown threshold returns
   `Busy` before that batch receives a sequence or WAL record. Normal writes
   can wait for a flush slot, delay at the slowdown threshold, or drain L0
   at the stop threshold.
3. One queued writer becomes the leader. `WriteQueueState::drain_group`
   groups payloads up to `min(write_buffer_size / 2, 16 MiB)`, always taking
   the head request even when it exceeds that size. Followers wait for their
   own result. The leader rechecks immutable capacity for queued
   `no_slowdown` requests and the actual L0 count under `DB::inner`.
4. The leader checks metadata budgets and reserves one sequence per admitted
   operation, in request order. A rejected member can have successful
   neighbors. Each WAL-enabled batch appends one logical record using
   `WalWriter::add_record_parts` with a sequence/count header and the batch's
   encoded payload. `DisableWal` batches can leave gaps in the WAL sequences.
5. The group shares a WAL flush for asynchronous writes, or a data sync when
   required by an admitted `WalSync` write or an outstanding WAL fence.
   After this succeeds, the leader inserts the assigned operations into the
   active memtable in sequence order and publishes `committed_sequence` with
   `Release` ordering. Readers can now see the whole batch.

`DB::sync_wal()` captures the committed sequence. If the last confirmed WAL
sync already covers it, the call returns immediately. Otherwise a fence joins
the writer queue and can share a group's sync. It adds zero records and
consumes zero sequences, even when it is the only request. On Linux,
`wal_bytes_per_sync` controls an advisory writeback hint after an asynchronous
flush. Only a confirmed sync establishes WAL durability.
[Group commit and fence](../../src/db.rs) · [WAL writer](../../src/wal/writer.rs)

### Admission limits

- Point keys and both range endpoints are limited to 8 MiB each.
  `MAX_WRITE_ENTRY_SIZE` allows `64 MiB − 72 bytes` for key plus value.
  `MAX_RANGE_DELETE_SIZE` allows `32 MiB − 4,168 bytes` for both endpoints
  combined. These limits leave room for SST framing.
- SST files split between distinct user keys. `admit_same_key_runs` caps
  the metadata charge for one unsplittable key in the active memtable at
  32 MiB, including earlier versions of that key.
- One elementary range-deletion interval must fit its retained sequence
  numbers in a single leaf. `admit_range_stripes` rejects a batch containing
  a range deletion when the number of distinct live snapshot sequences has
  reached `MAX_RANGE_STRIPES`. Registering another snapshot at an existing
  sequence adds no distinct sequence. [Batch limits](../../src/types.rs) ·
  [SST size invariants](../../src/sst/table_builder.rs)

## Freeze and FIFO flush

The active memtable stores point entries in arena slabs and indexes range
deletions separately. Its estimated size includes reserved slab bytes, the
range list and endpoints, and the range index. When the estimate reaches
half the configured write buffer, the leader can ask `mmdb-flush` to prepare
a standby WAL. At the freeze threshold, or on an explicit flush of a
nonempty memtable, MMDB rotates the generation. [Memtable accounting](../../src/memtable/mod.rs)

| Stage | State transition |
|---|---|
| Freeze | The write leader takes the old WAL writer, seals it with `sync_data()` outside `DB::inner`, and uses a standby or newly created WAL. It swaps in a new active memtable, puts the frozen one at the front of the immutable list, and publishes the updated `SuperVersion`. |
| Build L0 | One flush worker processes queued generations oldest first. It finishes and syncs the new SST files, syncs their parent directory, and prepares validated table readers and the configured L0 pins. |
| Install | Under `DB::inner` the worker installs a MANIFEST edit containing the new L0 files and next WAL number. The new files remain protected by a compaction claim while the worker confirms MANIFEST durability outside the mutex. |
| Retire | After confirmation, the worker removes the oldest immutable memtable, refreshes the read view, and releases the claim. It can now retire the old WAL, signal compaction, and advance the durable generation counter. |

`DB::flush()` captures a target generation while serializing with writers
and waits until the durable counter covers it. Later writers can fill a new
active memtable while this wait is in progress. A failed MANIFEST sync
preserves the old WAL and immutable generation and wakes waiters with a
failure. [Flush worker and generation tests](../../src/db/flush.rs)

### Failure at the acknowledgement boundary

If a later batch in a group fails to append its WAL record, an earlier batch
whose required WAL operation succeeds and whose mutations are published gets
`Ok(())`. The failing batch and the unapplied suffix get an error. A failed
group WAL flush or sync stops publication. An automatic freeze failure
happens **after** publication, so it preserves an already committed batch's
acknowledgement. These failures latch an error for subsequent operations on
the open handle. [Write group](../../src/db.rs) · [Freeze failure tests](../../src/db/flush.rs)

## Sorted ingestion

`DB::ingest_sorted` takes write leadership and streams strictly ascending
key/value pairs directly into bottom-level SSTs. The whole span from its
first to its last key must be disjoint from active and frozen entries,
memtable tombstones, live SST file-key bounds, and range-deletion extents.
The installer rechecks that span under `DB::inner`. Unordered, duplicate,
oversized, or overlapping input returns `InvalidArgument`.

All rows share one fresh sequence. The installer applies one MANIFEST edit,
protects the installed files from compaction, and confirms MANIFEST durability.
It then caps the outgoing view at `ingest_seq - 1`, publishes a new
`SuperVersion`, and advances `committed_sequence`. A reader holding the
outgoing view cannot mix a later sequence with sources that omit the ingested
span. Earlier snapshots still see their registered state.

Before installation, `PendingSsts` cleans up abandoned outputs. If the edit
was installed and its durability confirmation fails, the referenced files
are retained and the handle enters fail-stop. [Ingest implementation and race tests](../../src/db/ingest.rs)

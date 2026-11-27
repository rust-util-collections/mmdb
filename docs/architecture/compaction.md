# Compaction and garbage collection

Compaction chooses SST files, merges the versions that must survive, and
installs the replacement set in the MANIFEST. The old files stay available
while the replacement edit's durability is being confirmed.
[Caller-facing maintenance operations](../usage.md#compaction-and-properties) ·
[Architecture overview](../architecture.md)

## Choose and claim files

| Trigger | Work selected |
|---|---|
| Automatic L0 compaction or a foreground L0 write stall | Pick the entire current L0 set for the L0-to-L1 job and include overlapping L1 files. L0 files can overlap each other. This move runs as one range. |
| Deeper-level budget or read-sampling hint | Pick files from a source level and include overlapping files from the target level. The automatic size-based picker uses level scores and target overlap. |
| `DB::compact()` | Wait for previously queued flushes and perform a full compaction of the installed SST levels. |
| `DB::compact_range(begin, end)` | For a nonempty interval, flush the captured generation and select files that overlap `[begin, end)`. Unbounded ends are allowed. File selection may extend beyond the requested keys. |

The picker considers file-key bounds and resident range-deletion partition
extents. This catches a tombstone that reaches beyond its file's last
stored key. It claims the input file numbers under `DB::inner` before
starting the merge, keeping competing jobs from claiming those inputs.
[Pickers and extent checks](../../src/compaction/leveled.rs) · [Maintenance entry points](../../src/db.rs)

## Build the replacement

A single source file can move to another level with a metadata-only edit if
the target has no overlap and the effective filter is a no-op. This
**trivial move** preserves the file number and rewrites zero SST bytes.

For a rewrite, the job releases `DB::inner` before merging data or building
SSTs. An eligible job below L0 can split at target-level file boundaries, up
to `max_subcompactions`. Subranges are half-open and the boundary key belongs
to the range on its right. `rate_limiter_bytes_per_sec` paces compaction output.
[Merge and split implementation](../../src/compaction/leveled.rs) · [Rate limiter](../../src/rate_limiter.rs)

### Versions, deletions, and output boundaries

- **Retaining versions.** The merge keeps the newest state and the older
  versions needed by registered snapshots. It includes point deletions and
  the visible sequence stripes of covering range tombstones when resolving
  which values survive.
- **Determining bottommost coverage.** `is_bottommost_level` checks unselected
  files from the source (or rewritten) level through the bottom, including
  their range-deletion extents. When the job is bottommost for its range,
  old deletion markers can be discarded and surviving values strictly older
  than `oldest_snapshot_seq` can have their sequence set to zero.
  `RangeFragment::keep_from` drops range stripes strictly below the same cutoff.
- **Splitting files.** A cut waits for a change of user key so versions at
  one key stay together. At a cut `k`, `TableBuilder::clip_at` splits a
  crossing tombstone into `[begin, k)` and `[k, end)` with the retained
  sequences on both sides. File bounds use the range fragments actually
  emitted after splitting and pruning. [SST builder and boundary tests](../../src/sst/table_builder.rs)

### Filters and lazy deletion

With no snapshots in the job's captured set, a `CompactionFilter` can
inspect the newest value of an eligible key. `Remove` and `ChangeValue` take
effect only if bottommost coverage is established for the job or verified for
that key. If a snapshot is active at install and the filter changed output,
the install discards that output.

`lazy_delete` and `lazy_delete_batch` register keys in memory for this filter.
A new value written while its key is still registered can also be removed
by a later rewrite. Pending registrations are lost at restart. A positive
`lazy_delete_compaction_threshold` schedules a sweep when registrations reach
the threshold. The scheduler can retry after snapshots drop. Flush and
maintenance passes prune registrations once the keys are gone.
[Filter contract](../../src/options.rs) · [Lazy-delete tests](../../tests/lazy_delete.rs)

## Install and retire files

1. Finish and sync the output SSTs and their directory entries. Prepare the
   table readers before reacquiring `DB::inner`.
2. Under the mutex, verify that inputs are still live, output extents still
   fit, and the snapshot checks still pass. Discarded builds clean up their
   new files. A discarded trivial move leaves its existing SST in place.
3. Apply the MANIFEST edit, evict the replaced cache entries, and refresh
   `SuperVersion`. Confirm MANIFEST durability outside `DB::inner` before
   unlinking superseded inputs. In-flight reads keep their original readers.
4. A fatal background or write-stall compaction publishes its failure before
   releasing its input claims. A retryable manual compaction failure releases
   the claims and signals pending work. [Compaction installation](../../src/compaction/leveled.rs) ·
   [Worker and write-stall paths](../../src/db.rs)

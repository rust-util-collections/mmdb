# Compaction Review Patterns

**Files:** `leveled.rs`, `mod.rs`, `src/db.rs` (`LevelRewrite`, `drain_l0`, `DeadKeySweepScheduler`).

For file selection, garbage collection, and publication order, see the
[compaction chapter](../../architecture/compaction.md).

## Invariants

**C1 Output disjointness & user-key cuts (L1+)** — outputs are strictly monotonic and non-overlapping. Output file splits (`pending_cut`) defer until `user_key != current_file_user_key` so a cut never lands mid-user-key or between `OutputEntry::Range(begin=K)` and `OutputEntry::Point(K@seq)`.

**C2 Tombstone & snapshot-stripe retention** — keep point `Deletion`s and range tombstones at non-bottommost levels or when covered by an active snapshot. At `is_bottommost`, `RangeFragment::keep_from(oldest_snapshot_seq)` drops stripes strictly `< oldest_snapshot_seq`, matching the strict inequality `entry_seq < oldest_snapshot_seq` used for `Value` sequence zeroing (so surviving stripes always satisfy `t_seq > 0` and never invert `t_seq > entry_seq`). `is_bottommost_level` checks union extents across `first_checked_level..num_levels`.

**C2a Range fragments & split clipping** — `collect_range_fragments` reads packed SST v2 intervals once (`fill_cache = false`), sharing `Arc<[u8]>` endpoints and `Arc<[u64]>` ascending stripe arrays across sub-jobs. Sub-jobs clip fragments to `[lo, hi)` (`lo < hi`) and seek points at `InternalKey::new(lo, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion)` so boundary keys and `MAX_SEQUENCE_NUMBER` tombstones at `lo` belong exclusively to the right sub-job. Across SST file cuts at `K_split`, `TableBuilder::clip_at(K_split)` tiles `[begin, K_split)` and `[K_split, end)` with identical retained stripes and zero gaps; `projected_meta_size()` subtracts `carried_projection` so carried fragments do not force 1-key file splits.

**C3 Zero-I/O extent closure** — `overlapping_files_for_inputs` and `add_file_extents` close source and target files over resident `TableReader::range_partition_extents()` (no disk I/O under `DB::inner`) and return target files in level order so `compute_split_points` stays strictly increasing.

**C4 Durable install & `PendingSsts`** — `execute_compaction_io` finishes and syncs the output SSTs, syncs their directory, then calls `PendingSsts::keep()` to hand the files to the install path. `install_compaction` removes newly created outputs if it discards or rejects the edit. On successful install it evicts replaced inputs from caches. The caller confirms MANIFEST durability before `run_post_compaction_cleanup` unlinks the superseded files.

**C5 Stale inputs, `l0_install_epoch`, & snapshot races** — under `DB::inner`, verify all inputs remain live, outputs do not overlap unexpected target files (using `output_tombstones` captured in I/O phase), and `!(output.filter_mutated && live_snapshots_present)`. Full-level `LevelRewrite` claims inputs via `CompactionClaim`; if an L0 in-place rewrite (`0 -> 0`) sees `l0_install_epoch` change, discard created outputs (never live trivial-move inputs) and switch to `L0 -> L1`.

**C6 Filter & typed errors** — A filter may examine the newest point value when the job captured no active snapshots. Honor `Remove` or `ChangeValue` only after whole-range or per-key bottommost verification. `SstBuilds` blocks re-entrant DB mutations/close from filter/collector callbacks (`ErrorKind::InvalidArgument`). Preserve typed `MergingIterator::error()` (`ErrorKind::Corruption` / `ErrorKind::Io` + `source()`) on synchronous `compact()` / `compact_range()` rather than reclassifying as `ErrorKind::Background` when the DB is not fail-stopped.

The automatic L0→L1 picker claims the entire current L0 set and runs one range. A
deeper-level parallel test exercises a different path. With `bench-metrics`,
the number of jobs with multiple target files, ranges started, peak
simultaneous ranges on one DB, and jobs that joined multiple ranges measure
different events. A trivial move adds zero rewritten SST bytes. The
[two-range experiment](../../benchmarks/l0-profile/) records the tested
prototype and the outstanding foreground read-latency guardrail.

## Checklist

- [ ] Outputs strictly monotonic, non-overlapping, cut only at user-key boundaries
- [ ] `RangeFragment` stripes clipped cleanly across sub-compaction `[lo, hi)` and SST `clip_at` splits
- [ ] Bottommost `keep_from(oldest_snapshot_seq)` matches strict `< oldest_snapshot_seq` point seq zeroing
- [ ] `overlapping_files_for_inputs` and `is_bottommost_level` include resident `range_partition_extents()` and preserve target level order
- [ ] `DirSynced` + `confirm_manifest_durable` complete before unlinking inputs; `PendingSsts` cleans failed outputs
- [ ] Discarded installs (`stale`, `snapshot_raced`, `l0_install_epoch`) never delete live trivial-move input SSTs
- [ ] `CompactionFilter` runs once per logical key; `LazyDeleteFilter::is_noop` checks both user filter and `dead_keys`
- [ ] `MergingIterator` `Corruption`/`Io` errors keep their `ErrorKind` and `source()`

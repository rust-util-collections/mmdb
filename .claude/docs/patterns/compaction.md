# Compaction Review Patterns

**Files:** `leveled.rs`, `mod.rs`.

**Arch:** L0 overlap → L1+ sorted disjoint; trigger L0 count ≥ `l0_compaction_trigger`;
sub-compaction on L(n+1) bounds; trivial move = single file, no target overlap;
streaming O(block) memory.

## Invariants

**C1 Output disjoint (L1+)** — non-overlapping ranges or binary search misses keys.
Split points use exclusive boundaries.

**C2 Tombstone retention** — keep at non-bottommost. Bottommost drop only if no
live snapshot covers. `is_bottommost` must consider **all** lower levels, not only +1.

**C2a Range tomb across sub-jobs** — collect full `all_range_del_entries` /
`all_raw_tombstones` once; share by ref; each sub-job builds local
`RangeTombstoneTracker` before merge. Bounds must not truncate equality coverage.

**C3 Seq zero** — bottommost only, and only keys without snapshot dependency.

**C4 Sub boundary** — boundary key to exactly one sub-job (exclusive start/end).

**C5 Delete inputs after durable install** — inputs unlinked only after outputs
written and replacing MANIFEST edit durably synced. `install_compaction` may
return `PostCompactionCleanup` after `log_and_apply`; caller syncs MANIFEST
before `run_post_compaction_cleanup`.

**C5a Stale install** — under lock, inputs still in current Version; no output
overlap with a target file the edit neither deletes nor produces. Stale outputs
deleted off MANIFEST. Prec check under DB lock must not read SSTs; tombstone
extents come from `CompactionOutput::output_tombstones` captured in I/O phase.

**C6 Filter** — when bottommost + no snapshots: each eligible surviving Value once
after visibility; Keep/Remove/ChangeValue on that entry. Same eligibility for
normal and forced.

## Bug patterns

**Zombies (tech 4.1):** tombdrop non-bottommost; key still deeper.
**Stall:** compact holds `db_mutex` waiting on work that needs it — release before I/O.
**Rate limiter:** burst + no indefinite block.
**Bottommost range:** union of **all** input extents (`add_file_extents`), not per-file only.

## Checklist

- [ ] Outputs monotonic non-overlapping
- [ ] Tombstones kept non-bottommost; range dels reach all sub-jobs
- [ ] Seq zero only bottommost w/o snapshot dep
- [ ] Sub boundaries no dup/skip
- [ ] MANIFEST durable before input unlink
- [ ] Filter once per logical KV when eligible
- [ ] Trivial/no-op cannot skip effective filter
- [ ] Partial fail leaves consistent DB
- [ ] Rate limiter no unbounded stall
- [ ] Stale-input check before VersionEdit apply
- [ ] `is_bottommost` uses aggregate extents

# Concurrency Review Patterns

## Model

```
Writers → group-commit leader → admit_range_stripes + admit_same_key_runs → vectored WAL + MemTable → Release committed_sequence
Freeze → leader seals old WAL + uses standby or creates WAL outside inner → push_front immutable_memtables → FlushQueue
Flush worker (mmdb-flush) → FIFO SST build + DirSynced + log_and_apply_synced + confirm_manifest_durable → pop_back + unlink old WAL
Readers → pin ArcSwap<SuperVersion> BEFORE Acquire committed_sequence → MemTable and SST v2 lookups
Compaction / LevelRewrite → claim inputs under inner → unlocked SST I/O (SstBuilds guard) → revalidate + install under inner
Close → MaintenanceState drain → write_queue drain → final flush → worker join → cache/LOCK teardown
```

## Lock & Synchronization Hierarchy

Derive edges from live code (`src/db.rs`, `src/db/flush.rs`, `src/db/read.rs`, `src/db/snapshot.rs`):

`lock_file` → `maintenance` / `write_queue` → `inner` → `compacting_files` → `{snapshot_list, manifest_writer, dir_sync, flush_queue.progress, table_cache, block_cache, dead_keys, read_compaction_hints}` → `{compaction_notify, bg_error, builds}`.

`parking_lot` locks are non-reentrant. Group commit holds `inner` while
appending and flushing or syncing the WAL and applying the group to memory.
WAL sealing and fallback creation run outside `inner`. A stale standby file
can be unlinked under `inner`. Flush and compaction build SSTs and confirm
MANIFEST durability outside `inner`. Installing the edit, including a
possible MANIFEST rotation, runs under `inner`.

## Invariants

**CC1 View-before-sequence read barrier** — `ReadState::point_lookup` and `scan` load `SuperVersion` *before* loading `committed_sequence` (`Ordering::Acquire`), while writers store `committed_sequence` (`Ordering::Release`) only after all MemTable inserts finish. Prevents compaction GC from dropping versions below a newly observed sequence before the reader pins `SuperVersion`.

**CC2 Group commit & post-commit boundary** — cap batch groups by `MAX_WRITE_BATCH_GROUP_BYTES` (always admitting entry 0), reserve contiguous sequences, append vectored WAL records in group order (`DisableWal` skipped; a fence with an outstanding target requires `sync_data()` even with 0 ops), `sync_data()` / `flush()`, insert into `active_memtable` in the same order, publish `committed_sequence` (`Release`), and advance `synced_wal_seq` on sync. Once `committed_sequence` is published, any post-commit failure (`append_error` on later batches or `submit_active()` auto-freeze error) must still return `Ok(())` to already-committed callers while latching fail-stop for subsequent calls.

**CC3 Unlocked WAL rotation & FIFO `FlushQueue`** — only the active `write_queue` leader (or `close()` after draining writers and `maintenance`) enters `freeze_memtable`, so taking `wal_writer = None` across unlocked seal/create cannot race another writer; on seal/create error, restore `old_wal` and fail-stop. `immutable_memtables` pushes newest-first (`push_front`), `SuperVersion` reads front-to-back, and `mmdb-flush` pops oldest (`pop_back`) only *after* `confirm_manifest_durable` succeeds. A valid nonempty `no_slowdown` batch receives `ErrorKind::Busy` from a full
immutable queue or when L0 reaches its slowdown threshold (including stop
pressure). Reject before assigning that request a sequence or appending its
WAL record. A group can still publish an admitted neighbor. Waiters release
their stack-backed requests only after leader completion.

**CC4 Unlocked compaction, claims, & `l0_install_epoch`** — release `CompactionClaim` *before* signaling `compaction_notify` so woken workers never see newly flushed L0 SSTs still in `in_flight` and go back to sleep. Full-level `LevelRewrite` records `l0_install_epoch` (incremented on every freeze and flush completion); if `0 -> 0` races a freeze/flush, discard created outputs and switch to `L0 -> L1`. `SstBuilds` tracks threads inside flush/compaction SST builds so filter/collector callbacks calling mutating DB methods or `close()` fail fast with `ErrorKind::InvalidArgument` instead of deadlocking.

**CC5 Fail-stop publication order** — publish `*bg_error.lock() = Some(msg)` and `has_bg_error.store(true, Ordering::Release)` *before* waking `FlushQueue` waiters (`flush_queue.fail(...)`), and serialize directory/MANIFEST `fsync` failures with `VersionSet::poisoned` (`Release`) so woken or queued threads immediately fail `ReadState::check_usable()` (`ErrorKind::Background`). `check_usable()` checks `closed` *before* `fail_stop_error()` on normal reads, while `DB::close()` surfaces any fail-stop error to callers.

**CC6 `SnapshotLease` & `MaintenanceState` lifecycle** — A borrowed `Snapshot` holds `&DB`. Clones of an `ArcSnapshot` share one
`SnapshotLease` with a `Weak<ReadState>`, allowing the DB to close and
release its readers and directory lock. Both snapshot kinds are DB-bound.
A cross-DB read returns `InvalidArgument` while the originating handle is
open. Check dead-key sweep retries after snapshot release. `DB::close()`
drains maintenance operations (flush, compact, compact range, and sorted
ingestion), then waits for write leadership before stopping workers.

## Checklist

- [ ] Acyclic lock order, including group WAL I/O under `inner` and unlocked WAL seal and SST builds
- [ ] `SuperVersion` pinned before `committed_sequence` (`Acquire`); `committed_sequence` (`Release`) stored after MemTable inserts
- [ ] Post-commit rotation/queue errors never return `Err` for already-committed writes
- [ ] `CompactionClaim` dropped before notifying `compaction_notify`; condvar waits loop-recheck predicates
- [ ] `bg_error` + `has_bg_error` (`Release`) stored before `flush_queue.fail(...)`
- [ ] `SstBuilds` guards all SST-building paths against callback re-entrancy
- [ ] `MaintenanceState`, `write_queue`, `mmdb-flush`, and `mmdb-compaction` drain cleanly in `close()` and `Drop`

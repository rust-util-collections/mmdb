# Concurrency Review Patterns

## Model

```
Writers → admit_range_stripes + admit_same_key_runs → group-commit leader → vectored WAL + MemTable → Release committed_sequence
Freeze → leader seals old WAL + creates new WAL outside inner → push_front immutable_memtables → FlushQueue
Flush worker (mmdb-flush) → FIFO SST build + DirSynced + log_and_apply_synced + confirm_manifest_durable → pop_back + unlink old WAL
Readers → pin ArcSwap<SuperVersion> BEFORE Acquire committed_sequence → lock-free MemTable + SST v2
Compaction / LevelRewrite → claim inputs under inner → unlocked SST I/O (SstBuilds guard) → revalidate + install under inner
Close → MaintenanceState drain → write_queue drain → final flush → worker join → cache/LOCK teardown
```

## Lock & Synchronization Hierarchy

Derive edges from live code (`src/db.rs`, `src/db/flush.rs`, `src/db/read.rs`, `src/db/snapshot.rs`):

`lock_file` → `maintenance` / `write_queue` → `inner` → `compacting_files` → `{snapshot_list, manifest_writer, dir_sync, flush_queue.progress, table_cache, block_cache, dead_keys, read_compaction_hints}` → `{compaction_notify, bg_error, builds}`.

`parking_lot` locks are **non-reentrant**. Never hold `inner` across SST builds, SST/WAL/MANIFEST `fsync`, parent-dir `fsync`, WAL rotation (`old_wal.sync_data()` + `WalWriter::new`), or file unlinks (only periodic 1,000-edit MANIFEST snapshot rotation syncs inside `VersionSet`).

## Invariants

**CC1 View-before-sequence read barrier** — `ReadState::point_lookup` and `scan` load `SuperVersion` *before* loading `committed_sequence` (`Ordering::Acquire`), while writers store `committed_sequence` (`Ordering::Release`) only after all MemTable inserts finish. Prevents compaction GC from dropping versions below a newly observed sequence before the reader pins `SuperVersion`.

**CC2 Group commit & post-commit boundary** — cap batch groups by `MAX_WRITE_BATCH_GROUP_BYTES` (always admitting entry 0), reserve contiguous sequences, append vectored WAL records in group order (`DisableWal` skipped; `WalFence` forces `sync_data()` even with 0 ops), `sync_data()` / `flush()`, insert into `active_memtable` in the same order, publish `committed_sequence` (`Release`), and update `synced_wal_seq`. Once `committed_sequence` is published, any post-commit failure (`append_error` on later batches or `submit_active()` auto-freeze error) must still return `Ok(())` to already-committed callers while latching fail-stop for subsequent calls.

**CC3 Unlocked WAL rotation & FIFO `FlushQueue`** — only the active `write_queue` leader (or `close()` after draining writers and `maintenance`) enters `freeze_memtable`, so taking `wal_writer = None` across unlocked seal/create cannot race another writer; on seal/create error, restore `old_wal` and fail-stop. `immutable_memtables` pushes newest-first (`push_front`), `SuperVersion` reads front-to-back, and `mmdb-flush` pops oldest (`pop_back`) only *after* `confirm_manifest_durable` succeeds. `FlushQueue::check_capacity(true)` returns `ErrorKind::Busy` immediately when `no_slowdown` is set and the queue is full.

**CC4 Unlocked compaction, claims, & `l0_install_epoch`** — release `CompactionClaim` *before* signaling `compaction_notify` so woken workers never see newly flushed L0 SSTs still in `in_flight` and go back to sleep. Full-level `LevelRewrite` records `l0_install_epoch` (incremented on every freeze and flush completion); if `0 -> 0` races a freeze/flush, discard created outputs and switch to `L0 -> L1`. `SstBuilds` tracks threads inside flush/compaction SST builds so filter/collector callbacks calling mutating DB methods or `close()` fail fast with `ErrorKind::InvalidArgument` instead of deadlocking.

**CC5 Fail-stop publication order** — publish `*bg_error.lock() = Some(msg)` and `has_bg_error.store(true, Ordering::Release)` *before* waking `FlushQueue` waiters (`flush_queue.fail(...)`), and serialize directory/MANIFEST `fsync` failures with `VersionSet::poisoned` (`Release`) so woken or queued threads immediately fail `ReadState::check_usable()` (`ErrorKind::Background`). `check_usable()` checks `closed` *before* `fail_stop_error()` on normal reads, while `DB::close()` surfaces any fail-stop error to callers.

**CC6 `SnapshotLease` & `MaintenanceState` lifecycle** — `Snapshot` and `ArcSnapshot` validate process-unique `db_id` (`InvalidArgument` cross-DB) and hold `Weak<ReadState>` (returning `DbClosed` after `close()`/`Drop` without retaining `TableCache`, `Version`, or `LOCK`). Releasing the last snapshot transitions `DeadKeySweepScheduler` (`Running -> RunningPending`) without losing retries across multi-worker compaction pools. `DB::close()` drains `MaintenanceState` (`flush`, `compact`, `compact_range`) before draining `write_queue` and stopping workers.

## Checklist

- [ ] Acyclic lock order; zero `inner` hold across SST/WAL/MANIFEST/dir sync or unlink I/O
- [ ] `SuperVersion` pinned before `committed_sequence` (`Acquire`); `committed_sequence` (`Release`) stored after MemTable inserts
- [ ] Post-commit rotation/queue errors never return `Err` for already-committed writes
- [ ] `CompactionClaim` dropped before notifying `compaction_notify`; condvar waits loop-recheck predicates
- [ ] `bg_error` + `has_bg_error` (`Release`) stored before `flush_queue.fail(...)`
- [ ] `SstBuilds` guards all SST-building paths against callback re-entrancy
- [ ] `MaintenanceState`, `write_queue`, `mmdb-flush`, and `mmdb-compaction` drain cleanly in `close()` and `Drop`

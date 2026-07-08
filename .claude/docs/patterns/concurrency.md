# Concurrency Model Review Patterns

## Architecture Overview

MMDB uses a single-writer / multi-reader concurrency model:

```
Writers:  [User Thread] → group commit leader → WAL + MemTable (serialized)
Readers:  [User Thread] → ArcSwap<SuperVersion> load (lock-free) → MemTable + SST
Background: [Compaction Thread(s)] → read SSTs, write new SSTs, update VersionSet
Flush:    [Write-path auto-flush, explicit flush()/compact_range(), close()] → freeze MemTable → write SST → update VersionSet
```

## Key Synchronization Primitives

| Primitive | Location | Protects |
|-----------|----------|----------|
| `Mutex` (parking_lot) | db.rs | Write path serialization, VersionSet updates |
| `ArcSwap<SuperVersion>` | db.rs | Lock-free read path |
| `Arc<MemTable>` | db.rs | MemTable lifetime across readers |
| `Atomic*` | db.rs, skiplist | Counters, flags, skiplist node pointers |
| `write_cv` (parking_lot Condvar) | db.rs | Group commit write queue — writers wait for leadership/notification |
| `compaction_notify` (std Condvar) | db.rs | Background compaction thread wake-up — work available notification |
| `std::thread::scope` | compaction | Sub-compaction parallelism |

> **Note**: L0 backpressure does NOT use a condvar. There are exactly two condvar
> wait sites in db.rs: `write_cv` and `compaction_notify`.

## Critical Invariants

### INV-CC1: Lock Ordering
If multiple locks must be held simultaneously, they must always be acquired in the same order. Known ordering:
1. `db_mutex` (write path)
2. `version_set` lock (if separate from db_mutex)
3. No other locks should be held while holding these

**Check**: Verify no path acquires locks in reverse order. Verify no lock is held across a blocking call (fsync, condvar wait) that could cause priority inversion.

### INV-CC2: ArcSwap Publish Protocol
When publishing a new SuperVersion:
1. First: ensure all data the SuperVersion references is fully initialized
2. Then: `arc_swap.store(new_sv)` with appropriate ordering
3. Old SuperVersion's data must remain valid until all readers holding the old `Guard` drop it.

**Check**: Verify no use of `arc_swap.store()` before the referenced MemTable/SST data is fully committed.

### INV-CC3: Group Commit Atomicity
The group commit leader must:
1. Collect all pending write batches
2. Assign sequence numbers contiguously
3. Write ALL to WAL
4. Fsync
5. Insert ALL into MemTable
6. Notify all waiters

Steps 2-5 must happen atomically (under the write lock). Step 6 may happen after releasing the lock.
**Check**: Verify the write lock is held from step 2 through step 5.

### INV-CC4: Background Thread Safety
Compaction threads must not hold `db_mutex` while performing I/O (reading/writing SST files). The 3-phase lock-release pattern:
1. Acquire mutex → pick compaction inputs → release mutex
2. Perform compaction/flush I/O (no mutex)
3. Acquire mutex → apply VersionEdit → release mutex

Flush never runs on the background compaction threads (they only compact SSTs; they never freeze/flush a memtable). The real flush paths are:
- **Write-path auto-flush** (`write_batch_group`): freezes under `db_mutex`, drops the lock during the SST write, re-acquires to install
- **Explicit `flush()` / `compact_range()`**: same pattern — lock released during the SST write
- **`close()`** via `freeze_and_flush`: holds the lock throughout (shutdown path, not performance-critical)

Synchronous compaction entry points follow the same 3-phase pattern: `drain_l0` (used by `flush()`'s post-flush trigger AND inline from the write path when L0 reaches `l0_stop_trigger` in `maybe_throttle_writes`) loops short-lock pick → unlocked merge I/O → short-lock install → unlocked manifest sync. `force_compact_all` (only reachable from the explicit `compact()` / `compact_range(None, None)` admin APIs) first runs `drain_l0`, then force-merges each level via `force_merge_level`, which holds `db_mutex` for that level's merge I/O — acceptable for the administrative path, but it must never become reachable from the ordinary write path.

Error policy per caller of `drain_l0` (v4.1.0):
- **`flush()`'s post-flush trigger**: a drain failure is non-fatal *unless* the engine is in a fail-stop state (background error set, or MANIFEST writer poisoned). A pre-persist rejection (e.g. `log_and_apply` refusing an edit) leaves nothing applied and nothing on disk, so `flush()` logs a warning and returns `Ok` — the memtable durability the caller asked for has already been achieved, and the next tick / background thread retries from a fresh pick.
- **`maybe_throttle_writes` (stop trigger)**: any failure fail-stops (sets background error) — last line of defense against unbounded L0 growth.
- **Inside `drain_l0` itself**: a manifest *sync* failure sets the background error before propagating (the edit is applied in memory but durability is unconfirmed — same policy as the background thread).

**Check**: Verify I/O operations happen between mutex release and re-acquire. For auto-flush in the write path, verify the lock is dropped before SST write and re-acquired after.

### INV-CC5: Write Stall Behavior (L0 Backpressure)
When L0 file count (cached in an atomic, no lock) exceeds a threshold, the write path applies backpressure — without any condvar:
- Slowdown (`l0_slowdown_trigger`): progressive `thread::sleep` delay, longer as L0 count grows
- Stop (`l0_stop_trigger`): the writer runs **inline** `drain_l0()` synchronously (short-lock pick/install pattern — the merge I/O itself runs without `db_mutex`) to reduce L0 count; if that compaction fails, the DB fail-stops (background error is set) rather than admitting the write

**Check**: Verify the atomic L0 counter is refreshed after every install that changes L0 (flush install, compaction install, inline compaction). Verify the stop path fail-stops on compaction error instead of silently admitting writes.

### INV-CC6: Snapshot Lifetime
A snapshot pins a sequence number. All data visible at that sequence must remain accessible until the snapshot is dropped. This means:
- Compaction must not drop entries visible to any live snapshot
- SST files referenced by any live snapshot's SuperVersion must not be deleted

**Check**: Verify snapshot list is consulted during compaction AND file deletion.

## Common Bug Patterns

### Deadlock via Lock Inversion
Thread A holds `db_mutex`, then tries to acquire `version_lock`.
Thread B holds `version_lock`, then tries to acquire `db_mutex`.
**Check**: Map all lock acquisition paths and verify no cycles.

### Lost Wakeup
A thread waits on a condvar under lock, but the notifier calls `notify_all()` before the waiter acquires the lock and enters `wait()`. The notification is lost, and the waiter blocks forever.

**Two condvar wait patterns in db.rs** — verify each is in a loop re-checking its condition:

1. **`write_cv`** (group commit queue): `while !req.done && wq.leader_active { write_cv.wait(&mut wq); }` — writers wait until their batch is processed or they should become leader
2. **`compaction_notify`** (background compaction wake): `while !*has_work && !shutdown { has_work = cvar.wait(has_work).unwrap(); }` — compaction threads wait for work, use `std::sync::Condvar` paired with `std::sync::Mutex<bool>`
**Check**: Each wait site uses `while condition { condvar.wait(...) }` loop (not `if`). Each notify happens after the condition is changed under the lock the waiter holds in the wait loop.

### ArcSwap Load + Modify Race
Two threads both load the current SuperVersion, modify it, and try to store back. One modification is lost.
**Check**: Verify SuperVersion updates go through a serialized path (under db_mutex), not via compare-and-swap on ArcSwap.

### Sub-Compaction Data Race
Two sub-compaction threads write to the same shared mutable state.
**Check**: Verify `std::thread::scope` closures only capture immutable references to shared state, and each thread has its own mutable output buffer.

## Review Checklist
- [ ] Lock ordering is consistent across all code paths
- [ ] ArcSwap publish happens AFTER referenced data is fully initialized
- [ ] Group commit holds write lock from seq assignment through MemTable insert
- [ ] Background threads release db_mutex during I/O
- [ ] Condvar wait is in a loop with condition re-check
- [ ] Snapshots consulted during compaction tombstone/sequence decisions
- [ ] Sub-compaction threads have no shared mutable state
- [ ] No blocking I/O while holding any lock
- [ ] Atomic operations use correct memory ordering (Release/Acquire at minimum)

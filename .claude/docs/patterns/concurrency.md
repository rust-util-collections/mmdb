# Concurrency Model Review Patterns

## Architecture Overview

MMDB uses a single-writer / multi-reader concurrency model:

```
Writers:  [User Thread] → group commit leader → WAL + MemTable (serialized)
Readers:  [User Thread] → ArcSwap<SuperVersion> load (lock-free) → MemTable + SST
Background: [Compaction Thread(s)] → read SSTs, write new SSTs, update VersionSet
Flush:    [Background Thread] → freeze MemTable → write SST → update VersionSet
```

## Key Synchronization Primitives

| Primitive | Location | Protects |
|-----------|----------|----------|
| `Mutex` (parking_lot) | db.rs | Write path serialization, VersionSet updates |
| `ArcSwap<SuperVersion>` | db.rs | Lock-free read path |
| `Arc<MemTable>` | db.rs | MemTable lifetime across readers |
| `Atomic*` | db.rs, skiplist | Counters, flags, skiplist node pointers |
| `Condvar` | db.rs | Write stall (L0 backpressure) |
| `std::thread::scope` | compaction | Sub-compaction parallelism |

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
Compaction and flush threads must not hold `db_mutex` while performing I/O (reading/writing SST files). They should:
1. Acquire mutex → pick compaction inputs → release mutex
2. Perform compaction I/O (no mutex)
3. Acquire mutex → apply VersionEdit → release mutex

**Check**: Verify I/O operations happen between mutex release and re-acquire.

### INV-CC5: Write Stall Signaling
When L0 file count exceeds the slowdown/stop threshold:
- Slowdown: artificial delay on write path
- Stop: block writers until compaction reduces L0 count
The signal (condvar) must be sent AFTER compaction completes AND VersionEdit is applied.

**Check**: Verify condvar::notify is called after the new version (with fewer L0 files) is installed.

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
Writer blocks on condvar waiting for compaction. Compaction completes and calls notify, but the writer hasn't entered wait() yet — notification is lost.
**Check**: Verify condvar wait is in a loop that re-checks the condition: `while l0_count >= stop_threshold { condvar.wait(&mut guard); }`

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

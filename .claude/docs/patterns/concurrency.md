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

> **Note**: L0 backpressure does not use a condvar. The known wait protocols in
> `db.rs` are `write_cv` and `compaction_notify`; derive the live inventory when
> either synchronization path changes.

## Critical Invariants

### INV-CC1: Lock Ordering
Nested locks must have an acyclic order derived from current code; do not invent
a separate "version_set lock" that does not exist. The main `db_mutex`,
MANIFEST-writer lock, write-queue lock, cache locks, and notification locks have
different responsibilities.

**Check**: Build the actual lock graph for changed paths and verify no reverse
edge. On normal write/compaction paths, do not hold `db_mutex` across SST or
MANIFEST I/O. Dedicated I/O locks and the documented shutdown path may span
blocking calls and must be assessed by their own protocol.

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
3. Append every WAL-enabled batch in that same order
4. Fsync when any grouped request asked for `sync`; otherwise flush the WAL
   buffer (skip WAL entirely for `disable_wal` requests)
5. Insert successfully assigned batches into MemTable in the same order
6. Publish `committed_sequence`, then notify waiters

The leader holds the write lock through the mutation, but readers are lock-free:
`committed_sequence` is the visibility publication barrier. A reader must never
use a sequence that has not finished MemTable insertion.
**Check**: Verify ordering in both loops, the fsync-vs-flush aggregation rule,
and Release publication after all assigned inserts.

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

Error policy per caller of `drain_l0`:
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
- A reader or iterator that pinned an older SuperVersion must retain its
  `Arc<TableReader>` ownership even if the obsolete pathname is unlinked after
  durable MANIFEST install

**Check**: Verify the snapshot list controls MVCC garbage collection and old
SuperVersions keep readers/data alive across cache eviction and pathname
cleanup.

## Common Bug Patterns

### Deadlock via Lock Inversion
Thread A holds one subsystem lock and waits for a second while another thread
holds them in the reverse order.
**Check**: Map all lock acquisition paths and verify no cycles.

### Lost Wakeup
A thread waits on a condvar under lock, but the notifier calls `notify_all()` before the waiter acquires the lock and enters `wait()`. The notification is lost, and the waiter blocks forever.

**Known condvar wait patterns in db.rs** — derive the live set and verify each
wait is in a loop re-checking its condition:

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
- [ ] No blocking SST/MANIFEST I/O under `db_mutex` on normal paths; documented dedicated-lock/shutdown exceptions are justified
- [ ] Atomic ordering matches the required guarantee (`Relaxed` is valid for counters/hints; publication edges use Acquire/Release or stronger)

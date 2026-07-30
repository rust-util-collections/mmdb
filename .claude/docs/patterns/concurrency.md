# Concurrency Review Patterns

## Model

```
Writers → group-commit leader → WAL + MemTable (serialized)
Readers → ArcSwap<SuperVersion> (lock-free) → MemTable + SST
BG compact → SST I/O unlocked; VersionSet under lock
Flush → write-path auto / flush() / compact_range / close
```

| Primitive | Where | Role |
|-----------|-------|------|
| `Mutex` (parking_lot) | db | Write serialize, VersionSet |
| `ArcSwap<SuperVersion>` | db | Lock-free reads |
| `Arc<MemTable>` | db | MemTable lifetime |
| `Atomic*` | db, skiplist | Counters, links |
| `write_cv` | db | Group-commit queue |
| `compaction_notify` (std) | db | BG compact wake |
| `thread::scope` | compact | Sub-jobs |

L0 backpressure uses sleep/inline drain — no condvar. Inventory live wait sites when changing sync.

## Invariants

**CC1 Lock order** — acyclic graph from **current** code (`db_mutex`, MANIFEST writer,
write-queue, cache, notify locks). No reverse edges. Normal write/compact: no
`db_mutex` across SST/MANIFEST I/O. Dedicated I/O locks and shutdown may block — judge by protocol.

**CC2 ArcSwap publish** — fully init referenced data → `store` → old SV stays valid until guards drop.

**CC3 Group commit** — collect → contiguous seq → WAL in order → fsync if any
`sync` else flush (skip WAL if `disable_wal`) → MemTable same order → publish
`committed_sequence` (Release) → notify. Barrier for lock-free readers: never expose
seq before inserts finish.

**CC4 BG unlocked I/O** — pick under lock → I/O unlocked → apply under lock.
Compact threads never freeze/flush memtables. Auto-flush / flush / compact_range:
drop lock for SST write. `close`/freeze_and_flush may hold lock (shutdown).

`drain_l0`: short pick → unlocked merge → short install → unlocked MANIFEST sync
(from flush post-trigger and `maybe_throttle_writes` stop). `force_merge_level`
may hold lock for admin merge I/O — never from ordinary write path.

`drain_l0` errors: flush post-trigger non-fatal unless fail-stop/poison (durability already done); stop-trigger → fail-stop; in-drain MANIFEST sync fail → set BG error then propagate.

**CC5 L0 backpressure** — atomic L0 count, no condvar. Slowdown = progressive sleep;
stop = inline `drain_l0` unlocked merge; fail-stop on compact error (no silent admit).
Refresh atomic after every L0-changing install.

**CC6 Snapshots** — pin seq; compact must keep covered entries; old SuperVersion
Arcs keep readers alive across unlink/evict after durable MANIFEST.

## Bug patterns

**Lock inversion deadlock** — map all paths.
**Lost wakeup** — `while cond { wait }` not `if`; notify after cond change under same lock.
  Known: `write_cv` (done/leader), `compaction_notify` (has_work/shutdown).
**ArcSwap load-modify-store** — lost update; all SV updates under `db_mutex`.
**Sub-compact races** — scope closures: immutable shared; own output buffers.

## Checklist

- [ ] Acyclic lock order on changed paths
- [ ] ArcSwap after data fully committed
- [ ] Group commit order + release start after inserts
- [ ] No `db_mutex` across normal SST/MANIFEST I/O
- [ ] Condvar waits loop + recheck
- [ ] Snapshots drive tombstone/seq decisions
- [ ] Sub-jobs: no shared mut
- [ ] Dedicated-lock/shutdown I/O justified
- [ ] Atomic order matches need (`Relaxed` counters OK; publication stronger)

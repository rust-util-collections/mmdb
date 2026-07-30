# MMDB Design Anti-Patterns

Design lens for reviews. Language/protocol bugs: `technical-patterns.md`, `patterns/*`.
Report only with a concrete failure; apply `false-positive-guide.md` first.

| Prefix | Question |
|--------|----------|
| D-LOCK | Convoy, deadlock, or needless serialization? |
| D-RES | Acquire released on every path under load/error? |
| D-BOUND | Work, fan-out, memory, or retries uncapped? |
| D-STATE | Concurrent/partial exec → illegal or split state? |
| D-FAIL | Error/crash outcome safe, observable, unambiguous? |
| D-API | Export/default/semantics break callers? |

## D-LOCK

- **1** Global exclusive lock where ArcSwap/atomics/channels suffice, **or** lock-free snapshot used for a latest-state decision (SuperVersion rules).
- **2** Lock across disk I/O, fsync, network, or heavy work.
- **3** Nested locks without documented order (`patterns/concurrency.md`).
- **4** Check-then-act / lazy init without one continuous guard.

Skip documented single-writer + intentional snapshot reads unless a multi-writer convoy is shown.

## D-RES

- **1** Unbounded memory (pin, vec, map) without lifecycle cap/eviction.
- **2** FD/mmap leak on error (RAII `Drop` is fine).
- **3** Background thread not stopped on DB close.
- **4** Temp/obsolete WAL/SST not cleaned after success or recoverable failure.
- **5** Work continues after cancel/drop when protocol requires stop.

## D-BOUND

- **1** Unbounded queue/batch (write group, compact jobs) without backpressure.
- **2** Unbounded concurrency when fan-out can grow with L0.
- **3** Retry/compaction without budget that amplifies load.
- **4** Hot-path full-file materialization when streaming/blocks suffice.
- **5** Expensive work before a cheap reject (corrupt/range/options).

## D-STATE

- **1** Multi-step durable transition not atomic (WAL↔MemTable; MANIFEST↔CURRENT; partial VersionEdit).
- **2** Dual write without single install order / recovery owner.
- **3** Illegal MVCC/seq (dup seq, `committed_sequence` before inserts, wrong snapshot cutoff).
- **4** TOCTOU outside the serializing lock (compact pick, delete vs open).
- **5** Ordering/uniqueness from wall clock only under concurrency.

## D-FAIL

- **1** Durability side effect dropped after log/catch while reporting success.
- **2** Partial success returned as full success.
- **3** Silent degrade (checksum ignored; corrupt table as empty).
- **4** Distinct failures collapsed (retry vs fatal unclear).
- **5** Crash mid-protocol → unrecoverable or lying on-disk state.

## D-API

- **1** `src/lib.rs` export change without docs/tests.
- **2** Default/error/iterator/snapshot semantic change without call-site evidence.
- **3** Internal detail leaked into public surface without need.

## Apply when

- Multi-subsystem or design-shaped diff: walk relevant families once.
- Full audit: after subsystem depth, on write path / install / cache pin / unsafe / public API.
- Small shape-preserving edit: skip empty families.

Severity: `review-core.md`. IDs label evidence; they are not findings alone.

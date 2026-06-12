# Known Issues & Limitations

This document tracks known design limitations and won't-fix issues that are not bugs but may affect users.

> **See also**: `.claude/audit.md` — the audit backlog with detailed technical findings (severity, root cause, suggested fixes). This file documents user-visible limitations; the audit backlog documents code-level issues.

## Design Limitations

### max_immutable_memtables Not Enforced
The `max_immutable_memtables` option (default: 4) is declared but not enforced by the write path. The freeze trigger checks `active_memtable.approximate_size() >= write_buffer_size` without limiting the number of immutable memtables. In practice, flush keeps up with ingest so the count rarely exceeds 1-2, but under extreme write pressure it could grow unbounded.

### Mutex-Held I/O in Explicit Compact
`compact()` (and the optional post-flush compaction inside `flush()` / the inline L0-stop compaction in the write path) runs `do_compaction()`, which holds the DB mutex during compaction I/O. This blocks writers (and other maintenance) for the duration — point reads stay lock-free via `ArcSwap<SuperVersion>`. The explicit `flush()` SST write itself releases the mutex during I/O, as does background compaction. This is a known trade-off: the synchronous compaction path prioritizes correctness (no race with concurrent writes) over throughput.

### MANIFEST Compaction Under DB Mutex
Every 1000th MANIFEST edit triggers manifest compaction (create new file, write, fsync, rename, fsync directory). This happens inside `log_and_apply()` which holds the DB mutex. For most workloads this is imperceptible (~1ms every 1000 writes), but under sustained small-write workloads it adds a periodic latency spike.

### Range Tombstone O(N) Scan in Point Lookups
The point-lookup `get` path iterates ALL L1+ files that have range deletions to check for covering tombstones (O(files_with_range_dels)). This is O(N) in the number of tombstone-bearing files. The iterator path bounds this by checking `smallest_key > upper_bound` to skip files entirely past the range, but the point-lookup path cannot apply the same optimization since range tombstones can extend past a file's largest key.

### InternalKey Panics on Malformed Data
`InternalKey::from_encoded()`, `InternalKey::from_encoded_slice()`, and `InternalKeyRef::new()` use `assert!` guards that panic (in both debug and release builds, with a clear message) if the encoded key is < 8 bytes. `InternalKey::new()` needs no guard — it builds the key from components and always appends the 8-byte trailer. The design assumes all encoded keys come from CRC-protected storage and WAL, making this condition unreachable in practice.

### DbOptions Debug Missing Fields
The `Debug` implementation for `DbOptions` omits ~14 of ~30 fields (including `l0_slowdown_trigger`, `l0_stop_trigger`, `rate_limiter_bytes_per_sec`, `compression_per_level`, and others). Debug output is intentionally concise — use `{:#?}` for full display via derived Debug on inner types.

## Non-Issues (Known Design Decisions)

### Invalid ValueType → Deletion (Not an Error)
`ValueType::from_u8()` returns `None` for values > 2, and callers use `.unwrap_or(ValueType::Deletion)` to treat unknown types as deletions. This is intentional: (1) all stored data is CRC-protected, making corruption unreachable in practice; (2) treating unknown types as deletions is a fail-safe that removes data rather than silently propagating corruption.

### Corrupt Block Entry Could Read Restart Array
A corrupt varint in a block entry could produce lengths that fall between `restart_offset` and `data.len()`, causing the entry decoder to read restart-array bytes. CRC32 verification (per-block, checked before any decoder runs) blocks corrupt blocks. The window exists only under CRC32 collision (~2^-32 probability).

### "Safety:" Comment in Non-Unsafe Context
`types.rs:73-75` uses `// Safety:` prefix in a regular function (`unpack_sequence_and_type`, not `unsafe`). This is a design rationale note, not an unsafe-code safety justification. The prefix is a style choice to emphasize the invariant the function relies on.

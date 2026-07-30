# Audit Findings

> Auto-managed by /x-review and /x-fix.
>
> **Won't Fix ≠ permanent.** Re-evaluate an entry when a review touches its
> code, callers, assumptions, or subsystem; a full audit re-evaluates every
> entry.
>
> **Rejected is not Won't Fix.** Rejected entries are disproven claims, not
> deferred defects. Re-check them only when their cited code or invariant
> changes.

## Open

*(none)*

---

## Won't Fix

### [MEDIUM] iterator: seek paths do not overlap cross-source I/O prefetch
- **Where**: `src/iterator/merge.rs` (`init_heap`, seek / direction switch), `src/iterator/source.rs` (`prefetch_hint`, `seek_to`)
- **What**: Explicit seeks and bidirectional direction switches synchronously position and decode each source before heap initialization can issue cross-source prefetch hints.
- **Reason**: SST index entries are already memory-resident, so a targeted pre-seek hint phase is feasible, and direction switches make the path warmer than explicit seeks alone. However, `posix_fadvise` is advisory and no controlled cold-cache multi-source benchmark demonstrates a material latency regression; changing the protocol without that evidence remains disproportionate.

### [LOW] manifest: file-number arithmetic can overflow at `u64::MAX`
- **Where**: `src/manifest/version_set.rs` (`new_file_number`, `reserve_file_numbers`, MANIFEST rotation)
- **What**: File allocation, reservations, and MANIFEST rotation increment `u64` counters without checked arithmetic.
- **Reason**: Reaching exhaustion through production allocation requires roughly 1.8e19 file numbers; all reservation counts are bounded by in-memory workload sizes. The failure is mathematically real but not practically reachable. Revisit if identifiers become externally supplied or allocation jumps by unbounded amounts.

### [LOW] rate_limiter: `request()` f64 subtraction can stop converging for enormous values
- **Where**: `src/rate_limiter.rs`
- **What**: For a single request around hundreds of petabytes, `chunk` can fall below half an ULP of `remaining`, making `remaining -= chunk` a no-op and the loop non-terminating.
- **Reason**: Every production call passes one entry's encoded size, bounded by the 64 MiB write-entry limit and allocatable memory. The private API cannot receive the theoretical trigger.

### [LOW] compaction: near-duplicate merge-loop logic
- **Where**: `src/compaction/leveled.rs` (normal sub-compaction vs `force_merge_level`)
- **What**: Normal and forced compaction independently implement closely related tombstone, snapshot, deduplication, filter, and sequence-zeroing logic.
- **Reason**: Both paths are currently consistent, while extracting one shared state machine across their different sub-range and streaming protocols would carry disproportionate regression risk. Revisit when a correctness change must touch either loop.

### [LOW] SST: restart-count validation can overflow `usize` on 32-bit targets
- **Where**: `src/sst/block.rs`
- **What**: `(num_restarts as usize) * 4 + 4` can overflow on a 32-bit target for corrupted input.
- **Reason**: The supported and CI target is 64-bit Linux; no 32-bit support is declared. Revisit if 32-bit targets are added.

### [VERY LOW] memtable: skiplist node destructors skipped if `all_nodes.push` unwinds after `ptr::write`
- **Where**: `src/memtable/skiplist_impl.rs`
- **What**: If the `all_nodes` bookkeeping push unwound between `ptr::write` initializing a node and the push completing, the node's key/value heap allocations would never be dropped.
- **Reason**: `Vec::push` aborts via `handle_alloc_error` on allocation failure, and its capacity-overflow panic requires `len > isize::MAX`; no unwinding path reaches the window on supported targets. Reordering the unsafe insert protocol to close an unreachable leak carries more regression risk than value.

### [MEDIUM] API: `WriteBatch` has no entry-count or aggregate-size cap
- **Where**: `src/types.rs` (WriteBatch), `src/db.rs` (`write_batch_inner`)
- **What**: A caller can assemble arbitrarily large batches; the write path encodes the whole batch as one WAL record and applies it under the write lock.
- **Reason**: Batch memory is allocated by the caller before `write()` is ever reached, so an engine-side cap cannot protect the process — it only adds config surface. Engine invariants are protected by the per-entry caps (`MAX_USER_KEY_SIZE`, `MAX_WRITE_ENTRY_SIZE`) and the WAL u32 entry-count guard; the transient WAL-encode duplication is bounded by the caller's own batch size.

### [MEDIUM] API: snapshots and iterators are uncapped pinning resources
- **Where**: `src/db.rs` (`SnapshotList`, iterator constructors)
- **What**: Every live snapshot/iterator pins a SuperVersion (memtables, SST readers); nothing limits how many a caller may hold.
- **Reason**: These are caller-owned RAII handles — the standard LSM engine contract (RocksDB likewise imposes no cap). An engine-side limit would turn application handle leaks into spurious engine errors instead of a diagnosable application defect.

### [MEDIUM] options: no global memory budget across subsystems
- **Where**: `src/options.rs` (`DbOptions`)
- **What**: There is no `max_total_memory`-style option enforcing one budget across memtables, caches, iterators, and compaction.
- **Reason**: Cross-subsystem budget accounting is a feature request, not a defect; each subsystem is individually bounded and documented (`write_buffer_size`, `block_cache_capacity`, `max_open_files`, rate limiter). Revisit if a hosting environment requires hard aggregate limits.

### [LOW] options: `num_levels` accepts arbitrarily large values
- **Where**: `src/options.rs`, `src/db.rs` (open-time validation)
- **What**: Only `num_levels >= 2` is validated; a huge value allocates per-level `Vec` headers in every `Version` and one merge source per level in every iterator.
- **Reason**: The cost is linear, small, and entirely self-inflicted configuration; introducing an upper bound now could refuse to open stores created with larger values. Revisit if per-level state stops being O(1).

---

## Rejected

### [MEDIUM] WAL: `WalWriter` needs a `Drop` impl to avoid losing buffered records
- **Where**: `src/wal/writer.rs`
- **What**: Claim: `BufWriter` discards its buffer on drop, so a `WalWriter` dropped without an explicit flush silently loses up to one buffer of records.
- **Reason**: The premise is false — `std::io::BufWriter`'s `Drop` flushes the buffer (only errors are ignored, per its documentation). Independently, every commit path flushes or syncs the WAL before acknowledging a write, so drop-time behavior only concerns unacknowledged data on panic unwind.

### [MEDIUM] memtable: range tombstones evade `approximate_size` accounting
- **Where**: `src/memtable/mod.rs`
- **What**: Claim: valid `delete_range(begin, end)` entries with `begin < end` are nearly free in `approximate_size()`, so their volume never triggers a flush.
- **Reason**: `MemTable::put` accounts the duplicated begin/end keys plus `MemRangeTombstone` struct overhead for every valid `RangeDeletion` entry, in addition to the skiplist entry itself. Invalid/no-op ranges are a separate open write-path defect because they are discarded before this accounting.

### [MEDIUM] write path: group-commit queue depth is unbounded
- **Where**: `src/db.rs` (`WriteQueueState`)
- **What**: Claim: the `VecDeque<*mut WriteRequest>` grows without limit, allowing unbounded memory growth under write pressure.
- **Reason**: Each queue entry is a raw pointer to a *blocked* caller's stack frame; a thread enqueues at most one request and then waits on the condvar until the leader completes it. Queue depth therefore equals the number of concurrently blocked writer threads — the caller's thread budget — and cannot accumulate beyond it.

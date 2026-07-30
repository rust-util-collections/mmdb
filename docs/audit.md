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
### [HIGH] compaction: range-scoped picking can move tombstones below covered source keys
- **Where**: `src/compaction/leveled.rs` (`pick_compaction_for_range`)
- **What**: A narrow range compaction selects source files by their point-key metadata but does not close the selection over overlapping same-level siblings. It can move a range tombstone to L1 while a covered point remains in L0, after which level-aware iteration ignores the deeper tombstone and resurrects the point.
- **Why**: Target-level overlap is transitively closed, but source-level overlap is not; `is_bottommost_level` only controls dropping and sequence zeroing, not whether a tombstone may move below a covered key.
- **Suggested fix**: Transitively close source inputs before selecting target inputs for L0 and L1+ range picks. Regress a narrow compaction over separate point/tombstone SSTs and check get plus forward/reverse iteration.

### [HIGH] iterator: overlapping range tombstones have quadratic construction and storage
- **Where**: `src/iterator/range_del.rs` (`FragmentedRangeTombstoneList`), `src/sst/table_reader/mod.rs`
- **What**: Every endpoint fragment clones and sorts every active `(sequence, level)` pair. Nested tombstones therefore require quadratic work and memory; per-table caching then expands the fragments and aggregate iterators fragment them again.
- **Why**: The representation materializes the fragment-by-active-tombstone product instead of retaining each raw interval once or sharing active state.
- **Suggested fix**: Preserve an O(T) canonical form and use an O(T)-space query representation or bounded fallback for high overlap. Regress nested N/2N inputs with cardinality and coverage assertions.

### [HIGH] API: lazy-delete pruning can forget a concurrent rewrite
- **Where**: `src/db.rs` (`prune_settled_dead_keys`, write path)
- **What**: Pruning observes an absent key without holding writer serialization, then removes its registration after a concurrent put commits the same key. Future compaction preserves that rewrite even though it occurred while lazy deletion was active.
- **Why**: The absence probe and registration removal are a check-then-act sequence with no generation or write-order validation.
- **Suggested fix**: Make final validation/removal atomic with relevant writes, or use equivalent registration generations. Add a deterministic race between the absence probe and removal.

### [MEDIUM] open: partial compaction-thread spawn failure leaks earlier workers
- **Where**: `src/db.rs` (background compaction thread startup)
- **What**: If a later worker spawn fails, `DB::open` returns immediately while already spawned workers retain shared state and wait indefinitely.
- **Why**: The error path neither signals shutdown nor joins the handles accumulated before the failed spawn.
- **Suggested fix**: On partial startup failure, signal and notify shutdown, then join all prior workers before returning the spawn error. Regress with an injectable failure on worker N.

### [MEDIUM] iterator: `BidiIterator` hides underlying lazy-iteration failures
- **Where**: `src/iterator/bidi_iter.rs`
- **What**: SST I/O or corruption can make a lazy bidirectional iterator return `None`, but it exposes no `error()` accessor. The initial reverse transition also replaces the underlying state before all early returns, risking loss of the source error.
- **Why**: The wrapper preserves the item-only `Iterator` surface but omits `DBIterator`'s separate error channel.
- **Suggested fix**: Forward the underlying error for every lazy state and preserve state across reverse initialization. Regress forward and reverse failures.

### [MEDIUM] SST: L0 first-block pinning performs large reads under the DB mutex
- **Where**: `src/db.rs` (`install_flush`), `src/sst/table_reader/mod.rs` (`pin_metadata_in_cache`)
- **What**: Default L0 pinning can synchronously read, checksum, allocate, and decompress a legal first data block of up to 64 MiB during locked flush installation, convoying writers and admin paths.
- **Why**: Unlocked prewarm loads structural metadata but not the first data block; the cache miss is serviced only after the DB mutex is reacquired.
- **Suggested fix**: Prepare the first-block cache payload before the install critical section and only publish/pin it after a successful install, with failure cleanup. Regress using a blocking loader or prepared-pin hook.

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

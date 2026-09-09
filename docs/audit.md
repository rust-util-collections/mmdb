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

### [CRITICAL] WAL/MANIFEST: tail classification can discard later complete records
- **Where**: `src/wal/reader.rs` (`read_physical_record`), `src/db.rs` and `src/manifest/version_set.rs` (recovery)
- **What**: Failed physical reads classify a zero-suffixed payload or an EOF short read as a torn tail without inspecting the bytes already consumed under the untrusted length.
- **Why**: An enlarged middle-record length can consume a later checksum-valid record ending in zero, or extend beyond EOF. Recovery then accepts the prefix and can delete the WAL or truncate the MANIFEST containing the later committed state.
- **Suggested fix**: Before allowing tail recovery, check the failed payload's actual consumed bytes for later checksum-valid physical fragments; preserve ordinary torn/zero-extended tail recovery and test both failure shapes through WAL and MANIFEST recovery.

### [HIGH] read path: sequence capture can precede the retained file view
- **Where**: `src/db.rs` (`get_with_options`, `iter_with_range`, `iter_with_prefix`, `iter_with_batch`)
- **What**: Ordinary reads resolve their sequence before pinning a SuperVersion.
- **Why**: A concurrent overwrite, flush, and non-bottommost compaction can discard the version at that sequence before the reader loads its file view. The remaining newer version is filtered out, so an always-present key can appear absent. Ordinary reads do not register a snapshot.
- **Suggested fix**: Pin the SuperVersion before resolving the committed sequence on every read constructor; exercise the publication gap deterministically.

### [HIGH] iterator: backward heap construction reopens exhausted sources
- **Where**: `src/iterator/merge.rs` (`init_heap`), `src/iterator/level_iter.rs` (failed backward seek)
- **What**: Backward heap initialization issues forward prefetch/peek operations even though backward seeks have already positioned each source.
- **Why**: With `b"b"` in L1 and `b"a\xff"` in the memtable, reverse prefix seeks for `b"a\xff"` reopen the out-of-range L1 source. Its key triggers the prefix stop before the matching memtable key is returned.
- **Suggested fix**: Populate forward buffers only in forward mode; backward heaps must use the buffers seeded by backward positioning. Test reverse prefix seeks and exhausted single/multiple sources.

### [MEDIUM] iterator: lazy bidirectional wrapping loses a buffered entry
- **Where**: `src/iterator/bidi_iter.rs` (first lazy `next_back`), `src/iterator/db_iter.rs` (`ensure_current`, `last_user_key`)
- **What**: The backward frontier uses the last examined user key as if it had already been consumed.
- **Why**: `valid()` or `key()` buffers a visible entry without returning it. Wrapping that iterator in `BidiIterator::lazy` then excludes the buffered key, so a one-key iterator becomes empty.
- **Suggested fix**: Preserve the inclusive boundary of buffered entries separately from the exclusive boundary of consumed entries; test wrapping after inspection and after consumption.

### [MEDIUM] write path: a maximum-sized range deletion cannot be flushed
- **Where**: `src/db.rs` (`write_batch_inner`), `src/sst/table_builder.rs` (range-deletion metadata limit), `src/types.rs` (write limits)
- **What**: The generic write-entry limit exceeds the range-deletion metadata budget by 4096 bytes.
- **Why**: An otherwise valid range deletion near `MAX_WRITE_ENTRY_SIZE` can be acknowledged into the WAL, then fail every flush and writable recovery because its single metadata entry exceeds `META_BLOCK_HARD_LIMIT`.
- **Suggested fix**: Validate a range-specific payload ceiling before WAL/sequence assignment, tie it to the builder's framing allowance, and test atomic rejection plus a flushable boundary entry.

### [LOW] CI: read-only integration tests are never executed
- **Where**: `.github/workflows/ci.yml` (`test` job)
- **What**: CI enumerates the integration binaries but omits `tests/read_only.rs`.
- **Why**: Compilation alone does not exercise the read-only mutation guards, residual WAL recovery, or cooperative locking checks, so behavioral regressions can pass CI.
- **Suggested fix**: Run the existing read-only integration binary in the test job.


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
- **Reason**: Batch memory is allocated by the caller before `write()` is ever reached, so an engine-side cap cannot protect the process — it only adds config surface. The per-entry caps and WAL u32 entry-count guard bound individual encodings; the transient WAL-encode duplication is bounded by the caller's own batch size. Per-user-key SST metadata accumulation is a separate limitation below.

### [MEDIUM] SST: metadata for one user key can exceed a single-file limit
- **Where**: `src/db.rs` (`write_memtable_ssts`), `src/sst/table_builder.rs` (`projected_index_size`, range-deletion accounting)
- **What**: Output splitting waits for a user-key boundary. Four versions of one 8 MiB key under the default 4 KiB block size can exceed the index budget while remaining below the default memtable threshold. Multiple large range tombstones sharing one begin key can similarly exceed their metadata block budget. Writes can be acknowledged before flush and writable recovery reject the oversized metadata.
- **Reason**: WAL data remains intact, and read-only recovery can inspect it. Larger `block_size` can recover the point-version case; ordinary small vsdb keys do not approach it. A general fix requires per-key admission accounting across writes/recovery or a format and lookup change permitting same-key metadata to span files. Those changes are disproportionate for this unusual key/endpoint workload; the independently fixable single-range-entry boundary remains actionable above.

### [MEDIUM] API: snapshots and iterators are uncapped pinning resources
- **Where**: `src/db.rs` (`SnapshotList`, iterator constructors)
- **What**: Snapshots register retention sequences; iterators hold owning memtable/SST reader references. Nothing limits how many handles a caller may hold.
- **Reason**: These are caller-owned RAII handles — the standard LSM engine contract (RocksDB likewise imposes no cap). An engine-side limit would turn application handle leaks into spurious engine errors instead of a diagnosable application defect.

### [MEDIUM] options: no global memory budget across subsystems
- **Where**: `src/options.rs` (`DbOptions`)
- **What**: There is no `max_total_memory`-style option enforcing one budget across memtables, caches, iterators, and compaction.
- **Reason**: Cross-subsystem budget accounting is a feature request, not a defect; each subsystem is individually bounded and documented (`write_buffer_size`, `block_cache_capacity`, `max_open_files`, rate limiter). Revisit if a hosting environment requires hard aggregate limits.

### [LOW] options: `num_levels` accepts arbitrarily large values
- **Where**: `src/options.rs`, `src/db.rs` (open-time validation)
- **What**: Only `num_levels >= 2` is validated; a huge value allocates per-level `Vec` headers in every `Version` and one merge source per level in every iterator.
- **Reason**: The cost is linear, small, and entirely self-inflicted configuration; introducing an upper bound now could refuse to open stores created with larger values. Revisit if per-level state stops being O(1).

### [LOW] API: `open_read_only` cannot open stores configured with `num_levels > 7`
- **Where**: `src/db.rs` (`open_read_only`), `src/options.rs` (`num_levels` default = 7)
- **What**: `open_read_only` builds `DbOptions::default()`, so recovery refuses any store that has a live file at `level >= 7` with `ErrorKind::Corruption` ("configured with num_levels 7"), even though the store is healthy under its original `num_levels`.
- **Reason**: `num_levels` is not persisted in MANIFEST, so the convenience method cannot infer it; the documented fallback (`DB::open_read_only_with_options` and the matching `num_levels`) already covers non-default stores, and the error message names that fix. Persisting or deriving `num_levels` is disproportionate for an unusual (>7-level) configuration.

---

## Rejected

### write path: `close`/`compact_range`/stop-drain holding `write_queue` across install can deadlock a group-commit leader
- **Where**: `src/db.rs` (`close`, `compact_range`, `maybe_throttle_writes`, `wait_for_write_leader_idle`)
- **Claim**: The new `wait_for_write_leader_idle` barrier could deadlock against a leader that needs `write_queue` to finish, or against `prune_settled_dead_keys` re-acquiring it.
- **Reason**: The wait predicate is `leader_active`, which a leader clears only after re-acquiring `write_queue` in `write_batch_group`'s completion step and calling `write_cv.notify_all()` — so the waiter is always woken. `maybe_throttle_writes` runs before the caller enqueues or becomes leader, and every `prune_settled_dead_keys` call site (`flush`, `compact`, `compact_range`, the leader's post-wake harvest) releases the guard first, so no path re-enters the non-reentrant mutex while holding it.

### iterator: `decode_internal_key` returning owned `uk` in the reverse path adds a hot-path allocation
- **Where**: `src/iterator/db_iter.rs` (`prev`, the backward resolve loop)
- **Claim**: Replacing the borrowed `&ikey[..uk_len]` with `uk.to_vec()` allocates once per entry examined during backward iteration.
- **Reason**: The allocation is forced by the borrow checker, not incidental: the loop moves `ikey` into `prev_overshoot` and into `best_entry` while `uk` is still live. The same function already clones the user key into `candidate_uk` and `current_bound` on the normal path, so the added cost is one small `Vec` per examined entry on a path that already performs per-entry block decoding and clones — not a material change in path class, and no benchmark shows a regression.

### [MEDIUM] WAL: `WalWriter` needs a `Drop` impl to avoid losing buffered records
- **Where**: `src/wal/writer.rs`
- **What**: Claim: `BufWriter` discards its buffer on drop, so a `WalWriter` dropped without an explicit flush silently loses up to one buffer of records.
- **Reason**: The premise is false — `std::io::BufWriter`'s `Drop` flushes the buffer (only errors are ignored, per its documentation). Independently, every commit path flushes or syncs the WAL before acknowledging a write, so drop-time behavior only concerns unacknowledged data on panic unwind.

### [MEDIUM] memtable: range tombstones evade `approximate_size` accounting
- **Where**: `src/memtable/mod.rs`
- **What**: Claim: valid `delete_range(begin, end)` entries with `begin < end` are nearly free in `approximate_size()`, so their volume never triggers a flush.
- **Reason**: `MemTable::put` accounts the duplicated begin/end keys plus `MemRangeTombstone` struct overhead for every valid `RangeDeletion` entry, in addition to the skiplist entry itself. Empty/inverted ranges are removed in `write_batch_inner` before WAL encoding and sequence assignment.

### [MEDIUM] write path: group-commit queue depth is unbounded
- **Where**: `src/db.rs` (`WriteQueueState`)
- **What**: Claim: the `VecDeque<*mut WriteRequest>` grows without limit, allowing unbounded memory growth under write pressure.
- **Reason**: Each queue entry is a raw pointer to a *blocked* caller's stack frame; a thread enqueues at most one request and then waits on the condvar until the leader completes it. Queue depth therefore equals the number of concurrently blocked writer threads — the caller's thread budget — and cannot accumulate beyond it.

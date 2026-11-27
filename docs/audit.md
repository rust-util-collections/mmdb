# Storage-Engine Review Findings

> Maintained by `$x-review`, `$x-fix`, and `$x-overhaul`. Forms and severities: `docs/ai/review-core.md`.
>
> Severities: CRITICAL, HIGH, MEDIUM, LOW.
>
> **Won't Fix** records confirmed defects accepted under the [disposition rules](ai/workflow-policy.md#5-finding-dispositions). During review, record defects confined to unsupported architectures here with the affected target and evidence. Re-check entries when their code or assumptions change and during full audits.
>
> **Rejected** is a disproven claim, not a deferred defect. Re-check it only when its cited code or invariant changes.
>
> Do not reclassify an entry just to empty Open. No dates.

## Open

None.

---

## Won't Fix

### [MEDIUM] compaction: a 4 GiB SST size target truncates to zero on 32-bit targets

- **Where**: `src/compaction/leveled.rs` (`execute_sub_compaction_io`, the `target_file_size_base as usize` split check).
- **What**: Compaction casts the configured `u64` target to `usize`. On a 32-bit target, `target_file_size_base = 1 << 32` becomes zero, so a rewrite requests an SST split at every distinct user-key boundary.
- **Reason**: Confirmed by tracing the integer conversion and split predicate. The supported 64-bit ABIs preserve this value. The project accepts the 32-bit behavior under its [architecture policy](../README.md#supported-architectures).

### [LOW] lazy_delete: in-flight compaction wait test uses a fixed sleep
- **Where**: `tests/lazy_delete.rs` (`compact_waits_for_inflight_compaction_and_removes_dead_keys`, the 200 ms delay before opening the filter gate)
- **What**: The test releases the background compaction gate after a fixed delay instead of after `compact_range` has observed the in-flight claim. An unlucky schedule can let `compact_range` start after the release, so the wait path is not exercised; the final assertion can still pass.
- **Reason**: The final assertion catches the primary regression whenever the early-return path is taken, and the scheduler state machine has deterministic unit coverage in `src/db.rs`. Observing the wait predicate from the test needs a hook inside the production path under test, which is disproportionate for this timing-only gap. Retained as an accepted test limitation; the snapshot-deferral test no longer depends on timing.

### [LOW] sst: Linux `advise_willneed` converts `posix_fadvise` arguments to `i64` instead of `libc::off_t`

- **Where**: `src/sst/table_reader/mod.rs` (`TableReader::advise_willneed`, the `i64::try_from` conversions passed to `libc::posix_fadvise`).
- **What**: `advise_willneed` converts `offset` and `len` with `i64::try_from` and passes them directly to `libc::posix_fadvise`. On 32-bit GNU, uClibc, and Android Linux targets (`libc` `b32`), `libc::off_t` is `i32`, so compiling for those targets fails with `E0308` mismatched types.
- **Reason**: Confirmed against `libc-0.2.191` target definitions (`off_t = i32` on 32-bit Linux ABIs vs `off_t = i64` on 64-bit Linux ABIs). Both supported architectures (`x86_64` and `aarch64` with 64-bit ABIs) define `libc::off_t` as `i64`, so 32-bit targets are outside the [supported architecture guarantee](../README.md#supported-architectures).

---

## Rejected

### iterator: backward-to-forward switching skips remaining keys after a saved overshoot
- **Where**: `src/iterator/bidi_iter.rs` (`lazy`, first backward call and forward resumption), `src/iterator/db_iter.rs` (`prev`, `next_lazy`)
- **Claim**: With keys `a,b,c,d,e`, passing an iterator positioned by `seek_to_last(); prev()` to `BidiIterator::lazy`, consuming `e,d` through `next_back()`, then calling `next()` should return `c,b,a` rather than `None`.
- **Reason**: `lazy` preserves the input iterator's remaining forward entries. After this positioning, the current key is `d` and direct forward collection returns `d,e`. Consuming both from the back exhausts that range; `c,b,a` are outside it. For an unpositioned iterator, consuming `e,d` from the back leaves `a,b,c` in forward order. Public integration tests `test_bidi_reverse_primed_preserves_remaining_entries` and `test_bidi_backward_then_forward_reads_remaining_in_order` cover both cases; the reverse-primed case checks every two-read direction combination against the directly collected sequence in memtable and flushed/compacted storage.

### SST: an index separator that is a range-deletion key makes point lookups miss a visible entry
- **Where**: `src/sst/table_builder.rs` (`shorten_separator`), `src/sst/table_reader/mod.rs` (`point_value_with_probe`, `partition_point_index_leaf`)
- **Claim**: An index separator encoded with `ValueType::RangeDeletion` can exceed a visible data key in its block or skip a block that holds the target.
- **Reason**: `RangeDeletion` entries route directly to `add_range_fragment` without touching `data_block`, and `shorten_separator` emits `InternalKey::new(&prefix, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion)` only when `lo < prefix < hi` strictly as user keys (equal-user-key boundaries retain the full unshortened `last_key`). Thus `last_key < separator < next_first_key` holds in internal-key order without crossing any data key's user key, and `ReadState::point_lookup` folds covering range tombstones independently via `covering_sequence`.

### write path: `close`/`compact_range`/stop-drain holding `write_queue` across install can deadlock a group-commit leader
- **Where**: `src/db.rs` (`acquire_write_barrier`, `release_write_leadership`, `maybe_throttle_writes`, `prune_settled_dead_keys`)
- **Claim**: A barrier waiting for a leader holds `write_queue` across that wait, preventing the leader from finishing, or re-enters the mutex during pruning.
- **Reason**: The barrier releases the mutex in `write_cv.wait`. The leader clears `leader_active` under the mutex and notifies waiting barriers. Throttling runs before enqueue, and the flush, compaction and group-completion paths release their write guards before pruning.

### WAL: `WalWriter` needs a `Drop` impl to avoid losing buffered records
- **Where**: `src/wal/writer.rs`
- **Claim**: `BufWriter` discards its buffer on drop, so a `WalWriter` dropped without an explicit flush silently loses up to one buffer of records.
- **Reason**: `std::io::BufWriter` attempts to flush its buffer on drop and ignores errors. For WAL-enabled batches processed by a write group, the explicit flush or sync establishes the acknowledgement boundary. Increasing the buffer capacity preserves this behavior.

### memtable: range tombstones evade `approximate_size` accounting
- **Where**: `src/memtable/mod.rs`, `src/types.rs`
- **Claim**: valid `delete_range(begin, end)` entries with `begin < end` are nearly free in `approximate_size()`, so their volume never triggers a flush.
- **Reason**: Valid range deletions charge arena slabs, the raw range vector, endpoint bytes, and tombstone-index heap (`tombstone_index_bytes`) across `MemTable::put`, `put_batch`, and frozen fragmentation. Those charges accumulate toward rotation.

### write path: group-commit queue depth is unbounded
- **Where**: `src/db.rs` (`WriteQueueState`)
- **Claim**: the `VecDeque<*mut WriteRequest>` grows without limit, allowing unbounded memory growth under write pressure.
- **Reason**: A waiting follower has one outstanding request and spins or parks inside its synchronous write call. Queueing more live requests requires more caller threads. The arrival path checks `can_drain_with` before taking leadership, and a promoted follower must be the queue head.

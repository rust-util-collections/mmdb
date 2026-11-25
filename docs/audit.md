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

- **Where**: `src/compaction/leveled.rs:539,710–723` (`execute_sub_compaction_io`).
- **What**: Compaction casts the configured `u64` target to `usize`. On a 32-bit target, `target_file_size_base = 1 << 32` becomes zero, so a rewrite requests an SST split at every distinct user-key boundary.
- **Reason**: Confirmed by tracing the integer conversion and split predicate. The supported 64-bit ABIs preserve this value. The project accepts the 32-bit behavior under its [architecture policy](../README.md#supported-architectures).

### [MEDIUM] v4 exporter: the Windows rename binding uses the wrong ABI on 32-bit x86

- **Where**: `tools/v4-export/src/main.rs` (`publish`), `tools/v4-export/Cargo.toml`, and `renamest` 0.1.3 (`src/windows.rs:8–13,26`).
- **What**: The exporter's Windows backend declares `MoveFileExW` with `extern "C"`. The Win32 binding uses `extern "system"`, which selects `stdcall` on 32-bit x86. This mismatch can prevent linking or cause incorrect stack cleanup if the call resolves. [Windows binding](https://docs.rs/windows-sys/0.61.2/src/windows_sys/Windows/Win32/Storage/FileSystem/mod.rs.html#273) · [Rust ABI reference](https://doc.rust-lang.org/reference/items/external-blocks.html#abi)
- **Reason**: Static inspection establishes the declaration mismatch. Its effect is specific to the unsupported 32-bit x86 Windows ABI. Re-check the entry when the exporter changes rename backends or updates the dependency.

### [LOW] lazy_delete: in-flight compaction wait test uses a fixed sleep
- **Where**: `tests/lazy_delete.rs` (`compact_waits_for_inflight_compaction_and_removes_dead_keys`, the 200 ms delay before opening the filter gate)
- **What**: The test releases the background compaction gate after a fixed delay instead of after `compact_range` has observed the in-flight claim. An unlucky schedule can let `compact_range` start after the release, so the wait path is not exercised; the final assertion can still pass.
- **Reason**: The final assertion catches the primary regression whenever the early-return path is taken, and the scheduler state machine has deterministic unit coverage in `src/db.rs`. Observing the wait predicate from the test needs a hook inside the production path under test, which is disproportionate for this timing-only gap. Retained as an accepted test limitation; the snapshot-deferral test no longer depends on timing.

---

## Rejected

### iterator: backward-to-forward switching skips remaining keys after a saved overshoot
- **Where**: `src/iterator/bidi_iter.rs` (`lazy`, first backward call and forward resumption), `src/iterator/db_iter.rs` (`prev`, `next_lazy`)
- **Claim**: With keys `a,b,c,d,e`, passing an iterator positioned by `seek_to_last(); prev()` to `BidiIterator::lazy`, consuming `e,d` through `next_back()`, then calling `next()` should return `c,b,a` rather than `None`.
- **Reason**: `lazy` preserves the input iterator's remaining forward entries. After this positioning, the current key is `d` and direct forward collection returns `d,e`. Consuming both from the back exhausts that range; `c,b,a` are outside it. For an unpositioned iterator, consuming `e,d` from the back leaves `a,b,c` in forward order. Public integration tests `test_bidi_reverse_primed_preserves_remaining_entries` and `test_bidi_backward_then_forward_reads_remaining_in_order` cover both cases; the reverse-primed case checks every two-read direction combination against the directly collected sequence in memtable and flushed/compacted storage.

### SST: an index separator that is a range-deletion key makes point lookups miss a visible entry
- **Where**: `src/sst/table_builder.rs` (`shorten_separator`), `src/sst/table_reader/mod.rs` (`point_value`, `find_data_block`)
- **Claim**: An index separator encoded with `ValueType::RangeDeletion` can exceed a visible data key in its block or skip a block that holds the target.
- **Reason**: In v5, `RangeDeletion` entries route directly to `add_range_fragment` without touching `data_block`, and `shorten_separator` emits `InternalKey::new(&prefix, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion)` only when `lo < prefix < hi` strictly as user keys (equal-user-key boundaries retain the full unshortened `last_key`). Thus `last_key < separator < next_first_key` holds in internal-key order without crossing any data key's user key, and `ReadState::point_lookup` folds covering range tombstones independently via `covering_sequence`.

### write path: `close`/`compact_range`/stop-drain holding `write_queue` across install can deadlock a group-commit leader
- **Where**: `src/db.rs` (`close`, `compact_range`, `maybe_throttle_writes`, `wait_for_write_leader_idle`)
- **Claim**: The new `wait_for_write_leader_idle` barrier could deadlock against a leader that needs `write_queue` to finish, or against `prune_settled_dead_keys` re-acquiring it.
- **Reason**: The wait predicate is `leader_active`, which a leader clears only after re-acquiring `write_queue` in `enqueue_write`'s completion step (or `flush_generation` / `WriteLeaderGuard`) and calling `write_cv.notify_all()` — so the waiter is always woken. `maybe_throttle_writes` runs before the caller enqueues or becomes leader, and every `prune_settled_dead_keys` call site (`flush`, `compact`, `compact_range`, the leader's post-wake harvest) releases the guard first, so no path re-enters the non-reentrant mutex while holding it.

### WAL: `WalWriter` needs a `Drop` impl to avoid losing buffered records
- **Where**: `src/wal/writer.rs`
- **Claim**: `BufWriter` discards its buffer on drop, so a `WalWriter` dropped without an explicit flush silently loses up to one buffer of records.
- **Reason**: The premise is false — `std::io::BufWriter`'s `Drop` flushes the buffer (only errors are ignored, per its documentation). Independently, each WAL-enabled write group flushes or syncs the WAL before acknowledging its WAL-enabled batches, so drop-time behavior only concerns unacknowledged data on panic unwind.

### memtable: range tombstones evade `approximate_size` accounting
- **Where**: `src/memtable/mod.rs`, `src/types.rs`
- **Claim**: valid `delete_range(begin, end)` entries with `begin < end` are nearly free in `approximate_size()`, so their volume never triggers a flush.
- **Reason**: `MemTable::put` accounts newly reserved arena slab capacity, duplicated `begin`/`end` endpoint bytes, and `MemRangeTombstone` vector capacity growth for every valid `RangeDeletion` entry. Empty/inverted ranges (`begin >= end`) are ignored in `WriteBatch::delete_range_prefixed` before WAL encoding and sequence assignment (and guarded again in `MemTable::put`).

### write path: group-commit queue depth is unbounded
- **Where**: `src/db.rs` (`WriteQueueState`)
- **Claim**: the `VecDeque<*mut WriteRequest>` grows without limit, allowing unbounded memory growth under write pressure.
- **Reason**: Each queue entry is a raw pointer to a *blocked* caller's stack frame; a thread enqueues at most one request and then waits on the condvar until the leader completes it. Queue depth therefore equals the number of concurrently blocked writer threads — the caller's thread budget — and cannot accumulate beyond it.

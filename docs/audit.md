# Storage-Engine Review Findings

> Maintained by `$x-review`, `$x-fix`, and `$x-overhaul`. Forms and severities: `docs/ai/review-core.md`.
>
> Severities: CRITICAL, HIGH, MEDIUM, LOW.
>
> **Won't Fix** is a confirmed defect whose complete fix has disproportionate cost or regression risk. Not a feature request or a documented contract. `$x-review` does not add these. Re-check one when a review touches its code, callers, assumptions, or subsystem; a full audit re-checks every entry.
>
> **Rejected** is a disproven claim, not a deferred defect. Re-check it only when its cited code or invariant changes.
>
> Do not reclassify an entry just to empty Open. No dates.

## Open

None.

---

## Won't Fix

### [LOW] manifest: file-number arithmetic can overflow at `u64::MAX`
- **Where**: `src/manifest/version_set.rs` (`new_file_number`, `reserve_file_numbers`, MANIFEST rotation)
- **What**: File allocation, reservations, and MANIFEST rotation increment `u64` counters without checked arithmetic.
- **Reason**: Reaching exhaustion through production allocation requires roughly 1.8e19 file numbers; all reservation counts are bounded by in-memory workload sizes. Replay takes a stored `next_file_number` without an upper bound (unlike `last_sequence`), but a value near `u64::MAX` needs a checksum-valid MANIFEST record or an on-disk file name this allocator never produced. The failure is mathematically real but not practically reachable. Revisit if identifiers become externally supplied or allocation jumps by unbounded amounts.

### [LOW] SST: restart-count validation can overflow `usize` on 32-bit targets
- **Where**: `src/sst/block.rs`
- **What**: `(num_restarts as usize) * 4 + 4` can overflow on a 32-bit target for corrupted input.
- **Reason**: The supported and CI target is 64-bit Linux; no 32-bit support is declared. Revisit if 32-bit targets are added.

### [MEDIUM] SST: metadata for one user key can exceed a single-file limit
- **Where**: `src/db.rs` (`write_memtable_ssts`), `src/sst/table_builder.rs` (`projected_index_size`, range-deletion accounting)
- **What**: Output splitting waits for a user-key boundary. Four versions of one 8 MiB key under the default 4 KiB block size can exceed the index budget while remaining below the default memtable threshold. Multiple large range tombstones sharing one begin key can similarly exceed their metadata block budget. Writes can be acknowledged before flush and writable recovery reject the oversized metadata.
- **Reason**: WAL data remains intact, and read-only recovery can inspect it. Larger `block_size` can recover the point-version case; ordinary small vsdb keys do not approach it. A general fix requires per-key admission accounting across writes/recovery or a format and lookup change permitting same-key metadata to span files. Those changes are disproportionate for this unusual key/endpoint workload; single-range admission now reserves framing and split headroom, but repeated begin keys can still exceed the aggregate budget.

### [LOW] options: `num_levels` accepts arbitrarily large values
- **Where**: `src/options.rs`, `src/db.rs` (open-time validation)
- **What**: Only `num_levels >= 2` is validated; a huge value allocates per-level `Vec` headers in every `Version` and one merge source per level in every iterator.
- **Reason**: The cost is linear and entirely self-inflicted configuration; an extreme value such as `usize::MAX` aborts inside `DB::open` while allocating per-level vectors rather than reaching a stored state. Introducing an upper bound now could refuse to open stores created with larger values. Revisit if per-level state stops being O(1).

### [LOW] lazy_delete: in-flight compaction wait test uses a fixed sleep
- **Where**: `tests/lazy_delete.rs` (`compact_waits_for_inflight_compaction_and_removes_dead_keys`, the 200 ms delay before opening the filter gate)
- **What**: The test releases the background compaction gate after a fixed delay instead of after `compact_range` has observed the in-flight claim. An unlucky schedule can let `compact_range` start after the release, so the wait path is not exercised; the final assertion can still pass.
- **Reason**: The final assertion catches the primary regression whenever the early-return path is taken, and the scheduler state machine has deterministic unit coverage in `src/db.rs`. Observing the wait predicate from the test needs a hook inside the production path under test, which is disproportionate for this timing-only gap. Retained as an accepted test limitation; the snapshot-deferral test no longer depends on timing.

---

## Rejected

### SST: an index separator that is a range-deletion key makes point lookups miss a visible entry
- **Where**: `src/sst/table_builder.rs:281-297` (`last_key` set for `RangeDeletion` entries), `src/sst/table_reader/mod.rs` (`get_internal_with_seq`)
- **Claim**: A block separator taken from a buffered range-deletion key can exceed every data key in its block, so an index seek skips a block that holds the target.
- **Reason**: A miss needs seek key S ≤ separator R < entry X with S and X sharing a user key, which forces R to share that user key with X.seq < R.seq ≤ S.seq, so X is always covered by R. `DB::get` folds the file's covering tombstone sequence in before any point lookup, and iterator `seek` continues into the next block.

### write path: `close`/`compact_range`/stop-drain holding `write_queue` across install can deadlock a group-commit leader
- **Where**: `src/db.rs` (`close`, `compact_range`, `maybe_throttle_writes`, `wait_for_write_leader_idle`)
- **Claim**: The new `wait_for_write_leader_idle` barrier could deadlock against a leader that needs `write_queue` to finish, or against `prune_settled_dead_keys` re-acquiring it.
- **Reason**: The wait predicate is `leader_active`, which a leader clears only after re-acquiring `write_queue` in `write_batch_group`'s completion step and calling `write_cv.notify_all()` — so the waiter is always woken. `maybe_throttle_writes` runs before the caller enqueues or becomes leader, and every `prune_settled_dead_keys` call site (`flush`, `compact`, `compact_range`, the leader's post-wake harvest) releases the guard first, so no path re-enters the non-reentrant mutex while holding it.

### iterator: `decode_internal_key` returning owned `uk` in the reverse path adds a hot-path allocation
- **Where**: `src/iterator/db_iter.rs` (`prev`, the backward resolve loop)
- **Claim**: Replacing the borrowed `&ikey[..uk_len]` with `uk.to_vec()` allocates once per entry examined during backward iteration.
- **Reason**: The allocation is forced by the borrow checker, not incidental: the loop moves `ikey` into `prev_overshoot` and into `best_entry` while `uk` is still live. The same function already clones the user key into `candidate_uk` and `current_bound` on the normal path, so the added cost is one small `Vec` per examined entry on a path that already performs per-entry block decoding and clones — not a material change in path class, and no benchmark shows a regression.

### WAL: `WalWriter` needs a `Drop` impl to avoid losing buffered records
- **Where**: `src/wal/writer.rs`
- **Claim**: `BufWriter` discards its buffer on drop, so a `WalWriter` dropped without an explicit flush silently loses up to one buffer of records.
- **Reason**: The premise is false — `std::io::BufWriter`'s `Drop` flushes the buffer (only errors are ignored, per its documentation). Independently, every commit path flushes or syncs the WAL before acknowledging a write, so drop-time behavior only concerns unacknowledged data on panic unwind.

### memtable: range tombstones evade `approximate_size` accounting
- **Where**: `src/memtable/mod.rs`
- **Claim**: valid `delete_range(begin, end)` entries with `begin < end` are nearly free in `approximate_size()`, so their volume never triggers a flush.
- **Reason**: `MemTable::put` accounts the duplicated begin/end keys plus `MemRangeTombstone` struct overhead for every valid `RangeDeletion` entry, in addition to the skiplist entry itself. Empty/inverted ranges are removed in `write_batch_inner` before WAL encoding and sequence assignment.

### write path: group-commit queue depth is unbounded
- **Where**: `src/db.rs` (`WriteQueueState`)
- **Claim**: the `VecDeque<*mut WriteRequest>` grows without limit, allowing unbounded memory growth under write pressure.
- **Reason**: Each queue entry is a raw pointer to a *blocked* caller's stack frame; a thread enqueues at most one request and then waits on the condvar until the leader completes it. Queue depth therefore equals the number of concurrently blocked writer threads — the caller's thread budget — and cannot accumulate beyond it.

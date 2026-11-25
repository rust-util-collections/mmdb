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

### [MEDIUM] SST: partitioned index leaves are not validated against their directory partition bounds
- **Where**: `src/sst/table_reader/mod.rs:338-388`, `src/sst/table_reader/mod.rs:660-673`, `src/sst/table_reader/iterator.rs:296-302`
- **What**: When a corrupt SST index directory handle or re-CRC'd index leaf disagrees with `TableReader::partitions`, point lookups and iterators/compaction should surface `ErrorKind::Corruption` (matching the `range_partitions` checks in `covering_sequence` and `visit_ranges`). Instead, `find_data_block` returns `Ok(None)` when `seek_by_reuse` misses inside a selected partition, and `index_leaf` / `TableIterator` accept empty or out-of-partition leaves as clean data (`iter_error() == None`), allowing silent key loss or a `TableBuilder::add` ordering panic during compaction.
- **Why**: `IndexWriter` guarantees each partition `part` points to a non-empty leaf ending at `partitions[part].0` and (when `part > 0`) starting strictly after `partitions[part - 1].0`. `index_leaf` only validates intra-leaf entry pairs via `parse_index_entries`, and `find_data_block` neither rejects `seek_by_reuse == None` nor checks `decode_internal_key` / `(partitions[part - 1].0, partitions[part].0]` bounds on the matched entry.
- **Fix**: In `TableReader::index_leaf`, reject empty leaves, require `sep.compare_internal(&entries.last().unwrap().separator_key).is_eq()`, and when `part > 0` require `self.partitions[part - 1].0.compare_internal(&entries[0].first_key).is_lt()`. In `TableReader::find_data_block`, return `Err(Error::corruption("index leaf ends before its separator"))` on `None` and validate `decode_internal_key` plus partition bounds on the matched entry.

### [MEDIUM] manifest: `VersionSet` accepts empty, malformed, or inverted `FileMetaData` key bounds
- **Where**: `src/manifest/version_set.rs:192-248`
- **What**: `VersionSet::open_table` and `VersionSet::validate_level_disjointness` should reject any `FileMetaData` whose `smallest_key` or `largest_key` is shorter than an 8-byte `InternalKey`, carries an invalid `ValueType` byte, or has `smallest_key > largest_key` with `ErrorKind::Corruption`. Instead, malformed or inverted bounds install cleanly on live apply and MANIFEST replay, causing `ReadState::point_lookup` and `LevelIterator` binary search to skip live SSTs and return `Ok(None)`.
- **Why**: `VersionEdit::decode` (`src/manifest/version_edit.rs:114-117`) slices raw length-prefixed bytes without `InternalKey` validation, `open_table` checks only `has_range_deletions`, and `validate_level_disjointness` (`line 236`) skips pairs with empty `largest_key`/`smallest_key` and never checks intra-file `smallest_key <= largest_key`.
- **Fix**: Validate `decode_internal_key(&meta.smallest_key)?`, `decode_internal_key(&meta.largest_key)?`, `compare_internal_key(&meta.smallest_key, &meta.largest_key).is_le()`, and `!reader.partitions.is_empty() || reader.has_range_deletions()` in `VersionSet::open_table`, and remove the empty-key skip in `VersionSet::validate_level_disjointness`.

### [MEDIUM] flush: `FlushContext::flush` signals `compaction_notify` while still holding `CompactionClaim` on new L0 SSTs
- **Where**: `src/db/flush.rs:169-216`, `src/compaction/leveled.rs:822-833`
- **What**: After an auto-flush installs new L0 SSTs at or above `l0_compaction_trigger`, the woken background compaction worker should see those L0 files unclaimed and compact L0 into L1. Instead, `_claim` in `FlushContext::flush` drops at line `216` *after* `compaction_notify` is signaled at lines `213-214`; if `mmdb-compaction` wakes and runs `pick_compaction` first, `pick_l0_compaction` sees the new L0 SST in `in_flight`, returns `None`, and goes back to sleep with L0 uncompacted above trigger.
- **Why**: `CompactionClaim::drop` notifies `CompactionClaims::released` rather than `compaction_notify`, and foreground writes below `l0_slowdown_trigger` do not re-signal `compaction_notify`. Explicit `DB::flush()` masks this in existing tests because it calls `drain_l0(false)` after `FlushContext::flush` returns and `_claim` has dropped.
- **Fix**: Bind `claim` in `FlushContext::flush` and explicitly `drop(claim)` after retiring the flushed memtable from `immutable_memtables` and refreshing `SuperVersion` (`src/db/flush.rs:205`), before locking and notifying `self.compaction_notify`.

### [MEDIUM] compaction: `execute_sub_compaction_io` reclassifies `MergingIterator` `Corruption` and `Io` errors as `ErrorKind::Background`
- **Where**: `src/compaction/leveled.rs:745-750`
- **What**: Synchronous `DB::compact()` / `DB::compact_range(...)` over a corrupt SST data block or read failure should preserve the underlying `ErrorKind::Corruption` / `ErrorKind::Io` and `source()` chain from `MergingIterator::error()`. Instead, `execute_sub_compaction_io` wraps `e` in `Error::background(format!("sub-compaction merge error: {}", e))`, returning `ErrorKind::Background` even though the engine is not fail-stopped (`has_bg_error` and `manifest_poisoned` remain `false`).
- **Why**: `MergingIterator::error()` was refactored from `Option<&str>` in v4 to `Option<&Error>` in v5, but line `746` still stringifies `e` into `Error::background(...)`, disagreeing with `collect_range_fragments` (`line 1127`) and `ErrorKind::Background`'s fail-stop contract (`src/error.rs:50-53`). Background workers and stop-trigger inline drains already latch `set_bg_error` / `Error::background` at their own call sites.
- **Fix**: Return `Err(e.clone().context("sub-compaction merge error"))` in `execute_sub_compaction_io`.

### [MEDIUM] SST: `TableIterator::seek`, `seek_for_prev`, and `load_next_block` read out-of-bounds index leaves past `upper_bound`
- **Where**: `src/sst/table_reader/iterator.rs:275-293`, `src/sst/table_reader/iterator.rs:448-458`, `src/sst/table_reader/iterator.rs:673-677`
- **What**: Seeking an upper-bounded iterator (`iterate_upper_bound = Some(ub)`) to `target >= ub` should exhaust or clamp to `ub` without reading, caching, or failing on excluded SST index partitions. Instead, `TableIterator::seek` calls `select_partition(seek_partition(target))` before checking `user_key(target) >= ub`, `load_next_block` calls `ensure_index()` before checking `index_pos == usize::MAX`, and `seek_for_prev` loads excluded partitions between `target` and `ub` before clamping.
- **Why**: `DBIterator::seek` forwards `target >= ub` directly to `merger.seek_opt`. In v4 the flat index was already resident on `TableReader`; in v5 `select_partition` / `ensure_index` perform disk `pread`, decompression, block-cache insertion, and corruption latching on ~64 KiB index leaves. Existing upper-bound tests used single-partition SSTs and corrupted only excluded data blocks.
- **Fix**: Check `user_key(target) >= ub` and call `exhaust_at_upper_bound()` before `select_partition` in `TableIterator::seek`; check `self.err.is_some() || self.index_pos == usize::MAX` before `self.ensure_index()` in `load_next_block`; and clamp `target` with `user_key(target) >= ub` to `InternalKey::new(ub, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion)` at the start of `TableIterator::seek_for_prev`.

### [MEDIUM] manifest: `sync_new_sst_parent_dir` opens transient directory handles unlocked without serializing `poisoned` publication
- **Where**: `src/manifest/version_set.rs:37-49`, `src/manifest/version_set.rs:849-858`, `src/manifest/version_set.rs:947-959`
- **What**: Directory `fsync` failures on SST creation or `CURRENT` rotation must fail-stop `VersionSet::poisoned` before any concurrent flush or compaction can observe a clean directory sync, commit its `VersionEdit`, and unlink superseded WAL/SST files (matching `confirm_manifest_durable` at `src/manifest/version_set.rs:62-81`). Instead, `sync_new_sst_parent_dir` runs unlocked and opens a fresh `File::open(path)` per call without holding a guard across `sync_all()` and `poisoned.store(true, Ordering::Release)`.
- **Why**: Moving SST directory sync outside `DB::inner` in v5 allows concurrent `sync_new_sst_parent_dir` calls. If one caller's `fsync_directory` returns `Err` and is preempted before `poisoned.store(true, Ordering::Release)`, a concurrent caller can open a fresh directory descriptor (which on Linux `errseq_t` also misses writeback errors recorded prior to `open()`), succeed, persist its `VersionEdit`, and delete `old_wal`.
- **Fix**: Retain an open directory `File` under a `Mutex` on `VersionSet` (opened on writable `create`/`recover`) and serialize `sync_new_sst_parent_dir` and `maybe_compact_manifest` directory syncs plus `poisoned.store(true, Ordering::Release)` under that guard.

### [LOW] docs: `DB::open_read_only` and `DB::open_read_only_with_options` rustdoc contradicts v5 MANIFEST geometry recovery
- **Where**: `src/db.rs:763-767`, `src/db.rs:793-796`
- **What**: Public rustdoc on `DB::open_read_only` and `DB::open_read_only_with_options` should match v5 behavior (`src/lib.rs:34-35`, `README.md:418`), where `num_levels` and `prefix_len` are stored in the MANIFEST `DBHeader` and adopted automatically on read-only open. Instead, both docstrings still state `num_levels` is not stored in the MANIFEST and instruct callers to pass the writer's `num_levels`.
- **Why**: Leftover v4 rustdoc paragraphs missed when `DBHeader` and `versions.geometry()` adoption (`src/db.rs:978-1009`) were introduced in `261a370`.
- **Fix**: Update `src/db.rs:763-767` and `src/db.rs:793-796` to document that read-only open adopts `num_levels` and `prefix_len` from the MANIFEST `DBHeader` and ignores caller-supplied geometry/writer options.

### [LOW] build/CI: `make ci` omits `tools/v4-export` tests, CI `fmt`/`clippy` skip `tools/v4-export`, and `.gitignore` leaves `tools/v4-export/target/` untracked
- **Where**: `Makefile:21-35`, `.github/workflows/ci.yml:21-44`, `.github/workflows/ci.yml:65`, `.gitignore:1`
- **What**: `Makefile` `ci` (documented as `# Same gate as .github/workflows/ci.yml`) omits `cargo test --manifest-path tools/v4-export/Cargo.toml`, neither `Makefile` nor `.github/workflows/ci.yml` runs `fmt --check` or `clippy --all-targets -- -D warnings` on `tools/v4-export`, and `.gitignore` uses root-anchored `/target` so building/testing `tools/v4-export` leaves `?? tools/v4-export/target/` untracked.
- **Why**: `tools/v4-export` is excluded from the root Cargo workspace (`exclude = ["tools/v4-export"]`) because it depends on both `mmdb = "=4.3.15"` and path `../..`, so root Cargo commands skip it unless `--manifest-path tools/v4-export/Cargo.toml` is passed explicitly.
- **Fix**: Add `--manifest-path tools/v4-export/Cargo.toml` test/lint/fmt checks to `Makefile` and `.github/workflows/ci.yml`, and change `/target` to `target/` in `.gitignore`.

---

## Won't Fix

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
- **Reason**: The wait predicate is `leader_active`, which a leader clears only after re-acquiring `write_queue` in `write_batch_group`'s completion step and calling `write_cv.notify_all()` — so the waiter is always woken. `maybe_throttle_writes` runs before the caller enqueues or becomes leader, and every `prune_settled_dead_keys` call site (`flush`, `compact`, `compact_range`, the leader's post-wake harvest) releases the guard first, so no path re-enters the non-reentrant mutex while holding it.

### WAL: `WalWriter` needs a `Drop` impl to avoid losing buffered records
- **Where**: `src/wal/writer.rs`
- **Claim**: `BufWriter` discards its buffer on drop, so a `WalWriter` dropped without an explicit flush silently loses up to one buffer of records.
- **Reason**: The premise is false — `std::io::BufWriter`'s `Drop` flushes the buffer (only errors are ignored, per its documentation). Independently, every commit path flushes or syncs the WAL before acknowledging a write, so drop-time behavior only concerns unacknowledged data on panic unwind.

### memtable: range tombstones evade `approximate_size` accounting
- **Where**: `src/memtable/mod.rs`, `src/types.rs`
- **Claim**: valid `delete_range(begin, end)` entries with `begin < end` are nearly free in `approximate_size()`, so their volume never triggers a flush.
- **Reason**: `MemTable::put` accounts newly reserved arena slab capacity, duplicated `begin`/`end` endpoint bytes, `MemRangeTombstone` vector capacity growth, and `same_key_metadata` bucket charges for every valid `RangeDeletion` entry. Empty/inverted ranges (`begin >= end`) are ignored in `WriteBatch::delete_range_prefixed` before WAL encoding and sequence assignment (and guarded again in `MemTable::put`).

### write path: group-commit queue depth is unbounded
- **Where**: `src/db.rs` (`WriteQueueState`)
- **Claim**: the `VecDeque<*mut WriteRequest>` grows without limit, allowing unbounded memory growth under write pressure.
- **Reason**: Each queue entry is a raw pointer to a *blocked* caller's stack frame; a thread enqueues at most one request and then waits on the condvar until the leader completes it. Queue depth therefore equals the number of concurrently blocked writer threads — the caller's thread budget — and cannot accumulate beyond it.

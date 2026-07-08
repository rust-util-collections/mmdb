# Audit Findings

> Auto-managed by /x-review and /x-fix.
>
> **Won't Fix ≠ permanent.** Every entry under `## Won't Fix` must be
> re-evaluated against the current codebase on each audit. Surrounding code
> changes, new callers, or API evolution may make a previously-disproportionate
> fix straightforward — or make the finding obsolete. Never silently carry
> forward a Won't Fix entry without fresh assessment.

## Open

*(none)*

---

## Resolved

### [CRITICAL] sst: Malformed block entries silently swallowed as clean end-of-block, causing silent data loss and silent seek-misses
- **Where**: `src/sst/block.rs` (`decode_entry_at`, `decode_entry_reuse`, `seek_by`, `seek_for_prev_by`, `iter_from_restart`, `iter_restart_segment`, `BlockIterator::next`); `src/sst/table_reader/mod.rs` (`get_internal_with_seq`, `cached_range_tombstones`); `src/sst/table_reader/iterator.rs` (`cursor_next_lazy`, `seek_within_block`)
- **What**: `decode_entry_at`/`decode_entry_reuse` return `Option`, using `None` to mean both "cleanly reached end of block" and "entry is structurally malformed" (shared>prev_key.len(), value_end>data.len(), bad varint — all bounds-checked/panic-safe, but ambiguous). Every caller uses the `Some(..) => .., None => break/None` idiom with no way to tell these apart, and none set an error. Empirically reproduced: a single corrupted `unshared_len` byte in entry #3 of a 5-entry block (with CRC recomputed over the corrupted bytes to isolate this code path from the whole-block CRC check) causes the full production `TableIterator` path (open → seek_to_first → next()) to silently return only 2 of 5 entries with `iter_error() == None`. Consequences: point lookups can return false "not found"; forward iteration silently truncates the block and transparently resumes at the next block; `seek()`'s binary search/linear scan can silently miss a key that exists after the corruption point; `cached_range_tombstones()` silently drops range tombstones from the corruption point onward, which can let previously-deleted data resurface during compaction/reads.
- **Why**: Same functions/failure-class as technical-patterns.md 2.4 (Block Prefix Compression Corruption). Contradicts the codebase's own demonstrated fail-fast intent: `test_deferred_block_read_error_does_not_skip_to_next_block` explicitly asserts a whole-block CRC failure must propagate as an error rather than silently skip to the next block — the identical contract is violated one layer deeper for partial/mid-block entry corruption not covered by the pre-decompression whole-block CRC.
- **Suggested fix**: Change `decode_entry_at`/`decode_entry_reuse` to return `Result<Option<(...)>, Error>` (`Ok(None)` = clean EOF at a restart boundary; `Err` = malformed entry). Update all callers to propagate `Err` as a hard error — in `TableIterator`, set `self.err` (mirroring the existing whole-block CRC failure path) instead of returning `None`/`break`; in `TableReader::get_internal_with_seq`/`cached_range_tombstones`, propagate via `?` instead of silently treating corruption as "not found"/"no more tombstones".
- **Resolution**: Fixed — `decode_entry_at`/`decode_entry_reuse` now return `Result<(...), Error>` (no `Option` wrapper: every caller-supplied offset was already guaranteed in-bounds, so there was no legitimate "clean EOF" case at this level). All callers updated to propagate corruption as a hard error: `Block::seek_by`/`seek_for_prev_by`/`iter_from_restart`/`iter_restart_segment`/`first_key_at_restart` now return `Result<Option<...>>`/`Result<Vec<...>>`; `BlockIterator` gained an `error()` accessor mirroring `TableIterator`'s `iter_error()` convention; `TableReader::get_internal_with_seq`/`get_internal`/`cached_range_tombstones`/`iter` and `TableIterator::cursor_next_lazy`/`seek_within_block`/`next_into`/`next_lazy`/`prev`/`seek_for_prev`/`seek_to_last` all propagate the error instead of silently treating it as exhaustion.

---

### [CRITICAL] manifest: Pre-write directory fsync in `log_and_apply` doesn't poison on failure, unlike 3 structurally identical fsync branches in the same file
- **Where**: `src/manifest/version_set.rs` (`log_and_apply`'s pre-write `Self::fsync_directory(&self.db_path)` call for new SST files' directory entries; contrast `add_record` failure, `maybe_compact_manifest`'s old-writer sync failure, and `maybe_compact_manifest`'s post-rename directory fsync — all three poison on failure via `self.poisoned.store(true, ...)`)
- **What**: Before persisting a `VersionEdit` that adds new SST files, `log_and_apply` fsyncs the DB directory so new files' directory entries survive a crash. If this fsync fails, the error propagates via `?` but `self.poisoned` is NOT set — unlike 3 other fsync/sync failure branches in the same file (including the *same* `fsync_directory()` call at a different call site in `maybe_compact_manifest`, which DOES poison). Exploitable with `max_background_compactions > 1` (e.g. `DbOptions::write_heavy()` sets it to 4): the background thread loop only checks `shutdown` before waiting for the next job, never `has_bg_error`, so sibling compaction threads keep calling `log_and_apply` after this failure. Since `self.poisoned` (checked at the top of `log_and_apply`) is the only thing that stops them, a sibling thread's own `fsync_directory(&self.db_path)` call on the same path can spuriously "succeed" (fsyncgate) while a prior thread's SST directory entry was never durably persisted. `TableBuilder::finish` only fsyncs SST file *content*, never its parent directory, so this call is the sole durability guard for SST directory entries in the entire codebase.
- **Why**: Violates the fail-stop policy ("every in-service manifest writer fsync failure poisons the shared flag consistently") and the classic "fsyncgate" hazard the codebase's own comments invoke at the sibling call site.
- **Suggested fix**: Poison on this failure too, matching the sibling branches.
- **Resolution**: Fixed — the pre-write `fsync_directory` failure branch now stores `self.poisoned.store(true, Ordering::Release)` before returning the error, matching the sibling branches exactly.

---

### [HIGH] write-read-path/compaction: `force_merge_level` holds `db_mutex` across a full-level compaction rewrite, automatically triggered by ordinary `lazy_delete` use
- **Where**: `src/compaction/leveled.rs` (`force_merge_level`); `src/db.rs` (dead-key-sweep loop in the background compaction thread; `lazy_delete`/`lazy_delete_batch` triggers); `src/options.rs` (`lazy_delete_compaction_threshold`, default 10,000); contrast `src/db.rs`'s `force_compact_all` doc comment, which explicitly scopes this exact lock-hold tradeoff to "administrative" callers only
- **What**: `force_merge_level` takes `&mut VersionSet` (never a releasable guard) and performs a full synchronous read-merge-rewrite-fsync-delete of every SST in a level while the caller holds `bg_inner.lock()` — the same mutex `write_batch_group` needs for every write and `snapshot_seq()` needs for new snapshots. The dead-key sweep (armed automatically once `lazy_delete`/`lazy_delete_batch` cross the default-enabled threshold of 10,000) reaches this exact path with no administrative opt-in, freezing all writers and new-snapshot creation for the duration of a full-level rewrite. The single-file/no-op early-return doesn't help here because `LazyDeleteFilter` is non-noop whenever the sweep is running.
- **Why**: Violates concurrency.md INV-CC4 (background threads must not hold `db_mutex` during I/O; 3-phase lock-release pattern). `force_compact_all`'s own comment justifies this lock-hold behavior only for its own (administrative, opt-in) callers — the automatic dead-key sweep is exactly the case that reasoning excludes.
- **Suggested fix**: Split `force_merge_level` into an unlocked "build output files" phase and a short-locked "install VersionEdit + refresh l0_file_count/SuperVersion" phase, mirroring the existing `execute_compaction_io`/`install_compaction` split used by debt-driven compaction and `drain_l0`. At minimum, document the write-stall in `lazy_delete`'s doc comment and consider defaulting `lazy_delete_compaction_threshold` to 0 (opt-in).
- **Resolution**: Fixed via the lower-risk "at minimum" option rather than the full lock-split refactor (deferred given the regression risk of restructuring the codebase's most complex, invariant-critical function this late in the audit cycle): `lazy_delete_compaction_threshold`'s default changed from `10_000` to `0` (disabled), making the automatic, lock-holding sweep strictly opt-in. `DbOptions::lazy_delete_compaction_threshold`'s doc and `DB::lazy_delete`/`lazy_delete_batch`'s doc comments now explicitly document the write-stall tradeoff so callers who opt in (set a nonzero threshold) make an informed choice, mirroring `force_compact_all`'s already-administrative, already-documented tradeoff. The full `force_merge_level` lock-split remains a valid future improvement if the opt-in default proves insufficient.

---

### [HIGH] sst: Zstd block decompression always allocates a full 64MB buffer regardless of actual block size
- **Where**: `src/sst/table_reader/mod.rs` (Zstd decompression call site using `MAX_DECOMPRESSED_BLOCK_SIZE`); contrast the LZ4 path in the same file, which correctly sizes via a prepended real size
- **What**: `zstd::bulk::decompress(data, capacity)` internally calls `Decompressor::upper_bound()` to use the frame's embedded content size as the real allocation size — but `upper_bound()` requires the `experimental` cargo feature, which is NOT enabled (`zstd = "0.13"`, no extra features). Without it, `upper_bound()` always returns `None`, so the full 64MB `MAX_DECOMPRESSED_BLOCK_SIZE` is used verbatim as the `Vec::with_capacity()` size on every single Zstd block decompression, regardless of actual content size (typically a few KB). The oversized-capacity `Vec` flows into `Arc<Vec<u8>>` and potentially into the block cache, which does not `shrink_to_fit()` on insert.
- **Why**: Hot-path allocation cost (get()/iteration decompression path) per false-positive-guide.md FP-9 ("block decode" is explicitly Hot). Asymmetric with the correctly-sized LZ4 path.
- **Suggested fix**: Prepend the true uncompressed size before the Zstd payload in `table_builder.rs`'s block-writing path (mirroring how LZ4 already does via `compress_prepend_size`), then have the reader use that validated (bounded against `MAX_DECOMPRESSED_BLOCK_SIZE`) real size as the `Vec::with_capacity` argument instead of the fixed constant.
- **Resolution**: Fixed differently from the suggested fix, avoiding an on-disk format change: `zstd::bulk::compress()` already embeds the true uncompressed content size in every frame by default (confirmed via the `zstd` crate's own test suite), and that size is readable through the **stable** (non-experimental) `zstd::zstd_safe::get_frame_content_size()` API — no Cargo feature change or wire-format change needed. The reader now calls this to right-size the `decompress()` capacity (still bounded against `MAX_DECOMPRESSED_BLOCK_SIZE` to guard against a corrupted/adversarial frame header), falling back to the old flat-64MB behavior only if a frame lacks an embedded size.

---

### [HIGH] iterator: `LevelIterator` lacks a zero-copy `next_lazy()` override, forcing an unnecessary value copy on every L1+ entry
- **Where**: `src/iterator/level_iter.rs` (`impl SeekableIterator for LevelIterator`, no `next_lazy` override despite having `next_into`); `src/iterator/source.rs` (default `next_lazy` trait impl, `advance_into_buffers`'s and `seek_to`'s Level arms); contrast `src/sst/table_reader/iterator.rs` (`TableIterator::next_lazy`, which returns zero-copy `LazyValue::BlockRef`)
- **What**: `SeekableIterator::next_lazy()` is documented as zero-copy for SST sources. `TableIterator` (used directly for L0) correctly honors this. `LevelIterator` (the only way L1+ levels enter merge sources, per `db.rs`'s `iter()`/`iter_with_range()`/`iter_with_prefix()`) never overrides `next_lazy()`, so it silently falls back to the default trait impl, which allocates a `Vec<u8>` and byte-copies via `next_into`. Since the bulk of data in any populated LSM-tree lives in L1+, every value read from there pays a heap allocation + memcpy it doesn't need.
- **Why**: Violates the documented zero-copy contract for SST-backed sources; hot-path performance regression affecting every full/range/prefix scan touching L1+.
- **Suggested fix**: Add a `next_lazy` override to `impl SeekableIterator for LevelIterator`, mirroring `next_into`'s existing file-transition loop but delegating to the child `TableIterator`'s `next_lazy` instead of `next_into`.
- **Resolution**: Fixed exactly as suggested — added a `next_lazy` override to `LevelIterator` mirroring `next_into`'s file-transition loop, delegating to the child `TableIterator::next_lazy` for zero-copy `LazyValue::BlockRef` values instead of `next_into`'s byte-copy.

---

### [HIGH] compaction: The parallel sub-compaction path (`execute_compaction_io` with `actual_subs > 1`) has zero test coverage anywhere in the repository
- **Where**: `src/compaction/leveled.rs` (`execute_compaction_io`'s `thread::scope` parallel path; all "sub_compaction"-named tests); `tests/integration.rs`
- **What**: All "sub_compaction"-named tests set `max_subcompactions: 4` but call `db.compact()`, which routes through `force_compact_all()` → `force_merge_level()` — a function that never reads `max_subcompactions` at all, so these tests exercise the always-serial merge loop, not the parallel one. The only other `max_subcompactions>1` usage in the repo does one put+flush with no compaction call. `build_sub_tasks`/`compute_split_points` boundary correctness (INV-C4), the atomic file-number counter under real contention, and per-thread panic/cleanup handling have never been exercised under genuine multi-threaded execution.
- **Why**: Matches technical-patterns.md 4.3 (Sub-Compaction Boundary Key Duplication) — a regression in the most concurrency-sensitive code in the highest-invariant-density file in the codebase would currently ship undetected.
- **Suggested fix**: Add a test that builds a `CompactionTask` with several target-level files and calls `execute_compaction_io` directly with `max_subcompactions > 1`, asserting `actual_subs > 1` was actually taken. Fix the existing "sub_compaction"-named tests to reach `execute_compaction_io` (e.g. via `compact_range(Some(a), Some(b))`, which does reach it) instead of `db.compact()`.
- **Resolution**: Fixed — added `test_execute_compaction_io_actually_parallelizes_with_max_subcompactions`, which hand-builds a `CompactionTask` with 3 disjoint target-level files (mirroring `test_discarded_trivial_move_preserves_live_input_file`'s pattern) and calls `execute_compaction_io` directly with `max_subcompactions: 4`. A new test-only `PARALLEL_SUB_COMPACTIONS_TAKEN` atomic counter, incremented only inside the `thread::scope` branch, directly proves the parallel path was genuinely taken (not just that serial-path output happened to be correct), in addition to verifying the merged output covers every input key exactly once.

---

### [HIGH] cross-cutting: Compaction pick/install phases can block the write-serializing lock on disk I/O for a lazily-cached range-tombstone block
- **Where**: `src/compaction/leveled.rs` (`file_overlaps_extent`, `is_bottommost_level`, `install_compaction`'s `outputs_overlap_unexpected_current_files`); `src/db.rs` (all 4 production pick-phase call sites and all 4 production install-phase call sites, all inside documented "short lock" blocks); `src/sst/table_reader/mod.rs` (`cached_range_tombstones`, a lazy `OnceLock` never eagerly warmed); `src/cache/table_cache.rs` (`prewarm`, which doesn't cover this)
- **What**: `file_overlaps_extent()` falls back to `tf.reader.get_range_tombstones()` (real disk I/O reading the range-del meta block) whenever a file `has_range_deletions` and cheap metadata-only overlap is inconclusive. This backs both the pick-phase `is_bottommost_level()` call and the install-phase stale-output check, both of which run while `self.inner`/`bg_inner` (the same lock `write_batch_group` needs for every write) is held. `TableReader`'s range-tombstone cache is populated lazily on first access; `TableCache::prewarm()` only covers footer/index/filter parsing for new files, never this tombstone cache nor existing current-version files. First access for any file not recently read (cold restart, deep levels rarely touched by point reads, evicted TableReader) is a genuine blocking read while the write lock is held.
- **Why**: Violates the documented INV-CC4 4-phase pattern (pick/install phases must be metadata-only "short lock" phases, with I/O confined to the unlocked phase).
- **Suggested fix**: Move the `is_bottommost_level(...)` call in each db.rs picking site to just after lock release (the captured `Version`/inputs don't require the lock to evaluate). Have `TableReader::open_with_all` eagerly load the range-tombstone block whenever `meta.has_range_deletions` is true, exactly as index/filter blocks are already eagerly parsed at open time — this removes the lazy-first-access gap for both call chains.
- **Resolution**: Fixed via the second (broader) suggested fix: `TableReader::open_with_all` now eagerly calls `cached_range_tombstones()` right after construction whenever the file's parsed `range_del_handle` is present (a reliable proxy for "has range deletions via the cheap dedicated-block path" for any file written by current code — `write_range_del_block` only produces a non-default handle in that case). This removes the lazy-first-access gap at its root for both the pick-phase and install-phase call chains, without needing to touch `db.rs`'s lock structure at 8 call sites. The rare legacy fallback path (old-format files with no dedicated range-del block) is intentionally left lazy, since eagerly scanning every data block would regress `open()` for old files with zero range deletions.

---

### [HIGH] cross-cutting: `compact_range()` defers its SuperVersion refresh across an entire multi-level cascade, keeping every already-unlinked input SST's file handle alive
- **Where**: `src/db.rs` (`compact_range`'s loop — `install_super_version` called only once, after the loop, while `run_post_compaction_cleanup` unlinks old SSTs per-iteration inside the loop); contrast `drain_l0`, `force_compact_all`, and the background compaction loops in the same file, which all refresh per-iteration/per-step
- **What**: `compact_range(begin, end)` with an explicit range cascades level-by-level (normal LSM behavior). Each iteration installs its `VersionEdit` then immediately deletes the old input SSTs from disk — but `install_super_version()`, which republishes the reader-visible `ArcSwap<SuperVersion>`, runs only once, after the entire loop. Until then, the live SuperVersion still points at the original pre-loop `Arc<Version>`, whose file list holds `Arc<TableReader>` (open fds) for every file already unlinked across every level the cascade touched. POSIX unlink-of-open-fd is safe for correctness, but none of those files' disk space is reclaimed until the single final refresh.
- **Why**: Violates the resource-lifecycle expectation demonstrated by 3 correct sibling call sites in the same file — a superseded SST's `Arc<TableReader>` (and its disk space) should be released promptly per compaction step. In the realistic use case (reclaiming space after a bulk delete over a key range), the operation invoked specifically to free disk space can instead transiently double usage for its full duration.
- **Suggested fix**: Call `self.install_super_version(&inner)` immediately after each iteration's `install_compaction()` call inside `compact_range`'s loop (mirror `drain_l0`'s pattern). No new locking needed and no concurrency risk introduced.
- **Resolution**: Fixed exactly as suggested — `compact_range`'s loop now calls `self.l0_file_count.store(...)` and `self.install_super_version(&inner)` inside the same per-iteration lock block as `install_compaction`, immediately before the lock is dropped, mirroring `drain_l0`'s placement. The redundant post-loop refresh (which only ran once, after the whole cascade) was removed.

---

### [HIGH] iterator: Backward iteration ignores cross-level range-tombstone pruning, hiding live keys that forward iteration shows correctly
- **Where**: `src/iterator/db_iter.rs` (`is_range_deleted_no_level_filter`, called from `resolve_prev_user_key`, reachable from `prev()`, `seek_for_prev()`, `seek_to_last()`, and `BidiIterator::next_back()`); `src/iterator/merge.rs` (`prev_entry`, `peek_source_level`)
- **What**: Forward iteration (`next_visible`) correctly computes the winning entry's source level via `peek_source_level()` (called before the entry is popped) and passes it to the tombstone-coverage check, so a tombstone strictly deeper than the key's level is correctly excluded from coverage — this is the exact mechanism that guards bottommost sequence-zeroed keys (see `test_db_iterator_deeper_level_tombstone_does_not_cover_key`). Backward iteration instead always calls `is_range_deleted_no_level_filter()`, which hardcodes `level_filter=None`, so any tombstone with `seq > key_seq` is treated as covering, regardless of level. Verified directly: `resolve_prev_user_key`'s collection loop calls `self.merger.prev_entry()` repeatedly but never calls `peek_source_level()` at all, so a level-1 key with a level-2 tombstone (the exact scenario the forward-path regression test proves must NOT hide the key) is incorrectly hidden when reached via `seek_to_last()`/`prev()`. No existing test exercises this cross-level scenario for backward iteration (`test_db_iterator_prev_with_tombstones` only covers same-level point deletions).
- **Why**: Violates INV-I3 (range tombstone coverage must use correct level-aware seq comparison for every yielded key) and breaks direction-switch consistency (the same snapshot must yield the same visible key set regardless of scan direction). The analogous forward-path defect was previously fixed, but that fix never touched the backward code path, which uses a structurally separate implementation.
- **Suggested fix**: In `resolve_prev_user_key`'s collection loop, call `self.merger.peek_source_level()` immediately before each `self.merger.prev_entry()` call (including the `prev_overshoot`-consuming first call), track it as `best_level` alongside `best_seq`/`best_entry` (stashing it with `prev_overshoot` for the carried-over entry across loop iterations), then replace the `is_range_deleted_no_level_filter` call with the level-aware check, matching the forward path exactly. Add a backward-direction counterpart to `test_db_iterator_deeper_level_tombstone_does_not_cover_key`.
- **Resolution**: Fixed exactly as suggested — added `prev_entry_with_level()` calling `peek_source_level()` immediately before `prev_entry()`; `resolve_prev_user_key` now tracks `best_level` (and threads it through `prev_overshoot`, whose type gained a `usize` level field) and uses the level-aware tombstone check matching the forward path. `is_range_deleted_no_level_filter` was removed (zero remaining callers). Added 3 new tests including a backward-direction counterpart to the forward-path regression test.

---

### [MEDIUM] memtable: `MemTableCursorIter::prev()` silently wraps around to the last entry when called again after backward exhaustion
- **Where**: `src/memtable/skiplist.rs` (`prev()`, contrast `next()`)
- **What**: `prev()` overloads a single sentinel (`self.cursor.is_null()`) to mean both "freshly past-the-end, step back to the last entry" and "already exhausted backward, nothing precedes the current position." When backward exhaustion occurs, `prev()` sets `self.cursor = null` and returns `None` — but the *next* call to `prev()` re-enters the same branch and unconditionally jumps back to the last entry instead of staying exhausted. Not currently reachable via any production caller (masked by `MergingIterator::prev_entry()`'s own state tracking in a different module), but there is zero direct unit test coverage for `prev()`/`seek_for_prev()`/`current()` in `skiplist.rs`'s own test module.
- **Why**: Violates the documented contract of `SeekableIterator::prev()` ("returns None if we've moved before the first entry") — once exhausted, repeated calls are conventionally expected to keep returning `None`. The masking is caller-side happenstance in a different module, not a structural guarantee.
- **Suggested fix**: Track forward/backward exhaustion as distinct cursor state instead of overloading one null sentinel — e.g. add an `exhausted_backward: bool` flag or a 3-state enum. Set it in `prev()`'s "no predecessor found" branch; clear it in every method that repositions the cursor by other means. Add direct unit tests for `prev()`/`seek_for_prev()`/`current()`, including the double-exhaustion case.
- **Resolution**: Fixed exactly as suggested — added an `exhausted_backward: bool` field, set in `prev()`'s no-predecessor branch and cleared by every repositioning method (`next()`/`next_into()`/`next_lazy()` also now correctly resume forward from the first entry instead of staying stuck). Added 4 new direct unit tests including the double-exhaustion regression.

---

### [MEDIUM] compaction: `task_has_range_deletions` unconditionally forces single-threaded compaction whenever any input file has range deletions, making INV-C2a's cross-sub-task sharing unreachable
- **Where**: `src/compaction/leveled.rs` (the `max_subs` gate: `task.level == 0 || task_has_range_deletions(task)`; `task_has_range_deletions`; the `RangeTombstoneTracker` pre-population/sharing machinery this disables)
- **What**: The gate is task-wide, not per-sub-task — confirmed only one such check exists in the file. Since real parallel execution (`actual_subs > 1`) can only occur when the file set has zero range deletions, INV-C2a's pre-population/sharing design (explicitly built to propagate a tombstone across multiple concurrent sub-compactions) can never actually run with more than one sub-task. Because tombstones are retained (by design) until they reach the bottommost level, a single `delete_range` can force every subsequent compaction that touches that data back to single-threaded for a potentially long time. Tracing `build_sub_tasks` shows it doesn't need to be tombstone-extent-aware for parallelism to work correctly, since tombstone coverage is handled entirely through the separate shared collections — lifting this restriction would not require new machinery.
- **Why**: Contradicts INV-C2a's review checklist item ("range tombstones propagated to all sub-compactions") — the code builds a capability the gate prevents from ever being exercised in its intended multi-way role.
- **Suggested fix**: Remove the `task_has_range_deletions(task)` clause and rely on the existing pre-population/sharing machinery, validating with the new multi-threaded test recommended above. If there is an undocumented correctness reason for the restriction, add a rationale comment instead.
- **Resolution**: Fixed exactly as suggested — removed the `task_has_range_deletions(task)` clause from the `max_subs` gate (verified safe: `all_range_del_entries`/`all_raw_tombstones` are collected once from all input files and shared by reference with every sub-task regardless of `actual_subs`, so each sub-compaction already had complete tombstone visibility independent of this gate). The now-dead `task_has_range_deletions` function was removed.

---

### [MEDIUM] manifest: `write_current_file_tmp`'s `CURRENT.tmp` reuses the same path/inode across MANIFEST-rotation retries
- **Where**: `src/manifest/version_set.rs` (`write_current_file_tmp`, its call site in `maybe_compact_manifest`; contrast `new_manifest_number`'s fresh-every-call allocation, and `rename_current_file`'s failure branch, which DOES clean up `CURRENT.tmp`)
- **What**: `write_current_file_tmp` always targets the same hard-coded `db_path.join("CURRENT.tmp")` on every retry; `fs::File::create` truncates the existing inode rather than allocating a new one. Unlike `rename_current_file`'s failure branch, `write_current_file_tmp`'s own failure branch does NOT remove `CURRENT.tmp` on error, so a partially-written `CURRENT.tmp` from a failed attempt is left in place and reused (truncated in place) by the next retry. If `sync_all()` fails once then "succeeds" spuriously on the very next retry on the same inode, the rename and post-rename directory fsync can both succeed while CURRENT's on-disk bytes were never actually flushed. `recover_with_cache` trusts CURRENT's content verbatim with no checksum.
- **Why**: Same fsyncgate class the file already poisons for elsewhere.
- **Suggested fix**: Write CURRENT via a per-attempt unique temp name (e.g. `CURRENT.tmp.{new_manifest_number}`) so retries never reuse an inode, mirroring the new-manifest-file pattern. Additionally remove `CURRENT.tmp` in the failure branch for cleanliness regardless.
- **Resolution**: Fixed exactly as suggested — `write_current_file_tmp`/`rename_current_file` now build the temp path as `CURRENT.tmp.{manifest_number}` (using the already-allocated manifest number), so retries never reuse an inode. The `write_current_file_tmp` failure branch in `maybe_compact_manifest` now also removes the temp file, matching `rename_current_file`'s existing cleanup pattern.

---

### [MEDIUM] cache: `invalidate_file()` can miss an offset inserted concurrently, leaving a stale block live past its file's removal
- **Where**: `src/cache/block_cache.rs` (`insert`, `take_file`, `invalidate_file`)
- **What**: `insert()` writes into the shared moka store first and only afterward records the offset in the reverse index — not atomic as a pair. `invalidate_file()` reads-and-clears that same index via `take_file()` then invalidates only the offsets it got back. If a concurrent reader's `insert()` for file F lands its moka write before `invalidate_file(F)`'s `take_file()` runs, but its `index.add()` lands after, `take_file()` returns a snapshot missing that offset — `invalidate_file` never targets it, and the block stays live in moka indefinitely past the file's removal, reclaimed only by incidental LRU pressure.
- **Why**: A narrower cousin of technical-patterns.md 3.3 (Block Cache Memory Pressure), for the unpinned LRU path rather than pinned blocks (growth stays capacity-bounded, not unbounded). Impact is bounded since file numbers are never reused — the stale entry is never wrong data, only delayed reclaim.
- **Suggested fix**: Document as an accepted, bounded tradeoff alongside the `detached` field's existing comment. A full fix would need insert's moka-write + index-add to be atomic per (member,file), reintroducing the per-insert contention `FO_SHARDS` was designed to avoid.
- **Resolution**: Fixed exactly as suggested (documentation-only, no logic change) — added doc comments on `insert()` and `invalidate_file()` explaining the race, why it's bounded (file numbers never reused), and why a full atomicity fix is rejected (would reintroduce the contention `FO_SHARDS` avoids), alongside the `detached` field's existing similar-tradeoff comment.

---

### [MEDIUM] types-encoding: `RateLimiter` debt accounting is unsound under concurrent `request()` callers, letting aggregate throughput exceed the configured rate by ~N×
- **Where**: `src/rate_limiter.rs` (`RateLimiterInner.available`, `next_chunk`, `request`); `src/compaction/leveled.rs` (sub-compaction threads sharing one `Arc<RateLimiter>`)
- **What**: `available` (f64) has no floor and `next_chunk`'s `wait_secs` is capped at `MAX_SLEEP_SECS` (60s) based only on the calling thread's own read of `available`. The lock is dropped before the sleep, so a second concurrent thread can lock, read the still-unpaid negative `available` left by the first, and compute its own independently-capped wait against an already-deep deficit. Simulation confirms a stable (not runaway) equilibrium at ~N× the configured rate for N concurrent threads (~2.0x/4.0x/7.95x for N=2/4/8). Reachability requires `rate_limiter_bytes_per_sec > 0` (default 0, disabled), `max_subcompactions > 1` (default 1), and large entry sizes — all non-default, with no existing test combining the two settings.
- **Why**: `available` underflows without bound under concurrency; the per-call sleep cap silently discards real debt instead of deferring it correctly.
- **Suggested fix**: Not a one-line patch — a naive floor clamp doesn't resolve the overshoot. Needs either (a) a thread that hits the cap looping additional ≤MAX_SLEEP_SECS sleeps until its own chunk's true deficit is fully paid before returning, or (b) serializing admission via a FIFO ticket (compute each request's start/end time analytically while holding the lock, sleep to that computed time outside it), so concurrent requesters' waits compose additively instead of independently racing the same counter. Add a multi-threaded regression test once fixed.
- **Resolution**: Fixed via option (a) — a thread whose chunk needs more than `MAX_SLEEP_SECS` now loops additional capped sleeps (tracked via a purely local `remaining_wait` variable, never re-read from the shared `available` counter) until its own chunk's true, uncapped deficit is fully repaid, instead of a single capped sleep that silently forgave the excess. Added a multi-threaded regression test (`test_rate_limiter_concurrent_requests_converge_to_configured_rate`) that fails against the pre-fix code (~2.7-2.85x) and passes reliably against the fix.

---

### [MEDIUM] style: `iter_with_prefix()` doc comment misstates bloom filter granularity as per-block when it is file-level
- **Where**: `src/db.rs` (`iter_with_prefix` doc comment); contrast `src/options.rs` (`prefix_len` doc, correctly file-level), `src/sst/table_reader/mod.rs` (`prefix_may_match`, whose own doc correctly says "per file"), `src/sst/table_builder.rs` (prefix filter construction)
- **What**: The doc comment states pruning works via "per-block bloom filters." In reality, the prefix bloom filter is built once per SST file — `TableBuilder` accumulates a single `prefix_set` across every entry in the whole file and serializes it as one `BloomFilter` under one "filter.prefix" metaindex entry per file. There is no per-block bloom check anywhere in the SST format (the regular whole-key bloom filter is likewise built file-wide). This phrase appears nowhere else in the codebase — an isolated inaccuracy.
- **Why**: Doc-code alignment on a public API (`DB` is publicly re-exported) — misleading documentation about pruning granularity could lead API consumers to incorrect assumptions about per-block vs per-file selectivity/performance characteristics.
- **Suggested fix**: Change "per-block bloom filters" to "a per-file bloom filter" (or "whole-file bloom filter"), matching the accurate wording already used in `options.rs`.
- **Resolution**: Fixed exactly as suggested — `db.rs`'s doc comment now says "whose file-level bloom filter reports that `prefix` is absent."

---

### [LOW] types-encoding: `ReadOptions.snapshot` typed `Option<u64>` instead of `Option<SequenceNumber>`
- **Where**: `src/options.rs` (`ReadOptions.snapshot`); contrast `src/db.rs` (`Snapshot::sequence() -> SequenceNumber`)
- **What**: `types::SequenceNumber` (`pub type SequenceNumber = u64`, re-exported from `lib.rs`) is the crate's semantic name for sequence numbers, used as the return type of `Snapshot::sequence()`. But `ReadOptions.snapshot`, whose doc comment explicitly couples it to `Snapshot::sequence()`'s output, is typed as a raw `Option<u64>`. Confirmed isolated — no other `u64` field in `options.rs` represents a sequence number.
- **Why**: Naming-consistency / forward-compatibility self-documentation gap. Zero functional impact today (plain alias, identical layout).
- **Suggested fix**: Change `pub snapshot: Option<u64>` to `pub snapshot: Option<SequenceNumber>` (source-compatible, no call-site changes required).
- **Resolution**: Fixed exactly as suggested.

---

### [LOW] cache: `pin_l0_filter_and_index_blocks_in_cache` doc contradicts its actual behavior
- **Where**: `src/options.rs` (doc + field); `src/sst/table_reader/mod.rs` (`pin_metadata_in_cache`, actual impl; `TableReader`'s `index_block`/`filter_data`/`prefix_filter_data` fields); `src/cache/block_cache.rs` (`insert_pinned`'s own accurate doc)
- **What**: The option's doc says "Pin L0 index and filter blocks in block cache (never evict)," mirroring RocksDB's option of the same name. The actual implementation never puts index/filter blocks in `block_cache` — they are unconditional direct `TableReader` fields, always resident regardless of this flag's value. What the flag actually gates: eagerly warming the index-entries cache, and pinning only the file's first *data* block via `insert_pinned`.
- **Why**: Doc-code alignment convention — same class/severity as the already-Resolved "block-cache-usage" finding. An operator disabling this flag to reduce index/filter memory would get zero savings there while unknowingly losing first-block pinning and eager warm-up.
- **Suggested fix**: Reword the option's doc to describe actual behavior.
- **Resolution**: Fixed exactly as suggested — reworded to state the flag eagerly warms the index-entry cache and pins each new L0 file's first data block, and that index/filter blocks are never stored in `block_cache` regardless of this setting.

---

### [LOW] style: Repeated inline `std::fs::OpenOptions` path in `wal/reader.rs` test code instead of an import
- **Where**: `src/wal/reader.rs` (test module, 3 occurrences of inline `std::fs::OpenOptions::new()`)
- **What**: Three separate test functions each write `std::fs::OpenOptions::new()...` fully inline rather than importing `OpenOptions` once. Codebase precedent goes the other way: `wal/writer.rs` and `db.rs` both import once at top-level and use bare `OpenOptions::new()`; even test code elsewhere (`sst/table_reader/iterator.rs`) has a scoped `use std::fs::OpenOptions;` inside its test module.
- **Why**: CLAUDE.md convention "Prefer imports over inline paths" (3+ uses of same path in a file).
- **Suggested fix**: Add `use std::fs::OpenOptions;` to the `mod tests` imports and use bare `OpenOptions::new()` at all three call sites.
- **Resolution**: Fixed exactly as suggested.

---

### [LOW] cache: `get_property("block-cache-usage")` doc doesn't caveat pool-wide aggregation under a shared `BlockCachePool`
- **Where**: `src/db.rs` (doc comment on `get_property`, and the `"block-cache-usage"` match arm); `src/cache/block_cache.rs` (`BlockCache::entry_count`)
- **What**: `BlockCache::entry_count()` returns `self.pool.entry_count() + self.pinned.lock().len()`, where `pool.entry_count()` is the whole `BlockCachePool`'s moka entry count summed across every attached member — not just the calling DB's own share. `DB::get_property("block-cache-usage")` forwards this value verbatim, but its doc comment ("approximate block cache entry count") was not updated when `DbOptions::block_cache` pool-sharing was introduced, and doesn't mention that an idle DB sharing a pool with a busy DB will report (almost) the busy DB's entry count as its own. Empirically confirmed during review: two DBs sharing one pool, only one written/read — the idle DB's `get_property("block-cache-usage")` reports nearly the same non-zero number as the active one.
- **Why**: Doc-code alignment (public API changes must update corresponding docs) and API-contract stability (observable behavior changes for adopters of the new opt-in sharing feature). Severity is LOW, not MEDIUM: the feature is opt-in (`None` default is byte-for-byte unchanged from pre-commit behavior), the lower-level `BlockCache`/`BlockCachePool::entry_count()` and `DbOptions::block_cache` doc comments already accurately describe the pool-wide/shared-capacity semantics, and the property is purely observational (no internal control-flow depends on it) — per this project's own severity rubric, stale/incomplete documentation is a LOW-severity finding.
- **Suggested fix**: Add a caveat clause to the `get_property` doc list entry, e.g. "approximate block cache entry count (pool-wide total when `DbOptions::block_cache` is a shared pool — see `BlockCachePool`)".
- **Resolution**: Fixed — the `get_property` doc entry now states the count is the pool-wide LRU total across every attached member (plus this DB's own pinned entries) when a shared `BlockCachePool` is attached.

---

### [LOW] cache: un-merged `std::sync` imports in `block_cache.rs`
- **Where**: `src/cache/block_cache.rs` (top-of-file imports)
- **What**: `use std::sync::Arc;` and `use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};` are two separate `use` statements sharing the `std::sync::` prefix. The commit that introduced the shared-pool feature modified the second line (adding `AtomicBool`/`AtomicU64`) without merging it with the adjacent, unchanged `Arc` import.
- **Why**: Violates the project's grouped-imports convention (CLAUDE.md: "merge common prefixes: `use std::sync::{Arc, Mutex};`"). `src/compaction/leveled.rs` is the exact precedent in this codebase for the merged form of this same `Arc` + `atomic::{...}` combination. `block_cache.rs` is the only file in `src/` with two separate top-level `std::sync::` import lines.
- **Suggested fix**:
  ```rust
  use std::sync::{
      Arc,
      atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
  };
  ```
- **Resolution**: Fixed — all `std::` imports in `block_cache.rs` merged into one grouped block (`collections` + `sync::{Arc, atomic::{...}}`), matching the `compaction/leveled.rs` precedent.

---

## Won't Fix

### [LOW] rate_limiter: `request()` f64 subtraction is a no-op for values ≳ 2.5e17, theoretical infinite loop
- **Where**: `src/rate_limiter.rs` (`request`, the `remaining -= chunk` convergence loop)
- **What**: For a single request of ~hundreds of petabytes, `chunk` can fall below `ULP(remaining)/2`, making the f64 subtraction a no-op and the loop non-terminating. Mathematically confirmed.
- **Reason**: Not practically reachable. Every call site passes single-entry sizes (internal key + value bytes, bounded by allocatable memory); triggering the loop requires first materializing a ~2.5e17-byte entry in RAM. The module is private, so no external caller can inject an arbitrary value. Guarding would add cost/complexity to a hot path for an input that cannot occur. Revisit if `request()` is ever exposed to sizes not backed by in-memory buffers.

---

### [REJECTED] db: Dead-key sweep `?` on `force_merge_level` error "permanently kills the background compaction thread"
- **Where**: `src/db.rs` (dead-key sweep block in the background compaction loop)
- **What**: External review claimed a transient sweep error breaks the thread's main loop, silently and permanently disabling all future compaction.
- **Reason**: Rejected — not a bug. The error path calls `set_error`, which sets `has_bg_error`; from then on **every** public entry point fails loudly via `check_usable()`. This is the engine's deliberate fail-stop-until-reopen policy, identical to every other background error path (debt-driven picks, hint compactions, flush). Nothing is silent, and "no further compaction" is exactly the intended fail-stop behavior.

---

### [REJECTED] db: Dead-key sweep's L0 pass "never fires the filter because L0 is never bottommost"
- **Where**: `src/db.rs` (sweep loop starting at level 0), `src/compaction/leveled.rs` (`is_bottommost_level`)
- **What**: External review claimed `is_bottommost` is always false for L0 when `num_levels > 1`, making the L0 sweep pass useless.
- **Reason**: Rejected — factually wrong. `is_bottommost_level` is data-driven, not level-count-driven: it checks whether any *deeper level contains files overlapping the inputs*. On a settled store whose data sits only in L0 (the exact scenario the sweep exists for), it returns true and the filter fires at L0. When deeper overlapping data exists, skipping the filter at L0 is required for MVCC correctness (a newer L0 tombstone must not be filtered away below an older value). Residual single-file no-op rewrites are separately eliminated by the `force_merge_level` early-return fix.

---

### [LOW] compaction: Near-duplicate merge-loop logic between `execute_sub_compaction_io` and `force_merge_level`
- **Where**: `src/compaction/leveled.rs` — the tombstone/sequence/snapshot-dedup/filter merge logic (~150 lines) is duplicated nearly verbatim between the two functions.
- **What**: Both implementations were independently verified correct and mutually consistent (same snapshot-retention algorithm, same sequence-zeroing condition, same filter gating). No functional bug today.
- **Reason**: Merging two independently-verified, invariant-critical ~150-line blocks (INV-C2, INV-C2a, INV-C3, INV-C6 all converge here) for a maintainability-only, non-functional finding carries disproportionate regression risk relative to its severity. Revisit if a correctness fix is ever needed in one of the two functions — apply the same fix to both, independently re-verified, at that time.

---

### [LOW] SST: Unvalidated `BlockHandle` offset/size flow into readahead arithmetic and the `posix_fadvise` unsafe call
- **Where**: `src/sst/table_reader/iterator.rs` (`maybe_readahead`, `prefetch_first_block`); consumed by `src/sst/table_reader/mod.rs` (`advise_willneed`)
- **What**: `maybe_readahead()` computes `len` via unchecked `u64` addition on index-derived `BlockHandle` values before the normal bounds check (which only runs when the block is actually read later). The result is cast `as i64` for the FFI call, which could go negative on a corrupted index, contradicting the SAFETY comment's claim.
- **Reason**: `posix_fadvise` is a pure advisory hint — its return value is discarded, and a nonsensical offset/length cannot cause memory unsafety; the real bounds check still gates actual reads. Only concrete downside is a possible debug-build overflow panic on a corrupted index block. Low enough risk/impact to defer; revisit if the readahead logic is touched for other reasons.

---

### [LOW] SST: `Block::new()` restart-count validation can overflow `usize` on 32-bit targets
- **Where**: `src/sst/block.rs` (restart-count validation in `Block::new()`)
- **What**: `(num_restarts as usize) * 4 + 4` can overflow a 32-bit `usize` for large corrupted `num_restarts` values, potentially causing an out-of-bounds panic instead of a clean `Error::corruption`.
- **Reason**: CI only targets `ubuntu-latest` (64-bit x86_64) and `Cargo.toml` has no 32-bit target support; on any 64-bit target this cannot overflow for any `u32` value. Purely a latent portability gap on unsupported targets — revisit if 32-bit target support is ever added.

---

### [MEDIUM] iterator: `MergingIterator` seek path (`seek`/`seek_to_first`/`seek_for_prev`) still defeats cross-source I/O-overlap prefetch
- **Where**: `src/iterator/merge.rs` (`seek`, `seek_to_first`, `seek_for_prev`), `src/iterator/source.rs` (`seek_to`/`seek_for_prev_to`/`seek_to_first_impl`, each combining position+decode into one synchronous call)
- **What**: Every seek-based entry point (the mechanism `db.rs`'s `iter_with_prefix()` uses internally, and the pattern most of the test suite exercises via `iter.seek(...)`) positions each source sequentially and synchronously decodes its entry before `init_heap()`'s prefetch phase runs, eliminating I/O-overlap for the hot seek path. `LevelIterator::prefetch_first_block()` has been added (fixing the cold-start/no-seek case), but the seek path itself is unchanged.
- **Reason**: `SeekableIterator`'s trait contract (in `src/iterator/source.rs`) has no split "position-only" phase — every seek implementation combines positioning and decoding into one call. Cleanly separating these would require changing the trait's method signatures/semantics across every implementor (`TableIterator`, `LevelIterator`, `MemTableCursorIter`, boxed/Vec sources), a cross-cutting, moderately invasive change with real regression risk to defer for a dedicated, focused effort rather than bundle into this audit cycle. The immediate, common-case win (cold-start prefetch for `LevelIterator`) has been captured; this residual gap only affects warm I/O-overlap opportunity, not correctness.

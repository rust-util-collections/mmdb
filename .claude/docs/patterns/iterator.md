# Iterator Subsystem Review Patterns

## Files
- `src/iterator/db_iter.rs` (~49KB) — user-facing DBIterator
- `src/iterator/merge.rs` (~25KB) — MergingIterator (heap-based)
- `src/iterator/source.rs` (~20KB) — IterSource wrapper + SeekableIterator trait (extracted from merge.rs)
- `src/iterator/level_iter.rs` (~25KB) — lazy two-level iterator for L1+
- `src/iterator/bidi_iter.rs` (~16KB) — bidirectional support
- `src/iterator/range_del.rs` (~19KB) — range tombstone tracking

## Architecture
- DBIterator: deduplication, snapshot filtering, tombstone filtering, prefix bounds
- MergingIterator: min-heap merge of K sources with single-source fast path
- IterSource: uniform peeked-entry adapter over memtable/SST/level/boxed sources (lazy values)
- LevelIterator: deferred block reads, binary search on level
- BidiIterator: direction switch with heap rebuild
- FragmentedRangeTombstoneList: tombstones pre-collected at iterator creation, fragmented into non-overlapping intervals; immutable, O(log T) binary search per key (range_del.rs also keeps the sweep-line `RangeTombstoneTracker` used by compaction)

## Critical Invariants

### INV-I1: Forward Monotonicity
`next()` must always advance the user key. If the same user key appears in multiple sources (different sequence numbers), only the latest visible version is yielded.
**Check**: Verify skip-same-user-key logic in DBIterator after MergingIterator::next().

### INV-I2: No Missing Keys
Every key in the valid range that is visible at the snapshot's sequence number must be yielded exactly once.
**Check**: Verify that all sources (active MemTable, immutable MemTables, L0 files, L1+ levels) are included in the MergingIterator.

### INV-I3: Range Tombstone Coverage
If a range tombstone `[start, end)` with seq S covers a key K with seq S' < S, the key must be filtered out.
**Check**: Verify `FragmentedRangeTombstoneList` is consulted for every key yielded by MergingIterator, and the check uses the correct sequence comparison. For the DBIterator path, every yielded key is checked against the pre-fragmented tombstone list via O(log T) binary search.

### INV-I4: Direction Switch Correctness
Switching from forward to backward (or vice versa) must not skip or duplicate any keys.
**Check**: Verify all child iterators in the heap are repositioned correctly on direction switch. The current key should be yielded again if it hasn't been consumed.

### INV-I5: Prefix Bound Termination
`iter_with_prefix()` must stop at the exact prefix boundary, not one key before or after.
**Check**: Verify prefix comparison is `key[..prefix_len] == prefix`, not `key <= prefix_end`.

### INV-I6: Snapshot Sequence Filtering
For each user key, the iterator must return the LATEST version with `seq <= snapshot_seq`, skipping all versions with `seq > snapshot_seq`.
**Check**: Verify the version scan loop doesn't stop at the first version — it must find the LATEST version that is still <= snapshot_seq.

## Common Bug Patterns

### Heap Corruption After Seek (technical-patterns.md 5.1)
After `seek()`, a child iterator's position changes but the heap is not rebuilt.
**Trigger**: seek() positions one child, but other children retain stale positions.

### Double Yield After Direction Switch
Switching from `next()` to `prev()` yields the current key twice.
**Check**: Verify the direction switch logic accounts for whether the current key has been consumed.

### Range Tombstone Not Checked on Seek
`seek(target)` lands on a key covered by a range tombstone, but the covering-tombstone check is skipped for the new position.
**Check**: Verify every key yielded after seek() is still checked against the FragmentedRangeTombstoneList (the list is immutable and stateless — the risk is a code path that bypasses the per-key query, not stale tracker state).

### LevelIterator File Boundary Skip
When crossing from one SST file to the next in a level, a key at the exact boundary is skipped.
**Check**: Verify LevelIterator's file transition logic — the first key of the new file must be yielded.

## Key Optimizations (not documented individually above)

These patterns exist in the codebase and should be checked during review:

- **`next_into()` buffer reuse**: Copies entries directly into caller-provided buffers — avoids per-entry heap allocation. Verify no aliasing with live MemTable/SST data.
- **Prefetch-based init_heap I/O**: `MergingIterator` issues `posix_fadvise(WILLNEED)` prefetch hints for all seekable sources before draining them during heap initialization (overlaps I/O across sources).
- **SetBounds propagation**: `ReadOptions` bounds are propagated to `LevelIterator` and `TableIterator` sub-iterators for early termination.
- **SkipPoint callback**: `ReadOptions.skip_point` filter allows callers to skip entries without consulting the value.
- **LevelIterator file skip by bound**: Files whose smallest user key is `>= upper_bound` are skipped entirely; the bound is also pushed into each opened `TableIterator`.
- **Cross-level tombstone pruning**: A tombstone from level L may only delete keys from levels deeper than L (`level >= source_level` tombstones are ignored). `FragmentedRangeTombstoneList` tracks per-level tombstone origin to enforce this — deeper-or-same-level tombstones must never suppress shallower keys.
- **`posix_fadvise` sequential readahead**: Detects sequential block access patterns and issues `WILLNEED` hints to the OS page cache.
- **Deferred block read**: SST index stores `first_key` per block; seek positions the iterator without reading data blocks until `next()` is called.
- **L0 metadata pinning**: Index and bloom filter blocks for L0 files are pinned in cache via `insert_pinned()` and unpinned when the file is compacted.
- **Atomic L0 counter**: Write-throttle checks use an atomic counter for L0 file count, avoiding mutex contention on the hot write path.

## Review Checklist
- [ ] MergingIterator heap invariant maintained after every next()/prev()/seek()
- [ ] DBIterator skips same-user-key entries with higher sequence numbers correctly
- [ ] All sources included: active MemTable + immutable MemTables + L0 files + L1+ levels
- [ ] FragmentedRangeTombstoneList consulted for every yielded key
- [ ] Direction switch rebuilds heap and repositions all children
- [ ] Prefix bound check is byte-exact, not lexicographic over-bound
- [ ] Seek handles the case where target key is covered by a range tombstone
- [ ] Iterator holds Arc references to prevent SuperVersion/MemTable/SST cleanup during iteration
- [ ] `next_into()` buffer reuse doesn't alias with live data
- [ ] SetBounds propagated correctly to LevelIterator and TableIterator sub-iterators
- [ ] SkipPoint callback filter applied without advancing past matching entries
- [ ] Prefetch hints (fadvise) issued at correct sequential-access detection points
- [ ] L0 pinned blocks unpinned after compaction deletes the L0 file

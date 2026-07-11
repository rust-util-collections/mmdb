# MemTable Subsystem Review Patterns

## Files
- `src/memtable/mod.rs` — MemTable interface
- `src/memtable/skiplist.rs` — SkipList wrapper
- `src/memtable/skiplist_impl.rs` — lock-free skiplist implementation

## Architecture
- Lock-free skiplist: single-writer, concurrent multi-reader
- OrdInternalKey wrapper for comparison (user_key ASC, seq DESC)
- Approximate size tracking per insert
- Separate range tombstone collection
- Arena-style allocation for skiplist nodes
- `MemTableCursorIter`: raw pointer-based level-0 chain iterator for O(1) sequential access

## Critical Invariants

### INV-M1: Single-Writer Guarantee
Only one thread may insert into the active MemTable at a time. The group commit leader serializes writes.
**Check**: Normal active-MemTable writes occur under DB write serialization.
Recovery and test builders may call `put()` without that lock only while the
MemTable is local and unpublished; no two writers may call it concurrently.

### INV-M2: Concurrent Read Safety
Readers must see a consistent view even while a writer is inserting.
**Check**: Verify skiplist uses atomic pointer stores with appropriate memory ordering (Release for writes, Acquire for reads).

### INV-M3: Key Ordering
The skiplist must maintain `(user_key ASC, sequence_number DESC)` ordering. This ensures that for a given user key, the latest version appears first.
**Check**: Verify OrdInternalKey comparator: compare user_key bytes first, then compare sequence number in REVERSE order.

### INV-M4: Size Tracking Accuracy
`MemTable::put()` must update the atomic size estimate for every stored
representation. Severe undercounting delays flush and can cause OOM. The
current calculation includes encoded key/value bytes, approximate skiplist-node
overhead, and the duplicated range-tombstone collection entry.

**Check**: Verify all `ValueType` branches (`Value`, `Deletion`,
`RangeDeletion`) flow through the size calculation and that new duplicated
storage is included. `approximate_size()` itself is only the atomic read.

### INV-M5: Range Tombstone Isolation
Range tombstones stored in the MemTable must be accessible independently from point entries, for the RangeTombstoneTracker.
**Check**: Verify range tombstones go into the dedicated collection AND are queryable via a separate iterator.

## Common Bug Patterns

### Memory Ordering Bug (technical-patterns.md 6.1)
Skiplist node's next pointer is written with `Relaxed` ordering, causing a reader on another core to see the pointer before the node's key/value data is visible.
**Check**: Verify `store(..., Ordering::Release)` for link updates and `load(..., Ordering::Acquire)` for reads.

### Arena Fragmentation
Arena allocator wastes space when entries are variable-sized, causing MemTable to use more memory than `approximate_size()` reports.
**Check**: Verify arena allocation is aligned and the size tracker accounts for alignment padding.

### Duplicate Key Handling
Two entries with the same (user_key, seq, type) are inserted. The skiplist may keep both or overwrite — either behavior changes semantics.
**Check**: Verify this case cannot happen (sequence numbers are unique) or that skiplist behavior is correct for duplicates.

## Review Checklist
- [ ] `MemTable::put()` has one writer at a time (DB lock in live paths; unpublished local ownership during recovery/setup)
- [ ] Skiplist atomic ordering: Release for stores, Acquire for loads
- [ ] OrdInternalKey comparison: user_key ASC, then seq DESC
- [ ] Size tracker (`approximate_size()`) updated for all entry types (point + range tombstone + node overhead)
- [ ] Range tombstones stored in separate collection with independent iteration
- [ ] No data races between concurrent reader and single writer
- [ ] Arena allocation accounts for alignment in size tracking
- [ ] `MemTableCursorIter` raw pointers remain valid (arena-backed nodes, no individual deallocation)
- [ ] `MemTableCursorIter: Send` safety: raw pointers into arena are Send-safe because arena outlives all iterators

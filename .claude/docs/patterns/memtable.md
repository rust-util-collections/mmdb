# MemTable Subsystem Review Patterns

## Files
- `src/memtable/mod.rs` — MemTable interface
- `src/memtable/skiplist.rs` (~13KB) — SkipList wrapper
- `src/memtable/skiplist_impl.rs` (~32KB) — lock-free skiplist implementation

## Architecture
- Lock-free skiplist: single-writer, concurrent multi-reader
- OrdInternalKey wrapper for comparison (user_key ASC, seq DESC)
- Approximate size tracking per insert
- Separate range tombstone collection
- Arena-style allocation for skiplist nodes

## Critical Invariants

### INV-M1: Single-Writer Guarantee
Only one thread may insert into the active MemTable at a time. The group commit leader serializes writes.
**Check**: Verify MemTable::add() is only called under the write lock (db_mutex or equivalent).

### INV-M2: Concurrent Read Safety
Readers must see a consistent view even while a writer is inserting.
**Check**: Verify skiplist uses atomic pointer stores with appropriate memory ordering (Release for writes, Acquire for reads).

### INV-M3: Key Ordering
The skiplist must maintain `(user_key ASC, sequence_number DESC)` ordering. This ensures that for a given user key, the latest version appears first.
**Check**: Verify OrdInternalKey comparator: compare user_key bytes first, then compare sequence number in REVERSE order.

### INV-M4: Size Tracking Accuracy
`approximate_memory_usage()` must not undercount by more than a small constant factor. Severe undercounting delays flush, causing OOM.
**Check**: Verify every insert path (Put, Delete, DeleteRange) adds `key.len() + value.len() + overhead` to the size counter.

### INV-M5: Range Tombstone Isolation
Range tombstones stored in the MemTable must be accessible independently from point entries, for the RangeTombstoneTracker.
**Check**: Verify range tombstones go into the dedicated collection AND are queryable via a separate iterator.

## Common Bug Patterns

### Memory Ordering Bug (technical-patterns.md 6.1)
Skiplist node's next pointer is written with `Relaxed` ordering, causing a reader on another core to see the pointer before the node's key/value data is visible.
**Check**: Verify `store(..., Ordering::Release)` for link updates and `load(..., Ordering::Acquire)` for reads.

### Arena Fragmentation
Arena allocator wastes space when entries are variable-sized, causing MemTable to use more memory than `approximate_memory_usage()` reports.
**Check**: Verify arena allocation is aligned and the size tracker accounts for alignment padding.

### Duplicate Key Handling
Two entries with the same (user_key, seq, type) are inserted. The skiplist may keep both or overwrite — either behavior changes semantics.
**Check**: Verify this case cannot happen (sequence numbers are unique) or that skiplist behavior is correct for duplicates.

## Review Checklist
- [ ] MemTable::add() only called under write serialization
- [ ] Skiplist atomic ordering: Release for stores, Acquire for loads
- [ ] OrdInternalKey comparison: user_key ASC, then seq DESC
- [ ] Size tracker updated for all entry types (point + range tombstone)
- [ ] Range tombstones stored in separate collection with independent iteration
- [ ] No data races between concurrent reader and single writer
- [ ] Arena allocation accounts for alignment in size tracking

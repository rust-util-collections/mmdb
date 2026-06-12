# Compaction Subsystem Review Patterns

## Files
- `src/compaction/leveled.rs` (~61KB, most complex module)

## Architecture
- Leveled compaction: L0 (overlapping) → L1+ (sorted, disjoint)
- Trigger: L0 file count >= `l0_compaction_trigger`
- Sub-compaction: parallel threads split on L(n+1) file boundaries
- Trivial move: single L(n) file, no L(n+1) overlap → rename without rewrite
- Streaming: O(block) memory, no full-file load

## Critical Invariants

### INV-C1: Output File Key Disjointness
L1+ output files must have non-overlapping key ranges. If two output files overlap, binary search at read time may miss keys.
**Check**: Verify the split-point logic ensures exclusive boundaries between consecutive output files.

### INV-C2: Tombstone Retention
Tombstones at non-bottommost levels MUST be written to output. At bottommost level, tombstones are dropped only if no live snapshot covers them.
**Check**: Verify `is_bottommost_level` accounts for ALL levels below, not just `level + 1`.

### INV-C2a: Range Tombstone Sub-Compaction Propagation
When a range tombstone spans sub-compaction boundaries, it must be propagated to all sub-compactions whose key ranges intersect it. The `RangeTombstoneTracker` (pre-populated with all input range deletions) is shared across sub-compactions so that each sub-compaction's `CompactionIter` sees the complete set of covering tombstones.

**Check**: Verify the `RangeTombstoneTracker` is fully constructed from all input files BEFORE sub-compactions are spawned. Sub-compaction boundaries must not truncate range tombstone coverage.

### INV-C3: Sequence Number Zeroing
Sequence numbers may only be zeroed at the bottommost level, and only for keys with no live snapshot dependency.
**Check**: Verify sequence zeroing logic checks both bottommost AND snapshot list.

### INV-C4: Sub-Compaction Boundary Correctness
When splitting work across threads, the boundary key must be assigned to exactly one sub-compaction.
**Check**: Verify exclusive-start or exclusive-end semantics at split points. A key equal to the boundary goes to exactly one side.

### INV-C5: Input File Deletion Safety
Compaction input files may only be deleted after the output files are fully written AND the MANIFEST records the new version.
**Check**: Verify file deletion happens AFTER `VersionSet::log_and_apply()` succeeds.

### INV-C5a: Stale-Output Detection in `install_compaction`
After compaction I/O completes and the lock is re-acquired to install results, verify that the SuperVersion used to pick inputs has not been superseded by a concurrent flush or another compaction. If a newer version exists, the output files from this compaction may reference stale inputs and MUST be discarded rather than installed.

**Check**: Verify `install_compaction()` checks that its input files are still present in the current Version before applying the VersionEdit. Output files built from stale inputs must be deleted without being added to the MANIFEST.

### INV-C6: Compaction Filter Contract
User-provided `CompactionFilter` receives every key-value pair exactly once. Filter decisions (Keep/Remove/ChangeValue) must be applied atomically per entry.
**Check**: Verify filter is called inside the merge loop, not on intermediate state.

## Common Bug Patterns

### Zombie Keys (technical-patterns.md 4.1)
Deleted keys reappear after compaction. Root cause: tombstone dropped at non-bottommost level.
**Trigger**: Put(k, v1) → Delete(k) → Compact L0→L1 → Compact L1→L2 (drops tombstone) → v1 still in L3.

### Compaction Stall
Background compaction thread deadlocks, causing write stall (L0 file count grows to stop limit).
**Trigger**: Compaction thread holds `db_mutex` while waiting for a condition that requires `db_mutex`.
**Check**: Verify compaction releases `db_mutex` before any blocking operation.

### Rate Limiter Starvation
Token bucket rate limiter starves compaction during heavy writes, causing L0 buildup.
**Check**: Verify rate limiter allows burst capacity and doesn't block indefinitely.

### Aggregate Key Range Overlap
When computing whether a file overlaps with deeper levels (for `is_bottommost_level` and tombstone decisions), the overlap check must consider the aggregate key ranges of all input files, not individual file ranges. A single input file may have a narrow key range, but the compaction as a whole spans a wider range that overlaps files in deeper levels.

**Check**: Verify `add_file_extents()` collects the union of all input file user-key extents before checking for overlaps in deeper levels. Individual-file checks will miss overlaps covered by other files in the same compaction.

## Review Checklist
- [ ] Output files have monotonically increasing, non-overlapping key ranges
- [ ] Tombstones retained at non-bottommost levels; range tombstones propagated to all sub-compactions
- [ ] Sequence zeroing only at bottommost with no snapshot dependency
- [ ] Sub-compaction boundaries don't duplicate or skip keys
- [ ] MANIFEST updated before input file deletion
- [ ] CompactionFilter called once per logical key-value
- [ ] Trivial move conditions are sufficient (1 input file, no overlap, no compaction filter)
- [ ] Error handling: partial compaction failure leaves DB in consistent state
- [ ] Rate limiter doesn't cause unbounded stalls
- [ ] `install_compaction` checks for stale inputs before applying VersionEdit
- [ ] `is_bottommost_level` uses aggregate key range extents from all input files, not per-file ranges

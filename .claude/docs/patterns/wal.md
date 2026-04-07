# WAL Subsystem Review Patterns

## Files
- `src/wal/writer.rs` — append-only writer with group commit
- `src/wal/reader.rs` — replay for crash recovery
- `src/wal/record.rs` — record format (checksum, length, type, payload)

## Architecture
- Block-based: records fragmented across fixed-size blocks
- Record types: Full, First, Middle, Last (for records spanning blocks)
- CRC32 checksum per record fragment
- Group commit: leader collects batches → single fsync
- Recovery: replay WAL to reconstruct MemTable

## Critical Invariants

### INV-W1: Write Ordering
Batches written to WAL must appear in the same order as their sequence number assignment. If batch A has seq 100 and batch B has seq 101, A must be written before B in the WAL.
**Check**: Verify group commit leader writes batches in sequence order, not arrival order.

### INV-W2: CRC Coverage
CRC must cover the type byte AND the payload. If CRC only covers payload, a corrupted type byte (changing First to Full) would be undetected.
**Check**: Verify `crc32(type_byte || payload)` in both writer and reader.

### INV-W3: Record Fragmentation Correctness
A record larger than block_size must be split into First + Middle* + Last fragments. Each fragment must be independently checksummed.
**Check**: Verify fragment boundary calculation accounts for the header size within each block.

### INV-W4: Recovery Completeness
WAL replay must recover ALL complete records. An incomplete record at the end of the WAL (due to crash mid-write) should be silently skipped, not cause an error.
**Check**: Verify reader handles truncated final record gracefully.

### INV-W5: Fsync Semantics
After fsync, all data written before the fsync call must be durable. Group commit must fsync AFTER writing all batches in the group, not before.
**Check**: Verify fsync ordering: write(batch_A) → write(batch_B) → fsync() → notify(A, B).

### INV-W6: WAL-MemTable Consistency
The WAL and MemTable must contain the same data. If a write is in the WAL but not in the MemTable (or vice versa), recovery will diverge from the pre-crash state.
**Check**: Verify WAL write happens BEFORE MemTable insert (WAL is the source of truth for recovery).

## Common Bug Patterns

### Partial Write on Crash (technical-patterns.md 2.3)
Process crashes after writing the record header but before writing the payload.
**Recovery behavior**: Reader sees valid header but payload is truncated or zeroed.
**Check**: Verify reader validates payload length against header length field.

### Group Commit Notification Race
Leader completes fsync and notifies waiters, but a waiter's data hasn't actually been written yet (writer interleaving bug).
**Check**: Verify leader writes ALL collected batches BEFORE fsync, and the notification is AFTER fsync.

### WAL File Not Deleted After Flush
Old WAL files accumulate because the deletion condition checks the wrong sequence number.
**Check**: Verify WAL file deletion uses the flushed MemTable's maximum sequence number.

## Review Checklist
- [ ] Write ordering matches sequence number ordering
- [ ] CRC covers type byte + payload
- [ ] Record fragmentation handles boundary correctly (last bytes of a block)
- [ ] Recovery handles truncated final record without error
- [ ] Fsync happens after all batch writes, before notification
- [ ] WAL write precedes MemTable insert
- [ ] Old WAL files deleted after successful flush + MANIFEST update
- [ ] Group commit: all waiters' data is included in the fsync'd write

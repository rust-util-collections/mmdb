# WAL Subsystem Review Patterns

## Files
- `src/wal/writer.rs` — append-only writer with group commit
- `src/wal/reader.rs` — replay for crash recovery
- `src/wal/record.rs` — record format (checksum, length, type, payload)

## Architecture
- Block-based: records fragmented across fixed-size blocks
- Record types: Full, First, Middle, Last (for records spanning blocks)
- CRC32 checksum per record fragment
- Group commit: leader collects batches → ordered WAL append → one flush/fsync
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
WAL replay must recover every complete record without turning arbitrary
corruption into prefix recovery. `WalReader` surfaces a truncated record as
corruption; DB recovery may tolerate it only in the highest recoverable WAL
when every remaining byte is zero padding/a zero-extended tail.
**Check**: Earlier-WAL corruption, mid-log corruption, or non-zero data after a
bad record must fail open loudly. Only the active torn tail is truncated.

### INV-W5: Fsync Semantics
When any grouped request sets `WriteOptions::sync`, all WAL-enabled records
appended before the shared sync must be durable before acknowledgement. A group
with no sync request flushes the userspace buffer but does not promise crash
durability.
**Check**: Verify append(A) → append(B) → sync-or-flush → MemTable publication
→ notify, and that one sync request upgrades the whole group's WAL durability.

### INV-W6: WAL-MemTable Consistency
For WAL-enabled requests, every acknowledged MemTable mutation must have the
corresponding earlier WAL record. `disable_wal` is an explicit exception whose
data may disappear after a crash.
**Check**: Verify WAL append/flush-or-sync happens before MemTable insertion for
enabled requests and that disabled requests are never advertised as durable.

## Common Bug Patterns

### Partial Write on Crash (technical-patterns.md 2.3)
Process crashes after writing the record header but before writing the payload.
**Recovery behavior**: Reader sees valid header but payload is truncated or zeroed.
**Check**: Verify reader validates payload length against header length field.

### Group Commit Notification Race
Leader flushes/syncs and notifies waiters before a request's MemTable entries
and committed sequence are published.
**Check**: Verify all assigned entries are inserted and `committed_sequence` is
published before notification.

### WAL File Not Deleted After Flush
Old WAL files accumulate because the deletion condition checks the wrong sequence number.
**Check**: Verify WAL file deletion uses the flushed MemTable's maximum sequence number.

## Review Checklist
- [ ] Write ordering matches sequence number ordering
- [ ] CRC covers type byte + payload
- [ ] Record fragmentation handles boundary correctly (last bytes of a block)
- [ ] Recovery tolerates only the active WAL's zero-padded torn tail
- [ ] WAL flush/fsync aggregation matches `sync` and `disable_wal` options
- [ ] WAL write precedes MemTable insert
- [ ] Old WAL files deleted after successful flush + MANIFEST update
- [ ] Group commit notifies only after assigned MemTable entries are published

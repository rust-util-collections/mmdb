# WAL Review Patterns

**Files:** `writer.rs`, `reader.rs`, `record.rs`.

**Arch:** Fixed blocks; Full/First/Middle/Last fragments; CRC per fragment;
group commit = order batches → append → one flush/fsync; recovery rebuilds MemTable.

## Invariants

**W1 Order** — WAL write order = seq assignment order (not bare arrival).
**W2 CRC** — covers type **and** payload (`crc32(type||payload)`). Type-only corrupt must fail.
**W3 Fragments** — oversize → First+Middle*+Last; each checksummed; header size in block boundary math.
**W4 Recovery** — all complete records. Truncated record = corruption unless
highest recoverable WAL and remaining bytes are zero-pad/torn tail only.
Earlier/mid-log/non-zero after bad → hard fail; only active torn tail truncated.
**W5 Fsync** — any grouped `sync=true` ⇒ all prior WAL-enabled records durable before ack.
No-sync group: flush buffer only (no crash durability claim).
**W6 WAL↔MemTable** — for WAL-on requests, ack ⇒ earlier matching WAL record.
`disable_wal` may lose data after crash — never market as durable.

## Bug patterns

**Partial write (tech 2.3)** — header without full payload; reader checks length.
**Notify race** — notify only after MemTable inserts + `committed_sequence` publish.
**Stale WAL delete** — use flushed mem max seq; after MANIFEST acknowledges state.

## Checklist

- [ ] Write order ≡ seq order
- [ ] CRC type+payload both sides
- [ ] Fragment boundaries handle end-of-block
- [ ] Recovery: only active zero-pad torn tail tolerated
- [ ] flush/fsync respects sync + disable_wal
- [ ] WAL before MemTable for enabled requests
- [ ] Old WAL deleted after flush + MANIFEST
- [ ] Notify after assigned entries published

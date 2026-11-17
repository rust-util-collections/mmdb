# WAL Review Patterns

**Files:** `writer.rs`, `reader.rs`, `record.rs`.

**Arch:** Fixed blocks; Full/First/Middle/Last fragments; CRC per fragment;
group commit = order batches → append → one flush/fsync; recovery rebuilds MemTable.

## Invariants

**W1 Order** — WAL write order = seq assignment order (not bare arrival).
**W2 CRC** — covers type **and** record bytes (`crc32(type || data)`). A changed type byte must produce `ErrorKind::Corruption`.
**W3 Fragments** — oversize → First+Middle*+Last; each checksummed; header size in block boundary math.
**W4 Recovery** — retain all complete records. An incomplete record returns a
recovery error unless it belongs to the highest recoverable WAL and the
remaining bytes establish an interrupted final append (possibly zero-padded).
If the stored CRC verifies complete data under another valid record type or
record length, the header is damaged (`ErrorKind::Corruption`); a zero suffix
must not permit prefix truncation. An invalid earlier record, or non-zero bytes
after the failed record, must stop recovery and leave the file intact
(and `open_read_only` never truncates any file).
**W5 Fsync** — any grouped `sync=true` ⇒ all prior WAL-enabled records durable before ack.
No-sync group: flush buffer only (no power-loss durability claim).
**W6 WAL↔MemTable** — for WAL-on requests, ack ⇒ earlier matching WAL record.
`disable_wal` omits WAL durability by contract — never document otherwise.

## Bug patterns

**Partial write (tech 2.3)** — header without complete record data; reader checks length.
**Notify race** — notify only after MemTable inserts + `committed_sequence` publish.
**Stale WAL delete** — use flushed mem max seq; after MANIFEST acknowledges state.

## Checklist

- [ ] Write order ≡ seq order
- [ ] CRC covers `type + data` on both writer and reader
- [ ] Fragment boundaries handle end-of-block
- [ ] Recovery: only active zero-pad torn tail tolerated (and never truncated in read-only mode)
- [ ] flush/fsync respects sync + disable_wal
- [ ] WAL before MemTable for enabled requests
- [ ] Old WAL deleted after flush + MANIFEST
- [ ] Notify after assigned entries published

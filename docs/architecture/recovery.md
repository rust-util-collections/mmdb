# Database files and recovery

On reopen, MMDB reconstructs the live SST set from the MANIFEST, then
replays valid batches from eligible WALs. Writable open installs recovered
rows on disk. Read-only open retains them in memory.
[Opening a store](../usage.md#open-and-configure-a-store) · [Architecture overview](../architecture.md)

## Files in a database directory

| Name | Role and format |
|---|---|
| `LOCK` | On Unix a writable opener takes an exclusive nonblocking `flock`. Read-only openers share a lock if the file exists. With no `LOCK`, or on a platform without Unix `flock`, a read-only caller must keep the directory stable. |
| `CURRENT` | Names the active `MANIFEST-{number}`. On creation and rotation, MMDB syncs the new MANIFEST, writes and syncs a temporary `CURRENT` file, renames it to `CURRENT`, and syncs the directory. |
| `MANIFEST-*` | WAL-framed metadata. The first logical record is the 16-byte `DBHeader` containing `MMDB` followed by three little-endian `u32` fields for format version (5), level count (2–16), and prefix length. Further `VersionEdit` records describe file additions and deletions, WAL and file numbers, and sequence state. |
| `*.wal` | A log of encoded write batches. Its 32 KiB blocks contain `Full`, `First`, `Middle`, or `Last` fragments. Each seven-byte header holds CRC32 of the type and payload, a little-endian 16-bit payload length, and the type byte. A batch starts with its little-endian first sequence (8 bytes) and operation count (4 bytes). |
| `*.sst` | An immutable sorted string table in SST v2 format. It contains point entries, range-deletion intervals, or both. Its 48-byte footer locates the metaindex and index directory. [SST layout](sst-and-cache.md#sst-v2-physical-layout) |

The crate major version, MANIFEST format version, and SST format version
are independent. MMDB v6 uses MANIFEST format 5 and SST v2, which is why
it can reopen a v5 store in place. [Upgrade procedure](../migration.md)

## Create a store

`DB::open` uses `CURRENT` as the existence marker. When creating missing
directories it opens and syncs each new entry's parent before descending
further. It resolves the directory path so later operations use that
location even if a symlink in the supplied path changes.

Under the directory lock the opener checks again for an existing store.
A directory containing WALs, SSTs, or committed MANIFEST state without
`CURRENT` yields `Corruption`. Creation can retry an interrupted initial
publication if all that remains is compatible with an uncommitted empty
store. It writes and syncs the header and initial edit before publishing
`CURRENT`. [Directory creation](../../src/db/directory.rs) · [Initial MANIFEST](../../src/manifest/version_set.rs)

## Validate the MANIFEST and live SSTs

1. Read `CURRENT`, open the named MANIFEST, and decode its header. A missing
   referenced MANIFEST or an unsupported header yields `Corruption`. A
   writable opener checks the requested level count and prefix length and
   reports `InvalidArgument` on mismatch. A read-only opener adopts both.
2. Replay the edits to determine the surviving files, then open those SSTs.
   Validate sizes, file-key bounds, range-deletion metadata, and disjoint
   file-key bounds within every level below L0.
3. Verify that MANIFEST `last_sequence` covers the sequence evidence for
   every live SST. Newly built tables include an eight-byte little-endian
   `table.max_sequence` in the checksummed metaindex. For older tables
   without it, **both** open modes scan index keys, readable point blocks,
   and range-deletion stripes on each open until those tables are rewritten.
   A corrupt point block can surface when a read visits it.
4. A structurally torn MANIFEST tail followed only by EOF or zero padding
   can be ignored after the valid prefix. Writable open reopens the MANIFEST
   for append at the valid offset after checking the recovered file set.
   Read-only open leaves its bytes intact.

[MANIFEST recovery and validation](../../src/manifest/version_set.rs) ·
[SST bounds and maximum sequence](../../src/sst/table_reader/mod.rs)

## Replay eligible WALs

A nonzero MANIFEST `log_number` identifies a required WAL. A missing
required WAL yields `Corruption` in either mode. MMDB enumerates WALs with
numbers at or above that floor and replays them in increasing order,
checking fragment framing and batch contents. Writable recovery allocates
fresh file numbers beyond those already on disk.

Only the **highest nonempty eligible WAL** can have a recoverable torn tail.
A higher-numbered empty standby file does not move this boundary. The reader
must classify the failed fragment as a truncation candidate and confirm that
only zero padding or EOF follows. A malformed complete batch or truncation in
an earlier WAL fails the open. [WAL reader](../../src/wal/reader.rs) ·
[Interrupted-write recovery tests](../../tests/crash_recovery.rs)

| Open mode | Result after successful replay |
|---|---|
| `DB::open` | While validating the WALs, stage recovered L0 SSTs when replay reaches the write-buffer threshold and at WAL boundaries. Once all eligible WALs validate, create a fresh WAL and install one MANIFEST edit with its number, the recovered files, and the last sequence. Sync the MANIFEST before removing obsolete WALs and orphan SSTs. |
| `DB::open_read_only` | Retain replayed entries in the active memtable. Start no flush or compaction workers. A large WAL can exceed the configured write buffer because recovery keeps the rows in memory. |

If a later WAL fails validation, the writable opener discards staged recovery
SSTs. The source WALs remain available because the combined recovery edit
has not been installed. [Open and replay paths](../../src/db.rs) ·
[Read-only acceptance tests](../../tests/read_only.rs)

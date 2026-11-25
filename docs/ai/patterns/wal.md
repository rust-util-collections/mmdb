# WAL Review Patterns

**Files:** `writer.rs`, `reader.rs`, `record.rs`, `src/db.rs` (`write_batch_group`, `freeze_memtable`, `open_impl`), `src/db/flush.rs`.

**Arch:** 32 KiB (`BLOCK_SIZE`) blocks with 7-byte (`HEADER_SIZE`) `Full`/`First`/`Middle`/`Last` fragment headers (`crc32(type || payload)`); vectored `WalWriter::add_record_parts` (`PartsCursor`) streams `[wal_header(first_seq), batch.payload]` without intermediate concatenation; group commit supports `DurabilityMode::{WalAsync, WalSync, DisableWal}` plus `DB::sync_wal()` (`WalFence`); leader rotates WALs outside `DB::inner` while `mmdb-flush` unlinks `old_wal` only after durable MANIFEST install.

## Invariants

**W1 Sequence & group order** — WAL record order equals contiguous sequence assignment order. `DisableWal` batches still reserve sequences and apply to `active_memtable` in group order while omitting WAL records.

**W2 CRC & vectored framing** — `PartsCursor` skips empty interior slices, zero-pads `< 7B` block trailers, emits a 0-length `First` fragment when `remaining == 7` with non-empty data, and computes streaming `crc32([record_type as u8] || fragment_bytes)` identical to `WalReader`. A failed batch append latches the DB background error. A failed MANIFEST append poisons the VersionSet.

**W3 Durability modes, `WalFence`, & `wal_bytes_per_sync`** — `WalWriter::new` syncs the parent directory on creation, making `WalWriter::sync_data()` (`BufWriter::flush` + `fdatasync`) sufficient for append/rotation durability (`WalWriter::sync()` = `sync_all` is used after `open_append_truncated` and on MANIFEST). Any grouped `WalSync` or `WalFence { target_seq > synced_wal_seq }` runs `wal.sync_data()` (even when grouped only with `DisableWal` batches) and advances `synced_wal_seq` to `last_seq`; `WalAsync` runs `wal.flush()` without advancing `synced_wal_seq`. `wal_bytes_per_sync` (`sync_file_range`) is strictly an advisory writeback hint (`end_offset >= last_writeback`) and never substitutes for `sync_data()`.

**W4 Post-commit acknowledgement & rotation** — once `wal.sync_data()`/`flush()`, `active_memtable` inserts, and `committed_sequence.store(Release)` complete, committed requests must receive `Ok(())` even if a later batch in the group hits `append_error` or post-commit `submit_active()` (`freeze_memtable` / `FlushQueue::check_capacity`) fails. Failed `freeze_memtable` restores `old_wal`, keeps `wal_number` and `active_memtable` intact, and fail-stops subsequent writes.

**W5 Multi-WAL bounded recovery & torn-tail isolation** — verify the WAL named by a nonzero MANIFEST `log_number` exists, then replay eligible WALs in ascending order, spilling mid-WAL (`>= write_buffer_size`) and at WAL boundaries into `PendingSsts` without appending the recovery edit until every WAL validates, then commit one atomic `VersionEdit` + `sync_manifest()` before `remove_orphan_files`. Tolerate an incomplete record (`last_error_is_truncation() && rest_is_zero_padding()`) **only** on `recoverable_tail_wal_num` (highest non-empty `*.wal >= log_number`); checksum mismatches, malformed batch payloads/trailing bytes, or earlier-WAL truncations fail closed with `ErrorKind::Corruption` before installing staged recovery SSTs or retiring the source WALs. Read-only replay keeps recovered entries in memory and leaves store files intact.

## Checklist

- [ ] `PartsCursor` handles empty slices, 0-byte records, and 32 KiB block-tail boundaries identically to `WalReader`
- [ ] A fence already covered by `synced_wal_seq` succeeds immediately. An outstanding target requires `sync_data()` even for a fence-only group
- [ ] Post-commit `append_error` / `submit_active` failures preserve `Ok(())` for already-committed requests while latching fail-stop
- [ ] `old_wal` unlinked only after `FlushContext::flush` confirms MANIFEST durability; `mmdb-flush` exits on failure so newer WALs never leapfrog a failed generation
- [ ] Rotation consumes a ready zero-length standby WAL or creates a replacement. Empty higher WALs do not change the eligible torn-tail file
- [ ] Writable multi-WAL recovery stages spills via `PendingSsts` and tolerates zero-padded torn tails solely on `recoverable_tail_wal_num` (never mutating files in `open_read_only`)

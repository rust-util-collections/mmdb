# MANIFEST / VersionSet Review Patterns

**Files:** `version_set.rs`, `version_edit.rs`, `version.rs`, `file_numbers.rs`, `mod.rs`; tests: `crash_recovery.rs`, `integration.rs`, `read_only.rs`.

**Arch:** v5 `MANIFEST-*` is WAL-framed: record #0 is the 16-byte `DBHeader` (`"MMDB" || 5u32 || levels: u32 || prefix_len: u32`), followed by incremental `VersionEdit`s; `CURRENT` names the active MANIFEST. `Version` (`files: Vec<Arc<Vec<TableFile>>>`) shares unchanged levels across versions via `Arc::make_mut`. `FileNumbers` provides one checked `AtomicU64` sequence across WAL, MANIFEST, and SST files; `PendingSsts` owns one job's uncommitted SST numbers and unlinks only its own files on drop unless `.keep()` is called after `log_and_apply_synced`.

## Invariants

**MAN1 Reject = not applied** — `log_and_apply_synced` validates level range, exact-level deletions, duplicate/live file numbers, `open_table` (`has_range_deletions`, valid 8+-byte `InternalKey` bounds, `smallest_key <= largest_key`), and L1+ `validate_level_disjointness` (with no empty-key skips) *before* appending to MANIFEST or mutating `self.current`.

**MAN2 `DirSynced` & new-SST durability order** — SST `finish()` (`sync_all`) → `sync_new_sst_parent_dir` (`DirSynced`) → `log_and_apply_synced` → `confirm_manifest_durable` *before* acknowledging flush durability, retiring `immutable_memtables`, or unlinking superseded WAL/SST inputs.

**MAN3 Serialized fail-stop (`VersionSet::poisoned`)** — failed MANIFEST append, `confirm_manifest_durable`, `sync_new_sst_parent_dir`, or post-`CURRENT` directory sync latches shared `Arc<AtomicBool>` `poisoned` (`Release`) until reopen. Both `confirm_manifest_durable` and directory syncs must serialize `(check_poison, sync, store_poison)` on retained descriptors so no concurrent caller can observe a clean second `fsync` and unlink inputs before `poisoned` is published.

**MAN4 Recovery ≡ live apply & atomic multi-WAL spill** — `DBHeader::read` (`levels` in `2..=16`, version `5`) and writable geometry match (`levels`, `prefix_len` → `ErrorKind::InvalidArgument` on mismatch; `ErrorKind::Corruption` on v4/invalid header) run *before* `VersionEdit` replay, `WalWriter::open_append_truncated`, or any store file mutation. Read-only open adopts `versions.geometry()` and never opens or truncates MANIFEST/WAL files. Writable multi-WAL recovery stages spilled SSTs via `PendingSsts` and commits one atomic `VersionEdit` (`new_files` in ascending number order, `log_number = new_wal`, `last_sequence = max_seq`) + `sync_manifest()` *before* `remove_orphan_files`.

**MAN5 Monotonic IDs** — `FileNumbers` (`checked_add(1)` → `ErrorKind::Background` on `u64::MAX` exhaustion) and `last_sequence` never reuse or regress; recovery advances `FileNumbers` past every live/on-disk file number before creating new files.

**MAN6 `CURRENT` creation & rotation** — `create_with_cache` writes `DBHeader` + initial `VersionEdit` and `sync_manifest()`s *before* publishing `CURRENT`. `maybe_compact_manifest` writes `DBHeader` + snapshot `VersionEdit` to `MANIFEST-{new}`, syncs both new and old MANIFESTs, writes+syncs `CURRENT.tmp.{new}`, renames `CURRENT`, installs the writer, and syncs `db_path` (keeping the old MANIFEST and latching `poisoned` if post-rename directory sync fails).

## Checklist

- [ ] `DBHeader` validated before tail truncation or store writes; preserved on every MANIFEST rotation
- [ ] Writable reopen rejects `(num_levels, prefix_len)` mismatch with `InvalidArgument`; read-only adopts stored geometry
- [ ] `open_table` validates `decode_internal_key` + `smallest_key <= largest_key` + `has_range_deletions`; `validate_level_disjointness` never skips empty bounds
- [ ] `DirSynced` + `confirm_manifest_durable` precede WAL/SST unlinks; `PendingSsts` cleans failed builds without touching sibling jobs
- [ ] `confirm_manifest_durable` and `sync_new_sst_parent_dir` serialize `fsync` failure with `poisoned` publication
- [ ] Multi-WAL recovery stages spills without mutating MANIFEST until all WALs validate
- [ ] `CURRENT` tmp/sync/rename/dir-sync order keeps fallback MANIFEST on post-rename dir-sync failure

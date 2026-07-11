# MANIFEST and VersionSet Review Patterns

## Files

- `src/manifest/version_set.rs` — edit validation, persistence, recovery, rotation
- `src/manifest/version_edit.rs` — durable tagged encoding
- `src/manifest/version.rs` — immutable live file-set snapshots
- `src/manifest/mod.rs` — module boundary
- `tests/crash_recovery.rs` — durability and orphan-cleanup coverage

## Architecture

`VersionEdit` records file additions/deletions and monotonic counters in a
WAL-framed MANIFEST. `CURRENT` names the active MANIFEST. Recovery replays edits
into an immutable `Version`; periodic rotation writes a full snapshot to a new
MANIFEST and atomically publishes it through `CURRENT`.

## Critical invariants

### INV-MAN1: Rejection contract

`VersionSet::log_and_apply()` returning `Err` means the edit was not appended or
installed. Callers rely on this to delete newly-built output SSTs safely.

**Check**: Open/validate every new SST, level, and exact-level deletion before
appending. After append/install, maintenance such as rotation must defer or
poison rather than surface an ambiguous `Err`.

### INV-MAN2: New-SST durability ordering

Before a MANIFEST record that references a new SST can become durable:

1. SST contents are finished and synced;
2. the DB directory is synced so the SST directory entry is durable;
3. the edit is appended;
4. the MANIFEST is synced before the operation reports durable completion.

**Check**: Never delete input files before the installed edit is durably synced.

### INV-MAN3: Fail-stop poisoning

A failed MANIFEST append, MANIFEST sync, new-SST directory sync, or
post-CURRENT-publish directory sync can make later success ambiguous. The
writer must poison and reject further edits until reopen where documented.

**Check**: All sync handles share the same poison flag and public DB operations
observe it.

### INV-MAN4: Recovery equivalence

Recovery must reconstruct the same final file set and counters as in-process
application.

**Check**: Replay deletions at the exact recorded level, process delete-before-
add for trivial moves, reject duplicate/live-level mismatches and missing SSTs,
and tolerate corruption only at a torn zero-padded tail. Open only files live
after the full replay.

### INV-MAN5: Monotonic identifiers

File numbers and sequence numbers must never regress or be reused.

**Check**: Apply/recovery take the maximum forward file allocator value;
parallel reservations are reconciled with `ensure_file_number_at_least()`.

### INV-MAN6: CURRENT publication

The new MANIFEST snapshot and old writer must both contain all applied edits
before publication. Write and sync a unique `CURRENT.tmp.<manifest_number>`,
rename it to `CURRENT`, install the matching writer in-process, then sync the
directory. Retain the old MANIFEST if post-publish durability is uncertain.

**Check**: Pre-publish failures discard/defer the new snapshot. Post-publish
directory-sync failure poisons further edits instead of switching back or
letting old/new MANIFESTs diverge.

### INV-MAN7: Durable encoding compatibility

`VersionEdit` tag meanings are persisted format. Existing tags must never be
reinterpreted; new fields use new tags and old tags retain their decode
semantics.

**Check**: Bounds-check every length before slicing and preserve legacy tag
decoding.

## Review checklist

- [ ] `Err` from `log_and_apply` still means no edit applied
- [ ] New SST and directory durability precede MANIFEST durability
- [ ] Every ambiguous write/sync failure poisons consistently
- [ ] Recovery and live apply enforce the same level/file invariants
- [ ] Torn-tail tolerance cannot hide valid later records
- [ ] File allocator and last-sequence bookkeeping never regress
- [ ] CURRENT temp write, sync, rename, install, and directory sync are ordered
- [ ] Old MANIFEST deletion occurs only after durable CURRENT publication
- [ ] Persisted tags remain append-only and backward compatible

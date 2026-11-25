# MANIFEST / VersionSet Review Patterns

**Files:** `version_set`, `version_edit`, `version`, `file_numbers`, `mod`; tests: `crash_recovery`.

**Arch:** VersionEdit in WAL-framed MANIFEST; CURRENT names active MANIFEST;
replay → immutable `Version` (`files: Vec<Arc<Vec<TableFile>>>` sharing unchanged
levels across versions via `Arc::make_mut`); rotation = full snapshot + atomic
CURRENT swap; read-only open replays without truncating or opening a MANIFEST writer.

## Invariants

**MAN1 Reject = not applied** — `log_and_apply` Err ⇒ no append/install (callers may delete new SSTs).
Validate before append; after install, rotation is deferred or latches fail-stop (`VersionSet::poisoned`) — never an ambiguous Err.

**MAN2 New-SST durability order** — SST finish+sync → DB dir sync → edit append → MANIFEST sync before durable OK. Never unlink inputs before durable installed edit.

**MAN3 Fail-stop latch (`VersionSet::poisoned`)** — failed MANIFEST append/sync, new-SST dir sync, or post-CURRENT dir sync sets `poisoned` until reopen where documented. Shared `Arc<AtomicBool>` so public operations observe fail-stop immediately.

**MAN4 Recovery ≡ live apply** — same file set/counters. Exact-level deletes; delete-before-add for trivial move; reject dups/level mismatch/missing SST; incomplete zero-padded final append only (left untouched on disk in `open_read_only`). Open only files live after full replay.

**MAN5 IDs monotonic** — file# / seq never reuse or regress. Concurrent file creators share one checked allocator. Replay and stale edits can only advance it.

**MAN6 CURRENT publish** — both old writer and new snapshot hold all edits →
`CURRENT.tmp.<n>` write+sync → rename CURRENT → install writer → dir sync.
Uncertain post-publish durability ⇒ keep old MANIFEST. Pre-publish fail discard/defer;
post dir-sync fail sets fail-stop `poisoned` (no switch-back / divergence).

**MAN7 Encoding** — require the v5 DBHeader before replay or repair. Validate 2 through 16 levels, persisted prefix length, edit tags and field lengths.

## Checklist

- [ ] Err ⇒ nothing applied; unchanged level `Arc`s preserved for pinned readers
- [ ] SST+dir before MANIFEST durability
- [ ] Ambiguous write/sync latches `VersionSet::poisoned` consistently
- [ ] Recovery = live invariants
- [ ] Tail recovery must retain every later complete record
- [ ] Allocators/last-seq never regress
- [ ] CURRENT tmp/sync/rename/install/dir order
- [ ] Old MANIFEST delete only after durable CURRENT
- [ ] Writable reopen matches stored geometry, read-only adopts it, and a format mismatch preserves the files

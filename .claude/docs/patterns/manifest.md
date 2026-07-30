# MANIFEST / VersionSet Review Patterns

**Files:** `version_set`, `version_edit`, `version`, `mod`; tests: `crash_recovery`.

**Arch:** VersionEdit in WAL-framed MANIFEST; CURRENT names active MANIFEST;
replay → immutable Version; rotation = full snapshot + atomic CURRENT swap.

## Invariants

**MAN1 Reject = not applied** — `log_and_apply` Err ⇒ no append/install (callers may delete new SSTs).
Validate before append; after install, rotation deferred/poison — never ambiguous Err.

**MAN2 New-SST durability order** — SST finish+sync → DB dir sync → edit append → MANIFEST sync before durable OK. Never unlink inputs before durable installed edit.

**MAN3 Fail-stop poison** — failed MANIFEST append/sync, new-SST dir sync, or post-CURRENT dir sync → poison until reopen where documented. Shared poison; public ops observe it.

**MAN4 Recovery ≡ live apply** — same file set/counters. Exact-level deletes; delete-before-add for trivial move; reject dups/level mismatch/missing SST; torn zero-pad tail only. Open only files live after full replay.

**MAN5 IDs monotonic** — file# / seq never reuse or regress. Max-forward apply; `ensure_file_number_at_least` for parallel reserve.

**MAN6 CURRENT publish** — both old writer and new snapshot hold all edits →
`CURRENT.tmp.<n>` write+sync → rename CURRENT → install writer → dir sync.
Uncertain post-publish durability ⇒ keep old MANIFEST. Pre-publish fail discard/defer;
post dir-sync fail poison (no switch-back / divergence).

**MAN7 Encoding** — tags append-only; never reinterpret old tags. Bounds-check lengths.

## Checklist

- [ ] Err ⇒ nothing applied
- [ ] SST+dir before MANIFEST durability
- [ ] Ambiguous write/sync → consistent poison
- [ ] Recovery = live invariants
- [ ] Torn-tail cannot hide later valid records
- [ ] Allocators/last-seq never regress
- [ ] CURRENT tmp/sync/rename/install/dir order
- [ ] Old MANIFEST delete only after durable CURRENT
- [ ] Tags backward compatible

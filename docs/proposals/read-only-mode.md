# Proposal: Read-Only Open Mode

Status: **Draft** · Author: ktmlm · Scope: `DB::open` family, read path, shutdown path

## 1. Goal

Let a DB be opened on a **read-only medium** (RO filesystem, read-only
permissions, snapshot mount) and read from, with **zero write syscalls to the
store directory** from open through drop. Today this is impossible: `DB::open`
unconditionally writes, so an RO disk fails at the first write with `EROFS`
before any read succeeds.

Non-goals: multi-process "read-only + live writer" shared access, and
removing the existing writable fast path.

## 2. Current write inventory (the exact problem)

Every write during the lifetime of a `DB`, mapped to source:

### 2.1 Open — `DB::open` (`src/db.rs:525`)

| # | Site | Operation |
|---|------|-----------|
| 1 | `db.rs:551` | `create_dir_all` (only `create_if_missing`) |
| 2 | `db.rs:574-581` | `LOCK` file: `create(true).write(true).open` + `flock(LOCK_EX)` |
| 3 | `version_set.rs:330-333` | `WalWriter::open_append_truncated` — reopen MANIFEST for append (even with no corrupt tail) |
| 4 | `db.rs:727-729` | `WalWriter::new` — create the fresh WAL |
| 5 | `db.rs:734-770` | flush recovered WAL → SST (`write_memtable_ssts`) |
| 6 | `db.rs:769-783` | `log_and_apply` + `sync_manifest` — **every** open writes one MANIFEST edit (new `log_number`) |
| 7 | `db.rs:788` | `remove_orphan_files` — unlink obsolete files |
| 8 | `db.rs:842+` | spawn background compaction threads |

### 2.2 Read path — should be inert, but leaks

`get_with_options` runs `check_read_compaction()` (`db.rs:2962-2977`), which
pushes level-≥2 samples into `read_compaction_hints` and calls
`signal_compaction()` — **a read can start a background compaction**, i.e. a
write. This is the only read-path write leak.

### 2.3 Shutdown

- `close()` (`db.rs:2810`): `freeze_and_flush` (write SST) + WAL `sync` +
  `remove_orphan_files`.
- `Drop` (`db.rs:4186`): WAL `sync` + release LOCK/caches.

## 3. Architecture — where the logical spaces are entangled

The read space and write space are entangled at exactly three points; fixing
them is both the clean design **and** the thing that makes read-only cheap.

**E1 — Open is a fused recover+arm sequence.** `open` interleaves "recover
read state" (CURRENT → MANIFEST → SST → WAL→memtable) with "arm the writer"
(create WAL, append MANIFEST, flush, spawn compaction). There is no seam. In
reality the recovery loop is *already* read-only all the way up to the flush at
`db.rs:734` — the writes only begin at "create fresh WAL" (`db.rs:727`).
Read-only is literally "stop before the arm-write tail."

**E2 — The read path can trigger writes** (`check_read_compaction`). This
coupling is undesirable on its own; read-only requires severing it.

**E3 — The manifest writer is only *half*-optional.** It is already
`Arc<Mutex<Option<WalWriter>>>` (`version_set.rs:41`), but
`recover_with_cache` always constructs `Some` via `open_append_truncated`
(`version_set.rs:332`). Making the `None` case first-class (read-only) is a
small, real change, not a new abstraction.

### Recommendation on structure

Keep **one** `DB` type with a `read_only: bool` mode, rather than a separate
`ReadOnlyDB` type. Rationale:

- The write-only state (`write_queue`, `WalWriter`, compaction handles,
  `dead_keys`, `snapshot_list`) is already isolated behind `Option`-typed seams
  or is simply left unused when writes never happen.
- A second type would double the public API and break `vsdb`'s `DbOptions`-based
  call sites for marginal type-safety gain.

The structural change is **phase-splitting `open`**, not a rewrite.

## 4. Design

### 4.1 API

```rust
// options.rs
pub struct DbOptions {
    // ...
    /// Open without ever writing to the store. Reads only; all write
    /// methods return `Error::read_only`. `create_if_missing`/`error_if_exists`
    /// are ignored (treated as false).
    pub read_only: bool,          // default false
}
```

Convenience entry point (thin wrapper, reads better than a bool):

```rust
// db.rs / lib.rs
impl DB {
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(DbOptions { read_only: true, ..DbOptions::default() }, path)
    }
}
```

### 4.2 Split `open` into two phases

```
open(options, path)
  ├─ recover_read_state(path)            // pure read, shared by both modes
  │     read CURRENT → recover VersionSet (manifest_writer = None when RO)
  │     read_dir → wal_numbers → replay WAL → active_memtable
  │     open SST handles in recovered version
  └─ arm_write_capability()              // SKIPPED entirely when read_only
        create LOCK + flock(EX)
        create fresh WAL
        if recovered memtable non-empty → flush to SST
        log_and_apply(new log_number) + sync_manifest
        remove_orphan_files
        spawn compaction threads
```

Concretely in `db.rs`:

1. Recover phase is the existing code from `db.rs:626-721` (read_dir → sort →
   replay into `active_memtable`), **plus** a `read_only` flag threaded into
   `VersionSet::recover_with_cache`.
2. Wrap everything from `db.rs:727` (`let wal_number = ...; WalWriter::new`)
   through `db.rs:788` (`remove_orphan_files`) and `db.rs:842+` (spawn) in
   `if !read_only { ... }`. For read-only, skip; set `wal_writer: None`,
   `wal_number: 0`, and leave `max_sequence`/`next_file_number` as recovered
   (they are never consumed because no writes happen).

### 4.3 Per-site changes

| Site | Read-only behavior |
|------|--------------------|
| `create_dir_all` (`db.rs:551`) | skip; `open_read_only` must fail if `CURRENT` missing (reuse the `!CURRENT.exists()` guard at `db.rs:552`) |
| LOCK (`db.rs:573`) | do **not** `create`/`write`. If a `LOCK` file already exists, take `flock(LOCK_SH)` cooperatively; otherwise proceed unlocked. Many readers may share |
| MANIFEST reopen (`version_set.rs:332`) | pass `read_only`; skip `open_append_truncated`; leave `manifest_writer = None`. Add an explicit guard in `log_and_apply`/`sync_manifest`: write on a `None` writer → `Error::read_only` (defensive; the read-only path never calls them) |
| fresh WAL (`db.rs:727`) | skip |
| WAL→SST flush (`db.rs:734`) | **skip the flush, keep the memtable** — see §5 |
| MANIFEST edit+sync (`db.rs:769/781`) | skip |
| `remove_orphan_files` (`db.rs:788`) | skip |
| compaction threads (`db.rs:842`) | do not spawn; `compaction_handles` empty |
| `check_read_compaction()` (`db.rs:2962`) | short-circuit when `read_only` (and only then) |
| `close()` (`db.rs:2810`) | skip `freeze_and_flush` and WAL `sync`; only join threads (none) + release caches/LOCK |
| `Drop` (`db.rs:4190`) | mind `wal_writer.is_none()` → no sync |
| write methods (`put`/`delete`/`WriteBatch` apply/`flush`/`compact*`) | `check_usable` → return `Error::read_only()` |

### 4.4 Error type

Add `ErrorKind::ReadOnly` + `Error::read_only()` in `src/error.rs`. All write
entry points return it; it must **not** be conflated with an `EROFS` I/O error.

## 5. Semantics: residual WAL (the correctness crux)

If the RO medium holds WAL files with committed-but-unflushed data, reading
them into the memtable is **correct** — the data is recoverable on every open,
and no disk write is needed. The current recovery loop already does exactly
this *before* flushing (`db.rs:670-721`). Read-only therefore:

- **default**: replay WAL → `active_memtable`, do not flush. Reads see the
  committed data; the WAL stays on disk and is re-read on the next open.
- torn-tail tolerance (`db.rs:693-718`) is unchanged and applies.

No refusal-to-open is required. (Contrast: silently *dropping* the WAL would
be data loss — never do that.)

## 6. Phased implementation

1. **Phase 0 — seams (no behavior change).** Thread a `read_only` param through
   `VersionSet::recover_with_cache` (default `false`), make `manifest_writer =
   None` path legal, add `read_only` field to `DbOptions` (default `false`).
   All existing tests green.
2. **Phase 1 — split `open`.** Extract `recover_read_state` / gate the
   arm-write tail behind `if !read_only`. Writable path behavior identical.
3. **Phase 2 — read-only behavior.** Apply §4.3 table; add `ErrorKind::ReadOnly`;
   short-circuit `check_read_compaction`; fix `close`/`Drop`.
4. **Phase 3 — hardening.** Sever `check_read_compaction` coupling behind a mode
   check free of the read hot path; write entry points return `ReadOnly`.

## 7. Risks & verification

- **Regression of the writable hot path**: gated by `if !read_only`, so the
  audit should diff "writable behavior before/after" via the existing
  `crash_recovery` / `e2e_scenarios` / `proptest_db` suites — no changes
  expected.
- **Accidental write in read-only**: add a canary that wraps the store dir and
  asserts zero `write`/`create`/`unlink` syscalls during read-only
  open+read+drop (`test-utils`). This is the acceptance test.
- **WAL-with-residual-data**: new test opens RO over a store that was killed
  mid-write (reuse `DB::simulate_crash`), asserts committed keys are readable
  and the WAL file is untouched.
- **`remove_orphan_files` / MANIFEST edit are the two "silent" writes** most
  likely to be missed — covered explicitly by the canary, not by unit review.

## 8. Open questions

- Should `open_read_only` refuse when `CURRENT` is absent (recommended) or
  implicitly treat it as "empty DB"? Recommended: refuse, mirroring `db.rs:552`.
- Do we need cooperative `flock(LOCK_SH)` against a live writer, or is
  "proceed unlocked" acceptable for v1? Recommended: SH when the file exists,
  unlocked otherwise — cheap and useful.

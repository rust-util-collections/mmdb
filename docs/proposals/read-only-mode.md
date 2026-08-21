# Proposal: Read-Only Open Mode

Status: **Implemented** · Author: ktmlm · Scope: `DB::open` family, read path,
mutation guards, shutdown path

## 1. Goal

The implemented mode lets an existing DB be opened on a **read-only medium**
(RO filesystem, read-only permissions, immutable snapshot mount) and read
from, with **zero write-intent syscalls targeting the store directory** from
open through drop. Before this mode, `DB::open` created/opened writable files
and performed recovery writes before any read could succeed.

Read-only mode guarantees:

- `CURRENT`, MANIFEST, SST, and recoverable WAL state are visible to reads;
- open, reads, `close`, and `Drop` do not create, append, truncate, sync,
  rename, or unlink files in the store;
- disk-mutating APIs fail with `ErrorKind::ReadOnly` before side effects;
- the existing writable path retains its lock-before-recovery ordering and
  behavior.

Non-goals:

- reading concurrently with a live writer when no cooperative `LOCK` file is
  available;
- making a mutable directory snapshot consistent; the caller must provide a
  stable snapshot or successful cooperative lock;
- removing or slowing the existing writable fast path.

## 2. Current write-capability inventory

### 2.1 Writable open — `DB::open_impl`

| Site | Operation |
|------|-----------|
| `DB::open_impl` writable preflight | `create_dir_all`; create/open `LOCK` writable and take `flock(LOCK_EX)` |
| `VersionSet::recover_with_cache_mode` | reopen and truncate MANIFEST to its valid tail |
| `DB::open_impl` writer-arming branch | create the fresh WAL; flush recovered WAL state to SST |
| `DB::open_impl` writer-arming branch | append and sync the new log number in MANIFEST |
| `DB::remove_orphan_files` | unlink obsolete files |
| `DB::open_impl` worker loop | spawn workers that can compact and write |

The existing exclusive lock is acquired **before** MANIFEST/WAL recovery. That
ordering is a correctness boundary and must not be moved into a post-recovery
"arm writer" phase.

### 2.2 Read and memory-only paths

`get_with_options` periodically calls `check_read_compaction()`. It adds
level-≥2 hints and signals a compaction worker, so a point read can indirectly
cause store writes.

Snapshot tracking, cache population, statistics, and iterator state mutate
memory but not the store and remain allowed. They must not be confused with
the zero-store-write guarantee.

### 2.3 Explicit and indirect mutation APIs

Disk-mutating public entry points are `put*`, `delete*`, `delete_range*`,
`write*`, `flush`, `compact`, and `compact_range`.

`lazy_delete` and `lazy_delete_batch` are also logically mutating operations:
they update `dead_keys` and can request a background force-rewrite. Their
current `()` return type cannot report `ErrorKind::ReadOnly`; §4.5 defines the
compatibility policy.

### 2.4 Shutdown

- `close()` can flush the recovered/active memtable to SST, rotate WAL, append
  and sync MANIFEST, and unlink the retired WAL.
- `Drop` syncs the WAL when one is present, then releases workers, caches, and
  the directory lock.

## 3. Architecture

Keep one `DB` type with a private `read_only: bool` capability flag rather than
adding a parallel `ReadOnlyDB` API or changing the exhaustively constructible
public `DbOptions` struct. The read-visible state is already represented by
`SuperVersion`; writable resources can be absent or inert:

- `DBInner::wal_writer` is already `Option<WalWriter>`;
- `VersionSet::manifest_writer` is already
  `Arc<Mutex<Option<WalWriter>>>`;
- `compaction_handles` can be empty;
- write queues, compaction notifications, and dead-key state can remain
  allocated but inactive.

The structural change is to split `open` into **preflight/lock**, **read-state
recovery**, and **optional writer arming**. Recovery is not the first phase:
both writable and cooperatively locked read-only opens must lock before they
read mutable metadata.

## 4. Design

### 4.1 API and option semantics

```rust
impl DB {
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_read_only_with_options(DbOptions::default(), path)
    }

    pub fn open_read_only_with_options(
        options: DbOptions,
        path: impl AsRef<Path>,
    ) -> Result<Self> {
        Self::open_impl(options, path, true)
    }
}
```

The read-only entry points:

- the DB must already exist; `CURRENT` missing is an error;
- `create_if_missing` and `error_if_exists` are normalized to `false`;
- writer-only tuning options are retained in `DbOptions` but never activate
  writer machinery or writer-only validation;
- callers needing non-default read/cache settings use
  `DB::open_read_only_with_options`.

Keeping the mode out of `DbOptions` preserves source compatibility for
downstream exhaustive struct literals. `DB::open` remains the writable entry
point; the private `open_impl` receives the capability explicitly.

### 4.2 Three-phase open

```text
open_impl(options, path, read_only)
  ├─ preflight_and_lock(path, mode)
  │    RW: create directory if allowed; create/open LOCK RW; flock(EX)
  │    RO: require CURRENT; open existing LOCK read-only; flock(SH)
  │        if LOCK is absent, continue only under documented snapshot semantics
  ├─ recover_read_state(path, mode)
  │    read CURRENT → replay MANIFEST → open live SST handles
  │    enumerate WALs → replay recoverable records into active_memtable
  │    RO: manifest_writer = None; never truncate a torn MANIFEST tail
  └─ arm_write_capability()                 // RW only
       create fresh WAL
       flush non-empty recovered memtable to SST
       log_and_apply(new log_number) + sync_manifest
       remove_orphan_files
       spawn compaction workers
```

Requirements:

1. Preserve the writable path's existing lock-before-recovery order.
2. Acquire an existing read-only `LOCK` with `LOCK_SH | LOCK_NB` **before**
   reading CURRENT/MANIFEST. A writer's `LOCK_EX` and other shared readers then
   cooperate correctly.
3. If `LOCK` is absent, do not create it in read-only mode. An unlocked open is
   permitted only because live-writer sharing is a non-goal; document that the
   underlying snapshot must be stable.
4. Read-only mode calls an explicit recover-only VersionSet entry point. It
   must not call `open_with_cache`, whose missing-CURRENT branch creates a new
   MANIFEST, even after a separate `exists()` check.
5. Keep recovered WAL contents in `active_memtable`. Publish them through the
   initial `SuperVersion`; do not flush or clear them.

The `flock` guarantees above apply on Unix. On targets without Unix `flock`,
both read-only and writable handles lack OS-level exclusion, so callers must
treat a read-only open as the unlocked snapshot case.

### 4.3 VersionSet read-only state

Extend `recover_with_cache` (or add a named recover-only wrapper) so read-only
recovery leaves `manifest_writer = None` and does not call
`WalWriter::open_append_truncated`.

`None` must become a fail-closed state, not a silent no-op:

- `VersionSet::log_and_apply` checks for a writer at function entry, before
  opening new SSTs, directory fsync, in-memory version changes, or manifest
  rotation;
- `VersionSet::sync_manifest` returns `ErrorKind::ReadOnly` on `None`;
- `confirm_manifest_durable` also rejects a `None` writer instead of treating
  an empty handle as a successful sync.

These checks are defensive backstops. Correct read-only control flow never
reaches them.

### 4.4 DB state construction

For read-only mode:

- set `wal_writer: None`;
- retain the recovered `versions.log_number()` as `wal_number` rather than
  using the invalid sentinel `0` (changing `wal_number` to `Option<u64>` is
  preferable if the wider refactor is acceptable);
- set `committed_sequence` to the maximum of MANIFEST and recovered WAL
  sequences;
- initialize the otherwise-unused next-write sequence consistently with the
  recovered state, so shared read/snapshot helpers retain existing invariants;
- leave `compaction_handles` empty and do not send the startup compaction
  signal.

### 4.5 Mutation guards

Keep `check_usable()` mode-neutral because all read APIs call it. Add a
separate guard:

```rust
fn check_writable(&self) -> Result<()> {
    self.check_usable()?;
    if self.read_only {
        return Err(Error::read_only());
    }
    Ok(())
}
```

Use `check_writable()` at every disk-mutating public entry point and again at
central internal mutation gateways such as `write_batch_group`. The redundant
internal guard protects future call sites. Define whether an empty
`WriteBatch` returns `ReadOnly` or `Ok(())`; recommended behavior is
`ReadOnly`, because the method itself is unavailable in this mode.

For `lazy_delete` and `lazy_delete_batch`, the recommended compatible v1
behavior is a documented no-op in read-only mode. They cannot affect reads
until compaction and cannot report an error without breaking their signatures.
Add fallible `try_lazy_delete* -> Result<()>` variants if callers need explicit
feedback; changing the existing signatures should be reserved for a major
release. `clear_dead_keys` remains an allowed memory-only cleanup operation.

### 4.6 Read path, close, and Drop

- Short-circuit `maybe_check_read_compaction` before counters, hints, or
  notification work when read-only. With no workers this is also a bounded
  memory requirement, not only a write-prevention measure.
- `signal_compaction` should defensively no-op in read-only mode.
- `close()` skips `freeze_and_flush` and WAL sync, then performs ordinary
  in-memory shutdown and releases the shared lock/caches.
- `Drop` already skips WAL sync when `wal_writer` is `None`; keep that invariant
  explicit in tests.

### 4.7 Error type

Add `ErrorKind::ReadOnly` and `Error::read_only()`. It represents a capability
error and must not be conflated with an `EROFS` I/O error. Add the new variant
to `ErrorKind::as_str` and error-kind tests.

## 5. Residual WAL semantics

If the medium contains committed-but-unflushed WAL data, replaying it into the
memtable is required for correctness. The existing recovery loop already
replays eligible WALs before its flush step.

Read-only behavior is therefore:

- replay every WAL selected by the existing
  `wal_number >= versions.log_number()` rule in file-number order;
- preserve the existing strict corruption checks and recoverable active-tail
  tolerance;
- retain replayed point entries, deletions, and range deletions in the active
  memtable for the DB lifetime;
- expose the maximum recovered committed sequence to normal reads and
  snapshots;
- leave every WAL byte unchanged and repeat recovery on the next open.

Silently ignoring WALs or refusing every store with residual WAL would make
read-only snapshots unnecessarily incomplete. A corrupt non-recoverable WAL
continues to fail open exactly as it does in writable mode.

## 6. Implementation phases

1. **Capability seams, no writable behavior change.** Add the private DB
   `read_only` capability, `ErrorKind::ReadOnly`, `check_writable`, and
   fail-closed optional MANIFEST writer semantics. Keep the writable open flow
   byte-for-byte equivalent.
2. **Split open without reordering the lock.** Extract preflight/lock and
   recovery helpers, still exercising only the writable mode in tests.
3. **Construct read-only state.** Add shared/read-only lock handling, explicit
   recover-only VersionSet flow, residual-WAL memtable publication, and inert
   background state.
4. **Close all mutation paths.** Gate public/internal writers, read-triggered
   compaction, lazy-delete behavior, `close`, and `Drop`.
5. **Acceptance hardening.** Add immutable-file and syscall-level tests, then
   run all existing crash-recovery, end-to-end, and property suites unchanged.

## 7. Verification

### 7.1 Functional matrix

Cover at least:

- SST-only DB on read-only permissions;
- residual WAL containing puts, point deletes, and range deletes;
- recoverable torn WAL tail and recoverable torn MANIFEST tail;
- non-recoverable corruption still failing closed;
- `CURRENT` absent, `LOCK` absent, and `LOCK` present;
- multiple shared readers;
- shared reader rejected while a writer holds `LOCK_EX`, and writer rejected
  while readers hold `LOCK_SH`;
- every disk mutation API returning `ErrorKind::ReadOnly` before side effects;
- `close` and implicit `Drop` with a non-empty recovered memtable;
- writable regression suites: `crash_recovery`, `e2e_scenarios`, and
  `proptest_db`.

### 7.2 Zero-write acceptance

The repository currently calls `std::fs` directly, so `test-utils` cannot
literally wrap the store directory without first introducing a filesystem
abstraction. Use two complementary checks:

1. A portable integration test snapshots the recursive file set, content
   hashes, sizes, and modification times before read-only open/read/close/drop
   and asserts they are unchanged afterward.
2. A Linux acceptance test runs that scenario under `strace` (following
   threads) and rejects store-targeted `open`/`openat` calls with write/create/
   truncate flags, plus write/pwrite/truncate/rename/unlink/fsync/fdatasync
   operations. This catches transient create-then-delete behavior that an
   after-the-fact hash cannot see.

A permissions-only test is useful for demonstrating operation on an RO medium,
but is not sufficient evidence of zero attempted writes and may be ineffective
when tests run with elevated privileges.

## 8. Resolved policy decisions

- `CURRENT` absent: refuse read-only open; never create an empty DB.
- Existing `LOCK`: take non-blocking `LOCK_SH` before recovery.
- Missing `LOCK`: proceed unlocked only for a caller-provided stable snapshot;
  concurrent live writers remain unsupported.
- Residual WAL: replay and retain in memory.
- Existing `lazy_delete*`: documented no-op in read-only v1; fallible variants
  may be added separately.
- One `DB` type: retained; capability checks provide runtime enforcement.

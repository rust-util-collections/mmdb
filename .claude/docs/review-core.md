# MMDB Review Core

Evidence standard and subsystem map. Apply `pragmatic-engineering.md`: only
findings/process that remove a concrete failure mode.

## 1. Context

1. Full diff + surrounding functions.
2. Map each changed code file via Subsystem Map; load guides, callers, tests.
3. Design-shaped / multi-subsystem diffs → also `design-patterns.md`.
4. Full audit → tracked-file ledger first (no static/size guesses).

### Subsystem Map

One primary row per Rust file. Concurrency/unsafe are overlays.

| Subsystem | Files | Guides |
|-----------|-------|--------|
| write/read | `src/db.rs`, `options.rs`, `error.rs`, `stats.rs` | `technical-patterns.md`, `patterns/concurrency.md`, `unsafe-audit.md` (`db.rs`) |
| memtable | `src/memtable/**` | `patterns/memtable.md`, `unsafe-audit.md` |
| WAL | `src/wal/**` | `patterns/wal.md` |
| SST | `src/sst/**` | `patterns/sst.md`, `unsafe-audit.md` if `unsafe` |
| iterator | `src/iterator/**` | `patterns/iterator.md` |
| compaction | `src/compaction/**` | `patterns/compaction.md` |
| manifest | `src/manifest/**` | `patterns/manifest.md`, `concurrency.md` |
| cache | `src/cache/**` | `patterns/cache.md`, `concurrency.md` |
| types/API | `types.rs`, `lib.rs`, `rate_limiter.rs` | `technical-patterns.md` |

Guides under `.claude/docs/patterns/`. Tests/benches/CI/docs/`.claude` → map to
the subsystem they cover; check alignment.

## 2. Risk (effort, not a finding)

| Class | Examples | Default |
|-------|----------|---------|
| Conc/unsafe | atomics, raw ptr, lock order, threads | CRITICAL |
| Durability | WAL, MANIFEST, SST, checksums | HIGH |
| Control/resource | branches, open/close, cleanup | HIGH |
| API/behavior | exports, defaults, iterator | HIGH |
| Errors | propagate, fail-stop, retry | MEDIUM |
| Perf | hot/warm complexity, locks, I/O | context |
| Tests/docs/config | coverage/alignment | LOW unless wrong |

## 3. Evidence

For each risky change:

1. Name the invariant (mapped guide).
2. Build a realistic trigger (input, order, crash, corrupt boundary).
3. Trace full path (callers, cleanup, existing guards).
4. State outcome: wrong value, loss, corruption, panic, leak, deadlock, or **quantified** hot-path cost.
5. Note smallest regression test that would fail pre-fix.

**Boundaries:** empty/single entry, first/last block keys, restarts, L0 limits,
snapshots, malformed disk data, max sizes, partial I/O errors.

**Concurrency:** Build lock/atomic protocol from code. Cycles, guard lifetime,
publication, wait predicates, shutdown. `Relaxed` OK for counter/hints; Acquire/Release only when a happens-before edge is required.

**Crash:** Crash points on write/sync/dir-sync/MANIFEST/CURRENT/delete.
MANIFEST append ≠ CURRENT replace (`patterns/manifest.md`).

**Perf:** Hot/warm only; quantify cost and path class. Cold micro-opts are not findings.

**Design:** Locks, ownership, queues/fan-out, multi-step install, degrade, exports
→ applicable D-\* in `design-patterns.md`; skip empty families.

**Placeholder (CRITICAL in non-test prod when it ships behavior):**
`todo!` / `unimplemented!` / stand-in `unreachable!`; dummy returns where real
work is required; `// TODO|FIXME|HACK` for unfinished required behavior;
`if false` / `#[cfg(any())]` around incomplete required paths. Grep before
calling dead code. Cleanup-only → LOW/skip.

**API:** Only `src/lib.rs` re-exports are public. Align behavior, defaults,
errors, docs, tests when they change.

## 4. Deterministic / style

fmt / compile / clippy → tools, not agents. Still LOW if tools miss:

- no `#[allow(...)]`
- import repeated paths; group prefixes
- public docs + `CLAUDE.md` + this map + guides stay aligned
- every unsafe has accurate `// SAFETY:`

## 5. Audit (`docs/audit.md`)

- Prune fixed in-scope `Open`.
- Re-check `Won't Fix` / `Rejected` when cited code/callers/assumptions/subsystem
  touched; full audit → all entries.
- Real but disproportionate → `Won't Fix` + current reason.
- Material disproven → `Rejected` (no severity). Drop routine noise.
- No dates/freshness markers.

```text
[SEVERITY] subsystem: summary
WHERE: file:line_range
TRIGGER: input/order/failure
OUTCOME: observable wrong behavior
WHY: invariant + why guards fail
FIX: minimal direction + regression test
```

- **CRITICAL**: loss/corruption, UB, memory safety, unrecoverable durability
- **HIGH**: wrong results, deadlock, realistic crash, exhaustion, material hot-path hit
- **MEDIUM**: edge bug, error-policy gap, bounded leak
- **LOW**: convention/docs with real maintenance cost

Observations ≠ `## Open`.

## Quality gate

Concrete trigger + outcome only. Refute via `false-positive-guide.md`. Agent
agreement and pattern IDs are not proof.

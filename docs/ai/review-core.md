# MMDB Review Core

Evidence standard, subsystem map, and subagent handoff rules. Apply
`pragmatic-engineering.md`: keep only findings and steps that eliminate a
concrete storage failure mode. Specify the exact local API/file operation,
invariant, and expected vs observed result on MMDB's embedded data structures
and local files.

## 1. Context

1. Read the full diff and surrounding functions, callers, error paths, and tests.
2. Map each Rust file via the Subsystem Map and load the corresponding guides.
3. Multi-subsystem or design-shaped diffs → also load `design-patterns.md`.
4. Full `all` review → build a complete tracked-file ledger first across `src/`,
   `tests/`, `benches/`, `examples/`, build/CI configs, public docs, `.agents/`,
   and `docs/ai/` (no size guesses or silent omissions).

### Subsystem Map

One primary row per Rust file; concurrency and `unsafe` guides apply as overlays
where relevant. In `all` scope, large files (`src/db.rs`,
`src/compaction/leveled.rs`) may be partitioned into disjoint line-range
sections in the coordinator's ledger so each section has one read-only reviewer.

| Subsystem | Files | Guides |
|-----------|-------|--------|
| write/read | `src/db.rs`, `src/options.rs`, `src/error.rs`, `src/stats.rs` | `technical-patterns.md`, `patterns/concurrency.md`, `patterns/unsafe-audit.md` (`db.rs`) |
| memtable | `src/memtable/**` | `patterns/memtable.md`, `patterns/unsafe-audit.md` |
| WAL | `src/wal/**` | `patterns/wal.md` |
| SST | `src/sst/**` | `patterns/sst.md`, `patterns/unsafe-audit.md` (`table_reader/mod.rs`) |
| iterator | `src/iterator/**` | `patterns/iterator.md` |
| compaction | `src/compaction/**` | `patterns/compaction.md` |
| manifest | `src/manifest/**` | `patterns/manifest.md`, `patterns/concurrency.md` |
| cache | `src/cache/**` | `patterns/cache.md`, `patterns/concurrency.md` |
| types/API | `src/types.rs`, `src/lib.rs`, `src/rate_limiter.rs` | `technical-patterns.md` |
| test allocator | `src/test_alloc.rs` (`#[cfg(test)]`) | `patterns/unsafe-audit.md` |

Guides live under `docs/ai/patterns/`. Map `tests/**`, `benches/**`,
`examples/**`, CI/Makefile, public docs, `.agents/**`, and `docs/ai/**` to the
subsystems they exercise or document and verify contract alignment.

## 2. Review priority (effort allocation, not automatic severity)

| Class | Focus | Default |
|-------|-------|---------|
| Concurrency / `unsafe` | atomics, arena/queue raw pointers, lock order, worker lifecycle | CRITICAL |
| Durability | WAL, MANIFEST, SST fsync/dir-sync order, CRC32 framing | HIGH |
| Control / resources | open/read-only/close paths, error propagation, cache/file cleanup | HIGH |
| API / semantics | `src/lib.rs` exports, `DbOptions` defaults, MVCC & iterator bounds | HIGH |
| Error handling | fail-stop latch (`VersionSet::poisoned`), typed `ErrorKind` | MEDIUM |
| Performance | quantified hot/warm-path allocations, lock contention, extra I/O | context |
| Tests / docs / build | regression coverage, doc-code alignment, CI/gate parity | LOW unless broken |

## 3. Evidence

For every candidate defect:

1. Name the storage invariant (citing the mapped guide).
2. State concrete local conditions: API call sequence, key/sequence ordering,
   interrupted write / file truncation, or invalid stored length/checksum field.
3. Trace the full execution path through callers, cleanup, and existing guards.
4. State the observable result: wrong value/visibility, lost acknowledged data,
   unhandled decode error or panic, leaked resource/pin, deadlock/stall, or
   **quantified** hot/warm-path cost.
5. Specify the smallest deterministic regression test (in a `tempfile` DB) that
   fails before the fix.

**Boundaries:** empty/single-entry blocks, first/last keys, restart intervals,
half-open `[start, end)` ranges, L0 count triggers, MVCC snapshot stripes, max
key/entry constants, truncated records, partial I/O errors.

**Concurrency:** derive the lock and atomic graph from live code (`inner` mutex,
`write_queue`, `manifest_writer`, `pin_updates`, `snapshots`, condvars). Verify
acyclic lock order, guard lifetimes across disk I/O, `Release`/`Acquire`
publication edges, loop-rechecked wait predicates, and clean worker shutdown.

**Durability:** check state transitions across write, file sync, directory sync,
MANIFEST append, `CURRENT` rename, and obsolete-file unlink (`patterns/manifest.md`).

**Performance:** report hot/warm-path regressions only with path class and
concrete allocation, lock, or I/O cost; skip cold-path micro-optimizations.

**Design:** walk applicable `D-*` checks in `design-patterns.md` for locks,
resource lifecycles, bounded queues/memory, multi-step state transitions, error
visibility, and public exports.

**Placeholders (CRITICAL in shipped paths when required behavior is missing):**
`todo!`, `unimplemented!`, stand-in `unreachable!`, stub returns, or unfinished
`TODO`/`FIXME` on active code paths. Search callers before labeling code dead.

**Public API:** only items re-exported in `src/lib.rs` are public. Keep code,
defaults, typed errors, doc comments, `README.md`, and tests aligned.

## 4. Deterministic checks vs review findings

`rustfmt`, `cargo check`, and `cargo clippy -- -D warnings` are automated gates,
not audit registry findings. Import style, naming, and formatting are never
registry entries.

Record static-inspection findings only for rules automated tools cannot check:

- `#[allow(...)]` attributes added to suppress warnings instead of fixing them
- public behavior, defaults, `ErrorKind`, or docs that disagree with the code
- missing, vague, or inaccurate `// SAFETY:` preconditions on `unsafe` blocks

## 5. Findings registry (`docs/audit.md`)

Severities: `CRITICAL`, `HIGH`, `MEDIUM`, `LOW` only.

- **CRITICAL**: silent data loss, wrong stored/returned bytes, broken Rust
  `unsafe` precondition, or unrecoverable on-disk state after an interrupted write
- **HIGH**: wrong query/iterator results, deadlock, realistic recovery failure,
  unbounded memory/FD growth, or material hot-path regression
- **MEDIUM**: boundary-case bug, error-propagation gap, or bounded resource leak
- **LOW**: contract, test, or documentation drift with concrete maintenance cost

Disposition permissions follow `workflow-policy.md` §5.

`Open` entry format:

```markdown
### [SEVERITY] subsystem: summary
- **Where**: file:line_range
- **What**: expected behavior and observed result
- **Why**: concrete conditions, invariant, and why existing guards miss it
- **Fix**: minimal root-cause correction
```

`Won't Fix` uses `Where`, `What`, and `Reason`. `Rejected` uses `Where`,
`Claim`, and `Reason` (no severity). Omit feature requests, documented
contracts, and style notes.

## Agent handoff and response

Prefer direct in-session review for small or single-subsystem scopes. When
delegating read-only subsystem batches (`all` or large multi-subsystem diffs),
pass this self-contained handoff with a concrete storage-invariant question:

```text
Context: MMDB is an in-process embedded Rust LSM key-value library (vsdb storage
backend) operating only on caller-owned local database files; there is no
network service, daemon, IPC endpoint, or external privilege boundary.
Task: Verify MMDB storage-engine invariants in the assigned files/line ranges.
Repository: <absolute path>
Baseline and scope: <HEAD, frozen scope, dirty baseline paths to leave untouched>
Assigned files: <disjoint file list, or file:line_range sections for large files>
Read first: <absolute paths to workflow-policy.md, review-core.md,
             false-positive-guide.md, and mapped subsystem guides>
Mode: Read-only code inspection; do not edit files or run git commit/reset.
Invariants to check: <concrete storage invariants, e.g., MVCC sequence cutoff,
                      prefix-block restart decoding, WAL/MANIFEST record replay,
                      or lock acquisition order>
Test context: <isolated tempfile::TempDir and deterministic local API/file ops,
               or static code tracing only when test execution is not assigned>
Verified context: <relevant facts already established, or none>
Return: Reviewed files/line ranges; confirmed section-5 entries (if any) with
exact operation/input, state, expected vs observed result, and existing guards
checked; test command + output if executed; any assigned scope left unreviewed.
```

Establish expected behavior from MMDB's API contract or a reference model
(including seek position, half-open `[lower, upper)` bounds, and snapshot
visibility) before classifying behavior as a defect. Distinguish executed test
runs from static code tracing.

## Quality gate

Every kept finding must cite concrete local conditions, verified code paths, and
expected vs observed results, and must survive `false-positive-guide.md`.
Subagent consensus or pattern labels alone are never proof.

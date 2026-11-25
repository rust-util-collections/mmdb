# MMDB Review Core

Evidence standard, subsystem map, and subagent handoff rules. Apply
`pragmatic-engineering.md`: keep only findings and steps that eliminate a
concrete storage failure mode. Specify the exact local API/file operation,
invariant, and expected vs observed result on MMDB's embedded data structures
and local files.

Use the [architecture support policy](workflow-policy.md#supported-architectures)
when checking platform-specific claims. Name the affected target and distinguish
static evidence from a native test result. Record confirmed defects confined
to unsupported architectures under `Won't Fix`.

## 1. Context

1. Read the full diff and surrounding functions, callers, error paths, and tests.
2. Map each Rust file via the Subsystem Map and load the corresponding guides.
3. Multi-subsystem or design-shaped diffs → also load `design-patterns.md`.
4. For an `all` review, derive the tracked-file ledger from `git ls-files`.
   Include reader guides, design records, project skills and their UI metadata,
   alongside the source, tests, examples, benchmarks, and build configuration.

### Documentation map

| When checking | Compare against |
|---------------|-----------------|
| Public operations, errors, defaults | [Usage guide](../usage.md), public rustdoc on the items re-exported by `src/lib.rs`, and the corresponding API tests |
| Read, write, recovery, format, or resource behavior | [Storage architecture](../architecture.md), the subsystem implementation and its tests |
| Store and caller migration | [Migration guide](../migration.md), `tools/v4-export/src/main.rs` and its golden-store tests |
| Historical measurements or proposed designs | [Design records](../proposals/) and their cited revisions. Use the current implementation to establish what shipped |
| Skill scope, outputs, and validation | [Project guide](../../AGENTS.md#skills), the four skill entry points and their `agents/openai.yaml`, [workflow policy](workflow-policy.md) and [commit protocol](commit-protocol.md) |

The [README](../../README.md#documentation) is the reader index. Update the
page that explains a changed contract and check links to moved sections.
For documentation drift, cite the discrepant claim and the API declaration,
operation, test, or Cargo target that establishes the correction.

### Subsystem Map

One primary row per Rust file; concurrency and `unsafe` guides apply as overlays
where relevant. In `all` scope, large files (`src/db.rs`,
`src/compaction/leveled.rs`, `src/sst/table_reader/{mod.rs,iterator.rs}`,
`src/sst/table_builder.rs`, `src/iterator/db_iter.rs`,
`src/manifest/version_set.rs`) may be partitioned into disjoint line-range
sections in the coordinator's ledger so each section has one read-only reviewer.

| Subsystem | Files | Guides |
|-----------|-------|--------|
| write / flush / lifecycle | `src/db.rs`, `src/db/directory.rs`, `src/db/flush.rs`, `src/options.rs`, `src/error.rs`, `src/stats.rs` | `technical-patterns.md`, `patterns/concurrency.md`, `patterns/wal.md`, `patterns/manifest.md`, `patterns/unsafe-audit.md` (`db.rs`) |
| sorted ingest | `src/db/ingest.rs` (ascending disjoint span, one sequence, bottommost install) | `patterns/manifest.md`, `patterns/concurrency.md` |
| read / snapshots / pins | `src/db/read.rs`, `src/db/snapshot.rs`, `src/pinned.rs` | `technical-patterns.md`, `patterns/concurrency.md`, `patterns/iterator.md`, `patterns/unsafe-audit.md` (`pinned.rs`) |
| memtable | `src/memtable/**` | `patterns/memtable.md`, `patterns/unsafe-audit.md` |
| WAL | `src/wal/**` | `patterns/wal.md` |
| SST v2 | `src/sst/**` | `patterns/sst.md`, `patterns/unsafe-audit.md` (`table_reader/mod.rs`) |
| iterator | `src/iterator/**` | `patterns/iterator.md` |
| compaction | `src/compaction/**` | `patterns/compaction.md`, `patterns/concurrency.md` |
| manifest | `src/manifest/**` | `patterns/manifest.md`, `patterns/concurrency.md` |
| cache | `src/cache/**` | `patterns/cache.md`, `patterns/concurrency.md` |
| types / API | `src/types.rs`, `src/lib.rs`, `src/rate_limiter.rs` | `technical-patterns.md` |
| v4 migration tool | `tools/v4-export/**` | `patterns/manifest.md`, `patterns/iterator.md` |
| test allocator | `src/test_alloc.rs` (`#[cfg(test)]`), `tests/arena_miri.rs` | `patterns/unsafe-audit.md` |

Guides live under `docs/ai/patterns/`. Map `tests/**` (`crash_recovery`,
`integration`, `e2e_scenarios`, `proptest_db`, `bidi_debug`, `lazy_delete`,
`shared_cache`, `read_only`, `arena_miri`), `benches/**`, `examples/**`,
CI/Makefile/`.gitignore`, public docs, `.agents/**`, and `docs/ai/**` to the
subsystems they exercise or document and verify contract alignment.

## 2. Review priority (effort allocation, not automatic severity)

| Class | Focus | Default |
|-------|-------|---------|
| Concurrency / `unsafe` | Stacked/Tree Borrows provenance, atomics, lock order, `FlushQueue` / `MaintenanceState` / worker lifecycle | CRITICAL |
| Durability | `DurabilityMode` + `WalFence`, `DirSynced`, MANIFEST `DBHeader`/sync, multi-WAL recovery, CRC32 framing | HIGH |
| Control / resources | open/read-only/close paths, `PendingSsts`, `SnapshotLease` (`Weak<ReadState>`), cache/FD cleanup | HIGH |
| API / semantics | `src/lib.rs` exports, `DbOptions` builders/defaults, MVCC & `[lower, upper)` bounds, `step_next`/`step_prev` | HIGH |
| Error handling | fail-stop latches (`VersionSet::poisoned`, `ReadState::has_bg_error`, `FlushQueue::fail`), typed `ErrorKind` (`Busy`, `Background`, `Corruption`, `ReadOnly`, `DbClosed`) | MEDIUM |
| Performance | quantified hot/warm-path allocations, lock hold across I/O, extra partition leaf reads | context |
| Tests / docs / build | regression coverage, doc-code alignment, `tools/v4-export` & CI/gate parity | LOW unless broken |

## 3. Evidence

For every candidate defect:

1. Name the storage invariant (citing the mapped guide).
2. State concrete local conditions: API call sequence, key/sequence ordering,
   interrupted write / file truncation, or invalid stored length/checksum field.
3. Trace the full execution path through callers, cleanup, and existing guards.
4. State the observable result: wrong value/visibility, lost acknowledged data,
   post-commit `Err` on already-applied writes, unhandled decode error or panic,
   leaked resource/pin, deadlock/stall, or **quantified** hot/warm-path cost.
5. For a storage defect, specify the smallest deterministic regression test
   that fails before the fix. For a documentation discrepancy, identify the
   code, example, test, or link that establishes the expected result.

**Boundaries:** empty/single-entry blocks, first/last keys, restart intervals,
partitioned index/`rangedel` leaf boundaries vs directory entries, half-open
`[start, end)` ranges, L0 count triggers & `l0_install_epoch`, MVCC snapshot
stripes (`MAX_RANGE_STRIPES`), `META_BLOCK_SPLIT_THRESHOLD` / `MAX_USER_KEY_SIZE`
constants, truncated records, partial or post-commit I/O errors.

**Concurrency:** derive the lock and atomic graph from live code (`lock_file`,
`maintenance`, `write_queue`, `inner`, `compacting_files`, `snapshot_list`,
`manifest_writer`, `flush_queue`, `pin_updates`, `SstBuilds`, condvars). Verify
acyclic lock order, unlocked SST builds and WAL sealing, `SuperVersion`-before-`committed_sequence`
read barriers, claim release before condvar notify, fail-stop flag store before
waiter wake, loop-rechecked wait predicates, and clean worker shutdown.

**Durability:** check state transitions across SST sync, `DirSynced`, MANIFEST
append, `confirm_manifest_durable`, `CURRENT` rename, WAL rotation, and
obsolete-file unlink (`patterns/manifest.md`, `patterns/wal.md`).

**Performance:** report hot/warm-path regressions only with path class and
concrete allocation, lock, or I/O cost. Assess maintenance overhead by its
effect on normal reads and writes. [Performance priorities](pragmatic-engineering.md#performance-priorities)

**Design:** walk applicable `D-*` checks in `design-patterns.md` for locks,
resource lifecycles, bounded queues/memory, multi-step state transitions, error
visibility, and public exports.

**Placeholders (CRITICAL in shipped paths when required behavior is missing):**
`todo!`, `unimplemented!`, stand-in `unreachable!`, stub returns, or unfinished
`TODO`/`FIXME` on active code paths. Search callers before labeling code dead.

**Public API:** only items re-exported in `src/lib.rs` are public. Keep code,
defaults, typed errors, item rustdoc, [reader documentation](#documentation-map), and tests aligned.

## 4. Deterministic checks vs review findings

`rustfmt`, `cargo check`, and `cargo clippy -- -D warnings` are automated gates,
not audit registry findings. Import style, naming, and formatting are never
registry entries.

Record static-inspection findings only for rules automated tools cannot check:

- `#[allow(...)]` attributes added to suppress warnings instead of fixing them
- public behavior, defaults, `ErrorKind`, or docs that disagree with the code
- missing, vague, or inaccurate `// SAFETY:` preconditions on `unsafe` blocks
- CI / `Makefile` / `.gitignore` omissions that skip a standalone crate such as `tools/v4-export`

## 5. Findings registry (`docs/audit.md`)

Severities: `CRITICAL`, `HIGH`, `MEDIUM`, `LOW` only.

- **CRITICAL**: silent data loss, wrong stored/returned bytes, broken Rust
  `unsafe` precondition, or unrecoverable on-disk state after an interrupted write
- **HIGH**: wrong query/iterator results, deadlock, realistic recovery failure,
  unbounded memory/FD growth, or material hot-path regression
- **MEDIUM**: boundary-case bug, post-commit or typed `ErrorKind` propagation
  gap, missing on-disk metadata cross-check, or bounded resource/wakeup race
- **LOW**: contract, test, build-gate, or documentation drift with concrete maintenance cost

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
                      SST v2 directory-leaf bounds, WAL/MANIFEST record replay,
                      Stacked/Tree Borrows provenance, or lock acquisition order>
Test context: <isolated tempfile::TempDir and deterministic local API/file ops,
               or static code tracing only when test execution is not assigned>
Verified context: <relevant facts already established, or none>
Return: Reviewed files/line ranges; confirmed section-5 entries (if any) with
exact operation/input, state, expected vs observed result, and existing guards
checked; test command + output if executed; any assigned scope left unreviewed.
```

Establish expected behavior from MMDB's API contract or a reference model
(including seek position, half-open `[lower, upper)` bounds, lending vs
`Iterator` error rules, and snapshot visibility) before classifying behavior as
a defect. Distinguish executed test runs from static code tracing.

## Quality gate

Every kept finding must cite concrete local conditions, verified code paths, and
expected vs observed results, and must survive `false-positive-guide.md`.
Subagent consensus or pattern labels alone are never proof.

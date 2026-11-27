# MMDB Review Core

Use the subsystem map to select the invariants for a change. For a storage
finding, name the API or file operation, the expected result and the
observed failure. For documentation or workflow changes, identify the
ambiguity or failure the edit removes and cite the contract or check that
establishes the intended behavior.

Use the [architecture support policy](workflow-policy.md#supported-architectures)
when checking platform-specific claims. Name the affected target and distinguish
static evidence from a native test result. Record confirmed defects confined
to unsupported architectures under `Won't Fix`.

## 1. Context

1. Read the full diff and surrounding functions, callers, error paths, and tests.
2. Map each Rust file via the Subsystem Map and load the corresponding guides.
3. For multi-subsystem or design-shaped diffs, apply the [design checks](design-patterns.md).
4. For an `all` review, derive the tracked-file ledger from `git ls-files`.
   Include reader guides, benchmark reports, review guides, project skills and
   their UI metadata, alongside the source, tests, examples, benchmarks, and
   build configuration.

### Documentation map

| When checking | Compare against |
|---------------|-----------------|
| Public operations, errors, defaults | [Usage guide](../usage.md), public rustdoc on the items re-exported by [the crate root](../../src/lib.rs), and the corresponding API tests |
| Read, write, compaction, or resource behavior | The relevant chapter in the [architecture subsystem map](../architecture.md#subsystem-map), its implementation, and its tests |
| Persistence and version compatibility | [Database files and recovery](../architecture/recovery.md), [upgrade guide](../migration.md), and the [native v5 fixture](../../tests/fixtures/v5.0.1/) |
| Engine performance, diagnostics, or historical comparisons | [Benchmark reports](../benchmarks/README.md) and [performance priorities](#performance-priorities). Match the revision, feature flags, workload and counter scope to the claim. |
| Skill scope, outputs, and validation | [Project guide](../../AGENTS.md#skills), the four skill entry points and their `agents/openai.yaml`, [workflow policy](workflow-policy.md) and [commit protocol](commit-protocol.md) |

The [documentation index](../README.md) connects the user guides, architecture
chapters, benchmark reports, and contributor workflows. Update the page that
explains a changed contract and check links to moved sections.
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
| write / flush / lifecycle | `src/db.rs`, `src/db/directory.rs`, `src/db/flush.rs`, `src/options.rs`, `src/error.rs` | [concurrency](patterns/concurrency.md), [WAL](patterns/wal.md), [MANIFEST](patterns/manifest.md), [unsafe code](patterns/unsafe-audit.md) (`db.rs`) |
| memory and diagnostics | `src/db/memory.rs`, `src/profile.rs`, `src/stats.rs`; L0 job counters belong to the compaction row | [cache](patterns/cache.md), [concurrency](patterns/concurrency.md), [performance priorities](#performance-priorities) |
| sorted ingest | `src/db/ingest.rs` (ascending disjoint span, one sequence, bottommost install) | [MANIFEST](patterns/manifest.md), [concurrency](patterns/concurrency.md) |
| read / snapshots / pins | `src/db/read.rs`, `src/db/snapshot.rs`, `src/pinned.rs` | [concurrency](patterns/concurrency.md), [iterator](patterns/iterator.md), [SST](patterns/sst.md) (grouped block reuse), [memtable](patterns/memtable.md) (arena-backed pins) |
| memtable | `src/memtable/**` and `tests/arena_miri.rs` (concurrent arena pins) | [memtable](patterns/memtable.md), [unsafe code](patterns/unsafe-audit.md) |
| WAL | `src/wal/**` | [WAL](patterns/wal.md) |
| SST v2 | `src/sst/**` | [SST](patterns/sst.md), [unsafe code](patterns/unsafe-audit.md) (`table_reader/mod.rs`) |
| iterator | `src/iterator/**` | [iterator](patterns/iterator.md) |
| compaction | `src/compaction/**` | [compaction](patterns/compaction.md), [concurrency](patterns/concurrency.md) |
| manifest | `src/manifest/**` | [MANIFEST](patterns/manifest.md), [concurrency](patterns/concurrency.md) |
| cache | `src/cache/**` | [cache](patterns/cache.md), [concurrency](patterns/concurrency.md) |
| types / API | `src/types.rs`, `src/lib.rs`, `src/rate_limiter.rs` | [Public contracts](../usage.md), [storage contract checks](#storage-contract-checks), [WAL](patterns/wal.md) (batch encoding), [memtable](patterns/memtable.md) (admission budgets), [compaction](patterns/compaction.md) & [concurrency](patterns/concurrency.md) (`rate_limiter.rs`) |
| test allocator | `src/test_alloc.rs` (test-only thread-local allocation counters) | [unsafe code](patterns/unsafe-audit.md) |

Guides live under `docs/ai/patterns/`. Map `tests/**` (`crash_recovery`,
`integration`, `e2e_scenarios`, `proptest_db`, `bidi_debug`, `lazy_delete`,
`shared_cache`, `grouped_reads`, `read_only`, `arena_miri`), `benches/**`, `examples/**`,
CI/Makefile/`.gitignore`, public docs, `.agents/**`, and `docs/ai/**` to the
subsystems they exercise or document. Under `docs/benchmarks/**`, distinguish
executable drivers and result validators from archived patches and raw data.
Use the recorded revisions to identify measured code.

### Storage contract checks

Select the row that matches the changed operation. These checks also locate
sibling callers for a fix and a focused regression test.

| Operation | What to compare | Evidence |
|---|---|---|
| Admit a batch or fence its WAL | A nonempty `no_slowdown` request can receive `Busy` at the L0 limit or from the immutable queue. Check the rejected member's sequence, WAL and rows alongside an admitted neighbor. A native `DB::sync_wal()` adds no user record or sequence. Exercise rotation and reopen on the requested durability path. | [Write admission](../../src/db.rs), [queue and fence tests](../../src/db/flush.rs), [recovery tests](../../tests/crash_recovery.rs) |
| Read a key, group or range | Compare owned, pinned and existence reads at one snapshot. `get_many_pinned_with_options` fixes one view and sequence for up to `MAX_GROUPED_READ_KEYS`, preserves duplicates and input order, and publishes the returned-byte count only after all slots succeed. For scans check lending errors and the owned iterator's terminal status. | [Read path](../../src/db/read.rs), [grouped SST lookup](../../src/sst/table_reader/mod.rs), [grouped-read tests](../../tests/grouped_reads.rs), [iterator contract](../usage.md) |
| Sample shared memory | `BlockCachePool::usage()` reports segmented and overflow charges once per pool. `DB::memory_usage()` reports each member's current memtables and L0 pin payload. Hold a returned pin through eviction or close and verify its contents independently of these gauges. | [Memory API](../../src/db/memory.rs), [cache](../../src/cache/block_cache.rs), [shared-pool tests](../../tests/shared_cache.rs) |
| Reopen an existing store | Check the MANIFEST format version, persisted level count and prefix length. Writable reopen must reject mismatched geometry. Read-only open adopts that geometry and replays eligible residual WAL records into memory. Check visible SST and WAL rows at reopen and verify a subsequent write after another reopen. | [MANIFEST](patterns/manifest.md), [`native_v5_store_recovers_and_accepts_new_writes`](../../tests/integration.rs), [read-only tests](../../tests/read_only.rs), [recovery tests](../../tests/crash_recovery.rs) |

For claims about VSDB collection latency, pin lifetime or namespace-wide memory,
check the MMDB dependency and adapter in the VSDB revision under test and
measure the caller's workload.

## 2. Review priority (effort allocation, not automatic severity)

| Class | Focus | Default |
|-------|-------|---------|
| Concurrency / `unsafe` | Stacked/Tree Borrows provenance, atomics, lock order, `FlushQueue` / `MaintenanceState` / worker lifecycle | CRITICAL |
| Durability | `DurabilityMode` + `WalFence`, `DirSynced`, MANIFEST `DBHeader`/sync, multi-WAL recovery, CRC32 framing | HIGH |
| Control / resources | open/read-only/close paths, `PendingSsts`, `SnapshotLease` (`Weak<ReadState>`), cache/FD cleanup | HIGH |
| API / semantics | `src/lib.rs` exports, `DbOptions` builders/defaults, MVCC & `[lower, upper)` bounds, `step_next`/`step_prev` | HIGH |
| Error handling | fail-stop latches (`VersionSet::poisoned`, `ReadState::has_bg_error`, `FlushQueue::fail`), typed `ErrorKind` (`Busy`, `Background`, `Corruption`, `ReadOnly`, `DbClosed`) | MEDIUM |
| Performance | quantified hot/warm-path allocations, lock hold across I/O, extra partition leaf reads | context |
| Tests / docs / build | regression coverage, doc-code alignment, CI/gate parity | LOW unless broken |

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
obsolete-file unlink ([MANIFEST](patterns/manifest.md), [WAL](patterns/wal.md)).

**Design:** walk applicable `D-*` checks in `design-patterns.md` for locks,
resource lifecycles, bounded queues/memory, multi-step state transitions, error
visibility, and public exports.

**Placeholders (CRITICAL in shipped paths when required behavior is missing):**
`todo!`, `unimplemented!`, stand-in `unreachable!`, stub returns, or unfinished
`TODO`/`FIXME` on active code paths. Search callers before labeling code dead.

**Public API** — start at [the crate root](../../src/lib.rs) and follow
the public methods on its exposed types. Check the applicable feature set for
the default library, optional `bench-metrics` counters, or `test-utils`
helpers. Keep defaults, typed errors, item rustdoc, reader documentation
and tests aligned.

### Performance priorities

Compare the same read, scan, or write workload before and after a change.
Keep MVCC visibility, ordering and durability fixed. Record the control,
operation class, offered load, CPU and memory budgets, and actual worker or
shard scope. Compare paired changes against control-to-control variation.
A candidate remains provisional while a protected latency, throughput, or
resource budget is unresolved. Keep raw runs and deferred prototypes with
their [benchmark report](../benchmarks/README.md).

- **Writers.** Measure arrival-to-ack and service latency separately. Use
  ordinary builds for the latency comparison and separate `bench-metrics`
  runs for per-thread timings. Report paced foreground p99, successful puts/s
  and bulk batches/s at saturation. Include background flush or compaction
  when it limits admission. Compare memory, L0 files and pending immutable
  tables at the end of each arm.
- **Bounded reads.** Compare scalar and grouped requests on the same trace and
  snapshot with matching pin lifetimes. Report full-request p99 and qualify
  device traffic separately. Claims about a caller require measuring the
  complete operation on the same graph or collection. The 32-key engine
  workload and the VSDB VecDex query gave different results.
  [Grouped reads](../benchmarks/grouped-reads/README.md) · [VSDB application results](../benchmarks/README.md#vsdb-application-results)
- **Maintenance.** Occasional cleanup can take longer for correctness or
  simplicity. Keep tool-specific optimizations in the tool. Before adding state, locking or branches to
  the engine, establish a measurable benefit to normal operations. For
  cleanup under a writer lock or with a growing queue, measure the cost to
  foreground reads and writes.

## 4. Deterministic checks vs review findings

`rustfmt`, `cargo check`, and `cargo clippy -- -D warnings` are automated gates,
not audit registry findings. Import style, naming, and formatting are never
registry entries.

Record static-inspection findings only for rules automated tools cannot check:

- `#[allow(...)]` attributes added to suppress warnings instead of fixing them
- public behavior, defaults, `ErrorKind`, or docs that disagree with the code
- missing, vague, or inaccurate `// SAFETY:` preconditions on `unsafe` blocks
- CI or `Makefile` omissions that skip an engine test target or feature configuration

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

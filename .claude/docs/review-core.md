# MMDB Review Core

Evidence standard and subsystem map. Apply `pragmatic-engineering.md`: only
findings/process that remove a concrete failure mode.
Use `workflow-policy.md`'s neutral task framing in every agent request and
returned finding. Report the storage operation and expected/actual results;
do not substitute a different scenario for the code path being reviewed.

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
| types/API | `src/types.rs`, `src/lib.rs`, `src/rate_limiter.rs` | `technical-patterns.md` |

Guides under `.claude/docs/patterns/`. Tests/benches/CI/docs/`.claude` → map to
the subsystem they cover; check alignment.

## 2. Review priority (effort, not a finding)

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

For each change requiring detailed review:

1. Name the invariant (mapped guide).
2. Describe concrete conditions (input, operation order, interrupted write, invalid stored field).
3. Trace full path (callers, cleanup, existing guards).
4. State the observed result: incorrect value, missing persisted data, unreadable record, panic, retained resource, blocked operation, or **quantified** hot-path cost.
5. Note smallest regression test that would fail pre-fix.

**Boundaries:** empty/single entry, first/last block keys, restarts, L0 limits,
snapshots, invalid stored fields, max sizes, partial I/O errors.

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

fmt, compile, and clippy are tools, not findings. Import order, naming, and
formatting are not findings.

Record only what those tools will not see:

- `#[allow(...)]` in production code
- public behavior, defaults, errors, or docs that disagree
- `// SAFETY:` missing, vague, or false

## 5. Findings registry (`docs/audit.md`)

Severities: CRITICAL, HIGH, MEDIUM, LOW. No other label.

- **CRITICAL**: loss, corruption, UB, memory safety, unrecoverable durability
- **HIGH**: wrong results, deadlock, realistic crash, exhaustion, material hot-path hit
- **MEDIUM**: edge bug, error-policy gap, bounded leak
- **LOW**: contract or docs drift with a real maintenance cost

Which workflow may add or move each disposition: `workflow-policy.md` §5.

Open:

```markdown
### [SEVERITY] subsystem: summary
- **Where**: file:line_range
- **What**: expected behavior and observed result
- **Why**: conditions, invariant, and why existing checks miss
- **Fix**: minimal correction
```

Won't Fix uses Where, What, and Reason. Rejected uses Where, Claim, and Reason,
with no severity. Observations, style, feature requests, and documented
contracts are not entries.

## Agent handoff and response

Use this compact handoff, filling only the relevant fields:

```text
Task: Review local MMDB storage-engine reliability in the assigned files.
Repository: <absolute path>
Baseline and scope: <HEAD, scope, existing changes to preserve>
Assigned files: <exact, disjoint file list>
Read first: <absolute paths to workflow-policy.md (Task framing section),
             review-core.md, false-positive-guide.md, and applicable
             subsystem guides>
Mode: Read-only investigation; no repository edits or commits.
Question: <the behavior or invariants to check>
Evidence already available: <relevant verified facts, or none>
Return: Files actually reviewed; registry-ready entries from section 5;
test command and result if one was executed; existing checks that ruled
candidates out; work still incomplete.
```

For a test result, state the setup, operation sequence, expected output, actual
output, and command run. Distinguish executed tests from code tracing and
proposed tests. Do not use speculative intent or a dramatic narrative to
explain an ordinary concurrency, decoding, or recovery case.

## Quality gate

Concrete conditions and expected/observed results only. Check each finding
against `false-positive-guide.md`. Agent agreement and pattern IDs are not proof.

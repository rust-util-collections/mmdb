# MMDB Review Core Methodology

This document defines the evidence standard and canonical subsystem mapping for
MMDB reviews.

## 1. Context and coverage

Before analyzing a change:

1. Read the complete diff and surrounding functions.
2. Map every changed code file through the Subsystem Map.
3. Read all mapped guides, callers, and directly relevant tests.
4. For a full audit, build a tracked-file ledger first; do not rely on a static
   list or approximate file sizes.

### Subsystem Map (canonical)

Every Rust source file has one primary row. Concurrency and unsafe guides are
cross-cutting overlays, not substitutes for the primary guide.

| Subsystem | File patterns | Pattern guide(s) |
|-----------|---------------|------------------|
| write/read path | `src/db.rs`, `src/options.rs`, `src/error.rs`, `src/stats.rs` | `technical-patterns.md`, `patterns/concurrency.md`, `patterns/unsafe-audit.md` for `db.rs` |
| memtable | `src/memtable/**/*.rs` | `patterns/memtable.md`, `patterns/unsafe-audit.md` |
| WAL | `src/wal/**/*.rs` | `patterns/wal.md` |
| SST | `src/sst/**/*.rs` | `patterns/sst.md`, `patterns/unsafe-audit.md` for files containing `unsafe` |
| iterator | `src/iterator/**/*.rs` | `patterns/iterator.md` |
| compaction | `src/compaction/**/*.rs` | `patterns/compaction.md` |
| manifest | `src/manifest/**/*.rs` | `patterns/manifest.md`, `patterns/concurrency.md` |
| cache | `src/cache/**/*.rs` | `patterns/cache.md`, `patterns/concurrency.md` |
| types and public API | `src/types.rs`, `src/lib.rs`, `src/rate_limiter.rs` | `technical-patterns.md` |

Pattern guides live in `.claude/docs/patterns/`. Tests, benches, Cargo/CI
configuration, public docs, and `.claude/` are supporting surfaces: map them to
the subsystem whose behavior they specify, then check consistency.

## 2. Risk classification

| Category | Examples | Default risk |
|----------|----------|--------------|
| Concurrency/unsafe | atomics, raw pointers, lock order, thread lifecycle | CRITICAL |
| Durability/encoding | WAL, MANIFEST, SST format, checksums | HIGH |
| Control/resource flow | branches, loops, open/close, cleanup | HIGH |
| API/behavior | public types, defaults, iterator semantics | HIGH |
| Error handling | propagation, fail-stop policy, retry semantics | MEDIUM |
| Performance | complexity, allocation, lock or I/O on hot path | context-dependent |
| Tests/docs/config | coverage or contract alignment | LOW unless behavior is wrong |

Classification prioritizes review effort; it is not itself a finding.

## 3. Evidence protocol

For each risky change:

1. **State the invariant** from the mapped guide.
2. **Construct a trigger** using realistic input, ordering, crash point, or
   corruption boundary.
3. **Trace the full path**, including callers, cleanup, and existing guards.
4. **State the observable outcome**: wrong value, data loss, corruption, panic,
   leak, deadlock, or quantified hot-path regression.
5. **Check regression coverage** and identify the smallest test that would fail
   before the fix.

### Boundary conditions

Consider empty/single-entry state, first/last block keys, restart boundaries,
L0 thresholds, snapshot cutoffs, malformed persisted data, maximum supported
sizes, and error paths after partial I/O.

### Concurrency

Build the actual lock/atomic protocol from current code. Check lock cycles,
guard lifetimes, publication order, wait predicates, and thread shutdown.
`Relaxed` is valid for counters or hints that carry no publication guarantee;
require Acquire/Release only where a happens-before edge is needed.

### Crash safety

Enumerate crash points around file writes, file sync, directory sync, MANIFEST
append/sync, CURRENT publication, and file deletion. Use
`patterns/manifest.md` for the exact persistence ordering; MANIFEST append and
CURRENT replacement are different protocols.

### Performance

Require hot/warm-path evidence and quantify the added work. Cold-path
micro-optimizations are not findings.

### API contract

Only curated re-exports in `src/lib.rs` are public API. Check behavior,
defaults, error semantics, and corresponding docs/tests when those exports
change.

## 4. Deterministic and style checks

Formatting, compilation, and Clippy diagnostics belong to deterministic tools,
not speculative review agents. Repository-specific conventions that tools do
not enforce are still LOW findings:

- no `#[allow(...)]`;
- import repeated inline paths and group common prefixes;
- keep public API docs, `CLAUDE.md`, this map, and pattern guides aligned;
- every unsafe operation has an accurate `// SAFETY:` contract.

## 5. Audit registry

Consult `docs/audit.md` before final reporting:

- Verify relevant `Open` entries against current code and prune fixed ones.
- Re-evaluate `Won't Fix` and `Rejected` entries when this review touches their
  cited code, callers, assumptions, or subsystem. A full audit re-evaluates all.
- Keep a real but disproportionate issue under `Won't Fix` with a current
  reason.
- Put only a recurring/material disproven claim under `Rejected`; it has no
  severity because it is not a finding. Discard routine refuted candidates.
- Never add dates or freshness markers.

### Finding format

```text
[SEVERITY] subsystem: summary
WHERE: file:line_range
TRIGGER: concrete input/order/failure point
OUTCOME: observable incorrect behavior
WHY: violated invariant and why existing guards do not prevent it
FIX: minimal safe direction and regression test
```

### Severity

- **CRITICAL**: data loss/corruption, undefined behavior, exploitable security,
  or unrecoverable durability violation.
- **HIGH**: incorrect results, deadlock, realistic crash, resource exhaustion,
  or material hot-path regression.
- **MEDIUM**: reachable edge-case bug, error-policy gap, or bounded leak.
- **LOW**: repository convention, clarity, or documentation defect with a
  concrete maintenance cost.

Observations and questions may appear in the report but never in `## Open`.

## Quality gate

Retain only findings with a concrete trigger and outcome. Refute candidates
against `.claude/docs/false-positive-guide.md`. Agent agreement, severity
labels, and pattern-name matching are not substitutes for evidence.

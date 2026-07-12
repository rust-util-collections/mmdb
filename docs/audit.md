# Audit Findings

> Auto-managed by /x-review and /x-fix.
>
> **Won't Fix ≠ permanent.** Re-evaluate an entry when a review touches its
> code, callers, assumptions, or subsystem; a full audit re-evaluates every
> entry.
>
> **Rejected is not Won't Fix.** Rejected entries are disproven claims, not
> deferred defects. Re-check them only when their cited code or invariant
> changes.

## Open

### [LOW] docs: `make all` is documented as running `check`
- **Where**: `README.md:391-400`, `Makefile:1-17`
- **What**: The README says `make all` runs `fmt + lint + check + test`, while the target depends only on `fmt`, `lint`, and `test`.
- **Why**: Contributors relying on the documented aggregate command may assume `cargo check` and `cargo check --tests` ran when they did not.
- **Suggested fix**: Remove `check` from the README description so it matches the Makefile and `CLAUDE.md`.

---

## Won't Fix

### [MEDIUM] iterator: seek paths do not overlap cross-source I/O prefetch
- **Where**: `src/iterator/merge.rs:243-305`, `src/iterator/source.rs:370-382`
- **What**: Explicit seeks and bidirectional direction switches synchronously position and decode each source before heap initialization can issue cross-source prefetch hints.
- **Reason**: SST index entries are already memory-resident, so a targeted pre-seek hint phase is feasible, and direction switches make the path warmer than explicit seeks alone. However, `posix_fadvise` is advisory and no controlled cold-cache multi-source benchmark demonstrates a material latency regression; changing the protocol without that evidence remains disproportionate.

---

### [LOW] manifest: file-number arithmetic can overflow at `u64::MAX`
- **Where**: `src/manifest/version_set.rs:565-577,641-651`
- **What**: File allocation, reservations, and MANIFEST rotation increment `u64` counters without checked arithmetic.
- **Reason**: Reaching exhaustion through production allocation requires roughly 1.8e19 file numbers; all reservation counts are bounded by in-memory workload sizes. The failure is mathematically real but not practically reachable. Revisit if identifiers become externally supplied or allocation jumps by unbounded amounts.

---

### [LOW] rate_limiter: `request()` f64 subtraction can stop converging for enormous values
- **Where**: `src/rate_limiter.rs:78-107,145-178`
- **What**: For a single request around hundreds of petabytes, `chunk` can fall below half an ULP of `remaining`, making `remaining -= chunk` a no-op and the loop non-terminating.
- **Reason**: Every production call passes one entry's encoded size, bounded by the 64 MiB write-entry limit and allocatable memory. The private API cannot receive the theoretical trigger.

---

### [LOW] compaction: near-duplicate merge-loop logic
- **Where**: `src/compaction/leveled.rs:610-853,1563-1830`
- **What**: Normal and forced compaction independently implement closely related tombstone, snapshot, deduplication, filter, and sequence-zeroing logic.
- **Reason**: Both paths are currently consistent, while extracting one shared state machine across their different sub-range and streaming protocols would carry disproportionate regression risk. Revisit when a correctness change must touch either loop.

---

### [LOW] SST: restart-count validation can overflow `usize` on 32-bit targets
- **Where**: `src/sst/block.rs:75-85`
- **What**: `(num_restarts as usize) * 4 + 4` can overflow on a 32-bit target for corrupted input.
- **Reason**: The supported and CI target is 64-bit Linux; no 32-bit support is declared. Revisit if 32-bit targets are added.

---

## Rejected

### db: a dead-key sweep error silently kills background compaction
- **Where**: `src/db.rs:1043-1104,2536-2545`
- **Claim**: Propagating a sweep error permanently disables compaction without surfacing the failure.
- **Reason**: The thread records the error through `set_error`, sets `has_bg_error`, and every public entry point then fails through `check_usable()`. This is the engine's deliberate fail-stop-until-reopen policy, not a silent loss of maintenance.

---

### db: the L0 dead-key sweep can never apply its filter
- **Where**: `src/db.rs:1043-1084`, `src/compaction/leveled.rs:1882-1915`
- **Claim**: L0 is never bottommost when the database has multiple configured levels, so its sweep pass is useless.
- **Reason**: Bottommost status is data-driven. A settled store with no deeper overlap can filter L0; when deeper overlap exists, retaining the L0 entry is required until the deeper data is handled. The sweep processes deeper levels first and retains work queued during a pass, so shallower copies are revisited after deeper data is removed.

---

### manifest: additions-before-deletions diverge from recovery for production edits
- **Where**: `src/manifest/version_set.rs:178-232,321-399`, `src/compaction/leveled.rs:1078-1102,1255-1284`
- **Claim**: A current edit can delete and re-add the same file number at the same level, causing live apply and replay to produce different versions.
- **Reason**: Every production edit either moves a file to a different level, allocates fresh output numbers, adds flush files only, or writes a full snapshot without deletions. No caller can produce the same-level collision required by the claim.

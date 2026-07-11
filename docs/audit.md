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

*(none)*

---

## Won't Fix

### [LOW] rate_limiter: `request()` f64 subtraction is a no-op for values ≳ 2.5e17, theoretical infinite loop
- **Where**: `src/rate_limiter.rs` (`request`, the `remaining -= chunk` convergence loop)
- **What**: For a single request of ~hundreds of petabytes, `chunk` can fall below `ULP(remaining)/2`, making the f64 subtraction a no-op and the loop non-terminating. Mathematically confirmed.
- **Reason**: Not practically reachable. Every call site passes single-entry sizes (internal key + value bytes, bounded by allocatable memory); triggering the loop requires first materializing a ~2.5e17-byte entry in RAM. The module is private, so no external caller can inject an arbitrary value. Guarding would add cost/complexity to a hot path for an input that cannot occur. Revisit if `request()` is ever exposed to sizes not backed by in-memory buffers.

---

### [LOW] compaction: Near-duplicate merge-loop logic between `execute_sub_compaction_io` and `force_merge_level`
- **Where**: `src/compaction/leveled.rs` — the tombstone/sequence/snapshot-dedup/filter merge logic (~150 lines) is duplicated nearly verbatim between the two functions.
- **What**: Both implementations were independently verified correct and mutually consistent (same snapshot-retention algorithm, same sequence-zeroing condition, same filter gating). No functional bug today.
- **Reason**: Merging two independently-verified, invariant-critical ~150-line blocks (INV-C2, INV-C2a, INV-C3, INV-C6 all converge here) for a maintainability-only, non-functional finding carries disproportionate regression risk relative to its severity. Revisit if a correctness fix is ever needed in one of the two functions — apply the same fix to both, independently re-verified, at that time.

---

### [LOW] SST: Unvalidated `BlockHandle` offset/size flow into readahead arithmetic and the `posix_fadvise` unsafe call
- **Where**: `src/sst/table_reader/iterator.rs` (`maybe_readahead`, `prefetch_first_block`); consumed by `src/sst/table_reader/mod.rs` (`advise_willneed`)
- **What**: `maybe_readahead()` computes `len` via unchecked `u64` addition on index-derived `BlockHandle` values before the normal bounds check (which only runs when the block is actually read later). The result is cast `as i64` for the FFI call, which could go negative on a corrupted index, contradicting the SAFETY comment's claim.
- **Reason**: `posix_fadvise` is a pure advisory hint — its return value is discarded, and a nonsensical offset/length cannot cause memory unsafety; the real bounds check still gates actual reads. Only concrete downside is a possible debug-build overflow panic on a corrupted index block. Low enough risk/impact to defer; revisit if the readahead logic is touched for other reasons.

---

### [LOW] SST: `Block::new()` restart-count validation can overflow `usize` on 32-bit targets
- **Where**: `src/sst/block.rs` (restart-count validation in `Block::new()`)
- **What**: `(num_restarts as usize) * 4 + 4` can overflow a 32-bit `usize` for large corrupted `num_restarts` values, potentially causing an out-of-bounds panic instead of a clean `Error::corruption`.
- **Reason**: CI only targets `ubuntu-latest` (64-bit x86_64) and `Cargo.toml` has no 32-bit target support; on any 64-bit target this cannot overflow for any `u32` value. Purely a latent portability gap on unsupported targets — revisit if 32-bit target support is ever added.

---

### [MEDIUM] iterator: `MergingIterator` seek path (`seek`/`seek_to_first`/`seek_for_prev`) still defeats cross-source I/O-overlap prefetch
- **Where**: `src/iterator/merge.rs` (`seek`, `seek_to_first`, `seek_for_prev`), `src/iterator/source.rs` (`seek_to`/`seek_for_prev_to`/`seek_to_first_impl`, each combining position+decode into one synchronous call)
- **What**: Every seek-based entry point (the mechanism `db.rs`'s `iter_with_prefix()` uses internally, and the pattern most of the test suite exercises via `iter.seek(...)`) positions each source sequentially and synchronously decodes its entry before `init_heap()`'s prefetch phase runs, eliminating I/O-overlap for the hot seek path. `LevelIterator::prefetch_first_block()` has been added (fixing the cold-start/no-seek case), but the seek path itself is unchanged.
- **Reason**: `SeekableIterator`'s trait contract (in `src/iterator/source.rs`) has no split "position-only" phase — every seek implementation combines positioning and decoding into one call. Cleanly separating these would require changing the trait's method signatures/semantics across every implementor (`TableIterator`, `LevelIterator`, `MemTableCursorIter`, boxed/Vec sources), a cross-cutting, moderately invasive change with real regression risk to defer for a dedicated, focused effort rather than bundle into this audit cycle. The immediate, common-case win (cold-start prefetch for `LevelIterator`) has been captured; this residual gap only affects warm I/O-overlap opportunity, not correctness.

---

## Rejected

### db: Dead-key sweep `?` on `force_merge_level` error "permanently kills the background compaction thread"
- **Where**: `src/db.rs` (dead-key sweep block in the background compaction loop)
- **Claim**: A transient sweep error breaks the thread's main loop, silently and permanently disabling all future compaction.
- **Reason**: The error path calls `set_error`, which sets `has_bg_error`; from then on every public entry point fails loudly via `check_usable()`. This is the engine's deliberate fail-stop-until-reopen policy, identical to every other background error path (debt-driven picks, hint compactions, flush). Nothing is silent, and "no further compaction" is the intended fail-stop behavior.

---

### db: Dead-key sweep's L0 pass "never fires the filter because L0 is never bottommost"
- **Where**: `src/db.rs` (sweep loop starting at level 0), `src/compaction/leveled.rs` (`is_bottommost_level`)
- **Claim**: `is_bottommost` is always false for L0 when `num_levels > 1`, making the L0 sweep pass useless.
- **Reason**: `is_bottommost_level` is data-driven, not level-count-driven: it checks whether any deeper level contains files overlapping the inputs. On a settled store whose data sits only in L0 (the scenario the sweep exists for), it returns true and the filter fires at L0. When deeper overlapping data exists, skipping the filter at L0 is required for MVCC correctness. Residual single-file no-op rewrites are separately eliminated by the `force_merge_level` early return.

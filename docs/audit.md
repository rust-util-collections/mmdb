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

### [MEDIUM] cache: fixed segmentation makes valid blocks permanently uncacheable
- **Where**: `src/cache/block_cache.rs:39-52,129-170,239-289`
- **What**: The pool always uses 64 Moka segments, each limited to `ceil(total_capacity / 64)`, so an entry larger than one segment's share is rejected even when it fits the total configured capacity.
- **Why**: A 4 KiB cache cannot retain one normal 4 KiB block, and the default 64 MiB cache permanently misses any valid block larger than 1 MiB. This contradicts the documented pool-global and historical private-cache semantics.
- **Suggested fix**: Choose the segment count adaptively for small capacities, document the per-segment admission bound for larger blocks, and add regressions for a cache whose capacity equals one normal block.

---

### [MEDIUM] db: explicit `close()` does not quiesce background threads or release the directory lock
- **Where**: `src/db.rs:142-213,2311-2349,3544-3571`
- **What**: `close()` marks the handle closed and flushes it, but compaction shutdown, thread joins, and the `LOCK` file release occur only in `Drop`.
- **Why**: Retaining a closed handle keeps worker threads and the directory flock alive indefinitely, and an in-flight compaction may continue after `close()` reports completion.
- **Suggested fix**: Share an idempotent shutdown/join helper between `close()` and `Drop`, make the lock file interior-mutable so `close()` can release it, and test reopening while the closed handle is still alive.

---

### [MEDIUM] db: automatic dead-key sweeping is non-convergent
- **Where**: `src/db.rs:953-992,1843-1866,2216-2293`, `src/compaction/leveled.rs:1473-1506`
- **What**: The sweep runs one ascending level pass after a one-shot threshold crossing, consumes its flag even when snapshots prevent filtering, and never re-arms for later registrations once the set is already above threshold.
- **Why**: A newest L0 copy can remain after deeper copies are removed, subsequent dead keys never trigger another sweep, and a snapshot-blocked sweep is permanently lost.
- **Suggested fix**: Sweep deepest-to-shallowest, request work for every new unique key while above threshold, and retain/re-arm pending work until a snapshot-free pass can apply the filter. Cover multi-level, repeated-registration, and snapshot-release cases.

---

### [MEDIUM] manifest: recovery accepts overlapping L1+ files
- **Where**: `src/manifest/version_set.rs:209-266,321-359`, `src/db.rs:1213-1273`
- **What**: Recovery and generic edit application sort L1+ files but never validate the non-overlap invariant required by binary-search reads.
- **Why**: A malformed but checksum-valid MANIFEST can install overlapping files; point lookup may choose the later file by smallest key and miss a key that exists only in the earlier overlapping file.
- **Suggested fix**: Validate adjacent L1+ user-key ranges after replay and before applying an edit, returning corruption on overlap. Add live-apply and reopen regressions.

---

### [MEDIUM] manifest: stale edits can regress `last_sequence`
- **Where**: `src/manifest/version_set.rs:165-176,442-454`
- **What**: `next_file_number` is advanced with `max`, but `last_sequence` is overwritten directly during both replay and live apply.
- **Why**: A stale or malformed checksum-valid edit can lower the recovered sequence, causing later writes to reuse lower sequence numbers and lose MVCC precedence to older entries.
- **Suggested fix**: Keep `last_sequence` monotonic with `max` in both paths and reject values above `MAX_SEQUENCE_NUMBER`. Add a stale-edit regression.

---

### [MEDIUM] SST: entry decoding can consume the restart directory
- **Where**: `src/sst/block.rs:75-105,379-496`
- **What**: Entry decoders bound key/value ends against the full block allocation rather than the entry region ending at `restart_offset`.
- **Why**: A malformed checksum-valid length can make an entry swallow restart offsets and the restart count, returning fabricated value bytes instead of corruption.
- **Suggested fix**: Pass the entry-region limit into both decode helpers and reject any decoded end beyond it. Add iterator and seek regressions for trailer overrun.

---

### [MEDIUM] SST: zero prefix-filter length can create false negatives
- **Where**: `src/sst/table_reader/mod.rs:560-629`, `src/sst/table_builder.rs:449-464`
- **What**: The reader accepts a prefix-filter block paired with `prefix_len == 0`, a combination the writer never emits.
- **Why**: Every query then probes the filter with the empty prefix; if that bit is absent, the SST is skipped even though it contains matching keys.
- **Suggested fix**: Reject inconsistent prefix-filter metadata during table open, or conservatively bypass the filter. Add a malformed-metaindex regression.

---

### [MEDIUM] SST: bloom filters can exceed the reader's block-size limit
- **Where**: `src/sst/filter.rs:23-38`, `src/sst/table_builder.rs:180-194,272-284,436-464`, `src/sst/table_reader/mod.rs:450-473`
- **What**: Builder size projections and hard checks cover data, index, and range-deletion blocks but omit whole-key and prefix bloom blocks.
- **Why**: Public option combinations with many small keys or very high `bloom_bits_per_key` can write a filter above 64 MiB; `TableBuilder::finish()` succeeds but `TableReader::open()` rejects the resulting SST.
- **Suggested fix**: Include projected filter sizes in split decisions and enforce the reader-compatible hard limit before writing either filter block. Add builder-level boundary regressions.

---

### [LOW] CI: tracked correctness suites are not executed
- **Where**: `.github/workflows/ci.yml:44-55`, `tests/bidi_debug.rs:1-31`, `tests/lazy_delete.rs:1-260`, `tests/shared_cache.rs:1-103`
- **What**: CI enumerates selected integration targets and omits bidirectional, lazy-delete, and shared-cache tests.
- **Why**: Regressions in mixed-direction iteration, automatic deletion, or cross-DB cache isolation can merge while every CI job passes.
- **Suggested fix**: Run the complete debug test set while explicitly skipping only the profiling-only scale test.

---

### [LOW] options: `max_immutable_memtables` is a public no-op
- **Where**: `src/options.rs:18-21,112-153`, `README.md:54-55`
- **What**: The option is exposed, defaulted, and printed but never read by the database.
- **Why**: Callers can tune it without any behavioral effect despite the public claim that accepted configuration knobs are implemented.
- **Suggested fix**: Document the compatibility no-op and its synchronous-flush reason now, then either remove it in the next major release or implement a real bounded immutable-memtable queue.

---

### [LOW] benchmarks: group-level metadata misstates heterogeneous workloads
- **Where**: `benches/benchmarks.rs:198-206,257-278,461-501`, `README.md:375-386`
- **What**: The cold random-read case executes 500 gets while reporting 1,000 elements, range delete reports 10,000 operations for one tombstone write, and same-process page-cache data is described as I/O-bound cold cache.
- **Why**: Criterion throughput and the README overstate measured work and can mislead performance comparisons.
- **Suggested fix**: Set throughput per benchmark case, define range-delete units explicitly, rename the small-block-cache cases, and align the README methodology.

---

### [LOW] example: canonical iteration ignores deferred errors
- **Where**: `examples/basic.rs:48-62`, `src/iterator/db_iter.rs:247-251,927-932`
- **What**: The example collects or consumes iterators without checking `DBIterator::error()` afterward.
- **Why**: A deferred SST read failure produces partial output while the canonical example still exits successfully, teaching callers to miss the API's error channel.
- **Suggested fix**: Retain each iterator, check `error()` after exhaustion, and keep the example compile-tested.

---

### [LOW] docs: comparison table claims the removed iterator pool still exists
- **Where**: `COMPARISON.md:5-16`, `README.md:364-366`
- **What**: The MMDB column says "Yes" for an iterator object pool while the same row and migration guide say it was removed.
- **Why**: The feature matrix gives callers contradictory API and performance expectations.
- **Suggested fix**: Mark the feature as absent and retain the removal rationale.

---

### [LOW] docs: README presents an incomplete public API inventory
- **Where**: `README.md:191-282`, `src/lib.rs:24-36`
- **What**: The section is titled "Public API" and presents `impl DB`, but omits exported methods and types, including `iter_with_options()` even while recommending it in the same block.
- **Why**: Callers relying on the repository documentation cannot discover supported iteration, batch-overlay, lazy-delete, path, and prefix-step surfaces.
- **Suggested fix**: Either make the inventory complete or label it as selected API and link prominently to docs.rs, with compile-checked snippets for the recommended omitted paths.

---

### [LOW] tests: `no_slowdown` coverage accepts a broken implementation
- **Where**: `tests/integration.rs:807-850`
- **What**: The test records whether a pressured `no_slowdown` write fails, then discards the result and accepts both outcomes.
- **Why**: An implementation that never rejects stalled writes passes the named regression.
- **Suggested fix**: Disable background compaction for the setup, deterministically build L0 above the slowdown threshold, and require `InvalidArgument`.

---

### [LOW] tests: property model does not verify final deletions
- **Where**: `tests/proptest_db.rs:13-80`
- **What**: The final consistency pass checks only keys still present in the model.
- **Why**: A deleted key that is never chosen by a later random `Get` can remain visible without failing the property test.
- **Suggested fix**: Track every generated key and compare the final database result with `model.get()` for both present and absent keys.

---

### [LOW] tests: snapshot-compaction regression omits the snapshot assertion
- **Where**: `tests/integration.rs:511-580`
- **What**: The test keeps a snapshot alive across compaction but checks it only before compaction, and its comments still claim snapshots are not tracked.
- **Why**: A regression that drops versions needed by an active snapshot passes while the test documents obsolete behavior.
- **Suggested fix**: Assert the snapshot's original values after compaction and update the comments to the current snapshot-retention contract.

---

## Won't Fix

### [MEDIUM] iterator: seek paths do not overlap cross-source I/O prefetch
- **Where**: `src/iterator/merge.rs:293-305`, `src/iterator/source.rs:370-382`
- **What**: Seek entry points synchronously position and decode each source before heap initialization can issue cross-source prefetch hints.
- **Reason**: A targeted pre-seek hint hook is possible, but `posix_fadvise` is advisory and no controlled cold-cache multi-source benchmark demonstrates a material latency regression. Changing the seek protocol without that evidence would add cross-implementation complexity for an unquantified optimization.

---

### [LOW] manifest: file-number arithmetic can overflow at `u64::MAX`
- **Where**: `src/manifest/version_set.rs:497-509,581-584`
- **What**: File allocation, reservations, and MANIFEST rotation increment `u64` counters without checked arithmetic.
- **Reason**: Reaching exhaustion through production allocation requires roughly 1.8e19 file numbers; all reservation counts are bounded by in-memory workload sizes. The failure is mathematically real but not practically reachable. Revisit if identifiers become externally supplied or allocation jumps by unbounded amounts.

---

### [LOW] rate_limiter: `request()` f64 subtraction can stop converging for enormous values
- **Where**: `src/rate_limiter.rs:92-124`
- **What**: For a single request around hundreds of petabytes, `chunk` can fall below half an ULP of `remaining`, making `remaining -= chunk` a no-op and the loop non-terminating.
- **Reason**: Every production call passes one entry's encoded size, bounded by the 64 MiB write-entry limit and allocatable memory. The private API cannot receive the theoretical trigger.

---

### [LOW] compaction: near-duplicate merge-loop logic
- **Where**: `src/compaction/leveled.rs:657-808,1595-1740`
- **What**: Normal and forced compaction independently implement closely related tombstone, snapshot, deduplication, filter, and sequence-zeroing logic.
- **Reason**: Both paths are currently consistent, while extracting one shared state machine across their different sub-range and streaming protocols would carry disproportionate regression risk. Revisit when a correctness change must touch either loop.

---

### [LOW] SST: readahead trusts unvalidated `BlockHandle` arithmetic
- **Where**: `src/sst/table_reader/iterator.rs:701-716,892-900`, `src/sst/table_reader/mod.rs:697-714`
- **What**: Readahead computes unchecked ranges and casts them to signed syscall arguments before the normal block-read bounds check.
- **Reason**: `posix_fadvise` is advisory and the actual read remains checked, so the concrete impact is limited to a possible debug overflow panic on corrupted index metadata. Revisit when readahead is otherwise changed.

---

### [LOW] SST: restart-count validation can overflow `usize` on 32-bit targets
- **Where**: `src/sst/block.rs:75-85`
- **What**: `(num_restarts as usize) * 4 + 4` can overflow on a 32-bit target for corrupted input.
- **Reason**: The supported and CI target is 64-bit Linux; no 32-bit support is declared. Revisit if 32-bit targets are added.

---

## Rejected

### db: a dead-key sweep error silently kills background compaction
- **Where**: `src/db.rs:953-1012,2431-2453`
- **Claim**: Propagating a sweep error permanently disables compaction without surfacing the failure.
- **Reason**: The thread records the error through `set_error`, sets `has_bg_error`, and every public entry point then fails through `check_usable()`. This is the engine's deliberate fail-stop-until-reopen policy, not a silent loss of maintenance.

---

### db: the L0 dead-key sweep can never apply its filter
- **Where**: `src/db.rs:953-992`, `src/compaction/leveled.rs:1882-1910`
- **Claim**: L0 is never bottommost when the database has multiple configured levels, so its sweep pass is useless.
- **Reason**: Bottommost status is data-driven. A settled store with no deeper overlap can filter L0; when deeper overlap exists, retaining the L0 entry is required until the deeper data is handled. The actionable defect is the sweep's non-convergent ordering and scheduling, recorded above.

---

### manifest: additions-before-deletions diverge from recovery for production edits
- **Where**: `src/manifest/version_set.rs:178-232,321-399`, `src/compaction/leveled.rs:1078-1102,1255-1284`
- **Claim**: A current edit can delete and re-add the same file number at the same level, causing live apply and replay to produce different versions.
- **Reason**: Every production edit either moves a file to a different level, allocates fresh output numbers, adds flush files only, or writes a full snapshot without deletions. No caller can produce the same-level collision required by the claim.

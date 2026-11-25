# MMDB v5 / VSDB v18 joint design review

## Executive decisions

1. **Make the MMDB v5 upgrade and v17→v18 storage migration one release project.** Simply changing VSDB’s dependency from 4.3.15 to 5.x is insufficient: today’s migration deliberately performs no row-by-row MMDB conversion.
2. **Replace VSDB’s emulated WAL fences and value-materializing existence checks with MMDB v5 primitives.** This is the clearest low-risk joint improvement.
3. **Correct block-cache capacity semantics before publishing VSDB’s memory-budget claims.** MMDB’s normal and overflow stores can each hold the configured pool capacity; pins are additional.
4. **Expose short-lived projected reads through VSDB, then fix `PersistentBTree`’s repeated parsing.** Avoid returning long-lived `PinnedSlice`s by default.
5. **Defer key-layout redesign, native column families, value separation, and speculative read batching.** Their migration and correctness costs are not justified by the present evidence.

The first three are **pre-release decisions**. The fourth requires an API decision now, but most optimization work can follow.

## Evidence and its limits

Relevant public source snapshots were MMDB commit [`4916abb`](https://github.com/rust-util-collections/mmdb/tree/4916abbcb6222aa225efb3d95be66f71ee5a2ad6) and VSDB commit [`7082a88`](https://github.com/rust-util-collections/vsdb/tree/7082a888e8defc9a563653b14b0e0b16277f6496). I did not execute tests or benchmarks. **Limitation of this review:** the local appendix CSV was not readable through the available inspection interface, so I deliberately make no claims from its numerical results; the recommendations below are source-led and use repository-recorded historical figures only as hypotheses.

### Strongest source evidence

- **The versions do not currently line up.**
  `./vsdb/Cargo.toml` sets `mmdb = "4.3.15"`, while `./mmdb/Cargo.toml` is version `5.0.0`. MMDB v5 replaced `WriteOptions { sync, .. }`-style durability with `DurabilityMode` and added `DB::contains_key`, `DB::get_pinned`, and `DB::sync_wal`.

- **VSDB currently duplicates engine functionality.**
  In `./vsdb/core/src/common/engine/mmdb.rs`:
  - `MmDB::try_contains_key` calls `DB::get` and discards the value.
  - `sync_db_wal` performs a synchronous deletion of `META_KEY_WAL_FENCE`.
  - `MmdbReadView` also ultimately materializes looked-up values.
  These are direct operations, not inferred costs: a fence writes an extra WAL/memtable tombstone which will later participate in compaction; containment can copy a large value merely to produce `bool`.

- **The migration currently assumes MMDB data remains usable without conversion.**
  `./vsdb/migration/src/universe.rs` scans existing shard stores read-only, writes v18 engine metadata, and explicitly does not perform a row-by-row rewrite.
  In contrast, `./mmdb/docs/proposals/v5-roadmap.md` requires the v4 exporter and `DB::ingest_sorted`; writable v5 does not silently rewrite v4. `./mmdb/tools/v4-export/tests/golden.rs` validates a golden fixture and clean failure before publication, but not crash interruption at every publication stage.

- **Pool capacity is not aggregate evictable capacity.**
  `./mmdb/src/cache/block_cache.rs`, `BlockCachePool::new`, constructs both a segmented `inner` cache and an `overflow` cache, each with `max_capacity(capacity_bytes)`. Oversized-per-segment entries move to overflow. Member-local `PinnedBlocks` are separate.
  `./mmdb/tests/shared_cache.rs` has valuable member-isolation tests—`test_shared_pool_members_never_alias` and `test_shared_pool_close_isolates_members`—but no capacity, overflow, pin-accounting, or byte-usage assertions.
  Therefore `./vsdb/docs/proposals/shared-mem-pool.md` is right that budgets are not hard RSS limits, but its nominal pool arithmetic still understates the evictable stores’ possible combined capacity.

- **A concrete collection-layer inefficiency exists independently of microbenchmark noise.**
  `./vsdb/strata/src/basic/persistent_btree/mod.rs`:
  - `PersistentBTree::get` calls `lookup`.
  - `lookup` calls `find_leaf`, receives that leaf’s bytes and index, then calls `NodeView::open` on the same leaf again.
  - A leaf-only inline-value hit performs one object read but two leaf opens. Cold height \(h\), excluding the leaf, costs \(h+1\) object reads and \(h+2\) opens.

  `./vsdb/strata/src/basic/persistent_btree/iter.rs` retains leaf bytes but reconstructs `NodeView` in `next()` for every item. Out-of-line values additionally call `read_object` per blob. These are CPU/I/O mechanisms visible directly in the callers.

- **Current benchmarks mostly measure warm hits and small values.**
  VSDB’s `Mapx` and `MapxOrd` benchmarks use existing keys and usually 64–128-byte values; they do not predetermine a cold-cache state. `strata/benches/versioned.rs` has a current-state miss case, but no historical-miss case and no large-value/diff workload.
  MMDB’s small-cache tests can still hit the OS page cache. Its `benches/v5_profile.rs` tail estimates use only 20,000 samples per operation class and combine contention, background work, and scheduling.

Consequently, **an MMDB `DB::get` timing and a VSDB `Mapx::get` timing are not competing measurements of the same operation**. VSDB adds key encoding, the eight-byte collection prefix, shard routing, wrappers, serialization, and sometimes snapshot/tree traversal. Differences cannot causally be assigned to the engine without operation counters or matched engine-versus-end-to-end experiments.

Historical v18 figures in `./vsdb/docs/proposals/v18-roadmap.md` also predate later changes and use MMDB 4.3.15. In particular, the reported 1,025-byte blob cliff and expensive large-value diffs motivate investigation, not a new threshold chosen by intuition.

---

# Prioritized proposals

## P0 — Bind the v5 upgrade to a staged, resumable VSDB migration

**Pre-release compatibility decision: required if v18 ships on MMDB v5.**

- **Operation changed:** `vsdb_migrate::migrate_universe` and each namespace/shard’s v17→v18 engine preparation.
- **Causal benefit:** prevents v18 from publishing `VSENG018`/format-18 metadata around stores MMDB v5 cannot open writable. Staging also avoids destructive in-place conversion.
- **Realistic alternative:** keep VSDB v18 on MMDB 4.3.x. This is the safe fallback, but forfeits the joint API and v5 engine improvements and creates another migration later.
- **Risks:** temporary disk amplification, incomplete external-namespace conversion, duplicate replay or lost WAL-resident writes, incorrect level geometry, and a false “Established” state after a crash.
- **Correctness invariants:**
  - Preserve every map’s eight-byte little-endian prefix and `prefix % shard_count` placement unless performing a separately reviewed re-sharding.
  - Never mark an engine Established until **all** expected shards are valid v5 stores.
  - Preserve point values, deletes, range tombstones, and the snapshot-visible final state of residual WALs.
  - A crash must either leave the untouched v17 source usable or leave a validated forward-resumable staging journal; it must not require guessing from directory names.
  - Do not couple format-18 publication to successful recovery with corruption silently accepted. MMDB only permits structural truncation at the selected WAL tail.

### Staged migration

1. Stop all v17 processes, including external namespaces; lock every source.
2. Record source identities, namespace paths, shard counts, target MMDB v5 geometry, and a migration UUID in a durable journal.
3. Export each MMDB 4 shard **read-only** into isolated staging directories using the v4 exporter and v5 `DB::ingest_sorted`. Keep source files immutable.
4. Durably sync each staged SST/MANIFEST and parent directories. Reopen staged shards read-only and validate at least ordered iteration, entry counts, checksums, and application-held collection metadata.
5. Only after all shards validate, publish v18 engine metadata and registry state in the existing journal order. Publication must remain forward-resumable; staging completion alone must never authorize normal v18 opens.
6. Retain the v17 sources until an explicit, documented cleanup step. Do not claim in-place backward compatibility after accepting new v18 writes.

External namespaces mean the whole migration cannot be made one atomic filesystem rename. The journal and Established fence, rather than a single rename, provide global safety.

### Crash/reopen and rollback

Add process-kill/fault-injection cases before and after staged SST sync, after one of several shards completes, before registry commitment, and immediately afterward. Every retry must converge to the same final state.

**Rollback means reopening the preserved v17 sources before the v18 copy has accepted writes.** After new writes, automatic rollback is unsafe; it requires a forward export or an explicitly acknowledged loss of those writes. The current migration’s deletion of legacy lifecycle state is therefore not, by itself, an acceptable rollback design for the v5 conversion.

**Experiment and decision rule:** construct a fixture containing memtable-only writes, flushed SSTs, point/range deletes, binary keys, multiple namespaces, and external namespaces. Inject failure at every journal boundary. Ship only if all crashes either reopen original v17 data without changes or resume to a byte/entry-equivalent v5 store, with no partially Established engine. Any source mutation, required manual directory repair, or ambiguous post-failure open is a release blocker.

---

## P0 — Use MMDB v5 containment and WAL fences directly

**Pre-release compatibility decision: adopt.**

- **Operations changed:** `MmDB::try_contains_key`, `MmdbReadView::contains_key`, `sync_db_wal`, `try_sync_all_wals`, and all `Mapx`/`MapxOrd`/read-view wrappers that reach them.
- **Causal benefit:** `DB::contains_key` resolves visibility without constructing an owned value; `DB::sync_wal` uses `WalFence` rather than writing `META_KEY_WAL_FENCE`. This removes real allocations/copies and the extra tombstone’s WAL, memtable, and compaction work.
- **Realistic alternative:** retain the tombstone fence and value-based containment behind adapters. That is appropriate only as a temporary MMDB 4 fallback, not as the v18/v5 implementation.
- **Risks and invariants:**
  - Fence the same shard and all earlier successful writes; do not turn shard-level sync into a cross-shard transaction.
  - Preserve nonempty `commit_sync()` durability acknowledgment and persistence-error ambiguity. Do not silently redefine an empty batch as a fence; callers wanting a fence should use `try_sync_wal`.
  - Containment must handle empty values, newest-value selection, range tombstones, snapshots, and `WriteBatchWithIndex` visibility exactly as `get(...).is_some()` did.
  - A hint from a Bloom false positive must not change the result; false negatives are forbidden.
  - Read-only sync stays a no-op, and the first error in multi-shard sync remains observable even if earlier shards already synced.
- **Experiment:** separately measure engine operations and VSDB end-to-end calls. For fences, use prior asynchronous writes followed by repeated syncs and assert stable logical entry count plus no fence-record/WAL-size growth. For containment, test hit/miss and values spanning 0 B, 128 B, 16 KiB, and a large memtable-resident value; include snapshots and range deletion. Run repeated fresh-process comparisons with confidence intervals. **Adopt if operation counts and correctness invariants hold and no end-to-end p50/p99 regression exceeds 2%; otherwise keep the semantics but investigate the direct v5 implementation—do not restore unnecessary tombstones merely to match timings.**

---

## P0 — Make cache capacity aggregate, observable, and separate from pins

**Pre-release compatibility decision: define and fix the contract; no format change is needed.**

- **Operation changed:** `BlockCachePool::new`/`insert_entry`, overflow admission, usage reporting, and VSDB’s per-engine budget calculation in `MmDB::open_at`.
- **Causal benefit:** ensures `BlockCachePool::new(C)` means approximately `C` of evictable cache charge, not up to `C` in `inner` plus another `C` in overflow. Separate pin reporting prevents small values from invisibly retaining whole SST blocks or memtable arenas under a nominal budget.
- **Realistic alternative:** explicitly budget two pools, such as a small shared main pool plus a bounded overflow allowance. This is easier to implement and tune, but it must change VSDB’s arithmetic and user-facing documentation rather than calling both stores one `C`-byte pool.
- **Risks and invariants:**
  - Strict cache limits can hurt workloads dominated by large index/data blocks; per-segment admission must not cause pathological eviction.
  - Capacity remains an allocation-charge target, not an RSS guarantee. Allocator capacity, metadata, active/flushing memtables, tree caches, and query transients need separate accounting.
  - Cross-member key isolation, close/detach behavior, concurrent insertion invalidation, and value bytes must remain exactly correct.
  - Quiesced normal-plus-overflow charge must not exceed the documented limit except for explicitly reported transient/pinned allowances.
- **Experiment:** use oversized-per-segment blocks around the current segment threshold, a mixed hot/cold workload, and several `C` values. Add byte-usage and pin-usage instrumentation; entry count alone is insufficient. Assert aggregate usage after cache maintenance and separately test long-lived pins after compaction and close. Compare hit rate, p95/p99 point latency and RSS against both current behavior and an explicit split budget. **Choose aggregate capacity unless achieving it costs more than 5% p99 latency in the large-block workload; in that case choose an explicitly accounted overflow budget, not the present undocumented doubling. Any isolation or accounting violation blocks release.**
- **Follow-up only:** investigate a shared write-buffer/flush governor across namespaces, modeled on RocksDB’s [`WriteBufferManager`](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/write_buffer_manager.h). It should not delay the cache fix.

---

## P1 — Add scoped projected reads, then remove `PersistentBTree`’s repeated parsing

**API shape belongs before release; implementation can be staged.**

### 1A. Scoped value projection

- **Operation changed:** raw map/read-view gets, typed `Mapx*::get`, blob reads, and eventually iterator value access.
- **Proposed API:** something equivalent to `get_with<R>(key, FnOnce(&[u8]) -> R) -> Option<R>`, built on `DB::get_pinned`. The pin remains inside the call; existing `get` becomes an owned adapter.
- **Causal benefit:** decodes `V` directly from projected bytes rather than copying encoded bytes into a temporary `Vec<u8>` and then deserializing. It also provides a foundation for inspecting large blobs without unconditionally materializing them.
- **Realistic alternative:** copy into a reusable scratch buffer. That removes repeated allocation but not the copy and is preferable if callback-based reads show poor code structure or unexpected pin retention.
- **Risks and invariants:** VSDB’s `ValueEnDe::decode_value` returns owned data, so this is **not** zero-copy application deserialization. No encoded-value borrow may escape the callback. Keep public returned `PinnedSlice`s out of the ordinary API because one small value can retain a block up to 64 MiB or a memtable arena (`./mmdb/src/pinned.rs`). Existing owned `get` results and all serialization/trailing-byte errors must remain identical.
- **Decision experiment:** instrument allocations/copies for 128 B, 16 KiB, and 64 KiB values in both memtable and SST reads, with all pins dropped at callback exit. Keep if large-value copies disappear and small-value p50 does not regress beyond noise; otherwise retain the API only where it benefits tree/diff callers.

### 1B. `PersistentBTree` point and iterator cursors

- **Operation changed:** `lookup`/`find_leaf` and `BTreeIter::{descend,next,advance}`.
- **Causal benefit:** point lookup parses the found leaf once. Iteration validates header/slot state once per leaf instead of once per returned item. These changes have no storage-format or rebalancing risk.
- **Realistic alternative:** leave point lookup alone and optimize only scans if profiling shows `NodeView::open` is negligible.
- **Risks and invariants:** avoid an unsafe self-referential `Vec<u8>` plus borrowed `NodeView`. Use a cursor owning the bytes and copied slot metadata, or a vetted self-referential owner. Preserve lower-bound inclusivity/exclusivity, nondecreasing order, snapshot root leases, shared-node reuse, and blob-ID reads.
- **Decision experiment:** point hit/miss at several heights plus ranges of 100/1,000/5,000 and full 5,000-entry scans, with page-read counters enabled. Ship parsing changes only with no change in logical results or object-read counts and at least an 8% median scan improvement (or a clearly measured allocation/CPU reduction at stable latency). Keep the point fix independently if beneficial.

### Later large-value follow-up

Add raw/lazy diff entries before considering more B-tree layout work. `./vsdb/strata/src/versioned/diff.rs::DiffEntry::decode` eagerly materializes every value. A raw stream lets callers inspect keys and selected values without decoding both sides of every modification. The eager API remains an adapter. **Do not compare encoded bytes to classify semantic equality unless canonical encoding is guaranteed**; `Mapx` equality already accounts for cases such as signed zero and NaN.

Blob read-ahead or `MultiGet` is a **hypothesis**, not yet a recommendation. First establish with a large-value workload that blob reads dominate rather than page decoding or typed deserialization.

---

## Defer or reject

- **Native MMDB column families / per-map SSTs:** reject for v18. The current `prefix || user_key` model shares memtables and compaction across many small maps and preserves natural prefix scans. Native physical isolation would multiply files, buffers, and compaction work and require a full key-layout migration.
- **Shorter or variable-width collection prefixes:** defer. They save bytes but complicate globally durable allocation, restart identity, and lexicographic prefix boundaries. The current 64-bit identity and eight-byte `prefix_len` are part of both MMDB SST configuration and VSDB recovery.
- **Routing maps by a hash of `(prefix, user_key)`:** reject: it destroys ordered prefix ranges and monatomic map batches.
- **MMDB value separation/blob files:** defer. VSDB already has collection-level blobs above 1 KiB. Adding a second LSM-level value lifetime/GC model before measuring end-to-end large-value reads would compound complexity.
- **Raising/lowering VSDB’s 1 KiB blob threshold from historical latency alone:** defer. The cliff includes page/object and serialization effects; measure copy count, read count, and cache residency first.
- **Broad `MultiGet` implementation:** defer until a real caller supplies useful batches. A vector API can shift copies rather than remove them; RocksDB’s documented [`MultiGet`/`PinnableSlice` APIs](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/db.h) provide a useful model but should not drive the design ahead of the blob evidence.
- **Tuning Bloom bits/probe count from the present hit-heavy benchmarks:** reject as decision evidence. Separate absent keys within an existing collection prefix from probes of entirely absent collections, and report false-positive rate plus expected probes per level. Keep persisted filter hash/probe layout gated by MMDB’s `filter.format`; payloads do not self-describe arbitrary algorithm changes.
- **More shards to make a hot map faster:** reject as a general fix. A map remains on one shard; additional shards increase static buffers, WALs, and compaction threads and can reduce nominal per-shard capacity.
- **`lazy_delete` as durable immediate deletion:** reject that interpretation. It remains readable before compaction and registrations are process-local; recovery must replay persistent GC/root state rather than assume deletion registration survived.

## Release order

1. Land v5 API integration **with** the staged migration contract.
2. Fix cache-capacity semantics and add memory/accounting crash tests.
3. Freeze the scoped projected-read API without exposing unrestricted pins.
4. Implement low-risk B-tree cursor and lookup improvements.
5. Re-run matched engine-only and VSDB end-to-end workloads, including misses, cold/warm cases, large values, realistic reopen phases, and repeated fresh processes.
6. Only then consider blob prefetch, cross-namespace write governance, or more invasive storage changes.

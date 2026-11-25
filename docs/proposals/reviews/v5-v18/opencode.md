# Joint MMDB v5 ↔ VSDB v18 design review

**Scope note:** the proposed numbers below are either read directly from the supplied sources/CSV or explicitly marked as *hypotheses to measure*. The prior comparison (`mmdb/docs/proposals/mmdb-v5-vsdb-v18-improvements.md` + `mmdb/docs/benchmarks/point-ops/runs.csv`) is single-host, 128-byte values, one warm reader, and no sustained overwrite or large-value work; everything I propose below is chosen because that comparison leaves the gap open, not to re-run its rows. I made no code, benchmark, or test changes and read no external source other than what the repositories link.

---


### 1. Most important evidence, and what it cannot tell us

| Evidence | Path | What it supports | Limitation |
|---|---|---|---|
| 12 writer rows: 8-writer Put no-sync 414 kops/s vs RocksDB 594 (1.44×), vs Pebble 1,258 (3.0×) | `mmdb/docs/proposals/mmdb-v5-vsdb-v18-improvements.md` L23–L35, `mmdb/docs/benchmarks/point-ops/runs.csv` | The engine's real gap is **write concurrency**, not single-writer or point reads | 128-byte value, uniform key visit, one structural locality pattern; says nothing about page-cache-free sustained overwrite |
| Pebble ack precedes the OS WAL write; RocksDB and MMDB do not | same doc L33–L35, L62–L68 | **Cross-engine "Put" numbers are not the same durability contract** | The comparison acknowledges this but does not re-normalise |
| Per-shard write-buffer sizing: `min(if B>16 GiB {B/4/S} else {1 GiB/S}, 512 MiB)`, `max(min(legacy, B/8/S), 4 MiB)` | `vsdb/docs/proposals/shared-mem-pool.md` L45–L47 | The default namespace gets 128 MiB/shard at 2 GiB and 16 shards | 1 GiB/S cap means no budget above 16 GiB raises buffer size |
| Leader inserts admitted memtable entries **while holding `DBInner`**, then publishes; freeze (WAL `sync_data` + `File::create` + dir fsync) happens on the leader | `mmdb/src/db.rs` `write_batch_group` L3029–L3200; `mmdb/docs/architecture.md` L5–L9; `mmdb/docs/ai/patterns/commit-protocol.md` | Single writer lock across WAL *and* memtable insert — the 1.50×→2.38× scaling drop (MMDB 1.50 vs RocksDB 2.38) | Not measured; the doc lists it as "where larger workloads can diverge" (L92) |
| `FrozenMemtable` freeze is synchronous with `WalWriter::new` (create + `sync_parent_dir`) | `mmdb/src/db/flush.rs` `prepare_standby_wal` vs `mmdb/src/db.rs` `freeze_memtable`; `mmdb/docs/proposals/v5-roadmap.md` §A (~4.9 ms floor, L~280–290 of that note) | A real, measured-by-the-team `p999` spike from WAL rotation on the write leader | Roadmap note predates the standby-WAL fix; I did not re-measure |
| Collection → prefix → shard: `Collection → Prefix (u64) → prefix % 16`; prefix allocation is a persisted `__SYSTEM__/__prefix_ceiling__` counter, persisted, deduped via `RECOVERED_PREFIXES` | `vsdb/core/src/common/engine/mmdb.rs` L139–L330, L698–L800; `vsdb/core/src/common/mod.rs` L42–L60; `vsdb/core/src/common/engine/mod.rs` L42–L100 | **The v18 format already assumes an external ordered identity**; the same collection prefix is reused for every table in the tree, including `VecDex`'s node IDs and SlotDex's slot IDs | Not directly measured, but a structural fact of the format |
| `DagMap` destroys with `batch.delete_range(&DATA, &END)` then per-node `delete_range(&EDGE, &DATA)` | `vsdb/strata/src/dagmap/raw/mod.rs` L400–L435, L590–L605 | Large range deletions in the write path, one WAL record each | Frequency not measured |
| `PersistentBTree` mutation issues one engine batch per **operation**, per tree, not per branch | `vsdb/strata/src/basic/persistent_btree/mod.rs` `pending`/`flush_pending` L~681–L760 | A 3-branch VerMap edit sends up to ~10+ rows through one leader call | Frequency not measured |
| `SlotDex::insert_batch` coalesces, but each slot is a separate key `0x00 | slot_be | key_be` and every level is a separate row | `vsdb/strata/src/slotdex/mod.rs` L22–L31, L592–L720 | Write amplification under batched import not measured |
| Per-call `db.snapshot()` + `snapshot.read_options()` per `get`/`range` in every `MapxRaw` read, and per `PersistentBTree` `reader()`/lease per node read | `vsdb/core/src/common/engine/mod.rs` L42–L110, L142–L160, L298–L331; `vsdb/strata/src/basic/persistent_btree/mod.rs` L1134–L1170, L507–L536 | **Every read path pays a snapshot registration** (`DBInner::snapshot_seq` holds `inner` — L~2018 `mmdb/src/db.rs`) and, for trees, one engine `get` per B+ tree node | Snapshot cost is measured in the v18 evaluation (`vsdb/docs/evaluations/v18-versioned.md` L~200) but the *collection read* cost of it is not isolated |
| `MetaIndexData` records `filter.format` and rejects `Some(1)`; only `FILTER_FORMAT_BLOCKED_64B` accepted | `mmdb/src/sst/table_reader/mod.rs` L1205–L1212, L1150–L1195; `mmdb/src/sst/format.rs` L26–L28 | **Changing probe math later is a format break**, not a build flag | — |
| `probes` uses affine 8-way probes in one 64 B line (`h1 & 511`, `h1 >> 9 & 511`, …, `h2 >> 9 & 511`) | `mmdb/src/sst/filter.rs` L48–L66 | False-positive floor is a function of *probe scheme*, independent of `bits_per_key` | The comparison's §B (65,536-mask floor, ~6.1–6.6× FPR reduction) is its own unpublished claim, not measured in `runs.csv` |
| `filter.format` is per-SST, *and* filter budget is per-level at build time | `mmdb/src/db.rs` `l0_build_opts` L303–L322, `mmdb/src/options.rs` L520–L545; `mmdb/docs/architecture.md` L78–L84 | Monkey-style budget reallocation needs **no format change** if the builder is told the level | — |
| `ingest_sorted` rejects any overlap with point entries, **range-deletion partitions of any level**, or range tombstones | `mmdb/src/db/ingest.rs` L44–L85, L153–L165, L180–L215, L230–L290 | The gate is global: one overlapping file in the whole key span rejects | — |
| `ingest_sorted` requires input sorted; bottom-level target is `target_file_size_base` | `mmdb/src/db/ingest.rs` L167–L190, L215–L225 | No random-order import path at all | — |
| `sync_wal` in VSDB is a **fake** fence: a one-point `delete_with_options(WalFence)` record | `vsdb/core/src/common/engine/mmdb.rs` L453–L485 (`META_KEY_WAL_FENCE`) | Every `sync_wal` costs a full group-commit cycle; no CAS-free fast path | The fact that the semantic fence works is not measured |
| `open_read_only` takes a shared `LOCK`; writer takes `LOCK_EX` | `mmdb/docs/usage.md` L58–L64; `mmdb/docs/proposals/read-only-mode.md` §4.2 | Read-only and writable cannot share an engine | — |
| v18 `OpenMode` is **process-wide**, and `Namespace::close` refuses while handles live | `vsdb/core/src/common/mod.rs` L319–L338; `vsdb/core/src/common/namespace.rs` L1272+ | You cannot have one namespace read-only and another writable in one process | Not measured |
| `recover_ceiling` recomputes from `highest_issued_prefix` only when `__prefix_ceiling__` is absent | `vsdb/core/src/common/engine/mmdb.rs` L676–L696 | The allocator persists one `u64` per base dir, shared by all engines | — |
| `.bucket`/`0x10` map: per-namespace reserved capacity is a fixed fraction of the nibble tree | `vsdb/core/src/basic/mapx_raw/mod.rs` L110–L180, L400–L520 | Every `MapxRaw` allocates a process-wide `AtomicU64` allocator plus a `Mutex` | Countable, not measured |
| Workspace pins `mmdb = "4.3.15"` while the mmdb repo snapshot is `5.0.0` | `vsdb/core/Cargo.toml` L20; `mmdb/Cargo.toml` L3 | **VSDB v18 does not run on the MMDB v5 snapshot**; the joint design must specify a version or a compatibility path | No compatibility shim in VSDB for v5 |
| v18 evaluation measured a **snapshot read** (`Snapshot::get` 885 ns vs `VerMap::get` 1,313 ns at N=100k) and a **working-state Merkle diff** at 1.69 ms/step for 2,000 dirty keys | `vsdb/docs/evaluations/v18-versioned.md` L~200; `vsdb/docs/proposals/v18-roadmap.md` L~231–506 | Branch-insert costs 1.7× the tree read cost; a full rebuild is cheaper than an O(K²) diff | Both are tree-internal, not collection-level |

**Material issue the initial comparison missed (my primary hypothesis).** The comparison measures the engine in isolation against RocksDB and Pebble on **one-level-uniform 8-byte key / 128-byte value** traffic, and separately says "cold storage, sustained overwrites, and tail latency remain unmeasured here." It does not measure the shape VSDB actually generates:

1. **One leader per engine for many collections.** VSDB routes `prefix % 16` to one shard, but every `PersistentBTree`, `SlotDex`, `VerMap` operation and every `MapxRaw` batch goes through the *same* group-commit leader and *same* `DBInner` lock inside that shard. A single 200-node `VerMap` edit and 100 single-key writes on an unrelated `MapxRaw` in the same shard contend on one mutex. The comparison's "many writers" rows use 8 OS threads on one engine; VSDB's analogue is *many collections per engine*.
2. **Snapshot registration per node and per point read.** If that path locks `DBInner` per node read (as `snapshot_seq` does in `mmdb/src/db.rs`), a range scan of a 500k-node tree costs 500k inner-locks and a 500k snapshot registrations per scan — a **multiplicative** cost on a workload the comparison never runs.
3. **The v18 format already owns the ordering.** VSDB's collections encode their ordering *in the prefix* (`slot_be`, `0x10|object_id`), so **cross-collection prefix-ordered merging is not needed** for most collection reads. This removes the reason to keep a shared ordered merge at the collection layer, which is the current bottleneck (§4.2).

I rate #1 and #2 as **high-confidence hypotheses**, #3 as **format-design fact with unverified cost**. They should be tested before any large MMDB-side change.

---

### 2. Prioritised proposal

Three pre-release compatibility decisions must be settled **before** implementation, because each changes persisted bytes or public API:

**P0 (compatibility gate, do first).**
- **(a) Version the pair.** VSDB v18 is pinned to `mmdb = "4.3.15"` (`vsdb/core/Cargo.toml:20`). Any joint improvement must either (i) ship a v5 capability gate in `core` with an explicit version check, or (ii) be expressed as `core`-side constructs that work on 4.3.15. I recommend (ii) for everything below P4, because it keeps v18 loadable on both and avoids a hard major bump that the compatibility policy (`vsdb/docs/ai/compatibility-policy.md` §"Accepted breaks") says an agent must not decide unilaterally.
- **(b) Freeze `filter.format`.** The only supported value is `FILTER_FORMAT_BLOCKED_64B` (`mmdb/src/sst/table_reader/mod.rs:1205`). Adding `format = 3` (independent probes, §P4.3) requires `manifests` version bump + old-SST decode + v5-vs-v4 reader compatibility, i.e. a major bump in MMDB. **Recommendation: do not add format 3.** Fix FPR within format 2 first (per-level budget, `bloom_bits_per_key` per level, optional bottommost elision), then measure; if a scheme still needs independent probes, put the probe math in the *reader* keyed on a new `filter.format` value and require a manifest format bump — not a silent default.
- **(c) Reserve identity space.** VSDB v18 prefixes (`v` from `PREFIX_ALLOC_START = 4_096_0000`, `vsdb/core/src/common/mod.rs:60`) must be widened or explicitly namespaced before MMDB-level extensions that change key shape. I recommend **not** changing the 64-bit prefix now; instead put any collection-layer extension under a *separate* 16-bit tag inside the existing u64, i.e. `prefix = (collection_tag << 48) | raw`, and reject a `raw` that collides.

**P1 (highest expected value, low risk): de-duplicate the per-read `snapshot()` registration and prove it.** Add a `DB::snapshot_ref_checked`/`ReadOptions::with_held_view` escape hatch (or a `DB::peek(key, sequence)` helper) so collection readers that hold a `PersistentBTreeReader` lease or a `MapxRawReadView` can pass a **held snapshot** instead of registering a new one. `PersistentBTree` currently calls `self.nodes.reader()` per node (`persistent_btree/mod.rs:1136`, `:1163`, `:523`), and `mmdb`'s `snapshot()` calls `snapshot_seq()` which takes `inner` — see `mmdb/src/db.rs` L~2018–2030. This is a **purely additive** v5 API (new method, no format change), and it is the single change that would move a 500k-node collection read from "N inner locks" to "N pins on one already-held read view."

**P2 (high expected value, moderate risk): route collection mutations through an explicit multi-collection unit.** Add `DB::write_many(&[WriteBatch])` (or a `WriteBatch::append(batch)`), which assigns one sequence range across all batches and writes one WAL record, with **documented cross-collection ordering**: the WAL order is the commit order, and the "batch" is a single internal group. VSDB currently issues one `write_with_options` per collection operation, so a `VerMap` branch edit plus the `SlotDex` count update is **two** group commits and two WAL records per logical transaction. The format does not require it (collections are independent), but it removes leader churn without changing any prefix or tag.

**P3 (medium, real measured cost): make `ingest_sorted` a first-class bulk-load primitive and wire the migration to it.** VSDB's own exporter (`mmdb/docs/proposals/mmdb-v5-vsdb-v18-improvements.md` L94 and the v4 exporter) already sorts; `ingest_sorted` already skips WAL and memtable (`mmdb/src/db/ingest.rs:167–290`). Add: (i) `ingest_sorted_from_iter` taking `IntoIterator<(K,V)>` with an internal check that completes **after** building (so a non-strict input fails with a documented `InvalidArgument` before any manifest edit), (ii) a variant that accepts *unsorted but globally strictly increasing* input and does an internal sort into a bounded spill file, and (iii) a `prefix` argument so it can target a single VSDB prefix's full key span instead of the whole DB. This is the change that makes **VDB's v17→v18 migration** (`vsdb/migration/`) use a storage-level bulk path instead of per-collection transactions, and it is where the "1,000–10,000 keys per write" cost the comparison never measured actually lives.

**P4 (lower priority, only if measurement justifies): widening the v18 read path.** (i) multi-level Bloom budget (Monkey) using the existing per-level `bloom_bits_for_level` — **zero format change**; (ii) optional bottommost filter elision (`optimize_filters_for_hits`, already plumbed but off by default, `mmdb/src/options.rs:520–545`); (iii) cursor/window iterator for the 500k-node scans — requires a format change (cursor state in the prefix) and should be **deferred** until P1/P2 measurements are in.

**P5 (deferred, do not do now): prefix-relocatable collection state.** Currently every table row of a collection is `prefix || ...`. Moving one collection to another shard or namespace (which `Namespace::close` and `Namespace::new` invite, `vsdb/core/src/common/engine/mmdb.rs` L671–700) means rewriting every row. I recommend **not** designing that now: it changes the on-disk layout, breaks the recovered-prefix allocator (`recover_ceiling`, `mmdb.rs:676–696`), and there is no measurement of the cost it would save.

**P6 (reject): read-only peer access.** `open_read_only` takes `LOCK_SH`, writable takes `LOCK_EX` (`mmdb/docs/usage.md` L58–L64), and VSDB `OpenMode` is process-wide (`vsdb/core/src/common/mod.rs:319–338`). A v5 change to allow `LOCK_SH` while a writer lives would break the writable open lock ordering that `read-only-mode.md` §4.1–4.2 treats as a correctness boundary. **Reject**; document it as a non-goal and, if a second-process read is needed, use RocksDB/Pebble-style `OpenAsSecondary` (a different format) or a separate store.

**P7 (reject): a global MMDB write-buffer manager shared across engines.** `vsdb/docs/proposals/shared-mem-pool.md` L142–166 is explicit that it needs an independent write-path/recovery audit and fault tests; no v5 API supports it, and the default-namespace sizing (`shared-mem-pool.md` L45–47) is already tuned. **Defer to P4.4 at the earliest**, and only after P1/P2 show write-side pressure that static sizing cannot absorb.

---

### 3. Main proposals: operation, causal benefit, alternative, risks/invariants, experiment + decision rule

For each I give the operation it changes, the causal mechanism, the realistic alternative, the correctness invariants that must hold, and a decision rule with a named threshold. All measurements are **end-to-end through VSDB** (engine-level numbers are labeled "engine").

**P1 — Held-view reads (additive `v5` API).**
- *Operation changed:* `MapxRaw::get`, `MapxRawReadView::get/range`, `PersistentBTree::get/iter/range`, `SlotDex::query` reads. Each currently does `ns.engine().read_view(prefix)` (or `Mapx::read_view`) → `DBInner::snapshot()` per call.
- *Causal benefit:* removes one `DBInner` lock acquisition and one snapshot registration per node/point. The v18 evaluation already attributes 885 ns of a 1,313 ns `VerMap::get` to head+lease overhead; the read-view variant should remove a good part of that at collection scale. **Engine-level** number: expected `DB::snapshot` registration cost ~tens of ns per call (hypothesis; measure). **End-to-end** prediction: node-read-heavy tree scans improve more than the ratio suggests because the lock is on the hot path for every node.
- *Alternative:* change `PersistentBTree` to own one `MapxRawReadView` per tree and pass it down. This is smaller than a v5 API and should be tried first. **Recommendation: do the VSDB-side ownership fix first; only add the v5 escape hatch if tree reads still show lock contention.**
- *Risks / invariants:* a `PinnedSlice` from a borrowed snapshot must remain valid after `Drop` of the owning `DB` (already guaranteed for `SnapshotRef`); the escape hatch must reject a view from a different `DB` with `InvalidArgument` (mirrors `usage.md`'s "Using an open snapshot with another database returns `InvalidArgument`"); no new public `Snapshot` type in v5; no change to `Iterator` lending semantics.
- *Experiment + decision rule:* 1M-node `PersistentBTree` (vs. 0.5M) built via `bulk_load`; end-to-end `range(..)` and `get` at 3 cache sizes; plus `read only` engine counters `snapshot_count`/`inner_lock_stall` (add a counter if absent). **Decision rule:** if p50 of node-read drops > 15% and p99 does not regress > 5%, keep. If the tree-side ownership fix alone reaches > 80% of the gain, close P1 as "not needed."

**P2 — Cross-collection write batch.**
- *Operation changed:* `DB::write_batch_group` (currently one WAL record per `WriteBatch`), VSDB `MapxRaw`/`PersistentBTree` commit sites.
- *Causal benefit:* halves (or thirds) the leader/WAL/memtable-insert rounds per logical transaction. It also lets the group-insert happen once for a `VerMap` branch edit instead of once per node, which is the largest source of per-tree-row WAL amplification.
- *Risks / invariants:* (i) sequence assignment must be monotone per batch and **no gaps** inside one group (the group already assigns `first_seq + i`, `mmdb/src/db.rs` L3140–3160); (ii) visibility is the **group** publication, so a crash mid-group must either replay all records or none — MMDB's WAL replay is per record with a checksummed tail (`mmdb/src/wal/reader.rs` `rest_is_zero_padding`, `mmdb/docs/architecture.md` L100–L110), so the group is atomic **at the record level** but the *semantic* atomicity is the caller's; (iii) `DisableWal` batches must not share the group with `WalSync` groups in a way that delays the sync; (iv) VSDB `MapxRaw` batch commit must reject mixed read-only / writable requests.
- *Alternative:* coalesce at the **VSDB** layer — make each collection operation already emit one batch and add `Namespace::commit_all(&[Prefix])`. This avoids any v5 change and is where the fix likely belongs first.
- *Experiment + decision rule:* 1,000 `VerMap` 3-branch inserts, each with one `SlotDex` count update, run as (a) current 2 batches, (b) one `write_many`. **Decision rule:** end-to-end p50 batch rate > 1.5× current with no p99 regression; then promote to `v5` API; otherwise keep the VSDB-side coalescing and close P2.

**P3 — `ingest_sorted` as the migration bulk path.**
- *Operation changed:* `DB::ingest_sorted` input contract and placement (`mmdb/src/db/ingest.rs:126–290`).
- *Causal benefit:* v17→v18 migration (`vsdb/migration/`) writes hundreds of thousands of rows through per-collection transactions; routing those through `ingest_sorted` is a 1× (no-WAL, no-memtable) write path instead of 1× WAL + memtable + compaction. This is the single biggest write-amplification reduction available without changing the format.
- *Risks / invariants:* the gate `inner_overlaps_user_range` checks **point entries and range-deletion partition extents of every level** (`ingest.rs:44–85`) — that is correct and must be preserved, including the `range_partition_extents()` check; the `ingest_ceiling` must be set so concurrent reads do not see ingest rows at a stale sequence (`ingest.rs:270–290`); the single-sequence rule is required to keep pre-existing snapshots valid (`ingest.rs:153–165`); `ingest_sorted` currently rejects **overlapping-but-distinct** keys, so the migration must sort.
- *Experiment + decision rule:* migrate a 10M-row v17 fixture with (a) per-collection writes, (b) `ingest_sorted`, (c) internal-sort variant. **Decision rule:** if (b) completes with < 1.2× the WAL bytes of the current path and end-to-end migration wall-time > 2× faster, wire it into `vsdb_migrate`; else stop and fix the specific hot collection.

**P4.1 — Per-level Bloom budget (Monkey) with `filter.format = 2`.**
- *Operation changed:* `bloom_bits_for_level` (`mmdb/src/options.rs:520–545`) and `l0_build_opts` / `ingest_build_opts` (`mmdb/src/db.rs:303–322`, `ingest.rs:167–190`).
- *Causal benefit:* at fixed total filter bytes, the cumulative false-positive cost of an 8-file probe (`vsdb` has 7 levels + L0) drops. The comparison's own §B estimates ~3.3–6.6× reduction; those are *derived*, not measured, and the 65,536-mask floor makes the gain sensitive to `bits_per_key`.
- *Risks / invariants:* the filter budget must not change `MetaIndexData::bloom` length validation (`filter.rs:68–130`); the build must pass the same `prefix_bloom_bits_per_key` for prefix filters; `compaction` must re-derive the budget per output level, not inherit the input's.
- *Experiment + decision rule:* 1M keys, 7 levels, `bloom_bits_per_key = 12` base; compare uniform vs. `[20,20,20,17,11]`-style allocation at equal total bytes. **Decision rule:** if the *measured* p99 of a miss-heavy workload improves > 10% and the filter byte count is unchanged or reduced, keep. If the measured FPR improvement is < 10× the theoretical prediction, the probe scheme (format 2) is the limit and P4.3 becomes mandatory or the whole item is deferred.

**P4.2 — Optional bottommost filter elision.**
- *Operation changed:* `BloomFilter` construction on `level == num_levels - 1` (and `optimize_filters_for_hits` already exists in `options.rs:520–545`).
- *Causal benefit:* removes ~85–90% of filter memory on the 90%-of-keys live level, allowing `bloom_bits_per_key` to be doubled elsewhere. **No format change** if the reader treats `format = 2` with `bloom: None` as "must look at every SST" (it already allows `bloom: None` when `prefix.is_none()`? — *must be verified*; `table_reader/mod.rs:1194` requires `filter.format` present iff `bloom.is_some() || prefix.is_some()`, so a filterless bottommost SST needs a `filter.format` sentinel, **which is a format change**). **Decision rule:** keep only if the reader can encode "no filter" without changing the persisted `filter.format`; otherwise defer to the same change as P4.3.

**P4.3 — Independent-probe Bloom scheme (format 3, deferred).**
- *Operation changed:* `probes` (`filter.rs:48–66`) + persisted `filter.format` value.
- *Causal benefit:* the 65,536-mask floor caps the benefit at `format = 2`; `format = 3` recovers the Poisson floor. This is the only place where a format change is clearly worth it — but it must wait for P4.1/P4.2 measurements, and it requires the manifest `format_version` bump and old-SST decode the compatibility policy demands.
- *Risks / invariants:* `filter.format` is stored in `MetaIndexData` (`table_reader/mod.rs:1150–1200`); a new value must be rejected by the *reader* unless the manifest format version permits it; all in-repo SST readers must still open v4-era files.
- *Experiment + decision rule:* build a 1M-key, 5-level database with `format = 2` and `format = 3` at 12 and 20 bpk; measure FPR on 10^7 probes. **Decision rule:** ship `format = 3` only if `p99` of a miss-heavy read improves > 20% and the FPR gain is > 5× the format-2 gain. Otherwise keep the format frozen.

**P5 — Cursor/window iterator (defer).**
- *Operation changed:* `PersistentBTree` iteration and `SlotDex` pagination.
- *Causal benefit:* removes per-node `reader()` + `get` round-trips (P1's cousin). But a cursor requires a **cursor state row** in the collection prefix, i.e. a format change, and the v18 evaluation already notes `VecDex` search is 4.6 ms at 1% selectivity (`v18-trie-vecdex.md`), where the cursor is not the bottleneck.
- *Decision rule:* only if P1 shows node-read cost dominates and a cursor reduces end-to-end scan time > 5×.

---

### 4. Concrete collection-layer change grounded in a real caller

**Change: give `PersistentBTree` a single `MapxRawReadView` (one MMDB `Snapshot`) for the lifetime of a tree handle, and pass it to `read_object`/`read_cached_reader_arc` instead of calling `self.nodes.reader()` per node.**

Grounding — the real call chain:

- `PersistentBTree::get(root, key)` → `lookup(root, key, |id| self.object(id))`, and `object` calls `read_map_object` → `nodes.get(...)` (`persistent_btree/mod.rs:1119–1127`, `507–513`).
- `PersistentBTree::iter/range` → `BTreeIter::new(self.nodes.reader(), ...)`, and `BTreeIter` calls `read_object(&self.nodes, id)` per node (`iter.rs:23–60`, `mod.rs:1134–1170`).
- `read_cached_reader_arc` is the *hot* path and calls `nodes.get(object_key(id))`, one engine `get` **and one `DB::snapshot()` registration per node** (`persistent_btree/mod.rs:523–536`).
- `MapxRaw::reader()` → `engine::Mapx::reader()` → `self.prefix_bytes()` → `ns.engine().read_view(prefix)` (`mapx_raw/mod.rs:149–153`, `core/engine/mod.rs:142–160`), which is `DB::snapshot()` on the one shard.

That is a **snapshot registration + `inner` lock per node**, on a workload the initial comparison never runs. The v18 evaluation records `Snapshot::get` = 452–1,178 ns vs `VerMap::get` = 628–1,657 ns — the gap is exactly the head+lease overhead, and a 500k-node range scan pays it 500k times.

Proposed minimal change (VSDB-only, no format change):
1. Add a `BTreeReadGuard<'a>` (or an `impl ReadState`) that holds a `MapxRawReadView` for `&PersistentBTree` and exposes `object(&self, id: NodeId) -> Vec<u8>` (or `Arc<[u8]>` via `read_cached_reader_arc`'s `Arc<[u8]>` path).
2. Change `BTreeIter`/`BTreeReader` to accept `&MapxRawReadView` and use `get`/`get_pinned`-style borrowing rather than `MapxRaw::get` per node. This is internal (`strata` crate), no public API change for callers.
3. Keep `read_map_object(nodes: &MapxRaw, ...)` for the mutable path (it does not need a snapshot: it is a writer).
4. Add a `#[cfg(test)]` assertion that a read of the same tree from two views sees a consistent committed sequence, and that `iter()` on a tree whose writer commits a new version mid-iteration does not resurrect the old root (test against `persistent_btree` `iter` tests).

**Staged migration / crash / rollback for P1 and this read-view change:**

| Stage | What ships | Migration | Crash behaviour | Rollback |
|---|---|---|---|---|
| 1 | `BTreeReadGuard` internal only; no caller change | none — behind the existing `impl` | No on-disk change. If a process dies mid-build, the tree is still a normal versioned `MapxRaw`; next build reloads the root from its handle | Revert the commit; old code compiles unchanged |
| 2 | Switch `iter`/`range` to the guard; add `debug_assert` that the view and the writer's `Seq` are from the same `ns` | none — collection handles are re-opened from `VSTYPE04` handle bytes, which do not encode the guard | A reader crash does not affect the store. A writer crash mid-edit must still go through the existing `pending`/`flush_pending` path; the guard is read-only | Revert commit; per-handle read views are re-derived on open |
| 3 | Export the guard (if callers need it) behind a `#[cfg(feature)]` or a new `MapxRaw::read_view_borrow()` | **Wire format unchanged**; no migration | same | n/a |

**Why this is safe across reopen:** the guard holds a `Snapshot` (registered sequence) that is *shorter* than any writer commit, and `PersistentBTree` derives its version from the same `MapxRaw` handle bytes that are serialized in the `VSTYPE04` frame (`strata/src/common/frame.rs` L40). A crash never loses a committed node; a reader that dies leaves the version intact.

**Cross-namespace case:** the guard is per-`Namespace`; if the tree's `MapxRaw` is `shadow()`ed or `clone_colocated`'d, the read view must come from the same `MapxRaw` used by `object`. Add a `debug_assert` that the guard's `InstanceId` matches the tree's.

---

### 5. Ideas I would defer or reject, and why

| Idea | Verdict | Why |
|---|---|---|
| `filter.format = 3` (independent probes) | **Defer** to P4.3 after P4.1/P4.2 measurement | The 65,536-mask floor is real, but the 6.1–6.6× FPR reduction is a *derived* claim (v5-roadmap §B), not in `runs.csv`; a format change requires a manifest bump and old-SST decode |
| Cursor/window iterator | **Defer** | Format change (cursor state in prefix) without measurement of a node-read-dominated workload |
| Prefix-relocatable collection state | **Reject** now | Changes the 64-bit prefix contract (`common/mod.rs:42–60`), breaks `recover_ceiling`'s recovery-from-highest-issuers path (`mmdb.rs:676–696`), and no cost model supports it yet |
| Multi-engine shared write-buffer manager | **Reject** | `shared-mem-pool.md:142–166` already requires a write-path/recovery audit; no v5 API; not justified by the comparison |
| Cross-collection prefix (logical transaction) spanning `MapxRaw`s | **Reject** | `WriteBatch` has one prefix; `mmdb/src/types.rs:373–406` `put_prefixed` exists but no multi-prefix batch. A transaction must pick one engine (or a sharded write path) and VSDB already has `Namespace::new_colocated` — use that, not a new transaction type |
| Read-only access to a live writable store | **Reject** | `LOCK_SH`/`LOCK_EX` in `usage.md:58–64`; `read-only-mode.md:4.1–4.2` calls the lock ordering a correctness boundary |
| `ingest_sorted` with random-order input | **Defer** | Requires a spill file and a two-pass scan; first make the migration sort (P3) and measure |
| Versioned "working transaction" / B+ tree working transaction | **Defer** | `v18-roadmap.md` L~506 lists "VerMap sweeping and working transactions" as deferred; the Merkle diff (1.69 ms/step for 2,000 keys) is the real cost, not the transaction |
| More shards (16 → 32/64) | **Defer** | `NUM_SHARDS` is pinned for routing compatibility (`mmdb.rs:147–152`); a change is a **format/routing** break and would change every prefix → shard mapping |

---

### 6. Assumptions and hypotheses (explicitly labelled)

- **H1 (high confidence, format fact):** every `MapxRaw` per-namespace table reserves a fixed process-wide `AtomicU64` allocator and a `Mutex`, and the tree/SlotDex/VerMap layers each allocate their own `MapxRaw`. Therefore one collection of each type costs 4–6 allocator/mutex objects, and per-namespace capacity is per-table. **Not measured**; test by allocating N maps of each type and counting RSS/allocator count.
- **H2 (medium confidence, measurement gap):** a 500k-node `PersistentBTree` range scan pays ≥ 200 ns of `inner` lock + snapshot registration per node. **Hypothesis**; the v18 evaluation's per-read 885 ns vs 628 ns ratio is the only anchor.
- **H3 (low confidence, cross-engine comparison artifact):** Pebble's no-sync Put is not comparable to MMDB's. The comparison says so but does not re-normalise; I treat the comparison's absolute Put numbers as *relative engine cost at equal durability acknowledgement*, not equal crash semantics.
- **A1:** `fsync` on the standby-WAL path is measured as part of `freeze_memtable`; the v5-roadmap §A figure (~4.9 ms p999 floor) predates the standby-WAL fix, so the **current** floor is unknown and must be re-measured before P2 is prioritised.
- **A2:** the `ingest_sorted` gate `inner_overlaps_user_range` is called **twice** (before build and after build, `ingest.rs:153–165` and `:230–240`) under `inner` lock each time — that is a **second full range scan of every level** per ingest, and a candidate cost for P3.
- **A3:** VSDB v18 on `mmdb 4.3.15` has no `ingest_sorted` (verified: no caller) and no `snapshot_held`/view escape hatch; the whole P1/P3 set is a **cross-repo** surface change.
- **A4:** the `v18-roadmap.md` §10.7 numbers (127.4 ms vs 87.4 ms; max 26.6 ms vs 0.31 ms batch latency) are from an earlier snapshot (`cc2f41c`) and should be re-run before using them to justify a buffer-size change.

**Bottom line:** the comparison's measured gap is real but *not* the whole story. The most valuable joint change is not a bigger memtable, more subcompactions, or a new filter scheme — it is making the per-node/per-call `snapshot()` and per-operation WAL record disappear from the collection read and transaction paths, which are both invisible to a 128-byte single-key benchmark and both already measurable inside the v18 evaluation. Everything on the MMDB side should be additive and format-neutral until P0 is settled; the one place I would accept a format change is a **probe-scheme** change for Bloom, and only after the per-level budget is shown to be insufficient.

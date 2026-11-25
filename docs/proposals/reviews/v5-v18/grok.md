# Joint release decisions for MMDB 5.0.0 and VSDB 18.0.0

Ship three changes with the first cutover. Keep the on-disk collection format already implemented for v18. Treat the single-host point-operation table as an engine measurement, not as a VSDB end-to-end result.

## Evidence and limits

The supplied comparison is one process, one directory, one LSM, on a Ryzen 9 5900X, Linux 7.0, Btrfs zstd. Five shuffled rounds. Keys are 8 bytes, values 128 bytes of `0x42`. No-sync fills use 131,072 keys. Sync fills use 8,192. Flush and close are outside the write timer. SST compression is off, blocks are 4 KiB, Bloom base is 12 bits/key, write buffer and block cache are 64 MiB. Medians, in thousands of operations per second, from `mmdb/docs/proposals/mmdb-v5-vsdb-v18-improvements.md` and `mmdb/docs/benchmarks/point-ops/runs.csv`:

| Operation | MMDB 5.0.0 | RocksDB 11.13.2 | Pebble 2.1.7 |
|---|---:|---:|---:|
| Put, 1 writer, no per-write sync | 276 | 250 | 1,338 |
| Put, 8 writers, no per-write sync | 414 | 594 | 1,258 |
| Put, 8 writers, sync every write | 6.08 | 7.30 | 6.42 |
| Warm Get after 1 writer | 1,286 | 1,197 | 802 |

Eight writers raise MMDB no-sync throughput 1.50× and RocksDB 2.38×. After the no-sync fill, MMDB and RocksDB each have one L0 SST. Pebble’s no-sync acknowledgement can return before the WAL reaches the OS (`pebble/options.go` at v2.1.7, lines 350–367 in the comparison’s permalink). Only Pebble’s sync row is a durability peer. RocksDB `enable_pipelined_write` was off in that run (RocksDB `options.h` at v11.13.2, the comparison’s lines 1413–1458).

What that run does not measure: VSDB, more than one prefix, a durability fence, L0 backlog, cold cache, compression, 16 KiB blocks, or tail latency. Those are the production path.

VSDB 18.0.0 still declares `mmdb = "4.3.15"` in `vsdb/Cargo.toml`. The sibling engine is `mmdb` 5.0.0. `MmDB::sync_db_wal` still builds `WriteOptions { sync: true, .. }`, which is not a field on v5 `WriteOptions`. v5 durability is `DurabilityMode::{WalAsync, WalSync, DisableWal}` in `mmdb/src/options.rs`. The wrapper also assigns `ReadOptions::iterate_lower_bound` from `Option<Vec<u8>>` while the field is `Option<Cow<[u8]>>`. This pair does not compile as one workspace today. That is a release blocker, and the old call is also the wrong operation.

Engine paths checked against this tree:

- Group commit holds `DB::inner` across vectored WAL append, the group flush or `sync_data`, and sequential `MemTable::put`, then publishes `committed_sequence`. See `DB::write_batch_group` in `mmdb/src/db.rs`.
- `DB::sync_wal` enqueues `WriteRequestKind::WalFence` and adds no batch. A no-op fence whose target is already covered returns before the queue.
- `freeze_memtable` calls `old_wal.sync_data()` before installing the next WAL, then sets `synced_wal_seq`. SST install stays on the flush worker (`mmdb/src/db/flush.rs`, `FlushQueue::flush`). The VSDB comment that a successful write has already installed earlier WALs as SSTs describes MMDB 4.3, not this code.
- `FlushQueue::check_capacity` waits while `submitted - durable >= max_immutable_memtables`. The default and the VSDB open both set `max_immutable_memtables: 4` (`DbOptions::default` in `mmdb/src/options.rs`; `mmdb_open` in `vsdb/core/src/common/engine/mmdb.rs`). On 4.3 that field did nothing. On v5 it allows one active memtable plus four frozen ones.
- `DB::write_memtable_ssts` emits another L0 file only when index metadata crosses `META_BLOCK_SPLIT_THRESHOLD`. A normal memtable becomes one SST whose smallest and largest keys are the whole memtable.
- `LeveledCompaction::pick_l0_compaction` takes every L0 file. `overlapping_files_for_inputs` then takes the aggregate smallest-to-largest span, including gaps, because output is cut by size and an L1 file left in a gap would overlap the output. `max_subcompactions` is forced to 1 when `task.level == 0`. VSDB sets `max_subcompactions: 4` and `max_background_compactions: 1`. The 4 does no L0 work.
- A scan copies a sequence and pins sources. `ReadState::scan` clones memtables and an `Arc<Version>` into `LevelIterator`. It does not keep the `Snapshot` registration. Dropping the snapshot after `MmDB::range` returns is safe for that iterator. Keeping a raw sequence and opening a later read is not. `MmdbReadView` holds the `Snapshot`, which is the right shape for repeated gets.

Collection layout checked:

- Every map key is `prefix.to_le_bytes()` plus the user key (`Mapx::prefix_bytes`). `PREFIX_SIZE` is 8. `PREFIX_ALLOC_START` is `40_960_000` (`0x0271_0000`) in `vsdb/core/src/common/mod.rs`. Shard is `prefix % shard_count` (`NUM_SHARDS` is 16 on the default namespace).
- `Mapx::sync_wal` calls `MmDB::sync_db_wal`, which point-deletes `META_KEY_WAL_FENCE` (`[1u8]`). That key is outside every 8-byte prefix.
- B+ tree pages and blobs share one prefix. `object_key` writes `0x10` or `0x12` after that prefix (`vsdb/strata/src/basic/persistent_btree/nodes.rs`). `PersistentBTree::leaf_val_eq` treats values longer than 1,024 bytes as blobs. `PersistentBTree::object` loads internal pages through `Mapx::get` into an owned `Arc<[u8]>` clock cache.

**Hypothesis, engine versus end to end.** The 8-writer gap (414k versus 594k puts/s) is consistent with one leader holding `DB::inner` for both WAL I/O and memtable insertion. It is not evidence that VSDB’s steady state is 30% behind RocksDB. VSDB adds postcard, an 8-byte prefix, 16 KiB blocks, LZ4 on L0/L1 and Zstd below, a 16-way shard fan-out, and the fence key above. Those costs are absent from `runs.csv`.

## What the comparison missed

A one-byte fence plus little-endian prefixes makes L0 metadata span unrelated collections.

`40_960_000` and `40_960_016` both route to shard 0. Their key prefixes are the little-endian bytes `00 00 71 02 00 00 00 00` and `10 00 71 02 00 00 00 00`. The same shard’s first sixteen prefixes are striped across the first byte (`0x00`, `0x10`, … `0xF0`). One memtable that holds two of them has a key span covering the gap. `pick_l0_compaction` then compacts every L1 file in that gap, because the output would otherwise overlap those files.

`sync_wal` makes this happen for a single collection. The fence key `01` sorts between those two prefixes. The flushed SST’s smallest key becomes `01` while the largest key stays in the collection. The tombstone remains in file metadata until a bottommost compaction drops a deletion older than the oldest snapshot (`mmdb/src/compaction/leveled.rs`, the bottommost `ValueType::Deletion` branch). Until then, later page-only flushes overlap that wide L1 file and rewrite cold keys, including `0x12` blobs that share the collection prefix.

The benchmark never writes a key shorter than 8 bytes and never mixes prefixes, so its single post-flush L0 file does not show this. The v18 roadmap’s “put the hot tree on a 1-shard namespace” advice avoids cross-collection striping for that one tree. It does not stop the fence key from widening that tree’s files, and a 1-shard namespace that holds many maps is the worst span.

The second miss is memory. `mmdb_open` still says that on MMDB 4.3 the immutable cap is unused and residency is at most two buffers. v5 honors the cap. Default namespace, 2 GiB budget, 16 shards: the formula yields a 16 MiB write buffer (`min(1 GiB/16, budget/8/16)` with the 4 MiB floor). Peak memtables become `16 × (1 + 4) × 16 MiB = 1,280 MiB`, plus a 256 MiB shared block cache. The comment’s intended flush peak is about 512 MiB. A 1-shard namespace given a 2 GiB budget gets a 256 MiB buffer; five of those are 1,280 MiB of memtables. Section 10.7 of `vsdb/docs/proposals/v18-roadmap.md` measured synchronous flush on MMDB 4.3.15. It does not describe this queue.

Iterator lifetime is not the missed bug. `LevelIterator::for_level` keeps the `Version` it was built with. Sequence zeroing in a newer version does not rewrite that pinned version. Do not “fix” `MmDB::range` by storing the `Snapshot` for the iterator’s whole life. That would pin MVCC history and block bottommost cleanup for every scan.

## Pre-release decisions

These are compatibility and resource decisions for the first versions that can open a real universe. They do not need a new `VSMAPX02` or a new SST magic.

1. **Cut the VSDB engine wrapper onto MMDB 5 in the same release that publishes either crate.** Map `MmdbBatch::commit` to `DurabilityMode::WalAsync` and `commit_sync` to `WalSync`. Build iteration bounds with `Cow::Owned`. Keep `prefix_len: PREFIX_SIZE`. Keep WAL batch bodies byte-compatible with the v4 record layout already implemented in `WriteBatch`. A v4.3 process cannot open a v5 directory (`DBHeader` format version 5, SST v2). Rollback of data is the existing v17 universe plus `vsdb_migrate`, not a dual reader.

2. **Stop writing `META_KEY_WAL_FENCE` before any supported v18 directory exists.** `Mapx::sync_wal` and `Mapx::try_sync_wal` should call `DB::sync_wal` on the collection’s shard. Details are the collection section below.

3. **Set `max_immutable_memtables` to 1 on every VSDB shard for this release.** Writers still continue into a new active memtable while one frozen table flushes. They wait when that flush is still running and the new table is also full. Peak memtable residency returns to the two-buffer ceiling the sizing comment already documents (about 512 MiB of memtables on the default 2 GiB / 16-shard engine). Do not leave the inherited 4 in place. Revisit 2 only with the cgroup experiment below.

4. **Leave collection bytes alone.** Little-endian prefixes, `0x10` / `0x12` object keys, handle frames, and the 1,024-byte blob split stay. Prefix-aware compaction is an engine behavior on the existing `prefix_len`, not a key-format change.

## Proposals

### 1. Prefix-bounded compaction when `prefix_len > 0`

**Operation.** L0 compaction and flush output cutting. Today one memtable is one L0 SST, and one L0 job unions every L0 span with every L1 file in the gaps.

**Change.** For stores with `prefix_len > 0`, stream compaction keys in order and cut outputs at a prefix change. Include an L1 file only when some input point range or input tombstone extent actually overlaps it. A gap file whose keys were not in the inputs and which no input tombstone covers stays installed and keeps L1 disjointness. Apply the same cut in `write_memtable_ssts` so a mixed memtable becomes one tight SST per prefix instead of one spanning SST. Keys shorter than `prefix_len` (the historical fence, if present) are their own group.

VSDB tombstones are already one prefix: `MmdbBatch::new_wiped` deletes `[prefix, prefix_successor)`. The engine still has to honor a tombstone that crosses prefixes. That tombstone overlaps the gap, so those L1 files stay inputs. Fragment it at prefix boundaries the way SST v2 already fragments tombstones at file boundaries.

**Causal benefit.** A hot collection stops rewriting cold collections that only shared a memtable or a fence key. Blob keys under `0x12` stay put when the L0 file contains only `0x10` pages and the files no longer share a spanning metadata range. L0 file-count pressure stays modest because the default namespace rarely puts hundreds of live prefixes in one 16 MiB buffer. This is the steady-state write path the point-op table never entered.

**Realistic alternative.** Change `Mapx::prefix_bytes` to big-endian so shard-mates cluster. That shrinks spans even with today’s union picker, and it rewrites every `VSMAPX02` prefix, the ceiling scanner (`from_le_bytes` in `note_meta_bytes`), and every migrator fixture. Prefix-bounded compaction fixes the rewrite without a handle change. Big-endian can wait.

**Risks and invariants.** L1+ files stay non-overlapping. All versions of one user key stay in one output. A range tombstone remains visible at every key it covers, at the sequences captured for the job. Flush and compaction still publish every new SST in one `VersionEdit` after the directory sync (`FlushQueue::flush` already adds every output to one edit). A crash before that edit leaves only unreferenced pending files. Trivial moves of an already single-prefix file stay legal. `max_subcompactions` can split disjoint prefix groups. It must not split one overlapping group into jobs that each need the same tombstone inputs.

**Experiment and decision rule.** One shard, `prefix_len = 8`, little-endian prefixes `40_960_000 + 16k` for `k = 0..64`, 1 KiB values. Overwrite only prefix 0 for three write-buffer cycles while the other 63 prefixes are already at L1. Also run one cycle that calls the old fence delete once. Adopt if cold-prefix SST bytes rewritten fall by at least 5× versus today’s picker, hot put throughput drops by at most 10%, and a tombstone covering two prefixes still hides the gap keys at a snapshot taken before the job. Reject the cut if the job ever installs an L1 range that overlaps a file it did not take as input.

### 2. Collection durability: `Mapx::sync_wal` calls `DB::sync_wal`

**Caller.** `Mapx::sync_wal` and `Mapx::try_sync_wal` in `vsdb/core/src/common/engine/mod.rs`. Implementation is `MmDB::sync_db_wal`. History and batch callers that need a durable prefix already go through this method (`try_sync_wal` on maps, namespaces, and typed tests).

**Operation.** Replace the synchronous point deletion of `[1]` with `DB::sync_wal` on that shard only. Untouched maps still must not allocate a prefix. Read-only remains a no-op.

**Causal benefit.** Durability no longer inserts a key. File metadata stops growing a permanent low bound from the fence. Sequence numbers and the memtable stop advancing on a pure fence. `DB::sync_wal` already coalesces with the group leader and `sync_data`s the active WAL. Rotated WALs were sealed in `freeze_memtable` before the new WAL was published, so a fence record is not required to cover them.

**Realistic alternative.** Keep the deletion and teach the picker to ignore keys shorter than `prefix_len` when computing spans. The tombstone would still occupy the memtable, advance MVCC, and need a special case in every overlap helper. `sync_wal` already exists and adds no record.

**Risks and invariants.** `sync_wal` must not acknowledge a sequence the active WAL has not flushed. After rotation, `synced_wal_seq` covers the sealed WAL. A failed `sync_data` still fail-stops the database, as the fence path does today. Empty `WriteBatch` skips remain. Do not implement the fence as `delete` plus `WalSync`. Concurrent `WalAsync` writers on the same shard stay ordered by the same group commit. A namespace `try_sync_all_wals` fans out per shard. There is still no cross-shard atomic commit. That is the current collection contract. One batch is one prefix and therefore one shard.

**Staged migration, crash, reopen, rollback.**

- Stage 0, before the tag: land the caller with the MMDB 5 dependency bump. No manifest version bump. No handle rewrite. `vsdb_migrate` does not need a new prefix encoding.
- Directories created by development builds may already contain a deletion of key `01`. Prefix iteration does not yield it. User gets are unchanged. The wide `smallest_key` shrinks when a later compaction drops that deletion at the bottommost level. No offline rewrite is required for user data. Rebuilding development universes is enough if wide files are inconvenient before that compaction.
- Crash after `sync_wal` returns: the committed WAL prefix is durable even if the memtable has not been installed as an SST. Reopen replays residual WALs, including multiple sealed generations, through the v5 recovery path. Crash before `sync_wal` returns: the same `WalAsync` tail can be lost as with a fence that had not yet `sync_data`’d. Read-only open still replays into memory and requires a stable directory. It does not tail a live writer.
- Rollback of the VSDB binary to a fence-writing build, on a v5 directory, only starts writing `01` again. Rollback of the directory to MMDB 4.3 is unsupported. Keep the pre-migration v17 copy until the new universe has been reopened and checked.

**Experiment and decision rule.** Crash harness, not a throughput bake-off. Process A writes `WalAsync` batches, calls `Mapx::try_sync_wal`, then `simulate_crash` is the wrong tool after a successful return. Kill after return and reopen. Every fenced batch must be present. Kill after a `WalAsync` write and before `sync_wal` returns. Those writes may be absent. A second case writes the old fence key into an existing SST, reopens with the new binary, and checks that a prefix scan of a real collection neither returns key `01` nor fails. Ship only if both hold. A useful side measurement is L0 `smallest_key` length after a sync plus flush: it should be 8 plus the user key, not 1.

### 3. Make the immutable-memtable cap match the budget comment

**Operation.** Open options in `mmdb_open`, and the backpressure in `FlushQueue::check_capacity`.

**Change.** Pass `max_immutable_memtables: 1`. Update the comment so it describes v5: one sealed table may flush while writers fill the new active table. When both are full, writers wait or, if `no_slowdown` is ever used, receive `ErrorKind::Busy`. Keep the current buffer formula for this release.

**Causal benefit.** Avoids an accidental 5× memtable footprint against a budget that was written for 2×. Default engine peak returns to about 512 MiB of memtables and 256 MiB of block cache, plus per-tree page caches (`clamp(budget/64, 64 KiB, 16 MiB)` each). This is the cgroup `memory.high` failure the comment already records. It will not show up in `runs.csv`, which used one 64 MiB buffer and did not fill a queue.

**Realistic alternative.** Keep 4 and divide `write_buffer_size` by 5. That preserves more flush slack and cuts the batch of keys that fit in RAM before a stall. The §10.7 burst (60 MiB into a 256 MiB dedicated buffer) would start flushing. Prefer the documented 2× ceiling until a stall measurement says the extra queue is worth the RSS.

**Risks and invariants.** `check_capacity` runs before sequence assignment, so a waiting writer does not publish a hole. Flush failure still latches `bg_error` and must not advance `log_number` past the failed generation. `DB::close` still drains or fail-stops. Recovery may see the active WAL plus one sealed predecessor, which v5 already replays. Setting the cap to 1 does not restore 4.3’s synchronous flush. Acknowledged `WalAsync` data can sit in a frozen memtable and a sealed WAL while the SST is still building.

**Experiment and decision rule.** Default 16-shard engine and a 1-shard namespace, budget 2 GiB, sustained overwrite of one prefix, cgroup `memory.high` at the budget. Record RSS, flush wait, p99 put, and L0 file count for `max_immutable_memtables` in {1, 2, 4} at the current buffer size. Keep 1 unless 2 cuts p99 put by at least 25% and peak RSS stays under the budget. Do not adopt 4 unless RSS stays under the budget and the stall rate is lower than at 2. Run this after proposal 1, or the L0 span rewrite will dominate the tail.

## Follow-ups, after the cutover is stable

**Two-stage group commit.** Split `write_batch_group` so the next group’s WAL append can overlap memtable insertion. The v5 roadmap’s P4 section already states the invariants: drain the memtable queue before rotation, validate admission before the WAL append, and publish `committed_sequence` in order. Standby WAL creation is already implemented. Do not also rewrite the skiplist to lock-free CAS in that change. The arena mutex is uncontended while one leader inserts.

**Hypothesis.** Most of the 1.50× versus 2.38× scaling gap is serialized memtable insertion under the same mutex as the WAL flush. **Experiment.** Same driver as `runs.csv`, writers in {1, 2, 4, 8, 16}, one key per batch, `WalAsync`, five fresh processes. Adopt if 8-writer throughput rises by at least 20% and p99 acknowledgement across a WAL rotation rises by at most 10%. A crash test must show no record from the new WAL applied to the old memtable. If stage 2 CPU is not saturated, stop. A concurrent skiplist is a later project.

This is an engine result. Re-measure one VSDB map only after it passes. Postcard and prefix encoding can hide a 20% engine gain.

**Borrowed page decode, same caller as the copy.** `PersistentBTree::object` is the real read path for `get`, `estimate_entries`, and `count_nodes`. For a cache miss on an internal page, `DB::get_pinned` can feed `NodeView::open`, then `PinnedSlice::into_vec` only when the clock cache stores the `Arc<[u8]>`. Leaf and blob reads that already need an owned buffer can stay on `get`. **Invariant.** The page cache must not store a `PinnedSlice`. That would retain a memtable arena or a whole SST block for the life of the cache entry, outside `block_cache_capacity`. No format change, no migration. Rollback is a code rollback.

**Existence checks.** `Mapx::try_contains_key` calls `DB::get` and drops the bytes (`MmDB::try_contains_key`). Point it at `DB::contains_key`. Tree `contains_key` still has to read pages. No migration.

## Decline or defer

- **Engine blob files (`format_version` 6) for this release.** VSDB already stores btree values larger than 1,024 bytes under `0x12`. Another indirection adds a read and a garbage-collection protocol. Revisit only if, after prefix-bounded compaction, compaction of a hot `0x10` range still rewrites large `0x12` payloads at a measured rate that dominates WAL bytes. A second Mapx prefix for blobs would also break the one-prefix batch unless both prefixes are forced onto the same shard. That is a format and crash-recovery project, not a cutover fix.
- **Big-endian collection prefixes.** Real locality win, disproportionate format cost while proposal 1 exists. v18 has not shipped, which is why it is tempting. It still touches every embedded handle and the v17 converter. Decline for this tag.
- **Raising `max_subcompactions` or `max_background_compactions` as the L0 fix.** L0 jobs ignore the subcompaction count. Sixteen shards already start one compaction thread each.
- **Matching Pebble’s no-sync throughput.** That acknowledgement can precede the kernel write. VSDB’s `WalAsync` flushes to the OS before return. Keep that.
- **MultiGet, column families, merge operators, range keys, pessimistic transactions, overlapping ingest, and online checkpoint.** No current caller issues a batch of independent point gets. B+ tree descent and HNSW search are dependent reads. A prefix scan is the multi-key API VSDB already uses. Shards are the namespace and the atomicity domain. Cross-shard batches would be a new product contract. `DB::ingest_sorted` remains disjoint-range only. The migrator writes through collection APIs. Checkpoint and secondary tailing stay out of this release. They need a file-set stability story on top of live WALs and the Unix `LOCK`.
- **Backward memtable links.** Worth measuring only for a memtable-resident reverse scan. SlotDex’s descending walk is the candidate caller. SST scans are a different path. Do not block the release on it.
- **`optimize_filters_for_hits`.** Misses still matter for vacant map entries. Prefix filters already follow `prefix_len`. Leave whole-key bottom filters on.
- **Holding `Snapshot` inside `MmdbIter`.** The pinned `Version` is the scan’s consistency mechanism. An extra registration would stall tombstone and sequence cleanup for every open iterator.

## Release order

Wire the v5 API and `DB::sync_wal`, and set the immutable cap to 1, before any public v18 directory is supported. Land prefix-bounded compaction in that same engine release if its overlap test passes. If the test slips, ship the fence removal anyway so new files stop growing a `01` bound, and do not advertise multi-map compaction cost from `runs.csv`. Pipeline the write path only after the decision rule above. Leave `0x10` / `0x12`, little-endian prefixes, and SST v2 as they are.

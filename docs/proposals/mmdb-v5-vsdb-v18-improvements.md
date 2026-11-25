# Joint proposal for MMDB v5 and VSDB v18

**Status** — Final design recommendation for maintainer decision, 2026-11-25.
The engineering review covers MMDB `4916abb` and VSDB `7082a88`. The 60
point-operation runs used MMDB `f1cb8f6`. Proposed performance gains still
require measurement on the ported stack.
[Four independent reports, claim checks and test results](reviews/v5-v18/README.md)

## Decisions before the first releases

Adopt MMDB v5 in `vsdb_core` and define the joint upgrade path first. For
small writes, the strongest evidence is a throughput gap under concurrency.
For reads, v5 can remove the temporary value copy in the VSDB adapter.
Ship the native API port, a tested conversion path, and a shard memory
profile with explicit accounting together. Keep new collection layouts and
engine concurrency changes independently releasable. The first optimization
trial should measure a foreground map update during a large VerMap batch on
the same shard. That result helps distinguish queue contention from a cost
VSDB can reduce by changing the unit of submission.
[Measured point operations](#measured-point-operations) · [Existing v5 implementation and deferred work](v5-roadmap.md#72-implementation-outcomes) · [VSDB v18 design record][vs-roadmap]

| When | Proposed decision | Responsibility and reason |
|---|---|---|
| Before v5/v18 cutover | Adopt native fences, DB-bound read views and prefixed batches. Agree on retryable admission errors and the offline upgrade matrix. | MMDB defines durability and file recovery. VSDB maps those into shard, collection and handle semantics. A joint cutover lets operators convert and validate both formats within one outage. |
| First performance investigation | Compare a collection operation batched by VSDB with the same workload on MMDB's current and candidate write paths. Investigate L0 separately under sustained overwrite. | VSDB can remove submissions. MMDB can shorten the time spent serving each submission. Both should be compared with today's batched path. |
| Resource gate | Select the VSDB shard write-buffer profile under sustained load before cutover. Account separately for shared cache entries, overflow, pins and immutable memtables. | v18 currently carries over a v4 setting whose effect changes in v5. Size the system using observed memory and stalls. |
| Additive candidates | Trial batched reads within one VecDex expansion and staged VerMap edits on one branch. Scope the first multi-prefix operation to co-located DagMap child creation. | Each has a real caller and can be compared with a cheaper alternative on the ported stack. |
| Format decision | Keep inline SST values, current handle/prefix encodings and the existing VecDex graph layout for the first release. Revisit physical checkpoints and generation-based graph rebuilds against a specified restore or in-place memory requirement. | VSDB knows which graph or tree objects are reachable. MMDB knows which bytes compaction rewrites. The small-value benchmark measures neither cost. |

At `7082a88`, the VSDB v18 workspace declares MMDB 4.3.15. The
[comparison](#mmdb-rocksdb-and-pebble) covers 60 direct engine runs. Joint
development can use the sibling v5 checkout for the runtime while retaining
a v4 reader for offline conversion. The release sequence would publish the
verified v5 dependency before the v18 crates that consume it.
[VSDB workspace manifest][vs-manifest] · [Shard adapter][vs-adapter]

## Integrate existing v5 primitives

- **Commit and fence one shard.** Map ordinary and synchronous VSDB batch
  commits to `WalAsync` and `WalSync`. Replace the synchronous delete of
  `META_KEY_WAL_FENCE` with `DB::sync_wal()`. This removes a WAL record,
  sequence allocation and tombstone from each fence. Keep synchronous
  publication as one synchronous write. A separate commit followed by a
  fence exposes the new state before a durable acknowledgement. A namespace
  fence still visits each shard and retains its read-only behavior. Removing
  the one-byte fence key can also narrow a flush file's key span when that key
  sorts outside the user rows. Measure this effect before changing the picker.
  [MMDB durability](../usage.md#write-and-choose-durability) · [VSDB fence][vs-fence] · [VSDB batch contract][vs-batches]
- **Keep ownership at the caller boundary.** Use `DB::contains_key` and
  `Snapshot::contains_key` for existence checks. For decoders that consume
  bytes immediately, pass a slice from `get_pinned` or a lending scan step,
  then release it. Keep owned results for callers that retain values. Port
  option literals to builders, prefix bounds to v5 read options and iterator
  failures to typed errors. A detached range must retain its captured view
  after the originating read-view handle is dropped. Measure allocations on
  hits and scans as well as memory retained by outstanding pins.
  [MMDB reads](../usage.md#read-pin-and-iterate) · [VSDB read views][vs-views]
- **Encode prefixed batch entries directly.** Pass the eight-byte map prefix
  and user-key suffix to `WriteBatch::put_prefixed`, `delete_prefixed` and
  `delete_range_prefixed`. This avoids assembling a temporary full key,
  including the heap fallback for long keys. A whole-map wipe still uses
  the successor of the map prefix and precedes replacement rows in the batch.
  [VSDB batch builder][vs-adapter] · [MMDB prefixed encoding](../../src/types.rs)
- **Make a retry decision possible.** Today a valid `no_slowdown` write can
  return `Busy` at the full immutable queue and `InvalidArgument` at the L0
  slowdown threshold. Propose `Busy` for either transient admission refusal,
  with a corresponding retryable classification in VSDB's fallible API.
  A rejected request consumes no WAL record or sequence. A malformed batch
  remains an argument error. Check both stall paths and error propagation
  before documenting the v18 contract. [Current MMDB error behavior](../usage.md#errors) · [VSDB storage errors][vs-errors]

## Reduce the cost of a logical write

**VSDB can change the unit of submission.** A DagMap child is created on
its parent's shard. Today child metadata is written first, then a parent
batch advances the edge allocator and registers the child. Evaluate a
`vsdb_core` operation that takes validated, co-located map handles and builds
one MMDB `WriteBatch` across their prefixes. DagMap could publish those three
rows together with one acknowledgement. Keep prefix allocation durable under
its existing rules. An unused allocated prefix after interruption is safe.
The required visible outcomes are the old relationship or the complete new
one, including the parent's advanced edge ID. Reject mismatched engines or
shards before any append, preserve caller alias exclusion, and keep large
multi-phase prune operations on their resumable path. This requires a routing
and batching change in VSDB and uses MMDB's existing prefixed mutations.
[DagMap child creation][vs-dag] · [Co-location checks][vs-colocation] · [MMDB batch encoding](../../src/types.rs)

**For dependent edits, publish one VerMap working root.** Today
`VerMap::write_batch` already combines edits whose values are known up
front. A proposed branch edit session would also let a later edit read its
own earlier writes. Under the existing writer-exclusion contract, read
through a bounded overlay over one captured branch root, then use the
existing working-root publication path once. Aborting before publication
leaves the prior root visible. An I/O error during publication follows the
existing reopen-and-reconcile path. Keep the initial overlay in memory
with an explicit size limit. If a later variant spills objects, reuse the
existing stage records and durable next-object-ID floor so recovery cannot
reallocate an unpublished ID. A subsequent history commit keeps its
normal durability semantics. Compare allocations, page rewrites and root publications
per completed read-modify-write sequence with individual edits. Use the
existing batch as the control for precomputed edits. This offers a
collection-scoped alternative to implementing general MMDB transactions.
[VerMap batch publication][vs-vermap] · [Branch and snapshot semantics][vs-versioned]

**MMDB can overlap work across submissions.** In the short point benchmark,
eight writers achieved 414k puts/s against RocksDB's 594k without per-write
sync. The group leader holds `DB::inner` across WAL work and ordered
memtable application. First measure queue wait, group size, WAL append/sync,
apply time and wakeups for scalar puts, same-shard DagMap edits and VerMap
multi-entry batches. If WAL and apply each occupy material time, evaluate a
bounded queue between one WAL sequencer and one ordered memtable writer.
Sample queue depth, group size and wakeups outside the caller's timed loop.
The current group drain already has a byte limit and allows one larger legal
batch through. A second stage must admit that case. Polling `DB::get_property`
on every write would add contention on the mutex being measured.
For a fixed group size, WAL time (W) and apply time (A), ideal overlap
reduces the saturated interval from `W + A` towards `max(W, A)`. If one
stage dominates, target its cost or the batch shape first. Queue handoff
and contention reduce the gain available from overlap.
[Current group commit](../../src/db.rs) · [Earlier pipeline design](v5-roadmap.md#72-implementation-outcomes) · [VerMap batch publication][vs-vermap]

Bound admitted bytes and reserve per-generation sequence and same-key capacity
before appending. Expose only a fully applied sequence prefix. Drain in-flight
groups before switching WAL/memtable generations, installing a sorted ingest,
or closing. Preserve per-request completion and fences across mixed
`WalSync`, `WalAsync` and `DisableWal` groups. Inject a pause between WAL
and apply and fail an append or sync in a multi-group run. Snapshots must
never see half a batch, and every successful `WalSync` must survive reopen.
In the same-shard test, time both new snapshot registration and reads from
an already-held view alongside the writer. Registration uses the same
`DB::inner` lock as group WAL sync. Repeat with the large batch on a
different shard, and with both compressible and high-entropy payloads on a
filesystem with compression disabled. Report filesystem and SST compression
separately. Aggregate puts/s can hide latency paid by a small foreground
request. The single-writer apply lane also avoids adding concurrent arena splicing to
this experiment. [Write and recovery invariants](../architecture.md)

**Treat L0 stalls as a separate bottleneck.** The short fill left one MMDB
L0 SST. To investigate a backlog, keep the full automatic L0 input claim
and its overlapping L1 inputs, but try running one job in half-open user-key
ranges split at existing L1 boundaries. Each range sees all covering range
tombstones, including those beginning in another range. All versions of a
user key stay together. Clip tombstones at outputs and install all outputs
in one revalidated MANIFEST edit. Fall back to a serial job if the target
offers too few boundaries. Repeated seeks across L0 inputs can outweigh the
parallelism. Compare serial and split jobs through many flushes, with fixed
total worker and memory budgets across shards. Require lower stall time or
p99 at matched admitted bytes/s, and identical snapshot/reopen results with
cross-boundary deletions, racing flushes and compaction filters.
[Picker, subcompaction and install](../../src/compaction/leveled.rs) · [VSDB shard sizing][vs-sizing]

VSDB offers a second way to study L0 cost. Preload several cold map prefixes
on one shard, then interleave updates to two hot prefixes with cold keys
between them. Compare native fences with the former one-byte delete before
trying output cuts near prefix boundaries. Keep a minimum output size and
cap files per flush. One SST per touched prefix can itself trip MMDB's
default file-count thresholds of 4, 8 and 12. Omit a cold L1 file only if
every new output interval stays disjoint from it and crossing tombstones
remain effective at all live snapshots. Pages and logical blobs of a single
map share an eight-byte prefix, so a prefix cut cannot separate them. Use
cold bytes rewritten, file counts and foreground stall time to choose
between splitting the job and narrowing its input span.
[L0 overlap selection](../../src/compaction/leveled.rs) · [Flush file construction](../../src/db.rs) · [Compaction thresholds](../../src/options.rs)

## Change how collections read and rebuild

**Batch one VecDex neighbor list.** Expanding an HNSW node yields a list of
candidate IDs. VSDB currently fetches each eligible vector separately from
the same read view. Propose a fallible, single-shard MMDB multi-get with one
pinned read view and sequence, input order preserved, duplicate keys
preserved, one missing result per absent key and a typed error on failure.
Group candidate keys by SST and block while applying the same point/range
deletion rules as scalar reads. VSDB would construct physical keys inside
the prefix-bound raw view and decode a bounded group of vectors immediately.
Replay distances and filter callbacks in traversal order, respecting ties,
the visited set and visit cap. The next expansion still depends on those
distances. A VerMap logical key also needs a tree descent before its physical
leaf is known. [MMDB scalar lookup](../../src/db/read.rs) · [HNSW expansion][vs-hnsw] · [Vector fetch][vs-vectors]

Start with scalar pinned decoding on the same v5 read view. Compare it with
one grouped lookup per expansion for 1, 8 and 32 candidates, in hot and
disk-backed indexes. Collect end-to-end query p99, bytes copied, blocks read
and peak pinned bytes. The scalar and grouped paths must produce the same
IDs, distances and filter behavior on the same graph, including while a
writer updates it. Adopt the grouped path if it earns its sorting and memory
cost on real queries.

**Test a cheaper collection read path first.** Use an internal VSDB callback
to decode a value while its MMDB pin is alive. The caller gets its usual
owned typed value and decoding errors. For `PersistentBTree`, a successful
`find_leaf` opens the leaf, and `lookup` opens the same bytes again.
`BTreeIter::next` also opens its current leaf on each row. `NodeView::open`
checks the header and slot-array bounds in constant time. Try retaining
those fields beside the owned leaf bytes. Compare this change separately
from projected decoding, counting object reads, bytes copied and elapsed
time for point hits, misses and bounded scans. Preserve range bounds, blob
lookups and the existing root lease. A mutable tree handle must still see
subsequent root publications. [Tree lookup][vs-tree] · [Leaf header][vs-node] · [Tree iterator][vs-tree-iter]

**Give a large VecDex rebuild a bounded-memory publication path.** Current
`compact()` stages a replacement graph in one wiped batch. For callers that
must retain the existing `InstanceId`, consider moving the active graph
generation into a small state record within the same prefix. Build an inactive
generation in bounded, WAL-enabled batches on that shard. Record its identity
before staging and preserve the node-ID allocation floor. Stream the mapping
and vector checks before switching, including counts, canonical keys,
dimensions and finite components. If validation fails, readers keep using
the original graph. Publish the completed generation selector with one
`WalSync` batch. Hold the collection's writer exclusion or atomically check
a mutation revision at publication so concurrent updates cannot disappear.
Readers resolve state and rows at one snapshot. Retire old ranges after
publication while older views can still read them. Reopen discards an
incomplete build or resumes cleanup after a completed switch. Bound vector
materialization, construction scratch and insertion-order randomization as
well as WAL batch size. Measure peak RSS, temporary disk, rebuild time,
search recall and foreground p99 against today's rebuild
and a fresh-index rebuild with an application handle swap. Moving this
candidate into a later release needs a tagged graph layout and conversion of existing
graphs. Until a stable-identity rebuild demonstrates the need for that cost, keep
the existing layout for the first release. [Current VecDex rebuild][vs-compact] · [v18 design alternatives][vs-roadmap]

## Resource ownership and physical formats

**Make write-buffer sizing part of v18 cutover.** In VSDB's 2 GiB,
16-shard configuration, each shard gets a 16 MiB write-buffer setting.
At the configured immutable limit of four, one active plus four frozen
buffers per shard represents 1,280 MiB of nominal thresholds before caches
or query memory. Actual residency depends on admission, batch overshoot,
allocation and pins. A limit of one can impose a foreground stall.
`FlushQueue::check_capacity` runs before every nonempty write and can block
as soon as one submitted generation is waiting for durability, even with
room in the new active table. Compare limits of 1, 2 and 4 with adjusted
per-shard sizes at a fixed process budget. Choose the release profile from
measured peak RSS, admitted bytes/s and write p99 on both a single hot shard
and simultaneous writers across shards. [VSDB sizing][vs-sizing] · [Write admission](../../src/db.rs) · [Flush queue](../../src/db/flush.rs)

For the initial port, report the configured capacities of the segmented
and overflow caches separately and allow for both in VSDB sizing. Count
pool-wide usage once across shards and keep member pins separate. A later
aggregate byte budget is viable if it still admits a valid block larger
than one segment, evicts and invalidates it correctly, and meets the latency
budget on mixed block sizes. Capping the overflow cache at one segment
would exclude every entry for which it exists.
[Cache admission and tests](../../src/cache/block_cache.rs) · [Shared-pool tests](../../tests/shared_cache.rs)

| Decision | Proposal and evidence needed |
|---|---|
| **Budget across shards** | A VSDB engine already shares a block cache across its shards. Its v4 buffer sizing assumes a synchronous flush. In v5 a shard can retain an active table plus queued immutable tables. Have MMDB expose distinct mutable, immutable, cached and pinned byte estimates. Have VSDB account for the shared pool once and include collection page caches. Evaluate per-shard limits first, then a shared write-buffer budget that can flush a busy shard before throttling writers. Compare one hot shard and 16 simultaneously active shards at the same process budget, including reopen, RSS and write p99. Keep enough headroom for a flush to complete under pressure. [MMDB cache and flush](../architecture.md) · [VSDB memory design][vs-memory] |
| **Large values** | VerMap already stores values larger than 1 KiB as separately keyed logical blobs and reuses unchanged descriptors during edits. MMDB still compacts the payload of each surviving KV row. Measure VSDB logical bytes, WAL, flush and compaction bytes separately on retained branches with 1/16/64 KiB values. Compare descriptor reuse and batching first. Reopen MMDB physical value separation only if repeated live-payload rewrites dominate. A blob design must sync payloads before publishing references and account for pinned readers, checkpoints, mixed-live-file GC, read latency and total temporary disk. [Tree layout and earlier experiments][vs-roadmap] · [MMDB value-separation design](v5-roadmap.md#72-implementation-outcomes) |
| **Physical checkpoint** | Propose one MMDB directory at a committed cut. Fence writer admission while earlier generations, including `DisableWal` writes, reach durable SSTs. Capture the sequence and Version and secure the live SST names or open files against compaction unlink. Release writers before bulk copying. Generate an independent MANIFEST, matching empty WAL, allocator floor, `CURRENT` and lock file. Sync and publish an absent destination from staging. Verify the copy opens independently after source compaction and close, and measure foreground pause as well as copy time. A VSDB universe checkpoint additionally needs a coordinated cut across shards, namespace registry, allocator and application-owned handle files. Require application participation for those files. Until that cut is defined, use a quiesced full-universe backup for migration. [MANIFEST snapshots](../../src/manifest/version_edit.rs) · [Obsolete SST cleanup](../../src/compaction/leveled.rs) · [VSDB backup scope][vs-migration] |

The proposed v5 baseline keeps the 16-byte MANIFEST header, SST v2 and inline
value records. Additive read APIs, shard batching and parallel compaction
can evolve with that format. For any accepted incompatible engine or graph
encoding, assign an explicit version discriminator, specify older-reader
rejection and identify a decoder or export path for existing development
stores before writing the first new file. Keep the MMDB physical version,
the VSDB universe marker and each collection's schema distinct.
General conflict-checked transactions, merge operands, column families,
catch-up readers and overlapping SST ingest remain options if a concrete
caller needs their guarantees. The [feature comparison](#features-that-change-the-callers-design)
records their semantics. [MMDB format checks](../../src/manifest/version_set.rs) · [VSDB compatibility policy][vs-policy]

## Cutover and acceptance

For either v4 source below, stop writes across the universe and its
application-owned files and protect a complete offline backup. Inventory
namespace IDs, shard placement and allocator floors at that cut. Rebind
explicit namespace paths to the staged external roots before any converter
opens them. A missing shard in an established engine aborts the cutover.
Preserve a pending initializer so VSDB can resume it through its normal
lifecycle. Only existing, anchored v4 stores are inputs to the shard exporter.

| Starting store | Proposed route to v18 on MMDB v5 |
|---|---|
| Fresh universe | Create v18 metadata and v5 shards directly using the agreed geometry. Verify read-only inspection and writable reopen. |
| v17 collections on MMDB v4 | On the isolated copy, run the v4-backed v17→v18 converter and the application's typed conversions for saved and embedded handles. Verify that v18-on-v4 copy, close it, then take the shard-conversion path below. |
| v18 collections on MMDB v4 | Run the v4 exporter for each existing shard into a fresh staged directory with its source geometry. Preserve the surrounding VSDB metadata and routing. Export creates fresh engine sequence history while preserving visible keys and values. |
| v18 collections on MMDB v5 | Open a validated, complete generation normally. An interrupted staging tree can be rebuilt from the protected source. Diagnose unexpected formats or a mixture of shard generations before cutover. |

Keep VSDB's logical collection conversion and MMDB's physical shard export
as independently verifiable steps within the same offline window. The MMDB
exporter preserves visible key/value pairs. Old sequence numbers, obsolete
row versions and physical tombstones do not travel to the new engine.
Existing logical prefixes, shard placement and allocator floors do.
The current `vsdb_migrate` also depends on MMDB and reads legacy shards.
Keep a v4-capable offline converter available when moving the v18 runtime to
v5. Verify the staged inventory, handles, allocator ceiling, branch roots,
DAG child mappings, proofs and vector queries before switching the application
to the complete generation. For each closed source shard, compare an ordered
digest of length-delimited visible key/value pairs and the row count with its
destination, checking the iterator's final status on both. Then open the
staged generation through the **writable v18 adapter**. A generic v5
read-only open adopts the stored geometry and can succeed even if the export
used a zero prefix length where VSDB requires eight. For external paths on
different filesystems, use
an offline configuration switch to a complete set of staged paths. Separate
directory renames cannot publish them atomically. Before accepting new
writes, rollback can select the protected source with its matching binary.
After new writes, rollback also needs reconciliation or replay.
[MMDB exporter and publication](../migration.md) · [VSDB upgrade order and handle conversions][vs-migration] · [Legacy shard inspection][vs-migrate-engine]

**Later verification.** First port the adapter and run the joint conversion on
frozen v17 fixtures and real v4-format shards with residual WALs, an external
namespace and saved handles. Interrupt between shard exports and around final
publication. A successful cutover must read every restored handle and allocate
new IDs above the preserved floors. Then test each performance candidate
against the ported baseline in isolation on x86_64 and AArch64. Match logical
operations, durability, memory, compression and concurrency. Repeat runs
through multiple flushes. Record throughput, read/write p99 and run-to-run
spread, with single-writer and warm-read controls. Carry a candidate forward
when its intended workload improves beyond that spread and recovery,
snapshot visibility and the agreed resource budget hold. A candidate that
needs more evidence can ship separately from the joint cutover.
[MMDB recovery tests](../../tests/crash_recovery.rs) · [Exporter fixtures](../../tools/v4-export/tests/golden.rs) · [VSDB migration fixtures][vs-fixtures]

[vs-tree]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/strata/src/basic/persistent_btree/mod.rs#L507-L605
[vs-node]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/strata/src/basic/persistent_btree/nodes.rs#L49-L83
[vs-tree-iter]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/strata/src/basic/persistent_btree/iter.rs
[vs-adapter]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/core/src/common/engine/mmdb.rs
[vs-batches]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/docs/proposals/durable-batches-and-read-views.md
[vs-colocation]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/core/src/common/engine/mod.rs#L494-L533
[vs-compact]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/strata/src/vecdex/mod.rs#L1347-L1448
[vs-dag]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/strata/src/dagmap/raw/mod.rs#L227-L264
[vs-errors]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/core/src/common/error.rs#L150-L169
[vs-fence]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/core/src/common/engine/mmdb.rs#L457-L492
[vs-fixtures]: https://github.com/rust-util-collections/vsdb/tree/7082a888e8defc9a563653b14b0e0b16277f6496/migration/tests/fixtures
[vs-hnsw]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/strata/src/vecdex/hnsw.rs#L204-L245
[vs-manifest]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/Cargo.toml#L16-L21
[vs-memory]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/docs/proposals/shared-mem-pool.md
[vs-migrate-engine]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/migration/src/universe.rs#L493-L549
[vs-migration]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/migration/README.md
[vs-policy]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/docs/ai/compatibility-policy.md
[vs-roadmap]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/docs/proposals/v18-roadmap.md
[vs-sizing]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/core/src/common/engine/mmdb.rs#L1457-L1553
[vs-vectors]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/strata/src/vecdex/mod.rs#L1463-L1532
[vs-vermap]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/strata/src/versioned/map.rs#L680-L701
[vs-versioned]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/strata/docs/versioned.md
[vs-views]: https://github.com/rust-util-collections/vsdb/blob/7082a888e8defc9a563653b14b0e0b16277f6496/core/src/common/engine/mod.rs#L63-L132

## MMDB, RocksDB, and Pebble

This comparison covers a single process using local files, as of 2026-11-25.
The references are MMDB 5.0.0 at `f1cb8f6`, RocksDB 11.13.2 (C++), and
Pebble 2.1.7 (Go). Each uses its own database format.
[MMDB revision](https://github.com/rust-util-collections/mmdb/commit/f1cb8f6d06fc039e53bdbee5f55a6971f8ff2c7b) · [RocksDB release](https://github.com/meta/rocksdb/releases/tag/v11.13.2) · [Pebble release](https://github.com/cockroachdb/pebble/releases/tag/v2.1.7)

For small keys and values, the clearest measured gap to RocksDB was **write
throughput with eight concurrent writers**. MMDB served warm, single-key SST
hits well on the same host. For a long-running store, L0 compaction under
sustained overwrites, batched disk reads, and compaction of large values are
three distinct performance questions. Online physical backups and
conflict-checked read-modify-write require support above MMDB.

### Measured point operations

Median throughput across five runs, in **thousands of operations per second**.
Each Put wrote one 128-byte value. Each Get was a single-reader hit after an
explicit flush and a complete warm-up pass. [Raw times and SST counts](../benchmarks/point-ops/runs.csv)

| Operation | MMDB 5.0.0 | RocksDB 11.13.2 | Pebble 2.1.7 |
|---|---:|---:|---:|
| Put, 1 writer, no per-write sync | 276 | 250 | 1,338 |
| Put, 8 writers, no per-write sync | 414 | 594 | 1,258 |
| Put, 1 writer, sync every write | 1.54 | 1.52 | 1.61 |
| Put, 8 writers, sync every write | 6.08 | 7.30 | 6.42 |
| Warm Get after fill by 1 writer | 1,286 | 1,197 | 802 |
| Warm Get after fill by 8 writers | 1,248 | 1,164 | 799 |

With no per-write sync, moving from one to eight writers increased MMDB
throughput **1.50×** and RocksDB throughput **2.38×**. At eight writers MMDB
was 30% below RocksDB on that setting and 17% below it with a durable
acknowledgement on every Put. **Pebble's no-sync acknowledgement can precede
the WAL write to the OS**, which matters both for timing and for survival of
a process crash. Its sync rows provide the comparable durability boundary.
[Pebble acknowledgement semantics](https://github.com/cockroachdb/pebble/blob/v2.1.7/options.go#L350-L367)

After the 131,072-key fill, MMDB and RocksDB each had one L0 SST, and
Pebble had nine at the post-flush observation. These warm reads include
the resulting layouts. Cold storage, sustained overwrites, and tail latency
remain unmeasured here. [Per-run layouts](../benchmarks/point-ops/runs.csv)

<details>
<summary>Workload, acknowledgement semantics, and reproduction</summary>

- One Ryzen 9 5900X, x86_64 Linux 7.0.0-44-generic, local NVMe with Btrfs
  (`compress=zstd:3`). Each run used a fresh process and database on the
  same filesystem. Five rounds ran all twelve engine/writer/durability
  combinations one at a time in shuffled order.
- One 8-byte big-endian key and one 128-byte value filled with `0x42` per
  Put. For `W` writers, worker `w` wrote `((i * W + w) * 65537) mod N`
  for `i = 0 .. N/W` (exclusive). This visits every key once with structured
  locality. The no-per-write-sync runs used 131,072 keys. The per-write-sync
  runs used 8,192 keys and each writer advanced in key order.
- Each engine had a 64 MiB write-buffer setting, 64 MiB block-cache setting,
  4 KiB data blocks, Bloom base setting of 12 bits/key, WAL enabled and SST
  compression disabled. RocksDB used its default HyperClockCache algorithm
  with the capacity set explicitly. MMDB adjusts Bloom density by level.
  Pebble grows its first memtables towards the configured size, so the same
  fill can produce a different L0 layout.
  [MMDB options](../../src/options.rs) · [RocksDB cache selection](https://github.com/meta/rocksdb/blob/v11.13.2/table/block_based/block_based_table_factory.cc#L496-L505) · [Pebble memtable sizing](https://github.com/cockroachdb/pebble/blob/v2.1.7/db.go#L398-L405)
- Writes were timed from worker release through completion of the foreground
  calls. The explicit flush and close were outside that interval. Without a
  per-write sync, MMDB `WalAsync` and RocksDB's default WAL flush hand bytes
  to the OS before acknowledgement. Pebble `NoSync` can still have bytes
  buffered in the process. The sync rows requested MMDB `WalSync`, RocksDB
  `WriteOptions::sync`, or Pebble `Sync` on every call. All three can share
  a sync among concurrent writers.
  [MMDB durability](../usage.md#write-and-choose-durability) · [RocksDB WAL options](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/options.h#L1584-L1587) · [Pebble write options](https://github.com/cockroachdb/pebble/blob/v2.1.7/options.go#L347-L372)
- After flush, one reader visited `(i * 8191) mod N` for a full warm-up and
  again for the timed pass in the 131,072-key runs. Each hit checked length
  and first byte and released the returned value immediately. The APIs were
  `get_pinned`, `Get(PinnableSlice*)`, and `Get` plus `Close`. Only the write
  phase used multiple clients.
- The standalone drivers take `DB_DIRECTORY WRITERS [sync]`, with a fresh
  directory per run. Build the Rust driver as a binary crate with a path
  dependency on the indicated MMDB revision and its locked dependencies
  (release profile, thin LTO, one codegen unit, rustc 1.100.0 in this run).
  Link the C++20 driver to the tagged RocksDB static library (Clang 21,
  upstream optimized build with `-march=native`).
  Build the Go driver with `github.com/cockroachdb/pebble/v2@v2.1.7`
  (Go 1.26.8). The CSV records elapsed seconds and the observed post-flush
  SST count at each level.
  [Rust driver](../benchmarks/point-ops/mmdb.rs) · [C++ driver](../benchmarks/point-ops/rocksdb.cc) · [Go driver](../benchmarks/point-ops/pebble.go) · [All runs](../benchmarks/point-ops/runs.csv)

</details>

### Where larger workloads can diverge

| Workload | What differs in the storage path | Measurement that would settle the impact |
|---|---|---|
| **Many writers** | MMDB's group leader shares WAL flush/sync across requests, then inserts their entries into the memtable while still holding `DB::inner`. Flush of a frozen memtable already runs in the background. Pebble serializes WAL order but allows concurrent memtable insertion and ordered publication. RocksDB supports concurrent memtable writes and separately offers `enable_pipelined_write` to overlap adjacent groups' WAL and memtable stages. That option defaults off in the run above. [MMDB write path](../../src/db.rs) · [Pebble commit](https://github.com/cockroachdb/pebble/blob/v2.1.7/commit.go) · [RocksDB write options](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/options.h#L1413-L1458) | Sweep writer count and entries per batch at fixed durability. Measure p99 acknowledgement latency through repeated WAL rotations and flushes. The point-operation totals include allocation, scheduling and WAL costs. |
| **Overwrites that build an L0 backlog** | MMDB's automatic L0→L1 picker takes all current L0 files and overlapping L1 files. That job has one subcompaction even if `max_subcompactions` is raised. New flushes can accumulate while it runs, and a point read can probe multiple overlapping L0 files. Pebble organizes L0 into sublevels and splits flushes to enable concurrent L0 compactions. RocksDB can split an eligible L0 compaction by key range. [MMDB picker and execution](../../src/compaction/leveled.rs) · [Pebble sublevels](https://github.com/cockroachdb/pebble/blob/v2.1.7/README.md#advantages) · [RocksDB subcompaction](https://github.com/meta/rocksdb/wiki/Subcompaction) | Overwrite a populated multilevel store across many flushes. Track admitted writes/s, L0 file count, stalls and read/write p99 while compaction competes with foreground I/O. |
| **Many independent keys per request** | MMDB exposes scalar `get` and `get_pinned`. A loop on one snapshot preserves a common read sequence, but repeats table and index probes per key. RocksDB's optimized `MultiGet` can group work by file/block and issue multiple reads together where the filesystem supports it. [MMDB point reads](../../src/db/read.rs) · [RocksDB MultiGet](https://github.com/meta/rocksdb/wiki/MultiGet-Performance) | Compare 1, 8 and 64 scattered keys per request at equal snapshot semantics. Measure end-to-end request p99 and device reads with both warm data and a working set larger than RAM. |
| **Frequent compaction of large live values** | MMDB stores values inline. A compaction that merges tables writes each surviving payload into a new SST. A trivial file move preserves the bytes in place. With `enable_blob_files`, RocksDB can put values above a configured threshold in blob files and compact their references. Pebble 2.1.7 also has an experimental value-separation policy for columnar blocks, disabled by default. Separation trades payload rewrites for an extra lookup on some reads and garbage collection of old blobs. [MMDB compaction](../../src/compaction/leveled.rs) · [RocksDB BlobDB](https://github.com/meta/rocksdb/wiki/BlobDB) · [Pebble options](https://github.com/cockroachdb/pebble/blob/v2.1.7/options.go#L745-L752) | Overwrite 1, 16 and 64 KiB values through several levels. Count WAL, SST and blob bytes written per logical byte, including blob GC, then compare point and scan latency. |
| **Many SSTs or DB handles under a memory budget** | MMDB partitions the point and range-deletion indexes. Each live reader retains directories and Bloom filters. Memtables, member-local pins and separately budgeted overflow cache entries also consume memory beyond a shared block-cache setting. RocksDB can load filter partitions on demand and coordinate memtable memory across DBs with `WriteBufferManager`. [MMDB cache accounting](../architecture.md#sst-v2-and-cache-memory) · [RocksDB partitioned filters](https://github.com/meta/rocksdb/wiki/Partitioned-Index-Filters) · [Write buffer manager](https://github.com/meta/rocksdb/wiki/Write-Buffer-Manager) | Fix the process memory budget and vary live SST and DB counts. Record RSS, reopen latency and p99 reads of a small hot range. |
| **Backward scans of recent writes** | MMDB supports lending steps in both directions. Its memtable cursor searches for the predecessor on a backward step. Pebble's memtable skiplist follows a backward link. This difference concerns rows still in memtables. [MMDB cursor](../../src/memtable/skiplist.rs) · [Pebble cursor](https://github.com/cockroachdb/pebble/blob/v2.1.7/internal/arenaskl/iterator.go#L220-L225) | Compare forward and backward scans over the same bounds, first before flush and then on SSTs, with equivalent value ownership. |

The repository's Criterion suite covers MMDB-only reads, scans, writes and
compression. Its 256 KiB cache cases still benefit from the OS page cache.
The mixed profile reports p99/p999 during buffer rotation with two readers
and two writers. Cross-engine steady-state and cold-device latency still
require separate runs. [Criterion workloads](../../benches/benchmarks.rs) · [Mixed profile](../../benches/v5_profile.rs)

### Features that change the caller's design

| Caller requirement | MMDB 5.0.0 | External reference |
|---|---|---|
| **Take a reopenable copy while writes continue** | A snapshot pins an MVCC sequence on the live handle. It can drive a logical export. A physical directory copy needs a stable file set, for example after a clean close. On Unix the usual read-only open takes a shared `LOCK` that conflicts with the writer. | RocksDB `Checkpoint::CreateCheckpoint` and Pebble `DB.Checkpoint` create openable directories from a live DB. Pebble's `WithFlushedWAL()` includes preceding unsynced writes. RocksDB also has incremental backup and restore via `BackupEngine`. [MMDB open and snapshots](../usage.md) · [RocksDB checkpoint](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/utilities/checkpoint.h) · [BackupEngine](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/utilities/backup_engine.h) · [Pebble checkpoint](https://github.com/cockroachdb/pebble/blob/v2.1.7/checkpoint.go) |
| **Read new commits from a second process** | Read-only open replays eligible WALs once and requires the directory to stay stable. MMDB exposes no API to follow a running writer. | RocksDB `OpenAsSecondary` can coexist with a primary. The caller advances its view with `TryCatchUpWithPrimary`. Reading retained WAL history by sequence is a separate API, `GetUpdatesSince`. [MMDB read-only mode](../usage.md#read-only-databases) · [RocksDB database interface](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/db.h) |
| **Commit only if a value read earlier still holds** | `WriteBatch` publishes multiple mutations atomically. `WriteBatchWithIndex` can overlay pending mutations on an iterator. Two callers that both read a counter at 7 can each successfully put 8. The caller must serialize that read-modify-write to retain both increments. | RocksDB provides pessimistic and optimistic transactions with `GetForUpdate` for tracked reads. Pebble also leaves a general transaction layer to its caller. [MMDB batches](../usage.md#write-and-choose-durability) · [RocksDB transactions](https://github.com/meta/rocksdb/wiki/Transactions) · [Pebble feature scope](https://github.com/cockroachdb/pebble/blob/v2.1.7/README.md) |
| **Load prebuilt tables into an occupied key range** | `DB::ingest_sorted` constructs bottom-level SSTs from strictly ascending pairs while serializing against writers. The entire first-to-last span must be disjoint from live data, including tombstone extents. Interleaved new keys can therefore require ordinary writes even when no key is a duplicate. | RocksDB `IngestExternalFile` can install `SstFileWriter` outputs over existing keys. Pebble `DB.Ingest` can do the same with mutually disjoint input files. Both may flush an overlapping memtable. [MMDB sorted ingestion](../architecture.md#sorted-ingestion) · [RocksDB ingestion options](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/options.h#L2990-L3050) · [Pebble Ingest](https://github.com/cockroachdb/pebble/blob/v2.1.7/ingest.go#L1051-L1102) |
| **Tune namespaces independently and update them atomically** | Prefixes share one keyspace and one LSM configuration. Separate DB directories allow independent tuning but require separate commits. | RocksDB column families have their own memtables, SSTs and options, share a WAL, and support one cross-family write batch. Pebble also uses one keyspace. [MMDB options](../../src/options.rs) · [RocksDB column families](https://github.com/meta/rocksdb/wiki/Column-Families) · [Pebble compatibility](https://github.com/cockroachdb/pebble/blob/v2.1.7/README.md#rocksdb-compatibility) |
| **Record an increment or append without reading the old value** | Batches store puts and point/range deletes. An application that needs the previous value must supply its own update logic and concurrency control. | RocksDB `DB::Merge` and Pebble `DB.Merge` record operands. A configured merge operator combines them for a key. [MMDB batch types](../../src/types.rs) · [RocksDB merge operator](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/merge_operator.h) · [Pebble batch operations](https://github.com/cockroachdb/pebble/blob/v2.1.7/batch.go) |
| **Attach versioned data to a key span** | `delete_range(begin, end)` hides covered point records under MVCC. Storing an application-defined value for a whole interval requires a separate encoding. | Pebble `RangeKeySet`, `RangeKeyUnset` and `RangeKeyDelete` manage span data with suffixes. Iterators can expose those spans alongside point keys. [MMDB ranges](../usage.md#read-pin-and-iterate) · [Pebble range-key API](https://github.com/cockroachdb/pebble/blob/v2.1.7/db.go#L192-L222) |

### Common operations

| Operation | MMDB | RocksDB | Pebble |
|---|---|---|---|
| Read a value | `DB::get` returns owned bytes. `get_pinned` can retain an SST block or memtable | `DB::Get` with a string or `PinnableSlice` | `DB.Get` returns bytes and a closer |
| Group mutations | `WriteBatch` and `DB::write` | `WriteBatch` and `DB::Write` | `Batch` and `DB.Apply` |
| Delete a key range | `DB::delete_range` covers `[begin, end)` | `DB::DeleteRange` | `DB.DeleteRange` |
| Read a stable view | `DB::snapshot` or `DB::arc_snapshot` | `DB::GetSnapshot` | `DB.NewSnapshot` |
| Position a cursor | `seek` (≥), `seek_for_prev` (≤) | `Seek` (≥), `SeekForPrev` (≤) | `SeekGE` (≥), `SeekLT` (<) |
| Detect an iterator failure | Lending steps return `Result`. Owned iteration exposes `error()` | `Iterator::status` | `Iterator.Error` |

MMDB also ships bounded and prefix scans, prefix Bloom pruning, block-property
filters, snapshot-aware range tombstones, per-level LZ4/Zstd compression,
compaction filters and parallel subcompaction below L0.
[MMDB storage architecture](../architecture.md) · [MMDB usage](../usage.md) · [RocksDB database](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/db.h) · [RocksDB iterator](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/iterator.h) · [Pebble database](https://github.com/cockroachdb/pebble/blob/v2.1.7/db.go) · [Pebble iterator](https://github.com/cockroachdb/pebble/blob/v2.1.7/iterator.go)

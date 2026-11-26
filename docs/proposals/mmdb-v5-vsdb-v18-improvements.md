# MMDB v5 and VSDB v18 — storage decisions and measured results

MMDB v5 implements bounded grouped reads, a native WAL fence, per-database and
shared-pool memory gauges, and sorted ingestion into a disjoint key range.
VSDB v18 uses these engine contracts for its shard adapter. Its ordinary
VecDex search uses scalar pinned decoding. [MMDB API](../usage.md) · [VSDB v18](https://github.com/rust-util-collections/vsdb/blob/master/docs/v18.md)

## Engine results

| Operation | Measured result and resulting behavior |
|---|---|
| 32 clustered point reads on one snapshot | On a warm single SST, grouped request p99 fell from 27.12 to 18.21 µs. The paired gain was 33.30% across five pairs, above 7.41% control variation. Block acquisitions fell from 64 to 3.07 per request. On verified storage reads, clustered p99 improved by 69.63%. The scattered 32-key gain of 10.10% was within 10.26% control variation. The API preserves input order, misses and duplicate slots, and returns a typed error on a failed read. [Grouped-read results](../benchmarks/grouped-reads/README.md) |
| Foreground puts during large batches on one DB | A cheaper group handoff raised small-put p99 by a paired median of 8.10% in the 100,000-put trace. An ordered replay of the warmed two-issuer trace projected only 0.167% median p99 improvement from overlapping WAL and apply. MMDB keeps its serial group-commit path. The projection models recorded events. [Writer and recovery results](../benchmarks/engine-profile/results.md) |
| L0 compaction under overwrites | Two parallel ranges reduced foreground stall time per admitted GiB by 97.94%, but the paired read p99 worsened by 5.08%, with 33.80% control variation. Cutting flush outputs on a prefix boundary doubled the file count and increased logical cold-row bytes emitted by 35% on the gap fixture. MMDB retains serial L0→L1 compaction and the ordinary flush layout. [L0 results](../benchmarks/l0-profile/README.md) · [Prefix-cut results](../benchmarks/l0-profile/prefix-cuts/README.md) |
| Memory across 16 handles sharing a pool | At a fixed per-shard allowance of 30 MiB, two immutables and a 10 MiB buffer raised saturation throughput relative to four and 6 MiB. In the longer hot-shard test, foreground p99 worsened in all five pairs, by a paired median of 5.60%. The engine retains an immutable limit of four. Pool charges, member pins, current memtables and process peak RSS have different ownership. [Engine memory measurements](../benchmarks/engine-profile/results.md) · [Gauge definitions](../architecture.md#sst-v2-and-cache-memory) |

A nonempty write with `no_slowdown` returns `Busy` under eligible L0 or
immutable-queue pressure, before consuming a sequence or appending a WAL
record. `DB::sync_wal()` makes earlier WAL-backed writes durable on the same
DB without adding a mutation. The standalone v4 exporter copies visible
rows, including residual-WAL updates and empty values, to a new v5 store.
A writable reopen checks the caller's level count and prefix length.
[Durability and errors](../usage.md) · [Format conversion](../migration.md)

At revision `90b220e`, all 25 engine validation commands passed on native
x86_64 Linux and in an AArch64 Linux full-system virtual machine. Each
target passed 585 default runtime cases and two doctests, plus 429
overlapping feature-enabled library tests. The arena pin test also passed
under Miri. The reported latency and throughput runs used native x86_64.
[Validation receipt](../benchmarks/engine-profile/validation.json)

## VSDB application results

One VSDB namespace owns a pool and one or more MMDB shards. VSDB v18
uses seven levels and eight-byte prefixes. DagMap publishes parent and
child records in a single checked, co-located batch. VerMap dependent
edits publish one changed working root. Upgrading an existing universe
requires a complete offline generation, including any externally placed
namespaces. [VSDB storage](https://github.com/rust-util-collections/vsdb/blob/master/docs/storage.md) · [VSDB migration](https://github.com/rust-util-collections/vsdb/blob/master/migration/README.md)

The stock v18/v4 versus v18/v5 comparisons measure complete caller
operations on x86_64 Linux:

- Creating a child in a default 16-shard DagMap reduced p99 by 33.38%. On
  the v5 adapter alone, atomic registration reduced engine writes per child
  from two to one. [Publication trials](https://github.com/rust-util-collections/vsdb/blob/master/docs/evaluations/v18-publication/README.md)
- Hot-shard write saturation rose 59.53%. With all 16 shards active, gross
  saturated byte throughput stayed within the control spread, but foreground
  small-put completions/s fell 31.63%. The selected 2 GiB namespace profile
  uses four immutable slots, a 16 MiB per-shard buffer and 256 MiB for
  each of the two evictable stores. The measured RSS applies to the stated
  workloads. [Adapter and resource trials](https://github.com/rust-util-collections/vsdb/blob/master/docs/evaluations/v18-adapter/README.md)
- The same warm 8,192-node VecDex graph produced identical ordered query
  results. Median run p99 was 184.57 µs on v4 and 253.38 µs on v5, a
  paired regression of 37.48%. Grouping vectors did not meet the full-query
  adoption criterion. On the final stock B+ tree fixture, hit and miss p99
  rose by 12.03% and 11.83%. [VecDex queries](https://github.com/rust-util-collections/vsdb/blob/master/docs/evaluations/v18-vecdex-v5/README.md) · [B+ tree measurements](https://github.com/rust-util-collections/vsdb/blob/master/docs/evaluations/v18-tree-v5/stock/README.md)

## Performance criteria

The MMDB candidate comparisons used matched work, durability and resource
budgets. Each primary comparison used at least five fresh-process pairs.
P99 was computed within each run. A qualifying paired median gain had to
exceed both the workload target and its control-to-control variation,
with four of five pairs in the same direction. The targets were 10% lower
p99 per clustered 32-key request, 15% lower foreground small-put
arrival-to-ack p99, or 20% less L0 stall time per admitted GiB. A 5%
guard applied to protected latency, throughput or SST write amplification
where relevant. The 16-shard profile used a 1 GiB measured engine RSS
ceiling, at least 95% of reference saturation throughput and at most 105%
of reference foreground p99. These criteria apply to their respective
fixtures. [Engine workloads](../benchmarks/engine-profile/README.md) · [Grouped-read workload](../benchmarks/grouped-reads/README.md)

## MMDB, RocksDB, and Pebble

The cross-engine comparison used MMDB v5 at `f1cb8f6`, RocksDB 11.13.2
and Pebble 2.1.7. Each cell is median throughput across five runs, in
**thousands of operations per second**. The warm read used one reader
after a complete flush and warm-up.

| Operation | MMDB v5 | RocksDB 11.13.2 | Pebble 2.1.7 |
|---|---:|---:|---:|
| Put, 1 writer, no per-write sync | 276 | 250 | 1,338 |
| Put, 8 writers, no per-write sync | 414 | 594 | 1,258 |
| Put, 1 writer, sync every write | 1.54 | 1.52 | 1.61 |
| Put, 8 writers, sync every write | 6.08 | 7.30 | 6.42 |
| Warm Get after fill by 1 writer | 1,286 | 1,197 | 802 |
| Warm Get after fill by 8 writers | 1,248 | 1,164 | 799 |

With eight writers, MMDB throughput was 30% below RocksDB for writes
without per-call sync and 17% below RocksDB with per-call sync. Single-reader
warm-hit throughput was higher on MMDB in both fill layouts. Without per-call
sync, MMDB and RocksDB handed WAL bytes to the OS before acknowledgement.
Pebble could acknowledge with bytes still buffered in its process. The
sync rows request durable acknowledgements on all three engines. After the
131,072-key fill, MMDB and RocksDB each had one L0 SST and Pebble had nine.
[All 60 runs](../benchmarks/point-ops/runs.csv) · [RocksDB WAL policy](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/options.h#L1584-L1587) · [Pebble write policy](https://github.com/cockroachdb/pebble/blob/v2.1.7/options.go#L347-L372)

The drivers ran one engine at a time in shuffled order on a Ryzen 9
5900X, Linux/x86_64 and local NVMe with Btrfs compression enabled.
Every put used an eight-byte key and a 128-byte repeated value. The
durable-write runs used 8,192 keys and the asynchronous runs 131,072.
All engines had 64 MiB write-buffer and block-cache settings, 4 KiB
blocks, a 12-bit/key base Bloom setting, enabled WAL and uncompressed
SST data blocks. Flush and close occurred after the timed writes. Warm
reads released each result immediately. To reproduce, build the drivers
against the versions above and run each as `DRIVER FRESH_DB_DIR WRITERS [sync]`
with one or eight writers. The measured builds used rustc 1.100.0 with
thin LTO and one codegen unit, Clang 21 with the optimized native
RocksDB library, and Go 1.26.8. [Rust driver](../benchmarks/point-ops/mmdb.rs) · [C++ driver](../benchmarks/point-ops/rocksdb.cc) · [Go driver](../benchmarks/point-ops/pebble.go)

### Storage and API differences

| Requirement | MMDB v5 | External implementation |
|---|---|---|
| Concurrent writes and L0 backlog | One group leader orders WAL writes and memtable application. The L0→L1 job claims overlapping input and executes serially. Deeper compactions can split by key range. | RocksDB supports concurrent memtable writes and optional WAL/write-group pipelining, and can split eligible L0 compactions. Pebble orders WAL commits and uses L0 sublevels. [RocksDB write options](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/options.h#L1413-L1458) · [Subcompactions](https://github.com/meta/rocksdb/wiki/Subcompaction) · [Pebble commit pipeline](https://github.com/cockroachdb/pebble/blob/v2.1.7/commit.go) · [L0 sublevels](https://github.com/cockroachdb/pebble/blob/v2.1.7/README.md#advantages) |
| Independent point reads | Up to 32 keys per request can reuse SST index and data blocks on one snapshot. Storage misses use synchronous reads. | RocksDB's optimized MultiGet groups by file/block and can issue parallel I/O on supported filesystems. [MultiGet](https://github.com/meta/rocksdb/wiki/MultiGet-Performance) |
| Large values and memory management | Values live inline in SSTs. A pool can share evictable blocks across DBs. Each DB retains its own write buffers, and live readers retain Bloom filters. | RocksDB can place large values in blob files, partition filters, and coordinate write-buffer budgets across DBs. Pebble 2.1.7 offers experimental value separation. [BlobDB](https://github.com/meta/rocksdb/wiki/BlobDB) · [Partitioned filters](https://github.com/meta/rocksdb/wiki/Partitioned-Index-Filters) · [WriteBufferManager](https://github.com/meta/rocksdb/wiki/Write-Buffer-Manager) · [Pebble value separation](https://github.com/cockroachdb/pebble/blob/v2.1.7/options.go#L745-L752) |
| Live backups and second-process readers | A snapshot holds an MVCC view on the open DB. Physical copying requires a stable file set. Read-only open replays eligible WALs once and the directory must remain stable. | RocksDB and Pebble expose live directory checkpoints. RocksDB also offers incremental backups, secondary catch-up and retained WAL updates. [RocksDB checkpoint](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/utilities/checkpoint.h) · [BackupEngine](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/utilities/backup_engine.h) · [RocksDB DB interface](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/db.h) · [Pebble checkpoint](https://github.com/cockroachdb/pebble/blob/v2.1.7/checkpoint.go) |
| Conflict-checked updates and independent keyspaces | A batch atomically publishes mutations in one DB. Callers serialize read-modify-write conflicts. Prefixes share that DB's LSM configuration. | RocksDB provides pessimistic and optimistic transactions, merge operators and column families that can share an atomic write. Pebble supports merge operators and application-defined range keys. [Transactions](https://github.com/meta/rocksdb/wiki/Transactions) · [Column families](https://github.com/meta/rocksdb/wiki/Column-Families) · [RocksDB merger](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/merge_operator.h) · [Pebble batch API](https://github.com/cockroachdb/pebble/blob/v2.1.7/batch.go) |
| Ingesting into an occupied key range | `DB::ingest_sorted` builds bottom-level SSTs. Its full key span, including tombstone extents, must be disjoint from live data. | RocksDB and Pebble can ingest compatible prebuilt SSTs over existing data, subject to input-file and snapshot rules. [MMDB sorted ingest](../architecture.md#sorted-ingestion) · [RocksDB ingestion](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/options.h#L2990-L3050) · [Pebble ingestion](https://github.com/cockroachdb/pebble/blob/v2.1.7/ingest.go) |

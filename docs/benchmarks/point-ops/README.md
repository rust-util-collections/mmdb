# MMDB, RocksDB, and Pebble

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
[All 60 runs](runs.csv) · [RocksDB WAL policy](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/options.h#L1584-L1587) · [Pebble write policy](https://github.com/cockroachdb/pebble/blob/v2.1.7/options.go#L347-L372)

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
RocksDB library, and Go 1.26.8. [Rust driver](mmdb.rs) · [C++ driver](rocksdb.cc) · [Go driver](pebble.go)

## Storage and API differences

| Requirement | MMDB v5 | External implementation |
|---|---|---|
| Concurrent writes and L0 backlog | One group leader orders WAL writes and memtable application. The L0→L1 job claims overlapping input and executes serially. Deeper compactions can split by key range. | RocksDB supports concurrent memtable writes and optional WAL/write-group pipelining, and can split eligible L0 compactions. Pebble orders WAL commits and uses L0 sublevels. [RocksDB write options](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/options.h#L1413-L1458) · [Subcompactions](https://github.com/meta/rocksdb/wiki/Subcompaction) · [Pebble commit pipeline](https://github.com/cockroachdb/pebble/blob/v2.1.7/commit.go) · [L0 sublevels](https://github.com/cockroachdb/pebble/blob/v2.1.7/README.md#advantages) |
| Independent point reads | The timed driver called scalar `get_pinned`. Later v5 builds (`c97bb5c`) added grouped pinned reads for up to 32 slots at one sequence. [Grouped-read trials](../grouped-reads/README.md) | RocksDB's optimized MultiGet groups by file/block and can issue parallel I/O on supported filesystems. [MultiGet](https://github.com/meta/rocksdb/wiki/MultiGet-Performance) |
| Large values and memory management | Values live inline in SSTs. A pool can share evictable blocks across DBs. Each DB retains its own write buffers, and live readers retain Bloom filters. | RocksDB can place large values in blob files, partition filters, and coordinate write-buffer budgets across DBs. Pebble 2.1.7 offers experimental value separation. [BlobDB](https://github.com/meta/rocksdb/wiki/BlobDB) · [Partitioned filters](https://github.com/meta/rocksdb/wiki/Partitioned-Index-Filters) · [WriteBufferManager](https://github.com/meta/rocksdb/wiki/Write-Buffer-Manager) · [Pebble value separation](https://github.com/cockroachdb/pebble/blob/v2.1.7/options.go#L745-L752) |
| Live backups and second-process readers | A snapshot holds an MVCC view on the open DB. Physical copying requires a stable file set. Read-only open replays eligible WALs once and the directory must remain stable. | RocksDB and Pebble expose live directory checkpoints. RocksDB also offers incremental backups, secondary catch-up and retained WAL updates. [RocksDB checkpoint](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/utilities/checkpoint.h) · [BackupEngine](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/utilities/backup_engine.h) · [RocksDB DB interface](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/db.h) · [Pebble checkpoint](https://github.com/cockroachdb/pebble/blob/v2.1.7/checkpoint.go) |
| Conflict-checked updates and independent keyspaces | A batch atomically publishes mutations in one DB. Callers serialize read-modify-write conflicts. Prefixes share that DB's LSM configuration. | RocksDB provides pessimistic and optimistic transactions, merge operators and column families that can share an atomic write. Pebble supports merge operators and application-defined range keys. [Transactions](https://github.com/meta/rocksdb/wiki/Transactions) · [Column families](https://github.com/meta/rocksdb/wiki/Column-Families) · [RocksDB merger](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/merge_operator.h) · [Pebble batch API](https://github.com/cockroachdb/pebble/blob/v2.1.7/batch.go) |
| Ingesting into an occupied key range | `DB::ingest_sorted` builds bottom-level SSTs. Its full span must be disjoint from existing file ranges and tombstone extents. [MMDB sorted ingest](../../architecture/writes.md#sorted-ingestion) | RocksDB and Pebble can ingest compatible prebuilt SSTs over existing data, subject to input-file and snapshot rules. · [RocksDB ingestion](https://github.com/meta/rocksdb/blob/v11.13.2/include/rocksdb/options.h#L2990-L3050) · [Pebble ingestion](https://github.com/cockroachdb/pebble/blob/v2.1.7/ingest.go) |

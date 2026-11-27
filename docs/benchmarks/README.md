# MMDB benchmarks and VSDB v18 results

The published measurements below were collected on MMDB v5 revisions.
Each report identifies its fixture, binary or source revision, controls, and
raw observations. To measure the code in this checkout, use the commands
under [Run a benchmark](#run-a-benchmark). For the current API guarantees,
see the [usage guide](../usage.md).

## Four-host tuning study

The study ran on Apple M5/macOS and three physical Linux hosts across APFS,
ext4, ZFS, and single- and dual-device Btrfs. The 5.0.1 follow-up compared
binaries directly and counted compaction through a common endpoint.

| Question | Evidence |
|---|---|
| How did grouped reads, shard buffers, SST compression, and durable WAL writes vary by host and volume? | [5.0.1 findings](four-host/README.md#follow-up-on-mmdb-501) · [Earlier v5 results](four-host/README.md#historical-campaign-at-828e47a) |
| How much memory did a larger decoded block cache consume when the SST files were already in the OS page cache? | [Measured latency and RSS](four-host/README.md#paying-for-an-application-resident-working-set) · [Capacity driver and controls](cache-capacity/README.md) |
| How do I verify the archived results or repeat the comparisons? | [Reproduction instructions](four-host/reproduce.md) · [5.0.1 retest protocol](four-host/retest.md) · [Original protocol](four-host/protocol.md) |

## Engine workloads

| Question on the x86_64 Linux fixture | Report |
|---|---|
| When can grouped keys reuse index and data blocks? | [Grouped reads, storage-read coverage, and pin lifetimes](grouped-reads/README.md) |
| How do concurrent bulk writes affect a foreground put? | [Shared and separate DBs](engine-profile/README.md#small-foreground-writes-during-bulk-updates) · [Writer handoff](engine-profile/README.md#writer-handoff-and-overlap) · [Writer-stage replay](engine-profile/writer-trace/README.md) |
| What changes with a fixed memory allowance or a residual WAL? | [16-shard memory trial](engine-profile/README.md#sixteen-shards-under-a-1-gib-process-budget) · [Writable and read-only reopen](engine-profile/README.md#reopen-from-a-known-residual-wal) |
| How do WAL fences, L0 parallelism, and prefix cuts affect stalls and rewrites? | [L0 trials and controls](l0-profile/README.md) |
| How did scalar reads and writes compare across MMDB, RocksDB, and Pebble? | [Point operations and WAL policies](point-ops/README.md) |

The validation receipt for revision `90b220e` identifies the tests run on
native x86_64 Linux and in an AArch64 Linux full-system VM. [Validation receipt](engine-profile/validation.json)

### Physical ARM64 on macOS

The Apple M5 reports cover native APFS runs of the grouped-read, mixed-writer,
shard-memory, recovery, and compaction workloads. Use each report's within-host
controls when comparing methods. [Grouped reads and native tests](apple-m5/README.md) ·
[Engine measurements and replay](apple-m5/engine.md)

## VSDB application results

These trials measured complete VSDB v18 operations on MMDB v5 on x86_64
Linux. A namespace owned a pool and one or more MMDB shards with seven
levels and eight-byte prefixes. VecDex used scalar pinned decoding, DagMap
published parent and child records in a single co-located batch, and VerMap
dependent edits published one changed working root.
[VSDB v18](https://github.com/rust-util-collections/vsdb/blob/master/docs/v18.md) · [VSDB storage](https://github.com/rust-util-collections/vsdb/blob/master/docs/storage.md)

- Atomic registration reduced engine writes per DagMap child from two to
  one. [Publication trials](https://github.com/rust-util-collections/vsdb/blob/master/docs/evaluations/v18-publication/README.md)
- The selected 2 GiB namespace profile used four immutable slots, a 16 MiB
  per-shard buffer, and 256 MiB for each of the two evictable stores. The
  measured RSS applies to those workloads. [Adapter and resource trials](https://github.com/rust-util-collections/vsdb/blob/master/docs/evaluations/v18-adapter/README.md)
- On a warm 8,192-node VecDex graph, median query p99 was 253.38 µs.
  Grouping vectors missed the adoption criterion for a complete query.
  [VecDex measurements](https://github.com/rust-util-collections/vsdb/blob/master/docs/evaluations/v18-vecdex-v5/README.md) ·
  [B+ tree measurements](https://github.com/rust-util-collections/vsdb/blob/master/docs/evaluations/v18-tree-v5/stock/README.md)

## Performance criteria

Each study defines its workload, resource budget, and acceptance thresholds.
The paired studies compute p99 within a single run, compare control and
candidate runs with matched work (A/B), and measure variation using two
independent control runs (A/A). The four-host protocol also defines what
qualifies as a physical storage read. [Four-host decision rules](four-host/protocol.md#analysis-rules-fixed-before-timing) ·
[Engine settings and controls](engine-profile/README.md#workloads) ·
[Grouped-read evidence](grouped-reads/README.md#verified-storage-reads-and-pins)

## Run a benchmark

Run these commands from the repository root with stable Rust.

```sh
cargo +stable bench --bench benchmarks -- warm
cargo +stable bench --bench read_write_profile
cargo +stable bench --bench engine_profile -- mixed
```

The first command selects Criterion's warm workloads. Its small-block-cache
workloads can also hit the OS page cache. The concurrent reader/writer
profile defaults to two immutable memtables and accepts
`MMDB_PROFILE_IMMUTABLES` to change the limit. The engine profile records
stage timings in a separate `bench-metrics` build. Use the report's pinned
revision and settings to reproduce a historical result.
[Criterion driver](../../benches/benchmarks.rs) · [Concurrent reader/writer profile](../../benches/read_write_profile.rs) ·
[Engine profile settings](engine-profile/README.md#workloads)

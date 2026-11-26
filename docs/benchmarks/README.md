# MMDB benchmarks and VSDB v18 results

## Engine workloads

| Workload | Result on the x86_64 fixture |
|---|---|
| [Grouped point reads](grouped-reads/README.md) | Grouping 32 clustered keys reduced request p99 on a warm SST and on verified storage reads. The scattered-key storage result fell within control variation. |
| [Foreground puts during bulk writes](engine-profile/README.md#small-foreground-writes-during-bulk-updates) | Moving bulk writes to a second DB improved small-put p99 beyond control variation in the repeated-byte asynchronous trial. |
| [Writer handoff](engine-profile/README.md#writer-handoff-and-overlap) | The shorter handoff missed its latency target. Replaying the warmed two-issuer trace also showed little opportunity to overlap WAL and memtable work. |
| [Memory across 16 shards](engine-profile/README.md#sixteen-shards-under-a-1-gib-process-budget) | Two immutable slots improved saturation throughput at equal buffer allowance, but exceeded the hot-shard p99 budget. The reference keeps four slots. |
| [Reopen with a residual WAL](engine-profile/README.md#reopen-from-a-known-residual-wal) | Writable and read-only opens recovered every row. Read-only open retained the replayed rows in memory. |
| [L0 compaction and WAL fences](l0-profile/README.md) | Two-range compaction cut foreground stalls, but the experiment could not establish whether read p99 stayed within budget. An extra prefix cut increased cold-row rewrites. MMDB retains serial L0 compaction and the ordinary flush layout. |
| [MMDB, RocksDB, and Pebble](point-ops/README.md) | MMDB had lower eight-writer put throughput than RocksDB and higher single-reader warm-hit throughput in both fill layouts. The report gives WAL acknowledgement policies for each engine. |

At revision `90b220e`, all 25 engine validation commands passed on native
x86_64 Linux and in an AArch64 Linux full-system virtual machine. Each
target passed 585 default runtime cases and two doctests, plus 429
overlapping feature-enabled library tests. The arena pin test also passed
under Miri.
[Validation receipt](engine-profile/validation.json)

### Physical ARM64 on macOS

On Apple M5, 32 clustered keys had 25.30% lower request p99 with grouped
reads on a warm SST and 66.64% lower p99 with verified device reads. The
scattered device gain was 5.48%, above this host's 2.01% control spread.
At an equal 30 MiB write-buffer allowance per shard, two immutable slots
and a 10 MiB buffer improved hot-shard foreground p99 by 10.89% and
saturation throughput by 35.06% relative to four slots and a 6 MiB buffer.
Two-range L0 compaction exceeded the read p99 regression ceiling on this
host. Debug and release each passed 577 runtime cases and two doctests.
[M5 measurements, controls and native test receipt](apple-m5/README.md)

## VSDB application results

One VSDB namespace owns a pool and one or more MMDB shards. VSDB v18
uses seven levels and eight-byte prefixes. Its ordinary VecDex search uses
scalar pinned decoding. DagMap publishes parent and
child records in a single checked, co-located batch. VerMap dependent
edits publish one changed working root. Upgrading an existing universe
requires a complete offline generation, including any externally placed
namespaces. [VSDB v18](https://github.com/rust-util-collections/vsdb/blob/master/docs/v18.md) · [VSDB storage](https://github.com/rust-util-collections/vsdb/blob/master/docs/storage.md) · [VSDB migration](https://github.com/rust-util-collections/vsdb/blob/master/migration/README.md)

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
fixtures. [Engine workloads](engine-profile/README.md) · [Grouped-read workload](grouped-reads/README.md)

## Run a benchmark

From the repository root, run the Criterion warm-cache suite, the concurrent
reader/writer profile, or the engine mixed workload with stable Rust.

```sh
cargo +stable bench --bench benchmarks -- warm
cargo +stable bench --bench v5_profile
cargo +stable bench --bench engine_profile -- mixed
```

Both the warm-cache and small-block-cache Criterion workloads can read from
the OS page cache. The v5 profile defaults to two immutable memtables. Use
`MMDB_PROFILE_IMMUTABLES` to compare queue limits across runs. The engine
profile records diagnostic stage timings in a separate `bench-metrics` build.
[Criterion driver](../../benches/benchmarks.rs) · [v5 profile](../../benches/v5_profile.rs) · [Engine profile settings](engine-profile/README.md#workloads)

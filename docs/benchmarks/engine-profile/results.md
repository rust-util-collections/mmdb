# MMDB engine profile results

These experiments exercise the engine directly on a Ryzen 9 5900X, x86_64 Linux, with the processes restricted to four physical cores (4–7). Rust 1.100.0 built the bench in the optimized profile. Each child owns fresh DB directories on the local NVMe filesystem and uses uncompressed SST data blocks with a 4 KiB target. The test directory inherited Btrfs `compression=none` within a mount using `compress=zstd:3`. The mixed and memory arms preload 32,768 rows in each DB and warm the scalar read set before timing. Each raw packet records binary fingerprints, trace seeds and configuration. The paired runs additionally record completed operations and the timing boundary. [Workload definitions](README.md) · [Decision thresholds](../../proposals/mmdb-v5-execution-plan.md#4-measure-the-limiting-engine-operation)

To regenerate the full paired table for a raw suite, run
`python3 docs/benchmarks/engine-profile/summarize.py RAW.jsonl.gz`.
The summary validates complete arms, rates and source counts before computing
within-pair deltas. Timing columns show the median for each binary, while
the gain is the median of the ratios inside individual pairs. Positive gains favor the compared arm. Its A/A band is
the largest absolute change between matched control runs. Run the synthetic evidence regressions with
`python3 -B -m unittest discover -s docs/benchmarks/engine-profile -p test_summarize.py`.

## Sixteen shards under a 1 GiB process budget

Each run opened 16 DBs with a shared block cache. The pool allowed up to 256 MiB in its segmented store and 256 MiB in its overflow store. Each DB finished a blocking preload flush before timing, leaving zero pending immutable generations. One foreground thread wrote 128-byte values and another read the warm keys on shard zero. Sixteen other writers issued 32-entry batches with 256-byte values, either to shard zero or to one shard each. The paced trace issued 10,000 requests per stream. The separate saturation trace counted all writes completed by the same workers in an eight-second window, including requests in progress at its deadline.

The fixed-buffer series compared three immutable limits at 16 MiB per write buffer. The equal-budget series instead held the nominal write-buffer allowance at 30 MiB per shard, calculated as `(1 + immutable limit) × buffer size`. The hot/all-active control offers were 489/1,534 requests per second per stream for the 16 MiB series, and 251/1,381 for the equal-budget series. A placement's p99 ratio uses its own control at the same offer.

| Sweep | Immutable limit / buffer | One hot shard p99, ms | Hot saturation, MiB/s | Hot peak RSS, MiB | All active p99, ms | All saturation, MiB/s | All peak RSS, MiB |
|---|---|---:|---:|---:|---:|---:|---:|
| Fixed buffer | 4 / 16 MiB (control) | 1.981 | 123.2 | 202.9 | 15.548 | 303.2 | 936.4 |
| Fixed buffer | 1 / 16 MiB | 64.703 | 102.6 | 210.3 | 90.530 | 293.7 | 362.0 |
| Fixed buffer | 2 / 16 MiB | 1.921 | 118.7 | 183.7 | 12.679 | 289.2 | 625.5 |
| Equal allowance | 4 / 6 MiB (control) | 2.090 | 58.8 | 155.4 | 18.829 | 234.7 | 528.3 |
| Equal allowance | 1 / 15 MiB | 52.920 | 99.5 | 208.8 | 60.330 | 289.9 | 342.8 |
| Equal allowance | 2 / 10 MiB | 2.156 | 90.5 | 175.7 | 12.109 | 258.4 | 455.2 |

The p99 and saturation columns are per-arm medians. Peak RSS is the largest high-water mark over the two load modes, including both A/A controls for reference geometries. Every geometry stayed below 1 GiB. Every paired paced arm completed the offer and passed the backlog screen. The fixed-buffer sweep favors retaining depth four. Depth one incurred a multi-fold p99 penalty in both placements. Depth two lost 5.69% of paired all-active saturation throughput, exceeding the 5% guard with a 2.64% A/A band.

In the equal-allowance sweep, depth two gained 53.19% hot and 9.33% all-active saturation throughput, with every pair favorable and A/A bands of 16.58% and 1.23%. The paired foreground p99 changed by +3.15% in duration on the hot trace and −28.28% on the all-active trace. Their 12.21% and 36.07% A/A bands could not establish a 5% tail guard. Depth one lost the tail comparison on both placements. [Per-geometry guards, RSS and component measurements](memory-summary.csv) · [Raw initial memory matrix](final-memory.jsonl.gz)

The initial LSM state had 16 L0 files across the 16 DBs and zero pending immutables. Each entry below gives the median end-state L0 files / pending immutables after the scheduled work, plus the median measured compaction output per admitted logical byte. The byte counter measures newly written compaction output and excludes the initial flush files.

| Equal-allowance load | Depth 4 / 6 MiB at end | Depth 2 / 10 MiB at end | Compaction output / logical byte, depth 4 → 2 |
|---|---:|---:|---:|
| Hot paced | 21 / 1 | 21 / 0 | 6.020 → 3.626 |
| All active paced | 60 / 0 | 32 / 0 | 0.475 → 0.317 |
| Hot saturation | 15 / 0 | 24 / 0 | 3.516 → 2.220 |
| All active saturation | 76 / 40 | 53 / 8 | 0.435 → 0.292 |

The hot saturation candidate left nine more L0 files across the database set than its control. Its throughput figure includes that difference in work remaining when the window closed. In all-active saturation, the candidate admitted more bytes and left fewer pending flushes and L0 files. Under a fixed offered count, it completed fewer flushes, 149 versus 249 on one hot shard and 144 versus 240 across all shards. Its larger buffer reduces flush turnover. The two placements consequently give different tradeoffs for VSDB.

The separate instrumented hot-shard runs explain the depth-one penalty. At 16 MiB, writers accumulated 69.54 seconds waiting for immutable capacity at depth one and zero at depths two and four. With the equal allowance, depth one at 15 MiB accumulated 59.71 seconds of the same wait, again with zero at depths two and four. Cumulative waiting across concurrent writers can exceed elapsed time. The fixed-buffer all-active sample reached about 272 MiB across 16 immutable generations, versus about 17 MiB and one generation with a single hot shard. These component maxima came from periodic 50 ms observations on the instrumented binary. Whole-process RSS in the primary table is measured independently on the ordinary binary.

### Focused p99 repeat and caller decision

**Retain the current immutable limit of four across placements.** Only depth two with a 10 MiB buffer remained a plausible alternative within the 30 MiB allowance after the first sweep. The focused repeat doubled the paced trace to 20,000 requests per stream, using the same rates, binary and placements. Each placement ran five fresh A/A pairs and five independent A/B pairs. All 40 processes completed all offered work and all pairs passed the backlog test.

| Placement | Depth 4 / 6 MiB median p99 | Depth 2 / 10 MiB median p99 | Paired gain and range | Largest A/A difference | Pairs favoring depth 2 |
|---|---:|---:|---|---:|---:|
| One hot shard | 2.017 ms | 2.143 ms | −5.60% (−14.73% to −0.02%) | 11.11% | 0/5 |
| All active | 17.327 ms | 13.223 ms | +35.17% (−73.35% to +49.92%) | 73.98% | 3/5 |

Depth two missed the 5% hot p99 ceiling on the paired median. Both A/A bands exceeded 5%. In the longer hot runs, the median endpoint was 19 L0 files and one immutable at depth four versus 18 and zero at depth two. With all shards active the corresponding endpoints were 52 and nine versus 49 and zero. All runs started at 16 L0 files and zero immutables. The depth-two throughput and saturation RSS results make this geometry worth considering for VSDB deployments dominated by simultaneous writes across shards. Its actual p99 under a caller's mix remains a deployment input. The experiment supplies no common lower-latency profile to replace the existing depth. [Per-seed repeat and debt](memory-tail-extension-summary.csv) · [Raw focused repeat](memory-tail-extension.jsonl.gz) · [Reproduction command](README.md)

## Small foreground writes during bulk updates

One producer issued 10,000 puts with 128-byte values. A second producer issued a 4,096-entry batch per 128 foreground arrivals. The split arm routed only the bulk producer to another local DB. Both placements had the same calibrated offer and identical per-round keys. All four workloads completed all offered operations and passed the backlog screens in all matched pairs.

| Payload and durability | Offered small puts/s | Same-shard p99 | Split p99 | Paired gain | Largest A/A difference | Pairs favoring split |
|---|---:|---:|---:|---:|---:|---:|
| Repeated bytes, `WalAsync` | 16,765 | 4.346 ms | 1.525 ms | +64.92% | 34.96% | 5/5 |
| High entropy, `WalAsync` | 18,911 | 4.068 ms | 2.429 ms | +48.89% | 450.34% | 5/5 |
| Repeated bytes, `WalSync` | 923 | 5.263 ms | 2.621 ms | +51.39% | 1,419.43% | 5/5 |
| High entropy, `WalSync` | 1,006 | 4.857 ms | 2.738 ms | +44.87% | 287.29% | 4/5 |

Only the repeated-byte async median exceeded its own small-put A/A band. The high-entropy async result had the expected direction in every pair, but baseline repeats make its effect size uncertain. The contrast between two read operations is also visible. On the high-entropy synchronous workload, fresh-snapshot read p99 was 9.912 ms with one DB and 7.687 ms with split placement. A read reusing a held snapshot measured 91.2 µs and 81.2 µs, respectively. Capture and reuse have materially different end-to-end costs under concurrent writes. Each bulk class had 79 requests per run, so its table reports throughput without using its p99 for a decision. [Paired summary, including gain ranges](mixed-summary.csv) · [Raw mixed runs](final-mixed.jsonl.gz)

## Where the writer spent time

In the high-entropy asynchronous trial, the two writers on one shard accumulated 199.5 ms of leader-queue waiting. Moving the bulk producer to a second shard eliminated that particular wait. Total WAL work and memtable insertion stayed close in the two placements. With sync on each write, WAL work dominated both placements.

| High-entropy trace | Groups | WAL append + flush/sync, ms | Memtable apply, ms | Leader-queue wait across threads, ms |
|---|---:|---:|---:|---:|
| WalAsync, same shard | 10,076 | 120.3 | 120.4 | 199.5 |
| WalAsync, separate shards | 10,079 | 123.6 | 123.6 | 0.0 |
| WalSync, same shard | 10,076 | 6,141.9 | 135.6 | 314.7 |
| WalSync, separate shards | 10,079 | 6,977.1 | 138.3 | 0.0 |

Each cell above is one diagnostic run at the same recorded offer and first-round seed as its uninstrumented counterpart. Each ran 10,000 small puts and 79 batches of 4,096 entries. Stage times belong to the executing thread. A leader can apply other callers' requests while those callers wait. Wall time, wait time and leader-stage totals overlap. In the high-entropy synchronized same-shard case, the instrumented small-put p99 was about four times the median of its two uninstrumented controls. Use the paired uninstrumented trials for latency decisions. See the traced ordering of individual groups for the limits on overlapping the two stages. [Stage observations](stage-summary.csv) · [Ordered writer replay](writer-trace/README.md)

To reproduce the additional split-placement measurements, run the metrics binary with `MMDB_PROFILE_OPS=10000`, `MMDB_PROFILE_SEED=20261126`, `MMDB_PROFILE_PLACEMENT=split` and the corresponding `SYNC`, `ENTROPY` and `RATE` settings from the stage CSV. Use the eight-byte-prefix `mixed` case, identical binary and `taskset -c 4-7` as in the primary run. The supplemental packet contains all four command outputs. [Supplemental raw runs](split-stage-samples.jsonl.gz)

## Would cheaper writer handoff improve small-write p99?

**Retain the current group storage and wakeup behavior.** The alternative inlined short request/assignment lists and skipped a wakeup when the leader served only itself with no pending writer or maintenance barrier. A targeted repeat increased each primary trace to 100,000 small puts and 782 bulk batches. It used the same 18,911 offered scalar puts/s on one DB, high-entropy values and `WalAsync`, with new seeds and independent A/A and A/B pairs.

| Foreground requests per run | Control median p99 | Candidate median p99 | Median paired gain | Paired gain range | Largest A/A difference | Favorable pairs |
|---:|---:|---:|---:|---:|---:|---:|
| 10,000 | 4.533 ms | 4.237 ms | +21.36% | −229.58% to +85.30% | 84.60% | 4/5 |
| 100,000 | 7.177 ms | 9.194 ms | −8.10% | −62.16% to +75.71% | 210.10% | 2/5 |

The short trace reached the nominal 15% threshold but its median gain was smaller than the control's A/A noise band. The extension reversed the median direction. All offered work completed and all five extended paired backlog checks passed. The primary failed both the 15% improvement and consistency requirements, so no further singleton extension was needed. This spread limits the result to an adoption decision on this fixture. The separate ordered-stage replay evaluates a two-stage pipeline and retains the serial writer for this workload. [Extended pairs](handoff-extension-summary.csv) · [Raw extension](handoff-primary-extension.jsonl.gz) · [Pipeline opportunity](writer-trace/README.md)

The full guard matrix covered 150 fresh processes. In the async same-DB mix, bulk completions stayed at 148.83 versus 148.91 batches/s and compaction output at 0.164 bytes per admitted logical byte for each binary. Peak RSS remained below 59 MiB across the matrix. Paced warm singleton read p99 met the 5% screen in both rate configurations, and paced async singleton write p99 met it at 62.21 versus 62.27 µs. Closed-loop singleton p99 had 10–39% A/A variation. The synchronous mixed foreground p99 went from 22.19 to 61.99 ms, with a paired median 179% increase and only two of five pairs favoring the candidate. The synchronous paced singleton write had 421% A/A variation. These controls provide no basis to adopt the alternative for sync workloads. [Guard measurements](handoff-summary.csv) · [Raw handoff matrix](final-handoff.jsonl.gz)

Reproduce the targeted extension with the same control and handoff candidate executables used in the ordinary matrix:

```sh
python3 docs/benchmarks/engine-profile/extend_handoff.py ./control ./candidate extension.jsonl.gz \
    --cpus 4-7 --tmpdir /path/to/uncompressed/scratch --rate 18911
```

## Reopen from a known residual WAL

A child wrote and synced the fixture, then exited without closing the DB. Each fixture used a 64 MiB preparation buffer, so every geometry reopened the same amount of WAL for the same seed and row count. The parent timed only the open call, sampled current and peak process memory immediately after it, then checked every value. All 70 processes passed. The CSV retains all 14 cells separately, with five processes in each.

| Fixture | WAL bytes | Writable median range across six geometries | Read-only median at depth 4 / 6 MiB | Maximum observed writable peak at open |
|---|---:|---:|---:|---:|
| 10,000 rows | 1,720,364 | 13.007–13.622 ms | 6.399 ms | 8.2 MiB |
| 100,000 rows | 17,203,612 | 110.800–112.788 ms | 61.747 ms | 23.3 MiB |

At 100,000 rows, the 6 MiB writable buffer's maximum recorded peak at open was 14.0 MiB versus 23.2 MiB at 16 MiB and the same immutable limit of four. Read-only open retains replayed entries in memory and uses a different recovery path, with a 23.2 MiB maximum peak for the same 100,000 rows. Linux current and high-water memory are successive samples. A few KiB of allocation can occur between them, and the later verification scan can raise the high-water mark further. [All reopen geometries](reopen-summary.csv) · [Raw reopen runs](final-reopen.jsonl.gz) · [Reopen driver](../../../benches/engine_profile/reopen.rs)

# Engine profile

The optimized `engine_profile` benchmark measures foreground puts, bulk
writes, pinned reads, fresh and held snapshots, memory usage, and recovery
of a known residual WAL. Each arm runs in a fresh process and owns its DB
directories under `TMPDIR`. The ordinary binary measures latency, while
`bench-metrics` provides separate per-thread phase timings. [Driver](../../../benches/engine_profile/main.rs)

Jump to the [workload settings](#workloads) or [build and run commands](#build-and-run) to reproduce the trials.

These direct-engine results are from a Ryzen 9 5900X, x86_64 Linux, local
NVMe/Btrfs with compression disabled in the scratch directory, Rust 1.100.0
and CPUs 4–7. Each process used fresh DBs and uncompressed SSTs with a 4 KiB
target block size. Each p99 describes one run. Each gain is computed per pair.
A/A is the largest absolute difference between two matched control runs.

## Sixteen shards under a 1 GiB process budget

Retain an immutable limit of four as the common reference. The fixed-buffer
comparison used 16 MiB per buffer. The equal-allowance comparison held
`(1 + immutable limit) × buffer size` to 30 MiB per shard. Both opened
16 DBs with a shared pool allowing 256 MiB for each evictable store.
A 128-byte put and warm read targeted shard zero. Sixteen writers sent
32-entry batches of 256-byte values to one hot shard or one shard each.
Each paced stream completed 10,000 operations. The separate saturation arm
counted all completions during the eight-second worker window.

| Sweep | Immutable limit / buffer | One hot shard p99, ms | Hot saturation, MiB/s | Hot peak RSS, MiB | All active p99, ms | All saturation, MiB/s | All peak RSS, MiB |
|---|---|---:|---:|---:|---:|---:|---:|
| Fixed buffer | 4 / 16 MiB (control) | 1.981 | 123.2 | 202.9 | 15.548 | 303.2 | 936.4 |
| Fixed buffer | 1 / 16 MiB | 64.703 | 102.6 | 210.3 | 90.530 | 293.7 | 362.0 |
| Fixed buffer | 2 / 16 MiB | 1.921 | 118.7 | 183.7 | 12.679 | 289.2 | 625.5 |
| Equal allowance | 4 / 6 MiB (control) | 2.090 | 58.8 | 155.4 | 18.829 | 234.7 | 528.3 |
| Equal allowance | 1 / 15 MiB | 52.920 | 99.5 | 208.8 | 60.330 | 289.9 | 342.8 |
| Equal allowance | 2 / 10 MiB | 2.156 | 90.5 | 175.7 | 12.109 | 258.4 | 455.2 |

The p99 and throughput are per-arm medians, and RSS is the highest value
across both loads. Every configuration completed the paced offer and
stayed under 1 GiB. Depth one raised foreground p99 in both placements.
At a fixed 16 MiB buffer, depth two lost 5.69% of all-active paired
saturation throughput (A/A 2.64%). Under the 30 MiB allowance, depth
two with 10 MiB buffers improved saturation by 53.19% hot and 9.33%
all-active in five of five pairs, against A/A 16.58% and 1.23%.
The nominal allowance excludes cache and transient allocations.
[All geometries, debt and gauges](memory-summary.csv) · [Raw memory data](final-memory.jsonl.gz)

| Equal-allowance load | Depth 4 / 6 MiB at end | Depth 2 / 10 MiB at end | Compaction output / logical byte, depth 4 → 2 |
|---|---:|---:|---:|
| Hot paced | 21 / 1 | 21 / 0 | 6.020 → 3.626 |
| All active paced | 60 / 0 | 32 / 0 | 0.475 → 0.317 |
| Hot saturation | 15 / 0 | 24 / 0 | 3.516 → 2.220 |
| All active saturation | 76 / 40 | 53 / 8 | 0.435 → 0.292 |

The hot saturation candidate left nine additional L0 files relative to its
control. In the all-active arm it left fewer L0 files and pending immutable
tables while admitting more bytes. The depth-one, 16 MiB diagnostic
accumulated 69.54 s of immutable-capacity waits across worker threads
on the hot shard, compared with zero at depths two and four.

A longer fixed-offer comparison completed 20,000 requests per stream.
All five independent A/A and A/B pairs passed the backlog check at
251 requests/s per stream hot and 1,381 all-active.

| Placement | Depth 4 / 6 MiB median p99 | Depth 2 / 10 MiB median p99 | Paired gain and range | Largest A/A difference | Pairs favoring depth 2 |
|---|---:|---:|---|---:|---:|
| One hot shard | 2.017 ms | 2.143 ms | −5.60% (−14.73% to −0.02%) | 11.11% | 0/5 |
| All active | 17.327 ms | 13.223 ms | +35.17% (−73.35% to +49.92%) | 73.98% | 3/5 |

The paired hot p99 cost at depth two exceeded the 5% budget in the
median, with all five pairs slower. The all-active p99 gain was within
control variation. All-active write throughput remains a reason to test
that geometry on a deployment's own traffic. The VSDB v18 adapter's
separate end-to-end trial selected depth four and 16 MiB buffers at its
2 GiB/16-shard setting. [Per-seed guard](memory-tail-extension-summary.csv) · [Raw guard data](memory-tail-extension.jsonl.gz) · [VSDB adapter comparison](https://github.com/rust-util-collections/vsdb/blob/master/docs/evaluations/v18-adapter/README.md)

## Small foreground writes during bulk updates

A scalar producer sent 10,000 puts of 128-byte values. A bulk producer
sent one 4,096-entry batch for each 128 scalar arrivals. The split
configuration moved the bulk producer to a second DB. Both placements
received the same offer and matched keys for every pair.

| Payload and durability | Offered small puts/s | Same-shard p99 | Split p99 | Paired gain | Largest A/A difference | Pairs favoring split |
|---|---:|---:|---:|---:|---:|---:|
| Repeated bytes, `WalAsync` | 16,765 | 4.346 ms | 1.525 ms | +64.92% | 34.96% | 5/5 |
| High entropy, `WalAsync` | 18,911 | 4.068 ms | 2.429 ms | +48.89% | 450.34% | 5/5 |
| Repeated bytes, `WalSync` | 923 | 5.263 ms | 2.621 ms | +51.39% | 1,419.43% | 5/5 |
| High entropy, `WalSync` | 1,006 | 4.857 ms | 2.738 ms | +44.87% | 287.29% | 4/5 |

Only the repeated-byte async result exceeded its A/A band. In the
high-entropy synchronous mix, obtaining a fresh snapshot and reading
measured 9.912 ms p99 on one DB and 7.687 ms on separate DBs. Reads
reusing an existing snapshot measured 91.2 µs and 81.2 µs. Each bulk
class had 79 completions, so only its throughput contributes to the
decision. [Matched pairs](mixed-summary.csv) · [Raw mixed runs](final-mixed.jsonl.gz)

For the same offer and first seed, independent diagnostic runs recorded
the following cumulative times per executing thread. Time waiting for
a leader can overlap its WAL or apply work.

| High-entropy trace | Groups | WAL append + flush/sync, ms | Memtable apply, ms | Leader-queue wait across threads, ms |
|---|---:|---:|---:|---:|
| WalAsync, same shard | 10,076 | 120.3 | 120.4 | 199.5 |
| WalAsync, separate shards | 10,079 | 123.6 | 123.6 | 0.0 |
| WalSync, same shard | 10,076 | 6,141.9 | 135.6 | 314.7 |
| WalSync, separate shards | 10,079 | 6,977.1 | 138.3 | 0.0 |

The asynchronous mixed trial eliminated leader-queue waiting when the
two write streams used separate DBs. Sync WAL work dominated at both
placements. Thread-local instrumentation perturbs timing; the latency
comparison above uses ordinary builds. [Stage observations](stage-summary.csv) · [Split diagnostics](split-stage-samples.jsonl.gz)

## Writer handoff and overlap

Retain the existing serial writer and group storage. With inline short
request lists and an uncontended wakeup fast path, the extended same-DB
trace (100,000 small puts, 782 bulk batches, high entropy, WalAsync)
had median run p99 of 7.177 ms for the control and 9.194 ms for the
candidate. The paired median gain was **−8.10%**, range −62.16% to
+75.71%, with two of five favorable pairs and A/A 210.10%.
In the separate synchronous mix, p99 also increased, from 22.19 to
61.99 ms. The trial did not meet the 15% latency target. Five independent
ordered-stage replays on the warmed two-issuer trace projected a median
gain of only 0.167% even with zero additional handoff cost.
[Extended data](handoff-primary-extension.jsonl.gz) · [Extended pairs](handoff-extension-summary.csv) · [Guard matrix](handoff-summary.csv) · [Raw guard data](final-handoff.jsonl.gz) · [Writer replay model and verification](writer-trace/README.md)

## Reopen from a known residual WAL

A child wrote and synced the fixture, then exited without closing the DB.
Each seed and size used the same residual WAL across geometries, with a
64 MiB preparation buffer. Only open was timed. The parent sampled RSS at
open and verified every recovered row. All 70 processes passed.

| Fixture | WAL bytes | Writable median range across six geometries | Read-only median at depth 4 / 6 MiB | Maximum observed writable peak at open |
|---|---:|---:|---:|---:|
| 10,000 rows | 1,720,364 | 13.007–13.622 ms | 6.399 ms | 8.2 MiB |
| 100,000 rows | 17,203,612 | 110.800–112.788 ms | 61.747 ms | 23.3 MiB |

At 100,000 rows and immutable depth four, the 6 MiB writable buffer
peaked at 14.0 MiB on open versus 23.2 MiB for the 16 MiB buffer.
Read-only open keeps replayed entries in memory and peaked at 23.2 MiB
on that fixture. [All reopen geometries](reopen-summary.csv) · [Raw reopen data](final-reopen.jsonl.gz) · [Reopen implementation](../../../benches/engine_profile/reopen.rs)

## Workloads

| Setting | Meaning |
|---|---|
| `MMDB_PROFILE_OPS`, `SEED` | Primary operations per stream and deterministic trace seed. All settings in this table use the `MMDB_PROFILE_` prefix. |
| `RATE` | Offered scalar operations/s per foreground stream. Zero selects a closed-loop saturation run. Each worker replays its own scheduled arrivals, including arrivals while an earlier call is blocked. |
| `SECONDS` | Common duration of a saturation window. With a nonzero value every stream runs until the same deadline. The fixed-count smoke trace sets this to zero. |
| `PLACEMENT` | `hot` uses one DB for all traffic. For `mixed`, `split` moves the large batches to a second DB. For `memory`, `all` activates all 16 DBs sharing one pool. The `hot` memory case also opens and preloads 16 handles. |
| `CONTROL` | `mixed` enables all streams, `writer` selects one scalar writer and `reader` one warm pinned reader. |
| `BUFFER_MIB`, `IMMUTABLES`, `POOL_MIB` | Per-DB write-buffer size, immutable limit and the configured capacity of each evictable store in the shared pool. |
| `ENTROPY`, `SYNC`, `SAMPLE` | Boolean switches for deterministic pseudorandom payloads, per-write WAL sync, and independent 50 ms memory sampling. The compiled `bench-metrics` feature separately enables thread-local stage clocks. |
| `BATCH_ENTRIES`, `BATCH_RATIO` | Mixed default is 4,096 entries of 256 bytes in one batch per 128 foreground puts. Standalone `memory` defaults to 64-entry batches, while `run.py` sets 32 for the 16-shard sweeps. |
| `PREPARE_MIB`, `READ_ONLY` | Reopen child write-buffer size (defaults to `BUFFER_MIB`; `run.py` sets 64 MiB) and boolean switch selecting `DB::open_read_only_with_options` over writable `DB::open`. |

Mixed and memory cases preload 32,768 16-byte keys per DB with eight-byte
prefixes and warm the scalar read set. Ordinary writes contain 128-byte
values. Fixed-buffer and equal-allowance memory sweeps use their own
matched control geometries and rates. The optional 50 ms sampler reports
component maxima. Linux process high-water RSS includes fixture setup and
harness allocations. These components can peak at different times.

For paced tests the offered rate is 70% of a sustained, verified control
rate. Check completions and outstanding requests at the midpoint and end.
Saturation uses a common deadline and bounded per-worker latency reservoirs
weighted by completions. Within-run p99 requires at least 10,000 completed
latency samples. Service time starts after key/batch construction, while
arrival-to-ack includes preparation and any queueing. Each reopen fixture
comes from a child process that synced its writes and exited before closing.
The parent times open and then verifies the entire visible row stream.

## Build and run

Use separate target directories for the ordinary control, the instrumented
control and any candidate. Cargo prints their executable paths with
`cargo +stable bench --bench engine_profile --no-run`; add
`--features bench-metrics` for the instrumented build. For the exact
recorded mixed, memory and handoff sources, create two detached checkouts
at `cf40cab` and restore `benches/engine_profile` from `7b46443` in both.
Apply the [short-group patch](short-group.patch) in the handoff candidate
only. The reopen measurements use `bb81151`.

From the repository root, use an uncompressed scratch directory on a local
filesystem. Each packet includes the effective configuration, seeds, binary
fingerprints, raw counts and run metrics.

```sh
d=docs/benchmarks/engine-profile
python3 "$d/run.py" mixed ./control mixed.jsonl.gz --metrics ./instrumented --cpus 4-7 --tmpdir "$scratch"
python3 "$d/run.py" memory ./control memory.jsonl.gz --metrics ./instrumented --cpus 4-7 --tmpdir "$scratch"
python3 "$d/run.py" handoff ./control handoff.jsonl.gz --candidate ./candidate --cpus 4-7 --tmpdir "$scratch"
python3 "$d/run.py" reopen ./reopen reopen.jsonl.gz --cpus 4-7 --tmpdir "$scratch"
python3 "$d/run.py" memory-tail ./control tail.jsonl.gz --cpus 4-7 --tmpdir "$scratch" --ops 20000 --seed 20461125
python3 "$d/extend_handoff.py" ./control ./candidate extended.jsonl.gz --cpus 4-7 --tmpdir "$scratch" --rate 18911
python3 -B "$d/summarize.py" "$d/final-memory.jsonl.gz"
python3 -B -m unittest discover -s "$d" -p 'test_*.py'
```

For the 20,000-request memory guard the hot and all-active offers were
251 and 1,381 requests/s per stream. Each seed has an independent A/A
and A/B control. The raw RSS columns record actual high-water observations.
The matched-load status and p99/throughput guards determine eligibility.
Use `summarize.py` on the retained mixed, memory, memory-tail, handoff and
reopen packets. The 100,000-request handoff comparison has its own
[per-seed table](handoff-extension-summary.csv). Supplemental split-shard
write-stage counts use the instrumented mixed driver at the recorded
rates, 10,000 requests and seed 20261126. [Stage counters and rates](stage-summary.csv)

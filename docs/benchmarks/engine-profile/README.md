# Reproducing engine workloads

The optimized `engine_profile` benchmark measures foreground puts, bulk
writes, pinned reads, fresh and held snapshots, memory usage, and recovery
of a known residual WAL. Each arm runs in a fresh process and owns its DB
directories under `TMPDIR`. The ordinary binary measures latency, while
`bench-metrics` provides separate per-thread phase timings. [Measured results](results.md) · [Driver](../../../benches/engine_profile/main.rs)

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
| `BATCH_ENTRIES`, `BATCH_RATIO` | Mixed default is 4,096 entries of 256 bytes in one batch per 128 foreground puts. Each memory worker uses 64-entry batches by default. |

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

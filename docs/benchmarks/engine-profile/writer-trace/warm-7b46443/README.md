# Writer overlap on the warmed, paced control

**Decision** — Keep the serial writer for this two-issuer workload. The
five traces gave a median projected small-put arrival-to-ack p99 improvement
of **0.167%**, a maximum of **0.309%**, and zero pairs reaching the 15%
screen. The alternative that makes an entire neighboring scalar group free
gave at most **0.411%**. These are same-trace model results. Every measured
process used the serial engine. [Per-pair verification](analysis.json) ·
[Ten observed runs](summary.jsonl)

## Why repeat the earlier trace

The [first screen](../README.md) used the harness at `cf40cab`.
`7b46443` preloads all DBs, then reads all 32,768 scalar keys in DB zero
with `get_pinned` before the shared start barrier. The scalar, fresh-snapshot
and held-snapshot readers can affect writer scheduling and the availability
of the next group. This repeat uses that warm path. In these fixed-count
paced arms the common-duration saturation window and reservoir sampling
are inactive. Every completed request contributes one percentile sample
of equal weight. Both harnesses end the throughput window at the last
successful acknowledgement. The corrected outstanding-work cutoff and
cache-miss counter are reported by the newer driver.
[Corrected driver revision](https://github.com/rust-util-collections/mmdb/blob/7b4644369d65b4d7a0a09a582840b99991518e16/benches/engine_profile/concurrent.rs)

The matched offer is 18,911 scalar arrivals/s for each foreground stream
on one DB with `WalAsync`, a bulk arrival every 128 scalar intervals,
4,096 entries per batch, high-entropy payloads, 16 MiB write buffer,
immutable limit 4 and 64 MiB pool. Seeds 20261126 through 20261130 and
cores 4–7 match the original collection. Every fresh process ran in a
NOCOMPRESS Btrfs directory. The measured classes each completed 10,000
scalar operations or 79 batches, with zero rejections. The scheduled
nanosecond timestamp of every small/bulk call equals its counterpart in
the original five traces. The serial replay reconstructs every new
acknowledgement exactly.

| Pair | Recorded groups | Recorded small p99, ms | Projected p99, ms | Gain | Free-neighbor gain |
|---:|---:|---:|---:|---:|---:|
| 1 | 10,074 | 7.199 | 7.187 | 0.167% | 0.224% |
| 2 | 10,075 | 5.300 | 5.300 | 0.000% | 0.000% |
| 3 | 10,073 | 4.992 | 4.978 | 0.292% | 0.292% |
| 4 | 10,078 | 6.312 | 6.312 | 0.000% | 0.000% |
| 5 | 10,076 | 3.793 | 3.782 | 0.309% | 0.411% |

The WAL and apply intervals totaled 146–155 ms and 122–132 ms per run,
respectively. The model overlapped 64–69 eligible group pairs per run
for a total intersection of 0.531–1.674 ms. Tracing-enabled small-put
p99 ranged from 74% below to 44% above its paired tracing-disabled
observation. That variation limits inference about actual latency.
The screen preserves group membership, sequence/generation boundaries,
issue preparation, and scheduling gaps. Changing issuer count, workload
or the way requests group requires a new screen.

Run `python3 verify.py` in this directory to check both sets of schedules,
all new trace digests, every source replay and the screening table.

Apply the [diagnostic patch](trace.patch) to a detached checkout of
`7b46443`, then use the [same recording and replay procedure](../README.md#replay-and-reproduction)
with `../record.py`. The patch collects the timestamps in thread-local
memory during the workload and writes them out after the driver's
end-of-run counters have been sampled. `../replay.py trace-N.csv.gz`
reads each archived trace. Its uncompressed SHA-256 and the measured
binary SHA-256 are recorded alongside the operation counts in
[summary.jsonl](summary.jsonl).

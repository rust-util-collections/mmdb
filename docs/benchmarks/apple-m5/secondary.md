# Prefix cuts and writer-stage replay on Apple M5

## A bounded prefix cut increased cold rewrites

At `5528950`, five matched comparisons produced 40 flushes per process. Writing one extra SST at an eligible eight-byte prefix boundary increased the installed file count from 40 to 80 in every pair. The serial compaction picker still selected all 18 original cold-only L1 files, totaling 1,717,902 bytes. Both the ordinary and one-cut binaries counted cold rows and verified snapshot, current and writable-reopened contents. [Original fixture, patch and Linux results](../l0-profile/README.md#bounded-prefix-cuts-during-flush) · [Native runs](secondary/prefix.csv)

| Metric | Ordinary flush | One cut | Paired change | Largest A/A difference |
|---|---:|---:|---:|---:|
| L0 compaction rewrites through close | 20 | 34 | +75.0% | 5.3% |
| Cold logical bytes emitted through close | 33,423,360 | 56,819,712 | +75.0% | 5.3% |
| New compaction-output file bytes through close | 56,456,549 | 95,952,087 | +75.0% | 5.3% |
| Foreground L0 stall ms/GiB admitted | 195,618 | 575,142 | +201.0% | 13.0% |
| Write arrival-to-ack p99, seconds | 2.262 | 3.652 | +78.6% | 1.2% |

The arm columns are medians of the five A/B observations. The paired change is the median percentage increase calculated within each pair. All 20 processes completed 16,384 reads and 16,384 writes. The offered write trace spanned 0.819 seconds. Writers took 2.81–3.31 seconds to finish in the ordinary A/B arm and 4.37–5.25 seconds with the extra cut. Queueing therefore contributes to arrival-to-ack p99. The candidate increased both cold emission and foreground stalls in all five pairs, missing the 20% stall-reduction threshold. [Metric ranges and controls](secondary/summary.json)

## The two-stage writer model fell below its p99 threshold

At `7b46443`, two issuers submitted 10,000 scalar puts and 79 batches of 4,096 entries in each run. Five warmed traces alternated with five tracing-disabled controls. The serial replay reproduced every acknowledgement. The alternative replay allowed the next group's WAL work to overlap the previous group's memtable insertion at zero additional handoff cost, while keeping group order and WAL-generation boundaries. The median modeled small-put p99 reduction was **0.00%**, with a range of **0.00–2.00%**. None reached the 15% screen. The original Linux trial reported a 0.167% median reduction. [Model and Linux measurements](../engine-profile/writer-trace/README.md) · [Native observations](secondary/writer/summary.jsonl)

| Trace | Serial p99, ms | Modeled p99, ms | Reduction | Overlapping stages | Total overlap, ms |
|---|---:|---:|---:|---:|---:|
| 1 | 5.969 | 5.969 | 0.00% | 66 | 1.040 |
| 2 | 6.386 | 6.386 | 0.00% | 67 | 0.746 |
| 3 | 7.542 | 7.391 | 2.00% | 67 | 1.197 |
| 4 | 5.561 | 5.561 | 0.00% | 66 | 0.819 |
| 5 | 10.673 | 10.673 | 0.00% | 61 | 0.880 |

## Reproduce

From the repository root, use Python 3.12 or later on macOS. The preparation step restores the original revisions and applies the upstream patches in a new scratch directory. Both experiments use the shared native benchmark dependency lock, separate release targets and `bench-metrics`. The prefix candidate runs both boundary tests before the trials. The native harness uses macOS scheduling and samples process memory through Darwin APIs. [Shared benchmark lock](bench.Cargo.lock.gz) · [Host, binary and input digests](secondary/metadata.json) · [Reproduction driver](secondary/reproduce.py)

```sh
area=target/native-m5-secondary
driver=docs/benchmarks/apple-m5/secondary/reproduce.py
python3 -B "$driver" prepare "$area"
python3 -B "$driver" build "$area"
python3 -B "$driver" run "$area"
python3 -B docs/benchmarks/apple-m5/secondary/analyze.py "$area/results"
```

To verify the archived observations directly, pass `docs/benchmarks/apple-m5/secondary` to the analysis script. It checks paired workload sizes, trace digests, scheduled arrivals, sequence order and reconstructed acknowledgements before reporting the paired results. [Analysis script](secondary/analyze.py)

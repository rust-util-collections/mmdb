# MMDB v5 on Apple M5

These runs used a physical Mac17,2 with an Apple M5 (four performance cores,
six efficiency cores), 16 GiB RAM, 16 KiB system pages and a case-sensitive
APFS volume on the internal SSD. macOS 27.2.1 (26B131) scheduled the native
arm64 processes, with AC power connected. Rust 1.100.0 targeted
`aarch64-apple-darwin`. The Linux measurements use a Ryzen 9 5900X and
explicit core affinity. Compare the two methods **within each host**.

The primary engine control was `d5569da` with the test and benchmark overlays
recorded at `25be24a`. The documentation updates through `25be24a` have the same
production engine source. The production scalar comparison used `277ef08`, and the historical
flush-cut and writer-trace experiments used `5528950` and `7b46443`.
The earlier AArch64 validation executed tests in a full-system Linux VM.
[Earlier validation receipt](../engine-profile/validation.json)

The build record identifies the native executables, source overlays, patch
digests, features and dependency locks. [Build provenance](build-provenance.json)

The writer, L0 compaction, 16-shard memory and WAL recovery measurements
have a separate set of paired traces. Further experiments measured
prefix-aligned flush cuts and replayed the warmed two-issuer write trace.
[Engine workloads](engine.md) · [Flush cuts and writer replay](secondary.md)

## Test process file limits

The login shell used for the measurements inherited a soft open-file limit
of 4,096 and an unlimited hard limit. As a negative control, the same default
library test binary was launched directly with soft limit 256 and ordinary
libtest concurrency. Eight of 425 tests returned OS error 24, including the
range-tombstone test opening live SSTs. With hard limit 256 and starting soft
limit 128, the test launcher selected 256 and one test thread. The range
tombstone test and all 22 WAL reader/writer tests passed under that limit.

Starting `make test` at soft limit 256 with a higher hard limit ran the
children at 4,096. Debug and release each passed 577 runtime cases and two
doctests. The feature-enabled library suite passed 429 overlapping cases.
The profiling-only test was ignored as configured. The syscall trace in
the earlier Linux suite is Linux-specific. [Native test counts](validation.json)

The WAL creation failure fixture had a separate scheduling dependency. Its
first write could trigger standby WAL preparation and let the worker use the
file number reserved by the test. The test now keeps the write below that
threshold and verifies the foreground creation failure, retained writer and
data after reopen. [Regression test](../../../src/db/flush.rs) ·
[Test launcher](../../../tools/with-test-fds.sh)

A second low-limit control ran the full suite with one test thread. The
old `test_multi_thread_compaction` alone could exhaust 256 descriptors:
its 4 KiB write buffer could freeze on the first arena slab, leaving
hundreds of disjoint single-row SSTs for 500 ascending puts. The revised
fixture issues the same 500 puts in 25 flushes with overlapping key
ranges, configures four compaction workers, and checks that the background
workers actually rewrote SST bytes before manual compaction. It verifies
every value afterwards. With soft limit 128 and hard limit 256, both debug
and release passed five consecutive focused runs. The final full
`make test` also passed at hard limit 256 with one test thread, with
identical per-binary counts to the run that raised the soft limit to
4,096. [Integration fixture](../../../tests/integration.rs) · [Both complete runs](validation.json)

## Grouped point reads

On this host, grouping 32 nearby keys reduced warm request p99 by 25.30%
and device-read p99 by 66.64%. All five pairs favored the grouped method
in each case. Both methods returned the same visible rows, with index/data
block acquisitions falling from 64 to 3.07 per request. Scattered device
reads improved in all five pairs. The median gain of 5.48% exceeded the
2.01% A/A spread and fell below the 10% target used for clustered keys.
The Linux scattered-device result fell within its own A/A variation.
[Linux matrix](../grouped-reads/README.md)

| Cache | Keys | Scalar p99, µs | Grouped p99, µs | Paired gain | Max A/A |
|---|---|---:|---:|---:|---:|
| Engine warm | 32 clustered | 23.08 | 17.21 | 25.30% | 1.99% |
| Engine warm | 32 scattered | 32.88 | 31.50 | 6.05% | 29.19% |
| OS page cache | 32 clustered | 506.46 | 34.12 | 93.19% | 86.76% |
| OS page cache | 32 scattered | 529.21 | 176.12 | 65.14% | 8.51% |
| Device reads | 32 clustered | 793.21 | 264.62 | 66.64% | 9.41% |
| Device reads | 32 scattered | 3,997.38 | 3,777.04 | 5.48% | 2.01% |

On the clustered page-cache workload the five A/B gains were 93.09–94.63%,
with unusually wide A/A variation. The minimum share of I/O-qualified
requests across all device arms was 96.89% for clustered and 100% for
scattered keys. Both scattered device arms read about 522 kB per request.
The warm scattered result changed direction between pairs.

In independent production builds, warm clustered p99 fell by 33.24% on
one SST and by 26.09% on overlapping L0 and bottom-level SSTs. The current
scalar API was 2.81% slower than the historical scalar control, within
the 5% budget in every pair. The separate instrumented, one-key grouped
trial had a 2.40% median gain but one pair was 5.31% slower and A/A
variation reached 15.57%. [All cells and controls](grouped-summary.csv) ·
[140 instrumented and 60 production runs with fixture metadata](grouped-runs.tar.gz)

Both methods read through the same snapshot and retain every result until
the whole request returns. Each cell uses five fresh-process scalar/scalar
pairs (A/A) and five independent scalar/grouped pairs (A/B), matching seeds
and alternating arm order. P99 covers all requests. The latency columns are
medians across the five A/B runs per arm. Gain is the median of the five
within-pair reductions. The A/A column is the largest absolute difference
between paired scalar controls.

The 262,144-row fixture matches the Linux single-SST fixture. The scattered
device workload uses its 1,048,576-row counterpart. Warm runs populate the
engine cache. Page-cache runs disable the engine cache and read the complete
SST before timing. Device runs request file-scoped page invalidation every
128 requests. For this host the evidence is Darwin's per-process
`ri_diskio_bytesread`. Each warm or page-cache run requires zero measured
bytes. Each device run requires at least 4 KiB for 95% of the requests in
**each** arm, including the A/A controls. [Harness](../../../benches/grouped_reads.rs) ·
[Darwin counters and eviction](../../../benches/grouped_reads/macos.rs)

For a repeat, prepare two fresh stores with the repository's
`grouped_reads prepare DIR ROWS` release executable. Use 262,144 rows for
the small store and 1,048,576 for the large store. The examples below use
the executable printed by Cargo, Python 3 and an unused output name.

```sh
d=docs/benchmarks/grouped-reads
python3 -B "$d/trials.py" "$probe" "$small" warm32.csv \
  --cpus native --revision HEAD --matrix primary --fanout 32 --samples 300000
python3 -B "$d/trials.py" "$probe" "$large" scattered32.csv \
  --cpus native --revision HEAD --matrix device --locality scattered --fanout 32 --samples 10000
```

The warm grouped singleton uses the primary matrix with fanout one and
1,000,000 requests per process. The other 32-key cells use 10,000 requests
per process. The production controls use `guard.py --cpus native` with
50,000 requests for groups and 1,000,000 for singleton reads. The overlap
fixture uses `prepare-overlap DIR 262144` to place odd-key updates in L0.

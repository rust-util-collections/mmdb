# Grouped pinned point reads

At revision `fca4802`, a 32-key clustered request against a warm single SST
had median run p99 of **18.21 µs** with `DB::get_many_pinned_with_options`
and **27.12 µs** with 32 pinned gets on one snapshot. The median within-pair
gain was **33.30%** across five pairs (32.27–35.88%). Its largest A/A
difference was 7.41%. The same trace reduced index/data block acquisitions
from 64 to 3.07 per request on average. Both arms delivered the same 32
values with the same ownership lifetime. These trials used the MMDB API.
VSDB collection workloads require separate measurement. [Primary raw trials](results/final/primary.csv) · [Host and fixture](results/final/primary.metadata.json)

A separate normal-dependency build compared the existing single-key API
at `72d1dd9` and the corrected code, with all engine profiling features
disabled. The initial 300,000-request runs had a 6.21% maximum A/A
difference. A longer calibration of 1,000,000 requests per process gave a
paired median change of −0.63% and a maximum absolute A/A difference of
3.20%. Every A/B pair stayed within the 5% regression budget.
[Initial guard](results/final/default-scalar.csv) · [Longer guard](results/final/default-scalar-extended.csv)

The grouped singleton itself also passed its longer calibration at 1,000,000
requests per process. Its paired median change was 0.00% (−1.29% to +1.56%),
with a largest A/A difference of 2.20%. In separate uninstrumented 32-key
release builds, clustered single-SST p99 improved by 31.54% and the overlapping L0/bottom-SST fixture
by 26.79%, both across five favorable pairs. The latter checks returned
versions for known odd-key overwrites. [Grouped singleton](results/final/primary-singleton-extended.csv) · [Production single SST](results/final/release-fixture.csv) · [Production overlapping SSTs](results/final/release-overlap.csv)

The production integration check repeated the 32-key single-SST workload
with separate release builds of `277ef08` (scalar control) and `736b87e`
(grouped after integration with the writer/metrics work). At 50,000 requests
per process the five paired gains were 35.16–51.29%, with median **37.55%**.
Median run p99 was 28.71 µs / 17.93 µs and the largest A/A difference was
3.87%. MMDB used its default feature set in both binaries.
[Integrated production trials](results/final/integration-release.csv) · [Production build and overlap fixture digests](results/final/production-builds.json)

## Workload matrix

These Linux/x86_64 trials used a Ryzen 9 5900X pinned to CPU 4 and the
release binary identified in the fixture metadata. Each cell has five scalar/scalar pairs and five scalar/grouped pairs. Each
pair uses the same key trace and alternates execution order across rounds.
The two p99 columns show the median of each arm across five scalar/grouped pairs.
The gain is the median change calculated *within* those pairs. This accounts
for shifts in host latency between runs. The A/A column gives the largest
absolute difference between two scalar runs on a matched trace. Block probes
count index and data block acquisitions, including engine-cache hits.
In these single-SST fixtures scalar calls probe one table per key. Grouped
calls probe one per distinct key. The verifier replays the scattered trace
to check the count across duplicate keys.

| Cache | Locality | Keys | Scalar p99 µs | Grouped p99 µs | Paired gain | Max A/A difference | Block probes per request, scalar / grouped |
|---|---|---:|---:|---:|---:|---:|---:|
| warm | clustered | 1 | 3.11 | 3.14 | +0.00% | 2.20% | 2.00 / 2.00 |
| warm | clustered | 8 | 9.14 | 6.91 | +23.67% | 4.29% | 16.00 / 2.24 |
| warm | clustered | 32 | 27.12 | 18.21 | +33.30% | 7.41% | 64.00 / 3.07 |
| warm | scattered | 1 | 3.08 | 3.12 | -0.35% | 0.93% | 2.00 / 2.00 |
| warm | scattered | 8 | 12.87 | 11.97 | +5.45% | 20.20% | 16.00 / 13.42 |
| warm | scattered | 32 | 40.09 | 38.03 | +5.64% | 14.73% | 64.00 / 40.61 |
| page | clustered | 1 | 30.32 | 30.55 | -0.20% | 2.77% | 2.00 / 2.00 |
| page | clustered | 8 | 221.48 | 34.29 | +84.42% | 2.49% | 16.00 / 2.24 |
| page | clustered | 32 | 876.15 | 46.89 | +94.68% | 2.71% | 64.00 / 3.07 |
| page | scattered | 1 | 30.57 | 30.61 | -0.20% | 0.94% | 2.00 / 2.00 |
| page | scattered | 8 | 219.81 | 191.92 | +12.54% | 2.47% | 16.00 / 13.42 |
| page | scattered | 32 | 861.49 | 300.61 | +65.04% | 2.83% | 64.00 / 40.61 |
| device | clustered | 1 | 289.77 | 289.15 | +0.39% | 1.33% | 2.00 / 2.00 |
| device | clustered | 8 | 544.35 | 377.13 | +31.12% | 1.06% | 16.00 / 2.24 |
| device | clustered | 32 | 1,385.25 | 421.73 | +69.63% | 0.91% | 64.00 / 3.07 |
| device | scattered | 1 | 290.00 | 288.86 | -0.39% | 1.65% | 2.00 / 2.00 |
| device | scattered | 8 | 1,383.90 | 1,331.74 | +4.65% | 20.31% | 16.00 / 13.42 |
| device | scattered | 32 | 5,776.73 | 4,607.26 | +10.10% | 10.26% | 64.00 / 52.88 |

The 8- and 32-key clustered gains exceeded the A/A spread in all three
regimes. On scattered keys, gains exceeded the measured spread only in
the page-cache runs. With zero engine-cache admission, both arms issued
buffered reads and checksum validations for blocks they acquired. Reusing
validated blocks can save CPU even when Linux issues zero storage reads.
Across all six singleton controls, the median paired change stayed within
0.4% on this host.

The table uses the extended controls for both warm singletons and the
page-cache clustered singleton. The original short warm scattered A/A
spread was 29.08%, and the extended 300,000-request maximum was 0.93%.
For the page-cache clustered singleton, a short A/A pair differed by
47.38%. The prespecified 50,000-request recalibration gave 2.77% maximum
A/A. Other secondary cells used 10,000 requests per process.
[Warm trace](results/final/warm.csv) · [Extended warm scattered singleton](results/final/warm-singleton.csv) · [Page trace](results/final/page.csv) · [Extended page singleton](results/final/page-singleton-clustered-extended.csv)

### Device evidence

Coverage below is the minimum across all twenty runs in a cell. The
conditional p99 includes only requests with at least 4 KiB of Linux
`read_bytes`. The final column divides each process's Linux `read_bytes`
by **all completed requests**, then takes the median per arm across the
five scalar/grouped pairs. The workload matrix above uses full-request p99.

| Locality | Keys | Minimum coverage | I/O-qualified p99 µs, scalar / grouped | Linux read bytes per request, scalar / grouped |
|---|---:|---:|---:|---:|
| clustered | 1 | 99.14% | 289.78 / 289.30 | 12,911 / 12,911 |
| clustered | 8 | 98.88% | 545.54 / 377.41 | 16,148 / 17,039 |
| clustered | 32 | 98.14% | 1,385.55 / 421.98 | 26,898 / 33,724 |
| scattered | 1 | 99.16% | 290.07 / 288.90 | 12,940 / 12,940 |
| scattered | 8 | 100.00% | 1,383.90 / 1,331.74 | 70,864 / 71,286 |
| scattered | 32 | 100.00% | 5,776.73 / 4,607.26 | 284,125 / 285,696 |

Five of the six device cells used the original 262,144 rows. Eight
of the twenty original 32-key scattered observations missed 95% coverage,
with a minimum of 92.94%. The original 120 observations are archived.
The eligible subset contains all twenty rows of each qualifying original
cell. The entire scattered 32-key cell was then repeated with matched arms,
seeds and cache settings on one 1,048,576-row SST (152,622,691 bytes).
All 200,000 requests in that replacement met the 4 KiB condition. The
larger keyspace also changes the potential for block reuse.
[Unfiltered device trace](results/final/device-original.csv) · [Eligible original cells](results/final/device-qualified-cells.csv) · [Cell-selection metadata](results/final/device-qualified-cells.metadata.json) · [Larger fixture trace](results/final/device-scattered-32-large.csv) · [Larger fixture and binary digests](results/final/device-scattered-32-large.metadata.json)

## Mechanism and correctness

One DB read view fixes the sources and one read sequence fixes visibility.
The request sorts input slots internally, coalesces duplicates and retains
validated blocks in two recent SST cursors. Results are restored to original
input order. Storage misses use the existing synchronous positional reads.
With the engine cache disabled, reusing one index leaf also avoids repeatedly
validating its OS-cached bytes. In an uncached fixture with alternating odd
keys in L0 and a bottom table, earlier code acquired 64 blocks across 32 slots. The two
cursors acquired four (one index leaf and one data block per SST) and the
32 returned values shared two allocations. A snapshot lookup past the
final partition of a third SST preserves these cursors. That case acquired
eight blocks before the partition check was corrected and four afterward.
A separate regression test reads a valid lower key twice and then encounters
a corrupt later SST. Before correction the failed request added 12 to
`stats.bytes_read`. It now drops the earlier pins and adds zero. Overlapping
more than two candidate files can evict a cursor and require another block access.
[Grouped-read regression tests](../../../tests/grouped_reads.rs) · [Preliminary experiments](results/README.md)

## Reproduce the comparison

Use a supported Linux local filesystem, Python 3.11+, and an explicit CPU
affinity. The device arm uses Python's file-scoped `posix_fadvise` to request
eviction and samples the calling process's Linux `read_bytes` counter.
Compile a release binary with the test instrumentation enabled. Cargo prints
the executable path. Use a new directory for the store and retain the
neighboring `.rows` file across trials.

```sh
cargo +stable bench --features test-utils --bench grouped_reads --no-run
"$probe" prepare "$fixture" 262144
python3 -B docs/benchmarks/grouped-reads/trials.py "$probe" "$fixture" primary.csv \
    --matrix primary --samples 300000 --cpus "$cpus" --revision "$revision"
```

Run the remaining `warm`, `page` and `device` matrices with 10,000 requests
per run. To extend the scattered singleton control, select `--matrix warm
--fanout 1 --locality scattered --samples 300000`. The runner places machine,
binary, fixture and filesystem metadata beside the CSV and checks each
run's cache evidence. Use `summarize.py FILE.csv ...` for the paired results
and `python3 -B -m unittest discover -s docs/benchmarks/grouped-reads` for the
cache-regime classification checks.

If a device cell fails the storage criterion, preserve the observations and
repeat the entire paired cell on a larger fixture. To reproduce this case,
prepare 1,048,576 rows and select `--matrix device --locality scattered --fanout 32` with both arms using that new fixture.

Both read methods pin the same snapshot. Timing starts after construction
of the key list and stops when the complete result vector returns.
Every value is then checked against its key and all pins released together.
A caller that releases a scalar pin immediately after decoding it has a
different retention pattern from this all-results workload.
`peak_pin_bytes` charges each distinct returned SST allocation once, using its
buffer capacity. The fixture budget of 143,360 bytes covers the returned
allocations for this 128-byte value, 4 KiB block workload. Reader scratch,
memtables, and process RSS have different ownership and sizes. With cache
admission disabled a scalar loop may load the same data block into a new
allocation for each key. Grouped pins can instead share that allocation.
For example, on the first 10,000-request clustered 32-key page-cache trace,
the maximum simultaneous returned capacity was 131,890 bytes for the scalar
loop and 12,361 bytes for grouped reads. The largest observed capacity across
all accepted cells and both arms was 131,890 bytes.

The 38,155,840-byte bottom-level SST contains 262,144 deterministic eight-byte
keys and high-entropy inline values. Nearby queries permute keys in one
consecutive group. Scattered queries draw uniformly over the same keyspace.
The warm arm preloads all rows into a 128 MiB engine cache. The page and device
arms disable engine cache admission. The page arm reads the entire SST through
the OS cache before timing. The device arm requests eviction every 128 queries
on the fixture SST only. A warm or page trial requires zero kernel-reported
storage reads during measurement. A device trial requires at least 4 KiB of Linux
`read_bytes` on at least 95% of requests. Its primary statistic is p99 over the full matched
trace. The percentile conditional on those I/O-qualified requests is
reported separately. Other blocks in a qualified request can be OS-cache hits.
Readahead and the underlying SSD controller can affect those latencies.

For production-build controls, build `scalar_guard.rs` twice as a standalone
binary whose sole dependency is the corresponding MMDB path.
Use separate Cargo target directories for the two builds, the resolved
lockfile, and the regular release profile with thin LTO and one codegen unit.
Leave MMDB's default features in effect. Pass those
two binaries and the same fixture to `guard.py BASELINE CANDIDATE FIXTURE
OUTPUT --cpus CPUSET --samples 300000`. This guard calls the published
single-key API and checks the returned value after its timer. Declare a local
`grouped-api = []` feature in the guard crate. Enable it only when building the
candidate guard so it can also invoke the new method. MMDB itself retains its
default feature set. Use `--fanout 32 --samples 50000` for uninstrumented
group trials on the single-SST fixture and a separately prepared
`prepare-overlap DIR 262144` fixture. The latter holds all keys at the bottom
and odd-key replacements in one L0 file. The guard checks the visible version
besides the key and value length.

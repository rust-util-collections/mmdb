# Grouped pinned point reads

At revision `fca4802`, a 32-key clustered request against a warm single SST
had median run p99 of **18.21 µs** with `DB::get_many_pinned_with_options`
and **27.12 µs** with 32 pinned gets on one snapshot. The median within-pair
gain was **33.30%** across five pairs (32.27–35.88%). Its largest A/A
difference was 7.41%. The same trace reduced index/data block acquisitions
from 64 to 3.07 per request on average. Both arms delivered the same 32
values with the same ownership lifetime. These reads measure the MMDB API. The distribution of actual collection queries remains a separate VSDB
integration decision. [Primary raw trials](results/final/primary.csv) · [Host and fixture](results/final/primary.metadata.json)

A separate normal-dependency build compared the existing single-key API
at `72d1dd9` and the corrected code, with all engine profiling features
disabled. The initial 300,000-request runs had a 6.21% maximum A/A
difference. A longer calibration of 1,000,000 requests per process gave a
paired median change of −0.63% and a maximum absolute A/A difference of
3.20%. Every A/B pair stayed within the 5% regression budget.
[Initial guard](results/final/default-scalar.csv) · [Longer guard](results/final/default-scalar-extended.csv)

The grouped singleton itself also passed its longer calibration at 1,000,000
requests per process. Its paired median change was 0.00% (−1.29% to +1.56%),
with a largest A/A difference of 2.20%. In separate uninstrumented 32-key release builds, clustered
single-SST p99 improved by 31.54% and the overlapping L0/bottom-SST fixture
by 26.79%, both across five favorable pairs. The latter checks returned
versions for known odd-key overwrites. [Grouped singleton](results/final/primary-singleton-extended.csv) · [Production single SST](results/final/release-fixture.csv) · [Production overlapping SSTs](results/final/release-overlap.csv)

The production integration check repeated the 32-key single-SST workload
with separate release builds of `277ef08` (scalar control) and `736b87e`
(grouped after integration with the writer/metrics work). At 50,000 requests
per process the five paired gains were 35.16–51.29%, with median **37.55%**.
Median run p99 was 28.71 µs / 17.93 µs and the largest A/A difference was
3.87%. MMDB used its default feature set in both binaries.
[Integrated production trials](results/final/integration-release.csv)

## Mechanism and correctness

One DB read view fixes the sources and one read sequence fixes visibility.
The request sorts input slots internally, coalesces duplicates and retains
validated blocks in two recent SST cursors. Results are restored to original
input order. In an uncached fixture with alternating odd keys in L0 and a
bottom table, earlier code acquired 64 blocks across 32 slots. The two
cursors acquired four (one index leaf and one data block per SST) and the
32 returned values shared two allocations. A snapshot lookup past the
final partition of a third SST preserves these cursors. Errors discard any
earlier pins and leave `stats.bytes_read` unchanged. Overlapping more than
two candidate files can evict a cursor and require another block access.
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

Both read methods pin the same snapshot. Timing starts after construction
of the key list and stops when the complete result vector returns.
Every value is then checked against its key and all pins released together.
A caller that releases a scalar pin immediately after decoding it has a
different retention pattern from this all-results workload.
`peak_pin_bytes` charges each distinct returned SST allocation once, using its
buffer capacity. The fixture budget of 143,360 bytes covers the returned
allocations for this 128-byte value, 4 KiB block workload. Reader scratch,
memtables, and process RSS have different ownership and sizes.

The 38,155,840-byte bottom-level SST contains 262,144 deterministic eight-byte
keys and high-entropy inline values. Nearby queries permute keys in one
consecutive group; scattered queries draw uniformly over the same keyspace.
The warm arm preloads all rows into a 128 MiB engine cache. The page and device
arms disable engine cache admission. The page arm reads the entire SST through
the OS cache before timing. The device arm requests eviction every 128 queries
on the fixture SST only. A warm or page trial requires zero kernel-reported
storage reads during measurement. A device trial requires at least 4 KiB of Linux-accounted storage reads on
at least 95% of requests. Its primary statistic is p99 over the full matched
trace. The percentile conditional on those I/O-qualified requests is
reported separately. Other blocks in a qualified request can be OS-cache hits.
Readahead and the underlying SSD controller can affect those latencies.

For production-build controls, build `scalar_guard.rs` twice as a standalone
binary whose sole dependency is the corresponding MMDB path.
Use separate Cargo target directories for the two builds, the resolved lockfile, and the regular release profile with thin LTO and
one codegen unit, and leave MMDB's default features in effect. Pass those
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

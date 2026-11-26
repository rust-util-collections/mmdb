# Grouped pinned point reads

For 32 clustered keys in a warm single SST, 32 scalar pinned reads took
**27.12 µs** at request p99 and one `DB::get_many_pinned_with_options`
took **18.21 µs**. The median paired reduction was **33.30%** (32.27–35.88%,
five favorable pairs, largest scalar/scalar change 7.41%). Index/data block
acquisitions fell from 64 to 3.07 per request. The public API resolves up to
32 keys against one view and sequence, retains the validated blocks in two
SST cursors and restores the original input order. The same snapshot and
result lifetime apply to both benchmark arms. [Implementation](../../../src/db/read.rs) · [Regression tests](../../../tests/grouped_reads.rs)

Separate production builds with default features confirmed a **37.55%**
paired p99 reduction on the single-SST workload and **26.79%** on a
two-SST fixture with odd-key updates in L0. A scalar API control using
one million requests per process measured a −0.63% gain, within its
3.20% scalar/scalar variation. The effect of using grouped reads in an
application depends on its keys and traversal. [Production single SST](results/final/integration-release.csv) · [Overlapping SSTs](results/final/release-overlap.csv) · [Scalar control](results/final/default-scalar-extended.csv) · [Build fingerprints](results/final/production-builds.json) · [VSDB VecDex query measurements](https://github.com/rust-util-collections/vsdb/blob/master/docs/evaluations/v18-vecdex-v5/README.md)

## Request latency

Each Linux/x86_64 cell used five independent scalar/scalar (A/A) and five
scalar/grouped pairs pinned to CPU 4 of a Ryzen 9 5900X. The p99 columns
are per-arm medians. Gain is computed inside each matched pair over the
full completed request population. A/A is the largest absolute change
in a matched pair of scalar controls. Each warm/page trial observed zero
Linux storage reads. The warm arm used a 128 MiB MMDB cache. The page
arm read the SST into the OS page cache with MMDB block caching disabled.
The disk arm verified Linux read traffic and also disabled the MMDB cache.
[Fixture, compiler and host](results/final/primary.metadata.json)

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

The 8- and 32-key clustered gains exceeded control variation in all three
regimes. For scattered keys, only the page-cache cell showed a gain beyond
that variation. All six singleton median changes were within 0.4%. The
table uses longer controls for both warm singletons and the clustered
page-cache singleton. [Warm primary](results/final/primary.csv) · [Warm matrix](results/final/warm.csv) · [Page matrix](results/final/page.csv) · [Clustered singleton](results/final/primary-singleton-extended.csv) · [Scattered singleton](results/final/warm-singleton.csv) · [Page singleton](results/final/page-singleton-clustered-extended.csv)

## Verified storage reads and pins

Every accepted disk trial registered at least 4 KiB in Linux process
`read_bytes` on at least 95% of its requests. Coverage is the minimum across
all 20 processes in each cell. The conditional p99 below includes only
qualified requests. The preceding table uses the complete trace. Storage
bytes per request divides by **all** completed requests. Device readahead
and mixed page hits in one query can affect those counts.

| Locality | Keys | Minimum coverage | I/O-qualified p99 µs, scalar / grouped | Linux read bytes per request, scalar / grouped |
|---|---:|---:|---:|---:|
| clustered | 1 | 99.14% | 289.78 / 289.30 | 12,911 / 12,911 |
| clustered | 8 | 98.88% | 545.54 / 377.41 | 16,148 / 17,039 |
| clustered | 32 | 98.14% | 1,385.55 / 421.98 | 26,898 / 33,724 |
| scattered | 1 | 99.16% | 290.07 / 288.90 | 12,940 / 12,940 |
| scattered | 8 | 100.00% | 1,383.90 / 1,331.74 | 70,864 / 71,286 |
| scattered | 32 | 100.00% | 5,776.73 / 4,607.26 | 284,125 / 285,696 |

The main fixture is one 38,155,840-byte SST with 262,144 eight-byte
keys, 128-byte high-entropy values and 4 KiB blocks. For 32 scattered
disk reads the storage coverage at that size fell to 92.94%. The reported
cell uses the complete five pairs of each kind on one 1,048,576-row SST
instead. All 200,000 requests in the larger fixture met the threshold.
The larger keyspace also changes the opportunity for block reuse.
[Qualified disk cells](results/final/device-qualified-cells.csv) · [Coverage and selection](results/final/device-qualified-cells.metadata.json) · [Larger matched fixture](results/final/device-scattered-32-large.csv)

The timed operation ends when the complete result vector returns. The
driver then validates each value and releases the pins together.
`peak_pin_bytes` counts capacity of distinct SST allocations held by
returned pins. The maximum observed was 131,890 bytes against the
143,360-byte fixture budget. In the first clustered 32-key page-cache
trace, the maxima were 131,890 bytes for scalar reads and 12,361 for
grouped reads. A caller releasing pins as it decodes has a different
lifetime and footprint.

## Reproduction

From the repository root build the Rust benchmark and prepare a fresh local
fixture. Set `probe` to the executable Cargo prints and `fixture` to an
unused directory. Set `cpus` to the desired affinity on Linux or to
`native` for macOS scheduling.
The runner stores the executable and fixture digests beside each output CSV.
[Rust benchmark](../../../benches/grouped_reads.rs) · [Apple M5 results and I/O accounting](../apple-m5/README.md)

```sh
cargo +stable bench --features test-utils --bench grouped_reads --no-run
"$probe" prepare "$fixture" 262144
d=docs/benchmarks/grouped-reads
python3 -B "$d/trials.py" "$probe" "$fixture" primary.csv \
    --matrix primary --samples 300000 --cpus "$cpus" --revision HEAD
python3 -B "$d/summarize.py" "$d"/results/final/{primary,warm,page,device-qualified-cells,device-scattered-32-large}.csv
python3 -B "$d/summarize.py" "$d"/results/final/{primary-singleton-extended,warm-singleton,page-singleton-clustered-extended}.csv
python3 -B -m unittest discover -s "$d"
```

For the warm, page and device matrices use 10,000 requests per process.
The clustered warm singleton used `--matrix primary --fanout 1` with one
million requests, the scattered warm singleton used 300,000 requests and
the clustered page singleton used 50,000. The latter two use `--matrix warm --fanout 1 --locality scattered` and
`--matrix page --fanout 1 --locality clustered`, respectively. For
the large disk cell create a separate 1,048,576-row fixture and select
`--matrix device --locality scattered --fanout 32`.

The measured grouped sources carry the identifiers `fca4802` and
`736b87e` in the raw metadata. For the integrated comparison, main-history
`c97bb5c` contains the exact same crate manifest, Rust library and benchmarks
as `736b87e`. Build the scalar control at `277ef08`. The earlier measured
engine and probe can be reconstructed in a detached checkout at `72d1dd9`
using the grouped-read change from main history:

```sh
git diff c97bb5c^ c97bb5c -- src benches/grouped_reads.rs | git apply --3way
git show c97bb5c:Cargo.toml | tail -5 >> Cargo.toml
```

For the independent uninstrumented guard, build [scalar_guard.rs](scalar_guard.rs)
as a standalone binary with MMDB as its sole path dependency. Use separate
target directories, a regular release profile with thin LTO/one codegen
unit and MMDB default features for both builds. Enable a guard-local
`grouped-api = []` feature only for the candidate. Run [guard.py](guard.py)
with the baseline, candidate, fixture and output paths, `--cpus "$cpus"`
and `--samples 1000000` for the singleton or `--fanout 32 --samples 50000`
for grouped reads. The overlapping fixture is built with
`"$probe" prepare-overlap "$fixture" 262144`.

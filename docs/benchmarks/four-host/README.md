# MMDB v5 on four physical hosts

For the direct binary comparison and results after compaction, start with
the [5.0.1 follow-up](#follow-up-on-mmdb-501). For the original workload
tables by host and volume, use the [baseline at `828e47a`](#historical-campaign-at-828e47a).
To rebuild the drivers or verify results from either archive, see the
[reproduction instructions](reproduce.md).

Both archives preserve the v5 performance observations byte for byte. This
checkout carries engine-only excerpts of the accompanying build and test
receipts. The campaign manifests identify the source commit, original
archive digests, and digests of the excerpts. [Original v5 publication](https://github.com/rust-util-collections/mmdb/tree/e0e490dce4eb53f03ed7e56d036779e906dfb3c1/docs/benchmarks/four-host)

## Follow-up on MMDB 5.0.1

The follow-up freezes production revision `b18532d` and retains
`828e47a` as the binary control. The [retest protocol](retest.md)
keeps the work and decision thresholds comparable while sharing physical
observations between questions. See the [frozen tool and lock
fingerprints](retest-5.0.1.json).

**Workflow check.** On each dual-volume Linux host, 130 distinct
standard storage/reopen executions supplied 160 observations across
the codec, WAL, reopen and disk-comparison views. Those 30 shared
observations per host were verified against the same child output,
timestamps, counters, binary and work ID. The four primary-only warm
cells removed another 60 duplicate warm executions. On M5 a repeat
invocation loaded 25 completed standard checkpoints (60 physical
observations) in 0.094 seconds with byte-identical output and zero new
measurements. A Linux CPU-affinity query was corrected at preflight;
the already committed 20-process version sample was reused.

**Same-session version comparison.** Each entry is the paired gain of
5.0.1 over `828e47a` at identical settings / the largest old/old
control change. Positive values mean less time or greater throughput.
The storage sentinel uses high-entropy, uncompressed SSTs and disabled
MMDB cache admission. The writer sentinel uses 4 frozen slots / 6 MiB,
one hot shard, and the same eight-second saturation window in both arms.

| Primary stack | Sorted ingest | Prewarmed scan | Random read p99 | Saturation throughput | Through final compaction |
|---|---:|---:|---:|---:|---:|
| M5 / APFS | −3.15% / 46.63% | −1.37% / 30.25% | −0.85% / 0.84% | +0.13% / 93.43% | −1.43% / 122.01% |
| 5900X / dual Btrfs | +1.28% / 2.83% | +5.50% / 4.72% | −32.31% / 3.62% | −3.31% / 13.29% | −3.32% / 12.77% |
| EPYC / dual Btrfs | +0.25% / 4.95% | +1.28% / 18.47% | +3.11% / 5.23% | −0.60% / 2.68% | −0.55% / 5.36% |
| 9950X / ZFS | −0.49% / 11.37% | −0.72% / 4.55% | −0.30% / 8.29% | −2.59% / 4.10% | −1.42% / 4.33% |

All four within-host throughput changes fall inside their control bands,
both at the foreground boundary and at the common maintenance endpoint.

The first 5900X revision sentinel produced a 32.31% increase in prewarmed
random-read p99 (five matched pairs, 3.62% maximum A/A change). This
triggered an independent confirmation **after** the primary measurement:
same uncompressed, high-entropy SST, two unchanged binaries, 40,000
validated lookups per process, five fresh A/A and A/B pairs. The
confirmation uses seeds 20261227–20261231 on the original four-core
mask and repeats them on CPU 4 to check scheduling sensitivity. Each
panel retains its own primary statistic and noise band. A reproducible
four-core loss must also clear the 5% guard and the independent A/A
band, with at least four pairs slower, on the same-affinity repeat.
The single-core panel diagnoses placement sensitivity separately.

The same-affinity confirmation reproduced the loss: median run p99
was 33.48 µs on the old binary and 44.77 µs on 5.0.1. The within-pair
increase was 32.73%, with four of five pairs slower and 4.92% A/A
variation. Restricted to CPU 4, the two medians were 31.99 and 32.03 µs;
the paired median change was 0.00%, with 0.78% A/A variation. One
single-core pair still exhibited a large loss. These measurements
establish a repeatable **four-core-mask** effect on this host and trace.
They do not identify a particular cache patch, core migration, or a
general x86 penalty.

**Locality and cache fit.** Warm clustered grouping on the four primary
stacks lowered 32-key p99 by 25.83% on M5, 28.41% on 5900X, 27.25% on
EPYC and 40.76% on 9950X. The four physically accepted scattered-device
cells gained only 2.66–6.83%. ZFS again recorded zero physical reads in
its device pilots. On EPYC, the fresh 64 GiB working set reproduced the
application-cache result: 71.68 to 19.21 µs, a paired 73.23% reduction
versus 73.27% previously. The larger cache still consumed about 60.5 GiB
of additional process RSS.

The 8 GiB 9950X replication needs a cache-regime qualification on 5.0.1.
Its primary 512 MiB / 12 GiB comparison had a 76.65% paired p99 gain
(A/A 18.21%), but two of the 20 processes accounted for disk reads:
24,784,896 bytes in one A/A control and 4,096 bytes in a large-cache
A/B arm. It failed the zero-read rule. In a separate confirmation we
explicitly read all 8,615,640,765 bytes of the same 33 SSTs before any
timed process, verified 100% file residency, and used independent seeds.
That run had zero process disk-read bytes throughout the timed probes
in every arm. Median p99 was 44.76 / 11.03 µs, paired gain 75.36%,
A/A 10.75%, five pairs favorable. For a cache-fit recommendation,
verify residency **and** measured disk bytes; configuring a large cache
or performing a partial warm-up does not establish the intended regime.

**SST compression.** The encoded-space gains remained 82.33% for patterned
values and 2.16% for high entropy. LZ4 shortened patterned sorted ingest
by 26.70–44.68% on the six Linux paths; M5 ingest took 35.27% longer.
The earlier study observed longer patterned-data random-read p99 medians
on all seven paths. On 5.0.1 the 5900X dual-drive and EPYC single-drive
p99 comparisons fell inside their control spread. Five stacks showed a repeatable
penalty with MMDB's block cache disabled. EPYC dual Btrfs newly cleared
that loss screen (57.54% longer p99 versus 51.80% A/A).

**WAL volume selection.** The 5900X dual-drive p99 remained slower
than single WD (+77.30% duration, 10.74% A/A). The EPYC result needs a
qualification change: the dual Intel volume had the lower p99 in every
pair, but its 65.23% median reduction is inside the 84.47% A/A spread
of the single-drive control. The historical reduction of 63.58% had
cleared a 0.86% control band. On the 9950X, ZFS lowered sync-put p99 by
48.26% relative to ext4 (6.91% A/A), down from 63.52% in the previous
trial. Both periods used two different drives of the same model. These
are storage-stack comparisons; a cross-session magnitude change alone
does not isolate a change of engine version.

The 9950X prewarmed uncompressed scan took 18.40 ms on ext4 and 26.89 ms
on ZFS (44.90% paired increase, A/A 8.02%, previously 34.36%). The
random-read p99 also rose by a paired 6.96% on ZFS (A/A 4.47%), crossing
the 5% loss guard on this fixture. The previous estimate of a 0.40%
increase was inside its A/A spread.

### Shard settings on 5.0.1

Five matched A/B pairs compare two immutable slots / 10 MiB with four slots / 6 MiB at the same nominal 30 MiB per DB. Positive gains mean higher throughput or lower p99. The latency columns also show the maximum independent A/A variation. The final two columns measure logical bytes over the elapsed time from the original start through forced flush/compaction.

| Stack | Hot saturation gain | All-active saturation gain | Hot put p99 gain / A/A | All-active put p99 gain / A/A | Hot through compact | All-active through compact |
|---|---:|---:|---:|---:|---:|---:|
| M5 / APFS | +38.48% | +39.05% | +58.33% / 355.57% | +35.63% / 457.68% | +38.82% | +36.42% |
| 5900X / dual Btrfs | +44.64% | +15.67% | -5.38% / 7.42% | -13.72% / 17.66% | +41.70% | +14.38% |
| 5900X / single Btrfs | +40.11% | +13.96% | -6.20% / 1.97% | +24.16% / 42.81% | +39.88% | +13.73% |
| EPYC / dual Btrfs | +43.53% | +17.03% | -0.22% / 14.24% | +23.79% / 31.30% | +41.61% | +15.84% |
| EPYC / single Btrfs | +35.65% | +18.64% | +1.77% / 3.78% | +8.15% / 25.74% | +34.46% | +17.82% |
| 9950X / ext4 | +37.29% | +14.10% | +5.30% / 24.92% | +61.76% / 35.55% | +35.39% | +16.17% |
| 9950X / ZFS | +35.61% | +10.30% | -318.59% / 41.20% | +9.94% / 20.78% | +33.01% | +11.74% |

All 280 fixed-rate arms passed the workload and backlog checks. All 560 arms reached zero L0 files and zero pending immutables at the explicit terminal sample; peak foreground-window RSS stayed below 1 GiB (maximum 611.60 MiB). All-active saturation now cleared the benefit screen on **seven of seven** stacks, including EPYC single Btrfs, previously inside control variation. The gain after full maintenance was 11.74–36.42% and passed on all seven. Hot-shard saturation cleared the foreground screen on **six of seven**. On M5 the 38.48% gain was inside 41.35% A/A, unlike the previous qualified +42.53%; all five paired gains were positive.

The fixed-work all-active arms still ended the foreground window with median 0 versus 45 L0 files. Compaction output per logical byte was 0.634 versus 0.324 before maintenance, and **0.714 versus 0.404 after both configurations reached the common endpoint** on all seven paths. The output gap persisted after the prescribed drain. This is a finite-trace engine counter, not a measurement of device lifetime write amplification.

The hot-shard p99 loss remained reproducible on 9950X/ZFS: 0.398 to 1.665 ms, a 318.59% paired increase (41.20% A/A). On the same host/ext4 the old 16.79% loss did not reproduce: the new +5.30% gain was inside 24.92% A/A. The 5900X single WD still exceeded the loss guard (6.20% slower, A/A 1.97%); the 5.38% loss on dual Btrfs was inside 7.42% A/A. **Zero of seven stacks passed the combined portability screen.** Use the larger buffers as an explicit throughput choice and retain a separate hot-shard latency control.

### Evidence and verification

The archived retest contains **1,510 formal physical observations**, including 160 independent old/new sentinel processes. Reusing storage controls eliminated 90 duplicated executions, and the warm-read selection omitted 60 redundant secondary-volume processes. The comparable tuning matrix used 1,350 executions versus 1,500 before the added version sentinels. There are separately labelled 40 CPU-mask confirmation processes, 20 explicitly prewarmed capacity confirmations and four rejected ZFS device pilots. All four hosts passed their functional gates, including the revised shared-cache tests. The retest archive contains 136 files and their SHA-256 manifest. [Raw retest archive](retest-observations.tar.gz) · [Archive digests](retest-5.0.1.json) · [Reproduce both summaries and their differences](reproduce.md#optimized-501-retest)

## Historical campaign at `828e47a`

This campaign builds revision `828e47a` on four physical machines and
exercises seven storage configurations. The measurement plan specifies the
build, fixtures, placement, statistical thresholds and resource guards.
The Linux hosts use separate benchmark clones and dedicated scratch
directories. [Protocol](protocol.md) · [Frozen inputs](freeze.json) ·
[Same-host disk comparisons](volume-protocol.md) · [Reproduction](reproduce.md)

The archive contains 1,500 formal observations and the separate
diagnostic trials. The final summaries were regenerated from an extraction
of the published archive. All seven paths had the same small-SST SHA-256,
and the four accepted scattered-device cells had the same large-SST
SHA-256. For each payload, seed and codec, the encoded SST byte counts
matched across all seven paths. Each physical host passed the grouped-read
and crash-recovery suites (7 and 27 cases), followed by the final library
and integration checks (435 and 74 cases). [Raw observations and receipts](observations.tar.gz) ·
[Coverage and archive digest](freeze.json) · [Regenerate detailed tables and per-stack decisions](reproduce.md#recalculate-the-results)

### Which configuration to use

| Workload or resource constraint | Starting configuration |
|---|---|
| Many small shards; protect foreground write p99 | At the tested 30 MiB nominal allowance per handle, use four immutable slots and a 6 MiB write buffer as the reference. Check the hot shard explicitly even if most traffic spreads across shards. |
| Same allowance; sustained bulk throughput takes priority | Compare two slots and a 10 MiB buffer against that reference. This geometry trades fewer, larger flushes for different queue depth, tail latency and ending L0 debt. Apply it explicitly to the intended workload. |
| Random reads of a frequently reused working set, with RSS headroom | Size the MMDB cache to retain the decoded hot blocks. Check its weighted charge and hit/miss probes, process RSS and file-cache residency independently. Use a single shared pool when multiple DBs share a budget. |
| Compressible data, with encoded size or Linux sorted-import speed as the objective | Try LZ4 SST compression and measure decompression on cache misses. Leave high-entropy, latency-sensitive data uncompressed. Keep filesystem compression and MMDB compression separate in comparisons. |
| Nearby keys requested together | Use one snapshot and bounded grouped pinned reads. A commodity CPU and a server CPU both benefited; the scattered device-read workloads yielded much smaller gains. |
| A durable acknowledgement for every write | Retain `WalSync` and select the volume using measured sync latency. The two Btrfs single-to-RAID comparisons pointed in opposite directions. |

These are explicit resource choices. A library-wide CPU/OS/filesystem
selector would conceal the workload-dependent tail regressions. The
sharded comparison held the write-buffer allowance at 30 MiB; it did not
compare the larger general-purpose `DbOptions::default()` configuration.

**Library-preset decision.** Before the first 80-arm shard matrix finished,
we set a portability check for promoting an optional 30 MiB shard-buffer
recipe. Count independent physical hosts, with each host contributing at
most one qualifying stack. Require at least two hosts where the candidate
clears the throughput screen under both placements. In that recommended
scope all fixed offers must be met, peaks must fit within 1 GiB and the
protected foreground p99 must fit the 5% guard. Treat a favorable median
with a >5% regression in individual pairs inside a wide A/A band as an
uncertain guard. Report other stacks separately. A workload can use the
existing setters to select a geometry with a measured local tradeoff.

The runnable example starts with a shared 256 MiB-per-store cache and
the caller's fixed prefix, then applies the two measured buffer layouts
to separate local DBs. Run `cargo +stable run --example shard_buffers`
to exercise both through batch writes, a durability fence and grouped
reads. [Rust configuration example](../../../examples/shard_buffers.rs)

### Sixteen write-active shards

Both arms have one active memtable per DB. The reference permits four frozen buffers at 6 MiB each; the candidate has two frozen slots and a 10 MiB buffer. All 16 DBs share one block cache, with 256 MiB allowed in each weighted store. A paced run offers 200 requests/s per stream on the hot placement or 320 per stream with all shards active. A separate eight-second window measures saturation.

Each percentage cell gives **paired gain / largest absolute A/A change**. A positive gain means higher throughput or lower p99. The peak column is the highest process RSS over both profiles and load types, including A/A.

| Stack | Hot saturation throughput | All-active saturation throughput | Hot foreground put p99 | All-active foreground put p99 | Peak RSS, MiB |
|---|---:|---:|---:|---:|---:|
| M5 / APFS | +42.53% / 35.31% | +43.87% / 40.44% | +16.72% / 406.26% | +38.32% / 78.01% | 299.5 |
| 5900X / dual Btrfs | +42.38% / 9.27% | +14.10% / 4.58% | -5.21% / 0.53% | -5.70% / 14.78% | 377.8 |
| 5900X / single Btrfs | +35.96% / 11.69% | +15.07% / 1.89% | -5.33% / 1.61% | +17.96% / 20.23% | 383.5 |
| EPYC / dual Btrfs | +36.84% / 5.63% | +18.23% / 2.35% | -4.00% / 10.80% | +22.86% / 15.01% | 362.4 |
| EPYC / single Btrfs | +29.98% / 13.75% | +17.42% / 19.94% | +47.38% / 72.53% | -4.56% / 378.00% | 442.3 |
| 9950X / ext4 | +44.63% / 3.79% | +13.84% / 6.13% | -16.79% / 4.45% | +69.92% / 67.32% | 615.5 |
| 9950X / ZFS | +35.51% / 3.38% | +10.19% / 3.62% | -335.08% / 10.23% | +13.61% / 10.85% | 575.5 |


All 280 paced observations completed the declared count and passed the
backlog checks. All 280 saturation observations completed their issued
work. Each stack stayed below the 1 GiB process ceiling. Hot-shard
saturation favored the larger buffers in all 35 A/B pairs and cleared
the 10% screen and A/A spread on every stack. All-active throughput also
rose in all 35 A/B pairs. It cleared both screens on six stacks; the
EPYC single-drive gain of 17.42% was inside its 19.94% A/A spread.

Tail latency supplies a different answer. On the 9950X/ZFS hot shard,
q2/10 increased median run p99 from 0.366 to 1.592 ms, a paired
335.08% increase. The same stack gained 13.61% on all-active put p99.
Both 5900X volumes exceeded the 5% hot-shard put guard with q2/10. On
the M5 hot shard only three of five p99 pairs favored q2/10, and the
16.72% median gain was inside a 406.26% control spread. The earlier
M5 study's latency conclusion cannot be transferred to this engine
revision and offered load.

For matched all-active paced work, the median ending L0 file count summed
over the 16 DBs was zero for the reference and 45 for the candidate on
every stack. Median compaction output per logical byte was respectively
0.634 and 0.324. These counts were sampled at the end of the timed
window. They describe engine work and remaining files; an eight-second
saturation gain does not establish steady-state lifetime write amplification.

**Preset decision.** Zero of seven stacks passed the combined
throughput, RSS and latency screen. Keep the two buffer layouts explicit
in deployment code. Choose the larger buffer for saturated throughput
when the measured tail tradeoff and remaining L0 files fit the application.
Use the four-slot reference to protect the hot-shard latency budget and
compare on the target volume. [Recalculate per-stack guards and paired gains](reproduce.md#recalculate-the-results)

### Single-snapshot reads

These are median per-process p99s for a complete 32-key clustered request
with one warm SST. The release probe enables `test-utils` for SST/block
attribution; the same executable runs both methods on each machine.
Each method returned all 32 values with matching pin
lifetimes. Each row includes five independent scalar/scalar and five
scalar/grouped pairs, 300,000 requests per process. The gain is the median
of within-pair reductions. A/A is the maximum absolute control change.

| Host and volume | Scalar p99, µs | Grouped p99, µs | Paired gain | A/A |
|---|---:|---:|---:|---:|
| M5 / APFS | 10.92 | 7.88 | 27.86% | 2.64% |
| 5900X / Btrfs dual NVMe | 20.40 | 14.45 | 28.96% | 0.98% |
| 5900X / Btrfs single NVMe | 20.46 | 14.42 | 30.01% | 1.73% |
| EPYC / Btrfs dual NVMe | 23.44 | 17.25 | 26.66% | 1.02% |
| EPYC / Btrfs single NVMe | 23.39 | 17.30 | 26.12% | 18.65% |
| 9950X / ext4 | 17.64 | 10.28 | 41.86% | 1.32% |
| 9950X / ZFS | 17.53 | 10.19 | 41.85% | 0.81% |

Use `get_many_pinned_with_options` when the application already needs
nearby keys together. All five comparisons favored grouping on every
host and volume. Engine block acquisitions fell from 64 per request to
about 3.07. These warm-cache observations measure CPU and engine behavior;
changing the scratch volume on a given host barely moved the median. A
CPU's advertised boost frequency alone cannot predict the absolute p99.

For 32 scattered keys on the large device fixture, every completed
10,000-request arm on the four eligible paths met the process I/O threshold
on 100% of requests. Paired p99 gains were 4.74% on APFS (3.88% A/A),
6.62% on 5900X RAID (10.06% A/A), 6.05% on EPYC RAID (1.33% A/A) and
3.01% on 9950X ext4 (8.33% A/A). None reached the 10% materiality screen.
Use these absolute request timings as storage-stack observations. The
locality of the requested keys determines the amount of reusable work.

**ZFS cache classification.** In two independent 10,000-request seeds
for both methods, the Linux process counters qualified every request as
a read of at least 4 KiB. The physical NVMe read counter increased by
zero bytes in each of the four pilots, with zero ARC demand misses.
The ZFS device cell therefore has no physical-read p99. The primary
OS-prewarmed clustered trace showed 11,892,235 ARC demand hits, zero
demand misses, and 532,480 bytes of whole-device traffic over the series.
A separate 20-process replication observed zero physical reads and zero
ARC demand misses in each process. The primary timing remains the
ARC-prewarmed observation; the replication supplies attribution evidence.

### SST compression

LZ4 reduced the encoded SST size by **82.33%** on the repeated-pattern
values and **2.16%** on high-entropy values on all seven paths. Across the
seven high-entropy runs the median paired ingest duration increased by
33.12–88.18% and the observed prewarmed random-read p99 increased by
42.14–63.84%. On EPYC single Btrfs, the 45.10% high-entropy p99
increase was inside the 99.11% A/A spread.
The workload builds 262,144 sorted 256-byte values and reads without
engine block-cache admission. Readers which already hold the decoded
blocks have a different cost.

| Repeated-pattern values | Paired sorted-ingest gain with LZ4 | Paired scan gain | Paired random-read p99 gain |
|---|---:|---:|---:|
| M5 / APFS | −28.50% | −12.79% | −57.26% |
| 5900X / dual Btrfs | +40.74% | −7.32% | −41.13% |
| 5900X / single Btrfs | +39.93% | −8.13% | −40.46% |
| EPYC / dual Btrfs | +37.61% | −13.95% | −30.80% |
| EPYC / single Btrfs | +31.60% | −10.87% | −38.01% |
| 9950X / ext4 | +28.42% | −5.23% | −39.75% |
| 9950X / ZFS | +23.41% | −12.57% | −46.46% |

On the compressible Linux fixtures, shrinking the files reduced sorted
ingest time; the M5/APFS arm paid extra ingest time. Random-read p99
increased in the median on all seven. On EPYC dual Btrfs the 30.80%
estimate lies inside the 43.79% A/A spread. Choose
`with_compression(CompressionType::Lz4)` when the encoded-size saving
on representative data warrants the read cost. Keep uncompressed blocks
for high-entropy values or for a latency-sensitive workload which must
repeatedly load and decode blocks. The scratch filesystems used here had
filesystem compression disabled; layering two codecs needs its own test.

Space percentages use the sum of logical lengths of the finished SST
files. On ZFS, immediate post-install `st_blocks` for the two SSTs was
only 1,024 allocated bytes in these trials. A separately checked,
long-lived 38,155,840-byte SST reported 38,298,112 allocated bytes. The
immediate allocation counter cannot establish the physical space saving.

### Durable small writes and storage selection

The disk comparison alternated two scratch volumes on the same host. Each
process issued 10,000 individual 128-byte `WalSync` puts, measured
acknowledgement, issued a final fence and exited without closing. A
new reader recovered the WAL and verified every row. Both arms use the
same durability policy.

| Reference → candidate volume | Reference put p99 | Candidate put p99 | Paired latency reduction | A/A |
|---|---:|---:|---:|---:|
| 5900X: single WD → two Kingston / Btrfs RAID0 | 0.753 ms | 1.302 ms | −73.85% | 10.10% |
| EPYC: single aigo → two Intel / Btrfs RAID0 | 2.121 ms | 0.770 ms | +63.58% | 0.86% |
| 9950X: ext4 → ZFS on a second same-model Intel NVMe | 94.92 µs | 37.58 µs | +63.52% | 12.08% |

All five A/B pairs agreed in each case. These are three different
storage choices for latency-sensitive synchronous commits. The two
Btrfs machines favored opposite disk layouts. On the 9950X, ZFS
reduced synchronous write latency but increased the prewarmed scan time
from 19.06 to 25.75 ms (+34.36% paired duration, 4.41% A/A spread).
Prewarmed uncompressed point-read differences stayed within A/A for
all three volume pairs. The ext4/ZFS comparison uses different physical
drives with different volume roles. [Matched volume protocol](volume-protocol.md)

Keep `WalSync` when each acknowledged write must survive power loss.
When an application can acknowledge durability at a group boundary,
MMDB also supports `WalAsync` followed by `sync_wal()`. The separate
WAL-policy series records the per-call p99 and final fence time for
both routes. Its async acknowledgements precede that durable fence.

Each stack also reopened a separate, identical 17,203,612-byte residual
WAL. Every process verified all 100,000 rows after timing the open call.
The table gives the median of five independent runs per open mode; the
pool was 64 MiB, the writable buffer 6 MiB and immutable depth four.

| Stack | Read-only open, ms | Writable open, ms |
|---|---:|---:|
| M5 / APFS | 35.72 | 69.28 |
| 5900X / dual Btrfs | 61.05 | 90.95 |
| 5900X / single Btrfs | 59.31 | 90.15 |
| EPYC / dual Btrfs | 52.46 | 78.37 |
| EPYC / single Btrfs | 56.69 | 105.21 |
| 9950X / ext4 | 18.71 | 32.02 |
| 9950X / ZFS | 20.40 | 30.82 |

An initial M5 run used a 256 MiB pool. It is retained as a diagnostic;
the table and the cross-host validator use the new 64 MiB run.

### Paying for an application-resident working set

On EPYC, the residency samples immediately before and after the random
probes both showed all 64.19 GiB of SST files in the OS page cache.
Five A/A and five A/B pairs
compared 4 GiB and 96 GiB configured per MMDB cache store. Actual warm
weighted cache charge rose from approximately 4.00 to 64.13 GiB. Median
end-of-run process RSS rose from 4.13 to 64.66 GiB. The median cache
probe hit fraction rose from 52.50% to 99.9985%. Each process verified
100,000 random 8 KiB values and read zero bytes from storage during
those probes.

The larger application cache reduced median run p99 from 70.71 to
18.97 µs. The within-pair median reduction was 73.27% (72.83–73.48%,
five favorable pairs, 0.84% maximum A/A). This makes the resource cost
explicit: avoiding repeated file/block processing for this workload
consumed about 60.54 GiB of additional process RSS even with the kernel
already holding the file data. [Capacity thresholds and driver](../cache-capacity/README.md) ·
[Scaled replication design](capacity-replication.md)

On the 9950X/ext4 host, scaling the same value working set to 8 GiB and
the two configured capacities to 512 MiB and 12 GiB reproduced the
effect. The larger arm's actual warm charge was 7.99 GiB, with 8.09 GiB
end RSS versus 0.56 GiB in the smaller arm. Median run p99 fell from
50.26 to 11.29 µs, a paired median reduction of 78.56% (73.99–87.12%,
5/5, A/A 19.96%). Every arm had 100% SST residency and zero process
disk-read bytes during measurement.

This supports a **working-set-fit** recommendation: for repeated random
reads, give MMDB enough cache to keep the decoded hot blocks when the
process has the measured RSS headroom. Check deltas of
`stats.block_cache_hits` and `stats.block_cache_misses`, read the actual
pool charge with `block_cache_pool().usage()`, and monitor host memory
as well. On ZFS, include the independently managed ARC in the host
budget. The shard matrix's 1 GiB ceiling counts process RSS only.
The trials compare two endpoints; neither identifies the minimum
capacity required to obtain the benefit.

```rust
use mmdb::{BlockCachePool, DbOptions};
use std::sync::Arc;

// Supply a measured budget for each evictable store.
let per_store_bytes = 12_u64 << 30;
let private = DbOptions::default().with_block_cache_capacity(per_store_bytes);

// For multiple handles sharing that budget, create the pool once.
let pool = Arc::new(BlockCachePool::new(per_store_bytes));
let shared = DbOptions::default().with_block_cache(Some(Arc::clone(&pool)));
```

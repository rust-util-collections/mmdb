# Engine workloads on Apple M5

Each arm ran as a fresh release-build process on the local APFS volume,
under the native scheduler. The main production control used `d5569da`.
The prefix-cut and writer-replay sections identify their earlier source
revisions. The ordinary and instrumented drivers share Darwin process
counters. The main paired comparisons use five seeds. At each seed, two processes repeat
the control (A/A) and two other processes run the control and candidate
(A/B). Process order alternates with the seed. Tables report the median
within-pair change and the largest absolute A/A change. A positive gain
means lower latency or higher throughput. [Host configuration](README.md)

## Reopen after a synced WAL

The fixture writer used a 64 MiB write buffer, synced the WAL, and exited
before closing the DB. The parent measured the open call, sampled memory
immediately afterwards, then verified every recovered value. Five fresh
processes exercised each of six writable geometries and one read-only
geometry at both fixture sizes. All 70 completed.

| Rows | Residual WAL bytes | Writable open, median range across geometries | Read-only open, median at depth 4 / 6 MiB |
|---:|---:|---:|---:|
| 10,000 | 1,720,364 | 15.528–16.670 ms | 3.607 ms |
| 100,000 | 17,203,612 | 68.529–72.614 ms | 36.159 ms |

At 100,000 rows and immutable depth four, the 6 MiB writable buffer
reached a maximum of 13.91 MiB peak RSS immediately after open. The
16 MiB buffer reached 24.30 MiB. Scanning every row subsequently raised
both process peaks. [Raw reopen trials](engine/reopen.jsonl.gz) ·
[Timing and RSS by geometry](engine/reopen.csv)

## Native WAL fence on a single hot prefix

One foreground stream overwrote a single prefix, issuing either a
synchronous delete of the old sentinel key or a native `DB::sync_wal()`
after every 256 puts. Both arms used the production serial L0 merge and
the same cold-row attribution counters. Each run served 16,384 reads and
16,384 writes against 24 starting L1 files and verified both the snapshot
and the reopened DB.

With the sentinel, compaction consumed 2,004,219 bytes of original cold
SSTs in every run. The native fence retained all those files and emitted
zero cold rows. Median compaction output in the independent A/B arms fell
from 51,277,277 to 11,238,105 bytes (78.08%, A/A 5.27%). In this fixture
the cold keys sort between the sentinel and the hot prefix. The counter
measures generated SST file lengths. [Raw fence trials](engine/l0-fence-1-5.jsonl.gz) ·
[Paired counters](engine/l0-fence-1-5.csv)

## Two-range L0 compaction

The two-hot-prefix fixture offered 65,536 reads and writes per class at
20,000/s per class. The control and candidate each used one background
coordinator. The prototype split eligible L0 jobs across two build ranges.
Each arm completed 79 rewriting L0 jobs, peaked at one or two simultaneous
ranges respectively, and closed with one L0 file. Every process checked
the rows visible through the held snapshot and again after writable reopen.

| Metric | Serial median | Two-range median | Paired gain | Largest A/A change |
|---|---:|---:|---:|---:|
| Foreground stall ms / logical GiB | 183,922 | 135,061 | +28.15% | 11.45% |
| Read arrival-to-ack p99 | 20.125 µs | 21.750 µs | −8.34% | 2.72% |
| Write arrival-to-ack p99 | 8.726 s | 8.088 s | +8.09% | 9.66% |

All five comparisons improved stall time and exceeded the 20% target on
the paired median. All five worsened read p99. The 8.34% regression
exceeded both the 5% guard and control variation. Production keeps serial
L0 compaction. The large write tail includes accumulated backlog at the
deliberately high offer used to create L0 pressure.
[Raw parallel trials](engine/l0-parallel-1-5.jsonl.gz) ·
[Paired compaction and latency counters](engine/l0-parallel-1-5.csv)

### Prefix cuts during flush

At `5528950`, adding one SST cut at an eight-byte prefix boundary doubled
the installed file count from 40 to 80 across 40 flushes in each A/B pair.
Both variants still selected all 18 original cold-only L1 files
(1,717,902 bytes). The candidate emitted 75.0% more cold logical bytes and
increased foreground stall time per GiB by 201.0%. Each process verified the
snapshot, current rows, and rows after writable reopen. The paired output,
latency, and control ranges are in the recorded summary.
[M5 runs](secondary/prefix.csv) · [Paired metrics](secondary/summary.json) ·
[Original fixture and Linux results](../l0-profile/README.md#bounded-prefix-cuts-during-flush)

## Foreground writes during bulk updates

The mixed workload runs a 128-byte foreground put, a pinned get, and fresh
and held snapshot readers alongside 4,096-entry batches. With entropy
enabled and `WalAsync`, the calibrated offer was 12,415 scalar requests/s
per stream, with one bulk batch per 128 scalar requests. All five A/A and
five A/B pairs completed the paced offer and passed the backlog checks.
Moving the bulk writer to a separate DB reduced foreground put p99 in
five of five pairs. The paired median reduction was 43.18% (per-arm
medians 5.928 and 3.441 ms), inside the 55.57% A/A band. Both placements
completed approximately 106 MiB/s of logical writes at this offered load.
[Raw mixed trials and diagnostic stage samples](engine/mixed-async-1-5.jsonl.gz) ·
[Paired latency, throughput and RSS](engine/mixed-async-1-5.csv)

The short-group handoff prototype keeps small writer groups inline and
skips a wakeup when an uncontended leader serves itself. An extended
same-DB trial issued 100,000 foreground requests per scalar stream and
782 bulk batches at the same 12,415/s offer. Every A/A and A/B pair
passed the backlog check. The paired median foreground put p99 gain was
0.25%, with a range of −13.41% to +17.48%. Three pairs favored the
prototype. The predeclared gain target was 15% and A/A varied by 12.38%.
The existing serial group commit remains the production choice.
[Raw handoff trials](engine/handoff-async-1-5.jsonl.gz) ·
[Paired handoff metrics](engine/handoff-async-1-5.csv)

### Writer-stage replay

At `7b46443`, two issuers submitted 10,000 scalar puts and 79 batches of
4,096 entries per run. Five warmed traces alternated with tracing-disabled
controls. Serial replay reproduced every acknowledgement. The model allowed
one group's WAL work to overlap the previous group's memtable insertion
while preserving queue order and WAL-generation boundaries. With zero extra
handoff cost, the median modeled reduction in small-put p99 was 0.00%
(range 0.00–2.00%), below the 15% threshold in every trace.
[Trace measurements](secondary/writer/summary.jsonl) · [Per-trace replay results](secondary/summary.json) ·
[Model and Linux measurements](../engine-profile/writer-trace/README.md)

## Sixteen shards under a 1 GiB budget

The equal-allowance comparison opens 16 DBs sharing a block cache whose
segmented and overflow stores each have a 256 MiB capacity. Depth four
with 6 MiB buffers and depth two with 10 MiB buffers both allow a
nominal 30 MiB per DB for its active and queued buffers. Each DB starts
with 32,768 preloaded rows. A foreground writer and reader use shard zero.
The 16 batch writers either share shard zero (hot) or each use a different
DB (all active).

At the fixed offer, each process executes 20,000 foreground puts,
20,000 reads, and 320,000 batches of 32 entries. Each process therefore
submits 2,880,000 foreground and 2,785,280,000 batch logical write bytes
including keys. Saturation instead uses an eight-second shared deadline.
The summarizer checks completed batches, logical bytes, the shard and
issuer counts and the outstanding work in the paced arms. The M5 rates
were 205 requests/s per stream hot and 328/s all-active. Both represent
70% of a paced control rate verified in the corresponding placement.

All 40 paced processes completed the exact stream and passed the backlog
checks. The 40 saturation processes completed every foreground request
issued during their shared eight-second window. Each
placement and load had five independent A/A and five A/B pairs. Six
calibration runs preceded those 80 comparison processes.

| Placement and metric | Depth 4 / 6 MiB | Depth 2 / 10 MiB | Paired gain | Largest A/A change | Pairs favoring depth two |
|---|---:|---:|---:|---:|---:|
| Hot, paced foreground put p99 | 5.918 ms | 5.274 ms | +10.89% | 7.59% | 5/5 |
| Hot, saturation logical write throughput | 83.54 MiB/s | 100.05 MiB/s | +35.06% | 9.77% | 5/5 |
| All active, paced foreground put p99 | 20.587 ms | 13.174 ms | +36.71% | 75.82% | 4/5 |
| All active, saturation logical write throughput | 112.26 MiB/s | 141.50 MiB/s | +26.45% | 8.03% | 5/5 |

The maximum process peak RSS across both loads was 98.78 / 93.22 MiB
for the two hot-shard profiles, and 517.86 / 465.17 MiB all-active.
All arms fit under 1 GiB. Median saturation endpoints for depth four and
depth two were 25 and 23 L0 files hot, with one immutable generation in
either case. All-active endpoints were 68 L0 files and 48 immutables at
depth four, and 64 files and 16 immutables at depth two. During the
timed windows, median compaction output bytes per logical byte fell from
3.564 to 2.599 hot and from 0.263 to 0.181 all-active.

For this M5 workload and its 30 MiB nominal allowance, depth two with a
10 MiB buffer increased saturation throughput in both placements and
reduced foreground p99 on the hot shard beyond the measured control
spread. All-active p99 gains ranged from −15.74% to +43.69% and their
median stayed inside the 75.82% control band. The x86_64 longer hot trial
measured a 5.60% p99 regression with this geometry. Shard geometry
therefore remains a caller decision based on its hardware and traffic.
[M5 raw memory trials](engine/memory-all-1-5.jsonl.gz) ·
[Paired throughput, tails, RSS and ending debt](engine/memory-all-1-5.csv) ·
[x86_64 memory trial](../engine-profile/README.md)

## Replay

Build a release-profile `engine_profile` from the control source with the
Darwin counter module included. Build it again with `bench-metrics` to
capture the diagnostic stage sample. Build `l0_profile` with
`bench-metrics`. For an experimental arm, apply the
[short-group writer patch](../engine-profile/short-group.patch) or the
[two-range L0 patch](../l0-profile/two-range.patch) to its own copy of
the same source before building. Give each source tree a separate Cargo
target directory. The root dev-dependency enables `test-utils` in these
bench executables. The packets record SHA-256 digests of the tested binaries
and patches. [Native build provenance](build-provenance.json) ·
[Build settings](../engine-profile/README.md)

For reopen, run the engine matrix with `--cpus native --rounds 5 --seed
20261126`. For the paired benchmarks, the replay script reads the exact
settings, seeds and arm order from the selected packet. Run one suite at
a time on an idle machine. For example, set `L0_CONTROL` to the production
`l0_profile` executable and `SCRATCH` to a directory on the volume under
test. Run this command from the repository root.

```sh
python3 -B docs/benchmarks/apple-m5/replay_engine.py \
  docs/benchmarks/apple-m5/engine/l0-fence-1-5.jsonl.gz fence-repeat.jsonl.gz \
  --tmpdir "$SCRATCH" --binary l0 "$L0_CONTROL"
python3 -B docs/benchmarks/apple-m5/summarize_engine.py fence-repeat.jsonl.gz
```

Engine profiles report Darwin task resident bytes and `getrusage`
high-water resident bytes, converted to KiB. Linux uses
`/proc/self/status` and the published Linux matrix pins CPUs. L0's
in-process RSS observer reads Linux procfs and omits its RSS fields on
this Mac. Its file-length, compaction, stall and latency counters are
available on both systems. Use within-host comparisons for these trials.

### Reproduce the prefix-cut and writer-replay trials

Run from the repository root on macOS with Python 3.12 or later. The
driver restores the two original revisions, applies the patches, and builds
each arm with the shared native benchmark lock and `bench-metrics`. The
prefix candidate runs both boundary tests before timing. [Build and host record](secondary/metadata.json) ·
[Benchmark lock](bench.Cargo.lock.gz) · [Driver](secondary/reproduce.py)

```sh
area=target/native-m5-secondary
driver=docs/benchmarks/apple-m5/secondary/reproduce.py
python3 -B "$driver" prepare "$area"
python3 -B "$driver" build "$area"
python3 -B "$driver" run "$area"
python3 -B docs/benchmarks/apple-m5/secondary/analyze.py "$area/results"
```

To verify the archived measurements directly, run
`python3 -B docs/benchmarks/apple-m5/secondary/analyze.py docs/benchmarks/apple-m5/secondary`
from the repository root. This checks workload sizes, trace hashes,
scheduled arrivals, and reconstructed acknowledgements. [Analysis script](secondary/analyze.py)

# L0 compaction and prefix overlap experiment

**Decision on 2026-11-25** — Retain serial execution for L0→L1. A two-range
prototype reduced foreground L0 stall time in every paired trial. The
foreground read p99 guardrail remained inconclusive after extending both
arms to four times the original duration. The [prototype patch](two-range.patch)
contains the tested engine change and its regression test.

## Paired results

Each row below summarizes five baseline/candidate pairs on x86_64. The
third numeric column is the median of the five within-pair improvements.
Positive values favor the prototype. The last column is the largest
absolute within-pair change across five serial/serial runs. Independent
column medians can have a different ordering from the paired ratios.

| Operations per class | Metric | Serial median | Two-range median | Paired gain | A/A noise band |
|---:|---|---:|---:|---:|---:|
| 16,384 | Foreground L0 stall ms / logical GiB | 33,495.3 | 2,779.8 | 91.82% | 7.41% |
| 16,384 | Write arrival-to-ack p99, ms | 11.828 | 6.497 | 58.11% | 60.28% |
| 16,384 | Read arrival-to-ack p99, ms | 0.386 | 0.435 | −9.34% | 517.06% |
| 65,536 | Foreground L0 stall ms / logical GiB | 33,932.7 | 698.4 | 97.94% | 26.46% |
| 65,536 | Write arrival-to-ack p99, ms | 10.329 | 2.291 | 77.50% | 95.30% |
| 65,536 | Read arrival-to-ack p99, ms | 0.443 | 0.437 | −5.08% | 33.80% |

The longer run produced 159 flushes, 79 multiple-target L0 rewrites and
one residual L0 file after close in every A/B arm. All 79 candidate jobs
executed multiple ranges, with a per-DB peak of two concurrent ranges.
The serial arm wrote 223,218,911 bytes of compaction output, the candidate
223,211,621. Both completed all offered operations. The primary improvement
cleared 20% and its A/A band. The read p99 variation prevents certification
of the 5% control limit. A future experiment can isolate scheduler and
device interference and replay a production write/read mix before changing
engine defaults. [16,384-operation runs](runs-16384.csv) · [65,536-operation runs](runs-65536.csv) · [Acceptance criteria](../../proposals/mmdb-v5-execution-plan.md#4-measure-the-limiting-engine-operation)

**Native fence.** On the single-hot-prefix fixture, five serial/serial pairs
compared a synchronous deletion of key `[1]` with `DB::sync_wal()` every
256 puts. The native fence left all original cold SSTs in place. The
synthetic deletion widened the L0 span and consumed 2,004,219 bytes of
original cold-only SSTs in each run. Total newly written compaction output
fell from 51,277,277 to 11,238,105 bytes with the native fence. Logical cold
row bytes emitted during the entire workload fell from 38,993,920 to zero.
These numbers depend on placement of the cold keys between the historical
sentinel and the one hot prefix. The two-hot-prefix fixture still includes
cold L1 files in the gap even with a native fence.

## Workload and counters

- One fresh local DB per process, two levels, eight 8-byte prefixes, 1,024
  rows per prefix, 16-byte keys and 256-byte high-entropy values. Separate,
  sorted ingests create exactly 24 L1 files, with no L0 files before timing.
  The `gap` arm alternates between the lowest and highest prefixes. The
  `native` and `sentinel` arms use only the highest. A full scan checks the
  preload, and one live snapshot retains it through the writes.
- Write-buffer size 128 KiB, immutable limit 4, target SST size 128 KiB,
  L0 compaction/slowdown/stop counts 2/3/5, default private block cache and
  uncompressed SSTs. One background compaction coordinator; a single whole-L0
  claim limits the workload to one L0 job at once. Both binaries were confined
  to the same four physical cores (4–7) of a Ryzen 9 5900X under Linux
  7.0.0-44-generic. Temporary DB directories inherited the Btrfs `NOCOMPRESS`
  inode attribute. The NVMe filesystem otherwise has `compress=zstd:3`.
- One scalar reader and one writer used identical seeded scheduled arrivals
  at 20,000 operations/s per class. A serial saturation pilot completed
  about 28,800 puts/s. The write path uses WAL-enabled ordinary puts and
  the selected per-256 fence. A worker records both service time and
  scheduled arrival through acknowledgement; fence delay can therefore
  carry into subsequent writes. The full pre-update snapshot, live rows
  and writable-reopened rows are checked independently against the generated
  key order and expected version for every row.
- `l0_stall_ns` sums only actual slowdown sleep and stop-barrier/drain time
  on the writer thread. Immutable capacity waits are separate. The denominator
  is successful user puts times 272 logical bytes. `input_file_bytes` sums
  selected SST lengths. `output_file_bytes` counts newly created SST lengths.
  Both exclude trivial moves, and neither is a disk-sector traffic counter.
  Cold physical bytes refer to original cold-only file IDs retired during
  the run. A diagnostic block-property collector counts emitted cold user
  rows exactly in this fixed-width, cold-rows-never-updated fixture. The
  primary arms disable this collector. On the 16,384-operation `observer`
  pairs it added about 0.49% to SST output, and the absolute changes in
  latency varied across runs.
- Timed measurements end at foreground completion. Counters suffixed
  `after_workers` are captured after the workers join. Lifetime totals
  include the explicit final flush and work completed by background threads
  before close. Reported peak RSS is sampled after both workers finish.
  The run covers one active DB. It does not establish a cross-DB worker
  budget or prove multi-shard scheduling behavior.

## Reproduction

Start from the revision containing this driver, create a second worktree,
and apply the patch there. Compile each binary separately with the same
toolchain and features. The test below fails on the serial engine with
the prototype test added alone and passes after the split is enabled.
The test parks the output builder, installs a newer L0 file, and checks
multiple snapshots across a spanning range tombstone and two split-boundary
rewrites before writable reopen.

```sh
cargo +stable test --features bench-metrics --lib split_l0_preserves_snapshot_stripes_and_concurrent_flush_across_boundaries
cargo +stable test --features bench-metrics --lib compaction::leveled::tests
cargo +stable bench --features bench-metrics --bench l0_profile --no-run
```

Pass the reported executable paths to [replay.py](replay.py) on Linux.
Supply a scratch directory on the target filesystem. Each benchmark process
creates its own temporary DB under that directory and removes it on exit.

```sh
python3 replay.py --baseline "$serial_binary" --candidate "$candidate_binary" \
    --cpus 4,5,6,7 --tmpdir "$scratch" --output short.csv \
    --operations 16384 aa ab fence observer
python3 replay.py --baseline "$serial_binary" --candidate "$candidate_binary" \
    --cpus 4,5,6,7 --tmpdir "$scratch" --output extended.csv \
    --operations 65536 aa ab
```

A/B and B/A order alternates by pair. Both binaries use the same driver
and profile instrumentation. The comparison holds CPU allocation,
background coordinator count, compression, durability and data fixed.
The 16,384-operation collector A/B rows estimate the incremental cost of
cold attribution. Rustc was stable 1.100.0 in these runs. The benchmark
executables had SHA-256 hashes
`216b26a2bd4951e5c680a337474dd1c55754cd2102fcc2f18a37dc20606e0061`
(serial) and
`2fdb2e2261d1d8ad41b9b1f899c97f7c45b2d403e2bda6ff7fc25ad66525a9a7`
(two ranges). [Benchmark driver](../../../benches/l0_profile.rs)

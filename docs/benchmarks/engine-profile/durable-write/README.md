# Same-group WAL seal experiment

The control used MMDB `3b403ca5` (6.0.5). After a successful group sync,
the candidate passed the active WAL generation from `write_batch_group`
to `freeze_memtable`. Automatic rotation skipped the second sync only
for the same generation under continuous write leadership. Explicit flush
and close took the ordinary seal path.

## Decision

Keep the unconditional old-WAL seal. With this rule, `freeze_memtable`
synchronizes the outgoing WAL directly after taking ownership of its writer.
The candidate saved the expected sync. Its foreground p99 was higher in
four of six paired rounds. The largest absolute control/control change
was 38.16%, and the ordinary
builds failed the primary adoption criterion and the 2% service-p99 guard.
The performance trial ended at that decision. The x86_64 correctness
results below cover the candidate. AArch64 runtime behavior and the
remaining saturation and async guards were not tested in this trial.

| Ordinary-build metric | Control median | Candidate median | Paired candidate/control geometric mean | One-sided 95% upper ratio bound |
|---|---:|---:|---:|---:|
| Foreground put, arrival-to-ack p99 | 4.839 ms | 5.183 ms | 1.150 | 1.411 |
| Foreground put, native service p99 | 4.426 ms | 4.685 ms | 1.094 | 1.210 |

The medians summarize the six per-arm p99s. The ratios compare the two
binaries within each seed. The lower one-sided bound for arrival-to-ack
was 0.937, so these data also leave the size and direction of a repeatable
effect uncertain. All 24 ordinary runs completed their scheduled work.
All twelve A/B arms ended with one L0 file and zero immutable memtables.
Median drain times were 69.04 ms (control) and 68.47 ms (candidate).
[Original run records](runs.jsonl.gz) · [Recompute and verify](summarize.py)

## Measured rotation costs

The diagnostic build times the complete outgoing `WalWriter::sync_data()`
call, including its buffer flush. Each trace row associates this interval
with the native request that led the rotation.

| Baseline workload | Automatic rotations | Median seal | Maximum seal | Total seal / total service time of rotating leaders |
|---|---:|---:|---:|---:|
| 4 MiB buffer, small writes and bulk writes, 140 scalar writes/s | 19 | 3.25 µs | 3.418 ms | 7.595 / 200.593 ms |
| 64 MiB buffer, small writes and bulk writes, 140 scalar writes/s | 5 | 2.58 µs | 2.90 µs | 0.012 / 69.238 ms |
| 4 MiB buffer, one writer, 50,000 small writes at 400/s | 2 | 1.019 ms | 2.034 ms | 2.037 / 8.005 ms |

Three bulk requests accounted for 7.54 ms of the 4 MiB mixed arm's seal
time. The 64 MiB arm used one bulk batch per 32 scalar writes so several
rotations would occur. The 4 MiB mixed arm used one per 128. In the
candidate counter check, 2,500 small writes and 20 bulk batches at the
4 MiB setting produced five rotations and zero old-WAL seal calls.
Diagnostic timings are single-run observations.

The targeted file observer, called after a successful old-file data sync,
saw two syncs on the control and one on the candidate for the group that
triggered rotation. The candidate's old WAL bytes matched the sync-time
image, and both keys were visible on reopening the copied database. This
held for both new and standby WALs. A later async group's required seal
still latched an injected error after acknowledgment. On x86_64 Linux,
469 default library tests, 474 library tests with `bench-metrics`, all 28
crash-recovery tests and all 75 integration tests passed on the candidate.
Both all-target Clippy configurations passed with warnings denied.

## Fixture and comparison

The ordinary binaries ran serially on x86_64 Linux, AMD EPYC 9474F, CPUs
16–19, Rust 1.100.0. Each process created a fresh database under `/tmp`
on local NVMe Btrfs (`/dev/nvme2n1p2`, mount option `compress=zstd:3`).
Each arm used the same preload and deterministic request generator. The
engine's SST compression setting was the library default. The host was
shared. Other benchmark activity was observed on a different CPU set
and NVMe volume during the primary comparison.
[Engine profile](../README.md#workloads)

The primary workload uses a 4 MiB write buffer, two immutable slots and
`WalSync`. Per run it issues 10,000 scheduled scalar writes, 79 batches
of 4,096 entries, and 10,000 requests each for reads, fresh snapshots and
held snapshots on one DB. The 2,000-operation calibration sustained the
200/s offer. At 400/s it completed 347 scalar writes/s and had 206 still
outstanding at the last scheduled arrival. The primary rate was fixed at
140/s, 70% of the sustained offer. The separate saturation pilot used
independently unpaced streams.

For each of six seeds (2110–2115), use two independent control runs for
control/control variation and a further control and candidate for the
paired comparison. Alternate the order each seed. Measure the primary
endpoint from scheduled arrival to native acknowledgment of the scalar
write. Service time starts at native entry. Treat each complete round as
one observation. Apply a one-sided 95% Student interval to the mean of
the six paired log ratios, then exponentiate. A lower p99 is favorable.
The primary reduction must exceed the largest absolute control/control
variation. The protected p99 upper ratio bound is 1.02. The protected
saturation throughput lower bound is 0.98. Proceed to the remaining
guards only if the primary comparison supports adoption.
[Adoption criteria](../durable-write-plan.md#4-decide-on-ordinary-builds)

## Verify the recorded results

Each record contains the executable checksum, profile settings, exit
status and driver output. Run the following command from the repository
root to check the 24 ordinary runs and recompute the per-seed p99s, paired
bounds and per-worker seal counts from the archived measurements.

```sh
python3 -B docs/benchmarks/engine-profile/durable-write/summarize.py
```

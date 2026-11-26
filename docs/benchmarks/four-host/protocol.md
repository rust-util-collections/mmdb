# Four-host MMDB v5 tuning protocol

Engine revision: `828e47a1d019e3fd0854a3d9a0d98b72e873350c`.
Freeze date: 2026-11-26, before collecting the decision samples. The
preceding M5 and Ryzen reports are historical controls. Each decision below
uses fresh measurements of the frozen engine, including on Apple Silicon.

## Questions and experimental units

1. Does a shallower immutable queue with larger buffers improve a 16-shard
   writer at the same nominal memory allowance? Does the answer change
   between a single hot shard and all shards writing?
2. How much do nearby pinned reads benefit from grouping on different CPUs?
   How do OS-resident data, physical I/O and ZFS ARC hits affect that result?
3. When is LZ4 SST compression worthwhile for scans, random reads and
   on-disk space? What are the acknowledged cost of synchronous writes and
   the cost of reopening a residual WAL on each storage stack?
4. With a genuinely large, resident working set, does buying more MMDB
   block cache improve p99 relative to relying on the OS page cache?

| Physical host | Primary storage | Within-host storage comparison | CPU allocation |
|---|---|---|---|
| Apple M5, 16 GiB, macOS 27.2.1 | Internal SSD, case-sensitive APFS | One volume | Native scheduling |
| Ryzen 9 5900X, Ubuntu 26.04.1 / Linux 7.0 | Two Kingston KC3000 NVMe, Btrfs RAID0 (home) | Single WD NVMe, Btrfs (tmp) | CPU 4 for point reads, 4–7 for the engine |
| EPYC 9474F, Gentoo / Linux 6.18 | Two Intel SSDPF2KX076TZ NVMe, Btrfs RAID0 (home) | Single aigo P5000Z NVMe, Btrfs (root) | CPU 4 for point reads, 4–7 for the engine |
| Ryzen 9 9950X, Ubuntu 24.04.5 / Linux 6.8 | Single Intel SSDPF2KX019XZ NVMe, ZFS | A second SSD of the same model, ext4 | CPU 4 for point reads, 4,5,8,9 for the engine |

The Linux four-core sets comprise distinct physical cores, two in each
of two L3 groups. One experiment runs at a time on each host. Different
hosts may run concurrently. The 9950X pair holds CPU, kernel and SSD model
constant but changes physical drive and system-volume role. The Btrfs
pairs also change drive count and model. The four hosts give deployment
comparisons; none isolates the causal effect of a kernel version.

## Freeze, preflight and provenance

- Use a new clone and private output, build and scratch directories on
  each host. Record full commit, tree, source cleanliness, target triple,
  stable rustc and binary SHA-256. Use rustc 1.100.0, the same release
  settings and the [frozen Cargo lock](../apple-m5/bench.Cargo.lock.gz)
  (uncompressed SHA-256 `6a212e9b0094bdd134fac3a3cda14120a82869fa2eef27f25144420c7f9eb27a`).
  Keep any external measurement harness and its lock separately identified.
- Run the grouped-read and crash-recovery tests.
  Confirm actual mount, physical devices, free space, CPU topology,
  affinity, quotas and memory. Record compression and cache policy. On
  Btrfs mark only the empty scratch directory NOCOMP before creating
  stores. On the dedicated ZFS dataset use compression off, recordsize
  128 KiB, sync standard and ARC enabled. Preserve OS durability semantics.
- Preserve raw per-process observations, arm order, matched seeds, exact
  requested and completed work, and any failed qualification. Rebuild
  fixtures on the frozen engine. Keep previous-version results separate.

## Matrix and measurements

| Workload | Coverage and controls | Primary readouts |
|---|---|---|
| Grouped pinned reads | Warm and OS-prewarmed clustered 32-key requests on all seven paths. One 262,144-row SST, one snapshot; 300,000 warm and 10,000 OS-prewarmed requests per process. Scattered 32-key device trial on each primary path and both 9950X drives, 1,048,576 rows, 10,000 requests. | Full-request p99, block acquisitions, returned-pin bytes, verified storage-read coverage |
| Shard writer | All seven paths; 16 DBs, high-entropy values, WalAsync, 256 MiB per shared cache store. Control four frozen slots / 6 MiB, candidate two / 10 MiB: 30 MiB active-plus-frozen nominal per handle. Hot and all-active placements, separately at 200 and 320 offered requests/s per stream, 20,000 requests per stream. Independent eight-second saturation windows. | Foreground put and read p99, admitted logical MiB/s, peak process RSS, compaction bytes, remaining L0 files and immutables |
| SST storage options | All seven paths. On the same sorted 64 MiB value payload compare uncompressed and LZ4 SSTs, separately for deterministic repeated-pattern and high-entropy records. Reopen and verify every row. Engine block-cache admission disabled for OS-prewarmed scan and random-read comparisons. | Sorted-ingest time including durable publication, SST size, scan time, random-read p99, verification counts |
| WAL operations | Each path: 10,000 128-byte single puts under WalSync and under WalAsync with a final explicit fence, with separate fences and reopen checks; five fresh processes per mode. Also run the existing 100,000-row residual-WAL fixture with 64 MiB preparation buffer and 4 / 6 MiB open geometry, both read-only and writable, five times each. | Per-put p99, fence time, open time and peak RSS, successfully recovered rows; distinguish acknowledgement guarantees |
| Capacity | EPYC Btrfs RAID0. New 64 GiB value fixture (8,388,608 × 8 KiB). Four versus 96 GiB configured per cache store; 100,000 random point reads after warmup. Record effective weighted bytes, hits, user-process RSS/HWM, OS file residency, device reads and available memory. | Capacity-qualified random-read p99, hit rate and incremental RSS; report scope as a high-memory read workload |

The shard phases replay the same seed set and process ordering as the M5
protocol, with the new fixed common offers. For the 20,000-request paced
trace require 320,000 completed 32-entry batches, 2,785,280,000 logical
batch bytes, 2,880,000 small-put bytes and zero rejected requests.
Apply the existing [backlog check](../engine-profile/paced_load.py) to
each arm. Overloaded fixed-rate cells describe overload. A repeat at a
lower common rate, if needed, gets a separate label and includes all arms.

## Analysis rules fixed before timing

- For tuning decisions use five independent reference/reference (A/A)
  pairs and five reference/candidate (A/B) pairs, with matched seeds and
  alternating order in fresh processes. Compute p99 within each arm.
  Use the median of within-pair percentage gains, the largest absolute
  A/A change, and the number of favorable pairs. Absolute latency
  comparisons across hosts use matched work and offered load.
- Require at least four of five favorable pairs, a gain above control
  variation and a practically material improvement (10% for latency or
  throughput, 20% for SST space). Protect foreground p99 and throughput
  with a 5% regression guard. Report noisy protected metrics as uncertain.
  Distinguish saturated throughput from paced p99. Keep the 1 GiB
  **process RSS** ceiling for the shard matrix and report outstanding
  work. Account for ZFS ARC separately. For the capacity experiment also
  require all five latency gains positive, at least 48 GiB actually
  charged in the large arm, at most 5 GiB in the small arm, ≥99% file
  residency before and after random reads and zero measured disk reads.
- A device-latency claim requires ≥4 KiB of process-accounted disk reads
  for ≥95% of requests in **every arm**, plus host/device corroboration
  on ZFS. Record ARC hits/misses and vdev or physical sectors before and
  after trials. If two independent pilot seeds for both methods show no
  physical reads, retain the pilot as an ARC observation and stop that
  device cell. Prewarmed ZFS reads with nonzero process counters but zero
  device traffic have their own category.
- Recommend explicit settings for the measured workload and resource
  budget. Add an opt-in preset only for an actionable combination with
  repeatable evidence and a clear applicability boundary. A single host
  for a CPU family, OS, or filesystem supports a deployment-specific
  observation. Keep default selection explicit where multiple factors vary.

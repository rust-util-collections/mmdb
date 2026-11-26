# Retesting MMDB 5.0.1 on four hosts

## Protocol fixed before collection

Compare the production engine at `b18532d` with `828e47a` on the same
M5, 5900X, EPYC and 9950X. Upstream `608fef6` at freeze contains the
same production tree as `b18532d` and adds the previous campaign report.
The four independent clones are read-only with respect to production
source. Install the identical benchmark-only post-window hook in both
builds. Use the same compiler, release flags, physical CPU allocation,
mounts, fixture geometry, compression policies, seeds and offered loads
as in the [original protocol](protocol.md). Keep campaign output outside
normal MMDB and VSDB working directories.

### Changes to collection

1. Preflight source/tree, dependencies, binary hashes, tool versions,
   CPU mask, scratch device and cache policy. Derive the engine, storage
   and capacity Cargo locks by changing the local MMDB package version.
   Verify every registry version and checksum. Check actual
   reopen configuration on the first run and probe the capacity tool's
   `fincore` output before the first formal pair.
2. Serialize deliberate workloads using one host lock. Checkpoint a
   complete matched pair (or a coupled group of pairs) with its exact
   schedule and immutable identity. Write its checksum and publish it
   atomically. Resuming skips verified complete blocks, retries an
   interrupted block in full and rejects different code, binaries or
   settings. Keep failed attempts separate from decision samples.
3. Run the one-SST warm clustered test once per physical host: primary
   stacks are M5/APFS, 5900X/dual Btrfs, EPYC/dual Btrfs and 9950X/ZFS.
   All seven paths still get the OS-prewarmed test, full shard writer
   matrix, both codecs and payloads, WAL policies and residual-WAL reopen.
   Rebuild fixtures with 5.0.1. Retain the original device cells and
   ZFS physical-I/O/ARC pilot rule. Repeat both capacity experiments
   with independent fresh processes and new fixtures.
4. Interleave the two disks by matched block. Reuse the actual uncompressed
   high-entropy controls across the codec and direct-volume questions;
   schedule the two volume references together. Reuse the descriptive
   sync-WAL runs from the direct-volume trials. Keep distinct A/A and
   A/B processes within every analysis. Save physical observation IDs
   and the mapping from executions to analysis views. The two-volume
   storage/WAL/reopen/bridge schedule uses 130 physical observations per
   Linux host rather than the preceding campaign's 160. On one-volume
   macOS it uses the same 60.
5. Keep all 560 shard arms: 20,000 requests/stream at hot/all rates
   200/320, 8-second saturation windows, five independent A/A and five A/B
   pairs. After the primary timing and peak RSS sample, call `flush()`
   and then `compact()` on each handle. Require zero ending L0 files and
   pending immutables, and record compaction bytes, SST size, maintenance
   time and time from the **original** start to the common terminal state.
   Through-terminal throughput includes measurement bookkeeping and the
   explicit full compactions. This second metric measures a prescribed
   maintenance endpoint, not normal background steady state. Keep service
   and issue-delay p99 alongside arrival-to-ack p99.
6. Add two revision controls on each primary volume. In fresh matched
   old/old and old/new processes, run the **same configuration** of
   (a) the uncompressed high-entropy sorted ingest, scan and pinned gets
   and (b) the hot saturation writer at 4 slots / 6 MiB. Five pairs of
   each kind give 20 processes per comparison. This separates a binary
   effect from changing both the version and the tuning setting.

The shared observations and removing duplicate warm cells save 150
executions across the comparable matrix. The revision controls add 160
purpose-specific observations. Keep per-volume reader eligibility,
identical work/byte counts, the 1 GiB process RSS limit, independent
controls and ZFS device corroboration. The fixed shard trace has a
scheduled lower bound of approximately 59.5 minutes per volume before
setup and maintenance. Wall-clock time and incident recovery costs are
separate from the number of physical samples.

### Deciding which previous conclusions to revise

Use the original 10% latency/throughput, 20% SST-size and 5% protected
regression thresholds, with at least four of five favorable pairs and
gain outside the A/A band. Keep the five-of-five capacity criterion.
Report a material shift when a protected guard is newly crossed beyond
noise, a previously supported gain is no longer supported, or the new
effect differs by at least ten percentage points outside the control
spread of both campaigns. Distinguish inconclusive results from
opposite-direction evidence. Cross-session changes in a configuration
gain do not establish a revision effect. Use the same-session revision
controls for that claim, and report both versions' hashes. The optional
shard preset still needs the original portability and tail guards on at
least two independent physical hosts. Keep a canonical raw archive and
report; regenerate expanded CSVs and session journals outside Git.

### Reproduce and resume

The [freeze record](retest-5.0.1.json) identifies the production tree,
runner sources and lock digests. Use Python 3.9 or later. Run
`python3 -B derive_locks.py "$locks"` in this directory and copy the
resulting three locks to their respective private build trees. Use the
historical locks for the old control. In both engine checkouts install
the frozen `benches/engine_profile/concurrent.rs` instrumentation from
the workflow commit, then build as in the [original instructions](reproduce.md).
Keep the same release features and compiler flags in each version pair.

Set `MMDB_TRIAL_HOST_LOCK` to one file in the host's private work root
for **every** timed command. `retest_reads.py`, `retest_memory.py` and
`retest_storage.py` document their inputs via `--help`; the latter two
take both volumes in reference/candidate order on Linux. Repeat the
same command and output directory to resume. Capacity uses the same
lease and checkpoints with `run.py --checkpoint-dir DIR --engine-dir
CLONE --lockfile CAPACITY_LOCK`. Use a private `fincore` when needed.
Keep journals in scratch. Publish the completed packets, work-ID
mappings, failed eligibility evidence and final host receipts.

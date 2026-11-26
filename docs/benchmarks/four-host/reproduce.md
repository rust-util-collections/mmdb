# Reproducing the four-host comparison

Use the SHA and dependency digests in [freeze.json](freeze.json).
Create an independent checkout for each host. Keep all fixtures, Cargo
targets and results in that experiment's own paths on the intended device.
Verify the mount of `$scratch`, rather than relying on the path name.
Commit `608fef67569c463a2a1eff64a88c15f099d583d4` retains the exact
external measurement tools for the `828e47a` campaign.

```sh
git clone "$source" "$work/clone"
git -C "$work/clone" checkout --detach 828e47a1d019e3fd0854a3d9a0d98b72e873350c
cd "$work/clone"
gzip -dc docs/benchmarks/apple-m5/bench.Cargo.lock.gz > Cargo.lock
gzip -dc "$campaign/exporter.lock.gz" > tools/v4-export/Cargo.lock
cargo +stable test --locked --test grouped_reads --test crash_recovery
CI=1 cargo +stable test --locked --manifest-path tools/v4-export/Cargo.toml --test golden
cargo +stable bench --locked --no-run --bench engine_profile
cargo +stable bench --locked --no-run --bench grouped_reads --features test-utils
```

Use the two benchmark binaries Cargo prints. The root dev-dependency enables
`test-utils`; stage timers are disabled for the measured engine profile.
Copy this campaign's standalone driver, scripts and schedule from the
checkout containing the report. Place the storage crate at its documented
relative path in the isolated measurement checkout so its MMDB dependency
resolves to the frozen source. An external sibling crate may instead set
only its MMDB path to that clone. Decompress `storage.lock.gz` as the
standalone crate's `Cargo.lock` and build it with
`cargo +stable build --release --locked --manifest-path "$storage_manifest"`.
Keep the core lock and standalone lock distinct.

The storage driver has 63 registry dependencies and the capacity driver
has 57 in their resolved locks. Every included registry package version
and checksum occurs in the frozen engine lock. The standalone crates link
MMDB with its default features. [Storage lock](storage.lock.gz) ·
[Capacity lock](capacity.lock.gz)

## Execute one experiment at a time per machine

- For grouped reads, prepare fixtures of 262,144 and 1,048,576 rows on
  each relevant volume. Use `docs/benchmarks/grouped-reads/trials.py`
  with the frozen revision, base seed 20261125, fanout 32 and the
  sample counts from the protocol. The warm clustered workload is in
  the `primary` matrix. Record whole-device sectors and ARC hits and
  misses on ZFS. Preserve unsuccessful qualifications alongside the
  measurements. [Grouped workload](../grouped-reads/README.md)
- Replay the complete 80-process memory schedule per path with the
  frozen engine's `docs/benchmarks/apple-m5/replay_engine.py`. On Linux
  run the Python process under `taskset -c "$cpus"` so all measured
  children inherit the selected four cores. Host receipts document the
  effective affinity. On macOS use the native scheduler. The replay
  header's `original_binary_sha256` identifies the source of the seed
  schedule; `binary_sha256.control` identifies the executable that ran
  these observations and must match the reopen receipt. [Memory
  schedule](memory-plan.jsonl.gz)
- For the residual WAL, run `engine_profile reopen` with the
  `MMDB_PROFILE_` settings `OPS=100000`, `PREPARE_MIB=64`,
  `BUFFER_MIB=6`, `IMMUTABLES=4`, `POOL_MIB=64` and
  `SEED=20261126+round`. For rounds 1–5 run `READ_ONLY=true` and
  `false`, alternating the order. Retain the full output of both
  processes and verify 100,000 recovered rows.
- For storage options run [storage_trials.py](storage_trials.py).
  The timed sorted ingest includes deterministic input generation and
  durable install. Each file is then read end to end before the timed
  lending scan and 40,000 random point reads with engine cache disabled.
  All looked-up values and the full ordered stream are verified against
  the generator. The WAL writer emits acknowledgement and final-fence
  times, exits without closing the DB, and a fresh opener verifies
  every row. The Linux device list contains the underlying whole disks.
- For the three paired-volume bridges, run [volume_pairs.py](volume_pairs.py)
  with single Btrfs or ext4 as reference and dual Btrfs or ZFS as candidate.
  For the EPYC high-memory run, follow the separate [capacity
  driver](../cache-capacity/README.md) with the same frozen engine.

```sh
taskset -c "$cpus" python3 -B docs/benchmarks/apple-m5/replay_engine.py \
  "$campaign/memory-plan.jsonl.gz" memory.jsonl.gz --tmpdir "$scratch" --binary control "$engine"
python3 -B "$campaign/storage_trials.py" "$storage" storage.jsonl.gz \
  --cpus "$cpus" --devices "$devices" --scratch "$scratch" --stack "$stack" --revision "$revision"
```

On macOS omit the `taskset` prefix and the device list, and use `--cpus
native` for the storage driver. Keep filesystem compression off in the
benchmark scratch directories. Compression of the actual SST blocks is
the setting varied in the LZ4 trials. Do not change global cache settings.

## Recalculate the results

The publication uses one directory per storage stack. Each contains its
host receipt, paired grouped-read CSVs and metadata, the 80-process
memory packet, the 50-process codec/WAL packet and the 10 verified
reopens. The three cross-volume packets sit alongside these directories.
ZFS includes the rejected device pilots and ARC/device counters; the
prewarmed ARC series is identified separately. Each capacity run records
actual cache charges as well as separate process and OS gauges.

```sh
mkdir -p "$evidence"
tar -xzf "$campaign/observations.tar.gz" -C "$evidence"
python3 -B -m unittest discover -s "$campaign" -p test_analysis.py
python3 -B "$campaign/analyze.py" "$evidence/raw" "$summary"
```

The output gives arm medians, pairwise gains, A/A spread, counts and
eligibility. Ending file counts and total compaction output describe
remaining or performed work. Different 8-second saturation arms can
admit different logical byte totals. Use the compaction-output-per-logical-byte
column when comparing that cost. The two WAL acknowledgement policies
are reported independently. The tarball also contains a SHA-256 manifest
of the canonical observation files. Host receipts retain CPU/volume
details and executable hashes; private account names and scratch paths
may be replaced by their benchmark roles in the public copies.

## Optimized 5.0.1 retest

Use the [retest freeze](retest-5.0.1.json) and [selection rules](retest.md).
The workflow commit is `31d495f`; `07aa56b` fixes the Linux affinity
query used before memory trials. Use the engine clone at `b18532d` and a
separate `828e47a` clone for the old control. Copy the same frozen
`benches/engine_profile/concurrent.rs` into **both** benchmark checkouts
before compiling. Its optional post-window compact measurement changes
the harness. All production source files must match the respective engine
revision. Build the same storage driver twice with the new and historical
locks. The 5.0.1 locks are generated on demand:

```sh
python3 -B "$campaign/derive_locks.py" "$locks" --manifest "$new_clone/Cargo.toml"
python3 -B "$campaign/derive_locks.py" "$locks" --manifest "$new_clone/Cargo.toml" --check
```

On Linux the first volume below is the single-drive reference and the
second is the RAID or ZFS candidate. `$cpus` is the four-core set in the
protocol. Set the host lease for the whole session and choose an empty,
private output directory. The commands below illustrate the 5900X.

```sh
export MMDB_TRIAL_HOST_LOCK="$work/.host-measurement.lock"
taskset -c 4-7 python3 -c 'import os; assert os.sched_getaffinity(0) == {4, 5, 6, 7}'
revision=b18532df32ee89af88734577e23be6e87041b0c1
old=828e47a1d019e3fd0854a3d9a0d98b72e873350c
python3 -B "$campaign/retest_storage.py" "$out" --revision "$revision" \
  --storage "$storage" --engine "$engine" --old-storage "$old_storage" --old-revision "$old" \
  --volume 5900x-single "$single" nvme1n1 --volume 5900x-raid "$raid" nvme2n1,nvme3n1 \
  --primary 5900x-raid --cpus 4-7
python3 -B "$campaign/retest_reads.py" "$probe" "$single/fixtures" "$out/5900x-single/grouped" \
  --stack 5900x-single --revision "$revision" --devices nvme1n1
python3 -B "$campaign/retest_reads.py" "$probe" "$raid/fixtures" "$out/5900x-raid/grouped" \
  --stack 5900x-raid --revision "$revision" --devices nvme2n1,nvme3n1
python3 -B "$campaign/retest_memory.py" "$out" --revision "$revision" \
  --engine "$engine" --old-engine "$old_engine" --old-revision "$old" \
  --volume 5900x-single "$single" --volume 5900x-raid "$raid" --primary 5900x-raid --cpus 4-7
```

On macOS pass one `m5-apfs` volume, `--cpus native`, and `-` for
the storage runner's disk list. Reuse the same command, path and binaries
after an interruption. Checkpoint identity includes the measured binary
and runner fingerprints; new identities need new output directories.
The capacity driver's `--checkpoint-dir`, `--engine-dir` and `--lockfile`
select its resumable mode.

To compare the published packets, extract each archive into its own
directory, preserving its `raw/` layout. Keep the generated tables in a
working directory. The comparison validates the work-ID mapping, including
distinct controls within each analysis and identical raw payloads in
shared views. A change of evidence status is reported even if both
campaigns estimated the same direction.

```sh
mkdir -p "$previous" "$current"
tar -xzf "$campaign/observations.tar.gz" -C "$previous"
tar -xzf "$campaign/retest-observations.tar.gz" -C "$current"
python3 -B "$campaign/analyze.py" "$previous/raw" "$previous/summary"
python3 -B "$campaign/analyze.py" "$current/raw" "$current/summary" \
  --retest-freeze "$campaign/retest-5.0.1.json" --compare "$previous/summary"
```

`changes.csv` compares the configuration gains across campaigns;
the `version-*` rows in the new `paired.csv` compare binaries inside
one campaign. `executions.json` reconciles physical storage processes
with their analysis views. `through_compact_MiB_s` uses the clock from
the common start through the explicit terminal maintenance pass.

# Reproducing the four-host comparison

Use the SHA and dependency digests in [freeze.json](freeze.json).
Create an independent checkout for each host. Keep all fixtures, Cargo
targets and results in that experiment's own paths on the intended device.
Verify the mount of `$scratch`, rather than relying on the path name.

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

# Engine workload profiles

Run each arm as a separate optimized process on a local filesystem. The
`engine_profile` bench creates temporary database directories under `TMPDIR`.
It reports small puts, bulk writes, ordinary pinned gets, new snapshot reads
and reads with one held snapshot as separate classes. The recorded trials
show per-class latency, queue capacity effects, outstanding compaction work
and recovery costs. [Measured results and raw runs](results.md)

```sh
cargo +stable bench --bench engine_profile -- mixed
MMDB_PROFILE_PLACEMENT=all MMDB_PROFILE_IMMUTABLES=2 cargo +stable bench --bench engine_profile -- memory
cargo +stable bench --bench engine_profile -- reopen
cargo +stable bench --bench engine_profile --features bench-metrics -- mixed
```

| Setting | Meaning |
|---|---|
| `MMDB_PROFILE_OPS`, `SEED` | Primary operations per stream and deterministic trace seed. All settings in this table use the `MMDB_PROFILE_` prefix. |
| `RATE` | Offered scalar operations/s per foreground stream. Zero selects a closed-loop saturation run. Each worker replays its own scheduled arrivals, including arrivals while an earlier call is blocked. |
| `SECONDS` | Common duration of a saturation window. With a nonzero value every stream runs until the same deadline. The fixed-count smoke trace sets this to zero. |
| `PLACEMENT` | `hot` uses one DB for all traffic. For `mixed`, `split` moves the large batches to a second DB. For `memory`, `all` activates all 16 DBs sharing one pool. The `hot` memory case also opens and preloads 16 handles. |
| `CONTROL` | `mixed` enables all streams, `writer` selects one scalar writer and `reader` one warm pinned reader. |
| `BUFFER_MIB`, `IMMUTABLES`, `POOL_MIB` | Per-DB write-buffer size, immutable limit and the configured capacity of each evictable store in the shared pool. |
| `ENTROPY`, `SYNC`, `SAMPLE` | Boolean switches for deterministic pseudorandom payloads, per-write WAL sync, and independent 50 ms memory sampling. The compiled `bench-metrics` feature separately enables thread-local stage clocks. |
| `BATCH_ENTRIES`, `BATCH_RATIO` | Mixed default is 4,096 entries of 256 bytes in one batch per 128 foreground puts. Each memory worker uses 64-entry batches by default. |

The 16-byte keys have an eight-byte prefix. All DBs preload 32,768 scalar
rows, flush them and report the initial live set before timing. Scalar writes
carry 128-byte values. Reported logical write bytes include the key and
value. WAL and SST compression are separate from filesystem compression.
The paired driver uses the engine's default uncompressed SST blocks.
The fixed-buffer sweep brackets alternatives with depth 4 and 16 MiB.
The equal-envelope sweep brackets alternatives with depth 4 and 6 MiB,
and holds `(1 + depth) × buffer size` at 30 MiB per member. Each sweep
calibrates its own offered load. Both use the same process RSS ceiling.
Separate sampled runs cover every placement and geometry in both sweeps.
Current runs include the post-worker observation in component maxima and
print it as `sampled_after`. Archived runs without this field used the initial
and periodic 50 ms observations only. A brief high between samples can still
be missed. Process peak RSS is the independent OS high-water mark.
The paced calibration starts with a finite trace only as a rate estimate,
then brackets a sustainable rate with time-stamped arrivals in the requested
mix. The chosen offer is 70% of the best verified rate. Backlog at each cut
includes the last scheduled arrival of each active stream. Both calibration
and load qualification allow the maximum of two requests, the contributing
stream count and 1% of offered requests (rounded down) per class.
Saturation runs use
one common deadline so a fast reader does not exit before slow writers.
Throughput includes every completion. A bounded per-worker reservoir keeps
latency memory stable in duration-based trials, with percentile weights
proportional to each stream's completions.
The reopen child writes deterministic rows, syncs the WAL and exits without
dropping its handle. The matrix sets `MMDB_PROFILE_PREPARE_MIB=64` so
writable replay can use a smaller buffer than the original writer. It also
includes a `MMDB_PROFILE_READ_ONLY=true` arm. The parent measures open, then
checks every recovered row. Record open-time memory separately from the
verification scan.

For the recorded mixed, memory and writer-handoff trials, the measured
worktree used the concurrent driver later committed at `7b46443`. To
reconstruct the paired source from reachable commits, create a detached
worktree at `cf40cab` and run `git restore --source=7b46443 -- benches/engine_profile`
in it. Build that control and a copy with the short-group patch applied.
The reconstructed candidate writer and every engine-profile Rust source
match the original candidate checkout byte for byte. For the separate
reopen measurements, use `bb81151`, which includes the 64 MiB preparation
buffer and the read-only arm. Each raw suite header records which binary
digests it used. The extended memory guard reuses the original control
binary. The later Python evidence checks run after collection.

The [short-group candidate](short-group.patch) also applies at `7b46443` in a
separate checkout. It stores short request lists inline and skips a wakeup
only for an uncontended leader serving itself alone. Use matching source
and compilation settings for the control. Pass the observed asynchronous
and synchronous control rates to `--handoff-rates` to reuse the mixed trace.

For paired runs, build each benchmark target with `--no-run`, save its
executable at a distinct path, then invoke the matrix driver. The final line
of Cargo's `--no-run` output identifies the executable. Use an uncompressed
scratch directory for the entropy controls. Record any inherited filesystem
compression setting separately from the mount options.

```sh
python3 docs/benchmarks/engine-profile/run.py mixed ./control mixed.jsonl.gz --metrics ./instrumented --cpus 4-7 --tmpdir /path/to/scratch
python3 docs/benchmarks/engine-profile/run.py memory ./control memory.jsonl.gz --metrics ./instrumented --cpus 4-7 --tmpdir /path/to/scratch
python3 docs/benchmarks/engine-profile/run.py handoff ./control handoff.jsonl.gz --candidate ./candidate --cpus 4-7 --tmpdir /path/to/scratch
python3 docs/benchmarks/engine-profile/run.py reopen ./control reopen.jsonl.gz --cpus 4-7 --tmpdir /path/to/scratch
```

The focused memory p99 repeat uses the original uninstrumented binary. It compares depth 2 at 10 MiB with depth 4 at 6 MiB. The hot and all-active placements retain their respective 251 and 1,381 requests/s per stream from the 30 MiB-envelope calibration. Each seed runs an independent control/control pair and control/candidate pair in separate processes. The ordinary summarizer computes its noise against the first A/A arm and its gain against the separate A/B control. RSS budget flags describe the observed peak in each arm. A matched pair that cannot sustain the offer receives `offered-load-unqualified` on its p99 and RSS guard rows, alongside the observed memory figures.

```sh
python3 docs/benchmarks/engine-profile/run.py memory-tail ./control memory-tail-extension.jsonl.gz \
    --cpus 4-7 --tmpdir /path/to/scratch --ops 20000 --seed 20461125
python3 docs/benchmarks/engine-profile/summarize.py memory-tail-extension.jsonl.gz
```

Each compressed JSON-lines record contains the per-run settings, revision-
specific binary digest in the header, success/rejection counts, outstanding
work at the halfway point and at the end of the offered schedule, latencies
and memory/LSM measurements. Each trial drains its trace. Interpret p99 on
a per-run basis. Classes below 10,000 successes are labeled
`p99_qualified=false` and only their completion, throughput and stage
measurements are used for decisions. Service time starts after preparing
the key or batch. Arrival-to-ack also includes preparation and any backlog.
The issue-delay percentile measures dispatch after the later of the
scheduled instant and the previous acknowledgement, exposing wakeup and
harness overhead. Linux CPU ticks cover the timed worker window. Reopen
reports resident and peak memory immediately after open and a separate
peak after verifying all values. TLS counters belong
to the worker executing a stage. A follower's wait overlaps time spent in
another worker's WAL and apply stages. RSS includes the benchmark driver,
preload and temporary allocations. [Decision protocol](../../proposals/mmdb-v5-execution-plan.md#4-measure-the-limiting-engine-operation)

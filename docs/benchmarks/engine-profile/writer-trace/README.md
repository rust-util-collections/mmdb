# Opportunity for ordered WAL and memtable overlap

**Decision on 2026-11-25** — Defer a second write stage for the tested
same-shard workload. Traces show similar aggregate WAL and apply times
(148–154 ms and 122–132 ms per run), but the groups that could run together
have very different sizes. An optimistic replay of the recorded stage
durations improved small-put p99 by a median of 0.065%; the largest modeled
gain was 4.23%. This is a feasibility estimate. The engine executed its
existing serial writer in all ten processes.

A [second five-pair screen with the warmed driver at 7b46443](warm-7b46443/README.md)
replayed the identical scheduled arrivals after warming the scalar cache.
It found a median modeled p99 gain of 0.167%, with a maximum of 0.309%.
Both screens support deferring the pipeline on this trace. The measurements
below retain the first driver's original preload behavior.

## Measured trace and projection

One synchronous API caller supplied 10,000 small puts, another supplied 79
batches of 4,096 entries. Each caller submitted its next request only after
the preceding one returned. A small group's own WAL could overlap a prior
bulk apply; a bulk WAL could overlap a prior small apply. Two successive
small groups cannot overlap one another on this trace. A group serving both
callers occupies both until release.

| Pair | Observed groups | Enqueued before preceding apply ended¹ | Projected groups with actual interval overlap | Modeled overlap total, ms | Recorded small-put p99, ms | Projected p99, ms | Projected gain | Free-neighbor sensitivity gain |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 10,073 | 67 | 66 | 2.101 | 4.754 | 4.587 | 3.53% | 3.92% |
| 2 | 10,076 | 63 | 56 | 0.481 | 4.531 | 4.531 | 0.00% | 0.32% |
| 3 | 10,075 | 71 | 68 | 0.544 | 3.931 | 3.765 | 4.23% | 4.83% |
| 4 | 10,074 | 61 | 57 | 0.467 | 10.574 | 10.567 | 0.065% | 0.065% |
| 5 | 10,076 | 64 | 61 | 0.970 | 6.178 | 6.178 | 0.00% | 0.051% |

¹ All members of the next recorded group were enqueued, and the WAL generation
matched. There were five generation transitions in each run.

The experiment used the execution plan's 15% small-put arrival-to-ack p99
threshold. To justify a live pipeline trial on this workload, the projected
gain had to reach that threshold at the median and in at least four of five
traces. None met it. For an intentionally more generous sensitivity check,
grant a solo scalar group adjacent to a solo bulk group zero scheduler,
admission, WAL, apply and release service time, even if the neighbor was not
available to overlap it. The maximum sensitivity gain was 4.83%.

Recording timestamps and natural A/A variation can change latency. Relative
small-put p99 with tracing enabled versus the alternating tracing-disabled
run ranged from −41% to +58%. Do not infer a precise instrument overhead
or a production p99 from these traces. The threshold comparison is performed
within each recorded run. Group order, membership, service costs, caller
preparation and scheduler delays are fixed inputs. A workload with more
independent writers or a redesign that changes snapshot-lock contention or
group formation would require its own measurement.

## Reconstruction and controls

The serial replay first reconstructs every original acknowledged call
timestamp exactly. The projected replay then uses the same records:

- Record scheduled arrival, enqueue under the write-queue lock and caller
  acknowledgement. Keep the measured preparation/dispatch delay after the
  later of the scheduled arrival and that caller's previous acknowledgement.
  This prevents the model from inventing extra outstanding scalar requests.
- For each FIFO group record its sequence range, membership shape, generation,
  admission, WAL and apply interval endpoints, and release under the queue
  lock. Replay the observed scheduler gap after the group becomes ready,
  including the readiness of both members of a coalesced group.
- Run one WAL/admission lane and one ordered apply/release lane. The next
  group can start after the prior WAL completes when all members are ready.
  Apply waits for FIFO release and completion of its own WAL. A generation
  change waits for the entire preceding group. The model charges zero
  additional handoff or reservation cost. It has at most two outstanding
  write requests under this workload. It never moves an acknowledgement
  before the group's applied publication.
- Compute arrival-to-ack p99 separately inside each process, using the
  same nearest-rank calculation as the driver. Record both the number and
  actual duration of projected intersections between WAL and apply intervals.
  All five traces reconstructed their 10,079 observed acknowledgements exactly.

## Replay and reproduction

The five losslessly compressed CSVs contain request (`R`) and group (`G`)
timestamps in nanoseconds relative to the common workload clock. Their
uncompressed SHA-256 hashes and the ten actual run summaries are recorded
in [summary.jsonl](summary.jsonl). To replay the original observations:

```sh
python3 -m unittest test_replay.py
for trace in trace-*.csv.xz
do
    python3 replay.py "$trace"
done
```

The [diagnostic patch](trace.patch) applies to MMDB revision `cf40cab`.
It adds thread-local trace buffers to a detached instrumentation build.
The measured path used fixed-period arrivals, 16 MiB per-DB write buffer,
immutable limit 4, 64 MiB shared pool setting, high-entropy inputs, ordinary
WAL-enabled acknowledgement, and cores 4–7. The [mixed driver](../README.md)
calibrated 18,911 arrivals/s for each scalar stream and one bulk arrival per
128 intervals. All runs used separate processes, the same NOCOMPRESS Btrfs
scratch directory, and the same Linux x86_64 host. Trace collection on/off
alternated by pair. The detached program was built with stable Rust in the
bench profile with `bench-metrics`.

Apply the patch in a separate checkout at that revision, then build
`cargo +stable bench --bench engine_profile --features bench-metrics --no-run`.
Use the resulting executable path with the [recording driver](record.py).
Run with an otherwise idle CPU and I/O allocation. Fresh traces are saved
as gzip CSVs, which [replay.py](replay.py) also accepts.

```sh
python3 record.py --binary "$engine_trace" --cpus 4-7 --tmpdir "$scratch" \
    --out fresh-results --rate 18911 --seed 20261125
```

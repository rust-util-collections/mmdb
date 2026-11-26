# Ordered writer-stage replay

MMDB keeps its serial WAL and memtable writer. On five warmed traces, an
optimistic two-stage replay reduced small-put arrival-to-ack p99 by a median
**0.167%** (range 0–0.309%). All five fell below the 15% screening threshold.
The engine ran the serial writer throughout. [Per-pair results](analysis.json) · [Observed runs and trace hashes](summary.jsonl)

The Ryzen 9 5900X Linux fixture placed two write issuers on one DB. One sent
10,000 scalar puts at 18,911 offered arrivals/s. The other sent 79 batches
of 4,096 entries, one per 128 scalar intervals. Each waited for its previous
acknowledgement before submitting another request. The DB had a 16 MiB write
buffer, immutable depth four and a 64 MiB pool setting.

Serial replay reconstructs every acknowledgement from the observed enqueue,
group, WAL, apply and release timestamps. The alternative allows the next
WAL stage to overlap the previous apply stage while preserving FIFO groups,
generation boundaries, caller readiness and measured service durations. It
charges zero extra handoff time. Only 64–69 pairs of stages overlapped in
each run, for 0.531–1.674 ms total. A sensitivity replay making the entire
neighboring scalar group free gave a maximum gain of 0.411%. Other issuer
counts, workloads or group formation can change the available overlap.

From this directory, run `python3 -B verify.py` to check the five trace
hashes, arrival schedules, serial acknowledgements and projected p99s.
Run `python3 -B -m unittest test_replay.py` for the synthetic timing checks.

To collect a new trace, apply the [instrumentation patch](trace.patch) in a
checkout at `7b46443` and build
`cargo +stable bench --bench engine_profile --features bench-metrics --no-run`.
Use the printed executable, uncompressed scratch on a local device and
otherwise idle CPUs. Observations alternate recording on and off in fresh
processes. [Recording driver](record.py) · [Replay model](replay.py)

```sh
python3 -B record.py --binary "$engine_trace" --cpus 4-7 --tmpdir "$scratch" \
    --out fresh-results --rate 18911 --seed 20261125
```

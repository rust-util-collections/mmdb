# Level-zero compaction and WAL fences

MMDB keeps serial execution for L0→L1 compaction. A two-range prototype
reduced foreground stall time per admitted GiB by 97.94% on the two-hot-prefix
fixture. The paired read p99 increased 5.08%, and the 33.80% serial/serial
variation exceeded the 5% regression limit. This single-DB trace establishes
a stall reduction under L0 pressure. The read guard and cross-DB worker budget
remain unresolved. [Five paired A/A and A/B trials](runs-65536.csv)

| Metric, 65,536 requests per class | Serial median | Two-range median | Median paired gain | Largest A/A difference |
|---|---:|---:|---:|---:|
| Foreground L0 stall ms per logical GiB written | 33,932.7 | 698.4 | 97.94% | 26.46% |
| Write arrival-to-ack p99, ms | 10.329 | 2.291 | 77.50% | 95.30% |
| Read arrival-to-ack p99, ms | 0.443 | 0.437 | −5.08% | 33.80% |

Each process admitted all work, completed 159 flushes and 79 multiple-target
L0 rewrites, and finished with one L0 file after close. In the candidate,
all 79 jobs executed multiple ranges, up to two concurrently on this DB.
Serial and candidate wrote 223,218,911 and 223,211,621 compaction-output
bytes. Paired gains are calculated inside each pair. The arm medians can
have a different ordering.

**Native fence.** With one hot prefix, a synchronous deletion of the key
`[1]` every 256 puts widened the L0 span and consumed 2,004,219 bytes of
original cold-only SSTs in each run. With the same writes and fence cadence,
`DB::sync_wal()` left all those SSTs in place. New compaction output fell
from 51,277,277 to 11,238,105 bytes. Logical cold-row bytes emitted fell
from 38,993,920 to zero. The cold files were between the sentinel and hot
prefix. With two hot prefixes the cold gap is still inside the L0 key
span. [Five matched pairs](fence.csv)

**Prefix-aware flush cuts.** With two hot prefixes and the production
full-L0 picker, one extra output cut doubled the installed files from 40
to 80 over 40 flushes. All original cold L1 inputs were still consumed and
logical cold emissions rose 35%. MMDB keeps its existing flush policy.
[Prefix-cut test, data and reproduction](prefix-cuts/README.md)

## Fixture and reproduction

One writer and one reader each received 20,000 scheduled requests/s on
Linux x86_64, Ryzen 9 5900X, CPUs 4–7 and local NVMe/Btrfs with compression
disabled in the scratch directory. Each process began with 24 L1 files
and no L0 files, eight eight-byte prefixes and 1,024 rows per prefix
(16-byte key plus 256-byte value). Snapshot, live and writable-reopened
rows were checked independently. The write buffer and target SST were
128 KiB, immutable depth four, and compaction/slowdown/stop thresholds
2/3/5. The two-range prototypes use one coordinator and the same CPU
allocation. Reported stall time includes L0 slowdown/stop waits. Immutable-queue waits have their own counter. Input and output byte
counters use file lengths and exclude trivial moves. Cold logical bytes
come from the diagnostic row collector. Counters ending `after_workers`
cover foreground completion, the final counters include the close/drain.

Build [the driver](../../../benches/l0_profile.rs) in a serial worktree and
one with the [two-range patch](two-range.patch) applied. The patch applies
at `5528950`. At the measured revision `c8b4d40`, use the version of the
patch from `538678e`. Build each revision into a separate target directory
with `cargo +stable bench --features bench-metrics --bench l0_profile --no-run`.
The recorded serial and two-range binaries had SHA-256 fingerprints
`216b26a2bd4951e5c680a337474dd1c55754cd2102fcc2f18a37dc20606e0061`
and `2fdb2e2261d1d8ad41b9b1f899c97f7c45b2d403e2bda6ff7fc25ad66525a9a7`.
The candidate includes a snapshot/tombstone regression with overlapping
L0 range executions during a concurrent flush. In the candidate worktree run
`cargo +stable test --features bench-metrics --lib split_l0_preserves_snapshot_stripes_and_concurrent_flush_across_boundaries`.

From this directory run fresh, paired binaries against an empty scratch
directory. Alternate order within each pair.

```sh
python3 replay.py --baseline "$serial_binary" --candidate "$candidate_binary" \
    --cpus 4-7 --tmpdir "$scratch" --operations 65536 --output split.csv aa ab
python3 replay.py --baseline "$serial_binary" --candidate "$candidate_binary" \
    --cpus 4-7 --tmpdir "$scratch" --operations 16384 --output fence.csv.new fence
```

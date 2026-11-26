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

## Bounded prefix cuts during flush

MMDB keeps its existing flush policy. A single additional prefix cut doubled
the file count at the same number of flushes and increased cold-row rewrites.
All original cold-only L1 files still entered compaction.

| Five A/B pairs, 16,384 requests per class | Control median | One-cut median | Paired gain | Paired gain range | Max A/A difference |
|---|---:|---:|---:|---:|---:|
| Installed L0 files over the run | 40 | 80 | −100.00% | −100.00% to −100.00% | 0.00% |
| L0 rewrites through close | 20 | 27 | −35.00% | −40.00% to −30.00% | 0.00% |
| Foreground stall ms/GiB of admitted writes | 32,893.9 | 148,096.1 | −346.31% | −385.13% to −92.97% | 54.27% |
| Cold logical bytes emitted through close | 33,423,360 | 45,121,536 | −35.00% | −40.00% to −30.00% | 0.00% |
| New compaction-output file bytes through close | 56,456,549 | 75,705,968 | −34.10% | −39.14% to −29.06% | 0.00% |
| Foreground read arrival-to-ack p99, µs | 490.7 | 548.7 | −13.96% | −59.00% to +3.30% | 45.54% |
| Foreground write arrival-to-ack p99, µs | 12,063.8 | 66,166.2 | −446.93% | −580.88% to +3.77% | 254.85% |

Positive gain favors the optional cut. Stall time increased in all five comparisons, and each arm retired 1,717,902 bytes of original cold-only SSTs. The cold-row collector was enabled identically on both arms. Short latency tails varied appreciably, but the candidate missed both its cold-rewrite objective and the 20% stall-improvement screen. Each process completed the prescribed trace and checked its snapshot, live and writable-reopened rows.

At close, the remaining L0 file counts differed. The control closed with zero L0 files in all five comparisons and the candidate with 0–4. At foreground completion, the control had emitted 53,611,715 bytes of new compaction SSTs in each run. The candidate had already emitted 70,016,530–75,706,198 bytes. Extra output was therefore present before cleanup as well. A different picker that skipped disjoint gaps would need new output-interval and crossing-tombstone proofs. This trial measures cuts with today's complete L0 claim and serial execution. [All 20 observations](prefix-cuts/runs.csv) · [Replay driver](prefix-cuts/trials.py)

The cut separates two eight-byte prefixes when there is at least 32 KiB
of uncompressed payload on each side. Both resulting SSTs still belong
to the same whole-L0 claim. Its aggregate key span includes the cold gap,
and the extra files reach the existing L0 thresholds sooner. The candidate
test covers a range deletion across the cut and the visibility of boundary
versions through flush, compaction and writable reopen.
[Picker implementation](../../../src/compaction/leveled.rs)

### Replay the prefix-cut trial

Run the commands from this report’s directory. Set `patch` to the absolute path of `prefix-cuts/one-cut.patch`. Start both worktrees at `5528950`. In the control tree, apply the diagnostic additions to `src/db/flush.rs`, `src/compaction/profile.rs` and `benches/l0_profile.rs`. Apply the entire patch to the candidate. The diagnostic increments once per newly installed L0 SST and the benchmark reports it as `flushed_ssts`. The two binaries used Rust stable 1.100.0 with `bench-metrics` and independent MMDB rebuilds. Control SHA-256 was `e63453bdf581ae3ebb8c47a5338f5b6c2b85cd252d820d22935029fb0ed7f229` and candidate SHA-256 was `8a22f508a9f0421a66d65c4a981c0b320a92bbd8916b03962f7282e670791b1f`. [Experimental patch](prefix-cuts/one-cut.patch)

```sh
git -C "$control_tree" apply --include=benches/l0_profile.rs --include=src/db/flush.rs \
    --include=src/compaction/profile.rs "$patch"
git -C "$candidate_tree" apply "$patch"
cargo +stable test --manifest-path "$candidate_tree/Cargo.toml" --lib full_l0_claim_selects_the_same_cold_gap_after_disjoint_flushes
cargo +stable test --manifest-path "$candidate_tree/Cargo.toml" --lib one_prefix_cut_preserves_stripes_and_crossing_range_deletion
cargo +stable bench --manifest-path "$control_tree/Cargo.toml" --features bench-metrics --bench l0_profile --no-run
cargo +stable bench --manifest-path "$candidate_tree/Cargo.toml" --features bench-metrics --bench l0_profile --no-run
python3 prefix-cuts/trials.py --baseline "$control_binary" --candidate "$candidate_binary" \
    --l0-replay replay.py --tmpdir "$scratch" --cpus 4-7 --operations 16384 --rate 20000 \
    --output fresh-runs.csv
```

The runner uses the original `gap` fixture with one serial range in both arms, six cold prefixes between the two updated prefixes, and the cold-row observer enabled in every run. All comparisons use the same durability, seeds, CPU allocation, starting LSM layout, collector, and 20,000 operations/s offered per reader and writer. Each fresh process verifies every held snapshot, current row and reopened row. Compare matched A/B runs against the independently repeated A/A band.

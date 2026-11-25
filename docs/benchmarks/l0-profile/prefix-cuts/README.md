# Bounded prefix cuts during flush

**Retain the existing flush policy.** The bounded prefix cut doubled installed L0 SSTs for the same 40 durable flushes in every candidate run. All original cold-only L1 files still entered a compaction. The lower file count in the control limited how often it rewrote those cold prefixes.

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

Closing debt varied. The control closed with zero L0 files in all five comparisons and the candidate with 0–4. At foreground completion, the control had emitted 53,611,715 bytes of new compaction SSTs in each run. The candidate had already emitted 70,016,530–75,706,198 bytes. Extra output was therefore present before cleanup as well. A different picker that skipped disjoint gaps would need new output-interval and crossing-tombstone proofs. This trial measures cuts with today's complete L0 claim and serial execution. [All 20 observations](runs.csv) · [Replay driver](trials.py)

## Design and correctness

This experiment changes one decision inside `DB::write_memtable_ssts`. For an uncompressed output with a configured prefix, it can start one additional SST at a prefix change, provided each side contains at least 32 KiB of point-value payload. Uncompressed data blocks store those value bytes verbatim. The prepass therefore gives a conservative bound that excludes key savings and range-tombstone endpoints. Existing mandatory metadata cuts continue to take precedence. The extra boundary uses the same user-key transition, tombstone clipping, carry and atomic flush install as an ordinary cut.

The deterministic picker control ingests a single cold L1 point file between two hot prefixes. It repeats the pick with one broad L0 SST and with two explicitly flushed, point-disjoint L0 SSTs. Both picks include the cold L1 input. Current whole-L0 selection seeds the target overlap search with the aggregate key span. Splitting the flush can reach the existing file-count thresholds sooner without shrinking that span. [Picker](../../../../src/compaction/leveled.rs)

The candidate test produced two L0 SSTs instead of one with three substantial prefixes, a crossing range deletion, two versions at a cut boundary and old/deleted/current snapshots. It checked point reads and the full ordered scan through flush, compaction and writable reopen. A second flush with a small trailing prefix emitted one SST. The two-file assertion failed on the control with the test alone and passed once the patch enabled the cut.

## Replay

Start both worktrees at `5528950`. In the control tree, apply the diagnostic additions to `src/db/flush.rs`, `src/compaction/profile.rs` and `benches/l0_profile.rs`. Apply the entire patch to the candidate. The diagnostic increments once per newly installed L0 SST and the benchmark reports it as `flushed_ssts`. The two binaries used Rust stable 1.100.0 with `bench-metrics` and independent MMDB rebuilds. Control SHA-256 was `e63453bdf581ae3ebb8c47a5338f5b6c2b85cd252d820d22935029fb0ed7f229` and candidate SHA-256 was `8a22f508a9f0421a66d65c4a981c0b320a92bbd8916b03962f7282e670791b1f`. [Experimental patch](one-cut.patch)

```sh
git -C "$control_tree" apply --include=benches/l0_profile.rs --include=src/db/flush.rs \
    --include=src/compaction/profile.rs "$patch"
git -C "$candidate_tree" apply "$patch"
cargo +stable test --manifest-path "$candidate_tree/Cargo.toml" --lib full_l0_claim_selects_the_same_cold_gap_after_disjoint_flushes
cargo +stable test --manifest-path "$candidate_tree/Cargo.toml" --lib one_prefix_cut_preserves_stripes_and_crossing_range_deletion
cargo +stable bench --manifest-path "$control_tree/Cargo.toml" --features bench-metrics --bench l0_profile --no-run
cargo +stable bench --manifest-path "$candidate_tree/Cargo.toml" --features bench-metrics --bench l0_profile --no-run
python3 trials.py --baseline "$control_binary" --candidate "$candidate_binary" \
    --l0-replay ../replay.py --tmpdir "$scratch" --cpus 4-7 --operations 16384 --rate 20000 \
    --output runs.csv
```

The runner uses the original `gap` fixture with one serial range in both arms, six cold prefixes between the two updated prefixes, and the cold-row observer enabled in every run. All comparisons use the same durability, seeds, CPU allocation, starting LSM layout, collector, and 20,000 operations/s offered per reader and writer. Each fresh process verifies every held snapshot, current row and reopened row. Compare matched A/B runs against the independently repeated A/A band. [Original workload and accounting](../README.md)

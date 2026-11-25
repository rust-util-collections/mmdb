## Adequate

Units 1–3 name falsifiable outcomes, and part of each already runs.

Admission. One retryable `Busy` leaves the WAL, sequence, and visible rows unchanged, and a later retry succeeds. `FlushQueue::check_capacity` already returns `Busy`. `newest_immutable_wins_while_oldest_flush_is_parked` parks the oldest flush, expects `Busy`, and checks that `sync_wal` consumes no sequence. `usage.md` already places that queue error beside the L0 `InvalidArgument`. `write_batch_group` already fails only the `no_slowdown` member when the immutable queue is full.

Memory. The table matches the code. `pinned_bytes` is exact per member. `block-cache-usage` stays an entry count. Moka weighs buffer capacity on separate segmented and overflow stores, whose combined weight may exceed one capacity. `MemTable::approximate_size` is relaxed. RSS excludes retained pins and old views. Gauges are sampled outside the timed loop. `oversized_block_above_segment_share_is_retained_and_invalidated` and `tests/shared_cache.rs` cover retention, invalidation, and detach.

Boundary. `power_loss_discards_only_the_unsynced_suffix_across_wal_rotation` shows `sync_wal` adds no WAL bytes and no sequence. `v5_manifest_geometry_precedes_repair_and_read_only_adopts_it` rejects a wrong writable geometry. Golden tests cover residual WAL, iterator status, an unpublished destination after a bad SST read, and exclusive rename. Unit 5's correctness gates, and its rule to keep the simpler path when the signal is ambiguous, are usable. Grouped reads stay in MMDB and leave VecDex to VSDB. The 2026-11-25 note records the correctness gate. Candidate timings and AArch64 are pending.

## Edits

### 1. Section 4, "Choose a shard memory profile", and the handoff `v5_profile` loop

Hypothesis: a limit in {1, 2, 4} lowers foreground p99 at a fixed buffer and under one byte budget, for one hot DB and for 16 DBs.

`benches/v5_profile.rs` is one DB, a closed loop of two readers and two writers, one within-run p99, and `VmHWM` after a 32,768-key preload. The unset limit is 2; `DbOptions` defaults to 4. The bench buffer is 256 KiB; the default is 64 MiB. `docs/benchmarks/point-ops/runs.csv` is elapsed time for another mix. A closed-loop stall moves throughput and p99 together, so matched admitted load is not measured. No budget is defined, so "meets the memory budget" cannot fail.

Build a new driver. The current binary is only a smoke rerun. Control: defaults, one writer, `WalAsync`, compaction idle, same payload. Fix this planning cap before the run: pool capacity plus 16 × buffer × (1 + immutable limit). Interleave five fresh processes per arm. Pass only when median p99 beats every other arm by more than the winner's max−min, admitted bytes/s stay at or above the default arm's minimum, and peak RSS stays inside the cap. Otherwise keep `max_immutable_memtables = 4`. The cap and the five-run count are planning choices. This tree has no measured optimum behind them.

### 2. Self-test and handoff commands

These commands are existing checks. They stay green while the new contracts are missing.

`test_write_options_no_slowdown` expects `InvalidArgument` and ignores WAL length and sequence. Three flushes with compaction disabled are stable because flush stores `l0_file_count`. Admission fires at `l0_slowdown_trigger`, and open already rejects slowdown above stop, so a separate stop case repeats that predicate. Pass on `Busy`, an absent key, unchanged WAL and sequence, then compact, retry, and reopen. Keep queue pressure on the parked-flush test, including a neighbor `write_batch_group` already admitted.

Weighted-byte getters and the two-member test are unbuilt. After `run_pending_tasks`, match each store to the sum of inserted capacities. Require immutable bytes to rise while the oldest flush is parked and to fall after durable install. Exact equality on `approximate_size` will false-fail.

`exports_residual_wal_and_snapshot_striped_sst` opens the destination read-only at levels 3 and prefix 2. Add an eight-byte prefix, deletions, and a writable reopen, write, close, and read. Wrong geometry is already the integration test above. `test_execute_compaction_io_actually_parallelizes_with_max_subcompactions` drives L1-to-L2 `execute_compaction_io`. Its comment says `DB::compact` misses that branch. The L0 split and the serial fallback need their own reopen comparison.

### 3. Section 4, "Identify L0 debt"

Hypothesis: a `WalSync` delete of `[1]` rewrites more cold SST bytes than `DB::sync_wal`, at equal durability, on one `prefix_len = 8` store with cold prefixes at L1.

That key is VSDB's `META_KEY_WAL_FENCE`. MMDB can issue it as an ordinary delete, so this arm needs no VSDB runtime. `power_loss_discards_only_the_unsynced_suffix_across_wal_rotation` is the control for `sync_wal`: no added record and no sequence. `stats.compaction_bytes_written` is one total. Cold bytes are output files outside the two hot prefixes, plus any file whose smallest key is `[1]`. Leave p99 out of the decision: `sync_wal` appends nothing, so latency is biased toward it. Five interleaved pairs. Accept the extra rewrite only when it exceeds the `sync_wal` arm's max−min. Otherwise leave compaction unchanged. That margin is a planning noise rule. No cold-byte delta has been measured in this tree.

### 4. Section 5, "Writer overlap"

Hypothesis: on a saturated stream, the time between completed groups moves from `W + A` toward `max(W, A)`.

A waiting put still includes its own WAL and its own apply, so foreground p99 can stay flat while that interval falls. `W` and `A` timers are unbuilt. Add them and keep an uninstrumented comparison. Control: today's single stage, same batches, durability, and admitted load. Pass only if the uninstrumented interval drops by more than the control's max−min, each stage is at least one third of that interval, and p99 and peak RSS stay inside the control spread. One third is a planning cutoff for "material", with no measured split behind it. Below that cutoff, stop after the cheaper wakeup change. Keep pre-WAL sequence reservation and admission, drain on rotation and close, and partial append and sync faults.

## Before implementation

Rewrite the section 4 pass rule and the handoff bench loop so `v5_profile` and `runs.csv` remain a smoke rerun and a historical table. Do not choose a memory profile, and do not start writer overlap, from those runs. Units 1–3 can start once the new assertions in edit 2 are the completion checks.

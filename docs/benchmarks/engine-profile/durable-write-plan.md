# Durable writes and automatic WAL rotation

**Experiment completed on x86_64 Linux.** The candidate saved the second
sync during eligible rotations. The six paired rounds missed the foreground
p99 adoption criterion. Retain the existing seal. [Decision and raw measurements](durable-write/README.md)

**Original plan for MMDB 6.0.5, source revision `bbfd67e3`.** Measure whether
sealing the old WAL during automatic memtable rotation delays foreground
writers. If it does, evaluate reusing the sync completed by the same write
group. The production entry point is `DB::write_with_options`. The SQL
coordinator owns its admission, conflict checks, read-root publication and
reply timing.

The mqlite proposal `mmdb-durable-write-report.md` cites two single-connection
trials at 400 scheduled writes/s. Both committed 480 requests. Their
arrival-to-reply p99s were 3.04 and 75.59 ms. Both ended with 252 KiB of
memtable data, zero immutable memtables and zero completed flushes. The first
flush completed at the explicit checkpoint after timing. Use those runs to
study dispatch delay. Evaluate sealing on a workload that rotates the WAL
during measurement. [Downstream samples](https://github.com/rust-util-collections/mqlite/blob/62fb936b88ea4fea8ac979e15f3b64900a7cd3ec/docs/research/results/query-operators/mmdb/samples.jsonl) ·
[Write and flush sequence](../../architecture/writes.md)

## 1. Establish the foreground cost

Use the existing `engine_profile` writer control. Begin with one `WalSync`
writer, the downstream 4 MiB buffer and two immutable slots. Use 200 and 400
writes/s for comparison, then calibrate a sustainable offer on the device.
Repeat with the library's 64 MiB default buffer and with small writes
contending against bulk batches. Size the runs to observe repeated automatic
rotations. [Driver and settings](README.md#workloads) ·
[Library defaults](../../../src/options.rs)

```sh
MMDB_PROFILE_CONTROL=writer MMDB_PROFILE_SYNC=true MMDB_PROFILE_RATE=400 \
  MMDB_PROFILE_BUFFER_MIB=4 MMDB_PROFILE_IMMUTABLES=2 MMDB_PROFILE_OPS=100000 \
  cargo +stable bench --bench engine_profile -- mixed
```

Two short `bench-metrics` runs exercise the boundary before collecting
latency data. With the settings above and `MMDB_PROFILE_OPS=128`, the
x86_64 Linux run completed 128 writes and recorded zero freezes. With the
same 128 operations per scalar stream, `MMDB_PROFILE_CONTROL=mixed` and
`MMDB_PROFILE_BUFFER_MIB=1`, the default 4,096-entry bulk batch completed
alongside the scalar writes and the workers recorded one freeze.

Measure arrival-to-ack and native service time in the ordinary build. To
quantify client backlog, take actual dispatch minus scheduled arrival. The
driver's `issue_delay_ns` starts at the later of scheduled arrival and the
previous acknowledgment. [Timing boundaries](../../../benches/engine_profile/concurrent.rs)

Repeat with `--features bench-metrics`. Take `ThreadWriteProfile` on each
executing worker before the final drain. Inspect `freezes`, `freeze_ns`,
`wal_syncs`, `wal_sync_ns`, immutable waits and L0 stalls. In a diagnostic
pass with one writer, read and reset its counters per call to associate each
freeze with a request ordinal. The WAL timer includes the buffer flush,
and the freeze timer includes taking or creating the next WAL. If freezes
coincide with slow requests, time the old writer's seal directly. Count seal
decisions, eligible cases, syncs attempted, syncs skipped and failures.
Verify calls that reached `File::sync_data` independently of wrapper attempts.
Charge shared work once to the leader. [Thread profile](../../../src/profile.rs) ·
[WAL buffer and sync](../../../src/wal/writer.rs)

Advance when eligible seals account for repeatable foreground delay at a
representative buffer size. If only a forced-rotation run shows a cost,
record the result and stop. Compile additional diagnostic counters and
timers out of ordinary builds. A public `WriteReport`, group IDs or an
event sink would need a separate workload demonstrating which question
requires cross-thread attribution.

## 2. Prototype only the same group's automatic rotation

In `write_batch_group`, record the active WAL generation locally only after
all WAL appends and that writer's `sync_data()` succeed. The buffer is then
empty and the file contents and length are synchronized. If this invocation
also triggers `submit_active`, pass that evidence once to `freeze_memtable`.
Check the outgoing generation while taking its writer. Skipping the seal
requires continuous write leadership and no intervening modification of
that WAL, including across an immutable-capacity wait. The background
worker can prepare a different standby file. A mixed group qualifies when
its actual sync covers every appended byte. [Group execution and rotation](../../../src/db.rs) ·
[Standby preparation](../../../src/db/flush.rs)

Use the ordinary seal whenever the proof is absent. The proof expires with
the group. A later group that only flushes the WAL buffer, or a rotation
started by `DB::flush()` or `DB::close()`, takes the ordinary seal. A
sequence watermark or writeback hint cannot certify the file. Keeping the
evidence on the leader's stack avoids state in each queued request, a
persistent clean flag, global synchronization and disk-format changes.

Publish the committed sequence and complete followers before rotation.
Hold leadership through the rotation. Create or take the new WAL with its
directory entry durable. Retire the old WAL only after the SST installation
is durable in the MANIFEST. [Flush and retirement](../../architecture/writes.md#freeze-and-fifo-flush) ·
[Recovery](../../architecture/recovery.md)

## 3. Prove the boundary on isolated database files

| Case | Required observation |
|---|---|
| A successful `WalSync` group fills the memtable, with a standby WAL and with leader creation | Count syncs on the **old file for this group**. The control performs two, the candidate one. Compare old WAL bytes and length while the flush is paused. Reopen the recovery fixture and verify every key of each acknowledged durable batch. |
| A previously synced writer receives another async group that triggers rotation | Its buffer flush and advisory writeback still require a real seal sync. Also exercise a writer with bytes still buffered. Inject failure at the required seal and check the latched error and the results already delivered to followers. |
| A successful rotation is paused after follower completion | A follower can return and drop its request storage. A queued writer can append to the next WAL only after the current leader releases write leadership. |
| An append fails partway through a group or record, the group sync fails, or next-WAL creation fails | A successfully published prefix returns success. The failed batch and unapplied suffix return errors. A failed group sync stops publication. A post-publication rotation failure preserves acknowledgments and latches the error for later writes. |
| A WAL fence joins a group containing async or `DisableWal` writes | The fence adds zero records and consumes zero sequences. Check the prior snapshot and published view. On reopen, verify the WAL-enabled writes covered by the fence. Exercise `DisableWal` recovery after a durable SST flush. |
| Reopen with a torn tail | Writable recovery replays the valid data-WAL prefix into a new generation. Discarding a torn MANIFEST tail in `WalWriter::open_append_truncated` still requires its own sync. |

Build on the existing flush/fence, standby, truncation and simulated
unsynced-suffix tests. Add focused cases for the saved syscall and the
required fallback. Validate with `cargo +stable test --lib`,
`cargo +stable test --lib --features bench-metrics` and
`cargo +stable test --test crash_recovery` on supported x86_64 and AArch64
targets. [Flush tests](../../../src/db/flush.rs) ·
[WAL writer tests](../../../src/wal/writer.rs) · [MANIFEST reopen](../../../src/manifest/version_set.rs) ·
[Recovery suite](../../../tests/crash_recovery.rs)

## 4. Decide on ordinary builds

Fix the workload, seeds, offer and round count before comparing binaries.
Pair independent control/control and control/candidate runs on the same
device and filesystem. Use paced `WalSync` arrival-to-ack p99 as the primary
metric, with at least 10,000 samples per reported p99. Protect native service
p99, saturation puts/s and batches/s, and the async writer and concurrent
reader/snapshot controls. Check CPU, peak RSS, end-of-run L0 files, pending
immutable memtables and drain time for work shifted out of the timed
interval. Retain raw rounds and identify the platform. [Engine performance criteria](../../ai/review-core.md#performance-priorities) ·
[Matched-run protocol](README.md#workloads)

Adopt only if the correctness cases pass and the primary gain exceeds
control-to-control variation in independent paired rounds. For this
experiment, use a 2% regression allowance on protected ordinary-build
metrics. Compute one-sided 95% bounds across paired rounds for the
candidate/control ratios. The upper p99 bound must be at most 1.02 and the
lower throughput bound at least 0.98. An unresolved bound calls for
retaining the baseline seal. Keep the behavior change in one reversible
patch so restoring unconditional sealing requires no data migration.

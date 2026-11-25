# MMDB v5 implementation and self-test plan

**Starting point** — The joint proposal is committed at `8aacd0f`. This plan
delivers an independently testable MMDB engine and v4 exporter before the
VSDB adapter is ported. An MMDB “shard” in the experiments below is an
ordinary `DB` handle on its own temporary directory.
[Joint release decisions](mmdb-v5-vsdb-v18-improvements.md#decisions-before-the-first-releases) · [Independent reviews and verification](reviews/mmdb-v5-plan/README.md)

## Order of work

| Unit | Expected benefit and pass condition | Existing check → proof still to build |
|---|---|---|
| 1. Write admission | Both transient causes return `Busy`. Zero rejected records, sequence increments or visible rows. One retry after pressure clears applies exactly one batch. | The L0 test asserts the old error. Extend it with before/after storage checks, a stop-threshold case and an accepted-only mixed-group control. |
| 2. Memory accounting | Count each shared evictable charge once, report member pin bytes separately, and observe outstanding immutable generations. This gives a usable budget breakdown. | Existing tests cover entries, pins, overflow and member isolation. Add byte gauges and independent byte-delta assertions across freeze, install and detach. |
| 3. Export and API contracts | Zero missing, extra or different visible rows. Matching writable geometry succeeds and wrong geometry rejects. A successful fence preserves acknowledged WAL data. | Current v4 golden and WAL fault tests run today. Add an eight-byte prefix export, writable round trip, and missing mixed-prefix batch coverage. |
| 4. Workload profiles | Choose a queue profile within the declared RSS/throughput/p99 budgets and identify which part of a mixed write consumes the foreground latency budget. | The existing one-DB smoke benchmark runs today. Build the paced, saturation, multi-DB and residual-WAL arms and timing counters. |
| 5. Performance candidates | Screen for 15% lower small-write p99, 20% less L0 stall time per GiB, or 10% lower batched-read p99 in their respective workloads. | Current code supplies serial/scalar controls. The three candidate paths, their hit counters and their dedicated oracles are pending. See [the comparison protocol](#4-measure-the-limiting-engine-operation). |

Units 1–3 can land independently. Run the memory experiments after unit 2.
Review the mixed-write evidence before designing a second writer stage.
Keep experimental performance changes in separate commits so each can be
evaluated or reverted alone. The file formats, VSDB shard placement and
collection schemas are the compatibility baseline for these units.

## 1. Make write pressure retryable

Change the L0 `no_slowdown` refusal in `DB::write_batch_inner` to return
`ErrorKind::Busy`, matching `FlushQueue::check_capacity`. Keep malformed
batches and invalid open options classified as `InvalidArgument`. Preserve
the existing behavior for ordinary writers and the leader's per-request
immutable-queue check. Document the two transient conditions on
`WriteOptions::no_slowdown` and in the usage guide.

Start by changing the old expectation in `test_write_options_no_slowdown`.
In an isolated store, disable automatic L0 compaction, create three L0
files with explicit flushes, and reject a uniquely identifiable batch.
With other writers idle, require `Busy`, identical active WAL identity and
length, zero change in `snapshot().sequence()`, and no visible rejected row.
Call `DB::compact()` and verify the L0 count falls below the configured
threshold. Retry once and verify the expected sequence increment and values
after reopen. Exercise the slowdown threshold,
the stop threshold and the parked immutable queue separately. Use the existing
flush hook to hold pressure. For a mixed group with an admitted neighbor,
compare the WAL records, sequence advance and recovered rows with an
accepted-only control. The neighbor legitimately changes both WAL and sequence.
Keep a malformed request and an invalid open configuration as negative controls.
Success means all three pressure cases pass and every rejected mutation has
zero storage effects. The baseline L0 test currently passes by expecting
`InvalidArgument`. Keep public calls and file observations in the integration
test. Put assertions that need the current WAL number, leader or pause hook
in the existing in-crate test module. Record refusal and caller completion
as separate events in the mixed-group test. The leader can set a member's
error before the group releases its waiters. Use barriers to establish
event order. A stronger early-return guarantee would require its own
ownership design because a queued request points into the caller's stack.
[Write admission](../../src/db.rs) · [Flush capacity](../../src/db/flush.rs) · [Integration tests](../../tests/integration.rs) · [Current error contract](../usage.md#errors)

## 2. Account for the memory that MMDB can observe

Expose additive measurements through the production pool and DB interfaces
so the adapter can use them. Keep deterministic maintenance drains and
worker-pause hooks under test configuration. Document the unit, owner,
lifetime and approximate nature of each gauge.

| Owner | Proposed measurements and sources |
|---|---|
| One `BlockCachePool` | Configured capacity and approximate evictable charge in bytes for each of the segmented and overflow stores. Read Moka's weighted sizes. A caller sharing one pool across many DBs counts the pool once. |
| One `DB` | Member-local L0 pin payload bytes from `BlockCache::pinned_bytes`. Report active memtable bytes, immutable count and immutable bytes from one pinned `SuperVersion` and `MemTable::approximate_size`. |
| Process | Measure peak RSS separately. Old views and caller-held `PinnedSlice` objects can retain evicted blocks or whole generations. Cache charges and writer-buffer estimates cannot determine their full process footprint. |

Preserve the existing `block-cache-usage` **entry count**. Sample outside
the timed write loop. Check zero capacity, normal and over-segment entries,
replacement, same file number under different members, and one member
closing while the other still reads. At quiescence, drain Moka maintenance
in tests before comparing weighted bytes. Use non-evicting test capacities
and record each input vector's actual `capacity().max(1)` before insertion.
Use `len()` for member pins. Compute expected deltas from these inputs.
Give two members the same file number and offset but different payloads,
replace one entry, then detach one member. Pool-wide charge must equal the
sum of surviving evictable charges once, with the other member's values and
pin charge intact. A caller-held `PinnedSlice` must still read correctly
after detach without being counted as a member pin. For approximate
memtable gauges, park the oldest flush, observe the expected immutable
count and a positive byte estimate, release it, then wait for durable
installation and check the count and charge return to zero. Peak RSS
is a high-water mark and can stay high after all those gauges fall.
Keep normal and overflow capacities distinct in this unit. An aggregate
cap would need a separate eviction experiment that preserves oversized-block
admission. [Pool and pins](../../src/cache/block_cache.rs) · [Memtable accounting](../../src/memtable/mod.rs) · [Read views](../../src/db/read.rs) · [Shared-cache tests](../../tests/shared_cache.rs)

## 3. Prove the engine boundary for the adapter

- **Writes and fences.** Exercise one `WriteBatch` with two eight-byte
  prefixes in one DB, including a range wipe followed by replacement rows.
  Check all-or-none visibility and operation order under snapshots. A
  `WalSync` success must survive simulated loss of unsynced WAL bytes.
  `DB::sync_wal()` after `WalAsync` fences preceding writes across rotation
  without adding a record or consuming a sequence. Reuse existing fault
  injection for failed append/sync and test both reopen modes.
- **Reads.** Compare `get`, `get_pinned` and `contains_key` for hits,
  misses, empty values and covered keys at the same snapshot. Check pins
  held through flush, compaction, cache eviction and close, typed iterator
  errors, and prefix/range bounds. These APIs already exist. Add coverage
  only for a missing contract.
- **Export.** Extend the exporter golden tests with a v4 shard using an
  eight-byte prefix, known level count, flushed SSTs, residual WAL and
  deletions. Compare row count and the ordered length-delimited key/value
  stream or digest, checking final iterator status. Reopen the destination
  *writable* with the expected geometry, write, close and read it again.
  Wrong writable geometry must fail. Keep source files intact, preserve
  a destination created by a competing publisher and verify that a failed
  source scan leaves the destination unpublished. The exporter transfers
  visible rows into a fresh sequence history. Use records chosen so that
  skipping residual WAL replay, dropping an empty value, or ignoring a
  deletion changes the independently specified expected stream. Compare a
  pre/post inventory of the source file names and contents. For a source
  built with three levels and an eight-byte prefix, invoke the exporter with
  `SOURCE DEST 3 8`. Test legal but mismatching writable options `(4, 8)`
  and `(3, 2)` independently on the exported store, expect
  `InvalidArgument`, then perform the correct writable reopen. Run the
  publication test with `CI=1` on Linux so an unsupported exclusive rename
  cannot count as a successful export. The existing golden fixture uses a
  two-byte prefix and opens the result read-only.

[Batch and encoding tests](../../src/types.rs) · [WAL fault tests](../../src/db/flush.rs) · [Pinned and geometry tests](../../tests/integration.rs) · [Exporter](../../tools/v4-export/src/main.rs) · [Exporter golden tests](../../tools/v4-export/tests/golden.rs)

## 4. Measure the limiting engine operation

Extend the existing mixed profile with reproducible inputs. Start each trial
in a fresh process. Record revision, architecture, filesystem and compression,
block size, seed, key/value distribution, batch shape, WAL mode, cache
capacity and worker counts. Keep a warm scalar read and a single writer as
controls. Run the tests separately from the timed trials.
[Current mixed profile](../../benches/v5_profile.rs) · [Point-operation baseline](../benchmarks/point-ops/runs.csv)

**What runs today.** `v5_profile` uses one DB, a 256 KiB write buffer,
two getters and two single-put writers. Each worker measures 10,000 calls,
issuing the next when the last returns. It prints per-class p99/p999, flush
counts and, on Linux, process peak RSS. It ends with a flush. Use it to
check the profiling setup. The matched-load, multi-DB and residual-WAL
experiments below require a new or extended driver. The old point-operation
CSV provides engine throughput context only.

| Decision | Experimental arms | Outputs for the decision |
|---|---|---|
| Choose a shard memory profile | Use 16 DB directories and one shared pool. Drive one hot DB, then all 16 concurrently. Sweep immutable limits 1, 2 and 4 at fixed buffer size to isolate queue behavior, then vary per-DB buffer size to compare profiles at the same total memory budget. Exercise many flushes. | Admitted user bytes/s, foreground p99, stall time, sampled component bytes, peak RSS and residual-WAL reopen cost. Select the lowest-latency profile that meets the throughput and memory budget. Report the one-hot and all-active cases separately for VSDB. |
| Attribute small-write latency | A stream of scalar foreground puts and a stream of large multi-entry batches first share one DB, then use distinct DBs. Add a scalar reader, a reader creating fresh snapshots and a reader reusing one snapshot. Run `WalAsync` and `WalSync` separately. Vary payload entropy and include a filesystem with compression disabled. | Per-operation-class throughput and p99, group sizes/bytes, queue wait and wakeups, WAL append/flush/sync time, memtable apply and rotation time. Record samples with benchmark-only or opt-in instrumentation and compare to an uninstrumented run. |
| Identify L0 debt | Preload several L1 files and cold eight-byte prefixes. Overwrite two hot prefixes across many flushes. Compare the former one-byte sentinel delete with the native WAL fence, at equal durability. | Peak L0 files, time in slowdown/stop, input/output SST bytes and foreground read/write p99. Attribute rewritten cold bytes separately from total compaction bytes. Capture the L1 layout and active snapshot count. |

**Run the comparison.**

1. Pin the baseline and candidate revisions. Use the same driver build
   options, seeded rows, LSM starting layout, hardware and I/O budget for
   each pair. Predeclare the primary load, WAL mode, memory ceiling in
   bytes, throughput floor and p99 ceiling. As a reproducible pilot for the
   joint proposal's 2 GiB process scenario, reserve 1 GiB for MMDB RSS and
   leave 1 GiB for the caller. Calibrate a fixed offered rate at 70% of the
   control's sustainable rate. Require the workload to complete all offered operations in each class
   after drain with zero unexpected rejections and no growing backlog, retain at least 95% of control saturation
   throughput and stay within 105% of control p99. These are screening
   settings. Record the actual deployment envelope before recommending a
   VSDB configuration. For the 16-DB experiment, keep
   all handles open in both arms. Count both configured pool capacities
   once and budget separately for member pins and write buffers. Compare
   observed process peak RSS to the engine's share of the deployment budget.
   Preallocate histogram storage equally in both arms. Sample current RSS
   after preload and record whole-process peak RSS separately, including
   the harness and setup allocations.
   A concrete first writer trace uses 128-byte scalar puts alongside batches
   of 4,096 entries with 256-byte values, first on one DB and then on two.
   Record the encoded batch size. Use a distinct key stream per class, a
   fixed seed, and identical counts and arrival times across each pair.
2. For latency, replay identical **offered** arrival timestamps and bytes.
   Record scheduled arrival, entry to the DB call and acknowledgement.
   Report arrival-to-ack p99 as the primary latency, service p99 separately,
   and offered, completed, rejected and outstanding work per class. Drain
   outstanding requests. If retries are part of a workload, time them
   from the original arrival and report attempts separately. A fast `Busy`
   or a growing queue cannot buy a p99 improvement. For saturation, use fixed closed-loop worker counts and
   compare successful logical bytes/s at the registered memory and latency
   ceilings. Compare WAL modes separately.
3. Warm the store to its intended shape. Record measured flushes, L0→L1
   merges and stalls during the timed window. Prepare a separate, known
   residual WAL for reopen timing. A gracefully closed or flushed DB
   exercises a different recovery path. For disk-backed reads, verify
   actual storage reads with device or filesystem counters. Reducing only
   the engine cache can leave every page in the OS cache. For L0 accounting,
   `stats.compaction_bytes_written` is total output bytes. Identify cold-only
   input SSTs in the fixture, track which files the job consumed and which
   files it physically wrote. A trivial move writes zero new SST bytes.
   Use separate fixtures for two hypotheses. To isolate the sentinel, put
   cold L1 files between the one-byte sentinel and a single hot prefix,
   outside that prefix's own span. To test gaps in L0 selection, update two
   hot prefixes on either side of cold files with the native fence in both
   arms. Keep fence cadence identical. Compare a synchronous sentinel
   delete with a native WAL fence following the same ordinary writes.
   For mixed files report logical cold-prefix bytes emitted separately from
   compressed file bytes. For the L0 stall metric, sum foreground time
   spent in L0 slowdown/stop waits and divide by successfully admitted
   logical GiB. Track immutable-queue waits separately. An arm with zero
   baseline L0 wait cannot establish a relative stall reduction.
4. Calibrate by pairing the unmodified baseline with itself (A/A) using
   the same repetitions, layout and load as the comparison below. Then run
   at least five fresh-process baseline/candidate pairs in balanced AB/BA
   order. Keep raw per-run results and sample counts, compute p99 per class
   **within each run**, then report the median and range of paired deltas.
   Start with at least 10,000 completed samples per class for p99 (about
   100 observations in the upper 1%). Extend sampling through several
   independent flush and compaction cycles if those events determine the tail.
   Record whether the workload is accumulating debt by comparing start/end
   L0 files and pending flushes. Check instrumented against uninstrumented
   runs. `DB::get_property` acquires `DB::inner`.

**Proposed benefit targets.** The following are minimum worthwhile effects
for planning on a representative workload. They do not predict a speedup.
The larger changes to write ordering and compaction need larger wins than a
bounded read optimization. Replace a target with a documented deployment
requirement **before** collecting candidate results.

| Decision | Primary pass condition | Control and guardrail |
|---|---|---|
| Shard memory profile | Select a queue depth and buffer size meeting the predeclared peak-RSS ceiling, completed bytes/s floor and foreground p99 ceiling in both one-hot and all-active arms. Among eligible profiles, minimize the worse of the two per-arm p99 ratios to their respective controls. | Use the same total budget. Retain the current profile if it qualifies and the alternatives are indistinguishable. If none qualifies, report that result to VSDB. |
| Writer overlap | At least **15% lower foreground small-put arrival-to-ack p99** versus the current or already-improved serial writer under large same-DB batches at matched offered load. | Keep the achieved bulk-throughput floor, RSS ceiling and durability. Protect single-writer and warm-read p99. |
| Parallel L0 compaction | At least **20% less foreground time stalled per GiB of admitted logical writes** under sustained L0 pressure versus the serial job, with the same total worker/I/O budget. | Require actual L0 stalls in the control and an eligible L0 split in the candidate. Track foreground read/write p99, SST output bytes per logical byte and peak RSS. |
| Grouped point reads | At least **10% lower p99 per 32-key request** versus 32 scalar pinned reads on the same snapshot and key trace in the preselected cache regime. | Return the same ordered results and errors, enforce the pin budget, and report 1-key and 8-key results. Protect singleton p99. |

For the performance candidates, require the primary median gain to clear
both its target and the observed A/A noise band. For a lower-is-better
metric use `100 × (baseline − candidate) / baseline` within each pair.
For throughput reverse the sign. Use the largest absolute
paired A/A delta for that metric as a conservative screening band. Require
the expected direction in at least four of the first five pairs. Use a
proposed **5% maximum regression** in control p99, throughput or SST
write amplification where applicable. If control noise is too large to
distinguish 5%, or an interval is dominated by too few tail observations,
extend the runs and repeat. Report inconclusive evidence as a deferral.
Any visibility, durability, ordering or reopen failure rejects a candidate
regardless of its timing.

## 5. Promote only the candidates supported by the profiles

| Candidate and prerequisite | Smallest useful change | Correctness and stop condition |
|---|---|---|
| **Writer overlap** if both WAL and memtable application occupy material group time | First compare cheaper batch handoff/wakeup changes. If warranted, try one FIFO WAL stage and one ordered apply stage with bounded in-flight bytes and groups. The ideal interval changes from `W + A` towards `max(W, A)` for comparable group sizes. | Reserve sequence space, same-key admission and range-deletion capacity before WAL append. Admit a legal oversized first batch. Preserve request lifetime and publish only complete ordered batches. Drain on rotation, flush barriers, ingest and close. Pause between WAL and apply, mix all durability modes, inject partial append/sync failures and check recovery and per-request outcomes. Drop the candidate if the foreground tail or memory cost absorbs the gain. |
| **L0 parallel compaction** if backlog controls latency | Keep the full L0 claim and its closed set of L1 inputs. Split one job at suitable L1 user-key boundaries under a fixed worker budget across the active DBs. Record peak concurrent workers and compare inside the same CPU allocation. Fall back to serial execution when boundaries are insufficient. Evaluate prefix-aware flush cuts separately with bounded fan-out and a minimum output size. | The existing parallel-execution test covers L1→L2. Add an L0-specific hook or counter scoped to this job and prove at least two ranges executed. Keep every version of a user key together, clip crossing tombstones, preserve snapshot/filter visibility, revalidate input and output overlaps and install once. Compare serial and parallel results after concurrent flush and reopen. More files can trigger the 4/8/12 thresholds earlier. Record stalls and total I/O before accepting cuts. |
| **Grouped point reads** if independent-key fan-out beats scalar pinned reads | Prototype a bounded group on one pinned read view and one checked sequence. Resolve eligible keys by SST/block, restoring input order and duplicate slots. | Compare 1, 8 and 32 keys against `get_pinned` on the same snapshot. Cross clustered and scattered keys with warm engine cache, OS-cache-only hits and verified device reads. Count per-request table/block probes as well as elapsed time. Include absent/empty values, foreign snapshots, range tombstones beyond point bounds, racing ingest/flush, typed errors and pin retention. Use one snapshot for the full scalar control request and release results at matching points. Verify the candidate actually groups probes for keys sharing blocks. Advance the API only with a reproducible request-level win. End-to-end VecDex evaluation follows in VSDB. |

[Group admission](../../src/db.rs) · [L0 task construction and installation](../../src/compaction/leveled.rs) · [Point lookup](../../src/db/read.rs) · [Sorted ingest](../../src/db/ingest.rs)

## Self-test and handoff

These commands exercise existing baselines from the MMDB root. A filtered
command must report the named test as run. Add each new oracle to the focused
checks when its implementation lands. Run the full [repository gate](../ai/commit-protocol.md#final-repository-gate)
after the last accepted behavior change. Use the installed stable toolchain.
Record actual architecture coverage and run the correctness gates on
x86_64 and AArch64 before declaring both supported targets verified.

```sh
cargo +stable test --test integration test_write_options_no_slowdown -- --exact
cargo +stable test --lib db::flush::tests::newest_immutable_wins_while_oldest_flush_is_parked -- --exact
cargo +stable test --lib cache::block_cache::tests::oversized_block_above_segment_share_is_retained_and_invalidated -- --exact
cargo +stable test --test shared_cache
cargo +stable test --lib db::flush::tests::power_loss_discards_only_the_unsynced_suffix_across_wal_rotation -- --exact
cargo +stable test --test crash_recovery
CI=1 cargo +stable test --manifest-path tools/v4-export/Cargo.toml
cargo +stable test --test integration v5_manifest_geometry_precedes_repair_and_read_only_adopts_it -- --exact
cargo +stable test --lib compaction::leveled::tests::test_execute_compaction_io_actually_parallelizes_with_max_subcompactions -- --exact
cargo +stable test --test proptest_db
```

The existing short single-DB profile can be rerun before extending it. Use
a fresh process for each immutable limit so the Linux peak RSS measurement
belongs to one run.

```sh
for depth in 1 2 4
do
    MMDB_PROFILE_IMMUTABLES="$depth" cargo +stable bench --bench v5_profile
done
```

**Baseline on 2026-11-25.** The 17 repository-gate commands passed on
x86_64 Linux at `8aacd0f`, including formatting, Clippy, rustdoc, root
tests and exporter tests. There were 571 top-level passing test cases. The
read-only suite ran its syscall probe through `strace`. During plan review,
all ten focused commands above selected real tests and passed. Each of the
three smoke runs completed 20,000 reads and 20,000 writes. These observations
verify the baseline procedures. An AArch64 result and candidate performance
measurements are pending. [Review and run record](reviews/mmdb-v5-plan/README.md#what-was-checked-during-this-review)

For the VSDB port, hand off the accepted MMDB revision, property names with
units and ownership, the writable-reopen/export results, the raw workload
results with settings and run-to-run spread, and a disposition for each
performance candidate. Physical checkpoints, external value files and
VSDB collection layout changes can follow a separate decision once the
caller supplies a concrete requirement.

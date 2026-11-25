# Independent implementation milestone reviews

The [implementation plan](../../mmdb-v5-execution-plan.md) defines the
release and performance criteria. Reviewers inspected checkpoint snapshots.
The implementation team checked the reports against the corresponding
source, ran reproductions and evaluated performance separately.

## Review records

Each checkpoint received independent reviews from Grok Build, agy, Claude
Code and OpenCode, using their locally configured default models. Each
reviewer received the same English brief for that checkpoint. Separate
read-only mounts exposed the two repository snapshots and excluded other
reviewers' workspaces and reports. Resumed sessions supplied a final answer
where the first headless run ended after inspection. The archive contains
the complete briefs, exact checkout revisions and the twenty completed
reports. It excludes tool transcripts. Source links within the original
reports retain their review-workspace paths.
[Compressed UTF-8 milestone records](milestones.json.gz)

To read a report from this directory, select the checkpoint and tool by
name. The checkpoint names are `contracts`, `profiles`, `l0`, `grouped`
and `grouped-public`.

```sh
python3 -c 'import gzip,json; print(json.load(gzip.open("milestones.json.gz", "rt"))["checkpoints"]["l0"]["reports"]["grok"])'
```

## Decisions verified against code or tests

| Checkpoint | Review claim | Resolution and evidence |
|---|---|---|
| Contracts | agy found that transient L0 refusal could leave eligible work asleep and that the exporter only checked a newly added row after writable reopen. Grok and Claude Code accepted the admission/WAL ordering. | L0 backpressure signals compaction before returning `Busy`. Tests cover eligible work after a claim clears, unchanged WAL/sequence for the rejected write and an independently admitted group member. The exporter now checks the entire post-flush stream and both eight-byte prefixes. Existing directories, a dangling Unix destination symlink, CRC failures and sibling staging cleanup are checked at the CLI boundary. [Write path](../../../../src/db.rs) · [Export tests](../../../../tools/v4-export/tests/golden.rs) |
| Contracts | agy questioned invalidation of cross-store replacements and requested prefix scans during recovery. OpenCode proposed returning `Busy` to ordinary writers and reported a second geometry check was missing. | Both replacement directions now have direct invalidation/detach coverage. Pool gauges describe configured store capacities; their weights and member-local pin payloads have distinct owners. Blocking admission continues to wait or drain. Both wrong writable geometries compare the complete destination file inventory. Added snapshot prefix scans and a separate later torn-async-WAL image. The power-loss oracle still uses observed durable offsets. [Cache tests](../../../../tests/shared_cache.rs) · [Recovery tests](../../../../tests/crash_recovery.rs) · [Fault injection](../../../../src/db/flush.rs) |
| Profiles | Claude Code and agy identified unequal buffer-envelope controls, shorter-lived saturation streams, sampled gauges limited to one configuration, and insufficient observations for bulk p99. Grok requested dispatch-delay and reopen-memory attribution. | Pair fixed-buffer and fixed-envelope cases with their own controls. Run closed-loop streams for a common window, confirm the sustainable rate with a paced workload and qualify percentile sample counts. Record issuance delay separately from call service time; separate recovery from later verification memory. A diagnostic sampler can itself acquire `DB::inner`; compare sampled and unsampled runs. [Driver and replay](../../../benchmarks/engine-profile/) |
| Profiles | OpenCode claimed workers share one thread-local record and that the L0 no-pressure path inflates stall time. | Each stream has its own OS thread. The leader owns group timing; waiting may overlap that work. Only the actual slowdown sleep and stop branch enter the L0 timers. Report the per-thread sum, admitted logical bytes and per-DB/file scope of each counter. Selected file lengths do not measure device traffic. [Thread profile](../../../../src/profile.rs) · [L0 profiler](../../../../src/compaction/profile.rs) |
| L0 prototype | Grok, agy and Claude Code requested an unconditional parallel-branch test and a bounded cross-DB worker comparison. OpenCode disputed point-file pruning and the range counter's meaning. | The prototype test now parks all expected range builders simultaneously, allowing a racing flush before release, and compares snapshot and reopened streams across both split boundaries. Half-open subranges tile the keyspace; point files intersect the ranges containing their keys and the whole claim supplies tombstones independently. Instrumentation distinguishes range starts, completed multi-target jobs and simultaneous range execution. Peak L0 count includes the opening version. The one-DB trial measures fixed CPU allocation and per-DB peak concurrency; a cross-DB permit design remains open. [Patch, paired results and disposition](../../../benchmarks/l0-profile/) |
| L0 profiler | agy and Grok flagged fence time and possible end-of-run compaction debt. | Fence service has a separate sample class. Initial cold files retired, logical cold rows emitted and new SST bytes are separate quantities. The raw trials include both timed-window and closing counters. All 30 archived pairs ended with equal L0 counts and rewrite counts within each pair. The longer test cleared the stall target but could not certify the read-tail guardrail, so the production L0 merge stays serial. [Profile source](../../../../benches/l0_profile.rs) · [Raw paired results](../../../benchmarks/l0-profile/runs-65536.csv) |
| Grouped prototype | agy identified thrashing across overlapping SSTs and duplicate-key probes. Grok, Claude Code and OpenCode challenged periodic eviction as a sufficient device-read baseline and requested like-for-like ownership and metadata. | The revised candidate resolves one pinned view and sequence, coalesces duplicate slots, and keeps two request-local file cursors. The scalar control uses one snapshot, compares the same trace and retains results to a matching point. Trials record the fixture and binary identities, distinguish engine/page/device regimes, and classify actual device reads. A foreign snapshot is rejected by the same resolver used by scalar reads. [Grouped candidate and evaluation](../../mmdb-v5-execution-plan.md#5-promote-only-the-candidates-supported-by-the-profiles) |
| Grouped public candidate | Claude Code identified selection bias if p99 used only I/O-positive requests from each arm. OpenCode suspected duplicate hidden keys could inflate read statistics and that closed-handle precedence was inverted. | Use all completed requests for the primary within-regime percentile, qualify device evidence separately, and require sufficient request count for the decision. A duplicate visible value records one charge per returned hit, while hidden duplicates produce no result charge. The public candidate calls `check_usable` before testing the cap. Dedicated tests cover hidden duplicate slots and closed handles. |
| Grouped public candidate | Grok and agy traced a later corrupt key after earlier visible slots. agy also found that a probe past a file's final partition could prematurely evict a request cursor. | Treat these as acceptance checks. A group returning an error should charge zero returned bytes. A partition miss should leave the two reusable file cursors intact. Verify both on the corrected candidate, compare the actual release read path, and keep the device-read counter as a request-level qualifier. The final API disposition belongs to the [execution record](../../mmdb-v5-execution-plan.md#implementation-record). |

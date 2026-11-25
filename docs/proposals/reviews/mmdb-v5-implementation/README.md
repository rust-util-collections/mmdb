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

## Supplementary review of benchmark evidence tools

Four additional reviews inspected the Python measurement tools and reproduction instructions at `5127665` against `90b220e`. All four received one English brief specific to these tools and worked in separate read-only checkouts. These records supplement the 28 implementation and release reports. The saved binaries and observations are documented with their benchmark results. [Full reports and exact brief](evidence-tools.json.gz) · [Engine profile results](../../../benchmarks/engine-profile/)

| Reviewer | Finding and verification |
|---|---|
| Claude Code | A memory-tail run with excessive backlog and low RSS could receive `within-1-GiB`. Commit `83f1b30` sets the decision status to `offered-load-unqualified` when any of the four arms misses the offered load. Measured RSS maxima and the factual budget columns remain available even if the load fails. The grouped-read output now names the Linux read-byte counter explicitly. [Memory status tests](../../../benchmarks/engine-profile/test_summarize.py) · [Grouped output fields](../../../benchmarks/grouped-reads/results/README.md) |
| Grok Build | Confirmed that gains use the fresh A/B control and noise uses the two independent A/A processes. Commit `452a621` identifies the conditional latency as `linux_read_ge_4kib_p99_us`. The primary p99 includes every request. `linux_read_bytes_per_request` divides by all requests in each A/B arm. Grok also asked about the actual batch width. Commit `83f1b30` verifies emitted logical bytes as well as operation counts. Each complete memory-tail process must report 2,785,280,000 batch bytes for 320,000 batches of 32 entries, 2,880,000 small-put bytes and zero read bytes in these write-payload counters. [Workload and counters](../../../../benches/engine_profile/concurrent.rs) · [Device metrics](../../../benchmarks/grouped-reads/summarize.py) |
| OpenCode | Its claim that coverage excludes A/A rows was disproved. A synthetic complete cell with a 95.1% qualifying fraction in one A/A arm and 100% in both A/B arms reported `min_io_qualified=0.951` and 4,096 Linux read bytes per request in each A/B arm. Removing one A/B arm made the summarizer reject the cell. [Grouped summarizer](../../../benchmarks/grouped-reads/summarize.py) |
| agy | Confirmed the 30 MiB envelope, the predeclared per-stream rates of 251 and 1,381, and 20,000 completed small writes and reads in each arm. Recalculation from the integrated grouped-read CSV gives a 37.55% median full-trace p99 reduction across five A/B pairs and a 3.87% largest absolute A/A difference. [Memory-tail driver](../../../benchmarks/engine-profile/run.py) · [Raw integration results](../../../benchmarks/grouped-reads/results/final/integration-release.csv) |

### Confirmation of the evidence-tool corrections

A further four independent reviews checked `5127665..83f1b30`. Each verified that the focused memory latency repeat separates offered-load qualification from factual RSS, that the child process reports the expected logical writes, and that the grouped-read labels describe the measured populations. The archive preserves both rounds under `evidence-tools` and `evidence-confirmation`, giving eight supplementary reports alongside the 28 implementation and release reports. [Both supplementary checkpoints](evidence-tools.json.gz)

- **Claude Code and Grok Build** confirmed the load and byte checks. A direct check with a qualified schedule returned `within-1-GiB` at exactly 1,048,576 KiB and `over-1-GiB` one KiB higher. The regression for a failed schedule retains the same measured RSS columns on either side of that boundary. The raw grouped records already expose `peak_pin_bytes` and `pin_budget`. [Memory tests](../../../benchmarks/engine-profile/test_summarize.py) · [Grouped results](../../../benchmarks/grouped-reads/results/)
- **OpenCode** confirmed the guard and both Linux read labels. Its incidental question about a second median was checked against the calculation. Each A/B process contributes one ratio of total storage bytes to completed requests. Each arm's displayed value takes one median across five processes. [Grouped summarizer](../../../benchmarks/grouped-reads/summarize.py)
- **agy** confirmed the focused repeat and observed that the original `memory` suite keeps `peak_kib` as a numeric ceiling even when a synthetic paced run exceeds its backlog allowance. The original suite reports schedule qualification separately. Every paced process used in its published matrix met the offer, across all twelve profile and placement rows. The stricter `memory-tail` guard reports `offered-load-unqualified` on both the latency and RSS decision rows when any arm fails. [Original memory matrix](../../../benchmarks/engine-profile/memory-summary.csv) · [Profile guide](../../../benchmarks/engine-profile/README.md)

## Final release review

Four fresh reviews covered the release candidate at `90b220e`, with
particular attention to the executable and tooling changes since `223d058`.
The earlier integrated-source round reviewed `736b87e`; the development
rounds covered five smaller checkpoints. Together the three archives hold
**28 completed reports**. The OpenCode entry below is the final response
from its resumed session. All four reviewers found the final delta
satisfactory on static inspection.
[Final release reports and English brief](release.json.gz) · [Native and AArch64 validation receipts](../../../benchmarks/engine-profile/validation.json)

| Reviewer | Final check |
|---|---|
| Grok Build | Checked the reference/candidate RSS statuses, grouped read slot/byte semantics, end-of-run memory observation, and per-stream pacing. Its concern about mixing deadlines with a fixed arrival rate is addressed at option parsing: `Config::load` rejects simultaneous nonzero `RATE` and `SECONDS`. [Profile configuration](../../../../benches/engine_profile/main.rs) |
| agy | Confirmed successful-group byte accounting, failure precedence, request-local cursor clearing, refusal wakeups, export publication, and the updated feature-isolated gates. The same explicit configuration check settles its remaining manual-mode hypothesis. [Grouped lookup](../../../../src/db/read.rs) · [Feature and test gates](../../../ai/commit-protocol.md#final-repository-gate) |
| Claude Code | Confirmed that both reference repetitions influence the reference memory status, that the candidate has its own status, and that a very short multi-stream trial includes each issuer's newly arrived request. [Memory decisions](../../../benchmarks/engine-profile/summarize.py) · [Paced checks](../../../benchmarks/engine-profile/paced_load.py) |
| OpenCode | Confirmed the corrected late-error counter, cursor lifetime, and independent feature checks. For device evidence the benchmark counts a request only when the process records at least 4,096 storage bytes; qualification requires that for at least 95% of all requests. The reported p99 uses the full request population. This measures request-level storage activity and leaves individual key/block residency unresolved. [Benchmark](../../../../benches/grouped_reads.rs) · [Qualification](../../../benchmarks/grouped-reads/read_evidence.py) |

## Integrated-source review

After the five development checkpoints, the same four CLIs independently
reviewed the integrated snapshot `736b87e` against `4acd209`. The four
source reviews are archived separately from the twenty milestone reports.
The source review preceded the last evidence and documentation updates.
[Integrated-source reports and English brief](integrated-source.json.gz)

| Tool | Checked decision |
|---|---|
| Grok Build | Confirmed group visibility and all-or-error accounting, typed admission refusals, per-pool memory ownership, export/reopen behavior and serial L0. Its device-qualification caveat is workload-dependent. The runner rejects insufficient per-request read evidence and uses all requests to calculate p99. [Read evidence](../../../benchmarks/grouped-reads/read_evidence.py) |
| Claude Code | The memory summarizer's one-sided RSS status was ambiguous. Commit `a5560e8` reports reference and candidate ceilings separately and tests a reference exceeding the ceiling. Under the CI command `cargo check --benches --features bench-metrics`, Cargo emitted a `grouped_reads` compiler artifact with resolved features `bench-metrics`, `default`, and `test-utils`. The root dev-dependency supplies the last feature. [Summary tests](../../../benchmarks/engine-profile/test_summarize.py) · [CI](../../../../.github/workflows/ci.yml) |
| agy | Accepted the integrated storage, grouped-read failure accounting and WAL recovery contracts. Identified three additional validation gaps: the minimum outstanding-work tolerance on short aggregated memory streams, the final interval of the optional 50 ms memory sampler and the standalone library/rustdoc coverage of `bench-metrics`. Calibration and summaries now allow each issuing stream its just-arrived request. The sampler merges the existing post-worker observation into its maxima. CI, Make and the agent gate include isolated diagnostic-feature library Clippy and rustdoc checks. [Workload driver](../../../../benches/engine_profile/concurrent.rs) · [Repository gate](../../../ai/commit-protocol.md#final-repository-gate) |
| OpenCode | `point_lookup` and the grouped path both retain a view before checking handle state and validating a snapshot. An empty group resolves the supplied snapshot. The read harness enables its block cache only for warm mode. L0 CSVs distinguish the 24 starting **L1** files, selected bytes, completed range-build output, and actual concurrent ranges on one DB. Paced end-to-end latency starts at scheduled arrival. [Read path](../../../../src/db/read.rs) · [Read harness](../../../../benches/grouped_reads.rs) · [L0 fields](../../../benchmarks/l0-profile/) |

The short-queue regression failed on the previous allowance and passed
with the shared check. At the recorded 10,000 operations per foreground
stream, the memory Batch allowance is still 1,600 requests and the mixed
Batch allowance is still two. A 26 ms sampled 16-DB run captured the final
10,481,664 active bytes in its maxima before the 50 ms periodic interval.
Historical records without `sampled_after` retain their original sampling
coverage. Both new diagnostic-feature checks passed locally on x86_64.
[Backlog tests](../../../benchmarks/engine-profile/test_paced_load.py)

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
| Grouped public candidate | Grok and agy traced a later corrupt key after earlier visible slots. agy also found that a probe past a file's final partition could prematurely evict a request cursor. | The promoted implementation (`c97bb5c`) posts its read-byte total once after all keys succeed. It returns from past-partition misses before changing either cursor. The regression tests cover late corruption, hidden duplicates and an interposed third SST. Both one-SST and overlapping-SST production release checks passed. Device I/O evidence qualifies the request population; the p99 comparison uses all requests. [Regression tests](../../../../tests/grouped_reads.rs) · [Published results and limits](../../../benchmarks/grouped-reads/) |

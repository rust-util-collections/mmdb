## Assessment

The plan is appropriately staged and MMDB-local. Its expected benefits are mostly clear: a uniform retry contract, attributable memory, a verified v4-to-v5 boundary, and evidence-gated optimization. Several foundations already exist: `DB::write_batch_inner` currently returns `InvalidArgument` before enqueueing an L0-pressured write, while `FlushQueue::check_capacity` returns `Busy` (`mmdb/src/db.rs:2900`, `mmdb/src/db/flush.rs:68`). The queued-write test already exercises `Busy`, and a parked-flush hook exists (`mmdb/src/db/flush.rs:416`). WAL power-loss, oversized-block retention, and parallel-compaction-path tests also exist.

What is missing is chiefly new evidence, not wholesale redesign. `tests/integration.rs:780` currently asserts the old error and nothing about rejected-write atomicity or retry. `tests/shared_cache.rs:72` checks member isolation, not byte gauges. The exporter golden test checks two rows through `open_read_only`, not geometry or writable reopen (`tools/v4-export/tests/golden.rs:4`).

## Ranked edits

### 1. §§4–5: Register decision budgets and hypotheses before measuring

**Benefit:** prevent post-hoc selection and unjustified claims based on the idealized `W + A → max(W, A)` model. Define deployment-derived p99, throughput, and memory budgets plus a minimum operationally meaningful improvement for each candidate; label these **planning budgets**, not empirical results. The point-ops CSV is historical context, not a latency baseline for this machine or driver (`docs/benchmarks/point-ops/runs.csv`).

**Baseline/control:** immutable commit before each candidate, identical serialized scalar reader/writer and instrumented/uninstrumented controls; same admitted request stream. Interleave at least five fresh-process pairs as planned.

**Procedure/pass/defer:** record per-pair candidate deltas for foreground p99, admitted throughput, memory, and controls. Pass only when the paired improvement exceeds observed baseline spread, meets the predeclared budget, and controls show no reproducible regression; otherwise defer or retain serial code. “Material group time,” “backlog controls latency,” and “request-level win” need equally explicit entry criteria. The existing parallel-compaction test proves branch execution only (`src/compaction/leveled.rs:2492`), not performance improvement.

### 2. §4: Replace the current driver before comparing profiles

**Benefit:** make the results answer the stated questions. `benches/v5_profile.rs:37` launches **two writers and two readers**, not one writer; it measures requested fixed-operation latency, not offered-versus-admitted load or saturation. It also lacks the required WAL, grouping, queue-wait, apply, rotation, stall, and compaction-I/O attribution.

**Baseline/control:** retain this executable short profile only as a smoke benchmark. Build separate fixed-admitted-load and saturation drivers, with a single-instance warm scalar read/single-writer control. For memory arms, publish the accounting equation so “same total budget” includes the shared pool once and active/immutable buffers across all 16 DBs.

**Procedure/pass/defer:** warm up, then replay seeded request timestamps; run tests separately and sample gauges outside timed requests. Compare an uninstrumented run to quantify observer overhead. Defer profile selection unless one-hot and all-active results remain within memory/tail budgets in repeated fresh processes. New workload arms and opt-in timing counters are still to be built; `stats.compaction_bytes_written` is only an existing partial counter.

### 3. §2: Specify independent gauge expectations, not self-reconciliation

**Benefit:** avoid tautological tests and false process-memory conclusions. Gauge implementations comparing their own source fields would pass despite ownership errors.

**Baseline/control:** quiescent empty DB/shared pool; independently known inserted payload and pin sizes; a detached member beside a surviving reader; separately tracked old snapshots/`PinnedSlice`s.

**Procedure/pass/defer:** build a two-member test covering insert → freeze while oldest flush is parked → durable install → invalidation → replacement → detach, draining Moka before each comparison. Require documented deltas and pool cache-key isolation; do not require trivial mathematical memtable offsets, which `MemTable::approximate_size` intentionally approximates (`src/memtable/mod.rs:227`). Never expect peak RSS to fall or subtract gauges from RSS. Existing executable tests cover cache entry counts, retention, and isolation; public gauge surfaces and reconciliation assertions remain unbuilt.

### 4. §3 Export: Upgrade the golden fixture and negative assertions

**Benefit:** prevent a successful read-only, small fixture from certifying the adapter boundary.

**Procedure/pass:** construct the promised eight-byte-prefix, known-level, SST-plus-residual-WAL/deletion source; hash source files before/after and compare an independently specified ordered stream, row count, and terminal iterator status. Reopen writable with correct `prefix_len`/`num_levels`, append, close, and reread. Wrong geometry must fail; a competing destination must remain unchanged, and scan failure must leave nothing published. The existing damaged-SST test already covers only the last behavior partially (`golden.rs:43`).

**Before implementation:** first add the §4 predeclared budgets and a valid measurement driver; otherwise the subsequent optimization work cannot produce trustworthy accept/differ decisions.

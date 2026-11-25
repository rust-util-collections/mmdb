---
name: x-overhaul
description: Review scoped MMDB changes, prove each defect with a failing regression test, fix it and its sibling variants, and create atomic local commits. Invoke explicitly as $x-overhaul; empty scope is the latest commit and all selects the full repository.
---

# MMDB Scoped Review, Proven Fix, and Local Commit

MMDB is an in-process embedded Rust LSM key-value library operating on
caller-owned local files (`vsdb` backend; no network, daemon, or IPC service).
This workflow checks data-structure, MVCC, persistence, and concurrency
invariants, proves each confirmed defect with a test that fails before the fix,
fixes it together with its sibling variants as sequential atomic local commits,
and finalizes at most one eligible patch version bump with a local version tag.

Value comes from executed evidence, not volume: every fix carries a test that
fails before it and passes after it, and every reviewed fix is checked for its
siblings and its regression coverage.

Resolve repository paths from the Git root; Markdown links below are relative
to this file. Read invocation arguments from user prompt text. Never create a
branch, push, open a PR, publish, or rewrite history.

## Preflight

Run `git status --short --branch` and `git rev-parse HEAD` explicitly before
reviewing or editing. Record the branch, starting `HEAD`, package version in
`Cargo.toml` (at `HEAD` and in the worktree), and the staged, unstaged, and
untracked baseline for the invocation ledger in
[commit-protocol.md](../../../docs/ai/commit-protocol.md#invocation-ledger).

## Setup

Read first:
[workflow-policy.md](../../../docs/ai/workflow-policy.md),
[commit-protocol.md](../../../docs/ai/commit-protocol.md),
[review-core.md](../../../docs/ai/review-core.md),
[pragmatic-engineering.md](../../../docs/ai/pragmatic-engineering.md),
[technical-patterns.md](../../../docs/ai/technical-patterns.md),
[false-positive-guide.md](../../../docs/ai/false-positive-guide.md).
Multi-subsystem or design-shaped scope → also
[design-patterns.md](../../../docs/ai/design-patterns.md).

Compose the cited sections of `x-review`, `x-commit`, and `x-fix` under this
single invocation ledger; do not launch them as nested standalone workflows or
apply their standalone read-only, final-gate, or no-version rules.

## Input

Parse one optional scope with the table in
[workflow-policy.md](../../../docs/ai/workflow-policy.md#scope) (`empty` =
latest commit; `N`; `staged`; `worktree`; `all` = full repository; `<rev>`;
`<rev1>..<rev2>`). Reject `--fix`, multiple scopes, and unknown tokens.

## Protocol

### 1. Scope and ledger

1. Freeze resolved commits and owned paths before any edit.
2. `staged`: owns pre-staged paths/hunks only. Because `cargo` checks compile
   and test the working tree rather than the Git index, stop and report before
   editing if any staged file also has unstaged changes that affect validation.
3. `worktree`: owns tracked staged/unstaged changes and only user-intended
   untracked source/test files and explicitly requested untracked documentation.
   Leave untracked design drafts or scratch notes (such as `docs/proposals/*`)
   untouched unless the user asked to include them.
4. Committed scopes (`empty`, `N`, `<rev>`, `<rev1>..<rev2>`, `all`): leave all
   baseline worktree changes uncommitted and untouched; stop and report if an
   in-scope fix overlaps dirty baseline lines. List each frozen commit with its
   type (`fix`, `perf`, `refactor`, other) for the intent checks in Step 2.
5. `all`: build the tracked-file ledger from `git ls-files`. Use the
   [documentation and subsystem maps](../../../docs/ai/review-core.md#documentation-map)
   to assign coverage. Large files (`src/db.rs`,
   `src/compaction/leveled.rs`, `src/sst/table_reader/{mod.rs,iterator.rs}`,
   `src/sst/table_builder.rs`, `src/iterator/db_iter.rs`,
   `src/manifest/version_set.rs`) may be partitioned into disjoint line-range
   sections so each section has a single review owner in the ledger.
   Order review by risk: the Review priority classes in `review-core.md` first,
   and within them files with the most recent `fix(...)` commits
   (`git log -60 --format= --name-only --grep='^fix' -- src | sort | uniq -c |
   sort -rn`), because recent fix density marks code where more defects remain.
   Run the Final repository gate once before the first edit and record failures
   as pre-existing; with zero edits that run is the final gate.

### 2. Review

- **`staged` / `worktree`**: follow [x-commit](../x-commit/SKILL.md) Protocol
  §1–§2 on the frozen units (full functions/callers/tests, confirmed defects
  fixed in place). Fixes go through Step 3; commit in Step 3 while preserving
  staged/unstaged boundaries. Skip `x-commit` §4; Step 5 runs the gate once.
- **Committed scopes**: execute [x-review](../x-review/SKILL.md) Protocol §1–§4
  (`Scope`, `Evidence`, `Verify`, `Completeness`), holding `docs/audit.md`
  updates until Step 4. Review small or single-subsystem diffs directly in the
  current session. Delegate read-only subagents only when splitting large
  context helps (`all` or multi-subsystem ranges), using the
  [scoped handoff](../../../docs/ai/review-core.md#agent-handoff-and-response)
  with concrete storage-invariant questions while keeping the coverage ledger
  in the coordinating session.

**Commit intent checks** (each frozen commit in a historical diff scope):

| Commit type | Check | Finding when it fails |
|-------------|-------|-----------------------|
| any | Subject/body claim matches the diff; public behavior agrees with item rustdoc, the relevant reader guide, and tests | Contract drift (LOW) or the behavior defect itself |
| `fix(...)` | The diff removes the root cause, not one symptom; a test fails when the commit's `src/` hunks are reverted (revert probe below) | Incomplete fix (severity of the original defect); missing regression test (LOW) |
| `perf(...)` | Verify the measured path, control and feature flags against the [performance priorities](../../../docs/ai/pragmatic-engineering.md#performance-priorities). Check hot/warm scope (FP-9) and observable results at boundaries: empty/single-entry block, first/last restart, `[lower, upper)` bounds, reverse direction, snapshot cutoff, range tombstones, latched errors | Any changed result or dropped check is a correctness finding, not a perf note |
| `refactor(...)` | No behavior change: same `ErrorKind`, same order of fsync / dir-sync / MANIFEST append / `CURRENT` rename / unlink, same lock scope | Behavior defect at its own severity |

**Revert probe** (each `fix(...)` commit in the frozen range): when the commit's
`src/` paths have no baseline edits and no owned edits yet, reverse-apply only
those hunks (`git show <commit> -- <src paths> | git apply -R`), run the
targeted test filter for the subsystem, then re-apply
(`git show <commit> -- <src paths> | git apply`) and confirm
`git diff --quiet -- <src paths>` before any other step. A test that fails under
the revert is the commit's regression coverage; record its name. No failure →
coverage gap. If reverse-apply does not apply cleanly (later commits rewrote the
hunk), skip the probe and trace coverage statically, saying so in the report.

**Variant sweep** (each confirmed finding and each `fix(...)` commit in scope):

1. State the defect pattern as one violated invariant (for example "an error
   latched by an iterator stays latched on every later advance", "a partitioned
   SST leaf agrees with its resident directory bounds", "already-committed
   writes still return `Ok(())` when post-commit rotation fails").
2. Search for every sibling that must hold the same invariant: forward/reverse
   twins (`seek` vs `seek_for_prev` / `seek_to_last`), lending `step_next` /
   `step_prev` vs `Iterator::next` / `next_back`, `DBIterator` / `BidiIterator` /
   `LevelIterator` / `TableIterator` / `MergingIterator`, point-index
   (`partitions` / `index_leaf_block` / `find_data_block`) vs range-tombstone
   (`range_partitions` / `visit_ranges` / `covering_sequence`) leaves, `get` /
   `get_pinned` / `contains_key` and `Snapshot` / `ArcSnapshot`,
   `WalAsync` / `WalSync` / `DisableWal` + `sync_wal()` (`WalFence`), explicit
   `flush()` vs `write_batch_group` auto-freeze, `FlushQueue::run` vs
   `set_bg_error` vs compaction `set_error`, `confirm_manifest_durable` vs
   `sync_new_sst_parent_dir` in flush, compaction, and sorted ingestion,
   standby vs fallback WAL creation, writable vs read-only open, normal vs multi-WAL
   recovery path, `close` / `Drop` / `DB::simulate_crash()` teardown, and every
   caller of the guarded function or field (`rg` on the identifier, not only the
   diff).
3. Classify each sibling: already holds it (cite the guard), same-root-cause
   gap, or independent variant. Same-root-cause gaps join the finding's unit. A
   gap in a sibling the reviewed `fix(...)` commit should have covered is an
   incomplete fix and is in scope. Independent variants outside the frozen scope
   are not fixed here; record them as `Open` in Step 4.

### 3. Prove, then fix

Record confirmed unsupported-architecture-only findings under `Won't Fix` in
Step 4. Resolve actionable findings in scope sequentially (`CRITICAL` → `LOW`), following
[x-fix](../x-fix/SKILL.md) Protocol §1–§2 with these additions per unit:

1. **Red**: write the focused regression test first, in a `tempfile` DB with
   deterministic API calls, `DB::simulate_crash()`, or file truncation/byte
   edits. For a documentation defect, select an example, link check, or
   contract check that exercises the discrepant claim. Run the chosen check
   against the original tree and record the failure. Assert
   stalls and deadlocks with a bounded wait (`recv_timeout`, a joined thread with
   a deadline), never an unbounded hang.
2. **Test passes on unfixed code** → the finding is not demonstrated. Re-trace
   the conditions. Code-disproven → `Rejected` (material) or drop it. Fixing
   needs an interleaving the test cannot force → use existing deterministic
   hooks (`test-utils`, test gates or channels) or keep a static trace, and
   state "not reproduced; static proof" in the commit body and the report. Never
   claim a test fails before the fix without running it.
3. **Green**: apply the minimal root-cause fix, including same-root-cause
   siblings from the variant sweep. Re-run the filter: the new test passes and no
   test that passed in the red run fails. Failures already present in the red
   run are pre-existing; report them, do not absorb them.
4. Validate and commit per
   [commit-protocol.md](../../../docs/ai/commit-protocol.md#per-unit-validate-and-commit),
   removing any pre-existing `Open` entry for that issue in the same commit.

**Coverage gaps** from the revert probe: add one focused test per reviewed
`fix(...)` commit as its own `test(<subsystem>): ...` commit. It must pass at
`HEAD` and fail under the revert probe (or name the static trace when the probe
cannot apply).

Scope of fixes: historical diff scopes fix defects rooted in that diff that
remain present at `HEAD` (including incomplete fixes from the variant sweep),
plus any pre-existing `docs/audit.md` `Open` entry directly inside the frozen
scope. `all` resolves all confirmed new and existing `Open` entries and
re-checks every `Won't Fix` entry. Callers supply evidence, not unrelated fix
scope.

### 4. Registry

Do not write newly discovered fixable issues to `docs/audit.md` only to remove
them in the same run. Keep blocked or unaccepted breaking findings `Open`
without relabeling them to clear the list. Write `docs/audit.md` in one separate
documentation commit after code fixes only when needed for unresolved `Open`
blockers, independent variants outside the frozen scope, `Won't Fix` / material
`Rejected` dispositions, or stale-entry cleanup.

### 5. Aggregate check, final gate, and version tag

1. If commits were added (`starting_HEAD..HEAD`), review the aggregate diff
   once ([x-fix](../x-fix/SKILL.md) Protocol §3) for cross-commit interactions.
2. Run the [Final repository gate](../../../docs/ai/commit-protocol.md#final-repository-gate)
   once when Rust or build files changed, when the scope is `all`, or before
   committing an eligible version bump. Reuse passing results when the tree
   state has not changed. Attribute failures using the red runs and, for `all`,
   the pre-edit gate: report pre-existing out-of-scope failures separately from
   regressions introduced by this run.
3. Apply [Version and tag](../../../docs/ai/commit-protocol.md#version-and-tag)
   once only when all required checks pass with no in-scope `Open` findings, no
   unreviewed ledger gaps, and no dirty baseline in `Cargo.toml`. Never accept
   a public API or on-disk format break or choose a major bump on the user's
   behalf. No changes and no eligible version bump → no empty commit.

## Output

Report, in this order:

1. **Scope**: resolved scope, frozen commits, ledger coverage, and any gaps.
2. **Findings**: one row per finding with severity, location, invariant,
   disposition, proof (`red→green` with test name, `static proof` with reason,
   or `Rejected` reason), and commit hash.
3. **Reviewed fix commits**: revert-probe result (catching test, gap closed by
   a new test, or probe skipped) and variant-sweep outcome.
4. **Validation**: executed commands with pass/fail, pre-existing failures,
   and any optional host tool not present locally.
5. **Commits, version, and tag**: new commit hashes and subjects, version and
   tag outcome, and preserved baseline paths.
6. **Residual risk**: claims kept only by static trace, and independent
   variants recorded `Open` that a follow-up scope should cover.

Handle failed or incomplete steps per
[workflow-policy.md](../../../docs/ai/workflow-policy.md#4-validation-and-failure),
keeping validated local commits intact.

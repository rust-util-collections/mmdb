---
name: x-fix
description: Resolve the MMDB audit backlog sequentially, with one finding per validated local commit. Use only when the user explicitly invokes /x-fix.
disable-model-invocation: true
---

# Fix the MMDB Audit Backlog

Resolve every actionable entry in `docs/audit.md`, self-review the fixes, and
create local commits. Never push.

## Setup

1. Read `.claude/docs/workflow-policy.md`,
   `.claude/docs/commit-protocol.md`,
   `.claude/docs/review-core.md`,
   `.claude/docs/technical-patterns.md`, and
   `.claude/docs/false-positive-guide.md`.
2. Run the workflow-policy preflight and record the commit-protocol invocation
   ledger.
3. Read `docs/audit.md` as the primary work list. If `## Open` contains no
   entries, report "nothing to fix" and stop.

## Protocol

### 1. Triage

Process findings CRITICAL → HIGH → MEDIUM → LOW. Before editing each one:

1. Re-read the cited code, callers, tests, and mapped pattern guides.
2. Reproduce the trigger from current-code evidence.
3. Deduplicate entries that share one root cause.
4. If the claim is false, move it to `## Rejected` with evidence.
5. If it is real but a safe fix is currently disproportionate, move it to
   `## Won't Fix` with a precise reason.

### 2. Fix one finding, then commit it

**One finding/root cause per commit is blocking.** For each confirmed finding:

1. Implement a complete root-cause fix.
2. Add or update focused regression coverage.
3. Re-read the modified code and trace error, crash, concurrency, and cleanup
   paths.
4. Remove the resolved `## Open` entry. The code, tests, and that registry
   update are one commit unit.
5. Run the commit protocol's per-unit validation.
6. Stage exact paths/hunks, inspect `git diff --cached`, and commit before
   starting the next finding.

A registry-only disposition for one finding is also one commit unit. Multiple
symptoms may share a commit only when they are demonstrably the same root cause.
Never run mutating fix agents in parallel; parallelism is limited to read-only
investigation or independent validation.

### 3. Self-review

After processing the initial backlog:

1. Review `starting_HEAD..HEAD` plus any remaining worktree diff and all files
   changed by the fix commits, using `/x-review`'s evidence and verification
   rules.
2. Add any newly confirmed issue to `## Open`, then process it through the same
   one-finding procedure.
3. Stop on no-progress validation loops or unsafe overlap with baseline
   changes; do not stash, reset, or rewrite prior commits.

### 4. Final gate, version, and tag

Run the commit protocol's final repository gate. If any Rust source changed,
apply its single version-bump-and-release-tag policy once for the entire
invocation, not once per finding.

`docs/audit.md` must finish with no unresolved `## Open` entry unless execution
is blocked and the blocker is reported. Never add dates or freshness markers.

## Output

Report the initial disposition count, fixes, rejected and deferred entries,
validations, every commit hash/subject, the version and release-tag result, and
untouched baseline changes.

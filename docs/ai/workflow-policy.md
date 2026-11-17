# Reliability Review and Atomic Commit Policy

Shared workflow rules for `$x-review`, `$x-commit`, `$x-fix`, and `$x-overhaul`.
Skills must not weaken these rules. See also `pragmatic-engineering.md`.

**Hard rules:** user-invoked only · local commits only (never push or open a
PR) · no history rewrite · one independent issue per commit · no auto tag ·
at most one eligible patch bump only in `$x-overhaul`.

## Skill discovery and invocation

[AGENTS.md](../../AGENTS.md) is the shared repository entrypoint. Skills live
under [`.agents/skills/`](../../.agents/skills/) and are explicitly invoked with
`$x-review`, `$x-fix`, `$x-commit`, or `$x-overhaul`. Each skill's
`agents/openai.yaml` sets `allow_implicit_invocation: false`. Ordinary tasks do
not implicitly invoke a commit or version workflow. These instructions define
workflow scope; they do not alter the runtime's tool-approval controls. Pushing
requires a separate explicit user request and is never performed automatically.

## Scope

`$x-review` and `$x-overhaul` take one optional scope and nothing else. Read
the scope from the user's invocation text; empty means the latest commit.
Reject unknown tokens, multiple scopes, and `--fix` rather than guessing.

| Input | Scope |
|-------|-------|
| *(empty)* | Latest commit |
| `N` | Last N commits, N a positive integer |
| `staged` | `git diff --cached` |
| `worktree` | Tracked staged/unstaged changes + user-intended untracked source/test files |
| `all` | Full repository |
| `<rev>` | One commit; `git rev-parse --verify --quiet '<rev>^{commit}'` must succeed |
| `<rev1>..<rev2>` | Two-dot range; verify both commits (`git log <rev1>..<rev2>` and `git diff <rev1> <rev2>`), never three-dot |

Freeze resolved commits at invocation start. Quote revisions, reject option-like
tokens, and place pathspecs after `--`. Compare a single commit (including the
empty default) against its first parent, or the empty tree for a root commit.
Validate `HEAD~N` for numeric counts; clarify if an all-digit token also
resolves as a commit hash. Reject invalid or out-of-range counts rather than
broadening scope.

Historical scopes fix only defects rooted in that diff that remain present at
`HEAD`. Empty is never the full repository; full repository requires `all`.

## Storage-engine specification and reporting

MMDB is an in-process embedded Rust LSM key-value library (`vsdb` backend)
operating solely on caller-owned local database files (`CURRENT`, `MANIFEST-*`,
`*.wal`, `*.sst`, `LOCK`). It opens no network sockets, runs no daemon or RPC
endpoint, and defines no cross-user privilege boundary. Cargo dependency
downloads and Git commands are separate developer-tooling operations.

Describe every candidate, finding, test, progress update, and commit message in
concrete storage-engineering terms:

1. **Operation and input** — public API call sequence, `WriteBatch` contents,
   iterator bounds/direction, `DB::simulate_crash()`, or deterministic file
   truncation / header-byte edit in an isolated `tempfile::TempDir`.
2. **State and invariant** — MVCC sequence cutoff, internal-key ordering,
   prefix-block restart layout, WAL/MANIFEST record framing, or lock acquisition
   order.
3. **Expected vs observed behavior** — expected `Result` (`Ok(...)` or typed
   `Err(ErrorKind::Corruption)` / `ErrorKind::ReadOnly` / `ErrorKind::Io`) and
   persisted state versus the actual returned value, panic, retained allocation,
   or quantified hot-path cost.
4. **Existing checks and regression test** — why current guards do or do not
   cover the path, and the smallest deterministic test that fails before the fix.

Keep exact Rust and MMDB identifiers (`unsafe`, `ErrorKind::Corruption`,
`VersionSet::poisoned`, `SequenceNumber`, `ArcSwap`) verbatim, while conversing
in the user's language.

### Review-agent handoffs

Review small, single-commit, `staged`, `worktree`, or single-subsystem scopes
directly in the current session. Spawn read-only subagents only when splitting
large context materially helps (`all` or multi-subsystem ranges).

Give each delegated subagent a fresh, narrow prompt using the template in
`review-core.md`: include the embedded local-library context, repository path,
frozen `HEAD`/baseline, disjoint assigned files (or disjoint line ranges for
large files such as `src/db.rs` and `src/compaction/leveled.rs`), mapped pattern
guides, and a concrete storage-invariant question (for example, key ordering,
MVCC visibility, WAL/MANIFEST replay, or lock ordering) — never an open-ended
request to hunt vulnerabilities. Retain the full coverage ledger in the
coordinating session; smaller handoffs do not narrow the user's requested scope.

Derive scope and status from the current checkout and `docs/audit.md`, not from
prior session claims. If a subagent run stops early or returns incomplete
coverage, complete the remaining files locally or record the exact gap.

## 1. Preflight

Before mutating files or creating commits:

1. Run `git status --short --branch` and `git rev-parse HEAD` explicitly and
   record their output before reviewing or editing.
2. Separate staged, unstaged, and untracked baseline paths.
3. Stop on an unfinished merge/rebase/cherry-pick or detached `HEAD` unless the
   user resolves it.
4. Define this invocation's owned files/hunks; baseline changes stay untouched.
   In `worktree` mode, leave untracked design drafts or scratch notes (such as
   `docs/proposals/*`) outside the owned set unless the user asked to commit
   them.
5. **Commit workflows:** freeze owned paths and planned units before editing.
   Stage only the frozen set plus this invocation's in-unit fix/rustfmt paths —
   never paths created later by concurrent work.

A dirty worktree is permitted when ownership boundaries remain disjoint.

## 2. Preserve existing work

- Never run `git stash`, `git clean`, `git checkout --`, `git restore`, or
  destructive `git reset` to manufacture a clean tree, and do not create a
  separate worktree to hide dirty baseline files.
- Never revert, overwrite, stage, commit, or reformat unrelated baseline files.
- If a required fix overlaps dirty baseline hunks, or if `staged` scope touches
  a file that also has unstaged edits affecting worktree `cargo` validation,
  stop and report before editing.
- Delegated review agents are strictly read-only. Investigation and read-only
  checks may run in parallel; edits and commits on one checkout are sequential.

## 3. Atomic commit units

One independent issue, root cause, or behavior change → one commit:

- Bundle only its regression test, matching public docs, and any resolved
  `docs/audit.md` hunk.
- Group multiple symptoms only when they share one root cause.
- No drive-by refactors, unrelated lints, or whole-repo formatting churn.
- Stage exact paths/hunks (`git add -- <paths>`); never use `git add -A`, `-u`,
  `.`, or `git commit -a`. Inspect `git diff --cached` before every commit.
- Create new local commits only — never amend, rebase, rewrite history, tag,
  push, or open a PR.
- Never commit a `docs/audit.md` inventory ahead of fixing the defects in the
  same run. Remove pre-existing `Open` entries inside their fix commit; put
  remaining registry-only dispositions (`Won't Fix`, `Rejected`, unresolved
  `Open`, stale-entry cleanup) in one separate commit after code fixes.

## 4. Validation and failure

- Run the smallest proving checks per unit (`commit-protocol.md`); exclude
  `tests/scale_profile.rs` and `--release`.
- Worktree `cargo` commands compile and test all files currently on disk. If
  unrelated dirty baseline files alter compilation or test results and cannot be
  isolated, stop and report.
- Run the Final repository gate in `commit-protocol.md` once after the last
  behavior commit (or once on `all` / before an eligible version bump).
- Use explicit `cargo +stable` commands rather than `make` targets unless the
  user named a `make` target.
- Fix unit-caused failures before committing. Report pre-existing out-of-scope
  failures with the exact command and compiler/test output; never claim a clean
  gate when a check failed or was skipped.
- Stop and report if the same failure repeats without progress. Fix post-commit
  gate regressions in a new atomic commit and re-run the gate.

### Incomplete or failed steps

If a command or delegated subagent fails or terminates early, record the
operation, the exact error message returned, and the checks or files left
incomplete. Continue independent work only when safe to do so. Keep already
validated local commits intact and report incomplete checks separately from
confirmed source-code defects; an unrun check proves neither a defect nor a
passing gate.

## 5. Finding dispositions

| State | Meaning |
|-------|---------|
| `Open` | Confirmed, actionable storage defect |
| `Won't Fix` | Confirmed defect whose complete fix has disproportionate cost or regression risk (fix-time triage only; never for feature requests or documented contracts) |
| `Rejected` | Material claim disproven by code or tests (no severity label; omit routine noise) |
| *omit* | Feature request, documented contract, or pure style preference |

`$x-review` may add or prune `Open`, add `Rejected`, move a stale `Won't Fix`
to `Open` or `Rejected`, and delete entries shown never to have been defects; it
does not add `Won't Fix`. `$x-fix` and `$x-overhaul` may add `Won't Fix` during
triage and delete non-defects. Never relabel an active defect merely to empty
`Open`. Formats and severities follow `review-core.md`, with no dates or "last
reviewed" markers.

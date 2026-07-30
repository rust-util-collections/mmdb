# Atomic Commit Protocol

Validate → commit → version for `/x-commit`, `/x-fix`, `/x-overhaul`.
Use with `workflow-policy.md`.

## Invocation ledger

Before first edit, record:

- start `HEAD`, branch, package version at that `HEAD` and in the worktree;
- staged / unstaged / untracked baseline;
- **frozen owned paths** (sorted) and planned units;
- whether any tracked `.rs` will change.

Keep the ledger across commits (`git diff HEAD` loses earlier units). Stage only
freeze set + this-invocation fix/format paths.

## Per-unit validate and commit

1. One issue/root cause/behavior change + its tests/docs/audit only.
2. Checks:
   - Docs/config only: `git diff --check` + structure sanity; skip Rust gates.
   - Rust: `cargo fmt --all -- --check`; if needed, `make fmt` only on owned paths (inspect).
   - Rust: `make lint` — no `#[allow(...)]`.
3. Smallest proving tests:
   - docs-only → none;
   - one subsystem → its filter + relevant integration binary;
   - `db.rs` / write / compaction / manifest / cross-cutting → `cargo test`;
   - crash-safety → include `cargo test --test crash_recovery` unless already covered by a full `cargo test`.
4. On fail: fix if caused by the unit; else report pre-existing with evidence. No empty gates or infinite loops.
5. Stage exact freeze + unit fix/format paths — never `git add -A`.
6. `git diff --cached` = exactly one unit, no baseline/post-freeze paths.
7. Match repo commit style; HEREDOC multi-line; no co-author/generated-by.
8. Verify commit; compare `git status --short` to baseline.

Never amend a prior commit for a later fix.

## Final repository gate

After last behavior commit (once per stable code state):

1. `cargo fmt --all -- --check`
2. `make lint`
3. `make test` (debug + release)

Regression → new atomic commit, then re-run. Docs-only: skip Rust gates.

## Version bump and release tag

If any tracked `.rs` changed in this invocation:

1. Once: `Cargo.toml` `X.Y.Z` at start-HEAD → `X.Y.(Z+1)`. If baseline already has the target, verify only.
2. `cargo metadata --no-deps --format-version 1`.
3. Stage `Cargo.toml`, inspect cached diff.
4. Separate final commit (only exception to one-issue-one-commit). Never per-finding bumps.
5. Annotated tag on that commit: `v` + version (from metadata or `Cargo.toml`).

Skip when no Rust source changed. No empty commits. Do not force-add `Cargo.lock` (library).

## Final state

Report every new hash/subject and version result. Owned changes committed;
unrelated baseline untouched. Global clean worktree not required.

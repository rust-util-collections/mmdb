# Atomic Commit Protocol

Canonical validation, commit, and version procedure for `/x-commit`, `/x-fix`,
and `/x-overhaul`. Apply it together with `workflow-policy.md`.

## Invocation ledger

Before the first edit, record:

- starting `HEAD`, branch, package version at `HEAD`, and current worktree
  package version;
- staged, unstaged, and untracked baseline paths;
- planned commit units;
- whether this invocation changes any tracked Rust source file.

Keep this ledger across commits. A later `git diff HEAD` cannot reveal source
files already committed earlier in the same workflow.

## Per-unit validation and commit

For each independent commit unit:

1. Confirm that the diff contains one issue/root cause or behavior change plus
   only its required tests, docs, and audit entry.
2. Run deterministic checks appropriate to the unit:
   - Docs/config only: run `git diff --check` and validate affected internal
     paths/structured files; skip Rust formatting, lint, and tests.
   - Rust source: run `cargo fmt --all -- --check`. If formatting is needed, run
     `make fmt` only when its resulting changes can be confined to
     invocation-owned files; inspect the diff immediately.
   - For Rust source, run `make lint` and fix warnings at the source.
     `#[allow(...)]` is forbidden.
3. Run the smallest tests that prove the unit:
   - Docs/comments only: no Rust tests.
   - One subsystem: its unit-test filter plus the directly relevant integration
     test binary.
   - `src/db.rs`, write path, compaction, manifest, or cross-cutting behavior:
     `cargo test`.
   - Crash-safety behavior: ensure `cargo test --test crash_recovery` is
     included. Do not repeat it when an already-run `cargo test` covered it.
4. On failure, determine whether it is caused by the unit. Fix caused failures;
   report demonstrably pre-existing failures with evidence. Do not claim a
   passing gate or loop without progress.
5. Stage exact paths or hunks, never `git add -A`.
6. Inspect `git diff --cached` and verify that it contains exactly one unit and
   no unrelated baseline changes.
7. Match the repository's commit style and create a new commit. Use a HEREDOC
   for a multi-line message and omit co-author/generated-by trailers.
8. Verify the new commit and compare `git status --short` with the baseline.

Never amend a prior commit to absorb a later fix.

## Final repository gate

After the last behavior-affecting commit:

1. Run `cargo fmt --all -- --check`.
2. Run `make lint`.
3. Run `make test` (debug and release suites).

Run this gate once per unchanged final code state. If it exposes a regression,
fix that root cause in a new atomic commit and repeat the gate. Documentation-
only workflows skip Rust validation.

## Single version bump

If the invocation changed tracked `.rs` files:

1. Bump `Cargo.toml` from the version at the invocation-start `HEAD` (`X.Y.Z`) to
   `X.Y.(Z+1)` exactly once. If the intended target version was already present
   in the baseline, verify it instead of incrementing again.
2. Validate the manifest with
   `cargo metadata --no-deps --format-version 1`.
3. Stage `Cargo.toml` explicitly and inspect the cached diff.
4. Commit the release metadata as a dedicated final commit. This is the sole
   exception to the one-issue-one-commit rule; never bump once per finding.
5. Create an annotated git tag matching the new version:
   `git tag -a "v$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')" -m "v$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')"`
   (or construct the version string directly from `Cargo.toml`).
   The tag must point at the release commit and use the `v` prefix.

`Cargo.lock` is intentionally ignored because MMDB is a library; do not
force-add it.

Skip the bump when no Rust source changed. Do not create an empty commit.

## Final state

Report every new commit hash/subject and the version result. The worktree need
not be globally clean, but all invocation-owned changes must be committed and
all unrelated baseline changes must remain untouched.

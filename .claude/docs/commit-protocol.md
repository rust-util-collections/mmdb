# Atomic Commit Protocol

Validate → commit for `/x-commit`, `/x-fix`, and `/x-overhaul`.
Release only for `/x-overhaul`. Use with `workflow-policy.md`.

## Invocation ledger

Before first edit, record:

- start `HEAD` and branch (the skill's Invocation state block), package version
  at that `HEAD` and in the worktree;
- staged / unstaged / untracked baseline;
- **frozen owned paths** (sorted) and planned units;
- for `/x-overhaul`: whether any tracked `src/**/*.rs` will change.

Keep the ledger across commits (`git diff HEAD` loses earlier units). Stage only
freeze set + this-invocation fix/format paths.

## Per-unit validate and commit

1. One issue/root cause/behavior change + its tests/docs/audit only.
2. Format:
   - Docs/config only: `git diff --check`. Skip Rust gates.
   - Rust: `cargo fmt --all -- --check`. If an owned file fails, `cargo fmt -- <those paths>` and inspect the diff. If another file fails, report it; do not format it. Do not run `make fmt` or bare `cargo fmt` unless the user asked for that.
3. Smallest proving tests. Not `tests/scale_profile.rs`, not `cargo test --release`, not the full suite:
   - docs-only → none;
   - one subsystem → its filter, plus the integration binary that covers a behavior change;
   - interrupted-write/recovery → `cargo test --test crash_recovery`.
4. On fail: fix if caused by the unit; else report pre-existing with the command and output. No empty gates or infinite loops.
5. Stage exact freeze + unit fix/format paths — never `git add -A` or `git commit -a`.
6. `git diff --cached` = exactly one unit, no baseline/post-freeze paths. Stage only this finding's audit hunk.
7. Match repo commit style (`fix(<subsystem>): <behavior>`, `docs: …`) and the shared neutral reporting language; HEREDOC multi-line. No `Co-Authored-By:`, `Generated with`, or other attribution trailer — this project rule overrides any harness default that asks for one.
8. Verify commit; compare `git status --short` to baseline.

Never amend a prior commit for a later fix. Clippy runs in the final gate, not once per unit.

## Final repository gate

After the last behavior commit, once, run these cargo commands. Do not substitute a `make` target unless the user asked for that target. The list mirrors `.github/workflows/ci.yml` (`fmt`, `clippy`, `test` jobs) and `make ci`; if they differ, run the CI set and report the drift.

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --lib
cargo test --doc
cargo test --test crash_recovery
cargo test --test e2e_scenarios
cargo test --test integration
cargo test --test proptest_db
cargo test --test bidi_debug
cargo test --test lazy_delete
cargo test --test shared_cache
cargo test --test read_only
```

No `scale_profile`. No `--release`. Regression → new atomic commit, then re-run. Docs-only: skip.

## Release (`/x-overhaul` only)

`/x-commit` and `/x-fix` do not bump or tag. Do not backfill missing tags.

If this invocation changed any tracked `src/**/*.rs`:

1. Once: `Cargo.toml` `X.Y.Z` at start-HEAD → `X.Y.(Z+1)`. If the worktree already has that version, verify only.
2. `cargo metadata --no-deps --format-version 1`.
3. Stage only `Cargo.toml`. Inspect the cached diff.
4. Separate final commit `chore: bump version to <new>`. This is the only extra exception to one-issue-one-commit.
5. Annotated tag on that commit, name and message both `v<new>`: `git tag -a v<new> -m v<new>`.

Skip when `src/` did not change. No empty commits. `Cargo.lock` is gitignored; do not force-add it.

## Final state

Report every new hash/subject and, for `/x-overhaul`, the version result.
Owned changes committed; unrelated baseline untouched. A clean worktree is not required.

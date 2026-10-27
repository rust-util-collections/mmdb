# Atomic Commit Protocol

Validate → commit for `$x-commit`, `$x-fix`, and `$x-overhaul`.
Version finalization only for `$x-overhaul`; no automatic tags. Use with `workflow-policy.md`.

## Invocation ledger

Before first edit, record:

- start `HEAD` and branch (from the explicit preflight), package version
  at that `HEAD` and in the worktree;
- staged / unstaged / untracked baseline;
- **frozen owned paths** (sorted) and planned units;
- for `$x-overhaul`: whether tracked `.rs` changes since the last version commit
  make a version bump owed, including intended Rust changes in this invocation.

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

Use the official latest stable Rust under the toolchain policy in `AGENTS.md`.
Update/select stable before Rust checks; do not define or validate a minimum
compiler or compatible version range.

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

No `scale_profile`. No `--release`. Regression → new atomic commit, then re-run. Docs-only: skip unless this is a full `all` audit or an owed version bump needs
validation. A full audit runs this gate even without fixes.

## Version (`$x-overhaul` only; no tag)

`$x-commit` and `$x-fix` do not bump or tag. An overhaul completes one owed
patch bump after the required gate passes with no in-scope Open findings or
coverage gaps. Tracked `.rs` changes since the latest commit that changed the
crate's `version = ` in `Cargo.toml` make a bump owed, including intended Rust
changes committed by this invocation:

```sh
base=$(git log -1 --format=%H -G'^version = ' -- Cargo.toml)
git diff --name-only "$base" HEAD -- '*.rs'
```

Recheck after the last behavior commit; do not lose earlier units by looking
only at `git diff HEAD`. A blocked run keeps validated commits and leaves the
bump detectable for a later overhaul, even with no new fixes. Docs-only work
does not create an owed bump.

1. Compatible changes: increment the current agreed `X.Y.Z` to `X.Y.(Z+1)` once.
   Verify the starting and current manifest versions; do not overwrite divergent
   versions or unowned manifest edits.
2. `cargo metadata --no-deps --format-version 1` verifies the package version.
3. Stage only `Cargo.toml` and inspect the candidate diff.
4. Make one separate final commit `chore: bump version to <new>`. This is the
   version-only exception to one-root-cause-per-commit. No empty commit.

An agent cannot accept a public API or persisted-format break on the user's
behalf. An unaccepted break remains Open and uncommitted; an explicitly accepted
break requires a major increment and documented migration instead of a patch.
Never automatically create a tag, push, open a PR or publish. Do not backfill
missing tags. `Cargo.lock` is ignored; never force-add it.

## Final state

Report every new hash/subject and, for `$x-overhaul`, the version result.
Owned changes committed; unrelated baseline untouched. A clean worktree is not required.

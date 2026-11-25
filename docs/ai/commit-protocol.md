# Atomic Commit Protocol

Validation, staging, and local commit rules for `$x-commit`, `$x-fix`, and
`$x-overhaul`. Crate version finalization, including the local Git tag, applies
only to `$x-overhaul`. Use alongside `workflow-policy.md`.

## Invocation ledger

Before the first edit, record and retain across commits (`git diff HEAD` drops
earlier commits in the same run):

- branch and starting `HEAD` (from the explicit preflight), plus `package.version`
  in `Cargo.toml` at `HEAD` and in the working tree;
- staged, unstaged, and untracked baseline paths;
- **frozen owned paths** (sorted) and planned atomic commit units;
- for `$x-overhaul`: whether shipped library inputs changed since the latest
  `Cargo.toml` version commit and whether the current scope covers or adds
  shipped library changes.

Stage only paths from the frozen owned set plus in-unit fix/rustfmt paths added
by this invocation.

## Per-unit validate and commit

1. One independent issue, root cause, or behavior change + its focused
   regression test, matching docs, and any resolved `docs/audit.md` hunk.
2. Formatting check:
   - Docs/config only: `git diff --check` (skip Rust checks).
   - Rust edits: `cargo +stable fmt --all -- --check`. If an owned `.rs` file
     needs formatting, run
     `rustfmt +stable --edition 2024 --config skip_children=true <owned-paths>`
     and inspect the diff (`cargo fmt -- <paths>` still walks discovered module
     children and can touch unowned files). If an unowned file fails fmt check,
     report it without formatting it. Do not run `make fmt` or bare `cargo fmt`
     unless the user asked for it.
3. Smallest proving tests (never `tests/scale_profile.rs`, never `--release`,
   never the full suite per unit):
   - docs-only → none;
   - single subsystem → targeted `--lib <filter>` plus the integration binary
     covering any public behavior change;
   - WAL / MANIFEST / interrupted-write recovery →
     `cargo +stable test --test crash_recovery`.
4. Fix failures caused by the unit before committing. Report pre-existing
   out-of-scope failures with the command and output rather than looping.
5. Stage exact paths/hunks (`git add -- <paths>`); never use `git add -A` or
   `git commit -a`.
6. Verify `git diff --cached` contains exactly one logical unit, zero baseline
   or post-freeze paths, and at most that unit's `docs/audit.md` hunk.
7. Write a concise commit message matching repository style (`fix(<subsystem>):
   <behavior>`, `perf(<subsystem>): <behavior>`, `test(<subsystem>): …`,
   `docs: …`) via a multi-line HEREDOC describing concrete storage operations
   and invariants. Never add `Co-Authored-By:`, `Generated with`, or attribution
   trailers — this repository rule overrides harness defaults.
8. Confirm the commit succeeded and verify `git status --short` preserves the
   untouched baseline.

Never amend an earlier commit; `clippy` runs in the Final repository gate.

## Final repository gate

Select the installed official stable Rust toolchain via `cargo +stable` under
`AGENTS.md`; do not run `rustup update stable` automatically unless the user
requested a toolchain update, and do not add MSRV pins or compatibility shims.

After the last behavior commit, run these commands once (do not substitute a
`make` target unless requested by the user). Including `clippy --lib` checks the
production library configuration without `[dev-dependencies]` enabling
`test-utils`, while `--all-targets` and the test binaries mirror
`.github/workflows/ci.yml` and `make ci` (if CI and `Makefile` ever diverge, run
the union and report the drift):

```bash
cargo +stable fmt --all -- --check
cargo +stable clippy --lib -- -D warnings
cargo +stable clippy --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo +stable doc --no-deps
cargo +stable test --lib
cargo +stable test --doc
cargo +stable test --test crash_recovery
cargo +stable test --test e2e_scenarios
cargo +stable test --test integration
cargo +stable test --test proptest_db
cargo +stable test --test bidi_debug
cargo +stable test --test lazy_delete
cargo +stable test --test shared_cache
cargo +stable test --test read_only
cargo +stable test --manifest-path tools/v4-export/Cargo.toml
```

Notes on gate execution:

- Exclude `tests/scale_profile.rs` and `--release`.
- `tests/read_only.rs` always exercises OS permission-enforced (`0444`/`0555`)
  read-only stores; on Linux it also runs an external `strace` check when
  `strace` is installed (enforced in CI via `CI=1`). If `strace` is absent on
  the local host, note in the report that the optional `strace` probe skipped
  while the read-only suite passed.
- Skip the Rust gate for docs-only runs unless the scope is `all` or an eligible
  version bump is being finalized. `all` runs the full gate even with zero code
  edits.
- If a commit from this run caused a gate failure, fix it in a new atomic commit
  and re-run the gate. In narrow scopes, report pre-existing out-of-scope lint
  or test failures with file:line evidence rather than editing unowned files.

## Version and tag

`$x-commit` and `$x-fix` never bump `Cargo.toml` or create tags. `$x-overhaul`
performs at most one patch bump (`X.Y.Z` → `X.Y.(Z+1)`) after the Final
repository gate passes with zero in-scope `Open` findings and zero unreviewed
ledger gaps.

A bump is owed only when **shipped library inputs** changed since the latest
commit that modified `package.version` in `Cargo.toml` — specifically production
library Rust files (`src/**/*.rs` excluding `#[cfg(test)]`-only
`src/test_alloc.rs`) or shipped `[dependencies]` / `[target.*.dependencies]` /
`[features]` entries in `Cargo.toml`:

```sh
base=$(git log -1 --format=%H -G'^version = ' -- Cargo.toml)
git diff --name-only "$base" HEAD -- 'src/*.rs' 'src/**/*.rs' ':(exclude)src/test_alloc.rs'
```

Changes confined to `tests/`, `benches/`, `examples/`, `src/test_alloc.rs`,
`docs/`, `README.md`, `COMPARISON.md`, `TODO.md`, or `.agents/` do **not** owe a
version bump.

Scope eligibility:

- **`all` scope**: finalize one owed patch bump if shipped library inputs
  changed since `$base` (including finalizing an earlier unbumped library commit
  once the full audit and gate pass).
- **Narrow scopes** (`empty`, `N`, `staged`, `worktree`, `<rev>`,
  `<rev1>..<rev2>`): finalize one owed patch bump **only if** this invocation
  itself committed a shipped library change, or the frozen reviewed commit range
  itself contains shipped library changes since `$base`. Do not bump
  `Cargo.toml` during a docs/tests/benches-only narrow overhaul merely because
  unrelated earlier commits sit after `$base`.
- **Dirty `Cargo.toml` baseline**: if `Cargo.toml` already had uncommitted
  baseline edits at preflight, leave `Cargo.toml` untouched and report the
  baseline conflict instead of staging unowned manifest lines.

When eligible:

1. Verify that the starting and current versions match. Compute the next patch
   version and set `tag` to `v<new>`. Check for a collision with
   `git show-ref --verify --quiet "refs/tags/$tag"`. If the tag already exists,
   stop and report the conflict before editing `Cargo.toml`. Otherwise increment
   `package.version` from `X.Y.Z` to `X.Y.(Z+1)` once.
2. Run `cargo +stable metadata --no-deps --format-version 1` to verify manifest
   parsing and the updated version.
3. Stage only `Cargo.toml` and inspect `git diff --cached`.
4. Commit `chore: bump version to <new>` as one separate final commit (the sole
   exception to one-root-cause-per-commit; never an empty commit). Record
   its commit hash as `bump_commit`.
5. Create an annotated local tag with
   `git tag -a "$tag" -m "$tag" "$bump_commit"`. Verify that
   `git rev-parse --verify "refs/tags/${tag}^{commit}"` returns `bump_commit`.
   If the step remains blocked after addressing recoverable errors, preserve
   the version commit and any existing tag. Report the incomplete step with
   the tag name, expected commit, and error or observed target.

Never accept a public API or persisted on-disk format break on the user's
behalf: keep unaccepted breaking findings `Open` and uncommitted; an explicitly
user-approved breaking change uses a major bump and migration docs instead of an
automatic patch. Never backfill missing tags, push, open a PR, publish, or
force-add gitignored `Cargo.lock`.

## Final state

Report all new commit hashes/subjects and, for `$x-overhaul`, the version
outcome and the verified tag name and target commit when a bump was made.
Owned changes are committed; unrelated baseline files remain untouched.

# Working on MMDB

MMDB is an in-process embedded Rust LSM-Tree key-value library, built as the
local storage backend for [vsdb](https://github.com/rust-util-collections/vsdb).
Describe storage correctness, persistence, concurrency, and resource behavior in
terms of concrete API/file operations, invariants, and expected vs observed
results.

## What is this project?

MMDB implements leveled compaction, WAL-based recovery across interrupted
writes, MVCC snapshots, and lock-free acquisition of the read view via
`ArcSwap<SuperVersion>`.

The library runs entirely inside the caller's process on local database files
(`CURRENT`, `MANIFEST-*`, `*.wal`, `*.sst`, `LOCK`); it opens no network
sockets, exposes no RPC or daemon, and enforces no multi-tenant privilege
boundary. Reliability tests operate on isolated `tempfile` directories and
deterministic generated records. Cargo dependency downloads and Git operations
are separate developer-tooling actions with their own runtime controls.

## Supported architectures

MMDB guarantees correct behavior on 64-bit ARM (AArch64, `aarch64`) and
64-bit x86 (`x86_64`), using a 64-bit ABI. Other architectures are outside
the project’s tested correctness guarantee.
Record confirmed defects confined to other architectures, including 32-bit
targets, under `Won't Fix` in [the audit registry](docs/audit.md#wont-fix).
A defect that also affects a supported target follows normal review and fix triage.

## Build & Test

```bash
make all          # fmt + lint + test
make test         # debug/release suites, optional profile-counter tests, exporter
make ci           # fmt check + clippy + the CI test set (no release, no scale_profile)
make lint         # default and bench-metrics clippy, exporter clippy, rustdoc
make bench        # cargo bench with default features
cargo test --test integration   # integration tests only
cargo test --test crash_recovery
cargo test --test e2e_scenarios
cargo test --test proptest_db
cargo test --lib --features bench-metrics  # include opt-in diagnostic counters
```

Agents use the cargo commands in [commit-protocol.md](docs/ai/commit-protocol.md) for their own checks (including `--manifest-path tools/v4-export/Cargo.toml` for the separately built v4 exporter crate). Run a `make` target only when the user asks for it. That includes `make fmt`, which formats the whole repository; agents format only their owned Rust files with `rustfmt +stable --edition 2024 --config skip_children=true <paths>`. Passing paths after `cargo fmt --` still formats discovered crate targets and does not isolate ownership.

Toolchain: official latest stable Rust (edition 2024) for development, CI, and
releases, selected via `RUSTUP_TOOLCHAIN=stable` or `cargo +stable`. Do not run
`rustup update stable` automatically during agent workflows unless the user asks
for a toolchain update. Define no compatible Rust version range or minimum
compiler; do not add `rust-version`, numeric toolchain pins, minimum-version CI
jobs, or older-compiler compatibility shims.

## Architecture

Follow a write, read, compaction, or reopen in the [v5 storage architecture](docs/architecture.md).
The [subsystem map](docs/ai/review-core.md#subsystem-map) pairs each source area
with its review checklist. For public behavior and format conversion, see
[Using MMDB v5](docs/usage.md) and [Migrating to v5](docs/migration.md).

## Skills

Project skills live under `.agents/skills/<name>/SKILL.md` and are
explicitly invoked with `$x-review`, `$x-fix`, `$x-commit` or `$x-overhaul`.
Invocation arguments are plain prompt text; run the selected skill's preflight
commands explicitly. Each skill's `agents/openai.yaml` preserves explicit-only
selection with `allow_implicit_invocation: false`.

- [`$x-review`](.agents/skills/x-review/SKILL.md) — review a scope (default: latest commit). Updates `docs/audit.md` only
- [`$x-fix`](.agents/skills/x-fix/SKILL.md) — resolve Open findings, one validated commit each. No version bump
- [`$x-commit`](.agents/skills/x-commit/SKILL.md) — review and commit the current worktree. No version bump
- [`$x-overhaul`](.agents/skills/x-overhaul/SKILL.md) — same scopes as `$x-review` (empty = latest commit; `all` = full repo), then review, prove each fix with a test that fails before it, sweep sibling variants, and create atomic local commits. When shipped library changes qualify for a patch bump, bump once and tag the version commit locally. Never push or open a PR

All four workflows follow [workflow-policy.md](docs/ai/workflow-policy.md):
specify local storage operations, invariants, and expected vs observed results,
and use the scoped handoff in [review-core.md](docs/ai/review-core.md) when
delegating read-only subsystem checks while preserving exact Rust/API
identifiers and finding severities.

Supporting docs (`docs/ai/`):

- [workflow-policy.md](docs/ai/workflow-policy.md) — scope resolution, embedded-storage specification, scoped handoffs, worktree ownership, atomic commits
- [pragmatic-engineering.md](docs/ai/pragmatic-engineering.md) — engineering and [performance priorities](docs/ai/pragmatic-engineering.md#performance-priorities) for core operations, cleanup, and migration
- [technical-patterns.md](docs/ai/technical-patterns.md) / [design-patterns.md](docs/ai/design-patterns.md) — LSM bugs + D-\* design lens
- [review-core.md](docs/ai/review-core.md) — evidence/report templates + Subsystem Map
- [false-positive-guide.md](docs/ai/false-positive-guide.md) — check existing guarantees before recording findings
- [commit-protocol.md](docs/ai/commit-protocol.md) — validate → commit → version bump → local version tag
- `patterns/*` — per-subsystem checklists

The [audit registry](docs/audit.md) is maintained by `$x-review`, `$x-fix`, and `$x-overhaul`. Open is actionable; Won't Fix is an accepted defect; Rejected is a disproven claim.

## Conventions

- All clippy warnings are errors (CI enforced)
- **No `#[allow(...)]`** — fix warnings at the source, never suppress them
- **Prefer imports over inline paths** — avoid `std::foo::Bar::new()` inline in function bodies when the same path appears 3+ times in a file; add `use std::foo;` at file top (or `use std::foo::Bar;`) instead. Function-body `use` statements (scoped imports) are fine and don't count as inline paths. 1-2 inline uses of common `std::` items are acceptable.
- **Grouped imports** — merge common prefixes: `use std::sync::{Arc, atomic::Ordering};`
- **Doc-code alignment** — public API changes must update corresponding docs
- `parking_lot` for all Mutex/RwLock/Condvar synchronization (non-reentrant,
  no poisoning)
- Typed errors: `Error`/`ErrorKind` in `src/error.rs` (hand-rolled, no thiserror/ruc); propagate with `.ctx()` / `.with_ctx(|| ..)` — each hop records `file:line:column` via `#[track_caller]`
- `tracing` for logging
- Tests use `tempfile` for isolated DB directories
- Feature `bench-metrics` enables per-worker write-stage timing and per-DB L0 statistics. Use the [engine workload profiles](docs/benchmarks/engine-profile/) and [L0 evidence](docs/benchmarks/l0-profile/) for benchmark configurations, controls and counter units. The [grouped read trials](docs/benchmarks/grouped-reads/) record returned-pin ownership, actual storage-read coverage and independent scalar controls.
- Feature `test-utils` exposes `DB::simulate_crash()` for durability tests and `point_table_probes()` / `PinnedSlice::sst_backing_charge()` for grouped-read attribution. Under Miri it also exposes `concurrent_arena_pins()` for `cargo +nightly miri test --test arena_miri`
- Unsafe code is confined to the arena skiplist (`src/memtable/`), group-commit/file-lock and advisory WAL writeback paths (`src/db.rs`), advisory SST readahead (`src/sst/table_reader/mod.rs`), and the `#[cfg(test)]` counting allocator (`src/test_alloc.rs`); derive the live inventory via search and follow [unsafe-audit.md](docs/ai/patterns/unsafe-audit.md)
- Only the curated re-exports in `src/lib.rs` are public API; all modules are private — internal refactors are not breaking changes
- **No Co-Authored-By in commits** — never add `Co-Authored-By:` or similar trailers to commit messages; project commits are authored only by the human contributor

# Working on MMDB

Describe correctness, persistence, concurrency and resource behavior using
concrete operations and expected/observed results.

## What is this project?

MMDB is a pure-Rust LSM-Tree key-value storage engine, optimized as the backend for [vsdb](https://github.com/rust-util-collections/vsdb). It implements leveled compaction, WAL-based crash recovery, MVCC snapshots, and lock-free reads via `ArcSwap<SuperVersion>`.

## Build & Test

```bash
make all          # fmt + lint + test
make test         # cargo test && cargo test --release
make ci           # fmt check + clippy + the CI test set (no release, no scale_profile)
make lint         # cargo clippy --all-targets -- -D warnings (matches CI)
make bench        # cargo bench (criterion)
cargo test --test integration   # integration tests only
cargo test --test crash_recovery
cargo test --test e2e_scenarios
cargo test --test proptest_db
```

Agents use the cargo commands in [commit-protocol.md](docs/ai/commit-protocol.md) for their own checks. Run a `make` target only when the user asks for it. That includes `make fmt`, which formats the whole repository; agents format only their owned paths with `cargo fmt -- <paths>`.

Toolchain: official latest stable Rust (edition 2024); no MSRV floor is declared

## Architecture

| Subsystem | Key files | Purpose |
|-----------|-----------|---------|
| Write path | `src/db.rs` | Group commit: WAL → MemTable |
| Read path | `src/db.rs` | Lock-free via `ArcSwap<SuperVersion>` |
| MemTable | `src/memtable/` | Lock-free skiplist (single-writer, multi-reader) |
| WAL | `src/wal/` | Crash recovery, block-based records with CRC32 |
| SST | `src/sst/` | Prefix-compressed blocks, dedicated range-del block, bloom filters, LZ4/Zstd |
| Iterator | `src/iterator/` | Heap merge, range tombstones, bidirectional |
| Compaction | `src/compaction/leveled.rs` | Leveled strategy, sub-compaction parallelism |
| Manifest | `src/manifest/` | VersionSet, atomic version edits |
| Cache | `src/cache/` | Block cache (moka SegmentedCache via BlockCachePool), table handle cache |

## Skills

Project skills live under `.agents/skills/<name>/SKILL.md` and are
explicitly invoked with `$x-review`, `$x-fix`, `$x-commit` or `$x-overhaul`.
Invocation arguments are plain prompt text; run the selected skill's preflight
commands explicitly. Each skill's `agents/openai.yaml` preserves explicit-only
selection with `allow_implicit_invocation: false`.

- [`$x-review`](.agents/skills/x-review/SKILL.md) — review a scope (default: latest commit). Updates `docs/audit.md` only
- [`$x-fix`](.agents/skills/x-fix/SKILL.md) — resolve Open findings, one validated commit each. No version bump
- [`$x-commit`](.agents/skills/x-commit/SKILL.md) — review and commit the current worktree. No version bump
- [`$x-overhaul`](.agents/skills/x-overhaul/SKILL.md) — same scopes as `$x-review` (`all` for the full repo), then fix in-scope findings and make local commits, with one local release if `src/` changed. Never push or open a PR

All four workflows follow the neutral task/report wording in
[workflow-policy.md](docs/ai/workflow-policy.md). Review local storage
operations and expected/observed results. Use the scoped agent handoff in
[review-core.md](docs/ai/review-core.md); pass only relevant repository
context, and preserve precise Rust/API terms and finding severity.

Supporting docs (`docs/ai/`):

- [workflow-policy.md](docs/ai/workflow-policy.md) — neutral task framing, scoped handoffs, worktree ownership, atomic commits
- [pragmatic-engineering.md](docs/ai/pragmatic-engineering.md) — root goal, low variance
- [technical-patterns.md](docs/ai/technical-patterns.md) / [design-patterns.md](docs/ai/design-patterns.md) — LSM bugs + D-\* design lens
- [review-core.md](docs/ai/review-core.md) — evidence/report templates + Subsystem Map
- [false-positive-guide.md](docs/ai/false-positive-guide.md) — check existing guarantees before recording findings
- [commit-protocol.md](docs/ai/commit-protocol.md) — validate → commit → version/tag
- `patterns/*` — per-subsystem checklists

Audit registry: `docs/audit.md` — maintained by `$x-review`, `$x-fix`, and `$x-overhaul`. Open is actionable; Won't Fix is an accepted defect; Rejected is a disproven claim.

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
- Feature `test-utils` exposes `DB::simulate_crash()` for durability tests
- Unsafe code is concentrated in the skiplist, group-commit/file-lock paths, and the SST readahead syscall; derive the live inventory and follow [unsafe-audit.md](docs/ai/patterns/unsafe-audit.md)
- Only the curated re-exports in `src/lib.rs` are public API; all modules are private — internal refactors are not breaking changes
- **No Co-Authored-By in commits** — never add `Co-Authored-By:` or similar trailers to commit messages; project commits are authored only by the human contributor

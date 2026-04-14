# MMDB — Claude Code Project Guide

## What is this project?

MMDB is a pure-Rust LSM-Tree key-value storage engine, optimized as the backend for [vsdb](https://github.com/rust-util-collections/vsdb). It implements leveled compaction, WAL-based crash recovery, MVCC snapshots, and lock-free reads via `ArcSwap<SuperVersion>`.

## Build & Test

```bash
make all          # fmt + lint + test
make test         # cargo test && cargo test --release
make lint         # cargo clippy --all-targets -- -D warnings
make bench        # cargo bench (criterion)
cargo test --test integration   # integration tests only
cargo test --test crash_recovery
cargo test --test e2e_scenarios
cargo test --test proptest_db
```

MSRV: Rust 1.89 (edition 2024)

## Architecture

| Subsystem | Key files | Purpose |
|-----------|-----------|---------|
| Write path | `src/db.rs` | Group commit: WAL → MemTable |
| Read path | `src/db.rs` | Lock-free via `ArcSwap<SuperVersion>` |
| MemTable | `src/memtable/` | Lock-free skiplist (single-writer, multi-reader) |
| WAL | `src/wal/` | Crash recovery, block-based records with CRC32 |
| SST | `src/sst/` | Prefix-compressed blocks, bloom filters, LZ4/Zstd |
| Iterator | `src/iterator/` | Heap merge, range tombstones, bidirectional |
| Compaction | `src/compaction/leveled.rs` | Leveled strategy, sub-compaction parallelism |
| Manifest | `src/manifest/` | VersionSet, atomic version edits |
| Cache | `src/cache/` | Block cache (moka LRU), table handle cache |

## Commands

- `/x-review` — deep regression analysis of recent changes
- `/x-fix` — resolve all open findings in `.claude/audit.md`
- `/x-commit` — self-reviewing commit (review, fix, format, commit)
- `/x-auto` — full pipeline: review → fix → commit (incremental, uncommitted changes only)
- `/x-overhaul` — full codebase overhaul: review all → fix → commit

Supporting documentation in `.claude/docs/`:
- `technical-patterns.md` — cataloged bug patterns for LSM-Tree/Rust
- `review-core.md` — systematic review methodology
- `false-positive-guide.md` — rules for filtering spurious findings
- `patterns/` — per-subsystem review guides (compaction, iterator, WAL, SST, memtable, concurrency, unsafe-audit)

## Conventions

- All clippy warnings are errors (CI enforced)
- **No `#[allow(...)]`** — fix warnings at the source, never suppress them
- **Prefer imports over inline paths** — avoid `std::foo::Bar::new()` inline in function bodies when the same path appears 3+ times in a file; add `use std::foo;` at file top (or `use std::foo::Bar;`) instead. Function-body `use` statements (scoped imports) are fine and don't count as inline paths. 1-2 inline uses of common `std::` items are acceptable.
- **Grouped imports** — merge common prefixes: `use std::sync::{Arc, Mutex};`
- **Doc-code alignment** — public API changes must update corresponding docs
- `parking_lot` for Mutex/RwLock (non-reentrant, no poisoning)
- `thiserror` for error types
- `tracing` for logging
- Tests use `tempfile` for isolated DB directories
- Feature `test-utils` exposes `DB::simulate_crash()` for durability tests
- ~76 unsafe blocks, concentrated in skiplist and block parsing — all require `// SAFETY:` comments

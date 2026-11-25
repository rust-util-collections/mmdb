# MMDB

**The Storage Engine Behind [vsdb](https://github.com/rust-util-collections/vsdb).**

[![CI](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml/badge.svg)](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/mmdb.svg)](https://crates.io/crates/mmdb)
[![Docs.rs](https://docs.rs/mmdb/badge.svg)](https://docs.rs/mmdb)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024_edition-orange.svg)](https://www.rust-lang.org/)

MMDB is an embedded Rust LSM-tree for vsdb's prefix-partitioned keys, bulk
copy-on-write mutations, and compaction-driven garbage collection. Each
database is a local directory. The caller reads and writes byte strings through
the Rust API. Optimization work targets the vsdb workload. A writable handle runs flush and compaction workers in-process.

## Supported architectures

MMDB guarantees correct behavior on 64-bit ARM (AArch64, `aarch64`) and
64-bit x86 (`x86_64`), using a 64-bit ABI. Other architectures are outside
the project’s tested correctness guarantee.

## Getting started

```rust
use mmdb::{DB, DbOptions, WriteBatch};

fn main() -> mmdb::Result<()> {
    let db = DB::open(DbOptions::default(), "example.mmdb")?;
    let mut batch = WriteBatch::new();
    batch.put(b"user:alice", b"active");
    batch.put(b"user:bob", b"active");
    db.write(batch)?;
    db.sync_wal()?;

    let snapshot = db.snapshot();
    db.delete(b"user:alice")?;
    assert!(snapshot.contains_key(b"user:alice")?);
    assert!(!db.contains_key(b"user:alice")?);
    drop(snapshot);
    db.close()
}
```

The default write mode flushes WAL bytes to the OS before acknowledging a
batch. The example calls `sync_wal()` to make the two puts durable together.
Run `cargo +stable run --example basic` for a temporary-directory example
with prefix scans, bounded iteration, and typed errors. [Example source](examples/basic.rs)

## Documentation

| Task | Start here |
|------|------------|
| Choose durability, configure a store, read a snapshot, or scan a range | [Using MMDB v5](docs/usage.md) |
| Inspect a stable copy without writing to it | [Read-only databases](docs/usage.md#read-only-databases) |
| Convert a v4 store and update Rust call sites | [Migrating to v5](docs/migration.md) |
| Trace a write through the WAL, flush, recovery, and compaction, or follow a point lookup | [Storage architecture](docs/architecture.md) |
| Find public methods, options, defaults, and error types | [Crate API](https://docs.rs/mmdb), [exports in this checkout](src/lib.rs) |
| Evaluate the MMDB v5 and VSDB v18 improvements or compare RocksDB and Pebble | [Joint proposal](docs/proposals/mmdb-v5-vsdb-v18-improvements.md) · [Engine measurements and API comparison](docs/proposals/mmdb-v5-vsdb-v18-improvements.md#mmdb-rocksdb-and-pebble) |
| Implement and test the MMDB side before porting VSDB | [MMDB v5 execution plan](docs/proposals/mmdb-v5-execution-plan.md) |
| Understand earlier design decisions and benchmark baselines | [v5 design record](docs/proposals/v5-roadmap.md), [read-only design record](docs/proposals/read-only-mode.md) |
| Contribute or run the project's review and fix workflows | [Contributor and skill guide](AGENTS.md), [audit registry](docs/audit.md) |

## Build and measure

Use the stable Rust toolchain (edition 2024). From this checkout:

```sh
cargo +stable build
cargo +stable test --test integration
cargo +stable doc --no-deps --open
cargo +stable bench --bench benchmarks -- warm
cargo +stable bench --bench v5_profile
cargo +stable bench --bench engine_profile -- mixed
```

The Criterion suite includes warm-cache and small-block-cache workloads. Both
can read from the OS page cache. The separate v5 profile exercises concurrent
readers and writers. Set `MMDB_PROFILE_IMMUTABLES` on separate runs to compare
queue limits (the profile defaults to two).
The engine profile separates foreground writes, bulk batches and snapshot
reads. It also measures shared-pool memory and reopen with residual WALs.
Use `bench-metrics` for diagnostic stage timings. The bounded point-read
benchmark compares 1-, 8- and 32-key requests at one stable snapshot.
[Criterion benchmarks](benches/benchmarks.rs) · [v5 profile](benches/v5_profile.rs) · [Engine workloads and reproduction](docs/benchmarks/engine-profile/README.md) · [L0 results](docs/benchmarks/l0-profile/README.md) · [Grouped point-read results](docs/benchmarks/grouped-reads/README.md)

## License

[MIT](LICENSE)

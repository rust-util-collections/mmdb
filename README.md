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

MMDB 6 opens v5 databases in place. To upgrade a v4 database, first
convert it with MMDB 5, then open the resulting directory with MMDB 6.
The converter and all 5.x releases are maintained on the
[v5 branch](https://github.com/rust-util-collections/mmdb/tree/v5).

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
| Choose durability, configure a store, read a snapshot, or scan a range | [Using MMDB v6](docs/usage.md) |
| Inspect a stable copy without writing to it | [Read-only databases](docs/usage.md#read-only-databases) |
| Upgrade a database to v6 | [Upgrade path and reopening a v5 store](docs/migration.md), [changelog](CHANGELOG.md) |
| Trace a write through the WAL, flush, recovery, and compaction, or follow a point lookup | [Storage architecture](docs/architecture.md) |
| Find public methods, options, defaults, and error types | [Crate API](https://docs.rs/mmdb), [exports in this checkout](src/lib.rs) |
| Compare engines or reproduce a benchmark | [Benchmark reports and workloads](docs/benchmarks/README.md), [VSDB v18 application results](docs/benchmarks/README.md#vsdb-application-results) |
| Contribute or run the project's review and fix workflows | [Contributor and skill guide](AGENTS.md), [audit registry](docs/audit.md) |

## Build and test

Use the stable Rust toolchain (edition 2024). From this checkout:

```sh
cargo +stable build
cargo +stable test --test integration
cargo +stable doc --no-deps --open
```

When the shell supports `ulimit -Sn` and `ulimit -Hn`, `make test` and
`make ci` raise a low soft file descriptor limit to 4,096 when possible.
If the limit stays lower, they default to one test thread. Other shells
launch the tests with their inherited settings. For a focused run, use
`sh tools/with-test-fds.sh cargo +stable test --lib`.

## License

[MIT](LICENSE)

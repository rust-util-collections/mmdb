# MMDB

[![CI](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml/badge.svg)](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/mmdb.svg)](https://crates.io/crates/mmdb)
[![Docs.rs](https://docs.rs/mmdb/badge.svg)](https://docs.rs/mmdb)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024_edition-orange.svg)](https://www.rust-lang.org/)

MMDB is the embedded Rust key-value engine behind vsdb. Each database is a
local directory. Applications read and write byte strings, group mutations in
atomic batches, and scan ranges or prefixes at a snapshot. In vsdb these
operations support prefix-partitioned keys and bulk copy-on-write updates.
A writable handle runs its flush and compaction workers inside the caller's process.
[vsdb](https://github.com/rust-util-collections/vsdb)

## Getting started

Add `mmdb = "6"` to your application's `[dependencies]`. This example writes
two keys in one batch, makes the batch durable, and reads it through a
snapshot after deleting one of the keys.

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

By default, `write` makes the batch visible and flushes the write-ahead log
(WAL) to the OS. `sync_wal` confirms durability for preceding WAL-enabled
writes. For a runnable example that uses a temporary directory and also scans
keys, run `cargo +stable run --example basic`. [Example source](examples/basic.rs)

## Documentation

Find the usage and upgrade guides, storage architecture, benchmark reports,
and contributor workflows in the [documentation index](docs/README.md).
For method signatures and option types, see the [Rust API reference](https://docs.rs/mmdb).

## Supported architectures

MMDB guarantees correct behavior on 64-bit ARM (`aarch64`) and 64-bit x86
(`x86_64`), both with a 64-bit ABI. Other targets are outside the tested
correctness guarantee.

## Build and test

From the repository root, use the stable Rust toolchain (edition 2024).

```sh
cargo +stable build
cargo +stable test --test integration
cargo +stable doc --no-deps --open
```

The contributor guide lists the full suites and how to run tests with a low
file descriptor limit. [Build and test commands](AGENTS.md#build--test)

## License

[MIT](LICENSE)

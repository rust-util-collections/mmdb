# MMDB

[![CI](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml/badge.svg)](https://github.com/rust-util-collections/mmdb/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/mmdb.svg)](https://crates.io/crates/mmdb)
[![Docs.rs](https://docs.rs/mmdb/badge.svg)](https://docs.rs/mmdb)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024_edition-orange.svg)](https://www.rust-lang.org/)

MMDB is the embedded Rust key-value engine behind
[vsdb](https://github.com/rust-util-collections/vsdb). Each database is a local
directory. Applications read and write byte strings, group mutations in atomic
batches, and scan ranges or prefixes at a snapshot. In vsdb these operations
support prefix-partitioned keys and bulk copy-on-write updates. A writable
handle runs its flush and compaction workers inside the caller's process.

## Getting started

Add `mmdb = "6"` to your application's `[dependencies]`. Save a draft, take
a snapshot, then delete the draft. The snapshot can still read its text.

```rust
use mmdb::{DB, DbOptions};

fn main() -> mmdb::Result<()> {
    let db = DB::open(DbOptions::default(), "example.mmdb")?;
    db.put(b"draft", b"hello")?;
    let snapshot = db.snapshot();
    db.delete(b"draft")?;

    assert_eq!(snapshot.get(b"draft")?, Some(b"hello".to_vec()));
    assert_eq!(db.get(b"draft")?, None);
    db.close()
}
```

By default, `put` and `delete` flush the write-ahead log (WAL) to the OS.
`sync_wal()` makes preceding WAL-enabled writes durable. Run
`cargo +stable run --example basic` to sync a batch and scan its keys in a
temporary directory. [Example source](examples/basic.rs) ·
[Durability guarantees](https://github.com/rust-util-collections/mmdb/blob/master/docs/usage.md#write-and-choose-durability)

## Documentation

Find the usage and upgrade guides, storage architecture, benchmark reports,
and contributor workflows in the [repository documentation index](https://github.com/rust-util-collections/mmdb/blob/master/docs/README.md).
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
file descriptor limit. [Build and test commands](https://github.com/rust-util-collections/mmdb/blob/master/AGENTS.md#build--test)

## License

[MIT](LICENSE)

# MMDB documentation

For a first write and snapshot read, start with the example in the
[project README](../README.md#getting-started). The guides here describe
MMDB v6. Each benchmark report identifies the engine revision it measured.

## Use the library

| Task | Guide |
|---|---|
| Create or reopen a writable database, or inspect a stable copy read-only | [Open and configure a store](usage.md#open-and-configure-a-store) · [Read-only databases](usage.md#read-only-databases) |
| Choose what must survive an interruption | [Writes, WAL fences, and flush](usage.md#write-and-choose-durability) |
| Bulk-load sorted rows into an unused key span | [Sorted ingestion](usage.md#sorted-ingestion) |
| Choose between owned bytes, pins, snapshots, or a scan | [Read, pin, and iterate](usage.md#read-pin-and-iterate) |
| Compact a range, interpret a memory gauge, or handle a failed call | [Maintenance and properties](usage.md#compaction-and-properties) · [Errors](usage.md#errors) |
| Upgrade a v4 or v5 store | [Upgrade guide](migration.md) · [Changelog](../CHANGELOG.md) |

The Rust API reference lists the public methods and options. [API reference](https://docs.rs/mmdb) ·
[Public exports](../src/lib.rs)

Run either example from the repository root.

- `cargo +stable run --example basic` writes two users and lists both from a snapshot taken before a deletion. [Basic operations](../examples/basic.rs)
- `cargo +stable run --example shard_buffers` shows how buffer size and frozen slots add up to 30 MiB per shard. [Shard buffer budgets](../examples/shard_buffers.rs)

## Understand the engine

The architecture overview shows how the WAL, memtables, and SSTs fit
together. Its subsystem map links each operation to the chapter covering
the implementation and its invariants. [Storage architecture](architecture.md) ·
[Subsystem map](architecture.md#subsystem-map)

## Evaluate or maintain MMDB

- For performance results, select the workload and host. Each report links its
  measurements, controls, and reproduction instructions. [Benchmark index](benchmarks/README.md)
- For development commands, conventions, and explicitly invoked project
  workflows, start with the [contributor guide](../AGENTS.md).
- For a change under review, match the affected source area to its invariants
  and inspect the recorded findings. [Subsystem checklists](ai/review-core.md#subsystem-map) ·
  [Audit registry](audit.md)

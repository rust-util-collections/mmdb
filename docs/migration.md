# Migrating to v5

## Convert a v4 directory

MMDB v5 requires a v5 MANIFEST header and SST v2. Use the standalone exporter
to copy the visible rows from a stable v4 store into a new v5 directory.
Run from the repository root. The destination must be absent, its parent must
already exist, and it must be outside the source directory.

```sh
cargo +stable run --manifest-path tools/v4-export/Cargo.toml -- /data/v4 /data/v5 3 2
```

The last two positional arguments are the **source** v4 level count and
fixed prefix length. The example uses three levels and a two-byte prefix.
Omitting both selects the v4 defaults of seven levels and a zero prefix length.

1. The exporter opens the source read-only with MMDB 4.3.15, including valid residual WAL records.
2. It creates a staging directory under the destination parent. It preserves the prefix length and clamps the destination level count to 2–16.
3. It streams visible pairs in key order through `DB::ingest_sorted` and checks the source iterator's final error. Ingestion writes bottommost SSTs directly. The new database contains the visible values with a new sequence history.
4. After closing both databases, the exporter publishes the staging directory by rename. On Unix it reserves the destination name first and syncs the parent directory after the rename.

Use the destination's geometry for a subsequent writable open. To inspect
the result, `DB::open_read_only` adopts those values automatically. The
exporter works on MMDB key/value pairs. Changes to vsdb collection formats
require a migration at the vsdb layer. [Exporter](../tools/v4-export/src/main.rs) · [Golden-store tests](../tools/v4-export/tests/golden.rs)

## Update Rust callers

| v4 usage | v5 usage |
|----------|----------|
| Exhaustive option struct literals | Start with `Default` or a preset and chain `with_*` setters |
| `sync` and `disable_wal` write flags | Select `DurabilityMode::{WalAsync, WalSync, DisableWal}`. Use `DB::sync_wal()` to fence prior WAL-enabled writes |
| Raw snapshot sequence in `ReadOptions` | Pass a live snapshot to `with_snapshot` or call `snapshot.read_options()` |
| Owned iteration bounds | Supply borrowed slices or owned buffers to the range-bound setters |
| Value-sensitive block properties | Collect user keys from values and point deletions. Keep each collector name's meaning stable. A block may be skipped only if every collected key fails `key_matches` |
| Indexed batch conversion and range access | Propagate `into_batch()?`. `range_tombstones()` iterates borrowed endpoints and write positions |
| String iterator errors | Use fallible lending steps or check `error() -> Option<&Error>` after owned iteration |
| `pin_l0_filter_and_index_blocks_in_cache` | Use `pin_l0_first_block_and_warm_index` |

[Public exports](../src/lib.rs) · [Options and filter contracts](../src/options.rs) · [v5 usage](usage.md)

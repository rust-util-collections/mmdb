# Upgrading to MMDB v6

The supported data upgrade path is **v4 → v5 → v6**. Complete the first
step with the [v5 release's migration guide](https://github.com/rust-util-collections/mmdb/blob/e0e490dce4eb53f03ed7e56d036779e906dfb3c1/docs/migration.md).

## Reopen a v5 database

MMDB v6 uses the same MANIFEST header (format 5), SST v2 tables and WAL
records as v5. Existing v5 data can be upgraded in place.

1. Stop the v5 writer and close its database handles. Keep the complete
   database directory together, including any residual WALs.
2. Update the application's Cargo dependency to `mmdb = "6"`. Open the
   same path with `DB::open`, using the original `num_levels` and
   `prefix_len`. Opening replays eligible WAL records as usual.
3. Check the expected keys or scan the database before resuming writes.
   An owned iterator exposes errors through `error()` after the scan.

For read-only inspection, `DB::open_read_only` adopts the geometry from
the MANIFEST and replays valid residual WAL records in memory.
[Open and read options](usage.md) · [Database files and recovery](architecture.md#database-files-and-recovery)

# Upgrading to MMDB v6

| Database on disk | How to open it with v6 |
|---|---|
| MMDB v5 | Reopen the same directory in place with the original writable geometry. |
| MMDB v4 | Use the converter from the v5 maintenance release to create a **new v5 directory**, then open that destination with v6. |

MMDB v6 reads MANIFEST format 5, SST v2 files, and v5 WAL records.

## Reopen a v5 database

1. Stop the v5 writer and close its handles. Keep the whole store together,
   including `CURRENT`, its MANIFEST, live SSTs, and residual WALs. A recent
   acknowledged row may exist only in a WAL.
2. Update the application's dependency to `mmdb = "6"`. Call `DB::open` on
   the existing path with the original `num_levels` and `prefix_len`.
   Use `with_create_if_missing(false)` so a mistaken path fails. A geometry
   mismatch returns `ErrorKind::InvalidArgument`. An unsupported MANIFEST
   header returns `ErrorKind::Corruption`.
3. Verify expected keys, including recent updates and deletes, before
   resuming writes. For an ordered scan, `step_next()?` propagates a read
   failure. If you consume the owned `Iterator`, check `error()` after the scan.

The v5.0.1 fixture uses `num_levels = 3` and `prefix_len = 2`. On reopen,
`get(b"p:w")` returns the bytes `WAL only` from the replayed log, and
`get(b"p:b")` returns `None` because the log also recorded its deletion.
[v5.0.1 fixture](../tests/fixtures/v5.0.1/) · [Compatibility test](../tests/integration.rs)

To inspect before switching writers, `DB::open_read_only` adopts the stored
geometry and replays WALs in memory. Close it before the writable reopen in
step 2, which checks the application's geometry. [Read-only usage](usage.md#read-only-databases) ·
[Reopen cost for legacy SSTs](architecture/recovery.md#validate-the-manifest-and-live-ssts)

## Convert a v4 database

Follow the v5 converter's instructions for a stable source and a separate
destination. Supply the **source** level count and fixed prefix length. The
converter copies the visible key/value rows and assigns fresh sequence
history. Use the resulting v5 store's geometry when following the v6 reopen
steps above. The v5 guide also covers the application-level migration path
for a VSDB universe. [v4 conversion and verification](https://github.com/rust-util-collections/mmdb/blob/e0e490dce4eb53f03ed7e56d036779e906dfb3c1/docs/migration.md#convert-a-v4-directory)

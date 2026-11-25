# Changelog

## Unreleased v5

- A valid nonempty write with `no_slowdown` reports `ErrorKind::Busy` on
  transient L0 or immutable-queue pressure. A refusal appends no WAL record
  and assigns no sequence to that batch.
- `DB::memory_usage()` reports live write-buffer estimates and member-local
  L0 pin payload bytes. `BlockCachePool::usage()` reports the configured
  capacities and approximate weighted charges of the two shared cache stores.
  A same-key cache replacement retains its registration for file invalidation.
- The v4 exporter is tested with a three-level, eight-byte-prefix store
  containing both SST and residual WAL rows, including writable reopen with
  matching geometry. [Migration guide](docs/migration.md)

# Changelog

## Unreleased v6

- MMDB 6.0 opens existing v5 databases in place, including their residual WALs.
  Conversion of earlier databases is handled by the v5 release. [Upgrade path](docs/migration.md)

## v5

- `DB::get_many_pinned` and `get_many_pinned_with_options` return one
  ordered slot per input key at a single database view and sequence.
  Requests accept up to 32 keys and can reuse validated SST blocks.

- A valid nonempty write with `no_slowdown` reports `ErrorKind::Busy` on
  transient L0 or immutable-queue pressure. A refusal appends no WAL record
  and assigns no sequence to that batch.
- `DB::memory_usage()` reports live write-buffer estimates and member-local
  L0 pin payload bytes. `BlockCachePool::usage()` reports the configured
  capacities and approximate weighted charges of the two shared cache stores.
  A same-key cache replacement retains its registration for file invalidation.

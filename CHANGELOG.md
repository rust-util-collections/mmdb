# Changelog

<a name="unreleased-v6"></a>

## v6

| Crate version | Storage and API changes |
|---|---|
| 6.0.4 | New SSTs record their maximum sequence to speed reopen. File bounds use the range fragments actually emitted. Fatal worker and write-stall compactions publish the error before releasing claimed inputs. [Reopen checks](docs/architecture/recovery.md#validate-the-manifest-and-live-ssts) · [Compaction installation](docs/architecture/compaction.md#install-and-retire-files) |
| 6.0.3 | Reopen checks the MANIFEST against interior SST sequences and rejects an incorrect smallest file key. A point-block checksum failure can surface when the block is read. [SST validation](docs/architecture/recovery.md#validate-the-manifest-and-live-ssts) |
| 6.0.2 | Open validates SST bounds, including range-deletion keys, and reports `Corruption` for a missing referenced MANIFEST. Shutdown wakes flush waiters and interrupts rate-limiter sleeps. Lazy-delete sweeps can resume after snapshot release. [Recovery](docs/architecture/recovery.md) · [Shutdown](docs/architecture.md#fail-stop-error-latching-and-shutdown) |
| 6.0.1 | A queued `no_slowdown` batch rechecks L0 pressure before taking a sequence or appending a WAL record. [Write admission](docs/architecture/writes.md#admission-and-group-commit) |
| 6.0.0 | v5 databases reopen in place, including residual WALs. The v5 maintenance release supplies the v4 converter. [Upgrade guide](docs/migration.md) |

## v5

[Release notes on the v5 maintenance branch](https://github.com/rust-util-collections/mmdb/blob/v5/CHANGELOG.md)

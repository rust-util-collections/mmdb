//! MMDB — the pure-Rust LSM-Tree storage engine behind
//! [vsdb](https://github.com/rust-util-collections/vsdb).
//!
//! Open a local directory with [`DB::open`] and [`DbOptions`]. A [`WriteBatch`]
//! makes related mutations visible together. [`DurabilityMode`] controls
//! what reaches the WAL before acknowledgement. [`DB::sync_wal`] fences
//! preceding WAL-enabled writes, and [`DB::flush`] installs memtable data
//! in durable SSTs.
//!
//! Use [`DB::snapshot`] for a consistent view across reads, [`DB::get_pinned`]
//! to retain a value's backing storage, [`DB::get_many_pinned`] for a
//! bounded group of keys at one sequence, and [`DB::iter_with_range`] for a
//! scan with inclusive lower and exclusive upper bounds. The crate-root
//! re-exports below define the public API.
//!
//! # Supported architectures
//!
//! MMDB guarantees correct behavior on 64-bit ARM (AArch64) and 64-bit x86
//! (x86_64). Other architectures carry no tested correctness guarantee.
//!
//! # Read-only stores
//!
//! [`DB::open_read_only`] opens an existing store without modifying its
//! directory. It recovers valid residual WAL records into memory, and supports
//! normal reads, iterators, snapshots, properties, and clean shutdown. Disk
//! mutation APIs return [`ErrorKind::ReadOnly`]; the infallible
//! `DB::lazy_delete*` methods are no-ops.
//!
//! Here `path` names a store whose writer saved `user:alice` and closed.
//!
//! ```
//! use mmdb::{DB, ErrorKind};
//!
//! # let dir = tempfile::tempdir()?;
//! # let path = dir.path();
//! # let writer = DB::open(mmdb::DbOptions::default(), path)?;
//! # writer.put(b"user:alice", b"active")?;
//! # writer.close()?;
//! let db = DB::open_read_only(path)?;
//! assert!(db.contains_key(b"user:alice")?);
//!
//! let error = db.delete(b"user:alice").unwrap_err();
//! assert_eq!(error.kind(), ErrorKind::ReadOnly);
//! db.close()?;
//! # Ok::<(), mmdb::Error>(())
//! ```
//!
//! To choose read/cache settings for this handle, use
//! [`DB::open_read_only_with_options`]. Read-only recovery adopts the level count
//! and prefix length recorded in the MANIFEST.
//!
//! On Unix, an existing `LOCK` file provides cooperative shared locking. A
//! missing `LOCK` (and every platform without Unix `flock`) must be treated as
//! an unlocked immutable snapshot: keep the directory stable for the handle's
//! entire lifetime and do not use it alongside a live writer.

mod cache;
mod compaction;
mod db;
mod error;
mod iterator;
mod manifest;
mod memtable;
#[cfg(all(miri, feature = "test-utils"))]
#[doc(hidden)]
pub use memtable::arena_tests::concurrent_arena_pins;
mod options;
mod pinned;
mod profile;
mod rate_limiter;
mod sst;
mod stats;
#[cfg(test)]
mod test_alloc;
mod types;
mod wal;

// ---- Primary API ----
pub use cache::block_cache::{BlockCache, BlockCachePool, BlockCachePoolUsage};
#[cfg(feature = "bench-metrics")]
#[doc(hidden)]
pub use compaction::profile::L0CompactionProfile;
pub use db::{ArcSnapshot, DB, DbMemoryUsage, Snapshot, SnapshotRef};
pub use error::{Error, ErrorKind, Result, ResultExt};
pub use iterator::{BidiIterator, DBIterator};
pub use options::{
    BlockPropertyCollector, BlockPropertyFilter, CompactionFilter, CompactionFilterDecision,
    DbOptions, DurabilityMode, ReadOptions, SkipPointFn, WriteOptions,
};
pub use pinned::PinnedSlice;
#[cfg(feature = "bench-metrics")]
pub use profile::{ThreadWriteProfile, take_thread_write_profile};
pub use sst::format::CompressionType;
pub use types::{
    MAX_GROUPED_READ_KEYS, MAX_RANGE_DELETE_SIZE, MAX_USER_KEY_SIZE, MAX_WRITE_ENTRY_SIZE,
    SequenceNumber, WriteBatch, WriteBatchWithIndex,
};

/// Number of point probes into candidate SSTs by this thread in an experiment.
#[cfg(feature = "test-utils")]
#[doc(hidden)]
pub fn point_table_probes() -> u64 {
    sst::table_reader::POINT_TABLE_PROBES.with(std::cell::Cell::get)
}

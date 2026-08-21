//! MMDB — the pure-Rust LSM-Tree storage engine behind
//! [vsdb](https://github.com/rust-util-collections/vsdb).
//!
//! A high-performance LSM-Tree storage engine in pure Rust.
//!
//! The public API is the set of items re-exported from this crate root;
//! internal modules (WAL, SST, manifest, compaction, cache, ...) are
//! implementation details and deliberately private.
//!
//! # Read-only stores
//!
//! [`DB::open_read_only`] opens an existing store without modifying its
//! directory. It recovers valid residual WAL records into memory, and supports
//! normal reads, iterators, snapshots, properties, and clean shutdown. Disk
//! mutation APIs return [`ErrorKind::ReadOnly`]; the infallible
//! `DB::lazy_delete*` methods are no-ops.
//!
//! ```no_run
//! use mmdb::{DB, ErrorKind};
//!
//! # fn main() -> mmdb::Result<()> {
//! let db = DB::open_read_only("database-snapshot")?;
//! let value = db.get(b"key")?;
//!
//! let err = db.put(b"key", b"new value").unwrap_err();
//! assert_eq!(err.kind(), ErrorKind::ReadOnly);
//! # drop(value);
//! db.close()?;
//! # Ok(())
//! # }
//! ```
//!
//! For a store created with custom read/cache settings, use
//! [`DB::open_read_only_with_options`]. [`DbOptions::num_levels`] is not
//! persisted, so pass the writer's value when it differs from the default.
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
mod options;
mod rate_limiter;
mod sst;
mod stats;
mod types;
mod wal;

// ---- Primary API ----
pub use cache::block_cache::{BlockCache, BlockCachePool};
pub use db::{DB, Snapshot};
pub use error::{Error, ErrorKind, Result, ResultExt};
pub use iterator::{BidiIterator, DBIterator};
pub use options::{
    BlockPropertyCollector, BlockPropertyFilter, CompactionFilter, CompactionFilterDecision,
    DbOptions, ReadOptions, SkipPointFn, WriteOptions,
};
pub use sst::format::CompressionType;
pub use types::{
    MAX_USER_KEY_SIZE, MAX_WRITE_ENTRY_SIZE, SequenceNumber, WriteBatch, WriteBatchWithIndex,
};

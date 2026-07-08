//! MMDB — the pure-Rust LSM-Tree storage engine behind
//! [vsdb](https://github.com/rust-util-collections/vsdb).
//!
//! A high-performance LSM-Tree storage engine in pure Rust.
//!
//! The public API is the set of items re-exported from this crate root;
//! internal modules (WAL, SST, manifest, compaction, cache, ...) are
//! implementation details and deliberately private.

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

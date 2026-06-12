//! MMDB — the pure-Rust LSM-Tree storage engine behind
//! [vsdb](https://github.com/rust-util-collections/vsdb).
//!
//! A high-performance LSM-Tree storage engine in pure Rust.

pub mod cache;
pub mod compaction;
pub mod db;
pub mod error;
pub mod iterator;
pub mod manifest;
mod memtable;
pub mod options;
pub mod sst;
pub mod types;
pub mod wal;

pub mod rate_limiter;
pub mod stats;

// Re-export primary API types
pub use db::{DB, Snapshot, pool_return};
pub use error::{Error, Result};
pub use iterator::{BidiIterator, DBIterator, PooledIterator};
pub use options::{
    CompactionFilter, CompactionFilterDecision, DbOptions, ReadOptions, WriteOptions,
};
pub use sst::format::CompressionType;
pub use types::{SequenceNumber, WriteBatch, WriteBatchWithIndex};

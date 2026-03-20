//! MMDB — Modern Memory-Mapped Database
//!
//! A high-performance LSM-Tree storage engine in pure Rust.

pub mod cache;
pub mod compaction;
pub mod db;
pub mod error;
pub mod iterator;
pub mod manifest;
pub mod memtable;
pub mod options;
pub mod sst;
pub mod types;
pub mod wal;

pub mod rate_limiter;
pub mod stats;

// Re-export primary API types
pub use db::DB;
pub use db::Snapshot;
pub use db::pool_return;
pub use error::{Error, Result};
pub use iterator::BidiIterator;
pub use iterator::DBIterator;
pub use iterator::PooledIterator;
pub use options::{
    CompactionFilter, CompactionFilterDecision, DbOptions, ReadOptions, WriteOptions,
};
pub use sst::format::CompressionType;
pub use types::{SequenceNumber, WriteBatch, WriteBatchWithIndex};

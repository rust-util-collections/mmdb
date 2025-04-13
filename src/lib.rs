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
pub use error::{Error, Result};
pub use iterator::DBIterator;
pub use options::{
    CompactionFilter, CompactionFilterDecision, DbOptions, ReadOptions, WriteOptions,
};
pub use types::{SequenceNumber, WriteBatch};

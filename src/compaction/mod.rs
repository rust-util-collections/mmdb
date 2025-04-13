//! Compaction: background process to merge SST files and reduce read amplification.

pub mod leveled;

pub use leveled::LeveledCompaction;

//! Compaction: background process to merge SST files and reduce read amplification.

pub mod leveled;
#[cfg(feature = "bench-metrics")]
pub(crate) mod profile;

pub use leveled::LeveledCompaction;

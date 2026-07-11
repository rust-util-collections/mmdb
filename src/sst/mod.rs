//! Sorted String Table (SST) implementation.
//!
//! An SST file is immutable once written. Structure:
//! - Data blocks (prefix-compressed key-value pairs)
//! - Meta block (bloom filter)
//! - Meta index block
//! - Index block (one entry per data block)
//! - Footer (fixed size, points to index and meta index)

pub mod block;
pub mod block_builder;
pub mod filter;
pub mod format;
pub mod table_builder;
pub mod table_reader;

/// Hard ceiling for single-block metadata (index, range-del, and bloom
/// filter blocks). The reader rejects any block above
/// `MAX_DECOMPRESSED_BLOCK_SIZE`; the margin leaves room for the restart
/// array and block framing.
pub(crate) const META_BLOCK_HARD_LIMIT: usize = table_reader::MAX_DECOMPRESSED_BLOCK_SIZE - 4096;

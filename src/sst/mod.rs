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

pub use table_builder::TableBuilder;
pub use table_reader::TableReader;

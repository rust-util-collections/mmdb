//! Write-Ahead Log (WAL) for crash recovery.
//!
//! Each WAL file corresponds to one MemTable lifecycle.
//! Records are appended sequentially, each with a CRC32 checksum.

pub mod reader;
pub mod record;
pub mod writer;

pub use reader::WalReader;
pub use writer::WalWriter;

//! Error types for MMDB.

use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Corruption: {0}")]
    Corruption(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Not found")]
    NotFound,

    #[error("DB is closed")]
    DbClosed,

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Manifest error: {0}")]
    Manifest(String),
}

pub type Result<T> = std::result::Result<T, Error>;

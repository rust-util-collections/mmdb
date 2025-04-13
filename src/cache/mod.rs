//! Caching layer: block cache and table cache.

pub mod block_cache;
pub mod table_cache;

pub use block_cache::BlockCache;
pub use table_cache::TableCache;

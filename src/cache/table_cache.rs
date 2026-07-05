//! Table cache: caches SST table readers to avoid repeated file open/parse.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::cache::block_cache::BlockCache;
use crate::error::{Result, ResultExt};
use crate::sst::table_reader::TableReader;
use crate::stats::DbStats;

/// Cache for open TableReader instances.
pub struct TableCache {
    db_path: PathBuf,
    inner: moka::sync::Cache<u64, Arc<TableReader>>,
    block_cache: Option<Arc<BlockCache>>,
    stats: Option<Arc<DbStats>>,
}

impl TableCache {
    /// Create a new table cache.
    /// `max_open_files` is the maximum number of TableReader entries retained by
    /// this cache; live Versions and iterators can pin additional readers.
    #[cfg(test)]
    pub fn new(db_path: &Path, max_open_files: u64, block_cache: Option<Arc<BlockCache>>) -> Self {
        Self::new_with_stats(db_path, max_open_files, block_cache, None)
    }

    /// Create a new table cache with optional stats.
    pub fn new_with_stats(
        db_path: &Path,
        max_open_files: u64,
        block_cache: Option<Arc<BlockCache>>,
        stats: Option<Arc<DbStats>>,
    ) -> Self {
        Self {
            db_path: db_path.to_path_buf(),
            inner: moka::sync::Cache::builder()
                .max_capacity(max_open_files)
                .build(),
            block_cache,
            stats,
        }
    }

    /// Get or open a table reader for the given file number.
    /// Uses moka's `try_get_with` to coalesce concurrent loads for the same file.
    /// `Error: Clone` lets every coalesced waiter receive the full typed error
    /// chain instead of a stringified copy.
    pub fn get_reader(&self, file_number: u64) -> Result<Arc<TableReader>> {
        let db_path = self.db_path.clone();
        let block_cache = self.block_cache.clone();
        let stats = self.stats.clone();
        self.inner
            .try_get_with(file_number, || {
                let path = db_path.join(format!("{:06}.sst", file_number));
                TableReader::open_with_all(&path, file_number, block_cache, stats).map(Arc::new)
            })
            .map_err(|e: Arc<crate::error::Error>| (*e).clone())
            .with_ctx(|| format!("table cache load failed for file {:06}", file_number))
    }

    /// Best-effort pre-warm for newly-built SST files, meant to be called
    /// from a caller's *unlocked* I/O phase (flush/compaction), before the
    /// short locked phase that calls `VersionSet::log_and_apply`.
    /// `log_and_apply` itself calls `get_reader` for every new file to
    /// install its `Arc<TableReader>` into the new `Version` — if that file
    /// is already warm here, that call becomes a cache hit instead of a
    /// blocking file open + footer/index/filter parse while `db_mutex` is
    /// held. Failures don't abort anything (the authoritative open, and its
    /// error handling, still happen inside `log_and_apply`), but they are
    /// logged: a freshly-written SST failing to open here is always
    /// anomalous and is the earliest available signal for issues that would
    /// otherwise only surface as an install failure moments later.
    pub fn prewarm(&self, file_numbers: impl IntoIterator<Item = u64>) {
        for number in file_numbers {
            if let Err(e) = self.get_reader(number) {
                tracing::warn!("prewarm of freshly-built SST {:06} failed: {}", number, e);
            }
        }
    }

    /// Evict a file from the cache.
    pub fn evict(&self, file_number: u64) {
        self.inner.invalidate(&file_number);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::table_builder::{TableBuildOptions, TableBuilder};

    #[test]
    fn test_table_cache() {
        let dir = tempfile::tempdir().unwrap();

        // Create an SST file
        let path = dir.path().join("000001.sst");
        let mut builder = TableBuilder::new(&path, TableBuildOptions::default()).unwrap();
        builder.add(b"key", b"value").unwrap();
        builder.finish().unwrap();

        let cache = TableCache::new(dir.path(), 100, None);

        // First access opens the file
        let reader = cache.get_reader(1).unwrap();
        let val = reader.get(b"key").unwrap();
        assert_eq!(val, Some(b"value".to_vec()));

        // Second access should hit cache
        let reader2 = cache.get_reader(1).unwrap();
        assert!(Arc::ptr_eq(&reader, &reader2));

        // Evict and re-open
        cache.evict(1);
        cache.inner.run_pending_tasks();
        let reader3 = cache.get_reader(1).unwrap();
        assert!(!Arc::ptr_eq(&reader, &reader3));
    }
}

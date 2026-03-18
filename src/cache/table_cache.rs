//! Table cache: caches open SST file readers to avoid repeated file open/parse.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::cache::block_cache::BlockCache;
use crate::error::Result;
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
    /// `max_open_files` is the maximum number of SST files to keep open.
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
    pub fn get_reader(&self, file_number: u64) -> Result<Arc<TableReader>> {
        if let Some(reader) = self.inner.get(&file_number) {
            return Ok(reader);
        }

        let path = self.db_path.join(format!("{:06}.sst", file_number));
        let reader = Arc::new(TableReader::open_with_all(
            &path,
            file_number,
            self.block_cache.clone(),
            self.stats.clone(),
        )?);
        self.inner.insert(file_number, reader.clone());
        Ok(reader)
    }

    /// Evict a file from the cache.
    pub fn evict(&self, file_number: u64) {
        self.inner.invalidate(&file_number);
    }

    /// Number of cached readers.
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
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

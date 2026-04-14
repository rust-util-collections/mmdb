//! LRU block cache for SST data blocks, backed by moka.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;

/// Cache key: (sst_file_number, block_offset).
type CacheKey = (u64, u64);

/// Cached block data.
type CacheValue = Arc<Vec<u8>>;

/// Thread-safe LRU cache for SST data blocks.
/// Supports pinning entries that should never be evicted (e.g., L0 index/filter blocks).
pub struct BlockCache {
    inner: moka::sync::Cache<CacheKey, CacheValue>,
    /// Pinned entries — never evicted by LRU. Protected by mutex.
    pinned: Mutex<HashMap<CacheKey, CacheValue>>,
    /// Track which offsets belong to each file for bulk invalidation.
    file_offsets: Mutex<HashMap<u64, HashSet<u64>>>,
}

impl BlockCache {
    /// Create a new block cache with the given capacity in bytes.
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            inner: moka::sync::Cache::builder()
                .max_capacity(capacity_bytes)
                .weigher(|_key: &CacheKey, value: &CacheValue| -> u32 {
                    value.len().min(u32::MAX as usize) as u32
                })
                .build(),
            pinned: Mutex::new(HashMap::new()),
            file_offsets: Mutex::new(HashMap::new()),
        }
    }

    /// Look up a cached block. Pinned entries are checked first.
    pub fn get(&self, file_number: u64, block_offset: u64) -> Option<Arc<Vec<u8>>> {
        let key = (file_number, block_offset);
        if let Some(v) = self.pinned.lock().get(&key) {
            return Some(v.clone());
        }
        self.inner.get(&key)
    }

    /// Insert a block into the cache.
    pub fn insert(&self, file_number: u64, block_offset: u64, data: Vec<u8>) -> Arc<Vec<u8>> {
        let arc = Arc::new(data);
        self.inner.insert((file_number, block_offset), arc.clone());
        self.file_offsets
            .lock()
            .entry(file_number)
            .or_default()
            .insert(block_offset);
        arc
    }

    /// Insert a pinned block — never evicted by LRU.
    /// Used for L0 index and filter blocks.
    pub fn insert_pinned(
        &self,
        file_number: u64,
        block_offset: u64,
        data: Vec<u8>,
    ) -> Arc<Vec<u8>> {
        let key = (file_number, block_offset);
        let arc = Arc::new(data);
        self.pinned.lock().insert(key, arc.clone());
        self.file_offsets
            .lock()
            .entry(file_number)
            .or_default()
            .insert(block_offset);
        arc
    }

    /// Unpin all entries for a specific file (e.g., when L0 file is compacted away).
    pub fn unpin_file(&self, file_number: u64) {
        self.pinned.lock().retain(|&(f, _), _| f != file_number);
    }

    /// Invalidate all cached blocks for a specific file.
    /// Called after compaction removes an SST file.
    pub fn invalidate_file(&self, file_number: u64) {
        self.unpin_file(file_number);
        let offsets = self.file_offsets.lock().remove(&file_number);
        if let Some(offsets) = offsets {
            for offset in offsets {
                self.inner.invalidate(&(file_number, offset));
            }
            self.inner.run_pending_tasks();
        }
    }

    /// Invalidate a specific cached block.
    pub fn invalidate(&self, file_number: u64, block_offset: u64) {
        self.inner.invalidate(&(file_number, block_offset));
        if let Some(offsets) = self.file_offsets.lock().get_mut(&file_number) {
            offsets.remove(&block_offset);
        }
    }

    /// Current approximate entry count (includes pinned entries).
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count() + self.pinned.lock().len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_cache_basic() {
        let cache = BlockCache::new(1024 * 1024); // 1MB

        // Insert
        cache.insert(1, 0, vec![1, 2, 3]);
        cache.insert(1, 4096, vec![4, 5, 6]);
        cache.insert(2, 0, vec![7, 8, 9]);

        // Lookup
        assert_eq!(*cache.get(1, 0).unwrap(), vec![1, 2, 3]);
        assert_eq!(*cache.get(1, 4096).unwrap(), vec![4, 5, 6]);
        assert_eq!(*cache.get(2, 0).unwrap(), vec![7, 8, 9]);

        // Miss
        assert!(cache.get(3, 0).is_none());
    }

    #[test]
    fn test_block_cache_invalidate() {
        let cache = BlockCache::new(1024 * 1024);
        cache.insert(1, 0, vec![1]);
        cache.insert(1, 100, vec![2]);
        cache.insert(2, 0, vec![3]);

        cache.invalidate(1, 0);
        cache.inner.run_pending_tasks();

        assert!(cache.get(1, 0).is_none());
        assert!(cache.get(1, 100).is_some()); // only specific block invalidated
        assert!(cache.get(2, 0).is_some());
    }
}

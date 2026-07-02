//! LRU block cache for SST data blocks, backed by moka.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use moka::notification::RemovalCause;
use parking_lot::Mutex;

/// Cache key: (sst_file_number, block_offset).
type CacheKey = (u64, u64);

/// Cached block data.
type CacheValue = Arc<Vec<u8>>;

/// Reverse index: which block offsets belong to each file, used for bulk
/// invalidation. Shared with the moka eviction listener so LRU-evicted offsets
/// are pruned automatically (otherwise it would grow without bound).
type FileOffsets = Arc<Mutex<HashMap<u64, HashSet<u64>>>>;

/// Thread-safe LRU cache for SST data blocks.
/// Supports pinning entries that should never be evicted (e.g., L0 index/filter blocks).
pub struct BlockCache {
    inner: moka::sync::Cache<CacheKey, CacheValue>,
    /// Pinned entries — never evicted by LRU. Protected by mutex.
    pinned: Mutex<HashMap<CacheKey, CacheValue>>,
    /// Track which offsets belong to each file for bulk invalidation.
    file_offsets: FileOffsets,
    /// When true (capacity 0), caching is disabled: inserts are no-ops and
    /// lookups always miss. Honors the documented "0 disables caching" option.
    disabled: bool,
}

impl BlockCache {
    /// Create a new block cache with the given capacity in bytes.
    /// A capacity of 0 disables caching entirely.
    pub fn new(capacity_bytes: u64) -> Self {
        let file_offsets: FileOffsets = Arc::new(Mutex::new(HashMap::new()));
        let listener_offsets = file_offsets.clone();
        let inner = moka::sync::Cache::builder()
            .max_capacity(capacity_bytes)
            .weigher(|_key: &CacheKey, value: &CacheValue| -> u32 {
                value.len().min(u32::MAX as usize) as u32
            })
            .eviction_listener(
                move |key: Arc<CacheKey>, _value: CacheValue, _cause: RemovalCause| {
                    let (file_number, block_offset) = *key;
                    let mut map = listener_offsets.lock();
                    if let Some(set) = map.get_mut(&file_number) {
                        set.remove(&block_offset);
                        if set.is_empty() {
                            map.remove(&file_number);
                        }
                    }
                },
            )
            .build();
        Self {
            inner,
            pinned: Mutex::new(HashMap::new()),
            file_offsets,
            disabled: capacity_bytes == 0,
        }
    }

    /// Look up a cached block. Pinned entries are checked first.
    pub fn get(&self, file_number: u64, block_offset: u64) -> Option<Arc<Vec<u8>>> {
        if self.disabled {
            return None;
        }
        let key = (file_number, block_offset);
        if let Some(v) = self.pinned.lock().get(&key) {
            return Some(v.clone());
        }
        self.inner.get(&key)
    }

    /// Insert a block into the cache.
    pub fn insert(&self, file_number: u64, block_offset: u64, data: Vec<u8>) -> Arc<Vec<u8>> {
        let arc = Arc::new(data);
        if self.disabled {
            return arc;
        }
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
        let arc = Arc::new(data);
        if self.disabled {
            return arc;
        }
        let key = (file_number, block_offset);
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
        let key = (file_number, block_offset);
        self.pinned.lock().remove(&key);
        self.inner.invalidate(&key);
        let is_empty = {
            let mut offsets = self.file_offsets.lock();
            if let Some(set) = offsets.get_mut(&file_number) {
                set.remove(&block_offset);
                set.is_empty()
            } else {
                false
            }
        };
        if is_empty {
            self.file_offsets.lock().remove(&file_number);
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

//! LRU block cache for SST data blocks, backed by moka.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
/// Supports pinning entries that should never be evicted — in practice this is
/// just the first data block of each L0 file (see `insert_pinned`).
pub struct BlockCache {
    inner: moka::sync::Cache<CacheKey, CacheValue>,
    /// Pinned entries — never evicted by LRU. Protected by mutex.
    pinned: Mutex<HashMap<CacheKey, CacheValue>>,
    /// Fast-path hint: number of entries currently in `pinned`. `get()` checks
    /// this atomic before acquiring `pinned`'s mutex so the common case (no
    /// pinned entries at all, or a lookup for a key that isn't one) skips the
    /// lock entirely. `Relaxed` is sufficient since this is only a hint —
    /// `pinned` (guarded by its own mutex) remains the authoritative state and
    /// is always consulted whenever the counter reads nonzero.
    pinned_count: AtomicUsize,
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
            pinned_count: AtomicUsize::new(0),
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
        // Fast path: `pinned` is empty for the vast majority of lookups (only
        // one data block per L0 file is ever pinned), so skip its mutex
        // entirely unless the hint counter says there's something to find.
        if self.pinned_count.load(Ordering::Relaxed) != 0
            && let Some(v) = self.pinned.lock().get(&key)
        {
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
    /// Used to pin the first data block (smallest key) of an L0 file, so the
    /// merging iterator's initial `peek()` is always a cache hit. Index and
    /// filter blocks are held directly as fields on `TableReader` and never
    /// pass through this cache at all.
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
        if self.pinned.lock().insert(key, arc.clone()).is_none() {
            self.pinned_count.fetch_add(1, Ordering::Relaxed);
        }
        self.file_offsets
            .lock()
            .entry(file_number)
            .or_default()
            .insert(block_offset);
        arc
    }

    /// Unpin all entries for a specific file (e.g., when L0 file is compacted away).
    pub fn unpin_file(&self, file_number: u64) {
        let mut pinned = self.pinned.lock();
        let before = pinned.len();
        pinned.retain(|&(f, _), _| f != file_number);
        let removed = before - pinned.len();
        if removed != 0 {
            self.pinned_count.fetch_sub(removed, Ordering::Relaxed);
        }
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
    fn test_block_cache_pinned_fast_path() {
        let cache = BlockCache::new(1024 * 1024);

        // Nothing pinned yet: the hint counter is zero, so get() must take the
        // fast path (skip `pinned`'s mutex) and still miss cleanly.
        assert_eq!(cache.pinned_count.load(Ordering::Relaxed), 0);
        assert!(cache.get(1, 0).is_none());

        // Pinning bumps the counter and is immediately visible via get().
        cache.insert_pinned(1, 0, vec![9, 9, 9]);
        assert_eq!(cache.pinned_count.load(Ordering::Relaxed), 1);
        assert_eq!(*cache.get(1, 0).unwrap(), vec![9, 9, 9]);

        // Re-pinning the same key must not double-count.
        cache.insert_pinned(1, 0, vec![9, 9, 9]);
        assert_eq!(cache.pinned_count.load(Ordering::Relaxed), 1);

        // A normal (non-pinned) insert for another key doesn't touch the counter,
        // and is still reachable once the fast path falls through to moka.
        cache.insert(2, 0, vec![1, 2, 3]);
        assert_eq!(cache.pinned_count.load(Ordering::Relaxed), 1);
        assert_eq!(*cache.get(2, 0).unwrap(), vec![1, 2, 3]);

        // Unpinning drops the counter back to zero; pinned entries are never
        // mirrored into the moka-backed store, so the lookup now misses entirely.
        cache.unpin_file(1);
        assert_eq!(cache.pinned_count.load(Ordering::Relaxed), 0);
        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn test_block_cache_unpin_file_multiple_pinned_entries() {
        let cache = BlockCache::new(1024 * 1024);
        cache.insert_pinned(1, 0, vec![1]);
        cache.insert_pinned(1, 4096, vec![2]);
        cache.insert_pinned(2, 0, vec![3]);
        assert_eq!(cache.pinned_count.load(Ordering::Relaxed), 3);

        // Unpinning one file must decrement by exactly the number of entries
        // removed for that file, leaving other files' pinned entries intact.
        cache.unpin_file(1);
        assert_eq!(cache.pinned_count.load(Ordering::Relaxed), 1);
        assert!(cache.get(1, 0).is_none());
        assert!(cache.get(1, 4096).is_none());
        assert_eq!(*cache.get(2, 0).unwrap(), vec![3]);
    }
}

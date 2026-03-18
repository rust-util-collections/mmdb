//! MemTable: in-memory sorted key-value store backed by a lock-free skiplist.

pub mod skiplist;
pub mod skiplist_impl;

pub use skiplist::SkipListMemTable;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::types::{InternalKey, SequenceNumber, ValueType};

/// A cached range tombstone from a memtable.
pub struct MemRangeTombstone {
    pub begin: Vec<u8>,
    pub end: Vec<u8>,
    pub seq: SequenceNumber,
}

/// A MemTable stores recent writes in memory before they are flushed to SST.
///
/// Keys are InternalKey-encoded (user_key + seq + type), values are raw bytes.
/// The skiplist sorts by `compare_internal_key` ordering.
pub struct MemTable {
    inner: SkipListMemTable,
    /// Approximate memory usage in bytes.
    approximate_size: AtomicUsize,
    /// Whether this memtable contains any RangeDeletion entries.
    /// Used to skip the expensive O(N) range tombstone scan in get().
    has_range_deletions: AtomicBool,
    /// Dedicated collection of range tombstones for O(T) coverage checks
    /// instead of O(N) full memtable scan. Protected by Mutex since writes
    /// are single-writer (group commit model).
    range_tombstones: parking_lot::Mutex<Vec<MemRangeTombstone>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            inner: SkipListMemTable::new(),
            approximate_size: AtomicUsize::new(0),
            has_range_deletions: AtomicBool::new(false),
            range_tombstones: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// Insert an entry. `key` is the user key; it will be encoded as an InternalKey.
    pub fn put(&self, key: &[u8], value: &[u8], sequence: SequenceNumber, value_type: ValueType) {
        let ikey = InternalKey::new(key, sequence, value_type);
        let val = match value_type {
            ValueType::Value => value.to_vec(),
            ValueType::Deletion => Vec::new(),
            ValueType::RangeDeletion => {
                // Add to dedicated range tombstone collection for O(T) lookup
                self.range_tombstones.lock().push(MemRangeTombstone {
                    begin: key.to_vec(),
                    end: value.to_vec(),
                    seq: sequence,
                });
                // Release: ensures this flag is visible to any reader that
                // subsequently Acquires skiplist entries via atomic pointers.
                self.has_range_deletions.store(true, Ordering::Release);
                value.to_vec()
            }
        };
        let entry_size = ikey.encoded_len() + val.len() + 160; // Node struct overhead (inline [AtomicPtr; 12] + Vec headers)
        self.inner.insert(ikey.into_bytes(), val);
        self.approximate_size
            .fetch_add(entry_size, Ordering::Relaxed);
    }

    /// Look up a user key at or below the given sequence number.
    /// Returns `Some(Some(value))` for a found value, `Some(None)` for a deletion tombstone,
    /// or `None` if the key is not in this MemTable.
    pub fn get(&self, key: &[u8], sequence: SequenceNumber) -> Option<Option<Vec<u8>>> {
        self.get_with_seq(key, sequence).map(|(result, _)| result)
    }

    /// Like `get`, but also returns the sequence number of the found entry.
    pub fn get_with_seq(
        &self,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Option<(Option<Vec<u8>>, SequenceNumber)> {
        let search_key = InternalKey::new(key, sequence, ValueType::Value);
        self.inner.get_with_seq(search_key.as_bytes(), key)
    }

    /// Approximate memory usage in bytes.
    pub fn approximate_size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed)
    }

    /// Return an iterator over all entries in order.
    /// Each item is (encoded_internal_key, value).
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        self.inner.iter()
    }

    /// Return an iterator over all entries in reverse order.
    pub fn iter_rev(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        self.inner.iter_rev()
    }

    /// Get a raw pointer to the underlying skiplist for cursor-based iteration.
    /// The pointer is valid as long as this MemTable is alive.
    pub fn skiplist_ref(
        &self,
    ) -> *const skiplist_impl::ConcurrentSkipList<skiplist::OrdInternalKey, Vec<u8>> {
        self.inner.skiplist_ptr()
    }

    /// Whether this memtable contains any range deletion entries.
    pub fn has_range_deletions(&self) -> bool {
        // Acquire: pairs with the Release store in put() to ensure
        // visibility across cores on weakly-ordered architectures (ARM/RISC-V).
        self.has_range_deletions.load(Ordering::Acquire)
    }

    /// Find the highest-seq range tombstone covering `user_key` with seq <= `read_seq`.
    /// Returns 0 if no covering tombstone exists.
    /// O(T) where T = number of range tombstones, instead of O(N) full memtable scan.
    pub fn max_covering_tombstone_seq(
        &self,
        user_key: &[u8],
        read_seq: SequenceNumber,
    ) -> SequenceNumber {
        if !self.has_range_deletions() {
            return 0;
        }
        let tombstones = self.range_tombstones.lock();
        let mut max_seq: SequenceNumber = 0;
        for rt in tombstones.iter() {
            if rt.seq > read_seq {
                continue;
            }
            if user_key >= rt.begin.as_slice()
                && user_key < rt.end.as_slice()
                && rt.seq > max_seq
            {
                max_seq = rt.seq;
            }
        }
        max_seq
    }

    /// Return true if empty.
    pub fn is_empty(&self) -> bool {
        self.approximate_size.load(Ordering::Relaxed) == 0
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_put_get() {
        let mt = MemTable::new();
        mt.put(b"key1", b"value1", 1, ValueType::Value);
        mt.put(b"key2", b"value2", 2, ValueType::Value);

        // Should find key1 at seq >= 1
        assert_eq!(mt.get(b"key1", 1), Some(Some(b"value1".to_vec())));
        assert_eq!(mt.get(b"key2", 2), Some(Some(b"value2".to_vec())));

        // key1 not visible at seq 0
        assert_eq!(mt.get(b"key1", 0), None);
    }

    #[test]
    fn test_memtable_delete() {
        let mt = MemTable::new();
        mt.put(b"key1", b"value1", 1, ValueType::Value);
        mt.put(b"key1", b"", 2, ValueType::Deletion);

        // At seq 2, should see tombstone
        assert_eq!(mt.get(b"key1", 2), Some(None));
        // At seq 1, should see the value
        assert_eq!(mt.get(b"key1", 1), Some(Some(b"value1".to_vec())));
    }

    #[test]
    fn test_memtable_overwrite() {
        let mt = MemTable::new();
        mt.put(b"key1", b"v1", 1, ValueType::Value);
        mt.put(b"key1", b"v2", 2, ValueType::Value);

        assert_eq!(mt.get(b"key1", 2), Some(Some(b"v2".to_vec())));
        assert_eq!(mt.get(b"key1", 1), Some(Some(b"v1".to_vec())));
        assert_eq!(mt.get(b"key1", 10), Some(Some(b"v2".to_vec())));
    }

    #[test]
    fn test_memtable_iterator() {
        let mt = MemTable::new();
        mt.put(b"b", b"2", 1, ValueType::Value);
        mt.put(b"a", b"1", 2, ValueType::Value);
        mt.put(b"c", b"3", 3, ValueType::Value);

        let entries: Vec<_> = mt.iter().collect();
        // Should be sorted by internal key ordering: a, b, c
        assert_eq!(entries.len(), 3);
        let keys: Vec<&[u8]> = entries
            .iter()
            .map(|(k, _)| InternalKey::from_encoded(k.clone()).user_key().to_vec())
            .map(|_| &[] as &[u8])
            .collect();
        // Better test: check user key ordering
        let user_keys: Vec<Vec<u8>> = entries
            .iter()
            .map(|(k, _)| {
                let ik = InternalKey::from_encoded(k.clone());
                ik.user_key().to_vec()
            })
            .collect();
        let _ = keys;
        assert_eq!(user_keys[0], b"a");
        assert_eq!(user_keys[1], b"b");
        assert_eq!(user_keys[2], b"c");
    }

    #[test]
    fn test_memtable_approximate_size() {
        let mt = MemTable::new();
        assert_eq!(mt.approximate_size(), 0);
        mt.put(b"key", b"value", 1, ValueType::Value);
        assert!(mt.approximate_size() > 0);
    }

    #[test]
    fn test_memtable_single_writer_concurrent_readers() {
        use std::sync::Arc;
        use std::thread;

        let mt = Arc::new(MemTable::new());
        let num_entries = 800;

        // Single writer inserts all entries first (matches DB group commit model).
        for i in 0..num_entries {
            let t = i / 100;
            let j = i % 100;
            let key = format!("t{}_k{:04}", t, j);
            let val = format!("t{}_v{}", t, j);
            let seq = (i + 1) as u64;
            mt.put(key.as_bytes(), val.as_bytes(), seq, ValueType::Value);
        }

        // Spawn concurrent readers to verify all entries.
        let mut handles = Vec::new();
        for _ in 0..8 {
            let mt = Arc::clone(&mt);
            handles.push(thread::spawn(move || {
                for i in 0..num_entries {
                    let t = i / 100;
                    let j = i % 100;
                    let key = format!("t{}_k{:04}", t, j);
                    let val = format!("t{}_v{}", t, j);
                    let seq = (i + 1) as u64;
                    let result = mt.get(key.as_bytes(), seq);
                    assert_eq!(
                        result,
                        Some(Some(val.into_bytes())),
                        "missing key {} at seq {}",
                        key,
                        seq
                    );
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Verify total count via iterator
        let total: usize = mt.iter().count();
        assert_eq!(total, num_entries);
    }

    #[test]
    fn test_memtable_prefix_similar_keys() {
        let mt = MemTable::new();
        mt.put(b"a", b"val_a", 1, ValueType::Value);
        mt.put(b"ab", b"val_ab", 2, ValueType::Value);
        mt.put(b"abc", b"val_abc", 3, ValueType::Value);

        // Each key should be independently retrievable
        assert_eq!(mt.get(b"a", 10), Some(Some(b"val_a".to_vec())));
        assert_eq!(mt.get(b"ab", 10), Some(Some(b"val_ab".to_vec())));
        assert_eq!(mt.get(b"abc", 10), Some(Some(b"val_abc".to_vec())));

        // Non-existent prefix-similar keys should not be found
        assert_eq!(mt.get(b"abcd", 10), None);
        assert_eq!(mt.get(b"b", 10), None);

        // Visibility by sequence: "abc" at seq 3 should not be visible at seq 2
        assert_eq!(mt.get(b"abc", 2), None);
        assert_eq!(mt.get(b"abc", 3), Some(Some(b"val_abc".to_vec())));

        // Iterator should yield keys in sorted order: a, ab, abc
        let entries: Vec<_> = mt.iter().collect();
        assert_eq!(entries.len(), 3);
        let user_keys: Vec<Vec<u8>> = entries
            .iter()
            .map(|(k, _)| InternalKey::from_encoded(k.clone()).user_key().to_vec())
            .collect();
        assert_eq!(user_keys[0], b"a");
        assert_eq!(user_keys[1], b"ab");
        assert_eq!(user_keys[2], b"abc");
    }

    #[test]
    fn test_memtable_is_empty_states() {
        let mt = MemTable::new();

        // Brand new memtable should be empty
        assert!(mt.is_empty());
        assert_eq!(mt.approximate_size(), 0);

        // After a single put, no longer empty
        mt.put(b"k", b"v", 1, ValueType::Value);
        assert!(!mt.is_empty());
        assert!(mt.approximate_size() > 0);

        // After a deletion, still not empty (tombstone is an entry)
        let mt2 = MemTable::new();
        assert!(mt2.is_empty());
        mt2.put(b"k", b"", 1, ValueType::Deletion);
        assert!(!mt2.is_empty());
    }
}

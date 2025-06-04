//! SkipList-based MemTable implementation using a custom concurrent skiplist.

use super::skiplist_impl::ConcurrentSkipList;
use crate::types::{InternalKeyRef, ValueType, compare_internal_key};

/// Newtype wrapper for internal keys that implements `Ord` using `compare_internal_key`.
/// This ensures the skip list maintains logical internal key order directly,
/// eliminating the need for O(N log N) re-sorting on iteration.
#[derive(Clone, Debug)]
pub struct OrdInternalKey(Vec<u8>);

impl OrdInternalKey {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl PartialEq for OrdInternalKey {
    fn eq(&self, other: &Self) -> bool {
        compare_internal_key(&self.0, &other.0) == std::cmp::Ordering::Equal
    }
}

impl Eq for OrdInternalKey {}

impl PartialOrd for OrdInternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdInternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_internal_key(&self.0, &other.0)
    }
}

/// Lock-free concurrent skiplist MemTable.
///
/// Keys are encoded InternalKeys (user_key + 8-byte trailer).
/// The skiplist is ordered by `compare_internal_key` via the `OrdInternalKey` newtype.
pub struct SkipListMemTable {
    map: ConcurrentSkipList<OrdInternalKey, Vec<u8>>,
}

impl Default for SkipListMemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl SkipListMemTable {
    pub fn new() -> Self {
        Self {
            map: ConcurrentSkipList::new(),
        }
    }

    /// Insert an encoded internal key and value.
    pub fn insert(&self, encoded_key: Vec<u8>, value: Vec<u8>) {
        self.map.insert(OrdInternalKey(encoded_key), value);
    }

    /// Look up a user key. `search_key` is an encoded InternalKey used to seek.
    /// `user_key` is the raw user key for matching.
    ///
    /// Returns:
    /// - `Some(Some(value))` if a Value entry is found
    /// - `Some(None)` if a Deletion entry is found
    /// - `None` if no entry for this user key exists at or below the search sequence
    pub fn get(&self, search_key: &[u8], user_key: &[u8]) -> Option<Option<Vec<u8>>> {
        // With OrdInternalKey, the skip list is sorted by (user_key ASC, seq DESC).
        // Use O(log N) lower_bound to find the first entry >= search_key,
        // then check a few entries at level 0 for the matching user_key.
        let search = OrdInternalKey(search_key.to_vec());

        // lower_bound gives us the first entry >= search_key.
        // For the same user_key, higher seq comes first, so the first match
        // is the best (newest version <= target seq).
        let (k, v) = self.map.lower_bound(&search)?;
        let kb = k.as_bytes();
        if kb.len() < 8 {
            return None;
        }
        let entry_uk = &kb[..kb.len() - 8];
        if entry_uk == user_key {
            let entry_ref = InternalKeyRef::new(kb);
            return Some(match entry_ref.value_type() {
                ValueType::Value => Some(v),
                ValueType::Deletion | ValueType::RangeDeletion => None,
            });
        }
        // The first entry >= search has a different user_key → not found.
        None
    }

    /// Iterate over all entries in internal key order (user_key ASC, seq DESC).
    /// With `OrdInternalKey`, the skip list is already in the correct order —
    /// no sorting needed.
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        self.map.iter().map(|(k, v)| (k.0, v))
    }

    /// Iterate over all entries in reverse internal key order.
    pub fn iter_rev(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        self.map.iter().rev().map(|(k, v)| (k.0, v))
    }

    /// Iterate over a range of entries by OrdInternalKey bounds.
    pub fn range(
        &self,
        bounds: impl std::ops::RangeBounds<OrdInternalKey>,
    ) -> impl DoubleEndedIterator<Item = (Vec<u8>, Vec<u8>)> {
        self.map.range(bounds).map(|(k, v)| (k.0, v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::InternalKey;

    #[test]
    fn test_skiplist_insert_and_get() {
        let sl = SkipListMemTable::new();
        let ik = InternalKey::new(b"hello", 1, ValueType::Value);
        sl.insert(ik.into_bytes(), b"world".to_vec());

        let search = InternalKey::new(b"hello", 1, ValueType::Value);
        let result = sl.get(search.as_bytes(), b"hello");
        assert_eq!(result, Some(Some(b"world".to_vec())));
    }

    #[test]
    fn test_skiplist_seq_visibility() {
        let sl = SkipListMemTable::new();

        let ik1 = InternalKey::new(b"key", 5, ValueType::Value);
        sl.insert(ik1.into_bytes(), b"v5".to_vec());

        let ik2 = InternalKey::new(b"key", 10, ValueType::Value);
        sl.insert(ik2.into_bytes(), b"v10".to_vec());

        // Search at seq 10 should find v10
        let search = InternalKey::new(b"key", 10, ValueType::Value);
        assert_eq!(
            sl.get(search.as_bytes(), b"key"),
            Some(Some(b"v10".to_vec()))
        );

        // Search at seq 7 should find v5
        let search = InternalKey::new(b"key", 7, ValueType::Value);
        assert_eq!(
            sl.get(search.as_bytes(), b"key"),
            Some(Some(b"v5".to_vec()))
        );

        // Search at seq 3 should find nothing
        let search = InternalKey::new(b"key", 3, ValueType::Value);
        assert_eq!(sl.get(search.as_bytes(), b"key"), None);
    }

    #[test]
    fn test_skiplist_iter_order() {
        let sl = SkipListMemTable::new();

        // Insert variable-length keys out of logical order
        let ik1 = InternalKey::new(b"b", 1, ValueType::Value);
        sl.insert(ik1.into_bytes(), b"2".to_vec());
        let ik2 = InternalKey::new(b"a", 2, ValueType::Value);
        sl.insert(ik2.into_bytes(), b"1".to_vec());
        let ik3 = InternalKey::new(b"ab", 3, ValueType::Value);
        sl.insert(ik3.into_bytes(), b"12".to_vec());

        let entries: Vec<_> = sl.iter().collect();
        assert_eq!(entries.len(), 3);

        // Verify logical order: "a" < "ab" < "b"
        let user_keys: Vec<&[u8]> = entries.iter().map(|(k, _)| &k[..k.len() - 8]).collect();
        assert_eq!(user_keys[0], b"a");
        assert_eq!(user_keys[1], b"ab");
        assert_eq!(user_keys[2], b"b");
    }

    #[test]
    fn test_skiplist_same_key_seq_order() {
        let sl = SkipListMemTable::new();

        let ik1 = InternalKey::new(b"key", 1, ValueType::Value);
        sl.insert(ik1.into_bytes(), b"v1".to_vec());
        let ik2 = InternalKey::new(b"key", 5, ValueType::Value);
        sl.insert(ik2.into_bytes(), b"v5".to_vec());
        let ik3 = InternalKey::new(b"key", 3, ValueType::Value);
        sl.insert(ik3.into_bytes(), b"v3".to_vec());

        let entries: Vec<_> = sl.iter().collect();
        assert_eq!(entries.len(), 3);

        // Within same user_key, higher seq should come first
        let seqs: Vec<u64> = entries
            .iter()
            .map(|(k, _)| InternalKeyRef::new(k).sequence())
            .collect();
        assert_eq!(seqs, vec![5, 3, 1]);
    }

    #[test]
    fn test_skiplist_iter_rev() {
        let sl = SkipListMemTable::new();

        let ik1 = InternalKey::new(b"a", 1, ValueType::Value);
        sl.insert(ik1.into_bytes(), b"1".to_vec());
        let ik2 = InternalKey::new(b"b", 2, ValueType::Value);
        sl.insert(ik2.into_bytes(), b"2".to_vec());
        let ik3 = InternalKey::new(b"c", 3, ValueType::Value);
        sl.insert(ik3.into_bytes(), b"3".to_vec());

        let entries: Vec<_> = sl.iter_rev().collect();
        assert_eq!(entries.len(), 3);

        // Reverse order: c, b, a
        let user_keys: Vec<&[u8]> = entries.iter().map(|(k, _)| &k[..k.len() - 8]).collect();
        assert_eq!(user_keys[0], b"c");
        assert_eq!(user_keys[1], b"b");
        assert_eq!(user_keys[2], b"a");
    }
}

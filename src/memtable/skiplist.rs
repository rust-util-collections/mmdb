//! SkipList-based MemTable implementation using a custom concurrent skiplist.

use std::cmp::Ordering;
use std::sync::Arc;

use super::skiplist_impl::ConcurrentSkipList;
use crate::iterator::merge::SeekableIterator;
use crate::types::{InternalKeyRef, LazyValue, ValueType, compare_internal_key};

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
        compare_internal_key(&self.0, &other.0) == Ordering::Equal
    }
}

impl Eq for OrdInternalKey {}

impl PartialOrd for OrdInternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdInternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
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

    /// Get a raw pointer to the underlying skiplist for cursor-based iteration.
    pub fn skiplist_ptr(&self) -> *const ConcurrentSkipList<OrdInternalKey, Vec<u8>> {
        &self.map as *const _
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
        self.get_with_seq(search_key, user_key)
            .map(|(result, _seq)| result)
    }

    /// Like `get`, but also returns the sequence number of the found entry.
    /// Needed for range tombstone vs point entry sequence comparison.
    pub fn get_with_seq(
        &self,
        search_key: &[u8],
        user_key: &[u8],
    ) -> Option<(Option<Vec<u8>>, crate::types::SequenceNumber)> {
        let search = OrdInternalKey(search_key.to_vec());
        let (k, v) = self.map.lower_bound(&search)?;
        let kb = k.as_bytes();
        if kb.len() < 8 {
            return None;
        }
        let entry_uk = &kb[..kb.len() - 8];
        if entry_uk == user_key {
            let entry_ref = InternalKeyRef::new(kb);
            let seq = entry_ref.sequence();
            return Some((
                match entry_ref.value_type() {
                    ValueType::Value => Some(v),
                    ValueType::Deletion | ValueType::RangeDeletion => None,
                },
                seq,
            ));
        }
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

/// Cursor-based streaming iterator over a SkipListMemTable.
/// Does NOT collect/clone the entire skiplist upfront — walks the level-0
/// pointer chain on demand, cloning each entry only when `next()` is called.
///
/// Implements `SeekableIterator` so it can be used as a seekable source
/// in `MergingIterator`, eliminating the O(N) iterator creation cost.
pub struct MemTableCursorIter {
    /// Arc to keep the underlying memtable (and its skiplist) alive.
    _memtable: Arc<crate::memtable::MemTable>,
    /// Current node pointer in the level-0 chain. null = exhausted.
    cursor: *const (),
    /// Reference to the skiplist for node_kv/node_next0 access.
    skiplist: *const ConcurrentSkipList<OrdInternalKey, Vec<u8>>,
}

// SAFETY: The skiplist nodes are heap-allocated with stable pointers.
// The Arc<MemTable> keeps the skiplist alive for the iterator's lifetime.
// Reads are lock-free (Acquire ordering on atomic next pointers).
unsafe impl Send for MemTableCursorIter {}

impl MemTableCursorIter {
    pub fn new(memtable: Arc<crate::memtable::MemTable>) -> Self {
        let skiplist = memtable.skiplist_ref();
        let head = unsafe { &*skiplist }.head_ptr();
        Self {
            _memtable: memtable,
            cursor: head,
            skiplist,
        }
    }

    #[inline]
    fn sl(&self) -> &ConcurrentSkipList<OrdInternalKey, Vec<u8>> {
        unsafe { &*self.skiplist }
    }

    fn seek_internal(&mut self, target: &[u8]) {
        let search = OrdInternalKey(target.to_vec());
        self.cursor = self.sl().seek_ge_raw(&search);
    }
}

impl Iterator for MemTableCursorIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.is_null() {
            return None;
        }
        let (k, v) = unsafe { self.sl().node_kv(self.cursor) };
        let result = (k.as_bytes().to_vec(), v.clone());
        self.cursor = unsafe { self.sl().node_next0(self.cursor) };
        Some(result)
    }
}

impl SeekableIterator for MemTableCursorIter {
    fn seek_to(&mut self, target: &[u8]) {
        self.seek_internal(target);
    }

    fn current(&self) -> Option<(Vec<u8>, LazyValue)> {
        if self.cursor.is_null() {
            return None;
        }
        let (k, v) = unsafe { self.sl().node_kv(self.cursor) };
        Some((
            k.as_bytes().to_vec(),
            LazyValue::Inline(v.clone()),
        ))
    }

    /// Copy directly into caller buffers, reusing their capacity.
    /// After the first call, subsequent calls are memcpy-only (zero heap allocation).
    fn next_into(&mut self, key_buf: &mut Vec<u8>, value_buf: &mut Vec<u8>) -> bool {
        if self.cursor.is_null() {
            return false;
        }
        let (k, v) = unsafe { self.sl().node_kv(self.cursor) };
        key_buf.clear();
        key_buf.extend_from_slice(k.as_bytes());
        value_buf.clear();
        value_buf.extend_from_slice(v);
        self.cursor = unsafe { self.sl().node_next0(self.cursor) };
        true
    }

    /// Override default next_lazy to avoid temporary Vec::new() allocation.
    /// Writes key into caller buffer, returns value as LazyValue directly.
    fn next_lazy(&mut self, key_buf: &mut Vec<u8>) -> Option<LazyValue> {
        if self.cursor.is_null() {
            return None;
        }
        let (k, v) = unsafe { self.sl().node_kv(self.cursor) };
        key_buf.clear();
        key_buf.extend_from_slice(k.as_bytes());
        let lv = LazyValue::Inline(v.clone());
        self.cursor = unsafe { self.sl().node_next0(self.cursor) };
        Some(lv)
    }

    fn prev(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        if self.cursor.is_null() {
            // Exhausted forward — seek to last for backward iteration.
            // O(1) via cached tail pointer.
            let ptr = self.sl().tail_ptr();
            if ptr.is_null() {
                return None;
            }
            let (k, v) = unsafe { self.sl().node_kv(ptr) };
            let result = (
                k.as_bytes().to_vec(),
                LazyValue::Inline(v.clone()),
            );
            // Follow prev0 for the next prev() call — O(1).
            self.cursor = unsafe { self.sl().node_prev0(ptr) };
            return Some(result);
        }
        // We have a current cursor position. Follow the backward pointer.
        // O(1) via prev0 instead of O(log N) seek_lt_raw.
        let prev_ptr = unsafe { self.sl().node_prev0(self.cursor) };
        if prev_ptr.is_null() {
            self.cursor = std::ptr::null();
            return None;
        }
        let (pk, pv) = unsafe { self.sl().node_kv(prev_ptr) };
        let result = (
            pk.as_bytes().to_vec(),
            LazyValue::Inline(pv.clone()),
        );
        self.cursor = prev_ptr;
        Some(result)
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        let search = OrdInternalKey(target.to_vec());
        // seek_le_raw: find last entry <= target
        let ptr = self.sl().seek_le_raw(&search);
        if ptr.is_null() {
            self.cursor = std::ptr::null();
        } else {
            // Position on this entry — next call to next() returns it
            self.cursor = ptr;
        }
    }

    fn seek_to_first(&mut self) {
        self.cursor = self.sl().head_ptr();
    }

    fn seek_to_last(&mut self) {
        let ptr = self.sl().tail_ptr();
        self.cursor = ptr;
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

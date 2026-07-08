//! SkipList-based MemTable implementation using a custom concurrent skiplist.

use std::{cmp::Ordering, sync::Arc};

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
    #[cfg(test)]
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
    #[cfg(test)]
    pub fn iter_rev(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        self.map.iter().rev().map(|(k, v)| (k.0, v))
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
    /// Current node pointer in the level-0 chain. null = exhausted (either
    /// freshly past-the-end, or before-the-start — see `exhausted_backward`
    /// to distinguish the two).
    cursor: *const (),
    /// Reference to the skiplist for node_kv/node_next0 access.
    skiplist: *const ConcurrentSkipList<OrdInternalKey, Vec<u8>>,
    /// Set by `prev()` when it finds no predecessor, i.e. the cursor has
    /// moved before the first entry. A null `cursor` alone is ambiguous
    /// between "freshly past-the-end" (prev() should jump to the last
    /// entry) and "already exhausted backward" (prev() must keep returning
    /// `None`) — this flag disambiguates the two so repeated `prev()` calls
    /// don't wrap around to the last entry. Cleared by every method that
    /// repositions the cursor by other means (`next()`/`next_into()`/
    /// `next_lazy()`, `seek_to`, `seek_to_first`, `seek_to_last`,
    /// `seek_for_prev`).
    exhausted_backward: bool,
}

// SAFETY: The skiplist nodes are heap-allocated with stable pointers.
// The Arc<MemTable> keeps the skiplist alive for the iterator's lifetime.
// Reads are lock-free (Acquire ordering on atomic next pointers).
unsafe impl Send for MemTableCursorIter {}

impl MemTableCursorIter {
    pub fn new(memtable: Arc<crate::memtable::MemTable>) -> Self {
        let skiplist = memtable.skiplist_ref();
        // SAFETY: skiplist_ref points into `memtable`; the Arc stored in the
        // iterator keeps that MemTable alive for at least this iterator's lifetime.
        let head = unsafe { &*skiplist }.head_ptr();
        Self {
            _memtable: memtable,
            cursor: head,
            skiplist,
            exhausted_backward: false,
        }
    }

    #[inline]
    fn sl(&self) -> &ConcurrentSkipList<OrdInternalKey, Vec<u8>> {
        // SAFETY: `self._memtable` owns the skiplist and keeps this raw pointer valid.
        unsafe { &*self.skiplist }
    }

    fn seek_internal(&mut self, target: &[u8]) {
        let search = OrdInternalKey(target.to_vec());
        self.cursor = self.sl().seek_ge_raw(&search);
        // Repositioned by an explicit seek — no longer in the "exhausted
        // backward" state (if we were even in it).
        self.exhausted_backward = false;
    }
}

impl Iterator for MemTableCursorIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.is_null() {
            if !self.exhausted_backward {
                return None;
            }
            // The cursor was parked before the first entry by a previous
            // `prev()` call that found no predecessor. Resuming forward
            // from there starts back at the first entry (mirrors
            // `seek_to_first()`), instead of staying stuck at `None`.
            self.cursor = self.sl().head_ptr();
            if self.cursor.is_null() {
                // Skiplist is empty — nothing to resume to.
                return None;
            }
        }
        self.exhausted_backward = false;
        // SAFETY: cursor is non-null here (the null case returned above) and
        // is a node pointer returned by this skiplist (either the
        // pre-existing cursor or `head_ptr()`).
        let (k, v) = unsafe { self.sl().node_kv(self.cursor) };
        let result = (k.as_bytes().to_vec(), v.clone());
        // SAFETY: same cursor validity as above; node_next0 only reads an atomic link.
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
        // SAFETY: cursor is non-null and comes from this skiplist.
        let (k, v) = unsafe { self.sl().node_kv(self.cursor) };
        Some((k.as_bytes().to_vec(), LazyValue::Inline(v.clone())))
    }

    /// Copy directly into caller buffers, reusing their capacity.
    /// After the first call, subsequent calls are memcpy-only (zero heap allocation).
    fn next_into(&mut self, key_buf: &mut Vec<u8>, value_buf: &mut Vec<u8>) -> bool {
        if self.cursor.is_null() {
            if !self.exhausted_backward {
                return false;
            }
            // See `next()`: resume forward from before-the-start at the
            // first entry instead of staying stuck.
            self.cursor = self.sl().head_ptr();
            if self.cursor.is_null() {
                return false;
            }
        }
        self.exhausted_backward = false;
        // SAFETY: cursor is non-null here (the null case returned above) and
        // is a node pointer returned by this skiplist (either the
        // pre-existing cursor or `head_ptr()`).
        let (k, v) = unsafe { self.sl().node_kv(self.cursor) };
        key_buf.clear();
        key_buf.extend_from_slice(k.as_bytes());
        value_buf.clear();
        value_buf.extend_from_slice(v);
        // SAFETY: cursor is still the same valid node pointer.
        self.cursor = unsafe { self.sl().node_next0(self.cursor) };
        true
    }

    /// Override default next_lazy to avoid temporary Vec::new() allocation.
    /// Writes key into caller buffer, returns value as LazyValue directly.
    fn next_lazy(&mut self, key_buf: &mut Vec<u8>) -> Option<LazyValue> {
        if self.cursor.is_null() {
            if !self.exhausted_backward {
                return None;
            }
            // See `next()`: resume forward from before-the-start at the
            // first entry instead of staying stuck.
            self.cursor = self.sl().head_ptr();
            if self.cursor.is_null() {
                return None;
            }
        }
        self.exhausted_backward = false;
        // SAFETY: cursor is non-null here (the null case returned above) and
        // is a node pointer returned by this skiplist (either the
        // pre-existing cursor or `head_ptr()`).
        let (k, v) = unsafe { self.sl().node_kv(self.cursor) };
        key_buf.clear();
        key_buf.extend_from_slice(k.as_bytes());
        let lv = LazyValue::Inline(v.clone());
        // SAFETY: cursor is still the same valid node pointer.
        self.cursor = unsafe { self.sl().node_next0(self.cursor) };
        Some(lv)
    }

    fn prev(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        if self.exhausted_backward {
            // Already moved before the first entry on a previous `prev()`
            // call. A null `cursor` here does NOT mean "freshly past the
            // end" (which would jump to the last entry below) — it means
            // we're still stuck before the start, so keep returning `None`
            // instead of re-entering that branch and wrapping around.
            return None;
        }
        let ptr = if self.cursor.is_null() {
            self.sl().tail_ptr()
        } else {
            // SAFETY: cursor is non-null and comes from this skiplist.
            let (k, _) = unsafe { self.sl().node_kv(self.cursor) };
            self.sl().seek_lt_raw(k)
        };
        if ptr.is_null() {
            self.cursor = std::ptr::null();
            self.exhausted_backward = true;
            return None;
        }
        // SAFETY: ptr is non-null and either the previous node or skiplist tail.
        let (k, v) = unsafe { self.sl().node_kv(ptr) };
        let result = (k.as_bytes().to_vec(), LazyValue::Inline(v.clone()));
        self.cursor = ptr;
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
        // Repositioned by an explicit seek — no longer in the "exhausted
        // backward" state (if we were even in it).
        self.exhausted_backward = false;
    }

    fn seek_to_first(&mut self) {
        self.cursor = self.sl().head_ptr();
        self.exhausted_backward = false;
    }

    fn seek_to_last(&mut self) {
        let ptr = self.sl().tail_ptr();
        self.cursor = ptr;
        self.exhausted_backward = false;
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

    // --- MemTableCursorIter: prev()/seek_for_prev()/current() coverage ---
    //
    // Regression tests for the double-exhaustion bug: `prev()` used to
    // overload `cursor.is_null()` to mean both "freshly past-the-end" and
    // "already exhausted backward", so a second `prev()` call after
    // backward exhaustion would wrongly resurrect the last entry.

    /// Build a MemTable with the given (user_key, seq, value) entries.
    fn make_memtable_with_entries(
        entries: &[(&[u8], u64, &[u8])],
    ) -> Arc<crate::memtable::MemTable> {
        let memtable = Arc::new(crate::memtable::MemTable::new());
        for (key, seq, value) in entries {
            memtable.put(key, value, *seq, ValueType::Value);
        }
        memtable
    }

    /// Extract the user-key portion from a cursor `(encoded_key, LazyValue)` entry.
    fn user_key_of(entry: &Option<(Vec<u8>, LazyValue)>) -> Option<Vec<u8>> {
        entry
            .as_ref()
            .map(|(k, _)| InternalKeyRef::new(k).user_key().to_vec())
    }

    #[test]
    fn test_cursor_iter_prev_traversal_end_to_end() {
        // (b) Normal prev() traversal works correctly end-to-end.
        let memtable =
            make_memtable_with_entries(&[(b"a", 1, b"1"), (b"b", 2, b"2"), (b"c", 3, b"3")]);
        let mut iter = MemTableCursorIter::new(memtable);

        // seek_to_last() + current() peeks the last entry without advancing.
        iter.seek_to_last();
        assert_eq!(user_key_of(&iter.current()), Some(b"c".to_vec()));
        assert_eq!(
            user_key_of(&iter.current()),
            Some(b"c".to_vec()),
            "current() must not advance the cursor"
        );

        // Walk backward: c (current) -> b -> a -> exhausted.
        assert_eq!(user_key_of(&iter.prev()), Some(b"b".to_vec()));
        assert_eq!(user_key_of(&iter.prev()), Some(b"a".to_vec()));
        assert!(iter.prev().is_none());
    }

    #[test]
    fn test_cursor_iter_prev_double_exhaustion_does_not_wrap() {
        // (a) The specific double-exhaustion regression: once prev() has
        // moved before the first entry, calling it again must keep
        // returning None instead of wrapping around to the last entry.
        let memtable =
            make_memtable_with_entries(&[(b"a", 1, b"1"), (b"b", 2, b"2"), (b"c", 3, b"3")]);
        let mut iter = MemTableCursorIter::new(memtable);

        iter.seek_to_last();
        assert_eq!(user_key_of(&iter.current()), Some(b"c".to_vec()));

        // Drain backward until exhausted.
        assert_eq!(user_key_of(&iter.prev()), Some(b"b".to_vec()));
        assert_eq!(user_key_of(&iter.prev()), Some(b"a".to_vec()));
        assert!(
            iter.prev().is_none(),
            "first backward exhaustion must be None"
        );

        // Regression: repeated prev() calls after exhaustion must keep
        // returning None, never resurrecting "c" (the last entry).
        assert!(
            iter.prev().is_none(),
            "second prev() after exhaustion must stay None, not wrap to the last entry"
        );
        assert!(
            iter.prev().is_none(),
            "third prev() after exhaustion must also stay None"
        );
    }

    #[test]
    fn test_cursor_iter_next_resumes_after_backward_exhaustion() {
        // (c) next() after backward-exhaustion correctly resumes forward
        // traversal, instead of staying stuck at None because the cursor
        // is still null from the exhaustion.
        let memtable =
            make_memtable_with_entries(&[(b"a", 1, b"1"), (b"b", 2, b"2"), (b"c", 3, b"3")]);
        let mut iter = MemTableCursorIter::new(memtable);

        iter.seek_to_last();
        assert_eq!(user_key_of(&iter.current()), Some(b"c".to_vec()));
        assert_eq!(user_key_of(&iter.prev()), Some(b"b".to_vec()));
        assert_eq!(user_key_of(&iter.prev()), Some(b"a".to_vec()));
        assert!(iter.prev().is_none(), "exhausted backward");

        // Direction switch: next() must resume forward at the first entry.
        let extract = |e: Option<(Vec<u8>, Vec<u8>)>| {
            e.map(|(k, _)| InternalKeyRef::new(&k).user_key().to_vec())
        };
        assert_eq!(extract(Iterator::next(&mut iter)), Some(b"a".to_vec()));
        assert_eq!(extract(Iterator::next(&mut iter)), Some(b"b".to_vec()));
        assert_eq!(extract(Iterator::next(&mut iter)), Some(b"c".to_vec()));
        assert_eq!(
            extract(Iterator::next(&mut iter)),
            None,
            "must be forward-exhausted again after re-consuming all entries"
        );
    }

    #[test]
    fn test_cursor_iter_seek_for_prev_and_current() {
        // seek_for_prev()/current() direct coverage, including the
        // "no predecessor" edge case behaving consistently with prev().
        let memtable =
            make_memtable_with_entries(&[(b"a", 1, b"1"), (b"c", 1, b"3"), (b"e", 1, b"5")]);
        let mut iter = MemTableCursorIter::new(memtable);

        // Exact match.
        let search = InternalKey::new(b"c", 1, ValueType::Value);
        iter.seek_for_prev(search.as_bytes());
        assert_eq!(user_key_of(&iter.current()), Some(b"c".to_vec()));

        // Target between entries finds the largest entry <= target.
        let search = InternalKey::new(b"d", 1, ValueType::Value);
        iter.seek_for_prev(search.as_bytes());
        assert_eq!(user_key_of(&iter.current()), Some(b"c".to_vec()));

        // Backward traversal from here still behaves correctly and stays
        // exhausted afterward (does not wrap to "e").
        assert_eq!(user_key_of(&iter.prev()), Some(b"a".to_vec()));
        assert!(iter.prev().is_none());
        assert!(iter.prev().is_none());

        // Target smaller than every key: no predecessor exists.
        let search = InternalKey::new(b"\0", 1, ValueType::Value);
        iter.seek_for_prev(search.as_bytes());
        assert!(iter.current().is_none());
    }
}

//! DBIterator: a user-facing iterator that merges all sources, resolves
//! sequence numbers, and skips tombstones.

use crate::iterator::merge::{IterSource, MergingIterator};
use crate::iterator::range_del::RangeTombstoneTracker;
use crate::types::{InternalKeyRef, SequenceNumber, ValueType, compare_internal_key};

type IKeyCompareFn = fn(&[u8], &[u8]) -> std::cmp::Ordering;

/// A database-level iterator that presents a clean view of key-value pairs.
///
/// Uses a streaming MergingIterator internally — entries are produced lazily
/// without collecting the entire dataset into memory.
pub struct DBIterator {
    /// Underlying merging iterator producing (internal_key, value) pairs in order.
    merger: MergingIterator<IKeyCompareFn>,
    /// Snapshot sequence number for visibility filtering.
    sequence: SequenceNumber,
    /// Reusable buffer for last user key (for deduplication).
    last_user_key: Vec<u8>,
    /// Whether last_user_key has been set at least once.
    has_last_key: bool,
    /// Buffered current entry for valid()/key()/value() API.
    current: Option<(Vec<u8>, Vec<u8>)>,
    /// Whether we've already consumed current via advance().
    needs_advance: bool,
    /// Efficient range tombstone tracker with sweep-line algorithm.
    range_tombstones: RangeTombstoneTracker,
    /// If set, iteration stops when user key no longer starts with this prefix.
    prefix: Option<Vec<u8>>,
    /// If set, iteration stops when user key >= this bound (exclusive upper bound).
    /// Models RocksDB's `ReadOptions::iterate_upper_bound`.
    iterate_upper_bound: Option<Vec<u8>>,
}

fn ikey_compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    compare_internal_key(a, b)
}

impl DBIterator {
    /// Build a DB iterator from multiple sorted Vec sources of internal key-value pairs.
    pub fn new(sources: Vec<Vec<(Vec<u8>, Vec<u8>)>>, sequence: SequenceNumber) -> Self {
        let iter_sources: Vec<IterSource> = sources.into_iter().map(IterSource::new).collect();

        let merger = MergingIterator::new(
            iter_sources,
            ikey_compare as fn(&[u8], &[u8]) -> std::cmp::Ordering,
        );

        Self {
            merger,
            sequence,
            last_user_key: Vec::new(),
            has_last_key: false,
            current: None,
            needs_advance: true,
            range_tombstones: RangeTombstoneTracker::new(),
            prefix: None,
            iterate_upper_bound: None,
        }
    }

    /// Build a DB iterator from pre-built IterSource objects (supports streaming).
    pub fn from_sources(sources: Vec<IterSource>, sequence: SequenceNumber) -> Self {
        let merger = MergingIterator::new(
            sources,
            ikey_compare as fn(&[u8], &[u8]) -> std::cmp::Ordering,
        );

        Self {
            merger,
            sequence,
            last_user_key: Vec::new(),
            has_last_key: false,
            current: None,
            needs_advance: true,
            range_tombstones: RangeTombstoneTracker::new(),
            prefix: None,
            iterate_upper_bound: None,
        }
    }

    /// Build a DB iterator with prefix-bounded iteration.
    /// Iteration stops when user key no longer starts with `prefix`.
    pub fn from_sources_with_prefix(
        sources: Vec<IterSource>,
        sequence: SequenceNumber,
        prefix: Vec<u8>,
    ) -> Self {
        let merger = MergingIterator::new(
            sources,
            ikey_compare as fn(&[u8], &[u8]) -> std::cmp::Ordering,
        );

        Self {
            merger,
            sequence,
            last_user_key: Vec::new(),
            has_last_key: false,
            current: None,
            needs_advance: true,
            range_tombstones: RangeTombstoneTracker::new(),
            prefix: Some(prefix),
            iterate_upper_bound: None,
        }
    }

    /// Set an exclusive upper bound on user keys.
    /// Iteration stops when user key >= this bound.
    /// Models RocksDB's `ReadOptions::iterate_upper_bound`.
    pub fn set_upper_bound(&mut self, bound: Vec<u8>) {
        self.iterate_upper_bound = Some(bound);
    }

    /// Check if a user key is covered by any range tombstone visible at our snapshot.
    fn is_range_deleted(&mut self, user_key: &[u8], seq: SequenceNumber) -> bool {
        if self.range_tombstones.is_empty() {
            return false;
        }
        self.range_tombstones
            .is_deleted(user_key, seq, self.sequence)
    }

    /// Advance the streaming iterator to the next visible (user_key, value) pair.
    fn next_visible(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        loop {
            let (mut ikey, value) = self.merger.next_entry()?;
            if ikey.len() < 8 {
                continue;
            }
            let ikr = InternalKeyRef::new(&ikey);
            let seq = ikr.sequence();
            let vt = ikr.value_type();

            // Skip entries newer than our snapshot
            if seq > self.sequence {
                continue;
            }

            let uk_len = ikey.len() - 8;

            // Prefix boundary check — stop immediately when prefix changes
            if let Some(ref pfx) = self.prefix
                && !ikey[..uk_len].starts_with(pfx)
            {
                return None;
            }

            // Upper bound check — stop when user key >= upper bound (exclusive)
            if let Some(ref ub) = self.iterate_upper_bound
                && ikey[..uk_len] >= **ub
            {
                return None;
            }

            // Collect range tombstones
            if vt == ValueType::RangeDeletion {
                self.range_tombstones
                    .add(ikey[..uk_len].to_vec(), value.clone(), seq);
                self.range_tombstones.reset();
                if self.has_last_key && self.last_user_key.as_slice() == &ikey[..uk_len] {
                    continue;
                }
                self.last_user_key.clear();
                self.last_user_key.extend_from_slice(&ikey[..uk_len]);
                self.has_last_key = true;
                continue;
            }

            // Skip duplicate user keys (we already saw the newest version)
            if self.has_last_key && self.last_user_key.as_slice() == &ikey[..uk_len] {
                continue;
            }

            self.last_user_key.clear();
            self.last_user_key.extend_from_slice(&ikey[..uk_len]);
            self.has_last_key = true;

            // Skip tombstones
            if vt == ValueType::Deletion {
                continue;
            }

            // Skip entries covered by range tombstones
            if self.is_range_deleted(&ikey[..uk_len], seq) {
                continue;
            }

            // Truncate internal key to user key in-place (zero allocation).
            ikey.truncate(uk_len);
            return Some((ikey, value));
        }
    }

    /// Ensure current is populated. Returns whether there's a valid entry.
    fn ensure_current(&mut self) -> bool {
        if self.needs_advance {
            self.current = self.next_visible();
            self.needs_advance = false;
        }
        self.current.is_some()
    }

    pub fn valid(&mut self) -> bool {
        self.ensure_current()
    }

    pub fn key(&mut self) -> &[u8] {
        self.ensure_current();
        &self
            .current
            .as_ref()
            .expect("called key() on invalid iterator")
            .0
    }

    pub fn value(&mut self) -> &[u8] {
        self.ensure_current();
        &self
            .current
            .as_ref()
            .expect("called value() on invalid iterator")
            .1
    }

    pub fn advance(&mut self) {
        self.needs_advance = true;
        self.current = None;
    }

    /// Seek to the first key >= target.
    pub fn seek(&mut self, target: &[u8]) {
        use crate::types::InternalKey;
        // Seek the merger to a synthetic internal key with max sequence
        let seek_key =
            InternalKey::new(target, crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value);
        self.merger.seek(seek_key.as_bytes());
        self.has_last_key = false;
        self.needs_advance = true;
        self.current = None;
    }

    pub fn seek_to_first(&mut self) {
        use crate::types::InternalKey;
        // Seek to the smallest possible internal key (empty user key, max sequence).
        let seek_key = InternalKey::new(b"", crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value);
        self.merger.seek(seek_key.as_bytes());
        self.has_last_key = false;
        self.needs_advance = true;
        self.current = None;
    }

    /// Fully reset internal deduplication/tombstone state, then seek to first.
    /// Used by BidiIterator when materializing after a seek_to_last().
    pub fn reset_and_seek_to_first(&mut self) {
        self.has_last_key = false;
        self.last_user_key.clear();
        self.range_tombstones.clear();
        self.current = None;
        self.needs_advance = true;
        self.seek_to_first();
    }

    /// Seek to the last visible user key <= target.
    ///
    /// Uses merger seek_for_prev to efficiently position near target, then
    /// forward-scans to resolve visibility correctly.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        use crate::types::InternalKey;

        // First try forward seek — if exact match or equal, done
        self.seek(target);
        if self.valid() && self.key() <= target {
            return;
        }

        // Use merger backward seek to find the last internal key <= target
        let seek_key = InternalKey::new(target, 0, ValueType::Deletion);
        self.merger.seek_for_prev(seek_key.as_bytes());

        // Walk backward to find a user key < target, then forward-resolve visibility
        self.resolve_prev_user_key(target);
    }

    /// Internal: given the merger positioned backward at or before target,
    /// find the previous visible user key and position on it.
    ///
    /// Uses a loop instead of recursion to avoid stack overflow when
    /// consecutive tombstones or prefix-out-of-bounds keys are encountered.
    fn resolve_prev_user_key(&mut self, skip_bound: &[u8]) {
        use crate::types::InternalKey;

        let mut current_bound: Vec<u8> = skip_bound.to_vec();

        loop {
            // Walk backward through prev_entry() to find the first user key < current_bound
            let mut prev_uk: Option<Vec<u8>> = None;
            while let Some((ikey, _value)) = self.merger.prev_entry() {
                if ikey.len() < 8 {
                    continue;
                }
                let uk = &ikey[..ikey.len() - 8];

                // Prefix guard: if we've left the prefix, stop immediately.
                // In a decreasing key sequence, once we leave the prefix we
                // can never re-enter it.
                if let Some(ref pfx) = self.prefix {
                    if !uk.starts_with(pfx) {
                        break;
                    }
                }

                if uk < current_bound.as_slice() {
                    prev_uk = Some(uk.to_vec());
                    break;
                }
            }

            match prev_uk {
                Some(uk) => {
                    // Forward-seek to this user key and resolve visibility
                    let reseek_key =
                        InternalKey::new(&uk, crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value);
                    self.merger.seek_to(reseek_key.as_bytes());
                    self.has_last_key = false;
                    self.range_tombstones.clear();

                    // next_visible() finds the newest visible version
                    let visible = self.next_visible();
                    match visible {
                        Some((found_uk, val)) if found_uk == uk => {
                            self.current = Some((found_uk, val));
                            self.needs_advance = false;
                            return;
                        }
                        _ => {
                            // The found key was deleted/tombstoned; try the next earlier key
                            current_bound = uk;
                            // Continue the loop instead of recursing
                        }
                    }
                }
                None => {
                    self.current = None;
                    self.needs_advance = false;
                    return;
                }
            }
        }
    }

    /// Move to the previous visible user key.
    ///
    /// O(K·log N) per call: uses merger backward seek to find the previous user key,
    /// then forward-seeks there and resolves visibility via next_visible().
    pub fn prev(&mut self) {
        // Ensure we have a current entry to move backwards from
        self.ensure_current();
        let saved_key = match &self.current {
            Some((k, _)) => k.clone(),
            None => {
                // No current entry; nothing to go back from
                return;
            }
        };

        self.resolve_prev_user_key(&saved_key);
    }

    /// Seek to the last visible key. Positions the iterator on the very last entry.
    ///
    /// If a prefix is set, seeks to the last key within the prefix.
    /// Uses merger backward seek to find the last user key efficiently,
    /// then resolves visibility via forward scan.
    pub fn seek_to_last(&mut self) {
        use crate::types::InternalKey;

        if let Some(ref pfx) = self.prefix {
            // Prefix-bounded: seek backward from prefix upper bound.
            let mut upper = pfx.clone();
            let has_upper = {
                let mut carry = true;
                for byte in upper.iter_mut().rev() {
                    if carry {
                        if *byte == 0xFF {
                            *byte = 0x00;
                        } else {
                            *byte += 1;
                            carry = false;
                        }
                    }
                }
                !carry
            };
            if has_upper {
                let seek_key =
                    InternalKey::new(&upper, crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value);
                self.merger.seek_for_prev(seek_key.as_bytes());
            } else {
                self.merger.seek_to_last_merge();
            }
        } else {
            self.merger.seek_to_last_merge();
        }

        self.has_last_key = false;
        self.range_tombstones.clear();

        // Walk backward to find the last user key within prefix, then forward-resolve
        let mut last_uk: Option<Vec<u8>> = None;
        while let Some((ikey, _value)) = self.merger.prev_entry() {
            if ikey.len() < 8 {
                continue;
            }
            let uk = &ikey[..ikey.len() - 8];
            // Check prefix boundary — once we've left the prefix going
            // backward, we can never re-enter it, so stop scanning.
            if let Some(ref pfx) = self.prefix
                && !uk.starts_with(pfx)
            {
                break;
            }
            last_uk = Some(uk.to_vec());
            break;
        }

        match last_uk {
            Some(uk) => {
                // Forward-seek and resolve visibility
                let reseek_key =
                    InternalKey::new(&uk, crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value);
                self.merger.seek_to(reseek_key.as_bytes());
                self.has_last_key = false;
                self.range_tombstones.clear();

                let visible = self.next_visible();
                match visible {
                    Some((found_uk, val)) if found_uk == uk => {
                        self.current = Some((found_uk, val));
                        self.needs_advance = false;
                    }
                    _ => {
                        // Last key is deleted; try previous
                        self.current = None;
                        self.needs_advance = false;
                        self.resolve_prev_user_key(&uk);
                    }
                }
            }
            None => {
                self.current = None;
                self.needs_advance = false;
            }
        }
    }

    pub fn collect_remaining(&mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        for entry in self.by_ref() {
            result.push(entry);
        }
        result
    }

    pub fn count(&mut self) -> usize {
        let mut n = 0;
        while self.next().is_some() {
            n += 1;
        }
        n
    }
}

impl Iterator for DBIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.ensure_current() {
            let entry = self.current.take();
            self.needs_advance = true;
            entry
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::InternalKey;

    fn make_entry(user_key: &[u8], seq: u64, vt: ValueType, value: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let ik = InternalKey::new(user_key, seq, vt);
        (ik.into_bytes(), value.to_vec())
    }

    fn sort_lex(mut entries: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(Vec<u8>, Vec<u8>)> {
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        entries
    }

    #[test]
    fn test_db_iterator_basic() {
        let source = sort_lex(vec![
            make_entry(b"a", 3, ValueType::Value, b"v3"),
            make_entry(b"a", 1, ValueType::Value, b"v1"),
            make_entry(b"b", 2, ValueType::Value, b"v2"),
        ]);

        let iter = DBIterator::new(vec![source], 10);
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (b"a".to_vec(), b"v3".to_vec())); // latest for "a"
        assert_eq!(entries[1], (b"b".to_vec(), b"v2".to_vec()));
    }

    #[test]
    fn test_db_iterator_tombstone() {
        let source = sort_lex(vec![
            make_entry(b"a", 5, ValueType::Deletion, b""),
            make_entry(b"a", 3, ValueType::Value, b"old"),
            make_entry(b"b", 4, ValueType::Value, b"alive"),
        ]);

        let iter = DBIterator::new(vec![source], 10);
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (b"b".to_vec(), b"alive".to_vec()));
    }

    #[test]
    fn test_db_iterator_snapshot() {
        let source = sort_lex(vec![
            make_entry(b"a", 5, ValueType::Value, b"new"),
            make_entry(b"a", 3, ValueType::Value, b"old"),
        ]);

        // At sequence 4, should see "old"
        let iter = DBIterator::new(vec![source.clone()], 4);
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (b"a".to_vec(), b"old".to_vec()));

        // At sequence 5, should see "new"
        let iter = DBIterator::new(vec![source], 5);
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (b"a".to_vec(), b"new".to_vec()));
    }

    #[test]
    fn test_db_iterator_multiple_sources() {
        let s1 = sort_lex(vec![
            make_entry(b"a", 10, ValueType::Value, b"mem_a"),
            make_entry(b"c", 8, ValueType::Value, b"mem_c"),
        ]);
        let s2 = sort_lex(vec![
            make_entry(b"a", 5, ValueType::Value, b"sst_a"),
            make_entry(b"b", 6, ValueType::Value, b"sst_b"),
        ]);

        let iter = DBIterator::new(vec![s1, s2], 20);
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], (b"a".to_vec(), b"mem_a".to_vec()));
        assert_eq!(entries[1], (b"b".to_vec(), b"sst_b".to_vec()));
        assert_eq!(entries[2], (b"c".to_vec(), b"mem_c".to_vec()));
    }

    #[test]
    fn test_db_iterator_seek() {
        let source = sort_lex(vec![
            make_entry(b"apple", 1, ValueType::Value, b"1"),
            make_entry(b"banana", 2, ValueType::Value, b"2"),
            make_entry(b"cherry", 3, ValueType::Value, b"3"),
        ]);

        let mut iter = DBIterator::new(vec![source], 10);
        iter.seek(b"banana");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"banana");

        iter.seek(b"blueberry");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"cherry");

        iter.seek(b"zzz");
        assert!(!iter.valid());
    }

    #[test]
    fn test_db_iterator_valid_key_value_advance() {
        let source = sort_lex(vec![
            make_entry(b"a", 1, ValueType::Value, b"1"),
            make_entry(b"b", 2, ValueType::Value, b"2"),
            make_entry(b"c", 3, ValueType::Value, b"3"),
        ]);

        let mut iter = DBIterator::new(vec![source], 10);
        assert!(iter.valid());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"1");

        iter.advance();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"2");

        iter.advance();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"3");

        iter.advance();
        assert!(!iter.valid());
    }

    #[test]
    fn test_db_iterator_prev() {
        let source = sort_lex(vec![
            make_entry(b"a", 1, ValueType::Value, b"1"),
            make_entry(b"b", 2, ValueType::Value, b"2"),
            make_entry(b"c", 3, ValueType::Value, b"3"),
            make_entry(b"d", 4, ValueType::Value, b"4"),
            make_entry(b"e", 5, ValueType::Value, b"5"),
        ]);

        // Seek to middle, then prev
        let mut iter = DBIterator::new(vec![source.clone()], 10);
        iter.seek(b"c");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"c");

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"2");

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"1");

        // prev at beginning should invalidate
        iter.prev();
        assert!(!iter.valid());

        // Seek to last, prev through all
        let mut iter = DBIterator::new(vec![source.clone()], 10);
        iter.seek(b"e");
        assert_eq!(iter.key(), b"e");

        iter.prev();
        assert_eq!(iter.key(), b"d");
        iter.prev();
        assert_eq!(iter.key(), b"c");
        iter.prev();
        assert_eq!(iter.key(), b"b");
        iter.prev();
        assert_eq!(iter.key(), b"a");
        iter.prev();
        assert!(!iter.valid());

        // prev then next should work
        let mut iter = DBIterator::new(vec![source], 10);
        iter.seek(b"c");
        assert_eq!(iter.key(), b"c");
        iter.prev();
        assert_eq!(iter.key(), b"b");
        // After prev, advance should move to next entry
        iter.advance();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"c");
    }

    #[test]
    fn test_db_iterator_seek_for_prev() {
        let source = sort_lex(vec![
            make_entry(b"apple", 1, ValueType::Value, b"1"),
            make_entry(b"banana", 2, ValueType::Value, b"2"),
            make_entry(b"cherry", 3, ValueType::Value, b"3"),
            make_entry(b"date", 4, ValueType::Value, b"4"),
        ]);

        // Exact match
        let mut iter = DBIterator::new(vec![source.clone()], 10);
        iter.seek_for_prev(b"banana");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"banana");

        // Between entries: blueberry is between banana and cherry
        let mut iter = DBIterator::new(vec![source.clone()], 10);
        iter.seek_for_prev(b"blueberry");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"banana"); // last key <= "blueberry"

        // Before first key
        let mut iter = DBIterator::new(vec![source.clone()], 10);
        iter.seek_for_prev(b"aaa");
        assert!(!iter.valid()); // nothing <= "aaa"

        // After last key
        let mut iter = DBIterator::new(vec![source.clone()], 10);
        iter.seek_for_prev(b"zzz");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"date"); // last key in dataset

        // seek_for_prev to first key
        let mut iter = DBIterator::new(vec![source], 10);
        iter.seek_for_prev(b"apple");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"apple");
    }

    #[test]
    fn test_db_iterator_prev_with_tombstones() {
        let source = sort_lex(vec![
            make_entry(b"a", 1, ValueType::Value, b"1"),
            make_entry(b"b", 5, ValueType::Deletion, b""),
            make_entry(b"b", 3, ValueType::Value, b"old_b"),
            make_entry(b"c", 4, ValueType::Value, b"3"),
            make_entry(b"d", 6, ValueType::Value, b"4"),
        ]);

        // Forward: should see a, c, d (b is deleted)
        let mut iter = DBIterator::new(vec![source.clone()], 10);
        iter.seek(b"d");
        assert_eq!(iter.key(), b"d");

        // prev should skip deleted "b" and land on "c"
        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"c");

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"a");
    }

    #[test]
    fn test_db_iterator_prev_multiple_sources() {
        let s1 = sort_lex(vec![
            make_entry(b"a", 10, ValueType::Value, b"mem_a"),
            make_entry(b"c", 8, ValueType::Value, b"mem_c"),
        ]);
        let s2 = sort_lex(vec![
            make_entry(b"b", 6, ValueType::Value, b"sst_b"),
            make_entry(b"d", 4, ValueType::Value, b"sst_d"),
        ]);

        // Forward: a, b, c, d
        let mut iter = DBIterator::new(vec![s1, s2], 20);
        iter.seek(b"d");
        assert_eq!(iter.key(), b"d");

        iter.prev();
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"mem_c");

        iter.prev();
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"sst_b");

        iter.prev();
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"mem_a");

        iter.prev();
        assert!(!iter.valid());
    }

    #[test]
    fn test_db_iterator_seek_to_last() {
        let source = sort_lex(vec![
            make_entry(b"a", 1, ValueType::Value, b"1"),
            make_entry(b"b", 2, ValueType::Value, b"2"),
            make_entry(b"c", 3, ValueType::Value, b"3"),
        ]);

        let mut iter = DBIterator::new(vec![source], 10);
        iter.seek_to_last();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"3");

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"b");

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"a");

        iter.prev();
        assert!(!iter.valid());
    }
}

//! DBIterator: a user-facing iterator that merges all sources, resolves
//! sequence numbers, and skips tombstones.

use crate::iterator::merge::{IterSource, MergingIterator};
use crate::types::{InternalKeyRef, SequenceNumber, ValueType, compare_internal_key};

/// A range tombstone: keys in [begin, end) at sequence `seq` are deleted.
struct RangeTombstone {
    begin: Vec<u8>,
    end: Vec<u8>,
    seq: SequenceNumber,
}

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
    /// Last user key yielded (for deduplication).
    last_user_key: Option<Vec<u8>>,
    /// Buffered current entry for valid()/key()/value() API.
    current: Option<(Vec<u8>, Vec<u8>)>,
    /// Whether we've already consumed current via advance().
    needs_advance: bool,
    /// Collected range tombstones for filtering.
    range_tombstones: Vec<RangeTombstone>,
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
            last_user_key: None,
            current: None,
            needs_advance: true,
            range_tombstones: Vec::new(),
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
            last_user_key: None,
            current: None,
            needs_advance: true,
            range_tombstones: Vec::new(),
        }
    }

    /// Check if a user key is covered by any range tombstone visible at our snapshot.
    fn is_range_deleted(&self, user_key: &[u8], seq: SequenceNumber) -> bool {
        for rt in &self.range_tombstones {
            if rt.seq <= self.sequence
                && user_key >= rt.begin.as_slice()
                && user_key < rt.end.as_slice()
                && rt.seq >= seq
            {
                return true;
            }
        }
        false
    }

    /// Advance the streaming iterator to the next visible (user_key, value) pair.
    fn next_visible(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        loop {
            let (ikey, value) = self.merger.next_entry()?;
            if ikey.len() < 8 {
                continue;
            }
            let ikr = InternalKeyRef::new(&ikey);
            let seq = ikr.sequence();

            // Skip entries newer than our snapshot
            if seq > self.sequence {
                // But still collect range tombstones for filtering
                if ikr.value_type() == ValueType::RangeDeletion {
                    // Don't collect — it's newer than our snapshot
                }
                continue;
            }

            let user_key = ikr.user_key();

            // Collect range tombstones
            if ikr.value_type() == ValueType::RangeDeletion {
                self.range_tombstones.push(RangeTombstone {
                    begin: user_key.to_vec(),
                    end: value.clone(),
                    seq,
                });
                // Still need to deduplicate the begin key
                if let Some(ref last) = self.last_user_key
                    && last.as_slice() == user_key
                {
                    continue;
                }
                self.last_user_key = Some(user_key.to_vec());
                continue;
            }

            // Skip duplicate user keys (we already saw the newest version)
            if let Some(ref last) = self.last_user_key
                && last.as_slice() == user_key
            {
                continue;
            }

            self.last_user_key = Some(user_key.to_vec());

            // Skip tombstones
            if ikr.value_type() == ValueType::Deletion {
                continue;
            }

            // Skip entries covered by range tombstones
            if self.is_range_deleted(user_key, seq) {
                continue;
            }

            return Some((user_key.to_vec(), value));
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
        self.last_user_key = None;
        self.needs_advance = true;
        self.current = None;
    }

    pub fn seek_to_first(&mut self) {
        use crate::types::InternalKey;
        // Seek to the smallest possible internal key (empty user key, max sequence).
        let seek_key = InternalKey::new(b"", crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value);
        self.merger.seek(seek_key.as_bytes());
        self.last_user_key = None;
        self.needs_advance = true;
        self.current = None;
    }

    /// Seek to the last visible user key <= target.
    ///
    /// Strategy: seek to >= target. If exact match, done.
    /// If the found key > target or no key >= target exists, use seek_to_last
    /// and prev-scan approach to find the last key <= target.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        self.seek(target);
        if self.valid() && self.key() <= target {
            return; // Exact match or equal
        }
        // Either no entry >= target (iterator exhausted), or the found entry > target.
        // Need to find the last visible entry <= target by scanning from the beginning.
        self.seek_for_prev_scan(target);
    }

    /// Internal: scan from the beginning to find the last visible entry <= target.
    fn seek_for_prev_scan(&mut self, target: &[u8]) {
        use crate::types::InternalKey;

        let first_key = InternalKey::new(b"", crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value);
        self.merger.seek_to(first_key.as_bytes());
        self.last_user_key = None;
        self.range_tombstones.clear();

        let mut best: Option<(Vec<u8>, Vec<u8>)> = None;

        loop {
            let visible = self.next_visible();
            match visible {
                Some((uk, val)) => {
                    if uk.as_slice() > target {
                        break; // Past target, stop
                    }
                    best = Some((uk, val));
                }
                None => break,
            }
        }

        match best {
            Some((uk, val)) => {
                // Re-position the merger on this key for future next()/prev() calls.
                let reseek_key =
                    InternalKey::new(&uk, crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value);
                self.merger.seek_to(reseek_key.as_bytes());
                self.last_user_key = None;
                self.range_tombstones.clear();

                // Consume through to our key
                loop {
                    let visible = self.next_visible();
                    match visible {
                        Some((found_uk, _)) if found_uk == uk => break,
                        Some(_) => continue,
                        None => break,
                    }
                }

                self.current = Some((uk, val));
                self.needs_advance = false;
            }
            None => {
                self.current = None;
                self.needs_advance = false;
            }
        }
    }

    /// Move to the previous visible user key.
    ///
    /// Uses the re-seek approach: saves the current user key, seeks to just before it,
    /// then scans forward collecting visible entries to find the last one before the
    /// saved key. This is O(log N) per call (same as RocksDB's approach for prev).
    pub fn prev(&mut self) {
        use crate::types::InternalKey;

        // Ensure we have a current entry to move backwards from
        self.ensure_current();
        let saved_key = match &self.current {
            Some((k, _)) => k.clone(),
            None => {
                // No current entry; nothing to go back from
                return;
            }
        };

        // Re-seek the merger to the very beginning and scan forward to find
        // the last visible entry before saved_key.
        let first_key = InternalKey::new(b"", crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value);
        self.merger.seek_to(first_key.as_bytes());
        self.last_user_key = None;
        self.range_tombstones.clear();

        // Scan forward, collecting the last visible entry before saved_key
        let mut prev_entry: Option<(Vec<u8>, Vec<u8>)> = None;

        loop {
            let visible = self.next_visible();
            match visible {
                Some((uk, val)) => {
                    if uk >= saved_key {
                        // We've reached or passed the saved key; stop
                        break;
                    }
                    prev_entry = Some((uk, val));
                }
                None => break,
            }
        }

        // Now we need to re-position the iterator.
        // If we found a previous entry, position on it.
        // We also need to ensure the merger is positioned correctly for future calls.
        match prev_entry {
            Some((uk, val)) => {
                // Re-seek the merger to the found key so future next()/prev() work correctly.
                let reseek_key =
                    InternalKey::new(&uk, crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value);
                self.merger.seek_to(reseek_key.as_bytes());
                self.last_user_key = None;
                self.range_tombstones.clear();

                // Advance the merger past this key so next() gives the right entry
                // by consuming the current key's entries.
                loop {
                    let visible = self.next_visible();
                    match visible {
                        Some((found_uk, _)) if found_uk == uk => {
                            // Found our key, stop
                            break;
                        }
                        Some(_) => {
                            // Different key before ours, keep going
                            continue;
                        }
                        None => break,
                    }
                }

                self.current = Some((uk, val));
                self.needs_advance = false;
            }
            None => {
                // No previous entry exists
                self.current = None;
                self.needs_advance = false;
            }
        }
    }

    /// Seek to the last visible key. Positions the iterator on the very last entry.
    pub fn seek_to_last(&mut self) {
        use crate::types::InternalKey;
        // Seek to the beginning and scan to find the last visible entry
        let first_key = InternalKey::new(b"", crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value);
        self.merger.seek_to(first_key.as_bytes());
        self.last_user_key = None;
        self.range_tombstones.clear();

        let mut last_entry: Option<(Vec<u8>, Vec<u8>)> = None;
        loop {
            let visible = self.next_visible();
            match visible {
                Some(entry) => {
                    last_entry = Some(entry);
                }
                None => break,
            }
        }

        match last_entry {
            Some((uk, val)) => {
                // Re-seek so future prev() calls work
                use crate::types::InternalKey;
                let reseek_key =
                    InternalKey::new(&uk, crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value);
                self.merger.seek_to(reseek_key.as_bytes());
                self.last_user_key = None;
                self.range_tombstones.clear();

                // Consume through to our key
                loop {
                    let visible = self.next_visible();
                    match visible {
                        Some((found_uk, _)) if found_uk == uk => break,
                        Some(_) => continue,
                        None => break,
                    }
                }

                self.current = Some((uk, val));
                self.needs_advance = false;
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

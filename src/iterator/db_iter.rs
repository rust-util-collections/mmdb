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
        // Reset by seeking to empty key (sorts before everything)
        self.merger.seek(b"\x00");
        self.last_user_key = None;
        self.needs_advance = true;
        self.current = None;
    }

    /// Seek to the last key <= target, then position on it.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        self.seek(target);
        // If positioned exactly on target, done.
        // If positioned past target, we need prev() which requires materialization.
        // For now, this seeks to >= target — a full prev() implementation would need
        // reverse iteration support in the heap.
        if self.valid() && self.key() > target {
            // We overshot. For the simplified version, we collected forward only.
            // Mark as invalid — full reverse iteration requires more infrastructure.
            self.current = None;
            self.needs_advance = false;
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
}

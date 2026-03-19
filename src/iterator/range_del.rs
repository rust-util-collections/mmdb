//! Efficient range tombstone tracking for forward iteration.
//!
//! Uses a sweep-line approach: tombstones are sorted by begin key, and an "active set"
//! tracks which tombstones currently cover the iteration position. For forward iteration
//! with monotonically increasing keys, the amortized cost per key check is O(1).

use crate::types::SequenceNumber;

/// A range tombstone: keys in [begin, end) at sequence `seq` are deleted.
pub(crate) struct RangeTombstone {
    pub begin: Vec<u8>,
    pub end: Vec<u8>,
    pub seq: SequenceNumber,
}

/// Tracks active range tombstones for efficient forward-scan filtering.
pub(crate) struct RangeTombstoneTracker {
    /// All tombstones collected during iteration.
    tombstones: Vec<RangeTombstone>,
    /// Whether tombstones have been sorted by begin key.
    sorted: bool,
    /// Index of the next tombstone to consider activating.
    next_idx: usize,
    /// Indices of currently active tombstones (begin <= current key).
    active: Vec<usize>,
}

impl RangeTombstoneTracker {
    pub fn new() -> Self {
        Self {
            tombstones: Vec::new(),
            sorted: false,
            next_idx: 0,
            active: Vec::new(),
        }
    }

    /// Add a range tombstone. Must call `reset()` after adding to re-sort.
    pub fn add(&mut self, begin: Vec<u8>, end: Vec<u8>, seq: SequenceNumber) {
        self.tombstones.push(RangeTombstone { begin, end, seq });
        self.sorted = false;
    }

    /// Reset the sweep state (e.g., after seek or after adding new tombstones).
    pub fn reset(&mut self) {
        if !self.sorted && self.tombstones.len() > 1 {
            self.tombstones.sort_by(|a, b| a.begin.cmp(&b.begin));
            self.sorted = true;
        }
        self.next_idx = 0;
        self.active.clear();
    }

    /// Check if `user_key` at `seq` is deleted by any active range tombstone
    /// visible at `snapshot`.
    ///
    /// For optimal performance, call with monotonically increasing `user_key`.
    pub fn is_deleted(
        &mut self,
        user_key: &[u8],
        seq: SequenceNumber,
        snapshot: SequenceNumber,
    ) -> bool {
        // For small counts, linear scan is faster than the sweep overhead
        if self.tombstones.len() <= 4 {
            return self.linear_check(user_key, seq, snapshot);
        }

        // Ensure sorted
        if !self.sorted {
            self.reset();
        }

        // Activate new tombstones whose begin <= user_key
        while self.next_idx < self.tombstones.len() {
            if self.tombstones[self.next_idx].begin.as_slice() <= user_key {
                self.active.push(self.next_idx);
                self.next_idx += 1;
            } else {
                break;
            }
        }

        // Prune expired tombstones (end <= user_key) and check remaining
        let tombstones = &self.tombstones;
        self.active
            .retain(|&idx| tombstones[idx].end.as_slice() > user_key);

        for &idx in &self.active {
            let rt = &self.tombstones[idx];
            if rt.seq <= snapshot && rt.seq > seq {
                return true;
            }
        }

        false
    }

    /// Simple linear scan for small tombstone counts.
    fn linear_check(&self, user_key: &[u8], seq: SequenceNumber, snapshot: SequenceNumber) -> bool {
        for rt in &self.tombstones {
            if rt.seq <= snapshot
                && user_key >= rt.begin.as_slice()
                && user_key < rt.end.as_slice()
                && rt.seq > seq
            {
                return true;
            }
        }
        false
    }

    /// Whether any tombstones have been collected.
    pub fn is_empty(&self) -> bool {
        self.tombstones.is_empty()
    }
}

// ---------------------------------------------------------------------------
// FragmentedRangeTombstoneList — immutable, O(log T) binary-search index
// ---------------------------------------------------------------------------

/// A non-overlapping tombstone fragment: keys in [begin, end) are covered
/// by tombstones at the listed sequence numbers.
struct TombstoneFragment {
    begin: Vec<u8>,
    end: Vec<u8>,
    /// `(sequence_number, source_level)` pairs covering this interval,
    /// stored in **descending seq** order for efficient max-visible-seq lookup.
    /// The level enables cross-level pruning: a tombstone from level L can only
    /// delete keys from levels > L.
    seq_levels: Vec<(SequenceNumber, usize)>,
}

/// Pre-fragmented, immutable range tombstone index.
///
/// Overlapping raw tombstones are split at overlap points into non-overlapping
/// fragments sorted by `begin`. This enables O(log T) binary search for any
/// user key in any direction (forward or backward), replacing both the
/// sweep-line tracker and the linear-scan fallback.
///
/// Constructed once at iterator creation time from all sources' cached
/// tombstones; never mutated afterwards.
pub(crate) struct FragmentedRangeTombstoneList {
    /// Non-overlapping fragments sorted by `begin`. Guaranteed:
    /// - `fragments[i].end <= fragments[i+1].begin` (no overlap)
    /// - Each fragment has at least one seq in `seqs`
    fragments: Vec<TombstoneFragment>,
}

impl FragmentedRangeTombstoneList {
    /// Create an empty list (no tombstones).
    pub fn empty() -> Self {
        Self {
            fragments: Vec::new(),
        }
    }

    /// Build from raw tombstones: `(begin, end, seq)` triples.
    /// All tombstones are assigned level 0 (no cross-level pruning).
    pub fn new(raw: Vec<(Vec<u8>, Vec<u8>, SequenceNumber)>) -> Self {
        let with_levels: Vec<_> = raw.into_iter().map(|(b, e, s)| (b, e, s, 0usize)).collect();
        Self::new_with_levels(with_levels)
    }

    /// Build from raw tombstones with level info: `(begin, end, seq, level)`.
    ///
    /// Algorithm (RocksDB-style boundary sweep):
    /// 1. Collect all unique boundary points (begin and end keys), sort & dedup.
    /// 2. For each consecutive boundary pair `[b_i, b_{i+1})`, find all
    ///    tombstones that cover this interval and record their `(seq, level)`.
    /// 3. Skip intervals with no covering tombstones.
    pub fn new_with_levels(mut raw: Vec<(Vec<u8>, Vec<u8>, SequenceNumber, usize)>) -> Self {
        if raw.is_empty() {
            return Self::empty();
        }

        // Collect and sort unique boundary points.
        let mut boundaries: Vec<Vec<u8>> = Vec::with_capacity(raw.len() * 2);
        for (begin, end, _, _) in &raw {
            boundaries.push(begin.clone());
            boundaries.push(end.clone());
        }
        boundaries.sort();
        boundaries.dedup();

        // Sort tombstones by (begin ASC) for sweep efficiency.
        raw.sort_by(|a, b| a.0.cmp(&b.0));

        // Sweep boundaries left-to-right, tracking active tombstones.
        let mut fragments = Vec::new();
        let mut tomb_idx = 0;
        let mut active: Vec<usize> = Vec::new();

        for w in boundaries.windows(2) {
            let b_start = &w[0];
            let b_end = &w[1];

            // Activate tombstones whose begin <= b_start.
            while tomb_idx < raw.len() && raw[tomb_idx].0.as_slice() <= b_start.as_slice() {
                active.push(tomb_idx);
                tomb_idx += 1;
            }

            // Prune expired tombstones (end <= b_start).
            active.retain(|&idx| raw[idx].1.as_slice() > b_start.as_slice());

            if active.is_empty() {
                continue;
            }

            // Collect (seq, level) pairs from active tombstones, sorted by seq descending.
            let mut seq_levels: Vec<(SequenceNumber, usize)> =
                active.iter().map(|&idx| (raw[idx].2, raw[idx].3)).collect();
            seq_levels.sort_unstable_by(|a, b| b.0.cmp(&a.0));
            seq_levels.dedup_by_key(|sl| sl.0);

            fragments.push(TombstoneFragment {
                begin: b_start.clone(),
                end: b_end.clone(),
                seq_levels,
            });
        }

        Self { fragments }
    }

    /// Find the highest-seq tombstone covering `user_key` visible at `snapshot`.
    /// Returns 0 if no covering tombstone exists.
    /// O(log T) via binary search on non-overlapping fragments.
    pub fn max_covering_tombstone_seq(
        &self,
        user_key: &[u8],
        snapshot: SequenceNumber,
    ) -> SequenceNumber {
        self.max_covering_tombstone_seq_for_level(user_key, snapshot, None)
    }

    /// Level-aware variant: only considers tombstones from levels strictly less
    /// than `source_level`. A tombstone from level L can only delete keys from
    /// levels > L (deeper levels hold older data).
    /// Pass `None` for no level filtering (backward compatible).
    pub fn max_covering_tombstone_seq_for_level(
        &self,
        user_key: &[u8],
        snapshot: SequenceNumber,
        source_level: Option<usize>,
    ) -> SequenceNumber {
        if self.fragments.is_empty() {
            return 0;
        }

        // Binary search: find the last fragment whose begin <= user_key.
        let idx = self
            .fragments
            .partition_point(|f| f.begin.as_slice() <= user_key);
        if idx == 0 {
            return 0;
        }
        let frag = &self.fragments[idx - 1];

        // Check user_key is within [begin, end).
        if user_key >= frag.end.as_slice() {
            return 0;
        }

        // seq_levels are sorted by seq descending; find max visible seq,
        // optionally filtering by level.
        for &(seq, level) in &frag.seq_levels {
            if seq > snapshot {
                continue;
            }
            // Only tombstones from shallower levels can delete this key.
            if let Some(src_lvl) = source_level
                && level >= src_lvl
            {
                continue;
            }
            return seq;
        }
        0
    }

    /// Whether the list contains any tombstones.
    pub fn is_empty(&self) -> bool {
        self.fragments.is_empty()
    }

    /// Export tombstones as `(begin, end, seq)` triples.
    /// Each fragment may produce multiple triples (one per seq).
    pub fn tombstones(&self) -> Vec<(Vec<u8>, Vec<u8>, SequenceNumber)> {
        let mut result = Vec::new();
        for frag in &self.fragments {
            for &(seq, _level) in &frag.seq_levels {
                result.push((frag.begin.clone(), frag.end.clone(), seq));
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_tracker() {
        let mut tracker = RangeTombstoneTracker::new();
        assert!(!tracker.is_deleted(b"key", 1, 10));
    }

    #[test]
    fn test_single_tombstone() {
        let mut tracker = RangeTombstoneTracker::new();
        tracker.add(b"aaa".to_vec(), b"zzz".to_vec(), 5);
        tracker.reset();

        assert!(tracker.is_deleted(b"bbb", 3, 10));
        assert!(!tracker.is_deleted(b"bbb", 6, 10)); // seq > tombstone seq
        assert!(!tracker.is_deleted(b"000", 3, 10)); // before range
    }

    #[test]
    fn test_same_seq_not_deleted() {
        // A range tombstone at seq=5 must NOT delete entries also at seq=5.
        // This preserves atomicity for WriteBatch (delete_range + put at same seq).
        let mut tracker = RangeTombstoneTracker::new();
        tracker.add(b"a".to_vec(), b"z".to_vec(), 5);
        tracker.reset();

        // Same seq as tombstone: should NOT be deleted (strict >)
        assert!(!tracker.is_deleted(b"m", 5, 10));
        // Lower seq: should be deleted
        assert!(tracker.is_deleted(b"m", 4, 10));
    }

    #[test]
    fn test_forward_sweep() {
        let mut tracker = RangeTombstoneTracker::new();
        tracker.add(b"b".to_vec(), b"d".to_vec(), 5);
        tracker.add(b"f".to_vec(), b"h".to_vec(), 5);
        tracker.reset();

        assert!(!tracker.is_deleted(b"a", 1, 10));
        assert!(tracker.is_deleted(b"b", 1, 10));
        assert!(tracker.is_deleted(b"c", 1, 10));
        assert!(!tracker.is_deleted(b"d", 1, 10));
        assert!(!tracker.is_deleted(b"e", 1, 10));
        assert!(tracker.is_deleted(b"f", 1, 10));
        assert!(tracker.is_deleted(b"g", 1, 10));
        assert!(!tracker.is_deleted(b"h", 1, 10));
    }

    #[test]
    fn test_many_tombstones() {
        let mut tracker = RangeTombstoneTracker::new();
        for i in 0..100u32 {
            let begin = format!("key_{:04}", i * 2);
            let end = format!("key_{:04}", i * 2 + 1);
            tracker.add(begin.into_bytes(), end.into_bytes(), 5);
        }
        tracker.reset();

        // Even keys are deleted, odd keys are not
        for i in 0..200u32 {
            let key = format!("key_{:04}", i);
            let deleted = tracker.is_deleted(key.as_bytes(), 1, 10);
            if i % 2 == 0 && i < 200 {
                assert!(deleted, "key_{:04} should be deleted", i);
            }
        }
    }

    // -----------------------------------------------------------------------
    // FragmentedRangeTombstoneList tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_fragmented_empty() {
        let list = FragmentedRangeTombstoneList::empty();
        assert!(list.is_empty());
        assert_eq!(list.max_covering_tombstone_seq(b"any", 100), 0);

        let list2 = FragmentedRangeTombstoneList::new(vec![]);
        assert!(list2.is_empty());
    }

    #[test]
    fn test_fragmented_single_tombstone() {
        // [a, z) @ seq 5
        let list = FragmentedRangeTombstoneList::new(vec![(b"a".to_vec(), b"z".to_vec(), 5)]);
        assert!(!list.is_empty());

        // Inside range: covered
        assert_eq!(list.max_covering_tombstone_seq(b"a", 10), 5);
        assert_eq!(list.max_covering_tombstone_seq(b"m", 10), 5);
        assert_eq!(list.max_covering_tombstone_seq(b"y", 10), 5);

        // Outside range: not covered
        assert_eq!(list.max_covering_tombstone_seq(b"z", 10), 0); // end is exclusive
        assert_eq!(list.max_covering_tombstone_seq(b"\0", 10), 0); // before "a"

        // Snapshot filtering: tombstone seq 5 not visible at snapshot 3
        assert_eq!(list.max_covering_tombstone_seq(b"m", 3), 0);
        assert_eq!(list.max_covering_tombstone_seq(b"m", 5), 5);
    }

    #[test]
    fn test_fragmented_same_seq_not_deleted() {
        // Tombstone at seq=5. Entry at seq=5 should NOT be deleted (strict >).
        let list = FragmentedRangeTombstoneList::new(vec![(b"a".to_vec(), b"z".to_vec(), 5)]);
        // max_covering_tombstone_seq returns 5. Caller checks: 5 > entry_seq.
        // For entry_seq=5: 5 > 5 = false → not deleted. Correct.
        // For entry_seq=4: 5 > 4 = true → deleted. Correct.
        let max_seq = list.max_covering_tombstone_seq(b"m", 10);
        assert_eq!(max_seq, 5);
        assert!(max_seq <= 5); // same-seq: not deleted
        assert!(max_seq > 4); // older: deleted
    }

    #[test]
    fn test_fragmented_overlapping() {
        // [a, m) @ seq 5 and [f, z) @ seq 8
        // Fragments should be: [a,f)@{5}, [f,m)@{8,5}, [m,z)@{8}
        let list = FragmentedRangeTombstoneList::new(vec![
            (b"a".to_vec(), b"m".to_vec(), 5),
            (b"f".to_vec(), b"z".to_vec(), 8),
        ]);

        // Region [a, f): only tombstone @5
        assert_eq!(list.max_covering_tombstone_seq(b"a", 10), 5);
        assert_eq!(list.max_covering_tombstone_seq(b"c", 10), 5);
        assert_eq!(list.max_covering_tombstone_seq(b"e", 10), 5);

        // Region [f, m): both tombstones, max is 8
        assert_eq!(list.max_covering_tombstone_seq(b"f", 10), 8);
        assert_eq!(list.max_covering_tombstone_seq(b"h", 10), 8);
        assert_eq!(list.max_covering_tombstone_seq(b"l", 10), 8);

        // Region [m, z): only tombstone @8
        assert_eq!(list.max_covering_tombstone_seq(b"m", 10), 8);
        assert_eq!(list.max_covering_tombstone_seq(b"p", 10), 8);
        assert_eq!(list.max_covering_tombstone_seq(b"y", 10), 8);

        // Outside: not covered
        assert_eq!(list.max_covering_tombstone_seq(b"z", 10), 0);

        // Snapshot filtering: at snapshot 6, only seq 5 is visible
        assert_eq!(list.max_covering_tombstone_seq(b"h", 6), 5); // overlap region
        assert_eq!(list.max_covering_tombstone_seq(b"p", 6), 0); // only seq 8, not visible
    }

    #[test]
    fn test_fragmented_nested() {
        // [a, z) @ seq 5 fully contains [d, f) @ seq 8
        // Fragments: [a,d)@{5}, [d,f)@{8,5}, [f,z)@{5}
        let list = FragmentedRangeTombstoneList::new(vec![
            (b"a".to_vec(), b"z".to_vec(), 5),
            (b"d".to_vec(), b"f".to_vec(), 8),
        ]);

        assert_eq!(list.max_covering_tombstone_seq(b"b", 10), 5);
        assert_eq!(list.max_covering_tombstone_seq(b"d", 10), 8); // inner: max is 8
        assert_eq!(list.max_covering_tombstone_seq(b"e", 10), 8);
        assert_eq!(list.max_covering_tombstone_seq(b"f", 10), 5); // past inner, back to 5
        assert_eq!(list.max_covering_tombstone_seq(b"x", 10), 5);
    }

    #[test]
    fn test_fragmented_adjacent() {
        // [a, c) @ seq 5 and [c, f) @ seq 8 — adjacent, no overlap
        let list = FragmentedRangeTombstoneList::new(vec![
            (b"a".to_vec(), b"c".to_vec(), 5),
            (b"c".to_vec(), b"f".to_vec(), 8),
        ]);

        assert_eq!(list.max_covering_tombstone_seq(b"a", 10), 5);
        assert_eq!(list.max_covering_tombstone_seq(b"b", 10), 5);
        assert_eq!(list.max_covering_tombstone_seq(b"c", 10), 8); // exactly at boundary
        assert_eq!(list.max_covering_tombstone_seq(b"d", 10), 8);
        assert_eq!(list.max_covering_tombstone_seq(b"f", 10), 0); // past end
    }

    #[test]
    fn test_fragmented_many_tombstones() {
        // 100 non-overlapping tombstones
        let raw: Vec<_> = (0..100u32)
            .map(|i| {
                let begin = format!("key_{:04}", i * 2);
                let end = format!("key_{:04}", i * 2 + 1);
                (begin.into_bytes(), end.into_bytes(), 5u64)
            })
            .collect();
        let list = FragmentedRangeTombstoneList::new(raw);

        for i in 0..200u32 {
            let key = format!("key_{:04}", i);
            let max_seq = list.max_covering_tombstone_seq(key.as_bytes(), 10);
            if i % 2 == 0 {
                assert_eq!(max_seq, 5, "key_{:04} should be covered", i);
            } else {
                assert_eq!(max_seq, 0, "key_{:04} should NOT be covered", i);
            }
        }
    }

    #[test]
    fn test_fragmented_duplicate_seqs() {
        // Two tombstones at same seq covering different ranges
        let list = FragmentedRangeTombstoneList::new(vec![
            (b"a".to_vec(), b"d".to_vec(), 5),
            (b"c".to_vec(), b"f".to_vec(), 5),
        ]);
        // Overlap region [c, d) has seq 5 (deduped)
        assert_eq!(list.max_covering_tombstone_seq(b"b", 10), 5);
        assert_eq!(list.max_covering_tombstone_seq(b"c", 10), 5);
        assert_eq!(list.max_covering_tombstone_seq(b"e", 10), 5);
    }
}

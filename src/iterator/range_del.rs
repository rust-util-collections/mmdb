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
            if rt.seq <= snapshot && rt.seq >= seq {
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
                && rt.seq >= seq
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

    /// Clear all tombstones and reset state.
    pub fn clear(&mut self) {
        self.tombstones.clear();
        self.sorted = false;
        self.next_idx = 0;
        self.active.clear();
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
}

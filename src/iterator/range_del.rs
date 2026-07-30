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
        if !self.sorted {
            if self.tombstones.len() > 1 {
                self.tombstones.sort_by(|a, b| a.begin.cmp(&b.begin));
            }
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
//
// Query index construction, illustrated for two raw tombstones [a,z)@seq5
// and [d,f)@seq8:
//
//   boundaries         a    d    f    z
//   elementary ivals   [a,d) [d,f) [f,z)     <- leaves of the tree, 0..3
//
//   segment-tree assignment (range-update style):
//     seq5/lvl0  covers leaves [0,3): assigned to as few canonical
//                ancestor nodes as needed (O(log N) of them), NOT
//                cloned into each of the 3 leaves individually.
//     seq8/lvl0  covers leaf [1,2) only: assigned directly to that leaf.
//
// A point query for a key inside leaf 1 (`[d,f)`) walks root -> leaf,
// visiting the O(log N) ancestor nodes on that path and checking each one's
// (small) list for the best entry passing the caller's snapshot/level
// filters — it never scans a per-leaf list that duplicated every tombstone
// active there.

/// Elementary-interval boundaries for the query index: `bounds[i]` is the
/// begin key of interval `i`; `bounds[i + 1]` is its (exclusive) end.
/// Derived from every raw begin/end key, sorted and deduplicated. Some
/// intervals may have zero covering tombstones (gaps between tombstones);
/// queries landing there correctly see no coverage.
type Bounds = Vec<Vec<u8>>;

/// Segment tree over the elementary intervals in `Bounds`. Node `i`'s
/// children are `2*i` and `2*i+1` (1-indexed, root at `1`); node `1` spans
/// the whole `[0, num_intervals)` range. Each raw tombstone is inserted into
/// the O(log N) canonical nodes covering the interval range it spans — this
/// is what keeps total node-list size O(T log T) rather than the O(T^2) that
/// results from attaching a copy of every tombstone active at each
/// elementary interval to that interval directly.
type Tree = Vec<Vec<(SequenceNumber, usize)>>;

/// A safe upper bound on node count for a recursive 1-indexed segment tree
/// over `n` leaves, split via `mid = lo + (hi - lo) / 2`. Standard bound.
fn tree_node_capacity(num_intervals: usize) -> usize {
    if num_intervals == 0 {
        0
    } else {
        4 * num_intervals
    }
}

/// Insert `val` into every canonical node covering `[lo, hi)`.
fn tree_insert(
    tree: &mut Tree,
    num_intervals: usize,
    lo: usize,
    hi: usize,
    val: (SequenceNumber, usize),
) {
    if lo >= hi {
        return; // empty/invalid range: no coverage
    }
    tree_insert_rec(tree, 1, 0, num_intervals, lo, hi, val);
}

fn tree_insert_rec(
    tree: &mut Tree,
    node: usize,
    node_lo: usize,
    node_hi: usize,
    lo: usize,
    hi: usize,
    val: (SequenceNumber, usize),
) {
    if hi <= node_lo || node_hi <= lo {
        return; // disjoint
    }
    if lo <= node_lo && node_hi <= hi {
        tree[node].push(val); // this node is a canonical piece of [lo, hi)
        return;
    }
    // A unit-length node can never reach here: for integer bounds, any
    // interval overlapping [node_lo, node_lo + 1) must fully contain it
    // (lo <= node_lo and node_hi <= hi both hold), which the branch above
    // already caught. So `node_hi - node_lo >= 2` and `mid` strictly splits.
    let mid = node_lo + (node_hi - node_lo) / 2;
    tree_insert_rec(tree, 2 * node, node_lo, mid, lo, hi, val);
    tree_insert_rec(tree, 2 * node + 1, mid, node_hi, lo, hi, val);
}

/// Walk root -> `leaf`, returning the max seq among ancestor-node entries
/// passing the snapshot/level filters.
fn tree_query(
    tree: &Tree,
    num_intervals: usize,
    leaf: usize,
    snapshot: SequenceNumber,
    source_level: Option<usize>,
) -> SequenceNumber {
    let mut node = 1usize;
    let mut node_lo = 0usize;
    let mut node_hi = num_intervals;
    let mut best: SequenceNumber = 0;
    loop {
        // Entries are sorted (seq desc, level asc): the first one passing
        // both filters is this node's own best contribution.
        for &(seq, level) in &tree[node] {
            if seq > snapshot {
                continue;
            }
            if let Some(src_lvl) = source_level
                && level > src_lvl
            {
                continue;
            }
            if seq > best {
                best = seq;
            }
            break;
        }
        if node_hi - node_lo <= 1 {
            break;
        }
        let mid = node_lo + (node_hi - node_lo) / 2;
        if leaf < mid {
            node_hi = mid;
            node *= 2;
        } else {
            node_lo = mid;
            node = 2 * node + 1;
        }
    }
    best
}

/// Pre-fragmented, immutable range tombstone index.
///
/// Supports O(log T)-ish binary search + ancestor-walk lookup for any user
/// key, replacing both the sweep-line tracker and a linear-scan fallback,
/// without materializing the full active-tombstone set at every elementary
/// interval (which would be O(T) per interval and O(T^2) overall for deeply
/// nested/overlapping input — see the module-level diagram above).
///
/// Constructed once at iterator creation time from all sources' cached
/// tombstones; never mutated afterwards.
pub(crate) struct FragmentedRangeTombstoneList {
    /// Original `(begin, end, seq, level)` tuples exactly as supplied to the
    /// constructor. Source of truth for `tombstones()`; also lets `is_empty`
    /// answer without touching the query index.
    raw: Vec<(Vec<u8>, Vec<u8>, SequenceNumber, usize)>,
    /// Query index (empty when `raw` is empty).
    bounds: Bounds,
    tree: Tree,
}

impl FragmentedRangeTombstoneList {
    /// Create an empty list (no tombstones).
    pub fn empty() -> Self {
        Self {
            raw: Vec::new(),
            bounds: Vec::new(),
            tree: Vec::new(),
        }
    }

    /// Build from raw tombstones: `(begin, end, seq)` triples.
    /// All tombstones are assigned level 0 (no cross-level pruning).
    pub fn new(raw: Vec<(Vec<u8>, Vec<u8>, SequenceNumber)>) -> Self {
        let with_levels: Vec<_> = raw.into_iter().map(|(b, e, s)| (b, e, s, 0usize)).collect();
        Self::new_with_levels(with_levels)
    }

    /// Build from raw tombstones with level info: `(begin, end, seq, level)`.
    pub fn new_with_levels(raw: Vec<(Vec<u8>, Vec<u8>, SequenceNumber, usize)>) -> Self {
        if raw.is_empty() {
            return Self::empty();
        }

        // Elementary-interval boundaries: every distinct begin/end key.
        let mut bounds: Bounds = Vec::with_capacity(raw.len() * 2);
        for (begin, end, _, _) in &raw {
            bounds.push(begin.clone());
            bounds.push(end.clone());
        }
        bounds.sort();
        bounds.dedup();

        let num_intervals = bounds.len() - 1;
        let mut tree: Tree = vec![Vec::new(); tree_node_capacity(num_intervals)];

        for (begin, end, seq, level) in &raw {
            if begin >= end {
                continue; // empty/invalid range contributes no coverage
            }
            // `bounds` contains this tombstone's begin/end exactly (both were
            // pushed above), so these binary searches always hit.
            let lo = bounds
                .binary_search(begin)
                .expect("begin was pushed into bounds above");
            let hi = bounds
                .binary_search(end)
                .expect("end was pushed into bounds above");
            tree_insert(&mut tree, num_intervals, lo, hi, (*seq, *level));
        }

        for node in &mut tree {
            // Descending seq so the first entry passing the caller's
            // snapshot/level filters is the answer. Level ascending breaks
            // ties among equal seqs (possible once compaction zeroes several
            // bottommost tombstones' sequence numbers together), trying the
            // most permissive (shallowest) level first.
            node.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        }

        Self { raw, bounds, tree }
    }

    /// Find the highest-seq tombstone covering `user_key` visible at `snapshot`.
    /// Returns 0 if no covering tombstone exists.
    pub fn max_covering_tombstone_seq(
        &self,
        user_key: &[u8],
        snapshot: SequenceNumber,
    ) -> SequenceNumber {
        self.max_covering_tombstone_seq_for_level(user_key, snapshot, None)
    }

    /// Level-aware variant: excludes tombstones from levels strictly deeper
    /// than `source_level`. A tombstone that is *at or shallower than* the
    /// key's own level may still delete it — that is the ordinary, essential
    /// way range deletes (and same-level Put+DeleteRange, e.g. entirely
    /// within one MemTable) work, and must not be excluded. Only a tombstone
    /// strictly *deeper* than the key is excluded: since compaction only
    /// ever moves data to deeper levels over time, a tombstone that has
    /// already been compacted past the key's own level cannot represent a
    /// legitimate later-in-time delete of that key — trusting it would risk
    /// hiding a key whose sequence was zeroed at the bottommost level
    /// (`is_bottommost && seq < oldest_snapshot_seq`) based on a coincidental
    /// but unrelated deeper tombstone. Pass `None` for no level filtering
    /// (backward compatible).
    pub fn max_covering_tombstone_seq_for_level(
        &self,
        user_key: &[u8],
        snapshot: SequenceNumber,
        source_level: Option<usize>,
    ) -> SequenceNumber {
        let num_intervals = match self.bounds.len().checked_sub(1) {
            Some(0) | None => return 0,
            Some(n) => n,
        };

        // Binary search: find the last interval whose begin <= user_key.
        let idx = self.bounds[..num_intervals].partition_point(|b| b.as_slice() <= user_key);
        if idx == 0 {
            return 0;
        }
        let leaf = idx - 1;

        // Check user_key is within [begin, end).
        if user_key >= self.bounds[leaf + 1].as_slice() {
            return 0;
        }

        tree_query(&self.tree, num_intervals, leaf, snapshot, source_level)
    }

    /// Whether the list contains any tombstones.
    pub fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }

    /// Export tombstones as `(begin, end, seq)` triples — exactly the raw
    /// input, one triple per original tombstone (never re-expanded through
    /// the fragmented query index, which could otherwise multiply out an
    /// aggregate re-fragmentation downstream).
    pub fn tombstones(&self) -> Vec<(Vec<u8>, Vec<u8>, SequenceNumber)> {
        self.raw
            .iter()
            .map(|(b, e, s, _)| (b.clone(), e.clone(), *s))
            .collect()
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

    /// N tombstones nested inside one another (tombstone `i` covers
    /// `[i, 2N-i)` at seq `i+1`) is the adversarial case that made the old
    /// per-fragment representation clone and sort every active tombstone
    /// into every one of the ~2N elementary intervals it overlapped —
    /// O(N) work and memory attached to each of O(N) intervals, O(N^2)
    /// overall. The segment-tree index instead assigns each tombstone to
    /// O(log N) canonical nodes, so total stored entries stay near O(N log N).
    #[test]
    fn test_fragmented_nested_bounded_storage() {
        let n: usize = 2000;
        let raw: Vec<(Vec<u8>, Vec<u8>, SequenceNumber)> = (0..n)
            .map(|i| {
                let begin = format!("{i:06}").into_bytes();
                let end = format!("{:06}", 2 * n - i).into_bytes();
                (begin, end, (i + 1) as SequenceNumber)
            })
            .collect();

        let list = FragmentedRangeTombstoneList::new(raw);

        // Cardinality: total entries stored across the whole segment tree
        // must stay near O(n log n), nowhere close to the O(n^2) a
        // per-fragment clone would produce (>= n^2/4 = 1,000,000 for
        // n=2000, since at least half the elementary intervals are covered
        // by at least n/2 tombstones simultaneously).
        let total_entries: usize = list.tree.iter().map(|node| node.len()).sum();
        assert!(
            total_entries < n * 40,
            "expected roughly O(n log n) (n*40 = {}) but stored {total_entries} \
             entries for n={n} fully-nested tombstones",
            n * 40
        );
        assert!(
            total_entries < n * n / 4,
            "storage did not avoid the O(n^2) blowup: {total_entries} entries for n={n}"
        );

        // Coverage: the innermost point is covered by every tombstone
        // (highest seq wins); the outermost point only by the outermost
        // (seq=1) tombstone.
        let center = format!("{n:06}").into_bytes();
        assert_eq!(
            list.max_covering_tombstone_seq(&center, n as SequenceNumber),
            n as SequenceNumber,
            "innermost key must see the highest (innermost) seq"
        );
        assert_eq!(
            list.max_covering_tombstone_seq(b"000000", n as SequenceNumber),
            1,
            "outermost key is covered only by the outermost (seq=1) tombstone"
        );
        // A snapshot older than every seq must see no coverage at all.
        assert_eq!(list.max_covering_tombstone_seq(&center, 0), 0);
    }

    /// Exhaustive differential check against a brute-force oracle over a
    /// deliberately messy mix: full nesting, partial overlap, exact
    /// adjacency, identical ranges at different levels/seqs, and duplicate
    /// seqs at different levels (which compaction's bottommost seq-zeroing
    /// can legitimately produce — see `test_same_seq_not_deleted`-style
    /// scenarios at scale). Covers every requested key/snapshot/level
    /// combination, not just a few hand-picked points.
    #[test]
    fn test_fragmented_matches_brute_force_oracle() {
        fn brute_force(
            raw: &[(Vec<u8>, Vec<u8>, SequenceNumber, usize)],
            key: &[u8],
            snapshot: SequenceNumber,
            source_level: Option<usize>,
        ) -> SequenceNumber {
            let mut best = 0;
            for (b, e, s, l) in raw {
                let level_ok = source_level.is_none_or(|sl| *l <= sl);
                if *s <= snapshot
                    && key >= b.as_slice()
                    && key < e.as_slice()
                    && level_ok
                    && *s > best
                {
                    best = *s;
                }
            }
            best
        }

        let raw: Vec<(Vec<u8>, Vec<u8>, SequenceNumber, usize)> = vec![
            (vec![0], vec![100], 1, 0),
            (vec![10], vec![90], 5, 1),
            (vec![20], vec![80], 3, 0),
            (vec![20], vec![80], 9, 2),
            (vec![30], vec![40], 20, 3),
            (vec![40], vec![50], 21, 0),
            (vec![60], vec![70], 2, 2),
            (vec![0], vec![5], 0, 0),
            (vec![0], vec![5], 0, 1),
            (vec![95], vec![100], 30, 5),
            (vec![100], vec![110], 31, 0),
        ];

        let list = FragmentedRangeTombstoneList::new_with_levels(raw.clone());

        for key_byte in 0u8..=120 {
            let key = [key_byte];
            for snapshot in [0u64, 1, 2, 5, 9, 20, 21, 30, 31, u64::MAX] {
                for source_level in [None, Some(0usize), Some(1), Some(2), Some(3), Some(5)] {
                    let expected = brute_force(&raw, &key, snapshot, source_level);
                    let actual =
                        list.max_covering_tombstone_seq_for_level(&key, snapshot, source_level);
                    assert_eq!(
                        actual, expected,
                        "key={key_byte} snapshot={snapshot} source_level={source_level:?}"
                    );
                }
            }
        }
    }
}

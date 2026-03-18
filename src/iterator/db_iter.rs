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
    /// Overshoot buffer for backward iteration: when collecting entries for one
    /// user key, we may read the first entry of the *previous* user key. This
    /// field saves that entry so the next backward walk consumes it first.
    prev_overshoot: Option<(Vec<u8>, Vec<u8>)>,
    /// True when the last operation was a backward resolution (prev/seek_to_last).
    /// On the next forward iteration (next_visible), the merger must be re-seeked
    /// past the current user key to resume forward scanning correctly.
    backward_positioned: bool,
    /// Whether range tombstones have been preloaded from all sources.
    /// In backward iteration, range deletion entries are encountered AFTER the
    /// keys they cover (since begin_key < covered keys). Preloading ensures all
    /// tombstones are in the tracker before any backward visibility check.
    range_tombstones_preloaded: bool,
    /// Hint: if false, no source contains range deletions, so preload can be skipped.
    /// Set by DB layer which knows per-file and per-memtable range deletion status.
    may_have_range_deletions: bool,
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
            prev_overshoot: None,
            backward_positioned: false,
            range_tombstones_preloaded: false,
            may_have_range_deletions: true,
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
            prev_overshoot: None,
            backward_positioned: false,
            range_tombstones_preloaded: false,
            may_have_range_deletions: true,
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
            prev_overshoot: None,
            backward_positioned: false,
            range_tombstones_preloaded: false,
            may_have_range_deletions: true,
        }
    }

    /// Hint that no source contains range deletions, so the expensive
    /// preload scan can be skipped entirely on backward iteration.
    pub fn set_no_range_deletions(&mut self) {
        self.may_have_range_deletions = false;
        self.range_tombstones_preloaded = true; // nothing to preload
    }

    /// Set an exclusive upper bound on user keys.
    /// Iteration stops when user key >= this bound.
    /// Models RocksDB's `ReadOptions::iterate_upper_bound`.
    pub fn set_upper_bound(&mut self, bound: Vec<u8>) {
        self.iterate_upper_bound = Some(bound);
        // Invalidate any buffered entry — it may now be beyond the new bound.
        self.current = None;
        self.needs_advance = true;
    }

    /// Scan all merger entries to collect RangeDeletion entries into the tracker.
    /// Called once before the first backward operation to ensure all range tombstones
    /// are available for visibility checks. In backward order, range deletion entries
    /// are encountered AFTER the keys they cover.
    fn preload_range_tombstones(&mut self) {
        if self.range_tombstones_preloaded {
            return;
        }
        self.range_tombstones_preloaded = true;

        // Save merger state, scan from beginning for all range tombstones
        self.merger.seek_to_first();
        loop {
            let entry = self.merger.next_entry();
            match entry {
                Some((ikey, value)) => {
                    if ikey.len() < 8 {
                        continue;
                    }
                    let ikr = InternalKeyRef::new(&ikey);
                    if ikr.value_type() == ValueType::RangeDeletion {
                        let uk_len = ikey.len() - 8;
                        self.range_tombstones
                            .add(ikey[..uk_len].to_vec(), value, ikr.sequence());
                    }
                }
                None => break,
            }
        }
        self.range_tombstones.reset();
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
        // After a backward operation (prev/seek_to_last), the merger is in backward mode.
        // Re-seek forward past the last user key so forward iteration resumes correctly.
        if self.backward_positioned {
            self.backward_positioned = false;
            self.prev_overshoot = None;
            if self.has_last_key {
                use crate::types::InternalKey;
                // Seek past all entries for last_user_key. In compare_internal_key order,
                // (user_key, 0, Deletion) is the last entry for a user_key, so seeking
                // there positions us at or past it.
                let seek_key = InternalKey::new(&self.last_user_key, 0, ValueType::Deletion);
                self.merger.seek(seek_key.as_bytes());
                // has_last_key stays true so next_visible deduplicates correctly
            }
        }
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
        self.prev_overshoot = None;
        self.backward_positioned = false;
    }

    pub fn seek_to_first(&mut self) {
        use crate::types::InternalKey;
        // Seek to the smallest possible internal key (empty user key, max sequence).
        let seek_key = InternalKey::new(b"", crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value);
        self.merger.seek(seek_key.as_bytes());
        self.has_last_key = false;
        self.needs_advance = true;
        self.current = None;
        self.backward_positioned = false;
    }

    /// Fully reset internal deduplication/tombstone state, then seek to first.
    /// Used by BidiIterator when materializing after a seek_to_last().
    pub fn reset_and_seek_to_first(&mut self) {
        self.has_last_key = false;
        self.last_user_key.clear();
        self.range_tombstones.clear();
        self.prev_overshoot = None;
        self.backward_positioned = false;
        self.range_tombstones_preloaded = false;
        self.current = None;
        self.needs_advance = true;
        self.seek_to_first();
    }

    /// Seek to the last visible user key <= target.
    ///
    /// Uses a single backward seek + inline resolution. No redundant forward seek.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        use crate::types::InternalKey;

        // Preload range tombstones (one-time scan) so backward resolution can
        // correctly filter deleted keys.
        self.preload_range_tombstones();

        // Use merger backward seek to find the last internal key <= target
        let seek_key = InternalKey::new(target, 0, ValueType::Deletion);
        self.merger.seek_for_prev(seek_key.as_bytes());
        self.prev_overshoot = None;
        self.has_last_key = false;

        // Walk backward with inline resolution.
        // Use bound = target + \0 so that target itself is included in the search
        // (resolve_prev_user_key looks for user keys strictly < bound).
        let mut bound = target.to_vec();
        bound.push(0x00);
        self.resolve_prev_user_key(&bound);
    }

    /// Internal: given the merger positioned backward at or before target,
    /// find the previous visible user key and position on it.
    ///
    /// Uses inline backward resolution: walks backward through the merger,
    /// collecting all entries for each user key, and resolves visibility
    /// without any forward re-seek. O(1) amortized per call instead of O(K·logN).
    ///
    /// Internal key ordering: user_key ASC, seq DESC. When walking backward,
    /// entries for the same user_key appear in seq ascending order (low→high).
    /// The LAST entry with seq <= snapshot is the newest visible version.
    fn resolve_prev_user_key(&mut self, skip_bound: &[u8]) {
        let mut current_bound: Vec<u8> = skip_bound.to_vec();

        loop {
            // Collect all entries for the first user_key < current_bound.
            // Because internal keys sort (user_key ASC, seq DESC), walking backward
            // we first see the lowest-seq entries, then higher-seq ones for the same user_key.
            let mut candidate_uk: Option<Vec<u8>> = None;
            // Best visible entry: (user_key, value) with highest seq <= snapshot
            let mut best_entry: Option<(Vec<u8>, Vec<u8>)> = None;
            let mut best_seq: SequenceNumber = 0;
            // Track if best visible version is a deletion
            let mut best_is_deletion = false;

            // First, consume any overshoot entry saved from a previous backward walk
            let first_entry = self
                .prev_overshoot
                .take()
                .or_else(|| self.merger.prev_entry());

            let mut iter_entry = first_entry;

            while let Some((ikey, value)) = iter_entry.take() {
                if ikey.len() < 8 {
                    iter_entry = self.merger.prev_entry();
                    continue;
                }

                let uk_len = ikey.len() - 8;
                let uk = &ikey[..uk_len];

                // Prefix guard
                if let Some(ref pfx) = self.prefix
                    && !uk.starts_with(pfx)
                {
                    break;
                }

                // Skip entries >= current_bound (same or later user key)
                if uk >= current_bound.as_slice() {
                    iter_entry = self.merger.prev_entry();
                    continue;
                }

                let ikr = InternalKeyRef::new(&ikey);
                let seq = ikr.sequence();
                let vt = ikr.value_type();

                match &candidate_uk {
                    None => {
                        // First entry for a new user key < current_bound
                        candidate_uk = Some(uk.to_vec());
                    }
                    Some(cuk) => {
                        if uk != cuk.as_slice() {
                            // We've moved to a different (earlier) user key.
                            // Save this entry as overshoot for next iteration.
                            self.prev_overshoot = Some((ikey, value));
                            break;
                        }
                    }
                }

                // Collect range deletions into tracker
                if vt == ValueType::RangeDeletion {
                    self.range_tombstones.add(uk.to_vec(), value.clone(), seq);
                    iter_entry = self.merger.prev_entry();
                    continue;
                }

                // Track the highest-seq visible version for this user key.
                // Backward order = seq ascending, so each new entry has higher seq.
                if seq <= self.sequence && seq > best_seq {
                    best_seq = seq;
                    best_is_deletion = vt == ValueType::Deletion;
                    if !best_is_deletion {
                        // Truncate ikey to user key in-place
                        let mut user_key = ikey;
                        user_key.truncate(uk_len);
                        best_entry = Some((user_key, value));
                    } else {
                        best_entry = None;
                    }
                }

                iter_entry = self.merger.prev_entry();
            }

            match candidate_uk {
                Some(cuk) => {
                    if best_is_deletion {
                        // Newest visible version is a deletion — skip this user key
                        current_bound = cuk;
                        continue;
                    }
                    match best_entry {
                        Some((uk, val)) => {
                            // Check range tombstone coverage
                            if !self.range_tombstones.is_empty()
                                && self.range_tombstones.check_any_direction(
                                    &uk,
                                    best_seq,
                                    self.sequence,
                                )
                            {
                                // Covered by range tombstone — skip
                                current_bound = cuk;
                                continue;
                            }
                            // Save user key for forward re-seek on direction change
                            self.last_user_key.clear();
                            self.last_user_key.extend_from_slice(&uk);
                            self.has_last_key = true;
                            self.backward_positioned = true;
                            self.current = Some((uk, val));
                            self.needs_advance = false;
                            return;
                        }
                        None => {
                            // No visible version (all entries too new) — skip
                            current_bound = cuk;
                            continue;
                        }
                    }
                }
                None => {
                    // No more entries
                    self.current = None;
                    self.needs_advance = false;
                    self.backward_positioned = false;
                    return;
                }
            }
        }
    }

    /// Move to the previous visible user key.
    ///
    /// Uses inline backward resolution: O(1) amortized per call.
    pub fn prev(&mut self) {
        // Preload range tombstones if not yet done (one-time cost).
        self.preload_range_tombstones();

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
    /// then resolves visibility inline (no forward re-seek).
    pub fn seek_to_last(&mut self) {
        use crate::types::InternalKey;

        // Preload range tombstones (one-time scan) so backward resolution can
        // correctly filter deleted keys.
        self.preload_range_tombstones();

        // Determine the effective upper bound for backward seek.
        // Priority: prefix upper bound > iterate_upper_bound > seek_to_last_merge.
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
        } else if let Some(ref ub) = self.iterate_upper_bound {
            // Upper-bound constrained: seek backward from upper bound.
            let seek_key =
                InternalKey::new(ub, crate::types::MAX_SEQUENCE_NUMBER, ValueType::Value);
            self.merger.seek_for_prev(seek_key.as_bytes());
        } else {
            self.merger.seek_to_last_merge();
        }

        self.has_last_key = false;
        self.range_tombstones.reset();
        self.prev_overshoot = None;

        // Compute an effective upper bound for resolve_prev_user_key.
        // We need a key that's larger than any valid user key in the iteration range.
        let upper_bound = if let Some(ref ub) = self.iterate_upper_bound {
            ub.clone()
        } else {
            // Use a key larger than anything possible: [0xFF; 256]
            vec![0xFF; 256]
        };

        // Use inline backward resolution to find the last visible key
        self.resolve_prev_user_key(&upper_bound);
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

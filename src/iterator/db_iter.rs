//! DBIterator: a user-facing iterator that merges all sources, resolves
//! sequence numbers, and skips tombstones.

use std::cmp::Ordering;

use crate::iterator::merge::{IterSource, MergingIterator};
use crate::iterator::range_del::FragmentedRangeTombstoneList;
use crate::types::{
    InternalKeyRef, LazyValue, MAX_SEQUENCE_NUMBER, SequenceNumber, ValueType, compare_internal_key,
};

type IKeyCompareFn = fn(&[u8], &[u8]) -> Ordering;

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
    current: Option<(Vec<u8>, LazyValue)>,
    /// Whether we've already consumed current via advance().
    needs_advance: bool,
    /// Pre-fragmented, immutable range tombstone index for O(log T) coverage
    /// checks in any direction. Loaded upfront at creation time from all
    /// sources' cached tombstones — no inline collection or full-scan preload.
    range_tombstones: FragmentedRangeTombstoneList,
    /// If set, iteration stops when user key no longer starts with this prefix.
    prefix: Option<Vec<u8>>,
    /// If set, iteration stops when user key >= this bound (exclusive upper bound).
    /// Models RocksDB's `ReadOptions::iterate_upper_bound`.
    iterate_upper_bound: Option<Vec<u8>>,
    /// If set, inclusive lower bound on user keys.
    iterate_lower_bound: Option<Vec<u8>>,
    /// Overshoot buffer for backward iteration: when collecting entries for one
    /// user key, we may read the first entry of the *previous* user key. This
    /// field saves that entry so the next backward walk consumes it first.
    prev_overshoot: Option<(Vec<u8>, LazyValue)>,
    /// True when the last operation was a backward resolution (prev/seek_to_last).
    /// On the next forward iteration (next_visible), the merger must be re-seeked
    /// past the current user key to resume forward scanning correctly.
    backward_positioned: bool,
    /// Optional callback: if it returns `true` for a user key, that entry is skipped.
    skip_point: Option<crate::options::SkipPointFn>,
    /// Last seek target for TrySeekUsingNext optimization. When the next
    /// seek target >= this, the merger can try stepping forward instead of
    /// full re-seeking all sources.
    last_seek_key: Option<Vec<u8>>,
    /// Fast path: true when no range tombstones and no skip_point callback.
    /// Enables next_visible_clean() which skips tombstone checks.
    clean_read: bool,
}

fn ikey_compare(a: &[u8], b: &[u8]) -> Ordering {
    compare_internal_key(a, b)
}

impl DBIterator {
    /// Build a DB iterator from multiple sorted Vec sources of internal key-value pairs.
    pub fn new(sources: Vec<Vec<(Vec<u8>, Vec<u8>)>>, sequence: SequenceNumber) -> Self {
        let iter_sources: Vec<IterSource> = sources.into_iter().map(IterSource::new).collect();

        let merger =
            MergingIterator::new(iter_sources, ikey_compare as fn(&[u8], &[u8]) -> Ordering);

        Self {
            merger,
            sequence,
            last_user_key: Vec::new(),
            has_last_key: false,
            current: None,
            needs_advance: true,
            range_tombstones: FragmentedRangeTombstoneList::empty(),
            prefix: None,
            iterate_upper_bound: None,
            iterate_lower_bound: None,
            prev_overshoot: None,
            backward_positioned: false,
            skip_point: None,
            last_seek_key: None,
            clean_read: true,
        }
    }

    /// Build a DB iterator from pre-built IterSource objects (supports streaming).
    pub fn from_sources(sources: Vec<IterSource>, sequence: SequenceNumber) -> Self {
        let merger = MergingIterator::new(sources, ikey_compare as fn(&[u8], &[u8]) -> Ordering);

        Self {
            merger,
            sequence,
            last_user_key: Vec::new(),
            has_last_key: false,
            current: None,
            needs_advance: true,
            range_tombstones: FragmentedRangeTombstoneList::empty(),
            prefix: None,
            iterate_upper_bound: None,
            iterate_lower_bound: None,
            prev_overshoot: None,
            backward_positioned: false,
            skip_point: None,
            last_seek_key: None,
            clean_read: true,
        }
    }

    /// Build a DB iterator with prefix-bounded iteration.
    /// Iteration stops when user key no longer starts with `prefix`.
    pub fn from_sources_with_prefix(
        sources: Vec<IterSource>,
        sequence: SequenceNumber,
        prefix: Vec<u8>,
    ) -> Self {
        let merger = MergingIterator::new(sources, ikey_compare as fn(&[u8], &[u8]) -> Ordering);

        Self {
            merger,
            sequence,
            last_user_key: Vec::new(),
            has_last_key: false,
            current: None,
            needs_advance: true,
            range_tombstones: FragmentedRangeTombstoneList::empty(),
            prefix: Some(prefix),
            iterate_upper_bound: None,
            iterate_lower_bound: None,
            prev_overshoot: None,
            backward_positioned: false,
            skip_point: None,
            last_seek_key: None,
            clean_read: true,
        }
    }

    /// Reset the iterator with new sources and sequence, reusing allocated memory.
    /// Used by the iterator pool to avoid per-iteration allocation overhead.
    pub fn reset(&mut self, sources: Vec<IterSource>, sequence: SequenceNumber) {
        self.merger.reset(sources);
        self.sequence = sequence;
        self.last_user_key.clear();
        self.has_last_key = false;
        self.current = None;
        self.needs_advance = true;
        self.range_tombstones = FragmentedRangeTombstoneList::empty();
        self.prefix = None;
        self.iterate_upper_bound = None;
        self.iterate_lower_bound = None;
        self.prev_overshoot = None;
        self.backward_positioned = false;
        self.skip_point = None;
        self.last_seek_key = None;
        self.clean_read = true;
    }

    /// Reset with prefix-bounded iteration, reusing allocated memory.
    pub fn reset_with_prefix(
        &mut self,
        sources: Vec<IterSource>,
        sequence: SequenceNumber,
        prefix: Vec<u8>,
    ) {
        self.reset(sources, sequence);
        self.prefix = Some(prefix);
    }

    /// Set pre-collected range tombstones. Called by the DB layer after
    /// collecting tombstones from all memtables and SST files.
    pub fn set_range_tombstones(&mut self, tombstones: Vec<(Vec<u8>, Vec<u8>, SequenceNumber)>) {
        self.range_tombstones = FragmentedRangeTombstoneList::new(tombstones);
        self.clean_read = false;
    }

    /// Set range tombstones with level info for cross-level pruning.
    /// A tombstone from level L can only delete keys from levels > L.
    pub fn set_range_tombstones_with_levels(
        &mut self,
        tombstones: Vec<(Vec<u8>, Vec<u8>, SequenceNumber, usize)>,
    ) {
        self.range_tombstones = FragmentedRangeTombstoneList::new_with_levels(tombstones);
        self.clean_read = false;
    }

    /// No-op: tombstones are loaded upfront; emptiness is checked via is_empty().
    pub fn set_no_range_deletions(&mut self) {
        // Tombstones list is already empty by default.
    }

    /// Set an exclusive upper bound on user keys.
    /// Iteration stops when user key >= this bound.
    /// Models RocksDB's `ReadOptions::iterate_upper_bound`.
    pub fn set_upper_bound(&mut self, bound: Vec<u8>) {
        self.merger
            .set_bounds(self.iterate_lower_bound.as_deref(), Some(&bound));
        self.iterate_upper_bound = Some(bound);
        // Invalidate any buffered entry — it may now be beyond the new bound.
        self.current = None;
        self.needs_advance = true;
    }

    /// Set an inclusive lower bound on user keys.
    pub fn set_lower_bound(&mut self, bound: Vec<u8>) {
        self.merger
            .set_bounds(Some(&bound), self.iterate_upper_bound.as_deref());
        self.iterate_lower_bound = Some(bound);
        self.current = None;
        self.needs_advance = true;
    }

    /// Set both lower (inclusive) and upper (exclusive) bounds on user keys.
    pub fn set_bounds(&mut self, lower: Option<Vec<u8>>, upper: Option<Vec<u8>>) {
        self.merger.set_bounds(lower.as_deref(), upper.as_deref());
        self.iterate_lower_bound = lower;
        self.iterate_upper_bound = upper;
        self.current = None;
        self.needs_advance = true;
    }

    /// Return the first error from any underlying source iterator.
    /// Use after iteration returns `None` to distinguish normal exhaustion
    /// from I/O failures.
    pub fn error(&self) -> Option<String> {
        self.merger.error()
    }

    /// Check if a user key is covered by any range tombstone visible at our snapshot,
    /// without cross-level pruning. Used for backward iteration where the source level
    /// of the best entry may not match `last_source_level` (backward collection
    /// aggregates entries from multiple sources).
    fn is_range_deleted_no_level_filter(&self, user_key: &[u8], seq: SequenceNumber) -> bool {
        if self.range_tombstones.is_empty() {
            return false;
        }
        self.range_tombstones
            .max_covering_tombstone_seq_for_level(user_key, self.sequence, None)
            > seq
    }

    /// Set a skip-point callback. During iteration, any user key for which
    /// the callback returns `true` is silently skipped.
    pub fn set_skip_point(&mut self, f: crate::options::SkipPointFn) {
        self.skip_point = Some(f);
        self.clean_read = false;
    }

    /// Jump to the first key of the next prefix, skipping all remaining keys
    /// under the current prefix in O(log N) instead of O(keys_in_prefix).
    /// `prefix_len` is the number of bytes that define a prefix.
    pub fn next_prefix(&mut self, prefix_len: usize) {
        // Compute the successor prefix: increment the first `prefix_len` bytes.
        if let Some((ref key, _)) = self.current {
            // `current` already holds a user key (internal key was truncated
            // in next_visible), so do NOT call user_key() again.
            let user_key = key.as_slice();
            if user_key.len() >= prefix_len {
                let mut succ = user_key[..prefix_len].to_vec();
                // Increment: find rightmost byte < 0xFF and increment it
                let mut i = succ.len();
                while i > 0 {
                    i -= 1;
                    if succ[i] < 0xFF {
                        succ[i] += 1;
                        succ.truncate(i + 1);
                        // Use seek_opt with try_next=true since succ > current
                        use crate::types::InternalKey;
                        let seek_key =
                            InternalKey::new(&succ, MAX_SEQUENCE_NUMBER, ValueType::Value);
                        self.merger.seek_opt(seek_key.as_bytes(), true);
                        self.has_last_key = false;
                        self.needs_advance = true;
                        self.current = None;
                        self.prev_overshoot = None;
                        self.backward_positioned = false;
                        return;
                    }
                }
                // All bytes were 0xFF — no next prefix possible, exhaust iterator
                self.current = None;
                self.needs_advance = false;
                return;
            }
        }
        // No current entry — advance normally
        self.needs_advance = true;
    }

    /// Advance the streaming iterator to the next visible (user_key, value) pair.
    ///
    /// Uses peek_entry/advance_entry/take_entry to avoid heap allocations for
    /// skipped entries. Only the one returned entry pays the allocation cost.
    fn next_visible(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        // After a backward operation (prev/seek_to_last), the merger is in backward mode.
        // Re-seek forward past the last user key so forward iteration resumes correctly.
        if self.backward_positioned {
            self.backward_positioned = false;
            self.prev_overshoot = None;
            if self.has_last_key {
                use crate::types::InternalKey;
                let seek_key = InternalKey::new(&self.last_user_key, 0, ValueType::Deletion);
                self.merger.seek(seek_key.as_bytes());
            }
        }
        // Decisions extracted from peek_entry() to avoid borrow conflicts.
        // peek_entry() borrows self.merger; we need self.is_range_deleted() etc.
        enum Action {
            Skip,
            Take {
                uk_len: usize,
            },
            /// Deferred tombstone check — need peek_source_level() after
            /// the peek_entry() borrow ends so the heap is initialized.
            TakeCheckTombstone {
                uk_len: usize,
                seq: SequenceNumber,
            },
        }
        loop {
            let action = {
                let (ikey_ref, _value_ref) = self.merger.peek_entry()?;
                if ikey_ref.len() < 8 {
                    Action::Skip
                } else {
                    let ikr = InternalKeyRef::new(ikey_ref);
                    let seq = ikr.sequence();
                    let vt = ikr.value_type();

                    if seq > self.sequence {
                        Action::Skip
                    } else {
                        let uk_len = ikey_ref.len() - 8;

                        // Lower bound check
                        if let Some(ref lb) = self.iterate_lower_bound
                            && ikey_ref[..uk_len] < **lb
                        {
                            Action::Skip
                        }
                        // Prefix boundary check
                        else if let Some(ref pfx) = self.prefix
                            && !ikey_ref[..uk_len].starts_with(pfx)
                        {
                            return None;
                        }
                        // Upper bound check
                        else if let Some(ref ub) = self.iterate_upper_bound
                            && ikey_ref[..uk_len] >= **ub
                        {
                            return None;
                        } else if vt == ValueType::RangeDeletion {
                            // Skip RangeDeletion entries — tombstones are pre-loaded.
                            // Do NOT update last_user_key: RangeDeletion is not a point
                            // mutation for this user key. Updating it would suppress a
                            // same-key Value via dedup instead of the proper range
                            // tombstone coverage check (which uses strict >).
                            Action::Skip
                        } else if self.has_last_key
                            && self.last_user_key.as_slice() == &ikey_ref[..uk_len]
                        {
                            // Duplicate user key — already saw newest version
                            Action::Skip
                        } else {
                            // New user key — update dedup state
                            self.last_user_key.clear();
                            self.last_user_key.extend_from_slice(&ikey_ref[..uk_len]);
                            self.has_last_key = true;

                            if vt == ValueType::Deletion {
                                Action::Skip
                            } else if self.range_tombstones.is_empty() {
                                Action::Take { uk_len }
                            } else {
                                // Defer tombstone check until after peek_entry
                                // borrow ends so we can call peek_source_level().
                                Action::TakeCheckTombstone { uk_len, seq }
                            }
                        }
                    }
                }
            };

            match action {
                Action::Skip => {
                    self.merger.advance_entry();
                    continue;
                }
                Action::TakeCheckTombstone { uk_len, seq } => {
                    // Now that peek_entry() borrow is released and the heap is
                    // initialized, peek_source_level() returns the true level.
                    let source_level = self.merger.peek_source_level();
                    let level_filter = if source_level != usize::MAX {
                        Some(source_level)
                    } else {
                        None
                    };
                    let covered = self.range_tombstones.max_covering_tombstone_seq_for_level(
                        &self.last_user_key,
                        self.sequence,
                        level_filter,
                    ) > seq;
                    if covered {
                        self.merger.advance_entry();
                        continue;
                    }
                    let (mut ikey, value) = self.merger.take_entry()?;
                    ikey.truncate(uk_len);
                    if let Some(ref sp) = self.skip_point
                        && sp(&ikey)
                    {
                        continue;
                    }
                    return Some((ikey, value));
                }
                Action::Take { uk_len } => {
                    let (mut ikey, value) = self.merger.take_entry()?;
                    ikey.truncate(uk_len);
                    if let Some(ref sp) = self.skip_point
                        && sp(&ikey)
                    {
                        continue;
                    }
                    return Some((ikey, value));
                }
            }
        }
    }

    /// Streamlined next_visible for the common case: no range tombstones,
    /// no skip_point callback. Removes per-entry source_level tracking,
    /// tombstone checks, and skip_point callback dispatch.
    #[inline(always)]
    fn next_visible_clean(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        if self.backward_positioned {
            self.backward_positioned = false;
            self.prev_overshoot = None;
            if self.has_last_key {
                use crate::types::InternalKey;
                let seek_key = InternalKey::new(&self.last_user_key, 0, ValueType::Deletion);
                self.merger.seek(seek_key.as_bytes());
            }
        }
        loop {
            let (ikey_ref, _) = self.merger.peek_entry()?;
            if ikey_ref.len() < 8 {
                self.merger.advance_entry();
                continue;
            }
            let uk_len = ikey_ref.len() - 8;
            let ikr = InternalKeyRef::new(ikey_ref);
            let seq = ikr.sequence();
            let vt = ikr.value_type();

            if seq > self.sequence {
                self.merger.advance_entry();
                continue;
            }

            // Lower bound
            if let Some(ref lb) = self.iterate_lower_bound
                && ikey_ref[..uk_len] < **lb
            {
                self.merger.advance_entry();
                continue;
            }

            // Prefix boundary
            if let Some(ref pfx) = self.prefix
                && !ikey_ref[..uk_len].starts_with(pfx)
            {
                return None;
            }

            // Upper bound
            if let Some(ref ub) = self.iterate_upper_bound
                && ikey_ref[..uk_len] >= **ub
            {
                return None;
            }

            // Dedup + deletion check
            if vt == ValueType::RangeDeletion
                || (self.has_last_key && self.last_user_key.as_slice() == &ikey_ref[..uk_len])
            {
                self.merger.advance_entry();
                continue;
            }

            // New user key
            self.last_user_key.clear();
            self.last_user_key.extend_from_slice(&ikey_ref[..uk_len]);
            self.has_last_key = true;

            if vt == ValueType::Deletion {
                self.merger.advance_entry();
                continue;
            }

            let (mut ikey, value) = self.merger.take_entry()?;
            ikey.truncate(uk_len);
            return Some((ikey, value));
        }
    }

    /// Ensure current is populated. Returns whether there's a valid entry.
    fn ensure_current(&mut self) -> bool {
        if self.needs_advance {
            self.current = if self.clean_read {
                self.next_visible_clean()
            } else {
                self.next_visible()
            };
            self.needs_advance = false;
        }
        self.current.is_some()
    }

    pub fn valid(&mut self) -> bool {
        self.ensure_current()
    }

    pub fn key(&mut self) -> Option<&[u8]> {
        self.ensure_current();
        self.current.as_ref().map(|(k, _)| k.as_slice())
    }

    pub fn value(&mut self) -> Option<&[u8]> {
        self.ensure_current();
        self.current.as_ref().map(|(_, v)| v.as_slice())
    }

    pub fn advance(&mut self) {
        self.needs_advance = true;
        self.current = None;
    }

    /// Seek to the first key >= target.
    pub fn seek(&mut self, target: &[u8]) {
        use crate::types::InternalKey;
        let target = self
            .iterate_lower_bound
            .as_deref()
            .filter(|lb| target < *lb)
            .unwrap_or(target);
        // Seek the merger to a synthetic internal key with max sequence
        let seek_key = InternalKey::new(target, MAX_SEQUENCE_NUMBER, ValueType::Value);
        // TrySeekUsingNext: if the new target is strictly after the last seek target, use
        // incremental advancement instead of full re-seek.
        let try_next = self
            .last_seek_key
            .as_ref()
            .is_some_and(|prev| target > prev.as_slice());
        self.merger.seek_opt(seek_key.as_bytes(), try_next);
        self.last_seek_key = Some(target.to_vec());
        self.has_last_key = false;
        self.needs_advance = true;
        self.current = None;
        self.prev_overshoot = None;
        self.backward_positioned = false;
    }

    pub fn seek_to_first(&mut self) {
        // Use seek_to_first directly instead of seeking with empty key.
        self.merger.seek_to_first();
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
        // range_tombstones is immutable — no reset needed.
        self.prev_overshoot = None;
        self.backward_positioned = false;
        self.current = None;
        self.needs_advance = true;
        self.seek_to_first();
    }

    /// Seek to the last visible user key <= target.
    ///
    /// Uses a single backward seek + inline resolution. No redundant forward seek.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        use crate::types::InternalKey;

        let upper_clamps_target = self
            .iterate_upper_bound
            .as_deref()
            .is_some_and(|ub| ub <= target);
        let (seek_key, bound) = if upper_clamps_target {
            let ub = self.iterate_upper_bound.as_deref().unwrap();
            (
                InternalKey::new(ub, MAX_SEQUENCE_NUMBER, ValueType::Value),
                ub.to_vec(),
            )
        } else {
            let mut bound = target.to_vec();
            bound.push(0x00);
            (InternalKey::new(target, 0, ValueType::Deletion), bound)
        };

        // Use merger backward seek to find the last internal key below the effective bound.
        self.merger.seek_for_prev(seek_key.as_bytes());
        self.prev_overshoot = None;
        self.has_last_key = false;

        // Walk backward with inline resolution.
        // For an unclamped target, bound = target + \0 includes target itself.
        self.resolve_prev_user_key(Some(&bound));
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
    fn resolve_prev_user_key(&mut self, skip_bound: Option<&[u8]>) {
        let mut current_bound: Option<Vec<u8>> = skip_bound.map(|b| b.to_vec());

        loop {
            // Collect all entries for the first user_key < current_bound.
            // Because internal keys sort (user_key ASC, seq DESC), walking backward
            // we first see the lowest-seq entries, then higher-seq ones for the same user_key.
            let mut candidate_uk: Option<Vec<u8>> = None;
            // Best visible entry: (user_key, value) with highest seq <= snapshot
            let mut best_entry: Option<(Vec<u8>, LazyValue)> = None;
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

                // Lower bound guard
                if let Some(ref lb) = self.iterate_lower_bound
                    && uk < lb.as_slice()
                {
                    break;
                }

                // Skip entries >= current_bound (same or later user key)
                if let Some(ref bound) = current_bound
                    && uk >= bound.as_slice()
                {
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

                // Skip range deletion entries — tombstones are pre-loaded.
                if vt == ValueType::RangeDeletion {
                    iter_entry = self.merger.prev_entry();
                    continue;
                }

                // Track the highest-seq visible version for this user key.
                // Backward order = seq ascending, so each new entry has higher seq.
                // Use >= so that sequence-0 entries (produced by bottommost compaction)
                // are correctly picked up when best_seq starts at 0.
                if seq <= self.sequence && seq >= best_seq {
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
                        current_bound = Some(cuk);
                        continue;
                    }
                    match best_entry {
                        Some((uk, val)) => {
                            // Check range tombstone coverage (O(log T) binary search).
                            // Use no-level-filter variant: backward collection aggregates
                            // entries from multiple sources, so last_source_level may not
                            // correspond to the winning entry's source.
                            if self.is_range_deleted_no_level_filter(&uk, best_seq) {
                                // Covered by range tombstone — skip
                                current_bound = Some(cuk);
                                continue;
                            }
                            // Check skip_point callback
                            if let Some(ref sp) = self.skip_point
                                && sp(&uk)
                            {
                                current_bound = Some(cuk);
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
                            current_bound = Some(cuk);
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
        // Ensure we have a current entry to move backwards from
        self.ensure_current();
        let saved_key = match &self.current {
            Some((k, _)) => k.clone(),
            None => {
                // No current entry; nothing to go back from
                return;
            }
        };

        self.resolve_prev_user_key(Some(&saved_key));
    }

    /// Seek to the last visible key. Positions the iterator on the very last entry.
    ///
    /// If a prefix is set, seeks to the last key within the prefix.
    /// Uses merger backward seek to find the last user key efficiently,
    /// then resolves visibility inline (no forward re-seek).
    pub fn seek_to_last(&mut self) {
        use crate::types::InternalKey;

        // Determine the effective upper bound for backward seek.
        let mut resolve_bound = self.iterate_upper_bound.clone();
        if let Some(ref pfx) = self.prefix {
            // Prefix-bounded: seek backward from prefix upper bound.
            let mut upper = pfx.clone();
            if {
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
            } && resolve_bound.as_ref().is_none_or(|bound| upper < *bound)
            {
                resolve_bound = Some(upper);
            }
        }

        if let Some(ref ub) = resolve_bound {
            // Upper-bound constrained: seek backward from upper bound.
            let seek_key = InternalKey::new(ub, MAX_SEQUENCE_NUMBER, ValueType::Value);
            self.merger.seek_for_prev(seek_key.as_bytes());
        } else {
            self.merger.seek_to_last_merge();
        }

        self.has_last_key = false;
        self.prev_overshoot = None;

        // Use inline backward resolution to find the last visible key
        self.resolve_prev_user_key(resolve_bound.as_deref());
    }

    /// Return the last user key seen by next_visible(), if any.
    /// Used by BidiIterator for re-seeking on direction change.
    pub fn last_user_key(&self) -> Option<&[u8]> {
        if self.has_last_key {
            Some(&self.last_user_key)
        } else {
            None
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

impl DBIterator {
    /// Advance to the next visible entry, returning LazyValue without
    /// materializing it. Avoids the into_vec() copy that Iterator::next()
    /// must perform.
    #[inline(always)]
    pub fn next_lazy(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        if self.ensure_current() {
            let entry = self.current.take().unwrap();
            self.needs_advance = true;
            Some(entry)
        } else {
            None
        }
    }

    /// Take ownership of the current buffered entry without advancing.
    /// After this call, valid() returns false until the next seek/advance.
    #[inline(always)]
    pub fn take_current(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        let entry = self.current.take();
        self.needs_advance = false;
        entry
    }
}

impl Iterator for DBIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_lazy().map(|(k, lv)| (k, lv.into_vec()))
    }
}

/// A pooled iterator that automatically returns to the global pool on drop.
///
/// Wraps a [`DBIterator`] and implements `Deref`/`DerefMut` so it can be used
/// identically. When dropped, the iterator is reset (releasing `Arc<TableReader>`
/// references) and returned to the global pool for reuse by any thread.
pub struct PooledIterator {
    inner: Option<DBIterator>,
}

impl PooledIterator {
    /// Wrap a `DBIterator` for automatic pool return on drop.
    pub fn new(iter: DBIterator) -> Self {
        Self { inner: Some(iter) }
    }

    /// Take ownership of the inner iterator, disabling automatic pool return.
    pub fn into_inner(mut self) -> DBIterator {
        self.inner.take().unwrap()
    }
}

impl Drop for PooledIterator {
    fn drop(&mut self) {
        if let Some(iter) = self.inner.take() {
            crate::db::pool_return(iter);
        }
    }
}

impl std::ops::Deref for PooledIterator {
    type Target = DBIterator;
    fn deref(&self) -> &DBIterator {
        self.inner.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledIterator {
    fn deref_mut(&mut self) -> &mut DBIterator {
        self.inner.as_mut().unwrap()
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
        assert_eq!(iter.key().unwrap(), b"banana");

        iter.seek(b"blueberry");
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"cherry");

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
        assert_eq!(iter.key().unwrap(), b"a");
        assert_eq!(iter.value().unwrap(), b"1");

        iter.advance();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"b");
        assert_eq!(iter.value().unwrap(), b"2");

        iter.advance();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"c");
        assert_eq!(iter.value().unwrap(), b"3");

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
        assert_eq!(iter.key().unwrap(), b"c");

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"b");
        assert_eq!(iter.value().unwrap(), b"2");

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"a");
        assert_eq!(iter.value().unwrap(), b"1");

        // prev at beginning should invalidate
        iter.prev();
        assert!(!iter.valid());

        // Seek to last, prev through all
        let mut iter = DBIterator::new(vec![source.clone()], 10);
        iter.seek(b"e");
        assert_eq!(iter.key().unwrap(), b"e");

        iter.prev();
        assert_eq!(iter.key().unwrap(), b"d");
        iter.prev();
        assert_eq!(iter.key().unwrap(), b"c");
        iter.prev();
        assert_eq!(iter.key().unwrap(), b"b");
        iter.prev();
        assert_eq!(iter.key().unwrap(), b"a");
        iter.prev();
        assert!(!iter.valid());

        // prev then next should work
        let mut iter = DBIterator::new(vec![source], 10);
        iter.seek(b"c");
        assert_eq!(iter.key().unwrap(), b"c");
        iter.prev();
        assert_eq!(iter.key().unwrap(), b"b");
        // After prev, advance should move to next entry
        iter.advance();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"c");
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
        assert_eq!(iter.key().unwrap(), b"banana");

        // Between entries: blueberry is between banana and cherry
        let mut iter = DBIterator::new(vec![source.clone()], 10);
        iter.seek_for_prev(b"blueberry");
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"banana"); // last key <= "blueberry"

        // Before first key
        let mut iter = DBIterator::new(vec![source.clone()], 10);
        iter.seek_for_prev(b"aaa");
        assert!(!iter.valid()); // nothing <= "aaa"

        // After last key
        let mut iter = DBIterator::new(vec![source.clone()], 10);
        iter.seek_for_prev(b"zzz");
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"date"); // last key in dataset

        // seek_for_prev to first key
        let mut iter = DBIterator::new(vec![source], 10);
        iter.seek_for_prev(b"apple");
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"apple");
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
        assert_eq!(iter.key().unwrap(), b"d");

        // prev should skip deleted "b" and land on "c"
        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"c");

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"a");
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
        assert_eq!(iter.key().unwrap(), b"d");

        iter.prev();
        assert_eq!(iter.key().unwrap(), b"c");
        assert_eq!(iter.value().unwrap(), b"mem_c");

        iter.prev();
        assert_eq!(iter.key().unwrap(), b"b");
        assert_eq!(iter.value().unwrap(), b"sst_b");

        iter.prev();
        assert_eq!(iter.key().unwrap(), b"a");
        assert_eq!(iter.value().unwrap(), b"mem_a");

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
        assert_eq!(iter.key().unwrap(), b"c");
        assert_eq!(iter.value().unwrap(), b"3");

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"b");

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"a");

        iter.prev();
        assert!(!iter.valid());
    }

    #[test]
    fn test_seek_past_range_tombstone_begin() {
        // Bug 1 regression: seek(target) where target is inside a range tombstone
        // [b, e) must still hide keys in that range.
        // With old inline collection, seeking past "b" would miss the tombstone.
        let source = sort_lex(vec![
            make_entry(b"a", 1, ValueType::Value, b"1"),
            make_entry(b"b", 5, ValueType::RangeDeletion, b"e"), // [b, e) @ seq 5
            make_entry(b"b", 2, ValueType::Value, b"2"),
            make_entry(b"c", 3, ValueType::Value, b"3"),
            make_entry(b"d", 4, ValueType::Value, b"4"),
            make_entry(b"e", 6, ValueType::Value, b"6"),
        ]);

        // Pre-load tombstones the way the DB layer would.
        let mut iter = DBIterator::new(vec![source], 10);
        iter.set_range_tombstones(vec![(b"b".to_vec(), b"e".to_vec(), 5)]);

        // Seek to "c" — inside the tombstone range. "c"@3 should be hidden.
        iter.seek(b"c");
        assert!(iter.valid());
        // Next visible key should be "e" (first key outside tombstone range)
        assert_eq!(iter.key().unwrap(), b"e");
        assert_eq!(iter.value().unwrap(), b"6");
    }

    #[test]
    fn test_backward_range_tombstones_no_preload() {
        // Bug 3 fix: backward iteration with range tombstones should work
        // without requiring a full forward scan (preload_range_tombstones removed).
        let source = sort_lex(vec![
            make_entry(b"a", 1, ValueType::Value, b"1"),
            make_entry(b"b", 5, ValueType::RangeDeletion, b"d"), // [b, d) @ seq 5
            make_entry(b"b", 2, ValueType::Value, b"2"),
            make_entry(b"c", 3, ValueType::Value, b"3"),
            make_entry(b"d", 4, ValueType::Value, b"4"),
            make_entry(b"e", 6, ValueType::Value, b"6"),
        ]);

        let mut iter = DBIterator::new(vec![source], 10);
        iter.set_range_tombstones(vec![(b"b".to_vec(), b"d".to_vec(), 5)]);

        // seek_to_last should find "e", then prev should skip "d" (not deleted),
        // skip "c" (deleted by tombstone), skip "b" (deleted), land on "a".
        iter.seek_to_last();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"e");

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"d");

        iter.prev();
        assert!(iter.valid());
        // "c"@3 and "b"@2 are both deleted by [b,d)@5, so prev should reach "a"
        assert_eq!(iter.key().unwrap(), b"a");

        iter.prev();
        assert!(!iter.valid());
    }
}

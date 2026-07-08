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
    /// Start of the reserved batch-overlay sequence region (see
    /// `DB::iter_with_batch`). Entries with `seq >= floor` are uncommitted
    /// batch entries that must be visible *in addition to* entries at or
    /// below `sequence`. Real writers can never produce sequences in the
    /// reserved region, so the two visibility ranges never alias.
    batch_seq_floor: Option<SequenceNumber>,
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
    /// field saves that entry — along with the LSM level it came from
    /// (captured via `peek_source_level()` at the time it was taken, mirroring
    /// the forward path) — so the next backward walk consumes it first with
    /// correct cross-level tombstone pruning.
    prev_overshoot: Option<(Vec<u8>, LazyValue, usize)>,
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
    #[cfg(test)]
    pub(crate) fn new(sources: Vec<Vec<(Vec<u8>, Vec<u8>)>>, sequence: SequenceNumber) -> Self {
        let iter_sources: Vec<IterSource> = sources.into_iter().map(IterSource::new).collect();

        let merger =
            MergingIterator::new(iter_sources, ikey_compare as fn(&[u8], &[u8]) -> Ordering);

        Self {
            merger,
            sequence,
            batch_seq_floor: None,
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
    pub(crate) fn from_sources(sources: Vec<IterSource>, sequence: SequenceNumber) -> Self {
        let merger = MergingIterator::new(sources, ikey_compare as fn(&[u8], &[u8]) -> Ordering);

        Self {
            merger,
            sequence,
            batch_seq_floor: None,
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
    pub(crate) fn from_sources_with_prefix(
        sources: Vec<IterSource>,
        sequence: SequenceNumber,
        prefix: Vec<u8>,
    ) -> Self {
        let merger = MergingIterator::new(sources, ikey_compare as fn(&[u8], &[u8]) -> Ordering);

        Self {
            merger,
            sequence,
            batch_seq_floor: None,
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

    /// Set pre-collected range tombstones. Called by the DB layer after
    /// collecting tombstones from all memtables and SST files.
    #[cfg(test)]
    pub(crate) fn set_range_tombstones(
        &mut self,
        tombstones: Vec<(Vec<u8>, Vec<u8>, SequenceNumber)>,
    ) {
        self.range_tombstones = FragmentedRangeTombstoneList::new(tombstones);
        self.clean_read = false;
    }

    /// Set range tombstones with level info for cross-level pruning.
    /// A tombstone from level L may cover keys at level L or deeper; a
    /// tombstone strictly deeper than a key's source level never covers it.
    pub(crate) fn set_range_tombstones_with_levels(
        &mut self,
        tombstones: Vec<(Vec<u8>, Vec<u8>, SequenceNumber, usize)>,
    ) {
        self.range_tombstones = FragmentedRangeTombstoneList::new_with_levels(tombstones);
        self.clean_read = false;
    }

    /// Mark the start of the reserved batch-overlay sequence region. Entries
    /// (and range tombstones) with `seq >= floor` become visible in addition
    /// to entries at or below the snapshot sequence. Used by
    /// `DB::iter_with_batch` to overlay uncommitted batch writes on top of a
    /// snapshot read without aliasing committed sequence numbers.
    pub(crate) fn set_batch_seq_floor(&mut self, floor: SequenceNumber) {
        self.batch_seq_floor = Some(floor);
    }

    /// Visibility predicate: an entry is visible if it is within the snapshot
    /// (`seq <= sequence`) or part of the batch overlay (`seq >= floor`).
    #[inline]
    fn is_visible(&self, seq: SequenceNumber) -> bool {
        seq <= self.sequence || self.batch_seq_floor.is_some_and(|floor| seq >= floor)
    }

    /// Snapshot bound to use for range-tombstone visibility. The tombstone
    /// list is frozen at iterator creation: it contains only tombstones at or
    /// below the snapshot sequence, plus (for batch iterators) overlay
    /// tombstones in the reserved region — there is nothing in between. With
    /// a batch overlay present, every collected tombstone must be visible, so
    /// the bound is lifted to MAX.
    #[inline]
    fn tombstone_snapshot(&self) -> SequenceNumber {
        if self.batch_seq_floor.is_some() {
            MAX_SEQUENCE_NUMBER
        } else {
            self.sequence
        }
    }

    fn effective_forward_target(&self, target: &[u8]) -> Vec<u8> {
        let mut effective = target;
        if let Some(lb) = self.iterate_lower_bound.as_deref()
            && effective < lb
        {
            effective = lb;
        }
        if let Some(prefix) = self.prefix.as_deref()
            && effective < prefix
        {
            effective = prefix;
        }
        effective.to_vec()
    }

    /// Set an exclusive upper bound on user keys.
    /// Iteration stops when user key >= this bound.
    /// Models RocksDB's `ReadOptions::iterate_upper_bound`.
    pub(crate) fn set_upper_bound(&mut self, bound: Vec<u8>) {
        self.merger
            .set_bounds(self.iterate_lower_bound.as_deref(), Some(&bound));
        self.iterate_upper_bound = Some(bound);
        // Invalidate any buffered entry — it may now be beyond the new bound.
        self.current = None;
        self.needs_advance = true;
        self.prev_overshoot = None;
    }

    /// Set both lower (inclusive) and upper (exclusive) bounds on user keys.
    pub(crate) fn set_bounds(&mut self, lower: Option<Vec<u8>>, upper: Option<Vec<u8>>) {
        self.merger.set_bounds(lower.as_deref(), upper.as_deref());
        self.iterate_lower_bound = lower;
        self.iterate_upper_bound = upper;
        self.current = None;
        self.needs_advance = true;
        self.prev_overshoot = None;
    }

    /// Return the first error from any underlying source iterator.
    /// Use after iteration returns `None` to distinguish normal exhaustion
    /// from I/O failures.
    pub fn error(&self) -> Option<String> {
        self.merger.error()
    }

    /// Set a skip-point callback. During iteration, any user key for which
    /// the callback returns `true` is silently skipped.
    pub(crate) fn set_skip_point(&mut self, f: crate::options::SkipPointFn) {
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
        // Copy visibility parameters out of self: peek_entry() holds a
        // mutable borrow of self.merger across the checks below.
        let snapshot = self.sequence;
        let batch_floor = self.batch_seq_floor;
        loop {
            let action = {
                let (ikey_ref, _value_ref) = self.merger.peek_entry()?;
                if ikey_ref.len() < 8 {
                    Action::Skip
                } else {
                    let ikr = InternalKeyRef::new(ikey_ref);
                    let seq = ikr.sequence();
                    let vt = ikr.value_type();

                    if !(seq <= snapshot || batch_floor.is_some_and(|floor| seq >= floor)) {
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
                            if &ikey_ref[..uk_len] < pfx.as_slice() {
                                Action::Skip
                            } else {
                                return None;
                            }
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
                        self.tombstone_snapshot(),
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
        // Copy visibility parameters out of self: peek_entry() holds a
        // mutable borrow of self.merger across the checks below.
        let snapshot = self.sequence;
        let batch_floor = self.batch_seq_floor;
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

            if !(seq <= snapshot || batch_floor.is_some_and(|floor| seq >= floor)) {
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
                if &ikey_ref[..uk_len] < pfx.as_slice() {
                    self.merger.advance_entry();
                    continue;
                }
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
        let target = self.effective_forward_target(target);
        // Seek the merger to a synthetic internal key with max sequence
        let seek_key = InternalKey::new(&target, MAX_SEQUENCE_NUMBER, ValueType::Value);
        // TrySeekUsingNext: if the new target is strictly after the last seek target, use
        // incremental advancement instead of full re-seek.
        let try_next = self
            .last_seek_key
            .as_ref()
            .is_some_and(|prev| target.as_slice() > prev.as_slice());
        self.merger.seek_opt(seek_key.as_bytes(), try_next);
        self.last_seek_key = Some(target);
        self.has_last_key = false;
        self.needs_advance = true;
        self.current = None;
        self.prev_overshoot = None;
        self.backward_positioned = false;
    }

    pub fn seek_to_first(&mut self) {
        if self.prefix.is_some() || self.iterate_lower_bound.is_some() {
            self.seek(b"");
            return;
        }
        self.merger.seek_to_first();
        self.has_last_key = false;
        self.needs_advance = true;
        self.current = None;
        self.prev_overshoot = None;
        self.last_seek_key = None;
        self.backward_positioned = false;
    }

    /// The shortest key strictly greater than every key sharing this iterator's
    /// prefix (the exclusive upper bound of the prefix range). Returns `None`
    /// when no prefix is set, or the prefix is all `0xFF` (no finite successor).
    fn prefix_successor(&self) -> Option<Vec<u8>> {
        let mut succ = self.prefix.clone()?;
        let mut i = succ.len();
        while i > 0 {
            i -= 1;
            if succ[i] < 0xFF {
                succ[i] += 1;
                succ.truncate(i + 1);
                return Some(succ);
            }
        }
        None
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
        let (mut seek_key, mut bound) = if upper_clamps_target {
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

        // Clamp the backward seek to the prefix range. Without this, a target
        // beyond the prefix (e.g. prefix "a", target "zz") would seek onto a
        // larger non-prefix key ("z1"), where the prefix guard stops the backward
        // walk and misses valid prefix keys below it ("a1").
        if let Some(succ) = self.prefix_successor()
            && succ.as_slice() < bound.as_slice()
        {
            seek_key = InternalKey::new(&succ, MAX_SEQUENCE_NUMBER, ValueType::Value);
            bound = succ;
        }

        // Use merger backward seek to find the last internal key below the effective bound.
        self.merger.seek_for_prev(seek_key.as_bytes());
        self.prev_overshoot = None;
        self.has_last_key = false;

        // Walk backward with inline resolution.
        // For an unclamped target, bound = target + \0 includes target itself.
        self.resolve_prev_user_key(Some(&bound));
    }

    /// Fetch the next backward entry from the merger along with the LSM level
    /// it came from. Mirrors the forward path's `peek_entry()`, `peek_source_level()`,
    /// `take_entry()` ordering: `peek_source_level()` is called immediately before
    /// `prev_entry()`, so it reads the level of the entry `prev_entry()` is about to
    /// pop (`prev_entry()` takes from `self.sources[self.heap[0]]`, exactly what
    /// `peek_source_level()` reads).
    #[inline]
    fn prev_entry_with_level(&mut self) -> Option<(Vec<u8>, LazyValue, usize)> {
        let level = self.merger.peek_source_level();
        self.merger.prev_entry().map(|(k, v)| (k, v, level))
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
            // LSM level the current best_entry came from (for cross-level
            // tombstone pruning), captured via peek_source_level() at the
            // time the entry was taken from the merger.
            let mut best_level: usize = usize::MAX;

            // First, consume any overshoot entry saved from a previous backward walk
            let first_entry = self
                .prev_overshoot
                .take()
                .or_else(|| self.prev_entry_with_level());

            let mut iter_entry = first_entry;

            while let Some((ikey, value, level)) = iter_entry.take() {
                if ikey.len() < 8 {
                    iter_entry = self.prev_entry_with_level();
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
                    iter_entry = self.prev_entry_with_level();
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
                            // Save this entry (with its level) as overshoot for
                            // next iteration.
                            self.prev_overshoot = Some((ikey, value, level));
                            break;
                        }
                    }
                }

                // Skip range deletion entries — tombstones are pre-loaded.
                if vt == ValueType::RangeDeletion {
                    iter_entry = self.prev_entry_with_level();
                    continue;
                }

                // Track the highest-seq visible version for this user key.
                // Backward order = seq ascending, so each new entry has higher seq.
                // Use >= so that sequence-0 entries (produced by bottommost compaction)
                // are correctly picked up when best_seq starts at 0.
                if self.is_visible(seq) && seq >= best_seq {
                    best_seq = seq;
                    best_level = level;
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

                iter_entry = self.prev_entry_with_level();
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
                            // Check range tombstone coverage (O(log T) binary search),
                            // excluding tombstones strictly deeper than best_level —
                            // mirrors next_visible()'s Action::TakeCheckTombstone handling.
                            let level_filter = if best_level != usize::MAX {
                                Some(best_level)
                            } else {
                                None
                            };
                            let covered =
                                self.range_tombstones.max_covering_tombstone_seq_for_level(
                                    &uk,
                                    self.tombstone_snapshot(),
                                    level_filter,
                                ) > best_seq;
                            if covered {
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
        if let Some(succ) = self.prefix_successor()
            && resolve_bound.as_ref().is_none_or(|bound| succ < *bound)
        {
            // Prefix-bounded: seek backward from the prefix successor (the exclusive
            // upper bound of the prefix range), correctly truncated so a key just
            // past the prefix cannot shadow valid prefix keys.
            resolve_bound = Some(succ);
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
}

impl DBIterator {
    /// Advance to the next visible entry, returning LazyValue without
    /// materializing it. Avoids the into_vec() copy that Iterator::next()
    /// must perform.
    #[inline(always)]
    pub(crate) fn next_lazy(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        if self.ensure_current() {
            let entry = self.current.take().unwrap();
            self.needs_advance = true;
            Some(entry)
        } else {
            None
        }
    }
}

impl Iterator for DBIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_lazy().map(|(k, lv)| (k, lv.into_vec()))
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
    fn test_db_iterator_deeper_level_tombstone_does_not_cover_key() {
        // Regression test: a tombstone may only cover keys from its own
        // level or shallower — NEVER from a level strictly deeper than
        // itself (cross-level tombstone pruning, see iterator.md). This
        // guards the sequence-zeroing interaction: once a bottommost-level
        // key's sequence is zeroed by compaction (a legitimate optimization
        // for keys with no live snapshot dependency), a naive seq-only
        // tombstone comparison would treat ANY still-live tombstone as
        // covering it, including one that has already been compacted deeper
        // than the key and therefore cannot represent a legitimate
        // later-in-time delete of it. The per-source level tag is what
        // still correctly excludes such a tombstone in that case.
        let key_source = sort_lex(vec![make_entry(
            b"key5",
            0,
            ValueType::Value,
            b"still_alive",
        )]);
        let mut iter =
            DBIterator::from_sources(vec![IterSource::new(key_source).with_level(1)], 100);

        // Tombstone ["a", "z") at level 2 — strictly DEEPER than the key's
        // level 1. It must NOT cover the key despite having a much higher
        // sequence number than the key's (zeroed) sequence.
        iter.set_range_tombstones_with_levels(vec![(b"a".to_vec(), b"z".to_vec(), 50, 2)]);

        let entries: Vec<_> = iter.collect();
        assert_eq!(
            entries,
            vec![(b"key5".to_vec(), b"still_alive".to_vec())],
            "deeper-level tombstone incorrectly hid a live key with a lower (zeroed) sequence number"
        );
    }

    #[test]
    fn test_db_iterator_shallower_or_same_level_tombstone_covers_key() {
        // Complements the test above: a tombstone from the SAME level, or
        // from a level STRICTLY SHALLOWER than the key's source level, must
        // still be allowed to cover it — this is ordinary, essential range-
        // delete behavior (e.g. a DeleteRange applied in the active
        // MemTable must immediately hide matching keys already sitting in
        // deeper, already-flushed/compacted levels, and a DeleteRange must
        // also cover a Put in the very same MemTable). The invariant only
        // excludes STRICTLY deeper tombstones (see test above).
        for tombstone_level in [0usize, 1, 2] {
            let key_source = sort_lex(vec![make_entry(b"key5", 0, ValueType::Value, b"stale")]);
            let mut iter =
                DBIterator::from_sources(vec![IterSource::new(key_source).with_level(2)], 100);
            iter.set_range_tombstones_with_levels(vec![(
                b"a".to_vec(),
                b"z".to_vec(),
                50,
                tombstone_level,
            )]);
            let entries: Vec<_> = iter.collect();
            assert!(
                entries.is_empty(),
                "level-{} tombstone should still cover a level-2 key, got {:?}",
                tombstone_level,
                entries
            );
        }
    }

    #[test]
    fn test_db_iterator_prev_deeper_level_tombstone_does_not_cover_key() {
        // Backward-direction counterpart to
        // test_db_iterator_deeper_level_tombstone_does_not_cover_key: the
        // same cross-level tombstone pruning must hold when the key is
        // reached via seek_to_last()/prev() (resolve_prev_user_key's
        // collection loop), not just forward next_visible(). Before the fix,
        // resolve_prev_user_key always called is_range_deleted_no_level_filter
        // (level_filter=None), so this deeper tombstone would have incorrectly
        // hidden the key during backward traversal despite being correctly
        // excluded during forward traversal.
        let key_source = sort_lex(vec![make_entry(
            b"key5",
            0,
            ValueType::Value,
            b"still_alive",
        )]);
        let mut iter =
            DBIterator::from_sources(vec![IterSource::new(key_source).with_level(1)], 100);

        // Tombstone ["a", "z") at level 2 — strictly DEEPER than the key's
        // level 1. It must NOT cover the key despite having a much higher
        // sequence number than the key's (zeroed) sequence.
        iter.set_range_tombstones_with_levels(vec![(b"a".to_vec(), b"z".to_vec(), 50, 2)]);

        iter.seek_to_last();
        assert!(
            iter.valid(),
            "deeper-level tombstone incorrectly hid a live key with a lower (zeroed) \
             sequence number during backward traversal"
        );
        assert_eq!(iter.key().unwrap(), b"key5");
        assert_eq!(iter.value().unwrap(), b"still_alive");

        // No earlier key exists — prev() must report exhaustion, not resurrect
        // the tombstone-adjacent key incorrectly.
        iter.prev();
        assert!(!iter.valid());
    }

    #[test]
    fn test_db_iterator_prev_shallower_or_same_level_tombstone_covers_key() {
        // Backward-direction counterpart to
        // test_db_iterator_shallower_or_same_level_tombstone_covers_key:
        // same-level and shallower-level tombstones must still cover the key
        // when reached via seek_to_last()/resolve_prev_user_key.
        for tombstone_level in [0usize, 1, 2] {
            let key_source = sort_lex(vec![make_entry(b"key5", 0, ValueType::Value, b"stale")]);
            let mut iter =
                DBIterator::from_sources(vec![IterSource::new(key_source).with_level(2)], 100);
            iter.set_range_tombstones_with_levels(vec![(
                b"a".to_vec(),
                b"z".to_vec(),
                50,
                tombstone_level,
            )]);
            iter.seek_to_last();
            assert!(
                !iter.valid(),
                "level-{} tombstone should still cover a level-2 key during backward \
                 traversal, but iterator landed on a key",
                tombstone_level,
            );
        }
    }

    #[test]
    fn test_db_iterator_prev_overshoot_preserves_level_across_calls() {
        // Regression test for the prev_overshoot level-tracking fix: when
        // resolve_prev_user_key overshoots into the next (earlier) user key
        // while draining the current one, the overshot entry's level must be
        // captured and carried across to the NEXT resolve_prev_user_key
        // invocation (via prev_overshoot), not just within a single call.
        //
        // "z" (level 0) is the last key and is unrelated to the tombstone.
        // "key5" (level 1, seq 0) sits inside the tombstone's range and must
        // survive a level-2 (strictly deeper) tombstone. Walking backward via
        // seek_to_last() + prev() forces "key5" to be produced through the
        // prev_overshoot path captured while draining "z"'s group.
        let key_source = sort_lex(vec![
            make_entry(b"z", 20, ValueType::Value, b"unrelated_last"),
            make_entry(b"key5", 0, ValueType::Value, b"still_alive"),
        ]);
        let mut iter =
            DBIterator::from_sources(vec![IterSource::new(key_source).with_level(1)], 100);
        iter.set_range_tombstones_with_levels(vec![(b"a".to_vec(), b"z".to_vec(), 50, 2)]);

        iter.seek_to_last();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"z");
        assert_eq!(iter.value().unwrap(), b"unrelated_last");

        iter.prev();
        assert!(
            iter.valid(),
            "deeper-level tombstone incorrectly hid a live key carried via prev_overshoot"
        );
        assert_eq!(iter.key().unwrap(), b"key5");
        assert_eq!(iter.value().unwrap(), b"still_alive");

        iter.prev();
        assert!(!iter.valid());
    }

    /// Pins heap-backed backward iteration: a shallower range tombstone must
    /// prune deeper source keys, while a deeper tombstone must not hide keys
    /// from a shallower source in the same reverse scan.
    #[test]
    fn test_db_iterator_prev_multi_source_cross_level_tombstone_pruning() {
        let l0 = sort_lex(vec![
            make_entry(b"a", 10, ValueType::Value, b"l0_a"),
            make_entry(b"e", 10, ValueType::Value, b"l0_e"),
            make_entry(b"g", 10, ValueType::Value, b"l0_g"),
            make_entry(b"i", 10, ValueType::Value, b"l0_i"),
        ]);
        let l2 = sort_lex(vec![
            make_entry(b"b", 10, ValueType::Value, b"l2_b"),
            make_entry(b"c", 10, ValueType::Value, b"l2_c"),
            make_entry(b"d", 10, ValueType::Value, b"l2_d"),
            make_entry(b"f", 10, ValueType::Value, b"l2_f"),
            make_entry(b"h", 10, ValueType::Value, b"l2_h"),
        ]);
        let mut iter = DBIterator::from_sources(
            vec![
                IterSource::new(l0).with_level(0),
                IterSource::new(l2).with_level(2),
            ],
            100,
        );
        iter.set_range_tombstones_with_levels(vec![
            (b"b".to_vec(), b"e".to_vec(), 50, 0),
            (b"e".to_vec(), b"i".to_vec(), 50, 2),
        ]);

        iter.seek_to_last();
        let mut seen = Vec::new();
        while iter.valid() {
            seen.push((iter.key().unwrap().to_vec(), iter.value().unwrap().to_vec()));
            iter.prev();
        }

        assert_eq!(
            seen,
            vec![
                (b"i".to_vec(), b"l0_i".to_vec()),
                (b"g".to_vec(), b"l0_g".to_vec()),
                (b"e".to_vec(), b"l0_e".to_vec()),
                (b"a".to_vec(), b"l0_a".to_vec()),
            ],
            "heap-backed prev must attribute each popped entry to its real source level"
        );
    }

    /// Pins a forward-to-backward direction switch with multiple heap sources:
    /// after the heap is rebuilt for `prev()`, the candidate key's source level
    /// must still prevent a deeper tombstone from hiding a shallower entry.
    #[test]
    fn test_db_iterator_prev_after_forward_switch_preserves_heap_source_level() {
        let l0 = sort_lex(vec![make_entry(b"a", 10, ValueType::Value, b"l0_a")]);
        let l2 = sort_lex(vec![
            make_entry(b"b", 10, ValueType::Value, b"l2_b"),
            make_entry(b"c", 10, ValueType::Value, b"l2_c"),
        ]);
        let mut iter = DBIterator::from_sources(
            vec![
                IterSource::new(l0).with_level(0),
                IterSource::new(l2).with_level(2),
            ],
            100,
        );
        iter.set_range_tombstones_with_levels(vec![(b"a".to_vec(), b"b".to_vec(), 50, 2)]);

        iter.seek_to_first();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"a");
        iter.advance();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"b");

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"a");
        assert_eq!(iter.value().unwrap(), b"l0_a");
    }

    /// Pins half-open range tombstone boundaries during heap-backed backward
    /// iteration: `seek_for_prev(end)` must keep `end`, then `prev()` skips
    /// covered keys at and after `start` and lands before the range.
    #[test]
    fn test_db_iterator_seek_for_prev_multi_source_range_tombstone_end_exclusive() {
        let l0 = sort_lex(vec![make_entry(b"z", 10, ValueType::Value, b"l0_z")]);
        let l2 = sort_lex(vec![
            make_entry(b"a", 10, ValueType::Value, b"l2_a"),
            make_entry(b"b", 10, ValueType::Value, b"l2_b"),
            make_entry(b"c", 10, ValueType::Value, b"l2_c"),
            make_entry(b"d", 10, ValueType::Value, b"l2_d"),
        ]);
        let mut iter = DBIterator::from_sources(
            vec![
                IterSource::new(l0).with_level(0),
                IterSource::new(l2).with_level(2),
            ],
            100,
        );
        iter.set_range_tombstones_with_levels(vec![(b"b".to_vec(), b"d".to_vec(), 50, 0)]);

        iter.seek_for_prev(b"d");
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"d");
        assert_eq!(iter.value().unwrap(), b"l2_d");

        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"a");
        assert_eq!(iter.value().unwrap(), b"l2_a");
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
    fn test_prefix_iterator_forward_reposition_clamps_to_prefix() {
        let source = sort_lex(vec![
            make_entry(b"aaa", 1, ValueType::Value, b"before"),
            make_entry(b"foo1", 2, ValueType::Value, b"one"),
            make_entry(b"foo2", 3, ValueType::Value, b"two"),
            make_entry(b"zzz", 4, ValueType::Value, b"after"),
        ]);

        let mut iter = DBIterator::from_sources_with_prefix(
            vec![IterSource::new(source)],
            10,
            b"foo".to_vec(),
        );

        iter.seek_to_first();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"foo1");

        iter.seek(b"a");
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"foo1");

        iter.seek(b"foo2");
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"foo2");

        iter.seek(b"g");
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

//! DBIterator: a user-facing iterator that merges all sources, resolves
//! sequence numbers, and skips tombstones.

use std::{cmp::Ordering, sync::Arc};

use crate::iterator::{
    merge::{IterSource, MergingIterator},
    range_del::FragmentedRangeTombstoneList,
};
use crate::types::{
    LazyValue, MAX_SEQUENCE_NUMBER, SequenceNumber, ValueType, compare_internal_key,
    decode_internal_key,
};
use crate::{Error, options::IterConfig};

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
    /// Source zero contains the uncommitted batch when an overlay is present.
    has_batch_overlay: bool,
    /// Reusable buffer for last user key (for deduplication).
    last_user_key: Vec<u8>,
    /// Whether last_user_key has been set at least once.
    has_last_key: bool,
    /// Buffered current entry for valid()/key()/value() API.
    current: Option<(Vec<u8>, LazyValue)>,
    /// Recycled user-key buffer swapped with source `peeked_key` on each step.
    scratch_key: Vec<u8>,
    /// Recycled `LazyValue` wrapper updated in place across consecutive steps.
    scratch_val: LazyValue,
    /// Whether we've already consumed current via advance().
    needs_advance: bool,
    /// Pre-fragmented, immutable range tombstone index for O(log T) coverage
    /// checks in any direction. Loaded upfront at creation time from all
    /// sources' cached tombstones — no inline collection or full-scan preload.
    range_tombstones: FragmentedRangeTombstoneList,
    config: Arc<IterConfig>,
    /// Overshoot buffer for backward iteration: when collecting entries for one
    /// user key, we may read the first entry of the *previous* user key. This
    /// field retains the entry and its source for visibility and level checks.
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
    /// Semantic corruption while decoding an internal key (short trailer /
    /// unknown type). Surfaced via [`Self::error`] so callers do not treat it
    /// as normal exhaustion.
    key_decode_error: Option<Error>,
    /// Set when `next_prefix` jumps past a prefix that has no lexicographic
    /// successor (all `0xFF`, or `prefix_len == 0`). Keeps later `advance()`
    /// and `next_prefix()` calls exhausted until an explicit seek repositions.
    prefix_exhausted: bool,
}

fn ikey_compare(a: &[u8], b: &[u8]) -> Ordering {
    compare_internal_key(a, b)
}

impl DBIterator {
    /// Build a DB iterator from multiple sorted Vec sources of internal key-value pairs.
    #[cfg(test)]
    pub(crate) fn new(sources: Vec<Vec<(Vec<u8>, Vec<u8>)>>, sequence: SequenceNumber) -> Self {
        Self::from_sources(sources.into_iter().map(IterSource::new).collect(), sequence)
    }

    /// Build a DB iterator from pre-built IterSource objects (supports streaming).
    pub(crate) fn from_sources(sources: Vec<IterSource>, sequence: SequenceNumber) -> Self {
        let merger = MergingIterator::new(sources, ikey_compare as fn(&[u8], &[u8]) -> Ordering);

        Self {
            merger,
            sequence,
            has_batch_overlay: false,
            last_user_key: Vec::new(),
            has_last_key: false,
            current: None,
            scratch_key: Vec::new(),
            scratch_val: LazyValue::empty(),
            needs_advance: true,
            range_tombstones: FragmentedRangeTombstoneList::empty(),
            config: IterConfig::empty(),
            prev_overshoot: None,
            backward_positioned: false,
            skip_point: None,
            last_seek_key: None,
            clean_read: true,
            key_decode_error: None,
            prefix_exhausted: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_sources_with_prefix(
        sources: Vec<IterSource>,
        sequence: SequenceNumber,
        prefix: Vec<u8>,
    ) -> Self {
        let upper = crate::types::prefix_successor(&prefix);
        let mut iter = Self::from_sources(sources, sequence);
        iter.set_config(Arc::new(IterConfig {
            lower: Some(prefix),
            upper,
            ..Default::default()
        }));
        iter
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

    pub(crate) fn include_batch_source(&mut self) {
        self.has_batch_overlay = true;
    }

    /// Snapshot bound to use for range-tombstone visibility. The tombstone
    /// list is frozen at iterator creation: it contains only tombstones at or
    /// below the snapshot sequence, plus (for batch iterators) overlay
    /// tombstones in the reserved region — there is nothing in between. With
    /// a batch overlay present, every collected tombstone must be visible, so
    /// the bound is lifted to MAX.
    #[inline]
    fn tombstone_snapshot(&self) -> SequenceNumber {
        if self.has_batch_overlay {
            MAX_SEQUENCE_NUMBER
        } else {
            self.sequence
        }
    }

    fn effective_forward_target(&self, target: &[u8]) -> Vec<u8> {
        let mut effective = target;
        if let Some(lb) = self.config.lower.as_deref()
            && effective < lb
        {
            effective = lb;
        }
        effective.to_vec()
    }

    /// Tighten the inclusive lower bound, retaining the existing upper bound.
    pub(crate) fn set_lower_bound(&mut self, bound: Vec<u8>) {
        let mut config = (*self.config).clone();
        config.lower = Some(match config.lower.take() {
            Some(old) => old.max(bound),
            None => bound,
        });
        self.set_config(Arc::new(config));
    }

    pub(crate) fn set_upper_bound(&mut self, bound: Vec<u8>) {
        let mut config = (*self.config).clone();
        config.upper = Some(bound);
        self.set_config(Arc::new(config));
    }

    pub(crate) fn set_config(&mut self, config: Arc<IterConfig>) {
        self.merger.set_config(config.clone());
        if !config.filters.is_empty() {
            self.clean_read = false;
        }
        self.config = config;
        if self.has_failed() {
            self.halt_on_error();
            return;
        }
        self.recycle_current();
        self.needs_advance = true;
        self.prev_overshoot = None;
        self.prefix_exhausted = false;
    }

    #[inline]
    fn recycle_current(&mut self) {
        if let Some((mut k, v)) = self.current.take() {
            k.clear();
            if k.capacity() > self.scratch_key.capacity() {
                self.scratch_key = k;
            }
            self.scratch_val = v;
        }
    }

    #[inline]
    fn has_failed(&self) -> bool {
        self.key_decode_error.is_some() || self.merger.has_failed()
    }

    #[inline]
    fn halt_on_error(&mut self) {
        self.recycle_current();
        self.needs_advance = false;
        self.prev_overshoot = None;
        self.backward_positioned = false;
    }

    /// Return the first error from any underlying source iterator, or a
    /// key-decode corruption observed while filtering entries.
    /// A source read error ends iteration, and the error stays reported for
    /// the iterator's lifetime; later seeks do not resume it.
    /// Use after iteration returns `None` to distinguish normal exhaustion
    /// from I/O or semantic corruption failures.
    pub fn error(&self) -> Option<&Error> {
        self.key_decode_error
            .as_ref()
            .or_else(|| self.merger.error())
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
        if !self.ensure_current() {
            return;
        }
        if self.has_failed() {
            self.halt_on_error();
            return;
        }
        let Some((ref key, _)) = self.current else {
            return;
        };
        // `current` already holds a user key (internal key was truncated
        // in next_visible), so do NOT call user_key() again.
        if key.len() < prefix_len {
            self.advance();
            return;
        }
        let mut succ = key[..prefix_len].to_vec();
        // Increment: find rightmost byte < 0xFF and increment it.
        let mut i = succ.len();
        while i > 0 {
            i -= 1;
            if succ[i] < 0xFF {
                succ[i] += 1;
                succ.truncate(i + 1);
                let target = self.effective_forward_target(&succ);
                // Use seek_opt with try_next=true since target > current.
                use crate::types::InternalKey;
                let seek_key =
                    InternalKey::new(&target, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion);
                self.merger.seek_opt(seek_key.as_bytes(), true);
                self.last_seek_key = Some(target);
                self.has_last_key = false;
                self.needs_advance = true;
                self.recycle_current();
                self.prev_overshoot = None;
                self.backward_positioned = false;
                return;
            }
        }
        // All `prefix_len` bytes were 0xFF (or `prefix_len == 0`) — no next
        // prefix exists, so exhaust forward iteration until an explicit seek.
        self.prefix_exhausted = true;
        self.recycle_current();
        self.needs_advance = false;
        self.prev_overshoot = None;
        self.backward_positioned = false;
    }

    /// Advance the streaming iterator to the next visible `(user_key, value)` pair,
    /// populating `self.scratch_key` and `self.scratch_val` in place (`0` steady-state
    /// heap allocations for both skipped and returned rows).
    fn next_visible_into(&mut self) -> bool {
        if self.has_failed() {
            self.halt_on_error();
            return false;
        }
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
        // Decisions extracted from peek_key() to avoid borrow conflicts.
        // peek_key() borrows self.merger; we need self.is_range_deleted() etc.
        enum Action {
            Skip,
            Take {
                uk_len: usize,
            },
            /// Check the source level after dropping the borrowed key.
            TakeCheckTombstone {
                uk_len: usize,
                seq: SequenceNumber,
                source: usize,
            },
        }
        // Copy visibility parameters out of self: peek_key() holds a
        // mutable borrow of self.merger across the checks below.
        let snapshot = self.sequence;
        let has_batch_overlay = self.has_batch_overlay;
        loop {
            let action = {
                let Some((ikey_ref, source)) = self.merger.peek_key() else {
                    return false;
                };
                match decode_internal_key(ikey_ref) {
                    Err(e) => {
                        self.key_decode_error = Some(e);
                        self.halt_on_error();
                        return false;
                    }
                    Ok((_, seq, vt)) => {
                        if !(seq <= snapshot || (has_batch_overlay && source == 0)) {
                            Action::Skip
                        } else {
                            let uk_len = ikey_ref.len() - 8;

                            // Lower bound check
                            if let Some(ref lb) = self.config.lower
                                && ikey_ref[..uk_len] < **lb
                            {
                                Action::Skip
                            }
                            // Upper bound check
                            else if let Some(ref ub) = self.config.upper
                                && ikey_ref[..uk_len] >= **ub
                            {
                                return false;
                            } else if vt == ValueType::RangeDeletion {
                                // Skip RangeDeletion entries — tombstones are pre-loaded.
                                // Do NOT update last_user_key: RangeDeletion is not a point
                                // mutation for this user key. Updating it would suppress a
                                // same-key Value via-dedup instead of the proper range
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
                                    Action::TakeCheckTombstone {
                                        uk_len,
                                        seq,
                                        source,
                                    }
                                }
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
                Action::TakeCheckTombstone {
                    uk_len,
                    seq,
                    source,
                } => {
                    let source_level = self.merger.source_level(source);
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
                    if !self
                        .merger
                        .take_entry_into(&mut self.scratch_key, &mut self.scratch_val)
                    {
                        return false;
                    }
                    self.scratch_key.truncate(uk_len);
                    if self
                        .skip_point
                        .as_ref()
                        .is_some_and(|sp| sp(&self.scratch_key))
                        || !self.config.matches(&self.scratch_key)
                    {
                        continue;
                    }
                    return true;
                }
                Action::Take { uk_len } => {
                    if !self
                        .merger
                        .take_entry_into(&mut self.scratch_key, &mut self.scratch_val)
                    {
                        return false;
                    }
                    self.scratch_key.truncate(uk_len);
                    if self
                        .skip_point
                        .as_ref()
                        .is_some_and(|sp| sp(&self.scratch_key))
                        || !self.config.matches(&self.scratch_key)
                    {
                        continue;
                    }
                    return true;
                }
            }
        }
    }

    /// Streamlined next_visible for the common case: no range tombstones,
    /// no skip_point callback. Populates `self.scratch_key` and `self.scratch_val`
    /// with zero steady-state heap allocations.
    #[inline(always)]
    fn next_visible_clean_into(&mut self) -> bool {
        if self.has_failed() {
            self.halt_on_error();
            return false;
        }
        if self.backward_positioned {
            self.backward_positioned = false;
            self.prev_overshoot = None;
            if self.has_last_key {
                use crate::types::InternalKey;
                let seek_key = InternalKey::new(&self.last_user_key, 0, ValueType::Deletion);
                self.merger.seek(seek_key.as_bytes());
            }
        }
        // Copy visibility parameters out of self: peek_key() holds a
        // mutable borrow of self.merger across the checks below.
        let snapshot = self.sequence;
        let has_batch_overlay = self.has_batch_overlay;
        loop {
            let Some((ikey_ref, source)) = self.merger.peek_key() else {
                return false;
            };
            let (seq, vt) = match decode_internal_key(ikey_ref) {
                Err(e) => {
                    self.key_decode_error = Some(e);
                    self.halt_on_error();
                    return false;
                }
                Ok((_, seq, vt)) => (seq, vt),
            };
            let uk_len = ikey_ref.len() - 8;

            if !(seq <= snapshot || (has_batch_overlay && source == 0)) {
                self.merger.advance_entry();
                continue;
            }

            // Lower bound
            if let Some(ref lb) = self.config.lower
                && ikey_ref[..uk_len] < **lb
            {
                self.merger.advance_entry();
                continue;
            }

            // Upper bound
            if let Some(ref ub) = self.config.upper
                && ikey_ref[..uk_len] >= **ub
            {
                return false;
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

            if !self
                .merger
                .take_entry_into(&mut self.scratch_key, &mut self.scratch_val)
            {
                return false;
            }
            self.scratch_key.truncate(uk_len);
            return true;
        }
    }

    /// Ensure current is populated. Returns whether there's a valid entry.
    fn ensure_current(&mut self) -> bool {
        // Keep an already-resolved, unconsumed entry stable across repeated
        // valid()/key()/value() calls even if take_entry_into()'s lookahead into the
        // next block latched a source read error; the failure halts the
        // iterator once the caller advances past this entry.
        if !self.needs_advance {
            return self.current.is_some();
        }
        if self.has_failed() {
            self.halt_on_error();
            return false;
        }
        if self.prefix_exhausted {
            return false;
        }
        self.recycle_current();
        let found = if self.clean_read {
            self.next_visible_clean_into()
        } else {
            self.next_visible_into()
        };
        if found {
            let k = std::mem::take(&mut self.scratch_key);
            let v = std::mem::replace(&mut self.scratch_val, LazyValue::empty());
            self.current = Some((k, v));
        }
        self.needs_advance = false;
        self.current.is_some()
    }

    /// Consume and borrow the next visible row. A source error encountered
    /// during lookahead takes precedence over the candidate row.
    pub fn step_next(&mut self) -> crate::Result<Option<(&[u8], &[u8])>> {
        self.lend_next(true)
    }

    pub(super) fn lend_next(&mut self, checked: bool) -> crate::Result<Option<(&[u8], &[u8])>> {
        self.ensure_current();
        self.lend_current(checked)
    }

    fn lend_current(&mut self, checked: bool) -> crate::Result<Option<(&[u8], &[u8])>> {
        if checked && let Some(error) = self.error().cloned() {
            self.halt_on_error();
            return Err(error);
        }
        self.needs_advance = self.current.is_some();
        Ok(self
            .current
            .as_ref()
            .map(|(key, value)| (key.as_slice(), value.as_slice())))
    }

    /// Borrow the previous visible row. On a new cursor the first reverse
    /// step starts at the end. After a seek it consumes the positioned row.
    pub fn step_prev(&mut self) -> crate::Result<Option<(&[u8], &[u8])>> {
        if self.needs_advance {
            if let Some((key, _)) = self.current.take() {
                self.resolve_prev_user_key(Some(&key));
            } else if self.has_last_key {
                let key = self.last_user_key.clone();
                self.resolve_prev_user_key(Some(&key));
            } else if self.last_seek_key.is_some() {
                self.ensure_current();
            } else {
                self.seek_to_last();
            }
        }
        self.lend_current(true)
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
        self.next_lazy();
    }

    /// Seek to the first key >= target.
    pub fn seek(&mut self, target: &[u8]) {
        use crate::types::InternalKey;
        if self.has_failed() {
            self.halt_on_error();
            return;
        }
        self.prefix_exhausted = false;
        let target = self.effective_forward_target(target);
        // Seek the merger to a synthetic internal key with max sequence
        let seek_key = InternalKey::new(&target, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion);
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
        self.recycle_current();
        self.prev_overshoot = None;
        self.backward_positioned = false;
    }

    /// Position sources for their first forward read without making a public
    /// seek. An initial reverse step may still choose the end of the window.
    pub(crate) fn prime_lower_bound(&mut self) {
        if self.config.lower.is_some() {
            self.seek_to_first();
            self.last_seek_key = None;
        }
    }

    pub fn seek_to_first(&mut self) {
        if self.has_failed() {
            self.halt_on_error();
            return;
        }
        self.prefix_exhausted = false;
        if self.config.lower.is_some() {
            self.seek(b"");
            return;
        }
        self.merger.seek_to_first();
        self.has_last_key = false;
        self.needs_advance = true;
        self.recycle_current();
        self.prev_overshoot = None;
        self.last_seek_key = Some(Vec::new());
        self.backward_positioned = false;
    }

    /// Seek to the last visible user key <= target.
    ///
    /// Uses a single backward seek + inline resolution. No redundant forward seek.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        use crate::types::InternalKey;
        if self.has_failed() {
            self.halt_on_error();
            return;
        }
        self.prefix_exhausted = false;

        let upper_clamps_target = self.config.upper.as_deref().is_some_and(|ub| ub <= target);
        let (seek_key, bound) = if upper_clamps_target {
            let ub = self.config.upper.as_deref().unwrap();
            (
                InternalKey::new(ub, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion),
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
        if self.has_failed() {
            self.halt_on_error();
            return;
        }
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
            // tombstone pruning).
            let mut best_level: usize = usize::MAX;

            // First, consume any overshoot entry saved from a previous backward walk
            let first_entry = self
                .prev_overshoot
                .take()
                .or_else(|| self.merger.prev_entry());

            let mut iter_entry = first_entry;

            while let Some((ikey, value, source)) = iter_entry.take() {
                let (uk_len, seq, vt) = match decode_internal_key(&ikey) {
                    Err(e) => {
                        self.key_decode_error = Some(e);
                        self.halt_on_error();
                        return;
                    }
                    Ok((uk, seq, vt)) => (uk.len(), seq, vt),
                };
                let uk = &ikey[..uk_len];

                // Skip entries >= current_bound (same or later user key) before
                // checking prefix/lower bounds so boundary keys at or above
                // prefix_successor never prematurely terminate the walk.
                if let Some(ref bound) = current_bound
                    && uk >= bound.as_slice()
                {
                    iter_entry = self.merger.prev_entry();
                    continue;
                }

                // Lower bound guard
                if let Some(ref lb) = self.config.lower
                    && uk < lb.as_slice()
                {
                    break;
                }

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
                            self.prev_overshoot = Some((ikey, value, source));
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
                if (seq <= self.sequence || (self.has_batch_overlay && source == 0))
                    && seq >= best_seq
                {
                    best_seq = seq;
                    best_level = self.merger.source_level(source);
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

            // A failed source may hold newer versions of the candidate that
            // were never read, so the best entry seen is not the visible one.
            if self.merger.has_failed() {
                self.halt_on_error();
                return;
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
                            if self.skip_point.as_ref().is_some_and(|sp| sp(&uk))
                                || !self.config.matches(&uk)
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
        if self.has_failed() {
            self.halt_on_error();
            return;
        }
        self.prefix_exhausted = false;

        // Determine the effective upper bound for backward seek.
        let resolve_bound = self.config.upper.clone();
        if let Some(ref ub) = resolve_bound {
            // Upper-bound constrained: seek backward from upper bound.
            let seek_key = InternalKey::new(ub, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion);
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

    /// Build a one-entry-per-block SST from sorted `entries` and flip the
    /// stored CRC byte of the block holding `entries[corrupt]`.
    fn sst_with_corrupt_block(
        path: &std::path::Path,
        entries: &[(Vec<u8>, Vec<u8>)],
        corrupt: usize,
    ) -> crate::manifest::version::TableFile {
        use std::io::{Read, Seek, SeekFrom, Write};

        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::sst::table_reader::TableReader;

        let mut builder = TableBuilder::new(
            path,
            TableBuildOptions {
                block_size: 1,
                bloom_bits_per_key: 0,
                ..Default::default()
            },
        )
        .unwrap();
        for (k, v) in entries {
            builder.add(k, v).unwrap();
        }
        builder.finish().unwrap();

        let handle = {
            let reader = TableReader::open(path).unwrap();
            let index = reader.cached_index_entries().unwrap();
            assert_eq!(index.len(), entries.len(), "expected one block per entry");
            index[corrupt].handle
        };
        let crc_byte_offset = handle.offset + handle.size + 1;
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        f.seek(SeekFrom::Start(crc_byte_offset)).unwrap();
        let mut byte = [0u8; 1];
        f.read_exact(&mut byte).unwrap();
        f.seek(SeekFrom::Start(crc_byte_offset)).unwrap();
        f.write_all(&[byte[0].wrapping_add(1)]).unwrap();
        drop(f);

        crate::manifest::version::TableFile {
            meta: crate::manifest::version_edit::FileMetaData {
                number: 1,
                file_size: std::fs::metadata(path).unwrap().len(),
                smallest_key: entries[0].0.clone(),
                largest_key: entries[entries.len() - 1].0.clone(),
                has_range_deletions: false,
            },
            reader: std::sync::Arc::new(TableReader::open(path).unwrap()),
        }
    }

    /// Regression: a source whose block read fails must end the merged scan.
    /// Treating it as exhausted let an older value from a deeper level stand
    /// in for the newer tombstone in the unreadable block.
    #[test]
    fn source_read_error_stops_merge_instead_of_exposing_older_version() {
        use crate::sst::table_reader::TableIterator;

        let dir = tempfile::tempdir().unwrap();
        let newer = sst_with_corrupt_block(
            &dir.path().join("000001.sst"),
            &[
                make_entry(b"a", 20, ValueType::Value, b"a"),
                make_entry(b"k", 19, ValueType::Deletion, b""),
            ],
            1,
        );
        let sources = vec![
            IterSource::from_table_iter(TableIterator::new(newer.reader)).with_level(1),
            IterSource::new(vec![make_entry(b"k", 5, ValueType::Value, b"old")]).with_level(2),
        ];

        let mut iter = DBIterator::from_sources(sources, 100);
        let entries: Vec<_> = iter.by_ref().collect();
        assert!(
            !entries.iter().any(|(k, _)| k == b"k"),
            "deleted key surfaced from a deeper level: {entries:?}"
        );
        assert!(iter.error().is_some_and(|e| e.to_string().contains("CRC")));

        let sources = vec![
            IterSource::from_table_iter(TableIterator::new(
                sst_with_corrupt_block(
                    &dir.path().join("000002.sst"),
                    &[
                        make_entry(b"a", 20, ValueType::Value, b"a"),
                        make_entry(b"k", 19, ValueType::Deletion, b""),
                    ],
                    1,
                )
                .reader,
            ))
            .with_level(1),
            IterSource::new(vec![make_entry(b"k", 5, ValueType::Value, b"old")]).with_level(2),
        ];
        let mut iter = DBIterator::from_sources(sources, 100);
        iter.seek(b"k");
        assert!(!iter.valid(), "seek must not land on the older version");
        assert!(iter.error().is_some());
    }

    /// Regression: `LevelIterator` clears its error on every seek, and the
    /// direction switches DBIterator performs internally re-seek every source.
    /// The error must survive them so exhaustion is not reported as success.
    #[test]
    fn source_read_error_survives_internal_reseeks() {
        use crate::iterator::level_iter::LevelIterator;

        let dir = tempfile::tempdir().unwrap();
        let mut entries: Vec<_> = (0..10)
            .map(|i| make_entry(format!("a{i}").as_bytes(), 1 + i, ValueType::Value, b"v"))
            .collect();
        entries.extend(
            (0..10).map(|i| make_entry(format!("b{i}").as_bytes(), 11 + i, ValueType::Value, b"v")),
        );
        let file = sst_with_corrupt_block(&dir.path().join("000001.sst"), &entries, 5);
        let sources = vec![
            IterSource::from_level_iter(LevelIterator::new(vec![file])).with_level(1),
            IterSource::new(vec![make_entry(b"c1", 30, ValueType::Value, b"c")]).with_level(0),
        ];

        let mut iter = DBIterator::from_sources(sources, 100);
        iter.seek(b"c1");
        assert_eq!(iter.key(), Some(&b"c1"[..]));
        // Walk backward into the unreadable block (a5), then forward again:
        // the forward resume re-seeks every source.
        let mut seen = Vec::new();
        iter.prev();
        while iter.valid() {
            seen.push(iter.key().unwrap().to_vec());
            iter.prev();
        }
        iter.advance();
        assert!(!iter.valid(), "iteration resumed after a read error");
        iter.advance();
        assert!(!iter.valid(), "iteration resumed after a read error");
        assert!(
            iter.error().is_some_and(|e| e.to_string().contains("CRC")),
            "read error lost after internal re-seek; yielded {seen:?}"
        );
        assert!(
            !seen.iter().any(|k| k.as_slice() < b"a5".as_slice()),
            "keys past the failed block were yielded: {seen:?}"
        );
    }

    /// Regression: walking backward reads a key's versions oldest first. When
    /// the block with its newer version failed, `prev` still returned the
    /// older value it had already seen.
    #[test]
    fn backward_read_error_does_not_return_older_version() {
        use crate::sst::table_reader::TableIterator;

        let dir = tempfile::tempdir().unwrap();
        let file = sst_with_corrupt_block(
            &dir.path().join("000001.sst"),
            &[
                make_entry(b"k", 20, ValueType::Value, b"new"),
                make_entry(b"k", 5, ValueType::Value, b"old"),
            ],
            0,
        );
        let sources = vec![
            IterSource::new(vec![make_entry(b"a", 30, ValueType::Value, b"a")]).with_level(0),
            IterSource::from_table_iter(TableIterator::new(file.reader)).with_level(1),
        ];

        let mut iter = DBIterator::from_sources(sources, 100);
        iter.seek_to_last();
        assert_ne!(
            iter.value(),
            Some(&b"old"[..]),
            "an older version stood in for the unreadable newer one"
        );
        assert!(!iter.valid());
        assert!(iter.error().is_some_and(|e| e.to_string().contains("CRC")));
    }

    /// Regression: seek-using-next steps a source toward the target. When a
    /// step failed, the source was treated as exhausted and fully re-seeked,
    /// which cleared its error and skipped the unreadable block.
    #[test]
    fn seek_using_next_keeps_step_read_error() {
        use crate::iterator::level_iter::LevelIterator;

        let dir = tempfile::tempdir().unwrap();
        let entries: Vec<_> = (0..10)
            .map(|i| make_entry(format!("a{i}").as_bytes(), 1 + i, ValueType::Value, b"v"))
            .collect();
        let file = sst_with_corrupt_block(&dir.path().join("000001.sst"), &entries, 2);
        let sources = vec![
            IterSource::from_level_iter(LevelIterator::new(vec![file])).with_level(1),
            IterSource::new(vec![make_entry(b"z", 30, ValueType::Value, b"z")]).with_level(0),
        ];

        let mut iter = DBIterator::from_sources(sources, 100);
        iter.seek(b"a0");
        assert_eq!(iter.key(), Some(&b"a0"[..]));
        // Stepping from a1 toward a5 reads the unreadable a2 block.
        iter.seek(b"a5");
        assert!(!iter.valid(), "seek skipped past an unreadable block");
        assert!(iter.error().is_some_and(|e| e.to_string().contains("CRC")));
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

        for reverse_seek in [false, true] {
            if reverse_seek {
                iter.seek_for_prev(b"a");
            } else {
                iter.seek(b"a");
            }
            iter.advance();
            iter.advance();
            assert_eq!(iter.key(), Some(b"c".as_slice()));
            iter.advance();
            assert!(!iter.valid());
        }
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

    #[test]
    fn test_malformed_internal_key_surfaces_error() {
        // Short key must stop iteration and set error().
        let sources = vec![vec![(b"short".to_vec(), b"v".to_vec())]];
        let mut iter = DBIterator::new(sources, 100);
        assert!(iter.next().is_none());
        let err = iter.error().expect("decode error");
        assert!(err.to_string().contains("too short"), "got {err}");

        // Unknown type byte must not be coerced to Deletion (false absence).
        let packed = (5u64 << 8) | 0xEE;
        let mut bad = b"k".to_vec();
        bad.extend_from_slice(&(!packed).to_be_bytes());
        let sources = vec![vec![(bad, b"v".to_vec())]];
        let mut iter = DBIterator::new(sources, 100);
        assert!(iter.next().is_none());
        let err = iter.error().expect("type error");
        assert!(err.to_string().contains("invalid value_type"), "got {err}");
    }

    #[test]
    fn test_next_prefix_handles_lazy_cursor_and_max_prefix_exhaustion() {
        let source = sort_lex(vec![
            make_entry(b"ab", 1, ValueType::Value, b"short"),
            make_entry(b"aaa_1", 2, ValueType::Value, b"a1"),
            make_entry(b"aaa_2", 3, ValueType::Value, b"a2"),
            make_entry(b"bbb_1", 4, ValueType::Value, b"b1"),
            make_entry(b"bbb_2", 5, ValueType::Value, b"b2"),
            make_entry(b"ccc_1", 6, ValueType::Value, b"c1"),
            make_entry(b"\xff\xff_1", 7, ValueType::Value, b"f1"),
            make_entry(b"\xff\xff_2", 8, ValueType::Value, b"f2"),
        ]);

        // Fresh unmaterialized iterator: jumps past aaa_* -> ab, then short key
        // (< prefix_len) advances one step -> bbb_1, and lazy cursor after
        // seek/advance still jumps past the whole prefix.
        let mut iter = DBIterator::new(vec![source], 100);
        iter.next_prefix(4);
        assert_eq!(iter.key(), Some(b"ab".as_slice()));
        iter.next_prefix(4);
        assert_eq!(iter.key(), Some(b"bbb_1".as_slice()));

        // Lazy cursor immediately after seek(): must skip aaa_1/aaa_2 -> ab.
        iter.seek(b"aaa_1");
        iter.next_prefix(4);
        assert_eq!(iter.key(), Some(b"ab".as_slice()));

        // Lazy cursor immediately after advance(): must skip bbb_1/bbb_2 -> ccc_1.
        iter.advance();
        iter.next_prefix(4);
        assert_eq!(iter.key(), Some(b"ccc_1".as_slice()));

        // prefix_len == 0 has no successor prefix and exhausts forward iteration.
        iter.next_prefix(0);
        assert!(!iter.valid());
        iter.advance();
        assert!(!iter.valid());

        // Jumping past an all-0xFF prefix must exhaust and stay exhausted
        // across repeated next_prefix() and advance() calls until re-seeked.
        iter.seek(b"\xff\xff_1");
        assert_eq!(iter.key(), Some(b"\xff\xff_1".as_slice()));
        iter.next_prefix(2);
        assert!(!iter.valid());
        iter.next_prefix(2);
        assert!(!iter.valid());
        iter.advance();
        assert!(!iter.valid());
        assert!(iter.next().is_none());

        // Explicit seek clears prefix exhaustion.
        iter.seek_to_first();
        assert_eq!(iter.key(), Some(b"aaa_1".as_slice()));
    }

    #[test]
    fn test_key_decode_error_latches_across_seeks_and_direction_switches() {
        use crate::iterator::bidi_iter::BidiIterator;

        // 1. A short damaged key in a newer source must latch DBIterator shut
        //    so a subsequent seek(b"k") cannot expose a shadowed older value.
        let s_newer = vec![(b"k".to_vec(), b"".to_vec())];
        let s_older = vec![make_entry(b"k", 5, ValueType::Value, b"old")];
        let mut iter = DBIterator::new(vec![s_newer, s_older], 100);
        assert!(iter.next().is_none());
        assert!(iter.error().is_some());
        iter.seek(b"k");
        assert!(!iter.valid(), "seek must not resume after key_decode_error");
        assert!(iter.next().is_none());
        assert!(iter.error().is_some());

        // 2. A backward decode error must prevent advance() and BidiIterator::next()
        //    from resuming forward iteration on surrounding entries.
        let packed = (7u64 << 8) | 0xEE;
        let mut bad_m = b"m".to_vec();
        bad_m.extend_from_slice(&(!packed).to_be_bytes());
        let entries = vec![
            make_entry(b"a", 10, ValueType::Value, b"va"),
            (bad_m, b"vm".to_vec()),
            make_entry(b"z", 5, ValueType::Value, b"vz"),
        ];

        let mut db_iter = DBIterator::new(vec![entries.clone()], 100);
        db_iter.seek_to_last();
        assert!(!db_iter.valid());
        assert!(db_iter.error().is_some());
        db_iter.advance();
        assert!(!db_iter.valid());
        db_iter.seek_to_first();
        assert!(!db_iter.valid());

        let mut bidi = BidiIterator::lazy(DBIterator::new(vec![entries], 100));
        assert!(bidi.next_back().is_none());
        assert!(bidi.error().is_some());
        assert!(
            bidi.next().is_none(),
            "BidiIterator::next must not resume after next_back hit key_decode_error"
        );
        assert!(bidi.error().is_some());
    }

    #[test]
    fn lending_lookahead_returns_typed_error_instead_of_candidate() {
        use crate::{BidiIterator, ErrorKind, sst::table_reader::TableIterator};
        let dir = tempfile::tempdir().unwrap();
        let file = sst_with_corrupt_block(
            &dir.path().join("000001.sst"),
            &[
                make_entry(b"a", 2, ValueType::Value, b"va"),
                make_entry(b"b", 1, ValueType::Value, b"vb"),
            ],
            1,
        );
        let cursor = || {
            DBIterator::from_sources(
                vec![IterSource::from_table_iter(TableIterator::new(
                    file.reader.clone(),
                ))],
                2,
            )
        };
        let mut db = cursor();
        assert_eq!(db.step_next().unwrap_err().kind(), ErrorKind::Corruption);
        assert!(!db.valid());
        assert_eq!(db.step_prev().unwrap_err().kind(), ErrorKind::Corruption);
        let mut bidi = BidiIterator::lazy(cursor());
        assert_eq!(bidi.step_next().unwrap_err().kind(), ErrorKind::Corruption);
        assert_eq!(bidi.step_prev().unwrap_err().kind(), ErrorKind::Corruption);
    }

    #[test]
    fn cursor_api_preserves_resolved_entry_when_readahead_hits_corrupt_next_block() {
        use crate::iterator::bidi_iter::BidiIterator;
        use crate::sst::table_reader::TableIterator;

        let dir = tempfile::tempdir().unwrap();
        let file = sst_with_corrupt_block(
            &dir.path().join("000001.sst"),
            &[
                make_entry(b"a", 20, ValueType::Value, b"va"),
                make_entry(b"b", 19, ValueType::Value, b"vb"),
            ],
            1, // block 0 ("a") is valid; block 1 ("b") has a corrupt CRC
        );

        // 1. valid() -> key() -> value() -> advance() must match Iterator::next()
        let mut iter = DBIterator::from_sources(
            vec![
                IterSource::new(vec![]).with_level(0),
                IterSource::from_table_iter(TableIterator::new(file.reader.clone())).with_level(0),
            ],
            100,
        );
        assert!(iter.valid());
        assert_eq!(iter.key(), Some(&b"a"[..]));
        assert_eq!(iter.value(), Some(&b"va"[..]));
        iter.advance();
        assert!(!iter.valid());
        assert!(iter.error().is_some_and(|e| e.to_string().contains("CRC")));

        // 2. BidiIterator::lazy after valid() must still yield the unconsumed front entry
        let mut iter2 = DBIterator::from_sources(
            vec![
                IterSource::from_table_iter(TableIterator::new(file.reader.clone())).with_level(0),
            ],
            100,
        );
        assert!(iter2.valid());
        let mut bidi = BidiIterator::lazy(iter2);
        assert_eq!(bidi.next(), Some((b"a".to_vec(), b"va".to_vec())));
        assert_eq!(bidi.next(), None);
        assert!(bidi.error().is_some_and(|e| e.to_string().contains("CRC")));

        // 3. next_prefix() after valid() must halt instead of clearing the failure
        let mut iter3 = DBIterator::from_sources(
            vec![IterSource::from_table_iter(TableIterator::new(file.reader)).with_level(0)],
            100,
        );
        assert!(iter3.valid());
        iter3.next_prefix(0);
        assert!(!iter3.valid());
        assert!(iter3.error().is_some_and(|e| e.to_string().contains("CRC")));
    }

    #[test]
    fn prefix_reverse_scan_skips_boundary_entry_at_prefix_successor() {
        use crate::iterator::bidi_iter::BidiIterator;

        for boundary_vt in [ValueType::Value, ValueType::RangeDeletion] {
            let boundary_val: &[u8] = match boundary_vt {
                ValueType::RangeDeletion => b"z",
                _ => b"vb",
            };
            let entries = vec![
                make_entry(b"a1", 10, ValueType::Value, b"v1"),
                make_entry(b"a2", 11, ValueType::Value, b"v2"),
                make_entry(b"b", MAX_SEQUENCE_NUMBER, boundary_vt, boundary_val),
            ];

            // 1. seek_to_last on a prefix-bounded DBIterator
            let mut iter = DBIterator::from_sources_with_prefix(
                vec![IterSource::new(entries.clone()).with_level(0)],
                MAX_SEQUENCE_NUMBER,
                b"a".to_vec(),
            );
            iter.seek_to_last();
            assert_eq!(iter.key(), Some(&b"a2"[..]));
            iter.prev();
            assert_eq!(iter.key(), Some(&b"a1"[..]));
            iter.prev();
            assert!(!iter.valid());

            // 2. seek_for_prev past the prefix successor
            let mut iter2 = DBIterator::from_sources_with_prefix(
                vec![IterSource::new(entries.clone()).with_level(0)],
                MAX_SEQUENCE_NUMBER,
                b"a".to_vec(),
            );
            iter2.seek_for_prev(b"zz");
            assert_eq!(iter2.key(), Some(&b"a2"[..]));

            // 3. BidiIterator::lazy reverse drain
            let bidi = BidiIterator::lazy(DBIterator::from_sources_with_prefix(
                vec![IterSource::new(entries).with_level(0)],
                MAX_SEQUENCE_NUMBER,
                b"a".to_vec(),
            ));
            let rev: Vec<_> = bidi.rev().collect();
            assert_eq!(
                rev,
                vec![
                    (b"a2".to_vec(), b"v2".to_vec()),
                    (b"a1".to_vec(), b"v1".to_vec()),
                ]
            );
        }
    }

    #[test]
    fn step_next_allocates_zero_heap_bytes_in_steady_state() {
        use crate::{DB, DbOptions};

        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(
            DbOptions::default()
                .with_block_cache_capacity(8 << 20)
                .with_write_buffer_size(4 << 20),
            dir.path(),
        )
        .unwrap();

        // 1. MemTable scan with shadowed MVCC versions, point deletions, and
        //    post-snapshot invisible entries: steady-state step_next() must
        //    recycle key/value buffers with 0 heap allocations.
        let val_a = vec![b'a'; 256];
        let val_b = vec![b'b'; 256];
        for i in 0..300u32 {
            let k = format!("key_{i:06}");
            db.put(k.as_bytes(), &val_a).unwrap();
            if i % 2 == 0 {
                db.put(k.as_bytes(), &val_b).unwrap();
            }
            if i % 5 == 0 {
                db.delete(k.as_bytes()).unwrap();
            }
        }
        let snap = db.snapshot();
        for i in 0..100u32 {
            let k = format!("key_{:06}", i * 3);
            db.put(k.as_bytes(), &[b'z'; 512]).unwrap();
        }

        let mut mem_iter = snap.iter().unwrap();
        for _ in 0..3 {
            assert!(mem_iter.step_next().unwrap().is_some());
        }
        let (rows, mem_alloc) = crate::test_alloc::count_allocated(|| {
            let mut n = 0usize;
            while let Some((k, v)) = mem_iter.step_next().unwrap() {
                assert_eq!(k.len(), 10);
                assert_eq!(v.len(), 256);
                n += 1;
            }
            n
        });
        assert_eq!(rows, 240 - 3);
        assert_eq!(
            mem_alloc, 0,
            "steady-state MemTable step_next allocated {mem_alloc} heap bytes across {rows} rows"
        );
        drop(mem_iter);
        drop(snap);

        // 2. Multi-source merge (flushed multi-block SST + active MemTable):
        //    warm the SST blocks in BlockCache, then verify steady-state
        //    step_next() allocates 0 heap bytes across merged rows.
        db.flush().unwrap();
        for i in 0..300u32 {
            if i % 3 == 1 {
                let k = format!("key_{i:06}");
                db.put(k.as_bytes(), &val_b).unwrap();
            }
        }
        {
            let mut warm = db.iter().unwrap();
            while warm.step_next().unwrap().is_some() {}
        }

        let mut merged_iter = db.iter().unwrap();
        for _ in 0..4 {
            assert!(merged_iter.step_next().unwrap().is_some());
        }
        let (merged_rows, merged_alloc) = crate::test_alloc::count_allocated(|| {
            let mut n = 0usize;
            while let Some((k, v)) = merged_iter.step_next().unwrap() {
                assert_eq!(k.len(), 10);
                assert!(!v.is_empty());
                n += 1;
            }
            n
        });
        assert!(merged_rows > 200);
        assert_eq!(
            merged_alloc, 0,
            "steady-state merged SST+MemTable step_next allocated {merged_alloc} heap bytes"
        );
    }
}

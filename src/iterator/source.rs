//! Iterator source abstraction: wraps Vec, boxed, seekable, memtable, SST, and level
//! iterators behind a uniform `IterSource` interface. Also defines the `SeekableIterator`
//! trait that concrete iterators implement.

use std::{cmp::Ordering, sync::Arc};

use crate::{
    Error, iterator::level_iter::LevelIterator, memtable::skiplist::MemTableCursorIter,
    options::IterConfig, sst::table_reader::TableIterator, types::LazyValue,
};

/// A source of sorted (key, value) pairs — can be backed by a Vec or a streaming iterator.
pub struct IterSource {
    inner: IterSourceInner,
    /// Reusable key buffer for peeked entry.
    pub(crate) peeked_key: Vec<u8>,
    /// Lazy value buffer for peeked entry (zero-copy for SST and MemTable sources).
    pub(crate) peeked_value: LazyValue,
    /// Whether a peeked entry key is available.
    pub(crate) has_peeked: bool,
    /// Whether `peeked_value` has been populated for the current `peeked_key`.
    value_materialized: bool,
    /// LSM level this source belongs to. Memtable/L0 = 0, L1 = 1, etc.
    /// Defaults to usize::MAX (unknown/untagged).
    pub(crate) level: usize,
}

enum IterSourceInner {
    Vec {
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        pos: usize,
    },
    /// Test-only generic box; production uses typed Sst/Level/Memtable arms so
    /// `iter_error` can surface. Kept for unit tests of Merkel ports.
    #[cfg(test)]
    Boxed(Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>),
    /// Direct (non-boxed) memtable cursor — zero vtable dispatch.
    Memtable {
        iter: crate::memtable::skiplist::MemTableCursorIter,
    },
    /// Direct (non-boxed) SST table iterator — zero vtable dispatch.
    Sst {
        iter: crate::sst::table_reader::TableIterator,
    },
    /// Direct (non-boxed) level iterator — zero vtable dispatch.
    Level { iter: LevelIterator },
}

/// Trait for iterators that support seeking and bidirectional traversal.
///
/// Modeled after RocksDB's `InternalIterator` for production-quality
/// bidirectional iteration through the LSM stack.
pub trait SeekableIterator: Iterator<Item = (Vec<u8>, Vec<u8>)> {
    /// Seek to the first entry >= target.
    fn seek_to(&mut self, target: &[u8]);
    /// Hint the OS to prefetch the first data block. Default: no-op.
    /// Called by init_heap before peek() to overlap I/O across sources.
    fn prefetch_first_block(&mut self) {}
    /// Decode next entry directly into caller-provided buffers. Returns true if an entry
    /// was loaded, false if exhausted. Avoids intermediate allocation.
    fn next_into(&mut self, key_buf: &mut Vec<u8>, value_buf: &mut Vec<u8>) -> bool {
        match self.next() {
            Some((k, v)) => {
                *key_buf = k;
                *value_buf = v;
                true
            }
            None => false,
        }
    }

    /// Move to the previous entry. Returns the entry at the new position,
    /// or None if we've moved before the first entry.
    fn prev(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        None
    }

    /// Decode previous entry into reusable `(key_buf, LazyValue)` without copying values.
    fn prev_into_lazy(&mut self, key_buf: &mut Vec<u8>, val_out: &mut LazyValue) -> bool {
        match self.prev() {
            Some((k, v)) => {
                key_buf.clear();
                key_buf.extend_from_slice(&k);
                *val_out = v;
                true
            }
            None => false,
        }
    }

    /// Return the entry at the current cursor position WITHOUT advancing.
    /// Used after seek_for_prev() / seek_to_last() to peek the current entry
    /// without moving the cursor forward — critical for correct backward iteration.
    fn current(&self) -> Option<(Vec<u8>, LazyValue)> {
        None
    }

    /// Populate reusable `(key_buf, LazyValue)` from the current reverse cursor position.
    fn current_into_lazy(&self, key_buf: &mut Vec<u8>, val_out: &mut LazyValue) -> bool {
        match self.current() {
            Some((k, v)) => {
                key_buf.clear();
                key_buf.extend_from_slice(&k);
                *val_out = v;
                true
            }
            None => false,
        }
    }

    /// Seek to the last entry <= target.
    fn seek_for_prev(&mut self, target: &[u8]);

    /// Seek to the first entry.
    fn seek_to_first(&mut self);

    /// Seek to the last entry.
    fn seek_to_last(&mut self);

    /// Return the last error encountered during iteration, if any.
    /// This distinguishes normal exhaustion (None) from I/O failures.
    fn iter_error(&self) -> Option<&Error> {
        None
    }

    /// Set lower/upper bounds for iteration. Default: no-op.
    fn set_config(&mut self, _config: Arc<IterConfig>) {}
    #[cfg(test)]
    fn set_bounds(&mut self, lower: Option<&[u8]>, upper: Option<&[u8]>) {
        self.set_config(Arc::new(IterConfig::new(lower, upper, &[])));
    }

    /// Advance to the next entry and write ONLY its key into `key_buf` so skipped
    /// MVCC versions pay zero value allocation or `Arc` cloning.
    fn advance_key(&mut self, key_buf: &mut Vec<u8>) -> bool;

    /// Materialize the `LazyValue` for the entry most recently loaded by `advance_key`.
    fn materialize_value_into(&self, val_out: &mut LazyValue);
}

impl IterSource {
    pub fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self {
            inner: IterSourceInner::Vec { entries, pos: 0 },
            peeked_key: Vec::new(),
            peeked_value: LazyValue::empty(),
            has_peeked: false,
            value_materialized: false,
            level: usize::MAX,
        }
    }

    #[cfg(test)]
    pub fn from_boxed(iter: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>) -> Self {
        Self {
            inner: IterSourceInner::Boxed(iter),
            peeked_key: Vec::new(),
            peeked_value: LazyValue::empty(),
            has_peeked: false,
            value_materialized: false,
            level: usize::MAX,
        }
    }

    pub(crate) fn from_memtable(iter: MemTableCursorIter) -> Self {
        Self {
            inner: IterSourceInner::Memtable { iter },
            peeked_key: Vec::new(),
            peeked_value: LazyValue::empty(),
            has_peeked: false,
            value_materialized: false,
            level: usize::MAX,
        }
    }

    pub fn from_table_iter(iter: TableIterator) -> Self {
        Self {
            inner: IterSourceInner::Sst { iter },
            peeked_key: Vec::new(),
            peeked_value: LazyValue::empty(),
            has_peeked: false,
            value_materialized: false,
            level: usize::MAX,
        }
    }

    pub fn from_level_iter(iter: LevelIterator) -> Self {
        Self {
            inner: IterSourceInner::Level { iter },
            peeked_key: Vec::new(),
            peeked_value: LazyValue::empty(),
            has_peeked: false,
            value_materialized: false,
            level: usize::MAX,
        }
    }

    /// Tag this source with the LSM level it was read from (memtable/L0 = 0,
    /// L1 = 1, etc.). Required for cross-level range-tombstone pruning — a
    /// tombstone may cover keys from its own level or deeper, but a tombstone
    /// strictly deeper than a key's source level must never cover it. Sources
    /// left untagged (`usize::MAX`) disable the level filter for
    /// that source's entries, so every construction site feeding a
    /// `MergingIterator` used for tombstone-aware iteration must call this.
    pub(crate) fn with_level(mut self, level: usize) -> Self {
        self.level = level;
        self
    }

    /// Peek only the key slice without materializing `LazyValue` (`0` value work on skips).
    #[inline]
    pub fn peek_key_only(&mut self) -> Option<&[u8]> {
        if !self.has_peeked {
            self.has_peeked = self.advance_key_into_buffer();
        }
        if self.has_peeked {
            Some(self.peeked_key.as_slice())
        } else {
            None
        }
    }

    /// Recycle caller buffers via `swap` (`0` heap allocations in steady state).
    #[inline]
    pub fn take_peeked_into(&mut self, key_out: &mut Vec<u8>, val_out: &mut LazyValue) -> bool {
        if !self.has_peeked {
            self.has_peeked = self.advance_key_into_buffer();
        }
        if !self.has_peeked {
            return false;
        }
        std::mem::swap(key_out, &mut self.peeked_key);
        if self.value_materialized {
            std::mem::swap(val_out, &mut self.peeked_value);
        } else {
            match &self.inner {
                IterSourceInner::Memtable { iter } => iter.materialize_value_into(val_out),
                IterSourceInner::Sst { iter } => iter.materialize_value_into(val_out),
                IterSourceInner::Level { iter } => iter.materialize_value_into(val_out),
                IterSourceInner::Vec { .. } => unreachable!(),
                #[cfg(test)]
                IterSourceInner::Boxed(_) => unreachable!(),
            }
        }
        self.has_peeked = false;
        self.value_materialized = false;
        true
    }

    /// Take the peeked key/lazy-value, transferring ownership.
    pub fn take_peeked(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        let mut key_out = Vec::new();
        let mut val_out = LazyValue::empty();
        self.take_peeked_into(&mut key_out, &mut val_out)
            .then_some((key_out, val_out))
    }

    /// Advance the inner iterator and store its key in `peeked_key` (deferring value
    /// materialization for `Memtable` / `Sst` / `Level` sources).
    fn advance_key_into_buffer(&mut self) -> bool {
        match &mut self.inner {
            IterSourceInner::Vec { entries, pos } => {
                if *pos < entries.len() {
                    let (ref k, ref v) = entries[*pos];
                    self.peeked_key.clear();
                    self.peeked_key.extend_from_slice(k);
                    self.peeked_value = LazyValue::Inline(v.clone());
                    self.value_materialized = true;
                    *pos += 1;
                    true
                } else {
                    self.value_materialized = false;
                    false
                }
            }
            #[cfg(test)]
            IterSourceInner::Boxed(iter) => match iter.next() {
                Some((k, v)) => {
                    self.peeked_key = k;
                    self.peeked_value = LazyValue::Inline(v);
                    self.value_materialized = true;
                    true
                }
                None => {
                    self.value_materialized = false;
                    false
                }
            },
            IterSourceInner::Memtable { iter } => {
                self.value_materialized = false;
                iter.advance_key(&mut self.peeked_key)
            }
            IterSourceInner::Sst { iter } => {
                self.value_materialized = false;
                iter.advance_key(&mut self.peeked_key)
            }
            IterSourceInner::Level { iter } => {
                self.value_materialized = false;
                iter.advance_key(&mut self.peeked_key)
            }
        }
    }

    /// Advance the inner iterator backward and store the result in reusable buffers.
    /// Returns true if a new entry was loaded.
    fn reverse_advance_into_buffers(&mut self) -> bool {
        self.value_materialized = false;
        let ok = match &mut self.inner {
            IterSourceInner::Vec { entries, pos } => {
                if *pos >= 2 {
                    *pos -= 2;
                    let (ref k, ref v) = entries[*pos];
                    self.peeked_key.clear();
                    self.peeked_key.extend_from_slice(k);
                    self.peeked_value = LazyValue::Inline(v.clone());
                    *pos += 1;
                    true
                } else {
                    false
                }
            }
            #[cfg(test)]
            IterSourceInner::Boxed(_) => false,
            IterSourceInner::Memtable { iter } => {
                iter.prev_into_lazy(&mut self.peeked_key, &mut self.peeked_value)
            }
            IterSourceInner::Sst { iter } => {
                iter.prev_into_lazy(&mut self.peeked_key, &mut self.peeked_value)
            }
            IterSourceInner::Level { iter } => {
                iter.prev_into_lazy(&mut self.peeked_key, &mut self.peeked_value)
            }
        };
        self.value_materialized = ok;
        ok
    }

    /// Propagate iteration bounds to the underlying seekable iterator (if any).
    pub fn set_config(&mut self, config: Arc<IterConfig>) {
        match &mut self.inner {
            IterSourceInner::Memtable { iter } => iter.set_config(config),
            IterSourceInner::Sst { iter } => iter.set_config(config),
            IterSourceInner::Level { iter } => iter.set_config(config),
            _ => {}
        }
    }

    /// Return the first error from the underlying iterator, if any.
    pub fn iter_error(&self) -> Option<&Error> {
        match &self.inner {
            IterSourceInner::Sst { iter } => iter.iter_error(),
            IterSourceInner::Level { iter } => iter.iter_error(),
            _ => None,
        }
    }

    /// Issue a prefetch hint for the first data block (if backed by a seekable iterator).
    pub fn prefetch_hint(&mut self) {
        match &mut self.inner {
            IterSourceInner::Sst { iter } => iter.prefetch_first_block(),
            IterSourceInner::Level { iter } => iter.prefetch_first_block(),
            _ => {}
        }
    }

    /// Seek to the first key >= target, supporting backward movement.
    /// For Vec sources, resets position and re-scans from the appropriate point.
    /// For plain Boxed sources, can only seek forward (same as `seek`).
    pub fn seek_to<F: Fn(&[u8], &[u8]) -> Ordering>(&mut self, target: &[u8], compare: &F) {
        self.has_peeked = false;
        self.value_materialized = false;
        match &mut self.inner {
            IterSourceInner::Vec { entries, pos } => {
                let idx = entries.partition_point(|(k, _)| compare(k, target) == Ordering::Less);
                *pos = idx;
                if *pos < entries.len() {
                    let (ref k, ref v) = entries[*pos];
                    self.peeked_key.clear();
                    self.peeked_key.extend_from_slice(k);
                    self.peeked_value = LazyValue::Inline(v.clone());
                    self.has_peeked = true;
                    self.value_materialized = true;
                    *pos += 1;
                }
            }
            #[cfg(test)]
            IterSourceInner::Boxed(_) => {
                self.seek(target, compare);
            }
            IterSourceInner::Memtable { iter } => {
                iter.seek_to(target);
                if iter.advance_key(&mut self.peeked_key) {
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Sst { iter } => {
                iter.seek_to(target);
                if iter.advance_key(&mut self.peeked_key) {
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Level { iter } => {
                iter.seek_to(target);
                if iter.advance_key(&mut self.peeked_key) {
                    self.has_peeked = true;
                }
            }
        }
    }

    /// Seek forward until the current key >= target according to `compare`.
    /// Used by the test-only boxed source arm.
    #[cfg(test)]
    pub fn seek<F: Fn(&[u8], &[u8]) -> Ordering>(&mut self, target: &[u8], compare: &F) {
        // Discard peeked if < target
        loop {
            match self.peek_key_only() {
                Some(k) if compare(k, target) == Ordering::Less => {
                    self.has_peeked = false;
                    self.value_materialized = false;
                    self.has_peeked = self.advance_key_into_buffer();
                    if !self.has_peeked {
                        break;
                    }
                }
                _ => break,
            }
        }
    }

    /// Seek to the last key <= target (for backward iteration).
    pub fn seek_for_prev_to<F: Fn(&[u8], &[u8]) -> Ordering>(
        &mut self,
        target: &[u8],
        compare: &F,
    ) {
        self.has_peeked = false;
        self.value_materialized = false;
        match &mut self.inner {
            IterSourceInner::Vec { entries, pos } => {
                // partition_point returns first idx where key > target
                let idx = entries.partition_point(|(k, _)| compare(k, target) != Ordering::Greater);
                if idx > 0 {
                    *pos = idx - 1;
                    let (ref k, ref v) = entries[*pos];
                    self.peeked_key.clear();
                    self.peeked_key.extend_from_slice(k);
                    self.peeked_value = LazyValue::Inline(v.clone());
                    self.has_peeked = true;
                    self.value_materialized = true;
                    *pos += 1;
                }
            }
            IterSourceInner::Memtable { iter } => {
                iter.seek_for_prev(target);
                if iter.current_into_lazy(&mut self.peeked_key, &mut self.peeked_value) {
                    self.has_peeked = true;
                    self.value_materialized = true;
                }
            }
            IterSourceInner::Sst { iter } => {
                SeekableIterator::seek_for_prev(iter, target);
                if SeekableIterator::current_into_lazy(
                    iter,
                    &mut self.peeked_key,
                    &mut self.peeked_value,
                ) {
                    self.has_peeked = true;
                    self.value_materialized = true;
                }
            }
            IterSourceInner::Level { iter } => {
                iter.seek_for_prev(target);
                if iter.current_into_lazy(&mut self.peeked_key, &mut self.peeked_value) {
                    self.has_peeked = true;
                    self.value_materialized = true;
                }
            }
            #[cfg(test)]
            IterSourceInner::Boxed(_) => {}
        }
    }

    /// Seek to the first entry (for forward iteration from beginning).
    pub fn seek_to_first_impl(&mut self) {
        self.has_peeked = false;
        self.value_materialized = false;
        match &mut self.inner {
            IterSourceInner::Vec { entries, pos } => {
                *pos = 0;
                if !entries.is_empty() {
                    let (ref k, ref v) = entries[0];
                    self.peeked_key.clear();
                    self.peeked_key.extend_from_slice(k);
                    self.peeked_value = LazyValue::Inline(v.clone());
                    self.has_peeked = true;
                    self.value_materialized = true;
                    *pos = 1;
                }
            }
            IterSourceInner::Memtable { iter } => {
                iter.seek_to_first();
                if iter.advance_key(&mut self.peeked_key) {
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Sst { iter } => {
                iter.seek_to_first();
                if iter.advance_key(&mut self.peeked_key) {
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Level { iter } => {
                iter.seek_to_first();
                if iter.advance_key(&mut self.peeked_key) {
                    self.has_peeked = true;
                }
            }
            #[cfg(test)]
            IterSourceInner::Boxed(_) => {}
        }
    }

    /// Seek to the last entry (for backward iteration from end).
    pub fn seek_to_last_impl(&mut self) {
        self.has_peeked = false;
        self.value_materialized = false;
        match &mut self.inner {
            IterSourceInner::Vec { entries, pos } => {
                if !entries.is_empty() {
                    *pos = entries.len() - 1;
                    let (ref k, ref v) = entries[*pos];
                    self.peeked_key.clear();
                    self.peeked_key.extend_from_slice(k);
                    self.peeked_value = LazyValue::Inline(v.clone());
                    self.has_peeked = true;
                    self.value_materialized = true;
                    *pos += 1;
                }
            }
            IterSourceInner::Memtable { iter } => {
                iter.seek_to_last();
                if iter.current_into_lazy(&mut self.peeked_key, &mut self.peeked_value) {
                    self.has_peeked = true;
                    self.value_materialized = true;
                }
            }
            IterSourceInner::Sst { iter } => {
                SeekableIterator::seek_to_last(iter);
                if SeekableIterator::current_into_lazy(
                    iter,
                    &mut self.peeked_key,
                    &mut self.peeked_value,
                ) {
                    self.has_peeked = true;
                    self.value_materialized = true;
                }
            }
            IterSourceInner::Level { iter } => {
                iter.seek_to_last();
                if iter.current_into_lazy(&mut self.peeked_key, &mut self.peeked_value) {
                    self.has_peeked = true;
                    self.value_materialized = true;
                }
            }
            #[cfg(test)]
            IterSourceInner::Boxed(_) => {
                // Cannot seek to last on plain iterator
            }
        }
    }

    /// Advance backward: load the previous entry into the peek buffers.
    pub fn prev_advance(&mut self) -> bool {
        self.has_peeked = false;
        self.has_peeked = self.reverse_advance_into_buffers();
        self.has_peeked
    }

    /// Discard the current peeked entry so the next peek() will advance.
    #[inline]
    pub fn skip_peeked(&mut self) {
        self.has_peeked = false;
        self.value_materialized = false;
    }
}

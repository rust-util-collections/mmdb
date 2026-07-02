//! Iterator source abstraction: wraps Vec, boxed, seekable, memtable, SST, and level
//! iterators behind a uniform `IterSource` interface. Also defines the `SeekableIterator`
//! trait that concrete iterators implement.

use std::cmp::Ordering;

use crate::iterator::LevelIterator;
use crate::memtable::skiplist::MemTableCursorIter;
use crate::sst::table_reader::TableIterator;
use crate::types::LazyValue;

/// A source of sorted (key, value) pairs — can be backed by a Vec or a streaming iterator.
pub struct IterSource {
    inner: IterSourceInner,
    /// Reusable key buffer for peeked entry.
    pub(crate) peeked_key: Vec<u8>,
    /// Lazy value buffer for peeked entry (zero-copy for SST sources).
    pub(crate) peeked_value: LazyValue,
    /// Whether a peeked entry is available.
    pub(crate) has_peeked: bool,
    /// LSM level this source belongs to. Memtable/L0 = 0, L1 = 1, etc.
    /// Defaults to usize::MAX (unknown/untagged).
    pub(crate) level: usize,
}

enum IterSourceInner {
    Vec {
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        pos: usize,
    },
    Boxed(Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>),
    /// A seekable boxed source (e.g., backed by a TableIterator that supports seek).
    SeekableBoxed {
        iter: Box<dyn SeekableIterator + Send>,
    },
    /// Direct (non-boxed) memtable cursor — zero vtable dispatch.
    Memtable {
        iter: crate::memtable::skiplist::MemTableCursorIter,
    },
    /// Direct (non-boxed) SST table iterator — zero vtable dispatch.
    Sst {
        iter: crate::sst::table_reader::TableIterator,
    },
    /// Direct (non-boxed) level iterator — zero vtable dispatch.
    Level {
        iter: LevelIterator,
    },
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

    /// Decode previous entry directly into caller-provided buffers.
    fn prev_into(&mut self, key_buf: &mut Vec<u8>, value_buf: &mut Vec<u8>) -> bool {
        match self.prev() {
            Some((k, v)) => {
                *key_buf = k;
                *value_buf = v.into_vec();
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

    /// Seek to the last entry <= target.
    fn seek_for_prev(&mut self, target: &[u8]);

    /// Seek to the first entry.
    fn seek_to_first(&mut self);

    /// Seek to the last entry.
    fn seek_to_last(&mut self);

    /// Return the last error encountered during iteration, if any.
    /// This distinguishes normal exhaustion (None) from I/O failures.
    fn iter_error(&self) -> Option<String> {
        None
    }

    /// Set lower/upper bounds for iteration. Default: no-op.
    fn set_bounds(&mut self, _lower: Option<&[u8]>, _upper: Option<&[u8]>) {}

    /// Decode next entry with lazy value — key written into caller buffer,
    /// value returned as LazyValue (zero-copy for SST sources).
    /// Default impl delegates to next_into() and wraps as Inline.
    fn next_lazy(&mut self, key_buf: &mut Vec<u8>) -> Option<LazyValue> {
        // Default: allocate a temp value buffer and wrap as Inline.
        let mut val_buf = Vec::new();
        if self.next_into(key_buf, &mut val_buf) {
            Some(LazyValue::Inline(val_buf))
        } else {
            None
        }
    }
}

impl IterSource {
    pub fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self {
            inner: IterSourceInner::Vec { entries, pos: 0 },
            peeked_key: Vec::new(),
            peeked_value: LazyValue::empty(),
            has_peeked: false,
            level: usize::MAX,
        }
    }

    pub fn from_boxed(iter: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>) -> Self {
        Self {
            inner: IterSourceInner::Boxed(iter),
            peeked_key: Vec::new(),
            peeked_value: LazyValue::empty(),
            has_peeked: false,
            level: usize::MAX,
        }
    }

    pub fn from_seekable(iter: Box<dyn SeekableIterator + Send>) -> Self {
        Self {
            inner: IterSourceInner::SeekableBoxed { iter },
            peeked_key: Vec::new(),
            peeked_value: LazyValue::empty(),
            has_peeked: false,
            level: usize::MAX,
        }
    }

    pub(crate) fn from_memtable(iter: MemTableCursorIter) -> Self {
        Self {
            inner: IterSourceInner::Memtable { iter },
            peeked_key: Vec::new(),
            peeked_value: LazyValue::empty(),
            has_peeked: false,
            level: usize::MAX,
        }
    }

    pub fn from_table_iter(iter: TableIterator) -> Self {
        Self {
            inner: IterSourceInner::Sst { iter },
            peeked_key: Vec::new(),
            peeked_value: LazyValue::empty(),
            has_peeked: false,
            level: usize::MAX,
        }
    }

    pub fn from_level_iter(iter: LevelIterator) -> Self {
        Self {
            inner: IterSourceInner::Level { iter },
            peeked_key: Vec::new(),
            peeked_value: LazyValue::empty(),
            has_peeked: false,
            level: usize::MAX,
        }
    }

    /// Tag this source with its LSM level.
    pub fn with_level(mut self, level: usize) -> Self {
        self.level = level;
        self
    }

    /// Return the LSM level of this source.
    pub fn level(&self) -> usize {
        self.level
    }

    pub fn peek(&mut self) -> Option<(&[u8], &[u8])> {
        if !self.has_peeked {
            self.has_peeked = self.advance_into_buffers();
        }
        if self.has_peeked {
            Some((self.peeked_key.as_slice(), self.peeked_value.as_slice()))
        } else {
            None
        }
    }

    /// Take the peeked key/lazy-value, transferring ownership.
    pub fn take_peeked(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        if !self.has_peeked {
            self.has_peeked = self.advance_into_buffers();
        }
        if self.has_peeked {
            self.has_peeked = false;
            Some((
                std::mem::take(&mut self.peeked_key),
                std::mem::replace(&mut self.peeked_value, LazyValue::empty()),
            ))
        } else {
            None
        }
    }

    /// Advance the inner iterator and store the result in reusable buffers.
    /// Returns true if a new entry was loaded.
    fn advance_into_buffers(&mut self) -> bool {
        match &mut self.inner {
            IterSourceInner::Vec { entries, pos } => {
                if *pos < entries.len() {
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
            IterSourceInner::Boxed(iter) => match iter.next() {
                Some((k, v)) => {
                    self.peeked_key = k;
                    self.peeked_value = LazyValue::Inline(v);
                    true
                }
                None => false,
            },
            IterSourceInner::SeekableBoxed { iter } => match iter.next_lazy(&mut self.peeked_key) {
                Some(lv) => {
                    self.peeked_value = lv;
                    true
                }
                None => false,
            },
            IterSourceInner::Memtable { iter } => match iter.next_lazy(&mut self.peeked_key) {
                Some(lv) => {
                    self.peeked_value = lv;
                    true
                }
                None => false,
            },
            IterSourceInner::Sst { iter } => match iter.next_lazy(&mut self.peeked_key) {
                Some(lv) => {
                    self.peeked_value = lv;
                    true
                }
                None => false,
            },
            IterSourceInner::Level { iter } => match iter.next_lazy(&mut self.peeked_key) {
                Some(lv) => {
                    self.peeked_value = lv;
                    true
                }
                None => false,
            },
        }
    }

    /// Advance the inner iterator backward and store the result in reusable buffers.
    /// Returns true if a new entry was loaded.
    fn reverse_advance_into_buffers(&mut self) -> bool {
        match &mut self.inner {
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
            IterSourceInner::Boxed(_) => false,
            IterSourceInner::SeekableBoxed { iter } => {
                let mut tmp_val = Vec::new();
                if iter.prev_into(&mut self.peeked_key, &mut tmp_val) {
                    self.peeked_value = LazyValue::Inline(tmp_val);
                    true
                } else {
                    false
                }
            }
            IterSourceInner::Memtable { iter } => {
                let mut tmp_val = Vec::new();
                if iter.prev_into(&mut self.peeked_key, &mut tmp_val) {
                    self.peeked_value = LazyValue::Inline(tmp_val);
                    true
                } else {
                    false
                }
            }
            IterSourceInner::Sst { iter } => {
                let mut tmp_val = Vec::new();
                if iter.prev_into(&mut self.peeked_key, &mut tmp_val) {
                    self.peeked_value = LazyValue::Inline(tmp_val);
                    true
                } else {
                    false
                }
            }
            IterSourceInner::Level { iter } => {
                let mut tmp_val = Vec::new();
                if iter.prev_into(&mut self.peeked_key, &mut tmp_val) {
                    self.peeked_value = LazyValue::Inline(tmp_val);
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn is_exhausted(&mut self) -> bool {
        self.peek().is_none()
    }

    /// Propagate iteration bounds to the underlying seekable iterator (if any).
    pub fn set_bounds(&mut self, lower: Option<&[u8]>, upper: Option<&[u8]>) {
        match &mut self.inner {
            IterSourceInner::SeekableBoxed { iter } => iter.set_bounds(lower, upper),
            IterSourceInner::Memtable { iter } => iter.set_bounds(lower, upper),
            IterSourceInner::Sst { iter } => iter.set_bounds(lower, upper),
            IterSourceInner::Level { iter } => iter.set_bounds(lower, upper),
            _ => {}
        }
    }

    /// Return the first error from the underlying iterator, if any.
    pub fn iter_error(&self) -> Option<String> {
        match &self.inner {
            IterSourceInner::SeekableBoxed { iter } => iter.iter_error(),
            IterSourceInner::Sst { iter } => iter.iter_error(),
            IterSourceInner::Level { iter } => iter.iter_error(),
            _ => None,
        }
    }

    /// Issue a prefetch hint for the first data block (if backed by a seekable iterator).
    pub fn prefetch_hint(&mut self) {
        match &mut self.inner {
            IterSourceInner::SeekableBoxed { iter } => iter.prefetch_first_block(),
            IterSourceInner::Sst { iter } => iter.prefetch_first_block(),
            IterSourceInner::Level { iter } => iter.prefetch_first_block(),
            _ => {}
        }
    }

    /// Seek to the first key >= target, supporting backward movement.
    /// For Vec sources, resets position and re-scans from the appropriate point.
    /// For SeekableBoxed sources, delegates to the underlying iterator's seek.
    /// For plain Boxed sources, can only seek forward (same as `seek`).
    pub fn seek_to<F: Fn(&[u8], &[u8]) -> Ordering>(&mut self, target: &[u8], compare: &F) {
        self.has_peeked = false;
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
                    *pos += 1;
                }
            }
            IterSourceInner::Boxed(_) => {
                self.seek(target, compare);
            }
            // Seekable variants: seek + peek next entry (lazily — key lands in
            // the reused peeked_key buffer, value stays zero-copy for SST sources)
            IterSourceInner::SeekableBoxed { iter } => {
                iter.seek_to(target);
                if let Some(lv) = iter.next_lazy(&mut self.peeked_key) {
                    self.peeked_value = lv;
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Memtable { iter } => {
                iter.seek_to(target);
                if let Some(lv) = iter.next_lazy(&mut self.peeked_key) {
                    self.peeked_value = lv;
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Sst { iter } => {
                iter.seek_to(target);
                if let Some(lv) = iter.next_lazy(&mut self.peeked_key) {
                    self.peeked_value = lv;
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Level { iter } => {
                iter.seek_to(target);
                if let Some(lv) = iter.next_lazy(&mut self.peeked_key) {
                    self.peeked_value = lv;
                    self.has_peeked = true;
                }
            }
        }
    }

    /// Seek forward until the current key >= target according to `compare`.
    pub fn seek<F: Fn(&[u8], &[u8]) -> Ordering>(&mut self, target: &[u8], compare: &F) {
        // Discard peeked if < target
        loop {
            match self.peek() {
                Some((k, _)) if compare(k, target) == Ordering::Less => {
                    self.has_peeked = false;
                    self.has_peeked = self.advance_into_buffers();
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
                    *pos += 1;
                }
            }
            IterSourceInner::SeekableBoxed { iter } => {
                iter.seek_for_prev(target);
                if let Some((k, v)) = iter.current() {
                    self.peeked_key = k;
                    self.peeked_value = v;
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Memtable { iter } => {
                iter.seek_for_prev(target);
                if let Some((k, v)) = iter.current() {
                    self.peeked_key = k;
                    self.peeked_value = v;
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Sst { iter } => {
                SeekableIterator::seek_for_prev(iter, target);
                if let Some((k, v)) = SeekableIterator::current(iter) {
                    self.peeked_key = k;
                    self.peeked_value = v;
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Level { iter } => {
                iter.seek_for_prev(target);
                if let Some((k, v)) = iter.current() {
                    self.peeked_key = k;
                    self.peeked_value = v;
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Boxed(_) => {}
        }
    }

    /// Seek to the first entry (for forward iteration from beginning).
    pub fn seek_to_first_impl(&mut self) {
        self.has_peeked = false;
        match &mut self.inner {
            IterSourceInner::Vec { entries, pos } => {
                *pos = 0;
                if !entries.is_empty() {
                    let (ref k, ref v) = entries[0];
                    self.peeked_key.clear();
                    self.peeked_key.extend_from_slice(k);
                    self.peeked_value = LazyValue::Inline(v.clone());
                    self.has_peeked = true;
                    *pos = 1;
                }
            }
            IterSourceInner::SeekableBoxed { iter } => {
                iter.seek_to_first();
                if let Some((k, v)) = iter.next() {
                    self.peeked_key = k;
                    self.peeked_value = LazyValue::Inline(v);
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Memtable { iter } => {
                iter.seek_to_first();
                if let Some((k, v)) = iter.next() {
                    self.peeked_key = k;
                    self.peeked_value = LazyValue::Inline(v);
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Sst { iter } => {
                iter.seek_to_first();
                if let Some((k, v)) = iter.next() {
                    self.peeked_key = k;
                    self.peeked_value = LazyValue::Inline(v);
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Level { iter } => {
                iter.seek_to_first();
                if let Some((k, v)) = iter.next() {
                    self.peeked_key = k;
                    self.peeked_value = LazyValue::Inline(v);
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Boxed(_) => {}
        }
    }

    /// Seek to the last entry (for backward iteration from end).
    pub fn seek_to_last_impl(&mut self) {
        self.has_peeked = false;
        match &mut self.inner {
            IterSourceInner::Vec { entries, pos } => {
                if !entries.is_empty() {
                    *pos = entries.len() - 1;
                    let (ref k, ref v) = entries[*pos];
                    self.peeked_key.clear();
                    self.peeked_key.extend_from_slice(k);
                    self.peeked_value = LazyValue::Inline(v.clone());
                    self.has_peeked = true;
                    *pos += 1;
                }
            }
            IterSourceInner::SeekableBoxed { iter } => {
                iter.seek_to_last();
                if let Some((k, v)) = iter.current() {
                    self.peeked_key = k;
                    self.peeked_value = v;
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Memtable { iter } => {
                iter.seek_to_last();
                if let Some((k, v)) = iter.current() {
                    self.peeked_key = k;
                    self.peeked_value = v;
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Sst { iter } => {
                SeekableIterator::seek_to_last(iter);
                if let Some((k, v)) = SeekableIterator::current(iter) {
                    self.peeked_key = k;
                    self.peeked_value = v;
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Level { iter } => {
                iter.seek_to_last();
                if let Some((k, v)) = iter.current() {
                    self.peeked_key = k;
                    self.peeked_value = v;
                    self.has_peeked = true;
                }
            }
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
    }
}

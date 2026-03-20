//! MergingIterator: merges multiple sorted iterators into a single sorted stream.
//!
//! Uses a min-heap for O(N·log K) performance instead of O(N·K) linear scan.

use std::cmp::Ordering;

/// A source of sorted (key, value) pairs — can be backed by a Vec or a streaming iterator.
pub struct IterSource {
    inner: IterSourceInner,
    /// Reusable key buffer for peeked entry.
    peeked_key: Vec<u8>,
    /// Lazy value buffer for peeked entry (zero-copy for SST sources).
    peeked_value: crate::types::LazyValue,
    /// Whether a peeked entry is available.
    pub(crate) has_peeked: bool,
    /// LSM level this source belongs to. Memtable/L0 = 0, L1 = 1, etc.
    /// Defaults to usize::MAX (unknown/untagged).
    level: usize,
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
        iter: crate::iterator::LevelIterator,
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
    fn prev(&mut self) -> Option<(Vec<u8>, crate::types::LazyValue)> {
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
    fn current(&self) -> Option<(Vec<u8>, crate::types::LazyValue)> {
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
    fn next_lazy(&mut self, key_buf: &mut Vec<u8>) -> Option<crate::types::LazyValue> {
        // Default: allocate a temp value buffer and wrap as Inline.
        let mut val_buf = Vec::new();
        if self.next_into(key_buf, &mut val_buf) {
            Some(crate::types::LazyValue::Inline(val_buf))
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
            peeked_value: crate::types::LazyValue::empty(),
            has_peeked: false,
            level: usize::MAX,
        }
    }

    pub fn from_boxed(iter: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send>) -> Self {
        Self {
            inner: IterSourceInner::Boxed(iter),
            peeked_key: Vec::new(),
            peeked_value: crate::types::LazyValue::empty(),
            has_peeked: false,
            level: usize::MAX,
        }
    }

    pub fn from_seekable(iter: Box<dyn SeekableIterator + Send>) -> Self {
        Self {
            inner: IterSourceInner::SeekableBoxed { iter },
            peeked_key: Vec::new(),
            peeked_value: crate::types::LazyValue::empty(),
            has_peeked: false,
            level: usize::MAX,
        }
    }

    pub fn from_memtable(iter: crate::memtable::skiplist::MemTableCursorIter) -> Self {
        Self {
            inner: IterSourceInner::Memtable { iter },
            peeked_key: Vec::new(),
            peeked_value: crate::types::LazyValue::empty(),
            has_peeked: false,
            level: usize::MAX,
        }
    }

    pub fn from_table_iter(iter: crate::sst::table_reader::TableIterator) -> Self {
        Self {
            inner: IterSourceInner::Sst { iter },
            peeked_key: Vec::new(),
            peeked_value: crate::types::LazyValue::empty(),
            has_peeked: false,
            level: usize::MAX,
        }
    }

    pub fn from_level_iter(iter: crate::iterator::LevelIterator) -> Self {
        Self {
            inner: IterSourceInner::Level { iter },
            peeked_key: Vec::new(),
            peeked_value: crate::types::LazyValue::empty(),
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
    pub fn take_peeked(&mut self) -> Option<(Vec<u8>, crate::types::LazyValue)> {
        if !self.has_peeked {
            self.has_peeked = self.advance_into_buffers();
        }
        if self.has_peeked {
            self.has_peeked = false;
            Some((
                std::mem::take(&mut self.peeked_key),
                std::mem::replace(&mut self.peeked_value, crate::types::LazyValue::empty()),
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
                    self.peeked_value = crate::types::LazyValue::Inline(v.clone());
                    *pos += 1;
                    true
                } else {
                    false
                }
            }
            IterSourceInner::Boxed(iter) => match iter.next() {
                Some((k, v)) => {
                    self.peeked_key = k;
                    self.peeked_value = crate::types::LazyValue::Inline(v);
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
                    self.peeked_value = crate::types::LazyValue::Inline(v.clone());
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
                    self.peeked_value = crate::types::LazyValue::Inline(tmp_val);
                    true
                } else {
                    false
                }
            }
            IterSourceInner::Memtable { iter } => {
                let mut tmp_val = Vec::new();
                if iter.prev_into(&mut self.peeked_key, &mut tmp_val) {
                    self.peeked_value = crate::types::LazyValue::Inline(tmp_val);
                    true
                } else {
                    false
                }
            }
            IterSourceInner::Sst { iter } => {
                let mut tmp_val = Vec::new();
                if iter.prev_into(&mut self.peeked_key, &mut tmp_val) {
                    self.peeked_value = crate::types::LazyValue::Inline(tmp_val);
                    true
                } else {
                    false
                }
            }
            IterSourceInner::Level { iter } => {
                let mut tmp_val = Vec::new();
                if iter.prev_into(&mut self.peeked_key, &mut tmp_val) {
                    self.peeked_value = crate::types::LazyValue::Inline(tmp_val);
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
                    self.peeked_value = crate::types::LazyValue::Inline(v.clone());
                    self.has_peeked = true;
                    *pos += 1;
                }
            }
            IterSourceInner::Boxed(_) => {
                self.seek(target, compare);
            }
            // Seekable variants: seek + peek next entry
            IterSourceInner::SeekableBoxed { iter } => {
                iter.seek_to(target);
                if let Some((k, v)) = iter.next() {
                    self.peeked_key = k;
                    self.peeked_value = crate::types::LazyValue::Inline(v);
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Memtable { iter } => {
                iter.seek_to(target);
                if let Some((k, v)) = iter.next() {
                    self.peeked_key = k;
                    self.peeked_value = crate::types::LazyValue::Inline(v);
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Sst { iter } => {
                iter.seek_to(target);
                if let Some((k, v)) = iter.next() {
                    self.peeked_key = k;
                    self.peeked_value = crate::types::LazyValue::Inline(v);
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Level { iter } => {
                iter.seek_to(target);
                if let Some((k, v)) = iter.next() {
                    self.peeked_key = k;
                    self.peeked_value = crate::types::LazyValue::Inline(v);
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
                    self.peeked_value = crate::types::LazyValue::Inline(v.clone());
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
                    self.peeked_value = crate::types::LazyValue::Inline(v.clone());
                    self.has_peeked = true;
                    *pos = 1;
                }
            }
            IterSourceInner::SeekableBoxed { iter } => {
                iter.seek_to_first();
                if let Some((k, v)) = iter.next() {
                    self.peeked_key = k;
                    self.peeked_value = crate::types::LazyValue::Inline(v);
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Memtable { iter } => {
                iter.seek_to_first();
                if let Some((k, v)) = iter.next() {
                    self.peeked_key = k;
                    self.peeked_value = crate::types::LazyValue::Inline(v);
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Sst { iter } => {
                iter.seek_to_first();
                if let Some((k, v)) = iter.next() {
                    self.peeked_key = k;
                    self.peeked_value = crate::types::LazyValue::Inline(v);
                    self.has_peeked = true;
                }
            }
            IterSourceInner::Level { iter } => {
                iter.seek_to_first();
                if let Some((k, v)) = iter.next() {
                    self.peeked_key = k;
                    self.peeked_value = crate::types::LazyValue::Inline(v);
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
                    self.peeked_value = crate::types::LazyValue::Inline(v.clone());
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

/// Direction of iteration for MergingIterator.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Direction {
    Forward,
    Backward,
}

/// Merges multiple sorted iterators using a heap for O(N·log K) performance.
///
/// Supports bidirectional iteration:
/// - Forward: min-heap (smallest key at top)
/// - Backward: max-heap (largest key at top)
pub struct MergingIterator<F: Fn(&[u8], &[u8]) -> Ordering> {
    sources: Vec<IterSource>,
    compare: F,
    /// Heap of source indices.
    heap: Vec<usize>,
    /// Number of valid entries in the heap.
    heap_size: usize,
    /// Whether the heap has been built.
    initialized: bool,
    /// Current direction of iteration.
    direction: Direction,
    /// Current key (for direction switching).
    current_key: Vec<u8>,
    /// Fast path: bypass heap when exactly one source exists.
    /// Models RocksDB's MergeIteratorBuilder single-source optimization.
    single_source: bool,
    /// Tracks which source indices are currently in the valid heap region.
    /// Used by direction-switch methods to avoid re-seeking sources that
    /// are already properly positioned.
    in_heap: Vec<bool>,
    /// Inclusive lower bound on user keys (for bounds propagation).
    lower_bound: Option<Vec<u8>>,
    /// Exclusive upper bound on user keys (for bounds propagation).
    upper_bound: Option<Vec<u8>>,
    /// LSM level of the source that produced the last peeked/emitted entry.
    last_source_level: usize,
}

impl<F: Fn(&[u8], &[u8]) -> Ordering> MergingIterator<F> {
    pub fn new(sources: Vec<IterSource>, compare: F) -> Self {
        let n = sources.len();
        let single = n == 1;
        Self {
            sources,
            compare,
            heap: (0..n).collect(),
            heap_size: 0,
            initialized: false,
            direction: Direction::Forward,
            single_source: single,
            current_key: Vec::new(),
            in_heap: vec![false; n],
            lower_bound: None,
            upper_bound: None,
            last_source_level: usize::MAX,
        }
    }

    /// Replace sources and reset state, reusing allocated memory.
    pub fn reset(&mut self, sources: Vec<IterSource>) {
        let n = sources.len();
        self.sources = sources;
        self.heap.clear();
        self.heap.extend(0..n);
        self.heap_size = 0;
        self.initialized = false;
        self.direction = Direction::Forward;
        self.single_source = n == 1;
        self.current_key.clear();
        self.in_heap.clear();
        self.in_heap.resize(n, false);
        self.lower_bound = None;
        self.upper_bound = None;
        self.last_source_level = usize::MAX;
    }

    /// Build the initial heap from all non-exhausted sources.
    fn init_heap(&mut self) {
        if self.initialized {
            return;
        }
        self.initialized = true;

        // Phase 1: issue prefetch hints for all seekable sources (overlaps I/O)
        for source in self.sources.iter_mut() {
            source.prefetch_hint();
        }
        // Phase 2: peek all sources (I/O should hit page cache / block cache
        // thanks to the prefetch hints issued above).
        for source in self.sources.iter_mut() {
            let _ = source.peek();
        }

        // Collect non-exhausted source indices
        let mut valid = Vec::new();
        for i in 0..self.sources.len() {
            if self.sources[i].has_peeked {
                valid.push(i);
            }
        }
        self.heap = valid;
        self.heap_size = self.heap.len();

        // Update in_heap tracking
        for flag in self.in_heap.iter_mut() {
            *flag = false;
        }
        for i in 0..self.heap_size {
            self.in_heap[self.heap[i]] = true;
        }

        // Build heap (bottom-up)
        if self.heap_size > 1 {
            let start = (self.heap_size / 2).saturating_sub(1);
            for i in (0..=start).rev() {
                self.sift_down(i);
            }
        }
    }

    /// Compare two sources by their peeked key (direction-aware).
    /// In Forward mode: true if a < b (min-heap).
    /// In Backward mode: true if a > b (max-heap).
    #[inline]
    fn source_less(&self, a: usize, b: usize) -> bool {
        let sa = &self.sources[a];
        let sb = &self.sources[b];
        match (sa.has_peeked, sb.has_peeked) {
            (true, true) => {
                let cmp = (self.compare)(sa.peeked_key.as_slice(), sb.peeked_key.as_slice());
                match self.direction {
                    Direction::Forward => cmp == Ordering::Less,
                    Direction::Backward => cmp == Ordering::Greater,
                }
            }
            (true, false) => true,
            (false, true) => false,
            (false, false) => false,
        }
    }

    fn sift_down(&mut self, mut pos: usize) {
        loop {
            let left = 2 * pos + 1;
            let right = 2 * pos + 2;
            let mut smallest = pos;

            if left < self.heap_size && self.source_less(self.heap[left], self.heap[smallest]) {
                smallest = left;
            }
            if right < self.heap_size && self.source_less(self.heap[right], self.heap[smallest]) {
                smallest = right;
            }

            if smallest == pos {
                break;
            }
            self.heap.swap(pos, smallest);
            pos = smallest;
        }
    }

    /// Get the next (key, value) pair from the merged stream (forward direction).
    pub fn next_entry(&mut self) -> Option<(Vec<u8>, crate::types::LazyValue)> {
        // Direction check must come before single-source fast path:
        // after prev_entry() sets direction=Backward, the source is
        // backward-positioned and must be re-seeked forward.
        if self.direction != Direction::Forward {
            self.switch_to_forward();
        }

        // Single-source fast path: bypass heap entirely.
        if self.single_source {
            if !self.initialized {
                self.initialized = true;
                let _ = self.sources[0].peek();
            }
            return self.sources[0].take_peeked().inspect(|entry| {
                // Track current key for direction switching (same as multi-source path).
                self.current_key.clear();
                self.current_key.extend_from_slice(&entry.0);
                self.last_source_level = self.sources[0].level;
                let _ = self.sources[0].peek();
            });
        }

        self.init_heap();

        if self.heap_size == 0 {
            return None;
        }

        // The minimum is at heap[0]
        let min_idx = self.heap[0];
        let entry = self.sources[min_idx].take_peeked()?;

        self.current_key.clear();
        self.current_key.extend_from_slice(&entry.0);
        self.last_source_level = self.sources[min_idx].level;

        // Advance the source and peek its next entry
        let _ = self.sources[min_idx].peek();

        if self.sources[min_idx].has_peeked {
            // Source still has entries — sift down to maintain heap
            self.sift_down(0);
        } else {
            // Source exhausted — remove from heap
            self.in_heap[min_idx] = false;
            self.heap_size -= 1;
            if self.heap_size > 0 {
                self.heap.swap(0, self.heap_size);
                self.sift_down(0);
            }
        }

        Some(entry)
    }

    /// Get the previous (key, value) pair from the merged stream (backward direction).
    pub fn prev_entry(&mut self) -> Option<(Vec<u8>, crate::types::LazyValue)> {
        if self.direction != Direction::Backward {
            self.switch_to_backward();
        }

        // Single-source fast path: bypass heap entirely (mirrors next_entry).
        if self.single_source {
            if !self.initialized {
                self.initialized = true;
                let _ = self.sources[0].peek();
            }
            // Check has_peeked directly — do NOT call take_peeked() which
            // would trigger forward advance_into_buffers when source is exhausted backward.
            if !self.sources[0].has_peeked {
                return None;
            }
            return self.sources[0].take_peeked().inspect(|entry| {
                self.current_key.clear();
                self.current_key.extend_from_slice(&entry.0);
                self.last_source_level = self.sources[0].level;
                self.sources[0].prev_advance();
            });
        }

        self.init_heap();

        if self.heap_size == 0 {
            return None;
        }

        // In backward mode, heap[0] is the source with the LARGEST key (max-heap)
        let max_idx = self.heap[0];
        let entry = self.sources[max_idx].take_peeked()?;

        self.current_key.clear();
        self.current_key.extend_from_slice(&entry.0);
        self.last_source_level = self.sources[max_idx].level;

        // Move this source backward
        if self.sources[max_idx].prev_advance() {
            // Source still has entries backward — sift down to maintain heap
            self.sift_down(0);
        } else {
            // Source exhausted backward — remove from heap
            self.in_heap[max_idx] = false;
            self.heap_size -= 1;
            if self.heap_size > 0 {
                self.heap.swap(0, self.heap_size);
                self.sift_down(0);
            }
        }

        Some(entry)
    }

    /// Switch from backward to forward direction.
    /// Only re-seeks sources not currently in the heap; sources still in
    /// the heap already hold valid peeked entries and just need the heap
    /// rebuilt for forward (min-heap) ordering.
    fn switch_to_forward(&mut self) {
        self.direction = Direction::Forward;
        if self.current_key.is_empty() {
            for source in self.sources.iter_mut() {
                source.seek_to_first_impl();
            }
        } else {
            // Re-seek ALL sources: in-heap sources hold peeked entries
            // from the backward (max-heap) direction and are not valid
            // for a forward (min-heap) without re-positioning.
            for source in self.sources.iter_mut() {
                source.seek_to(&self.current_key, &self.compare);
            }
        }
        self.initialized = false;
        self.init_heap();
    }

    /// Switch from forward to backward direction.
    fn switch_to_backward(&mut self) {
        self.direction = Direction::Backward;
        if self.current_key.is_empty() {
            for source in self.sources.iter_mut() {
                source.seek_to_last_impl();
            }
        } else {
            // Re-seek ALL sources: in-heap sources hold peeked entries
            // from the forward (min-heap) direction and are not valid
            // for a backward (max-heap) without re-positioning.
            for source in self.sources.iter_mut() {
                source.seek_for_prev_to(&self.current_key, &self.compare);
            }
        }
        self.initialized = false;
        self.init_heap();
    }

    /// Collect all entries.
    pub fn collect_all(&mut self) -> Vec<(Vec<u8>, crate::types::LazyValue)> {
        let mut result = Vec::new();
        while let Some(entry) = self.next_entry() {
            result.push(entry);
        }
        result
    }

    /// Seek all sources to a target key, then rebuild the heap.
    /// Uses `seek_to` for seekable sources (O(log N) binary search)
    /// instead of forward-only linear scan.
    pub fn seek(&mut self, target: &[u8]) {
        self.direction = Direction::Forward;
        for source in self.sources.iter_mut() {
            source.seek_to(target, &self.compare);
        }
        self.current_key.clear();
        if self.single_source {
            self.initialized = true;
            return;
        }
        // Rebuild heap
        self.initialized = false;
        self.init_heap();
    }

    /// Optimized seek: when `try_next` is true and the current position is <=
    /// target (forward direction), try advancing sources with up to 4 `next()`
    /// calls before falling back to a full seek.  This avoids expensive binary
    /// search + I/O when keys are nearby (e.g. sequential prefix scans).
    pub fn seek_opt(&mut self, target: &[u8], try_next: bool) {
        // Fast path: if we can try seek-using-next and we're already forward
        // with a known position <= target, attempt incremental advancement.
        if try_next
            && self.direction == Direction::Forward
            && !self.current_key.is_empty()
            && (self.compare)(self.current_key.as_slice(), target) != Ordering::Greater
        {
            const MAX_STEPS: usize = 4;

            if self.single_source {
                // Single-source: just advance until >= target or fallback
                let src = &mut self.sources[0];
                let mut stepped = 0;
                while let Some((k, _)) = src.peek() {
                    if (self.compare)(k, target) != Ordering::Less {
                        break; // already >= target
                    }
                    src.has_peeked = false;
                    stepped += 1;
                    if stepped >= MAX_STEPS {
                        // Fallback to full seek
                        src.seek_to(target, &self.compare);
                        break;
                    }
                }
                self.current_key.clear();
                return;
            }

            // Multi-source: advance each source individually
            self.init_heap();
            for i in 0..self.sources.len() {
                if !self.sources[i].has_peeked {
                    continue;
                }
                let key_less = {
                    let k = self.sources[i].peeked_key.as_slice();
                    (self.compare)(k, target) == Ordering::Less
                };
                if !key_less {
                    continue; // already >= target
                }
                // Try stepping forward
                let mut stepped = 0;
                let mut reached = false;
                loop {
                    self.sources[i].has_peeked = false;
                    stepped += 1;
                    if let Some((k, _)) = self.sources[i].peek() {
                        if (self.compare)(k, target) != Ordering::Less {
                            reached = true;
                            break;
                        }
                    } else {
                        break; // exhausted
                    }
                    if stepped >= MAX_STEPS {
                        break;
                    }
                }
                if !reached && self.sources[i].has_peeked {
                    // Still behind; fallback to full seek
                    self.sources[i].seek_to(target, &self.compare);
                } else if !reached {
                    // exhausted — seek_to for final positioning
                    self.sources[i].seek_to(target, &self.compare);
                }
            }
            // Rebuild heap
            self.initialized = false;
            self.current_key.clear();
            self.init_heap();
            return;
        }

        // Fallback to full seek
        self.seek(target);
    }

    /// Seek all sources for prev to a target key, then rebuild the max-heap.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        self.direction = Direction::Backward;
        for source in self.sources.iter_mut() {
            source.seek_for_prev_to(target, &self.compare);
        }
        self.current_key.clear();
        self.initialized = false;
        self.init_heap();
    }

    /// Peek the current minimum entry without transferring ownership.
    /// Returns references to the key and value of the smallest entry.
    /// For the single-source fast path, this is a direct reference to the source's buffer.
    #[inline]
    pub fn peek_entry(&mut self) -> Option<(&[u8], &[u8])> {
        if self.direction != Direction::Forward {
            self.switch_to_forward();
        }

        if self.single_source {
            if !self.initialized {
                self.initialized = true;
                let _ = self.sources[0].peek();
            }
            return self.sources[0].peek();
        }

        self.init_heap();
        if self.heap_size == 0 {
            return None;
        }
        let min_idx = self.heap[0];
        self.sources[min_idx].peek()
    }

    /// Advance past the current minimum entry (discard it).
    /// For single-source: clears peeked flag and re-peeks (reuses buffer capacity).
    /// For multi-source: pops heap top, advances source, sifts down.
    ///
    /// Does NOT update current_key — only take_entry() and next_entry()/prev_entry()
    /// do that. Direction switching always uses explicit seek (seek_for_prev, seek)
    /// which re-positions independently of current_key.
    #[inline]
    pub fn advance_entry(&mut self) {
        if self.single_source {
            self.last_source_level = self.sources[0].level;
            self.sources[0].skip_peeked();
            let _ = self.sources[0].peek();
            return;
        }

        if self.heap_size == 0 {
            return;
        }

        let min_idx = self.heap[0];
        self.last_source_level = self.sources[min_idx].level;
        self.sources[min_idx].skip_peeked();
        let _ = self.sources[min_idx].peek();

        if self.sources[min_idx].has_peeked {
            self.sift_down(0);
        } else {
            self.in_heap[min_idx] = false;
            self.heap_size -= 1;
            if self.heap_size > 0 {
                self.heap.swap(0, self.heap_size);
                self.sift_down(0);
            }
        }
    }

    /// Take ownership of the current minimum entry.
    /// Uses take_peeked (which resets buffer capacity to 0 for the source).
    /// Only call this for entries that will actually be returned to the caller.
    pub fn take_entry(&mut self) -> Option<(Vec<u8>, crate::types::LazyValue)> {
        if self.single_source {
            self.last_source_level = self.sources[0].level;
            let entry = self.sources[0].take_peeked()?;
            self.current_key.clear();
            self.current_key.extend_from_slice(&entry.0);
            let _ = self.sources[0].peek();
            return Some(entry);
        }

        if self.heap_size == 0 {
            return None;
        }

        let min_idx = self.heap[0];
        self.last_source_level = self.sources[min_idx].level;
        let entry = self.sources[min_idx].take_peeked()?;

        self.current_key.clear();
        self.current_key.extend_from_slice(&entry.0);

        let _ = self.sources[min_idx].peek();

        if self.sources[min_idx].has_peeked {
            self.sift_down(0);
        } else {
            self.in_heap[min_idx] = false;
            self.heap_size -= 1;
            if self.heap_size > 0 {
                self.heap.swap(0, self.heap_size);
                self.sift_down(0);
            }
        }

        Some(entry)
    }

    /// Seek all sources to first, rebuild the min-heap.
    pub fn seek_to_first(&mut self) {
        self.direction = Direction::Forward;
        for source in self.sources.iter_mut() {
            source.seek_to_first_impl();
        }
        self.current_key.clear();
        if self.single_source {
            self.initialized = true;
            return;
        }
        self.initialized = false;
        self.init_heap();
    }

    /// Seek all sources to last, rebuild the max-heap.
    pub fn seek_to_last_merge(&mut self) {
        self.direction = Direction::Backward;
        for source in self.sources.iter_mut() {
            source.seek_to_last_impl();
        }
        self.current_key.clear();
        self.initialized = false;
        self.init_heap();
    }

    /// Set iteration bounds and propagate them to all sub-iterators.
    /// `lower` is inclusive, `upper` is exclusive (user keys).
    pub fn set_bounds(&mut self, lower: Option<&[u8]>, upper: Option<&[u8]>) {
        self.lower_bound = lower.map(|b| b.to_vec());
        self.upper_bound = upper.map(|b| b.to_vec());
        for source in self.sources.iter_mut() {
            source.set_bounds(lower, upper);
        }
    }

    /// Level of the source that produced the last emitted entry.
    /// Returns `usize::MAX` if no entry has been emitted yet.
    pub fn last_source_level(&self) -> usize {
        self.last_source_level
    }

    /// Return the LSM level of the source currently at the top of the heap
    /// (i.e., the source whose entry would be returned by peek_entry).
    /// Returns `usize::MAX` if no entry is available.
    pub fn peek_source_level(&self) -> usize {
        if self.single_source {
            return self.sources[0].level;
        }
        if self.heap_size == 0 {
            return usize::MAX;
        }
        self.sources[self.heap[0]].level
    }

    /// Return the first error from any source iterator.
    /// Use after iteration returns `None` to distinguish normal exhaustion
    /// from I/O failures.
    pub fn error(&self) -> Option<String> {
        for source in &self.sources {
            if let Some(e) = source.iter_error() {
                return Some(e);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merging_iterator_basic() {
        let s1 = IterSource::new(vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"e".to_vec(), b"5".to_vec()),
        ]);
        let s2 = IterSource::new(vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
            (b"f".to_vec(), b"6".to_vec()),
        ]);

        let mut merger = MergingIterator::new(vec![s1, s2], |a, b| a.cmp(b));
        let result = merger.collect_all();

        assert_eq!(result.len(), 6);
        let keys: Vec<&[u8]> = result.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, vec![b"a", b"b", b"c", b"d", b"e", b"f"]);
    }

    #[test]
    fn test_merging_iterator_empty() {
        let mut merger: MergingIterator<_> =
            MergingIterator::new(vec![], |a: &[u8], b: &[u8]| a.cmp(b));
        assert!(merger.next_entry().is_none());
    }

    #[test]
    fn test_merging_iterator_single_source() {
        let s1 = IterSource::new(vec![
            (b"x".to_vec(), b"1".to_vec()),
            (b"y".to_vec(), b"2".to_vec()),
        ]);
        let mut merger = MergingIterator::new(vec![s1], |a, b| a.cmp(b));
        let result = merger.collect_all();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_merging_iterator_duplicates() {
        let s1 = IterSource::new(vec![
            (b"a".to_vec(), b"s1".to_vec()),
            (b"b".to_vec(), b"s1".to_vec()),
        ]);
        let s2 = IterSource::new(vec![
            (b"a".to_vec(), b"s2".to_vec()),
            (b"c".to_vec(), b"s2".to_vec()),
        ]);

        let mut merger = MergingIterator::new(vec![s1, s2], |a, b| a.cmp(b));
        let result = merger.collect_all();

        // Both "a" entries should appear (source 1 first since it's checked first)
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0, b"a");
        assert_eq!(result[0].1.as_slice(), b"s1");
        assert_eq!(result[1].0, b"a");
        assert_eq!(result[1].1.as_slice(), b"s2");
    }

    #[test]
    fn test_merging_iterator_boxed_source() {
        let data = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];
        let s1 = IterSource::from_boxed(Box::new(data.into_iter()));
        let s2 = IterSource::new(vec![(b"b".to_vec(), b"2".to_vec())]);

        let mut merger = MergingIterator::new(vec![s1, s2], |a, b| a.cmp(b));
        let result = merger.collect_all();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0, b"a");
        assert_eq!(result[1].0, b"b");
        assert_eq!(result[2].0, b"c");
    }

    #[test]
    fn test_merging_iterator_many_sources() {
        // Test with 50 sources to validate heap correctness
        let sources: Vec<IterSource> = (0..50)
            .map(|i| {
                IterSource::new(vec![
                    (format!("key_{:04}_{:02}", i * 2, i).into_bytes(), vec![]),
                    (
                        format!("key_{:04}_{:02}", i * 2 + 1, i).into_bytes(),
                        vec![],
                    ),
                ])
            })
            .collect();

        let mut merger = MergingIterator::new(sources, |a, b| a.cmp(b));
        let result = merger.collect_all();
        assert_eq!(result.len(), 100);

        // Verify sorted order
        for i in 1..result.len() {
            assert!(result[i].0 >= result[i - 1].0, "not sorted at {}", i);
        }
    }

    #[test]
    fn test_merging_iterator_seek() {
        let s1 = IterSource::new(vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"e".to_vec(), b"5".to_vec()),
        ]);
        let s2 = IterSource::new(vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
            (b"f".to_vec(), b"6".to_vec()),
        ]);

        let mut merger = MergingIterator::new(vec![s1, s2], |a, b| a.cmp(b));
        merger.seek(b"c");
        let result = merger.collect_all();
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0, b"c");
        assert_eq!(result[1].0, b"d");
    }

    #[test]
    fn test_single_source_direction_switch() {
        // Bug 2 regression: single_source fast path must respect direction changes.
        // After prev_entry() sets direction=Backward, next_entry() must re-seek forward.
        let s1 = IterSource::new(vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ]);

        let mut merger = MergingIterator::new(vec![s1], |a, b| a.cmp(b));

        // Forward: get "a", "b", "c"
        assert_eq!(merger.next_entry().unwrap().0, b"a");
        assert_eq!(merger.next_entry().unwrap().0, b"b");
        assert_eq!(merger.next_entry().unwrap().0, b"c");
        // current_key = "c"

        // Backward: switch_to_backward re-seeks to "c" (seek_for_prev),
        // peeked = "c", prev_entry returns "c"
        assert_eq!(merger.prev_entry().unwrap().0, b"c");
        // prev_advance moves to "b"
        assert_eq!(merger.prev_entry().unwrap().0, b"b");
        // current_key = "b"

        // Forward again: switch_to_forward re-seeks to "b",
        // stream resumes from "b" forward.
        // Without the fix, direction wouldn't be switched and this would fail.
        assert_eq!(merger.next_entry().unwrap().0, b"b");
        assert_eq!(merger.next_entry().unwrap().0, b"c");
        assert_eq!(merger.next_entry().unwrap().0, b"d");
        assert!(merger.next_entry().is_none());
    }
}

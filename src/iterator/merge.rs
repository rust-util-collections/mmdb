//! MergingIterator: merges multiple sorted iterators into a single sorted stream.
//!
//! Uses a min-heap for O(N·log K) performance instead of O(N·K) linear scan.

use std::cmp::Ordering;

use crate::types::LazyValue;

pub use crate::iterator::source::{IterSource, SeekableIterator};

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
        }
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
    pub fn next_entry(&mut self) -> Option<(Vec<u8>, LazyValue)> {
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
    pub fn prev_entry(&mut self) -> Option<(Vec<u8>, LazyValue)> {
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
    #[cfg(test)]
    pub fn collect_all(&mut self) -> Vec<(Vec<u8>, LazyValue)> {
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
        // with a known position strictly before target, attempt incremental advancement.
        if try_next
            && self.direction == Direction::Forward
            && !self.current_key.is_empty()
            && (self.compare)(self.current_key.as_slice(), target) == Ordering::Less
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
                if !reached {
                    // Not yet at target — fallback to full seek
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
            self.sources[0].skip_peeked();
            let _ = self.sources[0].peek();
            return;
        }

        if self.heap_size == 0 {
            return;
        }

        let min_idx = self.heap[0];
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
    pub fn take_entry(&mut self) -> Option<(Vec<u8>, LazyValue)> {
        if self.single_source {
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

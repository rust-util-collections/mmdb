//! MergingIterator: merges multiple sorted iterators into a single sorted stream.
//!
//! Uses a min-heap for O(N·log K) performance instead of O(N·K) linear scan.

use std::cmp::Ordering;

/// A source of sorted (key, value) pairs — can be backed by a Vec or a streaming iterator.
pub struct IterSource {
    inner: IterSourceInner,
    /// Peeked entry (buffered for comparison).
    pub(crate) peeked: Option<(Vec<u8>, Vec<u8>)>,
}

enum IterSourceInner {
    Vec {
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        pos: usize,
    },
    Boxed(Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>),
}

impl IterSource {
    pub fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self {
            inner: IterSourceInner::Vec { entries, pos: 0 },
            peeked: None,
        }
    }

    pub fn from_boxed(iter: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>) -> Self {
        Self {
            inner: IterSourceInner::Boxed(iter),
            peeked: None,
        }
    }

    pub fn peek(&mut self) -> Option<(&[u8], &[u8])> {
        if self.peeked.is_none() {
            self.peeked = self.advance_inner();
        }
        self.peeked
            .as_ref()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
    }

    pub fn take_peeked(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.peeked.is_none() {
            self.peeked = self.advance_inner();
        }
        self.peeked.take()
    }

    fn advance_inner(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        match &mut self.inner {
            IterSourceInner::Vec { entries, pos } => {
                if *pos < entries.len() {
                    let entry = entries[*pos].clone();
                    *pos += 1;
                    Some(entry)
                } else {
                    None
                }
            }
            IterSourceInner::Boxed(iter) => iter.next(),
        }
    }

    pub fn is_exhausted(&mut self) -> bool {
        self.peek().is_none()
    }

    /// Seek forward until the current key >= target according to `compare`.
    pub fn seek<F: Fn(&[u8], &[u8]) -> Ordering>(&mut self, target: &[u8], compare: &F) {
        // Discard peeked if < target
        loop {
            match self.peek() {
                Some((k, _)) if compare(k, target) == Ordering::Less => {
                    self.peeked = None;
                    // Advance inner
                    self.peeked = self.advance_inner();
                    if self.peeked.is_none() {
                        break;
                    }
                }
                _ => break,
            }
        }
    }
}

/// Merges multiple sorted iterators using a min-heap for O(N·log K) performance.
pub struct MergingIterator<F: Fn(&[u8], &[u8]) -> Ordering> {
    sources: Vec<IterSource>,
    compare: F,
    /// Min-heap of source indices. heap[0] is the source with the smallest current key.
    heap: Vec<usize>,
    /// Number of valid entries in the heap.
    heap_size: usize,
    /// Whether the heap has been built.
    initialized: bool,
}

impl<F: Fn(&[u8], &[u8]) -> Ordering> MergingIterator<F> {
    pub fn new(sources: Vec<IterSource>, compare: F) -> Self {
        let n = sources.len();
        Self {
            sources,
            compare,
            heap: (0..n).collect(),
            heap_size: 0,
            initialized: false,
        }
    }

    /// Build the initial heap from all non-exhausted sources.
    fn init_heap(&mut self) {
        if self.initialized {
            return;
        }
        self.initialized = true;

        // Ensure all sources have a peeked value
        for source in self.sources.iter_mut() {
            let _ = source.peek();
        }

        // Collect non-exhausted source indices
        let mut valid = Vec::new();
        for i in 0..self.sources.len() {
            if self.sources[i].peeked.is_some() {
                valid.push(i);
            }
        }
        self.heap = valid;
        self.heap_size = self.heap.len();

        // Build heap (bottom-up)
        if self.heap_size > 1 {
            let start = (self.heap_size / 2).saturating_sub(1);
            for i in (0..=start).rev() {
                self.sift_down(i);
            }
        }
    }

    /// Compare two sources by their peeked key.
    #[inline]
    fn source_less(&self, a: usize, b: usize) -> bool {
        let ka = self.sources[a].peeked.as_ref().map(|(k, _)| k.as_slice());
        let kb = self.sources[b].peeked.as_ref().map(|(k, _)| k.as_slice());
        match (ka, kb) {
            (Some(a), Some(b)) => (self.compare)(a, b) == Ordering::Less,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => false,
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

    /// Get the next (key, value) pair from the merged stream.
    pub fn next_entry(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.init_heap();

        if self.heap_size == 0 {
            return None;
        }

        // The minimum is at heap[0]
        let min_idx = self.heap[0];
        let entry = self.sources[min_idx].take_peeked()?;

        // Advance the source and peek its next entry
        let _ = self.sources[min_idx].peek();

        if self.sources[min_idx].peeked.is_some() {
            // Source still has entries — sift down to maintain heap
            self.sift_down(0);
        } else {
            // Source exhausted — remove from heap
            self.heap_size -= 1;
            if self.heap_size > 0 {
                self.heap.swap(0, self.heap_size);
                self.sift_down(0);
            }
        }

        Some(entry)
    }

    /// Collect all entries.
    pub fn collect_all(&mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        while let Some(entry) = self.next_entry() {
            result.push(entry);
        }
        result
    }

    /// Seek all sources to a target key, then rebuild the heap.
    pub fn seek(&mut self, target: &[u8]) {
        for source in self.sources.iter_mut() {
            source.seek(target, &self.compare);
        }
        // Rebuild heap
        self.initialized = false;
        self.init_heap();
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
        assert_eq!(result[0], (b"a".to_vec(), b"s1".to_vec()));
        assert_eq!(result[1], (b"a".to_vec(), b"s2".to_vec()));
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
}

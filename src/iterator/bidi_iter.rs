//! BidiIterator: a bidirectional iterator that implements `DoubleEndedIterator`.
//!
//! Materializes visible entries at construction time (via the existing DBIterator
//! forward pipeline), then supports both `next()` and `next_back()` with cursor
//! crossing detection.

/// A bidirectional iterator over materialized (user_key, value) pairs.
///
/// Supports both forward (`next()`) and reverse (`next_back()`) iteration
/// with automatic cursor crossing detection (forward >= reverse → done).
pub struct BidiIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    front: usize,
    /// Exclusive upper bound for reverse iteration.
    back: usize,
}

impl BidiIterator {
    /// Create from a pre-sorted, deduplicated list of (user_key, value) pairs.
    pub fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        let len = entries.len();
        Self {
            entries,
            front: 0,
            back: len,
        }
    }

    /// Number of remaining entries (forward + reverse combined).
    pub fn remaining(&self) -> usize {
        self.back.saturating_sub(self.front)
    }

    /// Whether the iterator has been exhausted.
    pub fn is_empty(&self) -> bool {
        self.front >= self.back
    }
}

impl Iterator for BidiIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.front < self.back {
            let item = self.entries[self.front].clone();
            self.front += 1;
            Some(item)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.remaining();
        (r, Some(r))
    }
}

impl DoubleEndedIterator for BidiIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.back > self.front {
            self.back -= 1;
            Some(self.entries[self.back].clone())
        } else {
            None
        }
    }
}

impl ExactSizeIterator for BidiIterator {}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entries(keys: &[&[u8]]) -> Vec<(Vec<u8>, Vec<u8>)> {
        keys.iter()
            .map(|k| {
                (
                    k.to_vec(),
                    format!("val_{}", String::from_utf8_lossy(k)).into_bytes(),
                )
            })
            .collect()
    }

    #[test]
    fn test_forward_only() {
        let entries = make_entries(&[b"a", b"b", b"c"]);
        let mut it = BidiIterator::new(entries);
        assert_eq!(it.next().unwrap().0, b"a");
        assert_eq!(it.next().unwrap().0, b"b");
        assert_eq!(it.next().unwrap().0, b"c");
        assert!(it.next().is_none());
    }

    #[test]
    fn test_reverse_only() {
        let entries = make_entries(&[b"a", b"b", b"c"]);
        let mut it = BidiIterator::new(entries);
        assert_eq!(it.next_back().unwrap().0, b"c");
        assert_eq!(it.next_back().unwrap().0, b"b");
        assert_eq!(it.next_back().unwrap().0, b"a");
        assert!(it.next_back().is_none());
    }

    #[test]
    fn test_interleaved() {
        let entries = make_entries(&[b"a", b"b", b"c", b"d"]);
        let mut it = BidiIterator::new(entries);

        assert_eq!(it.next().unwrap().0, b"a"); // front=1
        assert_eq!(it.next_back().unwrap().0, b"d"); // back=3
        assert_eq!(it.next().unwrap().0, b"b"); // front=2
        assert_eq!(it.next_back().unwrap().0, b"c"); // back=2
        // Cursors meet → done
        assert!(it.next().is_none());
        assert!(it.next_back().is_none());
    }

    #[test]
    fn test_cursor_crossing() {
        let entries = make_entries(&[b"a", b"b", b"c", b"d", b"e"]);
        let mut it = BidiIterator::new(entries);

        assert_eq!(it.next().unwrap().0, b"a");
        assert_eq!(it.next().unwrap().0, b"b");
        assert_eq!(it.next_back().unwrap().0, b"e");
        assert_eq!(it.next_back().unwrap().0, b"d");
        // Only c remains
        assert_eq!(it.remaining(), 1);
        assert_eq!(it.next().unwrap().0, b"c");
        assert!(it.next().is_none());
        assert!(it.next_back().is_none());
    }

    #[test]
    fn test_empty() {
        let mut it = BidiIterator::new(vec![]);
        assert!(it.is_empty());
        assert!(it.next().is_none());
        assert!(it.next_back().is_none());
    }

    #[test]
    fn test_single_entry() {
        let entries = make_entries(&[b"x"]);
        let mut it = BidiIterator::new(entries.clone());
        assert_eq!(it.next().unwrap().0, b"x");
        assert!(it.next().is_none());

        let mut it = BidiIterator::new(entries);
        assert_eq!(it.next_back().unwrap().0, b"x");
        assert!(it.next_back().is_none());
    }

    #[test]
    fn test_exact_size() {
        let entries = make_entries(&[b"a", b"b", b"c"]);
        let mut it = BidiIterator::new(entries);
        assert_eq!(it.len(), 3);
        it.next();
        assert_eq!(it.len(), 2);
        it.next_back();
        assert_eq!(it.len(), 1);
    }
}

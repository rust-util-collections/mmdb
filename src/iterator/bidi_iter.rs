//! BidiIterator: a bidirectional iterator that implements `DoubleEndedIterator`.
//!
//! Supports two modes:
//! - **Materialized**: pre-sorted entries for immediate bidirectional access
//! - **Lazy**: wraps a `DBIterator` for streaming forward-only access;
//!   materializes on first backward access (`next_back()`).
//!
//! The lazy mode avoids O(N) startup cost for forward-only iteration patterns.

use crate::iterator::db_iter::DBIterator;

/// A bidirectional iterator over (user_key, value) pairs.
///
/// Supports both forward (`next()`) and reverse (`next_back()`) iteration
/// with automatic cursor crossing detection (forward >= reverse → done).
pub struct BidiIterator {
    inner: BidiInner,
}

enum BidiInner {
    /// Pre-materialized entries with front/back cursors.
    Materialized {
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        front: usize,
        /// Exclusive upper bound for reverse iteration.
        back: usize,
    },
    /// Lazy streaming via DBIterator. Forward-only; materializes on first next_back().
    Lazy {
        db_iter: Box<DBIterator>,
        /// Entries consumed so far via next() (for materialization on backward access).
        consumed: Vec<(Vec<u8>, Vec<u8>)>,
    },
}

impl BidiIterator {
    /// Create from a pre-sorted, deduplicated list of (user_key, value) pairs.
    pub fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        let len = entries.len();
        Self {
            inner: BidiInner::Materialized {
                entries,
                front: 0,
                back: len,
            },
        }
    }

    /// Create a lazy streaming iterator from a DBIterator.
    ///
    /// Forward iteration (`next()`) is streamed without materialization.
    /// On first backward access (`next_back()`), remaining entries are collected
    /// and the iterator switches to materialized mode.
    pub fn lazy(db_iter: DBIterator) -> Self {
        Self {
            inner: BidiInner::Lazy {
                db_iter: Box::new(db_iter),
                consumed: Vec::new(),
            },
        }
    }

    /// Number of remaining entries (forward + reverse combined).
    /// Only available in materialized mode; returns 0 in lazy mode.
    pub fn remaining(&self) -> usize {
        match &self.inner {
            BidiInner::Materialized { front, back, .. } => back.saturating_sub(*front),
            BidiInner::Lazy { .. } => 0,
        }
    }

    /// Whether the iterator has been exhausted.
    pub fn is_empty(&self) -> bool {
        match &self.inner {
            BidiInner::Materialized { front, back, .. } => front >= back,
            BidiInner::Lazy { .. } => false, // Can't know without consuming
        }
    }

    /// Force materialization if in lazy mode. After this call, `remaining()` is accurate.
    fn materialize(&mut self) {
        let old = std::mem::replace(
            &mut self.inner,
            BidiInner::Materialized {
                entries: Vec::new(),
                front: 0,
                back: 0,
            },
        );

        if let BidiInner::Lazy {
            mut db_iter,
            consumed,
        } = old
        {
            let front = consumed.len(); // Already consumed via next()
            let mut entries = consumed;
            for entry in db_iter.by_ref() {
                entries.push(entry);
            }
            let len = entries.len();
            self.inner = BidiInner::Materialized {
                entries,
                front,
                back: len,
            };
        } else {
            // Already materialized — put it back
            self.inner = old;
        }
    }
}

impl Iterator for BidiIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            BidiInner::Materialized {
                entries,
                front,
                back,
            } => {
                if *front < *back {
                    let item = entries[*front].clone();
                    *front += 1;
                    Some(item)
                } else {
                    None
                }
            }
            BidiInner::Lazy {
                db_iter, consumed, ..
            } => {
                let entry = db_iter.next()?;
                consumed.push(entry.clone());
                Some(entry)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            BidiInner::Materialized { front, back, .. } => {
                let r = back.saturating_sub(*front);
                (r, Some(r))
            }
            BidiInner::Lazy { .. } => (0, None),
        }
    }
}

impl DoubleEndedIterator for BidiIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        // Materialize on first backward access
        if matches!(self.inner, BidiInner::Lazy { .. }) {
            self.materialize();
        }

        match &mut self.inner {
            BidiInner::Materialized {
                entries,
                front,
                back,
            } => {
                if *back > *front {
                    *back -= 1;
                    Some(entries[*back].clone())
                } else {
                    None
                }
            }
            BidiInner::Lazy { .. } => {
                unreachable!("materialize should have converted to Materialized")
            }
        }
    }
}

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
        assert_eq!(it.remaining(), 3);
        it.next();
        assert_eq!(it.remaining(), 2);
        it.next_back();
        assert_eq!(it.remaining(), 1);
    }
}

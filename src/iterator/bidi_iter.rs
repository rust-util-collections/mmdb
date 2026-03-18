//! BidiIterator: a bidirectional iterator that implements `DoubleEndedIterator`.
//!
//! Supports two modes:
//! - **Materialized**: pre-sorted entries for immediate bidirectional access
//! - **Lazy**: wraps a `DBIterator` for streaming forward iteration.
//!   First `next_back()` uses `seek_to_last()` (O(log N)).
//!   Subsequent `next_back()` calls use `db_iter.prev()` for O(1) memory streaming.

use crate::iterator::db_iter::DBIterator;

/// A bidirectional iterator over (user_key, value) pairs.
pub struct BidiIterator {
    inner: BidiInner,
}

enum BidiInner {
    /// Pre-materialized entries with front/back cursors.
    Materialized {
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        front: usize,
        back: usize,
    },
    /// Lazy streaming via DBIterator. Forward `next()` is zero-overhead
    /// (no clone into consumed buffer). Backward access on first `next_back()`
    /// uses `seek_to_last()` (O(log N)).
    Lazy {
        db_iter: Box<DBIterator>,
        /// When true, skip backward support (pure forward streaming).
        forward_only: bool,
        /// Number of entries consumed via next() (for correct front cursor on materialize).
        fwd_count: usize,
    },
    /// After first next_back() via seek_to_last(), subsequent next_back() calls
    /// use db_iter.prev() for O(1) memory streaming backward.
    LazyBackStarted {
        db_iter: Box<DBIterator>,
        /// Number of entries consumed via next() before the first next_back().
        fwd_count: usize,
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
    pub fn lazy(db_iter: DBIterator) -> Self {
        Self {
            inner: BidiInner::Lazy {
                db_iter: Box::new(db_iter),
                forward_only: false,
                fwd_count: 0,
            },
        }
    }

    /// Create a forward-only lazy streaming iterator.
    /// `next_back()` returns `None`.
    pub fn lazy_forward(db_iter: DBIterator) -> Self {
        Self {
            inner: BidiInner::Lazy {
                db_iter: Box::new(db_iter),
                forward_only: true,
                fwd_count: 0,
            },
        }
    }

    /// Number of remaining entries. Only accurate in materialized mode.
    pub fn remaining(&self) -> usize {
        match &self.inner {
            BidiInner::Materialized { front, back, .. } => back.saturating_sub(*front),
            _ => 0,
        }
    }

    /// Whether the iterator has been exhausted.
    pub fn is_empty(&self) -> bool {
        match &self.inner {
            BidiInner::Materialized { front, back, .. } => front >= back,
            _ => false,
        }
    }

    /// Materialize from LazyBackStarted: re-seek to start, collect all entries,
    /// then set cursors to exclude already-consumed entries from both ends.
    /// This is the fallback path for mixed forward+backward iteration.
    fn materialize_from_back_started(
        db_iter: &mut DBIterator,
        fwd_count: usize,
        back_key: Option<&[u8]>,
    ) -> BidiInner {
        // Reset iterator state and collect everything from scratch.
        db_iter.reset_and_seek_to_first();
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for entry in db_iter.by_ref() {
            entries.push(entry);
        }
        let len = entries.len();
        // Find where the backward cursor is — everything at and after back_key was
        // already returned via next_back(), so back cursor stops there.
        let back = match back_key {
            Some(key) => entries.iter().rposition(|e| e.0 == key).unwrap_or(len),
            None => 0, // All entries consumed backward
        };
        BidiInner::Materialized {
            entries,
            front: fwd_count,
            back,
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
                db_iter, fwd_count, ..
            } => {
                let entry = db_iter.next()?;
                *fwd_count += 1;
                Some(entry)
            }
            BidiInner::LazyBackStarted { .. } => {
                // Mixed forward+backward: materialize then use Materialized path.
                // Extract the current backward cursor position for materialization.
                let old = std::mem::replace(
                    &mut self.inner,
                    BidiInner::Materialized {
                        entries: Vec::new(),
                        front: 0,
                        back: 0,
                    },
                );
                if let BidiInner::LazyBackStarted {
                    mut db_iter,
                    fwd_count,
                } = old
                {
                    // The db_iter is positioned at the current backward entry.
                    // Get its key for materialization cursor positioning.
                    let back_key = if db_iter.valid() {
                        Some(db_iter.key().to_vec())
                    } else {
                        None
                    };
                    self.inner = Self::materialize_from_back_started(
                        &mut db_iter,
                        fwd_count,
                        back_key.as_deref(),
                    );
                }
                self.next()
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            BidiInner::Materialized { front, back, .. } => {
                let r = back.saturating_sub(*front);
                (r, Some(r))
            }
            _ => (0, None),
        }
    }
}

impl DoubleEndedIterator for BidiIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
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
            BidiInner::Lazy { forward_only, .. } if *forward_only => None,
            BidiInner::Lazy { .. } => {
                // First next_back(): use seek_to_last() for O(log N) access.
                // This is the hot path for vsdb's `last()` = `iter().next_back()`.
                let old = std::mem::replace(
                    &mut self.inner,
                    BidiInner::Materialized {
                        entries: Vec::new(),
                        front: 0,
                        back: 0,
                    },
                );
                let BidiInner::Lazy {
                    mut db_iter,
                    fwd_count,
                    ..
                } = old
                else {
                    unreachable!()
                };

                db_iter.seek_to_last();
                if !db_iter.valid() {
                    return None;
                }
                let k = db_iter.key().to_vec();
                let v = db_iter.value().to_vec();

                self.inner = BidiInner::LazyBackStarted {
                    db_iter,
                    fwd_count,
                };

                Some((k, v))
            }
            BidiInner::LazyBackStarted { db_iter, .. } => {
                // Second+ next_back(): stream backward via db_iter.prev().
                // O(1) memory — no materialization needed.
                db_iter.prev();
                if db_iter.valid() {
                    let k = db_iter.key().to_vec();
                    let v = db_iter.value().to_vec();
                    Some((k, v))
                } else {
                    None
                }
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
        assert_eq!(it.next().unwrap().0, b"a");
        assert_eq!(it.next_back().unwrap().0, b"d");
        assert_eq!(it.next().unwrap().0, b"b");
        assert_eq!(it.next_back().unwrap().0, b"c");
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

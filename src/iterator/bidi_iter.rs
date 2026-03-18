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
        /// Snapshot of db_iter.last_user_key() at the Lazy→LazyBackStarted transition.
        /// Used for re-seeking forward on direction change.
        last_fwd_key: Option<Vec<u8>>,
        /// Last key returned by next_back(), used as forward stop boundary.
        last_back_key: Option<Vec<u8>>,
    },
    /// After calling next() while in LazyBackStarted, the db_iter is re-seeked
    /// forward with upper_bound set. Streams forward with O(1) memory.
    /// next_back() returns None (forward-only after direction switch).
    LazyFwdResumed { db_iter: Box<DBIterator> },
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
                // Switch to streaming forward — O(1) memory, no collect().
                let placeholder = BidiInner::Materialized {
                    entries: Vec::new(),
                    front: 0,
                    back: 0,
                };
                let old = std::mem::replace(&mut self.inner, placeholder);
                if let BidiInner::LazyBackStarted {
                    mut db_iter,
                    last_fwd_key,
                    last_back_key,
                    ..
                } = old
                {
                    // Re-seek forward past the last consumed forward key.
                    match &last_fwd_key {
                        Some(key) => {
                            db_iter.seek(key);
                            // Skip last_fwd_key itself (already returned via next()).
                            if db_iter.valid() && db_iter.key() == key.as_slice() {
                                db_iter.advance();
                            }
                        }
                        None => {
                            // Never called next() before backward mode — start from beginning.
                            db_iter.seek_to_first();
                        }
                    }

                    // Set upper bound to stop before already-returned backward entries.
                    if let Some(ref back_key) = last_back_key {
                        db_iter.set_upper_bound(back_key.clone());
                    }

                    // Stream forward — no materialization needed.
                    self.inner = BidiInner::LazyFwdResumed { db_iter };
                    self.next()
                } else {
                    unreachable!()
                }
            }
            BidiInner::LazyFwdResumed { db_iter } => db_iter.next(),
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
                let placeholder = BidiInner::Materialized {
                    entries: Vec::new(),
                    front: 0,
                    back: 0,
                };
                let old = std::mem::replace(&mut self.inner, placeholder);
                let BidiInner::Lazy { mut db_iter, .. } = old else {
                    unreachable!()
                };

                // Snapshot last_user_key before seek_to_last destroys it
                let last_fwd_key = db_iter.last_user_key().map(|k| k.to_vec());

                db_iter.seek_to_last();
                if !db_iter.valid() {
                    return None;
                }
                let k = db_iter.key().to_vec();
                let v = db_iter.value().to_vec();

                self.inner = BidiInner::LazyBackStarted {
                    db_iter,
                    last_fwd_key,
                    last_back_key: Some(k.clone()),
                };

                Some((k, v))
            }
            BidiInner::LazyFwdResumed { .. } => {
                // Need backward access — materialize remaining forward window
                // (bounded by upper_bound already set on db_iter).
                let placeholder = BidiInner::Materialized {
                    entries: Vec::new(),
                    front: 0,
                    back: 0,
                };
                let old = std::mem::replace(&mut self.inner, placeholder);
                let BidiInner::LazyFwdResumed { mut db_iter } = old else {
                    unreachable!()
                };
                let entries: Vec<(Vec<u8>, Vec<u8>)> = db_iter.by_ref().collect();
                let len = entries.len();
                self.inner = BidiInner::Materialized {
                    entries,
                    front: 0,
                    back: len,
                };
                self.next_back()
            }
            BidiInner::LazyBackStarted {
                db_iter,
                last_fwd_key,
                last_back_key,
            } => {
                // Second+ next_back(): stream backward via db_iter.prev().
                // O(1) memory — no materialization needed.
                db_iter.prev();
                if db_iter.valid() {
                    let k = db_iter.key().to_vec();
                    let v = db_iter.value().to_vec();
                    // Stop if backward cursor has crossed the forward frontier.
                    // Keep last_back_key unchanged — it's the correct upper bound
                    // for a subsequent next() materialization.
                    if let Some(fk) = last_fwd_key.as_deref()
                        && k.as_slice() <= fk
                    {
                        return None;
                    }
                    *last_back_key = Some(k.clone());
                    Some((k, v))
                } else {
                    // Keep last_back_key — it's the correct upper bound for a
                    // subsequent next() so it won't re-stream consumed entries.
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

    // Helper: create a lazy BidiIterator from user keys using DBIterator internals.
    fn make_lazy_bidi(keys: &[&[u8]]) -> BidiIterator {
        use crate::types::{InternalKey, ValueType};
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let ik = InternalKey::new(k, (i + 1) as u64, ValueType::Value);
                (ik.into_bytes(), format!("val_{}", i).into_bytes())
            })
            .collect();
        entries.sort_by(|(a, _), (b, _)| crate::types::compare_internal_key(a, b));
        let db_iter = DBIterator::new(vec![entries], 100);
        BidiIterator::lazy(db_iter)
    }

    #[test]
    fn test_lazy_forward_backward_forward() {
        // Bug 5 regression: mixed forward+backward must NOT OOM.
        let mut it = make_lazy_bidi(&[b"a", b"b", b"c", b"d", b"e", b"f"]);

        // Forward: consume a, b, c
        assert_eq!(it.next().unwrap().0, b"a");
        assert_eq!(it.next().unwrap().0, b"b");
        assert_eq!(it.next().unwrap().0, b"c");

        // Backward: consume f, e
        assert_eq!(it.next_back().unwrap().0, b"f");
        assert_eq!(it.next_back().unwrap().0, b"e");

        // Forward again (re-seek): should get d (between c and e)
        assert_eq!(it.next().unwrap().0, b"d");
        // d is the last entry between front (c) and back (e), so next should be None
        assert!(it.next().is_none());
    }

    #[test]
    fn test_lazy_back_then_forward_immediately() {
        // next_back() first (no prior next()), then next() to end.
        let mut it = make_lazy_bidi(&[b"a", b"b", b"c", b"d"]);

        // Back first: get d
        assert_eq!(it.next_back().unwrap().0, b"d");

        // Forward: should start from beginning, stop before d
        assert_eq!(it.next().unwrap().0, b"a");
        assert_eq!(it.next().unwrap().0, b"b");
        assert_eq!(it.next().unwrap().0, b"c");
        assert!(it.next().is_none());
    }

    #[test]
    fn test_lazy_interleaved_exhaustion() {
        // Alternate next/next_back until both sides meet.
        let mut it = make_lazy_bidi(&[b"a", b"b", b"c", b"d"]);

        assert_eq!(it.next().unwrap().0, b"a");
        assert_eq!(it.next_back().unwrap().0, b"d");
        assert_eq!(it.next_back().unwrap().0, b"c");
        // Now forward should get b, then stop (upper bound is c)
        assert_eq!(it.next().unwrap().0, b"b");
        assert!(it.next().is_none());
    }
}

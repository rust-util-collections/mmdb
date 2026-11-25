//! Streaming double-ended iteration over the unconsumed key range.

use crate::{DBIterator, Error, Result};

/// A double-ended iterator over visible user keys and values.
pub struct BidiIterator {
    inner: BidiInner,
}

enum BidiInner {
    Materialized {
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        front: usize,
        back: usize,
    },
    Lazy {
        cursor: Box<DBIterator>,
        forward_only: bool,
        backward: bool,
        /// Exclusive upper frontier after consuming a row from the back.
        back: Option<Vec<u8>>,
    },
}

impl BidiIterator {
    /// Create from sorted, deduplicated entries.
    pub fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self {
            inner: BidiInner::Materialized {
                back: entries.len(),
                entries,
                front: 0,
            },
        }
    }

    /// Stream the remaining window of an already positioned database iterator.
    pub fn lazy(db_iter: DBIterator) -> Self {
        Self::stream(db_iter, false)
    }

    /// Stream forward only. Reverse steps report exhaustion.
    pub fn lazy_forward(db_iter: DBIterator) -> Self {
        Self::stream(db_iter, true)
    }

    fn stream(cursor: DBIterator, forward_only: bool) -> Self {
        Self {
            inner: BidiInner::Lazy {
                cursor: Box::new(cursor),
                forward_only,
                backward: false,
                back: None,
            },
        }
    }

    /// Exact remaining count for materialized iterators. Lazy iterators return zero.
    pub fn remaining(&self) -> usize {
        self.size_hint().0
    }

    /// True when a materialized iterator has consumed its entire range.
    pub fn is_empty(&self) -> bool {
        self.size_hint().1 == Some(0)
    }

    /// First error encountered by the underlying database cursor.
    pub fn error(&self) -> Option<&Error> {
        match &self.inner {
            BidiInner::Materialized { .. } => None,
            BidiInner::Lazy { cursor, .. } => cursor.error(),
        }
    }

    /// Consume the front row of the remaining window and borrow its bytes.
    pub fn step_next(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.front(true)
    }

    fn front(&mut self, checked: bool) -> Result<Option<(&[u8], &[u8])>> {
        match &mut self.inner {
            BidiInner::Materialized {
                entries,
                front,
                back,
            } => {
                if front >= back {
                    return Ok(None);
                }
                let (key, value) = &entries[*front];
                *front += 1;
                Ok(Some((key, value)))
            }
            BidiInner::Lazy {
                cursor,
                backward,
                back,
                ..
            } => {
                if *backward {
                    if let Some(bound) = back {
                        cursor.set_upper_bound(bound.clone());
                    }
                    cursor.seek_to_first();
                    *backward = false;
                }
                cursor.lend_next(checked)
            }
        }
    }

    /// Consume the back row of the remaining window and borrow its bytes.
    pub fn step_prev(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        match &mut self.inner {
            BidiInner::Materialized {
                entries,
                front,
                back,
            } => {
                if front >= back {
                    return Ok(None);
                }
                *back -= 1;
                let (key, value) = &entries[*back];
                Ok(Some((key, value)))
            }
            BidiInner::Lazy {
                cursor,
                backward,
                back,
                forward_only,
            } => {
                if let Some(error) = cursor.error().cloned() {
                    cursor.halt_on_error();
                    return Err(error);
                }
                if *forward_only {
                    return Ok(None);
                }
                if !*backward {
                    // Preserve the next unconsumed forward key, including any
                    // initial seek position, before repositioning to the back.
                    let Some(first) = cursor.key().map(<[u8]>::to_vec) else {
                        return cursor.error().map_or(Ok(None), |e| Err(e.clone()));
                    };
                    cursor.set_lower_bound(first);
                    if let Some(last) = back {
                        cursor.set_upper_bound(last.clone());
                    }
                    cursor.seek_to_last();
                    *backward = true;
                }
                let row = cursor.step_prev()?;
                if let Some((key, _)) = row {
                    let frontier = back.get_or_insert_with(Vec::new);
                    frontier.clear();
                    frontier.extend_from_slice(key);
                }
                Ok(row)
            }
        }
    }
}

impl Iterator for BidiIterator {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        self.front(false)
            .ok()
            .flatten()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            BidiInner::Materialized { front, back, .. } => (back - front, Some(back - front)),
            _ => (0, None),
        }
    }
}

impl DoubleEndedIterator for BidiIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.step_prev()
            .ok()
            .flatten()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
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
    fn test_lazy_repeated_direction_switches_stay_streaming() {
        let mut it = make_lazy_bidi(&[b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"]);

        assert_eq!(it.next_back().unwrap().0, b"h");
        assert!(matches!(it.inner, BidiInner::Lazy { backward: true, .. }));
        assert_eq!(it.next().unwrap().0, b"a");
        assert!(matches!(
            it.inner,
            BidiInner::Lazy {
                backward: false,
                ..
            }
        ));
        assert_eq!(it.next_back().unwrap().0, b"g");
        assert!(matches!(it.inner, BidiInner::Lazy { backward: true, .. }));
        assert_eq!(it.next().unwrap().0, b"b");
        assert_eq!(it.next_back().unwrap().0, b"f");
        assert_eq!(it.next().unwrap().0, b"c");
        assert_eq!(it.next_back().unwrap().0, b"e");
        assert_eq!(it.next().unwrap().0, b"d");
        assert!(it.next_back().is_none());
        assert!(it.next().is_none());
        assert!(
            !matches!(it.inner, BidiInner::Materialized { .. }),
            "lazy direction switches must never materialize the remaining window"
        );
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

    #[test]
    fn test_error_none_for_materialized() {
        let it = BidiIterator::new(make_entries(&[b"a", b"b"]));
        assert!(it.error().is_none());
    }

    #[test]
    fn test_error_none_after_clean_lazy_exhaustion() {
        // Forward and backward exhaustion with no underlying failure must
        // both report no error.
        let mut it = make_lazy_bidi(&[b"a", b"b"]);
        while it.next().is_some() {}
        assert!(it.error().is_none());

        let mut it = make_lazy_bidi(&[b"a", b"b"]);
        while it.next_back().is_some() {}
        assert!(it.error().is_none());
    }

    /// Regression: a key too short to decode as an `InternalKey` must not
    /// look like a legitimately empty range. `next_back()`'s first call
    /// extracts the underlying `DBIterator` via `mem::replace` before
    /// calling `seek_to_last()`; every path out of that transition —
    /// including the `!valid()` early return exercised here — must
    /// restore it so `error()` can still report what stopped iteration.
    #[test]
    fn test_lazy_next_back_first_call_surfaces_decode_error() {
        let sources = vec![vec![(b"short".to_vec(), b"v".to_vec())]];
        let db_iter = DBIterator::new(sources, 100);
        let mut it = BidiIterator::lazy(db_iter);

        assert!(it.error().is_none(), "no error before any iteration");
        assert!(it.next_back().is_none());
        let err = it
            .error()
            .expect("the decode error must survive the reverse transition");
        assert!(err.to_string().contains("too short"), "got {err}");
    }

    /// Same bug, hit on the second `next_back()` after a direction switch
    /// (`LazyFwdResumed` -> `LazyBackStarted`), which re-seeks via the same
    /// extract-then-early-return-prone pattern as the first-call transition
    /// above, at a different call site.
    #[test]
    fn test_lazy_next_back_after_direction_switch_surfaces_decode_error() {
        use crate::types::{InternalKey, ValueType};

        // "a", "n", "z" are valid; "m" has a well-formed length but an
        // unrecognized value_type byte, so it sorts normally by its user
        // key instead of the "too-short" shortcut that always sorts first
        // (see `compare_internal_key`). Resolving a backward entry decodes
        // whichever entry immediately precedes it (to confirm there is no
        // further same-user-key version) — placing "m" directly before
        // "n" means resolving "z" (peeks "n") and forward-resolving "a"
        // both stay clean, and only the *second* next_back(), which lands
        // on "n", is the one that peeks "m" and hits the decode error.
        let a = InternalKey::new(b"a", 10, ValueType::Value).into_bytes();
        let n = InternalKey::new(b"n", 8, ValueType::Value).into_bytes();
        let z = InternalKey::new(b"z", 5, ValueType::Value).into_bytes();
        let packed = (7u64 << 8) | 0xEE;
        let mut bad = b"m".to_vec();
        bad.extend_from_slice(&(!packed).to_be_bytes());
        let sources = vec![vec![
            (a, b"va".to_vec()),
            (bad, b"vm".to_vec()),
            (n, b"vn".to_vec()),
            (z, b"vz".to_vec()),
        ]];
        let db_iter = DBIterator::new(sources, 100);
        let mut it = BidiIterator::lazy(db_iter);

        // 1st next_back(): seek_to_last() lands on "z" — decodes fine.
        assert_eq!(it.next_back().unwrap().0, b"z");
        assert!(matches!(it.inner, BidiInner::Lazy { backward: true, .. }));
        assert!(it.error().is_none());

        // next(): re-seeks forward below "z", lands on "a" — decodes fine,
        // transitions to LazyFwdResumed.
        assert_eq!(it.next().unwrap().0, b"a");
        assert!(matches!(
            it.inner,
            BidiInner::Lazy {
                backward: false,
                ..
            }
        ));
        assert!(it.error().is_none());

        // 2nd next_back(): LazyFwdResumed's handler re-seeks to the largest
        // key below "z", which is "n" — resolving it decodes the
        // immediately preceding malformed "m" entry.
        assert!(it.next_back().is_none());
        let err = it
            .error()
            .expect("the decode error must survive the second next_back()'s reverse transition");
        assert!(err.to_string().contains("invalid value_type"), "got {err}");
    }
}

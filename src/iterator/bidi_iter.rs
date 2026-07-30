//! BidiIterator: a bidirectional iterator that implements `DoubleEndedIterator`.
//!
//! Supports two modes:
//! - **Materialized**: pre-sorted entries for immediate bidirectional access
//! - **Lazy**: wraps a `DBIterator` for streaming forward iteration.
//!   First `next_back()` uses `seek_to_last()` (O(log N)).
//!   Direction switches re-seek between the consumed front/back keys, and
//!   subsequent calls stream with O(1) memory.

use std::mem;

use crate::iterator::db_iter::DBIterator;

/// A bidirectional iterator over (user_key, value) pairs.
pub struct BidiIterator {
    inner: BidiInner,
}

struct LazyBidiState {
    db_iter: Box<DBIterator>,
    /// Last key returned by `next()`, used to stop backward iteration.
    last_fwd_key: Option<Vec<u8>>,
    /// Last key returned by `next_back()`, used to stop forward iteration.
    last_back_key: Option<Vec<u8>>,
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
    LazyBackStarted(LazyBidiState),
    /// After calling next() while in LazyBackStarted, the db_iter is re-seeked
    /// forward between the consumed front/back keys. Both frontiers are retained
    /// so a later `next_back()` can re-seek and resume backward streaming.
    LazyFwdResumed(LazyBidiState),
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
    ///
    /// For materialized iterators this reports true exhaustion. For lazy
    /// iterators the remaining work is not tracked for a shared `&self`
    /// query, so this always returns `false` (same limitation as
    /// [`Self::remaining`]).
    pub fn is_empty(&self) -> bool {
        match &self.inner {
            BidiInner::Materialized { front, back, .. } => front >= back,
            _ => false,
        }
    }

    /// Return the first error from the underlying source iterator, or a
    /// key-decode corruption observed while filtering entries. Always
    /// `None` for a materialized iterator (built from an already-collected
    /// `Vec`, so there is no further I/O that can fail).
    ///
    /// Call after `next()`/`next_back()` returns `None` to distinguish
    /// normal exhaustion from an I/O or corruption failure that stopped
    /// iteration early — the lazy variants otherwise look identical to a
    /// cleanly exhausted range, mirroring [`DBIterator::error`].
    pub fn error(&self) -> Option<String> {
        match &self.inner {
            BidiInner::Materialized { .. } => None,
            BidiInner::Lazy { db_iter, .. } => db_iter.error(),
            BidiInner::LazyBackStarted(state) | BidiInner::LazyFwdResumed(state) => {
                state.db_iter.error()
            }
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
            BidiInner::LazyBackStarted(_) => {
                // Switch to streaming forward — O(1) memory, no collect().
                let placeholder = BidiInner::Materialized {
                    entries: Vec::new(),
                    front: 0,
                    back: 0,
                };
                let old = mem::replace(&mut self.inner, placeholder);
                if let BidiInner::LazyBackStarted(mut state) = old {
                    // Tighten the upper bound before re-seeking so no consumed
                    // backward entry can be returned from the front.
                    if let Some(ref back_key) = state.last_back_key {
                        state.db_iter.set_upper_bound(back_key.clone());
                    }

                    // Re-seek forward past the last consumed forward key.
                    match &state.last_fwd_key {
                        Some(key) => {
                            state.db_iter.seek(key);
                            // Skip last_fwd_key itself (already returned via next()).
                            if state.db_iter.valid() && state.db_iter.key() == Some(key.as_slice())
                            {
                                state.db_iter.advance();
                            }
                        }
                        None => {
                            // Never called next() before backward mode — start from beginning.
                            state.db_iter.seek_to_first();
                        }
                    }

                    // Stream forward — no materialization needed.
                    self.inner = BidiInner::LazyFwdResumed(state);
                    self.next()
                } else {
                    unreachable!()
                }
            }
            BidiInner::LazyFwdResumed(state) => {
                let entry = state.db_iter.next()?;
                state.last_fwd_key = Some(entry.0.clone());
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
                let old = mem::replace(&mut self.inner, placeholder);
                let BidiInner::Lazy { mut db_iter, .. } = old else {
                    unreachable!()
                };

                // Snapshot last_user_key before seek_to_last destroys it
                let last_fwd_key = db_iter.last_user_key().map(|k| k.to_vec());

                db_iter.seek_to_last();
                // Every path below restores `db_iter` into `LazyBackStarted`
                // before returning — including exhaustion/error and the
                // defensive key()/value() mismatch — so a later `error()`
                // call can still report an I/O or corruption failure that
                // stopped the seek early, instead of that failure looking
                // identical to a legitimately empty range (which losing
                // `db_iter` via an early `?` return would otherwise do).
                if !db_iter.valid() {
                    self.inner = BidiInner::LazyBackStarted(LazyBidiState {
                        db_iter,
                        last_fwd_key,
                        last_back_key: None,
                    });
                    return None;
                }
                let Some(k) = db_iter.key().map(|k| k.to_vec()) else {
                    // valid() == true should always mean key() is Some;
                    // treat any inconsistency as no data instead of losing
                    // `db_iter`.
                    self.inner = BidiInner::LazyBackStarted(LazyBidiState {
                        db_iter,
                        last_fwd_key,
                        last_back_key: None,
                    });
                    return None;
                };
                // If forward iteration already consumed this key (or past it),
                // there is nothing left to yield from the back; otherwise we would
                // re-yield a key already returned by next(). Mirrors the frontier
                // check in the LazyBackStarted branch.
                if let Some(fk) = last_fwd_key.as_deref()
                    && k.as_slice() <= fk
                {
                    self.inner = BidiInner::LazyBackStarted(LazyBidiState {
                        db_iter,
                        last_fwd_key,
                        last_back_key: Some(k),
                    });
                    return None;
                }
                let Some(v) = db_iter.value().map(|v| v.to_vec()) else {
                    self.inner = BidiInner::LazyBackStarted(LazyBidiState {
                        db_iter,
                        last_fwd_key,
                        last_back_key: Some(k),
                    });
                    return None;
                };
                self.inner = BidiInner::LazyBackStarted(LazyBidiState {
                    db_iter,
                    last_fwd_key,
                    last_back_key: Some(k.clone()),
                });

                Some((k, v))
            }
            BidiInner::LazyFwdResumed(_) => {
                // Re-seek to the back frontier instead of materializing the
                // remaining window. Direction switches stay O(1) in memory.
                let placeholder = BidiInner::Materialized {
                    entries: Vec::new(),
                    front: 0,
                    back: 0,
                };
                let old = mem::replace(&mut self.inner, placeholder);
                let BidiInner::LazyFwdResumed(mut state) = old else {
                    unreachable!()
                };

                if let Some(ref back_key) = state.last_back_key {
                    state.db_iter.set_upper_bound(back_key.clone());
                }
                state.db_iter.seek_to_last();
                // As in the `Lazy` branch above, every path below restores
                // `state` (and its `db_iter`) into `LazyBackStarted` before
                // returning, so `error()` can still see a failure that
                // stopped this seek early.
                if !state.db_iter.valid() {
                    self.inner = BidiInner::LazyBackStarted(state);
                    return None;
                }

                let Some(k) = state.db_iter.key().map(|k| k.to_vec()) else {
                    self.inner = BidiInner::LazyBackStarted(state);
                    return None;
                };
                if let Some(fwd_key) = state.last_fwd_key.as_deref()
                    && k.as_slice() <= fwd_key
                {
                    self.inner = BidiInner::LazyBackStarted(state);
                    return None;
                }
                let Some(v) = state.db_iter.value().map(|v| v.to_vec()) else {
                    self.inner = BidiInner::LazyBackStarted(state);
                    return None;
                };
                state.last_back_key = Some(k.clone());
                self.inner = BidiInner::LazyBackStarted(state);
                Some((k, v))
            }
            BidiInner::LazyBackStarted(state) => {
                // Second+ next_back(): stream backward via db_iter.prev().
                // O(1) memory — no materialization needed.
                state.db_iter.prev();
                if state.db_iter.valid() {
                    let k = state.db_iter.key()?.to_vec();
                    let v = state.db_iter.value()?.to_vec();
                    // Stop if backward cursor has crossed the forward frontier.
                    // Keep last_back_key unchanged — it's the correct upper bound
                    // for a subsequent next() materialization.
                    if let Some(fk) = state.last_fwd_key.as_deref()
                        && k.as_slice() <= fk
                    {
                        return None;
                    }
                    state.last_back_key = Some(k.clone());
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
    fn test_lazy_repeated_direction_switches_stay_streaming() {
        let mut it = make_lazy_bidi(&[b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"]);

        assert_eq!(it.next_back().unwrap().0, b"h");
        assert!(matches!(it.inner, BidiInner::LazyBackStarted(_)));
        assert_eq!(it.next().unwrap().0, b"a");
        assert!(matches!(it.inner, BidiInner::LazyFwdResumed(_)));
        assert_eq!(it.next_back().unwrap().0, b"g");
        assert!(matches!(it.inner, BidiInner::LazyBackStarted(_)));
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
        assert!(err.contains("too short"), "got {err}");
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
        assert!(matches!(it.inner, BidiInner::LazyBackStarted(_)));
        assert!(it.error().is_none());

        // next(): re-seeks forward below "z", lands on "a" — decodes fine,
        // transitions to LazyFwdResumed.
        assert_eq!(it.next().unwrap().0, b"a");
        assert!(matches!(it.inner, BidiInner::LazyFwdResumed(_)));
        assert!(it.error().is_none());

        // 2nd next_back(): LazyFwdResumed's handler re-seeks to the largest
        // key below "z", which is "n" — resolving it decodes the
        // immediately preceding malformed "m" entry.
        assert!(it.next_back().is_none());
        let err = it
            .error()
            .expect("the decode error must survive the second next_back()'s reverse transition");
        assert!(err.contains("invalid value_type"), "got {err}");
    }
}

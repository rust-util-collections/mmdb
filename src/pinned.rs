//! Retained view of a point-read value.

use crate::{memtable::MemTablePin, types::LazyValue};
use std::{ops::Deref, sync::Arc};

/// Immutable bytes returned by [`crate::DB::get_pinned`].
///
/// A pin keeps its entire SST block (up to 64 MiB) or memtable arena alive,
/// including after eviction, compaction, and database close. Those retained
/// bytes are outside the cache and write-buffer budgets. Use [`Self::into_vec`]
/// to retain just the value.
pub struct PinnedSlice(LazyValue);

/// Projection at a visible point entry. Existence reads consume only the length
/// and never construct a pin or increment its backing owner's reference count.
pub(crate) trait PointValue: Sized {
    fn project(len: usize, pin: impl FnOnce() -> PinnedSlice) -> Self;
    fn byte_len(&self) -> usize;
}

impl PointValue for usize {
    fn project(len: usize, _pin: impl FnOnce() -> PinnedSlice) -> Self {
        len
    }
    fn byte_len(&self) -> usize {
        *self
    }
}

impl PointValue for PinnedSlice {
    fn project(len: usize, pin: impl FnOnce() -> Self) -> Self {
        if len == 0 {
            Self(LazyValue::empty())
        } else {
            pin()
        }
    }
    fn byte_len(&self) -> usize {
        self.len()
    }
}

impl PinnedSlice {
    pub(crate) fn from_block(data: Arc<Vec<u8>>, offset: u32, len: u32) -> Self {
        Self(LazyValue::BlockRef { data, offset, len })
    }

    pub(crate) fn from_memtable(pin: MemTablePin) -> Self {
        Self(LazyValue::MemTable(pin))
    }

    /// Copy the value bytes and release this pin.
    pub fn into_vec(self) -> Vec<u8> {
        self.0.into_vec()
    }
}

impl Deref for PinnedSlice {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl AsRef<[u8]> for PinnedSlice {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl std::fmt::Debug for PinnedSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

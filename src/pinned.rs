//! Retained view of a point-read value.

use crate::{memtable::MemTablePin, types::LazyValue};
use std::{ops::Deref, sync::Arc};

/// Immutable bytes returned by [`crate::DB::get_pinned`] or by an occupied
/// result slot in [`crate::DB::get_many_pinned`].
///
/// A nonempty pin keeps its entire SST block (up to 64 MiB) or memtable arena alive,
/// including after eviction, compaction, and database close. Those retained
/// bytes are outside the cache and write-buffer budgets. Use [`Self::into_vec`]
/// to retain just the value.
pub struct PinnedSlice(LazyValue);

/// Projection at a visible point entry. Existence and owned-byte reads avoid
/// constructing a pin or touching its backing owner's reference count.
pub(crate) trait PointValue: Sized {
    fn project(value: &[u8], pin: impl FnOnce() -> PinnedSlice) -> Self;
    fn byte_len(&self) -> usize;
}

impl PointValue for usize {
    #[inline]
    fn project(value: &[u8], _pin: impl FnOnce() -> PinnedSlice) -> Self {
        value.len()
    }
    #[inline]
    fn byte_len(&self) -> usize {
        *self
    }
}

impl PointValue for Vec<u8> {
    #[inline]
    fn project(value: &[u8], _pin: impl FnOnce() -> PinnedSlice) -> Self {
        value.to_vec()
    }
    #[inline]
    fn byte_len(&self) -> usize {
        self.len()
    }
}

impl PointValue for PinnedSlice {
    #[inline]
    fn project(value: &[u8], pin: impl FnOnce() -> Self) -> Self {
        if value.is_empty() {
            Self(LazyValue::empty())
        } else {
            pin()
        }
    }
    #[inline]
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

    pub(crate) fn repin(&self) -> Self {
        Self(self.0.clone())
    }

    /// Report the SST block identity and allocation charge for retention checks
    /// in test-utils experiments. Memory addresses are used only as allocation IDs.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    pub fn sst_backing_charge(&self) -> Option<(usize, usize)> {
        match &self.0 {
            LazyValue::BlockRef { data, .. } => Some((Arc::as_ptr(data) as usize, data.capacity())),
            _ => None,
        }
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

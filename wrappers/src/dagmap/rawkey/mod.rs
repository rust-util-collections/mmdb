#[cfg(test)]
mod test;

use crate::{DagMapRaw, Orphan, ValueEnDe};
use mmdb_core::common::RawBytes;
use ruc::*;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

pub type ChildId = [u8];
pub type DagHead<V> = DagMapRawKey<V>;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct DagMapRawKey<V> {
    inner: DagMapRaw,
    _p: PhantomData<V>,
}

impl<V> DagMapRawKey<V>
where
    V: ValueEnDe,
{
    #[inline(always)]
    pub fn new(
        id: RawBytes,
        raw_parent: Option<&mut Orphan<DagMapRaw>>,
    ) -> Result<Self> {
        DagMapRaw::new(id, raw_parent).c(d!()).map(|inner| Self {
            inner,
            _p: PhantomData,
        })
    }

    #[inline(always)]
    pub fn into_inner(self) -> DagMapRaw {
        self.inner
    }

    ///
    /// # Safety
    ///
    #[inline(always)]
    pub unsafe fn shadow_inner(&self) -> DagMapRaw {
        self.inner.shadow()
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<V> {
        self.inner.get(key).map(|v| V::decode(&v).unwrap())
    }

    #[inline(always)]
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: &V) -> Option<V> {
        self.inner
            .insert(key, value.encode())
            .map(|v| V::decode(&v).unwrap())
    }

    /// Insert data bypassing the cache layer
    #[inline(always)]
    pub fn insert_direct(&mut self, key: impl AsRef<[u8]>, value: &V) -> Option<V> {
        self.inner
            .insert_direct(key, value.encode())
            .map(|v| V::decode(&v).unwrap())
    }

    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) -> Option<V> {
        self.inner.remove(key).map(|v| V::decode(&v).unwrap())
    }

    /// Remove data bypassing the cache layer
    #[inline(always)]
    pub fn remove_direct(&mut self, key: impl AsRef<[u8]>) -> Option<V> {
        self.inner
            .remove_direct(key)
            .map(|v| V::decode(&v).unwrap())
    }

    /// Flush all cached data into DB
    #[inline(always)]
    pub fn commit(&mut self) {
        self.inner.commit();
    }

    /// Return the new head of mainline,
    /// all instances should have been committed!
    #[inline(always)]
    pub fn prune(self) -> Result<DagHead<V>> {
        self.inner.prune().c(d!()).map(|inner| Self {
            inner,
            _p: PhantomData,
        })
    }

    /// Drop children that are in the `targets` list
    #[inline(always)]
    pub fn prune_children_include(&mut self, include_targets: &[impl AsRef<ChildId>]) {
        self.inner.prune_children_include(include_targets);
    }

    /// Drop children that are not in the `exclude_targets` list
    #[inline(always)]
    pub fn prune_children_exclude(&mut self, exclude_targets: &[impl AsRef<ChildId>]) {
        self.inner.prune_children_exclude(exclude_targets);
    }
}

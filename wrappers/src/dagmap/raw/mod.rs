#[cfg(test)]
mod test;

use crate::{DagMapId, MapxOrdRawKey, MapxRaw, Orphan};
use mmdb_core::{basic::mapx_raw, common::RawBytes};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    ops::{Deref, DerefMut},
};

type DagHead = DagMapRaw;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DagMapRaw {
    data: MapxRaw,

    parent: Orphan<Option<DagMapRaw>>,

    // child id --> child instance
    children: MapxOrdRawKey<DagMapRaw>,
}

impl DagMapRaw {
    pub fn new(id: &DagMapId, parent: &mut Orphan<Option<Self>>) -> Result<Self> {
        let r = Self {
            parent: unsafe { parent.shadow() },
            ..Default::default()
        };

        if let Some(p) = parent.get_mut().as_mut() {
            if p.children.contains_key(id) {
                return Err(eg!("Child ID exist!"));
            }
            p.children.insert(id, &r);
        }

        Ok(r)
    }

    /// # Safety
    ///
    /// This API breaks the semantic safety guarantees,
    /// but it is safe to use in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        Self {
            data: self.data.shadow(),
            parent: self.parent.shadow(),
            children: self.children.shadow(),
        }
    }

    #[inline(always)]
    pub fn is_dead(&self) -> bool {
        self.data.is_empty()
            && self.parent.get_value().is_none()
            && self.children.is_empty()
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<RawBytes> {
        let key = key.as_ref();

        let mut hdr = self;
        let mut hdr_owned;

        loop {
            if let Some(v) = hdr.data.get(key) {
                return alt!(v.is_empty(), None, Some(v));
            }
            if let Some(p) = hdr.parent.get_value() {
                hdr_owned = p;
                hdr = &hdr_owned;
            } else {
                return None;
            }
        }
    }

    #[inline(always)]
    pub fn get_mut(&mut self, key: impl AsRef<[u8]>) -> Option<ValueMut<'_>> {
        self.data.get_mut(key.as_ref()).map(|inner| ValueMut {
            value: inner.clone(),
            inner,
        })
    }

    #[inline(always)]
    pub fn insert(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Option<RawBytes> {
        self.data.insert(key.as_ref(), value)
    }

    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) -> Option<RawBytes> {
        self.data.insert(key.as_ref(), [])
    }

    /// Return the new head of mainline,
    /// all instances should have been committed!
    #[inline(always)]
    pub fn prune(self) -> Result<DagHead> {
        self.prune_mainline().c(d!())
    }

    // Return the new head of mainline
    fn prune_mainline(mut self) -> Result<DagHead> {
        let p = if let Some(p) = self.parent.get_value() {
            p
        } else {
            return Ok(self);
        };

        let mut linebuf = vec![p];
        while let Some(p) = linebuf.last().unwrap().parent.get_value() {
            linebuf.push(p);
        }

        let mid = linebuf.len() - 1;
        let (others, genesis) = linebuf.split_at_mut(mid);

        for i in others.iter().rev() {
            for (k, v) in i.data.iter() {
                genesis[0].data.insert(k, v);
            }
        }

        for (k, v) in self.data.iter() {
            genesis[0].data.insert(k, v);
        }

        let mut exclude_targets = vec![];
        for (id, child) in self.children.iter() {
            genesis[0].children.insert(&id, &child);
            exclude_targets.push(id);
        }

        // clean up
        *self.parent.get_mut() = None;
        self.data.clear();
        self.children.clear(); // disconnect from the mainline

        genesis[0].prune_children_exclude(&exclude_targets);

        // genesis[0]
        Ok(linebuf.pop().unwrap())
    }

    /// Drop children that are in the `targets` list
    #[inline(always)]
    pub fn prune_children_include(&mut self, include_targets: &[impl AsRef<DagMapId>]) {
        self.prune_children(include_targets, false);
    }

    /// Drop children that are not in the `exclude_targets` list
    #[inline(always)]
    pub fn prune_children_exclude(&mut self, exclude_targets: &[impl AsRef<DagMapId>]) {
        self.prune_children(exclude_targets, true);
    }

    fn prune_children(&mut self, targets: &[impl AsRef<DagMapId>], exclude_mode: bool) {
        let targets = targets.iter().map(|i| i.as_ref()).collect::<HashSet<_>>();

        let dropped_children = if exclude_mode {
            self.children
                .iter()
                .filter(|(id, _)| !targets.contains(&id.as_slice()))
                .collect::<Vec<_>>()
        } else {
            self.children
                .iter()
                .filter(|(id, _)| targets.contains(&id.as_slice()))
                .collect::<Vec<_>>()
        };

        for (id, _) in dropped_children.iter() {
            self.children.remove(id);
        }

        for (_, mut child) in dropped_children.into_iter() {
            child.destroy();
        }
    }

    /// Drop all data
    #[inline(always)]
    pub fn destroy(&mut self) {
        *self.parent.get_mut() = None;
        self.data.clear();

        let mut children = self.children.iter().map(|(_, c)| c).collect::<Vec<_>>();
        self.children.clear(); // optimize for recursive ops

        for c in children.iter_mut() {
            c.destroy();
        }
    }

    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.data.is_the_same_instance(&other_hdr.data)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ValueMut<'a> {
    value: RawBytes,
    inner: mapx_raw::ValueMut<'a>,
}

impl<'a> Drop for ValueMut<'a> {
    fn drop(&mut self) {
        self.inner.clone_from(&self.value);
    }
}

impl<'a> Deref for ValueMut<'a> {
    type Target = RawBytes;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a> DerefMut for ValueMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

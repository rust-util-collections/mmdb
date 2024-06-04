#[cfg(test)]
mod test;

use crate::{MapxOrdRawKey, MapxRaw, Orphan};
use mmdb_core::common::{RawBytes, TRASH_CLEANER};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub type ChildId = [u8];
pub type DagHead = DagMapRaw;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DagMapRaw {
    id: RawBytes,

    data: MapxRaw,

    #[serde(skip)]
    cache: HashMap<RawBytes, RawBytes>,

    parent: Option<Orphan<DagMapRaw>>,

    // child id --> child instance
    children: MapxOrdRawKey<DagMapRaw>,
}

impl DagMapRaw {
    pub fn new(id: RawBytes, parent: Option<&mut Orphan<Self>>) -> Result<Self> {
        let pshadow = parent.as_ref().map(|p| unsafe { p.shadow() });

        if let Some(p) = parent {
            let mut hdr = p.get_mut();
            if hdr.children.contains_key(&id) {
                Err(eg!("Child ID exist!"))
            } else {
                let i = Self {
                    id,
                    parent: pshadow,
                    ..Default::default()
                };
                hdr.children.insert(&i.id, &i);
                Ok(i)
            }
        } else {
            Ok(Self {
                id,
                parent: None,
                ..Default::default()
            })
        }
    }

    ///
    /// # Safety
    ///
    pub unsafe fn shadow(&self) -> Self {
        Self {
            id: self.id.clone(),
            data: self.data.shadow(),
            cache: HashMap::new(),
            parent: self.parent.as_ref().map(|p| p.shadow()),
            children: self.children.shadow(),
        }
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<RawBytes> {
        let key = key.as_ref();

        let mut hdr = self;
        let mut hdr_owned;

        loop {
            if let Some(v) = hdr
                .cache
                .get(key)
                .map(|v| v.to_owned())
                .or_else(|| hdr.data.get(key))
            {
                return alt!(v.is_empty(), None, Some(v));
            }
            if let Some(p) = hdr.parent.as_ref() {
                hdr_owned = p.get_value();
                hdr = &hdr_owned;
            } else {
                return None;
            }
        }
    }

    // Get data from the backend DB directly
    fn get_from_db(&self, key: impl AsRef<[u8]>) -> Option<RawBytes> {
        let key = key.as_ref();

        let mut hdr = self;
        let mut hdr_owned;

        loop {
            if let Some(v) = hdr.data.get(key) {
                return alt!(v.is_empty(), None, Some(v));
            }
            if let Some(p) = hdr.parent.as_ref() {
                hdr_owned = p.get_value();
                hdr = &hdr_owned;
            } else {
                return None;
            }
        }
    }

    #[inline(always)]
    pub fn insert(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Option<RawBytes> {
        if let Some(v) = self
            .cache
            .insert(key.as_ref().to_owned(), value.as_ref().to_owned())
        {
            Some(v)
        } else {
            self.get_from_db(key)
        }
    }

    /// Insert data bypassing the cache layer
    #[inline(always)]
    pub fn insert_direct(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Option<RawBytes> {
        let k = key.as_ref();
        self.cache.remove(k);
        self.data.insert(key, value)
    }

    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) -> Option<RawBytes> {
        let k = key.as_ref();

        if let Some(v) = self.get(k) {
            self.cache.insert(k.to_owned(), vec![]);
            Some(v)
        } else {
            None // did not exist already, insert nothing
        }
    }

    /// Remove data bypassing the cache layer
    #[inline(always)]
    pub fn remove_direct(&mut self, key: impl AsRef<[u8]>) -> Option<RawBytes> {
        let k = key.as_ref();

        if let Some(v) = self.get(k) {
            self.cache.remove(k);
            self.data.insert(k, []);
            Some(v)
        } else {
            None // did not exist already, insert nothing
        }
    }

    /// Flush all cached data into DB
    #[inline(always)]
    pub fn commit(&mut self) {
        for (k, v) in self.cache.drain() {
            self.data.insert(k, v);
        }
    }

    /// Return the new head of mainline,
    /// all instances should have been committed!
    #[inline(always)]
    pub fn prune(self) -> Result<DagHead> {
        self.prune_mainline().c(d!())
    }

    // Return the new head of mainline,
    // all instances should have been committed!
    fn prune_mainline(mut self) -> Result<DagHead> {
        if !self.cache.is_empty() {
            return Err(eg!("cache is dirty"));
        }

        let p = if let Some(p) = self.parent.as_ref() {
            p
        } else {
            return Ok(self);
        };

        let mut linebuf = vec![p.get_value()];
        while let Some(p) = linebuf.last().unwrap().parent.as_ref() {
            linebuf.push(p.get_value());
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

        // disconnect from nodes of the mainline
        self.children.clear();

        genesis[0].prune_children_exclude(&exclude_targets);

        Ok(linebuf.pop().unwrap())
    }

    /// Drop children that are in the `targets` list
    #[inline(always)]
    pub fn prune_children_include(&mut self, include_targets: &[impl AsRef<ChildId>]) {
        self.prune_children(include_targets, false);
    }

    /// Drop children that are not in the `exclude_targets` list
    #[inline(always)]
    pub fn prune_children_exclude(&mut self, exclude_targets: &[impl AsRef<ChildId>]) {
        self.prune_children(exclude_targets, true);
    }

    fn prune_children(&mut self, targets: &[impl AsRef<ChildId>], exclude_mode: bool) {
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

        for (id, child) in dropped_children.iter() {
            debug_assert_eq!(id, &child.id);
            self.children.remove(id);
        }

        TRASH_CLEANER.lock().execute(|| {
            for (_, mut child) in dropped_children.into_iter() {
                child.data.clear();
                child.cache.clear();
                for (_, mut c) in child.children.iter_mut() {
                    c.drop_children();
                }
            }
        });
    }

    fn drop_children(&mut self) {
        for (id, mut child) in self.children.iter_mut() {
            debug_assert_eq!(id, child.id);
            child.data.clear();
            child.cache.clear();
            for (_, mut c) in child.children.iter_mut() {
                c.drop_children();
            }
        }
        self.children.clear();
    }
}

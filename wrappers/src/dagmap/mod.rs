use crate::{MapxOrdRawKey, MapxRaw, Orphan};
use mmdb_core::common::{RawBytes, TRASH_CLEANER};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub type ChildId = [u8];
pub type DagHead = DagMap;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DagMap {
    id: RawBytes,
    data: MapxRaw,
    parent: Option<Orphan<DagMap>>,
    // child id --> child instance
    children: MapxOrdRawKey<DagMap>,
}

impl DagMap {
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

    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<RawBytes> {
        let mut hdr = self;
        let mut hdr_owned;
        loop {
            if let Some(v) = hdr.data.get(&key) {
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
        self.data.insert(key, value)
    }

    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) -> Option<RawBytes> {
        self.data.remove(key)
    }

    #[inline(always)]
    pub fn prune(self) -> DagHead {
        self.prune_mainline()
    }

    // Return the new head of mainline
    fn prune_mainline(mut self) -> DagHead {
        let p = if let Some(p) = self.parent.as_ref() {
            p
        } else {
            return self;
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

        let mut keep_only = vec![];
        for (id, child) in self.children.iter() {
            genesis[0].children.insert(&id, &child);
            keep_only.push(id);
        }

        // disconnect from nodes of the mainline
        self.children.clear();

        genesis[0].prune_children(&keep_only);

        linebuf.pop().unwrap()
    }

    /// Drop children that are not in the `keep_only` list
    pub fn prune_children(&mut self, keep_only: &[impl AsRef<ChildId>]) {
        let keep_only = keep_only.iter().map(|i| i.as_ref()).collect::<HashSet<_>>();
        let dropped_children = self
            .children
            .iter()
            .filter(|(id, _)| !keep_only.contains(&id.as_slice()))
            .collect::<Vec<_>>();

        for (id, child) in dropped_children.iter() {
            debug_assert_eq!(id, &child.id);
            self.children.remove(id);
        }

        TRASH_CLEANER.lock().execute(|| {
            for (_, mut child) in dropped_children.into_iter() {
                child.data.clear();
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
            for (_, mut c) in child.children.iter_mut() {
                c.drop_children();
            }
        }
        self.children.clear();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn dagmap_functions() {
        let mut i0 = DagMap::new(vec![0], None).unwrap();
        i0.insert("k0", "v0");
        assert_eq!(i0.get("k0").unwrap().as_slice(), "v0".as_bytes());
        assert!(i0.get("k1").is_none());
        let mut i0 = Orphan::new(i0);

        let mut i1 = DagMap::new(vec![1], Some(&mut i0)).unwrap();
        i1.insert("k1", "v1");
        assert_eq!(i1.get("k1").unwrap().as_slice(), "v1".as_bytes());
        assert_eq!(i1.get("k0").unwrap().as_slice(), "v0".as_bytes());
        let mut i1 = Orphan::new(i1);

        // Child ID exist
        assert!(DagMap::new(vec![1], Some(&mut i0)).is_err());

        let mut i2 = DagMap::new(vec![2], Some(&mut i1)).unwrap();
        i2.insert("k2", "v2");
        assert_eq!(i2.get("k2").unwrap().as_slice(), "v2".as_bytes());
        assert_eq!(i2.get("k1").unwrap().as_slice(), "v1".as_bytes());
        assert_eq!(i2.get("k0").unwrap().as_slice(), "v0".as_bytes());
        i2.insert("k2", "v2x");
        assert_eq!(i2.get("k2").unwrap().as_slice(), "v2x".as_bytes());
        assert_eq!(i2.get("k1").unwrap().as_slice(), "v1".as_bytes());
        assert_eq!(i2.get("k0").unwrap().as_slice(), "v0".as_bytes());
        i2.insert("k1", "v1x");
        assert_eq!(i2.get("k2").unwrap().as_slice(), "v2x".as_bytes());
        assert_eq!(i2.get("k1").unwrap().as_slice(), "v1x".as_bytes());
        assert_eq!(i2.get("k0").unwrap().as_slice(), "v0".as_bytes());
        i2.insert("k0", "v0x");
        assert_eq!(i2.get("k2").unwrap().as_slice(), "v2x".as_bytes());
        assert_eq!(i2.get("k1").unwrap().as_slice(), "v1x".as_bytes());
        assert_eq!(i2.get("k0").unwrap().as_slice(), "v0x".as_bytes());

        assert!(i1.get_value().get("k2").is_none());
        assert_eq!(
            i1.get_value().get("k1").unwrap().as_slice(),
            "v1".as_bytes()
        );
        assert_eq!(
            i1.get_value().get("k0").unwrap().as_slice(),
            "v0".as_bytes()
        );

        assert!(i0.get_value().get("k2").is_none());
        assert!(i0.get_value().get("k1").is_none());
        assert_eq!(
            i0.get_value().get("k0").unwrap().as_slice(),
            "v0".as_bytes()
        );

        let mut head = i2.prune();

        sleep_ms!(1000); // give some time to the async cleaner

        assert_eq!(head.get("k2").unwrap().as_slice(), "v2x".as_bytes());
        assert_eq!(head.get("k1").unwrap().as_slice(), "v1x".as_bytes());
        assert_eq!(head.get("k0").unwrap().as_slice(), "v0x".as_bytes());

        assert_eq!(
            i1.get_value().get("k2").unwrap().as_slice(),
            "v2x".as_bytes()
        );
        assert_eq!(
            i1.get_value().get("k1").unwrap().as_slice(),
            "v1x".as_bytes()
        );
        assert_eq!(
            i1.get_value().get("k0").unwrap().as_slice(),
            "v0x".as_bytes()
        );

        assert_eq!(
            i0.get_value().get("k2").unwrap().as_slice(),
            "v2x".as_bytes()
        );
        assert_eq!(
            i0.get_value().get("k1").unwrap().as_slice(),
            "v1x".as_bytes()
        );
        assert_eq!(
            i0.get_value().get("k0").unwrap().as_slice(),
            "v0x".as_bytes()
        );

        // prune with deep stack
        for i in 10u8..=255 {
            head.insert(i.to_be_bytes(), i.to_be_bytes());
            head = DagMap::new(vec![i], Some(&mut Orphan::new(head))).unwrap();
        }

        let head = head.prune();
        assert!(head.parent.is_none());
        assert!(head.children.is_empty());

        for i in 10u8..=255 {
            assert_eq!(
                head.get(i.to_be_bytes()).unwrap().as_slice(),
                i.to_be_bytes()
            );
        }
    }
}

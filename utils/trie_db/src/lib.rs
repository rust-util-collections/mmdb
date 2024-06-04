#![deny(warnings)]
#![allow(clippy::new_without_default)]

mod substrate_trie;

#[cfg(test)]
mod test;

pub use mmdb::{RawBytes, RawKey, RawValue, ValueEnDe};

use mmdb::{DagMapId, DagMapRaw, MapxOrdRawKey, Orphan};
use mmdb_hash_db::{sp_hash_db::EMPTY_PREFIX, KeccakHasher as H, TrieBackend};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::mem;
use trie_db::{
    CError, DBValue, HashDB, Hasher as _, Trie, TrieHash, TrieItem, TrieIterator, TrieKeyItem,
    TrieMut,
};

type L = substrate_trie::LayoutV1<H>;
type TrieDB<'a, 'cache> = trie_db::TrieDB<'a, 'cache, L>;
type TrieDBBuilder<'a, 'cache> = trie_db::TrieDBBuilder<'a, 'cache, L>;
type TrieDBMut<'a> = trie_db::TrieDBMut<'a, L>;
type TrieDBMutBuilder<'a> = trie_db::TrieDBMutBuilder<'a, L>;

pub type TrieRoot = TrieHash<L>;

pub type TrieIter<'a> = Box<dyn TrieIterator<L, Item = TrieItem<TrieHash<L>, CError<L>>> + 'a>;
pub type TrieKeyIter<'a> =
    Box<dyn TrieIterator<L, Item = TrieKeyItem<TrieHash<L>, CError<L>>> + 'a>;

// A root hash ?
type BackendId = DagMapId;
type HeaderSet = MapxOrdRawKey<TrieBackend>;

#[derive(Deserialize, Serialize)]
pub struct MptStore {
    // backend key ==> backend instance
    //
    // the backend key
    // - for the world state MPT, it is `[0]`(just an example)
    // - for the storage MPT, it is the bytes of a H160 address
    meta: MapxOrdRawKey<TrieBackend>,

    // BackendId(DagMapId) ==> backend instance
    // All nodes, aka headers
    header_set: HeaderSet,
}

impl MptStore {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            meta: MapxOrdRawKey::new(),
            header_set: MapxOrdRawKey::new(),
        }
    }

    #[inline(always)]
    pub fn new_backend(
        id: &BackendId, // A root hash ?
        parent: &mut Orphan<Option<DagMapRaw>>,
    ) -> Result<TrieBackend> {
        TrieBackend::new(id, parent).c(d!())
    }

    /// # Safety
    ///
    /// This API breaks the semantic safety guarantees,
    /// but it is safe to use in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        Self {
            meta: self.meta.shadow(),
            header_set: self.header_set.shadow(),
        }
    }

    #[inline(always)]
    pub fn trie_create(&mut self, backend_key: &[u8], backend: TrieBackend) -> Result<MptOnce> {
        self.put_backend(backend_key, &backend, false).c(d!())?;
        MptOnce::create_with_backend(backend, &self.header_set).c(d!())
    }

    #[inline(always)]
    pub fn trie_restore(
        &self,
        backend_key: &[u8],
        root: TrieRoot,
        search_history: bool,
    ) -> Result<MptOnce> {
        let r = self
            .get_backend(backend_key)
            .c(d!("backend not found"))
            .and_then(|backend| MptOnce::restore(backend, root, &self.header_set).c(d!()));

        if search_history && r.is_err() {
            let b = self.header_set.get(root).c(d!())?;
            MptOnce::restore(b, root, &self.header_set).c(d!())
        } else {
            r
        }
    }

    #[inline(always)]
    pub fn trie_remove(&mut self, backend_key: &[u8]) {
        self.remove_backend(backend_key);
    }

    /// Can be used to set the new blockchain mainline
    #[inline(always)]
    pub fn trie_swap_head(&mut self, backend_key: &[u8], root: TrieRoot) -> Result<()> {
        let b = self.header_set.get(root).c(d!())?;
        self.put_backend(backend_key, &b, true).c(d!())
    }

    #[inline(always)]
    fn get_backend(&self, backend_key: &[u8]) -> Option<TrieBackend> {
        self.meta.get(backend_key)
    }

    #[inline(always)]
    fn put_backend(
        &mut self,
        backend_key: &[u8],
        backend: &TrieBackend,
        force: bool,
    ) -> Result<()> {
        if force {
            self.meta.remove(backend_key);
        } else if self.meta.contains_key(backend_key) {
            return Err(eg!("backend key already exists"));
        }

        self.meta.insert(backend_key, backend);

        Ok(())
    }

    #[inline(always)]
    fn remove_backend(&mut self, backend_key: &[u8]) {
        self.meta.remove(backend_key);
    }
}

///
/// An owned MPT instance
///
/// # NOTE
///
/// The referenced field **MUST** be placed after the field that references it,
/// this is to ensure that the `Drop::drop` can be executed in the correct order,
/// so that UB will not occur
pub struct MptOnce {
    mpt: MptMut<'static>,
    root: TrieRoot,

    // self-reference
    #[allow(dead_code)]
    backend: Box<TrieBackend>,

    // A shadow of the instance in MptStore
    header_set: HeaderSet,
}

impl MptOnce {
    fn create_with_backend(backend: TrieBackend, header_set: &HeaderSet) -> Result<Self> {
        let backend = Box::into_raw(Box::new(backend));
        let mut mpt = MptMut::new(unsafe { &mut *backend });
        let root = mpt.commit();
        Ok(Self {
            mpt,
            root,
            backend: unsafe { Box::from_raw(backend) },
            header_set: unsafe { header_set.shadow() },
        })
    }

    fn restore(backend: TrieBackend, root: TrieRoot, header_set: &HeaderSet) -> Result<Self> {
        let backend = Box::into_raw(Box::new(backend));
        let mpt = MptMut::from_existing(unsafe { &mut *backend }, root).c(d!())?;
        Ok(Self {
            mpt,
            root,
            backend: unsafe { Box::from_raw(backend) },
            header_set: unsafe { header_set.shadow() },
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.mpt.get(key).c(d!())
    }

    pub fn contains(&self, key: &[u8]) -> Result<bool> {
        self.mpt.contains(key).c(d!())
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.mpt.insert(key, value).c(d!())
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.mpt.remove(key).c(d!()).map(|_| ())
    }

    pub fn clear(&mut self) -> Result<()> {
        self.mpt.clear().c(d!())
    }

    pub fn is_empty(&self) -> bool {
        self.mpt.is_empty()
    }

    pub fn commit(&mut self) -> Result<TrieRoot> {
        let root = self.mpt.commit();

        if self.header_set.contains_key(root) {
            return Err(eg!("the root value exists!"));
        }
        // root ==> its data version
        self.header_set.insert(root, &self.backend);

        let parent = unsafe { self.backend.shadow_backend() }.into_inner();
        // use the parent root as the `uuid` for the new DagMap
        let b = TrieBackend::new(&root, &mut Orphan::new(Some(parent))).c(d!())?;
        let mut new_hdr = Self::restore(b, root, &self.header_set).c(d!())?;

        mem::swap(self, &mut new_hdr);
        Ok(self.root)
    }

    pub fn root(&self) -> TrieRoot {
        self.root
    }

    pub fn ro_handle(&self, root: TrieRoot) -> Result<MptRo> {
        MptRo::from_existing(&self.backend, root).c(d!())
    }
}

impl ValueEnDe for MptOnce {
    fn try_encode(&self) -> Result<RawBytes> {
        Ok(self.encode())
    }

    fn encode(&self) -> RawBytes {
        [
            self.root.to_vec(),
            self.backend.encode(),
            self.header_set.encode(),
        ]
        .encode()
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let [r, b, h] = <[Vec<u8>; 3]>::decode(bytes).c(d!())?;

        alt!(H::LENGTH > r.len(), return Err(eg!("Invalid length")));
        let mut root = [0; H::LENGTH];
        root.copy_from_slice(&r[..H::LENGTH]);

        let backend = TrieBackend::decode(&b).c(d!())?;
        let header_set = HeaderSet::decode(&h).c(d!())?;

        Self::restore(backend, root, &header_set).c(d!())
    }
}

//
// # NOTE
//
// The referenced field **MUST** be placed after the field that references it,
// this is to ensure that the `drop`s can be executed in the correct order,
// so that UB will not occur
// A mutable MPT instance
struct MptMut<'a> {
    trie: TrieDBMut<'a>,

    // self-reference
    #[allow(dead_code)]
    meta: MptMeta,
}

impl<'a> MptMut<'a> {
    // keep private !!
    fn new(backend: &'a mut TrieBackend) -> Self {
        // The buf will be rewrited when building the target `Trie`,
        // so its original contents can be arbitrary values.
        let root_buf = Default::default();

        let meta = MptMeta::new(root_buf);

        let trie = TrieDBMutBuilder::new(backend, unsafe { &mut *meta.root }).build();

        Self { trie, meta }
    }

    fn from_existing(backend: &'a mut TrieBackend, root: TrieRoot) -> Result<Self> {
        if !backend.contains(&root, EMPTY_PREFIX) {
            return Err(eg!("Invalid state root: {:02x?}", root));
        }

        let meta = MptMeta::new(root);

        let trie = TrieDBMutBuilder::from_existing(backend, unsafe { &mut *meta.root }).build();

        Ok(Self { trie, meta })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.trie.get(key).c(d!())
    }

    fn contains(&self, key: &[u8]) -> Result<bool> {
        self.trie.contains(key).c(d!())
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.trie.insert(key, value).c(d!()).map(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.trie.remove(key).c(d!()).map(|_| ())
    }

    fn clear(&mut self) -> Result<()> {
        let root = self.commit();
        let keys = self.ro_handle(root).unwrap().key_iter().collect::<Vec<_>>();
        for k in keys.iter().map(|k| k.as_ref().unwrap()) {
            self.remove(k).c(d!())?;
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.trie.is_empty()
    }

    fn commit(&mut self) -> TrieRoot {
        *self.trie.root()
    }

    fn ro_handle(&self, root: TrieRoot) -> Result<MptRo> {
        MptRo::from_existing_dyn(self.trie.db(), root).c(d!())
    }
}

//
// # NOTE
//
// The referenced field **MUST** be placed after the field that references it,
// this is to ensure that the `drop`s can be executed in the correct order,
// so that UB will not occur
/// A readonly MPT instance
pub struct MptRo<'a> {
    trie: TrieDB<'a, 'a>,

    // self-reference
    #[allow(dead_code)]
    meta: MptMeta,
}

impl<'a> MptRo<'a> {
    pub fn from_existing(backend: &'a TrieBackend, root: TrieRoot) -> Result<Self> {
        if !backend.contains(&root, EMPTY_PREFIX) {
            return Err(eg!("Invalid state root: {:02x?}", root));
        }

        let meta = MptMeta::new(root);

        let trie = TrieDBBuilder::new(backend, unsafe { &*meta.root }).build();

        Ok(Self { trie, meta })
    }

    pub fn from_existing_dyn(backend: &dyn HashDB<H, DBValue>, root: TrieRoot) -> Result<Self> {
        let backend = &backend as *const &dyn HashDB<H, DBValue>;
        let backend = backend.cast::<&TrieBackend>();
        let backend = unsafe { *backend };
        MptRo::from_existing(backend, root).c(d!())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.trie.get(key).c(d!())
    }

    pub fn contains(&self, key: &[u8]) -> Result<bool> {
        self.trie.contains(key).c(d!())
    }

    pub fn iter(&self) -> TrieIter<'_> {
        pnk!(self.trie.iter())
    }

    pub fn key_iter(&self) -> TrieKeyIter<'_> {
        pnk!(self.trie.key_iter())
    }

    pub fn root(&self) -> TrieRoot {
        *self.trie.root()
    }
}

struct MptMeta {
    // self-reference
    #[allow(dead_code)]
    root: *mut TrieRoot,
}

impl MptMeta {
    fn new(root: TrieRoot) -> Self {
        Self {
            root: Box::into_raw(Box::new(root)),
        }
    }
}

impl Drop for MptMeta {
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.root));
        }
    }
}

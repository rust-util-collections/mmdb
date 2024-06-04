use super::*;
use std::collections::BTreeMap;

#[test]
fn trie_db_restore() {
    let mut s = MptStore::new();
    let b = MptStore::new_backend(&[0], &mut Orphan::new(None)).unwrap();
    let mut hdr = pnk!(s.trie_create(b"", b));

    pnk!(hdr.insert(b"key", b"value"));
    assert_eq!(b"value", pnk!(hdr.get(b"key")).unwrap().as_slice());

    let root = hdr.commit().unwrap();
    assert_eq!(root, hdr.root());

    let hdr_encoded = hdr.encode();
    drop(hdr);

    let mut hdr = pnk!(MptOnce::decode(&hdr_encoded));
    assert_eq!(b"value", pnk!(hdr.get(b"key")).unwrap().as_slice());
    assert_eq!(root, hdr.root());

    pnk!(hdr.insert(b"key1", b"value1"));
    assert_eq!(b"value1", pnk!(hdr.get(b"key1")).unwrap().as_slice());

    let old_hdr_ro = pnk!(hdr.ro_handle(root));
    assert_eq!(root, old_hdr_ro.root());
    assert_eq!(b"value", pnk!(old_hdr_ro.get(b"key")).unwrap().as_slice());
    assert!(pnk!(old_hdr_ro.get(b"key1")).is_none());

    let new_root = hdr.commit().unwrap();
    assert_eq!(new_root, hdr.root());
}

#[test]
fn trie_db_iter() {
    let mut s = MptStore::new();
    let b = MptStore::new_backend(&[0], &mut Orphan::new(None)).unwrap();
    let mut hdr = pnk!(s.trie_create(b"backend_key", b));
    assert!(hdr.is_empty());

    {
        let samples = (0u8..200).map(|i| ([i], [i])).collect::<Vec<_>>();
        samples.iter().for_each(|(k, v)| {
            pnk!(hdr.insert(k, v));
        });

        let root = hdr.commit().unwrap();

        let ro_hdr = hdr.ro_handle(root).unwrap();
        let bt = ro_hdr
            .iter()
            .map(|i| i.unwrap())
            .collect::<BTreeMap<_, _>>();

        bt.iter().enumerate().for_each(|(i, (k, v))| {
            assert_eq!(&[i as u8], k.as_slice());
            assert_eq!(k, v);
        });

        let keylist = ro_hdr.key_iter().map(|i| i.unwrap()).collect::<Vec<_>>();
        assert_eq!(keylist, bt.keys().cloned().collect::<Vec<_>>());
    }

    {
        let samples = (0u8..200).map(|i| ([i], [i + 1])).collect::<Vec<_>>();
        samples.iter().for_each(|(k, v)| {
            pnk!(hdr.insert(k, v));
        });

        let root = hdr.commit().unwrap();

        let ro_hdr = hdr.ro_handle(root).unwrap();
        let bt = ro_hdr
            .iter()
            .map(|i| i.unwrap())
            .collect::<BTreeMap<_, _>>();

        bt.iter().enumerate().for_each(|(i, (k, v))| {
            assert_eq!(&[i as u8], k.as_slice());
            assert_eq!(&[k[0] + 1], v.as_slice());
        });

        let keylist = ro_hdr.key_iter().map(|i| i.unwrap()).collect::<Vec<_>>();
        assert_eq!(keylist, bt.keys().cloned().collect::<Vec<_>>());
        assert!(!hdr.is_empty());
    }

    assert!(!hdr.is_empty());
    hdr.clear().unwrap();
    assert!(hdr.is_empty());
}

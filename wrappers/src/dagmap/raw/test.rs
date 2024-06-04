use super::*;

#[test]
fn dagmap_functions() {
    let mut i0 = DagMapRaw::new(vec![0], None).unwrap();
    i0.insert("k0", "v0");
    assert_eq!(i0.get("k0").unwrap().as_slice(), "v0".as_bytes());
    assert!(i0.get("k1").is_none());
    i0.commit();
    let mut i0 = Orphan::new(i0);

    let mut i1 = DagMapRaw::new(vec![1], Some(&mut i0)).unwrap();
    i1.insert_direct("k1", "v1");
    assert_eq!(i1.get("k1").unwrap().as_slice(), "v1".as_bytes());
    assert_eq!(i1.get("k0").unwrap().as_slice(), "v0".as_bytes());
    // i1.commit(); // need NOT `commit` when using `insert_direct`
    let mut i1 = Orphan::new(i1);

    // Child ID exist
    assert!(DagMapRaw::new(vec![1], Some(&mut i0)).is_err());

    let mut i2 = DagMapRaw::new(vec![2], Some(&mut i1)).unwrap();
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
    i2.commit();

    let mut head = pnk!(i2.prune());

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
        head.commit();
        head = DagMapRaw::new(vec![i], Some(&mut Orphan::new(head))).unwrap();
    }

    let mut head = pnk!(head.prune());
    assert!(head.parent.is_none());
    assert!(head.children.is_empty());

    for i in 10u8..=255 {
        assert_eq!(
            head.get(i.to_be_bytes()).unwrap().as_slice(),
            i.to_be_bytes()
        );
    }

    for i in 0u8..=255 {
        head.remove(i.to_be_bytes());
        assert!(head.get(i.to_be_bytes()).is_none());
    }
}

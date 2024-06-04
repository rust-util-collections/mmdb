use super::*;

macro_rules! s {
    ($i: expr) => {{ $i.as_bytes().to_vec() }};
}

#[test]
fn dagmap_functions() {
    let mut i0 = DagMapRawKey::new(vec![0], None).unwrap();
    i0.insert("k0", &s!("v0"));
    assert_eq!(i0.get("k0").unwrap(), s!("v0"));
    assert!(i0.get("k1").is_none());
    i0.commit();
    let mut i0_raw = Orphan::new(i0.into_inner());

    let mut i1 = DagMapRawKey::new(vec![1], Some(&mut i0_raw)).unwrap();
    i1.insert_direct("k1", &s!("v1"));
    assert_eq!(i1.get("k1").unwrap(), s!("v1"));
    assert_eq!(i1.get("k0").unwrap(), s!("v0"));
    // i1.commit(); // need NOT `commit` when using `insert_direct`
    let mut i1_raw = Orphan::new(i1.into_inner());

    // Child ID exist
    assert!(DagMapRawKey::<String>::new(vec![1], Some(&mut i0_raw)).is_err());

    let mut i2 = DagMapRawKey::new(vec![2], Some(&mut i1_raw)).unwrap();
    i2.insert("k2", &s!("v2"));
    assert_eq!(i2.get("k2").unwrap(), s!("v2"));
    assert_eq!(i2.get("k1").unwrap(), s!("v1"));
    assert_eq!(i2.get("k0").unwrap(), s!("v0"));
    i2.insert("k2", &s!("v2x"));
    assert_eq!(i2.get("k2").unwrap(), s!("v2x"));
    assert_eq!(i2.get("k1").unwrap(), s!("v1"));
    assert_eq!(i2.get("k0").unwrap(), s!("v0"));
    i2.insert("k1", &s!("v1x"));
    assert_eq!(i2.get("k2").unwrap(), s!("v2x"));
    assert_eq!(i2.get("k1").unwrap(), s!("v1x"));
    assert_eq!(i2.get("k0").unwrap(), s!("v0"));
    i2.insert("k0", &s!("v0x"));
    assert_eq!(i2.get("k2").unwrap(), s!("v2x"));
    assert_eq!(i2.get("k1").unwrap(), s!("v1x"));
    assert_eq!(i2.get("k0").unwrap(), s!("v0x"));

    assert!(i1_raw.get_value().get("k2").is_none());
    assert_eq!(i1_raw.get_value().get("k1").unwrap(), s!("v1").encode());
    assert_eq!(i1_raw.get_value().get("k0").unwrap(), s!("v0").encode());

    assert!(i0_raw.get_value().get("k2").is_none());
    assert!(i0_raw.get_value().get("k1").is_none());
    assert_eq!(i0_raw.get_value().get("k0").unwrap(), s!("v0").encode());
    i2.commit();

    let mut head = pnk!(i2.prune());

    sleep_ms!(1000); // give some time to the async cleaner

    assert_eq!(head.get("k2").unwrap(), s!("v2x"));
    assert_eq!(head.get("k1").unwrap(), s!("v1x"));
    assert_eq!(head.get("k0").unwrap(), s!("v0x"));

    assert_eq!(i1_raw.get_value().get("k2").unwrap(), s!("v2x").encode());
    assert_eq!(i1_raw.get_value().get("k1").unwrap(), s!("v1x").encode());
    assert_eq!(i1_raw.get_value().get("k0").unwrap(), s!("v0x").encode());

    assert_eq!(i0_raw.get_value().get("k2").unwrap(), s!("v2x").encode());
    assert_eq!(i0_raw.get_value().get("k1").unwrap(), s!("v1x").encode());
    assert_eq!(i0_raw.get_value().get("k0").unwrap(), s!("v0x").encode());

    // prune with deep stack
    for i in 10u8..=255 {
        head.insert(i.to_be_bytes(), &i.to_be_bytes().to_vec());
        head.commit();
        head = DagMapRawKey::new(vec![i], Some(&mut Orphan::new(head.into_inner())))
            .unwrap();
    }

    let mut head = pnk!(head.prune());

    for i in 10u8..=255 {
        assert_eq!(head.get(i.to_be_bytes()).unwrap(), i.to_be_bytes().to_vec());
    }

    for i in 0u8..=255 {
        head.remove(i.to_be_bytes());
        assert!(head.get(i.to_be_bytes()).is_none());
    }
}

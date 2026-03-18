use mmdb::{DB, DbOptions};

#[test]
fn debug_bidi_reverse() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions {
            create_if_missing: true,
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();
    for i in 0..10 {
        let key = format!("key_{:04}", i);
        db.put(key.as_bytes(), b"val").unwrap();
    }

    // Test pure reverse via iter_bidi
    let mut it = db.iter_bidi().unwrap();
    let mut count = 0;
    while let Some((k, _)) = it.next_back() {
        eprintln!("  next_back #{}: {:?}", count, String::from_utf8_lossy(&k));
        count += 1;
        if count > 15 {
            panic!("infinite loop!");
        }
    }
    eprintln!("Total reverse entries: {count}");
    assert_eq!(count, 10);
}

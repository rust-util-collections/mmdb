use mmdb::{DB, DbOptions};

/// Use a tiny write buffer so each put batch flushes to its own SST,
/// giving compaction multiple files to merge (avoiding the trivial-move
/// optimisation that skips the compaction filter).
fn make_db(dir: &std::path::Path) -> DB {
    DB::open(
        DbOptions {
            create_if_missing: true,
            write_buffer_size: 1024, // 1 KB — forces frequent flushes
            ..Default::default()
        },
        dir,
    )
    .unwrap()
}

#[test]
fn lazy_delete_removes_keys_on_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    // Write data in separate batches to create multiple SST files.
    for i in 0u32..100 {
        db.put(&i.to_be_bytes(), &[i as u8; 64]).unwrap();
    }

    // Lazy-delete some keys
    let dead: Vec<Vec<u8>> = (50u32..80).map(|i| i.to_be_bytes().to_vec()).collect();
    db.lazy_delete_batch(&dead);
    assert_eq!(db.dead_key_count(), 30);

    // Keys still readable before compaction
    assert!(db.get(&50u32.to_be_bytes()).unwrap().is_some());

    // Trigger compaction — merges SSTs through the filter
    db.compact_range(None::<&[u8]>, None::<&[u8]>).unwrap();

    // Dead keys should be gone after compaction
    for i in 50u32..80 {
        assert!(
            db.get(&i.to_be_bytes()).unwrap().is_none(),
            "key {} should have been removed by lazy_delete",
            i
        );
    }

    // Surviving keys should still be readable
    for i in 0u32..50 {
        assert!(
            db.get(&i.to_be_bytes()).unwrap().is_some(),
            "key {} should survive",
            i
        );
    }
    for i in 80u32..100 {
        assert!(
            db.get(&i.to_be_bytes()).unwrap().is_some(),
            "key {} should survive",
            i
        );
    }
}

#[test]
fn lazy_delete_single_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    // Write two separate batches to force two SSTs.
    db.put(b"keep", b"value1").unwrap();
    // Pad to push past the 1 KB write buffer.
    db.put(b"pad", &[0u8; 1024]).unwrap();
    db.put(b"remove", b"value2").unwrap();

    db.lazy_delete(b"remove");
    assert_eq!(db.dead_key_count(), 1);

    // Still readable before compaction
    assert!(db.get(b"remove").unwrap().is_some());

    db.compact_range(None::<&[u8]>, None::<&[u8]>).unwrap();

    assert!(db.get(b"remove").unwrap().is_none());
    assert_eq!(db.get(b"keep").unwrap().unwrap(), b"value1");
}

#[test]
fn clear_dead_keys_resets_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    let keys: Vec<Vec<u8>> = (0u32..10).map(|i| i.to_be_bytes().to_vec()).collect();
    db.lazy_delete_batch(&keys);
    assert_eq!(db.dead_key_count(), 10);

    db.clear_dead_keys();
    assert_eq!(db.dead_key_count(), 0);
}

/// Verify that `lazy_delete_batch` triggers background compaction when the
/// dead-key count newly crosses `lazy_delete_compaction_threshold`.
#[test]
fn lazy_delete_batch_auto_triggers_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions {
            create_if_missing: true,
            write_buffer_size: 1024,
            lazy_delete_compaction_threshold: 5,
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();

    // Write enough data across flushes so that compaction has work to do.
    for i in 0u32..20 {
        db.put(&i.to_be_bytes(), &[i as u8; 64]).unwrap();
    }

    // Register 6 keys — crosses the threshold of 5, should signal compaction.
    let dead: Vec<Vec<u8>> = (0u32..6).map(|i| i.to_be_bytes().to_vec()).collect();
    db.lazy_delete_batch(&dead);

    // Give background compaction a moment to run.
    std::thread::sleep(std::time::Duration::from_secs(2));

    // The dead keys should have been removed by background compaction.
    for i in 0u32..6 {
        assert!(
            db.get(&i.to_be_bytes()).unwrap().is_none(),
            "key {} should have been removed by auto-triggered compaction",
            i
        );
    }

    // Surviving keys should still be readable.
    for i in 6u32..20 {
        assert!(
            db.get(&i.to_be_bytes()).unwrap().is_some(),
            "key {} should survive",
            i
        );
    }
}

/// Verify that `lazy_delete_batch` accepts `&[&[u8]]` slices (not just
/// `Vec<u8>`), confirming the generic `impl AsRef<[u8]>` signature works.
#[test]
fn lazy_delete_batch_accepts_slices() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    db.put(b"a", b"1").unwrap();
    db.put(b"pad", &[0u8; 1024]).unwrap();
    db.put(b"b", b"2").unwrap();

    // Pass &[u8] slices — must compile with the generic signature.
    let keys: Vec<&[u8]> = vec![b"a", b"b"];
    db.lazy_delete_batch(keys);
    assert_eq!(db.dead_key_count(), 2);

    db.compact_range(None::<&[u8]>, None::<&[u8]>).unwrap();

    assert!(db.get(b"a").unwrap().is_none());
    assert!(db.get(b"b").unwrap().is_none());
}

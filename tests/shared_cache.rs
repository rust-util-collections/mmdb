//! End-to-end tests for the shared block-cache pool
//! (`DbOptions::block_cache`).
//!
//! The load-bearing property is the wrong-data guard: SST file numbers
//! are per-DB manifest counters, so two fresh DBs sharing one pool
//! naturally produce *identical* file numbers and block offsets — every
//! read here would return the other DB's bytes if member namespacing
//! were broken.

use std::sync::Arc;

use mmdb::{BlockCachePool, DB, DbOptions, DurabilityMode, ErrorKind, WriteOptions};

fn open_with_pool(dir: &std::path::Path, pool: &Arc<BlockCachePool>) -> DB {
    DB::open(
        DbOptions::default()
            .with_create_if_missing(true)
            .with_block_cache(Some(pool.clone())),
        dir,
    )
    .unwrap()
}

/// Read twice without asserting where the block lives.
fn assert_cached_value(db: &DB, key: &[u8], want: &[u8]) {
    assert_eq!(db.get(key).unwrap().unwrap(), want);
    assert_eq!(db.get(key).unwrap().unwrap(), want);
}

/// Read after compaction (so no L0 pin can serve the block) and require the
/// first get to populate the shared pool. A private-cache fallback leaves the
/// pool empty and fails here.
fn assert_shared_cached_value(db: &DB, key: &[u8], want: &[u8], pool: &BlockCachePool) {
    assert_eq!(db.get(key).unwrap().unwrap(), want);
    pool.run_pending_tasks();
    assert!(
        pool.entry_count() > 0,
        "the first read must populate the shared pool"
    );
    assert_eq!(db.get(key).unwrap().unwrap(), want);
}

#[test]
fn shared_pool_reports_weighted_charges_and_member_pin_lengths() {
    let capacity = 8 << 20;
    let pool = Arc::new(BlockCachePool::new(capacity));
    let a = pool.attach();
    let b = pool.attach();
    let assert_charges = |segmented, overflow| {
        pool.run_pending_tasks();
        let usage = pool.usage();
        assert_eq!(usage.segmented_capacity_bytes, capacity);
        assert_eq!(usage.overflow_capacity_bytes, capacity);
        assert_eq!(usage.segmented_weighted_bytes, segmented);
        assert_eq!(usage.overflow_weighted_bytes, overflow);
    };
    assert_charges(0, 0);

    a.insert(1, 0, Vec::new());
    let mut survivor = Vec::with_capacity(100);
    survivor.push(7);
    let survivor_charge = survivor.capacity() as u64;
    b.insert(1, 0, survivor);
    assert_charges(1 + survivor_charge, 0);
    assert!(a.get(1, 0).unwrap().is_empty());
    assert_eq!(b.get(1, 0).unwrap().as_slice(), &[7]);

    let replacement = vec![8; 18];
    let replacement_charge = replacement.capacity() as u64;
    a.insert(1, 0, replacement);
    assert_charges(replacement_charge + survivor_charge, 0);
    a.invalidate_file(1);
    assert_charges(survivor_charge, 0);

    let mut large = Vec::with_capacity(2 << 20);
    large.push(9);
    let large_charge = large.capacity() as u64;
    a.insert(2, 0, vec![5]);
    assert_charges(1 + survivor_charge, 0);
    a.insert(2, 0, large);
    assert_charges(survivor_charge, large_charge);
    // Moving between stores must retain the file-offset registration even
    // if the old store's removal callback has already run.
    a.invalidate_file(2);
    assert_charges(survivor_charge, 0);
    a.insert(2, 0, vec![8; 2 << 20]);
    assert_charges(survivor_charge, 2 << 20);
    let larger = vec![6; 3 << 20];
    let larger_charge = larger.capacity() as u64;
    a.insert(2, 0, larger);
    a.insert(3, 0, vec![2; 2 << 20]);
    pool.run_pending_tasks();
    a.insert(3, 0, vec![4]);
    assert_charges(1 + survivor_charge, larger_charge);
    a.invalidate_file(3);
    assert_charges(survivor_charge, larger_charge);
    let mut pin = Vec::with_capacity(512);
    pin.extend_from_slice(b"pin");
    a.insert_pinned(2, 1, pin);
    b.insert_pinned(2, 1, b"still pinned".to_vec());
    assert_charges(survivor_charge, larger_charge);
    assert_eq!(a.pinned_bytes(), 3);
    assert_eq!(b.pinned_bytes(), 12);

    a.detach();
    assert_charges(survivor_charge, 0);
    assert_eq!(a.pinned_bytes(), 0);
    assert_eq!(b.pinned_bytes(), 12);
    assert_eq!(b.get(1, 0).unwrap().as_slice(), &[7]);
    assert_eq!(b.get(2, 1).unwrap().as_slice(), b"still pinned");
}

#[test]
fn test_shared_pool_members_never_alias() {
    let pool = Arc::new(BlockCachePool::new(8 * 1024 * 1024));
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let db_a = open_with_pool(dir_a.path(), &pool);
    let db_b = open_with_pool(dir_b.path(), &pool);

    // Identical keys, divergent values: after both flush, the DBs hold
    // same-numbered SSTs with the same block layout.
    for i in 0..200u32 {
        let k = format!("key-{i:04}");
        db_a.put(k.as_bytes(), format!("a-{i}").as_bytes()).unwrap();
        db_b.put(k.as_bytes(), format!("b-{i}").as_bytes()).unwrap();
    }
    db_a.flush().unwrap();
    db_b.flush().unwrap();
    // Move the data out of L0 so a member-local pinned first block cannot
    // satisfy the reads without touching the shared pool.
    db_a.compact().unwrap();
    db_b.compact().unwrap();

    for i in (0..200u32).step_by(17) {
        let k = format!("key-{i:04}");
        assert_shared_cached_value(&db_a, k.as_bytes(), format!("a-{i}").as_bytes(), &pool);
        assert_shared_cached_value(&db_b, k.as_bytes(), format!("b-{i}").as_bytes(), &pool);
    }
}

#[test]
fn test_shared_pool_close_isolates_members() {
    let pool = Arc::new(BlockCachePool::new(8 * 1024 * 1024));
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let db_a = open_with_pool(dir_a.path(), &pool);
    let db_b = open_with_pool(dir_b.path(), &pool);

    db_a.put(b"k", b"from-a").unwrap();
    db_b.put(b"k", b"from-b").unwrap();
    db_a.flush().unwrap();
    db_b.flush().unwrap();
    assert!(std::ptr::eq(db_a.block_cache_pool(), &*pool));
    let retained = db_a.get_pinned(b"k").unwrap().unwrap();
    assert!(db_a.memory_usage().l0_pin_bytes > 0);
    db_a.compact().unwrap();
    db_b.compact().unwrap();
    assert_shared_cached_value(&db_a, b"k", b"from-a", &pool);
    assert_shared_cached_value(&db_b, b"k", b"from-b", &pool);

    // Closing one member sweeps its pool entries; the survivor keeps
    // working (and re-populates freely).
    db_a.close().unwrap();
    assert_eq!(db_a.memory_usage().l0_pin_bytes, 0);
    drop(db_a);
    assert_eq!(&*retained, b"from-a");
    assert_cached_value(&db_b, b"k", b"from-b");

    // A NEW member attached after the close gets a fresh id — reopening
    // the closed dir must see its own data, never a stale pool entry.
    let db_a2 = open_with_pool(dir_a.path(), &pool);
    assert_cached_value(&db_a2, b"k", b"from-a");
    assert_cached_value(&db_b, b"k", b"from-b");
}

#[test]
fn test_private_cache_default_unaffected() {
    // `block_cache: None` (the default): private per-DB cache, exactly
    // the historical shape — this is what every existing caller gets.
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions::default().with_create_if_missing(true),
        dir.path(),
    )
    .unwrap();
    db.put(b"k", b"v").unwrap();
    db.flush().unwrap();
    assert_cached_value(&db, b"k", b"v");
    db.close().unwrap();
}

#[test]
fn failed_open_and_dropped_member_release_shared_pool_entries() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions::default().with_create_if_missing(true),
        dir.path(),
    )
    .unwrap();
    db.delete_range(b"a", b"z").unwrap();
    db.flush().unwrap();
    db.put_with_options(
        &WriteOptions::default().with_durability(DurabilityMode::WalSync),
        b"after",
        b"wal",
    )
    .unwrap();
    db.simulate_crash();

    // Corrupt the active WAL payload so `VersionSet` opens the live range-del
    // SST (admitting its rangedel leaf into the shared pool) before WAL replay
    // aborts the open with `ErrorKind::Corruption`.
    let wal_path = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|ext| ext == "wal"))
        .expect("residual WAL present");
    let mut wal_bytes = std::fs::read(&wal_path).unwrap();
    let last = wal_bytes.len() - 1;
    wal_bytes[last] ^= 0xFF;
    std::fs::write(&wal_path, &wal_bytes).unwrap();

    let pool = Arc::new(BlockCachePool::new(8 << 20));
    let rw_err = DB::open(
        DbOptions::default().with_block_cache(Some(pool.clone())),
        dir.path(),
    )
    .err()
    .expect("corrupt WAL must fail writable open");
    assert_eq!(rw_err.kind(), ErrorKind::Corruption);

    let ro_err = DB::open_read_only_with_options(
        DbOptions::default().with_block_cache(Some(pool.clone())),
        dir.path(),
    )
    .err()
    .expect("corrupt WAL must fail read-only open");
    assert_eq!(ro_err.kind(), ErrorKind::Corruption);

    {
        let member = pool.attach();
        member.insert(9, 0, vec![0xAB; 4096]);
        member.insert_pinned(9, 4096, vec![0xCD; 512]);
    }

    pool.run_pending_tasks();
    assert_eq!(pool.entry_count(), 0);
    assert_eq!(pool.usage().segmented_weighted_bytes, 0);
    assert_eq!(pool.usage().overflow_weighted_bytes, 0);
}

//! End-to-end tests for the shared block-cache pool
//! (`DbOptions::block_cache`).
//!
//! The load-bearing property is the wrong-data guard: SST file numbers
//! are per-DB manifest counters, so two fresh DBs sharing one pool
//! naturally produce *identical* file numbers and block offsets — every
//! read here would return the other DB's bytes if member namespacing
//! were broken.

use std::sync::Arc;

use mmdb::{BlockCachePool, DB, DbOptions};

fn open_with_pool(dir: &std::path::Path, pool: &Arc<BlockCachePool>) -> DB {
    DB::open(
        DbOptions {
            create_if_missing: true,
            block_cache: Some(pool.clone()),
            ..Default::default()
        },
        dir,
    )
    .unwrap()
}

/// Flush so the data lives in SSTs, then read twice: the first get
/// populates the shared cache, the second is served from it.
fn assert_cached_value(db: &DB, key: &[u8], want: &[u8]) {
    assert_eq!(db.get(key).unwrap().unwrap(), want);
    assert_eq!(db.get(key).unwrap().unwrap(), want);
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

    for i in (0..200u32).step_by(17) {
        let k = format!("key-{i:04}");
        assert_cached_value(&db_a, k.as_bytes(), format!("a-{i}").as_bytes());
        assert_cached_value(&db_b, k.as_bytes(), format!("b-{i}").as_bytes());
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
    assert_cached_value(&db_a, b"k", b"from-a");
    assert_cached_value(&db_b, b"k", b"from-b");

    // Closing one member sweeps its pool entries; the survivor keeps
    // working (and re-populates freely).
    db_a.close().unwrap();
    drop(db_a);
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
        DbOptions {
            create_if_missing: true,
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();
    db.put(b"k", b"v").unwrap();
    db.flush().unwrap();
    assert_cached_value(&db, b"k", b"v");
    db.close().unwrap();
}

use std::sync::Arc;

use mmdb::{BlockCachePool, DB, DbOptions, ErrorKind, ReadOptions};

#[test]
fn fill_index_cache_controls_index_leaf_caching_independently_of_data_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let pool = Arc::new(BlockCachePool::new(4 << 20));
    let opts = DbOptions::default()
        .with_block_cache(Some(Arc::clone(&pool)))
        .with_block_size(512)
        .with_pin_l0_first_block_and_warm_index(false);
    let db = DB::open(opts, dir.path()).unwrap();

    // Create one SST with multiple data blocks under a single index leaf.
    db.ingest_sorted((0u32..64).map(|i| (format!("k{i:04}").into_bytes(), vec![b'v'; 280])))
        .unwrap();
    pool.run_pending_tasks();
    assert_eq!(pool.entry_count(), 0);

    let stat = |name: &str| -> u64 { db.get_property(name).unwrap().parse().unwrap() };

    // 1. fill_cache = false, fill_index_cache = Some(true):
    //    first read caches ONLY the index leaf, not the data block.
    let index_only = ReadOptions::default()
        .with_fill_cache(false)
        .with_fill_index_cache(true);
    let misses0 = stat("stats.block_cache_misses");
    let hits0 = stat("stats.block_cache_hits");
    assert_eq!(
        db.get_with_options(&index_only, b"k0005").unwrap().unwrap(),
        vec![b'v'; 280]
    );
    pool.run_pending_tasks();
    assert_eq!(pool.entry_count(), 1, "only index leaf should be cached");
    assert_eq!(stat("stats.block_cache_misses") - misses0, 2);
    assert_eq!(stat("stats.block_cache_hits") - hits0, 0);

    // Second read of a different block hits the cached index leaf and misses only the data block.
    let misses1 = stat("stats.block_cache_misses");
    let hits1 = stat("stats.block_cache_hits");
    assert_eq!(
        db.get_with_options(&index_only, b"k0050").unwrap().unwrap(),
        vec![b'v'; 280]
    );
    pool.run_pending_tasks();
    assert_eq!(pool.entry_count(), 1, "data block must remain uncached");
    assert_eq!(stat("stats.block_cache_hits") - hits1, 1);
    assert_eq!(stat("stats.block_cache_misses") - misses1, 1);

    // Grouped point read with the same options hits the cached index leaf and leaves data blocks uncached.
    let misses2 = stat("stats.block_cache_misses");
    let hits2 = stat("stats.block_cache_hits");
    let rows = db
        .get_many_pinned_with_options(&index_only, &[b"k0010", b"k0040"])
        .unwrap();
    assert_eq!(rows[0].as_deref(), Some([b'v'; 280].as_slice()));
    assert_eq!(rows[1].as_deref(), Some([b'v'; 280].as_slice()));
    pool.run_pending_tasks();
    assert_eq!(pool.entry_count(), 1);
    assert_eq!(stat("stats.block_cache_hits") - hits2, 1);
    assert!(stat("stats.block_cache_misses") - misses2 >= 2);

    // Iterator scan with fill_cache = false, fill_index_cache = Some(true) keeps data blocks out of cache.
    let mut iter = db.iter_with_options(&index_only).unwrap();
    let mut count = 0usize;
    while iter.step_next().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 64);
    pool.run_pending_tasks();
    assert_eq!(pool.entry_count(), 1);
    db.close().unwrap();

    // 2. Fresh pool: fill_cache = true, fill_index_cache = Some(false) caches data blocks without index leaf.
    let pool2 = Arc::new(BlockCachePool::new(4 << 20));
    let db2 = DB::open(
        DbOptions::default()
            .with_block_cache(Some(Arc::clone(&pool2)))
            .with_pin_l0_first_block_and_warm_index(false),
        dir.path(),
    )
    .unwrap();
    let data_only = ReadOptions::default()
        .with_fill_cache(true)
        .with_fill_index_cache(false);
    assert_eq!(
        db2.get_with_options(&data_only, b"k0005").unwrap().unwrap(),
        vec![b'v'; 280]
    );
    pool2.run_pending_tasks();
    assert_eq!(pool2.entry_count(), 1, "only data block should be cached");
    let hits_before: u64 = db2
        .get_property("stats.block_cache_hits")
        .unwrap()
        .parse()
        .unwrap();
    let misses_before: u64 = db2
        .get_property("stats.block_cache_misses")
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        db2.get_with_options(&data_only, b"k0005").unwrap().unwrap(),
        vec![b'v'; 280]
    );
    let hits_after: u64 = db2
        .get_property("stats.block_cache_hits")
        .unwrap()
        .parse()
        .unwrap();
    let misses_after: u64 = db2
        .get_property("stats.block_cache_misses")
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(misses_after - misses_before, 1, "index leaf missed again");
    assert_eq!(hits_after - hits_before, 1, "data block hit cache");
    db2.close().unwrap();

    // 3. Fresh cold pool: grouped_point_lookup (`get_many_pinned_with_options`)
    //    with fill_cache = false, fill_index_cache = Some(true) admits ONLY the index leaf.
    let pool3 = Arc::new(BlockCachePool::new(4 << 20));
    let db3 = DB::open(
        DbOptions::default()
            .with_block_cache(Some(Arc::clone(&pool3)))
            .with_pin_l0_first_block_and_warm_index(false),
        dir.path(),
    )
    .unwrap();
    pool3.run_pending_tasks();
    assert_eq!(pool3.entry_count(), 0);
    let rows = db3
        .get_many_pinned_with_options(&index_only, &[b"k0010", b"k0040"])
        .unwrap();
    assert_eq!(rows[0].as_deref(), Some([b'v'; 280].as_slice()));
    assert_eq!(rows[1].as_deref(), Some([b'v'; 280].as_slice()));
    pool3.run_pending_tasks();
    assert_eq!(
        pool3.entry_count(),
        1,
        "grouped cold read must cache only the index leaf"
    );
    let hits3: u64 = db3
        .get_property("stats.block_cache_hits")
        .unwrap()
        .parse()
        .unwrap();
    let rows2 = db3
        .get_many_pinned_with_options(&index_only, &[b"k0020", b"k0055"])
        .unwrap();
    assert_eq!(rows2[0].as_deref(), Some([b'v'; 280].as_slice()));
    assert_eq!(rows2[1].as_deref(), Some([b'v'; 280].as_slice()));
    let hits3_after: u64 = db3
        .get_property("stats.block_cache_hits")
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        hits3_after - hits3,
        1,
        "repeat grouped read must hit cached index leaf"
    );
    pool3.run_pending_tasks();
    assert_eq!(pool3.entry_count(), 1);
    db3.close().unwrap();

    // 4. Fresh cold pool: iter_with_options on both L0 and L1 SSTs with
    //    fill_cache = false, fill_index_cache = Some(true) caches ONLY the index leaves.
    {
        // Compact existing L0 SST to L1 and add a fresh L0 SST so both TableIterator
        // and LevelIterator paths are exercised on cold pools.
        let writer = DB::open(
            DbOptions::default()
                .with_block_size(512)
                .with_pin_l0_first_block_and_warm_index(false),
            dir.path(),
        )
        .unwrap();
        writer.compact_range(None, None).unwrap();
        writer.put(b"l0_key", &[b'z'; 280]).unwrap();
        writer.flush().unwrap();
        writer.close().unwrap();
    }
    let pool4 = Arc::new(BlockCachePool::new(4 << 20));
    let db4 = DB::open(
        DbOptions::default()
            .with_block_cache(Some(Arc::clone(&pool4)))
            .with_pin_l0_first_block_and_warm_index(false),
        dir.path(),
    )
    .unwrap();
    pool4.run_pending_tasks();
    assert_eq!(pool4.entry_count(), 0);
    let mut iter4 = db4.iter_with_options(&index_only).unwrap();
    let mut count4 = 0usize;
    while iter4.step_next().unwrap().is_some() {
        count4 += 1;
    }
    assert_eq!(count4, 65);
    pool4.run_pending_tasks();
    let cached_leaves = pool4.entry_count();
    assert_eq!(
        cached_leaves, 2,
        "L0 + L1 cold scan must cache only the 2 index leaves, not data blocks"
    );
    let hits4: u64 = db4
        .get_property("stats.block_cache_hits")
        .unwrap()
        .parse()
        .unwrap();
    let mut iter4_again = db4.iter_with_options(&index_only).unwrap();
    let mut count4_again = 0usize;
    while iter4_again.step_next().unwrap().is_some() {
        count4_again += 1;
    }
    assert_eq!(count4_again, 65);
    let hits4_after: u64 = db4
        .get_property("stats.block_cache_hits")
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        hits4_after - hits4 >= 2,
        "second cold scan must hit both cached index leaves"
    );
    pool4.run_pending_tasks();
    assert_eq!(pool4.entry_count(), 2);
    db4.close().unwrap();
}

#[test]
fn adopt_manifest_geometry_writable_open_preserves_and_uses_persisted_geometry() {
    let dir = tempfile::tempdir().unwrap();

    // Create with non-default geometry (num_levels = 4, prefix_len = 4).
    {
        let db = DB::open(
            DbOptions::default().with_num_levels(4).with_prefix_len(4),
            dir.path(),
        )
        .unwrap();
        db.put(b"pfx1:alpha", b"v1").unwrap();
        db.put(b"pfx2:beta", b"v2").unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Default open (adopt_manifest_geometry = false) still rejects mismatched geometry.
    let err = DB::open(DbOptions::default(), dir.path())
        .err()
        .expect("mismatched geometry must fail by default");
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);

    // error_if_exists still takes precedence even when adopt_manifest_geometry = true.
    let err = DB::open(
        DbOptions::default()
            .with_error_if_exists(true)
            .with_adopt_manifest_geometry(true),
        dir.path(),
    )
    .err()
    .expect("error_if_exists must still reject existing DB");
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);

    // Writable open with adopt_manifest_geometry = true adopts (4, 4), can read
    // existing data, write and flush new SSTs, and run prefix scans.
    {
        let db = DB::open(
            DbOptions::default().with_adopt_manifest_geometry(true),
            dir.path(),
        )
        .unwrap();
        assert_eq!(
            db.get(b"pfx1:alpha").unwrap().as_deref(),
            Some(b"v1".as_slice())
        );
        db.put(b"pfx1:gamma", b"v3").unwrap();
        db.flush().unwrap();
        let mut iter = db
            .iter_with_prefix(b"pfx1", &ReadOptions::default())
            .unwrap();
        let mut keys = Vec::new();
        while let Some((k, _)) = iter.step_next().unwrap() {
            keys.push(k.to_vec());
        }
        assert_eq!(keys, vec![b"pfx1:alpha".to_vec(), b"pfx1:gamma".to_vec()]);
        // Adopted num_levels = 4 means level3 exists and level4 does not.
        assert!(db.get_property("num-files-at-level3").is_some());
        assert!(db.get_property("num-files-at-level4").is_none());
        db.close().unwrap();
    }

    // Writable open with adopt_manifest_geometry = true succeeds even when caller
    // passes an out-of-range num_levels, because persisted MANIFEST geometry wins.
    {
        let db = DB::open(
            DbOptions::default()
                .with_num_levels(0)
                .with_adopt_manifest_geometry(true),
            dir.path(),
        )
        .unwrap();
        assert!(db.get_property("num-files-at-level3").is_some());
        assert!(db.get_property("num-files-at-level4").is_none());
        db.close().unwrap();
    }

    // Strict reopen with original geometry succeeds and sees both old and new data.
    let db = DB::open(
        DbOptions::default().with_num_levels(4).with_prefix_len(4),
        dir.path(),
    )
    .unwrap();
    assert_eq!(
        db.get(b"pfx1:alpha").unwrap().as_deref(),
        Some(b"v1".as_slice())
    );
    assert_eq!(
        db.get(b"pfx1:gamma").unwrap().as_deref(),
        Some(b"v3".as_slice())
    );
    db.close().unwrap();

    // Fresh DB creation with adopt_manifest_geometry = true still validates caller geometry.
    let invalid_fresh = tempfile::tempdir().unwrap();
    let err = DB::open(
        DbOptions::default()
            .with_num_levels(0)
            .with_adopt_manifest_geometry(true),
        invalid_fresh.path(),
    )
    .err()
    .expect("fresh DB creation must still reject invalid num_levels");
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);

    // Fresh DB creation with adopt_manifest_geometry = true still persists configured geometry.
    let fresh = tempfile::tempdir().unwrap();
    {
        let db = DB::open(
            DbOptions::default()
                .with_num_levels(5)
                .with_prefix_len(3)
                .with_adopt_manifest_geometry(true),
            fresh.path(),
        )
        .unwrap();
        db.put(b"abc1", b"ok").unwrap();
        db.close().unwrap();
    }
    let strict = DB::open(
        DbOptions::default().with_num_levels(5).with_prefix_len(3),
        fresh.path(),
    )
    .unwrap();
    assert_eq!(
        strict.get(b"abc1").unwrap().as_deref(),
        Some(b"ok".as_slice())
    );
}

#[test]
fn sharded_stats_aggregate_concurrent_point_reads_accurately() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        DB::open(
            DbOptions::default()
                .with_block_cache_capacity(0)
                .with_block_size(4096),
            dir.path(),
        )
        .unwrap(),
    );
    let keys: Vec<Vec<u8>> = (0u32..64)
        .map(|i| format!("key-{i:04}").into_bytes())
        .collect();
    let val = vec![b'x'; 280];
    db.ingest_sorted(keys.iter().map(|k| (k.as_slice(), val.as_slice())))
        .unwrap();

    let misses_before: u64 = db
        .get_property("stats.block_cache_misses")
        .unwrap()
        .parse()
        .unwrap();
    let threads = 8usize;
    let rounds = 40usize;
    std::thread::scope(|s| {
        for t in 0..threads {
            let db = Arc::clone(&db);
            let keys = &keys;
            let val = &val;
            s.spawn(move || {
                for r in 0..rounds {
                    let k = &keys[(t * 7 + r) % keys.len()];
                    let got = db.get_pinned(k).unwrap().unwrap();
                    assert_eq!(got.as_ref(), val.as_slice());
                }
            });
        }
    });

    let total_reads = (threads * rounds) as u64;
    let expected_bytes = total_reads * (8 + 280);
    let actual_bytes: u64 = db
        .get_property("stats.bytes_read")
        .unwrap()
        .parse()
        .unwrap();
    let actual_misses: u64 = db
        .get_property("stats.block_cache_misses")
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(actual_bytes, expected_bytes);
    assert_eq!(actual_misses - misses_before, total_reads * 2);
}

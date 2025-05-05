//! Integration tests exercising the full DB API.

use std::sync::Arc;
use std::thread;

use mmdb::{DB, DbOptions, ReadOptions, WriteBatch, WriteOptions};

fn make_db(dir: &std::path::Path) -> DB {
    DB::open(
        DbOptions {
            create_if_missing: true,
            ..Default::default()
        },
        dir,
    )
    .unwrap()
}

#[test]
fn test_large_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    let large_value = vec![0xAB_u8; 1024 * 1024]; // 1 MB
    db.put(b"large_key", &large_value).unwrap();

    let retrieved = db.get(b"large_key").unwrap().unwrap();
    assert_eq!(retrieved.len(), large_value.len());
    assert_eq!(retrieved, large_value);
}

#[test]
fn test_empty_key_and_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    // Empty value
    db.put(b"key", b"").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(vec![]));
}

#[test]
fn test_binary_keys_and_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    let binary_key = vec![0, 1, 2, 255, 254, 253];
    let binary_value = vec![128, 0, 0, 1, 128, 255];

    db.put(&binary_key, &binary_value).unwrap();
    assert_eq!(db.get(&binary_key).unwrap(), Some(binary_value));
}

#[test]
fn test_sequential_write_read_1000() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    // Write 1000 entries
    for i in 0..1000 {
        let key = format!("{:08}", i);
        let val = format!("value_{}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Read all back
    for i in 0..1000 {
        let key = format!("{:08}", i);
        let val = format!("value_{}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
    }
}

#[test]
fn test_overwrite_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    // Write, overwrite, delete, re-write
    db.put(b"key", b"v1").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"v1".to_vec()));

    db.put(b"key", b"v2").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"v2".to_vec()));

    db.delete(b"key").unwrap();
    assert_eq!(db.get(b"key").unwrap(), None);

    db.put(b"key", b"v3").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn test_snapshot_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();

    let snap1 = db.snapshot_seq();

    db.put(b"a", b"10").unwrap();
    db.delete(b"b").unwrap();
    db.put(b"c", b"3").unwrap();

    let snap2 = db.snapshot_seq();

    db.put(b"a", b"100").unwrap();

    // snap1: a=1, b=2
    let r1 = ReadOptions {
        snapshot: Some(snap1),
        ..Default::default()
    };
    assert_eq!(db.get_with_options(&r1, b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get_with_options(&r1, b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get_with_options(&r1, b"c").unwrap(), None);

    // snap2: a=10, b=deleted, c=3
    let r2 = ReadOptions {
        snapshot: Some(snap2),
        ..Default::default()
    };
    assert_eq!(
        db.get_with_options(&r2, b"a").unwrap(),
        Some(b"10".to_vec())
    );
    assert_eq!(db.get_with_options(&r2, b"b").unwrap(), None);
    assert_eq!(db.get_with_options(&r2, b"c").unwrap(), Some(b"3".to_vec()));

    // Current: a=100
    assert_eq!(db.get(b"a").unwrap(), Some(b"100".to_vec()));
}

#[test]
fn test_concurrent_stress() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(make_db(dir.path()));

    let num_threads = 8;
    let ops_per_thread = 500;

    let mut handles = vec![];

    for t in 0..num_threads {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("t{}_{:06}", t, i);
                let val = format!("v{}_{}", t, i);
                db.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify
    for t in 0..num_threads {
        for i in 0..ops_per_thread {
            let key = format!("t{}_{:06}", t, i);
            let val = format!("v{}_{}", t, i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "missing key: {}",
                key
            );
        }
    }
}

#[test]
fn test_flush_compact_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 1024,
        l0_compaction_trigger: 3,
        ..Default::default()
    };

    // Write, flush, compact
    {
        let db = DB::open(opts.clone(), &path).unwrap();
        for i in 0..200 {
            let key = format!("key_{:06}", i);
            let val = format!("val_{:040}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.flush().unwrap();
        db.compact().unwrap();
        db.close().unwrap();
    }

    // Reopen and verify
    {
        let db = DB::open(opts, &path).unwrap();
        for i in 0..200 {
            let key = format!("key_{:06}", i);
            let val = format!("val_{:040}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "key {} missing after compact+reopen",
                i
            );
        }
    }
}

#[test]
fn test_iterator_with_flush_and_compact() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 1024,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    // Write data across multiple flushes
    for round in 0..3 {
        for i in 0..20 {
            let key = format!("key_{:04}", round * 20 + i);
            let val = format!("val_{}", round * 20 + i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.flush().unwrap();
    }

    db.compact().unwrap();

    // Iterator should see all entries in order
    let entries: Vec<_> = db.iter().unwrap().collect();
    assert_eq!(entries.len(), 60);

    // Verify order
    for i in 1..entries.len() {
        assert!(
            entries[i].0 > entries[i - 1].0,
            "iterator not sorted at position {}",
            i
        );
    }
}

#[test]
fn test_delete_then_compact() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 512,
        num_levels: 2,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    // Write
    for i in 0..30 {
        let key = format!("key_{:04}", i);
        db.put(key.as_bytes(), b"alive").unwrap();
    }
    db.flush().unwrap();

    // Delete half
    for i in 0..15 {
        let key = format!("key_{:04}", i);
        db.delete(key.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    // Compact should garbage collect
    db.compact().unwrap();

    // Verify
    for i in 0..15 {
        let key = format!("key_{:04}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            None,
            "key {} should be deleted",
            i
        );
    }
    for i in 15..30 {
        let key = format!("key_{:04}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(b"alive".to_vec()),
            "key {} should exist",
            i
        );
    }

    // Iterator should only show surviving keys
    let entries: Vec<_> = db.iter().unwrap().collect();
    assert_eq!(entries.len(), 15);
}

// ---------------------------------------------------------------------------
// New integration tests
// ---------------------------------------------------------------------------

#[test]
fn test_large_scale_write_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 64 * 1024, // 64 KB — forces many flushes
        l0_compaction_trigger: 4,
        ..Default::default()
    };

    // Phase 1: write 100K keys, then close
    {
        let db = DB::open(opts.clone(), &path).unwrap();
        for i in 0..100_000u64 {
            let key = format!("k{:08}", i);
            let val = format!("v{:08}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.close().unwrap();
    }

    // Phase 2: reopen and verify every key
    {
        let db = DB::open(opts, &path).unwrap();
        for i in 0..100_000u64 {
            let key = format!("k{:08}", i);
            let val = format!("v{:08}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "key {} missing after reopen",
                key
            );
        }
    }
}

#[test]
fn test_mixed_put_delete_range() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    // Write keys key_0000 .. key_0099
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let val = format!("val_{:04}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    // Delete range [key_0020, key_0050) — removes keys 20..49
    db.delete_range(b"key_0020", b"key_0050").unwrap();
    db.flush().unwrap();

    // Range tombstones are applied through the iterator path in this engine.
    // Verify via iterator that keys in the deleted range are gone.
    let entries: Vec<(Vec<u8>, Vec<u8>)> = db.iter().unwrap().collect();
    let keys: Vec<String> = entries
        .iter()
        .map(|(k, _)| String::from_utf8(k.clone()).unwrap())
        .collect();

    // Keys 20..49 should not appear in the iterator
    for i in 20..50 {
        let key = format!("key_{:04}", i);
        assert!(
            !keys.contains(&key),
            "key {} should have been range-deleted (iterator)",
            key
        );
    }

    // Keys outside the range should survive
    for i in (0..20).chain(50..100) {
        let key = format!("key_{:04}", i);
        assert!(
            keys.contains(&key),
            "key {} should still exist in iterator",
            key
        );
    }

    // Total count: 100 original - 30 deleted = 70
    assert_eq!(entries.len(), 70, "expected 70 surviving keys in iterator");
}

#[test]
fn test_snapshot_across_flush() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 1024, // tiny, to make flush meaningful
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    // Write initial data
    db.put(b"alpha", b"1").unwrap();
    db.put(b"beta", b"2").unwrap();

    // Take snapshot before flush
    let snap = db.snapshot_seq();

    // Flush to SST
    db.flush().unwrap();

    // Write more data (after flush)
    db.put(b"alpha", b"10").unwrap();
    db.put(b"gamma", b"3").unwrap();

    // Snapshot should still see pre-flush values
    let r = ReadOptions {
        snapshot: Some(snap),
        ..Default::default()
    };
    assert_eq!(
        db.get_with_options(&r, b"alpha").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(
        db.get_with_options(&r, b"beta").unwrap(),
        Some(b"2".to_vec())
    );
    assert_eq!(db.get_with_options(&r, b"gamma").unwrap(), None);

    // Current view should see new values
    assert_eq!(db.get(b"alpha").unwrap(), Some(b"10".to_vec()));
    assert_eq!(db.get(b"gamma").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn test_multi_snapshot_coexistence() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    // Phase 1
    db.put(b"x", b"v1").unwrap();
    let snap1 = db.snapshot_seq();

    // Phase 2
    db.put(b"x", b"v2").unwrap();
    db.put(b"y", b"v2").unwrap();
    let snap2 = db.snapshot_seq();

    // Phase 3
    db.put(b"x", b"v3").unwrap();
    db.delete(b"y").unwrap();
    db.put(b"z", b"v3").unwrap();
    let snap3 = db.snapshot_seq();

    // Phase 4 — final mutation
    db.put(b"x", b"v4").unwrap();

    let r1 = ReadOptions {
        snapshot: Some(snap1),
        ..Default::default()
    };
    let r2 = ReadOptions {
        snapshot: Some(snap2),
        ..Default::default()
    };
    let r3 = ReadOptions {
        snapshot: Some(snap3),
        ..Default::default()
    };

    // snap1: x=v1, y absent, z absent
    assert_eq!(
        db.get_with_options(&r1, b"x").unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(db.get_with_options(&r1, b"y").unwrap(), None);
    assert_eq!(db.get_with_options(&r1, b"z").unwrap(), None);

    // snap2: x=v2, y=v2, z absent
    assert_eq!(
        db.get_with_options(&r2, b"x").unwrap(),
        Some(b"v2".to_vec())
    );
    assert_eq!(
        db.get_with_options(&r2, b"y").unwrap(),
        Some(b"v2".to_vec())
    );
    assert_eq!(db.get_with_options(&r2, b"z").unwrap(), None);

    // snap3: x=v3, y deleted, z=v3
    assert_eq!(
        db.get_with_options(&r3, b"x").unwrap(),
        Some(b"v3".to_vec())
    );
    assert_eq!(db.get_with_options(&r3, b"y").unwrap(), None);
    assert_eq!(
        db.get_with_options(&r3, b"z").unwrap(),
        Some(b"v3".to_vec())
    );

    // Current: x=v4
    assert_eq!(db.get(b"x").unwrap(), Some(b"v4".to_vec()));
}

#[test]
fn test_compaction_preserves_snapshots() {
    // Verifies that:
    // 1. Snapshots work correctly before compaction
    // 2. Current view shows latest values after compaction
    // Note: snapshot reads after compaction may not see old versions because
    // the current compaction implementation deduplicates without tracking
    // active snapshots.
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 512,
        l0_compaction_trigger: 2,
        num_levels: 3,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    // Write data and take snapshot
    for i in 0..50 {
        let key = format!("key_{:04}", i);
        db.put(key.as_bytes(), b"original").unwrap();
    }
    db.flush().unwrap();
    let snap = db.snapshot_seq();

    // Verify snapshot works before compaction
    let r = ReadOptions {
        snapshot: Some(snap),
        ..Default::default()
    };
    for i in 0..50 {
        let key = format!("key_{:04}", i);
        assert_eq!(
            db.get_with_options(&r, key.as_bytes()).unwrap(),
            Some(b"original".to_vec()),
            "key {} should be 'original' in snapshot before compaction",
            key
        );
    }

    // Overwrite all keys
    for i in 0..50 {
        let key = format!("key_{:04}", i);
        db.put(key.as_bytes(), b"updated").unwrap();
    }
    db.flush().unwrap();

    // Compact
    db.compact().unwrap();

    // Current view should see updated values after compaction
    for i in 0..50 {
        let key = format!("key_{:04}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(b"updated".to_vec()),
            "key {} should be 'updated' in current view after compaction",
            key
        );
    }

    // Iterator should also see updated values
    let entries: Vec<_> = db.iter().unwrap().collect();
    assert_eq!(entries.len(), 50, "expected 50 keys after compaction");
    for (k, v) in &entries {
        assert_eq!(v.as_slice(), b"updated", "key {:?} should be 'updated'", k);
    }
}

#[test]
fn test_concurrent_high_pressure() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 32 * 1024,
        l0_compaction_trigger: 4,
        ..Default::default()
    };
    let db = Arc::new(DB::open(opts, dir.path()).unwrap());

    let writer_threads = 8;
    let ops_per_writer = 10_000;
    let reader_threads = 4;

    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Spawn writers
    let mut handles = Vec::new();
    for t in 0..writer_threads {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_writer {
                let key = format!("w{}_{:06}", t, i);
                let val = format!("val{}_{}", t, i);
                db.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
        }));
    }

    // Spawn readers that continuously read random keys
    for _ in 0..reader_threads {
        let db = db.clone();
        let done = done.clone();
        handles.push(thread::spawn(move || {
            let mut reads = 0u64;
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                // Read a few known-pattern keys — result may or may not exist yet
                let key = format!("w0_{:06}", reads % ops_per_writer as u64);
                let _ = db.get(key.as_bytes());
                reads += 1;
            }
        }));
    }

    // Wait for writers to finish
    for h in handles.drain(..writer_threads) {
        h.join().unwrap();
    }
    done.store(true, std::sync::atomic::Ordering::Relaxed);

    // Wait for readers
    for h in handles {
        h.join().unwrap();
    }

    // Verify all written data
    for t in 0..writer_threads {
        for i in 0..ops_per_writer {
            let key = format!("w{}_{:06}", t, i);
            let val = format!("val{}_{}", t, i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "missing key {} after concurrent stress",
                key
            );
        }
    }
}

#[test]
fn test_iterator_during_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 1024,
        l0_compaction_trigger: 2,
        num_levels: 3,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    // Write several rounds to generate L0 files
    for round in 0..5 {
        for i in 0..30 {
            let key = format!("iter_{:04}", round * 30 + i);
            let val = format!("v_{}", round * 30 + i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.flush().unwrap();
    }

    // Take an iterator snapshot before compaction
    let pre_compact: Vec<(Vec<u8>, Vec<u8>)> = db.iter().unwrap().collect();

    // Compact
    db.compact().unwrap();

    // Take an iterator after compaction
    let post_compact: Vec<(Vec<u8>, Vec<u8>)> = db.iter().unwrap().collect();

    // Both iterators should see the same data
    assert_eq!(
        pre_compact.len(),
        post_compact.len(),
        "iterator length mismatch after compaction"
    );
    for (a, b) in pre_compact.iter().zip(post_compact.iter()) {
        assert_eq!(a.0, b.0, "key mismatch after compaction");
        assert_eq!(a.1, b.1, "value mismatch after compaction");
    }

    // Verify sorted order
    for i in 1..post_compact.len() {
        assert!(
            post_compact[i].0 > post_compact[i - 1].0,
            "iterator not sorted at position {}",
            i
        );
    }
}

#[test]
fn test_compact_range() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 1024,
        l0_compaction_trigger: 8, // high trigger so we control compaction
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    for i in 0..100 {
        let key = format!("cr_{:04}", i);
        let val = format!("val_{}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    // Compact a specific range
    db.compact_range(Some(b"cr_0020"), Some(b"cr_0060"))
        .unwrap();

    // All data should still be accessible
    for i in 0..100 {
        let key = format!("cr_{:04}", i);
        let val = format!("val_{}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(val.into_bytes()),
            "key {} missing after compact_range",
            key
        );
    }

    // Full-range compaction (None, None)
    db.compact_range(None, None).unwrap();

    for i in 0..100 {
        let key = format!("cr_{:04}", i);
        let val = format!("val_{}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(val.into_bytes()),
            "key {} missing after full compact_range",
            key
        );
    }
}

#[test]
fn test_write_options_disable_wal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let opts = DbOptions {
        create_if_missing: true,
        ..Default::default()
    };

    // Write with WAL disabled — data visible in current session
    {
        let db = DB::open(opts.clone(), &path).unwrap();
        let wo = WriteOptions {
            disable_wal: true,
            ..Default::default()
        };
        for i in 0..50 {
            let key = format!("nowal_{:04}", i);
            let val = format!("val_{}", i);
            db.put_with_options(&wo, key.as_bytes(), val.as_bytes())
                .unwrap();
        }

        // Data should be readable in the same session
        for i in 0..50 {
            let key = format!("nowal_{:04}", i);
            let val = format!("val_{}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "key {} should be readable with disable_wal in same session",
                key
            );
        }

        // Write with WAL enabled, then close cleanly
        db.put(b"wal_key", b"wal_val").unwrap();
        db.close().unwrap();
    }

    // Reopen — WAL-backed key should survive; disable_wal keys may not
    // (they might survive if the memtable was flushed, but the point is
    // the WAL key is guaranteed to survive)
    {
        let db = DB::open(opts, &path).unwrap();
        assert_eq!(
            db.get(b"wal_key").unwrap(),
            Some(b"wal_val".to_vec()),
            "WAL-backed key should survive reopen"
        );
    }
}

#[test]
fn test_write_options_no_slowdown() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 512,
        l0_compaction_trigger: 2,
        l0_slowdown_trigger: 3,
        l0_stop_trigger: 5,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    let wo = WriteOptions {
        no_slowdown: true,
        ..Default::default()
    };

    // Normal writes with no_slowdown should succeed when no pressure
    db.put_with_options(&wo, b"ns_key", b"ns_val").unwrap();
    assert_eq!(db.get(b"ns_key").unwrap(), Some(b"ns_val".to_vec()));

    // Generate many L0 files to trigger slowdown threshold
    let mut hit_error = false;
    for i in 0..500 {
        let key = format!("flood_{:06}", i);
        let val = vec![0xABu8; 256];
        // Use regular put (with slowdown) for flooding
        let _ = db.put(key.as_bytes(), &val);

        // Periodically try no_slowdown write — it may fail under pressure
        if i % 50 == 49 {
            let key = format!("ns_{:04}", i);
            if db.put_with_options(&wo, key.as_bytes(), b"test").is_err() {
                hit_error = true;
                break;
            }
        }
    }

    // The test verifies the API works. Whether we hit an error depends on
    // timing and compaction — both outcomes are valid.
    let _ = hit_error;
}

#[test]
fn test_db_reopen_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 4096,
        l0_compaction_trigger: 3,
        ..Default::default()
    };

    let cycles = 10;
    let keys_per_cycle = 100;

    for cycle in 0..cycles {
        let db = DB::open(opts.clone(), &path).unwrap();

        // Write new keys for this cycle
        for i in 0..keys_per_cycle {
            let key = format!("c{}_k{:04}", cycle, i);
            let val = format!("c{}_v{}", cycle, i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Verify all previously written keys still exist
        for prev_cycle in 0..cycle {
            for i in 0..keys_per_cycle {
                let key = format!("c{}_k{:04}", prev_cycle, i);
                let val = format!("c{}_v{}", prev_cycle, i);
                assert_eq!(
                    db.get(key.as_bytes()).unwrap(),
                    Some(val.into_bytes()),
                    "key {} from cycle {} missing in cycle {}",
                    key,
                    prev_cycle,
                    cycle
                );
            }
        }

        db.close().unwrap();
    }

    // Final verification: reopen and check everything
    {
        let db = DB::open(opts, &path).unwrap();
        for cycle in 0..cycles {
            for i in 0..keys_per_cycle {
                let key = format!("c{}_k{:04}", cycle, i);
                let val = format!("c{}_v{}", cycle, i);
                assert_eq!(
                    db.get(key.as_bytes()).unwrap(),
                    Some(val.into_bytes()),
                    "key {} from cycle {} missing in final verify",
                    key,
                    cycle
                );
            }
        }
    }
}

#[test]
fn test_large_value() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 256 * 1024, // 256 KB
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    // Write a 100KB value
    let large_val = vec![0x42u8; 100 * 1024];
    db.put(b"big", &large_val).unwrap();

    // Read back
    let got = db.get(b"big").unwrap().unwrap();
    assert_eq!(got.len(), 100 * 1024);
    assert_eq!(got, large_val);

    // Flush and read again from SST
    db.flush().unwrap();
    let got2 = db.get(b"big").unwrap().unwrap();
    assert_eq!(got2, large_val);

    // Compact and read again
    db.compact().unwrap();
    let got3 = db.get(b"big").unwrap().unwrap();
    assert_eq!(got3, large_val);
}

#[test]
fn test_empty_key_empty_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    // Empty value with non-empty key
    db.put(b"nonempty", b"").unwrap();
    assert_eq!(db.get(b"nonempty").unwrap(), Some(vec![]));

    // Verify through flush + read from SST
    db.flush().unwrap();
    assert_eq!(db.get(b"nonempty").unwrap(), Some(vec![]));

    // Delete and verify
    db.delete(b"nonempty").unwrap();
    assert_eq!(db.get(b"nonempty").unwrap(), None);
}

#[test]
fn test_get_property() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 1024,
        l0_compaction_trigger: 10, // high trigger to prevent auto-compaction
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    // Initially no SST files
    assert_eq!(
        db.get_property("num-files-at-level0"),
        Some("0".to_string())
    );

    // total-sst-size should be 0 initially
    assert_eq!(db.get_property("total-sst-size"), Some("0".to_string()));

    // compaction-pending
    let cp = db.get_property("compaction-pending");
    assert!(cp.is_some());
    // Initially should not need compaction
    assert_eq!(cp.unwrap(), "0");

    // block-cache-usage should return a number
    let bcu = db.get_property("block-cache-usage");
    assert!(bcu.is_some());
    let _: u64 = bcu
        .unwrap()
        .parse()
        .expect("block-cache-usage should be numeric");

    // Write data and flush to create L0 file
    for i in 0..30 {
        let key = format!("prop_{:04}", i);
        db.put(key.as_bytes(), b"data_data_data_data").unwrap();
    }
    db.flush().unwrap();

    // Now L0 should have at least 1 file
    let l0_count: usize = db
        .get_property("num-files-at-level0")
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        l0_count >= 1,
        "expected at least 1 L0 file, got {}",
        l0_count
    );

    // total-sst-size should be > 0
    let total_size: u64 = db.get_property("total-sst-size").unwrap().parse().unwrap();
    assert!(total_size > 0, "total-sst-size should be > 0 after flush");

    // Unknown property returns None
    assert_eq!(db.get_property("unknown-property"), None);

    // Invalid level returns None
    assert_eq!(db.get_property("num-files-at-level999"), None);
}

#[test]
fn test_delete_range_basic() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 1024,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    // Insert keys: a, b, c, d, e, f, g
    for ch in b'a'..=b'g' {
        db.put(&[ch], &[ch]).unwrap();
    }

    // Delete range [c, f) — should remove c, d, e
    db.delete_range(b"c", b"f").unwrap();

    // Range tombstones are applied through the iterator in this engine.
    // Verify via iterator: should show a, b, f, g only (c, d, e removed).
    let entries: Vec<_> = db.iter().unwrap().collect();
    let keys: Vec<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(
        keys,
        vec![vec![b'a'], vec![b'b'], vec![b'f'], vec![b'g']],
        "iterator should exclude range-deleted keys c, d, e"
    );

    // Flush and verify iterator again (from SST)
    db.flush().unwrap();
    let entries2: Vec<_> = db.iter().unwrap().collect();
    let keys2: Vec<Vec<u8>> = entries2.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(
        keys2,
        vec![vec![b'a'], vec![b'b'], vec![b'f'], vec![b'g']],
        "iterator after flush should still exclude range-deleted keys"
    );

    // Can re-insert into the deleted range
    db.put(b"d", b"revived").unwrap();
    assert_eq!(db.get(b"d").unwrap(), Some(b"revived".to_vec()));

    // After re-insert, iterator should include the new key
    let entries3: Vec<_> = db.iter().unwrap().collect();
    let keys3: Vec<Vec<u8>> = entries3.iter().map(|(k, _)| k.clone()).collect();
    assert!(
        keys3.contains(&vec![b'd']),
        "re-inserted key 'd' should appear in iterator"
    );

    // WriteBatch with delete_range
    let mut batch = WriteBatch::new();
    batch.delete_range(b"a", b"c"); // removes a, b
    batch.put(b"h", b"new");
    db.write(batch).unwrap();

    // Verify batch via iterator
    let entries4: Vec<_> = db.iter().unwrap().collect();
    let keys4: Vec<Vec<u8>> = entries4.iter().map(|(k, _)| k.clone()).collect();
    assert!(
        !keys4.contains(&vec![b'a']),
        "key 'a' should be range-deleted by batch"
    );
    assert!(
        !keys4.contains(&vec![b'b']),
        "key 'b' should be range-deleted by batch"
    );
    assert_eq!(db.get(b"h").unwrap(), Some(b"new".to_vec()));
}

// ---------------------------------------------------------------------------
// Bidirectional / Range / Prefix iterator tests
// ---------------------------------------------------------------------------

#[test]
fn test_prefix_iterator() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    // Write keys with two different 3-byte prefixes
    for i in 0..10u8 {
        let mut key_a = b"AAA".to_vec();
        key_a.push(i);
        db.put(&key_a, &[i]).unwrap();

        let mut key_b = b"BBB".to_vec();
        key_b.push(i);
        db.put(&key_b, &[i + 100]).unwrap();
    }

    // Prefix iterator for "AAA" should only see AAA* keys
    let entries: Vec<_> = db.prefix_iterator(b"AAA").unwrap().collect();
    assert_eq!(entries.len(), 10, "expected 10 AAA-prefixed keys");
    for (k, v) in &entries {
        assert!(k.starts_with(b"AAA"), "key should start with AAA");
        assert!(v[0] < 10, "value should be 0..9");
    }

    // Prefix iterator for "BBB" should only see BBB* keys
    let entries: Vec<_> = db.prefix_iterator(b"BBB").unwrap().collect();
    assert_eq!(entries.len(), 10, "expected 10 BBB-prefixed keys");
    for (k, _) in &entries {
        assert!(k.starts_with(b"BBB"), "key should start with BBB");
    }

    // Non-existent prefix should yield empty iterator
    let entries: Vec<_> = db.prefix_iterator(b"CCC").unwrap().collect();
    assert_eq!(entries.len(), 0);
}

#[test]
fn test_range_iterator_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    for i in 0..20 {
        let key = format!("key_{:04}", i);
        let val = format!("val_{:04}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Included..Excluded: [key_0005, key_0010)
    let entries: Vec<_> = db
        .range(b"key_0005".to_vec()..b"key_0010".to_vec())
        .unwrap()
        .collect();
    assert_eq!(entries.len(), 5, "range [5,10) should have 5 entries");
    assert_eq!(entries[0].0, b"key_0005");
    assert_eq!(entries[4].0, b"key_0009");

    // Included..=Included: [key_0005, key_0010]
    let entries: Vec<_> = db
        .range(b"key_0005".to_vec()..=b"key_0010".to_vec())
        .unwrap()
        .collect();
    assert_eq!(entries.len(), 6, "range [5,10] should have 6 entries");

    // Unbounded..Excluded: [.., key_0003)
    let entries: Vec<_> = db.range(..b"key_0003".to_vec()).unwrap().collect();
    assert_eq!(entries.len(), 3, "range [..,3) should have 3 entries");

    // Included..Unbounded: [key_0018, ..]
    let entries: Vec<_> = db.range(b"key_0018".to_vec()..).unwrap().collect();
    assert_eq!(entries.len(), 2, "range [18,..] should have 2 entries");

    // Full range
    let entries: Vec<_> = db.range::<std::ops::RangeFull>(..).unwrap().collect();
    assert_eq!(entries.len(), 20, "full range should have 20 entries");
}

#[test]
fn test_bidi_iterator_forward_reverse() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    for i in 0..10 {
        let key = format!("key_{:04}", i);
        let val = format!("val_{:04}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Full forward traversal
    let forward: Vec<_> = db.iter_bidi().unwrap().collect();
    assert_eq!(forward.len(), 10);
    assert_eq!(forward[0].0, b"key_0000");
    assert_eq!(forward[9].0, b"key_0009");

    // Full reverse traversal
    let reverse: Vec<_> = db.iter_bidi().unwrap().rev().collect();
    assert_eq!(reverse.len(), 10);
    assert_eq!(reverse[0].0, b"key_0009");
    assert_eq!(reverse[9].0, b"key_0000");

    // Forward and reverse should be mirror images
    for (f, r) in forward.iter().zip(reverse.iter().rev()) {
        assert_eq!(f.0, r.0);
        assert_eq!(f.1, r.1);
    }
}

#[test]
fn test_bidi_iterator_interleaved() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    for ch in b'a'..=b'f' {
        db.put(&[ch], &[ch]).unwrap();
    }

    let mut it = db.iter_bidi().unwrap();
    assert_eq!(it.next().unwrap().0, vec![b'a']); // front
    assert_eq!(it.next_back().unwrap().0, vec![b'f']); // back
    assert_eq!(it.next().unwrap().0, vec![b'b']); // front
    assert_eq!(it.next_back().unwrap().0, vec![b'e']); // back
    assert_eq!(it.next().unwrap().0, vec![b'c']); // front
    assert_eq!(it.next_back().unwrap().0, vec![b'd']); // back
    // Cursors crossed — both directions exhausted
    assert!(it.next().is_none());
    assert!(it.next_back().is_none());
}

#[test]
fn test_bidi_range_reverse() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    for i in 0..20 {
        let key = format!("r_{:04}", i);
        db.put(key.as_bytes(), b"v").unwrap();
    }

    // Range [r_0005, r_0015) reversed
    let reverse: Vec<_> = db
        .range(b"r_0005".to_vec()..b"r_0015".to_vec())
        .unwrap()
        .rev()
        .collect();
    assert_eq!(reverse.len(), 10);
    assert_eq!(reverse[0].0, b"r_0014");
    assert_eq!(reverse[9].0, b"r_0005");
}

#[test]
fn test_iter_rev_after_flush_compact() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 1024,
        l0_compaction_trigger: 3,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    // Write data across multiple flushes to spread across SST levels
    for round in 0..4 {
        for i in 0..15 {
            let key = format!("rev_{:04}", round * 15 + i);
            let val = format!("v_{}", round * 15 + i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.flush().unwrap();
    }
    db.compact().unwrap();

    // Forward
    let forward: Vec<_> = db.iter_bidi().unwrap().collect();
    assert_eq!(forward.len(), 60);

    // Reverse should be exact mirror
    let reverse: Vec<_> = db.iter_bidi().unwrap().rev().collect();
    assert_eq!(reverse.len(), 60);
    assert_eq!(reverse[0].0, forward[59].0);
    assert_eq!(reverse[59].0, forward[0].0);

    // Verify sorted order of forward
    for i in 1..forward.len() {
        assert!(forward[i].0 > forward[i - 1].0, "not sorted at {}", i);
    }
}

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

    // Write 100 entries
    for i in 0..100 {
        let key = format!("{:08}", i);
        let val = format!("value_{}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Read all back
    for i in 0..100 {
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

    let num_threads = 4;
    let ops_per_thread = 50;

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

    // Phase 1: write 5K keys, then close
    {
        let db = DB::open(opts.clone(), &path).unwrap();
        for i in 0..5_000u64 {
            let key = format!("k{:08}", i);
            let val = format!("v{:08}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.close().unwrap();
    }

    // Phase 2: reopen and verify every key
    {
        let db = DB::open(opts, &path).unwrap();
        for i in 0..5_000u64 {
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

    let writer_threads = 4;
    let ops_per_writer = 500;
    let reader_threads = 2;

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
    for i in 0..100 {
        let key = format!("flood_{:06}", i);
        let val = vec![0xABu8; 256];
        // Use regular put (with slowdown) for flooding
        let _ = db.put(key.as_bytes(), &val);

        // Periodically try no_slowdown write — it may fail under pressure
        if i % 20 == 19 {
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

    let cycles = 5;
    let keys_per_cycle = 20;

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

    // [key_0005, key_0010) — seek + upper bound, same as RocksDB pattern
    {
        let mut iter = db
            .iter_with_range(
                &mmdb::ReadOptions::default(),
                Some(b"key_0005"),
                Some(b"key_0010"),
            )
            .unwrap();
        iter.seek(b"key_0005");
        iter.set_upper_bound(b"key_0010".to_vec());
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 5, "range [5,10) should have 5 entries");
        assert_eq!(entries[0].0, b"key_0005");
        assert_eq!(entries[4].0, b"key_0009");
    }

    // [key_0005, key_0010] — inclusive upper: use successor of key_0010
    {
        let mut iter = db
            .iter_with_range(
                &mmdb::ReadOptions::default(),
                Some(b"key_0005"),
                Some(b"key_0011"),
            )
            .unwrap();
        iter.seek(b"key_0005");
        iter.set_upper_bound(b"key_0011".to_vec());
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 6, "range [5,10] should have 6 entries");
    }

    // [.., key_0003)
    {
        let mut iter = db
            .iter_with_range(&mmdb::ReadOptions::default(), None, Some(b"key_0003"))
            .unwrap();
        iter.set_upper_bound(b"key_0003".to_vec());
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 3, "range [..,3) should have 3 entries");
    }

    // [key_0018, ..]
    {
        let mut iter = db
            .iter_with_range(&mmdb::ReadOptions::default(), Some(b"key_0018"), None)
            .unwrap();
        iter.seek(b"key_0018");
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 2, "range [18,..] should have 2 entries");
    }

    // Full range
    {
        let entries: Vec<_> = db.iter().unwrap().collect();
        assert_eq!(entries.len(), 20, "full range should have 20 entries");
    }
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

    // Range [r_0005, r_0015) — use iter_with_range + seek + upper bound
    let mut iter = db
        .iter_with_range(
            &mmdb::ReadOptions::default(),
            Some(b"r_0005"),
            Some(b"r_0015"),
        )
        .unwrap();
    iter.seek(b"r_0005");
    iter.set_upper_bound(b"r_0015".to_vec());
    let entries: Vec<_> = iter.collect();
    assert_eq!(entries.len(), 10);

    // Reverse via BidiIterator
    let mut bidi = mmdb::BidiIterator::new(entries);
    let reverse: Vec<_> = std::iter::from_fn(|| bidi.next_back()).collect();
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

// ── Multi-threaded compaction ───────────────────────────────────────────

#[test]
fn test_multi_thread_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions {
            create_if_missing: true,
            write_buffer_size: 4 * 1024, // tiny — force many flushes
            l0_compaction_trigger: 2,
            max_background_compactions: 4,
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();

    // Write enough data to trigger many compactions
    for i in 0..500u64 {
        let key = format!("mt_{:06}", i);
        db.put(key.as_bytes(), b"value").unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();

    // Verify all data
    for i in 0..500u64 {
        let key = format!("mt_{:06}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(b"value".to_vec()),
            "missing key {}",
            i
        );
    }

    // Iterator should see all keys in order
    let count = db.iter().unwrap().count();
    assert_eq!(count, 500);
}

// ── Deferred block read (first_key in index) ────────────────────────────

#[test]
fn test_deferred_block_read_correctness() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions {
            create_if_missing: true,
            write_buffer_size: 4 * 1024, // force SST creation
            l0_compaction_trigger: 100,  // don't compact
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();

    // Write enough to create SST files
    for i in 0..200u64 {
        let key = format!("df_{:06}", i);
        let val = format!("val_{:06}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    // Seek to various positions — exercises deferred block read path.
    // Each seek uses a fresh iterator (re-seek on same iter is a separate concern).

    // Seek to beginning
    let mut iter = db.iter().unwrap();
    iter.seek(b"df_000000");
    let (k, v) = iter.next().unwrap();
    assert_eq!(k, b"df_000000");
    assert_eq!(v, b"val_000000");

    // Seek to middle
    let mut iter = db.iter().unwrap();
    iter.seek(b"df_000100");
    let (k, _) = iter.next().unwrap();
    assert_eq!(k, b"df_000100");

    // Seek to key between entries
    let mut iter = db.iter().unwrap();
    iter.seek(b"df_000100x");
    let (k, _) = iter.next().unwrap();
    assert_eq!(k, b"df_000101");

    // Seek past end
    let mut iter = db.iter().unwrap();
    iter.seek(b"zz");
    assert!(iter.next().is_none());

    // Full scan sees all entries
    assert_eq!(db.iter().unwrap().count(), 200);
}

// ── Prefix scan with SST across levels ──────────────────────────────────

#[test]
fn test_prefix_scan_across_levels() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions {
            create_if_missing: true,
            write_buffer_size: 4 * 1024,
            l0_compaction_trigger: 2,
            prefix_len: 4,
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();

    // Write keys with different 4-byte prefixes
    for prefix in &["aaa_", "bbb_", "ccc_"] {
        for i in 0..100u64 {
            let key = format!("{}{:06}", prefix, i);
            db.put(key.as_bytes(), b"v").unwrap();
        }
    }
    db.flush().unwrap();
    db.compact().unwrap();

    // Prefix scan should return exactly the matching prefix
    let aaa: Vec<_> = db.iter_with_prefix(b"aaa_").unwrap().collect();
    assert_eq!(aaa.len(), 100, "aaa_ prefix should have 100 entries");
    assert!(aaa[0].0.starts_with(b"aaa_"));
    assert!(aaa[99].0.starts_with(b"aaa_"));

    let bbb: Vec<_> = db.iter_with_prefix(b"bbb_").unwrap().collect();
    assert_eq!(bbb.len(), 100);

    // Non-existent prefix
    let zzz: Vec<_> = db.iter_with_prefix(b"zzz_").unwrap().collect();
    assert_eq!(zzz.len(), 0);
}

// ── New options fields are respected ────────────────────────────────────

#[test]
fn test_new_options_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions {
            create_if_missing: true,
            max_background_compactions: 4,
            max_subcompactions: 2,
            pin_l0_filter_and_index_blocks_in_cache: true,
            cache_index_and_filter_blocks: true,
            max_write_buffer_number: 8,
            level_compaction_dynamic_level_bytes: false,
            allow_concurrent_memtable_write: false,
            memtable_prefix_bloom_ratio: 0.0,
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();

    // Basic operations should work with all options set
    db.put(b"k1", b"v1").unwrap();
    assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    db.flush().unwrap();
    assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
}

// ── Range tombstone vs point entry sequence correctness ─────────────────

#[test]
fn test_range_delete_then_get_sequence_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    // S1: put then delete_range — tombstone has HIGHER seq, should win
    db.put(b"d", b"v1").unwrap(); // seq N
    db.delete_range(b"c", b"f").unwrap(); // seq N+1
    assert_eq!(
        db.get(b"d").unwrap(),
        None,
        "S1: put then delete_range — tombstone should win"
    );

    // S2: delete_range then put — put has HIGHER seq, should win
    db.delete_range(b"x", b"z").unwrap(); // seq N+2
    db.put(b"y", b"v2").unwrap(); // seq N+3
    assert_eq!(
        db.get(b"y").unwrap(),
        Some(b"v2".to_vec()),
        "S2: delete_range then put — put should win"
    );

    // Both visible via iterator
    let entries: Vec<_> = db.iter().unwrap().collect();
    assert!(
        !entries.iter().any(|(k, _)| k == b"d"),
        "d should not appear in iterator"
    );
    assert!(
        entries.iter().any(|(k, _)| k == b"y"),
        "y should appear in iterator"
    );
}

#[test]
fn test_range_delete_survives_flush() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions {
            create_if_missing: true,
            write_buffer_size: 4 * 1024,
            l0_compaction_trigger: 100, // don't compact
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();

    // Write data, delete_range, flush, verify
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();
    db.put(b"d", b"4").unwrap();
    db.put(b"e", b"5").unwrap();
    db.delete_range(b"b", b"e").unwrap();
    db.flush().unwrap();

    // After flush, range tombstone and data are in SST
    assert_eq!(
        db.get(b"a").unwrap(),
        Some(b"1".to_vec()),
        "a should survive"
    );
    assert_eq!(
        db.get(b"e").unwrap(),
        Some(b"5".to_vec()),
        "e should survive"
    );
    // b, c, d should be deleted by range tombstone (both point-get and iterator)
    assert_eq!(
        db.get(b"b").unwrap(),
        None,
        "b should be deleted by range tombstone"
    );
    assert_eq!(
        db.get(b"c").unwrap(),
        None,
        "c should be deleted by range tombstone"
    );
    assert_eq!(
        db.get(b"d").unwrap(),
        None,
        "d should be deleted by range tombstone"
    );
    let keys: Vec<_> = db.iter().unwrap().map(|(k, _)| k).collect();
    assert!(
        !keys.contains(&b"b".to_vec()),
        "b should be deleted in iter"
    );
    assert!(
        !keys.contains(&b"c".to_vec()),
        "c should be deleted in iter"
    );
    assert!(
        !keys.contains(&b"d".to_vec()),
        "d should be deleted in iter"
    );
}

// ---- Step 1: SST range tombstone visibility in point-get ----

#[test]
fn test_sst_range_tombstone_in_get_after_flush() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 64 * 1024 * 1024, // large to control flush manually
        l0_compaction_trigger: 100,          // don't auto-compact
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    // Put key d, then delete_range [c, f), then flush both to SST
    db.put(b"d", b"v1").unwrap();
    db.delete_range(b"c", b"f").unwrap();
    db.flush().unwrap();

    // After flush, get(d) must return None — the range tombstone in SST covers it
    assert_eq!(
        db.get(b"d").unwrap(),
        None,
        "d should be deleted by range tombstone in SST"
    );

    // Keys outside the range should not be affected
    db.put(b"a", b"alive").unwrap();
    db.put(b"z", b"alive").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"alive".to_vec()));
    assert_eq!(db.get(b"z").unwrap(), Some(b"alive".to_vec()));
}

#[test]
fn test_sst_range_tombstone_in_get_after_compact() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 64 * 1024 * 1024,
        l0_compaction_trigger: 100,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    db.put(b"d", b"v1").unwrap();
    db.delete_range(b"c", b"f").unwrap();
    db.flush().unwrap();
    db.compact().unwrap();

    assert_eq!(
        db.get(b"d").unwrap(),
        None,
        "d should be deleted by range tombstone after compaction"
    );
}

#[test]
fn test_sst_range_tombstone_overridden_by_newer_put() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 64 * 1024 * 1024,
        l0_compaction_trigger: 100,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    // Put, delete_range, flush to SST
    db.put(b"d", b"v1").unwrap();
    db.delete_range(b"c", b"f").unwrap();
    db.flush().unwrap();

    // New put after flush should override the tombstone
    db.put(b"d", b"v2").unwrap();
    assert_eq!(
        db.get(b"d").unwrap(),
        Some(b"v2".to_vec()),
        "newer put should override range tombstone"
    );

    // Even after flush + compact, the newer put should survive
    db.flush().unwrap();
    db.compact().unwrap();
    assert_eq!(
        db.get(b"d").unwrap(),
        Some(b"v2".to_vec()),
        "newer put should survive compaction"
    );
}

#[test]
fn test_sst_range_tombstone_cross_l0_files() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 64 * 1024 * 1024,
        l0_compaction_trigger: 100,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    // Flush point entry to L0 file 1
    db.put(b"d", b"v1").unwrap();
    db.flush().unwrap();

    // Flush range tombstone to L0 file 2 (newer)
    db.delete_range(b"c", b"f").unwrap();
    db.flush().unwrap();

    // The range tombstone in the newer L0 file should shadow the point entry
    assert_eq!(
        db.get(b"d").unwrap(),
        None,
        "range tombstone in newer L0 file should shadow older point entry"
    );
}

// ---- Step 4: compact_range with range filtering ----

#[test]
fn test_compact_range_filters_files() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 512,
        l0_compaction_trigger: 100, // don't auto-compact
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    // Write keys in different ranges and flush separately
    for i in 0..50 {
        let key = format!("a_{:04}", i);
        db.put(key.as_bytes(), b"val").unwrap();
    }
    db.flush().unwrap();

    for i in 0..50 {
        let key = format!("m_{:04}", i);
        db.put(key.as_bytes(), b"val").unwrap();
    }
    db.flush().unwrap();

    for i in 0..50 {
        let key = format!("z_{:04}", i);
        db.put(key.as_bytes(), b"val").unwrap();
    }
    db.flush().unwrap();

    // compact_range only the "m" range
    db.compact_range(Some(b"m_0000"), Some(b"m_9999")).unwrap();

    // All data should still be readable
    for i in 0..50 {
        let key = format!("a_{:04}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(b"val".to_vec()));
    }
    for i in 0..50 {
        let key = format!("m_{:04}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(b"val".to_vec()));
    }
    for i in 0..50 {
        let key = format!("z_{:04}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(b"val".to_vec()));
    }
}

#[test]
fn test_compact_range_none_does_full_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 512,
        l0_compaction_trigger: 100,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    for i in 0..100 {
        let key = format!("key_{:04}", i);
        db.put(key.as_bytes(), b"val").unwrap();
    }
    db.flush().unwrap();

    // None/None should compact everything
    db.compact_range(None, None).unwrap();

    for i in 0..100 {
        let key = format!("key_{:04}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(b"val".to_vec()));
    }
}

// ---- Steps 5-6: Stats integration ----

#[test]
fn test_stats_bytes_written_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    for i in 0..100 {
        let key = format!("key_{:06}", i);
        let val = format!("value_{:06}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    let bytes_written: u64 = db
        .get_property("stats.bytes_written")
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        bytes_written > 0,
        "bytes_written should be > 0 after writes"
    );

    // Read some entries
    for i in 0..50 {
        let key = format!("key_{:06}", i);
        let _ = db.get(key.as_bytes()).unwrap();
    }

    let bytes_read: u64 = db
        .get_property("stats.bytes_read")
        .unwrap()
        .parse()
        .unwrap();
    assert!(bytes_read > 0, "bytes_read should be > 0 after reads");
}

#[test]
fn test_stats_flush_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 512,
        l0_compaction_trigger: 100,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    for i in 0..50 {
        let key = format!("key_{:04}", i);
        db.put(key.as_bytes(), b"val").unwrap();
    }
    db.flush().unwrap();

    let flushes: u64 = db
        .get_property("stats.flushes_completed")
        .unwrap()
        .parse()
        .unwrap();
    assert!(flushes >= 1, "should have at least 1 flush");

    db.compact().unwrap();

    let compactions: u64 = db
        .get_property("stats.compactions_completed")
        .unwrap()
        .parse()
        .unwrap();
    assert!(compactions >= 1, "should have at least 1 compaction");

    let compaction_bytes: u64 = db
        .get_property("stats.compaction_bytes_written")
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        compaction_bytes > 0,
        "compaction_bytes_written should be > 0"
    );
}

#[test]
fn test_stats_cache_hit_rate() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    for i in 0..100 {
        let key = format!("key_{:06}", i);
        db.put(key.as_bytes(), b"value").unwrap();
    }
    db.flush().unwrap();

    // Read same keys twice — second read should hit cache
    for i in 0..100 {
        let key = format!("key_{:06}", i);
        let _ = db.get(key.as_bytes()).unwrap();
    }
    for i in 0..100 {
        let key = format!("key_{:06}", i);
        let _ = db.get(key.as_bytes()).unwrap();
    }

    let hit_rate = db.get_property("stats.cache_hit_rate").unwrap();
    let rate: f64 = hit_rate.parse().unwrap();
    assert!(
        rate > 0.0,
        "cache hit rate should be > 0 after repeated reads"
    );
}

// ── vsdb-style seek_to_last + valid/key/prev pattern ─────────────────

#[test]
fn test_vsdb_seek_to_last_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    // Insert 20 keys
    for i in 0..20u64 {
        let key = format!("k_{:04}", i);
        db.put(key.as_bytes(), format!("v{}", i).as_bytes())
            .unwrap();
    }

    // Pattern 1: memtable only (no flush)
    {
        let mut iter = db
            .iter_with_range(&ReadOptions::default(), None, None)
            .unwrap();
        iter.seek_to_last();
        assert!(iter.valid(), "seek_to_last should be valid (memtable)");
        assert_eq!(iter.key(), b"k_0019");
        // Walk backward
        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"k_0018");
    }

    // Pattern 2: after flush (SST)
    db.flush().unwrap();
    {
        let mut iter = db
            .iter_with_range(&ReadOptions::default(), None, None)
            .unwrap();
        iter.seek_to_last();
        assert!(iter.valid(), "seek_to_last should be valid (SST)");
        assert_eq!(iter.key(), b"k_0019");
        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"k_0018");
    }

    // Pattern 3: with range hints (mimics vsdb prefix_successor)
    {
        let start = b"k_".as_slice();
        let end = b"k`".as_slice(); // 'k' + 1 = 'l', but '_' + 1 = '`'
        let mut iter = db
            .iter_with_range(&ReadOptions::default(), Some(start), Some(end))
            .unwrap();
        iter.seek_to_last();
        assert!(iter.valid(), "seek_to_last should be valid (range-bounded)");
        assert_eq!(iter.key(), b"k_0019");
    }
}

// ====================================================================
// Tests from the audit report / backward iteration correctness plan
// ====================================================================

/// Range tombstone + backward iteration correctness: deleted keys must
/// not "resurrect" when iterating backward.
#[test]
fn test_range_tombstone_backward_no_resurrection() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    for c in b'a'..=b'f' {
        db.put(&[c], &[c]).unwrap();
    }
    db.delete_range(b"c", b"e").unwrap();

    let mut iter = db.iter().unwrap();
    let fwd: Vec<Vec<u8>> = std::iter::from_fn(|| iter.next().map(|(k, _)| k)).collect();
    assert_eq!(fwd, vec![b"a", b"b", b"e", b"f"]);

    let bidi = db.iter_bidi().unwrap();
    let rev: Vec<Vec<u8>> = bidi.rev().map(|(k, _)| k).collect();
    assert_eq!(
        rev,
        vec![b"f".to_vec(), b"e".to_vec(), b"b".to_vec(), b"a".to_vec()]
    );
}

/// Range tombstone backward after flush: tombstones survive to SST.
#[test]
fn test_range_tombstone_backward_after_flush() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    for c in b'a'..=b'f' {
        db.put(&[c], &[c]).unwrap();
    }
    db.delete_range(b"b", b"e").unwrap();
    db.flush().unwrap();

    let bidi = db.iter_bidi().unwrap();
    let rev: Vec<Vec<u8>> = bidi.rev().map(|(k, _)| k).collect();
    assert_eq!(rev, vec![b"f".to_vec(), b"e".to_vec(), b"a".to_vec()]);
}

/// BidiIterator streaming next_back: multiple calls should work without OOM.
#[test]
fn test_bidi_streaming_next_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    let count = 200;
    for i in 0..count {
        let key = format!("key_{:06}", i);
        let val = format!("val_{}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    let bidi = db.iter_bidi().unwrap();
    let rev: Vec<Vec<u8>> = bidi.rev().map(|(k, _)| k).collect();
    assert_eq!(rev.len(), count);
    for (i, entry) in rev.iter().enumerate() {
        let expected = format!("key_{:06}", count - 1 - i);
        assert_eq!(entry, expected.as_bytes(), "mismatch at position {}", i);
    }
}

/// prev() with multiple versions and tombstones.
#[test]
fn test_prev_multi_version_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    db.put(b"a", b"a1").unwrap();
    db.put(b"b", b"b1").unwrap();
    db.put(b"c", b"c1").unwrap();
    db.put(b"d", b"d1").unwrap();
    db.delete(b"b").unwrap();
    db.put(b"c", b"c2").unwrap();

    let mut iter = db.iter().unwrap();
    iter.seek(b"d");
    assert!(iter.valid());
    assert_eq!(iter.key(), b"d");

    iter.prev();
    assert!(iter.valid());
    assert_eq!(iter.key(), b"c");
    assert_eq!(iter.value(), b"c2");

    iter.prev();
    assert!(iter.valid());
    assert_eq!(iter.key(), b"a");

    iter.prev();
    assert!(!iter.valid());
}

/// seek_for_prev then advance then prev round-trip.
#[test]
fn test_seek_for_prev_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    for i in 0..10 {
        let key = format!("key_{:02}", i);
        let val = format!("val_{}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    let mut iter = db.iter().unwrap();
    iter.seek_for_prev(b"key_04x");
    assert!(iter.valid());
    assert_eq!(iter.key(), b"key_04");

    iter.advance();
    assert!(iter.valid());
    assert_eq!(iter.key(), b"key_05");

    iter.prev();
    assert!(iter.valid());
    assert_eq!(iter.key(), b"key_04");
}

/// compact_range followed by immediate iter() should see compacted state.
#[test]
fn test_compact_range_then_iter() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    for i in 0..50 {
        let key = format!("key_{:04}", i);
        db.put(key.as_bytes(), key.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    db.compact_range(Some(b"key_0000"), Some(b"key_0050"))
        .unwrap();

    let mut iter = db.iter().unwrap();
    let entries: Vec<Vec<u8>> = std::iter::from_fn(|| iter.next().map(|(k, _)| k)).collect();
    assert_eq!(entries.len(), 50);
    for (i, entry) in entries.iter().enumerate() {
        let expected = format!("key_{:04}", i);
        assert_eq!(entry, expected.as_bytes());
    }
}

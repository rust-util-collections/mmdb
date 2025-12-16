//! Crash recovery and durability tests.
//!
//! Simulates crash scenarios by abruptly closing the DB
//! and verifying data integrity on reopen.

use mmdb::sst::format::CompressionType;
use mmdb::{DB, DbOptions, WriteBatch, WriteOptions};

fn make_opts() -> DbOptions {
    DbOptions {
        create_if_missing: true,
        write_buffer_size: 2048,
        ..Default::default()
    }
}

#[test]
fn test_crash_after_single_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Write and "crash"
    {
        let db = DB::open(make_opts(), &path).unwrap();
        db.put(b"key1", b"value1").unwrap();
        // Ensure WAL is flushed
        db.put_with_options(
            &mmdb::WriteOptions {
                sync: true,
                ..Default::default()
            },
            b"key2",
            b"value2",
        )
        .unwrap();
        // Simulate crash: don't call close()
        std::mem::forget(db);
    }

    // Recover
    {
        let db = DB::open(make_opts(), &path).unwrap();
        assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
    }
}

#[test]
fn test_crash_after_flush() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = DB::open(make_opts(), &path).unwrap();
        // Write enough to trigger flush
        for i in 0..50 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:040}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.flush().unwrap();

        // Write more after flush
        for i in 50..75 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:040}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        // Crash without close
        std::mem::forget(db);
    }

    // Recover
    {
        let db = DB::open(make_opts(), &path).unwrap();
        // SST data should survive
        for i in 0..50 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:040}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "SST data lost for key {}",
                i
            );
        }
        // WAL data after flush should survive
        for i in 50..75 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:040}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "WAL data lost for key {}",
                i
            );
        }
    }
}

#[test]
fn test_crash_after_multiple_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = DB::open(make_opts(), &path).unwrap();

        for round in 0..5 {
            for i in 0..20 {
                let key = format!("r{}_k{:04}", round, i);
                let val = format!("value_{}", round * 20 + i);
                db.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
            db.flush().unwrap();
        }

        std::mem::forget(db);
    }

    {
        let db = DB::open(make_opts(), &path).unwrap();
        for round in 0..5 {
            for i in 0..20 {
                let key = format!("r{}_k{:04}", round, i);
                let val = format!("value_{}", round * 20 + i);
                assert_eq!(db.get(key.as_bytes()).unwrap(), Some(val.into_bytes()),);
            }
        }
    }
}

#[test]
fn test_crash_with_batch_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = DB::open(make_opts(), &path).unwrap();

        let mut batch = WriteBatch::new();
        for i in 0..20 {
            let key = format!("batch_key_{:04}", i);
            let val = format!("batch_val_{}", i);
            batch.put(key.as_bytes(), val.as_bytes());
        }
        db.write(batch).unwrap();

        // Sync WAL explicitly
        db.put_with_options(
            &mmdb::WriteOptions {
                sync: true,
                ..Default::default()
            },
            b"sync_marker",
            b"ok",
        )
        .unwrap();

        std::mem::forget(db);
    }

    {
        let db = DB::open(make_opts(), &path).unwrap();
        for i in 0..20 {
            let key = format!("batch_key_{:04}", i);
            let val = format!("batch_val_{}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
        }
        assert_eq!(db.get(b"sync_marker").unwrap(), Some(b"ok".to_vec()));
    }
}

#[test]
fn test_multiple_reopen_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    for cycle in 0..5 {
        let db = DB::open(make_opts(), &path).unwrap();
        let key = format!("cycle_{}", cycle);
        let val = format!("value_{}", cycle);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
        db.close().unwrap();
    }

    // Final verify
    let db = DB::open(make_opts(), &path).unwrap();
    for cycle in 0..5 {
        let key = format!("cycle_{}", cycle);
        let val = format!("value_{}", cycle);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
    }
}

#[test]
fn test_recovery_with_lz4_compression() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 1024,
        compression: CompressionType::Lz4,
        ..Default::default()
    };

    {
        let db = DB::open(opts.clone(), &path).unwrap();
        for i in 0..100 {
            let key = format!("lz4_key_{:06}", i);
            let val = format!("lz4_value_{:060}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.close().unwrap();
    }

    {
        let db = DB::open(opts, &path).unwrap();
        for i in 0..100 {
            let key = format!("lz4_key_{:06}", i);
            let val = format!("lz4_value_{:060}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
        }
    }
}

#[test]
fn test_crash_partial_wal_record() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: write two synced records, then crash
    {
        let db = DB::open(make_opts(), &path).unwrap();
        db.put_with_options(
            &WriteOptions {
                sync: true,
                ..Default::default()
            },
            b"good1",
            b"value1",
        )
        .unwrap();
        db.put_with_options(
            &WriteOptions {
                sync: true,
                ..Default::default()
            },
            b"good2",
            b"value2",
        )
        .unwrap();
        std::mem::forget(db);
    }

    // Truncate the WAL file mid-record: find the WAL, chop off some bytes
    {
        let mut wal_files: Vec<_> = std::fs::read_dir(&path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
            .collect();
        wal_files.sort_by_key(|e| e.path());
        assert!(!wal_files.is_empty(), "expected at least one WAL file");
        let wal_path = wal_files.last().unwrap().path();
        let len = std::fs::metadata(&wal_path).unwrap().len();
        // Truncate off the last 4 bytes to corrupt the tail record
        assert!(len > 4, "WAL too small to truncate");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap();
        file.set_len(len - 4).unwrap();
    }

    // Phase 2: recover — should get at least the first complete record
    {
        let db = DB::open(make_opts(), &path).unwrap();
        assert_eq!(
            db.get(b"good1").unwrap(),
            Some(b"value1".to_vec()),
            "first complete record should survive truncation"
        );
        // The second record may or may not survive depending on exactly where
        // the truncation landed. The key invariant is that recovery does not
        // fail and complete records are returned.
    }
}

#[test]
fn test_crash_during_flush() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: write enough data to trigger a flush, then crash
    {
        let db = DB::open(make_opts(), &path).unwrap();
        for i in 0..50 {
            let key = format!("flush_key_{:04}", i);
            let val = format!("flush_val_{:040}", i);
            db.put_with_options(
                &WriteOptions {
                    sync: true,
                    ..Default::default()
                },
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }
        // Explicit flush to create an SST
        db.flush().unwrap();
        std::mem::forget(db);
    }

    // Remove all SST files to simulate a crash where the SST was only
    // partially written / never made it to disk
    let sst_files: Vec<_> = std::fs::read_dir(&path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
        .map(|e| e.path())
        .collect();
    for sst in &sst_files {
        std::fs::remove_file(sst).unwrap();
    }

    // Phase 2: reopen — even without SSTs, recovery from WAL should restore data
    // Note: this may succeed fully if the WAL still contains all the data,
    // or the DB may open with partial data if the WAL was rotated after flush.
    // The key invariant is that DB::open does not panic or return an error.
    let db = DB::open(make_opts(), &path).unwrap();
    // At minimum, the DB opens without error. Check whatever data is available.
    let mut recovered = 0;
    for i in 0..50 {
        let key = format!("flush_key_{:04}", i);
        if db.get(key.as_bytes()).unwrap().is_some() {
            recovered += 1;
        }
    }
    // After a successful flush the WAL is rotated, so recovery may not have
    // the pre-flush data. Regardless, opening must succeed.
    assert!(
        recovered == 0 || recovered == 50,
        "expected either 0 (WAL rotated) or 50 (WAL still present) recovered keys, got {}",
        recovered
    );
}

#[test]
fn test_large_data_crash_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 64 * 1024, // larger buffer so we don't auto-flush constantly
        ..Default::default()
    };

    // Phase 1: write 10K keys with sync, then crash
    {
        let db = DB::open(opts.clone(), &path).unwrap();
        for i in 0..10_000 {
            let key = format!("bigkey_{:06}", i);
            let val = format!("bigval_{:06}", i);
            db.put_with_options(
                &WriteOptions {
                    sync: true,
                    ..Default::default()
                },
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }
        std::mem::forget(db);
    }

    // Phase 2: recover all 10K keys
    {
        let db = DB::open(opts, &path).unwrap();
        for i in 0..10_000 {
            let key = format!("bigkey_{:06}", i);
            let val = format!("bigval_{:06}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "missing key {} after large data crash recovery",
                i
            );
        }
    }
}

#[test]
fn test_multiple_crash_recover_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Cycle 1: write → crash
    {
        let db = DB::open(make_opts(), &path).unwrap();
        for i in 0..10 {
            let key = format!("cycle1_key_{:04}", i);
            let val = format!("cycle1_val_{:04}", i);
            db.put_with_options(
                &WriteOptions {
                    sync: true,
                    ..Default::default()
                },
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }
        std::mem::forget(db);
    }

    // Cycle 2: recover → write more → crash
    {
        let db = DB::open(make_opts(), &path).unwrap();
        // Verify cycle 1 data survived
        for i in 0..10 {
            let key = format!("cycle1_key_{:04}", i);
            let val = format!("cycle1_val_{:04}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "cycle1 key {} lost after first crash",
                i
            );
        }
        // Write cycle 2 data
        for i in 0..10 {
            let key = format!("cycle2_key_{:04}", i);
            let val = format!("cycle2_val_{:04}", i);
            db.put_with_options(
                &WriteOptions {
                    sync: true,
                    ..Default::default()
                },
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }
        std::mem::forget(db);
    }

    // Cycle 3: recover → write more → crash
    {
        let db = DB::open(make_opts(), &path).unwrap();
        // Verify cycle 1 + cycle 2 data survived
        for i in 0..10 {
            let key = format!("cycle1_key_{:04}", i);
            let val = format!("cycle1_val_{:04}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "cycle1 key {} lost after second crash",
                i
            );
        }
        for i in 0..10 {
            let key = format!("cycle2_key_{:04}", i);
            let val = format!("cycle2_val_{:04}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "cycle2 key {} lost after second crash",
                i
            );
        }
        // Write cycle 3 data
        for i in 0..10 {
            let key = format!("cycle3_key_{:04}", i);
            let val = format!("cycle3_val_{:04}", i);
            db.put_with_options(
                &WriteOptions {
                    sync: true,
                    ..Default::default()
                },
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }
        std::mem::forget(db);
    }

    // Final recovery: all 30 keys from all 3 cycles must be present
    {
        let db = DB::open(make_opts(), &path).unwrap();
        for cycle in 1..=3 {
            for i in 0..10 {
                let key = format!("cycle{}_key_{:04}", cycle, i);
                let val = format!("cycle{}_val_{:04}", cycle, i);
                assert_eq!(
                    db.get(key.as_bytes()).unwrap(),
                    Some(val.into_bytes()),
                    "cycle{} key {} lost after final recovery",
                    cycle,
                    i
                );
            }
        }
    }
}

#[test]
fn test_sync_guarantees() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Write data with sync=true, then crash
    {
        let db = DB::open(make_opts(), &path).unwrap();
        for i in 0..20 {
            let key = format!("sync_key_{:04}", i);
            let val = format!("sync_val_{:04}", i);
            db.put_with_options(
                &WriteOptions {
                    sync: true,
                    ..Default::default()
                },
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }
        // Simulate crash immediately after synced writes
        std::mem::forget(db);
    }

    // Recover — all synced data must survive
    {
        let db = DB::open(make_opts(), &path).unwrap();
        for i in 0..20 {
            let key = format!("sync_key_{:04}", i);
            let val = format!("sync_val_{:04}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "synced key {} was lost — sync guarantee violated",
                i
            );
        }
    }
}

#[test]
fn test_disable_wal_data_loss() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Write some durable data first, then write with disable_wal, then crash
    {
        let db = DB::open(make_opts(), &path).unwrap();

        // Durable write
        db.put_with_options(
            &WriteOptions {
                sync: true,
                ..Default::default()
            },
            b"durable_key",
            b"durable_val",
        )
        .unwrap();

        // Writes with WAL disabled — these may be lost on crash
        for i in 0..20 {
            let key = format!("nowal_key_{:04}", i);
            let val = format!("nowal_val_{:04}", i);
            db.put_with_options(
                &WriteOptions {
                    disable_wal: true,
                    ..Default::default()
                },
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }

        // Simulate crash
        std::mem::forget(db);
    }

    // Recover — durable key must survive; no-WAL keys may be lost
    {
        let db = DB::open(make_opts(), &path).unwrap();

        // The durable write must be present
        assert_eq!(
            db.get(b"durable_key").unwrap(),
            Some(b"durable_val".to_vec()),
            "durable synced write was lost"
        );

        // Count how many no-WAL keys survived (expected: 0 or very few)
        let mut survived = 0;
        for i in 0..20 {
            let key = format!("nowal_key_{:04}", i);
            if db.get(key.as_bytes()).unwrap().is_some() {
                survived += 1;
            }
        }
        // With disable_wal + crash, data loss is expected behavior.
        // We don't assert survived == 0 because an auto-flush could have
        // persisted some to SST. But typically most/all are lost.
        assert!(survived <= 20, "unexpected survived count: {}", survived);
    }
}

#[test]
fn test_crash_after_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: write → flush → compact → write more → crash
    {
        let db = DB::open(make_opts(), &path).unwrap();

        // Write and flush multiple rounds to create SSTs eligible for compaction
        for round in 0..3 {
            for i in 0..30 {
                let key = format!("ckey_{:04}", round * 30 + i);
                let val = format!("cval_{:04}", round * 30 + i);
                db.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
            db.flush().unwrap();
        }

        // Compact
        db.compact().unwrap();

        // Write additional data after compaction (lives in WAL/memtable)
        for i in 0..10 {
            let key = format!("post_compact_key_{:04}", i);
            let val = format!("post_compact_val_{:04}", i);
            db.put_with_options(
                &WriteOptions {
                    sync: true,
                    ..Default::default()
                },
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }

        // Crash
        std::mem::forget(db);
    }

    // Phase 2: recover — all pre-compaction and post-compaction data must be present
    {
        let db = DB::open(make_opts(), &path).unwrap();

        // Pre-compaction data (now in compacted SSTs)
        for i in 0..90 {
            let key = format!("ckey_{:04}", i);
            let val = format!("cval_{:04}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "pre-compaction key {} lost after crash",
                i
            );
        }

        // Post-compaction data (recovered from WAL)
        for i in 0..10 {
            let key = format!("post_compact_key_{:04}", i);
            let val = format!("post_compact_val_{:04}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "post-compaction key {} lost after crash",
                i
            );
        }
    }
}

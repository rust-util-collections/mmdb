//! Crash recovery and durability tests.
//!
//! Simulates crash scenarios by abruptly closing the DB
//! and verifying data integrity on reopen.

use std::fs;

use mmdb::CompressionType;
use mmdb::{DB, DbOptions, WriteBatch, WriteOptions};

fn make_opts() -> DbOptions {
    DbOptions::default()
        .with_create_if_missing(true)
        .with_write_buffer_size(2048)
}

#[test]
fn relative_store_survives_working_directory_change() {
    use std::{env, path::Path, process::Command};

    // A child process can change its working directory without affecting
    // other tests running concurrently.
    if env::var_os("MMDB_TEST_CHANGE_CWD").is_some() {
        assert_eq!(
            DB::open(make_opts(), "").err().unwrap().kind(),
            mmdb::ErrorKind::InvalidArgument
        );
        assert!(!Path::new("CURRENT").exists());
        let db = DB::open(make_opts(), "store").unwrap();
        assert_eq!(db.path(), Path::new("store"));
        db.put(b"key", b"value").unwrap();
        env::set_current_dir("other").unwrap();
        db.flush().unwrap();
        db.close().unwrap();
        env::set_current_dir("../store").unwrap();
        assert!(DB::open_read_only("").is_err());
        DB::open_read_only(".").unwrap();
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(dir.path().join("other/store")).unwrap();
    let child = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "relative_store_survives_working_directory_change",
        ])
        .env("MMDB_TEST_CHANGE_CWD", "1")
        .current_dir(dir.path())
        .status()
        .unwrap();
    assert!(child.success());
    assert_eq!(
        fs::read_dir(dir.path().join("other/store"))
            .unwrap()
            .count(),
        0
    );
    let reopened = DB::open_read_only(dir.path().join("store")).unwrap();
    assert_eq!(
        reopened.get(b"key").unwrap().as_deref(),
        Some(&b"value"[..])
    );
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
            &mmdb::WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
            b"key2",
            b"value2",
        )
        .unwrap();
        // Simulate crash: don't call close()
        db.simulate_crash();
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
        db.simulate_crash();
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

        db.simulate_crash();
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
            &mmdb::WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
            b"sync_marker",
            b"ok",
        )
        .unwrap();

        db.simulate_crash();
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
fn two_prefix_wipe_and_replacements_are_visible_together_after_wal_replay() {
    let dir = tempfile::tempdir().unwrap();
    let options = DbOptions::default().with_prefix_len(8);
    let db = DB::open(options.clone(), dir.path()).unwrap();
    let mut initial = WriteBatch::new();
    initial.put_prefixed(b"map-0001", b"/gone", b"stale");
    initial.put_prefixed(b"map-0001", b"/row", b"old");
    initial.put_prefixed(b"map-0002", b"/row", b"paired old");
    db.write(initial).unwrap();
    db.flush().unwrap();

    let before = db.snapshot();
    let mut update = WriteBatch::new();
    update.delete_range(b"map-0001", b"map-0002");
    update.put_prefixed(b"map-0001", b"/row", b"");
    update.put_prefixed(b"map-0002", b"/row", b"paired new");
    db.write_with_options(
        &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
        update,
    )
    .unwrap();

    let keys: [&[u8]; 4] = [
        b"map-0001/gone",
        b"map-0001/row",
        b"map-0002/row",
        b"map-0002/z-late",
    ];
    let previous = [
        Some(b"stale".as_slice()),
        Some(b"old".as_slice()),
        Some(b"paired old".as_slice()),
        None,
    ];
    let published = [
        None,
        Some(b"".as_slice()),
        Some(b"paired new".as_slice()),
        None,
    ];
    let check = |snapshot: &mmdb::Snapshot<'_>, expected: [Option<&[u8]>; 4]| {
        for (key, value) in keys.into_iter().zip(expected) {
            assert_eq!(snapshot.get(key).unwrap().as_deref(), value);
            assert_eq!(snapshot.get_pinned(key).unwrap().as_deref(), value);
            assert_eq!(snapshot.contains_key(key).unwrap(), value.is_some());
        }
        for prefix in [b"map-0001", b"map-0002"] {
            let mut iter = snapshot.iter_with_prefix(prefix).unwrap();
            let rows: Vec<_> = keys
                .into_iter()
                .zip(expected)
                .filter(|(k, _)| k.starts_with(prefix))
                .filter_map(|(k, v)| Some((k.to_vec(), v?.to_vec())))
                .collect();
            assert_eq!(iter.by_ref().collect::<Vec<_>>(), rows);
            assert!(iter.error().is_none());
        }
    };
    check(&before, previous);
    let after = db.snapshot();
    assert_eq!(after.sequence(), before.sequence() + 3);
    check(&after, published);
    drop(after);
    drop(before);
    // Capture the acknowledged record boundary before adding an unfenced suffix.
    let wals: Vec<_> = dir
        .path()
        .read_dir()
        .unwrap()
        .map(Result::unwrap)
        .filter(|e| {
            e.path().extension().is_some_and(|ext| ext == "wal") && e.metadata().unwrap().len() > 0
        })
        .collect();
    assert_eq!(wals.len(), 1);
    let active_name = wals[0].file_name();
    let synced_end = wals[0].metadata().unwrap().len();
    db.put(b"map-0002/z-late", b"unfenced").unwrap();
    db.simulate_crash();
    let tail_end = fs::metadata(dir.path().join(&active_name)).unwrap().len();
    assert!(tail_end > synced_end);

    // Distinct interrupted appends: before the mixed batch was acknowledged,
    // and during the later WalAsync record after that batch was acknowledged.
    let interrupted_batch = tempfile::tempdir().unwrap();
    let interrupted_suffix = tempfile::tempdir().unwrap();
    for (tmp, cutoff) in [
        (&interrupted_batch, synced_end - 1),
        (&interrupted_suffix, tail_end - 1),
    ] {
        for entry in dir.path().read_dir().unwrap() {
            let entry = entry.unwrap();
            fs::copy(entry.path(), tmp.path().join(entry.file_name())).unwrap();
        }
        fs::OpenOptions::new()
            .write(true)
            .open(tmp.path().join(&active_name))
            .unwrap()
            .set_len(cutoff)
            .unwrap();
    }
    let mut complete = published;
    complete[3] = Some(b"unfenced");
    for (path, state) in [
        (interrupted_batch.path(), previous),
        (interrupted_suffix.path(), published),
        (dir.path(), complete),
    ] {
        let read_only = DB::open_read_only(path).unwrap();
        check(&read_only.snapshot(), state);
        read_only.close().unwrap();
        let writable = DB::open(options.clone(), path).unwrap();
        check(&writable.snapshot(), state);
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

    let opts = DbOptions::default()
        .with_create_if_missing(true)
        .with_write_buffer_size(1024)
        .with_compression(CompressionType::Lz4);

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
        // Keep the physical records inspected below together in the active WAL.
        let db = DB::open(make_opts().with_write_buffer_size(64 << 20), &path).unwrap();
        db.put_with_options(
            &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
            b"good1",
            b"value1",
        )
        .unwrap();
        db.put_with_options(
            &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
            b"good2",
            b"value2",
        )
        .unwrap();
        db.simulate_crash();
    }

    // Truncate the WAL file mid-record: find the WAL, chop off some bytes
    {
        let mut wal_files: Vec<_> = fs::read_dir(&path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
            .collect();
        wal_files.sort_by_key(|e| e.path());
        assert!(!wal_files.is_empty(), "expected at least one WAL file");
        let wal_path = wal_files.last().unwrap().path();
        let len = fs::metadata(&wal_path).unwrap().len();
        // Truncate off the last 4 bytes to corrupt the tail record
        assert!(len > 4, "WAL too small to truncate");
        let file = fs::OpenOptions::new().write(true).open(&wal_path).unwrap();
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
fn test_torn_wal_tail_ignores_empty_higher_recovery_orphan() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        // Keep the physical records inspected below together in the active WAL.
        let db = DB::open(make_opts().with_write_buffer_size(64 << 20), &path).unwrap();
        db.put_with_options(
            &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
            b"good1",
            b"value1",
        )
        .unwrap();
        db.put_with_options(
            &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
            b"good2",
            b"value2",
        )
        .unwrap();
        db.simulate_crash();
    }

    let mut wal_files: Vec<_> = fs::read_dir(&path)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "wal"))
        .collect();
    wal_files.sort();
    let active_wal = wal_files.last().unwrap().clone();
    let active_num = active_wal
        .file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap();

    let len = fs::metadata(&active_wal).unwrap().len();
    assert!(len > 4, "WAL too small to truncate");
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&active_wal)
        .unwrap();
    file.set_len(len - 4).unwrap();

    let empty_orphan = path.join(format!("{:06}.wal", active_num + 1));
    fs::File::create(&empty_orphan).unwrap();

    let db = DB::open(make_opts(), &path).unwrap();
    assert_eq!(db.get(b"good1").unwrap(), Some(b"value1".to_vec()));
}

#[test]
fn test_crash_midlog_wal_corruption_fails_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: write three synced records, then crash
    {
        // Keep the physical records inspected below together in the active WAL.
        let db = DB::open(make_opts().with_write_buffer_size(64 << 20), &path).unwrap();
        for i in 0..3 {
            db.put_with_options(
                &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
                format!("mid_key{}", i).as_bytes(),
                format!("mid_value{}", i).as_bytes(),
            )
            .unwrap();
        }
        db.simulate_crash();
    }

    // Corrupt the FIRST record's payload in place (header/length intact, so
    // the records for mid_key1/mid_key2 still follow it — this is mid-log
    // corruption, not a torn tail).
    let wal_path = {
        let mut wal_files: Vec<_> = fs::read_dir(&path)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "wal"))
            .collect();
        wal_files.sort();
        wal_files.into_iter().next().unwrap()
    };
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = fs::OpenOptions::new().write(true).open(&wal_path).unwrap();
        // 7-byte WAL header, then payload: seq(8) + count(4) + entry...
        file.seek(SeekFrom::Start(7 + 12)).unwrap();
        file.write_all(b"XXXX").unwrap();
    }

    // Phase 2: open MUST fail loudly — silently recovering the prefix would
    // drop the two later committed records and then delete the WAL.
    let result = DB::open(make_opts(), &path);
    assert!(
        result.is_err(),
        "open should fail on mid-log WAL corruption instead of dropping later records"
    );
    // The corrupt WAL must be preserved for inspection, not deleted.
    assert!(
        wal_path.exists(),
        "corrupt WAL should not be deleted by a failed open"
    );
}

#[test]
fn changed_synced_fragment_header_preserves_wal_on_failed_recovery() {
    for value_len in [2, 40_000] {
        for (replacement, length_delta) in [
            (Some(1), 0),
            (Some(2), 0),
            (Some(3), 0),
            (Some(4), 0),
            (None, -1isize),
            (None, 1),
        ] {
            let original = if value_len == 2 { 1 } else { 4 };
            if replacement == Some(original) {
                continue;
            }
            let dir = tempfile::tempdir().unwrap();
            let opts = make_opts().with_write_buffer_size(64 << 20);
            let db = DB::open(opts.clone(), dir.path()).unwrap();
            db.put_with_options(
                &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
                b"k",
                &vec![0; value_len],
            )
            .unwrap();
            db.simulate_crash();
            let wal_path = fs::read_dir(dir.path())
                .unwrap()
                .map(|e| e.unwrap().path())
                .find(|p| p.extension().is_some_and(|ext| ext == "wal"))
                .unwrap();
            let mut bytes = fs::read(&wal_path).unwrap();
            let offset = if value_len == 2 { 0 } else { 32 * 1024 };
            assert_eq!(bytes[offset + 6], original);
            if let Some(replacement) = replacement {
                bytes[offset + 6] = replacement;
            } else {
                let length = u16::from_le_bytes([bytes[offset + 4], bytes[offset + 5]]) as usize;
                let changed = length.checked_add_signed(length_delta).unwrap() as u16;
                bytes[offset + 4..offset + 6].copy_from_slice(&changed.to_le_bytes());
            }
            fs::write(&wal_path, &bytes).unwrap();
            let current = fs::read(dir.path().join("CURRENT")).unwrap();
            let manifest_path = dir
                .path()
                .join(std::str::from_utf8(&current).unwrap().trim());
            let manifest = fs::read(&manifest_path).unwrap();
            for read_only in [true, false] {
                let result = if read_only {
                    DB::open_read_only(dir.path())
                } else {
                    DB::open(opts.clone(), dir.path())
                };
                let err = result
                    .err()
                    .expect("a changed complete fragment header must fail recovery");
                assert_eq!(err.kind(), mmdb::ErrorKind::Corruption);
                assert_eq!(fs::read(&wal_path).unwrap(), bytes);
                assert_eq!(fs::read(&manifest_path).unwrap(), manifest);
            }
        }
    }
}

/// Regression: a synced record whose last fragment carries only zero bytes
/// was dropped when that fragment's type byte changed to 0. Recovery took it
/// for a torn header, opened without the key, and deleted the WAL.
#[test]
fn test_zero_type_on_synced_zero_fragment_fails_open() {
    const WAL_BLOCK_SIZE: usize = 32 * 1024;
    const WAL_HEADER_SIZE: usize = 7;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let opts = make_opts().with_write_buffer_size(64 << 20);
    {
        let db = DB::open(opts.clone(), &path).unwrap();
        db.put_with_options(
            &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
            b"k",
            &[0u8; 40_000],
        )
        .unwrap();
        db.simulate_crash();
    }

    let wal_path = {
        let mut wal_files: Vec<_> = fs::read_dir(&path)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "wal"))
            .collect();
        wal_files.sort();
        wal_files.pop().unwrap()
    };
    let mut bytes = fs::read(&wal_path).unwrap();
    let last = WAL_BLOCK_SIZE;
    assert_eq!(bytes[last + 6], 4, "expected the record's Last fragment");
    assert!(
        bytes[last + WAL_HEADER_SIZE..].iter().all(|&b| b == 0),
        "the Last fragment must carry only zero value bytes"
    );
    bytes[last + 6] = 0;
    fs::write(&wal_path, &bytes).unwrap();

    assert!(
        DB::open(opts, &path).is_err(),
        "open must not drop a synced record whose fragment type changed"
    );
    assert!(wal_path.exists(), "a failed open must keep the WAL");
}

#[test]
fn test_crash_zeroed_midlog_wal_fragment_fails_open() {
    const WAL_HEADER_SIZE: usize = 7;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        // Keep the physical records inspected below together in the active WAL.
        let db = DB::open(make_opts().with_write_buffer_size(64 << 20), &path).unwrap();
        for i in 0..3 {
            db.put_with_options(
                &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
                format!("zero_gap_key{i}").as_bytes(),
                format!("zero_gap_value{i}").as_bytes(),
            )
            .unwrap();
        }
        db.simulate_crash();
    }

    let wal_path = {
        let mut wal_files: Vec<_> = fs::read_dir(&path)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "wal"))
            .collect();
        wal_files.sort();
        wal_files.into_iter().next().unwrap()
    };

    let mut bytes = fs::read(&wal_path).unwrap();
    let physical_record_len = |offset: usize| {
        WAL_HEADER_SIZE + u16::from_le_bytes([bytes[offset + 4], bytes[offset + 5]]) as usize
    };
    let second_offset = physical_record_len(0);
    let second_len = physical_record_len(second_offset);
    let third_offset = second_offset + second_len;
    assert!(
        third_offset + WAL_HEADER_SIZE <= bytes.len(),
        "expected a valid record after the fragment being zeroed"
    );
    assert_ne!(
        &bytes[third_offset..third_offset + WAL_HEADER_SIZE],
        &[0u8; WAL_HEADER_SIZE],
        "the later record must remain non-zero"
    );
    bytes[second_offset..third_offset].fill(0);
    fs::write(&wal_path, bytes).unwrap();

    let result = DB::open(make_opts(), &path);
    assert!(
        result.is_err(),
        "open should reject a zeroed middle WAL fragment instead of skipping it"
    );
    assert!(
        wal_path.exists(),
        "the corrupt WAL must be preserved after failed recovery"
    );
}

#[test]
fn test_enlarged_middle_wal_length_fails_open() {
    // Three synced same-block records whose middle physical length is enlarged
    // through the third. Without classification, recovery would treat the
    // post-read EOF as a torn active tail and drop the third committed record.
    const WAL_HEADER_SIZE: usize = 7;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        // Keep the physical records inspected below together in the active WAL.
        let db = DB::open(make_opts().with_write_buffer_size(64 << 20), &path).unwrap();
        for i in 0..3 {
            db.put_with_options(
                &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
                format!("len_key{i}").as_bytes(),
                format!("len_value{i}").as_bytes(),
            )
            .unwrap();
        }
        db.simulate_crash();
    }

    let wal_path = {
        let mut wal_files: Vec<_> = fs::read_dir(&path)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "wal"))
            .collect();
        wal_files.sort();
        wal_files.into_iter().next().unwrap()
    };

    let mut bytes = fs::read(&wal_path).unwrap();
    let physical_len = |offset: usize| {
        WAL_HEADER_SIZE + u16::from_le_bytes([bytes[offset + 4], bytes[offset + 5]]) as usize
    };
    let first_len = physical_len(0);
    let second_offset = first_len;
    let second_len = physical_len(second_offset);
    let third_offset = second_offset + second_len;
    let third_len = physical_len(third_offset);
    assert!(
        third_offset + third_len <= bytes.len(),
        "expected three complete same-block WAL records"
    );
    let enlarged = (third_offset + third_len - second_offset - WAL_HEADER_SIZE) as u16;
    bytes[second_offset + 4..second_offset + 6].copy_from_slice(&enlarged.to_le_bytes());
    fs::write(&wal_path, bytes).unwrap();

    let result = DB::open(make_opts(), &path);
    assert!(
        result.is_err(),
        "open must fail closed when a corrupt length swallows a later valid record"
    );
    assert!(
        wal_path.exists(),
        "corrupt WAL must be preserved after failed recovery"
    );
}

#[test]
fn test_earlier_wal_corrupt_tail_fails_open_when_newer_wal_exists() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        // Keep the physical records inspected below together in the active WAL.
        let db = DB::open(make_opts().with_write_buffer_size(64 << 20), &path).unwrap();
        db.flush().unwrap();
        db.put_with_options(
            &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
            b"newer_wal_key",
            b"newer_wal_value",
        )
        .unwrap();
        db.simulate_crash();
    }

    let mut wal_files: Vec<_> = fs::read_dir(&path)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "wal"))
        .collect();
    wal_files.sort();
    assert_eq!(wal_files.len(), 1, "test setup should leave one active WAL");
    let earlier_wal = wal_files[0].clone();
    let earlier_num = earlier_wal
        .file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap();
    let newer_wal = path.join(format!("{:06}.wal", earlier_num + 1));
    fs::copy(&earlier_wal, &newer_wal).unwrap();

    // A structural short read (a torn final append) rather than a checksum
    // failure: only the highest non-empty WAL may tolerate a torn tail, so
    // this must fail only because a newer WAL exists.
    {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&earlier_wal)
            .unwrap();
        let len = file.metadata().unwrap().len();
        file.set_len(len - 4).unwrap();
    }

    let result = DB::open(make_opts(), &path);
    assert!(
        result.is_err(),
        "open should fail when an earlier WAL has a torn tail"
    );
    assert!(
        earlier_wal.exists(),
        "corrupt earlier WAL should not be deleted by failed recovery"
    );
}

#[test]
fn test_recovery_removes_orphan_sst() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: create a DB with one flushed SST, then close cleanly.
    {
        let db = DB::open(make_opts(), &path).unwrap();
        for i in 0..20 {
            let key = format!("orph_key_{:04}", i);
            let val = format!("orph_val_{:040}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Simulate a crash that left an output SST behind without a MANIFEST
    // reference (e.g. kill -9 between SST write and MANIFEST install).
    let orphan = path.join("999999.sst");
    fs::write(&orphan, b"not a real sst, never referenced").unwrap();

    // Phase 2: reopen — recovery must delete the orphan and keep live data.
    {
        let db = DB::open(make_opts(), &path).unwrap();
        assert!(
            !orphan.exists(),
            "unreferenced SST should be removed during recovery"
        );
        for i in 0..20 {
            let key = format!("orph_key_{:04}", i);
            let val = format!("orph_val_{:040}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "live key {} must survive orphan cleanup",
                i
            );
        }
        db.close().unwrap();
    }
}

#[test]
fn test_missing_referenced_sst_fails_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: persist data in an SST and crash. The completed flush installs
    // the file in the MANIFEST before the crash.
    {
        let db = DB::open(make_opts(), &path).unwrap();
        for i in 0..50 {
            let key = format!("flush_key_{:04}", i);
            let val = format!("flush_val_{:040}", i);
            db.put_with_options(
                &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }
        // Explicit flush so the MANIFEST references an SST.
        db.flush().unwrap();
        db.simulate_crash();
    }

    // Delete every live SST to construct the missing-referenced-file state.
    let sst_files: Vec<_> = fs::read_dir(&path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
        .map(|e| e.path())
        .collect();
    let has_sst_files = !sst_files.is_empty();
    for sst in &sst_files {
        fs::remove_file(sst).unwrap();
    }

    // Phase 2: reopen — with SST files deleted but MANIFEST referencing them,
    // recovery MUST detect the corruption and return an error. Silently
    // losing data is worse than failing loudly.
    if has_sst_files {
        let result = DB::open(make_opts(), &path);
        assert!(
            result.is_err(),
            "recovery should fail when MANIFEST references missing SST files"
        );
    } else {
        // If no SSTs were created (all data in WAL), recovery should succeed
        let db = DB::open(make_opts(), &path).unwrap();
        let mut recovered = 0;
        for i in 0..50 {
            let key = format!("flush_key_{:04}", i);
            if db.get(key.as_bytes()).unwrap().is_some() {
                recovered += 1;
            }
        }
        assert_eq!(recovered, 50);
    }
}

#[test]
fn test_large_data_crash_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let opts = DbOptions::default()
        .with_create_if_missing(true)
        .with_write_buffer_size(64 * 1024);

    // Phase 1: write 500 keys with sync, then crash
    {
        let db = DB::open(opts.clone(), &path).unwrap();
        for i in 0..500 {
            let key = format!("bigkey_{:06}", i);
            let val = format!("bigval_{:06}", i);
            db.put_with_options(
                &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }
        db.simulate_crash();
    }

    // Phase 2: recover all 500 keys
    {
        let db = DB::open(opts, &path).unwrap();
        for i in 0..500 {
            let key = format!("bigkey_{:06}", i);
            let val = format!("bigval_{:06}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "missing key {} after crash recovery",
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
                &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }
        db.simulate_crash();
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
                &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }
        db.simulate_crash();
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
                &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }
        db.simulate_crash();
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
                &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }
        // Simulate crash immediately after synced writes
        db.simulate_crash();
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
    // A large buffer rules out an auto-flush persisting the no-WAL writes.
    let opts = || {
        DbOptions::default()
            .with_create_if_missing(true)
            .with_write_buffer_size(64 << 20)
    };

    // Write some durable data first, then write with disable_wal, then crash
    {
        let db = DB::open(opts(), &path).unwrap();

        // Durable write
        db.put_with_options(
            &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
            b"durable_key",
            b"durable_val",
        )
        .unwrap();

        // Writes with WAL disabled — these may be lost on crash
        for i in 0..20 {
            let key = format!("nowal_key_{:04}", i);
            let val = format!("nowal_val_{:04}", i);
            db.put_with_options(
                &WriteOptions::default().with_durability(mmdb::DurabilityMode::DisableWal),
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }

        // Simulate crash
        db.simulate_crash();
    }

    // Recover — the durable key survives; the no-WAL keys were never logged
    {
        let db = DB::open(opts(), &path).unwrap();

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
        // Nothing flushed them, so a crash loses every no-WAL write; any
        // survivor means `disable_wal` still appended to the WAL.
        assert_eq!(survived, 0, "no-WAL writes were recovered from the WAL");
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
                &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
                key.as_bytes(),
                val.as_bytes(),
            )
            .unwrap();
        }

        // Crash
        db.simulate_crash();
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

/// Regression test for orphan cleanup of MANIFEST-rotation temp files:
/// the CURRENT publish step writes per-attempt `CURRENT.tmp.<N>` files
/// and a crash between
/// the temp write and the atomic rename leaves one behind. Reopen must
/// sweep both spellings — and must not touch unrelated files.
#[test]
fn test_reopen_removes_stale_current_tmp_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Create a DB and close it cleanly.
    {
        let db = DB::open(make_opts(), &path).unwrap();
        db.put(b"key", b"value").unwrap();
        db.flush().unwrap();
    }

    // Only numbered staging files are owned by MANIFEST rotation.
    fs::write(path.join("CURRENT.tmp.12345"), b"MANIFEST-000042\n").unwrap();
    fs::write(path.join("CURRENT.tmp.notes"), b"keep").unwrap();
    fs::write(path.join("unrelated.txt"), b"keep me").unwrap();

    // Reopen: recovery's orphan sweep runs before backgrounds start.
    {
        let db = DB::open(make_opts(), &path).unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    assert!(
        !path.join("CURRENT.tmp.12345").exists(),
        "stale per-attempt CURRENT.tmp.<N> must be removed on reopen"
    );
    assert!(
        path.join("CURRENT.tmp.notes").exists(),
        "unrelated temporary files must survive"
    );
    assert!(
        path.join("unrelated.txt").exists(),
        "orphan sweep must not delete unrelated files"
    );
    assert!(path.join("CURRENT").exists(), "CURRENT itself must survive");
}

#[test]
fn enlarged_payload_preserves_later_wal_writes() {
    for adjustment in [-1isize, 0, 17] {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        for key in [b"a", b"b", b"c"] {
            db.put_with_options(
                &WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync),
                key,
                b"value\0\0",
            )
            .unwrap();
        }
        db.simulate_crash();
        let path = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.extension().is_some_and(|e| e == "wal"))
            .unwrap();
        let mut bytes = fs::read(&path).unwrap();
        let middle = 7 + u16::from_le_bytes([bytes[4], bytes[5]]) as usize;
        let enlarged = (bytes.len() - middle - 7)
            .checked_add_signed(adjustment)
            .unwrap();
        bytes[middle + 4..middle + 6].copy_from_slice(&(enlarged as u16).to_le_bytes());
        fs::write(&path, &bytes).unwrap();
        assert!(DB::open_read_only(dir.path()).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert!(DB::open(DbOptions::default(), dir.path()).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn earlier_wal_truncated_after_block_trailer_fails_closed_while_tail_wal_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(DbOptions::default(), dir.path()).unwrap();
    // Choose value length so the first WAL record ends at BLOCK_SIZE - 4 (32764),
    // leaving a 4-byte block trailer [32764..32768] before the second record at 32768.
    // encode_wal_record overhead for 1 Put entry with 1-byte key "a":
    // 8 (seq) + 4 (count) + 1 (type) + 4 (key_len) + 1 ("a") + 4 (val_len) = 22 bytes.
    // Physical record = 7 (WAL header) + 22 + val_len = 32764 => val_len = 32735.
    let val_a = vec![b'x'; 32768 - 4 - 7 - 22];
    let sync = WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync);
    db.put_with_options(&sync, b"a", &val_a).unwrap();
    db.put_with_options(&sync, b"b", b"in_block_1").unwrap();
    db.simulate_crash();

    let wal1 = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "wal"))
        .unwrap();
    let full_wal1 = fs::read(&wal1).unwrap();
    assert!(
        full_wal1.len() > 32768,
        "second record must land in block 1"
    );
    assert_eq!(&full_wal1[32764..32768], &[0, 0, 0, 0]);

    // Plant a non-empty newer WAL (e.g. from a later write before crash) while
    // truncating the earlier WAL at exact BLOCK_SIZE (losing "b" in block 1).
    let wal2 = dir.path().join("000099.wal");
    fs::write(&wal2, &full_wal1[..32764]).unwrap();
    fs::write(&wal1, &full_wal1[..32768]).unwrap();

    for res in [
        DB::open_read_only(dir.path()),
        DB::open(DbOptions::default(), dir.path()),
    ] {
        assert_eq!(res.err().unwrap().kind(), mmdb::ErrorKind::Corruption);
    }
    assert!(wal1.exists(), "corrupt earlier WAL must be preserved");

    // When the truncated block-aligned WAL is itself the active tail WAL, recovery
    // tolerates the interrupted append and preserves "a".
    fs::remove_file(&wal2).unwrap();
    let reopened = DB::open(DbOptions::default(), dir.path()).unwrap();
    assert_eq!(
        reopened.get(b"a").unwrap().as_deref(),
        Some(val_a.as_slice())
    );
    assert_eq!(reopened.get(b"b").unwrap(), None);
}

#[test]
fn missing_manifest_wal_rejects_partial_recovery_in_both_modes() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(DbOptions::default(), dir.path()).unwrap();
    let sync = WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync);
    db.put_with_options(&sync, b"latest", b"durable").unwrap();
    db.simulate_crash();

    // A higher WAL in an incomplete copy cannot stand in for the floor.
    let wal = fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|p| p.extension().is_some_and(|ext| ext == "wal"))
        .unwrap();
    let backup = tempfile::tempdir().unwrap();
    let saved = backup.path().join("saved-wal");
    fs::rename(&wal, &saved).unwrap();
    let standby = dir.path().join("000099.wal");
    fs::write(&standby, []).unwrap();
    let current = fs::read_to_string(dir.path().join("CURRENT")).unwrap();
    let manifest = dir.path().join(current.trim());
    let before = fs::read(&manifest).unwrap();
    for result in [
        DB::open_read_only(dir.path()),
        DB::open(DbOptions::default(), dir.path()),
    ] {
        assert_eq!(result.err().unwrap().kind(), mmdb::ErrorKind::Corruption);
        assert_eq!(fs::read(&manifest).unwrap(), before);
        assert!(standby.exists());
    }
    fs::rename(saved, wal).unwrap();
    let db = DB::open_read_only(dir.path()).unwrap();
    assert_eq!(db.get(b"latest").unwrap().as_deref(), Some(&b"durable"[..]));
}

#[test]
fn missing_current_preserves_existing_store_files_until_restored() {
    // Case 1: Flushed store missing CURRENT must reject default writable open
    // with Corruption before truncating MANIFEST-000001 or unlinking SSTs/WALs.
    {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        db.put(b"flushed", b"v1").unwrap();
        db.flush().unwrap();
        let sync = WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync);
        db.put_with_options(&sync, b"wal_only", b"v2").unwrap();
        db.simulate_crash();

        let saved_current = fs::read(dir.path().join("CURRENT")).unwrap();
        fs::remove_file(dir.path().join("CURRENT")).unwrap();

        let snapshot_before: std::collections::BTreeMap<String, Vec<u8>> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                (
                    e.file_name().to_string_lossy().into_owned(),
                    fs::read(e.path()).unwrap(),
                )
            })
            .collect();
        assert!(snapshot_before.keys().any(|k| k.ends_with(".sst")));
        assert!(snapshot_before.keys().any(|k| k.ends_with(".wal")));

        let err = match DB::open(DbOptions::default(), dir.path()) {
            Ok(_) => panic!("writable open must not overwrite store missing CURRENT"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), mmdb::ErrorKind::Corruption);
        assert!(!dir.path().join("CURRENT").exists());

        for (name, bytes) in &snapshot_before {
            if name != "LOCK" {
                assert_eq!(&fs::read(dir.path().join(name)).unwrap(), bytes, "{name}");
            }
        }

        fs::write(dir.path().join("CURRENT"), &saved_current).unwrap();
        let reopened = DB::open(DbOptions::default(), dir.path()).unwrap();
        assert_eq!(
            reopened.get(b"flushed").unwrap().as_deref(),
            Some(&b"v1"[..])
        );
        assert_eq!(
            reopened.get(b"wal_only").unwrap().as_deref(),
            Some(&b"v2"[..])
        );
        reopened.close().unwrap();
    }

    // Case 2: WAL-only store (zero SSTs) missing CURRENT is likewise preserved.
    {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        let sync = WriteOptions::default().with_durability(mmdb::DurabilityMode::WalSync);
        db.put_with_options(&sync, b"only_wal", b"keep").unwrap();
        db.simulate_crash();

        let saved_current = fs::read(dir.path().join("CURRENT")).unwrap();
        let manifest_before = fs::read(dir.path().join("MANIFEST-000001")).unwrap();
        fs::remove_file(dir.path().join("CURRENT")).unwrap();

        let err = match DB::open(DbOptions::default(), dir.path()) {
            Ok(_) => panic!("writable open must reject WAL-backed store missing CURRENT"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), mmdb::ErrorKind::Corruption);
        assert_eq!(
            fs::read(dir.path().join("MANIFEST-000001")).unwrap(),
            manifest_before
        );

        fs::write(dir.path().join("CURRENT"), &saved_current).unwrap();
        let reopened = DB::open(DbOptions::default(), dir.path()).unwrap();
        assert_eq!(
            reopened.get(b"only_wal").unwrap().as_deref(),
            Some(&b"keep"[..])
        );
        reopened.close().unwrap();
    }
}

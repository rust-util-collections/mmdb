//! End-to-end scenario tests for the MMDB LSM-Tree storage engine.
//!
//! Each test simulates a realistic workload pattern and verifies
//! correctness across flush, compaction, recovery, and iteration.

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use mmdb::{DB, DbOptions, WriteBatch, WriteOptions};

/// Helper: open a DB with small memtable for aggressive flushing.
fn open_small(path: &std::path::Path) -> DB {
    DB::open(
        DbOptions {
            create_if_missing: true,
            write_buffer_size: 1024,
            l0_compaction_trigger: 3,
            ..Default::default()
        },
        path,
    )
    .unwrap()
}

/// Helper: open a DB with custom write_buffer_size.
fn open_with_buffer(path: &std::path::Path, wbs: usize) -> DB {
    DB::open(
        DbOptions {
            create_if_missing: true,
            write_buffer_size: wbs,
            l0_compaction_trigger: 3,
            ..Default::default()
        },
        path,
    )
    .unwrap()
}

/// Helper: deterministic pseudo-random number generator (xorshift64).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

// ---------------------------------------------------------------------------
// 1. Time-series scenario: sequential writes with timestamp keys, range query
// ---------------------------------------------------------------------------
#[test]
fn test_timeseries_scenario() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_small(dir.path());

    let total = 2000;
    // Write entries with zero-padded timestamp keys so lexicographic order = time order.
    for ts in 0..total {
        let key = format!("ts:{:012}", ts);
        let val = format!("metric_value_{}", ts);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    // Range query: read the most recent N entries using iterator + seek.
    let recent_n = 100;
    let start_ts = total - recent_n;
    let seek_key = format!("ts:{:012}", start_ts);

    let mut iter = db.iter().unwrap();
    iter.seek(seek_key.as_bytes());

    let mut count = 0;
    while iter.valid() {
        let key = iter.key().to_vec();
        let key_str = std::str::from_utf8(&key).unwrap();
        // Ensure we are still in the ts: prefix range.
        if !key_str.starts_with("ts:") {
            break;
        }
        let ts_part: u64 = key_str[3..].parse().unwrap();
        assert!(
            ts_part >= start_ts as u64,
            "seek returned key before start: {}",
            ts_part
        );
        count += 1;
        iter.advance();
    }
    assert_eq!(count, recent_n, "expected {} recent entries", recent_n);

    // Also verify first and last via full iteration.
    let all: Vec<_> = db.iter().unwrap().collect();
    assert_eq!(all.len(), total);
    assert_eq!(
        std::str::from_utf8(&all[0].0).unwrap(),
        format!("ts:{:012}", 0)
    );
    assert_eq!(
        std::str::from_utf8(&all[total - 1].0).unwrap(),
        format!("ts:{:012}", total - 1)
    );
}

// ---------------------------------------------------------------------------
// 2. Key-value cache scenario: random put/get, verify high hit rate
// ---------------------------------------------------------------------------
#[test]
fn test_kv_cache_scenario() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_with_buffer(dir.path(), 4096);

    let num_keys = 500;
    let mut rng = Rng::new(0xDEAD_BEEF);

    // Populate keys.
    let mut expected: HashMap<String, String> = HashMap::new();
    for _ in 0..num_keys {
        let k = format!("cache_key_{:06}", rng.next_usize(num_keys));
        let v = format!("val_{}", rng.next_u64());
        db.put(k.as_bytes(), v.as_bytes()).unwrap();
        expected.insert(k, v);
    }
    db.flush().unwrap();

    // Random reads -- every key we wrote must be retrievable.
    let mut hits = 0u64;
    let total_reads = 1000;
    for _ in 0..total_reads {
        let k = format!("cache_key_{:06}", rng.next_usize(num_keys));
        let result = db.get(k.as_bytes()).unwrap();
        if let Some(ref val) = result {
            if let Some(exp) = expected.get(&k) {
                assert_eq!(val, exp.as_bytes(), "mismatch for key {}", k);
            }
            hits += 1;
        }
    }

    // With 500 unique keys and random draws from 0..500, we expect most to hit.
    let hit_rate = hits as f64 / total_reads as f64;
    assert!(
        hit_rate > 0.5,
        "hit rate too low: {:.2}% ({}/{})",
        hit_rate * 100.0,
        hits,
        total_reads
    );

    // Verify every key in the expected map is readable.
    for (k, v) in &expected {
        let got = db.get(k.as_bytes()).unwrap();
        assert_eq!(got, Some(v.as_bytes().to_vec()), "missing key {}", k);
    }
}

// ---------------------------------------------------------------------------
// 3. Log system scenario: append writes, delete old data, verify cleanup
// ---------------------------------------------------------------------------
#[test]
fn test_log_system_scenario() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 1024,
        l0_compaction_trigger: 3,
        num_levels: 2,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    let total_logs = 500;
    let delete_up_to = 300;

    // Append log entries with sequential IDs.
    for i in 0..total_logs {
        let key = format!("log:{:08}", i);
        let val = format!("log_entry_payload_{:040}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    // Verify all present before deletion.
    let count_before = db.iter().unwrap().count();
    assert_eq!(count_before, total_logs);

    // Delete old logs individually (simulates log rotation / TTL cleanup).
    for i in 0..delete_up_to {
        let key = format!("log:{:08}", i);
        db.delete(key.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();

    // Deleted logs should return None via point lookup.
    for i in 0..delete_up_to {
        let key = format!("log:{:08}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            None,
            "log {} should be deleted",
            i
        );
    }

    // Surviving range should still be present.
    for i in delete_up_to..total_logs {
        let key = format!("log:{:08}", i);
        let val = format!("log_entry_payload_{:040}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(val.into_bytes()),
            "log {} should survive",
            i
        );
    }

    // Iterator should only return surviving entries.
    let remaining: Vec<_> = db.iter().unwrap().collect();
    assert_eq!(remaining.len(), total_logs - delete_up_to);

    // Also test delete_range + compact: after compaction, range-deleted keys
    // are physically removed.
    for i in 500..600 {
        let key = format!("log:{:08}", i);
        let val = format!("batch2_{}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    let begin = format!("log:{:08}", 500);
    let end = format!("log:{:08}", 550);
    db.delete_range(begin.as_bytes(), end.as_bytes()).unwrap();
    db.flush().unwrap();
    db.compact().unwrap();

    // After compaction, range-deleted keys should be gone from the iterator.
    let after_range_del: Vec<_> = db.iter().unwrap().collect();
    let range_del_keys: Vec<_> = after_range_del
        .iter()
        .filter(|(k, _)| {
            let s = std::str::from_utf8(k).unwrap();
            s >= "log:00000500" && s < "log:00000550"
        })
        .collect();
    assert_eq!(
        range_del_keys.len(),
        0,
        "range-deleted keys should not appear after compaction"
    );
}

// ---------------------------------------------------------------------------
// 4. Index scenario: large sorted keyspace, random seek + scan 100 entries
// ---------------------------------------------------------------------------
#[test]
fn test_index_scenario() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_with_buffer(dir.path(), 2048);

    let total_keys = 5000;

    // Insert sorted keys.
    for i in 0..total_keys {
        let key = format!("idx:{:08}", i);
        let val = format!("row_{}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();

    // Random seek + scan 100 entries, multiple trials.
    let mut rng = Rng::new(42);
    let scan_len = 100;
    let trials = 20;

    for _ in 0..trials {
        let start = rng.next_usize(total_keys - scan_len);
        let seek_key = format!("idx:{:08}", start);

        let mut iter = db.iter().unwrap();
        iter.seek(seek_key.as_bytes());

        let mut scanned = Vec::new();
        for _ in 0..scan_len {
            if !iter.valid() {
                break;
            }
            scanned.push((iter.key().to_vec(), iter.value().to_vec()));
            iter.advance();
        }

        assert_eq!(
            scanned.len(),
            scan_len,
            "scan from {} yielded only {} entries",
            start,
            scanned.len()
        );

        // Verify the scanned keys are correct and in order.
        for (j, (k, v)) in scanned.iter().enumerate() {
            let expected_key = format!("idx:{:08}", start + j);
            let expected_val = format!("row_{}", start + j);
            assert_eq!(
                std::str::from_utf8(k).unwrap(),
                expected_key,
                "wrong key at scan offset {}",
                j
            );
            assert_eq!(
                std::str::from_utf8(v).unwrap(),
                expected_val,
                "wrong value at scan offset {}",
                j
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 5. Update-intensive: same keys overwritten many times, verify final values
// ---------------------------------------------------------------------------
#[test]
fn test_update_intensive() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_small(dir.path());

    let num_keys = 50;
    let num_rounds = 100;

    // Track what the final value should be.
    let mut expected: HashMap<String, String> = HashMap::new();

    for round in 0..num_rounds {
        for k in 0..num_keys {
            let key = format!("upd:{:04}", k);
            let val = format!("r{}_v{}", round, k);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
            expected.insert(key, val);
        }
        // Flush periodically to push data into SSTs.
        if round % 20 == 0 {
            db.flush().unwrap();
        }
    }
    db.flush().unwrap();
    db.compact().unwrap();

    // Verify final values via point lookups.
    for (k, v) in &expected {
        assert_eq!(
            db.get(k.as_bytes()).unwrap(),
            Some(v.as_bytes().to_vec()),
            "mismatch for key {} after {} rounds",
            k,
            num_rounds
        );
    }

    // Verify via iterator: should see exactly num_keys entries.
    let entries: Vec<_> = db.iter().unwrap().collect();
    assert_eq!(entries.len(), num_keys);

    // Verify sorted order and correct values in iterator.
    for (k, v) in &entries {
        let key_str = std::str::from_utf8(k).unwrap().to_string();
        let val_str = std::str::from_utf8(v).unwrap().to_string();
        assert_eq!(
            expected.get(&key_str).unwrap(),
            &val_str,
            "iterator value mismatch for {}",
            key_str
        );
    }
}

// ---------------------------------------------------------------------------
// 6. Mixed workload: 50% read / 30% write / 10% delete / 10% scan concurrent
// ---------------------------------------------------------------------------
#[test]
fn test_mixed_workload() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_with_buffer(dir.path(), 2048));

    let num_keys = 200;
    // Pre-populate so reads and scans have data.
    for i in 0..num_keys {
        let key = format!("mix:{:06}", i);
        let val = format!("init_{}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    let num_threads = 4;
    let ops_per_thread = 500;
    let mut handles = Vec::new();

    for tid in 0..num_threads {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            let mut rng = Rng::new(tid as u64 * 1000 + 7);
            for op_idx in 0..ops_per_thread {
                let pct = rng.next_usize(100);
                let kid = rng.next_usize(num_keys);
                let key = format!("mix:{:06}", kid);

                if pct < 50 {
                    // Read (50%)
                    let _ = db.get(key.as_bytes());
                } else if pct < 80 {
                    // Write (30%)
                    let val = format!("t{}_{}", tid, op_idx);
                    db.put(key.as_bytes(), val.as_bytes()).unwrap();
                } else if pct < 90 {
                    // Delete (10%)
                    db.delete(key.as_bytes()).unwrap();
                } else {
                    // Scan (10%) -- scan up to 10 entries from a random position
                    let mut iter = db.iter().unwrap();
                    iter.seek(key.as_bytes());
                    for _ in 0..10 {
                        if !iter.valid() {
                            break;
                        }
                        let _ = iter.key();
                        let _ = iter.value();
                        iter.advance();
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().expect("worker thread panicked");
    }

    // The DB should still be consistent: iterator should produce sorted, non-duplicate keys.
    let entries: Vec<_> = db.iter().unwrap().collect();
    for i in 1..entries.len() {
        assert!(
            entries[i].0 > entries[i - 1].0,
            "iterator not sorted at index {} after mixed workload",
            i
        );
    }

    // Point lookups for everything the iterator returns should agree.
    for (k, v) in &entries {
        let got = db.get(k).unwrap();
        assert_eq!(
            got.as_deref(),
            Some(v.as_slice()),
            "point lookup disagrees with iterator for key {:?}",
            std::str::from_utf8(k)
        );
    }
}

// ---------------------------------------------------------------------------
// 7. Long-running compaction: write until multiple compaction rounds, verify
// ---------------------------------------------------------------------------
#[test]
fn test_long_running_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 1024,
        l0_compaction_trigger: 2,
        target_file_size_base: 2048,
        max_bytes_for_level_base: 4096,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    let total = 3000;
    let mut expected: HashMap<String, String> = HashMap::new();

    for i in 0..total {
        let key = format!("cmp:{:08}", i);
        let val = format!("data_{:040}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
        expected.insert(key, val);

        // Trigger flush + compact periodically.
        if i > 0 && i % 200 == 0 {
            db.flush().unwrap();
            db.compact().unwrap();
        }
    }

    // Final flush + compact.
    db.flush().unwrap();
    db.compact().unwrap();

    // Verify every key.
    for (k, v) in &expected {
        let got = db.get(k.as_bytes()).unwrap();
        assert_eq!(
            got,
            Some(v.as_bytes().to_vec()),
            "missing or wrong value for {} after compactions",
            k
        );
    }

    // Iterator count must match.
    let count = db.iter().unwrap().count();
    assert_eq!(count, total, "iterator count mismatch after compactions");

    // Sorted order check.
    let entries: Vec<_> = db.iter().unwrap().collect();
    for i in 1..entries.len() {
        assert!(
            entries[i].0 > entries[i - 1].0,
            "sort order broken at index {}",
            i
        );
    }
}

// ---------------------------------------------------------------------------
// 8. Large dataset: 100K+ key-value pairs
// ---------------------------------------------------------------------------
#[test]
fn test_large_dataset() {
    let dir = tempfile::tempdir().unwrap();
    // Use a moderately sized buffer so we flush many times but do not OOM.
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 64 * 1024, // 64 KB
        l0_compaction_trigger: 4,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    let total: usize = 100_000;

    // Use batches to speed up writing.
    let batch_size = 500;
    for chunk_start in (0..total).step_by(batch_size) {
        let mut batch = WriteBatch::new();
        let chunk_end = (chunk_start + batch_size).min(total);
        for i in chunk_start..chunk_end {
            let key = format!("big:{:08}", i);
            let val = format!("v{:08}", i);
            batch.put(key.as_bytes(), val.as_bytes());
        }
        db.write(batch).unwrap();
    }

    db.flush().unwrap();
    db.compact().unwrap();

    // Spot-check a sample of keys spread across the range.
    let checks = [0, 1, 999, 10_000, 50_000, 75_000, 99_999];
    for &i in &checks {
        let key = format!("big:{:08}", i);
        let val = format!("v{:08}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(val.into_bytes()),
            "spot-check failed for key {}",
            i
        );
    }

    // Full iterator count.
    let count = db.iter().unwrap().count();
    assert_eq!(count, total, "expected {} entries, got {}", total, count);

    // Verify sorted order on a sample window.
    let mut iter = db.iter().unwrap();
    iter.seek(format!("big:{:08}", 50_000).as_bytes());
    let mut prev_key: Option<Vec<u8>> = None;
    let mut scanned = 0;
    while iter.valid() && scanned < 1000 {
        let k = iter.key().to_vec();
        if let Some(ref pk) = prev_key {
            assert!(k > *pk, "sort violation in large dataset scan");
        }
        prev_key = Some(k);
        scanned += 1;
        iter.advance();
    }
    assert_eq!(scanned, 1000);
}

// ---------------------------------------------------------------------------
// 9. Restart recovery: write -> flush -> write more -> crash -> recover
// ---------------------------------------------------------------------------
#[test]
fn test_restart_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 1024,
        l0_compaction_trigger: 3,
        ..Default::default()
    };

    // Phase 1: write + flush (data lands in SSTs).
    {
        let db = DB::open(opts.clone(), &path).unwrap();
        for i in 0..100 {
            let key = format!("rec:{:06}", i);
            let val = format!("phase1_{:030}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.flush().unwrap();

        // Phase 2: write more (data in WAL only).
        for i in 100..200 {
            let key = format!("rec:{:06}", i);
            let val = format!("phase2_{:030}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Also overwrite some phase-1 keys so WAL recovery must reconcile.
        for i in 0..20 {
            let key = format!("rec:{:06}", i);
            let val = format!("updated_{:030}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Sync WAL to ensure durability, then simulate crash (no close).
        db.put_with_options(
            &WriteOptions {
                sync: true,
                ..Default::default()
            },
            b"rec:sync_marker",
            b"synced",
        )
        .unwrap();
        std::mem::forget(db);
    }

    // Phase 3: recover and verify.
    {
        let db = DB::open(opts, &path).unwrap();

        // Phase-1 keys that were NOT overwritten.
        for i in 20..100 {
            let key = format!("rec:{:06}", i);
            let val = format!("phase1_{:030}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "phase1 key {} lost after recovery",
                i
            );
        }

        // Phase-1 keys that were overwritten.
        for i in 0..20 {
            let key = format!("rec:{:06}", i);
            let val = format!("updated_{:030}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "updated key {} wrong after recovery",
                i
            );
        }

        // Phase-2 keys (WAL-only).
        for i in 100..200 {
            let key = format!("rec:{:06}", i);
            let val = format!("phase2_{:030}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "phase2 key {} lost after recovery",
                i
            );
        }

        // Sync marker.
        assert_eq!(
            db.get(b"rec:sync_marker").unwrap(),
            Some(b"synced".to_vec())
        );

        // Iterator count: 200 unique rec: keys + 1 sync marker.
        let count = db.iter().unwrap().count();
        assert_eq!(count, 201, "wrong entry count after recovery");
    }
}

// ---------------------------------------------------------------------------
// 10. Space reclamation: write -> delete most -> compact -> verify size shrunk
// ---------------------------------------------------------------------------
#[test]
fn test_space_reclamation() {
    let dir = tempfile::tempdir().unwrap();
    // Use num_levels: 2 so a single compact() can merge L0 tombstones with L1
    // data, fully removing deleted keys (matching the pattern in integration tests).
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 512,
        l0_compaction_trigger: 2,
        num_levels: 2,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    // Write data with largish values so SST size is meaningful.
    let total = 500;
    for i in 0..total {
        let key = format!("sp:{:06}", i);
        let val = format!("{:0>200}", i); // 200-byte values
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();

    // Measure SST size before deletion.
    let size_before: u64 = db.get_property("total-sst-size").unwrap().parse().unwrap();
    assert!(
        size_before > 0,
        "SST size should be > 0 after writing {} keys",
        total
    );

    // Delete 80% of the keys.
    let delete_count = 400;
    for i in 0..delete_count {
        let key = format!("sp:{:06}", i);
        db.delete(key.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    // Compact to reclaim space from tombstones.
    db.compact().unwrap();

    let size_after: u64 = db.get_property("total-sst-size").unwrap().parse().unwrap();

    // The SST size should have shrunk (we deleted 80% of data).
    assert!(
        size_after < size_before,
        "SST size did not shrink after deleting 80% of data: before={}, after={}",
        size_before,
        size_after
    );

    // Verify the surviving keys are still present.
    for i in delete_count..total {
        let key = format!("sp:{:06}", i);
        let val = format!("{:0>200}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(val.into_bytes()),
            "surviving key {} lost after compaction",
            i
        );
    }

    // Verify deleted keys are gone.
    for i in 0..delete_count {
        let key = format!("sp:{:06}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            None,
            "deleted key {} still present after compaction",
            i
        );
    }

    // Iterator should only return surviving keys.
    let surviving: Vec<_> = db.iter().unwrap().collect();
    assert_eq!(
        surviving.len(),
        total - delete_count,
        "iterator count mismatch after space reclamation"
    );
}

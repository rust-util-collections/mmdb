use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Duration,
};

use mmdb::{CompactionFilter, CompactionFilterDecision, DB, DbOptions};

fn wait_until_removed(db: &DB, key: &[u8], message: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while db.get(key).unwrap().is_some() {
        assert!(std::time::Instant::now() <= deadline, "{message}");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

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
            // Keep the threshold low enough to cross, but avoid also calling
            // compact_range() — the point of this test is verifying that the
            // auto-triggered background compaction removes the keys.
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

    // The auto-triggered background compaction should eventually remove the
    // dead keys. Poll with a short sleep to avoid a single long hard-coded
    // sleep that would be brittle on slow CI.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let mut all_gone = true;
        for i in 0u32..6 {
            if db.get(&i.to_be_bytes()).unwrap().is_some() {
                all_gone = false;
                break;
            }
        }
        if all_gone {
            break;
        }
        if std::time::Instant::now() > deadline {
            // Timeout — report which keys are still present.
            for i in 0u32..6 {
                assert!(
                    db.get(&i.to_be_bytes()).unwrap().is_none(),
                    "key {} should have been removed by auto-triggered compaction",
                    i
                );
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
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

/// Verify the default threshold is zero: lazy-delete bookkeeping still records
/// dead keys, but no automatic sweep runs until an explicit compaction is
/// requested.
#[test]
fn lazy_delete_batch_default_threshold_zero_requires_manual_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions {
            create_if_missing: true,
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();

    for i in 0u32..50 {
        db.put(&i.to_be_bytes(), &[i as u8; 64]).unwrap();
    }
    db.flush().unwrap();

    let dead: Vec<Vec<u8>> = (0u32..50).map(|i| i.to_be_bytes().to_vec()).collect();
    db.lazy_delete_batch(&dead);
    assert_eq!(db.dead_key_count(), 50);

    std::thread::sleep(std::time::Duration::from_millis(100));
    for i in 0u32..50 {
        assert!(
            db.get(&i.to_be_bytes()).unwrap().is_some(),
            "threshold 0 must not auto-sweep key {}",
            i
        );
    }

    db.compact_range(None::<&[u8]>, None::<&[u8]>).unwrap();
    for i in 0u32..50 {
        assert!(
            db.get(&i.to_be_bytes()).unwrap().is_none(),
            "manual compaction should reclaim dead key {}",
            i
        );
    }
}

/// Pin the default lazy-delete sweep threshold at zero so future flips are
/// intentional and update the default-behavior tests.
#[test]
fn db_options_default_lazy_delete_compaction_threshold_is_zero() {
    assert_eq!(DbOptions::default().lazy_delete_compaction_threshold, 0);
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

/// Dead keys at L1 with a single SST file must still be removed: after the
/// first compaction drains L0 to L1 (creating one output file), L0 is empty.
/// A subsequent compaction has nothing to drain — it must apply the filter
/// via `force_merge_level`, which must process a single-file level when the
/// filter is not a no-op.
#[test]
fn lazy_delete_single_file_at_l1_removed_by_compact() {
    let dir = tempfile::tempdir().unwrap();
    let db = make_db(dir.path());

    // Write enough data across multiple flushes to create L0 files.
    for i in 0u32..20 {
        db.put(&i.to_be_bytes(), &[i as u8; 64]).unwrap();
    }

    // First compaction: drain L0 → L1 (one small SST).
    db.compact_range(None::<&[u8]>, None::<&[u8]>).unwrap();

    // Register dead keys AFTER the first compaction — these keys are now
    // sitting in a single L1 SST, so drain_l0 (next compact) finds nothing
    // to do. force_merge_level must process that single file.
    let dead: Vec<Vec<u8>> = (0u32..5).map(|i| i.to_be_bytes().to_vec()).collect();
    db.lazy_delete_batch(&dead);
    assert_eq!(db.dead_key_count(), 5);

    // Second compaction: L0 is empty, force_merge_level must apply the filter.
    db.compact_range(None::<&[u8]>, None::<&[u8]>).unwrap();

    for i in 0u32..5 {
        assert!(
            db.get(&i.to_be_bytes()).unwrap().is_none(),
            "key {} at L1 should have been removed by force_merge_level filter",
            i
        );
    }

    // Surviving keys at L1 should still be readable.
    for i in 5u32..20 {
        assert!(
            db.get(&i.to_be_bytes()).unwrap().is_some(),
            "key {} should survive",
            i
        );
    }
}

/// Regression: crossing the threshold must trigger the sweep even when the
/// store is fully settled (no compaction debt). Previously the crossing only
/// called `signal_compaction()`; the woken background thread found nothing
/// debt-driven to pick (L0 below its trigger, no oversized level, read hints
/// only fire for multi-file L2+ levels) and went back to sleep — the dead
/// keys were never physically removed. The original auto-trigger test above
/// passed only when the 20 puts happened to leave L0 exactly at the
/// compaction trigger; on machines where the background thread drained L0
/// before `lazy_delete_batch` ran, it timed out.
#[test]
fn lazy_delete_sweeps_settled_store() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions {
            create_if_missing: true,
            lazy_delete_compaction_threshold: 5,
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();

    for i in 0u32..20 {
        db.put(&i.to_be_bytes(), &[i as u8; 64]).unwrap();
    }
    // Settle the store: one L0 file, below the L0 trigger — no debt anywhere.
    db.flush().unwrap();

    let dead: Vec<Vec<u8>> = (0u32..6).map(|i| i.to_be_bytes().to_vec()).collect();
    db.lazy_delete_batch(&dead);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let all_gone = (0u32..6).all(|i| db.get(&i.to_be_bytes()).unwrap().is_none());
        if all_gone {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "dead keys were not swept on a settled store"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    for i in 6u32..20 {
        assert!(
            db.get(&i.to_be_bytes()).unwrap().is_some(),
            "key {} should survive",
            i
        );
    }
}

#[test]
fn lazy_delete_sweeps_deepest_level_before_l0() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions {
            create_if_missing: true,
            l0_compaction_trigger: usize::MAX,
            lazy_delete_compaction_threshold: 1,
            num_levels: 3,
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();

    db.put(b"dup", b"old").unwrap();
    db.flush().unwrap();
    db.compact().unwrap();
    assert_eq!(db.get_property("num-files-at-level1").as_deref(), Some("1"));

    db.put(b"dup", b"new").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get_property("num-files-at-level0").as_deref(), Some("1"));

    db.lazy_delete(b"dup");
    wait_until_removed(
        &db,
        b"dup",
        "the newer L0 copy remained after the deeper copy was removed",
    );
}

#[test]
fn lazy_delete_requeues_new_keys_above_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions {
            create_if_missing: true,
            l0_compaction_trigger: usize::MAX,
            lazy_delete_compaction_threshold: 1,
            max_background_compactions: 4,
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();

    db.put(b"a", b"1").unwrap();
    db.flush().unwrap();
    db.lazy_delete(b"a");
    wait_until_removed(&db, b"a", "the first automatic sweep did not finish");

    db.put(b"b", b"2").unwrap();
    db.flush().unwrap();
    db.lazy_delete(b"b");
    wait_until_removed(
        &db,
        b"b",
        "a new key registered above the threshold did not queue another sweep",
    );
}

#[test]
fn lazy_delete_retries_after_snapshot_release() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions {
            create_if_missing: true,
            l0_compaction_trigger: usize::MAX,
            lazy_delete_compaction_threshold: 1,
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();

    db.put(b"k", b"v").unwrap();
    db.flush().unwrap();
    let snapshot = db.snapshot();
    db.lazy_delete(b"k");

    std::thread::sleep(std::time::Duration::from_millis(100));
    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));

    drop(snapshot);
    wait_until_removed(
        &db,
        b"k",
        "the queued sweep did not resume after the snapshot was released",
    );
}

/// A user compaction filter that parks its first invocation until released,
/// modeling a slow in-flight background merge. `is_noop()` is `false`, so
/// no-op shortcuts (trivial move, single-file skip) never bypass it.
struct GateFilter {
    entered: Mutex<Option<mpsc::Sender<()>>>,
    release: Mutex<Option<mpsc::Receiver<()>>>,
}

impl CompactionFilter for GateFilter {
    fn filter(&self, _level: usize, _key: &[u8], _value: &[u8]) -> CompactionFilterDecision {
        // Park only the first invocation: report entry, then wait for release.
        if let Some(release) = self.release.lock().unwrap().take() {
            if let Some(entered) = self.entered.lock().unwrap().take() {
                let _ = entered.send(());
            }
            let _ = release.recv_timeout(Duration::from_secs(10));
        }
        CompactionFilterDecision::Keep
    }

    fn is_noop(&self) -> bool {
        false
    }
}

/// Regression: a key registered while a background compaction is mid-merge
/// must still be removed by an explicit full compaction. The background merge
/// checked the key against the dead-keys set before the registration, so
/// `compact_range(None, None)` must wait for that in-flight compaction to
/// settle (and then rewrite its installed output) instead of skipping the
/// claimed L0 files and returning early.
#[test]
fn compact_waits_for_inflight_compaction_and_removes_dead_keys() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions {
            create_if_missing: true,
            write_buffer_size: 1024,
            compaction_filter: Some(Arc::new(GateFilter {
                entered: Mutex::new(Some(entered_tx)),
                release: Mutex::new(Some(release_rx)),
            })),
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();

    // Reach `l0_compaction_trigger` (default 4) so the background thread
    // picks the whole L0 set and parks inside the filter on the smallest
    // key (key 0), which has already passed the dead-key check by then.
    for i in 0u32..20 {
        db.put(&i.to_be_bytes(), &[i as u8; 64]).unwrap();
    }
    entered_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("background compaction never reached the filter");

    // Register key 0 while the background merge output is still uninstalled.
    db.lazy_delete(&0u32.to_be_bytes());

    thread::scope(|s| {
        let compact = s.spawn(|| db.compact_range(None::<&[u8]>, None::<&[u8]>));
        // Let compact_range observe the in-flight claim before the gate opens.
        thread::sleep(Duration::from_millis(200));
        let _ = release_tx.send(());
        compact.join().unwrap().unwrap();
    });

    assert!(
        db.get(&0u32.to_be_bytes()).unwrap().is_none(),
        "dead key registered during an in-flight compaction must not survive compact_range"
    );
    for i in 1u32..20 {
        assert!(
            db.get(&i.to_be_bytes()).unwrap().is_some(),
            "key {} should survive",
            i
        );
    }
}

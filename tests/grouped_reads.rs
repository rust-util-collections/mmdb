use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, Write};

use mmdb::{DB, DbOptions, ErrorKind, ReadOptions, WriteBatch};

fn visible(db: &DB, opts: &ReadOptions<'_>, keys: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
    db.get_many_pinned_with_options(opts, keys)
        .unwrap()
        .into_iter()
        .map(|v| v.map(|v| v.into_vec()))
        .collect()
}

#[test]
fn grouped_reads_preserve_order_versions_and_cross_file_range_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions::default().with_l0_compaction_trigger(usize::MAX),
        dir.path(),
    )
    .unwrap();
    db.ingest_sorted([
        (b"b".as_slice(), b"old".as_slice()),
        (b"c", b"old"),
        (b"d", b""),
        (b"e", b"old"),
        (b"x", b"deep"),
    ])
    .unwrap();
    let before = db.snapshot();
    assert!(db.get_many_pinned(&[]).unwrap().is_empty());
    db.delete_range(b"a", b"d").unwrap();
    db.flush().unwrap();
    db.delete(b"e").unwrap();
    db.put(b"b", b"after range").unwrap();
    db.flush().unwrap();
    db.put(b"z", b"active").unwrap();

    let keys: &[&[u8]] = &[b"z", b"x", b"e", b"d", b"c", b"b", b"b", b"missing"];
    assert_eq!(
        visible(&db, &ReadOptions::default(), keys),
        [
            Some(b"active".as_slice()),
            Some(b"deep"),
            None,
            Some(b""),
            None,
            Some(b"after range"),
            Some(b"after range"),
            None
        ]
        .map(|v| v.map(<[u8]>::to_vec))
    );
    assert_eq!(
        visible(&db, &before.read_options(), keys),
        [
            None,
            Some(b"deep".as_slice()),
            Some(b"old"),
            Some(b""),
            Some(b"old"),
            Some(b"old"),
            Some(b"old"),
            None
        ]
        .map(|v| v.map(<[u8]>::to_vec))
    );
    let foreign_dir = tempfile::tempdir().unwrap();
    let foreign = DB::open(DbOptions::default(), foreign_dir.path()).unwrap();
    for inputs in [keys, &keys[..1], &keys[..0]] {
        assert_eq!(
            foreign
                .get_many_pinned_with_options(&before.read_options(), inputs)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidArgument
        );
    }
    assert_eq!(
        db.get_many_pinned_with_options(&before.read_options(), &[b"b".as_slice(); 33])
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    let retained = db.get_many_pinned(keys).unwrap();
    db.close().unwrap();
    assert_eq!(retained[0].as_deref(), Some(b"active".as_slice()));
    assert_eq!(retained[1].as_deref(), Some(b"deep".as_slice()));
    assert_eq!(
        db.get_many_pinned(&[b"b".as_slice(); 33])
            .unwrap_err()
            .kind(),
        ErrorKind::DbClosed
    );
    assert_eq!(
        db.get_many_pinned(&[]).unwrap_err().kind(),
        ErrorKind::DbClosed
    );
}

#[test]
fn grouped_reads_fetch_one_shared_data_block_and_index_leaf() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions::default()
        .with_block_cache_capacity(0)
        .with_block_size(16 << 10);
    let db = DB::open(opts, dir.path()).unwrap();
    db.ingest_sorted((0u32..256).map(|i| (i.to_be_bytes(), i.to_be_bytes())))
        .unwrap();
    let raw: Vec<_> = (0u32..32).rev().map(u32::to_be_bytes).collect();
    let keys: Vec<_> = raw.iter().map(|k| k.as_slice()).collect();
    let snap = db.snapshot();
    let misses = || {
        db.get_property("stats.block_cache_misses")
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    let old = misses();
    for key in &keys {
        assert_eq!(snap.get_pinned(key).unwrap().unwrap().as_ref(), *key);
    }
    let scalar = misses() - old;
    let grouped = visible(&db, &snap.read_options(), &keys);
    assert!(
        grouped
            .iter()
            .zip(keys)
            .all(|(a, b)| a.as_deref() == Some(b))
    );
    assert_eq!(scalar, 64);
    assert_eq!(misses() - old - scalar, 2);
}

#[test]
fn grouped_reads_respect_fill_cache_and_release_ephemeral_blocks() {
    use mmdb::BlockCachePool;
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let pool = Arc::new(BlockCachePool::new(1 << 20));
    let db = DB::open(
        DbOptions::default().with_block_cache(Some(pool.clone())),
        dir.path(),
    )
    .unwrap();
    db.ingest_sorted([(b"a", b"alpha".as_slice()), (b"b", b"bravo".as_slice())])
        .unwrap();
    let options = ReadOptions::default().with_fill_cache(false);
    let held = db
        .get_many_pinned_with_options(&options, &[b"b", b"a"])
        .unwrap();
    pool.run_pending_tasks();
    assert_eq!(pool.entry_count(), 0);
    assert_eq!(held[0].as_deref(), Some(b"bravo".as_slice()));
    let warm = db.get_many_pinned(&[b"a", b"b"]).unwrap();
    pool.run_pending_tasks();
    assert!(pool.entry_count() >= 2);
    db.close().unwrap();
    assert_eq!(pool.entry_count(), 0);
    assert_eq!(warm[0].as_deref(), Some(b"alpha".as_slice()));
    assert_eq!(held[1].as_deref(), Some(b"alpha".as_slice()));
}

#[test]
fn grouped_data_slots_are_scoped_to_their_file_and_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(DbOptions::default().with_block_size(256), dir.path()).unwrap();
    for start in [0u8, 64] {
        // Every file has many data slots in one leaf, with distinct values.
        db.ingest_sorted((start..start + 64).map(|i| ([i], [i; 96])))
            .unwrap();
    }
    let rows = db
        .get_many_pinned(&[&[68], &[2], &[66], &[3], &[67]])
        .unwrap();
    for (row, key) in rows.into_iter().zip([68, 2, 66, 3, 67]) {
        assert_eq!(row.unwrap().as_ref(), &[key; 96]);
    }
}

#[test]
fn grouped_reads_propagate_corrupt_data_instead_of_partial_results() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(DbOptions::default(), dir.path()).unwrap();
    db.ingest_sorted([(b"a", b"one"), (b"b", b"two")]).unwrap();
    db.ingest_sorted([(b"z", b"last")]).unwrap();
    db.close().unwrap();
    let path = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|f| f.ok().map(|f| f.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "sst"))
        .max()
        .unwrap();
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut b = [0];
    f.read_exact(&mut b).unwrap();
    b[0] ^= 1;
    f.rewind().unwrap();
    f.write_all(&b).unwrap();
    drop(f);
    let db = DB::open_read_only(dir.path()).unwrap();
    assert_eq!(&*db.get_pinned(b"a").unwrap().unwrap(), b"one");
    assert_eq!(
        db.get_pinned(b"z").unwrap_err().kind(),
        ErrorKind::Corruption
    );
    let before = db.get_property("stats.bytes_read").unwrap();
    assert_eq!(
        db.get_many_pinned(&[b"z", b"a", b"a", b"b"])
            .unwrap_err()
            .kind(),
        ErrorKind::Corruption
    );
    assert_eq!(
        db.get_property("stats.bytes_read").unwrap(),
        before,
        "failed request returned no bytes"
    );
}

#[test]
fn grouped_reads_see_complete_atomic_batches_during_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(
        DbOptions::default().with_write_buffer_size(8192),
        dir.path(),
    )
    .unwrap();
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|s| {
        let writer = s.spawn(|| {
            barrier.wait();
            for i in 0u32..400 {
                let mut batch = WriteBatch::new();
                for key in [b"left".as_slice(), b"right"] {
                    batch.put(key, &i.to_be_bytes());
                }
                db.write(batch).unwrap();
            }
        });
        barrier.wait();
        for _ in 0..400 {
            let pair = visible(&db, &ReadOptions::default(), &[b"left", b"right"]);
            assert_eq!(pair[0], pair[1]);
        }
        writer.join().unwrap();
    });
}

#[test]
fn grouped_reads_share_two_uncached_overlapping_files_and_duplicate_requests() {
    let dir = tempfile::tempdir().unwrap();
    let options = DbOptions::default()
        .with_block_cache_capacity(0)
        .with_block_size(16 << 10)
        .with_l0_compaction_trigger(usize::MAX);
    let db = DB::open(options, dir.path()).unwrap();
    db.ingest_sorted((0u8..32).map(|i| ([i], [i, 0]))).unwrap();
    let mut odd = WriteBatch::new();
    for i in (1u8..32).step_by(2) {
        odd.put(&[i], &[i, 1]);
    }
    db.write(odd).unwrap();
    db.flush().unwrap();
    let raw: Vec<_> = (0u8..32).rev().map(|id| [id]).collect();
    let keys: Vec<_> = raw.iter().map(|k| k.as_slice()).collect();
    let misses = || {
        db.get_property("stats.block_cache_misses")
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    // Key 10 has no visible point in the newest SST at this snapshot. Its
    // seek falls past that file's final partition, without loading a block.
    let snapshot = db.snapshot();
    db.put(&[10], &[10, 2]).unwrap();
    db.flush().unwrap();
    let before = misses();
    let rows = db
        .get_many_pinned_with_options(&snapshot.read_options(), &keys)
        .unwrap();
    let mut owners = Vec::new();
    for (row, key) in rows.iter().zip(&keys) {
        let row = row.as_ref().unwrap();
        assert_eq!(row.as_ref(), &[key[0], key[0] % 2]);
        owners.push(row.sst_backing_charge().unwrap().0);
    }
    owners.sort_unstable();
    owners.dedup();
    assert_eq!(
        misses() - before,
        4,
        "one index leaf and data block per file"
    );
    assert_eq!(owners.len(), 2);
    let read_bytes = || {
        db.get_property("stats.bytes_read")
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    let before_bytes = read_bytes();
    let samples = mmdb::point_table_probes();
    let duplicates = db.get_many_pinned(&[&[17], &[17], &[17]]).unwrap();
    assert_eq!(mmdb::point_table_probes() - samples, 1);
    assert_eq!(read_bytes() - before_bytes, 3 * (1 + 2));
    for row in duplicates {
        assert_eq!(row.as_deref(), Some(&[17, 1][..]));
    }
    db.delete_range(&[16], &[18]).unwrap();
    let before_masked = read_bytes();
    let masked = db.get_many_pinned(&[&[17], &[17], &[100], &[100]]).unwrap();
    assert!(masked.iter().all(Option::is_none));
    assert_eq!(read_bytes(), before_masked);
}

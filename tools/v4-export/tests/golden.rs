use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs,
    path::Path,
    process::Command,
};

fn directory_entries(path: &Path) -> BTreeSet<OsString> {
    path.read_dir()
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect()
}

// Some supported OS/filesystem combinations lack an exclusive directory rename.
// Exercise the retry path there and require publication on the Linux CI filesystem.
fn export(source: &Path, dest: &Path, prefix_len: usize) -> bool {
    let parent = dest.parent().unwrap();
    let original_entries = directory_entries(parent);
    let mut export = Command::new(env!("CARGO_BIN_EXE_mmdb-v4-export"));
    export
        .arg(source)
        .arg(dest)
        .arg("3")
        .arg(prefix_len.to_string());
    let output = export.output().unwrap();
    if !output.status.success()
        && String::from_utf8_lossy(&output.stderr)
            .contains("export requires an exclusive directory rename")
    {
        assert!(
            !(cfg!(target_os = "linux") && std::env::var_os("CI").is_some()),
            "the Linux CI filesystem must support exclusive directory publication"
        );
        assert!(!dest.exists());
        let retry = export.output().unwrap();
        assert!(!retry.status.success());
        assert_eq!(output.stderr, retry.stderr);
        assert_eq!(directory_entries(parent), original_entries);
        return false;
    }
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    true
}

#[test]
fn exports_residual_wal_and_snapshot_striped_sst() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/v4.3.15");
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("v5");
    if !export(&fixture, &dest, 2) {
        return;
    }
    let db = v5::DB::open_read_only(&dest).unwrap();
    let mut iter = db.iter().unwrap();
    assert_eq!(
        iter.by_ref().collect::<Vec<_>>(),
        [
            (b"aa".to_vec(), b"restored".to_vec()),
            (vec![255, 0], vec![0, 255])
        ]
    );
    assert!(iter.error().is_none());
}

fn store_files(path: &Path) -> BTreeMap<OsString, Vec<u8>> {
    path.read_dir()
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.file_name(), fs::read(entry.path()).unwrap())
        })
        .collect()
}

fn assert_visible_rows(mut iter: v5::DBIterator, expected: &[(Vec<u8>, Vec<u8>)]) {
    assert_eq!(iter.by_ref().collect::<Vec<_>>(), expected);
    assert!(iter.error().is_none());
}

#[test]
fn eight_byte_prefix_shard_exports_visible_rows_and_reopens_writable() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("v4");
    let dest = tmp.path().join("v5");
    let source_options = v4::DbOptions {
        num_levels: 3,
        prefix_len: 8,
        ..Default::default()
    };
    let old = v4::DB::open(source_options.clone(), &source).unwrap();
    let mut initial = v4::WriteBatch::new();
    initial.put(b"map-0001/a", b"stale");
    initial.put(b"map-0001/b", b"covered");
    initial.put(b"map-0001/b\xff", b"covered");
    initial.put(b"map-0001/c", b"exclusive end");
    initial.put(b"map-0002/a", b"deleted");
    old.write(initial).unwrap();
    old.flush().unwrap();

    let mut residual = v4::WriteBatch::new();
    residual.put(b"map-0001/a", b"new");
    residual.delete_range(b"map-0001/b", b"map-0001/c");
    residual.put(b"map-0001/b", b"restored");
    residual.delete(b"map-0002/a");
    residual.put(b"map-0002/\0", b"");
    old.write_with_options(
        &v4::WriteOptions {
            sync: true,
            ..Default::default()
        },
        residual,
    )
    .unwrap();
    // v4 Drop leaves the synced residual batch in the WAL. Explicit close
    // would flush the active table and lose this recovery boundary.
    drop(old);
    let before = store_files(&source);
    for extension in ["sst", "wal"] {
        assert!(before.iter().any(|(name, content)| {
            Path::new(name)
                .extension()
                .is_some_and(|ext| ext == extension)
                && !content.is_empty()
        }));
    }

    let expected: [(&[u8], &[u8]); 4] = [
        (b"map-0001/a", b"new"),
        (b"map-0001/b", b"restored"),
        (b"map-0001/c", b"exclusive end"),
        (b"map-0002/\0", b""),
    ];
    let expected = expected.map(|(key, value)| (key.to_vec(), value.to_vec()));
    let source_db = v4::DB::open_read_only_with_options(source_options, &source).unwrap();
    let mut source_iter = source_db.iter().unwrap();
    assert_eq!(source_iter.by_ref().collect::<Vec<_>>(), expected);
    assert!(source_iter.error().is_none());
    drop(source_iter);
    source_db.close().unwrap();
    drop(source_db);
    let published = export(&source, &dest, 8);
    assert_eq!(store_files(&source), before);
    if !published {
        return;
    }

    let geometry = v5::DbOptions::default()
        .with_num_levels(3)
        .with_prefix_len(8);
    let installed = store_files(&dest);
    for wrong in [
        geometry.clone().with_num_levels(4),
        geometry.clone().with_prefix_len(2),
    ] {
        assert_eq!(
            v5::DB::open(wrong, &dest).err().unwrap().kind(),
            v5::ErrorKind::InvalidArgument
        );
        assert_eq!(store_files(&dest), installed);
    }
    let writable = v5::DB::open(geometry.clone(), &dest).unwrap();
    assert_visible_rows(writable.iter().unwrap(), &expected);
    writable.put(b"map-0002/new", b"after export").unwrap();
    writable.close().unwrap();
    drop(writable);
    let reopened = v5::DB::open(geometry, &dest).unwrap();
    let mut final_rows = expected.to_vec();
    final_rows.push((b"map-0002/new".to_vec(), b"after export".to_vec()));
    assert_visible_rows(reopened.iter().unwrap(), &final_rows);
    for (prefix, rows) in [
        (b"map-0001", &final_rows[..3]),
        (b"map-0002", &final_rows[3..]),
    ] {
        let iter = reopened
            .iter_with_prefix(prefix, &v5::ReadOptions::default())
            .unwrap();
        assert_visible_rows(iter, rows);
    }
}

#[test]
fn sst_read_error_leaves_destination_unpublished() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/v4.3.15");
    let tmp = tempfile::tempdir().unwrap();
    let damaged = tmp.path().join("v4");
    fs::create_dir(&damaged).unwrap();
    for entry in fs::read_dir(fixture).unwrap() {
        let entry = entry.unwrap();
        fs::copy(entry.path(), damaged.join(entry.file_name())).unwrap();
    }
    let file = damaged.join("000004.sst");
    let mut bytes = fs::read(&file).unwrap();
    bytes[0] ^= 1;
    fs::write(&file, bytes).unwrap();
    let damaged_files = store_files(&damaged);
    let initial_entries = directory_entries(tmp.path());
    let dest = tmp.path().join("v5");
    let output = Command::new(env!("CARGO_BIN_EXE_mmdb-v4-export"))
        .arg(&damaged)
        .arg(&dest)
        .args(["3", "2"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("CRC"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!dest.exists());
    assert_eq!(store_files(&damaged), damaged_files);
    assert_eq!(directory_entries(tmp.path()), initial_entries);
}

#[test]
fn export_cli_preserves_an_existing_destination_and_cleans_staging() {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/v4.3.15");
    let source_files = store_files(&source);
    let tmp = tempfile::tempdir().unwrap();
    let reject = |dest: &Path| {
        let entries = directory_entries(tmp.path());
        let output = Command::new(env!("CARGO_BIN_EXE_mmdb-v4-export"))
            .arg(&source)
            .arg(dest)
            .args(["3", "2"])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert_eq!(store_files(&source), source_files);
        assert_eq!(directory_entries(tmp.path()), entries);
    };

    let dest = tmp.path().join("existing");
    fs::create_dir(&dest).unwrap();
    fs::write(dest.join("marker"), b"original destination").unwrap();
    let expected = store_files(&dest);
    reject(&dest);
    assert_eq!(store_files(&dest), expected);

    #[cfg(unix)]
    {
        let link = tmp.path().join("dangling");
        let missing = tmp.path().join("never-created");
        std::os::unix::fs::symlink(&missing, &link).unwrap();
        reject(&link);
        assert_eq!(fs::read_link(&link).unwrap(), missing);
        assert!(!missing.exists());
    }
}

#[test]
fn v5_appends_the_same_wal_record_as_the_v4_golden_writer() {
    let tmp = tempfile::tempdir().unwrap();
    let db = v5::DB::open(v5::DbOptions::default(), tmp.path()).unwrap();
    let mut setup = v5::WriteBatch::new();
    for id in 0..8 {
        setup.put(&[id], &[id]);
    }
    db.write_with_options(
        &v5::WriteOptions::default().with_durability(v5::DurabilityMode::DisableWal),
        setup,
    )
    .unwrap();
    // Sequence nine is the first record in the v4 fixture's residual WAL.
    let mut batch = v5::WriteBatch::new();
    batch.put_prefixed(b"a", b"a", b"restored");
    batch.put(b"ff", b"erased");
    batch.delete_range(b"ff", b"gg");
    batch.delete_prefixed(b"z", b"z");
    batch.put(&[255, 0], &[0, 255]);
    db.write(batch).unwrap();
    db.sync_wal().unwrap();
    let wals = fs::read_dir(tmp.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .collect::<Vec<_>>();
    assert_eq!(wals.len(), 1);
    assert_eq!(
        fs::read(&wals[0]).unwrap(),
        include_bytes!("../../../tests/fixtures/v4.3.15/000003.wal")
    );
}

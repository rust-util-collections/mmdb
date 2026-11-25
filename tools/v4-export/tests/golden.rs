use std::{path::Path, process::Command};

#[test]
fn exports_residual_wal_and_snapshot_striped_sst() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/v4.3.15");
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("v5");
    let mut export = Command::new(env!("CARGO_BIN_EXE_mmdb-v4-export"));
    export.arg(fixture).arg(&dest).args(["3", "2"]);
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
        assert_eq!(tmp.path().read_dir().unwrap().count(), 0);
        return;
    }
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
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

#[test]
fn sst_read_error_leaves_destination_unpublished() {
    use std::fs;
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
    let dest = tmp.path().join("v5");
    let output = Command::new(env!("CARGO_BIN_EXE_mmdb-v4-export"))
        .arg(damaged)
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
    let wals = std::fs::read_dir(tmp.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .collect::<Vec<_>>();
    assert_eq!(wals.len(), 1);
    assert_eq!(
        std::fs::read(&wals[0]).unwrap(),
        include_bytes!("../../../tests/fixtures/v4.3.15/000003.wal")
    );
}

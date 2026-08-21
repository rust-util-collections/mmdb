use std::{collections::BTreeMap, fs, path::Path, process::Command, time::SystemTime};

use mmdb::{DB, DbOptions, ErrorKind, WriteBatch};

#[derive(Debug, Eq, PartialEq)]
struct FileState {
    contents: Vec<u8>,
    len: u64,
    modified: SystemTime,
}

fn store_state(path: &Path) -> BTreeMap<String, FileState> {
    let mut files = BTreeMap::new();
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            let metadata = entry.metadata().unwrap();
            files.insert(
                entry.file_name().to_string_lossy().into_owned(),
                FileState {
                    contents: fs::read(entry.path()).unwrap(),
                    len: metadata.len(),
                    modified: metadata.modified().unwrap(),
                },
            );
        }
    }
    files
}

fn assert_read_only(err: mmdb::Error) {
    assert_eq!(err.kind(), ErrorKind::ReadOnly, "unexpected error: {err}");
}

#[cfg(unix)]
fn set_store_permissions(path: &Path, read_only: bool) {
    use std::os::unix::fs::PermissionsExt;

    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            fs::set_permissions(
                entry.path(),
                fs::Permissions::from_mode(if read_only { 0o444 } else { 0o644 }),
            )
            .unwrap();
        }
    }
    fs::set_permissions(
        path,
        fs::Permissions::from_mode(if read_only { 0o555 } else { 0o755 }),
    )
    .unwrap();
}

#[test]
fn read_only_replays_residual_wal_and_leaves_store_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    let db = DB::open(DbOptions::default(), path).unwrap();
    db.put(b"a", b"old-a").unwrap();
    db.put(b"b", b"old-b").unwrap();
    db.put(b"c", b"old-c").unwrap();
    db.flush().unwrap();
    db.put(b"a", b"new-a").unwrap();
    db.delete(b"b").unwrap();
    db.delete_range(b"c", b"d").unwrap();
    db.simulate_crash();

    let before = store_state(path);
    #[cfg(unix)]
    set_store_permissions(path, true);

    let db = DB::open_read_only(path).unwrap();
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(b"new-a".as_ref()));
    assert_eq!(db.get(b"b").unwrap(), None);
    assert_eq!(db.get(b"c").unwrap(), None);

    let mut batch = WriteBatch::new();
    batch.put(b"x", b"y");
    assert_read_only(db.put(b"x", b"y").unwrap_err());
    assert_read_only(db.delete(b"a").unwrap_err());
    assert_read_only(db.delete_range(b"a", b"z").unwrap_err());
    assert_read_only(db.write(batch).unwrap_err());
    assert_read_only(db.write(WriteBatch::new()).unwrap_err());
    assert_read_only(db.flush().unwrap_err());
    assert_read_only(db.compact().unwrap_err());
    assert_read_only(db.compact_range(None, None).unwrap_err());

    db.lazy_delete(b"a");
    db.lazy_delete_batch([b"a".as_slice(), b"b".as_slice()]);
    assert_eq!(db.dead_key_count(), 0);
    db.close().unwrap();
    drop(db);

    // Exercise the implicit Drop path separately while the recovered
    // memtable is still non-empty.
    let db = DB::open_read_only(path).unwrap();
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(b"new-a".as_ref()));
    drop(db);

    #[cfg(unix)]
    set_store_permissions(path, false);
    assert_eq!(store_state(path), before);
}

#[test]
fn read_only_requires_current_but_does_not_require_or_create_lock() {
    let empty = tempfile::tempdir().unwrap();
    let err = match DB::open_read_only(empty.path()) {
        Ok(_) => panic!("read-only open must reject a directory without CURRENT"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    assert!(store_state(empty.path()).is_empty());

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let db = DB::open(DbOptions::default(), path).unwrap();
    db.put(b"key", b"value").unwrap();
    db.close().unwrap();
    drop(db);

    fs::remove_file(path.join("LOCK")).unwrap();
    let before = store_state(path);
    let db = DB::open_read_only(path).unwrap();
    assert_eq!(db.get(b"key").unwrap().as_deref(), Some(b"value".as_ref()));
    drop(db);
    assert!(!path.join("LOCK").exists());
    assert_eq!(store_state(path), before);
}

#[test]
fn read_only_ignores_writer_only_open_options() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(DbOptions::default(), dir.path()).unwrap();
    db.close().unwrap();
    drop(db);

    let db = DB::open(
        DbOptions {
            read_only: true,
            create_if_missing: true,
            error_if_exists: true,
            l0_slowdown_trigger: 100,
            l0_stop_trigger: 1,
            ..Default::default()
        },
        dir.path(),
    )
    .unwrap();
    db.close().unwrap();
}

#[cfg(unix)]
#[test]
fn read_only_handles_share_lock_and_exclude_writers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let db = DB::open(DbOptions::default(), path).unwrap();
    db.close().unwrap();
    drop(db);

    let first = DB::open_read_only(path).unwrap();
    let second = DB::open_read_only(path).unwrap();
    let writer = DB::open(
        DbOptions {
            create_if_missing: false,
            ..Default::default()
        },
        path,
    );
    assert!(writer.is_err(), "LOCK_EX must conflict with shared readers");

    drop(first);
    drop(second);
    let writer = DB::open(
        DbOptions {
            create_if_missing: false,
            ..Default::default()
        },
        path,
    )
    .unwrap();
    let reader = DB::open_read_only(path);
    assert!(reader.is_err(), "LOCK_SH must conflict with a live writer");
    writer.close().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn read_only_uses_no_store_mutating_syscalls() {
    if Command::new("strace").arg("--version").output().is_err() {
        eprintln!("skipping syscall acceptance test: strace is unavailable");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let db = DB::open(DbOptions::default(), path).unwrap();
    db.put(b"probe", b"value").unwrap();
    db.simulate_crash();

    let trace_file = tempfile::NamedTempFile::new().unwrap();
    let output = Command::new("strace")
        .args([
            "-f",
            "-e",
            "trace=open,openat,creat,write,pwrite64,truncate,ftruncate,rename,renameat,renameat2,unlink,unlinkat,mkdir,mkdirat,rmdir,fsync,fdatasync",
            "-o",
        ])
        .arg(trace_file.path())
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "read_only_syscall_probe_child",
            "--ignored",
            "--nocapture",
        ])
        .env("MMDB_READ_ONLY_PROBE_PATH", path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "straced read-only probe failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let trace = fs::read_to_string(trace_file.path()).unwrap();
    let store = path.to_string_lossy();
    for line in trace.lines() {
        let store_path_mutation = line.contains(store.as_ref())
            && (line.contains("O_WRONLY")
                || line.contains("O_RDWR")
                || line.contains("O_CREAT")
                || line.contains("O_TRUNC")
                || line.contains("O_APPEND")
                || line.contains("creat(")
                || line.contains("truncate(")
                || line.contains("rename")
                || line.contains("unlink")
                || line.contains("mkdir")
                || line.contains("rmdir"));
        let forbidden_fd_mutation =
            line.contains("ftruncate(") || line.contains("fsync(") || line.contains("fdatasync(");
        assert!(
            !store_path_mutation && !forbidden_fd_mutation,
            "read-only probe issued a mutating syscall: {line}"
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "helper executed under strace by read_only_uses_no_store_mutating_syscalls"]
fn read_only_syscall_probe_child() {
    let path = std::env::var_os("MMDB_READ_ONLY_PROBE_PATH")
        .expect("MMDB_READ_ONLY_PROBE_PATH must be set by the parent test");
    let db = DB::open_read_only(path).unwrap();
    assert_eq!(
        db.get(b"probe").unwrap().as_deref(),
        Some(b"value".as_ref())
    );
    drop(db);
}

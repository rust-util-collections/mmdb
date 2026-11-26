use std::{collections::BTreeMap, fs, path::Path, time::SystemTime};

#[cfg(target_os = "linux")]
use std::process::Command;

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
    assert_read_only(db.sync_wal().unwrap_err());
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

    let db = DB::open_read_only_with_options(
        DbOptions::default()
            .with_create_if_missing(true)
            .with_error_if_exists(true)
            .with_l0_slowdown_trigger(100)
            .with_l0_stop_trigger(1),
        dir.path(),
    )
    .unwrap();
    db.close().unwrap();
}

#[cfg(unix)]
#[test]
fn directory_lock_reopen_does_not_see_stale_flock() {
    // close/fput can defer releasing a flock. A same-process reopen must not
    // observe EAGAIN after the previous handle has been dropped.
    for _ in 0..20 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let db = DB::open(DbOptions::default(), path).unwrap();
        db.put(b"k", b"v").unwrap();
        db.simulate_crash();
        let reader = DB::open_read_only(path).unwrap();
        assert_eq!(reader.get(b"k").unwrap().as_deref(), Some(b"v".as_slice()));
        drop(reader);

        let first = DB::open_read_only(path).unwrap();
        let second = DB::open_read_only(path).unwrap();
        drop(first);
        drop(second);
        let writer = DB::open(DbOptions::default().with_create_if_missing(false), path).unwrap();
        writer.close().unwrap();
    }
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
    let writer = DB::open(DbOptions::default().with_create_if_missing(false), path);
    assert!(writer.is_err(), "LOCK_EX must conflict with shared readers");

    drop(first);
    drop(second);
    let writer = DB::open(DbOptions::default().with_create_if_missing(false), path).unwrap();
    let reader = DB::open_read_only(path);
    assert!(reader.is_err(), "LOCK_SH must conflict with a live writer");
    writer.close().unwrap();
}

const STORE_TRACE: &str = "trace=open,openat,creat,write,writev,pwrite64,pwritev,pwritev2,truncate,ftruncate,rename,renameat,renameat2,unlink,unlinkat,mkdir,mkdirat,rmdir,fsync,fdatasync";

fn descriptor_path(argument: &str) -> Option<&Path> {
    let (fd, rest) = argument.trim().split_once('<')?;
    if fd != "AT_FDCWD" && fd.parse::<i32>().is_err() {
        return None;
    }
    Some(Path::new(rest.split_once('>')?.0))
}

fn store_mutating_syscall(line: &str, store: &str) -> bool {
    let Some((prefix, args)) = line.split_once('(') else {
        return false;
    };
    let syscall = prefix.split_whitespace().last().unwrap_or("");
    let args = args.split_once(") =").map_or(args, |(args, _)| args);
    match syscall {
        "write" | "writev" | "pwrite64" | "pwritev" | "pwritev2" | "ftruncate" | "fsync"
        | "fdatasync" => return descriptor_path(args).is_some_and(|path| path.starts_with(store)),
        "open" | "openat" => {
            // Flags follow the quoted pathname. A filename may contain flag names.
            let flags = args.rsplit('"').next().unwrap_or("");
            if !["O_WRONLY", "O_RDWR", "O_CREAT", "O_TRUNC", "O_APPEND"]
                .iter()
                .any(|flag| flags.contains(flag))
            {
                return false;
            }
        }
        "creat" | "truncate" | "rename" | "renameat" | "renameat2" | "unlink" | "unlinkat"
        | "mkdir" | "mkdirat" | "rmdir" => {}
        _ => return false,
    }
    // For *at calls, resolve each relative pathname against its annotated
    // directory descriptor. An absolute pathname ignores that descriptor.
    let mut fields = args.split('"');
    while let (Some(before), Some(name)) = (fields.next(), fields.next()) {
        let path = Path::new(name);
        if path.starts_with(store)
            || (path.is_relative()
                && descriptor_path(before.trim().trim_matches(','))
                    .is_some_and(|dir| dir.join(path).starts_with(store)))
        {
            return true;
        }
    }
    false
}

#[test]
fn read_only_trace_rejects_descriptor_write_intent() {
    let store = "/tmp/mmdb-store";
    for line in [
        r#"123 write(3</tmp/mmdb-store/000001.wal>, "x", 1) = -1 EBADF (Bad file descriptor)"#,
        r#"123 pwrite64(3</tmp/mmdb-store/000001.sst>, "x", 1, 0) = -1 EBADF (Bad file descriptor)"#,
        r#"123 writev(3</tmp/mmdb-store/000001.wal>, [{iov_base="x", iov_len=1}], 1) = -1 EBADF"#,
        r#"123 openat(3</tmp/mmdb-store>, "LOCK", O_RDWR) = -1 EACCES"#,
        r#"123 rename("/tmp/scratch/new", "/tmp/mmdb-store/CURRENT") = -1 EACCES"#,
        r#"123 openat(AT_FDCWD, "/tmp/mmdb-store/LOCK", O_WRONLY) = -1 EACCES (Permission denied)"#,
        "123 fsync(3</tmp/mmdb-store/000001.wal>) = 0",
    ] {
        let syscall = line
            .split_once('(')
            .unwrap()
            .0
            .split_whitespace()
            .last()
            .unwrap();
        assert!(
            STORE_TRACE
                .trim_start_matches("trace=")
                .split(',')
                .any(|s| s == syscall)
        );
        assert!(
            store_mutating_syscall(line, store),
            "missed write intent: {line}"
        );
    }
    for line in [
        r#"123 openat(AT_FDCWD, "/tmp/mmdb-store/LOCK", O_RDONLY) = 3</tmp/mmdb-store/LOCK>"#,
        r#"123 write(1<pipe:[123]>, "/tmp/mmdb-store", 15) = 15"#,
        r#"123 write(2</dev/pts/0>, "probe passed", 12) = 12"#,
        r#"123 write(3</tmp/mmdb-store-other/log>, "x", 1) = 1"#,
        "123 fsync(3</tmp/mmdb-store-other/log>) = 0",
        "123 fdatasync(3</tmp/unrelated>) = 0",
        "123 ftruncate(3</tmp/unrelated>, 0) = 0",
        r#"123 openat(AT_FDCWD, "/tmp/mmdb-store-other/LOCK", O_RDWR) = 3</tmp/mmdb-store-other/LOCK>"#,
        r#"123 openat(3</tmp/mmdb-store>, "/tmp/unrelated", O_RDWR) = -1 EACCES"#,
    ] {
        assert!(
            !store_mutating_syscall(line, store),
            "unrelated syscall: {line}"
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn read_only_uses_no_store_mutating_syscalls() {
    if Command::new("strace").arg("--version").output().is_err() {
        // The trace is the only check that no write-intent syscall reaches a
        // read-only store. A silent skip in CI would remove that coverage.
        assert!(
            std::env::var_os("CI").is_none(),
            "strace is required for the read-only syscall acceptance test in CI"
        );
        eprintln!("skipping syscall acceptance test: strace is unavailable");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("current");
    let corrupt_path = dir.path().join("corrupt");
    fs::create_dir(&corrupt_path).unwrap();
    fs::write(corrupt_path.join("CURRENT"), b"MANIFEST-000001\n").unwrap();
    fs::write(corrupt_path.join("MANIFEST-000001"), []).unwrap();
    let db = DB::open(DbOptions::default(), &path).unwrap();
    db.put(b"probe", b"value").unwrap();
    db.simulate_crash();

    let trace_file = tempfile::NamedTempFile::new().unwrap();
    let output = Command::new("strace")
        .args(["-f", "-yy", "-e", STORE_TRACE, "-o"])
        .arg(trace_file.path())
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "read_only_syscall_probe_child",
            "--ignored",
            "--nocapture",
        ])
        .env("MMDB_READ_ONLY_PROBE_PATH", &path)
        .env("MMDB_READ_ONLY_CORRUPT_PATH", corrupt_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "straced read-only probe failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let trace = fs::read_to_string(trace_file.path()).unwrap();
    let store = dir.path().to_string_lossy();
    for line in trace.lines() {
        assert!(
            !store_mutating_syscall(line, &store),
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
    assert!(db.contains_key(b"probe").unwrap());
    assert_eq!(db.sync_wal().unwrap_err().kind(), ErrorKind::ReadOnly);
    let corrupt = std::env::var_os("MMDB_READ_ONLY_CORRUPT_PATH").unwrap();
    assert_eq!(
        DB::open_read_only(corrupt).err().unwrap().kind(),
        ErrorKind::Corruption
    );
    drop(db);
}

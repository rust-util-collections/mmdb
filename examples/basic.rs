//! Canonical MMDB usage: open, write, read, batch, snapshot, iterate, delete.
//!
//! Run with: `cargo run --example basic`

use mmdb::{DB, DbOptions, ErrorKind, ReadOptions, WriteBatch, WriteOptions};

fn main() -> mmdb::Result<()> {
    let dir = tempfile::tempdir().expect("tempdir");

    // --- Open (presets: DbOptions::balanced/write_heavy/read_heavy) ---
    let opts = DbOptions {
        create_if_missing: true,
        ..DbOptions::balanced()
    };
    let db = DB::open(opts, dir.path())?;

    // --- Point writes & reads ---
    db.put(b"user:alice", b"1000")?;
    db.put(b"user:bob", b"2000")?;
    assert_eq!(db.get(b"user:alice")?, Some(b"1000".to_vec()));

    // Durable write: fsync WAL before acknowledging.
    let sync_opts = WriteOptions {
        sync: true,
        ..Default::default()
    };
    db.put_with_options(&sync_opts, b"user:carol", b"3000")?;

    // --- Atomic batch ---
    let mut batch = WriteBatch::new();
    batch.put(b"acct:1", b"open");
    batch.put(b"acct:2", b"open");
    batch.delete(b"user:bob");
    db.write(batch)?;
    assert_eq!(db.get(b"user:bob")?, None);

    // --- Snapshot: consistent point-in-time reads (RAII guard) ---
    let snap = db.snapshot();
    db.put(b"user:alice", b"9999")?;
    assert_eq!(
        db.get_with_options(&snap.read_options(), b"user:alice")?,
        Some(b"1000".to_vec()),
        "snapshot sees the old value"
    );
    assert_eq!(db.get(b"user:alice")?, Some(b"9999".to_vec()));
    drop(snap); // releases the snapshot so compaction may reclaim old versions

    // --- Prefix iteration (bloom-filter pruned) ---
    let users: Vec<_> = db
        .iter_with_prefix(b"user:", &ReadOptions::default())?
        .collect();
    println!("{} keys under user:", users.len());

    // --- Range iteration [acct:1, acct:9) with SST pruning ---
    // fill_cache=false keeps a one-shot scan from evicting hot blocks.
    let scan_opts = ReadOptions {
        fill_cache: false,
        ..Default::default()
    };
    for (k, v) in db.iter_with_range(&scan_opts, Some(b"acct:1"), Some(b"acct:9"))? {
        println!("{} = {}", String::from_utf8_lossy(&k), v.len());
    }

    // --- Range deletion ---
    db.delete_range(b"acct:1", b"acct:9")?;
    assert_eq!(db.get(b"acct:1")?, None);

    // --- Typed error handling ---
    match DB::open(
        DbOptions {
            num_levels: 0, // invalid on purpose
            ..Default::default()
        },
        dir.path().join("bad"),
    ) {
        Err(e) if e.kind() == ErrorKind::InvalidArgument => {
            println!("rejected as expected:\n{e}");
        }
        Err(e) => panic!("expected InvalidArgument, got {e:?}"),
        Ok(_) => panic!("expected InvalidArgument, got Ok"),
    }

    db.flush()?;
    Ok(())
}

//! List users from a snapshot taken before a deletion.
//! Run with `cargo +stable run --example basic`.

use mmdb::{DB, DbOptions, WriteBatch};

fn main() -> mmdb::Result<()> {
    let dir = tempfile::tempdir()?;
    let db = DB::open(DbOptions::default(), dir.path())?;

    let mut batch = WriteBatch::new();
    batch.put(b"user:alice", b"active");
    batch.put(b"user:bob", b"active");
    db.write(batch)?; // Both users become visible together.
    db.sync_wal()?; // The batch is now durable.

    let snapshot = db.snapshot();
    db.delete_range(b"user:alice", b"user:bob")?;
    assert!(!db.contains_key(b"user:alice")?);
    assert!(db.contains_key(b"user:bob")?); // The upper bound is exclusive.

    // The snapshot still lists user:alice followed by user:bob.
    let mut users = snapshot.iter_with_prefix(b"user:")?;
    while let Some((key, _)) = users.step_next()? {
        println!("{}", String::from_utf8_lossy(key));
    }

    db.close()
}

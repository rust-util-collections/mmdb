//! Compare two opt-in write-buffer layouts at the same per-shard allowance.
//! Run with `cargo run --example shard_buffers`. The timed results and
//! selection criteria are in `docs/benchmarks/four-host/README.md`.

use std::sync::Arc;

use mmdb::{BlockCachePool, DB, DbOptions, WriteBatch};

fn main() -> mmdb::Result<()> {
    let dir = tempfile::tempdir()?;
    let pool = Arc::new(BlockCachePool::new(256 << 20));
    let base = DbOptions::default()
        .with_prefix_len(8)
        .with_block_cache(Some(pool));

    // Count the active memtable as well as the frozen slots: 5 × 6 MiB.
    let reference = base
        .clone()
        .with_write_buffer_size(6 << 20)
        .with_max_immutable_memtables(4);
    // 3 × 10 MiB. Compare foreground p99 and outstanding L0 files before
    // choosing this layout for a workload with a tight latency budget.
    let larger_buffers = base
        .with_write_buffer_size(10 << 20)
        .with_max_immutable_memtables(2);

    for (name, options) in [("four-slots", reference), ("two-slots", larger_buffers)] {
        let db = DB::open(options, dir.path().join(name))?;
        let mut batch = WriteBatch::new();
        batch.put(b"account:alice", b"active");
        batch.put(b"account:bob", b"active");
        db.write(batch)?;
        db.sync_wal()?;
        let pins = db.get_many_pinned(&[b"account:alice", b"account:bob"])?;
        assert!(pins.iter().all(|v| v.as_deref() == Some(&b"active"[..])));
        db.close()?;
    }
    Ok(())
}

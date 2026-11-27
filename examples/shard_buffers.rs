//! Two layouts give each shard a nominal 30 MiB write-buffer allowance.
//! Run with `cargo +stable run --example shard_buffers`.

use std::sync::Arc;

use mmdb::{BlockCachePool, DB, DbOptions};

fn main() -> mmdb::Result<()> {
    let dir = tempfile::tempdir()?;
    let pool = Arc::new(BlockCachePool::new(256 << 20)); // 256 MiB per cache store.
    let base = DbOptions::default()
        .with_prefix_len(b"account:".len())
        .with_block_cache(Some(pool));

    for (frozen_slots, buffer_mib) in [(4, 6), (2, 10)] {
        let options = base
            .clone()
            .with_write_buffer_size(buffer_mib * 1024 * 1024)
            .with_max_immutable_memtables(frozen_slots);
        let db = DB::open(options, dir.path().join(format!("{frozen_slots}-slots")))?;
        db.put(b"account:alice", b"active")?;
        db.close()?;

        let total_mib = (1 + frozen_slots) * buffer_mib;
        println!("(1 active + {frozen_slots} frozen) × {buffer_mib} MiB = {total_mib} MiB");
    }
    Ok(())
}

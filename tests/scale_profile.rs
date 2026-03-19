use mmdb::{DB, DbOptions};
use std::time::Instant;

fn bench_at_scale(count: u64) {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        prefix_len: 8,
        write_buffer_size: 2 * 1024 * 1024 * 1024,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();
    let prefix: [u8; 8] = 42u64.to_be_bytes();
    for i in 0u64..count {
        let mut key = Vec::with_capacity(16);
        key.extend_from_slice(&prefix);
        key.extend_from_slice(&i.to_be_bytes());
        db.put(&key, &i.to_be_bytes()).unwrap();
    }

    let n = if count >= 100_000 { 5_000 } else { 20_000 };

    // iter_with_prefix + seek + take(10)
    let start = Instant::now();
    for _ in 0..n {
        let mut iter = db
            .iter_with_prefix(&prefix, &mmdb::ReadOptions::default())
            .unwrap();
        // seek to middle
        let mut seek_key = Vec::with_capacity(16);
        seek_key.extend_from_slice(&prefix);
        seek_key.extend_from_slice(&(count / 2).to_be_bytes());
        iter.seek(&seek_key);
        let c = iter.take(10).count();
        assert!(c > 0);
    }
    let d = start.elapsed();
    let us = d.as_nanos() as f64 / n as f64 / 1000.0;
    eprintln!("  entries={count:>8}  seek+take(10): {us:>8.2} µs/op");
}

#[test]
fn scale_test() {
    eprintln!("\n=== mmdb seek+take(10) at different data scales ===");
    for &count in &[100, 1_000, 10_000, 50_000] {
        bench_at_scale(count);
    }
}
